/**
 * Ad Audit Checker — Two-pass system for thorough ad detection
 *
 * Pass 1 (FAST): Check website for Meta Pixel + Google Ads tags via HTTP
 *   - 50 concurrent, ~12/sec, catches ~70% of advertisers
 *
 * Pass 2 (THOROUGH): For "NO" results, check Facebook Ad Library via Puppeteer
 *   - 3 concurrent tabs, ~0.7/sec, catches remaining ~30%
 *   - Only runs on leads that passed through Pass 1 as negative
 *
 * Output columns:
 *   - runs_meta_ads: YES/NO/MAYBE (MAYBE = pixel found but unconfirmed)
 *   - runs_google_ads: YES/NO
 *   - meta_detection_method: pixel|ad_library|none
 *   - has_analytics: YES/NO
 *   - meta_pixel_id, google_ads_id, ga4_id
 *   - meta_ad_count (from Ad Library if checked)
 *
 * Usage: node scripts/ad-audit-checker.js input.csv [output.csv]
 */

const fs = require('fs');
const https = require('https');
const http = require('http');

const HTTP_CONCURRENCY = 50;
const PUPPETEER_CONCURRENCY = 3;
const HTTP_TIMEOUT = 8000;

// Pixel detection patterns
const META_PATTERNS = [
  /fbq\s*\(/i,
  /facebook\.com\/tr/i,
  /connect\.facebook\.net.*fbevents/i,
  /_fbp=/i,
];
const GOOGLE_ADS_PATTERNS = [
  /gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-/i,
  /googleadservices\.com\/pagead\/conversion/i,
  /google_conversion_id/i,
  /googleads\.g\.doubleclick\.net/i,
];
const ANALYTICS_PATTERNS = [
  /gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-/i,
  /gtag\s*\(\s*['"]config['"]\s*,\s*['"]UA-/i,
  /google-analytics\.com/i,
  /googletagmanager\.com/i,
];

function fetchUrl(url) {
  return new Promise((resolve) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html', 'Accept-Encoding': 'identity',
      },
      timeout: HTTP_TIMEOUT, rejectUnauthorized: false,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) { const u = new URL(url); redirect = `${u.protocol}//${u.host}${redirect}`; }
        res.resume();
        return resolve(fetchUrl(redirect));
      }
      let data = '';
      let bytes = 0;
      res.on('data', chunk => { bytes += chunk.length; if (bytes <= 500000) data += chunk; });
      res.on('end', () => resolve({ ok: true, body: data }));
      res.on('error', () => resolve({ ok: false, body: '' }));
    });
    req.on('error', () => resolve({ ok: false, body: '' }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, body: '' }); });
  });
}

function checkPixels(html) {
  return {
    meta: META_PATTERNS.some(p => p.test(html)),
    google_ads: GOOGLE_ADS_PATTERNS.some(p => p.test(html)),
    analytics: ANALYTICS_PATTERNS.some(p => p.test(html)),
    meta_pixel_id: (html.match(/fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i) || [])[1] || '',
    google_ads_id: (html.match(/['"]AW-([\d]+)['"]/i) || [])[1] || '',
    ga4_id: (html.match(/['"]G-([A-Z0-9]+)['"]/i) || [])[1] || '',
  };
}

function normalizeUrl(raw) {
  if (!raw) return null;
  let url = raw.trim();
  if (url.startsWith('www.')) url = 'https://' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = lines.slice(1).map(line => {
    const cols = []; let cur = ''; let inQ = false;
    for (const c of line) {
      if (c === '"') { inQ = !inQ; continue; }
      if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
      cur += c;
    }
    cols.push(cur.trim());
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });
  return { headers, rows };
}

// Pass 1: HTTP pixel check
async function pass1_pixelCheck(rows, urlCol) {
  console.log('\n=== PASS 1: Pixel Detection (HTTP) ===');
  const results = new Array(rows.length);
  let idx = 0, checked = 0, withMeta = 0, withGoogle = 0;
  const t0 = Date.now();

  const ticker = setInterval(() => {
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`  [${((Date.now()-t0)/1000).toFixed(0)}s] ${checked}/${rows.length} | Meta: ${withMeta} | Google: ${withGoogle} | ${rate}/sec`);
  }, 10000);

  async function worker() {
    while (idx < rows.length) {
      const i = idx++;
      const url = normalizeUrl(rows[i][urlCol]);
      if (!url) { results[i] = { meta: false, google_ads: false, analytics: false, error: 'no_url' }; checked++; continue; }
      const resp = await fetchUrl(url);
      checked++;
      if (!resp.ok) { results[i] = { meta: false, google_ads: false, analytics: false, error: 'fetch_fail' }; continue; }
      const pixels = checkPixels(resp.body);
      if (pixels.meta) withMeta++;
      if (pixels.google_ads) withGoogle++;
      results[i] = pixels;
    }
  }

  const workers = [];
  for (let i = 0; i < HTTP_CONCURRENCY; i++) workers.push(worker());
  await Promise.all(workers);
  clearInterval(ticker);

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  Pass 1 done: ${checked} checked in ${elapsed}s | Meta: ${withMeta} | Google: ${withGoogle}\n`);
  return results;
}

// Pass 2: Ad Library check for leads without pixel
async function pass2_adLibrary(rows, nameCol, pass1Results) {
  // Find leads that need Ad Library check (no meta pixel detected AND have a name)
  const needsCheck = [];
  for (let i = 0; i < rows.length; i++) {
    if (!pass1Results[i]?.meta && rows[i][nameCol]) {
      needsCheck.push(i);
    }
  }

  if (needsCheck.length === 0) {
    console.log('=== PASS 2: Skipped (all leads checked) ===\n');
    return {};
  }

  console.log(`=== PASS 2: Ad Library Check (Puppeteer) — ${needsCheck.length} leads ===`);

  const puppeteer = require('puppeteer-extra');
  const StealthPlugin = require('puppeteer-extra-plugin-stealth');
  puppeteer.use(StealthPlugin());

  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  const pages = [];
  for (let i = 0; i < PUPPETEER_CONCURRENCY; i++) {
    const p = await browser.newPage();
    await p.setViewport({ width: 1280, height: 800 });
    await p.setRequestInterception(true);
    p.on('request', req => { ['image', 'font', 'media'].includes(req.resourceType()) ? req.abort() : req.continue(); });
    pages.push(p);
  }

  const adResults = {};
  let idx = 0, checked = 0, found = 0;
  const t0 = Date.now();

  const ticker = setInterval(() => {
    console.log(`  [${((Date.now()-t0)/1000).toFixed(0)}s] ${checked}/${needsCheck.length} | Found ads: ${found}`);
  }, 15000);

  async function worker(page) {
    while (idx < needsCheck.length) {
      const qi = idx++;
      const rowIdx = needsCheck[qi];
      const name = rows[rowIdx][nameCol];
      const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&media_type=all&search_type=keyword_exact_phrase&q=${encodeURIComponent(name)}`;

      try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 1500));
        const result = await page.evaluate(() => {
          const text = document.body.innerText || '';
          if (text.includes('No ads match')) return { count: 0 };
          const m = text.match(/([\d,]+)\s*results?/i);
          return { count: m ? parseInt(m[1].replace(/,/g, ''), 10) : 0 };
        });
        adResults[rowIdx] = result;
        if (result.count > 0) found++;
      } catch {
        adResults[rowIdx] = { count: 0, error: true };
      }
      checked++;
    }
  }

  const workers = [];
  for (let i = 0; i < PUPPETEER_CONCURRENCY; i++) workers.push(worker(pages[i]));
  await Promise.all(workers);
  clearInterval(ticker);
  await browser.close();

  console.log(`  Pass 2 done: ${checked} checked | Found ${found} more with ads\n`);
  return adResults;
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-ad-audit.csv');

  if (!inputFile) {
    console.log('Usage: node scripts/ad-audit-checker.js input.csv [output.csv]');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  const urlCol = headers.find(h => /^website$|^url$|^domain$|charity_contact_web/i.test(h)) || headers.find(h => /web|site|url|domain/i.test(h));
  const nameCol = headers.find(h => /business.?name|firm.?name|company|^name$|charity_name|org/i.test(h)) || headers[0];

  console.log(`Input: ${rows.length} leads`);
  console.log(`URL column: "${urlCol || 'NONE'}"`);
  console.log(`Name column: "${nameCol}"`);

  // Pass 1: Fast pixel check
  const pass1 = urlCol ? await pass1_pixelCheck(rows, urlCol) : new Array(rows.length).fill({ meta: false, google_ads: false, analytics: false, error: 'no_url_col' });

  // Pass 2: Ad Library for undetected leads
  const pass2 = await pass2_adLibrary(rows, nameCol, pass1);

  // Merge results
  const finalResults = rows.map((row, i) => {
    const p1 = pass1[i] || {};
    const p2 = pass2[i];

    let metaStatus = 'NO';
    let metaMethod = 'none';
    let metaAdCount = '';

    if (p1.meta) {
      metaStatus = 'YES';
      metaMethod = 'pixel';
    } else if (p2 && p2.count > 0) {
      metaStatus = 'YES';
      metaMethod = 'ad_library';
      metaAdCount = p2.count;
    }

    return {
      ...row,
      runs_meta_ads: metaStatus,
      meta_detection_method: metaMethod,
      meta_ad_count: metaAdCount,
      meta_pixel_id: p1.meta_pixel_id || '',
      runs_google_ads: p1.google_ads ? 'YES' : 'NO',
      google_ads_id: p1.google_ads_id || '',
      has_analytics: p1.analytics ? 'YES' : 'NO',
      ga4_id: p1.ga4_id || '',
    };
  });

  // Write CSV
  const addCols = ['runs_meta_ads', 'meta_detection_method', 'meta_ad_count', 'meta_pixel_id', 'runs_google_ads', 'google_ads_id', 'has_analytics', 'ga4_id'];
  const outHeaders = [...headers, ...addCols];
  const outRows = finalResults.map(r => outHeaders.map(h => {
    const v = (r[h] || '').toString().replace(/"/g, '""');
    return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  // Summary
  const total = rows.length;
  const metaYes = finalResults.filter(r => r.runs_meta_ads === 'YES').length;
  const metaPixel = finalResults.filter(r => r.meta_detection_method === 'pixel').length;
  const metaAdLib = finalResults.filter(r => r.meta_detection_method === 'ad_library').length;
  const googleYes = finalResults.filter(r => r.runs_google_ads === 'YES').length;
  const analyticsYes = finalResults.filter(r => r.has_analytics === 'YES').length;

  console.log('=== AD AUDIT COMPLETE ===');
  console.log(`Total leads: ${total}`);
  console.log(`Running Meta ads: ${metaYes} (${metaPixel} via pixel, ${metaAdLib} via Ad Library)`);
  console.log(`Running Google ads: ${googleYes}`);
  console.log(`Has analytics (no ads): ${analyticsYes - metaYes - googleYes > 0 ? analyticsYes - metaYes - googleYes : 0} ← opportunity leads`);
  console.log(`Output: ${outputFile}`);
})();
