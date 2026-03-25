/**
 * Ad Checker Final — Maximum accuracy Meta + Google ad detection
 *
 * THREE signals combined for highest accuracy:
 *   1. Meta Pixel on website (HTTP, instant)
 *   2. Facebook page link on website → search Ad Library by FB page name
 *   3. Fallback: search Ad Library by business name
 *
 * Pass 1 (HTTP, 50 concurrent, ~12/sec):
 *   - Check for Meta Pixel, Google Ads tag, Analytics
 *   - Extract Facebook page URL from website (facebook.com/xxx)
 *   - Result: pixel YES/NO + fb_page_slug
 *
 * Pass 2 (Puppeteer, 3 tabs, for Pass 1 negatives only):
 *   - If FB page slug found → search Ad Library by that slug (most accurate)
 *   - Else → search by business name
 *   - Extract page_ids from results for exact match
 *
 * Usage: node scripts/ad-checker-final.js input.csv [output.csv]
 */

const fs = require('fs');
const https = require('https');
const http = require('http');

const HTTP_CONCURRENCY = 50;
const PUPPETEER_CONCURRENCY = 3;

// Pixel patterns
const META_PIXEL = [/fbq\s*\(/i, /facebook\.com\/tr/i, /connect\.facebook\.net.*fbevents/i];
const GOOGLE_ADS = [/gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-/i, /googleadservices\.com\/pagead/i, /google_conversion_id/i];
const ANALYTICS = [/gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-/i, /google-analytics\.com/i, /googletagmanager\.com/i];

function fetchHttp(url) {
  return new Promise(resolve => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', 'Accept': 'text/html', 'Accept-Encoding': 'identity' },
      timeout: 8000, rejectUnauthorized: false,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let r = res.headers.location;
        if (r.startsWith('/')) { const u = new URL(url); r = u.protocol + '//' + u.host + r; }
        res.resume(); return resolve(fetchHttp(r));
      }
      let d = ''; let b = 0;
      res.on('data', c => { b += c.length; if (b < 500000) d += c; });
      res.on('end', () => resolve({ ok: true, body: d }));
      res.on('error', () => resolve({ ok: false, body: '' }));
    });
    req.on('error', () => resolve({ ok: false, body: '' }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, body: '' }); });
  });
}

function extractFbSlug(html) {
  const matches = html.match(/https?:\/\/(?:www\.)?facebook\.com\/([a-zA-Z0-9._-]{3,})/gi) || [];
  const skip = new Set(['sharer', 'share', 'dialog', 'tr', 'plugins', 'pages', 'groups', 'events', 'hashtag', 'login', 'help', 'business', 'privacy', 'policy']);
  for (const match of matches) {
    const m = match.match(/facebook\.com\/([a-zA-Z0-9._-]+)/i);
    if (m && !skip.has(m[1].toLowerCase()) && m[1].length > 2) return m[1];
  }
  return null;
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

// Pass 1: HTTP website scan
async function pass1(rows, urlCol) {
  console.log('\n=== PASS 1: Website Scan (HTTP) ===');
  const results = new Array(rows.length);
  let idx = 0, checked = 0, withPixel = 0, withFbPage = 0, withGoogleAds = 0;
  const t0 = Date.now();

  const ticker = setInterval(() => {
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`  [${((Date.now()-t0)/1000).toFixed(0)}s] ${checked}/${rows.length} | Pixel: ${withPixel} | FB Page: ${withFbPage} | Google: ${withGoogleAds} | ${rate}/sec`);
  }, 10000);

  async function worker() {
    while (idx < rows.length) {
      const i = idx++;
      const url = normalizeUrl(rows[i][urlCol]);
      if (!url) { results[i] = { meta_pixel: false, google_ads: false, analytics: false, fb_slug: null }; checked++; continue; }
      const resp = await fetchHttp(url);
      checked++;
      if (!resp.ok) { results[i] = { meta_pixel: false, google_ads: false, analytics: false, fb_slug: null, error: true }; continue; }

      const meta_pixel = META_PIXEL.some(p => p.test(resp.body));
      const google_ads = GOOGLE_ADS.some(p => p.test(resp.body));
      const analytics = ANALYTICS.some(p => p.test(resp.body));
      const fb_slug = extractFbSlug(resp.body);
      const pixel_id = (resp.body.match(/fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i) || [])[1] || '';
      const gads_id = (resp.body.match(/['"]AW-([\d]+)['"]/i) || [])[1] || '';

      if (meta_pixel) withPixel++;
      if (google_ads) withGoogleAds++;
      if (fb_slug) withFbPage++;

      results[i] = { meta_pixel, google_ads, analytics, fb_slug, pixel_id, gads_id };
    }
  }

  const workers = [];
  for (let i = 0; i < HTTP_CONCURRENCY; i++) workers.push(worker());
  await Promise.all(workers);
  clearInterval(ticker);

  console.log(`  Done: ${checked} in ${((Date.now()-t0)/1000).toFixed(1)}s | Pixel: ${withPixel} | FB Pages: ${withFbPage} | Google: ${withGoogleAds}\n`);
  return results;
}

// Pass 2: Ad Library check for non-pixel leads
async function pass2(rows, nameCol, pass1Results) {
  const needsCheck = [];
  for (let i = 0; i < rows.length; i++) {
    if (!pass1Results[i]?.meta_pixel && (rows[i][nameCol] || pass1Results[i]?.fb_slug)) {
      needsCheck.push(i);
    }
  }

  if (needsCheck.length === 0) {
    console.log('=== PASS 2: Skipped ===\n');
    return {};
  }

  console.log(`=== PASS 2: Ad Library Check — ${needsCheck.length} leads ===`);

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
    console.log(`  [${((Date.now()-t0)/1000).toFixed(0)}s] ${checked}/${needsCheck.length} | Found: ${found}`);
  }, 15000);

  async function worker(page) {
    while (idx < needsCheck.length) {
      const qi = idx++;
      const rowIdx = needsCheck[qi];
      const p1 = pass1Results[rowIdx] || {};

      // Use FB slug if available (more accurate), else business name
      const searchTerm = p1.fb_slug || rows[rowIdx][nameCol];
      if (!searchTerm) { checked++; continue; }

      const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&media_type=all&search_type=keyword_exact_phrase&q=${encodeURIComponent(searchTerm)}`;

      try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 1500));

        const result = await page.evaluate(() => {
          const text = document.body.innerText || '';
          if (text.includes('No ads match')) return { count: 0 };
          const m = text.match(/([\d,]+)\s*results?/i);
          return { count: m ? parseInt(m[1].replace(/,/g, ''), 10) : 0 };
        });

        adResults[rowIdx] = { ...result, searchedBy: p1.fb_slug ? 'fb_slug' : 'name', searchTerm };
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

  console.log(`  Done: ${checked} checked | Found ${found} more\n`);
  return adResults;
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-ad-audit.csv');

  if (!inputFile) { console.log('Usage: node scripts/ad-checker-final.js input.csv [output.csv]'); process.exit(1); }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);
  const urlCol = headers.find(h => /^website$|^url$|^domain$|charity_contact_web/i.test(h)) || headers.find(h => /web|site|url|domain/i.test(h));
  const nameCol = headers.find(h => /business.?name|firm.?name|company|^name$|charity_name|org/i.test(h)) || headers[0];

  console.log(`Input: ${rows.length} leads | URL: "${urlCol}" | Name: "${nameCol}"`);

  const p1 = urlCol ? await pass1(rows, urlCol) : new Array(rows.length).fill({});
  const p2 = await pass2(rows, nameCol, p1);

  // Merge
  const results = rows.map((row, i) => {
    const s1 = p1[i] || {};
    const s2 = p2[i];

    let metaStatus = 'NO';
    let method = 'none';
    let adCount = '';

    if (s1.meta_pixel) { metaStatus = 'YES'; method = 'pixel'; }
    else if (s2 && s2.count > 0) { metaStatus = 'YES'; method = 'ad_library'; adCount = s2.count; }

    return {
      ...row,
      runs_meta_ads: metaStatus,
      detection_method: method,
      meta_ad_count: adCount,
      meta_pixel_id: s1.pixel_id || '',
      facebook_page: s1.fb_slug ? `facebook.com/${s1.fb_slug}` : '',
      runs_google_ads: s1.google_ads ? 'YES' : 'NO',
      google_ads_id: s1.gads_id || '',
      has_analytics: s1.analytics ? 'YES' : 'NO',
    };
  });

  // Write CSV
  const addCols = ['runs_meta_ads', 'detection_method', 'meta_ad_count', 'meta_pixel_id', 'facebook_page', 'runs_google_ads', 'google_ads_id', 'has_analytics'];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.map(r => outHeaders.map(h => {
    const v = (r[h] || '').toString().replace(/"/g, '""');
    return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  // Stats
  const metaYes = results.filter(r => r.runs_meta_ads === 'YES').length;
  const byPixel = results.filter(r => r.detection_method === 'pixel').length;
  const byAdLib = results.filter(r => r.detection_method === 'ad_library').length;
  const googleYes = results.filter(r => r.runs_google_ads === 'YES').length;
  const hasFb = results.filter(r => r.facebook_page).length;
  const analyticsOnly = results.filter(r => r.has_analytics === 'YES' && r.runs_meta_ads === 'NO' && r.runs_google_ads === 'NO').length;

  console.log('=== RESULTS ===');
  console.log(`Total: ${rows.length}`);
  console.log(`Running Meta ads: ${metaYes} (${byPixel} pixel + ${byAdLib} Ad Library)`);
  console.log(`Running Google ads: ${googleYes}`);
  console.log(`Has Facebook page: ${hasFb}`);
  console.log(`Analytics only (opportunity): ${analyticsOnly}`);
  console.log(`Output: ${outputFile}`);
})();
