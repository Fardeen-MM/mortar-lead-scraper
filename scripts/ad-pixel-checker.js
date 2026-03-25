/**
 * Ad Pixel Checker — Lightning fast Meta + Google ad detection
 *
 * Checks business websites for Meta Pixel and Google Ads tags.
 * Pure HTTP — no Puppeteer, no browser, no API keys needed.
 *
 * Speed: 50 concurrent requests, ~100ms each = 100K leads in ~30 min
 *
 * Detects:
 *   - Meta Pixel (fbq) = running Facebook/Instagram ads
 *   - Google Ads (AW-) = running Google Ads
 *   - Google Analytics (GA4/UA) = has tracking but maybe no ads
 *   - Google Tag Manager = has tag management
 *   - TikTok Pixel = running TikTok ads
 *   - LinkedIn Insight Tag = running LinkedIn ads
 *
 * Usage: node scripts/ad-pixel-checker.js input.csv [output.csv] [concurrency]
 *
 * Input CSV needs a column: website, url, domain, or charity_contact_web
 */

const fs = require('fs');
const https = require('https');
const http = require('http');

const CONCURRENCY = parseInt(process.argv[4] || '50', 10);
const TIMEOUT = 8000; // 8 sec timeout per request

// Detection patterns
const PATTERNS = {
  meta_pixel: [
    /fbq\s*\(/i,
    /facebook\.com\/tr/i,
    /connect\.facebook\.net.*fbevents/i,
    /fb-pixel/i,
    /_fbp=/i,
  ],
  google_ads: [
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-/i,
    /googleadservices\.com\/pagead\/conversion/i,
    /google_conversion_id/i,
    /googleads\.g\.doubleclick\.net/i,
    /goog_report_conversion/i,
  ],
  google_analytics: [
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-/i,
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]UA-/i,
    /google-analytics\.com\/analytics/i,
    /googletagmanager\.com\/gtag/i,
  ],
  google_tag_manager: [
    /googletagmanager\.com\/gtm\.js/i,
    /GTM-[A-Z0-9]{6,}/i,
  ],
  tiktok_pixel: [
    /analytics\.tiktok\.com/i,
    /tiktok.*pixel/i,
  ],
  linkedin_insight: [
    /snap\.licdn\.com\/li\.lms-analytics/i,
    /linkedin.*insight/i,
    /_linkedin_partner_id/i,
  ],
};

// Extract pixel IDs
const ID_PATTERNS = {
  meta_pixel_id: /fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i,
  google_ads_id: /['"]AW-([\d]+)['"]/i,
  ga4_id: /['"]G-([A-Z0-9]+)['"]/i,
  gtm_id: /GTM-([A-Z0-9]{6,})/i,
};

function fetchUrl(url) {
  return new Promise((resolve) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: TIMEOUT,
      rejectUnauthorized: false,
    }, (res) => {
      // Follow redirects (up to 3)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) {
          const u = new URL(url);
          redirect = `${u.protocol}//${u.host}${redirect}`;
        }
        res.resume();
        return resolve(fetchUrl(redirect));
      }

      let data = '';
      let bytes = 0;
      const maxBytes = 500000; // Only read first 500KB

      res.on('data', chunk => {
        bytes += chunk.length;
        if (bytes <= maxBytes) data += chunk;
      });
      res.on('end', () => resolve({ ok: true, body: data, status: res.statusCode }));
      res.on('error', () => resolve({ ok: false, body: '', error: 'response error' }));
    });

    req.on('error', (err) => resolve({ ok: false, body: '', error: err.code || err.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, body: '', error: 'timeout' }); });
  });
}

function checkPixels(html) {
  const result = {};

  // Check each pattern group
  for (const [name, patterns] of Object.entries(PATTERNS)) {
    result[name] = patterns.some(p => p.test(html));
  }

  // Extract IDs
  for (const [name, pattern] of Object.entries(ID_PATTERNS)) {
    const m = html.match(pattern);
    result[name] = m ? m[1] : '';
  }

  // Summary flags
  result.runs_meta_ads = result.meta_pixel;
  result.runs_google_ads = result.google_ads;
  result.has_analytics = result.google_analytics || result.google_tag_manager;

  return result;
}

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = lines.slice(1).map(line => {
    const cols = [];
    let current = '';
    let inQ = false;
    for (const c of line) {
      if (c === '"') { inQ = !inQ; continue; }
      if (c === ',' && !inQ) { cols.push(current.trim()); current = ''; continue; }
      current += c;
    }
    cols.push(current.trim());
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });
  return { headers, rows };
}

function normalizeUrl(raw) {
  if (!raw) return null;
  let url = raw.trim();
  if (url.startsWith('www.')) url = 'https://' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

async function processQueue(rows, urlCol, concurrency) {
  const results = [];
  let idx = 0;
  let checked = 0;
  let withMeta = 0;
  let withGoogle = 0;
  let errors = 0;
  const t0 = Date.now();

  async function worker() {
    while (idx < rows.length) {
      const i = idx++;
      const row = rows[i];
      const rawUrl = row[urlCol];
      const url = normalizeUrl(rawUrl);

      if (!url) {
        results[i] = { ...row, runs_meta_ads: '', runs_google_ads: '', has_analytics: '', meta_pixel_id: '', google_ads_id: '', ga4_id: '', gtm_id: '', check_error: 'no url' };
        checked++;
        continue;
      }

      const resp = await fetchUrl(url);
      checked++;

      if (!resp.ok) {
        results[i] = { ...row, runs_meta_ads: 'ERROR', runs_google_ads: 'ERROR', has_analytics: '', meta_pixel_id: '', google_ads_id: '', ga4_id: '', gtm_id: '', check_error: resp.error };
        errors++;
        continue;
      }

      const pixels = checkPixels(resp.body);
      if (pixels.runs_meta_ads) withMeta++;
      if (pixels.runs_google_ads) withGoogle++;

      results[i] = {
        ...row,
        runs_meta_ads: pixels.runs_meta_ads ? 'YES' : 'NO',
        runs_google_ads: pixels.runs_google_ads ? 'YES' : 'NO',
        has_analytics: pixels.has_analytics ? 'YES' : 'NO',
        meta_pixel_id: pixels.meta_pixel_id || '',
        google_ads_id: pixels.google_ads_id || '',
        ga4_id: pixels.ga4_id || '',
        gtm_id: pixels.gtm_id || '',
        tiktok_ads: pixels.tiktok_pixel ? 'YES' : '',
        linkedin_ads: pixels.linkedin_insight ? 'YES' : '',
        check_error: '',
      };
    }
  }

  // Progress ticker
  const ticker = setInterval(() => {
    const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`[${elapsed}s] ${checked}/${rows.length} | Meta: ${withMeta} | Google: ${withGoogle} | Errors: ${errors} | ${rate}/sec`);
  }, 5000);

  // Launch workers
  const workers = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  clearInterval(ticker);

  return results;
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-ad-check.csv');

  if (!inputFile) {
    console.log('Usage: node scripts/ad-pixel-checker.js input.csv [output.csv] [concurrency]');
    console.log('Concurrency default: 50');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  // Find URL column
  const urlCol = headers.find(h => /^website$|^url$|^domain$|charity_contact_web|website_url/i.test(h)) ||
                 headers.find(h => /web|site|url|domain/i.test(h));

  if (!urlCol) {
    console.error('No website/URL column found. Headers:', headers.join(', '));
    process.exit(1);
  }

  // Count rows with URLs
  const withUrl = rows.filter(r => r[urlCol]?.trim()).length;
  console.log(`Input: ${rows.length} rows, ${withUrl} with URLs (column: "${urlCol}")`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Estimated time: ~${Math.ceil(withUrl / CONCURRENCY / 5)} seconds\n`);

  const t0 = Date.now();
  const results = await processQueue(rows, urlCol, CONCURRENCY);

  // Write output
  const addCols = ['runs_meta_ads', 'runs_google_ads', 'has_analytics', 'meta_pixel_id', 'google_ads_id', 'ga4_id', 'gtm_id', 'tiktok_ads', 'linkedin_ads', 'check_error'];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.map(r => outHeaders.map(h => {
    const v = (r[h] || '').toString().replace(/"/g, '""');
    return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  const totalSec = ((Date.now() - t0) / 1000).toFixed(1);
  const withMeta = results.filter(r => r.runs_meta_ads === 'YES').length;
  const withGoogle = results.filter(r => r.runs_google_ads === 'YES').length;
  const withAnalytics = results.filter(r => r.has_analytics === 'YES').length;
  const errs = results.filter(r => r.check_error && r.check_error !== 'no url').length;

  console.log(`\n=== AD PIXEL CHECK COMPLETE ===`);
  console.log(`Checked: ${withUrl} websites in ${totalSec}s (${(withUrl / totalSec).toFixed(1)}/sec)`);
  console.log(`Meta Pixel (FB ads): ${withMeta} (${Math.round(100 * withMeta / withUrl)}%)`);
  console.log(`Google Ads tag: ${withGoogle} (${Math.round(100 * withGoogle / withUrl)}%)`);
  console.log(`Google Analytics: ${withAnalytics} (${Math.round(100 * withAnalytics / withUrl)}%)`);
  console.log(`Errors: ${errs}`);
  console.log(`Output: ${outputFile}`);
})();
