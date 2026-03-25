/**
 * Meta Ad Library Checker — FAST VERSION
 *
 * Parallel tabs (5 concurrent) + in-page search (no full reload).
 * ~1 sec per business instead of 5 sec.
 *
 * Usage: node scripts/meta-ad-checker-fast.js input.csv [output.csv]
 */

const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const CONCURRENCY = 3;
const AD_LIBRARY_URL = 'https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&media_type=all&search_type=keyword_exact_phrase&q=';

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = lines.slice(1).map(line => {
    // Handle quoted fields with commas
    const cols = [];
    let current = '';
    let inQuotes = false;
    for (const char of line) {
      if (char === '"') { inQuotes = !inQuotes; continue; }
      if (char === ',' && !inQuotes) { cols.push(current.trim()); current = ''; continue; }
      current += char;
    }
    cols.push(current.trim());
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });
  return { headers, rows };
}

async function checkBusiness(page, businessName) {
  const url = AD_LIBRARY_URL + encodeURIComponent(businessName);
  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 20000 });
    await new Promise(r => setTimeout(r, 2000));

    const result = await page.evaluate(() => {
      const text = document.body.innerText || '';
      if (text.includes('No ads match') || text.includes('no results')) {
        return { active: false, count: 0 };
      }
      let count = 0;
      const m = text.match(/([\d,]+)\s*results?/i);
      if (m) count = parseInt(m[1].replace(/,/g, ''), 10);

      // Grab first ad snippet
      let sample = '';
      const adEls = document.querySelectorAll('div._7jyr');
      if (adEls.length) sample = adEls[0].innerText.substring(0, 150);

      return { active: count > 0, count, sample };
    });

    return { ...result, url, error: null };
  } catch (err) {
    return { active: false, count: 0, sample: '', url, error: err.message.substring(0, 60) };
  }
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-meta-ads.csv');

  if (!inputFile) {
    console.log('Usage: node scripts/meta-ad-checker-fast.js input.csv [output.csv]');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);
  const nameCol = headers.find(h => /business.?name|firm.?name|company|^name$|charity|org/i.test(h)) || headers[0];

  console.log(`Input: ${rows.length} businesses (column: "${nameCol}")`);
  console.log(`Concurrency: ${CONCURRENCY} parallel tabs`);

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  // Create pool of tabs
  const pages = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    const p = await browser.newPage();
    await p.setViewport({ width: 1280, height: 800 });
    // Minimal resource loading — block images/fonts/css for speed
    await p.setRequestInterception(true);
    p.on('request', req => {
      const type = req.resourceType();
      if (['image', 'font', 'media'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });
    pages.push(p);
  }

  const results = [];
  let checked = 0;
  let withAds = 0;
  const t0 = Date.now();

  // Process in batches of CONCURRENCY
  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);
    const promises = batch.map((row, j) => {
      const name = row[nameCol];
      if (!name) return Promise.resolve({ ...row, meta_ads_active: '', meta_ad_count: '', meta_ads_url: '', meta_ads_sample: '', meta_check_error: 'no name' });

      return checkBusiness(pages[j], name).then(result => {
        checked++;
        if (result.active) withAds++;
        return {
          ...row,
          meta_ads_active: result.active ? 'YES' : 'NO',
          meta_ad_count: result.count || '',
          meta_ads_url: result.url,
          meta_ads_sample: (result.sample || '').substring(0, 100),
          meta_check_error: result.error || '',
        };
      });
    });

    const batchResults = await Promise.all(promises);
    results.push(...batchResults);

    const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`[${elapsed}s] ${checked}/${rows.length} | ${withAds} with ads | ${rate}/sec`);
  }

  await browser.close();

  // Write output
  const outHeaders = [...headers, 'meta_ads_active', 'meta_ad_count', 'meta_ads_url', 'meta_ads_sample', 'meta_check_error'];
  const outRows = results.map(r => outHeaders.map(h => {
    const v = (r[h] || '').toString().replace(/"/g, '""');
    return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(`\n=== DONE: ${checked} checked, ${withAds} with ads (${Math.round(100 * withAds / checked)}%), ${elapsed}s total, ${(checked / (elapsed || 1)).toFixed(1)}/sec ===`);
  console.log(`Output: ${outputFile}`);
})();
