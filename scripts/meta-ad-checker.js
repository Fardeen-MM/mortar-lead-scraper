/**
 * Meta Ad Library Checker
 *
 * Takes a CSV of business names/domains → checks if running Meta ads.
 * Uses Puppeteer to search Facebook Ad Library for each business.
 *
 * Usage: node scripts/meta-ad-checker.js input.csv [output.csv]
 *
 * Input CSV must have a column: business_name or firm_name or name
 * Output CSV adds: meta_ads_active, meta_ad_count, meta_ads_url
 */

const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// Use keyword_exact_phrase for better matching — searches for the exact business name
const AD_LIBRARY_BASE = 'https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&media_type=all&search_type=keyword_exact_phrase&q=';

async function checkMetaAds(page, businessName) {
  const searchUrl = AD_LIBRARY_BASE + encodeURIComponent(businessName);

  try {
    await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 20000 });
    await new Promise(r => setTimeout(r, 2000));

    // Extract results
    const result = await page.evaluate(() => {
      const text = document.body.innerText;

      // Check for "No ads match" or empty results
      if (text.includes('No ads match') || text.includes('no results') || text.includes('No results found')) {
        return { active: false, count: 0, ads: [] };
      }

      // Try to find result count
      let count = 0;
      const countMatch = text.match(/([\d,]+)\s*results?/i) ||
                         text.match(/Showing\s*([\d,]+)/i) ||
                         text.match(/About\s*([\d,]+)/i);
      if (countMatch) {
        count = parseInt(countMatch[1].replace(/,/g, ''), 10);
      }

      // If no explicit count but there are ad cards, count them
      if (!count) {
        const adCards = document.querySelectorAll('[data-testid*="ad"], div[class*="AdCard"], div[class*="_99s5"]');
        count = adCards.length;
      }

      // Extract first few ad details
      const ads = [];
      const adElements = document.querySelectorAll('div._7jyr, div[class*="AdCard"]');
      adElements.forEach((el, i) => {
        if (i >= 3) return; // First 3 ads only
        const adText = el.innerText.substring(0, 300);
        ads.push(adText);
      });

      // Get advertiser name if shown
      let advertiser = '';
      const advertiserEl = document.querySelector('div._7jwu, span[class*="advertiser"]');
      if (advertiserEl) advertiser = advertiserEl.textContent.trim();

      return {
        active: count > 0,
        count,
        advertiser,
        ads: ads.slice(0, 3),
      };
    });

    return {
      ...result,
      url: searchUrl,
      error: null,
    };
  } catch (err) {
    return {
      active: false,
      count: 0,
      advertiser: '',
      ads: [],
      url: searchUrl,
      error: err.message.substring(0, 100),
    };
  }
}

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };

  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = lines.slice(1).map(line => {
    const cols = line.split(',').map(c => c.trim().replace(/^"|"$/g, ''));
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });

  return { headers, rows };
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-meta-ads.csv');

  if (!inputFile) {
    console.log('Usage: node scripts/meta-ad-checker.js input.csv [output.csv]');
    process.exit(1);
  }

  // Parse input CSV
  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  // Find the name column
  const nameCol = headers.find(h =>
    /business.?name|firm.?name|company|^name$/i.test(h)
  ) || headers.find(h => /org|charity/i.test(h)) || headers[0];

  console.log(`Input: ${rows.length} businesses (name column: "${nameCol}")`);
  console.log(`Output: ${outputFile}`);

  // Launch browser
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  const results = [];
  let checked = 0;
  let withAds = 0;
  const t0 = Date.now();

  for (const row of rows) {
    const name = row[nameCol];
    if (!name) continue;

    checked++;
    const result = await checkMetaAds(page, name);

    if (result.active) withAds++;

    results.push({
      ...row,
      meta_ads_active: result.active ? 'YES' : 'NO',
      meta_ad_count: result.count,
      meta_advertiser: result.advertiser,
      meta_ads_url: result.url,
      meta_ads_sample: result.ads[0] ? result.ads[0].substring(0, 100) : '',
      meta_check_error: result.error || '',
    });

    if (checked % 10 === 0 || checked <= 3) {
      const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
      const rate = (checked / ((Date.now() - t0) / 1000)).toFixed(1);
      console.log(`[${elapsed}s] ${checked}/${rows.length} checked | ${withAds} with ads | ${rate}/sec | Last: ${name.substring(0, 40)} → ${result.active ? 'ACTIVE (' + result.count + ' ads)' : 'NO ADS'}`);
    }

    // Small delay to avoid rate limiting
    await new Promise(r => setTimeout(r, 500));
  }

  await browser.close();

  // Write output CSV
  const outHeaders = [...headers, 'meta_ads_active', 'meta_ad_count', 'meta_advertiser', 'meta_ads_url', 'meta_ads_sample', 'meta_check_error'];
  const outRows = results.map(r => outHeaders.map(h => {
    const v = (r[h] || '').toString().replace(/"/g, '""');
    return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
  }).join(','));

  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);
  console.log(`\n=== META AD CHECK COMPLETE ===`);
  console.log(`Checked: ${checked} businesses`);
  console.log(`Running Meta Ads: ${withAds} (${Math.round(100 * withAds / checked)}%)`);
  console.log(`Time: ${elapsed} min`);
  console.log(`Output: ${outputFile}`);
})();
