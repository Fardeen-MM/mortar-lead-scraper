#!/usr/bin/env node
/**
 * NICEIC Scrape + CSV Export
 *
 * Full pipeline: acquire session → scrape all UK postcodes → export CSV
 *
 * Usage:
 *   node scripts/niceic-scrape.js                    # All 120+ postcodes
 *   node scripts/niceic-scrape.js --test              # 10 major city postcodes, 2 pages each
 *   node scripts/niceic-scrape.js --postcodes=5       # First 5 postcodes
 *   NICEIC_SESSION_ID=abc123 node scripts/niceic-scrape.js  # Pre-acquired session
 *
 * Output: output/niceic-uk-electricians.csv
 */

const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'niceic-uk-electricians.csv');

// CSV columns
const COLUMNS = [
  'firm_name', 'phone', 'email', 'website', 'city', 'state', 'zip',
  'address', 'bar_number', 'bar_status', 'practice_area', 'source',
  'distance', 'all_trades',
];

function escapeCsv(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function leadToCsv(lead) {
  return COLUMNS.map(col => escapeCsv(lead[col] || '')).join(',');
}

(async () => {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const postcodesArg = args.find(a => a.startsWith('--postcodes='));
  const maxPostcodes = postcodesArg ? parseInt(postcodesArg.split('=')[1], 10) : 0;

  console.log('=== NICEIC UK Electricians Scraper ===');
  console.log(`Mode: ${isTest ? 'TEST (10 postcodes, 2 pages)' : 'FULL (120+ postcodes)'}`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log('');

  // Load scraper
  const scraper = require('../scrapers/contractors/niceic');

  // Prepare options
  const options = {};
  if (isTest) {
    options.maxPages = 2;
    options.maxCities = 10;
  } else if (maxPostcodes) {
    options.maxCities = maxPostcodes;
  } else {
    // Full scrape — use extended postcode list
    options.extended = true;
  }

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Write CSV header
  fs.writeFileSync(OUTPUT_FILE, COLUMNS.join(',') + '\n');

  const leads = [];
  let captchaBlocks = 0;
  const startTime = Date.now();

  try {
    for await (const item of scraper.search('installer', options)) {
      if (item._captcha) {
        captchaBlocks++;
        console.log(`[CAPTCHA] ${item.reason}`);
        continue;
      }
      if (item._cityProgress) {
        const pct = Math.round((item._cityProgress.current / item._cityProgress.total) * 100);
        process.stdout.write(`\r[${pct}%] Postcode ${item._cityProgress.current}/${item._cityProgress.total} — ${leads.length} leads so far`);
        continue;
      }

      leads.push(item);
      fs.appendFileSync(OUTPUT_FILE, leadToCsv(item) + '\n');

      if (leads.length % 100 === 0) {
        process.stdout.write(`\r[...] ${leads.length} leads collected...`);
      }
    }
  } catch (err) {
    console.error('\nScrape error:', err.message);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const withPhone = leads.filter(l => l.phone).length;
  const withEmail = leads.filter(l => l.email).length;
  const withWebsite = leads.filter(l => l.website).length;

  console.log('\n');
  console.log('=== RESULTS ===');
  console.log(`Total leads:    ${leads.length}`);
  console.log(`With phone:     ${withPhone} (${Math.round(withPhone / Math.max(leads.length, 1) * 100)}%)`);
  console.log(`With email:     ${withEmail} (${Math.round(withEmail / Math.max(leads.length, 1) * 100)}%)`);
  console.log(`With website:   ${withWebsite} (${Math.round(withWebsite / Math.max(leads.length, 1) * 100)}%)`);
  console.log(`Captcha blocks: ${captchaBlocks}`);
  console.log(`Time elapsed:   ${elapsed}s`);
  console.log(`CSV exported:   ${OUTPUT_FILE}`);

  if (leads.length === 0 && captchaBlocks > 0) {
    console.log('\n--- HOW TO GET DATA ---');
    console.log('1. Visit: https://www.electricalcompetentperson.co.uk/search');
    console.log('2. Enter postcode SW1A 1AA, select "Installation", click Search');
    console.log('3. Open DevTools (F12) > Network tab');
    console.log('4. Find request to /api/directory/search');
    console.log('5. In Response JSON, copy the "searchSessionId" value');
    console.log('6. Re-run: NICEIC_SESSION_ID=<paste-here> node scripts/niceic-scrape.js');
  }
})();
