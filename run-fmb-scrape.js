#!/usr/bin/env node
/**
 * Run FMB scraper and export results to CSV.
 *
 * Usage: node run-fmb-scrape.js [--max-pages N] [--max-cities N] [--postcode XX]
 */

const fs = require('fs');
const path = require('path');
const fmb = require('./scrapers/contractors/fmb');

const OUTPUT_FILE = path.join(__dirname, 'output', 'fmb-uk-builders.csv');

// Parse CLI args
const args = process.argv.slice(2);
const options = {};
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--max-pages' && args[i + 1]) {
    options.maxPages = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--max-cities' && args[i + 1]) {
    options.maxCities = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--postcode' && args[i + 1]) {
    options.city = args[i + 1];
    i++;
  } else if (args[i] === '--skip-profiles') {
    options.skipProfiles = true;
  }
}

// CSV column order
const CSV_COLUMNS = [
  'firm_name', 'phone', 'website', 'city', 'state', 'zip',
  'profile_url', 'all_trades', 'about', 'bar_status', 'source',
  'practice_area',
];

function escapeCSV(val) {
  const s = String(val || '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function leadToCSVRow(lead) {
  return CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',');
}

async function main() {
  console.log('=== FMB UK Builder Scrape ===');
  console.log(`Options: ${JSON.stringify(options)}`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log('');

  // Write CSV header
  const header = CSV_COLUMNS.join(',');
  fs.writeFileSync(OUTPUT_FILE, header + '\n');

  const leads = [];
  let total = 0;
  let withPhone = 0;
  let withWebsite = 0;
  const startTime = Date.now();

  try {
    for await (const result of fmb.search('', options)) {
      // Skip progress signals
      if (result._cityProgress || result._captcha) {
        if (result._cityProgress) {
          console.log(`[progress] Postcode ${result._cityProgress.current}/${result._cityProgress.total}`);
        }
        if (result._captcha) {
          console.log(`[captcha] ${result.reason || result.city}`);
        }
        continue;
      }

      total++;
      if (result.phone) withPhone++;
      if (result.website) withWebsite++;

      // Append to CSV file
      fs.appendFileSync(OUTPUT_FILE, leadToCSVRow(result) + '\n');
      leads.push(result);

      // Progress every 25 leads
      if (total % 25 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`  [${elapsed}s] ${total} leads scraped (${withPhone} phone, ${withWebsite} website)`);
      }
    }
  } catch (err) {
    console.error(`\nScrape error: ${err.message}`);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n=== SCRAPE COMPLETE ===');
  console.log(`Total leads:    ${total}`);
  console.log(`With phone:     ${withPhone}`);
  console.log(`With website:   ${withWebsite}`);
  console.log(`Time elapsed:   ${elapsed}s`);
  console.log(`Output file:    ${OUTPUT_FILE}`);

  // Show sample
  if (leads.length > 0) {
    console.log('\n--- Sample leads ---');
    leads.slice(0, 5).forEach((l, i) => {
      console.log(`${i + 1}. ${l.firm_name} | ${l.city} | ${l.phone} | ${l.website}`);
    });
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
