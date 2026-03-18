#!/usr/bin/env node
/**
 * NHS.UK Practice Scraper — Standalone Runner
 *
 * Downloads dental and GP practice data from NHS.UK and exports to CSV.
 *
 * Usage:
 *   node scripts/run-nhs-scrape.js                     # Both dental + GP
 *   node scripts/run-nhs-scrape.js --dental             # Dental only
 *   node scripts/run-nhs-scrape.js --gp                 # GP only
 *   node scripts/run-nhs-scrape.js --dental --limit 100 # First 100 dental profile fetches
 *   node scripts/run-nhs-scrape.js --email-only         # Only output leads with email
 *
 * Output: data/nhs-uk-{type}-{date}.csv
 *
 * Strategy:
 *   GP: ODS Spine Directory API → enumerate all ~12,800 GP codes → fetch NHS.UK pages
 *   Dental: NHS.UK postcode search (200+ postcodes) → discover ~12,000 XV codes → fetch pages
 *   Each profile page has JSON-LD with email, phone, website, address, coordinates.
 *
 * Expected results:
 *   ~12,000 dental practices (63% with email = ~7,500 emails)
 *   ~12,500 GP practices (48% with email = ~6,000 emails)
 *   Total: ~24,500 practices, ~13,500 emails
 */

const fs = require('fs');
const path = require('path');

// Parse CLI flags
const args = process.argv.slice(2);
const dentalOnly = args.includes('--dental');
const gpOnly = args.includes('--gp');
const emailOnly = args.includes('--email-only');
const limitIdx = args.indexOf('--limit');
const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) : Infinity;

// Determine practice type
let practiceArea = null;
if (dentalOnly) practiceArea = 'dental';
else if (gpOnly) practiceArea = 'gp';

const scraper = require('../scrapers/contractors/nhs-uk');

// Output directory
const DATA_DIR = path.join(__dirname, '..', 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// CSV helpers
const CSV_COLUMNS = [
  'firm_name', 'email', 'phone', 'website', 'address', 'city', 'state', 'zip',
  'practice_area', 'nhs_type', 'bar_number', 'latitude', 'longitude', 'profile_url',
];

function escCsv(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function leadToCsvRow(lead) {
  return CSV_COLUMNS.map(col => escCsv(lead[col] || '')).join(',');
}

async function main() {
  const typeLabel = practiceArea || 'all';
  const dateStr = new Date().toISOString().slice(0, 10);
  const csvPath = path.join(DATA_DIR, `nhs-uk-${typeLabel}-${dateStr}.csv`);

  console.log(`\n=== NHS.UK Practice Scraper ===`);
  console.log(`Type: ${typeLabel}`);
  console.log(`Limit: ${limit === Infinity ? 'none' : limit}`);
  console.log(`Email only: ${emailOnly}`);
  console.log(`Output: ${csvPath}\n`);

  // Write CSV header
  const headerRow = CSV_COLUMNS.join(',');
  fs.writeFileSync(csvPath, headerRow + '\n');

  const startTime = Date.now();
  let total = 0;
  let withEmail = 0;
  let withPhone = 0;
  let withWebsite = 0;
  let written = 0;

  try {
    for await (const lead of scraper.search(practiceArea, {})) {
      // Skip progress signals
      if (lead._cityProgress || lead._captcha) continue;

      total++;

      if (lead.email) withEmail++;
      if (lead.phone) withPhone++;
      if (lead.website) withWebsite++;

      // Apply email-only filter
      if (emailOnly && !lead.email) continue;

      // Write to CSV
      fs.appendFileSync(csvPath, leadToCsvRow(lead) + '\n');
      written++;

      // Progress every 500
      if (total % 500 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`[${elapsed}s] ${total.toLocaleString()} scraped, ${withEmail.toLocaleString()} with email, ${written.toLocaleString()} written`);
      }

      // Check limit (applies to total profile fetches, not written rows)
      if (total >= limit) {
        console.log(`\nReached limit of ${limit} profile fetches`);
        break;
      }
    }
  } catch (err) {
    console.error(`\nError: ${err.message}`);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n=== Results ===`);
  console.log(`Total scraped: ${total.toLocaleString()}`);
  console.log(`With email:    ${withEmail.toLocaleString()} (${total > 0 ? Math.round(withEmail / total * 100) : 0}%)`);
  console.log(`With phone:    ${withPhone.toLocaleString()} (${total > 0 ? Math.round(withPhone / total * 100) : 0}%)`);
  console.log(`With website:  ${withWebsite.toLocaleString()} (${total > 0 ? Math.round(withWebsite / total * 100) : 0}%)`);
  console.log(`Written to CSV: ${written.toLocaleString()}`);
  console.log(`Time: ${elapsed}s`);
  console.log(`File: ${csvPath}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
