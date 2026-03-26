#!/usr/bin/env node
/**
 * Ad Verification Spot Checker
 *
 * Takes a result CSV from any ad checker tool and randomly picks N leads.
 * For each lead, shows:
 *   1. What our tools detected (meta/google signals, confidence)
 *   2. Direct verification URLs you can open in a browser:
 *      - Facebook Ad Library search URL
 *      - Google Ads Transparency search URL
 *      - The business website URL
 *
 * Usage: node scripts/ad-verify-spot-check.js results.csv [count]
 *
 * This is for MANUAL verification — open the links in Chrome and compare.
 */

const fs = require('fs');

function parseLine(line) {
  const cols = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}

const inputFile = process.argv[2];
const count = parseInt(process.argv[3] || '5', 10);

if (!inputFile) {
  console.log('Ad Verification Spot Checker');
  console.log('Usage: node scripts/ad-verify-spot-check.js results.csv [count]');
  console.log('\nRandomly picks leads and shows verification URLs to manually check.');
  process.exit(1);
}

const csvText = fs.readFileSync(inputFile, 'utf8');
const lines = csvText.split('\n').filter(l => l.trim());
const headers = parseLine(lines[0]);
const rows = [];
for (let i = 1; i < lines.length; i++) {
  const cols = parseLine(lines[i]);
  const obj = {};
  headers.forEach((h, j) => { obj[h] = cols[j] || ''; });
  rows.push(obj);
}

const col = n => headers.find(h => new RegExp(n, 'i').test(h)) || '';

// Pick random rows
const indices = [];
const available = [...Array(rows.length).keys()];
for (let i = 0; i < Math.min(count, rows.length); i++) {
  const idx = Math.floor(Math.random() * available.length);
  indices.push(available[idx]);
  available.splice(idx, 1);
}

console.log(`\n  Spot-checking ${indices.length} random leads from ${rows.length} total\n`);

for (const idx of indices) {
  const r = rows[idx];

  // Get relevant fields
  const email = r[col('^email$')] || r[col('email')] || '';
  const name = r[col('^name$')] || r[col('company')] || r[col('business.?name')] || '';
  const firstName = r[col('first.?name')] || '';
  const lastName = r[col('last.?name')] || '';
  const website = r[col('^website$')] || r[col('^url$')] || r[col('domain')] || '';
  const fbPage = r[col('facebook_page')] || r[col('^facebook$')] || '';
  const metaAds = r[col('meta_ads')] || r[col('runs_meta_ads')] || '';
  const metaConf = r[col('meta_confidence')] || '';
  const metaSig = r[col('meta_signals')] || '';
  const googleAds = r[col('google_ads')] || r[col('runs_google_ads')] || '';
  const googleConf = r[col('google_confidence')] || '';
  const category = r[col('lead_category')] || '';
  const pixelId = r[col('meta_pixel_id')] || '';
  const googleId = r[col('google_ads_id')] || '';

  const displayName = name || (firstName + ' ' + lastName).trim() || email.split('@')[0];
  const searchName = displayName.replace(/\.com|\.org|\.net|facebook\.com\//gi, '').trim();

  console.log('  ╔════════════════════════════════════════════════════════════');
  console.log(`  ║ #${idx + 1}: ${displayName} (${category})`);
  console.log('  ╠════════════════════════════════════════════════════════════');
  console.log(`  ║ Email:     ${email}`);
  console.log(`  ║ Website:   ${website}`);
  console.log(`  ║ FB Page:   ${fbPage}`);
  console.log(`  ║ Meta Ads:  ${metaAds} (confidence: ${metaConf}%) [${metaSig}]`);
  if (pixelId) console.log(`  ║ Pixel ID:  ${pixelId}`);
  console.log(`  ║ Google:    ${googleAds} (confidence: ${googleConf}%)`);
  if (googleId) console.log(`  ║ Google ID: AW-${googleId}`);
  console.log('  ║');
  console.log('  ║ VERIFY MANUALLY:');
  console.log(`  ║   FB Ad Library:  https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&q=${encodeURIComponent(searchName)}`);
  console.log(`  ║   Google Trans:   https://adstransparency.google.com/?search=${encodeURIComponent(searchName)}`);
  if (website) console.log(`  ║   Website:        ${website.startsWith('http') ? website : 'https://' + website}`);
  if (fbPage) console.log(`  ║   FB Page:        https://www.${fbPage}`);
  console.log('  ╚════════════════════════════════════════════════════════════\n');
}

console.log('  Open the verification URLs in Chrome to manually confirm.');
console.log('  Compare what you see in the ad libraries with what our tool detected.\n');
