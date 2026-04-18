#!/usr/bin/env node
/**
 * Scrape a single niche across Australian cities.
 * Usage: NICHE="dentist" node scripts/au-gmaps-single.js
 *   or:  node scripts/au-gmaps-single.js dentist
 */

const scraper = require('../scrapers/directories/google-maps');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const niche = process.argv[2] || process.env.NICHE || 'lawyer';
const cities = ['Sydney, Australia', 'Melbourne, Australia', 'Brisbane, Australia'];

function writeCsv(filename, leads) {
  const headers = ['firm_name', 'city', 'state', 'zip', 'phone', 'email', 'website', 'address', 'profile_url', 'practice_area', 'source'];
  const rows = leads.map(l =>
    headers.map(k => {
      const v = (l[k] || '').toString().replace(/"/g, '""');
      return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v;
    }).join(',')
  );
  const csvPath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(csvPath, [headers.join(','), ...rows].join('\n'));
  return csvPath;
}

(async () => {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const leads = [];
  let withPhone = 0, withWebsite = 0, withEmail = 0;
  const slug = 'au-' + niche.replace(/\s+/g, '-');

  console.log(`=== ${niche.toUpperCase()} — ${cities.length} cities ===`);

  for (const city of cities) {
    const cityStart = Date.now();
    console.log(`\n  [${city}] Starting...`);
    try {
      for await (const lead of scraper.search(niche, { city, maxPages: 1, niche })) {
        if (lead._cityProgress || lead._captcha) continue;
        leads.push(lead);
        if (lead.phone) withPhone++;
        if (lead.website) withWebsite++;
        if (lead.email) withEmail++;
      }
      const elapsed = ((Date.now() - cityStart) / 1000).toFixed(0);
      console.log(`  [${city}] Done in ${elapsed}s — ${leads.length} total leads`);
    } catch (err) {
      console.error(`  [${city}] ERROR: ${err.message}`);
    }
  }

  console.log(`\n  SUMMARY: ${leads.length} leads | ${withPhone} phones | ${withWebsite} websites | ${withEmail} emails`);

  if (leads.length > 0) {
    const filename = `gmaps-${slug}.csv`;
    const csvPath = writeCsv(filename, leads);
    console.log(`  Written: ${csvPath}`);
  }
})().catch(err => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
