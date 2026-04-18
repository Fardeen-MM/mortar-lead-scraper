#!/usr/bin/env node
/**
 * Worker process for a single niche scrape.
 * Called by au-gmaps-scrape.js via child_process.fork().
 * Sends results back via IPC message.
 */

const scraper = require('../scrapers/directories/google-maps');
const fs = require('fs');
const path = require('path');

const niche = process.env.NICHE;
const cities = JSON.parse(process.env.CITIES);
const OUTPUT_DIR = process.env.OUTPUT_DIR;

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
  const leads = [];
  let withPhone = 0, withWebsite = 0, withEmail = 0;
  const slug = 'au-' + niche.replace(/\s+/g, '-');

  for (const city of cities) {
    console.log(`\n  [${city}] Starting...`);
    const cityStart = Date.now();
    try {
      for await (const lead of scraper.search(niche, { city, maxPages: 1, niche })) {
        if (lead._cityProgress || lead._captcha) continue;
        leads.push(lead);
        if (lead.phone) withPhone++;
        if (lead.website) withWebsite++;
        if (lead.email) withEmail++;
      }
      const elapsed = ((Date.now() - cityStart) / 1000).toFixed(0);
      console.log(`  [${city}] Done in ${elapsed}s — running total: ${leads.length} leads`);
    } catch (err) {
      console.error(`  [${city}] ERROR: ${err.message}`);
    }
  }

  let file = null;
  if (leads.length > 0) {
    const filename = `gmaps-${slug}.csv`;
    file = writeCsv(filename, leads);
  }

  // Send results back to parent
  if (process.send) {
    process.send({ leads: leads.length, phones: withPhone, websites: withWebsite, emails: withEmail, file });
  }

  process.exit(0);
})().catch(err => {
  console.error(`Worker error (${niche}): ${err.message}`);
  if (process.send) {
    process.send({ leads: 0, phones: 0, websites: 0, emails: 0, file: null });
  }
  process.exit(1);
});
