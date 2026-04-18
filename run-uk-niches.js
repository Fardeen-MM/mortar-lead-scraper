#!/usr/bin/env node
/**
 * Run UK Google Maps scrapes for remaining niches.
 * Runs sequentially, one niche at a time.
 */
const scraper = require('./scrapers/directories/google-maps');
const fs = require('fs');

const niches = ['accountant', 'dentist', 'physiotherapist', 'beauty salon'];
const cities = ['London, UK', 'Manchester, UK', 'Birmingham, UK'];

(async () => {
  const summary = [];

  for (const niche of niches) {
    const leads = [];
    let count = 0, withPhone = 0, withWebsite = 0;
    const slug = 'uk-' + niche.replace(/\s+/g, '-');
    console.log(`\n=== Starting UK: ${niche} ===`);

    for (const city of cities) {
      try {
        for await (const lead of scraper.search(niche, { city, maxPages: 1, niche })) {
          if (lead._cityProgress || lead._captcha) continue;
          leads.push(lead);
          count++;
          if (lead.phone) withPhone++;
          if (lead.website) withWebsite++;
        }
      } catch (err) {
        console.error(`  ERROR ${niche} ${city}: ${err.message}`);
      }
    }

    console.log(`UK ${niche}: ${count} leads | ${withPhone} phones | ${withWebsite} websites`);
    summary.push({ niche, count, withPhone, withWebsite });

    if (leads.length > 0) {
      const h = ['firm_name','city','state','zip','phone','email','website','address','profile_url','practice_area','source'];
      const rows = leads.map(l => h.map(k => {
        const v = (l[k] || '').toString().replace(/"/g, '""');
        return v.includes(',') || v.includes('"') || v.includes('\n') ? '"' + v + '"' : v;
      }).join(','));
      fs.writeFileSync('output/gmaps-' + slug + '.csv', [h.join(','), ...rows].join('\n'));
      console.log(`Written: output/gmaps-${slug}.csv`);
    }
  }

  console.log('\n=== UK INDUSTRIES SUMMARY ===');
  let total = 0;
  for (const s of summary) {
    console.log(`  ${s.niche}: ${s.count} leads | ${s.withPhone} phones | ${s.withWebsite} websites`);
    total += s.count;
  }
  console.log(`  TOTAL: ${total} leads`);
  console.log('=== DONE ===');
})();
