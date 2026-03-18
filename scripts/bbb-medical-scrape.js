/**
 * BBB Medical Category Scraper
 *
 * Scrapes medical/health businesses from BBB using confirmed category slugs.
 * Uses the BBB scraper's internal methods directly, bypassing the CATEGORY_SLUGS mapping.
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the BBB scraper instance
const reg = require('../lib/registry').getRegistry();
const bbb = reg['BBB']();

// Confirmed working BBB medical category slugs
const MEDICAL_CATEGORIES = {
  'Dentist': 'dentist',
  'Chiropractors': 'chiropractors-dc',
  'Dermatologist': 'dermatologist',
  'Optometrist': 'optometrist',
  'Medical Spa': 'medical-spa',
  'Plastic Surgery': 'plastic-surgery',
};

const CITIES = [
  'Miami, FL',
  'Houston, TX',
  'Dallas, TX',
  'Atlanta, GA',
  'Phoenix, AZ',
  'Los Angeles, CA',
  'Chicago, IL',
  'Denver, CO',
  'Las Vegas, NV',
  'San Diego, CA',
];

const MAX_PAGES = 3;
const allLeads = [];
const seenUrls = new Set();

(async () => {
  const startTime = Date.now();

  for (const [catName, catSlug] of Object.entries(MEDICAL_CATEGORIES)) {
    for (const cityInput of CITIES) {
      try {
        // We need to directly use the BBB scraper but override the slug resolution.
        // The search() method resolves practiceArea -> slug via CATEGORY_SLUGS.
        // Since medical categories aren't in that map, we'll temporarily patch it.

        // Access the internal _buildListingUrl and _parseListingPage methods
        const { RateLimiter } = require('../lib/rate-limiter');
        const { log } = require('../lib/logger');
        const rateLimiter = new RateLimiter();

        const parts = cityInput.split(',').map(s => s.trim());
        const city = parts[0];
        const state = parts[1].toUpperCase();
        const citySlug = city.toLowerCase().replace(/\./g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

        let pagesFetched = 0;
        let page = 1;
        let consecutiveEmpty = 0;
        let cityCount = 0;

        while (pagesFetched < MAX_PAGES) {
          const url = `https://www.bbb.org/us/${state.toLowerCase()}/${citySlug}/category/${catSlug}` + (page > 1 ? `?page=${page}` : '');

          let response;
          try {
            await rateLimiter.wait();
            response = await bbb.httpGet(url, rateLimiter);
          } catch (err) {
            console.log(`  Request error: ${err.message.slice(0, 60)}`);
            break;
          }

          if (response.statusCode === 429 || response.statusCode === 403) {
            console.log(`  Got ${response.statusCode} — backing off`);
            const retry = await rateLimiter.handleBlock(response.statusCode);
            if (retry) continue;
            break;
          }

          if (response.statusCode === 404) {
            console.log(`  404 — no ${catName} in ${city}, ${state}`);
            break;
          }

          if (response.statusCode !== 200) {
            console.log(`  HTTP ${response.statusCode} — skipping`);
            break;
          }

          rateLimiter.resetBackoff();

          if (bbb.detectCaptcha(response.body)) {
            console.log(`  CAPTCHA detected — skipping ${city}`);
            break;
          }

          // Use the BBB scraper's internal parser
          const { results, totalCount } = bbb._parseListingPage(response.body);

          if (page === 1 && totalCount > 0) {
            console.log(`  ${catName} in ${city}, ${state}: ${totalCount} total results`);
          }

          if (results.length === 0) {
            consecutiveEmpty++;
            if (consecutiveEmpty >= 2 || page === 1) break;
            page++;
            pagesFetched++;
            continue;
          }

          consecutiveEmpty = 0;

          for (const result of results) {
            if (result.profile_url && seenUrls.has(result.profile_url)) continue;
            if (result.profile_url) seenUrls.add(result.profile_url);

            allLeads.push({
              ...result,
              trade_category: catName,
              source: 'bbb_medical',
            });
            cityCount++;
          }

          if (results.length < 13) break; // Less than ~15 per page means last page

          page++;
          pagesFetched++;
        }

        console.log(`${catName} | ${city}, ${state}: +${cityCount} leads (${allLeads.length} total)`);

      } catch (e) {
        console.log(`Error ${catName} ${cityInput}: ${e.message.slice(0, 80)}`);
      }
    }
  }

  // Write CSV
  const outputPath = path.join(__dirname, '..', 'output', 'bbb-medical.csv');
  const headers = 'first_name,last_name,firm_name,email,phone,website,city,state,zip,address,trade_category,bar_status,profile_url,source\n';
  const rows = allLeads.map(l => {
    return [
      l.first_name || '', l.last_name || '', l.firm_name || '', l.email || '',
      l.phone || '', l.website || '', l.city || '', l.state || '', l.zip || '',
      l.address || '', l.trade_category || '', l.bar_status || '',
      l.profile_url || '', l.source || 'bbb_medical'
    ].map(v => {
      const s = (v || '').toString().replace(/"/g, '""');
      return s.includes(',') ? '"' + s + '"' : s;
    }).join(',');
  }).join('\n');

  fs.writeFileSync(outputPath, headers + rows);

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n=== BBB MEDICAL SCRAPE COMPLETE ===');
  console.log(`Total leads: ${allLeads.length}`);
  console.log(`Unique businesses: ${seenUrls.size}`);
  console.log(`Time: ${elapsed} minutes`);
  console.log(`Output: ${outputPath}`);

  // Category breakdown
  const byCat = {};
  for (const l of allLeads) {
    byCat[l.trade_category] = (byCat[l.trade_category] || 0) + 1;
  }
  console.log('\nBreakdown by category:');
  for (const [cat, count] of Object.entries(byCat).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${cat}: ${count}`);
  }

})().catch(e => {
  console.error('FATAL:', e.stack);
  process.exit(1);
});
