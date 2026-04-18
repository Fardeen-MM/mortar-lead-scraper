/**
 * BBB Dentist Retry — Re-scrape the cities that failed due to DNS errors
 */

const fs = require('fs');
const path = require('path');
const reg = require('../lib/registry').getRegistry();
const bbb = reg['BBB']();
const { RateLimiter } = require('../lib/rate-limiter');

const CITIES = [
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

  for (const cityInput of CITIES) {
    const rateLimiter = new RateLimiter();
    const parts = cityInput.split(',').map(s => s.trim());
    const city = parts[0];
    const state = parts[1].toUpperCase();
    const citySlug = city.toLowerCase().replace(/\./g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

    let page = 1;
    let pagesFetched = 0;
    let consecutiveEmpty = 0;
    let cityCount = 0;

    while (pagesFetched < MAX_PAGES) {
      const url = `https://www.bbb.org/us/${state.toLowerCase()}/${citySlug}/category/dentist` + (page > 1 ? `?page=${page}` : '');

      let response;
      try {
        await rateLimiter.wait();
        response = await bbb.httpGet(url, rateLimiter);
      } catch (err) {
        console.log(`  Error: ${err.message.slice(0, 60)}`);
        // Retry once on DNS failure
        try {
          await new Promise(r => setTimeout(r, 5000));
          response = await bbb.httpGet(url, rateLimiter);
        } catch (err2) {
          console.log(`  Retry failed: ${err2.message.slice(0, 60)}`);
          break;
        }
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        const retry = await rateLimiter.handleBlock(response.statusCode);
        if (retry) continue;
        break;
      }
      if (response.statusCode === 404) { console.log(`  404 for ${city}`); break; }
      if (response.statusCode !== 200) { console.log(`  HTTP ${response.statusCode}`); break; }

      rateLimiter.resetBackoff();

      if (bbb.detectCaptcha(response.body)) {
        console.log(`  CAPTCHA — skipping ${city}`);
        break;
      }

      const { results, totalCount } = bbb._parseListingPage(response.body);

      if (page === 1 && totalCount > 0) {
        console.log(`  Dentist in ${city}, ${state}: ${totalCount} total results`);
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
        allLeads.push({ ...result, trade_category: 'Dentist', source: 'bbb_medical' });
        cityCount++;
      }

      if (results.length < 13) break;
      page++;
      pagesFetched++;
    }

    console.log(`Dentist | ${city}, ${state}: +${cityCount} (${allLeads.length} total)`);
  }

  // Append to existing CSV (skip header)
  const outputPath = path.join(__dirname, '..', 'output', 'bbb-medical.csv');
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

  if (allLeads.length > 0) {
    fs.appendFileSync(outputPath, '\n' + rows);
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n=== DENTIST RETRY COMPLETE ===`);
  console.log(`New leads: ${allLeads.length}`);
  console.log(`Time: ${elapsed} minutes`);
  console.log(`Appended to: ${outputPath}`);

})().catch(e => {
  console.error('FATAL:', e.stack);
  process.exit(1);
});
