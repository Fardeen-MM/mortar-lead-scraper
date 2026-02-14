#!/usr/bin/env node
const { getRegistry } = require('../lib/registry');
const registry = getRegistry();

async function testOne(code) {
  const start = Date.now();
  try {
    const scraper = registry[code]();
    const city = scraper.defaultCities?.[0] || null;
    let count = 0;
    let firstLead = null;
    for await (const item of scraper.search(null, { maxPages: 1, city })) {
      if (item._cityProgress || item._captcha) continue;
      count++;
      if (firstLead === null) firstLead = item;
      if (count >= 3) break;
    }
    const ms = Date.now() - start;
    console.log(`[${code}] ${count} leads in ${ms}ms`);
    if (firstLead) {
      console.log(`  First: ${firstLead.first_name || ''} ${firstLead.last_name || ''} | ${firstLead.city || ''} | ${firstLead.firm_name || ''}`);
    }
  } catch(e) {
    console.log(`[${code}] ERROR: ${e.message}`);
  }
}

(async () => {
  const codes = process.argv.slice(2);
  if (codes.length === 0) {
    codes.push('FR', 'AU-VIC', 'AU-WA');
  }
  for (const code of codes) {
    await testOne(code);
  }
})();
