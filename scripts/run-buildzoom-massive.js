#!/usr/bin/env node
/**
 * BuildZoom Massive Scrape — 7 trades x 15 cities x 5 pages
 * Run: node scripts/run-buildzoom-massive.js
 */
const reg = require('../lib/registry').getRegistry();
const fs = require('fs');
const { normalizeTrade } = require('../lib/normalizer');

(async () => {
  const bz = reg['BUILDZOOM']();
  const bzLeads = [];
  const trades = ['roofing', 'painting', 'plumbing', 'hvac', 'electrical', 'general contractor', 'landscaping'];
  const cities = ['Miami, FL', 'Houston, TX', 'Dallas, TX', 'Atlanta, GA', 'Phoenix, AZ', 'Tampa, FL', 'Orlando, FL', 'Charlotte, NC', 'Denver, CO', 'Nashville, TN', 'San Antonio, TX', 'Jacksonville, FL', 'Austin, TX', 'Las Vegas, NV', 'Raleigh, NC'];

  const startTime = Date.now();

  for (const trade of trades) {
    for (const city of cities) {
      try {
        for await (const lead of bz.search(trade, { maxPages: 5, city })) {
          if (lead._cityProgress || lead._captcha) continue;
          bzLeads.push({...lead, trade_category: normalizeTrade(trade)});
        }
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`[${elapsed}s] ${trade} | ${city}: ${bzLeads.length} total leads`);
      } catch(e) {
        console.log('BuildZoom error: ' + e.message.slice(0, 80));
      }
    }
  }

  const headers = 'first_name,last_name,firm_name,email,phone,website,city,state,trade_category,bar_status,profile_url,source\n';
  const rows = bzLeads.map(l => [
    l.first_name||'', l.last_name||'', l.firm_name||'', l.email||'', l.phone||'',
    l.website||'', l.city||'', l.state||'', l.trade_category||'', l.bar_status||'',
    l.profile_url||'', l.source||'buildzoom_contractors'
  ].map(v => {
    const s = (v||'').toString().replace(/"/g, '""');
    return s.includes(',') ? '"' + s + '"' : s;
  }).join(',')).join('\n');

  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/buildzoom-massive.csv', headers + rows);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  console.log(`\n=== BUILDZOOM COMPLETE in ${elapsed}s ===`);
  console.log(`TOTAL: ${bzLeads.length} leads`);
  console.log(`Phone: ${bzLeads.filter(l=>l.phone).length}`);
  console.log(`Website: ${bzLeads.filter(l=>l.website).length}`);
  console.log(`Email: ${bzLeads.filter(l=>l.email).length}`);
  console.log(`Profile URL: ${bzLeads.filter(l=>l.profile_url).length}`);
  console.log(`Output: output/buildzoom-massive.csv`);
})().catch(e => console.error(e.stack));
