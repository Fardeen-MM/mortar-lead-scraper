#!/usr/bin/env node
/**
 * Best Lawyers US Immigration — sweep per-state URLs for MUCH higher yield.
 * CA alone has 164 immigration lawyers (vs 57 total national aggregate page).
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const STATES = [
  'alabama','alaska','arizona','arkansas','california','colorado','connecticut',
  'delaware','district-of-columbia','florida','georgia','hawaii','idaho','illinois',
  'indiana','iowa','kansas','kentucky','louisiana','maine','maryland','massachusetts',
  'michigan','minnesota','mississippi','missouri','montana','nebraska','nevada',
  'new-hampshire','new-jersey','new-mexico','new-york','north-carolina','north-dakota',
  'ohio','oklahoma','oregon','pennsylvania','rhode-island','south-carolina','south-dakota',
  'tennessee','texas','utah','vermont','virginia','washington','west-virginia',
  'wisconsin','wyoming',
];

const COUNTRIES = [
  { slug: 'united-kingdom', country: 'UK' },
  { slug: 'canada', country: 'Canada' },
  { slug: 'australia', country: 'Australia' },
];

const OUTPUT = path.join(__dirname, '..', 'output', 'best-lawyers-immigration-global.csv');
const DELAY_MS = 3000;
const MAX_PAGES = 8;

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 20000,
    }, (res) => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parsePage(html, country, state) {
  const $ = cheerio.load(html);
  const results = [];
  $('.card-lawyer').each((_, el) => {
    const $el = $(el);
    const mailto = $el.find('a[href^="mailto:"]').first().attr('href') || '';
    const email = mailto.replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
    const phone = ($el.find('a[href^="tel:"]').first().attr('href') || '').replace(/^tel:/i, '').trim();
    const name = ($el.find('h3, h4').first().text().trim()) ||
                 ($el.find('.lawyer-name').first().text().trim());
    const firm = ($el.find('[class*="firm"]').first().text().trim()) ||
                 ($el.find('h5').first().text().trim());
    const location = $el.find('[class*="location"], [class*="city"]').first().text().trim();
    const profile = $el.find('a[href*="/lawyers/"]').first().attr('href') || '';
    if (email || name) {
      const parts = name.split(/\s+/).filter(Boolean);
      results.push({
        first_name: parts[0] || '',
        last_name: parts.slice(1).join(' '),
        email, phone,
        firm_name: firm,
        city: location,
        state,
        country,
        profile_url: profile ? (profile.startsWith('http') ? profile : 'https://www.bestlawyers.com' + profile) : '',
        niche: 'immigration attorney',
        source: 'best_lawyers',
      });
    }
  });
  return results;
}

async function scrapeRegion(slug, country, stateName) {
  const rows = [];
  const seen = new Set();
  let empty = 0;
  for (let page = 1; page <= MAX_PAGES; page++) {
    try {
      const html = await httpGet(`https://www.bestlawyers.com/${slug}/immigration-law?page=${page}`);
      const pageRows = parsePage(html, country, stateName);
      let newCount = 0;
      for (const r of pageRows) {
        const key = r.email || `${r.first_name}|${r.last_name}|${r.firm_name}`;
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push(r);
        newCount++;
      }
      if (pageRows.length === 0) { empty++; if (empty >= 2) break; } else { empty = 0; }
    } catch (err) {
      console.error(`    [${stateName}] page ${page}: ${err.message}`);
      break;
    }
    await new Promise(r => setTimeout(r, DELAY_MS));
  }
  return rows;
}

async function main() {
  console.log('=== Best Lawyers Immigration — 51 states + UK/CA/AU ===');
  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state',
                'profile_url', 'country', 'niche', 'source'];
  fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  let total = 0, totalEmails = 0;
  const globalSeen = new Set();

  for (const state of STATES) {
    const stateName = state.replace(/-/g, ' ').toUpperCase();
    console.log(`\n[US ${state}]`);
    const rows = await scrapeRegion(`united-states/${state}`, 'US', state);
    let added = 0;
    for (const r of rows) {
      const key = r.email || `${r.first_name}|${r.last_name}|${r.firm_name}`;
      if (globalSeen.has(key)) continue;
      globalSeen.add(key);
      if (r.email) totalEmails++;
      fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(r[c] || '')).join(',') + '\n');
      added++; total++;
    }
    console.log(`  ${rows.length} rows, ${added} new. Total: ${total}, emails: ${totalEmails}`);
  }

  for (const { slug, country } of COUNTRIES) {
    console.log(`\n[${country}]`);
    const rows = await scrapeRegion(slug, country, '');
    let added = 0;
    for (const r of rows) {
      const key = r.email || `${r.first_name}|${r.last_name}|${r.firm_name}`;
      if (globalSeen.has(key)) continue;
      globalSeen.add(key);
      if (r.email) totalEmails++;
      fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(r[c] || '')).join(',') + '\n');
      added++; total++;
    }
    console.log(`  ${rows.length} rows, ${added} new. Total: ${total}, emails: ${totalEmails}`);
  }

  console.log(`\n=== DONE — ${total} rows, ${totalEmails} unique emails ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
