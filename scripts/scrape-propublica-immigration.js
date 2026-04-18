#!/usr/bin/env node
/**
 * ProPublica Nonprofit Explorer — immigration-focused nonprofits.
 *
 * API: https://projects.propublica.org/nonprofits/api/v2/search.json?q=X&page=N
 * Free, no auth, 25 results/page, pagination supported.
 *
 * Yields: US 501(c) nonprofits with "immigration" in name or NTEE code.
 * Typical: 500-2000 organizations across keywords.
 * Output has no email/website directly — pipe through resolve-org-domains +
 * crawl-websites-for-emails for full enrichment.
 *
 * Usage:
 *   node scripts/scrape-propublica-immigration.js
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'propublica-immigration-nonprofits.csv');

// Immigration-focused search keywords
const KEYWORDS = [
  'immigration', 'immigrant', 'refugee', 'asylum', 'migrant',
  'naturalization', 'citizenship', 'visa', 'deportation',
  'DACA', 'sanctuary', 'border',
];

// NTEE codes relevant to immigration:
//   P40 Family Services · P84 Ethnic/Immigrant Centers · P85 Homeless ·
//   P99 Human Services Other · R26 Civil Rights · R30 Intergroup/Race Relations
//   I80 Crime/Legal-related Service · Q71 International Migration
const NTEE_CODES = ['P84', 'R26', 'R30', 'I80', 'Q71', 'P40'];

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
      },
      timeout: 30000,
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (err) { reject(new Error(`parse fail: ${err.message} — body: ${body.slice(0, 100)}`)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function searchKeyword(q) {
  const results = new Map();
  let page = 0;
  const maxPages = 50; // Safety cap — 25 per page × 50 = 1250 per keyword

  while (page < maxPages) {
    const url = `https://projects.propublica.org/nonprofits/api/v2/search.json?q=${encodeURIComponent(q)}&page=${page}`;
    try {
      const j = await httpGetJson(url);
      const orgs = j.organizations || [];
      if (orgs.length === 0) break;

      for (const o of orgs) {
        if (!results.has(o.ein)) results.set(o.ein, o);
      }

      const total = j.total_results || 0;
      const got = (page + 1) * 25;
      if (got >= total) break;
      page++;
      await sleep(500);
    } catch (err) {
      console.error(`  ERROR page ${page}: ${err.message}`);
      await sleep(2000);
      if (page > 0) break; // Give up if we have some results
      return results;
    }
  }

  return results;
}

async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('=== ProPublica Immigration Nonprofits ===');
  const all = new Map();

  for (const kw of KEYWORDS) {
    console.log(`\n[KW] "${kw}"...`);
    const results = await searchKeyword(kw);
    let added = 0;
    for (const [ein, org] of results) {
      if (!all.has(ein)) { all.set(ein, { ...org, discovered_via: `q:${kw}` }); added++; }
    }
    console.log(`  ${results.size} results (${added} new), total unique=${all.size}`);
    await sleep(1000);
  }

  console.log(`\nTotal unique nonprofits: ${all.size}`);

  // Write CSV
  const cols = [
    'firm_name', 'ein', 'city', 'state', 'country', 'ntee_code',
    'subseccd', 'niche', 'source', 'discovered_via',
  ];
  const rows = [];
  for (const org of all.values()) {
    rows.push({
      firm_name: org.name || '',
      ein: org.strein || org.ein || '',
      city: org.city || '',
      state: org.state || '',
      country: 'US',
      ntee_code: org.ntee_code || '',
      subseccd: org.subseccd || '',
      niche: 'immigration nonprofit',
      source: 'propublica',
      discovered_via: org.discovered_via || '',
    });
  }

  const header = cols.join(',');
  const body = rows.map(r => cols.map(c => escapeCsv(r[c] || '')).join(',')).join('\n');
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + body + '\n');

  // Stats
  const byState = {};
  for (const r of rows) byState[r.state || '?'] = (byState[r.state || '?'] || 0) + 1;

  console.log(`\n=== RESULTS ===`);
  console.log(`Total: ${rows.length}`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log(`\nTop 15 states:`);
  for (const [s, n] of Object.entries(byState).sort((a, b) => b[1] - a[1]).slice(0, 15)) {
    console.log(`  ${s}: ${n}`);
  }

  console.log('\nNext steps:');
  console.log(`  node scripts/resolve-org-domains.js --in ${OUTPUT_FILE} --out output/propublica-with-domains.csv`);
  console.log(`  node scripts/crawl-websites-for-emails.js --in output/propublica-with-domains.csv --out output/propublica-crawled.csv`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
