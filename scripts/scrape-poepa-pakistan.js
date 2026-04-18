#!/usr/bin/env node
/**
 * POEPA Pakistan — paginate offset=0,20,40...
 * 21 rows per request. ~3,340 total members (offset max ~3320).
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'poepa-pakistan-consultants.csv');
const MAX_OFFSET = 4000;
const STEP = 20;
const DELAY_MS = 1500;

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 20000, headers: { 'User-Agent': 'Mozilla/5.0' } },
      res => {
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

function parsePage(html) {
  const $ = cheerio.load(html);
  const rows = [];
  $('table tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 4) return;
    const cells = tds.map((_, td) => $(td).text().trim()).get();
    // Extract phones (tel: links)
    const phones = $(tr).find('a[href^="tel:"]').map((_, a) => $(a).attr('href').replace('tel:', '').trim()).get();
    rows.push({
      license: cells[0] || '',
      proprietor: cells[1] || '',
      title: cells[2] || '',
      membership: cells[3] || '',
      status: cells[4] || '',
      expiry: cells[5] || '',
      address: cells[6] || '',
      phones: phones.join(';'),
      niche: 'overseas employment promoter',
      country: 'Pakistan',
      source: 'poepa',
    });
  });
  return rows;
}

async function main() {
  const cols = ['license', 'proprietor', 'title', 'membership', 'status', 'expiry', 'address', 'phones', 'niche', 'country', 'source'];
  fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  const seen = new Set();
  let total = 0;
  let emptyStreak = 0;

  for (let offset = 0; offset <= MAX_OFFSET; offset += STEP) {
    try {
      const url = `https://poepamembers.poepa.com.pk/memberships/index?per_page=${offset}`;
      const html = await httpGet(url);
      const rows = parsePage(html);
      let added = 0;
      for (const r of rows) {
        const key = r.license || r.membership || r.proprietor;
        if (!key || seen.has(key)) continue;
        seen.add(key);
        fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(r[c] || '')).join(',') + '\n');
        added++;
        total++;
      }
      console.log(`  offset=${offset}: ${rows.length} rows (${added} new). Total: ${total}`);
      if (rows.length === 0 || added === 0) {
        emptyStreak++;
        if (emptyStreak >= 3) { console.log('  3 empty streaks — stopping'); break; }
      } else { emptyStreak = 0; }
    } catch (err) {
      console.error(`  offset=${offset}: ${err.message}`);
    }
    await new Promise(r => setTimeout(r, DELAY_MS));
  }

  console.log(`\n=== DONE — ${total} POEPA members ===`);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
