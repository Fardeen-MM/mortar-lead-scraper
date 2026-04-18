#!/usr/bin/env node
/**
 * SEC IAPD (Investment Adviser Public Disclosure) Firm Scraper
 * URL: https://api.adviserinfo.sec.gov/search/firm
 * Target: ~15K RIA firms
 *
 * Strategy:
 *  - Wildcard single-letter queries a-z + 0-9 (36 queries)
 *  - Paginate each via start offset (max start=10000 typically)
 *  - Dedup by firm_source_id as we stream
 *  - Resume via state file
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'sec-iapd-firms.csv');
const STATE_FILE = path.join(OUT_DIR, '.sec-iapd.state.json');

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const QUERIES = 'abcdefghijklmnopqrstuvwxyz0123456789'.split('');
const PAGE_SIZE = 100; // max server often honors — try 100 with nrows=100
const SORT = 'score+desc';

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (_) { /* ignore */ }
  }
  return { seen: {}, done: {}, queryIdx: 0, start: 0 };
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function httpGetJson(url, tries = 4) {
  return new Promise((resolve, reject) => {
    const attempt = (n) => {
      const opts = {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
        }
      };
      const req = https.get(url, opts, (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (c) => body += c);
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            try { resolve(JSON.parse(body)); }
            catch (e) { reject(new Error(`Parse error: ${e.message}`)); }
          } else if (res.statusCode === 429 || res.statusCode >= 500) {
            if (n < tries) {
              setTimeout(() => attempt(n + 1), 1500 * n);
            } else {
              reject(new Error(`HTTP ${res.statusCode}`));
            }
          } else {
            reject(new Error(`HTTP ${res.statusCode}: ${body.substring(0, 200)}`));
          }
        });
      });
      req.on('error', (e) => {
        if (n < tries) setTimeout(() => attempt(n + 1), 1500 * n);
        else reject(e);
      });
      req.setTimeout(30000, () => {
        req.destroy(new Error('timeout'));
      });
    };
    attempt(1);
  });
}

function toCsvField(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parseAddress(addressJson) {
  if (!addressJson) return { city: '', state: '', country: '' };
  try {
    const a = typeof addressJson === 'string' ? JSON.parse(addressJson) : addressJson;
    const o = a.officeAddress || a || {};
    return {
      city: o.city || '',
      state: o.state || '',
      country: o.country || ''
    };
  } catch (_) {
    return { city: '', state: '', country: '' };
  }
}

function hitToRow(hit) {
  const src = hit._source || {};
  const addr = parseAddress(src.firm_ia_address_details || src.firm_bd_address_details);
  const fid = src.firm_source_id || '';
  const sec = src.firm_full_sec_number || src.firm_ia_full_sec_number || src.firm_bd_full_sec_number || '';
  const isIa = !!(src.firm_ia_scope || src.firm_ia_full_sec_number || src.firm_ia_disclosure_fl);
  const isBd = !!(src.firm_bd_scope || src.firm_bd_full_sec_number || src.firm_scope);
  const type = isIa && isBd ? 'IA+BD' : (isIa ? 'IA' : (isBd ? 'BD' : ''));
  return {
    firm_name: src.firm_name || '',
    firm_id: fid,
    sec_number: sec,
    city: addr.city,
    state: addr.state,
    country: addr.country,
    firm_type: type,
    niche: 'financial-advisor',
    source: 'sec-iapd'
  };
}

async function main() {
  const state = loadState();
  const seen = state.seen;

  // Write header only if file doesn't exist
  const header = 'firm_name,firm_id,sec_number,city,state,country,firm_type,niche,source\n';
  if (!fs.existsSync(OUT_FILE)) {
    fs.writeFileSync(OUT_FILE, header);
  }
  const out = fs.createWriteStream(OUT_FILE, { flags: 'a' });

  // Allow CLI flags: --test to just do first 2 queries 2 pages; --max-pages N
  const args = process.argv.slice(2);
  const TEST = args.includes('--test');
  const MAX_PAGES_ARG = args.find(a => a.startsWith('--max-pages='));
  const MAX_PAGES = MAX_PAGES_ARG ? parseInt(MAX_PAGES_ARG.split('=')[1], 10) : (TEST ? 2 : 120); // 120*100 = 12K cap per query

  let totalNew = 0;
  const startIdx = state.queryIdx || 0;

  for (let qi = startIdx; qi < QUERIES.length; qi++) {
    const q = QUERIES[qi];
    if (state.done[q]) continue;
    if (TEST && qi >= 2) break; // test: first 2 queries only
    let start = (qi === startIdx) ? (state.start || 0) : 0;
    let pagesForQ = 0;
    let queryNew = 0;
    let queryTotal = 0;

    while (pagesForQ < MAX_PAGES) {
      const url = `https://api.adviserinfo.sec.gov/search/firm?query=${encodeURIComponent(q)}&hl=true&nrows=${PAGE_SIZE}&start=${start}&sort=${SORT}&includePrevious=false`;
      let json;
      try {
        json = await httpGetJson(url);
      } catch (e) {
        console.error(`[${q}] start=${start} ERR ${e.message}`);
        break;
      }
      const hits = (json && json.hits && Array.isArray(json.hits.hits)) ? json.hits.hits : [];
      queryTotal = (json && json.hits && json.hits.total) || queryTotal;
      if (!hits.length) break;

      let pageNew = 0;
      for (const h of hits) {
        const row = hitToRow(h);
        const key = row.firm_id || `${row.firm_name}|${row.city}|${row.state}`;
        if (!key || seen[key]) continue;
        seen[key] = 1;
        pageNew++;
        totalNew++;
        queryNew++;
        const line = [
          row.firm_name, row.firm_id, row.sec_number, row.city, row.state,
          row.country, row.firm_type, row.niche, row.source
        ].map(toCsvField).join(',') + '\n';
        out.write(line);
      }

      process.stdout.write(`[${q}] start=${start} hits=${hits.length} new=${pageNew} total_in_search=${queryTotal}\n`);
      start += PAGE_SIZE;
      pagesForQ++;

      // Save state every 5 pages
      if (pagesForQ % 5 === 0) {
        state.queryIdx = qi;
        state.start = start;
        saveState(state);
      }

      // If all hits were dupes, skip forward faster (but don't break — still more unique later)
      if (pageNew === 0 && hits.length < PAGE_SIZE) break;

      // Rate limit
      await new Promise(r => setTimeout(r, 250));
    }

    state.done[q] = { total: queryTotal, new: queryNew };
    state.queryIdx = qi + 1;
    state.start = 0;
    saveState(state);
    console.log(`[${q}] DONE. Added ${queryNew} new firms (search total=${queryTotal}). Overall new so far: ${totalNew}`);
  }

  out.end();
  saveState(state);
  console.log(`\nDONE. Total new firms added: ${totalNew}. CSV: ${OUT_FILE}`);
}

main().catch(e => { console.error(e); process.exit(1); });
