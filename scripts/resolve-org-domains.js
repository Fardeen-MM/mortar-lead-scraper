#!/usr/bin/env node
/**
 * Resolve org names → domains via DNS guessing (free, no API).
 *
 * For each row with organization name but no domain, generates common domain
 * patterns (.org first for nonprofits, .com, .net) and checks which ones
 * actually resolve in DNS + have MX records.
 *
 * Purpose: enrich EOIR / CICC rows that have firm_name but no domain.
 * Then downstream crawl-websites-for-emails can find their emails.
 *
 * Usage:
 *   node scripts/resolve-org-domains.js --in output/eoir-immigration-reps.csv --out output/eoir-with-domains.csv
 */

const fs = require('fs');
const path = require('path');
const dns = require('dns');

// Parse args
const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const s = a.replace(/^--/, '');
  if (s.includes('=')) { const [k, v] = s.split('='); args[k] = v; }
  else { const n = argv[i + 1]; if (n && !n.startsWith('--')) { args[s] = n; i++; } else args[s] = true; }
}

const INPUT_FILE = args.in;
const OUTPUT_FILE = args.out || (INPUT_FILE ? INPUT_FILE.replace(/\.csv$/, '-with-domains.csv') : null);
const CONCURRENCY = parseInt(args.concurrency) || 20;
const FIRM_COL = args['firm-col'] || 'organization';
const LIMIT = parseInt(args.limit) || 0;

if (!INPUT_FILE) {
  console.error('Usage: node scripts/resolve-org-domains.js --in input.csv [--firm-col organization]');
  process.exit(1);
}

// TLD priority for nonprofits / businesses
const TLDS = ['.org', '.com', '.net', '.edu', '.us', '.ca', '.co.uk', '.org.uk'];

// Strip legal/corp suffixes
const SUFFIXES = [
  'inc', 'inc.', 'llc', 'ltd', 'ltd.', 'corp', 'corp.', 'co', 'co.',
  'pllc', 'pc', 'pa', 'p.a.', 'p.c.', 'llp', 'l.l.p.',
  'foundation', 'organization', 'nonprofit', 'non-profit',
  'church', 'churches', 'ministries', 'ministry', 'diocese', 'parish',
  'services', 'service', 'association', 'society',
];

function cleanOrg(name) {
  if (!name) return '';
  let s = name.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')  // strip accents
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // Strip trailing legal suffix
  for (const suf of SUFFIXES) {
    if (s.endsWith(' ' + suf)) s = s.slice(0, -(suf.length + 1)).trim();
  }
  return s;
}

function generateDomains(org) {
  const cleaned = cleanOrg(org);
  if (!cleaned) return [];

  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length === 0) return [];

  const domains = new Set();

  // Full concatenation
  const full = words.join('');
  if (full.length >= 4 && full.length <= 40) {
    for (const tld of TLDS) domains.add(full + tld);
  }

  // First 2 words concatenated (very common for 3+ word orgs)
  if (words.length >= 2) {
    const first2 = words.slice(0, 2).join('');
    if (first2.length >= 4 && first2.length <= 30) {
      for (const tld of TLDS.slice(0, 3)) domains.add(first2 + tld);
    }
  }

  // Initials + common suffix
  if (words.length >= 2) {
    const initials = words.map(w => w[0]).join('');
    if (initials.length >= 2 && initials.length <= 6) {
      domains.add(initials + '.org');
    }
  }

  // Single-word orgs
  if (words.length === 1 && words[0].length >= 4) {
    for (const tld of TLDS) domains.add(words[0] + tld);
  }

  return [...domains].slice(0, 12); // cap per org
}

function hasMx(domain) {
  return new Promise(resolve => {
    const timer = setTimeout(() => resolve(false), 4000);
    dns.resolveMx(domain, (err, records) => {
      clearTimeout(timer);
      resolve(!err && records && records.length > 0);
    });
  });
}

async function resolveOrgDomain(orgName) {
  const candidates = generateDomains(orgName);
  if (!candidates.length) return '';

  // Test each in parallel
  const results = await Promise.all(candidates.map(async (domain) => {
    const ok = await hasMx(domain);
    return ok ? domain : null;
  }));

  return results.find(d => d) || '';
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else {
        if (c === ',') { out.push(cur); cur = ''; }
        else if (c === '"' && cur === '') inQ = true;
        else cur += c;
      }
    }
    out.push(cur);
    return out;
  }
  if (!lines.length) return { headers: [], rows: [] };
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {};
    headers.forEach((h, idx) => { rec[h] = cols[idx] || ''; });
    rows.push(rec);
  }
  return { headers, rows };
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(headers, rows) {
  const out = [headers.join(',')];
  for (const r of rows) out.push(headers.map(h => escapeCsv(r[h] || '')).join(','));
  fs.writeFileSync(OUTPUT_FILE, out.join('\n') + '\n');
}

async function main() {
  console.log(`=== Org Domain Resolver ===`);
  console.log(`Input:       ${INPUT_FILE}`);
  console.log(`Output:      ${OUTPUT_FILE}`);
  console.log(`Firm column: ${FIRM_COL}`);
  console.log(`Concurrency: ${CONCURRENCY}`);

  const text = fs.readFileSync(INPUT_FILE, 'utf-8');
  const { headers, rows } = parseCsv(text);
  let workingRows = LIMIT > 0 ? rows.slice(0, LIMIT) : rows;

  // Only process rows with firm/org but no domain
  const needDomain = workingRows.filter(r => {
    const orgValue = r[FIRM_COL] || r.firm_name || r.organization || '';
    return orgValue && !r.domain;
  });
  console.log(`Rows needing domain: ${needDomain.length} of ${workingRows.length}`);

  // Dedup by org name (so we only query each unique org once)
  const orgToDomain = new Map();
  const uniqueOrgs = new Set(needDomain.map(r => r[FIRM_COL] || r.firm_name || r.organization || ''));
  console.log(`Unique orgs: ${uniqueOrgs.size}`);

  const orgList = [...uniqueOrgs];
  let done = 0, found = 0;
  const start = Date.now();

  const workers = [];
  const queue = [...orgList];

  async function worker() {
    while (queue.length) {
      const org = queue.shift();
      if (!org) return;
      const domain = await resolveOrgDomain(org);
      orgToDomain.set(org, domain);
      done++;
      if (domain) found++;
      if (done % 50 === 0 || done === orgList.length) {
        const elapsed = (Date.now() - start) / 1000;
        const rate = (done / elapsed).toFixed(1);
        const eta = Math.round((orgList.length - done) / (parseFloat(rate) || 1));
        console.log(`  [${(done/orgList.length*100).toFixed(1)}%] ${done}/${orgList.length}, found=${found}, ${rate}/s, ETA ${eta}s`);
      }
    }
  }

  for (let i = 0; i < CONCURRENCY; i++) workers.push(worker());
  await Promise.all(workers);

  // Apply resolved domains to rows
  let applied = 0;
  for (const r of workingRows) {
    const org = r[FIRM_COL] || r.firm_name || r.organization || '';
    if (org && !r.domain && orgToDomain.has(org)) {
      const d = orgToDomain.get(org);
      if (d) {
        r.domain = d;
        r.domain_source = 'dns_guess';
        applied++;
      }
    }
  }

  const newHeaders = [...headers];
  if (!newHeaders.includes('domain')) newHeaders.push('domain');
  if (!newHeaders.includes('domain_source')) newHeaders.push('domain_source');

  writeCsv(newHeaders, workingRows);

  console.log(`\n=== DONE ===`);
  console.log(`Unique orgs tried:   ${orgList.length}`);
  console.log(`Orgs with domain:    ${found}`);
  console.log(`Rows enriched:       ${applied}`);
  console.log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
