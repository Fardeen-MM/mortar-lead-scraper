#!/usr/bin/env node
/**
 * Generic Email Enricher — for CSVs with `domain` but no names.
 *
 * For each domain, tries common "role" email prefixes (info, contact, hello,
 * admin, enquiries, office, support) and verifies each via the EmailWaterfall.
 * Outputs a new CSV with the first verified generic email per row.
 *
 * Use case: ICEF IAS agents (2,216 websites) — no names, but we can reach them
 * via info@domain.com if valid.
 *
 * Usage:
 *   node scripts/enrich-generic-emails.js --in output/icef-ias-global-agents.csv --out output/icef-enriched.csv
 *   node scripts/enrich-generic-emails.js --in input.csv --out output.csv --concurrency 8
 *
 * Concurrency: default 5. Higher = faster but more likely to hit rate limits.
 */

const fs = require('fs');
const path = require('path');
const { EmailWaterfall } = require('../lib/email-waterfall');

// Parse args (support both --key=val AND --key val)
const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const stripped = a.replace(/^--/, '');
  if (stripped.includes('=')) {
    const [k, v] = stripped.split('=');
    args[k] = v;
  } else {
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      args[stripped] = next;
      i++;
    } else {
      args[stripped] = true;
    }
  }
}

const INPUT_FILE = args.in;
const OUTPUT_FILE = args.out || (INPUT_FILE ? INPUT_FILE.replace(/\.csv$/, '-enriched.csv') : null);
const CONCURRENCY = parseInt(args.concurrency) || 5;
const LIMIT = parseInt(args.limit) || 0;

if (!INPUT_FILE) {
  console.error('Usage: node scripts/enrich-generic-emails.js --in input.csv [--out enriched.csv] [--concurrency 5]');
  process.exit(1);
}

// Common generic prefixes (ordered by likelihood of being valid)
const GENERIC_PREFIXES = [
  'info',
  'contact',
  'hello',
  'enquiries',
  'admin',
  'office',
  'support',
  'inquiries',
  'team',
  'mail',
];

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  if (!lines.length) return { headers: [], rows: [] };

  // Simple CSV parser — handles quoted fields with commas and embedded quotes
  function splitRow(line) {
    const out = [];
    let cur = '';
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') { inQ = false; }
        else { cur += c; }
      } else {
        if (c === ',') { out.push(cur); cur = ''; }
        else if (c === '"' && cur === '') { inQ = true; }
        else { cur += c; }
      }
    }
    out.push(cur);
    return out;
  }

  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const raw = lines[i];
    if (!raw.trim()) continue;
    const cols = splitRow(raw);
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
  for (const r of rows) {
    out.push(headers.map(h => escapeCsv(r[h] || '')).join(','));
  }
  fs.writeFileSync(OUTPUT_FILE, out.join('\n') + '\n');
}

async function tryDomain(waterfall, domain) {
  if (!domain) return { email: '', source: '', confidence: 0, status: 'no_domain' };

  for (const prefix of GENERIC_PREFIXES) {
    const candidate = `${prefix}@${domain}`;
    try {
      const v = await waterfall.verifyEmail(candidate);
      if (v && v.valid) {
        return {
          email: candidate,
          source: v.reason || 'generic_verify',
          confidence: v.confidence || 70,
          status: 'verified',
        };
      }
    } catch (err) {
      // skip
    }
  }
  return { email: '', source: '', confidence: 0, status: 'not_found' };
}

async function enrichBatch(waterfall, rows) {
  const results = [];
  let done = 0;
  let found = 0;
  const start = Date.now();

  async function processOne(row) {
    const domain = row.domain || row.Domain || '';
    const r = await tryDomain(waterfall, domain);
    Object.assign(row, {
      generic_email: r.email,
      email_confidence: r.confidence,
      email_source: r.source,
      email_status: r.status,
    });
    done++;
    if (r.email) found++;
    if (done % 25 === 0 || done === rows.length) {
      const pct = (done / rows.length * 100).toFixed(1);
      const rate = (done / ((Date.now() - start) / 1000)).toFixed(1);
      const eta = rows.length > done ? Math.round((rows.length - done) / rate) : 0;
      console.log(`  [${pct}%] ${done}/${rows.length} processed, ${found} emails found, ${rate}/s, ETA ${eta}s`);
    }
  }

  // Simple concurrency pool
  const pool = [];
  for (const row of rows) {
    const p = processOne(row).catch(() => {});
    pool.push(p);
    if (pool.length >= CONCURRENCY) {
      await Promise.race(pool);
      // Clean up completed promises
      for (let i = pool.length - 1; i >= 0; i--) {
        const promiseState = await Promise.race([pool[i], Promise.resolve('pending')]);
        // This is a hack — just trim completed
      }
      // Actually just wait for one slot
      await Promise.any(pool.map(p => p.then(() => 'done', () => 'done')));
      // Filter out resolved
      const stillPending = [];
      for (const p of pool) {
        // We can't really check resolution without side effects — just process sequentially
        stillPending.push(p);
      }
      pool.length = 0;
      pool.push(...stillPending);
    }
  }
  await Promise.all(pool);
  return found;
}

// Simpler concurrent version
async function runConcurrent(rows, worker, concurrency) {
  const queue = [...rows];
  let found = 0;
  let done = 0;
  const start = Date.now();

  async function workerLoop() {
    while (queue.length) {
      const row = queue.shift();
      if (!row) return;
      const ok = await worker(row);
      done++;
      if (ok) found++;
      if (done % 20 === 0 || done === rows.length) {
        const elapsed = (Date.now() - start) / 1000;
        const rate = (done / elapsed).toFixed(1);
        const eta = rows.length > done ? Math.round((rows.length - done) / (parseFloat(rate) || 1)) : 0;
        console.log(`  [${(done/rows.length*100).toFixed(1)}%] ${done}/${rows.length}, found=${found}, ${rate}/s, ETA ${eta}s`);
      }
    }
  }

  const workers = Array(concurrency).fill().map(() => workerLoop());
  await Promise.all(workers);
  return found;
}

async function main() {
  console.log(`=== Generic Email Enricher ===`);
  console.log(`Input:  ${INPUT_FILE}`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log(`Concurrency: ${CONCURRENCY}`);

  const text = fs.readFileSync(INPUT_FILE, 'utf-8');
  const { headers, rows } = parseCsv(text);
  console.log(`Loaded ${rows.length} rows, headers: ${headers.join(', ')}`);

  // Limit if requested
  const workingRows = LIMIT > 0 ? rows.slice(0, LIMIT) : rows;

  // Add new columns
  const newHeaders = [...headers];
  for (const newCol of ['generic_email', 'email_confidence', 'email_source', 'email_status']) {
    if (!newHeaders.includes(newCol)) newHeaders.push(newCol);
  }

  const waterfall = new EmailWaterfall();

  const incrementalSave = setInterval(() => {
    writeCsv(newHeaders, rows);
  }, 30000);

  const found = await runConcurrent(workingRows, async (row) => {
    const domain = row.domain || row.Domain || '';
    if (!domain) {
      row.generic_email = '';
      row.email_status = 'no_domain';
      return false;
    }
    const r = await tryDomain(waterfall, domain);
    row.generic_email = r.email;
    row.email_confidence = r.confidence;
    row.email_source = r.source;
    row.email_status = r.status;
    return !!r.email;
  }, CONCURRENCY);

  clearInterval(incrementalSave);
  writeCsv(newHeaders, rows);

  console.log(`\n=== DONE ===`);
  console.log(`Processed: ${workingRows.length}`);
  console.log(`Emails found: ${found} (${(found/workingRows.length*100).toFixed(1)}%)`);
  console.log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
