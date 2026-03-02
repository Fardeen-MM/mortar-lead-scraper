#!/usr/bin/env node
/**
 * Batch SMTP-verify emails from a law firm CSV.
 *
 * Usage:
 *   node scripts/verify-emails.js output/US-LAWFIRMS-MASTER.csv
 *   node scripts/verify-emails.js output/US-LAWFIRMS-MASTER.csv --concurrency 5
 */

const fs = require('fs');
const EmailVerifier = require('../lib/email-verifier');

const args = process.argv.slice(2);
const csvPath = args[0];
if (!csvPath) { console.error('Usage: node scripts/verify-emails.js <csv-file>'); process.exit(1); }

const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 3;

function parseCSV(text) {
  const lines = text.split('\n');
  const header = [];
  let field = '', inQuotes = false;
  for (const ch of lines[0]) {
    if (ch === '"') inQuotes = !inQuotes;
    else if (ch === ',' && !inQuotes) { header.push(field); field = ''; }
    else field += ch;
  }
  header.push(field);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const parts = []; field = ''; inQuotes = false;
    for (const ch of lines[i]) {
      if (ch === '"') inQuotes = !inQuotes;
      else if (ch === ',' && !inQuotes) { parts.push(field); field = ''; }
      else field += ch;
    }
    parts.push(field);
    const row = {};
    for (let j = 0; j < header.length; j++) row[header[j]] = parts[j] || '';
    rows.push(row);
  }
  return { header, rows };
}

async function run() {
  const csv = fs.readFileSync(csvPath, 'utf8');
  const { header, rows } = parseCSV(csv);
  const toVerify = rows.filter(r => r.email);
  console.log(`\nTotal rows: ${rows.length} | With email: ${toVerify.length} | Concurrency: ${CONCURRENCY}\n`);

  const verifier = new EmailVerifier();
  const results = { valid: 0, invalid: 0, catchAll: 0, error: 0 };
  const domainCache = {};
  let processed = 0;

  for (let i = 0; i < toVerify.length; i += CONCURRENCY) {
    const batch = toVerify.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (row) => {
      const email = row.email, domain = email.split('@')[1];
      if (domainCache[domain] === 'catch-all') { row._verified = 'catch-all'; results.catchAll++; return; }
      if (domainCache[domain] === 'no-mx') { row._verified = 'invalid'; row.email = ''; results.invalid++; return; }
      try {
        const result = await verifier.verify(email);
        if (result.catchAll) { domainCache[domain] = 'catch-all'; row._verified = 'catch-all'; results.catchAll++; }
        else if (result.valid) { row._verified = 'valid'; results.valid++; }
        else { row._verified = 'invalid'; row.email = ''; results.invalid++; }
      } catch { row._verified = 'error'; results.error++; }
    }));
    processed += batch.length;
    if (processed % 30 === 0 || processed === toVerify.length) {
      console.log(`[${Math.round(processed*100/toVerify.length)}%] ${processed}/${toVerify.length} — valid:${results.valid} invalid:${results.invalid} catch-all:${results.catchAll} error:${results.error}`);
    }
  }

  const outPath = csvPath.replace('.csv', '-verified.csv');
  const hl = header.join(',');
  const dl = rows.filter(r => r.email).map(r => header.map(h => { const v = r[h]||''; return v.includes(',')||v.includes('"') ? `"${v.replace(/"/g,'""')}"` : v; }).join(','));
  fs.writeFileSync(outPath, [hl, ...dl].join('\n'));

  const fullPath = csvPath.replace('.csv', '-cleaned.csv');
  const al = rows.map(r => header.map(h => { const v = r[h]||''; return v.includes(',')||v.includes('"') ? `"${v.replace(/"/g,'""')}"` : v; }).join(','));
  fs.writeFileSync(fullPath, [hl, ...al].join('\n'));

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  VERIFICATION COMPLETE`);
  console.log(`  Checked: ${toVerify.length} | Valid: ${results.valid} | Invalid: ${results.invalid} | Catch-all: ${results.catchAll} | Error: ${results.error}`);
  console.log(`  Verified CSV: ${outPath}`);
  console.log(`  Cleaned CSV:  ${fullPath}`);
  console.log(`${'═'.repeat(60)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
