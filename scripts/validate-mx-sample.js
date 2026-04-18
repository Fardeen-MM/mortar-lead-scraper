#!/usr/bin/env node
/**
 * Quick MX-record validation of a sample of email domains from master file.
 * Gives us a proxy for deliverability without needing SMTP (port 25 blocked).
 *
 * Usage: node scripts/validate-mx-sample.js [sample_size]
 */

const fs = require('fs');
const path = require('path');
const dns = require('dns');

const SAMPLE_SIZE = parseInt(process.argv[2]) || 200;
const INPUT = path.join(__dirname, '..', 'output', 'master-immigration-leads.csv');

function parseCsvRows(text) {
  const lines = text.split(/\r?\n/);
  const headers = lines[0].split(',');
  return lines.slice(1).filter(l => l.trim()).map(line => {
    const cols = [];
    let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else if (c === ',') { cols.push(cur); cur = ''; }
      else if (c === '"' && cur === '') inQ = true;
      else cur += c;
    }
    cols.push(cur);
    const rec = {};
    headers.forEach((h, i) => rec[h] = cols[i] || '');
    return rec;
  });
}

function hasMx(domain) {
  return new Promise(r => {
    const timer = setTimeout(() => r(null), 5000);
    dns.resolveMx(domain, (err, recs) => {
      clearTimeout(timer);
      r(!err && recs && recs.length > 0 ? recs[0].exchange : null);
    });
  });
}

async function main() {
  const rows = parseCsvRows(fs.readFileSync(INPUT, 'utf-8'));
  const withEmail = rows.filter(r => r.email && r.email.includes('@'));
  console.log(`Total with email: ${withEmail.length}`);

  // Sample
  const pick = Math.min(SAMPLE_SIZE, withEmail.length);
  const shuffle = [...withEmail].sort(() => Math.random() - 0.5).slice(0, pick);

  // Dedup by domain
  const domainMap = new Map();
  for (const r of shuffle) {
    const d = (r.email.split('@')[1] || '').toLowerCase();
    if (d && !domainMap.has(d)) domainMap.set(d, r);
  }
  const domains = [...domainMap.keys()];
  console.log(`Unique domains in sample: ${domains.length}`);

  let ok = 0, fail = 0;
  const providers = {};

  // Parallel
  const results = await Promise.all(domains.map(async d => {
    const mx = await hasMx(d);
    return { domain: d, mx };
  }));

  for (const r of results) {
    if (r.mx) {
      ok++;
      const provider = r.mx.toLowerCase();
      let key = 'other';
      if (provider.includes('mail.protection.outlook.com')) key = 'microsoft365';
      else if (provider.includes('google')) key = 'google_workspace';
      else if (provider.includes('yahoo')) key = 'yahoo';
      else if (provider.includes('proton')) key = 'proton';
      else if (provider.includes('zoho')) key = 'zoho';
      providers[key] = (providers[key] || 0) + 1;
    } else {
      fail++;
    }
  }

  console.log('\n=== MX Sanity Check ===');
  console.log(`Domains tested: ${domains.length}`);
  console.log(`With MX:        ${ok} (${(ok / domains.length * 100).toFixed(1)}%)`);
  console.log(`Without MX:     ${fail} (${(fail / domains.length * 100).toFixed(1)}%)`);
  console.log(`\nProvider breakdown (among MX-valid):`);
  for (const [p, n] of Object.entries(providers).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${p}: ${n} (${(n / ok * 100).toFixed(1)}%)`);
  }

  if (providers.microsoft365) {
    console.log(`\n→ ${providers.microsoft365} domains are on M365 → can verify via GetCredentialType API (free).`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
