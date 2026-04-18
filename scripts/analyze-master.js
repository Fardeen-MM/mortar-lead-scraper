#!/usr/bin/env node
/**
 * Analyze master-immigration-leads.csv and print a ready-to-use summary.
 *
 * Usage: node scripts/analyze-master.js [path]
 */

const fs = require('fs');
const path = require('path');

const FILE = process.argv[2] || path.join(__dirname, '..', 'output', 'master-immigration-leads.csv');

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

const text = fs.readFileSync(FILE, 'utf-8');
const { rows } = parseCsv(text);

// Basic counts
const withEmail = rows.filter(r => r.email);
const withPhone = rows.filter(r => r.phone);
const withDomain = rows.filter(r => r.domain);
const withFirstLast = rows.filter(r => r.first_name && r.last_name);

// By source
const sourceCount = {};
for (const r of rows) sourceCount[r.source || '?'] = (sourceCount[r.source || '?'] || 0) + 1;

// By source + with email
const sourceEmail = {};
for (const r of withEmail) sourceEmail[r.source || '?'] = (sourceEmail[r.source || '?'] || 0) + 1;

// By country
const countryCount = {};
for (const r of rows) countryCount[r.country || '?'] = (countryCount[r.country || '?'] || 0) + 1;
const countryEmail = {};
for (const r of withEmail) countryEmail[r.country || '?'] = (countryEmail[r.country || '?'] || 0) + 1;

// Unique domains (to estimate website email potential)
const uniqueDomains = new Set(withDomain.map(r => r.domain));

// Email TLDs — detect catch-alls by domain
const emailDomains = {};
for (const r of withEmail) {
  const d = (r.email.split('@')[1] || '').toLowerCase();
  emailDomains[d] = (emailDomains[d] || 0) + 1;
}

console.log('╔════════════════════════════════════════════════════════════════╗');
console.log('║         MASTER IMMIGRATION LEADS — ANALYSIS                    ║');
console.log('╠════════════════════════════════════════════════════════════════╣');
console.log(`║  Total leads:      ${String(rows.length).padEnd(42)} ║`);
console.log(`║  With email:       ${String(withEmail.length + ' (' + (withEmail.length / rows.length * 100).toFixed(1) + '%)').padEnd(42)} ║`);
console.log(`║  With phone:       ${String(withPhone.length + ' (' + (withPhone.length / rows.length * 100).toFixed(1) + '%)').padEnd(42)} ║`);
console.log(`║  With domain:      ${String(withDomain.length + ' (' + (withDomain.length / rows.length * 100).toFixed(1) + '%)').padEnd(42)} ║`);
console.log(`║  With full name:   ${String(withFirstLast.length + ' (' + (withFirstLast.length / rows.length * 100).toFixed(1) + '%)').padEnd(42)} ║`);
console.log(`║  Unique domains:   ${String(uniqueDomains.size).padEnd(42)} ║`);
console.log('╚════════════════════════════════════════════════════════════════╝');

console.log('\n▪ By source (rows / with email / email rate):');
for (const [s, n] of Object.entries(sourceCount).sort((a,b) => b[1] - a[1])) {
  const e = sourceEmail[s] || 0;
  const rate = (e / n * 100).toFixed(1);
  console.log(`  ${s.padEnd(30)} ${String(n).padStart(6)}  /  ${String(e).padStart(6)}  /  ${rate}%`);
}

console.log('\n▪ Top 20 countries (total / emails):');
for (const [c, n] of Object.entries(countryCount).sort((a,b) => b[1] - a[1]).slice(0, 20)) {
  const e = countryEmail[c] || 0;
  console.log(`  ${c.padEnd(25)} ${String(n).padStart(6)}  /  ${String(e).padStart(6)}`);
}

console.log('\n▪ Top 10 email domains (recipient concentration):');
for (const [d, n] of Object.entries(emailDomains).sort((a,b) => b[1] - a[1]).slice(0, 10)) {
  console.log(`  @${d.padEnd(40)} ${n}`);
}

// Personal emails vs corporate
const personalProviders = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'live.com', 'msn.com', 'proton.me', 'protonmail.com',
]);
const personal = withEmail.filter(r => personalProviders.has((r.email.split('@')[1] || '').toLowerCase())).length;
const corporate = withEmail.length - personal;
console.log(`\n▪ Email type:`);
console.log(`  Corporate (org domain): ${corporate} (${(corporate / withEmail.length * 100).toFixed(1)}%)`);
console.log(`  Personal (gmail etc):   ${personal} (${(personal / withEmail.length * 100).toFixed(1)}%)`);

// By niche
const nicheCount = {};
for (const r of rows) nicheCount[r.niche || '?'] = (nicheCount[r.niche || '?'] || 0) + 1;
console.log(`\n▪ By niche:`);
for (const [n, c] of Object.entries(nicheCount).sort((a,b) => b[1] - a[1])) {
  console.log(`  ${n.padEnd(40)} ${c}`);
}

console.log(`\nFile: ${FILE}`);
console.log(`Size: ${(fs.statSync(FILE).size / 1024).toFixed(0)} KB`);
