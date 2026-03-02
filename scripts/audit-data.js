#!/usr/bin/env node
/**
 * Data Quality Audit — analyze a CSV for issues and generate a report.
 *
 * Usage:
 *   node scripts/audit-data.js output/ALL-LAWFIRMS-MASTER.csv
 */

const fs = require('fs');

const INPUT = process.argv[2];
if (!INPUT) {
  console.log('Usage: node scripts/audit-data.js <csv-file>');
  process.exit(1);
}

function parseCSVLine(line) {
  const fields = [];
  let field = '', inQuotes = false;
  for (const ch of line) {
    if (ch === '"') inQuotes = !inQuotes;
    else if (ch === ',' && !inQuotes) { fields.push(field); field = ''; }
    else field += ch;
  }
  fields.push(field);
  return fields;
}

const csvText = fs.readFileSync(INPUT, 'utf8');
const lines = csvText.split('\n');
const header = parseCSVLine(lines[0]);

const rows = [];
for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const fields = parseCSVLine(lines[i]);
  const row = {};
  header.forEach((h, idx) => row[h] = fields[idx] || '');
  rows.push(row);
}

console.log(`\n${'═'.repeat(60)}`);
console.log(`  DATA QUALITY AUDIT: ${rows.length} leads`);
console.log(`${'═'.repeat(60)}\n`);

// --- Completeness ---
const completeness = {};
for (const col of header) {
  const filled = rows.filter(r => r[col] && r[col].trim()).length;
  completeness[col] = { filled, pct: Math.round(filled * 100 / rows.length) };
}

console.log('FIELD COMPLETENESS:');
for (const [col, stat] of Object.entries(completeness)) {
  const bar = '█'.repeat(Math.round(stat.pct / 5)) + '░'.repeat(20 - Math.round(stat.pct / 5));
  console.log(`  ${col.padEnd(22)} ${bar} ${stat.filled}/${rows.length} (${stat.pct}%)`);
}

// --- Email Quality ---
console.log('\nEMAIL QUALITY:');
const emailIssues = { missing: 0, invalid: 0, generic: 0, duplicate: 0, pattern: 0, verified: 0 };
const emailSet = new Set();
const emailDomains = {};

const GENERIC_LOCALS = new Set(['info', 'contact', 'admin', 'office', 'help', 'support', 'hello', 'mail', 'enquiries', 'inquiries', 'reception', 'general', 'team']);

for (const row of rows) {
  const email = (row.email || '').trim().toLowerCase();
  if (!email) { emailIssues.missing++; continue; }

  // Invalid format
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(email)) {
    emailIssues.invalid++;
    continue;
  }

  // Duplicate
  if (emailSet.has(email)) {
    emailIssues.duplicate++;
  }
  emailSet.add(email);

  // Generic
  const local = email.split('@')[0];
  if (GENERIC_LOCALS.has(local)) emailIssues.generic++;

  // Pattern-generated
  if ((row.email_source || '').includes('pattern')) emailIssues.pattern++;

  // Domain stats
  const domain = email.split('@')[1];
  emailDomains[domain] = (emailDomains[domain] || 0) + 1;
}

console.log(`  Missing email:     ${emailIssues.missing} (${Math.round(emailIssues.missing * 100 / rows.length)}%)`);
console.log(`  Invalid format:    ${emailIssues.invalid}`);
console.log(`  Duplicate emails:  ${emailIssues.duplicate}`);
console.log(`  Generic (info@):   ${emailIssues.generic}`);
console.log(`  Pattern-generated: ${emailIssues.pattern} (unverified)`);
console.log(`  Unique emails:     ${emailSet.size}`);

console.log('\n  Top email domains:');
Object.entries(emailDomains).sort((a, b) => b[1] - a[1]).slice(0, 15)
  .forEach(([d, c]) => console.log(`    ${c}x @${d}`));

// --- Phone Quality ---
console.log('\nPHONE QUALITY:');
let phoneMissing = 0, phoneInvalid = 0, phoneDuplicate = 0;
const phoneSet = new Set();
for (const row of rows) {
  const phone = (row.phone || '').replace(/\D/g, '');
  if (!phone) { phoneMissing++; continue; }
  if (phone.length < 7 || phone.length > 15) phoneInvalid++;
  if (phoneSet.has(phone)) phoneDuplicate++;
  phoneSet.add(phone);
}
console.log(`  Missing phone:     ${phoneMissing} (${Math.round(phoneMissing * 100 / rows.length)}%)`);
console.log(`  Invalid length:    ${phoneInvalid}`);
console.log(`  Duplicate phones:  ${phoneDuplicate}`);

// --- Name Quality ---
console.log('\nPERSON NAME QUALITY:');
let noName = 0, suspiciousFirst = 0, suspiciousLast = 0;
const SUSPICIOUS_WORDS = new Set([
  'law', 'firm', 'office', 'group', 'associates', 'legal', 'attorney',
  'service', 'services', 'center', 'clinic', 'company', 'inc', 'llc',
  'the', 'a', 'an', 'and', 'or',
]);

for (const row of rows) {
  const first = (row.first_name || '').trim();
  const last = (row.last_name || '').trim();
  if (!first && !last) { noName++; continue; }
  if (first && SUSPICIOUS_WORDS.has(first.toLowerCase())) suspiciousFirst++;
  if (last && SUSPICIOUS_WORDS.has(last.toLowerCase())) suspiciousLast++;
}
console.log(`  No person name:       ${noName} (${Math.round(noName * 100 / rows.length)}%)`);
console.log(`  Suspicious first:     ${suspiciousFirst}`);
console.log(`  Suspicious last:      ${suspiciousLast}`);

// --- Title Quality ---
console.log('\nTITLE DISTRIBUTION:');
const titles = {};
for (const row of rows) {
  const title = (row.title || '').trim();
  if (title) titles[title] = (titles[title] || 0) + 1;
}
const noTitle = rows.filter(r => !r.title || !r.title.trim()).length;
console.log(`  No title: ${noTitle} (${Math.round(noTitle * 100 / rows.length)}%)`);
Object.entries(titles).sort((a, b) => b[1] - a[1]).slice(0, 15)
  .forEach(([t, c]) => console.log(`    ${c}x "${t}"`));

// --- Geographic Distribution ---
console.log('\nGEOGRAPHIC DISTRIBUTION:');
const states = {};
for (const row of rows) {
  const state = row.state || row.country || 'unknown';
  states[state] = (states[state] || 0) + 1;
}
Object.entries(states).sort((a, b) => b[1] - a[1]).slice(0, 20)
  .forEach(([s, c]) => console.log(`    ${c}x ${s}`));

// --- Decision Maker Analysis ---
console.log('\nDECISION MAKER ANALYSIS:');
const DM_PATTERNS = /partner|founder|owner|managing|principal|director|president|ceo|chairman/i;
let dms = 0, dmsWithEmail = 0, dmsWithPhone = 0;
for (const row of rows) {
  const title = row.title || '';
  if (DM_PATTERNS.test(title)) {
    dms++;
    if (row.email) dmsWithEmail++;
    if (row.phone) dmsWithPhone++;
  }
}
console.log(`  Decision makers:       ${dms} (${Math.round(dms * 100 / rows.length)}% of all leads)`);
console.log(`  DMs with email:        ${dmsWithEmail} (${dms > 0 ? Math.round(dmsWithEmail * 100 / dms) : 0}%)`);
console.log(`  DMs with phone:        ${dmsWithPhone} (${dms > 0 ? Math.round(dmsWithPhone * 100 / dms) : 0}%)`);

// --- Source Distribution ---
console.log('\nSOURCE DISTRIBUTION:');
const sources = {};
for (const row of rows) {
  const src = row.source || 'unknown';
  sources[src] = (sources[src] || 0) + 1;
}
Object.entries(sources).sort((a, b) => b[1] - a[1]).slice(0, 15)
  .forEach(([s, c]) => console.log(`    ${c}x ${s}`));

// --- Email Source ---
console.log('\nEMAIL SOURCE:');
const emailSources = {};
for (const row of rows) {
  const src = row.email_source || (row.email ? 'unknown' : 'no_email');
  emailSources[src] = (emailSources[src] || 0) + 1;
}
Object.entries(emailSources).sort((a, b) => b[1] - a[1])
  .forEach(([s, c]) => console.log(`    ${c}x ${s}`));

// --- Actionability Score ---
console.log('\nACTIONABILITY:');
let gold = 0, silver = 0, bronze = 0, dead = 0;
for (const row of rows) {
  const hasEmail = !!row.email;
  const hasPhone = !!row.phone;
  const hasName = !!row.first_name;
  const hasDM = DM_PATTERNS.test(row.title || '');

  if (hasEmail && hasName && hasDM) gold++;
  else if (hasEmail && hasName) silver++;
  else if (hasEmail || hasPhone) bronze++;
  else dead++;
}
console.log(`  🥇 Gold (DM + email + name):  ${gold}`);
console.log(`  🥈 Silver (email + name):      ${silver}`);
console.log(`  🥉 Bronze (email or phone):    ${bronze}`);
console.log(`  💀 Dead (no contact):           ${dead}`);

console.log(`\n${'═'.repeat(60)}`);
console.log(`  SUMMARY: ${rows.length} leads | ${emailSet.size} unique emails | ${dms} DMs`);
console.log(`${'═'.repeat(60)}\n`);
