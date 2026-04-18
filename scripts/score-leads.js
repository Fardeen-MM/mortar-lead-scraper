#!/usr/bin/env node
/**
 * Score master immigration leads by ICP fit and outreach value.
 *
 * Scoring factors (0-100):
 *   - Country priority (CA > US > UK > AU > India > others)
 *   - Has corporate email (not gmail) = +15
 *   - Has full name (first+last) = +10
 *   - Has phone = +5
 *   - Source quality (CICC > EOIR > ICEF > Podcast > UK ESL)
 *   - Firm completeness (name, website, domain) = +5-10
 *
 * Outputs priority-sorted CSV: output/leads-scored.csv
 * Adds `score` column to each row.
 *
 * Usage: node scripts/score-leads.js
 */

const fs = require('fs');
const path = require('path');

const INPUT_FILE = path.join(__dirname, '..', 'output', 'master-immigration-leads.csv');
const OUTPUT_FILE = path.join(__dirname, '..', 'output', 'leads-scored.csv');

// Country priority (rough: higher = better target for immigration webinar funnel)
const COUNTRY_SCORES = {
  // Primary markets (proven conversion, NCCI tier)
  'CA': 30, 'Canada': 30,
  'US': 28, 'USA': 28, 'United States': 28,
  'GB': 26, 'UK': 26, 'United Kingdom': 26,
  'AU': 25, 'Australia': 25,
  'NZ': 22, 'New Zealand': 22,
  'IE': 22, 'Ireland': 22,
  // Major sending countries (high intent, their consultants help people move)
  'IN': 22, 'India': 22,
  'NP': 20, 'Nepal': 20,
  'NG': 20, 'Nigeria': 20,
  'PK': 19, 'Pakistan': 19,
  'BD': 19, 'Bangladesh': 19,
  'PH': 19, 'Philippines': 19,
  'BR': 18, 'Brazil': 18,
  'MX': 18, 'Mexico': 18,
  'TR': 18, 'Türkiye': 18, 'Turkey': 18,
};

const SOURCE_SCORES = {
  'cicc_registry': 25,              // Official Canadian immigration consultant
  'eoir_accredited_reps': 24,       // Official US accredited reps
  'icef_ias': 23,                   // ICEF-accredited global agents
  'immigration_podcast': 22,        // Immigration-focused podcast hosts
  'propublica': 20,                 // Immigration nonprofits (US)
  'english_uk_schools': 18,         // Adjacent (ESL)
  'english-uk-schools': 18,
};

const PERSONAL_EMAIL_DOMAINS = new Set([
  'gmail.com', 'yahoo.com', 'yahoo.ca', 'yahoo.co.uk', 'hotmail.com',
  'outlook.com', 'outlook.ca', 'aol.com', 'icloud.com', 'me.com',
  'live.com', 'msn.com', 'proton.me', 'protonmail.com', 'zoho.com',
  'mail.com', 'shaw.ca', 'rogers.com', 'sympatico.ca',
]);

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

function scoreRow(row) {
  let score = 0;
  let tags = [];

  // Source (25 max)
  const srcScore = SOURCE_SCORES[row.source] || 15;
  score += srcScore;
  tags.push(`src:${srcScore}`);

  // Country (30 max)
  const countryScore = COUNTRY_SCORES[row.country] || 10;
  score += countryScore;
  tags.push(`cty:${countryScore}`);

  // Email present + corporate vs personal (20 max)
  if (row.email) {
    score += 5;
    const domain = (row.email.split('@')[1] || '').toLowerCase();
    if (PERSONAL_EMAIL_DOMAINS.has(domain)) {
      score += 5;
      tags.push('email:personal');
    } else {
      score += 15;
      tags.push('email:corporate');
    }
  } else {
    tags.push('no_email');
  }

  // Name complete (10 max)
  if (row.first_name && row.last_name) {
    score += 10;
    tags.push('name:full');
  } else if (row.first_name || row.last_name) {
    score += 3;
  }

  // Phone (5)
  if (row.phone) { score += 5; tags.push('phone'); }

  // Domain/website (10)
  if (row.domain) { score += 5; }
  if (row.website) { score += 5; }

  return { score, tags: tags.join(',') };
}

function main() {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`Input not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  console.log('=== Immigration Lead Scoring ===');
  const text = fs.readFileSync(INPUT_FILE, 'utf-8');
  const { headers, rows } = parseCsv(text);
  console.log(`Loaded ${rows.length} rows`);

  for (const row of rows) {
    const { score, tags } = scoreRow(row);
    row.score = score;
    row.score_tags = tags;
  }

  // Sort descending by score
  rows.sort((a, b) => parseInt(b.score) - parseInt(a.score));

  // Add new columns
  const newHeaders = [...headers];
  if (!newHeaders.includes('score')) newHeaders.push('score');
  if (!newHeaders.includes('score_tags')) newHeaders.push('score_tags');

  // Write CSV
  const lines = [newHeaders.join(',')];
  for (const r of rows) lines.push(newHeaders.map(h => escapeCsv(r[h] || '')).join(','));
  fs.writeFileSync(OUTPUT_FILE, lines.join('\n') + '\n');

  // Stats
  const bucket = [0, 0, 0, 0, 0]; // 80+, 60-79, 40-59, 20-39, <20
  for (const r of rows) {
    const s = parseInt(r.score) || 0;
    if (s >= 80) bucket[0]++;
    else if (s >= 60) bucket[1]++;
    else if (s >= 40) bucket[2]++;
    else if (s >= 20) bucket[3]++;
    else bucket[4]++;
  }

  console.log(`\nScore distribution:`);
  console.log(`  80+ (hot):       ${bucket[0]}`);
  console.log(`  60-79 (warm):    ${bucket[1]}`);
  console.log(`  40-59 (average): ${bucket[2]}`);
  console.log(`  20-39 (cool):    ${bucket[3]}`);
  console.log(`  <20 (cold):      ${bucket[4]}`);

  // Top 20
  console.log(`\nTop 20 leads:`);
  for (const r of rows.slice(0, 20)) {
    console.log(`  ${r.score}  ${(r.email || '(no email)').padEnd(45)}  ${(r.country || '?').padEnd(15)}  ${(r.source || '?').padEnd(22)}  ${(r.firm_name || '').slice(0, 30)}`);
  }

  console.log(`\nOutput: ${OUTPUT_FILE}`);
}

main();
