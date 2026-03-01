#!/usr/bin/env node
/**
 * export-verified-only.js — Export only verified/scraped emails
 * Columns: first_name, last_name, email, email_quality, firm_name, website
 * Split by: US+Canada, UK, Australia, Europe-Asia
 */

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const EXPORTS_DIR = path.join(__dirname, '..', 'exports');

const CANADA = new Set(['CA-AB','CA-BC','CA-NL','CA-PE','CA-YT','CA-MB','CA-NB','CA-NS','CA-NT','CA-NU','CA-ON','CA-SK']);
const UK_STATES = new Set(['UK-SC','UK-EW-BAR','UK-EW','UK-NI']);
const AU_STATES = new Set(['AU-NSW','AU-QLD','AU-SA','AU-TAS','AU-VIC','AU-WA','AU-ACT','AU-NT']);
const EU_ASIA = new Set(['FR','IE','IT','HK','NZ','SG']);

const GENERIC_PREFIXES = new Set([
  'info','contact','office','admin','support','help','mail','enquiries',
  'reception','general','hello','team','sales','billing','noreply',
  'no-reply','webmaster','postmaster','hr','careers','media','press',
  'marketing','feedback','service','customerservice','clientservices'
]);

const HEADER = 'first_name,last_name,email,email_quality,firm_name,title,phone,website,city,state,country,practice_area,bar_status,admission_date,linkedin_url,decision_maker_score,lead_score,primary_source';

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function deriveCountry(state) {
  if (!state) return 'United States';
  if (CANADA.has(state)) return 'Canada';
  if (UK_STATES.has(state)) return 'United Kingdom';
  if (AU_STATES.has(state)) return 'Australia';
  if (state === 'FR') return 'France';
  if (state === 'IE') return 'Ireland';
  if (state === 'IT') return 'Italy';
  if (state === 'HK') return 'Hong Kong';
  if (state === 'NZ') return 'New Zealand';
  if (state === 'SG') return 'Singapore';
  return 'United States';
}

function toRow(lead) {
  return [
    csvEscape(lead.first_name),
    csvEscape(lead.last_name),
    csvEscape(lead.email),
    'verified',
    csvEscape(lead.firm_name),
    csvEscape(lead.title),
    csvEscape(lead.phone),
    csvEscape(lead.website),
    csvEscape(lead.city),
    csvEscape(lead.state),
    csvEscape(deriveCountry(lead.state)),
    csvEscape(lead.practice_area),
    csvEscape(lead.bar_status),
    csvEscape(lead.admission_date),
    csvEscape(lead.linkedin_url),
    csvEscape(lead.icp_score || 0),
    csvEscape(lead.lead_score || 0),
    csvEscape(lead.primary_source)
  ].join(',');
}

function getRegion(state) {
  if (!state) return 'us-canada';
  if (CANADA.has(state)) return 'us-canada';
  if (UK_STATES.has(state)) return 'uk';
  if (AU_STATES.has(state)) return 'australia';
  if (EU_ASIA.has(state)) return 'europe-asia';
  return 'us-canada'; // Default: US states
}

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(0) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function main() {
  console.log('=== Verified Emails Export ===\n');

  const db = new Database(DB_PATH, { readonly: true });

  // Get all verified emails (NOT generated_pattern)
  const leads = db.prepare(`
    SELECT first_name, last_name, email, firm_name, title, phone, website,
           city, state, practice_area, bar_status, admission_date,
           linkedin_url, icp_score, lead_score, primary_source, email_source
    FROM leads
    WHERE email IS NOT NULL AND email != ''
    AND (email_source IS NULL OR email_source != 'generated_pattern')
    AND first_name IS NOT NULL AND first_name != ''
    AND last_name IS NOT NULL AND last_name != ''
    ORDER BY icp_score DESC, lead_score DESC
  `).all();

  console.log(`Total verified email leads: ${leads.length.toLocaleString()}`);

  // Filter out deceased/disbarred and generic emails
  const clean = leads.filter(l => {
    const status = (l.bar_status || '').toLowerCase();
    if (status.includes('deceased') || status.includes('disbarred') || status.includes('revoked')) return false;
    const prefix = l.email.split('@')[0].toLowerCase();
    if (GENERIC_PREFIXES.has(prefix)) return false;
    return true;
  });

  console.log(`After removing deceased/generic: ${clean.length.toLocaleString()}`);

  // Dedup by email (keep highest scored)
  const seen = new Set();
  const deduped = [];
  for (const l of clean) {
    const em = l.email.toLowerCase().trim();
    if (seen.has(em)) continue;
    seen.add(em);
    deduped.push(l);
  }

  console.log(`After dedup: ${deduped.length.toLocaleString()}\n`);

  // Split by region
  const regions = {
    'us-canada': [],
    'uk': [],
    'australia': [],
    'europe-asia': []
  };

  for (const l of deduped) {
    const r = getRegion(l.state);
    regions[r].push(l);
  }

  // Write files
  let totalRows = 0;
  for (const [key, rLeads] of Object.entries(regions)) {
    if (rLeads.length === 0) continue;

    const fname = `verified-emails-${key}.csv`;
    const content = HEADER + '\n' + rLeads.map(toRow).join('\n') + '\n';
    const filePath = path.join(EXPORTS_DIR, fname);
    const size = Buffer.byteLength(content, 'utf8');

    fs.writeFileSync(filePath, content, 'utf8');
    totalRows += rLeads.length;

    console.log(`  ${fname}`);
    console.log(`    ${rLeads.length.toLocaleString()} rows | ${formatSize(size)}`);
  }

  console.log(`\nTotal: ${totalRows.toLocaleString()} verified emails across ${Object.values(regions).filter(r => r.length > 0).length} files`);

  db.close();
  console.log('Done.');
}

main();
