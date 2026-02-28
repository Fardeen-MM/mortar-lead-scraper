#!/usr/bin/env node
/**
 * export-cold-email.js — Export clean, priority-sorted leads for cold email
 *
 * Exports:
 *   PRIORITY FILES (leads you can actually contact):
 *   1. hot-leads-with-email.csv          — Decision makers (score >= 50) WITH real/verified email
 *   2. warm-leads-with-email.csv         — Everyone else WITH email
 *   3. leads-with-phone-no-email.csv     — Have phone but no email (for cold calling)
 *
 *   REGIONAL FILES (all contactable leads by geography):
 *   4. region-us-east.csv
 *   5. region-us-west.csv
 *   6. region-us-central.csv
 *   7. region-canada.csv
 *   8. region-uk.csv
 *   9. region-australia.csv
 *   10. region-europe-asia.csv
 *
 * Filters OUT:
 *   - Deceased, suspended, disbarred, revoked, resigned, retired
 *   - Inactive lawyers (unless they have email — some inactive still practice)
 *   - Leads missing both first AND last name
 *   - Duplicate emails (keeps highest-scored lead per email)
 *   - Generic emails (info@, contact@, office@, admin@, support@, etc.)
 *
 * Sort order: email quality tier → decision_maker_score → lead_score
 *
 * Usage: node scripts/export-cold-email.js
 */

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const EXPORTS_DIR = path.join(__dirname, '..', 'exports');

// ── Statuses to ALWAYS exclude (truly gone) ──────────────────────
const HARD_EXCLUDE_STATUSES = [
  'deceased', 'disbarred', 'revoked', 'surrendered'
];

// ── Statuses to exclude UNLESS lead has email ────────────────────
const SOFT_EXCLUDE_STATUSES = [
  'suspended', 'resigned', 'retired', 'inactive', 'disabled',
  'inactive voluntary', 'not eligible', 'not authorized'
];

// ── Generic email prefixes to filter out ─────────────────────────
const GENERIC_EMAIL_PREFIXES = new Set([
  'info', 'contact', 'office', 'admin', 'support', 'help',
  'mail', 'enquiries', 'inquiries', 'reception', 'general',
  'hello', 'team', 'sales', 'billing', 'accounts', 'noreply',
  'no-reply', 'webmaster', 'postmaster', 'abuse', 'legal',
  'hr', 'careers', 'jobs', 'media', 'press', 'marketing',
  'feedback', 'service', 'customerservice', 'clientservices'
]);

// ── Region mappings ──────────────────────────────────────────────
const US_EAST = new Set([
  'NY', 'FL', 'GA', 'PA', 'NC', 'SC', 'VA', 'MD', 'DE', 'CT',
  'ME', 'MA', 'NH', 'VT', 'RI', 'NJ', 'DC', 'WV'
]);

const US_WEST = new Set([
  'CA', 'WA', 'OR', 'CO', 'AZ', 'HI', 'AK', 'ID', 'NM', 'NV',
  'UT', 'MT', 'WY'
]);

const US_CENTRAL = new Set([
  'TX', 'IL', 'OH', 'MN', 'MO', 'MI', 'KY', 'TN', 'IN', 'WI',
  'IA', 'AR', 'MS', 'AL', 'LA', 'NE', 'KS', 'ND', 'SD', 'OK'
]);

const CANADA = new Set(['CA-AB', 'CA-BC', 'CA-NL', 'CA-PE', 'CA-YT', 'CA-MB', 'CA-NB', 'CA-NS', 'CA-NT', 'CA-NU', 'CA-ON', 'CA-SK']);
const UK = new Set(['UK-SC', 'UK-EW-BAR', 'UK-EW', 'UK-NI']);
const AUSTRALIA = new Set(['AU-NSW', 'AU-QLD', 'AU-SA', 'AU-TAS', 'AU-VIC', 'AU-WA', 'AU-ACT', 'AU-NT']);
const EUROPE_ASIA = new Set(['FR', 'IE', 'IT', 'HK', 'NZ', 'SG']);

// ── CSV columns (clean, cold-email-optimized) ────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'email', 'email_quality',
  'firm_name', 'title', 'phone', 'website',
  'city', 'state', 'country',
  'practice_area', 'bar_status', 'admission_date',
  'linkedin_url', 'decision_maker_score', 'lead_score',
  'primary_source'
];

const CSV_HEADER = CSV_COLUMNS.join(',');

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function deriveCountry(state) {
  if (!state) return '';
  if (CANADA.has(state)) return 'Canada';
  if (UK.has(state)) return 'United Kingdom';
  if (AUSTRALIA.has(state)) return 'Australia';
  if (state === 'FR') return 'France';
  if (state === 'IE') return 'Ireland';
  if (state === 'IT') return 'Italy';
  if (state === 'HK') return 'Hong Kong';
  if (state === 'NZ') return 'New Zealand';
  if (state === 'SG') return 'Singapore';
  return 'United States';
}

function emailQualityLabel(source) {
  if (!source) return 'scraped';
  if (source === 'bar' || source === 'profile') return 'verified';
  if (source === 'website-crawl') return 'verified';
  if (source === 'generated_pattern') return 'pattern_guess';
  if (source.startsWith('db:')) return 'scraped';
  return 'scraped';
}

function isGenericEmail(email) {
  if (!email) return false;
  const prefix = email.split('@')[0].toLowerCase();
  return GENERIC_EMAIL_PREFIXES.has(prefix);
}

function leadToCsvRow(lead) {
  return CSV_COLUMNS.map(col => {
    if (col === 'decision_maker_score') return csvEscape(lead.icp_score || 0);
    if (col === 'lead_score') return csvEscape(lead.lead_score || 0);
    if (col === 'country') return csvEscape(deriveCountry(lead.state));
    if (col === 'email_quality') return csvEscape(emailQualityLabel(lead.email_source));
    return csvEscape(lead[col]);
  }).join(',');
}

function getRegion(state) {
  if (US_EAST.has(state)) return 'us-east';
  if (US_WEST.has(state)) return 'us-west';
  if (US_CENTRAL.has(state)) return 'us-central';
  if (CANADA.has(state)) return 'canada';
  if (UK.has(state)) return 'uk';
  if (AUSTRALIA.has(state)) return 'australia';
  if (EUROPE_ASIA.has(state)) return 'europe-asia';
  // Remaining US states not in the 3 sets
  if (state && state.length === 2 && !state.includes('-')) return 'us-central';
  return null;
}

// ── Max file size before splitting (50 MB) ───────────────────────
const MAX_FILE_SIZE = 50 * 1024 * 1024;

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatNum(n) { return n.toLocaleString(); }

// ===========================================================================
// Main
// ===========================================================================
function main() {
  console.log('=== Cold Email Export (Clean + Priority) ===\n');

  if (!fs.existsSync(EXPORTS_DIR)) {
    fs.mkdirSync(EXPORTS_DIR, { recursive: true });
  }

  // Clean out old exports
  const oldFiles = fs.readdirSync(EXPORTS_DIR).filter(f => f.endsWith('.csv'));
  for (const f of oldFiles) {
    fs.unlinkSync(path.join(EXPORTS_DIR, f));
  }
  console.log(`Cleaned ${oldFiles.length} old CSV files\n`);

  const db = new Database(DB_PATH, { readonly: true });

  // ── Step 1: Load all leads ──────────────────────────────────────
  const allLeads = db.prepare(`
    SELECT
      id, first_name, last_name, email, firm_name, title, phone,
      website, city, state, practice_area, bar_status,
      admission_date, linkedin_url, icp_score, lead_score,
      primary_source, email_source
    FROM leads
    ORDER BY icp_score DESC, lead_score DESC
  `).all();
  console.log(`Total leads in DB: ${formatNum(allLeads.length)}`);

  // ── Step 2: Filter out junk ─────────────────────────────────────
  let cleaned = allLeads.filter(lead => {
    // Must have a name
    if (!lead.first_name || !lead.last_name) return false;
    if (!lead.first_name.trim() || !lead.last_name.trim()) return false;

    const status = (lead.bar_status || '').toLowerCase();

    // Always exclude deceased/disbarred
    for (const bad of HARD_EXCLUDE_STATUSES) {
      if (status.includes(bad)) return false;
    }

    // Soft-exclude inactive UNLESS they have email or phone (still reachable)
    const hasContact = (lead.email && lead.email.trim()) || (lead.phone && lead.phone.trim());
    if (!hasContact) {
      for (const soft of SOFT_EXCLUDE_STATUSES) {
        if (status.includes(soft)) return false;
      }
    }

    return true;
  });
  console.log(`After removing deceased/disbarred/no-name: ${formatNum(cleaned.length)}`);

  // ── Step 3: Clean emails — remove generic ones ──────────────────
  let genericRemoved = 0;
  for (const lead of cleaned) {
    if (lead.email && isGenericEmail(lead.email)) {
      lead.email = null;
      lead.email_source = null;
      genericRemoved++;
    }
  }
  console.log(`Generic emails removed (info@, contact@, etc.): ${genericRemoved}`);

  // ── Step 4: Deduplicate by email (keep highest-scored) ──────────
  const emailSeen = new Map();
  let emailDupes = 0;
  for (const lead of cleaned) {
    if (!lead.email || !lead.email.trim()) continue;
    const em = lead.email.toLowerCase().trim();
    if (emailSeen.has(em)) {
      const existing = emailSeen.get(em);
      const existingScore = (existing.icp_score || 0) * 100 + (existing.lead_score || 0);
      const thisScore = (lead.icp_score || 0) * 100 + (lead.lead_score || 0);
      if (thisScore > existingScore) {
        // This lead is better — blank out the old one's email
        existing.email = null;
        existing.email_source = null;
        emailSeen.set(em, lead);
      } else {
        lead.email = null;
        lead.email_source = null;
      }
      emailDupes++;
    } else {
      emailSeen.set(em, lead);
    }
  }
  console.log(`Duplicate emails removed: ${emailDupes}`);

  // ── Step 5: Must have SOME contact info to be exported ──────────
  const contactable = cleaned.filter(l =>
    (l.email && l.email.trim()) ||
    (l.phone && l.phone.trim()) ||
    (l.website && l.website.trim())
  );
  const noContact = cleaned.length - contactable.length;
  console.log(`Leads with zero contact info removed: ${formatNum(noContact)}`);
  console.log(`Contactable leads for export: ${formatNum(contactable.length)}\n`);

  // ── Stats ───────────────────────────────────────────────────────
  const withEmail = contactable.filter(l => l.email && l.email.trim());
  const verifiedEmail = withEmail.filter(l => {
    const s = l.email_source || '';
    return s === 'bar' || s === 'profile' || s === 'website-crawl' || s.startsWith('db:');
  });
  const generatedEmail = withEmail.filter(l => l.email_source === 'generated_pattern');
  const withPhone = contactable.filter(l => l.phone && l.phone.trim());
  const dms = contactable.filter(l => (l.icp_score || 0) >= 50);

  console.log('── Quality Breakdown ──');
  console.log(`  With email:        ${formatNum(withEmail.length)}`);
  console.log(`    Verified/scraped: ${formatNum(verifiedEmail.length)}`);
  console.log(`    Pattern guess:    ${formatNum(generatedEmail.length)}`);
  console.log(`  With phone:        ${formatNum(withPhone.length)}`);
  console.log(`  Decision makers:   ${formatNum(dms.length)}`);
  console.log('');

  // ── Sorting function ───────────────────────────────────────────
  // Priority: verified email first, then generated email, then phone-only
  // Within each tier: decision_maker_score DESC, lead_score DESC
  function prioritySort(a, b) {
    const aHasVerified = a.email && a.email_source !== 'generated_pattern' ? 1 : 0;
    const bHasVerified = b.email && b.email_source !== 'generated_pattern' ? 1 : 0;
    if (bHasVerified !== aHasVerified) return bHasVerified - aHasVerified;

    const aHasEmail = a.email ? 1 : 0;
    const bHasEmail = b.email ? 1 : 0;
    if (bHasEmail !== aHasEmail) return bHasEmail - aHasEmail;

    const aScore = (a.icp_score || 0);
    const bScore = (b.icp_score || 0);
    if (bScore !== aScore) return bScore - aScore;

    return (b.lead_score || 0) - (a.lead_score || 0);
  }

  const fileSummaries = [];
  let totalRows = 0;

  function writeCsv(filename, leads) {
    leads.sort(prioritySort);

    const rows = leads.map(leadToCsvRow);
    const content = CSV_HEADER + '\n' + rows.join('\n') + '\n';
    const filePath = path.join(EXPORTS_DIR, filename);
    const sizeBytes = Buffer.byteLength(content, 'utf8');

    // Split by state if too large
    if (sizeBytes > MAX_FILE_SIZE) {
      console.log(`  [!] ${filename} is ${formatSize(sizeBytes)} — splitting by state...`);
      return splitByState(filename, leads);
    }

    fs.writeFileSync(filePath, content, 'utf8');

    const emailCnt = leads.filter(l => l.email && l.email.trim()).length;
    const phoneCnt = leads.filter(l => l.phone && l.phone.trim()).length;
    const dmCnt = leads.filter(l => (l.icp_score || 0) >= 50).length;
    totalRows += leads.length;

    const summary = { file: filename, rows: leads.length, size: sizeBytes, emails: emailCnt, phones: phoneCnt, dms: dmCnt };
    fileSummaries.push(summary);

    console.log(`  ${filename}`);
    console.log(`    ${formatNum(leads.length)} rows | ${formatSize(sizeBytes)} | ${formatNum(emailCnt)} email | ${formatNum(phoneCnt)} phone | ${formatNum(dmCnt)} DMs`);
    console.log('');
  }

  function splitByState(filename, leads) {
    const base = filename.replace('.csv', '');
    const byState = {};
    for (const lead of leads) {
      const st = lead.state || 'other';
      if (!byState[st]) byState[st] = [];
      byState[st].push(lead);
    }

    const states = Object.keys(byState).sort((a, b) => byState[b].length - byState[a].length);
    for (const st of states) {
      const stLeads = byState[st];
      stLeads.sort(prioritySort);

      const splitName = `${base}-${st.toLowerCase().replace(/[^a-z0-9-]/g, '')}.csv`;
      const rows = stLeads.map(leadToCsvRow);
      const content = CSV_HEADER + '\n' + rows.join('\n') + '\n';
      const filePath = path.join(EXPORTS_DIR, splitName);
      const sizeBytes = Buffer.byteLength(content, 'utf8');
      fs.writeFileSync(filePath, content, 'utf8');

      const emailCnt = stLeads.filter(l => l.email && l.email.trim()).length;
      const phoneCnt = stLeads.filter(l => l.phone && l.phone.trim()).length;
      const dmCnt = stLeads.filter(l => (l.icp_score || 0) >= 50).length;
      totalRows += stLeads.length;

      const summary = { file: splitName, rows: stLeads.length, size: sizeBytes, emails: emailCnt, phones: phoneCnt, dms: dmCnt };
      fileSummaries.push(summary);

      console.log(`  ${splitName}`);
      console.log(`    ${formatNum(stLeads.length)} rows | ${formatSize(sizeBytes)} | ${formatNum(emailCnt)} email | ${formatNum(phoneCnt)} phone`);
    }
    console.log('');
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIORITY FILES — the ones you actually use for outreach
  // ═══════════════════════════════════════════════════════════════

  console.log('═══ PRIORITY FILES ═══\n');

  // 1. HOT: Decision makers with email (the gold)
  console.log('--- 1. Hot Leads (Decision Makers + Email) ---');
  const hotLeads = contactable.filter(l =>
    (l.icp_score || 0) >= 50 && l.email && l.email.trim()
  );
  writeCsv('01-hot-leads-decision-makers-with-email.csv', hotLeads);

  // 2. WARM: Everyone else with email
  console.log('--- 2. Warm Leads (All Others with Email) ---');
  const warmLeads = contactable.filter(l =>
    l.email && l.email.trim() && (l.icp_score || 0) < 50
  );
  writeCsv('02-warm-leads-with-email.csv', warmLeads);

  // 3. PHONE: Have phone but no email (cold calling list)
  console.log('--- 3. Phone Leads (No Email, Have Phone) ---');
  const phoneLeads = contactable.filter(l =>
    (!l.email || !l.email.trim()) &&
    l.phone && l.phone.trim()
  );
  writeCsv('03-phone-leads-no-email.csv', phoneLeads);

  // ═══════════════════════════════════════════════════════════════
  // REGIONAL FILES — all contactable leads broken by geography
  // ═══════════════════════════════════════════════════════════════

  console.log('═══ REGIONAL FILES (contactable leads by geography) ═══\n');

  const regionMap = {
    'us-canada': [],
    'uk': [],
    'australia': [],
    'europe-asia': [],
  };

  for (const lead of contactable) {
    const region = getRegion(lead.state);
    if (!region) continue;
    if (region === 'us-east' || region === 'us-west' || region === 'us-central' || region === 'canada') {
      regionMap['us-canada'].push(lead);
    } else if (regionMap[region]) {
      regionMap[region].push(lead);
    }
  }

  const regionNames = {
    'us-canada': 'US & Canada',
    'uk': 'United Kingdom',
    'australia': 'Australia',
    'europe-asia': 'Europe & Asia-Pacific',
  };

  let regionNum = 4;
  for (const [key, leads] of Object.entries(regionMap)) {
    if (leads.length === 0) continue;
    const padNum = String(regionNum).padStart(2, '0');
    console.log(`--- ${regionNum}. ${regionNames[key]} ---`);
    writeCsv(`${padNum}-region-${key}.csv`, leads);
    regionNum++;
  }

  // ═══════════════════════════════════════════════════════════════
  // MASTER FILE — everything contactable in one file
  // ═══════════════════════════════════════════════════════════════

  console.log('═══ MASTER FILE ═══\n');
  console.log(`--- ${regionNum}. All Contactable Leads ---`);
  const padNum = String(regionNum).padStart(2, '0');
  writeCsv(`${padNum}-all-contactable-leads.csv`, [...contactable]);

  db.close();

  // ── Final summary ──────────────────────────────────────────────
  console.log('═'.repeat(65));
  console.log('EXPORT SUMMARY');
  console.log('═'.repeat(65));
  console.log('');
  console.log('Files:');
  for (const s of fileSummaries) {
    const emailPct = s.rows > 0 ? Math.round(s.emails / s.rows * 100) : 0;
    console.log(`  ${s.file.padEnd(48)} ${formatNum(s.rows).padStart(7)} rows  ${formatSize(s.size).padStart(8)}  ${emailPct}% email`);
  }
  console.log('');
  const totalSize = fileSummaries.reduce((acc, s) => acc + s.size, 0);
  const totalEmails = fileSummaries.reduce((acc, s) => acc + s.emails, 0);
  console.log(`  Total files:  ${fileSummaries.length}`);
  console.log(`  Total rows:   ${formatNum(totalRows)}`);
  console.log(`  Total size:   ${formatSize(totalSize)}`);
  console.log(`  Total emails: ${formatNum(totalEmails)}`);
  console.log('\nDone.');
}

main();
