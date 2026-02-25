#!/usr/bin/env node
/**
 * email-generator.js — Generate best-guess email addresses for leads without one
 *
 * Strategy (3 targets, in priority order):
 *   1. Leads with website domain but no email → firstname.lastname@domain
 *   2. Leads at same firm as someone who HAS email → use that firm's domain
 *   3. Leads with firm name but no website/email → derive domain from firm name
 *
 * All generated emails use firstname.lastname@domain pattern.
 * Sets email_source = 'generated_pattern' to distinguish from verified emails.
 * Never overwrites existing emails.
 *
 * Usage: node scripts/email-generator.js [--dry-run]
 */

const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const DRY_RUN = process.argv.includes('--dry-run');

// ---------------------------------------------------------------------------
// Generic / free email domains to skip
// ---------------------------------------------------------------------------
const GENERIC_DOMAINS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'comcast.net', 'att.net', 'sbcglobal.net', 'verizon.net', 'cox.net',
  'charter.net', 'earthlink.net', 'bellsouth.net', 'protonmail.com',
  'mail.com', 'zoho.com', 'ymail.com', 'rocketmail.com',
  'googlemail.com', 'fastmail.com', 'tutanota.com',
]);

// ---------------------------------------------------------------------------
// Name suffixes to strip
// ---------------------------------------------------------------------------
const NAME_SUFFIXES = /\b(jr\.?|sr\.?|iii|iv|ii|esq\.?|ph\.?d\.?|j\.?d\.?|ll\.?m\.?|m\.?d\.?)\s*$/i;

// ---------------------------------------------------------------------------
// Firm name tokens to strip when deriving domain
// ---------------------------------------------------------------------------
const FIRM_STRIP_PATTERNS = [
  /\b(LLP|LLC|PLLC|P\.?A\.?|P\.?C\.?|Inc\.?|Ltd\.?|PLC|S\.?C\.?|APC)\b/gi,
  /\b(& Associates|and Associates)\b/gi,
  /\bAttorneys?\s+at\s+Law\b/gi,
  /\bAttorneys?\b/gi,
  /\bLaw\s+Firm\b/gi,
  /\bLaw\s+Group\b/gi,
  /\bLaw\s+Offices?\s+of\b/gi,
  /\bOffices?\s+of\b/gi,
  /\bThe\b/gi,
  /\bLegal\s+Group\b/gi,
  /\bLegal\s+Services?\b/gi,
  /\bProfessional\s+Association\b/gi,
  /\bProfessional\s+Corporation\b/gi,
  /\bChartered\b/gi,
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Strip accents and special characters from a name part
 */
function cleanName(name) {
  if (!name) return '';
  return name
    .normalize('NFD')                          // decompose accents
    .replace(/[\u0300-\u036f]/g, '')           // strip combining diacriticals
    .replace(NAME_SUFFIXES, '')                // strip Jr., Sr., III, etc.
    .trim()
    .toLowerCase()
    .replace(/[^a-z\-]/g, '')                  // keep only letters and hyphens
    .replace(/^-+|-+$/g, '');                  // trim leading/trailing hyphens
}

/**
 * Extract domain from a URL string
 */
function extractDomain(url) {
  if (!url) return null;
  try {
    // Add protocol if missing
    let normalized = url.trim();
    if (!/^https?:\/\//i.test(normalized)) {
      normalized = 'https://' + normalized;
    }
    const parsed = new URL(normalized);
    let host = parsed.hostname.toLowerCase();
    // Strip www.
    if (host.startsWith('www.')) host = host.slice(4);
    return host || null;
  } catch {
    return null;
  }
}

/**
 * Check if a domain is generic/free email
 */
function isGenericDomain(domain) {
  return !domain || GENERIC_DOMAINS.has(domain);
}

/**
 * Build firstname.lastname@domain email
 */
function buildEmail(firstName, lastName, domain) {
  const fn = cleanName(firstName);
  const ln = cleanName(lastName);
  if (!fn || !ln || !domain) return null;
  return `${fn}.${ln}@${domain}`;
}

/**
 * Extract domain from an email address
 */
function domainFromEmail(email) {
  if (!email || !email.includes('@')) return null;
  return email.split('@')[1].toLowerCase();
}

/**
 * Derive likely domain(s) from a firm name.
 * Returns an array of candidate domains (primary + law-suffixed variant).
 */
function deriveDomainFromFirm(firmName) {
  if (!firmName) return [];

  let cleaned = firmName;

  // Apply all strip patterns
  for (const pattern of FIRM_STRIP_PATTERNS) {
    cleaned = cleaned.replace(pattern, '');
  }

  // Remove ampersands and punctuation
  cleaned = cleaned
    .replace(/&/g, '')
    .replace(/[.,;:'"!?()[\]{}]/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!cleaned) return [];

  // Normalize to lowercase, remove remaining non-alphanumeric (except spaces)
  const words = cleaned
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, '')
    .trim()
    .split(/\s+/)
    .filter(w => w.length > 0);

  if (words.length === 0) return [];

  const joined = words.join('');

  // Primary: joined words + .com
  const primary = joined + '.com';
  // Variant: joined words + "law" + .com (very common for law firms)
  const lawVariant = joined + 'law.com';

  const results = [];
  if (!isGenericDomain(primary)) results.push(primary);
  if (!isGenericDomain(lawVariant)) results.push(lawVariant);

  return results;
}


// ===========================================================================
// Main
// ===========================================================================

function main() {
  console.log('=== Email Generator ===');
  console.log(`Database: ${DB_PATH}`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN (no writes)' : 'LIVE'}`);
  console.log('');

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Prepare the update statement
  const updateStmt = db.prepare(`
    UPDATE leads
    SET email = ?, email_source = 'generated_pattern', updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `);

  const stats = { target1: 0, target2: 0, target3: 0, skippedNoName: 0 };
  const samples = [];

  // Track which IDs we've already assigned an email to (across targets)
  const assigned = new Set();

  // -------------------------------------------------------------------------
  // TARGET 1: Leads with website domain but no email
  // -------------------------------------------------------------------------
  console.log('--- Target 1: Website domain → email ---');

  const t1Leads = db.prepare(`
    SELECT id, first_name, last_name, website
    FROM leads
    WHERE (email IS NULL OR email = '')
    AND website IS NOT NULL AND website != ''
  `).all();

  console.log(`  Candidates: ${t1Leads.length}`);

  const t1Updates = [];
  for (const lead of t1Leads) {
    const domain = extractDomain(lead.website);
    if (!domain || isGenericDomain(domain)) continue;

    const email = buildEmail(lead.first_name, lead.last_name, domain);
    if (!email) {
      stats.skippedNoName++;
      continue;
    }

    t1Updates.push({ id: lead.id, email });
    assigned.add(lead.id);
    stats.target1++;
    if (samples.length < 20) {
      samples.push({ target: 1, name: `${lead.first_name} ${lead.last_name}`, email, source: domain });
    }
  }

  if (!DRY_RUN) {
    const t1Transaction = db.transaction((updates) => {
      for (const u of updates) {
        updateStmt.run(u.email, u.id);
      }
    });
    t1Transaction(t1Updates);
  }
  console.log(`  Generated: ${stats.target1}`);

  // -------------------------------------------------------------------------
  // TARGET 2: Same firm as someone with email
  // -------------------------------------------------------------------------
  console.log('--- Target 2: Firm-mate email domain → email ---');

  // Build a map of firm_name → email domain (from leads that have email)
  const firmDomainMap = new Map();
  const firmEmails = db.prepare(`
    SELECT firm_name, email
    FROM leads
    WHERE email IS NOT NULL AND email != ''
    AND firm_name IS NOT NULL AND firm_name != ''
  `).all();

  for (const row of firmEmails) {
    const domain = domainFromEmail(row.email);
    if (domain && !isGenericDomain(domain)) {
      // Use the first non-generic domain we find for each firm
      if (!firmDomainMap.has(row.firm_name)) {
        firmDomainMap.set(row.firm_name, domain);
      }
    }
  }
  console.log(`  Firms with known domain: ${firmDomainMap.size}`);

  const t2Leads = db.prepare(`
    SELECT id, first_name, last_name, firm_name
    FROM leads
    WHERE (email IS NULL OR email = '')
    AND firm_name IS NOT NULL AND firm_name != ''
  `).all();

  const t2Updates = [];
  for (const lead of t2Leads) {
    if (assigned.has(lead.id)) continue; // Already handled by Target 1

    const domain = firmDomainMap.get(lead.firm_name);
    if (!domain) continue;

    const email = buildEmail(lead.first_name, lead.last_name, domain);
    if (!email) {
      stats.skippedNoName++;
      continue;
    }

    t2Updates.push({ id: lead.id, email });
    assigned.add(lead.id);
    stats.target2++;
    if (samples.length < 20) {
      samples.push({ target: 2, name: `${lead.first_name} ${lead.last_name}`, firm: lead.firm_name, email, source: domain });
    }
  }

  if (!DRY_RUN) {
    const t2Transaction = db.transaction((updates) => {
      for (const u of updates) {
        updateStmt.run(u.email, u.id);
      }
    });
    t2Transaction(t2Updates);
  }
  console.log(`  Generated: ${stats.target2}`);

  // -------------------------------------------------------------------------
  // TARGET 3: Derive domain from firm name
  // -------------------------------------------------------------------------
  console.log('--- Target 3: Firm name → derived domain → email ---');

  const t3Leads = db.prepare(`
    SELECT id, first_name, last_name, firm_name
    FROM leads
    WHERE (email IS NULL OR email = '')
    AND firm_name IS NOT NULL AND firm_name != ''
  `).all();

  const t3Updates = [];
  for (const lead of t3Leads) {
    if (assigned.has(lead.id)) continue; // Already handled by Target 1 or 2

    const candidates = deriveDomainFromFirm(lead.firm_name);
    if (candidates.length === 0) continue;

    // Use the primary domain candidate (first one)
    const domain = candidates[0];
    const email = buildEmail(lead.first_name, lead.last_name, domain);
    if (!email) {
      stats.skippedNoName++;
      continue;
    }

    t3Updates.push({ id: lead.id, email });
    assigned.add(lead.id);
    stats.target3++;
    if (samples.length < 20) {
      samples.push({ target: 3, name: `${lead.first_name} ${lead.last_name}`, firm: lead.firm_name, email, derived: domain });
    }
  }

  if (!DRY_RUN) {
    const t3Transaction = db.transaction((updates) => {
      for (const u of updates) {
        updateStmt.run(u.email, u.id);
      }
    });
    t3Transaction(t3Updates);
  }
  console.log(`  Generated: ${stats.target3}`);

  // -------------------------------------------------------------------------
  // Summary
  // -------------------------------------------------------------------------
  const total = stats.target1 + stats.target2 + stats.target3;
  console.log('');
  console.log('=== RESULTS ===');
  console.log(`  Target 1 (website domain):     ${stats.target1}`);
  console.log(`  Target 2 (firm-mate domain):   ${stats.target2}`);
  console.log(`  Target 3 (derived from firm):  ${stats.target3}`);
  console.log(`  Skipped (no first+last name):  ${stats.skippedNoName}`);
  console.log(`  ─────────────────────────────`);
  console.log(`  Total new emails generated:    ${total}`);
  console.log('');

  // Post-run stats
  const postStats = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN email_source = 'generated_pattern' THEN 1 ELSE 0 END) as generated
    FROM leads
  `).get();
  console.log(`  Database totals after run:`);
  console.log(`    Total leads:       ${postStats.total}`);
  console.log(`    With email:        ${postStats.with_email}`);
  console.log(`    Generated emails:  ${postStats.generated}`);
  console.log('');

  // Print samples
  console.log('=== SAMPLE GENERATED EMAILS (up to 20) ===');
  for (const s of samples) {
    const details = s.firm ? ` | firm: ${s.firm}` : '';
    const src = s.source || s.derived || '';
    console.log(`  [T${s.target}] ${s.name} → ${s.email}  (${src}${details})`);
  }
  console.log('');

  db.close();
  console.log('Done.');
}

main();
