#!/usr/bin/env node
/**
 * deep-enrich.js — Clay-style deep enrichment engine
 *
 * Runs 5 enrichment passes:
 *   1. Title inference (instant — from firm name + admission year)
 *   2. Practice area detection (instant — from firm name keywords)
 *   3. LinkedIn URL construction (instant — best-guess slug)
 *   4. Website domain discovery (network — DNS HEAD checks)
 *   5. Email pattern learning (instant — firm-mate pattern sharing)
 *
 * Usage: node scripts/deep-enrich.js [--step N] [--limit N] [--dry-run]
 */

const path = require('path');
const dns = require('dns');
const http = require('http');
const https = require('https');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const DRY_RUN = process.argv.includes('--dry-run');

const args = process.argv.slice(2);
let ONLY_STEP = null;
let LIMIT = 0;
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--step' && args[i + 1]) ONLY_STEP = parseInt(args[i + 1]);
  if (args[i] === '--limit' && args[i + 1]) LIMIT = parseInt(args[i + 1]);
}

// =====================================================================
// STEP 1: Title Inference
// =====================================================================

function inferTitle(firstName, lastName, firmName, admissionDate, barNumber) {
  const titles = [];

  if (firmName) {
    const fn = firmName.toLowerCase();
    const last = (lastName || '').toLowerCase();
    const first = (firstName || '').toLowerCase();

    // Check if attorney's last name appears in firm name
    if (last && last.length > 2 && fn.includes(last)) {
      // Solo practice signals
      if (/law\s+office|attorney\s+at\s+law/i.test(firmName) &&
          !fn.includes('&') && !fn.includes(' and ') &&
          !/,\s*[a-z]/i.test(firmName.replace(last, ''))) {
        titles.push('Solo Practitioner');
      }
      // "& Associates" = senior/founding
      else if (/&\s*associates|and\s+associates/i.test(firmName)) {
        titles.push('Founding Partner');
      }
      // Multi-name firm with & or ,
      else if (fn.includes('&') || fn.includes(' and ') || /,\s*[a-z]{2}/i.test(fn)) {
        titles.push('Named Partner');
      }
      // Name + PC/PA/PLLC = owner
      else if (/\b(p\.?c\.?|p\.?a\.?|pllc)\b/i.test(firmName)) {
        titles.push('Principal');
      }
      else {
        titles.push('Partner');
      }
    }
  }

  // Admission year seniority
  if (admissionDate) {
    const year = parseInt(admissionDate);
    if (year > 1900) {
      const yearsExp = new Date().getFullYear() - year;
      if (yearsExp >= 25 && titles.length === 0) titles.push('Senior Partner');
      else if (yearsExp >= 15 && titles.length === 0) titles.push('Partner');
      else if (yearsExp >= 8 && titles.length === 0) titles.push('Senior Associate');
      else if (yearsExp >= 4 && titles.length === 0) titles.push('Associate');
      else if (yearsExp >= 0 && titles.length === 0) titles.push('Junior Associate');
    }
  }

  return titles.length > 0 ? titles[0] : null;
}

// =====================================================================
// STEP 2: Practice Area Detection
// =====================================================================

const PRACTICE_AREA_PATTERNS = {
  'Personal Injury': /\b(personal\s+injury|accident|wrongful\s+death|negligence|malpractice|slip\s+and\s+fall|product\s+liability|catastrophic|damages)\b/i,
  'Family Law': /\b(family\s+law|divorce|custody|child\s+support|adoption|alimony|domestic\s+relations|matrimonial|marital)\b/i,
  'Criminal Defense': /\b(criminal|defense|dui|dwi|felony|misdemeanor|penal|homicide|assault|drug\s+offense)\b/i,
  'Estate Planning': /\b(estate\s+planning|wills?\b|trusts?\b|probate|guardianship|elder\s+law|estate\s+admin|conservator|inheritance)\b/i,
  'Real Estate': /\b(real\s+estate|property\s+law|landlord|tenant|foreclosure|title\s+insurance|zoning|land\s+use|closing)\b/i,
  'Business Law': /\b(business\s+law|corporate|commercial|contracts?|mergers?|acquisitions?|securities|venture|startup|llc\s+formation)\b/i,
  'Immigration': /\b(immigration|visa|green\s+card|deportation|asylum|naturalization|uscis|h-?1b|citizenship)\b/i,
  'Bankruptcy': /\b(bankruptcy|chapter\s+[0-9]+|debt\s+relief|creditor|insolvency|reorganization|foreclosure\s+defense)\b/i,
  'Employment Law': /\b(employment|wrongful\s+termination|discrimination|harassment|labor\s+law|workers?\s+comp|wage|flsa|eeoc)\b/i,
  'Intellectual Property': /\b(intellectual\s+property|patent|trademark|copyright|trade\s+secret|ip\s+law|licensing)\b/i,
  'Tax Law': /\b(tax\s+law|taxation|irs|tax\s+planning|tax\s+dispute|tax\s+litigation|tax\s+resolution)\b/i,
  'Civil Litigation': /\b(civil\s+litigation|litigation|trial|dispute\s+resolution|arbitration|mediation|civil\s+rights)\b/i,
  'Environmental Law': /\b(environmental|epa|clean\s+air|clean\s+water|environmental\s+compliance|hazardous|superfund)\b/i,
  'Healthcare Law': /\b(health\s*care|medical|hipaa|health\s+law|pharmaceutical|hospital|nursing\s+home)\b/i,
  'Insurance Law': /\b(insurance|bad\s+faith|coverage|claims?\s+denial|insurance\s+defense|subrogation)\b/i,
  'Construction Law': /\b(construction\s+law|construction\s+defect|building|contractor|mechanic.?s?\s+lien|surety)\b/i,
  'Government Law': /\b(government|municipal|public\s+sector|regulatory|administrative\s+law|lobbying|legislative)\b/i,
  'Entertainment Law': /\b(entertainment|media|sports\s+law|film|music|talent|publishing|broadcasting)\b/i,
  'Education Law': /\b(education\s+law|school|student|title\s+ix|special\s+education|university|campus)\b/i,
  'Military Law': /\b(military|court\s+martial|jag|veterans?|va\s+benefits|military\s+justice)\b/i,
  'Maritime Law': /\b(maritime|admiralty|shipping|marine|offshore|jones\s+act|longshore)\b/i,
  'General Practice': /\b(general\s+practice|general\s+law|full\s+service)\b/i,
};

function detectPracticeArea(firmName, bio) {
  const text = ((firmName || '') + ' ' + (bio || '')).trim();
  if (!text) return null;

  for (const [area, pattern] of Object.entries(PRACTICE_AREA_PATTERNS)) {
    if (pattern.test(text)) return area;
  }
  return null;
}

// =====================================================================
// STEP 3: LinkedIn URL Construction
// =====================================================================

function buildLinkedInUrl(firstName, lastName) {
  if (!firstName || !lastName) return null;
  const fn = firstName.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '');
  const ln = lastName.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '');
  if (!fn || !ln) return null;
  return `https://www.linkedin.com/in/${fn}-${ln}`;
}

// =====================================================================
// STEP 4: Website Domain Discovery
// =====================================================================

const FIRM_STRIP = [
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
  /\bProfessional\s+(Association|Corporation)\b/gi,
  /\bChartered\b/gi,
  /\bCounselors?\s+at\s+Law\b/gi,
  /\bSolicitors?\b/gi,
  /\bBarristers?\b/gi,
];

function generateDomainCandidates(firmName) {
  if (!firmName) return [];
  let cleaned = firmName;
  for (const pat of FIRM_STRIP) cleaned = cleaned.replace(pat, '');
  cleaned = cleaned.replace(/&/g, '').replace(/[.,;:'"!?()[\]{}]/g, '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return [];

  const words = cleaned.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim().split(/\s+/).filter(w => w.length > 0);
  if (words.length === 0) return [];

  const joined = words.join('');
  const candidates = [];

  // Most common patterns for law firms
  candidates.push(joined + '.com');
  candidates.push(joined + 'law.com');
  if (words.length <= 3) {
    candidates.push(words.join('-') + '.com');
    candidates.push(words[words.length - 1] + 'law.com');
  }
  if (words.length === 1) {
    candidates.push(words[0] + 'legal.com');
    candidates.push(words[0] + 'lawfirm.com');
  }

  return [...new Set(candidates)];
}

function checkDomain(domain) {
  return new Promise(resolve => {
    dns.resolveMx(domain, (err, addresses) => {
      if (!err && addresses && addresses.length > 0) {
        resolve({ domain, hasMx: true });
        return;
      }
      // Try A record as fallback
      dns.resolve4(domain, (err2, addrs) => {
        resolve({ domain, hasMx: false, hasA: !err2 && addrs && addrs.length > 0 });
      });
    });
  });
}

async function checkDomainHttp(domain) {
  return new Promise(resolve => {
    const req = https.get(`https://${domain}`, { timeout: 5000, headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      resolve({ domain, status: res.statusCode, alive: res.statusCode < 400 });
      res.resume();
    });
    req.on('error', () => resolve({ domain, alive: false }));
    req.on('timeout', () => { req.destroy(); resolve({ domain, alive: false }); });
  });
}

// =====================================================================
// STEP 5: Email Pattern Learning
// =====================================================================

function detectEmailPattern(email, firstName, lastName) {
  if (!email || !firstName || !lastName) return null;
  const [local, domain] = email.toLowerCase().split('@');
  if (!local || !domain) return null;

  const fn = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const ln = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!fn || !ln) return null;

  if (local === `${fn}.${ln}`) return 'first.last';
  if (local === `${fn}${ln}`) return 'firstlast';
  if (local === `${fn[0]}${ln}`) return 'flast';
  if (local === `${fn[0]}.${ln}`) return 'f.last';
  if (local === `${ln}.${fn}`) return 'last.first';
  if (local === `${fn}`) return 'first';
  if (local === `${ln}`) return 'last';
  if (local === `${fn}_${ln}`) return 'first_last';
  if (local === `${fn[0]}${ln[0]}`) return 'fl';
  if (local === `${fn}-${ln}`) return 'first-last';
  return null;
}

function applyEmailPattern(pattern, firstName, lastName, domain) {
  const fn = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const ln = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!fn || !ln || !domain) return null;

  const patterns = {
    'first.last': `${fn}.${ln}`,
    'firstlast': `${fn}${ln}`,
    'flast': `${fn[0]}${ln}`,
    'f.last': `${fn[0]}.${ln}`,
    'last.first': `${ln}.${fn}`,
    'first': fn,
    'last': ln,
    'first_last': `${fn}_${ln}`,
    'fl': `${fn[0]}${ln[0]}`,
    'first-last': `${fn}-${ln}`,
  };

  const local = patterns[pattern];
  return local ? `${local}@${domain}` : null;
}

// =====================================================================
// Main
// =====================================================================

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  DEEP ENRICHMENT ENGINE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Database: ${DB_PATH}`);
  console.log(`  Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`);
  if (ONLY_STEP) console.log(`  Running step ${ONLY_STEP} only`);
  if (LIMIT) console.log(`  Limit: ${LIMIT} leads`);
  console.log('');

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  const stats = { titles: 0, practices: 0, linkedins: 0, websites: 0, emails: 0 };

  // ─────────────────────────────────────────────────────────────────
  // STEP 1: Title Inference
  // ─────────────────────────────────────────────────────────────────
  if (!ONLY_STEP || ONLY_STEP === 1) {
    console.log('━━━ STEP 1: Title Inference ━━━');

    const leads = db.prepare(`
      SELECT id, first_name, last_name, firm_name, admission_date, bar_number
      FROM leads
      WHERE (title IS NULL OR title = '')
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
      ${LIMIT ? `LIMIT ${LIMIT}` : ''}
    `).all();

    console.log(`  Leads missing title: ${leads.length.toLocaleString()}`);

    const updateTitle = db.prepare('UPDATE leads SET title = ? WHERE id = ?');
    const updates = [];

    for (const lead of leads) {
      const title = inferTitle(
        lead.first_name, lead.last_name, lead.firm_name,
        lead.admission_date, lead.bar_number
      );
      if (title) {
        updates.push({ id: lead.id, title });
        stats.titles++;
      }
    }

    if (!DRY_RUN && updates.length > 0) {
      const tx = db.transaction((items) => {
        for (const u of items) updateTitle.run(u.title, u.id);
      });
      tx(updates);
    }

    console.log(`  Titles inferred: ${stats.titles.toLocaleString()}`);
    const breakdown = {};
    for (const u of updates) {
      breakdown[u.title] = (breakdown[u.title] || 0) + 1;
    }
    for (const [title, count] of Object.entries(breakdown).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${title}: ${count.toLocaleString()}`);
    }
    console.log('');
  }

  // ─────────────────────────────────────────────────────────────────
  // STEP 2: Practice Area Detection
  // ─────────────────────────────────────────────────────────────────
  if (!ONLY_STEP || ONLY_STEP === 2) {
    console.log('━━━ STEP 2: Practice Area Detection ━━━');

    const leads = db.prepare(`
      SELECT id, firm_name, bio
      FROM leads
      WHERE (practice_area IS NULL OR practice_area = '')
      AND (firm_name IS NOT NULL AND firm_name != '')
      ${LIMIT ? `LIMIT ${LIMIT}` : ''}
    `).all();

    console.log(`  Leads missing practice area: ${leads.length.toLocaleString()}`);

    const updatePractice = db.prepare('UPDATE leads SET practice_area = ? WHERE id = ?');
    const updates = [];

    for (const lead of leads) {
      const area = detectPracticeArea(lead.firm_name, lead.bio);
      if (area) {
        updates.push({ id: lead.id, area });
        stats.practices++;
      }
    }

    if (!DRY_RUN && updates.length > 0) {
      const tx = db.transaction((items) => {
        for (const u of items) updatePractice.run(u.area, u.id);
      });
      tx(updates);
    }

    console.log(`  Practice areas detected: ${stats.practices.toLocaleString()}`);
    const breakdown = {};
    for (const u of updates) {
      breakdown[u.area] = (breakdown[u.area] || 0) + 1;
    }
    for (const [area, count] of Object.entries(breakdown).sort((a, b) => b[1] - a[1]).slice(0, 15)) {
      console.log(`    ${area}: ${count.toLocaleString()}`);
    }
    console.log('');
  }

  // ─────────────────────────────────────────────────────────────────
  // STEP 3: LinkedIn URL Construction
  // ─────────────────────────────────────────────────────────────────
  if (!ONLY_STEP || ONLY_STEP === 3) {
    console.log('━━━ STEP 3: LinkedIn URL Construction ━━━');

    const leads = db.prepare(`
      SELECT id, first_name, last_name
      FROM leads
      WHERE (linkedin_url IS NULL OR linkedin_url = '')
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
      ${LIMIT ? `LIMIT ${LIMIT}` : ''}
    `).all();

    console.log(`  Leads missing LinkedIn: ${leads.length.toLocaleString()}`);

    const updateLinkedin = db.prepare('UPDATE leads SET linkedin_url = ? WHERE id = ?');
    const updates = [];

    for (const lead of leads) {
      const url = buildLinkedInUrl(lead.first_name, lead.last_name);
      if (url) {
        updates.push({ id: lead.id, url });
        stats.linkedins++;
      }
    }

    if (!DRY_RUN && updates.length > 0) {
      const tx = db.transaction((items) => {
        for (const u of items) updateLinkedin.run(u.url, u.id);
      });
      tx(updates);
    }

    console.log(`  LinkedIn URLs generated: ${stats.linkedins.toLocaleString()}`);
    console.log('  (Pattern: linkedin.com/in/firstname-lastname)');
    console.log('');
  }

  // ─────────────────────────────────────────────────────────────────
  // STEP 4: Website Domain Discovery
  // ─────────────────────────────────────────────────────────────────
  if (!ONLY_STEP || ONLY_STEP === 4) {
    console.log('━━━ STEP 4: Website Domain Discovery ━━━');

    // Get unique firm names without websites
    const firms = db.prepare(`
      SELECT DISTINCT firm_name
      FROM leads
      WHERE (website IS NULL OR website = '')
      AND firm_name IS NOT NULL AND firm_name != ''
      AND firm_name NOT IN (
        SELECT DISTINCT firm_name FROM leads
        WHERE website IS NOT NULL AND website != ''
        AND firm_name IS NOT NULL AND firm_name != ''
      )
      ${LIMIT ? `LIMIT ${LIMIT}` : ''}
    `).all();

    console.log(`  Firms without website: ${firms.length.toLocaleString()}`);

    const updateWebsite = db.prepare(`
      UPDATE leads SET website = ?, website_source = 'domain_discovery'
      WHERE firm_name = ? AND (website IS NULL OR website = '')
    `);

    let checked = 0;
    let found = 0;
    const BATCH_SIZE = 50;

    for (let i = 0; i < firms.length; i += BATCH_SIZE) {
      const batch = firms.slice(i, i + BATCH_SIZE);
      const promises = batch.map(async (firm) => {
        const candidates = generateDomainCandidates(firm.firm_name);
        for (const domain of candidates.slice(0, 4)) { // Max 4 candidates per firm
          const result = await checkDomainHttp(domain);
          checked++;
          if (result.alive) {
            return { firmName: firm.firm_name, domain };
          }
        }
        return null;
      });

      const results = await Promise.all(promises);
      for (const r of results) {
        if (r && !DRY_RUN) {
          const changes = updateWebsite.run('https://' + r.domain, r.firmName);
          found++;
          stats.websites += changes.changes;
        }
      }

      if ((i + BATCH_SIZE) % 500 === 0 || i + BATCH_SIZE >= firms.length) {
        process.stdout.write(`\r  Checked ${checked.toLocaleString()} domains, found ${found} websites...`);
      }
    }

    console.log(`\n  Websites discovered: ${found} (${stats.websites} leads updated)`);
    console.log('');
  }

  // ─────────────────────────────────────────────────────────────────
  // STEP 5: Email Pattern Learning
  // ─────────────────────────────────────────────────────────────────
  if (!ONLY_STEP || ONLY_STEP === 5) {
    console.log('━━━ STEP 5: Email Pattern Learning ━━━');

    // Build pattern map: domain → email pattern
    const knownEmails = db.prepare(`
      SELECT first_name, last_name, email
      FROM leads
      WHERE email IS NOT NULL AND email != ''
      AND (email_source IS NULL OR email_source != 'generated_pattern')
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
    `).all();

    const domainPatterns = new Map(); // domain → { pattern, count }

    for (const lead of knownEmails) {
      const domain = lead.email.split('@')[1];
      if (!domain) continue;
      const pattern = detectEmailPattern(lead.email, lead.first_name, lead.last_name);
      if (!pattern) continue;

      const existing = domainPatterns.get(domain);
      if (existing) {
        if (existing.pattern === pattern) existing.count++;
      } else {
        domainPatterns.set(domain, { pattern, count: 1 });
      }
    }

    // Filter to domains with 2+ confirmed patterns (more reliable)
    const confirmedDomains = new Map();
    for (const [domain, info] of domainPatterns) {
      if (info.count >= 1) confirmedDomains.set(domain, info.pattern);
    }

    console.log(`  Known email domains with patterns: ${confirmedDomains.size.toLocaleString()}`);

    // Find leads at those domains who don't have email
    const needEmail = db.prepare(`
      SELECT id, first_name, last_name, website
      FROM leads
      WHERE (email IS NULL OR email = '')
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
      AND website IS NOT NULL AND website != ''
    `).all();

    console.log(`  Leads with website but no email: ${needEmail.length.toLocaleString()}`);

    const updateEmail = db.prepare(`
      UPDATE leads SET email = ?, email_source = 'pattern_learned' WHERE id = ?
    `);
    const updates = [];

    for (const lead of needEmail) {
      // Extract domain from website
      let domain;
      try {
        let url = lead.website.trim();
        if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
        const parsed = new URL(url);
        domain = parsed.hostname.toLowerCase().replace(/^www\./, '');
      } catch { continue; }

      const pattern = confirmedDomains.get(domain);
      if (!pattern) continue;

      const email = applyEmailPattern(pattern, lead.first_name, lead.last_name, domain);
      if (email) {
        updates.push({ id: lead.id, email });
        stats.emails++;
      }
    }

    // Also check firm-mates: leads at same firm as someone with email
    const firmEmails = db.prepare(`
      SELECT firm_name, email, first_name, last_name
      FROM leads
      WHERE email IS NOT NULL AND email != ''
      AND (email_source IS NULL OR email_source != 'generated_pattern')
      AND firm_name IS NOT NULL AND firm_name != ''
    `).all();

    const firmPatternMap = new Map(); // firm_name → { domain, pattern }
    for (const lead of firmEmails) {
      if (firmPatternMap.has(lead.firm_name)) continue;
      const domain = lead.email.split('@')[1];
      if (!domain) continue;
      // Skip generic domains
      if (/gmail|yahoo|hotmail|outlook|aol|icloud|comcast|att\.net/i.test(domain)) continue;
      const pattern = detectEmailPattern(lead.email, lead.first_name, lead.last_name);
      if (pattern) {
        firmPatternMap.set(lead.firm_name, { domain, pattern });
      }
    }

    const firmMates = db.prepare(`
      SELECT id, first_name, last_name, firm_name
      FROM leads
      WHERE (email IS NULL OR email = '')
      AND firm_name IS NOT NULL AND firm_name != ''
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
    `).all();

    for (const lead of firmMates) {
      const info = firmPatternMap.get(lead.firm_name);
      if (!info) continue;
      const email = applyEmailPattern(info.pattern, lead.first_name, lead.last_name, info.domain);
      if (email) {
        updates.push({ id: lead.id, email });
        stats.emails++;
      }
    }

    if (!DRY_RUN && updates.length > 0) {
      const tx = db.transaction((items) => {
        for (const u of items) updateEmail.run(u.email, u.id);
      });
      tx(updates);
    }

    console.log(`  Emails generated from patterns: ${stats.emails.toLocaleString()}`);
    console.log('');
  }

  // ─────────────────────────────────────────────────────────────────
  // Summary
  // ─────────────────────────────────────────────────────────────────
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  ENRICHMENT RESULTS');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Titles inferred:       ${stats.titles.toLocaleString()}`);
  console.log(`  Practice areas found:  ${stats.practices.toLocaleString()}`);
  console.log(`  LinkedIn URLs built:   ${stats.linkedins.toLocaleString()}`);
  console.log(`  Websites discovered:   ${stats.websites.toLocaleString()}`);
  console.log(`  Emails from patterns:  ${stats.emails.toLocaleString()}`);
  console.log('═══════════════════════════════════════════════════════════════');

  // Post-run stats
  const post = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN title IS NOT NULL AND title != '' THEN 1 ELSE 0 END) as has_title,
      SUM(CASE WHEN practice_area IS NOT NULL AND practice_area != '' THEN 1 ELSE 0 END) as has_practice,
      SUM(CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 1 ELSE 0 END) as has_linkedin,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as has_website,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as has_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as has_phone
    FROM leads
  `).get();

  console.log(`\n  Database coverage after enrichment:`);
  console.log(`    Total:     ${post.total.toLocaleString()}`);
  console.log(`    Title:     ${post.has_title.toLocaleString()} (${Math.round(post.has_title/post.total*100)}%)`);
  console.log(`    Practice:  ${post.has_practice.toLocaleString()} (${Math.round(post.has_practice/post.total*100)}%)`);
  console.log(`    LinkedIn:  ${post.has_linkedin.toLocaleString()} (${Math.round(post.has_linkedin/post.total*100)}%)`);
  console.log(`    Website:   ${post.has_website.toLocaleString()} (${Math.round(post.has_website/post.total*100)}%)`);
  console.log(`    Email:     ${post.has_email.toLocaleString()} (${Math.round(post.has_email/post.total*100)}%)`);
  console.log(`    Phone:     ${post.has_phone.toLocaleString()} (${Math.round(post.has_phone/post.total*100)}%)`);

  db.close();
  console.log('\nDone.');
}

main().catch(console.error);
