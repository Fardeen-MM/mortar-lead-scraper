#!/usr/bin/env node
/**
 * Decision-Maker Scorer
 *
 * Scores every lead in the SQLite database for decision-maker likelihood
 * and writes the result into the `icp_score` column.
 *
 * Scoring rules (additive, theoretical max ~150):
 *   1. Named partner detection              +40
 *   2. Solo practitioner signals             +35
 *   3. Senior partner indicators             +30
 *   4. Title-based scoring                   +5 to +50
 *   5. Seniority by admission date           +0 to +30
 *   6. Firm structure signals                +10 to +15
 *
 * Usage:  node scripts/decision-maker-scorer.js
 */

const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const BATCH_SIZE = 1000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Normalise a string for safe comparison (lowercase, collapse whitespace, strip punctuation) */
function norm(s) {
  if (!s) return '';
  return s.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
}

/** Check if `haystack` contains `needle` (both normalised) */
function firmContainsName(firmNorm, name) {
  if (!firmNorm || !name || name.length < 2) return false;
  return firmNorm.includes(norm(name));
}

/** Parse a year from admission_date (supports "2004", "2005-09-16", etc.) */
function parseAdmissionYear(admissionDate) {
  if (!admissionDate) return null;
  const m = admissionDate.match(/(\d{4})/);
  return m ? parseInt(m[1], 10) : null;
}

// ---------------------------------------------------------------------------
// Scoring functions
// ---------------------------------------------------------------------------

function scoreLead(lead) {
  let score = 0;
  const reasons = [];

  const firstName = (lead.first_name || '').trim();
  const lastName = (lead.last_name || '').trim();
  const firmName = (lead.firm_name || '').trim();
  const title = (lead.title || '').trim();
  const admissionDate = (lead.admission_date || '').trim();

  const firmNorm = norm(firmName);
  const firstNorm = norm(firstName);
  const lastNorm = norm(lastName);
  const fullNameNorm = norm(firstName + ' ' + lastName);
  const titleLower = title.toLowerCase();

  // Skip leads with no name
  if (!lastName && !firstName) return { score: 0, reasons: ['no name'] };

  // -------------------------------------------------------------------------
  // 1. Named partner detection (+40)
  //    Last name appears in firm name, OR "Law Office(s) of First Last"
  // -------------------------------------------------------------------------
  let isNamedPartner = false;
  if (lastNorm.length >= 2 && firmContainsName(firmNorm, lastNorm)) {
    isNamedPartner = true;
  }
  // Also check "Law Office of [First Last]" / "Law Offices of [First Last]"
  const lawOfficePattern = /law offices? of/i;
  if (lawOfficePattern.test(firmName) && firmContainsName(firmNorm, fullNameNorm)) {
    isNamedPartner = true;
  }

  // -------------------------------------------------------------------------
  // 2. Solo practitioner signals (+35)
  //    Firm = "Law Office(s) of ..." with lead's name, OR firm IS the person's name
  // -------------------------------------------------------------------------
  let isSolo = false;

  // "Law Office of [name]" pattern
  if (lawOfficePattern.test(firmName) && firmContainsName(firmNorm, lastNorm)) {
    isSolo = true;
  }

  // Firm name is just the person's name (possibly with "Mr.", "Ms.", middle initials, etc.)
  // Normalise both and compare
  const firmClean = firmNorm
    .replace(/\b(mr|mrs|ms|dr|esq|jr|sr|ii|iii|iv|pa|pc|llc|pllc|llp)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const nameClean = fullNameNorm
    .replace(/\b(mr|mrs|ms|dr|esq|jr|sr|ii|iii|iv)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (firmClean && nameClean && (firmClean === nameClean || firmClean.startsWith(nameClean) || nameClean.startsWith(firmClean))) {
    // Firm is essentially the person's name
    if (firmClean.length >= 4 && nameClean.length >= 4) {
      isSolo = true;
    }
  }

  // -------------------------------------------------------------------------
  // 3. Senior partner indicators (+30)
  //    Firm contains "& Associates" or "& Partners" AND lead's name is in firm
  // -------------------------------------------------------------------------
  let isSeniorPartner = false;
  if (/&\s*(associates|partners)/i.test(firmName) && isNamedPartner) {
    isSeniorPartner = true;
  }

  // Apply partner/solo/senior scores (non-overlapping — take highest applicable)
  // Solo gets both solo signal + named partner since they usually overlap
  if (isSolo) {
    score += 35;
    reasons.push('solo practitioner (+35)');
    // Solo practitioners with "& Associates" are also senior partners
    if (isSeniorPartner) {
      score += 30;
      reasons.push('senior partner w/ associates (+30)');
    } else if (isNamedPartner) {
      score += 40;
      reasons.push('named partner (+40)');
    }
  } else if (isSeniorPartner) {
    score += 30;
    reasons.push('senior partner w/ associates (+30)');
    score += 40;
    reasons.push('named partner (+40)');
  } else if (isNamedPartner) {
    score += 40;
    reasons.push('named partner (+40)');
  }

  // -------------------------------------------------------------------------
  // 4. Title-based scoring
  // -------------------------------------------------------------------------
  if (titleLower) {
    if (/\b(partner|principal|owner|founding|managing)\b/i.test(titleLower)) {
      score += 50;
      reasons.push('title: partner/principal/owner/founding/managing (+50)');
    } else if (/\bof\s+counsel\b/i.test(titleLower)) {
      score += 20;
      reasons.push('title: of counsel (+20)');
    } else if (/\b(associate|staff)\b/i.test(titleLower)) {
      score += 5;
      reasons.push('title: associate/staff (+5)');
    }
  }

  // -------------------------------------------------------------------------
  // 5. Seniority by admission date
  // -------------------------------------------------------------------------
  const admYear = parseAdmissionYear(admissionDate);
  if (admYear) {
    const currentYear = new Date().getFullYear();
    const yearsAdmitted = currentYear - admYear;
    if (yearsAdmitted >= 20) {
      score += 30;
      reasons.push(`seniority: ${yearsAdmitted}yr (+30)`);
    } else if (yearsAdmitted >= 10) {
      score += 20;
      reasons.push(`seniority: ${yearsAdmitted}yr (+20)`);
    } else if (yearsAdmitted >= 5) {
      score += 10;
      reasons.push(`seniority: ${yearsAdmitted}yr (+10)`);
    } else {
      reasons.push(`seniority: ${yearsAdmitted}yr (+0)`);
    }
  }

  // -------------------------------------------------------------------------
  // 6. Firm structure signals
  // -------------------------------------------------------------------------
  if (/\bP\.?A\.?\b/i.test(firmName) || /\bP\.?C\.?\b/i.test(firmName)) {
    score += 15;
    reasons.push('firm: PA/PC professional corp (+15)');
  } else if (/\bLLP\b/i.test(firmName)) {
    score += 10;
    reasons.push('firm: LLP (+10)');
  } else if (/\bPLLC\b/i.test(firmName)) {
    score += 10;
    reasons.push('firm: PLLC (+10)');
  } else if (/\bLLC\b/i.test(firmName)) {
    score += 10;
    reasons.push('firm: LLC (+10)');
  }

  return { score, reasons };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
  console.log('=== Decision-Maker Scorer ===');
  console.log(`Database: ${DB_PATH}`);

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Ensure icp_score column exists
  try {
    db.prepare('SELECT icp_score FROM leads LIMIT 1').get();
  } catch {
    db.exec('ALTER TABLE leads ADD COLUMN icp_score INTEGER DEFAULT 0');
    console.log('Added icp_score column to leads table.');
  }

  const totalLeads = db.prepare('SELECT COUNT(*) as c FROM leads').get().c;
  console.log(`Total leads to score: ${totalLeads}\n`);

  const selectStmt = db.prepare(
    'SELECT id, first_name, last_name, firm_name, title, admission_date FROM leads LIMIT ? OFFSET ?'
  );
  const updateStmt = db.prepare('UPDATE leads SET icp_score = ? WHERE id = ?');

  // Wrap updates in a transaction per batch
  const updateBatch = db.transaction((updates) => {
    for (const { id, score } of updates) {
      updateStmt.run(score, id);
    }
  });

  let processed = 0;
  let offset = 0;

  // Score distribution tracking
  const distribution = {
    '0': 0,
    '1-24': 0,
    '25-49': 0,
    '50-74': 0,
    '75-99': 0,
    '100-124': 0,
    '125-150': 0,
  };

  const topLeads = []; // Keep top 20

  while (offset < totalLeads) {
    const batch = selectStmt.all(BATCH_SIZE, offset);
    if (batch.length === 0) break;

    const updates = [];

    for (const lead of batch) {
      const { score, reasons } = scoreLead(lead);
      updates.push({ id: lead.id, score });

      // Track distribution
      if (score === 0) distribution['0']++;
      else if (score < 25) distribution['1-24']++;
      else if (score < 50) distribution['25-49']++;
      else if (score < 75) distribution['50-74']++;
      else if (score < 100) distribution['75-99']++;
      else if (score < 125) distribution['100-124']++;
      else distribution['125-150']++;

      // Track top leads
      if (topLeads.length < 20 || score > topLeads[topLeads.length - 1].score) {
        topLeads.push({
          id: lead.id,
          name: `${lead.first_name || ''} ${lead.last_name || ''}`.trim(),
          firm: lead.firm_name || '',
          title: lead.title || '',
          score,
          reasons,
        });
        topLeads.sort((a, b) => b.score - a.score);
        if (topLeads.length > 20) topLeads.length = 20;
      }
    }

    updateBatch(updates);
    processed += batch.length;
    offset += BATCH_SIZE;

    console.log(`  Scored ${processed}/${totalLeads} leads (batch ${Math.ceil(offset / BATCH_SIZE)})`);
  }

  // Final stats
  console.log('\n=== Score Distribution ===');
  const maxBar = 40;
  const maxCount = Math.max(...Object.values(distribution), 1);
  for (const [range, count] of Object.entries(distribution)) {
    const bar = '#'.repeat(Math.round((count / maxCount) * maxBar));
    const pct = ((count / totalLeads) * 100).toFixed(1);
    console.log(`  ${range.padStart(7)}: ${String(count).padStart(6)} (${pct.padStart(5)}%) ${bar}`);
  }

  const nonZero = totalLeads - distribution['0'];
  console.log(`\n  Total leads scored > 0: ${nonZero} / ${totalLeads} (${((nonZero / totalLeads) * 100).toFixed(1)}%)`);
  console.log(`  Mean score (all): ${(topLeads.length > 0 ? 'see below' : 'N/A')}`);

  // Compute mean from DB
  const avg = db.prepare('SELECT AVG(icp_score) as avg, MAX(icp_score) as max FROM leads').get();
  console.log(`  Average ICP score: ${avg.avg ? avg.avg.toFixed(1) : 0}`);
  console.log(`  Max ICP score: ${avg.max || 0}`);

  console.log('\n=== Top 20 Decision-Maker Leads ===');
  console.log('─'.repeat(110));
  console.log(
    '  #  ' +
    'Score  ' +
    'Name'.padEnd(25) +
    'Firm'.padEnd(40) +
    'Reasons'
  );
  console.log('─'.repeat(110));
  topLeads.forEach((l, i) => {
    const num = String(i + 1).padStart(3);
    const sc = String(l.score).padStart(4);
    const nm = l.name.substring(0, 24).padEnd(25);
    const fm = l.firm.substring(0, 39).padEnd(40);
    const rs = l.reasons.join(', ');
    console.log(`${num}  ${sc}  ${nm}${fm}${rs}`);
  });
  console.log('─'.repeat(110));

  db.close();
  console.log('\nDone. All leads scored and saved to icp_score column.');
}

main();
