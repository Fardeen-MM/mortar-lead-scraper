#!/usr/bin/env node
/**
 * practice-area-inferrer.js
 *
 * Fills the practice_area column for all leads in the SQLite database.
 *
 * Inference strategy (checked in order — first match wins):
 *   1. If practice_specialties is already populated -> normalize and use it
 *   2. Firm name pattern matching (case-insensitive, whole-word-bounded)
 *   3. Bio text matching (same keyword patterns)
 *   4. If no match -> leave blank (don't guess)
 *
 * FALSE-POSITIVE PREVENTION:
 *   - Industry-ambiguous words (insurance, construction, entertainment, etc.)
 *     are ONLY matched when paired with legal context words (law, lawyer,
 *     attorney, legal, counsel, defense/defence, litigation).
 *   - Single words like "injury", "criminal", "family" are safe because
 *     non-law-firms rarely use them in their names, but we still use \b
 *     word boundaries to avoid substring matches (e.g. "Westbury" != "bury").
 *   - Firm names like "Greenfield", "Kirkland", "Holland" are NOT matched
 *     for "environmental", "land", etc. because we use word boundaries.
 *
 * Usage:
 *   node scripts/practice-area-inferrer.js
 */

const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');

// ---------------------------------------------------------------------------
// Pattern builder helpers
// ---------------------------------------------------------------------------

/**
 * Escape regex special characters in a literal string.
 */
function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Build a regex that matches any of the given literal phrases,
 * bounded by word boundaries (or equivalent for non-word-char edges).
 */
function anyOf(phrases) {
  const alts = phrases.map(p => {
    const esc = escapeRegex(p);
    const pre = /^\w/.test(p) ? '\\b' : '(?:^|\\s)';
    const suf = /\w$/.test(p) ? '\\b' : '(?:$|\\s)';
    return pre + esc + suf;
  });
  return new RegExp(alts.join('|'), 'i');
}

/**
 * Build a regex requiring keyword(s) NEAR legal-context words.
 * Matches: "<keyword> ... law/lawyer/attorney/legal/counsel" or
 *          "law/lawyer/attorney/legal/counsel ... <keyword>"
 * within the same firm name (which is typically short).
 *
 * This prevents matching "Nationwide Insurance" (no legal word)
 * while matching "Your Insurance Attorney, PLLC" (has "attorney").
 */
function withLegalContext(keywords) {
  // Legal context words that signal this is a law practice, not an industry company
  const legalWords = '(?:law|lawyer|lawyers|attorney|attorneys|legal|counsel|litigation|defense|defence|advocate|advocates|solicitor|solicitors|barrister|barristers)';
  const kwPattern = keywords.map(k => escapeRegex(k)).join('|');

  // keyword ... legal  OR  legal ... keyword  (within same string)
  const fwd = `(?:${kwPattern})\\b[\\s\\S]*?\\b${legalWords}`;
  const bwd = `${legalWords}\\b[\\s\\S]*?\\b(?:${kwPattern})`;
  return new RegExp(`(?:${fwd}|${bwd})`, 'i');
}

// ---------------------------------------------------------------------------
// Practice area rules
//
// Two tiers:
//   SAFE_RULES: Patterns specific enough that they can match anywhere
//               (firm name, bio) without legal-context requirement.
//   GUARDED_RULES: Industry-ambiguous words that REQUIRE a legal-context
//                  word nearby when matching firm names.
//
// Order matters — first match wins, so more specific rules come first.
// ---------------------------------------------------------------------------

const SAFE_RULES = [
  // Very specific multi-word phrases — no ambiguity
  ['Medical Malpractice',   anyOf(['medical malpractice', 'med mal', 'medical negligence'])],
  ['Real Estate',           anyOf(['real estate law', 'real estate lawyer', 'real estate attorney', 'real estate legal', 'real property law'])],
  ['Civil Rights',          anyOf(['civil rights', 'civil liberties'])],
  ['Intellectual Property',  anyOf(['intellectual property', 'patent law', 'trademark law', 'copyright law', 'patent attorney', 'patent lawyer'])],
  ['Data Privacy',          anyOf(['data privacy', 'data protection', 'privacy law'])],

  // Personal injury — "injury" in firm names almost always means PI law
  ['Personal Injury',       anyOf(['personal injury', 'injury law', 'injury lawyer', 'injury lawyers', 'injury attorney', 'injury attorneys', 'injury firm', 'accident law', 'accident lawyer', 'accident attorney'])],

  // Family law — "family law" is unambiguous; bare "family" needs guard
  ['Family Law',            anyOf(['family law', 'family lawyer', 'family lawyers', 'family legal', 'divorce law', 'divorce lawyer', 'divorce attorney', 'custody law', 'child support', 'matrimonial'])],

  // Criminal — "criminal" in a firm name nearly always means criminal law
  // NOTE: "defense attorney" alone is too broad (could be any defense); require "criminal" prefix
  ['Criminal Defense',      anyOf(['criminal defense', 'criminal defence', 'criminal law', 'criminal lawyer', 'criminal lawyers', 'dui law', 'dui lawyer', 'dui attorney', 'dwi law', 'dwi lawyer', 'dwi attorney', 'defence solicitor'])],

  // Immigration — always legal context
  ['Immigration',           anyOf(['immigration law', 'immigration lawyer', 'immigration attorney', 'immigration legal', 'visa law'])],

  // Bankruptcy — "bankruptcy law/lawyer/attorney/court" is fine
  ['Bankruptcy',            anyOf(['bankruptcy law', 'bankruptcy lawyer', 'bankruptcy attorney', 'bankruptcy court', 'debt relief law', 'debt relief lawyer'])],

  // Estate planning — multi-word patterns are safe
  ['Estate Planning',       anyOf(['estate planning', 'estate law', 'probate law', 'probate lawyer', 'probate attorney', 'trust law', 'trusts and estates', 'wills and estates', 'wills & estates', 'elder law', 'elder lawyer'])],

  // Corporate — require "law" or similar
  ['Corporate',             anyOf(['corporate law', 'corporate lawyer', 'corporate legal', 'business law', 'business lawyer', 'business attorney', 'commercial law', 'commercial lawyer', 'mergers and acquisitions', 'mergers & acquisitions'])],

  // Tax — require legal context
  ['Tax Law',               anyOf(['tax law', 'tax attorney', 'tax lawyer', 'tax counsel', 'tax legal'])],

  // Employment — require legal context
  ['Employment Law',        anyOf(['employment law', 'employment lawyer', 'employment attorney', 'labor law', 'labour law', 'workers compensation law', 'workplace law', 'workplace lawyer'])],

  // Education — ONLY explicit "education law" phrases; NOT "school of law" (= law schools)
  ['Education Law',         anyOf(['education law', 'education lawyer', 'education attorney'])],

  // Maritime — specific enough
  ['Maritime Law',          anyOf(['maritime law', 'admiralty law', 'admiralty lawyer'])],

  // Military / Veterans — require legal context
  ['Military Law',          anyOf(['military law', 'military lawyer', 'military justice', 'veterans law', 'veterans lawyer', 'jag office', 'military legal'])],

  // International — ONLY match "international law/trade law", NOT bare "international"
  ['International Law',     anyOf(['international law', 'international trade law', 'trade law'])],

  // Healthcare — require legal context
  ['Healthcare Law',        anyOf(['healthcare law', 'health law', 'health care law', 'healthcare lawyer', 'healthcare attorney'])],

  // Environmental — require legal context
  ['Environmental Law',     anyOf(['environmental law', 'environmental lawyer', 'environmental attorney', 'environmental legal'])],

  // Entertainment — explicit phrases only; NOT bare "media" (catches "mediation")
  ['Entertainment Law',     anyOf(['entertainment law', 'media law', 'sports law'])],

  // General litigation — "trial lawyer/attorneys" is clear; bare "litigation" needs context
  ['General Litigation',    anyOf(['trial lawyer', 'trial lawyers', 'trial attorney', 'trial attorneys', 'trial law', 'litigation law', 'litigation lawyer', 'litigation counsel'])],
];

// Guarded rules: match ONLY if a legal-context word appears in the same firm name.
// These catch firms like "Your Insurance Attorney" but skip "Nationwide Insurance".
// Guarded rules: match ONLY if a legal-context word appears in the same firm name.
// These catch firms like "Your Insurance Attorney" but skip "Nationwide Insurance".
// NOTE: "school" is excluded from education (matches "School of Law" = law schools).
// NOTE: "media" is excluded from entertainment (matches "Mediation").
const GUARDED_RULES = [
  ['Insurance Law',         withLegalContext(['insurance'])],
  ['Construction Law',      withLegalContext(['construction'])],
  ['Securities Law',        withLegalContext(['securities'])],
  ['Technology Law',        withLegalContext(['technology', 'cyber', 'cybersecurity'])],
  ['Entertainment Law',     withLegalContext(['entertainment', 'sports'])],
  ['Environmental Law',     withLegalContext(['environmental'])],
  ['Healthcare Law',        withLegalContext(['health', 'healthcare'])],
  ['Real Estate',           withLegalContext(['real estate', 'property', 'title'])],
  ['Corporate',             withLegalContext(['corporate', 'business', 'commercial'])],
  ['Employment Law',        withLegalContext(['employment', 'labor', 'labour', 'workers', 'workplace'])],
  ['Tax Law',               withLegalContext(['tax'])],
  ['International Law',     withLegalContext(['international', 'trade'])],
  ['General Litigation',    withLegalContext(['litigation'])],
];

// Single-word patterns that are SAFE in firm names because they almost
// exclusively appear in legal firm names (not industry companies).
// "Injury Lawyers" — no insurance company calls itself "Injury".
// "Criminal" — ditto. "Immigration" — ditto.
const FIRM_SAFE_SINGLE_WORDS = [
  ['Personal Injury',    anyOf(['injury'])],
  ['Criminal Defense',   anyOf(['criminal'])],
  ['Family Law',         anyOf(['divorce', 'custody'])],
  ['Immigration',        anyOf(['immigration'])],
  ['Bankruptcy',         anyOf(['bankruptcy'])],
  ['Estate Planning',    anyOf(['probate', 'elder'])],
  ['General Litigation', anyOf(['litigation'])],
];

// ---------------------------------------------------------------------------
// Normalize practice_specialties into a clean practice area label
// ---------------------------------------------------------------------------
function normalizeSpecialties(raw) {
  if (!raw || !raw.trim()) return null;

  const text = raw.toLowerCase().trim();

  // Try matching against safe rules
  for (const [label, regex] of SAFE_RULES) {
    if (regex.test(text)) return label;
  }

  // If it's short enough, just title-case it and use as-is
  if (text.length < 60) {
    return text.replace(/\b\w/g, c => c.toUpperCase());
  }

  return null;
}

// ---------------------------------------------------------------------------
// Match against firm name
// ---------------------------------------------------------------------------
function matchFirmName(firmName) {
  if (!firmName || !firmName.trim()) return null;

  // Pass 1: safe multi-word patterns
  for (const [label, regex] of SAFE_RULES) {
    if (regex.test(firmName)) return label;
  }

  // Pass 2: guarded rules (need legal context word nearby)
  for (const [label, regex] of GUARDED_RULES) {
    if (regex.test(firmName)) return label;
  }

  // Pass 3: safe single-word patterns for firm names
  for (const [label, regex] of FIRM_SAFE_SINGLE_WORDS) {
    if (regex.test(firmName)) return label;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Match against bio text
// ---------------------------------------------------------------------------
function matchBio(bio) {
  if (!bio || !bio.trim()) return null;

  // Only use safe rules for bio (multi-word, specific patterns)
  for (const [label, regex] of SAFE_RULES) {
    if (regex.test(bio)) return label;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
function main() {
  console.log('=== Practice Area Inferrer ===');
  console.log(`Database: ${DB_PATH}`);

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  const total = db.prepare('SELECT COUNT(*) as c FROM leads').get().c;
  console.log(`Total leads: ${total}\n`);

  // Fetch all leads
  const leads = db.prepare(`
    SELECT id, practice_area, practice_specialties, firm_name, bio
    FROM leads
  `).all();

  const update = db.prepare(`
    UPDATE leads SET practice_area = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
  `);

  const stats = {
    total: leads.length,
    alreadySet: 0,
    fromSpecialties: 0,
    fromFirmName: 0,
    fromBio: 0,
    noMatch: 0,
  };
  const distribution = {};

  const startTime = Date.now();

  // Use a transaction for performance
  const runBatch = db.transaction((batch) => {
    for (const { id, area } of batch) {
      update.run(area, id);
    }
  });

  let batch = [];
  const BATCH_SIZE = 500;

  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];

    // Progress
    if (i > 0 && i % 2000 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(`  Progress: ${i}/${leads.length} (${elapsed}s elapsed)`);
    }

    // Skip if already has a non-empty practice_area
    if (lead.practice_area && lead.practice_area.trim()) {
      stats.alreadySet++;
      const area = lead.practice_area.trim();
      distribution[area] = (distribution[area] || 0) + 1;
      continue;
    }

    let area = null;

    // 1. Try practice_specialties
    area = normalizeSpecialties(lead.practice_specialties);
    if (area) {
      stats.fromSpecialties++;
    }

    // 2. Try firm name
    if (!area) {
      area = matchFirmName(lead.firm_name);
      if (area) {
        stats.fromFirmName++;
      }
    }

    // 3. Try bio
    if (!area) {
      area = matchBio(lead.bio);
      if (area) {
        stats.fromBio++;
      }
    }

    // 4. No match
    if (!area) {
      stats.noMatch++;
      continue;
    }

    distribution[area] = (distribution[area] || 0) + 1;
    batch.push({ id: lead.id, area });

    if (batch.length >= BATCH_SIZE) {
      runBatch(batch);
      batch = [];
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    runBatch(batch);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // Print results
  console.log('\n=== Results ===');
  console.log(`Total leads:           ${stats.total}`);
  console.log(`Already had area:      ${stats.alreadySet}`);
  console.log(`From specialties:      ${stats.fromSpecialties}`);
  console.log(`From firm name:        ${stats.fromFirmName}`);
  console.log(`From bio:              ${stats.fromBio}`);
  console.log(`No match (left blank): ${stats.noMatch}`);
  console.log(`Time: ${elapsed}s`);

  const filled = stats.alreadySet + stats.fromSpecialties + stats.fromFirmName + stats.fromBio;
  const pct = ((filled / stats.total) * 100).toFixed(1);
  console.log(`\nTotal filled: ${filled}/${stats.total} (${pct}%)`);

  console.log('\n=== Practice Area Distribution ===');
  const sorted = Object.entries(distribution).sort((a, b) => b[1] - a[1]);
  for (const [area, count] of sorted) {
    const bar = '#'.repeat(Math.min(count, 50));
    console.log(`  ${area.padEnd(25)} ${String(count).padStart(5)}  ${bar}`);
  }

  db.close();
  console.log('\nDone.');
}

main();
