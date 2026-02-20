/**
 * Lead Database — persistent SQLite store for all scraped leads
 *
 * Accumulates leads across ALL scraper runs into a single database.
 * Provides deduplication, source tracking, and TAM measurement.
 *
 * Schema:
 *   leads: Core lead data (name, firm, city, state, phone, email, website)
 *   lead_sources: Many-to-many relationship between leads and data sources
 *   scrape_runs: Metadata about each scrape run
 *
 * Dedup keys (checked in order):
 *   1. bar_number + state (strongest — unique per attorney per jurisdiction)
 *   2. email (unique per person)
 *   3. first_name + last_name + city + state (fuzzy match)
 *   4. phone (same phone = same entity)
 */

const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');

/** Derive country code from state code */
function deriveCountry(state) {
  if (!state) return 'US';
  if (state.startsWith('CA-')) return 'CA';
  if (state.startsWith('UK-')) return 'UK';
  if (state.startsWith('AU-')) return 'AU';
  if (state === 'SA') return 'AU'; // South Australia
  if (['FR', 'IE', 'IT', 'DE-BRAK', 'ES'].includes(state)) return state === 'DE-BRAK' ? 'DE' : state;
  if (state === 'HK') return 'HK';
  if (state === 'NZ') return 'NZ';
  if (state === 'SG') return 'SG';
  if (state === 'IN-DL') return 'IN';
  if (state === 'ZA') return 'ZA';
  return 'US';
}

let _db = null;

function getDb() {
  if (_db) return _db;

  _db = new Database(DB_PATH);
  _db.pragma('journal_mode = WAL');

  _db.exec(`
    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      first_name TEXT,
      last_name TEXT,
      firm_name TEXT,
      city TEXT,
      state TEXT,
      country TEXT DEFAULT 'US',
      phone TEXT,
      email TEXT,
      website TEXT,
      bar_number TEXT,
      bar_status TEXT,
      admission_date TEXT,
      practice_area TEXT,
      title TEXT,
      linkedin_url TEXT,
      bio TEXT,
      education TEXT,
      languages TEXT,
      practice_specialties TEXT,
      email_source TEXT,
      phone_source TEXT,
      website_source TEXT,
      email_verified INTEGER DEFAULT 0,
      email_catch_all INTEGER DEFAULT 0,
      primary_source TEXT,
      google_place_id TEXT,
      profile_url TEXT,
      rating REAL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_leads_bar ON leads(bar_number, state);
    CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);
    CREATE INDEX IF NOT EXISTS idx_leads_name_city ON leads(last_name, first_name, city, state);
    CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
    CREATE INDEX IF NOT EXISTS idx_leads_firm_city ON leads(firm_name, city, state);
    CREATE INDEX IF NOT EXISTS idx_leads_state ON leads(state);
    CREATE INDEX IF NOT EXISTS idx_leads_country ON leads(country);

    CREATE TABLE IF NOT EXISTS lead_sources (
      lead_id INTEGER NOT NULL,
      source TEXT NOT NULL,
      scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id),
      UNIQUE(lead_id, source)
    );

    CREATE TABLE IF NOT EXISTS scrape_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      state TEXT,
      source TEXT,
      practice_area TEXT,
      leads_found INTEGER DEFAULT 0,
      leads_new INTEGER DEFAULT 0,
      leads_updated INTEGER DEFAULT 0,
      emails_found INTEGER DEFAULT 0,
      started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      completed_at DATETIME
    );
  `);

  return _db;
}

/**
 * Find an existing lead that matches the given data.
 * Returns the lead ID if found, null if new.
 */
function findExistingLead(lead) {
  const db = getDb();

  // 1. Bar number + state (strongest match)
  if (lead.bar_number && lead.state) {
    const match = db.prepare(
      'SELECT id FROM leads WHERE bar_number = ? AND state = ?'
    ).get(lead.bar_number, lead.state);
    if (match) return match.id;
  }

  // 2. Email match
  if (lead.email) {
    const email = lead.email.toLowerCase().trim();
    const match = db.prepare(
      'SELECT id FROM leads WHERE LOWER(email) = ?'
    ).get(email);
    if (match) return match.id;
  }

  // 3. Name + city + state match
  if (lead.first_name && lead.last_name && lead.city && lead.state) {
    const match = db.prepare(
      'SELECT id FROM leads WHERE LOWER(first_name) = ? AND LOWER(last_name) = ? AND LOWER(city) = ? AND state = ?'
    ).get(
      lead.first_name.toLowerCase().trim(),
      lead.last_name.toLowerCase().trim(),
      lead.city.toLowerCase().trim(),
      lead.state
    );
    if (match) return match.id;
  }

  // 4. Phone match (strip non-digits for comparison)
  if (lead.phone) {
    const digits = lead.phone.replace(/\D/g, '');
    if (digits.length >= 7) {
      // Match on last 10 digits (ignore country code differences)
      const suffix = digits.slice(-10);
      const matches = db.prepare(
        "SELECT id, phone FROM leads WHERE phone != '' AND phone IS NOT NULL"
      ).all();
      for (const m of matches) {
        const mDigits = (m.phone || '').replace(/\D/g, '').slice(-10);
        if (mDigits === suffix) return m.id;
      }
    }
  }

  return null;
}

/**
 * Insert or update a lead. Returns { id, isNew, wasUpdated }.
 */
function upsertLead(lead) {
  const db = getDb();
  const existingId = findExistingLead(lead);

  if (existingId) {
    // Update: fill in any missing fields (don't overwrite existing data)
    const existing = db.prepare('SELECT * FROM leads WHERE id = ?').get(existingId);
    const updates = {};
    const fieldsToMerge = [
      'phone', 'email', 'website', 'firm_name', 'bar_number', 'bar_status',
      'admission_date', 'practice_area', 'title', 'linkedin_url', 'bio',
      'education', 'languages', 'practice_specialties', 'profile_url',
      'email_source', 'phone_source', 'website_source', 'google_place_id',
    ];

    for (const field of fieldsToMerge) {
      if (lead[field] && (!existing[field] || existing[field] === '')) {
        updates[field] = lead[field];
      }
    }

    // Update rating if new one is available
    if (lead.rating && !existing.rating) {
      updates.rating = lead.rating;
    }

    const wasUpdated = Object.keys(updates).length > 0;
    if (wasUpdated) {
      // Recompute lead score with merged data
      const merged = { ...existing, ...updates };
      const newScore = computeLeadScore(merged);
      updates.lead_score = newScore;

      const setClauses = Object.keys(updates).map(k => `${k} = ?`).join(', ');
      const values = Object.values(updates);
      db.prepare(
        `UPDATE leads SET ${setClauses}, updated_at = CURRENT_TIMESTAMP WHERE id = ?`
      ).run(...values, existingId);
    }

    // Add source tracking
    if (lead.source || lead.primary_source) {
      const source = lead.source || lead.primary_source;
      db.prepare(
        'INSERT OR IGNORE INTO lead_sources (lead_id, source) VALUES (?, ?)'
      ).run(existingId, source);
    }

    return { id: existingId, isNew: false, wasUpdated };
  }

  // Insert new lead
  const result = db.prepare(`
    INSERT INTO leads (
      first_name, last_name, firm_name, city, state, country,
      phone, email, website, bar_number, bar_status, admission_date,
      practice_area, title, linkedin_url, bio, education, languages,
      practice_specialties, email_source, phone_source, website_source,
      primary_source, google_place_id, profile_url, rating, lead_score
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    lead.first_name || '', lead.last_name || '', lead.firm_name || '',
    lead.city || '', lead.state || '', lead.country || deriveCountry(lead.state),
    lead.phone || '', lead.email || '', lead.website || '',
    lead.bar_number || '', lead.bar_status || '', lead.admission_date || '',
    lead.practice_area || '', lead.title || '', lead.linkedin_url || '',
    lead.bio || '', lead.education || '', lead.languages || '',
    lead.practice_specialties || '',
    lead.email_source || '', lead.phone_source || '', lead.website_source || '',
    lead.source || lead.primary_source || '',
    lead.google_place_id || lead._googlePlaceId || '',
    lead.profile_url || '',
    lead.rating || lead._rating || null,
    computeLeadScore(lead)
  );

  const newId = result.lastInsertRowid;

  // Add source tracking
  if (lead.source || lead.primary_source) {
    const source = lead.source || lead.primary_source;
    db.prepare(
      'INSERT OR IGNORE INTO lead_sources (lead_id, source) VALUES (?, ?)'
    ).run(newId, source);
  }

  return { id: newId, isNew: true, wasUpdated: false };
}

/**
 * Batch upsert leads. Returns { inserted, updated, unchanged }.
 */
function batchUpsert(leads, source) {
  const db = getDb();
  const stats = { inserted: 0, updated: 0, unchanged: 0 };

  const transaction = db.transaction(() => {
    for (const lead of leads) {
      if (source && !lead.source) lead.source = source;
      const result = upsertLead(lead);
      if (result.isNew) stats.inserted++;
      else if (result.wasUpdated) stats.updated++;
      else stats.unchanged++;
    }
  });

  transaction();
  return stats;
}

/**
 * Record a scrape run.
 */
function recordScrapeRun(run) {
  const db = getDb();
  return db.prepare(`
    INSERT INTO scrape_runs (state, source, practice_area, leads_found, leads_new, leads_updated, emails_found, completed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
  `).run(
    run.state || '', run.source || '', run.practice_area || '',
    run.leadsFound || 0, run.leadsNew || 0, run.leadsUpdated || 0,
    run.emailsFound || 0
  );
}

/**
 * Get TAM statistics.
 */
function getStats() {
  const db = getDb();
  const total = db.prepare('SELECT COUNT(*) as count FROM leads').get().count;
  const withEmail = db.prepare("SELECT COUNT(*) as count FROM leads WHERE email != '' AND email IS NOT NULL").get().count;
  const withPhone = db.prepare("SELECT COUNT(*) as count FROM leads WHERE phone != '' AND phone IS NOT NULL").get().count;
  const withWebsite = db.prepare("SELECT COUNT(*) as count FROM leads WHERE website != '' AND website IS NOT NULL").get().count;
  const verified = db.prepare('SELECT COUNT(*) as count FROM leads WHERE email_verified = 1').get().count;
  const uniqueFirms = db.prepare("SELECT COUNT(DISTINCT firm_name) as count FROM leads WHERE firm_name != ''").get().count;

  const byState = db.prepare(
    "SELECT state, COUNT(*) as count FROM leads WHERE state != '' GROUP BY state ORDER BY count DESC"
  ).all();

  const bySource = db.prepare(
    'SELECT source, COUNT(*) as count FROM lead_sources GROUP BY source ORDER BY count DESC'
  ).all();

  const byCountry = db.prepare(
    "SELECT country, COUNT(*) as count FROM leads WHERE country != '' GROUP BY country ORDER BY count DESC"
  ).all();

  // Quality tier breakdown
  const contactable = db.prepare(
    "SELECT COUNT(*) as count FROM leads WHERE (email != '' AND email IS NOT NULL) OR (phone != '' AND phone IS NOT NULL)"
  ).get().count;
  const goldLeads = db.prepare(
    "SELECT COUNT(*) as count FROM leads WHERE email != '' AND email IS NOT NULL AND phone != '' AND phone IS NOT NULL"
  ).get().count;
  const silverLeads = contactable - goldLeads;

  // Recent activity
  const last24h = db.prepare(
    "SELECT COUNT(*) as count FROM leads WHERE updated_at > datetime('now', '-1 day')"
  ).get().count;
  const last7d = db.prepare(
    "SELECT COUNT(*) as count FROM leads WHERE updated_at > datetime('now', '-7 days')"
  ).get().count;

  // Top cities
  const topCities = db.prepare(
    "SELECT city || ', ' || state as location, COUNT(*) as count FROM leads WHERE city != '' GROUP BY city, state ORDER BY count DESC LIMIT 20"
  ).all();

  // Source coverage breakdown
  const emailSources = db.prepare(
    "SELECT email_source, COUNT(*) as count FROM leads WHERE email_source != '' AND email_source IS NOT NULL GROUP BY email_source ORDER BY count DESC"
  ).all();

  return {
    total,
    withEmail,
    withPhone,
    withWebsite,
    verified,
    uniqueFirms,
    contactable,
    goldLeads,
    silverLeads,
    last24h,
    last7d,
    byState,
    bySource,
    byCountry,
    topCities,
    emailSources,
    coverage: {
      email: total > 0 ? Math.round(withEmail / total * 100) : 0,
      phone: total > 0 ? Math.round(withPhone / total * 100) : 0,
      website: total > 0 ? Math.round(withWebsite / total * 100) : 0,
      contactable: total > 0 ? Math.round(contactable / total * 100) : 0,
    },
  };
}

/**
 * Search leads.
 */
function searchLeads(query, options = {}) {
  const db = getDb();
  const { state, country, hasEmail, hasPhone, hasWebsite, practiceArea, minScore, maxScore, tags, sort, order, limit = 100, offset = 0 } = options;

  let where = [];
  let params = [];

  if (query) {
    where.push("(first_name LIKE ? OR last_name LIKE ? OR firm_name LIKE ? OR city LIKE ? OR email LIKE ?)");
    const q = `%${query}%`;
    params.push(q, q, q, q, q);
  }
  if (state) {
    where.push('state = ?');
    params.push(state);
  }
  if (country) {
    where.push('country = ?');
    params.push(country);
  }
  if (hasEmail) {
    where.push("email != '' AND email IS NOT NULL");
  }
  if (hasPhone) {
    where.push("phone != '' AND phone IS NOT NULL");
  }
  if (hasWebsite) {
    where.push("website != '' AND website IS NOT NULL");
  }
  if (practiceArea) {
    where.push("practice_area LIKE ?");
    params.push(`%${practiceArea}%`);
  }
  if (minScore !== undefined && minScore !== null) {
    where.push("lead_score >= ?");
    params.push(Number(minScore));
  }
  if (maxScore !== undefined && maxScore !== null) {
    where.push("lead_score <= ?");
    params.push(Number(maxScore));
  }
  if (tags) {
    where.push("tags LIKE ?");
    params.push(`%${tags}%`);
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  // Sortable columns (whitelist for safety)
  const sortableCols = { name: 'last_name', first_name: 'first_name', firm: 'firm_name', city: 'city', state: 'state', score: 'lead_score', email: 'email', phone: 'phone', source: 'primary_source', updated: 'updated_at', created: 'created_at' };
  const sortCol = sortableCols[sort] || 'updated_at';
  const sortDir = order === 'asc' ? 'ASC' : 'DESC';

  // Count total for this query
  const countParams = [...params];
  const total = db.prepare(`SELECT COUNT(*) as c FROM leads ${whereClause}`).get(...countParams).c;

  params.push(limit, offset);
  const leads = db.prepare(
    `SELECT * FROM leads ${whereClause} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`
  ).all(...params);

  return { leads, total };
}

/**
 * Get leads without email that have a website (candidates for email finding).
 */
function getLeadsNeedingEmail(limit = 500) {
  const db = getDb();
  return db.prepare(`
    SELECT * FROM leads
    WHERE (email = '' OR email IS NULL)
      AND website != '' AND website IS NOT NULL
    ORDER BY updated_at DESC
    LIMIT ?
  `).all(limit);
}

/**
 * Update a lead's email verification status.
 */
function updateEmailVerification(leadId, verified, catchAll = false) {
  const db = getDb();
  db.prepare(
    'UPDATE leads SET email_verified = ?, email_catch_all = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
  ).run(verified ? 1 : 0, catchAll ? 1 : 0, leadId);
}

/**
 * Export leads to an array (for CSV export).
 */
function exportLeads(options = {}) {
  const db = getDb();
  const { state, country, hasEmail, hasPhone, verified } = options;

  let where = [];
  let params = [];

  if (state) { where.push('state = ?'); params.push(state); }
  if (country) { where.push('country = ?'); params.push(country); }
  if (hasEmail) { where.push("email != '' AND email IS NOT NULL"); }
  if (hasPhone) { where.push("phone != '' AND phone IS NOT NULL"); }
  if (verified) { where.push('email_verified = 1'); }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

  return db.prepare(`SELECT * FROM leads ${whereClause} ORDER BY state, city, last_name`).all(...params);
}

/**
 * Look up a lead in the master DB by name + city for cross-reference enrichment.
 * Returns phone/email/website/firm from previous scrapes if found.
 * Used by the waterfall as an instant (SQLite) enrichment step.
 */
function lookupByNameCity(firstName, lastName, city, state) {
  const db = getDb();
  if (!firstName || !lastName) return null;

  const fn = firstName.toLowerCase().trim();
  const ln = lastName.toLowerCase().trim();

  let where = 'LOWER(first_name) = ? AND LOWER(last_name) = ?';
  let params = [fn, ln];

  // Prefer exact city+state match, fall back to state-only, then name-only
  if (city && state) {
    const match = db.prepare(
      `SELECT phone, email, website, firm_name, profile_url, bar_number, practice_area, primary_source
       FROM leads WHERE ${where} AND LOWER(city) = ? AND state = ?
       ORDER BY updated_at DESC LIMIT 1`
    ).get(...params, city.toLowerCase().trim(), state);
    if (match) return stripEmpty(match);
  }

  if (state) {
    const match = db.prepare(
      `SELECT phone, email, website, firm_name, profile_url, bar_number, practice_area, primary_source
       FROM leads WHERE ${where} AND state = ?
       ORDER BY updated_at DESC LIMIT 1`
    ).get(...params, state);
    if (match) return stripEmpty(match);
  }

  // Name-only match (riskier, but useful for cross-state enrichment)
  const match = db.prepare(
    `SELECT phone, email, website, firm_name, profile_url, bar_number, practice_area, primary_source
     FROM leads WHERE ${where}
     ORDER BY updated_at DESC LIMIT 1`
  ).get(...params);
  if (match) return stripEmpty(match);

  return null;
}

/**
 * Batch lookup: find matching leads in the master DB for a set of leads.
 * Returns a Map of index → enrichment data.
 * Much faster than individual lookups — uses a single query per batch.
 */
function batchLookupByNameCity(leads) {
  const db = getDb();
  const results = new Map();

  // Build a set of unique name+city+state combos
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    if (!lead.first_name || !lead.last_name) continue;

    const fn = lead.first_name.toLowerCase().trim();
    const ln = lead.last_name.toLowerCase().trim();
    const city = (lead.city || '').toLowerCase().trim();
    const state = lead.state || '';

    // Try name+city+state first
    let match = null;
    if (city && state) {
      match = db.prepare(
        `SELECT phone, email, website, firm_name, profile_url, practice_area, primary_source
         FROM leads WHERE LOWER(first_name) = ? AND LOWER(last_name) = ? AND LOWER(city) = ? AND state = ?
         ORDER BY updated_at DESC LIMIT 1`
      ).get(fn, ln, city, state);
    }

    // Fall back to name+state
    if (!match && state) {
      match = db.prepare(
        `SELECT phone, email, website, firm_name, profile_url, practice_area, primary_source
         FROM leads WHERE LOWER(first_name) = ? AND LOWER(last_name) = ? AND state = ?
         ORDER BY updated_at DESC LIMIT 1`
      ).get(fn, ln, state);
    }

    if (match) {
      const clean = stripEmpty(match);
      if (clean) results.set(i, clean);
    }
  }

  return results;
}

/**
 * Find and merge duplicate leads in the master DB.
 * Scans all leads and merges records that match on name+city+state.
 * The winner keeps all its fields; the loser's non-empty fields fill gaps.
 * Returns { merged, fieldsRecovered }.
 */
function mergeDuplicates(options = {}) {
  const db = getDb();
  const { dryRun = false, onProgress } = options;

  // Find potential duplicates by name+city+state
  const dupes = db.prepare(`
    SELECT LOWER(first_name) || '|' || LOWER(last_name) || '|' || LOWER(city) || '|' || state as key,
           GROUP_CONCAT(id) as ids, COUNT(*) as cnt
    FROM leads
    WHERE first_name != '' AND last_name != ''
    GROUP BY LOWER(first_name), LOWER(last_name), LOWER(city), state
    HAVING cnt > 1
    ORDER BY cnt DESC
  `).all();

  let merged = 0;
  let fieldsRecovered = 0;
  const fieldsToMerge = [
    'phone', 'email', 'website', 'firm_name', 'bar_number', 'bar_status',
    'admission_date', 'practice_area', 'title', 'linkedin_url', 'bio',
    'education', 'languages', 'practice_specialties', 'profile_url',
    'email_source', 'phone_source', 'website_source', 'google_place_id',
  ];

  const transaction = db.transaction(() => {
    for (let i = 0; i < dupes.length; i++) {
      const { ids: idStr } = dupes[i];
      const ids = idStr.split(',').map(Number);

      // Load all records
      const records = ids.map(id =>
        db.prepare('SELECT * FROM leads WHERE id = ?').get(id)
      ).filter(Boolean);

      if (records.length < 2) continue;

      // Pick the "best" record as winner: most non-empty fields
      records.sort((a, b) => {
        const scoreA = fieldsToMerge.filter(f => a[f] && a[f] !== '').length;
        const scoreB = fieldsToMerge.filter(f => b[f] && b[f] !== '').length;
        return scoreB - scoreA;
      });

      const winner = records[0];
      const losers = records.slice(1);

      // Merge loser fields into winner
      let updates = {};
      for (const loser of losers) {
        for (const field of fieldsToMerge) {
          if ((!winner[field] || winner[field] === '') && loser[field] && loser[field] !== '') {
            winner[field] = loser[field];
            updates[field] = loser[field];
            fieldsRecovered++;
          }
        }
      }

      if (!dryRun) {
        // Update winner with merged fields
        if (Object.keys(updates).length > 0) {
          const setClauses = Object.keys(updates).map(k => `${k} = ?`).join(', ');
          const values = Object.values(updates);
          db.prepare(
            `UPDATE leads SET ${setClauses}, updated_at = CURRENT_TIMESTAMP WHERE id = ?`
          ).run(...values, winner.id);
        }

        // Move sources from losers to winner (ignore conflicts)
        for (const loser of losers) {
          db.prepare(
            'INSERT OR IGNORE INTO lead_sources (lead_id, source, scraped_at) SELECT ?, source, scraped_at FROM lead_sources WHERE lead_id = ?'
          ).run(winner.id, loser.id);
          db.prepare('DELETE FROM lead_sources WHERE lead_id = ?').run(loser.id);
        }

        // Delete loser records
        for (const loser of losers) {
          db.prepare('DELETE FROM leads WHERE id = ?').run(loser.id);
        }
      }

      merged += losers.length;

      if (onProgress && i % 100 === 0) {
        onProgress(i, dupes.length, merged, fieldsRecovered);
      }
    }
  });

  transaction();

  return { duplicateGroups: dupes.length, merged, fieldsRecovered, dryRun };
}

/**
 * Get per-state coverage analysis: total, email%, phone%, website% for each state.
 * Returns array sorted by total leads descending.
 */
function getStateCoverage() {
  const db = getDb();
  return db.prepare(`
    SELECT
      state,
      country,
      COUNT(*) as total,
      SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) as with_website,
      ROUND(SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as email_pct,
      ROUND(SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as phone_pct,
      ROUND(SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as website_pct
    FROM leads
    WHERE state != ''
    GROUP BY state
    ORDER BY total DESC
  `).all();
}

/** Strip empty string values from an object, return null if nothing useful */
function stripEmpty(obj) {
  const result = {};
  let hasData = false;
  for (const [k, v] of Object.entries(obj)) {
    if (v && typeof v === 'string' && v.trim() !== '') {
      result[k] = v;
      hasData = true;
    } else if (v && typeof v !== 'string') {
      result[k] = v;
      hasData = true;
    }
  }
  return hasData ? result : null;
}

/**
 * Compute a 0-100 quality score for a lead based on data completeness.
 * Scoring: email (+30), phone (+25), website (+15), firm (+10), practice_area (+10), verified (+10)
 */
function computeLeadScore(lead) {
  let score = 0;
  if (lead.email) score += 30;
  if (lead.phone) score += 25;
  if (lead.website) score += 15;
  if (lead.firm_name) score += 10;
  if (lead.practice_area) score += 10;
  if (lead.email_verified) score += 10;
  return score;
}

/**
 * Batch score all leads and store in the `lead_score` column.
 * Returns { scored, avgScore }.
 */
function batchScoreLeads() {
  const db = getDb();

  // Ensure lead_score column exists
  try {
    db.exec('ALTER TABLE leads ADD COLUMN lead_score INTEGER DEFAULT 0');
  } catch {} // Column already exists

  const update = db.prepare('UPDATE leads SET lead_score = ? WHERE id = ?');
  const leads = db.prepare('SELECT id, email, phone, website, firm_name, practice_area, email_verified FROM leads').all();

  let totalScore = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      const score = computeLeadScore(lead);
      update.run(score, lead.id);
      totalScore += score;
    }
  });
  txn();

  return {
    scored: leads.length,
    avgScore: leads.length > 0 ? Math.round(totalScore / leads.length) : 0,
  };
}

/**
 * Get score distribution: how many leads at each score tier.
 */
function getScoreDistribution() {
  const db = getDb();

  // Ensure column exists
  try {
    db.exec('ALTER TABLE leads ADD COLUMN lead_score INTEGER DEFAULT 0');
  } catch {}

  return {
    excellent: db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score >= 80').get().c,
    good: db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score >= 55 AND lead_score < 80').get().c,
    fair: db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score >= 30 AND lead_score < 55').get().c,
    poor: db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score > 0 AND lead_score < 30').get().c,
    none: db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score = 0 OR lead_score IS NULL').get().c,
    avgScore: db.prepare('SELECT ROUND(AVG(lead_score)) as avg FROM leads WHERE lead_score > 0').get().avg || 0,
  };
}

/**
 * Get scrape freshness: when each state was last scraped, lead count, and age in days.
 * Returns array sorted by days_since_scrape descending (stalest first).
 */
function getScrapeHistory() {
  const db = getDb();

  return db.prepare(`
    SELECT
      state,
      MAX(completed_at) as last_scraped,
      SUM(leads_found) as total_scraped,
      SUM(leads_new) as total_new,
      SUM(emails_found) as total_emails,
      COUNT(*) as run_count,
      ROUND(julianday('now') - julianday(MAX(completed_at))) as days_since_scrape
    FROM scrape_runs
    WHERE state != '' AND state != 'BULK'
    GROUP BY state
    ORDER BY days_since_scrape DESC
  `).all();
}

/**
 * Get smart recommendations for the user.
 * Returns array of { type, priority, title, description, action }.
 */
function getRecommendations() {
  const db = getDb();
  const recommendations = [];

  // 1. States never scraped (have scrapers but no runs)
  const { getScraperMetadata } = require('./registry');
  const metadata = getScraperMetadata();
  const workingScrapers = Object.entries(metadata).filter(([, m]) => m.working).map(([code]) => code);
  const scrapedStates = new Set(
    db.prepare("SELECT DISTINCT state FROM scrape_runs WHERE state != '' AND state != 'BULK'").all().map(r => r.state)
  );
  const neverScraped = workingScrapers.filter(s => !scrapedStates.has(s) && !['MARTINDALE', 'LAWYERS-COM', 'GOOGLE-PLACES', 'JUSTIA', 'AVVO', 'FINDLAW'].includes(s));
  if (neverScraped.length > 0) {
    recommendations.push({
      type: 'never-scraped',
      priority: 'high',
      title: `${neverScraped.length} states never scraped`,
      description: `These working scrapers have never been run: ${neverScraped.slice(0, 8).join(', ')}${neverScraped.length > 8 ? '...' : ''}`,
      action: 'bulk-scrape',
      states: neverScraped,
    });
  }

  // 2. Stale states (last scraped > 7 days ago)
  const staleStates = db.prepare(`
    SELECT state, MAX(completed_at) as last_scraped,
           ROUND(julianday('now') - julianday(MAX(completed_at))) as days_ago
    FROM scrape_runs WHERE state != '' AND state != 'BULK'
    GROUP BY state
    HAVING days_ago > 7
    ORDER BY days_ago DESC
    LIMIT 10
  `).all();
  if (staleStates.length > 0) {
    recommendations.push({
      type: 'stale',
      priority: 'medium',
      title: `${staleStates.length} states stale (>7 days)`,
      description: staleStates.slice(0, 5).map(s => `${s.state} (${s.days_ago}d ago)`).join(', '),
      action: 'bulk-scrape',
      states: staleStates.map(s => s.state),
    });
  }

  // 3. Low email coverage states (have leads but <10% email)
  const lowEmail = db.prepare(`
    SELECT state, COUNT(*) as total,
           SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email,
           ROUND(SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as email_pct
    FROM leads WHERE state != ''
    GROUP BY state
    HAVING total >= 20 AND email_pct < 10
    ORDER BY total DESC
    LIMIT 10
  `).all();
  if (lowEmail.length > 0) {
    recommendations.push({
      type: 'low-email',
      priority: 'high',
      title: `${lowEmail.length} states with <10% email coverage`,
      description: lowEmail.slice(0, 5).map(s => `${s.state} (${s.email_pct}% of ${s.total})`).join(', '),
      action: 'find-emails',
      states: lowEmail.map(s => s.state),
    });
  }

  // 4. Leads with website but no email (enrichment opportunity)
  const websiteNoEmail = db.prepare(`
    SELECT COUNT(*) as c FROM leads
    WHERE website != '' AND website IS NOT NULL AND (email = '' OR email IS NULL)
  `).get().c;
  if (websiteNoEmail > 0) {
    recommendations.push({
      type: 'enrichment',
      priority: 'medium',
      title: `${websiteNoEmail.toLocaleString()} leads have website but no email`,
      description: 'Run SMTP email finder to discover emails from firm websites.',
      action: 'find-emails',
    });
  }

  // 5. Firm data sharing opportunity
  const firmShareOpportunity = db.prepare(`
    SELECT COUNT(*) as c FROM leads l1
    WHERE (l1.website = '' OR l1.website IS NULL)
      AND l1.firm_name != '' AND l1.firm_name IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM leads l2
        WHERE LOWER(l2.firm_name) = LOWER(l1.firm_name)
          AND l2.city = l1.city AND l2.state = l1.state
          AND l2.website != '' AND l2.website IS NOT NULL
      )
  `).get().c;
  if (firmShareOpportunity > 0) {
    recommendations.push({
      type: 'firm-share',
      priority: 'medium',
      title: `${firmShareOpportunity.toLocaleString()} leads can inherit firm website`,
      description: 'Share data across colleagues at the same firm to fill missing fields.',
      action: 'share-firm-data',
    });
  }

  // 5b. Leads with firm name but no website
  const firmNoWebsite = db.prepare(`
    SELECT COUNT(*) as c FROM leads
    WHERE firm_name != '' AND firm_name IS NOT NULL
      AND (website = '' OR website IS NULL)
  `).get().c;
  if (firmNoWebsite > 0) {
    recommendations.push({
      type: 'enrichment',
      priority: 'low',
      title: `${firmNoWebsite.toLocaleString()} leads have firm but no website`,
      description: 'Run website finder to discover firm websites from business name.',
      action: 'find-websites',
    });
  }

  // 6. Unscored leads (NULL means never scored, 0 means scored but no data)
  try {
    const unscored = db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score IS NULL').get().c;
    if (unscored > 50) {
      recommendations.push({
        type: 'scoring',
        priority: 'low',
        title: `${unscored.toLocaleString()} leads need scoring`,
        description: 'Run batch scoring to calculate data quality scores.',
        action: 'score-leads',
      });
    }
  } catch {}

  // Sort by priority
  const priorityOrder = { high: 0, medium: 1, low: 2 };
  recommendations.sort((a, b) => priorityOrder[a.priority] - priorityOrder[b.priority]);

  return recommendations;
}

/**
 * Share data across firm members — if one lawyer at a firm has website/phone,
 * propagate to other lawyers at the same firm who are missing that data.
 * Returns { firmsProcessed, leadsUpdated, fieldsShared }.
 */
function shareFirmData() {
  const db = getDb();

  // Find firms with multiple leads where at least one has a website
  const firms = db.prepare(`
    SELECT firm_name, city, state, COUNT(*) as cnt,
           MAX(CASE WHEN website != '' AND website IS NOT NULL THEN website ELSE NULL END) as best_website,
           MAX(CASE WHEN phone != '' AND phone IS NOT NULL THEN phone ELSE NULL END) as best_phone
    FROM leads
    WHERE firm_name != '' AND firm_name IS NOT NULL
    GROUP BY LOWER(firm_name), city, state
    HAVING cnt > 1 AND (best_website IS NOT NULL OR best_phone IS NOT NULL)
  `).all();

  let firmsProcessed = 0;
  let leadsUpdated = 0;
  let fieldsShared = 0;

  const txn = db.transaction(() => {
    for (const firm of firms) {
      let updated = false;

      // Share website to leads at this firm that are missing it
      if (firm.best_website) {
        const result = db.prepare(`
          UPDATE leads SET website = ?, website_source = 'firm-share',
                          lead_score = lead_score + 15,
                          updated_at = CURRENT_TIMESTAMP
          WHERE LOWER(firm_name) = LOWER(?) AND city = ? AND state = ?
            AND (website = '' OR website IS NULL)
        `).run(firm.best_website, firm.firm_name, firm.city, firm.state);
        if (result.changes > 0) {
          leadsUpdated += result.changes;
          fieldsShared += result.changes;
          updated = true;
        }
      }

      if (updated) firmsProcessed++;
    }
  });

  txn();
  return { firmsProcessed, leadsUpdated, fieldsShared };
}

/**
 * Deduce websites from email domains — if a lawyer has email john@smithlaw.com,
 * their website is likely smithlaw.com (if no website already set).
 * Returns { leadsUpdated }.
 */
function deduceWebsitesFromEmail() {
  const db = getDb();

  // Common free email providers to skip
  const freeProviders = new Set([
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'mail.com', 'protonmail.com', 'live.com', 'msn.com',
    'comcast.net', 'att.net', 'verizon.net', 'sbcglobal.net', 'cox.net',
    'charter.net', 'earthlink.net', 'me.com', 'mac.com', 'ymail.com',
  ]);

  const leads = db.prepare(`
    SELECT id, email FROM leads
    WHERE email != '' AND email IS NOT NULL
      AND (website = '' OR website IS NULL)
  `).all();

  let updated = 0;
  const update = db.prepare(`
    UPDATE leads SET website = ?, website_source = 'email-domain',
                    lead_score = lead_score + 15,
                    updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `);

  const txn = db.transaction(() => {
    for (const lead of leads) {
      const domain = lead.email.split('@')[1];
      if (!domain || freeProviders.has(domain.toLowerCase())) continue;
      update.run('https://' + domain, lead.id);
      updated++;
    }
  });

  txn();
  return { leadsUpdated: updated };
}

/**
 * Get recent scrape activity for the timeline.
 */
function getRecentActivity(limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT id, state, source, practice_area, leads_found, leads_new, leads_updated,
           emails_found, started_at, completed_at,
           ROUND((julianday(completed_at) - julianday(started_at)) * 86400) as duration_secs
    FROM scrape_runs
    WHERE completed_at IS NOT NULL
    ORDER BY completed_at DESC
    LIMIT ?
  `).all(limit);
}

/**
 * Get distinct practice areas for filter dropdown.
 */
function getDistinctPracticeAreas() {
  const db = getDb();
  return db.prepare(`
    SELECT DISTINCT practice_area, COUNT(*) as count
    FROM leads
    WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area
    ORDER BY count DESC
    LIMIT 100
  `).all();
}

/**
 * Get distinct tags for filter dropdown.
 */
function getDistinctTags() {
  const db = getDb();
  // Ensure tags column exists
  try {
    db.prepare("SELECT tags FROM leads LIMIT 1").get();
  } catch {
    db.exec("ALTER TABLE leads ADD COLUMN tags TEXT DEFAULT ''");
  }
  const rows = db.prepare(`
    SELECT tags FROM leads WHERE tags IS NOT NULL AND tags != ''
  `).all();
  const tagSet = new Set();
  for (const row of rows) {
    row.tags.split(',').forEach(t => {
      const trimmed = t.trim();
      if (trimmed) tagSet.add(trimmed);
    });
  }
  return [...tagSet].sort();
}

/**
 * Tag multiple leads.
 */
function tagLeads(leadIds, tag, remove = false) {
  const db = getDb();
  // Ensure tags column exists
  try {
    db.prepare("SELECT tags FROM leads LIMIT 1").get();
  } catch {
    db.exec("ALTER TABLE leads ADD COLUMN tags TEXT DEFAULT ''");
  }

  const tagStr = tag.trim();
  if (!tagStr) return { updated: 0 };

  let updated = 0;
  const txn = db.transaction(() => {
    for (const id of leadIds) {
      const lead = db.prepare("SELECT tags FROM leads WHERE id = ?").get(id);
      if (!lead) continue;

      const currentTags = (lead.tags || '').split(',').map(t => t.trim()).filter(Boolean);

      if (remove) {
        const newTags = currentTags.filter(t => t !== tagStr);
        db.prepare("UPDATE leads SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").run(newTags.join(','), id);
      } else {
        if (!currentTags.includes(tagStr)) {
          currentTags.push(tagStr);
          db.prepare("UPDATE leads SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?").run(currentTags.join(','), id);
        }
      }
      updated++;
    }
  });
  txn();
  return { updated };
}

/**
 * Delete multiple leads by ID.
 */
function deleteLeads(leadIds) {
  const db = getDb();
  let deleted = 0;
  const txn = db.transaction(() => {
    for (const id of leadIds) {
      db.prepare("DELETE FROM lead_sources WHERE lead_id = ?").run(id);
      const r = db.prepare("DELETE FROM leads WHERE id = ?").run(id);
      deleted += r.changes;
    }
  });
  txn();
  return { deleted };
}

/**
 * Get a single lead by ID with all data.
 */
function getLeadById(id) {
  const db = getDb();
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(id);
  if (!lead) return null;

  const sources = db.prepare("SELECT source, scraped_at FROM lead_sources WHERE lead_id = ? ORDER BY scraped_at DESC").all(id);
  return { ...lead, sources };
}

module.exports = {
  getDb,
  upsertLead,
  batchUpsert,
  findExistingLead,
  recordScrapeRun,
  getStats,
  searchLeads,
  getLeadsNeedingEmail,
  updateEmailVerification,
  exportLeads,
  lookupByNameCity,
  batchLookupByNameCity,
  mergeDuplicates,
  getStateCoverage,
  computeLeadScore,
  batchScoreLeads,
  getScoreDistribution,
  getScrapeHistory,
  getRecommendations,
  shareFirmData,
  deduceWebsitesFromEmail,
  getRecentActivity,
  getDistinctPracticeAreas,
  getDistinctTags,
  tagLeads,
  deleteLeads,
  getLeadById,
};
