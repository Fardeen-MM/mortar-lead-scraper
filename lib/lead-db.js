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

  // Add optional columns if they don't exist yet
  const addColIfMissing = (col, type) => {
    try { _db.prepare(`SELECT ${col} FROM leads LIMIT 1`).get(); }
    catch { _db.exec(`ALTER TABLE leads ADD COLUMN ${col} ${type}`); }
  };
  addColIfMissing('tags', "TEXT DEFAULT ''");
  addColIfMissing('notes', "TEXT DEFAULT ''");
  addColIfMissing('lead_score', "INTEGER DEFAULT 0");
  addColIfMissing('enrichment_steps', "TEXT DEFAULT ''"); // comma-separated: profile,martindale,lawyerscom,website-crawl
  addColIfMissing('last_enriched_at', "DATETIME");
  addColIfMissing('pipeline_stage', "TEXT DEFAULT 'new'"); // new, contacted, replied, meeting, client
  addColIfMissing('email_type', "TEXT DEFAULT ''"); // professional, personal, role_based, generic
  addColIfMissing('confidence_score', "INTEGER DEFAULT 0"); // 0-100
  addColIfMissing('source_count', "INTEGER DEFAULT 1"); // how many sources corroborate this lead
  addColIfMissing('previous_firm', "TEXT DEFAULT ''"); // for change detection
  addColIfMissing('firm_changed_at', "DATETIME"); // when firm change was detected

  // Tag definitions (color-coded)
  _db.exec(`
    CREATE TABLE IF NOT EXISTS tag_definitions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      color TEXT DEFAULT '#6366f1',
      description TEXT DEFAULT '',
      auto_rule TEXT DEFAULT '', -- JSON: conditions for auto-tagging
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS lead_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      field_name TEXT NOT NULL,
      old_value TEXT,
      new_value TEXT,
      detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      scrape_run_id INTEGER,
      FOREIGN KEY (lead_id) REFERENCES leads(id)
    );
    CREATE INDEX IF NOT EXISTS idx_snapshots_lead ON lead_snapshots(lead_id);
    CREATE INDEX IF NOT EXISTS idx_snapshots_field ON lead_snapshots(field_name);
  `);

  // Lead lists / campaigns
  _db.exec(`
    CREATE TABLE IF NOT EXISTS lead_lists (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      color TEXT DEFAULT '#6366f1',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS list_members (
      list_id INTEGER NOT NULL,
      lead_id INTEGER NOT NULL,
      added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (list_id) REFERENCES lead_lists(id) ON DELETE CASCADE,
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
      UNIQUE(list_id, lead_id)
    );
    CREATE INDEX IF NOT EXISTS idx_list_members_list ON list_members(list_id);
    CREATE INDEX IF NOT EXISTS idx_list_members_lead ON list_members(lead_id);
  `);

  // Lead changelog / audit trail
  _db.exec(`
    CREATE TABLE IF NOT EXISTS lead_changelog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      field TEXT,
      old_value TEXT,
      new_value TEXT,
      source TEXT DEFAULT 'manual',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_changelog_lead ON lead_changelog(lead_id);
    CREATE INDEX IF NOT EXISTS idx_changelog_time ON lead_changelog(created_at);
  `);

  // Export history
  _db.exec(`
    CREATE TABLE IF NOT EXISTS export_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      format TEXT NOT NULL,
      lead_count INTEGER DEFAULT 0,
      filters TEXT,
      filename TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Data quality alerts
  _db.exec(`
    CREATE TABLE IF NOT EXISTS quality_alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER,
      alert_type TEXT NOT NULL,
      severity TEXT DEFAULT 'warning',
      message TEXT,
      resolved INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_alerts_type ON quality_alerts(alert_type);
    CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON quality_alerts(resolved);
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
  const { state, country, hasEmail, hasPhone, hasWebsite, practiceArea, minScore, maxScore, tags, source, sort, order, limit = 100, offset = 0 } = options;

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
  if (source) {
    where.push("primary_source = ?");
    params.push(source);
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

/**
 * Get detailed analytics for a single state.
 */
function getStateDetails(stateCode) {
  const db = getDb();
  const total = db.prepare('SELECT COUNT(*) as cnt FROM leads WHERE state = ?').get(stateCode).cnt;
  if (total === 0) return null;

  const fieldCounts = db.prepare(`
    SELECT
      SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as email,
      SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) as phone,
      SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) as website,
      SUM(CASE WHEN firm_name != '' AND firm_name IS NOT NULL THEN 1 ELSE 0 END) as firm,
      SUM(CASE WHEN practice_area != '' AND practice_area IS NOT NULL THEN 1 ELSE 0 END) as practice,
      SUM(CASE WHEN profile_url != '' AND profile_url IS NOT NULL THEN 1 ELSE 0 END) as profile_url,
      SUM(CASE WHEN title != '' AND title IS NOT NULL THEN 1 ELSE 0 END) as title,
      SUM(CASE WHEN linkedin_url != '' AND linkedin_url IS NOT NULL THEN 1 ELSE 0 END) as linkedin,
      ROUND(AVG(CASE WHEN lead_score IS NOT NULL THEN lead_score ELSE 0 END)) as avg_score
    FROM leads WHERE state = ?
  `).get(stateCode);

  const topCities = db.prepare(`
    SELECT city, COUNT(*) as cnt
    FROM leads WHERE state = ? AND city IS NOT NULL AND city != ''
    GROUP BY city ORDER BY cnt DESC LIMIT 10
  `).all(stateCode);

  const topFirms = db.prepare(`
    SELECT firm_name, COUNT(*) as cnt
    FROM leads WHERE state = ? AND firm_name IS NOT NULL AND firm_name != ''
    GROUP BY firm_name ORDER BY cnt DESC LIMIT 10
  `).all(stateCode);

  const scoreDistribution = db.prepare(`
    SELECT
      SUM(CASE WHEN lead_score >= 80 THEN 1 ELSE 0 END) as excellent,
      SUM(CASE WHEN lead_score >= 55 AND lead_score < 80 THEN 1 ELSE 0 END) as good,
      SUM(CASE WHEN lead_score >= 30 AND lead_score < 55 THEN 1 ELSE 0 END) as fair,
      SUM(CASE WHEN lead_score < 30 THEN 1 ELSE 0 END) as poor
    FROM leads WHERE state = ?
  `).get(stateCode);

  const recentRuns = db.prepare(`
    SELECT started_at, leads_found, leads_new, emails_found
    FROM scrape_runs WHERE state = ?
    ORDER BY started_at DESC LIMIT 5
  `).all(stateCode);

  return {
    state: stateCode, total,
    fields: fieldCounts,
    topCities, topFirms,
    scoreDistribution,
    recentRuns,
  };
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
const DEFAULT_SCORING_RULES = [
  { field: 'email', points: 30, condition: 'is_not_empty' },
  { field: 'phone', points: 25, condition: 'is_not_empty' },
  { field: 'website', points: 15, condition: 'is_not_empty' },
  { field: 'firm_name', points: 10, condition: 'is_not_empty' },
  { field: 'practice_area', points: 10, condition: 'is_not_empty' },
  { field: 'email_verified', points: 10, condition: 'equals_1' },
];

function initScoringTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS scoring_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      field TEXT NOT NULL,
      points INTEGER NOT NULL,
      condition TEXT NOT NULL DEFAULT 'is_not_empty',
      enabled INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
  // Seed defaults if empty
  const count = db.prepare('SELECT COUNT(*) as cnt FROM scoring_rules').get();
  if (count.cnt === 0) {
    const insert = db.prepare('INSERT INTO scoring_rules (field, points, condition) VALUES (?, ?, ?)');
    for (const r of DEFAULT_SCORING_RULES) {
      insert.run(r.field, r.points, r.condition);
    }
  }
}

function getScoringRules() {
  initScoringTable();
  const db = getDb();
  return db.prepare('SELECT * FROM scoring_rules ORDER BY points DESC').all();
}

function updateScoringRule(id, updates) {
  initScoringTable();
  const db = getDb();
  const sets = [];
  const params = [];
  if (updates.points !== undefined) { sets.push('points = ?'); params.push(updates.points); }
  if (updates.enabled !== undefined) { sets.push('enabled = ?'); params.push(updates.enabled ? 1 : 0); }
  if (updates.field) { sets.push('field = ?'); params.push(updates.field); }
  if (updates.condition) { sets.push('condition = ?'); params.push(updates.condition); }
  if (sets.length === 0) return { updated: false };
  params.push(id);
  db.prepare(`UPDATE scoring_rules SET ${sets.join(', ')} WHERE id = ?`).run(...params);
  return { updated: true };
}

function addScoringRule(field, points, condition = 'is_not_empty') {
  initScoringTable();
  const db = getDb();
  const result = db.prepare('INSERT INTO scoring_rules (field, points, condition) VALUES (?, ?, ?)').run(field, points, condition);
  return { id: result.lastInsertRowid };
}

function deleteScoringRule(id) {
  initScoringTable();
  const db = getDb();
  db.prepare('DELETE FROM scoring_rules WHERE id = ?').run(id);
  return { deleted: true };
}

function computeLeadScore(lead) {
  initScoringTable();
  const db = getDb();
  const rules = db.prepare('SELECT * FROM scoring_rules WHERE enabled = 1').all();
  let score = 0;
  for (const rule of rules) {
    const val = lead[rule.field];
    let match = false;
    switch (rule.condition) {
      case 'is_not_empty': match = val && val !== '' && val !== '0'; break;
      case 'equals_1': match = val == 1; break;
      case 'is_empty': match = !val || val === ''; break;
    }
    if (match) score += rule.points;
  }
  return Math.min(score, 100);
}

function getScoreBreakdown(lead) {
  initScoringTable();
  const db = getDb();
  const rules = db.prepare('SELECT * FROM scoring_rules WHERE enabled = 1').all();
  const breakdown = [];
  for (const rule of rules) {
    const val = lead[rule.field];
    let match = false;
    switch (rule.condition) {
      case 'is_not_empty': match = val && val !== '' && val !== '0'; break;
      case 'equals_1': match = val == 1; break;
      case 'is_empty': match = !val || val === ''; break;
    }
    breakdown.push({ field: rule.field, points: rule.points, earned: match ? rule.points : 0, condition: rule.condition });
  }
  return breakdown;
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
 * Get distinct primary_source values from leads.
 */
function getDistinctSources() {
  const db = getDb();
  return db.prepare(`
    SELECT DISTINCT primary_source FROM leads
    WHERE primary_source IS NOT NULL AND primary_source != ''
    ORDER BY primary_source
  `).all().map(r => r.primary_source);
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

/**
 * Get daily growth data for sparkline charts.
 * Returns leads created per day and cumulative totals.
 */
function getDailyGrowth(days = 30) {
  const db = getDb();
  return db.prepare(`
    SELECT date(created_at) as day,
           COUNT(*) as created,
           SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email,
           SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) as with_phone,
           SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) as with_website
    FROM leads
    WHERE created_at >= date('now', '-' || ? || ' days')
    GROUP BY date(created_at)
    ORDER BY day ASC
  `).all(days);
}

/**
 * Get field completeness breakdown for the whole DB.
 */
function getFieldCompleteness() {
  const db = getDb();
  const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
  if (total === 0) return { total: 0, fields: {} };
  const fields = {};
  const cols = ['email', 'phone', 'website', 'firm_name', 'bar_number', 'practice_area', 'title', 'linkedin_url', 'bio', 'education'];
  for (const col of cols) {
    const count = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE ${col} IS NOT NULL AND ${col} != ''`).get().c;
    fields[col] = { count, pct: Math.round(100 * count / total) };
  }
  return { total, fields };
}

/**
 * Update a single lead's fields. Only updates provided non-null fields.
 */
function updateLead(id, updates) {
  const db = getDb();
  const allowedFields = [
    'first_name', 'last_name', 'firm_name', 'city', 'state', 'country',
    'phone', 'email', 'website', 'bar_number', 'bar_status', 'admission_date',
    'practice_area', 'title', 'linkedin_url', 'bio', 'education', 'languages',
    'tags', 'notes',
  ];
  // Get existing lead for changelog
  const existing = db.prepare('SELECT * FROM leads WHERE id = ?').get(id);
  if (!existing) return { updated: false };

  const sets = [];
  const values = [];
  const changes = [];
  for (const field of allowedFields) {
    if (updates[field] !== undefined) {
      const oldVal = existing[field] || '';
      const newVal = updates[field] || '';
      if (oldVal !== newVal) {
        changes.push({ field, oldValue: oldVal, newValue: newVal });
      }
      sets.push(`${field} = ?`);
      values.push(updates[field]);
    }
  }
  if (sets.length === 0) return { updated: false };
  sets.push("updated_at = CURRENT_TIMESTAMP");
  values.push(id);
  db.prepare(`UPDATE leads SET ${sets.join(', ')} WHERE id = ?`).run(...values);

  // Log changes
  for (const c of changes) {
    logChange(id, 'update', c.field, c.oldValue, c.newValue, 'manual');
  }

  return { updated: true, changes: changes.length };
}

/**
 * Find potential duplicates for a lead (fuzzy matching).
 */
function findPotentialDuplicates(limit = 100) {
  const db = getDb();
  // Find leads with same last_name + first initial + city (but different IDs)
  return db.prepare(`
    SELECT a.id as id1, b.id as id2,
           a.first_name as fn1, a.last_name as ln1, a.city as city1, a.state as state1,
           a.email as email1, a.phone as phone1, a.firm_name as firm1,
           b.first_name as fn2, b.last_name as ln2, b.city as city2, b.state as state2,
           b.email as email2, b.phone as phone2, b.firm_name as firm2,
           CASE
             WHEN LOWER(a.email) = LOWER(b.email) AND a.email != '' THEN 'email'
             WHEN a.phone = b.phone AND a.phone != '' THEN 'phone'
             ELSE 'name+city'
           END as match_type
    FROM leads a
    JOIN leads b ON a.id < b.id
    WHERE (
      (LOWER(a.email) = LOWER(b.email) AND a.email IS NOT NULL AND a.email != '')
      OR (a.phone = b.phone AND a.phone IS NOT NULL AND a.phone != '' AND length(a.phone) >= 7)
      OR (
        LOWER(a.last_name) = LOWER(b.last_name)
        AND LOWER(a.first_name) = LOWER(b.first_name)
        AND LOWER(a.city) = LOWER(b.city)
        AND a.state = b.state
        AND a.last_name != '' AND a.first_name != ''
      )
    )
    LIMIT ?
  `).all(limit);
}

/**
 * Merge two leads: keep the one with more data, delete the other.
 */
function mergeLeadPair(keepId, deleteId) {
  const db = getDb();
  const keep = db.prepare("SELECT * FROM leads WHERE id = ?").get(keepId);
  const del = db.prepare("SELECT * FROM leads WHERE id = ?").get(deleteId);
  if (!keep || !del) return { merged: false, reason: 'Lead not found' };

  const fieldsToMerge = [
    'phone', 'email', 'website', 'firm_name', 'bar_number', 'bar_status',
    'admission_date', 'practice_area', 'title', 'linkedin_url', 'bio',
    'education', 'languages', 'practice_specialties', 'profile_url',
    'email_source', 'phone_source', 'website_source', 'tags', 'notes',
  ];

  const updates = {};
  for (const field of fieldsToMerge) {
    if (del[field] && (!keep[field] || keep[field] === '')) {
      updates[field] = del[field];
    }
  }

  const txn = db.transaction(() => {
    // Merge fields
    if (Object.keys(updates).length > 0) {
      const sets = Object.keys(updates).map(k => `${k} = ?`);
      sets.push("updated_at = CURRENT_TIMESTAMP");
      db.prepare(`UPDATE leads SET ${sets.join(', ')} WHERE id = ?`)
        .run(...Object.values(updates), keepId);
    }
    // Transfer sources
    const sources = db.prepare("SELECT source FROM lead_sources WHERE lead_id = ?").all(deleteId);
    for (const s of sources) {
      try {
        db.prepare("INSERT OR IGNORE INTO lead_sources (lead_id, source) VALUES (?, ?)").run(keepId, s.source);
      } catch {}
    }
    // Delete the duplicate
    db.prepare("DELETE FROM lead_sources WHERE lead_id = ?").run(deleteId);
    db.prepare("DELETE FROM leads WHERE id = ?").run(deleteId);
  });
  txn();

  // Log the merge
  logChange(keepId, 'merge', 'merged_with', String(deleteId), null, 'manual');
  return { merged: true, fieldsRecovered: Object.keys(updates).length, updatedFields: Object.keys(updates) };
}

function getMergePreview(keepId, deleteId) {
  const db = getDb();
  const keep = db.prepare("SELECT * FROM leads WHERE id = ?").get(keepId);
  const del = db.prepare("SELECT * FROM leads WHERE id = ?").get(deleteId);
  if (!keep || !del) return null;

  const fieldsToMerge = [
    'phone', 'email', 'website', 'firm_name', 'bar_number', 'bar_status',
    'admission_date', 'practice_area', 'title', 'linkedin_url', 'bio',
    'education', 'languages', 'practice_specialties', 'profile_url',
  ];

  const preview = [];
  for (const field of fieldsToMerge) {
    const keepVal = keep[field] || '';
    const delVal = del[field] || '';
    const willMerge = delVal && !keepVal;
    preview.push({ field, keepValue: keepVal, deleteValue: delVal, willMerge });
  }
  return { keep, delete: del, preview, willRecover: preview.filter(p => p.willMerge).length };
}

function autoMergeDuplicates(confidenceThreshold = 90) {
  const db = getDb();
  const dupes = findPotentialDuplicates(500);
  let merged = 0;
  let fieldsRecovered = 0;

  const txn = db.transaction(() => {
    for (const dupe of dupes) {
      // Calculate confidence
      let confidence = 0;
      if (dupe.match_type === 'email') confidence = 100;
      else if (dupe.match_type === 'phone') confidence = 95;
      else confidence = 80; // name+city

      if (confidence < confidenceThreshold) continue;

      // Determine which to keep (prefer the one with more data)
      const lead1 = db.prepare("SELECT * FROM leads WHERE id = ?").get(dupe.id1);
      const lead2 = db.prepare("SELECT * FROM leads WHERE id = ?").get(dupe.id2);
      if (!lead1 || !lead2) continue;

      const score1 = computeLeadScore(lead1);
      const score2 = computeLeadScore(lead2);
      const keepId = score1 >= score2 ? dupe.id1 : dupe.id2;
      const deleteId = score1 >= score2 ? dupe.id2 : dupe.id1;

      const result = mergeLeadPair(keepId, deleteId);
      if (result.merged) {
        merged++;
        fieldsRecovered += result.fieldsRecovered;
      }
    }
  });
  txn();
  return { merged, fieldsRecovered };
}

// ============================================
// Lead Lists / Campaigns
// ============================================

function createList(name, description = '', color = '#6366f1') {
  const db = getDb();
  const result = db.prepare("INSERT INTO lead_lists (name, description, color) VALUES (?, ?, ?)").run(name, description, color);
  return { id: result.lastInsertRowid, name, description, color };
}

function getLists() {
  const db = getDb();
  return db.prepare(`
    SELECT ll.*, COUNT(lm.lead_id) as member_count
    FROM lead_lists ll
    LEFT JOIN list_members lm ON ll.id = lm.list_id
    GROUP BY ll.id
    ORDER BY ll.updated_at DESC
  `).all();
}

function getList(id) {
  const db = getDb();
  const list = db.prepare("SELECT * FROM lead_lists WHERE id = ?").get(id);
  if (!list) return null;
  const members = db.prepare(`
    SELECT l.* FROM leads l
    JOIN list_members lm ON l.id = lm.lead_id
    WHERE lm.list_id = ?
    ORDER BY lm.added_at DESC
  `).all(id);
  return { ...list, members };
}

function updateList(id, updates) {
  const db = getDb();
  const sets = [];
  const values = [];
  if (updates.name !== undefined) { sets.push("name = ?"); values.push(updates.name); }
  if (updates.description !== undefined) { sets.push("description = ?"); values.push(updates.description); }
  if (updates.color !== undefined) { sets.push("color = ?"); values.push(updates.color); }
  if (sets.length === 0) return { updated: false };
  sets.push("updated_at = CURRENT_TIMESTAMP");
  values.push(id);
  db.prepare(`UPDATE lead_lists SET ${sets.join(', ')} WHERE id = ?`).run(...values);
  return { updated: true };
}

function deleteList(id) {
  const db = getDb();
  db.prepare("DELETE FROM list_members WHERE list_id = ?").run(id);
  db.prepare("DELETE FROM lead_lists WHERE id = ?").run(id);
  return { deleted: true };
}

function addToList(listId, leadIds) {
  const db = getDb();
  let added = 0;
  const txn = db.transaction(() => {
    for (const leadId of leadIds) {
      try {
        db.prepare("INSERT OR IGNORE INTO list_members (list_id, lead_id) VALUES (?, ?)").run(listId, leadId);
        added++;
      } catch {}
    }
    db.prepare("UPDATE lead_lists SET updated_at = CURRENT_TIMESTAMP WHERE id = ?").run(listId);
  });
  txn();
  return { added };
}

function removeFromList(listId, leadIds) {
  const db = getDb();
  let removed = 0;
  const txn = db.transaction(() => {
    for (const leadId of leadIds) {
      const r = db.prepare("DELETE FROM list_members WHERE list_id = ? AND lead_id = ?").run(listId, leadId);
      removed += r.changes;
    }
  });
  txn();
  return { removed };
}

function getLeadLists(leadId) {
  const db = getDb();
  return db.prepare(`
    SELECT ll.id, ll.name, ll.color
    FROM lead_lists ll
    JOIN list_members lm ON ll.id = lm.list_id
    WHERE lm.lead_id = ?
  `).all(leadId);
}

/**
 * Get scraper health stats from scrape_runs.
 */
function getScraperHealth() {
  const db = getDb();
  return db.prepare(`
    SELECT state,
      COUNT(*) as total_runs,
      SUM(CASE WHEN leads_found > 0 THEN 1 ELSE 0 END) as successful_runs,
      ROUND(100.0 * SUM(CASE WHEN leads_found > 0 THEN 1 ELSE 0 END) / COUNT(*)) as success_rate,
      ROUND(AVG(leads_found)) as avg_leads,
      ROUND(AVG(leads_new)) as avg_new,
      MAX(completed_at) as last_run,
      SUM(leads_found) as total_leads
    FROM scrape_runs
    WHERE completed_at IS NOT NULL
    GROUP BY state
    ORDER BY total_runs DESC
  `).all();
}

/**
 * Get enrichment summary stats — how many leads have been through each enrichment step
 */
function getEnrichmentStats() {
  const db = getDb();
  const total = db.prepare('SELECT COUNT(*) as cnt FROM leads').get().cnt;
  const withEmail = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email IS NOT NULL AND email != ''").get().cnt;
  const withPhone = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE phone IS NOT NULL AND phone != ''").get().cnt;
  const withWebsite = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE website IS NOT NULL AND website != ''").get().cnt;
  const enriched = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE enrichment_steps IS NOT NULL AND enrichment_steps != ''").get().cnt;

  // Count per enrichment step
  const steps = {};
  const rows = db.prepare("SELECT enrichment_steps FROM leads WHERE enrichment_steps IS NOT NULL AND enrichment_steps != ''").all();
  rows.forEach(r => {
    r.enrichment_steps.split(',').forEach(s => {
      const step = s.trim();
      if (step) steps[step] = (steps[step] || 0) + 1;
    });
  });

  // Source attribution
  const emailSources = db.prepare(`
    SELECT email_source, COUNT(*) as cnt
    FROM leads WHERE email_source IS NOT NULL AND email_source != ''
    GROUP BY email_source ORDER BY cnt DESC
  `).all();
  const phoneSources = db.prepare(`
    SELECT phone_source, COUNT(*) as cnt
    FROM leads WHERE phone_source IS NOT NULL AND phone_source != ''
    GROUP BY phone_source ORDER BY cnt DESC
  `).all();
  const websiteSources = db.prepare(`
    SELECT website_source, COUNT(*) as cnt
    FROM leads WHERE website_source IS NOT NULL AND website_source != ''
    GROUP BY website_source ORDER BY cnt DESC
  `).all();

  // Enrichment opportunities: leads that could benefit from more enrichment
  const needsEmail = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE (email IS NULL OR email = '')").get().cnt;
  const needsPhone = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE (phone IS NULL OR phone = '')").get().cnt;
  const hasWebsiteNoEmail = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE website IS NOT NULL AND website != '' AND (email IS NULL OR email = '')").get().cnt;
  const hasProfileNoEmail = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE profile_url IS NOT NULL AND profile_url != '' AND (email IS NULL OR email = '')").get().cnt;

  return {
    total, withEmail, withPhone, withWebsite, enriched,
    steps,
    emailSources, phoneSources, websiteSources,
    opportunities: { needsEmail, needsPhone, hasWebsiteNoEmail, hasProfileNoEmail },
  };
}

/**
 * Get activity feed — recent scrape runs, enrichments, and imports
 */
function getActivityFeed(limit = 50) {
  const db = getDb();
  const scrapes = db.prepare(`
    SELECT 'scrape' as type, state, leads_found, leads_new, emails_found,
           started_at, completed_at,
           ROUND((julianday(completed_at) - julianday(started_at)) * 86400) as duration_secs
    FROM scrape_runs
    WHERE completed_at IS NOT NULL
    ORDER BY completed_at DESC
    LIMIT ?
  `).all(limit);

  return scrapes.map(s => ({
    ...s,
    duration: s.duration_secs ? formatDuration(s.duration_secs) : null,
  }));
}

function formatDuration(secs) {
  if (secs < 60) return `${Math.round(secs)}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${Math.round(secs % 60)}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

// ============================================================
// CHANGELOG / AUDIT TRAIL
// ============================================================

/**
 * Log a change to a lead (called internally when updating leads)
 */
function logChange(leadId, action, field, oldValue, newValue, source = 'manual') {
  const db = getDb();
  db.prepare(`
    INSERT INTO lead_changelog (lead_id, action, field, old_value, new_value, source)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(leadId, action, field || null, oldValue || null, newValue || null, source);
}

/**
 * Get changelog for a specific lead
 */
function getLeadChangelog(leadId, limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT * FROM lead_changelog
    WHERE lead_id = ?
    ORDER BY created_at DESC
    LIMIT ?
  `).all(leadId, limit);
}

/**
 * Get recent changes across all leads
 */
function getRecentChanges(limit = 100) {
  const db = getDb();
  return db.prepare(`
    SELECT c.*, l.first_name, l.last_name, l.state
    FROM lead_changelog c
    LEFT JOIN leads l ON c.lead_id = l.id
    ORDER BY c.created_at DESC
    LIMIT ?
  `).all(limit);
}

// ============================================================
// EXPORT HISTORY
// ============================================================

function recordExport(format, leadCount, filters, filename) {
  const db = getDb();
  db.prepare(`
    INSERT INTO export_history (format, lead_count, filters, filename)
    VALUES (?, ?, ?, ?)
  `).run(format, leadCount, JSON.stringify(filters || {}), filename || null);
}

function getExportHistory(limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT * FROM export_history
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limit);
}

// ============================================================
// DATA QUALITY ALERTS
// ============================================================

/**
 * Run quality checks on all leads and generate alerts
 */
function runQualityChecks() {
  const db = getDb();
  let alertsCreated = 0;

  // Clear old unresolved alerts before re-running
  db.prepare("DELETE FROM quality_alerts WHERE resolved = 0").run();

  // 1. Invalid email patterns
  const badEmails = db.prepare(`
    SELECT id, email, first_name, last_name FROM leads
    WHERE email IS NOT NULL AND email != ''
    AND (email NOT LIKE '%@%.%'
      OR email LIKE '%@example.%'
      OR email LIKE '%@test.%'
      OR email LIKE '%@localhost%'
      OR email LIKE 'noreply@%'
      OR email LIKE 'no-reply@%'
      OR email LIKE 'info@%'
      OR email LIKE 'admin@%'
      OR email LIKE 'office@%')
  `).all();
  const insertAlert = db.prepare(`
    INSERT INTO quality_alerts (lead_id, alert_type, severity, message) VALUES (?, ?, ?, ?)
  `);
  for (const l of badEmails) {
    const isGeneric = /^(info|admin|office|noreply|no-reply)@/i.test(l.email);
    const severity = isGeneric ? 'warning' : 'error';
    const msg = isGeneric
      ? `Generic email (${l.email}) — likely not personal`
      : `Suspicious email format: ${l.email}`;
    insertAlert.run(l.id, 'bad-email', severity, msg);
    alertsCreated++;
  }

  // 2. Duplicate emails (same email on multiple leads)
  const dupeEmails = db.prepare(`
    SELECT email, COUNT(*) as cnt FROM leads
    WHERE email IS NOT NULL AND email != ''
    GROUP BY email HAVING cnt > 1
    ORDER BY cnt DESC LIMIT 100
  `).all();
  for (const d of dupeEmails) {
    const leads = db.prepare('SELECT id FROM leads WHERE email = ?').all(d.email);
    for (const l of leads) {
      insertAlert.run(l.id, 'duplicate-email', 'warning', `Email ${d.email} shared by ${d.cnt} leads`);
      alertsCreated++;
    }
  }

  // 3. Leads with profile_url but no email (enrichment opportunity)
  const enrichOpps = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE profile_url IS NOT NULL AND profile_url != ''
    AND (email IS NULL OR email = '')
  `).get();
  if (enrichOpps.cnt > 0) {
    insertAlert.run(null, 'enrichment-opportunity', 'info',
      `${enrichOpps.cnt} leads have profile URLs but no email — run waterfall enrichment`);
    alertsCreated++;
  }

  // 4. Stale data (leads not updated in 30+ days)
  const staleCount = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE updated_at < datetime('now', '-30 days')
  `).get();
  if (staleCount.cnt > 100) {
    insertAlert.run(null, 'stale-data', 'info',
      `${staleCount.cnt} leads haven't been updated in 30+ days`);
    alertsCreated++;
  }

  // 5. Low score leads (score 0-10 with missing critical data)
  const lowScoreCount = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE lead_score <= 10 AND lead_score >= 0
  `).get();
  if (lowScoreCount.cnt > 50) {
    insertAlert.run(null, 'low-quality-batch', 'warning',
      `${lowScoreCount.cnt} leads have very low quality scores (0-10)`);
    alertsCreated++;
  }

  return { alertsCreated };
}

function getQualityAlerts(options = {}) {
  const db = getDb();
  const { resolved = false, type, limit = 100 } = options;
  let where = [];
  let params = [];

  where.push('resolved = ?');
  params.push(resolved ? 1 : 0);

  if (type) {
    where.push('alert_type = ?');
    params.push(type);
  }

  params.push(limit);
  return db.prepare(`
    SELECT a.*, l.first_name, l.last_name, l.state, l.email as lead_email
    FROM quality_alerts a
    LEFT JOIN leads l ON a.lead_id = l.id
    WHERE ${where.join(' AND ')}
    ORDER BY a.created_at DESC
    LIMIT ?
  `).all(...params);
}

function resolveAlert(alertId) {
  const db = getDb();
  db.prepare('UPDATE quality_alerts SET resolved = 1 WHERE id = ?').run(alertId);
}

function getAlertSummary() {
  const db = getDb();
  return db.prepare(`
    SELECT alert_type, severity, COUNT(*) as cnt
    FROM quality_alerts WHERE resolved = 0
    GROUP BY alert_type, severity
    ORDER BY cnt DESC
  `).all();
}

/**
 * Get top firms with lead counts and field coverage.
 */
/**
 * Compute overall database health score (0-100).
 * Factors: email coverage, phone coverage, website coverage, freshness, score distribution, alert count
 */
function getDatabaseHealth() {
  const db = getDb();
  const total = db.prepare('SELECT COUNT(*) as cnt FROM leads').get().cnt;
  if (total === 0) return { score: 0, grade: 'N/A', factors: {} };

  const emailPct = db.prepare("SELECT 100.0 * SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as pct FROM leads").get().pct || 0;
  const phonePct = db.prepare("SELECT 100.0 * SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as pct FROM leads").get().pct || 0;
  const websitePct = db.prepare("SELECT 100.0 * SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as pct FROM leads").get().pct || 0;
  const avgScore = db.prepare("SELECT AVG(lead_score) as avg FROM leads").get().avg || 0;

  // Freshness: how many leads were updated in the last 7 days
  const freshPct = db.prepare("SELECT 100.0 * SUM(CASE WHEN updated_at >= datetime('now', '-7 days') THEN 1 ELSE 0 END) / COUNT(*) as pct FROM leads").get().pct || 0;

  // Alert count penalty
  let alertPenalty = 0;
  try {
    const alertCount = db.prepare("SELECT COUNT(*) as cnt FROM quality_alerts WHERE resolved = 0").get().cnt;
    alertPenalty = Math.min(alertCount * 0.5, 15); // Max 15 point penalty
  } catch {}

  // Compute weighted score
  const emailScore = Math.min(emailPct / 50 * 25, 25);       // 25 pts max (50% email = full score)
  const phoneScore = Math.min(phonePct / 50 * 20, 20);       // 20 pts max
  const websiteScore = Math.min(websitePct / 30 * 15, 15);   // 15 pts max
  const qualityScore = Math.min(avgScore / 70 * 20, 20);     // 20 pts max (avg 70 = full)
  const freshScore = Math.min(freshPct / 50 * 20, 20);       // 20 pts max

  const raw = emailScore + phoneScore + websiteScore + qualityScore + freshScore - alertPenalty;
  const score = Math.max(0, Math.min(100, Math.round(raw)));

  const grade = score >= 80 ? 'A' : score >= 65 ? 'B' : score >= 50 ? 'C' : score >= 35 ? 'D' : 'F';

  return {
    score, grade, total,
    factors: {
      email: { pct: Math.round(emailPct), score: Math.round(emailScore), max: 25 },
      phone: { pct: Math.round(phonePct), score: Math.round(phoneScore), max: 20 },
      website: { pct: Math.round(websitePct), score: Math.round(websiteScore), max: 15 },
      quality: { avg: Math.round(avgScore), score: Math.round(qualityScore), max: 20 },
      freshness: { pct: Math.round(freshPct), score: Math.round(freshScore), max: 20 },
      alerts: { penalty: Math.round(alertPenalty) },
    },
  };
}

/**
 * Find leads similar to a given lead (same city+practice, same firm, or same state+practice).
 */
function findSimilarLeads(leadId, limit = 10) {
  const db = getDb();
  const lead = db.prepare('SELECT * FROM leads WHERE id = ?').get(leadId);
  if (!lead) return [];

  // Find by same firm (but different person)
  const sameFirm = lead.firm_name ? db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, lead_score
    FROM leads WHERE firm_name = ? AND id != ? LIMIT 5
  `).all(lead.firm_name, leadId) : [];

  // Find by same city + state (different person)
  const sameCity = (lead.city && lead.state) ? db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, lead_score
    FROM leads WHERE city = ? AND state = ? AND id != ?
    AND id NOT IN (${sameFirm.map(l => l.id).join(',') || 0})
    ORDER BY lead_score DESC LIMIT 5
  `).all(lead.city, lead.state, leadId) : [];

  return { sameFirm, sameCity };
}

function getTopFirms(limit = 20) {
  const db = getDb();
  return db.prepare(`
    SELECT
      firm_name,
      COUNT(*) as lead_count,
      GROUP_CONCAT(DISTINCT state) as states,
      SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) as with_website,
      MIN(website) as sample_website
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_name != 'N/A'
    GROUP BY firm_name
    HAVING lead_count >= 3
    ORDER BY lead_count DESC
    LIMIT ?
  `).all(limit);
}

// ── Pipeline stage management ──

const PIPELINE_STAGES = ['new', 'contacted', 'replied', 'meeting', 'client'];

function getPipelineStats() {
  const db = getDb();
  const stages = {};
  for (const stage of PIPELINE_STAGES) {
    const row = db.prepare(`
      SELECT COUNT(*) as count,
        SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email
      FROM leads WHERE pipeline_stage = ?
    `).get(stage);
    stages[stage] = { count: row.count, withEmail: row.with_email };
  }
  // Conversion rates
  const total = Object.values(stages).reduce((s, v) => s + v.count, 0);
  return { stages, total, stageOrder: PIPELINE_STAGES };
}

function getLeadsByStage(stage, limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, website,
      lead_score, pipeline_stage, practice_area, tags, updated_at
    FROM leads WHERE pipeline_stage = ?
    ORDER BY lead_score DESC, updated_at DESC
    LIMIT ?
  `).all(stage, limit);
}

function moveLeadToStage(leadId, stage) {
  if (!PIPELINE_STAGES.includes(stage)) throw new Error('Invalid stage: ' + stage);
  const db = getDb();
  const old = db.prepare('SELECT pipeline_stage FROM leads WHERE id = ?').get(leadId);
  if (!old) throw new Error('Lead not found');
  db.prepare('UPDATE leads SET pipeline_stage = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?').run(stage, leadId);
  logChange(leadId, 'stage_change', 'pipeline_stage', old.pipeline_stage, stage, 'manual');
  return { id: leadId, oldStage: old.pipeline_stage, newStage: stage };
}

function bulkMoveToStage(leadIds, stage) {
  if (!PIPELINE_STAGES.includes(stage)) throw new Error('Invalid stage: ' + stage);
  const db = getDb();
  const move = db.prepare('UPDATE leads SET pipeline_stage = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?');
  const getStage = db.prepare('SELECT pipeline_stage FROM leads WHERE id = ?');
  let moved = 0;
  const txn = db.transaction(() => {
    for (const id of leadIds) {
      const old = getStage.get(id);
      if (old && old.pipeline_stage !== stage) {
        move.run(stage, id);
        logChange(id, 'stage_change', 'pipeline_stage', old.pipeline_stage, stage, 'bulk');
        moved++;
      }
    }
  });
  txn();
  return { moved };
}

// ── Scheduled scrapes ──

function initSchedulesTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS scrape_schedules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      state TEXT NOT NULL,
      practice_area TEXT DEFAULT '',
      frequency TEXT NOT NULL DEFAULT 'weekly',
      day_of_week INTEGER DEFAULT 1,
      hour INTEGER DEFAULT 6,
      enabled INTEGER DEFAULT 1,
      last_run_at DATETIME,
      next_run_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
}

function getSchedules() {
  initSchedulesTable();
  const db = getDb();
  return db.prepare('SELECT * FROM scrape_schedules ORDER BY state').all();
}

function createSchedule(opts) {
  initSchedulesTable();
  const db = getDb();
  const { state, practiceArea = '', frequency = 'weekly', dayOfWeek = 1, hour = 6 } = opts;
  const nextRun = computeNextRun(frequency, dayOfWeek, hour);
  const result = db.prepare(`
    INSERT INTO scrape_schedules (state, practice_area, frequency, day_of_week, hour, next_run_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(state, practiceArea, frequency, dayOfWeek, hour, nextRun);
  return { id: result.lastInsertRowid };
}

function updateSchedule(id, updates) {
  initSchedulesTable();
  const db = getDb();
  const allowed = ['frequency', 'day_of_week', 'hour', 'enabled', 'practice_area'];
  const sets = [];
  const params = [];
  for (const [k, v] of Object.entries(updates)) {
    if (allowed.includes(k)) { sets.push(`${k} = ?`); params.push(v); }
  }
  if (sets.length === 0) return { updated: false };
  params.push(id);
  db.prepare(`UPDATE scrape_schedules SET ${sets.join(', ')} WHERE id = ?`).run(...params);
  // Recompute next_run_at
  const sched = db.prepare('SELECT * FROM scrape_schedules WHERE id = ?').get(id);
  if (sched) {
    const nextRun = computeNextRun(sched.frequency, sched.day_of_week, sched.hour);
    db.prepare('UPDATE scrape_schedules SET next_run_at = ? WHERE id = ?').run(nextRun, id);
  }
  return { updated: true };
}

function deleteSchedule(id) {
  initSchedulesTable();
  const db = getDb();
  db.prepare('DELETE FROM scrape_schedules WHERE id = ?').run(id);
  return { deleted: true };
}

function markScheduleRun(id) {
  initSchedulesTable();
  const db = getDb();
  const sched = db.prepare('SELECT * FROM scrape_schedules WHERE id = ?').get(id);
  if (!sched) return;
  const nextRun = computeNextRun(sched.frequency, sched.day_of_week, sched.hour);
  db.prepare('UPDATE scrape_schedules SET last_run_at = CURRENT_TIMESTAMP, next_run_at = ? WHERE id = ?').run(nextRun, id);
}

function getDueSchedules() {
  initSchedulesTable();
  const db = getDb();
  return db.prepare(`
    SELECT * FROM scrape_schedules
    WHERE enabled = 1 AND (next_run_at IS NULL OR next_run_at <= datetime('now'))
    ORDER BY next_run_at ASC
  `).all();
}

function computeNextRun(frequency, dayOfWeek, hour) {
  const now = new Date();
  const next = new Date(now);
  next.setMinutes(0, 0, 0);
  next.setHours(hour);
  if (frequency === 'daily') {
    if (next <= now) next.setDate(next.getDate() + 1);
  } else if (frequency === 'weekly') {
    const diff = (dayOfWeek - now.getDay() + 7) % 7 || 7;
    next.setDate(now.getDate() + diff);
    if (next <= now) next.setDate(next.getDate() + 7);
  } else if (frequency === 'monthly') {
    next.setDate(1);
    next.setMonth(next.getMonth() + 1);
  }
  return next.toISOString();
}

// ── Smart segments ──

function initSegmentsTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS saved_segments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      filters TEXT NOT NULL,
      color TEXT DEFAULT '#6366f1',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
}

function getSegments() {
  initSegmentsTable();
  const db = getDb();
  const segs = db.prepare('SELECT * FROM saved_segments ORDER BY name').all();
  return segs.map(s => ({ ...s, filters: JSON.parse(s.filters) }));
}

function createSegment(name, description, filters, color = '#6366f1') {
  initSegmentsTable();
  const db = getDb();
  const result = db.prepare(`
    INSERT INTO saved_segments (name, description, filters, color) VALUES (?, ?, ?, ?)
  `).run(name, description, JSON.stringify(filters), color);
  return { id: result.lastInsertRowid };
}

function updateSegment(id, updates) {
  initSegmentsTable();
  const db = getDb();
  const sets = [];
  const params = [];
  if (updates.name) { sets.push('name = ?'); params.push(updates.name); }
  if (updates.description !== undefined) { sets.push('description = ?'); params.push(updates.description); }
  if (updates.filters) { sets.push('filters = ?'); params.push(JSON.stringify(updates.filters)); }
  if (updates.color) { sets.push('color = ?'); params.push(updates.color); }
  if (sets.length === 0) return { updated: false };
  sets.push('updated_at = CURRENT_TIMESTAMP');
  params.push(id);
  db.prepare(`UPDATE saved_segments SET ${sets.join(', ')} WHERE id = ?`).run(...params);
  return { updated: true };
}

function deleteSegment(id) {
  initSegmentsTable();
  const db = getDb();
  db.prepare('DELETE FROM saved_segments WHERE id = ?').run(id);
  return { deleted: true };
}

function querySegment(filters) {
  const db = getDb();
  const where = [];
  const params = [];

  for (const condition of filters) {
    const { field, operator, value } = condition;
    const safeFields = ['state', 'country', 'city', 'firm_name', 'practice_area', 'email', 'phone',
      'website', 'tags', 'primary_source', 'pipeline_stage', 'lead_score', 'bar_status',
      'email_source', 'phone_source', 'website_source', 'title'];
    if (!safeFields.includes(field)) continue;

    switch (operator) {
      case 'equals':
        where.push(`${field} = ?`); params.push(value); break;
      case 'not_equals':
        where.push(`${field} != ?`); params.push(value); break;
      case 'contains':
        where.push(`${field} LIKE ?`); params.push(`%${value}%`); break;
      case 'starts_with':
        where.push(`${field} LIKE ?`); params.push(`${value}%`); break;
      case 'is_empty':
        where.push(`(${field} IS NULL OR ${field} = '')`); break;
      case 'is_not_empty':
        where.push(`(${field} IS NOT NULL AND ${field} != '')`); break;
      case 'greater_than':
        where.push(`CAST(${field} AS REAL) > ?`); params.push(Number(value)); break;
      case 'less_than':
        where.push(`CAST(${field} AS REAL) < ?`); params.push(Number(value)); break;
      case 'in':
        if (Array.isArray(value) && value.length > 0) {
          where.push(`${field} IN (${value.map(() => '?').join(',')})`);
          params.push(...value);
        }
        break;
    }
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
  const count = db.prepare(`SELECT COUNT(*) as cnt FROM leads ${whereClause}`).get(...params);
  return { count: count.cnt, filters };
}

function querySegmentLeads(filters, limit = 100, offset = 0) {
  const db = getDb();
  const where = [];
  const params = [];

  for (const condition of filters) {
    const { field, operator, value } = condition;
    const safeFields = ['state', 'country', 'city', 'firm_name', 'practice_area', 'email', 'phone',
      'website', 'tags', 'primary_source', 'pipeline_stage', 'lead_score', 'bar_status',
      'email_source', 'phone_source', 'website_source', 'title'];
    if (!safeFields.includes(field)) continue;

    switch (operator) {
      case 'equals':
        where.push(`${field} = ?`); params.push(value); break;
      case 'not_equals':
        where.push(`${field} != ?`); params.push(value); break;
      case 'contains':
        where.push(`${field} LIKE ?`); params.push(`%${value}%`); break;
      case 'starts_with':
        where.push(`${field} LIKE ?`); params.push(`${value}%`); break;
      case 'is_empty':
        where.push(`(${field} IS NULL OR ${field} = '')`); break;
      case 'is_not_empty':
        where.push(`(${field} IS NOT NULL AND ${field} != '')`); break;
      case 'greater_than':
        where.push(`CAST(${field} AS REAL) > ?`); params.push(Number(value)); break;
      case 'less_than':
        where.push(`CAST(${field} AS REAL) < ?`); params.push(Number(value)); break;
      case 'in':
        if (Array.isArray(value) && value.length > 0) {
          where.push(`${field} IN (${value.map(() => '?').join(',')})`);
          params.push(...value);
        }
        break;
    }
  }

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
  return db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, website,
      lead_score, pipeline_stage, practice_area, tags, primary_source, updated_at
    FROM leads ${whereClause}
    ORDER BY lead_score DESC
    LIMIT ? OFFSET ?
  `).all(...params, limit, offset);
}

// ── Webhooks ──

function initWebhooksTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS webhooks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT NOT NULL,
      events TEXT NOT NULL,
      secret TEXT DEFAULT '',
      enabled INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS webhook_deliveries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      webhook_id INTEGER NOT NULL,
      event TEXT NOT NULL,
      status_code INTEGER,
      response TEXT,
      success INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE
    );
  `);
}

function getWebhooks() {
  initWebhooksTable();
  const db = getDb();
  return db.prepare('SELECT * FROM webhooks ORDER BY created_at DESC').all().map(w => ({
    ...w, events: JSON.parse(w.events)
  }));
}

function createWebhook(url, events, secret = '') {
  initWebhooksTable();
  const db = getDb();
  const result = db.prepare('INSERT INTO webhooks (url, events, secret) VALUES (?, ?, ?)').run(url, JSON.stringify(events), secret);
  return { id: result.lastInsertRowid };
}

function updateWebhook(id, updates) {
  initWebhooksTable();
  const db = getDb();
  const sets = [];
  const params = [];
  if (updates.url) { sets.push('url = ?'); params.push(updates.url); }
  if (updates.events) { sets.push('events = ?'); params.push(JSON.stringify(updates.events)); }
  if (updates.enabled !== undefined) { sets.push('enabled = ?'); params.push(updates.enabled ? 1 : 0); }
  if (updates.secret !== undefined) { sets.push('secret = ?'); params.push(updates.secret); }
  if (sets.length === 0) return { updated: false };
  params.push(id);
  db.prepare(`UPDATE webhooks SET ${sets.join(', ')} WHERE id = ?`).run(...params);
  return { updated: true };
}

function deleteWebhook(id) {
  initWebhooksTable();
  const db = getDb();
  db.prepare('DELETE FROM webhooks WHERE id = ?').run(id);
  return { deleted: true };
}

function getWebhooksByEvent(event) {
  initWebhooksTable();
  const db = getDb();
  const all = db.prepare("SELECT * FROM webhooks WHERE enabled = 1").all();
  return all.filter(w => {
    const events = JSON.parse(w.events);
    return events.includes(event) || events.includes('*');
  });
}

function logWebhookDelivery(webhookId, event, statusCode, response, success) {
  initWebhooksTable();
  const db = getDb();
  db.prepare('INSERT INTO webhook_deliveries (webhook_id, event, status_code, response, success) VALUES (?, ?, ?, ?, ?)').run(webhookId, event, statusCode, (response || '').slice(0, 500), success ? 1 : 0);
}

function getWebhookDeliveries(webhookId, limit = 20) {
  initWebhooksTable();
  const db = getDb();
  return db.prepare('SELECT * FROM webhook_deliveries WHERE webhook_id = ? ORDER BY created_at DESC LIMIT ?').all(webhookId, limit);
}

// ── Lead Notes ──

function initNotesTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS lead_notes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      content TEXT NOT NULL,
      author TEXT DEFAULT 'user',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_notes_lead ON lead_notes(lead_id);
  `);
}

function addNote(leadId, content, author = 'user') {
  initNotesTable();
  const db = getDb();
  const result = db.prepare('INSERT INTO lead_notes (lead_id, content, author) VALUES (?, ?, ?)').run(leadId, content, author);
  logChange(leadId, 'note_added', 'notes', null, content.slice(0, 100), author);
  return { id: result.lastInsertRowid };
}

function getLeadNotes(leadId) {
  initNotesTable();
  const db = getDb();
  return db.prepare('SELECT * FROM lead_notes WHERE lead_id = ? ORDER BY created_at DESC').all(leadId);
}

function deleteNote(noteId) {
  initNotesTable();
  const db = getDb();
  db.prepare('DELETE FROM lead_notes WHERE id = ?').run(noteId);
  return { deleted: true };
}

function getLeadTimeline(leadId, limit = 50) {
  initNotesTable();
  const db = getDb();
  // Combine changelog entries + notes into unified timeline
  const changes = db.prepare(`
    SELECT 'change' as type, id, action, field, old_value, new_value, source, created_at
    FROM lead_changelog WHERE lead_id = ? ORDER BY created_at DESC LIMIT ?
  `).all(leadId, limit);
  const notes = db.prepare(`
    SELECT 'note' as type, id, content, author, created_at
    FROM lead_notes WHERE lead_id = ? ORDER BY created_at DESC LIMIT ?
  `).all(leadId, limit);
  // Merge and sort by date
  const timeline = [...changes, ...notes].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  return timeline.slice(0, limit);
}

// ── Bulk updates ──

function bulkUpdateLeads(leadIds, updates) {
  const db = getDb();
  const allowed = ['pipeline_stage', 'tags', 'practice_area', 'city', 'state', 'firm_name', 'title'];
  const sets = [];
  const params = [];
  for (const [k, v] of Object.entries(updates)) {
    if (allowed.includes(k)) { sets.push(`${k} = ?`); params.push(v); }
  }
  if (sets.length === 0) return { updated: 0 };
  sets.push('updated_at = CURRENT_TIMESTAMP');

  const getStmt = db.prepare('SELECT * FROM leads WHERE id = ?');
  const updateStmt = db.prepare(`UPDATE leads SET ${sets.join(', ')} WHERE id = ?`);
  let updated = 0;

  const txn = db.transaction(() => {
    for (const id of leadIds) {
      const old = getStmt.get(id);
      if (!old) continue;
      updateStmt.run(...params, id);
      // Log changes
      for (const [k, v] of Object.entries(updates)) {
        if (allowed.includes(k) && old[k] !== v) {
          logChange(id, 'bulk_update', k, old[k], v, 'bulk');
        }
      }
      updated++;
    }
  });
  txn();
  return { updated };
}

// ── Email Verification Stats ──

function getVerificationStats() {
  const db = getDb();
  const total = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email IS NOT NULL AND email != ''").get().cnt;
  const verified = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email_verified = 1").get().cnt;
  const catchAll = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email_catch_all = 1").get().cnt;
  const invalid = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email_verified = -1").get().cnt;
  const unverified = total - verified - invalid;
  return { total, verified, invalid, catchAll, unverified };
}

function bulkImportVerification(verifications) {
  const db = getDb();
  const update = db.prepare('UPDATE leads SET email_verified = ?, email_catch_all = ?, updated_at = CURRENT_TIMESTAMP WHERE email = ?');
  let updated = 0;
  const txn = db.transaction(() => {
    for (const v of verifications) {
      const verified = v.valid ? 1 : (v.valid === false ? -1 : 0);
      const catchAll = v.catchAll ? 1 : 0;
      const result = update.run(verified, catchAll, v.email);
      if (result.changes > 0) updated++;
    }
  });
  txn();
  return { updated, total: verifications.length };
}

// ── Export templates ──

function initExportTemplatesTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS export_templates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      format TEXT NOT NULL DEFAULT 'csv',
      columns TEXT NOT NULL,
      column_renames TEXT DEFAULT '{}',
      filters TEXT DEFAULT '{}',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
  // Seed built-in templates if empty
  const count = db.prepare('SELECT COUNT(*) as cnt FROM export_templates').get();
  if (count.cnt === 0) {
    const insert = db.prepare('INSERT INTO export_templates (name, format, columns, column_renames) VALUES (?, ?, ?, ?)');
    insert.run('Instantly', 'csv',
      JSON.stringify(['email', 'first_name', 'last_name', 'firm_name', 'phone', 'website', 'city', 'state']),
      JSON.stringify({ firm_name: 'company_name' }));
    insert.run('SmartLead', 'csv',
      JSON.stringify(['email', 'first_name', 'last_name', 'firm_name', 'phone', 'city', 'state', 'tags']),
      JSON.stringify({ firm_name: 'company', city: 'location' }));
    insert.run('HubSpot', 'csv',
      JSON.stringify(['email', 'first_name', 'last_name', 'firm_name', 'phone', 'website', 'city', 'state', 'title', 'linkedin_url']),
      JSON.stringify({ firm_name: 'company', linkedin_url: 'linkedin' }));
    insert.run('Full Export', 'csv',
      JSON.stringify(['first_name', 'last_name', 'email', 'phone', 'website', 'firm_name', 'city', 'state', 'country', 'practice_area', 'title', 'bar_number', 'bar_status', 'lead_score', 'pipeline_stage', 'tags', 'primary_source', 'email_source', 'phone_source']),
      JSON.stringify({}));
  }
}

function getExportTemplates() {
  initExportTemplatesTable();
  const db = getDb();
  return db.prepare('SELECT * FROM export_templates ORDER BY name').all().map(t => ({
    ...t, columns: JSON.parse(t.columns), columnRenames: JSON.parse(t.column_renames), filters: JSON.parse(t.filters || '{}')
  }));
}

function createExportTemplate(name, columns, columnRenames = {}, filters = {}) {
  initExportTemplatesTable();
  const db = getDb();
  const result = db.prepare('INSERT INTO export_templates (name, columns, column_renames, filters) VALUES (?, ?, ?, ?)').run(
    name, JSON.stringify(columns), JSON.stringify(columnRenames), JSON.stringify(filters)
  );
  return { id: result.lastInsertRowid };
}

function deleteExportTemplate(id) {
  initExportTemplatesTable();
  const db = getDb();
  db.prepare('DELETE FROM export_templates WHERE id = ?').run(id);
  return { deleted: true };
}

// ── Pipeline Analytics ──

function getPipelineFunnel() {
  const db = getDb();
  const stages = ['new', 'contacted', 'replied', 'meeting', 'client'];
  const funnel = [];
  let prevCount = null;
  for (const stage of stages) {
    const row = db.prepare('SELECT COUNT(*) as count FROM leads WHERE pipeline_stage = ?').get(stage);
    const dropoff = prevCount !== null && prevCount > 0 ? ((1 - row.count / prevCount) * 100).toFixed(1) : null;
    funnel.push({ stage, count: row.count, dropoff: dropoff ? parseFloat(dropoff) : null });
    if (row.count > 0 || prevCount !== null) prevCount = row.count;
  }
  return funnel;
}

function getSourceEffectiveness() {
  const db = getDb();
  return db.prepare(`
    SELECT primary_source as source,
      COUNT(*) as total,
      AVG(lead_score) as avg_score,
      SUM(CASE WHEN email != '' AND email IS NOT NULL THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone != '' AND phone IS NOT NULL THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN website != '' AND website IS NOT NULL THEN 1 ELSE 0 END) as with_website,
      SUM(CASE WHEN pipeline_stage IN ('contacted','replied','meeting','client') THEN 1 ELSE 0 END) as engaged
    FROM leads
    WHERE primary_source IS NOT NULL AND primary_source != ''
    GROUP BY primary_source
    ORDER BY avg_score DESC
  `).all();
}

// ── Search Suggestions ──

function getSearchSuggestions(query, limit = 10) {
  if (!query || query.length < 2) return [];
  const db = getDb();
  const q = `%${query}%`;
  // Search across names, firms, cities
  const names = db.prepare(`
    SELECT DISTINCT first_name || ' ' || last_name as label, 'name' as type, id
    FROM leads WHERE first_name || ' ' || last_name LIKE ? LIMIT ?
  `).all(q, limit);
  const firms = db.prepare(`
    SELECT DISTINCT firm_name as label, 'firm' as type, MIN(id) as id
    FROM leads WHERE firm_name LIKE ? AND firm_name != '' GROUP BY firm_name LIMIT ?
  `).all(q, limit);
  const cities = db.prepare(`
    SELECT DISTINCT city || ', ' || state as label, 'city' as type, MIN(id) as id
    FROM leads WHERE city LIKE ? AND city != '' GROUP BY city, state LIMIT ?
  `).all(q, limit);
  return [...names, ...firms, ...cities].slice(0, limit);
}

// ===================== EMAIL CLASSIFICATION =====================

const PERSONAL_DOMAINS = new Set([
  'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com','icloud.com',
  'mail.com','protonmail.com','zoho.com','yandex.com','gmx.com','live.com',
  'msn.com','comcast.net','att.net','verizon.net','cox.net','sbcglobal.net',
  'me.com','mac.com','fastmail.com','tutanota.com','hey.com',
]);

const ROLE_PREFIXES = new Set([
  'info','contact','admin','support','office','help','billing','sales',
  'legal','marketing','hr','reception','general','team','hello','enquiries',
  'enquiry','inquiry','inquiries','noreply','no-reply','webmaster','postmaster',
]);

function classifyEmail(email) {
  if (!email) return '';
  const lower = email.toLowerCase().trim();
  const atIdx = lower.indexOf('@');
  if (atIdx < 0) return '';
  const local = lower.slice(0, atIdx);
  const domain = lower.slice(atIdx + 1);

  if (PERSONAL_DOMAINS.has(domain)) return 'personal';
  if (ROLE_PREFIXES.has(local.split('.')[0]) || ROLE_PREFIXES.has(local)) return 'role_based';
  // Check if it looks like a generic catch-all
  if (['info', 'contact', 'office', 'admin', 'general'].includes(local)) return 'generic';
  return 'professional';
}

/**
 * Classify all emails in the database and update email_type column.
 */
function classifyAllEmails() {
  const db = getDb();
  const leads = db.prepare("SELECT id, email FROM leads WHERE email IS NOT NULL AND email != '' AND (email_type IS NULL OR email_type = '')").all();
  let classified = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      const type = classifyEmail(lead.email);
      if (type) {
        db.prepare("UPDATE leads SET email_type = ? WHERE id = ?").run(type, lead.id);
        classified++;
      }
    }
  });
  txn();
  return { classified, total: leads.length };
}

/**
 * Get email classification breakdown.
 */
function getEmailClassification() {
  const db = getDb();
  const rows = db.prepare(`
    SELECT email_type, COUNT(*) as count FROM leads
    WHERE email IS NOT NULL AND email != ''
    GROUP BY email_type ORDER BY count DESC
  `).all();
  const total = rows.reduce((s, r) => s + r.count, 0);
  return { breakdown: rows, total };
}

// ===================== CONFIDENCE SCORING =====================

/**
 * Compute confidence score for a lead (0-100).
 * Higher when: multiple sources agree, data is recent, more fields filled.
 */
function computeConfidenceScore(leadId) {
  const db = getDb();
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId);
  if (!lead) return 0;

  let score = 0;

  // Source count bonus (max 30 pts)
  const sources = db.prepare("SELECT COUNT(*) as cnt FROM lead_sources WHERE lead_id = ?").get(leadId);
  const srcCount = sources?.cnt || 1;
  score += Math.min(srcCount * 10, 30);

  // Field completeness (max 35 pts)
  const fields = ['email', 'phone', 'website', 'firm_name', 'title', 'linkedin_url', 'practice_area'];
  const filled = fields.filter(f => lead[f] && lead[f].toString().trim()).length;
  score += Math.round((filled / fields.length) * 35);

  // Email quality (max 15 pts)
  if (lead.email) {
    const emailType = lead.email_type || classifyEmail(lead.email);
    if (emailType === 'professional') score += 15;
    else if (emailType === 'personal') score += 8;
    else if (emailType === 'role_based') score += 3;
  }

  // Verification bonus (max 10 pts)
  if (lead.email_verified === 1) score += 10;
  else if (lead.email_catch_all === 1) score += 3;

  // Freshness (max 10 pts)
  if (lead.updated_at) {
    const daysSince = (Date.now() - new Date(lead.updated_at).getTime()) / 86400000;
    if (daysSince < 7) score += 10;
    else if (daysSince < 30) score += 7;
    else if (daysSince < 90) score += 4;
    else if (daysSince < 180) score += 2;
  }

  return Math.min(score, 100);
}

/**
 * Batch compute confidence scores for all leads.
 */
function batchComputeConfidence() {
  const db = getDb();
  const leads = db.prepare("SELECT id FROM leads").all();
  let updated = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      const score = computeConfidenceScore(lead.id);
      db.prepare("UPDATE leads SET confidence_score = ?, source_count = (SELECT COUNT(*) FROM lead_sources WHERE lead_id = ?) WHERE id = ?").run(score, lead.id, lead.id);
      updated++;
    }
  });
  txn();
  return { updated };
}

/**
 * Get confidence score distribution.
 */
function getConfidenceDistribution() {
  const db = getDb();
  const high = db.prepare("SELECT COUNT(*) as count FROM leads WHERE confidence_score >= 80").get().count;
  const medium = db.prepare("SELECT COUNT(*) as count FROM leads WHERE confidence_score >= 50 AND confidence_score < 80").get().count;
  const low = db.prepare("SELECT COUNT(*) as count FROM leads WHERE confidence_score >= 20 AND confidence_score < 50").get().count;
  const veryLow = db.prepare("SELECT COUNT(*) as count FROM leads WHERE confidence_score < 20").get().count;
  return { high, medium, low, veryLow, total: high + medium + low + veryLow };
}

// ===================== CHANGE DETECTION =====================

/**
 * Detect changes between current leads and previous scrape data.
 * Records changes in lead_snapshots table.
 */
function detectChanges(newLeadData, scrapeRunId) {
  const db = getDb();
  const trackFields = ['firm_name', 'phone', 'email', 'website', 'bar_status', 'title', 'city'];
  let changes = 0;

  const existing = findExistingLead(newLeadData);
  if (!existing) return { changes: 0, isNew: true };

  for (const field of trackFields) {
    const oldVal = (existing[field] || '').toString().trim();
    const newVal = (newLeadData[field] || '').toString().trim();
    if (oldVal && newVal && oldVal !== newVal) {
      db.prepare(`
        INSERT INTO lead_snapshots (lead_id, field_name, old_value, new_value, scrape_run_id)
        VALUES (?, ?, ?, ?, ?)
      `).run(existing.id, field, oldVal, newVal, scrapeRunId || null);
      changes++;

      // Track firm changes specifically
      if (field === 'firm_name') {
        db.prepare("UPDATE leads SET previous_firm = ?, firm_changed_at = CURRENT_TIMESTAMP WHERE id = ?")
          .run(oldVal, existing.id);
      }
    }
  }

  return { changes, isNew: false, leadId: existing.id };
}

/**
 * Get recent changes across all leads.
 */
function getRecentChanges2(limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT s.*, l.first_name, l.last_name, l.state
    FROM lead_snapshots s
    JOIN leads l ON l.id = s.lead_id
    ORDER BY s.detected_at DESC
    LIMIT ?
  `).all(limit);
}

/**
 * Get leads who recently changed firms (buying signal!).
 */
function getFirmChanges(limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT id, first_name, last_name, firm_name, previous_firm, firm_changed_at, city, state, email, phone
    FROM leads
    WHERE previous_firm IS NOT NULL AND previous_firm != ''
    ORDER BY firm_changed_at DESC
    LIMIT ?
  `).all(limit);
}

/**
 * Get change history for a specific lead.
 */
function getLeadChangeHistory(leadId) {
  const db = getDb();
  return db.prepare(`
    SELECT * FROM lead_snapshots WHERE lead_id = ? ORDER BY detected_at DESC
  `).all(leadId);
}

// ===================== TAG DEFINITIONS (COLOR-CODED) =====================

function getTagDefinitions() {
  const db = getDb();
  // Get definitions and attach lead counts
  const defs = db.prepare("SELECT * FROM tag_definitions ORDER BY name").all();
  for (const def of defs) {
    const count = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE ',' || tags || ',' LIKE ?").get(`%,${def.name},%`);
    def.lead_count = count?.cnt || 0;
  }
  return defs;
}

function createTagDefinition(name, color = '#6366f1', description = '', autoRule = '') {
  const db = getDb();
  return db.prepare("INSERT INTO tag_definitions (name, color, description, auto_rule) VALUES (?, ?, ?, ?)").run(name, color, description, autoRule);
}

function updateTagDefinition(id, updates) {
  const db = getDb();
  const fields = [];
  const vals = [];
  if (updates.name !== undefined) { fields.push('name = ?'); vals.push(updates.name); }
  if (updates.color !== undefined) { fields.push('color = ?'); vals.push(updates.color); }
  if (updates.description !== undefined) { fields.push('description = ?'); vals.push(updates.description); }
  if (updates.autoRule !== undefined) { fields.push('auto_rule = ?'); vals.push(updates.autoRule); }
  if (fields.length === 0) return { changes: 0 };
  vals.push(id);
  return db.prepare(`UPDATE tag_definitions SET ${fields.join(', ')} WHERE id = ?`).run(...vals);
}

function deleteTagDefinition(id) {
  const db = getDb();
  return db.prepare("DELETE FROM tag_definitions WHERE id = ?").run(id);
}

/**
 * Auto-tag leads based on tag definition rules.
 * Rules are JSON: { field, operator, value }
 */
function runAutoTagging() {
  const db = getDb();
  const defs = db.prepare("SELECT * FROM tag_definitions WHERE auto_rule IS NOT NULL AND auto_rule != ''").all();
  let tagged = 0;

  for (const def of defs) {
    let rule;
    try { rule = JSON.parse(def.auto_rule); } catch { continue; }
    if (!rule.field || !rule.value) continue;

    let leads;
    const field = rule.field.replace(/[^a-z_]/g, '');
    if (rule.operator === 'contains') {
      leads = db.prepare(`SELECT id, tags FROM leads WHERE ${field} LIKE ?`).all(`%${rule.value}%`);
    } else if (rule.operator === 'equals') {
      leads = db.prepare(`SELECT id, tags FROM leads WHERE ${field} = ?`).all(rule.value);
    } else if (rule.operator === 'is_empty') {
      leads = db.prepare(`SELECT id, tags FROM leads WHERE (${field} IS NULL OR ${field} = '')`).all();
    } else if (rule.operator === 'is_not_empty') {
      leads = db.prepare(`SELECT id, tags FROM leads WHERE ${field} IS NOT NULL AND ${field} != ''`).all();
    } else {
      continue;
    }

    for (const lead of leads) {
      const currentTags = (lead.tags || '').split(',').filter(Boolean);
      if (!currentTags.includes(def.name)) {
        currentTags.push(def.name);
        db.prepare("UPDATE leads SET tags = ? WHERE id = ?").run(currentTags.join(','), lead.id);
        tagged++;
      }
    }
  }
  return { tagged, rules: defs.length };
}

// ===================== LEAD COMPARISON =====================

/**
 * Compare two leads side-by-side for merging.
 */
function compareLeads(leadId1, leadId2) {
  const db = getDb();
  const lead1 = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId1);
  const lead2 = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId2);
  if (!lead1 || !lead2) return null;

  const compareFields = [
    'first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state',
    'website', 'bar_number', 'bar_status', 'title', 'linkedin_url', 'practice_area',
    'bio', 'education', 'admission_date', 'country',
  ];

  const comparison = [];
  let matchCount = 0;
  let totalComparable = 0;

  for (const field of compareFields) {
    const val1 = (lead1[field] || '').toString().trim();
    const val2 = (lead2[field] || '').toString().trim();
    const bothEmpty = !val1 && !val2;
    const match = val1 && val2 && val1.toLowerCase() === val2.toLowerCase();

    if (!bothEmpty) totalComparable++;
    if (match) matchCount++;

    comparison.push({
      field,
      value1: val1 || null,
      value2: val2 || null,
      match,
      onlyIn1: !!val1 && !val2,
      onlyIn2: !val1 && !!val2,
      conflict: !!val1 && !!val2 && !match,
    });
  }

  // Sources
  const sources1 = db.prepare("SELECT source FROM lead_sources WHERE lead_id = ?").all(leadId1).map(r => r.source);
  const sources2 = db.prepare("SELECT source FROM lead_sources WHERE lead_id = ?").all(leadId2).map(r => r.source);

  return {
    lead1: { id: lead1.id, name: `${lead1.first_name} ${lead1.last_name}`, score: lead1.lead_score, sources: sources1 },
    lead2: { id: lead2.id, name: `${lead2.first_name} ${lead2.last_name}`, score: lead2.lead_score, sources: sources2 },
    comparison,
    matchPercentage: totalComparable > 0 ? Math.round((matchCount / totalComparable) * 100) : 0,
    totalFields: compareFields.length,
    conflicts: comparison.filter(c => c.conflict).length,
    complementary: comparison.filter(c => c.onlyIn1 || c.onlyIn2).length,
  };
}

/**
 * Merge two leads with user-specified field selections.
 * fieldChoices: { fieldName: 1 or 2 } (which lead's value to keep)
 */
function mergeLeadsWithChoices(keepId, mergeId, fieldChoices = {}) {
  const db = getDb();
  const keep = db.prepare("SELECT * FROM leads WHERE id = ?").get(keepId);
  const merge = db.prepare("SELECT * FROM leads WHERE id = ?").get(mergeId);
  if (!keep || !merge) return { error: 'Lead not found' };

  const mergeableFields = [
    'first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state',
    'website', 'bar_number', 'bar_status', 'title', 'linkedin_url', 'practice_area',
    'bio', 'education', 'admission_date', 'country',
  ];

  const updates = {};
  for (const field of mergeableFields) {
    if (fieldChoices[field] === 2) {
      // Use merge lead's value
      updates[field] = merge[field];
    } else if (fieldChoices[field] === 1) {
      // Keep original (no change needed unless empty)
      updates[field] = keep[field];
    } else {
      // Auto: keep original if not empty, else use merge
      updates[field] = keep[field] || merge[field];
    }
  }

  // Merge tags
  const tags1 = (keep.tags || '').split(',').filter(Boolean);
  const tags2 = (merge.tags || '').split(',').filter(Boolean);
  const mergedTags = [...new Set([...tags1, ...tags2])].join(',');

  // Apply updates
  const setClauses = mergeableFields.map(f => `${f} = ?`).join(', ');
  const values = mergeableFields.map(f => updates[f] || null);
  db.prepare(`UPDATE leads SET ${setClauses}, tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`).run(...values, mergedTags, keepId);

  // Transfer sources
  const mergeSources = db.prepare("SELECT source FROM lead_sources WHERE lead_id = ?").all(mergeId);
  for (const s of mergeSources) {
    try {
      db.prepare("INSERT OR IGNORE INTO lead_sources (lead_id, source) VALUES (?, ?)").run(keepId, s.source);
    } catch {}
  }

  // Transfer notes
  try {
    db.prepare("UPDATE lead_notes SET lead_id = ? WHERE lead_id = ?").run(keepId, mergeId);
  } catch {}

  // Log the merge
  try {
    logChange(keepId, 'merge', `Merged with lead #${mergeId} (${merge.first_name} ${merge.last_name})`);
  } catch {}

  // Delete the merged lead
  db.prepare("DELETE FROM lead_sources WHERE lead_id = ?").run(mergeId);
  db.prepare("DELETE FROM leads WHERE id = ?").run(mergeId);

  return { success: true, keptId: keepId, mergedId: mergeId, fieldsUpdated: Object.keys(fieldChoices).length };
}

/**
 * Get data staleness report.
 */
function getStalenessReport() {
  const db = getDb();
  const now = Date.now();
  const fresh = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE updated_at >= datetime('now', '-7 days')").get().cnt;
  const recent = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE updated_at >= datetime('now', '-30 days') AND updated_at < datetime('now', '-7 days')").get().cnt;
  const aging = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE updated_at >= datetime('now', '-90 days') AND updated_at < datetime('now', '-30 days')").get().cnt;
  const stale = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE updated_at < datetime('now', '-90 days') OR updated_at IS NULL").get().cnt;
  const total = fresh + recent + aging + stale;

  // Stale by state
  const staleByState = db.prepare(`
    SELECT state, COUNT(*) as cnt FROM leads
    WHERE updated_at < datetime('now', '-90 days') OR updated_at IS NULL
    GROUP BY state ORDER BY cnt DESC LIMIT 10
  `).all();

  return { fresh, recent, aging, stale, total, staleByState };
}

// ===================== IMPORT PREVIEW =====================

/**
 * Parse CSV headers and return preview + auto-mapped columns.
 * Does NOT import — just previews.
 */
function previewImportMapping(headers, sampleRows) {
  const fieldMaps = {
    first_name: ['first_name', 'firstname', 'first', 'given_name', 'fname'],
    last_name: ['last_name', 'lastname', 'last', 'surname', 'family_name', 'lname'],
    email: ['email', 'email_address', 'e_mail', 'emailaddress'],
    phone: ['phone', 'phone_number', 'telephone', 'tel', 'mobile', 'cell', 'work_phone'],
    firm_name: ['firm_name', 'firm', 'company', 'company_name', 'organization', 'org', 'employer'],
    city: ['city', 'town', 'locality'],
    state: ['state', 'state_code', 'province', 'region'],
    country: ['country', 'country_code'],
    website: ['website', 'url', 'web', 'site', 'homepage', 'website_url'],
    bar_number: ['bar_number', 'bar_num', 'license_number', 'license', 'bar_id'],
    bar_status: ['bar_status', 'status', 'license_status'],
    practice_area: ['practice_area', 'practice', 'specialty', 'specialization', 'area_of_practice'],
    title: ['title', 'job_title', 'position'],
    linkedin_url: ['linkedin_url', 'linkedin', 'linkedin_profile'],
  };

  const lowerHeaders = headers.map(h => h.toLowerCase().replace(/[^a-z0-9]/g, '_'));
  const mapping = {};

  for (const [field, aliases] of Object.entries(fieldMaps)) {
    for (let i = 0; i < lowerHeaders.length; i++) {
      if (aliases.includes(lowerHeaders[i])) {
        mapping[field] = headers[i];
        break;
      }
    }
  }

  // Handle full_name
  if (!mapping.first_name && !mapping.last_name) {
    const nameIdx = lowerHeaders.findIndex(h => h === 'name' || h === 'full_name' || h === 'fullname');
    if (nameIdx >= 0) mapping._full_name = headers[nameIdx];
  }

  const unmapped = headers.filter(h => !Object.values(mapping).includes(h));
  const mappedCount = Object.keys(mapping).filter(k => !k.startsWith('_')).length;

  return {
    headers,
    mapping,
    unmappedHeaders: unmapped,
    mappedCount,
    totalHeaders: headers.length,
    sampleRows: sampleRows.slice(0, 5),
    availableFields: Object.keys(fieldMaps),
  };
}

// ===================== EMAIL VALIDATION (IN-HOUSE) =====================

const DISPOSABLE_DOMAINS = new Set([
  'mailinator.com','guerrillamail.com','tempmail.com','throwaway.email','yopmail.com',
  'sharklasers.com','guerrillamailblock.com','grr.la','guerrillamail.info','guerrillamail.biz',
  'guerrillamail.de','guerrillamail.net','tempail.com','dispostable.com','trashmail.com',
  'mailnesia.com','tempr.email','discard.email','discardmail.com','fakeinbox.com',
  'mailcatch.com','mintemail.com','tempinbox.com','trash-mail.com','mytemp.email',
]);

function validateEmailSyntax(email) {
  if (!email) return { valid: false, reason: 'empty' };
  const trimmed = email.trim().toLowerCase();
  // Basic RFC 5322 simplified check
  const re = /^[a-z0-9!#$%&'*+/=?^_`{|}~.-]+@[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/;
  if (!re.test(trimmed)) return { valid: false, reason: 'invalid_syntax' };
  if (trimmed.length > 254) return { valid: false, reason: 'too_long' };
  const [local, domain] = trimmed.split('@');
  if (local.length > 64) return { valid: false, reason: 'local_too_long' };
  if (DISPOSABLE_DOMAINS.has(domain)) return { valid: false, reason: 'disposable' };
  return { valid: true, domain, local };
}

/**
 * Validate email via DNS MX record lookup.
 * Returns: { valid, hasMx, mxRecords, reason }
 */
async function validateEmailMX(email) {
  const syntax = validateEmailSyntax(email);
  if (!syntax.valid) return { ...syntax, hasMx: false };

  const dns = require('dns').promises;
  try {
    const mxRecords = await dns.resolveMx(syntax.domain);
    if (mxRecords && mxRecords.length > 0) {
      return { valid: true, hasMx: true, mxRecords: mxRecords.map(r => r.exchange), domain: syntax.domain };
    }
    // Fallback: check for A record
    try {
      await dns.resolve4(syntax.domain);
      return { valid: true, hasMx: false, reason: 'no_mx_but_has_a_record', domain: syntax.domain };
    } catch {
      return { valid: false, hasMx: false, reason: 'domain_not_found', domain: syntax.domain };
    }
  } catch (err) {
    if (err.code === 'ENOTFOUND' || err.code === 'ENODATA') {
      return { valid: false, hasMx: false, reason: 'domain_not_found', domain: syntax.domain };
    }
    return { valid: false, hasMx: false, reason: 'dns_error', domain: syntax.domain };
  }
}

/**
 * Batch validate emails for all leads (syntax + MX).
 * Updates email_verified and email_type columns.
 */
async function batchValidateEmails(onProgress) {
  const db = getDb();
  const leads = db.prepare("SELECT id, email FROM leads WHERE email IS NOT NULL AND email != ''").all();
  const total = leads.length;
  let validated = 0, valid = 0, invalid = 0, disposable = 0;

  // Group by domain to avoid repeated MX lookups
  const domainCache = new Map();

  for (const lead of leads) {
    const syntax = validateEmailSyntax(lead.email);
    if (!syntax.valid) {
      db.prepare("UPDATE leads SET email_verified = -1 WHERE id = ?").run(lead.id);
      invalid++;
      validated++;
      if (syntax.reason === 'disposable') disposable++;
      if (onProgress && validated % 50 === 0) onProgress({ validated, total, valid, invalid });
      continue;
    }

    // Check MX (cached per domain)
    if (!domainCache.has(syntax.domain)) {
      try {
        const dns = require('dns').promises;
        const mx = await dns.resolveMx(syntax.domain);
        domainCache.set(syntax.domain, mx && mx.length > 0);
      } catch {
        domainCache.set(syntax.domain, false);
      }
    }

    const hasMx = domainCache.get(syntax.domain);
    if (hasMx) {
      db.prepare("UPDATE leads SET email_verified = 1 WHERE id = ?").run(lead.id);
      valid++;
    } else {
      db.prepare("UPDATE leads SET email_verified = -1 WHERE id = ?").run(lead.id);
      invalid++;
    }
    validated++;
    if (onProgress && validated % 50 === 0) onProgress({ validated, total, valid, invalid });
  }

  return { total, validated, valid, invalid, disposable, domainsChecked: domainCache.size };
}

// ===================== ICP SCORING =====================

function getIcpCriteria() {
  const db = getDb();
  try {
    db.prepare("SELECT 1 FROM icp_criteria LIMIT 1").get();
  } catch {
    db.exec(`
      CREATE TABLE IF NOT EXISTS icp_criteria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        field TEXT NOT NULL,
        operator TEXT NOT NULL,
        value TEXT NOT NULL,
        weight INTEGER DEFAULT 10,
        label TEXT DEFAULT '',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }
  return db.prepare("SELECT * FROM icp_criteria ORDER BY weight DESC").all();
}

function addIcpCriterion(field, operator, value, weight = 10, label = '') {
  const db = getDb();
  // Ensure table exists
  getIcpCriteria();
  return db.prepare("INSERT INTO icp_criteria (field, operator, value, weight, label) VALUES (?, ?, ?, ?, ?)").run(field, operator, value, weight, label);
}

function deleteIcpCriterion(id) {
  const db = getDb();
  return db.prepare("DELETE FROM icp_criteria WHERE id = ?").run(id);
}

function updateIcpCriterion(id, updates) {
  const db = getDb();
  const fields = [];
  const vals = [];
  for (const [k, v] of Object.entries(updates)) {
    if (['field', 'operator', 'value', 'weight', 'label'].includes(k)) {
      fields.push(`${k} = ?`);
      vals.push(v);
    }
  }
  if (fields.length === 0) return { changes: 0 };
  vals.push(id);
  return db.prepare(`UPDATE icp_criteria SET ${fields.join(', ')} WHERE id = ?`).run(...vals);
}

/**
 * Compute ICP score for a lead (0-100).
 */
function computeIcpScore(leadId) {
  const db = getDb();
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId);
  if (!lead) return 0;

  const criteria = getIcpCriteria();
  if (criteria.length === 0) return 50; // no ICP defined

  const totalWeight = criteria.reduce((s, c) => s + c.weight, 0);
  if (totalWeight === 0) return 50;

  let earned = 0;
  for (const c of criteria) {
    const val = (lead[c.field] || '').toString().toLowerCase().trim();
    const target = (c.value || '').toLowerCase().trim();
    let match = false;

    switch (c.operator) {
      case 'equals': match = val === target; break;
      case 'contains': match = val.includes(target); break;
      case 'starts_with': match = val.startsWith(target); break;
      case 'is_not_empty': match = val.length > 0; break;
      case 'is_empty': match = val.length === 0; break;
      case 'in_list': match = target.split(',').map(s => s.trim()).includes(val); break;
    }

    if (match) earned += c.weight;
  }

  return Math.round((earned / totalWeight) * 100);
}

/**
 * Batch compute ICP scores for all leads.
 */
function batchComputeIcpScores() {
  const db = getDb();
  // Ensure icp_score column exists
  try { db.prepare("SELECT icp_score FROM leads LIMIT 1").get(); }
  catch { db.exec("ALTER TABLE leads ADD COLUMN icp_score INTEGER DEFAULT 0"); }

  const leads = db.prepare("SELECT id FROM leads").all();
  let updated = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      const score = computeIcpScore(lead.id);
      db.prepare("UPDATE leads SET icp_score = ? WHERE id = ?").run(score, lead.id);
      updated++;
    }
  });
  txn();
  return { updated };
}

/**
 * Get ICP score distribution.
 */
function getIcpDistribution() {
  const db = getDb();
  try { db.prepare("SELECT icp_score FROM leads LIMIT 1").get(); }
  catch { return { perfect: 0, great: 0, good: 0, fair: 0, poor: 0, total: 0 }; }

  const perfect = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE icp_score >= 90").get().cnt;
  const great = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE icp_score >= 70 AND icp_score < 90").get().cnt;
  const good = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE icp_score >= 50 AND icp_score < 70").get().cnt;
  const fair = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE icp_score >= 25 AND icp_score < 50").get().cnt;
  const poor = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE icp_score < 25").get().cnt;
  return { perfect, great, good, fair, poor, total: perfect + great + good + fair + poor };
}

// ===================== SAVED SEARCHES & ALERTS =====================

function getSavedSearches() {
  const db = getDb();
  try { db.prepare("SELECT 1 FROM saved_searches LIMIT 1").get(); }
  catch {
    db.exec(`
      CREATE TABLE IF NOT EXISTS saved_searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        filters TEXT NOT NULL, -- JSON
        alert_enabled INTEGER DEFAULT 0,
        last_count INTEGER DEFAULT 0,
        last_checked_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }
  return db.prepare("SELECT * FROM saved_searches ORDER BY created_at DESC").all();
}

function createSavedSearch(name, filters, alertEnabled = false) {
  const db = getDb();
  getSavedSearches(); // ensure table
  const filtersJson = typeof filters === 'string' ? filters : JSON.stringify(filters);
  return db.prepare("INSERT INTO saved_searches (name, filters, alert_enabled) VALUES (?, ?, ?)").run(name, filtersJson, alertEnabled ? 1 : 0);
}

function deleteSavedSearch(id) {
  const db = getDb();
  return db.prepare("DELETE FROM saved_searches WHERE id = ?").run(id);
}

function updateSavedSearchCount(id, count) {
  const db = getDb();
  return db.prepare("UPDATE saved_searches SET last_count = ?, last_checked_at = CURRENT_TIMESTAMP WHERE id = ?").run(count, id);
}

/**
 * Check saved searches for new matches (alerts).
 * Returns array of { search, newCount, previousCount }.
 */
function checkSavedSearchAlerts() {
  const db = getDb();
  const searches = getSavedSearches().filter(s => s.alert_enabled);
  const alerts = [];

  for (const search of searches) {
    let filters;
    try { filters = JSON.parse(search.filters); } catch { continue; }

    // Build WHERE clause from filters
    const where = [];
    const params = [];
    if (filters.state) { where.push("state = ?"); params.push(filters.state); }
    if (filters.country) { where.push("country = ?"); params.push(filters.country); }
    if (filters.city) { where.push("city LIKE ?"); params.push(`%${filters.city}%`); }
    if (filters.practice_area) { where.push("practice_area LIKE ?"); params.push(`%${filters.practice_area}%`); }
    if (filters.hasEmail) { where.push("email IS NOT NULL AND email != ''"); }
    if (filters.hasPhone) { where.push("phone IS NOT NULL AND phone != ''"); }
    if (filters.minScore) { where.push("lead_score >= ?"); params.push(filters.minScore); }
    if (filters.search) { where.push("(first_name || ' ' || last_name || ' ' || COALESCE(firm_name,'')) LIKE ?"); params.push(`%${filters.search}%`); }

    const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
    const currentCount = db.prepare(`SELECT COUNT(*) as cnt FROM leads ${whereClause}`).get(...params).cnt;

    if (currentCount > search.last_count) {
      alerts.push({
        search: { id: search.id, name: search.name },
        newCount: currentCount - search.last_count,
        previousCount: search.last_count,
        currentCount,
      });
    }

    updateSavedSearchCount(search.id, currentCount);
  }

  return alerts;
}

// ===================== RECENTLY ADMITTED ATTORNEYS (BUYING SIGNAL) =====================

function getRecentAdmissions(months = 6, limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, admission_date, practice_area
    FROM leads
    WHERE admission_date IS NOT NULL AND admission_date != ''
      AND admission_date >= date('now', '-' || ? || ' months')
    ORDER BY admission_date DESC
    LIMIT ?
  `).all(months, limit);
}

function getAdmissionSignals() {
  const db = getDb();
  const recent = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE admission_date >= date('now', '-6 months') AND admission_date IS NOT NULL AND admission_date != ''").get().cnt;
  const thisYear = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE admission_date >= date('now', '-12 months') AND admission_date IS NOT NULL AND admission_date != ''").get().cnt;
  const byState = db.prepare(`
    SELECT state, COUNT(*) as cnt FROM leads
    WHERE admission_date >= date('now', '-6 months') AND admission_date IS NOT NULL AND admission_date != ''
    GROUP BY state ORDER BY cnt DESC LIMIT 10
  `).all();
  return { recent6m: recent, recent12m: thisYear, byState };
}

// ===================== COLUMN VISIBILITY / TABLE CONFIG =====================

function getTableConfig() {
  const db = getDb();
  try { db.prepare("SELECT 1 FROM table_config LIMIT 1").get(); }
  catch {
    db.exec(`
      CREATE TABLE IF NOT EXISTS table_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        config TEXT NOT NULL -- JSON
      )
    `);
  }
  const row = db.prepare("SELECT config FROM table_config WHERE name = 'default'").get();
  if (row) return JSON.parse(row.config);
  return null;
}

function saveTableConfig(config) {
  const db = getDb();
  getTableConfig(); // ensure table
  const json = typeof config === 'string' ? config : JSON.stringify(config);
  db.prepare("INSERT OR REPLACE INTO table_config (name, config) VALUES ('default', ?)").run(json);
  return { saved: true };
}

// ===================== OUTREACH SEQUENCES =====================

function ensureSequenceTables() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS sequences (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      status TEXT DEFAULT 'draft', -- draft, active, paused, archived
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS sequence_steps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sequence_id INTEGER NOT NULL,
      step_number INTEGER NOT NULL,
      channel TEXT DEFAULT 'email', -- email, linkedin, call
      subject TEXT DEFAULT '',
      body TEXT DEFAULT '',
      delay_days INTEGER DEFAULT 0,
      variant TEXT DEFAULT 'A', -- A/B testing
      FOREIGN KEY (sequence_id) REFERENCES sequences(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS sequence_enrollments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sequence_id INTEGER NOT NULL,
      lead_id INTEGER NOT NULL,
      current_step INTEGER DEFAULT 1,
      status TEXT DEFAULT 'active', -- active, paused, completed, replied
      enrolled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      last_step_at DATETIME,
      FOREIGN KEY (sequence_id) REFERENCES sequences(id),
      FOREIGN KEY (lead_id) REFERENCES leads(id),
      UNIQUE(sequence_id, lead_id)
    );
  `);
}

function getSequences() {
  const db = getDb();
  ensureSequenceTables();
  const seqs = db.prepare("SELECT * FROM sequences ORDER BY updated_at DESC").all();
  for (const s of seqs) {
    s.steps = db.prepare("SELECT * FROM sequence_steps WHERE sequence_id = ? ORDER BY step_number, variant").all(s.id);
    s.enrolled = db.prepare("SELECT COUNT(*) as cnt FROM sequence_enrollments WHERE sequence_id = ?").get(s.id).cnt;
    s.active = db.prepare("SELECT COUNT(*) as cnt FROM sequence_enrollments WHERE sequence_id = ? AND status = 'active'").get(s.id).cnt;
  }
  return seqs;
}

function createSequence(name, description = '') {
  const db = getDb();
  ensureSequenceTables();
  return db.prepare("INSERT INTO sequences (name, description) VALUES (?, ?)").run(name, description);
}

function addSequenceStep(sequenceId, stepNumber, channel, subject, body, delayDays = 0, variant = 'A') {
  const db = getDb();
  ensureSequenceTables();
  return db.prepare("INSERT INTO sequence_steps (sequence_id, step_number, channel, subject, body, delay_days, variant) VALUES (?, ?, ?, ?, ?, ?, ?)").run(sequenceId, stepNumber, channel, subject, body, delayDays, variant);
}

function deleteSequence(id) {
  const db = getDb();
  db.prepare("DELETE FROM sequence_steps WHERE sequence_id = ?").run(id);
  db.prepare("DELETE FROM sequence_enrollments WHERE sequence_id = ?").run(id);
  return db.prepare("DELETE FROM sequences WHERE id = ?").run(id);
}

function enrollInSequence(sequenceId, leadIds) {
  const db = getDb();
  ensureSequenceTables();
  let enrolled = 0;
  const txn = db.transaction(() => {
    for (const leadId of leadIds) {
      try {
        db.prepare("INSERT OR IGNORE INTO sequence_enrollments (sequence_id, lead_id) VALUES (?, ?)").run(sequenceId, leadId);
        enrolled++;
      } catch {}
    }
  });
  txn();
  return { enrolled };
}

function getSequenceEnrollments(sequenceId) {
  const db = getDb();
  ensureSequenceTables();
  return db.prepare(`
    SELECT se.*, l.first_name, l.last_name, l.email, l.firm_name, l.city, l.state
    FROM sequence_enrollments se
    JOIN leads l ON l.id = se.lead_id
    WHERE se.sequence_id = ?
    ORDER BY se.enrolled_at DESC
  `).all(sequenceId);
}

/**
 * Generate personalized email from sequence step template.
 */
function renderSequenceStep(stepId, leadId) {
  const db = getDb();
  ensureSequenceTables();
  const step = db.prepare("SELECT * FROM sequence_steps WHERE id = ?").get(stepId);
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId);
  if (!step || !lead) return null;

  const vars = {
    '{{first_name}}': lead.first_name || '',
    '{{last_name}}': lead.last_name || '',
    '{{firm_name}}': lead.firm_name || 'your firm',
    '{{city}}': lead.city || '',
    '{{state}}': lead.state || '',
    '{{practice_area}}': lead.practice_area || 'your practice',
    '{{title}}': lead.title || 'Attorney',
    '{{email}}': lead.email || '',
  };

  let subject = step.subject || '';
  let body = step.body || '';
  for (const [k, v] of Object.entries(vars)) {
    subject = subject.replace(new RegExp(k.replace(/[{}]/g, '\\$&'), 'g'), v);
    body = body.replace(new RegExp(k.replace(/[{}]/g, '\\$&'), 'g'), v);
  }

  return { subject, body, channel: step.channel, stepNumber: step.step_number };
}

// ===================== LEAD ACTIVITY TRACKING =====================

function ensureActivityTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS lead_activities (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      action TEXT NOT NULL, -- viewed, emailed, called, exported, tagged, enriched, stage_changed, note_added
      details TEXT DEFAULT '',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id)
    );
    CREATE INDEX IF NOT EXISTS idx_activities_lead ON lead_activities(lead_id);
    CREATE INDEX IF NOT EXISTS idx_activities_action ON lead_activities(action);
  `);
}

function trackActivity(leadId, action, details = '') {
  const db = getDb();
  ensureActivityTable();
  return db.prepare("INSERT INTO lead_activities (lead_id, action, details) VALUES (?, ?, ?)").run(leadId, action, details);
}

function getLeadActivities(leadId, limit = 50) {
  const db = getDb();
  ensureActivityTable();
  return db.prepare("SELECT * FROM lead_activities WHERE lead_id = ? ORDER BY created_at DESC LIMIT ?").all(leadId, limit);
}

function getEngagementScore(leadId) {
  const db = getDb();
  ensureActivityTable();
  const weights = { viewed: 1, emailed: 5, called: 10, exported: 3, tagged: 2, enriched: 2, stage_changed: 4, note_added: 3 };
  const activities = db.prepare("SELECT action, COUNT(*) as cnt FROM lead_activities WHERE lead_id = ? GROUP BY action").all(leadId);
  let score = 0;
  for (const a of activities) {
    score += (weights[a.action] || 1) * a.cnt;
  }
  return Math.min(score, 100);
}

function getMostEngagedLeads(limit = 20) {
  const db = getDb();
  ensureActivityTable();
  return db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.firm_name, l.city, l.state, l.email,
           COUNT(a.id) as activity_count,
           MAX(a.created_at) as last_activity
    FROM leads l
    JOIN lead_activities a ON a.lead_id = l.id
    GROUP BY l.id
    ORDER BY activity_count DESC
    LIMIT ?
  `).all(limit);
}

// ===================== FIRM ENRICHMENT =====================

function enrichFirmData() {
  const db = getDb();
  // Add firm-level columns if missing
  const addCol = (col, type) => {
    try { db.prepare(`SELECT ${col} FROM leads LIMIT 1`).get(); }
    catch { db.exec(`ALTER TABLE leads ADD COLUMN ${col} ${type}`); }
  };
  addCol('firm_size', "INTEGER DEFAULT 0");
  addCol('firm_practice_areas', "TEXT DEFAULT ''");
  addCol('firm_locations', "TEXT DEFAULT ''");

  // Count attorneys per firm
  const firms = db.prepare(`
    SELECT firm_name, COUNT(*) as size,
           GROUP_CONCAT(DISTINCT practice_area) as practices,
           GROUP_CONCAT(DISTINCT city || ', ' || state) as locations
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != ''
    GROUP BY LOWER(firm_name)
    HAVING COUNT(*) >= 2
  `).all();

  let updated = 0;
  const txn = db.transaction(() => {
    for (const firm of firms) {
      db.prepare(`
        UPDATE leads SET firm_size = ?, firm_practice_areas = ?, firm_locations = ?
        WHERE LOWER(firm_name) = LOWER(?)
      `).run(firm.size, firm.practices || '', firm.locations || '', firm.firm_name);
      updated++;
    }
  });
  txn();

  return { firmsEnriched: firms.length, leadsUpdated: updated };
}

function getFirmDirectory(limit = 50, minSize = 2) {
  const db = getDb();
  try { db.prepare("SELECT firm_size FROM leads LIMIT 1").get(); }
  catch { return []; }

  return db.prepare(`
    SELECT firm_name,
           MAX(firm_size) as size,
           MAX(firm_practice_areas) as practices,
           MAX(firm_locations) as locations,
           COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as emails,
           COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) as phones,
           MAX(website) as website
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_size >= ?
    GROUP BY LOWER(firm_name)
    ORDER BY size DESC
    LIMIT ?
  `).all(minSize, limit);
}

// ===================== LOOKALIKE FINDER =====================

function findLookalikes(leadId, limit = 20) {
  const db = getDb();
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId);
  if (!lead) return [];

  // Score candidates based on similarity
  const candidates = db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, practice_area, lead_score,
           CASE WHEN practice_area = ? THEN 20 ELSE 0 END +
           CASE WHEN city = ? THEN 15 ELSE 0 END +
           CASE WHEN state = ? THEN 10 ELSE 0 END +
           CASE WHEN firm_size > 0 AND firm_size BETWEEN ? AND ? THEN 10 ELSE 0 END +
           CASE WHEN email IS NOT NULL AND email != '' THEN 5 ELSE 0 END +
           CASE WHEN phone IS NOT NULL AND phone != '' THEN 5 ELSE 0 END
           as similarity_score
    FROM leads
    WHERE id != ? AND (
      practice_area = ? OR
      city = ? OR
      state = ?
    )
    ORDER BY similarity_score DESC, lead_score DESC
    LIMIT ?
  `).all(
    lead.practice_area || '', lead.city || '', lead.state || '',
    Math.max(1, (lead.firm_size || 1) - 5), (lead.firm_size || 1) + 5,
    leadId,
    lead.practice_area || '__none__', lead.city || '__none__', lead.state || '__none__',
    limit
  );

  return candidates;
}

function findBatchLookalikes(leadIds, limit = 50) {
  const db = getDb();
  if (leadIds.length === 0) return [];

  // Get common attributes from input leads
  const leads = db.prepare(`SELECT practice_area, city, state FROM leads WHERE id IN (${leadIds.map(() => '?').join(',')})`)
    .all(...leadIds);

  const practices = [...new Set(leads.map(l => l.practice_area).filter(Boolean))];
  const cities = [...new Set(leads.map(l => l.city).filter(Boolean))];
  const states = [...new Set(leads.map(l => l.state).filter(Boolean))];

  const placeholders = leadIds.map(() => '?').join(',');

  // Find leads matching these attributes but not in the input set
  return db.prepare(`
    SELECT id, first_name, last_name, firm_name, city, state, email, phone, practice_area, lead_score,
           (CASE WHEN practice_area IN (${practices.map(() => '?').join(',') || "''"}) THEN 25 ELSE 0 END +
            CASE WHEN city IN (${cities.map(() => '?').join(',') || "''"}) THEN 20 ELSE 0 END +
            CASE WHEN state IN (${states.map(() => '?').join(',') || "''"}) THEN 10 ELSE 0 END) as similarity_score
    FROM leads
    WHERE id NOT IN (${placeholders})
      AND (practice_area IN (${practices.map(() => '?').join(',') || "''"})
           OR city IN (${cities.map(() => '?').join(',') || "''"})
           OR state IN (${states.map(() => '?').join(',') || "''"})
      )
    ORDER BY similarity_score DESC, lead_score DESC
    LIMIT ?
  `).all(...practices, ...cities, ...states, ...leadIds, ...practices, ...cities, ...states, limit);
}

// ===================== LEAD SCORE DECAY =====================

function applyScoreDecay(decayPercent = 5, inactiveDays = 30) {
  const db = getDb();
  ensureActivityTable();

  // Leads that haven't been engaged with in the last N days lose points
  const result = db.prepare(`
    UPDATE leads SET lead_score = MAX(0, lead_score - ?)
    WHERE id NOT IN (
      SELECT DISTINCT lead_id FROM lead_activities
      WHERE created_at > datetime('now', '-' || ? || ' days')
    ) AND lead_score > 0
  `).run(decayPercent, inactiveDays);

  return { decayed: result.changes, decayPercent, inactiveDays };
}

function getDecayPreview(inactiveDays = 30) {
  const db = getDb();
  ensureActivityTable();

  const count = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE id NOT IN (
      SELECT DISTINCT lead_id FROM lead_activities
      WHERE created_at > datetime('now', '-' || ? || ' days')
    ) AND lead_score > 0
  `).get(inactiveDays).cnt;

  return { wouldDecay: count, inactiveDays };
}

// ===================== DO NOT CONTACT LIST =====================

function ensureDncTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS dnc_list (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL, -- email, domain, phone, name
      value TEXT NOT NULL,
      reason TEXT DEFAULT '',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(type, value)
    );
    CREATE INDEX IF NOT EXISTS idx_dnc_type ON dnc_list(type);
  `);
}

function addToDnc(type, value, reason = '') {
  const db = getDb();
  ensureDncTable();
  const normalized = value.trim().toLowerCase();
  try {
    return db.prepare("INSERT OR IGNORE INTO dnc_list (type, value, reason) VALUES (?, ?, ?)").run(type, normalized, reason);
  } catch { return { changes: 0 }; }
}

function removeFromDnc(id) {
  const db = getDb();
  ensureDncTable();
  return db.prepare("DELETE FROM dnc_list WHERE id = ?").run(id);
}

function getDncList(type = null) {
  const db = getDb();
  ensureDncTable();
  if (type) return db.prepare("SELECT * FROM dnc_list WHERE type = ? ORDER BY created_at DESC").all(type);
  return db.prepare("SELECT * FROM dnc_list ORDER BY type, created_at DESC").all();
}

function checkDnc(lead) {
  const db = getDb();
  ensureDncTable();
  const checks = [];
  if (lead.email) {
    const email = lead.email.toLowerCase();
    const domain = email.split('@')[1] || '';
    const emailMatch = db.prepare("SELECT 1 FROM dnc_list WHERE type = 'email' AND value = ?").get(email);
    const domainMatch = db.prepare("SELECT 1 FROM dnc_list WHERE type = 'domain' AND value = ?").get(domain);
    if (emailMatch) checks.push('email');
    if (domainMatch) checks.push('domain');
  }
  if (lead.phone) {
    const phone = lead.phone.replace(/\D/g, '');
    const phoneMatch = db.prepare("SELECT 1 FROM dnc_list WHERE type = 'phone' AND value = ?").get(phone);
    if (phoneMatch) checks.push('phone');
  }
  return { blocked: checks.length > 0, reasons: checks };
}

function batchCheckDnc() {
  const db = getDb();
  ensureDncTable();
  // Get all DNC entries
  const dncEmails = new Set(db.prepare("SELECT value FROM dnc_list WHERE type = 'email'").all().map(r => r.value));
  const dncDomains = new Set(db.prepare("SELECT value FROM dnc_list WHERE type = 'domain'").all().map(r => r.value));
  const dncPhones = new Set(db.prepare("SELECT value FROM dnc_list WHERE type = 'phone'").all().map(r => r.value));

  const leads = db.prepare("SELECT id, email, phone FROM leads").all();
  let blocked = 0;
  const blockedIds = [];

  for (const lead of leads) {
    let isBlocked = false;
    if (lead.email) {
      const email = lead.email.toLowerCase();
      const domain = email.split('@')[1] || '';
      if (dncEmails.has(email) || dncDomains.has(domain)) isBlocked = true;
    }
    if (lead.phone) {
      const phone = lead.phone.replace(/\D/g, '');
      if (dncPhones.has(phone)) isBlocked = true;
    }
    if (isBlocked) { blocked++; blockedIds.push(lead.id); }
  }

  return { total: leads.length, blocked, blockedIds };
}

// ===================== SMART DUPLICATE DETECTION =====================

function findSmartDuplicates(limit = 100) {
  const db = getDb();

  // Strategy 1: Same email (different IDs)
  const emailDups = db.prepare(`
    SELECT GROUP_CONCAT(id) as ids, email, COUNT(*) as cnt
    FROM leads WHERE email IS NOT NULL AND email != ''
    GROUP BY LOWER(email) HAVING COUNT(*) > 1
    LIMIT ?
  `).all(Math.floor(limit / 3));

  // Strategy 2: Same first+last name in same city
  const nameCityDups = db.prepare(`
    SELECT GROUP_CONCAT(id) as ids,
           first_name || ' ' || last_name || ' (' || city || ', ' || state || ')' as match_key,
           COUNT(*) as cnt
    FROM leads
    WHERE first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
    GROUP BY LOWER(first_name), LOWER(last_name), LOWER(city), LOWER(state)
    HAVING COUNT(*) > 1
    LIMIT ?
  `).all(Math.floor(limit / 3));

  // Strategy 3: Same phone number
  const phoneDups = db.prepare(`
    SELECT GROUP_CONCAT(id) as ids, phone, COUNT(*) as cnt
    FROM leads WHERE phone IS NOT NULL AND phone != ''
    GROUP BY REPLACE(REPLACE(REPLACE(REPLACE(phone, '-', ''), '(', ''), ')', ''), ' ', '')
    HAVING COUNT(*) > 1
    LIMIT ?
  `).all(Math.floor(limit / 3));

  const groups = [];
  const seen = new Set();

  for (const d of emailDups) {
    const key = 'email:' + d.email;
    if (!seen.has(key)) {
      seen.add(key);
      groups.push({ type: 'email', matchValue: d.email, ids: d.ids.split(',').map(Number), count: d.cnt });
    }
  }
  for (const d of nameCityDups) {
    const key = 'name:' + d.match_key;
    if (!seen.has(key)) {
      seen.add(key);
      groups.push({ type: 'name+city', matchValue: d.match_key, ids: d.ids.split(',').map(Number), count: d.cnt });
    }
  }
  for (const d of phoneDups) {
    const key = 'phone:' + d.phone;
    if (!seen.has(key)) {
      seen.add(key);
      groups.push({ type: 'phone', matchValue: d.phone, ids: d.ids.split(',').map(Number), count: d.cnt });
    }
  }

  return { groups: groups.sort((a, b) => b.count - a.count), total: groups.length };
}

function autoMergeDuplicates(dryRun = true) {
  const db = getDb();
  const { groups } = findSmartDuplicates(500);
  let merged = 0;
  const mergeLog = [];

  const txn = db.transaction(() => {
    for (const group of groups) {
      if (group.ids.length !== 2) continue; // Only auto-merge pairs

      const leads = group.ids.map(id => db.prepare("SELECT * FROM leads WHERE id = ?").get(id)).filter(Boolean);
      if (leads.length !== 2) continue;

      // Pick the "better" lead (more data)
      const score = (l) => [l.email, l.phone, l.website, l.firm_name, l.title, l.linkedin_url].filter(Boolean).length;
      const [keep, discard] = score(leads[0]) >= score(leads[1]) ? leads : [leads[1], leads[0]];

      if (dryRun) {
        mergeLog.push({ keepId: keep.id, discardId: discard.id, type: group.type, matchValue: group.matchValue });
      } else {
        // Merge missing fields from discard into keep
        const updates = [];
        const values = [];
        for (const field of ['email', 'phone', 'website', 'firm_name', 'title', 'linkedin_url', 'practice_area', 'bar_number']) {
          if ((!keep[field] || keep[field] === '') && discard[field] && discard[field] !== '') {
            updates.push(`${field} = ?`);
            values.push(discard[field]);
          }
        }
        if (updates.length > 0) {
          db.prepare(`UPDATE leads SET ${updates.join(', ')} WHERE id = ?`).run(...values, keep.id);
        }
        db.prepare("DELETE FROM leads WHERE id = ?").run(discard.id);
        mergeLog.push({ keepId: keep.id, discardId: discard.id, type: group.type });
        merged++;
      }
    }
  });
  txn();

  return { dryRun, merged, candidates: mergeLog.length, mergeLog: mergeLog.slice(0, 50) };
}

// ===================== TERRITORY MANAGEMENT =====================

function ensureTerritoryTables() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS territories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      description TEXT DEFAULT '',
      states TEXT DEFAULT '', -- comma-separated state codes
      cities TEXT DEFAULT '', -- comma-separated city names
      owner TEXT DEFAULT '', -- assigned rep
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS lead_territory (
      lead_id INTEGER NOT NULL,
      territory_id INTEGER NOT NULL,
      assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (lead_id, territory_id),
      FOREIGN KEY (lead_id) REFERENCES leads(id),
      FOREIGN KEY (territory_id) REFERENCES territories(id)
    );
  `);
}

function getTerritories() {
  const db = getDb();
  ensureTerritoryTables();
  const territories = db.prepare("SELECT * FROM territories ORDER BY name").all();
  for (const t of territories) {
    t.leadCount = db.prepare("SELECT COUNT(*) as cnt FROM lead_territory WHERE territory_id = ?").get(t.id).cnt;
  }
  return territories;
}

function createTerritory(name, description = '', states = '', cities = '', owner = '') {
  const db = getDb();
  ensureTerritoryTables();
  return db.prepare("INSERT INTO territories (name, description, states, cities, owner) VALUES (?, ?, ?, ?, ?)").run(name, description, states, cities, owner);
}

function deleteTerritory(id) {
  const db = getDb();
  ensureTerritoryTables();
  db.prepare("DELETE FROM lead_territory WHERE territory_id = ?").run(id);
  return db.prepare("DELETE FROM territories WHERE id = ?").run(id);
}

function assignLeadsToTerritory(territoryId) {
  const db = getDb();
  ensureTerritoryTables();
  const territory = db.prepare("SELECT * FROM territories WHERE id = ?").get(territoryId);
  if (!territory) return { assigned: 0 };

  const states = territory.states ? territory.states.split(',').map(s => s.trim()).filter(Boolean) : [];
  const cities = territory.cities ? territory.cities.split(',').map(s => s.trim().toLowerCase()).filter(Boolean) : [];

  let conditions = [];
  let params = [];
  if (states.length > 0) {
    conditions.push(`state IN (${states.map(() => '?').join(',')})`);
    params.push(...states);
  }
  if (cities.length > 0) {
    conditions.push(`LOWER(city) IN (${cities.map(() => '?').join(',')})`);
    params.push(...cities);
  }
  if (conditions.length === 0) return { assigned: 0 };

  const leads = db.prepare(`SELECT id FROM leads WHERE ${conditions.join(' OR ')}`).all(...params);
  let assigned = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      try {
        db.prepare("INSERT OR IGNORE INTO lead_territory (lead_id, territory_id) VALUES (?, ?)").run(lead.id, territoryId);
        assigned++;
      } catch {}
    }
  });
  txn();
  return { assigned, total: leads.length };
}

function getTerritoryLeads(territoryId, limit = 50) {
  const db = getDb();
  ensureTerritoryTables();
  return db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.phone, l.firm_name, l.city, l.state, l.lead_score
    FROM leads l JOIN lead_territory lt ON lt.lead_id = l.id
    WHERE lt.territory_id = ?
    ORDER BY l.lead_score DESC
    LIMIT ?
  `).all(territoryId, limit);
}

// ===================== SOURCE ATTRIBUTION =====================

function getSourceAttribution() {
  const db = getDb();
  const sources = db.prepare(`
    SELECT primary_source as source,
           COUNT(*) as total,
           COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
           COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) as with_phone,
           COUNT(CASE WHEN website IS NOT NULL AND website != '' THEN 1 END) as with_website,
           AVG(lead_score) as avg_score,
           MAX(created_at) as last_lead
    FROM leads
    WHERE primary_source IS NOT NULL AND primary_source != ''
    GROUP BY primary_source
    ORDER BY total DESC
  `).all();

  return sources.map(s => ({
    ...s,
    avg_score: Math.round(s.avg_score || 0),
    emailRate: s.total > 0 ? Math.round((s.with_email / s.total) * 100) : 0,
    phoneRate: s.total > 0 ? Math.round((s.with_phone / s.total) * 100) : 0,
  }));
}

// ===================== SINGLE LEAD ENRICHMENT ON-DEMAND =====================

function getLeadForEnrichment(leadId) {
  const db = getDb();
  const lead = db.prepare("SELECT * FROM leads WHERE id = ?").get(leadId);
  if (!lead) return null;

  // Find what's missing
  const missing = [];
  if (!lead.email) missing.push('email');
  if (!lead.phone) missing.push('phone');
  if (!lead.website) missing.push('website');
  if (!lead.firm_name) missing.push('firm_name');
  if (!lead.title) missing.push('title');
  if (!lead.linkedin_url) missing.push('linkedin_url');

  // Find potential enrichment sources
  const sources = [];
  if (lead.state && ['CA', 'NY'].includes(lead.state)) sources.push('name-lookup');
  if (lead.state && lead.state.startsWith('AU-')) sources.push('au-nsw-lookup');
  if (lead.website && !lead.email) sources.push('website-crawl');
  if (lead.email && !lead.website) sources.push('email-domain');
  sources.push('martindale', 'lawyers-com');

  return { lead, missing, sources, completeness: Math.round(((6 - missing.length) / 6) * 100) };
}

// ===================== INTENT SIGNALS & BUYING TRIGGERS =====================

function getIntentSignals() {
  const db = getDb();
  ensureActivityTable();

  // Composite intent: engagement + data freshness + admission recency + firm growth
  const signals = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.firm_name, l.city, l.state, l.email, l.lead_score,
           COALESCE(a.activity_count, 0) as engagement,
           CASE WHEN l.admission_date != '' AND l.admission_date > date('now', '-6 months') THEN 30 ELSE 0 END as new_admission_pts,
           CASE WHEN l.firm_changed_at IS NOT NULL THEN 20 ELSE 0 END as firm_change_pts,
           CASE WHEN l.email IS NOT NULL AND l.email != '' THEN 10 ELSE 0 END as has_email_pts,
           CASE WHEN l.phone IS NOT NULL AND l.phone != '' THEN 5 ELSE 0 END as has_phone_pts
    FROM leads l
    LEFT JOIN (
      SELECT lead_id, COUNT(*) as activity_count FROM lead_activities GROUP BY lead_id
    ) a ON a.lead_id = l.id
    ORDER BY (COALESCE(a.activity_count, 0) * 3 +
              CASE WHEN l.admission_date != '' AND l.admission_date > date('now', '-6 months') THEN 30 ELSE 0 END +
              CASE WHEN l.firm_changed_at IS NOT NULL THEN 20 ELSE 0 END +
              CASE WHEN l.email IS NOT NULL AND l.email != '' THEN 10 ELSE 0 END) DESC
    LIMIT 50
  `).all();

  return signals.map(s => ({
    ...s,
    intentScore: Math.min(100, s.engagement * 3 + s.new_admission_pts + s.firm_change_pts + s.has_email_pts + s.has_phone_pts),
  }));
}

function getPracticeAreaTrends() {
  const db = getDb();
  // Which practice areas are growing fastest (most new leads recently)
  return db.prepare(`
    SELECT practice_area, COUNT(*) as total,
           COUNT(CASE WHEN created_at > date('now', '-30 days') THEN 1 END) as last_30d,
           COUNT(CASE WHEN created_at > date('now', '-7 days') THEN 1 END) as last_7d
    FROM leads
    WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area
    HAVING total >= 5
    ORDER BY last_30d DESC
    LIMIT 20
  `).all();
}

// ===================== LEAD ROUTING RULES =====================

function ensureRoutingTables() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS routing_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      priority INTEGER DEFAULT 0,
      conditions TEXT NOT NULL, -- JSON array of {field, operator, value}
      action_type TEXT NOT NULL, -- tag, territory, pipeline, sequence
      action_value TEXT NOT NULL,
      enabled INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
}

function getRoutingRules() {
  const db = getDb();
  ensureRoutingTables();
  return db.prepare("SELECT * FROM routing_rules ORDER BY priority DESC, id").all();
}

function createRoutingRule(name, conditions, actionType, actionValue, priority = 0) {
  const db = getDb();
  ensureRoutingTables();
  return db.prepare("INSERT INTO routing_rules (name, conditions, action_type, action_value, priority) VALUES (?, ?, ?, ?, ?)")
    .run(name, JSON.stringify(conditions), actionType, actionValue, priority);
}

function deleteRoutingRule(id) {
  const db = getDb();
  ensureRoutingTables();
  return db.prepare("DELETE FROM routing_rules WHERE id = ?").run(id);
}

function runRoutingRules() {
  const db = getDb();
  ensureRoutingTables();
  const rules = db.prepare("SELECT * FROM routing_rules WHERE enabled = 1 ORDER BY priority DESC").all();
  let totalApplied = 0;

  for (const rule of rules) {
    const conditions = JSON.parse(rule.conditions);
    // Build WHERE clause from conditions
    const clauses = [];
    const params = [];
    for (const cond of conditions) {
      switch (cond.operator) {
        case 'equals':
          clauses.push(`${cond.field} = ?`); params.push(cond.value); break;
        case 'contains':
          clauses.push(`${cond.field} LIKE ?`); params.push(`%${cond.value}%`); break;
        case 'starts_with':
          clauses.push(`${cond.field} LIKE ?`); params.push(`${cond.value}%`); break;
        case 'is_empty':
          clauses.push(`(${cond.field} IS NULL OR ${cond.field} = '')`); break;
        case 'is_not_empty':
          clauses.push(`(${cond.field} IS NOT NULL AND ${cond.field} != '')`); break;
        case 'greater_than':
          clauses.push(`CAST(${cond.field} AS INTEGER) > ?`); params.push(parseInt(cond.value)); break;
        case 'less_than':
          clauses.push(`CAST(${cond.field} AS INTEGER) < ?`); params.push(parseInt(cond.value)); break;
      }
    }
    if (clauses.length === 0) continue;

    const allowedFields = new Set(['state', 'city', 'practice_area', 'firm_name', 'lead_score', 'email', 'phone', 'pipeline_stage', 'tags', 'email_type', 'confidence_score', 'icp_score', 'bar_status']);
    const allFieldsSafe = conditions.every(c => allowedFields.has(c.field));
    if (!allFieldsSafe) continue;

    const leads = db.prepare(`SELECT id FROM leads WHERE ${clauses.join(' AND ')}`).all(...params);

    // Apply action
    let applied = 0;
    for (const lead of leads) {
      switch (rule.action_type) {
        case 'tag':
          db.prepare("UPDATE leads SET tags = CASE WHEN tags IS NULL OR tags = '' THEN ? ELSE tags || ',' || ? END WHERE id = ? AND (tags IS NULL OR tags NOT LIKE ?)").run(rule.action_value, rule.action_value, lead.id, `%${rule.action_value}%`);
          applied++; break;
        case 'pipeline':
          db.prepare("UPDATE leads SET pipeline_stage = ? WHERE id = ?").run(rule.action_value, lead.id);
          applied++; break;
      }
    }
    totalApplied += applied;
  }

  return { rulesRun: rules.length, leadsAffected: totalApplied };
}

// ===================== DATA COMPLETENESS HEATMAP =====================

function getCompletenessHeatmap() {
  const db = getDb();
  const fields = ['email', 'phone', 'website', 'firm_name', 'practice_area', 'title', 'linkedin_url', 'bar_number'];

  const states = db.prepare(`
    SELECT state, COUNT(*) as total,
           ${fields.map(f => `COUNT(CASE WHEN ${f} IS NOT NULL AND ${f} != '' THEN 1 END) as ${f}_count`).join(',\n           ')}
    FROM leads
    WHERE state IS NOT NULL AND state != ''
    GROUP BY state
    ORDER BY total DESC
    LIMIT 30
  `).all();

  return states.map(s => {
    const fieldRates = {};
    for (const f of fields) {
      fieldRates[f] = s.total > 0 ? Math.round((s[f + '_count'] / s.total) * 100) : 0;
    }
    return { state: s.state, total: s.total, fields: fieldRates };
  });
}

function getEnrichmentRecommendations() {
  const db = getDb();

  // Which states have the most leads missing email?
  const emailGaps = db.prepare(`
    SELECT state, COUNT(*) as missing
    FROM leads
    WHERE (email IS NULL OR email = '') AND state IS NOT NULL AND state != ''
    GROUP BY state
    ORDER BY missing DESC
    LIMIT 10
  `).all();

  // Which states have the most leads missing phone?
  const phoneGaps = db.prepare(`
    SELECT state, COUNT(*) as missing
    FROM leads
    WHERE (phone IS NULL OR phone = '') AND state IS NOT NULL AND state != ''
    GROUP BY state
    ORDER BY missing DESC
    LIMIT 10
  `).all();

  // Leads with website but no email (best crawl candidates)
  const crawlCandidates = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE website IS NOT NULL AND website != '' AND (email IS NULL OR email = '')
  `).get().cnt;

  // Leads with email but no website (can derive domain)
  const domainCandidates = db.prepare(`
    SELECT COUNT(*) as cnt FROM leads
    WHERE email IS NOT NULL AND email != '' AND (website IS NULL OR website = '')
  `).get().cnt;

  return { emailGaps, phoneGaps, crawlCandidates, domainCandidates };
}

// ===================== INSTANTLY-OPTIMIZED EXPORT =====================

function exportForInstantly(filters = {}) {
  const db = getDb();
  ensureDncTable();

  // Get DNC lists for filtering
  const dncEmails = new Set(db.prepare("SELECT value FROM dnc_list WHERE type = 'email'").all().map(r => r.value));
  const dncDomains = new Set(db.prepare("SELECT value FROM dnc_list WHERE type = 'domain'").all().map(r => r.value));

  let where = "WHERE email IS NOT NULL AND email != ''";
  const params = [];

  if (filters.state) { where += " AND state = ?"; params.push(filters.state); }
  if (filters.practiceArea) { where += " AND practice_area LIKE ?"; params.push(`%${filters.practiceArea}%`); }
  if (filters.minScore) { where += " AND lead_score >= ?"; params.push(parseInt(filters.minScore)); }
  if (filters.pipelineStage) { where += " AND pipeline_stage = ?"; params.push(filters.pipelineStage); }
  if (filters.tags) { where += " AND tags LIKE ?"; params.push(`%${filters.tags}%`); }

  const leads = db.prepare(`
    SELECT first_name, last_name, email, phone, firm_name as company_name,
           title as job_title, city, state, website as company_url,
           linkedin_url, practice_area, lead_score, email_type, confidence_score
    FROM leads ${where}
    ORDER BY lead_score DESC, confidence_score DESC
  `).all(...params);

  // Filter out DNC
  const clean = leads.filter(l => {
    const email = l.email.toLowerCase();
    const domain = email.split('@')[1] || '';
    return !dncEmails.has(email) && !dncDomains.has(domain);
  });

  // Format for Instantly
  return clean.map(l => ({
    email: l.email,
    first_name: l.first_name || '',
    last_name: l.last_name || '',
    company_name: l.company_name || '',
    phone: l.phone || '',
    website: l.company_url || '',
    personalization: `${l.practice_area || 'legal'} attorney in ${l.city || l.state || 'your area'}`,
    custom1: l.job_title || 'Attorney',
    custom2: l.linkedin_url || '',
    custom3: l.practice_area || '',
  }));
}

// ===================== LEAD NOTES / COMMENTS =====================

function ensureNotesTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS lead_notes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      author TEXT DEFAULT 'user',
      content TEXT NOT NULL,
      pinned INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (lead_id) REFERENCES leads(id)
    );
    CREATE INDEX IF NOT EXISTS idx_notes_lead ON lead_notes(lead_id);
  `);
}

function addNote(leadId, content, author = 'user') {
  const db = getDb();
  ensureNotesTable();
  const result = db.prepare("INSERT INTO lead_notes (lead_id, content, author) VALUES (?, ?, ?)").run(leadId, content, author);
  // Track activity
  try { trackActivity(leadId, 'note_added', content.substring(0, 100)); } catch {}
  return { id: result.lastInsertRowid };
}

function getLeadNotes(leadId) {
  const db = getDb();
  ensureNotesTable();
  return db.prepare("SELECT * FROM lead_notes WHERE lead_id = ? ORDER BY pinned DESC, created_at DESC").all(leadId);
}

function deleteNote(noteId) {
  const db = getDb();
  ensureNotesTable();
  return db.prepare("DELETE FROM lead_notes WHERE id = ?").run(noteId);
}

function togglePinNote(noteId) {
  const db = getDb();
  ensureNotesTable();
  return db.prepare("UPDATE lead_notes SET pinned = CASE WHEN pinned = 1 THEN 0 ELSE 1 END WHERE id = ?").run(noteId);
}

function getRecentNotes(limit = 20) {
  const db = getDb();
  ensureNotesTable();
  return db.prepare(`
    SELECT n.*, l.first_name, l.last_name, l.firm_name
    FROM lead_notes n JOIN leads l ON l.id = n.lead_id
    ORDER BY n.created_at DESC LIMIT ?
  `).all(limit);
}

// ===================== A/B TESTING FOR SEQUENCES =====================

function getSequenceVariantStats(sequenceId) {
  const db = getDb();
  ensureSequenceTables();
  // Get steps with variant counts
  const steps = db.prepare(`
    SELECT step_number, variant, subject, body,
           (SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ss.sequence_id) as total_enrolled
    FROM sequence_steps ss
    WHERE sequence_id = ?
    ORDER BY step_number, variant
  `).all(sequenceId);

  // Group by step number
  const grouped = {};
  for (const step of steps) {
    if (!grouped[step.step_number]) grouped[step.step_number] = [];
    grouped[step.step_number].push(step);
  }
  return grouped;
}

function assignVariant(sequenceId, leadId) {
  // Randomly assign A or B variant
  const variants = ['A', 'B'];
  return variants[Math.floor(Math.random() * variants.length)];
}

// ===================== LEAD LIFECYCLE TIMELINE =====================

function getLeadTimeline(leadId) {
  const db = getDb();
  ensureActivityTable();
  ensureNotesTable();

  const events = [];

  // Activities
  try {
    const activities = db.prepare("SELECT 'activity' as type, action as event, details, created_at FROM lead_activities WHERE lead_id = ?").all(leadId);
    events.push(...activities);
  } catch {}

  // Notes
  try {
    const notes = db.prepare("SELECT 'note' as type, 'note_added' as event, content as details, created_at FROM lead_notes WHERE lead_id = ?").all(leadId);
    events.push(...notes);
  } catch {}

  // Lead creation
  const lead = db.prepare("SELECT created_at, updated_at, pipeline_stage, tags FROM leads WHERE id = ?").get(leadId);
  if (lead) {
    events.push({ type: 'system', event: 'lead_created', details: '', created_at: lead.created_at });
    if (lead.updated_at && lead.updated_at !== lead.created_at) {
      events.push({ type: 'system', event: 'lead_updated', details: '', created_at: lead.updated_at });
    }
  }

  // Change history (snapshots)
  try {
    const changes = db.prepare("SELECT 'change' as type, field_name as event, old_value || ' → ' || new_value as details, detected_at as created_at FROM lead_snapshots WHERE lead_id = ?").all(leadId);
    events.push(...changes);
  } catch {}

  // Sequence enrollments
  try {
    const enrollments = db.prepare(`
      SELECT 'sequence' as type, 'enrolled_in_sequence' as event, s.name as details, se.enrolled_at as created_at
      FROM sequence_enrollments se JOIN sequences s ON s.id = se.sequence_id
      WHERE se.lead_id = ?
    `).all(leadId);
    events.push(...enrollments);
  } catch {}

  return events.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
}

// ===================== SMART LIST BUILDER (COMPOUND FILTERS) =====================

function ensureSmartListTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS smart_lists (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      filters TEXT NOT NULL, -- JSON: {logic: 'AND'|'OR', conditions: [{field, operator, value}]}
      auto_update INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      last_count INTEGER DEFAULT 0
    );
  `);
}

function getSmartLists() {
  const db = getDb();
  ensureSmartListTable();
  const lists = db.prepare("SELECT * FROM smart_lists ORDER BY created_at DESC").all();
  // Update counts
  for (const list of lists) {
    try {
      const count = executeSmartListQuery(JSON.parse(list.filters), true);
      if (count !== list.last_count) {
        db.prepare("UPDATE smart_lists SET last_count = ? WHERE id = ?").run(count, list.id);
        list.last_count = count;
      }
    } catch {}
  }
  return lists;
}

function createSmartList(name, description, filters) {
  const db = getDb();
  ensureSmartListTable();
  const count = executeSmartListQuery(filters, true);
  return db.prepare("INSERT INTO smart_lists (name, description, filters, last_count) VALUES (?, ?, ?, ?)").run(name, description, JSON.stringify(filters), count);
}

function deleteSmartList(id) {
  const db = getDb();
  ensureSmartListTable();
  return db.prepare("DELETE FROM smart_lists WHERE id = ?").run(id);
}

function executeSmartListQuery(filters, countOnly = false) {
  const db = getDb();
  const logic = filters.logic || 'AND';
  const conditions = filters.conditions || [];
  if (conditions.length === 0) return countOnly ? 0 : [];

  const allowedFields = new Set(['state', 'city', 'practice_area', 'firm_name', 'lead_score', 'email', 'phone', 'website', 'pipeline_stage', 'tags', 'email_type', 'confidence_score', 'icp_score', 'bar_status', 'title', 'linkedin_url', 'country']);
  const clauses = [];
  const params = [];

  for (const cond of conditions) {
    if (!allowedFields.has(cond.field)) continue;
    switch (cond.operator) {
      case 'equals': clauses.push(`${cond.field} = ?`); params.push(cond.value); break;
      case 'not_equals': clauses.push(`${cond.field} != ?`); params.push(cond.value); break;
      case 'contains': clauses.push(`${cond.field} LIKE ?`); params.push(`%${cond.value}%`); break;
      case 'starts_with': clauses.push(`${cond.field} LIKE ?`); params.push(`${cond.value}%`); break;
      case 'is_empty': clauses.push(`(${cond.field} IS NULL OR ${cond.field} = '')`); break;
      case 'is_not_empty': clauses.push(`(${cond.field} IS NOT NULL AND ${cond.field} != '')`); break;
      case 'greater_than': clauses.push(`CAST(${cond.field} AS INTEGER) > ?`); params.push(parseInt(cond.value)); break;
      case 'less_than': clauses.push(`CAST(${cond.field} AS INTEGER) < ?`); params.push(parseInt(cond.value)); break;
    }
  }

  if (clauses.length === 0) return countOnly ? 0 : [];
  const joiner = logic === 'OR' ? ' OR ' : ' AND ';

  if (countOnly) {
    return db.prepare(`SELECT COUNT(*) as cnt FROM leads WHERE ${clauses.join(joiner)}`).get(...params).cnt;
  }
  return db.prepare(`SELECT * FROM leads WHERE ${clauses.join(joiner)} ORDER BY lead_score DESC LIMIT 500`).all(...params);
}

function getSmartListLeads(listId, limit = 100) {
  const db = getDb();
  ensureSmartListTable();
  const list = db.prepare("SELECT * FROM smart_lists WHERE id = ?").get(listId);
  if (!list) return [];
  const filters = JSON.parse(list.filters);
  const leads = executeSmartListQuery(filters);
  return leads.slice(0, limit);
}

// ===================== CUSTOM SCORING MODELS =====================

function ensureScoringModelTable() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS scoring_models (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      is_active INTEGER DEFAULT 0,
      weights TEXT NOT NULL, -- JSON: {email: 20, phone: 15, website: 10, ...}
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
}

function getScoringModels() {
  const db = getDb();
  ensureScoringModelTable();
  return db.prepare("SELECT * FROM scoring_models ORDER BY is_active DESC, created_at DESC").all();
}

function createScoringModel(name, weights) {
  const db = getDb();
  ensureScoringModelTable();
  return db.prepare("INSERT INTO scoring_models (name, weights) VALUES (?, ?)").run(name, JSON.stringify(weights));
}

function activateScoringModel(id) {
  const db = getDb();
  ensureScoringModelTable();
  db.prepare("UPDATE scoring_models SET is_active = 0").run();
  return db.prepare("UPDATE scoring_models SET is_active = 1 WHERE id = ?").run(id);
}

function deleteScoringModel(id) {
  const db = getDb();
  ensureScoringModelTable();
  return db.prepare("DELETE FROM scoring_models WHERE id = ?").run(id);
}

function applyCustomScoring() {
  const db = getDb();
  ensureScoringModelTable();
  const model = db.prepare("SELECT * FROM scoring_models WHERE is_active = 1").get();
  if (!model) return { error: 'No active scoring model' };

  const weights = JSON.parse(model.weights);
  // Build dynamic score calculation
  const leads = db.prepare("SELECT id, email, phone, website, firm_name, practice_area, title, linkedin_url, bar_number FROM leads").all();

  let updated = 0;
  const txn = db.transaction(() => {
    for (const lead of leads) {
      let score = 0;
      if (lead.email && lead.email !== '') score += (weights.email || 0);
      if (lead.phone && lead.phone !== '') score += (weights.phone || 0);
      if (lead.website && lead.website !== '') score += (weights.website || 0);
      if (lead.firm_name && lead.firm_name !== '') score += (weights.firm_name || 0);
      if (lead.practice_area && lead.practice_area !== '') score += (weights.practice_area || 0);
      if (lead.title && lead.title !== '') score += (weights.title || 0);
      if (lead.linkedin_url && lead.linkedin_url !== '') score += (weights.linkedin || 0);
      if (lead.bar_number && lead.bar_number !== '') score += (weights.bar_number || 0);
      score = Math.min(100, Math.max(0, score));
      db.prepare("UPDATE leads SET lead_score = ? WHERE id = ?").run(score, lead.id);
      updated++;
    }
  });
  txn();

  return { model: model.name, leadsScored: updated };
}

// ===================== CAMPAIGN MANAGEMENT =====================

function ensureCampaignTables() {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS campaigns (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      status TEXT DEFAULT 'draft', -- draft, active, paused, completed
      lead_count INTEGER DEFAULT 0,
      sent_count INTEGER DEFAULT 0,
      open_count INTEGER DEFAULT 0,
      reply_count INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      sent_at DATETIME
    );
    CREATE TABLE IF NOT EXISTS campaign_leads (
      campaign_id INTEGER NOT NULL,
      lead_id INTEGER NOT NULL,
      status TEXT DEFAULT 'pending', -- pending, sent, opened, replied, bounced
      sent_at DATETIME,
      opened_at DATETIME,
      replied_at DATETIME,
      PRIMARY KEY (campaign_id, lead_id),
      FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
      FOREIGN KEY (lead_id) REFERENCES leads(id)
    );
  `);
}

function getCampaigns() {
  const db = getDb();
  ensureCampaignTables();
  return db.prepare("SELECT * FROM campaigns ORDER BY created_at DESC").all();
}

function createCampaign(name, description = '') {
  const db = getDb();
  ensureCampaignTables();
  return db.prepare("INSERT INTO campaigns (name, description) VALUES (?, ?)").run(name, description);
}

function deleteCampaign(id) {
  const db = getDb();
  ensureCampaignTables();
  db.prepare("DELETE FROM campaign_leads WHERE campaign_id = ?").run(id);
  return db.prepare("DELETE FROM campaigns WHERE id = ?").run(id);
}

function addLeadsToCampaign(campaignId, leadIds) {
  const db = getDb();
  ensureCampaignTables();
  let added = 0;
  const txn = db.transaction(() => {
    for (const lid of leadIds) {
      try {
        db.prepare("INSERT OR IGNORE INTO campaign_leads (campaign_id, lead_id) VALUES (?, ?)").run(campaignId, lid);
        added++;
      } catch {}
    }
    db.prepare("UPDATE campaigns SET lead_count = (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = ?) WHERE id = ?").run(campaignId, campaignId);
  });
  txn();
  return { added };
}

function getCampaignLeads(campaignId, limit = 100) {
  const db = getDb();
  ensureCampaignTables();
  return db.prepare(`
    SELECT cl.*, l.first_name, l.last_name, l.email, l.firm_name, l.city, l.state
    FROM campaign_leads cl JOIN leads l ON l.id = cl.lead_id
    WHERE cl.campaign_id = ?
    ORDER BY cl.sent_at DESC NULLS LAST
    LIMIT ?
  `).all(campaignId, limit);
}

function updateCampaignStatus(campaignId, status) {
  const db = getDb();
  ensureCampaignTables();
  const extra = status === 'active' ? ", sent_at = CURRENT_TIMESTAMP" : '';
  return db.prepare(`UPDATE campaigns SET status = ?${extra} WHERE id = ?`).run(status, campaignId);
}

// ===================== CROSS-SOURCE DEDUP VIEW =====================

function getCrossSourceDuplicates(limit = 50) {
  const db = getDb();
  // Find leads that appear from multiple scraper sources
  return db.prepare(`
    SELECT LOWER(first_name) || ' ' || LOWER(last_name) || ' ' || LOWER(city) as match_key,
           GROUP_CONCAT(DISTINCT primary_source) as sources,
           COUNT(DISTINCT primary_source) as source_count,
           GROUP_CONCAT(id) as ids,
           COUNT(*) as total,
           MAX(CASE WHEN email IS NOT NULL AND email != '' THEN email END) as best_email,
           MAX(CASE WHEN phone IS NOT NULL AND phone != '' THEN phone END) as best_phone,
           MAX(CASE WHEN website IS NOT NULL AND website != '' THEN website END) as best_website,
           MAX(first_name) as first_name, MAX(last_name) as last_name, MAX(city) as city, MAX(state) as state
    FROM leads
    WHERE first_name IS NOT NULL AND first_name != '' AND last_name IS NOT NULL AND last_name != ''
    GROUP BY LOWER(first_name), LOWER(last_name), LOWER(city)
    HAVING COUNT(DISTINCT primary_source) > 1
    ORDER BY source_count DESC, total DESC
    LIMIT ?
  `).all(limit);
}

// ===================== KPI DASHBOARD METRICS =====================

function getKpiMetrics() {
  const db = getDb();
  ensureDncTable();
  ensureCampaignTables();

  const total = db.prepare("SELECT COUNT(*) as cnt FROM leads").get().cnt;
  const withEmail = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email IS NOT NULL AND email != ''").get().cnt;
  const withPhone = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE phone IS NOT NULL AND phone != ''").get().cnt;
  const avgScore = db.prepare("SELECT AVG(lead_score) as avg FROM leads WHERE lead_score > 0").get().avg || 0;
  const addedThisWeek = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE created_at > date('now', '-7 days')").get().cnt;
  const campaignReady = db.prepare("SELECT COUNT(*) as cnt FROM leads WHERE email IS NOT NULL AND email != '' AND lead_score >= 20").get().cnt;
  const dncBlocked = db.prepare("SELECT COUNT(*) as cnt FROM dnc_list").get().cnt;
  const activeCampaigns = db.prepare("SELECT COUNT(*) as cnt FROM campaigns WHERE status = 'active'").get().cnt;
  const uniqueFirms = db.prepare("SELECT COUNT(DISTINCT LOWER(firm_name)) as cnt FROM leads WHERE firm_name IS NOT NULL AND firm_name != ''").get().cnt;
  const statesCovered = db.prepare("SELECT COUNT(DISTINCT state) as cnt FROM leads WHERE state IS NOT NULL AND state != ''").get().cnt;

  return {
    total, withEmail, withPhone,
    emailRate: total > 0 ? Math.round((withEmail / total) * 100) : 0,
    phoneRate: total > 0 ? Math.round((withPhone / total) * 100) : 0,
    avgScore: Math.round(avgScore),
    addedThisWeek, campaignReady, dncBlocked, activeCampaigns,
    uniqueFirms, statesCovered,
  };
}

// ============================================================
// BATCH 18: Lead Import, Engagement Heatmap, Bulk Actions, Comparison
// ============================================================

// --- Lead List Import/CSV Merge ---
function importLeads(leads, options = {}) {
  const db = getDb();
  const { dedup = true, overwriteEmpty = true, source = 'csv-import' } = options;
  let added = 0, updated = 0, skipped = 0;
  const errors = [];

  const insertOrUpdate = db.transaction((records) => {
    for (const raw of records) {
      try {
        // Normalize field names
        const lead = {};
        for (const [k, v] of Object.entries(raw)) {
          const key = k.trim().toLowerCase().replace(/\s+/g, '_');
          lead[key] = typeof v === 'string' ? v.trim() : v;
        }

        // Map common aliases
        if (lead.firstname && !lead.first_name) lead.first_name = lead.firstname;
        if (lead.lastname && !lead.last_name) lead.last_name = lead.lastname;
        if (lead.company && !lead.firm_name) lead.firm_name = lead.company;
        if (lead.organization && !lead.firm_name) lead.firm_name = lead.organization;
        if (lead.telephone && !lead.phone) lead.phone = lead.telephone;
        if (lead.mobile && !lead.phone) lead.phone = lead.mobile;
        if (lead.url && !lead.website) lead.website = lead.url;
        if (lead.web && !lead.website) lead.website = lead.web;
        if (lead.province && !lead.state) lead.state = lead.province;
        if (lead.zip && !lead.zip_code) lead.zip_code = lead.zip;
        if (lead.postal_code && !lead.zip_code) lead.zip_code = lead.postal_code;
        if (lead.linkedin && !lead.linkedin_url) lead.linkedin_url = lead.linkedin;

        if (!lead.first_name && !lead.last_name && !lead.email) {
          skipped++;
          continue;
        }

        // Check for existing
        if (dedup) {
          let existing = null;
          if (lead.email) {
            existing = db.prepare('SELECT id FROM leads WHERE LOWER(email) = LOWER(?)').get(lead.email);
          }
          if (!existing && lead.first_name && lead.last_name && lead.city) {
            existing = db.prepare('SELECT id FROM leads WHERE LOWER(first_name) = LOWER(?) AND LOWER(last_name) = LOWER(?) AND LOWER(city) = LOWER(?)').get(lead.first_name, lead.last_name, lead.city);
          }

          if (existing && overwriteEmpty) {
            // Merge: only fill empty fields
            const current = db.prepare('SELECT * FROM leads WHERE id = ?').get(existing.id);
            const updates = [];
            const vals = [];
            for (const field of ['email', 'phone', 'website', 'firm_name', 'linkedin_url', 'practice_area', 'title', 'bio']) {
              if (lead[field] && (!current[field] || current[field] === '')) {
                updates.push(`${field} = ?`);
                vals.push(lead[field]);
              }
            }
            if (updates.length > 0) {
              db.prepare(`UPDATE leads SET ${updates.join(', ')}, updated_at = datetime('now') WHERE id = ?`).run(...vals, existing.id);
              updated++;
            } else {
              skipped++;
            }
            continue;
          } else if (existing) {
            skipped++;
            continue;
          }
        }

        // Insert new lead
        lead.primary_source = source;
        lead.created_at = new Date().toISOString();
        lead.updated_at = lead.created_at;

        const fields = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state', 'website', 'linkedin_url', 'practice_area', 'title', 'bio', 'bar_number', 'bar_status', 'primary_source', 'zip_code', 'created_at', 'updated_at'];
        const present = fields.filter(f => lead[f]);
        const placeholders = present.map(() => '?').join(', ');
        const values = present.map(f => lead[f]);

        db.prepare(`INSERT INTO leads (${present.join(', ')}) VALUES (${placeholders})`).run(...values);
        added++;
      } catch (e) {
        errors.push({ lead: raw, error: e.message });
        skipped++;
      }
    }
  });

  insertOrUpdate(leads);

  // Rescore new leads
  if (added > 0) {
    try { batchScoreLeads(); } catch {}
  }

  return { added, updated, skipped, errors: errors.slice(0, 10), total: leads.length };
}

function getImportFieldMapping(sampleHeaders) {
  const mapping = {};
  const knownFields = {
    first_name: ['first_name', 'firstname', 'first name', 'given name', 'fname'],
    last_name: ['last_name', 'lastname', 'last name', 'surname', 'family name', 'lname'],
    email: ['email', 'email address', 'e-mail', 'email_address'],
    phone: ['phone', 'telephone', 'phone number', 'mobile', 'cell', 'phone_number', 'tel'],
    firm_name: ['firm_name', 'firm', 'company', 'organization', 'org', 'company name', 'employer'],
    city: ['city', 'town', 'municipality'],
    state: ['state', 'province', 'region', 'state_code'],
    website: ['website', 'url', 'web', 'homepage', 'website_url'],
    linkedin_url: ['linkedin_url', 'linkedin', 'linkedin url', 'linkedin_profile'],
    practice_area: ['practice_area', 'practice area', 'specialty', 'specialization', 'area of practice'],
    title: ['title', 'job title', 'position', 'role'],
    bar_number: ['bar_number', 'bar number', 'bar_id', 'license number', 'license_number'],
    zip_code: ['zip_code', 'zip', 'postal_code', 'postal code', 'postcode'],
  };

  for (const header of sampleHeaders) {
    const normalized = header.trim().toLowerCase().replace(/[^a-z0-9_\s]/g, '');
    for (const [field, aliases] of Object.entries(knownFields)) {
      if (aliases.includes(normalized)) {
        mapping[header] = field;
        break;
      }
    }
    if (!mapping[header]) {
      mapping[header] = null; // unmapped
    }
  }

  return mapping;
}

// --- Engagement Heatmap ---
function getEngagementHeatmap() {
  const db = getDb();
  // Create table if not exists
  try { db.prepare('SELECT 1 FROM lead_activities LIMIT 1').get(); } catch { return { grid: [], topActions: [] }; }

  const rows = db.prepare(`
    SELECT
      CAST(strftime('%w', created_at) AS INTEGER) as day_of_week,
      CAST(strftime('%H', created_at) AS INTEGER) as hour,
      COUNT(*) as count
    FROM lead_activities
    WHERE created_at >= datetime('now', '-30 days')
    GROUP BY day_of_week, hour
    ORDER BY day_of_week, hour
  `).all();

  // Build 7x24 grid
  const grid = Array.from({ length: 7 }, () => Array(24).fill(0));
  for (const row of rows) {
    grid[row.day_of_week][row.hour] = row.count;
  }

  // Top actions
  const topActions = db.prepare(`
    SELECT action, COUNT(*) as count
    FROM lead_activities
    WHERE created_at >= datetime('now', '-30 days')
    GROUP BY action
    ORDER BY count DESC
    LIMIT 10
  `).all();

  return { grid, topActions };
}

function getLeadEngagementSparkline(leadId) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM lead_activities LIMIT 1').get(); } catch { return []; }

  return db.prepare(`
    SELECT DATE(created_at) as date, COUNT(*) as count
    FROM lead_activities
    WHERE lead_id = ?
    AND created_at >= datetime('now', '-14 days')
    GROUP BY date
    ORDER BY date
  `).all(leadId);
}

function getEngagementTimeline(days = 30) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM lead_activities LIMIT 1').get(); } catch { return []; }

  return db.prepare(`
    SELECT DATE(created_at) as date, action, COUNT(*) as count
    FROM lead_activities
    WHERE created_at >= datetime('now', '-' || ? || ' days')
    GROUP BY date, action
    ORDER BY date
  `).all(days);
}

// --- Bulk Actions ---
function bulkTagLeads(leadIds, tag) {
  const db = getDb();
  const update = db.prepare(`
    UPDATE leads SET tags = CASE
      WHEN tags IS NULL OR tags = '' THEN ?
      WHEN tags LIKE '%' || ? || '%' THEN tags
      ELSE tags || ',' || ?
    END,
    updated_at = datetime('now')
    WHERE id = ?
  `);

  const run = db.transaction((ids) => {
    let count = 0;
    for (const id of ids) {
      update.run(tag, tag, tag, id);
      count++;
    }
    return count;
  });

  return { tagged: run(leadIds) };
}

function bulkRemoveTag(leadIds, tag) {
  const db = getDb();
  const run = db.transaction((ids) => {
    let count = 0;
    for (const id of ids) {
      const lead = db.prepare('SELECT tags FROM leads WHERE id = ?').get(id);
      if (!lead || !lead.tags) continue;
      const tags = lead.tags.split(',').map(t => t.trim()).filter(t => t !== tag);
      db.prepare('UPDATE leads SET tags = ?, updated_at = datetime(\'now\') WHERE id = ?').run(tags.join(','), id);
      count++;
    }
    return count;
  });

  return { updated: run(leadIds) };
}

function bulkAssignOwner(leadIds, owner) {
  const db = getDb();
  // Ensure owner column exists
  try {
    db.prepare('SELECT owner FROM leads LIMIT 1').get();
  } catch {
    db.prepare('ALTER TABLE leads ADD COLUMN owner TEXT DEFAULT NULL').run();
  }

  const stmt = db.prepare('UPDATE leads SET owner = ?, updated_at = datetime(\'now\') WHERE id = ?');
  const run = db.transaction((ids) => {
    let count = 0;
    for (const id of ids) { stmt.run(owner, id); count++; }
    return count;
  });

  return { assigned: run(leadIds) };
}

function bulkEnrollInCampaign(leadIds, campaignId) {
  return addLeadsToCampaign(campaignId, leadIds);
}

function bulkEnrollInSequence(leadIds, sequenceId) {
  return enrollInSequence(sequenceId, leadIds);
}

function getOwners() {
  const db = getDb();
  try {
    db.prepare('SELECT owner FROM leads LIMIT 1').get();
  } catch {
    db.prepare('ALTER TABLE leads ADD COLUMN owner TEXT DEFAULT NULL').run();
  }

  return db.prepare(`
    SELECT owner, COUNT(*) as lead_count,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads
    WHERE owner IS NOT NULL AND owner != ''
    GROUP BY owner
    ORDER BY lead_count DESC
  `).all();
}

function getLeadsByOwner(owner, limit = 100) {
  const db = getDb();
  try {
    db.prepare('SELECT owner FROM leads LIMIT 1').get();
  } catch {
    db.prepare('ALTER TABLE leads ADD COLUMN owner TEXT DEFAULT NULL').run();
  }

  return db.prepare('SELECT * FROM leads WHERE owner = ? ORDER BY lead_score DESC LIMIT ?').all(owner, limit);
}

// --- Lead Comparison & Merge ---
function getLeadComparisonData(leadIds) {
  const db = getDb();
  if (!leadIds || leadIds.length < 2) return { leads: [], fields: [] };

  const placeholders = leadIds.map(() => '?').join(',');
  const leads = db.prepare(`SELECT * FROM leads WHERE id IN (${placeholders})`).all(...leadIds);

  // List fields with differences
  const compFields = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state', 'website', 'linkedin_url', 'practice_area', 'title', 'bio', 'bar_number', 'bar_status', 'zip_code', 'primary_source', 'lead_score'];
  const fields = [];

  for (const field of compFields) {
    const values = leads.map(l => l[field] || '');
    const unique = [...new Set(values.filter(v => v !== ''))];
    fields.push({
      name: field,
      values: leads.map(l => ({ leadId: l.id, value: l[field] || '' })),
      hasDifference: unique.length > 1,
      hasData: unique.length > 0,
    });
  }

  return { leads, fields };
}

function mergeLeadsWithPicks(targetId, sourceIds, fieldPicks = {}) {
  const db = getDb();

  const target = db.prepare('SELECT * FROM leads WHERE id = ?').get(targetId);
  if (!target) throw new Error('Target lead not found');

  // Apply field picks (each pick is field → leadId to take from)
  const updates = [];
  const vals = [];
  for (const [field, fromLeadId] of Object.entries(fieldPicks)) {
    if (fromLeadId === targetId) continue; // Already has this value
    const sourceLead = db.prepare('SELECT * FROM leads WHERE id = ?').get(fromLeadId);
    if (sourceLead && sourceLead[field]) {
      updates.push(`${field} = ?`);
      vals.push(sourceLead[field]);
    }
  }

  if (updates.length > 0) {
    db.prepare(`UPDATE leads SET ${updates.join(', ')}, updated_at = datetime('now') WHERE id = ?`).run(...vals, targetId);
  }

  // Delete source leads
  const deleteIds = sourceIds.filter(id => id !== targetId);
  if (deleteIds.length > 0) {
    const ph = deleteIds.map(() => '?').join(',');
    db.prepare(`DELETE FROM leads WHERE id IN (${ph})`).run(...deleteIds);
  }

  return { targetId, merged: deleteIds.length, fieldsUpdated: updates.length };
}

// ============================================================
// BATCH 19: Leaderboard, Automation Rules, Data Quality, Export Profiles
// ============================================================

// --- Lead Leaderboard ---
function getLeaderboard(options = {}) {
  const db = getDb();
  const { state, practiceArea, metric = 'lead_score', limit = 50 } = options;

  const allowedMetrics = ['lead_score', 'engagement_score', 'completeness'];
  const metricCol = allowedMetrics.includes(metric) ? metric : 'lead_score';

  let where = '1=1';
  const params = [];
  if (state) { where += ' AND state = ?'; params.push(state); }
  if (practiceArea) { where += ' AND practice_area LIKE ?'; params.push('%' + practiceArea + '%'); }

  let orderBy;
  if (metricCol === 'completeness') {
    orderBy = `(CASE WHEN email IS NOT NULL AND email != '' THEN 20 ELSE 0 END +
      CASE WHEN phone IS NOT NULL AND phone != '' THEN 15 ELSE 0 END +
      CASE WHEN website IS NOT NULL AND website != '' THEN 10 ELSE 0 END +
      CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 10 ELSE 0 END +
      CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 10 ELSE 0 END +
      CASE WHEN practice_area IS NOT NULL AND practice_area != '' THEN 5 ELSE 0 END +
      CASE WHEN title IS NOT NULL AND title != '' THEN 5 ELSE 0 END) DESC`;
  } else if (metricCol === 'engagement_score') {
    // Use the engagement_score column if it exists, else lead_score
    try { db.prepare('SELECT engagement_score FROM leads LIMIT 1').get(); orderBy = 'engagement_score DESC'; } catch { orderBy = 'lead_score DESC'; }
  } else {
    orderBy = 'lead_score DESC';
  }

  params.push(limit);
  const leads = db.prepare(`
    SELECT id, first_name, last_name, email, phone, firm_name, city, state, practice_area,
      lead_score, website, linkedin_url, title,
      (CASE WHEN email IS NOT NULL AND email != '' THEN 20 ELSE 0 END +
       CASE WHEN phone IS NOT NULL AND phone != '' THEN 15 ELSE 0 END +
       CASE WHEN website IS NOT NULL AND website != '' THEN 10 ELSE 0 END +
       CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 10 ELSE 0 END +
       CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 10 ELSE 0 END +
       CASE WHEN practice_area IS NOT NULL AND practice_area != '' THEN 5 ELSE 0 END +
       CASE WHEN title IS NOT NULL AND title != '' THEN 5 ELSE 0 END) as completeness
    FROM leads
    WHERE ${where}
    ORDER BY ${orderBy}
    LIMIT ?
  `).all(...params);

  return leads;
}

function getLeaderboardByState(limit = 5) {
  const db = getDb();
  return db.prepare(`
    SELECT state, COUNT(*) as total,
      ROUND(AVG(lead_score), 1) as avg_score,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      MAX(lead_score) as top_score
    FROM leads
    WHERE state IS NOT NULL AND state != ''
    GROUP BY state
    ORDER BY avg_score DESC
    LIMIT ?
  `).all(limit);
}

// --- Lifecycle Automation Rules ---
function getAutomationRules() {
  const db = getDb();
  try {
    db.prepare('SELECT 1 FROM automation_rules LIMIT 1').get();
  } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS automation_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      trigger_event TEXT NOT NULL,
      conditions TEXT NOT NULL DEFAULT '{}',
      action_type TEXT NOT NULL,
      action_value TEXT NOT NULL DEFAULT '',
      enabled INTEGER DEFAULT 1,
      run_count INTEGER DEFAULT 0,
      last_run_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`).run();
  }
  return db.prepare('SELECT * FROM automation_rules ORDER BY created_at DESC').all();
}

function createAutomationRule(name, triggerEvent, conditions, actionType, actionValue) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM automation_rules LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS automation_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      trigger_event TEXT NOT NULL,
      conditions TEXT NOT NULL DEFAULT '{}',
      action_type TEXT NOT NULL,
      action_value TEXT NOT NULL DEFAULT '',
      enabled INTEGER DEFAULT 1,
      run_count INTEGER DEFAULT 0,
      last_run_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`).run();
  }

  const validEvents = ['lead_added', 'lead_updated', 'score_changed', 'email_found', 'dnc_added'];
  if (!validEvents.includes(triggerEvent)) throw new Error('Invalid trigger event. Valid: ' + validEvents.join(', '));

  const validActions = ['tag', 'move_stage', 'enroll_sequence', 'enroll_campaign', 'assign_owner', 'notify'];
  if (!validActions.includes(actionType)) throw new Error('Invalid action type. Valid: ' + validActions.join(', '));

  return db.prepare(`INSERT INTO automation_rules (name, trigger_event, conditions, action_type, action_value)
    VALUES (?, ?, ?, ?, ?)`).run(name, triggerEvent, JSON.stringify(conditions), actionType, actionValue);
}

function deleteAutomationRule(id) {
  const db = getDb();
  db.prepare('DELETE FROM automation_rules WHERE id = ?').run(id);
}

function toggleAutomationRule(id) {
  const db = getDb();
  db.prepare('UPDATE automation_rules SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE id = ?').run(id);
}

function runAutomationRules(event = 'lead_added', leads = []) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM automation_rules LIMIT 1').get(); } catch { return { triggered: 0 }; }

  const rules = db.prepare('SELECT * FROM automation_rules WHERE enabled = 1 AND trigger_event = ?').all(event);
  let triggered = 0;

  for (const rule of rules) {
    const conditions = JSON.parse(rule.conditions || '{}');
    const matching = leads.filter(lead => {
      for (const [field, condition] of Object.entries(conditions)) {
        const val = lead[field];
        if (typeof condition === 'object') {
          if (condition.op === 'gt' && !(Number(val) > Number(condition.value))) return false;
          if (condition.op === 'lt' && !(Number(val) < Number(condition.value))) return false;
          if (condition.op === 'eq' && val !== condition.value) return false;
          if (condition.op === 'not_empty' && (!val || val === '')) return false;
          if (condition.op === 'contains' && (!val || !val.toLowerCase().includes(condition.value.toLowerCase()))) return false;
        } else {
          if (val !== condition) return false;
        }
      }
      return true;
    });

    if (matching.length > 0) {
      const ids = matching.map(l => l.id);
      switch (rule.action_type) {
        case 'tag':
          bulkTagLeads(ids, rule.action_value);
          break;
        case 'move_stage':
          for (const id of ids) {
            try { db.prepare("UPDATE leads SET pipeline_stage = ?, updated_at = datetime('now') WHERE id = ?").run(rule.action_value, id); } catch {}
          }
          break;
        case 'assign_owner':
          bulkAssignOwner(ids, rule.action_value);
          break;
      }
      triggered += matching.length;
      db.prepare("UPDATE automation_rules SET run_count = run_count + ?, last_run_at = datetime('now') WHERE id = ?").run(matching.length, rule.id);
    }
  }

  return { triggered, rulesEvaluated: rules.length };
}

// --- Data Quality Score ---
function getDataQualityReport(limit = 50) {
  const db = getDb();
  const leads = db.prepare(`
    SELECT id, first_name, last_name, email, phone, firm_name, city, state, website,
      linkedin_url, practice_area, title, bar_number, bar_status, primary_source, lead_score
    FROM leads
    ORDER BY lead_score ASC
    LIMIT ?
  `).all(limit);

  return leads.map(lead => {
    const missing = [];
    const suggestions = [];
    const source = (lead.primary_source || '').toLowerCase();

    if (!lead.email || lead.email === '') {
      missing.push('email');
      if (lead.website) suggestions.push({ field: 'email', action: 'Crawl firm website', method: 'website-crawl', priority: 'high' });
      if (['FL', 'GA', 'CA-YT', 'IE'].includes(lead.state)) suggestions.push({ field: 'email', action: `Check ${lead.state} bar profile`, method: 'bar-profile', priority: 'high' });
      suggestions.push({ field: 'email', action: 'Search Martindale', method: 'martindale-crossref', priority: 'medium' });
    }

    if (!lead.phone || lead.phone === '') {
      missing.push('phone');
      if (['CA', 'OR', 'TX'].includes(lead.state)) suggestions.push({ field: 'phone', action: `Fetch ${lead.state} bar profile page`, method: 'bar-profile', priority: 'high' });
      suggestions.push({ field: 'phone', action: 'Search Martindale', method: 'martindale-crossref', priority: 'medium' });
    }

    if (!lead.website || lead.website === '') {
      missing.push('website');
      if (lead.email) {
        const domain = lead.email.split('@')[1];
        if (domain && !['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'].includes(domain)) {
          suggestions.push({ field: 'website', action: 'Derive from email domain: ' + domain, method: 'email-derive', priority: 'high' });
        }
      }
      suggestions.push({ field: 'website', action: 'Search Martindale', method: 'martindale-crossref', priority: 'medium' });
    }

    if (!lead.linkedin_url || lead.linkedin_url === '') { missing.push('linkedin_url'); }
    if (!lead.firm_name || lead.firm_name === '') { missing.push('firm_name'); }
    if (!lead.practice_area || lead.practice_area === '') { missing.push('practice_area'); }

    const totalFields = 10;
    const filledFields = totalFields - missing.length;
    const qualityScore = Math.round((filledFields / totalFields) * 100);

    return {
      id: lead.id,
      name: `${lead.first_name || ''} ${lead.last_name || ''}`.trim(),
      state: lead.state,
      qualityScore,
      missing,
      suggestions: suggestions.slice(0, 3),
      currentScore: lead.lead_score,
    };
  });
}

function getDataQualitySummary() {
  const db = getDb();
  const total = db.prepare('SELECT COUNT(*) as c FROM leads').get().c;
  const withAll = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email != '' AND email IS NOT NULL AND phone != '' AND phone IS NOT NULL AND website != '' AND website IS NOT NULL").get().c;
  const missingEmail = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NULL OR email = ''").get().c;
  const missingPhone = db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NULL OR phone = ''").get().c;
  const missingWebsite = db.prepare("SELECT COUNT(*) as c FROM leads WHERE website IS NULL OR website = ''").get().c;
  const missingFirm = db.prepare("SELECT COUNT(*) as c FROM leads WHERE firm_name IS NULL OR firm_name = ''").get().c;

  return {
    total,
    complete: withAll,
    completeRate: total > 0 ? Math.round((withAll / total) * 100) : 0,
    missingEmail, missingPhone, missingWebsite, missingFirm,
    emailRate: total > 0 ? Math.round(((total - missingEmail) / total) * 100) : 0,
    phoneRate: total > 0 ? Math.round(((total - missingPhone) / total) * 100) : 0,
    websiteRate: total > 0 ? Math.round(((total - missingWebsite) / total) * 100) : 0,
  };
}

// --- Export Profiles ---
function getExportProfiles() {
  const db = getDb();
  try {
    db.prepare('SELECT 1 FROM export_profiles LIMIT 1').get();
  } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS export_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      filters TEXT NOT NULL DEFAULT '{}',
      columns TEXT NOT NULL DEFAULT '[]',
      format TEXT DEFAULT 'csv',
      last_exported_at TEXT,
      export_count INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    )`).run();
  }
  return db.prepare('SELECT * FROM export_profiles ORDER BY created_at DESC').all();
}

function createExportProfile(name, description, filters, columns) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM export_profiles LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS export_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      filters TEXT NOT NULL DEFAULT '{}',
      columns TEXT NOT NULL DEFAULT '[]',
      format TEXT DEFAULT 'csv',
      last_exported_at TEXT,
      export_count INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    )`).run();
  }

  return db.prepare('INSERT INTO export_profiles (name, description, filters, columns) VALUES (?, ?, ?, ?)').run(
    name, description || '', JSON.stringify(filters || {}), JSON.stringify(columns || [])
  );
}

function deleteExportProfile(id) {
  const db = getDb();
  db.prepare('DELETE FROM export_profiles WHERE id = ?').run(id);
}

function runExportProfile(id) {
  const db = getDb();
  const profile = db.prepare('SELECT * FROM export_profiles WHERE id = ?').get(id);
  if (!profile) throw new Error('Export profile not found');

  const filters = JSON.parse(profile.filters || '{}');
  const columns = JSON.parse(profile.columns || '[]');

  let where = '1=1';
  const params = [];

  if (filters.state) { where += ' AND state = ?'; params.push(filters.state); }
  if (filters.practiceArea) { where += ' AND practice_area LIKE ?'; params.push('%' + filters.practiceArea + '%'); }
  if (filters.minScore) { where += ' AND lead_score >= ?'; params.push(Number(filters.minScore)); }
  if (filters.hasEmail) { where += " AND email IS NOT NULL AND email != ''"; }
  if (filters.hasPhone) { where += " AND phone IS NOT NULL AND phone != ''"; }
  if (filters.pipelineStage) { where += ' AND pipeline_stage = ?'; params.push(filters.pipelineStage); }
  if (filters.tags) { where += ' AND tags LIKE ?'; params.push('%' + filters.tags + '%'); }

  const selectCols = columns.length > 0 ? columns.join(', ') : '*';
  const leads = db.prepare(`SELECT ${selectCols} FROM leads WHERE ${where} ORDER BY lead_score DESC`).all(...params);

  // Update export count
  db.prepare("UPDATE export_profiles SET export_count = export_count + 1, last_exported_at = datetime('now') WHERE id = ?").run(id);

  return leads;
}

// ============================================================
// BATCH 20: Contact Timeline, Warm-Up Scoring, Multi-View, Search
// ============================================================

// --- Contact Timeline ---
function getContactTimeline(leadId, limit = 50) {
  const db = getDb();
  // Ensure contact_log table exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound',
      subject TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`).run();
  }

  return db.prepare(`
    SELECT cl.*, l.first_name, l.last_name, l.email
    FROM contact_log cl
    LEFT JOIN leads l ON cl.lead_id = l.id
    WHERE cl.lead_id = ?
    ORDER BY cl.contact_at DESC
    LIMIT ?
  `).all(leadId, limit);
}

function logContact(leadId, channel, direction, subject, notes, outcome) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound',
      subject TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`).run();
  }

  const validChannels = ['email', 'phone', 'linkedin', 'meeting', 'sms', 'other'];
  if (!validChannels.includes(channel)) throw new Error('Invalid channel. Valid: ' + validChannels.join(', '));

  return db.prepare('INSERT INTO contact_log (lead_id, channel, direction, subject, notes, outcome) VALUES (?, ?, ?, ?, ?, ?)').run(
    leadId, channel, direction || 'outbound', subject || '', notes || '', outcome || ''
  );
}

function getContactStats(leadId) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch { return { total: 0, channels: [], lastContact: null }; }

  const total = db.prepare('SELECT COUNT(*) as c FROM contact_log WHERE lead_id = ?').get(leadId).c;
  const channels = db.prepare('SELECT channel, COUNT(*) as count FROM contact_log WHERE lead_id = ? GROUP BY channel ORDER BY count DESC').all(leadId);
  const last = db.prepare('SELECT contact_at FROM contact_log WHERE lead_id = ? ORDER BY contact_at DESC LIMIT 1').get(leadId);

  return { total, channels, lastContact: last?.contact_at || null };
}

function getRecentContacts(limit = 30) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch { return []; }

  return db.prepare(`
    SELECT cl.*, l.first_name, l.last_name, l.email, l.firm_name
    FROM contact_log cl
    LEFT JOIN leads l ON cl.lead_id = l.id
    ORDER BY cl.contact_at DESC
    LIMIT ?
  `).all(limit);
}

// --- Warm-Up Scoring ---
function computeWarmUpScore(leadId) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch { return { score: 0, recency: 0, frequency: 0 }; }

  // Recency: how recently was last contact (100 for today, decays to 0 at 90 days)
  const last = db.prepare('SELECT contact_at FROM contact_log WHERE lead_id = ? ORDER BY contact_at DESC LIMIT 1').get(leadId);
  let recencyScore = 0;
  if (last) {
    const daysSince = Math.floor((Date.now() - new Date(last.contact_at).getTime()) / 86400000);
    recencyScore = Math.max(0, Math.round(100 - (daysSince * 100 / 90)));
  }

  // Frequency: number of contacts in last 30 days (capped at 50 points for 10+ contacts)
  const count30d = db.prepare("SELECT COUNT(*) as c FROM contact_log WHERE lead_id = ? AND contact_at >= datetime('now', '-30 days')").get(leadId).c;
  const frequencyScore = Math.min(50, count30d * 5);

  const warmUpScore = Math.round((recencyScore * 0.6) + (frequencyScore * 0.4));

  return { score: warmUpScore, recency: recencyScore, frequency: frequencyScore, contactsLast30d: count30d };
}

function batchComputeWarmUp(limit = 100) {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch { return []; }

  // Get leads with contacts
  const leadsWithContacts = db.prepare(`
    SELECT DISTINCT cl.lead_id, l.first_name, l.last_name, l.email, l.state, l.lead_score
    FROM contact_log cl
    LEFT JOIN leads l ON cl.lead_id = l.id
    ORDER BY cl.contact_at DESC
    LIMIT ?
  `).all(limit);

  return leadsWithContacts.map(l => ({
    ...l,
    warmUp: computeWarmUpScore(l.lead_id),
  }));
}

// --- Multi-View Data (Kanban, Cards) ---
function getKanbanData(limit = 200) {
  const db = getDb();
  const stages = ['new', 'contacted', 'qualified', 'proposal', 'negotiation', 'won', 'lost'];
  const result = {};

  for (const stage of stages) {
    result[stage] = db.prepare(`
      SELECT id, first_name, last_name, email, phone, firm_name, city, state, lead_score, pipeline_stage
      FROM leads
      WHERE pipeline_stage = ?
      ORDER BY lead_score DESC
      LIMIT ?
    `).all(stage, limit);
  }

  // Count leads with null/empty pipeline_stage as 'new'
  const unassigned = db.prepare(`
    SELECT id, first_name, last_name, email, phone, firm_name, city, state, lead_score, pipeline_stage
    FROM leads
    WHERE pipeline_stage IS NULL OR pipeline_stage = ''
    ORDER BY lead_score DESC
    LIMIT ?
  `).all(limit);

  result['new'] = [...(result['new'] || []), ...unassigned].slice(0, limit);

  return { stages, data: result };
}

function getCardViewData(options = {}) {
  const db = getDb();
  const { state, sortBy = 'lead_score', limit = 50, offset = 0 } = options;
  const allowedSorts = ['lead_score', 'first_name', 'last_name', 'city', 'created_at'];
  const sort = allowedSorts.includes(sortBy) ? sortBy : 'lead_score';
  const dir = sort === 'lead_score' ? 'DESC' : 'ASC';

  let where = '1=1';
  const params = [];
  if (state) { where += ' AND state = ?'; params.push(state); }

  params.push(limit, offset);
  return db.prepare(`
    SELECT id, first_name, last_name, email, phone, firm_name, city, state, website,
      linkedin_url, practice_area, title, lead_score, pipeline_stage, tags
    FROM leads
    WHERE ${where}
    ORDER BY ${sort} ${dir}
    LIMIT ? OFFSET ?
  `).all(...params);
}

// --- Search with Typeahead ---
function searchTypeahead(query, limit = 10) {
  const db = getDb();
  if (!query || query.length < 2) return [];

  const pattern = '%' + query + '%';
  return db.prepare(`
    SELECT id, first_name, last_name, email, firm_name, city, state, lead_score,
      CASE
        WHEN LOWER(first_name || ' ' || last_name) LIKE LOWER(?) THEN 'name'
        WHEN LOWER(firm_name) LIKE LOWER(?) THEN 'firm'
        WHEN LOWER(email) LIKE LOWER(?) THEN 'email'
        WHEN LOWER(city) LIKE LOWER(?) THEN 'city'
        ELSE 'other'
      END as match_type
    FROM leads
    WHERE LOWER(first_name || ' ' || last_name) LIKE LOWER(?)
      OR LOWER(firm_name) LIKE LOWER(?)
      OR LOWER(email) LIKE LOWER(?)
      OR LOWER(city) LIKE LOWER(?)
    ORDER BY lead_score DESC
    LIMIT ?
  `).all(pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, limit);
}

function getFilterFacets() {
  const db = getDb();
  const states = db.prepare("SELECT state, COUNT(*) as count FROM leads WHERE state IS NOT NULL AND state != '' GROUP BY state ORDER BY count DESC").all();
  const practiceAreas = db.prepare("SELECT practice_area, COUNT(*) as count FROM leads WHERE practice_area IS NOT NULL AND practice_area != '' GROUP BY practice_area ORDER BY count DESC LIMIT 20").all();
  const stages = db.prepare("SELECT COALESCE(pipeline_stage, 'new') as stage, COUNT(*) as count FROM leads GROUP BY COALESCE(pipeline_stage, 'new') ORDER BY count DESC").all();
  const tags = db.prepare("SELECT tags FROM leads WHERE tags IS NOT NULL AND tags != ''").all();

  // Parse comma-separated tags
  const tagCounts = {};
  for (const row of tags) {
    for (const tag of row.tags.split(',').map(t => t.trim()).filter(Boolean)) {
      tagCounts[tag] = (tagCounts[tag] || 0) + 1;
    }
  }
  const tagFacets = Object.entries(tagCounts).map(([tag, count]) => ({ tag, count })).sort((a, b) => b.count - a.count).slice(0, 20);

  const scoreRanges = [
    { label: '80+', min: 80, max: 100 },
    { label: '60-79', min: 60, max: 79 },
    { label: '40-59', min: 40, max: 59 },
    { label: '20-39', min: 20, max: 39 },
    { label: '0-19', min: 0, max: 19 },
  ];
  for (const range of scoreRanges) {
    range.count = db.prepare('SELECT COUNT(*) as c FROM leads WHERE lead_score >= ? AND lead_score <= ?').get(range.min, range.max).c;
  }

  return { states, practiceAreas, stages, tags: tagFacets, scoreRanges };
}

// ============================================================
// BATCH 21: Enrichment Queue, Firm Intelligence, Dedup Queue, Audit Log
// ============================================================

// --- Enrichment Queue ---
function _ensureEnrichmentQueue() {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM enrichment_queue LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS enrichment_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id INTEGER NOT NULL,
      priority INTEGER DEFAULT 50,
      status TEXT DEFAULT 'pending',
      source TEXT DEFAULT '',
      fields_requested TEXT DEFAULT '[]',
      fields_filled TEXT DEFAULT '[]',
      error TEXT DEFAULT '',
      attempts INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      started_at TEXT,
      completed_at TEXT,
      FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`).run();
  }
}

function getEnrichmentQueueStatus() {
  const db = getDb();
  _ensureEnrichmentQueue();
  const pending = db.prepare("SELECT COUNT(*) as c FROM enrichment_queue WHERE status = 'pending'").get().c;
  const processing = db.prepare("SELECT COUNT(*) as c FROM enrichment_queue WHERE status = 'processing'").get().c;
  const completed = db.prepare("SELECT COUNT(*) as c FROM enrichment_queue WHERE status = 'completed'").get().c;
  const failed = db.prepare("SELECT COUNT(*) as c FROM enrichment_queue WHERE status = 'failed'").get().c;
  const recent = db.prepare("SELECT eq.*, l.first_name, l.last_name, l.state FROM enrichment_queue eq LEFT JOIN leads l ON eq.lead_id = l.id ORDER BY eq.created_at DESC LIMIT 20").all();
  return { pending, processing, completed, failed, total: pending + processing + completed + failed, recent };
}

function addToEnrichmentQueue(leadIds, source = 'manual', fieldsRequested = []) {
  const db = getDb();
  _ensureEnrichmentQueue();
  const stmt = db.prepare('INSERT OR IGNORE INTO enrichment_queue (lead_id, priority, source, fields_requested) VALUES (?, ?, ?, ?)');
  const run = db.transaction((ids) => {
    let added = 0;
    for (const id of ids) {
      const lead = db.prepare('SELECT lead_score FROM leads WHERE id = ?').get(id);
      const priority = lead ? lead.lead_score : 50;
      try { stmt.run(id, priority, source, JSON.stringify(fieldsRequested)); added++; } catch {}
    }
    return added;
  });
  return { added: run(leadIds) };
}

function processEnrichmentQueue(batchSize = 10) {
  const db = getDb();
  _ensureEnrichmentQueue();
  const items = db.prepare("SELECT eq.*, l.* FROM enrichment_queue eq LEFT JOIN leads l ON eq.lead_id = l.id WHERE eq.status = 'pending' ORDER BY eq.priority DESC LIMIT ?").all(batchSize);
  let processed = 0, enriched = 0;

  for (const item of items) {
    db.prepare("UPDATE enrichment_queue SET status = 'processing', started_at = datetime('now') WHERE id = ?").run(item.id);
    const filled = [];
    // Check what's missing and could be filled
    if (!item.email && item.website) filled.push('email_crawl_candidate');
    if (!item.phone) filled.push('phone_missing');
    if (!item.website && item.email) {
      const domain = item.email.split('@')[1];
      if (domain && !['gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com'].includes(domain)) {
        db.prepare("UPDATE leads SET website = ?, updated_at = datetime('now') WHERE id = ? AND (website IS NULL OR website = '')").run('https://' + domain, item.lead_id);
        filled.push('website');
      }
    }
    db.prepare("UPDATE enrichment_queue SET status = 'completed', completed_at = datetime('now'), fields_filled = ?, attempts = attempts + 1 WHERE id = ?").run(JSON.stringify(filled), item.id);
    processed++;
    if (filled.length > 0) enriched++;
  }
  return { processed, enriched, remaining: db.prepare("SELECT COUNT(*) as c FROM enrichment_queue WHERE status = 'pending'").get().c };
}

function clearEnrichmentQueue(status = 'completed') {
  const db = getDb();
  _ensureEnrichmentQueue();
  const result = db.prepare('DELETE FROM enrichment_queue WHERE status = ?').run(status);
  return { deleted: result.changes };
}

// --- Firm Intelligence ---
function getFirmIntelligence(limit = 50) {
  const db = getDb();
  return db.prepare(`
    SELECT firm_name,
      COUNT(*) as headcount,
      COUNT(DISTINCT state) as states_present,
      COUNT(DISTINCT city) as cities_present,
      GROUP_CONCAT(DISTINCT state) as states,
      GROUP_CONCAT(DISTINCT city) as top_cities,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      MAX(lead_score) as top_score,
      GROUP_CONCAT(DISTINCT practice_area) as practice_areas
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != ''
    GROUP BY LOWER(TRIM(firm_name))
    HAVING headcount >= 2
    ORDER BY headcount DESC
    LIMIT ?
  `).all(limit);
}

function getFirmDetail(firmName) {
  const db = getDb();
  const leads = db.prepare('SELECT * FROM leads WHERE LOWER(TRIM(firm_name)) = LOWER(TRIM(?)) ORDER BY lead_score DESC').all(firmName);
  const practiceBreakdown = db.prepare(`
    SELECT practice_area, COUNT(*) as count
    FROM leads WHERE LOWER(TRIM(firm_name)) = LOWER(TRIM(?)) AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area ORDER BY count DESC
  `).all(firmName);
  const stateBreakdown = db.prepare(`
    SELECT state, COUNT(*) as count
    FROM leads WHERE LOWER(TRIM(firm_name)) = LOWER(TRIM(?)) AND state IS NOT NULL
    GROUP BY state ORDER BY count DESC
  `).all(firmName);

  return {
    firmName,
    headcount: leads.length,
    leads: leads.slice(0, 50),
    practiceBreakdown,
    stateBreakdown,
    emailRate: leads.length > 0 ? Math.round((leads.filter(l => l.email).length / leads.length) * 100) : 0,
    avgScore: leads.length > 0 ? Math.round(leads.reduce((s, l) => s + (l.lead_score || 0), 0) / leads.length) : 0,
  };
}

// --- Dedup Merge Queue ---
function _ensureDedupQueue() {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM dedup_queue LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS dedup_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lead_id_a INTEGER NOT NULL,
      lead_id_b INTEGER NOT NULL,
      match_type TEXT NOT NULL,
      confidence INTEGER DEFAULT 0,
      status TEXT DEFAULT 'pending',
      resolution TEXT DEFAULT '',
      created_at TEXT DEFAULT (datetime('now')),
      resolved_at TEXT
    )`).run();
  }
}

function scanForDuplicates(limit = 200) {
  const db = getDb();
  _ensureDedupQueue();
  let found = 0;

  // Email matches (confidence: 95)
  const emailDupes = db.prepare(`
    SELECT a.id as id_a, b.id as id_b
    FROM leads a JOIN leads b ON LOWER(a.email) = LOWER(b.email) AND a.id < b.id
    WHERE a.email IS NOT NULL AND a.email != ''
    LIMIT ?
  `).all(limit);
  for (const d of emailDupes) {
    try { db.prepare("INSERT INTO dedup_queue (lead_id_a, lead_id_b, match_type, confidence) VALUES (?, ?, 'email', 95)").run(d.id_a, d.id_b); found++; } catch {}
  }

  // Name+City matches (confidence: 75)
  const nameDupes = db.prepare(`
    SELECT a.id as id_a, b.id as id_b
    FROM leads a JOIN leads b ON LOWER(a.first_name) = LOWER(b.first_name) AND LOWER(a.last_name) = LOWER(b.last_name) AND LOWER(a.city) = LOWER(b.city) AND a.id < b.id
    WHERE a.first_name IS NOT NULL AND a.last_name IS NOT NULL AND a.city IS NOT NULL AND a.city != ''
    LIMIT ?
  `).all(limit);
  for (const d of nameDupes) {
    try { db.prepare("INSERT INTO dedup_queue (lead_id_a, lead_id_b, match_type, confidence) VALUES (?, ?, 'name_city', 75)").run(d.id_a, d.id_b); found++; } catch {}
  }

  // Phone matches (confidence: 90)
  const phoneDupes = db.prepare(`
    SELECT a.id as id_a, b.id as id_b
    FROM leads a JOIN leads b ON a.phone = b.phone AND a.id < b.id
    WHERE a.phone IS NOT NULL AND a.phone != '' AND LENGTH(a.phone) >= 7
    LIMIT ?
  `).all(limit);
  for (const d of phoneDupes) {
    try { db.prepare("INSERT INTO dedup_queue (lead_id_a, lead_id_b, match_type, confidence) VALUES (?, ?, 'phone', 90)").run(d.id_a, d.id_b); found++; } catch {}
  }

  return { found, total: db.prepare("SELECT COUNT(*) as c FROM dedup_queue WHERE status = 'pending'").get().c };
}

function getDedupQueue(limit = 50) {
  const db = getDb();
  _ensureDedupQueue();
  return db.prepare(`
    SELECT dq.*,
      a.first_name as a_first, a.last_name as a_last, a.email as a_email, a.phone as a_phone, a.firm_name as a_firm, a.city as a_city, a.state as a_state, a.lead_score as a_score,
      b.first_name as b_first, b.last_name as b_last, b.email as b_email, b.phone as b_phone, b.firm_name as b_firm, b.city as b_city, b.state as b_state, b.lead_score as b_score
    FROM dedup_queue dq
    LEFT JOIN leads a ON dq.lead_id_a = a.id
    LEFT JOIN leads b ON dq.lead_id_b = b.id
    WHERE dq.status = 'pending'
    ORDER BY dq.confidence DESC
    LIMIT ?
  `).all(limit);
}

function resolveDedupItem(id, resolution, keepId) {
  const db = getDb();
  _ensureDedupQueue();
  const item = db.prepare('SELECT * FROM dedup_queue WHERE id = ?').get(id);
  if (!item) throw new Error('Dedup item not found');

  if (resolution === 'merge' && keepId) {
    const deleteId = keepId === item.lead_id_a ? item.lead_id_b : item.lead_id_a;
    const keeper = db.prepare('SELECT * FROM leads WHERE id = ?').get(keepId);
    const donor = db.prepare('SELECT * FROM leads WHERE id = ?').get(deleteId);
    if (keeper && donor) {
      const updates = [];
      const vals = [];
      for (const field of ['email', 'phone', 'website', 'firm_name', 'linkedin_url', 'practice_area', 'title', 'bio']) {
        if ((!keeper[field] || keeper[field] === '') && donor[field] && donor[field] !== '') {
          updates.push(`${field} = ?`);
          vals.push(donor[field]);
        }
      }
      if (updates.length > 0) {
        db.prepare(`UPDATE leads SET ${updates.join(', ')}, updated_at = datetime('now') WHERE id = ?`).run(...vals, keepId);
      }
      db.prepare('DELETE FROM leads WHERE id = ?').run(deleteId);
    }
  }

  db.prepare("UPDATE dedup_queue SET status = 'resolved', resolution = ?, resolved_at = datetime('now') WHERE id = ?").run(resolution, id);
  return { success: true };
}

function getDedupStats() {
  const db = getDb();
  _ensureDedupQueue();
  return {
    pending: db.prepare("SELECT COUNT(*) as c FROM dedup_queue WHERE status = 'pending'").get().c,
    resolved: db.prepare("SELECT COUNT(*) as c FROM dedup_queue WHERE status = 'resolved'").get().c,
    byType: db.prepare("SELECT match_type, COUNT(*) as count, ROUND(AVG(confidence)) as avg_confidence FROM dedup_queue WHERE status = 'pending' GROUP BY match_type ORDER BY avg_confidence DESC").all(),
  };
}

// --- Activity Audit Log ---
function _ensureAuditLog() {
  const db = getDb();
  try { db.prepare('SELECT 1 FROM audit_log LIMIT 1').get(); } catch {
    db.prepare(`CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action TEXT NOT NULL,
      entity_type TEXT DEFAULT 'lead',
      entity_id INTEGER,
      details TEXT DEFAULT '{}',
      user_name TEXT DEFAULT 'system',
      ip_address TEXT DEFAULT '',
      created_at TEXT DEFAULT (datetime('now'))
    )`).run();
  }
}

function logAuditEvent(action, entityType, entityId, details = {}, userName = 'system') {
  const db = getDb();
  _ensureAuditLog();
  return db.prepare('INSERT INTO audit_log (action, entity_type, entity_id, details, user_name) VALUES (?, ?, ?, ?, ?)').run(
    action, entityType, entityId, JSON.stringify(details), userName
  );
}

function getAuditLog(options = {}) {
  const db = getDb();
  _ensureAuditLog();
  const { action, entityType, entityId, limit = 100, offset = 0 } = options;

  let where = '1=1';
  const params = [];
  if (action) { where += ' AND action = ?'; params.push(action); }
  if (entityType) { where += ' AND entity_type = ?'; params.push(entityType); }
  if (entityId) { where += ' AND entity_id = ?'; params.push(entityId); }

  params.push(limit, offset);
  return db.prepare(`SELECT * FROM audit_log WHERE ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params);
}

function getAuditStats() {
  const db = getDb();
  _ensureAuditLog();
  const total = db.prepare('SELECT COUNT(*) as c FROM audit_log').get().c;
  const today = db.prepare("SELECT COUNT(*) as c FROM audit_log WHERE DATE(created_at) = DATE('now')").get().c;
  const byAction = db.prepare('SELECT action, COUNT(*) as count FROM audit_log GROUP BY action ORDER BY count DESC LIMIT 15').all();
  const byUser = db.prepare('SELECT user_name, COUNT(*) as count FROM audit_log GROUP BY user_name ORDER BY count DESC LIMIT 10').all();
  const recentHours = db.prepare(`
    SELECT strftime('%H', created_at) as hour, COUNT(*) as count
    FROM audit_log WHERE created_at >= datetime('now', '-24 hours')
    GROUP BY hour ORDER BY hour
  `).all();
  return { total, today, byAction, byUser, recentHours };
}

function exportAuditLog(options = {}) {
  const db = getDb();
  _ensureAuditLog();
  const { startDate, endDate, action, limit = 5000 } = options;
  let where = '1=1';
  const params = [];
  if (startDate) { where += ' AND created_at >= ?'; params.push(startDate); }
  if (endDate) { where += ' AND created_at <= ?'; params.push(endDate); }
  if (action) { where += ' AND action = ?'; params.push(action); }
  params.push(limit);
  return db.prepare(`SELECT * FROM audit_log WHERE ${where} ORDER BY created_at DESC LIMIT ?`).all(...params);
}

// BATCH 22: Lifecycle Tracking, Sequence Analytics, Activity Scoring, Bulk Enrichment

// --- Lifecycle Tracking ---
function _ensureStageTransitions() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS stage_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    from_stage TEXT DEFAULT '',
    to_stage TEXT NOT NULL,
    transitioned_at TEXT DEFAULT (datetime('now')),
    duration_hours REAL DEFAULT 0,
    triggered_by TEXT DEFAULT 'system'
  )`);
}

function recordStageTransition(leadId, fromStage, toStage, triggeredBy = 'system') {
  const db = getDb();
  _ensureStageTransitions();
  // Calculate duration since last transition
  const last = db.prepare(`SELECT transitioned_at FROM stage_transitions WHERE lead_id = ? ORDER BY id DESC LIMIT 1`).get(leadId);
  let durationHours = 0;
  if (last) {
    const diff = Date.now() - new Date(last.transitioned_at).getTime();
    durationHours = Math.round((diff / 3600000) * 100) / 100;
  }
  return db.prepare(`INSERT INTO stage_transitions (lead_id, from_stage, to_stage, duration_hours, triggered_by) VALUES (?, ?, ?, ?, ?)`).run(leadId, fromStage || '', toStage, durationHours, triggeredBy);
}

function getLifecycleAnalytics() {
  const db = getDb();
  _ensureStageTransitions();
  // Avg duration per stage
  const avgDuration = db.prepare(`
    SELECT to_stage as stage, ROUND(AVG(duration_hours), 1) as avg_hours, COUNT(*) as transitions,
    ROUND(MIN(duration_hours), 1) as min_hours, ROUND(MAX(duration_hours), 1) as max_hours
    FROM stage_transitions WHERE duration_hours > 0
    GROUP BY to_stage ORDER BY avg_hours DESC
  `).all();
  // Conversion rates between stages
  const conversions = db.prepare(`
    SELECT from_stage, to_stage, COUNT(*) as count
    FROM stage_transitions WHERE from_stage != ''
    GROUP BY from_stage, to_stage ORDER BY count DESC LIMIT 20
  `).all();
  // Bottlenecks: stages with highest avg duration
  const bottlenecks = avgDuration.filter(s => s.avg_hours > 0).sort((a, b) => b.avg_hours - a.avg_hours).slice(0, 5);
  // Velocity: avg total time from first to last stage per lead
  const velocity = db.prepare(`
    SELECT ROUND(AVG(total_hours), 1) as avg_total_hours, COUNT(*) as leads_completed
    FROM (SELECT lead_id, ROUND((julianday(MAX(transitioned_at)) - julianday(MIN(transitioned_at))) * 24, 1) as total_hours
    FROM stage_transitions GROUP BY lead_id HAVING COUNT(*) >= 2)
  `).get();
  // Recent transitions
  const recent = db.prepare(`
    SELECT st.*, l.first_name, l.last_name, l.firm_name
    FROM stage_transitions st LEFT JOIN leads l ON st.lead_id = l.id
    ORDER BY st.id DESC LIMIT 20
  `).all();
  return { avgDuration, conversions, bottlenecks, velocity: velocity || { avg_total_hours: 0, leads_completed: 0 }, recent };
}

function getLeadLifecycle(leadId) {
  const db = getDb();
  _ensureStageTransitions();
  return db.prepare(`SELECT * FROM stage_transitions WHERE lead_id = ? ORDER BY id ASC`).all(leadId);
}

// --- Sequence Performance Analytics ---
function _ensureSequenceEvents() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS sequence_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sequence_id INTEGER NOT NULL,
    step_number INTEGER DEFAULT 1,
    lead_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    variant TEXT DEFAULT 'A',
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now'))
  )`);
}

function recordSequenceEvent(sequenceId, stepNumber, leadId, eventType, variant = 'A', metadata = {}) {
  const db = getDb();
  _ensureSequenceEvents();
  return db.prepare(`INSERT INTO sequence_events (sequence_id, step_number, lead_id, event_type, variant, metadata) VALUES (?, ?, ?, ?, ?, ?)`).run(sequenceId, stepNumber, leadId, eventType, variant, JSON.stringify(metadata));
}

function getSequenceAnalytics(sequenceId) {
  const db = getDb();
  _ensureSequenceEvents();
  // Per-step metrics
  const steps = db.prepare(`
    SELECT step_number,
      SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) as sent,
      SUM(CASE WHEN event_type = 'opened' THEN 1 ELSE 0 END) as opened,
      SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) as replied,
      SUM(CASE WHEN event_type = 'bounced' THEN 1 ELSE 0 END) as bounced,
      SUM(CASE WHEN event_type = 'clicked' THEN 1 ELSE 0 END) as clicked,
      SUM(CASE WHEN event_type = 'unsubscribed' THEN 1 ELSE 0 END) as unsubscribed
    FROM sequence_events WHERE sequence_id = ?
    GROUP BY step_number ORDER BY step_number
  `).all(sequenceId);
  // Calculate rates
  const enriched = steps.map(s => ({
    ...s,
    open_rate: s.sent > 0 ? Math.round((s.opened / s.sent) * 100) : 0,
    reply_rate: s.sent > 0 ? Math.round((s.replied / s.sent) * 100) : 0,
    bounce_rate: s.sent > 0 ? Math.round((s.bounced / s.sent) * 100) : 0,
  }));
  // A/B variant comparison
  const variants = db.prepare(`
    SELECT variant, event_type, COUNT(*) as count
    FROM sequence_events WHERE sequence_id = ?
    GROUP BY variant, event_type
  `).all(sequenceId);
  // Overall totals
  const totals = db.prepare(`
    SELECT
      SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) as total_sent,
      SUM(CASE WHEN event_type = 'opened' THEN 1 ELSE 0 END) as total_opened,
      SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) as total_replied,
      SUM(CASE WHEN event_type = 'bounced' THEN 1 ELSE 0 END) as total_bounced,
      COUNT(DISTINCT lead_id) as unique_leads
    FROM sequence_events WHERE sequence_id = ?
  `).get(sequenceId);
  return { steps: enriched, variants, totals: totals || {} };
}

function getAllSequencePerformance() {
  const db = getDb();
  _ensureSequenceEvents();
  try {
    const seqs = db.prepare(`SELECT id, name FROM sequences`).all();
    return seqs.map(seq => {
      const stats = db.prepare(`
        SELECT
          SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) as sent,
          SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) as replied,
          SUM(CASE WHEN event_type = 'bounced' THEN 1 ELSE 0 END) as bounced,
          COUNT(DISTINCT lead_id) as leads
        FROM sequence_events WHERE sequence_id = ?
      `).get(seq.id);
      return { ...seq, ...stats, reply_rate: stats.sent > 0 ? Math.round((stats.replied / stats.sent) * 100) : 0 };
    });
  } catch { return []; }
}

// --- Activity Scoring (Recency-Weighted) ---
function computeActivityScore(leadId, config = {}) {
  const db = getDb();
  const { decayDays = 90, weights = { email_open: 5, email_reply: 20, call: 15, meeting: 25, linkedin: 10, note: 3 } } = config;
  // Gather all activities for this lead
  let activities = [];
  try {
    activities = db.prepare(`SELECT activity_type, created_at FROM lead_activities WHERE lead_id = ? ORDER BY created_at DESC`).all(leadId);
  } catch { /* table may not exist */ }
  // Add contacts
  try {
    const contacts = db.prepare(`SELECT channel as activity_type, created_at FROM contact_log WHERE lead_id = ? ORDER BY created_at DESC`).all(leadId);
    activities = activities.concat(contacts);
  } catch { /* table may not exist */ }
  if (activities.length === 0) return { score: 0, totalActivities: 0, recentActivities: 0, breakdown: {} };
  const now = Date.now();
  const decayMs = decayDays * 86400000;
  let score = 0;
  let recentCount = 0;
  const breakdown = {};
  for (const act of activities) {
    const age = now - new Date(act.created_at).getTime();
    const recencyMultiplier = Math.max(0, 1 - (age / decayMs));
    const weight = weights[act.activity_type] || 5;
    const actScore = Math.round(weight * recencyMultiplier * 100) / 100;
    score += actScore;
    if (age < 7 * 86400000) recentCount++;
    breakdown[act.activity_type] = (breakdown[act.activity_type] || 0) + 1;
  }
  return { score: Math.round(score * 10) / 10, totalActivities: activities.length, recentActivities: recentCount, breakdown };
}

function batchActivityScores(limit = 100) {
  const db = getDb();
  // Get leads with most recent activities
  let leadIds = [];
  try {
    leadIds = db.prepare(`SELECT DISTINCT lead_id FROM lead_activities ORDER BY created_at DESC LIMIT ?`).all(limit).map(r => r.lead_id);
  } catch { /* table may not exist */ }
  try {
    const contactIds = db.prepare(`SELECT DISTINCT lead_id FROM contact_log ORDER BY created_at DESC LIMIT ?`).all(limit).map(r => r.lead_id);
    leadIds = [...new Set([...leadIds, ...contactIds])];
  } catch { /* table may not exist */ }
  const results = leadIds.slice(0, limit).map(id => {
    const lead = db.prepare(`SELECT id, first_name, last_name, email, state FROM leads WHERE id = ?`).get(id);
    if (!lead) return null;
    const score = computeActivityScore(id);
    return { ...lead, ...score };
  }).filter(Boolean).sort((a, b) => b.score - a.score);
  return results;
}

function getActivityScoreConfig() {
  const db = getDb();
  try {
    const row = db.prepare(`SELECT value FROM settings WHERE key = 'activity_score_config'`).get();
    return row ? JSON.parse(row.value) : { decayDays: 90, weights: { email_open: 5, email_reply: 20, call: 15, meeting: 25, linkedin: 10, note: 3 } };
  } catch { return { decayDays: 90, weights: { email_open: 5, email_reply: 20, call: 15, meeting: 25, linkedin: 10, note: 3 } }; }
}

function updateActivityScoreConfig(config) {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)`);
  db.prepare(`INSERT OR REPLACE INTO settings (key, value) VALUES ('activity_score_config', ?)`).run(JSON.stringify(config));
  return { updated: true };
}

// --- Bulk Enrichment ---
function _ensureBulkEnrichmentRuns() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS bulk_enrichment_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_leads INTEGER DEFAULT 0,
    processed INTEGER DEFAULT 0,
    enriched INTEGER DEFAULT 0,
    fields_filled TEXT DEFAULT '{}',
    status TEXT DEFAULT 'pending',
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    source_filter TEXT DEFAULT ''
  )`);
}

function createBulkEnrichmentRun(leadIds, sourceFilter = '') {
  const db = getDb();
  _ensureBulkEnrichmentRuns();
  const result = db.prepare(`INSERT INTO bulk_enrichment_runs (total_leads, source_filter) VALUES (?, ?)`).run(leadIds.length, sourceFilter);
  const runId = result.lastInsertRowid;
  // Queue leads for enrichment
  _ensureEnrichmentQueue();
  const insert = db.prepare(`INSERT OR IGNORE INTO enrichment_queue (lead_id, source, priority, fields_requested) VALUES (?, ?, ?, ?)`);
  let added = 0;
  for (const id of leadIds) {
    const lead = db.prepare(`SELECT lead_score FROM leads WHERE id = ?`).get(id);
    try {
      insert.run(id, 'bulk-run-' + runId, lead?.lead_score || 0, 'email,phone,website');
      added++;
    } catch { /* dupe */ }
  }
  return { runId, queued: added };
}

function getBulkEnrichmentRuns(limit = 20) {
  const db = getDb();
  _ensureBulkEnrichmentRuns();
  return db.prepare(`SELECT * FROM bulk_enrichment_runs ORDER BY id DESC LIMIT ?`).all(limit);
}

function processBulkEnrichmentBatch(runId, batchSize = 20) {
  const db = getDb();
  _ensureBulkEnrichmentRuns();
  _ensureEnrichmentQueue();
  const run = db.prepare(`SELECT * FROM bulk_enrichment_runs WHERE id = ?`).get(runId);
  if (!run) return { error: 'Run not found' };
  if (run.status === 'completed') return { error: 'Run already completed' };
  // Update status
  if (run.status === 'pending') db.prepare(`UPDATE bulk_enrichment_runs SET status = 'processing' WHERE id = ?`).run(runId);
  // Process pending items from this run's queue
  const source = 'bulk-run-' + runId;
  const items = db.prepare(`SELECT eq.*, l.email, l.phone, l.website, l.firm_name FROM enrichment_queue eq JOIN leads l ON eq.lead_id = l.id WHERE eq.source = ? AND eq.status = 'pending' LIMIT ?`).all(source, batchSize);
  let enriched = 0;
  const fieldsFilled = {};
  for (const item of items) {
    db.prepare(`UPDATE enrichment_queue SET status = 'processing' WHERE id = ?`).run(item.id);
    let filled = 0;
    // Derive website from email
    if (!item.website && item.email && item.email.includes('@')) {
      const domain = item.email.split('@')[1];
      if (domain && !['gmail.com', 'yahoo.com', 'hotmail.com', 'aol.com', 'outlook.com'].includes(domain.toLowerCase())) {
        db.prepare(`UPDATE leads SET website = ? WHERE id = ? AND (website IS NULL OR website = '')`).run('https://' + domain, item.lead_id);
        filled++;
        fieldsFilled.website = (fieldsFilled.website || 0) + 1;
      }
    }
    // Share firm data
    if (item.firm_name && (!item.phone || !item.email || !item.website)) {
      const firmLead = db.prepare(`SELECT phone, email, website FROM leads WHERE firm_name = ? AND id != ? AND (phone != '' OR email != '' OR website != '') LIMIT 1`).get(item.firm_name, item.lead_id);
      if (firmLead) {
        if (!item.phone && firmLead.phone) { db.prepare(`UPDATE leads SET phone = ? WHERE id = ? AND (phone IS NULL OR phone = '')`).run(firmLead.phone, item.lead_id); filled++; fieldsFilled.phone = (fieldsFilled.phone || 0) + 1; }
        if (!item.email && firmLead.email) { db.prepare(`UPDATE leads SET email = ? WHERE id = ? AND (email IS NULL OR email = '')`).run(firmLead.email, item.lead_id); filled++; fieldsFilled.email = (fieldsFilled.email || 0) + 1; }
        if (!item.website && firmLead.website) { db.prepare(`UPDATE leads SET website = ? WHERE id = ? AND (website IS NULL OR website = '')`).run(firmLead.website, item.lead_id); filled++; fieldsFilled.website = (fieldsFilled.website || 0) + 1; }
      }
    }
    if (filled > 0) enriched++;
    db.prepare(`UPDATE enrichment_queue SET status = 'completed' WHERE id = ?`).run(item.id);
  }
  const processed = run.processed + items.length;
  const totalEnriched = run.enriched + enriched;
  const existingFields = JSON.parse(run.fields_filled || '{}');
  for (const [k, v] of Object.entries(fieldsFilled)) existingFields[k] = (existingFields[k] || 0) + v;
  const isComplete = processed >= run.total_leads;
  db.prepare(`UPDATE bulk_enrichment_runs SET processed = ?, enriched = ?, fields_filled = ?, status = ?${isComplete ? ", completed_at = datetime('now')" : ''} WHERE id = ?`).run(processed, totalEnriched, JSON.stringify(existingFields), isComplete ? 'completed' : 'processing', runId);
  return { processed: items.length, enriched, totalProcessed: processed, totalEnriched, fieldsFilled: existingFields, complete: isComplete };
}

function getBulkEnrichmentDiff(runId) {
  const db = getDb();
  _ensureBulkEnrichmentRuns();
  _ensureEnrichmentQueue();
  const source = 'bulk-run-' + runId;
  const items = db.prepare(`
    SELECT eq.lead_id, l.first_name, l.last_name, l.email, l.phone, l.website, l.firm_name, eq.status
    FROM enrichment_queue eq JOIN leads l ON eq.lead_id = l.id
    WHERE eq.source = ? ORDER BY eq.id
  `).all(source);
  return items;
}

// BATCH 23: Relationship Graph, Data Freshness, Scoring Comparison, Geographic Clustering

// --- Lead Relationship Graph ---
function _ensureRelationships() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS lead_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id_a INTEGER NOT NULL,
    lead_id_b INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,
    strength REAL DEFAULT 0.5,
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(lead_id_a, lead_id_b, relationship_type)
  )`);
}

function buildRelationshipGraph(leadId) {
  const db = getDb();
  _ensureRelationships();
  const lead = db.prepare(`SELECT * FROM leads WHERE id = ?`).get(leadId);
  if (!lead) return { lead: null, connections: [] };
  // Auto-detect relationships
  const connections = [];
  // Same firm
  if (lead.firm_name) {
    const firmPeers = db.prepare(`SELECT id, first_name, last_name, email, city, state, lead_score FROM leads WHERE firm_name = ? AND id != ? LIMIT 20`).all(lead.firm_name, leadId);
    firmPeers.forEach(p => connections.push({ ...p, relationship: 'same_firm', strength: 0.8 }));
  }
  // Same city + practice area
  if (lead.city && lead.practice_area) {
    const cityPeers = db.prepare(`SELECT id, first_name, last_name, firm_name, email, lead_score FROM leads WHERE city = ? AND practice_area = ? AND id != ? AND (firm_name IS NULL OR firm_name != ?) LIMIT 10`).all(lead.city, lead.practice_area, leadId, lead.firm_name || '');
    cityPeers.forEach(p => connections.push({ ...p, relationship: 'same_city_practice', strength: 0.5 }));
  }
  // Stored relationships
  const stored = db.prepare(`
    SELECT lr.*,
      CASE WHEN lr.lead_id_a = ? THEN lr.lead_id_b ELSE lr.lead_id_a END as other_id
    FROM lead_relationships lr
    WHERE lr.lead_id_a = ? OR lr.lead_id_b = ?
  `).all(leadId, leadId, leadId);
  for (const rel of stored) {
    const other = db.prepare(`SELECT id, first_name, last_name, firm_name, email, city, state, lead_score FROM leads WHERE id = ?`).get(rel.other_id);
    if (other && !connections.find(c => c.id === other.id)) {
      connections.push({ ...other, relationship: rel.relationship_type, strength: rel.strength });
    }
  }
  return { lead: { id: lead.id, first_name: lead.first_name, last_name: lead.last_name, firm_name: lead.firm_name, city: lead.city, state: lead.state }, connections };
}

function addRelationship(leadIdA, leadIdB, type, strength = 0.5, metadata = {}) {
  const db = getDb();
  _ensureRelationships();
  const a = Math.min(leadIdA, leadIdB);
  const b = Math.max(leadIdA, leadIdB);
  return db.prepare(`INSERT OR REPLACE INTO lead_relationships (lead_id_a, lead_id_b, relationship_type, strength, metadata) VALUES (?, ?, ?, ?, ?)`).run(a, b, type, strength, JSON.stringify(metadata));
}

function getFirmNetwork(limit = 30) {
  const db = getDb();
  const firms = db.prepare(`
    SELECT firm_name, COUNT(*) as headcount,
      COUNT(DISTINCT city) as cities, COUNT(DISTINCT state) as states,
      GROUP_CONCAT(DISTINCT state) as state_list,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE firm_name IS NOT NULL AND firm_name != ''
    GROUP BY firm_name HAVING headcount >= 2 ORDER BY headcount DESC LIMIT ?
  `).all(limit);
  return firms;
}

// --- Data Freshness Monitoring ---
function _ensureFreshnessLog() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS freshness_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    field_name TEXT NOT NULL,
    verified_at TEXT DEFAULT (datetime('now')),
    source TEXT DEFAULT 'scrape',
    UNIQUE(lead_id, field_name)
  )`);
}

function recordFieldVerification(leadId, fieldName, source = 'scrape') {
  const db = getDb();
  _ensureFreshnessLog();
  return db.prepare(`INSERT OR REPLACE INTO freshness_log (lead_id, field_name, verified_at, source) VALUES (?, ?, datetime('now'), ?)`).run(leadId, fieldName, source);
}

function getFreshnessReport(options = {}) {
  const db = getDb();
  _ensureFreshnessLog();
  const { staleDays = 90, limit = 100 } = options;
  // Stale leads: updated_at > staleDays ago
  const staleLeads = db.prepare(`
    SELECT id, first_name, last_name, email, state, primary_source, updated_at,
      ROUND(julianday('now') - julianday(updated_at)) as days_stale
    FROM leads WHERE updated_at < datetime('now', '-' || ? || ' days')
    ORDER BY updated_at ASC LIMIT ?
  `).all(staleDays, limit);
  // Freshness by state
  const byState = db.prepare(`
    SELECT state, COUNT(*) as total,
      SUM(CASE WHEN updated_at >= datetime('now', '-30 days') THEN 1 ELSE 0 END) as fresh_30d,
      SUM(CASE WHEN updated_at >= datetime('now', '-90 days') THEN 1 ELSE 0 END) as fresh_90d,
      SUM(CASE WHEN updated_at < datetime('now', '-90 days') THEN 1 ELSE 0 END) as stale_90d,
      ROUND(AVG(julianday('now') - julianday(updated_at)), 1) as avg_age_days
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY stale_90d DESC
  `).all();
  // Re-scrape suggestions
  const rescrape = byState.filter(s => s.stale_90d > s.total * 0.3).map(s => ({
    state: s.state, staleCount: s.stale_90d, totalCount: s.total,
    stalePct: Math.round((s.stale_90d / s.total) * 100), avgAgeDays: s.avg_age_days,
  }));
  // Overall stats
  const overall = db.prepare(`
    SELECT COUNT(*) as total,
      SUM(CASE WHEN updated_at >= datetime('now', '-30 days') THEN 1 ELSE 0 END) as fresh_30d,
      SUM(CASE WHEN updated_at < datetime('now', '-90 days') THEN 1 ELSE 0 END) as stale_90d,
      ROUND(AVG(julianday('now') - julianday(updated_at)), 1) as avg_age_days
    FROM leads
  `).get();
  return { overall: overall || {}, staleLeads, byState, rescrape };
}

function getLeadFreshness(leadId) {
  const db = getDb();
  _ensureFreshnessLog();
  const lead = db.prepare(`SELECT updated_at, created_at FROM leads WHERE id = ?`).get(leadId);
  if (!lead) return null;
  const fieldChecks = db.prepare(`SELECT field_name, verified_at, source FROM freshness_log WHERE lead_id = ? ORDER BY field_name`).all(leadId);
  const ageDays = Math.round((Date.now() - new Date(lead.updated_at).getTime()) / 86400000);
  return { leadId, updatedAt: lead.updated_at, createdAt: lead.created_at, ageDays, fieldChecks };
}

// --- Scoring Model Comparison ---
function compareScoringModels(modelIdA, modelIdB) {
  const db = getDb();
  try {
    const modelA = db.prepare(`SELECT * FROM scoring_models WHERE id = ?`).get(modelIdA);
    const modelB = db.prepare(`SELECT * FROM scoring_models WHERE id = ?`).get(modelIdB);
    if (!modelA || !modelB) return { error: 'Model not found' };
    const weightsA = JSON.parse(modelA.weights || '{}');
    const weightsB = JSON.parse(modelB.weights || '{}');
    // Score a sample of leads with both models
    const leads = db.prepare(`SELECT id, first_name, last_name, email, phone, website, firm_name, city, state, lead_score FROM leads ORDER BY lead_score DESC LIMIT 200`).all();
    const results = leads.map(lead => {
      let scoreA = 0, scoreB = 0;
      for (const [field, weight] of Object.entries(weightsA)) {
        if (lead[field] && lead[field] !== '') scoreA += weight;
      }
      for (const [field, weight] of Object.entries(weightsB)) {
        if (lead[field] && lead[field] !== '') scoreB += weight;
      }
      return { id: lead.id, name: (lead.first_name || '') + ' ' + (lead.last_name || ''), scoreA, scoreB, diff: scoreA - scoreB };
    });
    // Correlation
    const avgA = results.reduce((s, r) => s + r.scoreA, 0) / results.length;
    const avgB = results.reduce((s, r) => s + r.scoreB, 0) / results.length;
    // Top promoted/demoted
    const sorted = [...results].sort((a, b) => b.diff - a.diff);
    const promoted = sorted.slice(0, 10);
    const demoted = sorted.slice(-10).reverse();
    // Distribution comparison
    const distA = { min: Math.min(...results.map(r => r.scoreA)), max: Math.max(...results.map(r => r.scoreA)), avg: Math.round(avgA * 10) / 10 };
    const distB = { min: Math.min(...results.map(r => r.scoreB)), max: Math.max(...results.map(r => r.scoreB)), avg: Math.round(avgB * 10) / 10 };
    return {
      modelA: { id: modelA.id, name: modelA.name, weights: weightsA, distribution: distA },
      modelB: { id: modelB.id, name: modelB.name, weights: weightsB, distribution: distB },
      sampleSize: results.length, promoted, demoted,
    };
  } catch (err) { return { error: err.message }; }
}

function getScoringModelRankings() {
  const db = getDb();
  try {
    const models = db.prepare(`SELECT * FROM scoring_models ORDER BY created_at DESC`).all();
    return models.map(m => {
      const weights = JSON.parse(m.weights || '{}');
      const fieldCount = Object.keys(weights).length;
      const maxPossible = Object.values(weights).reduce((s, w) => s + w, 0);
      return { ...m, fieldCount, maxPossible, weights };
    });
  } catch { return []; }
}

// --- Geographic Clustering ---
function getGeographicClusters(options = {}) {
  const db = getDb();
  const { minClusterSize = 5, limit = 50 } = options;
  // City clusters
  const cityClusters = db.prepare(`
    SELECT city, state, COUNT(*) as lead_count,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT firm_name) as unique_firms,
      COUNT(DISTINCT practice_area) as practice_areas
    FROM leads WHERE city IS NOT NULL AND city != ''
    GROUP BY city, state HAVING lead_count >= ? ORDER BY lead_count DESC LIMIT ?
  `).all(minClusterSize, limit);
  // State-level aggregation
  const stateClusters = db.prepare(`
    SELECT state, COUNT(*) as lead_count, COUNT(DISTINCT city) as cities,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT firm_name) as unique_firms
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY lead_count DESC
  `).all();
  // Under-penetrated markets (cities with few leads relative to their state)
  const underPenetrated = db.prepare(`
    SELECT l.city, l.state, COUNT(*) as lead_count, s.state_total,
      ROUND(CAST(COUNT(*) AS REAL) / s.state_total * 100, 1) as pct_of_state
    FROM leads l
    JOIN (SELECT state, COUNT(*) as state_total FROM leads GROUP BY state) s ON l.state = s.state
    WHERE l.city IS NOT NULL AND l.city != '' AND s.state_total >= 20
    GROUP BY l.city, l.state HAVING lead_count >= 3 AND pct_of_state < 5
    ORDER BY s.state_total DESC, lead_count ASC LIMIT 20
  `).all();
  // Market penetration summary
  const totalLeads = db.prepare(`SELECT COUNT(*) as count FROM leads`).get().count;
  const totalCities = db.prepare(`SELECT COUNT(DISTINCT city) as count FROM leads WHERE city IS NOT NULL AND city != ''`).get().count;
  const totalStates = db.prepare(`SELECT COUNT(DISTINCT state) as count FROM leads WHERE state IS NOT NULL AND state != ''`).get().count;
  return {
    summary: { totalLeads, totalCities, totalStates, avgPerCity: totalCities > 0 ? Math.round(totalLeads / totalCities) : 0 },
    cityClusters, stateClusters, underPenetrated,
  };
}

function getMarketPenetration(state) {
  const db = getDb();
  const cities = db.prepare(`
    SELECT city, COUNT(*) as lead_count,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      COUNT(DISTINCT firm_name) as firms, ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE state = ? AND city IS NOT NULL AND city != ''
    GROUP BY city ORDER BY lead_count DESC
  `).all(state);
  const total = db.prepare(`SELECT COUNT(*) as count FROM leads WHERE state = ?`).get(state);
  return { state, totalLeads: total?.count || 0, cities };
}

// BATCH 24: Priority Inbox, Practice Area Analytics, Source ROI, Compliance Dashboard

// --- Priority Inbox ---
function getPriorityInbox(limit = 25) {
  const db = getDb();
  // Get leads with best composite score for outreach
  const leads = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.phone, l.firm_name, l.city, l.state,
      l.lead_score, l.pipeline_stage, l.practice_area, l.website, l.updated_at
    FROM leads l
    WHERE l.email IS NOT NULL AND l.email != ''
    AND (l.pipeline_stage IS NULL OR l.pipeline_stage NOT IN ('won', 'lost', 'unqualified'))
    ORDER BY l.lead_score DESC LIMIT ?
  `).all(limit * 3);
  // Score each lead for priority
  const now = Date.now();
  const scored = leads.map(lead => {
    let priority = 0;
    // Base score (40% weight)
    priority += (lead.lead_score || 0) * 0.4;
    // Has phone bonus (20%)
    if (lead.phone) priority += 20;
    // Has website bonus (10%)
    if (lead.website) priority += 10;
    // Freshness bonus (15%) — recently updated leads get more priority
    const age = (now - new Date(lead.updated_at).getTime()) / 86400000;
    if (age < 7) priority += 15;
    else if (age < 30) priority += 10;
    else if (age < 90) priority += 5;
    // Pipeline stage bonus (15%)
    if (lead.pipeline_stage === 'qualified') priority += 15;
    else if (lead.pipeline_stage === 'contacted') priority += 10;
    else if (!lead.pipeline_stage || lead.pipeline_stage === 'new') priority += 5;
    // Check if recently contacted
    let lastContact = null;
    try {
      const contact = db.prepare(`SELECT created_at FROM contact_log WHERE lead_id = ? ORDER BY created_at DESC LIMIT 1`).get(lead.id);
      if (contact) {
        lastContact = contact.created_at;
        const daysSince = (now - new Date(contact.created_at).getTime()) / 86400000;
        if (daysSince < 3) priority -= 20; // Recently contacted — deprioritize
        else if (daysSince > 14) priority += 10; // Due for follow-up
      } else {
        priority += 5; // Never contacted — should reach out
      }
    } catch {}
    const reason = !lastContact ? 'Never contacted' : 'Follow-up due';
    return { ...lead, priority: Math.round(priority * 10) / 10, reason, lastContact };
  });
  return scored.sort((a, b) => b.priority - a.priority).slice(0, limit);
}

function getSmartRecommendations() {
  const db = getDb();
  const recs = [];
  // Leads with email but never contacted
  const uncontacted = db.prepare(`
    SELECT COUNT(*) as count FROM leads
    WHERE email IS NOT NULL AND email != '' AND id NOT IN (SELECT DISTINCT lead_id FROM contact_log)
  `).get();
  if (uncontacted?.count > 0) recs.push({ type: 'outreach', message: `${uncontacted.count} leads with email never contacted`, priority: 'high', count: uncontacted.count });
  // Leads in contacted stage > 7 days without follow-up
  try {
    const stale = db.prepare(`
      SELECT COUNT(*) as count FROM leads l
      WHERE l.pipeline_stage = 'contacted'
      AND l.id NOT IN (SELECT lead_id FROM contact_log WHERE created_at > datetime('now', '-7 days'))
    `).get();
    if (stale?.count > 0) recs.push({ type: 'follow_up', message: `${stale.count} contacted leads need follow-up (7+ days)`, priority: 'medium', count: stale.count });
  } catch {}
  // High-score leads without email
  const noEmail = db.prepare(`SELECT COUNT(*) as count FROM leads WHERE lead_score >= 50 AND (email IS NULL OR email = '')`).get();
  if (noEmail?.count > 0) recs.push({ type: 'enrich', message: `${noEmail.count} high-score leads missing email — enrich these first`, priority: 'high', count: noEmail.count });
  // New leads added this week
  const newThisWeek = db.prepare(`SELECT COUNT(*) as count FROM leads WHERE created_at >= datetime('now', '-7 days')`).get();
  if (newThisWeek?.count > 0) recs.push({ type: 'review', message: `${newThisWeek.count} new leads added this week`, priority: 'low', count: newThisWeek.count });
  return recs;
}

// --- Practice Area Analytics ---
function getPracticeAreaAnalytics(limit = 30) {
  const db = getDb();
  const areas = db.prepare(`
    SELECT practice_area, COUNT(*) as lead_count,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT state) as states, COUNT(DISTINCT firm_name) as firms,
      COUNT(DISTINCT city) as cities
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area ORDER BY lead_count DESC LIMIT ?
  `).all(limit);
  // Firm specialization — firms with multiple practice areas
  const firmSpec = db.prepare(`
    SELECT firm_name, GROUP_CONCAT(DISTINCT practice_area) as practices, COUNT(DISTINCT practice_area) as pa_count, COUNT(*) as headcount
    FROM leads WHERE firm_name IS NOT NULL AND firm_name != '' AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY firm_name HAVING pa_count >= 2 ORDER BY pa_count DESC LIMIT 20
  `).all();
  // Cross-sell opportunities: firms in one area that overlap with another
  const crossSell = db.prepare(`
    SELECT a.practice_area as area_a, b.practice_area as area_b,
      COUNT(DISTINCT a.firm_name) as shared_firms
    FROM leads a JOIN leads b ON a.firm_name = b.firm_name AND a.practice_area != b.practice_area
    WHERE a.practice_area IS NOT NULL AND b.practice_area IS NOT NULL AND a.firm_name IS NOT NULL
    GROUP BY a.practice_area, b.practice_area HAVING shared_firms >= 2
    ORDER BY shared_firms DESC LIMIT 20
  `).all();
  return { areas, firmSpecialization: firmSpec, crossSell };
}

// --- Source ROI Tracking ---
function getSourceROI() {
  const db = getDb();
  const sources = db.prepare(`
    SELECT primary_source,
      COUNT(*) as total_leads,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as with_website,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT state) as states, COUNT(DISTINCT city) as cities,
      SUM(CASE WHEN pipeline_stage IN ('qualified', 'won') THEN 1 ELSE 0 END) as converted
    FROM leads WHERE primary_source IS NOT NULL AND primary_source != ''
    GROUP BY primary_source ORDER BY total_leads DESC
  `).all();
  // Calculate effectiveness metrics
  return sources.map(s => ({
    ...s,
    email_rate: s.total_leads > 0 ? Math.round((s.with_email / s.total_leads) * 100) : 0,
    phone_rate: s.total_leads > 0 ? Math.round((s.with_phone / s.total_leads) * 100) : 0,
    conversion_rate: s.total_leads > 0 ? Math.round((s.converted / s.total_leads) * 100) : 0,
    completeness: s.total_leads > 0 ? Math.round(((s.with_email + s.with_phone + s.with_website) / (s.total_leads * 3)) * 100) : 0,
  }));
}

function getSourceComparison(sourceA, sourceB) {
  const db = getDb();
  const getSourceStats = (source) => {
    const stats = db.prepare(`
      SELECT COUNT(*) as total,
        SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
        SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
        ROUND(AVG(lead_score), 1) as avg_score,
        COUNT(DISTINCT city) as cities
      FROM leads WHERE primary_source = ?
    `).get(source);
    return { source, ...stats };
  };
  return { a: getSourceStats(sourceA), b: getSourceStats(sourceB) };
}

// --- Compliance Dashboard ---
function _ensureCompliance() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS compliance_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    consent_type TEXT NOT NULL,
    status TEXT DEFAULT 'granted',
    source TEXT DEFAULT 'scrape',
    recorded_at TEXT DEFAULT (datetime('now')),
    expires_at TEXT,
    notes TEXT DEFAULT ''
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS opt_outs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    reason TEXT DEFAULT '',
    opted_out_at TEXT DEFAULT (datetime('now')),
    source TEXT DEFAULT 'manual'
  )`);
}

function recordConsent(leadId, consentType, status = 'granted', source = 'scrape', notes = '') {
  const db = getDb();
  _ensureCompliance();
  return db.prepare(`INSERT INTO compliance_log (lead_id, consent_type, status, source, notes) VALUES (?, ?, ?, ?, ?)`).run(leadId, consentType, status, source, notes);
}

function addOptOut(email, reason = '', source = 'manual') {
  const db = getDb();
  _ensureCompliance();
  return db.prepare(`INSERT OR IGNORE INTO opt_outs (email, reason, source) VALUES (?, ?, ?)`).run(email, reason, source);
}

function removeOptOut(email) {
  const db = getDb();
  _ensureCompliance();
  return db.prepare(`DELETE FROM opt_outs WHERE email = ?`).run(email);
}

function getComplianceDashboard() {
  const db = getDb();
  _ensureCompliance();
  // Opt-out stats
  const optOutCount = db.prepare(`SELECT COUNT(*) as count FROM opt_outs`).get().count;
  const recentOptOuts = db.prepare(`SELECT * FROM opt_outs ORDER BY opted_out_at DESC LIMIT 20`).all();
  // DNC overlap
  let dncCount = 0;
  try { dncCount = db.prepare(`SELECT COUNT(*) as count FROM dnc_list`).get().count; } catch {}
  // Leads with email in opt-out list
  const blocked = db.prepare(`SELECT COUNT(*) as count FROM leads l JOIN opt_outs o ON l.email = o.email WHERE l.email IS NOT NULL AND l.email != ''`).get().count;
  // Data retention — leads older than certain thresholds
  const retention = db.prepare(`
    SELECT
      SUM(CASE WHEN created_at < datetime('now', '-365 days') THEN 1 ELSE 0 END) as older_1yr,
      SUM(CASE WHEN created_at < datetime('now', '-180 days') THEN 1 ELSE 0 END) as older_6mo,
      SUM(CASE WHEN created_at < datetime('now', '-90 days') THEN 1 ELSE 0 END) as older_90d,
      COUNT(*) as total
    FROM leads
  `).get();
  // Compliance score: higher is better (no opt-outs, fresh data, DNC managed)
  const totalLeads = retention?.total || 1;
  const complianceScore = Math.max(0, 100 - Math.round((blocked / totalLeads) * 100) - Math.round(((retention?.older_1yr || 0) / totalLeads) * 50));
  return {
    complianceScore,
    optOuts: { total: optOutCount, recent: recentOptOuts, blocked },
    dnc: { total: dncCount },
    retention: retention || {},
  };
}

function checkEmailCompliance(email) {
  const db = getDb();
  _ensureCompliance();
  const optOut = db.prepare(`SELECT * FROM opt_outs WHERE email = ?`).get(email);
  let dnc = null;
  try { dnc = db.prepare(`SELECT * FROM dnc_list WHERE email = ? OR phone = ?`).get(email, email); } catch {}
  return { email, optedOut: !!optOut, optOutDetails: optOut, dnc: !!dnc, safe: !optOut && !dnc };
}

// BATCH 25: Journey Timeline, Predictive Scoring, Team Performance, Email Deliverability

// --- Lead Journey Timeline ---
function getLeadJourney(leadId) {
  const db = getDb();
  const events = [];
  // Base lead data
  const lead = db.prepare(`SELECT * FROM leads WHERE id = ?`).get(leadId);
  if (!lead) return [];
  events.push({ type: 'created', date: lead.created_at, detail: `Lead created from ${lead.primary_source || 'unknown'}` });
  if (lead.updated_at !== lead.created_at) events.push({ type: 'updated', date: lead.updated_at, detail: 'Lead data updated' });
  // Stage transitions
  try {
    db.prepare(`SELECT * FROM stage_transitions WHERE lead_id = ? ORDER BY transitioned_at`).all(leadId)
      .forEach(t => events.push({ type: 'stage_change', date: t.transitioned_at, detail: `${t.from_stage || 'new'} → ${t.to_stage}`, extra: { duration: t.duration_hours } }));
  } catch {}
  // Contacts
  try {
    db.prepare(`SELECT * FROM contact_log WHERE lead_id = ? ORDER BY created_at`).all(leadId)
      .forEach(c => events.push({ type: 'contact', date: c.created_at, detail: `${c.direction || ''} ${c.channel}: ${c.subject || ''}`.trim(), extra: { outcome: c.outcome } }));
  } catch {}
  // Notes
  try {
    db.prepare(`SELECT * FROM notes WHERE lead_id = ? ORDER BY created_at`).all(leadId)
      .forEach(n => events.push({ type: 'note', date: n.created_at, detail: (n.content || '').substring(0, 100) }));
  } catch {}
  // Activities
  try {
    db.prepare(`SELECT * FROM lead_activities WHERE lead_id = ? ORDER BY created_at`).all(leadId)
      .forEach(a => events.push({ type: 'activity', date: a.created_at, detail: `${a.activity_type}: ${a.description || ''}` }));
  } catch {}
  // Tags
  if (lead.tags) {
    lead.tags.split(',').forEach(tag => events.push({ type: 'tag', date: lead.updated_at, detail: `Tagged: ${tag.trim()}` }));
  }
  // Sequence enrollments
  try {
    db.prepare(`SELECT se.*, s.name FROM sequence_enrollments se LEFT JOIN sequences s ON se.sequence_id = s.id WHERE se.lead_id = ?`).all(leadId)
      .forEach(e => events.push({ type: 'sequence', date: e.enrolled_at, detail: `Enrolled in: ${e.name || 'Sequence #' + e.sequence_id}` }));
  } catch {}
  return events.sort((a, b) => new Date(b.date) - new Date(a.date));
}

// --- Predictive Lead Scoring ---
function getPredictiveScores(limit = 100) {
  const db = getDb();
  // Analyze what field combos correlate with conversion (pipeline_stage = won/qualified)
  const converted = db.prepare(`SELECT * FROM leads WHERE pipeline_stage IN ('won', 'qualified') LIMIT 500`).all();
  const allLeads = db.prepare(`SELECT * FROM leads LIMIT 1000`).all();
  // Feature importance: which fields are present more often in converted leads
  const fields = ['email', 'phone', 'website', 'firm_name', 'practice_area', 'city', 'bar_number'];
  const importance = {};
  const baseRates = {};
  for (const f of fields) {
    const convRate = converted.length > 0 ? converted.filter(l => l[f] && l[f] !== '').length / converted.length : 0;
    const allRate = allLeads.length > 0 ? allLeads.filter(l => l[f] && l[f] !== '').length / allLeads.length : 0;
    importance[f] = { conversionRate: Math.round(convRate * 100), baseRate: Math.round(allRate * 100), lift: allRate > 0 ? Math.round((convRate / allRate) * 100) / 100 : 0 };
    baseRates[f] = allRate;
  }
  // Score all leads with predictive model
  const leads = db.prepare(`SELECT id, first_name, last_name, email, phone, website, firm_name, practice_area, city, state, lead_score, pipeline_stage FROM leads ORDER BY lead_score DESC LIMIT ?`).all(limit);
  const scored = leads.map(lead => {
    let probability = 10; // base probability
    for (const f of fields) {
      if (lead[f] && lead[f] !== '') {
        probability += (importance[f]?.lift || 1) * 8;
      }
    }
    probability = Math.min(99, Math.max(1, Math.round(probability)));
    return { ...lead, conversionProbability: probability };
  });
  return {
    featureImportance: importance,
    predictions: scored.sort((a, b) => b.conversionProbability - a.conversionProbability),
    modelStats: { totalLeads: allLeads.length, convertedLeads: converted.length, conversionRate: allLeads.length > 0 ? Math.round((converted.length / allLeads.length) * 100) : 0 },
  };
}

// --- Team Performance ---
function getTeamPerformance() {
  const db = getDb();
  let owners = [];
  try {
    owners = db.prepare(`
      SELECT owner, COUNT(*) as assigned,
        SUM(CASE WHEN pipeline_stage IN ('won', 'qualified') THEN 1 ELSE 0 END) as converted,
        SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
        ROUND(AVG(lead_score), 1) as avg_score
      FROM leads WHERE owner IS NOT NULL AND owner != '' GROUP BY owner ORDER BY assigned DESC
    `).all();
  } catch {}
  // Contact activity per owner
  const ownerActivity = {};
  try {
    const contacts = db.prepare(`
      SELECT l.owner, COUNT(*) as contacts, COUNT(DISTINCT c.lead_id) as unique_leads
      FROM contact_log c JOIN leads l ON c.lead_id = l.id
      WHERE l.owner IS NOT NULL AND l.owner != ''
      GROUP BY l.owner
    `).all();
    contacts.forEach(c => { ownerActivity[c.owner] = { contacts: c.contacts, uniqueLeads: c.unique_leads }; });
  } catch {}
  const enriched = owners.map(o => ({
    ...o,
    contacts: ownerActivity[o.owner]?.contacts || 0,
    uniqueContacted: ownerActivity[o.owner]?.uniqueLeads || 0,
    conversionRate: o.assigned > 0 ? Math.round((o.converted / o.assigned) * 100) : 0,
    contactRate: o.assigned > 0 ? Math.round(((ownerActivity[o.owner]?.uniqueLeads || 0) / o.assigned) * 100) : 0,
  }));
  // Overall stats
  const total = db.prepare(`SELECT COUNT(*) as count FROM leads WHERE owner IS NOT NULL AND owner != ''`).get();
  return { members: enriched, totalAssigned: total?.count || 0, teamSize: owners.length };
}

// --- Email Deliverability Insights ---
function getEmailDeliverability() {
  const db = getDb();
  // Domain breakdown
  const domains = db.prepare(`
    SELECT LOWER(SUBSTR(email, INSTR(email, '@') + 1)) as domain, COUNT(*) as count
    FROM leads WHERE email IS NOT NULL AND email != '' AND email LIKE '%@%'
    GROUP BY domain ORDER BY count DESC LIMIT 30
  `).all();
  // Free vs corporate
  const freeProviders = ['gmail.com', 'yahoo.com', 'hotmail.com', 'aol.com', 'outlook.com', 'icloud.com', 'mail.com', 'protonmail.com'];
  let freeCount = 0, corpCount = 0;
  domains.forEach(d => {
    if (freeProviders.includes(d.domain)) freeCount += d.count;
    else corpCount += d.count;
  });
  // Email format patterns
  const patterns = db.prepare(`
    SELECT
      SUM(CASE WHEN email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com' OR email LIKE '%@hotmail.com' OR email LIKE '%@aol.com' OR email LIKE '%@outlook.com' THEN 1 ELSE 0 END) as free_email,
      SUM(CASE WHEN email IS NOT NULL AND email != '' AND email NOT LIKE '%@gmail.com' AND email NOT LIKE '%@yahoo.com' AND email NOT LIKE '%@hotmail.com' AND email NOT LIKE '%@aol.com' AND email NOT LIKE '%@outlook.com' THEN 1 ELSE 0 END) as corporate_email,
      COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as total_emails,
      COUNT(*) as total_leads
    FROM leads
  `).get();
  // Deliverability risk: flagged domains, catch-all domains, etc.
  const riskDomains = domains.filter(d => d.count >= 10 && !freeProviders.includes(d.domain)).map(d => ({
    ...d,
    risk: d.count > 50 ? 'high_volume' : 'normal',
    type: freeProviders.includes(d.domain) ? 'free' : 'corporate',
  }));
  return {
    summary: {
      totalEmails: patterns?.total_emails || 0,
      totalLeads: patterns?.total_leads || 0,
      emailRate: patterns?.total_leads > 0 ? Math.round((patterns.total_emails / patterns.total_leads) * 100) : 0,
      freeEmail: freeCount, corporateEmail: corpCount,
      corporateRate: (freeCount + corpCount) > 0 ? Math.round((corpCount / (freeCount + corpCount)) * 100) : 0,
    },
    topDomains: domains,
    riskDomains,
  };
}

// BATCH 26: Tagging Rules, Nurture Cadence, Custom Fields, Score Decay

// --- Tagging Rules Engine ---
function _ensureTagRules() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS tag_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    conditions TEXT NOT NULL,
    logic TEXT DEFAULT 'AND',
    enabled INTEGER DEFAULT 1,
    last_run TEXT,
    matches INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  )`);
}

function getTagRules() {
  const db = getDb();
  _ensureTagRules();
  return db.prepare(`SELECT * FROM tag_rules ORDER BY created_at DESC`).all();
}

function createTagRule(name, tag, conditions, logic = 'AND') {
  const db = getDb();
  _ensureTagRules();
  return db.prepare(`INSERT INTO tag_rules (name, tag, conditions, logic) VALUES (?, ?, ?, ?)`).run(name, tag, JSON.stringify(conditions), logic);
}

function deleteTagRule(id) {
  const db = getDb();
  _ensureTagRules();
  return db.prepare(`DELETE FROM tag_rules WHERE id = ?`).run(id);
}

function toggleTagRule(id) {
  const db = getDb();
  _ensureTagRules();
  const rule = db.prepare(`SELECT enabled FROM tag_rules WHERE id = ?`).get(id);
  if (!rule) return { error: 'Rule not found' };
  return db.prepare(`UPDATE tag_rules SET enabled = ? WHERE id = ?`).run(rule.enabled ? 0 : 1, id);
}

function runTagRules() {
  const db = getDb();
  _ensureTagRules();
  const rules = db.prepare(`SELECT * FROM tag_rules WHERE enabled = 1`).all();
  let totalMatches = 0;
  for (const rule of rules) {
    const conditions = JSON.parse(rule.conditions || '[]');
    if (conditions.length === 0) continue;
    // Build WHERE clause
    const whereParts = conditions.map(c => {
      if (c.op === 'eq') return `${c.field} = '${c.value}'`;
      if (c.op === 'neq') return `${c.field} != '${c.value}'`;
      if (c.op === 'contains') return `${c.field} LIKE '%${c.value}%'`;
      if (c.op === 'empty') return `(${c.field} IS NULL OR ${c.field} = '')`;
      if (c.op === 'not_empty') return `(${c.field} IS NOT NULL AND ${c.field} != '')`;
      if (c.op === 'gt') return `CAST(${c.field} AS REAL) > ${parseFloat(c.value)}`;
      if (c.op === 'lt') return `CAST(${c.field} AS REAL) < ${parseFloat(c.value)}`;
      return '1=1';
    });
    const joiner = rule.logic === 'OR' ? ' OR ' : ' AND ';
    const where = whereParts.join(joiner);
    try {
      const leads = db.prepare(`SELECT id, tags FROM leads WHERE ${where}`).all();
      let matches = 0;
      for (const lead of leads) {
        const existingTags = (lead.tags || '').split(',').map(t => t.trim()).filter(Boolean);
        if (!existingTags.includes(rule.tag)) {
          existingTags.push(rule.tag);
          db.prepare(`UPDATE leads SET tags = ? WHERE id = ?`).run(existingTags.join(','), lead.id);
          matches++;
        }
      }
      db.prepare(`UPDATE tag_rules SET last_run = datetime('now'), matches = ? WHERE id = ?`).run(matches, rule.id);
      totalMatches += matches;
    } catch {}
  }
  return { rulesRun: rules.length, totalTagged: totalMatches };
}

// --- Nurture Cadence Tracking ---
function getNurtureCadence(limit = 50) {
  const db = getDb();
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  const leads = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.phone, l.city, l.state, l.pipeline_stage, l.lead_score,
      (SELECT MAX(contact_at) FROM contact_log WHERE lead_id = l.id) as last_contact,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id) as total_contacts,
      (SELECT channel FROM contact_log WHERE lead_id = l.id ORDER BY contact_at DESC LIMIT 1) as last_channel
    FROM leads l
    WHERE l.email IS NOT NULL AND l.email != ''
    ORDER BY l.lead_score DESC LIMIT ?
  `).all(limit);
  const now = Date.now();
  return leads.map(l => {
    const daysSince = l.last_contact ? Math.round((now - new Date(l.last_contact).getTime()) / 86400000) : null;
    let cadenceStatus = 'never';
    let suggestedAction = 'Initial outreach via email';
    if (daysSince === null) {
      cadenceStatus = 'never_contacted';
      suggestedAction = 'Send initial cold email';
    } else if (daysSince <= 3) {
      cadenceStatus = 'active';
      suggestedAction = 'Wait for response';
    } else if (daysSince <= 7) {
      cadenceStatus = 'warm';
      suggestedAction = l.last_channel === 'email' ? 'Follow up via phone' : 'Send follow-up email';
    } else if (daysSince <= 14) {
      cadenceStatus = 'cooling';
      suggestedAction = 'Send value-add follow-up';
    } else if (daysSince <= 30) {
      cadenceStatus = 'cold';
      suggestedAction = 'Re-engagement email with new angle';
    } else {
      cadenceStatus = 'dormant';
      suggestedAction = 'Archive or restart sequence';
    }
    return { ...l, daysSinceContact: daysSince, cadenceStatus, suggestedAction, totalContacts: l.total_contacts };
  });
}

function getCadenceAnalytics() {
  const db = getDb();
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  // Avg contacts per stage — use LEFT JOIN to avoid correlated subquery in AVG
  const byStage = db.prepare(`
    SELECT l.pipeline_stage as stage, COUNT(DISTINCT l.id) as leads,
      ROUND(CAST(COUNT(cl.id) AS REAL) / MAX(COUNT(DISTINCT l.id), 1), 1) as avg_contacts,
      COUNT(DISTINCT CASE WHEN cl.contact_at > datetime('now', '-7 days') THEN l.id END) as active_7d
    FROM leads l LEFT JOIN contact_log cl ON cl.lead_id = l.id
    WHERE l.pipeline_stage IS NOT NULL AND l.pipeline_stage != ''
    GROUP BY l.pipeline_stage
  `).all();
  // Optimal cadence: contacts/stage for converted leads
  const optimal = db.prepare(`
    SELECT l.pipeline_stage as stage,
      ROUND(CAST(COUNT(cl.id) AS REAL) / MAX(COUNT(DISTINCT l.id), 1), 1) as avg_contacts
    FROM leads l LEFT JOIN contact_log cl ON cl.lead_id = l.id
    WHERE l.pipeline_stage IN ('won', 'qualified')
    GROUP BY l.pipeline_stage
  `).all();
  // Gap analysis: leads due for contact
  const gapAnalysis = [];
  const total = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''`).get().c;
  const withContact = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log`).get().c;
  gapAnalysis.push({ cadenceStatus: 'never', count: total - withContact });
  if (withContact > 0) {
    const active = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log WHERE contact_at > datetime('now', '-3 days')`).get().c;
    const warm = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log WHERE contact_at BETWEEN datetime('now', '-7 days') AND datetime('now', '-3 days')`).get().c;
    const cooling = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log WHERE contact_at BETWEEN datetime('now', '-14 days') AND datetime('now', '-7 days')`).get().c;
    const cold = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log WHERE contact_at BETWEEN datetime('now', '-30 days') AND datetime('now', '-14 days')`).get().c;
    const dormant = db.prepare(`SELECT COUNT(DISTINCT lead_id) as c FROM contact_log WHERE contact_at < datetime('now', '-30 days')`).get().c;
    gapAnalysis.push({ cadenceStatus: 'active', count: active }, { cadenceStatus: 'warm', count: warm }, { cadenceStatus: 'cooling', count: cooling }, { cadenceStatus: 'cold', count: cold }, { cadenceStatus: 'dormant', count: dormant });
  }
  return { byStage, optimal, gapAnalysis };
}

// --- Custom Fields ---
function _ensureCustomFields() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS custom_field_defs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    field_name TEXT UNIQUE NOT NULL,
    field_type TEXT NOT NULL DEFAULT 'text',
    options TEXT DEFAULT '[]',
    required INTEGER DEFAULT 0,
    default_value TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS custom_field_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT DEFAULT '',
    UNIQUE(lead_id, field_name)
  )`);
}

function getCustomFieldDefs() {
  const db = getDb();
  _ensureCustomFields();
  return db.prepare(`SELECT * FROM custom_field_defs ORDER BY field_name`).all();
}

function createCustomField(fieldName, fieldType = 'text', options = [], required = false, defaultValue = '') {
  const db = getDb();
  _ensureCustomFields();
  return db.prepare(`INSERT INTO custom_field_defs (field_name, field_type, options, required, default_value) VALUES (?, ?, ?, ?, ?)`).run(fieldName, fieldType, JSON.stringify(options), required ? 1 : 0, defaultValue);
}

function deleteCustomField(fieldName) {
  const db = getDb();
  _ensureCustomFields();
  db.prepare(`DELETE FROM custom_field_values WHERE field_name = ?`).run(fieldName);
  return db.prepare(`DELETE FROM custom_field_defs WHERE field_name = ?`).run(fieldName);
}

function setCustomFieldValue(leadId, fieldName, value) {
  const db = getDb();
  _ensureCustomFields();
  return db.prepare(`INSERT OR REPLACE INTO custom_field_values (lead_id, field_name, value) VALUES (?, ?, ?)`).run(leadId, fieldName, String(value));
}

function getCustomFieldValues(leadId) {
  const db = getDb();
  _ensureCustomFields();
  return db.prepare(`SELECT field_name, value FROM custom_field_values WHERE lead_id = ?`).all(leadId);
}

function getCustomFieldStats() {
  const db = getDb();
  _ensureCustomFields();
  const defs = db.prepare(`SELECT * FROM custom_field_defs`).all();
  return defs.map(d => {
    const filled = db.prepare(`SELECT COUNT(*) as count FROM custom_field_values WHERE field_name = ? AND value != ''`).get(d.field_name);
    return { ...d, filledCount: filled?.count || 0 };
  });
}

// --- Score Decay ---
function _ensureDecayConfig() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS score_decay_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    enabled INTEGER DEFAULT 0,
    decay_type TEXT DEFAULT 'linear',
    decay_rate REAL DEFAULT 1.0,
    decay_interval_days INTEGER DEFAULT 7,
    min_score INTEGER DEFAULT 0,
    exempt_stages TEXT DEFAULT '["won"]',
    last_run TEXT
  )`);
  try {
    db.prepare(`INSERT OR IGNORE INTO score_decay_config (id) VALUES (1)`).run();
  } catch {}
}

function getDecayConfig() {
  const db = getDb();
  _ensureDecayConfig();
  return db.prepare(`SELECT * FROM score_decay_config WHERE id = 1`).get();
}

function updateDecayConfig(config) {
  const db = getDb();
  _ensureDecayConfig();
  const { enabled, decayType, decayRate, decayIntervalDays, minScore, exemptStages } = config;
  return db.prepare(`UPDATE score_decay_config SET enabled = ?, decay_type = ?, decay_rate = ?, decay_interval_days = ?, min_score = ?, exempt_stages = ? WHERE id = 1`).run(
    enabled ? 1 : 0, decayType || 'linear', decayRate || 1.0, decayIntervalDays || 7, minScore || 0, JSON.stringify(exemptStages || ['won'])
  );
}

function runScoreDecay() {
  const db = getDb();
  _ensureDecayConfig();
  const config = db.prepare(`SELECT * FROM score_decay_config WHERE id = 1`).get();
  if (!config || !config.enabled) return { applied: 0, skipped: 'disabled' };
  const exemptStages = JSON.parse(config.exempt_stages || '["won"]');
  const exemptClause = exemptStages.map(s => `'${s}'`).join(',');
  // Get leads eligible for decay
  const leads = db.prepare(`
    SELECT id, lead_score, updated_at, pipeline_stage FROM leads
    WHERE lead_score > ? AND (pipeline_stage IS NULL OR pipeline_stage NOT IN (${exemptClause}))
    AND updated_at < datetime('now', '-' || ? || ' days')
  `).all(config.min_score, config.decay_interval_days);
  let applied = 0;
  for (const lead of leads) {
    const daysSinceUpdate = Math.round((Date.now() - new Date(lead.updated_at).getTime()) / 86400000);
    const periods = Math.floor(daysSinceUpdate / config.decay_interval_days);
    let newScore;
    if (config.decay_type === 'exponential') {
      newScore = Math.round(lead.lead_score * Math.pow(1 - config.decay_rate / 100, periods));
    } else {
      newScore = Math.round(lead.lead_score - (config.decay_rate * periods));
    }
    newScore = Math.max(config.min_score, newScore);
    if (newScore !== lead.lead_score) {
      db.prepare(`UPDATE leads SET lead_score = ? WHERE id = ?`).run(newScore, lead.id);
      applied++;
    }
  }
  db.prepare(`UPDATE score_decay_config SET last_run = datetime('now') WHERE id = 1`).run();
  return { applied, eligible: leads.length, config: { type: config.decay_type, rate: config.decay_rate, interval: config.decay_interval_days } };
}

function getDecayPreview2(limit = 20) {
  const db = getDb();
  _ensureDecayConfig();
  const config = db.prepare(`SELECT * FROM score_decay_config WHERE id = 1`).get();
  if (!config) return [];
  const leads = db.prepare(`
    SELECT id, first_name, last_name, lead_score, updated_at, pipeline_stage FROM leads
    WHERE lead_score > ? AND updated_at < datetime('now', '-' || ? || ' days')
    ORDER BY updated_at ASC LIMIT ?
  `).all(config.min_score || 0, config.decay_interval_days || 7, limit);
  return leads.map(l => {
    const days = Math.round((Date.now() - new Date(l.updated_at).getTime()) / 86400000);
    const periods = Math.floor(days / (config.decay_interval_days || 7));
    let projectedScore;
    if (config.decay_type === 'exponential') {
      projectedScore = Math.round(l.lead_score * Math.pow(1 - (config.decay_rate || 1) / 100, periods));
    } else {
      projectedScore = Math.round(l.lead_score - ((config.decay_rate || 1) * periods));
    }
    projectedScore = Math.max(config.min_score || 0, projectedScore);
    return { ...l, daysSinceUpdate: days, projectedScore, scoreDrop: l.lead_score - projectedScore };
  });
}

// --- Lookalike Finder (Batch 27) ---
function findLookalikes(leadId, limit = 20) {
  const db = getDb();
  const lead = db.prepare('SELECT * FROM leads WHERE id = ?').get(leadId);
  if (!lead) return { target: null, matches: [] };
  // Score similarity based on matching attributes
  const candidates = db.prepare(`
    SELECT id, first_name, last_name, email, phone, city, state, firm_name, practice_area, lead_score, primary_source
    FROM leads WHERE id != ? LIMIT 5000
  `).all(leadId);
  const scored = candidates.map(c => {
    let similarity = 0;
    if (c.city && lead.city && c.city.toLowerCase() === lead.city.toLowerCase()) similarity += 25;
    if (c.state && lead.state && c.state === lead.state) similarity += 15;
    if (c.practice_area && lead.practice_area && c.practice_area.toLowerCase() === lead.practice_area.toLowerCase()) similarity += 25;
    if (c.firm_name && lead.firm_name && c.firm_name.toLowerCase() === lead.firm_name.toLowerCase()) similarity += 20;
    if (c.lead_score && lead.lead_score) {
      const diff = Math.abs(c.lead_score - lead.lead_score);
      if (diff <= 5) similarity += 15;
      else if (diff <= 15) similarity += 10;
      else if (diff <= 30) similarity += 5;
    }
    const hasEmail = c.email ? 1 : 0;
    const targetHasEmail = lead.email ? 1 : 0;
    if (hasEmail === targetHasEmail) similarity += 5;
    return { ...c, similarity };
  }).filter(c => c.similarity >= 20).sort((a, b) => b.similarity - a.similarity).slice(0, limit);
  return { target: { id: lead.id, first_name: lead.first_name, last_name: lead.last_name, city: lead.city, state: lead.state, practice_area: lead.practice_area, firm_name: lead.firm_name, lead_score: lead.lead_score }, matches: scored };
}

// --- Conversion Funnel (Batch 27) ---
function getConversionFunnel() {
  const db = getDb();
  const stages = ['new', 'contacted', 'qualified', 'proposal', 'won', 'lost'];
  const stageCounts = {};
  for (const stage of stages) {
    stageCounts[stage] = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE pipeline_stage = ?`).get(stage).c;
  }
  // Funnel with drop-off rates
  const funnel = [];
  let prevCount = null;
  for (const stage of stages) {
    const count = stageCounts[stage] || 0;
    const dropOff = prevCount != null && prevCount > 0 ? Math.round((1 - count / prevCount) * 100) : 0;
    const conversionRate = prevCount != null && prevCount > 0 ? Math.round((count / prevCount) * 100) : 100;
    funnel.push({ stage, count, dropOff, conversionRate });
    if (count > 0) prevCount = count;
    else if (prevCount === null) prevCount = count;
  }
  // Avg time per stage from lifecycle table
  let stageTimings = [];
  try {
    stageTimings = db.prepare(`
      SELECT to_stage as stage, ROUND(AVG(JULIANDAY(transitioned_at) - JULIANDAY(
        (SELECT MAX(transitioned_at) FROM lead_lifecycle ll2 WHERE ll2.lead_id = lead_lifecycle.lead_id AND ll2.transitioned_at < lead_lifecycle.transitioned_at)
      )), 1) as avg_days
      FROM lead_lifecycle GROUP BY to_stage
    `).all();
  } catch { /* lifecycle table may not exist */ }
  // Conversion by source
  const bySource = db.prepare(`
    SELECT primary_source as source, pipeline_stage as stage, COUNT(*) as count
    FROM leads WHERE primary_source IS NOT NULL AND primary_source != '' AND pipeline_stage IS NOT NULL
    GROUP BY primary_source, pipeline_stage ORDER BY primary_source
  `).all();
  const sources = {};
  for (const r of bySource) {
    if (!sources[r.source]) sources[r.source] = {};
    sources[r.source][r.stage] = r.count;
  }
  return { funnel, stageTimings, bySource: sources, totalLeads: Object.values(stageCounts).reduce((a, b) => a + b, 0) };
}

// --- Lead Velocity (Batch 27) ---
function getLeadVelocity(days = 30) {
  const db = getDb();
  // Daily acquisition rate
  const daily = db.prepare(`
    SELECT DATE(created_at) as day, COUNT(*) as count
    FROM leads WHERE created_at >= datetime('now', '-' || ? || ' days')
    GROUP BY DATE(created_at) ORDER BY day
  `).all(days);
  // By source
  const bySource = db.prepare(`
    SELECT primary_source as source, COUNT(*) as count,
      MIN(created_at) as first_lead, MAX(created_at) as last_lead
    FROM leads WHERE created_at >= datetime('now', '-' || ? || ' days') AND primary_source IS NOT NULL AND primary_source != ''
    GROUP BY primary_source ORDER BY count DESC
  `).all(days);
  // By state
  const byState = db.prepare(`
    SELECT state, COUNT(*) as count
    FROM leads WHERE created_at >= datetime('now', '-' || ? || ' days') AND state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY count DESC LIMIT 20
  `).all(days);
  // Weekly rollup
  const weekly = [];
  const weekMap = {};
  for (const d of daily) {
    const dt = new Date(d.day);
    const weekStart = new Date(dt);
    weekStart.setDate(dt.getDate() - dt.getDay());
    const key = weekStart.toISOString().slice(0, 10);
    weekMap[key] = (weekMap[key] || 0) + d.count;
  }
  for (const [week, count] of Object.entries(weekMap)) {
    weekly.push({ week, count });
  }
  const totalNew = daily.reduce((sum, d) => sum + d.count, 0);
  const avgDaily = days > 0 ? Math.round(totalNew / days * 10) / 10 : 0;
  // Velocity trend: compare first half vs second half
  const mid = Math.floor(daily.length / 2);
  const firstHalf = daily.slice(0, mid).reduce((s, d) => s + d.count, 0);
  const secondHalf = daily.slice(mid).reduce((s, d) => s + d.count, 0);
  const trend = firstHalf > 0 ? Math.round(((secondHalf - firstHalf) / firstHalf) * 100) : 0;
  return { daily, weekly, bySource, byState, totalNew, avgDaily, trend, days };
}

// --- Data Completeness Matrix (Batch 27) ---
function getCompletenessMatrix() {
  const db = getDb();
  const fields = ['email', 'phone', 'website', 'firm_name', 'city', 'state', 'practice_area', 'bar_number', 'bar_status', 'admission_date'];
  // By source
  const sources = db.prepare(`SELECT DISTINCT primary_source FROM leads WHERE primary_source IS NOT NULL AND primary_source != '' ORDER BY primary_source`).all().map(r => r.primary_source);
  const matrix = [];
  for (const source of sources) {
    const total = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE primary_source = ?`).get(source).c;
    if (total === 0) continue;
    const row = { source, total, fields: {} };
    for (const field of fields) {
      const filled = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE primary_source = ? AND ${field} IS NOT NULL AND ${field} != ''`).get(source).c;
      row.fields[field] = { filled, total, rate: Math.round((filled / total) * 100) };
    }
    matrix.push(row);
  }
  // Best source per field
  const bestSources = {};
  for (const field of fields) {
    let best = null;
    let bestRate = 0;
    for (const row of matrix) {
      if (row.fields[field] && row.fields[field].rate > bestRate) {
        bestRate = row.fields[field].rate;
        best = row.source;
      }
    }
    bestSources[field] = { source: best, rate: bestRate };
  }
  // Gap recommendations
  const gaps = [];
  for (const field of fields) {
    const totalFilled = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE ${field} IS NOT NULL AND ${field} != ''`).get().c;
    const totalAll = db.prepare('SELECT COUNT(*) as c FROM leads').get().c;
    const rate = totalAll > 0 ? Math.round((totalFilled / totalAll) * 100) : 0;
    if (rate < 50) {
      gaps.push({ field, fillRate: rate, totalMissing: totalAll - totalFilled, recommendation: bestSources[field]?.source ? `Best source: ${bestSources[field].source} (${bestSources[field].rate}%)` : 'No source provides this field' });
    }
  }
  return { matrix, bestSources, gaps, fields, sourceCount: sources.length };
}

// --- Lead Clustering (Batch 28) ---
function getLeadClusters() {
  const db = getDb();
  // Cluster by firm size (frequency of firm_name as proxy)
  const firms = db.prepare(`
    SELECT firm_name, COUNT(*) as headcount,
      GROUP_CONCAT(DISTINCT city) as cities,
      GROUP_CONCAT(DISTINCT state) as states,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE firm_name IS NOT NULL AND firm_name != ''
    GROUP BY LOWER(firm_name) HAVING headcount >= 2
    ORDER BY headcount DESC LIMIT 100
  `).all();
  // Categorize into tiers
  const clusters = { solo: { count: 0, leads: 0, avgScore: 0, totalScore: 0 }, small: { count: 0, leads: 0, avgScore: 0, totalScore: 0 }, mid: { count: 0, leads: 0, avgScore: 0, totalScore: 0 }, large: { count: 0, leads: 0, avgScore: 0, totalScore: 0 } };
  const soloCount = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE firm_name IS NULL OR firm_name = ''`).get().c;
  const singleFirms = db.prepare(`SELECT COUNT(*) as c FROM (SELECT firm_name FROM leads WHERE firm_name IS NOT NULL AND firm_name != '' GROUP BY LOWER(firm_name) HAVING COUNT(*) = 1)`).get().c;
  clusters.solo = { count: singleFirms + soloCount, leads: singleFirms + soloCount, avgScore: 0, label: 'Solo/Unknown' };
  for (const f of firms) {
    const tier = f.headcount >= 20 ? 'large' : f.headcount >= 5 ? 'mid' : 'small';
    clusters[tier].count++;
    clusters[tier].leads += f.headcount;
    clusters[tier].totalScore = (clusters[tier].totalScore || 0) + (f.avg_score * f.headcount);
  }
  for (const tier of ['small', 'mid', 'large']) {
    clusters[tier].avgScore = clusters[tier].leads > 0 ? Math.round(clusters[tier].totalScore / clusters[tier].leads) : 0;
    clusters[tier].label = tier === 'small' ? 'Small (2-4)' : tier === 'mid' ? 'Mid (5-19)' : 'Large (20+)';
    delete clusters[tier].totalScore;
  }
  // Revenue potential: large firms with high scores
  const topOpportunities = firms.filter(f => f.headcount >= 3 && f.avg_score >= 30).slice(0, 15).map(f => ({
    firm: f.firm_name, headcount: f.headcount, cities: f.cities, avgScore: f.avg_score,
    emailRate: f.headcount > 0 ? Math.round((f.with_email / f.headcount) * 100) : 0,
    revenueEstimate: Math.round(f.headcount * f.avg_score * 10) // Simple proxy
  }));
  return { clusters, topFirms: firms.slice(0, 20), topOpportunities };
}

// --- A/B Test Framework (Batch 28) ---
function _ensureAbTests() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS ab_tests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    variant_a TEXT NOT NULL,
    variant_b TEXT NOT NULL,
    metric TEXT DEFAULT 'response_rate',
    status TEXT DEFAULT 'draft',
    created_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS ab_test_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    test_id INTEGER NOT NULL,
    lead_id INTEGER NOT NULL,
    variant TEXT NOT NULL,
    outcome TEXT DEFAULT '',
    responded INTEGER DEFAULT 0,
    assigned_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (test_id) REFERENCES ab_tests(id) ON DELETE CASCADE,
    UNIQUE(test_id, lead_id)
  )`);
}
function getAbTests() {
  const db = getDb();
  _ensureAbTests();
  const tests = db.prepare('SELECT * FROM ab_tests ORDER BY created_at DESC').all();
  return tests.map(t => {
    const aCount = db.prepare('SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = ?').get(t.id, 'A').c;
    const bCount = db.prepare('SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = ?').get(t.id, 'B').c;
    const aResponded = db.prepare('SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = ? AND responded = 1').get(t.id, 'A').c;
    const bResponded = db.prepare('SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = ? AND responded = 1').get(t.id, 'B').c;
    const aRate = aCount > 0 ? Math.round((aResponded / aCount) * 100) : 0;
    const bRate = bCount > 0 ? Math.round((bResponded / bCount) * 100) : 0;
    let winner = null;
    if (aCount >= 30 && bCount >= 30) {
      if (aRate > bRate + 5) winner = 'A';
      else if (bRate > aRate + 5) winner = 'B';
      else winner = 'tie';
    }
    return { ...t, variantA: { count: aCount, responded: aResponded, rate: aRate }, variantB: { count: bCount, responded: bResponded, rate: bRate }, winner };
  });
}
function createAbTest(name, description, variantA, variantB, metric) {
  const db = getDb();
  _ensureAbTests();
  return db.prepare('INSERT INTO ab_tests (name, description, variant_a, variant_b, metric) VALUES (?, ?, ?, ?, ?)').run(name, description || '', variantA, variantB, metric || 'response_rate');
}
function assignLeadsToAbTest(testId, leadIds) {
  const db = getDb();
  _ensureAbTests();
  const insert = db.prepare('INSERT OR IGNORE INTO ab_test_assignments (test_id, lead_id, variant) VALUES (?, ?, ?)');
  let aCount = 0, bCount = 0;
  const txn = db.transaction(() => {
    for (const lid of leadIds) {
      const variant = Math.random() < 0.5 ? 'A' : 'B';
      const result = insert.run(testId, lid, variant);
      if (result.changes) { if (variant === 'A') aCount++; else bCount++; }
    }
  });
  txn();
  db.prepare("UPDATE ab_tests SET status = 'running' WHERE id = ? AND status = 'draft'").run(testId);
  return { assigned: aCount + bCount, variantA: aCount, variantB: bCount };
}
function recordAbTestOutcome(testId, leadId, responded) {
  const db = getDb();
  _ensureAbTests();
  return db.prepare('UPDATE ab_test_assignments SET responded = ?, outcome = ? WHERE test_id = ? AND lead_id = ?').run(responded ? 1 : 0, responded ? 'responded' : 'no_response', testId, leadId);
}
function deleteAbTest(id) {
  const db = getDb();
  _ensureAbTests();
  db.prepare('DELETE FROM ab_test_assignments WHERE test_id = ?').run(id);
  return db.prepare('DELETE FROM ab_tests WHERE id = ?').run(id);
}

// --- Re-engagement Scoring (Batch 28) ---
function getReengagementLeads(limit = 30) {
  const db = getDb();
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  // Find dormant high-value leads
  const leads = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.phone, l.city, l.state, l.firm_name, l.lead_score, l.pipeline_stage,
      l.updated_at, l.tags,
      (SELECT MAX(contact_at) FROM contact_log WHERE lead_id = l.id) as last_contact,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id) as total_contacts
    FROM leads l
    WHERE l.lead_score >= 30 AND l.email IS NOT NULL AND l.email != ''
    ORDER BY l.lead_score DESC LIMIT 500
  `).all();
  const now = Date.now();
  const scored = leads.map(l => {
    const daysSinceUpdate = Math.round((now - new Date(l.updated_at).getTime()) / 86400000);
    const daysSinceContact = l.last_contact ? Math.round((now - new Date(l.last_contact).getTime()) / 86400000) : 999;
    // Re-engagement probability: higher score + longer dormant = higher need but lower probability
    let reengageScore = l.lead_score;
    if (daysSinceContact > 30) reengageScore += 10; // Needs re-engagement
    if (daysSinceContact > 60) reengageScore += 15;
    if (l.total_contacts === 0) reengageScore += 20; // Never contacted = high opportunity
    if (l.total_contacts >= 3 && daysSinceContact > 30) reengageScore -= 10; // Already tried
    const probability = Math.max(5, Math.min(95, 100 - Math.min(daysSinceContact, 120)));
    let strategy = 'Send personalized email';
    if (l.total_contacts === 0) strategy = 'Initial cold outreach — high value untouched lead';
    else if (daysSinceContact > 90) strategy = 'Fresh angle re-engagement — reference industry news';
    else if (daysSinceContact > 30) strategy = 'Gentle check-in with value proposition';
    else strategy = 'Follow up on previous conversation';
    return { ...l, daysSinceUpdate, daysSinceContact, reengageScore, probability, strategy };
  }).filter(l => l.daysSinceContact > 14 || l.total_contacts === 0)
    .sort((a, b) => b.reengageScore - a.reengageScore).slice(0, limit);
  const summary = {
    totalDormant: scored.length,
    neverContacted: scored.filter(l => l.total_contacts === 0).length,
    avgScore: scored.length > 0 ? Math.round(scored.reduce((s, l) => s + l.lead_score, 0) / scored.length) : 0,
    avgDaysDormant: scored.length > 0 ? Math.round(scored.reduce((s, l) => s + l.daysSinceContact, 0) / scored.length) : 0
  };
  return { leads: scored, summary };
}

// --- Attribution Model (Batch 28) ---
function getAttributionModel() {
  const db = getDb();
  // First-touch attribution: which source brought the lead
  const firstTouch = db.prepare(`
    SELECT primary_source as source, COUNT(*) as leads,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      SUM(CASE WHEN pipeline_stage IN ('qualified', 'proposal', 'won') THEN 1 ELSE 0 END) as converted
    FROM leads WHERE primary_source IS NOT NULL AND primary_source != ''
    GROUP BY primary_source ORDER BY leads DESC
  `).all();
  // Enrichment attribution: which enrichment steps added the most data
  const enrichAttribution = {};
  const emailSources = db.prepare(`
    SELECT email_source as source, COUNT(*) as count FROM leads
    WHERE email_source IS NOT NULL AND email_source != '' GROUP BY email_source ORDER BY count DESC
  `).all();
  const phoneSources = db.prepare(`
    SELECT phone_source as source, COUNT(*) as count FROM leads
    WHERE phone_source IS NOT NULL AND phone_source != '' GROUP BY phone_source ORDER BY count DESC
  `).all();
  const websiteSources = db.prepare(`
    SELECT website_source as source, COUNT(*) as count FROM leads
    WHERE website_source IS NOT NULL AND website_source != '' GROUP BY website_source ORDER BY count DESC
  `).all();
  enrichAttribution.email = emailSources;
  enrichAttribution.phone = phoneSources;
  enrichAttribution.website = websiteSources;
  // Pipeline impact by source
  const pipelineImpact = db.prepare(`
    SELECT primary_source as source, pipeline_stage as stage, COUNT(*) as count
    FROM leads WHERE primary_source IS NOT NULL AND primary_source != '' AND pipeline_stage IS NOT NULL AND pipeline_stage != ''
    GROUP BY primary_source, pipeline_stage ORDER BY count DESC
  `).all();
  const impactMap = {};
  for (const r of pipelineImpact) {
    if (!impactMap[r.source]) impactMap[r.source] = {};
    impactMap[r.source][r.stage] = r.count;
  }
  // Source quality score = (avg_score * email_rate * conversion_rate)
  const sourceQuality = firstTouch.map(s => {
    const emailRate = s.leads > 0 ? s.with_email / s.leads : 0;
    const convRate = s.leads > 0 ? s.converted / s.leads : 0;
    const quality = Math.round((s.avg_score || 0) * (emailRate + 0.1) * (convRate + 0.1) * 100) / 100;
    return { source: s.source, leads: s.leads, avgScore: s.avg_score, emailRate: Math.round(emailRate * 100), convRate: Math.round(convRate * 100), quality };
  }).sort((a, b) => b.quality - a.quality);
  return { firstTouch, enrichAttribution, pipelineImpact: impactMap, sourceQuality };
}

// --- Response Time & SLA Tracking (Batch 29) ---
function getResponseTimeSLA() {
  const db = getDb();
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  // Leads contacted but awaiting response
  const pendingResponse = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.city, l.state, l.lead_score, l.owner,
      (SELECT MAX(contact_at) FROM contact_log WHERE lead_id = l.id AND direction = 'outbound') as last_outbound,
      (SELECT MAX(contact_at) FROM contact_log WHERE lead_id = l.id AND direction = 'inbound') as last_inbound,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id) as total_contacts
    FROM leads l
    WHERE l.pipeline_stage = 'contacted'
    ORDER BY l.lead_score DESC LIMIT 100
  `).all();
  const now = Date.now();
  const slaThreshold = 24; // hours
  const withSLA = pendingResponse.map(l => {
    const lastOut = l.last_outbound ? new Date(l.last_outbound).getTime() : null;
    const lastIn = l.last_inbound ? new Date(l.last_inbound).getTime() : null;
    const hoursSinceOutbound = lastOut ? Math.round((now - lastOut) / 3600000) : null;
    const responseTimeHours = lastOut && lastIn && lastIn > lastOut ? Math.round((lastIn - lastOut) / 3600000) : null;
    const slaStatus = hoursSinceOutbound === null ? 'no_contact' : hoursSinceOutbound <= slaThreshold ? 'within_sla' : 'overdue';
    return { ...l, hoursSinceOutbound, responseTimeHours, slaStatus, slaThreshold };
  });
  const overdue = withSLA.filter(l => l.slaStatus === 'overdue').length;
  const withinSla = withSLA.filter(l => l.slaStatus === 'within_sla').length;
  // Avg response time by owner
  const byOwner = db.prepare(`
    SELECT l.owner, COUNT(DISTINCT l.id) as leads,
      COUNT(cl.id) as total_contacts
    FROM leads l LEFT JOIN contact_log cl ON cl.lead_id = l.id
    WHERE l.owner IS NOT NULL AND l.owner != ''
    GROUP BY l.owner ORDER BY leads DESC
  `).all();
  return { leads: withSLA.slice(0, 30), summary: { total: pendingResponse.length, overdue, withinSla, slaThreshold }, byOwner };
}

// --- Market Saturation Analysis (Batch 29) ---
function getMarketSaturation() {
  const db = getDb();
  // Coverage by city — our leads vs estimated market
  const cities = db.prepare(`
    SELECT city, state, COUNT(*) as our_leads,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      GROUP_CONCAT(DISTINCT primary_source) as sources
    FROM leads WHERE city IS NOT NULL AND city != ''
    GROUP BY LOWER(city), state
    HAVING our_leads >= 5
    ORDER BY our_leads DESC LIMIT 50
  `).all();
  // Estimate saturation: larger cities likely have more lawyers
  const tierCities = { 'New York': 50000, 'Los Angeles': 35000, 'Chicago': 25000, 'Houston': 18000, 'Phoenix': 12000,
    'Philadelphia': 15000, 'San Antonio': 8000, 'San Diego': 12000, 'Dallas': 15000, 'Miami': 15000,
    'Atlanta': 14000, 'London': 30000, 'Edinburgh': 8000, 'Sydney': 20000, 'Melbourne': 15000, 'Toronto': 25000 };
  const saturation = cities.map(c => {
    const estimated = tierCities[c.city] || (c.our_leads < 50 ? c.our_leads * 20 : c.our_leads * 10);
    const penetration = Math.min(100, Math.round((c.our_leads / estimated) * 100));
    return { ...c, estimatedMarket: estimated, penetration };
  });
  // Under-served: low penetration + high potential
  const underServed = saturation.filter(c => c.penetration < 30 && c.our_leads >= 10)
    .sort((a, b) => (a.penetration - b.penetration)).slice(0, 10);
  // Coverage by state
  const byState = db.prepare(`
    SELECT state, COUNT(*) as leads, COUNT(DISTINCT city) as cities,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY leads DESC
  `).all();
  return { cities: saturation, underServed, byState };
}

// --- Enrichment Waterfall Viz (Batch 29) ---
function getEnrichmentWaterfall() {
  const db = getDb();
  // Count data at each enrichment stage
  const total = db.prepare('SELECT COUNT(*) as c FROM leads').get().c;
  const withEmail = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
  const withPhone = db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NOT NULL AND phone != ''").get().c;
  const withWebsite = db.prepare("SELECT COUNT(*) as c FROM leads WHERE website IS NOT NULL AND website != ''").get().c;
  const withFirm = db.prepare("SELECT COUNT(*) as c FROM leads WHERE firm_name IS NOT NULL AND firm_name != ''").get().c;
  // By source attribution
  const emailBySource = db.prepare(`
    SELECT email_source as source, COUNT(*) as count FROM leads
    WHERE email_source IS NOT NULL AND email_source != '' GROUP BY email_source ORDER BY count DESC
  `).all();
  const phoneBySource = db.prepare(`
    SELECT phone_source as source, COUNT(*) as count FROM leads
    WHERE phone_source IS NOT NULL AND phone_source != '' GROUP BY phone_source ORDER BY count DESC
  `).all();
  const websiteBySource = db.prepare(`
    SELECT website_source as source, COUNT(*) as count FROM leads
    WHERE website_source IS NOT NULL AND website_source != '' GROUP BY website_source ORDER BY count DESC
  `).all();
  // Enrichment steps order: bar_scrape → profile → cross_ref → website_crawl
  const steps = [
    { step: 'Bar Scrape', description: 'Initial data from bar directory search', fills: { email: 0, phone: 0, website: 0 } },
    { step: 'Profile Fetch', description: 'Detail page from same bar directory', fills: { email: 0, phone: 0, website: 0 } },
    { step: 'Cross-Reference', description: 'Martindale + Lawyers.com lookups', fills: { email: 0, phone: 0, website: 0 } },
    { step: 'Website Crawl', description: 'Firm website email extraction', fills: { email: 0, phone: 0, website: 0 } }
  ];
  const sourceMap = { bar: 0, profile: 1, martindale: 2, 'lawyers-com': 2, 'cross-ref': 2, 'website-crawl': 3, website: 3 };
  for (const s of emailBySource) { const idx = sourceMap[s.source]; if (idx !== undefined) steps[idx].fills.email += s.count; }
  for (const s of phoneBySource) { const idx = sourceMap[s.source]; if (idx !== undefined) steps[idx].fills.phone += s.count; }
  for (const s of websiteBySource) { const idx = sourceMap[s.source]; if (idx !== undefined) steps[idx].fills.website += s.count; }
  return { total, coverage: { email: withEmail, phone: withPhone, website: withWebsite, firm: withFirm }, steps, emailBySource, phoneBySource, websiteBySource };
}

// --- Competitive Intelligence (Batch 29) ---
function getCompetitiveIntelligence() {
  const db = getDb();
  // Firms across multiple jurisdictions
  const multiStateFirms = db.prepare(`
    SELECT firm_name, COUNT(DISTINCT state) as states, COUNT(*) as attorneys,
      GROUP_CONCAT(DISTINCT state) as state_list,
      GROUP_CONCAT(DISTINCT city) as city_list,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE firm_name IS NOT NULL AND firm_name != '' AND state IS NOT NULL
    GROUP BY LOWER(firm_name) HAVING states >= 2
    ORDER BY states DESC, attorneys DESC LIMIT 30
  `).all();
  // Practice area landscape by city
  const practiceByCity = db.prepare(`
    SELECT city, practice_area, COUNT(*) as count
    FROM leads WHERE city IS NOT NULL AND city != '' AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY LOWER(city), LOWER(practice_area) HAVING count >= 3
    ORDER BY count DESC LIMIT 50
  `).all();
  const cityPractice = {};
  for (const r of practiceByCity) {
    if (!cityPractice[r.city]) cityPractice[r.city] = [];
    cityPractice[r.city].push({ practice: r.practice_area, count: r.count });
  }
  // Top firms by practice area
  const firmsByPractice = db.prepare(`
    SELECT practice_area, firm_name, COUNT(*) as attorneys
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != '' AND firm_name IS NOT NULL AND firm_name != ''
    GROUP BY LOWER(practice_area), LOWER(firm_name) HAVING attorneys >= 3
    ORDER BY practice_area, attorneys DESC
  `).all();
  const practiceLeaders = {};
  for (const r of firmsByPractice) {
    if (!practiceLeaders[r.practice_area]) practiceLeaders[r.practice_area] = [];
    if (practiceLeaders[r.practice_area].length < 5) {
      practiceLeaders[r.practice_area].push({ firm: r.firm_name, attorneys: r.attorneys });
    }
  }
  return { multiStateFirms, cityPractice, practiceLeaders };
}

// --- Email Sequence Builder (Batch 30) ---
function _ensureSequenceTemplates() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS sequence_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    steps TEXT DEFAULT '[]',
    variables TEXT DEFAULT '["first_name","last_name","firm_name","city","state","practice_area","email"]',
    status TEXT DEFAULT 'draft',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  )`);
}
function getSequenceTemplates() {
  const db = getDb();
  _ensureSequenceTemplates();
  const templates = db.prepare('SELECT * FROM sequence_templates ORDER BY updated_at DESC').all();
  return templates.map(t => ({ ...t, steps: JSON.parse(t.steps || '[]'), variables: JSON.parse(t.variables || '[]') }));
}
function createSequenceTemplate(name, description, steps) {
  const db = getDb();
  _ensureSequenceTemplates();
  return db.prepare('INSERT INTO sequence_templates (name, description, steps) VALUES (?, ?, ?)').run(name, description || '', JSON.stringify(steps || []));
}
function updateSequenceTemplate(id, updates) {
  const db = getDb();
  _ensureSequenceTemplates();
  const sets = [];
  const values = [];
  if (updates.name) { sets.push('name = ?'); values.push(updates.name); }
  if (updates.description !== undefined) { sets.push('description = ?'); values.push(updates.description); }
  if (updates.steps) { sets.push('steps = ?'); values.push(JSON.stringify(updates.steps)); }
  if (updates.status) { sets.push('status = ?'); values.push(updates.status); }
  sets.push("updated_at = datetime('now')");
  values.push(id);
  return db.prepare(`UPDATE sequence_templates SET ${sets.join(', ')} WHERE id = ?`).run(...values);
}
function deleteSequenceTemplate(id) {
  const db = getDb();
  _ensureSequenceTemplates();
  return db.prepare('DELETE FROM sequence_templates WHERE id = ?').run(id);
}
function renderSequenceTemplate(templateId, leadId) {
  const db = getDb();
  _ensureSequenceTemplates();
  const template = db.prepare('SELECT * FROM sequence_templates WHERE id = ?').get(templateId);
  if (!template) return null;
  const lead = db.prepare('SELECT * FROM leads WHERE id = ?').get(leadId);
  if (!lead) return null;
  const steps = JSON.parse(template.steps || '[]');
  const rendered = steps.map(step => {
    let subject = step.subject || '';
    let body = step.body || '';
    for (const [key, val] of Object.entries(lead)) {
      const placeholder = new RegExp('\\{' + key + '\\}', 'g');
      subject = subject.replace(placeholder, val || '');
      body = body.replace(placeholder, val || '');
    }
    return { ...step, subject, body };
  });
  return { template: { id: template.id, name: template.name }, lead: { id: lead.id, first_name: lead.first_name, last_name: lead.last_name, email: lead.email }, rendered };
}

// --- Data Quality Rules (Batch 30) ---
function _ensureQualityRules() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS data_quality_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    field TEXT NOT NULL,
    check_type TEXT NOT NULL,
    check_value TEXT DEFAULT '',
    severity TEXT DEFAULT 'warning',
    flag_tag TEXT DEFAULT '',
    enabled INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
  )`);
}
function getQualityRules() {
  const db = getDb();
  _ensureQualityRules();
  return db.prepare('SELECT * FROM data_quality_rules ORDER BY created_at DESC').all();
}
function createQualityRule(name, field, checkType, checkValue, severity, flagTag) {
  const db = getDb();
  _ensureQualityRules();
  return db.prepare('INSERT INTO data_quality_rules (name, field, check_type, check_value, severity, flag_tag) VALUES (?, ?, ?, ?, ?, ?)').run(name, field, checkType, checkValue || '', severity || 'warning', flagTag || '');
}
function deleteQualityRule(id) {
  const db = getDb();
  _ensureQualityRules();
  return db.prepare('DELETE FROM data_quality_rules WHERE id = ?').run(id);
}
function runQualityRules() {
  const db = getDb();
  _ensureQualityRules();
  const rules = db.prepare('SELECT * FROM data_quality_rules WHERE enabled = 1').all();
  let totalFlagged = 0;
  const results = [];
  for (const rule of rules) {
    let where = '';
    switch (rule.check_type) {
      case 'contains': where = `${rule.field} LIKE '%' || ? || '%'`; break;
      case 'not_contains': where = `${rule.field} NOT LIKE '%' || ? || '%'`; break;
      case 'equals': where = `${rule.field} = ?`; break;
      case 'starts_with': where = `${rule.field} LIKE ? || '%'`; break;
      case 'empty': where = `(${rule.field} IS NULL OR ${rule.field} = '')`; break;
      case 'regex_match': where = `${rule.field} LIKE ?`; break;
      default: continue;
    }
    try {
      const params = rule.check_type === 'empty' ? [] : [rule.check_value];
      const count = db.prepare(`SELECT COUNT(*) as c FROM leads WHERE ${where}`).get(...params).c;
      if (count > 0 && rule.flag_tag) {
        const leads = db.prepare(`SELECT id, tags FROM leads WHERE ${where}`).all(...params);
        const update = db.prepare('UPDATE leads SET tags = ? WHERE id = ?');
        const txn = db.transaction(() => {
          for (const lead of leads) {
            const tags = (lead.tags || '').split(',').map(t => t.trim()).filter(Boolean);
            if (!tags.includes(rule.flag_tag)) {
              tags.push(rule.flag_tag);
              update.run(tags.join(','), lead.id);
            }
          }
        });
        txn();
      }
      totalFlagged += count;
      results.push({ rule: rule.name, field: rule.field, check: rule.check_type, flagged: count, tag: rule.flag_tag });
    } catch { /* skip bad rules */ }
  }
  return { totalFlagged, rulesRun: rules.length, results };
}

// --- Unified Timeline (Batch 30) ---
function getLeadTimeline(leadId, limit = 50) {
  const db = getDb();
  const events = [];
  // Lead creation
  const lead = db.prepare('SELECT created_at, updated_at, pipeline_stage, tags, notes FROM leads WHERE id = ?').get(leadId);
  if (!lead) return [];
  events.push({ type: 'created', timestamp: lead.created_at, details: 'Lead created' });
  // Contact log
  try {
    const contacts = db.prepare('SELECT * FROM contact_log WHERE lead_id = ? ORDER BY contact_at DESC').all(leadId);
    for (const c of contacts) {
      events.push({ type: 'contact', timestamp: c.contact_at, details: `${c.direction} ${c.channel}${c.subject ? ': ' + c.subject : ''}`, channel: c.channel, direction: c.direction });
    }
  } catch { /* table may not exist */ }
  // Stage transitions
  try {
    const transitions = db.prepare('SELECT * FROM lead_lifecycle WHERE lead_id = ? ORDER BY transitioned_at DESC').all(leadId);
    for (const t of transitions) {
      events.push({ type: 'stage_change', timestamp: t.transitioned_at, details: `Stage: ${t.from_stage || '?'} → ${t.to_stage}` });
    }
  } catch { /* table may not exist */ }
  // Audit log entries
  try {
    const audits = db.prepare("SELECT * FROM audit_log WHERE entity_type = 'lead' AND entity_id = ? ORDER BY created_at DESC LIMIT 20").all(leadId);
    for (const a of audits) {
      events.push({ type: 'audit', timestamp: a.created_at, details: `${a.action}: ${a.details || ''}` });
    }
  } catch { /* table may not exist */ }
  // Notes
  try {
    const notes = db.prepare('SELECT * FROM lead_notes WHERE lead_id = ? ORDER BY created_at DESC').all(leadId);
    for (const n of notes) {
      events.push({ type: 'note', timestamp: n.created_at, details: n.content, author: n.author });
    }
  } catch { /* table may not exist */ }
  events.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return events.slice(0, limit);
}

// --- Export Scheduler (Batch 30) ---
function _ensureExportSchedules() {
  const db = getDb();
  db.exec(`CREATE TABLE IF NOT EXISTS export_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    filters TEXT DEFAULT '{}',
    columns TEXT DEFAULT '[]',
    frequency TEXT DEFAULT 'weekly',
    last_exported TEXT,
    next_export TEXT,
    export_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT (datetime('now'))
  )`);
}
function getExportSchedules() {
  const db = getDb();
  _ensureExportSchedules();
  const schedules = db.prepare('SELECT * FROM export_schedules ORDER BY created_at DESC').all();
  return schedules.map(s => ({ ...s, filters: JSON.parse(s.filters || '{}'), columns: JSON.parse(s.columns || '[]') }));
}
function createExportSchedule(name, filters, columns, frequency) {
  const db = getDb();
  _ensureExportSchedules();
  const freqDays = { daily: 1, weekly: 7, biweekly: 14, monthly: 30 };
  const days = freqDays[frequency] || 7;
  const nextExport = new Date(Date.now() + days * 86400000).toISOString().slice(0, 19).replace('T', ' ');
  return db.prepare('INSERT INTO export_schedules (name, filters, columns, frequency, next_export) VALUES (?, ?, ?, ?, ?)').run(name, JSON.stringify(filters || {}), JSON.stringify(columns || []), frequency || 'weekly', nextExport);
}
function deleteExportSchedule(id) {
  const db = getDb();
  _ensureExportSchedules();
  return db.prepare('DELETE FROM export_schedules WHERE id = ?').run(id);
}
function runExportSchedule(id) {
  const db = getDb();
  _ensureExportSchedules();
  const schedule = db.prepare('SELECT * FROM export_schedules WHERE id = ?').get(id);
  if (!schedule) return null;
  const filters = JSON.parse(schedule.filters || '{}');
  let where = '1=1';
  const params = [];
  if (filters.state) { where += ' AND state = ?'; params.push(filters.state); }
  if (filters.hasEmail) { where += " AND email IS NOT NULL AND email != ''"; }
  if (filters.minScore) { where += ' AND lead_score >= ?'; params.push(filters.minScore); }
  if (filters.tag) { where += " AND tags LIKE '%' || ? || '%'"; params.push(filters.tag); }
  if (filters.since) { where += ' AND created_at >= ?'; params.push(filters.since); }
  const leads = db.prepare(`SELECT * FROM leads WHERE ${where} ORDER BY lead_score DESC LIMIT 10000`).all(...params);
  const freqDays = { daily: 1, weekly: 7, biweekly: 14, monthly: 30 };
  const days = freqDays[schedule.frequency] || 7;
  const nextExport = new Date(Date.now() + days * 86400000).toISOString().slice(0, 19).replace('T', ' ');
  db.prepare("UPDATE export_schedules SET last_exported = datetime('now'), next_export = ?, export_count = export_count + 1 WHERE id = ?").run(nextExport, id);
  return { scheduleId: id, name: schedule.name, leadsExported: leads.length, nextExport };
}

// --- Propensity Model (Batch 31) ---
function getPropensityScores(limit = 50) {
  const db = getDb();
  const leads = db.prepare(`
    SELECT id, first_name, last_name, email, phone, website, city, state, firm_name, practice_area,
      lead_score, pipeline_stage, tags, created_at, updated_at
    FROM leads ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  const scored = leads.map(l => {
    let propensity = 0;
    // Data completeness factors (0-40)
    if (l.email) propensity += 15;
    if (l.phone) propensity += 10;
    if (l.website) propensity += 5;
    if (l.firm_name) propensity += 5;
    if (l.practice_area) propensity += 5;
    // Score factor (0-20)
    propensity += Math.min(20, Math.round((l.lead_score || 0) / 5));
    // Engagement factor (0-20)
    const contacts = db.prepare('SELECT COUNT(*) as c FROM contact_log WHERE lead_id = ?').get(l.id).c;
    if (contacts > 0) propensity += Math.min(15, contacts * 5);
    const responded = db.prepare("SELECT COUNT(*) as c FROM contact_log WHERE lead_id = ? AND direction = 'inbound'").get(l.id).c;
    if (responded > 0) propensity += 5;
    // Recency factor (0-10)
    const daysSince = Math.round((Date.now() - new Date(l.updated_at).getTime()) / 86400000);
    if (daysSince <= 7) propensity += 10;
    else if (daysSince <= 30) propensity += 5;
    // Firm size proxy (0-10)
    if (l.firm_name) {
      const firmSize = db.prepare('SELECT COUNT(*) as c FROM leads WHERE LOWER(firm_name) = LOWER(?)').get(l.firm_name).c;
      if (firmSize >= 10) propensity += 10;
      else if (firmSize >= 3) propensity += 5;
    }
    propensity = Math.min(100, propensity);
    const likelihood = propensity >= 70 ? 'high' : propensity >= 40 ? 'medium' : 'low';
    return { id: l.id, first_name: l.first_name, last_name: l.last_name, email: l.email, city: l.city, state: l.state, lead_score: l.lead_score, propensity, likelihood, contacts, firm_name: l.firm_name };
  }).sort((a, b) => b.propensity - a.propensity);
  const distribution = { high: scored.filter(s => s.likelihood === 'high').length, medium: scored.filter(s => s.likelihood === 'medium').length, low: scored.filter(s => s.likelihood === 'low').length };
  return { leads: scored, distribution, avgPropensity: scored.length > 0 ? Math.round(scored.reduce((s, l) => s + l.propensity, 0) / scored.length) : 0 };
}

// --- Cohort Analysis (Batch 31) ---
function getCohortAnalysis() {
  const db = getDb();
  // Group by acquisition week
  const cohorts = db.prepare(`
    SELECT strftime('%Y-W%W', created_at) as cohort_week,
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN pipeline_stage = 'contacted' THEN 1 ELSE 0 END) as contacted,
      SUM(CASE WHEN pipeline_stage IN ('qualified', 'proposal', 'won') THEN 1 ELSE 0 END) as converted,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads
    GROUP BY cohort_week ORDER BY cohort_week DESC LIMIT 12
  `).all();
  // Retention: % still in active pipeline per cohort
  const retention = cohorts.map(c => {
    const activeRate = c.total > 0 ? Math.round(((c.contacted + c.converted) / c.total) * 100) : 0;
    const emailRate = c.total > 0 ? Math.round((c.with_email / c.total) * 100) : 0;
    return { ...c, activeRate, emailRate };
  });
  // Cohort comparison: first vs last
  const oldest = retention[retention.length - 1];
  const newest = retention[0];
  const comparison = oldest && newest ? {
    scoreImprovement: Math.round((newest.avg_score || 0) - (oldest.avg_score || 0)),
    emailRateChange: (newest.emailRate || 0) - (oldest.emailRate || 0),
    volumeChange: (newest.total || 0) - (oldest.total || 0)
  } : null;
  return { cohorts: retention, comparison };
}

// --- Communication Preferences (Batch 31) ---
function getChannelPreferences(limit = 30) {
  const db = getDb();
  // Ensure contact_log exists
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  // Channel effectiveness
  const channels = db.prepare(`
    SELECT channel, COUNT(*) as total_touches,
      COUNT(DISTINCT lead_id) as unique_leads,
      SUM(CASE WHEN outcome = 'responded' OR direction = 'inbound' THEN 1 ELSE 0 END) as responses
    FROM contact_log GROUP BY channel ORDER BY total_touches DESC
  `).all();
  const channelScores = channels.map(c => ({
    ...c, responseRate: c.total_touches > 0 ? Math.round((c.responses / c.total_touches) * 100) : 0,
    effectiveness: c.total_touches > 0 ? Math.round((c.responses / c.total_touches) * 100 + (c.unique_leads * 2)) : 0
  }));
  // Per-lead channel preference
  const leadPrefs = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.phone, l.city,
      (SELECT channel FROM contact_log WHERE lead_id = l.id AND (outcome = 'responded' OR direction = 'inbound') ORDER BY contact_at DESC LIMIT 1) as best_channel,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id) as total_touches
    FROM leads l
    WHERE l.id IN (SELECT DISTINCT lead_id FROM contact_log)
    ORDER BY total_touches DESC LIMIT ?
  `).all(limit);
  // Recommended channel for leads without contact history
  const recommendations = {
    withEmailOnly: db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != '' AND (phone IS NULL OR phone = '')").get().c,
    withPhoneOnly: db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NOT NULL AND phone != '' AND (email IS NULL OR email = '')").get().c,
    withBoth: db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != '' AND phone IS NOT NULL AND phone != ''").get().c,
    withNeither: db.prepare("SELECT COUNT(*) as c FROM leads WHERE (email IS NULL OR email = '') AND (phone IS NULL OR phone = '')").get().c
  };
  return { channels: channelScores, leadPreferences: leadPrefs, recommendations };
}

// --- Jurisdiction Benchmarks (Batch 31) ---
function getJurisdictionBenchmarks() {
  const db = getDb();
  const benchmarks = db.prepare(`
    SELECT state,
      COUNT(*) as total_leads,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as email_count,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phone_count,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as website_count,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT city) as cities,
      COUNT(DISTINCT LOWER(firm_name)) as firms
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY total_leads DESC
  `).all();
  const enriched = benchmarks.map(b => ({
    ...b,
    emailRate: b.total_leads > 0 ? Math.round((b.email_count / b.total_leads) * 100) : 0,
    phoneRate: b.total_leads > 0 ? Math.round((b.phone_count / b.total_leads) * 100) : 0,
    websiteRate: b.total_leads > 0 ? Math.round((b.website_count / b.total_leads) * 100) : 0,
    dataQuality: 0
  }));
  // Calculate data quality composite score
  for (const b of enriched) {
    b.dataQuality = Math.round((b.emailRate * 0.4 + b.phoneRate * 0.3 + b.websiteRate * 0.2 + Math.min(100, b.avg_score * 2) * 0.1));
  }
  // Global averages
  const totalLeads = enriched.reduce((s, b) => s + b.total_leads, 0);
  const globalAvg = {
    emailRate: totalLeads > 0 ? Math.round(enriched.reduce((s, b) => s + b.email_count, 0) / totalLeads * 100) : 0,
    phoneRate: totalLeads > 0 ? Math.round(enriched.reduce((s, b) => s + b.phone_count, 0) / totalLeads * 100) : 0,
    avgScore: totalLeads > 0 ? Math.round(enriched.reduce((s, b) => s + (b.avg_score * b.total_leads), 0) / totalLeads * 10) / 10 : 0
  };
  const best = enriched.sort((a, b) => b.dataQuality - a.dataQuality).slice(0, 5);
  const worst = enriched.sort((a, b) => a.dataQuality - b.dataQuality).slice(0, 5);
  return { benchmarks: enriched.sort((a, b) => b.total_leads - a.total_leads), globalAvg, best, worst };
}

// --- Deal Size Estimation (Batch 32) ---
function getDealEstimates(limit = 40) {
  const db = getDb();
  const leads = db.prepare(`
    SELECT id, first_name, last_name, email, phone, city, state, firm_name, practice_area, lead_score, pipeline_stage
    FROM leads WHERE email IS NOT NULL AND email != '' ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  // City tier multipliers
  const tier1Cities = new Set(['new york', 'los angeles', 'chicago', 'houston', 'miami', 'dallas', 'san francisco', 'washington', 'boston', 'atlanta', 'london', 'toronto', 'sydney']);
  const tier2Cities = new Set(['philadelphia', 'phoenix', 'san diego', 'denver', 'seattle', 'minneapolis', 'detroit', 'portland', 'orlando', 'edinburgh', 'melbourne', 'vancouver']);
  // Practice area value multipliers
  const highValuePractice = new Set(['corporate', 'mergers', 'intellectual property', 'patent', 'securities', 'tax', 'real estate', 'banking']);
  const estimates = leads.map(l => {
    let baseValue = 500; // Base deal value
    // Score factor
    baseValue += (l.lead_score || 0) * 20;
    // City tier
    const cityLower = (l.city || '').toLowerCase();
    if (tier1Cities.has(cityLower)) baseValue *= 2.0;
    else if (tier2Cities.has(cityLower)) baseValue *= 1.5;
    // Practice area
    const practice = (l.practice_area || '').toLowerCase();
    if (highValuePractice.has(practice)) baseValue *= 1.8;
    // Firm size proxy
    if (l.firm_name) {
      const firmSize = db.prepare('SELECT COUNT(*) as c FROM leads WHERE LOWER(firm_name) = LOWER(?)').get(l.firm_name).c;
      if (firmSize >= 10) baseValue *= 1.5;
      else if (firmSize >= 3) baseValue *= 1.2;
    }
    // Pipeline stage multiplier
    const stageMult = { new: 0.1, contacted: 0.2, qualified: 0.4, proposal: 0.7, won: 1.0 };
    const weightedValue = Math.round(baseValue * (stageMult[l.pipeline_stage] || 0.1));
    return { id: l.id, first_name: l.first_name, last_name: l.last_name, city: l.city, state: l.state, firm_name: l.firm_name, practice_area: l.practice_area, lead_score: l.lead_score, pipeline_stage: l.pipeline_stage, dealEstimate: Math.round(baseValue), weightedValue };
  });
  const totalPipeline = estimates.reduce((s, e) => s + e.dealEstimate, 0);
  const weightedPipeline = estimates.reduce((s, e) => s + e.weightedValue, 0);
  const avgDeal = estimates.length > 0 ? Math.round(totalPipeline / estimates.length) : 0;
  return { leads: estimates, totalPipeline, weightedPipeline, avgDeal, count: estimates.length };
}

// --- Outreach Calendar (Batch 32) ---
function getOutreachCalendar() {
  const db = getDb();
  // Ensure contact_log
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  // Daily capacity: suggested 50 emails + 20 calls
  const emailCapacity = 50;
  const callCapacity = 20;
  // Leads ready for outreach
  const emailReady = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != '' AND pipeline_stage = 'new'").get().c;
  const phoneReady = db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NOT NULL AND phone != '' AND pipeline_stage = 'new'").get().c;
  // Days to clear backlog
  const emailDays = emailCapacity > 0 ? Math.ceil(emailReady / emailCapacity) : 0;
  const callDays = callCapacity > 0 ? Math.ceil(phoneReady / callCapacity) : 0;
  // Activity by day of week
  const byDayOfWeek = db.prepare(`
    SELECT CASE CAST(strftime('%w', contact_at) AS INTEGER)
      WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed'
      WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' END as day_name,
      COUNT(*) as count
    FROM contact_log GROUP BY strftime('%w', contact_at)
    ORDER BY CAST(strftime('%w', contact_at) AS INTEGER)
  `).all();
  // Next 7 days plan
  const plan = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date();
    d.setDate(d.getDate() + i);
    const dayName = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getDay()];
    const isWeekend = d.getDay() === 0 || d.getDay() === 6;
    plan.push({ date: d.toISOString().slice(0, 10), dayName, emails: isWeekend ? 0 : emailCapacity, calls: isWeekend ? 0 : callCapacity, isWeekend });
  }
  return { capacity: { emails: emailCapacity, calls: callCapacity }, backlog: { emailReady, phoneReady, emailDays, callDays }, byDayOfWeek, plan };
}

// --- Risk Scoring (Batch 32) ---
function getRiskScores(limit = 30) {
  const db = getDb();
  // Ensure contact_log
  try { db.prepare('SELECT 1 FROM contact_log LIMIT 1').get(); } catch {
    db.exec(`CREATE TABLE IF NOT EXISTS contact_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id INTEGER NOT NULL, channel TEXT NOT NULL,
      direction TEXT DEFAULT 'outbound', subject TEXT DEFAULT '', notes TEXT DEFAULT '', outcome TEXT DEFAULT '',
      contact_at TEXT DEFAULT (datetime('now')), FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
    )`);
  }
  const leads = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.email, l.city, l.state, l.lead_score, l.pipeline_stage, l.updated_at,
      (SELECT MAX(contact_at) FROM contact_log WHERE lead_id = l.id) as last_contact,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id) as total_contacts,
      (SELECT COUNT(*) FROM contact_log WHERE lead_id = l.id AND direction = 'inbound') as inbound_contacts
    FROM leads l
    WHERE l.pipeline_stage IN ('contacted', 'qualified', 'proposal')
    ORDER BY l.lead_score DESC LIMIT 500
  `).all();
  const now = Date.now();
  const scored = leads.map(l => {
    let riskScore = 0;
    // Time since last contact
    const daysSinceContact = l.last_contact ? Math.round((now - new Date(l.last_contact).getTime()) / 86400000) : 999;
    if (daysSinceContact > 30) riskScore += 40;
    else if (daysSinceContact > 14) riskScore += 25;
    else if (daysSinceContact > 7) riskScore += 10;
    // No inbound response
    if (l.total_contacts > 2 && l.inbound_contacts === 0) riskScore += 30;
    else if (l.total_contacts > 0 && l.inbound_contacts === 0) riskScore += 15;
    // Score decay signal
    const daysSinceUpdate = Math.round((now - new Date(l.updated_at).getTime()) / 86400000);
    if (daysSinceUpdate > 30) riskScore += 15;
    // Low engagement + high attempts = high risk
    if (l.total_contacts >= 5 && l.inbound_contacts === 0) riskScore += 15;
    riskScore = Math.min(100, riskScore);
    const riskLevel = riskScore >= 60 ? 'high' : riskScore >= 30 ? 'medium' : 'low';
    const action = riskScore >= 60 ? 'Escalate or archive' : riskScore >= 30 ? 'Re-engage with new approach' : 'Continue current cadence';
    return { ...l, riskScore, riskLevel, action, daysSinceContact, daysSinceUpdate };
  }).filter(l => l.riskScore > 0).sort((a, b) => b.riskScore - a.riskScore).slice(0, limit);
  const distribution = { high: scored.filter(s => s.riskLevel === 'high').length, medium: scored.filter(s => s.riskLevel === 'medium').length, low: scored.filter(s => s.riskLevel === 'low').length };
  return { leads: scored, distribution };
}

// --- Network Mapping (Batch 32) ---
function getNetworkMap(limit = 30) {
  const db = getDb();
  // Firm-based clusters: leads connected by same firm
  const firmClusters = db.prepare(`
    SELECT firm_name, COUNT(*) as members,
      GROUP_CONCAT(DISTINCT city) as cities,
      GROUP_CONCAT(DISTINCT state) as states,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE firm_name IS NOT NULL AND firm_name != ''
    GROUP BY LOWER(firm_name) HAVING members >= 3
    ORDER BY members DESC LIMIT ?
  `).all(limit);
  // City clusters: top connected cities
  const cityClusters = db.prepare(`
    SELECT city, state, COUNT(*) as leads, COUNT(DISTINCT LOWER(firm_name)) as firms,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads WHERE city IS NOT NULL AND city != ''
    GROUP BY LOWER(city), state HAVING leads >= 10
    ORDER BY leads DESC LIMIT 20
  `).all();
  // Practice area networks
  const practiceNetworks = db.prepare(`
    SELECT practice_area, COUNT(*) as leads, COUNT(DISTINCT LOWER(firm_name)) as firms,
      COUNT(DISTINCT city) as cities
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY LOWER(practice_area) HAVING leads >= 5
    ORDER BY leads DESC LIMIT 15
  `).all();
  // Key connectors: leads at firms with most cross-city presence
  const connectors = db.prepare(`
    SELECT l.id, l.first_name, l.last_name, l.firm_name, l.city, l.state, l.email, l.lead_score,
      (SELECT COUNT(DISTINCT city) FROM leads WHERE LOWER(firm_name) = LOWER(l.firm_name)) as firm_cities,
      (SELECT COUNT(*) FROM leads WHERE LOWER(firm_name) = LOWER(l.firm_name)) as firm_size
    FROM leads l WHERE l.firm_name IS NOT NULL AND l.firm_name != ''
    GROUP BY LOWER(l.firm_name)
    HAVING firm_cities >= 2
    ORDER BY firm_cities DESC, firm_size DESC LIMIT 15
  `).all();
  return { firmClusters, cityClusters, practiceNetworks, connectors };
}

// --- Journey Mapping (Batch 33) ---
function getJourneyMapping(limit = 50) {
  const db = getDb();
  // Stage distribution
  const stages = db.prepare(`
    SELECT pipeline_stage, COUNT(*) as count,
      ROUND(AVG(lead_score), 1) as avg_score,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone
    FROM leads GROUP BY pipeline_stage ORDER BY count DESC
  `).all();
  // Touchpoint analysis — how many enrichment sources each lead passed through
  const touchpoints = db.prepare(`
    SELECT
      CASE
        WHEN email IS NOT NULL AND email != '' AND phone IS NOT NULL AND phone != '' AND website IS NOT NULL AND website != '' THEN 'fully_enriched'
        WHEN email IS NOT NULL AND email != '' AND (phone IS NOT NULL AND phone != '' OR website IS NOT NULL AND website != '') THEN 'mostly_enriched'
        WHEN email IS NOT NULL AND email != '' OR phone IS NOT NULL AND phone != '' OR website IS NOT NULL AND website != '' THEN 'partially_enriched'
        ELSE 'raw'
      END as enrichment_stage,
      COUNT(*) as count
    FROM leads GROUP BY enrichment_stage
  `).all();
  // Journey by source
  const bySource = db.prepare(`
    SELECT primary_source, pipeline_stage, COUNT(*) as count
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source, pipeline_stage
    ORDER BY primary_source, count DESC
  `).all();
  // Time in stage (based on updated_at vs created_at)
  const avgTime = db.prepare(`
    SELECT pipeline_stage,
      ROUND(AVG(JULIANDAY(COALESCE(updated_at, created_at)) - JULIANDAY(created_at)), 1) as avg_days_in_stage,
      COUNT(*) as count
    FROM leads GROUP BY pipeline_stage
  `).all();
  // Recent transitions
  let transitions = [];
  try {
    transitions = db.prepare(`
      SELECT new_stage, COUNT(*) as count,
        ROUND(AVG(JULIANDAY('now') - JULIANDAY(transitioned_at)), 1) as avg_days_ago
      FROM stage_transitions
      WHERE transitioned_at >= DATE('now', '-30 days')
      GROUP BY new_stage ORDER BY count DESC
    `).all();
  } catch(e) {}
  return { stages, touchpoints, bySource, avgTime, transitions };
}

// --- Scoring Audit (Batch 33) ---
function getScoringAudit(limit = 40) {
  const db = getDb();
  // Score breakdown components
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, firm_name, lead_score, pipeline_stage,
      CASE WHEN email IS NOT NULL AND email != '' THEN 20 ELSE 0 END as email_pts,
      CASE WHEN phone IS NOT NULL AND phone != '' THEN 15 ELSE 0 END as phone_pts,
      CASE WHEN website IS NOT NULL AND website != '' THEN 10 ELSE 0 END as website_pts,
      CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 10 ELSE 0 END as firm_pts,
      CASE WHEN practice_area IS NOT NULL AND practice_area != '' THEN 5 ELSE 0 END as practice_pts,
      CASE WHEN bar_number IS NOT NULL AND bar_number != '' THEN 5 ELSE 0 END as bar_pts,
      CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 5 ELSE 0 END as linkedin_pts,
      CASE WHEN bio IS NOT NULL AND bio != '' THEN 5 ELSE 0 END as bio_pts
    FROM leads ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  // Score distribution histogram
  const histogram = db.prepare(`
    SELECT
      CASE
        WHEN lead_score >= 90 THEN '90-100'
        WHEN lead_score >= 80 THEN '80-89'
        WHEN lead_score >= 70 THEN '70-79'
        WHEN lead_score >= 60 THEN '60-69'
        WHEN lead_score >= 50 THEN '50-59'
        WHEN lead_score >= 40 THEN '40-49'
        WHEN lead_score >= 30 THEN '30-39'
        WHEN lead_score >= 20 THEN '20-29'
        WHEN lead_score >= 10 THEN '10-19'
        ELSE '0-9'
      END as bucket,
      COUNT(*) as count
    FROM leads GROUP BY bucket ORDER BY bucket DESC
  `).all();
  // Top factors across all leads
  const factors = db.prepare(`
    SELECT
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as have_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as have_phone,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as have_website,
      SUM(CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 1 ELSE 0 END) as have_firm,
      SUM(CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 1 ELSE 0 END) as have_linkedin,
      SUM(CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) as have_bio,
      COUNT(*) as total
    FROM leads
  `).get();
  // Score changes (via audit log if available)
  let recentChanges = [];
  try {
    recentChanges = db.prepare(`
      SELECT entity_id, old_value, new_value, created_at
      FROM audit_log WHERE action = 'score_change'
      ORDER BY created_at DESC LIMIT 20
    `).all();
  } catch(e) {}
  return { leads, histogram, factors, recentChanges };
}

// --- Geographic Expansion (Batch 33) ---
function getGeoExpansion() {
  const db = getDb();
  // Current coverage density by state
  const stateCoverage = db.prepare(`
    SELECT state, COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(DISTINCT city) as cities_covered
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY total DESC
  `).all();
  // City density — top cities by lead count
  const topCities = db.prepare(`
    SELECT city, state, COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE city IS NOT NULL AND city != ''
    GROUP BY city, state ORDER BY total DESC LIMIT 30
  `).all();
  // Underserved states — states with few leads relative to others
  const underserved = db.prepare(`
    SELECT state, COUNT(*) as total, COUNT(DISTINCT city) as cities
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state HAVING total < 50
    ORDER BY total ASC LIMIT 15
  `).all();
  // Practice area gaps — which practice areas have low coverage in which states
  const practiceGaps = db.prepare(`
    SELECT state, practice_area, COUNT(*) as count
    FROM leads
    WHERE state IS NOT NULL AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY state, practice_area
    ORDER BY state, count DESC
  `).all();
  // Success patterns — states with highest email/phone rates
  const successPatterns = db.prepare(`
    SELECT state, COUNT(*) as total,
      ROUND(100.0 * SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as email_rate,
      ROUND(100.0 * SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as phone_rate
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state HAVING total >= 10
    ORDER BY email_rate DESC LIMIT 20
  `).all();
  return { stateCoverage, topCities, underserved, practiceGaps, successPatterns };
}

// --- Freshness Alerts (Batch 33) ---
function getFreshnessAlerts() {
  const db = getDb();
  // Staleness buckets
  const staleness = db.prepare(`
    SELECT
      CASE
        WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) > 90 THEN 'critical_90d+'
        WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) > 60 THEN 'warning_60d+'
        WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) > 30 THEN 'aging_30d+'
        ELSE 'fresh'
      END as freshness,
      COUNT(*) as count
    FROM leads GROUP BY freshness
  `).all();
  // Per-source last scraped
  const sourceAge = db.prepare(`
    SELECT primary_source,
      MAX(COALESCE(updated_at, created_at)) as last_updated,
      ROUND(JULIANDAY('now') - JULIANDAY(MAX(COALESCE(updated_at, created_at))), 0) as days_ago,
      COUNT(*) as total
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY days_ago DESC
  `).all();
  // Re-scrape priorities — states with oldest data
  const rescrapePriority = db.prepare(`
    SELECT state, COUNT(*) as total,
      ROUND(AVG(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at))), 0) as avg_age_days,
      MAX(COALESCE(updated_at, created_at)) as newest,
      MIN(COALESCE(updated_at, created_at)) as oldest
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY avg_age_days DESC LIMIT 20
  `).all();
  // Data decay — leads that may have stale contact info
  const decayRisk = db.prepare(`
    SELECT id, first_name, last_name, city, state, email, phone,
      COALESCE(updated_at, created_at) as last_updated,
      ROUND(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)), 0) as days_stale
    FROM leads
    WHERE JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) > 60
    ORDER BY days_stale DESC LIMIT 20
  `).all();
  return { staleness, sourceAge, rescrapePriority, decayRisk };
}

// --- Merge Candidates (Batch 34) ---
function getMergeCandidates(limit = 30) {
  const db = getDb();
  // Same name + city pairs
  const nameDupes = db.prepare(`
    SELECT l1.id as id1, l2.id as id2,
      l1.first_name, l1.last_name, l1.city, l1.state,
      l1.email as email1, l2.email as email2,
      l1.phone as phone1, l2.phone as phone2,
      l1.firm_name as firm1, l2.firm_name as firm2,
      l1.lead_score as score1, l2.lead_score as score2,
      'name_city' as match_type
    FROM leads l1
    JOIN leads l2 ON l1.id < l2.id
      AND LOWER(l1.first_name) = LOWER(l2.first_name)
      AND LOWER(l1.last_name) = LOWER(l2.last_name)
      AND LOWER(l1.city) = LOWER(l2.city)
    LIMIT ?
  `).all(limit);
  // Same email (non-empty)
  const emailDupes = db.prepare(`
    SELECT l1.id as id1, l2.id as id2,
      l1.first_name as fn1, l1.last_name as ln1,
      l2.first_name as fn2, l2.last_name as ln2,
      l1.email, l1.city as city1, l2.city as city2,
      l1.lead_score as score1, l2.lead_score as score2,
      'email' as match_type
    FROM leads l1
    JOIN leads l2 ON l1.id < l2.id
      AND LOWER(l1.email) = LOWER(l2.email)
    WHERE l1.email IS NOT NULL AND l1.email != ''
    LIMIT ?
  `).all(limit);
  const summary = db.prepare(`
    SELECT COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads
  `).get();
  return { nameDupes, emailDupes, summary };
}

function getMergePreview(id1, id2) {
  const db = getDb();
  const l1 = db.prepare('SELECT * FROM leads WHERE id = ?').get(id1);
  const l2 = db.prepare('SELECT * FROM leads WHERE id = ?').get(id2);
  if (!l1 || !l2) return { error: 'Lead not found' };
  // Show which fields differ
  const fields = ['first_name','last_name','email','phone','website','firm_name','city','state','practice_area','bar_number','linkedin_url','bio','title'];
  const comparison = fields.map(f => ({
    field: f, val1: l1[f] || '', val2: l2[f] || '',
    differs: (l1[f]||'') !== (l2[f]||''),
    recommendation: (l1[f] && !l2[f]) ? 'keep_1' : (!l1[f] && l2[f]) ? 'keep_2' : (l1[f]===l2[f]) ? 'same' : 'manual'
  }));
  return { lead1: l1, lead2: l2, comparison };
}

function executeMerge(keepId, mergeId) {
  const db = getDb();
  const keep = db.prepare('SELECT * FROM leads WHERE id = ?').get(keepId);
  const merge = db.prepare('SELECT * FROM leads WHERE id = ?').get(mergeId);
  if (!keep || !merge) return { error: 'Lead not found' };
  // Fill empty fields from merge into keep
  const fields = ['email','phone','website','firm_name','practice_area','bar_number','linkedin_url','bio','title'];
  const updates = {};
  for (const f of fields) {
    if ((!keep[f] || keep[f] === '') && merge[f] && merge[f] !== '') {
      updates[f] = merge[f];
    }
  }
  if (Object.keys(updates).length > 0) {
    const setClauses = Object.keys(updates).map(k => `${k} = ?`).join(', ');
    db.prepare(`UPDATE leads SET ${setClauses}, updated_at = datetime('now') WHERE id = ?`).run(...Object.values(updates), keepId);
  }
  // Delete the merged lead
  db.prepare('DELETE FROM leads WHERE id = ?').run(mergeId);
  return { kept: keepId, deleted: mergeId, fieldsFilled: Object.keys(updates) };
}

// --- Outreach Analytics (Batch 34) ---
function getOutreachAnalytics() {
  const db = getDb();
  // Contact attempt stats
  let contactStats = { total: 0, byMethod: [], byDayOfWeek: [] };
  try {
    contactStats.total = db.prepare('SELECT COUNT(*) as c FROM contact_log').get().c;
    contactStats.byMethod = db.prepare(`
      SELECT contact_method, COUNT(*) as count,
        SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) as responded,
        ROUND(100.0 * SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) / MAX(COUNT(*), 1), 1) as response_rate
      FROM contact_log GROUP BY contact_method ORDER BY count DESC
    `).all();
    contactStats.byDayOfWeek = db.prepare(`
      SELECT CASE CAST(strftime('%w', contact_at) AS INTEGER)
        WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed'
        WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' END as day_name,
        COUNT(*) as count,
        SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) as responded
      FROM contact_log GROUP BY day_name ORDER BY count DESC
    `).all();
  } catch(e) {}
  // Leads by contact attempt count
  let attemptDistribution = [];
  try {
    attemptDistribution = db.prepare(`
      SELECT
        CASE
          WHEN contact_count = 0 THEN '0 attempts'
          WHEN contact_count = 1 THEN '1 attempt'
          WHEN contact_count BETWEEN 2 AND 3 THEN '2-3 attempts'
          WHEN contact_count BETWEEN 4 AND 5 THEN '4-5 attempts'
          ELSE '6+ attempts'
        END as bucket,
        COUNT(*) as count
      FROM leads GROUP BY bucket ORDER BY bucket
    `).all();
  } catch(e) {
    attemptDistribution = db.prepare(`SELECT 'no data' as bucket, 0 as count`).all();
  }
  // Follow-up effectiveness
  let followUpDecay = [];
  try {
    followUpDecay = db.prepare(`
      SELECT contact_count,
        COUNT(*) as leads_at_count,
        SUM(CASE WHEN pipeline_stage IN ('responded','qualified','converted') THEN 1 ELSE 0 END) as converted
      FROM leads WHERE contact_count > 0
      GROUP BY contact_count ORDER BY contact_count LIMIT 10
    `).all();
  } catch(e) {}
  return { contactStats, attemptDistribution, followUpDecay };
}

// --- ICP Scoring (Batch 34) ---
function getIcpScoring(limit = 40) {
  const db = getDb();
  // City tier scoring
  const cityTiers = { 'New York': 3, 'Los Angeles': 3, 'Chicago': 3, 'Houston': 3, 'Miami': 3, 'San Francisco': 3, 'Dallas': 2, 'Phoenix': 2, 'Philadelphia': 2, 'San Antonio': 2, 'San Diego': 2, 'Austin': 2, 'Denver': 2, 'Seattle': 2, 'Boston': 2, 'Atlanta': 2, 'London': 3, 'Sydney': 3, 'Toronto': 3, 'Melbourne': 2, 'Vancouver': 2 };
  // Score each lead
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, firm_name, practice_area,
      email, phone, website, linkedin_url, bar_number, lead_score
    FROM leads ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  const scored = leads.map(l => {
    let icp = 0;
    if (l.email) icp += 25;
    if (l.phone) icp += 20;
    if (l.website) icp += 10;
    if (l.linkedin_url) icp += 10;
    if (l.firm_name) icp += 10;
    if (l.practice_area) icp += 5;
    if (l.bar_number) icp += 5;
    const tier = cityTiers[l.city] || 1;
    icp += tier * 5;
    return { ...l, icp_score: Math.min(icp, 100), city_tier: tier };
  });
  // Distribution
  const distribution = { excellent: 0, good: 0, fair: 0, poor: 0 };
  const allLeads = db.prepare('SELECT email, phone, website, linkedin_url, firm_name, practice_area, bar_number, city FROM leads').all();
  allLeads.forEach(l => {
    let s = 0;
    if (l.email) s += 25; if (l.phone) s += 20; if (l.website) s += 10; if (l.linkedin_url) s += 10;
    if (l.firm_name) s += 10; if (l.practice_area) s += 5; if (l.bar_number) s += 5;
    s += (cityTiers[l.city] || 1) * 5;
    if (s >= 75) distribution.excellent++;
    else if (s >= 50) distribution.good++;
    else if (s >= 25) distribution.fair++;
    else distribution.poor++;
  });
  return { leads: scored, distribution, total: allLeads.length };
}

// --- Pipeline Velocity (Batch 34) ---
function getPipelineVelocity() {
  const db = getDb();
  // Stage counts and flow
  const stages = db.prepare(`
    SELECT pipeline_stage, COUNT(*) as count,
      ROUND(AVG(lead_score), 1) as avg_score,
      MIN(created_at) as earliest,
      MAX(COALESCE(updated_at, created_at)) as latest
    FROM leads GROUP BY pipeline_stage ORDER BY count DESC
  `).all();
  // Stage transitions if available
  let transitions = [];
  try {
    transitions = db.prepare(`
      SELECT old_stage, new_stage, COUNT(*) as count,
        ROUND(AVG(JULIANDAY(transitioned_at) - JULIANDAY(
          (SELECT MAX(transitioned_at) FROM stage_transitions s2
           WHERE s2.lead_id = stage_transitions.lead_id AND s2.transitioned_at < stage_transitions.transitioned_at)
        )), 1) as avg_days
      FROM stage_transitions
      GROUP BY old_stage, new_stage ORDER BY count DESC LIMIT 20
    `).all();
  } catch(e) {}
  // Bottleneck detection — stages where leads have been longest
  const bottlenecks = db.prepare(`
    SELECT pipeline_stage,
      COUNT(*) as stuck_count,
      ROUND(AVG(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at))), 1) as avg_days_stuck
    FROM leads GROUP BY pipeline_stage
    ORDER BY avg_days_stuck DESC
  `).all();
  // Weekly velocity (new leads per week)
  const weeklyVelocity = db.prepare(`
    SELECT strftime('%Y-W%W', created_at) as week,
      COUNT(*) as new_leads,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads GROUP BY week ORDER BY week DESC LIMIT 12
  `).all();
  return { stages, transitions, bottlenecks, weeklyVelocity };
}

// --- Relationship Graph (Batch 35) ---
function getRelationshipGraph(limit = 30) {
  const db = getDb();
  // Firm connections — people at same firm in different cities
  const firmBridges = db.prepare(`
    SELECT l1.firm_name, l1.city as city1, l2.city as city2,
      COUNT(*) as connections,
      GROUP_CONCAT(DISTINCT l1.first_name || ' ' || l1.last_name) as people1,
      GROUP_CONCAT(DISTINCT l2.first_name || ' ' || l2.last_name) as people2
    FROM leads l1
    JOIN leads l2 ON LOWER(l1.firm_name) = LOWER(l2.firm_name)
      AND l1.city != l2.city AND l1.id < l2.id
    WHERE l1.firm_name IS NOT NULL AND l1.firm_name != ''
    GROUP BY l1.firm_name, l1.city, l2.city
    ORDER BY connections DESC LIMIT ?
  `).all(limit);
  // Practice area clusters
  const practiceGroups = db.prepare(`
    SELECT practice_area, COUNT(*) as count,
      COUNT(DISTINCT city) as cities,
      COUNT(DISTINCT firm_name) as firms,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area ORDER BY count DESC LIMIT 20
  `).all();
  // City pairs — cities that share the most firms
  const cityPairs = db.prepare(`
    SELECT l1.city as city1, l2.city as city2,
      COUNT(DISTINCT LOWER(l1.firm_name)) as shared_firms
    FROM leads l1
    JOIN leads l2 ON LOWER(l1.firm_name) = LOWER(l2.firm_name)
      AND l1.city < l2.city
    WHERE l1.firm_name IS NOT NULL AND l1.firm_name != ''
      AND l1.city IS NOT NULL AND l2.city IS NOT NULL
    GROUP BY l1.city, l2.city
    HAVING shared_firms >= 2
    ORDER BY shared_firms DESC LIMIT 15
  `).all();
  return { firmBridges, practiceGroups, cityPairs };
}

// --- Enrichment ROI (Batch 35) ---
function getEnrichmentROI() {
  const db = getDb();
  // Fields filled by source
  const bySource = db.prepare(`
    SELECT primary_source,
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as websites,
      SUM(CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 1 ELSE 0 END) as linkedins,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY total DESC
  `).all();
  // Calculate enrichment efficiency
  const efficiency = bySource.map(s => ({
    source: s.primary_source,
    total: s.total,
    fieldsPerLead: Math.round(((s.emails + s.phones + s.websites + s.linkedins) / Math.max(s.total, 1)) * 100) / 100,
    emailRate: Math.round((s.emails / Math.max(s.total, 1)) * 1000) / 10,
    phoneRate: Math.round((s.phones / Math.max(s.total, 1)) * 1000) / 10,
    avg_score: s.avg_score
  }));
  // Overall stats
  const overall = db.prepare(`
    SELECT COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as websites,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads
  `).get();
  return { bySource, efficiency, overall };
}

// --- Engagement Prediction (Batch 35) ---
function getEngagementPrediction(limit = 40) {
  const db = getDb();
  const cityTiers = { 'New York': 3, 'Los Angeles': 3, 'Chicago': 3, 'Houston': 3, 'Miami': 3, 'San Francisco': 3, 'Dallas': 2, 'Phoenix': 2, 'Philadelphia': 2, 'Austin': 2, 'Denver': 2, 'Seattle': 2, 'Boston': 2, 'Atlanta': 2, 'London': 3, 'Sydney': 3, 'Toronto': 3 };
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, firm_name, practice_area,
      email, phone, website, linkedin_url, lead_score, pipeline_stage
    FROM leads ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  const scored = leads.map(l => {
    let prob = 0;
    // Data completeness drives engagement probability
    if (l.email) prob += 30;
    if (l.phone) prob += 20;
    if (l.website) prob += 10;
    if (l.linkedin_url) prob += 10;
    // City tier bonus
    const tier = cityTiers[l.city] || 1;
    prob += tier * 5;
    // Firm presence bonus
    if (l.firm_name && l.firm_name.length > 3) prob += 5;
    // Practice area boost
    if (l.practice_area) prob += 5;
    // Stage bonus
    if (l.pipeline_stage === 'qualified') prob += 10;
    if (l.pipeline_stage === 'contacted') prob += 5;
    return { ...l, engagement_prob: Math.min(prob, 100), city_tier: tier };
  });
  scored.sort((a, b) => b.engagement_prob - a.engagement_prob);
  // Distribution
  const dist = { high: 0, medium: 0, low: 0 };
  scored.forEach(s => {
    if (s.engagement_prob >= 60) dist.high++;
    else if (s.engagement_prob >= 35) dist.medium++;
    else dist.low++;
  });
  return { leads: scored, distribution: dist };
}

// --- Campaign Performance (Batch 35) ---
function getCampaignPerformance() {
  const db = getDb();
  let campaigns = [];
  try {
    campaigns = db.prepare(`
      SELECT c.id, c.name, c.status, c.target_count, c.created_at,
        (SELECT COUNT(*) FROM campaign_leads cl WHERE cl.campaign_id = c.id) as assigned,
        (SELECT COUNT(*) FROM campaign_leads cl WHERE cl.campaign_id = c.id AND cl.status = 'converted') as converted,
        (SELECT COUNT(*) FROM campaign_leads cl WHERE cl.campaign_id = c.id AND cl.status = 'responded') as responded
      FROM campaigns c ORDER BY c.created_at DESC LIMIT 20
    `).all();
  } catch(e) {}
  // Best performing practice areas
  const byPractice = db.prepare(`
    SELECT practice_area, COUNT(*) as total,
      SUM(CASE WHEN pipeline_stage IN ('qualified','converted') THEN 1 ELSE 0 END) as converted,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area ORDER BY avg_score DESC LIMIT 15
  `).all();
  // Top converting cities
  const byCityPerf = db.prepare(`
    SELECT city, state, COUNT(*) as total,
      ROUND(AVG(lead_score), 1) as avg_score,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email
    FROM leads WHERE city IS NOT NULL AND city != ''
    GROUP BY city, state ORDER BY avg_score DESC LIMIT 15
  `).all();
  // Source comparison
  const bySourcePerf = db.prepare(`
    SELECT primary_source, COUNT(*) as total,
      ROUND(AVG(lead_score), 1) as avg_score,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY avg_score DESC
  `).all();
  return { campaigns, byPractice, byCityPerf, bySourcePerf };
}

// --- Prioritization Matrix (Batch 36) ---
function getPrioritizationMatrix(limit = 50) {
  const db = getDb();
  const cityTiers = { 'New York': 3, 'Los Angeles': 3, 'Chicago': 3, 'Houston': 3, 'Miami': 3, 'San Francisco': 3, 'Dallas': 2, 'Phoenix': 2, 'Philadelphia': 2, 'Austin': 2, 'Denver': 2, 'Seattle': 2, 'Boston': 2, 'Atlanta': 2, 'London': 3, 'Sydney': 3, 'Toronto': 3, 'Melbourne': 2 };
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, firm_name, practice_area,
      email, phone, website, linkedin_url, lead_score, pipeline_stage,
      created_at, updated_at
    FROM leads ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  const scored = leads.map(l => {
    // Recency weight (0-25)
    const daysSince = Math.max(0, (Date.now() - new Date(l.updated_at || l.created_at).getTime()) / 86400000);
    const recency = Math.max(0, 25 - daysSince);
    // Completeness weight (0-30)
    let completeness = 0;
    if (l.email) completeness += 8;
    if (l.phone) completeness += 6;
    if (l.website) completeness += 5;
    if (l.linkedin_url) completeness += 4;
    if (l.firm_name) completeness += 4;
    if (l.practice_area) completeness += 3;
    // Market value weight (0-25)
    const tier = cityTiers[l.city] || 1;
    const marketValue = tier * 8;
    // Engagement potential (0-20)
    let engagement = 0;
    if (l.email && l.phone) engagement += 10;
    else if (l.email || l.phone) engagement += 5;
    if (l.pipeline_stage === 'qualified') engagement += 10;
    else if (l.pipeline_stage === 'contacted') engagement += 5;
    const total = Math.round(recency + completeness + marketValue + engagement);
    const quadrant = (completeness >= 20 && marketValue >= 16) ? 'star' :
      (completeness >= 20) ? 'nurture' : (marketValue >= 16) ? 'enrich' : 'deprioritize';
    return { ...l, priority_score: Math.min(total, 100), recency: Math.round(recency), completeness, marketValue, engagement, quadrant, city_tier: tier };
  });
  scored.sort((a, b) => b.priority_score - a.priority_score);
  const quadrants = { star: 0, nurture: 0, enrich: 0, deprioritize: 0 };
  scored.forEach(s => quadrants[s.quadrant]++);
  return { leads: scored, quadrants };
}

// --- Firm Aggregation (Batch 36) ---
function getFirmAggregation(limit = 30) {
  const db = getDb();
  const firms = db.prepare(`
    SELECT firm_name, COUNT(*) as headcount,
      COUNT(DISTINCT city) as offices,
      COUNT(DISTINCT state) as states,
      COUNT(DISTINCT practice_area) as practice_areas,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as with_website,
      ROUND(AVG(lead_score), 1) as avg_score,
      GROUP_CONCAT(DISTINCT city) as cities
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_name != 'Solo Practitioner'
    GROUP BY LOWER(firm_name)
    HAVING headcount >= 2
    ORDER BY headcount DESC LIMIT ?
  `).all(limit);
  const totals = db.prepare(`
    SELECT COUNT(DISTINCT LOWER(firm_name)) as unique_firms,
      COUNT(*) as total_leads,
      SUM(CASE WHEN firm_name IS NULL OR firm_name = '' THEN 1 ELSE 0 END) as no_firm
    FROM leads
  `).get();
  return { firms, totals };
}

// --- Improvement Recommendations (Batch 36) ---
function getImprovementRecs() {
  const db = getDb();
  // Field gap analysis
  const gaps = db.prepare(`
    SELECT
      SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_email,
      SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phone,
      SUM(CASE WHEN website IS NULL OR website = '' THEN 1 ELSE 0 END) as missing_website,
      SUM(CASE WHEN firm_name IS NULL OR firm_name = '' THEN 1 ELSE 0 END) as missing_firm,
      SUM(CASE WHEN linkedin_url IS NULL OR linkedin_url = '' THEN 1 ELSE 0 END) as missing_linkedin,
      SUM(CASE WHEN practice_area IS NULL OR practice_area = '' THEN 1 ELSE 0 END) as missing_practice,
      COUNT(*) as total
    FROM leads
  `).get();
  // Quick wins — leads needing just 1 field to reach score >= 50
  const quickWins = db.prepare(`
    SELECT id, first_name, last_name, city, state, lead_score,
      CASE
        WHEN email IS NULL OR email = '' THEN 'email'
        WHEN phone IS NULL OR phone = '' THEN 'phone'
        WHEN website IS NULL OR website = '' THEN 'website'
        WHEN firm_name IS NULL OR firm_name = '' THEN 'firm'
        ELSE 'other'
      END as missing_field
    FROM leads
    WHERE lead_score BETWEEN 30 AND 49
      AND (
        (email IS NOT NULL AND email != '' AND phone IS NOT NULL AND phone != '') OR
        (email IS NOT NULL AND email != '' AND website IS NOT NULL AND website != '') OR
        (phone IS NOT NULL AND phone != '' AND website IS NOT NULL AND website != '')
      )
    ORDER BY lead_score DESC LIMIT 20
  `).all();
  // Enrichment action estimates
  const actions = [];
  if (gaps.missing_email > 0) actions.push({ action: 'Website email crawl', potential: Math.min(gaps.missing_email, db.prepare("SELECT COUNT(*) as c FROM leads WHERE (email IS NULL OR email = '') AND website IS NOT NULL AND website != ''").get().c), field: 'email' });
  if (gaps.missing_phone > 0) actions.push({ action: 'Martindale cross-ref', potential: Math.min(gaps.missing_phone, Math.round(gaps.missing_phone * 0.3)), field: 'phone' });
  if (gaps.missing_website > 0) actions.push({ action: 'Profile page fetch', potential: Math.round(gaps.missing_website * 0.4), field: 'website' });
  return { gaps, quickWins, actions };
}

// --- Lifecycle Funnel (Batch 36) ---
function getLifecycleFunnel() {
  const db = getDb();
  // Funnel stages
  const funnel = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' OR phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as enriched,
      SUM(CASE WHEN pipeline_stage IN ('contacted','responded','qualified','converted') THEN 1 ELSE 0 END) as contacted,
      SUM(CASE WHEN pipeline_stage IN ('responded','qualified','converted') THEN 1 ELSE 0 END) as responded,
      SUM(CASE WHEN pipeline_stage IN ('qualified','converted') THEN 1 ELSE 0 END) as qualified,
      SUM(CASE WHEN pipeline_stage = 'converted' THEN 1 ELSE 0 END) as converted
    FROM leads
  `).get();
  // Drop-off rates
  const rates = {
    raw_to_enriched: funnel.total > 0 ? Math.round((funnel.enriched / funnel.total) * 1000) / 10 : 0,
    enriched_to_contacted: funnel.enriched > 0 ? Math.round((funnel.contacted / funnel.enriched) * 1000) / 10 : 0,
    contacted_to_responded: funnel.contacted > 0 ? Math.round((funnel.responded / funnel.contacted) * 1000) / 10 : 0,
    responded_to_qualified: funnel.responded > 0 ? Math.round((funnel.qualified / funnel.responded) * 1000) / 10 : 0,
    qualified_to_converted: funnel.qualified > 0 ? Math.round((funnel.converted / funnel.qualified) * 1000) / 10 : 0
  };
  // Conversion factors — what do converted leads have in common?
  const conversionFactors = db.prepare(`
    SELECT
      ROUND(AVG(CASE WHEN email IS NOT NULL AND email != '' THEN 1.0 ELSE 0 END) * 100, 1) as email_rate,
      ROUND(AVG(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1.0 ELSE 0 END) * 100, 1) as phone_rate,
      ROUND(AVG(CASE WHEN website IS NOT NULL AND website != '' THEN 1.0 ELSE 0 END) * 100, 1) as website_rate,
      ROUND(AVG(lead_score), 1) as avg_score,
      COUNT(*) as count
    FROM leads WHERE pipeline_stage IN ('qualified','converted')
  `).get();
  // By source progression
  const bySource = db.prepare(`
    SELECT primary_source, COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' OR phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as enriched,
      SUM(CASE WHEN pipeline_stage != 'new' THEN 1 ELSE 0 END) as progressed
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY total DESC LIMIT 15
  `).all();
  return { funnel, rates, conversionFactors, bySource };
}

// --- Cadence Optimizer (Batch 37) ---
function getCadenceOptimizer() {
  const db = getDb();
  // Contact frequency analysis
  let frequencyAnalysis = [];
  try {
    frequencyAnalysis = db.prepare(`
      SELECT contact_count,
        COUNT(*) as leads_at_count,
        ROUND(AVG(lead_score), 1) as avg_score,
        SUM(CASE WHEN pipeline_stage IN ('responded','qualified','converted') THEN 1 ELSE 0 END) as positive_outcome
      FROM leads WHERE contact_count > 0
      GROUP BY contact_count ORDER BY contact_count LIMIT 10
    `).all();
  } catch(e) {}
  // Day of week performance
  let dayPerformance = [];
  try {
    dayPerformance = db.prepare(`
      SELECT CASE CAST(strftime('%w', contact_at) AS INTEGER)
        WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday' END as day_name,
        CAST(strftime('%w', contact_at) AS INTEGER) as day_num,
        COUNT(*) as contacts,
        SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) as responses
      FROM contact_log GROUP BY day_num ORDER BY day_num
    `).all();
  } catch(e) {}
  // Leads needing contact
  let needsContact = { never_contacted: 0, needs_followup: 0, over_contacted: 0, total: 0 };
  try {
    needsContact = db.prepare(`
      SELECT
        SUM(CASE WHEN contact_count = 0 THEN 1 ELSE 0 END) as never_contacted,
        SUM(CASE WHEN contact_count BETWEEN 1 AND 2 THEN 1 ELSE 0 END) as needs_followup,
        SUM(CASE WHEN contact_count >= 5 THEN 1 ELSE 0 END) as over_contacted,
        COUNT(*) as total
      FROM leads
    `).get();
  } catch(e) {
    needsContact = db.prepare('SELECT COUNT(*) as total, COUNT(*) as never_contacted, 0 as needs_followup, 0 as over_contacted FROM leads').get();
  }
  // Recommended cadence
  const recommendations = [
    { rule: 'First contact within 24h of enrichment', priority: 'high' },
    { rule: 'Follow up 3 days after first contact', priority: 'high' },
    { rule: 'Max 5 attempts per lead before cooling off', priority: 'medium' },
    { rule: 'Cool off 30 days after 5th attempt', priority: 'medium' }
  ];
  return { frequencyAnalysis, dayPerformance, needsContact, recommendations };
}

// --- Scoring Calibration (Batch 37) ---
function getScoringCalibration() {
  const db = getDb();
  // Score distribution vs ideal
  const distribution = db.prepare(`
    SELECT
      CASE
        WHEN lead_score >= 90 THEN '90-100'
        WHEN lead_score >= 80 THEN '80-89'
        WHEN lead_score >= 70 THEN '70-79'
        WHEN lead_score >= 60 THEN '60-69'
        WHEN lead_score >= 50 THEN '50-59'
        WHEN lead_score >= 40 THEN '40-49'
        WHEN lead_score >= 30 THEN '30-39'
        WHEN lead_score >= 20 THEN '20-29'
        WHEN lead_score >= 10 THEN '10-19'
        ELSE '0-9'
      END as bucket,
      COUNT(*) as count
    FROM leads GROUP BY bucket ORDER BY bucket
  `).all();
  const total = distribution.reduce((s, d) => s + d.count, 0);
  // Score stats
  const stats = db.prepare(`
    SELECT ROUND(AVG(lead_score), 1) as mean,
      MIN(lead_score) as min_score, MAX(lead_score) as max_score,
      COUNT(*) as total
    FROM leads
  `).get();
  // Median calculation
  const median = db.prepare(`
    SELECT lead_score FROM leads ORDER BY lead_score
    LIMIT 1 OFFSET ?
  `).get(Math.floor((stats.total || 1) / 2));
  stats.median = median ? median.lead_score : 0;
  // Score vs outcome correlation
  const scoreOutcome = db.prepare(`
    SELECT
      CASE WHEN lead_score >= 50 THEN 'high' ELSE 'low' END as score_group,
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as with_phone,
      SUM(CASE WHEN pipeline_stage != 'new' THEN 1 ELSE 0 END) as progressed
    FROM leads GROUP BY score_group
  `).all();
  // Inflation check
  const lowScorePercent = Math.round(distribution.filter(d => parseInt(d.bucket) < 30).reduce((s, d) => s + d.count, 0) / Math.max(total, 1) * 100);
  const calibration = lowScorePercent > 80 ? 'deflated' : lowScorePercent < 20 ? 'inflated' : 'balanced';
  return { distribution, stats, scoreOutcome, calibration, lowScorePercent };
}

// --- Practice Market Size (Batch 37) ---
function getPracticeMarketSize() {
  const db = getDb();
  // Practice area coverage
  const practiceAreas = db.prepare(`
    SELECT practice_area, COUNT(*) as total,
      COUNT(DISTINCT city) as cities,
      COUNT(DISTINCT state) as states,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    GROUP BY practice_area ORDER BY total DESC LIMIT 25
  `).all();
  // City x practice matrix (top combinations)
  const cityPractice = db.prepare(`
    SELECT city, practice_area, COUNT(*) as count
    FROM leads
    WHERE city IS NOT NULL AND city != '' AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY city, practice_area
    HAVING count >= 3
    ORDER BY count DESC LIMIT 30
  `).all();
  // Market gaps — cities with single practice area representation
  const gaps = db.prepare(`
    SELECT city, state, COUNT(DISTINCT practice_area) as practice_count, COUNT(*) as total
    FROM leads
    WHERE city IS NOT NULL AND practice_area IS NOT NULL AND practice_area != ''
    GROUP BY city, state
    HAVING practice_count = 1 AND total >= 5
    ORDER BY total DESC LIMIT 15
  `).all();
  // Overall market stats
  const overall = db.prepare(`
    SELECT COUNT(DISTINCT practice_area) as unique_practices,
      COUNT(DISTINCT city) as unique_cities,
      COUNT(*) as total
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
  `).get();
  return { practiceAreas, cityPractice, gaps, overall };
}

// --- Pipeline Health (Batch 37) ---
function getPipelineHealth() {
  const db = getDb();
  // Source quality metrics
  const sourceHealth = db.prepare(`
    SELECT primary_source,
      COUNT(*) as total,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
      SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as websites,
      SUM(CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 1 ELSE 0 END) as firms,
      ROUND(AVG(lead_score), 1) as avg_score,
      ROUND(AVG(
        (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
        (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
        (CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 1 ELSE 0 END) +
        (CASE WHEN practice_area IS NOT NULL AND practice_area != '' THEN 1 ELSE 0 END)
      ), 2) as avg_fields_per_lead,
      MAX(COALESCE(updated_at, created_at)) as last_active
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY total DESC
  `).all();
  // Data quality overview
  const quality = db.prepare(`
    SELECT
      ROUND(100.0 * SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as email_rate,
      ROUND(100.0 * SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as phone_rate,
      ROUND(100.0 * SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as website_rate,
      ROUND(100.0 * SUM(CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 1 ELSE 0 END) / COUNT(*), 1) as firm_rate,
      COUNT(*) as total
    FROM leads
  `).get();
  // Degrading sources — sources where recent leads have lower quality
  const degrading = sourceHealth.filter(s => s.avg_fields_per_lead < 1.5 && s.total >= 10)
    .map(s => ({ source: s.primary_source, fields_per_lead: s.avg_fields_per_lead, total: s.total }));
  return { sourceHealth, quality, degrading };
}

// --- Affinity Scoring (Batch 38) ---
function getAffinityScoring(limit = 40) {
  const db = getDb();
  // Top practice-city combinations
  const topCombos = db.prepare(`
    SELECT practice_area, city, state, COUNT(*) as count,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads
    WHERE practice_area IS NOT NULL AND practice_area != '' AND city IS NOT NULL AND city != ''
    GROUP BY practice_area, city, state
    HAVING count >= 2
    ORDER BY count DESC LIMIT 20
  `).all();
  // Score leads by practice-city affinity
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, firm_name, practice_area,
      email, phone, website, lead_score
    FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''
    ORDER BY lead_score DESC LIMIT ?
  `).all(limit);
  // Calculate affinity based on how populated their practice+city combo is
  const comboCounts = {};
  topCombos.forEach(c => { comboCounts[`${c.practice_area}|${c.city}`] = c.count; });
  const scored = leads.map(l => {
    const key = `${l.practice_area}|${l.city}`;
    const clusterSize = comboCounts[key] || 1;
    const affinity = Math.min(Math.round(clusterSize * 10 + l.lead_score * 0.5), 100);
    return { ...l, affinity_score: affinity, cluster_size: clusterSize };
  });
  scored.sort((a, b) => b.affinity_score - a.affinity_score);
  return { leads: scored, topCombos };
}

// --- Scraper Gaps (Batch 38) ---
function getScraperGaps() {
  const db = getDb();
  // Coverage by source
  const sourceCoverage = db.prepare(`
    SELECT primary_source, COUNT(*) as total,
      COUNT(DISTINCT city) as cities,
      COUNT(DISTINCT state) as states,
      SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as emails,
      SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as phones,
      ROUND(AVG(lead_score), 1) as avg_score,
      ROUND(AVG(
        (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
        (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
        (CASE WHEN firm_name IS NOT NULL AND firm_name != '' THEN 1 ELSE 0 END)
      ), 2) as quality_score
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY quality_score DESC
  `).all();
  // Low quality sources
  const lowQuality = sourceCoverage.filter(s => s.quality_score < 1.5 && s.total >= 10);
  // State coverage gaps
  const stateCoverage = db.prepare(`
    SELECT state, COUNT(*) as total, COUNT(DISTINCT city) as cities,
      COUNT(DISTINCT primary_source) as sources
    FROM leads WHERE state IS NOT NULL AND state != ''
    GROUP BY state ORDER BY total ASC LIMIT 20
  `).all();
  // Recommendations
  const recommendations = [];
  if (lowQuality.length > 0) {
    recommendations.push({ type: 'improve', message: `${lowQuality.length} scrapers producing low-quality data`, sources: lowQuality.map(s => s.primary_source) });
  }
  const smallStates = stateCoverage.filter(s => s.total < 20);
  if (smallStates.length > 0) {
    recommendations.push({ type: 'expand', message: `${smallStates.length} states with fewer than 20 leads`, states: smallStates.map(s => s.state) });
  }
  return { sourceCoverage, lowQuality, stateCoverage, recommendations };
}

// --- Freshness Index (Batch 38) ---
function getFreshnessIndex(limit = 40) {
  const db = getDb();
  const leads = db.prepare(`
    SELECT id, first_name, last_name, city, state, primary_source, lead_score,
      COALESCE(updated_at, created_at) as last_updated,
      ROUND(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)), 1) as age_days
    FROM leads ORDER BY age_days DESC LIMIT ?
  `).all(limit);
  // Freshness score: 100 = today, 0 = 90+ days old
  const scored = leads.map(l => ({
    ...l,
    freshness_index: Math.max(0, Math.round(100 - (l.age_days / 90) * 100))
  }));
  // Freshness by source
  const bySource = db.prepare(`
    SELECT primary_source,
      ROUND(AVG(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at))), 1) as avg_age,
      COUNT(*) as total,
      ROUND(AVG(CASE WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) <= 7 THEN 100
        WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) <= 30 THEN 70
        WHEN JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) <= 60 THEN 40
        ELSE 10 END), 0) as avg_freshness
    FROM leads WHERE primary_source IS NOT NULL
    GROUP BY primary_source ORDER BY avg_freshness ASC
  `).all();
  // Re-scrape batches
  const rescrapeBatches = db.prepare(`
    SELECT state, COUNT(*) as stale_count,
      ROUND(AVG(JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at))), 0) as avg_age
    FROM leads WHERE JULIANDAY('now') - JULIANDAY(COALESCE(updated_at, created_at)) > 30
      AND state IS NOT NULL
    GROUP BY state ORDER BY stale_count DESC LIMIT 15
  `).all();
  return { leads: scored, bySource, rescrapeBatches };
}

// --- Firm Growth (Batch 38) ---
function getFirmGrowth() {
  const db = getDb();
  // Firm headcount with creation dates
  const firms = db.prepare(`
    SELECT firm_name, COUNT(*) as headcount,
      MIN(created_at) as first_seen,
      MAX(created_at) as last_seen,
      COUNT(DISTINCT city) as offices,
      ROUND(AVG(lead_score), 1) as avg_score
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_name != 'Solo Practitioner'
    GROUP BY LOWER(firm_name)
    HAVING headcount >= 3
    ORDER BY headcount DESC LIMIT 25
  `).all();
  // New firms (first appeared recently)
  const newFirms = db.prepare(`
    SELECT firm_name, COUNT(*) as headcount, MIN(created_at) as first_seen
    FROM leads
    WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_name != 'Solo Practitioner'
    GROUP BY LOWER(firm_name)
    HAVING MIN(created_at) >= DATE('now', '-7 days') AND headcount >= 2
    ORDER BY headcount DESC LIMIT 15
  `).all();
  // Firm size distribution
  const sizeDistribution = db.prepare(`
    SELECT
      CASE
        WHEN cnt >= 50 THEN 'large_50+'
        WHEN cnt >= 20 THEN 'medium_20-49'
        WHEN cnt >= 5 THEN 'small_5-19'
        ELSE 'micro_2-4'
      END as size_bucket,
      COUNT(*) as firm_count
    FROM (
      SELECT LOWER(firm_name) as fn, COUNT(*) as cnt
      FROM leads WHERE firm_name IS NOT NULL AND firm_name != '' AND firm_name != 'Solo Practitioner'
      GROUP BY fn HAVING cnt >= 2
    ) GROUP BY size_bucket ORDER BY size_bucket
  `).all();
  return { firms, newFirms, sizeDistribution };
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
  getDailyGrowth,
  getFieldCompleteness,
  updateLead,
  findPotentialDuplicates,
  mergeLeadPair,
  createList,
  getLists,
  getList,
  updateList,
  deleteList,
  addToList,
  removeFromList,
  getLeadLists,
  getScraperHealth,
  getEnrichmentStats,
  getActivityFeed,
  getDistinctSources,
  getStateDetails,
  getTopFirms,
  getDatabaseHealth,
  findSimilarLeads,
  // Changelog
  logChange,
  getLeadChangelog,
  getRecentChanges,
  // Export history
  recordExport,
  getExportHistory,
  // Quality alerts
  runQualityChecks,
  getQualityAlerts,
  resolveAlert,
  getAlertSummary,
  // Pipeline
  getPipelineStats,
  getLeadsByStage,
  moveLeadToStage,
  bulkMoveToStage,
  PIPELINE_STAGES,
  // Schedules
  getSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  markScheduleRun,
  getDueSchedules,
  // Segments
  getSegments,
  createSegment,
  updateSegment,
  deleteSegment,
  querySegment,
  querySegmentLeads,
  // Webhooks
  getWebhooks,
  createWebhook,
  updateWebhook,
  deleteWebhook,
  getWebhooksByEvent,
  logWebhookDelivery,
  getWebhookDeliveries,
  // Notes
  addNote,
  getLeadNotes,
  deleteNote,
  getLeadTimeline,
  // Bulk updates
  bulkUpdateLeads,
  // Scoring rules
  getScoringRules,
  updateScoringRule,
  addScoringRule,
  deleteScoringRule,
  getScoreBreakdown,
  // Email verification
  getVerificationStats,
  bulkImportVerification,
  // Merge
  getMergePreview,
  autoMergeDuplicates,
  // Export templates
  getExportTemplates,
  createExportTemplate,
  deleteExportTemplate,
  // Analytics
  getPipelineFunnel,
  getSourceEffectiveness,
  // Search suggestions
  getSearchSuggestions,
  // Email classification
  classifyEmail,
  classifyAllEmails,
  getEmailClassification,
  // Confidence scoring
  computeConfidenceScore,
  batchComputeConfidence,
  getConfidenceDistribution,
  // Change detection
  detectChanges,
  getRecentChanges2,
  getFirmChanges,
  getLeadChangeHistory,
  // Tag definitions
  getTagDefinitions,
  createTagDefinition,
  updateTagDefinition,
  deleteTagDefinition,
  runAutoTagging,
  // Lead comparison
  compareLeads,
  mergeLeadsWithChoices,
  // Staleness
  getStalenessReport,
  // Import preview
  previewImportMapping,
  // Email validation
  validateEmailSyntax,
  validateEmailMX,
  batchValidateEmails,
  // ICP scoring
  getIcpCriteria,
  addIcpCriterion,
  deleteIcpCriterion,
  updateIcpCriterion,
  computeIcpScore,
  batchComputeIcpScores,
  getIcpDistribution,
  // Saved searches
  getSavedSearches,
  createSavedSearch,
  deleteSavedSearch,
  checkSavedSearchAlerts,
  // Signals
  getRecentAdmissions,
  getAdmissionSignals,
  // Table config
  getTableConfig,
  saveTableConfig,
  // Sequences
  getSequences,
  createSequence,
  addSequenceStep,
  deleteSequence,
  enrollInSequence,
  getSequenceEnrollments,
  renderSequenceStep,
  // Activity tracking
  trackActivity,
  getLeadActivities,
  getEngagementScore,
  getMostEngagedLeads,
  // Firm enrichment
  enrichFirmData,
  getFirmDirectory,
  // Lookalikes
  findLookalikes,
  findBatchLookalikes,
  // Score decay
  applyScoreDecay,
  getDecayPreview,
  // DNC list
  addToDnc,
  removeFromDnc,
  getDncList,
  checkDnc,
  batchCheckDnc,
  // Smart duplicates
  findSmartDuplicates,
  autoMergeDuplicates,
  // Territories
  getTerritories,
  createTerritory,
  deleteTerritory,
  assignLeadsToTerritory,
  getTerritoryLeads,
  // Source attribution
  getSourceAttribution,
  // Single lead enrichment
  getLeadForEnrichment,
  // Intent signals
  getIntentSignals,
  getPracticeAreaTrends,
  // Routing rules
  getRoutingRules,
  createRoutingRule,
  deleteRoutingRule,
  runRoutingRules,
  // Completeness heatmap
  getCompletenessHeatmap,
  getEnrichmentRecommendations,
  // Instantly export
  exportForInstantly,
  // Notes
  addNote,
  getLeadNotes,
  deleteNote,
  togglePinNote,
  getRecentNotes,
  // A/B testing
  getSequenceVariantStats,
  assignVariant,
  // Lead timeline
  getLeadTimeline,
  // Smart lists
  getSmartLists,
  createSmartList,
  deleteSmartList,
  getSmartListLeads,
  // Custom scoring models
  getScoringModels,
  createScoringModel,
  activateScoringModel,
  deleteScoringModel,
  applyCustomScoring,
  // Campaigns
  getCampaigns,
  createCampaign,
  deleteCampaign,
  addLeadsToCampaign,
  getCampaignLeads,
  updateCampaignStatus,
  // Cross-source dedup
  getCrossSourceDuplicates,
  // KPI metrics
  getKpiMetrics,
  // Lead import (Batch 18)
  importLeads,
  getImportFieldMapping,
  // Engagement heatmap (Batch 18)
  getEngagementHeatmap,
  getLeadEngagementSparkline,
  getEngagementTimeline,
  // Bulk actions (Batch 18)
  bulkTagLeads,
  bulkRemoveTag,
  bulkAssignOwner,
  bulkEnrollInCampaign,
  bulkEnrollInSequence,
  getOwners,
  getLeadsByOwner,
  // Lead comparison (Batch 18)
  getLeadComparisonData,
  mergeLeadsWithPicks,
  // Leaderboard (Batch 19)
  getLeaderboard,
  getLeaderboardByState,
  // Automation rules (Batch 19)
  getAutomationRules,
  createAutomationRule,
  deleteAutomationRule,
  toggleAutomationRule,
  runAutomationRules,
  // Data quality (Batch 19)
  getDataQualityReport,
  getDataQualitySummary,
  // Export profiles (Batch 19)
  getExportProfiles,
  createExportProfile,
  deleteExportProfile,
  runExportProfile,
  // Contact timeline (Batch 20)
  getContactTimeline,
  logContact,
  getContactStats,
  getRecentContacts,
  // Warm-up scoring (Batch 20)
  computeWarmUpScore,
  batchComputeWarmUp,
  // Multi-view (Batch 20)
  getKanbanData,
  getCardViewData,
  // Search (Batch 20)
  searchTypeahead,
  getFilterFacets,
  // Enrichment queue (Batch 21)
  getEnrichmentQueueStatus,
  addToEnrichmentQueue,
  processEnrichmentQueue,
  clearEnrichmentQueue,
  // Firm intelligence (Batch 21)
  getFirmIntelligence,
  getFirmDetail,
  // Dedup queue (Batch 21)
  scanForDuplicates,
  getDedupQueue,
  resolveDedupItem,
  getDedupStats,
  // Audit log (Batch 21)
  logAuditEvent,
  getAuditLog,
  getAuditStats,
  exportAuditLog,
  // Lifecycle tracking (Batch 22)
  recordStageTransition,
  getLifecycleAnalytics,
  getLeadLifecycle,
  // Sequence analytics (Batch 22)
  recordSequenceEvent,
  getSequenceAnalytics,
  getAllSequencePerformance,
  // Activity scoring (Batch 22)
  computeActivityScore,
  batchActivityScores,
  getActivityScoreConfig,
  updateActivityScoreConfig,
  // Bulk enrichment (Batch 22)
  createBulkEnrichmentRun,
  getBulkEnrichmentRuns,
  processBulkEnrichmentBatch,
  getBulkEnrichmentDiff,
  // Relationship graph (Batch 23)
  buildRelationshipGraph,
  addRelationship,
  getFirmNetwork,
  // Data freshness (Batch 23)
  recordFieldVerification,
  getFreshnessReport,
  getLeadFreshness,
  // Scoring comparison (Batch 23)
  compareScoringModels,
  getScoringModelRankings,
  // Geographic clustering (Batch 23)
  getGeographicClusters,
  getMarketPenetration,
  // Priority inbox (Batch 24)
  getPriorityInbox,
  getSmartRecommendations,
  // Practice area analytics (Batch 24)
  getPracticeAreaAnalytics,
  // Source ROI (Batch 24)
  getSourceROI,
  getSourceComparison,
  // Compliance (Batch 24)
  recordConsent,
  addOptOut,
  removeOptOut,
  getComplianceDashboard,
  checkEmailCompliance,
  // Journey timeline (Batch 25)
  getLeadJourney,
  // Predictive scoring (Batch 25)
  getPredictiveScores,
  // Team performance (Batch 25)
  getTeamPerformance,
  // Email deliverability (Batch 25)
  getEmailDeliverability,
  // Tagging rules (Batch 26)
  getTagRules, createTagRule, deleteTagRule, toggleTagRule, runTagRules,
  // Nurture cadence (Batch 26)
  getNurtureCadence, getCadenceAnalytics,
  // Custom fields (Batch 26)
  getCustomFieldDefs, createCustomField, deleteCustomField, setCustomFieldValue, getCustomFieldValues, getCustomFieldStats,
  // Score decay (Batch 26)
  getDecayConfig, updateDecayConfig, runScoreDecay, getDecayPreview2,
  // Lookalike finder (Batch 27)
  findLookalikes,
  // Conversion funnel (Batch 27)
  getConversionFunnel,
  // Lead velocity (Batch 27)
  getLeadVelocity,
  // Completeness matrix (Batch 27)
  getCompletenessMatrix,
  // Lead clustering (Batch 28)
  getLeadClusters,
  // A/B test framework (Batch 28)
  getAbTests, createAbTest, assignLeadsToAbTest, recordAbTestOutcome, deleteAbTest,
  // Re-engagement scoring (Batch 28)
  getReengagementLeads,
  // Attribution model (Batch 28)
  getAttributionModel,
  // Response time SLA (Batch 29)
  getResponseTimeSLA,
  // Market saturation (Batch 29)
  getMarketSaturation,
  // Enrichment waterfall (Batch 29)
  getEnrichmentWaterfall,
  // Competitive intelligence (Batch 29)
  getCompetitiveIntelligence,
  // Sequence templates (Batch 30)
  getSequenceTemplates, createSequenceTemplate, updateSequenceTemplate, deleteSequenceTemplate, renderSequenceTemplate,
  // Data quality rules (Batch 30)
  getQualityRules, createQualityRule, deleteQualityRule, runQualityRules,
  // Unified timeline (Batch 30)
  getLeadTimeline,
  // Export scheduler (Batch 30)
  getExportSchedules, createExportSchedule, deleteExportSchedule, runExportSchedule,
  // Propensity model (Batch 31)
  getPropensityScores,
  // Cohort analysis (Batch 31)
  getCohortAnalysis,
  // Channel preferences (Batch 31)
  getChannelPreferences,
  // Jurisdiction benchmarks (Batch 31)
  getJurisdictionBenchmarks,
  // Deal estimation (Batch 32)
  getDealEstimates,
  // Outreach calendar (Batch 32)
  getOutreachCalendar,
  // Risk scoring (Batch 32)
  getRiskScores,
  // Network mapping (Batch 32)
  getNetworkMap,
  // Journey mapping (Batch 33)
  getJourneyMapping,
  // Scoring audit (Batch 33)
  getScoringAudit,
  // Geographic expansion (Batch 33)
  getGeoExpansion,
  // Freshness alerts (Batch 33)
  getFreshnessAlerts,
  // Merge candidates (Batch 34)
  getMergeCandidates, getMergePreview, executeMerge,
  // Outreach analytics (Batch 34)
  getOutreachAnalytics,
  // ICP scoring (Batch 34)
  getIcpScoring,
  // Pipeline velocity (Batch 34)
  getPipelineVelocity,
  // Relationship graph (Batch 35)
  getRelationshipGraph,
  // Enrichment ROI (Batch 35)
  getEnrichmentROI,
  // Engagement prediction (Batch 35)
  getEngagementPrediction,
  // Campaign performance (Batch 35)
  getCampaignPerformance,
  // Prioritization matrix (Batch 36)
  getPrioritizationMatrix,
  // Firm aggregation (Batch 36)
  getFirmAggregation,
  // Improvement recommendations (Batch 36)
  getImprovementRecs,
  // Lifecycle funnel (Batch 36)
  getLifecycleFunnel,
  // Cadence optimizer (Batch 37)
  getCadenceOptimizer,
  // Scoring calibration (Batch 37)
  getScoringCalibration,
  // Practice market size (Batch 37)
  getPracticeMarketSize,
  // Pipeline health (Batch 37)
  getPipelineHealth,
  // Affinity scoring (Batch 38)
  getAffinityScoring,
  // Scraper gaps (Batch 38)
  getScraperGaps,
  // Freshness index (Batch 38)
  getFreshnessIndex,
  // Firm growth (Batch 38)
  getFirmGrowth,
};
