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

  return { merged: true, fieldsRecovered: Object.keys(updates).length };
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
};
