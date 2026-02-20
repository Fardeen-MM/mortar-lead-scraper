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
      primary_source, google_place_id, profile_url, rating
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    lead.first_name || '', lead.last_name || '', lead.firm_name || '',
    lead.city || '', lead.state || '', lead.country || 'US',
    lead.phone || '', lead.email || '', lead.website || '',
    lead.bar_number || '', lead.bar_status || '', lead.admission_date || '',
    lead.practice_area || '', lead.title || '', lead.linkedin_url || '',
    lead.bio || '', lead.education || '', lead.languages || '',
    lead.practice_specialties || '',
    lead.email_source || '', lead.phone_source || '', lead.website_source || '',
    lead.source || lead.primary_source || '',
    lead.google_place_id || lead._googlePlaceId || '',
    lead.profile_url || '',
    lead.rating || lead._rating || null
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

  return {
    total,
    withEmail,
    withPhone,
    withWebsite,
    verified,
    uniqueFirms,
    byState,
    bySource,
    byCountry,
    coverage: {
      email: total > 0 ? Math.round(withEmail / total * 100) : 0,
      phone: total > 0 ? Math.round(withPhone / total * 100) : 0,
      website: total > 0 ? Math.round(withWebsite / total * 100) : 0,
    },
  };
}

/**
 * Search leads.
 */
function searchLeads(query, options = {}) {
  const db = getDb();
  const { state, country, hasEmail, limit = 100, offset = 0 } = options;

  let where = [];
  let params = [];

  if (query) {
    where.push("(first_name LIKE ? OR last_name LIKE ? OR firm_name LIKE ? OR city LIKE ?)");
    const q = `%${query}%`;
    params.push(q, q, q, q);
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

  const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
  params.push(limit, offset);

  return db.prepare(
    `SELECT * FROM leads ${whereClause} ORDER BY updated_at DESC LIMIT ? OFFSET ?`
  ).all(...params);
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
};
