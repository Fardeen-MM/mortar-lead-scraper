/**
 * Domain Pattern Database — Persistent storage for email patterns.
 *
 * Pre-computes and caches email patterns per domain so enrichment
 * becomes instant: pattern lookup + string concat = microseconds per lead.
 *
 * SPEED:
 *   Cold (first time seeing domain): ~0.6s (live verification)
 *   Warm (pattern cached):           ~0.004ms (hash lookup + string concat)
 *   1M leads with warm cache:        ~4 seconds
 *
 * Usage:
 *   const { DomainPatternDB } = require('./lib/domain-pattern-db');
 *   const db = new DomainPatternDB();
 *   db.set('acme.com', { pattern: 'first.last', provider: 'microsoft', confidence: 95 });
 *   const email = db.apply('acme.com', 'John', 'Smith'); // john.smith@acme.com
 */

const path = require('path');
const fs = require('fs');

let Database;
try { Database = require('better-sqlite3'); } catch { Database = null; }

class DomainPatternDB {
  constructor(dbPath) {
    this.dbPath = dbPath || path.join(__dirname, '..', 'data', 'domain-patterns.db');

    // In-memory cache for instant lookups
    this.cache = new Map();

    // Initialize SQLite if available, otherwise use JSON file fallback
    if (Database) {
      this._initSqlite();
    } else {
      this._initJson();
    }
  }

  // ─── SQLite Backend ──────────────────────────────────────────

  _initSqlite() {
    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.db = new Database(this.dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS domain_patterns (
        domain TEXT PRIMARY KEY,
        pattern TEXT NOT NULL,
        provider TEXT,
        mx_host TEXT,
        confidence INTEGER DEFAULT 70,
        verified_count INTEGER DEFAULT 1,
        catch_all INTEGER DEFAULT 0,
        first_verified TEXT,
        last_verified TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS email_cache (
        email TEXT PRIMARY KEY,
        valid INTEGER,
        source TEXT,
        confidence INTEGER,
        verified_at TEXT DEFAULT (datetime('now'))
      );

      CREATE INDEX IF NOT EXISTS idx_dp_confidence ON domain_patterns(confidence DESC);
      CREATE INDEX IF NOT EXISTS idx_dp_provider ON domain_patterns(provider);
    `);

    // Prepared statements
    this._stmtGet = this.db.prepare('SELECT * FROM domain_patterns WHERE domain = ?');
    this._stmtUpsert = this.db.prepare(`
      INSERT INTO domain_patterns (domain, pattern, provider, mx_host, confidence, verified_count, catch_all, first_verified, last_verified)
      VALUES (@domain, @pattern, @provider, @mx_host, @confidence, @verified_count, @catch_all, @first_verified, @last_verified)
      ON CONFLICT(domain) DO UPDATE SET
        pattern = CASE
          WHEN @pattern = excluded.pattern THEN @pattern
          ELSE domain_patterns.pattern
        END,
        confidence = CASE
          WHEN @pattern = domain_patterns.pattern
          THEN MIN(99, domain_patterns.confidence + 5)
          ELSE MAX(40, domain_patterns.confidence - 10)
        END,
        verified_count = CASE
          WHEN @pattern = domain_patterns.pattern
          THEN domain_patterns.verified_count + 1
          ELSE domain_patterns.verified_count
        END,
        provider = COALESCE(@provider, domain_patterns.provider),
        mx_host = COALESCE(@mx_host, domain_patterns.mx_host),
        last_verified = @last_verified,
        updated_at = datetime('now')
    `);
    this._stmtAll = this.db.prepare('SELECT * FROM domain_patterns ORDER BY confidence DESC');
    this._stmtStats = this.db.prepare(`
      SELECT
        COUNT(*) as total_domains,
        SUM(CASE WHEN confidence >= 80 THEN 1 ELSE 0 END) as high_confidence,
        SUM(CASE WHEN confidence >= 60 AND confidence < 80 THEN 1 ELSE 0 END) as medium_confidence,
        SUM(CASE WHEN confidence < 60 THEN 1 ELSE 0 END) as low_confidence,
        SUM(verified_count) as total_verifications,
        SUM(catch_all) as catch_all_domains,
        ROUND(AVG(confidence), 1) as avg_confidence
      FROM domain_patterns
    `);
    this._stmtByProvider = this.db.prepare(`
      SELECT provider, COUNT(*) as count FROM domain_patterns GROUP BY provider ORDER BY count DESC
    `);
    this._stmtByPattern = this.db.prepare(`
      SELECT pattern, COUNT(*) as count FROM domain_patterns GROUP BY pattern ORDER BY count DESC
    `);

    // Email cache statements
    this._stmtEmailGet = this.db.prepare('SELECT * FROM email_cache WHERE email = ?');
    this._stmtEmailUpsert = this.db.prepare(`
      INSERT OR REPLACE INTO email_cache (email, valid, source, confidence, verified_at)
      VALUES (@email, @valid, @source, @confidence, datetime('now'))
    `);

    // Load all patterns into memory for instant access
    this._loadToMemory();

    this.backend = 'sqlite';
  }

  _loadToMemory() {
    const rows = this._stmtAll.all();
    this.cache.clear();
    for (const row of rows) {
      this.cache.set(row.domain, {
        pattern: row.pattern,
        provider: row.provider,
        mxHost: row.mx_host,
        confidence: row.confidence,
        count: row.verified_count,
        catchAll: row.catch_all === 1,
      });
    }
  }

  // ─── JSON Fallback ───────────────────────────────────────────

  _initJson() {
    this.jsonPath = this.dbPath.replace(/\.db$/, '.json');
    this.db = null;
    this.backend = 'json';

    if (fs.existsSync(this.jsonPath)) {
      try {
        const data = JSON.parse(fs.readFileSync(this.jsonPath, 'utf8'));
        for (const [domain, info] of Object.entries(data)) {
          this.cache.set(domain, info);
        }
      } catch {}
    }
  }

  _saveJson() {
    if (this.backend !== 'json') return;
    const obj = {};
    for (const [domain, info] of this.cache) {
      obj[domain] = info;
    }
    const dir = path.dirname(this.jsonPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(this.jsonPath, JSON.stringify(obj, null, 2));
  }

  // ─── Public API ──────────────────────────────────────────────

  /**
   * Get pattern for a domain. Returns null if unknown.
   */
  get(domain) {
    return this.cache.get(domain) || null;
  }

  /**
   * Check if domain has a high-confidence pattern (>= threshold).
   */
  has(domain, minConfidence = 80) {
    const p = this.cache.get(domain);
    return p && p.confidence >= minConfidence;
  }

  /**
   * Store/update a domain pattern.
   */
  set(domain, info) {
    const now = new Date().toISOString();
    const existing = this.cache.get(domain);

    if (this.db) {
      this._stmtUpsert.run({
        domain,
        pattern: info.pattern,
        provider: info.provider || null,
        mx_host: info.mxHost || null,
        confidence: info.confidence || 70,
        verified_count: 1,
        catch_all: info.catchAll ? 1 : 0,
        first_verified: info.email || null,
        last_verified: info.email || null,
      });
    }

    // Update in-memory cache
    if (existing && existing.pattern === info.pattern) {
      existing.count = (existing.count || 1) + 1;
      existing.confidence = Math.min(99, (existing.confidence || 70) + 5);
      if (info.provider) existing.provider = info.provider;
    } else if (existing && existing.pattern !== info.pattern) {
      existing.confidence = Math.max(40, existing.confidence - 10);
    } else {
      this.cache.set(domain, {
        pattern: info.pattern,
        provider: info.provider || null,
        mxHost: info.mxHost || null,
        confidence: info.confidence || 70,
        count: 1,
        catchAll: info.catchAll || false,
      });
    }

    if (this.backend === 'json') this._saveJson();
  }

  /**
   * Mark a domain as catch-all (SMTP verification unreliable).
   */
  setCatchAll(domain, isCatchAll = true) {
    const existing = this.cache.get(domain);
    if (existing) {
      existing.catchAll = isCatchAll;
      if (this.db) {
        this.db.prepare('UPDATE domain_patterns SET catch_all = ? WHERE domain = ?')
          .run(isCatchAll ? 1 : 0, domain);
      }
    }
  }

  /**
   * Apply a known pattern to generate an email address.
   * Returns the email string or null if pattern unknown.
   */
  apply(domain, firstName, lastName) {
    const p = this.cache.get(domain);
    if (!p || !p.pattern) return null;

    const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
    const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
    if (!f || !l) return null;

    return this._applyPattern(p.pattern, f, l, domain);
  }

  /**
   * Apply pattern with confidence info. Returns { email, confidence, source } or null.
   */
  applyWithMeta(domain, firstName, lastName) {
    const p = this.cache.get(domain);
    if (!p || !p.pattern || p.confidence < 60) return null;

    const email = this.apply(domain, firstName, lastName);
    if (!email) return null;

    return {
      email,
      confidence: p.confidence,
      source: `pattern_db_${p.provider || 'unknown'}`,
      status: p.confidence >= 80 ? 'verified_pattern' : 'likely_pattern',
      pattern: p.pattern,
      provider: p.provider,
    };
  }

  // ─── Bulk Operations ─────────────────────────────────────────

  /**
   * Enrich leads instantly using cached patterns.
   * Returns { enriched: [...], cold: [...] } — enriched have emails, cold need live verification.
   */
  enrichBulk(leads) {
    const enriched = [];
    const cold = [];

    for (const lead of leads) {
      let domain = lead.domain;
      if (!domain && lead.website) {
        const m = lead.website.match(/https?:\/\/(?:www\.)?([^\/\?]+)/);
        if (m) domain = m[1].toLowerCase();
      }
      if (!domain || !lead.first_name || !lead.last_name || lead.last_name.length <= 2) {
        cold.push(lead);
        continue;
      }

      const result = this.applyWithMeta(domain, lead.first_name, lead.last_name);
      if (result) {
        enriched.push({ ...lead, domain, ...result });
      } else {
        cold.push({ ...lead, domain });
      }
    }

    return { enriched, cold };
  }

  // ─── Email Cache ─────────────────────────────────────────────

  getEmail(email) {
    if (!this.db) return null;
    return this._stmtEmailGet.get(email) || null;
  }

  setEmail(email, valid, source, confidence) {
    if (!this.db) return;
    this._stmtEmailUpsert.run({ email, valid: valid ? 1 : 0, source, confidence });
  }

  // ─── Stats ───────────────────────────────────────────────────

  getStats() {
    if (this.db) {
      return {
        ...this._stmtStats.get(),
        byProvider: this._stmtByProvider.all(),
        byPattern: this._stmtByPattern.all(),
      };
    }
    // JSON fallback stats
    let total = 0, highConf = 0, providers = {}, patterns = {};
    for (const [, info] of this.cache) {
      total++;
      if (info.confidence >= 80) highConf++;
      providers[info.provider || 'unknown'] = (providers[info.provider || 'unknown'] || 0) + 1;
      patterns[info.pattern] = (patterns[info.pattern] || 0) + 1;
    }
    return { total_domains: total, high_confidence: highConf, byProvider: providers, byPattern: patterns };
  }

  get size() { return this.cache.size; }

  // ─── Import / Export ─────────────────────────────────────────

  /**
   * Import patterns from an EmailWaterfall instance's in-memory caches.
   */
  importFromWaterfall(waterfall) {
    let imported = 0;
    for (const [domain, info] of waterfall.patternCache) {
      this.set(domain, {
        pattern: info.pattern,
        confidence: info.confidence,
        count: info.count,
      });
      imported++;
    }
    // Import MX cache
    for (const [domain, mx] of waterfall.mxCache) {
      if (!mx) continue;
      const existing = this.cache.get(domain);
      if (existing && !existing.provider) {
        existing.provider = mx.provider;
        existing.mxHost = mx.mxHost;
        if (this.db) {
          this.db.prepare('UPDATE domain_patterns SET provider = ?, mx_host = ? WHERE domain = ?')
            .run(mx.provider, mx.mxHost, domain);
        }
      }
    }
    // Import catch-all cache
    for (const [domain, isCatchAll] of waterfall.catchAllCache) {
      if (isCatchAll) this.setCatchAll(domain, true);
    }
    return imported;
  }

  /**
   * Seed an EmailWaterfall instance with patterns from the database.
   */
  seedWaterfall(waterfall) {
    let seeded = 0;
    for (const [domain, info] of this.cache) {
      if (info.confidence >= 60) {
        waterfall.patternCache.set(domain, {
          pattern: info.pattern,
          confidence: info.confidence,
          count: info.count || 1,
        });
        seeded++;
      }
      if (info.provider) {
        waterfall.mxCache.set(domain, {
          provider: info.provider,
          mxHost: info.mxHost,
          allMx: info.mxHost ? [info.mxHost] : [],
        });
      }
      if (info.catchAll) {
        waterfall.catchAllCache.set(domain, true);
      }
    }
    return seeded;
  }

  // ─── Internal ────────────────────────────────────────────────

  _applyPattern(pattern, f, l, domain) {
    const fi = f[0];
    const li = l[0];
    switch (pattern) {
      case 'first.last':  return `${f}.${l}@${domain}`;
      case 'flast':       return `${fi}${l}@${domain}`;
      case 'firstlast':   return `${f}${l}@${domain}`;
      case 'f.last':      return `${fi}.${l}@${domain}`;
      case 'first_last':  return `${f}_${l}@${domain}`;
      case 'last.first':  return `${l}.${f}@${domain}`;
      case 'first':       return `${f}@${domain}`;
      case 'last':        return `${l}@${domain}`;
      case 'lastf':       return `${l}${fi}@${domain}`;
      case 'firstl':      return `${f}${li}@${domain}`;
      case 'first-last':  return `${f}-${l}@${domain}`;
      case 'lastfirst':   return `${l}${f}@${domain}`;
      case 'first.l':     return `${f}.${li}@${domain}`;
      case 'last.f':      return `${l}.${fi}@${domain}`;
      default:            return `${f}.${l}@${domain}`;
    }
  }

  close() {
    if (this.db) this.db.close();
  }
}

module.exports = { DomainPatternDB };
