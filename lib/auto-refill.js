/**
 * Auto-Refill System — Ensures clients never run out of leads mid-campaign.
 *
 * Architecture:
 *   1. Each client has a lead pool with target thresholds
 *   2. Monitor checks pool levels every 6 hours (or on-demand)
 *   3. When pool drops below threshold, triggers a scrape + enrich job
 *   4. New leads are added to the client's pool automatically
 *   5. Credits are debited from client balance
 *
 * Storage: SQLite (extends lead-db.js pattern)
 *
 * Usage:
 *   const { AutoRefill } = require('./lib/auto-refill');
 *   const refill = new AutoRefill();
 *
 *   // Create a client subscription
 *   refill.createSubscription({
 *     clientId: 'client@firm.com',
 *     firmName: 'Smith & Associates',
 *     niche: 'personal injury lawyer',
 *     states: ['FL', 'GA', 'TX'],
 *     targetMonthly: 500,
 *     creditBalance: 1000,
 *   });
 *
 *   // Check all clients and trigger refills as needed
 *   await refill.checkAll();
 */

const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

let Database;
try { Database = require('better-sqlite3'); } catch { Database = null; }

class AutoRefill {
  constructor(dbPath) {
    this.dbPath = dbPath || path.join(__dirname, '..', 'data', 'refill.db');
    this.jobs = new Map(); // in-progress job tracking

    if (Database) {
      this._initDb();
    } else {
      console.warn('AutoRefill: better-sqlite3 not available, using in-memory mode');
      this.db = null;
      this.subscriptions = new Map();
    }
  }

  _initDb() {
    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.db = new Database(this.dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS subscriptions (
        client_id TEXT PRIMARY KEY,
        firm_name TEXT,
        niche TEXT DEFAULT 'lawyer',
        states TEXT,
        practice_areas TEXT,
        target_monthly INTEGER DEFAULT 100,
        min_email_pct INTEGER DEFAULT 70,
        pool_size INTEGER DEFAULT 0,
        leads_delivered_this_month INTEGER DEFAULT 0,
        credit_balance REAL DEFAULT 0,
        cost_per_lead REAL DEFAULT 1.0,
        status TEXT DEFAULT 'active',
        last_refill_at TEXT,
        last_check_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      );

      CREATE TABLE IF NOT EXISTS client_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        firm_name TEXT,
        domain TEXT,
        city TEXT,
        state TEXT,
        title TEXT,
        linkedin_url TEXT,
        email_source TEXT,
        email_confidence TEXT,
        added_at TEXT DEFAULT (datetime('now')),
        delivered INTEGER DEFAULT 0,
        delivered_at TEXT,
        FOREIGN KEY(client_id) REFERENCES subscriptions(client_id)
      );

      CREATE TABLE IF NOT EXISTS refill_jobs (
        job_id TEXT PRIMARY KEY,
        client_id TEXT NOT NULL,
        target_count INTEGER,
        actual_count INTEGER DEFAULT 0,
        emails_found INTEGER DEFAULT 0,
        states TEXT,
        niche TEXT,
        cost REAL DEFAULT 0,
        status TEXT DEFAULT 'queued',
        started_at TEXT,
        completed_at TEXT,
        error TEXT,
        FOREIGN KEY(client_id) REFERENCES subscriptions(client_id)
      );

      CREATE INDEX IF NOT EXISTS idx_cl_client ON client_leads(client_id);
      CREATE INDEX IF NOT EXISTS idx_cl_delivered ON client_leads(client_id, delivered);
      CREATE INDEX IF NOT EXISTS idx_rj_client ON refill_jobs(client_id);
      CREATE INDEX IF NOT EXISTS idx_rj_status ON refill_jobs(status);
    `);
  }

  // ─── Subscription Management ─────────────────────────────────

  createSubscription({ clientId, firmName, niche, states, practiceAreas, targetMonthly, creditBalance, costPerLead }) {
    if (!this.db) return;
    this.db.prepare(`
      INSERT OR REPLACE INTO subscriptions
      (client_id, firm_name, niche, states, practice_areas, target_monthly, credit_balance, cost_per_lead, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')
    `).run(
      clientId,
      firmName || '',
      niche || 'lawyer',
      JSON.stringify(states || []),
      JSON.stringify(practiceAreas || []),
      targetMonthly || 100,
      creditBalance || 0,
      costPerLead || 1.0
    );
  }

  getSubscription(clientId) {
    if (!this.db) return null;
    const sub = this.db.prepare('SELECT * FROM subscriptions WHERE client_id = ?').get(clientId);
    if (sub) {
      sub.states = JSON.parse(sub.states || '[]');
      sub.practice_areas = JSON.parse(sub.practice_areas || '[]');
    }
    return sub;
  }

  listSubscriptions(status = 'active') {
    if (!this.db) return [];
    return this.db.prepare('SELECT * FROM subscriptions WHERE status = ? ORDER BY updated_at DESC').all(status)
      .map(s => ({ ...s, states: JSON.parse(s.states || '[]'), practice_areas: JSON.parse(s.practice_areas || '[]') }));
  }

  pauseSubscription(clientId) {
    if (!this.db) return;
    this.db.prepare("UPDATE subscriptions SET status = 'paused', updated_at = datetime('now') WHERE client_id = ?")
      .run(clientId);
  }

  resumeSubscription(clientId) {
    if (!this.db) return;
    this.db.prepare("UPDATE subscriptions SET status = 'active', updated_at = datetime('now') WHERE client_id = ?")
      .run(clientId);
  }

  addCredits(clientId, amount) {
    if (!this.db) return;
    this.db.prepare("UPDATE subscriptions SET credit_balance = credit_balance + ?, updated_at = datetime('now') WHERE client_id = ?")
      .run(amount, clientId);
  }

  // ─── Pool Management ─────────────────────────────────────────

  /**
   * Get number of undelivered leads in a client's pool.
   */
  getPoolSize(clientId) {
    if (!this.db) return 0;
    const row = this.db.prepare('SELECT COUNT(*) as count FROM client_leads WHERE client_id = ? AND delivered = 0')
      .get(clientId);
    return row ? row.count : 0;
  }

  /**
   * Get leads ready for delivery.
   */
  getLeadsForDelivery(clientId, limit = 50) {
    if (!this.db) return [];
    return this.db.prepare(`
      SELECT * FROM client_leads
      WHERE client_id = ? AND delivered = 0
      ORDER BY added_at DESC
      LIMIT ?
    `).all(clientId, limit);
  }

  /**
   * Mark leads as delivered.
   */
  markDelivered(leadIds) {
    if (!this.db || !leadIds.length) return;
    const stmt = this.db.prepare("UPDATE client_leads SET delivered = 1, delivered_at = datetime('now') WHERE id = ?");
    const tx = this.db.transaction(() => {
      for (const id of leadIds) stmt.run(id);
    });
    tx();
  }

  /**
   * Add leads to a client's pool.
   */
  addLeadsToPool(clientId, leads) {
    if (!this.db || !leads.length) return 0;
    const stmt = this.db.prepare(`
      INSERT INTO client_leads
      (client_id, first_name, last_name, email, phone, firm_name, domain, city, state, title, linkedin_url, email_source, email_confidence)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    let added = 0;
    const tx = this.db.transaction(() => {
      for (const l of leads) {
        stmt.run(
          clientId,
          l.first_name || '', l.last_name || '', l.email || '', l.phone || '',
          l.firm_name || '', l.domain || '', l.city || '', l.state || '',
          l.title || '', l.linkedin_url || '', l.email_source || '', l.email_confidence || ''
        );
        added++;
      }
    });
    tx();

    // Update pool size
    this.db.prepare("UPDATE subscriptions SET pool_size = pool_size + ?, updated_at = datetime('now') WHERE client_id = ?")
      .run(added, clientId);

    return added;
  }

  // ─── Refill Logic ────────────────────────────────────────────

  /**
   * Check if a client needs a refill.
   */
  needsRefill(clientId) {
    const sub = this.getSubscription(clientId);
    if (!sub || sub.status !== 'active') return { needed: false, reason: 'inactive' };

    const poolSize = this.getPoolSize(clientId);
    const dailyRate = sub.target_monthly / 30;
    const daysRemaining = dailyRate > 0 ? poolSize / dailyRate : 999;

    // Refill if less than 7 days of leads remaining
    if (daysRemaining < 7) {
      const deficit = Math.ceil(sub.target_monthly * 1.5) - poolSize;
      const cost = deficit * (sub.cost_per_lead || 1);

      if (sub.credit_balance < cost) {
        return { needed: true, reason: 'low_credits', deficit, cost, balance: sub.credit_balance };
      }

      return { needed: true, reason: 'low_pool', poolSize, daysRemaining: Math.round(daysRemaining * 10) / 10, deficit, cost };
    }

    return { needed: false, reason: 'sufficient', poolSize, daysRemaining: Math.round(daysRemaining * 10) / 10 };
  }

  /**
   * Trigger a refill job for a client.
   * Returns the job ID. The actual scraping happens asynchronously.
   */
  triggerRefill(clientId, options = {}) {
    const sub = this.getSubscription(clientId);
    if (!sub) throw new Error(`Subscription not found: ${clientId}`);

    const check = this.needsRefill(clientId);
    if (!check.needed && !options.force) {
      return { triggered: false, reason: check.reason, ...check };
    }

    if (check.reason === 'low_credits' && !options.force) {
      return { triggered: false, reason: 'insufficient_credits', balance: sub.credit_balance, needed: check.cost };
    }

    const jobId = crypto.randomUUID();
    const targetCount = options.count || check.deficit || sub.target_monthly;

    if (this.db) {
      this.db.prepare(`
        INSERT INTO refill_jobs (job_id, client_id, target_count, states, niche, status, started_at)
        VALUES (?, ?, ?, ?, ?, 'queued', datetime('now'))
      `).run(jobId, clientId, targetCount, JSON.stringify(sub.states), sub.niche);
    }

    this.jobs.set(jobId, {
      jobId, clientId, targetCount,
      states: sub.states,
      niche: sub.niche,
      status: 'queued',
      startedAt: new Date().toISOString(),
    });

    return { triggered: true, jobId, targetCount, states: sub.states, niche: sub.niche };
  }

  /**
   * Complete a refill job — called after scraping finishes.
   */
  completeJob(jobId, leads) {
    const job = this.jobs.get(jobId);
    if (!job) return;

    const emailCount = leads.filter(l => l.email).length;

    // Add leads to client pool
    const added = this.addLeadsToPool(job.clientId, leads);

    // Debit credits
    const sub = this.getSubscription(job.clientId);
    const cost = added * (sub ? sub.cost_per_lead || 1 : 1);
    if (this.db) {
      this.db.prepare(`
        UPDATE subscriptions
        SET credit_balance = MAX(0, credit_balance - ?),
            leads_delivered_this_month = leads_delivered_this_month + ?,
            last_refill_at = datetime('now'),
            updated_at = datetime('now')
        WHERE client_id = ?
      `).run(cost, added, job.clientId);

      this.db.prepare(`
        UPDATE refill_jobs
        SET status = 'completed', actual_count = ?, emails_found = ?, cost = ?, completed_at = datetime('now')
        WHERE job_id = ?
      `).run(added, emailCount, cost, jobId);
    }

    job.status = 'completed';
    job.actualCount = added;
    job.emailsFound = emailCount;
    job.cost = cost;

    return { jobId, added, emailCount, cost };
  }

  /**
   * Fail a refill job.
   */
  failJob(jobId, error) {
    if (this.db) {
      this.db.prepare("UPDATE refill_jobs SET status = 'failed', error = ?, completed_at = datetime('now') WHERE job_id = ?")
        .run(error, jobId);
    }
    const job = this.jobs.get(jobId);
    if (job) job.status = 'failed';
  }

  /**
   * Check ALL active subscriptions and return which need refills.
   */
  checkAll() {
    const subs = this.listSubscriptions('active');
    const results = [];

    for (const sub of subs) {
      const check = this.needsRefill(sub.client_id);
      this.db?.prepare("UPDATE subscriptions SET last_check_at = datetime('now') WHERE client_id = ?")
        .run(sub.client_id);

      results.push({
        clientId: sub.client_id,
        firmName: sub.firm_name,
        ...check,
      });
    }

    return results;
  }

  // ─── Stats ───────────────────────────────────────────────────

  getClientStats(clientId) {
    if (!this.db) return null;

    const sub = this.getSubscription(clientId);
    if (!sub) return null;

    const poolSize = this.getPoolSize(clientId);
    const totalDelivered = this.db.prepare(
      'SELECT COUNT(*) as count FROM client_leads WHERE client_id = ? AND delivered = 1'
    ).get(clientId)?.count || 0;
    const totalAdded = this.db.prepare(
      'SELECT COUNT(*) as count FROM client_leads WHERE client_id = ?'
    ).get(clientId)?.count || 0;
    const emailCount = this.db.prepare(
      "SELECT COUNT(*) as count FROM client_leads WHERE client_id = ? AND email != ''"
    ).get(clientId)?.count || 0;
    const refillCount = this.db.prepare(
      "SELECT COUNT(*) as count FROM refill_jobs WHERE client_id = ? AND status = 'completed'"
    ).get(clientId)?.count || 0;

    return {
      ...sub,
      poolSize,
      totalDelivered,
      totalAdded,
      emailCount,
      emailRate: totalAdded > 0 ? Math.round(emailCount / totalAdded * 100) : 0,
      refillCount,
      daysRemaining: sub.target_monthly > 0 ? Math.round(poolSize / (sub.target_monthly / 30) * 10) / 10 : 999,
    };
  }

  getSystemStats() {
    if (!this.db) return {};

    const totalSubs = this.db.prepare('SELECT COUNT(*) as c FROM subscriptions').get()?.c || 0;
    const activeSubs = this.db.prepare("SELECT COUNT(*) as c FROM subscriptions WHERE status = 'active'").get()?.c || 0;
    const totalLeads = this.db.prepare('SELECT COUNT(*) as c FROM client_leads').get()?.c || 0;
    const totalDelivered = this.db.prepare('SELECT COUNT(*) as c FROM client_leads WHERE delivered = 1').get()?.c || 0;
    const totalRevenue = this.db.prepare("SELECT COALESCE(SUM(cost), 0) as c FROM refill_jobs WHERE status = 'completed'").get()?.c || 0;
    const totalJobs = this.db.prepare("SELECT COUNT(*) as c FROM refill_jobs WHERE status = 'completed'").get()?.c || 0;

    return {
      totalSubscriptions: totalSubs,
      activeSubscriptions: activeSubs,
      totalLeadsInSystem: totalLeads,
      totalDelivered,
      totalRevenue: Math.round(totalRevenue * 100) / 100,
      totalRefillJobs: totalJobs,
    };
  }

  // ─── Cleanup ─────────────────────────────────────────────────

  /**
   * Reset monthly counters (run on 1st of each month).
   */
  resetMonthlyCounters() {
    if (!this.db) return;
    this.db.prepare("UPDATE subscriptions SET leads_delivered_this_month = 0, updated_at = datetime('now')")
      .run();
  }

  close() {
    if (this.db) this.db.close();
  }
}

module.exports = { AutoRefill };
