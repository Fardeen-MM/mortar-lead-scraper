/**
 * Bulk Email Finder — Find emails for hundreds of leads in minutes.
 *
 * Strategy: Detect the email PATTERN per domain (not per person).
 * If first.last@domain.com works for one person, it works for everyone there.
 *
 * Steps per domain:
 *   1. MX lookup (cached)
 *   2. Catch-all detection (cached)
 *   3. Test 6 patterns with ONE person from that domain
 *   4. Apply winning pattern to ALL people at that domain
 *
 * For 500 leads across 300 domains:
 *   - ~300 MX lookups (instant, DNS)
 *   - ~300 catch-all checks (1 SMTP each)
 *   - ~1800 pattern checks worst case (6 per domain)
 *   - Total: ~2100 SMTP connections, 30 concurrent → ~70 seconds
 *
 * Usage:
 *   const finder = new BulkEmailFinder({ concurrency: 30, timeout: 5000 });
 *   const results = await finder.findEmails(leads);
 *   // Modifies leads in-place: sets lead.email, lead.email_source
 *   // Returns stats: { found, catchAll, noMx, failed, domains }
 */

const dns = require('dns');
const net = require('net');

const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com',
]);

// 6 most common B2B email patterns (ordered by frequency)
const PATTERNS = [
  (f, l) => `${f}.${l}`,         // john.smith@ (most common)
  (f, l) => `${f}`,              // john@
  (f, l) => `${f[0]}${l}`,       // jsmith@
  (f, l) => `${f}${l}`,          // johnsmith@
  (f, l) => `${f[0]}.${l}`,      // j.smith@
  (f, l) => `${l}`,              // smith@
];

class BulkEmailFinder {
  constructor(options = {}) {
    this.concurrency = options.concurrency || 30;
    this.timeout = options.timeout || 5000;
    this.fromDomain = options.fromDomain || 'mortar.app';
    this.fromEmail = options.fromEmail || 'verify@mortar.app';
    this.onProgress = options.onProgress || null;

    // Caches
    this._mxCache = new Map();
    this._catchAllCache = new Map();
    this._patternCache = new Map(); // domain → winning pattern index (or -1)

    this.stats = {
      found: 0,
      catchAll: 0,
      noMx: 0,
      failed: 0,
      domains: 0,
      smtpChecks: 0,
    };
  }

  /**
   * Find emails for an array of leads. Modifies leads in-place.
   * Each lead needs: { first_name, last_name, domain }
   * Sets: lead.email, lead.email_source = 'smtp_pattern'
   *
   * @returns {object} Stats
   */
  async findEmails(leads) {
    // Group leads by domain
    const byDomain = new Map();
    for (const lead of leads) {
      if (lead.email) continue; // Already has email
      if (!lead.first_name || !lead.last_name || !lead.domain) continue;
      if (lead.last_name.length <= 2) continue; // Obfuscated name
      if (FREE_PROVIDERS.has(lead.domain.toLowerCase())) continue;

      const domain = lead.domain.toLowerCase();
      if (!byDomain.has(domain)) byDomain.set(domain, []);
      byDomain.get(domain).push(lead);
    }

    const domains = [...byDomain.keys()];
    this.stats.domains = domains.length;

    if (domains.length === 0) return this.stats;

    console.log(`  Processing ${domains.length} domains for ${leads.filter(l => !l.email && l.domain).length} leads...`);

    // Process domains concurrently
    let idx = 0;
    let processed = 0;
    const startTime = Date.now();

    const workers = [];
    for (let w = 0; w < Math.min(this.concurrency, domains.length); w++) {
      workers.push((async () => {
        while (idx < domains.length) {
          const domain = domains[idx++];
          const domainLeads = byDomain.get(domain);

          try {
            await this._processDomain(domain, domainLeads);
          } catch (e) {
            // Domain failed entirely — skip
            this.stats.failed++;
          }

          processed++;
          if (this.onProgress && processed % 20 === 0) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
            const rate = Math.round(processed / (elapsed / 60));
            this.onProgress(processed, domains.length, this.stats.found, rate);
          }
        }
      })());
    }

    await Promise.all(workers);
    return this.stats;
  }

  /**
   * Process a single domain: find the email pattern, apply to all leads.
   */
  async _processDomain(domain, leads) {
    // Step 1: MX lookup
    const mxHost = await this._getMxHost(domain);
    if (!mxHost) {
      this.stats.noMx++;
      return;
    }

    // Step 2: Catch-all detection
    const catchAll = await this._isCatchAll(domain, mxHost);
    if (catchAll) {
      // Catch-all: can't verify, use most common pattern (first.last@)
      this.stats.catchAll++;
      for (const lead of leads) {
        const f = lead.first_name.toLowerCase().replace(/[^a-z]/g, '');
        const l = lead.last_name.toLowerCase().replace(/[^a-z]/g, '');
        if (f && l) {
          lead.email = `${f}.${l}@${domain}`;
          lead.email_source = 'smtp_catchall';
          this.stats.found++;
        }
      }
      return;
    }

    // Step 3: Find winning pattern using the first lead with a name
    const testLead = leads.find(l => {
      const f = l.first_name.toLowerCase().replace(/[^a-z]/g, '');
      const l2 = l.last_name.toLowerCase().replace(/[^a-z]/g, '');
      return f && l2;
    });

    if (!testLead) return;

    const f = testLead.first_name.toLowerCase().replace(/[^a-z]/g, '');
    const l = testLead.last_name.toLowerCase().replace(/[^a-z]/g, '');

    let winningPattern = -1;

    for (let i = 0; i < PATTERNS.length; i++) {
      const local = PATTERNS[i](f, l);
      const email = `${local}@${domain}`;
      this.stats.smtpChecks++;

      const result = await this._smtpCheck(email, mxHost);
      if (result.valid) {
        winningPattern = i;
        // Set email for the test lead
        testLead.email = email;
        testLead.email_source = 'smtp_pattern';
        this.stats.found++;
        break;
      }
    }

    if (winningPattern === -1) {
      this.stats.failed++;
      return;
    }

    // Step 4: Apply winning pattern to ALL remaining leads at this domain
    for (const lead of leads) {
      if (lead.email) continue; // Already set (test lead)
      const lf = lead.first_name.toLowerCase().replace(/[^a-z]/g, '');
      const ll = lead.last_name.toLowerCase().replace(/[^a-z]/g, '');
      if (!lf || !ll) continue;

      const local = PATTERNS[winningPattern](lf, ll);
      lead.email = `${local}@${domain}`;
      lead.email_source = 'smtp_pattern';
      this.stats.found++;
    }
  }

  // ─── Low-level helpers ─────────────────────────────────────────

  async _getMxHost(domain) {
    if (this._mxCache.has(domain)) return this._mxCache.get(domain);
    try {
      const records = await new Promise((resolve, reject) => {
        dns.resolveMx(domain, (err, addrs) => err ? reject(err) : resolve(addrs || []));
      });
      if (records.length === 0) { this._mxCache.set(domain, null); return null; }
      records.sort((a, b) => a.priority - b.priority);
      const host = records[0].exchange;
      this._mxCache.set(domain, host);
      return host;
    } catch {
      this._mxCache.set(domain, null);
      return null;
    }
  }

  async _isCatchAll(domain, mxHost) {
    if (this._catchAllCache.has(domain)) return this._catchAllCache.get(domain);
    const random = `xyztest-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}@${domain}`;
    const result = await this._smtpCheck(random, mxHost);
    this._catchAllCache.set(domain, result.valid);
    this.stats.smtpChecks++;
    return result.valid;
  }

  _smtpCheck(email, mxHost) {
    return new Promise((resolve) => {
      const socket = new net.Socket();
      let step = 'connect';
      let resolved = false;

      const finish = (result) => {
        if (resolved) return;
        resolved = true;
        socket.destroy();
        resolve(result);
      };

      const timer = setTimeout(() => {
        finish({ valid: false, code: 0, message: 'timeout' });
      }, this.timeout);

      socket.on('error', () => { clearTimeout(timer); finish({ valid: false, code: 0, message: 'error' }); });
      socket.on('close', () => { clearTimeout(timer); finish({ valid: false, code: 0, message: 'closed' }); });

      socket.on('data', (data) => {
        const resp = data.toString();
        const code = parseInt(resp.substring(0, 3), 10);

        if (step === 'connect' && code === 220) {
          step = 'ehlo';
          socket.write(`EHLO ${this.fromDomain}\r\n`);
        } else if (step === 'ehlo' && code === 250) {
          step = 'mail';
          socket.write(`MAIL FROM:<${this.fromEmail}>\r\n`);
        } else if (step === 'mail' && code === 250) {
          step = 'rcpt';
          socket.write(`RCPT TO:<${email}>\r\n`);
        } else if (step === 'rcpt') {
          clearTimeout(timer);
          socket.write('QUIT\r\n');
          finish({ valid: code === 250, code, message: resp.trim() });
        } else if (code >= 500) {
          clearTimeout(timer);
          finish({ valid: false, code, message: resp.trim() });
        }
      });

      socket.connect(25, mxHost);
    });
  }
}

module.exports = BulkEmailFinder;
