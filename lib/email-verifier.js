/**
 * Email Verifier — SMTP-based email verification + pattern generation
 *
 * Zero dependencies — uses Node.js built-in `dns` and `net` modules.
 *
 * Features:
 *   1. MX lookup — find mail servers for a domain
 *   2. SMTP RCPT TO — verify email exists without sending
 *   3. Catch-all detection — detect domains that accept all addresses
 *   4. Email pattern generation — generate likely emails from name + domain
 *   5. Batch verification — verify multiple emails with rate limiting
 *
 * Usage:
 *   const verifier = new EmailVerifier();
 *   const result = await verifier.verify('john@example.com');
 *   // { email, valid, catchAll, mxHost, code, message }
 *
 *   const emails = verifier.generatePatterns('John', 'Smith', 'smithlaw.com');
 *   // ['john@smithlaw.com', 'jsmith@smithlaw.com', ...]
 *
 *   const best = await verifier.findBestEmail('John', 'Smith', 'smithlaw.com');
 *   // 'john.smith@smithlaw.com' (first pattern that verifies)
 */

const dns = require('dns');
const net = require('net');
const { log } = require('./logger');

// Common role-based addresses to skip (not personal emails)
const ROLE_ADDRESSES = new Set([
  'info', 'contact', 'office', 'admin', 'support', 'hello',
  'team', 'sales', 'marketing', 'hr', 'help', 'enquiries',
  'reception', 'billing', 'accounts', 'general', 'mail',
  'noreply', 'no-reply', 'donotreply', 'postmaster', 'webmaster',
]);

// Disposable/free email providers — don't try to generate patterns for these
const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com',
]);

class EmailVerifier {
  constructor(options = {}) {
    this.timeout = options.timeout || 10000; // 10s per SMTP connection
    this.fromEmail = options.fromEmail || 'verify@mortar.app';
    this.fromDomain = options.fromDomain || 'mortar.app';
    this.concurrency = options.concurrency || 3;
    this.retries = options.retries || 1;

    // Cache MX records per domain to avoid repeated lookups
    this._mxCache = new Map();
    // Cache catch-all status per domain
    this._catchAllCache = new Map();
    // Cache verification results
    this._resultCache = new Map();

    this.stats = {
      verified: 0,
      invalid: 0,
      catchAll: 0,
      errors: 0,
      mxLookups: 0,
    };
  }

  /**
   * Look up MX records for a domain. Cached.
   * @returns {string|null} Best MX host or null
   */
  async getMxHost(domain) {
    if (this._mxCache.has(domain)) return this._mxCache.get(domain);

    this.stats.mxLookups++;
    try {
      const records = await new Promise((resolve, reject) => {
        dns.resolveMx(domain, (err, addresses) => {
          if (err) reject(err);
          else resolve(addresses || []);
        });
      });

      if (records.length === 0) {
        this._mxCache.set(domain, null);
        return null;
      }

      // Sort by priority (lowest = best), take first
      records.sort((a, b) => a.priority - b.priority);
      const host = records[0].exchange;
      this._mxCache.set(domain, host);
      return host;
    } catch {
      this._mxCache.set(domain, null);
      return null;
    }
  }

  /**
   * Open an SMTP connection and check if an email address is deliverable.
   *
   * SMTP flow: EHLO → MAIL FROM → RCPT TO → check response code
   * 250 = exists, 550 = doesn't exist, other = inconclusive
   */
  async smtpCheck(email, mxHost) {
    return new Promise((resolve) => {
      const socket = new net.Socket();
      let step = 'connect';
      let response = '';
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

      socket.on('error', () => {
        clearTimeout(timer);
        finish({ valid: false, code: 0, message: 'connection error' });
      });

      socket.on('close', () => {
        clearTimeout(timer);
        finish({ valid: false, code: 0, message: 'connection closed' });
      });

      socket.on('data', (data) => {
        response = data.toString();
        const code = parseInt(response.substring(0, 3), 10);

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
          if (code === 250) {
            socket.write('QUIT\r\n');
            finish({ valid: true, code, message: response.trim() });
          } else if (code === 550 || code === 551 || code === 553 || code === 554) {
            socket.write('QUIT\r\n');
            finish({ valid: false, code, message: response.trim() });
          } else if (code === 452 || code === 421) {
            // Rate limited or temporarily unavailable
            socket.write('QUIT\r\n');
            finish({ valid: false, code, message: 'rate limited' });
          } else {
            // 450 = temp failure, treat as inconclusive
            socket.write('QUIT\r\n');
            finish({ valid: false, code, message: response.trim() });
          }
        } else if (code >= 500) {
          clearTimeout(timer);
          finish({ valid: false, code, message: response.trim() });
        }
      });

      socket.connect(25, mxHost);
    });
  }

  /**
   * Check if a domain is a catch-all (accepts any address). Cached.
   * Tests by sending RCPT TO with a random nonexistent address.
   */
  async isCatchAll(domain, mxHost) {
    if (this._catchAllCache.has(domain)) return this._catchAllCache.get(domain);

    // Generate a random address that almost certainly doesn't exist
    const random = `verify-test-${Date.now()}-${Math.random().toString(36).slice(2, 10)}@${domain}`;
    const result = await this.smtpCheck(random, mxHost);

    const catchAll = result.valid; // If random address is "valid", it's catch-all
    this._catchAllCache.set(domain, catchAll);

    if (catchAll) this.stats.catchAll++;
    return catchAll;
  }

  /**
   * Verify a single email address.
   *
   * @param {string} email
   * @returns {{ email, valid, catchAll, mxHost, code, message }}
   */
  async verify(email) {
    if (!email || !email.includes('@')) {
      return { email, valid: false, catchAll: false, mxHost: null, code: 0, message: 'invalid format' };
    }

    // Check cache
    if (this._resultCache.has(email)) return this._resultCache.get(email);

    const domain = email.split('@')[1].toLowerCase();

    // Skip free providers — we can't really verify them reliably
    if (FREE_PROVIDERS.has(domain)) {
      const result = { email, valid: false, catchAll: false, mxHost: null, code: 0, message: 'free provider - cannot verify' };
      this._resultCache.set(email, result);
      return result;
    }

    // Step 1: MX lookup
    const mxHost = await this.getMxHost(domain);
    if (!mxHost) {
      const result = { email, valid: false, catchAll: false, mxHost: null, code: 0, message: 'no MX records' };
      this._resultCache.set(email, result);
      this.stats.invalid++;
      return result;
    }

    // Step 2: SMTP RCPT TO check
    let smtpResult;
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      smtpResult = await this.smtpCheck(email, mxHost);
      if (smtpResult.code !== 0) break; // Got a response, stop retrying
      if (attempt < this.retries) {
        await new Promise(r => setTimeout(r, 2000)); // Wait before retry
      }
    }

    // Step 3: Catch-all detection (only if email seems valid)
    let catchAll = false;
    if (smtpResult.valid) {
      catchAll = await this.isCatchAll(domain, mxHost);
    }

    const result = {
      email,
      valid: smtpResult.valid,
      catchAll,
      mxHost,
      code: smtpResult.code,
      message: smtpResult.message,
    };

    this._resultCache.set(email, result);

    if (result.valid) this.stats.verified++;
    else this.stats.invalid++;

    return result;
  }

  /**
   * Generate email pattern candidates from a person's name and domain.
   * Ordered by frequency (most common patterns first).
   *
   * @param {string} firstName
   * @param {string} lastName
   * @param {string} domain
   * @returns {string[]} Array of email candidates
   */
  generatePatterns(firstName, lastName, domain) {
    if (!firstName || !lastName || !domain) return [];
    if (FREE_PROVIDERS.has(domain.toLowerCase())) return [];

    const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
    const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
    if (!f || !l) return [];

    const fi = f[0]; // first initial
    const d = domain.toLowerCase();

    // Ordered by frequency across law firms (most common first)
    return [
      `${f}@${d}`,              // john@
      `${f}${l}@${d}`,          // johnsmith@
      `${f}.${l}@${d}`,         // john.smith@
      `${fi}${l}@${d}`,         // jsmith@
      `${fi}.${l}@${d}`,        // j.smith@
      `${l}@${d}`,              // smith@
      `${f}_${l}@${d}`,         // john_smith@
      `${l}.${f}@${d}`,         // smith.john@
      `${l}${fi}@${d}`,         // smithj@
      `${fi}${l[0]}@${d}`,      // js@ (initials only — less common)
      `${f}.${l[0]}@${d}`,      // john.s@
      `${f}${l[0]}@${d}`,       // johns@
    ];
  }

  /**
   * Find the best email for a person at a domain.
   * Generates patterns, then verifies each until one passes.
   *
   * @param {string} firstName
   * @param {string} lastName
   * @param {string} domain
   * @returns {string} Best verified email, or '' if none found
   */
  async findBestEmail(firstName, lastName, domain) {
    const patterns = this.generatePatterns(firstName, lastName, domain);
    if (patterns.length === 0) return '';

    // First check if domain has MX records at all
    const mxHost = await this.getMxHost(domain);
    if (!mxHost) return '';

    // Check if catch-all — if so, we can't verify, return most common pattern
    const catchAll = await this.isCatchAll(domain, mxHost);
    if (catchAll) {
      // Can't distinguish real from fake on catch-all domains
      // Return most common pattern (first.last@ for law firms)
      return patterns[2]; // john.smith@ is most common in law firms
    }

    // Try each pattern until one verifies
    for (const email of patterns) {
      const result = await this.smtpCheck(email, mxHost);
      if (result.valid) {
        this.stats.verified++;
        return email;
      }
      // Small delay between attempts to avoid rate limiting
      await new Promise(r => setTimeout(r, 500));
    }

    return ''; // None verified
  }

  /**
   * Batch verify emails for multiple leads.
   * For leads with email: verify it.
   * For leads without email but with website: try to find one.
   *
   * @param {object[]} leads - Array of lead objects
   * @param {object} options
   * @param {boolean} [options.verifyExisting=true] - Verify existing emails
   * @param {boolean} [options.findMissing=true] - Find emails for leads without one
   * @param {function} [options.onProgress] - Callback(current, total, detail)
   * @param {function} [options.isCancelled] - Returns true to stop
   * @returns {{ verified, invalid, found, catchAll, skipped }}
   */
  async batchProcess(leads, options = {}) {
    const {
      verifyExisting = true,
      findMissing = true,
      onProgress,
      isCancelled = () => false,
    } = options;

    const stats = { verified: 0, invalid: 0, found: 0, catchAll: 0, skipped: 0 };
    let processed = 0;
    const total = leads.length;

    for (const lead of leads) {
      if (isCancelled()) break;

      if (lead.email && verifyExisting) {
        // Verify existing email
        const result = await this.verify(lead.email);
        if (result.valid) {
          lead._emailVerified = true;
          stats.verified++;
          if (result.catchAll) {
            lead._emailCatchAll = true;
            stats.catchAll++;
          }
        } else {
          lead._emailVerified = false;
          stats.invalid++;
        }
      } else if (!lead.email && lead.website && findMissing) {
        // Try to find email from website domain
        let domain;
        try {
          const url = lead.website.startsWith('http') ? lead.website : 'https://' + lead.website;
          domain = new URL(url).hostname.replace(/^www\./, '');
        } catch {
          stats.skipped++;
          processed++;
          continue;
        }

        if (FREE_PROVIDERS.has(domain)) {
          stats.skipped++;
          processed++;
          continue;
        }

        const email = await this.findBestEmail(
          lead.first_name || '',
          lead.last_name || '',
          domain
        );

        if (email) {
          lead.email = email;
          lead.email_source = 'smtp-pattern';
          stats.found++;
        }
      } else {
        stats.skipped++;
      }

      processed++;
      if (onProgress) onProgress(processed, total, `${lead.first_name || ''} ${lead.last_name || ''}`);
    }

    return stats;
  }

  /**
   * Extract domain from a website URL.
   */
  static extractDomain(website) {
    if (!website) return '';
    try {
      const url = website.startsWith('http') ? website : 'https://' + website;
      return new URL(url).hostname.replace(/^www\./, '');
    } catch {
      return '';
    }
  }

  /**
   * Check if an email is a role-based address (info@, contact@, etc.)
   */
  static isRoleAddress(email) {
    if (!email) return false;
    const local = email.split('@')[0].toLowerCase();
    return ROLE_ADDRESSES.has(local);
  }

  getStats() {
    return { ...this.stats };
  }

  /**
   * Clear all caches.
   */
  clearCache() {
    this._mxCache.clear();
    this._catchAllCache.clear();
    this._resultCache.clear();
  }
}

module.exports = EmailVerifier;
