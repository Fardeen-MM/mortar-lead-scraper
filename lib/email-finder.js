/**
 * Email Finder — visit firm websites with Puppeteer to scrape emails
 *
 * Only runs for leads that DON'T already have an email from the bar directory.
 * Checks: homepage, /contact, /about, footer, mailto links, meta tags.
 * Handles: dead sites, timeouts, SSL errors, JS-rendered pages.
 */

const puppeteer = require('puppeteer');
const { log } = require('./logger');
const { RateLimiter, sleep } = require('./rate-limiter');

// Email regex — matches standard email patterns, excludes obvious false positives
const EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

// Domains that are never real contact emails
const IGNORE_DOMAINS = new Set([
  'example.com', 'test.com', 'email.com', 'yoursite.com', 'yourdomain.com',
  'sentry.io', 'wixpress.com', 'w3.org', 'schema.org', 'googleapis.com',
  'google.com', 'facebook.com', 'twitter.com', 'instagram.com',
  'wordpress.com', 'wp.com', 'gravatar.com', 'cloudflare.com',
  'jquery.com', 'bootstrapcdn.com', 'fontawesome.com',
  'sentry-next.wixpress.com', 'googletagmanager.com',
]);

// File extensions that aren't pages
const SKIP_EXTENSIONS = /\.(jpg|jpeg|png|gif|svg|pdf|doc|docx|zip|css|js|ico|woff|woff2|ttf|eot)$/i;

/**
 * Check if an email looks like a real contact email.
 */
function cleanEmail(email) {
  if (!email) return '';
  // Strip mailto: prefix
  email = email.replace(/^mailto:/i, '');
  // Strip query params (e.g., ?subject=...)
  email = email.split('?')[0];
  // Decode URL-encoded characters (%20, %40, etc.)
  try { email = decodeURIComponent(email); } catch {}
  // Strip leading/trailing whitespace and non-printable chars
  return email.replace(/^\s+|\s+$/g, '').replace(/[\x00-\x1f]/g, '').toLowerCase();
}

function isValidEmail(email) {
  email = cleanEmail(email);
  if (!email || email.length < 5 || email.length > 100) return false;
  const domain = email.split('@')[1]?.toLowerCase();
  if (!domain) return false;
  if (IGNORE_DOMAINS.has(domain)) return false;
  // Skip image/file emails
  if (SKIP_EXTENSIONS.test(email)) return false;
  // Skip noreply/automated addresses
  const local = email.split('@')[0].toLowerCase();
  if (['noreply', 'no-reply', 'donotreply', 'mailer-daemon', 'postmaster', 'webmaster'].includes(local)) return false;
  return true;
}

/**
 * Extract emails from page content.
 */
async function extractEmailsFromPage(page) {
  const emails = new Set();

  try {
    // 1. Check mailto links
    const mailtoEmails = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('a[href^="mailto:"]'))
        .map(a => a.href.replace('mailto:', '').split('?')[0].trim().toLowerCase())
        .filter(Boolean);
    });
    mailtoEmails.forEach(e => emails.add(e));

    // 2. Check page text content for email patterns
    const textContent = await page.evaluate(() => document.body?.innerText || '');
    const textEmails = textContent.match(EMAIL_REGEX) || [];
    textEmails.forEach(e => emails.add(e.toLowerCase()));

    // 3. Check meta tags
    const metaEmails = await page.evaluate(() => {
      const results = [];
      document.querySelectorAll('meta').forEach(meta => {
        const content = meta.getAttribute('content') || '';
        const matches = content.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g);
        if (matches) results.push(...matches);
      });
      return results;
    });
    metaEmails.forEach(e => emails.add(e.toLowerCase()));

    // 4. Check structured data (JSON-LD)
    const ldEmails = await page.evaluate(() => {
      const results = [];
      document.querySelectorAll('script[type="application/ld+json"]').forEach(el => {
        try {
          const data = JSON.parse(el.textContent);
          if (data.email) results.push(data.email);
          if (data.contactPoint?.email) results.push(data.contactPoint.email);
        } catch {}
      });
      return results;
    });
    ldEmails.forEach(e => emails.add(e.toLowerCase()));

  } catch {
    // Page might have navigated away or errored — that's fine
  }

  return [...emails].map(cleanEmail).filter(isValidEmail);
}

/**
 * Find contact/about page URLs from the current page.
 */
async function findContactPages(page, baseDomain) {
  try {
    const links = await page.evaluate((domain) => {
      return Array.from(document.querySelectorAll('a[href]'))
        .map(a => ({ href: a.href, text: a.textContent.toLowerCase().trim() }))
        .filter(({ href, text }) => {
          try {
            const url = new URL(href);
            if (url.hostname.replace('www.', '') !== domain.replace('www.', '')) return false;
          } catch { return false; }
          return (
            text.includes('contact') || text.includes('about') ||
            text.includes('team') || text.includes('attorneys') ||
            text.includes('lawyers') || text.includes('staff') ||
            href.includes('/contact') || href.includes('/about') ||
            href.includes('/team') || href.includes('/attorneys') ||
            href.includes('/our-team') || href.includes('/our-firm')
          );
        })
        .map(({ href }) => href);
    }, baseDomain);

    // Deduplicate and limit
    return [...new Set(links)].slice(0, 3);
  } catch {
    return [];
  }
}

class EmailFinder {
  constructor(options = {}) {
    this.browser = null;
    this.rateLimiter = new RateLimiter({ minDelay: 3000, maxDelay: 6000 });
    this.proxy = options.proxy || null;
    this.stats = { visited: 0, found: 0, failed: 0 };
  }

  async init() {
    const launchOptions = {
      headless: true,
      protocolTimeout: 60000,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    };

    if (this.proxy) {
      launchOptions.args.push(`--proxy-server=${this.proxy}`);
    }

    this.browser = await puppeteer.launch(launchOptions);
  }

  async close() {
    if (this.browser) await this.browser.close();
  }

  /**
   * Find email for a firm by visiting their website.
   * Returns the best email found, or '' if none.
   */
  async findEmail(website) {
    if (!website) return '';
    this.stats.visited++;

    // Ensure URL has protocol
    let url = website.trim();
    if (!url.startsWith('http')) url = 'https://' + url;

    let baseDomain;
    try {
      baseDomain = new URL(url).hostname;
    } catch {
      log.warn(`Invalid URL: ${website}`);
      this.stats.failed++;
      return '';
    }

    const page = await this.browser.newPage();
    const ua = this.rateLimiter.getUserAgent();
    await page.setUserAgent(ua);
    await page.setDefaultTimeout(10000);

    const allEmails = new Set();

    try {
      // Visit homepage
      await this.rateLimiter.wait();
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });
      const homeEmails = await extractEmailsFromPage(page);
      homeEmails.forEach(e => allEmails.add(e));

      // If we already found emails, we might be done — but check contact page too
      const contactPages = await findContactPages(page, baseDomain);

      // Visit contact/about pages (up to 2 more)
      for (const contactUrl of contactPages.slice(0, 2)) {
        if (allEmails.size >= 3) break; // Enough emails
        try {
          await sleep(1500 + Math.random() * 2000);
          await page.goto(contactUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });
          const pageEmails = await extractEmailsFromPage(page);
          pageEmails.forEach(e => allEmails.add(e));
        } catch {
          // Contact page failed — that's fine
        }
      }
    } catch (err) {
      // Dead site, timeout, SSL error — all expected
      this.stats.failed++;
    } finally {
      await page.close().catch(() => {});
    }

    const emailList = [...allEmails];

    if (emailList.length > 0) {
      this.stats.found++;
      // Prefer emails on the firm's own domain
      const firmDomain = baseDomain.replace(/^www\./, '');
      const onDomainEmails = emailList.filter(e => e.endsWith('@' + firmDomain));
      // Prefer non-info@ addresses (more likely to be a specific person)
      const personalEmails = (onDomainEmails.length > 0 ? onDomainEmails : emailList)
        .filter(e => !e.startsWith('info@') && !e.startsWith('office@') && !e.startsWith('admin@'));

      return personalEmails[0] || onDomainEmails[0] || emailList[0];
    }

    return '';
  }

  /**
   * Find email for a specific person by visiting their firm website.
   * Scores emails by name match so we pick the right attorney's email.
   *
   * @param {string} website - Firm website URL
   * @param {string} firstName - Attorney first name
   * @param {string} lastName - Attorney last name
   * @returns {string} Best matching email, or '' if none found
   */
  async findEmailForPerson(website, firstName, lastName) {
    if (!website) return '';
    this.stats.visited++;

    let url = website.trim();
    if (!url.startsWith('http')) url = 'https://' + url;

    let baseDomain;
    try {
      baseDomain = new URL(url).hostname;
    } catch {
      log.warn(`Invalid URL: ${website}`);
      this.stats.failed++;
      return '';
    }

    const page = await this.browser.newPage();
    const ua = this.rateLimiter.getUserAgent();
    await page.setUserAgent(ua);
    await page.setDefaultTimeout(10000);

    const allEmails = new Set();
    const first = (firstName || '').toLowerCase();
    const last = (lastName || '').toLowerCase();

    try {
      // Visit homepage
      await this.rateLimiter.wait();
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });
      const homeEmails = await extractEmailsFromPage(page);
      homeEmails.forEach(e => allEmails.add(e));

      // Find contact and attorney-specific pages
      const contactPages = await findContactPages(page, baseDomain);

      // Also look for attorney-specific bio pages
      if (last) {
        const bioPages = await this._findAttorneyBioPages(page, baseDomain, first, last);
        contactPages.push(...bioPages);
      }

      // Visit contact/bio pages (up to 3)
      const uniquePages = [...new Set(contactPages)].slice(0, 3);
      for (const contactUrl of uniquePages) {
        if (allEmails.size >= 5) break;
        try {
          await sleep(1500 + Math.random() * 2000);
          await page.goto(contactUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });
          const pageEmails = await extractEmailsFromPage(page);
          pageEmails.forEach(e => allEmails.add(e));
        } catch {
          // Failed — that's fine
        }
      }
    } catch (err) {
      this.stats.failed++;
    } finally {
      await page.close().catch(() => {});
    }

    const emailList = [...allEmails];
    if (emailList.length === 0) return '';

    this.stats.found++;

    // Score emails by name match
    const firmDomain = baseDomain.replace(/^www\./, '');
    return this._scoreEmails(emailList, first, last, firmDomain);
  }

  /**
   * Find attorney-specific bio pages on a firm website.
   */
  async _findAttorneyBioPages(page, baseDomain, first, last) {
    try {
      const links = await page.evaluate((domain, firstName, lastName) => {
        return Array.from(document.querySelectorAll('a[href]'))
          .map(a => ({ href: a.href, text: a.textContent.toLowerCase().trim() }))
          .filter(({ href, text }) => {
            try {
              const url = new URL(href);
              if (url.hostname.replace('www.', '') !== domain.replace('www.', '')) return false;
            } catch { return false; }
            // Match links containing the attorney's last name
            const hrefLower = href.toLowerCase();
            return (
              text.includes(lastName) ||
              hrefLower.includes(lastName) ||
              (firstName && text.includes(firstName) && text.includes(lastName))
            );
          })
          .map(({ href }) => href);
      }, baseDomain, first, last);

      return [...new Set(links)].slice(0, 2);
    } catch {
      return [];
    }
  }

  /**
   * Score emails by name match and return the best one.
   * Prefers: exact name match > last name match > firm domain > any valid email
   * Skips: generic addresses (info@, office@, admin@, contact@, support@)
   */
  _scoreEmails(emails, first, last, firmDomain) {
    const GENERIC_PREFIXES = ['info', 'office', 'admin', 'contact', 'support', 'hello', 'enquiries', 'reception', 'mail'];
    const scored = emails.map(email => {
      const local = email.split('@')[0].toLowerCase();
      const domain = email.split('@')[1]?.toLowerCase() || '';
      let score = 0;

      // Skip generic addresses
      if (GENERIC_PREFIXES.includes(local)) {
        score = -10;
        return { email, score };
      }

      // On firm domain
      if (domain === firmDomain) score += 5;

      // Name matching
      if (first && last) {
        // john.smith@ or jsmith@ or smith.john@
        if (local.includes(first) && local.includes(last)) score += 20;
        else if (local.includes(last)) score += 10;
        else if (local.includes(first)) score += 5;
        // First initial + last: jsmith@
        if (first && local === first[0] + last) score += 15;
        // first.last@ or first_last@
        if (local === `${first}.${last}` || local === `${first}_${last}`) score += 20;
      }

      return { email, score };
    });

    scored.sort((a, b) => b.score - a.score);
    // Return best non-generic email
    const best = scored.find(s => s.score >= 0);
    return best ? best.email : '';
  }

  /**
   * Batch find emails for multiple leads.
   * Groups by domain to avoid crawling the same firm twice.
   *
   * @param {object[]} leads - Array of leads with website, first_name, last_name
   * @param {function} [onProgress] - Callback(current, total, leadName)
   * @param {function} [isCancelled] - Returns true if operation should stop
   * @param {object} [domainCache] - DomainEmailCache instance (optional)
   * @returns {object} Stats: { emailsFound, websitesVisited, skipped }
   */
  async batchFindEmails(leads, onProgress, isCancelled, domainCache) {
    const stats = { emailsFound: 0, websitesVisited: 0, skipped: 0 };

    // Group leads by domain for efficiency
    const domainGroups = new Map();
    for (const lead of leads) {
      if (lead.email || !lead.website) {
        stats.skipped++;
        continue;
      }
      let domain;
      try {
        const url = lead.website.startsWith('http') ? lead.website : 'https://' + lead.website;
        domain = new URL(url).hostname.replace(/^www\./, '');
      } catch {
        stats.skipped++;
        continue;
      }
      if (!domainGroups.has(domain)) {
        domainGroups.set(domain, []);
      }
      domainGroups.get(domain).push(lead);
    }

    let processed = 0;
    const total = leads.filter(l => !l.email && l.website).length;

    for (const [domain, domainLeads] of domainGroups) {
      if (isCancelled && isCancelled()) break;

      // Check domain cache first
      if (domainCache) {
        const cached = domainCache.get(domain);
        if (cached) {
          for (const lead of domainLeads) {
            const email = this._scoreEmails(
              cached.emails,
              (lead.first_name || '').toLowerCase(),
              (lead.last_name || '').toLowerCase(),
              domain
            );
            if (email) {
              lead.email = email;
              lead.email_source = 'website-crawl';
              stats.emailsFound++;
            }
            processed++;
            if (onProgress) onProgress(processed, total, `${lead.first_name} ${lead.last_name}`);
          }
          continue;
        }
      }

      // Crawl the first lead's website to get all emails for this domain
      const firstLead = domainLeads[0];
      stats.websitesVisited++;

      // Use findEmail to get ALL emails from this website
      const url = firstLead.website.startsWith('http') ? firstLead.website : 'https://' + firstLead.website;
      const page = await this.browser.newPage();
      const ua = this.rateLimiter.getUserAgent();
      await page.setUserAgent(ua);
      await page.setDefaultTimeout(10000);

      const allEmails = new Set();
      try {
        await this.rateLimiter.wait();
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });
        const homeEmails = await extractEmailsFromPage(page);
        homeEmails.forEach(e => allEmails.add(e));

        const baseDomain = new URL(url).hostname;
        const contactPages = await findContactPages(page, baseDomain);

        for (const contactUrl of contactPages.slice(0, 2)) {
          try {
            await sleep(1500 + Math.random() * 2000);
            await page.goto(contactUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });
            const pageEmails = await extractEmailsFromPage(page);
            pageEmails.forEach(e => allEmails.add(e));
          } catch {}
        }
      } catch {} finally {
        await page.close().catch(() => {});
      }

      const emailList = [...allEmails];

      // Store in domain cache
      if (domainCache && emailList.length > 0) {
        domainCache.set(domain, { emails: emailList, timestamp: Date.now() });
      }

      // Match each lead at this domain
      for (const lead of domainLeads) {
        if (emailList.length > 0) {
          const email = this._scoreEmails(
            emailList,
            (lead.first_name || '').toLowerCase(),
            (lead.last_name || '').toLowerCase(),
            domain
          );
          if (email) {
            lead.email = email;
            lead.email_source = 'website-crawl';
            stats.emailsFound++;
          }
        }
        processed++;
        if (onProgress) onProgress(processed, total, `${lead.first_name} ${lead.last_name}`);
      }
    }

    return stats;
  }

  getStats() {
    return { ...this.stats };
  }
}

module.exports = EmailFinder;
