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
function isValidEmail(email) {
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

  return [...emails].filter(isValidEmail);
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

  getStats() {
    return { ...this.stats };
  }
}

module.exports = EmailFinder;
