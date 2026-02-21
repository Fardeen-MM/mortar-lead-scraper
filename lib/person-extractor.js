/**
 * Person Extractor — extract individual people from business websites
 *
 * Given a business website, finds people (staff, team members, partners)
 * by analyzing team/about pages for names, titles, emails, and phones.
 *
 * Extraction strategies (tried in order):
 *   1. JSON-LD / Microdata — structured Person schema markup
 *   2. Card patterns — .team-member, .staff-card, .person, .bio-card selectors
 *   3. Heading heuristic — <h2>/<h3>/<h4> with proper-case 2-3 word names
 *   4. Image alt text — <img alt="John Smith"> near card containers
 *
 * Integrates with existing email-finder.js and email-verifier.js for
 * finding/verifying emails for extracted people.
 */

const puppeteer = require('puppeteer');
const { log } = require('./logger');
const { RateLimiter, sleep } = require('./rate-limiter');

// Common team/about page paths to check
const TEAM_PATHS = [
  '/team', '/our-team', '/about', '/about-us', '/staff', '/people',
  '/attorneys', '/lawyers', '/professionals', '/who-we-are',
  '/meet-the-team', '/our-people', '/leadership', '/partners',
  '/our-staff', '/our-doctors', '/dentists', '/providers',
];

// Name validation regex — 2-4 word proper-case names
const NAME_REGEX = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,3}$/;

// Title/role keywords that indicate a person's professional role
const TITLE_KEYWORDS = [
  'partner', 'associate', 'director', 'manager', 'founder',
  'owner', 'ceo', 'cto', 'cfo', 'coo', 'president', 'vp',
  'vice president', 'principal', 'counsel', 'attorney',
  'dentist', 'doctor', 'physician', 'surgeon', 'therapist',
  'broker', 'agent', 'consultant', 'advisor', 'planner',
  'engineer', 'technician', 'specialist', 'coordinator',
  'dds', 'dmd', 'md', 'do', 'esq', 'phd', 'rn', 'pa-c',
];

// Words that look like names but aren't
const FALSE_NAME_WORDS = new Set([
  'our team', 'the team', 'about us', 'contact us', 'get started',
  'learn more', 'read more', 'view more', 'see more', 'meet our',
  'our story', 'our mission', 'free consultation', 'schedule now',
  'book now', 'get in touch', 'call now', 'email us', 'follow us',
  'privacy policy', 'terms of service', 'all rights reserved',
  'powered by', 'designed by', 'built by', 'copyright',
]);

class PersonExtractor {
  constructor(options = {}) {
    this._browser = null;
    this._proxy = options.proxy;
  }

  async init() {
    let pup;
    try {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteerExtra.use(StealthPlugin());
      pup = puppeteerExtra;
    } catch {
      pup = puppeteer;
    }

    const launchOpts = {
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    };

    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this._browser = await pup.launch(launchOpts);
    log.info('[PersonExtractor] Browser launched');
  }

  async close() {
    if (this._browser) {
      await this._browser.close().catch(() => {});
      this._browser = null;
    }
  }

  /**
   * Extract people from a single website.
   * @param {string} website - Base URL of the business
   * @returns {object[]} Array of { first_name, last_name, title, email, phone, linkedin_url }
   */
  async extractPeople(website) {
    if (!this._browser) throw new Error('Browser not initialized — call init() first');
    if (!website) return [];

    // Normalize URL
    let baseUrl = website.trim();
    if (!/^https?:\/\//i.test(baseUrl)) baseUrl = 'https://' + baseUrl;
    baseUrl = baseUrl.replace(/\/+$/, '');

    const allPeople = new Map(); // Dedup by name

    const page = await this._browser.newPage();

    try {
      // Block heavy resources
      await page.setRequestInterception(true);
      page.on('request', (req) => {
        const type = req.resourceType();
        if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      await page.setViewport({ width: 1280, height: 900 });

      // 1. Try the homepage first
      await this._extractFromUrl(page, baseUrl, allPeople);

      // 2. Try team/about pages
      for (const path of TEAM_PATHS) {
        if (allPeople.size >= 50) break; // Cap at 50 people per site

        const url = baseUrl + path;
        try {
          const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });
          if (response && response.status() === 200) {
            await this._extractFromUrl(page, url, allPeople, true);
          }
        } catch {
          // Page doesn't exist or timeout — skip
        }
      }
    } catch (err) {
      log.warn(`[PersonExtractor] Error on ${baseUrl}: ${err.message}`);
    } finally {
      await page.close().catch(() => {});
    }

    return Array.from(allPeople.values());
  }

  /**
   * Extract people from a single page URL.
   */
  async _extractFromUrl(page, url, peopleMap, alreadyNavigated = false) {
    try {
      if (!alreadyNavigated) {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
      }

      await sleep(500); // Let JS render

      const pageData = await page.evaluate(() => {
        const data = { jsonLd: [], cards: [], headings: [], imgAlts: [] };

        // --- Strategy 1: JSON-LD ---
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of scripts) {
          try {
            const json = JSON.parse(script.textContent);
            const items = Array.isArray(json) ? json : [json];
            for (const item of items) {
              if (item['@type'] === 'Person' || item['@type'] === 'Physician' || item['@type'] === 'Dentist') {
                data.jsonLd.push({
                  name: item.name || '',
                  title: item.jobTitle || item.description || '',
                  email: item.email || '',
                  phone: item.telephone || '',
                  url: item.url || '',
                  image: item.image || '',
                });
              }
              // Check for Organization with employees
              if (item.employee) {
                const employees = Array.isArray(item.employee) ? item.employee : [item.employee];
                for (const emp of employees) {
                  if (typeof emp === 'object') {
                    data.jsonLd.push({
                      name: emp.name || '',
                      title: emp.jobTitle || '',
                      email: emp.email || '',
                      phone: emp.telephone || '',
                      url: emp.url || '',
                    });
                  }
                }
              }
              // Check for members
              if (item.member) {
                const members = Array.isArray(item.member) ? item.member : [item.member];
                for (const m of members) {
                  if (typeof m === 'object') {
                    data.jsonLd.push({
                      name: m.name || '',
                      title: m.jobTitle || '',
                      email: m.email || '',
                    });
                  }
                }
              }
            }
          } catch {}
        }

        // --- Strategy 2: Card patterns ---
        const cardSelectors = [
          '.team-member', '.staff-card', '.person', '.bio-card',
          '.team-card', '.member-card', '.profile-card', '.doctor-card',
          '.attorney-card', '.lawyer-card', '.provider-card',
          '[class*="team-member"]', '[class*="staff"]', '[class*="person-card"]',
          '[class*="bio-card"]', '[class*="team_member"]', '[class*="team-item"]',
          '.et_pb_team_member', '.elementor-team-member', // Common page builders
          '.wp-block-team-member',
        ];

        for (const sel of cardSelectors) {
          const cards = document.querySelectorAll(sel);
          for (const card of cards) {
            const nameEl = card.querySelector('h2, h3, h4, h5, .name, .title, [class*="name"]');
            const titleEl = card.querySelector('.position, .role, .title, .designation, [class*="position"], [class*="role"], [class*="title"]');
            const emailEl = card.querySelector('a[href^="mailto:"]');
            const phoneEl = card.querySelector('a[href^="tel:"]');
            const linkedinEl = card.querySelector('a[href*="linkedin.com"]');

            if (nameEl) {
              data.cards.push({
                name: nameEl.textContent.trim(),
                title: titleEl ? titleEl.textContent.trim() : '',
                email: emailEl ? emailEl.href.replace('mailto:', '').split('?')[0] : '',
                phone: phoneEl ? phoneEl.href.replace('tel:', '') : '',
                linkedin: linkedinEl ? linkedinEl.href : '',
              });
            }
          }
        }

        // --- Strategy 3: Heading heuristic ---
        const headings = document.querySelectorAll('h2, h3, h4');
        for (const h of headings) {
          const text = h.textContent.trim();
          // Skip headings that are too long or too short
          if (text.length < 4 || text.length > 50) continue;
          // Skip common non-name headings
          if (/team|staff|about|contact|service|practice|our |the /i.test(text)) continue;

          // Look for title/role in adjacent sibling or parent
          let title = '';
          const nextEl = h.nextElementSibling;
          if (nextEl) {
            const nextText = nextEl.textContent.trim();
            if (nextText.length < 80) title = nextText;
          }

          // Look for email in nearby elements
          let email = '';
          let phone = '';
          let linkedin = '';
          const parent = h.parentElement;
          if (parent) {
            const emailLink = parent.querySelector('a[href^="mailto:"]');
            if (emailLink) email = emailLink.href.replace('mailto:', '').split('?')[0];
            const phoneLink = parent.querySelector('a[href^="tel:"]');
            if (phoneLink) phone = phoneLink.href.replace('tel:', '');
            const liLink = parent.querySelector('a[href*="linkedin.com"]');
            if (liLink) linkedin = liLink.href;
          }

          data.headings.push({ name: text, title, email, phone, linkedin });
        }

        // --- Strategy 4: Image alt text ---
        const imgs = document.querySelectorAll('img[alt]');
        for (const img of imgs) {
          const alt = img.getAttribute('alt') || '';
          if (alt.length < 4 || alt.length > 50) continue;
          if (/logo|icon|banner|header|background|placeholder/i.test(alt)) continue;

          // Check if parent has card-like structure
          const parent = img.closest('div, article, li, figure');
          let title = '';
          if (parent) {
            const titleEl = parent.querySelector('.position, .role, .title, [class*="position"], [class*="role"]');
            if (titleEl) title = titleEl.textContent.trim();
          }

          data.imgAlts.push({ name: alt, title });
        }

        return data;
      });

      // Process extracted data into people
      // Priority: JSON-LD > Cards > Headings > Image Alts

      // JSON-LD people
      for (const person of pageData.jsonLd) {
        this._addPerson(peopleMap, person.name, person.title, person.email, person.phone, person.url);
      }

      // Card people
      for (const card of pageData.cards) {
        this._addPerson(peopleMap, card.name, card.title, card.email, card.phone, card.linkedin);
      }

      // Heading people (only add if name matches pattern)
      for (const h of pageData.headings) {
        if (this._looksLikeName(h.name)) {
          this._addPerson(peopleMap, h.name, h.title, h.email, h.phone, h.linkedin);
        }
      }

      // Image alt people (only add if name matches pattern)
      for (const img of pageData.imgAlts) {
        if (this._looksLikeName(img.name)) {
          this._addPerson(peopleMap, img.name, img.title, '', '', '');
        }
      }
    } catch (err) {
      log.warn(`[PersonExtractor] Page extraction error on ${url}: ${err.message}`);
    }
  }

  /**
   * Check if text looks like a person's name.
   */
  _looksLikeName(text) {
    if (!text) return false;
    const cleaned = text.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn)$/gi, '')
      .trim();

    if (cleaned.length < 4 || cleaned.length > 40) return false;
    if (FALSE_NAME_WORDS.has(cleaned.toLowerCase())) return false;
    if (/\d/.test(cleaned)) return false; // Names don't have digits
    if (NAME_REGEX.test(cleaned)) return true;

    // Looser check: 2-3 words, first letter uppercase
    const parts = cleaned.split(/\s+/);
    if (parts.length < 2 || parts.length > 4) return false;
    return parts.every(p => /^[A-Z]/.test(p));
  }

  /**
   * Check if text looks like a professional title.
   */
  _looksLikeTitle(text) {
    if (!text || text.length > 100) return false;
    const lower = text.toLowerCase();
    return TITLE_KEYWORDS.some(kw => lower.includes(kw));
  }

  /**
   * Add a person to the map, deduplicating by normalized name.
   */
  _addPerson(peopleMap, nameRaw, title, email, phone, linkedinUrl) {
    if (!nameRaw) return;

    // Clean name
    const name = nameRaw.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    if (!this._looksLikeName(name)) return;

    const parts = name.split(/\s+/);
    const firstName = parts[0];
    const lastName = parts[parts.length - 1];

    const key = `${firstName.toLowerCase()}_${lastName.toLowerCase()}`;

    // Only use title if it looks like a real professional title
    const cleanTitle = (title && this._looksLikeTitle(title)) ? title.trim() : '';

    // Clean email
    const cleanEmail = (email || '').toLowerCase().trim();
    const validEmail = cleanEmail && cleanEmail.includes('@') && !cleanEmail.includes('example.com')
      ? cleanEmail : '';

    // Clean phone
    const cleanPhone = (phone || '').replace(/[^0-9+\-() .]/g, '').trim();

    // Clean LinkedIn
    let cleanLinkedIn = '';
    if (linkedinUrl && /linkedin\.com/i.test(linkedinUrl)) {
      cleanLinkedIn = linkedinUrl;
    }

    if (peopleMap.has(key)) {
      // Merge: fill missing fields
      const existing = peopleMap.get(key);
      if (!existing.title && cleanTitle) existing.title = cleanTitle;
      if (!existing.email && validEmail) existing.email = validEmail;
      if (!existing.phone && cleanPhone) existing.phone = cleanPhone;
      if (!existing.linkedin_url && cleanLinkedIn) existing.linkedin_url = cleanLinkedIn;
    } else {
      peopleMap.set(key, {
        first_name: firstName,
        last_name: lastName,
        title: cleanTitle,
        email: validEmail,
        phone: cleanPhone,
        linkedin_url: cleanLinkedIn,
      });
    }
  }

  /**
   * Extract people from multiple businesses.
   * @param {object[]} businesses - Array of { firm_name, website, city, state, phone }
   * @param {function} onProgress - Callback(current, total, businessName)
   * @param {function} isCancelled - Returns true if cancelled
   * @returns {{ peopleFound: number, websitesVisited: number, results: object[] }}
   */
  async batchExtract(businesses, onProgress, isCancelled) {
    const rateLimiter = new RateLimiter({ minDelay: 3000, maxDelay: 6000 });
    const results = [];
    let websitesVisited = 0;
    let totalPeople = 0;

    const withWebsite = businesses.filter(b => b.website);

    for (let i = 0; i < withWebsite.length; i++) {
      if (isCancelled && isCancelled()) break;

      const biz = withWebsite[i];

      if (onProgress) onProgress(i + 1, withWebsite.length, biz.firm_name || biz.website);

      await rateLimiter.wait();

      try {
        const people = await this.extractPeople(biz.website);
        websitesVisited++;

        for (const person of people) {
          // Inherit business data
          results.push({
            ...person,
            firm_name: biz.firm_name || '',
            city: biz.city || '',
            state: biz.state || '',
            website: biz.website || '',
            firm_phone: biz.phone || '',
          });
          totalPeople++;
        }

        if (people.length > 0) {
          log.info(`[PersonExtractor] Found ${people.length} people at ${biz.firm_name || biz.website}`);
        }
      } catch (err) {
        log.warn(`[PersonExtractor] Failed for ${biz.website}: ${err.message}`);
      }
    }

    return { peopleFound: totalPeople, websitesVisited, results };
  }
}

module.exports = PersonExtractor;
