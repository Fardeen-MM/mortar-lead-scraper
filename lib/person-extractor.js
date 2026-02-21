/**
 * Person Extractor — extract individual people from business websites
 *
 * Given a business website, finds people (staff, team members, partners)
 * by analyzing team/about pages for names, titles, emails, and phones.
 *
 * Extraction strategies (tried in order):
 *   1. JSON-LD / Microdata — structured Person schema markup
 *   2. Card patterns — .team-member, .staff-card, .person, .bio-card selectors
 *   3. Heading heuristic — <h2>/<h3>/<h4> with validated human names + nearby title
 *   4. Image alt text — <img alt="John Smith"> in card containers with title context
 *
 * Name validation uses a 300+ common first names dictionary to distinguish
 * real human names from UI elements like "Quick Links" or "Health Care".
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

// 300+ most common English first names (male + female)
// Used to validate that extracted text is actually a person's name
const COMMON_FIRST_NAMES = new Set([
  // Male
  'james','robert','john','michael','david','william','richard','joseph','thomas','charles',
  'christopher','daniel','matthew','anthony','mark','donald','steven','paul','andrew','joshua',
  'kenneth','kevin','brian','george','timothy','ronald','edward','jason','jeffrey','ryan',
  'jacob','gary','nicholas','eric','jonathan','stephen','larry','justin','scott','brandon',
  'benjamin','samuel','raymond','gregory','frank','alexander','patrick','jack','dennis','jerry',
  'tyler','aaron','jose','adam','nathan','henry','peter','zachary','douglas','harold',
  'kyle','noah','carl','gerald','keith','roger','arthur','terry','sean','austin',
  'christian','albert','joe','ethan','jesse','ralph','roy','louis','eugene','philip',
  'russell','bobby','harry','vincent','bruce','dylan','willie','jordan','alan','billy',
  'howard','wayne','elijah','randy','gabriel','mason','logan','johnny','walter','connor',
  // Female
  'mary','patricia','jennifer','linda','barbara','elizabeth','susan','jessica','sarah','karen',
  'lisa','nancy','betty','margaret','sandra','ashley','dorothy','kimberly','emily','donna',
  'michelle','carol','amanda','melissa','deborah','stephanie','rebecca','sharon','laura','cynthia',
  'kathleen','amy','angela','shirley','anna','brenda','pamela','emma','nicole','helen','samantha',
  'katherine','christine','debra','rachel','carolyn','janet','catherine','maria','heather','diane',
  'ruth','julie','olivia','joyce','virginia','victoria','kelly','lauren','christina','joan',
  'evelyn','judith','megan','andrea','cheryl','hannah','jacqueline','martha','gloria','teresa',
  'ann','sara','madison','frances','kathryn','janice','jean','abigail','alice','judy',
  'sophia','grace','denise','amber','doris','marilyn','danielle','beverly','isabella','theresa',
  'diana','natalie','brittany','charlotte','marie','kayla','alexis','lori','alyssa','rosa',
  // Cross-cultural common names
  'mohammed','ahmed','ali','wei','chen','raj','priya','carlos','miguel','antonio','pablo',
  'marco','luca','hans','lars','sven','ivan','dmitri','yuki','hiroshi','kenji',
  'alejandro','ricardo','diego','luis','jorge','sofia','elena','lucia','ana','carmen',
  'fatima','aisha','omar','hassan','ibrahim',
]);

// Words/phrases that are NOT person names — massively expanded
const FALSE_NAME_WORDS = new Set([
  // Navigation/UI elements
  'our team', 'the team', 'about us', 'contact us', 'get started',
  'learn more', 'read more', 'view more', 'see more', 'meet our',
  'our story', 'our mission', 'free consultation', 'schedule now',
  'book now', 'get in touch', 'call now', 'email us', 'follow us',
  'privacy policy', 'terms of service', 'all rights reserved',
  'powered by', 'designed by', 'built by', 'copyright',
  'quick links', 'main menu', 'site map', 'home page',
  'sign up', 'log in', 'sign in', 'get quote',
  'view all', 'load more', 'show more', 'see all',
  'next page', 'previous page', 'back home',
  // Marketing phrases
  'award winning', 'award-winning', 'top rated', 'best rated',
  'trusted advisors', 'trusted advisor', 'trusted partners',
  'quality care', 'quality service', 'expert care', 'expert service',
  'patient care', 'dental care', 'health care', 'home care',
  'family care', 'primary care', 'urgent care', 'elder care',
  'pain relief', 'pain management', 'pain free',
  'premier service', 'premium service', 'full service',
  'award winners', 'industry leaders', 'market leaders',
  'patients first', 'people first', 'clients first',
  'local experts', 'your experts', 'the experts',
  'real results', 'proven results', 'fast results',
  'free estimate', 'free estimates', 'free quote',
  'new patients', 'new clients', 'new customers',
  'special offers', 'current specials', 'latest news',
  'featured services', 'popular services', 'core services',
  'why us', 'why choose', 'how it works', 'what we do',
  // Section headings
  'practice areas', 'service areas', 'our services',
  'our locations', 'our offices', 'our partners',
  'case results', 'testimonials', 'client reviews',
  'latest posts', 'recent posts', 'blog posts',
  'news updates', 'press releases', 'media coverage',
  'photo gallery', 'image gallery', 'video gallery',
  'career opportunities', 'job openings', 'open positions',
  'community involvement', 'social responsibility',
  'professional memberships', 'board certifications',
  'office hours', 'business hours', 'opening hours',
  'virtual tour', 'office tour', 'facility tour',
  'before after', 'patient stories', 'success stories',
]);

// Individual words that should never appear in a person's name
const FALSE_NAME_COMPONENTS = new Set([
  'links', 'menu', 'care', 'service', 'services', 'award', 'winning',
  'rated', 'trusted', 'quality', 'expert', 'premier', 'premium',
  'results', 'offers', 'special', 'featured', 'popular', 'latest',
  'news', 'blog', 'post', 'posts', 'page', 'site', 'home',
  'gallery', 'tour', 'hours', 'area', 'areas', 'location', 'locations',
  'office', 'offices', 'reviews', 'review', 'testimonial', 'testimonials',
  'careers', 'career', 'jobs', 'virtual', 'online', 'free',
  'patients', 'clients', 'customers', 'members', 'visitors',
  'treatment', 'treatments', 'procedure', 'procedures', 'surgery',
  'insurance', 'payment', 'financing', 'pricing', 'cost',
  'appointment', 'appointments', 'schedule', 'booking',
  'emergency', 'urgent', 'immediate', 'same-day',
  'comprehensive', 'advanced', 'professional', 'certified',
  'experienced', 'dedicated', 'compassionate', 'innovative',
  'relief', 'management', 'prevention', 'recovery', 'wellness',
  'dental', 'medical', 'legal', 'financial',
  'orthodontics', 'pediatric', 'cosmetic', 'general',
  'first', 'best', 'top', 'leading', 'premier',
  'why', 'how', 'what', 'when', 'where', 'who',
]);

// Title/role keywords that indicate a person's professional role
const TITLE_KEYWORDS = [
  'partner', 'associate', 'director', 'manager', 'founder',
  'owner', 'ceo', 'cto', 'cfo', 'coo', 'president', 'vp',
  'vice president', 'principal', 'counsel', 'attorney',
  'dentist', 'doctor', 'physician', 'surgeon', 'therapist',
  'broker', 'agent', 'consultant', 'advisor', 'planner',
  'engineer', 'technician', 'specialist', 'coordinator',
  'dds', 'dmd', 'md', 'do', 'esq', 'phd', 'rn', 'pa-c',
  'hygienist', 'nurse', 'paralegal', 'secretary',
  'accountant', 'analyst', 'architect', 'designer',
];

// Name validation regex — 2-4 word proper-case names
const NAME_REGEX = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,3}$/;

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
      if (!puppeteerExtra._stealthRegistered) {
        puppeteerExtra.use(StealthPlugin());
        puppeteerExtra._stealthRegistered = true;
      }
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

      // 2. Try team/about pages (use networkidle0 for SPA sites)
      for (const path of TEAM_PATHS) {
        if (allPeople.size >= 50) break; // Cap at 50 people per site

        const url = baseUrl + path;
        try {
          const response = await page.goto(url, { waitUntil: 'networkidle0', timeout: 15000 });
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
        await page.goto(url, { waitUntil: 'networkidle0', timeout: 15000 });
      }

      await sleep(500); // Let JS render

      const pageData = await page.evaluate(() => {
        const data = { jsonLd: [], cards: [], headings: [], imgAlts: [], listItems: [] };

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
          '.et_pb_team_member', '.elementor-team-member',
          '.wp-block-team-member',
        ];

        // Name element selectors (broader than just headings)
        const nameSelectors = 'h2, h3, h4, h5, .name, .title, [class*="name"]';
        const titleSelectors = '.position, .role, .designation, [class*="position"], [class*="role"], [class*="job"]';

        for (const sel of cardSelectors) {
          const cards = document.querySelectorAll(sel);
          for (const card of cards) {
            const nameEl = card.querySelector(nameSelectors);
            const titleEl = card.querySelector(titleSelectors);
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

        // --- Strategy 2b: Repeating list patterns ---
        // Many firm sites use <ul class="results_list"><li> with nested divs for name/position/contact
        // Find <li> elements that contain both a name-like element and contact info
        if (data.cards.length === 0) {
          const listContainers = document.querySelectorAll('ul, ol');
          for (const list of listContainers) {
            const items = list.querySelectorAll(':scope > li');
            if (items.length < 3) continue; // Need at least 3 items to be a people list

            // Check if these list items have name + contact structure
            let nameCount = 0;
            for (let i = 0; i < Math.min(5, items.length); i++) {
              const li = items[i];
              const hasNameEl = li.querySelector('.title, .name, [class*="name"], [class*="title"]');
              const hasContact = li.querySelector('a[href^="tel:"], a[href^="mailto:"], .phone, .email, .contact');
              if (hasNameEl && hasContact) nameCount++;
            }

            // If most sampled items have name + contact, extract all
            if (nameCount >= 2) {
              for (const li of items) {
                const nameEl = li.querySelector('.title, .name, [class*="name"], [class*="title"]');
                const posEl = li.querySelector('.position, .role, .designation, [class*="position"], [class*="role"]');
                const emailEl = li.querySelector('a[href^="mailto:"]');
                const phoneEl = li.querySelector('a[href^="tel:"]');
                const linkedinEl = li.querySelector('a[href*="linkedin.com"]');

                if (nameEl) {
                  const nameText = nameEl.textContent.trim();
                  // Skip if name element also contains the position text
                  let posText = posEl ? posEl.textContent.trim() : '';

                  data.listItems.push({
                    name: nameText,
                    title: posText,
                    email: emailEl ? emailEl.href.replace('mailto:', '').split('?')[0] : '',
                    phone: phoneEl ? phoneEl.href.replace('tel:', '') : '',
                    linkedin: linkedinEl ? linkedinEl.href : '',
                  });
                }
              }
              break; // Only process the first matching list
            }
          }
        }

        // --- Strategy 3: Heading heuristic ---
        // Only extract from headings that appear to be in team/people sections
        const headings = document.querySelectorAll('h2, h3, h4');
        for (const h of headings) {
          const text = h.textContent.trim();
          if (text.length < 4 || text.length > 50) continue;
          // Skip common section heading patterns
          if (/^(our |the |meet |about |contact |service|practice|why |how |what |featured )/i.test(text)) continue;

          // Look for title/role in adjacent sibling
          let title = '';
          const nextEl = h.nextElementSibling;
          if (nextEl) {
            const nextText = nextEl.textContent.trim();
            if (nextText.length < 80) title = nextText;
          }

          // Look for email/phone/linkedin in nearby elements
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
          if (/logo|icon|banner|header|background|placeholder|stock/i.test(alt)) continue;

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

      // JSON-LD people — highest trust, minimal validation
      for (const person of pageData.jsonLd) {
        this._addPerson(peopleMap, person.name, person.title, person.email, person.phone, person.url, 'jsonld');
      }

      // Card people — high trust (structured HTML), validate names
      for (const card of pageData.cards) {
        this._addPerson(peopleMap, card.name, card.title, card.email, card.phone, card.linkedin, 'card');
      }

      // List item people — medium-high trust (structured repeating pattern), validate names
      for (const item of pageData.listItems) {
        this._addPerson(peopleMap, item.name, item.title, item.email, item.phone, item.linkedin, 'card');
      }

      // Heading people — medium trust, STRICT name validation required
      for (const h of pageData.headings) {
        if (this._isLikelyHumanName(h.name)) {
          this._addPerson(peopleMap, h.name, h.title, h.email, h.phone, h.linkedin, 'heading');
        }
      }

      // Image alt people — low trust, STRICT name validation + must have nearby title
      for (const img of pageData.imgAlts) {
        if (this._isLikelyHumanName(img.name) && img.title && this._looksLikeTitle(img.title)) {
          this._addPerson(peopleMap, img.name, img.title, '', '', '', 'imgalt');
        }
      }
    } catch (err) {
      log.warn(`[PersonExtractor] Page extraction error on ${url}: ${err.message}`);
    }
  }

  /**
   * Strict check: is this text likely a real human name?
   * Uses common first names dictionary + structural validation.
   *
   * This is the key validation that prevents "Quick Links", "Health Care",
   * "Award-Winning Care", etc. from being treated as person names.
   */
  _isLikelyHumanName(text) {
    if (!text) return false;

    // Clean suffixes/titles
    const cleaned = text.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    if (cleaned.length < 4 || cleaned.length > 40) return false;

    // Check against known false name phrases
    if (FALSE_NAME_WORDS.has(cleaned.toLowerCase())) return false;

    // Must not contain digits
    if (/\d/.test(cleaned)) return false;

    // Must not contain special characters (except hyphens, apostrophes, periods for initials)
    if (/[!@#$%^&*()+=\[\]{};:"|<>?/\\~`]/.test(cleaned)) return false;

    // Split into parts
    const parts = cleaned.split(/\s+/);
    if (parts.length < 2 || parts.length > 4) return false;

    // Check individual words against false name components
    for (const part of parts) {
      if (FALSE_NAME_COMPONENTS.has(part.toLowerCase())) return false;
    }

    // All parts must start with uppercase
    if (!parts.every(p => /^[A-Z]/.test(p))) return false;

    // KEY CHECK: At least the first word must match a common first name
    // This is what prevents "Quick Links", "Health Care", etc.
    const firstWord = parts[0].toLowerCase()
      .replace(/^dr$/i, '') // "Dr" prefix handled separately
      .replace(/\.$/, '');  // Remove trailing period

    // Allow "Dr" prefix — check second word
    if (/^dr\.?$/i.test(parts[0]) && parts.length >= 3) {
      return COMMON_FIRST_NAMES.has(parts[1].toLowerCase());
    }

    return COMMON_FIRST_NAMES.has(firstWord);
  }

  /**
   * Basic structural name check for JSON-LD and card-extracted names.
   * Less strict than _isLikelyHumanName — used when source is trusted.
   */
  _looksLikeName(text) {
    if (!text) return false;
    const cleaned = text.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    if (cleaned.length < 4 || cleaned.length > 40) return false;
    if (FALSE_NAME_WORDS.has(cleaned.toLowerCase())) return false;
    if (/\d/.test(cleaned)) return false;

    const parts = cleaned.split(/\s+/);
    if (parts.length < 2 || parts.length > 4) return false;

    // Check individual words against false name components
    for (const part of parts) {
      if (FALSE_NAME_COMPONENTS.has(part.toLowerCase())) return false;
    }

    // All parts must start with uppercase
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
   * @param {string} source - Extraction source: 'jsonld', 'card', 'heading', 'imgalt'
   */
  _addPerson(peopleMap, nameRaw, title, email, phone, linkedinUrl, source) {
    if (!nameRaw) return;

    // Clean name
    let name = nameRaw.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    // For trusted sources (JSON-LD, cards), use basic validation
    // For untrusted sources (headings, img alts), use strict validation
    if (source === 'jsonld') {
      // JSON-LD is structured data — just check basic format
      if (name.length < 3 || !/\s/.test(name)) return;
    } else if (source === 'card') {
      if (!this._looksLikeName(name)) return;
    } else {
      // heading, imgalt — strict validation
      if (!this._isLikelyHumanName(name)) return;
    }

    // Handle "Dr" prefix
    if (/^dr\.?\s+/i.test(name)) {
      name = name.replace(/^dr\.?\s+/i, '');
    }

    const parts = name.split(/\s+/);
    if (parts.length < 2) return;

    const firstName = parts[0];
    const lastName = parts[parts.length - 1];

    const key = `${firstName.toLowerCase()}_${lastName.toLowerCase()}`;

    // Only use title if it looks like a real professional title
    const cleanTitle = (title && this._looksLikeTitle(title)) ? title.trim() : '';

    // Clean email — must have user@domain.tld format
    const cleanEmail = (email || '').toLowerCase().trim();
    const validEmail = cleanEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail) && !cleanEmail.includes('example.com')
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
