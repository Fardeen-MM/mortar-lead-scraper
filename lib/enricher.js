/**
 * Enricher — enrich leads with website, title, LinkedIn, bio, education, etc.
 *
 * Tier 1: Derive website from email domain (free, instant)
 * Tier 2: Visit firm website with Puppeteer (free, ~5s/lead)
 *         - Find bio page matching attorney last name
 *         - Extract LinkedIn, title, education via regex
 * Tier 3: LLM fallback for missing title (~$0.001/lead, optional)
 */

const puppeteer = require('puppeteer');
const { RateLimiter, sleep } = require('./rate-limiter');

// Freemail domains — skip website derivation for these
const FREEMAIL_DOMAINS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'comcast.net', 'me.com', 'mac.com', 'live.com',
  'msn.com', 'att.net', 'sbcglobal.net', 'verizon.net', 'cox.net',
  'bellsouth.net', 'charter.net', 'earthlink.net', 'optonline.net',
  'protonmail.com', 'proton.me', 'zoho.com', 'ymail.com',
  'rocketmail.com', 'mail.com', 'gmx.com', 'fastmail.com',
]);

// Common attorney/team page paths to check
const TEAM_PATHS = [
  '/attorneys', '/team', '/our-team', '/professionals', '/people',
  '/lawyers', '/our-attorneys', '/about', '/about-us', '/our-firm',
  '/staff', '/attorney', '/our-lawyers', '/members',
];

// Known attorney titles
const TITLE_PATTERNS = [
  // "Name, Title" or "Title | Name" patterns
  /(?:partner|managing partner|senior partner|founding partner|named partner|equity partner)/i,
  /(?:associate|senior associate|junior associate)/i,
  /(?:of counsel|special counsel|general counsel)/i,
  /(?:shareholder|principal|director|member)/i,
  /(?:attorney|lawyer|legal counsel|counsel)/i,
  /(?:founder|co-founder|owner)/i,
];

const TITLE_KEYWORDS = [
  'Managing Partner', 'Senior Partner', 'Founding Partner', 'Named Partner', 'Equity Partner', 'Partner',
  'Senior Associate', 'Junior Associate', 'Associate',
  'Of Counsel', 'Special Counsel', 'General Counsel',
  'Shareholder', 'Principal', 'Director', 'Member',
  'Founder', 'Co-Founder', 'Owner',
  'Attorney', 'Lawyer', 'Counsel',
];

// Education patterns
const EDUCATION_PATTERNS = [
  /J\.?D\.?\s*,?\s*[A-Z][a-z]/,
  /Juris\s+Doctor/i,
  /(?:University|College|School)\s+of\s+[A-Z][a-zA-Z\s]+(?:School of Law|Law School|College of Law)?/,
  /[A-Z][a-zA-Z]+\s+(?:University|College|Law School|School of Law)/,
  /(?:B\.?A\.?|B\.?S\.?|M\.?A\.?|M\.?S\.?|LL\.?M\.?|LL\.?B\.?|Ph\.?D\.?)\s*,?\s*[A-Z]/,
];

// Language patterns
const LANGUAGE_LIST = [
  'Spanish', 'Portuguese', 'French', 'German', 'Italian', 'Mandarin',
  'Cantonese', 'Chinese', 'Japanese', 'Korean', 'Arabic', 'Russian',
  'Hindi', 'Urdu', 'Tagalog', 'Vietnamese', 'Polish', 'Haitian Creole',
  'Creole', 'Hebrew', 'Turkish', 'Farsi', 'Persian', 'Greek',
];

class Enricher {
  /**
   * @param {object} options
   * @param {boolean} [options.deriveWebsite=true]  - Derive website from email domain
   * @param {boolean} [options.scrapeWebsite=true]  - Visit firm websites with Puppeteer
   * @param {boolean} [options.findLinkedIn=true]    - Extract LinkedIn URLs from pages
   * @param {boolean} [options.extractWithAI=false]  - Use LLM fallback for missing data
   * @param {string}  [options.proxy]                - Proxy URL for Puppeteer
   * @param {EventEmitter} [options.emitter]         - EventEmitter for progress events
   */
  constructor(options = {}) {
    this.deriveWebsite = options.deriveWebsite !== false;
    this.scrapeWebsite = options.scrapeWebsite !== false;
    this.findLinkedIn = options.findLinkedIn !== false;
    this.extractWithAI = options.extractWithAI === true;
    this.proxy = options.proxy || null;
    this.emitter = options.emitter || null;

    this.browser = null;
    this.anthropic = null;
    this.rateLimiter = new RateLimiter({ minDelay: 2000, maxDelay: 5000 });

    // Domain cache: { domain: { homepage, teamPageUrl, teamPageText } }
    this.domainCache = new Map();

    this.stats = {
      total: 0,
      websitesDerived: 0,
      websitesVisited: 0,
      titlesFound: 0,
      linkedInFound: 0,
      educationFound: 0,
      languagesFound: 0,
      llmCalls: 0,
      errors: 0,
    };
  }

  async init() {
    if (this.scrapeWebsite) {
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

    if (this.extractWithAI) {
      const apiKey = process.env.ANTHROPIC_API_KEY;
      if (apiKey) {
        try {
          const Anthropic = require('@anthropic-ai/sdk');
          this.anthropic = new Anthropic({ apiKey });
        } catch (err) {
          console.error('[enricher] Failed to load Anthropic SDK:', err.message);
          this.extractWithAI = false;
        }
      } else {
        this.extractWithAI = false;
      }
    }
  }

  async close() {
    if (this.browser) {
      await this.browser.close().catch((err) => {
        console.error('[enricher] Browser close error:', err.message);
      });
    }
  }

  /**
   * Enrich all leads. Emits 'enrichment-progress' events.
   * @param {Array} leads - Array of lead objects (mutated in place)
   * @param {Function} [cancelCheck] - Optional () => boolean callback; if it returns true, stop early
   */
  async enrichAll(leads, cancelCheck) {
    this.stats.total = leads.length;

    for (let i = 0; i < leads.length; i++) {
      // Check for cancellation before each lead
      if (cancelCheck && cancelCheck()) {
        break;
      }

      try {
        await this.enrichLead(leads[i]);
      } catch (err) {
        this.stats.errors++;
        console.error(`[enricher] Failed to enrich lead ${leads[i].first_name} ${leads[i].last_name}:`, err.message);
      }

      // Emit progress
      if (this.emitter) {
        this.emitter.emit('enrichment-progress', {
          current: i + 1,
          total: leads.length,
          leadName: `${leads[i].first_name} ${leads[i].last_name}`,
        });
      }

      // Log every 10
      if ((i + 1) % 10 === 0 && this.emitter) {
        this.emitter.emit('log', {
          level: 'enrich',
          message: `Enriched ${i + 1}/${leads.length} leads`,
        });
      }
    }

    return this.stats;
  }

  /**
   * Enrich a single lead in place.
   */
  async enrichLead(lead) {
    // Tier 1: Derive website from email
    if (this.deriveWebsite && !lead.website && lead.email) {
      const derived = this.deriveWebsiteFromEmail(lead.email);
      if (derived) {
        lead.website = derived;
        this.stats.websitesDerived++;
      }
    }

    // Tier 2: Scrape firm website
    if (this.scrapeWebsite && this.browser && lead.website) {
      try {
        const scraped = await this.scrapeWebsiteContent(lead.website, lead);
        if (scraped) {
          // Extract structured data via regex
          const extracted = this.extractStructuredData(scraped.pageText, lead);

          if (extracted.title && !lead.title) {
            lead.title = extracted.title;
            this.stats.titlesFound++;
          }
          if (extracted.education && !lead.education) {
            lead.education = extracted.education;
            this.stats.educationFound++;
          }
          if (extracted.languages && !lead.languages) {
            lead.languages = extracted.languages;
            this.stats.languagesFound++;
          }
          if (extracted.practiceSpecialties && !lead.practice_specialties) {
            lead.practice_specialties = extracted.practiceSpecialties;
          }
          if (extracted.bio && !lead.bio) {
            lead.bio = extracted.bio;
          }

          // LinkedIn from page links
          if (this.findLinkedIn && scraped.linkedInUrls.length > 0 && !lead.linkedin_url) {
            lead.linkedin_url = scraped.linkedInUrls[0];
            this.stats.linkedInFound++;
          }
        }
      } catch (err) {
        this.stats.errors++;
        console.error(`[enricher] Website scrape failed for ${lead.website}:`, err.message);
      }
    }

    // Tier 3: LLM fallback if title still empty
    if (this.extractWithAI && this.anthropic && !lead.title && lead.website) {
      try {
        const llmResult = await this.llmFallback(lead);
        if (llmResult.title) {
          lead.title = llmResult.title;
          this.stats.titlesFound++;
        }
        if (llmResult.bio && !lead.bio) {
          lead.bio = llmResult.bio;
        }
        this.stats.llmCalls++;
      } catch (err) {
        console.error(`[enricher] LLM fallback failed for ${lead.first_name} ${lead.last_name}:`, err.message);
      }
    }
  }

  /**
   * Tier 1: Derive website URL from email domain.
   * Skips freemail providers.
   */
  deriveWebsiteFromEmail(email) {
    if (!email || !email.includes('@')) return null;
    const domain = email.split('@')[1].toLowerCase().trim();
    if (!domain || FREEMAIL_DOMAINS.has(domain)) return null;
    return `https://${domain}`;
  }

  /**
   * Tier 2: Visit the firm website and find bio page for this attorney.
   * Uses domain cache so multiple attorneys at the same firm don't re-fetch.
   *
   * @returns {{ pageText: string, linkedInUrls: string[] }} or null
   */
  async scrapeWebsiteContent(websiteUrl, lead) {
    let url = websiteUrl.trim();
    if (!url.startsWith('http')) url = 'https://' + url;

    let domain;
    try {
      domain = new URL(url).hostname.replace(/^www\./, '');
    } catch (err) {
      console.error(`[enricher] Invalid URL "${url}":`, err.message);
      return null;
    }

    this.stats.websitesVisited++;
    const lastName = (lead.last_name || '').toLowerCase();
    const firstName = (lead.first_name || '').toLowerCase();

    // Check domain cache
    let cached = this.domainCache.get(domain);

    const page = await this.browser.newPage();
    const ua = this.rateLimiter.getUserAgent();
    await page.setUserAgent(ua);
    await page.setDefaultTimeout(10000);

    const linkedInUrls = [];
    let pageText = '';

    try {
      // If we don't have this domain cached, fetch homepage + find team page
      if (!cached) {
        await this.rateLimiter.wait();
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });

        const homepageText = await page.evaluate(() => document.body?.innerText || '');
        const homepageLinks = await this.extractAllLinks(page, domain);

        // Find LinkedIn links on homepage
        const homeLinkedIn = this.findLinkedInLinks(homepageLinks);
        linkedInUrls.push(...homeLinkedIn);

        // Find team/attorney page
        let teamPageUrl = null;
        for (const link of homepageLinks) {
          const path = link.path.toLowerCase();
          if (TEAM_PATHS.some(tp => path.startsWith(tp) || path === tp + '/')) {
            teamPageUrl = link.href;
            break;
          }
        }

        cached = {
          homepageText,
          teamPageUrl,
          teamPageLinks: homepageLinks,
        };
        // LRU eviction: if cache is too large, delete oldest entry
        if (this.domainCache.size >= 1000) {
          const oldest = this.domainCache.keys().next().value;
          this.domainCache.delete(oldest);
        }
        this.domainCache.set(domain, cached);
      }

      // Visit team page if found (look for bio link matching this attorney)
      let bioPageUrl = null;
      if (cached.teamPageUrl) {
        await this.rateLimiter.wait();
        await page.goto(cached.teamPageUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });

        const teamLinks = await this.extractAllLinks(page, domain);
        const teamLinkedIn = this.findLinkedInLinks(teamLinks);
        linkedInUrls.push(...teamLinkedIn);

        // Find bio page link for this attorney by last name
        for (const link of teamLinks) {
          const hrefLower = link.href.toLowerCase();
          const textLower = link.text.toLowerCase();
          if (
            (lastName && (hrefLower.includes(lastName) || textLower.includes(lastName))) &&
            (hrefLower.includes('/attorney') || hrefLower.includes('/team') ||
             hrefLower.includes('/people') || hrefLower.includes('/professional') ||
             hrefLower.includes('/lawyer') || hrefLower.includes('/bio') ||
             hrefLower.includes('/staff') || hrefLower.includes('/member'))
          ) {
            bioPageUrl = link.href;
            break;
          }
        }

        // Also try matching on the team page text itself if no bio link
        if (!bioPageUrl) {
          const teamText = await page.evaluate(() => document.body?.innerText || '');
          pageText = teamText;
        }
      }

      // Visit bio page if found
      if (bioPageUrl) {
        await this.rateLimiter.wait();
        await page.goto(bioPageUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });

        pageText = await page.evaluate(() => document.body?.innerText || '');
        const bioLinks = await this.extractAllLinks(page, domain);
        const bioLinkedIn = this.findLinkedInLinks(bioLinks);
        linkedInUrls.push(...bioLinkedIn);
      }

      // If still no page text, use homepage text
      if (!pageText) {
        pageText = cached.homepageText || '';
      }

    } catch (err) {
      console.error(`[enricher] Site unreachable ${domain}:`, err.message);
    } finally {
      await page.close().catch(() => {});
    }

    // Deduplicate LinkedIn URLs
    const uniqueLinkedIn = [...new Set(linkedInUrls)];

    // If we have LinkedIn URLs, try to match by attorney name
    let matchedLinkedIn = uniqueLinkedIn;
    if (uniqueLinkedIn.length > 1 && lastName) {
      const nameMatched = uniqueLinkedIn.filter(u =>
        u.toLowerCase().includes(lastName)
      );
      if (nameMatched.length > 0) matchedLinkedIn = nameMatched;
    }

    return { pageText, linkedInUrls: matchedLinkedIn };
  }

  /**
   * Extract all links from a page.
   */
  async extractAllLinks(page, domain) {
    try {
      return await page.evaluate((d) => {
        return Array.from(document.querySelectorAll('a[href]')).map(a => {
          let path = '';
          try { path = new URL(a.href).pathname; } catch {}
          return {
            href: a.href,
            text: a.textContent?.trim() || '',
            path,
          };
        });
      }, domain);
    } catch (err) {
      console.error(`[enricher] extractAllLinks failed for ${domain}:`, err.message);
      return [];
    }
  }

  /**
   * Find LinkedIn profile URLs from link list.
   */
  findLinkedInLinks(links) {
    return links
      .filter(l => l.href && l.href.includes('linkedin.com/in/'))
      .map(l => {
        // Clean the URL
        try {
          const u = new URL(l.href);
          return `https://www.linkedin.com${u.pathname}`.replace(/\/+$/, '');
        } catch (err) {
          console.error(`[enricher] Bad LinkedIn URL "${l.href}":`, err.message);
          return l.href;
        }
      });
  }

  /**
   * Tier 2: Extract structured data from page text using regex/patterns.
   */
  extractStructuredData(pageText, lead) {
    const result = {
      title: null,
      education: null,
      languages: null,
      practiceSpecialties: null,
      bio: null,
    };

    if (!pageText) return result;

    const fullName = `${lead.first_name} ${lead.last_name}`;
    const lastName = lead.last_name || '';

    // --- Title extraction ---
    result.title = this.extractTitle(pageText, fullName, lastName);

    // --- Education extraction ---
    result.education = this.extractEducation(pageText);

    // --- Languages extraction ---
    result.languages = this.extractLanguages(pageText);

    // --- Practice specialties ---
    result.practiceSpecialties = this.extractPracticeSpecialties(pageText);

    // --- Bio extraction ---
    result.bio = this.extractBio(pageText, fullName, lastName);

    return result;
  }

  /**
   * Extract title from page text.
   */
  extractTitle(pageText, fullName, lastName) {
    const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);

    // Strategy 1: "Name, Title" or "Name | Title" pattern
    for (const line of lines) {
      if (!line.includes(lastName)) continue;

      // "John Smith, Partner" or "John Smith | Partner"
      const commaMatch = line.match(new RegExp(
        `${escapeRegex(lastName)}[^,|]*[,|]\\s*(.+)`, 'i'
      ));
      if (commaMatch) {
        const candidate = commaMatch[1].trim();
        if (this.isValidTitle(candidate)) {
          return this.cleanTitle(candidate);
        }
      }
    }

    // Strategy 2: Title on adjacent line after name
    for (let i = 0; i < lines.length; i++) {
      if (!lines[i].includes(lastName)) continue;

      // Check next 1-2 lines for a title keyword
      for (let j = 1; j <= 2 && i + j < lines.length; j++) {
        const nextLine = lines[i + j];
        if (this.isValidTitle(nextLine)) {
          return this.cleanTitle(nextLine);
        }
      }
    }

    // Strategy 3: Standalone title near the top of the page (bio page)
    for (let i = 0; i < Math.min(lines.length, 15); i++) {
      const line = lines[i];
      if (line.length > 2 && line.length < 50 && this.isValidTitle(line)) {
        return this.cleanTitle(line);
      }
    }

    return null;
  }

  /**
   * Check if a string looks like a valid attorney title.
   */
  isValidTitle(text) {
    if (!text || text.length > 80) return false;
    const lower = text.toLowerCase().trim();
    return TITLE_KEYWORDS.some(t => lower.includes(t.toLowerCase()));
  }

  /**
   * Clean up an extracted title.
   */
  cleanTitle(text) {
    let title = text.trim();
    // Remove trailing punctuation
    title = title.replace(/[,;|]+$/, '').trim();
    // If it contains the title keyword, extract just the relevant part
    for (const keyword of TITLE_KEYWORDS) {
      if (title.toLowerCase().includes(keyword.toLowerCase())) {
        // If the title is short enough, use as-is
        if (title.length <= 50) return title;
        // Otherwise extract just the keyword
        return keyword;
      }
    }
    return title;
  }

  /**
   * Extract education from page text.
   */
  extractEducation(pageText) {
    const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);
    const eduEntries = [];

    for (const line of lines) {
      // Skip very long lines (unlikely to be education)
      if (line.length > 200) continue;

      for (const pattern of EDUCATION_PATTERNS) {
        const match = line.match(pattern);
        if (match) {
          // Get the full line but truncate if needed
          let edu = line.length <= 120 ? line : match[0];
          if (!eduEntries.includes(edu)) {
            eduEntries.push(edu);
          }
          break;
        }
      }

      if (eduEntries.length >= 3) break;
    }

    return eduEntries.length > 0 ? eduEntries.join('; ') : null;
  }

  /**
   * Extract languages from page text.
   */
  extractLanguages(pageText) {
    // Look for "Languages:" or "Fluent in" section
    const langSectionMatch = pageText.match(
      /(?:Languages?|Fluent\s+in|Speaks?)\s*:?\s*([^\n]+)/i
    );
    if (langSectionMatch) {
      const section = langSectionMatch[1].trim();
      // Filter to known languages
      const found = LANGUAGE_LIST.filter(lang =>
        section.toLowerCase().includes(lang.toLowerCase())
      );
      if (found.length > 0) return found.join(', ');
      // If it looks like a language list, return it
      if (section.length < 100 && section.includes(',')) return section;
    }

    // Fallback: look for language names mentioned near language-related context
    const contextMatch = pageText.match(
      /(?:bilingual|multilingual|fluent|speaks?|language)[^\n]{0,100}/i
    );
    if (contextMatch) {
      const found = LANGUAGE_LIST.filter(lang =>
        contextMatch[0].toLowerCase().includes(lang.toLowerCase())
      );
      if (found.length > 0) return found.join(', ');
    }

    return null;
  }

  /**
   * Extract practice specialties from page text.
   */
  extractPracticeSpecialties(pageText) {
    // Look for practice area sections
    const sectionMatch = pageText.match(
      /(?:Practice\s+Areas?|Areas?\s+of\s+(?:Practice|Focus|Expertise)|Specialties|Specializations?)\s*:?\s*\n([\s\S]{10,500}?)(?:\n\n|\n[A-Z])/i
    );
    if (sectionMatch) {
      const section = sectionMatch[1];
      // Extract list items (lines starting with bullet-like chars or short lines)
      const items = section.split('\n')
        .map(l => l.replace(/^[\s•·\-–—*]+/, '').trim())
        .filter(l => l.length > 2 && l.length < 80);
      if (items.length > 0) {
        return items.slice(0, 8).join(', ');
      }
    }

    return null;
  }

  /**
   * Extract a short bio from page text.
   */
  extractBio(pageText, fullName, lastName) {
    const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);

    // Find the first paragraph that mentions the attorney name and is bio-like
    for (const line of lines) {
      if (line.length < 40 || line.length > 500) continue;
      if (!line.includes(lastName)) continue;

      // Bio-like: contains "practice", "experience", "represent", "focus", "law", "attorney", "counsel"
      const lower = line.toLowerCase();
      if (lower.includes('practice') || lower.includes('experience') ||
          lower.includes('represent') || lower.includes('focus') ||
          lower.includes('attorney') || lower.includes('counsel') ||
          lower.includes('law') || lower.includes('specializ')) {
        // Truncate to ~200 chars at a sentence boundary
        if (line.length <= 200) return line;
        const truncated = line.substring(0, 200);
        const lastPeriod = truncated.lastIndexOf('.');
        if (lastPeriod > 100) return truncated.substring(0, lastPeriod + 1);
        return truncated + '...';
      }
    }

    return null;
  }

  /**
   * Tier 3: LLM fallback — only called when regex failed to find title.
   */
  async llmFallback(lead) {
    if (!this.anthropic) return {};

    // Get cached page text
    let domain;
    try {
      const url = lead.website.startsWith('http') ? lead.website : 'https://' + lead.website;
      domain = new URL(url).hostname.replace(/^www\./, '');
    } catch (err) {
      console.error(`[enricher] LLM fallback: bad URL "${lead.website}":`, err.message);
      return {};
    }

    const cached = this.domainCache.get(domain);
    const pageText = cached?.homepageText || '';

    if (!pageText || pageText.length < 50) return {};

    // Truncate to ~2000 chars to keep cost low
    const truncatedText = pageText.substring(0, 2000);

    try {
      const response = await this.anthropic.messages.create({
        model: 'claude-3-5-haiku-20241022',
        max_tokens: 150,
        messages: [{
          role: 'user',
          content: `Extract the title/position and a one-sentence bio for attorney "${lead.first_name} ${lead.last_name}" from this law firm website text. If you can't find them, say "unknown".

Website text:
${truncatedText}

Respond in exactly this format:
Title: [title or "unknown"]
Bio: [one sentence or "unknown"]`,
        }],
      });

      const text = response.content[0]?.text || '';
      const titleMatch = text.match(/Title:\s*(.+)/i);
      const bioMatch = text.match(/Bio:\s*(.+)/i);

      const result = {};
      if (titleMatch && !titleMatch[1].toLowerCase().includes('unknown')) {
        result.title = titleMatch[1].trim();
      }
      if (bioMatch && !bioMatch[1].toLowerCase().includes('unknown')) {
        result.bio = bioMatch[1].trim();
      }

      return result;
    } catch (err) {
      console.error(`[enricher] LLM API call failed for ${lead.first_name} ${lead.last_name}:`, err.message);
      return {};
    }
  }

  getStats() {
    return { ...this.stats };
  }
}

/**
 * Escape regex special characters in a string.
 */
function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

module.exports = Enricher;
