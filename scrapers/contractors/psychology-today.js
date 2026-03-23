/**
 * Psychology Today — Therapist Directory Scraper
 *
 * Source: https://www.psychologytoday.com/us/therapists/profile-listings
 * Records: 100,000+ US therapist profiles
 * Method: Puppeteer (stealth) + Cheerio — Cloudflare-protected, needs JS rendering
 *
 * Strategy:
 *   1. Profile listings at /us/therapists/profile-listings/{state}/{letter}
 *   2. For each state, iterate letters A-Z
 *   3. Paginate within each letter (20 results per page)
 *   4. Extract: name, credentials, city, state, zip, phone, profile URL
 *   5. Uses puppeteer-extra with stealth plugin to bypass Cloudflare
 *
 * Profile URL: /us/therapists/{name-slug}-{city}-{state}/{id}
 * Listing data: name, credentials, location, phone, brief statement, profile link
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.psychologytoday.com';
const PER_PAGE = 20;

// State name slugs for Psychology Today URLs
const STATE_SLUGS = {
  'new-york': 'NY', 'california': 'CA', 'texas': 'TX', 'florida': 'FL',
  'illinois': 'IL', 'pennsylvania': 'PA', 'ohio': 'OH', 'georgia': 'GA',
  'north-carolina': 'NC', 'michigan': 'MI', 'new-jersey': 'NJ',
  'virginia': 'VA', 'washington': 'WA', 'arizona': 'AZ', 'massachusetts': 'MA',
  'tennessee': 'TN', 'indiana': 'IN', 'missouri': 'MO', 'maryland': 'MD',
  'wisconsin': 'WI', 'colorado': 'CO', 'minnesota': 'MN', 'south-carolina': 'SC',
  'alabama': 'AL', 'louisiana': 'LA', 'kentucky': 'KY', 'oregon': 'OR',
  'oklahoma': 'OK', 'connecticut': 'CT', 'utah': 'UT', 'iowa': 'IA',
  'nevada': 'NV', 'arkansas': 'AR', 'mississippi': 'MS', 'kansas': 'KS',
  'new-mexico': 'NM', 'nebraska': 'NE', 'idaho': 'ID', 'west-virginia': 'WV',
  'hawaii': 'HI', 'new-hampshire': 'NH', 'maine': 'ME', 'montana': 'MT',
  'rhode-island': 'RI', 'delaware': 'DE', 'south-dakota': 'SD',
  'north-dakota': 'ND', 'alaska': 'AK', 'vermont': 'VT', 'wyoming': 'WY',
  'district-of-columbia': 'DC',
};

const PRACTICE_AREA_CODES = {
  'all': '',
  'therapist': '',
  'therapy': '',
  'counseling': '',
  'anxiety': 'anxiety',
  'depression': 'depression',
  'trauma': 'trauma',
  'ptsd': 'ptsd',
  'couples': 'couples-counseling',
  'marriage': 'marriage-counseling',
  'addiction': 'addiction',
  'eating disorders': 'eating-disorders',
  'ocd': 'ocd',
  'adhd': 'adhd',
  'grief': 'grief',
};

// Default: top 10 US states by population
const DEFAULT_STATES = [
  'new-york', 'california', 'texas', 'florida', 'illinois',
  'pennsylvania', 'ohio', 'georgia', 'north-carolina', 'michigan',
];

const LETTERS = 'abcdefghijklmnopqrstuvwxyz'.split('');

class PsychologyTodayScraper extends BaseScraper {
  constructor() {
    super({
      name: 'psychology_today',
      stateCode: 'PSYCH-TODAY',
      baseUrl: BASE_URL,
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_STATES,
    });
    this.browser = null;
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('psychology-today: not used'); }
  parseResultsPage() { throw new Error('psychology-today: not used'); }
  extractResultCount() { throw new Error('psychology-today: not used'); }

  transformResult(lead, practiceArea) {
    lead.source = 'psychology_today';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Launch Puppeteer with stealth plugin for Cloudflare bypass.
   */
  async _initBrowser() {
    if (this.browser) return;
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

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

    // Railway: use system Chromium
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this.browser = await puppeteer.launch(launchOptions);
    log.info('[PSYCH-TODAY] Browser launched with stealth plugin');
  }

  /**
   * Close the browser when done.
   */
  async _closeBrowser() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
  }

  /**
   * Fetch a page using Puppeteer (bypasses Cloudflare).
   * Returns HTML string or null on failure.
   */
  async _fetchPage(url) {
    const page = await this.browser.newPage();
    try {
      await page.setViewport({ width: 1280, height: 800 });
      await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

      const response = await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: 30000,
      });

      if (!response) return null;

      const status = response.status();
      if (status === 404) return null;
      if (status !== 200) {
        log.warn(`[PSYCH-TODAY] HTTP ${status} for ${url}`);
        return null;
      }

      // Wait a moment for any dynamic content
      await new Promise(r => setTimeout(r, 1000));

      const html = await page.content();

      // Check for Cloudflare challenge
      if (html.includes('challenge-platform') || html.includes('cf-browser-verification')) {
        // Wait longer for Cloudflare to resolve
        log.info('[PSYCH-TODAY] Cloudflare challenge — waiting...');
        await new Promise(r => setTimeout(r, 5000));
        const html2 = await page.content();
        if (html2.includes('challenge-platform')) {
          log.warn('[PSYCH-TODAY] Cloudflare challenge not resolved');
          return null;
        }
        return html2;
      }

      return html;
    } catch (err) {
      log.warn(`[PSYCH-TODAY] Page fetch error: ${err.message}`);
      return null;
    } finally {
      await page.close();
    }
  }

  /**
   * Parse therapist listings from a profile-listings page.
   * HTML structure: .results-row contains:
   *   .profile-title (name link)
   *   .profile-subtitle-credentials (credential text)
   *   .profile-location .address (city link + ", ST ZIP" text)
   *   .results-row-phone (phone text) or a[href^="tel:"] (phone link)
   *   button.results-row-cta-email (contact form, NOT real email)
   */
  _parseListings($) {
    const results = [];
    const seen = new Set();

    $('.results-row').each((_, row) => {
      const $row = $(row);

      // --- Profile URL + ID ---
      const $titleLink = $row.find('.profile-title').first();
      if (!$titleLink.length) return;

      const href = $titleLink.attr('href') || '';
      if (!href || seen.has(href)) return;
      seen.add(href);

      let profileUrl = href;
      if (profileUrl.startsWith('/')) profileUrl = BASE_URL + profileUrl;

      const idMatch = href.match(/\/(\d+)\/?$/);
      const profileId = idMatch ? idMatch[1] : '';

      // --- Name ---
      const nameText = $titleLink.text().trim();
      if (!nameText) return;
      const { firstName, lastName } = this.splitName(nameText);

      // --- Credentials ---
      const credential = $row.find('.profile-subtitle-credentials').first().text().trim();

      // --- Location (city, state, zip) ---
      let city = '';
      let state = '';
      let zip = '';

      // City from .address link
      const $cityLink = $row.find('.profile-location .address a');
      if ($cityLink.length) {
        city = $cityLink.text().trim();
      }

      // State + zip from .address text content (", NY 10023")
      const addressText = $row.find('.profile-location .address').text().replace(/\s+/g, ' ').trim();
      const locMatch = addressText.match(/,\s*([A-Z]{2})\s+(\d{5})/);
      if (locMatch) {
        state = locMatch[1];
        zip = locMatch[2];
      } else {
        // Try to get state from URL: /therapists/...-ny/12345
        const slugMatch = href.match(/-([a-z]{2})\/\d+\/?$/);
        if (slugMatch) state = slugMatch[1].toUpperCase();
      }

      // --- Phone ---
      let phone = '';
      // Try .results-row-phone text first
      const $phoneSpan = $row.find('.results-row-phone');
      if ($phoneSpan.length) {
        phone = $phoneSpan.first().text().trim();
      }
      // Fall back to tel: link
      if (!phone) {
        const $tel = $row.find('a[href^="tel:"]');
        if ($tel.length) {
          const telHref = $tel.first().attr('href') || '';
          // Format: tel:+16464900139 → (646) 490-0139
          const digits = telHref.replace(/[^\d]/g, '');
          if (digits.length >= 10) {
            const d = digits.length === 11 ? digits.substring(1) : digits;
            phone = `(${d.substring(0, 3)}) ${d.substring(3, 6)}-${d.substring(6, 10)}`;
            // Handle extensions
            if (telHref.includes(',')) {
              const extMatch = telHref.match(/,+(\d+)/);
              if (extMatch) phone += ` x${extMatch[1]}`;
            }
          }
        }
      }

      if (!firstName && !lastName) return;

      results.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        credential: credential,
        city: city,
        state: state,
        zip: zip,
        country: 'US',
        phone: phone,
        email: '', // PT doesn't expose email on listings
        website: '',
        bar_number: profileId,
        bar_status: 'Active',
        profile_url: profileUrl,
        practice_area: 'Therapy',
        trade_category: 'Therapy',
        license_type: 'Therapist',
      });
    });

    return results;
  }

  /**
   * Get max page number from pagination links.
   */
  _getMaxPage($) {
    let maxPage = 0;
    // Look for page links in pagination
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      const pageMatch = href.match(/[?&]page=(\d+)/);
      if (pageMatch) {
        const p = parseInt(pageMatch[1], 10);
        if (p > maxPage) maxPage = p;
      }
    });
    // Also check for numbered links that look like pagination
    $('a').each((_, el) => {
      const text = $(el).text().trim();
      if (/^\d+$/.test(text)) {
        const p = parseInt(text, 10);
        // Only count if it's reasonable (< 500) and the link is in a nav/pagination area
        if (p > maxPage && p < 500) {
          const href = $(el).attr('href') || '';
          if (href.includes('profile-listings')) maxPage = p;
        }
      }
    });
    return maxPage;
  }

  /**
   * Main search generator.
   * Uses Puppeteer with stealth to bypass Cloudflare.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter({ minDelay: 2000, maxDelay: 4000 });

    // Initialize browser
    await this._initBrowser();

    let states;
    if (options.city) {
      const slug = options.city.toLowerCase().replace(/\s+/g, '-');
      states = [slug];
    } else {
      states = [...DEFAULT_STATES];
    }
    if (options.maxCities) {
      states = states.slice(0, options.maxCities);
    }

    const maxPagesPerLetter = options.maxPages || 100;

    log.scrape(`[PSYCH-TODAY] Starting scrape: ${states.length} states, ${LETTERS.length} letters each`);

    const seenIds = new Set();
    let totalYielded = 0;
    let totalWithPhone = 0;
    let totalWithEmail = 0;
    let challengeCount = 0;

    try {
      for (let si = 0; si < states.length; si++) {
        const stateSlug = states[si];
        const stateCode = STATE_SLUGS[stateSlug] || stateSlug.toUpperCase();

        yield { _cityProgress: { current: si + 1, total: states.length } };
        log.scrape(`[PSYCH-TODAY] State ${si + 1}/${states.length}: ${stateSlug} (${stateCode})`);

        let stateYielded = 0;

        for (const letter of LETTERS) {
          let page = 0;
          let consecutiveEmpty = 0;

          while (page < maxPagesPerLetter) {
            let url = `${BASE_URL}/us/therapists/profile-listings/${stateSlug}/${letter}`;
            if (page > 0) url += `?page=${page}`;

            await rateLimiter.wait();
            const html = await this._fetchPage(url);

            if (!html) {
              challengeCount++;
              if (challengeCount >= 5) {
                log.error('[PSYCH-TODAY] Too many failures — aborting');
                yield { _captcha: true, city: stateSlug, reason: 'Cloudflare persistent block' };
                return;
              }
              break;
            }

            challengeCount = 0; // Reset on success
            const $ = cheerio.load(html);
            const listings = this._parseListings($);

            if (listings.length === 0) {
              consecutiveEmpty++;
              if (consecutiveEmpty >= 2) break;
              page++;
              continue;
            }

            consecutiveEmpty = 0;

            for (const lead of listings) {
              const id = lead.bar_number || lead.profile_url;
              if (seenIds.has(id)) continue;
              seenIds.add(id);

              if (!lead.state) lead.state = stateCode;
              if (lead.phone) totalWithPhone++;
              if (lead.email) totalWithEmail++;

              yield this.transformResult(lead, practiceArea);
              totalYielded++;
              stateYielded++;
            }

            const maxPage = this._getMaxPage($);
            if (page >= maxPage) break;
            page++;
          }
        }

        log.success(`[PSYCH-TODAY] ${stateSlug}: ${stateYielded} therapists (${totalYielded} total)`);
      }
    } finally {
      await this._closeBrowser();
    }

    log.success(`[PSYCH-TODAY] Finished — ${totalYielded} therapists (${totalWithPhone} with phone, ${totalWithEmail} with email)`);
  }
}

module.exports = new PsychologyTodayScraper();
