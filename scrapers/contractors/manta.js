/**
 * Manta.com — US Business Directory Scraper (1.7M+ Construction Businesses)
 *
 * Source: https://www.manta.com
 * Method: Puppeteer (Manta uses Cloudflare WAF — plain HTTP gets 403)
 *         + Cheerio for HTML/JSON-LD parsing once page content is loaded
 *
 * Coverage: US-wide — 1.7M+ construction businesses with EMAIL as standard field
 *
 * URL Patterns:
 *   Search: https://www.manta.com/search?search={category}+contractors&search_location={City},+{State}&pg={N}
 *   Profile: https://www.manta.com/c/{id}/{slug}
 *
 * Data available from profile pages:
 *   - Business name, phone, EMAIL, website, full address
 *   - Contact person name, SIC/NAICS codes, employee count, revenue
 *   - Category, description, hours, founding date
 *   - JSON-LD LocalBusiness schema (primary extraction method)
 *   - Microdata / HTML fallback extraction
 *
 * Search pagination: &pg=N (1-indexed)
 * Results per page: ~30 businesses
 *
 * Strategy:
 *   1. Puppeteer loads search pages, extract profile links (/c/...)
 *   2. Puppeteer loads each profile page
 *   3. Cheerio parses HTML for JSON-LD + microdata + text patterns
 *   4. Yield leads with email, phone, website, contact person
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.manta.com';
const RESULTS_PER_PAGE = 30;

/**
 * Category slug mapping: practice area -> Manta search term.
 * These map to the &search= parameter in the search URL.
 */
const CATEGORY_SLUGS = {
  'general contractor': 'general-contractors',
  'roofing': 'roofing-contractors',
  'plumbing': 'plumbing-contractors',
  'painting': 'painting-contractors',
  'hvac': 'heating-contractors',
  'electrical': 'electrical-contractors',
  'landscaping': 'landscaping-services',
};

/**
 * Default cities — major US metros.
 * Each entry is "City, ST" format.
 */
const DEFAULT_CITIES = [
  'Miami, FL',
  'Houston, TX',
  'Los Angeles, CA',
  'Phoenix, AZ',
  'Dallas, TX',
  'Atlanta, GA',
  'Chicago, IL',
  'Denver, CO',
  'Seattle, WA',
  'Tampa, FL',
  'Charlotte, NC',
  'Columbus, OH',
  'Philadelphia, PA',
  'San Antonio, TX',
  'Nashville, TN',
];

class MantaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'manta_contractors',
      stateCode: 'MANTA',
      baseUrl: BASE_URL,
      pageSize: RESULTS_PER_PAGE,
      practiceAreaCodes: CATEGORY_SLUGS,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — base class stubs not used
  buildSearchUrl() { throw new Error('manta: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('manta: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('manta: extractResultCount() not used — search() overridden'); }

  /**
   * Override CAPTCHA detection — Manta uses Cloudflare.
   * Don't false-positive on generic "captcha" strings in page scripts.
   */
  detectCaptcha(body) {
    return body.includes('challenge-form') ||
           body.includes('Just a moment') ||
           body.includes('cf-browser-verification') ||
           body.includes('Checking your browser');
  }

  /**
   * Override transformResult to use 'manta_contractors' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'manta_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Build the Manta search URL.
   *
   * @param {string} category - Category slug (e.g., 'roofing-contractors')
   * @param {string} city - City name
   * @param {string} state - 2-letter state code
   * @param {number} page - Page number (1-indexed)
   * @returns {string}
   */
  _buildSearchUrl(category, city, state, page) {
    const search = encodeURIComponent(category.replace(/-/g, ' '));
    const location = encodeURIComponent(`${city}, ${state}`);
    let url = `${BASE_URL}/search?search_source=nav&search=${search}&search_location=${location}`;
    if (page > 1) {
      url += `&pg=${page}`;
    }
    return url;
  }

  /**
   * Parse a "City, ST" string.
   */
  _parseCityInput(cityInput) {
    const parts = cityInput.split(',').map(s => s.trim());
    if (parts.length >= 2) {
      return { city: parts[0], state: parts[1].toUpperCase() };
    }
    return { city: parts[0], state: '' };
  }

  /**
   * Extract profile links from a Manta search results page.
   * Profile links follow the pattern: /c/{id}/{slug}
   *
   * @param {string} html - Raw HTML content
   * @returns {{ profileUrls: string[], totalCount: number }}
   */
  _parseSearchPage(html) {
    const $ = cheerio.load(html);
    const profileUrls = [];
    const seen = new Set();

    // Look for profile links matching /c/{id}/{slug}
    $('a[href*="/c/"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      // Match /c/{alphanum-id}/{slug} pattern — Manta business profile URLs
      const match = href.match(/^(?:https?:\/\/www\.manta\.com)?\/c\/(m[a-z0-9]+)\/([a-z0-9-]+)/i);
      if (!match) return;

      const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;

      // Dedup by the ID portion
      const id = match[1];
      if (seen.has(id)) return;
      seen.add(id);

      profileUrls.push(fullUrl);
    });

    // Try to extract total count
    let totalCount = 0;
    const bodyText = $('body').text();
    // Look for "X results" or "Showing X" or "of X" patterns
    const countMatch = bodyText.match(/(?:of|about|found|showing)\s+([\d,]+)\s+(?:results|businesses|listings)/i);
    if (countMatch) {
      totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);
    }
    // Fallback: look for just "N results"
    if (!totalCount) {
      const fallback = bodyText.match(/([\d,]+)\s+results/i);
      if (fallback) {
        totalCount = parseInt(fallback[1].replace(/,/g, ''), 10);
      }
    }

    return { profileUrls, totalCount };
  }

  /**
   * Parse a Manta business profile page for structured data.
   * Tries multiple extraction strategies:
   *   1. JSON-LD (application/ld+json) — most reliable
   *   2. Meta tags (og:, description)
   *   3. Microdata / HTML element patterns
   *   4. Text regex patterns
   *
   * @param {string} html - Raw HTML content
   * @returns {object} Lead data
   */
  _parseProfilePage(html) {
    const $ = cheerio.load(html);
    const result = {
      first_name: '',
      last_name: '',
      firm_name: '',
      phone: '',
      email: '',
      website: '',
      city: '',
      state: '',
      zip: '',
      address: '',
      bar_number: '', // SIC/NAICS
      bar_status: '', // Category
    };

    // --- 1. JSON-LD extraction (primary) ---
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        const data = JSON.parse(raw);

        // Handle both single object and array formats
        const items = Array.isArray(data) ? data : [data];
        for (const item of items) {
          const type = item['@type'] || '';
          // Match LocalBusiness and subtypes
          if (!type || typeof type !== 'string') continue;
          if (!type.includes('Business') && !type.includes('Contractor') &&
              !type.includes('Service') && !type.includes('Store') &&
              !type.includes('Organization') && !type.includes('Place') &&
              type !== 'Electrician' && type !== 'Plumber' &&
              type !== 'RoofingContractor' && type !== 'GeneralContractor' &&
              type !== 'HVACBusiness' && type !== 'LandscapingBusiness') continue;

          if (item.name) result.firm_name = item.name;
          if (item.telephone) result.phone = item.telephone;
          if (item.email) result.email = item.email.replace(/^mailto:/i, '');

          if (item.url && !item.url.includes('manta.com')) {
            result.website = item.url;
          }
          if (item.sameAs) {
            const sameAs = Array.isArray(item.sameAs) ? item.sameAs : [item.sameAs];
            for (const url of sameAs) {
              if (url && !url.includes('manta.com') && !this.isExcludedDomain(url)) {
                if (!result.website) result.website = url;
                break;
              }
            }
          }

          if (item.address) {
            const addr = item.address;
            if (addr.streetAddress) result.address = addr.streetAddress;
            if (addr.addressLocality) result.city = addr.addressLocality;
            if (addr.addressRegion) result.state = addr.addressRegion;
            if (addr.postalCode) result.zip = addr.postalCode;
          }

          // Contact person from JSON-LD (employee, founder, member)
          const contactPerson = item.employee || item.founder || item.member;
          if (contactPerson) {
            const person = Array.isArray(contactPerson) ? contactPerson[0] : contactPerson;
            if (person && person.name) {
              const parts = this.splitName(person.name);
              result.first_name = parts.firstName;
              result.last_name = parts.lastName;
            }
          }
        }
      } catch (e) {
        // Malformed JSON-LD — skip
      }
    });

    // --- 2. Meta tag extraction (fallback) ---
    if (!result.firm_name) {
      const ogTitle = $('meta[property="og:title"]').attr('content') || '';
      if (ogTitle) {
        // Strip " - Manta.com" suffix
        result.firm_name = ogTitle.replace(/\s*[-|]\s*Manta\.?com\s*$/i, '').trim();
      }
    }

    if (!result.firm_name) {
      const title = $('title').text() || '';
      result.firm_name = title.replace(/\s*[-|]\s*Manta\.?com\s*$/i, '')
                              .replace(/\s*[-|]\s*\w+,\s*\w{2}\s*$/i, '')
                              .trim();
    }

    // --- 3. Microdata / itemprop extraction (secondary) ---
    if (!result.phone) {
      const phoneProp = $('[itemprop="telephone"]').first().text().trim();
      if (phoneProp) result.phone = phoneProp;
    }

    if (!result.email) {
      // Check itemprop="email"
      const emailProp = $('[itemprop="email"]').first();
      if (emailProp.length) {
        const emailText = emailProp.attr('content') || emailProp.attr('href') || emailProp.text();
        if (emailText) {
          result.email = emailText.replace(/^mailto:/i, '').trim();
        }
      }
    }

    if (!result.website) {
      // Look for itemprop="url" that isn't manta.com
      $('[itemprop="url"]').each((_, el) => {
        const href = $(el).attr('href') || $(el).attr('content') || $(el).text();
        if (href && !href.includes('manta.com') && !this.isExcludedDomain(href)) {
          result.website = href.trim();
          return false;
        }
      });
    }

    if (!result.city) {
      const locality = $('[itemprop="addressLocality"]').first().text().trim();
      if (locality) result.city = locality;
    }
    if (!result.state) {
      const region = $('[itemprop="addressRegion"]').first().text().trim();
      if (region) result.state = region;
    }
    if (!result.zip) {
      const postal = $('[itemprop="postalCode"]').first().text().trim();
      if (postal) result.zip = postal;
    }
    if (!result.address) {
      const street = $('[itemprop="streetAddress"]').first().text().trim();
      if (street) result.address = street;
    }

    // --- 4. mailto: links ---
    if (!result.email) {
      $('a[href^="mailto:"]').each((_, el) => {
        const href = ($(el).attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
        if (href && href.includes('@') && !href.includes('manta.com') && !href.includes('example.com')) {
          result.email = href;
          return false;
        }
      });
    }

    // --- 5. tel: links ---
    if (!result.phone) {
      $('a[href^="tel:"]').each((_, el) => {
        const href = ($(el).attr('href') || '').replace(/^tel:/, '').trim();
        if (href && href.length >= 10) {
          result.phone = href;
          return false;
        }
      });
    }

    // --- 6. Text pattern extraction ---
    const bodyText = $('body').text();

    // Email from body text (fallback)
    if (!result.email) {
      const emailMatch = bodyText.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
      if (emailMatch) {
        const email = emailMatch[1].toLowerCase();
        if (!email.includes('manta.com') && !email.includes('example.com') &&
            !email.includes('sentry.io') && !email.includes('cloudflare')) {
          result.email = email;
        }
      }
    }

    // Phone from body text (fallback)
    if (!result.phone) {
      const phoneMatch = bodyText.match(/\((\d{3})\)\s*(\d{3})[.-](\d{4})/);
      if (phoneMatch) {
        result.phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;
      }
    }

    // Website from external links (fallback)
    if (!result.website) {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim().toLowerCase();
        if ((text.includes('website') || text.includes('visit')) &&
            !href.includes('manta.com') && !this.isExcludedDomain(href)) {
          result.website = href;
          return false;
        }
      });
    }

    // Contact person from text patterns (fallback)
    if (!result.first_name) {
      // Look for "Contact: Name" or "Owner: Name" patterns
      const contactMatch = bodyText.match(
        /(?:Contact|Owner|Principal|Manager|President)[:\s]+(?:Mr\.|Ms\.|Mrs\.|Dr\.)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/
      );
      if (contactMatch) {
        const parts = this.splitName(contactMatch[1].trim());
        result.first_name = parts.firstName;
        result.last_name = parts.lastName;
      }
    }

    // SIC/NAICS codes
    const sicMatch = bodyText.match(/SIC(?:\s*Code)?[:\s]+(\d{4,6})/i);
    if (sicMatch) {
      result.bar_number = `SIC: ${sicMatch[1]}`;
    }
    const naicsMatch = bodyText.match(/NAICS(?:\s*Code)?[:\s]+(\d{4,6})/i);
    if (naicsMatch) {
      const naics = `NAICS: ${naicsMatch[1]}`;
      result.bar_number = result.bar_number ? `${result.bar_number}, ${naics}` : naics;
    }

    // Category from breadcrumbs or page text
    const categoryMatch = bodyText.match(/(?:Category|Industry|Type)[:\s]+([A-Za-z\s&,]+?)(?:\n|SIC|NAICS|Revenue)/i);
    if (categoryMatch) {
      result.bar_status = categoryMatch[1].trim().substring(0, 100);
    }

    return result;
  }

  /**
   * Launch Puppeteer with stealth settings.
   * @returns {Browser} Puppeteer browser instance
   */
  async _launchBrowser() {
    let puppeteer;
    try {
      puppeteer = require('puppeteer');
    } catch (e) {
      // Try puppeteer-extra with stealth plugin
      try {
        puppeteer = require('puppeteer-extra');
        const StealthPlugin = require('puppeteer-extra-plugin-stealth');
        puppeteer.use(StealthPlugin());
      } catch (e2) {
        throw new Error('Neither puppeteer nor puppeteer-extra is available');
      }
    }

    const launchOptions = {
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--window-size=1920,1080',
      ],
    };

    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    return puppeteer.launch(launchOptions);
  }

  /**
   * Configure a Puppeteer page with realistic settings.
   * @param {Page} page - Puppeteer page
   */
  async _configurePage(page) {
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    );

    await page.setViewport({ width: 1920, height: 1080 });

    // Block heavy resources to speed up loading
    await page.setRequestInterception(true);
    page.on('request', req => {
      const type = req.resourceType();
      if (['image', 'font', 'media'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });
  }

  /**
   * Check if page is a Cloudflare challenge / bot block.
   * @param {Page} page - Puppeteer page
   * @returns {boolean}
   */
  async _isBlocked(page) {
    return page.evaluate(() => {
      const title = document.title || '';
      return title.includes('Just a moment') ||
             title.includes('Attention Required') ||
             !!document.querySelector('#challenge-running') ||
             !!document.querySelector('#cf-challenge-running') ||
             !!document.querySelector('.cf-browser-verification');
    });
  }

  /**
   * Wait for Cloudflare challenge to resolve (up to 15s).
   * @param {Page} page - Puppeteer page
   * @returns {boolean} true if challenge resolved
   */
  async _waitForChallenge(page) {
    for (let i = 0; i < 5; i++) {
      await new Promise(r => setTimeout(r, 3000));
      const stillBlocked = await this._isBlocked(page);
      if (!stillBlocked) return true;
    }
    return false;
  }

  /**
   * Extract profile links from search results using Puppeteer.
   *
   * @param {Page} page - Puppeteer page
   * @param {string} url - Search URL
   * @returns {{ profileUrls: string[], totalCount: number }|null}
   */
  async _fetchSearchPage(page, url) {
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

      // Check for Cloudflare challenge
      if (await this._isBlocked(page)) {
        log.warn('[Manta] Cloudflare challenge detected — waiting...');
        const resolved = await this._waitForChallenge(page);
        if (!resolved) return null;
      }

      // Wait for content to render
      await new Promise(r => setTimeout(r, 2000));

      // Get page HTML and parse with cheerio
      const html = await page.content();
      return this._parseSearchPage(html);
    } catch (err) {
      log.warn(`[Manta] Failed to fetch search page: ${err.message}`);
      return null;
    }
  }

  /**
   * Fetch and parse a business profile page.
   *
   * @param {Page} page - Puppeteer page
   * @param {string} profileUrl - Profile URL
   * @returns {object|null} Lead data or null
   */
  async _fetchProfilePage(page, profileUrl) {
    try {
      await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: 25000 });

      // Handle Cloudflare
      if (await this._isBlocked(page)) {
        const resolved = await this._waitForChallenge(page);
        if (!resolved) return null;
      }

      // Wait for content
      await new Promise(r => setTimeout(r, 1500));

      const html = await page.content();
      const data = this._parseProfilePage(html);

      // Must have at least a firm name to be useful
      if (!data.firm_name) return null;

      data.profile_url = profileUrl;
      return data;
    } catch (err) {
      log.warn(`[Manta] Failed to fetch profile: ${err.message}`);
      return null;
    }
  }

  /**
   * Main search generator — uses Puppeteer to scrape Manta search + profile pages.
   *
   * Strategy:
   *   1. For each city: search Manta for the category
   *   2. Extract profile URLs from search results
   *   3. Visit each profile page and extract rich data
   *   4. Yield leads with email, phone, website, contact person
   *
   * @param {string} practiceArea - Practice area to search (e.g., 'roofing')
   * @param {object} options - Search options
   */
  async *search(practiceArea, options = {}) {
    // Resolve category
    const practiceKey = (practiceArea || 'roofing').toLowerCase().trim();
    const categorySlug = CATEGORY_SLUGS[practiceKey] || practiceKey.replace(/\s+/g, '-');

    log.scrape(`[Manta] Starting search: category=${categorySlug}`);

    // Launch browser
    let browser;
    try {
      browser = await this._launchBrowser();
    } catch (err) {
      log.error(`[Manta] Failed to launch Puppeteer: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: 'Puppeteer not available — Manta requires browser rendering (Cloudflare WAF)' };
      return;
    }

    try {
      const page = await browser.newPage();
      await this._configurePage(page);

      // Determine cities to search
      let citiesToSearch = options.city ? [options.city] : [...DEFAULT_CITIES];
      if (options.maxCities) citiesToSearch = citiesToSearch.slice(0, options.maxCities);

      const maxPagesPerCity = options.maxPages || 5;
      const seenProfileIds = new Set();
      let totalYielded = 0;

      for (let ci = 0; ci < citiesToSearch.length; ci++) {
        const cityInput = citiesToSearch[ci];
        const { city, state } = this._parseCityInput(cityInput);

        if (!state) {
          log.warn(`[Manta] Skipping "${cityInput}" — no state code (use "City, ST" format)`);
          continue;
        }

        yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
        log.scrape(`[Manta] Searching ${categorySlug} in ${city}, ${state}`);

        let pageNum = 1;
        let pagesFetched = 0;
        let totalResults = 0;
        let consecutiveEmpty = 0;
        let cityYielded = 0;

        while (pagesFetched < maxPagesPerCity) {
          const url = this._buildSearchUrl(categorySlug, city, state, pageNum);
          log.info(`[Manta] Search page ${pageNum} — ${url}`);

          // Polite delay between search pages
          await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));

          const searchResult = await this._fetchSearchPage(page, url);

          if (!searchResult) {
            log.warn(`[Manta] Search page failed for ${city} page ${pageNum}`);
            yield { _captcha: true, city: `${city}, ${state}`, page: pageNum };
            break;
          }

          const { profileUrls, totalCount } = searchResult;

          if (pageNum === 1 && totalCount > 0) {
            totalResults = totalCount;
            const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
            log.success(`[Manta] Found ~${totalResults.toLocaleString()} results for ${city}, ${state} (est. ${totalPages} pages)`);
          }

          if (profileUrls.length === 0) {
            consecutiveEmpty++;
            if (consecutiveEmpty >= 2 || pageNum === 1) {
              if (pageNum === 1) {
                log.info(`[Manta] No results for ${categorySlug} in ${city}, ${state}`);
              } else {
                log.info(`[Manta] ${consecutiveEmpty} consecutive empty pages — done with ${city}`);
              }
              break;
            }
            pageNum++;
            pagesFetched++;
            continue;
          }

          consecutiveEmpty = 0;
          log.info(`[Manta] Found ${profileUrls.length} profile links on page ${pageNum}`);

          // Visit each profile page
          for (const profileUrl of profileUrls) {
            // Extract ID for dedup
            const idMatch = profileUrl.match(/\/c\/(m[a-z0-9]+)\//i);
            const profileId = idMatch ? idMatch[1] : profileUrl;
            if (seenProfileIds.has(profileId)) continue;
            seenProfileIds.add(profileId);

            // Polite delay between profile fetches (2-5s)
            await new Promise(r => setTimeout(r, 2000 + Math.random() * 3000));

            const data = await this._fetchProfilePage(page, profileUrl);
            if (!data) continue;

            const lead = this.transformResult({
              first_name: data.first_name || '',
              last_name: data.last_name || '',
              firm_name: data.firm_name || '',
              phone: data.phone || '',
              email: data.email || '',
              website: data.website || '',
              city: data.city || city,
              state: data.state || state,
              zip: data.zip || '',
              address: data.address || '',
              bar_number: data.bar_number || '',
              bar_status: data.bar_status || '',
              profile_url: profileUrl,
              source: 'manta_contractors',
            }, practiceArea);

            yield lead;
            cityYielded++;
            totalYielded++;
          }

          log.info(`[Manta] Page ${pageNum}: ${profileUrls.length} profiles (${cityYielded} leads from ${city})`);

          // Check if we've likely exhausted results
          if (profileUrls.length < RESULTS_PER_PAGE - 5) {
            log.success(`[Manta] Completed ${city}, ${state}: ${cityYielded} leads`);
            break;
          }

          // Safety: don't go beyond reasonable page count
          const maxPossiblePages = totalResults > 0
            ? Math.ceil(totalResults / RESULTS_PER_PAGE)
            : 100;
          if (pageNum >= maxPossiblePages) {
            log.success(`[Manta] Completed all ${pageNum} pages for ${city}, ${state}`);
            break;
          }

          pageNum++;
          pagesFetched++;
        }

        // Polite delay between cities (5-10s)
        if (ci < citiesToSearch.length - 1) {
          await new Promise(r => setTimeout(r, 5000 + Math.random() * 5000));
        }
      }

      log.success(`[Manta] Scrape complete — ${totalYielded} total leads from ${seenProfileIds.size} profiles`);

    } finally {
      if (browser) await browser.close();
    }
  }
}

module.exports = new MantaScraper();
