/**
 * Gas Safe Register — UK Mandatory Gas Engineer Registry
 *
 * Source: https://www.gassaferegister.co.uk/find-an-engineer/
 * Records: ~130,000 registered gas businesses across the UK
 * Method: Puppeteer with stealth (Imperva/Incapsula WAF requires JS execution)
 *
 * The Gas Safe Register is the official UK registration body for gas engineers.
 * By law, all gas engineers must be registered. The public directory provides:
 *   - Business/trading name
 *   - Gas Safe registration number (6-7 digits)
 *   - Trading address (town, postcode)
 *   - Phone number
 *   - Work categories (appliance types: boilers, cookers, fires, etc.)
 *   - ID card expiry date
 *
 * Search is by postcode with configurable radius (5-50 miles).
 * Individual business profile pages may contain additional contact details.
 *
 * WAF: The entire domain is behind Imperva/Incapsula WAF which serves
 * "Pardon Our Interruption" challenge pages to automated requests.
 * Even puppeteer-extra-plugin-stealth may be blocked — the scraper
 * detects this and yields _captcha signals. Success depends on IP
 * reputation and may require residential proxies.
 *
 * URL patterns:
 *   Search: /find-an-engineer/by-postcode/?postcode={POSTCODE}
 *   Profile: /find-an-engineer/engineer-profile/{REG_NUMBER}/
 *
 * Note: Email is NOT displayed on public Gas Safe profiles. Emails can
 * be derived via the enrichment pipeline using the business website.
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.gassaferegister.co.uk';
const SEARCH_PATH = '/find-an-engineer/by-postcode/';
const PROFILE_PATH = '/find-an-engineer/engineer-profile/';

// Default postcodes covering major UK areas — one per region
const DEFAULT_POSTCODES = [
  'SW1',    // London (Central)
  'EC1',    // London (City)
  'M1',     // Manchester
  'B1',     // Birmingham
  'LS1',    // Leeds
  'G1',     // Glasgow
  'L1',     // Liverpool
  'EH1',    // Edinburgh
  'BS1',    // Bristol
  'S1',     // Sheffield
];

// Extended postcodes for comprehensive UK coverage
const EXTENDED_POSTCODES = [
  ...DEFAULT_POSTCODES,
  'NE1',    // Newcastle
  'NG1',    // Nottingham
  'LE1',    // Leicester
  'CF1',    // Cardiff
  'BT1',    // Belfast
  'AB1',    // Aberdeen
  'PO1',    // Portsmouth
  'SO1',    // Southampton
  'OX1',    // Oxford
  'CB1',    // Cambridge
  'EX1',    // Exeter
  'PL1',    // Plymouth
  'BA1',    // Bath
  'BN1',    // Brighton
  'CT1',    // Canterbury
  'IP1',    // Ipswich
  'NR1',    // Norwich
  'YO1',    // York
  'HU1',    // Hull
  'DY1',    // Dudley
  'CV1',    // Coventry
];

// Gas appliance categories (for practice_area filtering)
const PRACTICE_AREA_CODES = {
  'boiler': 'boiler',
  'boilers': 'boiler',
  'cooker': 'cooker',
  'cookers': 'cooker',
  'fire': 'fire',
  'fires': 'fire',
  'gas fire': 'fire',
  'water heater': 'water heater',
  'warm air': 'warm air',
  'all': 'all',
};

class GasSafeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'gas-safe-register',
      stateCode: 'GAS-SAFE',
      baseUrl: BASE_URL,
      pageSize: 20, // Typical results per page
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_POSTCODES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('gas-safe: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('gas-safe: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('gas-safe: extractResultCount() not used — search() overridden'); }

  /**
   * Detect Imperva/Incapsula WAF challenge in page content.
   */
  _isWafBlocked(content) {
    return content.includes('Pardon Our Interruption') ||
           content.includes('_Incapsula_Resource') ||
           content.includes('Incapsula incident') ||
           content.includes('Access denied') && content.includes('Imperva') ||
           content.includes('Request unsuccessful');
  }

  /**
   * Launch Puppeteer with stealth plugin for WAF bypass.
   * Returns { browser, page } or null if Puppeteer unavailable.
   */
  async _launchBrowser() {
    try {
      const puppeteer = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteer.use(StealthPlugin());

      const launchOptions = {
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-blink-features=AutomationControlled',
        ],
      };
      if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
      }

      const browser = await puppeteer.launch(launchOptions);
      const page = await browser.newPage();

      // Set realistic browser headers
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );
      await page.setExtraHTTPHeaders({
        'Accept-Language': 'en-GB,en;q=0.9',
      });

      // Block images/fonts/media for speed
      await page.setRequestInterception(true);
      page.on('request', req => {
        const type = req.resourceType();
        if (['image', 'font', 'media'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      return { browser, page };
    } catch (err) {
      log.error(`[GasSafe] Failed to launch Puppeteer: ${err.message}`);
      return null;
    }
  }

  /**
   * Navigate to a URL with WAF challenge detection and retry.
   * Waits up to 15s for the Incapsula challenge to resolve.
   *
   * @param {Page} page - Puppeteer page instance
   * @param {string} url - URL to navigate to
   * @returns {string|null} Page HTML content, or null if WAF blocked
   */
  async _navigateWithWaf(page, url) {
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    } catch (err) {
      log.warn(`[GasSafe] Navigation timeout for ${url}: ${err.message}`);
      // Continue anyway — some content may have loaded
    }

    // Check initial load
    let content = await page.content();
    if (!this._isWafBlocked(content) && content.length > 2000) {
      return content;
    }

    // Wait for WAF challenge resolution (poll every 2s for up to 15s)
    for (let i = 0; i < 8; i++) {
      await new Promise(r => setTimeout(r, 2000));
      content = await page.content();
      if (!this._isWafBlocked(content) && content.length > 2000) {
        log.info(`[GasSafe] WAF challenge resolved after ${(i + 1) * 2}s`);
        return content;
      }
    }

    // Try one reload after cookies are set
    try {
      await page.reload({ waitUntil: 'networkidle2', timeout: 20000 });
      await new Promise(r => setTimeout(r, 3000));
      content = await page.content();
      if (!this._isWafBlocked(content) && content.length > 2000) {
        log.info('[GasSafe] WAF challenge resolved after reload');
        return content;
      }
    } catch (err) {
      // Ignore reload errors
    }

    return null; // WAF blocked
  }

  /**
   * Parse search results from the HTML content using Cheerio.
   * Gas Safe search results are rendered server-side (Umbraco CMS).
   *
   * Expected HTML structure (based on standard Umbraco patterns):
   *   .search-results / .engineer-results — container
   *     .result-item / .engineer-card — each business
   *       Business name, registration number, address, phone, work types
   */
  _parseSearchResults(html) {
    const $ = cheerio.load(html);
    const results = [];

    // Strategy 1: Look for structured result cards
    // Gas Safe uses Umbraco CMS — common patterns for search results
    const cardSelectors = [
      '.search-result', '.result-item', '.engineer-card', '.engineer-result',
      '.find-engineer-result', '.business-result', '[class*="result"]',
      '.results-list li', '.results-list > div', '.search-results > div',
      'article', '.card',
    ];

    let cards = $([]);
    for (const sel of cardSelectors) {
      const found = $(sel);
      if (found.length > 0) {
        // Filter to actual result cards (must contain business-like text)
        const filtered = found.filter(function() {
          const text = $(this).text();
          return text.length > 50 && (
            /\d{5,7}/.test(text) ||  // Registration number
            /gas safe/i.test(text) ||
            /registered/i.test(text) ||
            /0\d{3,4}\s?\d{3,4}/.test(text) // UK phone number
          );
        });
        if (filtered.length > 0) {
          cards = filtered;
          log.info(`[GasSafe] Found ${cards.length} result cards with selector: ${sel}`);
          break;
        }
      }
    }

    cards.each((_, card) => {
      const $card = $(card);
      const text = $card.text();

      const result = this._extractFromCard($, $card, text);
      if (result && result.firm_name) {
        results.push(result);
      }
    });

    // Strategy 2: If no cards found via selectors, try regex extraction on full body
    if (results.length === 0) {
      const bodyText = $('body').text();
      const regexResults = this._extractFromBodyText(bodyText, $);
      results.push(...regexResults);
    }

    // Strategy 3: Try extracting from links to profile pages
    if (results.length === 0) {
      $('a[href*="engineer-profile"]').each((_, el) => {
        const $link = $(el);
        const href = $link.attr('href') || '';
        const regNumMatch = href.match(/engineer-profile\/(\d+)/);
        if (!regNumMatch) return;

        const regNumber = regNumMatch[1];
        const name = $link.text().trim();
        const $container = $link.closest('div, li, article, section');
        const containerText = $container.length ? $container.text() : '';

        const phoneMatch = containerText.match(/(?:Tel|Phone|Contact)[:\s]*(\+?[\d\s()-]{10,15})/i) ||
                          containerText.match(/(0\d{3,4}\s?\d{3,4}\s?\d{0,4})/);

        results.push({
          firm_name: name || '',
          bar_number: regNumber,
          phone: phoneMatch ? phoneMatch[1].trim() : '',
          city: '',
          zip: '',
          profile_url: href.startsWith('http') ? href : `${BASE_URL}${href}`,
        });
      });
    }

    return results;
  }

  /**
   * Extract a business lead from a single result card.
   */
  _extractFromCard($, $card, text) {
    // Business/trading name — usually a heading or bold link
    let firmName = '';
    const nameEl = $card.find('h2, h3, h4, .business-name, .trading-name, [class*="name"] a, a[href*="engineer-profile"]').first();
    if (nameEl.length) {
      firmName = nameEl.text().trim();
    }

    // Registration number (5-7 digit Gas Safe number)
    const regMatch = text.match(/(?:Reg(?:istration)?\.?\s*(?:No\.?|Number)?[:\s#]*)?(\d{5,7})/);
    const regNumber = regMatch ? regMatch[1] : '';

    // Phone number (UK format)
    const phoneMatch = text.match(/(0\d{2,4}[\s-]?\d{3,4}[\s-]?\d{3,4})/) ||
                       text.match(/(\+44[\s-]?\d{2,4}[\s-]?\d{3,4}[\s-]?\d{3,4})/);
    const phone = phoneMatch ? phoneMatch[1].trim() : '';

    // Postcode (UK format)
    const postcodeMatch = text.match(/([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})/i);
    const postcode = postcodeMatch ? postcodeMatch[1].toUpperCase() : '';

    // Town/city — often near the postcode
    let city = '';
    const addressEl = $card.find('.address, .location, address, [class*="address"]').first();
    if (addressEl.length) {
      const addrText = addressEl.text().trim();
      // Extract city from address (usually before postcode)
      const cityMatch = addrText.match(/([A-Za-z\s]+),?\s*[A-Z]{1,2}\d/);
      if (cityMatch) city = cityMatch[1].trim();
    }

    // Profile URL
    let profileUrl = '';
    const profileLink = $card.find('a[href*="engineer-profile"]').first();
    if (profileLink.length) {
      profileUrl = profileLink.attr('href') || '';
      if (profileUrl && !profileUrl.startsWith('http')) {
        profileUrl = BASE_URL + profileUrl;
      }
      // Extract name from link if not found
      if (!firmName) firmName = profileLink.text().trim();
    }

    // Work types / appliance categories
    const workTypes = [];
    $card.find('.work-type, .appliance, .category, [class*="work"], [class*="appli"]').each((_, el) => {
      workTypes.push($(el).text().trim());
    });

    if (!firmName && !regNumber) return null;

    return {
      firm_name: firmName,
      bar_number: regNumber,
      phone: this._formatUkPhone(phone),
      city,
      zip: postcode,
      profile_url: profileUrl,
      practice_area: workTypes.join(', '),
    };
  }

  /**
   * Fallback: extract businesses from plain body text using regex patterns.
   */
  _extractFromBodyText(bodyText, $) {
    const results = [];
    // Look for registration numbers in the text
    const regNumbers = bodyText.match(/\b\d{5,7}\b/g);
    if (!regNumbers) return results;

    // Deduplicate
    const seen = new Set();
    for (const num of regNumbers) {
      if (seen.has(num)) continue;
      seen.add(num);

      // Try to find context around this number
      results.push({
        firm_name: '',
        bar_number: num,
        phone: '',
        city: '',
        zip: '',
        profile_url: `${BASE_URL}${PROFILE_PATH}${num}/`,
      });
    }

    return results;
  }

  /**
   * Parse an individual business profile page for detailed information.
   */
  _parseProfilePage(html) {
    const $ = cheerio.load(html);
    const data = {};

    const bodyText = $('body').text();

    // Business name
    const nameEl = $('h1, .business-name, .trading-name, [class*="company"], [class*="business"]').first();
    if (nameEl.length) {
      data.firm_name = nameEl.text().trim();
    }

    // Registration number
    const regMatch = bodyText.match(/(?:Gas Safe Register|Registration|Reg)[\s:]*#?\s*(\d{5,7})/i);
    if (regMatch) data.bar_number = regMatch[1];

    // Phone
    const phoneEl = $('a[href^="tel:"]').first();
    if (phoneEl.length) {
      data.phone = this._formatUkPhone(phoneEl.attr('href').replace('tel:', '').trim());
    } else {
      const phoneMatch = bodyText.match(/(0\d{2,4}[\s-]?\d{3,4}[\s-]?\d{3,4})/);
      if (phoneMatch) data.phone = this._formatUkPhone(phoneMatch[1]);
    }

    // Email (rare on Gas Safe but check anyway)
    const emailEl = $('a[href^="mailto:"]').first();
    if (emailEl.length) {
      data.email = emailEl.attr('href').replace('mailto:', '').trim().toLowerCase();
    } else {
      const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
      if (emailMatch) {
        const email = emailMatch[0].toLowerCase();
        if (!email.includes('gassaferegister') && !email.includes('example.com')) {
          data.email = email;
        }
      }
    }

    // Website
    const links = $('a[href^="http"]');
    links.each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href && !href.includes('gassaferegister') && !this.isExcludedDomain(href)) {
        if (!data.website) data.website = href;
      }
    });

    // Address
    const addrEl = $('address, .address, [class*="address"], [itemprop="address"]').first();
    if (addrEl.length) {
      const addrText = addrEl.text().trim();
      data.address = addrText;

      // Extract city
      const cityMatch = addrText.match(/([A-Za-z\s]+),?\s*[A-Z]{1,2}\d/);
      if (cityMatch) data.city = cityMatch[1].trim();

      // Extract postcode
      const pcMatch = addrText.match(/([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})/i);
      if (pcMatch) data.zip = pcMatch[1].toUpperCase();
    }

    // Work categories / appliance types
    const workTypes = [];
    $('[class*="work-categor"], [class*="appliance"], .category-list li, .work-type').each((_, el) => {
      workTypes.push($(el).text().trim());
    });
    if (workTypes.length) data.practice_area = workTypes.join(', ');

    // ID card expiry
    const expiryMatch = bodyText.match(/(?:expir|valid until|ID card)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})/i);
    if (expiryMatch) data.bar_status = `Active (expires ${expiryMatch[1]})`;

    return data;
  }

  /**
   * Format a UK phone number.
   */
  _formatUkPhone(phone) {
    if (!phone) return '';
    let digits = phone.replace(/[^\d+]/g, '');
    if (digits.startsWith('+44')) digits = '0' + digits.slice(3);
    if (digits.startsWith('44') && digits.length > 10) digits = '0' + digits.slice(2);
    if (digits.length >= 10 && digits.length <= 12) return digits;
    return phone.trim();
  }

  /**
   * Async generator that yields Gas Safe registered businesses.
   *
   * Strategy:
   * 1. Launch Puppeteer with stealth to bypass Imperva WAF
   * 2. For each postcode, load the search results page
   * 3. Parse business listings from the HTML
   * 4. Optionally visit individual profile pages for more data
   * 5. Yield normalized lead objects
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Determine postcodes to search
    const postcodes = options.city
      ? [options.city]
      : (options.extended ? EXTENDED_POSTCODES : DEFAULT_POSTCODES);
    const postcodesToSearch = options.maxCities
      ? postcodes.slice(0, options.maxCities)
      : postcodes;

    log.scrape(`[GasSafe] Starting search: ${postcodesToSearch.length} postcodes, practiceArea=${practiceArea || 'all'}`);

    // Launch browser
    const session = await this._launchBrowser();
    if (!session) {
      yield {
        _captcha: true,
        city: 'all',
        reason: 'Puppeteer not available — Gas Safe Register requires browser rendering (Imperva WAF)',
      };
      return;
    }

    const { browser, page } = session;
    const seenRegNumbers = new Set();
    let totalYielded = 0;
    let wafBlocked = false;

    try {
      // Warm up: load the homepage first to establish cookies
      log.info('[GasSafe] Warming up session (loading homepage)...');
      const homeContent = await this._navigateWithWaf(page, BASE_URL);
      if (!homeContent) {
        log.warn('[GasSafe] Imperva WAF blocked homepage — automated access denied');
        log.info('[GasSafe] The Gas Safe Register uses Imperva/Incapsula WAF which blocks automated browsers');
        log.info('[GasSafe] This may work with residential proxies or manual cookie injection');
        yield {
          _captcha: true,
          city: 'all',
          reason: 'Imperva/Incapsula WAF — all automated access blocked. Requires residential proxy or manual session cookies.',
        };
        wafBlocked = true;
      }

      if (!wafBlocked) {
        // Brief wait after homepage
        await new Promise(r => setTimeout(r, 2000));

        for (let ci = 0; ci < postcodesToSearch.length; ci++) {
          const postcode = postcodesToSearch[ci];
          yield { _cityProgress: { current: ci + 1, total: postcodesToSearch.length } };
          log.scrape(`[GasSafe] Searching postcode: ${postcode}`);

          let pageNum = 0;
          let searchUrl = `${BASE_URL}${SEARCH_PATH}?postcode=${encodeURIComponent(postcode)}`;

          while (pageNum < maxPages) {
            log.info(`[GasSafe] Page ${pageNum + 1} — ${searchUrl}`);

            await rateLimiter.wait();
            const content = await this._navigateWithWaf(page, searchUrl);

            if (!content) {
              log.warn(`[GasSafe] WAF blocked search for postcode ${postcode}`);
              yield { _captcha: true, city: postcode, reason: 'Imperva WAF blocked request' };
              break;
            }

            // Parse results from the page
            const results = this._parseSearchResults(content);
            log.info(`[GasSafe] Found ${results.length} businesses for postcode ${postcode} (page ${pageNum + 1})`);

            if (results.length === 0) {
              break;
            }

            // Yield each result
            for (const biz of results) {
              // Dedup by registration number
              if (biz.bar_number && seenRegNumbers.has(biz.bar_number)) continue;
              if (biz.bar_number) seenRegNumbers.add(biz.bar_number);

              // Build lead object
              const lead = {
                first_name: '',
                last_name: '',
                firm_name: biz.firm_name || '',
                city: biz.city || '',
                state: 'UK',
                zip: biz.zip || '',
                phone: biz.phone || '',
                email: biz.email || '',
                website: biz.website || '',
                bar_number: biz.bar_number || '',
                bar_status: biz.bar_status || 'Gas Safe Registered',
                source: 'gas_safe_contractors',
                practice_area: practiceArea || biz.practice_area || 'Gas Engineer',
                profile_url: biz.profile_url || (biz.bar_number ? `${BASE_URL}${PROFILE_PATH}${biz.bar_number}/` : ''),
              };

              yield this.transformResult(lead, practiceArea || 'Gas Engineer');
              totalYielded++;
            }

            // Check for next page link
            const nextPageUrl = await page.evaluate(() => {
              const nextLink = document.querySelector(
                'a.next, a[rel="next"], .pagination a:last-child, [class*="next"] a, ' +
                'a[aria-label="Next"], .pager a:last-child'
              );
              if (nextLink && nextLink.getAttribute('href')) {
                return nextLink.getAttribute('href');
              }
              return null;
            });

            if (!nextPageUrl) break;

            searchUrl = nextPageUrl.startsWith('http') ? nextPageUrl : `${BASE_URL}${nextPageUrl}`;
            pageNum++;

            // Polite delay between pages (3-6s)
            await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));
          }

          // Polite delay between postcodes (5-10s)
          if (ci < postcodesToSearch.length - 1) {
            await new Promise(r => setTimeout(r, 5000 + Math.random() * 5000));
          }
        }
      }

      log.success(`[GasSafe] Scrape complete — ${totalYielded} businesses yielded (${seenRegNumbers.size} unique)`);

    } finally {
      if (browser) await browser.close();
    }
  }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'gas_safe_contractors';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }
}

module.exports = new GasSafeScraper();
