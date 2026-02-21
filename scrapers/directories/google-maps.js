/**
 * Google Maps Scraper (Free, Puppeteer-based)
 *
 * Source: Google Maps via Puppeteer
 * Method: Browser automation (no API key needed)
 * Data:   Business name, address, phone, website, rating, Google Maps URL
 *
 * Free alternative to Google Places API. Uses Puppeteer to scrape
 * Google Maps search results. Supports any business niche.
 *
 * Pass options.niche = "dentists", "plumbers", etc. Defaults to "lawyers".
 * Pass options.city = "Miami" to search a specific city.
 *
 * Technical notes:
 * - Uses puppeteer-extra with stealth plugin for anti-bot
 * - Blocks image/tile requests for speed
 * - Uses aria-label and data-tooltip attributes (stable selectors)
 * - Rate limited: 5-10s between page loads
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

// Major cities — same as google-places.js
const DEFAULT_CITY_ENTRIES = [
  // US (top 30)
  { city: 'New York', stateCode: 'NY', country: 'US' },
  { city: 'Los Angeles', stateCode: 'CA', country: 'US' },
  { city: 'Chicago', stateCode: 'IL', country: 'US' },
  { city: 'Houston', stateCode: 'TX', country: 'US' },
  { city: 'Phoenix', stateCode: 'AZ', country: 'US' },
  { city: 'Philadelphia', stateCode: 'PA', country: 'US' },
  { city: 'San Diego', stateCode: 'CA', country: 'US' },
  { city: 'Dallas', stateCode: 'TX', country: 'US' },
  { city: 'Austin', stateCode: 'TX', country: 'US' },
  { city: 'Jacksonville', stateCode: 'FL', country: 'US' },
  { city: 'San Francisco', stateCode: 'CA', country: 'US' },
  { city: 'Seattle', stateCode: 'WA', country: 'US' },
  { city: 'Denver', stateCode: 'CO', country: 'US' },
  { city: 'Nashville', stateCode: 'TN', country: 'US' },
  { city: 'Boston', stateCode: 'MA', country: 'US' },
  { city: 'Atlanta', stateCode: 'GA', country: 'US' },
  { city: 'Miami', stateCode: 'FL', country: 'US' },
  { city: 'Tampa', stateCode: 'FL', country: 'US' },
  { city: 'Minneapolis', stateCode: 'MN', country: 'US' },
  { city: 'Cleveland', stateCode: 'OH', country: 'US' },
  // Canada
  { city: 'Toronto', stateCode: 'CA-ON', country: 'CA' },
  { city: 'Vancouver', stateCode: 'CA-BC', country: 'CA' },
  { city: 'Montreal', stateCode: 'CA-QC', country: 'CA' },
  { city: 'Calgary', stateCode: 'CA-AB', country: 'CA' },
  // UK
  { city: 'London', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Manchester', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Edinburgh', stateCode: 'UK-SC', country: 'UK' },
  // Australia
  { city: 'Sydney', stateCode: 'AU-NSW', country: 'AU' },
  { city: 'Melbourne', stateCode: 'AU-VIC', country: 'AU' },
  { city: 'Brisbane', stateCode: 'AU-QLD', country: 'AU' },
];

class GoogleMapsScraper extends BaseScraper {
  constructor() {
    super({
      name: 'google-maps',
      stateCode: 'GOOGLE-MAPS',
      baseUrl: 'https://www.google.com/maps',
      pageSize: 20,
      practiceAreaCodes: {},
      defaultCities: DEFAULT_CITY_ENTRIES.map(e => e.city),
    });

    this._cityEntries = DEFAULT_CITY_ENTRIES;
    this._browser = null;
  }

  /**
   * Launch Puppeteer browser with stealth plugin.
   */
  async _ensureBrowser() {
    if (this._browser) return;

    let puppeteer;
    try {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteerExtra.use(StealthPlugin());
      puppeteer = puppeteerExtra;
    } catch {
      puppeteer = require('puppeteer');
    }

    const launchOpts = {
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-web-security',
      ],
    };

    // Railway uses system Chromium
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this._browser = await puppeteer.launch(launchOpts);
  }

  async _closeBrowser() {
    if (this._browser) {
      await this._browser.close().catch(() => {});
      this._browser = null;
    }
  }

  /**
   * Search Google Maps for businesses across cities.
   */
  async *search(practiceArea, options = {}) {
    const niche = (options.niche || 'lawyers').trim();
    const rateLimiter = new RateLimiter({ minDelay: 5000, maxDelay: 10000 });
    const maxCities = options.maxCities || null;

    // Filter cities
    let cities = [...this._cityEntries];
    if (options.city) {
      cities = cities.filter(c =>
        c.city.toLowerCase().includes(options.city.toLowerCase())
      );
      if (cities.length === 0) {
        cities = [{ city: options.city, stateCode: '', country: 'US' }];
      }
    }

    if (maxCities) cities = cities.slice(0, maxCities);

    try {
      await this._ensureBrowser();
    } catch (err) {
      log.error(`[Google Maps] Failed to launch browser: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Browser launch failed: ${err.message}` };
      return;
    }

    try {
      for (let i = 0; i < cities.length; i++) {
        const cityEntry = cities[i];
        yield { _cityProgress: { current: i + 1, total: cities.length } };

        const query = `${niche} in ${cityEntry.city}`;
        log.info(`[Google Maps] Searching: "${query}"`);

        await rateLimiter.wait();

        try {
          const results = await this._scrapeMapResults(query, options.maxPages);

          for (const result of results) {
            const lead = this._parseMapResult(result, cityEntry, niche);
            if (lead) yield lead;
          }

          log.info(`[Google Maps] Found ${results.length} results for "${query}"`);
        } catch (err) {
          log.warn(`[Google Maps] Failed for "${query}": ${err.message}`);
        }
      }
    } finally {
      await this._closeBrowser();
    }
  }

  /**
   * Scrape Google Maps search results page.
   * @param {string} query - Search query like "dentists in Miami"
   * @param {number} [maxScrolls] - Max scroll iterations (null = scroll until no more)
   * @returns {object[]} Array of business data
   */
  async _scrapeMapResults(query, maxScrolls) {
    const page = await this._browser.newPage();

    try {
      // Block images and tiles for speed
      await page.setRequestInterception(true);
      page.on('request', (req) => {
        const type = req.resourceType();
        if (['image', 'media', 'font'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      await page.setViewport({ width: 1280, height: 900 });

      // Navigate to Google Maps search
      const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

      // Wait for results feed
      await page.waitForSelector('div[role="feed"]', { timeout: 15000 }).catch(() => {});

      // Check for consent dialog and dismiss
      await this._dismissConsent(page);

      // Scroll the results feed to load more results
      const scrollLimit = maxScrolls ? Math.min(maxScrolls, 6) : 6;
      await this._scrollFeed(page, scrollLimit);

      // Extract results from the feed
      const results = await page.evaluate(() => {
        const items = [];
        const feed = document.querySelector('div[role="feed"]');
        if (!feed) return items;

        // Each result is an <a> element with an aria-label inside the feed
        const links = feed.querySelectorAll('a[aria-label]');

        for (const link of links) {
          const name = link.getAttribute('aria-label') || '';
          if (!name) continue;

          const href = link.getAttribute('href') || '';

          // Try to find rating
          let rating = '';
          const ratingEl = link.closest('[data-value]') ||
            link.querySelector('span[role="img"]');
          if (ratingEl) {
            const ariaLabel = ratingEl.getAttribute('aria-label') || '';
            const match = ariaLabel.match(/([\d.]+)\s*star/i);
            if (match) rating = match[1];
          }

          items.push({ name, href, rating });
        }

        return items;
      });

      // For each result, click and extract detail info
      const detailedResults = [];
      const maxDetails = Math.min(results.length, 60); // Cap at 60 per query

      for (let i = 0; i < maxDetails; i++) {
        try {
          const detail = await this._extractDetail(page, i);
          if (detail) {
            detail.name = detail.name || results[i].name;
            detail.mapsUrl = results[i].href || '';
            detail.rating = detail.rating || results[i].rating;
            detailedResults.push(detail);
          }
        } catch {
          // If detail extraction fails, still capture basic info
          if (results[i].name) {
            detailedResults.push({
              name: results[i].name,
              mapsUrl: results[i].href || '',
              rating: results[i].rating,
              phone: '',
              website: '',
              address: '',
            });
          }
        }
      }

      return detailedResults;
    } finally {
      await page.close().catch(() => {});
    }
  }

  /**
   * Dismiss Google consent/cookie dialog if present.
   */
  async _dismissConsent(page) {
    try {
      // Google consent form button selectors
      const consentSelectors = [
        'button[aria-label="Accept all"]',
        'button[aria-label="Reject all"]',
        'form[action*="consent"] button',
        'button:has-text("Accept")',
      ];

      for (const sel of consentSelectors) {
        const btn = await page.$(sel);
        if (btn) {
          await btn.click();
          await sleep(1000);
          break;
        }
      }
    } catch {
      // Consent dialog may not appear
    }
  }

  /**
   * Scroll the results feed to load more items.
   */
  async _scrollFeed(page, maxScrolls) {
    for (let i = 0; i < maxScrolls; i++) {
      const previousCount = await page.evaluate(() => {
        const feed = document.querySelector('div[role="feed"]');
        return feed ? feed.querySelectorAll('a[aria-label]').length : 0;
      });

      // Scroll the feed container
      await page.evaluate(() => {
        const feed = document.querySelector('div[role="feed"]');
        if (feed) feed.scrollTop = feed.scrollHeight;
      });

      await sleep(2000);

      const newCount = await page.evaluate(() => {
        const feed = document.querySelector('div[role="feed"]');
        return feed ? feed.querySelectorAll('a[aria-label]').length : 0;
      });

      // Check for "end of results" text
      const endReached = await page.evaluate(() => {
        const feed = document.querySelector('div[role="feed"]');
        if (!feed) return true;
        const text = feed.innerText || '';
        return text.includes("You've reached the end of the list") ||
               text.includes('No more results');
      });

      if (endReached || newCount === previousCount) break;
    }
  }

  /**
   * Click on a result and extract detail information.
   */
  async _extractDetail(page, index) {
    // Click the result
    const clicked = await page.evaluate((idx) => {
      const feed = document.querySelector('div[role="feed"]');
      if (!feed) return false;
      const links = feed.querySelectorAll('a[aria-label]');
      if (idx >= links.length) return false;
      links[idx].click();
      return true;
    }, index);

    if (!clicked) return null;

    // Wait for detail panel to load
    await sleep(1500);

    // Extract detail data
    const detail = await page.evaluate(() => {
      const result = { name: '', phone: '', website: '', address: '', rating: '' };

      // Business name from header
      const heading = document.querySelector('h1');
      if (heading) result.name = heading.textContent.trim();

      // Phone — look for tel: links or copy phone button
      const phoneBtn = document.querySelector('button[data-tooltip="Copy phone number"]') ||
                        document.querySelector('a[href^="tel:"]');
      if (phoneBtn) {
        if (phoneBtn.href && phoneBtn.href.startsWith('tel:')) {
          result.phone = phoneBtn.href.replace('tel:', '');
        } else {
          // Extract from aria-label or nearby text
          const label = phoneBtn.getAttribute('aria-label') || '';
          const phoneMatch = label.match(/[\d()+\- .]{7,}/);
          if (phoneMatch) result.phone = phoneMatch[0].trim();
          // Fallback: look at button's parent text
          if (!result.phone) {
            const parent = phoneBtn.closest('[data-tooltip]')?.parentElement;
            if (parent) {
              const text = parent.textContent || '';
              const pm = text.match(/[\d()+\- .]{7,}/);
              if (pm) result.phone = pm[0].trim();
            }
          }
        }
      }

      // Fallback: look for any tel: link on the page detail panel
      if (!result.phone) {
        const telLinks = document.querySelectorAll('a[href^="tel:"]');
        for (const tl of telLinks) {
          result.phone = tl.href.replace('tel:', '');
          break;
        }
      }

      // Website
      const websiteLink = document.querySelector('a[data-tooltip="Open website"]') ||
                          document.querySelector('a[aria-label*="website" i]');
      if (websiteLink) {
        result.website = websiteLink.href || '';
      }

      // Fallback: look for external links that aren't social media
      if (!result.website) {
        const allLinks = document.querySelectorAll('a[href^="http"]');
        for (const link of allLinks) {
          const href = link.href || '';
          if (href.includes('google.com') || href.includes('facebook.com') ||
              href.includes('yelp.com') || href.includes('instagram.com') ||
              href.includes('twitter.com') || href.includes('youtube.com')) continue;
          // Check if it looks like a business website
          if (!href.includes('/maps/') && !href.includes('/search')) {
            result.website = href;
            break;
          }
        }
      }

      // Address
      const addressBtn = document.querySelector('button[data-tooltip="Copy address"]');
      if (addressBtn) {
        const label = addressBtn.getAttribute('aria-label') || '';
        result.address = label.replace(/^Address:\s*/i, '').trim();
        if (!result.address) {
          const parent = addressBtn.closest('[data-tooltip]')?.parentElement;
          if (parent) result.address = parent.textContent.trim();
        }
      }

      // Rating
      const ratingEl = document.querySelector('span[role="img"][aria-label*="star" i]');
      if (ratingEl) {
        const ratingMatch = (ratingEl.getAttribute('aria-label') || '').match(/([\d.]+)/);
        if (ratingMatch) result.rating = ratingMatch[1];
      }

      return result;
    });

    return detail;
  }

  /**
   * Parse a Maps result into a lead object.
   */
  _parseMapResult(result, cityEntry, niche) {
    if (!result || !result.name) return null;

    const { firstName, lastName, firmName } = this._parseBusinessName(result.name);

    // Parse address for city/state
    const { city, state } = this._parseAddress(result.address || '', cityEntry);

    const nicheTag = niche && !/^lawyers?$/i.test(niche) ? `_${niche.replace(/\s+/g, '_')}` : '';

    return this.transformResult({
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName || result.name,
      city: city || cityEntry.city,
      state: state || cityEntry.stateCode,
      phone: result.phone || '',
      website: result.website || '',
      email: '',
      bar_number: '',
      bar_status: '',
      admission_date: '',
      source: `google_maps${nicheTag}`,
      profile_url: result.mapsUrl || '',
      _rating: result.rating || '',
    }, '');
  }

  /**
   * Try to determine if a business name is a person or a firm.
   */
  _parseBusinessName(name) {
    const lower = name.toLowerCase();

    const firmIndicators = [
      'llp', 'pllc', 'p.c.', 'p.a.', 'llc', 'inc', 'ltd', 'plc',
      '& associates', 'and associates', 'law firm', 'law office',
      'law group', 'law center', 'legal', 'attorneys at law',
      'attorneys-at-law', 'dental', 'clinic', 'group', 'center',
      'services', 'solutions', 'company', 'corp', 'studio',
    ];

    const isFirm = firmIndicators.some(ind => lower.includes(ind)) ||
      name.includes('&') || name.includes(' and ');

    if (isFirm) {
      return { firstName: '', lastName: '', firmName: name };
    }

    // Simple person detection: 2-3 word proper-case names
    const cleaned = name.replace(/,?\s*(dr\.?|dds|dmd|md|esq\.?|attorney|lawyer|phd)/gi, '').trim();
    const parts = cleaned.split(/\s+/);
    if (parts.length >= 2 && parts.length <= 3) {
      const allProperCase = parts.every(p => /^[A-Z][a-z]/.test(p));
      if (allProperCase) {
        return { firstName: parts[0], lastName: parts[parts.length - 1], firmName: '' };
      }
    }

    return { firstName: '', lastName: '', firmName: name };
  }

  /**
   * Parse city and state from a formatted address string.
   */
  _parseAddress(address, cityEntry) {
    if (!address) return { city: '', state: '' };

    const parts = address.split(',').map(p => p.trim());

    if (parts.length >= 3) {
      const cityCandidate = parts[parts.length - 3] || '';
      const stateZip = parts[parts.length - 2] || '';

      const stateMatch = stateZip.match(/^([A-Z]{2})\s+\d/);
      if (stateMatch) {
        return { city: cityCandidate, state: stateMatch[1] };
      }
    }

    return { city: cityEntry.city, state: cityEntry.stateCode };
  }
}

module.exports = new GoogleMapsScraper();
