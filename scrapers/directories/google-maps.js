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
 * - Geo-grid splitting: subdivides cities into ~2km cells for comprehensive coverage
 * - Blocks image/tile requests for speed
 * - Extracts data from feed cards (name, rating, category, address)
 * - Clicks each result for phone/website, waits for panel to match
 * - Rate limited: 5-10s between page loads
 * - Deduplicates by business name within a city
 */

const BaseScraper = require('../base-scraper');
const https = require('https');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

// Nominatim bounding-box cache (avoids re-fetching during a session, capped at 100 entries)
const _boundsCache = new Map();
const BOUNDS_CACHE_MAX = 100;
function _boundsCacheSet(key, val) {
  if (_boundsCache.size >= BOUNDS_CACHE_MAX) {
    const oldest = _boundsCache.keys().next().value;
    _boundsCache.delete(oldest);
  }
  _boundsCache.set(key, val);
}

// Major cities — same as google-places.js
const DEFAULT_CITY_ENTRIES = [
  // US (top 20)
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
      // Only register stealth once on the singleton
      if (!puppeteerExtra._stealthRegistered) {
        puppeteerExtra.use(StealthPlugin());
        puppeteerExtra._stealthRegistered = true;
      }
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
        '--disable-blink-features=AutomationControlled',
        '--window-size=1280,900',
      ],
    };

    // Railway uses system Chromium
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this._browser = await puppeteer.launch(launchOpts);
  }

  /**
   * Apply anti-detection patches to a new page.
   * Defense-in-depth on top of stealth plugin.
   */
  async _applyAntiDetection(page) {
    await page.evaluateOnNewDocument(() => {
      // Hide webdriver flag
      Object.defineProperty(navigator, 'webdriver', { get: () => false });

      // Fake plugins array (headless Chrome has 0 plugins)
      Object.defineProperty(navigator, 'plugins', {
        get: () => [
          { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
          { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
          { name: 'Native Client', filename: 'internal-nacl-plugin' },
        ],
      });

      // Fake languages (headless may lack this)
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

      // Fake WebGL vendor/renderer (headless gives "Google SwiftShader")
      const getParameter = WebGLRenderingContext.prototype.getParameter;
      WebGLRenderingContext.prototype.getParameter = function (param) {
        if (param === 37445) return 'Intel Inc.';      // UNMASKED_VENDOR_WEBGL
        if (param === 37446) return 'Intel Iris OpenGL Engine'; // UNMASKED_RENDERER_WEBGL
        return getParameter.call(this, param);
      };
    });

    // Set a realistic user agent
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    );
  }

  async _closeBrowser() {
    if (this._browser) {
      await this._browser.close().catch(() => {});
      this._browser = null;
    }
  }

  // ─── Geo-grid helpers ────────────────────────────────────────────

  /**
   * Fetch bounding box for a city from Nominatim (free, 1 req/s).
   * Returns { south, north, west, east } or null.
   */
  async _getNominatimBounds(cityName, countryCode) {
    const key = `${cityName}|${countryCode}`;
    if (_boundsCache.has(key)) return _boundsCache.get(key);

    // Map project country codes to ISO 3166-1 alpha-2 for Nominatim
    const isoMap = { UK: 'GB' };
    const isoCode = isoMap[countryCode] || countryCode;

    const query = isoCode
      ? `${cityName}, ${isoCode}`
      : cityName;
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=1`;

    try {
      const data = await new Promise((resolve, reject) => {
        const req = https.get(url, {
          headers: { 'User-Agent': 'MortarLeadScraper/1.0 (contact@mortarmetrics.com)' },
          timeout: 10000,
        }, (res) => {
          let body = '';
          res.on('data', c => body += c);
          res.on('end', () => {
            try { resolve(JSON.parse(body)); }
            catch { reject(new Error('Invalid JSON from Nominatim')); }
          });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Nominatim timeout')); });
      });

      if (!data || !data[0] || !data[0].boundingbox) {
        _boundsCacheSet(key, null);
        return null;
      }

      // Nominatim boundingbox = [south, north, west, east] as strings
      const [south, north, west, east] = data[0].boundingbox.map(Number);
      const bounds = { south, north, west, east };
      _boundsCacheSet(key, bounds);
      return bounds;
    } catch (err) {
      log.warn(`[Google Maps] Nominatim lookup failed for "${cityName}": ${err.message}`);
      // Don't cache errors — allow retry on next call
      return null;
    }
  }

  /**
   * Subdivide a bounding box into grid cells of approximately cellSizeKm.
   * Returns array of { lat, lng } center points.
   */
  _generateGridCells(bounds, cellSizeKm = 2) {
    const { south, north, west, east } = bounds;

    // 1 degree latitude ≈ 111 km
    const latStep = cellSizeKm / 111;

    // 1 degree longitude ≈ 111 * cos(midLat) km
    const midLat = (south + north) / 2;
    const lngStep = cellSizeKm / (111 * Math.cos(midLat * Math.PI / 180));

    const cells = [];
    for (let lat = south + latStep / 2; lat < north; lat += latStep) {
      for (let lng = west + lngStep / 2; lng < east; lng += lngStep) {
        cells.push({
          lat: Math.round(lat * 1000000) / 1000000,
          lng: Math.round(lng * 1000000) / 1000000,
        });
      }
    }

    return cells;
  }

  /**
   * Evenly sample n items from an array (preserves spatial distribution).
   */
  _sampleEvenly(arr, n) {
    if (n >= arr.length) return arr;
    const step = arr.length / n;
    const result = [];
    for (let i = 0; i < n; i++) {
      result.push(arr[Math.floor(i * step)]);
    }
    return result;
  }

  // ─── Main search ────────────────────────────────────────────────

  /**
   * Search Google Maps for businesses across cities using geo-grid splitting.
   * Each city is subdivided into ~2km grid cells for comprehensive coverage.
   */
  async *search(practiceArea, options = {}) {
    const niche = (options.niche || 'lawyers').trim();
    const rateLimiter = new RateLimiter({ minDelay: 5000, maxDelay: 10000 });
    const maxCities = options.maxCities || null;
    const isTestMode = !!(options.maxPages);

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

        // Dedup set for this city (prevents same business appearing from multiple grid cells)
        const seenInCity = new Set();

        // Get bounding box for geo-grid
        const bounds = await this._getNominatimBounds(cityEntry.city, cityEntry.country);
        await sleep(1100); // Nominatim rate limit: 1 req/s

        if (bounds) {
          const allCells = this._generateGridCells(bounds, 2);
          // In test mode: 4 cells. Production: cap at 80 cells (evenly spaced sample for huge cities).
          const maxGridCells = isTestMode ? Math.min(4, allCells.length) : Math.min(80, allCells.length);
          const cells = allCells.length <= maxGridCells
            ? allCells
            : this._sampleEvenly(allCells, maxGridCells);

          log.info(`[Google Maps] City "${cityEntry.city}": ${allCells.length} grid cells (using ${cells.length})`);

          for (let g = 0; g < cells.length; g++) {
            const cell = cells[g];
            const query = `${niche} in ${cityEntry.city}`;
            const cellLabel = `cell ${g + 1}/${cells.length}`;

            log.info(`[Google Maps] ${cellLabel}: @${cell.lat},${cell.lng}`);
            await rateLimiter.wait();

            try {
              const results = await this._scrapeMapResults(query, options.maxPages, cell);
              let newCount = 0;

              for (const result of results) {
                // Dedup by normalized business name within this city
                const dedupKey = result.name.toLowerCase().replace(/[^a-z0-9]/g, '');
                if (seenInCity.has(dedupKey)) continue;
                seenInCity.add(dedupKey);

                const lead = this._parseMapResult(result, cityEntry, niche);
                if (lead) {
                  yield lead;
                  newCount++;
                }
              }

              log.info(`[Google Maps] ${cellLabel}: ${results.length} results, ${newCount} new (${seenInCity.size} total unique)`);
            } catch (err) {
              log.warn(`[Google Maps] ${cellLabel} failed: ${err.message}`);
            }
          }

          log.info(`[Google Maps] City "${cityEntry.city}" complete: ${seenInCity.size} unique businesses`);
        } else {
          // Fallback: no bounds available, use simple city search (no grid)
          const query = `${niche} in ${cityEntry.city}`;
          log.info(`[Google Maps] Searching (no grid): "${query}"`);
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
      }
    } finally {
      await this._closeBrowser();
    }
  }

  /**
   * Scrape Google Maps search results page.
   * @param {string} query - Search query like "dentists in Miami"
   * @param {number} [maxScrolls] - Max scroll iterations (null = scroll until no more)
   * @param {{ lat: number, lng: number }} [coords] - Optional center coordinates for geo-grid
   * @returns {object[]} Array of business data
   */
  async _scrapeMapResults(query, maxScrolls, coords) {
    const page = await this._browser.newPage();

    try {
      // Anti-detection patches (before any navigation)
      await this._applyAntiDetection(page);

      // Block images and tiles for speed
      await page.setRequestInterception(true);
      page.on('request', (req) => {
        try {
          const type = req.resourceType();
          if (['image', 'media', 'font'].includes(type)) {
            req.abort();
          } else {
            req.continue();
          }
        } catch { /* request already handled */ }
      });

      await page.setViewport({ width: 1280, height: 900 });

      // Navigate to Google Maps search — with optional geo-grid coordinates
      // Zoom 15 ≈ ~3km visible, good for 2km grid cells
      let url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;
      if (coords) {
        url += `/@${coords.lat},${coords.lng},15z`;
      }
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

      // Wait for results feed
      await page.waitForSelector('div[role="feed"]', { timeout: 15000 }).catch(() => {});

      // Check for consent dialog and dismiss
      await this._dismissConsent(page);

      // Scroll the results feed to load more results
      // Geo-grid cells need fewer scrolls (smaller area = fewer results)
      const defaultScrolls = coords ? 3 : 6;
      const scrollLimit = maxScrolls ? Math.min(maxScrolls, defaultScrolls) : defaultScrolls;
      await this._scrollFeed(page, scrollLimit);

      // Extract basic info from feed cards (name, rating, category, address snippet)
      const feedResults = await this._extractFeedCards(page);

      // For each result, click to get phone + website from detail panel
      // Cap per-cell at 30 for grid (speed), or 60 for single query
      const detailedResults = [];
      const maxDetails = Math.min(feedResults.length, coords ? 30 : 60);

      for (let i = 0; i < maxDetails; i++) {
        const feedItem = feedResults[i];
        try {
          const detail = await this._extractDetailByClick(page, i, feedItem.name);
          detailedResults.push({
            name: feedItem.name,
            mapsUrl: feedItem.href || '',
            rating: feedItem.rating || '',
            category: feedItem.category || '',
            address: detail.address || feedItem.addressSnippet || '',
            phone: detail.phone || '',
            website: detail.website || '',
          });
        } catch {
          // If detail extraction fails, still capture basic info from feed
          detailedResults.push({
            name: feedItem.name,
            mapsUrl: feedItem.href || '',
            rating: feedItem.rating || '',
            category: feedItem.category || '',
            address: feedItem.addressSnippet || '',
            phone: '',
            website: '',
          });
        }
      }

      return detailedResults;
    } finally {
      await page.close().catch(() => {});
    }
  }

  /**
   * Extract business data from feed cards WITHOUT clicking.
   * Each card in the feed has name, rating, category, and address snippet.
   */
  async _extractFeedCards(page) {
    return page.evaluate(() => {
      const items = [];
      const feed = document.querySelector('div[role="feed"]');
      if (!feed) return items;

      // Each result card is an <a> with aria-label inside the feed
      const links = feed.querySelectorAll('a[aria-label]');

      for (const link of links) {
        const ariaLabel = (link.getAttribute('aria-label') || '').trim();
        if (!ariaLabel) continue;

        // Filter out navigation/action links that aren't business names
        // These typically start with "Visit", "Directions to", "Search nearby", etc.
        if (/^(Visit |Directions |Search |Share |Save |Suggest |Send |Identify |Claim )/i.test(ariaLabel)) {
          continue;
        }
        // Skip very long aria-labels (likely descriptions, not names)
        if (ariaLabel.length > 100) continue;
        // Skip single-word names (likely UI elements)
        if (!/\s/.test(ariaLabel) && ariaLabel.length < 20) continue;

        const href = link.getAttribute('href') || '';

        // Extract rating from within the card
        let rating = '';
        const container = link.closest('div') || link;
        const ratingEl = container.querySelector('span[role="img"]');
        if (ratingEl) {
          const ratingLabel = ratingEl.getAttribute('aria-label') || '';
          const match = ratingLabel.match(/([\d.]+)\s*star/i);
          if (match) rating = match[1];
        }

        // Extract text content from the card for category and address
        let category = '';
        let addressSnippet = '';
        const cardParent = link.closest('div');
        if (cardParent) {
          const textNodes = cardParent.querySelectorAll('span, div');
          const texts = [];
          for (const node of textNodes) {
            const t = node.textContent.trim();
            if (t && t !== ariaLabel && t.length < 100 && t.length > 2) {
              texts.push(t);
            }
          }
          // Usually: category is first short text, address has numbers/commas
          for (const t of texts) {
            if (!category && !t.match(/[\d,]/) && t.length < 40) {
              category = t;
            }
            if (!addressSnippet && /\d/.test(t) && (t.includes(',') || t.includes(' '))) {
              addressSnippet = t;
            }
          }
        }

        items.push({ name: ariaLabel, href, rating, category, addressSnippet });
      }

      return items;
    });
  }

  /**
   * Click a specific result in the feed and extract phone/website/address
   * from the detail panel. Waits for the panel header to match the expected name.
   */
  async _extractDetailByClick(page, index, expectedName) {
    // Click the result
    const clicked = await page.evaluate((idx) => {
      const feed = document.querySelector('div[role="feed"]');
      if (!feed) return false;

      // Get only business-name links (filter same as _extractFeedCards)
      const allLinks = feed.querySelectorAll('a[aria-label]');
      const businessLinks = [];
      for (const link of allLinks) {
        const label = (link.getAttribute('aria-label') || '').trim();
        if (!label) continue;
        if (/^(Visit |Directions |Search |Share |Save |Suggest |Send |Identify |Claim )/i.test(label)) continue;
        if (label.length > 100) continue;
        if (!/\s/.test(label) && label.length < 20) continue;
        businessLinks.push(link);
      }

      if (idx >= businessLinks.length) return false;
      businessLinks[idx].click();
      return true;
    }, index);

    if (!clicked) return { phone: '', website: '', address: '' };

    // Wait for the detail panel to load with the correct business
    // Poll for the h1 to match expected name (most load in 1-2s)
    const maxWait = 3000;
    const pollInterval = 250;
    let waited = 0;

    while (waited < maxWait) {
      await sleep(pollInterval);
      waited += pollInterval;

      const h1Text = await page.evaluate(() => {
        const h1 = document.querySelector('h1');
        return h1 ? h1.textContent.trim() : '';
      });

      // The panel has loaded with the right business
      if (h1Text && expectedName && this._namesMatch(h1Text, expectedName)) {
        break;
      }

      // If h1 is present and non-empty but doesn't match after 1.5s, accept it
      // (Google may truncate/abbreviate the name)
      if (h1Text && waited >= 1500) break;
    }

    // Extract detail data — scoped to the visible detail panel
    const detail = await page.evaluate(() => {
      const result = { phone: '', website: '', address: '' };

      // Phone: look for tel: links or phone button with aria-label
      // The detail panel has buttons with data-tooltip or aria-label containing the phone
      const allButtons = document.querySelectorAll('button[aria-label], a[aria-label]');
      for (const btn of allButtons) {
        const label = (btn.getAttribute('aria-label') || '').toLowerCase();
        const dataTooltip = (btn.getAttribute('data-tooltip') || '').toLowerCase();

        // Phone button
        if (label.includes('phone:') || dataTooltip === 'copy phone number') {
          // aria-label format: "Phone: (305) 555-1234"
          const phoneMatch = (btn.getAttribute('aria-label') || '').match(/phone:\s*([\d()+\- .]+)/i);
          if (phoneMatch) {
            result.phone = phoneMatch[1].trim();
          } else {
            // Try extracting from button's visible text
            const text = btn.textContent || '';
            const pm = text.match(/[\d()+\- .]{7,}/);
            if (pm) result.phone = pm[0].trim();
          }
        }

        // Website button
        if (label.includes('website:') || dataTooltip === 'open website') {
          // aria-label format: "Website: www.example.com"
          const urlMatch = (btn.getAttribute('aria-label') || '').match(/website:\s*(.+)/i);
          if (urlMatch) {
            let url = urlMatch[1].trim();
            if (url && !/^https?:\/\//i.test(url)) url = 'https://' + url;
            result.website = url;
          } else if (btn.href && !btn.href.includes('google.com')) {
            result.website = btn.href;
          }
        }

        // Address button
        if (label.includes('address:') || dataTooltip === 'copy address') {
          const addrMatch = (btn.getAttribute('aria-label') || '').match(/address:\s*(.+)/i);
          if (addrMatch) {
            result.address = addrMatch[1].trim();
          }
        }
      }

      // Fallback: tel: links
      if (!result.phone) {
        const telLinks = document.querySelectorAll('a[href^="tel:"]');
        if (telLinks.length > 0) {
          result.phone = telLinks[0].href.replace('tel:', '').replace(/%20/g, '');
        }
      }

      // Fallback: external website links (not google/social media)
      if (!result.website) {
        const extLinks = document.querySelectorAll('a[data-tooltip="Open website"]');
        if (extLinks.length > 0 && extLinks[0].href) {
          result.website = extLinks[0].href;
        }
      }

      return result;
    });

    return detail;
  }

  /**
   * Check if two business names match (allows for truncation/abbreviation).
   */
  _namesMatch(name1, name2) {
    const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
    const n1 = normalize(name1);
    const n2 = normalize(name2);
    // Exact match or one starts with the other
    return n1 === n2 || n1.startsWith(n2) || n2.startsWith(n1) ||
      // First 10 chars match (handles truncation)
      (n1.length >= 10 && n2.length >= 10 && n1.slice(0, 10) === n2.slice(0, 10));
  }

  /**
   * Dismiss Google consent/cookie dialog if present.
   */
  async _dismissConsent(page) {
    try {
      const consentSelectors = [
        'button[aria-label="Accept all"]',
        'button[aria-label="Reject all"]',
        'form[action*="consent"] button',
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
    const scrollStart = Date.now();
    const maxScrollTime = 30000; // 30s safety timeout for entire scroll sequence
    for (let i = 0; i < maxScrolls; i++) {
      if (Date.now() - scrollStart > maxScrollTime) break;
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
