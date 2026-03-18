/**
 * Houzz Professional Directory Scraper
 *
 * Source: https://www.houzz.com/professionals/
 * Records: 2M+ professional profiles (contractors, designers, architects, etc.)
 * Method: Puppeteer (Houzz is fully client-side rendered — no server-side HTML with listing data)
 *
 * URL pattern: https://www.houzz.com/professionals/{category}/c/{City}--{State}
 * Pagination: ?fi={offset} query parameter (15 results per page)
 *
 * Available fields:
 *   Business Name, Location (City, State), Phone, Website, Rating, Review Count
 *   Profile URL (individual profile pages may contain email)
 *
 * Categories:
 *   general-contractor, roofing-and-gutters, painting, plumbing,
 *   heating-and-cooling, electricians, landscape-contractors
 *
 * The page renders listing cards with business info via React/styled-components.
 * Puppeteer is required to execute JS and extract the rendered DOM content.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Map of friendly practice area names to Houzz URL slugs
const PRACTICE_AREA_CODES = {
  'general contractor': 'general-contractor',
  'general': 'general-contractor',
  'roofing': 'roofing-and-gutters',
  'gutters': 'roofing-and-gutters',
  'painting': 'painting',
  'plumbing': 'plumbing',
  'hvac': 'heating-and-cooling',
  'heating': 'heating-and-cooling',
  'cooling': 'heating-and-cooling',
  'air conditioning': 'heating-and-cooling',
  'electrical': 'electricians',
  'electrician': 'electricians',
  'landscaping': 'landscape-contractors',
  'landscape': 'landscape-contractors',
};

// Default cities to search (major US metros formatted for Houzz URL pattern)
const DEFAULT_CITIES = [
  'Houston--TX',
  'Miami--FL',
  'Dallas--TX',
  'Los-Angeles--CA',
  'Chicago--IL',
  'Phoenix--AZ',
  'Atlanta--GA',
  'Denver--CO',
  'Seattle--WA',
  'Tampa--FL',
];

// Number of results Houzz loads per page
const PAGE_SIZE = 15;

class HouzzScraper extends BaseScraper {
  constructor() {
    super({
      name: 'houzz-contractors',
      stateCode: 'HOUZZ',
      baseUrl: 'https://www.houzz.com',
      pageSize: PAGE_SIZE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('houzz: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('houzz: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('houzz: extractResultCount() not used — search() overridden'); }

  /**
   * Build the Houzz professional directory URL for a category + city.
   *
   * @param {string} categorySlug - URL slug for the category (e.g., 'general-contractor')
   * @param {string} citySlug - City slug (e.g., 'Houston--TX')
   * @param {number} offset - Pagination offset (0, 15, 30, ...)
   * @returns {string} Full URL
   */
  _buildUrl(categorySlug, citySlug, offset = 0) {
    const base = `https://www.houzz.com/professionals/${categorySlug}/c/${citySlug}`;
    return offset > 0 ? `${base}?fi=${offset}` : base;
  }

  /**
   * Parse city and state from a Houzz city slug.
   * 'Houston--TX' -> { city: 'Houston', state: 'TX' }
   * 'Los-Angeles--CA' -> { city: 'Los Angeles', state: 'CA' }
   */
  _parseCitySlug(slug) {
    const parts = slug.split('--');
    if (parts.length === 2) {
      const city = parts[0].replace(/-/g, ' ');
      const state = parts[1].toUpperCase();
      return { city, state };
    }
    return { city: slug, state: '' };
  }

  /**
   * Extract professional listings from the Puppeteer page DOM.
   * Tries multiple selector strategies to handle Houzz's evolving markup.
   */
  async _extractListings(page) {
    return page.evaluate(() => {
      const results = [];

      // Strategy 1: Look for search result cards by known class patterns
      // Houzz uses styled-components with generated class names, but the
      // structural patterns remain: card containers with business info inside.
      const cards = document.querySelectorAll(
        '[data-testid="pro-search-result"], ' +
        '.hz-pro-search-result, ' +
        '[class*="ProResult"], ' +
        '[class*="proResult"], ' +
        '[class*="SearchResult"], ' +
        'article[class*="pro"], ' +
        'div[class*="pro-search-result"]'
      );

      if (cards.length > 0) {
        cards.forEach(card => {
          const result = extractFromCard(card);
          if (result) results.push(result);
        });
      }

      // Strategy 2: If no cards found, try broader selectors
      if (results.length === 0) {
        // Look for links to professional profiles
        const proLinks = document.querySelectorAll('a[href*="/professionals/"][href*="pfvw"]');
        const seen = new Set();

        proLinks.forEach(link => {
          const href = link.getAttribute('href') || '';
          if (seen.has(href)) return;
          seen.add(href);

          // Walk up to find the containing card
          const container = link.closest('div[class]') ||
                           link.closest('article') ||
                           link.closest('li') ||
                           link.parentElement?.parentElement;

          if (container) {
            const result = extractFromCard(container);
            if (result) {
              result.profileUrl = result.profileUrl || href;
              results.push(result);
            }
          }
        });
      }

      // Strategy 3: Even broader — find all elements with business-like content
      if (results.length === 0) {
        // Look for star rating elements and walk up to their container
        const stars = document.querySelectorAll(
          '[class*="star-rate"], [class*="StarRate"], [class*="rating"], [aria-label*="star"]'
        );

        const containers = new Set();
        stars.forEach(star => {
          let el = star;
          for (let i = 0; i < 5; i++) {
            el = el.parentElement;
            if (!el) break;
          }
          if (el && !containers.has(el)) {
            containers.add(el);
            const result = extractFromCard(el);
            if (result) results.push(result);
          }
        });
      }

      function extractFromCard(card) {
        const text = card.textContent || '';

        // Find business name — usually the first prominent link or heading
        let name = '';
        const nameEl = card.querySelector('h2 a, h3 a, [class*="Name"] a, [class*="name"] a, a[class*="title"]');
        if (nameEl) {
          name = nameEl.textContent.trim();
        } else {
          // Try heading elements
          const heading = card.querySelector('h2, h3');
          if (heading) name = heading.textContent.trim();
        }

        // Find profile URL
        let profileUrl = '';
        const profileLink = card.querySelector('a[href*="/professionals/"], a[href*="/pro/"], a[href*="pfvw"]');
        if (profileLink) {
          profileUrl = profileLink.getAttribute('href') || '';
          if (profileUrl.startsWith('/')) {
            profileUrl = 'https://www.houzz.com' + profileUrl;
          }
          // Extract name from link text if not found above
          if (!name) name = profileLink.textContent.trim();
        }

        // Skip if no name found
        if (!name || name.length < 2) return null;
        // Skip navigation/filter links
        if (name.toLowerCase().includes('filter') || name.toLowerCase().includes('sort')) return null;

        // Find phone number
        let phone = '';
        const phoneEl = card.querySelector('a[href^="tel:"], [class*="phone"], [class*="Phone"]');
        if (phoneEl) {
          phone = phoneEl.getAttribute('href')?.replace('tel:', '') || phoneEl.textContent.trim();
        }
        // Regex fallback for phone in text
        if (!phone) {
          const phoneMatch = text.match(/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/);
          if (phoneMatch) phone = phoneMatch[0];
        }

        // Find website
        let website = '';
        const websiteEl = card.querySelector(
          'a[class*="website"], a[class*="Website"], ' +
          'a[href*="redirect"], a[rel="nofollow noopener"]'
        );
        if (websiteEl) {
          const href = websiteEl.getAttribute('href') || '';
          if (href && !href.includes('houzz.com')) {
            website = href;
          }
        }

        // Find location text
        let location = '';
        const locEl = card.querySelector(
          '[class*="location"], [class*="Location"], ' +
          '[class*="address"], [class*="Address"], ' +
          'address'
        );
        if (locEl) {
          location = locEl.textContent.trim();
        }

        // Parse "City, ST" from location
        let city = '', state = '';
        if (location) {
          const match = location.match(/([A-Za-z\s.]+),\s*([A-Z]{2})/);
          if (match) {
            city = match[1].trim();
            state = match[2];
          } else {
            city = location;
          }
        }

        // Find rating
        let rating = '';
        const ratingEl = card.querySelector(
          '[class*="rating"], [class*="Rating"], [aria-label*="star"], [aria-label*="rating"]'
        );
        if (ratingEl) {
          const ariaLabel = ratingEl.getAttribute('aria-label') || '';
          const ratingMatch = ariaLabel.match(/([\d.]+)/);
          if (ratingMatch) rating = ratingMatch[1];
        }

        // Find review count
        let reviewCount = '';
        const reviewEl = card.querySelector(
          '[class*="review"], [class*="Review"]'
        );
        if (reviewEl) {
          const countMatch = reviewEl.textContent.match(/(\d+)\s*(reviews?|ratings?)/i);
          if (countMatch) reviewCount = countMatch[1];
        }

        return {
          name: name.trim(),
          phone: phone.replace(/[^\d+()-.\s]/g, '').trim(),
          website,
          city,
          state,
          profileUrl,
          rating,
          reviewCount,
          location: location.trim(),
        };
      }

      return results;
    });
  }

  /**
   * Attempt to extract additional data (email, phone, website) from a
   * professional's profile page.
   */
  async _scrapeProfilePage(page, profileUrl) {
    try {
      await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: 20000 });
      await page.waitForSelector('body', { timeout: 8000 });

      // Check for Cloudflare / bot challenge
      const isChallenge = await page.evaluate(() =>
        document.title.includes('Just a moment') ||
        document.title.includes('Attention Required') ||
        !!document.querySelector('#challenge-running')
      );
      if (isChallenge) return {};

      return page.evaluate(() => {
        const data = {};

        // JSON-LD structured data
        const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of ldScripts) {
          try {
            const json = JSON.parse(script.textContent);
            const biz = Array.isArray(json) ? json[0] : json;
            if (biz.telephone) data.phone = biz.telephone;
            if (biz.email) data.email = biz.email;
            if (biz.url && !biz.url.includes('houzz.com')) data.website = biz.url;
            if (biz.address) {
              if (biz.address.addressLocality) data.city = biz.address.addressLocality;
              if (biz.address.addressRegion) data.state = biz.address.addressRegion;
            }
          } catch (e) { /* ignore parse errors */ }
        }

        // Look for contact info in the page
        const bodyText = document.body?.textContent || '';

        // Email regex
        if (!data.email) {
          const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
          if (emailMatch) {
            const email = emailMatch[0].toLowerCase();
            if (!email.includes('houzz.com') && !email.includes('example.com')) {
              data.email = email;
            }
          }
        }

        // Phone from tel: links
        if (!data.phone) {
          const telLink = document.querySelector('a[href^="tel:"]');
          if (telLink) {
            data.phone = telLink.getAttribute('href').replace('tel:', '').trim();
          }
        }

        // Website link (non-Houzz external links)
        if (!data.website) {
          const extLinks = document.querySelectorAll('a[rel*="nofollow"], a[target="_blank"]');
          for (const link of extLinks) {
            const href = link.getAttribute('href') || '';
            if (href && !href.includes('houzz.com') && !href.includes('google.com') &&
                !href.includes('facebook.com') && !href.includes('instagram.com') &&
                !href.includes('twitter.com') && !href.includes('pinterest.com') &&
                !href.includes('youtube.com') && !href.includes('yelp.com') &&
                (href.startsWith('http://') || href.startsWith('https://'))) {
              data.website = href;
              break;
            }
          }
        }

        return data;
      });
    } catch (err) {
      return {};
    }
  }

  /**
   * Main search generator. Uses Puppeteer to scrape Houzz's JS-rendered pages.
   *
   * @param {string} practiceArea - Category name (e.g., 'general contractor', 'plumbing')
   * @param {object} options - Search options
   * @param {string} options.city - Specific city slug (e.g., 'Houston--TX')
   * @param {number} options.maxPages - Limit pages per city (default 5, test mode uses 2)
   * @param {number} options.maxCities - Limit number of cities
   */
  async *search(practiceArea, options = {}) {
    // Resolve category slug
    let categorySlug = practiceArea
      ? (this.practiceAreaCodes[practiceArea.toLowerCase()] || practiceArea.toLowerCase().replace(/\s+/g, '-'))
      : 'general-contractor';

    log.scrape(`[Houzz] Starting search: category=${categorySlug}, practiceArea=${practiceArea || 'general contractor'}`);

    // Launch Puppeteer
    let browser;
    try {
      const puppeteer = require('puppeteer');
      const launchOptions = {
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
        ],
      };
      if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
      }
      browser = await puppeteer.launch(launchOptions);
    } catch (err) {
      log.error(`[Houzz] Failed to launch Puppeteer: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: 'Puppeteer not available — Houzz requires browser rendering' };
      return;
    }

    try {
      const page = await browser.newPage();

      // Set a realistic user agent
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );

      // Block images/fonts/media to speed up scraping
      await page.setRequestInterception(true);
      page.on('request', req => {
        const resourceType = req.resourceType();
        if (['image', 'font', 'media', 'stylesheet'].includes(resourceType)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      // Determine cities to search
      let cities = [...DEFAULT_CITIES];
      if (options.city) {
        // Accept either 'Houston--TX' or 'Houston' format
        if (options.city.includes('--')) {
          cities = [options.city];
        } else {
          // Try to match a default city
          const match = DEFAULT_CITIES.find(c =>
            c.toLowerCase().replace(/-/g, ' ').startsWith(options.city.toLowerCase())
          );
          cities = match ? [match] : [options.city];
        }
      }
      if (options.maxCities) cities = cities.slice(0, options.maxCities);

      const maxPagesPerCity = options.maxPages || 5;
      let totalYielded = 0;

      for (let ci = 0; ci < cities.length; ci++) {
        const citySlug = cities[ci];
        const { city: parsedCity, state: parsedState } = this._parseCitySlug(citySlug);

        yield { _cityProgress: { current: ci + 1, total: cities.length } };
        log.scrape(`[Houzz] Searching ${categorySlug} in ${parsedCity}, ${parsedState}`);

        let offset = 0;
        let pageNum = 0;
        let consecutiveEmpty = 0;

        while (pageNum < maxPagesPerCity) {
          const url = this._buildUrl(categorySlug, citySlug, offset);
          log.info(`[Houzz] Page ${pageNum + 1} — ${url}`);

          try {
            // Navigate to the listing page
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

            // Wait for content to render
            await page.waitForSelector('body', { timeout: 10000 });

            // Additional wait for dynamic content
            await new Promise(r => setTimeout(r, 2000));

            // Check for bot challenge
            const isChallenge = await page.evaluate(() =>
              document.title.includes('Just a moment') ||
              document.title.includes('Attention Required') ||
              !!document.querySelector('#challenge-running')
            );

            if (isChallenge) {
              log.warn(`[Houzz] Bot challenge detected — waiting 8s then retrying...`);
              await new Promise(r => setTimeout(r, 8000));
              const stillBlocked = await page.evaluate(() =>
                document.title.includes('Just a moment') ||
                document.title.includes('Attention Required')
              );
              if (stillBlocked) {
                log.warn(`[Houzz] Cannot bypass challenge for ${parsedCity} — skipping city`);
                yield { _captcha: true, city: parsedCity, reason: 'Bot detection / challenge page' };
                break;
              }
            }

            // Extract listings from the page
            const listings = await this._extractListings(page);

            if (listings.length === 0) {
              consecutiveEmpty++;
              if (consecutiveEmpty >= 2) {
                log.info(`[Houzz] 2 consecutive empty pages for ${parsedCity} — moving on`);
                break;
              }
              pageNum++;
              offset += PAGE_SIZE;
              continue;
            }

            consecutiveEmpty = 0;
            log.info(`[Houzz] Found ${listings.length} listings on page ${pageNum + 1} for ${parsedCity}`);

            // Yield each listing as a lead
            for (const listing of listings) {
              const lead = {
                first_name: '',
                last_name: '',
                firm_name: listing.name || '',
                city: listing.city || parsedCity,
                state: listing.state || parsedState,
                phone: listing.phone || '',
                email: '',
                website: listing.website || '',
                bar_number: '',
                bar_status: '',
                admission_date: '',
                source: 'houzz_contractors',
                profile_url: listing.profileUrl || '',
                practice_area: practiceArea || categorySlug.replace(/-/g, ' '),
              };

              yield this.transformResult(lead, practiceArea || categorySlug.replace(/-/g, ' '));
              totalYielded++;
            }

            pageNum++;
            offset += PAGE_SIZE;

            // Polite delay between pages (3-6 seconds)
            await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));

          } catch (err) {
            log.warn(`[Houzz] Error on page ${pageNum + 1} for ${parsedCity}: ${err.message}`);
            break;
          }
        }

        // Polite delay between cities (5-10 seconds)
        if (ci < cities.length - 1) {
          await new Promise(r => setTimeout(r, 5000 + Math.random() * 5000));
        }
      }

      log.success(`[Houzz] Finished — yielded ${totalYielded} professionals across ${cities.length} cities`);

    } finally {
      if (browser) await browser.close();
    }
  }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'houzz_contractors';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }
}

module.exports = new HouzzScraper();
