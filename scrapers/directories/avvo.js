/**
 * Avvo.com Lawyer Directory Scraper
 *
 * Source: https://www.avvo.com/
 * Method: Puppeteer (Cloudflare blocks curl) + JSON-LD structured data per card
 * Data:   Individual lawyer profiles with name, phone, address, firm, education
 *
 * URL Patterns:
 *   All lawyers:   /all-lawyers/{state}/{city}.html?page={n}
 *   By practice:   /{practice-slug}/{state}/{city}.html?page={n}
 *   Profile:       /attorneys/{zip}-{state}-{name}-{id}.html
 *
 * 20 organic results per page, paginated via ?page=N.
 * Each card contains JSON-LD with Person schema including phone, address, firm.
 *
 * Valuable for: phone numbers, firm data, profile enrichment.
 * Great cross-reference source for bar-scraped leads missing phone/website.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

/**
 * Major US cities for default scraping.
 */
const DEFAULT_CITY_ENTRIES = [
  { city: 'New York', stateCode: 'NY', slug: 'new_york' },
  { city: 'Los Angeles', stateCode: 'CA', slug: 'los_angeles' },
  { city: 'Chicago', stateCode: 'IL', slug: 'chicago' },
  { city: 'Houston', stateCode: 'TX', slug: 'houston' },
  { city: 'Phoenix', stateCode: 'AZ', slug: 'phoenix' },
  { city: 'Philadelphia', stateCode: 'PA', slug: 'philadelphia' },
  { city: 'San Antonio', stateCode: 'TX', slug: 'san_antonio' },
  { city: 'San Diego', stateCode: 'CA', slug: 'san_diego' },
  { city: 'Dallas', stateCode: 'TX', slug: 'dallas' },
  { city: 'Miami', stateCode: 'FL', slug: 'miami' },
  { city: 'Atlanta', stateCode: 'GA', slug: 'atlanta' },
  { city: 'Boston', stateCode: 'MA', slug: 'boston' },
  { city: 'Denver', stateCode: 'CO', slug: 'denver' },
  { city: 'Seattle', stateCode: 'WA', slug: 'seattle' },
  { city: 'Nashville', stateCode: 'TN', slug: 'nashville' },
  { city: 'Charlotte', stateCode: 'NC', slug: 'charlotte' },
  { city: 'San Francisco', stateCode: 'CA', slug: 'san_francisco' },
  { city: 'Portland', stateCode: 'OR', slug: 'portland' },
  { city: 'Las Vegas', stateCode: 'NV', slug: 'las_vegas' },
  { city: 'Minneapolis', stateCode: 'MN', slug: 'minneapolis' },
];

/**
 * State code to lowercase abbreviation mapping.
 */
const STATE_CODES = {
  AL: 'al', AK: 'ak', AZ: 'az', AR: 'ar', CA: 'ca', CO: 'co', CT: 'ct',
  DE: 'de', DC: 'dc', FL: 'fl', GA: 'ga', HI: 'hi', ID: 'id', IL: 'il',
  IN: 'in', IA: 'ia', KS: 'ks', KY: 'ky', LA: 'la', ME: 'me', MD: 'md',
  MA: 'ma', MI: 'mi', MN: 'mn', MS: 'ms', MO: 'mo', MT: 'mt', NE: 'ne',
  NV: 'nv', NH: 'nh', NJ: 'nj', NM: 'nm', NY: 'ny', NC: 'nc', ND: 'nd',
  OH: 'oh', OK: 'ok', OR: 'or', PA: 'pa', RI: 'ri', SC: 'sc', SD: 'sd',
  TN: 'tn', TX: 'tx', UT: 'ut', VT: 'vt', VA: 'va', WA: 'wa', WV: 'wv',
  WI: 'wi', WY: 'wy',
};

class AvvoScraper extends BaseScraper {
  constructor() {
    super({
      name: 'avvo',
      stateCode: 'AVVO',
      baseUrl: 'https://www.avvo.com',
      pageSize: 20,
      practiceAreaCodes: {},
      defaultCities: DEFAULT_CITY_ENTRIES.map(e => e.city),
    });
    this._cityEntries = DEFAULT_CITY_ENTRIES;
  }

  /**
   * Convert city name to Avvo URL slug.
   * "Los Angeles" → "los_angeles", "San Francisco" → "san_francisco"
   */
  _citySlug(cityName) {
    return cityName.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
  }

  /**
   * Find state code for a city from our default entries.
   */
  _stateForCity(cityName) {
    const entry = this._cityEntries.find(
      e => e.city.toLowerCase() === cityName.toLowerCase()
    );
    return entry ? entry.stateCode : null;
  }

  /**
   * Parse a Person JSON-LD object from an Avvo card into a lead.
   */
  _parseJsonLd(jsonLd, cityName, stateCode) {
    if (!jsonLd || jsonLd['@type'] !== 'Person') return null;

    const nameParts = (jsonLd.name || '').split(' ');
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ') || '';

    const worksFor = jsonLd.worksFor || {};
    const address = worksFor.address || {};

    // Extract phone — remove parentheses/dashes
    const rawPhone = worksFor.telephone || '';

    // Profile URL from @id
    const profileUrl = jsonLd['@id'] || jsonLd.url || '';

    // Education
    const education = (jsonLd.alumniOf || [])
      .map(a => a.name || '')
      .filter(Boolean)
      .join('; ');

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: worksFor.name || '',
      city: address.addressLocality || cityName,
      state: address.addressRegion || stateCode,
      phone: rawPhone,
      website: '', // Avvo doesn't include firm website in JSON-LD
      email: '', // No email in Avvo listings
      bar_number: '',
      bar_status: 'Active',
      source: 'avvo',
      profile_url: profileUrl,
      education,
      zip_code: address.postalCode || '',
      street_address: address.streetAddress || '',
    };
  }

  /**
   * Scrape a single page of Avvo results using Puppeteer.
   * Returns { leads, hasNextPage, totalResults }.
   */
  async _scrapePage(browser, url, cityName, stateCode, rateLimiter) {
    const page = await browser.newPage();
    await page.setUserAgent(rateLimiter.getUserAgent());
    await page.setDefaultTimeout(30000);

    const leads = [];
    let hasNextPage = false;
    let totalResults = 0;

    try {
      await rateLimiter.wait();
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

      const data = await page.evaluate(() => {
        const result = { jsonLds: [], hasNext: false, total: 0 };

        // Extract JSON-LD from each organic card
        const cards = document.querySelectorAll('.organic-card');
        for (const card of cards) {
          const script = card.querySelector('script[type="application/ld+json"]');
          if (script) {
            try {
              result.jsonLds.push(JSON.parse(script.textContent));
            } catch {}
          }
        }

        // Check for next page
        const nextLink = document.querySelector('.pagination .next a[rel="next"]');
        result.hasNext = !!nextLink;

        // Total results count
        const desc = document.querySelector('.page-description');
        if (desc) {
          const match = desc.textContent.match(/([\d,]+)\s+(criminal|family|immigration|personal|all|bankruptcy|business|tax|estate|employment|real)/i)
            || desc.textContent.match(/([\d,]+)\s+\w+\s+(lawyer|attorney)/i);
          if (match) {
            result.total = parseInt(match[1].replace(/,/g, ''), 10);
          }
        }

        return result;
      });

      hasNextPage = data.hasNext;
      totalResults = data.total;

      for (const jsonLd of data.jsonLds) {
        const lead = this._parseJsonLd(jsonLd, cityName, stateCode);
        if (lead && lead.first_name && lead.last_name) {
          leads.push(lead);
        }
      }
    } catch (err) {
      log.warn(`Avvo page error: ${err.message}`);
    } finally {
      await page.close().catch(() => {});
    }

    return { leads, hasNextPage, totalResults };
  }

  /**
   * Main search generator — scrapes Avvo city by city.
   */
  async *search(practiceArea, options = {}) {
    const puppeteer = require('puppeteer');
    const maxPages = options.maxPages || 5;
    const maxCities = options.maxCities || this._cityEntries.length;
    const rateLimiter = new RateLimiter({ minDelay: 3000, maxDelay: 6000 });

    // Determine cities to scrape
    let cities;
    if (options.city) {
      const sc = options.stateCode || options.state || this._stateForCity(options.city);
      cities = [{ city: options.city, stateCode: sc || 'CA', slug: this._citySlug(options.city) }];
    } else {
      cities = this._cityEntries.slice(0, maxCities);
    }

    // Launch browser
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });

    try {
      for (let ci = 0; ci < cities.length; ci++) {
        const { city, stateCode, slug } = cities[ci];
        const stateSlug = STATE_CODES[stateCode] || stateCode.toLowerCase();

        yield { _cityProgress: { current: ci + 1, total: cities.length } };
        log.info(`Searching: all attorneys in ${city}, ${stateCode}`);

        for (let page = 1; page <= maxPages; page++) {
          const pageParam = page > 1 ? `?page=${page}` : '';
          const url = `${this.baseUrl}/all-lawyers/${stateSlug}/${slug}.html${pageParam}`;

          log.info(`Page ${page} — ${url}`);

          const result = await this._scrapePage(browser, url, city, stateCode, rateLimiter);

          if (page === 1 && result.totalResults > 0) {
            const totalPages = Math.ceil(result.totalResults / 20);
            log.success(`Found ${result.totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
          }

          for (const lead of result.leads) {
            yield this.transformResult(lead, practiceArea);
          }

          if (!result.hasNextPage || result.leads.length === 0) {
            break;
          }

          // Small delay between pages
          await sleep(2000 + Math.random() * 2000);
        }
      }
    } finally {
      await browser.close().catch(() => {});
    }
  }
}

module.exports = new AvvoScraper();
