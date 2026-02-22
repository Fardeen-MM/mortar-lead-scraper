/**
 * FindLaw.com Lawyer Directory Scraper
 *
 * Source: https://lawyers.findlaw.com/
 * Method: Puppeteer (Cloudflare blocks curl) + JSON-LD structured data
 * Data:   Law firm listings with firm name, phone, profile URL, location
 *
 * URL Pattern:
 *   /{practice-area}/{state}/{city}/  (e.g. /criminal-law/florida/miami/)
 *
 * FindLaw puts ALL listings in a single JSON-LD ItemList on each page.
 * No pagination needed — the entire result set is in one page load.
 * Each item is a LegalService with name, telephone, profile URL, and address.
 *
 * Note: FindLaw lists law FIRMS, not individual lawyers. Firm names may
 * contain attorney names (e.g., "Smith & Jones, LLP"). Profile pages
 * may have individual attorney data.
 *
 * Valuable for: firm phone numbers, firm names for cross-referencing.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

/**
 * Practice area URL slugs for FindLaw.
 */
const PRACTICE_AREA_SLUGS = {
  'criminal defense': 'criminal-law',
  'personal injury': 'personal-injury',
  'family law': 'family-law',
  'immigration': 'immigration-law',
  'bankruptcy': 'bankruptcy',
  'business': 'business-litigation',
  'real estate': 'real-estate',
  'employment': 'employment-labor-law',
  'estate planning': 'estate-planning',
  'tax': 'tax-law',
  'dui': 'dui-dwi',
  'traffic': 'traffic-violations',
};

/**
 * State full names for URL generation.
 */
const STATE_NAMES = {
  AL: 'alabama', AK: 'alaska', AZ: 'arizona', AR: 'arkansas',
  CA: 'california', CO: 'colorado', CT: 'connecticut', DE: 'delaware',
  DC: 'district-of-columbia', FL: 'florida', GA: 'georgia', HI: 'hawaii',
  ID: 'idaho', IL: 'illinois', IN: 'indiana', IA: 'iowa',
  KS: 'kansas', KY: 'kentucky', LA: 'louisiana', ME: 'maine',
  MD: 'maryland', MA: 'massachusetts', MI: 'michigan', MN: 'minnesota',
  MS: 'mississippi', MO: 'missouri', MT: 'montana', NE: 'nebraska',
  NV: 'nevada', NH: 'new-hampshire', NJ: 'new-jersey', NM: 'new-mexico',
  NY: 'new-york', NC: 'north-carolina', ND: 'north-dakota', OH: 'ohio',
  OK: 'oklahoma', OR: 'oregon', PA: 'pennsylvania', RI: 'rhode-island',
  SC: 'south-carolina', SD: 'south-dakota', TN: 'tennessee', TX: 'texas',
  UT: 'utah', VT: 'vermont', VA: 'virginia', WA: 'washington',
  WV: 'west-virginia', WI: 'wisconsin', WY: 'wyoming',
};

/**
 * Default cities across US states.
 */
const DEFAULT_CITY_ENTRIES = [
  { city: 'New York', stateCode: 'NY' },
  { city: 'Los Angeles', stateCode: 'CA' },
  { city: 'Chicago', stateCode: 'IL' },
  { city: 'Houston', stateCode: 'TX' },
  { city: 'Phoenix', stateCode: 'AZ' },
  { city: 'Philadelphia', stateCode: 'PA' },
  { city: 'San Antonio', stateCode: 'TX' },
  { city: 'San Diego', stateCode: 'CA' },
  { city: 'Dallas', stateCode: 'TX' },
  { city: 'Miami', stateCode: 'FL' },
  { city: 'Atlanta', stateCode: 'GA' },
  { city: 'Boston', stateCode: 'MA' },
  { city: 'Denver', stateCode: 'CO' },
  { city: 'Seattle', stateCode: 'WA' },
  { city: 'Nashville', stateCode: 'TN' },
  { city: 'Charlotte', stateCode: 'NC' },
  { city: 'San Francisco', stateCode: 'CA' },
  { city: 'Portland', stateCode: 'OR' },
  { city: 'Las Vegas', stateCode: 'NV' },
  { city: 'Minneapolis', stateCode: 'MN' },
];

class FindLawScraper extends BaseScraper {
  constructor() {
    super({
      name: 'findlaw',
      stateCode: 'FINDLAW',
      baseUrl: 'https://lawyers.findlaw.com',
      pageSize: 100,
      practiceAreaCodes: PRACTICE_AREA_SLUGS,
      defaultCities: DEFAULT_CITY_ENTRIES.map(e => e.city),
    });
    this._cityEntries = DEFAULT_CITY_ENTRIES;
  }

  /**
   * Convert city name to URL slug.
   * "Los Angeles" → "los-angeles", "San Francisco" → "san-francisco"
   */
  _citySlug(cityName) {
    return cityName.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
  }

  _stateForCity(cityName) {
    const entry = this._cityEntries.find(
      e => e.city.toLowerCase() === cityName.toLowerCase()
    );
    return entry ? entry.stateCode : null;
  }

  /**
   * Try to extract first/last name from a firm name.
   * "John Smith, Attorney" → { first: "John", last: "Smith" }
   * "Smith & Jones LLP" → { first: "", last: "Smith" } (firm, not person)
   */
  _parseFirmName(firmName) {
    if (!firmName) return { first_name: '', last_name: '', is_firm: true };

    // Remove common suffixes
    const cleaned = firmName
      .replace(/&amp;/g, '&')
      .replace(/,?\s*(LLC|LLP|PLLC|PA|P\.?A\.?|PC|P\.?C\.?|Inc\.?|Attorney|Attorneys|at Law|Law\s*(Firm|Group|Office|Offices|Center))\.?\s*$/i, '')
      .trim();

    // If it contains &, it's a multi-partner firm
    if (cleaned.includes('&') || cleaned.includes(' and ')) {
      return { first_name: '', last_name: '', is_firm: true, firm_name: firmName.replace(/&amp;/g, '&') };
    }

    // Try to split into first/last
    const parts = cleaned.split(/\s+/);
    if (parts.length === 2) {
      return { first_name: parts[0], last_name: parts[1], is_firm: false };
    }
    if (parts.length === 3) {
      // Could be "John M. Smith" or "The Smith Firm"
      if (parts[0].toLowerCase() === 'the' || parts[0].toLowerCase() === 'law') {
        return { first_name: '', last_name: '', is_firm: true, firm_name: firmName.replace(/&amp;/g, '&') };
      }
      return { first_name: parts[0], last_name: parts[2], is_firm: false };
    }

    // 4+ words or single word — treat as firm (don't put firm name in last_name)
    if (parts.length > 3 || parts.length === 1) {
      return { first_name: '', last_name: '', is_firm: true, firm_name: firmName.replace(/&amp;/g, '&') };
    }

    // Shouldn't reach here, but fallback to firm
    return { first_name: '', last_name: '', is_firm: true, firm_name: firmName.replace(/&amp;/g, '&') };
  }

  /**
   * Parse a LegalService JSON-LD item into a lead.
   */
  _parseItem(item, cityName, stateCode) {
    if (!item || item['@type'] !== 'LegalService') return null;

    const rawName = (item.name || '').replace(/&amp;/g, '&');
    const phone = item.telephone || '';
    const address = item.address || {};
    const profilePage = item.mainEntityOfPage || {};
    const profileUrl = profilePage['@id'] || profilePage.url || '';

    const nameInfo = this._parseFirmName(rawName);

    return {
      first_name: nameInfo.first_name || '',
      last_name: nameInfo.last_name || '',
      firm_name: nameInfo.is_firm ? rawName : '',
      city: cityName,
      state: address.addressRegion || stateCode,
      phone,
      website: '',
      email: '',
      bar_number: '',
      bar_status: 'Active',
      source: 'findlaw',
      profile_url: profileUrl,
    };
  }

  /**
   * Scrape a single FindLaw page using Puppeteer.
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
        const result = { items: [], total: 0, hasNext: false };

        // Find the JSON-LD script
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of scripts) {
          try {
            const json = JSON.parse(script.textContent);
            if (json['@type'] === 'CollectionPage' && json.mainEntity) {
              result.total = parseInt(json.mainEntity.numberOfItems || '0', 10);
              result.items = json.mainEntity.itemListElement || [];
              break;
            }
          } catch {}
        }

        // Check for Next page button
        const nextBtn = document.querySelector('a.fl-pagination-button[href*="page="]');
        if (nextBtn && nextBtn.textContent.trim() === 'Next') {
          result.hasNext = true;
        }

        return result;
      });

      hasNextPage = data.hasNext;
      totalResults = data.total;

      for (const item of data.items) {
        const lead = this._parseItem(item, cityName, stateCode);
        if (lead && (lead.first_name || lead.firm_name)) {
          leads.push(lead);
        }
      }
    } catch (err) {
      log.warn(`FindLaw page error: ${err.message}`);
    } finally {
      await page.close().catch(() => {});
    }

    return { leads, hasNextPage, totalResults };
  }

  /**
   * Main search generator — scrapes FindLaw city by city with pagination.
   */
  async *search(practiceArea, options = {}) {
    const puppeteer = require('puppeteer');
    const maxPages = options.maxPages || 5;
    const maxCities = options.maxCities || this._cityEntries.length;
    const rateLimiter = new RateLimiter({ minDelay: 5000, maxDelay: 10000 });

    // Determine practice area slug
    const practiceSlug = practiceArea
      ? (PRACTICE_AREA_SLUGS[practiceArea.toLowerCase()] || 'criminal-law')
      : 'criminal-law';

    // Determine cities to scrape
    let cities;
    if (options.city) {
      const sc = options.stateCode || options.state || this._stateForCity(options.city);
      cities = [{ city: options.city, stateCode: sc || 'CA' }];
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
        const { city, stateCode } = cities[ci];
        const stateName = STATE_NAMES[stateCode] || stateCode.toLowerCase();
        const citySlug = this._citySlug(city);

        yield { _cityProgress: { current: ci + 1, total: cities.length } };
        log.info(`Searching: ${practiceSlug} firms in ${city}, ${stateCode}`);

        for (let page = 1; page <= maxPages; page++) {
          const pageParam = page > 1 ? `?page=${page}` : '';
          const url = `${this.baseUrl}/${practiceSlug}/${stateName}/${citySlug}/${pageParam}`;

          log.info(`Page ${page} — ${url}`);

          const result = await this._scrapePage(browser, url, city, stateCode, rateLimiter);

          if (page === 1 && result.totalResults > 0) {
            log.success(`Found ${result.totalResults} listings for ${city}, ${stateCode}`);
          }

          for (const lead of result.leads) {
            yield this.transformResult(lead, practiceArea);
          }

          if (!result.hasNextPage || result.leads.length === 0) {
            break;
          }

          await sleep(3000 + Math.random() * 3000);
        }

        // Small delay between cities
        if (ci < cities.length - 1) {
          await sleep(3000 + Math.random() * 3000);
        }
      }
    } finally {
      await browser.close().catch(() => {});
    }
  }
}

module.exports = new FindLawScraper();
