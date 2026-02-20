/**
 * Google Places Law Firm Scraper
 *
 * Source: Google Places API (Text Search)
 * Method: REST API with API key
 * Data:   Business name, address, phone, website, rating, Google Maps URL
 *
 * Searches for "lawyers" and "law firm" across major cities worldwide.
 * Uses Google Places API (New) Text Search endpoint.
 *
 * Requirements:
 *   - GOOGLE_PLACES_API_KEY env var (get from Google Cloud Console)
 *   - Enable "Places API (New)" in Google Cloud project
 *   - Free tier: $200/month credit (~6,600 text searches)
 *
 * Each text search returns up to 20 results. We paginate via nextPageToken
 * for up to 60 results per query (3 pages max per Google's limit).
 */

const BaseScraper = require('../base-scraper');
const https = require('https');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');
const { titleCase } = require('../../lib/normalizer');

// Major cities across all covered countries
// Each entry: { city, stateCode, country, region }
const DEFAULT_CITY_ENTRIES = [
  // --- US (top 50 metros by legal market size) ---
  { city: 'New York', stateCode: 'NY', country: 'US' },
  { city: 'Los Angeles', stateCode: 'CA', country: 'US' },
  { city: 'Chicago', stateCode: 'IL', country: 'US' },
  { city: 'Houston', stateCode: 'TX', country: 'US' },
  { city: 'Phoenix', stateCode: 'AZ', country: 'US' },
  { city: 'Philadelphia', stateCode: 'PA', country: 'US' },
  { city: 'San Antonio', stateCode: 'TX', country: 'US' },
  { city: 'San Diego', stateCode: 'CA', country: 'US' },
  { city: 'Dallas', stateCode: 'TX', country: 'US' },
  { city: 'Austin', stateCode: 'TX', country: 'US' },
  { city: 'Jacksonville', stateCode: 'FL', country: 'US' },
  { city: 'San Jose', stateCode: 'CA', country: 'US' },
  { city: 'Fort Worth', stateCode: 'TX', country: 'US' },
  { city: 'Columbus', stateCode: 'OH', country: 'US' },
  { city: 'Charlotte', stateCode: 'NC', country: 'US' },
  { city: 'Indianapolis', stateCode: 'IN', country: 'US' },
  { city: 'San Francisco', stateCode: 'CA', country: 'US' },
  { city: 'Seattle', stateCode: 'WA', country: 'US' },
  { city: 'Denver', stateCode: 'CO', country: 'US' },
  { city: 'Nashville', stateCode: 'TN', country: 'US' },
  { city: 'Washington', stateCode: 'DC', country: 'US' },
  { city: 'Oklahoma City', stateCode: 'OK', country: 'US' },
  { city: 'Boston', stateCode: 'MA', country: 'US' },
  { city: 'Las Vegas', stateCode: 'NV', country: 'US' },
  { city: 'Portland', stateCode: 'OR', country: 'US' },
  { city: 'Memphis', stateCode: 'TN', country: 'US' },
  { city: 'Louisville', stateCode: 'KY', country: 'US' },
  { city: 'Baltimore', stateCode: 'MD', country: 'US' },
  { city: 'Milwaukee', stateCode: 'WI', country: 'US' },
  { city: 'Albuquerque', stateCode: 'NM', country: 'US' },
  { city: 'Tucson', stateCode: 'AZ', country: 'US' },
  { city: 'Sacramento', stateCode: 'CA', country: 'US' },
  { city: 'Kansas City', stateCode: 'MO', country: 'US' },
  { city: 'Atlanta', stateCode: 'GA', country: 'US' },
  { city: 'Miami', stateCode: 'FL', country: 'US' },
  { city: 'Raleigh', stateCode: 'NC', country: 'US' },
  { city: 'Tampa', stateCode: 'FL', country: 'US' },
  { city: 'Minneapolis', stateCode: 'MN', country: 'US' },
  { city: 'Cleveland', stateCode: 'OH', country: 'US' },
  { city: 'Pittsburgh', stateCode: 'PA', country: 'US' },
  { city: 'St. Louis', stateCode: 'MO', country: 'US' },
  { city: 'Cincinnati', stateCode: 'OH', country: 'US' },
  { city: 'Orlando', stateCode: 'FL', country: 'US' },
  { city: 'Salt Lake City', stateCode: 'UT', country: 'US' },
  { city: 'Richmond', stateCode: 'VA', country: 'US' },
  { city: 'Hartford', stateCode: 'CT', country: 'US' },
  { city: 'Birmingham', stateCode: 'AL', country: 'US' },
  { city: 'New Orleans', stateCode: 'LA', country: 'US' },
  { city: 'Honolulu', stateCode: 'HI', country: 'US' },
  { city: 'Detroit', stateCode: 'MI', country: 'US' },

  // --- Canada ---
  { city: 'Toronto', stateCode: 'CA-ON', country: 'CA' },
  { city: 'Vancouver', stateCode: 'CA-BC', country: 'CA' },
  { city: 'Montreal', stateCode: 'CA-QC', country: 'CA' },
  { city: 'Calgary', stateCode: 'CA-AB', country: 'CA' },
  { city: 'Edmonton', stateCode: 'CA-AB', country: 'CA' },
  { city: 'Ottawa', stateCode: 'CA-ON', country: 'CA' },
  { city: 'Winnipeg', stateCode: 'CA-MB', country: 'CA' },
  { city: 'Halifax', stateCode: 'CA-NS', country: 'CA' },

  // --- UK ---
  { city: 'London', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Manchester', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Birmingham', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Leeds', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Edinburgh', stateCode: 'UK-SC', country: 'UK' },
  { city: 'Glasgow', stateCode: 'UK-SC', country: 'UK' },
  { city: 'Bristol', stateCode: 'UK-EW', country: 'UK' },
  { city: 'Liverpool', stateCode: 'UK-EW', country: 'UK' },

  // --- Australia ---
  { city: 'Sydney', stateCode: 'AU-NSW', country: 'AU' },
  { city: 'Melbourne', stateCode: 'AU-VIC', country: 'AU' },
  { city: 'Brisbane', stateCode: 'AU-QLD', country: 'AU' },
  { city: 'Perth', stateCode: 'AU-WA', country: 'AU' },
  { city: 'Adelaide', stateCode: 'AU-SA', country: 'AU' },
  { city: 'Hobart', stateCode: 'AU-TAS', country: 'AU' },
];

class GooglePlacesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'google-places',
      stateCode: 'GOOGLE-PLACES',
      baseUrl: 'https://places.googleapis.com',
      pageSize: 20, // Google returns max 20 per page
      practiceAreaCodes: {},
      defaultCities: DEFAULT_CITY_ENTRIES.map(e => e.city),
    });

    this._cityEntries = DEFAULT_CITY_ENTRIES;
    this._apiKey = process.env.GOOGLE_PLACES_API_KEY || '';
  }

  /**
   * Search Google Places for law firms across cities.
   */
  async *search(practiceArea, options = {}) {
    if (!this._apiKey) {
      log.error('GOOGLE_PLACES_API_KEY not set — cannot search Google Places');
      yield { _captcha: true, city: 'all', reason: 'Missing GOOGLE_PLACES_API_KEY' };
      return;
    }

    const rateLimiter = new RateLimiter({ minDelay: 1000, maxDelay: 3000 });
    const maxCities = options.maxCities || (options.maxPages ? options.maxPages : null);

    // Filter cities if a specific city is requested
    let cities = [...this._cityEntries];
    if (options.city) {
      cities = cities.filter(c =>
        c.city.toLowerCase().includes(options.city.toLowerCase())
      );
      if (cities.length === 0) {
        // Try as a custom city with US default
        cities = [{ city: options.city, stateCode: '', country: 'US' }];
      }
    }

    if (maxCities) cities = cities.slice(0, maxCities);

    const searchQueries = ['lawyers', 'law firm'];

    for (let i = 0; i < cities.length; i++) {
      const cityEntry = cities[i];
      yield { _cityProgress: { current: i + 1, total: cities.length } };

      for (const query of searchQueries) {
        const textQuery = `${query} in ${cityEntry.city}`;
        log.info(`[Google Places] Searching: "${textQuery}"`);

        let pageToken = null;
        let pagesSearched = 0;
        const maxPages = options.maxPages ? Math.min(options.maxPages, 3) : 3;

        do {
          await rateLimiter.wait();

          try {
            const results = await this._textSearch(textQuery, pageToken);
            if (!results || !results.places || results.places.length === 0) break;

            for (const place of results.places) {
              const lead = this._parsePlaceResult(place, cityEntry);
              if (lead) yield lead;
            }

            pageToken = results.nextPageToken || null;
            pagesSearched++;
          } catch (err) {
            log.warn(`[Google Places] Search failed for "${textQuery}": ${err.message}`);
            break;
          }
        } while (pageToken && pagesSearched < maxPages);
      }
    }
  }

  /**
   * Call Google Places Text Search API (New).
   */
  async _textSearch(textQuery, pageToken) {
    const body = {
      textQuery,
      includedType: 'lawyer',
      languageCode: 'en',
    };

    if (pageToken) {
      body.pageToken = pageToken;
    }

    const requestBody = JSON.stringify(body);

    return new Promise((resolve, reject) => {
      const options = {
        hostname: 'places.googleapis.com',
        path: '/v1/places:searchText',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': this._apiKey,
          'X-Goog-FieldMask': 'places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri,places.rating,places.googleMapsUri,places.id,nextPageToken',
          'Content-Length': Buffer.byteLength(requestBody),
        },
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            if (res.statusCode !== 200) {
              reject(new Error(`API returned ${res.statusCode}: ${data.substring(0, 200)}`));
              return;
            }
            resolve(JSON.parse(data));
          } catch (err) {
            reject(new Error(`Failed to parse response: ${err.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });

      req.write(requestBody);
      req.end();
    });
  }

  /**
   * Parse a Google Places result into a lead object.
   */
  _parsePlaceResult(place, cityEntry) {
    if (!place || !place.displayName) return null;

    const name = place.displayName.text || place.displayName || '';
    if (!name) return null;

    // Try to extract person name vs firm name
    // Google Places usually returns the business name
    const { firstName, lastName, firmName } = this._parseBusinessName(name);

    // Parse address components
    const address = place.formattedAddress || '';
    const { city, state } = this._parseAddress(address, cityEntry);

    // Get phone — prefer national format
    const phone = place.nationalPhoneNumber || place.internationalPhoneNumber || '';

    // Get website
    const website = place.websiteUri || '';

    return this.transformResult({
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName || name,
      city: city || cityEntry.city,
      state: state || cityEntry.stateCode,
      phone,
      website,
      email: '',
      bar_number: '',
      bar_status: '',
      admission_date: '',
      source: 'google_places',
      profile_url: place.googleMapsUri || '',
      _rating: place.rating || '',
      _googlePlaceId: place.id || '',
    }, '');
  }

  /**
   * Try to determine if a business name is a person or a firm.
   * "Smith & Jones LLP" → firm
   * "John Smith, Attorney" → person
   */
  _parseBusinessName(name) {
    const lower = name.toLowerCase();

    // Firm indicators
    const firmIndicators = [
      'llp', 'pllc', 'p.c.', 'p.a.', 'llc', 'inc', 'ltd', 'plc',
      '& associates', 'and associates', 'law firm', 'law office',
      'law group', 'law center', 'legal', 'attorneys at law',
      'attorneys-at-law',
    ];

    const isFirm = firmIndicators.some(ind => lower.includes(ind)) ||
      name.includes('&') || name.includes(' and ');

    if (isFirm) {
      return { firstName: '', lastName: '', firmName: name };
    }

    // Person pattern: "John Smith" or "John Smith, Attorney at Law"
    const cleaned = name
      .replace(/,?\s*(attorney|lawyer|esq\.?|esquire|j\.?d\.?|counsel|solicitor|barrister)/gi, '')
      .replace(/,?\s*at law/gi, '')
      .trim();

    const parts = cleaned.split(/\s+/);
    if (parts.length >= 2 && parts.length <= 4) {
      // Likely a person name
      const firstName = titleCase(parts[0]);
      const lastName = titleCase(parts[parts.length - 1]);
      return { firstName, lastName, firmName: '' };
    }

    // Can't determine — treat as firm
    return { firstName: '', lastName: '', firmName: name };
  }

  /**
   * Parse city and state from a formatted address string.
   */
  _parseAddress(address, cityEntry) {
    if (!address) return { city: '', state: '' };

    // US format: "123 Main St, City, ST 12345, USA"
    // UK format: "123 Main St, City, AB1 2CD, UK"
    const parts = address.split(',').map(p => p.trim());

    // Try to find city and state from address parts
    if (parts.length >= 3) {
      // Second-to-last part before country often has city
      // Last or second-to-last has state/zip
      const cityCandidate = parts[parts.length - 3] || '';
      const stateZip = parts[parts.length - 2] || '';

      // Extract state code from "ST 12345" format
      const stateMatch = stateZip.match(/^([A-Z]{2})\s+\d/);
      if (stateMatch) {
        return { city: cityCandidate, state: stateMatch[1] };
      }
    }

    // Fallback to city entry data
    return { city: cityEntry.city, state: cityEntry.stateCode };
  }
}

module.exports = new GooglePlacesScraper();
