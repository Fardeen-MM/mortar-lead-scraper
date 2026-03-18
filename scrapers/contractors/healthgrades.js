/**
 * Healthgrades.com Scraper — Doctors, Dentists & Medical Practices
 *
 * Source: https://www.healthgrades.com
 * Records: Millions of US healthcare provider profiles
 * Method: HTTP + Cheerio — extracts RSC (React Server Components) payload from usearch pages
 *
 * URL pattern:
 *   https://www.healthgrades.com/usearch?what={specialty}&where={State}&pageNum={page}
 *
 * The usearch page is a Next.js app that returns SSR HTML with embedded RSC data
 * in `self.__next_f.push([1,"..."])` script tags. The RSC payload contains rich
 * JSON objects for each provider with these fields:
 *
 *   displayName, specialty, providerUrl, gender, npi, address (line1, line2),
 *   addresses[].location (cityName, region.regionAbbreviation, region.zip),
 *   surveyOverallRatingScore, surveyStarRatingScore, surveyUserCount,
 *   yearsSinceGraduation, acceptsNewPatients, primaryOfficeName,
 *   specialistDesc[], aboutMe, carePhilosophy
 *
 * Phone numbers are NOT in the listing data — they exist only on profile pages
 * and in tel: links for sponsored providers.
 *
 * The page shows ~20 organic providers per page (configurable via usearch-organic-provider-count).
 * Pagination: pageNum=1, pageNum=2, etc.
 *
 * Profile pages can be fetched for phone/website/education enrichment.
 * Profile URLs: /dentist/dr-{name}-{id}, /physician/dr-{name}-{id}, etc.
 */

const cheerio = require('cheerio');
const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Specialty -> Healthgrades search term mapping for the "what" parameter
// These are the exact terms Healthgrades accepts in its usearch endpoint.
// Note: use spaces not hyphens — Healthgrades URL-encodes them automatically.
const PRACTICE_AREA_CODES = {
  'dentist': 'dentist',
  'family doctor': 'primary care',
  'family practice': 'primary care',
  'family medicine': 'primary care',
  'primary care': 'primary care',
  'dermatologist': 'dermatologist',
  'dermatology': 'dermatologist',
  'chiropractor': 'chiropractor',
  'chiropractic': 'chiropractor',
  'optometrist': 'optometrist',
  'optometry': 'optometrist',
  'plastic surgeon': 'plastic surgeon',
  'plastic surgery': 'plastic surgeon',
  'cardiologist': 'cardiologist',
  'cardiology': 'cardiologist',
  'orthopedic': 'orthopedic surgeon',
  'orthopedic surgeon': 'orthopedic surgeon',
  'pediatrician': 'pediatrician',
  'pediatrics': 'pediatrician',
  'psychiatrist': 'psychiatrist',
  'psychiatry': 'psychiatrist',
  'neurologist': 'neurologist',
  'neurology': 'neurologist',
  'urologist': 'urologist',
  'urology': 'urologist',
  'ophthalmologist': 'ophthalmologist',
  'ophthalmology': 'ophthalmologist',
  'obgyn': 'obgyn',
  'ob/gyn': 'obgyn',
  'gynecologist': 'obgyn',
  'internist': 'internist',
  'internal medicine': 'internist',
  'gastroenterologist': 'gastroenterologist',
  'pulmonologist': 'pulmonologist',
  'endocrinologist': 'endocrinologist',
  'oncologist': 'oncologist',
  'allergist': 'allergist',
  'rheumatologist': 'rheumatologist',
  'podiatrist': 'podiatrist',
  'podiatry': 'podiatrist',
  'physical therapist': 'physical therapist',
  'physical therapy': 'physical therapist',
  'psychologist': 'psychologist',
  'ent': 'ear nose and throat',
  'ear nose throat': 'ear nose and throat',
};

// Default locations — state names as used in the "where" parameter
const DEFAULT_CITIES = [
  'Florida',
  'Texas',
  'California',
  'New York',
  'Illinois',
  'Georgia',
  'Pennsylvania',
  'Ohio',
  'North Carolina',
  'Michigan',
];

const BASE_URL = 'https://www.healthgrades.com';
const PAGE_SIZE = 20; // Healthgrades returns ~20 organic results per page

/**
 * Clean and normalize a name, removing "Dr." prefix and trailing credentials.
 * Credentials must appear after a comma to avoid matching inside names
 * (e.g., "Pavel" contains "PA", "Fernando" contains "DO").
 */
function cleanName(raw) {
  if (!raw) return '';
  return raw
    .replace(/^Dr\.\s*/i, '')
    // Strip trailing credentials only after a comma (e.g., ", DDS" or ", MD, FACS")
    .replace(/,\s*((?:M\.?D\.?|D\.?D\.?S\.?|D\.?O\.?|D\.?C\.?|D\.?P\.?M\.?|Ph\.?D\.?|D\.?M\.?D\.?|O\.?D\.?|D\.?N\.?P\.?|P\.?A\.?-?C?\.?|N\.?P\.?|R\.?N\.?|L\.?P\.?C\.?|L\.?C\.?S\.?W\.?|Psy\.?D\.?|F\.?A\.?C\.?S\.?|F\.?A\.?C\.?C\.?|F\.?A\.?A\.?D\.?)[\s,]*)+$/i, '')
    .replace(/,\s*$/, '')
    .trim();
}

/**
 * Extract credentials from a name string.
 * Only matches credentials after a comma separator.
 */
function extractCredential(raw) {
  if (!raw) return '';
  const match = raw.match(/,\s*((?:M\.?D\.?|D\.?D\.?S\.?|D\.?O\.?|D\.?C\.?|D\.?P\.?M\.?|Ph\.?D\.?|D\.?M\.?D\.?|O\.?D\.?|D\.?N\.?P\.?|P\.?A\.?-?C?\.?|N\.?P\.?)(?:\s*,\s*(?:M\.?D\.?|D\.?D\.?S\.?|D\.?O\.?|D\.?C\.?|F\.?A\.?C\.?S\.?|F\.?A\.?C\.?C\.?|F\.?A\.?A\.?D\.?|Ph\.?D\.?))*)/i);
  return match ? match[1].trim() : '';
}

/**
 * Normalize a phone number to (XXX) XXX-XXXX format.
 */
function normalizePhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return digits.length >= 7 ? raw.trim() : '';
}


class HealthgradesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'healthgrades',
      stateCode: 'HEALTHGRADES',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('healthgrades: buildSearchUrl() not used'); }
  parseResultsPage() { throw new Error('healthgrades: parseResultsPage() not used'); }
  extractResultCount() { throw new Error('healthgrades: extractResultCount() not used'); }

  transformResult(lead, practiceArea) {
    lead.source = 'healthgrades';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTP GET with redirect following and browser-like headers.
   */
  _httpGet(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const req = https.get(url, {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'none',
          'Sec-Fetch-User': '?1',
          'Upgrade-Insecure-Requests': '1',
        },
        timeout: 30000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `${BASE_URL}${redirect}`;
          return resolve(this._httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Build the usearch URL.
   * @param {string} specialty - Search term (e.g., 'dentist', 'family-practice')
   * @param {string} where - Location (e.g., 'Florida', 'Miami, FL')
   * @param {number} page - Page number (1-based)
   */
  _buildUrl(specialty, where, page = 1) {
    const params = new URLSearchParams({
      what: specialty,
      where: where,
    });
    if (page > 1) params.set('pageNum', String(page));
    return `${BASE_URL}/usearch?${params.toString()}`;
  }

  /**
   * Extract and decode the RSC (React Server Components) payload from script tags.
   * The payload is split across multiple `self.__next_f.push([1,"..."])` calls.
   * Returns the concatenated decoded string.
   */
  _extractRscPayload($) {
    let payload = '';
    $('script').each((_, s) => {
      const html = $(s).html() || '';
      if (!html.startsWith('self.__next_f.push(')) return;
      const match = html.match(/self\.__next_f\.push\(\[1,"(.*)"\]\)/s);
      if (match) {
        try {
          // Decode the escaped JSON string
          const decoded = JSON.parse('"' + match[1] + '"');
          payload += decoded;
        } catch (e) { /* skip malformed chunks */ }
      }
    });
    return payload;
  }

  /**
   * Extract provider JSON objects from the RSC payload.
   * Provider objects contain `displayName` and `providerUrl` keys.
   * We search for `"displayName":"Dr. ..."` markers and walk to find
   * the enclosing JSON object boundaries.
   */
  _extractProvidersFromRsc(rscPayload) {
    const providers = [];
    const seen = new Set();

    // Find all displayName occurrences that look like provider names
    const regex = /"displayName":"(Dr\.[^"]+)"/g;
    let match;

    while ((match = regex.exec(rscPayload)) !== null) {
      const idx = match.index;

      // Walk backwards to find the opening { of the provider object
      let start = idx;
      let braceCount = 0;
      for (let i = idx; i >= Math.max(0, idx - 10000); i--) {
        if (rscPayload[i] === '}') braceCount++;
        if (rscPayload[i] === '{') {
          braceCount--;
          if (braceCount < 0) { start = i; break; }
        }
      }

      // Walk forward to find the matching closing }
      let end = idx;
      braceCount = 0;
      for (let i = start; i < Math.min(rscPayload.length, start + 30000); i++) {
        if (rscPayload[i] === '{') braceCount++;
        if (rscPayload[i] === '}') {
          braceCount--;
          if (braceCount === 0) { end = i + 1; break; }
        }
      }

      const objStr = rscPayload.substring(start, end);
      try {
        const obj = JSON.parse(objStr);
        // Validate: must have displayName and providerUrl
        if (obj.displayName && obj.providerUrl && !seen.has(obj.providerUrl)) {
          seen.add(obj.providerUrl);
          providers.push(obj);
        }
      } catch (e) { /* skip objects that don't parse cleanly */ }
    }

    return providers;
  }

  /**
   * Also extract phone numbers from tel: links in the HTML (sponsored providers).
   * Returns a Map of providerUrl -> phone number.
   */
  _extractPhonesFromHtml($) {
    const phones = new Map();
    $('a[href^="tel:"]').each((_, el) => {
      const phone = normalizePhone($(el).attr('href').replace('tel:', ''));
      if (!phone) return;

      // Walk up to find a nearby profile link
      let parent = $(el).parent();
      for (let i = 0; i < 15 && parent.length; i++) {
        const profileLink = parent.find('a[href*="/dentist/dr-"], a[href*="/physician/dr-"], a[href*="/chiropractor/dr-"], a[href*="/optometrist/dr-"]').first();
        if (profileLink.length) {
          const href = profileLink.attr('href') || '';
          if (href && !phones.has(href)) {
            phones.set(href, phone);
          }
          break;
        }
        parent = parent.parent();
      }
    });
    return phones;
  }

  /**
   * Map a Healthgrades RSC provider object to a normalized lead.
   */
  _mapProvider(obj, phoneMap) {
    const rawName = obj.displayName || '';
    const name = cleanName(rawName);
    const { firstName, lastName } = this.splitName(name);

    if (!firstName && !lastName) return null;

    // Address from the top-level address object
    const addr = obj.address || {};
    const addressLine1 = addr.line1 || '';
    const addressLine2 = addr.line2 || ''; // "City, ST ZIP"

    // Also try the detailed addresses array
    const addrDetail = obj.addresses?.[0] || {};
    const location = addrDetail.location || {};
    const region = location.region || location.locRegion || {};

    const city = location.cityName || '';
    const state = region.regionAbbreviation || location.regionAbbreviation || '';
    const zip = region.zip || '';

    // Phone from HTML tel: links (if available for this provider)
    const phone = phoneMap?.get(obj.providerUrl) || '';

    // Profile URL
    let profileUrl = obj.providerUrl || '';
    if (profileUrl && !profileUrl.startsWith('http')) {
      profileUrl = `${BASE_URL}${profileUrl}`;
    }

    // Rating
    const rating = obj.surveyOverallRatingScore || obj.surveyStarRatingScore || '';
    const reviewCount = obj.surveyUserCount || '';

    // Practice name
    const firmName = obj.primaryOfficeName || '';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      credential: extractCredential(rawName),
      address: addressLine1,
      city,
      state,
      zip,
      phone,
      email: '',
      website: '',
      bar_number: obj.npi || '', // NPI number in bar_number field
      profile_url: profileUrl,
      rating: rating ? String(rating) : '',
      review_count: reviewCount ? String(reviewCount) : '',
      practice_area: obj.specialty || (obj.specialistDesc || [])[0] || '',
      gender: obj.gender || '',
      years_experience: obj.yearsSinceGraduation || '',
      accepts_new_patients: obj.acceptsNewPatients ? 'Yes' : 'No',
      source: 'healthgrades',
    };
  }

  /**
   * Parse a provider profile page for phone, website, and other details.
   * Profile pages use the same RSC payload pattern as search pages.
   */
  parseProfilePage($) {
    const result = {};

    // Extract phone from tel: links (most reliable)
    const telLink = $('a[href^="tel:"]').first();
    if (telLink.length) {
      result.phone = normalizePhone(telLink.attr('href').replace('tel:', ''));
    }

    // Try JSON-LD for structured data
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const ld = JSON.parse($(el).html());
        if (ld && (ld['@type'] === 'Physician' || ld['@type'] === 'Dentist' ||
            ld['@type'] === 'MedicalBusiness')) {
          if (ld.telephone && !result.phone) result.phone = normalizePhone(ld.telephone);
          if (ld.address?.streetAddress && !result.address) result.address = ld.address.streetAddress;
          if (ld.address?.addressLocality && !result.city) result.city = ld.address.addressLocality;
          if (ld.address?.addressRegion && !result.state) result.state = ld.address.addressRegion;
          if (ld.address?.postalCode && !result.zip) result.zip = ld.address.postalCode;
        }
      } catch (e) { /* skip */ }
    });

    // Try RSC payload for embedded provider data
    const rscPayload = this._extractRscPayload($);
    if (rscPayload) {
      // Look for phone in RSC
      const phoneMatch = rscPayload.match(/"(?:phone|telephone|phoneNumber)":"(\+?[\d\s\-().]+)"/);
      if (phoneMatch && !result.phone) {
        result.phone = normalizePhone(phoneMatch[1]);
      }

      // Look for website in RSC
      const websiteMatch = rscPayload.match(/"(?:website|practiceWebsite|websiteUrl)":"(https?:\/\/[^"]+)"/);
      if (websiteMatch && !result.website) {
        const url = websiteMatch[1];
        if (!this.isExcludedDomain(url) && !url.includes('healthgrades.com')) {
          result.website = url;
        }
      }

      // Look for practice/firm name
      const practiceMatch = rscPayload.match(/"(?:officeName|practiceName|practiceGroupName)":"([^"]+)"/);
      if (practiceMatch && !result.firm_name) {
        result.firm_name = practiceMatch[1];
      }
    }

    // Website from HTML
    if (!result.website) {
      const websiteEl = $('a[href*="://"][rel="nofollow"], a[data-qa="website-link"]').first();
      if (websiteEl.length) {
        const href = websiteEl.attr('href') || '';
        if (href && !this.isExcludedDomain(href) && !href.includes('healthgrades.com')) {
          result.website = href;
        }
      }
    }

    return result;
  }

  /**
   * Check if there are more results pages.
   * Looks for page links in the HTML and in the RSC payload.
   */
  _hasMorePages($, currentPage) {
    // Check for Next page link in HTML
    const nextLink = $('a[rel="next"], a[aria-label="Next page"], a[aria-label="next"]');
    if (nextLink.length) return true;

    // Check for page number links > current page
    let maxPageFound = currentPage;
    $('a[href*="pageNum="]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const pageMatch = href.match(/pageNum=(\d+)/);
      if (pageMatch) {
        const pageNum = parseInt(pageMatch[1], 10);
        if (pageNum > maxPageFound) maxPageFound = pageNum;
      }
    });

    return maxPageFound > currentPage;
  }

  /**
   * Main search generator.
   *
   * Strategy:
   *   1. Use /usearch?what={specialty}&where={state} for each location
   *   2. Fetch HTML and extract RSC payload from self.__next_f.push() scripts
   *   3. Parse provider JSON objects from the RSC payload
   *   4. Also extract phone numbers from tel: links for sponsored providers
   *   5. Paginate through results using pageNum parameter
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // Resolve specialty to search term
    let searchTerm = 'dentist'; // Default
    if (practiceArea) {
      const key = practiceArea.toLowerCase().trim();
      searchTerm = PRACTICE_AREA_CODES[key] || key;
    }

    log.scrape(`Healthgrades: Searching "${searchTerm}" (practice area: ${practiceArea || 'dentist'})`);

    // Determine locations to search
    let locations;
    if (options.city) {
      locations = [options.city];
    } else {
      locations = [...DEFAULT_CITIES];
    }

    // Apply maxCities limit
    if (options.maxCities && locations.length > options.maxCities) {
      locations = locations.slice(0, options.maxCities);
    }

    const maxPages = options.maxPages || 20;
    let totalYielded = 0;
    let totalWithPhone = 0;
    let totalWithNpi = 0;
    let totalWithAddress = 0;

    for (let li = 0; li < locations.length; li++) {
      const where = locations[li];
      yield { _cityProgress: { current: li + 1, total: locations.length } };

      log.scrape(`Healthgrades: Searching ${searchTerm} in ${where}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let locationTotal = 0;

      while (page <= maxPages) {
        if (options.maxPages && pagesFetched >= options.maxPages) break;

        const url = this._buildUrl(searchTerm, where, page);
        log.info(`Healthgrades: Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Healthgrades: Request failed: ${err.message}`);
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Healthgrades: Got ${response.statusCode} — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.warn(`Healthgrades: HTTP ${response.statusCode} for ${where} page ${page}`);
          break;
        }

        pagesFetched++;

        // Check for anti-bot / CAPTCHA
        if (response.body.includes('challenge-platform') ||
            response.body.includes('cf-browser-verification') ||
            (response.body.includes('Just a moment') && response.body.length < 10000)) {
          log.warn('Healthgrades: Cloudflare challenge detected');
          yield { _captcha: true, city: where, page, reason: 'Cloudflare challenge' };
          break;
        }

        const $ = cheerio.load(response.body);

        // Extract RSC payload
        const rscPayload = this._extractRscPayload($);

        if (!rscPayload || rscPayload.length < 100) {
          log.warn(`Healthgrades: No RSC payload found for ${where} page ${page}`);
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) break;
          page++;
          continue;
        }

        // Extract provider objects from RSC
        const providerObjects = this._extractProvidersFromRsc(rscPayload);

        // Extract phone numbers from tel: links in HTML
        const phoneMap = this._extractPhonesFromHtml($);

        if (providerObjects.length === 0) {
          if (page === 1) {
            log.info(`Healthgrades: No providers found for ${searchTerm} in ${where}`);
          }
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) break;
          page++;
          continue;
        }

        consecutiveEmpty = 0;

        if (page === 1) {
          log.success(`Healthgrades: Found ${providerObjects.length} providers on page 1 for ${where}` +
            (phoneMap.size > 0 ? ` (${phoneMap.size} with phone from HTML)` : ''));
        }

        // Map and yield each provider
        for (const obj of providerObjects) {
          const lead = this._mapProvider(obj, phoneMap);
          if (!lead) continue;

          // Track stats
          if (lead.phone) totalWithPhone++;
          if (lead.bar_number) totalWithNpi++;
          if (lead.address) totalWithAddress++;

          yield this.transformResult(lead, practiceArea || lead.practice_area);
          totalYielded++;
          locationTotal++;
        }

        // Check for more pages
        if (!this._hasMorePages($, page)) {
          log.info(`Healthgrades: No more pages for ${where} (${locationTotal} providers from ${page} pages)`);
          break;
        }

        page++;
      }

      if (locationTotal > 0) {
        log.success(`Healthgrades: ${where} complete — ${locationTotal} providers`);
      }
    }

    log.success([
      'Healthgrades scrape complete:',
      `${totalYielded.toLocaleString()} providers yielded`,
      `(${totalWithPhone.toLocaleString()} with phone,`,
      `${totalWithNpi.toLocaleString()} with NPI,`,
      `${totalWithAddress.toLocaleString()} with address)`,
    ].join(' '));
  }
}

module.exports = new HealthgradesScraper();
