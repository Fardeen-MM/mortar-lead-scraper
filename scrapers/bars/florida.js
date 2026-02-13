/**
 * Florida Bar Association Scraper
 *
 * Source: https://www.floridabar.org/directories/find-mbr/
 * Method: HTTP GET + Cheerio (results are server-rendered)
 * Emails: Cloudflare XOR-obfuscated, decoded in-scraper
 *
 * Practice area codes discovered from rendered dropdown:
 *   I01 = Immigration, P02 = Personal Injury, F01 = Family,
 *   C16 = Criminal, E10 = Estate Planning, T01 = Tax,
 *   L01 = Labor and Employment, B02 = Bankruptcy, R01 = Real Estate
 */

const https = require('https');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.floridabar.org/directories/find-mbr/';
const PAGE_SIZE = 50; // Max allowed by the site

// Map user-friendly practice area names to Florida Bar codes
const PRACTICE_AREA_CODES = {
  'immigration':       'I01',
  'personal injury':   'P02',
  'family':            'F01',
  'family law':        'F01',
  'criminal':          'C16',
  'criminal defense':  'C16',
  'estate planning':   'E10',
  'estate':            'E10',
  'tax':               'T01',
  'tax law':           'T01',
  'employment':        'L01',
  'labor':             'L01',
  'bankruptcy':        'B02',
  'real estate':       'R01',
  'civil litigation':  'C03',
  'business':          'B04',
  'corporate':         'C15',
  'elder':             'E02',
  'intellectual property': 'I05',
  'medical malpractice':  'M04',
  'workers comp':      'W02',
  'adoption':          'A03',
  'juvenile':          'J02',
  'construction':      'C11',
  'environmental':     'E08',
};

/**
 * Decode Cloudflare email protection.
 * Format: hex string where first 2 chars are XOR key,
 * remaining pairs are email chars XORed with the key.
 */
function decodeCloudflareEmail(encoded) {
  if (!encoded) return '';
  // Strip the /cdn-cgi/l/email-protection# prefix if present
  const hex = encoded.replace(/.*#/, '');
  if (hex.length < 4) return '';

  const key = parseInt(hex.substring(0, 2), 16);
  let email = '';
  for (let i = 2; i < hex.length; i += 2) {
    const charCode = parseInt(hex.substring(i, i + 2), 16) ^ key;
    email += String.fromCharCode(charCode);
  }
  return email;
}

/**
 * Make an HTTPS GET request with custom headers.
 */
function httpGet(url, rateLimiter) {
  return new Promise((resolve, reject) => {
    const ua = rateLimiter.getUserAgent();
    const options = {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
      timeout: 15000,
    };

    const req = https.get(url, options, (res) => {
      // Handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return resolve(httpGet(res.headers.location, rateLimiter));
      }

      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timed out'));
    });
  });
}

/**
 * Decode HTML entities (e.g., &amp; → &, &#39; → ')
 */
function decodeEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)));
}

/**
 * Parse a search results page and extract attorney records.
 */
function parseResultsPage($) {
  const attorneys = [];

  $('li.profile-compact').each((_, el) => {
    const $el = $(el);

    // Name + profile URL
    const nameLink = $el.find('p.profile-name a');
    const fullName = nameLink.text().trim();
    const profileUrl = nameLink.attr('href') || '';
    const barNumMatch = profileUrl.match(/num=(\d+)/);

    // Bar number (also in text)
    const barText = $el.find('p.profile-bar-number').text().trim();
    const barNumber = barNumMatch ? barNumMatch[1] : (barText.match(/\d+/) || [''])[0];

    // Status
    const status = $el.find('.member-status').text().trim();
    const eligibility = $el.find('.eligibility').text().trim();

    // Contact block — firm, address, phone, email
    const contactBlock = $el.find('.profile-contact');
    const contactParagraphs = contactBlock.find('p');

    let firmName = '';
    let address = '';
    let city = '';
    let state = '';
    let zip = '';
    let phone = '';
    let email = '';

    // First <p> in contact block typically has firm + address
    if (contactParagraphs.length > 0) {
      const addressHtml = contactParagraphs.first().html() || '';
      const addressParts = addressHtml.split('<br>').map(s =>
        decodeEntities(s.replace(/<[^>]+>/g, '').trim())
      ).filter(Boolean);

      if (addressParts.length >= 1) firmName = addressParts[0];
      if (addressParts.length >= 3) {
        address = addressParts[1];
        // Last part is usually "City, ST ZIP"
        const cityStateZip = addressParts[addressParts.length - 1];
        const csMatch = cityStateZip.match(/^(.+),\s*([A-Z]{2})\s+([\d-]+)$/);
        if (csMatch) {
          city = csMatch[1].trim();
          state = csMatch[2].trim();
          zip = csMatch[3].trim();
        } else {
          // Fallback: might just be city, state
          const simpleMatch = cityStateZip.match(/^(.+),\s*([A-Z]{2})/);
          if (simpleMatch) {
            city = simpleMatch[1].trim();
            state = simpleMatch[2].trim();
          }
        }
      } else if (addressParts.length === 2) {
        // Might be just firm + city/state
        const csMatch = addressParts[1].match(/^(.+),\s*([A-Z]{2})\s*([\d-]*)/);
        if (csMatch) {
          city = csMatch[1].trim();
          state = csMatch[2].trim();
        }
      }
    }

    // Second <p> typically has phone + email
    if (contactParagraphs.length > 1) {
      const phoneHtml = contactParagraphs.eq(1).html() || '';

      // Phone from tel: link
      const telMatch = phoneHtml.match(/href="tel:([^"]+)"/);
      if (telMatch) phone = telMatch[1];

      // Email — check for Cloudflare protection first
      const cfMatch = phoneHtml.match(/email-protection#([a-f0-9]+)/);
      if (cfMatch) {
        email = decodeCloudflareEmail(cfMatch[1]);
      } else {
        // Check for plain mailto
        const mailtoMatch = phoneHtml.match(/mailto:([^"]+)/);
        if (mailtoMatch) email = mailtoMatch[1];
      }

      // Also check for icon-email with decoded text
      if (!email) {
        const emailEl = $(contactParagraphs[1]).find('a.icon-email');
        if (emailEl.length) {
          const href = emailEl.attr('href') || '';
          if (href.includes('email-protection#')) {
            const hex = href.replace(/.*#/, '');
            email = decodeCloudflareEmail(hex);
          } else if (href.startsWith('mailto:')) {
            email = href.replace('mailto:', '');
          }
        }
      }
    }

    // Certifications
    const certs = $el.find('.profile-certs').text().trim();

    // Split full name into first/last
    const nameParts = fullName.split(/\s+/);
    let firstName = '';
    let lastName = '';
    if (nameParts.length >= 2) {
      firstName = nameParts[0];
      lastName = nameParts[nameParts.length - 1];
    } else if (nameParts.length === 1) {
      lastName = nameParts[0];
    }

    // If firm name looks like an address (starts with digits), it's likely no firm
    if (firmName && /^\d/.test(firmName)) {
      address = firmName + (address ? ', ' + address : '');
      firmName = '';
    }

    attorneys.push({
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city,
      state: state || 'FL',
      phone,
      email,
      website: '',  // Not available from bar search — filled by email-finder
      bar_number: barNumber,
      bar_status: status || eligibility,
      certifications: certs,
      profile_url: profileUrl,
    });
  });

  return attorneys;
}

/**
 * Extract total result count from the page.
 */
function extractResultCount($) {
  const text = $('body').text();
  const match = text.match(/Showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)\s+results/i);
  if (match) return parseInt(match[1].replace(/,/g, ''), 10);
  return 0;
}

/**
 * Build the search URL for the Florida Bar directory.
 */
function buildSearchUrl(options = {}) {
  const params = new URLSearchParams();

  if (options.city) {
    params.set('locType', 'C');
    params.set('locValue', options.city);
  }

  params.set('eligible', 'Y');
  params.set('pageNumber', String(options.page || 1));
  params.set('pageSize', String(PAGE_SIZE));

  if (options.practiceCode) {
    params.set('pracAreas', options.practiceCode);
  }

  return `${BASE_URL}?${params.toString()}`;
}

module.exports = {
  name: 'florida',
  stateCode: 'FL',
  baseUrl: BASE_URL,

  /**
   * Map a user-friendly practice area string to a Florida Bar code.
   * Returns the code or null if not found.
   */
  resolvePracticeCode(practiceArea) {
    if (!practiceArea) return null;
    const key = practiceArea.toLowerCase().trim();
    // Direct match
    if (PRACTICE_AREA_CODES[key]) return PRACTICE_AREA_CODES[key];
    // Partial match
    for (const [name, code] of Object.entries(PRACTICE_AREA_CODES)) {
      if (name.includes(key) || key.includes(name)) return code;
    }
    // If it looks like a code already (e.g., "I01"), pass through
    if (/^[A-Z]\d{2}$/.test(practiceArea)) return practiceArea;
    return null;
  },

  /**
   * Async generator that yields attorney records from the Florida Bar.
   *
   * @param {string} practiceArea - Practice area name or code
   * @param {object} options - { city, minYear, maxPages, proxy }
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" — searching without filter`);
      log.info(`Available areas: ${Object.keys(PRACTICE_AREA_CODES).join(', ')}`);
    }

    // Florida major cities to search (if no specific city given)
    const cities = options.city
      ? [options.city]
      : [
          'Miami', 'Fort Lauderdale', 'West Palm Beach', 'Orlando',
          'Tampa', 'Jacksonville', 'St. Petersburg', 'Naples',
          'Boca Raton', 'Tallahassee', 'Gainesville', 'Sarasota',
          'Fort Myers', 'Daytona Beach', 'Pensacola', 'Coral Gables',
        ];

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, FL`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = buildSearchUrl({
          city,
          practiceCode,
          page,
        });

        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from Florida Bar`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (response.body.includes('captcha') || response.body.includes('CAPTCHA') ||
            response.body.includes('challenge-form')) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = extractResultCount($);
          if (totalResults === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / PAGE_SIZE);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

          // Warn if results seem capped
          if (totalResults === 10000 || totalResults === 5000) {
            log.warn(`Result count ${totalResults} looks capped — you may be missing data for ${city}`);
          }
        }

        const attorneys = parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            log.warn(`2 consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Filter by admission year if specified
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }

          // Add metadata
          attorney.source = `florida_bar`;
          attorney.practice_area = practiceArea || '';

          yield attorney;
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / PAGE_SIZE);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  },

  // Export for testing
  decodeCloudflareEmail,
  parseResultsPage,
  PRACTICE_AREA_CODES,
};
