/**
 * Atlantic Home Warranty (AHWP) — Builder Directory Scraper
 *
 * Source: https://www.ahwp.org/members-directory
 * Platform: Joomla CMS — plain HTML tables, paginated (50 per page)
 * Coverage: Nova Scotia, New Brunswick, Prince Edward Island, Newfoundland
 *
 * HTML structure:
 *   <table>
 *     <thead><tr><th>Name</th><th>City</th><th>Province</th><th>Email</th></tr></thead>
 *     <tbody>
 *       <tr>
 *         <td>Company Name</td>
 *         <td>City</td>
 *         <td>Province</td>
 *         <td><a href="mailto:email@example.com">email@example.com</a></td>
 *       </tr>
 *     </tbody>
 *   </table>
 *
 * Pagination: ?start=0, ?start=50, ?start=100, etc.
 * Province pages: /members-directory/nova-scotia, /members-directory/new-brunswick, etc.
 *
 * ~500+ registered builders across 4 Atlantic provinces.
 * Nearly all entries have mailto: email addresses.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.ahwp.org/members-directory';
const PAGE_SIZE = 50;

// Province slug -> state code mapping
const PROVINCE_MAP = {
  'nova-scotia':           'CA-NS',
  'new-brunswick':         'CA-NB',
  'prince-edward-island':  'CA-PE',
  'newfoundland':          'CA-NL',
};

// Province name (as it appears in table cells) -> state code
const PROVINCE_NAME_MAP = {
  'nova scotia':           'CA-NS',
  'new brunswick':         'CA-NB',
  'prince edward island':  'CA-PE',
  'newfoundland':          'CA-NL',
  'newfoundland and labrador': 'CA-NL',
};

class AtlanticHwScraper extends BaseScraper {
  constructor() {
    super({
      name: 'atlantic-hw',
      stateCode: 'ATLANTIC-HW',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'builder': 'builder',
        'home builder': 'builder',
        'construction': 'builder',
        'renovation': 'builder',
        'contractor': 'builder',
      },
      defaultCities: ['Nova Scotia', 'New Brunswick', 'Prince Edward Island', 'Newfoundland'],
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override transformResult to use 'atlantic_hw_contractors' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'atlantic_hw_contractors';
    lead.practice_area = practiceArea || lead.practice_area || 'Home Builder';
    return lead;
  }

  /**
   * Override CAPTCHA detection — AHWP pages include reCAPTCHA v3 script tags
   * (grecaptcha.execute) which run silently in the background and do not block
   * HTML scraping. The base class detectCaptcha() would false-positive on these.
   */
  detectCaptcha(body) {
    // Strip reCAPTCHA v3 references — they don't block content
    const stripped = body.replace(/grecaptcha/gi, '').replace(/recaptcha/gi, '');
    return stripped.includes('challenge-form');
  }

  /**
   * Map province slug to state code. Falls back to parsing province name from table cell.
   */
  _resolveStateCode(provinceSlug, provinceName) {
    if (provinceSlug && PROVINCE_MAP[provinceSlug]) {
      return PROVINCE_MAP[provinceSlug];
    }
    if (provinceName) {
      const key = provinceName.toLowerCase().trim();
      if (PROVINCE_NAME_MAP[key]) return PROVINCE_NAME_MAP[key];
    }
    return 'CA-NS'; // fallback
  }

  /**
   * Parse the HTML table from a directory page.
   * Returns array of lead objects.
   */
  _parseTable($, provinceSlug) {
    const leads = [];

    // Find the main data table — it has thead with Name/City/Province/Email columns
    $('table').each((_, table) => {
      const $table = $(table);
      const headers = [];
      $table.find('thead th, thead td').each((__, th) => {
        headers.push($(th).text().trim().toLowerCase());
      });

      // Verify this is the members table (must have name + email columns)
      const hasName = headers.some(h => h.includes('name'));
      const hasEmail = headers.some(h => h.includes('email'));
      if (!hasName && !hasEmail) return;

      // Determine column indices
      const nameIdx = headers.findIndex(h => h.includes('name'));
      const cityIdx = headers.findIndex(h => h.includes('city'));
      const provIdx = headers.findIndex(h => h.includes('province'));
      const emailIdx = headers.findIndex(h => h.includes('email'));

      $table.find('tbody tr').each((__, row) => {
        const cells = $(row).find('td');
        if (cells.length < 2) return;

        const firmName = $(cells[nameIdx >= 0 ? nameIdx : 0]).text().trim();
        if (!firmName) return;

        const city = cityIdx >= 0 ? $(cells[cityIdx]).text().trim() : '';
        const provinceName = provIdx >= 0 ? $(cells[provIdx]).text().trim() : '';
        const stateCode = this._resolveStateCode(provinceSlug, provinceName);

        // Extract email from mailto: link or plain text
        let email = '';
        const emailCell = emailIdx >= 0 ? $(cells[emailIdx]) : null;
        if (emailCell) {
          const mailtoLink = emailCell.find('a[href^="mailto:"]');
          if (mailtoLink.length) {
            email = mailtoLink.attr('href').replace(/^mailto:/i, '').trim();
          }
          if (!email) {
            email = emailCell.text().trim();
          }
          // Validate email shape
          if (email && !email.includes('@')) {
            email = '';
          }
        }

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          city: city,
          state: stateCode,
          country: 'CA',
          email: email.toLowerCase(),
          phone: '',
          website: '',
          bar_number: '',
          bar_status: 'Registered',
        });
      });
    });

    return leads;
  }

  /**
   * Detect pagination from the page — find the highest start= value.
   * Joomla pagination uses ?start=0, ?start=50, ?start=100, etc.
   * Returns the max start offset found, or 0 if no pagination.
   */
  _detectMaxStart($) {
    let maxStart = 0;

    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const match = href.match(/[?&]start=(\d+)/);
      if (match) {
        const val = parseInt(match[1], 10);
        if (val > maxStart) maxStart = val;
      }
    });

    return maxStart;
  }

  /**
   * Convert a province defaultCity name to a URL slug.
   */
  _provinceSlug(province) {
    return province.toLowerCase().replace(/\s+/g, '-');
  }

  /**
   * Main search generator — iterates provinces, handles pagination, yields leads.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const seen = new Set(); // dedup by firm_name+city+email

    // Determine which provinces to scrape
    let provinces = options.city ? [options.city] : this.defaultCities;
    if (options.maxCities) {
      provinces = provinces.slice(0, options.maxCities);
    }

    let totalYielded = 0;

    for (let pi = 0; pi < provinces.length; pi++) {
      const province = provinces[pi];
      const slug = this._provinceSlug(province);

      yield { _cityProgress: { current: pi + 1, total: provinces.length } };
      log.scrape(`AHWP: Scraping ${province} builders`);

      let pageStart = 0;
      let pagesFetched = 0;
      let provinceTotal = 0;

      while (true) {
        // Test mode page limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${province}`);
          break;
        }

        const url = pageStart === 0
          ? `${BASE_URL}/${slug}`
          : `${BASE_URL}/${slug}?start=${pageStart}`;

        log.info(`Page ${pagesFetched + 1} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${province}: ${err.message}`);
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from AHWP — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for ${province}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on AHWP — skipping`);
          yield { _captcha: true, city: province, reason: 'CAPTCHA detected' };
          break;
        }

        const $ = cheerio.load(response.body);
        const leads = this._parseTable($, slug);

        if (leads.length === 0) {
          if (pagesFetched === 0) {
            log.info(`No builder entries found for ${province}`);
          } else {
            log.info(`No more entries on page ${pagesFetched + 1} for ${province}`);
          }
          break;
        }

        // Detect max pagination on first page
        let maxStart = 0;
        if (pagesFetched === 0) {
          maxStart = this._detectMaxStart($);
          const estimatedTotal = maxStart + PAGE_SIZE;
          log.success(`Found ~${estimatedTotal} builders for ${province} (${Math.ceil(estimatedTotal / PAGE_SIZE)} pages)`);
        }

        // Yield leads with dedup
        for (const lead of leads) {
          const key = `${lead.firm_name}|${lead.city}|${lead.email}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);

          yield this.transformResult(lead, practiceArea);
          totalYielded++;
          provinceTotal++;
        }

        pagesFetched++;

        // Check if there are more pages
        // If we got fewer than PAGE_SIZE results, we're on the last page
        if (leads.length < PAGE_SIZE) {
          log.success(`Completed ${province}: ${provinceTotal} builders (${pagesFetched} pages)`);
          break;
        }

        pageStart += PAGE_SIZE;

        // Safety: if we detected maxStart on page 1, stop when we've passed it
        if (maxStart > 0 && pageStart > maxStart) {
          log.success(`Completed ${province}: ${provinceTotal} builders (${pagesFetched} pages)`);
          break;
        }
      }
    }

    log.success(`AHWP scrape complete: ${totalYielded} total builders across ${provinces.length} provinces`);
  }
}

module.exports = new AtlanticHwScraper();
