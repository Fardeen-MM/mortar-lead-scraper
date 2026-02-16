/**
 * Singapore Law Society Scraper
 *
 * Source: https://www.lawsociety.org.sg/find-a-featured-lawyer-law-firm/
 * Method: WordPress REST API (JSON) + HTML content parsing
 *
 * The Law Society of Singapore uses WordPress with a custom "firm" post type.
 * The WP REST API at /wp-json/wp/v2/firm exposes all featured firm listings
 * with full HTML content that includes firm name, address, phone, email,
 * website, practice areas, languages, and individual lawyer bios.
 *
 * NOTE: This is the "Featured Lawyer/Law Firm" directory -- a paid/advertising
 * listing. It is NOT a complete register of all Singapore lawyers. As of
 * Feb 2026 there are ~74 firm entries. For a complete register, the official
 * source is https://eservices.mlaw.gov.sg/lsra/search-lawyer-or-law-practice/
 * which requires JavaScript rendering and is not HTTP-scrapable.
 *
 * Strategy:
 *   1. Fetch all firms via WP REST API (paginated, ~74 total)
 *   2. Parse the Elementor HTML content for each firm to extract:
 *      - Firm name (from title or h1)
 *      - Contact info (address, phone, email, website)
 *      - Practice areas (from firm_category taxonomy)
 *      - Individual lawyer names (from h3 headings or bio sections)
 *   3. Yield one record per firm (with individual lawyers noted)
 *
 * Taxonomy endpoints also available:
 *   /wp-json/wp/v2/firm_category  (practice areas)
 *   /wp-json/wp/v2/location       (Central, East, North, South, West)
 *   /wp-json/wp/v2/language        (spoken languages)
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const https = require('https');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class SingaporeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'singapore',
      stateCode: 'SG',
      baseUrl: 'https://www.lawsociety.org.sg',
      pageSize: 100, // WP REST API max per_page
      practiceAreaCodes: {
        'accident':                     39,
        'personal injury':              39,
        'admiralty':                     40,
        'shipping':                     40,
        'arbitration':                  41,
        'dispute resolution':           41,
        'banking':                      42,
        'finance':                      42,
        'bankruptcy':                   43,
        'insolvency':                   43,
        'building':                     44,
        'construction':                 44,
        'civil litigation':             45,
        'litigation':                   45,
        'conveyancing':                 46,
        'landlord':                     46,
        'tenant':                       46,
        'corporate':                    48,
        'commercial':                   48,
        'criminal':                     49,
        'criminal law':                 49,
        'divorce':                      50,
        'family':                       50,
        'family law':                   50,
        'employment':                   51,
        'employment law':               51,
        'funds':                        52,
        'asset management':             52,
        'intellectual property':        53,
        'ip':                           53,
        'islamic law':                  54,
        'media':                        56,
        'internet':                     56,
        'it':                           56,
        'medical negligence':           57,
        'personal data':                58,
        'data protection':              58,
        'trusts':                       59,
        'wills':                        59,
        'probate':                      59,
      },
      defaultCities: ['Singapore'],
    });

    // Cache for taxonomy lookups
    this._categoryCache = null;
  }

  /**
   * Build the WP REST API URL for fetching firms.
   */
  buildSearchUrl({ practiceCode, page }) {
    const params = new URLSearchParams();
    params.set('per_page', String(this.pageSize));
    params.set('page', String(page || 1));
    if (practiceCode) {
      params.set('firm_category', String(practiceCode));
    }
    return `${this.baseUrl}/wp-json/wp/v2/firm?${params.toString()}`;
  }

  /**
   * Fetch JSON from a URL (adapted for WP REST API).
   */
  fetchJson(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
        },
        timeout: 15000,
      };

      const req = https.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return resolve(this.fetchJson(res.headers.location, rateLimiter));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            body: data,
            headers: {
              total: parseInt(res.headers['x-wp-total'] || '0', 10),
              totalPages: parseInt(res.headers['x-wp-totalpages'] || '0', 10),
            },
          });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Fetch and cache the firm_category taxonomy for ID-to-name resolution.
   */
  async fetchCategories(rateLimiter) {
    if (this._categoryCache) return this._categoryCache;

    const url = `${this.baseUrl}/wp-json/wp/v2/firm_category?per_page=100`;
    try {
      await rateLimiter.wait();
      const response = await this.fetchJson(url, rateLimiter);
      if (response.statusCode === 200) {
        const categories = JSON.parse(response.body);
        this._categoryCache = {};
        for (const cat of categories) {
          this._categoryCache[cat.id] = cat.name
            .replace(/&amp;/g, '&')
            .replace(/&#8211;/g, '-')
            .replace(/&#8217;/g, "'");
        }
      }
    } catch (err) {
      log.warn(`SG: Failed to fetch categories: ${err.message}`);
    }
    return this._categoryCache || {};
  }

  /**
   * Parse a single firm's Elementor HTML content to extract structured data.
   */
  parseFirmContent(htmlContent) {
    const $ = cheerio.load(htmlContent);
    const result = {
      address: '',
      phone: '',
      email: '',
      website: '',
      lawyers: [],
    };

    // Extract firm name from h1
    const h1 = $('h1').first().text().trim();
    if (h1) result.firm_name = this.decodeEntities(h1);

    // Extract contact information from icon list items (tel: and mailto: links)
    $('a[href^="tel:"]').each((_, el) => {
      if (!result.phone) {
        result.phone = decodeURIComponent($(el).attr('href').replace('tel:', '')).trim();
      }
    });

    $('a[href^="mailto:"]').each((_, el) => {
      if (!result.email) {
        result.email = $(el).attr('href').replace('mailto:', '').trim();
      }
    });

    // Extract website (external links that are not lawsociety.org.sg)
    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (!result.website &&
          !href.includes('lawsociety.org.sg') &&
          !href.includes('mailto:') &&
          !href.includes('tel:')) {
        result.website = href;
      }
    });

    // Extract address -- typically in a standalone div/p element near Contact Information.
    // Singapore addresses follow the pattern: <street/building>, Singapore <6-digit postal>
    // We look for short text blocks (not paragraphs of prose) containing this pattern.
    $('div, p, span, li').each((_, el) => {
      if (result.address) return;
      const $thisEl = $(el);
      // Skip elements with many children (likely containers, not address text)
      if ($thisEl.children().length > 3) return;
      const text = $thisEl.text().replace(/[\t\n\r]+/g, ' ').replace(/\s+/g, ' ').trim();
      // Address text should be short (< 150 chars) and contain "Singapore" + postal code
      if (text.length > 10 && text.length < 150 && /Singapore\s+\d{6}/.test(text)) {
        // Clean up: trim to just the address portion
        const addrMatch = text.match(/(\d+[A-Z]?\s+[^,]+(?:,\s*[^,]+)*,?\s*Singapore\s+\d{6})/);
        if (addrMatch) {
          result.address = addrMatch[1].replace(/\s+/g, ' ').trim();
        } else {
          // Fallback: use the whole text if it's short enough
          result.address = text;
        }
      }
    });

    // Extract individual lawyer names from the "Our Lawyers" section.
    //
    // The page uses H2 headings for everything: section titles, lawyer names,
    // and role/title headings. The pattern is:
    //   H2: "Our Lawyers"       <-- triggers lawyer section
    //   H2: "Julian Tay"        <-- lawyer name
    //   H2: "Managing Partner"  <-- role title (skip)
    //   H2: "Matthew Saw"       <-- lawyer name
    //   H2: "Partner"           <-- role title (skip)
    //   H2: "Languages"         <-- ends lawyer section
    //
    // We walk all H2 headings, collect text between "Our Lawyers" and the
    // next section heading, then filter out role/title entries.

    const sectionHeadings = ['about', 'contact', 'language', 'area of practice',
      'our lawyer', 'our team', 'overview', 'practice area', 'services',
      'accreditation', 'membership', 'mediation', 'arbitration', 'dispute',
      'related', 'mental capacity', 'qualification', 'certification',
      'testimonial', 'achievement', 'award', 'recognition'];

    // Role/title keywords -- these H2 entries are not names
    const roleKeywords = [
      'director', 'partner', 'associate', 'counsel', 'founder',
      'managing', 'senior', 'deputy', 'head of', 'consultant',
      'former', 'judge', 'solicitor', 'lawyer', 'mediator',
      'co-managing',
    ];

    // Non-name keywords that should disqualify an H2 from being a lawyer name
    const nonNameKeywords = [
      'year of', 'areas of', 'qualification', 'research',
      'contact', ' law', ' and ', ' related', ' resolution',
      'accreditation', 'membership', 'certification',
    ];

    let inLawyerSection = false;

    $('h2').each((_, el) => {
      const text = $(el).text().replace(/[\t\n\r]+/g, ' ').replace(/\s+/g, ' ').trim();
      const lower = text.toLowerCase();

      // Check if this H2 starts the "Our Lawyers" section
      if (lower.includes('our lawyer') || lower.includes('our team')) {
        inLawyerSection = true;
        return;
      }

      // Check if this H2 ends the lawyer section (a known section heading)
      if (inLawyerSection && sectionHeadings.some(h => lower.includes(h))) {
        inLawyerSection = false;
        return;
      }

      if (!inLawyerSection) return;

      // Skip if it looks like a role/title heading
      if (roleKeywords.some(r => lower.includes(r))) return;

      // Skip if it contains non-name keywords
      if (nonNameKeywords.some(k => lower.includes(k))) return;

      // A valid name should be 2+ chars, start with a letter, and have at least 2 words
      // (allowing for single-word Chinese names written in English)
      if (text.length < 3 || text.length > 80) return;
      if (!/^[A-Z]/.test(text)) return;
      if (text.split(/\s+/).length < 2) return;

      result.lawyers.push(text);
    });

    return result;
  }

  /**
   * Not used -- we override search() entirely.
   */
  parseResultsPage() { return []; }

  /**
   * Not used -- we get totals from WP response headers.
   */
  extractResultCount() { return 0; }

  /**
   * Override the main search generator.
   * Fetches all firms from the WP REST API and yields structured records.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`SG: Unknown practice area "${practiceArea}" -- searching without filter`);
      log.info(`SG: Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    yield { _cityProgress: { current: 1, total: 1 } };
    log.scrape(`SG: Searching ${practiceArea || 'all'} featured lawyers/firms in Singapore`);

    // Fetch categories for name resolution
    const categories = await this.fetchCategories(rateLimiter);

    let page = 1;
    let totalFirms = 0;
    let pagesFetched = 0;

    // Deduplicate: same firm appears under multiple practice area categories.
    // Track seen firm+lawyer combinations to avoid duplicate records.
    const seen = new Set();

    while (true) {
      // Check max pages limit
      if (options.maxPages && pagesFetched >= options.maxPages) {
        log.info(`SG: Reached max pages limit (${options.maxPages})`);
        break;
      }

      const url = this.buildSearchUrl({ practiceCode, page });
      log.info(`SG: API page ${page} -- ${url}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this.fetchJson(url, rateLimiter);
      } catch (err) {
        log.error(`SG: Request failed: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) continue;
        break;
      }

      // Handle rate limiting
      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`SG: Got ${response.statusCode}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (shouldRetry) continue;
        break;
      }

      if (response.statusCode !== 200) {
        log.error(`SG: Unexpected status ${response.statusCode} -- skipping`);
        break;
      }

      rateLimiter.resetBackoff();

      // Parse JSON response
      let firms;
      try {
        firms = JSON.parse(response.body);
      } catch (err) {
        log.error(`SG: Failed to parse JSON: ${err.message}`);
        break;
      }

      if (page === 1) {
        totalFirms = response.headers.total;
        const totalPages = response.headers.totalPages;
        log.success(`SG: Found ${totalFirms} featured firms (${totalPages} pages)`);
        if (totalFirms === 0) break;
      }

      if (!firms.length) {
        log.info(`SG: No more firms on page ${page}`);
        break;
      }

      // Process each firm
      for (const firm of firms) {
        const title = this.decodeEntities(
          (firm.title?.rendered || '')
            .replace(/&#8211;/g, '-')
            .replace(/&#8217;/g, "'")
            .replace(/&amp;/g, '&')
        );

        // Parse HTML content for detailed info
        const content = firm.content?.rendered || '';
        const parsed = this.parseFirmContent(content);

        // Resolve practice area categories
        const categoryIds = firm.firm_category || [];
        const categoryNames = categoryIds
          .map(id => categories[id] || '')
          .filter(Boolean);

        const firmName = parsed.firm_name || title.split(/\s*[-\u2013]\s*/)[0].trim();
        const profileUrl = firm.link || '';

        // If we found individual lawyers, yield one record per lawyer
        if (parsed.lawyers.length > 0) {
          for (const lawyerName of parsed.lawyers) {
            // Deduplicate by firm+lawyer combo (same lawyer appears in multiple
            // practice area listings for the same firm)
            const dedupeKey = `${firmName}|${lawyerName}`;
            if (seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);

            const { firstName, lastName } = this.splitName(lawyerName);

            yield this.transformResult({
              first_name: firstName,
              last_name: lastName,
              full_name: lawyerName,
              firm_name: firmName,
              city: 'Singapore',
              state: 'SG',
              phone: parsed.phone,
              email: parsed.email,
              website: parsed.website,
              bar_number: '',
              bar_status: '',
              practice_areas_list: categoryNames.join('; '),
              address: parsed.address,
              profile_url: profileUrl,
            }, practiceArea);
          }
        } else {
          // No individual lawyers found -- skip this entry.
          // Firm-only records without person names are not useful as leads.
          log.info(`SG: No individual lawyers found for "${firmName}" -- skipping`);
        }
      }

      // Check if we've fetched all pages
      const totalPages = response.headers.totalPages;
      if (page >= totalPages) {
        log.success(`SG: Completed all ${totalPages} pages`);
        break;
      }

      page++;
      pagesFetched++;
    }

    log.info(`SG: Note -- This scrapes the "Featured" directory only (~74 firms).`);
    log.info(`SG: For the complete lawyer register, see https://eservices.mlaw.gov.sg/lsra/search-lawyer-or-law-practice/`);
    log.info(`SG: The MLAW e-services portal requires JavaScript rendering and cannot be scraped via HTTP-only.`);
  }
}

module.exports = new SingaporeScraper();
