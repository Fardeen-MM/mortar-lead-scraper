/**
 * Hong Kong Law Society Scraper
 *
 * Source: https://www.hklawsoc.org.hk/en/Serve-the-Public/The-Law-List
 * Method: HTTP GET + Cheerio (server-rendered HTML with pageIndex pagination)
 *
 * The Law Society of Hong Kong publishes a public "Law List" containing:
 *   - Members with Practising Certificate (~11,690 solicitors)
 *   - Hong Kong Law Firms (~924 firms)
 *
 * Strategy — Two-phase scrape:
 *   Phase 1: Paginate through the "Members with Practising Certificate" listing
 *            at 30 members per page. Each row has: name (English), name (Chinese),
 *            and a link to /Member-Details?MemId={id}.
 *   Phase 2: For each member, fetch the detail page to extract:
 *            - Full name, admission date, practising status
 *            - Personal email (if listed)
 *            - Firm name, firm address, firm phone, firm fax, firm email
 *            - Post/position (e.g. Partner, Consultant, Associate)
 *            - Admissions in other jurisdictions
 *
 * Listing URL pattern:
 *   /en/serve-the-public/the-law-list/members-with-practising-certificate
 *     ?pageIndex={page}&sort=1&name={name}&jurisdictionId={id}
 *
 * Detail URL pattern:
 *   /en/Serve-the-Public/The-Law-List/Member-Details?MemId={id}
 *
 * Pagination: 30 results per page, ~390 total pages for the full list.
 * Total solicitors: ~11,690 (as of Feb 2026).
 *
 * Note: Hong Kong does not have distinct "cities" in the traditional sense.
 * The three main regions (Hong Kong Island, Kowloon, New Territories) are used
 * as defaultCities for interface consistency, but the scraper iterates through
 * the full alphabetical listing rather than filtering by location (the listing
 * does not support location-based filtering for individual members).
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class HongKongScraper extends BaseScraper {
  constructor() {
    super({
      name: 'hong-kong',
      stateCode: 'HK',
      baseUrl: 'https://www.hklawsoc.org.hk',
      pageSize: 30, // The listing returns 30 results per page
      practiceAreaCodes: {
        // The HK Law Society does not filter members by practice area
        // in their public listing. These codes are kept for interface
        // consistency but are not used in search URLs.
        'corporate': 'Corporate',
        'banking': 'Banking',
        'litigation': 'Litigation',
        'family': 'Family',
        'criminal': 'Criminal',
        'intellectual property': 'Intellectual Property',
        'real estate': 'Real Estate',
        'property': 'Property',
        'maritime': 'Maritime',
        'arbitration': 'Arbitration',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'tax': 'Tax',
        'insurance': 'Insurance',
      },
      defaultCities: ['Hong Kong', 'Kowloon', 'New Territories'],
    });

    this.membersListUrl = `${this.baseUrl}/en/serve-the-public/the-law-list/members-with-practising-certificate`;
    this.memberDetailUrl = `${this.baseUrl}/en/Serve-the-Public/The-Law-List/Member-Details`;
  }

  // --- BaseScraper overrides (required but not used since search() is overridden) ---

  buildSearchUrl({ page }) {
    const params = new URLSearchParams();
    params.set('pageIndex', String(page || 1));
    params.set('sort', '1'); // Sort by English name ascending
    return `${this.membersListUrl}?${params.toString()}`;
  }

  parseResultsPage() { return []; }
  extractResultCount() { return 0; }

  // --- HTML parsing helpers ---

  /**
   * Parse the members listing page to extract member names and MemIds.
   *
   * Each row in the table contains:
   *   <tr>
   *     <td>1</td>
   *     <td><a href='...Member-Details?MemId=6726'>ABATE DUNCAN ARTHUR WILLIAM</a></td>
   *     <td><a href='...Member-Details?MemId=6726'>石韻怡</a></td>
   *   </tr>
   *
   * @param {string} html - Raw HTML of the listing page
   * @returns {{ members: Array<{memId: string, nameEn: string, nameCn: string}>, totalRecords: number }}
   */
  parseListingPage(html) {
    const $ = cheerio.load(html);
    const members = [];

    // Extract total record count from "Showing 1 - 30 of 11690 Records"
    let totalRecords = 0;
    const showingText = $('body').text();
    const countMatch = showingText.match(/Showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)\s+Records/i);
    if (countMatch) {
      totalRecords = parseInt(countMatch[1].replace(/,/g, ''), 10);
    }

    // Parse table rows containing member links
    $('tr').each((_, tr) => {
      const $tr = $(tr);
      const tds = $tr.find('td');
      if (tds.length < 3) return;

      // Second td has the English name link
      const $nameLink = $(tds[1]).find('a[href*="MemId"]');
      if (!$nameLink.length) return;

      const href = $nameLink.attr('href') || '';
      const memIdMatch = href.match(/MemId=(\d+)/);
      if (!memIdMatch) return;

      const memId = memIdMatch[1];
      const nameEn = $nameLink.text().trim();

      // Third td has the Chinese name
      const $cnLink = $(tds[2]).find('a');
      const nameCn = $cnLink.length ? $cnLink.text().trim() : '';

      if (nameEn) {
        members.push({ memId, nameEn, nameCn });
      }
    });

    return { members, totalRecords };
  }

  /**
   * Parse a member detail page to extract full solicitor information.
   *
   * The detail page uses a table with rows like:
   *   <tr><td>Name (English)</td><td>ABATE DUNCAN ARTHUR WILLIAM</td></tr>
   *   <tr><td>Admission in Hong Kong</td><td>12/1994</td></tr>
   *   <tr><td>E-mail</td><td><a href='mailto:...'>...</a></td></tr>
   *   ...
   *   <tr><th>Firm</th></tr>
   *   <tr><td>Post</td><td>Consultant</td></tr>
   *   <tr><td>Firm/Company (English)</td><td>...</td></tr>
   *   ...
   *
   * @param {string} html - Raw HTML of the detail page
   * @returns {object} Extracted solicitor details
   */
  parseMemberDetail(html) {
    const $ = cheerio.load(html);
    const details = {
      name_en: '',
      name_cn: '',
      admission_hk: '',
      remark: '',
      personal_email: '',
      other_jurisdictions: [],
      post: '',
      firm_name_en: '',
      firm_name_cn: '',
      firm_address_en: '',
      firm_address_cn: '',
      firm_phone: '',
      firm_fax: '',
      firm_email: '',
      firm_id: '',
    };

    let inFirmSection = false;

    $('table tr').each((_, tr) => {
      const $tr = $(tr);

      // Check for the "Firm" section header
      const thText = $tr.find('th').text().trim();
      if (thText === 'Firm') {
        inFirmSection = true;
        return;
      }

      // Check for "Details of ..." header — reset firm section
      if (thText.includes('Details of')) {
        inFirmSection = false;
        return;
      }

      const tds = $tr.find('td');
      if (tds.length < 2) return;

      const label = $(tds[0]).text().replace(/\s+/g, ' ').trim();
      const valueTd = $(tds[1]);
      const valueText = valueTd.text().replace(/\s+/g, ' ').trim();
      const valueHtml = valueTd.html() || '';

      if (!inFirmSection) {
        // Member personal details section
        if (label === 'Name (English)') {
          details.name_en = valueText;
        } else if (label === 'Name (Chinese)') {
          details.name_cn = valueText;
        } else if (label === 'Admission in Hong Kong') {
          details.admission_hk = valueText;
        } else if (label === 'Remark') {
          details.remark = valueText;
        } else if (label === 'E-mail') {
          const mailMatch = valueHtml.match(/mailto:([^'"]+)/);
          if (mailMatch) details.personal_email = mailMatch[1].trim();
        }
      } else {
        // Firm section
        if (label === 'Post') {
          // Post is wrapped in a <lable> tag (sic - typo in the source HTML)
          details.post = valueTd.find('lable, label').text().trim() || valueText;
        } else if (label.includes('Firm/Company (English)')) {
          details.firm_name_en = valueText;
          // Extract FirmId from the link
          const firmLink = valueTd.find('a[href*="FirmId"]').attr('href') || '';
          const firmIdMatch = firmLink.match(/FirmId=(\d+)/);
          if (firmIdMatch) details.firm_id = firmIdMatch[1];
        } else if (label.includes('Firm/Company (Chinese)')) {
          details.firm_name_cn = valueText;
        } else if (label === 'Address (English)') {
          details.firm_address_en = valueText;
        } else if (label === 'Address (Chinese)') {
          details.firm_address_cn = valueText;
        } else if (label === 'Telephone') {
          details.firm_phone = valueText;
        } else if (label === 'Fax') {
          details.firm_fax = valueText;
        } else if (label === 'E-mail') {
          const mailMatch = valueHtml.match(/mailto:([^'"]+)/);
          if (mailMatch) details.firm_email = mailMatch[1].trim();
        }
      }
    });

    return details;
  }

  /**
   * Parse a HK Law Society member detail page for additional fields.
   *
   * The detail page at /en/Serve-the-Public/The-Law-List/Member-Details?MemId={id}
   * contains a table with personal details (name, admission date, email) and
   * firm details (post, firm name, address, phone, fax, email).
   *
   * This method delegates to parseMemberDetail() and maps the result to the
   * standard field names used by the waterfall enrichment pipeline.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the detail page
   * @returns {object} Additional fields: { phone, email, website, firm_name,
   *   bar_status, admission_date, address, fax, position }
   */
  parseProfilePage($) {
    // parseMemberDetail expects raw HTML, but we have a Cheerio instance.
    // Re-serialize the HTML and pass it through.
    const html = $.html();
    const detail = this.parseMemberDetail(html);
    const result = {};

    // Map parseMemberDetail fields to standard enrichment fields
    if (detail.firm_phone) result.phone = detail.firm_phone;
    if (detail.personal_email || detail.firm_email) {
      result.email = detail.personal_email || detail.firm_email;
    }
    if (detail.firm_name_en) result.firm_name = detail.firm_name_en;
    if (detail.firm_address_en) result.address = detail.firm_address_en;
    if (detail.firm_fax) result.fax = detail.firm_fax;
    if (detail.admission_hk) result.admission_date = detail.admission_hk;
    if (detail.post) result.position = detail.post;
    if (detail.remark) result.bar_status = detail.remark;
    if (detail.name_cn) result.name_chinese = detail.name_cn;

    // Infer city from the firm address
    if (detail.firm_address_en) {
      const city = this._inferRegion(detail.firm_address_en);
      if (city) result.city = city;
    }

    // Remove empty values
    for (const key of Object.keys(result)) {
      if (!result[key]) delete result[key];
    }

    return result;
  }

  /**
   * Parse a full name in "LAST FIRST MIDDLE" format (all uppercase) into components.
   * HK Law Society lists names as: SURNAME GIVEN_NAMES (e.g., "ABATE DUNCAN ARTHUR WILLIAM")
   *
   * @param {string} fullName - e.g. "CHAN TAI MAN"
   * @returns {{ firstName: string, lastName: string }}
   */
  _parseHkName(fullName) {
    if (!fullName) return { firstName: '', lastName: '' };
    const parts = fullName.trim().split(/\s+/);
    if (parts.length >= 2) {
      // First token is the surname, rest is given name
      const lastName = this._titleCase(parts[0]);
      const firstName = parts.slice(1).map(p => this._titleCase(p)).join(' ');
      return { firstName, lastName };
    }
    if (parts.length === 1) {
      return { firstName: '', lastName: this._titleCase(parts[0]) };
    }
    return { firstName: '', lastName: '' };
  }

  /**
   * Convert an UPPERCASE string to Title Case.
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  /**
   * Infer the HK region from an address string.
   * Returns 'Hong Kong', 'Kowloon', or 'New Territories' (or '' if unknown).
   */
  _inferRegion(address) {
    if (!address) return '';
    const upper = address.toUpperCase();

    // Hong Kong Island districts
    const hkIsland = [
      'CENTRAL', 'ADMIRALTY', 'WAN CHAI', 'WANCHAI', 'CAUSEWAY BAY',
      'NORTH POINT', 'QUARRY BAY', 'TAI KOO', 'SHAU KEI WAN',
      'CHAI WAN', 'ABERDEEN', 'AP LEI CHAU', 'HAPPY VALLEY',
      'SHEUNG WAN', 'SAI YING PUN', 'KENNEDY TOWN', 'MID-LEVELS',
      'THE PEAK', 'REPULSE BAY', 'STANLEY', 'POKFULAM',
      'HONG KONG', // generic
    ];
    if (hkIsland.some(d => upper.includes(d))) return 'Hong Kong';

    // Kowloon districts
    const kowloon = [
      'TSIM SHA TSUI', 'JORDAN', 'YAU MA TEI', 'MONG KOK',
      'PRINCE EDWARD', 'SHAM SHUI PO', 'CHEUNG SHA WAN',
      'KOWLOON BAY', 'KWUN TONG', 'HUNG HOM', 'TO KWA WAN',
      'KOWLOON TONG', 'KOWLOON CITY', 'DIAMOND HILL', 'WONG TAI SIN',
      'SAN PO KONG', 'LAI CHI KOK', 'MEI FOO', 'NGAU TAU KOK',
      'KOWLOON', // generic
    ];
    if (kowloon.some(d => upper.includes(d))) return 'Kowloon';

    // New Territories districts
    const nt = [
      'SHA TIN', 'SHATIN', 'TAI PO', 'FANLING', 'SHEUNG SHUI',
      'YUEN LONG', 'TUEN MUN', 'TSUEN WAN', 'KWAI CHUNG',
      'TSING YI', 'TUNG CHUNG', 'LANTAU', 'MA ON SHAN',
      'SAI KUNG', 'TSEUNG KWAN O', 'TIN SHUI WAI',
      'NEW TERRITORIES', // generic
    ];
    if (nt.some(d => upper.includes(d))) return 'New Territories';

    return '';
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields solicitor records from the HK Law Society.
   *
   * Strategy:
   * 1. Paginate through the members listing to collect MemIds and names.
   * 2. For each member, fetch the detail page to extract full contact information.
   * 3. Yield standardised attorney records.
   *
   * The HK Law Society listing does not support location or practice area filtering
   * for individual members, so we iterate through the full alphabetical list.
   * The "city" iteration is skipped — we yield a single city progress event.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    if (practiceArea) {
      log.warn(`HK: The Law Society of Hong Kong does not support practice area filtering in member listings`);
      log.info(`HK: Will fetch all members and return results unfiltered`);
    }

    yield { _cityProgress: { current: 1, total: 1 } };
    log.scrape(`HK: Searching all practising solicitors in Hong Kong`);

    let page = 1;
    let totalRecords = 0;
    let pagesFetched = 0;
    let consecutiveEmpty = 0;
    const seenIds = new Set();
    let totalDetailFetches = 0;
    const maxDetailFetches = options.maxPages ? 10 : Infinity; // Limit detail fetches in test mode

    while (true) {
      // Check max pages limit (--test flag sets this to 2)
      if (options.maxPages && pagesFetched >= options.maxPages) {
        log.info(`HK: Reached max pages limit (${options.maxPages})`);
        break;
      }

      const url = this.buildSearchUrl({ page });
      log.info(`HK: Listing page ${page} -- ${url}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpGet(url, rateLimiter);
      } catch (err) {
        log.error(`HK: Request failed: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) continue;
        break;
      }

      // Handle rate limiting / blocking
      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`HK: Got ${response.statusCode}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (shouldRetry) continue;
        break;
      }

      if (response.statusCode !== 200) {
        log.error(`HK: Unexpected status ${response.statusCode} -- skipping`);
        break;
      }

      rateLimiter.resetBackoff();

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`HK: CAPTCHA detected on listing page ${page}`);
        yield { _captcha: true, city: 'Hong Kong', page };
        break;
      }

      // Parse the listing page
      const { members, totalRecords: total } = this.parseListingPage(response.body);

      if (page === 1) {
        totalRecords = total;
        if (totalRecords === 0 || members.length === 0) {
          log.info(`HK: No members found on listing`);
          break;
        }
        const totalPages = Math.ceil(totalRecords / this.pageSize);
        log.success(`HK: Found ${totalRecords.toLocaleString()} practising solicitors (${totalPages} pages)`);
      }

      if (members.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
          log.warn(`HK: ${this.maxConsecutiveEmpty} consecutive empty pages -- stopping`);
          break;
        }
        page++;
        pagesFetched++;
        continue;
      }

      consecutiveEmpty = 0;

      // Process each member: fetch detail page for full record
      for (const member of members) {
        if (seenIds.has(member.memId)) continue;
        seenIds.add(member.memId);

        // Fetch full detail record (skip if over limit in test mode)
        let detail = null;
        const detailUrl = `${this.memberDetailUrl}?MemId=${member.memId}`;

        if (totalDetailFetches >= maxDetailFetches) {
          // In test mode, skip detail fetch but still yield basic record
          const attorney = this._buildAttorneyRecord(member, null);
          yield this.transformResult(attorney, practiceArea);
          continue;
        }

        try {
          await sleep(1500 + Math.random() * 3000); // 1.5-4.5s delay between detail fetches
          totalDetailFetches++;
          const detailResp = await this.httpGet(detailUrl, rateLimiter);

          if (detailResp.statusCode === 200) {
            if (!this.detectCaptcha(detailResp.body)) {
              detail = this.parseMemberDetail(detailResp.body);
            } else {
              log.warn(`HK: CAPTCHA on detail page for MemId ${member.memId}`);
            }
          } else if (detailResp.statusCode === 429 || detailResp.statusCode === 403) {
            log.warn(`HK: Rate limited on detail fetch for MemId ${member.memId}`);
            await rateLimiter.handleBlock(detailResp.statusCode);
          } else {
            log.warn(`HK: Status ${detailResp.statusCode} on detail for MemId ${member.memId}`);
          }
        } catch (err) {
          log.warn(`HK: Detail fetch failed for MemId ${member.memId}: ${err.message}`);
        }

        const attorney = this._buildAttorneyRecord(member, detail);

        // Apply admission year filter if specified
        if (options.minYear && attorney.admission_date) {
          const yearMatch = attorney.admission_date.match(/\d{4}/);
          if (yearMatch) {
            const year = parseInt(yearMatch[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
        }

        yield this.transformResult(attorney, practiceArea);
      }

      // Check if we've reached the last page
      const totalPages = Math.ceil(totalRecords / this.pageSize);
      if (page >= totalPages) {
        log.success(`HK: Completed all ${totalPages} pages`);
        break;
      }

      page++;
      pagesFetched++;
    }
  }

  /**
   * Build a normalised attorney record from the listing row and detail data.
   *
   * @param {{ memId: string, nameEn: string, nameCn: string }} member - From listing
   * @param {object|null} detail - From detail page (may be null)
   * @returns {object} Normalised attorney record
   */
  _buildAttorneyRecord(member, detail) {
    // Parse name from listing (format: "LAST FIRST MIDDLE")
    const nameEn = detail ? (detail.name_en || member.nameEn) : member.nameEn;
    const { firstName, lastName } = this._parseHkName(nameEn);
    const fullName = `${firstName} ${lastName}`.trim();

    // Firm info
    let firmName = '';
    let address = '';
    let phone = '';
    let email = '';
    let fax = '';
    let post = '';
    let admissionDate = '';
    let barStatus = '';
    let city = '';
    let nameCn = member.nameCn || '';

    if (detail) {
      firmName = detail.firm_name_en || '';
      address = detail.firm_address_en || '';
      phone = detail.firm_phone || '';
      email = detail.personal_email || detail.firm_email || '';
      fax = detail.firm_fax || '';
      post = detail.post || '';
      admissionDate = detail.admission_hk || '';
      nameCn = detail.name_cn || member.nameCn || '';

      // Determine bar status from remark
      if (detail.remark) {
        barStatus = detail.remark;
      }

      // Infer region from address
      city = this._inferRegion(address);
    }

    const profileUrl = `${this.memberDetailUrl}?MemId=${member.memId}`;

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      name_chinese: nameCn,
      firm_name: firmName,
      city: city,
      state: 'HK',
      country: 'Hong Kong',
      phone: phone,
      fax: fax,
      email: email,
      website: '',
      bar_number: member.memId,
      bar_status: barStatus,
      admission_date: admissionDate,
      position: post,
      address: address,
      profile_url: profileUrl,
    };
  }
}

module.exports = new HongKongScraper();
