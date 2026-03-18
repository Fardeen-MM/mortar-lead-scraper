/**
 * JCCP (Joint Council for Cosmetic Practitioners) Scraper
 *
 * Source: https://www.jccp.org.uk/MemberSearch
 * Records: ~1,039 registered UK aesthetic practitioners
 * Method: HTTP POST form submission (ASP.NET MVC, no CSRF token)
 *
 * Form endpoint: POST /MemberSearch
 *   Fields: Modality (0=All, 1=Chemical peels, 2=Lasers/IPL/LED, 3=Dermal fillers,
 *           4=Botulinum toxins, 5=Hair restoration), SearchKeyword (min 2 chars),
 *           PageSize, PageNumber (1-based via grid-page), MembersRecords
 *
 * Data extracted from HTML result cards:
 *   - Name (first + last), Member number (JCCP######)
 *   - Status (Active/Inactive)
 *   - Address (full line), Postcode
 *   - Phone (business landline)
 *   - Email address
 *   - Profession (Part A/B, regulatory body, PIN)
 *   - Modalities (treatment types + registration level)
 *   - Member since date
 *
 * Strategy: Search "JCCP" with Modality=0 returns ALL 1,039 members.
 *   50 results per page, 21 pages. Paginate via PageNumber field.
 *
 * Also supports per-modality search via practiceArea filter.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const PRACTICE_AREA_CODES = {
  'all': '0',
  'all treatments': '0',
  'chemical peels': '1',
  'skin rejuvenation': '1',
  'chemical peels and skin rejuvenation': '1',
  'lasers': '2',
  'ipl': '2',
  'led': '2',
  'lasers ipl led': '2',
  'dermal fillers': '3',
  'fillers': '3',
  'botulinum toxins': '4',
  'botox': '4',
  'hair restoration': '5',
  'hair restoration surgery': '5',
};

const PER_PAGE = 50;

class JCCPScraper extends BaseScraper {
  constructor() {
    super({
      name: 'jccp',
      stateCode: 'JCCP',
      baseUrl: 'https://www.jccp.org.uk',
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: [],
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('jccp: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('jccp: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('jccp: extractResultCount() not used — search() overridden'); }

  transformResult(lead, practiceArea) {
    lead.source = 'jccp';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * POST to /MemberSearch and return raw HTML body.
   */
  async _fetchPage(keyword, modality, pageNumber, rateLimiter) {
    const postData = `Modality=${modality}&SearchKeyword=${encodeURIComponent(keyword)}&PageSize=&PageNumber=${pageNumber}&MembersRecords=0`;

    return new Promise((resolve, reject) => {
      const https = require('https');
      const options = {
        hostname: 'www.jccp.org.uk',
        path: '/MemberSearch',
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Origin': 'https://www.jccp.org.uk',
          'Referer': 'https://www.jccp.org.uk/MemberSearch',
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          // Redirect means no results or session issue — return empty
          resolve({ statusCode: res.statusCode, body: '' });
          return;
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Parse result count from "X Results Found" text.
   */
  _extractResultCount($) {
    const text = $('h4').text();
    const match = text.match(/(\d[\d,]*)\s*Results?\s*Found/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse all member cards from the results page HTML.
   * Each result-item div contains: name, member number, status, address, phone, email,
   * profession, regulatory body, registration PIN, qualifications, modalities, member since.
   */
  _parseResults($) {
    const results = [];

    $('.result-item').each((i, el) => {
      const $item = $(el);

      // --- Name ---
      const nameEl = $item.find('ul.status li').first();
      const rawName = (nameEl.text() || '').replace(/\s+/g, ' ').trim();

      // --- Member number ---
      const memberNumEl = $item.find('ul.status li').eq(1);
      const memberNumText = (memberNumEl.text() || '').trim();
      const memberNumber = (memberNumText.match(/JCCP\d+/) || [''])[0];

      // --- Status ---
      const statusEl = $item.find('ul.status li').eq(2);
      const status = (statusEl.text() || '').trim();

      // --- Address ---
      const addressEl = $item.find('ul.address-info li').first().find('p');
      const fullAddress = (addressEl.text() || '').replace(/\s+/g, ' ').trim();

      // Parse address into components
      const { address, city, postcode } = this._parseAddress(fullAddress);

      // --- Phone ---
      const phoneEl = $item.find('a.tel');
      let phone = '';
      if (phoneEl.length) {
        phone = (phoneEl.text() || '').replace(/\s+/g, ' ').trim();
      }

      // --- Email ---
      const emailEl = $item.find('a.mail');
      let email = '';
      if (emailEl.length) {
        email = (emailEl.text() || '').replace(/\s+/g, ' ').trim().toLowerCase();
      }

      // --- Profession ---
      let profession = '';
      $item.find('.item-sec').each((j, sec) => {
        const label = $(sec).find('label').text().trim();
        if (label.startsWith('PROFESSION')) {
          profession = $(sec).find('.item-inside').text().replace(/\s+/g, ' ').trim();
        }
      });

      // --- Regulatory Body ---
      let regulatoryBody = '';
      $item.find('.item-sec').each((j, sec) => {
        const label = $(sec).find('label').text().trim();
        if (label.startsWith('REGULATORY BODY')) {
          regulatoryBody = $(sec).find('.item-inside').text().replace(/\s+/g, ' ').trim();
        }
      });

      // --- Registration PIN ---
      let regPin = '';
      $item.find('.item-sec').each((j, sec) => {
        const label = $(sec).find('label').text().trim();
        if (label.startsWith('REGISTRATION PIN')) {
          regPin = $(sec).find('.item-inside').text().replace(/\s+/g, ' ').trim();
        }
      });

      // --- Modalities (treatments) ---
      const modalities = [];
      $item.find('.modality tbody tr').each((j, row) => {
        const mod = $(row).find('td').first().text().replace(/\s+/g, ' ').trim();
        if (mod) modalities.push(mod);
      });

      // --- Member since ---
      let memberSince = '';
      $item.find('.item-sec').each((j, sec) => {
        const label = $(sec).find('label').text().trim();
        if (label.startsWith('MEMBER SINCE')) {
          memberSince = $(sec).find('.item-inside').text().replace(/\s+/g, ' ').trim();
        }
      });

      // --- Parse name ---
      // Names come as "FirstName LastName" or "Dr FirstName LastName"
      const cleanName = rawName
        .replace(/^(Dr|Mr|Mrs|Ms|Miss|Prof|Professor)\s+/i, '')
        .trim();
      const { firstName, lastName } = this.splitName(cleanName);

      // Only add if we have a name
      if (!firstName && !lastName) return;

      results.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '', // JCCP is individual practitioners, not firms
        address: address,
        city: city,
        state: 'UK',
        zip: postcode,
        phone: this._normalizeUKPhone(phone),
        email: email,
        website: '',
        bar_number: memberNumber,
        bar_status: status,
        admission_date: memberSince,
        profile_url: '',
        practice_area: modalities.join(', ') || '',
        // Extra fields
        jccp_profession: profession,
        jccp_regulatory_body: regulatoryBody,
        jccp_reg_pin: regPin,
        jccp_modalities: modalities.join('; '),
      });
    });

    return results;
  }

  /**
   * Parse a UK address string into components.
   * Input format: "1 St. Andrews Close, London, SW19 8NJ"
   */
  _parseAddress(fullAddress) {
    if (!fullAddress) return { address: '', city: '', postcode: '' };

    // UK postcode pattern
    const postcodeMatch = fullAddress.match(/[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}/i);
    const postcode = postcodeMatch ? postcodeMatch[0].trim().toUpperCase() : '';

    // Remove postcode from address for parsing
    let remaining = fullAddress;
    if (postcode) {
      remaining = remaining.replace(postcodeMatch[0], '').trim().replace(/,\s*$/, '');
    }

    // Split by commas and try to find city (usually second-to-last part)
    const parts = remaining.split(',').map(p => p.trim()).filter(Boolean);
    let city = '';
    let address = '';

    if (parts.length >= 2) {
      city = parts[parts.length - 1];
      address = parts.slice(0, -1).join(', ');
    } else if (parts.length === 1) {
      address = parts[0];
    }

    return { address, city, postcode };
  }

  /**
   * Normalize UK phone numbers.
   * JCCP stores them in format like "(077) 0991-3678" or "(020) 7935-8682"
   */
  _normalizeUKPhone(phone) {
    if (!phone) return '';
    // Strip all non-digit chars
    let digits = phone.replace(/\D/g, '');
    // If starts with 44, convert to 0
    if (digits.startsWith('44') && digits.length > 10) {
      digits = '0' + digits.substring(2);
    }
    // If starts with 447, it's a mobile
    if (digits.startsWith('447') && digits.length >= 11) {
      digits = '0' + digits.substring(2);
    }
    // Ensure starts with 0
    if (!digits.startsWith('0') && digits.length === 10) {
      digits = '0' + digits;
    }
    // Format: 0XXXX XXXXXX
    if (digits.length === 11) {
      if (digits.startsWith('020')) {
        return `${digits.substring(0, 3)} ${digits.substring(3, 7)} ${digits.substring(7)}`;
      }
      if (digits.startsWith('07')) {
        return `${digits.substring(0, 5)} ${digits.substring(5)}`;
      }
      return `${digits.substring(0, 5)} ${digits.substring(5)}`;
    }
    return digits || phone;
  }

  /**
   * Main search generator. Searches JCCP member register via POST form submission.
   *
   * @param {string} practiceArea - Treatment modality (e.g., 'botox', 'dermal fillers')
   * @param {object} options - Search options
   * @param {number} options.maxPages - Limit pages (default unlimited)
   * @param {string} options.city - Search keyword override (searches name, postcode, county)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // Resolve modality code
    const practiceKey = (practiceArea || 'all').toLowerCase().trim();
    const modality = PRACTICE_AREA_CODES[practiceKey] || '0';

    // Search keyword: "JCCP" returns all members; city override uses that as keyword
    const keyword = options.city || 'JCCP';

    log.scrape(`[JCCP] Starting search: modality=${modality}, keyword="${keyword}"`);

    // Fetch page 1 to get total count
    await rateLimiter.wait();
    let response;
    try {
      response = await this._fetchPage(keyword, modality, 0, rateLimiter);
    } catch (err) {
      log.error(`[JCCP] Failed to fetch page 1: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Request failed: ${err.message}` };
      return;
    }

    if (response.statusCode !== 200) {
      log.error(`[JCCP] Unexpected status ${response.statusCode} on page 1`);
      yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode}` };
      return;
    }

    let $ = cheerio.load(response.body);
    const totalResults = this._extractResultCount($);

    if (totalResults === 0) {
      log.info('[JCCP] No results found');
      return;
    }

    const totalPages = Math.ceil(totalResults / PER_PAGE);
    log.success(`[JCCP] Found ${totalResults} members (${totalPages} pages)`);

    yield { _cityProgress: { current: 1, total: totalPages } };

    const seenMembers = new Set();
    let totalYielded = 0;
    const maxPages = options.maxPages || totalPages;

    // Process page 1
    const page1Results = this._parseResults($);
    for (const lead of page1Results) {
      if (lead.bar_number && seenMembers.has(lead.bar_number)) continue;
      if (lead.bar_number) seenMembers.add(lead.bar_number);
      yield this.transformResult(lead, practiceArea);
      totalYielded++;
    }
    log.info(`[JCCP] Page 1: ${page1Results.length} members parsed, ${totalYielded} yielded`);

    // Fetch remaining pages
    for (let page = 2; page <= Math.min(totalPages, maxPages); page++) {
      yield { _cityProgress: { current: page, total: totalPages } };

      await rateLimiter.wait();

      try {
        response = await this._fetchPage(keyword, modality, page, rateLimiter);
      } catch (err) {
        log.error(`[JCCP] Failed to fetch page ${page}: ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`[JCCP] Page ${page} returned ${response.statusCode}`);
        continue;
      }

      $ = cheerio.load(response.body);
      const results = this._parseResults($);

      if (results.length === 0) {
        log.warn(`[JCCP] Page ${page} returned 0 results — may have reached end`);
        // Don't break immediately, could be a transient issue
        continue;
      }

      let pageYielded = 0;
      for (const lead of results) {
        if (lead.bar_number && seenMembers.has(lead.bar_number)) continue;
        if (lead.bar_number) seenMembers.add(lead.bar_number);
        yield this.transformResult(lead, practiceArea);
        totalYielded++;
        pageYielded++;
      }

      log.info(`[JCCP] Page ${page}/${totalPages}: ${results.length} parsed, ${pageYielded} new (${totalYielded} total)`);
    }

    log.success(`[JCCP] Finished — ${totalYielded} total members scraped`);
  }
}

module.exports = new JCCPScraper();
