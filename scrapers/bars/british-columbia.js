/**
 * British Columbia Law Society Scraper
 *
 * Source: https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/mbr-search.cfm
 * Method: ColdFusion GET form + DataTables HTML parsing
 *
 * The search form uses GET with fields: txt_last_nm, txt_given_nm, txt_city,
 * txt_search_type, is_submitted, results_no, member_search.
 * Results table (#searchResultTable) has a single Name column with links to detail pages.
 * Iterates last name initials (A-Z) per city since city-only search may not work.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class BritishColumbiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'british-columbia',
      stateCode: 'CA-BC',
      baseUrl: 'https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/mbr-search.cfm',
      pageSize: 25,
      practiceAreaCodes: {
        'family':                'Family',
        'family law':            'Family',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'real estate':           'Real Estate',
        'corporate/commercial':  'Corporate Commercial',
        'corporate':             'Corporate Commercial',
        'commercial':            'Corporate Commercial',
        'personal injury':       'Personal Injury',
        'employment':            'Employment',
        'labour':                'Labour',
        'immigration':           'Immigration',
        'estate planning/wills': 'Wills Estates',
        'estate planning':       'Wills Estates',
        'wills':                 'Wills Estates',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative',
        'environmental':         'Environmental',
        'aboriginal':            'Aboriginal',
        'insurance':             'Insurance',
      },
      defaultCities: [
        'Vancouver', 'Victoria', 'Surrey', 'Burnaby',
        'Richmond', 'Kelowna', 'Kamloops', 'Nanaimo',
      ],
    });

    // BC requires at least 2 characters for last name search
    this.lastNamePrefixes = ['Smith', 'Lee', 'Brown', 'Chan', 'Wong'];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Parse #searchResultTable — single Name column with links.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('#searchResultTable tbody tr').each((_, el) => {
      const $row = $(el);
      const td = $row.find('td[data-title="Name"]');
      if (!td.length) return;

      const link = td.find('a');
      const fullName = (link.text() || td.text()).trim();
      if (!fullName || fullName.length < 3) return;

      const profileLink = link.attr('href') || '';

      // Strip honorifics (KC, QC, etc.)
      const cleanName = fullName.replace(/,?\s*(KC|QC|K\.C\.|Q\.C\.)$/i, '').trim();
      const nameParts = this.splitName(cleanName);

      attorneys.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: cleanName,
        firm_name: '',
        city: '',
        state: 'CA-BC',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Practising',
        profile_url: profileLink.startsWith('http') ? profileLink
          : (profileLink ? `https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/${profileLink}` : ''),
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?)\s+found/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }

  /**
   * Override search() — GET-based ColdFusion form with last name initial iteration.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let totalForCity = 0;

      for (const prefix of this.lastNamePrefixes) {
        if (options.maxPages && totalForCity >= 5) break;

        const params = new URLSearchParams({
          is_submitted: '1',
          txt_search_type: 'begins',
          txt_last_nm: prefix,
          txt_given_nm: '',
          txt_city: city,
          member_search: 'Search',
          results_no: String(this.pageSize),
        });

        const url = `${this.baseUrl}?${params.toString()}`;
        log.info(`GET ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          continue;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city}/${letter} — skipping`);
          yield { _captcha: true, city };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) continue;

        log.success(`Found ${attorneys.length} results for ${city}/${prefix}`);

        for (const attorney of attorneys) {
          attorney.city = city;
          yield this.transformResult(attorney, practiceArea);
          totalForCity++;
        }
      }

      if (totalForCity > 0) {
        log.success(`Found ${totalForCity} total results for ${city}`);
      } else {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      }
    }
  }
}

module.exports = new BritishColumbiaScraper();
