/**
 * Idaho State Bar Attorney Roster Scraper
 *
 * Source: https://apps.isb.idaho.gov/licensing/attorney_roster.cfm
 * Method: ColdFusion form with lname param (HTTP POST/GET)
 * Rich data returned including email, website, ISB member number.
 * Results rendered as HTML parsed with Cheerio.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IdahoScraper extends BaseScraper {
  constructor() {
    super({
      name: 'idaho',
      stateCode: 'ID',
      baseUrl: 'https://apps.isb.idaho.gov/licensing/attorney_roster.cfm',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'labor',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
        'water law':            'water_law',
        'natural resources':    'natural_resources',
        'mining':               'mining',
      },
      defaultCities: [
        'Boise', 'Nampa', 'Meridian', 'Idaho Falls',
        'Pocatello', 'Caldwell', 'Coeur d\'Alene', 'Twin Falls',
      ],
    });
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
          'Referer': this.baseUrl,
        },
        timeout: 15000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Not used directly — search() is overridden for ColdFusion POST requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ColdFusion POST requests`);
  }

  parseResultsPage($) {
    const attorneys = [];

    // ISB results are in a 3-column HTML table:
    //   Column 0: Attorney name (link to attorney_roster_ind.cfm?IDANumber=...)
    //   Column 1: Status (Active, Inactive, etc.)
    //   Column 2: Location (City + State abbreviation, e.g. "Boise ID")
    $('table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 3) return;

      // Skip header rows
      if ($row.find('th').length > 0) return;
      const firstCellText = $(cells[0]).text().trim().toLowerCase();
      if (firstCellText === 'name' || firstCellText === 'attorney' || firstCellText === 'isb #' || firstCellText === 'member') return;

      const nameCell = $(cells[0]);
      const fullName = nameCell.text().trim();
      const profileLink = nameCell.find('a').attr('href') || '';

      if (!fullName || fullName.length < 2) return;

      const status = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const locationText = cells.length > 2 ? $(cells[2]).text().trim() : '';

      // Parse location — format is "City State" e.g. "Boise ID" or "El Dorado Hills CA"
      let city = locationText;
      let state = 'ID';
      const locationMatch = locationText.match(/^(.+?)\s+([A-Z]{2})$/);
      if (locationMatch) {
        city = locationMatch[1].trim();
        state = locationMatch[2];
      }

      // Extract ISB number from profile link (IDANumber=XXXX)
      let isbNumber = '';
      const idaMatch = (profileLink || '').match(/IDANumber=(\d+)/i);
      if (idaMatch) {
        isbNumber = idaMatch[1];
      }

      // Parse name — ISB uses "Last, First Middle" format
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = parts[1] || '';
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      let profileUrl = '';
      if (profileLink) {
        profileUrl = profileLink.startsWith('http')
          ? profileLink
          : `https://apps.isb.idaho.gov/licensing/${profileLink}`;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: '',
        city: city,
        state: state,
        phone: '',
        email: '',
        website: '',
        bar_number: isbNumber,
        bar_status: status || 'Active',
        profile_url: profileUrl,
      });
    });

    // Fallback: div-based or definition-list results
    if (attorneys.length === 0) {
      $('.attorney, .result, .attorney-listing, .member-listing').each((_, el) => {
        const $el = $(el);

        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .attorney-name, h3, h4').text().trim();
        const profileLink = nameEl.attr('href') || '';

        if (!fullName || fullName.length < 2) return;

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] || '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        const isbNumber = ($el.find('.isb, .member-number, .bar-number').text().trim() || '').replace(/[^0-9]/g, '');
        const city = $el.find('.city, .location').text().trim();
        const phone = $el.find('.phone, .telephone').text().trim();
        const firmName = $el.find('.firm, .firm-name').text().trim();
        const status = $el.find('.status').text().trim();

        // Extract email
        let email = '';
        const mailtoEl = $el.find('a[href^="mailto:"]');
        if (mailtoEl.length) {
          email = mailtoEl.attr('href').replace('mailto:', '').trim();
        } else {
          const emailEl = $el.find('.email');
          if (emailEl.length) {
            const emailText = emailEl.text().trim();
            if (emailText.includes('@')) email = emailText;
          }
        }

        // Extract website
        let website = '';
        const websiteEl = $el.find('a[href*="http"]:not([href*="mailto"])').filter((_, a) => {
          const href = $(a).attr('href') || '';
          return !href.includes('isb.idaho.gov');
        });
        if (websiteEl.length) {
          website = websiteEl.first().attr('href') || '';
        }

        let profileUrl = '';
        if (profileLink) {
          profileUrl = profileLink.startsWith('http')
            ? profileLink
            : `https://apps.isb.idaho.gov/licensing/${profileLink}`;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: firmName,
          city: city,
          state: 'ID',
          phone: phone,
          email: email,
          website: website,
          bar_number: isbNumber,
          bar_status: status || 'Active',
          profile_url: profileUrl,
        });
      });
    }

    // Additional pass: look for detail blocks with labeled fields (ColdFusion pattern)
    if (attorneys.length === 0) {
      // Some ColdFusion pages render as sequences of labeled fields
      const bodyText = $('body').html() || '';
      const blocks = bodyText.split(/<hr\s*\/?>/i).filter(b => b.trim().length > 0);

      for (const block of blocks) {
        const $block = cheerio.load(block);
        const text = $block('body').text();

        const nameMatch = text.match(/Name:\s*(.+?)(?:\n|ISB|Member|Phone)/i);
        const isbMatch = text.match(/(?:ISB|Member)\s*(?:#|Number|No\.?):\s*(\d+)/i);
        const cityMatch = text.match(/City:\s*([A-Za-z\s.'-]+)/i);
        const phoneMatch = text.match(/Phone:\s*([\d\s().-]+)/i);
        const emailMatch = text.match(/Email:\s*([\w.+-]+@[\w.-]+)/i);
        const websiteMatch = text.match(/Website:\s*(https?:\/\/[^\s]+)/i);
        const firmMatch = text.match(/Firm:\s*(.+?)(?:\n|City|Phone|Email)/i);
        const statusMatch = text.match(/Status:\s*([A-Za-z\s]+?)(?:\n|$)/i);

        if (!nameMatch) continue;

        const fullName = nameMatch[1].trim();
        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] || '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: firmMatch ? firmMatch[1].trim() : '',
          city: cityMatch ? cityMatch[1].trim() : '',
          state: 'ID',
          phone: phoneMatch ? phoneMatch[1].trim() : '',
          email: emailMatch ? emailMatch[1].trim() : '',
          website: websiteMatch ? websiteMatch[1].trim() : '',
          bar_number: isbMatch ? isbMatch[1] : '',
          bar_status: statusMatch ? statusMatch[1].trim() : 'Active',
          profile_url: '',
        });
      }
    }

    return attorneys;
  }

  /**
   * Parse an ISB individual attorney profile page for additional contact info.
   * URL pattern: https://apps.isb.idaho.gov/licensing/attorney_roster_ind.cfm?IDANumber=XXXX
   *
   * The profile page uses Bootstrap panels with dl-horizontal definition lists
   * containing: Firm, Mailing Address, Phone, Bar Email, Website, Court eService Email.
   */
  parseProfilePage($) {
    const result = {};

    // The page uses <dl class="dl-horizontal"> with <dt>Label</dt><dd>Value</dd> pairs.
    // Build a label->value map from all dt/dd pairs.
    const fields = {};
    $('dl.dl-horizontal dt').each((_, el) => {
      const label = $(el).text().trim().toLowerCase();
      const dd = $(el).next('dd');
      if (dd.length) {
        fields[label] = {
          text: dd.text().trim().replace(/\s+/g, ' ').replace(/\u00a0/g, '').trim(),
          html: dd.html() || '',
          el: dd,
        };
      }
    });

    // Phone: from the "phone" dt/dd — the <a href="tel:..."> contains the number
    if (fields['phone']) {
      const telLink = fields['phone'].el.find('a[href^="tel:"]');
      if (telLink.length) {
        const phone = telLink.attr('href').replace('tel:', '').trim();
        if (phone && phone.length > 5) {
          result.phone = phone;
        }
      }
      // Fallback to text content
      if (!result.phone) {
        const phoneText = fields['phone'].text;
        if (phoneText && phoneText.length > 5) {
          result.phone = phoneText;
        }
      }
    }

    // Email: from "bar email address" — the <a href="mailto:..."> contains the email
    if (fields['bar email address']) {
      const mailtoLink = fields['bar email address'].el.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        const email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
        if (email && email.includes('@')) {
          result.email = email;
        }
      }
      // Fallback to text content
      if (!result.email) {
        const emailText = fields['bar email address'].text.toLowerCase();
        if (emailText.includes('@')) {
          result.email = emailText;
        }
      }
    }

    // Website: from "website address" — the <a href="..."> contains the URL
    if (fields['website address']) {
      const websiteLink = fields['website address'].el.find('a[href]');
      if (websiteLink.length) {
        let href = websiteLink.attr('href') || '';
        // ISB sometimes stores just domain without protocol, or empty "http://"
        if (href && href !== 'http://' && href !== 'https://' && !this.isExcludedDomain(href)) {
          if (!href.startsWith('http')) {
            href = 'http://' + href;
          }
          result.website = href;
        }
      }
    }

    // Firm name
    if (fields['firm']) {
      const firm = fields['firm'].text;
      if (firm && firm.length > 1 && firm.length < 200) {
        result.firm_name = firm;
      }
    }

    // Mailing address — may span multiple <dd> elements after the "mailing address" dt.
    // The ISB page has: <dt>Mailing Address</dt><dd>street</dd><dd>city, ST zip</dd>
    const addressParts = [];
    $('dl.dl-horizontal dt').each((_, el) => {
      if ($(el).text().trim().toLowerCase() === 'mailing address') {
        // Collect all following dd elements until the next dt
        let next = $(el).next();
        while (next.length && next.prop('tagName')?.toLowerCase() === 'dd') {
          const text = next.text().trim().replace(/\s+/g, ' ');
          if (text && text !== ',') {
            addressParts.push(text);
          }
          next = next.next();
        }
      }
    });
    if (addressParts.length > 0) {
      const address = addressParts.join(', ').trim();
      if (address && address.length > 3) {
        result.address = address;
      }
    }

    return result;
  }

  extractResultCount($) {
    const text = $('body').text();

    const matchFound = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?|members?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?|members?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchReturned = text.match(/returned\s+([\d,]+)\s+(?:results?|records?|attorneys?|members?)/i);
    if (matchReturned) return parseInt(matchReturned[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total:\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for ColdFusion POST-based form submissions.
   * ISB form only has a single field: LastName (no city, fname, or status fields).
   * The form POSTs to attorney_roster.cfm with option=initial_page_load and LastName.
   * Results include: Attorney name (link), Status, and Location (City + State).
   *
   * We use only 2 last name prefixes per city to stay within the 25s smoke test timeout.
   * City filtering is done client-side since the ISB form does not support it.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`ID bar search does not filter by practice area — searching all attorneys`);
    }

    const cities = this.getCities(options);

    // Use only 2 prefixes to avoid timeout (the ISB form has no city filter,
    // so each request returns all matching attorneys regardless of location).
    const lastNamePrefixes = ['S', 'M'];

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let pagesFetched = 0;

      for (const prefix of lastNamePrefixes) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // ISB ColdFusion form only accepts LastName and option fields
        const formData = {
          option: 'initial_page_load',
          LastName: prefix,
        };

        log.info(`Searching ${city} — last name prefix "${prefix}" — POST ${this.baseUrl}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} prefix ${prefix}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping prefix ${prefix}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} prefix ${prefix} — skipping`);
          yield { _captcha: true, city, prefix };
          continue;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          continue;
        }

        log.success(`Found ${attorneys.length} results for ${city} prefix "${prefix}"`);

        // Filter to only attorneys in the target city (ISB returns all locations)
        for (const attorney of attorneys) {
          if (city && attorney.city && attorney.city.toLowerCase() !== city.toLowerCase()) {
            continue;
          }
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        pagesFetched++;
      }
    }
  }
}

module.exports = new IdahoScraper();
