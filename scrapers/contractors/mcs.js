/**
 * MCS Certified — UK Renewable Energy Installer Scraper
 *
 * Source: https://mcscertified.com/find-an-installer/
 * Records: ~5,630 certified installers (solar PV, heat pumps, biomass)
 * Method: HTTP + Cheerio — server-rendered HTML with pagination
 *
 * Strategy:
 *   1. Paginate through /find-an-installer/?page=N (default 12/page, can use 90)
 *   2. Parse installer cards for company name + "View Details" modal content
 *   3. Extract: email (mailto:), phone (tel:), address, certification number,
 *      regions covered, technologies, boiler upgrade scheme status
 *
 * Email: YES — direct mailto: links on installer cards/modals
 * Phone: YES — direct tel: links
 * Address: YES — full UK address with postcode
 *
 * Perfect niche: Solar/heat pump installers do $10-50K installations.
 * Consumers research heavily. A beautiful website = more sales.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://mcscertified.com';
const PER_PAGE = 90; // Max allowed

const PRACTICE_AREA_CODES = {
  'all': '',
  'solar': 'solar',
  'solar pv': 'solar',
  'heat pump': 'heat-pump',
  'air source heat pump': 'ashp',
  'ground source heat pump': 'gshp',
  'biomass': 'biomass',
  'wind': 'wind',
  'battery': 'battery',
};

class McsScraper extends BaseScraper {
  constructor() {
    super({
      name: 'mcs_certified',
      stateCode: 'MCS',
      baseUrl: BASE_URL,
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: [],
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('mcs: not used'); }
  parseResultsPage() { throw new Error('mcs: not used'); }
  extractResultCount() { throw new Error('mcs: not used'); }

  transformResult(lead, practiceArea) {
    lead.source = 'mcs_certified';
    if (practiceArea && practiceArea !== 'all' && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Parse installer data from a page of results.
   * Each installer has a card with name + technologies, and a hidden modal
   * with full details including email, phone, address.
   */
  _parseInstallers($) {
    const results = [];

    // Look for installer cards and their detail modals
    // The page has mailto: links with emails
    $('a[href^="mailto:"]').each((_, el) => {
      const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
      if (!email || email.includes('mcscertified') || email.includes('example')) return;

      // Walk up to find the installer container
      let $container = $(el).closest('div, li, article, section');
      // Try to find a larger container with all the details
      for (let i = 0; i < 5 && $container.length; i++) {
        if ($container.find('h2, h3').length && $container.find('a[href^="tel:"]').length) break;
        $container = $container.parent();
      }

      const containerText = $container.text().replace(/\s+/g, ' ').trim();

      // Company name — from h2 or h3
      let firmName = '';
      const $heading = $container.find('h2, h3').first();
      if ($heading.length) {
        firmName = $heading.text().trim();
      }

      // Phone
      let phone = '';
      const $tel = $container.find('a[href^="tel:"]');
      if ($tel.length) {
        phone = $tel.first().attr('href').replace('tel:', '').trim();
      }
      // Also check for phone text
      if (!phone) {
        const phoneMatch = containerText.match(/Telephone:\s*([\d\s]+)/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // Certification number
      let certNumber = '';
      const certMatch = containerText.match(/Certification Number:\s*([A-Z]+-\d+)/);
      if (certMatch) certNumber = certMatch[1];

      // Address
      let address = '';
      let city = '';
      let postcode = '';
      const addrMatch = containerText.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
      if (addrMatch) {
        address = addrMatch[1].trim().replace(/,\s*$/, '');
        // Extract postcode
        const pcMatch = address.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        if (pcMatch) {
          postcode = pcMatch[1].toUpperCase();
          if (postcode.length >= 5 && !postcode.includes(' ')) {
            postcode = postcode.slice(0, -3) + ' ' + postcode.slice(-3);
          }
        }
        // Extract city (usually UPPERCASE word before postcode)
        const parts = address.split(',').map(p => p.trim());
        for (let i = parts.length - 1; i >= 0; i--) {
          const part = parts[i].replace(/[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}/i, '').trim();
          if (part && part === part.toUpperCase() && part.length > 2 && !/^\d/.test(part)) {
            city = part.charAt(0) + part.slice(1).toLowerCase();
            break;
          }
          if (part && !part.match(/^\d/) && !part.match(/^Unit|^Floor|^Suite/i)) {
            city = part;
          }
        }
      }

      // Regions covered
      let regions = '';
      const regMatch = containerText.match(/Regions covered:\s*(.+?)(?=Boiler|Certification|$)/);
      if (regMatch) regions = regMatch[1].trim();

      // Technologies
      const technologies = [];
      $container.find('li').each((_, li) => {
        const tech = $(li).text().trim();
        if (tech && !tech.includes('View') && !tech.includes('mailto')) {
          technologies.push(tech);
        }
      });

      // Boiler Upgrade Scheme
      const busMatch = containerText.match(/Boiler Upgrade Scheme[^:]*:\s*(Yes|No)/i);
      const boilerScheme = busMatch ? busMatch[1] : '';

      if (!firmName && !email) return;

      results.push({
        first_name: '',
        last_name: '',
        firm_name: firmName,
        address: address,
        city: city,
        state: 'UK',
        zip: postcode,
        country: 'UK',
        phone: this._normalizePhone(phone),
        email: email,
        website: '',
        bar_number: certNumber,
        bar_status: 'Certified',
        profile_url: '',
        practice_area: technologies.join(', ') || 'Renewable Energy',
        trade_category: 'Renewable Energy',
        license_type: 'MCS Certified Installer',
        regions_covered: regions,
        boiler_upgrade_scheme: boilerScheme,
      });
    });

    return results;
  }

  /**
   * Normalize UK phone number.
   */
  _normalizePhone(phone) {
    if (!phone) return '';
    let digits = phone.replace(/\D/g, '');
    if (digits.startsWith('44') && digits.length > 10) digits = '0' + digits.substring(2);
    if (!digits.startsWith('0') && digits.length === 10) digits = '0' + digits;
    if (digits.length === 11) {
      if (digits.startsWith('020')) return `${digits.substring(0, 3)} ${digits.substring(3, 7)} ${digits.substring(7)}`;
      return `${digits.substring(0, 5)} ${digits.substring(5)}`;
    }
    return digits || phone;
  }

  /**
   * Get total result count from page text.
   */
  _getTotalResults($) {
    const text = $('body').text();
    const match = text.match(/Showing:\s*\d+\s*of\s*([\d,]+)\s*results/i) ||
                  text.match(/([\d,]+)\s*results/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Main search generator.
   * Paginates through /find-an-installer/?page=N with 90 results per page.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    log.scrape(`[MCS] Starting scrape of certified installers`);

    const seenEmails = new Set();
    let totalYielded = 0;
    let totalWithEmail = 0;
    let totalWithPhone = 0;
    let page = 1;

    while (page <= maxPages) {
      // Build URL with 90 results per page
      // MCS uses ?page=N and the page size is set via cookies/session
      // We'll use the default pagination and just iterate
      const url = `${BASE_URL}/find-an-installer/?page=${page}`;

      log.info(`[MCS] Page ${page} — ${url}`);
      yield { _cityProgress: { current: page, total: Math.min(maxPages, 63) } }; // ~5630/90 = 63 pages

      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpGet(url, rateLimiter);
      } catch (err) {
        log.warn(`[MCS] Failed to fetch page ${page}: ${err.message}`);
        break;
      }

      if (response.statusCode !== 200) {
        log.warn(`[MCS] HTTP ${response.statusCode} on page ${page}`);
        break;
      }

      const $ = cheerio.load(response.body);

      // Get total on first page
      if (page === 1) {
        const total = this._getTotalResults($);
        if (total > 0) {
          log.success(`[MCS] Found ${total.toLocaleString()} total installers`);
        }
      }

      const installers = this._parseInstallers($);

      if (installers.length === 0) {
        log.info(`[MCS] Page ${page}: no results — stopping`);
        break;
      }

      for (const lead of installers) {
        // Dedup by email
        if (lead.email && seenEmails.has(lead.email)) continue;
        if (lead.email) seenEmails.add(lead.email);

        if (lead.email) totalWithEmail++;
        if (lead.phone) totalWithPhone++;

        yield this.transformResult(lead, practiceArea);
        totalYielded++;
      }

      log.info(`[MCS] Page ${page}: ${installers.length} installers (${totalYielded} total, ${totalWithEmail} with email)`);

      // Check if there's a next page
      const hasNext = $('a[href*="page="]').filter((_, el) => {
        const text = $(el).text().trim().toLowerCase();
        return text === 'next' || text === '›' || text === '»';
      }).length > 0;

      if (!hasNext) {
        log.info(`[MCS] No more pages after page ${page}`);
        break;
      }

      page++;
    }

    log.success(`[MCS] Finished — ${totalYielded} installers (${totalWithEmail} with email, ${totalWithPhone} with phone)`);
  }
}

module.exports = new McsScraper();
