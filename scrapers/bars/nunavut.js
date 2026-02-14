/**
 * Nunavut Law Society Scraper
 *
 * Source: https://lawsociety.nu.ca/membership-directory
 * Method: Drupal site with paginated HTML — Cheerio parsing
 * ~420 entries
 *
 * Uses standard BaseScraper pagination with buildSearchUrl/parseResultsPage.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NunavutScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nunavut',
      stateCode: 'CA-NU',
      baseUrl: 'https://lawsociety.nu.ca/membership-directory',
      pageSize: 25,
      practiceAreaCodes: {
        'family':                'family',
        'family law':            'family',
        'criminal':              'criminal',
        'criminal defense':      'criminal',
        'real estate':           'real-estate',
        'corporate/commercial':  'corporate-commercial',
        'corporate':             'corporate-commercial',
        'commercial':            'corporate-commercial',
        'personal injury':       'personal-injury',
        'employment':            'employment',
        'labour':                'employment',
        'immigration':           'immigration',
        'estate planning/wills': 'wills-estates',
        'estate planning':       'wills-estates',
        'wills':                 'wills-estates',
        'intellectual property': 'intellectual-property',
        'civil litigation':      'civil-litigation',
        'litigation':            'civil-litigation',
        'tax':                   'tax',
        'administrative':        'administrative',
        'environmental':         'environmental',
        'aboriginal/indigenous': 'aboriginal',
      },
      defaultCities: [
        'Iqaluit',
      ],
    });
  }

  /**
   * Build Drupal paginated URL.
   * Drupal uses ?page=0, ?page=1, etc. (0-indexed).
   */
  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    // Drupal pagination is 0-indexed
    if (page && page > 1) {
      params.set('page', String(page - 1));
    }
    if (city) {
      params.set('field_city_value', city);
    }
    if (practiceCode) {
      params.set('field_practice_area_value', practiceCode);
    }
    const qs = params.toString();
    return qs ? `${this.baseUrl}?${qs}` : this.baseUrl;
  }

  /**
   * Parse Drupal membership directory HTML.
   * Handles common Drupal Views output: table rows or div-based views.
   */
  parseResultsPage($) {
    const attorneys = [];

    // Strategy 1: Drupal Views table
    $('table.views-table tbody tr, .view-content table tbody tr').each((_, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const fullName = nameCell.text().trim();
      if (!fullName || fullName.length < 3) return;

      const profileLink = nameCell.find('a').attr('href') || '';
      const firm = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const phone = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const status = cells.length > 4 ? $(cells[4]).text().trim() : '';

      // Check for email
      let email = '';
      const mailtoLink = $row.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

      // Parse name — may be "Last, First" format
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

      const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: displayName,
        firm_name: firm,
        city: city || 'Iqaluit',
        state: 'CA-NU',
        phone,
        email,
        website: '',
        bar_number: '',
        bar_status: status || 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://lawsociety.nu.ca${profileLink}` : ''),
      });
    });

    // Strategy 2: Drupal Views unformatted / div-based
    if (attorneys.length === 0) {
      $('.view-content .views-row, .view-content .node, .directory-item, .member-item').each((_, el) => {
        const $el = $(el);
        const text = $el.text().trim();
        if (!text || text.length < 5) return;

        // Try to find a name element
        const nameEl = $el.find('.views-field-title a, .views-field-name a, .field-name a, h3 a, h2 a, a').first();
        const fullName = nameEl.text().trim() || $el.find('.views-field-title, .views-field-name, .field-name').first().text().trim();
        if (!fullName || fullName.length < 3) return;

        const profileLink = nameEl.attr('href') || '';

        // Extract fields
        const firm = $el.find('.views-field-field-firm, .field-firm, .views-field-field-company').text().replace(/^[^:]*:\s*/, '').trim();
        const city = $el.find('.views-field-field-city, .field-city').text().replace(/^[^:]*:\s*/, '').trim();
        const phone = $el.find('.views-field-field-phone, .field-phone').text().replace(/^[^:]*:\s*/, '').trim();
        const status = $el.find('.views-field-field-status, .field-status').text().replace(/^[^:]*:\s*/, '').trim();

        let email = '';
        const mailtoLink = $el.find('a[href^="mailto:"]');
        if (mailtoLink.length) {
          email = mailtoLink.attr('href').replace('mailto:', '').trim();
        }

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

        const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: displayName,
          firm_name: firm,
          city: city || 'Iqaluit',
          state: 'CA-NU',
          phone,
          email,
          website: '',
          bar_number: '',
          bar_status: status || 'Active',
          profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://lawsociety.nu.ca${profileLink}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from Drupal Views pager.
   */
  extractResultCount($) {
    const text = $('body').text();

    // Drupal pager patterns
    const matchOf = text.match(/(?:Displaying|Showing)\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?|entries)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    // Count pager items to estimate total
    const lastPage = $('li.pager-last a, .pager__item--last a').attr('href');
    if (lastPage) {
      const pageMatch = lastPage.match(/page=(\d+)/);
      if (pageMatch) {
        return (parseInt(pageMatch[1], 10) + 1) * this.pageSize;
      }
    }

    // If no pager found, count visible items as the total
    const visibleCount = this.parseResultsPage($).length;
    return visibleCount;
  }
}

module.exports = new NunavutScraper();
