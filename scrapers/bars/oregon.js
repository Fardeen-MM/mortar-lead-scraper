/**
 * Oregon State Bar Scraper
 *
 * Source: https://www.osbar.org/members/start.asp
 * Method: HTTP GET with query params + Cheerio HTML parsing
 * Search params: lname, fname, barnum, city
 * Results rendered in HTML table format.
 */

const BaseScraper = require('../base-scraper');

class OregonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'oregon',
      stateCode: 'OR',
      baseUrl: 'https://www.osbar.org/members/start.asp',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'IM',
        'family':               'FL',
        'family law':           'FL',
        'criminal':             'CR',
        'criminal defense':     'CR',
        'personal injury':      'PI',
        'estate planning':      'EP',
        'estate':               'EP',
        'tax':                  'TX',
        'tax law':              'TX',
        'employment':           'EM',
        'labor':                'LB',
        'bankruptcy':           'BK',
        'real estate':          'RE',
        'civil litigation':     'CL',
        'business':             'BU',
        'corporate':            'CO',
        'elder':                'EL',
        'intellectual property':'IP',
        'medical malpractice':  'MM',
        'workers comp':         'WC',
        'environmental':        'EN',
        'construction':         'CN',
        'juvenile':             'JV',
        'administrative':       'AD',
      },
      defaultCities: [
        'Portland', 'Eugene', 'Salem', 'Bend',
        'Medford', 'Corvallis', 'Lake Oswego', 'Beaverton',
      ],
    });

    this.searchUrl = 'https://www.osbar.org/members/display.asp';
  }

  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    params.set('fn', '');
    params.set('ln', '');
    params.set('barnum', '');
    if (city) {
      params.set('city', city);
    }
    params.set('s', 'a'); // status: active
    if (page && page > 1) {
      params.set('p', String(page));
    }
    return `${this.searchUrl}?${params.toString()}`;
  }

  parseResultsPage($) {
    const attorneys = [];

    // OSB displays results in table rows
    $('table.searchresults tr, table.results tr, table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 3) return;

      // Try to detect header rows
      const firstCellText = $(cells[0]).text().trim();
      if (/^name$/i.test(firstCellText) || /^bar\s*(#|number)$/i.test(firstCellText)) return;

      // Extract from table cells — typical layout:
      // Name | Bar # | City | Status | Phone
      const nameCell = $(cells[0]);
      const fullName = nameCell.text().trim();
      const profileLink = nameCell.find('a').attr('href') || '';

      if (!fullName || fullName.length < 2) return;

      const barNumber = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const status = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const phone = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const email = cells.length > 5 ? $(cells[5]).text().trim() : '';

      // Parse name — OSB often uses "Last, First" format
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
          : `https://www.osbar.org/members/${profileLink}`;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: '',
        city: city,
        state: 'OR',
        phone: phone,
        email: email,
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        profile_url: profileUrl,
      });
    });

    // Fallback: try parsing div-based results
    if (attorneys.length === 0) {
      $('.attorney-result, .member-result, .search-result').each((_, el) => {
        const $el = $(el);

        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .attorney-name').text().trim();
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

        const barNumber = $el.find('.barnum, .bar-number').text().trim().replace(/[^0-9]/g, '');
        const city = $el.find('.city').text().trim();
        const phone = $el.find('.phone').text().trim();
        const email = $el.find('a[href^="mailto:"]').text().trim();
        const status = $el.find('.status').text().trim();

        let profileUrl = '';
        if (profileLink) {
          profileUrl = profileLink.startsWith('http')
            ? profileLink
            : `https://www.osbar.org/members/${profileLink}`;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: '',
          city: city,
          state: 'OR',
          phone: phone,
          email: email,
          website: '',
          bar_number: barNumber,
          bar_status: status || 'Active',
          profile_url: profileUrl,
        });
      });
    }

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();

    // Patterns like "123 members found" or "Results: 1-50 of 234"
    const matchFound = text.match(/([\d,]+)\s+(?:members?|attorneys?|results?|records?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:members?|results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }
}

module.exports = new OregonScraper();
