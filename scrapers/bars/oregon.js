/**
 * Oregon State Bar Scraper
 *
 * Source: https://www.osbar.org/members/start.asp
 * Method: HTTP GET with query params + Cheerio HTML parsing
 * Search params: last, first, bar, city, s (status)
 * Results rendered in HTML table format.
 */

const BaseScraper = require('../base-scraper');

class OregonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'oregon',
      stateCode: 'OR',
      baseUrl: 'https://www.osbar.org/members/start.asp',
      pageSize: 20,
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

    this.searchUrl = 'https://www.osbar.org/members/membersearch.asp';
  }

  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    params.set('first', '');
    params.set('last', '');
    params.set('bar', '');
    if (city) {
      params.set('city', city);
    }
    params.set('pastnames', '');
    if (page && page > 1) {
      params.set('cp', String(page));
    }
    return `${this.searchUrl}?${params.toString()}`;
  }

  parseResultsPage($) {
    const attorneys = [];

    // OSB displays results in table#tblResults with columns: Bar# | Name | City
    // Each row has onclick="location.href='membersearch_display.asp?b=XXXXXX'"
    $('#tblResults tbody tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 3) return;

      const barNumber = $(cells[0]).text().trim();
      const fullName = $(cells[1]).text().trim();
      const city = $(cells[2]).text().trim();

      if (!fullName || fullName.length < 2) return;
      // Skip header rows that might leak through
      if (/^(bar\s*#|name|city)$/i.test(barNumber)) return;

      // Extract detail link from row onclick attribute
      let profileUrl = '';
      const onclick = $row.attr('onclick') || '';
      const barMatch = onclick.match(/membersearch_display\.asp\?b=(\d+)/);
      if (barMatch) {
        profileUrl = `https://www.osbar.org/members/membersearch_display.asp?b=${barMatch[1]}`;
      }

      // Parse name — OSB uses "Last, First Middle" format
      // Also strips honorifics like "Mr.", "Ms.", "Mrs."
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        // Remove honorifics (Mr., Ms., Mrs., Dr., Hon.) from first name portion
        firstName = (parts[1] || '').replace(/^(Mr\.|Ms\.|Mrs\.|Dr\.|Hon\.)\s*/i, '').trim();
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: '',
        city: city,
        state: 'OR',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: 'Active',
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  /**
   * Parse an OSB member detail page for additional contact info.
   * URL pattern: https://www.osbar.org/members/membersearch_display.asp?b=XXXXXX
   *
   * The detail page has contact info, practice areas, and more.
   */
  parseProfilePage($) {
    const result = {};
    const bodyText = $('body').text();

    // Phone
    const phoneMatch = bodyText.match(/(?:Phone|Tel(?:ephone)?|Office):\s*([\d().\s-]+)/i) ||
                       bodyText.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
    if (phoneMatch) {
      result.phone = phoneMatch[1].trim();
    }

    // Email from mailto links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
    }

    // Website — look for links with website/firm/visit text, excluding bar site
    // and social media / legal directory domains (via isExcludedDomain)
    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const text = $(el).text().toLowerCase().trim();
      if ((text.includes('website') || text.includes('firm') || text.includes('visit')) &&
          href.startsWith('http') && !href.includes('osbar.org') &&
          !this.isExcludedDomain(href)) {
        result.website = href;
        return false;
      }
    });

    // Firm name
    const firmMatch = bodyText.match(/(?:Firm|Employer|Company)(?:\s*Name)?:\s*(.+?)(?:\n|$)/i);
    if (firmMatch) {
      const firm = firmMatch[1].trim();
      if (firm && firm.length > 1 && firm.length < 200) {
        result.firm_name = firm;
      }
    }

    // Practice areas
    const practiceMatch = bodyText.match(/(?:Practice\s*Areas?|Specialt(?:y|ies)):\s*(.+?)(?:\n|$)/i);
    if (practiceMatch) {
      result.practice_areas = practiceMatch[1].trim();
    }

    return result;
  }

  extractResultCount($) {
    const text = $('body').text();

    // OSB uses "<h3>253 Matches</h3>" in the paging header
    const matchMatches = text.match(/([\d,]+)\s+Match(?:es)?/i);
    if (matchMatches) return parseInt(matchMatches[1].replace(/,/g, ''), 10);

    // Fallback patterns
    const matchFound = text.match(/([\d,]+)\s+(?:members?|attorneys?|results?|records?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:members?|results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    return 0;
  }
}

module.exports = new OregonScraper();
