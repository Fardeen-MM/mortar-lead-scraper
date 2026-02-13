/**
 * Florida Bar Association Scraper
 *
 * Source: https://www.floridabar.org/directories/find-mbr/
 * Method: HTTP GET + Cheerio (results are server-rendered)
 * Emails: Cloudflare XOR-obfuscated, decoded via BaseScraper
 */

const BaseScraper = require('../base-scraper');

class FloridaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'florida',
      stateCode: 'FL',
      baseUrl: 'https://www.floridabar.org/directories/find-mbr/',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'I01',
        'personal injury':      'P02',
        'family':               'F01',
        'family law':           'F01',
        'criminal':             'C16',
        'criminal defense':     'C16',
        'estate planning':      'E10',
        'estate':               'E10',
        'tax':                  'T01',
        'tax law':              'T01',
        'employment':           'L01',
        'labor':                'L01',
        'bankruptcy':           'B02',
        'real estate':          'R01',
        'civil litigation':     'C03',
        'business':             'B04',
        'corporate':            'C15',
        'elder':                'E02',
        'intellectual property':'I05',
        'medical malpractice':  'M04',
        'workers comp':         'W02',
        'adoption':             'A03',
        'juvenile':             'J02',
        'construction':         'C11',
        'environmental':        'E08',
      },
      defaultCities: [
        'Miami', 'Fort Lauderdale', 'West Palm Beach', 'Orlando',
        'Tampa', 'Jacksonville', 'St. Petersburg', 'Naples',
        'Boca Raton', 'Tallahassee', 'Gainesville', 'Sarasota',
        'Fort Myers', 'Daytona Beach', 'Pensacola', 'Coral Gables',
      ],
    });
  }

  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    if (city) {
      params.set('locType', 'C');
      params.set('locValue', city);
    }
    params.set('eligible', 'Y');
    params.set('pageNumber', String(page || 1));
    params.set('pageSize', String(this.pageSize));
    if (practiceCode) {
      params.set('pracAreas', practiceCode);
    }
    return `${this.baseUrl}?${params.toString()}`;
  }

  parseResultsPage($) {
    const attorneys = [];

    $('li.profile-compact').each((_, el) => {
      const $el = $(el);

      // Name + profile URL
      const nameLink = $el.find('p.profile-name a');
      const fullName = nameLink.text().trim();
      const profileUrl = nameLink.attr('href') || '';
      const barNumMatch = profileUrl.match(/num=(\d+)/);

      // Bar number
      const barText = $el.find('p.profile-bar-number').text().trim();
      const barNumber = barNumMatch ? barNumMatch[1] : (barText.match(/\d+/) || [''])[0];

      // Status
      const status = $el.find('.member-status').text().trim();
      const eligibility = $el.find('.eligibility').text().trim();

      // Contact block
      const contactBlock = $el.find('.profile-contact');
      const contactParagraphs = contactBlock.find('p');

      let firmName = '';
      let address = '';
      let city = '';
      let state = '';
      let zip = '';
      let phone = '';
      let email = '';

      // First <p>: firm + address
      if (contactParagraphs.length > 0) {
        const addressHtml = contactParagraphs.first().html() || '';
        const addressParts = addressHtml.split('<br>').map(s =>
          this.decodeEntities(s.replace(/<[^>]+>/g, '').trim())
        ).filter(Boolean);

        if (addressParts.length >= 1) firmName = addressParts[0];
        if (addressParts.length >= 3) {
          address = addressParts[1];
          const parsed = this.parseCityStateZip(addressParts[addressParts.length - 1]);
          city = parsed.city;
          state = parsed.state;
          zip = parsed.zip;
        } else if (addressParts.length === 2) {
          const parsed = this.parseCityStateZip(addressParts[1]);
          city = parsed.city;
          state = parsed.state;
        }
      }

      // Second <p>: phone + email
      if (contactParagraphs.length > 1) {
        const phoneHtml = contactParagraphs.eq(1).html() || '';

        const telMatch = phoneHtml.match(/href="tel:([^"]+)"/);
        if (telMatch) phone = telMatch[1];

        // Cloudflare-protected email
        const cfMatch = phoneHtml.match(/email-protection#([a-f0-9]+)/);
        if (cfMatch) {
          email = this.decodeCloudflareEmail(cfMatch[1]);
        } else {
          const mailtoMatch = phoneHtml.match(/mailto:([^"]+)/);
          if (mailtoMatch) email = mailtoMatch[1];
        }

        // Fallback: icon-email element
        if (!email) {
          const emailEl = $(contactParagraphs[1]).find('a.icon-email');
          if (emailEl.length) {
            const href = emailEl.attr('href') || '';
            if (href.includes('email-protection#')) {
              email = this.decodeCloudflareEmail(href.replace(/.*#/, ''));
            } else if (href.startsWith('mailto:')) {
              email = href.replace('mailto:', '');
            }
          }
        }
      }

      const certs = $el.find('.profile-certs').text().trim();
      const { firstName, lastName } = this.splitName(fullName);

      // If firm name looks like an address, it's likely no firm
      if (firmName && /^\d/.test(firmName)) {
        address = firmName + (address ? ', ' + address : '');
        firmName = '';
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city,
        state: state || 'FL',
        phone,
        email,
        website: '',
        bar_number: barNumber,
        bar_status: status || eligibility,
        certifications: certs,
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/Showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)\s+results/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }
}

module.exports = new FloridaScraper();
