/**
 * Florida Bar Association Scraper
 *
 * Source: https://www.floridabar.org/directories/find-mbr/
 * Method: HTTP GET + Cheerio (results are server-rendered)
 * Emails: Cloudflare XOR-obfuscated, decoded via BaseScraper
 *
 * Profile pages: The profile URL (/directories/find-mbr/profile/?num=XXXXX)
 * renders client-side via JavaScript/iframe. The server-rendered enriched data
 * is available at /directories/find-mbr/?barNum=XXXXX, which returns the same
 * li.profile-compact HTML but with additional fields: cell phone, fax number,
 * and board certifications.
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

  /**
   * Parse a Florida Bar profile page for additional contact and certification info.
   *
   * NOTE: The Florida Bar profile URL (/directories/find-mbr/profile/?num=XXXXX)
   * renders content client-side via JavaScript. enrichFromProfile() converts it
   * to the barNum search URL (/directories/find-mbr/?barNum=XXXXX) which returns
   * server-rendered HTML with enriched data including cell phone, fax, and
   * board certifications.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the barNum search result page
   * @returns {object} Additional fields: { phone, email, firm_name, bar_status,
   *   practice_area, admission_date, website, cell_phone, fax }
   */
  parseProfilePage($) {
    const result = {};
    const profile = $('li.profile-compact').first();
    if (!profile.length) return result;

    const contactBlock = profile.find('.profile-contact');
    const contactParagraphs = contactBlock.find('p');

    // --- Phone numbers (office, cell, fax) ---
    if (contactParagraphs.length > 1) {
      const phoneHtml = contactParagraphs.eq(1).html() || '';

      // Office phone — first tel: link (labeled "Office:")
      const officeMatch = phoneHtml.match(/Office:\s*<a href="tel:([^"]+)"/i);
      if (officeMatch) {
        result.phone = officeMatch[1].trim();
      } else {
        // Fallback: first tel: link regardless of label
        const telMatch = phoneHtml.match(/href="tel:([^"]+)"/);
        if (telMatch) result.phone = telMatch[1].trim();
      }

      // Cell phone
      const cellMatch = phoneHtml.match(/Cell:\s*<a href="tel:([^"]+)"/i);
      if (cellMatch) {
        result.cell_phone = cellMatch[1].trim();
      }

      // Fax number (not a link, just text)
      const faxMatch = phoneHtml.match(/Fax:\s*([\d().\s-]+)/i);
      if (faxMatch) {
        result.fax = faxMatch[1].trim();
      }

      // Email — Cloudflare-protected or mailto
      const cfMatch = phoneHtml.match(/email-protection#([a-f0-9]+)/);
      if (cfMatch) {
        result.email = this.decodeCloudflareEmail(cfMatch[1]);
      } else {
        const mailtoMatch = phoneHtml.match(/mailto:([^"]+)/);
        if (mailtoMatch) result.email = mailtoMatch[1];
      }

      // Fallback: icon-email element
      if (!result.email) {
        const emailEl = contactParagraphs.eq(1).find('a.icon-email');
        if (emailEl.length) {
          const href = emailEl.attr('href') || '';
          if (href.includes('email-protection#')) {
            result.email = this.decodeCloudflareEmail(href.replace(/.*#/, ''));
          } else if (href.startsWith('mailto:')) {
            result.email = href.replace('mailto:', '');
          }
        }
      }
    }

    // --- Firm name (from address block) ---
    if (contactParagraphs.length > 0) {
      const addressHtml = contactParagraphs.first().html() || '';
      const addressParts = addressHtml.split('<br>').map(s =>
        this.decodeEntities(s.replace(/<[^>]+>/g, '').trim())
      ).filter(Boolean);

      if (addressParts.length >= 1) {
        const candidate = addressParts[0];
        // Only treat as firm name if it doesn't look like a street address
        if (candidate && !/^\d/.test(candidate)) {
          result.firm_name = candidate;
        }
      }
    }

    // --- Bar status ---
    const status = profile.find('.member-status').text().trim();
    const eligibility = profile.find('.eligibility').text().trim();
    if (status) result.bar_status = status;
    if (eligibility && !status) result.bar_status = eligibility;

    // --- Website (external links in contact block, excluding known non-firm sites) ---
    const floridaBarDomains = [
      'floridabar.org', 'imageserver.floridabar.org',
      'fla-lap.org', 'legalfuel.com', 'lawyersadvisinglawyers.com',
    ];
    const isExcluded = (href) =>
      this.isExcludedDomain(href) || floridaBarDomains.some(d => href.includes(d));

    contactBlock.find('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (!isExcluded(href)) {
        result.website = href;
        return false; // break
      }
    });

    // --- Board certifications ---
    const certsBlock = profile.find('.profile-certs');
    if (certsBlock.length) {
      const certItems = [];
      certsBlock.find('li').each((_, el) => {
        const text = $(el).text().trim();
        if (text) certItems.push(text);
      });
      if (certItems.length > 0) {
        result.practice_area = certItems.join('; ');
      }
    }

    // Remove empty string values before returning
    for (const key of Object.keys(result)) {
      if (result[key] === '' || result[key] === undefined || result[key] === null) {
        delete result[key];
      }
    }

    return result;
  }

  /**
   * Override enrichFromProfile to use the barNum search URL instead of the
   * profile URL. The profile URL (/directories/find-mbr/profile/?num=XXXXX)
   * renders content client-side, but the barNum search URL
   * (/directories/find-mbr/?barNum=XXXXX) returns server-rendered HTML with
   * enriched data (cell phone, fax, board certifications).
   *
   * @param {object} lead - The lead object (must have profile_url or bar_number)
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {object} Additional fields from the profile page
   */
  async enrichFromProfile(lead, rateLimiter) {
    // Extract bar number from profile URL or lead data
    let barNumber = lead.bar_number;
    if (!barNumber && lead.profile_url) {
      const match = lead.profile_url.match(/num=(\d+)/);
      if (match) barNumber = match[1];
    }
    if (!barNumber) return {};

    // Fetch the barNum search URL (server-rendered, not the JS-dependent profile URL)
    const enrichUrl = `${this.baseUrl}?barNum=${barNumber}`;
    const $ = await this.fetchProfilePage(enrichUrl, rateLimiter);
    if (!$) return {};

    return this.parseProfilePage($);
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/Showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)\s+results/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }
}

module.exports = new FloridaScraper();
