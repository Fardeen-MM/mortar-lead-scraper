/**
 * CICC Public Registry Scraper — College of Immigration and Citizenship Consultants
 *
 * Source: register.college-ic.ca
 * Method: HTTP GET profile enumeration (no Puppeteer needed)
 * Data:   Name, College ID, email, phone, company, city, province, license status
 *
 * The registry assigns sequential IDs (~1 to ~40,000). Each profile page is a
 * simple GET request. We filter for Type = "RCIC" (skip Contact/Organization records).
 *
 * HTML structure (iMIS/ASP.NET):
 *   - Name: <span style="font-size: 32px;">{Name}</span>
 *   - College ID: College ID</span> - R{digits}
 *   - Type: Type</span>...RCIC</span>
 *   - Eligibility: "Eligible to Provide Service" or "NOT Eligible..."
 *   - Current Licence: class + status in <span> pairs
 *   - Employment table (RadGrid): Company | Start Date | Country | Province/State | City | Email | Phone
 *   - Emails are Cloudflare-obfuscated: data-cfemail="hex" attribute needs XOR decoding
 *
 * ~12,000 RCICs expected across all of Canada.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const PROFILE_URL = 'https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/Licensee/Profile.aspx';

class CICCRegistryScraper extends BaseScraper {
  constructor() {
    super({
      name: 'cicc-registry',
      stateCode: 'CA-CICC',
      baseUrl: 'https://register.college-ic.ca',
      pageSize: 100,
      practiceAreaCodes: {},
      defaultCities: ['Canada'],
    });
  }

  /**
   * Decode Cloudflare email obfuscation.
   * Cloudflare encodes emails in data-cfemail attributes using XOR.
   * Format: first 2 hex chars = key, rest = XOR'd email chars.
   */
  _decodeCfEmail(encoded) {
    if (!encoded || encoded.length < 4) return null;
    try {
      const key = parseInt(encoded.substring(0, 2), 16);
      let email = '';
      for (let i = 2; i < encoded.length; i += 2) {
        const charCode = parseInt(encoded.substring(i, i + 2), 16) ^ key;
        email += String.fromCharCode(charCode);
      }
      return email.toLowerCase();
    } catch {
      return null;
    }
  }

  /**
   * Parse the Employment RadGrid table rows.
   * Columns: Company | Start Date | Country | Province/State | City | Email | Phone
   * Each row is <tr class="rgRow"> or <tr class="rgAltRow"> with 7 <td> cells.
   */
  _parseEmploymentTable(html) {
    const results = [];
    // Find the Employment section by its container ID, then extract the tbody
    const empSection = html.match(/id="[^"]*Employment[^"]*ResultsGrid_Grid1[^"]*"[^>]*class="RadGrid[^>]*>[\s\S]*?<tbody>([\s\S]*?)<\/tbody>/);
    if (!empSection) return results;

    const tbody = empSection[1];
    // Match each row (rgRow or rgAltRow)
    const rowPattern = /<tr\s+class="rg(?:Row|AltRow)"[^>]*>([\s\S]*?)<\/tr>/g;
    let rowMatch;
    while ((rowMatch = rowPattern.exec(tbody)) !== null) {
      const rowHtml = rowMatch[1];
      // Extract all <td> cells
      const cells = [];
      const cellPattern = /<td[^>]*>([\s\S]*?)<\/td>/g;
      let cellMatch;
      while ((cellMatch = cellPattern.exec(rowHtml)) !== null) {
        cells.push(cellMatch[1].trim());
      }

      if (cells.length >= 7) {
        const entry = {
          company: this._stripHtml(cells[0]),
          startDate: this._stripHtml(cells[1]),
          country: this._stripHtml(cells[2]),
          province: this._stripHtml(cells[3]),
          city: this._stripHtml(cells[4]),
          email: null,
          phone: this._stripHtml(cells[6]),
        };

        // Email cell may contain Cloudflare-obfuscated email
        const cfMatch = cells[5].match(/data-cfemail="([^"]+)"/);
        if (cfMatch) {
          entry.email = this._decodeCfEmail(cfMatch[1]);
        } else {
          // Try plain email
          const plainEmail = cells[5].match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
          if (plainEmail) entry.email = plainEmail[0].toLowerCase();
        }

        results.push(entry);
      }
    }
    return results;
  }

  /**
   * Strip HTML tags and decode common HTML entities from a string.
   */
  _stripHtml(str) {
    return str
      .replace(/<[^>]+>/g, '')
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&#39;/g, "'")
      .replace(/&quot;/g, '"')
      .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code)))
      .replace(/\s+/g, ' ')
      .trim();
  }

  /**
   * Parse a single profile page HTML and extract lead data.
   */
  _parseProfile(html, id) {
    if (!html || html.length < 500) return null;

    // Check for College ID (R followed by digits) — required for RCIC profiles
    const collegeIdMatch = html.match(/College ID<\/span>\s*-\s*(R\d{5,7})/);
    if (!collegeIdMatch) return null;

    // Check for RCIC type indicator
    const isRCIC = />\s*RCIC\s*</.test(html) || />\s*RISIA\s*</.test(html);
    if (!isRCIC) return null;

    // Skip inactive/ineligible consultants — only scrape active ones
    if (/NOT\s+Eligible/i.test(html)) return null;

    const result = {
      _profileId: id,
      college_id: collegeIdMatch[1],
      source: 'cicc_registry',
      country: 'CA',
    };

    // Extract name — first <span style="font-size: 32px;"> that contains a name
    const nameMatch = html.match(/<span\s+style="font-size:\s*32px;">([^<]+)<\/span>/);
    if (nameMatch) {
      const fullName = this._stripHtml(nameMatch[1]);
      if (fullName.length > 2 && !fullName.toLowerCase().includes('eligible') &&
          !fullName.toLowerCase().includes('not ')) {
        const parts = fullName.split(/\s+/);
        result.first_name = parts[0];
        result.last_name = parts.slice(1).join(' ');
      }
    }

    // Extract eligibility status
    const eligMatch = html.match(/<strong><span[^>]*>((?:NOT )?Eligible[^<]*)<\/span><\/strong>/i);
    if (eligMatch) {
      result.bar_status = eligMatch[1].trim();
    }

    // Extract current licence class and status from the Current Licence section
    const licenceSection = html.match(/Current Licence[\s\S]*?<div class="card-body">([\s\S]*?)<\/div>/);
    if (licenceSection) {
      const licBody = licenceSection[1];
      // Licence class: "Licence\t\t\tClass L3 - RCIC-IRB"
      const classMatch = licBody.match(/<span>Licence[\s\S]*?<\/span>\s*<span>(Class[^<]+)<\/span>/);
      if (classMatch) {
        result.title = classMatch[1].trim();
      }

      // Licence status: "Licence Status\t\t\tActive"
      const statusMatch = licBody.match(/Licence Status[\s\S]*?<\/span>\s*<span>([^<]+)<\/span>/);
      if (statusMatch) {
        result.bar_status = statusMatch[1].trim();
      }
    }

    // Parse Employment table — this is where email, phone, company, city, province live
    const employmentRows = this._parseEmploymentTable(html);
    if (employmentRows.length > 0) {
      // Use the most recent employment entry (first row = most recent)
      const current = employmentRows[0];

      if (current.company && current.company !== '&nbsp;') {
        result.firm_name = current.company;
      }
      if (current.city && current.city !== '&nbsp;') {
        result.city = current.city;
      }
      if (current.province && current.province !== '&nbsp;') {
        result.state = current.province;
      }
      if (current.email) {
        // Filter out college system emails
        if (!current.email.includes('college-ic.ca')) {
          result.email = current.email;
          const domainMatch = result.email.match(/@(.+)/);
          if (domainMatch) result.domain = domainMatch[1];
        }
      }
      if (current.phone && current.phone !== '&nbsp;') {
        result.phone = current.phone;
      }
    }

    // Must have at minimum a College ID and some name
    if (!result.college_id) return null;
    if (!result.first_name && !result.firm_name) return null;

    result.niche = 'immigration consultant';
    result.profile_url = `${PROFILE_URL}?ID=${id}&b9100e1006f6=2`;
    result.bar_number = result.college_id;

    return result;
  }

  /**
   * Search the CICC registry by enumerating profile IDs.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter({ minDelay: 300, maxDelay: 800 });
    // RCICs are concentrated in IDs 8000-30000 (before=Contacts, after=Organizations)
    const startId = options.startId || 8000;
    const endId = options.endId || 30000;
    const maxResults = options.maxResults || 0;
    const isTestMode = !!(options.maxPages);

    // In test mode, only scan a small range
    const effectiveEnd = isTestMode ? Math.min(startId + 100, endId) : endId;

    log.info(`[CICC] Scanning profile IDs ${startId} to ${effectiveEnd}`);
    yield { _cityProgress: { current: 0, total: effectiveEnd - startId } };

    let found = 0;
    let scanned = 0;
    let errors = 0;
    let consecutiveEmpty = 0;

    for (let id = startId; id <= effectiveEnd; id++) {
      scanned++;

      try {
        await rateLimiter.wait();
        const url = `${PROFILE_URL}?ID=${id}&b9100e1006f6=2`;
        const html = await this.httpGet(url);

        if (!html || html.length < 500) {
          consecutiveEmpty++;
          if (consecutiveEmpty > 500 && id > 25000) {
            log.info(`[CICC] 500 consecutive empty profiles at ID ${id}, stopping`);
            break;
          }
          continue;
        }

        consecutiveEmpty = 0;
        const lead = this._parseProfile(html, id);

        if (lead) {
          found++;
          yield this.transformResult(lead, practiceArea);

          if (found % 100 === 0) {
            log.info(`[CICC] Progress: ${found} found / ${scanned} scanned (ID: ${id})`);
            yield { _cityProgress: { current: scanned, total: effectiveEnd - startId } };
          }

          if (maxResults > 0 && found >= maxResults) {
            log.info(`[CICC] Reached max results: ${maxResults}`);
            break;
          }
        }
      } catch (err) {
        errors++;
        if (errors > 50) {
          log.warn(`[CICC] Too many errors (${errors}), pausing...`);
          await new Promise(r => setTimeout(r, 10000));
          errors = 0;
        }
      }
    }

    log.info(`[CICC] Complete: ${found} RCICs found from ${scanned} profiles scanned`);
  }
}

module.exports = new CICCRegistryScraper();
