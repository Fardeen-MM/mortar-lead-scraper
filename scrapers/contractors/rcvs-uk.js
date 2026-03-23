/**
 * RCVS UK — Find a Vet Practice Scraper
 *
 * Source: https://findavet.rcvs.org.uk/find-a-vet-practice/
 * Records: ~5,000+ registered UK veterinary practices
 * Method: HTTP + Cheerio — browse by county, paginate, extract contact info
 *
 * Strategy:
 *   1. Browse /find-a-vet-practice/by-county/{county}/ for each UK county
 *   2. Paginate via &p=1, &p=2, etc. (10 results per page)
 *   3. Extract practice name, address, phone, email from listing cards
 *   4. Emails use Cloudflare protection — decode via data-cfemail attribute
 *   5. Optionally fetch profile pages for website URL + full details
 *
 * Profile page URL: /find-a-vet-practice/{practice-name-postcode}/
 * Profile contains: name, phone (tel: link), email (CF protected), website,
 *   full address, opening hours, animals treated, accreditation status
 *
 * Dedup: by profile_url (contains practice slug + postcode)
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://findavet.rcvs.org.uk';
const PER_PAGE = 10;

// Practice area codes — maps to animal types for filtering
const PRACTICE_AREA_CODES = {
  'all': 'all',
  'small animal': 'small-animal',
  'equine': 'equine',
  'farm': 'farm',
  'exotic': 'exotic',
  'veterinary': 'all',
  'vet': 'all',
};

// Major UK counties — curated to cover the whole UK without excessive duplication
const UK_COUNTIES = [
  // England — major counties
  'avon', 'bedfordshire', 'berkshire', 'bristol', 'buckinghamshire',
  'cambridgeshire', 'cheshire', 'cleveland', 'cornwall', 'cumbria',
  'derbyshire', 'devon', 'dorset', 'county-durham', 'east-sussex',
  'essex', 'gloucestershire', 'greater-london', 'greater-manchester',
  'hampshire', 'herefordshire', 'hertfordshire', 'humberside',
  'kent', 'lancashire', 'leicestershire', 'lincolnshire',
  'merseyside', 'middlesex', 'norfolk', 'northamptonshire',
  'northumberland', 'north-yorkshire', 'nottinghamshire',
  'oxfordshire', 'rutland', 'shropshire', 'somerset',
  'south-yorkshire', 'staffordshire', 'suffolk', 'surrey',
  'east-yorkshire', 'tyne-and-wear', 'warwickshire',
  'west-midlands', 'west-sussex', 'west-yorkshire', 'wiltshire',
  'worcestershire',
  // Wales
  'carmarthenshire', 'ceredigion', 'clwyd', 'conwy', 'denbighshire',
  'flintshire', 'glamorgan', 'gwent', 'gwynedd', 'pembrokeshire', 'powys',
  // Scotland
  'aberdeenshire', 'angus', 'argyll', 'ayrshire', 'dumfriesshire',
  'edinburgh', 'fife', 'glasgow', 'highland', 'lanarkshire',
  'perthshire', 'renfrewshire', 'stirlingshire',
  // Northern Ireland
  'co-antrim', 'co-armagh', 'co-derry', 'co-down', 'co-fermanagh', 'co-tyrone',
  // Islands
  'channel-islands', 'isle-of-man', 'isle-of-wight', 'orkney', 'shetland',
];

class RcvsUkScraper extends BaseScraper {
  constructor() {
    super({
      name: 'rcvs_uk_vets',
      stateCode: 'RCVS-UK',
      baseUrl: BASE_URL,
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: [],
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('rcvs-uk: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('rcvs-uk: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('rcvs-uk: extractResultCount() not used — search() overridden'); }

  transformResult(lead, practiceArea) {
    lead.source = 'rcvs_uk_vets';
    if (practiceArea && practiceArea !== 'all') {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Extract total result count from "Page X of Y" text.
   * Also checks for total count text like "281 results".
   */
  _extractPageCount($) {
    let totalPages = 1;
    // Look for "Page X of Y" pattern
    const pageText = $('body').text();
    const pageMatch = pageText.match(/Page\s+\d+\s+of\s+(\d+)/i);
    if (pageMatch) {
      totalPages = parseInt(pageMatch[1], 10);
    }
    // Also check pagination links for highest page number
    $('a[href*="&p="]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const pMatch = href.match(/[&?]p=(\d+)/);
      if (pMatch) {
        const p = parseInt(pMatch[1], 10);
        if (p > totalPages) totalPages = p;
      }
    });
    return totalPages;
  }

  /**
   * Parse listing cards from a county/search results page.
   *
   * HTML structure:
   *   h2.item-title > a (practice name + href)
   *   div.item-address (address text + span.u-nowrap postcode)
   *   div.item-contact > span.item-contact-tel (phone)
   *   div.item-contact > a.item-contact-email[href^="mailto:"] (email)
   *   OR: Cloudflare-protected email via [data-cfemail] / email-protection link
   */
  _parseListings($) {
    const results = [];
    const seen = new Set();

    // Use the known CSS selector for practice title links
    $('h2.item-title a, a[href*="/find-a-vet-practice/"]').each((_, el) => {
      const $link = $(el);
      const href = $link.attr('href') || '';
      const name = $link.text().trim();

      // Must be a specific practice page
      if (!name || !href.match(/\/find-a-vet-practice\/[a-z0-9][\w-]+-[a-z0-9]+\/?$/i)) return;
      if (href.includes('by-county') || href.includes('by-accreditation') || href.includes('filter-')) return;
      if (seen.has(href)) return;
      seen.add(href);

      // Build full profile URL
      let profileUrl = href;
      if (profileUrl.startsWith('/')) {
        profileUrl = BASE_URL + profileUrl;
      }

      // Get parent container — walk up from h2 to the listing wrapper
      const $heading = $link.closest('h2');
      let $container = $heading.length ? $heading.parent() : $link.parent().parent().parent();

      // --- Address ---
      let address = '';
      let city = '';
      let postcode = '';

      // Try .item-address div first
      const $addr = $container.find('.item-address');
      if ($addr.length) {
        address = $addr.text().replace(/\s+/g, ' ').trim();
        // Postcode from span.u-nowrap
        const $pc = $addr.find('.u-nowrap, span');
        if ($pc.length) {
          postcode = $pc.text().trim().toUpperCase();
        }
        // Parse city: address format is "Street, City, County POSTCODE"
        const addrWithoutPC = address.replace(postcode, '').trim().replace(/,\s*$/, '');
        const parts = addrWithoutPC.split(',').map(p => p.trim()).filter(Boolean);
        if (parts.length >= 2) {
          // City is usually the second-to-last part (before county)
          // County is last, street is first
          city = parts.length >= 3 ? parts[parts.length - 2] : parts[parts.length - 1];
        } else if (parts.length === 1) {
          city = parts[0];
        }
      }

      // Fallback: extract postcode from URL slug
      if (!postcode) {
        const slugPcMatch = href.match(/([a-z]{1,2}\d[a-z\d]?)-?(\d[a-z]{2})\/?$/i);
        if (slugPcMatch) {
          postcode = (slugPcMatch[1] + ' ' + slugPcMatch[2]).toUpperCase();
        }
      }

      // Ensure postcode has a space
      if (postcode && postcode.length >= 5 && !postcode.includes(' ')) {
        postcode = postcode.slice(0, -3) + ' ' + postcode.slice(-3);
      }

      // --- Phone ---
      let phone = '';
      const $phoneTel = $container.find('.item-contact-tel');
      if ($phoneTel.length) {
        phone = $phoneTel.text().replace(/\s+/g, ' ').trim();
      }
      if (!phone) {
        // Fallback: any phone-like text
        const containerText = $container.text();
        const phoneMatch = containerText.match(/(\d{4,5}\s?\d{3,4}\s?\d{3,4})/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // --- Email ---
      let email = '';
      // Try mailto: link first (works when Cloudflare doesn't protect)
      const $mailto = $container.find('a.item-contact-email[href^="mailto:"], a[href^="mailto:"]');
      if ($mailto.length) {
        email = $mailto.attr('href').replace('mailto:', '').split('?')[0].trim();
      }
      // Try Cloudflare-protected email
      if (!email) {
        const $cfEmail = $container.find('[data-cfemail]');
        if ($cfEmail.length) {
          email = this.decodeCloudflareEmail($cfEmail.attr('data-cfemail'));
        }
      }
      if (!email) {
        const $cfLink = $container.find('a[href*="email-protection"]');
        if ($cfLink.length) {
          const cfHref = $cfLink.attr('href') || '';
          const cfMatch = cfHref.match(/email-protection#([a-f0-9]+)/i);
          if (cfMatch) {
            email = this.decodeCloudflareEmail(cfMatch[1]);
          }
        }
      }

      results.push({
        first_name: '',
        last_name: '',
        firm_name: name,
        address: address,
        city: city,
        state: 'UK',
        zip: postcode,
        country: 'UK',
        phone: this._normalizeUKPhone(phone),
        email: (email || '').toLowerCase().trim(),
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl,
        practice_area: 'Veterinary',
        trade_category: 'Veterinary',
        license_type: 'Vet Practice',
      });
    });

    return results;
  }

  /**
   * Parse address line into city component.
   * Format: "85 Earls Court Road, London W8 6EF"
   */
  _parseAddressLine(address, postcode) {
    if (!address) return { city: '' };

    // Remove postcode from address for city parsing
    let remaining = address;
    if (postcode) {
      remaining = remaining.replace(postcode, '').trim().replace(/,\s*$/, '');
    }

    // Split by comma, city is usually the last meaningful part
    const parts = remaining.split(',').map(p => p.trim()).filter(Boolean);
    let city = '';

    if (parts.length >= 2) {
      // City is usually the second-to-last or last part
      city = parts[parts.length - 1];
      // If it looks like a street (has numbers), use the previous part
      if (/^\d/.test(city) && parts.length >= 3) {
        city = parts[parts.length - 2];
      }
    } else if (parts.length === 1) {
      // Try to extract city from single line
      const words = parts[0].split(/\s+/);
      if (words.length >= 2) {
        // Last word(s) before postcode might be the city
        city = parts[0];
      }
    }

    return { city };
  }

  /**
   * Normalize UK phone numbers.
   * Input: "01225 472 960" or "020 7937 8215" or "+44 1225 472960"
   * Output: "01225 472960" or "020 7937 8215"
   */
  _normalizeUKPhone(phone) {
    if (!phone) return '';
    // Strip SVG/icon text that might precede the number
    const cleaned = phone.replace(/phone\d*/i, '').trim();
    let digits = cleaned.replace(/\D/g, '');
    // Remove +44 prefix
    if (digits.startsWith('44') && digits.length > 10) {
      digits = '0' + digits.substring(2);
    }
    // Add leading 0 if missing and exactly 10 digits
    if (!digits.startsWith('0') && digits.length === 10) {
      digits = '0' + digits;
    }
    if (digits.length === 11) {
      if (digits.startsWith('020')) {
        return `${digits.substring(0, 3)} ${digits.substring(3, 7)} ${digits.substring(7)}`;
      }
      if (digits.startsWith('07')) {
        return `${digits.substring(0, 5)} ${digits.substring(5)}`;
      }
      return `${digits.substring(0, 5)} ${digits.substring(5)}`;
    }
    return digits || cleaned;
  }

  /**
   * Fetch and parse a profile page for website URL and detailed address.
   */
  parseProfilePage($) {
    const result = {};

    // Website
    const $websiteLink = $('a[href^="http"]').filter((_, el) => {
      const href = $(el).attr('href') || '';
      const text = $(el).text().trim().toLowerCase();
      return (text === 'website' || text.includes('visit website') || text.includes('practice website')) &&
             !href.includes('rcvs.org.uk') && !href.includes('findavet');
    });
    if ($websiteLink.length) {
      result.website = $websiteLink.first().attr('href');
    }

    // Also look for any external link that's not RCVS
    if (!result.website) {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        if (!this.isExcludedDomain(href) && !href.includes('rcvs.org.uk') &&
            !href.includes('findavet') && !href.includes('google.com') &&
            !href.includes('maps.google')) {
          if (!result.website) result.website = href;
        }
      });
    }

    // Phone from tel: link
    const $tel = $('a[href^="tel:"]');
    if ($tel.length) {
      result.phone = this._normalizeUKPhone($tel.first().text().trim());
    }

    // Email from Cloudflare protection
    const $cfEmail = $('[data-cfemail]');
    if ($cfEmail.length) {
      result.email = this.decodeCloudflareEmail($cfEmail.attr('data-cfemail')).toLowerCase();
    }

    // Full address with postcode
    const bodyText = $('body').text();
    const postcodeMatch = bodyText.match(/([A-Z]{1,2}\d[A-Z\d]?\s+\d[A-Z]{2})/);
    if (postcodeMatch) {
      result.zip = postcodeMatch[1];
    }

    return result;
  }

  /**
   * Main search generator.
   * Browses RCVS by county, paginates through results, extracts practice data.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const counties = options.city ? [options.city.toLowerCase().replace(/\s+/g, '-')] : UK_COUNTIES;
    const maxCounties = options.maxCities ? Math.min(options.maxCities, counties.length) : counties.length;
    const maxPages = options.maxPages || Infinity;

    log.scrape(`[RCVS-UK] Starting scrape: ${maxCounties} counties, maxPages=${maxPages}`);

    const seenUrls = new Set();
    let totalYielded = 0;
    let totalWithEmail = 0;
    let totalWithPhone = 0;
    let totalWithWebsite = 0;

    for (let ci = 0; ci < maxCounties; ci++) {
      const county = counties[ci];
      yield { _cityProgress: { current: ci + 1, total: maxCounties } };

      log.info(`[RCVS-UK] County ${ci + 1}/${maxCounties}: ${county}`);

      // Fetch first page
      const firstPageUrl = `${BASE_URL}/find-a-vet-practice/by-county/${county}/`;
      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpGet(firstPageUrl, rateLimiter);
      } catch (err) {
        log.warn(`[RCVS-UK] Failed to fetch ${county}: ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`[RCVS-UK] ${county} returned HTTP ${response.statusCode}`);
        continue;
      }

      let $ = cheerio.load(response.body);
      const totalPages = Math.min(this._extractPageCount($), maxPages);

      // Parse first page
      const page1Results = this._parseListings($);
      let countyYielded = 0;

      for (const lead of page1Results) {
        if (lead.profile_url && seenUrls.has(lead.profile_url)) continue;
        if (lead.profile_url) seenUrls.add(lead.profile_url);

        if (lead.email) totalWithEmail++;
        if (lead.phone) totalWithPhone++;

        yield this.transformResult(lead, practiceArea);
        totalYielded++;
        countyYielded++;
      }

      // Paginate remaining pages
      for (let page = 2; page <= totalPages; page++) {
        const pageUrl = `${BASE_URL}/find-a-vet-practice/by-county/${county}/?p=${page}`;

        try {
          await rateLimiter.wait();
          response = await this.httpGet(pageUrl, rateLimiter);
        } catch (err) {
          log.warn(`[RCVS-UK] Failed to fetch ${county} page ${page}: ${err.message}`);
          continue;
        }

        if (response.statusCode !== 200) {
          log.warn(`[RCVS-UK] ${county} page ${page} returned HTTP ${response.statusCode}`);
          continue;
        }

        $ = cheerio.load(response.body);
        const results = this._parseListings($);

        if (results.length === 0) {
          log.info(`[RCVS-UK] ${county} page ${page}: no results — stopping`);
          break;
        }

        for (const lead of results) {
          if (lead.profile_url && seenUrls.has(lead.profile_url)) continue;
          if (lead.profile_url) seenUrls.add(lead.profile_url);

          if (lead.email) totalWithEmail++;
          if (lead.phone) totalWithPhone++;

          yield this.transformResult(lead, practiceArea);
          totalYielded++;
          countyYielded++;
        }
      }

      if (countyYielded > 0) {
        log.info(`[RCVS-UK] ${county}: ${countyYielded} practices (${totalYielded} total)`);
      }
    }

    log.success(`[RCVS-UK] Finished — ${totalYielded} practices (${totalWithEmail} with email, ${totalWithPhone} with phone)`);
  }
}

module.exports = new RcvsUkScraper();
