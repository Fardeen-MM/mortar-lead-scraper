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
   * Each practice entry has an h2 with a link, followed by address, phone, email.
   */
  _parseListings($) {
    const results = [];

    // Find all practice links — they point to /find-a-vet-practice/{slug}/
    const practiceLinks = $('a[href*="/find-a-vet-practice/"]').filter((_, el) => {
      const href = $(el).attr('href') || '';
      // Must be a specific practice page, not the search/county pages
      return href.match(/\/find-a-vet-practice\/[a-z0-9][\w-]+-[a-z0-9]+\/?$/i) &&
             !href.includes('by-county') && !href.includes('by-accreditation') &&
             !href.includes('filter-');
    });

    // Process each practice link and its surrounding context
    const seen = new Set();
    practiceLinks.each((_, el) => {
      const $link = $(el);
      const href = $link.attr('href') || '';
      const name = $link.text().trim();

      if (!name || seen.has(href)) return;
      seen.add(href);

      // Build full profile URL
      let profileUrl = href;
      if (profileUrl.startsWith('/')) {
        profileUrl = BASE_URL + profileUrl;
      } else if (profileUrl.startsWith('./')) {
        profileUrl = BASE_URL + '/find-a-vet-practice/' + profileUrl.substring(2);
      }

      // --- Extract postcode from URL slug ---
      // Slugs end with the postcode: "bath-vets4pets-ltd-bathba2-1es"
      const slugPostcodeMatch = href.match(/([a-z]{1,2}\d[a-z\d]?)-?(\d[a-z]{2})\/?$/i);
      let urlPostcode = '';
      if (slugPostcodeMatch) {
        urlPostcode = (slugPostcodeMatch[1] + ' ' + slugPostcodeMatch[2]).toUpperCase();
      }

      // Get the parent container to find address/phone/email
      // The h2 link and sibling p tags are at the same DOM level
      let $container = $link.closest('div, li, article, section, tr');
      if (!$container.length) {
        // Walk up from the link to find the h2, then use its parent
        const $heading = $link.closest('h2, h3');
        if ($heading.length) {
          $container = $heading.parent();
        } else {
          $container = $link.parent().parent().parent();
        }
      }

      // Extract the text content near this practice listing
      const containerText = $container.text().replace(/\s+/g, ' ').trim();

      // --- Phone ---
      let phone = '';
      // Look for phone2 class or tel: links near this entry
      const $phone = $container.find('.phone2, a[href^="tel:"]');
      if ($phone.length) {
        phone = $phone.first().text().trim();
      }
      if (!phone) {
        // Try regex on container text
        const phoneMatch = containerText.match(/(\d{3,5}\s?\d{3,4}\s?\d{3,4})/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // --- Email (Cloudflare protected) ---
      let email = '';
      // Look for data-cfemail attribute
      const $cfEmail = $container.find('[data-cfemail]');
      if ($cfEmail.length) {
        const encoded = $cfEmail.attr('data-cfemail');
        email = this.decodeCloudflareEmail(encoded);
      }
      // Also check for email-protection links
      if (!email) {
        const $emailLink = $container.find('a[href*="email-protection"]');
        if ($emailLink.length) {
          const href2 = $emailLink.attr('href') || '';
          const cfMatch = href2.match(/email-protection#([a-f0-9]+)/i);
          if (cfMatch) {
            email = this.decodeCloudflareEmail(cfMatch[1]);
          }
        }
      }
      // Check for envelope class
      if (!email) {
        const $envelope = $container.find('.envelope, a[href*="mailto:"]');
        if ($envelope.length) {
          const mailHref = $envelope.attr('href') || '';
          if (mailHref.startsWith('mailto:')) {
            email = mailHref.replace('mailto:', '').trim();
          } else {
            const text = $envelope.text().trim();
            if (text.includes('@')) email = text;
          }
        }
      }

      // --- Address ---
      let address = '';
      let postcode = urlPostcode;
      let city = '';

      // Look for address text near the practice name
      const $paragraphs = $container.find('p');
      $paragraphs.each((_, p) => {
        const pText = $(p).text().trim();
        // Address usually contains a postcode
        const pcMatch = pText.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        if (pcMatch && !address) {
          address = pText;
          postcode = pcMatch[1].toUpperCase().replace(/(\S)(\d)/, '$1$2');
          // Ensure space in postcode
          if (postcode.length >= 5 && !postcode.includes(' ')) {
            postcode = postcode.slice(0, -3) + ' ' + postcode.slice(-3);
          }
        }
      });

      // Also scan full container text for postcode if not found
      if (!postcode) {
        const textPcMatch = containerText.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        if (textPcMatch) {
          postcode = textPcMatch[1].toUpperCase();
          if (postcode.length >= 5 && !postcode.includes(' ')) {
            postcode = postcode.slice(0, -3) + ' ' + postcode.slice(-3);
          }
        }
      }

      // Parse city from address or container text
      if (address) {
        const { city: parsedCity } = this._parseAddressLine(address, postcode);
        city = parsedCity;
      }
      // Fallback: extract city from slug before postcode
      if (!city && href) {
        // e.g., /bath-vets4pets-ltd-bathba2-1es/ → try to find city
        const slugParts = href.replace(/.*\/find-a-vet-practice\//, '').replace(/\/$/, '').split('-');
        // The text before the postcode portion is often the city
        if (slugParts.length >= 2) {
          // Try to match known city name at start or middle of slug
          const fullSlug = slugParts.join(' ');
          // Try to extract from container text — look for text after address lines
          const cityMatch = containerText.match(/(?:^|\s)(London|Bristol|Bath|Manchester|Birmingham|Edinburgh|Glasgow|Cardiff|Belfast|Leeds|Liverpool|Sheffield|Newcastle|Nottingham|Oxford|Cambridge|Brighton|York|Exeter|Plymouth|Southampton|Norwich|Chester|Derby|Leicester|Worcester|Swansea|Aberdeen|Dundee|Inverness)(?:\s|$)/i);
          if (cityMatch) city = cityMatch[1];
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
   */
  _normalizeUKPhone(phone) {
    if (!phone) return '';
    let digits = phone.replace(/\D/g, '');
    if (digits.startsWith('44') && digits.length > 10) {
      digits = '0' + digits.substring(2);
    }
    if (!digits.startsWith('0') && digits.length === 10) {
      digits = '0' + digits;
    }
    if (digits.length === 11) {
      if (digits.startsWith('020')) {
        return `${digits.substring(0, 3)} ${digits.substring(3, 7)} ${digits.substring(7)}`;
      }
      return `${digits.substring(0, 5)} ${digits.substring(5)}`;
    }
    return digits || phone;
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
