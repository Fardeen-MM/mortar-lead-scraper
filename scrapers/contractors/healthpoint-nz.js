/**
 * Healthpoint.co.nz — NZ Dental & Medical Practice Scraper
 *
 * Source: https://www.healthpoint.co.nz
 * Method: Cheerio HTML scraping (server-rendered pages)
 * Coverage: New Zealand — dental practices + GPs
 *
 * Strategy:
 *   1. Fetch listing pages with pagination (?services=N offset, 40 per page)
 *   2. Extract practice detail URLs from listing cards
 *   3. Fetch each detail page for email, phone, website, address
 *
 * Data available from detail pages:
 *   - Practice name (h1), email (mailto:), phone (tel:), website (external link)
 *   - Address: street, suburb, city, postcode (structured under "Street Address" heading)
 *
 * Confirmed: Emails present in plain HTML on individual practice pages.
 *
 * Categories scraped:
 *   - Dental:  /dentistry/general-dentist/
 *   - GP:      /gps-accident-urgent-medical-care/gp/
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.healthpoint.co.nz';
const RESULTS_PER_PAGE = 40;

/**
 * Category paths on Healthpoint.
 * Each key is a practice area; value is the URL path segment.
 */
const CATEGORY_PATHS = {
  'dental':       '/dentistry/general-dentist/',
  'gp':           '/gps-accident-urgent-medical-care/gp/',
};

class HealthpointNzScraper extends BaseScraper {
  constructor() {
    super({
      name: 'healthpoint_nz',
      stateCode: 'HEALTHPOINT-NZ',
      baseUrl: BASE_URL,
      pageSize: RESULTS_PER_PAGE,
      practiceAreaCodes: {
        'dental': 'Dental',
        'dentist': 'Dental',
        'dentistry': 'Dental',
        'gp': 'GP',
        'doctor': 'GP',
        'general practice': 'GP',
        'medical': 'GP',
      },
      defaultCities: [], // Not city-based — scrapes all NZ listings
    });
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override CAPTCHA detection — Healthpoint doesn't use CAPTCHAs but may
   * have the word in meta tags or scripts. Only trigger on actual blocking.
   */
  detectCaptcha(body) {
    return body.includes('challenge-form') ||
           body.includes('Just a moment') ||
           body.includes('cf-browser-verification');
  }

  /**
   * Override transformResult to use our source name.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'healthpoint_nz';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Parse a listing page to extract all practice detail URLs.
   *
   * @param {string} html - Raw HTML of a listing page
   * @param {string} categoryPath - The category URL path (e.g., '/dentistry/general-dentist/')
   * @returns {{ urls: string[], totalCount: number }}
   */
  _parseListingPage(html, categoryPath) {
    const $ = cheerio.load(html);
    const urls = [];

    // Extract total result count from page text (e.g., "868 results")
    let totalCount = 0;
    const bodyText = $('body').text();
    const countMatch = bodyText.match(/([\d,]+)\s*results/i);
    if (countMatch) {
      totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);
    }

    // Find all practice links — they contain the category path in the href
    $('a[href]').each((_, el) => {
      const href = ($(el).attr('href') || '').trim();
      // Must be under the category path and have a trailing slug
      // e.g., /dentistry/general-dentist/the-dental-practice-limited/
      if (!href.startsWith(categoryPath)) return;
      // Must have a slug after the category path (not just the category itself)
      const slug = href.slice(categoryPath.length).replace(/\/$/, '');
      if (!slug || slug.includes('?')) return;
      // Skip pagination links and anchors
      if (slug.includes('#')) return;

      const fullUrl = `${BASE_URL}${href}`;
      if (!urls.includes(fullUrl)) {
        urls.push(fullUrl);
      }
    });

    return { urls, totalCount };
  }

  /**
   * Parse a practice detail page for contact information.
   *
   * @param {string} html - Raw HTML of the detail page
   * @returns {object} Extracted practice data
   */
  _parseDetailPage(html) {
    const $ = cheerio.load(html);
    const result = {
      first_name: '',
      last_name: '',
      firm_name: '',
      email: '',
      phone: '',
      website: '',
      address: '',
      city: '',
      state: 'NZ',
      zip: '',
      bar_number: '',
      bar_status: '',
      profile_url: '',
      source: 'healthpoint_nz',
    };

    // Practice name from h1
    result.firm_name = ($('h1').first().text() || '').trim();

    // Email from mailto: link
    $('a[href^="mailto:"]').each((_, el) => {
      if (result.email) return;
      const href = ($(el).attr('href') || '').replace('mailto:', '').trim().toLowerCase();
      if (href && href.includes('@') && !href.includes('healthpoint.co.nz')) {
        result.email = href.split('?')[0]; // Strip query params
      }
    });

    // Phone from tel: link
    $('a[href^="tel:"]').each((_, el) => {
      if (result.phone) return;
      const phoneText = $(el).text().trim();
      if (phoneText && phoneText.length >= 7) {
        result.phone = phoneText;
      }
    });

    // Website — look for external links (not healthpoint.co.nz, not tel/mailto)
    $('a[href^="http"]').each((_, el) => {
      if (result.website) return;
      const href = ($(el).attr('href') || '').trim();
      if (
        href &&
        !href.includes('healthpoint.co.nz') &&
        !href.includes('google.com/maps') &&
        !href.includes('maps.google') &&
        !href.includes('facebook.com') &&
        !href.includes('instagram.com') &&
        !href.includes('twitter.com') &&
        !href.includes('youtube.com') &&
        !href.includes('linkedin.com') &&
        !href.includes('tiktok.com') &&
        !href.includes('apple.com/maps')
      ) {
        result.website = href;
      }
    });

    // Address extraction — look for address-like content near "Street Address"
    // The structure is: "Street Address" heading followed by lines with street, suburb, city postcode
    const fullText = $('body').text();

    // Try structured extraction: after "Street Address" text
    const streetAddrMatch = fullText.match(/Street Address\s*\n?\s*(.+?)(?:\n\s*(.+?))?(?:\n\s*([A-Za-z\s]+)\s+(\d{4}))?/);
    if (streetAddrMatch) {
      result.address = (streetAddrMatch[1] || '').trim();
      const suburb = (streetAddrMatch[2] || '').trim();
      const cityAndPost = (streetAddrMatch[3] || '').trim();
      const postcode = (streetAddrMatch[4] || '').trim();

      if (postcode) {
        result.zip = postcode;
        result.city = cityAndPost || suburb;
      } else if (suburb) {
        result.city = suburb;
      }
    }

    // Fallback: try to find NZ postcode pattern (4 digits) near a city name
    if (!result.city) {
      // Look for "CityName NNNN" pattern (NZ postcodes are 4 digits)
      const cityPostMatch = fullText.match(/(Auckland|Wellington|Christchurch|Hamilton|Tauranga|Dunedin|Napier|Hastings|Palmerston North|Nelson|Rotorua|New Plymouth|Whangarei|Invercargill|Whanganui|Gisborne|Blenheim|Timaru|Taupo|Masterton|Levin|Ashburton|Pukekohe|Tokoroa|Queenstown|Whakatane|Kapiti|Porirua|Upper Hutt|Lower Hutt|Hutt|Manukau|North Shore|Waitakere|Papakura|Howick|Takapuna|Albany|Botany|Mangere|Otara|Papatoetoe|Onehunga|Mt Eden|Ponsonby|Remuera|Epsom|Parnell|Newmarket|Grey Lynn|Kingsland|Mt Albert|Avondale|Henderson|Te Atatu|Ranui|Massey|Kumeu|Helensville|Orewa|Hibiscus Coast|Silverdale|Whangaparaoa|Devonport|Birkenhead|Glenfield|Browns Bay|Milford|Campbells Bay|Mairangi Bay|Murrays Bay|Torbay|Long Bay|East Tamaki|Flat Bush|Dannemora|Pakuranga|Bucklands Beach|Half Moon Bay|Cockle Bay|Beachlands|Maraetai|Clevedon|Karaka|Drury|Ramarama|Bombay|Pokeno|Tuakau|Pukekohe|Waiuku|Patumahoe|Clarks Beach|Kaiaua|Miranda|Thames|Whitianga|Tairua|Whangamata|Pauanui|Coromandel|Cambridge|Te Awamutu|Morrinsville|Matamata|Huntly|Ngaruawahia|Raglan|Te Kuiti|Otorohanga|Waitomo|Putaruru|Mangakino|South Waikato|Waihi|Katikati|Te Puke|Mt Maunganui|Papamoa|Omokoroa|Opotiki|Kawerau|Murupara|Edgecumbe|Awakeri|Taneatua|Ruatoki|Galatea|Reporoa|Wairoa|Havelock North|Waipawa|Waipukurau|Dannevirke|Woodville|Pahiatua|Eketahuna|Carterton|Greytown|Martinborough|Featherston|Paraparaumu|Waikanae|Otaki|Shannon|Foxton|Bulls|Marton|Taihape|Ohakune|Raetihi|Wanganui|Hawera|Stratford|Inglewood|Waitara|Motueka|Richmond|Takaka|Murchison|Westport|Greymouth|Hokitika|Reefton|Rangiora|Kaiapoi|Woodend|Oxford|Amberley|Hanmer Springs|Cheviot|Kaikoura|Darfield|Lincoln|Rolleston|Prebbleton|Lyttelton|Akaroa|Geraldine|Temuka|Fairlie|Twizel|Oamaru|Waimate|Kurow|Palmerston|Wanaka|Cromwell|Alexandra|Clyde|Roxburgh|Balclutha|Milton|Lawrence|Gore|Winton|Riverton|Te Anau|Bluff|Stewart Island|Warkworth|Wellsford|Matakana|Snells Beach|Algies Bay|Sandspit|Leigh|Mangawhai|Kaiwaka|Maungaturoto|Dargaville|Kaikohe|Kerikeri|Paihia|Kaitaia|Rawene|Opononi|Hokianga|Kawakawa)\s+(\d{4})/);
      if (cityPostMatch) {
        result.city = cityPostMatch[1].trim();
        result.zip = cityPostMatch[2];
      }
    }

    // Also try simpler pattern: any text + 4-digit NZ postcode at end of a line
    if (!result.zip) {
      const postMatch = fullText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(\d{4})\b/);
      if (postMatch) {
        if (!result.city) result.city = postMatch[1].trim();
        result.zip = postMatch[2];
      }
    }

    // Try to get address from the map/directions link
    if (!result.address) {
      $('a[href*="maps.google"], a[href*="google.com/maps"]').each((_, el) => {
        if (result.address) return;
        const href = $(el).attr('href') || '';
        const daddrMatch = href.match(/daddr=([^&]+)/);
        if (daddrMatch) {
          const addr = decodeURIComponent(daddrMatch[1]).replace(/,\s*/g, ', ');
          result.address = addr;
        }
      });
    }

    // Try the location breadcrumb link for address
    if (!result.address) {
      $('a[href^="/"]').each((_, el) => {
        if (result.address) return;
        const href = $(el).attr('href') || '';
        // Pattern: /123-street-name-suburb-city/
        if (/^\/\d+-[a-z]/.test(href)) {
          const text = $(el).text().trim();
          if (text && text.includes(',')) {
            result.address = text;
          }
        }
      });
    }

    return result;
  }

  /**
   * Main search generator — scrapes Healthpoint listing + detail pages.
   *
   * @param {string} practiceArea - 'dental', 'gp', or empty (scrapes both)
   * @param {object} options - Search options
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Determine which categories to scrape
    let categoriesToScrape = [];
    const practiceKey = (practiceArea || '').toLowerCase().trim();

    if (practiceKey && CATEGORY_PATHS[practiceKey]) {
      categoriesToScrape = [{ key: practiceKey, path: CATEGORY_PATHS[practiceKey] }];
    } else if (practiceKey) {
      // Try to resolve via practiceAreaCodes
      const resolved = this.resolvePracticeCode(practiceArea);
      if (resolved === 'Dental') {
        categoriesToScrape = [{ key: 'dental', path: CATEGORY_PATHS['dental'] }];
      } else if (resolved === 'GP') {
        categoriesToScrape = [{ key: 'gp', path: CATEGORY_PATHS['gp'] }];
      } else {
        log.warn(`Healthpoint: Unknown practice area "${practiceArea}" — scraping all categories`);
        categoriesToScrape = Object.entries(CATEGORY_PATHS).map(([key, path]) => ({ key, path }));
      }
    } else {
      // No practice area specified — scrape all
      categoriesToScrape = Object.entries(CATEGORY_PATHS).map(([key, path]) => ({ key, path }));
    }

    log.scrape(`Healthpoint NZ: scraping ${categoriesToScrape.map(c => c.key).join(', ')}`);

    const seenUrls = new Set();
    let totalYielded = 0;
    let totalEmails = 0;

    for (let ci = 0; ci < categoriesToScrape.length; ci++) {
      const category = categoriesToScrape[ci];
      const categoryLabel = category.key.toUpperCase();
      yield { _cityProgress: { current: ci + 1, total: categoriesToScrape.length } };

      log.scrape(`--- Scraping category: ${categoryLabel} (${category.path}) ---`);

      // Phase 1: Collect all practice URLs from listing pages
      const practiceUrls = [];
      let page = 0;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${categoryLabel}`);
          break;
        }

        const offset = page * RESULTS_PER_PAGE;
        const url = page === 0
          ? `${BASE_URL}${category.path}`
          : `${BASE_URL}${category.path}?services=${offset}`;

        log.info(`Listing page ${page + 1} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Healthpoint listing request failed: ${err.message}`);
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Healthpoint returned ${response.statusCode} — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Healthpoint returned ${response.statusCode} on listing page`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on listing page — stopping`);
          yield { _captcha: true, city: categoryLabel, page: page + 1 };
          break;
        }

        const { urls, totalCount } = this._parseListingPage(response.body, category.path);

        if (page === 0 && totalCount > 0) {
          totalResults = totalCount;
          const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
          log.success(`Found ${totalResults.toLocaleString()} ${categoryLabel} practices (${totalPages} listing pages)`);
        }

        if (urls.length === 0) {
          log.info(`No more practice URLs on page ${page + 1} — done with listing collection`);
          break;
        }

        // Add new URLs (dedup against already seen)
        let newCount = 0;
        for (const u of urls) {
          if (!seenUrls.has(u)) {
            seenUrls.add(u);
            practiceUrls.push(u);
            newCount++;
          }
        }

        log.info(`Page ${page + 1}: ${urls.length} links found, ${newCount} new (${practiceUrls.length} total collected)`);

        // Check if we've exhausted pages
        if (totalResults > 0 && (page + 1) * RESULTS_PER_PAGE >= totalResults) {
          log.success(`Collected all listing pages for ${categoryLabel}`);
          break;
        }

        // If we got fewer URLs than expected, we're likely on the last page
        if (urls.length < 10) {
          log.info(`Few results on page ${page + 1} — likely last page`);
          break;
        }

        page++;
        pagesFetched++;
      }

      log.success(`Collected ${practiceUrls.length} ${categoryLabel} practice URLs — now fetching details`);

      // Phase 2: Fetch each practice detail page
      for (let i = 0; i < practiceUrls.length; i++) {
        const practiceUrl = practiceUrls[i];

        if ((i + 1) % 25 === 0 || i === 0) {
          log.info(`Fetching detail ${i + 1}/${practiceUrls.length} (${totalEmails} emails so far)`);
        }

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(practiceUrl, rateLimiter);
        } catch (err) {
          log.warn(`Failed to fetch ${practiceUrl}: ${err.message}`);
          continue;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Healthpoint returned ${response.statusCode} on detail page — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) {
            // Retry this URL
            i--;
            continue;
          }
          log.error(`Gave up after backoff — skipping remaining details`);
          break;
        }

        if (response.statusCode !== 200) {
          log.warn(`Detail page returned ${response.statusCode}: ${practiceUrl}`);
          continue;
        }

        rateLimiter.resetBackoff();

        try {
          const lead = this._parseDetailPage(response.body);
          lead.profile_url = practiceUrl;
          lead.practice_area = category.key === 'dental' ? 'Dental' : 'GP';

          // Skip if we couldn't even get the name
          if (!lead.firm_name) {
            log.warn(`No firm name found on ${practiceUrl} — skipping`);
            continue;
          }

          if (lead.email) totalEmails++;

          yield this.transformResult(lead, lead.practice_area);
          totalYielded++;
        } catch (err) {
          log.warn(`Error parsing detail page ${practiceUrl}: ${err.message}`);
        }
      }

      log.success(`Completed ${categoryLabel}: ${practiceUrls.length} practices scraped`);
    }

    log.success(`Healthpoint NZ scrape complete — ${totalYielded} practices, ${totalEmails} emails`);
  }
}

module.exports = new HealthpointNzScraper();
