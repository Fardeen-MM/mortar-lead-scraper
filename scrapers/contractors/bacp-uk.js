/**
 * BACP UK — Therapist Directory Scraper
 *
 * Source: https://www.bacp.co.uk/search/Therapists
 * Records: ~50,000+ registered UK therapists
 * Method: HTTP + Cheerio — location-based search, paginate, fetch profiles for website
 *
 * Strategy:
 *   1. Search /search/Therapists?LocationQuery={city}&distance=25 for major UK cities
 *   2. Paginate via &skip=0, &skip=10, &skip=20, etc. (10 results per page)
 *   3. Extract therapist name + profile URL from search cards
 *   4. Fetch individual profile pages for website URL, qualifications, specialisms
 *   5. Email enrichment via website crawl (pipeline's email-finder handles this)
 *
 * Profile URL: /therapists/{id}/{name-slug}/{location-postcode}
 * Profile data: name, location, website, qualifications, session types, specialisms
 *
 * NOTE: No email or phone displayed on profiles. Website URLs enable pipeline
 * email enrichment via the waterfall/email-finder system.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.bacp.co.uk';
const PER_PAGE = 10;

const PRACTICE_AREA_CODES = {
  'all': '',
  'therapy': '',
  'counselling': '',
  'anxiety': 'Anxiety',
  'depression': 'Depression',
  'trauma': 'Trauma',
  'ptsd': 'Trauma',
  'couples': 'Relationships',
  'relationships': 'Relationships',
  'addiction': 'Addiction',
  'eating disorders': 'EatingDisorders',
  'bereavement': 'Bereavement',
  'stress': 'Stress',
  'ocd': 'OCD',
  'self-esteem': 'SelfEsteem',
};

// Major UK cities — spread across regions for comprehensive coverage
// Distance=25 miles means overlapping coverage is fine (dedup by profile URL)
const DEFAULT_CITIES = [
  // England
  'London', 'Manchester', 'Birmingham', 'Leeds', 'Bristol',
  'Liverpool', 'Sheffield', 'Newcastle', 'Nottingham', 'Southampton',
  'Oxford', 'Cambridge', 'Brighton', 'Leicester', 'Plymouth',
  'Norwich', 'Exeter', 'York', 'Bath', 'Coventry',
  'Milton Keynes', 'Swindon', 'Peterborough', 'Ipswich', 'Carlisle',
  // Wales
  'Cardiff', 'Swansea',
  // Scotland
  'Glasgow', 'Edinburgh', 'Aberdeen', 'Dundee', 'Inverness',
  // Northern Ireland
  'Belfast',
];

class BacpUkScraper extends BaseScraper {
  constructor() {
    super({
      name: 'bacp_uk_therapists',
      stateCode: 'BACP-UK',
      baseUrl: BASE_URL,
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('bacp-uk: buildSearchUrl() not used'); }
  parseResultsPage() { throw new Error('bacp-uk: parseResultsPage() not used'); }
  extractResultCount() { throw new Error('bacp-uk: extractResultCount() not used'); }

  transformResult(lead, practiceArea) {
    lead.source = 'bacp_uk_therapists';
    if (practiceArea && practiceArea !== 'all' && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Extract total result count from "Showing results X to Y of Z" text.
   */
  _extractTotalResults($) {
    const text = $('body').text();
    const match = text.match(/of\s+([\d,]+)\s*$/im) ||
                  text.match(/Showing\s+results?\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)/i) ||
                  text.match(/([\d,]+)\s+results?\s+found/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse therapist cards from search results page.
   * Returns array of { name, profileUrl, location, price, description }.
   */
  _parseSearchResults($) {
    const results = [];

    // Find search result cards — look for profile links
    $('a[href*="/therapists/"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      // Must match /therapists/{id}/{slug}/{location}
      if (!href.match(/\/therapists\/\d+\//)) return;

      const text = $(el).text().trim();
      if (!text || text === 'View profile') return;

      // Parse name and location from link text
      // Format: "Name - Location" or just "Name"
      let name = text;
      let location = '';
      const dashIdx = text.lastIndexOf(' - ');
      if (dashIdx > 0) {
        name = text.substring(0, dashIdx).trim();
        location = text.substring(dashIdx + 3).trim();
      }

      // Build full URL
      let profileUrl = href;
      if (profileUrl.startsWith('/')) {
        profileUrl = BASE_URL + profileUrl;
      }

      // Parse location from URL slug if not in text
      if (!location) {
        const slugMatch = href.match(/\/therapists\/\d+\/[^/]+\/([^/]+)/);
        if (slugMatch) {
          location = slugMatch[1].replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        }
      }

      // Parse postcode from location
      let postcode = '';
      const pcMatch = (location || '').match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d?[A-Z]{0,2})/i);
      if (pcMatch) {
        postcode = pcMatch[1].toUpperCase();
      }

      // Parse city from location
      let city = location.replace(/[A-Z]{1,2}\d[A-Z\d]?\s*\d?[A-Z]{0,2}/i, '').trim();

      results.push({
        name,
        profileUrl,
        city,
        postcode,
      });
    });

    // Deduplicate by profile URL
    const seen = new Set();
    return results.filter(r => {
      if (seen.has(r.profileUrl)) return false;
      seen.add(r.profileUrl);
      return true;
    });
  }

  /**
   * Fetch a profile page and extract website, qualifications, and details.
   */
  async _fetchProfile(profileUrl, rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(profileUrl, rateLimiter);

      if (response.statusCode !== 200) return null;

      const $ = cheerio.load(response.body);

      // Name from h1
      const name = $('h1').first().text().trim();

      // Website — look for external links
      let website = '';
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        if (website) return;
        if (href.includes('bacp.co.uk')) return;
        if (this.isExcludedDomain(href)) return;
        if (href.includes('tidycal.com') || href.includes('linktr.ee') ||
            href.includes('calendly.com')) return; // Skip booking links
        website = href;
      });

      // If no direct website, check for any URL in the page text
      if (!website) {
        $('a[href^="http"]').each((_, el) => {
          const href = $(el).attr('href') || '';
          if (website) return;
          if (href.includes('bacp.co.uk')) return;
          // Accept booking/linktree as fallback for email finding
          if (!this.isExcludedDomain(href)) {
            website = href;
          }
        });
      }

      // Qualifications
      let qualification = '';
      const bodyText = $('body').text();
      const qualMatch = bodyText.match(/Registered\s+Member\s+(MBACP(?:\s+\(Accred\))?)/i) ||
                        bodyText.match(/(MBACP(?:\s+\(Accred\))?)/);
      if (qualMatch) qualification = qualMatch[1];

      // Types of therapy
      let therapyTypes = '';
      $('h3').each((_, el) => {
        if ($(el).text().trim() === 'Types of therapy') {
          const next = $(el).next('p');
          if (next.length) therapyTypes = next.text().trim();
        }
      });

      // Session price
      let price = '';
      const priceMatch = bodyText.match(/Sessions?\s+from\s+£([\d.]+)/i);
      if (priceMatch) price = `£${priceMatch[1]}`;

      // Locations
      const locations = [];
      $('h3').each((_, el) => {
        const text = $(el).text().trim();
        const locMatch = text.match(/Therapist\s*-\s*(.+)/i) ||
                         text.match(/Counsellor\s*-\s*(.+)/i);
        if (locMatch) locations.push(locMatch[1].trim());
      });

      return { name, website, qualification, therapyTypes, price, locations };
    } catch (err) {
      return null;
    }
  }

  /**
   * Main search generator.
   * Searches BACP by city, paginates, fetches profiles for website URLs.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    let cities;
    if (options.city) {
      cities = [options.city];
    } else {
      cities = [...DEFAULT_CITIES];
    }
    if (options.maxCities) {
      cities = cities.slice(0, options.maxCities);
    }

    // Specialism filter
    let specialism = '';
    if (practiceArea) {
      const key = practiceArea.toLowerCase().trim();
      specialism = PRACTICE_AREA_CODES[key] || '';
    }

    const maxPages = options.maxPages || 50; // Cap at 50 pages per city (500 results)
    const seenUrls = new Set();
    let totalYielded = 0;
    let totalWithWebsite = 0;
    let totalProfilesFetched = 0;

    log.scrape(`[BACP-UK] Starting scrape: ${cities.length} cities, specialism="${specialism}"`);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };

      log.info(`[BACP-UK] City ${ci + 1}/${cities.length}: ${city}`);

      // Build search URL
      let searchBase = `${BASE_URL}/search/Therapists?LocationQuery=${encodeURIComponent(city)}&distance=25`;
      if (specialism) searchBase += `&Specialisms=${encodeURIComponent(specialism)}`;

      // Fetch first page to get total count
      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpGet(searchBase, rateLimiter);
      } catch (err) {
        log.warn(`[BACP-UK] Failed to fetch ${city}: ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`[BACP-UK] ${city} returned HTTP ${response.statusCode}`);
        continue;
      }

      let $ = cheerio.load(response.body);
      const totalResults = this._extractTotalResults($);
      const totalPages = Math.min(Math.ceil(totalResults / PER_PAGE), maxPages);

      if (totalResults === 0) {
        log.info(`[BACP-UK] ${city}: no results`);
        continue;
      }

      log.info(`[BACP-UK] ${city}: ${totalResults} results (${totalPages} pages)`);

      let cityYielded = 0;

      // Process all pages
      for (let page = 0; page < totalPages; page++) {
        const skip = page * PER_PAGE;

        if (page > 0) {
          const pageUrl = `${searchBase}&skip=${skip}`;
          try {
            await rateLimiter.wait();
            response = await this.httpGet(pageUrl, rateLimiter);
          } catch (err) {
            log.warn(`[BACP-UK] Failed to fetch ${city} skip=${skip}: ${err.message}`);
            continue;
          }

          if (response.statusCode !== 200) continue;
          $ = cheerio.load(response.body);
        }

        const cards = this._parseSearchResults($);
        if (cards.length === 0 && page > 0) break;

        for (const card of cards) {
          if (seenUrls.has(card.profileUrl)) continue;
          seenUrls.add(card.profileUrl);

          // Fetch profile page for website
          const profile = await this._fetchProfile(card.profileUrl, rateLimiter);
          totalProfilesFetched++;

          // Split name
          const { firstName, lastName } = this.splitName(profile?.name || card.name);

          const lead = {
            first_name: firstName,
            last_name: lastName,
            firm_name: '',
            city: card.city || (profile?.locations?.[0] || ''),
            state: 'UK',
            zip: card.postcode,
            country: 'UK',
            phone: '',
            email: '',
            website: profile?.website || '',
            bar_number: '',
            bar_status: profile?.qualification || 'MBACP',
            profile_url: card.profileUrl,
            practice_area: profile?.therapyTypes || 'Therapy',
            trade_category: 'Therapy',
            license_type: 'Therapist',
          };

          if (lead.website) totalWithWebsite++;

          yield this.transformResult(lead, practiceArea);
          totalYielded++;
          cityYielded++;
        }

        // Progress log every 5 pages
        if ((page + 1) % 5 === 0) {
          log.info(`[BACP-UK] ${city}: page ${page + 1}/${totalPages}, ${cityYielded} therapists`);
        }
      }

      if (cityYielded > 0) {
        log.info(`[BACP-UK] ${city}: ${cityYielded} therapists (${totalYielded} total)`);
      }
    }

    log.success(`[BACP-UK] Finished — ${totalYielded} therapists (${totalWithWebsite} with website, ${totalProfilesFetched} profiles fetched)`);
  }
}

module.exports = new BacpUkScraper();
