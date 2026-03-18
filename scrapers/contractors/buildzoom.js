/**
 * BuildZoom — US-wide contractor directory (4M+ licensed contractors)
 *
 * Source: https://www.buildzoom.com
 * Platform: Angular SPA with server-rendered HTML search results + JSON-LD profile pages
 *
 * Search strategy:
 *   1. GET /search?keywords={trade}+{city}&page={N}  → HTML listing page (25 results/page)
 *      - Extract contractor profile slugs from <a href="/contractor/{slug}"> links
 *      - Extract "Showing X - Y of the best Z experts" for total count
 *   2. GET /contractor/{slug}  → Profile page with JSON-LD schema.org data
 *      - RoofingContractor / GeneralContractor / Electrician / Plumber / etc. schema
 *      - telephone, address, aggregateRating, hasOfferCatalog, areaServed
 *      - EducationalOccupationalCredential (license info)
 *      - GovernmentPermit (building permits)
 *
 * Data available:
 *   - Business name, phone, full address (street, city, state, zip)
 *   - License number, type, status
 *   - BZ Score (proprietary quality score, higher = better, 90+ = top 50%)
 *   - Services offered, review count/rating
 *   - Owner/principal names
 *   - Building permit history
 *
 * Rate limits: No official API; polite 3-6s delays between requests.
 * No CAPTCHA observed on search or profile pages as of 2026-03.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.buildzoom.com';
const PAGE_SIZE = 25; // BuildZoom returns 25 results per page

class BuildZoomScraper extends BaseScraper {
  constructor() {
    super({
      name: 'buildzoom_contractors',
      stateCode: 'BUILDZOOM',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'general contractor': 'general-contractor',
        'roofing': 'roofing',
        'plumbing': 'plumbing',
        'painting': 'painting',
        'hvac': 'hvac',
        'electrical': 'electrical',
        'landscaping': 'landscaping',
        'kitchen remodel': 'kitchen-remodel',
        'bathroom remodel': 'bathroom-remodel',
        'flooring': 'flooring',
        'carpentry': 'carpentry',
        'concrete': 'concrete',
        'fencing': 'fencing',
        'siding': 'siding',
        'windows': 'windows',
        'solar': 'solar',
        'demolition': 'demolition',
        'insulation': 'insulation',
        'masonry': 'masonry',
        'drywall': 'drywall',
      },
      defaultCities: [
        'Miami, FL',
        'Houston, TX',
        'Los Angeles, CA',
        'Phoenix, AZ',
        'Dallas, TX',
        'Atlanta, GA',
        'Chicago, IL',
        'Denver, CO',
        'Seattle, WA',
        'Tampa, FL',
      ],
    });
  }

  /**
   * Build search URL for BuildZoom.
   * Format: /search?keywords={trade}+{city}&page={N}
   */
  buildSearchUrl({ city, practiceCode, page }) {
    // Build keyword: trade + city (e.g. "roofing miami fl")
    const trade = practiceCode || 'general contractor';
    const location = (city || '').replace(/,\s*/g, ' ').trim();
    const keywords = `${trade} ${location}`.trim();
    const encoded = encodeURIComponent(keywords);
    let url = `${BASE_URL}/search?keywords=${encoded}`;
    if (page && page > 1) {
      url += `&page=${page}`;
    }
    return url;
  }

  /**
   * Extract total result count from "Showing X - Y of the best Z experts" text.
   */
  extractResultCount($) {
    const text = $.text();
    const match = text.match(/of the best\s+([\d,]+)\s+experts/i);
    if (match) {
      return parseInt(match[1].replace(/,/g, ''), 10);
    }
    // Fallback: look for any "X experts" pattern
    const fallback = text.match(/([\d,]+)\s+experts/i);
    if (fallback) {
      return parseInt(fallback[1].replace(/,/g, ''), 10);
    }
    return 0;
  }

  /**
   * Parse search results page — extract contractor profile slugs.
   * BuildZoom search pages are Angular-rendered, but profile links are in the HTML.
   * We collect slugs and then fetch individual profiles for rich data.
   */
  parseResultsPage($) {
    const slugs = [];
    const seen = new Set();

    // Find all /contractor/ links
    $('a[href^="/contractor/"]').each((_, el) => {
      const href = $(el).attr('href');
      if (!href) return;

      // Extract slug from /contractor/{slug} — skip /contractor/search etc.
      const match = href.match(/^\/contractor\/([a-z0-9][\w-]+)$/i);
      if (!match) return;

      const slug = match[1].toLowerCase();

      // Skip non-contractor slugs (navigation/utility pages)
      if (['search', 'new', 'login', 'signup', 'register', 'claim'].includes(slug)) return;

      if (!seen.has(slug)) {
        seen.add(slug);
        slugs.push(slug);
      }
    });

    // Return slug objects (they'll be enriched via profile fetching in search())
    return slugs.map(slug => ({ _slug: slug }));
  }

  /**
   * Parse a contractor profile page for rich structured data.
   * Extracts JSON-LD schema, license info, BZ score, and HTML fields.
   */
  parseContractorProfile($) {
    const result = {};

    // --- 1. Extract JSON-LD structured data ---
    const jsonLdScripts = $('script[type="application/ld+json"]');
    let mainSchema = null;
    const credentials = [];
    const permits = [];

    jsonLdScripts.each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        let parsed = JSON.parse(raw);

        // Handle JSON-LD arrays (BuildZoom wraps schemas in an array)
        const items = Array.isArray(parsed) ? parsed : [parsed];

        for (const data of items) {
          if (!data || !data['@type'] || typeof data['@type'] !== 'string') continue;
          const type = data['@type'];

          // Main contractor schema (RoofingContractor, GeneralContractor, Electrician, etc.)
          if (type.includes('Contractor') || type === 'Electrician' || type === 'Plumber' ||
              type === 'HomeAndConstructionBusiness' || type === 'LocalBusiness' ||
              type === 'ProfessionalService') {
            mainSchema = data;
          }

          // License/credential info
          if (type === 'EducationalOccupationalCredential') {
            credentials.push(data);
          }

          // Building permits
          if (type === 'GovernmentPermit') {
            permits.push(data);
          }
        }
      } catch (e) {
        // Malformed JSON-LD — skip
      }
    });

    // --- 2. Extract fields from main schema ---
    if (mainSchema) {
      result.firm_name = mainSchema.name || '';
      result.phone = this._cleanPhone(mainSchema.telephone);

      if (mainSchema.address) {
        const addr = mainSchema.address;
        result.street = addr.streetAddress || '';
        result.city = addr.addressLocality || '';
        result.state = addr.addressRegion || '';
        result.zip = addr.postalCode || '';
      }

      if (mainSchema.geo) {
        result.latitude = mainSchema.geo.latitude;
        result.longitude = mainSchema.geo.longitude;
      }

      if (mainSchema.aggregateRating) {
        result.rating = mainSchema.aggregateRating.ratingValue;
        result.review_count = mainSchema.aggregateRating.reviewCount;
      }

      if (mainSchema.hasOfferCatalog && mainSchema.hasOfferCatalog.name) {
        result.services = mainSchema.hasOfferCatalog.name;
      }

      if (mainSchema.areaServed) {
        result.area_served = Array.isArray(mainSchema.areaServed)
          ? mainSchema.areaServed.join(', ')
          : String(mainSchema.areaServed);
      }

      // Website from schema (sameAs or url)
      if (mainSchema.sameAs) {
        const sameAs = Array.isArray(mainSchema.sameAs) ? mainSchema.sameAs : [mainSchema.sameAs];
        for (const url of sameAs) {
          if (url && !url.includes('buildzoom.com') && !this.isExcludedDomain(url)) {
            result.website = url;
            break;
          }
        }
      }
    }

    // --- 3. Extract license info from credentials ---
    if (credentials.length > 0) {
      // Find the first active license
      let activeLicense = credentials.find(c =>
        c.credentialCategory && /active/i.test(JSON.stringify(c))
      );
      if (!activeLicense) activeLicense = credentials[0];

      if (activeLicense) {
        result.bar_number = activeLicense.name || activeLicense.credentialId || '';
        if (activeLicense.credentialCategory) {
          result.license_type = activeLicense.credentialCategory;
        }
      }
      result.license_count = credentials.length;
    }

    // --- 4. Extract BZ Score from page text ---
    const pageText = $.text();
    const bzMatch = pageText.match(/(?:BZ\s*SCORE|BuildZoom\s*Score)[:\s]*(\d+)/i);
    if (bzMatch) {
      result.bz_score = parseInt(bzMatch[1], 10);
    }

    // Fallback: look for score in meta or specific elements
    if (!result.bz_score) {
      const scoreMatch = pageText.match(/ranks? in (?:the )?top (\d+)%/i);
      if (scoreMatch) {
        result.bz_percentile = parseInt(scoreMatch[1], 10);
      }
    }

    // --- 5. Extract owner/principal names from HTML ---
    const ownerNames = [];
    const textContent = pageText;
    // Look for "Owner" or "Licensee" labels followed by names
    const ownerMatch = textContent.match(/Owner[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/);
    if (ownerMatch) {
      ownerNames.push(ownerMatch[1].trim());
    }

    // Look for owner info in specific page sections
    $('td, dd, span, div').each((_, el) => {
      const text = $(el).text().trim();
      if (/^(?:Owner|Principal|Licensee|Qualifier)$/i.test(text)) {
        const next = $(el).next().text().trim();
        if (next && next.length < 60 && /[A-Z]/.test(next)) {
          ownerNames.push(next);
        }
      }
    });

    if (ownerNames.length > 0) {
      result.owner_name = ownerNames[0];
    }

    // --- 6. Extract website from HTML if not in JSON-LD ---
    if (!result.website) {
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim().toLowerCase();
        if ((text.includes('website') || text.includes('visit site') || text.includes('company website')) &&
            href.startsWith('http') && !href.includes('buildzoom.com')) {
          if (!this.isExcludedDomain(href)) {
            result.website = href;
            return false; // break
          }
        }
      });
    }

    // Also check for external links that look like contractor websites
    if (!result.website) {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        if (href.includes('buildzoom.com')) return;
        if (this.isExcludedDomain(href)) return;
        // Skip known non-website links
        if (href.includes('blockrenovation.com') || href.includes('project/new')) return;
        // If it looks like a company domain, use it
        if (/^https?:\/\/(?:www\.)?[a-z0-9][\w-]*\.[a-z]{2,}(?:\/)?$/i.test(href)) {
          result.website = href;
          return false; // break
        }
      });
    }

    // --- 7. Extract email from page if present ---
    const emailMatch = pageText.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
    if (emailMatch && !emailMatch[1].includes('buildzoom') && !emailMatch[1].includes('example')) {
      result.email = emailMatch[1];
    }

    // --- 8. Permit count ---
    const permitMatch = pageText.match(/([\d,]+)\s+(?:building\s+)?permits?\s+(?:on file|since)/i);
    if (permitMatch) {
      result.permit_count = parseInt(permitMatch[1].replace(/,/g, ''), 10);
    } else {
      result.permit_count = permits.length || 0;
    }

    // --- 9. Insurance info ---
    const insuranceMatch = pageText.match(/(?:General\s+Liability|Insured)[:\s]*(.+?)(?:\n|$)/i);
    if (insuranceMatch) {
      result.insurance = insuranceMatch[1].trim().substring(0, 100);
    }

    const bondMatch = pageText.match(/Bonded[:\s]*\$?([\d,]+)/i);
    if (bondMatch) {
      result.bond_amount = bondMatch[1].replace(/,/g, '');
    }

    return result;
  }

  /**
   * Clean phone number — strip extensions and formatting.
   */
  _cleanPhone(phone) {
    if (!phone) return '';
    // Remove extensions
    let cleaned = phone.replace(/\s*(?:ext\.?|x)\s*\d+/i, '').trim();
    // Keep just digits and standard formatting
    return cleaned;
  }

  /**
   * Custom search generator — fetches search pages, then enriches from profiles.
   *
   * Two-pass approach:
   *   Pass 1: Fetch search page → extract contractor slugs
   *   Pass 2: Fetch each contractor's profile page → extract JSON-LD data
   *
   * This gives us much richer data than parsing the search listing HTML alone.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);
    const trade = practiceCode || practiceArea || 'general contractor';

    const cities = this.getCities(options);
    const seenSlugs = new Set(); // Global dedup across cities

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`BuildZoom: Searching ${trade} in ${city}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;

      while (true) {
        // Check max pages limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this.buildSearchUrl({ city, practiceCode: trade, page });
        log.info(`BuildZoom page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`BuildZoom request failed: ${err.message}`);
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`BuildZoom returned ${response.statusCode} — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`BuildZoom returned ${response.statusCode} for ${url}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on BuildZoom search — skipping ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total results on first page
        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            log.info(`No BuildZoom results for ${trade} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / PAGE_SIZE);
          log.success(`BuildZoom: ${totalResults.toLocaleString()} results (${totalPages} pages) for ${trade} in ${city}`);
        }

        // Extract contractor slugs from this page
        const slugEntries = this.parseResultsPage($);
        if (slugEntries.length === 0) {
          log.info(`No contractor links found on page ${page} — stopping`);
          break;
        }

        log.info(`Found ${slugEntries.length} contractors on page ${page}`);

        // Fetch each contractor's profile page for rich data
        for (const entry of slugEntries) {
          const slug = entry._slug;
          if (seenSlugs.has(slug)) continue;
          seenSlugs.add(slug);

          const profileUrl = `${BASE_URL}/contractor/${slug}`;

          try {
            await rateLimiter.wait();
            const profileRes = await this.httpGet(profileUrl, rateLimiter);

            if (profileRes.statusCode !== 200) {
              log.warn(`BuildZoom profile ${slug}: HTTP ${profileRes.statusCode}`);
              continue;
            }

            if (this.detectCaptcha(profileRes.body)) {
              log.warn(`CAPTCHA on BuildZoom profile ${slug}`);
              continue;
            }

            const $profile = cheerio.load(profileRes.body);
            const data = this.parseContractorProfile($profile);

            if (!data.firm_name) {
              log.warn(`No firm name extracted from ${slug}`);
              continue;
            }

            // Split owner name into first/last for lead format
            let firstName = '', lastName = '';
            if (data.owner_name) {
              const parts = this.splitName(data.owner_name);
              firstName = parts.firstName;
              lastName = parts.lastName;
            }

            const lead = this.transformResult({
              first_name: firstName,
              last_name: lastName,
              firm_name: data.firm_name,
              phone: data.phone || '',
              email: data.email || '',
              website: data.website || '',
              city: data.city || '',
              state: data.state || '',
              zip: data.zip || '',
              street: data.street || '',
              bar_number: data.bar_number || '',  // License number
              bar_status: data.bz_score ? `BZ Score: ${data.bz_score}` : '',
              profile_url: profileUrl,
              source: 'buildzoom_contractors',
              // Extra fields for enrichment
              _bz_score: data.bz_score || 0,
              _license_type: data.license_type || '',
              _license_count: data.license_count || 0,
              _services: data.services || '',
              _rating: data.rating || 0,
              _review_count: data.review_count || 0,
              _permit_count: data.permit_count || 0,
              _area_served: data.area_served || '',
              _insurance: data.insurance || '',
              _bond_amount: data.bond_amount || '',
            }, practiceArea);

            yield lead;
          } catch (err) {
            log.warn(`Failed to fetch BuildZoom profile ${slug}: ${err.message}`);
            continue;
          }
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / PAGE_SIZE);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${trade} in ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }

  /**
   * Profile page parser for waterfall enrichment.
   * If the profile_url is already a BuildZoom URL, parse it for additional data.
   */
  parseProfilePage($) {
    return this.parseContractorProfile($);
  }

  /**
   * Override transformResult to set source correctly.
   */
  transformResult(contractor, practiceArea) {
    contractor.source = contractor.source || 'buildzoom_contractors';
    contractor.practice_area = practiceArea || '';
    return contractor;
  }

  /**
   * Override CAPTCHA detection — BuildZoom uses Cloudflare but rarely triggers on search/profile.
   */
  detectCaptcha(body) {
    return body.includes('challenge-form') ||
           body.includes('Checking your browser') ||
           body.includes('cf-browser-verification');
  }
}

module.exports = new BuildZoomScraper();
