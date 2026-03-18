/**
 * Angi (formerly Angie's List / HomeAdvisor) — Contractor Directory Scraper
 *
 * Source: https://www.angi.com
 * Records: 200K+ paying service providers with verified reviews
 * Method: HTTP scraping of companylist category pages + profile page enrichment
 *
 * URL patterns:
 *   Category listing: https://www.angi.com/companylist/us/{state}/{city}/{trade}.htm
 *   Company profile:  https://www.angi.com/companylist/us/{state}/{city}/{slug}-reviews-{ID}.htm
 *   Near-me pages:    https://www.angi.com/nearme/{trade}/
 *
 * Data available from listing pages:
 *   - Business name, phone, address (city, state), rating, review count, profile URL
 *
 * Data available from profile pages (enrichment):
 *   - Email, phone, website, full address, hours, services, licensing, description
 *   - JSON-LD LocalBusiness/HomeAndConstructionBusiness schema
 *   - __NEXT_DATA__ embedded JSON with full company data
 *
 * Anti-bot: Angi uses aggressive bot protection (Akamai/Cloudflare).
 *   Direct HTTP requests currently return 403 on companylist pages.
 *   The scraper attempts HTTP with realistic headers and falls back to
 *   signaling CAPTCHA/block so the UI shows the correct status.
 *   Full parsing logic is implemented — will work when access is available
 *   (proxy rotation, Puppeteer stealth, etc.).
 *
 * Trade categories (confirmed URL slugs):
 *   roofing, painting, plumbing, heating-cooling, electrical,
 *   general-contractor, landscaping, fencing, flooring, siding,
 *   concrete, kitchen-remodel, bathroom-remodel, windows, decks-porches
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.angi.com';
const RESULTS_PER_PAGE = 20; // Angi typically shows ~20 listings per page

/**
 * Trade category mapping: practice area -> Angi URL slug
 */
const TRADE_SLUGS = {
  'roofing': 'roofing',
  'painting': 'painting',
  'plumbing': 'plumbing',
  'hvac': 'heating-cooling',
  'heating': 'heating-cooling',
  'cooling': 'heating-cooling',
  'air conditioning': 'heating-cooling',
  'electrical': 'electrical',
  'electrician': 'electrical',
  'general contractor': 'general-contractor',
  'general': 'general-contractor',
  'landscaping': 'landscaping',
  'landscape': 'landscaping',
  'fencing': 'fencing',
  'flooring': 'flooring',
  'siding': 'siding',
  'concrete': 'concrete',
  'kitchen remodel': 'kitchen-remodel',
  'bathroom remodel': 'bathroom-remodel',
  'windows': 'windows',
  'decks': 'decks-porches',
  'porches': 'decks-porches',
  'gutters': 'gutters',
  'garage doors': 'garage-doors',
  'insulation': 'insulation',
  'drywall': 'drywall',
  'masonry': 'masonry',
  'tree service': 'tree-service',
  'pest control': 'pest-control',
  'cleaning': 'house-cleaning',
  'moving': 'moving',
  'handyman': 'handyman',
};

/**
 * State code -> lowercase abbreviation for Angi URLs.
 */
const STATE_SLUGS = {
  'AL': 'alabama', 'AK': 'alaska', 'AZ': 'arizona', 'AR': 'arkansas',
  'CA': 'california', 'CO': 'colorado', 'CT': 'connecticut', 'DE': 'delaware',
  'DC': 'district-of-columbia', 'FL': 'florida', 'GA': 'georgia', 'HI': 'hawaii',
  'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa',
  'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana', 'ME': 'maine',
  'MD': 'maryland', 'MA': 'massachusetts', 'MI': 'michigan', 'MN': 'minnesota',
  'MS': 'mississippi', 'MO': 'missouri', 'MT': 'montana', 'NE': 'nebraska',
  'NV': 'nevada', 'NH': 'new-hampshire', 'NJ': 'new-jersey', 'NM': 'new-mexico',
  'NY': 'new-york', 'NC': 'north-carolina', 'ND': 'north-dakota', 'OH': 'ohio',
  'OK': 'oklahoma', 'OR': 'oregon', 'PA': 'pennsylvania', 'RI': 'rhode-island',
  'SC': 'south-carolina', 'SD': 'south-dakota', 'TN': 'tennessee', 'TX': 'texas',
  'UT': 'utah', 'VT': 'vermont', 'VA': 'virginia', 'WA': 'washington',
  'WV': 'west-virginia', 'WI': 'wisconsin', 'WY': 'wyoming',
};

/**
 * Default cities to search — major US metros with high Angi listing density.
 * Format: "City, ST" for parsing by _parseCityInput().
 */
const DEFAULT_CITIES = [
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
];


class AngiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'angi_contractors',
      stateCode: 'ANGI',
      baseUrl: BASE_URL,
      pageSize: RESULTS_PER_PAGE,
      practiceAreaCodes: {
        'roofing': 'Roofing',
        'painting': 'Painting',
        'plumbing': 'Plumbing',
        'hvac': 'HVAC / Heating & Cooling',
        'electrical': 'Electrical',
        'general contractor': 'General Contractor',
        'landscaping': 'Landscaping',
        'fencing': 'Fencing',
        'flooring': 'Flooring',
        'siding': 'Siding',
        'concrete': 'Concrete',
        'kitchen remodel': 'Kitchen Remodel',
        'bathroom remodel': 'Bathroom Remodel',
        'windows': 'Windows',
        'decks': 'Decks & Porches',
        'gutters': 'Gutters',
        'garage doors': 'Garage Doors',
        'insulation': 'Insulation',
        'drywall': 'Drywall',
        'masonry': 'Masonry',
        'tree service': 'Tree Service',
        'pest control': 'Pest Control',
        'cleaning': 'House Cleaning',
        'moving': 'Moving',
        'handyman': 'Handyman',
      },
      defaultCities: DEFAULT_CITIES,
    });
  }

  /**
   * Override CAPTCHA detection for Angi's specific bot protection patterns.
   * Angi uses Akamai Bot Manager and occasionally Cloudflare.
   */
  detectCaptcha(body) {
    return body.includes('challenge-form') ||
           body.includes('Just a moment') ||
           body.includes('cf-browser-verification') ||
           body.includes('Access Denied') ||
           body.includes('_Incapsula_Resource') ||
           body.includes('ak_bmsc') ||
           body.includes('g-recaptcha" data-sitekey');
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() not used — search() is overridden`);
  }

  /**
   * Override transformResult to use 'angi_contractors' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'angi_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Override httpGet to add Angi-specific headers.
   * Angi requires realistic browser headers to avoid immediate 403.
   */
  httpGet(url, rateLimiter, redirectCount = 0) {
    const https = require('https');
    const http = require('http');

    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Cache-Control': 'max-age=0',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'none',
          'Sec-Fetch-User': '?1',
          'Upgrade-Insecure-Requests': '1',
          'Referer': 'https://www.google.com/',
        },
        timeout: 20000,
      };

      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter, redirectCount + 1));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Slugify a city name for the Angi URL.
   * "Fort Lauderdale" -> "fort-lauderdale"
   * "St. Petersburg" -> "st-petersburg"
   */
  _slugifyCity(city) {
    return city
      .toLowerCase()
      .replace(/\./g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '');
  }

  /**
   * Parse "City, ST" into components.
   */
  _parseCityInput(cityInput) {
    const parts = cityInput.split(',').map(s => s.trim());
    if (parts.length >= 2) {
      return { city: parts[0], state: parts[1].toUpperCase() };
    }
    return { city: parts[0], state: '' };
  }

  /**
   * Build the Angi companylist URL for a category + city + state.
   *
   * Format: https://www.angi.com/companylist/us/{state-slug}/{city-slug}/{trade-slug}.htm
   *
   * Angi uses full state name slugs (florida, texas) not abbreviations.
   */
  _buildListingUrl(stateCode, citySlug, tradeSlug, page) {
    const stateSlug = STATE_SLUGS[stateCode] || stateCode.toLowerCase();
    let url = `${BASE_URL}/companylist/us/${stateSlug}/${citySlug}/${tradeSlug}.htm`;
    if (page > 1) {
      url += `?page=${page}`;
    }
    return url;
  }

  /**
   * Extract business listings from __NEXT_DATA__ JSON embedded in page.
   * Angi is a Next.js app — the initial page data is in a script tag.
   *
   * @param {string} html - Raw HTML of the listing page
   * @returns {{ results: object[], totalCount: number }}
   */
  _parseFromNextData(html) {
    const results = [];
    let totalCount = 0;

    // Extract __NEXT_DATA__ JSON
    const nextDataMatch = html.match(/<script\s+id="__NEXT_DATA__"\s+type="application\/json">([\s\S]*?)<\/script>/);
    if (!nextDataMatch) return { results, totalCount };

    let nextData;
    try {
      nextData = JSON.parse(nextDataMatch[1]);
    } catch (e) {
      log.warn('Angi: Failed to parse __NEXT_DATA__ JSON');
      return { results, totalCount };
    }

    // Navigate the Next.js page props tree
    const pageProps = nextData.props?.pageProps || {};
    const initialData = pageProps.initialData || pageProps.data || pageProps;

    // Look for business listings in common property names
    const listings = initialData.listings ||
                     initialData.companies ||
                     initialData.providers ||
                     initialData.searchResults?.results ||
                     initialData.companyList ||
                     [];

    totalCount = initialData.totalCount ||
                 initialData.total ||
                 initialData.searchResults?.totalCount ||
                 listings.length;

    for (const item of listings) {
      try {
        const lead = this._extractLeadFromNextDataItem(item);
        if (lead && lead.firm_name) {
          results.push(lead);
        }
      } catch (e) {
        // Skip malformed entries
      }
    }

    return { results, totalCount };
  }

  /**
   * Extract a lead from a __NEXT_DATA__ listing item.
   * Angi's data structures may vary, so we check multiple property names.
   */
  _extractLeadFromNextDataItem(item) {
    if (!item) return null;

    const name = item.companyName || item.name || item.businessName || item.firm_name || '';
    if (!name || name.length < 2) return null;

    // Address fields
    const address = item.address || item.location || {};
    const city = address.city || address.addressLocality || item.city || '';
    const state = address.state || address.addressRegion || item.state || '';
    const zip = address.zip || address.postalCode || item.zip || '';
    const street = address.street || address.streetAddress || item.street || '';

    // Contact info
    const phone = item.phone || item.telephone || item.phoneNumber || '';
    const email = item.email || item.contactEmail || '';
    const website = item.website || item.websiteUrl || item.url || '';

    // Rating & reviews
    const rating = item.rating || item.overallRating || item.averageRating || '';
    const reviewCount = item.reviewCount || item.numReviews || item.totalReviews || 0;

    // Profile URL
    let profileUrl = item.profileUrl || item.companyUrl || item.detailUrl || item.href || '';
    if (profileUrl && !profileUrl.startsWith('http')) {
      profileUrl = `${BASE_URL}${profileUrl}`;
    }

    // Company ID (for dedup)
    const companyId = item.companyId || item.id || item.serviceProviderId || '';

    return {
      first_name: '',
      last_name: '',
      firm_name: name.trim(),
      phone: this._cleanPhone(phone),
      email: (email || '').toLowerCase(),
      website: this._cleanWebsite(website),
      city,
      state,
      zip,
      street,
      bar_number: companyId ? String(companyId) : '',
      bar_status: rating ? `${rating}/5 (${reviewCount} reviews)` : '',
      profile_url: profileUrl,
      source: 'angi_contractors',
    };
  }

  /**
   * Parse business listings from HTML using Cheerio selectors.
   * Fallback when __NEXT_DATA__ is not available or empty.
   *
   * @param {string} html - Raw HTML of the listing page
   * @returns {{ results: object[], totalCount: number }}
   */
  _parseFromHtml(html) {
    const $ = cheerio.load(html);
    const results = [];
    let totalCount = 0;

    // Extract total count from page text
    const bodyText = $('body').text();
    const countMatch = bodyText.match(/([\d,]+)\s+(?:results?|professionals?|contractors?|pros?)\s+(?:found|near|in|for)/i);
    if (countMatch) {
      totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);
    }

    // --- Strategy 1: JSON-LD structured data ---
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        const data = JSON.parse(raw);

        // Could be an array of businesses or a single ItemList
        const items = data['@type'] === 'ItemList'
          ? (data.itemListElement || [])
          : (Array.isArray(data) ? data : [data]);

        for (const item of items) {
          const biz = item.item || item;
          if (!biz.name) continue;
          if (biz['@type'] && /Organization|LocalBusiness|HomeAndConstructionBusiness|ProfessionalService/i.test(biz['@type'])) {
            const addr = biz.address || {};
            results.push({
              first_name: '',
              last_name: '',
              firm_name: biz.name,
              phone: this._cleanPhone(biz.telephone),
              email: (biz.email || '').toLowerCase(),
              website: this._cleanWebsite(biz.url),
              city: addr.addressLocality || '',
              state: addr.addressRegion || '',
              zip: addr.postalCode || '',
              street: addr.streetAddress || '',
              bar_number: '',
              bar_status: biz.aggregateRating
                ? `${biz.aggregateRating.ratingValue}/5 (${biz.aggregateRating.reviewCount} reviews)`
                : '',
              profile_url: biz.url && biz.url.includes('angi.com') ? biz.url : '',
              source: 'angi_contractors',
            });
          }
        }
      } catch (e) {
        // Skip malformed JSON-LD
      }
    });

    if (results.length > 0) return { results, totalCount: totalCount || results.length };

    // --- Strategy 2: HTML card selectors ---
    // Angi listing pages typically have card-like containers for each business.
    // We try multiple selector patterns to handle markup variations.
    const cardSelectors = [
      '[data-testid*="company"], [data-testid*="provider"], [data-testid*="search-result"]',
      '[class*="CompanyCard"], [class*="companyCard"], [class*="ProviderCard"]',
      '[class*="search-result"], [class*="SearchResult"]',
      'article[class*="company"], div[class*="listing-card"]',
    ];

    let $cards = $();
    for (const selector of cardSelectors) {
      $cards = $(selector);
      if ($cards.length > 0) break;
    }

    $cards.each((_, card) => {
      const $card = $(card);
      const lead = this._extractLeadFromHtmlCard($, $card);
      if (lead && lead.firm_name) {
        results.push(lead);
      }
    });

    if (results.length > 0) return { results, totalCount: totalCount || results.length };

    // --- Strategy 3: Profile link extraction (like BuildZoom) ---
    // Find links to company review pages and extract basic info from context.
    const profileLinks = [];
    const seenHrefs = new Set();

    $('a[href*="-reviews-"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (!href.match(/-reviews-\d+\.htm/)) return;

      const fullHref = href.startsWith('http') ? href : `${BASE_URL}${href}`;
      const normalized = fullHref.replace(/\/$/, '');
      if (seenHrefs.has(normalized)) return;
      seenHrefs.add(normalized);

      const name = $(el).text().trim();
      if (name && name.length >= 3 && name.length <= 200) {
        profileLinks.push({ el, href: fullHref, name });
      }
    });

    for (const link of profileLinks) {
      const $el = $(link.el);
      // Walk up to find containing block with address/phone
      let $container = $el.parent();
      for (let i = 0; i < 8; i++) {
        const text = $container.text();
        if (/\(\d{3}\)\s*\d{3}-\d{4}/.test(text) || /\d{5}/.test(text)) break;
        const $parent = $container.parent();
        if ($parent.length === 0) break;
        $container = $parent;
      }

      const containerText = $container.text();

      // Phone
      let phone = '';
      const phoneMatch = containerText.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
      if (phoneMatch) phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;

      // City, State ZIP
      let city = '', state = '', zip = '';
      const addrMatch = containerText.match(/([A-Za-z\s.]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
      if (addrMatch) {
        city = addrMatch[1].trim();
        state = addrMatch[2];
        zip = addrMatch[3];
      }

      // Rating
      let ratingStr = '';
      const ratingMatch = containerText.match(/([\d.]+)\s*(?:\/\s*5|stars?)/i);
      if (ratingMatch) {
        const reviewMatch = containerText.match(/(\d+)\s*reviews?/i);
        ratingStr = reviewMatch
          ? `${ratingMatch[1]}/5 (${reviewMatch[1]} reviews)`
          : `${ratingMatch[1]}/5`;
      }

      results.push({
        first_name: '',
        last_name: '',
        firm_name: link.name,
        phone,
        email: '',
        website: '',
        city,
        state,
        zip,
        street: '',
        bar_number: '',
        bar_status: ratingStr,
        profile_url: link.href,
        source: 'angi_contractors',
      });
    }

    return { results, totalCount: totalCount || results.length };
  }

  /**
   * Extract a lead from an HTML card element.
   */
  _extractLeadFromHtmlCard($, $card) {
    const text = $card.text();

    // Business name — first heading or prominent link
    let name = '';
    const $nameEl = $card.find('h2 a, h3 a, [class*="name"] a, [class*="Name"] a, a[class*="title"]').first();
    if ($nameEl.length) {
      name = $nameEl.text().trim();
    } else {
      const $heading = $card.find('h2, h3').first();
      if ($heading.length) name = $heading.text().trim();
    }

    if (!name || name.length < 3) return null;
    if (/^(get a quote|book|learn more|see more|compare|find)/i.test(name)) return null;

    // Profile URL
    let profileUrl = '';
    const $profileLink = $card.find('a[href*="-reviews-"], a[href*="/companylist/"]').first();
    if ($profileLink.length) {
      profileUrl = $profileLink.attr('href') || '';
      if (profileUrl && !profileUrl.startsWith('http')) {
        profileUrl = `${BASE_URL}${profileUrl}`;
      }
    }

    // Phone
    let phone = '';
    const $phoneEl = $card.find('a[href^="tel:"]').first();
    if ($phoneEl.length) {
      phone = ($phoneEl.attr('href') || '').replace('tel:', '').trim();
    }
    if (!phone) {
      const phoneMatch = text.match(/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/);
      if (phoneMatch) phone = phoneMatch[0];
    }

    // Website
    let website = '';
    const $websiteEl = $card.find('a[class*="website"], a[rel*="nofollow"]').first();
    if ($websiteEl.length) {
      const href = $websiteEl.attr('href') || '';
      if (href && !href.includes('angi.com')) website = href;
    }

    // Location
    let city = '', state = '', zip = '';
    const addrMatch = text.match(/([A-Za-z\s.]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
    if (addrMatch) {
      city = addrMatch[1].trim();
      state = addrMatch[2];
      zip = addrMatch[3];
    }

    // Rating
    let ratingStr = '';
    const ratingMatch = text.match(/([\d.]+)\s*(?:\/\s*5|stars?)/i);
    if (ratingMatch) {
      const reviewMatch = text.match(/(\d+)\s*reviews?/i);
      ratingStr = reviewMatch
        ? `${ratingMatch[1]}/5 (${reviewMatch[1]} reviews)`
        : `${ratingMatch[1]}/5`;
    }

    return {
      first_name: '',
      last_name: '',
      firm_name: name.trim(),
      phone: this._cleanPhone(phone),
      email: '',
      website: this._cleanWebsite(website),
      city,
      state,
      zip,
      street: '',
      bar_number: '',
      bar_status: ratingStr,
      profile_url: profileUrl,
      source: 'angi_contractors',
    };
  }

  /**
   * Parse an Angi company profile page for additional details.
   * Extracts JSON-LD, __NEXT_DATA__, and HTML-based fields.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields for enrichment
   */
  parseProfilePage($) {
    const text = $('body').text();
    const extra = {};

    // --- 1. JSON-LD structured data ---
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        const data = JSON.parse(raw);
        const biz = Array.isArray(data) ? data[0] : data;

        if (biz.telephone && !extra.phone) {
          extra.phone = this._cleanPhone(biz.telephone);
        }
        if (biz.email && !extra.email) {
          const email = biz.email.toLowerCase();
          if (!email.includes('angi.com') && !email.includes('example.com')) {
            extra.email = email;
          }
        }
        if (biz.url && !biz.url.includes('angi.com') && !extra.website) {
          extra.website = biz.url;
        }
        if (biz.address) {
          if (biz.address.addressLocality) extra.city = biz.address.addressLocality;
          if (biz.address.addressRegion) extra.state = biz.address.addressRegion;
          if (biz.address.postalCode) extra.zip = biz.address.postalCode;
          if (biz.address.streetAddress) extra.street = biz.address.streetAddress;
        }
      } catch (e) {
        // Skip malformed JSON-LD
      }
    });

    // --- 2. __NEXT_DATA__ on profile page ---
    const nextDataMatch = $.html().match(/<script\s+id="__NEXT_DATA__"\s+type="application\/json">([\s\S]*?)<\/script>/);
    if (nextDataMatch) {
      try {
        const nextData = JSON.parse(nextDataMatch[1]);
        const pageProps = nextData.props?.pageProps || {};
        const company = pageProps.company || pageProps.provider || pageProps.companyData || pageProps;

        if (company.phone && !extra.phone) extra.phone = this._cleanPhone(company.phone);
        if (company.email && !extra.email) {
          const email = company.email.toLowerCase();
          if (!email.includes('angi.com')) extra.email = email;
        }
        if (company.website && !extra.website) {
          if (!company.website.includes('angi.com')) extra.website = company.website;
        }
        if (company.ownerName || company.contactName) {
          const ownerName = company.ownerName || company.contactName;
          const parts = this.splitName(ownerName);
          if (parts.firstName) extra.first_name = parts.firstName;
          if (parts.lastName) extra.last_name = parts.lastName;
        }
      } catch (e) {
        // Ignore parse errors
      }
    }

    // --- 3. Email from HTML ---
    if (!extra.email) {
      // Cloudflare email protection
      $('a[href*="email-protection"]').each((_, el) => {
        if (extra.email) return;
        const href = ($(el).attr('href') || '');
        const hexMatch = href.match(/email-protection#([a-f0-9]+)/i);
        if (hexMatch) {
          const decoded = this.decodeCloudflareEmail(hexMatch[1]);
          if (decoded && decoded.includes('@') && !decoded.includes('angi.com')) {
            extra.email = decoded.toLowerCase();
          }
        }
      });
    }
    if (!extra.email) {
      $('span[data-cfemail], a[data-cfemail]').each((_, el) => {
        if (extra.email) return;
        const encoded = $(el).attr('data-cfemail');
        if (encoded) {
          const decoded = this.decodeCloudflareEmail(encoded);
          if (decoded && decoded.includes('@') && !decoded.includes('angi.com')) {
            extra.email = decoded.toLowerCase();
          }
        }
      });
    }
    if (!extra.email) {
      $('a[href^="mailto:"]').each((_, el) => {
        if (extra.email) return;
        const href = ($(el).attr('href') || '').replace('mailto:', '').trim().toLowerCase();
        if (href && !href.includes('angi.com') && href.includes('@')) {
          extra.email = href.split('?')[0];
        }
      });
    }
    if (!extra.email) {
      const emailMatch = text.match(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/i);
      if (emailMatch) {
        const email = emailMatch[0].toLowerCase();
        if (!email.includes('angi.com') && !email.includes('example.com')) {
          extra.email = email;
        }
      }
    }

    // --- 4. Website from HTML ---
    if (!extra.website) {
      $('a[href]').each((_, el) => {
        if (extra.website) return;
        const href = ($(el).attr('href') || '').trim();
        const linkText = $(el).text().trim().toLowerCase();
        if ((linkText.includes('visit website') || linkText.includes('company website') || linkText === 'website') &&
            href.startsWith('http') && !href.includes('angi.com') && !this.isExcludedDomain(href)) {
          extra.website = href;
        }
      });
    }

    // --- 5. Phone from tel: links ---
    if (!extra.phone) {
      const $tel = $('a[href^="tel:"]').first();
      if ($tel.length) {
        extra.phone = this._cleanPhone($tel.attr('href').replace('tel:', ''));
      }
    }
    if (!extra.phone) {
      const phoneMatch = text.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
      if (phoneMatch) {
        extra.phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;
      }
    }

    // --- 6. Owner/contact name ---
    if (!extra.first_name) {
      const ownerMatch = text.match(/(?:Owner|Contact|Principal)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/);
      if (ownerMatch) {
        const parts = this.splitName(ownerMatch[1].trim());
        extra.first_name = parts.firstName;
        extra.last_name = parts.lastName;
      }
    }

    return extra;
  }

  /**
   * Profile parser is implemented — enables waterfall enrichment.
   */
  get hasProfileParser() {
    return true;
  }

  /**
   * Clean a phone number string.
   */
  _cleanPhone(phone) {
    if (!phone) return '';
    return phone.replace(/\s*(?:ext\.?|x)\s*\d+/i, '').trim();
  }

  /**
   * Clean a website URL — exclude Angi and social domains.
   */
  _cleanWebsite(url) {
    if (!url) return '';
    if (url.includes('angi.com') || url.includes('homeadvisor.com')) return '';
    if (this.isExcludedDomain(url)) return '';
    return url;
  }

  /**
   * Main search generator — scrapes Angi category listing pages.
   *
   * Strategy:
   *   1. Build companylist URL for trade + city + state
   *   2. Attempt HTTP fetch with realistic browser headers
   *   3. Parse __NEXT_DATA__ JSON or fall back to HTML parsing
   *   4. Paginate via ?page=N query parameter
   *   5. Signal CAPTCHA/block if Angi's anti-bot protection triggers
   *
   * Profile pages (profile_url) enable enrichment via waterfall pipeline
   * for email, phone, website, and owner name extraction.
   *
   * @param {string} practiceArea - Trade category (e.g., 'roofing', 'plumbing')
   * @param {object} options - Search options (maxPages, maxCities, city)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Resolve trade slug
    const practiceKey = (practiceArea || 'roofing').toLowerCase().trim();
    const tradeSlug = TRADE_SLUGS[practiceKey];

    if (!tradeSlug && practiceArea) {
      log.warn(`Angi: Unknown trade "${practiceArea}" — available: ${Object.keys(TRADE_SLUGS).join(', ')}`);
      log.info('Defaulting to "roofing"');
    }

    const slug = tradeSlug || 'roofing';

    // Determine cities to search
    let citiesToSearch;
    if (options.city) {
      citiesToSearch = [options.city];
    } else {
      citiesToSearch = [...this.defaultCities];
    }
    if (options.maxCities) {
      citiesToSearch = citiesToSearch.slice(0, options.maxCities);
    }

    log.scrape(`Angi search: trade=${slug}, cities=${citiesToSearch.length}`);

    // Track seen profile URLs to avoid duplicates across cities
    const seenUrls = new Set();
    let totalYielded = 0;
    let blockedCount = 0;

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const cityInput = citiesToSearch[ci];
      const { city, state } = this._parseCityInput(cityInput);

      if (!state) {
        log.warn(`Angi: Skipping "${cityInput}" — no state code (use "City, ST" format)`);
        continue;
      }

      if (!STATE_SLUGS[state]) {
        log.warn(`Angi: Unknown state "${state}" — skipping ${cityInput}`);
        continue;
      }

      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };

      const citySlug = this._slugifyCity(city);
      log.scrape(`Searching Angi: ${practiceArea || 'roofing'} in ${city}, ${state}`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;
      let consecutiveEmpty = 0;
      let cityYielded = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${city}`);
          break;
        }

        const url = this._buildListingUrl(state, citySlug, slug, page);
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Angi request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle bot protection (403 is the primary block response)
        if (response.statusCode === 403) {
          blockedCount++;
          log.warn(`Angi returned 403 for ${city}, ${state} — bot protection active`);

          if (blockedCount >= 3) {
            // After 3 blocked cities, signal CAPTCHA and stop
            log.warn('Angi: Blocked on 3+ cities — anti-bot protection prevents HTTP scraping');
            yield {
              _captcha: true,
              city: 'all',
              reason: 'Angi anti-bot protection (Akamai) blocks HTTP requests. Requires proxy rotation or Puppeteer stealth.',
            };
            return;
          }

          // Try backing off for this city
          const shouldRetry = await rateLimiter.handleBlock(403);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429) {
          log.warn(`Angi returned 429 (rate limited) for ${city}`);
          const shouldRetry = await rateLimiter.handleBlock(429);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 404) {
          log.warn(`Angi returned 404 for ${city}, ${state} — URL pattern may be wrong`);
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Angi returned ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();
        blockedCount = 0; // Reset block counter on success

        // Check for CAPTCHA/challenge page
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on Angi page for ${city} — skipping`);
          yield { _captcha: true, city: `${city}, ${state}`, page };
          break;
        }

        // --- Parse listings ---
        // Try __NEXT_DATA__ first (structured JSON), then fall back to HTML
        let parsed = this._parseFromNextData(response.body);

        if (parsed.results.length === 0) {
          parsed = this._parseFromHtml(response.body);
        }

        const { results, totalCount } = parsed;

        if (page === 1 && totalCount > 0) {
          totalResults = totalCount;
          const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
          log.success(`Angi: ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}, ${state}`);
        }

        if (results.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            if (cityYielded > 0) {
              log.info(`2 consecutive empty pages — done with ${city}`);
            } else if (page === 1) {
              log.info(`No results for ${practiceArea || 'roofing'} in ${city}, ${state}`);
            }
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Yield results with dedup
        for (const result of results) {
          if (result.profile_url && seenUrls.has(result.profile_url)) continue;
          if (result.profile_url) seenUrls.add(result.profile_url);

          yield this.transformResult(result, practiceArea);
          cityYielded++;
          totalYielded++;
        }

        log.info(`Page ${page}: ${results.length} listings (${cityYielded} total from ${city})`);

        // Check if we've likely exhausted results
        if (results.length < RESULTS_PER_PAGE - 3) {
          log.success(`Completed ${city}, ${state}: ${cityYielded} leads`);
          break;
        }

        // Safety cap on pagination
        const maxPossiblePages = totalResults > 0
          ? Math.ceil(totalResults / RESULTS_PER_PAGE)
          : 100;

        if (page >= maxPossiblePages) {
          log.success(`Completed all ${page} pages for ${city}, ${state}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }

    if (totalYielded > 0) {
      log.success(`Angi scrape complete — ${totalYielded} total leads from ${seenUrls.size} unique businesses`);
    } else {
      log.warn('Angi: No leads scraped — anti-bot protection likely blocked all requests');
      yield {
        _captcha: true,
        city: 'all',
        reason: 'Angi anti-bot protection (Akamai) blocks HTTP requests. Requires proxy rotation or Puppeteer stealth.',
      };
    }
  }
}

module.exports = new AngiScraper();
