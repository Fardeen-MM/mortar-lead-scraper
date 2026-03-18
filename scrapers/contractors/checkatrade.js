/**
 * Checkatrade UK — UK's largest trade directory (60K+ vetted tradespeople)
 *
 * Source: https://www.checkatrade.com
 * Records: 60,000+ vetted tradespeople across the UK
 * Method: Puppeteer with stealth (Cloudflare WAF blocks all plain HTTP)
 *
 * URL patterns:
 *   Search: https://www.checkatrade.com/Search/{Trade}/in/{City}
 *   Profile: https://www.checkatrade.com/trades/{company-slug}
 *
 * Trade names in URLs are singular with title case: Roofer, Plumber, Electrician, Builder
 * City names are title case: London, Manchester, Birmingham
 *
 * Search results page structure:
 *   - Company names appear as headings (h2-h6)
 *   - Profile links: /trades/{company-slug}
 *   - Ratings: X.XX (N reviews)
 *   - Phone is behind "Show phone number" button (NOT in search results)
 *   - Location text: city name
 *
 * Profile page structure:
 *   - JSON-LD LocalBusiness with: name, telephone, address, url, knowsAbout
 *   - Website: external link with text "Website"
 *   - Phone: in JSON-LD telephone field
 *   - Rating: in JSON-LD or page headings
 *
 * Strategy: Scrape search results for company names + profile URLs, then visit
 * each profile page to extract phone + website from JSON-LD structured data.
 *
 * Note: Email is NOT shown on public profiles. Websites enable
 * email derivation via the enrichment pipeline.
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.checkatrade.com';

// Trade names for Checkatrade Search URLs (singular, title case)
const TRADE_URL_NAMES = {
  'roofers': 'Roofer',
  'roofing': 'Roofer',
  'plumbers': 'Plumber',
  'plumbing': 'Plumber',
  'painters': 'Painter-Decorator',
  'painting': 'Painter-Decorator',
  'decorators': 'Painter-Decorator',
  'electricians': 'Electrician',
  'electrical': 'Electrician',
  'builders': 'Builder',
  'general builder': 'Builder',
  'carpenters': 'Carpenter',
  'carpentry': 'Carpenter',
  'plasterers': 'Plasterer',
  'plastering': 'Plasterer',
  'tilers': 'Tiler',
  'tiling': 'Tiler',
  'landscapers': 'Landscaper',
  'landscaping': 'Landscaper',
  'kitchen fitters': 'Kitchen-Fitter',
  'bathroom fitters': 'Bathroom-Fitter',
  'heating engineers': 'Heating-Engineer',
  'heating': 'Heating-Engineer',
  'boiler': 'Gas-Boiler-Servicing-Repair',
  'locksmiths': 'Locksmith',
  'pest control': 'Pest-Control',
  'damp proofing': 'Damp-Proofer',
  'drainage': 'Drainage',
  'fencing': 'Fencing-Contractor',
  'flooring': 'Flooring',
  'gardeners': 'Gardener',
  'gardening': 'Gardener',
  'guttering': 'Gutter-Cleaning-And-Clearance',
  'handyman': 'Handyman',
  'windows': 'Window-Fitter',
  'extensions': 'Extension-Builder',
  'loft conversions': 'Loft-Conversion',
  'rendering': 'Renderer',
  'scaffolding': 'Scaffolding',
  'tree surgeons': 'Tree-Surgeon',
};

// Default UK cities
const DEFAULT_CITIES = [
  'London', 'Manchester', 'Birmingham', 'Leeds',
  'Bristol', 'Edinburgh', 'Glasgow', 'Liverpool',
];

// Default trades to search when no practiceArea specified
const DEFAULT_TRADES = [
  'roofers', 'plumbers', 'painters', 'electricians', 'builders',
];

class CheckatradeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'checkatrade_contractors',
      stateCode: 'CHECKATRADE',
      baseUrl: BASE_URL,
      pageSize: 10,
      practiceAreaCodes: Object.fromEntries(
        Object.entries(TRADE_URL_NAMES).map(([k, v]) => [k, v])
      ),
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('checkatrade: buildSearchUrl() not used'); }
  parseResultsPage() { throw new Error('checkatrade: parseResultsPage() not used'); }
  extractResultCount() { throw new Error('checkatrade: extractResultCount() not used'); }

  /**
   * Launch Puppeteer with stealth plugin for Cloudflare bypass.
   */
  async _launchBrowser() {
    try {
      const puppeteer = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteer.use(StealthPlugin());

      const launchOptions = {
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-blink-features=AutomationControlled',
          '--window-size=1920,1080',
        ],
      };
      if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
      }

      const browser = await puppeteer.launch(launchOptions);
      const page = await browser.newPage();

      await page.setViewport({ width: 1920, height: 1080 });
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );
      await page.setExtraHTTPHeaders({
        'Accept-Language': 'en-GB,en;q=0.9',
      });

      // Only block images and media (keep stylesheets and fonts for page render)
      await page.setRequestInterception(true);
      page.on('request', req => {
        const type = req.resourceType();
        if (['image', 'media'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      return { browser, page };
    } catch (err) {
      log.error(`[Checkatrade] Failed to launch Puppeteer: ${err.message}`);
      return null;
    }
  }

  /**
   * Navigate to a URL with Cloudflare challenge detection.
   */
  async _navigateWithCf(page, url) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (err) {
      log.warn(`[Checkatrade] Navigation timeout for ${url}: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 4000));
    let content = await page.content();

    if (this._isCfChallenge(content)) {
      log.info('[Checkatrade] Cloudflare challenge — waiting...');
      for (let i = 0; i < 10; i++) {
        await new Promise(r => setTimeout(r, 2000));
        content = await page.content();
        if (!this._isCfChallenge(content) && content.length > 3000) {
          log.info(`[Checkatrade] Cloudflare resolved after ${(i + 1) * 2}s`);
          return content;
        }
      }
      return null;
    }

    if (content.length > 3000) return content;
    return null;
  }

  _isCfChallenge(content) {
    return content.includes('Just a moment') ||
           content.includes('challenge-platform') ||
           content.includes('cf-browser-verification') ||
           content.includes('Attention Required') ||
           content.includes('challenge-running');
  }

  /**
   * Build the search URL.
   * Correct pattern: /Search/{Trade}/in/{City}
   */
  _buildSearchUrl(tradeUrlName, city) {
    return `${BASE_URL}/Search/${tradeUrlName}/in/${encodeURIComponent(city)}`;
  }

  /**
   * Extract listing names + profile URLs from the search results page.
   * On Checkatrade search pages, each company card has:
   *   - A link to /trades/{slug} with the company name
   *   - Rating text (e.g., "9.98")
   *   - Review count (e.g., "227 reviews")
   *   - City name
   */
  async _extractSearchListings(page, searchCity) {
    return page.evaluate((searchCity) => {
      const results = [];
      const seen = new Set();

      // Collect all links to /trades/{slug} (company profiles)
      const allLinks = document.querySelectorAll('a[href*="/trades/"]');

      // Known category slugs to skip (not company profiles)
      const categoryPatterns = [
        'roofer', 'plumber', 'electrician', 'builder', 'painter', 'decorator',
        'carpenter', 'plasterer', 'tiler', 'landscaper', 'locksmith', 'handyman',
        'gardener', 'fencing', 'flooring', 'drainage', 'kitchen', 'bathroom',
        'heating', 'boiler', 'window', 'extension', 'loft', 'scaffold',
        'renderer', 'tree-surgeon', 'pest', 'damp', 'gutter',
      ];

      for (const link of allLinks) {
        const href = link.getAttribute('href') || '';
        const cleanHref = href.split('#')[0].split('?')[0]; // Strip hash/query
        const slug = cleanHref.replace(/^\/trades\//, '').replace(/\/$/, '');

        if (!slug) continue;

        // Skip category/location pages
        const isCategory = categoryPatterns.some(pat => slug.toLowerCase().startsWith(pat));
        if (isCategory) continue;

        // Skip duplicates
        if (seen.has(slug)) continue;
        seen.add(slug);

        // Extract company name from the link text or closest heading
        let name = '';
        const linkText = link.textContent.trim();

        // The link text often includes rating + reviews, so look for a heading ancestor
        const card = link.closest('div') || link.parentElement;
        if (card) {
          const heading = card.querySelector('h2, h3, h4, h5, h6');
          if (heading) {
            name = heading.textContent.trim();
          }
        }

        // Fallback: use the first substantive text from the link
        if (!name || name.length < 2) {
          // Clean the link text — remove ratings/reviews/city text
          name = linkText
            .replace(/\d+\.\d+/g, '')  // Remove ratings like 9.98
            .replace(/\(\d+ reviews?\)/gi, '') // Remove "(227 reviews)"
            .replace(/\d+ reviews?/gi, '')
            .replace(new RegExp(searchCity, 'gi'), '')
            .replace(/Services & skills?/gi, '')
            .replace(/Request a quote/gi, '')
            .replace(/Show phone number/gi, '')
            .replace(/Sponsored/gi, '')
            .replace(/Be the first to review/gi, '')
            .replace(/[A-Z][a-z]+ (?:Installation|Repair|Service|Removal|Replacement|Cleaning)/g, '')
            .replace(/\+\s*\d+\s*skills?/gi, '')
            .replace(/\s+/g, ' ')
            .trim();

          // If name starts with a single letter (the company initial circle), strip it
          if (/^[A-Z]\s/.test(name)) {
            name = name.substring(2).trim();
          }
        }

        if (!name || name.length < 3) continue;
        // Skip if the name looks like boilerplate
        if (/^(Sponsored|Featured|Search|Services|Request)/i.test(name)) continue;

        // Extract rating from the card text
        let rating = '';
        let reviewCount = '';
        if (card) {
          const cardText = card.textContent || '';
          const ratingMatch = cardText.match(/(\d+\.\d{2})\s*(?:\((\d+)\s*reviews?\))?/);
          if (ratingMatch) {
            rating = ratingMatch[1];
            if (ratingMatch[2]) reviewCount = ratingMatch[2];
          }
          if (!reviewCount) {
            const revMatch = cardText.match(/(\d+)\s*reviews?/i);
            if (revMatch) reviewCount = revMatch[1];
          }
        }

        const profileUrl = cleanHref.startsWith('http') ? cleanHref : `https://www.checkatrade.com${cleanHref}`;

        results.push({
          name,
          slug,
          rating,
          reviewCount,
          profileUrl,
          city: searchCity,
        });
      }

      return results;
    }, searchCity);
  }

  /**
   * Extract phone + website from a profile page via JSON-LD.
   * Checkatrade profile pages embed JSON-LD LocalBusiness data with:
   *   telephone, address, url, name, knowsAbout, aggregateRating
   *
   * If the page frame becomes detached, creates a new page automatically.
   */
  async _scrapeProfilePage(page, profileUrl, browser) {
    let activePage = page;
    try {
      const content = await this._navigateWithCf(activePage, profileUrl);
      if (!content) return {};

      // Wait for SPA to render
      await new Promise(r => setTimeout(r, 3000));

      return await this._extractProfileData(activePage);
    } catch (err) {
      // If frame is detached, create a new page
      if (err.message && (err.message.includes('detached') || err.message.includes('Execution context'))) {
        if (browser) {
          try {
            activePage = await browser.newPage();
            await activePage.setViewport({ width: 1920, height: 1080 });
            await activePage.setUserAgent(
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            );
            await activePage.setRequestInterception(true);
            activePage.on('request', req => {
              if (['image', 'media'].includes(req.resourceType())) req.abort();
              else req.continue();
            });

            const content = await this._navigateWithCf(activePage, profileUrl);
            if (!content) { await activePage.close(); return {}; }
            await new Promise(r => setTimeout(r, 3000));
            const data = await this._extractProfileData(activePage);
            await activePage.close();
            return data;
          } catch (innerErr) {
            try { await activePage.close(); } catch (_) {}
            return {};
          }
        }
      }
      return {};
    }
  }

  /**
   * Extract profile data from the current page using JSON-LD and DOM inspection.
   */
  async _extractProfileData(activePage) {
    return activePage.evaluate(() => {
      const data = {};

      // Primary: JSON-LD structured data
      const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of ldScripts) {
        try {
          const json = JSON.parse(script.textContent);
          const biz = Array.isArray(json) ? json[0] : json;
          if (biz.telephone) data.phone = biz.telephone;
          if (biz.address) {
            if (biz.address.addressLocality) data.city = biz.address.addressLocality;
            if (biz.address.addressRegion) data.county = biz.address.addressRegion;
            if (biz.address.postalCode) data.zip = biz.address.postalCode;
          }
          if (biz.aggregateRating) {
            data.rating = biz.aggregateRating.ratingValue || '';
            data.reviewCount = biz.aggregateRating.reviewCount || '';
          }
        } catch (_) { /* ignore */ }
      }

      // Website: look for external link with text "Website"
      const allLinks = document.querySelectorAll('a[href^="http"]');
      for (const link of allLinks) {
        const href = link.getAttribute('href') || '';
        const text = (link.textContent || '').trim().toLowerCase();
        if (text === 'website' && href && !href.includes('checkatrade.com')) {
          data.website = href;
          break;
        }
      }

      // Fallback: any external link that's not social/known directories
      if (!data.website) {
        const excludedDomains = [
          'checkatrade.com', 'google.com', 'facebook.com', 'twitter.com', 'x.com',
          'instagram.com', 'youtube.com', 'linkedin.com', 'pinterest.com', 'yelp.com',
          'app.link', 'braze.eu', 'trustmark.org', 'fmb.org', 'niceic.com',
          'gasregistersafety', 'gassaferegister', 'napit.org', 'elecsa.co',
          'trustpilot.com', 'mybuilder.com', 'ratedpeople.com',
          'googlesyndication.com', 'googletagmanager.com', 'google-analytics.com',
          'doubleclick.net', 'cookielaw.org', 'onetrust.com', 'optimizely.com',
        ];
        for (const link of allLinks) {
          const href = link.getAttribute('href') || '';
          if (!href || href.length < 10) continue;
          const isExcluded = excludedDomains.some(d => href.includes(d));
          if (!isExcluded && (href.startsWith('http://') || href.startsWith('https://'))) {
            data.website = href;
            break;
          }
        }
      }

      return data;
    });
  }

  /**
   * Format a UK phone number.
   */
  _formatUkPhone(phone) {
    if (!phone) return '';
    let digits = phone.replace(/[^\d+]/g, '');
    if (digits.startsWith('+44')) digits = '0' + digits.slice(3);
    if (digits.startsWith('44') && digits.length > 10) digits = '0' + digits.slice(2);
    if (digits.length >= 10 && digits.length <= 12) return digits;
    return phone.trim();
  }

  /**
   * Main search generator.
   *
   * Two-phase approach:
   * Phase 1: Scrape search results pages for company names + profile URLs
   * Phase 2: Visit each profile page to extract phone + website from JSON-LD
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || 3;

    // Determine trades to search
    let trades;
    if (practiceArea) {
      const urlName = TRADE_URL_NAMES[practiceArea.toLowerCase()] || practiceArea;
      trades = [{ name: practiceArea, urlName }];
    } else {
      trades = DEFAULT_TRADES.map(t => ({ name: t, urlName: TRADE_URL_NAMES[t] || t }));
    }

    // Determine cities
    let cities = [...DEFAULT_CITIES];
    if (options.city) cities = [options.city];
    if (options.maxCities) cities = cities.slice(0, options.maxCities);

    const totalSearches = trades.length * cities.length;
    log.scrape(`[Checkatrade] Starting: ${trades.length} trades x ${cities.length} cities = ${totalSearches} searches`);

    // Launch browser
    const session = await this._launchBrowser();
    if (!session) {
      yield { _captcha: true, city: 'all', reason: 'Puppeteer not available' };
      return;
    }

    const { browser, page } = session;
    const seenSlugs = new Set();
    const allListings = []; // Collect listings for phase 2
    let searchNum = 0;

    try {
      // Warm up: load homepage
      log.info('[Checkatrade] Warming up session...');
      const homeContent = await this._navigateWithCf(page, BASE_URL);
      if (!homeContent) {
        log.warn('[Checkatrade] Cloudflare blocked homepage');
        yield { _captcha: true, city: 'all', reason: 'Cloudflare WAF blocked all access' };
        await browser.close();
        return;
      }
      log.success('[Checkatrade] Homepage loaded — Cloudflare bypassed');
      await new Promise(r => setTimeout(r, 2000));

      // Phase 1: Collect company names + profile URLs from search pages
      log.info('[Checkatrade] Phase 1: Collecting listings from search pages...');

      for (const trade of trades) {
        for (let ci = 0; ci < cities.length; ci++) {
          const city = cities[ci];
          searchNum++;
          yield { _cityProgress: { current: searchNum, total: totalSearches } };
          log.scrape(`[Checkatrade] Searching ${trade.name} in ${city} (${searchNum}/${totalSearches})`);

          const url = this._buildSearchUrl(trade.urlName, city);
          log.info(`[Checkatrade] ${url}`);

          await rateLimiter.wait();
          const content = await this._navigateWithCf(page, url);

          if (!content) {
            log.warn(`[Checkatrade] Cloudflare blocked: ${trade.name} in ${city}`);
            yield { _captcha: true, city, reason: 'Cloudflare blocked' };
            continue;
          }

          // Wait for SPA to render the results
          await new Promise(r => setTimeout(r, 4000));

          const listings = await this._extractSearchListings(page, city);

          if (listings.length === 0) {
            log.info(`[Checkatrade] No results for ${trade.name} in ${city}`);
            continue;
          }

          // Dedup and collect
          let added = 0;
          for (const listing of listings) {
            if (seenSlugs.has(listing.slug)) continue;
            seenSlugs.add(listing.slug);
            listing.trade = trade.name;
            allListings.push(listing);
            added++;
          }

          log.success(`[Checkatrade] Found ${listings.length} listings for ${trade.name} in ${city} (${added} new)`);

          // Polite delay between searches (3-6s)
          await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));
        }
      }

      log.success(`[Checkatrade] Phase 1 complete: ${allListings.length} unique listings collected`);

      if (allListings.length === 0) {
        log.warn('[Checkatrade] No listings found — search pages may have rendered differently');
        return;
      }

      // Phase 2: Visit profile pages to extract phone + website
      log.info(`[Checkatrade] Phase 2: Enriching ${allListings.length} listings from profile pages...`);

      let totalYielded = 0;
      let withPhone = 0;
      let withWebsite = 0;
      let consecutiveErrors = 0;
      let activePage = page;

      for (let i = 0; i < allListings.length; i++) {
        const listing = allListings[i];

        if (i > 0 && i % 10 === 0) {
          yield { _cityProgress: { current: i, total: allListings.length } };
          log.info(`[Checkatrade] Enriched ${i}/${allListings.length} profiles (${withPhone} phones, ${withWebsite} websites)`);
        }

        // If too many consecutive errors, create a fresh page
        if (consecutiveErrors >= 3) {
          log.warn('[Checkatrade] Multiple consecutive errors — creating fresh page...');
          try {
            try { await activePage.close(); } catch (_) {}
            activePage = await browser.newPage();
            await activePage.setViewport({ width: 1920, height: 1080 });
            await activePage.setUserAgent(
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            );
            await activePage.setRequestInterception(true);
            activePage.on('request', req => {
              if (['image', 'media'].includes(req.resourceType())) req.abort();
              else req.continue();
            });
            // Warm up the new page
            await this._navigateWithCf(activePage, BASE_URL);
            await new Promise(r => setTimeout(r, 2000));
            consecutiveErrors = 0;
            log.success('[Checkatrade] Fresh page created and warmed up');
          } catch (err) {
            log.error(`[Checkatrade] Failed to create fresh page: ${err.message}`);
            break;
          }
        }

        await rateLimiter.wait();
        const profileData = await this._scrapeProfilePage(activePage, listing.profileUrl, browser);

        const lead = {
          first_name: '',
          last_name: '',
          firm_name: listing.name || '',
          city: profileData.city || listing.city || '',
          state: 'UK',
          county: profileData.county || '',
          zip: profileData.zip || '',
          phone: this._formatUkPhone(profileData.phone || ''),
          email: '',
          website: profileData.website || '',
          bar_number: '',
          bar_status: (profileData.rating || listing.rating)
            ? `${profileData.rating || listing.rating}/10 (${profileData.reviewCount || listing.reviewCount || 0} reviews)`
            : 'Checkatrade Vetted',
          source: 'checkatrade_contractors',
          practice_area: listing.trade || '',
          profile_url: listing.profileUrl || '',
        };

        if (lead.phone) {
          withPhone++;
          consecutiveErrors = 0;
        } else {
          consecutiveErrors++;
        }
        if (lead.website) withWebsite++;

        yield this.transformResult(lead, listing.trade);
        totalYielded++;

        // Polite delay between profile pages (2-4s)
        await new Promise(r => setTimeout(r, 2000 + Math.random() * 2000));
      }

      log.success(`[Checkatrade] Scrape complete — ${totalYielded} leads (${withPhone} phones, ${withWebsite} websites)`);

    } finally {
      if (browser) await browser.close();
    }
  }

  transformResult(lead, practiceArea) {
    lead.source = 'checkatrade_contractors';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }
}

module.exports = new CheckatradeScraper();
