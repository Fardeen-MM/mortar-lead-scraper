/**
 * Checkatrade UK — UK's largest trade directory (60K+ vetted tradespeople)
 *
 * Source: https://www.checkatrade.com
 * Records: 60,000+ vetted tradespeople across the UK
 * Method: Puppeteer with stealth (Cloudflare WAF blocks all plain HTTP)
 *
 * URL patterns:
 *   Search: https://www.checkatrade.com/search/?searchTerm={trade}&location={city}&page={n}
 *   Profile: https://www.checkatrade.com/trades/{company-slug}
 *
 * Available fields:
 *   Company name, phone, website, location (city), rating, review count,
 *   trade/category, profile URL, Checkatrade member since
 *
 * Checkatrade is fully behind Cloudflare CDN with browser challenge pages.
 * Plain HTTP requests (Node https, fetch, axios) always return 403.
 * Puppeteer with stealth plugin bypasses the JS challenge.
 *
 * Note: Email is NOT shown on public profiles. Websites enable
 * email derivation via the enrichment pipeline.
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.checkatrade.com';

// Trade slugs for Checkatrade URL paths
const TRADE_SLUGS = {
  'roofers': 'roofers',
  'roofing': 'roofers',
  'plumbers': 'plumbers',
  'plumbing': 'plumbers',
  'painters': 'painters-decorators',
  'painting': 'painters-decorators',
  'decorators': 'painters-decorators',
  'electricians': 'electricians',
  'electrical': 'electricians',
  'builders': 'builders',
  'general builder': 'builders',
  'carpenters': 'carpenters',
  'carpentry': 'carpenters',
  'plasterers': 'plasterers',
  'plastering': 'plasterers',
  'tilers': 'tilers',
  'tiling': 'tilers',
  'landscapers': 'landscapers',
  'landscaping': 'landscapers',
  'kitchen fitters': 'kitchen-fitters',
  'bathroom fitters': 'bathroom-fitters',
  'heating engineers': 'heating-engineers',
  'heating': 'heating-engineers',
  'boiler': 'boiler-installers',
  'locksmiths': 'locksmiths',
  'pest control': 'pest-control',
  'damp proofing': 'damp-proofing',
  'drainage': 'drainage',
  'fencing': 'fencing',
  'flooring': 'flooring',
  'gardeners': 'gardeners',
  'gardening': 'gardeners',
  'guttering': 'guttering-fascias',
  'handyman': 'handyman-services',
  'windows': 'windows-doors',
  'conservatories': 'conservatories',
  'extensions': 'extensions',
  'loft conversions': 'loft-conversions',
  'rendering': 'rendering',
  'scaffolding': 'scaffolding',
  'security': 'security-systems',
  'solar': 'solar-panels',
  'tree surgeons': 'tree-surgeons',
};

// Default UK cities to search
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
      pageSize: 20,
      practiceAreaCodes: Object.fromEntries(
        Object.entries(TRADE_SLUGS).map(([k, v]) => [k, v])
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

      // Block images/fonts/media for speed
      await page.setRequestInterception(true);
      page.on('request', req => {
        const type = req.resourceType();
        if (['image', 'font', 'media'].includes(type)) {
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
   * Navigate to a URL with Cloudflare challenge detection and retry.
   */
  async _navigateWithCf(page, url) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (err) {
      log.warn(`[Checkatrade] Navigation timeout for ${url}: ${err.message}`);
    }

    // Wait for page load
    await new Promise(r => setTimeout(r, 3000));

    let content = await page.content();

    // Check for Cloudflare challenge
    if (this._isCfChallenge(content)) {
      log.info('[Checkatrade] Cloudflare challenge detected — waiting for resolution...');
      // Wait up to 20s for the challenge to auto-resolve
      for (let i = 0; i < 10; i++) {
        await new Promise(r => setTimeout(r, 2000));
        content = await page.content();
        if (!this._isCfChallenge(content) && content.length > 3000) {
          log.info(`[Checkatrade] Cloudflare challenge resolved after ${(i + 1) * 2}s`);
          return content;
        }
      }

      // Try a reload after cookies are set
      try {
        await page.reload({ waitUntil: 'domcontentloaded', timeout: 20000 });
        await new Promise(r => setTimeout(r, 3000));
        content = await page.content();
        if (!this._isCfChallenge(content) && content.length > 3000) {
          log.info('[Checkatrade] Cloudflare resolved after reload');
          return content;
        }
      } catch (_) { /* ignore */ }

      return null; // Blocked
    }

    if (content.length > 3000) return content;
    return null;
  }

  /**
   * Detect Cloudflare challenge pages.
   */
  _isCfChallenge(content) {
    return content.includes('Just a moment') ||
           content.includes('challenge-platform') ||
           content.includes('cf-browser-verification') ||
           content.includes('Attention Required') ||
           content.includes('challenge-running') ||
           (content.includes('Cloudflare') && content.includes('Ray ID'));
  }

  /**
   * Build the search URL for a trade + city combination.
   * Checkatrade URL: /search/?searchTerm={trade}&location={city}&page={n}
   * Also supports: /trades/{trade-slug}/{city-slug}
   */
  _buildSearchUrl(trade, city, pageNum) {
    // Use the search endpoint with query params
    const params = new URLSearchParams();
    params.set('searchTerm', trade);
    params.set('location', city);
    if (pageNum > 1) params.set('page', String(pageNum));
    return `${BASE_URL}/search/?${params.toString()}`;
  }

  /**
   * Extract listings from the page using Puppeteer's DOM access.
   * Tries multiple strategies to handle Checkatrade's evolving markup.
   */
  async _extractListings(page) {
    return page.evaluate(() => {
      const results = [];
      const seen = new Set();

      // Strategy 1: Look for JSON-LD structured data (LocalBusiness schema)
      const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of ldScripts) {
        try {
          const json = JSON.parse(script.textContent);
          const items = Array.isArray(json) ? json : (json['@graph'] || [json]);
          for (const item of items) {
            if (item['@type'] === 'LocalBusiness' || item['@type'] === 'HomeAndConstructionBusiness' ||
                item['@type'] === 'Plumber' || item['@type'] === 'Electrician' ||
                item['@type'] === 'RoofingContractor' || item['@type'] === 'GeneralContractor') {
              const name = item.name || '';
              if (name && !seen.has(name)) {
                seen.add(name);
                results.push({
                  name,
                  phone: item.telephone || '',
                  website: item.url || '',
                  city: item.address?.addressLocality || '',
                  rating: item.aggregateRating?.ratingValue || '',
                  reviewCount: item.aggregateRating?.reviewCount || '',
                  profileUrl: item.url || '',
                });
              }
            }
          }
        } catch (_) { /* ignore parse errors */ }
      }

      if (results.length > 0) return results;

      // Strategy 2: Look for result cards via common selectors
      const cardSelectors = [
        '[data-testid*="search-result"]',
        '[class*="SearchResult"]',
        '[class*="search-result"]',
        '[class*="TradeCard"]',
        '[class*="trade-card"]',
        '[class*="CompanyCard"]',
        '[class*="company-card"]',
        '[class*="listing-card"]',
        '[class*="ListingCard"]',
        'article[class*="result"]',
        '.ch-card',
        '.trade-listing',
      ];

      let cards = [];
      for (const sel of cardSelectors) {
        const found = document.querySelectorAll(sel);
        if (found.length > 0) {
          cards = Array.from(found);
          break;
        }
      }

      // Strategy 3: Look for links to company profiles
      if (cards.length === 0) {
        const profileLinks = document.querySelectorAll('a[href*="/trades/"]');
        const containers = new Map();
        for (const link of profileLinks) {
          const href = link.getAttribute('href') || '';
          // Skip category/location links (e.g. /trades/roofers/london)
          const parts = href.replace(/^\/trades\//, '').split('/').filter(Boolean);
          if (parts.length < 1 || parts.length > 2) continue;
          // Skip if it looks like a category page (known trade slugs)
          const knownCategories = ['roofers','plumbers','painters-decorators','electricians','builders',
            'carpenters','plasterers','tilers','landscapers','heating-engineers','locksmiths'];
          if (knownCategories.includes(parts[0])) continue;

          // Use the link's closest container as the card
          const container = link.closest('div[class]') ||
                           link.closest('article') ||
                           link.closest('li') ||
                           link.parentElement?.parentElement;
          if (container && !containers.has(href)) {
            containers.set(href, container);
          }
        }
        cards = Array.from(containers.values());
      }

      for (const card of cards) {
        const text = card.textContent || '';

        // Company name
        let name = '';
        const nameEl = card.querySelector('h2, h3, h4, [class*="name"], [class*="Name"], [class*="title"], [class*="Title"]');
        if (nameEl) {
          name = nameEl.textContent.trim();
        }

        // Profile URL
        let profileUrl = '';
        const profileLink = card.querySelector('a[href*="/trades/"]');
        if (profileLink) {
          profileUrl = profileLink.getAttribute('href') || '';
          if (profileUrl.startsWith('/')) profileUrl = 'https://www.checkatrade.com' + profileUrl;
          if (!name) name = profileLink.textContent.trim();
        }

        if (!name || name.length < 2) continue;
        if (seen.has(name)) continue;
        seen.add(name);

        // Phone
        let phone = '';
        const phoneEl = card.querySelector('a[href^="tel:"]');
        if (phoneEl) {
          phone = phoneEl.getAttribute('href').replace('tel:', '').trim();
        } else {
          // UK phone regex
          const phoneMatch = text.match(/(0\d{2,4}[\s-]?\d{3,4}[\s-]?\d{3,4})/);
          if (phoneMatch) phone = phoneMatch[1];
        }

        // Website (external links)
        let website = '';
        const extLinks = card.querySelectorAll('a[href^="http"]');
        for (const link of extLinks) {
          const href = link.getAttribute('href') || '';
          if (href && !href.includes('checkatrade.com') && !href.includes('google.com') &&
              !href.includes('facebook.com') && !href.includes('twitter.com')) {
            website = href;
            break;
          }
        }

        // Location
        let city = '';
        const locEl = card.querySelector('[class*="location"], [class*="Location"], [class*="address"], address');
        if (locEl) {
          city = locEl.textContent.trim();
        }

        // Rating
        let rating = '';
        const ratingEl = card.querySelector('[class*="rating"], [class*="Rating"], [class*="score"], [class*="Score"]');
        if (ratingEl) {
          const ratingMatch = ratingEl.textContent.match(/([\d.]+)/);
          if (ratingMatch) rating = ratingMatch[1];
        }

        // Review count
        let reviewCount = '';
        const reviewMatch = text.match(/(\d+)\s*reviews?/i);
        if (reviewMatch) reviewCount = reviewMatch[1];

        results.push({
          name: name.replace(/\s+/g, ' ').trim(),
          phone: phone.replace(/[^\d+()-.\s]/g, '').trim(),
          website,
          city: city.replace(/\s+/g, ' ').trim(),
          rating,
          reviewCount,
          profileUrl,
        });
      }

      return results;
    });
  }

  /**
   * Extract detailed info from a company profile page.
   */
  async _scrapeProfilePage(page, profileUrl) {
    try {
      const content = await this._navigateWithCf(page, profileUrl);
      if (!content) return {};

      return page.evaluate(() => {
        const data = {};

        // JSON-LD
        const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of ldScripts) {
          try {
            const json = JSON.parse(script.textContent);
            const biz = Array.isArray(json) ? json[0] : json;
            if (biz.telephone) data.phone = biz.telephone;
            if (biz.url && !biz.url.includes('checkatrade.com')) data.website = biz.url;
            if (biz.address) {
              if (biz.address.addressLocality) data.city = biz.address.addressLocality;
            }
          } catch (_) { /* ignore */ }
        }

        // Phone from tel: links
        if (!data.phone) {
          const telLink = document.querySelector('a[href^="tel:"]');
          if (telLink) data.phone = telLink.getAttribute('href').replace('tel:', '').trim();
        }

        // Website link
        if (!data.website) {
          const links = document.querySelectorAll('a[href^="http"]');
          for (const link of links) {
            const href = link.getAttribute('href') || '';
            const text = (link.textContent || '').toLowerCase();
            if (href && !href.includes('checkatrade.com') && !href.includes('google.com') &&
                !href.includes('facebook.com') && !href.includes('twitter.com') &&
                !href.includes('instagram.com') && !href.includes('youtube.com') &&
                (text.includes('website') || text.includes('visit') || text.includes('www') ||
                 link.closest('[class*="website"], [class*="Website"]'))) {
              data.website = href;
              break;
            }
          }
        }

        // Look for external website links more broadly
        if (!data.website) {
          const allLinks = document.querySelectorAll('a[target="_blank"], a[rel*="nofollow"]');
          for (const link of allLinks) {
            const href = link.getAttribute('href') || '';
            if (href && href.startsWith('http') && !href.includes('checkatrade.com') &&
                !href.includes('facebook.com') && !href.includes('twitter.com') &&
                !href.includes('instagram.com') && !href.includes('youtube.com') &&
                !href.includes('google.com') && !href.includes('linkedin.com') &&
                !href.includes('pinterest.com') && !href.includes('yelp.com')) {
              data.website = href;
              break;
            }
          }
        }

        return data;
      });
    } catch (err) {
      return {};
    }
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
   * Check for next page and return whether more pages exist.
   */
  async _hasNextPage(page) {
    return page.evaluate(() => {
      // Look for next page button/link
      const nextBtn = document.querySelector(
        'a[aria-label="Next"], a[rel="next"], [class*="next"] a, ' +
        '[class*="Next"] a, [class*="pagination"] a:last-child, ' +
        'button[aria-label="Next"]'
      );
      if (nextBtn) {
        const isDisabled = nextBtn.getAttribute('disabled') !== null ||
                          nextBtn.classList.contains('disabled') ||
                          nextBtn.getAttribute('aria-disabled') === 'true';
        return !isDisabled;
      }
      return false;
    });
  }

  /**
   * Main search generator. Uses Puppeteer with stealth to bypass Cloudflare.
   *
   * @param {string} practiceArea - Trade name (e.g., 'roofers', 'plumbers')
   * @param {object} options - Search options
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || 5;

    // Determine trades to search
    let trades;
    if (practiceArea) {
      const slug = TRADE_SLUGS[practiceArea.toLowerCase()] || practiceArea.toLowerCase();
      trades = [{ name: practiceArea, slug }];
    } else {
      trades = DEFAULT_TRADES.map(t => ({ name: t, slug: TRADE_SLUGS[t] || t }));
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
      yield {
        _captcha: true,
        city: 'all',
        reason: 'Puppeteer not available — Checkatrade requires browser rendering (Cloudflare)',
      };
      return;
    }

    const { browser, page } = session;
    const seenNames = new Set();
    let totalYielded = 0;
    let searchNum = 0;

    try {
      // Warm up: load homepage to establish Cloudflare cookies
      log.info('[Checkatrade] Warming up session (loading homepage)...');
      const homeContent = await this._navigateWithCf(page, BASE_URL);
      if (!homeContent) {
        log.warn('[Checkatrade] Cloudflare blocked homepage — automated access denied');
        yield {
          _captcha: true,
          city: 'all',
          reason: 'Cloudflare WAF blocked all access. May need residential proxy.',
        };
        await browser.close();
        return;
      }
      log.success('[Checkatrade] Homepage loaded — Cloudflare bypassed');
      await new Promise(r => setTimeout(r, 2000));

      for (const trade of trades) {
        for (let ci = 0; ci < cities.length; ci++) {
          const city = cities[ci];
          searchNum++;
          yield { _cityProgress: { current: searchNum, total: totalSearches } };
          log.scrape(`[Checkatrade] Searching ${trade.name} in ${city} (${searchNum}/${totalSearches})`);

          let pageNum = 1;
          let consecutiveEmpty = 0;

          while (pageNum <= maxPages) {
            const url = this._buildSearchUrl(trade.slug, city, pageNum);
            log.info(`[Checkatrade] Page ${pageNum} — ${url}`);

            await rateLimiter.wait();
            const content = await this._navigateWithCf(page, url);

            if (!content) {
              log.warn(`[Checkatrade] Cloudflare blocked search for ${trade.name} in ${city}`);
              yield { _captcha: true, city, reason: 'Cloudflare blocked request' };
              break;
            }

            // Wait for listings to render
            await new Promise(r => setTimeout(r, 2000));

            const listings = await this._extractListings(page);

            if (listings.length === 0) {
              consecutiveEmpty++;
              if (consecutiveEmpty >= 2) {
                log.info(`[Checkatrade] No more results for ${trade.name} in ${city}`);
                break;
              }
              pageNum++;
              continue;
            }

            consecutiveEmpty = 0;
            log.info(`[Checkatrade] Found ${listings.length} listings on page ${pageNum} for ${trade.name} in ${city}`);

            for (const listing of listings) {
              // Dedup by company name
              const key = (listing.name || '').toLowerCase().trim();
              if (!key || seenNames.has(key)) continue;
              seenNames.add(key);

              const lead = {
                first_name: '',
                last_name: '',
                firm_name: listing.name || '',
                city: listing.city || city,
                state: 'UK',
                phone: this._formatUkPhone(listing.phone),
                email: '',
                website: listing.website || '',
                bar_number: '',
                bar_status: listing.rating ? `${listing.rating}/10 (${listing.reviewCount || 0} reviews)` : 'Checkatrade Vetted',
                source: 'checkatrade_contractors',
                practice_area: trade.name,
                profile_url: listing.profileUrl || '',
              };

              yield this.transformResult(lead, trade.name);
              totalYielded++;
            }

            // Check for next page
            const hasNext = await this._hasNextPage(page);
            if (!hasNext) break;

            pageNum++;

            // Polite delay between pages (3-6s)
            await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));
          }

          // Polite delay between city/trade combos (4-8s)
          await new Promise(r => setTimeout(r, 4000 + Math.random() * 4000));
        }
      }

      // Phase 2: Visit profile pages for leads missing website/phone
      const leadsNeedingProfiles = [];
      // (Profile enrichment is handled by the waterfall pipeline)

      log.success(`[Checkatrade] Scrape complete — ${totalYielded} leads (${seenNames.size} unique businesses)`);

    } finally {
      if (browser) await browser.close();
    }
  }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'checkatrade_contractors';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }
}

module.exports = new CheckatradeScraper();
