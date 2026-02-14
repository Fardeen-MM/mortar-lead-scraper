/**
 * Wyoming Bar Association Scraper
 *
 * Source: https://www.wyomingbar.org/directory/
 * Method: WordPress + downloadable Word doc — scrape directory page or find download
 *
 * The Wyoming State Bar publishes its member directory on a WordPress site.
 * It may also offer a downloadable Word document or PDF of the full list.
 * This scraper attempts to:
 *  1. Scrape the WordPress directory page for attorney listings
 *  2. Look for download links (Word doc, PDF, CSV)
 *  3. Parse paginated WordPress results
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WyomingScraper extends BaseScraper {
  constructor() {
    super({
      name: 'wyoming',
      stateCode: 'WY',
      baseUrl: 'https://www.wyomingbar.org/directory/',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'agricultural':          'Agricultural Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'employment':            'Employment Law',
        'energy':                'Energy Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'mineral':               'Mineral Law',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'water':                 'Water Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Cheyenne', 'Casper', 'Laramie', 'Gillette',
        'Rock Springs', 'Sheridan', 'Green River', 'Jackson',
      ],
    });

    this.origin = 'https://www.wyomingbar.org';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for WordPress directory`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for WordPress directory`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for WordPress directory`);
  }

  /**
   * HTTP POST for WordPress form submissions or API calls.
   */
  httpPost(url, data, rateLimiter, headers = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const isJson = typeof data === 'object' && !(data instanceof URLSearchParams);
      const postData = isJson ? JSON.stringify(data) : (typeof data === 'string' ? data : new URLSearchParams(data).toString());
      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': isJson ? 'application/json' : 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json,text/html,*/*',
          'Origin': this.origin,
          'Referer': this.baseUrl,
          ...headers,
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(opts, (res) => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(postData);
      req.end();
    });
  }

  /**
   * Look for download links on the directory page.
   */
  _findDownloadLinks($) {
    const links = [];
    const downloadPatterns = [
      'a[href$=".docx"]', 'a[href$=".doc"]', 'a[href$=".pdf"]',
      'a[href$=".xlsx"]', 'a[href$=".csv"]',
      'a[href*="download"]', 'a[href*="export"]',
      'a:contains("Download")', 'a:contains("download")',
      'a:contains("Word")', 'a:contains("PDF")',
      'a:contains("Directory")', 'a:contains("List")',
    ];

    for (const selector of downloadPatterns) {
      $(selector).each((_, el) => {
        let href = $(el).attr('href') || '';
        const text = $(el).text().trim();
        if (href && !href.includes('javascript:')) {
          if (href.startsWith('/')) href = `${this.origin}${href}`;
          else if (!href.startsWith('http')) href = `${this.origin}/${href}`;
          links.push({ url: href, text });
        }
      });
    }

    return links;
  }

  /**
   * Parse attorney listings from WordPress directory page.
   */
  _parseDirectoryPage(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // WordPress directory plugins use various formats

    // Format 1: Table-based directory
    $('table tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|result|page)/i.test(fullName)) return;
      if (fullName.length < 2 || fullName.length > 100) return;

      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
        city: cells.length > 2 ? $(cells[2]).text().trim() : '',
        state: 'WY',
        phone: cells.length > 3 ? $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '') : '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
      });
    });

    // Format 2: WordPress custom post type / directory plugin (div-based cards)
    if (attorneys.length === 0) {
      $('.member-listing, .directory-listing, .attorney-card, .attorney-listing, ' +
        '.wp-block-group, .entry-content .member, article.member, .directory-item, ' +
        '.elementor-post, .jet-listing-grid__item').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h2, h3, h4, .name, .member-name, .attorney-name, .entry-title').first();
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 2 || fullName.length > 100) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileUrl = nameEl.is('a') ? nameEl.attr('href') : $el.find('a').first().attr('href') || '';

        // Try to extract data from various field formats
        const textContent = $el.text();
        const phoneMatch = textContent.match(/(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);
        const emailLink = $el.find('a[href^="mailto:"]');
        const cityMatch = textContent.match(/(?:City|Location|Office)[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)/);

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $el.find('.firm, .firm-name, .company, .organization').text().trim(),
          city: cityMatch ? cityMatch[1].trim() : ($el.find('.city, .location, .address').text().trim().split(',')[0] || ''),
          state: 'WY',
          phone: phoneMatch ? phoneMatch[1] : '',
          email: emailLink.length ? emailLink.attr('href').replace('mailto:', '').trim() : '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
        });
      });
    }

    // Format 3: Simple list with name + details blocks
    if (attorneys.length === 0) {
      $('article, .post, .entry, .hentry').each((_, el) => {
        const $el = $(el);
        const title = $el.find('h2 a, h3 a, .entry-title a').first();
        const fullName = title.text().trim();
        if (!fullName || fullName.length < 2 || fullName.length > 100) return;

        // Check if this looks like a person name (not a blog post title)
        if (fullName.split(/\s+/).length > 5) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileUrl = title.attr('href') || '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: '',
          city: '',
          state: 'WY',
          phone: '',
          email: '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl,
        });
      });
    }

    return attorneys;
  }

  /**
   * Try WordPress REST API for custom post types.
   */
  async _tryWordPressApi(city, rateLimiter) {
    const wpApiPaths = [
      `/wp-json/wp/v2/member?search=${encodeURIComponent(city)}&per_page=${this.pageSize}`,
      `/wp-json/wp/v2/attorney?search=${encodeURIComponent(city)}&per_page=${this.pageSize}`,
      `/wp-json/wp/v2/lawyer?search=${encodeURIComponent(city)}&per_page=${this.pageSize}`,
      `/wp-json/wp/v2/directory?search=${encodeURIComponent(city)}&per_page=${this.pageSize}`,
      `/wp-json/wp/v2/posts?search=${encodeURIComponent(city)}&per_page=${this.pageSize}&categories=member`,
    ];

    for (const path of wpApiPaths) {
      const url = `${this.origin}${path}`;
      try {
        await rateLimiter.wait();
        const resp = await this.httpGet(url, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (Array.isArray(data) && data.length > 0) {
              log.success(`WordPress API found: ${url}`);
              return { url: url.split('?')[0], data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }
    }

    return null;
  }

  /**
   * Parse WordPress REST API response for attorney data.
   */
  _parseWpApiResponse(data) {
    const attorneys = [];
    if (!Array.isArray(data)) return attorneys;

    for (const post of data) {
      const fullName = (post.title?.rendered || post.title || '').replace(/<[^>]*>/g, '').trim();
      if (!fullName || fullName.length < 2) continue;

      const { firstName, lastName } = this.splitName(fullName);
      const content = (post.content?.rendered || '').replace(/<[^>]*>/g, ' ').trim();
      const phoneMatch = content.match(/(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);
      const emailMatch = content.match(/[\w.-]+@[\w.-]+\.\w{2,}/);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: post.acf?.firm_name || post.meta?.firm_name || '',
        city: post.acf?.city || post.meta?.city || '',
        state: 'WY',
        phone: phoneMatch ? phoneMatch[1] : (post.acf?.phone || post.meta?.phone || ''),
        email: emailMatch ? emailMatch[0] : (post.acf?.email || post.meta?.email || ''),
        website: post.acf?.website || post.meta?.website || '',
        bar_number: post.acf?.bar_number || post.meta?.bar_number || '',
        bar_status: 'Active',
        profile_url: post.link || '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Wyoming WordPress directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    log.scrape('Attempting to access WY Bar directory...');

    // Step 1: Fetch the directory page
    let directoryBody = '';
    let downloadLinks = [];

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.error(`WY Bar directory returned status ${response.statusCode}`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode} from WY Bar` };
        return;
      }

      directoryBody = response.body;
      const $ = cheerio.load(directoryBody);

      // Check for download links
      downloadLinks = this._findDownloadLinks($);
      if (downloadLinks.length > 0) {
        log.info(`Found ${downloadLinks.length} potential download link(s):`);
        for (const link of downloadLinks) {
          log.info(`  - ${link.text}: ${link.url}`);
        }
      }

      // Check for search form or directory content
      const hasSearchForm = $('form').filter((_, el) => {
        const action = $(el).attr('action') || '';
        const text = $(el).text().toLowerCase();
        return action.includes('search') || action.includes('directory') ||
               text.includes('search') || text.includes('find');
      }).length > 0;

      if (hasSearchForm) {
        log.info('Found search form on directory page');
      }
    } catch (err) {
      log.error(`Failed to fetch WY Bar directory: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    // Step 2: Try to parse attorneys directly from the page
    const pageAttorneys = this._parseDirectoryPage(directoryBody);
    if (pageAttorneys.length > 0) {
      log.success(`Found ${pageAttorneys.length} attorneys on directory page`);
      yield { _cityProgress: { current: 1, total: cities.length } };

      const citySet = new Set(cities.map(c => c.toLowerCase()));
      for (const attorney of pageAttorneys) {
        if (attorney.city && !citySet.has(attorney.city.toLowerCase())) continue;
        yield this.transformResult(attorney, practiceArea);
      }
    }

    // Step 3: Try WordPress REST API
    log.info('Trying WordPress REST API...');
    const wpResult = await this._tryWordPressApi(cities[0], rateLimiter);

    if (wpResult) {
      const wpAttorneys = this._parseWpApiResponse(wpResult.data);
      if (wpAttorneys.length > 0) {
        log.success(`WordPress API returned ${wpAttorneys.length} attorneys`);
        yield { _cityProgress: { current: 1, total: cities.length } };
        for (const attorney of wpAttorneys) {
          yield this.transformResult(attorney, practiceArea);
        }

        // Continue with remaining cities
        for (let ci = 1; ci < cities.length; ci++) {
          const city = cities[ci];
          yield { _cityProgress: { current: ci + 1, total: cities.length } };
          log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

          const wpApiBase = wpResult.url;
          let page = 1;
          let pagesFetched = 0;

          while (true) {
            if (options.maxPages && pagesFetched >= options.maxPages) break;

            const url = `${wpApiBase}?search=${encodeURIComponent(city)}&per_page=${this.pageSize}&page=${page}`;
            try {
              await rateLimiter.wait();
              const resp = await this.httpGet(url, rateLimiter);
              if (resp.statusCode !== 200) break;

              const data = JSON.parse(resp.body);
              const atts = this._parseWpApiResponse(data);
              if (atts.length === 0) break;

              for (const attorney of atts) {
                yield this.transformResult(attorney, practiceArea);
              }

              if (atts.length < this.pageSize) break;
              page++;
              pagesFetched++;
            } catch (_) {
              break;
            }
          }
        }
        return;
      }
    }

    // Step 4: Try paginated directory scraping
    if (pageAttorneys.length === 0) {
      // Try searching with city parameter in URL
      for (let ci = 0; ci < cities.length; ci++) {
        const city = cities[ci];
        yield { _cityProgress: { current: ci + 1, total: cities.length } };
        log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

        let page = 1;
        let pagesFetched = 0;

        while (true) {
          if (options.maxPages && pagesFetched >= options.maxPages) break;

          // Try common WordPress directory search URL patterns
          const searchUrls = [
            `${this.baseUrl}?search=${encodeURIComponent(city)}&page=${page}`,
            `${this.baseUrl}page/${page}/?search=${encodeURIComponent(city)}`,
            `${this.origin}/directory/?city=${encodeURIComponent(city)}&page=${page}`,
          ];

          let found = false;
          for (const url of searchUrls) {
            try {
              await rateLimiter.wait();
              const resp = await this.httpGet(url, rateLimiter);

              if (resp.statusCode !== 200) continue;

              const attorneys = this._parseDirectoryPage(resp.body);
              if (attorneys.length > 0) {
                if (page === 1) log.success(`Found ${attorneys.length} results for ${city}`);
                for (const attorney of attorneys) {
                  yield this.transformResult(attorney, practiceArea);
                }
                found = true;
                break;
              }
            } catch (_) { /* continue */ }
          }

          if (!found) {
            if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }

          page++;
          pagesFetched++;
        }
      }

      // If nothing found at all, report the download option
      if (downloadLinks.length > 0) {
        log.warn(`WY: Could not scrape directory pages, but download links were found:`);
        for (const link of downloadLinks) {
          log.warn(`WY:   ${link.text}: ${link.url}`);
        }
        log.warn(`WY: Consider downloading the directory file manually.`);
      } else {
        log.warn(`WY: WordPress directory — no parseable attorney data found.`);
        log.warn(`WY: The directory at ${this.baseUrl} may use a complex WordPress plugin.`);
        yield { _captcha: true, city: 'all', reason: 'WordPress directory — no parseable data found' };
      }
    }
  }
}

module.exports = new WyomingScraper();
