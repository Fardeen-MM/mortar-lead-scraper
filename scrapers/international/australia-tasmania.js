/**
 * Tasmania (AU-TAS) Law Society -- Find a Lawyer Directory Scraper
 *
 * Source: https://www.lst.org.au/find-a-lawyer/
 * Method: WordPress AJAX (Search & Filter Pro plugin + admin-ajax.php)
 *
 * The Law Society of Tasmania maintains a searchable Register of current
 * Tasmanian lawyers and law firms. The directory can be searched by name,
 * locality, or areas of practice.
 *
 * Technical stack:
 *   - WordPress CMS with Avada theme (Fusion Builder)
 *   - Search & Filter Pro plugin for dynamic filtering
 *   - Ajax Search Pro plugin for live search
 *   - jQuery-based AJAX loading via wp-admin/admin-ajax.php
 *   - SF_LDATA global: { ajax_url, home_url, extensions }
 *
 * The directory uses the standard WordPress 'post' content type with custom
 * taxonomies including 'area_of_practice' for practice area categorization.
 * However, the taxonomy terms are sparsely populated (only Criminal and Family
 * had entries at time of research), suggesting lawyer data may be stored as
 * post content or custom fields rather than taxonomy terms.
 *
 * Search & Filter Pro works by:
 *   1. Rendering a search form with filters on the page (loaded via JS)
 *   2. On filter change, POSTing to admin-ajax.php with action=search_filter_get_results
 *   3. Returning filtered HTML results which are injected into the page
 *
 * The form ID (sfid) is required for Search & Filter Pro AJAX calls. Since
 * it is dynamically rendered and not visible in static HTML, we attempt to
 * discover it by:
 *   a. Fetching the page and looking for sf-form-id in rendered HTML
 *   b. Trying common shortcode IDs
 *   c. Falling back to direct page scraping if AJAX fails
 *
 * Available data per lawyer:
 *   - Name, firm name, locality, areas of practice
 *   - Note: Phone, email, and detailed contact info may not be publicly exposed
 *
 * Limitations:
 *   - Search & Filter Pro forms are JS-rendered; static HTML fetch may not
 *     contain the full form configuration
 *   - The directory states it is "not compatible with a mobile phone or tablet"
 *   - Not all areas of practice may be listed
 *   - The members.lst.org.au subdomain (iMIS-based) refuses external connections
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class TasmaniaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-tasmania',
      stateCode: 'AU-TAS',
      baseUrl: 'https://www.lst.org.au',
      pageSize: 10, // WordPress default posts per page
      practiceAreaCodes: {
        'family': 'family',
        'family law': 'family',
        'criminal': 'criminal',
        'criminal law': 'criminal',
        'property': 'property',
        'property law': 'property',
        'commercial': 'commercial',
        'commercial law': 'commercial',
        'employment': 'employment',
        'employment law': 'employment',
        'litigation': 'litigation',
        'conveyancing': 'conveyancing',
        'personal injury': 'personal-injury',
        'wills': 'wills-and-estates',
        'estates': 'wills-and-estates',
        'wills and estates': 'wills-and-estates',
        'immigration': 'immigration',
        'taxation': 'taxation',
        'administrative': 'administrative-law',
        'administrative law': 'administrative-law',
        'building': 'building-and-construction',
        'construction': 'building-and-construction',
        'environment': 'environmental-law',
        'environmental': 'environmental-law',
        'planning': 'planning',
        'insurance': 'insurance',
        'intellectual property': 'intellectual-property',
        'native title': 'native-title',
        'workers compensation': 'workers-compensation',
      },
      defaultCities: ['Hobart', 'Launceston', 'Devonport', 'Burnie'],
    });

    this.ajaxUrl = `${this.baseUrl}/wp-admin/admin-ajax.php`;
    this.findLawyerUrl = `${this.baseUrl}/find-a-lawyer/`;
    this.wpApiUrl = `${this.baseUrl}/wp-json/wp/v2`;
  }

  // --- HTTP helpers ---

  /**
   * HTTP GET with full response including headers and cookies.
   */
  httpGetFull(url, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      if (cookies) {
        options.headers['Cookie'] = cookies;
      }

      const req = https.get(options, (res) => {
        const setCookies = res.headers['set-cookie'] || [];
        const cookieStr = setCookies.map(c => c.split(';')[0]).join('; ');

        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          const mergedCookies = cookies ? `${cookies}; ${cookieStr}` : cookieStr;
          return resolve(this.httpGetFull(redirect, rateLimiter, mergedCookies));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: cookies ? `${cookies}; ${cookieStr}` : cookieStr,
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * HTTP GET for JSON API responses (WordPress REST API).
   */
  httpGetJson(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      const req = https.get(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          totalPages: parseInt(res.headers['x-wp-totalpages'] || '0', 10),
          totalResults: parseInt(res.headers['x-wp-total'] || '0', 10),
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * HTTP POST to WordPress admin-ajax.php.
   * Used for Search & Filter Pro AJAX calls.
   */
  httpPostAjax(url, formData, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = new URLSearchParams(formData).toString();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': Buffer.byteLength(postBody),
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': this.baseUrl,
          'Referer': this.findLawyerUrl,
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      };

      if (cookies) {
        options.headers['Cookie'] = cookies;
      }

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  // --- WordPress REST API approach ---

  /**
   * Search for lawyer posts using the WordPress REST API.
   * This is the primary scraping strategy: use /wp-json/wp/v2/posts with
   * search and taxonomy filters.
   *
   * @param {string} searchTerm - Name or keyword to search for
   * @param {number} page - Page number (1-based)
   * @param {object} rateLimiter - RateLimiter instance
   * @returns {object} { posts, totalPages, totalResults }
   */
  async _wpApiSearch(searchTerm, page, rateLimiter) {
    const params = new URLSearchParams();
    params.set('per_page', '100'); // Max allowed by WP REST API
    params.set('page', String(page));
    params.set('orderby', 'title');
    params.set('order', 'asc');
    params.set('_fields', 'id,title,content,excerpt,link,tags,area_of_practice');

    if (searchTerm) {
      params.set('search', searchTerm);
    }

    const url = `${this.wpApiUrl}/posts?${params.toString()}`;
    const response = await this.httpGetJson(url, rateLimiter);

    if (response.statusCode !== 200) {
      return { posts: [], totalPages: 0, totalResults: 0 };
    }

    let posts;
    try {
      posts = JSON.parse(response.body);
    } catch (err) {
      return { posts: [], totalPages: 0, totalResults: 0 };
    }

    return {
      posts: Array.isArray(posts) ? posts : [],
      totalPages: response.totalPages,
      totalResults: response.totalResults,
    };
  }

  /**
   * Parse a WordPress post into a lawyer record.
   * The post content typically contains lawyer details embedded in HTML.
   */
  _parseWpPost(post) {
    const title = (post.title && post.title.rendered) || '';
    const content = (post.content && post.content.rendered) || '';
    const excerpt = (post.excerpt && post.excerpt.rendered) || '';
    const link = post.link || '';

    // Parse the title as the lawyer/firm name
    const cleanTitle = this.decodeEntities(title.replace(/<[^>]+>/g, '')).trim();

    // Parse content HTML for contact details
    const $ = cheerio.load(content);
    const contentText = $.text().replace(/\s+/g, ' ').trim();

    // Try to extract structured information from content
    let phone = '';
    let email = '';
    let address = '';
    let firmName = '';
    let city = '';
    let practiceAreas = '';

    // Phone patterns
    const phoneMatch = contentText.match(/(?:Phone|Tel|Ph)[:\s]*([+\d\s()-]{8,})/i) ||
                       contentText.match(/\b((?:\+61|0[2-9])\s*\d[\d\s-]{6,})\b/);
    if (phoneMatch) phone = phoneMatch[1].trim();

    // Email patterns
    const emailMatch = contentText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
    if (emailMatch) email = emailMatch[0];

    // Also check for Cloudflare-encoded emails
    $('a[href^="/cdn-cgi/l/email-protection"]').each((_, el) => {
      const encoded = $(el).attr('data-cfemail') || $(el).attr('href').split('#')[1];
      if (encoded && !email) {
        email = this.decodeCloudflareEmail(encoded);
      }
    });

    // Address patterns -- look for Tasmanian localities
    const addressMatch = contentText.match(
      /(\d+[^,]*(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Place|Pl|Terrace|Tce|Court|Ct|Crescent|Cres|Highway|Hwy|Lane|Ln|Way|Boulevard|Blvd)[^,]*,?\s*(?:Hobart|Launceston|Devonport|Burnie|Kingston|Sandy Bay|Glenorchy|Clarence|Moonah|New Town|Sorell|Ulverstone|Smithton|Queenstown|Rosny|Lindisfarne|Howrah|Bellerive|Battery Point)[^,]*)/i
    );
    if (addressMatch) address = addressMatch[1].trim();

    // City extraction from content
    const cityMatch = contentText.match(
      /\b(Hobart|Launceston|Devonport|Burnie|Kingston|Sandy Bay|Glenorchy|Clarence|Moonah|New Town|Sorell|Ulverstone|Smithton|Queenstown|Rosny|Lindisfarne|Howrah|Bellerive|Battery Point|George Town|Huonville|Bridgewater|Brighton|Claremont|Perth|Scottsdale|Wynyard|Deloraine|Longford|Campbell Town|Oatlands|Swansea|St Helens|Bicheno|Triabunna|Dover|Geeveston|Cygnet|Franklin|Margate)\b/i
    );
    if (cityMatch) city = cityMatch[1];

    // Practice areas from content
    const practiceMatch = contentText.match(
      /(?:Practice\s+Areas?|Areas?\s+of\s+Practice|Specialising?\s+in)[:\s]*([^.]{5,200})/i
    );
    if (practiceMatch) practiceAreas = practiceMatch[1].trim();

    // Determine if this is a lawyer name or firm name
    // If the title contains typical firm indicators, treat as firm
    const isFirm = /(?:lawyers?|solicitors?|legal|law\s+firm|barristers?|associates?|partners?|&|pty|ltd)/i.test(cleanTitle);

    let firstName = '';
    let lastName = '';
    let fullName = '';

    if (isFirm) {
      firmName = cleanTitle;
    } else {
      fullName = cleanTitle;
      const nameParts = this.splitName(cleanTitle);
      firstName = nameParts.firstName;
      lastName = nameParts.lastName;
    }

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: 'TAS',
      zip: '',
      country: 'Australia',
      phone: phone,
      email: email,
      website: '',
      bar_number: String(post.id || ''),
      bar_status: '',
      admission_date: '',
      profile_url: link,
      practice_areas: practiceAreas,
      address: address,
    };
  }

  // --- Search & Filter Pro AJAX approach ---

  /**
   * Attempt to discover the Search & Filter Pro form ID from the page source.
   * The sfid is needed for admin-ajax.php calls.
   */
  async _discoverSfFormId($) {
    // Strategy 1: Look for data-sf-form-id attribute
    const sfFormId = $('[data-sf-form-id]').attr('data-sf-form-id');
    if (sfFormId) return sfFormId;

    // Strategy 2: Look for searchandfilter class with id attribute
    const sfForm = $('form.searchandfilter, .searchandfilter');
    if (sfForm.length) {
      const id = sfForm.attr('id') || '';
      const match = id.match(/(\d+)/);
      if (match) return match[1];
    }

    // Strategy 3: Look for sf_form_id in inline scripts
    const scripts = $('script').toArray();
    for (const script of scripts) {
      const text = $(script).html() || '';
      const match = text.match(/sf_form_id['":\s]+(\d+)/);
      if (match) return match[1];

      const sfidMatch = text.match(/sfid['":\s]+(\d+)/);
      if (sfidMatch) return sfidMatch[1];
    }

    // Strategy 4: Look for shortcode reference in page source
    const bodyHtml = $.html();
    const shortcodeMatch = bodyHtml.match(/\[searchandfilter\s+id="?(\d+)"?\]/);
    if (shortcodeMatch) return shortcodeMatch[1];

    return null;
  }

  /**
   * Try to get results using Search & Filter Pro AJAX.
   * Posts to admin-ajax.php with the search-filter action.
   */
  async _sfProSearch(sfFormId, searchTerm, page, rateLimiter, cookies) {
    const formData = {
      action: 'search_filter_get_results',
      sfid: sfFormId,
      sf_paged: String(page),
    };

    if (searchTerm) {
      formData['_sf_s'] = searchTerm;
    }

    const response = await this.httpPostAjax(this.ajaxUrl, formData, rateLimiter, cookies);

    if (response.statusCode !== 200) {
      return { html: '', success: false };
    }

    // Response could be JSON with results HTML or raw HTML
    let html = response.body;
    try {
      const json = JSON.parse(response.body);
      if (json.results) html = json.results;
      if (json.html) html = json.html;
    } catch (e) {
      // Response is raw HTML, which is fine
    }

    return { html, success: true };
  }

  /**
   * Parse lawyer records from Search & Filter Pro AJAX HTML response.
   */
  _parseSfResults(html) {
    const $ = cheerio.load(html);
    const lawyers = [];

    // Search & Filter Pro typically returns posts in article/div containers
    const postSelectors = [
      'article',
      '.post',
      '.type-post',
      '.fusion-post-content',
      '.search-result',
      '.result-item',
      'li',
      '.entry',
    ];

    let posts = $();
    for (const selector of postSelectors) {
      posts = $(selector);
      if (posts.length > 0) break;
    }

    posts.each((_, el) => {
      const $post = $(el);

      // Get title/name
      const titleEl = $post.find('h1, h2, h3, h4, .entry-title, .post-title, a').first();
      const name = (titleEl.text() || '').trim();
      if (!name || name.length < 2) return;

      // Get link
      const link = titleEl.attr('href') || $post.find('a').first().attr('href') || '';

      // Get content text
      const contentText = $post.text().replace(/\s+/g, ' ').trim();

      // Extract details from content
      let phone = '';
      let email = '';
      let city = '';
      let practiceAreas = '';

      const phoneMatch = contentText.match(/(?:Phone|Tel|Ph)[:\s]*([+\d\s()-]{8,})/i) ||
                         contentText.match(/\b((?:\+61|0[2-9])\s*\d[\d\s-]{6,})\b/);
      if (phoneMatch) phone = phoneMatch[1].trim();

      const emailMatch = contentText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
      if (emailMatch) email = emailMatch[0];

      const cityMatch = contentText.match(
        /\b(Hobart|Launceston|Devonport|Burnie|Kingston|Sandy Bay|Glenorchy|Moonah|Sorell|Ulverstone)\b/i
      );
      if (cityMatch) city = cityMatch[1];

      const isFirm = /(?:lawyers?|solicitors?|legal|law\s+firm|barristers?|associates?|&|pty|ltd)/i.test(name);
      let firstName = '';
      let lastName = '';
      let fullName = '';
      let firmName = '';

      if (isFirm) {
        firmName = name;
      } else {
        fullName = name;
        const parts = this.splitName(name);
        firstName = parts.firstName;
        lastName = parts.lastName;
      }

      lawyers.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'TAS',
        zip: '',
        country: 'Australia',
        phone: phone,
        email: email,
        website: '',
        bar_number: '',
        bar_status: '',
        admission_date: '',
        profile_url: link.startsWith('http') ? link : (link ? `${this.baseUrl}${link}` : ''),
        practice_areas: practiceAreas,
      });
    });

    return lawyers;
  }

  // --- Direct page scraping approach ---

  /**
   * Scrape the Find a Lawyer page directly for any statically-rendered content.
   * This is a fallback if AJAX approaches fail.
   */
  _parseDirectPage($) {
    const lawyers = [];

    // Look for any structured lawyer listings in the page content
    // The Avada theme uses various layout containers
    const contentSelectors = [
      '.fusion-text',
      '.post-content',
      '.entry-content',
      '.page-content',
      '#content',
      'main',
    ];

    let contentArea = $();
    for (const selector of contentSelectors) {
      contentArea = $(selector);
      if (contentArea.length > 0) break;
    }

    // Look for links that might be lawyer profile links
    contentArea.find('a[href*="find-a-lawyer"], a[href*="lawyer"], a[href*="solicitor"]').each((_, el) => {
      const name = $(el).text().trim();
      if (name && name.length > 2 && !name.toLowerCase().includes('find') &&
          !name.toLowerCase().includes('search') && !name.toLowerCase().includes('register')) {
        const link = $(el).attr('href') || '';
        const { firstName, lastName } = this.splitName(name);

        lawyers.push({
          first_name: firstName,
          last_name: lastName,
          full_name: name,
          firm_name: '',
          city: '',
          state: 'TAS',
          zip: '',
          country: 'Australia',
          phone: '',
          email: '',
          website: '',
          bar_number: '',
          bar_status: '',
          admission_date: '',
          profile_url: link.startsWith('http') ? link : `${this.baseUrl}${link}`,
          practice_areas: '',
        });
      }
    });

    return lawyers;
  }

  // --- BaseScraper overrides ---

  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    if (city) params.set('_sf_s', city);
    if (practiceCode) params.set('_sfm_area_of_practice', practiceCode);
    params.set('sf_paged', String(page || 1));
    return `${this.findLawyerUrl}?${params.toString()}`;
  }

  parseResultsPage($) {
    return this._parseDirectPage($);
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/(\d+)\s+results?\s+found/i);
    if (match) return parseInt(match[1], 10);

    const showingMatch = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+(\d+)/i);
    if (showingMatch) return parseInt(showingMatch[1], 10);

    return 0;
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields lawyer records from the TAS Law Society directory.
   *
   * Multi-strategy approach:
   * 1. First, try the WordPress REST API (/wp-json/wp/v2/posts) with search queries
   *    for each city. This is the simplest and most reliable method if the lawyer
   *    data is stored as WordPress posts.
   *
   * 2. If WP REST API returns no results, try Search & Filter Pro AJAX. Fetch
   *    the find-a-lawyer page to discover the SF form ID, then POST to
   *    admin-ajax.php with search parameters.
   *
   * 3. If AJAX also fails, fall back to direct page scraping of the find-a-lawyer
   *    page and any paginated results URLs.
   *
   * 4. If all approaches fail, yield a placeholder with clear logging about
   *    what was tried and why it failed.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);
    const seen = new Set();
    let totalYielded = 0;

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for AU-TAS -- searching without filter`);
    }

    const cities = this.getCities(options);
    log.scrape('AU-TAS: Starting Law Society of Tasmania directory scrape');
    log.info(`AU-TAS: Directory URL: ${this.findLawyerUrl}`);
    log.info(`AU-TAS: Searching cities: ${cities.join(', ')}`);

    // ---- Strategy 1: WordPress REST API ----
    log.info('AU-TAS: Trying WordPress REST API approach (wp-json/wp/v2/posts)');

    let wpApiWorks = false;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`AU-TAS: Searching "${city}" via WP REST API`);

      if (options.maxPages && ci >= options.maxPages) {
        log.info(`AU-TAS: Reached max search limit (${options.maxPages}) -- stopping`);
        break;
      }

      let page = 1;
      let totalPages = 1;

      while (page <= totalPages) {
        try {
          await rateLimiter.wait();
          const result = await this._wpApiSearch(city, page, rateLimiter);

          if (page === 1) {
            totalPages = result.totalPages || 1;
            if (result.totalResults > 0) {
              log.success(`AU-TAS: WP API found ${result.totalResults} posts for "${city}" (${totalPages} pages)`);
              wpApiWorks = true;
            } else {
              log.info(`AU-TAS: WP API returned 0 results for "${city}"`);
              break;
            }
          }

          if (result.posts.length === 0) break;

          for (const post of result.posts) {
            const lawyer = this._parseWpPost(post);
            const key = lawyer.full_name || lawyer.firm_name || String(post.id);
            if (seen.has(key)) continue;
            seen.add(key);

            // Filter by city if the post content mentions it
            // (WP search is fuzzy, so we do our own filtering)
            if (lawyer.city || !options.city) {
              if (options.minYear && lawyer.admission_date) {
                const year = parseInt((lawyer.admission_date.match(/\d{4}/) || ['0'])[0], 10);
                if (year > 0 && year < options.minYear) continue;
              }
              yield this.transformResult(lawyer, practiceArea);
              totalYielded++;
            }
          }

          page++;
        } catch (err) {
          log.error(`AU-TAS: WP API error for "${city}" page ${page}: ${err.message}`);
          break;
        }
      }
    }

    if (wpApiWorks && totalYielded > 0) {
      log.success(`AU-TAS: WP REST API yielded ${totalYielded} records`);
      return;
    }

    // ---- Strategy 2: Search & Filter Pro AJAX ----
    log.info('AU-TAS: WP REST API did not return lawyer data. Trying Search & Filter Pro AJAX.');

    let cookies = '';
    let sfFormId = null;

    // Fetch the find-a-lawyer page to discover form ID and get cookies
    try {
      await rateLimiter.wait();
      const pageResponse = await this.httpGetFull(this.findLawyerUrl, rateLimiter);

      if (pageResponse.statusCode === 200) {
        cookies = pageResponse.cookies || '';
        const $ = cheerio.load(pageResponse.body);
        sfFormId = await this._discoverSfFormId($);

        if (sfFormId) {
          log.success(`AU-TAS: Discovered Search & Filter form ID: ${sfFormId}`);
        } else {
          log.info('AU-TAS: Could not discover Search & Filter form ID from page source');
          log.info('AU-TAS: The form may be loaded dynamically via JavaScript');

          // Try direct page scraping as a sub-strategy
          const directResults = this._parseDirectPage($);
          if (directResults.length > 0) {
            log.success(`AU-TAS: Found ${directResults.length} lawyer records from direct page scraping`);
            for (const lawyer of directResults) {
              const key = lawyer.full_name || lawyer.firm_name;
              if (!seen.has(key)) {
                seen.add(key);
                yield this.transformResult(lawyer, practiceArea);
                totalYielded++;
              }
            }
          }
        }
      }
    } catch (err) {
      log.error(`AU-TAS: Failed to fetch find-a-lawyer page: ${err.message}`);
    }

    // If we found the form ID, try AJAX searches
    if (sfFormId) {
      for (let ci = 0; ci < cities.length; ci++) {
        const city = cities[ci];
        log.scrape(`AU-TAS: Searching "${city}" via Search & Filter AJAX (sfid=${sfFormId})`);

        let page = 1;
        let maxPages = 20; // Safety limit

        while (page <= maxPages) {
          if (options.maxPages && page > options.maxPages) break;

          try {
            await rateLimiter.wait();
            const result = await this._sfProSearch(sfFormId, city, page, rateLimiter, cookies);

            if (!result.success) {
              log.error(`AU-TAS: SF Pro AJAX failed for "${city}" page ${page}`);
              break;
            }

            const lawyers = this._parseSfResults(result.html);

            if (lawyers.length === 0) {
              if (page === 1) {
                log.info(`AU-TAS: No SF Pro results for "${city}"`);
              }
              break;
            }

            log.success(`AU-TAS: SF Pro returned ${lawyers.length} records for "${city}" page ${page}`);

            for (const lawyer of lawyers) {
              const key = lawyer.full_name || lawyer.firm_name;
              if (seen.has(key)) continue;
              seen.add(key);

              yield this.transformResult(lawyer, practiceArea);
              totalYielded++;
            }

            // If fewer results than expected, no more pages
            if (lawyers.length < this.pageSize) break;

            page++;
          } catch (err) {
            log.error(`AU-TAS: SF Pro error for "${city}": ${err.message}`);
            break;
          }
        }
      }
    }

    // ---- Strategy 3: Paginated URL scraping ----
    if (totalYielded === 0) {
      log.info('AU-TAS: Trying paginated URL scraping as final fallback');

      for (let ci = 0; ci < cities.length; ci++) {
        const city = cities[ci];

        // Try Search & Filter URL format: ?_sf_s=Hobart&sf_paged=1
        for (let page = 1; page <= 5; page++) {
          if (options.maxPages && page > options.maxPages) break;

          const url = this.buildSearchUrl({ city, practiceCode, page });
          log.info(`AU-TAS: Fetching ${url}`);

          try {
            await rateLimiter.wait();
            const response = await this.httpGetFull(url, rateLimiter, cookies);

            if (response.statusCode !== 200) {
              log.info(`AU-TAS: Got status ${response.statusCode} for "${city}" page ${page}`);
              break;
            }

            if (this.detectCaptcha(response.body)) {
              log.warn('AU-TAS: CAPTCHA detected -- stopping');
              yield { _captcha: true, city, page };
              break;
            }

            const $ = cheerio.load(response.body);
            const lawyers = this._parseDirectPage($);

            if (lawyers.length === 0) break;

            for (const lawyer of lawyers) {
              const key = lawyer.full_name || lawyer.firm_name;
              if (seen.has(key)) continue;
              seen.add(key);
              yield this.transformResult(lawyer, practiceArea);
              totalYielded++;
            }

            // Check for next page link
            const hasNext = $('a.next, .pagination .next, a[rel="next"]').length > 0;
            if (!hasNext) break;
          } catch (err) {
            log.error(`AU-TAS: Page scrape error for "${city}": ${err.message}`);
            break;
          }
        }
      }
    }

    // ---- Final status report ----
    if (totalYielded > 0) {
      log.success(`AU-TAS: Directory scrape complete -- ${totalYielded} records yielded`);
    } else {
      log.warn('AU-TAS: All scraping strategies returned 0 results.');
      log.warn('AU-TAS: The Find a Lawyer directory at lst.org.au uses JavaScript-rendered');
      log.warn('AU-TAS: Search & Filter Pro forms that require a full browser environment.');
      log.warn('AU-TAS: The directory content loads dynamically via AJAX after JS execution.');
      log.info('AU-TAS: To scrape this directory, one of the following would be needed:');
      log.info('AU-TAS:   1. A headless browser (Puppeteer/Playwright) to render the JS');
      log.info('AU-TAS:   2. Discovery of the exact Search & Filter form ID (sfid)');
      log.info('AU-TAS:   3. Access to members.lst.org.au (currently refuses connections)');
      log.info('AU-TAS: Alternative: Legal Profession Board of Tasmania at lpbt.com.au');
      yield { _placeholder: true, reason: 'js_rendering_required', strategies_tried: [
        'wp_rest_api', 'search_filter_pro_ajax', 'paginated_url_scraping'
      ]};
    }
  }
}

module.exports = new TasmaniaScraper();
