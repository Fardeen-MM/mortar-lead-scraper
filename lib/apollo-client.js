/**
 * Apollo.io API Client
 *
 * Two modes:
 *   1. API Key mode (official API) — FREE search, but data is obfuscated on free tier
 *   2. Browser mode — launches Puppeteer, injects cookies, makes API calls from
 *      within browser context to bypass Cloudflare Turnstile. Gets full data
 *      including emails, phones, LinkedIn, etc. This is how Apify does it.
 *
 * Usage (browser mode — recommended):
 *   const apollo = new ApolloClient({ cookie: 'your-session-cookie', csrfToken: 'your-csrf' });
 *   await apollo.init();  // launches browser
 *   const results = await apollo.searchPeople({ person_titles: ['attorney'], person_locations: ['Florida, US'] });
 *   await apollo.close();
 *
 * Usage (API key mode):
 *   const apollo = new ApolloClient({ apiKey: 'your-api-key' });
 *   const results = await apollo.searchPeople({ ... });
 */

const https = require('https');
const { log } = require('./logger');

const SENIORITIES = ['owner', 'founder', 'c_suite', 'partner', 'vp', 'director', 'manager', 'senior', 'entry'];
const EMPLOYEE_RANGES = ['1,10', '11,20', '21,50', '51,100', '101,200', '201,500', '501,1000', '1001,2000', '2001,5000', '5001,10000', '10001,'];

class ApolloClient {
  constructor(config = {}) {
    if (typeof config === 'string') {
      this.apiKey = config;
      this.cookie = null;
    } else {
      this.apiKey = config.apiKey || null;
      this.cookie = config.cookie || null;
      this.csrfToken = config.csrfToken || null;
      this.email = config.email || null;
      this.password = config.password || null;
      this.chromeProfile = config.chromeProfile || null;
    }

    this.mode = (this.cookie || this.email || this.chromeProfile) ? 'browser' : 'api';
    this.browser = null;
    this.page = null;
    this.requestsThisMinute = 0;
    this.minuteStart = Date.now();
    this.maxPerMinute = (typeof config === 'object' ? config.maxPerMinute : null) || 50;
    this.maxPerDay = (typeof config === 'object' ? config.maxPerDay : null) || 2000;
    this.requestsToday = 0;
    this.dayStart = Date.now();

    this.stats = {
      requests: 0,
      people: 0,
      pages: 0,
      errors: 0,
      rateLimits: 0,
    };
  }

  /**
   * Initialize browser for cookie mode. Must call before searchPeople().
   *
   * Priority: Chrome profile > email/password > cookie injection.
   * Chrome profile (from apollo-login.js) is fastest and most reliable.
   */
  async init() {
    if (this.mode !== 'browser') return;

    const chromeProfile = this.chromeProfile || process.env.APOLLO_CHROME_PROFILE || '/tmp/apollo-session-profile';

    // Prefer Chrome profile (created by apollo-login.js) — has real session
    if (chromeProfile && require('fs').existsSync(chromeProfile)) {
      await this._loginWithChromeProfile(chromeProfile);
    } else {
      const email = this.email || process.env.APOLLO_EMAIL;
      const password = this.password || process.env.APOLLO_PASSWORD;

      // Need a generic browser for cookie/credential login
      const puppeteer = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteer.use(StealthPlugin());

      this.browser = await puppeteer.launch({
        headless: false,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--window-size=1280,900'],
        defaultViewport: null,
      });
      this.page = (await this.browser.pages())[0] || await this.browser.newPage();

      if (email && password) {
        await this._loginWithCredentials(email, password);
      } else {
        await this._loginWithCookies();
      }
    }
  }

  /**
   * Login using saved Chrome profile (created by apollo-login.js).
   */
  async _loginWithChromeProfile(profilePath) {
    log.info('[Apollo] Closing any existing browser instance...');
    if (this.browser) await this.browser.close().catch(() => {});

    // Re-launch with real Chrome + saved profile
    // MUST use headless: false — Cloudflare blocks headless mode even with stealth plugin.
    // With visible Chrome, both fetch() and XHR work fine from page context.
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

    log.info('[Apollo] Launching Chrome with saved profile (visible window)...');
    this.browser = await puppeteer.launch({
      headless: false,
      executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      userDataDir: profilePath,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-extensions',
        '--window-size=1280,900',
      ],
      defaultViewport: null,
    });

    this.page = (await this.browser.pages())[0] || await this.browser.newPage();

    log.info('[Apollo] Loading Apollo.io...');
    await this.page.goto('https://app.apollo.io/#/people', {
      waitUntil: 'networkidle2',
      timeout: 90000,
    });
    await this._sleep(3000);

    // Wait for SPA to fully initialize (check for login redirect)
    const url = this.page.url();
    const title = await this.page.title();
    log.info(`[Apollo] Page: ${title} | URL: ${url.slice(0, 80)}`);

    if (url.includes('/login') || url.includes('/signup')) {
      throw new Error('Apollo session expired. Run: node scripts/apollo-login.js');
    }

    // Verify API access with a quick test call
    const testResult = await this.page.evaluate(() => {
      return new Promise(resolve => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/v1/mixed_people/search');
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.withCredentials = true;
        xhr.timeout = 15000;
        xhr.onload = () => {
          try {
            const data = JSON.parse(xhr.responseText);
            resolve({ ok: true, people: (data.people || []).length, total: data.pagination?.total_entries || 0 });
          } catch { resolve({ ok: false, status: xhr.status, body: xhr.responseText.slice(0, 100) }); }
        };
        xhr.onerror = () => resolve({ ok: false, error: 'network' });
        xhr.ontimeout = () => resolve({ ok: false, error: 'timeout' });
        xhr.send(JSON.stringify({ page: 1, per_page: 1 }));
      });
    });

    if (!testResult.ok) {
      throw new Error(`Apollo API test failed: ${testResult.error || testResult.body || 'unknown'}`);
    }

    log.info(`[Apollo] API verified — ${testResult.people} test result(s)`);
    await this._extractCsrfToken();
    log.info('[Apollo] Ready — using Chrome profile session');
  }

  /**
   * Login via email/password — creates a fresh session that Cloudflare trusts.
   */
  async _loginWithCredentials(email, password) {
    log.info('[Apollo] Logging in with email/password...');

    await this.page.goto('https://app.apollo.io/#/login', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    // Wait for login form
    await this.page.waitForSelector('input[name="email"]', { timeout: 30000 });
    await this._sleep(1000);

    // Type email
    await this.page.type('input[name="email"]', email, { delay: 50 });
    await this._sleep(500);

    // Type password
    await this.page.type('input[name="password"]', password, { delay: 50 });
    await this._sleep(500);

    // Click login button
    await this.page.click('button[type="submit"]');

    // Wait for navigation to complete (could be 2FA, Cloudflare, or direct login)
    log.info('[Apollo] Waiting for login to complete...');
    await this.page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 60000 }).catch(() => {});
    await this._sleep(5000);

    // Check if we're logged in
    const url = this.page.url();
    if (url.includes('/login')) {
      // Check for error messages
      const error = await this.page.evaluate(() => {
        const el = document.querySelector('.error-message, .alert-danger, [data-testid="error"]');
        return el ? el.textContent : null;
      });
      throw new Error(`Apollo login failed: ${error || 'Check credentials'}`);
    }

    // Navigate to people search
    await this.page.goto('https://app.apollo.io/#/people', {
      waitUntil: 'networkidle2',
      timeout: 30000,
    }).catch(() => {});
    await this._sleep(3000);

    await this._extractCsrfToken();
    log.info('[Apollo] Logged in successfully via email/password');
  }

  /**
   * Login by injecting session cookies (may fail if Cloudflare rejects them).
   */
  async _loginWithCookies() {
    // Set cookies before navigating
    const cookies = this._parseCookies(this.cookie);
    if (cookies.length > 0) {
      await this.page.setCookie(...cookies);
    }

    log.info('[Apollo] Loading Apollo.io with session cookies...');
    await this.page.goto('https://app.apollo.io/', {
      waitUntil: 'domcontentloaded',
      timeout: 90000,
    });

    // Wait for Cloudflare Turnstile to auto-solve (if present)
    log.info('[Apollo] Waiting for page to stabilize...');
    for (let i = 0; i < 30; i++) {
      await this._sleep(2000);
      const pageContent = await this.page.content();
      if (!pageContent.includes('cf-turnstile') && !pageContent.includes('challenges.cloudflare.com')) {
        break;
      }
      if (i % 5 === 0 && i > 0) {
        log.info(`[Apollo] Cloudflare challenge pending... (${i * 2}s)`);
      }
    }

    await this.page.goto('https://app.apollo.io/#/people', {
      waitUntil: 'domcontentloaded',
      timeout: 30000,
    }).catch(() => {});
    await this._sleep(3000);

    const url = this.page.url();
    if (url.includes('/login') || url.includes('/signup')) {
      throw new Error('Apollo session expired. Set APOLLO_EMAIL + APOLLO_PASSWORD for fresh login.');
    }

    await this._extractCsrfToken();
    log.info('[Apollo] Browser ready (cookie mode)');
  }

  /**
   * Extract CSRF token from page cookies or meta tags.
   */
  async _extractCsrfToken() {
    const pageCookies = await this.page.cookies();
    const csrfCookie = pageCookies.find(c => c.name === 'X-CSRF-TOKEN');
    if (csrfCookie) {
      this.csrfToken = decodeURIComponent(csrfCookie.value);
      return;
    }
    const metaCsrf = await this.page.evaluate(() => {
      const meta = document.querySelector('meta[name="csrf-token"]');
      return meta ? meta.getAttribute('content') : null;
    });
    if (metaCsrf) this.csrfToken = metaCsrf;
  }

  /**
   * Close browser.
   */
  async close() {
    if (this.browser) {
      await this.browser.close().catch(() => {});
      this.browser = null;
      this.page = null;
    }
  }

  /**
   * Parse cookie string into Puppeteer cookie format.
   */
  _parseCookies(cookieStr) {
    if (!cookieStr) return [];
    return cookieStr.split(';').map(c => {
      const [name, ...valueParts] = c.trim().split('=');
      if (!name) return null;
      return {
        name: name.trim(),
        value: valueParts.join('=').trim(),
        domain: '.apollo.io',
        path: '/',
      };
    }).filter(Boolean);
  }

  /**
   * Search people via Apollo.
   */
  async searchPeople(filters = {}, page = 1, perPage = 25) {
    await this._enforceRateLimit();

    // Browser mode: Apollo caps at 25/page for internal API
    // API key mode: can request up to 100/page
    const effectivePerPage = this.mode === 'browser' ? 25 : Math.min(perPage, 100);

    const body = {
      ...filters,
      page,
      per_page: effectivePerPage,
    };

    let data;
    if (this.mode === 'browser') {
      data = await this._browserPost('/api/v1/mixed_people/search', body);
    } else {
      data = await this._httpPost('/api/v1/mixed_people/api_search', body);
    }

    this.stats.pages++;
    if (data.people) {
      this.stats.people += data.people.length;
    }

    const isEnriched = this.mode === 'browser';

    const pag = data.pagination || {};
    const totalEntries = pag.total_entries || data.total_entries || 0;
    const totalPages = pag.total_pages || (totalEntries ? Math.ceil(totalEntries / perPage) : 0);

    return {
      people: (data.people || []).map(p => this._mapPerson(p, isEnriched)),
      pagination: {
        page,
        per_page: perPage,
        total_entries: totalEntries,
        total_pages: totalPages,
      },
    };
  }

  /**
   * Async generator — search all pages, yielding leads one at a time.
   */
  async *searchAll(filters = {}, options = {}) {
    const maxPages = options.maxPages || 500;
    const maxResults = options.maxResults || 0;
    let page = 1;
    let totalYielded = 0;
    let consecutiveErrors = 0;

    while (page <= maxPages) {
      let result;
      try {
        result = await this.searchPeople(filters, page, 25);
        consecutiveErrors = 0;
      } catch (err) {
        consecutiveErrors++;
        if (err.message.includes('429') || err.message.includes('rate')) {
          this.stats.rateLimits++;
          log.warn(`[Apollo] Rate limited on page ${page} — waiting 60s...`);
          await this._sleep(60000);
          try {
            result = await this.searchPeople(filters, page, 25);
            consecutiveErrors = 0;
          } catch (retryErr) {
            log.error(`[Apollo] Retry failed: ${retryErr.message}`);
            if (consecutiveErrors >= 3) break;
            continue;
          }
        } else if (err.message.includes('Session expired')) {
          log.error(`[Apollo] ${err.message}`);
          break;
        } else {
          log.error(`[Apollo] Error on page ${page}: ${err.message}`);
          this.stats.errors++;
          if (consecutiveErrors >= 3) break;
          await this._sleep(2000);
          continue;
        }
      }

      if (!result || !result.people || result.people.length === 0) break;

      if (options.onPage) {
        options.onPage(page, result.pagination.total_pages, result.people.length, result.pagination.total_entries);
      }

      for (const person of result.people) {
        yield person;
        totalYielded++;
        if (maxResults > 0 && totalYielded >= maxResults) return;
      }

      if (page >= (result.pagination.total_pages || 1)) break;
      page++;

      // Small delay to avoid rate limits — 300ms is enough
      await this._sleep(300);
    }
  }

  /**
   * Make API call from within the browser context using XHR.
   * Uses visible Chrome with real session — Cloudflare trusts this.
   */
  async _browserPost(endpoint, body) {
    if (!this.page) throw new Error('Browser not initialized. Call apollo.init() first.');

    const result = await this.page.evaluate(async (endpoint, bodyStr) => {
      return new Promise((resolve) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', endpoint);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.withCredentials = true;
        xhr.timeout = 30000;
        xhr.onload = () => {
          const text = xhr.responseText;
          if (text.includes('cf-turnstile') || text.includes('challenges.cloudflare.com')) {
            resolve({ _error: true, status: xhr.status, message: 'Cloudflare challenge — session expired. Re-run: node scripts/apollo-login.js' });
            return;
          }
          if (xhr.status === 401) {
            resolve({ _error: true, status: 401, message: 'Session expired. Re-run: node scripts/apollo-login.js' });
            return;
          }
          try {
            resolve(JSON.parse(text));
          } catch (e) {
            resolve({ _error: true, status: xhr.status, message: 'JSON parse error: ' + text.slice(0, 200) });
          }
        };
        xhr.onerror = () => resolve({ _error: true, status: 0, message: 'Network error' });
        xhr.ontimeout = () => resolve({ _error: true, status: 0, message: 'Request timeout' });
        xhr.send(bodyStr);
      });
    }, endpoint, JSON.stringify(body));

    this.stats.requests++;

    if (result._error) {
      if (result.status === 429) throw new Error('Apollo API 429: Rate limited');
      log.error(`[Apollo] Browser error: ${result.message}`);
      throw new Error(`Apollo browser error: ${result.message}`);
    }

    // Debug: log what we got
    const peopleCount = (result.people || []).length;
    const total = result.total_entries || result.pagination?.total_entries || 0;
    log.info(`[Apollo] Response: ${peopleCount} people, ${total} total`);

    return result;
  }

  /**
   * HTTP POST for API key mode.
   */
  _httpPost(endpoint, body) {
    return new Promise((resolve, reject) => {
      const payload = JSON.stringify(body);
      const options = {
        hostname: 'api.apollo.io',
        path: endpoint,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache',
          'accept': 'application/json',
          'x-api-key': this.apiKey,
          'Content-Length': Buffer.byteLength(payload),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          this.stats.requests++;

          if (res.statusCode === 429) {
            return reject(new Error(`Apollo API 429: Rate limited`));
          }
          if (res.statusCode === 401 || res.statusCode === 403) {
            return reject(new Error(`Apollo API ${res.statusCode}: Invalid API key`));
          }
          if (res.statusCode !== 200) {
            let msg = data;
            try { msg = JSON.parse(data).message || data; } catch {}
            return reject(new Error(`Apollo API ${res.statusCode}: ${msg}`));
          }

          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(new Error(`Failed to parse Apollo response: ${err.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Apollo API timeout')); });
      req.write(payload);
      req.end();
    });
  }

  /**
   * Map Apollo person object to our lead format.
   */
  _mapPerson(p, isEnriched = false) {
    const org = p.organization || {};
    const contact = p.contact || {};

    const lead = {
      apollo_id: p.id || '',
      first_name: p.first_name || '',
      last_name: p.last_name || (p.name || '').replace(p.first_name || '', '').trim() || '',
      title: p.title || '',
      headline: p.headline || '',
      seniority: p.seniority || '',
      departments: Array.isArray(p.departments) ? p.departments.join(', ') : (p.departments || ''),
      linkedin_url: p.linkedin_url || '',
      photo_url: p.photo_url || '',
      city: p.city || '',
      state: p.state || '',
      country: p.country || '',
      email_status: p.email_status || '',

      firm_name: org.name || p.organization_name || '',
      website: org.website_url || '',
      domain: org.primary_domain || '',
      org_phone: org.sanitized_phone || org.primary_phone || org.phone || '',
      org_linkedin: org.linkedin_url || '',
      industry: org.industry || '',
      employee_count: org.estimated_num_employees || 0,
      founded_year: org.founded_year || '',
      keywords: Array.isArray(org.keywords) ? org.keywords.join(', ') : '',

      email: '',
      phone: '',
      personal_emails: [],
      phone_numbers: [],

      source: 'apollo',
      email_source: '',
      phone_source: '',
    };

    // Extract email — Apollo returns 'email_not_unlocked@domain.com' for locked emails on free tier
    if (p.email && !p.email.includes('not_unlocked')) {
      lead.email = p.email;
      lead.email_source = 'apollo';
    }

    // Extract phone numbers
    const phones = p.phone_numbers || (contact && contact.phone_numbers) || [];
    if (phones.length > 0) {
      lead.phone = phones[0].sanitized_number || phones[0].raw_number || '';
      lead.phone_source = 'apollo';
    } else if (org.sanitized_phone || org.primary_phone) {
      lead.phone = org.sanitized_phone || org.primary_phone || '';
      lead.phone_source = 'apollo_org';
    }

    const contactEmails = (contact && contact.contact_emails) || p.contact_emails || [];
    if (contactEmails.length > 0) {
      lead.personal_emails = contactEmails.map(e => e.email || e).filter(Boolean);
    }

    if (p.employment_history && p.employment_history.length > 0) {
      lead.employment_history = p.employment_history.map(e => ({
        company: e.organization_name || '',
        title: e.title || '',
        start: e.start_date || '',
        end: e.end_date || '',
        current: e.current || false,
      }));
    }

    return lead;
  }

  async _enforceRateLimit() {
    const now = Date.now();

    if (now - this.minuteStart > 60000) {
      this.requestsThisMinute = 0;
      this.minuteStart = now;
    }

    if (now - this.dayStart > 86400000) {
      this.requestsToday = 0;
      this.dayStart = now;
    }

    if (this.requestsToday >= this.maxPerDay) {
      throw new Error(`Apollo daily rate limit reached (${this.maxPerDay} requests). Wait 24 hours.`);
    }

    if (this.requestsThisMinute >= this.maxPerMinute) {
      const waitMs = 60000 - (now - this.minuteStart) + 1000;
      log.info(`[Apollo] Rate limit approaching — waiting ${(waitMs/1000).toFixed(1)}s...`);
      await this._sleep(waitMs);
      this.requestsThisMinute = 0;
      this.minuteStart = Date.now();
    }

    this.requestsThisMinute++;
    this.requestsToday++;
  }

  _sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }
}

module.exports = ApolloClient;
module.exports.SENIORITIES = SENIORITIES;
module.exports.EMPLOYEE_RANGES = EMPLOYEE_RANGES;
