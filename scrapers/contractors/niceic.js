/**
 * NICEIC UK — Registered Electricians via Electrical Competent Person Scheme
 *
 * Source: https://www.electricalcompetentperson.co.uk (aggregates NICEIC, NAPIT, ELECSA, etc.)
 * Also: https://www.niceic.com/find-a-tradesperson/
 * Records: ~40,000+ registered electricians across the UK
 *
 * The Electrical Competent Person Scheme Operators (ECPSO) register includes:
 *   - NICEIC (National Inspection Council for Electrical Installation Contracting)
 *   - NAPIT (National Association of Professional Inspectors and Testers)
 *   - ELECSA
 *   - Other Part P scheme operators
 *
 * API: GET https://www.electricalcompetentperson.co.uk/api/directory/search
 * Params: location, electricianType (Installer/Inspector), captchaToken,
 *         searchSessionId, pageNumber, pageSize, name
 *
 * Auth: Cloudflare Turnstile (invisible, sitekey: 0x4AAAAAAB9_a6vNRD7VWNZq)
 * First request requires captchaToken; subsequent pages use searchSessionId.
 *
 * Response: { results: [{ companyName, email, postcode, telephone, website,
 *   schemeName, distance, address, ... }], totalCount, searchSessionId, pageSize }
 *
 * Usage modes:
 *   1. NICEIC_SESSION_ID env var — skip captcha, use existing session
 *   2. Puppeteer + stealth — attempt auto-solve (may be blocked by Turnstile)
 *   3. Run scripts/niceic-get-session.js — manual captcha solve, exports session ID
 *
 * Coverage: 120+ UK postcode areas for nationwide scraping.
 */

const BaseScraper = require('../base-scraper');
const https = require('https');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_URL = 'https://www.electricalcompetentperson.co.uk/api/directory/search';
const SEARCH_URL = 'https://www.electricalcompetentperson.co.uk/search';
const PAGE_SIZE = 50;

// UK postcode areas — comprehensive coverage of all regions
const UK_POSTCODE_AREAS = [
  // London
  'EC1A 1BB', 'WC1A 1AA', 'E1 6AN', 'N1 0AA', 'NW1 0AA',
  'SE1 0AA', 'SW1A 1AA', 'W1A 0AA',
  // South East
  'BN1 1AA', 'CT1 1AA', 'GU1 1AA', 'ME1 1AA', 'MK1 1AA',
  'OX1 1AA', 'RG1 1AA', 'RH1 1AA', 'SL1 1AA', 'SO14 1AA',
  'TN1 1AA', 'PO1 1AA', 'HP1 1AA',
  // South West
  'BA1 1AA', 'BH1 1AA', 'BS1 1AA', 'DT1 1AA', 'EX1 1AA',
  'GL1 1AA', 'PL1 1AA', 'SN1 1AA', 'SP1 1AA', 'TA1 1AA',
  'TQ1 1AA', 'TR1 1AA',
  // West Midlands
  'B1 1AA', 'CV1 1AA', 'DY1 1AA', 'HR1 1AA', 'ST1 1AA',
  'TF1 1AA', 'WR1 1AA', 'WS1 1AA', 'WV1 1AA',
  // East Midlands
  'DE1 1AA', 'LE1 1AA', 'LN1 1AA', 'NG1 1AA', 'NN1 1AA',
  'PE1 1AA',
  // East of England
  'CB1 1AA', 'CM1 1AA', 'CO1 1AA', 'IP1 1AA', 'LU1 1AA',
  'NR1 1AA', 'SG1 1AA', 'SS1 1AA',
  // Yorkshire & Humber
  'BD1 1AA', 'DN1 1AA', 'HD1 1AA', 'HG1 1AA', 'HU1 1AA',
  'HX1 1AA', 'LS1 1AA', 'S1 1AA', 'WF1 1AA', 'YO1 1AA',
  // North West
  'BB1 1AA', 'BL1 1AA', 'CA1 1AA', 'CH1 1AA', 'CW1 1AA',
  'FY1 1AA', 'L1 1AA', 'LA1 1AA', 'M1 1AA', 'OL1 1AA',
  'PR1 1AA', 'SK1 1AA', 'WA1 1AA', 'WN1 1AA',
  // North East
  'DH1 1AA', 'DL1 1AA', 'NE1 1AA', 'SR1 1AA', 'TS1 1AA',
  // Wales
  'CF1 1AA', 'LD1 1AA', 'LL1 1AA', 'NP1 1AA', 'SA1 1AA', 'SY1 1AA',
  // Scotland
  'AB1 1AA', 'DD1 1AA', 'DG1 1AA', 'EH1 1AA', 'FK1 1AA',
  'G1 1AA', 'HS1 1AA', 'IV1 1AA', 'KA1 1AA', 'KW1 1AA',
  'KY1 1AA', 'ML1 1AA', 'PA1 1AA', 'PH1 1AA', 'TD1 1AA',
  // Northern Ireland
  'BT1 1AA', 'BT28 1AA', 'BT48 1AA',
];

// Default search postcodes — major cities for quick test runs
const DEFAULT_POSTCODES = [
  'SW1A 1AA', 'M1 1AA', 'B1 1AA', 'LS1 1AA', 'G1 1AA',
  'L1 1AA', 'EH1 1AA', 'BS1 1AA', 'S1 1AA', 'NE1 1AA',
];

class NiceicScraper extends BaseScraper {
  constructor() {
    super({
      name: 'niceic_electricians',
      stateCode: 'NICEIC',
      baseUrl: 'https://www.electricalcompetentperson.co.uk',
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'installer': 'Installer',
        'inspector': 'Inspector',
        'electrical': 'Installer',
        'electrician': 'Installer',
        'all': 'Installer',
      },
      defaultCities: DEFAULT_POSTCODES,
    });
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('niceic: buildSearchUrl() not used'); }
  parseResultsPage() { throw new Error('niceic: parseResultsPage() not used'); }
  extractResultCount() { throw new Error('niceic: extractResultCount() not used'); }

  /**
   * HTTPS GET against the directory search API.
   */
  _apiGet(params) {
    return new Promise((resolve, reject) => {
      const url = new URL(API_URL);
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined && v !== null && v !== '') {
          url.searchParams.set(k, String(v));
        }
      }

      https.get(url.toString(), {
        headers: {
          'Accept': 'application/json, text/plain, */*',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Referer': SEARCH_URL,
          'Accept-Language': 'en-GB,en;q=0.9',
        },
        timeout: 30000,
      }, res => {
        let body = '';
        res.on('data', d => { body += d; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      }).on('error', reject)
        .on('timeout', function() { this.destroy(); reject(new Error('API request timed out')); });
    });
  }

  /**
   * Format a UK phone number.
   */
  _formatPhone(phone) {
    if (!phone) return '';
    let digits = phone.replace(/[^\d+]/g, '');
    if (digits.startsWith('+44')) digits = '0' + digits.slice(3);
    if (digits.startsWith('44') && digits.length > 10) digits = '0' + digits.slice(2);
    if (digits.length >= 10 && digits.length <= 12) return digits;
    return phone.trim();
  }

  /**
   * Map an API result to a normalized lead object.
   */
  _mapResult(item, practiceArea) {
    const companyName = (item.companyName || '').trim();
    const phone = this._formatPhone(item.telephone || item.phone || '');
    const email = (item.email || '').trim().toLowerCase();
    let website = (item.website || '').trim();
    const postcode = (item.postcode || '').trim().toUpperCase();
    const address = (item.address || '').trim();
    const schemeName = (item.schemeName || item.scheme || '').trim();
    const distance = item.distance !== undefined ? String(item.distance) : '';
    const regNumber = (item.registrationNumber || item.regNumber || '').trim();

    // Extract city from address
    let city = '';
    if (address) {
      const parts = address.split(',').map(p => p.trim());
      for (let i = parts.length - 1; i >= 0; i--) {
        const part = parts[i];
        if (part && !/^[A-Z]{1,2}\d/.test(part) && part.length > 1) {
          city = part;
          break;
        }
      }
    }

    // Normalize website
    if (website && !website.startsWith('http://') && !website.startsWith('https://')) {
      website = 'https://' + website;
    }
    if (website && this.isExcludedDomain(website)) website = '';

    // Filter system emails
    const cleanEmail = (email.includes('niceic') || email.includes('elecsa') ||
                        email.includes('napit') || email.includes('ecpso') ||
                        email.includes('example.com'))
      ? '' : email;

    return {
      first_name: '',
      last_name: '',
      firm_name: companyName,
      city,
      state: 'UK',
      zip: postcode,
      phone,
      email: cleanEmail,
      website,
      bar_number: regNumber,
      bar_status: schemeName || 'NICEIC Registered',
      source: 'niceic_electricians',
      practice_area: practiceArea || 'Electrician',
      profile_url: '',
      address,
      distance,
      all_trades: schemeName,
    };
  }

  /**
   * Acquire a searchSessionId via Puppeteer (attempts Turnstile solve).
   * Returns sessionId string or null.
   */
  async _acquireSessionViaPuppeteer(postcode, electricianType) {
    let browser, page;
    try {
      const puppeteerMod = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteerMod.use(StealthPlugin());

      const launchOptions = {
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
      };
      if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
      }

      browser = await puppeteerMod.launch(launchOptions);
      page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

      // Block heavy resources
      await page.setRequestInterception(true);
      page.on('request', req => {
        if (['image', 'font', 'media'].includes(req.resourceType())) req.abort();
        else req.continue();
      });

      // Listen for API response
      let gotResponse = false;
      const result = await new Promise(async (resolve) => {
        const timer = setTimeout(() => { if (!gotResponse) resolve(null); }, 40000);

        page.on('response', async (res) => {
          if (gotResponse) return;
          if (res.url().includes('/api/directory/search') && res.status() === 200) {
            try {
              const body = await res.text();
              const data = JSON.parse(body);
              if (data.searchSessionId) {
                gotResponse = true;
                clearTimeout(timer);
                resolve(data);
              }
            } catch (e) { /* ignore */ }
          }
        });

        // Navigate and trigger search
        const searchUrl = `${SEARCH_URL}?postcode=${encodeURIComponent(postcode)}&electricianType=${encodeURIComponent(electricianType)}`;
        await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
        await new Promise(r => setTimeout(r, 3000));
        await page.evaluate(() => {
          const btns = Array.from(document.querySelectorAll('button')).filter(b => b.textContent.trim() === 'Search');
          if (btns.length) btns[0].click();
        });
        // Wait for potential Turnstile solve (30s)
        await new Promise(r => setTimeout(r, 30000));
        if (!gotResponse) { clearTimeout(timer); resolve(null); }
      });

      return result;
    } catch (err) {
      log.warn(`[NICEIC] Puppeteer session acquisition failed: ${err.message}`);
      return null;
    } finally {
      if (browser) await browser.close().catch(() => {});
    }
  }

  /**
   * Search a single postcode using a valid searchSessionId.
   * Paginates through all results.
   * Returns array of lead objects.
   */
  async _searchPostcode(sessionId, postcode, electricianType, rateLimiter, maxPages) {
    const leads = [];
    let pageNum = 1;
    let totalCount = null;

    while (pageNum <= maxPages) {
      await rateLimiter.wait();

      try {
        const resp = await this._apiGet({
          location: postcode,
          electricianType,
          searchSessionId: sessionId,
          pageNumber: pageNum,
          pageSize: PAGE_SIZE,
        });

        if (resp.statusCode === 403) {
          log.warn(`[NICEIC] Session expired (403) at ${postcode} page ${pageNum}`);
          return { leads, sessionExpired: true };
        }

        if (resp.statusCode !== 200) {
          log.warn(`[NICEIC] API returned ${resp.statusCode} for ${postcode}`);
          break;
        }

        const data = JSON.parse(resp.body);

        if (data.error) {
          log.warn(`[NICEIC] API error: ${data.error}`);
          if (data.error.includes('captcha')) return { leads, sessionExpired: true };
          break;
        }

        // Update session if returned
        if (data.searchSessionId) {
          sessionId = data.searchSessionId;
        }

        if (!data.results || data.results.length === 0) {
          if (pageNum === 1) log.info(`[NICEIC] No results for ${postcode}`);
          break;
        }

        if (pageNum === 1) {
          totalCount = data.totalCount || data.results.length;
          log.success(`[NICEIC] Found ${totalCount} results near ${postcode}`);
        }

        leads.push(...data.results);
        log.info(`[NICEIC] Page ${pageNum}: ${data.results.length} results (${leads.length} total)`);

        // Check if we've fetched all
        if (totalCount && leads.length >= totalCount) break;
        if (data.results.length < PAGE_SIZE) break;

        pageNum++;
      } catch (err) {
        log.warn(`[NICEIC] Error fetching ${postcode} page ${pageNum}: ${err.message}`);
        break;
      }
    }

    return { leads, sessionExpired: false, newSessionId: sessionId };
  }

  /**
   * Main search generator.
   *
   * Strategy:
   * 1. Check for NICEIC_SESSION_ID env var (pre-acquired session)
   * 2. If not set, try Puppeteer to acquire a session
   * 3. If captcha blocks, yield _captcha signal with instructions
   * 4. For each postcode, use the API with searchSessionId (no captcha)
   * 5. Dedup by firm name across postcodes
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    const practiceKey = (practiceArea || '').toLowerCase().trim();
    const electricianType = practiceKey === 'inspector' ? 'Inspector' : 'Installer';

    const postcodes = options.city
      ? [options.city]
      : (options.extended ? UK_POSTCODE_AREAS : DEFAULT_POSTCODES);
    const postcodesToSearch = options.maxCities
      ? postcodes.slice(0, options.maxCities)
      : postcodes;

    log.scrape(`[NICEIC] Starting search: ${postcodesToSearch.length} postcodes, type=${electricianType}`);

    // Try to get a session ID
    let sessionId = process.env.NICEIC_SESSION_ID || '';

    if (!sessionId) {
      log.info('[NICEIC] No NICEIC_SESSION_ID env var — attempting Puppeteer session acquisition...');
      const firstPostcode = postcodesToSearch[0] || 'SW1A 1AA';
      const result = await this._acquireSessionViaPuppeteer(firstPostcode, electricianType);

      if (result && result.searchSessionId) {
        sessionId = result.searchSessionId;
        log.success(`[NICEIC] Acquired session via Puppeteer: ${sessionId.substring(0, 20)}...`);
      } else {
        log.warn('[NICEIC] Puppeteer could not solve Turnstile captcha');
        log.info('[NICEIC] To use this scraper, acquire a session ID:');
        log.info('[NICEIC]   1. Visit https://www.electricalcompetentperson.co.uk/search');
        log.info('[NICEIC]   2. Search for any postcode (e.g. SW1A 1AA)');
        log.info('[NICEIC]   3. Open browser DevTools > Network tab');
        log.info('[NICEIC]   4. Find the /api/directory/search request');
        log.info('[NICEIC]   5. Copy the searchSessionId from the response JSON');
        log.info('[NICEIC]   6. Set NICEIC_SESSION_ID env var and re-run');
        yield {
          _captcha: true,
          city: 'all',
          reason: 'Cloudflare Turnstile — set NICEIC_SESSION_ID env var (see logs for instructions)',
        };
        return;
      }
    }

    // Validate session with a test request
    log.info('[NICEIC] Validating session...');
    const testResp = await this._apiGet({
      location: postcodesToSearch[0],
      electricianType,
      searchSessionId: sessionId,
      pageNumber: 1,
      pageSize: 1,
    }).catch(() => null);

    if (!testResp || testResp.statusCode === 403) {
      log.error('[NICEIC] Session ID is invalid or expired');
      yield {
        _captcha: true,
        city: 'all',
        reason: 'NICEIC_SESSION_ID is invalid or expired — get a fresh one',
      };
      return;
    }

    try {
      const testData = JSON.parse(testResp.body);
      if (testData.error && testData.error.includes('captcha')) {
        log.error('[NICEIC] Session requires captcha — expired');
        yield {
          _captcha: true,
          city: 'all',
          reason: 'Session expired — acquire a new NICEIC_SESSION_ID',
        };
        return;
      }
      // Update session if returned
      if (testData.searchSessionId) sessionId = testData.searchSessionId;
      log.success('[NICEIC] Session is valid');
    } catch (e) {
      log.warn('[NICEIC] Could not parse test response, proceeding anyway...');
    }

    // Track dedup
    const seenFirms = new Set();
    let totalYielded = 0;

    for (let ci = 0; ci < postcodesToSearch.length; ci++) {
      const postcode = postcodesToSearch[ci];
      yield { _cityProgress: { current: ci + 1, total: postcodesToSearch.length } };
      log.scrape(`[NICEIC] Searching postcode ${ci + 1}/${postcodesToSearch.length}: ${postcode}`);

      const result = await this._searchPostcode(sessionId, postcode, electricianType, rateLimiter, maxPages);

      if (result.sessionExpired) {
        log.error('[NICEIC] Session expired mid-scrape');
        yield { _captcha: true, city: postcode, reason: 'Session expired — re-acquire NICEIC_SESSION_ID' };
        break;
      }

      // Update session ID if rotated
      if (result.newSessionId) sessionId = result.newSessionId;

      // Yield deduplicated results
      for (const item of result.leads) {
        const lead = this._mapResult(item, practiceArea);
        if (!lead.firm_name) continue;

        const key = lead.firm_name.toLowerCase().replace(/\s+/g, ' ').trim();
        if (seenFirms.has(key)) continue;
        seenFirms.add(key);

        yield this.transformResult(lead, practiceArea);
        totalYielded++;
      }

      log.info(`[NICEIC] Running total: ${totalYielded} unique leads`);

      // Polite delay
      if (ci < postcodesToSearch.length - 1) {
        await new Promise(r => setTimeout(r, 2000 + Math.random() * 3000));
      }
    }

    log.success(`[NICEIC] Scrape complete — ${totalYielded} unique electricians from ${seenFirms.size} firms`);
  }

  transformResult(lead, practiceArea) {
    lead.source = 'niceic_electricians';
    if (practiceArea && !lead.practice_area) lead.practice_area = practiceArea;
    return lead;
  }
}

module.exports = new NiceicScraper();
