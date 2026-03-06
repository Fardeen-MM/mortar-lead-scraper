/**
 * LinkedIn Ad Library Scraper (Puppeteer-based)
 *
 * Source: linkedin.com/ad-library — public ad transparency tool
 * Method: Puppeteer + stealth plugin, scroll-to-load pagination
 * Data:   Advertiser name, ad count, ad copy, impressions, date range, LinkedIn URL, website
 *
 * options.niche = "personal injury lawyer", "dentist", etc. (default: "lawyer")
 * options.location = "United States", "New York", etc. (optional)
 * Env: LINKEDIN_COOKIES — JSON array or "li_at=xxx; JSESSIONID=yyy" string
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class LinkedInAdsScraper extends BaseScraper {
  constructor() {
    super({
      name: 'linkedin-ads',
      stateCode: 'LINKEDIN-ADS',
      baseUrl: 'https://www.linkedin.com/ad-library/',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: ['US'],
    });
    this._browser = null;
  }

  async _ensureBrowser() {
    if (this._browser) return;
    let puppeteer;
    try {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      if (!puppeteerExtra._stealthRegistered) {
        puppeteerExtra.use(StealthPlugin());
        puppeteerExtra._stealthRegistered = true;
      }
      puppeteer = puppeteerExtra;
    } catch { puppeteer = require('puppeteer'); }

    const args = [
      '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
      '--disable-gpu', '--disable-blink-features=AutomationControlled', '--window-size=1440,900',
    ];
    const launchOpts = { headless: 'new', args };
    if (process.env.PUPPETEER_EXECUTABLE_PATH) launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    this._browser = await puppeteer.launch(launchOpts);
  }

  async _setupPage(page) {
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      Object.defineProperty(navigator, 'plugins', {
        get: () => [{ name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
          { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
          { name: 'Native Client', filename: 'internal-nacl-plugin' }],
      });
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    });
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      try { ['image', 'media', 'font'].includes(req.resourceType()) ? req.abort() : req.continue(); }
      catch { /* already handled */ }
    });
    await page.setViewport({ width: 1440, height: 900 });
  }

  async _closeBrowser() {
    if (this._browser) { await this._browser.close().catch(() => {}); this._browser = null; }
  }

  async _injectCookies(page) {
    const raw = process.env.LINKEDIN_COOKIES;
    if (!raw) return false;
    try {
      let cookies;
      if (raw.trim().startsWith('[')) {
        cookies = JSON.parse(raw).map(c => ({
          name: c.name, value: c.value, domain: c.domain || '.linkedin.com',
          path: c.path || '/', httpOnly: true, secure: true,
        }));
      } else {
        cookies = raw.split(';').map(p => {
          const [name, ...rest] = p.trim().split('=');
          return { name: name.trim(), value: rest.join('=').trim(), domain: '.linkedin.com', path: '/', httpOnly: true, secure: true };
        }).filter(c => c.name && c.value);
      }
      if (cookies.length > 0) { await page.setCookie(...cookies); return true; }
    } catch (err) { log.warn(`[LinkedIn Ads] Cookie parse error: ${err.message}`); }
    return false;
  }

  async _isLoginWall(page) {
    return page.evaluate(() => {
      const url = window.location.href;
      if (url.includes('/login') || url.includes('/authwall') || url.includes('/checkpoint')) return true;
      if (document.querySelector('.join-form, .login-form, [data-test-id="authwall"], .auth-wall, .contextual-sign-in')) return true;
      const h1 = document.querySelector('h1');
      if (h1 && /sign in|join linkedin|log in/i.test(h1.textContent)) return true;
      return false;
    });
  }

  async _scrollForMore(page, maxScrolls) {
    let done = 0, prevH = 0, stable = 0;
    for (let i = 0; i < maxScrolls; i++) {
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await sleep(2500 + Math.random() * 2000);
      const h = await page.evaluate(() => document.body.scrollHeight);
      done++;
      if (h === prevH) { if (++stable >= 3) break; } else { stable = 0; }
      prevH = h;
      await sleep(1000 + Math.random() * 1500);
    }
    return done;
  }

  async _extractAdCards(page) {
    return page.evaluate(() => {
      const results = [];
      const selectors = [
        '[data-test-id="ad-library-ad-card"]', '.ad-library-ad-card', '.search-results__ad-card',
        '.ad-card', 'li[class*="ad-library"]', 'div[class*="ad-library-search"] li',
        '.artdeco-card', '.artdeco-list__item',
      ];
      let cards = [];
      for (const s of selectors) { cards = document.querySelectorAll(s); if (cards.length) break; }

      // Fallback: walk up from company links
      if (!cards.length) {
        const seen = new Set();
        for (const link of document.querySelectorAll('a[href*="/company/"], a[href*="/in/"]')) {
          let el = link.parentElement;
          for (let i = 0; i < 5 && el; i++, el = el.parentElement) {
            if (el.children.length >= 2 && el.offsetHeight > 50) {
              const k = el.innerHTML.substring(0, 100);
              if (!seen.has(k)) { seen.add(k); cards = [...cards, el]; }
              break;
            }
          }
        }
      }

      for (const card of cards) {
        const text = card.textContent || '';
        if (text.trim().length < 10) continue;

        let advertiser = '', linkedinUrl = '';
        const cl = card.querySelector('a[href*="/company/"]');
        if (cl) {
          advertiser = (cl.textContent || '').trim();
          linkedinUrl = cl.getAttribute('href') || '';
          if (linkedinUrl && !linkedinUrl.startsWith('http')) linkedinUrl = 'https://www.linkedin.com' + linkedinUrl;
          const m = linkedinUrl.match(/(https:\/\/www\.linkedin\.com\/company\/[^/?]+)/);
          if (m) linkedinUrl = m[1];
        }
        if (!advertiser) {
          const pl = card.querySelector('a[href*="/in/"]');
          if (pl) { advertiser = (pl.textContent || '').trim(); linkedinUrl = (pl.getAttribute('href') || ''); if (linkedinUrl && !linkedinUrl.startsWith('http')) linkedinUrl = 'https://www.linkedin.com' + linkedinUrl; }
        }
        if (!advertiser) { const h = card.querySelector('h3, h4, h2, strong'); if (h) advertiser = (h.textContent || '').trim(); }
        if (!advertiser || advertiser.length < 2 || advertiser.length > 150) continue;

        let adCount = 0;
        const acm = text.match(/(\d+)\s*(?:active\s*)?ads?\b/i);
        if (acm) adCount = parseInt(acm[1], 10);

        let adCopy = '';
        const ce = card.querySelector('[class*="ad-content"], [class*="ad-copy"], [class*="creative"], [class*="headline"], p');
        if (ce) { const t = (ce.textContent || '').trim(); if (t.length > 10 && t.length < 500 && t !== advertiser) adCopy = t.substring(0, 300); }

        let impressions = '';
        const im = text.match(/([\d.,]+[KMB]?\s*[-\u2013]\s*[\d.,]+[KMB]?)\s*impressions/i) || text.match(/(<?\s*[\d.,]+[KMB]?)\s*impressions/i);
        if (im) impressions = im[1].trim();

        let dateRange = '';
        const dm = text.match(/((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,4}\s*[-\u2013]\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,4})/i);
        if (dm) dateRange = dm[1].trim();
        if (!dateRange) { const sm = text.match(/(?:running since|started|active since)\s*:?\s*(.+?)(?:\n|$)/i); if (sm) dateRange = sm[1].trim().substring(0, 50); }

        let website = '';
        for (const a of card.querySelectorAll('a[href]')) {
          const h = a.getAttribute('href') || '';
          if (h && !h.includes('linkedin.com') && h.startsWith('http')) { website = h; break; }
        }

        results.push({ advertiser, linkedinUrl, adCount, adCopy, impressions, dateRange, website });
      }
      return results;
    });
  }

  _buildLead(card, niche, location) {
    if (!card || !card.advertiser) return null;
    let domain = '';
    if (card.website) { try { domain = new URL(card.website).hostname.replace(/^www\./, ''); } catch { /* */ } }
    const parts = (location || '').split(',').map(s => s.trim());

    return this.transformResult({
      first_name: '', last_name: '', firm_name: card.advertiser,
      city: parts[0] || '', state: parts[1] || '',
      phone: '', email: '', website: card.website || '',
      bar_number: '', bar_status: '', admission_date: '',
      source: 'linkedin_ads', profile_url: card.linkedinUrl || '',
      domain, linkedin_url: card.linkedinUrl || '', niche,
      ad_count: card.adCount || 0,
      _ad_copy: (card.adCopy || '').substring(0, 200),
      _impressions: card.impressions || '', _date_range: card.dateRange || '',
    }, '');
  }

  // ─── Main Search Generator ────────────────────────────────────

  async *search(practiceArea, options = {}) {
    const niche = (options.niche || practiceArea || 'lawyer').trim();
    const location = (options.location || '').trim();
    const isTest = !!(options.maxPages && options.maxPages <= 3);
    const maxScrolls = isTest ? 3 : (options.maxPages || 10);

    log.info(`[LinkedIn Ads] niche="${niche}", location="${location || 'any'}", test=${isTest}`);
    yield { _cityProgress: { current: 1, total: 1 } };

    try { await this._ensureBrowser(); } catch (err) {
      log.error(`[LinkedIn Ads] Browser launch failed: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Browser launch failed: ${err.message}` };
      return;
    }

    const seen = new Set();
    try {
      const page = await this._browser.newPage();
      try {
        await this._setupPage(page);
        const hasCookies = await this._injectCookies(page);

        const params = new URLSearchParams({ q: niche });
        if (location) params.set('location', location);
        const url = `https://www.linkedin.com/ad-library/search?${params}`;
        log.info(`[LinkedIn Ads] ${url}`);

        await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
        await sleep(3000 + Math.random() * 2000);

        if (await this._isLoginWall(page)) {
          const reason = hasCookies
            ? 'LinkedIn session expired. Refresh LINKEDIN_COOKIES.'
            : 'LinkedIn login required. Set LINKEDIN_COOKIES env var.';
          log.warn(`[LinkedIn Ads] ${reason}`);
          yield { _captcha: true, reason };
          for (const c of await this._extractAdCards(page)) { const l = this._buildLead(c, niche, location); if (l) yield l; }
          return;
        }

        // Wait for results
        let found = false;
        for (const sel of ['[data-test-id="ad-library-ad-card"]', '.ad-library-ad-card', '.artdeco-card', 'a[href*="/company/"]']) {
          try { await page.waitForSelector(sel, { timeout: 8000 }); found = true; break; } catch { /* next */ }
        }
        if (!found) {
          const empty = await page.evaluate(() => /no (results|ads) found|0 results/i.test(document.body.textContent || ''));
          if (empty) { log.info(`[LinkedIn Ads] No results for "${niche}"`); return; }
          log.warn('[LinkedIn Ads] No recognized selectors, extracting anyway');
        }

        const scrollsDone = await this._scrollForMore(page, maxScrolls);
        log.info(`[LinkedIn Ads] ${scrollsDone} scrolls`);

        const cards = await this._extractAdCards(page);
        let total = 0;
        for (const card of cards) {
          const key = (card.advertiser || '').toLowerCase().replace(/[^a-z0-9]/g, '');
          if (!key || seen.has(key)) continue;
          seen.add(key);
          const lead = this._buildLead(card, niche, location);
          if (lead) { yield lead; total++; }
        }
        log.success(`[LinkedIn Ads] ${total} advertisers for "${niche}"`);
      } finally { await page.close().catch(() => {}); }
    } finally { await this._closeBrowser(); }
  }
}

module.exports = new LinkedInAdsScraper();
