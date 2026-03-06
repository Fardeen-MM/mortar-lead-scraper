/**
 * Google Ads Transparency Center Scraper (Puppeteer-based)
 *
 * Source: adstransparency.google.com — no API key needed
 * Data:   Advertiser name, ad count, website, verification status, regions, date range
 *
 * Discovers businesses actively spending on Google Ads in any niche.
 * Businesses running ads = spending money on marketing = good lead targets.
 *
 * options.niche = "personal injury lawyer", "dentist", etc. Defaults to "lawyer".
 * options.location = "United States" to filter by region.
 *
 * Uses puppeteer-extra with stealth, blocks images/fonts, scrolls infinite-scroll results,
 * clicks into advertiser detail pages for website/ad count. Rate limited 5-10s.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class GoogleAdsScraper extends BaseScraper {
  constructor() {
    super({
      name: 'google-ads',
      stateCode: 'GOOGLE-ADS',
      baseUrl: 'https://adstransparency.google.com',
      pageSize: 30,
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
      if (!puppeteerExtra._stealthRegisteredAds) {
        puppeteerExtra.use(StealthPlugin());
        puppeteerExtra._stealthRegisteredAds = true;
      }
      puppeteer = puppeteerExtra;
    } catch { puppeteer = require('puppeteer'); }
    const launchOpts = {
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
        '--disable-gpu', '--disable-web-security',
        '--disable-blink-features=AutomationControlled', '--window-size=1280,900'],
    };
    if (process.env.PUPPETEER_EXECUTABLE_PATH) launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    this._browser = await puppeteer.launch(launchOpts);
  }

  async _applyAntiDetection(page) {
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      Object.defineProperty(navigator, 'plugins', {
        get: () => [{ name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
          { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
          { name: 'Native Client', filename: 'internal-nacl-plugin' }],
      });
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
      const gp = WebGLRenderingContext.prototype.getParameter;
      WebGLRenderingContext.prototype.getParameter = function (p) {
        if (p === 37445) return 'Intel Inc.';
        if (p === 37446) return 'Intel Iris OpenGL Engine';
        return gp.call(this, p);
      };
    });
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  }

  async _closeBrowser() {
    if (this._browser) { await this._browser.close().catch(() => {}); this._browser = null; }
  }

  async _blockHeavyResources(page) {
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      try { if (['image', 'media', 'font'].includes(req.resourceType())) req.abort(); else req.continue(); }
      catch { /* already handled */ }
    });
  }

  async _dismissDialogs(page) {
    try {
      for (const sel of ['button[aria-label="Accept all"]', 'button[aria-label="Reject all"]',
        'button[aria-label="Accept"]', 'button[aria-label="I agree"]',
        'form[action*="consent"] button', '[class*="consent"] button']) {
        const btn = await page.$(sel);
        if (btn) {
          const vis = await page.evaluate(el => el.getBoundingClientRect().width > 0, btn);
          if (vis) { await btn.click(); await sleep(1000); break; }
        }
      }
    } catch { /* no dialog */ }
  }

  // ─── Main search ──────────────────────────────────────────────

  async *search(practiceArea, options = {}) {
    const niche = (options.niche || practiceArea || 'lawyer').trim();
    const location = (options.location || '').trim();
    const rateLimiter = new RateLimiter({ minDelay: 5000, maxDelay: 10000 });
    const isTestMode = !!(options.maxPages);
    const maxScrolls = isTestMode ? Math.min(options.maxPages, 3) : 8;
    const maxResults = options.maxResults || (isTestMode ? 10 : 50);

    log.info(`[Google Ads] Searching advertisers for "${niche}"${location ? ` in ${location}` : ''}`);

    try { await this._ensureBrowser(); } catch (err) {
      log.error(`[Google Ads] Browser launch failed: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Browser launch failed: ${err.message}` };
      return;
    }

    const seen = new Set();
    try {
      yield { _cityProgress: { current: 1, total: 1 } };
      await rateLimiter.wait();

      let advertiserList;
      try { advertiserList = await this._scrapeSearchResults(niche, location, maxScrolls); }
      catch (err) {
        log.error(`[Google Ads] Search failed: ${err.message}`);
        yield { _captcha: true, city: 'all', reason: err.message };
        return;
      }
      log.info(`[Google Ads] Found ${advertiserList.length} advertisers`);

      const toDetail = advertiserList.slice(0, maxResults);
      for (let i = 0; i < toDetail.length; i++) {
        const adv = toDetail[i];
        const key = adv.advertiserId || adv.name.toLowerCase().replace(/[^a-z0-9]/g, '');
        if (seen.has(key)) continue;
        seen.add(key);

        log.info(`[Google Ads] Detailing ${i + 1}/${toDetail.length}: ${adv.name}`);
        await rateLimiter.wait();

        let detail = {};
        if (adv.advertiserId) {
          try { detail = await this._scrapeAdvertiserDetail(adv.advertiserId); }
          catch (err) { log.warn(`[Google Ads] Detail failed for ${adv.name}: ${err.message}`); }
        }
        const lead = this._buildLead(adv, detail, niche);
        if (lead) yield lead;
      }
      log.info(`[Google Ads] Complete: ${seen.size} unique advertisers`);
    } finally { await this._closeBrowser(); }
  }

  // ─── Search results page ──────────────────────────────────────

  async _scrapeSearchResults(niche, location, maxScrolls) {
    const page = await this._browser.newPage();
    try {
      await this._applyAntiDetection(page);
      await this._blockHeavyResources(page);
      await page.setViewport({ width: 1280, height: 900 });

      const url = `${this.baseUrl}/?region=anywhere`;
      log.info(`[Google Ads] Navigating to ${url}`);
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
      await sleep(3000);

      const content = await page.content();
      if (content.includes('unusual traffic') || content.includes('CAPTCHA'))
        throw new Error('Blocked: unusual traffic / CAPTCHA detected');

      await this._dismissDialogs(page);
      await this._typeSearch(page, niche);
      await sleep(5000);
      if (location) { await this._applyLocationFilter(page, location); await sleep(2000); }
      await this._scrollForMore(page, maxScrolls);
      return await this._extractCards(page);
    } finally { await page.close().catch(() => {}); }
  }

  async _typeSearch(page, query) {
    const selectors = [
      'input[type="search"]', 'input[aria-label*="Search"]', 'input[aria-label*="search"]',
      'input[placeholder*="Search"]', 'input[placeholder*="search"]', 'input[placeholder*="advertiser"]',
      '[role="searchbox"]', '[role="combobox"]', 'input.mat-input-element',
      'input.mdc-text-field__input', 'input[type="text"]',
    ];
    for (const sel of selectors) {
      const input = await page.$(sel);
      if (!input) continue;
      const vis = await page.evaluate(el => {
        const r = el.getBoundingClientRect(); const s = window.getComputedStyle(el);
        return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
      }, input);
      if (!vis) continue;
      log.info(`[Google Ads] Found search input: ${sel}`);
      await input.click({ delay: 100 }); await sleep(500);
      await page.keyboard.down('Control'); await page.keyboard.press('a');
      await page.keyboard.up('Control'); await page.keyboard.press('Backspace');
      await sleep(300);
      await page.keyboard.type(query, { delay: 80 + Math.random() * 60 });
      await sleep(1500); await page.keyboard.press('Enter');
      return;
    }
    log.info('[Google Ads] No search input found, trying URL-based search');
    await page.goto(`${this.baseUrl}/?text=${encodeURIComponent(query)}`, { waitUntil: 'networkidle2', timeout: 45000 });
  }

  async _applyLocationFilter(page, location) {
    try {
      for (const sel of ['button[aria-label*="Region"]', 'button[aria-label*="region"]',
        'button[aria-label*="Country"]', 'button[aria-label*="Location"]',
        '[class*="region"] button', '[class*="filter"] button']) {
        const btn = await page.$(sel);
        if (!btn) continue;
        await btn.click(); await sleep(1000);
        const fi = await page.$('[class*="filter"] input, [role="listbox"] input');
        if (fi) {
          await fi.type(location, { delay: 50 }); await sleep(1000);
          const opt = await page.$('[role="option"], [class*="option"]');
          if (opt) { await opt.click(); await sleep(500); }
        }
        return;
      }
      log.info('[Google Ads] No location filter found');
    } catch (err) { log.warn(`[Google Ads] Location filter failed: ${err.message}`); }
  }

  async _scrollForMore(page, maxScrolls) {
    let stale = 0; const start = Date.now();
    for (let i = 0; i < maxScrolls; i++) {
      if (Date.now() - start > 60000) break;
      const prev = await page.evaluate(() =>
        document.querySelectorAll('a[href*="/advertiser/"], [data-advertiser-id]').length);
      await page.evaluate(() => {
        for (const c of document.querySelectorAll('[class*="results"], [class*="list"], [role="list"], main'))
          if (c.scrollHeight > c.clientHeight) { c.scrollTop = c.scrollHeight; return; }
        window.scrollTo(0, document.body.scrollHeight);
      });
      await sleep(3000);
      const curr = await page.evaluate(() =>
        document.querySelectorAll('a[href*="/advertiser/"], [data-advertiser-id]').length);
      log.info(`[Google Ads] Scroll ${i + 1}/${maxScrolls}: ${prev} -> ${curr} items`);
      if (curr <= prev) { stale++; if (stale >= 3) break; } else { stale = 0; }
    }
  }

  async _extractCards(page) {
    return page.evaluate((baseUrl) => {
      const results = [], seen = new Set();
      // Strategy 1: Links to /advertiser/ pages
      for (const link of document.querySelectorAll('a[href*="/advertiser/"]')) {
        const href = link.getAttribute('href') || '';
        const m = href.match(/\/advertiser\/([A-Za-z0-9_-]+)/);
        if (!m) continue;
        const id = m[1];
        if (seen.has(id)) continue;
        seen.add(id);
        const card = link.closest('[class*="card"], [class*="item"], [class*="row"], li') || link.parentElement;
        const name = link.textContent.trim() ||
          (card && card.querySelector('h2, h3, [class*="name"], [class*="title"]')?.textContent?.trim()) || '';
        if (!name || name.length < 2) continue;
        let adCount = 0, verified = false;
        if (card) {
          const t = card.textContent || '';
          const cm = t.match(/([\d,]+)\+?\s*ads?/i);
          if (cm) adCount = parseInt(cm[1].replace(/,/g, ''), 10);
          verified = !!card.querySelector('[class*="verified"], [class*="badge"]');
        }
        const url = href.startsWith('http') ? href : baseUrl + (href.startsWith('/') ? '' : '/') + href;
        results.push({ name: name.replace(/\s+/g, ' ').trim(), advertiserId: id, verified, adCount, url });
      }
      // Strategy 2: data-advertiser-id elements
      if (results.length === 0) {
        for (const el of document.querySelectorAll('[data-advertiser-id]')) {
          const id = el.getAttribute('data-advertiser-id') || '';
          if (seen.has(id)) continue; seen.add(id);
          const ne = el.querySelector('h2, h3, [class*="name"], a');
          const name = ne ? ne.textContent.trim() : el.textContent.trim().slice(0, 100);
          if (!name || name.length < 2) continue;
          results.push({ name, advertiserId: id, verified: false, adCount: 0, url: '' });
        }
      }
      return results;
    }, this.baseUrl);
  }

  // ─── Advertiser detail page ───────────────────────────────────

  async _scrapeAdvertiserDetail(advertiserId) {
    const page = await this._browser.newPage();
    try {
      await this._applyAntiDetection(page);
      await this._blockHeavyResources(page);
      await page.setViewport({ width: 1280, height: 900 });
      await page.goto(`${this.baseUrl}/advertiser/${advertiserId}`, { waitUntil: 'networkidle2', timeout: 45000 });
      await sleep(3000);
      return await page.evaluate(() => {
        const r = { adCount: 0, regions: '', dateRange: '', website: '', adFormats: '' };
        const t = document.body.innerText || '';
        const cm = t.match(/([\d,]+)\+?\s*ads?\b/i);
        if (cm) r.adCount = parseInt(cm[1].replace(/,/g, ''), 10);
        const dm = t.match(/(\w+\s+\d{1,2},?\s+\d{4})\s*[-–]\s*(\w+\s+\d{1,2},?\s+\d{4})/);
        if (dm) r.dateRange = dm[0];
        const fmts = [];
        if (/\btext\s*ads?\b/i.test(t)) fmts.push('text');
        if (/\bimage\s*ads?\b/i.test(t)) fmts.push('image');
        if (/\bvideo\s*ads?\b/i.test(t)) fmts.push('video');
        r.adFormats = fmts.join(', ');
        const regs = [];
        for (const el of document.querySelectorAll('[class*="region"], [class*="country"], [class*="chip"]')) {
          const txt = el.textContent.trim();
          if (txt && txt.length >= 2 && txt.length <= 50) regs.push(txt);
        }
        r.regions = regs.slice(0, 20).join(', ');
        for (const link of document.querySelectorAll('a[href]')) {
          const h = link.getAttribute('href') || '';
          if (!h.startsWith('http') || /google\.com|gstatic|youtube|facebook|twitter/i.test(h)) continue;
          const lb = (link.textContent + ' ' + (link.getAttribute('aria-label') || '')).toLowerCase();
          if (lb.includes('visit') || lb.includes('website') || /^https?:\/\/[^/]+\/?$/.test(h)) { r.website = h; break; }
        }
        return r;
      });
    } finally { await page.close().catch(() => {}); }
  }

  // ─── Lead builder ─────────────────────────────────────────────

  _buildLead(advertiser, detail, niche) {
    if (!advertiser || !advertiser.name) return null;
    const website = detail.website || '';
    let domain = '';
    if (website) {
      try { domain = new URL(website.startsWith('http') ? website : `https://${website}`).hostname.replace(/^www\./, ''); }
      catch { /* invalid */ }
    }
    const nicheTag = niche ? `_${niche.replace(/\s+/g, '_')}` : '';
    return this.transformResult({
      first_name: '', last_name: '',
      firm_name: advertiser.name,
      city: '', state: '',
      phone: '', email: '',
      website,
      bar_number: '', bar_status: '', admission_date: '',
      source: `google_ads_transparency${nicheTag}`,
      profile_url: advertiser.url || (advertiser.advertiserId
        ? `${this.baseUrl}/advertiser/${advertiser.advertiserId}` : ''),
      _ad_count: detail.adCount || advertiser.adCount || 0,
      _ad_formats: detail.adFormats || '',
      _ad_regions: detail.regions || '',
      _ad_date_range: detail.dateRange || '',
      _verification_status: advertiser.verified ? 'verified' : '',
      _niche: niche, _domain: domain,
    }, '');
  }

  // ─── BaseScraper stubs (unused — search() is overridden) ──────
  buildSearchUrl() { return ''; }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
}

module.exports = new GoogleAdsScraper();
