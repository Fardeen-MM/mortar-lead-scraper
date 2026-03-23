/**
 * CICC Public Register — Puppeteer Search + Profile Scraper
 *
 * Source: https://register.college-ic.ca/Public-Register-EN/RCIC_Search.aspx
 * Records: ~17,251 registered immigration consultants across Canada
 * Method: Puppeteer (stealth) — ASP.NET + Telerik RadGrid + iMIS CMS
 *
 * Strategy:
 *   1. Navigate to search page, submit empty search to get all results
 *   2. Set page size to 50 (max) to reduce pages from 583 to ~233
 *   3. For each page of results, extract rows from the RadGrid
 *   4. Each row has: College ID, Name, Company, Type, Entitled to Practise
 *   5. Extract hidden profile link IDs from each row
 *   6. Navigate to each profile page
 *   7. Click "Licensee Details" tab (AJAX lazy-load) to reveal Employment table
 *   8. Extract from Employment table: email, phone, company, city, province, country
 *   9. Emails are Cloudflare-obfuscated (data-cfemail XOR encoding)
 *
 * Profile URL: https://register.college-ic.ca/Public-Register-EN/Licensee/Profile.aspx?ID={id}
 * Employment table columns: Company | Start Date | Country | Province/State | City | Email | Phone
 *
 * ~80% of profiles have email + phone on the Licensee Details tab.
 * No CAPTCHA, no anti-bot protection detected.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const SEARCH_URL = 'https://register.college-ic.ca/Public-Register-EN/RCIC_Search.aspx';
const PROFILE_SHORT = 'https://register.college-ic.ca/Pr_Profile_RCIC';
const PROFILE_BASE = 'https://register.college-ic.ca/Public-Register-EN/Licensee/Profile.aspx';

class CICCScraper extends BaseScraper {
  constructor() {
    super({
      name: 'cicc',
      stateCode: 'CICC',
      baseUrl: 'https://register.college-ic.ca',
      pageSize: 50,
      practiceAreaCodes: {
        'all': '',
        'rcic': 'RCIC',
        'risia': 'RISIA',
        'immigration': '',
      },
      defaultCities: [],
    });
    this.browser = null;
  }

  // search() is fully overridden
  buildSearchUrl() { throw new Error('cicc: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('cicc: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('cicc: extractResultCount() not used — search() overridden'); }

  transformResult(lead, practiceArea) {
    lead.source = 'cicc_register';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Launch Puppeteer with stealth plugin.
   */
  async _initBrowser() {
    if (this.browser) return;
    const puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());

    const launchOptions = {
      headless: true,
      protocolTimeout: 90000,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    };

    // Railway: use system Chromium
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this.browser = await puppeteer.launch(launchOptions);
    log.info('[CICC] Browser launched with stealth plugin');
  }

  /**
   * Close the browser.
   */
  async _closeBrowser() {
    if (this.browser) {
      try { await this.browser.close(); } catch (_) { /* ignore */ }
      this.browser = null;
    }
  }

  /**
   * Decode Cloudflare email obfuscation (XOR cipher).
   * First 2 hex chars = key, remaining pairs = XOR'd email chars.
   */
  _decodeCfEmail(encoded) {
    if (!encoded || encoded.length < 4) return null;
    try {
      const key = parseInt(encoded.substring(0, 2), 16);
      let email = '';
      for (let i = 2; i < encoded.length; i += 2) {
        const charCode = parseInt(encoded.substring(i, i + 2), 16) ^ key;
        email += String.fromCharCode(charCode);
      }
      return email.toLowerCase().trim();
    } catch {
      return null;
    }
  }

  /**
   * Navigate to the search page, submit an empty search to get all results,
   * then set page size to 50. Returns the Puppeteer page object positioned
   * at page 1 of results.
   */
  async _initSearchPage() {
    const page = await this.browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

    // Set longer default timeout for ASP.NET pages
    page.setDefaultNavigationTimeout(60000);
    page.setDefaultTimeout(30000);

    log.info('[CICC] Navigating to search page...');
    await page.goto(SEARCH_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await new Promise(r => setTimeout(r, 2000));

    // Click the Submit/Search button (empty search = all results)
    log.info('[CICC] Submitting empty search to get all registrants...');
    const submitBtnSelector = 'input[id*="SubmitButton"], input[value="Submit"], input[type="submit"][id*="Sheet0"]';
    await page.waitForSelector(submitBtnSelector, { timeout: 15000 });
    await page.click(submitBtnSelector);

    // Wait for the RadGrid to load with results
    await page.waitForSelector('table[id*="ResultsGrid_Grid1"] tr.rgRow, table[id*="ResultsGrid_Grid1"] tr.rgAltRow', { timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    // Try to set page size to 50 (max)
    try {
      await this._setPageSize(page, 50);
    } catch (err) {
      log.warn(`[CICC] Could not set page size to 50: ${err.message} — using default`);
    }

    return page;
  }

  /**
   * Set the RadGrid page size by interacting with the page size dropdown.
   */
  async _setPageSize(page, size) {
    // RadGrid page size is typically a dropdown or text input in the pager row
    // Look for the page size dropdown/input
    const pageSizeSelector = 'select[id*="PageSizeComboBox"], input[id*="PageSizeComboBox"], div[id*="PageSizeComboBox"]';
    const hasPagingControl = await page.$(pageSizeSelector);

    if (hasPagingControl) {
      // Try select element first
      const isSelect = await page.$('select[id*="PageSizeComboBox"]');
      if (isSelect) {
        await page.select('select[id*="PageSizeComboBox"]', String(size));
        await page.waitForSelector('table[id*="ResultsGrid_Grid1"] tr.rgRow', { timeout: 30000 });
        await new Promise(r => setTimeout(r, 2000));
        log.info(`[CICC] Page size set to ${size} via select`);
        return;
      }

      // Try RadComboBox (Telerik dropdown) — click to open, then select value
      const combo = await page.$('div[id*="PageSizeComboBox"]');
      if (combo) {
        await combo.click();
        await new Promise(r => setTimeout(r, 500));
        // Look for the dropdown items
        const itemSelector = `li[class*="rcbItem"]`;
        const items = await page.$$(itemSelector);
        for (const item of items) {
          const text = await page.evaluate(el => el.textContent.trim(), item);
          if (text === String(size)) {
            await item.click();
            await page.waitForSelector('table[id*="ResultsGrid_Grid1"] tr.rgRow', { timeout: 30000 });
            await new Promise(r => setTimeout(r, 2000));
            log.info(`[CICC] Page size set to ${size} via RadComboBox`);
            return;
          }
        }
      }
    }

    // Alternative: try changing page size via JavaScript postback
    // Telerik RadGrid uses __doPostBack with 'FireCommand' to change page size
    log.info(`[CICC] Attempting page size change via JS...`);
    const changed = await page.evaluate((sz) => {
      // Find the RadGrid's client-side object
      const gridTables = document.querySelectorAll('table[id*="ResultsGrid_Grid1"]');
      if (!gridTables.length) return false;

      // Try to find the page size input in the pager
      const pageSizeInputs = document.querySelectorAll('input[id*="PageSize"], input[id*="ChangePageSize"]');
      for (const input of pageSizeInputs) {
        input.value = String(sz);
        input.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
      }

      return false;
    }, size);

    if (changed) {
      await new Promise(r => setTimeout(r, 3000));
      log.info(`[CICC] Page size change attempted via JS input`);
    } else {
      log.info('[CICC] No page size control found — using default page size');
    }
  }

  /**
   * Extract rows from the current RadGrid page.
   *
   * Actual HTML structure per row:
   *   <td><a title="Select" href="/Pr_Profile_RCIC?ID=14656">Select</a></td>
   *   <td>R711474</td>                          — College ID
   *   <td>Harpreet Kaur -</td>                  — Name
   *   <td>GALT IMMIGRATION SOLUTIONS INC.</td>  — Company
   *   <td>RCIC-IRB - L3</td>                    — Type
   *   <td>Yes</td>                              — Entitled to Practise
   *   <td style="display:none;">14656</td>      — Hidden profile ID
   *
   * Returns array of { collegeId, name, company, type, eligible, _profileId }
   */
  async _extractGridRows(page) {
    return await page.evaluate(() => {
      const rows = [];
      const gridRows = document.querySelectorAll(
        'table[id*="ResultsGrid_Grid1"] tr.rgRow, table[id*="ResultsGrid_Grid1"] tr.rgAltRow'
      );

      for (const row of gridRows) {
        const cells = row.querySelectorAll('td');
        if (cells.length < 5) continue;

        let profileId = null;

        // Method 1: Extract from the "Select" link href — /Pr_Profile_RCIC?ID=14656
        const selectLink = row.querySelector('a[href*="ID="], a[title="Select"]');
        if (selectLink) {
          const href = selectLink.getAttribute('href') || '';
          const idMatch = href.match(/ID=(\d+)/i);
          if (idMatch) profileId = idMatch[1];
        }

        // Method 2: Hidden cell with display:none containing just the numeric ID
        if (!profileId) {
          for (const cell of cells) {
            if (cell.style && cell.style.display === 'none') {
              const val = cell.textContent.trim();
              if (/^\d{3,8}$/.test(val)) {
                profileId = val;
                break;
              }
            }
          }
        }

        // Method 3: Last cell is often the hidden ID
        if (!profileId) {
          const lastCell = cells[cells.length - 1];
          const val = lastCell.textContent.trim();
          if (/^\d{3,8}$/.test(val)) {
            profileId = val;
          }
        }

        if (!profileId) continue; // Skip rows without a usable ID

        // Extract visible cell texts (skip hidden cells)
        const visibleTexts = [];
        for (const cell of cells) {
          if (cell.style && cell.style.display === 'none') continue;
          if (cell.getAttribute('aria-hidden') === 'true') continue;
          visibleTexts.push(cell.textContent.replace(/\s+/g, ' ').trim());
        }

        // Column order: [Select], College ID, Name, Company, Type, Entitled to Practise
        rows.push({
          _profileId: profileId,
          _cells: visibleTexts,
          collegeId: visibleTexts[1] || '',
          name: visibleTexts[2] || '',
          company: visibleTexts[3] || '',
          type: visibleTexts[4] || '',
          eligible: visibleTexts[5] || '',
        });
      }
      return rows;
    });
  }

  /**
   * Get total result count and total pages from the RadGrid pager.
   */
  async _getGridInfo(page) {
    return await page.evaluate(() => {
      let totalResults = 0;
      let totalPages = 0;
      let currentPage = 1;
      let pageSize = 50; // default after our page size change

      // Count actual rows on this page to determine effective page size
      const currentRows = document.querySelectorAll(
        'table[id*="ResultsGrid_Grid1"] tr.rgRow, table[id*="ResultsGrid_Grid1"] tr.rgAltRow'
      );
      if (currentRows.length > 0) {
        pageSize = currentRows.length;
      }

      // Look for paging info text like "1-50 of 17251 items" in all pager areas
      const pagerElements = document.querySelectorAll('.rgInfoPart, .rgPager, div[id*="Pager"], .rgWrap');
      for (const el of pagerElements) {
        const text = el.textContent || '';
        // Match patterns like "1 - 50 of 17,251 items" or "items 1 to 50 of 17251"
        const totalMatch = text.match(/of\s+([\d,]+)\s*items?/i) ||
                          text.match(/([\d,]+)\s*items?\s*in/i) ||
                          text.match(/([\d,]+)\s*records?/i);
        if (totalMatch && !totalResults) {
          totalResults = parseInt(totalMatch[1].replace(/,/g, ''), 10);
        }
        const rangeMatch = text.match(/(\d+)\s*[-–]\s*(\d+)/);
        if (rangeMatch) {
          const from = parseInt(rangeMatch[1], 10);
          const to = parseInt(rangeMatch[2], 10);
          const rangeSize = to - from + 1;
          if (rangeSize > 0) {
            pageSize = rangeSize;
            currentPage = Math.ceil(from / pageSize);
          }
        }
      }

      if (totalResults > 0 && pageSize > 0) {
        totalPages = Math.ceil(totalResults / pageSize);
      }

      // Fallback: count pagination links for max visible page
      if (!totalPages) {
        const pageLinks = document.querySelectorAll('.rgNumPart a, a[id*="Page"], .rgPagerCell a');
        let maxPage = 0;
        for (const link of pageLinks) {
          const t = link.textContent.trim();
          if (/^\d+$/.test(t)) {
            const p = parseInt(t, 10);
            if (p > maxPage) maxPage = p;
          }
        }
        if (maxPage > 0) totalPages = maxPage;
      }

      return { totalResults, totalPages, currentPage, pageSize };
    });
  }

  /**
   * Navigate to the next page of the RadGrid by clicking the next page button.
   * Returns true if navigation succeeded, false if we're on the last page.
   */
  async _goToNextPage(page) {
    const navigated = await page.evaluate(() => {
      // Look for "Next" button in the RadGrid pager
      const nextBtns = document.querySelectorAll(
        '.rgPageNext:not(.rgDisabled), ' +
        'input[title="Next Page"]:not([disabled]), ' +
        'button[title="Next Page"]:not([disabled]), ' +
        'a[title="Next Page"], ' +
        '.rgArrPart2 button:not([disabled])'
      );

      for (const btn of nextBtns) {
        if (!btn.disabled && !btn.classList.contains('rgDisabled')) {
          btn.click();
          return true;
        }
      }
      return false;
    });

    if (!navigated) return false;

    // Wait for the grid to reload via AJAX
    try {
      await page.waitForFunction(() => {
        // Wait for loading indicator to appear and disappear
        const loading = document.querySelector('.raDiv, .rgLoading, .RadAjax_Default');
        return !loading || loading.style.display === 'none';
      }, { timeout: 15000 });
      await new Promise(r => setTimeout(r, 1500));
      // Verify rows are present
      await page.waitForSelector('table[id*="ResultsGrid_Grid1"] tr.rgRow, table[id*="ResultsGrid_Grid1"] tr.rgAltRow', { timeout: 15000 });
      return true;
    } catch (err) {
      log.warn(`[CICC] Next page navigation may have failed: ${err.message}`);
      // Check if there are still rows (might have loaded despite timeout)
      const hasRows = await page.$('table[id*="ResultsGrid_Grid1"] tr.rgRow');
      return !!hasRows;
    }
  }

  /**
   * Fetch a single profile page and extract lead data.
   * Opens a new tab, navigates, clicks Licensee Details tab, extracts data.
   */
  async _fetchProfile(profileId, basicData) {
    const profilePage = await this.browser.newPage();
    try {
      await profilePage.setViewport({ width: 1280, height: 900 });
      profilePage.setDefaultNavigationTimeout(45000);
      profilePage.setDefaultTimeout(20000);

      const url = `${PROFILE_SHORT}?ID=${profileId}`;
      await profilePage.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
      await new Promise(r => setTimeout(r, 800));

      // Extract basic profile info from the page
      const profileData = await profilePage.evaluate(() => {
        const text = document.body.innerText || '';
        const html = document.body.innerHTML || '';
        const result = {};

        // Name — large text span
        const nameEl = document.querySelector('span[style*="font-size: 32px"], span[style*="font-size:32px"]');
        if (nameEl) {
          result.name = nameEl.textContent.replace(/\s+/g, ' ').trim();
        }

        // College ID
        const idMatch = text.match(/College ID\s*[-:]\s*(R\d{4,7})/i);
        if (idMatch) result.collegeId = idMatch[1];

        // Type (RCIC, RISIA)
        const typeMatch = text.match(/Type\s*[-:]\s*(RCIC|RISIA)/i);
        if (typeMatch) result.type = typeMatch[1].toUpperCase();

        // Eligibility status
        if (/NOT\s+Eligible/i.test(text)) {
          result.eligible = false;
          result.barStatus = 'Not Eligible';
        } else if (/Eligible\s+to\s+Provide/i.test(text)) {
          result.eligible = true;
          result.barStatus = 'Eligible to Provide Service';
        }

        // Licence status
        const licStatusMatch = text.match(/Licence Status\s*[-:]?\s*(Active|Suspended|Revoked|Expired|Surrendered|Cancelled)/i);
        if (licStatusMatch) {
          result.licenceStatus = licStatusMatch[1];
        }

        // Licence class
        const classMatch = text.match(/Licence\s+(Class\s+\w+(?:\s*-\s*[\w-]+)?)/i);
        if (classMatch) {
          result.licenceClass = classMatch[1].trim();
        }

        return result;
      });

      // Now click the "Licensee Details" tab to load the Employment table.
      // The RadTabStrip uses AutoPostBack (full page navigation), so we must
      // use Puppeteer's click + waitForNavigation pattern.
      let employmentData = null;
      try {
        // Find the "Licensee Details" tab element
        const tabSelector = await profilePage.evaluate(() => {
          const tabs = document.querySelectorAll('.rtsLI a, .rtsLink, li[class*="rtsLI"] a, div[id*="radTab"] a');
          for (let i = 0; i < tabs.length; i++) {
            const text = tabs[i].textContent.trim().toLowerCase();
            if (text.includes('licensee details')) {
              // Return a unique selector for this tab
              tabs[i].setAttribute('data-cicc-tab', 'licensee-details');
              return '[data-cicc-tab="licensee-details"]';
            }
          }
          // Fallback: try second tab by index
          const allTabs = document.querySelectorAll('div[id*="radTab"] .rtsLink');
          if (allTabs.length >= 2) {
            allTabs[1].setAttribute('data-cicc-tab', 'licensee-details');
            return '[data-cicc-tab="licensee-details"]';
          }
          return null;
        });

        if (tabSelector) {
          // Click and wait for navigation (full postback)
          await Promise.all([
            profilePage.waitForNavigation({ waitUntil: 'networkidle2', timeout: 20000 }),
            profilePage.click(tabSelector),
          ]);
          await new Promise(r => setTimeout(r, 500));

          // Extract Employment table data
          employmentData = await profilePage.evaluate(() => {
            const entries = [];
            // Find the Employment RadGrid rows
            const empRows = document.querySelectorAll(
              'table[id*="Employment"] tr.rgRow, table[id*="Employment"] tr.rgAltRow'
            );

            for (const row of empRows) {
              const cells = row.querySelectorAll('td');
              if (cells.length < 7) continue;

              const entry = {
                company: cells[0].textContent.replace(/\s+/g, ' ').trim(),
                startDate: cells[1].textContent.replace(/\s+/g, ' ').trim(),
                country: cells[2].textContent.replace(/\s+/g, ' ').trim(),
                province: cells[3].textContent.replace(/\s+/g, ' ').trim(),
                city: cells[4].textContent.replace(/\s+/g, ' ').trim(),
                phone: cells[6].textContent.replace(/\s+/g, ' ').trim(),
              };

              // Email cell (index 5) — may be Cloudflare-obfuscated
              const emailCell = cells[5];
              const cfEl = emailCell.querySelector('[data-cfemail]');
              if (cfEl) {
                entry.cfEmail = cfEl.getAttribute('data-cfemail');
              } else {
                // Try plain text email
                const emailText = emailCell.textContent.replace(/\s+/g, ' ').trim();
                if (emailText.includes('@')) {
                  entry.plainEmail = emailText.toLowerCase();
                }
              }

              // Also check for mailto links
              const mailLink = emailCell.querySelector('a[href^="mailto:"]');
              if (mailLink) {
                const href = mailLink.getAttribute('href') || '';
                entry.plainEmail = href.replace('mailto:', '').toLowerCase().trim();
              }

              entries.push(entry);
            }

            return entries.length > 0 ? entries : null;
          });
        }
      } catch (err) {
        log.warn(`[CICC] Tab click failed for profile ${profileId}: ${err.message}`);
      }

      // Combine profile data with employment data
      const lead = {
        bar_number: profileData.collegeId || basicData.collegeId || '',
        first_name: '',
        last_name: '',
        firm_name: '',
        city: '',
        state: '',
        country: 'CA',
        phone: '',
        email: '',
        website: '',
        bar_status: profileData.licenceStatus || profileData.barStatus || basicData.eligible || '',
        title: profileData.licenceClass || '',
        license_type: profileData.type || basicData.type || '',
        profile_url: `${PROFILE_BASE}?ID=${profileId}`,
        niche: 'immigration consultant',
        trade_category: 'Immigration Consulting',
      };

      // Parse name — strip trailing "-" or "." artifacts from the registry
      const rawName = profileData.name || basicData.name || '';
      const fullName = rawName.replace(/\s*[-.]$/, '').trim();
      if (fullName) {
        const parts = fullName.split(/\s+/).filter(Boolean);
        if (parts.length >= 2) {
          lead.first_name = parts[0];
          lead.last_name = parts.slice(1).join(' ');
        } else if (parts.length === 1) {
          lead.last_name = parts[0];
        }
      }

      // Fill from employment data (use first/most recent entry)
      if (employmentData && employmentData.length > 0) {
        const emp = employmentData[0];
        if (emp.company && emp.company !== '&nbsp;' && emp.company.trim()) {
          lead.firm_name = emp.company;
        }
        if (emp.city && emp.city !== '&nbsp;' && emp.city.trim()) {
          lead.city = emp.city;
        }
        if (emp.province && emp.province !== '&nbsp;' && emp.province.trim()) {
          lead.state = emp.province;
        }
        if (emp.phone && emp.phone !== '&nbsp;' && emp.phone.trim()) {
          lead.phone = emp.phone;
        }

        // Decode email
        if (emp.cfEmail) {
          const decoded = this._decodeCfEmail(emp.cfEmail);
          if (decoded && decoded.includes('@') && !decoded.includes('college-ic.ca')) {
            lead.email = decoded;
          }
        } else if (emp.plainEmail && emp.plainEmail.includes('@') && !emp.plainEmail.includes('college-ic.ca')) {
          lead.email = emp.plainEmail;
        }
      }

      // Fallback: use company from grid data
      if (!lead.firm_name && basicData.company) {
        lead.firm_name = basicData.company;
      }

      return lead;
    } catch (err) {
      log.warn(`[CICC] Profile fetch error for ID ${profileId}: ${err.message}`);
      return null;
    } finally {
      try { await profilePage.close(); } catch (_) { /* ignore */ }
    }
  }

  /**
   * Main search generator.
   * Uses Puppeteer to navigate the search page RadGrid, then fetches each profile.
   */
  async *search(practiceArea, options = {}) {
    const maxPages = options.maxPages || Infinity;
    const seenIds = new Set();
    let totalYielded = 0;
    let totalWithEmail = 0;
    let totalWithPhone = 0;
    let totalProfilesFetched = 0;

    await this._initBrowser();

    let searchPage = null;

    try {
      // Initialize search page and get all results
      searchPage = await this._initSearchPage();

      // Get grid info (total results, pages)
      const gridInfo = await this._getGridInfo(searchPage);
      log.info(`[CICC] Grid info: ${JSON.stringify(gridInfo)}`);

      const effectiveMaxPages = Math.min(maxPages, gridInfo.totalPages || 999);
      log.scrape(`[CICC] Starting scrape: ~${gridInfo.totalResults || '?'} registrants, ${effectiveMaxPages} pages max`);

      yield { _cityProgress: { current: 1, total: effectiveMaxPages } };

      // Process each page
      for (let pageNum = 1; pageNum <= effectiveMaxPages; pageNum++) {
        log.info(`[CICC] Processing page ${pageNum}/${effectiveMaxPages}...`);
        yield { _cityProgress: { current: pageNum, total: effectiveMaxPages } };

        // Extract rows from current grid page
        const rows = await this._extractGridRows(searchPage);
        log.info(`[CICC] Page ${pageNum}: ${rows.length} rows extracted`);

        if (rows.length === 0) {
          log.warn(`[CICC] Page ${pageNum} has 0 rows — stopping`);
          break;
        }

        // For each row, fetch the profile page
        for (const row of rows) {
          const profileId = row._profileId;
          const collegeId = row.collegeId;
          const dedupKey = collegeId || profileId;

          if (!profileId) {
            log.warn(`[CICC] Row has no profile ID: ${JSON.stringify(row._cells?.slice(0, 3))}`);
            continue;
          }

          if (dedupKey && seenIds.has(dedupKey)) continue;
          if (dedupKey) seenIds.add(dedupKey);

          // Rate limit between profile fetches (polite but not excessive — no anti-bot)
          await new Promise(r => setTimeout(r, 300 + Math.random() * 400));

          totalProfilesFetched++;
          const lead = await this._fetchProfile(profileId, {
            collegeId: collegeId,
            name: row.name,
            company: row.company,
            type: row.type,
            eligible: row.eligible,
          });

          if (lead && (lead.first_name || lead.last_name || lead.firm_name)) {
            if (lead.email) totalWithEmail++;
            if (lead.phone) totalWithPhone++;

            yield this.transformResult(lead, practiceArea);
            totalYielded++;

            if (totalYielded % 50 === 0) {
              log.info(`[CICC] Progress: ${totalYielded} leads (${totalWithEmail} email, ${totalWithPhone} phone) from ${totalProfilesFetched} profiles`);
            }
          }
        }

        // Navigate to next page (unless we're on the last page)
        if (pageNum < effectiveMaxPages) {
          const moved = await this._goToNextPage(searchPage);
          if (!moved) {
            log.info(`[CICC] No more pages after page ${pageNum}`);
            break;
          }
          // Small delay after page navigation
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    } catch (err) {
      log.error(`[CICC] Search error: ${err.message}`);
      if (err.message.includes('CAPTCHA') || err.message.includes('challenge')) {
        yield { _captcha: true, city: 'all', reason: err.message };
      }
    } finally {
      if (searchPage) {
        try { await searchPage.close(); } catch (_) { /* ignore */ }
      }
      await this._closeBrowser();
    }

    log.success(`[CICC] Finished — ${totalYielded} leads (${totalWithEmail} email, ${totalWithPhone} phone) from ${totalProfilesFetched} profiles`);
  }
}

module.exports = new CICCScraper();
