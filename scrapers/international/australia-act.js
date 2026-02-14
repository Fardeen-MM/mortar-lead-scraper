/**
 * ACT (AU-ACT) Law Society -- Find a Lawyer Scraper
 *
 * Source: https://www.actlawsociety.asn.au/find-a-lawyer
 * Method: Puppeteer (headless browser) -- the Bond MCRM platform renders
 *         search results entirely via client-side JavaScript; no server-side
 *         HTML results or public JSON API is exposed.
 *
 * The ACT Law Society directory is hosted on Bond MCRM (actls.bond.software)
 * and provides search by:
 *   - Area of Practice (50+ categories)
 *   - Language Spoken
 *   - Workplace Type (Private, Government, Community Legal Centre, etc.)
 *   - Location (suburb or postcode)
 *   - Name (surname or first name)
 *
 * Strategy:
 *   1. Launch Puppeteer, navigate to /find-a-lawyer
 *   2. For each city, set the location filter and trigger search
 *   3. Wait for results to render
 *   4. Parse the result cards from the DOM
 *   5. Yield practitioner records
 *
 * Fields available per practitioner (from result cards):
 *   - Name, firm, suburb, areas of practice, languages, workplace type
 *   - Phone and email may or may not be shown depending on the directory
 *
 * ~800-1200 practising solicitors in the ACT.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

let puppeteer;
try {
  puppeteer = require('puppeteer');
} catch (e) {
  // Puppeteer may not be installed in all environments
}

class ActScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-act',
      stateCode: 'AU-ACT',
      baseUrl: 'https://www.actlawsociety.asn.au/find-a-lawyer',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative law':              'Administrative law',
        'alternative dispute resolution':  'Alternative dispute resolution',
        'animal law':                      'Animal law',
        'banking':                         'Banking and finance',
        'banking and finance':             'Banking and finance',
        'building':                        'Building and construction',
        'building and construction':       'Building and construction',
        'construction':                    'Building and construction',
        'business':                        'Business and corporate',
        'business and corporate':          'Business and corporate',
        'corporate':                       'Business and corporate',
        'children':                        "Children's law",
        "children's law":                  "Children's law",
        'civil litigation':                'Civil litigation',
        'commercial':                      'Commercial law',
        'commercial law':                  'Commercial law',
        'consumer law':                    'Consumer law',
        'contract':                        'Contract',
        'conveyancing':                    'Conveyancing',
        'copyright':                       'Copyright',
        'criminal':                        'Criminal law',
        'criminal law':                    'Criminal law',
        'debt collection':                 'Debt collection',
        'defamation':                      'Defamation',
        'discrimination':                  'Discrimination',
        'dispute resolution':              'Dispute resolution',
        'employment':                      'Employment and industrial',
        'employment and industrial':       'Employment and industrial',
        'industrial':                      'Employment and industrial',
        'environment':                     'Environment',
        'environmental':                   'Environment',
        'equity':                          'Equity and trusts',
        'equity and trusts':               'Equity and trusts',
        'trusts':                          'Equity and trusts',
        'family':                          'Family law',
        'family law':                      'Family law',
        'franchising':                     'Franchising',
        'government':                      'Government',
        'human rights':                    'Human rights',
        'immigration':                     'Immigration',
        'insurance':                       'Insurance',
        'intellectual property':           'Intellectual property',
        'international law':               'International law',
        'leases':                          'Leases and tenancy',
        'leases and tenancy':              'Leases and tenancy',
        'tenancy':                         'Leases and tenancy',
        'litigation':                      'Litigation',
        'migration':                       'Migration',
        'military':                        'Military law',
        'military law':                    'Military law',
        'motor vehicle':                   'Motor vehicle accident compensation',
        'negligence':                      'Negligence',
        'personal injury':                 'Personal injury',
        'planning':                        'Planning',
        'privacy':                         'Privacy',
        'property':                        'Property and real estate',
        'property and real estate':        'Property and real estate',
        'real estate':                     'Property and real estate',
        'strata':                          'Strata and community title',
        'taxation':                        'Taxation and revenue',
        'tax':                             'Taxation and revenue',
        'traffic':                         'Traffic',
        'wills':                           'Wills and estates',
        'estates':                         'Wills and estates',
        'wills and estates':               'Wills and estates',
        'work health':                     'Work health and safety',
        'workers compensation':            "Workers' compensation",
      },
      defaultCities: ['Canberra', 'Belconnen', 'Woden', 'Tuggeranong'],
    });
  }

  // --- BaseScraper overrides (not used since search() is fully overridden) ---

  buildSearchUrl({ city }) {
    return `${this.baseUrl}?Suburb=${encodeURIComponent(city || '')}`;
  }

  parseResultsPage() {
    return [];
  }

  extractResultCount() {
    return 0;
  }

  // --- Puppeteer-based search implementation ---

  /**
   * Async generator that yields practitioner records from the ACT Law Society.
   *
   * Uses Puppeteer to render the Bond MCRM JavaScript-driven directory.
   * Falls back to a clear placeholder message if Puppeteer is unavailable
   * or if the page structure has changed.
   */
  async *search(practiceArea, options = {}) {
    if (!puppeteer) {
      log.warn('AU-ACT: Puppeteer not available -- cannot render Bond MCRM JavaScript directory');
      log.warn('AU-ACT: Install puppeteer (npm install puppeteer) to enable this scraper');
      yield { _placeholder: true, reason: 'puppeteer_unavailable' };
      return;
    }

    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);
    const cities = this.getCities(options);
    const seen = new Set();

    let browser;
    try {
      browser = await puppeteer.launch({
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
        ],
      });

      const page = await browser.newPage();

      // Set a realistic user agent
      await page.setUserAgent(rateLimiter.getUserAgent());

      // Set viewport
      await page.setViewport({ width: 1280, height: 900 });

      // Block images and stylesheets for speed
      await page.setRequestInterception(true);
      page.on('request', (req) => {
        const type = req.resourceType();
        if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      for (let ci = 0; ci < cities.length; ci++) {
        const city = cities[ci];
        yield { _cityProgress: { current: ci + 1, total: cities.length } };
        log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, AU-ACT`);

        try {
          // Navigate to the find-a-lawyer page
          await rateLimiter.wait();
          await page.goto(this.baseUrl, {
            waitUntil: 'networkidle2',
            timeout: 30000,
          });

          // Wait for the search form to be present
          await page.waitForSelector('input, select', { timeout: 10000 });

          // Fill in the location field
          const locationFilled = await page.evaluate((cityName) => {
            // Try to find the location/suburb input field
            const inputs = document.querySelectorAll('input[type="text"], input:not([type])');
            for (const input of inputs) {
              const placeholder = (input.placeholder || '').toLowerCase();
              const label = input.closest('label')?.textContent?.toLowerCase() || '';
              const prev = input.previousElementSibling?.textContent?.toLowerCase() || '';
              const name = (input.name || '').toLowerCase();
              const id = (input.id || '').toLowerCase();

              if (placeholder.includes('suburb') || placeholder.includes('postcode') ||
                  placeholder.includes('location') || label.includes('suburb') ||
                  label.includes('location') || prev.includes('suburb') ||
                  prev.includes('location') || name.includes('suburb') ||
                  name.includes('location') || id.includes('suburb') ||
                  id.includes('location')) {
                input.value = cityName;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                return true;
              }
            }
            // Fallback: use the last text input (often the location field)
            const allInputs = document.querySelectorAll('input[type="text"]');
            if (allInputs.length > 0) {
              const lastInput = allInputs[allInputs.length - 1];
              lastInput.value = cityName;
              lastInput.dispatchEvent(new Event('input', { bubbles: true }));
              lastInput.dispatchEvent(new Event('change', { bubbles: true }));
              return true;
            }
            return false;
          }, city);

          if (!locationFilled) {
            log.warn(`AU-ACT: Could not find location input for ${city}`);
            continue;
          }

          // Select area of practice if specified
          if (practiceCode) {
            await page.evaluate((areaValue) => {
              const selects = document.querySelectorAll('select');
              for (const sel of selects) {
                const label = sel.closest('label')?.textContent?.toLowerCase() || '';
                const prev = sel.previousElementSibling?.textContent?.toLowerCase() || '';
                const name = (sel.name || '').toLowerCase();

                if (label.includes('area') || label.includes('practice') ||
                    prev.includes('area') || prev.includes('practice') ||
                    name.includes('area') || name.includes('practice')) {
                  // Find the matching option
                  for (const opt of sel.options) {
                    if (opt.text.toLowerCase().includes(areaValue.toLowerCase())) {
                      sel.value = opt.value;
                      sel.dispatchEvent(new Event('change', { bubbles: true }));
                      return true;
                    }
                  }
                }
              }
              return false;
            }, practiceCode);
          }

          // Click the search button
          const searchClicked = await page.evaluate(() => {
            const buttons = document.querySelectorAll('button, input[type="submit"], a.btn');
            for (const btn of buttons) {
              const text = (btn.textContent || btn.value || '').toLowerCase().trim();
              if (text === 'search' || text === 'find' || text.includes('search')) {
                btn.click();
                return true;
              }
            }
            // Fallback: submit any form on the page
            const forms = document.querySelectorAll('form');
            for (const form of forms) {
              if (form.querySelector('input[type="text"], select')) {
                form.submit();
                return true;
              }
            }
            return false;
          });

          if (!searchClicked) {
            log.warn(`AU-ACT: Could not find search button for ${city}`);
            continue;
          }

          // Wait for results to load
          await new Promise(resolve => setTimeout(resolve, 3000));

          // Try to wait for results container to appear/change
          try {
            await page.waitForFunction(() => {
              const body = document.body.innerText;
              // Check if we have results or a "no results" message
              return body.includes('Phone') || body.includes('Email') ||
                     body.includes('Sorry') || body.includes('result') ||
                     document.querySelectorAll('.search-result, .result, .lawyer, .listing, .card, .member').length > 0;
            }, { timeout: 10000 });
          } catch (e) {
            // Timeout waiting for results, continue anyway
          }

          // Additional wait for dynamic content
          await new Promise(resolve => setTimeout(resolve, 2000));

          // Extract results from the page
          const attorneys = await page.evaluate(() => {
            const results = [];

            // Strategy 1: Look for structured result cards/divs
            const resultContainers = document.querySelectorAll(
              '.search-result, .result-item, .lawyer-result, .listing-item, ' +
              '.directory-result, .member-result, [class*="result"], [class*="lawyer"], ' +
              '[class*="listing"], [class*="member"]'
            );

            if (resultContainers.length > 0) {
              for (const container of resultContainers) {
                const text = container.innerText || '';
                if (text.length < 10) continue;

                // Try to extract structured data
                const nameEl = container.querySelector('h2, h3, h4, .name, [class*="name"]');
                const name = nameEl ? nameEl.innerText.trim() : '';

                if (!name) continue;

                // Look for other fields
                const firmEl = container.querySelector('.firm, [class*="firm"], [class*="company"], [class*="practice"]');
                const phoneEl = container.querySelector('.phone, [class*="phone"], [class*="tel"], a[href^="tel:"]');
                const emailEl = container.querySelector('.email, [class*="email"], a[href^="mailto:"]');
                const addressEl = container.querySelector('.address, [class*="address"], [class*="location"]');

                results.push({
                  full_name: name,
                  firm_name: firmEl ? firmEl.innerText.trim() : '',
                  phone: phoneEl ? (phoneEl.href?.replace('tel:', '') || phoneEl.innerText.trim()) : '',
                  email: emailEl ? (emailEl.href?.replace('mailto:', '') || emailEl.innerText.trim()) : '',
                  address: addressEl ? addressEl.innerText.trim() : '',
                  raw_text: text.substring(0, 500),
                });
              }
            }

            // Strategy 2: Look for Google Maps markers/pins which may contain data
            if (results.length === 0 && typeof plottingAddresses !== 'undefined' && plottingAddresses) {
              // The page has a plottingAddresses variable for Google Maps
              try {
                const addresses = typeof plottingAddresses === 'string'
                  ? JSON.parse(plottingAddresses)
                  : plottingAddresses;
                if (Array.isArray(addresses)) {
                  for (const addr of addresses) {
                    results.push({
                      full_name: addr.Name || addr.name || '',
                      firm_name: addr.Firm || addr.firm || addr.FirmName || '',
                      phone: addr.Phone || addr.phone || '',
                      email: addr.Email || addr.email || '',
                      address: addr.Address || addr.address || '',
                      city: addr.Suburb || addr.suburb || addr.City || '',
                      postcode: addr.Postcode || addr.postcode || '',
                      lat: addr.Latitude || addr.lat || '',
                      lng: addr.Longitude || addr.lng || '',
                    });
                  }
                }
              } catch (e) {
                // plottingAddresses parse failed
              }
            }

            // Strategy 3: Look for any table with lawyer data
            if (results.length === 0) {
              const tables = document.querySelectorAll('table');
              for (const table of tables) {
                const rows = table.querySelectorAll('tr');
                for (let i = 1; i < rows.length; i++) {
                  const cells = rows[i].querySelectorAll('td');
                  if (cells.length >= 2) {
                    const name = cells[0]?.innerText?.trim() || '';
                    if (name && name.length > 2) {
                      results.push({
                        full_name: name,
                        firm_name: cells[1]?.innerText?.trim() || '',
                        phone: cells[2]?.innerText?.trim() || '',
                        email: cells[3]?.innerText?.trim() || '',
                        address: cells.length > 4 ? cells[4]?.innerText?.trim() : '',
                      });
                    }
                  }
                }
              }
            }

            // Strategy 4: Parse the raw body text for lawyer entries
            // The Bond MCRM directory may render results as a list
            if (results.length === 0) {
              const bodyText = document.body.innerText;
              const hasResults = !bodyText.includes('Sorry, there are no listed');
              if (hasResults) {
                // Try to find blocks that look like lawyer entries
                // Look for phone number patterns near names
                const blocks = bodyText.split(/\n{2,}/);
                for (const block of blocks) {
                  const phoneMatch = block.match(/(?:\+61|0[2-9])\s*[\d\s-]{7,}/);
                  const emailMatch = block.match(/[\w.-]+@[\w.-]+\.\w+/);
                  if (phoneMatch || emailMatch) {
                    const lines = block.split('\n').map(l => l.trim()).filter(Boolean);
                    if (lines.length >= 1 && lines[0].length > 2 && lines[0].length < 80) {
                      results.push({
                        full_name: lines[0],
                        firm_name: lines.length > 1 ? lines[1] : '',
                        phone: phoneMatch ? phoneMatch[0].trim() : '',
                        email: emailMatch ? emailMatch[0] : '',
                        raw_text: block.substring(0, 300),
                      });
                    }
                  }
                }
              }
            }

            return results;
          });

          log.info(`AU-ACT: Found ${attorneys.length} results for ${city}`);

          if (attorneys.length === 0) {
            // Check if there was a "no results" message
            const noResults = await page.evaluate(() => {
              return document.body.innerText.includes('Sorry, there are no listed');
            });

            if (noResults) {
              log.info(`AU-ACT: No results found for ${city}`);
            } else {
              log.warn(`AU-ACT: Page rendered but no results parsed for ${city} -- page structure may have changed`);
            }
            continue;
          }

          log.success(`AU-ACT: Found ${attorneys.length} results for ${city}`);

          // Yield parsed results
          for (const raw of attorneys) {
            const dedupKey = raw.full_name || raw.email || raw.phone;
            if (!dedupKey || seen.has(dedupKey)) continue;
            seen.add(dedupKey);

            const { firstName, lastName } = this.splitName(raw.full_name);

            const attorney = {
              first_name: firstName,
              last_name: lastName,
              full_name: raw.full_name || '',
              firm_name: raw.firm_name || '',
              city: raw.city || city,
              state: 'ACT',
              zip: raw.postcode || '',
              country: 'Australia',
              phone: raw.phone || '',
              email: raw.email || '',
              website: '',
              bar_number: '',
              bar_status: '',
              admission_date: '',
              profile_url: '',
              practice_areas: '',
              address: raw.address || '',
            };

            if (options.minYear && attorney.admission_date) {
              const yearMatch = attorney.admission_date.match(/\d{4}/);
              if (yearMatch) {
                const year = parseInt(yearMatch[0], 10);
                if (year > 0 && year < options.minYear) continue;
              }
            }

            yield this.transformResult(attorney, practiceArea);
          }

        } catch (err) {
          log.error(`AU-ACT: Error searching ${city}: ${err.message}`);

          // Check if it's a CAPTCHA or rendering issue
          if (err.message.includes('timeout') || err.message.includes('Navigation')) {
            log.warn('AU-ACT: Page may require JavaScript rendering or has changed structure');
          }
        }

        // Rate limit between cities
        await rateLimiter.wait();
      }

    } catch (err) {
      log.error(`AU-ACT: Browser launch failed: ${err.message}`);
      log.warn('AU-ACT: Ensure Puppeteer/Chromium is properly installed');
      yield { _placeholder: true, reason: 'browser_launch_failed', error: err.message };
    } finally {
      if (browser) {
        try {
          await browser.close();
        } catch (e) {
          // Ignore close errors
        }
      }
    }
  }
}

module.exports = new ActScraper();
