/**
 * Justia.com Lawyer Directory Scraper
 *
 * Source: https://www.justia.com/lawyers
 * Method: Puppeteer (Cloudflare managed challenge requires real browser)
 * Data:   Lawyer name, phone, website, location, practice areas
 *
 * URL Patterns:
 *   By state: https://www.justia.com/lawyers/{state-slug}/all-cities
 *   By practice+state: https://www.justia.com/lawyers/{practice-area}/{state-slug}
 *   Profile: https://lawyers.justia.com/lawyer/{name-slug}-{id}
 *
 * HIGH PRIORITY — 1M+ US lawyers, sometimes has email/phone/website.
 * Requires Puppeteer due to Cloudflare protection.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { titleCase } = require('../../lib/normalizer');

// State slugs for URL building
const STATE_SLUGS = {
  AL: 'alabama', AK: 'alaska', AZ: 'arizona', AR: 'arkansas',
  CA: 'california', CO: 'colorado', CT: 'connecticut', DE: 'delaware',
  DC: 'district-of-columbia', FL: 'florida', GA: 'georgia', HI: 'hawaii',
  ID: 'idaho', IL: 'illinois', IN: 'indiana', IA: 'iowa',
  KS: 'kansas', KY: 'kentucky', LA: 'louisiana', ME: 'maine',
  MD: 'maryland', MA: 'massachusetts', MI: 'michigan', MN: 'minnesota',
  MS: 'mississippi', MO: 'missouri', MT: 'montana', NE: 'nebraska',
  NV: 'nevada', NH: 'new-hampshire', NJ: 'new-jersey', NM: 'new-mexico',
  NY: 'new-york', NC: 'north-carolina', ND: 'north-dakota', OH: 'ohio',
  OK: 'oklahoma', OR: 'oregon', PA: 'pennsylvania', RI: 'rhode-island',
  SC: 'south-carolina', SD: 'south-dakota', TN: 'tennessee', TX: 'texas',
  UT: 'utah', VT: 'vermont', VA: 'virginia', WA: 'washington',
  WV: 'west-virginia', WI: 'wisconsin', WY: 'wyoming',
};

// Major states to scrape (sorted by legal market size)
const DEFAULT_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'NJ',
  'VA', 'MA', 'MI', 'WA', 'AZ', 'CO', 'MN', 'TN', 'MO', 'MD',
  'IN', 'WI', 'CT', 'OR', 'SC', 'KY', 'LA', 'AL', 'OK', 'NV',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

class JustiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'justia',
      stateCode: 'JUSTIA',
      baseUrl: 'https://www.justia.com',
      pageSize: 25,
      practiceAreaCodes: {
        'Personal Injury': 'personal-injury',
        'Criminal Defense': 'criminal-defense',
        'Family Law': 'family',
        'Business Law': 'business',
        'Estate Planning': 'estate-planning',
        'Bankruptcy': 'bankruptcy',
        'Real Estate': 'real-estate',
        'Employment': 'employment',
        'Immigration': 'immigration',
        'Intellectual Property': 'intellectual-property',
        'Tax': 'tax',
        'DUI/DWI': 'dui-dwi',
      },
      defaultCities: DEFAULT_STATES,
    });
  }

  /**
   * Search Justia for lawyers across US states.
   * Uses Puppeteer to bypass Cloudflare protection.
   */
  async *search(practiceArea, options = {}) {
    let browser;
    try {
      const puppeteer = require('puppeteer');
      const launchOptions = {
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
        ],
      };
      if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
      }
      browser = await puppeteer.launch(launchOptions);
    } catch (err) {
      log.error(`[Justia] Failed to launch Puppeteer: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: 'Puppeteer not available' };
      return;
    }

    try {
      const page = await browser.newPage();
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );

      // Resolve practice area slug
      const practiceSlug = practiceArea
        ? (this.practiceAreaCodes[practiceArea] || practiceArea.toLowerCase().replace(/\s+/g, '-'))
        : null;

      // Select states to search
      let states = [...DEFAULT_STATES];
      if (options.city) {
        // "city" field is overloaded to accept state codes for directory scrapers
        const upper = options.city.toUpperCase();
        if (STATE_SLUGS[upper]) {
          states = [upper];
        }
      }

      const maxStates = options.maxCities || (options.maxPages ? options.maxPages : null);
      if (maxStates) states = states.slice(0, maxStates);

      for (let i = 0; i < states.length; i++) {
        const stateCode = states[i];
        const stateSlug = STATE_SLUGS[stateCode];
        if (!stateSlug) continue;

        yield { _cityProgress: { current: i + 1, total: states.length } };

        // Build URL
        const url = practiceSlug
          ? `https://www.justia.com/lawyers/${practiceSlug}/${stateSlug}`
          : `https://www.justia.com/lawyers/${stateSlug}/all-cities`;

        log.info(`[Justia] Scraping ${stateCode}: ${url}`);

        let pageNum = 1;
        const maxPages = options.maxPages || 5;

        while (pageNum <= maxPages) {
          const pageUrl = pageNum === 1 ? url : `${url}?page=${pageNum}`;

          try {
            await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 30000 });

            // Wait for content to load
            await page.waitForSelector('body', { timeout: 10000 });

            // Check for Cloudflare challenge
            const isChallenge = await page.evaluate(() => {
              return document.title.includes('Just a moment') ||
                document.title.includes('Attention Required') ||
                document.querySelector('#challenge-running') !== null;
            });

            if (isChallenge) {
              log.warn(`[Justia] Cloudflare challenge on ${stateCode} page ${pageNum} — waiting...`);
              await new Promise(r => setTimeout(r, 5000));
              // Try to proceed after wait
              const stillChallenge = await page.evaluate(() =>
                document.title.includes('Just a moment')
              );
              if (stillChallenge) {
                log.warn(`[Justia] Cannot bypass Cloudflare for ${stateCode}`);
                break;
              }
            }

            // Extract lawyer data from the page
            const lawyers = await page.evaluate((sc) => {
              const results = [];

              // Try multiple selector patterns (Justia redesigns periodically)
              const cards = document.querySelectorAll(
                '.lawyer-card, .attorney-card, [data-lawyer-id], .jcard, ' +
                'article.listing, .listing-item, .lawyer-listing, .profile-card'
              );

              if (cards.length > 0) {
                cards.forEach(card => {
                  const nameEl = card.querySelector('h2 a, h3 a, .lawyer-name a, .name a, a.lawyer-link');
                  const phoneEl = card.querySelector('.phone, [href^="tel:"], .lawyer-phone');
                  const locationEl = card.querySelector('.location, .city, .lawyer-location, address');
                  const websiteEl = card.querySelector('a[href*="website"], a.website-link, .website a');
                  const profileEl = card.querySelector('a[href*="lawyers.justia.com"], h2 a, h3 a');

                  const name = nameEl?.textContent?.trim() || '';
                  const phone = phoneEl?.textContent?.trim() || phoneEl?.getAttribute('href')?.replace('tel:', '') || '';
                  const location = locationEl?.textContent?.trim() || '';
                  const website = websiteEl?.getAttribute('href') || '';
                  const profileUrl = profileEl?.getAttribute('href') || '';

                  if (!name) return;

                  const locParts = location.split(',').map(s => s.trim());
                  const city = locParts[0] || '';
                  const state = locParts.length > 1 ? locParts[locParts.length - 1].replace(/\d+/g, '').trim() : sc;

                  results.push({ name, phone, city, state, website, profileUrl });
                });
              }

              // Fallback: try to find links to individual lawyer profiles
              if (results.length === 0) {
                const links = document.querySelectorAll('a[href*="lawyers.justia.com/lawyer/"]');
                links.forEach(link => {
                  const name = link.textContent?.trim() || '';
                  const profileUrl = link.href || '';
                  const parentEl = link.closest('div, li, article, tr');
                  const phone = parentEl?.querySelector('[href^="tel:"]')?.textContent?.trim() || '';
                  const location = parentEl?.querySelector('.location, address, .city')?.textContent?.trim() || '';

                  if (!name || name.length < 3) return;

                  const locParts = location.split(',').map(s => s.trim());
                  const city = locParts[0] || '';

                  results.push({ name, phone, city, state: sc, website: '', profileUrl });
                });
              }

              return results;
            }, stateCode);

            if (lawyers.length === 0) {
              log.info(`[Justia] No results on ${stateCode} page ${pageNum}`);
              break;
            }

            for (const lawyer of lawyers) {
              const { firstName, lastName } = this._parseName(lawyer.name);
              if (!firstName && !lastName) continue;

              yield this.transformResult({
                first_name: firstName,
                last_name: lastName,
                firm_name: '',
                city: lawyer.city,
                state: stateCode,
                phone: lawyer.phone,
                website: lawyer.website,
                email: '',
                bar_number: '',
                bar_status: '',
                admission_date: '',
                source: 'justia',
                profile_url: lawyer.profileUrl,
              }, practiceArea || '');
            }

            log.info(`[Justia] ${stateCode} page ${pageNum}: ${lawyers.length} lawyers`);

            // Check for next page
            const hasNext = await page.evaluate(() => {
              const nextLink = document.querySelector('a[rel="next"], .pagination .next a, a.next-page');
              return !!nextLink;
            });

            if (!hasNext) break;
            pageNum++;

            // Polite delay
            await new Promise(r => setTimeout(r, 3000 + Math.random() * 3000));
          } catch (err) {
            log.warn(`[Justia] Error on ${stateCode} page ${pageNum}: ${err.message}`);
            break;
          }
        }

        // Delay between states
        await new Promise(r => setTimeout(r, 5000 + Math.random() * 5000));
      }
    } finally {
      if (browser) await browser.close();
    }
  }

  /**
   * Parse a full name into first and last name.
   * "John A. Smith" → { firstName: "John", lastName: "Smith" }
   * "Smith, John A." → { firstName: "John", lastName: "Smith" }
   */
  _parseName(fullName) {
    if (!fullName) return { firstName: '', lastName: '' };

    let cleaned = fullName
      .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|phd|md|llm)/gi, '')
      .trim();

    // "Last, First Middle" format
    if (cleaned.includes(',')) {
      const [last, ...rest] = cleaned.split(',').map(s => s.trim());
      const first = rest.join(' ').split(/\s+/)[0] || '';
      return { firstName: titleCase(first), lastName: titleCase(last) };
    }

    // "First [Middle] Last" format
    const parts = cleaned.split(/\s+/);
    if (parts.length >= 2) {
      return {
        firstName: titleCase(parts[0]),
        lastName: titleCase(parts[parts.length - 1]),
      };
    }

    return { firstName: '', lastName: titleCase(cleaned) };
  }
}

module.exports = new JustiaScraper();
