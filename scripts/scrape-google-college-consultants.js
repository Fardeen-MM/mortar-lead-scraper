#!/usr/bin/env node
/**
 * Google Maps — College Admissions Consultants Scraper
 *
 * Scrapes Google Maps for college admissions consultants, SAT/ACT tutors,
 * college essay consultants, scholarship consultants, and educational
 * consultants across the top 100 US metro areas.
 *
 * Strategy:
 *   1. Iterate through 7 search terms x 100 metro areas
 *   2. For each search, navigate to Google Maps, scroll to load all results
 *   3. Click each result to extract phone, website, address from detail panel
 *   4. Visit each business website briefly to extract owner/consultant names + emails
 *   5. Dedup by firm_name + city
 *   6. Restart browser every 20 searches to avoid rate-limiting
 *   7. Resume support via progress file
 *   8. Incremental CSV append
 *
 * Usage:
 *   node scripts/scrape-google-college-consultants.js --test         # 2-3 cities
 *   node scripts/scrape-google-college-consultants.js --all          # All 100 metros
 *   node scripts/scrape-google-college-consultants.js --resume       # Resume interrupted
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 4000;
const DELAY_MAX = 7000;
const NAV_TIMEOUT = 45000;
const SCROLL_WAIT = 2500;
const MAX_SCROLLS = 8;          // Scroll iterations to load results
const BROWSER_RESTART_EVERY = 20;
const WEBSITE_VISIT_TIMEOUT = 15000;
const MAX_WEBSITE_VISITS_PER_SEARCH = 15; // Limit website visits per search query

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-google-maps.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'college-consultants-gmaps-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── Search Terms ─────────────────────────────────────────────────────────────

const SEARCH_TERMS = [
  'college admissions consultant',
  'college counselor near me',
  'SAT ACT prep tutoring',
  'college essay consultant',
  'scholarship consultant',
  'college planning advisor',
  'independent educational consultant',
];

// ── Top 100 US Metro Areas ──────────────────────────────────────────────────

const METROS = [
  { city: 'New York', state: 'NY' },
  { city: 'Los Angeles', state: 'CA' },
  { city: 'Chicago', state: 'IL' },
  { city: 'Dallas', state: 'TX' },
  { city: 'Houston', state: 'TX' },
  { city: 'Washington', state: 'DC' },
  { city: 'Philadelphia', state: 'PA' },
  { city: 'Miami', state: 'FL' },
  { city: 'Atlanta', state: 'GA' },
  { city: 'Boston', state: 'MA' },
  { city: 'Phoenix', state: 'AZ' },
  { city: 'San Francisco', state: 'CA' },
  { city: 'Riverside', state: 'CA' },
  { city: 'Detroit', state: 'MI' },
  { city: 'Seattle', state: 'WA' },
  { city: 'Minneapolis', state: 'MN' },
  { city: 'San Diego', state: 'CA' },
  { city: 'Tampa', state: 'FL' },
  { city: 'Denver', state: 'CO' },
  { city: 'St. Louis', state: 'MO' },
  { city: 'Baltimore', state: 'MD' },
  { city: 'Orlando', state: 'FL' },
  { city: 'Charlotte', state: 'NC' },
  { city: 'San Antonio', state: 'TX' },
  { city: 'Portland', state: 'OR' },
  { city: 'Sacramento', state: 'CA' },
  { city: 'Pittsburgh', state: 'PA' },
  { city: 'Austin', state: 'TX' },
  { city: 'Las Vegas', state: 'NV' },
  { city: 'Cincinnati', state: 'OH' },
  { city: 'Kansas City', state: 'MO' },
  { city: 'Columbus', state: 'OH' },
  { city: 'Cleveland', state: 'OH' },
  { city: 'Indianapolis', state: 'IN' },
  { city: 'San Jose', state: 'CA' },
  { city: 'Nashville', state: 'TN' },
  { city: 'Virginia Beach', state: 'VA' },
  { city: 'Providence', state: 'RI' },
  { city: 'Milwaukee', state: 'WI' },
  { city: 'Jacksonville', state: 'FL' },
  { city: 'Memphis', state: 'TN' },
  { city: 'Oklahoma City', state: 'OK' },
  { city: 'Raleigh', state: 'NC' },
  { city: 'Richmond', state: 'VA' },
  { city: 'Louisville', state: 'KY' },
  { city: 'New Orleans', state: 'LA' },
  { city: 'Salt Lake City', state: 'UT' },
  { city: 'Hartford', state: 'CT' },
  { city: 'Birmingham', state: 'AL' },
  { city: 'Buffalo', state: 'NY' },
  { city: 'Rochester', state: 'NY' },
  { city: 'Grand Rapids', state: 'MI' },
  { city: 'Tucson', state: 'AZ' },
  { city: 'Tulsa', state: 'OK' },
  { city: 'Fresno', state: 'CA' },
  { city: 'Bridgeport', state: 'CT' },
  { city: 'Worcester', state: 'MA' },
  { city: 'Albuquerque', state: 'NM' },
  { city: 'Omaha', state: 'NE' },
  { city: 'Bakersfield', state: 'CA' },
  { city: 'Albany', state: 'NY' },
  { city: 'Knoxville', state: 'TN' },
  { city: 'Baton Rouge', state: 'LA' },
  { city: 'El Paso', state: 'TX' },
  { city: 'Allentown', state: 'PA' },
  { city: 'McAllen', state: 'TX' },
  { city: 'Dayton', state: 'OH' },
  { city: 'Columbia', state: 'SC' },
  { city: 'Greensboro', state: 'NC' },
  { city: 'Little Rock', state: 'AR' },
  { city: 'Stockton', state: 'CA' },
  { city: 'Charleston', state: 'SC' },
  { city: 'Akron', state: 'OH' },
  { city: 'Colorado Springs', state: 'CO' },
  { city: 'Boise', state: 'ID' },
  { city: 'Des Moines', state: 'IA' },
  { city: 'Wichita', state: 'KS' },
  { city: 'Spokane', state: 'WA' },
  { city: 'Madison', state: 'WI' },
  { city: 'Provo', state: 'UT' },
  { city: 'Winston-Salem', state: 'NC' },
  { city: 'Greenville', state: 'SC' },
  { city: 'Cape Coral', state: 'FL' },
  { city: 'Lakeland', state: 'FL' },
  { city: 'Chattanooga', state: 'TN' },
  { city: 'Fayetteville', state: 'AR' },
  { city: 'Deltona', state: 'FL' },
  { city: 'Ogden', state: 'UT' },
  { city: 'Honolulu', state: 'HI' },
  { city: 'Durham', state: 'NC' },
  { city: 'Lexington', state: 'KY' },
  { city: 'Palm Bay', state: 'FL' },
  { city: 'Springfield', state: 'MA' },
  { city: 'Sarasota', state: 'FL' },
  { city: 'Harrisburg', state: 'PA' },
  { city: 'Toledo', state: 'OH' },
  { city: 'Oxnard', state: 'CA' },
  { city: 'Scranton', state: 'PA' },
  { city: 'Syracuse', state: 'NY' },
  { city: 'Reno', state: 'NV' },
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return rawPhone.trim();
}

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

/**
 * Parse a person name from a business name.
 * Returns { first_name, last_name } or empty strings if not a person.
 */
function parseName(businessName) {
  if (!businessName) return { first_name: '', last_name: '' };

  let cleaned = businessName;

  // Strip leading "The"
  cleaned = cleaned.replace(/^the\s+/i, '');

  // "X Consulting/Tutoring/Services of John Smith"
  const ofMatch = cleaned.match(/(?:(?:Consulting|Tutoring|Services?|Academy|Center|Prep|Test Prep|College)\s+(?:of|by)\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .trim();
  } else {
    // Remove business suffixes — use word boundary \b to avoid partial matches
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Co\.?|Corp\.?)/gi, '')
      .replace(/\b(Consulting|Consultants?|Tutoring|Tutors?|Prep|Test\s+Prep|SAT|ACT|College|Admissions?|Education(al)?|Counseling|Counselors?|Advisors?|Advisory|Services?|Solutions?|Academy|Center|Centre|Group|Associates?|Partners?|Studio|Learning|Institute|Coaching|Coach|Experts?|Specialists?|Professionals?|Network|Hub|Zone|Lab|Pro|Plus|Premier|Elite|Global|National|Online|International|Point|Apply)\b/gi, '')
      .trim();
  }

  // Clean trailing/leading punctuation and whitespace
  cleaned = cleaned.replace(/^[,.\s&+|:—–-]+/, '').replace(/[,.\s&+|:—–-]+$/, '').trim();
  // Collapse multiple spaces
  cleaned = cleaned.replace(/\s{2,}/g, ' ').trim();

  // Skip if too short after cleaning
  if (cleaned.length < 3) return { first_name: '', last_name: '' };
  // Skip names with & or "and"
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Check if it looks like a person name (2-3 proper-case words)
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    // Reject if any word is a common business/UI word
    const bizWords = /^(consulting|college|admissions|education|educational|tutoring|services|prep|academy|center|group|learning|coaching|solutions|institute|partners|online|global|national|premier|elite|top|best|pro|expert|experts|point|apply|network|hub|zone|lab|plus|smart|bright|wise|star|peak|ivy|grad|path|keys|next|new|total|pure|real|true|first|east|west|north|south)$/i;
    for (const p of parts) {
      if (bizWords.test(p)) return { first_name: '', last_name: '' };
    }
    return { first_name: parts[0], last_name: lastName };
  }

  return { first_name: '', last_name: '' };
}

/**
 * Parse city/state from a full address string.
 * e.g. "123 Main St, Boston, MA 02101" -> { city: "Boston", state: "MA" }
 */
function parseAddress(address, fallbackCity, fallbackState) {
  if (!address) return { city: fallbackCity, state: fallbackState };
  // Try to find "City, ST" or "City, ST ZIP" pattern
  const match = address.match(/,\s*([A-Za-z\s]+?),\s*([A-Z]{2})\s*\d{0,5}/);
  if (match) {
    return { city: match[1].trim(), state: match[2] };
  }
  // Try just "City, ST"
  const match2 = address.match(/([A-Za-z\s]+),\s*([A-Z]{2})\s*$/);
  if (match2) {
    return { city: match2[1].trim(), state: match2[2] };
  }
  return { city: fallbackCity, state: fallbackState };
}

// ── CSV I/O ──────────────────────────────────────────────────────────────────

function appendCSV(lead, outPath) {
  const headerNeeded = !fs.existsSync(outPath) || fs.statSync(outPath).size === 0;
  const row = CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',');
  if (headerNeeded) {
    fs.writeFileSync(outPath, CSV_COLUMNS.join(',') + '\n' + row + '\n');
  } else {
    fs.appendFileSync(outPath, row + '\n');
  }
}

function writeFullCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedSearches: [], leads: [] };
}

function saveProgress(completedSearches, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedSearches,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Browser Management ───────────────────────────────────────────────────────

async function launchBrowser() {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1280,900',
    ],
  });

  return browser;
}

async function createPage(browser) {
  const page = await browser.newPage();

  // Anti-detection
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    Object.defineProperty(navigator, 'plugins', {
      get: () => [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
        { name: 'Native Client', filename: 'internal-nacl-plugin' },
      ],
    });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function (param) {
      if (param === 37445) return 'Intel Inc.';
      if (param === 37446) return 'Intel Iris OpenGL Engine';
      return getParameter.call(this, param);
    };
  });

  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1280, height: 900 });

  // Block heavy resources for speed
  await page.setRequestInterception(true);
  page.on('request', req => {
    const type = req.resourceType();
    if (['image', 'media', 'font'].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  return page;
}

// ── Google Maps Scraping ─────────────────────────────────────────────────────

/**
 * Dismiss Google consent/cookie dialog if present.
 */
async function dismissConsent(page) {
  try {
    const selectors = [
      'button[aria-label="Accept all"]',
      'button[aria-label="Reject all"]',
      'form[action*="consent"] button',
    ];
    for (const sel of selectors) {
      const btn = await page.$(sel);
      if (btn) {
        await btn.click();
        await sleep(1000);
        break;
      }
    }
  } catch {}
}

/**
 * Scroll the Google Maps results feed to load more entries.
 */
async function scrollFeed(page, maxScrolls) {
  const scrollStart = Date.now();
  const maxScrollTime = 40000;
  for (let i = 0; i < maxScrolls; i++) {
    if (Date.now() - scrollStart > maxScrollTime) break;

    const previousCount = await page.evaluate(() => {
      const feed = document.querySelector('div[role="feed"]');
      return feed ? feed.querySelectorAll('a[aria-label]').length : 0;
    });

    await page.evaluate(() => {
      const feed = document.querySelector('div[role="feed"]');
      if (feed) feed.scrollTop = feed.scrollHeight;
    });

    await sleep(SCROLL_WAIT);

    const newCount = await page.evaluate(() => {
      const feed = document.querySelector('div[role="feed"]');
      return feed ? feed.querySelectorAll('a[aria-label]').length : 0;
    });

    // Check for end of results
    const endReached = await page.evaluate(() => {
      const feed = document.querySelector('div[role="feed"]');
      if (!feed) return true;
      const text = feed.innerText || '';
      return text.includes("You've reached the end of the list") ||
             text.includes('No more results');
    });

    if (endReached || newCount === previousCount) break;
  }
}

/**
 * Extract business data from feed cards (without clicking).
 */
async function extractFeedCards(page) {
  return page.evaluate(() => {
    const items = [];
    const feed = document.querySelector('div[role="feed"]');
    if (!feed) return items;

    const links = feed.querySelectorAll('a[aria-label]');
    for (const link of links) {
      const ariaLabel = (link.getAttribute('aria-label') || '').trim();
      if (!ariaLabel) continue;

      // Filter out non-business links
      if (/^(Visit |Directions |Search |Share |Save |Suggest |Send |Identify |Claim )/i.test(ariaLabel)) continue;
      if (ariaLabel.length > 100) continue;
      if (!/\s/.test(ariaLabel) && ariaLabel.length < 20) continue;

      const href = link.getAttribute('href') || '';

      // Rating
      let rating = '';
      let ratingCount = 0;
      const container = link.closest('div') || link;
      const ratingEl = container.querySelector('span[role="img"]');
      if (ratingEl) {
        const ratingLabel = ratingEl.getAttribute('aria-label') || '';
        const match = ratingLabel.match(/([\d.]+)\s*star/i);
        if (match) rating = match[1];
      }
      if (container) {
        const allText = container.textContent || '';
        const countMatch = allText.match(/\((\d[\d,]*)\)/);
        if (countMatch) ratingCount = parseInt(countMatch[1].replace(/,/g, ''));
      }

      items.push({ name: ariaLabel, href, rating, ratingCount });
    }
    return items;
  });
}

/**
 * Names match check (handles truncation by Google).
 */
function namesMatch(name1, name2) {
  const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const n1 = normalize(name1);
  const n2 = normalize(name2);
  return n1 === n2 || n1.startsWith(n2) || n2.startsWith(n1) ||
    (n1.length >= 10 && n2.length >= 10 && n1.slice(0, 10) === n2.slice(0, 10));
}

/**
 * Click a result in the feed and extract detail panel data.
 */
async function extractDetailByClick(page, index, expectedName) {
  const clicked = await page.evaluate((idx) => {
    const feed = document.querySelector('div[role="feed"]');
    if (!feed) return false;
    const allLinks = feed.querySelectorAll('a[aria-label]');
    const businessLinks = [];
    for (const link of allLinks) {
      const label = (link.getAttribute('aria-label') || '').trim();
      if (!label) continue;
      if (/^(Visit |Directions |Search |Share |Save |Suggest |Send |Identify |Claim )/i.test(label)) continue;
      if (label.length > 100) continue;
      if (!/\s/.test(label) && label.length < 20) continue;
      businessLinks.push(link);
    }
    if (idx >= businessLinks.length) return false;
    businessLinks[idx].click();
    return true;
  }, index);

  if (!clicked) return { phone: '', website: '', address: '' };

  // Wait for detail panel
  const maxWait = 3000;
  const pollInterval = 250;
  let waited = 0;
  while (waited < maxWait) {
    await sleep(pollInterval);
    waited += pollInterval;
    const h1Text = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      return h1 ? h1.textContent.trim() : '';
    });
    if (h1Text && expectedName && namesMatch(h1Text, expectedName)) break;
    if (h1Text && waited >= 1500) break;
  }

  // Extract phone, website, address from detail panel
  const detail = await page.evaluate(() => {
    const result = { phone: '', website: '', address: '' };
    const allButtons = document.querySelectorAll('button[aria-label], a[aria-label]');
    for (const btn of allButtons) {
      const label = (btn.getAttribute('aria-label') || '').toLowerCase();
      const tooltip = (btn.getAttribute('data-tooltip') || '').toLowerCase();

      if (label.includes('phone:') || tooltip === 'copy phone number') {
        const phoneMatch = (btn.getAttribute('aria-label') || '').match(/phone:\s*([\d()+\- .]+)/i);
        if (phoneMatch) result.phone = phoneMatch[1].trim();
        else {
          const text = btn.textContent || '';
          const pm = text.match(/[\d()+\- .]{7,}/);
          if (pm) result.phone = pm[0].trim();
        }
      }

      if (label.includes('website:') || tooltip === 'open website') {
        const urlMatch = (btn.getAttribute('aria-label') || '').match(/website:\s*(.+)/i);
        if (urlMatch) {
          let url = urlMatch[1].trim();
          if (url && !/^https?:\/\//i.test(url)) url = 'https://' + url;
          result.website = url;
        } else if (btn.href && !btn.href.includes('google.com')) {
          result.website = btn.href;
        }
      }

      if (label.includes('address:') || tooltip === 'copy address') {
        const addrMatch = (btn.getAttribute('aria-label') || '').match(/address:\s*(.+)/i);
        if (addrMatch) result.address = addrMatch[1].trim();
      }
    }

    // Fallback: tel: links
    if (!result.phone) {
      const telLinks = document.querySelectorAll('a[href^="tel:"]');
      if (telLinks.length > 0) result.phone = telLinks[0].href.replace('tel:', '').replace(/%20/g, '');
    }

    // Fallback: website links
    if (!result.website) {
      const extLinks = document.querySelectorAll('a[data-tooltip="Open website"]');
      if (extLinks.length > 0 && extLinks[0].href) result.website = extLinks[0].href;
    }

    return result;
  });

  return detail;
}

/**
 * Scrape Google Maps search results for a query.
 */
async function scrapeGoogleMaps(browser, query, isTest) {
  const page = await createPage(browser);
  const results = [];

  try {
    const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });

    // Wait for feed
    await page.waitForSelector('div[role="feed"]', { timeout: 15000 }).catch(() => {});
    await dismissConsent(page);

    // Check if results exist
    const hasFeed = await page.evaluate(() => !!document.querySelector('div[role="feed"]'));
    if (!hasFeed) {
      log(`  No results feed found`);
      return results;
    }

    // Scroll to load results
    const scrollCount = isTest ? 3 : MAX_SCROLLS;
    await scrollFeed(page, scrollCount);

    // Extract feed cards
    const feedCards = await extractFeedCards(page);
    log(`  Found ${feedCards.length} results in feed`);

    // Click each to get details
    const maxDetails = Math.min(feedCards.length, 60);
    for (let i = 0; i < maxDetails; i++) {
      try {
        const detail = await extractDetailByClick(page, i, feedCards[i].name);
        results.push({
          name: feedCards[i].name,
          mapsUrl: feedCards[i].href || '',
          rating: feedCards[i].rating || '',
          ratingCount: feedCards[i].ratingCount || 0,
          phone: detail.phone || '',
          website: detail.website || '',
          address: detail.address || '',
        });
      } catch (err) {
        // Capture basic info even if click fails
        results.push({
          name: feedCards[i].name,
          mapsUrl: feedCards[i].href || '',
          rating: feedCards[i].rating || '',
          ratingCount: feedCards[i].ratingCount || 0,
          phone: '',
          website: '',
          address: '',
        });
      }
    }
  } catch (err) {
    log(`  Maps scrape error: ${err.message}`);
  } finally {
    await page.close().catch(() => {});
  }

  return results;
}

// ── Website Email/Name Extraction ────────────────────────────────────────────

/**
 * Visit a business website and try to extract owner/consultant names and emails.
 * Returns { emails: string[], names: { first_name, last_name, title }[] }
 */
async function scrapeWebsite(browser, websiteUrl) {
  const result = { emails: [], names: [] };
  if (!websiteUrl) return result;

  let page;
  try {
    page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    );
    await page.setViewport({ width: 1280, height: 900 });

    // Block heavy resources
    await page.setRequestInterception(true);
    page.on('request', req => {
      const type = req.resourceType();
      if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    await page.goto(websiteUrl, { waitUntil: 'domcontentloaded', timeout: WEBSITE_VISIT_TIMEOUT });
    await sleep(2000);

    // Extract emails and names from the page
    const pageData = await page.evaluate(() => {
      const data = { emails: [], names: [] };
      const body = document.body ? document.body.innerHTML : '';
      const text = document.body ? document.body.innerText : '';

      // Extract emails
      const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
      const rawEmails = (body.match(emailRegex) || []);
      // Filter out common junk emails
      const junkPatterns = /^(info@|contact@|admin@|support@|noreply@|no-reply@|hello@|sales@|help@|team@|office@|mail@|email@|webmaster@|enquir)/i;
      const imageExts = /\.(png|jpg|jpeg|gif|svg|webp|ico)$/i;
      const junkDomains = /(@sentry[-.]|@wixpress|@example\.|@domain\.|@test\.|@email\.com|@sentry\.io|@sentry-next|@placeholder|@yoursite|@yourdomain)/i;
      for (const em of rawEmails) {
        if (!junkPatterns.test(em) && !imageExts.test(em) && !junkDomains.test(em) && em.length < 60) {
          data.emails.push(em.toLowerCase());
        }
      }
      // Also collect generic ones as fallback (but still exclude junk domains)
      for (const em of rawEmails) {
        if (junkPatterns.test(em) && !imageExts.test(em) && !junkDomains.test(em) && em.length < 60) {
          data.emails.push(em.toLowerCase());
        }
      }

      // Extract person names - look for common patterns on consultant websites
      // "About [Name]", "Meet [Name]", "Dr. [Name]", "Founder: [Name]"
      // IMPORTANT: Reject common false positives from website UI text
      const REJECT_WORDS = new Set([
        'about','us','our','meet','the','team','why','how','what','your','learn','more',
        'read','click','here','view','see','get','find','open','close','new','all',
        'home','back','next','start','join','sign','log','my','east','west','north','south',
        'college','admissions','consulting','services','contact','help','free','top','best',
        'this','that','these','those','their','them','from','with','have','been',
        'hire','call','book','schedule','apply','submit','send',
      ]);
      const namePatterns = [
        /(?:(?:Founded by|Founder|CEO|Owner|Director|President|Principal)\s*[:—–-]?\s*)([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+(?:-[A-Z][a-z]+)?)/g,
        /(?:Dr\.?\s+)([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)/g,
      ];

      for (const pattern of namePatterns) {
        let match;
        while ((match = pattern.exec(text)) !== null) {
          const nameParts = match[1].trim().split(/\s+/);
          if (nameParts.length >= 2 && nameParts.length <= 3) {
            // Reject names where any part is a common UI word
            const hasRejectWord = nameParts.some(p => REJECT_WORDS.has(p.toLowerCase()));
            if (!hasRejectWord) {
              data.names.push({
                first_name: nameParts[0],
                last_name: nameParts[nameParts.length - 1],
                title: '',
              });
            }
          }
        }
      }

      // Check JSON-LD for person/org data
      const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of ldScripts) {
        try {
          const json = JSON.parse(script.textContent);
          const items = Array.isArray(json) ? json : [json];
          for (const item of items) {
            if (item['@type'] === 'Person' && item.name) {
              const parts = item.name.trim().split(/\s+/);
              if (parts.length >= 2) {
                data.names.push({
                  first_name: parts[0],
                  last_name: parts[parts.length - 1],
                  title: item.jobTitle || '',
                });
              }
            }
            // Check for email in LD+JSON
            if (item.email) {
              const em = item.email.replace('mailto:', '').toLowerCase();
              if (!data.emails.includes(em)) data.emails.push(em);
            }
          }
        } catch {}
      }

      // Deduplicate
      data.emails = [...new Set(data.emails)];

      return data;
    });

    result.emails = pageData.emails.slice(0, 5); // Cap at 5
    result.names = pageData.names.slice(0, 3);   // Cap at 3

    // Try About page if we didn't find names on the homepage
    if (result.names.length === 0) {
      try {
        const aboutLinks = await page.evaluate(() => {
          const links = document.querySelectorAll('a');
          for (const l of links) {
            const href = l.href || '';
            const text = (l.textContent || '').toLowerCase();
            if (text.includes('about') || text.includes('team') || text.includes('our story') ||
                href.includes('/about') || href.includes('/team') || href.includes('/our-team')) {
              return href;
            }
          }
          return null;
        });

        if (aboutLinks) {
          await page.goto(aboutLinks, { waitUntil: 'domcontentloaded', timeout: 10000 });
          await sleep(1500);

          const aboutData = await page.evaluate(() => {
            const names = [];
            const text = document.body ? document.body.innerText : '';
            const emails = [];

            // Email extraction
            const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
            const rawEmails = (text.match(emailRegex) || []);
            for (const em of rawEmails) {
              if (em.length < 60) emails.push(em.toLowerCase());
            }

            // Name extraction from about pages
            const REJECT_WORDS2 = new Set([
              'about','us','our','meet','the','team','why','how','what','your','learn','more',
              'read','click','here','view','see','get','find','open','close','new','all',
              'home','back','next','start','join','sign','log','my','east','west','north','south',
              'college','admissions','consulting','services','contact','help','free','top','best',
              'this','that','these','those','their','them','from','with','have','been',
              'hire','call','book','schedule','apply','submit','send',
            ]);
            const patterns = [
              /(?:(?:Founded by|Founder|CEO|Owner|Director|President|Principal|Consultant)\s*[:—–-]?\s*)([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+(?:-[A-Z][a-z]+)?)/g,
              /(?:Dr\.?\s+)([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)/g,
            ];
            for (const pattern of patterns) {
              let match;
              while ((match = pattern.exec(text)) !== null) {
                const parts = match[1].trim().split(/\s+/);
                if (parts.length >= 2 && parts.length <= 3) {
                  const hasReject = parts.some(p => REJECT_WORDS2.has(p.toLowerCase()));
                  if (!hasReject) {
                    names.push({
                      first_name: parts[0],
                      last_name: parts[parts.length - 1],
                      title: '',
                    });
                  }
                }
              }
            }

            // JSON-LD on about page
            const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (const script of ldScripts) {
              try {
                const json = JSON.parse(script.textContent);
                const items = Array.isArray(json) ? json : [json];
                for (const item of items) {
                  if (item['@type'] === 'Person' && item.name) {
                    const parts = item.name.trim().split(/\s+/);
                    if (parts.length >= 2) {
                      names.push({
                        first_name: parts[0],
                        last_name: parts[parts.length - 1],
                        title: item.jobTitle || '',
                      });
                    }
                  }
                }
              } catch {}
            }

            return { names, emails: [...new Set(emails)] };
          });

          if (aboutData.names.length > 0) result.names = aboutData.names.slice(0, 3);
          if (aboutData.emails.length > 0 && result.emails.length === 0) {
            result.emails = aboutData.emails.slice(0, 5);
          }
        }
      } catch {}
    }
  } catch (err) {
    // Website visit failures are non-fatal
  } finally {
    if (page) await page.close().catch(() => {});
  }

  return result;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const isResume = args.includes('--resume');

  if (!isTest && !isAll && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-google-college-consultants.js --test      # 2-3 cities');
    console.log('  node scripts/scrape-google-college-consultants.js --all       # All 100 metros');
    console.log('  node scripts/scrape-google-college-consultants.js --resume    # Resume interrupted');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  let metros = [...METROS];
  let allLeads = [];
  let completedSearches = [];
  const seenKeys = new Set();

  if (isTest) {
    metros = metros.slice(0, 3);
    log('TEST MODE: 3 cities only');
  }

  // Resume support
  if (isResume || isAll) {
    const progress = loadProgress();
    if (progress.completedSearches && progress.completedSearches.length > 0) {
      completedSearches = progress.completedSearches;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedSearches.length} searches done, ${allLeads.length} leads loaded`);
    }
  }

  // Build search list: each metro x each search term
  const allSearches = [];
  for (const metro of metros) {
    for (const term of SEARCH_TERMS) {
      const searchKey = `${metro.city},${metro.state}|${term}`;
      if (!completedSearches.includes(searchKey)) {
        allSearches.push({ metro, term, searchKey });
      }
    }
  }

  log('Google Maps — College Admissions Consultants Scraper');
  log(`Metros: ${metros.length} | Search terms: ${SEARCH_TERMS.length} | Searches remaining: ${allSearches.length}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  let browser = await launchBrowser();
  let searchesSinceRestart = 0;
  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let websiteVisits = 0;

  for (const search of allSearches) {
    processed++;
    searchesSinceRestart++;
    const { metro, term, searchKey } = search;
    const query = `${term} in ${metro.city}, ${metro.state}`;

    // Restart browser periodically
    if (searchesSinceRestart >= BROWSER_RESTART_EVERY) {
      log(`  [Restarting browser after ${searchesSinceRestart} searches]`);
      try { await browser.close(); } catch {}
      await sleep(5000 + Math.random() * 5000);
      browser = await launchBrowser();
      searchesSinceRestart = 0;
    }

    log(`[${processed}/${allSearches.length}] "${term}" in ${metro.city}, ${metro.state}`);

    try {
      // Scrape Google Maps
      const results = await scrapeGoogleMaps(browser, query, isTest);
      let searchNew = 0;
      let websiteVisitsThisSearch = 0;

      for (const r of results) {
        const addrParsed = parseAddress(r.address, metro.city, metro.state);
        const city = addrParsed.city || metro.city;
        const state = addrParsed.state || metro.state;
        const key = dedupKey(r.name, city);

        if (seenKeys.has(key)) continue;
        seenKeys.add(key);

        // Parse name from business name
        let { first_name, last_name } = parseName(r.name);
        let email = '';
        let title = '';

        // Visit website to get email + owner name (limited per search to avoid slowdowns)
        if (r.website && websiteVisitsThisSearch < MAX_WEBSITE_VISITS_PER_SEARCH) {
          try {
            const siteData = await scrapeWebsite(browser, r.website);
            websiteVisits++;
            websiteVisitsThisSearch++;

            // Use first personal email found
            if (siteData.emails.length > 0) {
              email = siteData.emails[0];
            }

            // Use name from website if we didn't parse one from business name
            if (!first_name && siteData.names.length > 0) {
              first_name = siteData.names[0].first_name;
              last_name = siteData.names[0].last_name;
              title = siteData.names[0].title || '';
            } else if (siteData.names.length > 0 && siteData.names[0].title) {
              title = siteData.names[0].title;
            }
          } catch {}
        }

        const lead = {
          first_name: first_name || '',
          last_name: last_name || '',
          firm_name: r.name,
          title,
          email,
          phone: formatPhone(r.phone),
          website: r.website || '',
          domain: extractDomain(r.website),
          city,
          state,
          country: 'US',
          niche: 'education',
          source: 'google-maps',
          profile_url: r.mapsUrl || '',
        };

        allLeads.push(lead);
        appendCSV(lead, OUT_FILE);
        searchNew++;
      }

      totalNew += searchNew;
      log(`  ${results.length} results, ${searchNew} new (${websiteVisitsThisSearch} websites visited)`);
    } catch (err) {
      log(`  ERROR: ${err.message}`);
      errors++;

      // Browser crash recovery
      if (err.message.includes('Navigation') || err.message.includes('closed') || err.message.includes('detached') || err.message.includes('Target')) {
        try { await browser.close(); } catch {}
        await sleep(10000);
        browser = await launchBrowser();
        searchesSinceRestart = 0;
      }
    }

    completedSearches.push(searchKey);

    // Save progress periodically
    if (processed % 5 === 0 && !isTest) {
      saveProgress(completedSearches, allLeads);
      log(`  [Progress saved: ${allLeads.length} total leads]`);
    }

    // Random delay between searches
    await randomDelay();
  }

  try { await browser.close(); } catch {}

  // Final write (overwrite with clean deduped data)
  if (allLeads.length > 0) {
    writeFullCSV(allLeads, OUT_FILE);
  }

  // Save final progress
  if (!isTest) {
    saveProgress(completedSearches, allLeads);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Searches completed: ${processed} (${errors} errors)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Website visits: ${websiteVisits}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && errors === 0 && allSearches.length > 0) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
