#!/usr/bin/env node
/**
 * ShowMeLocal.com College Admissions Consultants Scraper
 *
 * Scrapes ShowMeLocal business directory for college admissions consultants
 * and educational services across all 50 US states + DC.
 *
 * Strategy:
 *   1. Iterate US states via category-city URLs:
 *        https://www.showmelocal.com/{category}-in-{city}-{state}
 *      Also tries search URLs:
 *        https://www.showmelocal.com/search.aspx?q={term}&l={State}
 *   2. Parse search result pages for business listings
 *   3. Visit profile pages for details (phone, email, website, address)
 *   4. Paginate through results (page/2, page/3, etc.)
 *   5. Dedup by firm_name + city
 *   6. Resume support via progress JSON
 *
 * Note: ShowMeLocal serves a CAPTCHA ("Humanoid Verification") page to
 * automated requests. This scraper uses browser-like headers, cookie
 * persistence, and Referer chains to maximize chances of bypassing it.
 * If CAPTCHA is detected, the scraper logs a warning and skips that
 * request rather than hanging. Profile page enrichment is attempted but
 * may also trigger CAPTCHA.
 *
 * Data fields:
 *   - Business name (from card heading / profile)
 *   - Phone (from listing card / profile page)
 *   - Address (city, state from URL path / profile)
 *   - Website (from profile page)
 *   - Email (from profile page)
 *   - Profile URL (from listing card link)
 *
 * Usage:
 *   node scripts/scrape-showmelocal-college.js --test         # 3 states, 1 page each
 *   node scripts/scrape-showmelocal-college.js --all          # All 50 states + DC
 *   node scripts/scrape-showmelocal-college.js --resume       # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const MAX_RETRIES = 3;
const REQUEST_TIMEOUT = 30000;
const MAX_PAGES = 10;
const PROFILE_BATCH_SIZE = 5;       // Enrich N profiles per state/term combo
const PROFILE_DELAY_MIN = 2000;
const PROFILE_DELAY_MAX = 4000;
const INCREMENTAL_SAVE_EVERY = 20;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-showmelocal.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'showmelocal-college-progress.json');

const SOURCE = 'showmelocal';
const NICHE = 'education';
const COUNTRY = 'US';

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Search terms to try for each state
const SEARCH_TERMS = [
  'college admissions consultant',
  'college counselor',
  'educational consultant',
  'SAT prep',
  'tutoring',
];

// ShowMeLocal category slugs that map to our search terms
const CATEGORY_SLUGS = [
  'educational-consultant',
  'college-consultant',
  'college-counselor',
  'tutoring',
  'test-preparation',
  'sat-prep',
];

// All 50 US states + DC with full names and codes
const US_STATES = [
  { name: 'Alabama', code: 'AL' },
  { name: 'Alaska', code: 'AK' },
  { name: 'Arizona', code: 'AZ' },
  { name: 'Arkansas', code: 'AR' },
  { name: 'California', code: 'CA' },
  { name: 'Colorado', code: 'CO' },
  { name: 'Connecticut', code: 'CT' },
  { name: 'Delaware', code: 'DE' },
  { name: 'District of Columbia', code: 'DC' },
  { name: 'Florida', code: 'FL' },
  { name: 'Georgia', code: 'GA' },
  { name: 'Hawaii', code: 'HI' },
  { name: 'Idaho', code: 'ID' },
  { name: 'Illinois', code: 'IL' },
  { name: 'Indiana', code: 'IN' },
  { name: 'Iowa', code: 'IA' },
  { name: 'Kansas', code: 'KS' },
  { name: 'Kentucky', code: 'KY' },
  { name: 'Louisiana', code: 'LA' },
  { name: 'Maine', code: 'ME' },
  { name: 'Maryland', code: 'MD' },
  { name: 'Massachusetts', code: 'MA' },
  { name: 'Michigan', code: 'MI' },
  { name: 'Minnesota', code: 'MN' },
  { name: 'Mississippi', code: 'MS' },
  { name: 'Missouri', code: 'MO' },
  { name: 'Montana', code: 'MT' },
  { name: 'Nebraska', code: 'NE' },
  { name: 'Nevada', code: 'NV' },
  { name: 'New Hampshire', code: 'NH' },
  { name: 'New Jersey', code: 'NJ' },
  { name: 'New Mexico', code: 'NM' },
  { name: 'New York', code: 'NY' },
  { name: 'North Carolina', code: 'NC' },
  { name: 'North Dakota', code: 'ND' },
  { name: 'Ohio', code: 'OH' },
  { name: 'Oklahoma', code: 'OK' },
  { name: 'Oregon', code: 'OR' },
  { name: 'Pennsylvania', code: 'PA' },
  { name: 'Rhode Island', code: 'RI' },
  { name: 'South Carolina', code: 'SC' },
  { name: 'South Dakota', code: 'SD' },
  { name: 'Tennessee', code: 'TN' },
  { name: 'Texas', code: 'TX' },
  { name: 'Utah', code: 'UT' },
  { name: 'Vermont', code: 'VT' },
  { name: 'Virginia', code: 'VA' },
  { name: 'Washington', code: 'WA' },
  { name: 'West Virginia', code: 'WV' },
  { name: 'Wisconsin', code: 'WI' },
  { name: 'Wyoming', code: 'WY' },
];

// Major cities per state for category-based URL construction
const STATE_CITIES = {
  'AL': ['birmingham', 'huntsville', 'mobile', 'montgomery'],
  'AK': ['anchorage', 'fairbanks', 'juneau'],
  'AZ': ['phoenix', 'tucson', 'mesa', 'scottsdale', 'chandler'],
  'AR': ['little-rock', 'fayetteville', 'fort-smith'],
  'CA': ['los-angeles', 'san-francisco', 'san-diego', 'san-jose', 'sacramento', 'fresno', 'oakland', 'irvine', 'pasadena', 'riverside'],
  'CO': ['denver', 'colorado-springs', 'aurora', 'fort-collins', 'boulder'],
  'CT': ['hartford', 'new-haven', 'stamford', 'bridgeport', 'greenwich'],
  'DE': ['wilmington', 'dover', 'newark'],
  'DC': ['washington'],
  'FL': ['miami', 'orlando', 'tampa', 'jacksonville', 'fort-lauderdale', 'west-palm-beach', 'naples'],
  'GA': ['atlanta', 'savannah', 'augusta', 'marietta', 'athens'],
  'HI': ['honolulu'],
  'ID': ['boise', 'meridian'],
  'IL': ['chicago', 'naperville', 'aurora', 'schaumburg', 'evanston'],
  'IN': ['indianapolis', 'fort-wayne', 'carmel', 'fishers'],
  'IA': ['des-moines', 'cedar-rapids', 'iowa-city'],
  'KS': ['wichita', 'overland-park', 'kansas-city', 'olathe'],
  'KY': ['louisville', 'lexington', 'bowling-green'],
  'LA': ['new-orleans', 'baton-rouge', 'shreveport'],
  'ME': ['portland', 'bangor'],
  'MD': ['baltimore', 'bethesda', 'rockville', 'silver-spring', 'columbia'],
  'MA': ['boston', 'cambridge', 'worcester', 'springfield', 'newton'],
  'MI': ['detroit', 'grand-rapids', 'ann-arbor', 'lansing', 'troy'],
  'MN': ['minneapolis', 'saint-paul', 'bloomington', 'edina'],
  'MS': ['jackson', 'gulfport', 'hattiesburg'],
  'MO': ['kansas-city', 'saint-louis', 'springfield', 'columbia'],
  'MT': ['billings', 'missoula', 'great-falls'],
  'NE': ['omaha', 'lincoln'],
  'NV': ['las-vegas', 'reno', 'henderson'],
  'NH': ['manchester', 'concord', 'nashua'],
  'NJ': ['newark', 'jersey-city', 'princeton', 'hackensack', 'morristown', 'cherry-hill'],
  'NM': ['albuquerque', 'santa-fe', 'las-cruces'],
  'NY': ['new-york', 'brooklyn', 'queens', 'buffalo', 'albany', 'rochester', 'white-plains', 'great-neck'],
  'NC': ['charlotte', 'raleigh', 'durham', 'greensboro', 'chapel-hill'],
  'ND': ['fargo', 'bismarck'],
  'OH': ['columbus', 'cleveland', 'cincinnati', 'toledo', 'akron', 'dayton'],
  'OK': ['oklahoma-city', 'tulsa', 'norman'],
  'OR': ['portland', 'salem', 'eugene', 'bend'],
  'PA': ['philadelphia', 'pittsburgh', 'harrisburg', 'allentown', 'king-of-prussia'],
  'RI': ['providence', 'warwick', 'cranston'],
  'SC': ['charleston', 'columbia', 'greenville', 'myrtle-beach'],
  'SD': ['sioux-falls', 'rapid-city'],
  'TN': ['nashville', 'memphis', 'knoxville', 'chattanooga'],
  'TX': ['houston', 'dallas', 'san-antonio', 'austin', 'fort-worth', 'plano', 'frisco', 'the-woodlands'],
  'UT': ['salt-lake-city', 'provo', 'ogden'],
  'VT': ['burlington', 'montpelier'],
  'VA': ['virginia-beach', 'arlington', 'richmond', 'fairfax', 'alexandria', 'mclean'],
  'WA': ['seattle', 'tacoma', 'spokane', 'bellevue', 'redmond'],
  'WV': ['charleston', 'morgantown', 'huntington'],
  'WI': ['milwaukee', 'madison', 'green-bay'],
  'WY': ['cheyenne', 'casper', 'laramie'],
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay(min = DELAY_MIN, max = DELAY_MAX) {
  return sleep(min + Math.random() * (max - min));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
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

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

/**
 * Parse a person name from a business name.
 * Handles patterns like "John Smith Tutoring", "Smith Educational Consulting LLC".
 */
function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  let cleaned = firmName;

  // "XYZ of John Smith" pattern
  const ofMatch = cleaned.match(/(?:(?:College|Education|Academic|Consulting|Counseling|Tutoring)\s+(?:Service|Services|Center|Group|Consultants?|Counseling|Prep)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|&.*)/gi, '')
      .trim();
  } else {
    cleaned = cleaned
      .replace(/,?\s*\b(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Corp\.?|Esq\.?|APC|APLC)\b/gi, '')
      .replace(/\s*(College\s+(?:Admissions?|Counseling|Planning|Consulting|Prep)|Educational?\s+(?:Consulting|Consultants?|Services?|Group)|SAT\s+(?:Prep|Tutoring)|ACT\s+(?:Prep|Tutoring)|Test\s+Prep(?:aration)?|Tutoring\s+(?:Center|Services?|Group)?|Academic\s+(?:Coaching|Consulting|Services?)|Consulting\s+(?:Group|Services?|Firm)|& Associates?|Associates?|Academy|Learning\s+Center)$/gi, '')
      .trim();
  }

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip obvious company-style names
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  const skipWords = /^(educational?|college|tutoring|tutor|test|prep|sat|act|academic|learning|school|consulting|consultant|services?|center|group|solutions?|international|national|global|premier|elite|professional|advanced|online|virtual|smart)$/i;

  // Match "First Last" or "First M. Last"
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    const firstName = parts[0];
    if (skipWords.test(lastName) || skipWords.test(firstName)) return { first_name: '', last_name: '' };
    return { first_name: firstName, last_name: lastName };
  }

  return { first_name: '', last_name: '' };
}

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

/**
 * Convert a hyphenated city slug to title case.
 * "new-york" -> "New York", "fort-lauderdale" -> "Fort Lauderdale"
 */
function slugToTitle(slug) {
  if (!slug) return '';
  return slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ── Cookie Jar ───────────────────────────────────────────────────────────────
// Maintain cookies across requests to preserve any session established

let cookieJar = {};

function parseCookies(setCookieHeaders) {
  if (!setCookieHeaders) return;
  const headers = Array.isArray(setCookieHeaders) ? setCookieHeaders : [setCookieHeaders];
  for (const header of headers) {
    const parts = header.split(';')[0].split('=');
    if (parts.length >= 2) {
      cookieJar[parts[0].trim()] = parts.slice(1).join('=').trim();
    }
  }
}

function getCookieString() {
  return Object.entries(cookieJar).map(([k, v]) => `${k}=${v}`).join('; ');
}

// ── HTTP Fetcher with Retry, Redirect, Timeout ──────────────────────────────

function httpGet(url, retries = MAX_RETRIES, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const transport = parsedUrl.protocol === 'https:' ? https : http;

    const options = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      timeout: REQUEST_TIMEOUT,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://www.showmelocal.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        ...extraHeaders,
      },
    };

    // Include cookies
    const cookieStr = getCookieString();
    if (cookieStr) {
      options.headers['Cookie'] = cookieStr;
    }

    const req = transport.request(options, (res) => {
      // Save cookies from response
      parseCookies(res.headers['set-cookie']);

      // Handle redirects (up to 5 hops)
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `${parsedUrl.protocol}//${parsedUrl.hostname}${redirectUrl}`;
        } else if (!redirectUrl.startsWith('http')) {
          redirectUrl = `${parsedUrl.protocol}//${parsedUrl.hostname}/${redirectUrl}`;
        }
        // Consume the current response body
        res.resume();
        return httpGet(redirectUrl, retries, extraHeaders).then(resolve).catch(reject);
      }

      // 404 — no results for this search
      if (res.statusCode === 404) {
        res.resume();
        return resolve({ statusCode: 404, body: '', url });
      }

      // Rate limited or blocked
      if (res.statusCode === 429 || res.statusCode === 403) {
        res.resume();
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 8000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '', url });
      }

      // Server error — retry
      if (res.statusCode >= 500) {
        res.resume();
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Server error (${res.statusCode}), retrying in ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '', url });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: res.statusCode, body, url }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(5000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(5000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });

    req.end();
  });
}

// ── CAPTCHA Detection ────────────────────────────────────────────────────────

function isCaptchaPage(html) {
  if (!html) return false;
  const lower = html.toLowerCase();
  return (
    lower.includes('humanoid verification') ||
    lower.includes('captchacanvas') ||
    lower.includes('re-verify.aspx') ||
    lower.includes('set-checaptcha.aspx') ||
    (lower.includes('recaptcha') && lower.includes('showmelocal') && !lower.includes('class="listing'))
  );
}

// ── Search Results Parser ────────────────────────────────────────────────────
//
// ShowMeLocal listing pages use various HTML structures. We try multiple
// selector strategies to extract business cards from category pages
// and search result pages.

function parseSearchResults(html, stateCode) {
  if (!html || isCaptchaPage(html)) return { listings: [], totalCount: 0, captcha: isCaptchaPage(html) };

  const $ = cheerio.load(html);
  const listings = [];
  const seen = new Set();

  // Strategy 1: Card-based listings with IDs like card12345
  $('[id^="card"]').each((_, el) => {
    const $card = $(el);
    const cardId = $card.attr('id') || '';
    const bid = cardId.replace('card', '');
    if (!bid || seen.has(bid)) return;

    const name = $card.find('h2, h3, h4').first().text().trim() ||
                 $card.find('a[href*="profile"]').first().text().trim() ||
                 $card.find('.business-name, .biz-name, .listing-name').first().text().trim();
    if (!name || name.length < 2) return;

    seen.add(bid);

    // Phone: look for phone pattern in card text
    const cardText = $card.text();
    const phoneMatch = cardText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    const phone = phoneMatch ? phoneMatch[0] : '';

    // Profile URL
    let profileUrl = '';
    const profileLink = $card.find('a[href*="profile"]').first();
    if (profileLink.length) {
      profileUrl = profileLink.attr('href') || '';
      if (profileUrl && !profileUrl.startsWith('http')) {
        profileUrl = `https://www.showmelocal.com${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`;
      }
    } else if (bid) {
      profileUrl = `https://www.showmelocal.com/profile.aspx?bid=${bid}`;
    }

    // Try to parse city from the card text (typically "City, ST" pattern)
    let city = '';
    let state = stateCode;
    const locMatch = cardText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})/);
    if (locMatch) {
      city = locMatch[1];
      state = locMatch[2];
    }

    // Website from card
    let website = '';
    $card.find('a[href]').each((_, a) => {
      const href = $(a).attr('href') || '';
      if (href.startsWith('http') && !href.includes('showmelocal.com') && !href.includes('google') && !href.includes('facebook')) {
        website = href;
      }
    });

    listings.push({
      firm_name: name,
      phone: formatPhone(phone),
      city,
      state,
      website,
      profile_url: profileUrl,
      bid,
    });
  });

  // Strategy 2: Link-based extraction from any listing containers
  // Look for structured listing divs/sections
  $('div.listing, div.result, div.business-card, div.biz-card, li.listing, .search-result').each((_, el) => {
    const $listing = $(el);

    const name = $listing.find('h2 a, h3 a, h4 a, .business-name a, .biz-name a').first().text().trim() ||
                 $listing.find('h2, h3, h4, .business-name, .biz-name').first().text().trim();
    if (!name || name.length < 2) return;

    const key = name.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seen.has(key)) return;
    seen.add(key);

    let profileUrl = '';
    const link = $listing.find('a[href*="profile"], a[href*="/"]').first();
    if (link.length) {
      profileUrl = link.attr('href') || '';
      if (profileUrl && !profileUrl.startsWith('http')) {
        profileUrl = `https://www.showmelocal.com${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`;
      }
    }

    const listingText = $listing.text();
    const phoneMatch = listingText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    const phone = phoneMatch ? phoneMatch[0] : '';

    let city = '';
    let state = stateCode;
    const locMatch = listingText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})/);
    if (locMatch) {
      city = locMatch[1];
      state = locMatch[2];
    }

    let website = '';
    $listing.find('a[href]').each((_, a) => {
      const href = $(a).attr('href') || '';
      if (href.startsWith('http') && !href.includes('showmelocal.com') && !href.includes('google') && !href.includes('facebook')) {
        website = href;
      }
    });

    listings.push({
      firm_name: name,
      phone: formatPhone(phone),
      city,
      state,
      website,
      profile_url: profileUrl,
      bid: '',
    });
  });

  // Strategy 3: Broad link scan — find all profile links on the page
  if (listings.length === 0) {
    $('a[href*="profile.aspx"], a[href*="/profile/"]').each((_, el) => {
      const $a = $(el);
      const name = $a.text().trim();
      if (!name || name.length < 3 || name.length > 200) return;

      // Skip navigation/UI text
      if (/^(search|find|file|get|learn|start|sign|log|home|about|news|more|see|view|read|back|next|prev|page)/i.test(name)) return;

      const key = name.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (seen.has(key)) return;
      seen.add(key);

      let href = $a.attr('href') || '';
      if (href && !href.startsWith('http')) {
        href = `https://www.showmelocal.com${href.startsWith('/') ? '' : '/'}${href}`;
      }

      // Extract bid from profile URL
      const bidMatch = href.match(/bid=(\d+)/i) || href.match(/profile\/.*?-(\d+)\/?$/i);
      const bid = bidMatch ? bidMatch[1] : '';

      const parentText = $a.parent().text() || '';
      const phoneMatch = parentText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
      const phone = phoneMatch ? phoneMatch[0] : '';

      let city = '';
      let state = stateCode;
      const locMatch = parentText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})/);
      if (locMatch) {
        city = locMatch[1];
        state = locMatch[2];
      }

      listings.push({
        firm_name: name,
        phone: formatPhone(phone),
        city,
        state,
        website: '',
        profile_url: href,
        bid,
      });
    });
  }

  // Try to extract total result count from page
  let totalCount = 0;
  const countText = $('body').text();
  const countMatch = countText.match(/(\d+)\s+(?:results?|businesses?|listings?)\s+found/i) ||
                     countText.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+(\d+)/i) ||
                     countText.match(/(\d+)\s+total/i);
  if (countMatch) {
    totalCount = parseInt(countMatch[1], 10);
  }

  return { listings, totalCount, captcha: false };
}

// ── Profile Page Parser ──────────────────────────────────────────────────────

function parseProfilePage(html) {
  if (!html || isCaptchaPage(html)) return null;

  const $ = cheerio.load(html);
  const result = {};

  // Business name
  const name = $('h1').first().text().trim() ||
               $('.business-name, .biz-name, .profile-name').first().text().trim();
  if (name) result.firm_name = name;

  // Phone — look for tel: links first, then patterns in text
  const telLink = $('a[href^="tel:"]').first();
  if (telLink.length) {
    result.phone = formatPhone(telLink.text().trim() || telLink.attr('href').replace('tel:', ''));
  } else {
    const bodyText = $('body').text();
    const phoneMatch = bodyText.match(/(?:phone|tel|call)[:\s]*\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/i);
    if (phoneMatch) {
      const digits = phoneMatch[0].match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
      if (digits) result.phone = formatPhone(digits[0]);
    }
  }

  // Email — look for mailto: links, then scan text
  const mailLink = $('a[href^="mailto:"]').first();
  if (mailLink.length) {
    result.email = mailLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
  } else {
    const bodyText = $('body').text();
    const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
    if (emailMatch) {
      const email = emailMatch[0].toLowerCase();
      // Filter out common non-business emails
      if (!email.includes('showmelocal') && !email.includes('example.com') && !email.includes('sentry')) {
        result.email = email;
      }
    }
  }

  // Website — look for external links
  $('a[href]').each((_, el) => {
    if (result.website) return;
    const href = $(el).attr('href') || '';
    const text = $(el).text().toLowerCase().trim();
    if (
      href.startsWith('http') &&
      !href.includes('showmelocal.com') &&
      !href.includes('google.com') &&
      !href.includes('facebook.com') &&
      !href.includes('twitter.com') &&
      !href.includes('yelp.com') &&
      !href.includes('bbb.org') &&
      !href.includes('instagram.com') &&
      !href.includes('linkedin.com') &&
      !href.includes('youtube.com') &&
      !href.includes('pinterest.com') &&
      (text.includes('website') || text.includes('visit') || text.includes('www') || text.includes('http') || text === href)
    ) {
      result.website = href;
    }
  });

  // If no labeled website link, look for any external link with business-like domain
  if (!result.website) {
    $('a[href^="http"]').each((_, el) => {
      if (result.website) return;
      const href = $(el).attr('href') || '';
      if (
        !href.includes('showmelocal.com') &&
        !href.includes('google.com') &&
        !href.includes('facebook.com') &&
        !href.includes('twitter.com') &&
        !href.includes('yelp.com') &&
        !href.includes('bbb.org') &&
        !href.includes('instagram.com') &&
        !href.includes('linkedin.com') &&
        !href.includes('youtube.com') &&
        !href.includes('pinterest.com') &&
        !href.includes('tiktok.com') &&
        !href.includes('maps.google') &&
        !href.includes('javascript:')
      ) {
        result.website = href;
      }
    });
  }

  // Address parsing — look for common address patterns
  const bodyText = $('body').text();

  // Full address: "123 Main St, City, ST 12345"
  const addrMatch = bodyText.match(/(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Blvd|Dr|Rd|Ln|Way|Ct|Pl|Cir|Pkwy|Hwy|Suite|Ste|#)\w*\.?),?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
  if (addrMatch) {
    result.address = addrMatch[1].trim();
    result.city = addrMatch[2].trim();
    result.state = addrMatch[3];
    result.zip = addrMatch[4];
  } else {
    // Just City, ST pattern
    const cityStateMatch = bodyText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s+([A-Z]{2})\s+(\d{5})/);
    if (cityStateMatch) {
      result.city = cityStateMatch[1].trim();
      result.state = cityStateMatch[2];
      result.zip = cityStateMatch[3];
    }
  }

  // Categories/description
  const desc = $('meta[name="description"]').attr('content') || '';
  if (desc) result.description = desc;

  return Object.keys(result).length > 0 ? result : null;
}

// ── Pagination URL Builder ───────────────────────────────────────────────────
// ShowMeLocal uses /page/N at the end of category URLs

function buildPaginatedUrl(baseUrl, pageNum) {
  if (pageNum <= 1) return baseUrl;
  // For category URLs like /educational-consultant-in-new-york-ny
  // Pagination is /educational-consultant-in-new-york-ny/page/2
  if (baseUrl.includes('search.aspx') || baseUrl.includes('?')) {
    // Query string URL — add &page=N
    const sep = baseUrl.includes('?') ? '&' : '?';
    return `${baseUrl}${sep}page=${pageNum}`;
  }
  // Path-based URL — append /page/N
  return `${baseUrl.replace(/\/$/, '')}/page/${pageNum}`;
}

// ── Check for Next Page ──────────────────────────────────────────────────────

function hasNextPage(html, currentPage) {
  if (!html) return false;
  const $ = cheerio.load(html);

  // Look for "Next" link
  let hasNext = false;
  $('a').each((_, el) => {
    const text = $(el).text().trim().toLowerCase();
    if (text === 'next' || text === 'next >' || text === 'next page' || text === '>' || text === '>>') {
      hasNext = true;
    }
  });
  if (hasNext) return true;

  // Look for page N+1 link
  const nextPage = currentPage + 1;
  const found = $(`a[href*="page/${nextPage}"], a[href*="page=${nextPage}"]`).length > 0;
  return found;
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

// ── Build URLs to Scrape ─────────────────────────────────────────────────────

function buildAllUrls(stateObj) {
  const urls = [];
  const code = stateObj.code;
  const cities = STATE_CITIES[code] || [];

  // Category-based URLs: /category-in-city-st
  for (const category of CATEGORY_SLUGS) {
    for (const city of cities) {
      urls.push({
        url: `https://www.showmelocal.com/${category}-in-${city}-${code.toLowerCase()}`,
        type: 'category',
        category,
        city: slugToTitle(city),
        stateCode: code,
        key: `cat|${code}|${category}|${city}`,
      });
    }
  }

  // Search-based URLs: /search.aspx?q=term&l=State
  for (const term of SEARCH_TERMS) {
    const encoded = encodeURIComponent(term);
    const loc = encodeURIComponent(stateObj.name);
    urls.push({
      url: `https://www.showmelocal.com/search.aspx?q=${encoded}&l=${loc}`,
      type: 'search',
      category: term,
      city: '',
      stateCode: code,
      key: `search|${code}|${term}`,
    });
  }

  return urls;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const isResume = args.includes('--resume');

  if (!isTest && !isAll && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-showmelocal-college.js --test         # Test mode (3 states)');
    console.log('  node scripts/scrape-showmelocal-college.js --all          # All 50 states + DC');
    console.log('  node scripts/scrape-showmelocal-college.js --resume       # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  let states = [...US_STATES];
  let allLeads = [];
  let completedSearches = [];
  const seenKeys = new Set();

  if (isTest) {
    states = states.slice(0, 3); // Alabama, Alaska, Arizona
    log('TEST MODE: 3 states only');
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

  // Build all search URLs
  const allSearches = [];
  for (const stateObj of states) {
    const urls = buildAllUrls(stateObj);
    for (const item of urls) {
      if (!completedSearches.includes(item.key)) {
        allSearches.push(item);
      }
    }
  }

  const maxPages = isTest ? 2 : MAX_PAGES;

  log('='.repeat(60));
  log('ShowMeLocal.com College Admissions Consultants Scraper');
  log('='.repeat(60));
  log(`States: ${states.length} | Searches remaining: ${allSearches.length}`);
  log(`Category slugs: ${CATEGORY_SLUGS.length} | Search terms: ${SEARCH_TERMS.length}`);
  log(`Max pages per search: ${maxPages} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log(`Progress: ${PROGRESS_FILE}`);
  log('');

  let processed = 0;
  let totalNew = 0;
  let totalErrors = 0;
  let totalCaptcha = 0;
  let newSinceLastSave = 0;
  let profilesEnriched = 0;
  let profileCaptchas = 0;

  // Warm up: make an initial request to the homepage to get cookies
  log('Warming up: fetching homepage for cookies...');
  try {
    await httpGet('https://www.showmelocal.com/', 1);
    log(`  Cookies established: ${Object.keys(cookieJar).length} cookies`);
  } catch (err) {
    log(`  Warm-up failed: ${err.message} (continuing anyway)`);
  }
  await randomDelay();

  for (const search of allSearches) {
    processed++;
    const { url, type, category, stateCode, key } = search;
    const cityLabel = search.city ? ` (${search.city})` : '';

    log(`[${processed}/${allSearches.length}] ${type.toUpperCase()} "${category}" in ${stateCode}${cityLabel}`);

    let pageNum = 1;
    let searchNewCount = 0;
    let searchTotalCount = 0;
    let consecutiveEmpty = 0;

    while (pageNum <= maxPages) {
      const pageUrl = buildPaginatedUrl(url, pageNum);

      try {
        const { statusCode, body } = await httpGet(pageUrl);

        if (statusCode === 404) {
          if (pageNum === 1) log(`  404 Not Found — skipping`);
          break;
        }

        if (statusCode !== 200) {
          log(`  HTTP ${statusCode} on page ${pageNum} — skipping`);
          break;
        }

        // Check for CAPTCHA
        if (isCaptchaPage(body)) {
          totalCaptcha++;
          if (pageNum === 1) {
            log(`  CAPTCHA detected — ShowMeLocal blocked this request`);
          }
          break;
        }

        // Parse results
        const { listings, totalCount, captcha } = parseSearchResults(body, stateCode);

        if (captcha) {
          totalCaptcha++;
          log(`  CAPTCHA detected during parsing`);
          break;
        }

        if (pageNum === 1 && totalCount > 0) {
          const estPages = Math.min(Math.ceil(totalCount / 20), maxPages);
          log(`  ~${totalCount} results (~${estPages} pages)`);
        }

        if (listings.length === 0) {
          consecutiveEmpty++;
          if (pageNum === 1) {
            log(`  No listings found on page 1`);
          }
          if (consecutiveEmpty >= 2) break; // 2 empty pages in a row → stop
          pageNum++;
          await randomDelay();
          continue;
        }

        consecutiveEmpty = 0;
        searchTotalCount += listings.length;

        // Process each listing
        const newListings = [];
        for (const listing of listings) {
          const city = listing.city || search.city || '';
          const state = listing.state || stateCode;
          const dKey = dedupKey(listing.firm_name, city);

          if (seenKeys.has(dKey)) continue;
          seenKeys.add(dKey);

          const { first_name, last_name } = parseName(listing.firm_name);

          const lead = {
            first_name,
            last_name,
            firm_name: listing.firm_name,
            title: '',
            email: '',
            phone: listing.phone || '',
            website: listing.website || '',
            domain: extractDomain(listing.website),
            city,
            state,
            country: COUNTRY,
            niche: NICHE,
            source: SOURCE,
            profile_url: listing.profile_url || '',
          };

          allLeads.push(lead);
          newListings.push(lead);
          searchNewCount++;
          newSinceLastSave++;
        }

        // Profile enrichment for new listings that have profile URLs
        // Only enrich a batch per search to avoid excessive requests
        const toEnrich = newListings
          .filter(l => l.profile_url && (!l.phone || !l.website || !l.email))
          .slice(0, PROFILE_BATCH_SIZE);

        if (toEnrich.length > 0 && !isTest) {
          for (const lead of toEnrich) {
            try {
              await randomDelay(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX);
              const profileResp = await httpGet(lead.profile_url, 1);

              if (profileResp.statusCode === 200 && !isCaptchaPage(profileResp.body)) {
                const profileData = parseProfilePage(profileResp.body);
                if (profileData) {
                  // Merge profile data into lead (only fill missing fields)
                  if (profileData.phone && !lead.phone) lead.phone = formatPhone(profileData.phone);
                  if (profileData.email && !lead.email) lead.email = profileData.email;
                  if (profileData.website && !lead.website) {
                    lead.website = profileData.website;
                    lead.domain = extractDomain(profileData.website);
                  }
                  if (profileData.city && !lead.city) lead.city = profileData.city;
                  if (profileData.state && !lead.state) lead.state = profileData.state;
                  if (profileData.firm_name && lead.firm_name.length < profileData.firm_name.length) {
                    lead.firm_name = profileData.firm_name;
                  }
                  profilesEnriched++;
                }
              } else if (isCaptchaPage(profileResp.body)) {
                profileCaptchas++;
                // Stop enriching for this batch if we hit CAPTCHA
                break;
              }
            } catch (err) {
              // Profile enrichment is best-effort — don't crash
              log(`    Profile error: ${err.message}`);
            }
          }
        }

        // Check for next page
        if (!hasNextPage(body, pageNum)) break;
        pageNum++;
        await randomDelay();

      } catch (err) {
        log(`  ERROR on page ${pageNum}: ${err.message}`);
        totalErrors++;
        break;
      }
    }

    totalNew += searchNewCount;
    if (searchTotalCount > 0 || searchNewCount > 0) {
      log(`  Found ${searchTotalCount} listings, ${searchNewCount} new (total: ${allLeads.length})`);
    }

    completedSearches.push(key);

    // Incremental save
    if (newSinceLastSave >= INCREMENTAL_SAVE_EVERY && !isTest) {
      writeCSV(allLeads, OUT_FILE);
      saveProgress(completedSearches, allLeads);
      log(`  [Saved: ${allLeads.length} leads to CSV + progress]`);
      newSinceLastSave = 0;
    }

    // Save progress every 25 searches
    if (processed % 25 === 0 && !isTest) {
      writeCSV(allLeads, OUT_FILE);
      saveProgress(completedSearches, allLeads);
      log(`  [Checkpoint: ${allLeads.length} leads, ${completedSearches.length} searches done]`);
    }

    await randomDelay();
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) {
    saveProgress(completedSearches, allLeads);
  }

  // ── Stats ──────────────────────────────────────────────────────────────────

  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city).filter(Boolean)).size;

  log('');
  log('='.repeat(60));
  log('SCRAPE COMPLETE');
  log('='.repeat(60));
  log(`Searches completed: ${processed}`);
  log(`Errors: ${totalErrors} | CAPTCHA blocks: ${totalCaptcha} (search) + ${profileCaptchas} (profile)`);
  log(`Profiles enriched: ${profilesEnriched}`);
  log('');
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone:  ${withPhone} (${allLeads.length ? Math.round(withPhone / allLeads.length * 100) : 0}%)`);
  log(`With email:  ${withEmail} (${allLeads.length ? Math.round(withEmail / allLeads.length * 100) : 0}%)`);
  log(`With website: ${withWebsite} (${allLeads.length ? Math.round(withWebsite / allLeads.length * 100) : 0}%)`);
  log(`With parsed name: ${withName}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log('');
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run (no resume needed)
  if (!isTest && totalErrors === 0 && allSearches.length > 0 && totalCaptcha === 0) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
    log('Progress file cleaned up (complete run)');
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
