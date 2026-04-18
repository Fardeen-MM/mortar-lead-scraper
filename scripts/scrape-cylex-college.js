#!/usr/bin/env node
/**
 * Cylex.us College Admissions Consultants Scraper
 *
 * Scrapes https://www.cylex.us.com/ for college admissions consultants,
 * educational consultants, college counselors, SAT/ACT prep, and related
 * businesses across the US.
 *
 * Strategy:
 *   1. Iterate 7 search terms to maximize coverage
 *   2. For each term, paginate through search results pages
 *   3. Parse business cards from search results (name, phone, address, categories)
 *   4. Visit each profile page for additional details (website, email, hours, description)
 *   5. Dedup by firm_name + city to avoid duplicates across search terms
 *   6. Resume support via progress file
 *   7. Output CSV
 *
 * Cylex is relatively scraper-friendly (plain HTTP usually works).
 *
 * Usage:
 *   node scripts/scrape-cylex-college.js                  # Full scrape
 *   node scripts/scrape-cylex-college.js --test           # First 2 terms, 2 pages each, no profiles
 *   node scripts/scrape-cylex-college.js --resume         # Resume interrupted scrape
 *   node scripts/scrape-cylex-college.js --no-profiles    # Skip profile page enrichment
 *   node scripts/scrape-cylex-college.js --term "SAT prep"  # Single search term only
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ────────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const PROFILE_DELAY_MIN = 2000;
const PROFILE_DELAY_MAX = 3500;
const MAX_RETRIES = 3;
const REQUEST_TIMEOUT = 30000;
const MAX_PAGES = 50;                  // Safety cap — Cylex won't have this many pages per term
const INCREMENTAL_SAVE_EVERY = 25;     // Save CSV every N new leads

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-cylex.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'cylex-college-progress.json');

const SOURCE = 'cylex';
const NICHE = 'education';
const COUNTRY = 'US';

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Search terms — iterate all to maximize coverage
const SEARCH_TERMS = [
  'college admissions consultant',
  'college counselor',
  'educational consultant',
  'SAT prep',
  'ACT prep',
  'test prep tutoring',
  'college planning',
];

// ── CLI Args ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const TEST_MODE = hasFlag('test');
const RESUME = hasFlag('resume');
const NO_PROFILES = hasFlag('no-profiles');
const SINGLE_TERM = getArg('term');

// ── Helpers ───────────────────────────────────────────────────────────────────

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
  if (val == null) return '';
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
  const dir = path.dirname(outPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

/**
 * Attempt to parse a person's first/last name from a business name.
 * Only succeeds for patterns like "John Smith Tutoring" or "Jane Doe College Consulting LLC".
 */
function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  let cleaned = firmName;

  // Remove business suffixes
  cleaned = cleaned
    .replace(/,?\s*\b(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Corp\.?|Co\.?)\b/gi, '')
    .replace(/,?\s*(Educational?\s+(?:Consulting|Consultants?|Services?|Group)|College\s+(?:Consulting|Counseling|Prep(?:aration)?|Admissions?|Planning)|Tutoring\s+(?:Services?|Center|Group)?|Test\s+Prep(?:aration)?|SAT\s+(?:Prep|Tutoring)|ACT\s+(?:Prep|Tutoring)|Consulting\s+(?:Services?|Group|Firm)|Academic\s+(?:Coaching|Consulting|Services?)|& Associates?|Associates?|Academy|Learning\s+Center|Prep\s+Center)$/gi, '')
    .trim();

  // Remove trailing punctuation
  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip names starting with "The" — likely a brand name, not a person
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip names with & or "and" — likely a firm with multiple partners
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Words that are definitely not last names
  const skipWords = /^(college|education|educational|academic|consulting|consultants?|counseling|counselor|tutoring|learning|prep|planning|services?|center|group|associates?|firm|inc|llc|academy|school|institute)$/i;

  // Match "FirstName [MiddleInitial.] LastName[-Hyphenated]" pattern
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    if (skipWords.test(lastName)) return { first_name: '', last_name: '' };
    return {
      first_name: parts[0],
      last_name: lastName,
    };
  }

  return { first_name: '', last_name: '' };
}

/**
 * Generate a dedup key from firm name + city.
 */
function dedupKey(name, city) {
  return `${(name || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

// ── HTTP Helper ───────────────────────────────────────────────────────────────

/**
 * HTTP GET with retry logic, timeout handling, and redirect following.
 * Returns { statusCode, headers, body } or throws after MAX_RETRIES.
 */
function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
      timeout: REQUEST_TIMEOUT,
    }, (res) => {
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        let loc = res.headers.location;
        if (loc.startsWith('/')) {
          const parsed = new URL(url);
          loc = `${parsed.protocol}//${parsed.host}${loc}`;
        }
        res.resume(); // Drain the response
        return resolve(httpGet(loc, retries));
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks).toString('utf-8'),
        });
      });
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        const delay = (MAX_RETRIES - retries + 1) * 5000; // Exponential backoff
        log(`  HTTP error: ${err.message} — retrying in ${delay / 1000}s (${retries - 1} retries left)`);
        sleep(delay).then(() => resolve(httpGet(url, retries - 1)));
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        const delay = (MAX_RETRIES - retries + 1) * 5000;
        log(`  Request timed out — retrying in ${delay / 1000}s (${retries - 1} retries left)`);
        sleep(delay).then(() => resolve(httpGet(url, retries - 1)));
      } else {
        reject(new Error(`Request timed out after ${MAX_RETRIES} retries: ${url}`));
      }
    });
  });
}

/**
 * httpGet wrapper that handles non-200 responses with retry logic.
 * Returns the response object or null if all retries exhausted.
 */
async function fetchPage(url) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await httpGet(url, 0); // No inner retry — we handle it here

      if (resp.statusCode === 200) {
        return resp;
      }

      if (resp.statusCode === 404) {
        // Not found — no point retrying
        return null;
      }

      if (resp.statusCode === 429) {
        // Rate limited — back off significantly
        const wait = 15000 * attempt;
        log(`  Rate limited (429) — waiting ${wait / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(wait);
        continue;
      }

      if (resp.statusCode === 403) {
        // Forbidden — might be temp block
        const wait = 10000 * attempt;
        log(`  Forbidden (403) — waiting ${wait / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(wait);
        continue;
      }

      if (resp.statusCode >= 500) {
        // Server error — retry after delay
        const wait = 5000 * attempt;
        log(`  Server error (${resp.statusCode}) — retrying in ${wait / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(wait);
        continue;
      }

      // Other status — log and move on
      log(`  Unexpected HTTP ${resp.statusCode} for ${url}`);
      return null;

    } catch (err) {
      if (attempt < MAX_RETRIES) {
        const wait = 5000 * attempt;
        log(`  Fetch error: ${err.message} — retrying in ${wait / 1000}s (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(wait);
      } else {
        log(`  Fetch failed after ${MAX_RETRIES} attempts: ${err.message}`);
        return null;
      }
    }
  }
  return null;
}

// ── Search Results Parser ─────────────────────────────────────────────────────

/**
 * Build the Cylex search URL for a given query term and page number.
 * Page 1: https://www.cylex.us.com/s?q=college+admissions+consultant&z=
 * Page N: https://www.cylex.us.com/s?q=college+admissions+consultant&z=&p=N
 */
function buildSearchUrl(query, page) {
  const encoded = encodeURIComponent(query);
  if (page <= 1) {
    return `https://www.cylex.us.com/s?q=${encoded}&z=`;
  }
  return `https://www.cylex.us.com/s?q=${encoded}&z=&p=${page}`;
}

/**
 * Parse search results page HTML.
 * Cylex search results typically contain business listing cards with:
 *   - Business name (linked to profile)
 *   - Address (street, city, state, zip)
 *   - Phone number
 *   - Categories/tags
 *   - Rating
 *   - Short description/snippet
 *
 * The exact selectors may vary — we try multiple patterns.
 */
function parseSearchResults($) {
  const results = [];

  // Cylex uses various card patterns for search results.
  // Try the common patterns:

  // Pattern 1: .result-item or .list-item containers
  const resultSelectors = [
    '.result-item',
    '.list-item',
    '.search-result',
    '.business-card',
    '.listing-item',
    'article.result',
    '.company-item',
    '[data-type="business"]',
    '.search-results-item',
  ];

  let $results = $();
  for (const sel of resultSelectors) {
    $results = $(sel);
    if ($results.length > 0) break;
  }

  // If no results found via specific selectors, try to find links to business profile pages
  // Cylex profile URLs typically look like: /company/business-name-12345678.html
  // or /business/business-name.html
  if ($results.length === 0) {
    // Fallback: find all links that look like business profile links
    const profileLinks = new Set();
    $('a[href]').each((i, el) => {
      const href = $(el).attr('href') || '';
      // Cylex profile URLs: /company/name-id.html or similar patterns
      if (/\/(company|business|review)\/[^/]+\.html/i.test(href) ||
          /\/[^/]+-\d+\.html/i.test(href)) {
        profileLinks.add(href);
      }
    });

    // For each unique profile link, try to extract info from its surrounding context
    for (const href of profileLinks) {
      const $link = $(`a[href="${href}"]`).first();
      if (!$link.length) continue;

      const firmName = $link.text().trim();
      if (!firmName || firmName.length < 2) continue;

      // Look at the parent container for more info
      const $container = $link.closest('div, li, article, section, tr');

      let phone = '';
      let city = '';
      let state = '';
      let address = '';
      let profileUrl = href;

      // Make profile URL absolute
      if (profileUrl && !profileUrl.startsWith('http')) {
        profileUrl = `https://www.cylex.us.com${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`;
      }

      // Try to find phone in the container
      if ($container.length) {
        const containerText = $container.text();
        const phoneMatch = containerText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
        if (phoneMatch) phone = phoneMatch[0];

        // Try to find state code
        const stateMatch = containerText.match(/\b([A-Z]{2})\s+\d{5}\b/);
        if (stateMatch) state = stateMatch[1];

        // Try to find city, state pattern
        const cityStateMatch = containerText.match(/([A-Za-z\s'-]+),\s*([A-Z]{2})(?:\s+\d{5})?/);
        if (cityStateMatch) {
          city = cityStateMatch[1].trim();
          state = cityStateMatch[2];
        }
      }

      results.push({
        firm_name: firmName,
        phone,
        city,
        state,
        address,
        profile_url: profileUrl,
      });
    }

    return results;
  }

  // Parse each result card
  $results.each((i, el) => {
    const $el = $(el);

    // Business name — look for header links
    let firmName = '';
    let profileUrl = '';

    const nameSelectors = [
      'h2 a', 'h3 a', 'h4 a', '.name a', '.business-name a',
      '.company-name a', '.title a', 'a.business-name', 'a.company-name',
      'a[href*="company"]', 'a[href*="business"]',
    ];

    for (const sel of nameSelectors) {
      const $nameLink = $el.find(sel).first();
      if ($nameLink.length) {
        firmName = $nameLink.text().trim();
        profileUrl = $nameLink.attr('href') || '';
        break;
      }
    }

    if (!firmName) {
      // Try just the header text
      const $header = $el.find('h2, h3, h4').first();
      if ($header.length) firmName = $header.text().trim();
    }

    if (!firmName || firmName.length < 2) return; // Skip empty entries

    // Make profile URL absolute
    if (profileUrl && !profileUrl.startsWith('http')) {
      profileUrl = `https://www.cylex.us.com${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`;
    }

    // Phone number
    let phone = '';
    const phoneSelectors = [
      '.phone', '.telephone', '.tel', '[data-phone]',
      'a[href^="tel:"]', '.contact-phone', '.phone-number',
    ];
    for (const sel of phoneSelectors) {
      const $phone = $el.find(sel).first();
      if ($phone.length) {
        phone = $phone.attr('data-phone') || $phone.text().trim();
        break;
      }
    }
    if (!phone) {
      // Regex fallback on full card text
      const text = $el.text();
      const phoneMatch = text.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
      if (phoneMatch) phone = phoneMatch[0];
    }

    // Address
    let address = '';
    let city = '';
    let state = '';
    let zip = '';

    const addrSelectors = [
      '.address', '.location', '.adr', '.street-address',
      '.business-address', '[itemprop="address"]',
    ];
    for (const sel of addrSelectors) {
      const $addr = $el.find(sel).first();
      if ($addr.length) {
        address = $addr.text().trim().replace(/\s+/g, ' ');
        break;
      }
    }

    // Parse city/state from address text or card text
    const fullText = address || $el.text();
    const cityStateZipMatch = fullText.match(/([A-Za-z\s'-]+),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?/);
    if (cityStateZipMatch) {
      city = cityStateZipMatch[1].trim();
      state = cityStateZipMatch[2];
      zip = (cityStateZipMatch[3] || '').trim();
    }

    results.push({
      firm_name: firmName,
      phone,
      address,
      city,
      state,
      zip,
      profile_url: profileUrl,
    });
  });

  return results;
}

/**
 * Check if there is a next page link on the search results.
 * Returns true if a "next" or numbered next page link exists.
 */
function hasNextPage($, currentPage) {
  // Look for pagination links
  const paginationSelectors = [
    '.pagination', '.pager', '.page-navigation', 'nav[aria-label="pagination"]',
    '.pages', '.page-numbers',
  ];

  for (const sel of paginationSelectors) {
    const $pag = $(sel);
    if ($pag.length) {
      // Check for "Next" link
      const hasNext = $pag.find('a').toArray().some(a => {
        const text = $(a).text().trim().toLowerCase();
        return text === 'next' || text === 'next page' || text === '>' || text === '>>';
      });
      if (hasNext) return true;

      // Check for a page number link greater than current page
      const hasHigherPage = $pag.find('a').toArray().some(a => {
        const href = $(a).attr('href') || '';
        const pageMatch = href.match(/[?&]p=(\d+)/);
        if (pageMatch) return parseInt(pageMatch[1]) > currentPage;
        return false;
      });
      if (hasHigherPage) return true;
    }
  }

  // Fallback: check for any link with p=nextPage in the whole document
  const nextPageUrl = `p=${currentPage + 1}`;
  const hasLink = $(`a[href*="${nextPageUrl}"]`).length > 0;
  return hasLink;
}

// ── Profile Page Parser ───────────────────────────────────────────────────────

/**
 * Parse a Cylex business profile page for detailed info.
 * Returns an object with any additional fields found.
 */
function parseProfilePage($) {
  const detail = {
    phone: '',
    email: '',
    website: '',
    address: '',
    city: '',
    state: '',
    zip: '',
    description: '',
  };

  // Website
  const websiteSelectors = [
    'a[href][rel="nofollow noopener"]',
    'a.website-link',
    'a[data-website]',
    'a[href*="redirect"]',
    '.website a',
    '[itemprop="url"]',
  ];
  for (const sel of websiteSelectors) {
    const $ws = $(sel).first();
    if ($ws.length) {
      let href = $ws.attr('href') || '';
      // Some directories use redirect links — try to extract the real URL
      const redirectMatch = href.match(/[?&](?:url|redirect|to|link)=([^&]+)/i);
      if (redirectMatch) {
        try { href = decodeURIComponent(redirectMatch[1]); } catch {}
      }
      if (href && href.startsWith('http') && !href.includes('cylex')) {
        detail.website = href;
        break;
      }
      // Also check the visible text in case it's a URL
      const text = $ws.text().trim();
      if (text && /^(https?:\/\/|www\.)/i.test(text)) {
        detail.website = text.startsWith('http') ? text : `https://${text}`;
        break;
      }
    }
  }

  // Also look for website in schema.org structured data
  $('script[type="application/ld+json"]').each((i, el) => {
    if (detail.website) return;
    try {
      const json = JSON.parse($(el).html());
      if (json.url && !json.url.includes('cylex')) {
        detail.website = json.url;
      }
    } catch {}
  });

  // Email
  const emailLink = $('a[href^="mailto:"]').first();
  if (emailLink.length) {
    detail.email = emailLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
  }
  if (!detail.email) {
    // Look for email patterns in the page text
    const bodyText = $('body').text();
    const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
    if (emailMatch) {
      const candidate = emailMatch[0].toLowerCase();
      // Skip cylex's own emails
      if (!candidate.includes('cylex') && !candidate.includes('example')) {
        detail.email = candidate;
      }
    }
  }

  // Phone
  const phoneSelectors = [
    '.phone', '.telephone', '.tel', '[itemprop="telephone"]',
    'a[href^="tel:"]', '.contact-phone',
  ];
  for (const sel of phoneSelectors) {
    const $ph = $(sel).first();
    if ($ph.length) {
      const rawPhone = $ph.attr('href')
        ? $ph.attr('href').replace('tel:', '').trim()
        : $ph.text().trim();
      if (rawPhone && /\d{7,}/.test(rawPhone.replace(/\D/g, ''))) {
        detail.phone = rawPhone;
        break;
      }
    }
  }
  if (!detail.phone) {
    // Regex fallback
    const bodyText = $('body').text();
    const phoneMatch = bodyText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    if (phoneMatch) detail.phone = phoneMatch[0];
  }

  // Address — structured data
  const $street = $('[itemprop="streetAddress"]').first();
  const $locality = $('[itemprop="addressLocality"]').first();
  const $region = $('[itemprop="addressRegion"]').first();
  const $postal = $('[itemprop="postalCode"]').first();

  if ($street.length) detail.address = $street.text().trim();
  if ($locality.length) detail.city = $locality.text().trim();
  if ($region.length) detail.state = $region.text().trim();
  if ($postal.length) detail.zip = $postal.text().trim();

  // Fallback: parse address from page text
  if (!detail.city) {
    const addrSelectors = ['.address', '.location', '.adr', '.business-address'];
    for (const sel of addrSelectors) {
      const $addr = $(sel).first();
      if ($addr.length) {
        const text = $addr.text().trim().replace(/\s+/g, ' ');
        const match = text.match(/([A-Za-z\s'-]+),\s*([A-Z]{2})\s*(\d{5})?/);
        if (match) {
          detail.city = match[1].trim();
          detail.state = match[2];
          if (match[3]) detail.zip = match[3];
        }
        if (!detail.address) detail.address = text;
        break;
      }
    }
  }

  // JSON-LD structured data fallback
  $('script[type="application/ld+json"]').each((i, el) => {
    try {
      const json = JSON.parse($(el).html());
      if (json['@type'] === 'LocalBusiness' || json['@type'] === 'Organization' || json['@type'] === 'ProfessionalService') {
        if (!detail.phone && json.telephone) {
          detail.phone = Array.isArray(json.telephone) ? json.telephone[0] : json.telephone;
        }
        if (!detail.email && json.email) {
          const em = Array.isArray(json.email) ? json.email[0] : json.email;
          if (!em.includes('cylex')) detail.email = em.toLowerCase();
        }
        if (!detail.website && json.url && !json.url.includes('cylex')) {
          detail.website = json.url;
        }
        if (json.address) {
          const addr = json.address;
          if (!detail.city && addr.addressLocality) detail.city = addr.addressLocality;
          if (!detail.state && addr.addressRegion) detail.state = addr.addressRegion;
          if (!detail.zip && addr.postalCode) detail.zip = addr.postalCode;
          if (!detail.address && addr.streetAddress) detail.address = addr.streetAddress;
        }
      }
    } catch {}
  });

  // Description
  const descSelectors = [
    '.description', '.business-description', '.about', '[itemprop="description"]',
    '.company-description', '.info-text',
  ];
  for (const sel of descSelectors) {
    const $desc = $(sel).first();
    if ($desc.length) {
      detail.description = $desc.text().trim().substring(0, 500);
      break;
    }
  }

  return detail;
}

// ── US State Code Normalization ───────────────────────────────────────────────

const STATE_NAMES_TO_CODES = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'district of columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI',
  'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
  'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME',
  'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
  'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
  'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM',
  'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
  'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI',
  'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX',
  'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
  'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
};

const VALID_STATE_CODES = new Set(Object.values(STATE_NAMES_TO_CODES));

function normalizeState(rawState) {
  if (!rawState) return '';
  const trimmed = rawState.trim();
  if (trimmed.length === 2 && VALID_STATE_CODES.has(trimmed.toUpperCase())) {
    return trimmed.toUpperCase();
  }
  return STATE_NAMES_TO_CODES[trimmed.toLowerCase()] || trimmed;
}

// ── Progress / Resume ─────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedSearches: [], leads: [] };
}

function saveProgress(completedSearches, leads) {
  const dir = path.dirname(PROGRESS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedSearches,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Determine which search terms to use
  let searchTerms = [...SEARCH_TERMS];
  if (SINGLE_TERM) {
    searchTerms = [SINGLE_TERM];
  } else if (TEST_MODE) {
    searchTerms = searchTerms.slice(0, 2);
  }

  const maxPages = TEST_MODE ? 2 : MAX_PAGES;
  const skipProfiles = NO_PROFILES || TEST_MODE;

  let allLeads = [];
  let completedSearches = [];
  const seenKeys = new Set();

  // Resume support
  if (RESUME) {
    const progress = loadProgress();
    if (progress.completedSearches.length > 0) {
      completedSearches = progress.completedSearches;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedSearches.length} searches done, ${allLeads.length} leads loaded`);
    }
  }

  // Build the search list: each term is a search
  const allSearches = [];
  for (const term of searchTerms) {
    if (!completedSearches.includes(term)) {
      allSearches.push(term);
    }
  }

  log('='.repeat(60));
  log('Cylex.us College Admissions Consultants Scraper');
  log('='.repeat(60));
  log(`Search terms: ${searchTerms.length}`);
  log(`Searches remaining: ${allSearches.length}`);
  log(`Max pages per term: ${maxPages}`);
  log(`Profile enrichment: ${skipProfiles ? 'OFF' : 'ON'}`);
  log(`Test mode: ${TEST_MODE ? 'YES' : 'NO'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  let totalSearches = 0;
  let totalProfilesFetched = 0;
  let totalProfileErrors = 0;
  let totalNew = 0;
  let totalDups = 0;
  let newSinceLastSave = 0;
  let errors = 0;

  for (const term of allSearches) {
    totalSearches++;
    log(`[${totalSearches}/${allSearches.length}] Searching: "${term}"`);

    let page = 1;
    let termNew = 0;
    let termTotal = 0;
    let consecutiveEmptyPages = 0;

    while (page <= maxPages) {
      const searchUrl = buildSearchUrl(term, page);
      log(`  Page ${page}: ${searchUrl}`);

      const resp = await fetchPage(searchUrl);

      if (!resp) {
        log(`  Failed to fetch page ${page} — skipping`);
        errors++;
        break;
      }

      // Check for rate limiting or blocking indicators in the body
      if (resp.body.includes('captcha') || resp.body.includes('CAPTCHA') ||
          resp.body.includes('robot') || resp.body.includes('blocked')) {
        log(`  Possible CAPTCHA/block detected on page ${page} — backing off 30s`);
        await sleep(30000);
        // Retry once
        const retryResp = await fetchPage(searchUrl);
        if (!retryResp || retryResp.body.includes('captcha') || retryResp.body.includes('CAPTCHA')) {
          log(`  Still blocked — skipping remainder of this search term`);
          errors++;
          break;
        }
        // Use retry response
        Object.assign(resp, retryResp);
      }

      const $ = cheerio.load(resp.body);

      // Parse search results
      const listings = parseSearchResults($);

      if (listings.length === 0) {
        consecutiveEmptyPages++;
        if (page === 1) {
          log(`  No results found for "${term}"`);
        } else {
          log(`  Page ${page}: 0 results (end of results)`);
        }
        // If 2 consecutive empty pages, stop paginating
        if (consecutiveEmptyPages >= 2) break;
        // Still try the next page in case of a parsing glitch
        page++;
        await randomDelay();
        continue;
      }

      consecutiveEmptyPages = 0;
      termTotal += listings.length;
      log(`  Page ${page}: ${listings.length} listings found`);

      // Process each listing
      const newListings = [];
      for (const listing of listings) {
        const key = dedupKey(listing.firm_name, listing.city);
        if (seenKeys.has(key)) {
          totalDups++;
          continue;
        }
        seenKeys.add(key);
        newListings.push(listing);
      }

      // Fetch profile pages for new listings if enabled
      for (let li = 0; li < newListings.length; li++) {
        const listing = newListings[li];

        // Profile page enrichment
        if (!skipProfiles && listing.profile_url) {
          await randomDelay(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX);
          try {
            const profileResp = await fetchPage(listing.profile_url);
            if (profileResp) {
              const $profile = cheerio.load(profileResp.body);
              const detail = parseProfilePage($profile);
              totalProfilesFetched++;

              // Merge profile data — profile takes precedence for detailed fields
              if (detail.email) listing.email = detail.email;
              if (detail.website) listing.website = detail.website;
              if (detail.phone) listing.phone = detail.phone;
              if (detail.city) listing.city = detail.city;
              if (detail.state) listing.state = detail.state;
              if (detail.address) listing.address = detail.address;
              if (detail.zip) listing.zip = detail.zip;

              if ((li + 1) % 5 === 0 || li === newListings.length - 1) {
                log(`    Profiles: ${li + 1}/${newListings.length} (${totalProfilesFetched} total, ${totalProfileErrors} errors)`);
              }
            } else {
              totalProfileErrors++;
            }
          } catch (err) {
            totalProfileErrors++;
            log(`    Profile error for ${listing.firm_name}: ${err.message}`);
          }
        }

        // Build the final lead record
        const { first_name, last_name } = parseName(listing.firm_name);
        const stateCode = normalizeState(listing.state);

        allLeads.push({
          first_name,
          last_name,
          firm_name: listing.firm_name || '',
          title: '',
          email: (listing.email || '').toLowerCase(),
          phone: formatPhone(listing.phone || ''),
          website: listing.website || '',
          domain: extractDomain(listing.website || ''),
          city: listing.city || '',
          state: stateCode,
          country: COUNTRY,
          niche: NICHE,
          source: SOURCE,
          profile_url: listing.profile_url || '',
        });

        termNew++;
        totalNew++;
        newSinceLastSave++;
      }

      // Incremental save
      if (newSinceLastSave >= INCREMENTAL_SAVE_EVERY && !TEST_MODE) {
        writeCSV(allLeads, OUT_FILE);
        log(`  [Incremental save: ${allLeads.length} total leads]`);
        newSinceLastSave = 0;
      }

      // Check for next page
      const nextExists = hasNextPage($, page);
      if (!nextExists) {
        log(`  No more pages for "${term}" (stopped at page ${page})`);
        break;
      }

      page++;
      await randomDelay();
    }

    log(`  "${term}" complete: ${termTotal} total listings, ${termNew} new leads`);

    // Mark this search term as completed
    completedSearches.push(term);

    // Save progress after each term
    if (!TEST_MODE) {
      saveProgress(completedSearches, allLeads);
      writeCSV(allLeads, OUT_FILE);
      log(`  [Progress saved: ${allLeads.length} total leads]`);
    }

    // Delay between search terms
    if (totalSearches < allSearches.length) {
      await randomDelay();
    }
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!TEST_MODE) {
    saveProgress(completedSearches, allLeads);
  }

  // ── Summary ─────────────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state).filter(Boolean)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city).filter(Boolean)).size;

  log('');
  log('='.repeat(60));
  log('SCRAPE COMPLETE');
  log('='.repeat(60));
  log(`Time elapsed:        ${elapsed}s`);
  log(`Search terms:        ${searchTerms.length}`);
  log(`Searches completed:  ${completedSearches.length}`);
  log(`Errors:              ${errors}`);
  log(`Total unique leads:  ${allLeads.length}`);
  log(`Duplicates skipped:  ${totalDups}`);
  log(`Profiles fetched:    ${totalProfilesFetched}`);
  log(`Profile errors:      ${totalProfileErrors}`);
  log(`With email:          ${withEmail} (${allLeads.length ? (100 * withEmail / allLeads.length).toFixed(1) : 0}%)`);
  log(`With phone:          ${withPhone} (${allLeads.length ? (100 * withPhone / allLeads.length).toFixed(1) : 0}%)`);
  log(`With website:        ${withWebsite} (${allLeads.length ? (100 * withWebsite / allLeads.length).toFixed(1) : 0}%)`);
  log(`With parsed name:    ${withName}`);
  log(`Unique states:       ${uniqueStates}`);
  log(`Unique cities:       ${uniqueCities}`);
  log(`Output:              ${OUT_FILE}`);
  log('='.repeat(60));

  // State breakdown
  if (allLeads.length > 0) {
    const stateCounts = {};
    for (const l of allLeads) {
      const st = l.state || '(unknown)';
      stateCounts[st] = (stateCounts[st] || 0) + 1;
    }
    const sorted = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
    log('\nTop states:');
    for (const [st, count] of sorted.slice(0, 20)) {
      log(`  ${st}: ${count}`);
    }
  }

  // Clean up progress file on successful complete run (not test mode, no errors, all terms done)
  if (!TEST_MODE && errors === 0 && allSearches.length > 0 && completedSearches.length >= searchTerms.length) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
    log('\nProgress file cleaned up (all searches completed successfully)');
  }
}

main().catch(err => {
  log(`FATAL: ${err.message}`);
  console.error(err);
  process.exit(1);
});
