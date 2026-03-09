#!/usr/bin/env node
/**
 * Lawzana Immigration Lawyer Scraper (US + UK)
 *
 * Scrapes immigration lawyers from Lawzana.com for the United States and
 * United Kingdom. Uses the same JSON-LD approach that worked for AU scraping
 * (37 leads from the AU extra immigration scraper).
 *
 * Approach:
 *   1. Hit state-level (US) or city-level (UK) listing pages
 *   2. Parse JSON-LD ItemList for firm profile URLs
 *   3. Follow pagination (?page=N)
 *   4. Discover sub-city pages from each listing and scrape those too
 *   5. Visit each firm profile page for JSON-LD LegalService data
 *      (phone, email, address, website via sameAs)
 *
 * URL patterns:
 *   Listing:  https://lawzana.com/immigration-lawyers/{slug}
 *   Profile:  https://lawzana.com/lawyer/{firm-slug}/{city}
 *
 * Usage:
 *   node scripts/scrape-lawzana-immigration.js --test              # 2 US states + 2 UK cities
 *   node scripts/scrape-lawzana-immigration.js --all               # Full scrape (US + UK)
 *   node scripts/scrape-lawzana-immigration.js --all --country US  # US only
 *   node scripts/scrape-lawzana-immigration.js --all --country UK  # UK only
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const MAX_PAGES_PER_LISTING = 20;  // Safety cap for pagination

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-lawzana.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── US State slugs (all 50 + DC) ────────────────────────────────────────────
// These are used as the top-level listing pages. Sub-city pages are discovered
// automatically from each state listing.

const US_STATES = [
  { slug: 'alabama', state: 'AL' },
  { slug: 'alaska', state: 'AK' },
  { slug: 'arizona', state: 'AZ' },
  { slug: 'arkansas', state: 'AR' },
  { slug: 'california', state: 'CA' },
  { slug: 'colorado', state: 'CO' },
  { slug: 'connecticut', state: 'CT' },
  { slug: 'delaware', state: 'DE' },
  { slug: 'district-of-columbia', state: 'DC' },
  { slug: 'florida', state: 'FL' },
  { slug: 'georgia', state: 'GA' },
  { slug: 'hawaii', state: 'HI' },
  { slug: 'idaho', state: 'ID' },
  { slug: 'illinois', state: 'IL' },
  { slug: 'indiana', state: 'IN' },
  { slug: 'iowa', state: 'IA' },
  { slug: 'kansas', state: 'KS' },
  { slug: 'kentucky', state: 'KY' },
  { slug: 'louisiana', state: 'LA' },
  { slug: 'maine', state: 'ME' },
  { slug: 'maryland', state: 'MD' },
  { slug: 'massachusetts', state: 'MA' },
  { slug: 'michigan', state: 'MI' },
  { slug: 'minnesota', state: 'MN' },
  { slug: 'mississippi', state: 'MS' },
  { slug: 'missouri', state: 'MO' },
  { slug: 'montana', state: 'MT' },
  { slug: 'nebraska', state: 'NE' },
  { slug: 'nevada', state: 'NV' },
  { slug: 'new-hampshire', state: 'NH' },
  { slug: 'new-jersey', state: 'NJ' },
  { slug: 'new-mexico', state: 'NM' },
  { slug: 'new-york', state: 'NY' },
  { slug: 'north-carolina', state: 'NC' },
  { slug: 'north-dakota', state: 'ND' },
  { slug: 'ohio', state: 'OH' },
  { slug: 'oklahoma', state: 'OK' },
  { slug: 'oregon', state: 'OR' },
  { slug: 'pennsylvania', state: 'PA' },
  { slug: 'rhode-island', state: 'RI' },
  { slug: 'south-carolina', state: 'SC' },
  { slug: 'south-dakota', state: 'SD' },
  { slug: 'tennessee', state: 'TN' },
  { slug: 'texas', state: 'TX' },
  { slug: 'utah', state: 'UT' },
  { slug: 'vermont', state: 'VT' },
  { slug: 'virginia', state: 'VA' },
  { slug: 'washington', state: 'WA' },
  { slug: 'west-virginia', state: 'WV' },
  { slug: 'wisconsin', state: 'WI' },
  { slug: 'wyoming', state: 'WY' },
];

// US state slug → state code mapping for addressRegion resolution
const US_STATE_NAME_MAP = {};
for (const s of US_STATES) {
  US_STATE_NAME_MAP[s.slug.replace(/-/g, ' ')] = s.state;
}
// Also add full names
Object.assign(US_STATE_NAME_MAP, {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
  'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
  'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
  'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
  'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
  'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
  'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
  'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
  'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
  'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
  'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
});

// ── UK city slugs ────────────────────────────────────────────────────────────
// Discovered from /immigration-lawyers/united-kingdom (all 6 pages)

const UK_CITIES = [
  { slug: 'london-england', city: 'London', region: 'England' },
  { slug: 'birmingham-birmingham', city: 'Birmingham', region: 'England' },
  { slug: 'manchester-manchester', city: 'Manchester', region: 'England' },
  { slug: 'leeds-kent', city: 'Leeds', region: 'England' },
  { slug: 'glasgow-city-scotland', city: 'Glasgow', region: 'Scotland' },
  { slug: 'edinburgh-edinburgh', city: 'Edinburgh', region: 'Scotland' },
  { slug: 'bristol-bristol', city: 'Bristol', region: 'England' },
  { slug: 'sheffield-sheffield', city: 'Sheffield', region: 'England' },
  { slug: 'newcastle-upon-tyne', city: 'Newcastle upon Tyne', region: 'England' },
  { slug: 'nottingham-nottingham', city: 'Nottingham', region: 'England' },
  { slug: 'cardiff-cardiff', city: 'Cardiff', region: 'Wales' },
  { slug: 'belfast-belfast', city: 'Belfast', region: 'Northern Ireland' },
  { slug: 'leicester-leicester', city: 'Leicester', region: 'England' },
  { slug: 'coventry-coventry', city: 'Coventry', region: 'England' },
  { slug: 'bradford-bradford', city: 'Bradford', region: 'England' },
  { slug: 'brighton-brighton-and-hove', city: 'Brighton', region: 'England' },
  { slug: 'bournemouth', city: 'Bournemouth', region: 'England' },
  { slug: 'portsmouth-portsmouth', city: 'Portsmouth', region: 'England' },
  { slug: 'southampton-southampton', city: 'Southampton', region: 'England' },
  { slug: 'derby-derby', city: 'Derby', region: 'England' },
  { slug: 'peterborough-peterborough', city: 'Peterborough', region: 'England' },
  { slug: 'oxford-oxfordshire', city: 'Oxford', region: 'England' },
  { slug: 'reading-reading', city: 'Reading', region: 'England' },
  { slug: 'luton', city: 'Luton', region: 'England' },
  { slug: 'stoke-on-trent', city: 'Stoke-on-Trent', region: 'England' },
  { slug: 'chester-cheshire', city: 'Chester', region: 'England' },
  { slug: 'chelmsford-essex', city: 'Chelmsford', region: 'England' },
  { slug: 'maidstone-kent', city: 'Maidstone', region: 'England' },
  { slug: 'aberdeen-aberdeen-city', city: 'Aberdeen', region: 'Scotland' },
  { slug: 'inverness-highland', city: 'Inverness', region: 'Scotland' },
  { slug: 'doncaster-doncaster', city: 'Doncaster', region: 'England' },
  { slug: 'oldham-oldham', city: 'Oldham', region: 'England' },
  { slug: 'gateshead', city: 'Gateshead', region: 'England' },
  { slug: 'east-ham', city: 'East Ham', region: 'England' },
  { slug: 'mayfair', city: 'Mayfair', region: 'England' },
  { slug: 'brierley-hill', city: 'Brierley Hill', region: 'England' },
  { slug: 'bedford-bedford', city: 'Bedford', region: 'England' },
  { slug: 'weymouth-dorset', city: 'Weymouth', region: 'England' },
  { slug: 'chesterfield-derbyshire', city: 'Chesterfield', region: 'England' },
  { slug: 'royal-leamington-spa', city: 'Royal Leamington Spa', region: 'England' },
  { slug: 'high-wycombe-buckinghamshire', city: 'High Wycombe', region: 'England' },
  { slug: 'andover-hampshire', city: 'Andover', region: 'England' },
  { slug: 'london-colney', city: 'London Colney', region: 'England' },
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
  const str = String(val);
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

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function decodeHtml(str) {
  if (!str) return '';
  return str
    .replace(/&amp;amp;/g, '&')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/');
}

function cleanPhoneUS(raw) {
  if (!raw) return '';
  let digits = raw.replace(/\D/g, '');
  // Strip +1 country code
  if (digits.length === 11 && digits.startsWith('1')) {
    digits = digits.slice(1);
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return raw.trim();
}

function cleanPhoneUK(raw) {
  if (!raw) return '';
  let digits = raw.replace(/\D/g, '');
  // Strip +44 country code
  if (digits.startsWith('44') && digits.length >= 11) {
    digits = '0' + digits.slice(2);
  }
  // Format UK landline: 0XX XXXX XXXX or 0XXX XXX XXXX
  if (digits.length === 11 && digits.startsWith('0')) {
    if (digits.startsWith('02')) {
      return `${digits.slice(0, 3)} ${digits.slice(3, 7)} ${digits.slice(7)}`;
    }
    return `${digits.slice(0, 4)} ${digits.slice(4, 7)} ${digits.slice(7)}`;
  }
  return raw.trim();
}

function cleanPhone(raw, country) {
  if (!raw) return '';
  if (country === 'US') return cleanPhoneUS(raw);
  if (country === 'UK') return cleanPhoneUK(raw);
  return raw.trim();
}

function resolveState(addressRegion, fallbackState) {
  if (!addressRegion) return fallbackState || '';
  const key = addressRegion.toLowerCase().trim();
  // Direct 2-letter code
  if (/^[A-Z]{2}$/i.test(addressRegion.trim())) {
    return addressRegion.trim().toUpperCase();
  }
  return US_STATE_NAME_MAP[key] || fallbackState || addressRegion;
}

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Listing Page Parser ──────────────────────────────────────────────────────

/**
 * Parse a listing page and return:
 *   - firmUrls: array of { url, name } from JSON-LD ItemList
 *   - subCitySlugs: array of sub-city slugs discovered on the page
 *   - nextPage: number of next page if pagination exists, or null
 *   - totalItems: numberOfItems from JSON-LD
 */
function parseListingPage(body, currentSlug) {
  const $ = cheerio.load(body);
  const firmUrls = [];
  let totalItems = 0;

  // Parse JSON-LD ItemList
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());
      if (json['@type'] === 'ItemList' && json.itemListElement) {
        totalItems = json.numberOfItems || json.itemListElement.length;
        for (const item of json.itemListElement) {
          if (item.url && item.name) {
            firmUrls.push({ url: item.url, name: decodeHtml(item.name) });
          }
        }
      }
    } catch {}
  });

  // Discover sub-city page slugs (links to /immigration-lawyers/{slug} that differ from current)
  const subCitySlugs = new Set();
  const linkPattern = /href=["']\/?(?:https:\/\/lawzana\.com\/)?immigration-lawyers\/([^"'?]+)["']/g;
  let match;
  while ((match = linkPattern.exec(body)) !== null) {
    const slug = match[1];
    if (slug !== currentSlug && slug !== 'united-states' && slug !== 'united-kingdom') {
      subCitySlugs.add(slug);
    }
  }

  // Check for next page
  let nextPage = null;
  const pagePattern = /immigration-lawyers\/[^"'?]+\?page=(\d+)/g;
  let maxPage = 0;
  while ((match = pagePattern.exec(body)) !== null) {
    const p = parseInt(match[1]);
    if (p > maxPage) maxPage = p;
  }
  // The current page's "next" is determined by looking at pagination links
  // We need to know what current page is to find the next one
  if (maxPage > 0) {
    nextPage = maxPage; // Will be refined in the caller
  }

  return { firmUrls, subCitySlugs: [...subCitySlugs], nextPage, totalItems };
}

// ── Profile Page Parser ──────────────────────────────────────────────────────

function parseProfilePage(body) {
  const $ = cheerio.load(body);
  let firmData = null;

  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());
      if (json['@type'] === 'LegalService') {
        firmData = json;
      }
    } catch {}
  });

  return firmData;
}

/**
 * Extract the firm's external website from JSON-LD sameAs field.
 * Filters out social media and Lawzana's own URLs.
 */
function extractWebsite(firmData) {
  const socialPatterns = [
    'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
    'youtube.com', 'tiktok.com', 'pinterest.com', 'x.com', 'wa.me',
    'lawzana.com', 'google.com', 'yelp.com', 'avvo.com', 'justia.com',
    'findlaw.com', 'martindale.com', 'lawyers.com',
  ];

  const sameAs = firmData.sameAs;
  if (Array.isArray(sameAs)) {
    for (const s of sameAs) {
      if (typeof s !== 'string') continue;
      const url = s.startsWith('http') ? s : `https://${s}`;
      const isSocial = socialPatterns.some(p => url.toLowerCase().includes(p));
      if (!isSocial && s.includes('.')) {
        return url;
      }
    }
  } else if (typeof sameAs === 'string') {
    const isSocial = socialPatterns.some(p => sameAs.toLowerCase().includes(p));
    if (!isSocial) {
      return sameAs.startsWith('http') ? sameAs : `https://${sameAs}`;
    }
  }
  return '';
}

// ── Core Scraping Logic ──────────────────────────────────────────────────────

/**
 * Scrape a single listing page (one slug, one page number).
 * Returns array of firm profile URLs found.
 */
async function scrapeListingPage(slug, page) {
  const url = page === 1
    ? `https://lawzana.com/immigration-lawyers/${slug}`
    : `https://lawzana.com/immigration-lawyers/${slug}?page=${page}`;

  const { statusCode, body } = await httpGet(url);
  if (statusCode !== 200 || !body) {
    return { firmUrls: [], subCitySlugs: [], hasMore: false, totalItems: 0 };
  }

  const parsed = parseListingPage(body, slug);

  // Determine if there are more pages
  const hasMore = parsed.nextPage > page;

  return {
    firmUrls: parsed.firmUrls,
    subCitySlugs: parsed.subCitySlugs,
    hasMore,
    totalItems: parsed.totalItems,
  };
}

/**
 * Scrape all pages for a given slug. Returns all firm profile URLs + discovered sub-city slugs.
 */
async function scrapeAllPagesForSlug(slug, label) {
  const allFirmUrls = [];
  const allSubCitySlugs = new Set();
  const seenFirmUrls = new Set();

  for (let page = 1; page <= MAX_PAGES_PER_LISTING; page++) {
    log(`  [${label}] Page ${page}...`);

    const result = await scrapeListingPage(slug, page);

    // Add new firm URLs (dedup by URL)
    for (const firm of result.firmUrls) {
      if (!seenFirmUrls.has(firm.url)) {
        seenFirmUrls.add(firm.url);
        allFirmUrls.push(firm);
      }
    }

    // Collect sub-city slugs
    for (const s of result.subCitySlugs) {
      allSubCitySlugs.add(s);
    }

    if (result.firmUrls.length === 0 || !result.hasMore) {
      break;
    }

    await randomDelay();
  }

  return { firmUrls: allFirmUrls, subCitySlugs: [...allSubCitySlugs] };
}

/**
 * Visit firm profile pages and extract lead data.
 */
async function scrapeProfiles(firmUrls, country, fallbackState, fallbackCity, seenKeys) {
  const leads = [];

  for (let j = 0; j < firmUrls.length; j++) {
    const firm = firmUrls[j];
    const key = dedupKey(firm.name, '');
    if (seenKeys.has(key)) {
      continue;
    }

    await randomDelay();

    try {
      const { statusCode, body } = await httpGet(firm.url);
      if (statusCode !== 200 || !body) {
        log(`    [${j + 1}/${firmUrls.length}] HTTP ${statusCode}: ${firm.name}`);
        continue;
      }

      const firmData = parseProfilePage(body);
      if (!firmData) {
        log(`    [${j + 1}/${firmUrls.length}] No JSON-LD: ${firm.name}`);
        continue;
      }

      const address = firmData.address || {};
      const phone = cleanPhone(firmData.telephone || '', country);
      const email = (firmData.email || '').trim();
      const firmName = decodeHtml(firmData.name || firm.name);
      const city = address.addressLocality || fallbackCity || '';
      const website = extractWebsite(firmData);

      let state;
      if (country === 'US') {
        state = resolveState(address.addressRegion, fallbackState);
      } else {
        // UK: use addressRegion or fallback
        state = address.addressRegion || fallbackState || '';
      }

      const countryCode = country === 'US' ? 'US' : 'UK';

      // Deduplicate by firm name + city
      const fullKey = dedupKey(firmName, city);
      if (seenKeys.has(fullKey)) {
        continue;
      }
      seenKeys.add(fullKey);
      seenKeys.add(key);

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firmName,
        title: '',
        email,
        phone,
        website,
        domain: extractDomain(website),
        city,
        state,
        country: countryCode,
        niche: 'immigration',
        source: 'lawzana',
        profile_url: firm.url,
      });

      const contactInfo = [
        phone || 'no phone',
        email || 'no email',
        website ? extractDomain(website) : 'no website',
      ].join(' | ');
      log(`    [${j + 1}/${firmUrls.length}] ${firmName} (${city}) | ${contactInfo}`);

    } catch (err) {
      log(`    [${j + 1}/${firmUrls.length}] ERROR: ${err.message}`);
    }
  }

  return leads;
}

// ── Country Scrapers ─────────────────────────────────────────────────────────

async function scrapeUS(states, seenKeys) {
  const leads = [];
  log('');
  log('=== UNITED STATES ===');
  log(`States to scrape: ${states.length}`);

  const scrapedSlugs = new Set();  // Track which slugs we've already scraped

  for (let i = 0; i < states.length; i++) {
    const stateInfo = states[i];
    const label = `US ${i + 1}/${states.length} ${stateInfo.state}`;
    log(`[${label}] ${stateInfo.slug}`);

    scrapedSlugs.add(stateInfo.slug);

    try {
      // Scrape the state-level listing (with pagination)
      const { firmUrls, subCitySlugs } = await scrapeAllPagesForSlug(stateInfo.slug, label);
      log(`  Found ${firmUrls.length} firms, ${subCitySlugs.length} sub-city pages`);

      // Scrape firm profiles from state-level listing
      const stateLeads = await scrapeProfiles(firmUrls, 'US', stateInfo.state, '', seenKeys);
      leads.push(...stateLeads);

      // Now scrape each sub-city page
      const citiesToScrape = subCitySlugs.filter(s => !scrapedSlugs.has(s));
      for (let c = 0; c < citiesToScrape.length; c++) {
        const citySlug = citiesToScrape[c];
        scrapedSlugs.add(citySlug);

        await randomDelay();
        const cityLabel = `${label} city ${c + 1}/${citiesToScrape.length}`;
        log(`  [${cityLabel}] ${citySlug}`);

        try {
          const cityResult = await scrapeAllPagesForSlug(citySlug, cityLabel);

          if (cityResult.firmUrls.length > 0) {
            // Extract city name from slug (rough heuristic)
            const cityName = citySlug
              .replace(/-[a-z]+-?$/, '')  // Remove trailing state suffix like "-california"
              .split('-')
              .map(w => w.charAt(0).toUpperCase() + w.slice(1))
              .join(' ');

            const cityLeads = await scrapeProfiles(
              cityResult.firmUrls, 'US', stateInfo.state, cityName, seenKeys
            );
            leads.push(...cityLeads);
          }
        } catch (err) {
          log(`    City ERROR: ${err.message}`);
        }
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
    }

    // Progress save every 5 states
    if ((i + 1) % 5 === 0 && leads.length > 0) {
      log(`  [Progress: ${leads.length} US leads so far]`);
    }

    await randomDelay();
  }

  // Also scrape the US national page for any firms not in state pages
  log(`[US National] Scraping national listing...`);
  try {
    const { firmUrls } = await scrapeAllPagesForSlug('united-states', 'US National');
    if (firmUrls.length > 0) {
      const nationalLeads = await scrapeProfiles(firmUrls, 'US', '', '', seenKeys);
      leads.push(...nationalLeads);
      log(`  National page added ${nationalLeads.length} new leads`);
    }
  } catch (err) {
    log(`  National ERROR: ${err.message}`);
  }

  log(`US TOTAL: ${leads.length} leads`);
  return leads;
}

async function scrapeUK(cities, seenKeys) {
  const leads = [];
  log('');
  log('=== UNITED KINGDOM ===');
  log(`Cities to scrape: ${cities.length}`);

  const scrapedSlugs = new Set();

  for (let i = 0; i < cities.length; i++) {
    const cityInfo = cities[i];
    const label = `UK ${i + 1}/${cities.length} ${cityInfo.city}`;
    log(`[${label}] ${cityInfo.slug}`);

    scrapedSlugs.add(cityInfo.slug);

    try {
      const { firmUrls, subCitySlugs } = await scrapeAllPagesForSlug(cityInfo.slug, label);
      log(`  Found ${firmUrls.length} firms`);

      const cityLeads = await scrapeProfiles(
        firmUrls, 'UK', cityInfo.region, cityInfo.city, seenKeys
      );
      leads.push(...cityLeads);

    } catch (err) {
      log(`  ERROR: ${err.message}`);
    }

    // Progress save every 10 cities
    if ((i + 1) % 10 === 0 && leads.length > 0) {
      log(`  [Progress: ${leads.length} UK leads so far]`);
    }

    await randomDelay();
  }

  // Also scrape the UK national page
  log(`[UK National] Scraping national listing...`);
  try {
    const { firmUrls } = await scrapeAllPagesForSlug('united-kingdom', 'UK National');
    if (firmUrls.length > 0) {
      const nationalLeads = await scrapeProfiles(firmUrls, 'UK', '', '', seenKeys);
      leads.push(...nationalLeads);
      log(`  National page added ${nationalLeads.length} new leads`);
    }
  } catch (err) {
    log(`  National ERROR: ${err.message}`);
  }

  log(`UK TOTAL: ${leads.length} leads`);
  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const countryIdx = args.indexOf('--country');
  const countryFilter = countryIdx > -1 ? (args[countryIdx + 1] || '').toUpperCase() : 'ALL';

  if (!isTest && !isAll) {
    console.log('Lawzana Immigration Lawyer Scraper (US + UK)');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-lawzana-immigration.js --test              # Quick test (2 US states + 2 UK cities)');
    console.log('  node scripts/scrape-lawzana-immigration.js --all               # Full scrape (US + UK)');
    console.log('  node scripts/scrape-lawzana-immigration.js --all --country US  # US only');
    console.log('  node scripts/scrape-lawzana-immigration.js --all --country UK  # UK only');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  const seenKeys = new Set();
  let allLeads = [];

  const doUS = countryFilter === 'ALL' || countryFilter === 'US';
  const doUK = countryFilter === 'ALL' || countryFilter === 'UK';

  log('Lawzana Immigration Lawyer Scraper');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Countries: ${countryFilter}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // US scrape
  if (doUS) {
    const usStates = isTest ? US_STATES.slice(0, 2) : US_STATES;
    const usLeads = await scrapeUS(usStates, seenKeys);
    allLeads = allLeads.concat(usLeads);

    if (allLeads.length > 0) {
      writeCSV(allLeads, OUT_FILE);
      log(`[Progress saved: ${allLeads.length} leads]`);
    }
  }

  // UK scrape
  if (doUK) {
    const ukCities = isTest ? UK_CITIES.slice(0, 2) : UK_CITIES;
    const ukLeads = await scrapeUK(ukCities, seenKeys);
    allLeads = allLeads.concat(ukLeads);
  }

  // Final write
  if (allLeads.length > 0) {
    writeCSV(allLeads, OUT_FILE);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;
  const byCountry = {};
  const byState = {};
  for (const l of allLeads) {
    byCountry[l.country] = (byCountry[l.country] || 0) + 1;
    const key = `${l.country}-${l.state || 'unknown'}`;
    byState[key] = (byState[key] || 0) + 1;
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`Unique cities: ${uniqueCities}`);
  log('By country:');
  for (const [c, count] of Object.entries(byCountry)) {
    log(`  ${c}: ${count}`);
  }
  log('Top states/regions:');
  const sortedStates = Object.entries(byState).sort((a, b) => b[1] - a[1]).slice(0, 15);
  for (const [s, count] of sortedStates) {
    log(`  ${s}: ${count}`);
  }
  log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
