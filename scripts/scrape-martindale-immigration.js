#!/usr/bin/env node
/**
 * Martindale-Hubbell Immigration Lawyers Scraper
 *
 * Scrapes https://www.martindale.com/immigration-lawyers/{city-slug}/{state-slug}/
 * using HTTP GET + JSON-LD structured data (server-rendered HTML, no anti-scraping).
 *
 * Strategy:
 *   1. For each state, iterate through a list of major cities
 *   2. For each city, fetch paginated listing pages
 *   3. Extract attorney data from JSON-LD @graph arrays (LegalService entries)
 *   4. Dedup by profile URL + name+city
 *   5. Incremental save to CSV after each state
 *
 * Data fields from JSON-LD:
 *   - name (full attorney name)
 *   - address (streetAddress, addressLocality, addressRegion, postalCode)
 *   - telephone
 *   - url (website, filtered for excluded domains)
 *   - sameAs (Martindale profile URL)
 *   - image
 *
 * Output: output/us-immigration-lawyers-martindale.csv
 *
 * Usage:
 *   node scripts/scrape-martindale-immigration.js                    # All 51 jurisdictions
 *   node scripts/scrape-martindale-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-martindale-immigration.js --test             # Test mode (2 states, 2 pages)
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const BASE_URL = 'https://www.martindale.com';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-martindale.csv');
const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const DELAY_BETWEEN_STATES = 5000;
const MAX_PAGES_PER_CITY = 20;  // Safety cap
const MAX_CONSECUTIVE_EMPTY = 2;
const MAX_BLOCK_RETRIES = 3;
const REQUEST_TIMEOUT = 30000;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// User agents for rotation
const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
];

// Domains to exclude when capturing website URLs (directories, social media, etc.)
const EXCLUDED_DOMAINS = [
  'martindale.com', 'lawyers.com', 'avvo.com', 'justia.com', 'findlaw.com',
  'superlawyers.com', 'nolo.com', 'lawinfo.com', 'hg.org',
  'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
  'youtube.com', 'yelp.com', 'google.com', 'bbb.org',
];

// ── State → Cities mapping ──────────────────────────────────────────────────
// Major cities per state, ordered by population / immigration market size

const STATE_ABBREV_TO_NAME = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas',
  CA: 'California', CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware',
  DC: 'District of Columbia', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii',
  ID: 'Idaho', IL: 'Illinois', IN: 'Indiana', IA: 'Iowa',
  KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine',
  MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota',
  MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska',
  NV: 'Nevada', NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico',
  NY: 'New York', NC: 'North Carolina', ND: 'North Dakota', OH: 'Ohio',
  OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island',
  SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas',
  UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington',
  WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
};

const STATE_CITIES = {
  AL: ['Birmingham', 'Montgomery', 'Huntsville', 'Mobile'],
  AK: ['Anchorage', 'Fairbanks', 'Juneau'],
  AZ: ['Phoenix', 'Tucson', 'Mesa', 'Scottsdale', 'Chandler'],
  AR: ['Little Rock', 'Fort Smith', 'Fayetteville'],
  CA: ['Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Sacramento', 'Oakland', 'Fresno', 'Irvine', 'Santa Ana', 'Pasadena'],
  CO: ['Denver', 'Colorado Springs', 'Aurora', 'Boulder'],
  CT: ['Hartford', 'New Haven', 'Stamford', 'Bridgeport'],
  DE: ['Wilmington', 'Dover', 'Newark'],
  DC: ['Washington'],
  FL: ['Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Fort Lauderdale', 'West Palm Beach', 'St Petersburg'],
  GA: ['Atlanta', 'Savannah', 'Augusta', 'Columbus'],
  HI: ['Honolulu'],
  ID: ['Boise', 'Nampa', 'Idaho Falls'],
  IL: ['Chicago', 'Springfield', 'Naperville', 'Rockford'],
  IN: ['Indianapolis', 'Fort Wayne', 'Evansville'],
  IA: ['Des Moines', 'Cedar Rapids', 'Davenport'],
  KS: ['Wichita', 'Kansas City', 'Topeka', 'Overland Park'],
  KY: ['Louisville', 'Lexington', 'Bowling Green'],
  LA: ['New Orleans', 'Baton Rouge', 'Shreveport'],
  ME: ['Portland', 'Augusta', 'Bangor'],
  MD: ['Baltimore', 'Rockville', 'Silver Spring', 'Bethesda'],
  MA: ['Boston', 'Worcester', 'Springfield', 'Cambridge'],
  MI: ['Detroit', 'Grand Rapids', 'Ann Arbor', 'Lansing'],
  MN: ['Minneapolis', 'Saint Paul', 'Rochester'],
  MS: ['Jackson', 'Gulfport', 'Hattiesburg'],
  MO: ['Kansas City', 'St Louis', 'Springfield'],
  MT: ['Billings', 'Missoula', 'Great Falls'],
  NE: ['Omaha', 'Lincoln'],
  NV: ['Las Vegas', 'Reno', 'Henderson'],
  NH: ['Manchester', 'Nashua', 'Concord'],
  NJ: ['Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Trenton'],
  NM: ['Albuquerque', 'Santa Fe', 'Las Cruces'],
  NY: ['New York', 'Buffalo', 'Rochester', 'Albany', 'White Plains', 'Syracuse'],
  NC: ['Charlotte', 'Raleigh', 'Durham', 'Greensboro'],
  ND: ['Fargo', 'Bismarck', 'Grand Forks'],
  OH: ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron'],
  OK: ['Oklahoma City', 'Tulsa', 'Norman'],
  OR: ['Portland', 'Salem', 'Eugene'],
  PA: ['Philadelphia', 'Pittsburgh', 'Harrisburg', 'Allentown'],
  RI: ['Providence', 'Warwick', 'Cranston'],
  SC: ['Charleston', 'Columbia', 'Greenville'],
  SD: ['Sioux Falls', 'Rapid City'],
  TN: ['Nashville', 'Memphis', 'Knoxville', 'Chattanooga'],
  TX: ['Houston', 'Dallas', 'San Antonio', 'Austin', 'Fort Worth', 'El Paso', 'McAllen', 'Laredo'],
  UT: ['Salt Lake City', 'Provo', 'Ogden'],
  VT: ['Burlington', 'Montpelier'],
  VA: ['Arlington', 'Richmond', 'Virginia Beach', 'Alexandria', 'Fairfax'],
  WA: ['Seattle', 'Tacoma', 'Spokane', 'Bellevue'],
  WV: ['Charleston', 'Huntington', 'Morgantown'],
  WI: ['Milwaukee', 'Madison', 'Green Bay'],
  WY: ['Cheyenne', 'Casper', 'Laramie'],
};

// Ordered by immigration market size
const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function getRandomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function slugify(str) {
  return (str || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function isExcludedDomain(url) {
  if (!url) return false;
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    return EXCLUDED_DOMAINS.some(d => hostname.includes(d));
  } catch {
    return false;
  }
}

function extractDomain(url) {
  if (!url) return '';
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  // Remove common suffixes
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|ph\.?d\.?|m\.?d\.?|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = cleaned.split(' ').filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts[parts.length - 1],
  };
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(leads, filePath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(filePath, header + '\n' + rows.join('\n') + '\n');
}

// ── HTTP Client ─────────────────────────────────────────────────────────────

function httpGet(url, retries = 2) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const ua = getRandomUA();
    const req = mod.get(url, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
        'Referer': 'https://www.martindale.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1',
      },
      timeout: REQUEST_TIMEOUT,
    }, (res) => {
      // Handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) {
          redirect = `${BASE_URL}${redirect}`;
        }
        return resolve(httpGet(redirect, retries));
      }
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });
    req.on('error', (err) => {
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(err);
      }
    });
    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── JSON-LD Parsing ─────────────────────────────────────────────────────────

/**
 * Extract attorney entries from JSON-LD structured data in the HTML.
 * Martindale embeds <script type="application/ld+json"> with @graph arrays
 * containing LegalService/Attorney/Person entries.
 */
function parseJsonLd($, stateCode, cityName) {
  const attorneys = [];

  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const raw = $(el).html();
      if (!raw) return;
      const data = JSON.parse(raw);

      const graph = data['@graph'] || [];
      for (const entry of graph) {
        const type = entry['@type'];
        if (!type) continue;
        const types = Array.isArray(type) ? type : [type];
        const isLegal = types.some(t =>
          t === 'LegalService' || t === 'Attorney' || t === 'Lawyer' ||
          t === 'Organization' || t === 'LocalBusiness' || t === 'Person'
        );
        if (!isLegal) continue;

        const fullName = (entry.name || '').trim();
        if (!fullName) continue;

        const address = entry.address || {};
        const { first_name, last_name } = splitName(fullName);

        // sameAs, image, telephone can be arrays or strings
        const sameAs = Array.isArray(entry.sameAs) ? entry.sameAs[0] : entry.sameAs;
        const telephone = Array.isArray(entry.telephone) ? entry.telephone[0] : entry.telephone;

        // Filter website URL — exclude directories, social media
        const rawUrl = (entry.url || '').trim();
        const website = (rawUrl && !isExcludedDomain(rawUrl)) ? rawUrl : '';

        attorneys.push({
          first_name,
          last_name,
          firm_name: '',  // JSON-LD doesn't clearly separate firm from attorney name
          title: '',
          email: '',      // Not available in Martindale JSON-LD
          phone: (telephone || '').trim(),
          website,
          domain: extractDomain(website),
          city: (address.addressLocality || cityName || '').trim(),
          state: (address.addressRegion || stateCode || '').trim(),
          country: 'US',
          niche: 'immigration',
          source: 'martindale_immigration',
          profile_url: (sameAs || '').trim(),
        });
      }
    } catch (err) {
      // JSON parse error — skip this script block
    }
  });

  return attorneys;
}

/**
 * Check if there's a next page link in the pagination.
 */
function hasNextPage($, currentPage) {
  const nextPage = currentPage + 1;

  // Check for pagination links with ?page=N
  let found = false;
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (href.includes(`page=${nextPage}`)) {
      found = true;
      return false; // break
    }
  });
  if (found) return true;

  // Check for "next" link patterns
  const nextLink = $('a[rel="next"], a.next, a:contains("Next"), a:contains("next")');
  if (nextLink.length > 0) return true;

  return false;
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

/**
 * Scrape all pages of immigration lawyers for a given city+state on Martindale.
 * Returns array of attorney records.
 */
async function scrapeCity(stateCode, cityName, maxPages) {
  const stateName = STATE_ABBREV_TO_NAME[stateCode];
  if (!stateName) return [];

  const stateSlug = slugify(stateName);
  const citySlug = slugify(cityName);
  const results = [];

  let page = 1;
  let consecutiveEmpty = 0;
  let blockRetries = 0;

  while (page <= maxPages) {
    const url = page === 1
      ? `${BASE_URL}/immigration-lawyers/${citySlug}/${stateSlug}/`
      : `${BASE_URL}/immigration-lawyers/${citySlug}/${stateSlug}/?page=${page}`;

    let response;
    try {
      await randomDelay();
      response = await httpGet(url);
    } catch (err) {
      log(`    ERROR fetching ${cityName} page ${page}: ${err.message}`);
      break;
    }

    // Handle rate limiting / blocking
    if (response.statusCode === 429 || response.statusCode === 403) {
      blockRetries++;
      if (blockRetries > MAX_BLOCK_RETRIES) {
        log(`    BLOCKED on ${cityName} (${blockRetries} retries) — skipping city`);
        break;
      }
      const backoffMs = 10000 * Math.pow(2, blockRetries - 1); // 10s, 20s, 40s
      log(`    Rate limited (${response.statusCode}) on ${cityName} page ${page} — backing off ${backoffMs / 1000}s`);
      await sleep(backoffMs);
      continue; // Retry same page
    }

    // 404 means no results or past last page
    if (response.statusCode === 404) {
      if (page === 1) {
        // No immigration lawyers listed for this city
      }
      break;
    }

    if (response.statusCode !== 200) {
      log(`    Unexpected status ${response.statusCode} for ${cityName} page ${page}`);
      break;
    }

    // Reset block counter on success
    blockRetries = 0;

    // Parse HTML and extract JSON-LD
    const $ = cheerio.load(response.body);
    const attorneys = parseJsonLd($, stateCode, cityName);

    if (attorneys.length === 0) {
      consecutiveEmpty++;
      if (consecutiveEmpty >= MAX_CONSECUTIVE_EMPTY) {
        break;
      }
      page++;
      continue;
    }

    consecutiveEmpty = 0;
    results.push(...attorneys);

    // Check for next page
    if (!hasNextPage($, page)) {
      break;
    }

    page++;
  }

  return results;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');

  // Parse --states flag
  let statesFilter = null;
  const statesIdx = args.indexOf('--states');
  if (statesIdx >= 0 && args[statesIdx + 1]) {
    statesFilter = args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase());
  }

  let states;
  if (statesFilter) {
    states = statesFilter;
  } else if (isTest) {
    states = ['WY', 'VT']; // Small states for quick testing
  } else {
    states = ALL_STATES;
  }

  const maxPagesPerCity = isTest ? 2 : MAX_PAGES_PER_CITY;

  log('=== Martindale Immigration Lawyers Scraper ===');
  log(`States: ${states.length} (${states.join(', ')})`);
  log(`Test mode: ${isTest}`);
  log(`Max pages per city: ${maxPagesPerCity}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const allLeads = [];
  const seenKeys = new Set(); // Dedup by profile_url or name+city+state

  for (let si = 0; si < states.length; si++) {
    const stateCode = states[si];
    const cities = STATE_CITIES[stateCode] || [];

    log(`\n[${'='.repeat(50)}]`);
    log(`State ${si + 1}/${states.length}: ${stateCode} (${STATE_ABBREV_TO_NAME[stateCode]}) — ${cities.length} cities`);

    let stateLeadCount = 0;
    let stateNewCount = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const cityName = cities[ci];
      log(`  City ${ci + 1}/${cities.length}: ${cityName}, ${stateCode}`);

      const attorneys = await scrapeCity(stateCode, cityName, maxPagesPerCity);

      let cityNew = 0;
      for (const attorney of attorneys) {
        // Build dedup key
        const profileKey = attorney.profile_url
          ? `url:${attorney.profile_url}`
          : '';
        const nameKey = `name:${(attorney.first_name + ' ' + attorney.last_name).toLowerCase().trim()}|${attorney.city.toLowerCase()}|${attorney.state.toUpperCase()}`;

        if (profileKey && seenKeys.has(profileKey)) continue;
        if (seenKeys.has(nameKey)) continue;

        if (profileKey) seenKeys.add(profileKey);
        seenKeys.add(nameKey);

        allLeads.push(attorney);
        cityNew++;
        stateNewCount++;
      }

      stateLeadCount += attorneys.length;
      log(`    ${cityName}: ${attorneys.length} found, ${cityNew} new (deduped)`);
    }

    log(`  ${stateCode} total: ${stateLeadCount} found, ${stateNewCount} new unique`);
    log(`  Running total: ${allLeads.length} unique leads`);

    // Incremental save after each state
    writeCSV(allLeads, OUTPUT_FILE);

    // Delay between states
    if (si < states.length - 1) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 2000);
    }
  }

  // Final stats
  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${states.length}`);
  log(`Leads with phone: ${allLeads.filter(l => l.phone).length}`);
  log(`Leads with website: ${allLeads.filter(l => l.website).length}`);
  log(`Leads with profile URL: ${allLeads.filter(l => l.profile_url).length}`);
  log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
