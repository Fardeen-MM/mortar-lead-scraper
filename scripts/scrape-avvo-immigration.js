#!/usr/bin/env node
/**
 * Avvo Immigration Lawyers Scraper
 *
 * Scrapes https://www.avvo.com/immigration-lawyer/{state}/{city}.html
 * using Puppeteer + Stealth (Cloudflare protection requires real browser).
 *
 * Strategy:
 *   1. For each state, iterate through major immigration cities
 *   2. Load each page with Puppeteer, wait for Cloudflare challenge
 *   3. Extract lawyer data from JSON-LD (Person schema) inside .organic-card elements
 *   4. Paginate through all result pages (20 per page)
 *   5. Dedup by profile URL
 *   6. Incremental save to CSV after each state
 *   7. Resume support — skips states already in CSV
 *
 * Data fields from Avvo JSON-LD:
 *   - name (Person)
 *   - worksFor.name (firm)
 *   - worksFor.telephone (phone)
 *   - worksFor.address (city, state, zip, street)
 *   - alumniOf (education)
 *   - @id / url (profile URL)
 *
 * Output: output/us-immigration-lawyers-avvo.csv
 *
 * Usage:
 *   node scripts/scrape-avvo-immigration.js                    # Default 15 immigration states
 *   node scripts/scrape-avvo-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-avvo-immigration.js --all-states       # All 51 states
 *   node scripts/scrape-avvo-immigration.js --test             # Test mode (2 states, 2 cities, 2 pages each)
 *   node scripts/scrape-avvo-immigration.js --no-resume        # Ignore existing CSV, start fresh
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const DELAY_BETWEEN_STATES = 5000;
const CLOUDFLARE_WAIT = 10000;
const MAX_CLOUDFLARE_RETRIES = 3;
const NAV_TIMEOUT = 45000;
const RESULTS_PER_PAGE = 20;
const MAX_PAGES_DEFAULT = 25;    // 500 lawyers per city max

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-avvo.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Default immigration-heavy states
const DEFAULT_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC',
];

// All 51 (50 states + DC) ordered by immigration market size
const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

// State code → lowercase abbreviation for Avvo URLs
const STATE_ABBR = {
  AL: 'al', AK: 'ak', AZ: 'az', AR: 'ar', CA: 'ca', CO: 'co', CT: 'ct',
  DE: 'de', DC: 'dc', FL: 'fl', GA: 'ga', HI: 'hi', ID: 'id', IL: 'il',
  IN: 'in', IA: 'ia', KS: 'ks', KY: 'ky', LA: 'la', ME: 'me', MD: 'md',
  MA: 'ma', MI: 'mi', MN: 'mn', MS: 'ms', MO: 'mo', MT: 'mt', NE: 'ne',
  NV: 'nv', NH: 'nh', NJ: 'nj', NM: 'nm', NY: 'ny', NC: 'nc', ND: 'nd',
  OH: 'oh', OK: 'ok', OR: 'or', PA: 'pa', RI: 'ri', SC: 'sc', SD: 'sd',
  TN: 'tn', TX: 'tx', UT: 'ut', VT: 'vt', VA: 'va', WA: 'wa', WV: 'wv',
  WI: 'wi', WY: 'wy',
};

// Major immigration cities per state (Avvo uses underscores in slugs)
const STATE_CITIES = {
  CA: ['los_angeles', 'san_francisco', 'san_diego', 'san_jose', 'sacramento', 'oakland', 'irvine', 'fresno', 'riverside', 'santa_ana'],
  NY: ['new_york', 'brooklyn', 'buffalo', 'rochester', 'albany', 'white_plains', 'queens', 'bronx'],
  TX: ['houston', 'dallas', 'san_antonio', 'austin', 'el_paso', 'fort_worth', 'mcallen', 'plano'],
  FL: ['miami', 'orlando', 'tampa', 'jacksonville', 'fort_lauderdale', 'west_palm_beach', 'hialeah'],
  IL: ['chicago', 'springfield', 'naperville', 'aurora', 'rockford'],
  NJ: ['newark', 'jersey_city', 'trenton', 'edison', 'hackensack', 'elizabeth'],
  MA: ['boston', 'cambridge', 'worcester', 'springfield'],
  GA: ['atlanta', 'savannah', 'augusta', 'marietta'],
  PA: ['philadelphia', 'pittsburgh', 'harrisburg', 'allentown'],
  VA: ['arlington', 'fairfax', 'richmond', 'virginia_beach', 'alexandria'],
  MD: ['baltimore', 'bethesda', 'rockville', 'silver_spring'],
  WA: ['seattle', 'tacoma', 'spokane', 'bellevue'],
  AZ: ['phoenix', 'tucson', 'mesa', 'scottsdale'],
  CO: ['denver', 'colorado_springs', 'aurora', 'boulder'],
  NC: ['charlotte', 'raleigh', 'durham', 'greensboro'],
  OH: ['columbus', 'cleveland', 'cincinnati', 'dayton'],
  MI: ['detroit', 'grand_rapids', 'ann_arbor', 'dearborn'],
  CT: ['hartford', 'new_haven', 'stamford', 'bridgeport'],
  MN: ['minneapolis', 'saint_paul', 'bloomington'],
  OR: ['portland', 'eugene', 'salem'],
  NV: ['las_vegas', 'reno', 'henderson'],
  TN: ['nashville', 'memphis', 'knoxville', 'chattanooga'],
  MO: ['kansas_city', 'saint_louis', 'springfield'],
  IN: ['indianapolis', 'fort_wayne', 'south_bend'],
  WI: ['milwaukee', 'madison', 'green_bay'],
  SC: ['charleston', 'columbia', 'greenville'],
  KY: ['louisville', 'lexington'],
  LA: ['new_orleans', 'baton_rouge'],
  AL: ['birmingham', 'huntsville', 'montgomery'],
  OK: ['oklahoma_city', 'tulsa'],
  IA: ['des_moines', 'cedar_rapids'],
  UT: ['salt_lake_city', 'provo'],
  KS: ['wichita', 'kansas_city'],
  AR: ['little_rock', 'fayetteville'],
  MS: ['jackson', 'gulfport'],
  NE: ['omaha', 'lincoln'],
  NM: ['albuquerque', 'santa_fe', 'las_cruces'],
  HI: ['honolulu'],
  NH: ['manchester', 'concord'],
  ME: ['portland', 'bangor'],
  ID: ['boise', 'idaho_falls'],
  WV: ['charleston', 'morgantown'],
  MT: ['billings', 'missoula'],
  RI: ['providence', 'warwick'],
  DE: ['wilmington', 'dover'],
  SD: ['sioux_falls'],
  ND: ['fargo', 'bismarck'],
  AK: ['anchorage', 'fairbanks'],
  VT: ['burlington'],
  WY: ['cheyenne', 'casper'],
  DC: ['washington'],
};

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

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    // Remove titles/suffixes
    .replace(/^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?)\s+/i, '')
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

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
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

const PROGRESS_FILE = OUTPUT_FILE.replace('.csv', '-progress.json');

/**
 * Load progress tracker (states completed).
 */
function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      return new Set(data.statesCompleted || []);
    }
  } catch {}
  return new Set();
}

/**
 * Save progress tracker.
 */
function saveProgress(statesCompleted) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    statesCompleted: [...statesCompleted],
    updatedAt: new Date().toISOString(),
  }, null, 2));
}

/**
 * Load existing CSV for resume support.
 * Returns { leads: [], seenProfileUrls: Set }
 */
function loadExistingCSV(csvPath) {
  const result = { leads: [], seenProfileUrls: new Set() };

  if (!fs.existsSync(csvPath)) return result;

  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length <= 1) return result;

  const headers = lines[0].split(',');

  for (let i = 1; i < lines.length; i++) {
    // Simple CSV parse (handles quoted fields)
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (const ch of lines[i]) {
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === ',' && !inQuotes) {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
    fields.push(current);

    const lead = {};
    for (let j = 0; j < headers.length && j < fields.length; j++) {
      lead[headers[j]] = fields[j];
    }

    if (lead.profile_url) {
      result.seenProfileUrls.add(lead.profile_url);
    }
    result.leads.push(lead);
  }

  return result;
}

/**
 * Convert city slug to display name.
 * "los_angeles" → "Los Angeles"
 */
function cityDisplayName(slug) {
  return slug.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

async function isCloudflareChallenge(page) {
  return page.evaluate(() => {
    return document.title.includes('Just a moment') ||
      document.title.includes('Attention Required') ||
      document.querySelector('#challenge-running') !== null ||
      document.querySelector('#challenge-stage') !== null;
  });
}

async function waitForCloudflare(page, maxRetries = MAX_CLOUDFLARE_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    const blocked = await isCloudflareChallenge(page);
    if (!blocked) return true;
    log(`  Cloudflare challenge detected, waiting ${CLOUDFLARE_WAIT / 1000}s (attempt ${i + 1}/${maxRetries})...`);
    await sleep(CLOUDFLARE_WAIT);
  }
  return !(await isCloudflareChallenge(page));
}

/**
 * Extract lawyer data from a single Avvo results page.
 * Returns { leads: [], hasNextPage: boolean, totalResults: number }
 */
async function extractPageData(page, stateCode) {
  return page.evaluate((sc) => {
    const result = { leads: [], hasNextPage: false, totalResults: 0 };

    // Extract JSON-LD from each organic card
    const cards = document.querySelectorAll('.organic-card');
    for (const card of cards) {
      const script = card.querySelector('script[type="application/ld+json"]');
      if (!script) continue;

      try {
        const jsonLd = JSON.parse(script.textContent);
        if (!jsonLd || jsonLd['@type'] !== 'Person') continue;

        const name = jsonLd.name || '';
        if (!name || name.length < 2) continue;

        const worksFor = jsonLd.worksFor || {};
        const address = worksFor.address || {};

        result.leads.push({
          name,
          firm_name: worksFor.name || '',
          phone: worksFor.telephone || '',
          city: address.addressLocality || '',
          state: address.addressRegion || sc,
          zip_code: address.postalCode || '',
          street_address: address.streetAddress || '',
          profile_url: jsonLd['@id'] || jsonLd.url || '',
          education: (jsonLd.alumniOf || []).map(a => a.name || '').filter(Boolean).join('; '),
        });
      } catch (e) {
        // Skip unparseable JSON-LD
      }
    }

    // Check for next page link
    const nextLink = document.querySelector('.pagination .next a[rel="next"]');
    if (nextLink) result.hasNextPage = true;

    // Also check for numbered pagination links beyond current
    const paginationLinks = document.querySelectorAll('.pagination a[href*="page="]');
    if (paginationLinks.length > 0) {
      const currentPageMatch = window.location.search.match(/page=(\d+)/);
      const currentPage = currentPageMatch ? parseInt(currentPageMatch[1]) : 1;
      for (const link of paginationLinks) {
        const m = link.href.match(/page=(\d+)/);
        if (m && parseInt(m[1]) > currentPage) {
          result.hasNextPage = true;
          break;
        }
      }
    }

    // Total results count from page description
    const desc = document.querySelector('.page-description');
    if (desc) {
      const match = desc.textContent.match(/([\d,]+)\s+immigration/i)
        || desc.textContent.match(/([\d,]+)\s+\w+\s+(lawyer|attorney)/i);
      if (match) {
        result.totalResults = parseInt(match[1].replace(/,/g, ''), 10);
      }
    }

    return result;
  }, stateCode);
}

/**
 * Scrape all pages for a specific city on Avvo.
 * URL pattern: /immigration-lawyer/{state_abbr}/{city_slug}.html?page={n}
 */
async function scrapeCity(page, stateCode, citySlug, maxPages, seenProfileUrls) {
  const stateAbbr = STATE_ABBR[stateCode] || stateCode.toLowerCase();
  const leads = [];
  let totalResults = 0;

  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    const pageParam = pageNum > 1 ? `?page=${pageNum}` : '';
    const url = `https://www.avvo.com/immigration-lawyer/${stateAbbr}/${citySlug}.html${pageParam}`;

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`    ERROR navigating to ${citySlug} page ${pageNum}: ${err.message}`);
      break;
    }

    // Wait for Cloudflare
    const passed = await waitForCloudflare(page);
    if (!passed) {
      log(`    BLOCKED by Cloudflare on ${citySlug} page ${pageNum}`);
      break;
    }

    // Check for 404 / no results page
    const pageTitle = await page.title();
    if (pageTitle.includes('Page Not Found') || pageTitle.includes('404')) {
      if (pageNum === 1) log(`    ${cityDisplayName(citySlug)}: not found (404)`);
      break;
    }

    const data = await extractPageData(page, stateCode);

    if (pageNum === 1 && data.totalResults > 0) {
      totalResults = data.totalResults;
      const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
      log(`    ${cityDisplayName(citySlug)}: ${totalResults} immigration lawyers (${totalPages} pages)`);
    }

    if (data.leads.length === 0) {
      if (pageNum === 1) log(`    ${cityDisplayName(citySlug)}: no results`);
      break;
    }

    let newCount = 0;
    for (const raw of data.leads) {
      if (raw.profile_url && seenProfileUrls.has(raw.profile_url)) continue;
      if (raw.profile_url) seenProfileUrls.add(raw.profile_url);

      const { first_name, last_name } = parseName(raw.name);

      leads.push({
        first_name,
        last_name,
        firm_name: raw.firm_name,
        title: '',
        email: '',
        phone: cleanPhone(raw.phone),
        website: '',       // Avvo JSON-LD doesn't include website
        domain: '',
        city: raw.city || cityDisplayName(citySlug),
        state: raw.state || stateCode,
        country: 'US',
        niche: 'immigration',
        source: 'avvo',
        profile_url: raw.profile_url,
      });
      newCount++;
    }

    if (pageNum === 1 && data.leads.length > 0) {
      log(`    Page 1: ${data.leads.length} cards, ${newCount} new`);
    } else if (data.leads.length > 0) {
      log(`    Page ${pageNum}: ${data.leads.length} cards, ${newCount} new`);
    }

    if (!data.hasNextPage) break;

    // Rate limit between pages
    await randomDelay();
  }

  return { leads, totalResults };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const allStatesMode = args.includes('--all-states');
  const noResume = args.includes('--no-resume');
  let statesFilter = null;

  const statesArg = args.find(a => a.startsWith('--states='));
  if (statesArg) {
    statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
  } else {
    // Also support --states CA,NY format (space separated)
    const statesIdx = args.indexOf('--states');
    if (statesIdx >= 0 && args[statesIdx + 1]) {
      statesFilter = args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase());
    }
  }

  let states;
  if (statesFilter) {
    states = statesFilter;
  } else if (allStatesMode) {
    states = ALL_STATES;
  } else if (isTest) {
    states = ['CA', 'NY'];  // Test with just 2 states
  } else {
    states = DEFAULT_STATES;
  }

  const maxCitiesPerState = isTest ? 2 : 999;
  const maxPagesPerCity = isTest ? 2 : MAX_PAGES_DEFAULT;

  log('========================================');
  log('  Avvo Immigration Lawyers Scraper');
  log('========================================');
  log(`States: ${states.length} (${states.join(', ')})`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Max cities/state: ${isTest ? maxCitiesPerState : 'all'}`);
  log(`Max pages/city: ${maxPagesPerCity}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Resume support — load existing CSV + progress file
  let allLeads = [];
  let seenProfileUrls = new Set();
  let statesCompleted = new Set();

  if (!noResume) {
    const existing = loadExistingCSV(OUTPUT_FILE);
    statesCompleted = loadProgress();
    if (existing.leads.length > 0) {
      allLeads = existing.leads;
      seenProfileUrls = existing.seenProfileUrls;
      log(`RESUME: loaded ${allLeads.length} existing leads from CSV`);
      log(`RESUME: states already done: ${[...statesCompleted].join(', ') || 'none'}`);
    }
  } else {
    // Clean up progress file on fresh start
    if (fs.existsSync(PROGRESS_FILE)) fs.unlinkSync(PROGRESS_FILE);
  }

  // Launch browser
  log('Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--window-size=1920,1080',
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Warm up session — visit Avvo homepage to get cookies
  log('Warming up session...');
  try {
    await page.goto('https://www.avvo.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(3000);
    const warmupOk = !(await isCloudflareChallenge(page));
    log(`Session warmup: ${warmupOk ? 'OK' : 'Cloudflare challenge'}`);
    if (!warmupOk) {
      log('Waiting for Cloudflare to resolve...');
      await sleep(15000);
      const stillBlocked = await isCloudflareChallenge(page);
      if (stillBlocked) {
        log('WARNING: Could not bypass Cloudflare on warmup. Will retry per-page.');
      }
    }
  } catch (err) {
    log(`Warmup navigation error (non-fatal): ${err.message}`);
  }

  let totalStatesProcessed = 0;
  let consecutiveBlocks = 0;

  for (const stateCode of states) {
    // Resume — skip already-completed states
    if (!noResume && statesCompleted.has(stateCode)) {
      log(`\nSkipping ${stateCode} (already completed)`);
      continue;
    }

    if (!STATE_ABBR[stateCode]) {
      log(`Unknown state code: ${stateCode}`);
      continue;
    }

    totalStatesProcessed++;
    log(`\n${'='.repeat(50)}`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode}`);

    // Cool down if too many consecutive blocks
    if (consecutiveBlocks >= 3) {
      log(`  ${consecutiveBlocks} consecutive blocks — pausing 60s to cool down...`);
      await sleep(60000);
      consecutiveBlocks = 0;
    }

    // Get cities for this state
    const cities = (STATE_CITIES[stateCode] || [stateCode.toLowerCase()]).slice(0, maxCitiesPerState);
    if (cities.length === 0) {
      log(`  No cities defined for ${stateCode} — skipping`);
      continue;
    }

    let stateLeadCount = 0;
    let stateBlocked = true;

    for (let ci = 0; ci < cities.length; ci++) {
      const citySlug = cities[ci];
      log(`  City ${ci + 1}/${cities.length}: ${cityDisplayName(citySlug)}`);

      const { leads, totalResults } = await scrapeCity(
        page, stateCode, citySlug, maxPagesPerCity, seenProfileUrls
      );

      if (leads.length > 0 || totalResults > 0) {
        stateBlocked = false;
      }

      allLeads.push(...leads);
      stateLeadCount += leads.length;

      // Rate limit between cities
      if (ci < cities.length - 1) {
        await randomDelay();
      }
    }

    if (stateBlocked) {
      consecutiveBlocks++;
    } else {
      consecutiveBlocks = 0;
    }

    log(`  ${stateCode} total: ${stateLeadCount} new leads`);

    // Mark state as completed and save progress
    statesCompleted.add(stateCode);
    saveProgress(statesCompleted);

    // Incremental save after each state
    writeCSV(allLeads, OUTPUT_FILE);
    log(`  Saved ${allLeads.length} total leads to CSV`);

    // Delay between states
    if (totalStatesProcessed < states.length) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 3000);
    }
  }

  await browser.close();

  // Final write
  writeCSV(allLeads, OUTPUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}`);
  log(`With phone: ${withPhone}`);
  log(`With firm name: ${withFirm}`);
  log(`With full name: ${withName}`);
  log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
