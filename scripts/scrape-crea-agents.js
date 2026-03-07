#!/usr/bin/env node
/**
 * Scrape real estate agents from CREA / REALTOR.ca — the national Canadian REALTOR directory.
 *
 * Source: https://www.realtor.ca  (api2.realtor.ca/Listing.svc/PropertySearch_Post)
 * Method: Puppeteer (stealth) navigates to realtor.ca map search pages.
 *         The website's JavaScript calls api2.realtor.ca internally.
 *         We intercept those API responses to extract agent (Individual) data
 *         from property listings. Agents are deduplicated by IndividualID.
 *
 * NOTE: realtor.ca uses Incapsula WAF which aggressively blocks scrapers.
 *       - Uses puppeteer-extra + stealth-plugin to minimize detection
 *       - If IP gets blocked (Error 15), wait 30-60 min or use a different IP
 *       - Works best from residential IPs or via VPN/proxy
 *       - The --proxy flag allows routing through a SOCKS5/HTTP proxy
 *
 * Expected: 30,000-80,000+ unique agents across all provinces.
 *
 * Dependencies: puppeteer-extra, puppeteer-extra-plugin-stealth (already in project)
 *               + Node.js built-in modules (fs, path).
 *
 * Features:
 *   - Puppeteer-stealth to bypass Incapsula WAF
 *   - API response interception for structured agent data
 *   - Geographic search across 90+ Canadian cities
 *   - Agent dedup by IndividualID (CREA's unique agent identifier)
 *   - Province filtering (--province=ON, --province=BC, etc.)
 *   - Auto-saves progress CSV every 500 new agents
 *   - Rate limiting (configurable, default 3000ms)
 *   - Resume support (reads existing progress file)
 *   - Proxy support (--proxy=socks5://host:port)
 *   - Test mode (--test: 2 cities, 1 page each)
 *
 * Usage:
 *   node scripts/scrape-crea-agents.js --test              # Quick test (2 cities)
 *   node scripts/scrape-crea-agents.js                     # Full scrape (all cities)
 *   node scripts/scrape-crea-agents.js --province=ON       # Ontario only
 *   node scripts/scrape-crea-agents.js --province=BC       # British Columbia only
 *   node scripts/scrape-crea-agents.js --resume            # Resume from progress file
 *   node scripts/scrape-crea-agents.js --delay=5000        # Custom delay (ms)
 *   node scripts/scrape-crea-agents.js --max-pages=10      # Max pages per city
 *   node scripts/scrape-crea-agents.js --proxy=socks5://127.0.0.1:1080
 *   node scripts/scrape-crea-agents.js --headful           # Run with visible browser
 */

const fs = require('fs');
const path = require('path');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const m = arg.match(/^--([^=]+)(?:=(.*))?$/);
  if (m) args[m[1]] = m[2] !== undefined ? m[2] : true;
}

const DELAY_MS      = parseInt(args.delay) || 3000;
const TEST_MODE     = !!args.test;
const RESUME        = !!args.resume;
const PROVINCE      = (args.province || '').toUpperCase();
const MAX_PAGES     = parseInt(args['max-pages']) || (TEST_MODE ? 1 : 51);
const PER_PAGE      = 200;
const PROXY         = args.proxy || '';
const HEADFUL       = !!args.headful;
const OUTPUT_DIR    = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR,
  PROVINCE ? `crea-agents-${PROVINCE}.csv` : 'crea-agents.csv'
);

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'individual_id',
];

// ── Province metadata ───────────────────────────────────────────────────
const PROVINCES = {
  AB: 'Alberta',
  BC: 'British Columbia',
  MB: 'Manitoba',
  NB: 'New Brunswick',
  NL: 'Newfoundland and Labrador',
  NS: 'Nova Scotia',
  NT: 'Northwest Territories',
  NU: 'Nunavut',
  ON: 'Ontario',
  PE: 'Prince Edward Island',
  QC: 'Quebec',
  SK: 'Saskatchewan',
  YT: 'Yukon',
};

// ── City bounding boxes ─────────────────────────────────────────────────
// [latMin, latMax, lonMin, lonMax]. Overlap is fine — dedup by IndividualID.
const CITIES = [
  // Ontario
  { name: 'Toronto',         prov: 'ON', bbox: [43.58, 43.86, -79.64, -79.12] },
  { name: 'Ottawa',          prov: 'ON', bbox: [45.24, 45.50, -75.85, -75.55] },
  { name: 'Mississauga',     prov: 'ON', bbox: [43.52, 43.65, -79.75, -79.54] },
  { name: 'Brampton',        prov: 'ON', bbox: [43.65, 43.80, -79.82, -79.66] },
  { name: 'Hamilton',        prov: 'ON', bbox: [43.20, 43.30, -79.95, -79.75] },
  { name: 'London',          prov: 'ON', bbox: [42.90, 43.05, -81.35, -81.15] },
  { name: 'Markham',         prov: 'ON', bbox: [43.83, 43.95, -79.40, -79.22] },
  { name: 'Vaughan',         prov: 'ON', bbox: [43.77, 43.90, -79.58, -79.42] },
  { name: 'Kitchener',       prov: 'ON', bbox: [43.38, 43.48, -80.55, -80.40] },
  { name: 'Windsor',         prov: 'ON', bbox: [42.27, 42.35, -83.10, -82.90] },
  { name: 'Richmond Hill',   prov: 'ON', bbox: [43.83, 43.93, -79.50, -79.38] },
  { name: 'Oakville',        prov: 'ON', bbox: [43.40, 43.50, -79.75, -79.60] },
  { name: 'Burlington',      prov: 'ON', bbox: [43.30, 43.42, -79.85, -79.72] },
  { name: 'Sudbury',         prov: 'ON', bbox: [46.45, 46.55, -81.05, -80.90] },
  { name: 'Barrie',          prov: 'ON', bbox: [44.35, 44.42, -79.72, -79.62] },
  { name: 'Oshawa',          prov: 'ON', bbox: [43.85, 43.93, -78.92, -78.80] },
  { name: 'St. Catharines',  prov: 'ON', bbox: [43.13, 43.21, -79.28, -79.18] },
  { name: 'Guelph',          prov: 'ON', bbox: [43.50, 43.58, -80.30, -80.18] },
  { name: 'Cambridge',       prov: 'ON', bbox: [43.33, 43.42, -80.38, -80.28] },
  { name: 'Kingston',        prov: 'ON', bbox: [44.20, 44.28, -76.55, -76.43] },
  { name: 'Thunder Bay',     prov: 'ON', bbox: [48.35, 48.45, -89.30, -89.18] },
  { name: 'Waterloo',        prov: 'ON', bbox: [43.44, 43.52, -80.58, -80.48] },
  { name: 'Niagara Falls',   prov: 'ON', bbox: [43.05, 43.13, -79.12, -79.02] },

  // British Columbia
  { name: 'Vancouver',       prov: 'BC', bbox: [49.20, 49.32, -123.25, -123.02] },
  { name: 'Surrey',          prov: 'BC', bbox: [49.08, 49.22, -122.88, -122.68] },
  { name: 'Burnaby',         prov: 'BC', bbox: [49.20, 49.30, -123.02, -122.88] },
  { name: 'Richmond',        prov: 'BC', bbox: [49.12, 49.20, -123.22, -123.08] },
  { name: 'Coquitlam',       prov: 'BC', bbox: [49.24, 49.32, -122.82, -122.72] },
  { name: 'Kelowna',         prov: 'BC', bbox: [49.82, 49.92, -119.52, -119.40] },
  { name: 'Victoria',        prov: 'BC', bbox: [48.40, 48.48, -123.42, -123.30] },
  { name: 'Nanaimo',         prov: 'BC', bbox: [49.13, 49.22, -124.00, -123.90] },
  { name: 'Kamloops',        prov: 'BC', bbox: [50.65, 50.75, -120.40, -120.28] },
  { name: 'Langley',         prov: 'BC', bbox: [49.05, 49.15, -122.70, -122.55] },
  { name: 'Abbotsford',      prov: 'BC', bbox: [49.02, 49.10, -122.38, -122.25] },
  { name: 'North Vancouver', prov: 'BC', bbox: [49.30, 49.38, -123.12, -123.00] },
  { name: 'White Rock',      prov: 'BC', bbox: [49.01, 49.06, -122.82, -122.76] },

  // Alberta
  { name: 'Calgary',         prov: 'AB', bbox: [50.90, 51.18, -114.25, -113.90] },
  { name: 'Edmonton',        prov: 'AB', bbox: [53.42, 53.65, -113.68, -113.35] },
  { name: 'Red Deer',        prov: 'AB', bbox: [52.22, 52.32, -113.85, -113.75] },
  { name: 'Lethbridge',      prov: 'AB', bbox: [49.65, 49.73, -112.88, -112.78] },
  { name: 'St. Albert',      prov: 'AB', bbox: [53.60, 53.68, -113.65, -113.55] },
  { name: 'Medicine Hat',    prov: 'AB', bbox: [50.02, 50.08, -110.72, -110.62] },
  { name: 'Grande Prairie',  prov: 'AB', bbox: [55.15, 55.20, -118.85, -118.78] },
  { name: 'Airdrie',         prov: 'AB', bbox: [51.26, 51.32, -114.05, -113.95] },
  { name: 'Spruce Grove',    prov: 'AB', bbox: [53.53, 53.57, -113.92, -113.85] },
  { name: 'Fort McMurray',   prov: 'AB', bbox: [56.70, 56.78, -111.42, -111.32] },

  // Quebec
  { name: 'Montreal',        prov: 'QC', bbox: [45.42, 45.62, -73.75, -73.48] },
  { name: 'Quebec City',     prov: 'QC', bbox: [46.78, 46.88, -71.32, -71.18] },
  { name: 'Laval',           prov: 'QC', bbox: [45.52, 45.62, -73.80, -73.65] },
  { name: 'Gatineau',        prov: 'QC', bbox: [45.42, 45.52, -75.78, -75.60] },
  { name: 'Longueuil',       prov: 'QC', bbox: [45.48, 45.55, -73.55, -73.45] },
  { name: 'Sherbrooke',      prov: 'QC', bbox: [45.38, 45.45, -71.95, -71.85] },
  { name: 'Saguenay',        prov: 'QC', bbox: [48.40, 48.48, -71.10, -71.00] },
  { name: 'Levis',           prov: 'QC', bbox: [46.75, 46.83, -71.22, -71.12] },
  { name: 'Trois-Rivieres',  prov: 'QC', bbox: [46.33, 46.38, -72.58, -72.50] },
  { name: 'Terrebonne',      prov: 'QC', bbox: [45.68, 45.75, -73.68, -73.58] },

  // Manitoba
  { name: 'Winnipeg',        prov: 'MB', bbox: [49.78, 49.98, -97.25, -97.05] },
  { name: 'Brandon',         prov: 'MB', bbox: [49.82, 49.88, -99.98, -99.90] },
  { name: 'Steinbach',       prov: 'MB', bbox: [49.51, 49.54, -96.70, -96.65] },

  // Saskatchewan
  { name: 'Saskatoon',       prov: 'SK', bbox: [52.08, 52.20, -106.72, -106.55] },
  { name: 'Regina',          prov: 'SK', bbox: [50.40, 50.50, -104.68, -104.55] },
  { name: 'Prince Albert',   prov: 'SK', bbox: [53.18, 53.23, -105.78, -105.72] },
  { name: 'Moose Jaw',       prov: 'SK', bbox: [50.38, 50.42, -105.58, -105.50] },

  // Nova Scotia
  { name: 'Halifax',         prov: 'NS', bbox: [44.58, 44.72, -63.65, -63.48] },
  { name: 'Dartmouth',       prov: 'NS', bbox: [44.62, 44.70, -63.58, -63.50] },
  { name: 'Sydney',          prov: 'NS', bbox: [46.12, 46.18, -60.22, -60.15] },
  { name: 'Truro',           prov: 'NS', bbox: [45.35, 45.38, -63.30, -63.25] },

  // New Brunswick
  { name: 'Moncton',         prov: 'NB', bbox: [46.07, 46.15, -64.85, -64.75] },
  { name: 'Saint John',      prov: 'NB', bbox: [45.25, 45.32, -66.10, -65.98] },
  { name: 'Fredericton',     prov: 'NB', bbox: [45.92, 45.98, -66.70, -66.60] },

  // Newfoundland and Labrador
  { name: "St. John's",      prov: 'NL', bbox: [47.52, 47.60, -52.78, -52.65] },
  { name: 'Mount Pearl',     prov: 'NL', bbox: [47.50, 47.54, -52.82, -52.75] },
  { name: 'Corner Brook',    prov: 'NL', bbox: [48.93, 48.98, -57.98, -57.92] },

  // Prince Edward Island
  { name: 'Charlottetown',   prov: 'PE', bbox: [46.22, 46.28, -63.17, -63.10] },
  { name: 'Summerside',      prov: 'PE', bbox: [46.38, 46.42, -63.82, -63.76] },

  // Northwest Territories
  { name: 'Yellowknife',     prov: 'NT', bbox: [62.44, 62.48, -114.42, -114.35] },

  // Yukon
  { name: 'Whitehorse',      prov: 'YT', bbox: [60.70, 60.76, -135.10, -135.00] },

  // Nunavut
  { name: 'Iqaluit',         prov: 'NU', bbox: [63.73, 63.77, -68.55, -68.48] },
];

// ── Extract agents from API response JSON ───────────────────────────────
function extractAgents(json, cityName, provCode) {
  const agents = [];
  const results = json.Results || [];
  for (const listing of results) {
    const individuals = listing.Individual || [];
    for (const ind of individuals) {
      const agent = parseIndividual(ind, cityName, provCode);
      if (agent) agents.push(agent);
    }
  }
  return agents;
}

function parseIndividual(ind, cityName, provCode) {
  if (!ind || !ind.IndividualID) return null;

  const fullName = (ind.Name || '').trim();
  let firstName = '', lastName = '';
  if (fullName) {
    const parts = fullName.split(/\s+/).filter(Boolean);
    if (parts.length === 1) {
      lastName = parts[0];
    } else if (parts.length === 2) {
      firstName = parts[0];
      lastName = parts[1];
    } else if (parts.length >= 3) {
      firstName = parts[0];
      lastName = parts.slice(1).join(' ');
    }
  }

  const org = ind.Organization || {};
  const firmName = (org.Name || '').trim();

  let phone = '';
  const phones = ind.Phones || [];
  for (const p of phones) {
    if (p.PhoneNumber) {
      const areaCode = p.AreaCode || '';
      const number = p.PhoneNumber || '';
      phone = normalizePhone(areaCode + number);
      break;
    }
  }

  let email = '';
  const emails = ind.Emails || [];
  for (const e of emails) {
    if (typeof e === 'string' && e.includes('@')) {
      email = e.trim().toLowerCase();
      break;
    } else if (e && typeof e === 'object') {
      const candidate = (e.ContactId || e.Email || e.email || '').trim().toLowerCase();
      if (candidate.includes('@')) { email = candidate; break; }
    }
  }

  let website = '';
  const websites = ind.Websites || [];
  for (const w of websites) {
    const url = (typeof w === 'string') ? w : (w.Website || w.Url || '');
    if (url) {
      website = url.trim();
      if (website && !website.startsWith('http')) website = 'https://' + website;
      break;
    }
  }

  let domain = '';
  if (website) { try { domain = new URL(website).hostname.replace(/^www\./, ''); } catch {} }
  if (!domain && email && email.includes('@')) domain = email.split('@')[1];

  let city = cityName;
  const orgAddr = org.Address || {};
  if (orgAddr.AddressText) {
    const addrCity = extractCityFromAddress(orgAddr.AddressText);
    if (addrCity) city = addrCity;
  }

  let title = (ind.Position || '').trim();
  if (!title) title = 'Real Estate Agent';

  return {
    first_name: titleCase(firstName),
    last_name: titleCase(lastName),
    firm_name: firmName,
    title: title,
    email: email,
    phone: phone,
    website: website,
    domain: domain,
    city: titleCase(city),
    state: PROVINCES[provCode] || provCode,
    country: 'CA',
    niche: 'real estate agent',
    source: 'crea',
    individual_id: String(ind.IndividualID),
  };
}

function extractCityFromAddress(addrText) {
  if (!addrText) return '';
  const parts = addrText.split('|');
  const lastPart = (parts[parts.length - 1] || '').trim();
  const m = lastPart.match(/^([^,]+),/);
  if (m) return m[1].trim();
  return '';
}

// ── Helpers ─────────────────────────────────────────────────────────────
function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|\s|[-'])\w/g, c => c.toUpperCase());
}

function normalizePhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits[0] === '1') return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
  return phone;
}

function escapeCSV(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead => CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(','));
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function loadExistingLeads() {
  if (!fs.existsSync(PROGRESS_FILE)) return { leads: [], seenIds: new Set() };
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return { leads: [], seenIds: new Set() };
    const headers = lines[0].split(',');
    const idIdx = headers.indexOf('individual_id');
    const leads = [];
    const seenIds = new Set();
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
      if (idIdx >= 0 && vals[idIdx]) seenIds.add(vals[idIdx]);
    }
    return { leads, seenIds };
  } catch { return { leads: [], seenIds: new Set() }; }
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; }
        else inQuotes = false;
      } else current += ch;
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ',') { result.push(current); current = ''; }
      else current += ch;
    }
  }
  result.push(current);
  return result;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();

  let cities = CITIES;
  if (PROVINCE) {
    cities = cities.filter(c => c.prov === PROVINCE);
    if (cities.length === 0) {
      console.error(`  No cities for province "${PROVINCE}". Valid: ${Object.keys(PROVINCES).join(', ')}`);
      process.exit(1);
    }
  }
  if (TEST_MODE) cities = cities.slice(0, 2);

  let { leads: allLeads, seenIds } = RESUME
    ? loadExistingLeads() : { leads: [], seenIds: new Set() };

  if (RESUME && allLeads.length > 0) {
    console.log(`  Resuming with ${allLeads.length} existing agents (${seenIds.size} unique IDs)`);
  }

  console.log('');
  console.log('=============================================================');
  console.log(`  MORTAR -- CREA/REALTOR.ca Agent Scraper${TEST_MODE ? ' (TEST MODE)' : ''}`);
  console.log('=============================================================');
  console.log('');
  console.log(`  Province:    ${PROVINCE || 'ALL PROVINCES'}`);
  console.log(`  Cities:      ${cities.length}`);
  console.log(`  Max pages:   ${MAX_PAGES} per city`);
  console.log(`  Per page:    ${PER_PAGE} listings`);
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Proxy:       ${PROXY || 'none'}`);
  console.log(`  Output:      ${PROGRESS_FILE}`);
  console.log('');

  // ── Launch browser with stealth ──────────────────────────────────────
  let puppeteer;
  try {
    puppeteer = require('puppeteer-extra');
    const StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteer.use(StealthPlugin());
  } catch {
    // Fallback to regular puppeteer if stealth not available
    try {
      puppeteer = require('puppeteer');
      console.log('  [WARN] puppeteer-extra-plugin-stealth not available, using plain puppeteer');
    } catch {
      console.error('  puppeteer not installed. Run: npm install puppeteer');
      process.exit(1);
    }
  }

  const launchArgs = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-blink-features=AutomationControlled',
    '--window-size=1440,900',
  ];
  if (PROXY) launchArgs.push(`--proxy-server=${PROXY}`);

  console.log(`  [BROWSER] Launching ${HEADFUL ? 'headful' : 'headless'}...`);
  const browser = await puppeteer.launch({
    headless: HEADFUL ? false : 'new',
    args: launchArgs,
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );

  // ── Set up API response interception ──────────────────────────────────
  const pendingResponses = [];
  let responseResolve = null;

  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('PropertySearch_Post') || url.includes('PropertySearch')) {
      try {
        const text = await response.text();
        if (text && text.length > 200 && text.startsWith('{')) {
          const json = JSON.parse(text);
          if (json.Results) {
            if (responseResolve) {
              responseResolve(json);
              responseResolve = null;
            } else {
              pendingResponses.push(json);
            }
          }
        }
      } catch {}
    }
  });

  function waitForApiResponse(timeoutMs = 20000) {
    return new Promise((resolve, reject) => {
      if (pendingResponses.length > 0) { resolve(pendingResponses.shift()); return; }
      const timer = setTimeout(() => {
        responseResolve = null;
        reject(new Error('API response timeout'));
      }, timeoutMs);
      responseResolve = (data) => { clearTimeout(timer); resolve(data); };
    });
  }

  // ── Navigate to realtor.ca to establish session ───────────────────────
  console.log('  [BROWSER] Navigating to realtor.ca...');
  try {
    await page.goto('https://www.realtor.ca', {
      waitUntil: 'networkidle2',
      timeout: 45000,
    });
    await sleep(5000);
  } catch (e) {
    console.log(`  [BROWSER] Load: ${e.message}`);
  }

  // Check if we passed the Incapsula challenge
  const bodyLen = await page.evaluate(() => document.body.innerHTML.length);
  const isBlocked = await page.evaluate(() => {
    const text = document.body.innerText || '';
    return text.includes('Access Denied') ||
           text.includes('security check') ||
           text.includes('blocked by our security') ||
           document.body.innerHTML.length < 1000;
  });

  if (isBlocked) {
    console.log('');
    console.log('  *** BLOCKED BY INCAPSULA WAF ***');
    console.log('');
    console.log('  Realtor.ca\'s Imperva Incapsula firewall has blocked this request.');
    console.log('  This typically happens when:');
    console.log('    1. Your IP has been flagged from previous scraping attempts');
    console.log('    2. Headless browser detection triggered a block');
    console.log('');
    console.log('  Solutions:');
    console.log('    - Wait 30-60 minutes for the IP block to expire');
    console.log('    - Use a VPN or residential proxy:');
    console.log('        node scripts/scrape-crea-agents.js --proxy=socks5://host:port');
    console.log('    - Run with --headful flag (visible browser, harder to detect):');
    console.log('        node scripts/scrape-crea-agents.js --headful --test');
    console.log('    - Deploy to Railway (different IP):');
    console.log('        npx @railway/cli up --service mortar-lead-scraper');
    console.log('');
    await browser.close();
    process.exit(1);
  }

  console.log(`  [BROWSER] Session established (body: ${bodyLen} bytes).`);
  console.log('');
  console.log('  Starting agent extraction from property listings...');
  console.log('');

  let totalRequests = 0;
  let newAgents = 0;
  let duplicates = 0;
  let errors = 0;
  let consecutiveErrors = 0;
  let lastSaveCount = 0;
  let withEmail = allLeads.filter(l => l.email).length;
  let withPhone = allLeads.filter(l => l.phone).length;
  let citiesComplete = 0;

  for (const city of cities) {
    citiesComplete++;
    const cityLabel = `${city.name}, ${city.prov}`;
    console.log(`  [${citiesComplete}/${cities.length}] ${cityLabel}`);

    let currentPage = 1;
    let cityNewAgents = 0;
    let hasMore = true;

    while (hasMore && currentPage <= MAX_PAGES) {
      totalRequests++;
      const [latMin, latMax, lonMin, lonMax] = city.bbox;

      try {
        // Clear stale responses
        pendingResponses.length = 0;

        // Build the map URL with city bbox coordinates
        const mapHash = [
          `LatitudeMin=${latMin}`,
          `LatitudeMax=${latMax}`,
          `LongitudeMin=${lonMin}`,
          `LongitudeMax=${lonMax}`,
          `CurrentPage=${currentPage}`,
          `RecordsPerPage=${PER_PAGE}`,
          `PropertySearchTypeId=1`,
          `TransactionTypeId=2`,
          `SortBy=6`,
          `SortOrder=D`,
          `CultureId=1`,
          `ApplicationId=37`,
        ].join('&');

        const targetUrl = `https://www.realtor.ca/map#${mapHash}`;

        // Navigate to the map URL
        await page.goto(targetUrl, {
          waitUntil: 'networkidle2',
          timeout: 30000,
        });

        // Wait for API call to be made by the SPA
        await sleep(5000);

        // Try to get the intercepted API response
        let json;
        try {
          json = await waitForApiResponse(15000);
        } catch {
          // Fallback: try to extract data from the DOM
          json = null;
        }

        if (!json) {
          // If no API response captured, the SPA may not have triggered a search.
          // Try to trigger it by updating the hash and waiting.
          await page.evaluate((hash) => {
            window.location.hash = hash;
            // Try to trigger a hashchange event
            window.dispatchEvent(new HashChangeEvent('hashchange'));
          }, mapHash);
          await sleep(5000);

          try {
            json = await waitForApiResponse(10000);
          } catch {
            throw new Error('No API response after navigation and hash update');
          }
        }

        const paging = json.Paging || {};
        const totalRecords = paging.TotalRecords || 0;
        const totalPages = paging.TotalPages || 0;

        const agents = extractAgents(json, city.name, city.prov);
        let pageNew = 0, pageDup = 0;

        for (const agent of agents) {
          if (seenIds.has(agent.individual_id)) { pageDup++; duplicates++; continue; }
          seenIds.add(agent.individual_id);
          allLeads.push(agent);
          newAgents++; cityNewAgents++; pageNew++;
          if (agent.email) withEmail++;
          if (agent.phone) withPhone++;
        }

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const rate = elapsed > 0 ? (newAgents / (elapsed / 60)).toFixed(0) : '?';

        if (agents.length > 0 || currentPage === 1) {
          console.log(`    p${currentPage}/${totalPages || '?'}: ${agents.length} agents (${pageNew} new, ${pageDup} dup) | ${totalRecords} listings | Total: ${allLeads.length} | ${rate}/min`);
        }

        consecutiveErrors = 0;
        hasMore = currentPage < totalPages && agents.length > 0;
        currentPage++;

        if (newAgents > 0 && newAgents - lastSaveCount >= 500) {
          writeCSV(PROGRESS_FILE, allLeads);
          lastSaveCount = newAgents;
          console.log(`    [SAVE] ${allLeads.length} agents saved`);
        }

      } catch (err) {
        errors++;
        consecutiveErrors++;
        console.log(`    [ERR] p${currentPage}: ${err.message}`);

        if (consecutiveErrors >= 3 && consecutiveErrors <= 4) {
          console.log('    [REFRESH] Re-navigating to realtor.ca...');
          try {
            await page.goto('https://www.realtor.ca', { waitUntil: 'networkidle2', timeout: 30000 });
            await sleep(5000);
            console.log('    [REFRESH] Done.');
            consecutiveErrors = 0;
            continue;
          } catch (refreshErr) {
            console.log(`    [REFRESH] Failed: ${refreshErr.message}`);
          }
        }

        if (consecutiveErrors >= 5) {
          console.log('    [PAUSE] 5 errors. Waiting 30s, skipping city...');
          await sleep(30000);
          consecutiveErrors = 0;
          hasMore = false;
        }

        currentPage++;
      }

      await sleep(DELAY_MS);
    }

    if (cityNewAgents > 0) {
      console.log(`    -> ${cityLabel}: ${cityNewAgents} new agents found`);
    }
  }

  try { await browser.close(); } catch {}

  // ── Final save ──────────────────────────────────────────────────────
  if (allLeads.length > 0) {
    writeCSV(PROGRESS_FILE, allLeads);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const suffix = PROVINCE ? `-${PROVINCE}` : '';
    const finalFile = `crea-agents${suffix}_${timestamp}.csv`;
    const finalPath = path.join(OUTPUT_DIR, finalFile);
    writeCSV(finalPath, allLeads);
    console.log(`\n  Final file: ${finalPath}`);
  }

  // ── Summary ─────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const uniqueFirms = new Set(allLeads.filter(l => l.firm_name).map(l => l.firm_name)).size;
  const uniqueCities = new Set(allLeads.filter(l => l.city).map(l => l.city)).size;
  const byProvince = {};
  for (const l of allLeads) byProvince[l.state] = (byProvince[l.state] || 0) + 1;

  console.log('');
  console.log('=============================================================');
  console.log('  CREA SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  Province:       ${PROVINCE || 'ALL'}`);
  console.log(`  Cities:         ${citiesComplete}`);
  console.log(`  Requests:       ${totalRequests}`);
  console.log(`  Total agents:   ${allLeads.length}`);
  console.log(`  With email:     ${withEmail} (${Math.round(withEmail / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  With phone:     ${withPhone} (${Math.round(withPhone / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  Unique firms:   ${uniqueFirms}`);
  console.log(`  Unique cities:  ${uniqueCities}`);
  console.log(`  Duplicates:     ${duplicates}`);
  console.log(`  Errors:         ${errors}`);
  console.log(`  Time:           ${elapsed}s (${Math.round(elapsed / 60)}min)`);
  console.log(`  Output:         ${PROGRESS_FILE}`);
  if (Object.keys(byProvince).length > 0) {
    console.log('  By province:');
    for (const [prov, count] of Object.entries(byProvince).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${prov}: ${count}`);
    }
  }
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
