#!/usr/bin/env node
/**
 * YellowPages.com College Admissions Consultants Scraper
 *
 * Scrapes https://www.yellowpages.com/search?search_terms=college+admissions+consultant&geo_location_terms={City,ST}
 * using Puppeteer + Stealth (YP blocks plain HTTP requests with 403).
 *
 * Strategy:
 *   1. Iterate through 164 major US cities covering all 50 states + DC
 *   2. For each city, search 5 terms: college admissions consultant, college counselor,
 *      SAT ACT prep, educational consultant, college planning
 *   3. Extract business name, phone, address, website, categories from result cards
 *   4. Paginate through all result pages (30 per page, up to 10 pages)
 *   5. Dedup by business name + city
 *   6. Resume support: skip already-scraped cities via progress file
 *   7. Output CSV
 *
 * Data fields:
 *   - Business name (from .business-name)
 *   - Phone (from .phones.phone.primary)
 *   - Address (from .street-address + .locality)
 *   - Website (from a.track-visit-website)
 *   - Categories (from .categories)
 *   - Profile URL (from .business-name href)
 *
 * Usage:
 *   node scripts/scrape-yellowpages-college.js --test         # 3 cities
 *   node scripts/scrape-yellowpages-college.js --all-cities   # All 164 cities
 *   node scripts/scrape-yellowpages-college.js --resume       # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const NAV_TIMEOUT = 45000;
const RESULTS_PER_PAGE = 30;
const MAX_PAGES = 10;          // YP usually has up to ~25 pages, but 10 is plenty
const PAGE_LOAD_WAIT = 3000;   // Wait after page loads for JS rendering
const CF_CHALLENGE_WAIT = 15000; // Wait for Cloudflare challenge to resolve
const CF_MAX_RETRIES = 3;       // Max retries per Cloudflare challenge
const BROWSER_RESTART_EVERY = 25; // Restart browser every N searches to avoid fingerprinting
const INCREMENTAL_SAVE_EVERY = 25; // Save CSV every N new results

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-yellowpages.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'yellowpages-college-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Search terms — run all 5 to maximize coverage
const SEARCH_TERMS = [
  'college admissions consultant',
  'college counselor',
  'SAT ACT prep',
  'educational consultant',
  'college planning',
];

// ── City Database (164 cities covering all 50 states + DC) ──────────────────

const CITIES = [
  // Alabama
  { city: 'Birmingham', state: 'AL', loc: 'Birmingham, AL' },
  { city: 'Huntsville', state: 'AL', loc: 'Huntsville, AL' },
  { city: 'Mobile', state: 'AL', loc: 'Mobile, AL' },
  // Alaska
  { city: 'Anchorage', state: 'AK', loc: 'Anchorage, AK' },
  // Arizona
  { city: 'Phoenix', state: 'AZ', loc: 'Phoenix, AZ' },
  { city: 'Tucson', state: 'AZ', loc: 'Tucson, AZ' },
  { city: 'Mesa', state: 'AZ', loc: 'Mesa, AZ' },
  { city: 'Scottsdale', state: 'AZ', loc: 'Scottsdale, AZ' },
  // Arkansas
  { city: 'Little Rock', state: 'AR', loc: 'Little Rock, AR' },
  { city: 'Fayetteville', state: 'AR', loc: 'Fayetteville, AR' },
  // California
  { city: 'Los Angeles', state: 'CA', loc: 'Los Angeles, CA' },
  { city: 'San Francisco', state: 'CA', loc: 'San Francisco, CA' },
  { city: 'San Diego', state: 'CA', loc: 'San Diego, CA' },
  { city: 'San Jose', state: 'CA', loc: 'San Jose, CA' },
  { city: 'Sacramento', state: 'CA', loc: 'Sacramento, CA' },
  { city: 'Fresno', state: 'CA', loc: 'Fresno, CA' },
  { city: 'Oakland', state: 'CA', loc: 'Oakland, CA' },
  { city: 'Riverside', state: 'CA', loc: 'Riverside, CA' },
  { city: 'Bakersfield', state: 'CA', loc: 'Bakersfield, CA' },
  { city: 'Irvine', state: 'CA', loc: 'Irvine, CA' },
  { city: 'Pasadena', state: 'CA', loc: 'Pasadena, CA' },
  { city: 'Santa Ana', state: 'CA', loc: 'Santa Ana, CA' },
  { city: 'Long Beach', state: 'CA', loc: 'Long Beach, CA' },
  { city: 'Anaheim', state: 'CA', loc: 'Anaheim, CA' },
  // Colorado
  { city: 'Denver', state: 'CO', loc: 'Denver, CO' },
  { city: 'Colorado Springs', state: 'CO', loc: 'Colorado Springs, CO' },
  { city: 'Aurora', state: 'CO', loc: 'Aurora, CO' },
  { city: 'Fort Collins', state: 'CO', loc: 'Fort Collins, CO' },
  // Connecticut
  { city: 'Hartford', state: 'CT', loc: 'Hartford, CT' },
  { city: 'New Haven', state: 'CT', loc: 'New Haven, CT' },
  { city: 'Stamford', state: 'CT', loc: 'Stamford, CT' },
  { city: 'Bridgeport', state: 'CT', loc: 'Bridgeport, CT' },
  // Delaware
  { city: 'Wilmington', state: 'DE', loc: 'Wilmington, DE' },
  // District of Columbia
  { city: 'Washington', state: 'DC', loc: 'Washington, DC' },
  // Florida
  { city: 'Miami', state: 'FL', loc: 'Miami, FL' },
  { city: 'Orlando', state: 'FL', loc: 'Orlando, FL' },
  { city: 'Tampa', state: 'FL', loc: 'Tampa, FL' },
  { city: 'Jacksonville', state: 'FL', loc: 'Jacksonville, FL' },
  { city: 'Fort Lauderdale', state: 'FL', loc: 'Fort Lauderdale, FL' },
  { city: 'West Palm Beach', state: 'FL', loc: 'West Palm Beach, FL' },
  { city: 'Hialeah', state: 'FL', loc: 'Hialeah, FL' },
  { city: 'St. Petersburg', state: 'FL', loc: 'St. Petersburg, FL' },
  // Georgia
  { city: 'Atlanta', state: 'GA', loc: 'Atlanta, GA' },
  { city: 'Savannah', state: 'GA', loc: 'Savannah, GA' },
  { city: 'Augusta', state: 'GA', loc: 'Augusta, GA' },
  { city: 'Marietta', state: 'GA', loc: 'Marietta, GA' },
  // Hawaii
  { city: 'Honolulu', state: 'HI', loc: 'Honolulu, HI' },
  // Idaho
  { city: 'Boise', state: 'ID', loc: 'Boise, ID' },
  // Illinois
  { city: 'Chicago', state: 'IL', loc: 'Chicago, IL' },
  { city: 'Aurora', state: 'IL', loc: 'Aurora, IL' },
  { city: 'Naperville', state: 'IL', loc: 'Naperville, IL' },
  { city: 'Schaumburg', state: 'IL', loc: 'Schaumburg, IL' },
  // Indiana
  { city: 'Indianapolis', state: 'IN', loc: 'Indianapolis, IN' },
  { city: 'Fort Wayne', state: 'IN', loc: 'Fort Wayne, IN' },
  // Iowa
  { city: 'Des Moines', state: 'IA', loc: 'Des Moines, IA' },
  { city: 'Cedar Rapids', state: 'IA', loc: 'Cedar Rapids, IA' },
  // Kansas
  { city: 'Wichita', state: 'KS', loc: 'Wichita, KS' },
  { city: 'Kansas City', state: 'KS', loc: 'Kansas City, KS' },
  // Kentucky
  { city: 'Louisville', state: 'KY', loc: 'Louisville, KY' },
  { city: 'Lexington', state: 'KY', loc: 'Lexington, KY' },
  // Louisiana
  { city: 'New Orleans', state: 'LA', loc: 'New Orleans, LA' },
  { city: 'Baton Rouge', state: 'LA', loc: 'Baton Rouge, LA' },
  // Maine
  { city: 'Portland', state: 'ME', loc: 'Portland, ME' },
  // Maryland
  { city: 'Baltimore', state: 'MD', loc: 'Baltimore, MD' },
  { city: 'Silver Spring', state: 'MD', loc: 'Silver Spring, MD' },
  { city: 'Rockville', state: 'MD', loc: 'Rockville, MD' },
  { city: 'Bethesda', state: 'MD', loc: 'Bethesda, MD' },
  // Massachusetts
  { city: 'Boston', state: 'MA', loc: 'Boston, MA' },
  { city: 'Worcester', state: 'MA', loc: 'Worcester, MA' },
  { city: 'Springfield', state: 'MA', loc: 'Springfield, MA' },
  { city: 'Cambridge', state: 'MA', loc: 'Cambridge, MA' },
  // Michigan
  { city: 'Detroit', state: 'MI', loc: 'Detroit, MI' },
  { city: 'Grand Rapids', state: 'MI', loc: 'Grand Rapids, MI' },
  { city: 'Lansing', state: 'MI', loc: 'Lansing, MI' },
  { city: 'Ann Arbor', state: 'MI', loc: 'Ann Arbor, MI' },
  // Minnesota
  { city: 'Minneapolis', state: 'MN', loc: 'Minneapolis, MN' },
  { city: 'Saint Paul', state: 'MN', loc: 'Saint Paul, MN' },
  { city: 'Bloomington', state: 'MN', loc: 'Bloomington, MN' },
  // Mississippi
  { city: 'Jackson', state: 'MS', loc: 'Jackson, MS' },
  // Missouri
  { city: 'Kansas City', state: 'MO', loc: 'Kansas City, MO' },
  { city: 'Saint Louis', state: 'MO', loc: 'Saint Louis, MO' },
  { city: 'Springfield', state: 'MO', loc: 'Springfield, MO' },
  // Montana
  { city: 'Billings', state: 'MT', loc: 'Billings, MT' },
  { city: 'Missoula', state: 'MT', loc: 'Missoula, MT' },
  // Nebraska
  { city: 'Omaha', state: 'NE', loc: 'Omaha, NE' },
  { city: 'Lincoln', state: 'NE', loc: 'Lincoln, NE' },
  // Nevada
  { city: 'Las Vegas', state: 'NV', loc: 'Las Vegas, NV' },
  { city: 'Reno', state: 'NV', loc: 'Reno, NV' },
  { city: 'Henderson', state: 'NV', loc: 'Henderson, NV' },
  // New Hampshire
  { city: 'Manchester', state: 'NH', loc: 'Manchester, NH' },
  // New Jersey
  { city: 'Newark', state: 'NJ', loc: 'Newark, NJ' },
  { city: 'Jersey City', state: 'NJ', loc: 'Jersey City, NJ' },
  { city: 'Elizabeth', state: 'NJ', loc: 'Elizabeth, NJ' },
  { city: 'Hackensack', state: 'NJ', loc: 'Hackensack, NJ' },
  { city: 'Paterson', state: 'NJ', loc: 'Paterson, NJ' },
  { city: 'Trenton', state: 'NJ', loc: 'Trenton, NJ' },
  // New Mexico
  { city: 'Albuquerque', state: 'NM', loc: 'Albuquerque, NM' },
  { city: 'Las Cruces', state: 'NM', loc: 'Las Cruces, NM' },
  // New York
  { city: 'New York', state: 'NY', loc: 'New York, NY' },
  { city: 'Brooklyn', state: 'NY', loc: 'Brooklyn, NY' },
  { city: 'Queens', state: 'NY', loc: 'Queens, NY' },
  { city: 'Bronx', state: 'NY', loc: 'Bronx, NY' },
  { city: 'Buffalo', state: 'NY', loc: 'Buffalo, NY' },
  { city: 'Albany', state: 'NY', loc: 'Albany, NY' },
  { city: 'Rochester', state: 'NY', loc: 'Rochester, NY' },
  { city: 'White Plains', state: 'NY', loc: 'White Plains, NY' },
  { city: 'Staten Island', state: 'NY', loc: 'Staten Island, NY' },
  { city: 'Hempstead', state: 'NY', loc: 'Hempstead, NY' },
  // North Carolina
  { city: 'Charlotte', state: 'NC', loc: 'Charlotte, NC' },
  { city: 'Raleigh', state: 'NC', loc: 'Raleigh, NC' },
  { city: 'Durham', state: 'NC', loc: 'Durham, NC' },
  { city: 'Greensboro', state: 'NC', loc: 'Greensboro, NC' },
  // North Dakota
  { city: 'Fargo', state: 'ND', loc: 'Fargo, ND' },
  // Ohio
  { city: 'Columbus', state: 'OH', loc: 'Columbus, OH' },
  { city: 'Cleveland', state: 'OH', loc: 'Cleveland, OH' },
  { city: 'Cincinnati', state: 'OH', loc: 'Cincinnati, OH' },
  { city: 'Toledo', state: 'OH', loc: 'Toledo, OH' },
  { city: 'Akron', state: 'OH', loc: 'Akron, OH' },
  { city: 'Dayton', state: 'OH', loc: 'Dayton, OH' },
  // Oklahoma
  { city: 'Oklahoma City', state: 'OK', loc: 'Oklahoma City, OK' },
  { city: 'Tulsa', state: 'OK', loc: 'Tulsa, OK' },
  // Oregon
  { city: 'Portland', state: 'OR', loc: 'Portland, OR' },
  { city: 'Salem', state: 'OR', loc: 'Salem, OR' },
  { city: 'Eugene', state: 'OR', loc: 'Eugene, OR' },
  // Pennsylvania
  { city: 'Philadelphia', state: 'PA', loc: 'Philadelphia, PA' },
  { city: 'Pittsburgh', state: 'PA', loc: 'Pittsburgh, PA' },
  { city: 'Harrisburg', state: 'PA', loc: 'Harrisburg, PA' },
  { city: 'Allentown', state: 'PA', loc: 'Allentown, PA' },
  // Rhode Island
  { city: 'Providence', state: 'RI', loc: 'Providence, RI' },
  // South Carolina
  { city: 'Charleston', state: 'SC', loc: 'Charleston, SC' },
  { city: 'Columbia', state: 'SC', loc: 'Columbia, SC' },
  { city: 'Greenville', state: 'SC', loc: 'Greenville, SC' },
  // South Dakota
  { city: 'Sioux Falls', state: 'SD', loc: 'Sioux Falls, SD' },
  // Tennessee
  { city: 'Nashville', state: 'TN', loc: 'Nashville, TN' },
  { city: 'Memphis', state: 'TN', loc: 'Memphis, TN' },
  { city: 'Knoxville', state: 'TN', loc: 'Knoxville, TN' },
  { city: 'Chattanooga', state: 'TN', loc: 'Chattanooga, TN' },
  // Texas
  { city: 'Houston', state: 'TX', loc: 'Houston, TX' },
  { city: 'Dallas', state: 'TX', loc: 'Dallas, TX' },
  { city: 'San Antonio', state: 'TX', loc: 'San Antonio, TX' },
  { city: 'Austin', state: 'TX', loc: 'Austin, TX' },
  { city: 'Fort Worth', state: 'TX', loc: 'Fort Worth, TX' },
  { city: 'El Paso', state: 'TX', loc: 'El Paso, TX' },
  { city: 'Arlington', state: 'TX', loc: 'Arlington, TX' },
  { city: 'McAllen', state: 'TX', loc: 'McAllen, TX' },
  { city: 'Laredo', state: 'TX', loc: 'Laredo, TX' },
  { city: 'Plano', state: 'TX', loc: 'Plano, TX' },
  { city: 'Brownsville', state: 'TX', loc: 'Brownsville, TX' },
  // Utah
  { city: 'Salt Lake City', state: 'UT', loc: 'Salt Lake City, UT' },
  { city: 'Provo', state: 'UT', loc: 'Provo, UT' },
  // Vermont
  { city: 'Burlington', state: 'VT', loc: 'Burlington, VT' },
  // Virginia
  { city: 'Virginia Beach', state: 'VA', loc: 'Virginia Beach, VA' },
  { city: 'Arlington', state: 'VA', loc: 'Arlington, VA' },
  { city: 'Richmond', state: 'VA', loc: 'Richmond, VA' },
  { city: 'Fairfax', state: 'VA', loc: 'Fairfax, VA' },
  { city: 'Alexandria', state: 'VA', loc: 'Alexandria, VA' },
  // Washington
  { city: 'Seattle', state: 'WA', loc: 'Seattle, WA' },
  { city: 'Tacoma', state: 'WA', loc: 'Tacoma, WA' },
  { city: 'Spokane', state: 'WA', loc: 'Spokane, WA' },
  { city: 'Bellevue', state: 'WA', loc: 'Bellevue, WA' },
  // West Virginia
  { city: 'Charleston', state: 'WV', loc: 'Charleston, WV' },
  // Wisconsin
  { city: 'Milwaukee', state: 'WI', loc: 'Milwaukee, WI' },
  { city: 'Madison', state: 'WI', loc: 'Madison, WI' },
  // Wyoming
  { city: 'Cheyenne', state: 'WY', loc: 'Cheyenne, WY' },
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
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|APC|APLC)/gi, '')
      .replace(/\s*(College\s+(?:Admissions?|Counseling|Planning|Consulting|Prep)|Educational?\s+(?:Consulting|Consultants?|Services?|Group)|SAT\s+(?:Prep|Tutoring)|ACT\s+(?:Prep|Tutoring)|Test\s+Prep|Tutoring\s+(?:Center|Services?|Group)|Academic\s+(?:Coaching|Consulting|Services?)|Consulting\s+(?:Group|Services?|Firm)|& Associates?|Associates?|A Professional\s+.*)/gi, '')
      .trim();
  }

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  const skipWords = /^(college|education|educational|academic|consulting|consultants?|counseling|counselor|tutoring|learning|prep|planning|services?|center|group|associates?|firm|inc|llc)$/i;

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

function parseLocality(localityText) {
  if (!localityText) return { city: '', state: '', zip: '' };
  const match = localityText.match(/^(.+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?/);
  if (match) {
    return { city: match[1].trim(), state: match[2], zip: (match[3] || '').trim() };
  }
  return { city: localityText.trim(), state: '', zip: '' };
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

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

// ── Page Parser (runs inside Puppeteer) ─────────────────────────────────────

async function extractResultsFromPage(page) {
  return await page.evaluate(() => {
    const results = [];
    document.querySelectorAll('.result').forEach(el => {
      const nameEl = el.querySelector('.business-name');
      if (!nameEl) return;

      const name = nameEl.textContent.trim();
      if (!name) return;

      const phone = el.querySelector('.phones.phone.primary')
        ? el.querySelector('.phones.phone.primary').textContent.trim()
        : '';
      const street = el.querySelector('.street-address')
        ? el.querySelector('.street-address').textContent.trim()
        : '';
      const locality = el.querySelector('.locality')
        ? el.querySelector('.locality').textContent.trim()
        : '';
      const categories = el.querySelector('.categories')
        ? el.querySelector('.categories').textContent.trim().replace(/\s+/g, ' ')
        : '';
      const profileUrl = nameEl.href || '';

      // Website: look for the track-visit-website link
      let website = '';
      const wsLink = el.querySelector('a.track-visit-website');
      if (wsLink) {
        website = wsLink.href;
      }

      results.push({ name, phone, street, locality, website, categories, profileUrl });
    });
    return results;
  });
}

async function getResultCount(page) {
  return await page.evaluate(() => {
    const el = document.querySelector('.showing-count');
    if (!el) return 0;
    const match = el.textContent.match(/of\s+([\d,]+)/);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  });
}

async function hasNextPage(page) {
  return await page.evaluate(() => {
    const links = document.querySelectorAll('.pagination a');
    for (const a of links) {
      if (a.textContent.trim() === 'Next') return true;
    }
    return false;
  });
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

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllCities = args.includes('--all-cities');
  const isResume = args.includes('--resume');

  if (!isTest && !isAllCities && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-yellowpages-college.js --test         # Test mode (3 cities)');
    console.log('  node scripts/scrape-yellowpages-college.js --all-cities   # All cities');
    console.log('  node scripts/scrape-yellowpages-college.js --resume       # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  let cities = [...CITIES];
  let allLeads = [];
  let completedSearches = [];
  const seenKeys = new Set();

  if (isTest) {
    cities = cities.slice(0, 3);
    log('TEST MODE: 3 cities only');
  }

  // Resume support
  if (isResume || isAllCities) {
    const progress = loadProgress();
    if (progress.completedSearches.length > 0 && (isResume || isAllCities)) {
      completedSearches = progress.completedSearches;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedSearches.length} searches done, ${allLeads.length} leads loaded`);
    }
  }

  // Build search list: each city x each search term
  const allSearches = [];
  for (const cityInfo of cities) {
    for (const term of SEARCH_TERMS) {
      const searchKey = `${cityInfo.loc}|${term}`;
      if (!completedSearches.includes(searchKey)) {
        allSearches.push({ cityInfo, term, searchKey });
      }
    }
  }

  log(`YellowPages.com College Admissions Consultants Scraper`);
  log(`Cities: ${cities.length} | Search terms: ${SEARCH_TERMS.length} | Searches remaining: ${allSearches.length}`);
  log(`Test: ${isTest} | Max pages per search: ${isTest ? 2 : MAX_PAGES}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Browser management functions
  async function launchBrowser() {
    const b = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
      ],
    });
    const p = await b.newPage();
    await p.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await p.setViewport({ width: 1280, height: 800 });

    // Block images, fonts, CSS to speed up loading
    await p.setRequestInterception(true);
    p.on('request', req => {
      const type = req.resourceType();
      if (['image', 'font', 'stylesheet', 'media'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    return { browser: b, page: p };
  }

  // Wait for Cloudflare challenge to resolve
  async function waitForCloudflare(page, maxWait = CF_CHALLENGE_WAIT) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const title = await page.title();
      if (!title.includes('moment') && !title.includes('Cloudflare') && !title.includes('Just a')) {
        return true;
      }
      await sleep(2000);
    }
    return false;
  }

  // Navigate to URL with Cloudflare retry logic
  async function navigateWithCF(page, url) {
    for (let attempt = 0; attempt < CF_MAX_RETRIES; attempt++) {
      await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: NAV_TIMEOUT,
      });
      await sleep(PAGE_LOAD_WAIT);

      const title = await page.title();
      if (title.includes('moment') || title.includes('Cloudflare') || title.includes('Just a')) {
        log(`  Cloudflare challenge detected (attempt ${attempt + 1}/${CF_MAX_RETRIES}), waiting...`);
        const resolved = await waitForCloudflare(page);
        if (resolved) {
          log(`  Challenge resolved`);
          return true;
        }
        if (attempt < CF_MAX_RETRIES - 1) {
          log(`  Challenge not resolved, retrying after longer wait...`);
          await sleep(10000 * (attempt + 1));
        }
      } else if (title.includes('blocked') || title.includes('Access Denied')) {
        log(`  ACCESS DENIED, waiting 30s...`);
        await sleep(30000);
      } else {
        return true;
      }
    }
    return false;
  }

  let { browser, page } = await launchBrowser();
  let searchesSinceBrowserRestart = 0;

  const maxPagesPerSearch = isTest ? 2 : MAX_PAGES;
  let processed = 0;
  let totalNew = 0;
  let newSinceLastSave = 0;
  let errors = 0;
  let rateLimited = 0;
  let cfBlocks = 0;

  for (const search of allSearches) {
    processed++;
    searchesSinceBrowserRestart++;
    const { cityInfo, term, searchKey } = search;
    const encodedTerms = encodeURIComponent(term);
    const encodedLoc = encodeURIComponent(cityInfo.loc);
    const baseUrl = `https://www.yellowpages.com/search?search_terms=${encodedTerms}&geo_location_terms=${encodedLoc}`;

    // Restart browser periodically to avoid fingerprinting/rate-limiting
    if (searchesSinceBrowserRestart >= BROWSER_RESTART_EVERY) {
      log(`  [Restarting browser to avoid rate-limiting]`);
      try { await browser.close(); } catch {}
      await sleep(5000);
      ({ browser, page } = await launchBrowser());
      searchesSinceBrowserRestart = 0;
    }

    log(`[${processed}/${allSearches.length}] "${term}" in ${cityInfo.city}, ${cityInfo.state}`);

    let pageNum = 1;
    let cityNewCount = 0;
    let cityTotalCount = 0;

    while (pageNum <= maxPagesPerSearch) {
      const url = pageNum === 1 ? baseUrl : `${baseUrl}&page=${pageNum}`;

      try {
        const loaded = await navigateWithCF(page, url);
        if (!loaded) {
          log(`  Cloudflare blocked — restarting browser and retrying...`);
          cfBlocks++;
          try { await browser.close(); } catch {}
          await sleep(15000);
          ({ browser, page } = await launchBrowser());
          searchesSinceBrowserRestart = 0;

          // One more attempt with fresh browser
          const retryLoaded = await navigateWithCF(page, url);
          if (!retryLoaded) {
            log(`  Still blocked after browser restart — skipping this search`);
            rateLimited++;
            break;
          }
        }

        // Check for "no results" or "did not match"
        const noResults = await page.evaluate(() => {
          const body = document.body.textContent || '';
          return body.includes('did not match') || body.includes('No results found') || body.includes('0 results');
        });
        if (noResults) {
          if (pageNum === 1) log(`  No results found`);
          break;
        }

        // Get total count on first page
        if (pageNum === 1) {
          const total = await getResultCount(page);
          if (total > 0) {
            const totalPages = Math.min(Math.ceil(total / RESULTS_PER_PAGE), maxPagesPerSearch);
            log(`  ${total} total results (~${totalPages} pages)`);
          }
        }

        // Extract results
        const results = await extractResultsFromPage(page);
        if (results.length === 0) break;

        cityTotalCount += results.length;

        for (const r of results) {
          const parsed = parseLocality(r.locality);
          const city = parsed.city || cityInfo.city;
          const state = parsed.state || cityInfo.state;
          const key = dedupKey(r.name, city);

          if (seenKeys.has(key)) continue;
          seenKeys.add(key);

          const { first_name, last_name } = parseName(r.name);

          allLeads.push({
            first_name,
            last_name,
            firm_name: r.name,
            title: '',
            email: '',
            phone: formatPhone(r.phone),
            website: r.website,
            domain: extractDomain(r.website),
            city,
            state,
            country: 'US',
            niche: 'education',
            source: 'yellowpages',
            profile_url: r.profileUrl,
          });
          cityNewCount++;
          newSinceLastSave++;
        }

        // Incremental save every INCREMENTAL_SAVE_EVERY new results
        if (newSinceLastSave >= INCREMENTAL_SAVE_EVERY && !isTest) {
          writeCSV(allLeads, OUT_FILE);
          log(`  [Incremental save: ${allLeads.length} total leads written to CSV]`);
          newSinceLastSave = 0;
        }

        // Check for next page
        const hasNext = await hasNextPage(page);
        if (!hasNext) break;

        pageNum++;
        await randomDelay();

      } catch (err) {
        log(`  ERROR on page ${pageNum}: ${err.message}`);
        errors++;
        // If navigation error, restart browser
        if (err.message.includes('Navigation') || err.message.includes('closed') || err.message.includes('detached')) {
          try { await browser.close(); } catch {}
          await sleep(10000);
          ({ browser, page } = await launchBrowser());
          searchesSinceBrowserRestart = 0;
        }
        break;
      }
    }

    totalNew += cityNewCount;
    log(`  Found ${cityTotalCount} listings, ${cityNewCount} new`);

    completedSearches.push(searchKey);

    // Save progress every 10 searches
    if (processed % 10 === 0 && !isTest) {
      saveProgress(completedSearches, allLeads);
      writeCSV(allLeads, OUT_FILE);
      log(`  [Progress saved: ${allLeads.length} total leads]`);
    }

    await randomDelay();
  }

  try { await browser.close(); } catch {}

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) {
    saveProgress(completedSearches, allLeads);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Searches completed: ${processed} (${errors} errors, ${rateLimited} rate-limited, ${cfBlocks} CF blocks)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
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
