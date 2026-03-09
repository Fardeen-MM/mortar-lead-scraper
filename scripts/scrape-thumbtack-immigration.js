#!/usr/bin/env node
/**
 * Thumbtack Immigration Lawyers Scraper
 *
 * Scrapes https://www.thumbtack.com/{state}/{city}/immigration-lawyers/
 * using Puppeteer + Stealth (Next.js SPA with anti-bot measures).
 *
 * Strategy:
 *   1. Iterate through 150+ major US cities covering all 50 states + DC
 *   2. Load each city's listing page with Puppeteer
 *   3. Extract data from __NEXT_DATA__ JSON (proListResults) and JSON-LD
 *   4. Click "See more" to load additional results
 *   5. Visit each profile page to get city/state/zip details
 *   6. Dedup by business name + city
 *   7. Resume support via progress file
 *   8. Output CSV
 *
 * Data available on Thumbtack:
 *   - Business name, city, state, zip code
 *   - Rating, review count, hires count
 *   - Profile URL, years in business
 *   - NOTE: No phone, email, or website — Thumbtack hides contact info
 *
 * Output: output/us-immigration-lawyers-thumbtack.csv
 *
 * Usage:
 *   node scripts/scrape-thumbtack-immigration.js --test         # 5 cities
 *   node scripts/scrape-thumbtack-immigration.js --all-cities   # All 150+ cities
 *   node scripts/scrape-thumbtack-immigration.js --resume       # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const NAV_TIMEOUT = 30000;
const PAGE_LOAD_WAIT = 2000;
const NEXT_DATA_WAIT = 5000;  // Extra wait for __NEXT_DATA__ to appear

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'us-immigration-lawyers-thumbtack.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'thumbtack-immigration-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── City Database (150+ cities covering all 50 states + DC) ──────────────────
// URL pattern: /{state-lower}/{city-slug}/immigration-lawyers/

const CITIES = [
  // Alabama
  { city: 'Birmingham', state: 'AL', stateSlug: 'al', citySlug: 'birmingham' },
  { city: 'Huntsville', state: 'AL', stateSlug: 'al', citySlug: 'huntsville' },
  { city: 'Mobile', state: 'AL', stateSlug: 'al', citySlug: 'mobile' },
  // Alaska
  { city: 'Anchorage', state: 'AK', stateSlug: 'ak', citySlug: 'anchorage' },
  // Arizona
  { city: 'Phoenix', state: 'AZ', stateSlug: 'az', citySlug: 'phoenix' },
  { city: 'Tucson', state: 'AZ', stateSlug: 'az', citySlug: 'tucson' },
  { city: 'Mesa', state: 'AZ', stateSlug: 'az', citySlug: 'mesa' },
  { city: 'Scottsdale', state: 'AZ', stateSlug: 'az', citySlug: 'scottsdale' },
  // Arkansas
  { city: 'Little Rock', state: 'AR', stateSlug: 'ar', citySlug: 'little-rock' },
  { city: 'Fayetteville', state: 'AR', stateSlug: 'ar', citySlug: 'fayetteville' },
  // California
  { city: 'Los Angeles', state: 'CA', stateSlug: 'ca', citySlug: 'los-angeles' },
  { city: 'San Francisco', state: 'CA', stateSlug: 'ca', citySlug: 'san-francisco' },
  { city: 'San Diego', state: 'CA', stateSlug: 'ca', citySlug: 'san-diego' },
  { city: 'San Jose', state: 'CA', stateSlug: 'ca', citySlug: 'san-jose' },
  { city: 'Sacramento', state: 'CA', stateSlug: 'ca', citySlug: 'sacramento' },
  { city: 'Fresno', state: 'CA', stateSlug: 'ca', citySlug: 'fresno' },
  { city: 'Oakland', state: 'CA', stateSlug: 'ca', citySlug: 'oakland' },
  { city: 'Riverside', state: 'CA', stateSlug: 'ca', citySlug: 'riverside' },
  { city: 'Irvine', state: 'CA', stateSlug: 'ca', citySlug: 'irvine' },
  { city: 'Santa Ana', state: 'CA', stateSlug: 'ca', citySlug: 'santa-ana' },
  { city: 'Long Beach', state: 'CA', stateSlug: 'ca', citySlug: 'long-beach' },
  { city: 'Pasadena', state: 'CA', stateSlug: 'ca', citySlug: 'pasadena' },
  // Colorado
  { city: 'Denver', state: 'CO', stateSlug: 'co', citySlug: 'denver' },
  { city: 'Colorado Springs', state: 'CO', stateSlug: 'co', citySlug: 'colorado-springs' },
  { city: 'Aurora', state: 'CO', stateSlug: 'co', citySlug: 'aurora' },
  // Connecticut
  { city: 'Hartford', state: 'CT', stateSlug: 'ct', citySlug: 'hartford' },
  { city: 'New Haven', state: 'CT', stateSlug: 'ct', citySlug: 'new-haven' },
  { city: 'Stamford', state: 'CT', stateSlug: 'ct', citySlug: 'stamford' },
  { city: 'Bridgeport', state: 'CT', stateSlug: 'ct', citySlug: 'bridgeport' },
  // Delaware
  { city: 'Wilmington', state: 'DE', stateSlug: 'de', citySlug: 'wilmington' },
  // District of Columbia
  { city: 'Washington', state: 'DC', stateSlug: 'dc', citySlug: 'washington' },
  // Florida
  { city: 'Miami', state: 'FL', stateSlug: 'fl', citySlug: 'miami' },
  { city: 'Orlando', state: 'FL', stateSlug: 'fl', citySlug: 'orlando' },
  { city: 'Tampa', state: 'FL', stateSlug: 'fl', citySlug: 'tampa' },
  { city: 'Jacksonville', state: 'FL', stateSlug: 'fl', citySlug: 'jacksonville' },
  { city: 'Fort Lauderdale', state: 'FL', stateSlug: 'fl', citySlug: 'fort-lauderdale' },
  { city: 'West Palm Beach', state: 'FL', stateSlug: 'fl', citySlug: 'west-palm-beach' },
  { city: 'Hialeah', state: 'FL', stateSlug: 'fl', citySlug: 'hialeah' },
  { city: 'St. Petersburg', state: 'FL', stateSlug: 'fl', citySlug: 'saint-petersburg' },
  // Georgia
  { city: 'Atlanta', state: 'GA', stateSlug: 'ga', citySlug: 'atlanta' },
  { city: 'Savannah', state: 'GA', stateSlug: 'ga', citySlug: 'savannah' },
  { city: 'Augusta', state: 'GA', stateSlug: 'ga', citySlug: 'augusta' },
  { city: 'Marietta', state: 'GA', stateSlug: 'ga', citySlug: 'marietta' },
  // Hawaii
  { city: 'Honolulu', state: 'HI', stateSlug: 'hi', citySlug: 'honolulu' },
  // Idaho
  { city: 'Boise', state: 'ID', stateSlug: 'id', citySlug: 'boise' },
  // Illinois
  { city: 'Chicago', state: 'IL', stateSlug: 'il', citySlug: 'chicago' },
  { city: 'Aurora', state: 'IL', stateSlug: 'il', citySlug: 'aurora' },
  { city: 'Naperville', state: 'IL', stateSlug: 'il', citySlug: 'naperville' },
  { city: 'Schaumburg', state: 'IL', stateSlug: 'il', citySlug: 'schaumburg' },
  // Indiana
  { city: 'Indianapolis', state: 'IN', stateSlug: 'in', citySlug: 'indianapolis' },
  { city: 'Fort Wayne', state: 'IN', stateSlug: 'in', citySlug: 'fort-wayne' },
  // Iowa
  { city: 'Des Moines', state: 'IA', stateSlug: 'ia', citySlug: 'des-moines' },
  { city: 'Cedar Rapids', state: 'IA', stateSlug: 'ia', citySlug: 'cedar-rapids' },
  // Kansas
  { city: 'Wichita', state: 'KS', stateSlug: 'ks', citySlug: 'wichita' },
  { city: 'Kansas City', state: 'KS', stateSlug: 'ks', citySlug: 'kansas-city' },
  // Kentucky
  { city: 'Louisville', state: 'KY', stateSlug: 'ky', citySlug: 'louisville' },
  { city: 'Lexington', state: 'KY', stateSlug: 'ky', citySlug: 'lexington' },
  // Louisiana
  { city: 'New Orleans', state: 'LA', stateSlug: 'la', citySlug: 'new-orleans' },
  { city: 'Baton Rouge', state: 'LA', stateSlug: 'la', citySlug: 'baton-rouge' },
  // Maine
  { city: 'Portland', state: 'ME', stateSlug: 'me', citySlug: 'portland' },
  // Maryland
  { city: 'Baltimore', state: 'MD', stateSlug: 'md', citySlug: 'baltimore' },
  { city: 'Silver Spring', state: 'MD', stateSlug: 'md', citySlug: 'silver-spring' },
  { city: 'Rockville', state: 'MD', stateSlug: 'md', citySlug: 'rockville' },
  { city: 'Bethesda', state: 'MD', stateSlug: 'md', citySlug: 'bethesda' },
  // Massachusetts
  { city: 'Boston', state: 'MA', stateSlug: 'ma', citySlug: 'boston' },
  { city: 'Worcester', state: 'MA', stateSlug: 'ma', citySlug: 'worcester' },
  { city: 'Cambridge', state: 'MA', stateSlug: 'ma', citySlug: 'cambridge' },
  // Michigan
  { city: 'Detroit', state: 'MI', stateSlug: 'mi', citySlug: 'detroit' },
  { city: 'Grand Rapids', state: 'MI', stateSlug: 'mi', citySlug: 'grand-rapids' },
  { city: 'Ann Arbor', state: 'MI', stateSlug: 'mi', citySlug: 'ann-arbor' },
  // Minnesota
  { city: 'Minneapolis', state: 'MN', stateSlug: 'mn', citySlug: 'minneapolis' },
  { city: 'Saint Paul', state: 'MN', stateSlug: 'mn', citySlug: 'saint-paul' },
  // Mississippi
  { city: 'Jackson', state: 'MS', stateSlug: 'ms', citySlug: 'jackson' },
  // Missouri
  { city: 'Kansas City', state: 'MO', stateSlug: 'mo', citySlug: 'kansas-city' },
  { city: 'Saint Louis', state: 'MO', stateSlug: 'mo', citySlug: 'saint-louis' },
  { city: 'Springfield', state: 'MO', stateSlug: 'mo', citySlug: 'springfield' },
  // Montana
  { city: 'Billings', state: 'MT', stateSlug: 'mt', citySlug: 'billings' },
  { city: 'Missoula', state: 'MT', stateSlug: 'mt', citySlug: 'missoula' },
  // Nebraska
  { city: 'Omaha', state: 'NE', stateSlug: 'ne', citySlug: 'omaha' },
  { city: 'Lincoln', state: 'NE', stateSlug: 'ne', citySlug: 'lincoln' },
  // Nevada
  { city: 'Las Vegas', state: 'NV', stateSlug: 'nv', citySlug: 'las-vegas' },
  { city: 'Reno', state: 'NV', stateSlug: 'nv', citySlug: 'reno' },
  { city: 'Henderson', state: 'NV', stateSlug: 'nv', citySlug: 'henderson' },
  // New Hampshire
  { city: 'Manchester', state: 'NH', stateSlug: 'nh', citySlug: 'manchester' },
  // New Jersey
  { city: 'Newark', state: 'NJ', stateSlug: 'nj', citySlug: 'newark' },
  { city: 'Jersey City', state: 'NJ', stateSlug: 'nj', citySlug: 'jersey-city' },
  { city: 'Elizabeth', state: 'NJ', stateSlug: 'nj', citySlug: 'elizabeth' },
  { city: 'Paterson', state: 'NJ', stateSlug: 'nj', citySlug: 'paterson' },
  { city: 'Hackensack', state: 'NJ', stateSlug: 'nj', citySlug: 'hackensack' },
  // New Mexico
  { city: 'Albuquerque', state: 'NM', stateSlug: 'nm', citySlug: 'albuquerque' },
  { city: 'Las Cruces', state: 'NM', stateSlug: 'nm', citySlug: 'las-cruces' },
  // New York
  { city: 'New York', state: 'NY', stateSlug: 'ny', citySlug: 'new-york' },
  { city: 'Brooklyn', state: 'NY', stateSlug: 'ny', citySlug: 'brooklyn' },
  { city: 'Queens', state: 'NY', stateSlug: 'ny', citySlug: 'queens' },
  { city: 'Bronx', state: 'NY', stateSlug: 'ny', citySlug: 'bronx' },
  { city: 'Buffalo', state: 'NY', stateSlug: 'ny', citySlug: 'buffalo' },
  { city: 'Albany', state: 'NY', stateSlug: 'ny', citySlug: 'albany' },
  { city: 'Rochester', state: 'NY', stateSlug: 'ny', citySlug: 'rochester' },
  { city: 'White Plains', state: 'NY', stateSlug: 'ny', citySlug: 'white-plains' },
  { city: 'Staten Island', state: 'NY', stateSlug: 'ny', citySlug: 'staten-island' },
  { city: 'Hempstead', state: 'NY', stateSlug: 'ny', citySlug: 'hempstead' },
  // North Carolina
  { city: 'Charlotte', state: 'NC', stateSlug: 'nc', citySlug: 'charlotte' },
  { city: 'Raleigh', state: 'NC', stateSlug: 'nc', citySlug: 'raleigh' },
  { city: 'Durham', state: 'NC', stateSlug: 'nc', citySlug: 'durham' },
  { city: 'Greensboro', state: 'NC', stateSlug: 'nc', citySlug: 'greensboro' },
  // North Dakota
  { city: 'Fargo', state: 'ND', stateSlug: 'nd', citySlug: 'fargo' },
  // Ohio
  { city: 'Columbus', state: 'OH', stateSlug: 'oh', citySlug: 'columbus' },
  { city: 'Cleveland', state: 'OH', stateSlug: 'oh', citySlug: 'cleveland' },
  { city: 'Cincinnati', state: 'OH', stateSlug: 'oh', citySlug: 'cincinnati' },
  { city: 'Toledo', state: 'OH', stateSlug: 'oh', citySlug: 'toledo' },
  { city: 'Akron', state: 'OH', stateSlug: 'oh', citySlug: 'akron' },
  // Oklahoma
  { city: 'Oklahoma City', state: 'OK', stateSlug: 'ok', citySlug: 'oklahoma-city' },
  { city: 'Tulsa', state: 'OK', stateSlug: 'ok', citySlug: 'tulsa' },
  // Oregon
  { city: 'Portland', state: 'OR', stateSlug: 'or', citySlug: 'portland' },
  { city: 'Salem', state: 'OR', stateSlug: 'or', citySlug: 'salem' },
  { city: 'Eugene', state: 'OR', stateSlug: 'or', citySlug: 'eugene' },
  // Pennsylvania
  { city: 'Philadelphia', state: 'PA', stateSlug: 'pa', citySlug: 'philadelphia' },
  { city: 'Pittsburgh', state: 'PA', stateSlug: 'pa', citySlug: 'pittsburgh' },
  { city: 'Harrisburg', state: 'PA', stateSlug: 'pa', citySlug: 'harrisburg' },
  { city: 'Allentown', state: 'PA', stateSlug: 'pa', citySlug: 'allentown' },
  // Rhode Island
  { city: 'Providence', state: 'RI', stateSlug: 'ri', citySlug: 'providence' },
  // South Carolina
  { city: 'Charleston', state: 'SC', stateSlug: 'sc', citySlug: 'charleston' },
  { city: 'Columbia', state: 'SC', stateSlug: 'sc', citySlug: 'columbia' },
  { city: 'Greenville', state: 'SC', stateSlug: 'sc', citySlug: 'greenville' },
  // South Dakota
  { city: 'Sioux Falls', state: 'SD', stateSlug: 'sd', citySlug: 'sioux-falls' },
  // Tennessee
  { city: 'Nashville', state: 'TN', stateSlug: 'tn', citySlug: 'nashville' },
  { city: 'Memphis', state: 'TN', stateSlug: 'tn', citySlug: 'memphis' },
  { city: 'Knoxville', state: 'TN', stateSlug: 'tn', citySlug: 'knoxville' },
  // Texas
  { city: 'Houston', state: 'TX', stateSlug: 'tx', citySlug: 'houston' },
  { city: 'Dallas', state: 'TX', stateSlug: 'tx', citySlug: 'dallas' },
  { city: 'San Antonio', state: 'TX', stateSlug: 'tx', citySlug: 'san-antonio' },
  { city: 'Austin', state: 'TX', stateSlug: 'tx', citySlug: 'austin' },
  { city: 'Fort Worth', state: 'TX', stateSlug: 'tx', citySlug: 'fort-worth' },
  { city: 'El Paso', state: 'TX', stateSlug: 'tx', citySlug: 'el-paso' },
  { city: 'Arlington', state: 'TX', stateSlug: 'tx', citySlug: 'arlington' },
  { city: 'McAllen', state: 'TX', stateSlug: 'tx', citySlug: 'mcallen' },
  { city: 'Laredo', state: 'TX', stateSlug: 'tx', citySlug: 'laredo' },
  { city: 'Plano', state: 'TX', stateSlug: 'tx', citySlug: 'plano' },
  { city: 'Brownsville', state: 'TX', stateSlug: 'tx', citySlug: 'brownsville' },
  // Utah
  { city: 'Salt Lake City', state: 'UT', stateSlug: 'ut', citySlug: 'salt-lake-city' },
  { city: 'Provo', state: 'UT', stateSlug: 'ut', citySlug: 'provo' },
  // Vermont
  { city: 'Burlington', state: 'VT', stateSlug: 'vt', citySlug: 'burlington' },
  // Virginia
  { city: 'Virginia Beach', state: 'VA', stateSlug: 'va', citySlug: 'virginia-beach' },
  { city: 'Arlington', state: 'VA', stateSlug: 'va', citySlug: 'arlington' },
  { city: 'Richmond', state: 'VA', stateSlug: 'va', citySlug: 'richmond' },
  { city: 'Fairfax', state: 'VA', stateSlug: 'va', citySlug: 'fairfax' },
  { city: 'Alexandria', state: 'VA', stateSlug: 'va', citySlug: 'alexandria' },
  // Washington
  { city: 'Seattle', state: 'WA', stateSlug: 'wa', citySlug: 'seattle' },
  { city: 'Tacoma', state: 'WA', stateSlug: 'wa', citySlug: 'tacoma' },
  { city: 'Spokane', state: 'WA', stateSlug: 'wa', citySlug: 'spokane' },
  { city: 'Bellevue', state: 'WA', stateSlug: 'wa', citySlug: 'bellevue' },
  // West Virginia
  { city: 'Charleston', state: 'WV', stateSlug: 'wv', citySlug: 'charleston' },
  // Wisconsin
  { city: 'Milwaukee', state: 'WI', stateSlug: 'wi', citySlug: 'milwaukee' },
  { city: 'Madison', state: 'WI', stateSlug: 'wi', citySlug: 'madison' },
  // Wyoming
  { city: 'Cheyenne', state: 'WY', stateSlug: 'wy', citySlug: 'cheyenne' },
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

  // Handle "Name at Firm" pattern: "Vanessa Molina at Michelle Goff Law Group PC"
  const atMatch = cleaned.match(/^([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)\s+at\s+/i);
  if (atMatch) {
    cleaned = atMatch[1];
  }

  // Handle "Firm (PersonName, Esq.)" pattern: "CHO LAW LLC (Minwon Cho, Esq.)"
  const parenMatch = cleaned.match(/\(([^)]+)\)/);
  if (parenMatch) {
    const inner = parenMatch[1]
      .replace(/,?\s*Esq\.?/gi, '')
      .replace(/,?\s*(Jr\.?|Sr\.?|III|IV|II)$/gi, '')
      .trim();
    if (/^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(inner)) {
      const parts = inner.split(/\s+/);
      return {
        first_name: parts[0],
        last_name: parts[parts.length - 1],
      };
    }
  }

  // "Law Office(s) of John Smith" / "Law Firm of John Smith"
  const ofMatch = cleaned.match(/(?:Law\s+(?:Office|Offices|Firm|Group|Practice|Center)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|Attorney.*|Law.*|&.*)/gi, '')
      .trim();
  } else if (!atMatch) {
    // Strip common suffixes to try to get a person name
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .replace(/\s*(Law\s+(?:Firm|Group|Office|Offices|Practice|Center)|Legal\s+(?:Group|Services?|Team)|Immigration\s+(?:Law|Attorneys?|Lawyers?)|Attorneys?\s+at\s+Law|Attorneys?|Lawyers?|& Associates?|Associates?)/gi, '')
      .trim();
  }

  // Remove trailing punctuation
  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip names starting with "The"
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip multi-partner names (Smith & Jones)
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  const lawWords = /^(law|legal|firm|group|office|pllc|plc|pc|immigration|attorneys?|lawyers?|counselors?|associates?|services?)$/i;

  // If what remains looks like a person name (2-4 words)
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    if (lawWords.test(lastName)) return { first_name: '', last_name: '' };
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

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedCities: [], leads: [] };
}

function saveProgress(completedCities, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedCities,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Thumbtack Page Extraction ────────────────────────────────────────────────

/**
 * Extract listings from a Thumbtack city page using Puppeteer.
 * Strategy:
 *   1. Load city listing page using domcontentloaded (faster)
 *   2. Wait for __NEXT_DATA__ to appear
 *   3. Extract from __NEXT_DATA__ Apollo cache (richest data)
 *   4. Extract from JSON-LD (ItemList > LocalBusiness) as backup
 *   5. Extract from visible DOM cards as fallback
 *   6. Try clicking "See more" to load additional results
 *
 * NOTE: Thumbtack does NOT expose phone, email, or website on any page.
 * Contact info is hidden behind their messaging system. We extract:
 * business name, city, state, rating, review count, profile URL.
 */
async function extractListings(page, cityInfo) {
  const url = `https://www.thumbtack.com/${cityInfo.stateSlug}/${cityInfo.citySlug}/immigration-lawyers/`;

  try {
    // Use networkidle2 — Thumbtack needs JS to finish rendering
    await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  } catch (err) {
    // Timeout is OK — Thumbtack has many background requests
    // The page data is usually loaded before timeout
  }

  // Wait for __NEXT_DATA__ to appear and page to hydrate
  try {
    await page.waitForSelector('#__NEXT_DATA__', { timeout: NEXT_DATA_WAIT });
  } catch {
    // May already be there
  }

  await sleep(PAGE_LOAD_WAIT);

  // Check for Cloudflare / bot detection
  const pageTitle = await page.title();
  if (pageTitle.includes('Just a moment') || pageTitle.includes('Attention Required')) {
    log(`  Cloudflare challenge detected, waiting 12s...`);
    await sleep(12000);
  }

  // Check if redirected completely away
  const currentUrl = page.url();
  if (!currentUrl.includes('immigration') && !currentUrl.includes(cityInfo.citySlug)) {
    log(`  Redirected to: ${currentUrl.slice(0, 80)} — skipping`);
    return [];
  }

  // Strategy 1: Extract from __NEXT_DATA__ (richest source)
  // Data lives at: props.pageProps.frontDoorPage.proListSection.proListResults[]
  // Each ProListResult has: businessSummaryPrefab.businessSummary.businessName,
  //   servicePk, url, businessFacts (hires, location), reviewSummaryPrefab
  let listings = await page.evaluate(() => {
    const results = [];
    try {
      const nextDataEl = document.querySelector('#__NEXT_DATA__');
      if (!nextDataEl) return results;

      const data = JSON.parse(nextDataEl.textContent);
      const props = data && data.props && data.props.pageProps ? data.props.pageProps : {};
      const fdp = props.frontDoorPage;
      if (!fdp) return results;

      // Primary path: frontDoorPage.proListSection.proListResults
      const proListResults = fdp.proListSection && fdp.proListSection.proListResults
        ? fdp.proListSection.proListResults : [];

      for (const pro of proListResults) {
        if (!pro) continue;

        // Business name is nested in businessSummaryPrefab
        const bs = pro.businessSummaryPrefab && pro.businessSummaryPrefab.businessSummary
          ? pro.businessSummaryPrefab.businessSummary : {};

        const businessName = bs.businessName || '';
        if (!businessName) continue;

        // Rating info
        const rs = bs.reviewSummaryPrefab && bs.reviewSummaryPrefab.reviewSummary
          ? bs.reviewSummaryPrefab.reviewSummary : {};
        const rating = rs.averageRating && rs.averageRating.rating ? rs.averageRating.rating : 0;
        const reviewCount = rs.numReviews || 0;

        // Hires from businessFacts
        let numHires = 0;
        const facts = pro.businessFacts || [];
        for (const fact of facts) {
          const desc = fact.description || '';
          const hiresMatch = desc.match(/(\d+)\s+hires?\s+on\s+Thumbtack/i);
          if (hiresMatch) numHires = parseInt(hiresMatch[1], 10);
        }

        results.push({
          businessName,
          servicePk: pro.servicePk || '',
          reviewCount,
          rating,
          numHires,
          profileUrl: pro.url || '',
          badge: bs.badge || '',
        });
      }
    } catch (e) {}
    return results;
  });

  // Strategy 2: Extract from JSON-LD ItemList
  const jsonLdListings = await page.evaluate(() => {
    const results = [];
    try {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of scripts) {
        const json = JSON.parse(script.textContent);
        if (json['@type'] === 'ItemList' && json.itemListElement) {
          for (const item of json.itemListElement) {
            const biz = item.item || item;
            if (biz && biz.name) {
              results.push({
                businessName: biz.name,
                rating: biz.aggregateRating?.ratingValue || 0,
                reviewCount: biz.aggregateRating?.reviewCount || 0,
                profileUrl: biz.url || '',
              });
            }
          }
        }
      }
    } catch (e) {}
    return results;
  });

  // Strategy 3: Try clicking "See more" to load additional results
  try {
    // Try multiple selectors for the "See more" button
    const seeMoreSelectors = [
      'a[data-testid="see-more-cta"]',
      'a[href*="immigration-attorney"]',
      'button[class*="SeeMore"]',
    ];
    for (const sel of seeMoreSelectors) {
      const btn = await page.$(sel);
      if (btn) {
        const btnText = await page.evaluate(el => el.textContent, btn);
        if (btnText && btnText.toLowerCase().includes('see more')) {
          log(`  Found "See more" button, clicking...`);
          await btn.click();
          await sleep(3000);
          break;
        }
      }
    }
  } catch (e) {
    // Not critical
  }

  // Strategy 4: DOM link extraction — find profile links in page
  const domListings = await page.evaluate(() => {
    const results = [];
    const seenServicePks = new Set();
    try {
      const allLinks = document.querySelectorAll('a[href*="/immigration-attorney/"][href*="/service/"]');
      for (const link of allLinks) {
        const href = link.getAttribute('href') || '';

        // Skip external/share links
        if (href.includes('fb-messenger') || href.includes('facebook.com') ||
            href.includes('twitter.com') || href.includes('share') ||
            href.includes('mailto:')) continue;
        if (href.startsWith('http') && !href.includes('thumbtack.com')) continue;

        // Skip review section links (contain #ServicePageReviews)
        if (href.includes('#ServicePageReview')) continue;
        if (href.includes('#Service')) continue;

        // Extract servicePk to dedup
        const pkMatch = href.match(/\/service\/(\d+)/);
        if (!pkMatch) continue;
        const pk = pkMatch[1];
        if (seenServicePks.has(pk)) continue;
        seenServicePks.add(pk);

        // Extract business name from the slug in the URL
        const slugMatch = href.match(/\/immigration-attorney\/([^/]+)\/service\//);
        const slug = slugMatch ? slugMatch[1] : '';
        // Convert slug to readable name
        const nameFromSlug = slug
          .replace(/-/g, ' ')
          .replace(/\b\w/g, c => c.toUpperCase());

        results.push({
          businessName: nameFromSlug || '',
          profileUrl: href,
          servicePk: pk,
        });
      }
    } catch (e) {}
    return results;
  });

  // Merge all results, dedup by business name
  const seen = new Set();
  const merged = [];

  for (const list of [listings, jsonLdListings, domListings]) {
    for (const item of list) {
      const key = (item.businessName || '').toLowerCase().trim();
      if (key && !seen.has(key)) {
        seen.add(key);
        merged.push(item);
      }
    }
  }

  return merged;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllCities = args.includes('--all-cities');
  const isResume = args.includes('--resume');

  if (!isTest && !isAllCities && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-thumbtack-immigration.js --test         # Test mode (5 cities)');
    console.log('  node scripts/scrape-thumbtack-immigration.js --all-cities   # All cities');
    console.log('  node scripts/scrape-thumbtack-immigration.js --resume       # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  let cities = [...CITIES];
  let allLeads = [];
  let completedCities = [];
  const seenKeys = new Set();

  if (isTest) {
    cities = cities.slice(0, 5);
    log('TEST MODE: 5 cities only');
  }

  // Resume support
  if (isResume || isAllCities) {
    const progress = loadProgress();
    if (progress.completedCities.length > 0 && (isResume || isAllCities)) {
      completedCities = progress.completedCities;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
        const firmKey = 'firm:' + (lead.firm_name || '').toLowerCase().trim();
        if (firmKey !== 'firm:') seenKeys.add(firmKey);
      }
      log(`Resuming: ${completedCities.length} cities already done, ${allLeads.length} leads loaded`);
    }
  }

  const citiesToScrape = cities.filter(c => {
    const key = `${c.stateSlug}/${c.citySlug}`;
    return !completedCities.includes(key);
  });

  log(`Thumbtack Immigration Lawyers Scraper`);
  log(`Total cities: ${cities.length} | Remaining: ${citiesToScrape.length} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Launch Puppeteer
  log('Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      '--window-size=1920,1080',
    ],
  });

  let page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

  // Block heavy resources to speed things up
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (['image', 'font', 'media'].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let emptyPages = 0;

  for (const cityInfo of citiesToScrape) {
    processed++;
    const cityKey = `${cityInfo.stateSlug}/${cityInfo.citySlug}`;
    log(`[${processed}/${citiesToScrape.length}] ${cityInfo.city}, ${cityInfo.state}`);

    try {
      // Extract listings from the city page
      const listings = await extractListings(page, cityInfo);

      if (listings.length === 0) {
        log(`  No listings found`);
        emptyPages++;
        completedCities.push(cityKey);
        await randomDelay();
        continue;
      }

      log(`  Found ${listings.length} listings on city page`);

      let newCount = 0;

      for (const listing of listings) {
        const firmName = listing.businessName || '';
        const city = listing.city || cityInfo.city;
        const state = listing.state || cityInfo.state;

        // Dedup by servicePk (most reliable) and also by firm_name+city
        const spk = listing.servicePk || '';
        if (spk && seenKeys.has('spk:' + spk)) continue;
        const key = dedupKey(firmName, city);
        if (seenKeys.has(key)) continue;

        // Also dedup by firm_name alone (same firm in different nearby cities)
        const firmKey = 'firm:' + (firmName || '').toLowerCase().trim();
        if (firmKey !== 'firm:' && seenKeys.has(firmKey)) continue;

        if (spk) seenKeys.add('spk:' + spk);
        seenKeys.add(key);
        seenKeys.add(firmKey);

        const { first_name, last_name } = parseName(firmName);

        // Build profile URL — strip tracking query params for cleaner URLs
        let profileUrl = listing.profileUrl || '';
        if (profileUrl && !profileUrl.startsWith('http')) {
          profileUrl = `https://www.thumbtack.com${profileUrl}`;
        }
        // Strip query params (category_pk, ir_referrer, etc.) — keep just the path
        if (profileUrl.includes('?')) {
          profileUrl = profileUrl.split('?')[0];
        }
        // Strip hash fragments too
        if (profileUrl.includes('#')) {
          profileUrl = profileUrl.split('#')[0];
        }

        // NOTE: Thumbtack does NOT expose phone, email, or website on any page.
        // Contact info is behind their messaging system. We get: name, city, state,
        // rating, reviews, hires, and profile URL.
        const lead = {
          first_name,
          last_name,
          firm_name: firmName,
          title: '',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city,
          state,
          country: 'US',
          niche: 'immigration',
          source: 'thumbtack',
          profile_url: profileUrl,
        };

        allLeads.push(lead);
        newCount++;
      }

      totalNew += newCount;
      log(`  Added ${newCount} new leads (${listings.length - newCount} dupes/skipped)`);

      completedCities.push(cityKey);

      // Save progress every 5 cities
      if (processed % 5 === 0 && !isTest) {
        saveProgress(completedCities, allLeads);
        writeCSV(allLeads, OUT_FILE);
        log(`  [Progress saved: ${allLeads.length} total leads]`);
      }

    } catch (err) {
      const msg = err.message || '';
      // Handle detached frame by creating a new page
      if (msg.includes('detached Frame') || msg.includes('Execution context')) {
        log(`  Frame detached — recreating page...`);
        try { await page.close(); } catch {}
        page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
        await page.setRequestInterception(true);
        page.on('request', (req) => {
          const type = req.resourceType();
          if (['image', 'font', 'media'].includes(type)) {
            req.abort();
          } else {
            req.continue();
          }
        });
      } else {
        log(`  ERROR: ${msg.slice(0, 100)}`);
      }
      errors++;
    }

    await randomDelay();
  }

  // Close browser
  await browser.close();

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) {
    saveProgress(completedCities, allLeads);
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
  log(`Cities scraped: ${processed} (${emptyPages} empty, ${errors} errors)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && errors === 0 && citiesToScrape.length === cities.length) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
