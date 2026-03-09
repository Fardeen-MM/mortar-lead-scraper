#!/usr/bin/env node
/**
 * Yelp Immigration Lawyers Scraper (Fusion API)
 *
 * Uses the Yelp Fusion API v3 to search for immigration lawyers.
 * Yelp's website uses DataDome anti-bot protection that blocks ALL
 * automated access (Puppeteer, curl, plain HTTP), so we use the
 * official API instead.
 *
 * API endpoint: GET https://api.yelp.com/v3/businesses/search
 * API details:  GET https://api.yelp.com/v3/businesses/{id}
 *
 * Setup:
 *   1. Create a Yelp app at https://www.yelp.com/developers/v3/manage_app
 *   2. Set YELP_API_KEY environment variable
 *
 * Data available via Yelp API:
 *   - Business name, phone, address, city, state, zip
 *   - Rating, review count, price level
 *   - Categories, Yelp URL, profile image
 *   - Coordinates (lat/lng)
 *   - Business details page: website, hours, photos
 *
 * Limits:
 *   - Free trial: 5,000 calls in 30 days
 *   - Starter plan: 300 calls/day
 *   - Search returns up to 50 per call (limit param), max offset 240
 *   - So ~5 calls per city to get all results
 *
 * Output: output/immigration-lawyers-yelp.csv
 *
 * Usage:
 *   node scripts/scrape-yelp-immigration.js --test                   # 5 cities
 *   node scripts/scrape-yelp-immigration.js --all-cities             # All cities
 *   node scripts/scrape-yelp-immigration.js --country US             # US only
 *   node scripts/scrape-yelp-immigration.js --country UK             # UK only
 *   node scripts/scrape-yelp-immigration.js --country CA             # Canada only
 *   node scripts/scrape-yelp-immigration.js --country ALL            # All countries
 *   node scripts/scrape-yelp-immigration.js --country ALL --resume   # Resume interrupted
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ── Load .env if present ─────────────────────────────────────────────────────
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}

// ── Config ───────────────────────────────────────────────────────────────────

const API_KEY = process.env.YELP_API_KEY || '';
const API_BASE = 'https://api.yelp.com/v3';
const SEARCH_LIMIT = 50;       // Max per API call
const MAX_OFFSET = 240;        // Yelp caps results at 240
const DELAY_BETWEEN_CALLS = 1200; // 1.2s between API calls (stay under QPS limit)
const DELAY_BETWEEN_CITIES = 2000; // 2s between cities
const DETAIL_DELAY = 800;      // Delay between detail calls

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-yelp.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'yelp-immigration-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url', 'rating', 'review_count', 'address',
];

// ── City Database ────────────────────────────────────────────────────────────

const US_CITIES = [
  // Alabama
  { city: 'Birmingham', state: 'AL', loc: 'Birmingham, AL', country: 'US' },
  { city: 'Huntsville', state: 'AL', loc: 'Huntsville, AL', country: 'US' },
  { city: 'Mobile', state: 'AL', loc: 'Mobile, AL', country: 'US' },
  // Alaska
  { city: 'Anchorage', state: 'AK', loc: 'Anchorage, AK', country: 'US' },
  // Arizona
  { city: 'Phoenix', state: 'AZ', loc: 'Phoenix, AZ', country: 'US' },
  { city: 'Tucson', state: 'AZ', loc: 'Tucson, AZ', country: 'US' },
  { city: 'Mesa', state: 'AZ', loc: 'Mesa, AZ', country: 'US' },
  { city: 'Scottsdale', state: 'AZ', loc: 'Scottsdale, AZ', country: 'US' },
  // Arkansas
  { city: 'Little Rock', state: 'AR', loc: 'Little Rock, AR', country: 'US' },
  { city: 'Fayetteville', state: 'AR', loc: 'Fayetteville, AR', country: 'US' },
  // California
  { city: 'Los Angeles', state: 'CA', loc: 'Los Angeles, CA', country: 'US' },
  { city: 'San Francisco', state: 'CA', loc: 'San Francisco, CA', country: 'US' },
  { city: 'San Diego', state: 'CA', loc: 'San Diego, CA', country: 'US' },
  { city: 'San Jose', state: 'CA', loc: 'San Jose, CA', country: 'US' },
  { city: 'Sacramento', state: 'CA', loc: 'Sacramento, CA', country: 'US' },
  { city: 'Fresno', state: 'CA', loc: 'Fresno, CA', country: 'US' },
  { city: 'Oakland', state: 'CA', loc: 'Oakland, CA', country: 'US' },
  { city: 'Riverside', state: 'CA', loc: 'Riverside, CA', country: 'US' },
  { city: 'Irvine', state: 'CA', loc: 'Irvine, CA', country: 'US' },
  { city: 'Santa Ana', state: 'CA', loc: 'Santa Ana, CA', country: 'US' },
  { city: 'Long Beach', state: 'CA', loc: 'Long Beach, CA', country: 'US' },
  { city: 'Pasadena', state: 'CA', loc: 'Pasadena, CA', country: 'US' },
  // Colorado
  { city: 'Denver', state: 'CO', loc: 'Denver, CO', country: 'US' },
  { city: 'Colorado Springs', state: 'CO', loc: 'Colorado Springs, CO', country: 'US' },
  { city: 'Aurora', state: 'CO', loc: 'Aurora, CO', country: 'US' },
  // Connecticut
  { city: 'Hartford', state: 'CT', loc: 'Hartford, CT', country: 'US' },
  { city: 'New Haven', state: 'CT', loc: 'New Haven, CT', country: 'US' },
  { city: 'Stamford', state: 'CT', loc: 'Stamford, CT', country: 'US' },
  { city: 'Bridgeport', state: 'CT', loc: 'Bridgeport, CT', country: 'US' },
  // Delaware
  { city: 'Wilmington', state: 'DE', loc: 'Wilmington, DE', country: 'US' },
  // District of Columbia
  { city: 'Washington', state: 'DC', loc: 'Washington, DC', country: 'US' },
  // Florida
  { city: 'Miami', state: 'FL', loc: 'Miami, FL', country: 'US' },
  { city: 'Orlando', state: 'FL', loc: 'Orlando, FL', country: 'US' },
  { city: 'Tampa', state: 'FL', loc: 'Tampa, FL', country: 'US' },
  { city: 'Jacksonville', state: 'FL', loc: 'Jacksonville, FL', country: 'US' },
  { city: 'Fort Lauderdale', state: 'FL', loc: 'Fort Lauderdale, FL', country: 'US' },
  { city: 'West Palm Beach', state: 'FL', loc: 'West Palm Beach, FL', country: 'US' },
  { city: 'Hialeah', state: 'FL', loc: 'Hialeah, FL', country: 'US' },
  { city: 'St. Petersburg', state: 'FL', loc: 'St. Petersburg, FL', country: 'US' },
  // Georgia
  { city: 'Atlanta', state: 'GA', loc: 'Atlanta, GA', country: 'US' },
  { city: 'Savannah', state: 'GA', loc: 'Savannah, GA', country: 'US' },
  { city: 'Augusta', state: 'GA', loc: 'Augusta, GA', country: 'US' },
  { city: 'Marietta', state: 'GA', loc: 'Marietta, GA', country: 'US' },
  // Hawaii
  { city: 'Honolulu', state: 'HI', loc: 'Honolulu, HI', country: 'US' },
  // Idaho
  { city: 'Boise', state: 'ID', loc: 'Boise, ID', country: 'US' },
  // Illinois
  { city: 'Chicago', state: 'IL', loc: 'Chicago, IL', country: 'US' },
  { city: 'Aurora', state: 'IL', loc: 'Aurora, IL', country: 'US' },
  { city: 'Naperville', state: 'IL', loc: 'Naperville, IL', country: 'US' },
  { city: 'Schaumburg', state: 'IL', loc: 'Schaumburg, IL', country: 'US' },
  // Indiana
  { city: 'Indianapolis', state: 'IN', loc: 'Indianapolis, IN', country: 'US' },
  { city: 'Fort Wayne', state: 'IN', loc: 'Fort Wayne, IN', country: 'US' },
  // Iowa
  { city: 'Des Moines', state: 'IA', loc: 'Des Moines, IA', country: 'US' },
  { city: 'Cedar Rapids', state: 'IA', loc: 'Cedar Rapids, IA', country: 'US' },
  // Kansas
  { city: 'Wichita', state: 'KS', loc: 'Wichita, KS', country: 'US' },
  { city: 'Kansas City', state: 'KS', loc: 'Kansas City, KS', country: 'US' },
  // Kentucky
  { city: 'Louisville', state: 'KY', loc: 'Louisville, KY', country: 'US' },
  { city: 'Lexington', state: 'KY', loc: 'Lexington, KY', country: 'US' },
  // Louisiana
  { city: 'New Orleans', state: 'LA', loc: 'New Orleans, LA', country: 'US' },
  { city: 'Baton Rouge', state: 'LA', loc: 'Baton Rouge, LA', country: 'US' },
  // Maine
  { city: 'Portland', state: 'ME', loc: 'Portland, ME', country: 'US' },
  // Maryland
  { city: 'Baltimore', state: 'MD', loc: 'Baltimore, MD', country: 'US' },
  { city: 'Silver Spring', state: 'MD', loc: 'Silver Spring, MD', country: 'US' },
  { city: 'Rockville', state: 'MD', loc: 'Rockville, MD', country: 'US' },
  { city: 'Bethesda', state: 'MD', loc: 'Bethesda, MD', country: 'US' },
  // Massachusetts
  { city: 'Boston', state: 'MA', loc: 'Boston, MA', country: 'US' },
  { city: 'Worcester', state: 'MA', loc: 'Worcester, MA', country: 'US' },
  { city: 'Cambridge', state: 'MA', loc: 'Cambridge, MA', country: 'US' },
  // Michigan
  { city: 'Detroit', state: 'MI', loc: 'Detroit, MI', country: 'US' },
  { city: 'Grand Rapids', state: 'MI', loc: 'Grand Rapids, MI', country: 'US' },
  { city: 'Ann Arbor', state: 'MI', loc: 'Ann Arbor, MI', country: 'US' },
  // Minnesota
  { city: 'Minneapolis', state: 'MN', loc: 'Minneapolis, MN', country: 'US' },
  { city: 'Saint Paul', state: 'MN', loc: 'Saint Paul, MN', country: 'US' },
  // Mississippi
  { city: 'Jackson', state: 'MS', loc: 'Jackson, MS', country: 'US' },
  // Missouri
  { city: 'Kansas City', state: 'MO', loc: 'Kansas City, MO', country: 'US' },
  { city: 'Saint Louis', state: 'MO', loc: 'Saint Louis, MO', country: 'US' },
  { city: 'Springfield', state: 'MO', loc: 'Springfield, MO', country: 'US' },
  // Montana
  { city: 'Billings', state: 'MT', loc: 'Billings, MT', country: 'US' },
  { city: 'Missoula', state: 'MT', loc: 'Missoula, MT', country: 'US' },
  // Nebraska
  { city: 'Omaha', state: 'NE', loc: 'Omaha, NE', country: 'US' },
  { city: 'Lincoln', state: 'NE', loc: 'Lincoln, NE', country: 'US' },
  // Nevada
  { city: 'Las Vegas', state: 'NV', loc: 'Las Vegas, NV', country: 'US' },
  { city: 'Reno', state: 'NV', loc: 'Reno, NV', country: 'US' },
  { city: 'Henderson', state: 'NV', loc: 'Henderson, NV', country: 'US' },
  // New Hampshire
  { city: 'Manchester', state: 'NH', loc: 'Manchester, NH', country: 'US' },
  // New Jersey
  { city: 'Newark', state: 'NJ', loc: 'Newark, NJ', country: 'US' },
  { city: 'Jersey City', state: 'NJ', loc: 'Jersey City, NJ', country: 'US' },
  { city: 'Elizabeth', state: 'NJ', loc: 'Elizabeth, NJ', country: 'US' },
  { city: 'Paterson', state: 'NJ', loc: 'Paterson, NJ', country: 'US' },
  { city: 'Hackensack', state: 'NJ', loc: 'Hackensack, NJ', country: 'US' },
  // New Mexico
  { city: 'Albuquerque', state: 'NM', loc: 'Albuquerque, NM', country: 'US' },
  { city: 'Las Cruces', state: 'NM', loc: 'Las Cruces, NM', country: 'US' },
  // New York
  { city: 'New York', state: 'NY', loc: 'New York, NY', country: 'US' },
  { city: 'Brooklyn', state: 'NY', loc: 'Brooklyn, NY', country: 'US' },
  { city: 'Queens', state: 'NY', loc: 'Queens, NY', country: 'US' },
  { city: 'Bronx', state: 'NY', loc: 'Bronx, NY', country: 'US' },
  { city: 'Buffalo', state: 'NY', loc: 'Buffalo, NY', country: 'US' },
  { city: 'Albany', state: 'NY', loc: 'Albany, NY', country: 'US' },
  { city: 'Rochester', state: 'NY', loc: 'Rochester, NY', country: 'US' },
  { city: 'White Plains', state: 'NY', loc: 'White Plains, NY', country: 'US' },
  { city: 'Staten Island', state: 'NY', loc: 'Staten Island, NY', country: 'US' },
  { city: 'Hempstead', state: 'NY', loc: 'Hempstead, NY', country: 'US' },
  // North Carolina
  { city: 'Charlotte', state: 'NC', loc: 'Charlotte, NC', country: 'US' },
  { city: 'Raleigh', state: 'NC', loc: 'Raleigh, NC', country: 'US' },
  { city: 'Durham', state: 'NC', loc: 'Durham, NC', country: 'US' },
  { city: 'Greensboro', state: 'NC', loc: 'Greensboro, NC', country: 'US' },
  // North Dakota
  { city: 'Fargo', state: 'ND', loc: 'Fargo, ND', country: 'US' },
  // Ohio
  { city: 'Columbus', state: 'OH', loc: 'Columbus, OH', country: 'US' },
  { city: 'Cleveland', state: 'OH', loc: 'Cleveland, OH', country: 'US' },
  { city: 'Cincinnati', state: 'OH', loc: 'Cincinnati, OH', country: 'US' },
  { city: 'Toledo', state: 'OH', loc: 'Toledo, OH', country: 'US' },
  { city: 'Akron', state: 'OH', loc: 'Akron, OH', country: 'US' },
  // Oklahoma
  { city: 'Oklahoma City', state: 'OK', loc: 'Oklahoma City, OK', country: 'US' },
  { city: 'Tulsa', state: 'OK', loc: 'Tulsa, OK', country: 'US' },
  // Oregon
  { city: 'Portland', state: 'OR', loc: 'Portland, OR', country: 'US' },
  { city: 'Salem', state: 'OR', loc: 'Salem, OR', country: 'US' },
  { city: 'Eugene', state: 'OR', loc: 'Eugene, OR', country: 'US' },
  // Pennsylvania
  { city: 'Philadelphia', state: 'PA', loc: 'Philadelphia, PA', country: 'US' },
  { city: 'Pittsburgh', state: 'PA', loc: 'Pittsburgh, PA', country: 'US' },
  { city: 'Harrisburg', state: 'PA', loc: 'Harrisburg, PA', country: 'US' },
  { city: 'Allentown', state: 'PA', loc: 'Allentown, PA', country: 'US' },
  // Rhode Island
  { city: 'Providence', state: 'RI', loc: 'Providence, RI', country: 'US' },
  // South Carolina
  { city: 'Charleston', state: 'SC', loc: 'Charleston, SC', country: 'US' },
  { city: 'Columbia', state: 'SC', loc: 'Columbia, SC', country: 'US' },
  { city: 'Greenville', state: 'SC', loc: 'Greenville, SC', country: 'US' },
  // South Dakota
  { city: 'Sioux Falls', state: 'SD', loc: 'Sioux Falls, SD', country: 'US' },
  // Tennessee
  { city: 'Nashville', state: 'TN', loc: 'Nashville, TN', country: 'US' },
  { city: 'Memphis', state: 'TN', loc: 'Memphis, TN', country: 'US' },
  { city: 'Knoxville', state: 'TN', loc: 'Knoxville, TN', country: 'US' },
  // Texas
  { city: 'Houston', state: 'TX', loc: 'Houston, TX', country: 'US' },
  { city: 'Dallas', state: 'TX', loc: 'Dallas, TX', country: 'US' },
  { city: 'San Antonio', state: 'TX', loc: 'San Antonio, TX', country: 'US' },
  { city: 'Austin', state: 'TX', loc: 'Austin, TX', country: 'US' },
  { city: 'Fort Worth', state: 'TX', loc: 'Fort Worth, TX', country: 'US' },
  { city: 'El Paso', state: 'TX', loc: 'El Paso, TX', country: 'US' },
  { city: 'Arlington', state: 'TX', loc: 'Arlington, TX', country: 'US' },
  { city: 'McAllen', state: 'TX', loc: 'McAllen, TX', country: 'US' },
  { city: 'Laredo', state: 'TX', loc: 'Laredo, TX', country: 'US' },
  { city: 'Plano', state: 'TX', loc: 'Plano, TX', country: 'US' },
  { city: 'Brownsville', state: 'TX', loc: 'Brownsville, TX', country: 'US' },
  // Utah
  { city: 'Salt Lake City', state: 'UT', loc: 'Salt Lake City, UT', country: 'US' },
  { city: 'Provo', state: 'UT', loc: 'Provo, UT', country: 'US' },
  // Vermont
  { city: 'Burlington', state: 'VT', loc: 'Burlington, VT', country: 'US' },
  // Virginia
  { city: 'Virginia Beach', state: 'VA', loc: 'Virginia Beach, VA', country: 'US' },
  { city: 'Arlington', state: 'VA', loc: 'Arlington, VA', country: 'US' },
  { city: 'Richmond', state: 'VA', loc: 'Richmond, VA', country: 'US' },
  { city: 'Fairfax', state: 'VA', loc: 'Fairfax, VA', country: 'US' },
  { city: 'Alexandria', state: 'VA', loc: 'Alexandria, VA', country: 'US' },
  // Washington
  { city: 'Seattle', state: 'WA', loc: 'Seattle, WA', country: 'US' },
  { city: 'Tacoma', state: 'WA', loc: 'Tacoma, WA', country: 'US' },
  { city: 'Spokane', state: 'WA', loc: 'Spokane, WA', country: 'US' },
  { city: 'Bellevue', state: 'WA', loc: 'Bellevue, WA', country: 'US' },
  // West Virginia
  { city: 'Charleston', state: 'WV', loc: 'Charleston, WV', country: 'US' },
  // Wisconsin
  { city: 'Milwaukee', state: 'WI', loc: 'Milwaukee, WI', country: 'US' },
  { city: 'Madison', state: 'WI', loc: 'Madison, WI', country: 'US' },
  // Wyoming
  { city: 'Cheyenne', state: 'WY', loc: 'Cheyenne, WY', country: 'US' },
];

const UK_CITIES = [
  { city: 'London', state: '', loc: 'London, United Kingdom', country: 'UK' },
  { city: 'Manchester', state: '', loc: 'Manchester, United Kingdom', country: 'UK' },
  { city: 'Birmingham', state: '', loc: 'Birmingham, United Kingdom', country: 'UK' },
  { city: 'Leeds', state: '', loc: 'Leeds, United Kingdom', country: 'UK' },
  { city: 'Liverpool', state: '', loc: 'Liverpool, United Kingdom', country: 'UK' },
  { city: 'Bristol', state: '', loc: 'Bristol, United Kingdom', country: 'UK' },
  { city: 'Edinburgh', state: '', loc: 'Edinburgh, United Kingdom', country: 'UK' },
  { city: 'Glasgow', state: '', loc: 'Glasgow, United Kingdom', country: 'UK' },
  { city: 'Sheffield', state: '', loc: 'Sheffield, United Kingdom', country: 'UK' },
  { city: 'Nottingham', state: '', loc: 'Nottingham, United Kingdom', country: 'UK' },
  { city: 'Leicester', state: '', loc: 'Leicester, United Kingdom', country: 'UK' },
  { city: 'Newcastle', state: '', loc: 'Newcastle upon Tyne, United Kingdom', country: 'UK' },
  { city: 'Cardiff', state: '', loc: 'Cardiff, United Kingdom', country: 'UK' },
  { city: 'Belfast', state: '', loc: 'Belfast, United Kingdom', country: 'UK' },
  { city: 'Brighton', state: '', loc: 'Brighton, United Kingdom', country: 'UK' },
  { city: 'Southampton', state: '', loc: 'Southampton, United Kingdom', country: 'UK' },
  { city: 'Oxford', state: '', loc: 'Oxford, United Kingdom', country: 'UK' },
  { city: 'Cambridge', state: '', loc: 'Cambridge, United Kingdom', country: 'UK' },
  { city: 'Reading', state: '', loc: 'Reading, United Kingdom', country: 'UK' },
  { city: 'Coventry', state: '', loc: 'Coventry, United Kingdom', country: 'UK' },
];

const CA_CITIES = [
  { city: 'Toronto', state: 'ON', loc: 'Toronto, ON, Canada', country: 'CA' },
  { city: 'Vancouver', state: 'BC', loc: 'Vancouver, BC, Canada', country: 'CA' },
  { city: 'Montreal', state: 'QC', loc: 'Montreal, QC, Canada', country: 'CA' },
  { city: 'Calgary', state: 'AB', loc: 'Calgary, AB, Canada', country: 'CA' },
  { city: 'Edmonton', state: 'AB', loc: 'Edmonton, AB, Canada', country: 'CA' },
  { city: 'Ottawa', state: 'ON', loc: 'Ottawa, ON, Canada', country: 'CA' },
  { city: 'Winnipeg', state: 'MB', loc: 'Winnipeg, MB, Canada', country: 'CA' },
  { city: 'Mississauga', state: 'ON', loc: 'Mississauga, ON, Canada', country: 'CA' },
  { city: 'Brampton', state: 'ON', loc: 'Brampton, ON, Canada', country: 'CA' },
  { city: 'Hamilton', state: 'ON', loc: 'Hamilton, ON, Canada', country: 'CA' },
  { city: 'Surrey', state: 'BC', loc: 'Surrey, BC, Canada', country: 'CA' },
  { city: 'Halifax', state: 'NS', loc: 'Halifax, NS, Canada', country: 'CA' },
  { city: 'London', state: 'ON', loc: 'London, ON, Canada', country: 'CA' },
  { city: 'Markham', state: 'ON', loc: 'Markham, ON, Canada', country: 'CA' },
  { city: 'Vaughan', state: 'ON', loc: 'Vaughan, ON, Canada', country: 'CA' },
  { city: 'Kitchener', state: 'ON', loc: 'Kitchener, ON, Canada', country: 'CA' },
  { city: 'Windsor', state: 'ON', loc: 'Windsor, ON, Canada', country: 'CA' },
  { city: 'Victoria', state: 'BC', loc: 'Victoria, BC, Canada', country: 'CA' },
  { city: 'Saskatoon', state: 'SK', loc: 'Saskatoon, SK, Canada', country: 'CA' },
  { city: 'Regina', state: 'SK', loc: 'Regina, SK, Canada', country: 'CA' },
  { city: 'Richmond Hill', state: 'ON', loc: 'Richmond Hill, ON, Canada', country: 'CA' },
  { city: 'Scarborough', state: 'ON', loc: 'Scarborough, ON, Canada', country: 'CA' },
  { city: 'North York', state: 'ON', loc: 'North York, ON, Canada', country: 'CA' },
  { city: 'Burnaby', state: 'BC', loc: 'Burnaby, BC, Canada', country: 'CA' },
  { city: 'St. John\'s', state: 'NL', loc: 'St. John\'s, NL, Canada', country: 'CA' },
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

/**
 * Make an HTTPS GET request and return parsed JSON.
 */
function apiGet(urlStr) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        'Accept': 'application/json',
        'User-Agent': 'mortar-lead-scraper/1.0',
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode === 429) {
            resolve({ error: 'RATE_LIMIT', status: 429 });
          } else if (res.statusCode === 401) {
            resolve({ error: 'UNAUTHORIZED', status: 401, message: json.error?.description || 'Invalid API key' });
          } else if (res.statusCode !== 200) {
            resolve({ error: 'HTTP_ERROR', status: res.statusCode, message: json.error?.description || '' });
          } else {
            resolve(json);
          }
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  let cleaned = firmName;

  // "Law Office(s) of John Smith" / "Law Firm of John Smith"
  const ofMatch = cleaned.match(/(?:Law\s+(?:Office|Offices|Firm|Group|Practice|Center)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|Attorney.*|Law.*|&.*)/gi, '')
      .trim();
  } else {
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .replace(/\s*(Law\s+(?:Firm|Group|Office|Offices|Practice|Center)|Legal\s+(?:Group|Services?|Team)|Immigration\s+(?:Law|Attorneys?|Lawyers?)|Attorneys?\s+at\s+Law|Attorneys?|Lawyers?|& Associates?|Associates?)/gi, '')
      .trim();
  }

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  const lawWords = /^(law|legal|firm|group|office|pllc|plc|pc|immigration|attorneys?|lawyers?|counselors?|associates?|services?)$/i;

  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    if (lawWords.test(lastName)) return { first_name: '', last_name: '' };
    return { first_name: parts[0], last_name: lastName };
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

function normalizePhone(phone) {
  if (!phone) return '';
  let digits = phone.replace(/[^\d+]/g, '');
  if (digits.startsWith('+1') && digits.length === 12) digits = digits.slice(2);
  if (digits.startsWith('1') && digits.length === 11) digits = digits.slice(1);
  if (digits.startsWith('+44')) digits = '0' + digits.slice(3);
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return phone.trim();
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

// ── Yelp API Functions ───────────────────────────────────────────────────────

/**
 * Search for immigration lawyers in a specific location.
 * Uses categories filter for immigration lawyers + general lawyers.
 * Paginates through all results (max 240 per Yelp limit).
 */
async function searchCity(cityInfo, apiCallCount) {
  const results = [];
  let offset = 0;
  let totalResults = 0;
  let pages = 0;

  while (offset < MAX_OFFSET) {
    const params = new URLSearchParams({
      term: 'immigration lawyer',
      location: cityInfo.loc,
      limit: String(SEARCH_LIMIT),
      offset: String(offset),
      sort_by: 'best_match',
      // Categories for lawyers/legal services
      categories: 'immigrationlaw,lawyers,legalservices',
    });

    const url = `${API_BASE}/businesses/search?${params.toString()}`;
    const data = await apiGet(url);
    apiCallCount.total++;

    if (data.error) {
      if (data.error === 'RATE_LIMIT') {
        log(`    Rate limited. Waiting 60s...`);
        await sleep(60000);
        continue; // Retry same page
      }
      if (data.error === 'UNAUTHORIZED') {
        throw new Error(`API key invalid: ${data.message}`);
      }
      log(`    API error: ${data.status} ${data.message || data.error}`);
      break;
    }

    const businesses = data.businesses || [];
    totalResults = data.total || 0;
    pages++;

    if (businesses.length === 0) break;

    for (const biz of businesses) {
      results.push({
        id: biz.id,
        name: biz.name || '',
        phone: biz.display_phone || biz.phone || '',
        rating: biz.rating || '',
        reviewCount: biz.review_count || '',
        url: biz.url || '',
        address: biz.location ? [
          biz.location.address1,
          biz.location.address2,
          biz.location.address3,
        ].filter(Boolean).join(', ') : '',
        city: biz.location?.city || cityInfo.city,
        state: biz.location?.state || cityInfo.state,
        zipCode: biz.location?.zip_code || '',
        country: biz.location?.country || '',
        categories: (biz.categories || []).map(c => c.title).join(', '),
        isClosed: biz.is_closed,
      });
    }

    offset += SEARCH_LIMIT;

    // Stop if we got all results or less than a full page
    if (offset >= totalResults || businesses.length < SEARCH_LIMIT) break;

    await sleep(DELAY_BETWEEN_CALLS);
  }

  return { results, totalResults, pages, apiCalls: pages };
}

/**
 * Get business details (website, hours, etc.) from the detail endpoint.
 * Only needed for businesses where we want the website URL.
 */
async function getBusinessDetails(bizId) {
  const url = `${API_BASE}/businesses/${encodeURIComponent(bizId)}`;
  const data = await apiGet(url);

  if (data.error) {
    return {};
  }

  return {
    website: data.url || '', // This is Yelp URL; no direct website in search
    // The Yelp API doesn't return the business's own website URL directly
    // It returns the Yelp profile URL. Website is only on the Yelp page itself.
    hours: data.hours || [],
    photos: data.photos || [],
    specialHours: data.special_hours || [],
  };
}

/**
 * Determine country code from Yelp's location.country or our city data.
 */
function resolveCountry(yelpCountry, cityCountry) {
  if (cityCountry) return cityCountry;
  const map = { 'US': 'US', 'CA': 'CA', 'GB': 'UK', 'UK': 'UK' };
  return map[yelpCountry] || yelpCountry || 'US';
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllCities = args.includes('--all-cities');
  const isResume = args.includes('--resume');

  // Parse --country flag
  const countryIdx = args.indexOf('--country');
  let countryFilter = 'ALL';
  if (countryIdx !== -1 && args[countryIdx + 1]) {
    countryFilter = args[countryIdx + 1].toUpperCase();
  }
  if (isAllCities && countryIdx === -1) countryFilter = 'ALL';

  if (!isTest && !isAllCities && countryIdx === -1 && !isResume) {
    console.log('Yelp Immigration Lawyers Scraper (Fusion API)');
    console.log('');
    console.log('Requires: YELP_API_KEY environment variable');
    console.log('  Get key at: https://www.yelp.com/developers/v3/manage_app');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-yelp-immigration.js --test                   # Test mode (5 cities)');
    console.log('  node scripts/scrape-yelp-immigration.js --all-cities             # All cities, all countries');
    console.log('  node scripts/scrape-yelp-immigration.js --country US             # US cities only');
    console.log('  node scripts/scrape-yelp-immigration.js --country UK             # UK cities only');
    console.log('  node scripts/scrape-yelp-immigration.js --country CA             # Canada cities only');
    console.log('  node scripts/scrape-yelp-immigration.js --country ALL            # All countries');
    console.log('  node scripts/scrape-yelp-immigration.js --country ALL --resume   # Resume interrupted');
    process.exit(0);
  }

  if (!API_KEY) {
    console.error('ERROR: YELP_API_KEY environment variable is required.');
    console.error('');
    console.error('To get a free API key:');
    console.error('  1. Go to https://www.yelp.com/developers/v3/manage_app');
    console.error('  2. Create an app (free trial: 5,000 calls in 30 days)');
    console.error('  3. Copy your API Key');
    console.error('  4. Run: YELP_API_KEY=your_key node scripts/scrape-yelp-immigration.js --test');
    process.exit(1);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Build city list based on country filter
  let cities = [];
  if (countryFilter === 'ALL' || countryFilter === 'US') {
    cities.push(...US_CITIES);
  }
  if (countryFilter === 'ALL' || countryFilter === 'UK') {
    cities.push(...UK_CITIES);
  }
  if (countryFilter === 'ALL' || countryFilter === 'CA') {
    cities.push(...CA_CITIES);
  }

  let allLeads = [];
  let completedCities = [];
  const seenKeys = new Set();

  if (isTest) {
    cities = cities.slice(0, 5);
    log('TEST MODE: 5 cities only');
  }

  // Resume support
  if (isResume) {
    const progress = loadProgress();
    if (progress.completedCities.length > 0) {
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
    const key = `${c.country}:${c.loc}`;
    return !completedCities.includes(key);
  });

  log(`Yelp Immigration Lawyers Scraper (Fusion API)`);
  log(`Country filter: ${countryFilter} | Total cities: ${cities.length} | Remaining: ${citiesToScrape.length} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Validate API key with a test call
  log('Validating API key...');
  const testResult = await apiGet(`${API_BASE}/businesses/search?term=lawyer&location=New+York&limit=1`);
  if (testResult.error === 'UNAUTHORIZED') {
    console.error(`ERROR: Invalid API key: ${testResult.message}`);
    process.exit(1);
  }
  if (testResult.error) {
    console.error(`ERROR: API test failed: ${testResult.status} ${testResult.message || testResult.error}`);
    process.exit(1);
  }
  log(`API key valid. Test returned ${testResult.total || 0} results.`);

  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let emptyPages = 0;
  const apiCallCount = { total: 1 }; // Already used 1 for validation

  for (const cityInfo of citiesToScrape) {
    processed++;
    const cityKey = `${cityInfo.country}:${cityInfo.loc}`;
    log(`[${processed}/${citiesToScrape.length}] ${cityInfo.city}, ${cityInfo.state || cityInfo.country} (${cityInfo.country}) [API calls: ${apiCallCount.total}]`);

    try {
      const { results, totalResults, pages } = await searchCity(cityInfo, apiCallCount);

      if (results.length === 0) {
        log(`  No results found (total: ${totalResults})`);
        emptyPages++;
        completedCities.push(cityKey);
        await sleep(DELAY_BETWEEN_CITIES);
        continue;
      }

      log(`  Found ${results.length} businesses (total available: ${totalResults}, pages: ${pages})`);

      let newCount = 0;

      for (const biz of results) {
        // Skip closed businesses
        if (biz.isClosed) continue;

        const firmName = biz.name || '';
        const city = biz.city || cityInfo.city;
        const state = biz.state || cityInfo.state;
        const country = resolveCountry(biz.country, cityInfo.country);

        // Dedup by firm_name + city
        const key = dedupKey(firmName, city);
        if (seenKeys.has(key)) continue;

        // Also dedup by firm_name alone (same firm in nearby cities)
        const firmKey = 'firm:' + firmName.toLowerCase().trim();
        if (firmKey !== 'firm:' && seenKeys.has(firmKey)) continue;

        seenKeys.add(key);
        seenKeys.add(firmKey);

        const phone = normalizePhone(biz.phone || '');
        const { first_name, last_name } = parseName(firmName);

        // Clean Yelp URL — remove tracking params
        let profileUrl = biz.url || '';
        if (profileUrl.includes('?')) {
          profileUrl = profileUrl.split('?')[0];
        }

        const fullAddress = [biz.address, city, state, biz.zipCode].filter(Boolean).join(', ');

        const lead = {
          first_name,
          last_name,
          firm_name: firmName,
          title: '',
          email: '',
          phone,
          website: '', // Yelp API doesn't return external website in search
          domain: '',
          city,
          state,
          country,
          niche: 'immigration',
          source: 'yelp',
          profile_url: profileUrl,
          rating: String(biz.rating || ''),
          review_count: String(biz.reviewCount || ''),
          address: fullAddress,
        };

        allLeads.push(lead);
        newCount++;
      }

      totalNew += newCount;
      log(`  Added ${newCount} new leads (${results.length - newCount} dupes/closed/skipped)`);

      completedCities.push(cityKey);

      // Save progress every 5 cities
      if (processed % 5 === 0 && !isTest) {
        saveProgress(completedCities, allLeads);
        writeCSV(allLeads, OUT_FILE);
        log(`  [Progress saved: ${allLeads.length} total leads, ${apiCallCount.total} API calls]`);
      }

    } catch (err) {
      const msg = err.message || '';
      log(`  ERROR: ${msg.slice(0, 120)}`);
      errors++;

      // If unauthorized, stop
      if (msg.includes('API key invalid') || msg.includes('UNAUTHORIZED')) {
        log('API key invalid. Stopping.');
        break;
      }
    }

    await sleep(DELAY_BETWEEN_CITIES);
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) {
    saveProgress(completedCities, allLeads);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const withAddress = allLeads.filter(l => l.address).length;
  const uniqueStates = new Set(allLeads.filter(l => l.state).map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;
  const countries = {};
  allLeads.forEach(l => { countries[l.country] = (countries[l.country] || 0) + 1; });

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Cities scraped: ${processed} (${emptyPages} empty, ${errors} errors)`);
  log(`Total API calls: ${apiCallCount.total}`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With address: ${withAddress}`);
  log(`With parsed name: ${withName}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`By country: ${JSON.stringify(countries)}`);
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
