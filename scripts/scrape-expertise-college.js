#!/usr/bin/env node
/**
 * Expertise.com College / Education Scraper
 *
 * Scrapes education-related categories from Expertise.com:
 *   - /business/trade-schools/{state}/{city}         (confirmed working)
 *   - /education/college-counselors/{state}/{city}    (may 404 — site restructured)
 *   - /education/tutoring/{state}/{city}              (may 404)
 *   - /education/test-prep/{state}/{city}             (may 404)
 *   - /education/academic-tutoring/{state}/{city}     (may 404)
 *
 * Uses pure HTTP + Cheerio with JSON-LD LocalBusiness extraction.
 * Copies the exact pattern from scrape-expertise-immigration.js.
 *
 * Note: Expertise.com removed most education categories in a site
 * restructure. /business/trade-schools is the only confirmed working
 * education-adjacent category. The other paths are attempted in case
 * they exist for specific cities or are re-added in the future.
 *
 * Usage:
 *   node scripts/scrape-expertise-college.js --test         # 5 cities, all categories
 *   node scripts/scrape-expertise-college.js --all-cities   # All 164 cities
 *   node scripts/scrape-expertise-college.js --resume       # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-expertise.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'expertise-college-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── URL Categories to Scrape ─────────────────────────────────────────────────
// Multiple patterns attempted since exact slugs vary / may have been removed.
// /business/trade-schools is the only confirmed working education category.

const CATEGORY_PATHS = [
  'business/trade-schools',
  'education/college-counselors',
  'education/tutoring',
  'education/test-prep',
  'education/academic-tutoring',
];

// ── City Database (164 cities covering all 50 states + DC) ───────────────────
// URL pattern: /{category-path}/{state-slug}/{city-slug}

const CITIES = [
  // Alabama
  { city: 'Birmingham', state: 'AL', stateSlug: 'alabama', citySlug: 'birmingham' },
  { city: 'Huntsville', state: 'AL', stateSlug: 'alabama', citySlug: 'huntsville' },
  { city: 'Mobile', state: 'AL', stateSlug: 'alabama', citySlug: 'mobile' },
  // Alaska
  { city: 'Anchorage', state: 'AK', stateSlug: 'alaska', citySlug: 'anchorage' },
  // Arizona
  { city: 'Phoenix', state: 'AZ', stateSlug: 'arizona', citySlug: 'phoenix' },
  { city: 'Tucson', state: 'AZ', stateSlug: 'arizona', citySlug: 'tucson' },
  { city: 'Mesa', state: 'AZ', stateSlug: 'arizona', citySlug: 'mesa' },
  { city: 'Scottsdale', state: 'AZ', stateSlug: 'arizona', citySlug: 'scottsdale' },
  // Arkansas
  { city: 'Little Rock', state: 'AR', stateSlug: 'arkansas', citySlug: 'little-rock' },
  { city: 'Fayetteville', state: 'AR', stateSlug: 'arkansas', citySlug: 'fayetteville' },
  // California
  { city: 'Los Angeles', state: 'CA', stateSlug: 'california', citySlug: 'los-angeles' },
  { city: 'San Francisco', state: 'CA', stateSlug: 'california', citySlug: 'san-francisco' },
  { city: 'San Diego', state: 'CA', stateSlug: 'california', citySlug: 'san-diego' },
  { city: 'San Jose', state: 'CA', stateSlug: 'california', citySlug: 'san-jose' },
  { city: 'Sacramento', state: 'CA', stateSlug: 'california', citySlug: 'sacramento' },
  { city: 'Fresno', state: 'CA', stateSlug: 'california', citySlug: 'fresno' },
  { city: 'Oakland', state: 'CA', stateSlug: 'california', citySlug: 'oakland' },
  { city: 'Riverside', state: 'CA', stateSlug: 'california', citySlug: 'riverside' },
  { city: 'Bakersfield', state: 'CA', stateSlug: 'california', citySlug: 'bakersfield' },
  { city: 'Irvine', state: 'CA', stateSlug: 'california', citySlug: 'irvine' },
  { city: 'Pasadena', state: 'CA', stateSlug: 'california', citySlug: 'pasadena' },
  { city: 'Santa Ana', state: 'CA', stateSlug: 'california', citySlug: 'santa-ana' },
  { city: 'Long Beach', state: 'CA', stateSlug: 'california', citySlug: 'long-beach' },
  { city: 'Anaheim', state: 'CA', stateSlug: 'california', citySlug: 'anaheim' },
  // Colorado
  { city: 'Denver', state: 'CO', stateSlug: 'colorado', citySlug: 'denver' },
  { city: 'Colorado Springs', state: 'CO', stateSlug: 'colorado', citySlug: 'colorado-springs' },
  { city: 'Aurora', state: 'CO', stateSlug: 'colorado', citySlug: 'aurora' },
  { city: 'Fort Collins', state: 'CO', stateSlug: 'colorado', citySlug: 'fort-collins' },
  // Connecticut
  { city: 'Hartford', state: 'CT', stateSlug: 'connecticut', citySlug: 'hartford' },
  { city: 'New Haven', state: 'CT', stateSlug: 'connecticut', citySlug: 'new-haven' },
  { city: 'Stamford', state: 'CT', stateSlug: 'connecticut', citySlug: 'stamford' },
  { city: 'Bridgeport', state: 'CT', stateSlug: 'connecticut', citySlug: 'bridgeport' },
  // Delaware
  { city: 'Wilmington', state: 'DE', stateSlug: 'delaware', citySlug: 'wilmington' },
  // District of Columbia
  { city: 'Washington', state: 'DC', stateSlug: 'district-of-columbia', citySlug: 'washington' },
  // Florida
  { city: 'Miami', state: 'FL', stateSlug: 'florida', citySlug: 'miami' },
  { city: 'Orlando', state: 'FL', stateSlug: 'florida', citySlug: 'orlando' },
  { city: 'Tampa', state: 'FL', stateSlug: 'florida', citySlug: 'tampa' },
  { city: 'Jacksonville', state: 'FL', stateSlug: 'florida', citySlug: 'jacksonville' },
  { city: 'Fort Lauderdale', state: 'FL', stateSlug: 'florida', citySlug: 'fort-lauderdale' },
  { city: 'West Palm Beach', state: 'FL', stateSlug: 'florida', citySlug: 'west-palm-beach' },
  { city: 'Hialeah', state: 'FL', stateSlug: 'florida', citySlug: 'hialeah' },
  { city: 'St. Petersburg', state: 'FL', stateSlug: 'florida', citySlug: 'saint-petersburg' },
  // Georgia
  { city: 'Atlanta', state: 'GA', stateSlug: 'georgia', citySlug: 'atlanta' },
  { city: 'Savannah', state: 'GA', stateSlug: 'georgia', citySlug: 'savannah' },
  { city: 'Augusta', state: 'GA', stateSlug: 'georgia', citySlug: 'augusta' },
  { city: 'Marietta', state: 'GA', stateSlug: 'georgia', citySlug: 'marietta' },
  // Hawaii
  { city: 'Honolulu', state: 'HI', stateSlug: 'hawaii', citySlug: 'honolulu' },
  // Idaho
  { city: 'Boise', state: 'ID', stateSlug: 'idaho', citySlug: 'boise' },
  // Illinois
  { city: 'Chicago', state: 'IL', stateSlug: 'illinois', citySlug: 'chicago' },
  { city: 'Aurora', state: 'IL', stateSlug: 'illinois', citySlug: 'aurora' },
  { city: 'Naperville', state: 'IL', stateSlug: 'illinois', citySlug: 'naperville' },
  { city: 'Schaumburg', state: 'IL', stateSlug: 'illinois', citySlug: 'schaumburg' },
  // Indiana
  { city: 'Indianapolis', state: 'IN', stateSlug: 'indiana', citySlug: 'indianapolis' },
  { city: 'Fort Wayne', state: 'IN', stateSlug: 'indiana', citySlug: 'fort-wayne' },
  // Iowa
  { city: 'Des Moines', state: 'IA', stateSlug: 'iowa', citySlug: 'des-moines' },
  { city: 'Cedar Rapids', state: 'IA', stateSlug: 'iowa', citySlug: 'cedar-rapids' },
  // Kansas
  { city: 'Wichita', state: 'KS', stateSlug: 'kansas', citySlug: 'wichita' },
  { city: 'Kansas City', state: 'KS', stateSlug: 'kansas', citySlug: 'kansas-city' },
  // Kentucky
  { city: 'Louisville', state: 'KY', stateSlug: 'kentucky', citySlug: 'louisville' },
  { city: 'Lexington', state: 'KY', stateSlug: 'kentucky', citySlug: 'lexington' },
  // Louisiana
  { city: 'New Orleans', state: 'LA', stateSlug: 'louisiana', citySlug: 'new-orleans' },
  { city: 'Baton Rouge', state: 'LA', stateSlug: 'louisiana', citySlug: 'baton-rouge' },
  // Maine
  { city: 'Portland', state: 'ME', stateSlug: 'maine', citySlug: 'portland' },
  // Maryland
  { city: 'Baltimore', state: 'MD', stateSlug: 'maryland', citySlug: 'baltimore' },
  { city: 'Silver Spring', state: 'MD', stateSlug: 'maryland', citySlug: 'silver-spring' },
  { city: 'Rockville', state: 'MD', stateSlug: 'maryland', citySlug: 'rockville' },
  { city: 'Bethesda', state: 'MD', stateSlug: 'maryland', citySlug: 'bethesda' },
  // Massachusetts
  { city: 'Boston', state: 'MA', stateSlug: 'massachusetts', citySlug: 'boston' },
  { city: 'Worcester', state: 'MA', stateSlug: 'massachusetts', citySlug: 'worcester' },
  { city: 'Springfield', state: 'MA', stateSlug: 'massachusetts', citySlug: 'springfield' },
  { city: 'Cambridge', state: 'MA', stateSlug: 'massachusetts', citySlug: 'cambridge' },
  // Michigan
  { city: 'Detroit', state: 'MI', stateSlug: 'michigan', citySlug: 'detroit' },
  { city: 'Grand Rapids', state: 'MI', stateSlug: 'michigan', citySlug: 'grand-rapids' },
  { city: 'Lansing', state: 'MI', stateSlug: 'michigan', citySlug: 'lansing' },
  { city: 'Ann Arbor', state: 'MI', stateSlug: 'michigan', citySlug: 'ann-arbor' },
  // Minnesota
  { city: 'Minneapolis', state: 'MN', stateSlug: 'minnesota', citySlug: 'minneapolis' },
  { city: 'Saint Paul', state: 'MN', stateSlug: 'minnesota', citySlug: 'saint-paul' },
  { city: 'Bloomington', state: 'MN', stateSlug: 'minnesota', citySlug: 'bloomington' },
  // Mississippi
  { city: 'Jackson', state: 'MS', stateSlug: 'mississippi', citySlug: 'jackson' },
  // Missouri
  { city: 'Kansas City', state: 'MO', stateSlug: 'missouri', citySlug: 'kansas-city' },
  { city: 'Saint Louis', state: 'MO', stateSlug: 'missouri', citySlug: 'saint-louis' },
  { city: 'Springfield', state: 'MO', stateSlug: 'missouri', citySlug: 'springfield' },
  // Montana
  { city: 'Billings', state: 'MT', stateSlug: 'montana', citySlug: 'billings' },
  { city: 'Missoula', state: 'MT', stateSlug: 'montana', citySlug: 'missoula' },
  // Nebraska
  { city: 'Omaha', state: 'NE', stateSlug: 'nebraska', citySlug: 'omaha' },
  { city: 'Lincoln', state: 'NE', stateSlug: 'nebraska', citySlug: 'lincoln' },
  // Nevada
  { city: 'Las Vegas', state: 'NV', stateSlug: 'nevada', citySlug: 'las-vegas' },
  { city: 'Reno', state: 'NV', stateSlug: 'nevada', citySlug: 'reno' },
  { city: 'Henderson', state: 'NV', stateSlug: 'nevada', citySlug: 'henderson' },
  // New Hampshire
  { city: 'Manchester', state: 'NH', stateSlug: 'new-hampshire', citySlug: 'manchester' },
  // New Jersey
  { city: 'Newark', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'newark' },
  { city: 'Jersey City', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'jersey-city' },
  { city: 'Elizabeth', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'elizabeth' },
  { city: 'Hackensack', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'hackensack' },
  { city: 'Paterson', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'paterson' },
  { city: 'Trenton', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'trenton' },
  // New Mexico
  { city: 'Albuquerque', state: 'NM', stateSlug: 'new-mexico', citySlug: 'albuquerque' },
  { city: 'Las Cruces', state: 'NM', stateSlug: 'new-mexico', citySlug: 'las-cruces' },
  // New York
  { city: 'New York City', state: 'NY', stateSlug: 'new-york', citySlug: 'nyc' },
  { city: 'Brooklyn', state: 'NY', stateSlug: 'new-york', citySlug: 'brooklyn' },
  { city: 'Queens', state: 'NY', stateSlug: 'new-york', citySlug: 'queens' },
  { city: 'Bronx', state: 'NY', stateSlug: 'new-york', citySlug: 'bronx' },
  { city: 'Buffalo', state: 'NY', stateSlug: 'new-york', citySlug: 'buffalo' },
  { city: 'Albany', state: 'NY', stateSlug: 'new-york', citySlug: 'albany' },
  { city: 'Rochester', state: 'NY', stateSlug: 'new-york', citySlug: 'rochester' },
  { city: 'White Plains', state: 'NY', stateSlug: 'new-york', citySlug: 'white-plains' },
  { city: 'Staten Island', state: 'NY', stateSlug: 'new-york', citySlug: 'staten-island' },
  { city: 'Hempstead', state: 'NY', stateSlug: 'new-york', citySlug: 'hempstead' },
  // North Carolina
  { city: 'Charlotte', state: 'NC', stateSlug: 'north-carolina', citySlug: 'charlotte' },
  { city: 'Raleigh', state: 'NC', stateSlug: 'north-carolina', citySlug: 'raleigh' },
  { city: 'Durham', state: 'NC', stateSlug: 'north-carolina', citySlug: 'durham' },
  { city: 'Greensboro', state: 'NC', stateSlug: 'north-carolina', citySlug: 'greensboro' },
  // North Dakota
  { city: 'Fargo', state: 'ND', stateSlug: 'north-dakota', citySlug: 'fargo' },
  // Ohio
  { city: 'Columbus', state: 'OH', stateSlug: 'ohio', citySlug: 'columbus' },
  { city: 'Cleveland', state: 'OH', stateSlug: 'ohio', citySlug: 'cleveland' },
  { city: 'Cincinnati', state: 'OH', stateSlug: 'ohio', citySlug: 'cincinnati' },
  { city: 'Toledo', state: 'OH', stateSlug: 'ohio', citySlug: 'toledo' },
  { city: 'Akron', state: 'OH', stateSlug: 'ohio', citySlug: 'akron' },
  { city: 'Dayton', state: 'OH', stateSlug: 'ohio', citySlug: 'dayton' },
  // Oklahoma
  { city: 'Oklahoma City', state: 'OK', stateSlug: 'oklahoma', citySlug: 'oklahoma-city' },
  { city: 'Tulsa', state: 'OK', stateSlug: 'oklahoma', citySlug: 'tulsa' },
  // Oregon
  { city: 'Portland', state: 'OR', stateSlug: 'oregon', citySlug: 'portland' },
  { city: 'Salem', state: 'OR', stateSlug: 'oregon', citySlug: 'salem' },
  { city: 'Eugene', state: 'OR', stateSlug: 'oregon', citySlug: 'eugene' },
  // Pennsylvania
  { city: 'Philadelphia', state: 'PA', stateSlug: 'pennsylvania', citySlug: 'philadelphia' },
  { city: 'Pittsburgh', state: 'PA', stateSlug: 'pennsylvania', citySlug: 'pittsburgh' },
  { city: 'Harrisburg', state: 'PA', stateSlug: 'pennsylvania', citySlug: 'harrisburg' },
  { city: 'Allentown', state: 'PA', stateSlug: 'pennsylvania', citySlug: 'allentown' },
  // Rhode Island
  { city: 'Providence', state: 'RI', stateSlug: 'rhode-island', citySlug: 'providence' },
  // South Carolina
  { city: 'Charleston', state: 'SC', stateSlug: 'south-carolina', citySlug: 'charleston' },
  { city: 'Columbia', state: 'SC', stateSlug: 'south-carolina', citySlug: 'columbia' },
  { city: 'Greenville', state: 'SC', stateSlug: 'south-carolina', citySlug: 'greenville' },
  // South Dakota
  { city: 'Sioux Falls', state: 'SD', stateSlug: 'south-dakota', citySlug: 'sioux-falls' },
  // Tennessee
  { city: 'Nashville', state: 'TN', stateSlug: 'tennessee', citySlug: 'nashville' },
  { city: 'Memphis', state: 'TN', stateSlug: 'tennessee', citySlug: 'memphis' },
  { city: 'Knoxville', state: 'TN', stateSlug: 'tennessee', citySlug: 'knoxville' },
  { city: 'Chattanooga', state: 'TN', stateSlug: 'tennessee', citySlug: 'chattanooga' },
  // Texas
  { city: 'Houston', state: 'TX', stateSlug: 'texas', citySlug: 'houston' },
  { city: 'Dallas', state: 'TX', stateSlug: 'texas', citySlug: 'dallas' },
  { city: 'San Antonio', state: 'TX', stateSlug: 'texas', citySlug: 'san-antonio' },
  { city: 'Austin', state: 'TX', stateSlug: 'texas', citySlug: 'austin' },
  { city: 'Fort Worth', state: 'TX', stateSlug: 'texas', citySlug: 'fort-worth' },
  { city: 'El Paso', state: 'TX', stateSlug: 'texas', citySlug: 'el-paso' },
  { city: 'Arlington', state: 'TX', stateSlug: 'texas', citySlug: 'arlington' },
  { city: 'McAllen', state: 'TX', stateSlug: 'texas', citySlug: 'mcallen' },
  { city: 'Laredo', state: 'TX', stateSlug: 'texas', citySlug: 'laredo' },
  { city: 'Plano', state: 'TX', stateSlug: 'texas', citySlug: 'plano' },
  { city: 'Brownsville', state: 'TX', stateSlug: 'texas', citySlug: 'brownsville' },
  // Utah
  { city: 'Salt Lake City', state: 'UT', stateSlug: 'utah', citySlug: 'salt-lake-city' },
  { city: 'Provo', state: 'UT', stateSlug: 'utah', citySlug: 'provo' },
  // Vermont
  { city: 'Burlington', state: 'VT', stateSlug: 'vermont', citySlug: 'burlington' },
  // Virginia
  { city: 'Virginia Beach', state: 'VA', stateSlug: 'virginia', citySlug: 'virginia-beach' },
  { city: 'Arlington', state: 'VA', stateSlug: 'virginia', citySlug: 'arlington' },
  { city: 'Richmond', state: 'VA', stateSlug: 'virginia', citySlug: 'richmond' },
  { city: 'Fairfax', state: 'VA', stateSlug: 'virginia', citySlug: 'fairfax' },
  { city: 'Alexandria', state: 'VA', stateSlug: 'virginia', citySlug: 'alexandria' },
  // Washington
  { city: 'Seattle', state: 'WA', stateSlug: 'washington', citySlug: 'seattle' },
  { city: 'Tacoma', state: 'WA', stateSlug: 'washington', citySlug: 'tacoma' },
  { city: 'Spokane', state: 'WA', stateSlug: 'washington', citySlug: 'spokane' },
  { city: 'Bellevue', state: 'WA', stateSlug: 'washington', citySlug: 'bellevue' },
  // West Virginia
  { city: 'Charleston', state: 'WV', stateSlug: 'west-virginia', citySlug: 'charleston' },
  // Wisconsin
  { city: 'Milwaukee', state: 'WI', stateSlug: 'wisconsin', citySlug: 'milwaukee' },
  { city: 'Madison', state: 'WI', stateSlug: 'wisconsin', citySlug: 'madison' },
  // Wyoming
  { city: 'Cheyenne', state: 'WY', stateSlug: 'wyoming', citySlug: 'cheyenne' },
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

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/');
}

function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  // Education businesses rarely have person names as the firm name.
  // Only attempt extraction if we see a clear "of PersonName" pattern
  // or the firm name after stripping looks obviously like a person name.

  let cleaned = firmName;

  // "XYZ of John Smith" pattern
  const ofMatch = cleaned.match(/(?:(?:School|Academy|Institute|Center|Centre|Tutoring|Learning|Consulting|Services?|College|Education|Prep)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|&.*)/gi, '')
      .trim();
  } else {
    // Strip common suffixes to try to get a person name
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .replace(/\s*(Tutoring|Tutor|Tutors|Academy|School|Schools|Institute|College|University|Learning\s+Center|Education|Educational|Prep|Test\s+Prep|SAT\s+Prep|ACT\s+Prep|Consulting|Consultants?|Services?|Group|Center|Centre|Training|Coaching|Campus|& Associates?|Associates?)/gi, '')
      .trim();
  }

  // Remove trailing punctuation
  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip names starting with "The" (e.g., "The Princeton Review")
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if it contains & or "and" (multi-partner like "Smith & Jones")
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if it contains numbers (e.g. addresses or numbered institutions)
  if (/\d/.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if it contains a dash followed by a place name (e.g. "Carrington College - Phoenix")
  if (/\s+-\s+/.test(cleaned)) return { first_name: '', last_name: '' };

  // Words that should never appear in an extracted person name
  const nonNameWords = /^(school|academy|institute|college|university|learning|education|educational|tutoring|tutor|prep|consulting|consultants?|services?|group|center|centre|training|coaching|associates?|medical|dental|technical|vocational|community|career|electrical|apprenticeship|partnership|technology|network|foundation|national|american|allied|health|sciences?|nursing|massage|therapy|oriental|medicine|assistant|programs?|online|virtual|digital|global|international|regional|western|eastern|northern|southern|central|metro|urban|premier|advanced|professional|creative|modern|classic)$/i;

  // If what remains looks like a person name (2-3 words, no special chars)
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    // Check ALL parts against non-name words, not just the last
    for (const part of parts) {
      const stripped = part.replace(/\.$/, '');
      if (nonNameWords.test(stripped)) return { first_name: '', last_name: '' };
    }
    return {
      first_name: parts[0],
      last_name: parts[parts.length - 1],
    };
  }

  return { first_name: '', last_name: '' };
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
  return rawPhone;
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

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    let settled = false;

    function settle(fn) {
      if (settled) return;
      settled = true;
      fn();
    }

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

    const req = mod.get(url, options, (res) => {
      // Handle redirects
      if ([301, 302, 307, 308].includes(res.statusCode)) {
        res.resume(); // drain the response body
        let redirectUrl = res.headers.location || '';
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        // If redirect goes to a generic category page (not our specific listing), treat as 404
        const redirectPath = redirectUrl.replace(/https?:\/\/[^/]+/, '');
        if (/^\/(health-wellness|legal|business|lifestyle|finance|insurance|home-improvement)\/?$/.test(redirectPath)) {
          return settle(() => resolve({ statusCode: 404, body: '' }));
        }
        if (!redirectUrl) return settle(() => resolve({ statusCode: res.statusCode, body: '' }));
        return settle(() => httpGet(redirectUrl, retries).then(resolve).catch(reject));
      }

      if (res.statusCode === 404) {
        res.resume();
        return settle(() => resolve({ statusCode: 404, body: '' }));
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        res.resume();
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return settle(() => sleep(wait).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject));
        }
        return settle(() => resolve({ statusCode: res.statusCode, body: '' }));
      }

      if (res.statusCode !== 200) {
        res.resume();
        return settle(() => resolve({ statusCode: res.statusCode, body: '' }));
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => settle(() => resolve({ statusCode: 200, body })));
      res.on('error', (err) => settle(() => reject(err)));
    });

    req.on('error', (err) => {
      if (settled) return;
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        settle(() => sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject));
      } else {
        settle(() => reject(err));
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (settled) return;
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        settle(() => sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject));
      } else {
        settle(() => reject(new Error('Request timed out')));
      }
    });
  });
}

// ── Page Parser ──────────────────────────────────────────────────────────────

function parseExpertisePage(html, cityInfo, categoryPath) {
  const $ = cheerio.load(html);
  const leads = [];

  // Extract JSON-LD blocks - each provider has its own LocalBusiness block
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());

      // Only process LocalBusiness entries (skip FAQPage, WebPage, etc.)
      if (json['@type'] !== 'LocalBusiness') return;

      const rawName = json.name || '';
      if (!rawName) return;

      const name = decodeHtmlEntities(rawName);
      const address = json.address || {};
      const phone = formatPhone(json.telephone || '');
      const website = json.url || '';
      const city = address.addressLocality || cityInfo.city;
      const state = address.addressRegion || cityInfo.state;

      // Build profile URL from @id
      const profileUrl = json['@id'] || '';

      const { first_name, last_name } = parseName(name);

      leads.push({
        first_name,
        last_name,
        firm_name: name,
        title: '',
        email: '',
        phone,
        website,
        domain: extractDomain(website),
        city,
        state,
        country: 'US',
        niche: 'education',
        source: 'expertise',
        profile_url: profileUrl,
      });
    } catch (e) {
      // Skip malformed JSON-LD
    }
  });

  return leads;
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedKeys: [], leads: [] };
}

function saveProgress(completedKeys, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedKeys,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Category Probe ───────────────────────────────────────────────────────────
// Test each category path against a known large city to see which are live.
// This prevents wasting hundreds of requests on 404-only categories.

async function probeCategories() {
  const probeCity = { stateSlug: 'california', citySlug: 'los-angeles' };
  const live = [];

  log('Probing category paths to find which are live...');
  for (const catPath of CATEGORY_PATHS) {
    const url = `https://www.expertise.com/${catPath}/${probeCity.stateSlug}/${probeCity.citySlug}`;
    try {
      const { statusCode } = await httpGet(url);
      if (statusCode === 200) {
        log(`  ${catPath} -> LIVE`);
        live.push(catPath);
      } else {
        log(`  ${catPath} -> ${statusCode} (skipping)`);
      }
    } catch (err) {
      log(`  ${catPath} -> ERROR: ${err.message} (skipping)`);
    }
    await sleep(1500);
  }

  if (live.length === 0) {
    log('WARNING: No live categories found. Will try all anyway.');
    return [...CATEGORY_PATHS];
  }
  return live;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllCities = args.includes('--all-cities');
  const isResume = args.includes('--resume');

  if (!isTest && !isAllCities && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-expertise-college.js --test         # Test mode (5 cities, all categories)');
    console.log('  node scripts/scrape-expertise-college.js --all-cities   # All 164 cities x live categories');
    console.log('  node scripts/scrape-expertise-college.js --resume       # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Probe which category paths actually return results
  const liveCategories = await probeCategories();
  log(`Live categories: ${liveCategories.join(', ')}`);
  log('');

  let cities = [...CITIES];
  let allLeads = [];
  let completedKeys = [];
  const seenKeys = new Set();

  if (isTest) {
    cities = cities.slice(0, 5);
    log('TEST MODE: 5 cities only');
  }

  // Resume support
  if (isResume || isAllCities) {
    const progress = loadProgress();
    if (progress.completedKeys && progress.completedKeys.length > 0 && isResume) {
      completedKeys = progress.completedKeys;
      allLeads = progress.leads || [];
      // Rebuild dedup set
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedKeys.length} city+category combos already done, ${allLeads.length} leads loaded`);
    }
  }

  // Build work items: every city x live category combination
  const workItems = [];
  for (const city of cities) {
    for (const catPath of liveCategories) {
      const key = `${catPath}/${city.stateSlug}/${city.citySlug}`;
      if (!completedKeys.includes(key)) {
        workItems.push({ city, catPath, key });
      }
    }
  }

  const totalCombos = cities.length * liveCategories.length;
  log(`Expertise.com College / Education Scraper`);
  log(`Cities: ${cities.length} | Categories: ${liveCategories.length} | Total combos: ${totalCombos}`);
  log(`Remaining: ${workItems.length} | Test: ${isTest}`);
  log(`Categories: ${liveCategories.join(', ')}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let notFound = 0;
  let pagesWithResults = 0;
  const categoryHits = {};
  liveCategories.forEach(c => { categoryHits[c] = 0; });

  for (const item of workItems) {
    processed++;
    const { city: cityInfo, catPath, key } = item;
    const url = `https://www.expertise.com/${catPath}/${cityInfo.stateSlug}/${cityInfo.citySlug}`;
    log(`[${processed}/${workItems.length}] ${catPath} | ${cityInfo.city}, ${cityInfo.state}`);

    try {
      const { statusCode, body } = await httpGet(url);

      if (statusCode === 404) {
        log(`  404 - no page`);
        notFound++;
        completedKeys.push(key);
        await randomDelay();
        continue;
      }

      if (statusCode !== 200 || !body) {
        log(`  HTTP ${statusCode} - skipping`);
        errors++;
        await randomDelay();
        continue;
      }

      const leads = parseExpertisePage(body, cityInfo, catPath);
      let newCount = 0;

      for (const lead of leads) {
        const dk = dedupKey(lead.firm_name, lead.city);
        if (seenKeys.has(dk)) continue;
        seenKeys.add(dk);
        allLeads.push(lead);
        newCount++;
      }

      totalNew += newCount;
      if (leads.length > 0) {
        pagesWithResults++;
        categoryHits[catPath] = (categoryHits[catPath] || 0) + leads.length;
      }
      log(`  Found ${leads.length} listings, ${newCount} new (${leads.length - newCount} dupes) | Total: ${allLeads.length}`);

      completedKeys.push(key);

      // Save progress every 20 requests
      if (processed % 20 === 0 && !isTest) {
        saveProgress(completedKeys, allLeads);
        writeCSV(allLeads, OUT_FILE);
        log(`  [Progress saved: ${allLeads.length} total leads]`);
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
      errors++;
    }

    await randomDelay();
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  saveProgress(completedKeys, allLeads);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Requests: ${processed} (${notFound} not found, ${errors} errors)`);
  log(`Pages with results: ${pagesWithResults}`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log('');
  log('Category breakdown:');
  for (const [cat, count] of Object.entries(categoryHits)) {
    log(`  ${cat}: ${count} listings`);
  }
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && errors === 0 && workItems.length === totalCombos) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
