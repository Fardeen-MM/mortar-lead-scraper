#!/usr/bin/env node
/**
 * Expertise.com Immigration Lawyers Scraper
 *
 * Scrapes https://www.expertise.com/legal/immigration-lawyers/{state}/{city}
 * using pure HTTP + Cheerio (JSON-LD structured data is in static HTML).
 *
 * Strategy:
 *   1. Iterate through 150+ major US cities covering all 50 states + DC
 *   2. For each city page, extract JSON-LD LocalBusiness blocks
 *   3. Parse firm name, phone, website, address from structured data
 *   4. Dedup by name+city to avoid duplicates across overlapping city searches
 *   5. Resume support: skip already-scraped cities via progress file
 *   6. Output CSV
 *
 * Usage:
 *   node scripts/scrape-expertise-immigration.js --test         # 5 cities
 *   node scripts/scrape-expertise-immigration.js --all-cities   # All 150+ cities
 *   node scripts/scrape-expertise-immigration.js --resume       # Resume interrupted scrape
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
const OUT_FILE = path.join(OUT_DIR, 'us-immigration-lawyers-expertise.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'expertise-immigration-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── City Database (150+ cities covering all 50 states + DC) ──────────────────
// URL pattern: /legal/immigration-lawyers/{state-slug}/{city-slug}

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
  // Colorado
  { city: 'Denver', state: 'CO', stateSlug: 'colorado', citySlug: 'denver' },
  { city: 'Colorado Springs', state: 'CO', stateSlug: 'colorado', citySlug: 'colorado-springs' },
  { city: 'Aurora', state: 'CO', stateSlug: 'colorado', citySlug: 'aurora' },
  // Connecticut
  { city: 'Hartford', state: 'CT', stateSlug: 'connecticut', citySlug: 'hartford' },
  { city: 'New Haven', state: 'CT', stateSlug: 'connecticut', citySlug: 'new-haven' },
  { city: 'Stamford', state: 'CT', stateSlug: 'connecticut', citySlug: 'stamford' },
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
  // Georgia
  { city: 'Atlanta', state: 'GA', stateSlug: 'georgia', citySlug: 'atlanta' },
  { city: 'Savannah', state: 'GA', stateSlug: 'georgia', citySlug: 'savannah' },
  { city: 'Augusta', state: 'GA', stateSlug: 'georgia', citySlug: 'augusta' },
  // Hawaii
  { city: 'Honolulu', state: 'HI', stateSlug: 'hawaii', citySlug: 'honolulu' },
  // Idaho
  { city: 'Boise', state: 'ID', stateSlug: 'idaho', citySlug: 'boise' },
  // Illinois
  { city: 'Chicago', state: 'IL', stateSlug: 'illinois', citySlug: 'chicago' },
  { city: 'Aurora', state: 'IL', stateSlug: 'illinois', citySlug: 'aurora' },
  { city: 'Naperville', state: 'IL', stateSlug: 'illinois', citySlug: 'naperville' },
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
  // Massachusetts
  { city: 'Boston', state: 'MA', stateSlug: 'massachusetts', citySlug: 'boston' },
  { city: 'Worcester', state: 'MA', stateSlug: 'massachusetts', citySlug: 'worcester' },
  { city: 'Springfield', state: 'MA', stateSlug: 'massachusetts', citySlug: 'springfield' },
  // Michigan
  { city: 'Detroit', state: 'MI', stateSlug: 'michigan', citySlug: 'detroit' },
  { city: 'Grand Rapids', state: 'MI', stateSlug: 'michigan', citySlug: 'grand-rapids' },
  { city: 'Lansing', state: 'MI', stateSlug: 'michigan', citySlug: 'lansing' },
  // Minnesota
  { city: 'Minneapolis', state: 'MN', stateSlug: 'minnesota', citySlug: 'minneapolis' },
  { city: 'Saint Paul', state: 'MN', stateSlug: 'minnesota', citySlug: 'saint-paul' },
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
  // New Hampshire
  { city: 'Manchester', state: 'NH', stateSlug: 'new-hampshire', citySlug: 'manchester' },
  // New Jersey
  { city: 'Newark', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'newark' },
  { city: 'Jersey City', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'jersey-city' },
  { city: 'Elizabeth', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'elizabeth' },
  { city: 'Hackensack', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'hackensack' },
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
  // North Carolina
  { city: 'Charlotte', state: 'NC', stateSlug: 'north-carolina', citySlug: 'charlotte' },
  { city: 'Raleigh', state: 'NC', stateSlug: 'north-carolina', citySlug: 'raleigh' },
  { city: 'Durham', state: 'NC', stateSlug: 'north-carolina', citySlug: 'durham' },
  // North Dakota
  { city: 'Fargo', state: 'ND', stateSlug: 'north-dakota', citySlug: 'fargo' },
  // Ohio
  { city: 'Columbus', state: 'OH', stateSlug: 'ohio', citySlug: 'columbus' },
  { city: 'Cleveland', state: 'OH', stateSlug: 'ohio', citySlug: 'cleveland' },
  { city: 'Cincinnati', state: 'OH', stateSlug: 'ohio', citySlug: 'cincinnati' },
  { city: 'Toledo', state: 'OH', stateSlug: 'ohio', citySlug: 'toledo' },
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
  // Washington
  { city: 'Seattle', state: 'WA', stateSlug: 'washington', citySlug: 'seattle' },
  { city: 'Tacoma', state: 'WA', stateSlug: 'washington', citySlug: 'tacoma' },
  { city: 'Spokane', state: 'WA', stateSlug: 'washington', citySlug: 'spokane' },
  // West Virginia
  { city: 'Charleston', state: 'WV', stateSlug: 'west-virginia', citySlug: 'charleston' },
  // Wisconsin
  { city: 'Milwaukee', state: 'WI', stateSlug: 'wisconsin', citySlug: 'milwaukee' },
  { city: 'Madison', state: 'WI', stateSlug: 'wisconsin', citySlug: 'madison' },
  // Wyoming
  { city: 'Cheyenne', state: 'WY', stateSlug: 'wyoming', citySlug: 'cheyenne' },

  // ── Additional high-immigration cities for coverage ──
  // California extras
  { city: 'Irvine', state: 'CA', stateSlug: 'california', citySlug: 'irvine' },
  { city: 'Pasadena', state: 'CA', stateSlug: 'california', citySlug: 'pasadena' },
  { city: 'Santa Ana', state: 'CA', stateSlug: 'california', citySlug: 'santa-ana' },
  { city: 'Long Beach', state: 'CA', stateSlug: 'california', citySlug: 'long-beach' },
  { city: 'Anaheim', state: 'CA', stateSlug: 'california', citySlug: 'anaheim' },
  // Florida extras
  { city: 'Hialeah', state: 'FL', stateSlug: 'florida', citySlug: 'hialeah' },
  { city: 'St. Petersburg', state: 'FL', stateSlug: 'florida', citySlug: 'saint-petersburg' },
  // Texas extras
  { city: 'Plano', state: 'TX', stateSlug: 'texas', citySlug: 'plano' },
  { city: 'Brownsville', state: 'TX', stateSlug: 'texas', citySlug: 'brownsville' },
  // New York extras
  { city: 'Staten Island', state: 'NY', stateSlug: 'new-york', citySlug: 'staten-island' },
  { city: 'Hempstead', state: 'NY', stateSlug: 'new-york', citySlug: 'hempstead' },
  // New Jersey extras
  { city: 'Paterson', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'paterson' },
  { city: 'Trenton', state: 'NJ', stateSlug: 'new-jersey', citySlug: 'trenton' },
  // Virginia extras
  { city: 'Alexandria', state: 'VA', stateSlug: 'virginia', citySlug: 'alexandria' },
  // Maryland extras
  { city: 'Bethesda', state: 'MD', stateSlug: 'maryland', citySlug: 'bethesda' },
  // Georgia extras
  { city: 'Marietta', state: 'GA', stateSlug: 'georgia', citySlug: 'marietta' },
  // Illinois extras
  { city: 'Schaumburg', state: 'IL', stateSlug: 'illinois', citySlug: 'schaumburg' },
  // Massachusetts extras
  { city: 'Cambridge', state: 'MA', stateSlug: 'massachusetts', citySlug: 'cambridge' },
  // Arizona extras
  { city: 'Scottsdale', state: 'AZ', stateSlug: 'arizona', citySlug: 'scottsdale' },
  // Colorado extras
  { city: 'Fort Collins', state: 'CO', stateSlug: 'colorado', citySlug: 'fort-collins' },
  // Washington extras
  { city: 'Bellevue', state: 'WA', stateSlug: 'washington', citySlug: 'bellevue' },
  // North Carolina extras
  { city: 'Greensboro', state: 'NC', stateSlug: 'north-carolina', citySlug: 'greensboro' },
  // Michigan extras
  { city: 'Ann Arbor', state: 'MI', stateSlug: 'michigan', citySlug: 'ann-arbor' },
  // Minnesota extras
  { city: 'Bloomington', state: 'MN', stateSlug: 'minnesota', citySlug: 'bloomington' },
  // Connecticut extras
  { city: 'Bridgeport', state: 'CT', stateSlug: 'connecticut', citySlug: 'bridgeport' },
  // Nevada extras
  { city: 'Henderson', state: 'NV', stateSlug: 'nevada', citySlug: 'henderson' },
  // Tennessee extras
  { city: 'Chattanooga', state: 'TN', stateSlug: 'tennessee', citySlug: 'chattanooga' },
  // Ohio extras
  { city: 'Akron', state: 'OH', stateSlug: 'ohio', citySlug: 'akron' },
  { city: 'Dayton', state: 'OH', stateSlug: 'ohio', citySlug: 'dayton' },
  // Pennsylvania extras
  { city: 'Allentown', state: 'PA', stateSlug: 'pennsylvania', citySlug: 'allentown' },
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

  // Try to extract a person name from the firm name
  let cleaned = firmName;

  // "Law Office(s) of John Smith" / "Law Firm of John Smith"
  const ofMatch = cleaned.match(/(?:Law\s+(?:Office|Offices|Firm|Group|Practice)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|Attorney.*|Law.*|&.*)/gi, '')
      .trim();
  } else {
    // Strip common suffixes to try to get a person name
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .replace(/\s*(Law\s+(?:Firm|Group|Office|Offices|Practice|Center)|Legal\s+(?:Group|Services?|Team)|Immigration\s+(?:Law|Attorneys?|Lawyers?)|Attorneys?\s+at\s+Law|Attorneys?|Lawyers?|& Associates?|Associates?)/gi, '')
      .trim();
  }

  // Remove trailing punctuation
  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip names starting with "The" (e.g., "The Love Law Firm")
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if it contains & or "and" (multi-partner firms like "Smith & Jones")
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if the "last name" would be a law-related word
  const lawWords = /^(law|legal|firm|group|office|pllc|plc|pc|immigration|attorneys?|lawyers?|counselors?|associates?)$/i;

  // If what remains looks like a person name (2-3 words, no special chars)
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    // Don't treat "Law", "Legal", etc. as a last name
    if (lawWords.test(lastName)) return { first_name: '', last_name: '' };
    return {
      first_name: parts[0],
      last_name: lastName,
    };
  }

  return { first_name: '', last_name: '' };
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  // Raw phone from JSON-LD is digits only (e.g., "3109787426")
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

// ── Page Parser ──────────────────────────────────────────────────────────────

function parseExpertisePage(html, cityInfo) {
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
        niche: 'immigration',
        source: 'expertise.com',
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
  return { completedCities: [], leads: [] };
}

function saveProgress(completedCities, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedCities,
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
    console.log('  node scripts/scrape-expertise-immigration.js --test         # Test mode (5 cities)');
    console.log('  node scripts/scrape-expertise-immigration.js --all-cities   # All cities');
    console.log('  node scripts/scrape-expertise-immigration.js --resume       # Resume interrupted scrape');
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
    if (progress.completedCities.length > 0 && isResume) {
      completedCities = progress.completedCities;
      allLeads = progress.leads || [];
      // Rebuild dedup set
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedCities.length} cities already done, ${allLeads.length} leads loaded`);
    }
  }

  const citiesToScrape = cities.filter(c => {
    const key = `${c.stateSlug}/${c.citySlug}`;
    return !completedCities.includes(key);
  });

  log(`Expertise.com Immigration Lawyers Scraper`);
  log(`Total cities: ${cities.length} | Remaining: ${citiesToScrape.length} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let notFound = 0;

  for (const cityInfo of citiesToScrape) {
    processed++;
    const url = `https://www.expertise.com/legal/immigration-lawyers/${cityInfo.stateSlug}/${cityInfo.citySlug}`;
    log(`[${processed}/${citiesToScrape.length}] ${cityInfo.city}, ${cityInfo.state} -> ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);

      if (statusCode === 404) {
        log(`  404 Not Found - no page for ${cityInfo.city}, ${cityInfo.state}`);
        notFound++;
        completedCities.push(`${cityInfo.stateSlug}/${cityInfo.citySlug}`);
        await randomDelay();
        continue;
      }

      if (statusCode !== 200 || !body) {
        log(`  HTTP ${statusCode} - skipping`);
        errors++;
        await randomDelay();
        continue;
      }

      const leads = parseExpertisePage(body, cityInfo);
      let newCount = 0;

      for (const lead of leads) {
        const key = dedupKey(lead.firm_name, lead.city);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        allLeads.push(lead);
        newCount++;
      }

      totalNew += newCount;
      log(`  Found ${leads.length} listings, ${newCount} new (${leads.length - newCount} dupes)`);

      completedCities.push(`${cityInfo.stateSlug}/${cityInfo.citySlug}`);

      // Save progress every 10 cities
      if (processed % 10 === 0 && !isTest) {
        saveProgress(completedCities, allLeads);
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
  saveProgress(completedCities, allLeads);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Cities scraped: ${processed} (${notFound} not found, ${errors} errors)`);
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
