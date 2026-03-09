#!/usr/bin/env node
/**
 * Lawyer Legion Immigration Lawyers Scraper
 *
 * Scrapes https://lawyers.lawyerlegion.com/{state}/immigration-law
 * using pure HTTP + Cheerio.
 *
 * Strategy:
 *   1. Iterate all 50 US states + DC
 *   2. For each state, paginate through listing pages (50 per page, up to 250)
 *   3. Parse lawyer cards from HTML: name, firm, phone, website, address
 *   4. Also parse map_points JS variable for structured backup data
 *   5. Dedup by name+city to avoid duplicates
 *   6. Resume support: skip completed states, save incrementally
 *   7. Output CSV
 *
 * Usage:
 *   node scripts/scrape-lawyerlegion-immigration.js --test              # 2-3 states, 2 pages max
 *   node scripts/scrape-lawyerlegion-immigration.js --states CA,TX,NY   # Specific states
 *   node scripts/scrape-lawyerlegion-immigration.js --all-states        # All 50 states + DC
 *   node scripts/scrape-lawyerlegion-immigration.js --all-states        # Resume auto-detected
 *   node scripts/scrape-lawyerlegion-immigration.js --enrich --test     # Enrich 10 profiles (test)
 *   node scripts/scrape-lawyerlegion-immigration.js --enrich            # Enrich all profiles
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
const PAGE_SIZE = 50;
const MAX_PAGES = 10; // safety cap (250 results = 5 pages, but allow more)

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'us-immigration-lawyers-lawyerlegion.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'lawyerlegion-immigration-progress.json');
const ENRICH_PROGRESS_FILE = path.join(OUT_DIR, 'lawyerlegion-enrich-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'fax', 'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url', 'bar_admissions', 'years_licensed',
];

// ── State Database (50 states + DC) ──────────────────────────────────────────
// URL pattern: lawyers.lawyerlegion.com/{state-slug}/immigration-law

const STATES = [
  { code: 'AL', slug: 'alabama', name: 'Alabama' },
  { code: 'AK', slug: 'alaska', name: 'Alaska' },
  { code: 'AZ', slug: 'arizona', name: 'Arizona' },
  { code: 'AR', slug: 'arkansas', name: 'Arkansas' },
  { code: 'CA', slug: 'california', name: 'California' },
  { code: 'CO', slug: 'colorado', name: 'Colorado' },
  { code: 'CT', slug: 'connecticut', name: 'Connecticut' },
  { code: 'DE', slug: 'delaware', name: 'Delaware' },
  { code: 'DC', slug: 'district-of-columbia', name: 'District of Columbia' },
  { code: 'FL', slug: 'florida', name: 'Florida' },
  { code: 'GA', slug: 'georgia', name: 'Georgia' },
  { code: 'HI', slug: 'hawaii', name: 'Hawaii' },
  { code: 'ID', slug: 'idaho', name: 'Idaho' },
  { code: 'IL', slug: 'illinois', name: 'Illinois' },
  { code: 'IN', slug: 'indiana', name: 'Indiana' },
  { code: 'IA', slug: 'iowa', name: 'Iowa' },
  { code: 'KS', slug: 'kansas', name: 'Kansas' },
  { code: 'KY', slug: 'kentucky', name: 'Kentucky' },
  { code: 'LA', slug: 'louisiana', name: 'Louisiana' },
  { code: 'ME', slug: 'maine', name: 'Maine' },
  { code: 'MD', slug: 'maryland', name: 'Maryland' },
  { code: 'MA', slug: 'massachusetts', name: 'Massachusetts' },
  { code: 'MI', slug: 'michigan', name: 'Michigan' },
  { code: 'MN', slug: 'minnesota', name: 'Minnesota' },
  { code: 'MS', slug: 'mississippi', name: 'Mississippi' },
  { code: 'MO', slug: 'missouri', name: 'Missouri' },
  { code: 'MT', slug: 'montana', name: 'Montana' },
  { code: 'NE', slug: 'nebraska', name: 'Nebraska' },
  { code: 'NV', slug: 'nevada', name: 'Nevada' },
  { code: 'NH', slug: 'new-hampshire', name: 'New Hampshire' },
  { code: 'NJ', slug: 'new-jersey', name: 'New Jersey' },
  { code: 'NM', slug: 'new-mexico', name: 'New Mexico' },
  { code: 'NY', slug: 'new-york', name: 'New York' },
  { code: 'NC', slug: 'north-carolina', name: 'North Carolina' },
  { code: 'ND', slug: 'north-dakota', name: 'North Dakota' },
  { code: 'OH', slug: 'ohio', name: 'Ohio' },
  { code: 'OK', slug: 'oklahoma', name: 'Oklahoma' },
  { code: 'OR', slug: 'oregon', name: 'Oregon' },
  { code: 'PA', slug: 'pennsylvania', name: 'Pennsylvania' },
  { code: 'RI', slug: 'rhode-island', name: 'Rhode Island' },
  { code: 'SC', slug: 'south-carolina', name: 'South Carolina' },
  { code: 'SD', slug: 'south-dakota', name: 'South Dakota' },
  { code: 'TN', slug: 'tennessee', name: 'Tennessee' },
  { code: 'TX', slug: 'texas', name: 'Texas' },
  { code: 'UT', slug: 'utah', name: 'Utah' },
  { code: 'VT', slug: 'vermont', name: 'Vermont' },
  { code: 'VA', slug: 'virginia', name: 'Virginia' },
  { code: 'WA', slug: 'washington', name: 'Washington' },
  { code: 'WV', slug: 'west-virginia', name: 'West Virginia' },
  { code: 'WI', slug: 'wisconsin', name: 'Wisconsin' },
  { code: 'WY', slug: 'wyoming', name: 'Wyoming' },
];

// Map full state names to codes for normalization
const STATE_NAME_TO_CODE = {};
for (const s of STATES) {
  STATE_NAME_TO_CODE[s.name.toLowerCase()] = s.code;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/')
    .replace(/&ndash;/g, '\u2013')
    .replace(/&mdash;/g, '\u2014');
}

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

  // Clean up name
  let cleaned = fullName.trim()
    .replace(/,?\s*(Esq\.?|Jr\.?|Sr\.?|III|II|IV)$/i, '')
    .trim();

  // Split into parts
  const parts = cleaned.split(/\s+/);
  if (parts.length < 2) return { first_name: cleaned, last_name: '' };

  // Handle "First Middle Last" or "First Last"
  const first_name = parts[0];
  const last_name = parts[parts.length - 1];

  return { first_name, last_name };
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
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

/**
 * Check if a URL is a valid lawyer website (not a social media, maps, or platform link).
 */
const EXCLUDED_DOMAINS = [
  'lawyerlegion.com', 'google.com', 'googleapis.com', 'maps.google.com',
  'facebook.com', 'twitter.com', 'x.com', 'linkedin.com', 'instagram.com',
  'youtube.com', 'tiktok.com', 'pinterest.com', 'yelp.com', 'avvo.com',
  'justia.com', 'findlaw.com', 'martindale.com', 'lawyers.com',
  'superlawyers.com', 'nolo.com',
];

function isValidWebsite(url) {
  if (!url) return false;
  try {
    const hostname = new URL(url).hostname.toLowerCase().replace(/^www\./, '');
    return !EXCLUDED_DOMAINS.some(d => hostname === d || hostname.endsWith('.' + d));
  } catch {
    return false;
  }
}

function normalizeStateCode(stateStr) {
  if (!stateStr) return '';
  // Already a code
  if (/^[A-Z]{2}$/.test(stateStr)) return stateStr;
  // Lookup by name
  const code = STATE_NAME_TO_CODE[(stateStr || '').toLowerCase().trim()];
  return code || stateStr;
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

function dedupKey(firstName, lastName, city) {
  return `${(firstName || '').toLowerCase().trim()}|${(lastName || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
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
        } else if (!redirectUrl.startsWith('http')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}/${redirectUrl}`;
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

// ── Page Parsers ─────────────────────────────────────────────────────────────

/**
 * Extract total count from "Showing attorneys X-Y out of Z" text.
 * Uses Cheerio to read the .search-showing element (handles &ndash; etc.)
 */
function extractTotalCount($) {
  const showingText = $('.search-showing').text().trim();
  if (showingText) {
    const match = showingText.match(/out\s+of\s+(\d+)/i);
    if (match) return parseInt(match[1], 10);
  }
  return 0;
}

/**
 * Parse the map_points JavaScript variable for structured lawyer data.
 * Format: { "id": [lat, lng, name, street, city, stateName, zip, profileUrl, firmName], ... }
 */
function parseMapPoints(html) {
  const results = [];
  // Use lazy match to capture the JSON object between { and };
  const match = html.match(/var\s+map_points\s*=\s*(\{[\s\S]*?\});/);
  if (!match) return results;

  try {
    const data = JSON.parse(match[1]);
    for (const [id, arr] of Object.entries(data)) {
      if (!Array.isArray(arr) || arr.length < 9) continue;
      results.push({
        id,
        lat: arr[0],
        lng: arr[1],
        name: decodeHtmlEntities((arr[2] || '').trim()),
        street: decodeHtmlEntities((arr[3] || '').trim()),
        city: decodeHtmlEntities((arr[4] || '').trim()),
        stateName: (arr[5] || '').trim(),
        zip: (arr[6] || '').trim(),
        profileUrl: (arr[7] || '').trim(),
        firmName: decodeHtmlEntities((arr[8] || '').trim()),
      });
    }
  } catch (e) {
    // map_points parse failed, fall through to HTML parsing
  }
  return results;
}

/**
 * Parse each .listing card to extract phone and website, keyed by profile URL.
 * Structure per card:
 *   - Profile link: a[href] matching /{state}/{name}-{digits}
 *   - Phone: a[href^="tel:"] (appears 2x per card: visible + mobile "TAP TO CALL")
 *   - Website: a.site-btn[href] (external URL, only present if lawyer has a website)
 */
function parseListingCards($) {
  const phoneMap = {};    // profileUrl -> phone
  const websiteMap = {};  // profileUrl -> website

  $('.listing').each((_, el) => {
    const card = $(el);

    // Find the profile URL (first link matching the pattern /{state}/{name}-{id})
    let profileUrl = '';
    card.find('a[href]').each((_, a) => {
      if (profileUrl) return; // already found
      const href = $(a).attr('href') || '';
      if (href.match(/lawyerlegion\.com\/[a-z-]+\/[a-z-]+-\d+$/)) {
        profileUrl = href;
      }
    });
    if (!profileUrl) return;

    // Extract phone from tel: link (take first one, skip duplicates)
    const telLink = card.find('a[href^="tel:"]').first();
    if (telLink.length) {
      const phone = telLink.attr('href').replace('tel:', '').trim();
      if (phone) phoneMap[profileUrl] = phone;
    }

    // Extract website from .site-btn link
    const siteBtn = card.find('a.site-btn[href]').first();
    if (siteBtn.length) {
      const href = siteBtn.attr('href') || '';
      if (href.startsWith('http') && !href.includes('lawyerlegion.com')) {
        websiteMap[profileUrl] = href;
      }
    }
  });

  return { phoneMap, websiteMap };
}

/**
 * Parse a single listing page and return leads array.
 */
function parseListingPage(html, stateInfo) {
  const $ = cheerio.load(html);
  const leads = [];

  // 1. Parse map_points for structured data (name, firm, address, profile URL)
  const mapEntries = parseMapPoints(html);

  // 2. Parse HTML .listing cards for phone + website (not in map_points)
  const { phoneMap, websiteMap } = parseListingCards($);

  // 2b. Build a secondary lookup by ID suffix (handles + chars in map_points URLs)
  const phoneById = {};
  const websiteById = {};
  for (const [url, phone] of Object.entries(phoneMap)) {
    const idMatch = url.match(/-(\d+)$/);
    if (idMatch) phoneById[idMatch[1]] = phone;
  }
  for (const [url, website] of Object.entries(websiteMap)) {
    const idMatch = url.match(/-(\d+)$/);
    if (idMatch) websiteById[idMatch[1]] = website;
  }

  // 3. Build lead objects by merging map_points data with card-extracted phone/website
  for (const entry of mapEntries) {
    const { first_name, last_name } = parseName(entry.name);
    const stateCode = normalizeStateCode(entry.stateName) || stateInfo.code;
    const phone = formatPhone(phoneMap[entry.profileUrl] || phoneById[entry.id] || '');
    const website = websiteMap[entry.profileUrl] || websiteById[entry.id] || '';

    leads.push({
      first_name,
      last_name,
      firm_name: entry.firmName || '',
      title: '',
      email: '',
      phone,
      website,
      domain: extractDomain(website),
      city: entry.city || '',
      state: stateCode,
      country: 'US',
      niche: 'immigration',
      source: 'lawyerlegion',
      profile_url: entry.profileUrl || '',
    });
  }

  // 4. Fallback: if map_points was empty, parse profile links from HTML directly
  if (leads.length === 0) {
    const profileRegex = new RegExp(`lawyerlegion\\.com/${stateInfo.slug}/[a-z-]+-\\d+$`);
    const seenUrls = new Set();

    $('.listing').each((_, el) => {
      const card = $(el);
      let profileUrl = '';
      let name = '';

      card.find('a[href]').each((_, a) => {
        if (profileUrl) return;
        const href = $(a).attr('href') || '';
        if (profileRegex.test(href)) {
          profileUrl = href;
          const linkText = $(a).text().trim();
          // Use first non-empty link text as name (skip "View profile", "read more" etc.)
          if (linkText && linkText.length > 2 && linkText.length < 80 &&
              !/view|profile|more|call|message|compare|favorite|tap|email|website/i.test(linkText)) {
            name = linkText;
          }
        }
      });

      if (!profileUrl || seenUrls.has(profileUrl)) return;
      seenUrls.add(profileUrl);

      const { first_name, last_name } = parseName(name);

      // Phone
      const telLink = card.find('a[href^="tel:"]').first();
      const phone = telLink.length ? formatPhone(telLink.attr('href').replace('tel:', '').trim()) : '';

      // Website
      const siteBtn = card.find('a.site-btn[href]').first();
      let website = '';
      if (siteBtn.length) {
        const href = siteBtn.attr('href') || '';
        if (href.startsWith('http') && !href.includes('lawyerlegion.com')) website = href;
      }

      const fullUrl = profileUrl.startsWith('http') ? profileUrl : `https://lawyers.lawyerlegion.com${profileUrl}`;

      leads.push({
        first_name,
        last_name,
        firm_name: '',
        title: '',
        email: '',
        phone,
        website,
        domain: extractDomain(website),
        city: '',
        state: stateInfo.code,
        country: 'US',
        niche: 'immigration',
        source: 'lawyerlegion',
        profile_url: fullUrl,
      });
    });
  }

  return leads;
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedStates: [], leads: [] };
}

function saveProgress(completedStates, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedStates,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── State Scraper ────────────────────────────────────────────────────────────

async function scrapeState(stateInfo, maxPages) {
  const allLeads = [];
  let page = 1;
  let totalCount = 0;
  let totalPages = 1;

  while (page <= totalPages && page <= maxPages) {
    const url = page === 1
      ? `https://lawyers.lawyerlegion.com/${stateInfo.slug}/immigration-law`
      : `https://lawyers.lawyerlegion.com/${stateInfo.slug}/immigration-law?page=${page}`;

    log(`  Page ${page}/${totalPages > 1 ? totalPages : '?'}: ${url}`);

    const { statusCode, body } = await httpGet(url);

    if (statusCode === 404) {
      log(`    404 - no immigration lawyers page for ${stateInfo.name}`);
      break;
    }

    if (statusCode !== 200 || !body) {
      log(`    HTTP ${statusCode} - skipping page`);
      break;
    }

    // On first page, extract total count to compute pages
    if (page === 1) {
      const $page = cheerio.load(body);
      totalCount = extractTotalCount($page);
      if (totalCount > 0) {
        totalPages = Math.ceil(totalCount / PAGE_SIZE);
        log(`    Total: ${totalCount} lawyers across ${totalPages} page(s)`);
      } else {
        log(`    Could not detect total count, will scrape page 1 only`);
      }
    }

    const leads = parseListingPage(body, stateInfo);
    log(`    Parsed ${leads.length} lawyers from page ${page}`);
    allLeads.push(...leads);

    // If we got fewer than expected, we've reached the end
    if (leads.length === 0) break;

    page++;
    if (page <= totalPages && page <= maxPages) {
      await randomDelay();
    }
  }

  return allLeads;
}

// ── CSV Reader ───────────────────────────────────────────────────────────────

/**
 * Parse a CSV line handling quoted fields with commas inside.
 */
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

/**
 * Read existing CSV file into array of lead objects.
 */
function readCSV(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const leads = [];

  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    const lead = {};
    for (let j = 0; j < headers.length; j++) {
      lead[headers[j]] = (fields[j] || '').trim();
    }
    // Ensure all CSV_COLUMNS exist
    for (const col of CSV_COLUMNS) {
      if (!(col in lead)) lead[col] = '';
    }
    leads.push(lead);
  }

  return leads;
}

// ── Profile Page Parser ──────────────────────────────────────────────────────

/**
 * Parse a Lawyer Legion profile page and extract enrichment data.
 *
 * Profile pages contain:
 *   - JS vars: var Firm = "..."; var street_1 = "..."; etc.
 *   - JSON-LD schema.org with telephone, address
 *   - #Contact div with Office phone, Fax, address
 *   - Bar license section with state, status, year
 *   - About section with "Licensed for X years"
 *   - Website links
 */
function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const result = {
    phone: '',
    fax: '',
    email: '',
    website: '',
    firm_name: '',
    bar_admissions: '',
    years_licensed: '',
  };

  // 1. Extract from JavaScript variables
  const firmMatch = html.match(/var\s+Firm\s*=\s*"([^"]*)"/);
  if (firmMatch && firmMatch[1].trim()) {
    result.firm_name = decodeHtmlEntities(firmMatch[1].trim());
  }

  // 2. Extract from JSON-LD schema.org
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      let parsed = JSON.parse($(el).html());
      // Handle array of objects (common: [{Person}, {LegalService}])
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const data of items) {
        if (data['@type'] === 'LegalService' || data['@type'] === 'Attorney') {
          if (data.telephone && !result.phone) {
            result.phone = data.telephone;
          }
          if (data.url && !result.website) {
            if (isValidWebsite(data.url)) {
              result.website = data.url;
            }
          }
        }
      }
    } catch {}
  });

  // 3. Extract from #Contact section
  const contactDiv = $('#Contact');
  if (contactDiv.length) {
    // Phone & fax from <div class="telnum"> elements
    contactDiv.find('.telnum').each((_, el) => {
      const text = $(el).text().trim();
      const officeMatch = text.match(/Office:\s*([\d\(\)\s\-\.]+)/);
      if (officeMatch && !result.phone) {
        result.phone = officeMatch[1].trim();
      }
      const faxMatch = text.match(/Fax:\s*([\d\(\)\s\-\.]+)/);
      if (faxMatch && !result.fax) {
        result.fax = faxMatch[1].trim();
      }
    });

    // Fallback: phone from general contact text
    if (!result.phone) {
      const contactText = contactDiv.text();
      const officeMatch = contactText.match(/Office:\s*([\d\(\)\s\-\.]+)/);
      if (officeMatch) {
        result.phone = officeMatch[1].trim();
      }
    }

    // Email from mailto: link
    contactDiv.find('a[href^="mailto:"]').each((_, a) => {
      const email = $(a).attr('href').replace('mailto:', '').trim();
      if (email && email.includes('@')) {
        result.email = email;
      }
    });

    // Website from external link in contact section (skip social/message links)
    contactDiv.find('a[href]').each((_, a) => {
      const href = $(a).attr('href') || '';
      if (href.startsWith('http') && !href.startsWith('mailto:') && !result.website && isValidWebsite(href)) {
        result.website = href;
      }
    });
  }

  // 4. Extract phone from tel: links anywhere on the page
  if (!result.phone) {
    $('a[href^="tel:"]').first().each((_, a) => {
      const phone = $(a).attr('href').replace('tel:', '').trim();
      if (phone) result.phone = phone;
    });
  }

  // 5. Extract website from site-btn or external links
  if (!result.website) {
    $('a.site-btn[href]').each((_, a) => {
      const href = $(a).attr('href') || '';
      if (href.startsWith('http') && isValidWebsite(href)) {
        result.website = href;
      }
    });
  }

  // Also check for website in JS: look for window.open or href patterns
  if (!result.website) {
    const websiteJsMatch = html.match(/(?:website|url)\s*[:=]\s*["'](https?:\/\/[^"']+)["']/i);
    if (websiteJsMatch && isValidWebsite(websiteJsMatch[1])) {
      result.website = websiteJsMatch[1];
    }
  }

  // Final validation: clear invalid websites
  if (result.website && !isValidWebsite(result.website)) {
    result.website = '';
  }

  // 6. Extract bar admissions and year
  // HTML structure: .resume-block contains:
  //   .rb-img > img[src*="statebars/Alabama.png"]
  //   .rb-txt > .rb-title > a (state name)
  //           > .rb-subtxt (status text, e.g. "Active - Member in Good Standing")
  //           > .rb-subtxt (year, e.g. "2011")
  const barAdmissions = [];
  $('.resume-block').each((_, block) => {
    const $block = $(block);
    const img = $block.find('img[src*="statebars/"]');
    if (!img.length) return;

    // State name from the title link or from the image filename
    let stateName = $block.find('.rb-title a').text().trim();
    if (!stateName) {
      const src = img.attr('src') || '';
      const m = src.match(/statebars\/([^.]+)\.png/);
      if (m) stateName = m[1].replace(/-/g, ' ');
    }
    if (!stateName) return;

    // Status and year from .rb-subtxt divs
    let status = '';
    let year = '';
    $block.find('.rb-subtxt').each((_, sub) => {
      const text = $(sub).text().trim();
      if (/^(19|20)\d{2}$/.test(text)) {
        year = text;
      } else if (/active|inactive|suspended|retired|deceased/i.test(text)) {
        status = text;
      }
    });

    barAdmissions.push(`${stateName}${year ? ' (' + year + ')' : ''}${status ? ' - ' + status : ''}`);
  });

  if (barAdmissions.length > 0) {
    result.bar_admissions = barAdmissions.join('; ');
  }

  // 7. Extract years licensed from about text
  const aboutText = $('body').text();
  const yearsMatch = aboutText.match(/Licensed\s+for\s+(\d+)\s+years?/i);
  if (yearsMatch) {
    result.years_licensed = yearsMatch[1];
  }

  // If no years_licensed from text, compute from earliest bar admission year
  if (!result.years_licensed && barAdmissions.length > 0) {
    const years = barAdmissions.map(a => {
      const m = a.match(/\((\d{4})\)/);
      return m ? parseInt(m[1], 10) : 0;
    }).filter(y => y > 0);
    if (years.length > 0) {
      const earliest = Math.min(...years);
      result.years_licensed = String(new Date().getFullYear() - earliest);
    }
  }

  // Format phone
  result.phone = formatPhone(result.phone);

  return result;
}

// ── Enrichment Phase ─────────────────────────────────────────────────────────

/**
 * Load enrichment progress (set of visited profile URLs).
 */
function loadEnrichProgress() {
  try {
    if (fs.existsSync(ENRICH_PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(ENRICH_PROGRESS_FILE, 'utf8'));
      return new Set(data.visitedUrls || []);
    }
  } catch {}
  return new Set();
}

/**
 * Save enrichment progress.
 */
function saveEnrichProgress(visitedUrls) {
  fs.writeFileSync(ENRICH_PROGRESS_FILE, JSON.stringify({
    visitedUrls: Array.from(visitedUrls),
    count: visitedUrls.size,
    updatedAt: new Date().toISOString(),
  }));
}

/**
 * Enrich leads by visiting profile pages.
 * Reads CSV, visits each profile URL, extracts contact data, updates CSV.
 */
async function enrichLeads(isTest) {
  const maxProfiles = isTest ? 10 : Infinity;

  log('');
  log('='.repeat(60));
  log('ENRICHMENT PHASE');
  log(`Reading CSV: ${OUT_FILE}`);
  log(`Test mode: ${isTest} (max ${isTest ? 10 : 'all'} profiles)`);
  log('='.repeat(60));
  log('');

  // Read existing CSV
  const leads = readCSV(OUT_FILE);
  if (leads.length === 0) {
    log('ERROR: No leads found in CSV. Run scraper first.');
    return;
  }
  log(`Loaded ${leads.length} leads from CSV`);

  // Load enrichment progress (resume support)
  const visitedUrls = loadEnrichProgress();
  log(`Resume: ${visitedUrls.size} profiles already visited`);

  // Filter to leads that have a profile_url and haven't been visited yet
  const toEnrich = [];
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    if (!lead.profile_url) continue;
    if (visitedUrls.has(lead.profile_url)) continue;
    toEnrich.push({ index: i, lead });
  }

  log(`Profiles to enrich: ${toEnrich.length} (skipping ${visitedUrls.size} already visited)`);

  if (toEnrich.length === 0) {
    log('All profiles already visited. Delete enrich progress file to re-run:');
    log(`  rm ${ENRICH_PROGRESS_FILE}`);
    return;
  }

  const total = Math.min(toEnrich.length, maxProfiles);
  let enriched = 0;
  let errors = 0;
  let newPhones = 0;
  let newEmails = 0;
  let newWebsites = 0;
  let newFirms = 0;
  let http404s = 0;
  const startTime = Date.now();

  for (let i = 0; i < total; i++) {
    const { index, lead } = toEnrich[i];
    const profileUrl = lead.profile_url;

    const progress = `[${i + 1}/${total}]`;
    log(`${progress} ${lead.first_name} ${lead.last_name} (${lead.state})`);
    log(`  URL: ${profileUrl}`);

    try {
      const { statusCode, body } = await httpGet(profileUrl);

      if (statusCode === 404) {
        log('  => 404 Not Found (profile removed)');
        http404s++;
        visitedUrls.add(profileUrl);
        continue;
      }

      if (statusCode !== 200 || !body) {
        log(`  => HTTP ${statusCode} - skipping`);
        errors++;
        continue; // Don't mark as visited so we retry later
      }

      const data = parseProfilePage(body);

      // Merge enrichment data into lead (only fill empty fields)
      let updated = false;

      if (data.phone && !lead.phone) {
        leads[index].phone = data.phone;
        newPhones++;
        updated = true;
      }
      if (data.email && !lead.email) {
        leads[index].email = data.email;
        newEmails++;
        updated = true;
      }
      if (data.website && !lead.website) {
        leads[index].website = data.website;
        leads[index].domain = extractDomain(data.website);
        newWebsites++;
        updated = true;
      }
      if (data.fax) {
        leads[index].fax = formatPhone(data.fax);
        updated = true;
      }
      if (data.firm_name && !lead.firm_name) {
        leads[index].firm_name = data.firm_name;
        newFirms++;
        updated = true;
      }
      if (data.bar_admissions) {
        leads[index].bar_admissions = data.bar_admissions;
        updated = true;
      }
      if (data.years_licensed) {
        leads[index].years_licensed = data.years_licensed;
        updated = true;
      }

      enriched++;
      visitedUrls.add(profileUrl);

      const fields = [];
      if (data.phone) fields.push(`phone: ${data.phone}`);
      if (data.email) fields.push(`email: ${data.email}`);
      if (data.website) fields.push(`web: ${extractDomain(data.website)}`);
      if (data.firm_name) fields.push(`firm: ${data.firm_name.slice(0, 30)}`);
      if (data.bar_admissions) fields.push(`bar: ${data.bar_admissions.slice(0, 40)}`);
      if (data.years_licensed) fields.push(`${data.years_licensed}yr`);

      log(`  => ${fields.length > 0 ? fields.join(' | ') : 'no new data'}`);

    } catch (err) {
      log(`  => ERROR: ${err.message}`);
      errors++;
    }

    // Save every 25 profiles
    if ((i + 1) % 25 === 0 || i === total - 1) {
      writeCSV(leads, OUT_FILE);
      saveEnrichProgress(visitedUrls);
      log(`  [saved: ${i + 1}/${total} profiles, CSV updated]`);
    }

    // Rate limiting: 2-3 second delay
    if (i < total - 1) {
      await randomDelay();
    }
  }

  // Final save
  writeCSV(leads, OUT_FILE);
  saveEnrichProgress(visitedUrls);

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const totalLeads = leads.length;
  const withPhone = leads.filter(l => l.phone).length;
  const withEmail = leads.filter(l => l.email).length;
  const withWebsite = leads.filter(l => l.website).length;
  const withFirm = leads.filter(l => l.firm_name).length;
  const withBar = leads.filter(l => l.bar_admissions).length;

  log('');
  log('='.repeat(60));
  log('ENRICHMENT COMPLETE');
  log('='.repeat(60));
  log(`Time: ${Math.floor(elapsed / 60)}m ${elapsed % 60}s`);
  log(`Profiles visited: ${enriched} (${errors} errors, ${http404s} 404s)`);
  log(`New data found:`);
  log(`  +${newPhones} phones, +${newEmails} emails, +${newWebsites} websites, +${newFirms} firms`);
  log(`Overall totals (${totalLeads} leads):`);
  log(`  With phone:     ${withPhone} (${Math.round(withPhone / totalLeads * 100)}%)`);
  log(`  With email:     ${withEmail} (${Math.round(withEmail / totalLeads * 100)}%)`);
  log(`  With website:   ${withWebsite} (${Math.round(withWebsite / totalLeads * 100)}%)`);
  log(`  With firm:      ${withFirm} (${Math.round(withFirm / totalLeads * 100)}%)`);
  log(`  With bar info:  ${withBar} (${Math.round(withBar / totalLeads * 100)}%)`);
  log(`Total visited: ${visitedUrls.size}/${totalLeads}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isEnrich = args.includes('--enrich');
  const isAllStates = args.includes('--all-states');
  const statesArgIdx = args.indexOf('--states');
  const specificStates = statesArgIdx >= 0 && args[statesArgIdx + 1]
    ? args[statesArgIdx + 1].toUpperCase().split(',')
    : null;

  // Enrichment mode: visit profile pages to extract contact details
  if (isEnrich) {
    await enrichLeads(isTest);
    return;
  }

  if (!isTest && !isAllStates && !specificStates) {
    console.log('Lawyer Legion Immigration Lawyers Scraper');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --test              # Test: 3 states, 2 pages each');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --states CA,TX,NY   # Specific states');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --all-states        # All 50 states + DC');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --enrich --test     # Enrich 10 profiles (test)');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --enrich            # Enrich all profiles');
    console.log('');
    console.log('Resume is automatic: completed states are skipped on re-run.');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Determine which states to scrape
  let statesToScrape = [...STATES];
  let maxPagesPerState = MAX_PAGES;

  if (isTest) {
    statesToScrape = STATES.filter(s => ['CA', 'TX', 'WY'].includes(s.code));
    maxPagesPerState = 2;
    log('TEST MODE: CA, TX, WY only, max 2 pages each');
  } else if (specificStates) {
    statesToScrape = STATES.filter(s => specificStates.includes(s.code));
    if (statesToScrape.length === 0) {
      console.error(`No valid state codes found in: ${specificStates.join(', ')}`);
      console.error(`Valid codes: ${STATES.map(s => s.code).join(', ')}`);
      process.exit(1);
    }
    log(`Scraping ${statesToScrape.length} specific state(s): ${statesToScrape.map(s => s.code).join(', ')}`);
  }

  // Load progress for resume
  let allLeads = [];
  let completedStates = [];
  const seenKeys = new Set();

  const progress = loadProgress();
  if (progress.completedStates.length > 0) {
    completedStates = progress.completedStates;
    allLeads = progress.leads || [];
    for (const lead of allLeads) {
      seenKeys.add(dedupKey(lead.first_name, lead.last_name, lead.city));
    }
    log(`Resuming: ${completedStates.length} states done, ${allLeads.length} leads loaded`);
  }

  // Filter out completed states
  const remaining = statesToScrape.filter(s => !completedStates.includes(s.code));

  log('');
  log('='.repeat(60));
  log('Lawyer Legion Immigration Lawyers Scraper');
  log(`States: ${statesToScrape.length} total | Remaining: ${remaining.length} | Test: ${isTest}`);
  log(`Max pages per state: ${maxPagesPerState}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));
  log('');

  let statesProcessed = 0;
  let totalNew = 0;
  let totalDupes = 0;
  let errors = 0;
  let emptyStates = 0;

  for (const stateInfo of remaining) {
    statesProcessed++;
    log(`[${statesProcessed}/${remaining.length}] ${stateInfo.name} (${stateInfo.code})`);

    try {
      const stateLeads = await scrapeState(stateInfo, maxPagesPerState);

      let newCount = 0;
      let dupeCount = 0;

      for (const lead of stateLeads) {
        const key = dedupKey(lead.first_name, lead.last_name, lead.city);
        // Skip empty names
        if (!lead.first_name && !lead.last_name) {
          dupeCount++;
          continue;
        }
        if (seenKeys.has(key)) {
          dupeCount++;
          continue;
        }
        seenKeys.add(key);
        allLeads.push(lead);
        newCount++;
      }

      totalNew += newCount;
      totalDupes += dupeCount;

      if (stateLeads.length === 0) emptyStates++;

      log(`  => ${stateLeads.length} found, ${newCount} new, ${dupeCount} skipped (dupes/empty)`);

      completedStates.push(stateInfo.code);

      // Save progress after each state
      saveProgress(completedStates, allLeads);
      writeCSV(allLeads, OUT_FILE);

    } catch (err) {
      log(`  ERROR: ${err.message}`);
      errors++;
    }

    // Delay between states
    if (statesProcessed < remaining.length) {
      await randomDelay();
    }
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  saveProgress(completedStates, allLeads);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withDomain = allLeads.filter(l => l.domain).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  log('');
  log('='.repeat(60));
  log('SCRAPE COMPLETE');
  log('='.repeat(60));
  log(`States processed: ${statesProcessed} (${errors} errors, ${emptyStates} empty)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`  With phone:   ${withPhone} (${allLeads.length ? Math.round(withPhone / allLeads.length * 100) : 0}%)`);
  log(`  With website:  ${withWebsite} (${allLeads.length ? Math.round(withWebsite / allLeads.length * 100) : 0}%)`);
  log(`  With domain:   ${withDomain}`);
  log(`  With firm:     ${withFirm}`);
  log(`  Unique states: ${uniqueStates}`);
  log(`  Unique cities: ${uniqueCities}`);
  log(`Duplicates skipped: ${totalDupes}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));

  // Clean up progress file on successful complete run (all states, no errors)
  if (!isTest && errors === 0 && remaining.length === statesToScrape.length && statesToScrape.length === STATES.length) {
    try { fs.unlinkSync(PROGRESS_FILE); log('Progress file cleaned up (complete run).'); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
