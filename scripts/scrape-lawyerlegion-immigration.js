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

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
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

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllStates = args.includes('--all-states');
  const statesArgIdx = args.indexOf('--states');
  const specificStates = statesArgIdx >= 0 && args[statesArgIdx + 1]
    ? args[statesArgIdx + 1].toUpperCase().split(',')
    : null;

  if (!isTest && !isAllStates && !specificStates) {
    console.log('Lawyer Legion Immigration Lawyers Scraper');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --test              # Test: 3 states, 2 pages each');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --states CA,TX,NY   # Specific states');
    console.log('  node scripts/scrape-lawyerlegion-immigration.js --all-states        # All 50 states + DC');
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
