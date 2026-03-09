#!/usr/bin/env node
/**
 * Lawfully Immigration Lawyers Scraper
 *
 * Scrapes https://www.lawfully.com/lawyers/ using the Next.js data API.
 *
 * Strategy:
 *   1. Fetch the listing page via Next.js _next/data API to get all lawyer slugs
 *   2. Also fetch each available state individually to catch state-only lawyers
 *   3. For each unique lawyer slug, fetch the detailed profile via Next.js data API
 *   4. Extract: name, phone, website, firm, address, practice areas
 *   5. Dedup by urlSlug
 *   6. Output CSV
 *
 * Data fields from profiles:
 *   - lawyerName, firmName, officeNumber, website
 *   - address (city, state, zipcode)
 *   - schools (education), licences (bar admissions)
 *   - specialityTypes (practice areas)
 *   - careers (job history)
 *
 * Output: output/us-immigration-lawyers-lawfully.csv
 *
 * Usage:
 *   node scripts/scrape-lawfully-immigration.js              # Full run (all lawyers)
 *   node scripts/scrape-lawfully-immigration.js --test        # Test mode (first 5 lawyers)
 *   node scripts/scrape-lawfully-immigration.js --no-resume   # Ignore existing CSV, start fresh
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const BASE_URL = 'https://www.lawfully.com';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-lawfully.csv');
const PROGRESS_FILE = OUTPUT_FILE.replace('.csv', '-progress.json');

const DELAY_MIN = 2000;
const DELAY_MAX = 3000;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

// Next.js build ID — needed for _next/data API. Will be auto-detected from the site.
let BUILD_ID = null;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// All known states on Lawfully (discovered from API)
const KNOWN_STATES = [
  'CA', 'FL', 'GA', 'IL', 'MD', 'MA', 'MI', 'NJ', 'NY', 'NC', 'OH', 'TX', 'VT',
];

// User agents for rotation
const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

// State abbreviation to full name (for URL slugs)
const STATE_ABBR_TO_SLUG = {
  AL: 'alabama', AK: 'alaska', AZ: 'arizona', AR: 'arkansas',
  CA: 'california', CO: 'colorado', CT: 'connecticut', DE: 'delaware',
  DC: 'district-of-columbia', FL: 'florida', GA: 'georgia', HI: 'hawaii',
  ID: 'idaho', IL: 'illinois', IN: 'indiana', IA: 'iowa',
  KS: 'kansas', KY: 'kentucky', LA: 'louisiana', ME: 'maine',
  MD: 'maryland', MA: 'massachusetts', MI: 'michigan', MN: 'minnesota',
  MS: 'mississippi', MO: 'missouri', MT: 'montana', NE: 'nebraska',
  NV: 'nevada', NH: 'new-hampshire', NJ: 'new-jersey', NM: 'new-mexico',
  NY: 'new-york', NC: 'north-carolina', ND: 'north-dakota', OH: 'ohio',
  OK: 'oklahoma', OR: 'oregon', PA: 'pennsylvania', RI: 'rhode-island',
  SC: 'south-carolina', SD: 'south-dakota', TN: 'tennessee', TX: 'texas',
  UT: 'utah', VT: 'vermont', VA: 'virginia', WA: 'washington',
  WV: 'west-virginia', WI: 'wisconsin', WY: 'wyoming',
};

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

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?)\s+/i, '')
    // Remove suffixes only at word boundaries (not inside words like HAJDARPASIC)
    .replace(/,?\s*\b(esq\.?|esquire|j\.?d\.?|attorney|lawyer|ph\.?d\.?|m\.?d\.?|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?)\b/gi, '')
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

function titleCase(str) {
  if (!str) return '';
  return str.replace(/\w\S*/g, txt =>
    txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
  );
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    return new URL(u).hostname.replace(/^www\./, '');
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

// ── HTTP Client ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const ua = getRandomUA();
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      headers: {
        'User-Agent': ua,
        'Accept': 'application/json, text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
        'Referer': 'https://www.lawfully.com/',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(options, (res) => {
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

// ── Build ID Detection ──────────────────────────────────────────────────────

/**
 * Auto-detect the Next.js build ID from the Lawfully homepage.
 * The build ID changes on every deployment, so we must discover it dynamically.
 */
async function detectBuildId() {
  log('Detecting Next.js build ID...');
  const res = await httpGet(`${BASE_URL}/lawyers/states/all`);
  if (res.statusCode !== 200) {
    throw new Error(`Failed to load listing page: HTTP ${res.statusCode}`);
  }

  // Look for buildId in __NEXT_DATA__
  const match = res.body.match(/"buildId"\s*:\s*"([^"]+)"/);
  if (match) {
    BUILD_ID = match[1];
    log(`Build ID detected: ${BUILD_ID}`);
    return BUILD_ID;
  }

  // Fallback: look for _next/static/{buildId} references
  const staticMatch = res.body.match(/_next\/static\/([a-zA-Z0-9_-]+)\//);
  if (staticMatch) {
    BUILD_ID = staticMatch[1];
    log(`Build ID detected (from static): ${BUILD_ID}`);
    return BUILD_ID;
  }

  throw new Error('Could not detect Next.js build ID from page HTML');
}

// ── Next.js Data API ────────────────────────────────────────────────────────

/**
 * Fetch data from the Next.js _next/data API.
 * Returns parsed JSON or null on failure.
 */
async function fetchNextData(pagePath) {
  const url = `${BASE_URL}/_next/data/${BUILD_ID}${pagePath}.json`;
  try {
    const res = await httpGet(url);

    if (res.statusCode === 404) {
      // Build ID may have changed — try to re-detect
      log(`  404 on data API — build ID may have changed`);
      return null;
    }

    if (res.statusCode !== 200) {
      log(`  HTTP ${res.statusCode} on ${pagePath}`);
      return null;
    }

    return JSON.parse(res.body);
  } catch (err) {
    log(`  ERROR fetching ${pagePath}: ${err.message}`);
    return null;
  }
}

// ── Listing Fetcher ─────────────────────────────────────────────────────────

/**
 * Fetch all lawyer slugs from the listing pages.
 * Checks both /lawyers/states/all and each individual state page.
 */
async function fetchAllLawyerSlugs() {
  const slugMap = new Map(); // slug -> basic listing data

  // 1) Fetch the "all" listing
  log('Fetching /lawyers/states/all ...');
  const allData = await fetchNextData('/lawyers/states/all');
  if (allData && allData.pageProps && allData.pageProps.listLawyer) {
    const lawyers = allData.pageProps.listLawyer;
    log(`  Found ${lawyers.length} lawyers on "all" page`);
    for (const l of lawyers) {
      if (l.urlSlug) {
        slugMap.set(l.urlSlug, {
          lawyerName: l.lawyerName,
          firmName: l.firmName || '',
          city: (l.address && l.address.city) || '',
          state: (l.address && l.address.state) || '',
          urlSlug: l.urlSlug,
        });
      }
    }

    // Discover available states from the API response
    // States are objects like { value: "CA", description: "California", key: "CA", label: "CA" }
    if (allData.pageProps.availableStates) {
      const apiStates = allData.pageProps.availableStates;
      const stateCodes = apiStates.map(s => typeof s === 'string' ? s : (s.value || s.key || ''));
      log(`  Available states from API: ${stateCodes.join(', ')}`);
      // Add any states we didn't know about
      for (const s of stateCodes) {
        if (s && !KNOWN_STATES.includes(s)) {
          KNOWN_STATES.push(s);
        }
      }
    }
  } else {
    log('  WARNING: Could not fetch "all" listing');
  }

  await randomDelay();

  // 2) Fetch each state individually to catch state-only lawyers
  for (const stateCode of KNOWN_STATES) {
    if (!stateCode || typeof stateCode !== 'string') continue;

    log(`Fetching /lawyers/states/${stateCode} ...`);
    const stateData = await fetchNextData(`/lawyers/states/${stateCode}`);
    if (stateData && stateData.pageProps && stateData.pageProps.listLawyer) {
      const lawyers = stateData.pageProps.listLawyer;
      let newCount = 0;
      for (const l of lawyers) {
        if (l.urlSlug && !slugMap.has(l.urlSlug)) {
          slugMap.set(l.urlSlug, {
            lawyerName: l.lawyerName,
            firmName: l.firmName || '',
            city: (l.address && l.address.city) || '',
            state: (l.address && l.address.state) || '',
            urlSlug: l.urlSlug,
          });
          newCount++;
        }
      }
      log(`  ${stateCode}: ${lawyers.length} lawyers, ${newCount} new`);
    } else {
      log(`  ${stateCode}: no data`);
    }

    await randomDelay();
  }

  return slugMap;
}

// ── Profile Fetcher ─────────────────────────────────────────────────────────

/**
 * Fetch detailed profile data for a single lawyer.
 * Returns a lead object or null on failure.
 */
async function fetchLawyerProfile(urlSlug) {
  const data = await fetchNextData(`/lawyers/${urlSlug}`);
  if (!data || !data.pageProps) {
    return null;
  }

  // Profile data is nested under pageProps.lawyer
  const pp = data.pageProps.lawyer || data.pageProps;
  if (!pp.lawyerName) {
    log(`  WARNING: No lawyerName found in profile data for ${urlSlug}`);
    return null;
  }

  // Extract name
  const fullName = (pp.lawyerName || '').trim();
  const { first_name, last_name } = parseName(fullName);

  // Fix name casing if ALL CAPS
  const fixCase = (name) => {
    if (!name) return '';
    if (name === name.toUpperCase() && name.length > 1) {
      return titleCase(name);
    }
    return name;
  };

  // Extract address
  const address = pp.address || {};
  let city = (address.city || '').trim();
  const state = (address.state || '').trim();
  // Fix ALL CAPS city names (e.g., "SKOKIE" → "Skokie", "SAN FRANCISCO" → "San Francisco")
  if (city && city === city.toUpperCase() && city.length > 1) {
    city = titleCase(city);
  }

  // Extract phone
  const phone = cleanPhone(pp.officeNumber || '');

  // Extract website
  let website = (pp.website || '').trim();
  if (website && !website.startsWith('http')) {
    website = 'https://' + website;
  }

  // Extract title from careers (most recent)
  let title = '';
  if (pp.careers && Array.isArray(pp.careers) && pp.careers.length > 0) {
    // Sort by start date descending to get most recent
    const sorted = [...pp.careers].sort((a, b) => {
      const dateA = a.startAt || '';
      const dateB = b.startAt || '';
      return dateB.localeCompare(dateA);
    });
    title = (sorted[0].title || '').trim();
  }

  // Profile URL
  const profileUrl = `${BASE_URL}/lawyers/${urlSlug}/`;

  return {
    first_name: fixCase(first_name),
    last_name: fixCase(last_name),
    firm_name: (pp.firmName || '').trim(),
    title,
    email: '',  // Not available on Lawfully profiles
    phone,
    website,
    domain: extractDomain(website),
    city,
    state,
    country: 'US',
    niche: 'immigration',
    source: 'lawfully',
    profile_url: profileUrl,
  };
}

// ── Resume Support ──────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      return new Set(data.slugsCompleted || []);
    }
  } catch {}
  return new Set();
}

function saveProgress(slugsCompleted) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    slugsCompleted: [...slugsCompleted],
    updatedAt: new Date().toISOString(),
  }, null, 2));
}

function loadExistingCSV(csvPath) {
  const result = { leads: [], seenSlugs: new Set() };
  if (!fs.existsSync(csvPath)) return result;

  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length <= 1) return result;

  const headers = lines[0].split(',');

  for (let i = 1; i < lines.length; i++) {
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

    // Extract slug from profile_url
    if (lead.profile_url) {
      const slugMatch = lead.profile_url.match(/\/lawyers\/([^/]+)\/?$/);
      if (slugMatch) {
        result.seenSlugs.add(slugMatch[1]);
      }
    }
    result.leads.push(lead);
  }

  return result;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const noResume = args.includes('--no-resume');

  log('========================================');
  log('  Lawfully Immigration Lawyers Scraper');
  log('========================================');
  log(`Mode: ${isTest ? 'TEST (first 5 lawyers)' : 'FULL'}`);
  log(`Resume: ${noResume ? 'disabled' : 'enabled'}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Step 1: Detect build ID
  await detectBuildId();

  // Step 2: Fetch all lawyer slugs from listing pages
  log('\n--- Phase 1: Discover all lawyers ---');
  const slugMap = await fetchAllLawyerSlugs();
  const allSlugs = [...slugMap.keys()];
  log(`\nTotal unique lawyers discovered: ${allSlugs.length}`);

  if (allSlugs.length === 0) {
    log('ERROR: No lawyers found. Build ID may have changed or site structure updated.');
    process.exit(1);
  }

  // Limit in test mode
  const slugsToProcess = isTest ? allSlugs.slice(0, 5) : allSlugs;

  // Resume support
  let allLeads = [];
  let slugsCompleted = new Set();

  if (!noResume) {
    const existing = loadExistingCSV(OUTPUT_FILE);
    slugsCompleted = loadProgress();
    if (existing.leads.length > 0) {
      allLeads = existing.leads;
      log(`RESUME: loaded ${allLeads.length} existing leads from CSV`);
      log(`RESUME: ${slugsCompleted.size} profiles already fetched`);
    }
  } else {
    if (fs.existsSync(PROGRESS_FILE)) fs.unlinkSync(PROGRESS_FILE);
  }

  // Step 3: Fetch detailed profiles
  log(`\n--- Phase 2: Fetch ${slugsToProcess.length} lawyer profiles ---`);
  let fetchedCount = 0;
  let skippedCount = 0;
  let errorCount = 0;

  for (let i = 0; i < slugsToProcess.length; i++) {
    const slug = slugsToProcess[i];

    // Skip if already fetched (resume)
    if (!noResume && slugsCompleted.has(slug)) {
      skippedCount++;
      continue;
    }

    log(`[${i + 1}/${slugsToProcess.length}] Fetching profile: ${slug}`);
    await randomDelay();

    const lead = await fetchLawyerProfile(slug);
    if (lead) {
      // Check if this lead already exists (by profile URL)
      const existingIdx = allLeads.findIndex(l => l.profile_url === lead.profile_url);
      if (existingIdx >= 0) {
        allLeads[existingIdx] = lead; // Update with fresh data
      } else {
        allLeads.push(lead);
      }
      fetchedCount++;

      const info = [
        lead.first_name && lead.last_name ? `${lead.first_name} ${lead.last_name}` : '?',
        lead.firm_name || '',
        lead.city && lead.state ? `${lead.city}, ${lead.state}` : '',
        lead.phone ? 'phone' : '',
        lead.website ? 'website' : '',
      ].filter(Boolean).join(' | ');
      log(`  OK: ${info}`);
    } else {
      errorCount++;
      log(`  FAILED: could not fetch profile for ${slug}`);

      // If we get too many failures, the build ID may have changed
      if (errorCount >= 5 && fetchedCount === 0) {
        log('\nToo many consecutive failures. Build ID may have changed.');
        log('Attempting to re-detect build ID...');
        try {
          await detectBuildId();
          errorCount = 0; // Reset error count after re-detection
        } catch (err) {
          log(`Failed to re-detect build ID: ${err.message}`);
          break;
        }
      }
    }

    // Mark as completed
    slugsCompleted.add(slug);
    saveProgress(slugsCompleted);

    // Incremental save every 10 profiles
    if ((fetchedCount + skippedCount) % 10 === 0 || i === slugsToProcess.length - 1) {
      writeCSV(allLeads, OUTPUT_FILE);
    }
  }

  // Final save
  writeCSV(allLeads, OUTPUT_FILE);

  // Clean up progress file on successful completion
  if (fetchedCount > 0 && errorCount === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const withTitle = allLeads.filter(l => l.title).length;

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads in CSV: ${allLeads.length}`);
  log(`Profiles fetched this run: ${fetchedCount}`);
  log(`Profiles skipped (resumed): ${skippedCount}`);
  log(`Errors: ${errorCount}`);
  log('---');
  log(`With full name: ${withName}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With firm name: ${withFirm}`);
  log(`With title: ${withTitle}`);
  log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
