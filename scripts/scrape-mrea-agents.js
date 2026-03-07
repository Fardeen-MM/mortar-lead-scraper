#!/usr/bin/env node
/**
 * Scrape ALL licensed real estate agents from Manitoba, Canada.
 *
 * Source: https://rols.mb.ca/aspx/pubinquiry  (Manitoba Securities Commission registry)
 * Method: ASP.NET WebForms — POST with ViewState tokens.
 *
 * Strategy:
 *   Phase 1 — "%" wildcard last-name search returns ALL agents in one response.
 *             Each row: full name, brokerage, designation.
 *   Phase 2 — City-based brokerage search for known Manitoba cities.
 *             Builds a brokerage->city lookup so agents inherit their brokerage's city.
 *
 * Fields extracted: first_name, last_name, firm_name, title (designation),
 *                   city (from brokerage-city mapping)
 * Note: The ROLS registry does NOT expose email, phone, or website.
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, fs, path).
 *
 * Features:
 *   - Single-request wildcard fetch for all agents
 *   - City mapping via brokerage-to-city lookup
 *   - Dedup by full name + brokerage
 *   - Rate limiting (500ms between requests)
 *   - Test mode (--test): fetches only "smith" + 3 cities
 *
 * Usage:
 *   node scripts/scrape-mrea-agents.js
 *   node scripts/scrape-mrea-agents.js --test
 *   node scripts/scrape-mrea-agents.js --delay=500
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ── CLI args ──────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS  = parseInt(args.delay) || 500;
const TEST_MODE = Boolean(args.test);
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'mrea-real-estate-agents.csv');

const BASE_URL = 'https://rols.mb.ca';
const SEARCH_PATH = '/aspx/pubinquiry';
const SEARCH_URL = BASE_URL + SEARCH_PATH;

// Known Manitoba cities/towns for brokerage-city mapping
const MANITOBA_CITIES = [
  'Winnipeg', 'Brandon', 'Steinbach', 'Thompson', 'Portage la Prairie',
  'Winkler', 'Selkirk', 'Morden', 'Dauphin', 'The Pas',
  'Flin Flon', 'Stonewall', 'Gimli', 'Neepawa', 'Virden',
  'Carman', 'Altona', 'Swan River', 'Beausejour', 'Niverville',
  'Morris', 'Killarney', 'Roblin', 'Minnedosa', 'Russell',
  'Lorette', 'Oakbank', 'Headingley', 'St. Andrews', 'East St. Paul',
  'West St. Paul', 'Springfield', 'Ritchot', 'Tache', 'La Salle',
  'St. Clements', 'Hanover', 'Ste. Anne', 'Lac du Bonnet', 'Pinawa',
  'Souris', 'Deloraine', 'Boissevain', 'Melita', 'Rivers',
  'Oak Bluff', 'Birds Hill', 'St. Vital', 'Transcona', 'Charleswood',
];

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// ── CSV columns ───────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source',
];

const CSV_HEADER = CSV_COLUMNS.join(',');

function escCsv(val) {
  if (!val) return '';
  const s = String(val).replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

function leadToCsvRow(lead) {
  return CSV_COLUMNS.map(col => escCsv(lead[col])).join(',');
}

// ── Helpers ───────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function titleCase(str) {
  if (!str) return '';
  return str
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map(w => {
      if (/^(o\/a|of|the|and|in|at|for|de|du|la|le|des|von|van)$/i.test(w)) {
        return w.toLowerCase();
      }
      if (/^(RE\/MAX|BGIS|RBC|BMO|TD|CIBC|MLS|LLC|LTD|INC|CORP)$/i.test(w)) {
        return w.toUpperCase();
      }
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(' ');
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  // Last word is last name, everything else is first name(s)
  const last_name = parts[parts.length - 1];
  const first_name = parts.slice(0, -1).join(' ');
  return { first_name: titleCase(first_name), last_name: titleCase(last_name) };
}

// ── HTTPS helpers ─────────────────────────────────────────────────────────
function httpsGet(urlStr, cookies) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    };
    if (cookies) options.headers['Cookie'] = cookies;

    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]);
        resolve({ status: res.statusCode, body: data, setCookies });
      });
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error('GET timeout')); });
    req.end();
  });
}

function httpsPost(urlStr, postBody, cookies) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'User-Agent': USER_AGENT,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postBody),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    };
    if (cookies) options.headers['Cookie'] = cookies;

    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]);
        resolve({ status: res.statusCode, body: data, setCookies });
      });
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error('POST timeout')); });
    req.write(postBody);
    req.end();
  });
}

// ── ASP.NET form helpers ──────────────────────────────────────────────────
function extractTokens(html) {
  const vsMatch = html.match(/name="__VIEWSTATE"[^>]*value="([^"]*)"/);
  const vsgMatch = html.match(/name="__VIEWSTATEGENERATOR"[^>]*value="([^"]*)"/);
  const evMatch = html.match(/name="__EVENTVALIDATION"[^>]*value="([^"]*)"/);
  if (!vsMatch || !vsgMatch || !evMatch) return null;
  return {
    __VIEWSTATE: vsMatch[1],
    __VIEWSTATEGENERATOR: vsgMatch[1],
    __EVENTVALIDATION: evMatch[1],
  };
}

// ── Search by last name ───────────────────────────────────────────────────
async function searchByLastName(query) {
  // Step 1: GET the form to obtain fresh tokens
  const page = await httpsGet(SEARCH_URL);
  if (page.status !== 200) {
    throw new Error(`GET failed with status ${page.status}`);
  }
  const tokens = extractTokens(page.body);
  if (!tokens) throw new Error('Failed to extract ASP.NET tokens');

  // Step 2: POST the search
  const params = new URLSearchParams();
  params.set('__VIEWSTATE', tokens.__VIEWSTATE);
  params.set('__VIEWSTATEGENERATOR', tokens.__VIEWSTATEGENERATOR);
  params.set('__EVENTVALIDATION', tokens.__EVENTVALIDATION);
  params.set('txtLastName', query);
  params.set('txtBrokerage', '');
  params.set('txtCity', '');
  params.set('imbLastName.x', '5');
  params.set('imbLastName.y', '5');

  const result = await httpsPost(SEARCH_URL, params.toString());
  if (result.status !== 200) {
    throw new Error(`POST failed with status ${result.status}`);
  }
  return result.body;
}

// ── Search by city ────────────────────────────────────────────────────────
async function searchByCity(city) {
  // Step 1: GET the form
  const page = await httpsGet(SEARCH_URL);
  if (page.status !== 200) {
    throw new Error(`GET failed with status ${page.status}`);
  }
  const tokens = extractTokens(page.body);
  if (!tokens) throw new Error('Failed to extract ASP.NET tokens');

  // Step 2: POST the city search
  const params = new URLSearchParams();
  params.set('__VIEWSTATE', tokens.__VIEWSTATE);
  params.set('__VIEWSTATEGENERATOR', tokens.__VIEWSTATEGENERATOR);
  params.set('__EVENTVALIDATION', tokens.__EVENTVALIDATION);
  params.set('txtLastName', '');
  params.set('txtBrokerage', '');
  params.set('txtCity', city);
  params.set('imbCity.x', '5');
  params.set('imbCity.y', '5');

  const result = await httpsPost(SEARCH_URL, params.toString());
  if (result.status !== 200) {
    throw new Error(`POST failed with status ${result.status}`);
  }
  return result.body;
}

// ── Parse name search results ─────────────────────────────────────────────
function parseNameResults(html) {
  const agents = [];

  // Each result row has:
  //   DatagridRealEstate_lblMember_DisplayName_N  -> full name
  //   DatagridRealEstate_lnkBrokerage_DisplayName_N -> brokerage name (link)
  //   DatagridRealEstate_lblDesignation_Display_N -> designation/title
  const namePattern = /lblMember_DisplayName_(\d+)"[^>]*>([^<]+)</g;
  const brokeragePattern = /lnkBrokerage_DisplayName_(\d+)"[^>]*>([^<]+)</g;
  const designationPattern = /lblDesignation_Display_(\d+)"[^>]*>([^<]+)</g;

  const names = {};
  const brokerages = {};
  const designations = {};

  let m;
  while ((m = namePattern.exec(html)) !== null) {
    names[m[1]] = m[2].trim();
  }
  while ((m = brokeragePattern.exec(html)) !== null) {
    brokerages[m[1]] = m[2].trim();
  }
  while ((m = designationPattern.exec(html)) !== null) {
    designations[m[1]] = m[2].trim();
  }

  for (const idx of Object.keys(names)) {
    const fullName = names[idx];
    const brokerage = brokerages[idx] || '';
    const designation = designations[idx] || '';

    const { first_name, last_name } = splitName(fullName);

    // Clean up brokerage: extract "o/a" operating-as name if present
    let firmDisplay = brokerage;
    const oaMatch = brokerage.match(/o\/a\s+(.+)/i);
    if (oaMatch) {
      firmDisplay = oaMatch[1].trim();
    }

    agents.push({
      first_name,
      last_name,
      firm_name: firmDisplay,
      firm_name_full: brokerage,
      title: designation,
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: '',
      state: 'Manitoba',
      country: 'CA',
      niche: 'real estate agent',
      source: 'mrea',
    });
  }

  return agents;
}

// ── Parse city search results (brokerage -> city mapping) ─────────────────
function parseCityResults(html, cityName) {
  const mapping = {};

  // City search rows have:
  //   DatagridRealEstate_lnkBrokerage_DisplayName_N -> brokerage name
  //   DatagridRealEstate_lblBroker_DisplayName_N -> registrant/broker name
  //   DatagridRealEstate_lblCategory_Display_N -> category
  const brokeragePattern = /lnkBrokerage_DisplayName_(\d+)"[^>]*>([^<]+)</g;

  let m;
  while ((m = brokeragePattern.exec(html)) !== null) {
    const brokerageName = m[2].trim();
    // Normalize for matching: lowercase, no extra whitespace
    const key = brokerageName.toLowerCase().replace(/\s+/g, ' ');
    mapping[key] = cityName;
  }

  return mapping;
}

// ── Main ──────────────────────────────────────────────────────────────────
(async () => {
  console.log('=== MREA Manitoba Real Estate Agent Scraper ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  console.log(`Delay: ${DELAY_MS}ms`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log('');

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // ── Phase 1: Fetch all agents ──────────────────────────────────────────
  console.log('--- Phase 1: Fetching all agents ---');
  let allAgents;
  const searchQuery = TEST_MODE ? 'smith' : '%';
  const searchLabel = TEST_MODE ? '"smith"' : '"%" (all agents)';

  try {
    console.log(`Searching by last name: ${searchLabel} ...`);
    const html = await searchByLastName(searchQuery);
    allAgents = parseNameResults(html);
    console.log(`  Found ${allAgents.length} agents`);
  } catch (err) {
    console.error('Error fetching agents:', err.message);
    process.exit(1);
  }

  if (allAgents.length === 0) {
    console.log('No agents found. Exiting.');
    process.exit(0);
  }

  // ── Phase 2: Build brokerage -> city mapping ───────────────────────────
  console.log('\n--- Phase 2: Building brokerage-city mapping ---');
  const brokerageCityMap = {};
  const citiesToSearch = TEST_MODE
    ? ['Winnipeg', 'Brandon', 'Steinbach']
    : MANITOBA_CITIES;

  for (let i = 0; i < citiesToSearch.length; i++) {
    const city = citiesToSearch[i];
    try {
      process.stdout.write(`  [${i + 1}/${citiesToSearch.length}] ${city} ... `);
      const html = await searchByCity(city);
      const mapping = parseCityResults(html, city);
      const count = Object.keys(mapping).length;
      Object.assign(brokerageCityMap, mapping);
      console.log(`${count} brokerages`);
    } catch (err) {
      console.log(`ERROR: ${err.message}`);
    }
    if (i < citiesToSearch.length - 1) await sleep(DELAY_MS);
  }

  console.log(`\nTotal brokerage-city mappings: ${Object.keys(brokerageCityMap).length}`);

  // ── Phase 3: Assign cities and deduplicate ─────────────────────────────
  console.log('\n--- Phase 3: Assigning cities and deduplicating ---');

  // Assign city from brokerage mapping
  let citiesAssigned = 0;
  for (const agent of allAgents) {
    const key = (agent.firm_name_full || '').toLowerCase().replace(/\s+/g, ' ');
    if (brokerageCityMap[key]) {
      agent.city = brokerageCityMap[key];
      citiesAssigned++;
    }
  }
  console.log(`  Cities assigned: ${citiesAssigned}/${allAgents.length}`);

  // Dedup by full name + brokerage (case-insensitive)
  const seen = new Set();
  const deduped = [];
  for (const agent of allAgents) {
    const dedupKey = [
      agent.first_name.toLowerCase(),
      agent.last_name.toLowerCase(),
      agent.firm_name.toLowerCase(),
    ].join('|');

    if (seen.has(dedupKey)) continue;
    seen.add(dedupKey);
    deduped.push(agent);
  }

  const dupes = allAgents.length - deduped.length;
  console.log(`  Deduped: ${allAgents.length} -> ${deduped.length} (${dupes} duplicates removed)`);

  // ── Phase 4: Write CSV ─────────────────────────────────────────────────
  console.log('\n--- Phase 4: Writing CSV ---');

  const rows = [CSV_HEADER];
  for (const agent of deduped) {
    rows.push(leadToCsvRow(agent));
  }
  fs.writeFileSync(OUTPUT_FILE, rows.join('\n') + '\n', 'utf-8');
  console.log(`  Wrote ${deduped.length} leads to ${OUTPUT_FILE}`);

  // ── Stats ──────────────────────────────────────────────────────────────
  console.log('\n=== Results ===');
  console.log(`  Total agents scraped: ${allAgents.length}`);
  console.log(`  After dedup: ${deduped.length}`);
  console.log(`  With city: ${deduped.filter(a => a.city).length}`);

  // Designation breakdown
  const designations = {};
  for (const a of deduped) {
    const d = a.title || 'Unknown';
    designations[d] = (designations[d] || 0) + 1;
  }
  console.log('  Designation breakdown:');
  for (const [d, count] of Object.entries(designations).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${d}: ${count}`);
  }

  // City breakdown (top 10)
  const cities = {};
  for (const a of deduped) {
    const c = a.city || '(unknown)';
    cities[c] = (cities[c] || 0) + 1;
  }
  console.log('  Top cities:');
  const sortedCities = Object.entries(cities).sort((a, b) => b[1] - a[1]).slice(0, 10);
  for (const [c, count] of sortedCities) {
    console.log(`    ${c}: ${count}`);
  }

  console.log('\nDone.');
})();
