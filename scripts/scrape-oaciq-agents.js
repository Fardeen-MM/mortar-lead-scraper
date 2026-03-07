#!/usr/bin/env node
/**
 * Scrape ALL registered real estate brokers from the OACIQ registry.
 *
 * Source: registre.oaciq.com (Organisme d'autoreglementation du courtage immobilier du Quebec)
 * Expected: ~17,000+ brokers across 17 Quebec regions
 *
 * Strategy:
 *   - GET /en/find-broker to get Rails CSRF token + session cookie
 *   - POST search form with each of 17 Quebec regions
 *   - Paginate through all results (10 per page) via GET
 *   - Parse HTML table rows with regex (zero npm deps)
 *   - Deduplicate by broker licence ID (hex identifier)
 *   - Detail pages require Cloudflare Turnstile CAPTCHA — skipped
 *
 * HTML structure (confirmed 2026-03-06):
 *   <table id="find-brokers-result">
 *     <tbody>
 *       <tr class="leading-5 border-b border-blue-light">
 *         <td class="h-14">
 *           <span class="fa fa-user"></span>            <!-- fa-user = broker, fa-home = agency -->
 *           <a class="alert-registre" href="https://registre.oaciq.com/en/find-broker/44562AED">Alexandre Cote</a>
 *         </td>
 *         <td>
 *           <a class="alert-registre" href="...">RE/MAX ELITE</a>
 *         </td>
 *         <td class="text-right">
 *           <span class="tag info whitespace-nowrap">Drummondville</span>
 *         </td>
 *       </tr>
 *
 * Pagination: GET links like /en/find-broker?find_broker[region]=910&page=2
 *   Last page link: <a ... href="...page=16">&gt;&gt;</a>
 *
 * Features:
 *   - Zero npm dependencies (Node.js https + regex parsing)
 *   - Auto-saves progress CSV every 500 leads
 *   - Resume support (reads existing progress file)
 *   - Rate limiting (configurable, default 500ms)
 *   - Session refresh on auth failures (422/403)
 *   - Skips agencies (fa-home), keeps only individual brokers (fa-user)
 *
 * Usage:
 *   node scripts/scrape-oaciq-agents.js
 *   node scripts/scrape-oaciq-agents.js --region=920       # Montreal only
 *   node scripts/scrape-oaciq-agents.js --resume            # Continue from progress
 *   node scripts/scrape-oaciq-agents.js --delay=800         # Custom delay (ms)
 *   node scripts/scrape-oaciq-agents.js --test              # First region, 3 pages only
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 500;
const TEST_MODE = !!args.test;
const MAX_TEST_PAGES = 3;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'oaciq-real-estate-agents-progress.csv');
const FINAL_FILE = path.join(OUTPUT_DIR, 'oaciq-real-estate-agents.csv');
const BASE_URL = 'https://registre.oaciq.com';

// All 17 Quebec regions with OACIQ form values
const REGIONS = [
  { id: '907', name: 'Bas Saint-Laurent' },
  { id: '923', name: 'Saguenay-Lac-Saint-Jean' },
  { id: '909', name: 'Capitale-Nationale' },
  { id: '918', name: 'Mauricie' },
  { id: '913', name: 'Estrie' },
  { id: '920', name: 'Montreal' },
  { id: '922', name: 'Outaouais' },
  { id: '908', name: 'Abitibi-Temiscamingue' },
  { id: '912', name: 'Cote-Nord' },
  { id: '921', name: 'Nord-du-Quebec' },
  { id: '914', name: 'Gaspesie-iles-de-la-Madeleine' },
  { id: '911', name: 'Chaudiere-Appalaches' },
  { id: '917', name: 'Laval' },
  { id: '915', name: 'Lanaudiere' },
  { id: '916', name: 'Laurentides' },
  { id: '919', name: 'Monteregie' },
  { id: '910', name: 'Centre-du-Quebec' },
];

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'licence_number', 'region', 'profile_url',
];

// ---------------------------------------------------------------------------
// HTTP helper — uses only Node.js built-in https module
// ---------------------------------------------------------------------------

function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || 443,
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(options.headers || {}),
      },
    };

    const req = https.request(reqOptions, (res) => {
      const setCookies = res.headers['set-cookie'] || [];
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({
        status: res.statusCode,
        headers: res.headers,
        setCookies,
        body: data,
      }));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });

    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Session management (Rails CSRF + session cookie)
// ---------------------------------------------------------------------------

let sessionCookie = '';
let authToken = '';

async function refreshSession() {
  const res = await httpRequest(`${BASE_URL}/en/find-broker`, {
    method: 'GET',
  });

  // Extract session cookie (e.g., _oaciq_session_2026=...)
  for (const cookie of res.setCookies) {
    const match = cookie.match(/(_oaciq_session_\d+)=([^;]+)/);
    if (match) {
      sessionCookie = `${match[1]}=${match[2]}`;
    }
  }

  // Extract authenticity_token from hidden form field
  // <input type="hidden" name="authenticity_token" value="..." autocomplete="off" />
  const authMatch = res.body.match(/name="authenticity_token"\s+value="([^"]+)"/);
  if (authMatch) authToken = authMatch[1];

  if (!sessionCookie || !authToken) {
    throw new Error('Failed to obtain session: cookie=' + !!sessionCookie + ' auth=' + !!authToken);
  }

  return { sessionCookie, authToken };
}

// ---------------------------------------------------------------------------
// Search & pagination
// ---------------------------------------------------------------------------

async function searchRegion(regionId) {
  const body = [
    `authenticity_token=${encodeURIComponent(authToken)}`,
    'find_broker%5Bname%5D=',
    'find_broker%5Blicence_number%5D=',
    'find_broker%5Binclude_revoked_brokers%5D=0',
    'find_broker%5Barea_of_practice%5D=',
    'find_broker%5Bagency_name%5D=',
    `find_broker%5Bregion%5D=${regionId}`,
    'find_broker%5Bcity%5D=',
    'commit=Search',
  ].join('&');

  const res = await httpRequest(`${BASE_URL}/en/find-broker`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': sessionCookie,
      'Referer': `${BASE_URL}/en/find-broker`,
    },
    body,
  });

  // Update session cookie if server issued a new one
  for (const cookie of res.setCookies) {
    const match = cookie.match(/(_oaciq_session_\d+)=([^;]+)/);
    if (match) {
      sessionCookie = `${match[1]}=${match[2]}`;
    }
  }

  // Update authenticity_token from the response page
  const newAuth = res.body.match(/name="authenticity_token"\s+value="([^"]+)"/);
  if (newAuth) authToken = newAuth[1];

  return res;
}

async function fetchPage(regionId, page) {
  const params = [
    'find_broker%5Bagency_name%5D=',
    'find_broker%5Barea_of_practice%5D=',
    'find_broker%5Bcity%5D=',
    'find_broker%5Binclude_revoked_brokers%5D=0',
    'find_broker%5Blicence_number%5D=',
    'find_broker%5Bname%5D=',
    `find_broker%5Bregion%5D=${regionId}`,
    `page=${page}`,
  ].join('&');

  const res = await httpRequest(`${BASE_URL}/en/find-broker?${params}`, {
    method: 'GET',
    headers: {
      'Cookie': sessionCookie,
      'Referer': `${BASE_URL}/en/find-broker`,
    },
  });

  // Update session cookie if server issued a new one
  for (const cookie of res.setCookies) {
    const match = cookie.match(/(_oaciq_session_\d+)=([^;]+)/);
    if (match) {
      sessionCookie = `${match[1]}=${match[2]}`;
    }
  }

  return res;
}

// ---------------------------------------------------------------------------
// HTML parsing — zero dependencies (regex-based)
// ---------------------------------------------------------------------------

/**
 * Parse broker results from the HTML table.
 *
 * Each result row looks like:
 *   <tr class="leading-5 border-b border-blue-light">
 *     <td class="h-14">
 *       <span class="fa fa-user"></span>       <!-- or fa-home for agencies -->
 *       <a class="alert-registre" href="https://registre.oaciq.com/en/find-broker/44562AED">Name</a>
 *     </td>
 *     <td>
 *       <a class="alert-registre" href="...">Agency Name</a>
 *     </td>
 *     <td class="text-right">
 *       <span class="tag info whitespace-nowrap">City</span>
 *     </td>
 *   </tr>
 */
function parseResultsPage(html) {
  const leads = [];

  // Extract each table row
  const rowRegex = /<tr class="leading-5 border-b border-blue-light">([\s\S]*?)<\/tr>/g;
  let rowMatch;

  while ((rowMatch = rowRegex.exec(html)) !== null) {
    const rowHtml = rowMatch[1];

    // Check icon type — skip agencies (fa-home), keep brokers (fa-user)
    const isAgency = rowHtml.includes('fa-home') || rowHtml.includes('fa-building');
    if (isAgency) continue;

    // Extract all <td> contents
    const tdRegex = /<td[^>]*>([\s\S]*?)<\/td>/g;
    const cells = [];
    let tdMatch;
    while ((tdMatch = tdRegex.exec(rowHtml)) !== null) {
      cells.push(tdMatch[1]);
    }
    if (cells.length < 3) continue;

    // Cell 0: broker name + link
    const nameLinkMatch = cells[0].match(/<a[^>]*href="([^"]*)"[^>]*>([^<]*)<\/a>/);
    if (!nameLinkMatch) continue;

    const profileUrl = nameLinkMatch[1].trim();
    const fullName = decodeHtmlEntities(nameLinkMatch[2].trim());
    const brokerId = profileUrl.split('/').pop() || '';

    if (!fullName) continue;

    // Cell 1: agency name
    const agencyMatch = cells[1].match(/<a[^>]*>([^<]*)<\/a>/);
    const agencyName = agencyMatch ? decodeHtmlEntities(agencyMatch[1].trim()) : '';

    // Cell 2: city
    const cityMatch = cells[2].match(/<span[^>]*>([^<]*)<\/span>/);
    const city = cityMatch ? decodeHtmlEntities(cityMatch[1].trim()) : '';

    // Parse name into first/last
    const { firstName, lastName } = parseName(fullName);

    leads.push({
      first_name: firstName,
      last_name: lastName,
      firm_name: agencyName,
      title: 'Real Estate Broker',
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: city,
      state: 'Quebec',
      country: 'CA',
      niche: 'real estate agent',
      source: 'oaciq',
      licence_number: brokerId,
      region: '',  // filled by caller
      profile_url: profileUrl,
    });
  }

  return leads;
}

/**
 * Find the last page number from pagination links.
 *
 * Pattern: <a ... href="...page=16">&gt;&gt;</a>   (last page link shows >>)
 * Also checks all page=N links and returns the highest.
 */
function getLastPage(html) {
  let maxPage = 1;
  const pageRegex = /page=(\d+)/g;
  let match;

  while ((match = pageRegex.exec(html)) !== null) {
    const p = parseInt(match[1]);
    if (p > maxPage) maxPage = p;
  }

  return maxPage;
}

function parseName(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 0) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };

  // OACIQ names appear as "First Last" or "First Middle Last"
  const firstName = parts[0];
  const lastName = parts.slice(1).join(' ');
  return { firstName, lastName };
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&eacute;/g, 'e')
    .replace(/&egrave;/g, 'e')
    .replace(/&agrave;/g, 'a')
    .replace(/&ocirc;/g, 'o')
    .replace(/&ccedil;/g, 'c')
    .replace(/&icirc;/g, 'i')
    .replace(/&ucirc;/g, 'u')
    .replace(/&Eacute;/g, 'E')
    .replace(/&Egrave;/g, 'E')
    .replace(/&icirc;/g, 'i')
    .replace(/&nbsp;/g, ' ');
}

// ---------------------------------------------------------------------------
// CSV handling
// ---------------------------------------------------------------------------

function escapeCSV(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n') + '\n');
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return { leads: [], completedRegions: new Set() };

  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return { leads: [], completedRegions: new Set() };

    const headers = lines[0].split(',');
    const leads = [];
    const completedRegions = new Set();

    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
      if (lead.region) completedRegions.add(lead.region);
    }

    return { leads, completedRegions };
  } catch (err) {
    console.error(`  [WARN] Could not load progress file: ${err.message}`);
    return { leads: [], completedRegions: new Set() };
  }
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current);
  return result;
}

// ---------------------------------------------------------------------------
// Sleep helper
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();

  // Filter to specific region if requested
  let regionsToScrape = REGIONS;
  if (args.region) {
    regionsToScrape = REGIONS.filter(r => r.id === args.region);
    if (regionsToScrape.length === 0) {
      console.error(`Unknown region ID: ${args.region}`);
      console.error('Valid regions:', REGIONS.map(r => `${r.id} (${r.name})`).join(', '));
      process.exit(1);
    }
  }

  // In test mode, only do the first (smallest) region
  if (TEST_MODE && !args.region) {
    // Use Centre-du-Quebec (910) — small region, ~160 brokers
    regionsToScrape = REGIONS.filter(r => r.id === '910');
  }

  // Resume support
  let allLeads = [];
  let completedRegions = new Set();
  const seenIds = new Set();

  if (args.resume) {
    const progress = loadProgress();
    allLeads = progress.leads;
    completedRegions = progress.completedRegions;
    allLeads.forEach(l => { if (l.licence_number) seenIds.add(l.licence_number); });
    console.log(`  Loaded ${allLeads.length} existing leads from progress file`);
    console.log(`  Completed regions: ${[...completedRegions].join(', ') || 'none'}`);
  }

  console.log('\n===================================================================');
  console.log('  MORTAR -- OACIQ Quebec Real Estate Broker Scraper');
  console.log('===================================================================\n');
  console.log(`  Regions:     ${regionsToScrape.length} of ${REGIONS.length}`);
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Test mode:   ${TEST_MODE ? 'YES (max ' + MAX_TEST_PAGES + ' pages per region)' : 'no'}`);
  console.log(`  Output:      ${FINAL_FILE}`);
  console.log('');

  // Get initial session
  console.log('  [SESSION] Fetching CSRF token and session cookie...');
  await refreshSession();
  console.log('  [SESSION] OK - session established\n');

  let totalPages = 0;
  let totalErrors = 0;
  let duplicates = 0;
  let sessionRefreshes = 0;
  let lastSaveCount = 0;

  for (let ri = 0; ri < regionsToScrape.length; ri++) {
    const region = regionsToScrape[ri];

    // Skip completed regions when resuming
    if (args.resume && completedRegions.has(region.name)) {
      console.log(`  [${ri + 1}/${regionsToScrape.length}] ${region.name} -- SKIPPED (already completed)`);
      continue;
    }

    console.log(`  [${ri + 1}/${regionsToScrape.length}] ${region.name} (region ${region.id}) -- starting...`);

    // POST the initial search
    let retries = 0;
    let searchRes;
    while (retries < 3) {
      try {
        searchRes = await searchRegion(region.id);
        if (searchRes.status === 200) break;

        // Session expired or auth issue
        if (searchRes.status === 422 || searchRes.status === 403) {
          console.log(`    [REFRESH] Got ${searchRes.status}, refreshing session...`);
          await refreshSession();
          sessionRefreshes++;
          retries++;
          await sleep(2000);
          continue;
        }

        console.log(`    [WARN] Search returned status ${searchRes.status}`);
        retries++;
        await sleep(3000);
      } catch (err) {
        console.log(`    [ERROR] Search failed: ${err.message}`);
        retries++;
        await sleep(5000);
      }
    }

    if (!searchRes || searchRes.status !== 200) {
      console.log(`    [SKIP] Could not search region ${region.name} after ${retries} retries`);
      totalErrors++;
      continue;
    }

    // Parse first page results
    const firstPageLeads = parseResultsPage(searchRes.body);
    const lastPage = getLastPage(searchRes.body);
    const effectiveLastPage = TEST_MODE ? Math.min(lastPage, MAX_TEST_PAGES) : lastPage;

    console.log(`    Page 1/${lastPage}${TEST_MODE && lastPage > MAX_TEST_PAGES ? ' (capped at ' + effectiveLastPage + ')' : ''} -- ${firstPageLeads.length} brokers`);

    let regionLeadCount = 0;
    let regionDups = 0;

    // Add first page leads
    for (const lead of firstPageLeads) {
      lead.region = region.name;
      if (seenIds.has(lead.licence_number)) {
        regionDups++;
        duplicates++;
        continue;
      }
      seenIds.add(lead.licence_number);
      allLeads.push(lead);
      regionLeadCount++;
    }

    totalPages++;
    await sleep(DELAY_MS);

    // Paginate through remaining pages
    for (let page = 2; page <= effectiveLastPage; page++) {
      let pageRetries = 0;
      let pageRes;

      while (pageRetries < 3) {
        try {
          pageRes = await fetchPage(region.id, page);
          if (pageRes.status === 200) break;

          if (pageRes.status === 422 || pageRes.status === 403) {
            console.log(`    [REFRESH] Got ${pageRes.status} on page ${page}, refreshing session...`);
            await refreshSession();
            sessionRefreshes++;
            // Re-do initial search to re-establish pagination context
            await searchRegion(region.id);
            await sleep(1000);
          }
          pageRetries++;
          await sleep(2000);
        } catch (err) {
          pageRetries++;
          if (pageRetries >= 3) {
            console.log(`    [ERROR] Page ${page}: ${err.message}`);
          }
          await sleep(3000);
        }
      }

      if (!pageRes || pageRes.status !== 200) {
        totalErrors++;
        continue;
      }

      const pageLeads = parseResultsPage(pageRes.body);

      for (const lead of pageLeads) {
        lead.region = region.name;
        if (seenIds.has(lead.licence_number)) {
          regionDups++;
          duplicates++;
          continue;
        }
        seenIds.add(lead.licence_number);
        allLeads.push(lead);
        regionLeadCount++;
      }

      totalPages++;

      // Progress logging every 25 pages or on last page
      if (page % 25 === 0 || page === effectiveLastPage) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`    Page ${page}/${effectiveLastPage} -- ${regionLeadCount} new (${regionDups} dups) -- total: ${allLeads.length} -- ${elapsed}s`);
      }

      // Save progress every 500 new leads
      if (allLeads.length - lastSaveCount >= 500) {
        writeCSV(PROGRESS_FILE, allLeads);
        lastSaveCount = allLeads.length;
        console.log(`    [SAVE] Progress saved: ${allLeads.length} leads`);
      }

      await sleep(DELAY_MS);
    }

    console.log(`    DONE: ${regionLeadCount} new brokers, ${regionDups} duplicates\n`);

    // Save after each region completes
    writeCSV(PROGRESS_FILE, allLeads);
    lastSaveCount = allLeads.length;
  }

  // Final save
  if (allLeads.length > 0) {
    // Save progress file
    writeCSV(PROGRESS_FILE, allLeads);

    // Save final output file
    writeCSV(FINAL_FILE, allLeads);

    // Also save timestamped copy
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const tsPath = path.join(OUTPUT_DIR, `oaciq-real-estate-agents_${timestamp}.csv`);
    writeCSV(tsPath, allLeads);
  }

  // Stats
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const withCity = allLeads.filter(l => l.city).length;
  const uniqueFirms = new Set(allLeads.map(l => l.firm_name).filter(Boolean)).size;

  // Region breakdown
  const regionCounts = {};
  allLeads.forEach(l => {
    const r = l.region || 'Unknown';
    regionCounts[r] = (regionCounts[r] || 0) + 1;
  });

  console.log('\n===================================================================');
  console.log('       OACIQ SCRAPE COMPLETE');
  console.log('===================================================================');
  console.log(`  Total brokers: ${allLeads.length}`);
  console.log(`  With firm:     ${withFirm} (${Math.round(withFirm / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  With city:     ${withCity} (${Math.round(withCity / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  Unique firms:  ${uniqueFirms}`);
  console.log(`  Duplicates:    ${duplicates}`);
  console.log(`  Pages fetched: ${totalPages}`);
  console.log(`  Errors:        ${totalErrors}`);
  console.log(`  Session refs:  ${sessionRefreshes}`);
  console.log(`  Time:          ${elapsed}s (${Math.round(elapsed / 60)}min)`);
  console.log('-------------------------------------------------------------------');
  console.log('  Region breakdown:');

  Object.entries(regionCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([region, count]) => {
      console.log(`    ${region.padEnd(35)} ${String(count).padStart(6)}`);
    });

  console.log('-------------------------------------------------------------------');
  console.log(`  Output: ${FINAL_FILE}`);
  console.log('===================================================================\n');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
