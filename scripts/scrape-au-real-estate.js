#!/usr/bin/env node
/**
 * Scrape Australian real estate agents from publicly accessible sources.
 *
 * Sources:
 *   1. RateMyAgent sitemaps — agent names + profile URLs (40K+ agents)
 *      - sitemap-sales-agents-0001.xml through 0004.xml
 *      - sitemap-agencies-sales-0001.xml (9K+ agencies with suburb hints)
 *      - sitemap-sales-locality-*.xml (suburb-to-state mapping)
 *   2. RateMyAgent locality/agents pages — per-suburb agent listings (Datadome-blocked,
 *      saved for future Puppeteer enrichment)
 *
 * Strategy:
 *   - Parse sitemaps to extract agent names, agency names, and suburb/state data
 *   - Agent URLs follow pattern: /real-estate-agent/{first}-{last}-{code}/sales/overview
 *   - Agency URLs follow pattern: /real-estate-agency/{name}-{suburb?}-{code}/sales/overview
 *   - Locality URLs follow pattern: /real-estate-profile/sales/{suburb}-{state}-{postcode}
 *   - Cross-reference agency suburb hints with locality state mapping
 *   - Dedup by name+code (unique slug)
 *
 * Note: All major AU real estate portals (realestate.com.au, domain.com.au, ratemyagent.com.au,
 *       homely.com.au, yellowpages.com.au) use aggressive bot protection (Kasada, Datadome,
 *       Cloudflare). Only sitemaps are accessible via plain HTTP.
 *       State registries (NSW Fair Trading, VIC Consumer Affairs) are SPAs requiring JS.
 *       State REIs (REINSW, REIV, REIQ, REIWA) are SPAs or iMIS systems requiring JS.
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, fs, path).
 *
 * Output CSV format:
 *   first_name,last_name,firm_name,title,email,phone,website,domain,city,state,country,niche,source
 *
 * Usage:
 *   node scripts/scrape-au-real-estate.js                      # full scrape (all sitemaps)
 *   node scripts/scrape-au-real-estate.js --test               # test mode (first sitemap chunk only)
 *   node scripts/scrape-au-real-estate.js --delay=500          # custom delay between requests
 *   node scripts/scrape-au-real-estate.js --agents-only        # skip agency/locality sitemaps
 *   node scripts/scrape-au-real-estate.js --max-chunks=2       # limit sitemap chunks
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 500;
const TEST_MODE = !!args.test;
const AGENTS_ONLY = !!args['agents-only'];
const MAX_CHUNKS = parseInt(args['max-chunks']) || (TEST_MODE ? 1 : 99);
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'au-real-estate-agents.csv');

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url',
];

// ── Known AU suburbs → state mapping (common ones for agency name parsing) ──
// Built from locality sitemaps at runtime, but seed with major cities
const KNOWN_SUBURBS = {
  'sydney': 'NSW', 'melbourne': 'VIC', 'brisbane': 'QLD', 'perth': 'WA',
  'adelaide': 'SA', 'hobart': 'TAS', 'darwin': 'NT', 'canberra': 'ACT',
  'gold-coast': 'QLD', 'sunshine-coast': 'QLD', 'newcastle': 'NSW',
  'wollongong': 'NSW', 'geelong': 'VIC', 'cairns': 'QLD', 'townsville': 'QLD',
  'toowoomba': 'QLD', 'ballarat': 'VIC', 'bendigo': 'VIC', 'albury': 'NSW',
  'launceston': 'TAS', 'mackay': 'QLD', 'rockhampton': 'QLD', 'bunbury': 'WA',
  'bundaberg': 'QLD', 'wagga-wagga': 'NSW', 'hervey-bay': 'QLD',
  'mildura': 'VIC', 'shepparton': 'VIC', 'gladstone': 'QLD', 'tamworth': 'NSW',
  'port-macquarie': 'NSW', 'orange': 'NSW', 'dubbo': 'NSW', 'bathurst': 'NSW',
  'lismore': 'NSW', 'warrnambool': 'VIC', 'mount-gambier': 'SA',
  'alice-springs': 'NT', 'katherine': 'NT', 'palmerston': 'NT',
  'wyndham': 'VIC', 'denham-court': 'NSW', 'kimba': 'SA',
  'maryborough': 'QLD', 'parramatta': 'NSW', 'chatswood': 'NSW',
  'bondi': 'NSW', 'manly': 'NSW', 'cronulla': 'NSW', 'penrith': 'NSW',
  'liverpool': 'NSW', 'campbelltown': 'NSW', 'bankstown': 'NSW',
  'blacktown': 'NSW', 'hornsby': 'NSW', 'epping': 'NSW',
  'st-kilda': 'VIC', 'fitzroy': 'VIC', 'richmond': 'VIC',
  'south-yarra': 'VIC', 'toorak': 'VIC', 'hawthorn': 'VIC',
  'surfers-paradise': 'QLD', 'broadbeach': 'QLD', 'noosa': 'QLD',
  'fremantle': 'WA', 'subiaco': 'WA', 'joondalup': 'WA',
  'glenelg': 'SA', 'norwood': 'SA', 'unley': 'SA',
  'sandy-bay': 'TAS', 'kingston': 'TAS',
};

// ── State codes ─────────────────────────────────────────────────────────
const VALID_STATES = new Set(['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']);

// ── HTTP GET helper ─────────────────────────────────────────────────────
function httpGet(url) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/xml,application/xml,text/html,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
    };

    const req = https.request(options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        httpGet(res.headers.location).then(resolve).catch(reject);
        res.resume();
        return;
      }

      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });

    req.on('error', reject);
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error(`Timeout fetching ${url}`));
    });
    req.end();
  });
}

// ── XML parsing helpers (no deps) ───────────────────────────────────────
function extractLocs(xml) {
  const locs = [];
  const regex = /<loc>([^<]+)<\/loc>/g;
  let match;
  while ((match = regex.exec(xml)) !== null) {
    locs.push(match[1].trim());
  }
  return locs;
}

function extractSitemapLocs(xml) {
  // Extract <loc> inside <sitemap> elements (sitemap index)
  const locs = [];
  const regex = /<sitemap>\s*<loc>([^<]+)<\/loc>/g;
  let match;
  while ((match = regex.exec(xml)) !== null) {
    locs.push(match[1].trim());
  }
  return locs;
}

// ── Name parsing from URL slug ──────────────────────────────────────────
/**
 * Parse agent name from RateMyAgent URL slug.
 * Pattern: {first}-{last}-{code} or {first}-{middle}-{last}-{code}
 * Code is always 5 chars: 2 letters + 3 digits (e.g., ed645, hr701)
 */
function parseAgentSlug(slug) {
  // Remove the code suffix (e.g., "-ed645")
  const codeMatch = slug.match(/^(.+)-([a-z]{2}\d{3})$/);
  if (!codeMatch) return null;

  const namePart = codeMatch[1];
  const code = codeMatch[2];
  const parts = namePart.split('-').filter(Boolean);

  if (parts.length === 0) return null;

  let firstName, lastName;
  if (parts.length === 1) {
    firstName = parts[0];
    lastName = '';
  } else if (parts.length === 2) {
    firstName = parts[0];
    lastName = parts[1];
  } else {
    // Multi-part name: first = first part, last = last part
    // e.g., "melissa-martin-smith" -> first=melissa, last=martin-smith
    // But some have middle names: "aaron-jay-camilleri" -> first=aaron, last=camilleri
    // Heuristic: if 3 parts, first=first, last=last (middle dropped)
    // if 4+ parts, first=first, last=last two joined
    firstName = parts[0];
    if (parts.length === 3) {
      // Could be first-middle-last or first-last1-last2
      lastName = parts[2]; // Most common: first-middle-last
    } else {
      lastName = parts.slice(-2).join('-');
    }
  }

  return {
    first_name: titleCase(firstName),
    last_name: titleCase(lastName),
    code: code,
  };
}

/**
 * Parse agency name and optional suburb from RateMyAgent agency URL slug.
 * Pattern: {agency-name}-{suburb?}-{code}
 * Code is always 5 chars: 2 letters + 3 digits
 */
function parseAgencySlug(slug) {
  const codeMatch = slug.match(/^(.+)-([a-z]{2}\d{3})$/);
  if (!codeMatch) return null;

  const namePart = codeMatch[1];
  const code = codeMatch[2];

  // Try to detect suburb at end of name by checking against known suburbs
  let agencyName = namePart;
  let suburb = '';
  let state = '';

  // Check if last 1-3 hyphenated segments match a known suburb
  const parts = namePart.split('-');
  for (let suburbLen = 3; suburbLen >= 1; suburbLen--) {
    if (parts.length <= suburbLen) continue;
    const candidateSuburb = parts.slice(-suburbLen).join('-');
    if (KNOWN_SUBURBS[candidateSuburb]) {
      suburb = candidateSuburb;
      state = KNOWN_SUBURBS[candidateSuburb];
      agencyName = parts.slice(0, -suburbLen).join('-');
      break;
    }
  }

  // Also check for state abbreviations at the end (e.g., "exp-australia-tas")
  if (!state) {
    const lastPart = parts[parts.length - 1].toUpperCase();
    if (VALID_STATES.has(lastPart)) {
      state = lastPart;
      agencyName = parts.slice(0, -1).join('-');
    }
  }

  return {
    agency_name: titleCase(agencyName.replace(/-/g, ' ')),
    suburb: titleCase(suburb.replace(/-/g, ' ')),
    state: state,
    code: code,
  };
}

/**
 * Parse locality URL to extract suburb, state, and postcode.
 * Pattern: /real-estate-profile/sales/{suburb}-{state}-{postcode}
 */
function parseLocalityUrl(url) {
  const match = url.match(/\/real-estate-profile\/sales\/(.+)$/);
  if (!match) return null;

  let slug = match[1].replace(/\/(agents|agencies|property-listings)$/, '');

  // Pattern: suburb-name-STATE-POSTCODE
  const locMatch = slug.match(/^(.+)-(nsw|vic|qld|wa|sa|tas|act|nt)-(\d{4})$/i);
  if (!locMatch) return null;

  return {
    suburb: locMatch[1],
    state: locMatch[2].toUpperCase(),
    postcode: locMatch[3],
  };
}

// ── Helpers ─────────────────────────────────────────────────────────────
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|\s|[-'])\w/g, c => c.toUpperCase());
}

function escapeCSV(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
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
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── Sitemap fetching with chunked sub-sitemap support ───────────────────
async function fetchSitemapUrls(indexUrl, maxChunks) {
  console.log(`  Fetching sitemap index: ${indexUrl}`);
  const indexXml = await httpGet(indexUrl);

  // Check if this is a sitemap index or a direct urlset
  const isIndex = indexXml.includes('<sitemapindex');
  if (!isIndex) {
    // Direct urlset
    return extractLocs(indexXml);
  }

  // Sitemap index — fetch each child sitemap
  const chunkUrls = extractSitemapLocs(indexXml);
  console.log(`  Found ${chunkUrls.length} sitemap chunks (fetching up to ${maxChunks})`);

  const allUrls = [];
  for (let i = 0; i < Math.min(chunkUrls.length, maxChunks); i++) {
    const chunkUrl = chunkUrls[i];
    console.log(`  Fetching chunk ${i + 1}/${Math.min(chunkUrls.length, maxChunks)}: ${path.basename(chunkUrl)}`);

    try {
      const chunkXml = await httpGet(chunkUrl);
      const urls = extractLocs(chunkXml);
      allUrls.push(...urls);
      console.log(`    -> ${urls.length} URLs`);
    } catch (err) {
      console.log(`    [ERR] ${err.message}`);
    }

    if (i < Math.min(chunkUrls.length, maxChunks) - 1) {
      await sleep(DELAY_MS);
    }
  }

  return allUrls;
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('=============================================================');
  console.log(`  MORTAR -- AU Real Estate Agent Scraper${TEST_MODE ? ' (TEST MODE)' : ''}`);
  console.log('=============================================================');
  console.log('');
  console.log('  Sources:');
  console.log('    - RateMyAgent agent sitemaps (40K+ agents)');
  if (!AGENTS_ONLY) {
    console.log('    - RateMyAgent agency sitemaps (9K+ agencies)');
    console.log('    - RateMyAgent locality sitemaps (suburb/state mapping)');
  }
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Max chunks:  ${MAX_CHUNKS}`);
  console.log(`  Output:      ${OUTPUT_FILE}`);
  console.log('');

  // ── Step 1: Build suburb → state mapping from locality sitemaps ────
  if (!AGENTS_ONLY) {
    console.log('--- Step 1: Building suburb → state mapping ---');
    try {
      const localityUrls = await fetchSitemapUrls(
        'https://www.ratemyagent.com.au/sitemap-sales-locality.xml',
        TEST_MODE ? 1 : MAX_CHUNKS
      );

      let localityCount = 0;
      for (const url of localityUrls) {
        const loc = parseLocalityUrl(url);
        if (loc && !KNOWN_SUBURBS[loc.suburb]) {
          KNOWN_SUBURBS[loc.suburb] = loc.state;
          localityCount++;
        }
      }
      console.log(`  Loaded ${localityCount} new suburb → state mappings (total: ${Object.keys(KNOWN_SUBURBS).length})`);
    } catch (err) {
      console.log(`  [WARN] Failed to load locality sitemaps: ${err.message}`);
      console.log('  Continuing with built-in suburb mapping...');
    }
    console.log('');
    await sleep(DELAY_MS);
  }

  // ── Step 2: Parse agency sitemaps ──────────────────────────────────
  const agencyMap = new Map(); // code → { agency_name, suburb, state }

  if (!AGENTS_ONLY) {
    console.log('--- Step 2: Parsing agency sitemaps ---');
    try {
      const agencyUrls = await fetchSitemapUrls(
        'https://www.ratemyagent.com.au/sitemap-agencies-sales.xml',
        MAX_CHUNKS
      );

      const overviewUrls = agencyUrls.filter(u => u.endsWith('/sales/overview'));
      console.log(`  Found ${overviewUrls.length} unique agencies`);

      for (const url of overviewUrls) {
        const match = url.match(/\/real-estate-agency\/([^/]+)\//);
        if (!match) continue;

        const parsed = parseAgencySlug(match[1]);
        if (parsed) {
          parsed.profile_url = url;
          agencyMap.set(parsed.code, parsed);
        }
      }
      console.log(`  Parsed ${agencyMap.size} agency records`);

      // Stats
      const withState = Array.from(agencyMap.values()).filter(a => a.state).length;
      const withSuburb = Array.from(agencyMap.values()).filter(a => a.suburb).length;
      console.log(`  With state: ${withState}, With suburb: ${withSuburb}`);
    } catch (err) {
      console.log(`  [WARN] Failed to load agency sitemaps: ${err.message}`);
    }
    console.log('');
    await sleep(DELAY_MS);
  }

  // ── Step 3: Parse agent sitemaps ───────────────────────────────────
  console.log('--- Step 3: Parsing agent sitemaps ---');

  const agentUrls = await fetchSitemapUrls(
    'https://www.ratemyagent.com.au/sitemap-sales-agents.xml',
    MAX_CHUNKS
  );

  // Filter to overview URLs only (dedup different pages for same agent)
  const agentOverviewUrls = agentUrls.filter(u => u.endsWith('/sales/overview'));
  console.log(`  Found ${agentOverviewUrls.length} unique agent overview URLs`);

  const seenCodes = new Set();
  const allLeads = [];
  let duplicates = 0;

  for (const url of agentOverviewUrls) {
    const match = url.match(/\/real-estate-agent\/([^/]+)\//);
    if (!match) continue;

    const parsed = parseAgentSlug(match[1]);
    if (!parsed) continue;

    // Dedup by code
    if (seenCodes.has(parsed.code)) {
      duplicates++;
      continue;
    }
    seenCodes.add(parsed.code);

    const lead = {
      first_name: parsed.first_name,
      last_name: parsed.last_name,
      firm_name: '',
      title: 'Real Estate Agent',
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: '',
      state: '',
      country: 'AU',
      niche: 'real estate agent',
      source: 'ratemyagent',
      profile_url: url,
    };

    allLeads.push(lead);
  }

  console.log(`  Parsed ${allLeads.length} unique agents (${duplicates} duplicates skipped)`);
  console.log('');

  // ── Step 4: Build agency leads (with firm name, city, state) ──────
  const agencyLeads = [];
  if (agencyMap.size > 0) {
    console.log('--- Step 4: Building agency leads ---');

    for (const [code, agency] of agencyMap) {
      agencyLeads.push({
        first_name: '',
        last_name: '',
        firm_name: agency.agency_name,
        title: 'Real Estate Agency',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: agency.suburb,
        state: agency.state,
        country: 'AU',
        niche: 'real estate agent',
        source: 'ratemyagent',
        profile_url: agency.profile_url || '',
      });
    }

    console.log(`  Built ${agencyLeads.length} agency leads`);
    const agenciesWithState = agencyLeads.filter(l => l.state).length;
    const agenciesWithCity = agencyLeads.filter(l => l.city).length;
    console.log(`  With state: ${agenciesWithState}, With city: ${agenciesWithCity}`);
    console.log('');
  }

  // ── Step 5: Save CSVs ─────────────────────────────────────────────
  console.log('--- Step 5: Saving CSVs ---');

  // Save agents
  if (allLeads.length > 0) {
    writeCSV(OUTPUT_FILE, allLeads);
    console.log(`  Agents: ${allLeads.length} leads -> ${OUTPUT_FILE}`);
  }

  // Save agencies separately
  const AGENCY_FILE = path.join(OUTPUT_DIR, 'au-real-estate-agencies.csv');
  if (agencyLeads.length > 0) {
    writeCSV(AGENCY_FILE, agencyLeads);
    console.log(`  Agencies: ${agencyLeads.length} leads -> ${AGENCY_FILE}`);
  }

  // Save combined file (agents + agencies)
  const combinedLeads = [...allLeads, ...agencyLeads];
  const COMBINED_FILE = path.join(OUTPUT_DIR, 'au-real-estate-combined.csv');
  if (combinedLeads.length > 0) {
    writeCSV(COMBINED_FILE, combinedLeads);
    console.log(`  Combined: ${combinedLeads.length} leads -> ${COMBINED_FILE}`);
  }

  // Timestamped copy
  if (combinedLeads.length > 0) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const finalPath = path.join(OUTPUT_DIR, `au-real-estate-combined_${timestamp}.csv`);
    writeCSV(finalPath, combinedLeads);
    console.log(`  Timestamped: ${finalPath}`);
  }

  // ── Summary ─────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const uniqueFirstNames = new Set(allLeads.map(l => l.first_name)).size;
  const agentWithState = allLeads.filter(l => l.state).length;
  const agentWithCity = allLeads.filter(l => l.city).length;
  const agentWithFirm = allLeads.filter(l => l.firm_name).length;
  const agenciesWithState = agencyLeads.filter(l => l.state).length;
  const agenciesWithCity = agencyLeads.filter(l => l.city).length;

  // State distribution for agencies
  const stateDistrib = {};
  for (const lead of agencyLeads) {
    if (lead.state) {
      stateDistrib[lead.state] = (stateDistrib[lead.state] || 0) + 1;
    }
  }

  console.log('');
  console.log('=============================================================');
  console.log('  AU REAL ESTATE SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  Agents:           ${allLeads.length} (${uniqueFirstNames} unique first names)`);
  console.log(`  Agencies:         ${agencyLeads.length}`);
  console.log(`  Combined total:   ${combinedLeads.length}`);
  console.log(`  Agent with firm:  ${agentWithFirm}`);
  console.log(`  Agent with state: ${agentWithState}`);
  console.log(`  Agency w/ state:  ${agenciesWithState}`);
  console.log(`  Agency w/ city:   ${agenciesWithCity}`);
  console.log(`  Suburb mappings:  ${Object.keys(KNOWN_SUBURBS).length}`);
  console.log(`  Duplicates:       ${duplicates}`);
  console.log(`  Time:             ${elapsed}s`);

  if (Object.keys(stateDistrib).length > 0) {
    console.log('');
    console.log('  Agency state distribution:');
    Object.entries(stateDistrib)
      .sort((a, b) => b[1] - a[1])
      .forEach(([state, count]) => {
        console.log(`    ${state}: ${count}`);
      });
  }

  console.log('');
  console.log('  Output files:');
  console.log(`    Agents:   ${OUTPUT_FILE}`);
  if (agencyLeads.length > 0) console.log(`    Agencies: ${AGENCY_FILE}`);
  console.log(`    Combined: ${COMBINED_FILE}`);
  console.log('');
  console.log('  Note: Email, phone, and detailed firm/city data require');
  console.log('  Puppeteer-based profile enrichment (RateMyAgent uses Datadome');
  console.log('  bot protection on all HTML pages). Profile URLs are saved in');
  console.log('  the CSV for future enrichment.');
  console.log('');
  console.log('  Enrichment path:');
  console.log('  1. Puppeteer with stealth plugin to access RateMyAgent profiles');
  console.log('  2. Cross-reference with Google Places API (niche search)');
  console.log('  3. NSW Fair Trading licence check (verify.licence.nsw.gov.au)');
  console.log('     requires Puppeteer (React SPA)');
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
