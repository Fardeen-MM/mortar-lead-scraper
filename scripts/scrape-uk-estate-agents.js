#!/usr/bin/env node
/**
 * UK Estate Agent Scraper
 *
 * Sources:
 *   1. Rightmove — Largest UK property portal, JSON-embedded agent listings
 *      URL pattern: /estate-agents/find.html?searchLocation=X&radius=Y&agentTypes=1&index=Z
 *      Data source: __NEXT_DATA__ JSON → props.pageProps.data.results.agentsData.agents[]
 *      Fields: name, branchDisplayName, brandName, telephoneNumbers, branchLink, aboutLink
 *      Detail pages: /estate-agents/agent/{Slug}/{Branch-ID}.html → full address + postcode
 *
 *   2. Propertymark (NAEA) — Professional body, 13,656 members
 *      URL: /find-an-agent/ with loadMore API
 *      Detail pages: /company/{slug}.html → phone, address, services
 *
 * Strategy:
 *   Phase 1: Search Rightmove by major UK cities (covers ~90% of active agents)
 *   Phase 2: Fetch detail pages for full address/postcode
 *   Phase 3: Write CSV with dedup by firm_name + city
 *
 * Usage:
 *   node scripts/scrape-uk-estate-agents.js [options]
 *
 *   --cities     Comma-separated cities (default: major UK cities)
 *   --max-pages  Max pages per city (default: 50, 0 = unlimited)
 *   --no-detail  Skip detail page fetching (faster, less data)
 *   --test       Test mode: 2 cities, 2 pages each
 *   --radius     Search radius in miles (default: 0.0 for exact city)
 *   --output     Output CSV path
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ─── CLI Args ──────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const TEST_MODE = hasFlag('test');
const SKIP_DETAIL = hasFlag('no-detail');
const MAX_PAGES = parseInt(getArg('max-pages') || (TEST_MODE ? '2' : '50')) || 50;
const RADIUS = getArg('radius') || '0.0';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = getArg('output') || path.join(OUTPUT_DIR, 'uk-estate-agents.csv');
const DELAY_MS = 500;
const DETAIL_DELAY_MS = 400;

// Major UK cities — ordered by population/market size
// Each search returns agents in and around that city
const DEFAULT_CITIES = [
  // England — Major cities
  'London', 'Birmingham', 'Manchester', 'Leeds', 'Liverpool',
  'Newcastle upon Tyne', 'Sheffield', 'Bristol', 'Nottingham', 'Leicester',
  'Coventry', 'Bradford', 'Brighton', 'Plymouth', 'Southampton',
  'Portsmouth', 'Reading', 'Derby', 'Stoke-on-Trent', 'Wolverhampton',
  'Sunderland', 'Oxford', 'Cambridge', 'York', 'Norwich',
  'Exeter', 'Bath', 'Canterbury', 'Chester', 'Gloucester',
  'Milton Keynes', 'Northampton', 'Luton', 'Peterborough', 'Ipswich',
  // England — Towns and additional areas
  'Bournemouth', 'Swindon', 'Colchester', 'Chelmsford', 'Maidstone',
  'Guildford', 'Crawley', 'Basingstoke', 'Slough', 'Watford',
  'St Albans', 'Cheltenham', 'Worcester', 'Hereford', 'Shrewsbury',
  'Lincoln', 'Hull', 'Middlesbrough', 'Carlisle', 'Durham',
  'Blackpool', 'Preston', 'Bolton', 'Wigan', 'Warrington',
  'Barnsley', 'Doncaster', 'Huddersfield', 'Wakefield', 'Halifax',
  // Scotland
  'Edinburgh', 'Glasgow', 'Aberdeen', 'Dundee', 'Inverness',
  'Stirling', 'Perth',
  // Wales
  'Cardiff', 'Swansea', 'Newport', 'Wrexham', 'Bangor',
  // Northern Ireland
  'Belfast', 'Derry', 'Lisburn', 'Newry',
];

const CUSTOM_CITIES = getArg('cities');
const CITIES = CUSTOM_CITIES
  ? CUSTOM_CITIES.split(',').map(c => c.trim())
  : (TEST_MODE ? DEFAULT_CITIES.slice(0, 2) : DEFAULT_CITIES);

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source'
];

// ─── HTTP Helpers ──────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpGet(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || 443,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        ...opts.headers
      }
    };

    const req = https.request(options, (res) => {
      // Follow redirects (up to 5)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `https://${parsed.hostname}${res.headers.location}`;
        return httpGet(redirectUrl, opts).then(resolve).catch(reject);
      }

      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ data, status: res.statusCode, headers: res.headers }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.end();
  });
}

// ─── Rightmove Scraper ────────────────────────────────────────

/**
 * Resolve a city name to a Rightmove locationIdentifier via their typeahead API
 * Returns e.g. "REGION^87490" for "London"
 */
async function resolveLocation(cityName) {
  try {
    const url = `https://los.rightmove.co.uk/typeahead?query=${encodeURIComponent(cityName)}&limit=1`;
    const res = await httpGet(url, {
      headers: {
        'Accept': 'application/json',
      }
    });
    if (res.status !== 200) return null;

    // Response should be JSON with Accept: application/json header
    let data;
    try {
      data = JSON.parse(res.data);
    } catch (e) {
      // Might be XML — parse with regex fallback
      const idMatch = res.data.match(/<id>([^<]+)<\/id>/);
      const typeMatch = res.data.match(/<type>([^<]+)<\/type>/);
      if (idMatch) {
        return `${typeMatch ? typeMatch[1] : 'REGION'}^${idMatch[1]}`;
      }
      return null;
    }

    const matches = data.matches || data;
    if (Array.isArray(matches) && matches.length > 0) {
      const match = matches[0];
      const id = match.id;
      const type = match.type || 'REGION';
      return `${type}^${id}`;
    }
    return null;
  } catch (e) {
    return null;
  }
}

/**
 * Extract __NEXT_DATA__ JSON from Rightmove page HTML
 */
function extractNextData(html) {
  // Try script#__NEXT_DATA__ first
  const scriptMatch = html.match(/<script\s+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (scriptMatch) {
    try {
      return JSON.parse(scriptMatch[1]);
    } catch (e) {
      // Fall through
    }
  }

  // Try finding the JSON blob in any script tag
  const jsonMatch = html.match(/"agentsData"\s*:\s*\{/);
  if (jsonMatch) {
    // Find the enclosing script tag content
    const scriptTags = html.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [];
    for (const tag of scriptTags) {
      if (tag.includes('"agentsData"')) {
        const content = tag.replace(/<script[^>]*>/, '').replace(/<\/script>/, '');
        try {
          return JSON.parse(content);
        } catch (e) {
          // Try extracting just the agentsData portion
          const agentsMatch = content.match(/"agentsData"\s*:\s*(\{[\s\S]*?\})\s*[,}]/);
          if (agentsMatch) {
            try {
              return { props: { pageProps: { data: { results: { agentsData: JSON.parse(agentsMatch[1]) } } } } };
            } catch (e2) {
              // Continue
            }
          }
        }
      }
    }
  }

  return null;
}

/**
 * Parse agents from Rightmove __NEXT_DATA__ JSON
 */
function parseAgentsFromNextData(nextData) {
  try {
    const agentsData = nextData?.props?.pageProps?.data?.results?.agentsData;
    if (!agentsData) return { agents: [], total: 0 };

    const total = agentsData.total || 0;
    const rawAgents = agentsData.agents || [];

    const agents = rawAgents.map(a => {
      // Extract phone numbers
      const phones = (a.telephoneNumbers || []).map(t => ({
        number: t.number || t.directNumber || '',
        type: t.type || ''
      })).filter(t => t.number);

      // Primary phone: prefer RESALE type, then first available
      const salesPhone = phones.find(p => p.type === 'RESALE');
      const phone = salesPhone ? salesPhone.number : (phones[0]?.number || '');

      // Branch name often includes location: "Foxtons - Islington"
      const branchName = a.branchDisplayName || a.name || '';
      const brandName = a.brandName || '';

      // Extract city from branch name — format is "Brand, Location" or "Brand - Location"
      let city = '';
      const commaIdx = branchName.lastIndexOf(', ');
      const dashIdx = branchName.lastIndexOf(' - ');
      if (commaIdx > 0) {
        city = branchName.substring(commaIdx + 2).trim();
      } else if (dashIdx > 0) {
        city = branchName.substring(dashIdx + 3).trim();
      }

      // About/detail page URL
      const aboutHref = a.aboutLink?.href || a.branchLink?.href || '';
      const detailUrl = aboutHref
        ? `https://www.rightmove.co.uk${aboutHref}`
        : '';

      return {
        id: a.id,
        firm_name: brandName || branchName.split(/[,\-]/)[0].trim(),
        branch_name: branchName,
        phone,
        all_phones: phones,
        city,
        detail_url: detailUrl,
        is_sales: !!a.sales,
        is_lettings: !!a.lettings,
        is_estate_agent: !!a.estateAgent,
        logo: a.logoPath || '',
      };
    });

    return { agents, total };
  } catch (e) {
    return { agents: [], total: 0 };
  }
}

/**
 * Search Rightmove for estate agents in a given location
 * @param {string} locationIdentifier - e.g. "REGION^87490"
 * @param {string} radius - search radius in miles
 * @param {number} maxPages - max pages to fetch
 */
async function searchRightmove(locationIdentifier, radius = '0.0', maxPages = 50) {
  const allAgents = [];
  let totalResults = 0;
  const PAGE_SIZE = 20; // URL index increments by 20, though JSON may return up to 30

  for (let page = 0; page < maxPages; page++) {
    const index = page * PAGE_SIZE;
    const url = `https://www.rightmove.co.uk/estate-agents/find.html`
      + `?locationIdentifier=${encodeURIComponent(locationIdentifier)}`
      + `&radius=${radius}`
      + `&agentTypes=1`
      + `&index=${index}`;

    try {
      const res = await httpGet(url);

      if (res.status !== 200) {
        if (res.status === 500 || res.status === 404) {
          // Location not found or server error — skip
          break;
        }
        console.error(`    HTTP ${res.status} for ${locationIdentifier} page ${page + 1}`);
        break;
      }

      const nextData = extractNextData(res.data);
      if (!nextData) {
        // Try to detect error page
        if (res.data.includes('something has gone wrong') || res.data.includes('status":500')) {
          break;
        }
        console.error(`    Could not extract data for ${locationIdentifier} page ${page + 1}`);
        break;
      }

      const { agents, total } = parseAgentsFromNextData(nextData);
      if (page === 0) {
        totalResults = total;
      }

      if (agents.length === 0) break;

      allAgents.push(...agents);

      // Check if we've fetched all results
      if (index + PAGE_SIZE >= totalResults) break;
      if (index + PAGE_SIZE >= 1000) break; // Rightmove caps at 50 pages (1000 results)

      await sleep(DELAY_MS);
    } catch (e) {
      console.error(`    Error searching ${locationIdentifier} page ${page + 1}: ${e.message}`);
      break;
    }
  }

  return { agents: allAgents, total: totalResults };
}

/**
 * Fetch Rightmove agent detail page for address info
 * Detail page __NEXT_DATA__ path:
 *   props.pageProps.data.branchProfileResponse.agentProfileResponse
 * Fields: branchAddress, branchPostcode, branchMainTelephone,
 *         branchSalesTelephone, branchLettingsTelephone,
 *         companyName, companyTradingName
 */
async function fetchAgentDetail(detailUrl) {
  try {
    const res = await httpGet(detailUrl);
    if (res.status !== 200) return null;

    const html = res.data;
    let address = '', postcode = '', county = '', email = '', website = '';
    let mainPhone = '', salesPhone = '', lettingsPhone = '';
    let companyName = '';

    // Primary: __NEXT_DATA__ JSON
    const nextData = extractNextData(html);
    if (nextData) {
      const profile = nextData?.props?.pageProps?.data?.branchProfileResponse?.agentProfileResponse;
      if (profile) {
        address = profile.branchAddress || '';
        postcode = profile.branchPostcode || '';
        mainPhone = profile.branchMainTelephone || '';
        salesPhone = profile.branchSalesTelephone || '';
        lettingsPhone = profile.branchLettingsTelephone || '';
        companyName = profile.companyName || profile.companyTradingName || '';

        // Clean up address: replace newlines with ", "
        address = address.replace(/\n/g, ', ').replace(/,\s*,/g, ',').trim();
      }
    }

    // Fallback: regex for postcode if not found in JSON
    if (!postcode) {
      const pcMatch = html.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
      if (pcMatch) postcode = pcMatch[1].trim();
    }

    // Derive county from postcode
    if (postcode) {
      county = postcodeToCounty(postcode);
    }

    return {
      address,
      postcode,
      county,
      email,
      website,
      mainPhone,
      salesPhone,
      lettingsPhone,
      companyName,
    };
  } catch (e) {
    return null;
  }
}

// ─── UK Postcode to County Mapping ────────────────────────────

function postcodeToCounty(postcode) {
  if (!postcode) return '';
  const area = postcode.replace(/\s+/g, '').match(/^([A-Z]{1,2})/i);
  if (!area) return '';

  const POSTCODE_COUNTIES = {
    'AB': 'Aberdeenshire', 'AL': 'Hertfordshire', 'B': 'West Midlands',
    'BA': 'Somerset', 'BB': 'Lancashire', 'BD': 'West Yorkshire',
    'BH': 'Dorset', 'BL': 'Greater Manchester', 'BN': 'East Sussex',
    'BR': 'Greater London', 'BS': 'Bristol', 'BT': 'Northern Ireland',
    'CA': 'Cumbria', 'CB': 'Cambridgeshire', 'CF': 'South Glamorgan',
    'CH': 'Cheshire', 'CM': 'Essex', 'CO': 'Essex',
    'CR': 'Greater London', 'CT': 'Kent', 'CV': 'West Midlands',
    'CW': 'Cheshire', 'DA': 'Kent', 'DD': 'Angus',
    'DE': 'Derbyshire', 'DG': 'Dumfries and Galloway', 'DH': 'County Durham',
    'DL': 'County Durham', 'DN': 'South Yorkshire', 'DT': 'Dorset',
    'DY': 'West Midlands', 'E': 'Greater London', 'EC': 'Greater London',
    'EH': 'Edinburgh', 'EN': 'Hertfordshire', 'EX': 'Devon',
    'FK': 'Stirlingshire', 'FY': 'Lancashire', 'G': 'Glasgow',
    'GL': 'Gloucestershire', 'GU': 'Surrey', 'HA': 'Greater London',
    'HD': 'West Yorkshire', 'HG': 'North Yorkshire', 'HP': 'Buckinghamshire',
    'HR': 'Herefordshire', 'HS': 'Western Isles', 'HU': 'East Yorkshire',
    'HX': 'West Yorkshire', 'IG': 'Greater London', 'IP': 'Suffolk',
    'IV': 'Highland', 'KA': 'Ayrshire', 'KT': 'Surrey',
    'KW': 'Highland', 'KY': 'Fife', 'L': 'Merseyside',
    'LA': 'Lancashire', 'LD': 'Powys', 'LE': 'Leicestershire',
    'LL': 'Gwynedd', 'LN': 'Lincolnshire', 'LS': 'West Yorkshire',
    'LU': 'Bedfordshire', 'M': 'Greater Manchester', 'ME': 'Kent',
    'MK': 'Buckinghamshire', 'ML': 'Lanarkshire', 'N': 'Greater London',
    'NE': 'Tyne and Wear', 'NG': 'Nottinghamshire', 'NN': 'Northamptonshire',
    'NP': 'Gwent', 'NR': 'Norfolk', 'NW': 'Greater London',
    'OL': 'Greater Manchester', 'OX': 'Oxfordshire', 'PA': 'Renfrewshire',
    'PE': 'Cambridgeshire', 'PH': 'Perthshire', 'PL': 'Devon',
    'PO': 'Hampshire', 'PR': 'Lancashire', 'RG': 'Berkshire',
    'RH': 'Surrey', 'RM': 'Greater London', 'S': 'South Yorkshire',
    'SA': 'West Glamorgan', 'SE': 'Greater London', 'SG': 'Hertfordshire',
    'SK': 'Cheshire', 'SL': 'Berkshire', 'SM': 'Greater London',
    'SN': 'Wiltshire', 'SO': 'Hampshire', 'SP': 'Wiltshire',
    'SR': 'Tyne and Wear', 'SS': 'Essex', 'ST': 'Staffordshire',
    'SW': 'Greater London', 'SY': 'Shropshire', 'TA': 'Somerset',
    'TD': 'Scottish Borders', 'TF': 'Shropshire', 'TN': 'Kent',
    'TQ': 'Devon', 'TR': 'Cornwall', 'TS': 'Cleveland',
    'TW': 'Greater London', 'UB': 'Greater London', 'W': 'Greater London',
    'WA': 'Cheshire', 'WC': 'Greater London', 'WD': 'Hertfordshire',
    'WF': 'West Yorkshire', 'WN': 'Greater Manchester', 'WR': 'Worcestershire',
    'WS': 'West Midlands', 'WV': 'West Midlands', 'YO': 'North Yorkshire',
    'ZE': 'Shetland',
  };

  const raw = area[1].toUpperCase();
  // Try 2-char code first (e.g., "BH"), then single-char (e.g., "B")
  if (raw.length === 2) {
    return POSTCODE_COUNTIES[raw] || POSTCODE_COUNTIES[raw[0]] || '';
  }
  return POSTCODE_COUNTIES[raw] || '';
}

// ─── CSV Helpers ──────────────────────────────────────────────

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function extractDomain(urlOrEmail) {
  if (!urlOrEmail) return '';
  try {
    if (urlOrEmail.includes('@')) {
      const domain = urlOrEmail.split('@')[1]?.toLowerCase() || '';
      if (['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'].includes(domain)) return '';
      return domain;
    }
    const u = new URL(urlOrEmail.startsWith('http') ? urlOrEmail : `https://${urlOrEmail}`);
    return u.hostname.replace(/^www\./, '').toLowerCase();
  } catch (e) {
    return '';
  }
}

// ─── Dedup Key ────────────────────────────────────────────────

function dedupKey(firmName, city) {
  return `${(firmName || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

// ─── Main ─────────────────────────────────────────────────────

async function main() {
  console.log('=== UK Estate Agent Scraper ===\n');
  console.log(`Mode: ${TEST_MODE ? 'TEST (limited)' : 'FULL'}`);
  console.log(`Cities: ${CITIES.length}`);
  console.log(`Max pages per city: ${MAX_PAGES}`);
  console.log(`Radius: ${RADIUS} miles`);
  console.log(`Detail pages: ${SKIP_DETAIL ? 'SKIP' : 'YES'}`);
  console.log(`Output: ${OUTPUT_FILE}\n`);

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // ─── Phase 0: Resolve city names to Rightmove location IDs ──

  console.log('Phase 0: Resolving city names to Rightmove location IDs...\n');

  const resolvedCities = [];
  for (let ci = 0; ci < CITIES.length; ci++) {
    const city = CITIES[ci];
    const locId = await resolveLocation(city);
    if (locId) {
      resolvedCities.push({ name: city, locationIdentifier: locId });
    } else {
      console.log(`  SKIP: Could not resolve "${city}"`);
    }
    // Small delay between typeahead lookups
    if (ci < CITIES.length - 1) await sleep(200);
  }

  console.log(`Resolved ${resolvedCities.length}/${CITIES.length} cities\n`);

  // ─── Phase 1: Search Rightmove by city ───────────────────

  console.log('Phase 1: Searching Rightmove by city...\n');

  const seen = new Map(); // dedupKey -> agent record
  let totalSearched = 0;
  let totalSkipped = 0;

  for (let ci = 0; ci < resolvedCities.length; ci++) {
    const { name: city, locationIdentifier } = resolvedCities[ci];
    process.stdout.write(`  [${ci + 1}/${resolvedCities.length}] ${city}...`);

    const { agents, total } = await searchRightmove(locationIdentifier, RADIUS, MAX_PAGES);
    totalSearched += agents.length;

    let newCount = 0;
    for (const agent of agents) {
      // Use search city if agent city is empty
      const agentCity = agent.city || city;
      const key = dedupKey(agent.firm_name, agentCity);

      if (seen.has(key)) {
        totalSkipped++;
        // Merge data: keep the version with more info
        const existing = seen.get(key);
        if (!existing.phone && agent.phone) existing.phone = agent.phone;
        if (!existing.detail_url && agent.detail_url) existing.detail_url = agent.detail_url;
        continue;
      }

      seen.set(key, {
        ...agent,
        city: agentCity,
        search_city: city,
      });
      newCount++;
    }

    console.log(` ${total} total, ${agents.length} fetched, ${newCount} new (total unique: ${seen.size})`);
    await sleep(DELAY_MS);
  }

  console.log(`\nPhase 1 complete: ${seen.size} unique agents (${totalSkipped} duplicates skipped)`);

  // ─── Phase 2: Fetch detail pages ─────────────────────────

  const agents = Array.from(seen.values());

  if (!SKIP_DETAIL) {
    console.log(`\nPhase 2: Fetching detail pages for address/email...`);

    let detailCount = 0;
    let addressCount = 0;
    let emailCount = 0;
    let websiteCount = 0;

    // In test mode, only fetch first 5 detail pages
    const detailLimit = TEST_MODE ? 5 : agents.length;

    for (let i = 0; i < Math.min(agents.length, detailLimit); i++) {
      const agent = agents[i];
      if (!agent.detail_url) continue;

      if ((i + 1) % 100 === 0 || i === 0) {
        process.stdout.write(`  [${i + 1}/${Math.min(agents.length, detailLimit)}] Fetching details...`);
      }

      const detail = await fetchAgentDetail(agent.detail_url);
      if (detail) {
        detailCount++;
        if (detail.address) { agent.address = detail.address; addressCount++; }
        if (detail.postcode) agent.postcode = detail.postcode;
        if (detail.county) agent.county = detail.county;
        if (detail.email) { agent.email = detail.email; emailCount++; }
        if (detail.website) { agent.website = detail.website; websiteCount++; }
        if (detail.companyName) agent.company_name = detail.companyName;
        // Merge phones: prefer detail page phones if they have more info
        if (detail.salesPhone) agent.sales_phone = detail.salesPhone;
        if (detail.lettingsPhone) agent.lettings_phone = detail.lettingsPhone;
        if (detail.mainPhone && !agent.phone) agent.phone = detail.mainPhone;
      }

      if ((i + 1) % 100 === 0) {
        console.log(` ${addressCount} addresses, ${emailCount} emails, ${websiteCount} websites`);
      }

      await sleep(DETAIL_DELAY_MS);
    }

    console.log(`\nPhase 2 complete: ${detailCount} detail pages fetched`);
    console.log(`  Addresses: ${addressCount} | Emails: ${emailCount} | Websites: ${websiteCount}`);
  } else {
    console.log('\nPhase 2: SKIPPED (--no-detail flag)');
  }

  // ─── Phase 3: Write CSV ──────────────────────────────────

  console.log(`\nPhase 3: Writing CSV...`);

  const csvLines = [CSV_HEADERS.join(',')];

  for (const agent of agents) {
    const firmName = agent.firm_name || agent.branch_name || '';
    const city = agent.city || '';
    const county = agent.county || '';
    const phone = agent.phone || '';
    const email = agent.email || '';
    const website = agent.website || '';
    const domain = extractDomain(website) || extractDomain(email);

    // Estate agents are firms, not individuals — leave first/last name empty
    // unless we later add person extraction
    const row = [
      '',                           // first_name
      '',                           // last_name
      escapeCsv(firmName),          // firm_name
      '',                           // title
      escapeCsv(email),             // email
      escapeCsv(phone),             // phone
      escapeCsv(website),           // website
      escapeCsv(domain),            // domain
      escapeCsv(city),              // city
      escapeCsv(county),            // state (county)
      'UK',                         // country
      'estate agent',               // niche
      'rightmove',                  // source
    ];
    csvLines.push(row.join(','));
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  console.log(`CSV written to ${OUTPUT_FILE}`);
  console.log(`Total rows: ${agents.length}`);

  // ─── Stats ───────────────────────────────────────────────

  const stats = {
    total: agents.length,
    withPhone: agents.filter(a => a.phone).length,
    withEmail: agents.filter(a => a.email).length,
    withWebsite: agents.filter(a => a.website).length,
    withAddress: agents.filter(a => a.address).length,
    salesOnly: agents.filter(a => a.is_sales && !a.is_lettings).length,
    lettingsOnly: agents.filter(a => !a.is_sales && a.is_lettings).length,
    both: agents.filter(a => a.is_sales && a.is_lettings).length,
  };

  console.log('\n─── Summary ───');
  console.log(`Total agents:  ${stats.total}`);
  console.log(`With phone:    ${stats.withPhone}`);
  console.log(`With email:    ${stats.withEmail}`);
  console.log(`With website:  ${stats.withWebsite}`);
  console.log(`With address:  ${stats.withAddress}`);
  console.log(`Sales only:    ${stats.salesOnly}`);
  console.log(`Lettings only: ${stats.lettingsOnly}`);
  console.log(`Sales+Lettings:${stats.both}`);

  // Top cities
  const cityCounts = {};
  for (const a of agents) {
    if (a.city) cityCounts[a.city] = (cityCounts[a.city] || 0) + 1;
  }
  console.log('\nTop 15 Cities:');
  Object.entries(cityCounts).sort((a, b) => b[1] - a[1]).slice(0, 15)
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  // Top counties
  const countyCounts = {};
  for (const a of agents) {
    if (a.county) countyCounts[a.county] = (countyCounts[a.county] || 0) + 1;
  }
  if (Object.keys(countyCounts).length) {
    console.log('\nTop 10 Counties:');
    Object.entries(countyCounts).sort((a, b) => b[1] - a[1]).slice(0, 10)
      .forEach(([k, v]) => console.log(`  ${k}: ${v}`));
  }

  // Top firms (by branch count)
  const firmCounts = {};
  for (const a of agents) {
    if (a.firm_name) firmCounts[a.firm_name] = (firmCounts[a.firm_name] || 0) + 1;
  }
  console.log('\nTop 15 Firms (by branch count):');
  Object.entries(firmCounts).sort((a, b) => b[1] - a[1]).slice(0, 15)
    .forEach(([k, v]) => console.log(`  ${k}: ${v} branches`));
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
