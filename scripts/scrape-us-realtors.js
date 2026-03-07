#!/usr/bin/env node
/**
 * Scrape US real estate agents from state regulatory commission directories.
 *
 * Working Sources:
 *   1. NY DOS  (New York Dept of State) — HTML scraping via POST search
 *      URL: https://appext20.dos.ny.gov/nydos/
 *      Method: POST search by last name + first name prefix (min 2 chars each)
 *      Data: name, license number, license type, address, firm (via detail page)
 *
 *   2. TX TREC (Texas Real Estate Commission) — Typesense JSON API
 *      URL: https://www.trec.texas.gov/ts/collections/licenses/documents/search
 *      Method: Typesense search by lastName prefix, public API key
 *      Data: name, license number, license type, organization name
 *
 * Investigated but not working:
 *   - FL DBPR: ASP session management rejects automated requests
 *   - CA DRE: Requires CAPTCHA
 *   - Realtor.com: 429 rate limiting / anti-bot
 *   - Zillow: 403 Forbidden
 *   - Homes.com: 403 Forbidden
 *   - NAR: Requires authentication
 *   - ARELLO: Cloudflare Turnstile CAPTCHA
 *   - OH, PA, IL, GA, NC, MI: Scaffolded, not yet implemented
 *
 * Strategy: Uses state regulatory commission directories that expose
 * searchable data without CAPTCHA. NY uses HTML form POST + regex parsing.
 * TX uses a reverse-engineered Typesense JSON API (public key in JS bundle).
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, http, fs, path).
 * HTML parsing done with regex.
 *
 * Features:
 *   - Multi-state scraper architecture (add states by implementing a source adapter)
 *   - 2-letter last name + 2-letter first name prefix iteration
 *   - Deduplication by license_number or name+city
 *   - Auto-saves progress CSV every 500 new leads
 *   - Rate limiting (configurable, default 500ms)
 *   - State selection via --state=NY,FL,TX
 *   - Resume support (reads existing progress file)
 *   - Segment support for parallel execution (--segment=A-F, G-L, M-R, S-Z)
 *
 * Output CSV format:
 *   first_name,last_name,firm_name,title,email,phone,website,domain,city,state,country,niche,source,license_number
 *
 * Usage:
 *   node scripts/scrape-us-realtors.js                       # All states (NY + TX)
 *   node scripts/scrape-us-realtors.js --state=NY            # NY only
 *   node scripts/scrape-us-realtors.js --state=TX            # TX only
 *   node scripts/scrape-us-realtors.js --state=NY,TX         # NY + TX
 *   node scripts/scrape-us-realtors.js --segment=A-F         # Last names A-F
 *   node scripts/scrape-us-realtors.js --test                # Small sample (1 prefix "sm")
 *   node scripts/scrape-us-realtors.js --delay=500           # Custom delay (ms)
 *   node scripts/scrape-us-realtors.js --resume              # Resume from progress file
 *
 * Estimates (full run):
 *   NY: 26 last name prefixes x 140 first name prefixes = ~3,640 search requests
 *       Each search yields 0-30 agents. ~50k-100k agents expected.
 *   TX: 26 x 26 = 676 last name prefix searches via Typesense API
 *       Each yields up to 2,500 agents (paginated 250/page). ~200k+ agents expected.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { URL } = require('url');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 500;
const TEST_MODE = !!args.test;
const SEGMENT = (args.segment || '').toUpperCase();
const SELECTED_STATES = (args.state || '').toUpperCase().split(',').filter(Boolean);
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR,
  SEGMENT ? `us-realtors-segment-${SEGMENT}.csv` : 'us-realtors.csv'
);

// ── Segment letter ranges ───────────────────────────────────────────────
const SEGMENTS = {
  'A-F': 'ABCDEF',
  'G-L': 'GHIJKL',
  'M-R': 'MNOPQR',
  'S-Z': 'STUVWXYZ',
};

function getLetterRange() {
  if (TEST_MODE) return ['S'];
  if (SEGMENT && SEGMENTS[SEGMENT]) return SEGMENTS[SEGMENT].split('');
  return 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
}

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'license_number',
];

// ── Helpers ─────────────────────────────────────────────────────────────
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|\s|[-'])\w/g, c => c.toUpperCase());
}

function normalizePhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return phone;
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

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
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

function loadExistingLeads() {
  if (!fs.existsSync(PROGRESS_FILE)) return { leads: [], seenKeys: new Set() };
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return { leads: [], seenKeys: new Set() };
    const headers = lines[0].split(',');
    const licIdx = headers.indexOf('license_number');
    const fnIdx = headers.indexOf('first_name');
    const lnIdx = headers.indexOf('last_name');
    const cityIdx = headers.indexOf('city');
    const stIdx = headers.indexOf('state');
    const leads = [];
    const seenKeys = new Set();
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
      const key = dedupKey(
        vals[licIdx] || '',
        vals[fnIdx] || '',
        vals[lnIdx] || '',
        vals[cityIdx] || '',
        vals[stIdx] || ''
      );
      if (key) seenKeys.add(key);
    }
    return { leads, seenKeys };
  } catch {
    return { leads: [], seenKeys: new Set() };
  }
}

function dedupKey(licenseNumber, firstName, lastName, city, state) {
  if (licenseNumber) return `lic:${licenseNumber}`;
  if (firstName && lastName && city) {
    return `name:${firstName.toLowerCase()}|${lastName.toLowerCase()}|${city.toLowerCase()}|${state.toLowerCase()}`;
  }
  return '';
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── HTTP helpers (no dependencies) ──────────────────────────────────────
function httpRequest(urlStr, options = {}) {
  // Enforce max redirects
  if ((options._redirectCount || 0) > 5) {
    return Promise.reject(new Error(`Too many redirects: ${urlStr}`));
  }

  return new Promise((resolve, reject) => {
    const parsed = new URL(urlStr);
    // Always upgrade http to https to avoid mixed-content issues (NY DOS redirects to http://)
    const useHttps = true;
    const mod = useHttps ? https : http;
    const actualUrl = urlStr.replace(/^http:\/\//, 'https://');
    const actualParsed = new URL(actualUrl);

    const reqOptions = {
      hostname: actualParsed.hostname,
      port: actualParsed.port || 443,
      path: actualParsed.pathname + actualParsed.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(options.headers || {}),
      },
    };

    if (options.cookies) {
      reqOptions.headers['Cookie'] = options.cookies;
    }

    const req = mod.request(reqOptions, (res) => {
      // Collect Set-Cookie headers
      const setCookies = res.headers['set-cookie'] || [];
      const cookieStr = setCookies.map(c => c.split(';')[0]).join('; ');

      // Follow redirects (up to 5)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = new URL(res.headers.location, actualUrl).href;
        const mergedCookies = [options.cookies, cookieStr].filter(Boolean).join('; ');
        res.resume(); // Discard body
        // Clean redirect: GET only, no body, no Content-Length/Content-Type
        const redirectHeaders = { ...(options.headers || {}) };
        delete redirectHeaders['Content-Length'];
        delete redirectHeaders['Content-Type'];
        httpRequest(redirectUrl, {
          method: 'GET',
          headers: redirectHeaders,
          cookies: mergedCookies,
          _redirectCount: (options._redirectCount || 0) + 1,
        }).then(resolve).catch(reject);
        return;
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({
        status: res.statusCode,
        body: data,
        cookies: cookieStr,
        headers: res.headers,
      }));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error(`Timeout: ${urlStr}`));
    });

    if (options.body) {
      if (reqOptions.method === 'GET') reqOptions.method = 'POST';
      req.write(options.body);
    }
    req.end();
  });
}

function httpPost(urlStr, body, extraHeaders = {}, cookies = '') {
  return httpRequest(urlStr, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body).toString(),
      ...extraHeaders,
    },
    body,
    cookies,
  });
}

// ── HTML entity decode ──────────────────────────────────────────────────
function decodeEntities(str) {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ');
}

function stripTags(html) {
  return html
    .replace(/<!--[\s\S]*?-->/g, '')  // Remove HTML comments first
    .replace(/<[^>]*>/g, '');
}

// ═══════════════════════════════════════════════════════════════════════
//  SOURCE: NY DOS (New York Department of State)
//  URL: https://appext20.dos.ny.gov/nydos/
//
//  Method: POST search by last name prefix (min 2 chars) + first name
//  prefix (min 2 chars). Returns HTML table with name, license number,
//  license type, status, expiry date. Detail page has address, firm.
//
//  No CAPTCHA. No API key. 30 results per page. Session cookies needed.
// ═══════════════════════════════════════════════════════════════════════

const NY_BASE = 'https://appext20.dos.ny.gov/nydos';
const NY_FIRST_PREFIXES = TEST_MODE
  ? ['Jo', 'Ma']
  : [
    'Ab', 'Ad', 'Ah', 'Ai', 'Al', 'Am', 'An', 'Ar', 'As', 'Au', 'Av',
    'Ba', 'Be', 'Bi', 'Bl', 'Bo', 'Br', 'Bu',
    'Ca', 'Ce', 'Ch', 'Ci', 'Cl', 'Co', 'Cr', 'Cu', 'Cy',
    'Da', 'De', 'Di', 'Do', 'Dr', 'Du', 'Dy',
    'Ed', 'Ei', 'El', 'Em', 'En', 'Er', 'Es', 'Et', 'Eu', 'Ev',
    'Fa', 'Fe', 'Fi', 'Fl', 'Fr', 'Fu',
    'Ga', 'Ge', 'Gi', 'Gl', 'Go', 'Gr', 'Gu',
    'Ha', 'He', 'Hi', 'Ho', 'Hu', 'Hy',
    'Ia', 'Ib', 'Id', 'Ig', 'Il', 'Im', 'In', 'Ir', 'Is', 'Iv',
    'Ja', 'Je', 'Ji', 'Jo', 'Ju',
    'Ka', 'Ke', 'Ki', 'Ko', 'Kr', 'Ku', 'Ky',
    'La', 'Le', 'Li', 'Lo', 'Lu', 'Ly',
    'Ma', 'Me', 'Mi', 'Mo', 'Mu', 'My',
    'Na', 'Ne', 'Ni', 'No', 'Nu',
    'Ob', 'Oc', 'Ol', 'Om', 'Or', 'Os', 'Ow',
    'Pa', 'Pe', 'Ph', 'Pi', 'Po', 'Pr', 'Pu',
    'Qu',
    'Ra', 'Re', 'Rh', 'Ri', 'Ro', 'Ru', 'Ry',
    'Sa', 'Sc', 'Se', 'Sh', 'Si', 'Sk', 'Sl', 'Sm', 'Sn', 'So', 'Sp', 'St', 'Su', 'Sv', 'Sw', 'Sy',
    'Ta', 'Te', 'Th', 'Ti', 'To', 'Tr', 'Tu', 'Ty',
    'Um', 'Un', 'Ur', 'Us',
    'Va', 'Ve', 'Vi', 'Vo', 'Vu',
    'Wa', 'We', 'Wh', 'Wi', 'Wo', 'Wy',
    'Xa', 'Xi',
    'Ya', 'Ye', 'Yi', 'Yo', 'Yu',
    'Za', 'Ze', 'Zi', 'Zo', 'Zu',
  ];

async function nyGetSession() {
  const res = await httpRequest(`${NY_BASE}/selSearchType.do`);
  return res.cookies || '';
}

async function nySearchByName(lastName, firstName, cookies) {
  const body = [
    'searchType=name',
    'indOrgInd=I',
    `surname=${encodeURIComponent(lastName)}`,
    `firstName=${encodeURIComponent(firstName)}`,
    'organizationName=',
    'pageSize=30',
    'search=Search',
  ].join('&');

  const res = await httpPost(`${NY_BASE}/searchByName.do`, body, {
    'Referer': `${NY_BASE}/searchByName.do`,
  }, cookies);

  // If redirected, the body is from the redirect target
  return { html: res.body, cookies: res.cookies || cookies };
}

async function nyGetDetail(anchor, cookies) {
  const res = await httpRequest(`${NY_BASE}/details.do?anchor=${anchor}`, {
    cookies,
    headers: { 'Referer': `${NY_BASE}/searchByName.do` },
  });
  return res.body;
}

function nyParseListPage(html) {
  const agents = [];

  // Match rows: <tr class="itemRow"> or <tr class="itemRowAlt">
  const rowRegex = /<tr class="(?:itemRow|itemRowAlt)">([\s\S]*?)<\/tr>/g;
  let match;

  while ((match = rowRegex.exec(html)) !== null) {
    const row = match[1];

    // Extract cells
    const cellRegex = /<td[^>]*>\s*<span class="item">([\s\S]*?)<\/span>\s*<\/td>/g;
    const cells = [];
    let cellMatch;
    while ((cellMatch = cellRegex.exec(row)) !== null) {
      cells.push(stripTags(decodeEntities(cellMatch[1])).replace(/\s+/g, ' ').trim());
    }

    if (cells.length < 4) continue;

    // Extract anchor from detail link
    const anchorMatch = row.match(/href="details\.do\?anchor=([^"]+)"/);
    const anchor = anchorMatch ? anchorMatch[1] : '';

    // cells: [name, license_number, license_type, status, expiry_date]
    const fullName = cells[0];
    const licenseNumber = cells[1];
    const licenseType = cells[2] || '';
    const status = cells[3] || '';

    // Only keep Current licenses
    if (!status.toLowerCase().includes('current')) continue;

    // Only keep real estate license types
    const isRealEstate = /salesperson|broker/i.test(licenseType);
    if (!isRealEstate) continue;

    // Parse name
    const nameParts = fullName.trim().split(/\s+/).filter(Boolean);
    let firstName = '', lastName = '';
    if (nameParts.length === 1) {
      lastName = nameParts[0];
    } else if (nameParts.length === 2) {
      firstName = nameParts[0];
      lastName = nameParts[1];
    } else {
      firstName = nameParts[0];
      lastName = nameParts[nameParts.length - 1];
    }

    // Title from license type
    let title = 'Real Estate Agent';
    if (/salesperson/i.test(licenseType)) title = 'Real Estate Salesperson';
    else if (/associate.*broker/i.test(licenseType)) title = 'Associate Real Estate Broker';
    else if (/corporate.*broker|corp.*broker/i.test(licenseType)) title = 'Corporate Real Estate Broker';
    else if (/broker/i.test(licenseType)) title = 'Real Estate Broker';

    agents.push({
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      license_number: licenseNumber,
      title,
      anchor,
      _raw_name: fullName,
    });
  }

  return agents;
}

function nyParseDetailPage(html) {
  const info = { city: '', county: '', address: '', zip: '', firm_name: '' };

  // Extract address from nested table
  // Pattern: Practice Locate or Mailing address section
  const addressSection = html.match(
    /Practice Locate[\s\S]*?<table>([\s\S]*?)<\/table>/i
  ) || html.match(
    /Address:[\s\S]*?<table>([\s\S]*?)<\/table>/i
  );

  if (addressSection) {
    const cells = [];
    const cellRegex = /<td[^>]*>\s*<span class="item">([\s\S]*?)<\/span>\s*<\/td>/g;
    let cm;
    while ((cm = cellRegex.exec(addressSection[1])) !== null) {
      cells.push(stripTags(decodeEntities(cm[1])).replace(/\s+/g, ' ').trim());
    }

    // cells typically: [street, suite/apt, city+state, county, zip, country]
    for (const cell of cells) {
      const cityState = cell.match(/^([A-Za-z\s.'-]+)\s*,\s*NY\b/);
      if (cityState) {
        info.city = titleCase(cityState[1].trim());
      }
      // County
      if (/^[A-Z]{2,}$/.test(cell.trim()) && cell.trim().length > 2 && cell.trim().length < 20) {
        info.county = titleCase(cell.trim());
      }
      // Zip code
      if (/^\d{5}(-\d{4})?$/.test(cell.trim())) {
        info.zip = cell.trim();
      }
    }
  }

  // Extract firm name from Related Party table
  const relatedParty = html.match(
    /Related Party Name[\s\S]*?<td class="itemCell"[^>]*>\s*<div class="item">\s*([\s\S]*?)\s*<\/div>/i
  );
  if (relatedParty) {
    info.firm_name = stripTags(decodeEntities(relatedParty[1])).replace(/\s+/g, ' ').trim();
  }

  return info;
}

async function *nyScrape(letterRange) {
  console.log('  [NY] Initializing session...');
  let cookies = await nyGetSession();
  let requestCount = 0;
  let detailsFetched = 0;
  const MAX_DETAILS_PER_SEARCH = TEST_MODE ? 2 : 5; // Limit detail page fetches

  // Build last name prefixes from letter range
  const lastPrefixes = [];
  for (const letter of letterRange) {
    if (TEST_MODE) {
      lastPrefixes.push(letter.toLowerCase() + 'm');
    } else {
      for (const second of 'abcdefghijklmnopqrstuvwxyz') {
        lastPrefixes.push(letter.toLowerCase() + second);
      }
    }
  }

  const firstPrefixes = TEST_MODE ? ['Jo'] : NY_FIRST_PREFIXES;
  const totalCombinations = lastPrefixes.length * firstPrefixes.length;

  console.log(`  [NY] ${lastPrefixes.length} last name prefixes x ${firstPrefixes.length} first name prefixes = ${totalCombinations} combinations`);

  let combinationIndex = 0;
  for (const lastPrefix of lastPrefixes) {
    for (const firstPrefix of firstPrefixes) {
      combinationIndex++;
      requestCount++;

      // Refresh session every 200 requests
      if (requestCount % 200 === 0) {
        console.log('  [NY] Refreshing session...');
        cookies = await nyGetSession();
        await sleep(DELAY_MS);
      }

      try {
        const { html, cookies: newCookies } = await nySearchByName(lastPrefix, firstPrefix, cookies);
        if (newCookies) cookies = newCookies;

        const agents = nyParseListPage(html);

        if (agents.length > 0) {
          let detailCount = 0;
          for (const agent of agents) {
            // Fetch detail page for address/firm (rate-limited, capped per search)
            if (agent.anchor && detailCount < MAX_DETAILS_PER_SEARCH) {
              try {
                await sleep(DELAY_MS);
                const detailHtml = await nyGetDetail(agent.anchor, cookies);
                const detail = nyParseDetailPage(detailHtml);
                agent.city = detail.city;
                agent.firm_name = detail.firm_name;
                agent.zip = detail.zip;
                detailCount++;
                detailsFetched++;
              } catch (detailErr) {
                // Skip detail errors silently — still yield the agent without detail
              }
            }

            yield {
              first_name: agent.first_name,
              last_name: agent.last_name,
              firm_name: agent.firm_name || '',
              title: agent.title,
              email: '',
              phone: '',
              website: '',
              domain: '',
              city: agent.city || '',
              state: 'NY',
              country: 'US',
              niche: 'real estate agent',
              source: 'ny-dos',
              license_number: agent.license_number,
            };
          }
        }

        if (combinationIndex % 50 === 0 || agents.length > 0) {
          const pct = ((combinationIndex / totalCombinations) * 100).toFixed(1);
          console.log(`  [NY] [${combinationIndex}/${totalCombinations}] "${lastPrefix}" + "${firstPrefix}" -> ${agents.length} agents (${pct}%) [${detailsFetched} details]`);
        }

      } catch (err) {
        console.log(`  [NY] [ERR] "${lastPrefix}" + "${firstPrefix}": ${err.message}`);
        // On error, try refreshing session
        try {
          cookies = await nyGetSession();
        } catch (e) {
          // ignore
        }
      }

      await sleep(DELAY_MS);
    }
  }
}


// ═══════════════════════════════════════════════════════════════════════
//  SOURCE: FL DBPR (Florida Dept of Business & Professional Regulation)
//  URL: https://www.myfloridalicense.com/wl11.asp
//
//  Method: Multi-step ASP form. Session cookie (ASPSESSIONID) needed.
//  Board 25 = Real Estate. Search by last name + first name prefix.
//
//  Returns HTML table with name, license number, address, county.
// ═══════════════════════════════════════════════════════════════════════

const FL_BASE = 'https://www.myfloridalicense.com';

async function flGetSession() {
  // Step 1: Hit mode=0 to get session cookie
  const res0 = await httpRequest(`${FL_BASE}/wl11.asp?mode=0&SID=&brd=25&typ=N`);
  return res0.cookies || '';
}

async function flSearch(lastName, firstName, cookies) {
  // Step 1: Navigate to search form (mode=1)
  const res1 = await httpRequest(
    `${FL_BASE}/wl11.asp?mode=1&search=Name&SID=&brd=25&typ=N`,
    { cookies }
  );
  const sessionCookies = [cookies, res1.cookies].filter(Boolean).join('; ');
  await sleep(200);

  // Step 2: POST the search form (mode=2)
  const formData = [
    'hSID=',
    'hSearchType=Name',
    'hLastName=',
    'hFirstName=',
    'hMiddleName=',
    'hOrgName=',
    'hSearchOpt=',
    'hSearchOpt2=',
    'hSearchAltName=',
    'hSearchPartName=',
    'hSearchFuzzy=',
    'hDivision=ALL',
    'hBoard=',
    'hLicenseType=',
    'hSpecQual=',
    'hAddrType=',
    'hCity=',
    'hCounty=',
    'hState=',
    'hLicNbr=',
    'hCurrPage=',
    'hTotalPages=',
    'hTotalRecords=',
    'hBoardType=25',
    'hDDChange=',
    'hPageAction=',
    `LastName=${encodeURIComponent(lastName)}`,
    `FirstName=${encodeURIComponent(firstName)}`,
    'MiddleName=',
    'OrgName=',
    'LicNbr=',
    'AddrType=N',
    'Address=',
    'City=',
    'County=',
    'State=',
    'Zip=',
    'Ph1area=',
    'Ph1nbr=',
    'Status=CUR',
    'LicType=',
    'LicenseType=',
  ].join('&');

  const res2 = await httpPost(
    `${FL_BASE}/wl11.asp?mode=2&search=Name&SID=&brd=25&typ=N`,
    formData,
    { 'Referer': `${FL_BASE}/wl11.asp?mode=1&search=Name&SID=&brd=25&typ=N` },
    sessionCookies
  );

  return { html: res2.body, cookies: [sessionCookies, res2.cookies].filter(Boolean).join('; ') };
}

function flParseResults(html) {
  const agents = [];

  // Florida uses a table with class="list" for results
  // Each result row has: name (link), license #, license type, status, address, county
  const rowRegex = /<tr[^>]*class="(?:list|listalt|listAlt)"[^>]*>([\s\S]*?)<\/tr>/gi;
  let match;

  while ((match = rowRegex.exec(html)) !== null) {
    const row = match[1];
    const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    const cells = [];
    let cellMatch;
    while ((cellMatch = cellRegex.exec(row)) !== null) {
      cells.push(stripTags(decodeEntities(cellMatch[1])).replace(/\s+/g, ' ').trim());
    }

    if (cells.length < 3) continue;

    // Parse the result data
    // Typical columns: Name, License#, LicenseType, Status, ExpiryDate, PrimaryAddress, County
    const fullName = cells[0] || '';
    const licenseNumber = cells[1] || '';
    const licenseType = cells[2] || '';
    const status = cells[3] || '';

    // Only current, real estate licenses
    if (!status.toLowerCase().includes('cur')) continue;
    if (!/real estate|sales assoc|broker/i.test(licenseType)) continue;

    const nameParts = fullName.split(/,\s*/);
    let firstName = '', lastName = '';
    if (nameParts.length >= 2) {
      lastName = nameParts[0].trim();
      firstName = nameParts[1].trim().split(/\s+/)[0]; // First word of remaining
    } else {
      const parts = fullName.trim().split(/\s+/);
      firstName = parts[0] || '';
      lastName = parts[parts.length - 1] || '';
    }

    let title = 'Real Estate Agent';
    if (/sales/i.test(licenseType)) title = 'Real Estate Sales Associate';
    else if (/broker/i.test(licenseType)) title = 'Real Estate Broker';

    const city = cells.length > 5 ? titleCase(cells[5] || '') : '';
    const county = cells.length > 6 ? titleCase(cells[6] || '') : '';

    agents.push({
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      firm_name: '',
      title,
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: city || county,
      state: 'FL',
      country: 'US',
      niche: 'real estate agent',
      source: 'fl-dbpr',
      license_number: licenseNumber,
    });
  }

  return agents;
}

async function *flScrape(letterRange) {
  console.log('  [FL] Initializing session...');
  let cookies;
  try {
    cookies = await flGetSession();
  } catch (err) {
    console.log(`  [FL] Failed to get session: ${err.message}`);
    console.log('  [FL] Skipping Florida (session error)');
    return;
  }

  const lastPrefixes = [];
  for (const letter of letterRange) {
    if (TEST_MODE) {
      lastPrefixes.push(letter + 'mi');
    } else {
      for (const second of 'abcdefghijklmnopqrstuvwxyz') {
        lastPrefixes.push(letter + second);
      }
    }
  }

  // Florida requires first name to be blank or at least 1 char; we use just last name
  const totalPrefixes = lastPrefixes.length;
  console.log(`  [FL] ${totalPrefixes} last name prefixes to search`);

  for (let i = 0; i < lastPrefixes.length; i++) {
    const prefix = lastPrefixes[i];

    try {
      const { html, cookies: newCookies } = await flSearch(prefix, '', cookies);
      if (newCookies) cookies = newCookies;

      const agents = flParseResults(html);

      for (const agent of agents) {
        yield agent;
      }

      if (agents.length > 0 || (i + 1) % 20 === 0) {
        const pct = (((i + 1) / totalPrefixes) * 100).toFixed(1);
        console.log(`  [FL] [${i + 1}/${totalPrefixes}] "${prefix}" -> ${agents.length} agents (${pct}%)`);
      }

    } catch (err) {
      console.log(`  [FL] [ERR] "${prefix}": ${err.message}`);
      // Re-init session on error
      try {
        cookies = await flGetSession();
      } catch (e) { /* ignore */ }
    }

    await sleep(DELAY_MS);
  }
}


// ═══════════════════════════════════════════════════════════════════════
//  SOURCE: TX TREC (Texas Real Estate Commission)
//  URL: https://www.trec.texas.gov/license-search/
//
//  Method: Typesense search API (reverse-engineered from the Vue.js SPA).
//  Collection: "licenses", searchable fields: lastName, firstName.
//  Returns JSON with firstName, lastName, organizationName, type,
//  status, customId (license number), county, renewalInfo.
//
//  No CAPTCHA. Public API key. Pagination via page param.
// ═══════════════════════════════════════════════════════════════════════

const TX_TYPESENSE_URL = 'https://www.trec.texas.gov/ts/collections/licenses/documents/search';
const TX_API_KEY = 'HvqEl9eBZY6YjQBAU8uW4e9KBGHRvqrd';
const TX_PER_PAGE = 250; // Max per page for Typesense

async function txTypesenseSearch(query, page) {
  const params = new URLSearchParams({
    q: query,
    query_by: 'lastName,firstName',
    sort_by: '_text_match:desc,lastName:asc,firstName:asc',
    filter_by: 'status.value:!=Inactive&&type.subType:[Salesperson,Broker Individual,Broker Business Entity]',
    per_page: TX_PER_PAGE.toString(),
    page: page.toString(),
    include_fields: 'firstName,middleName,lastName,organizationName,customId,type,status,county,renewalInfo,detailId',
  });

  const url = `${TX_TYPESENSE_URL}?${params.toString()}`;
  const res = await httpRequest(url, {
    headers: {
      'X-TYPESENSE-API-KEY': TX_API_KEY,
      'Accept': 'application/json',
    },
  });

  if (res.status !== 200) {
    throw new Error(`TX Typesense HTTP ${res.status}`);
  }

  return JSON.parse(res.body);
}

function txParseHits(data) {
  const agents = [];
  const hits = data.hits || [];

  for (const hit of hits) {
    const doc = hit.document;
    if (!doc) continue;

    const firstName = (doc.firstName || '').trim();
    const lastName = (doc.lastName || '').trim();
    const licenseNumber = (doc.customId || '').trim();
    const orgName = (doc.organizationName || '').trim();
    const typeSubType = (doc.type && doc.type.subType) || '';
    const county = (doc.county || '').trim();

    if (!firstName && !lastName) continue;

    let title = 'Real Estate Agent';
    if (/salesperson/i.test(typeSubType)) title = 'Real Estate Sales Agent';
    else if (/broker.*individual/i.test(typeSubType)) title = 'Real Estate Broker';
    else if (/broker.*business/i.test(typeSubType)) title = 'Real Estate Broker (Business Entity)';

    // Filter out self-referential org names (agent's own name, not a firm)
    // TREC org names are often just "LASTNAME, FIRSTNAME MIDDLENAME" or "FIRSTNAME MIDDLENAME LASTNAME"
    const orgNorm = orgName.toLowerCase().replace(/[-]/g, ' ').trim();
    const fNorm = firstName.toLowerCase().replace(/[-]/g, ' ').trim();
    const lNorm = lastName.toLowerCase().replace(/[-]/g, ' ').trim();
    const isSelfRef =
      orgNorm.startsWith(`${lNorm}, ${fNorm}`) ||
      (orgNorm.startsWith(`${fNorm} `) && orgNorm.includes(lNorm)) ||
      orgNorm === `${fNorm} ${lNorm}` ||
      orgNorm === `${lNorm}, ${fNorm}`;

    agents.push({
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      firm_name: isSelfRef ? '' : titleCase(orgName),
      title,
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: titleCase(county), // TREC only provides county, not city
      state: 'TX',
      country: 'US',
      niche: 'real estate agent',
      source: 'tx-trec',
      license_number: licenseNumber,
    });
  }

  return agents;
}

async function *txScrape(letterRange) {
  console.log('  [TX] Initializing Typesense search...');

  const lastPrefixes = [];
  for (const letter of letterRange) {
    if (TEST_MODE) {
      lastPrefixes.push(letter.toLowerCase() + 'm');
    } else {
      for (const second of 'abcdefghijklmnopqrstuvwxyz') {
        lastPrefixes.push(letter.toLowerCase() + second);
      }
    }
  }

  const totalPrefixes = lastPrefixes.length;
  console.log(`  [TX] ${totalPrefixes} last name prefixes to search`);

  for (let i = 0; i < lastPrefixes.length; i++) {
    const prefix = lastPrefixes[i];

    try {
      // First page
      const data = await txTypesenseSearch(prefix, 1);
      const totalFound = data.found || 0;
      const agents = txParseHits(data);

      for (const agent of agents) {
        yield agent;
      }

      // Paginate if needed (up to 10 pages to avoid excessive requests)
      const maxPages = TEST_MODE ? 1 : Math.min(Math.ceil(totalFound / TX_PER_PAGE), 10);
      for (let page = 2; page <= maxPages; page++) {
        await sleep(DELAY_MS);
        const pageData = await txTypesenseSearch(prefix, page);
        const pageAgents = txParseHits(pageData);
        for (const agent of pageAgents) {
          yield agent;
        }
      }

      if (agents.length > 0 || (i + 1) % 20 === 0) {
        const pct = (((i + 1) / totalPrefixes) * 100).toFixed(1);
        console.log(`  [TX] [${i + 1}/${totalPrefixes}] "${prefix}" -> ${totalFound} found, ${agents.length} on page 1 (${pct}%)`);
      }

    } catch (err) {
      console.log(`  [TX] [ERR] "${prefix}": ${err.message}`);
    }

    await sleep(DELAY_MS);
  }
}


// ═══════════════════════════════════════════════════════════════════════
//  SOURCE: OH ELICENSE (Ohio Dept of Commerce)
//  URL: https://elicense.ohio.gov/oh_verifylicense
//
//  Method: GET search with query params. Board = Real Estate.
// ═══════════════════════════════════════════════════════════════════════

async function *ohScrape(letterRange) {
  console.log('  [OH] Ohio eLicense search...');

  const lastPrefixes = [];
  for (const letter of letterRange) {
    if (TEST_MODE) {
      lastPrefixes.push(letter + 'mi');
    } else {
      for (const second of 'abcdefghijklmnopqrstuvwxyz') {
        lastPrefixes.push(letter + second);
      }
    }
  }

  const totalPrefixes = lastPrefixes.length;
  console.log(`  [OH] ${totalPrefixes} last name prefixes to search`);

  for (let i = 0; i < lastPrefixes.length; i++) {
    const prefix = lastPrefixes[i];

    try {
      const params = new URLSearchParams({
        lastName: prefix,
        firstName: '',
        licenseNumber: '',
        board: 'Real Estate &amp; Professional',
        licenseType: '',
        city: '',
        state: 'OH',
        zip: '',
        county: '',
      });

      const url = `https://elicense.ohio.gov/oh_verifylicense?${params.toString()}`;
      const res = await httpRequest(url);
      const html = res.body;

      // Parse Ohio results (similar table format)
      const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
      let match;

      while ((match = rowRegex.exec(html)) !== null) {
        const row = match[1];
        if (row.includes('<th')) continue;

        const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
        const cells = [];
        let cellMatch;
        while ((cellMatch = cellRegex.exec(row)) !== null) {
          cells.push(stripTags(decodeEntities(cellMatch[1])).replace(/\s+/g, ' ').trim());
        }

        if (cells.length < 3) continue;

        const fullName = cells[0] || '';
        const licenseNumber = cells[1] || '';
        const licenseType = cells[2] || '';

        if (!/sales|broker|real estate/i.test(licenseType)) continue;

        const nameParts = fullName.split(/,\s*/);
        let firstName = '', lastName = '';
        if (nameParts.length >= 2) {
          lastName = nameParts[0].trim();
          firstName = nameParts[1].trim().split(/\s+/)[0];
        }

        yield {
          first_name: titleCase(firstName),
          last_name: titleCase(lastName),
          firm_name: '',
          title: /broker/i.test(licenseType) ? 'Real Estate Broker' : 'Real Estate Salesperson',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: cells.length > 4 ? titleCase(cells[4] || '') : '',
          state: 'OH',
          country: 'US',
          niche: 'real estate agent',
          source: 'oh-elicense',
          license_number: licenseNumber,
        };
      }

      if ((i + 1) % 20 === 0) {
        const pct = (((i + 1) / totalPrefixes) * 100).toFixed(1);
        console.log(`  [OH] [${i + 1}/${totalPrefixes}] "${prefix}" (${pct}%)`);
      }

    } catch (err) {
      console.log(`  [OH] [ERR] "${prefix}": ${err.message}`);
    }

    await sleep(DELAY_MS);
  }
}


// ═══════════════════════════════════════════════════════════════════════
//  MAIN SCRAPER ENGINE
// ═══════════════════════════════════════════════════════════════════════

// Map of state codes to scraper generators
// FL: Florida DBPR ASP session management rejects automated requests — disabled
// OH: Ohio eLicense needs verification — disabled for now
const STATE_SCRAPERS = {
  'NY': { name: 'New York (DOS)', scraper: nyScrape },
  'TX': { name: 'Texas (TREC)', scraper: txScrape },
  // 'FL': { name: 'Florida (DBPR)', scraper: flScrape },
  // 'OH': { name: 'Ohio (eLicense)', scraper: ohScrape },
};

async function main() {
  const startTime = Date.now();
  const letterRange = getLetterRange();

  // Load existing progress for resume
  let { leads: allLeads, seenKeys } = args.resume
    ? loadExistingLeads()
    : { leads: [], seenKeys: new Set() };

  if (args.resume && allLeads.length > 0) {
    console.log(`  Resuming with ${allLeads.length} existing leads (${seenKeys.size} unique keys)`);
  }

  // Determine which states to scrape
  const statesToScrape = SELECTED_STATES.length > 0
    ? SELECTED_STATES.filter(s => STATE_SCRAPERS[s])
    : Object.keys(STATE_SCRAPERS);

  if (SELECTED_STATES.length > 0) {
    const unknown = SELECTED_STATES.filter(s => !STATE_SCRAPERS[s]);
    if (unknown.length > 0) {
      console.log(`  Warning: Unknown states ignored: ${unknown.join(', ')}`);
      console.log(`  Available: ${Object.keys(STATE_SCRAPERS).join(', ')}`);
    }
  }

  const segmentLabel = SEGMENT || 'ALL';
  const modeLabel = TEST_MODE ? ' (TEST MODE)' : '';

  console.log('');
  console.log('=============================================================');
  console.log(`  MORTAR -- US Real Estate Agent Scraper${modeLabel}`);
  console.log('=============================================================');
  console.log('');
  console.log(`  States:      ${statesToScrape.map(s => `${s} (${STATE_SCRAPERS[s].name})`).join(', ')}`);
  console.log(`  Segment:     ${segmentLabel}`);
  console.log(`  Letters:     ${letterRange.join('')}`);
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Output:      ${PROGRESS_FILE}`);
  console.log('');

  let newLeads = 0;
  let duplicates = 0;
  let errors = 0;
  let lastSaveCount = 0;
  const stateCounts = {};

  for (const stateCode of statesToScrape) {
    const { name, scraper } = STATE_SCRAPERS[stateCode];
    console.log(`\n  ── Scraping ${stateCode}: ${name} ──────────────────────────`);
    stateCounts[stateCode] = 0;

    try {
      const gen = scraper(letterRange);
      for await (const lead of gen) {
        const key = dedupKey(lead.license_number, lead.first_name, lead.last_name, lead.city, lead.state);
        if (key && seenKeys.has(key)) {
          duplicates++;
          continue;
        }
        if (key) seenKeys.add(key);

        allLeads.push(lead);
        newLeads++;
        stateCounts[stateCode]++;

        // Save progress every 500 new leads
        if (newLeads > 0 && newLeads - lastSaveCount >= 500) {
          writeCSV(PROGRESS_FILE, allLeads);
          lastSaveCount = newLeads;
          console.log(`  [SAVE] ${allLeads.length} leads saved to ${path.basename(PROGRESS_FILE)}`);
        }
      }
    } catch (err) {
      errors++;
      console.log(`  [ERR] ${stateCode} scraper failed: ${err.message}`);
    }

    console.log(`  [${stateCode}] Done. ${stateCounts[stateCode]} agents collected.`);
  }

  // ── Final save ──────────────────────────────────────────────────────
  if (allLeads.length > 0) {
    writeCSV(PROGRESS_FILE, allLeads);

    // Also save timestamped final file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const finalFile = `us-realtors_${timestamp}.csv`;
    const finalPath = path.join(OUTPUT_DIR, finalFile);
    writeCSV(finalPath, allLeads);
    console.log(`\n  Final file: ${finalPath}`);
  }

  // ── Summary ─────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const uniqueFirms = new Set(allLeads.filter(l => l.firm_name).map(l => l.firm_name)).size;
  const uniqueCities = new Set(allLeads.filter(l => l.city).map(l => l.city)).size;
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const brokers = allLeads.filter(l => /broker/i.test(l.title)).length;
  const salespersons = allLeads.filter(l => /sales/i.test(l.title)).length;

  console.log('');
  console.log('=============================================================');
  console.log('  US REALTOR SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  States:        ${statesToScrape.join(', ')}`);
  console.log(`  Segment:       ${segmentLabel}`);
  for (const [st, count] of Object.entries(stateCounts)) {
    console.log(`    ${st}:           ${count}`);
  }
  console.log(`  Total agents:  ${allLeads.length}`);
  console.log(`  Salespersons:  ${salespersons}`);
  console.log(`  Brokers:       ${brokers}`);
  console.log(`  With email:    ${withEmail} (${Math.round(withEmail / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  With phone:    ${withPhone} (${Math.round(withPhone / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  Unique firms:  ${uniqueFirms}`);
  console.log(`  Unique cities: ${uniqueCities}`);
  console.log(`  Duplicates:    ${duplicates}`);
  console.log(`  Errors:        ${errors}`);
  console.log(`  Time:          ${elapsed}s (${Math.round(elapsed / 60)}min)`);
  console.log(`  Output:        ${PROGRESS_FILE}`);
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
