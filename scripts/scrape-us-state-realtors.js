#!/usr/bin/env node
/**
 * US State Real Estate Agent License Scraper
 *
 * Scrapes real estate agent/broker licences from US state regulatory databases.
 * Uses only Node.js built-in modules (https, http, fs, path, querystring).
 *
 * Working scrapers:
 *   CA  — California DRE (Department of Real Estate)
 *         POST form search, returns all matches in one page.
 *         Fields: name, license_type, city, license_id, status (via detail page)
 *
 *   NY  — New York DOS (Department of State)
 *         POST form search with session cookies. Requires 2+ char last name, 2+ char first name.
 *         Fields: name, license_number, license_type, status, expiry_date
 *         License types: Salesperson, Associate Broker, Individual Broker, Corporate Broker, etc.
 *
 *   GA  — Georgia GREC (Real Estate Commission)
 *         POST form search with CSRF token. Results limited to 50 per search.
 *         Fields: name, license_number, license_type (SLSP/BRKR), status, firm_name
 *
 * Placeholder scrapers (need Puppeteer — session/JS dependent):
 *   FL  — Florida DBPR (ASP session required, does not work with plain HTTP)
 *   TX  — Texas TREC (Drupal JS-rendered, needs browser execution)
 *
 * Usage:
 *   node scripts/scrape-us-state-realtors.js                  # Run all working states
 *   node scripts/scrape-us-state-realtors.js --state=CA       # Run California only
 *   node scripts/scrape-us-state-realtors.js --state=NY       # Run New York only
 *   node scripts/scrape-us-state-realtors.js --state=GA       # Run Georgia only
 *   node scripts/scrape-us-state-realtors.js --test           # Test mode (small sample)
 *   node scripts/scrape-us-state-realtors.js --state=CA --test
 *
 * Output: output/us-realtors-{state}.csv
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

// ── CLI args ────────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const STATE_FILTER = (args.state || '').toUpperCase();
const TEST_MODE = !!args.test;
const DELAY_MS = parseInt(args.delay) || 500;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');

// ── CSV columns ─────────────────────────────────────────────────────────────
const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source'
];

// ── Utilities ───────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let clean = fullName.replace(/\s*\([^)]*\)\s*/g, ' ').trim().replace(/\s+/g, ' ');
  // Handle "Last, First Middle" format
  if (clean.includes(',')) {
    const parts = clean.split(',').map(s => s.trim());
    const last = parts[0];
    const firstParts = (parts[1] || '').split(' ');
    return { first_name: firstParts[0] || '', last_name: last };
  }
  const parts = clean.split(' ');
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return { first_name: parts.slice(0, -1).join(' '), last_name: parts[parts.length - 1] };
}

function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&nbsp;/gi, ' ');
}

function toCsvRow(lead) {
  return CSV_HEADERS.map(h => escapeCsv(lead[h] || '')).join(',');
}

function writeCsv(filePath, leads) {
  const rows = [CSV_HEADERS.join(','), ...leads.map(toCsvRow)];
  fs.writeFileSync(filePath, rows.join('\n') + '\n');
}

// ── HTTP helpers ────────────────────────────────────────────────────────────
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(options.headers || {}),
      },
    };

    if (options.body) {
      reqOptions.headers['Content-Type'] = 'application/x-www-form-urlencoded';
      reqOptions.headers['Content-Length'] = Buffer.byteLength(options.body);
    }

    const req = mod.request(reqOptions, (res) => {
      // Follow redirects (up to 5)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
        // Carry cookies forward
        const newCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]);
        const existingCookies = options.headers?.Cookie || '';
        const mergedCookies = [...(existingCookies ? existingCookies.split('; ') : []), ...newCookies]
          .filter(Boolean)
          .join('; ');
        const redirectOpts = { ...options, method: 'GET', body: undefined, headers: { ...options.headers, Cookie: mergedCookies } };
        return httpRequest(redirectUrl, redirectOpts).then(resolve).catch(reject);
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const cookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]);
        resolve({ data, status: res.statusCode, cookies, headers: res.headers });
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });

    if (options.body) req.write(options.body);
    req.end();
  });
}


// ═══════════════════════════════════════════════════════════════════════════
// CALIFORNIA DRE
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeCA() {
  const state = 'CA';
  const source = 'ca-dre';
  const leads = [];
  const seen = new Set();

  // Two-letter prefixes for last name search
  const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');
  let prefixes;
  if (TEST_MODE) {
    prefixes = ['Zz']; // Very small result set for testing
  } else {
    // Two-letter prefixes Aa-Zz — CA DRE requires at least 2 chars
    prefixes = [];
    for (const first of alphabet) {
      for (const second of alphabet) {
        prefixes.push(first.toUpperCase() + second);
      }
    }
  }

  console.log(`[CA] Starting California DRE scrape (${prefixes.length} prefixes, test=${TEST_MODE})`);

  for (const prefix of prefixes) {
    try {
      console.log(`[CA] Searching last name prefix: "${prefix}"...`);
      const body = querystring.stringify({
        h_nextstep: 'SEARCH',
        LICENSEE_NAME: prefix,
        CITY_STATE: '',
        LICENSE_ID: '',
      });

      const res = await httpRequest('https://www2.dre.ca.gov/PublicASP/pplinfo.asp?start=1', {
        method: 'POST',
        body,
        headers: { Referer: 'https://www2.dre.ca.gov/PublicASP/pplinfo.asp' },
      });

      if (res.status !== 200) {
        console.log(`[CA] HTTP ${res.status} for prefix "${prefix}", skipping`);
        await sleep(DELAY_MS);
        continue;
      }

      // Parse match count
      const countMatch = res.data.match(/(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+matches/);
      const totalMatches = countMatch ? parseInt(countMatch[3]) : 0;

      // Parse table rows: each row has License ID, Name, License Type, City, MLO
      // Structure: <td>\n\t    <a href="pplinfo.asp?License_id=XXX">XXX</a></td>\n          <td> Name </td><td>Type</td><td>City</td>
      const rowRegex = /<a\s+href="pplinfo\.asp\?License_id=(\d+)">\d+<\/a><\/td>\s*<td>([^<]+(?:<i>[^<]*<\/i>)?)<\/td><td>([^<]+)<\/td><td>([^<]*)<\/td>/gis;
      let match;
      let count = 0;

      while ((match = rowRegex.exec(res.data)) !== null) {
        const licenseId = match[1].trim();
        const rawName = match[2].replace(/<i>[^<]*<\/i>/g, '').trim();
        const licenseType = match[3].trim();
        const city = match[4].trim();

        // Skip non-person license types (corporations, DBAs, partnerships)
        const lt = licenseType.toLowerCase();
        if (lt === 'corporation' || lt === 'partnership' || lt === 'dba') continue;
        // Only include Salesperson and Broker types
        if (!lt.includes('salesperson') && !lt.includes('broker')) continue;

        // Dedup by license ID
        if (seen.has(licenseId)) continue;
        seen.add(licenseId);

        // Parse "Last, First Middle" name format
        const { first_name, last_name } = parseName(rawName);
        if (!first_name && !last_name) continue;

        // Map license types
        let title = 'Real Estate Agent';
        if (lt.includes('broker')) title = 'Real Estate Broker';
        else if (lt.includes('salesperson')) title = 'Real Estate Salesperson';

        leads.push({
          first_name: titleCase(first_name),
          last_name: titleCase(last_name),
          firm_name: '',
          title,
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: titleCase(city),
          state,
          country: 'US',
          niche: 'real estate agent',
          source,
        });
        count++;
      }

      console.log(`[CA] Prefix "${prefix}": ${count} new leads (${totalMatches} total matches)`);

      // In test mode, stop after first prefix
      if (TEST_MODE && leads.length > 0) break;

    } catch (err) {
      console.error(`[CA] Error on prefix "${prefix}": ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  return leads;
}


// ═══════════════════════════════════════════════════════════════════════════
// NEW YORK DOS
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeNY() {
  const state = 'NY';
  const source = 'ny-dos';
  const leads = [];
  const seen = new Set();

  // NY DOS requires min 2 chars for both surname AND firstName.
  // Strategy: Use "Search by Name for Specified License Type" to pre-filter to
  //           Salesperson type, then iterate 2-letter surname + 2-letter firstName combos.
  //           We search 3 real estate license types: Salesperson, Assoc Broker, Individual Broker.
  //
  // For each license type, iterate 2-letter last name prefixes (Aa-Zz = 676 combos)
  // combined with common first name prefixes that cover most names.
  // PageSize=30 (max supported by the site).

  const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');

  // Common 2-letter first name prefixes that cover the majority of English first names
  const FIRST_PREFIXES = [
    'Ab','Ad','Al','Am','An','Ar','As','Au',
    'Ba','Be','Bi','Bo','Br','Bu',
    'Ca','Ce','Ch','Ci','Cl','Co','Cr','Cu','Cy',
    'Da','De','Di','Do','Dr','Du','Dy',
    'Ed','El','Em','Er','Es','Et','Ev',
    'Fa','Fe','Fl','Fr',
    'Ga','Ge','Gi','Gl','Go','Gr','Gu',
    'Ha','He','Hi','Ho','Hu',
    'Ia','Il','In','Ir','Is','Iv',
    'Ja','Je','Ji','Jo','Ju',
    'Ka','Ke','Ki','Ko','Kr','Ku','Ky',
    'La','Le','Li','Lo','Lu','Ly',
    'Ma','Me','Mi','Mo','Mu','My',
    'Na','Ne','Ni','No','Nu',
    'Ol','Or','Os','Ow',
    'Pa','Pe','Ph','Pi','Pr',
    'Qu',
    'Ra','Re','Ri','Ro','Ru','Ry',
    'Sa','Se','Sh','Si','So','St','Su','Sy',
    'Ta','Te','Th','Ti','To','Tr','Ty',
    'Va','Ve','Vi','Vo',
    'Wa','We','Wi','Wo',
    'Ya','Yi','Yo','Yu',
    'Za','Ze','Zo',
  ];

  // License types to search
  const licenseTypes = [
    { id: '7001040:SP', label: 'Salesperson', title: 'Real Estate Salesperson' },
    { id: '7001030:AB', label: 'Associate Broker', title: 'Real Estate Associate Broker' },
    { id: '7001035:IB', label: 'Individual Broker', title: 'Real Estate Broker' },
  ];

  // Build last name prefixes
  let lastNamePrefixes;
  if (TEST_MODE) {
    lastNamePrefixes = ['Sm']; // Single test prefix
  } else {
    lastNamePrefixes = [];
    for (const f of alphabet) {
      for (const s of alphabet) {
        lastNamePrefixes.push(f.toUpperCase() + s);
      }
    }
  }

  // In test mode, only a few first name prefixes
  const firstPrefixes = TEST_MODE ? ['Jo', 'Ma'] : FIRST_PREFIXES;

  console.log(`[NY] Starting New York DOS scrape (${lastNamePrefixes.length} last name prefixes x ${firstPrefixes.length} first name prefixes, test=${TEST_MODE})`);

  for (const licType of licenseTypes) {
    console.log(`[NY] License type: ${licType.label}`);

    for (const lastPrefix of lastNamePrefixes) {
      let prefixLeads = 0;

      for (const firstPrefix of firstPrefixes) {
        try {
          // Fresh session for each search (NY DOS sessions are ephemeral)
          const sessRes = await httpRequest('https://appext20.dos.ny.gov/nydos/selSearchType.do');
          const sessionCookies = sessRes.cookies.join('; ');
          await sleep(150);

          // Navigate to license type selection
          await httpRequest('https://appext20.dos.ny.gov/nydos/selLicType.do?type=name', {
            headers: { Cookie: sessionCookies },
          });
          await sleep(150);

          // Select license type
          const selectBody = querystring.stringify({ licTypeId: licType.id, select: 'Next' });
          await httpRequest('https://appext20.dos.ny.gov/nydos/selLicType.do', {
            method: 'POST',
            body: selectBody,
            headers: { Cookie: sessionCookies, Referer: 'https://appext20.dos.ny.gov/nydos/selLicType.do?type=name' },
          });
          await sleep(150);

          // Search
          const searchBody = querystring.stringify({
            searchType: 'name',
            indOrgInd: 'I',
            surname: lastPrefix,
            firstName: firstPrefix,
            organizationName: '',
            pageSize: '30',
            search: 'Search',
          });

          const searchRes = await httpRequest('https://appext20.dos.ny.gov/nydos/searchByName.do', {
            method: 'POST',
            body: searchBody,
            headers: { Cookie: sessionCookies, Referer: 'https://appext20.dos.ny.gov/nydos/searchByName.do' },
          });

          if (searchRes.status !== 200 || !searchRes.data.includes('itemRow')) {
            await sleep(DELAY_MS);
            continue;
          }

          // Parse results — the HTML has this structure per row:
          // <tr class="itemRow">
          //   <td><span><a href="details.do?anchor=..."><!-- <a href="..."> --> NAME </a></span></td>
          //   <td><span>LICENSE_NUMBER</span></td>
          //   <td><span>LICENSE_TYPE</span></td>
          //   <td><span>STATUS</span></td>
          //   <td><span>EXPIRY</span></td>
          // </tr>
          // We need to extract the name after the HTML comment
          const rowRegex = /<tr\s+class="itemRow(?:Alt)?">\s*[\s\S]*?-->\s*([\s\S]*?)\s*<\/a>[\s\S]*?<span[^>]*>([^<]+)<\/span>[\s\S]*?<span[^>]*>([^<]+)<\/span>[\s\S]*?<span[^>]*>([\s\S]*?)<\/span>/gi;
          let rowMatch;

          while ((rowMatch = rowRegex.exec(searchRes.data)) !== null) {
            const rawName = rowMatch[1].replace(/\s+/g, ' ').trim();
            const licenseNumber = rowMatch[2].trim();
            const status = rowMatch[4].replace(/\s+/g, ' ').trim();

            // Only current licenses
            if (!status.toLowerCase().includes('current')) continue;

            // Dedup by license number
            if (seen.has(licenseNumber)) continue;
            seen.add(licenseNumber);

            // Parse name (FIRST MIDDLE LAST)
            const nameParts = rawName.split(' ').filter(Boolean);
            let first_name, last_name;
            if (nameParts.length <= 1) {
              first_name = nameParts[0] || '';
              last_name = '';
            } else {
              last_name = nameParts[nameParts.length - 1];
              first_name = nameParts.slice(0, -1).join(' ');
            }

            leads.push({
              first_name: titleCase(first_name),
              last_name: titleCase(last_name),
              firm_name: '',
              title: licType.title,
              email: '',
              phone: '',
              website: '',
              domain: '',
              city: '',
              state,
              country: 'US',
              niche: 'real estate agent',
              source,
            });
            prefixLeads++;
          }

        } catch (err) {
          // Silently skip errors on individual searches
        }

        await sleep(DELAY_MS);
      }

      if (prefixLeads > 0) {
        console.log(`[NY] Last name "${lastPrefix}" (${licType.label}): ${prefixLeads} leads`);
      }

      if (TEST_MODE && leads.length > 0) break;
    }

    if (TEST_MODE && leads.length > 0) break;
  }

  return leads;
}


// ═══════════════════════════════════════════════════════════════════════════
// GEORGIA GREC
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeGA() {
  const state = 'GA';
  const source = 'ga-grec';
  const leads = [];
  const seen = new Set();

  // GA GREC limits results to 50 per search, so we use 2-letter prefixes
  // to keep under that limit for most combinations
  const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');

  let prefixes;
  if (TEST_MODE) {
    prefixes = ['Zab']; // Small set with known results
  } else {
    // 2-letter prefixes
    prefixes = [];
    for (const first of alphabet) {
      for (const second of alphabet) {
        prefixes.push(first.toUpperCase() + second);
      }
    }
  }

  console.log(`[GA] Starting Georgia GREC scrape (${prefixes.length} prefixes, test=${TEST_MODE})`);

  for (const prefix of prefixes) {
    try {
      // Step 1: GET the search page to get CSRF token + session cookies
      const pageRes = await httpRequest('https://ata.grec.state.ga.us/Account/Search');
      if (pageRes.status !== 200) {
        console.log(`[GA] HTTP ${pageRes.status} getting search page, skipping prefix "${prefix}"`);
        await sleep(DELAY_MS);
        continue;
      }

      // Extract CSRF token
      const tokenMatch = pageRes.data.match(/name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"/);
      if (!tokenMatch) {
        console.error(`[GA] Could not extract CSRF token for prefix "${prefix}"`);
        await sleep(DELAY_MS);
        continue;
      }
      const csrfToken = tokenMatch[1];
      const cookies = pageRes.cookies.join('; ');

      await sleep(200);

      // Step 2: POST the search
      const searchBody = querystring.stringify({
        __RequestVerificationToken: csrfToken,
        EntityType: 'RE-PR',
        LastName: prefix,
        AuthorizationNumber: '',
        City: '',
        Name: '',
        submit: 'Search',
      });

      const searchRes = await httpRequest('https://ata.grec.state.ga.us/Account/Search', {
        method: 'POST',
        body: searchBody,
        headers: {
          Cookie: cookies,
          Referer: 'https://ata.grec.state.ga.us/Account/Search',
        },
      });

      if (searchRes.status !== 200) {
        console.log(`[GA] HTTP ${searchRes.status} searching prefix "${prefix}"`);
        await sleep(DELAY_MS);
        continue;
      }

      // Check if results were limited (means we need finer prefixes)
      const limitedMatch = searchRes.data.match(/Search results limited to (\d+)/);
      const isLimited = !!limitedMatch;

      // Parse table rows
      // Pattern: <tr class="bg-light text-center">
      //   <td>\n NAME \n <td>LICENSE_NUM</td>
      //   <td>TYPE</td> (SLSP or BRKR)
      //   <td>STATUS</td>
      //   <td>RENEWAL_DATE</td>
      //   <td>FIRM_NAME</td>
      //   <td>FIRM_ROLE</td>
      const rowRegex = /<tr\s+class="bg-light text-center">\s*(?:<td>)?\s*<td>\s*([^<]+)<td>(\d+)<\/td>\s*<td>(SLSP|BRKR|ABKR)<\/td>\s*<td>([^<]+)<\/td>\s*<td>([^<]*)<\/td>\s*(?:<td>([^<]*)<\/td>)?/gi;
      let rowMatch;
      let count = 0;

      while ((rowMatch = rowRegex.exec(searchRes.data)) !== null) {
        const rawName = rowMatch[1].trim();
        const licenseNum = rowMatch[2].trim();
        const licenseType = rowMatch[3].trim();
        const status = rowMatch[4].trim();
        const renewalDate = rowMatch[5].trim();
        const firmName = rowMatch[6] ? rowMatch[6].trim() : '';

        // Only include active licenses
        if (status.toUpperCase() !== 'ACTIVE') continue;

        // Dedup by license number
        if (seen.has(licenseNum)) continue;
        seen.add(licenseNum);

        // Parse name (format: "FIRST LAST" or "FIRST MIDDLE LAST")
        const nameParts = rawName.replace(/\s+/g, ' ').trim().split(' ');
        let first_name, last_name;
        if (nameParts.length === 1) {
          first_name = nameParts[0];
          last_name = '';
        } else {
          last_name = nameParts[nameParts.length - 1];
          first_name = nameParts.slice(0, -1).join(' ');
        }

        // Map license type
        let title = 'Real Estate Agent';
        if (licenseType === 'BRKR' || licenseType === 'ABKR') title = 'Real Estate Broker';
        else if (licenseType === 'SLSP') title = 'Real Estate Salesperson';

        // Clean firm name (skip generic statuses, decode HTML entities)
        const decodedFirm = decodeHtmlEntities(firmName);
        const cleanFirm = (decodedFirm === 'REVOCATIONS' || decodedFirm === 'DECEASED' ||
          decodedFirm === 'FAILED TO RENEW-LAPSED' || decodedFirm === '') ? '' : decodedFirm;

        leads.push({
          first_name: titleCase(first_name),
          last_name: titleCase(last_name),
          firm_name: titleCase(cleanFirm),
          title,
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: '',
          state,
          country: 'US',
          niche: 'real estate agent',
          source,
        });
        count++;
      }

      if (count > 0 || isLimited) {
        console.log(`[GA] Prefix "${prefix}": ${count} active leads${isLimited ? ' (results limited to 50!)' : ''}`);
      }

      // If results were limited, split into 3-letter prefixes
      if (isLimited && !TEST_MODE) {
        console.log(`[GA] Splitting "${prefix}" into 3-letter prefixes...`);
        for (const third of alphabet) {
          const subPrefix = prefix + third;
          try {
            // Fresh CSRF token for each request
            const subPageRes = await httpRequest('https://ata.grec.state.ga.us/Account/Search');
            const subTokenMatch = subPageRes.data.match(/name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"/);
            if (!subTokenMatch) continue;

            const subBody = querystring.stringify({
              __RequestVerificationToken: subTokenMatch[1],
              EntityType: 'RE-PR',
              LastName: subPrefix,
              AuthorizationNumber: '',
              City: '',
              Name: '',
              submit: 'Search',
            });

            const subRes = await httpRequest('https://ata.grec.state.ga.us/Account/Search', {
              method: 'POST',
              body: subBody,
              headers: {
                Cookie: subPageRes.cookies.join('; '),
                Referer: 'https://ata.grec.state.ga.us/Account/Search',
              },
            });

            const subRowRegex = /<tr\s+class="bg-light text-center">\s*(?:<td>)?\s*<td>\s*([^<]+)<td>(\d+)<\/td>\s*<td>(SLSP|BRKR|ABKR)<\/td>\s*<td>([^<]+)<\/td>\s*<td>([^<]*)<\/td>\s*(?:<td>([^<]*)<\/td>)?/gi;
            let subRowMatch;
            let subCount = 0;
            while ((subRowMatch = subRowRegex.exec(subRes.data)) !== null) {
              const rn = subRowMatch[1].trim();
              const ln = subRowMatch[2].trim();
              const lt = subRowMatch[3].trim();
              const st = subRowMatch[4].trim();
              const fn = subRowMatch[6] ? subRowMatch[6].trim() : '';

              if (st.toUpperCase() !== 'ACTIVE') continue;
              if (seen.has(ln)) continue;
              seen.add(ln);

              const np = rn.replace(/\s+/g, ' ').trim().split(' ');
              let fi = np.length > 1 ? np.slice(0, -1).join(' ') : np[0];
              let la = np.length > 1 ? np[np.length - 1] : '';

              let ti = 'Real Estate Agent';
              if (lt === 'BRKR' || lt === 'ABKR') ti = 'Real Estate Broker';
              else if (lt === 'SLSP') ti = 'Real Estate Salesperson';

              const dfn = decodeHtmlEntities(fn);
              const cf = (dfn === 'REVOCATIONS' || dfn === 'DECEASED' || dfn === 'FAILED TO RENEW-LAPSED' || dfn === '') ? '' : dfn;

              leads.push({
                first_name: titleCase(fi), last_name: titleCase(la),
                firm_name: titleCase(cf), title: ti,
                email: '', phone: '', website: '', domain: '',
                city: '', state, country: 'US',
                niche: 'real estate agent', source,
              });
              subCount++;
            }

            if (subCount > 0) {
              console.log(`[GA]   Sub-prefix "${subPrefix}": ${subCount} active leads`);
            }

            await sleep(DELAY_MS);
          } catch (err) {
            console.error(`[GA] Error on sub-prefix "${subPrefix}": ${err.message}`);
          }
        }
      }

      if (TEST_MODE && leads.length > 0) break;

    } catch (err) {
      console.error(`[GA] Error on prefix "${prefix}": ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  return leads;
}


// ═══════════════════════════════════════════════════════════════════════════
// FLORIDA DBPR (Placeholder — needs Puppeteer/browser for ASP session)
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeFL() {
  console.log('[FL] Florida DBPR requires ASP.NET session state that cannot be maintained');
  console.log('[FL] via plain HTTP requests. This scraper needs Puppeteer or a browser.');
  console.log('[FL] Board code: 25 (Real Estate)');
  console.log('[FL] URL: https://www.myfloridalicense.com/wl11.asp?mode=1&search=Name&SID=&brd=25');
  console.log('[FL] Skipping — placeholder only.');
  return [];
}


// ═══════════════════════════════════════════════════════════════════════════
// TEXAS TREC (Placeholder — Drupal JS-rendered search)
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeTX() {
  console.log('[TX] Texas TREC uses a JavaScript-rendered Drupal search that requires');
  console.log('[TX] browser execution. This scraper needs Puppeteer.');
  console.log('[TX] URL: https://www.trec.texas.gov/apps/license-holder-search/');
  console.log('[TX] Skipping — placeholder only.');
  return [];
}


// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

const SCRAPERS = {
  CA: { fn: scrapeCA, name: 'California DRE', working: true },
  NY: { fn: scrapeNY, name: 'New York DOS', working: true },
  GA: { fn: scrapeGA, name: 'Georgia GREC', working: true },
  FL: { fn: scrapeFL, name: 'Florida DBPR', working: false },
  TX: { fn: scrapeTX, name: 'Texas TREC', working: false },
};

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  US State Real Estate Agent License Scraper');
  console.log(`  Mode: ${TEST_MODE ? 'TEST (small sample)' : 'FULL'}`);
  console.log(`  State filter: ${STATE_FILTER || 'ALL'}`);
  console.log(`  Delay: ${DELAY_MS}ms between requests`);
  console.log('═══════════════════════════════════════════════════════════\n');

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const statesToRun = STATE_FILTER
    ? [STATE_FILTER]
    : Object.keys(SCRAPERS).filter(s => SCRAPERS[s].working);

  let totalLeads = 0;

  for (const stateCode of statesToRun) {
    const scraper = SCRAPERS[stateCode];
    if (!scraper) {
      console.error(`Unknown state: ${stateCode}. Available: ${Object.keys(SCRAPERS).join(', ')}`);
      continue;
    }

    console.log(`\n--- ${scraper.name} (${stateCode}) ---`);
    const startTime = Date.now();

    try {
      const leads = await scraper.fn();
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

      if (leads.length > 0) {
        const outputFile = path.join(OUTPUT_DIR, `us-realtors-${stateCode.toLowerCase()}.csv`);
        writeCsv(outputFile, leads);
        console.log(`[${stateCode}] Saved ${leads.length} leads to ${outputFile} (${elapsed}s)`);
        totalLeads += leads.length;
      } else {
        console.log(`[${stateCode}] No leads scraped (${elapsed}s)`);
      }
    } catch (err) {
      console.error(`[${stateCode}] Fatal error: ${err.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`  COMPLETE — ${totalLeads} total leads across ${statesToRun.length} state(s)`);
  console.log('═══════════════════════════════════════════════════════════');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
