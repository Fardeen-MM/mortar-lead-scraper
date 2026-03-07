#!/usr/bin/env node
/**
 * Scrape ALL registered real estate agents from RECA (Real Estate Council of Alberta).
 *
 * Source: https://reports.myreca.ca/publicsearch.aspx
 * Method: ASP.NET WebForms 3-step session flow per search:
 *         1. GET initial page (get session cookie + ViewState)
 *         2. POST "Search by Person" button (get updated ViewState)
 *         3. POST search query with name prefix (get report results)
 *
 * Strategy: Iterate 2-letter last name prefixes (Aa-Zz = 676 combos).
 *           Auto-split to 3-letter prefixes when a 2-letter returns 500+ results.
 *           Dedup by contact GUID from drillthrough URLs.
 *           Filter to "Licensed" status only.
 *
 * Fields extracted: first_name, last_name, firm_name (brokerage), title (class),
 *                   city, contact_id (RECA internal GUID)
 * Note: RECA does NOT expose email or phone in public search results.
 *
 * Zero npm dependencies -- uses only Node.js built-in modules (https, fs, path).
 *
 * Features:
 *   - 2-letter last name prefix iteration (676 combinations)
 *   - Auto-split to 3-letter prefixes if 500+ results
 *   - Dedup by RECA contact GUID
 *   - Auto-saves progress CSV every 500 new leads
 *   - Rate limiting (configurable, default 500ms)
 *   - Segment support for parallel execution (--segment=A-F, G-L, M-R, S-Z)
 *   - Resume support (reads existing progress file)
 *   - Graceful ASP.NET session management with auto-refresh
 *
 * Usage:
 *   node scripts/scrape-reca-agents.js
 *   node scripts/scrape-reca-agents.js --segment=A-F
 *   node scripts/scrape-reca-agents.js --segment=S-Z
 *   node scripts/scrape-reca-agents.js --resume
 *   node scripts/scrape-reca-agents.js --delay=500
 *   node scripts/scrape-reca-agents.js --test   (only runs prefix "sm")
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

const DELAY_MS       = parseInt(args.delay) || 500;
const TEST_MODE      = !!args.test;
const RESUME_MODE    = !!args.resume;
const SEGMENT        = (args.segment || '').toUpperCase();
const SPLIT_THRESHOLD = 500;
const SAVE_EVERY     = 500;
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const PROGRESS_FILE  = path.join(OUTPUT_DIR,
  SEGMENT ? `reca-segment-${SEGMENT}.csv` : 'reca-real-estate-agents.csv'
);

const BASE_URL = 'https://reports.myreca.ca';
const SEARCH_PATH = '/publicsearch.aspx';

// ── Segment letter ranges ─────────────────────────────────────────────────
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

function buildPrefixes() {
  const letters = getLetterRange();
  const seconds = 'abcdefghijklmnopqrstuvwxyz'.split('');
  const prefixes = [];
  for (const first of letters) {
    if (TEST_MODE) {
      prefixes.push(first.toLowerCase() + 'm');
    } else {
      for (const second of seconds) {
        prefixes.push(first.toLowerCase() + second);
      }
    }
  }
  return prefixes;
}

function build3LetterPrefixes(twoLetter) {
  const thirds = 'abcdefghijklmnopqrstuvwxyz'.split('');
  return thirds.map(c => twoLetter + c);
}

// ── CSV columns ───────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'contact_id',
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

function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .split(/[\s-]+/)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(str.includes('-') ? '-' : ' ')
    .replace(/\bO\/a\b/g, 'o/a')
    .replace(/\bRe\/max\b/gi, 'RE/MAX')
    .replace(/\bLtd\b/gi, 'Ltd.')
    .replace(/\bInc\b/gi, 'Inc.')
    .replace(/\bCorp\b/gi, 'Corp.');
}

// ── Sleep helper ──────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── HTTPS helpers ─────────────────────────────────────────────────────────
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

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
        const setCookies = res.headers['set-cookie'] || [];
        resolve({ status: res.statusCode, body: data, setCookies });
      });
    });
    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('GET timeout')); });
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
        'Origin': BASE_URL,
        'Referer': BASE_URL + SEARCH_PATH,
      },
    };
    if (cookies) options.headers['Cookie'] = cookies;

    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const setCookies = res.headers['set-cookie'] || [];
        resolve({ status: res.statusCode, body: data, setCookies });
      });
    });
    req.on('error', reject);
    req.setTimeout(180000, () => { req.destroy(); reject(new Error('POST timeout')); });
    req.write(postBody);
    req.end();
  });
}

// ── URL encoding helper ───────────────────────────────────────────────────
function encodeFormData(fields) {
  return Object.entries(fields)
    .map(([k, v]) => encodeURIComponent(k) + '=' + encodeURIComponent(v))
    .join('&');
}

// ── Parse ASP.NET hidden fields ───────────────────────────────────────────
function parseHiddenField(html, fieldName) {
  const esc = fieldName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`name="${esc}"[^>]*value="([^"]*)"`, 'i');
  const m = html.match(re);
  return m ? m[1] : '';
}

// ── Cookie jar ────────────────────────────────────────────────────────────
class CookieJar {
  constructor() {
    this.cookies = {};
  }

  update(setCookieHeaders) {
    for (const header of setCookieHeaders) {
      const parts = header.split(';')[0].split('=');
      const name = parts[0].trim();
      const value = parts.slice(1).join('=').trim();
      this.cookies[name] = value;
    }
  }

  toString() {
    return Object.entries(this.cookies)
      .map(([k, v]) => `${k}=${v}`)
      .join('; ');
  }
}

// ── Session manager ───────────────────────────────────────────────────────
// Manages the ASP.NET session: initial GET, "Search by Person" click, then search
class SessionManager {
  constructor() {
    this.jar = new CookieJar();
    this.viewState = '';
    this.viewStateGenerator = '';
    this.eventValidation = '';
    this.ready = false;
  }

  /** Step 1 + 2: Get initial page, click "Search by Person" */
  async init() {
    // Step 1: GET the initial page
    const res1 = await httpsGet(BASE_URL + SEARCH_PATH, null);
    if (res1.status !== 200) throw new Error(`Step 1: HTTP ${res1.status}`);
    this.jar.update(res1.setCookies);
    this._parseFormFields(res1.body);

    await sleep(200);

    // Step 2: POST "Search by Person"
    const postBody = encodeFormData({
      '__VIEWSTATE': this.viewState,
      '__VIEWSTATEGENERATOR': this.viewStateGenerator,
      '__EVENTVALIDATION': this.eventValidation,
      'Button1': 'Search by Person',
    });
    const res2 = await httpsPost(BASE_URL + SEARCH_PATH, postBody, this.jar.toString());
    if (res2.status !== 200) throw new Error(`Step 2: HTTP ${res2.status}`);
    this.jar.update(res2.setCookies);
    this._parseFormFields(res2.body);

    // Verify the person search form is now visible
    if (!res2.body.includes('First and/or Last')) {
      throw new Error('Step 2: Person search form not found in response');
    }

    this.ready = true;
  }

  /** Step 3: Perform a person name search.
   *  IMPORTANT: The RECA Report Viewer mutates ViewState after each search,
   *  making subsequent searches on the same session return 0 results.
   *  We must re-init (steps 1+2) before each search query. */
  async search(nameQuery) {
    // Always re-init session before each search — Report Viewer requires it
    await this.init();

    const postBody = encodeFormData({
      '__VIEWSTATE': this.viewState,
      '__VIEWSTATEGENERATOR': this.viewStateGenerator,
      '__EVENTVALIDATION': this.eventValidation,
      'TextBox2': nameQuery,
      'TextBox3': '',  // City (empty = all cities)
      'Button3': 'Search',
    });

    const res = await httpsPost(BASE_URL + SEARCH_PATH, postBody, this.jar.toString());
    if (res.status !== 200) throw new Error(`Search: HTTP ${res.status}`);
    this.jar.update(res.setCookies);
    this._parseFormFields(res.body);

    // Check for session expiry
    if (res.body.includes('ASP.NET session has expired') || res.body.includes('could not be found')) {
      this.ready = false;
      throw new Error('SESSION_EXPIRED');
    }

    return res.body;
  }

  _parseFormFields(html) {
    this.viewState = parseHiddenField(html, '__VIEWSTATE');
    this.viewStateGenerator = parseHiddenField(html, '__VIEWSTATEGENERATOR');
    this.eventValidation = parseHiddenField(html, '__EVENTVALIDATION');
  }
}

// ── Parse Report Viewer results ───────────────────────────────────────────
// Optimized single-pass parser for large HTML responses (up to 36MB+).
// Instead of building per-row regexes, we do ONE global scan to extract
// all cell data keyed by (cellId, rowNum), then assemble rows.
function parseResults(html) {
  const leads = [];

  // Check for report content
  if (!html.includes('oReportDiv') || html.includes('No results found')) {
    return leads;
  }

  // Column IDs in the RECA Report Viewer table:
  //   298 = Status, 302 = View link, 306 = First, 310 = Middle,
  //   314 = Last, 318 = AKA, 322 = Brokerage, 326 = City,
  //   330 = Class/Title, 334 = Issue Date

  // PASS 1: Extract all cell text values in one scan
  // Pattern: ID="...{cellId}iT2R0x{rowNum}_aria"...>...<div...>TEXT</div>
  const cellData = {};      // { "rowNum" -> { cellId -> text } }
  const cellRe = /ID="[^"]*?(\d+)iT2R0x(\d+)_aria"[^>]*>(?:\s*<[^>]+>)*\s*(?:<div[^>]*>([^<]*)<\/div>|([^<]+))/gs;
  let cm;
  while ((cm = cellRe.exec(html)) !== null) {
    const cellId = cm[1];
    const rowNum = cm[2];
    const text = (cm[3] || cm[4] || '').trim();
    if (!text) continue;
    if (!cellData[rowNum]) cellData[rowNum] = {};
    cellData[rowNum][cellId] = decodeHtmlEntities(text);
  }

  // PASS 2: Extract all contact GUIDs in one scan
  // Pattern: ...302iT2R0x{rowNum}...contactid={GUID}
  const contactIds = {};    // { "rowNum" -> guid }
  const contactRe = /302iT2R0x(\d+)[^"]*"[^>]*>\s*<a[^>]*data-drillThroughUrl="[^"]*contactid=([a-f0-9-]+)/gs;
  let ctm;
  while ((ctm = contactRe.exec(html)) !== null) {
    contactIds[ctm[1]] = ctm[2];
  }

  // PASS 3: Extract brokerage names from drillthrough URLs in one scan
  const brokerageNames = {};  // { "rowNum" -> name }
  const brokRe = /322iT2R0x(\d+)[^"]*"[^>]*>\s*<a[^>]*data-drillThroughUrl="[^"]*LastName=([^&"]+)/gs;
  let bm;
  while ((bm = brokRe.exec(html)) !== null) {
    try {
      brokerageNames[bm[1]] = decodeURIComponent(bm[2].replace(/\+/g, ' '));
    } catch (e) {
      brokerageNames[bm[1]] = bm[2].replace(/\+/g, ' ');
    }
  }

  // Assemble leads from collected data
  const rowNums = Object.keys(cellData).sort((a, b) => parseInt(a) - parseInt(b));

  for (const rn of rowNums) {
    const cells = cellData[rn];

    // Status check - only keep "Licensed"
    const status = (cells['298'] || '').trim();
    if (status !== 'Licensed') continue;

    const firstName = (cells['306'] || '').trim();
    const lastName = (cells['314'] || '').trim();
    const city = (cells['326'] || '').trim();
    const classTitle = (cells['330'] || '').trim();
    const brokerageName = cells['322'] || brokerageNames[rn] || '';
    const contactId = contactIds[rn] || '';

    // Skip rows with no name
    if (!firstName && !lastName) continue;

    // Map class to display title
    let title = classTitle;
    if (classTitle === 'Associate') title = 'Real Estate Associate';
    else if (classTitle === 'Associate Broker') title = 'Associate Broker';
    else if (classTitle === 'Broker') title = 'Real Estate Broker';

    leads.push({
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      firm_name: brokerageName,
      title,
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: titleCase(city),
      state: 'Alberta',
      country: 'CA',
      niche: 'real estate agent',
      source: 'reca',
      contact_id: contactId,
    });
  }

  return leads;
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#32;/g, ' ');
}

// ── Count results in HTML ─────────────────────────────────────────────────
function countResults(html) {
  // Count "View" links which correspond to individual results
  const viewMatches = html.match(/>View</g);
  return viewMatches ? viewMatches.length : 0;
}

// ── Resume: load existing leads from CSV ──────────────────────────────────
function loadExistingLeads() {
  const seen = new Map(); // contact_id -> true
  if (!fs.existsSync(PROGRESS_FILE)) return seen;

  const content = fs.readFileSync(PROGRESS_FILE, 'utf-8');
  const lines = content.trim().split('\n').slice(1); // skip header
  for (const line of lines) {
    // Extract contact_id (last column)
    const cols = parseCsvLine(line);
    const contactId = cols[CSV_COLUMNS.indexOf('contact_id')] || '';
    if (contactId) {
      seen.set(contactId, true);
    }
  }
  console.log(`  Loaded ${seen.size} existing leads from ${PROGRESS_FILE}`);
  return seen;
}

function parseCsvLine(line) {
  const cols = [];
  let current = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuote = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuote = true;
      } else if (ch === ',') {
        cols.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  cols.push(current);
  return cols;
}

// ── Save to CSV ───────────────────────────────────────────────────────────
function saveCsv(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const lines = [CSV_HEADER];
  for (const lead of leads) {
    lines.push(leadToCsvRow(lead));
  }
  fs.writeFileSync(PROGRESS_FILE, lines.join('\n') + '\n');
}

// ── Main ──────────────────────────────────────────────────────────────────
async function main() {
  console.log('='.repeat(70));
  console.log('  RECA Real Estate Agent Scraper (Alberta, Canada)');
  console.log('  Source: https://reports.myreca.ca/publicsearch.aspx');
  console.log(`  Mode: ${TEST_MODE ? 'TEST' : SEGMENT ? `Segment ${SEGMENT}` : 'FULL'}`);
  console.log(`  Delay: ${DELAY_MS}ms | Save every: ${SAVE_EVERY} leads`);
  console.log(`  Output: ${PROGRESS_FILE}`);
  console.log('='.repeat(70));

  // Load existing leads for dedup/resume
  const seenIds = RESUME_MODE ? loadExistingLeads() : new Map();
  const allLeads = [];

  // If resuming, reload existing leads into allLeads
  if (RESUME_MODE && fs.existsSync(PROGRESS_FILE)) {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf-8');
    const lines = content.trim().split('\n').slice(1);
    for (const line of lines) {
      const cols = parseCsvLine(line);
      const lead = {};
      CSV_COLUMNS.forEach((col, i) => { lead[col] = cols[i] || ''; });
      allLeads.push(lead);
    }
    console.log(`  Resumed with ${allLeads.length} existing leads`);
  }

  let totalNew = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let lastSaveCount = allLeads.length;

  const prefixes = buildPrefixes();
  console.log(`\n  Processing ${prefixes.length} prefix(es)...\n`);

  // Create session manager
  let session = new SessionManager();

  for (let i = 0; i < prefixes.length; i++) {
    const prefix = prefixes[i];
    const pctDone = ((i / prefixes.length) * 100).toFixed(1);

    let retries = 0;
    let success = false;

    while (retries < 3 && !success) {
      try {
        // Perform the search (session.search() handles re-init automatically)
        const html = await session.search(prefix);
        const resultCount = countResults(html);
        const leads = parseResults(html);

        // Check if we need to split into 3-letter prefixes
        if (resultCount >= SPLIT_THRESHOLD && prefix.length === 2) {
          console.log(`  [${pctDone}%] "${prefix}" -> ${resultCount} results (>=${SPLIT_THRESHOLD}), splitting to 3-letter...`);

          const subPrefixes = build3LetterPrefixes(prefix);
          for (const subPrefix of subPrefixes) {
            try {
              const subHtml = await session.search(subPrefix);
              const subLeads = parseResults(subHtml);
              const subCount = countResults(subHtml);

              let subNew = 0;
              for (const lead of subLeads) {
                const key = lead.contact_id || `${lead.first_name}|${lead.last_name}|${lead.city}`;
                if (seenIds.has(key)) {
                  totalSkipped++;
                  continue;
                }
                seenIds.set(key, true);
                allLeads.push(lead);
                totalNew++;
                subNew++;
              }

              if (subCount > 0) {
                console.log(`    "${subPrefix}" -> ${subCount} results, ${subNew} new (total: ${allLeads.length})`);
              }

              await sleep(DELAY_MS);
            } catch (subErr) {
              if (subErr.message === 'SESSION_EXPIRED') {
                console.log(`    "${subPrefix}" session expired, refreshing...`);
                session = new SessionManager();
                // Will re-init on next search call
              } else {
                console.error(`    "${subPrefix}" ERROR: ${subErr.message}`);
                totalErrors++;
              }
            }

            // Save progress
            if (allLeads.length - lastSaveCount >= SAVE_EVERY) {
              saveCsv(allLeads);
              lastSaveCount = allLeads.length;
              console.log(`    [saved] ${allLeads.length} leads to CSV`);
            }
          }

          success = true;
          continue;
        }

        // Process results from the 2-letter prefix
        let newCount = 0;
        for (const lead of leads) {
          const key = lead.contact_id || `${lead.first_name}|${lead.last_name}|${lead.city}`;
          if (seenIds.has(key)) {
            totalSkipped++;
            continue;
          }
          seenIds.set(key, true);
          allLeads.push(lead);
          totalNew++;
          newCount++;
        }

        console.log(`  [${pctDone}%] "${prefix}" -> ${resultCount} results, ${leads.length} licensed, ${newCount} new (total: ${allLeads.length})`);
        success = true;

        // Save progress periodically
        if (allLeads.length - lastSaveCount >= SAVE_EVERY) {
          saveCsv(allLeads);
          lastSaveCount = allLeads.length;
          console.log(`    [saved] ${allLeads.length} leads to CSV`);
        }

      } catch (err) {
        retries++;
        if (err.message === 'SESSION_EXPIRED') {
          console.log(`  [${pctDone}%] "${prefix}" session expired, refreshing... (retry ${retries}/3)`);
          session = new SessionManager();
        } else {
          console.error(`  [${pctDone}%] "${prefix}" ERROR: ${err.message} (retry ${retries}/3)`);
          totalErrors++;
        }

        if (retries < 3) {
          await sleep(DELAY_MS * 2);
        }
      }
    }

    if (!success) {
      console.error(`  [FAIL] "${prefix}" failed after 3 retries, skipping`);
    }

    await sleep(DELAY_MS);
  }

  // Final save
  saveCsv(allLeads);

  console.log('\n' + '='.repeat(70));
  console.log('  SCRAPE COMPLETE');
  console.log(`  Total leads:    ${allLeads.length}`);
  console.log(`  New this run:   ${totalNew}`);
  console.log(`  Duplicates:     ${totalSkipped}`);
  console.log(`  Errors:         ${totalErrors}`);
  console.log(`  Output:         ${PROGRESS_FILE}`);
  console.log('='.repeat(70));
}

main().catch(err => {
  console.error('\nFATAL:', err);
  process.exit(1);
});
