#!/usr/bin/env node
/**
 * Ireland Law Society "Find A Solicitor" scraper.
 *
 * Source landing: https://www.lawsociety.ie/find-a-solicitor
 * Real directory:  https://www.lawsociety.ie/find-a-solicitor/Solicitor-Firm-Search/
 *
 * The public page is a SPA. The real data lives behind a JSON endpoint that the
 * page's JS calls on form submit:
 *
 *   GET /UniversalSearch/GetSearchResults
 *       ?pageId=154320
 *       &txtSearchNameLocation=*         (wildcard returns the full register)
 *       &ddLookingFor=Solicitor
 *       &ddLocatedIn=Ireland              (does not actually filter — all 12,664 come back regardless)
 *       &pageNo=N                         (1..totalPages, pageSize=10)
 *
 * Response JSON shape:
 *   {
 *     solicitorResults: [ { solicitorNumber, title, christianName, surname, suffix,
 *                           firmName, firmType, address1..5, county, country, postCode,
 *                           telephone1, mobile1, faxNumber, emailAddress, directEmail,
 *                           webAddress, admitted, qualification, practicingSubStatus,
 *                           hidePersonalDetails, ... } ],
 *     totalPages: 1267,
 *     resultsText: "1 - 10 of 12664 result(s) for '*' in solicitor",
 *     ...
 *   }
 *
 * Strategy:
 *   - Page through pageNo=1..totalPages (reported as ~1,267; spec caps at 1,900 so we use totalPages)
 *   - Dedup by solicitorNumber (and by name+firm as a belt-and-braces fallback)
 *   - Resumable: state file saved every 50 pages
 *   - Concurrency 1 for listing pages (endpoint already returns 10 records/page, 1s delay)
 *   - NO per-profile fetch needed: all email/phone/address/website are already in the list JSON
 *     (so the spec's "concurrency 5 for detail fetches" is moot — we stay polite on listing)
 *
 * Note on the spec's `?practicearea=Immigration Law` filter: this parameter belongs to the
 * mediator search (`ddPracticeArea`), not the solicitor search. The Law Society's solicitor
 * directory has no practice-area filter. We therefore scrape the FULL register and
 * post-filter by firm/name/profile-text keywords into a second CSV.
 *
 * Output:
 *   /output/ireland-solicitors-all.csv
 *   /output/ireland-solicitors-immigration.csv
 */

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_ALL = path.join(OUTPUT_DIR, 'ireland-solicitors-all.csv');
const OUTPUT_IMM = path.join(OUTPUT_DIR, 'ireland-solicitors-immigration.csv');
const STATE_FILE = path.join(OUTPUT_DIR, '.ireland-solicitors-state.json');

const MAX_PAGES_CAP = 1900;           // spec cap
const STATE_SAVE_EVERY = 50;          // pages
const LIST_DELAY_MS = 1000;           // 1s between list pages
const MAX_EMPTY_PAGES = 5;            // stop after N consecutive empty responses
const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 3;

const PAGE_ID = 154320;               // from <input id="currentPageId" value="154320" />
const ENDPOINT = 'https://www.lawsociety.ie/UniversalSearch/GetSearchResults';

const COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
  'address', 'city', 'profile_url', 'country', 'niche', 'source',
];

const IMMIGRATION_RE = /immigration|asylum|visa|refugee/i;

// --- args -------------------------------------------------------------------
const args = process.argv.slice(2);
const argMaxPages = (() => {
  const i = args.indexOf('--max-pages');
  return i !== -1 ? parseInt(args[i + 1], 10) : null;
})();
const TEST_MODE = args.includes('--test');
const RESET = args.includes('--reset');

// --- http helper ------------------------------------------------------------
function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.lawsociety.ie/find-a-solicitor/Solicitor-Firm-Search/',
      },
      timeout: REQUEST_TIMEOUT_MS,
    }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function httpGetJson(url, attempts = MAX_RETRIES) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      const body = await httpGet(url);
      return JSON.parse(body);
    } catch (err) {
      lastErr = err;
      const wait = 1500 * (i + 1);
      console.warn(`  retry ${i + 1}/${attempts} after error: ${err.message} (wait ${wait}ms)`);
      await sleep(wait);
    }
  }
  throw lastErr;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// --- CSV --------------------------------------------------------------------
function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(filePath, rows) {
  const out = [COLUMNS.join(',')];
  for (const r of rows) out.push(COLUMNS.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(filePath, out.join('\n') + '\n');
}

// --- field mapping ----------------------------------------------------------
function decodeEntities(s) {
  if (!s) return '';
  // Cheerio can decode lightweight HTML entities that appear in firm/address strings
  try { return cheerio.load('<x>' + s + '</x>')('x').text(); }
  catch { return s; }
}

function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function cleanPhone(raw) {
  if (!raw) return '';
  return String(raw).replace(/[^0-9+]/g, '').trim();
}

function formatAddress(s) {
  const parts = [s.address1, s.address2, s.address3, s.address4, s.address5, s.postCode]
    .map(x => decodeEntities(String(x || '').trim()))
    .filter(Boolean);
  return parts.join(', ');
}

function inferCity(s) {
  // Ireland entries: address2 or address3 is usually the town; county is often a fallback.
  // Safe fallback: use county (well-populated) rather than guess.
  const candidates = [s.county, s.address4, s.address3, s.address2]
    .map(x => decodeEntities(String(x || '').trim()))
    .filter(Boolean);
  return candidates[0] || '';
}

function pickCountry(s) {
  const c = String(s.country || '').trim();
  if (!c) return 'Ireland';
  return c;
}

function mapSolicitor(s) {
  const first = decodeEntities(String(s.christianName || '').trim());
  const last = decodeEntities(String(s.surname || '').trim());
  const firm = decodeEntities(String(s.firmName || '').trim());
  const email = String(s.directEmail || s.emailAddress || '').trim().toLowerCase();
  const phone = cleanPhone(s.telephone1 || s.mobile1 || '');
  const website = String(s.webAddress || '').trim();
  const address = formatAddress(s);
  const city = inferCity(s);
  const country = pickCountry(s);
  const profileUrl = s.solicitorNumber
    ? `https://www.lawsociety.ie/find-a-solicitor/Solicitor-Firm-Search/?filters=t_Solicitor!q_${encodeURIComponent(s.solicitorNumber)}`
    : '';

  return {
    first_name: first,
    last_name: last,
    firm_name: firm,
    email,
    phone,
    website,
    address,
    city,
    profile_url: profileUrl,
    country,
    niche: 'solicitor',
    source: 'law_society_ireland',
    // hidden fields (not written to CSV) used for dedup/filtering
    _id: s.solicitorNumber || '',
    _practicingSubStatus: s.practicingSubStatus || '',
    _qualification: s.qualification || '',
  };
}

// --- state ------------------------------------------------------------------
function loadState() {
  if (RESET) return null;
  if (!fs.existsSync(STATE_FILE)) return null;
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
    if (!s || typeof s !== 'object') return null;
    return s;
  } catch { return null; }
}

function saveState(state) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// --- immigration filter -----------------------------------------------------
function isImmigrationMatch(row) {
  const blob = [row.firm_name, row.first_name, row.last_name, row._qualification, row._practicingSubStatus]
    .filter(Boolean).join(' ');
  return IMMIGRATION_RE.test(blob);
}

// --- core -------------------------------------------------------------------
function buildUrl(pageNo) {
  const qs = new URLSearchParams({
    pageId: String(PAGE_ID),
    txtSearchNameLocation: '*',
    ddLookingFor: 'Solicitor',
    ddLocatedIn: 'Ireland',
    ddPractisingStatus: '',
    ddOrganisationType: 'All organisations',
    ddFirmOrganisationType: 'All organisations',
    ddFirmPublicOrganisation: 'All organisations',
    cbIncludeFirmsAbroad: 'true',
    cbOffersRemoteMediation: 'false',
    ddSortBy: '',
    ddMediatorLocatedIn: 'All locations',
    ddPracticeArea: 'All practice areas',
    pageNo: String(pageNo),
  });
  return ENDPOINT + '?' + qs.toString();
}

async function fetchPage(pageNo) {
  const url = buildUrl(pageNo);
  const data = await httpGetJson(url);
  const list = data.solicitorResults || [];
  return {
    totalPages: data.totalPages || null,
    resultsText: data.resultsText || '',
    items: list,
    divNoResultsVisible: !!data.divNoResultsVisible,
  };
}

async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('=== Ireland Law Society — Solicitor Scraper ===');
  if (TEST_MODE) console.log('MODE: TEST (5 pages only)');

  const state = loadState() || { nextPage: 1, rows: [], seenIds: [], seenKeys: [], totalPages: null };
  const seenIds = new Set(state.seenIds);
  const seenKeys = new Set(state.seenKeys);
  const rows = state.rows.slice();

  const startPage = state.nextPage || 1;
  console.log(`Starting at page ${startPage} (${rows.length} rows already in state)`);

  // Probe page 1 for totalPages if we don't have it
  if (!state.totalPages) {
    try {
      const probe = await fetchPage(1);
      state.totalPages = probe.totalPages || null;
      console.log(`Discovered totalPages = ${state.totalPages} ("${probe.resultsText}")`);
    } catch (err) {
      console.error('FATAL: probe failed:', err.message);
      process.exit(1);
    }
  }

  const effectiveMax = TEST_MODE
    ? 5
    : Math.min(argMaxPages || MAX_PAGES_CAP, state.totalPages || MAX_PAGES_CAP, MAX_PAGES_CAP);

  console.log(`Will iterate pages ${startPage}..${effectiveMax}`);

  let emptyStreak = 0;
  for (let pageNo = startPage; pageNo <= effectiveMax; pageNo++) {
    let resp;
    try {
      resp = await fetchPage(pageNo);
    } catch (err) {
      console.error(`[page ${pageNo}] FAILED: ${err.message} — skipping`);
      await sleep(LIST_DELAY_MS * 2);
      continue;
    }

    let added = 0;
    for (const s of resp.items) {
      const row = mapSolicitor(s);
      const id = row._id;
      const key = `${row.first_name}|${row.last_name}|${row.firm_name}`.toLowerCase();
      if (id && seenIds.has(id)) continue;
      if (!id && seenKeys.has(key)) continue;
      if (id) seenIds.add(id);
      seenKeys.add(key);
      rows.push(row);
      added++;
    }

    console.log(`[page ${pageNo}/${effectiveMax}] +${added} (running total: ${rows.length}) — ${resp.resultsText || ''}`);

    if (resp.items.length === 0) {
      emptyStreak++;
      if (emptyStreak >= MAX_EMPTY_PAGES) {
        console.log(`  ${MAX_EMPTY_PAGES} consecutive empty pages — stopping`);
        break;
      }
    } else {
      emptyStreak = 0;
    }

    // persist state
    if (pageNo % STATE_SAVE_EVERY === 0 || pageNo === effectiveMax) {
      state.nextPage = pageNo + 1;
      state.rows = rows;
      state.seenIds = Array.from(seenIds);
      state.seenKeys = Array.from(seenKeys);
      saveState(state);
      console.log(`  -> state saved at page ${pageNo}`);
    }

    await sleep(LIST_DELAY_MS);
  }

  // Final write
  writeCsv(OUTPUT_ALL, rows);
  const imm = rows.filter(isImmigrationMatch);
  writeCsv(OUTPUT_IMM, imm);

  // Also persist state at the end
  state.nextPage = Math.min((state.nextPage || 1), effectiveMax + 1);
  state.rows = rows;
  state.seenIds = Array.from(seenIds);
  state.seenKeys = Array.from(seenKeys);
  saveState(state);

  const withEmail = rows.filter(r => r.email).length;
  const withPhone = rows.filter(r => r.phone).length;
  const withWebsite = rows.filter(r => r.website).length;

  console.log('\n=== RESULTS ===');
  console.log(`Total rows:       ${rows.length}`);
  console.log(`With email:       ${withEmail}`);
  console.log(`With phone:       ${withPhone}`);
  console.log(`With website:     ${withWebsite}`);
  console.log(`Immigration hits: ${imm.length}`);
  console.log(`Output ALL:       ${OUTPUT_ALL}`);
  console.log(`Output IMM:       ${OUTPUT_IMM}`);
  console.log(`State:            ${STATE_FILE}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
