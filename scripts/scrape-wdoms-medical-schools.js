#!/usr/bin/env node
/**
 * WDOMS (World Directory of Medical Schools) Scraper
 *
 * Iterates every country from /Home/GetCountriesList and POSTs to /
 * to harvest every medical school (name + city + country + WHO ID).
 *
 * Payload (form-encoded):
 *   sCountryName={countryCode}  -- required, numeric code from GetCountriesList
 *   sSchoolName=                 -- empty
 *   sCityName=                   -- empty
 *   sOperationFlag=              -- empty (Y/N filters to still-operational)
 *   iPageNumber=N                -- paginated 20/page approximately
 *   UN_MemberStatus=Y            -- Y = include non-UN states
 *   sWdomsFromAddedDate=         -- empty
 *   sWdomsToAddedDate=           -- empty
 *   sessionId=                   -- empty for public search
 *
 * Output columns: school_name,country_code,country_name,city,website,who_id,niche,source
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const OUT_DIR = path.resolve(__dirname, '..', 'output');
const STATE_DIR = path.resolve(__dirname, 'state');
const OUT_CSV = path.join(OUT_DIR, 'wdoms-medical-schools.csv');
const STATE_FILE = path.join(STATE_DIR, 'wdoms-medical-schools.state.json');

for (const d of [OUT_DIR, STATE_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

function httpsRequest(url, { method = 'GET', headers = {}, body = null } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const opts = {
      hostname: u.hostname,
      port: 443,
      path: u.pathname + u.search,
      method,
      headers: {
        'User-Agent': UA,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        ...headers,
      },
    };
    const req = https.request(opts, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks).toString('utf8'),
        });
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => req.destroy(new Error('timeout')));
    if (body) req.write(body);
    req.end();
  });
}

async function getCountries() {
  const res = await httpsRequest('https://search.wdoms.org/Home/GetCountriesList?sUnmember=Y', {
    headers: {
      'X-Requested-With': 'XMLHttpRequest',
      'Referer': 'https://search.wdoms.org/home',
    },
  });
  if (res.statusCode !== 200) throw new Error(`GetCountriesList ${res.statusCode}`);
  return JSON.parse(res.body);
}

async function searchPage(countryCode, page) {
  const body = new URLSearchParams({
    sCountryName: String(countryCode),
    sSchoolName: '',
    sCityName: '',
    sOperationFlag: '',
    iPageNumber: String(page),
    UN_MemberStatus: 'Y',
    sWdomsFromAddedDate: '',
    sWdomsToAddedDate: '',
    sessionId: '',
  }).toString();
  const res = await httpsRequest('https://search.wdoms.org/', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'X-Requested-With': 'XMLHttpRequest',
      'Referer': 'https://search.wdoms.org/home',
      'Origin': 'https://search.wdoms.org',
    },
    body,
  });
  if (res.statusCode !== 200) throw new Error(`POST ${res.statusCode}`);
  return res.body;
}

function parseResults(html, countryName) {
  const $ = cheerio.load(html);
  const rows = [];
  let total = null;
  const totalTxt = $('.pagination').first().text();
  const mTotal = totalTxt.match(/Total Records Found:\s*(\d+)/i);
  if (mTotal) total = parseInt(mTotal[1], 10);

  $('#tblResult tbody tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 4) return;
    const whoId = $(tds[0]).text().trim();
    const country = $(tds[1]).text().trim();
    const schoolName = $(tds[2]).text().replace(/\s+/g, ' ').trim();
    const city = $(tds[3]).text().replace(/\s+/g, ' ').trim();
    if (!schoolName) return;
    rows.push({
      school_name: schoolName,
      country_name: country || countryName,
      city,
      who_id: whoId,
    });
  });
  return { rows, total };
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function writeHeader() {
  fs.writeFileSync(OUT_CSV, 'school_name,country_code,country_name,city,website,who_id,niche,source\n');
}

function appendRow(r) {
  const line = [
    r.school_name,
    r.country_code,
    r.country_name,
    r.city,
    '', // website — detail page requires JS; leave blank
    r.who_id,
    'medical school',
    'wdoms',
  ].map(csvEscape).join(',') + '\n';
  fs.appendFileSync(OUT_CSV, line);
}

function loadState() {
  if (fs.existsSync(STATE_FILE)) { try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {} }
  return { doneCountries: {}, totalRows: 0 };
}
function saveState(s) { fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2)); }
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const argLimit = process.argv.indexOf('--limit');
  const limit = argLimit > -1 ? parseInt(process.argv[argLimit + 1], 10) : null;

  console.log('[WDOMS] Fetching countries list…');
  const countries = await getCountries();
  console.log(`[WDOMS] Got ${countries.length} countries`);

  const state = loadState();
  if (!state.totalRows) writeHeader();

  let globalRows = state.totalRows || 0;
  let countriesDone = 0;

  for (const c of countries) {
    countriesDone++;
    if (limit && countriesDone > limit) {
      console.log(`[WDOMS] Reached --limit ${limit}, stopping`);
      break;
    }
    if (state.doneCountries[c.countryCode]) continue;

    let page = 1;
    let totalExpected = null;
    let countryRows = 0;
    while (true) {
      try {
        const html = await searchPage(c.countryCode, page);
        const { rows, total } = parseResults(html, c.countryName);
        if (total !== null) totalExpected = total;
        for (const r of rows) {
          appendRow({ ...r, country_code: c.countryCode });
          globalRows++;
          countryRows++;
        }
        process.stdout.write(`[WDOMS] ${countriesDone}/${countries.length} ${c.countryName} — page ${page} got ${rows.length} (country total so far: ${countryRows}${totalExpected ? '/' + totalExpected : ''})\n`);
        if (rows.length === 0) break;
        if (totalExpected && countryRows >= totalExpected) break;
        if (page > 200) { console.log('[WDOMS] safety page cap hit'); break; }
        page++;
        await sleep(350);
      } catch (e) {
        console.log(`[WDOMS] ${c.countryName} page ${page} ERROR: ${e.message} — retrying in 3s`);
        await sleep(3000);
        try {
          const html = await searchPage(c.countryCode, page);
          const { rows, total } = parseResults(html, c.countryName);
          if (total !== null) totalExpected = total;
          for (const r of rows) {
            appendRow({ ...r, country_code: c.countryCode });
            globalRows++;
            countryRows++;
          }
          if (rows.length === 0) break;
          if (totalExpected && countryRows >= totalExpected) break;
          page++;
          await sleep(500);
        } catch (e2) {
          console.log(`[WDOMS] ${c.countryName} page ${page} second error: ${e2.message}. Skipping country.`);
          break;
        }
      }
    }
    state.doneCountries[c.countryCode] = { rows: countryRows, total: totalExpected, name: c.countryName };
    state.totalRows = globalRows;
    saveState(state);
  }

  console.log(`\n[WDOMS] Done. Total schools: ${globalRows}. CSV: ${OUT_CSV}`);
}

main().catch((e) => {
  console.error('[WDOMS] Fatal:', e);
  process.exit(1);
});
