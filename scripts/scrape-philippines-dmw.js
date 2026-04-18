#!/usr/bin/env node
/**
 * Philippines DMW (Department of Migrant Workers, formerly POEA)
 * Licensed Recruitment Agencies — Land-based
 *
 * Discovered API (by inspecting the SPA JS bundle at
 *   https://dmw.gov.ph/archives/v1/assets/licensed-recruitment-agencies-*.js):
 *
 *   GET https://master-api.dmw.gov.ph/api/v1/public/licensed-agencies?page={n}&name=
 *   Headers:
 *     Accept: application/json
 *     x-api-key: RTA0X0lOWFcycm9KU29WTlZxNDUzSDY5enc5OWFxY2ktWkxVdkFwZjEyMjkwNTA2MTE
 *
 * Response:
 *   { meta: { total, perPage, currentPage, lastPage, ... },
 *     data: [ { name, classification, license_status, license_status_date,
 *              license_expiration_date, is_valid, representative, address,
 *              municipality_province, city_province, contact_number, eMail,
 *              data_as_of } ] }
 *
 * Total: ~3,788 licensed agencies (as of 2026-04).
 *
 * Outputs CSV with columns matching mortar-lead-scraper conventions.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'philippines-dmw-recruitment-agencies.csv');
const STATE_FILE = OUTPUT.replace(/\.csv$/, '.state.json');
const API_BASE = 'https://master-api.dmw.gov.ph/api/v1/public/licensed-agencies';
const API_KEY = 'RTA0X0lOWFcycm9KU29WTlZxNDUzSDY5enc5OWFxY2ktWkxVdkFwZjEyMjkwNTA2MTE';
const DELAY_MS = 600;       // polite rate-limit
const MAX_RETRIES = 3;

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        Accept: 'application/json',
        'x-api-key': API_KEY,
        'User-Agent': 'Mozilla/5.0 (compatible; mortar-dmw-scraper/1.0)',
      },
      timeout: 30000,
    }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf-8'))); }
        catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function httpGetJsonRetry(url) {
  let lastErr;
  for (let i = 0; i < MAX_RETRIES; i++) {
    try { return await httpGetJson(url); }
    catch (e) { lastErr = e; await sleep(2000 * (i + 1)); }
  }
  throw lastErr;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function escapeCsv(v) {
  if (v == null) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n'))
    return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function splitEmail(raw) {
  // DMW returns multiple emails separated by "/" or ";" — pick the first valid one
  if (!raw) return '';
  const candidates = String(raw).split(/[\/;,]/).map((s) => s.trim()).filter(Boolean);
  const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  for (const c of candidates) {
    if (EMAIL_RE.test(c)) return c.toLowerCase();
  }
  return '';
}

function splitPhone(raw) {
  if (!raw) return '';
  // first phone number, stripped of spaces
  const candidates = String(raw).split(/[\/;,]/).map((s) => s.trim()).filter(Boolean);
  return candidates[0] || '';
}

function loadState() {
  if (!fs.existsSync(STATE_FILE)) return { lastPage: 0, seenKeys: {} };
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); }
  catch { return { lastPage: 0, seenKeys: {} }; }
}

function saveState(st) { fs.writeFileSync(STATE_FILE, JSON.stringify(st, null, 2)); }

function agencyKey(a) {
  // Dedup by name (no explicit license_no in API payload, but status_date +
  // expiration form a stable unique tuple too). Name is canonical here.
  return String(a.name || '').trim().toUpperCase();
}

async function main() {
  console.log('=== Philippines DMW (POEA) Licensed Recruitment Agencies ===');
  console.log('API:', API_BASE);

  const cols = [
    'agency_name', 'license_no', 'license_status', 'license_status_date',
    'license_expiration_date', 'classification', 'representative',
    'address', 'city', 'province', 'phone', 'email', 'website',
    'country', 'niche', 'source',
  ];

  const state = loadState();
  let firstWrite = !fs.existsSync(OUTPUT);
  const out = fs.createWriteStream(OUTPUT, { flags: firstWrite ? 'w' : 'a' });
  if (firstWrite) out.write(cols.join(',') + '\n');

  // Probe page 1 to learn totals
  const probe = await httpGetJsonRetry(`${API_BASE}?page=1&name=`);
  const total = probe.meta?.total || 0;
  const lastPage = probe.meta?.lastPage || 1;
  console.log(`Total agencies: ${total} | Pages: ${lastPage}`);

  let written = 0;
  let skipped = 0;
  let emailCount = 0;
  let phoneCount = 0;
  const startPage = Math.max(1, state.lastPage + 1);

  for (let page = startPage; page <= lastPage; page++) {
    let payload;
    try {
      payload = page === 1 ? probe : await httpGetJsonRetry(`${API_BASE}?page=${page}&name=`);
    } catch (e) {
      console.error(`Page ${page} failed: ${e.message}`);
      continue;
    }

    const rows = payload.data || [];
    for (const a of rows) {
      const key = agencyKey(a);
      if (!key || state.seenKeys[key]) { skipped++; continue; }
      state.seenKeys[key] = 1;

      const email = splitEmail(a.eMail);
      const phone = splitPhone(a.contact_number);
      if (email) emailCount++;
      if (phone) phoneCount++;

      const addressFull = [a.address, a.municipality_province, a.city_province]
        .filter(Boolean).map((s) => String(s).trim()).join(', ');

      const row = [
        a.name || '',
        '',  // license_no not provided in this feed (license_status_date + expiration are the distinguishers)
        a.license_status || '',
        a.license_status_date || '',
        a.license_expiration_date || '',
        a.classification || '',
        a.representative || '',
        addressFull,
        a.city_province || '',
        a.municipality_province || '',
        phone,
        email,
        '',  // website not in DMW feed
        'Philippines',
        'recruitment agency',
        'dmw-philippines',
      ].map(escapeCsv).join(',');

      out.write(row + '\n');
      written++;
    }

    state.lastPage = page;
    saveState(state);

    console.log(`Page ${page}/${lastPage}  rows:${rows.length}  written:${written}  email:${emailCount}  phone:${phoneCount}`);
    await sleep(DELAY_MS);
  }

  out.end();
  console.log(`\nDone. Wrote ${written} agencies (${emailCount} emails, ${phoneCount} phones). Skipped ${skipped} dupes.`);
  console.log(`Output: ${OUTPUT}`);
}

if (require.main === module) {
  main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
}
