#!/usr/bin/env node
/**
 * TherapyDen US therapist scraper.
 *
 * Source tree:
 *   https://www.therapyden.com/therapists/us                 -> 51 state pages
 *   https://www.therapyden.com/therapists/us/{state}         -> list of cities
 *   https://www.therapyden.com/therapists/us/{state}/{city}  -> ~20 therapist cards
 *   https://www.therapyden.com/therapist/{slug}              -> profile
 *
 * Notes:
 *   - `/therapists?page=N` is a SPA landing that just lists featured cities, not
 *     a paginated feed. `?page=2` on a city page returns identical content to
 *     page 1 (site caps at first 20 results per city).
 *   - The `/search` endpoint is Ajax + CSRF protected (Craft CMS mongo-db
 *     action). Too fragile to rely on.
 *   - City-tree iteration is the stable, polite approach: ~3-5k cities and
 *     ~20 therapists/city = ~40-80k unique therapist profile pages.
 *
 * Strategy:
 *   1. Fetch /therapists/us → extract 50 state codes + DC (51 total).
 *   2. For each state, fetch /therapists/us/{state} → extract city paths.
 *   3. For each city page, extract /therapist/{slug} URLs (20 per page).
 *   4. Fetch each unique profile → parse JSON-LD + DOM for contact info.
 *
 * Concurrency:
 *   - 1 at a time for state/city index pages (polite, 1-1.5s spacing)
 *   - 8 concurrent for profile fetches
 *
 * Output: /output/therapyden-therapists.csv
 *
 * Columns (shared with scrape-noomii-coaches.js):
 *   name, first_name, last_name, title, phone, email, website,
 *   city, state, category, price_range, profile_url, country, niche, source
 *
 * CLI:
 *   node scrape-therapyden-therapists.js                    # full run
 *   node scrape-therapyden-therapists.js --test             # 5 cities only
 *   node scrape-therapyden-therapists.js --max-cities 100   # cap cities
 *   node scrape-therapyden-therapists.js --state ny         # restrict to NY
 *   node scrape-therapyden-therapists.js --reset            # wipe state
 */

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'therapyden-therapists.csv');
const STATE_FILE = path.join(OUTPUT_DIR, '.therapyden-therapists-state.json');

const BASE = 'https://www.therapyden.com';

const LIST_DELAY_MS = 1200;
const PROFILE_DELAY_MS = 400;
const PROFILE_CONCURRENCY = 8;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 3;

const COLUMNS = [
  'name', 'first_name', 'last_name', 'title', 'phone', 'email', 'website',
  'city', 'state', 'category', 'price_range', 'profile_url',
  'country', 'niche', 'source',
];

// --- args -------------------------------------------------------------------
const args = process.argv.slice(2);
function argValue(flag) {
  const i = args.indexOf(flag);
  return i !== -1 ? args[i + 1] : null;
}
const TEST_MODE = args.includes('--test');
const RESET = args.includes('--reset');
const argMaxCities = parseInt(argValue('--max-cities'), 10) || null;
const argState = argValue('--state') ? argValue('--state').toLowerCase() : null;

// --- http --------------------------------------------------------------------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.get({
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: REQUEST_TIMEOUT_MS,
    }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        const loc = res.headers.location;
        res.resume();
        if (loc) return resolve(httpGet(loc.startsWith('http') ? loc : BASE + loc));
        return reject(new Error('Redirect with no location'));
      }
      if (res.statusCode !== 200) {
        res.resume();
        const err = new Error(`HTTP ${res.statusCode}`);
        err.statusCode = res.statusCode;
        return reject(err);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function httpGetRetry(url, attempts = MAX_RETRIES) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try { return await httpGet(url); }
    catch (err) {
      lastErr = err;
      // Heavy backoff on 429 (rate limit)
      const wait = err.statusCode === 429
        ? 5000 + 5000 * i
        : 1200 * (i + 1);
      await sleep(wait);
    }
  }
  throw lastErr;
}

// --- CSV ---------------------------------------------------------------------
function escapeCsv(v) {
  if (v == null) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(filePath, rows) {
  const lines = [COLUMNS.join(',')];
  for (const r of rows) lines.push(COLUMNS.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(filePath, lines.join('\n') + '\n');
}

// --- state -------------------------------------------------------------------
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

// --- helpers -----------------------------------------------------------------
function cleanPhone(raw) {
  if (!raw) return '';
  return String(raw).replace(/[^0-9+]/g, '').trim();
}

function splitName(full) {
  if (!full) return { first: '', last: '' };
  const s = String(full).trim().replace(/\s+/g, ' ');
  // strip pronoun suffix like "(He/Him/His)" or ", LMFT"
  const clean = s.replace(/\s*\([^)]*\)\s*$/, '').replace(/,.*$/, '').trim();
  const parts = clean.split(/\s+/);
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts.slice(1).join(' ') };
}

function extractJsonLd($) {
  const blocks = [];
  $('script[type="application/ld+json"]').each((i, el) => {
    try { blocks.push(JSON.parse($(el).contents().text())); } catch {}
  });
  return blocks;
}

function flattenLd(blocks) {
  const out = [];
  for (const b of blocks) {
    if (!b) continue;
    if (Array.isArray(b)) { out.push(...b); continue; }
    if (b['@graph'] && Array.isArray(b['@graph'])) out.push(...b['@graph']);
    else out.push(b);
  }
  return out;
}

function pickStr(v) {
  if (Array.isArray(v)) {
    for (const x of v) if (typeof x === 'string' && x.trim()) return x.trim();
    return '';
  }
  return typeof v === 'string' ? v.trim() : '';
}

function normalizeWebsite(u) {
  if (!u) return '';
  let s = String(u).trim();
  if (!s) return '';
  if (!/^https?:\/\//i.test(s)) s = 'http://' + s;
  return s;
}

// --- list/state discovery ---------------------------------------------------
async function discoverStates() {
  const html = await httpGetRetry(`${BASE}/therapists/us`);
  const $ = cheerio.load(html);
  const states = new Set();
  $('a[href^="/therapists/us/"]').each((i, el) => {
    const h = ($(el).attr('href') || '').split('#')[0].split('?')[0];
    const m = h.match(/^\/therapists\/us\/([a-z]{2})\/?$/i);
    if (m) states.add(m[1].toLowerCase());
  });
  return [...states].sort();
}

async function discoverCities(stateCode) {
  const html = await httpGetRetry(`${BASE}/therapists/us/${stateCode}`);
  const $ = cheerio.load(html);
  const cities = new Set();
  $(`a[href^="/therapists/us/${stateCode}/"]`).each((i, el) => {
    const h = ($(el).attr('href') || '').split('#')[0].split('?')[0];
    // exact shape: /therapists/us/XX/citySlug
    const m = h.match(new RegExp(`^/therapists/us/${stateCode}/([^/]+)$`, 'i'));
    if (m && m[1] && m[1].length > 0) cities.add(h);
  });
  return [...cities].sort();
}

async function extractTherapistUrls(cityPath) {
  // cityPath example: /therapists/us/ny/new-york
  const html = await httpGetRetry(`${BASE}${cityPath}`);
  const $ = cheerio.load(html);
  const urls = new Set();
  $('a[href^="/therapist/"]').each((i, el) => {
    const h = ($(el).attr('href') || '').split('#')[0].split('?')[0];
    if (h.startsWith('/therapist/') && h !== '/therapist/') urls.add(BASE + h);
  });
  return [...urls];
}

// --- profile parsing --------------------------------------------------------
function parseProfile(html, profileUrl, fallbackState) {
  const $ = cheerio.load(html);
  const blocks = flattenLd(extractJsonLd($));

  // TherapyDen profile ld has a Physician @type with name/telephone/address
  const phy = blocks.find(b => b && (
    b['@type'] === 'Physician' ||
    (Array.isArray(b['@type']) && b['@type'].includes('Physician'))
  ));

  const entity = phy || blocks.find(b => b && b.name && b.telephone) || {};

  const nameLdRaw = pickStr(entity.name);
  const telephone = cleanPhone(pickStr(entity.telephone));

  const address = entity.address || {};
  const city = pickStr(address.addressLocality);
  let state = pickStr(address.addressRegion).toUpperCase();
  if (!state) state = (fallbackState || '').toUpperCase();

  const countryRaw = pickStr(address.addressCountry);
  const country = countryRaw ? (countryRaw === 'US' ? 'United States' : countryRaw) : 'United States';

  // description → becomes a title fallback
  const description = pickStr(entity.description);

  // -- DOM side: name, title, email, website, price --
  // h1 has name with pronoun in parens: "Sohaib Javed (He/Him/His)"
  const h1 = $('h1').first().text().trim().replace(/\s+/g, ' ');
  const name = h1 || nameLdRaw;

  // credentials / license - typically rendered under the h1 as license title
  // e.g. "Licensed Clinical Mental Health Counselor, LMHC-D"
  let title = '';
  const NAV_JUNK_RE = /find help|informations?:|my clients|book.*online|contact|about me|my practice|specialties|credentials|education|insurance|services|sessions|fees?|languages/i;
  const CRED_POS_RE = /licens|counsel|therap|psychologist|social work|LCSW|LMFT|LPC|LMHC|PsyD|MSW|PhD|LMSW|LSW|RSW|MFT|M\.Ed|MA,|NCC|ACSW|CCHt|CSW|MHC|MCC|PMHNP|ChT/i;
  // try first sibling block under h1 with likely credential text
  const credEl = $('h1').first().nextAll().filter((i, el) => {
    const t = $(el).text().trim();
    if (!t || t.length > 200) return false;
    if (NAV_JUNK_RE.test(t)) return false;
    return CRED_POS_RE.test(t);
  }).first();
  if (credEl.length) title = credEl.text().trim().replace(/\s+/g, ' ');
  if (!title) {
    // try card-style class names
    const tEl = $('.card-title, .profession, [class*="license"], [class*="title"]').first();
    if (tEl.length) {
      const t = tEl.text().trim().replace(/\s+/g, ' ').substring(0, 200);
      if (!NAV_JUNK_RE.test(t)) title = t;
    }
  }
  // strip credential to title sentence if too long
  if (title.length > 250) title = title.substring(0, 250);

  // email — filter out therapyden's own contact email
  let email = '';
  $('a[href^="mailto:"]').each((i, el) => {
    if (email) return;
    const raw = ($(el).attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
    if (raw && !raw.endsWith('@therapyden.com')) email = raw;
  });

  // website — pull from external http links, filter socials + therapyden
  let website = '';
  $('a[href^="http"]').each((i, el) => {
    if (website) return;
    const h = $(el).attr('href') || '';
    if (!h) return;
    if (/therapyden\.com|facebook\.com|twitter\.com|linkedin\.com|instagram\.com|youtube\.com|tiktok\.com|pinterest\.com|google\.com\/maps|clientsecure\.me|simplepractice\.com|therapyportal\.com|schedule\.psychologytoday|psychologytoday\.com|squareup\.com|calendly\.com|cognitoforms\.com|therapyappointment\.com|jane\.app/i.test(h)) return;
    // skip asset links
    if (/\.(png|jpg|jpeg|gif|svg|ico|css|js)(\?|$)/i.test(h)) return;
    website = normalizeWebsite(h);
  });

  // price range — look for $nn - $nnn pattern in body
  let priceRange = '';
  const bodyTxt = $('body').text();
  const mp = bodyTxt.match(/\$\s*(\d{2,4})\s*[\-–—]\s*\$?\s*(\d{2,4})/);
  if (mp) priceRange = `$${mp[1]} - $${mp[2]}`;

  const { first, last } = splitName(name || nameLdRaw);

  return {
    name: name || nameLdRaw,
    first_name: first,
    last_name: last,
    title,
    phone: telephone,
    email,
    website,
    city,
    state,
    category: 'therapist',
    price_range: priceRange,
    profile_url: profileUrl,
    country,
    niche: 'therapist',
    source: 'therapyden',
  };
}

// --- batch profile fetcher --------------------------------------------------
async function fetchProfilesBatch(urls, fallbackStateMap, concurrency = PROFILE_CONCURRENCY) {
  const results = [];
  let idx = 0;
  async function worker() {
    while (idx < urls.length) {
      const myIdx = idx++;
      const url = urls[myIdx];
      try {
        const html = await httpGetRetry(url);
        const row = parseProfile(html, url, fallbackStateMap.get(url) || '');
        results.push(row);
      } catch (err) {
        results.push({
          name: '', first_name: '', last_name: '', title: '',
          phone: '', email: '', website: '',
          city: '', state: fallbackStateMap.get(url) || '',
          category: 'therapist',
          price_range: '',
          profile_url: url,
          country: 'United States',
          niche: 'therapist',
          source: 'therapyden',
          _error: err.message,
        });
      }
      await sleep(PROFILE_DELAY_MS);
    }
  }
  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);
  return results;
}

// --- main --------------------------------------------------------------------
async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('=== TherapyDen US Therapist Scraper ===');
  if (TEST_MODE) console.log('MODE: TEST (5 cities only)');
  if (argState) console.log(`STATE FILTER: ${argState}`);

  const state = loadState() || {
    stateCodes: null,
    stateCities: {},       // stateCode -> [/therapists/us/xx/city, ...]
    cityProfiles: {},      // cityPath -> [profileUrl, ...]
    profilesCompleted: [],
    rows: [],
  };
  const seenProfiles = new Set(state.profilesCompleted);
  const rows = state.rows.slice();

  // ---- Phase 1: state list ----
  if (!state.stateCodes || !state.stateCodes.length) {
    console.log('\n[phase 1] discovering states...');
    state.stateCodes = await discoverStates();
    console.log(`  found ${state.stateCodes.length} states: ${state.stateCodes.join(', ')}`);
    saveState(state);
  }

  const targetStates = argState
    ? state.stateCodes.filter(s => s === argState)
    : state.stateCodes;

  // ---- Phase 2: discover cities per state ----
  console.log('\n[phase 2] discovering cities per state...');
  for (const sc of targetStates) {
    if (state.stateCities[sc] && state.stateCities[sc].length) {
      console.log(`  [${sc}] cached ${state.stateCities[sc].length} cities`);
      continue;
    }
    try {
      const cities = await discoverCities(sc);
      state.stateCities[sc] = cities;
      console.log(`  [${sc}] found ${cities.length} cities`);
      saveState(state);
    } catch (err) {
      console.warn(`  [${sc}] FAILED: ${err.message}`);
      state.stateCities[sc] = [];
      saveState(state);
    }
    await sleep(LIST_DELAY_MS);
  }

  // ---- Phase 3: gather therapist URLs per city ----
  // Build a flat list of (cityPath, stateCode) to iterate
  let cityList = [];
  for (const sc of targetStates) {
    for (const cp of (state.stateCities[sc] || [])) {
      cityList.push({ cityPath: cp, stateCode: sc });
    }
  }
  if (TEST_MODE) cityList = cityList.slice(0, 5);
  if (argMaxCities) cityList = cityList.slice(0, argMaxCities);
  console.log(`\n[phase 3] ${cityList.length} cities to crawl for therapist URLs`);

  let cityIdx = 0;
  for (const { cityPath, stateCode } of cityList) {
    cityIdx++;
    if (state.cityProfiles[cityPath]) {
      // already done
      continue;
    }
    try {
      const urls = await extractTherapistUrls(cityPath);
      state.cityProfiles[cityPath] = urls;
      console.log(`  [${cityIdx}/${cityList.length}] ${cityPath} → ${urls.length} therapists`);
    } catch (err) {
      console.warn(`  [${cityIdx}/${cityList.length}] ${cityPath} FAILED: ${err.message}`);
      state.cityProfiles[cityPath] = [];
    }
    if (cityIdx % 25 === 0) saveState(state);
    await sleep(LIST_DELAY_MS);
  }
  saveState(state);

  // ---- Phase 4: fetch all unique therapist profiles ----
  const urlToState = new Map();
  for (const sc of targetStates) {
    for (const cp of (state.stateCities[sc] || [])) {
      for (const u of (state.cityProfiles[cp] || [])) {
        if (!urlToState.has(u)) urlToState.set(u, sc.toUpperCase());
      }
    }
  }
  const allUrls = [...urlToState.keys()].filter(u => !seenProfiles.has(u));
  console.log(`\n[phase 4] ${allUrls.length} unique profile URLs to fetch (${seenProfiles.size} already done)`);

  const BATCH = 40;
  for (let i = 0; i < allUrls.length; i += BATCH) {
    const chunk = allUrls.slice(i, i + BATCH);
    const batchRows = await fetchProfilesBatch(chunk, urlToState);

    for (const row of batchRows) {
      if (row._error) {
        console.warn(`  [profile] ERR ${row.profile_url} — ${row._error}`);
        seenProfiles.add(row.profile_url);
        continue;
      }
      if (!row.name && !row.phone && !row.city) {
        seenProfiles.add(row.profile_url);
        continue;
      }
      rows.push(row);
      seenProfiles.add(row.profile_url);
    }

    state.profilesCompleted = [...seenProfiles];
    state.rows = rows;
    saveState(state);

    const withPhone = rows.filter(r => r.phone).length;
    const withEmail = rows.filter(r => r.email).length;
    const withWebsite = rows.filter(r => r.website).length;
    console.log(`  [batch ${Math.floor(i/BATCH)+1}/${Math.ceil(allUrls.length/BATCH)}] total=${rows.length} phone=${withPhone} email=${withEmail} site=${withWebsite}`);
  }

  // ---- Phase 5: final CSV ----
  writeCsv(OUTPUT_FILE, rows);
  const withPhone = rows.filter(r => r.phone).length;
  const withEmail = rows.filter(r => r.email).length;
  const withWebsite = rows.filter(r => r.website).length;
  const withCity = rows.filter(r => r.city).length;

  console.log('\n=== RESULTS ===');
  console.log(`Total rows:    ${rows.length}`);
  console.log(`With phone:    ${withPhone}`);
  console.log(`With email:    ${withEmail}`);
  console.log(`With website:  ${withWebsite}`);
  console.log(`With city:     ${withCity}`);
  console.log(`Output:        ${OUTPUT_FILE}`);
  console.log(`State:         ${STATE_FILE}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
