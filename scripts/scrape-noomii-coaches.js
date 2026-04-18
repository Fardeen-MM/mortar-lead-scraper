#!/usr/bin/env node
/**
 * Noomii coach directory scraper.
 *
 * Source:  https://www.noomii.com/{category}?page=N
 * Profile: https://www.noomii.com/users/{slug}
 *
 * Strategy:
 *   - Iterate 6 categories × pages 1..N
 *         /life-coaches, /business-coaches, /executive-coaches,
 *         /career-coaches, /leadership-coaches, /health-and-fitness-coaches
 *   - Each listing page renders ~20 unique .coach cards; dedup across
 *     categories by profile_url (same coach shows under multiple categories)
 *   - For each unique profile, fetch the /users/{slug} page and parse JSON-LD
 *     (it carries name, telephone, address, priceRange, sameAs[external site],
 *      jobTitle, description) — this is richer than any card-level info
 *   - Concurrency 8 for profile fetches. Polite 1–1.5s between list pages.
 *   - Resumable state saved every 25 profile batches (200 profiles)
 *
 * Output:
 *   /output/noomii-coaches.csv
 *
 * Columns (shared with scrape-therapyden-therapists.js):
 *   name, first_name, last_name, title, phone, email, website,
 *   city, state, category, price_range, profile_url, country, niche, source
 *
 * CLI:
 *   node scrape-noomii-coaches.js                  # full run
 *   node scrape-noomii-coaches.js --test           # 5 list pages / category only
 *   node scrape-noomii-coaches.js --max-pages 50   # cap list pages
 *   node scrape-noomii-coaches.js --reset          # wipe state and restart
 *   node scrape-noomii-coaches.js --category life-coaches   # single category
 */

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'noomii-coaches.csv');
const STATE_FILE = path.join(OUTPUT_DIR, '.noomii-coaches-state.json');

const BASE = 'https://www.noomii.com';

const CATEGORIES = [
  'life-coaches',
  'business-coaches',
  'executive-coaches',
  'career-coaches',
  'leadership-coaches',
  'health-and-fitness-coaches',
];

const LIST_DELAY_MS = 1200;
const PROFILE_DELAY_MS = 400;          // between individual profile batches
const PROFILE_CONCURRENCY = 8;
const SAVE_EVERY_PROFILES = 200;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 3;
const MAX_EMPTY_PAGES = 3;

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
const argMaxPages = parseInt(argValue('--max-pages'), 10) || null;
const argCategory = argValue('--category');

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
        return reject(new Error(`Redirect with no location`));
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
  const clean = String(full).trim().replace(/\s+/g, ' ');
  // strip trailing credentials (MBA, PCC, ACC, ICF etc.) after the last letter name
  // simplest: split on first space for first, rest for last
  const parts = clean.split(/\s+/);
  if (parts.length === 1) return { first: parts[0], last: '' };
  // drop trailing credentials like MBA, PhD, LCSW, MA, MS, ICF, PCC, ACC, CPC,
  // EdD, MSc, RN, LLC, ltd, Jr, Sr
  const CREDS = /^(MBA|PhD|LCSW|MA|MS|MSc|PCC|ACC|CPC|ICF|EdD|Jr|Sr|II|III|IV|RN|LLC|Ltd|CMC|MCC|CTP|MEd|MPH|MFA|LPC|LMFT)[\.,]?$/i;
  while (parts.length > 2 && CREDS.test(parts[parts.length - 1].replace(/[.,]$/, ''))) parts.pop();
  const first = parts[0];
  const last = parts.slice(1).join(' ');
  return { first, last };
}

function extractJsonLd($) {
  const blocks = [];
  $('script[type="application/ld+json"]').each((i, el) => {
    try {
      const raw = $(el).contents().text();
      blocks.push(JSON.parse(raw));
    } catch { /* ignore */ }
  });
  return blocks;
}

function flattenLd(blocks) {
  // flatten any @graph containers so we can scan a list of entities
  const out = [];
  for (const b of blocks) {
    if (!b) continue;
    if (Array.isArray(b)) { out.push(...b); continue; }
    if (b['@graph'] && Array.isArray(b['@graph'])) out.push(...b['@graph']);
    else out.push(b);
  }
  return out;
}

function pickFirstString(...vals) {
  for (const v of vals) {
    if (Array.isArray(v)) {
      for (const x of v) if (typeof x === 'string' && x.trim()) return x.trim();
    } else if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return '';
}

function normalizeWebsite(u) {
  if (!u) return '';
  let s = String(u).trim();
  if (!/^https?:\/\//i.test(s)) s = 'http://' + s;
  return s;
}

function stateAbbrev(stateName) {
  if (!stateName) return '';
  const map = {
    'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA',
    'colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA',
    'hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA',
    'kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD',
    'massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS',
    'missouri':'MO','montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH',
    'new jersey':'NJ','new mexico':'NM','new york':'NY','north carolina':'NC',
    'north dakota':'ND','ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA',
    'rhode island':'RI','south carolina':'SC','south dakota':'SD','tennessee':'TN',
    'texas':'TX','utah':'UT','vermont':'VT','virginia':'VA','washington':'WA',
    'west virginia':'WV','wisconsin':'WI','wyoming':'WY','district of columbia':'DC',
  };
  const k = String(stateName).trim().toLowerCase();
  if (map[k]) return map[k];
  // already a code?
  if (/^[A-Z]{2}$/i.test(k)) return k.toUpperCase();
  return stateName;
}

// --- list page parsing -------------------------------------------------------
function parseListPage(html) {
  const $ = cheerio.load(html);
  const profileUrls = new Set();
  $('.coach a[href^="/users/"]').each((i, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const clean = href.split('#')[0].split('?')[0];
    if (clean.startsWith('/users/') && clean !== '/users/') profileUrls.add(BASE + clean);
  });
  return [...profileUrls];
}

// --- profile parsing ---------------------------------------------------------
function parseProfile(html, profileUrl, category) {
  const $ = cheerio.load(html);
  const blocks = flattenLd(extractJsonLd($));

  const localBiz = blocks.find(b => b && (b['@type'] === 'LocalBusiness' || (Array.isArray(b['@type']) && b['@type'].includes('LocalBusiness'))));
  const person = blocks.find(b => b && (b['@type'] === 'Person' || (Array.isArray(b['@type']) && b['@type'].includes('Person'))));

  // Accept either / merge
  const entity = localBiz || person || {};

  const name = pickFirstString(entity.name, $('h1').first().text().trim());
  const telephone = cleanPhone(pickFirstString(entity.telephone));
  const priceRange = pickFirstString(entity.priceRange);

  const address = entity.address || {};
  const city = pickFirstString(address.addressLocality);
  const stateRaw = pickFirstString(address.addressRegion);
  const countryRaw = pickFirstString(address.addressCountry);
  const country = countryRaw || 'United States';
  const state = stateAbbrev(stateRaw);

  // sameAs = array of external profile links; we want the non-social,
  // non-noomii one as the coach's website
  let website = '';
  const sameAs = [].concat(entity.sameAs || person?.sameAs || []);
  for (const s of sameAs) {
    if (!s || typeof s !== 'string') continue;
    if (/facebook\.com|twitter\.com|linkedin\.com|instagram\.com|youtube\.com|tiktok\.com|pinterest\.com|noomii\.com/i.test(s)) continue;
    website = normalizeWebsite(s);
    break;
  }

  // Fallback: scan any external link that is clearly a personal website
  if (!website) {
    $('a[href^="http"]').each((i, el) => {
      if (website) return;
      const h = $(el).attr('href') || '';
      if (/noomii\.com|facebook\.com|twitter\.com|linkedin\.com|instagram\.com|youtube\.com|tiktok\.com|pinterest\.com/i.test(h)) return;
      website = normalizeWebsite(h);
    });
  }

  // jobTitle may come from person entity — else use category as fallback
  const title = pickFirstString(person?.jobTitle, entity.jobTitle);

  // first + last name
  const { first, last } = splitName(name);

  return {
    name,
    first_name: first,
    last_name: last,
    title,
    phone: telephone,
    email: '',          // Noomii doesn't expose emails on profile (contact form only)
    website,
    city,
    state,
    category,
    price_range: priceRange,
    profile_url: profileUrl,
    country,
    niche: 'coach',
    source: 'noomii',
  };
}

// --- per-category listing crawler -------------------------------------------
async function discoverTotalPages(categorySlug) {
  const html = await httpGetRetry(`${BASE}/${categorySlug}?page=1`);
  const $ = cheerio.load(html);
  let maxPage = 1;
  $('a[href*="page="]').each((i, el) => {
    const m = ($(el).attr('href') || '').match(/page=(\d+)/);
    if (m) maxPage = Math.max(maxPage, parseInt(m[1], 10));
  });
  // first page profile URLs so caller can reuse:
  const urls = parseListPage(html);
  return { totalPages: maxPage, firstPageUrls: urls };
}

async function crawlCategory(categorySlug, maxPages, knownUrls) {
  // Page 1 was already probed in discoverTotalPages — we re-fetch it here for
  // simplicity but it's cheap because results are tiny.
  const allUrls = new Set();
  let emptyStreak = 0;
  for (let page = 1; page <= maxPages; page++) {
    let urls;
    try {
      const html = await httpGetRetry(`${BASE}/${categorySlug}?page=${page}`);
      urls = parseListPage(html);
    } catch (err) {
      console.warn(`  [${categorySlug} p${page}] list fetch FAILED: ${err.message}`);
      await sleep(LIST_DELAY_MS * 2);
      continue;
    }

    if (urls.length === 0) {
      emptyStreak++;
      console.log(`  [${categorySlug} p${page}] 0 cards (streak=${emptyStreak})`);
      if (emptyStreak >= MAX_EMPTY_PAGES) {
        console.log(`  [${categorySlug}] ${MAX_EMPTY_PAGES} empty pages — stop`);
        break;
      }
    } else {
      emptyStreak = 0;
    }

    let newCount = 0;
    for (const u of urls) {
      if (!allUrls.has(u)) { allUrls.add(u); newCount++; }
    }
    console.log(`  [${categorySlug} p${page}/${maxPages}] +${newCount} (cat uniq=${allUrls.size}, global=${knownUrls.size + allUrls.size})`);
    await sleep(LIST_DELAY_MS);
  }
  return [...allUrls];
}

// --- batch profile fetcher ---------------------------------------------------
async function fetchProfilesBatch(urls, category, concurrency = PROFILE_CONCURRENCY) {
  const results = [];
  let idx = 0;
  async function worker() {
    while (idx < urls.length) {
      const myIdx = idx++;
      const url = urls[myIdx];
      try {
        const html = await httpGetRetry(url);
        const row = parseProfile(html, url, category);
        results.push(row);
      } catch (err) {
        results.push({
          name: '', first_name: '', last_name: '', title: '',
          phone: '', email: '', website: '',
          city: '', state: '', category,
          price_range: '',
          profile_url: url,
          country: '',
          niche: 'coach',
          source: 'noomii',
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

  console.log('=== Noomii Coach Scraper ===');
  if (TEST_MODE) console.log('MODE: TEST (5 list pages per category)');
  if (argCategory) console.log(`CATEGORY FILTER: ${argCategory}`);

  const state = loadState() || {
    categoryUrls: {},           // slug -> [profileUrl,...]
    profilesCompleted: [],      // [profileUrl,...]
    rows: [],
  };
  const seenProfiles = new Set(state.profilesCompleted);
  const rows = state.rows.slice();

  const targetCategories = argCategory
    ? CATEGORIES.filter(c => c === argCategory)
    : CATEGORIES;

  // ---- Phase 1: discover profile URLs per category ----
  for (const cat of targetCategories) {
    if (state.categoryUrls[cat] && state.categoryUrls[cat].length) {
      console.log(`[cat ${cat}] using cached ${state.categoryUrls[cat].length} URLs`);
      continue;
    }

    console.log(`\n[cat ${cat}] probing for total pages...`);
    let totalPages, firstPageUrls;
    try {
      ({ totalPages, firstPageUrls } = await discoverTotalPages(cat));
    } catch (err) {
      console.warn(`[cat ${cat}] probe FAILED: ${err.message}`);
      state.categoryUrls[cat] = [];
      saveState(state);
      continue;
    }

    const effectiveMax = TEST_MODE
      ? Math.min(5, totalPages)
      : Math.min(argMaxPages || totalPages, totalPages);
    console.log(`[cat ${cat}] totalPages=${totalPages}, will crawl ${effectiveMax}`);

    const seedSet = new Set(firstPageUrls);
    // We will re-crawl starting at page 1 inside crawlCategory. To keep logic
    // simple and avoid duplicating the page-1 fetch, just trust it.
    const urls = await crawlCategory(cat, effectiveMax, new Set());
    // Merge with firstPageUrls just in case
    for (const u of firstPageUrls) urls.push(u);
    const uniq = [...new Set(urls)];
    state.categoryUrls[cat] = uniq;
    console.log(`[cat ${cat}] discovered ${uniq.length} unique profile URLs`);
    saveState(state);
  }

  // ---- Phase 2: fetch each unique profile URL ----
  // Build a merged list across categories, dedup, preserve which category found it first
  const urlToCategory = new Map();
  for (const cat of targetCategories) {
    for (const u of (state.categoryUrls[cat] || [])) {
      if (!urlToCategory.has(u)) urlToCategory.set(u, cat);
    }
  }
  const allUrls = [...urlToCategory.keys()].filter(u => !seenProfiles.has(u));
  console.log(`\n[phase 2] ${allUrls.length} profiles to fetch (${seenProfiles.size} already done)`);

  // fetch in chunks of ~40 at a time then persist state
  const BATCH = 40;
  for (let i = 0; i < allUrls.length; i += BATCH) {
    const chunk = allUrls.slice(i, i + BATCH);
    const byCatChunks = new Map();
    for (const u of chunk) {
      const c = urlToCategory.get(u);
      if (!byCatChunks.has(c)) byCatChunks.set(c, []);
      byCatChunks.get(c).push(u);
    }

    // Just fetch all as one batch but carry category info in the call
    const batchResults = [];
    for (const [cat, urls] of byCatChunks.entries()) {
      const r = await fetchProfilesBatch(urls, cat);
      batchResults.push(...r);
    }

    for (const row of batchResults) {
      if (row._error) {
        console.warn(`  [profile] ERR ${row.profile_url} — ${row._error}`);
        seenProfiles.add(row.profile_url);       // skip retrying this run
        continue;
      }
      if (!row.name && !row.phone && !row.website) {
        // empty row, skip but mark done
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
    const withWebsite = rows.filter(r => r.website).length;
    console.log(`  [batch ${Math.floor(i/BATCH)+1}/${Math.ceil(allUrls.length/BATCH)}] total=${rows.length} phone=${withPhone} website=${withWebsite}`);
  }

  // ---- Phase 3: final CSV write ----
  writeCsv(OUTPUT_FILE, rows);
  const withPhone = rows.filter(r => r.phone).length;
  const withWebsite = rows.filter(r => r.website).length;
  const withCity = rows.filter(r => r.city).length;

  console.log('\n=== RESULTS ===');
  console.log(`Total rows:    ${rows.length}`);
  console.log(`With phone:    ${withPhone}`);
  console.log(`With website:  ${withWebsite}`);
  console.log(`With city:     ${withCity}`);
  console.log(`Output:        ${OUTPUT_FILE}`);
  console.log(`State:         ${STATE_FILE}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
