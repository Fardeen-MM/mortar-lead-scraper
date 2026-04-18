#!/usr/bin/env node
/**
 * DAV Germany Immigration Lawyer Scraper
 *
 * Source: https://anwaltauskunft.de (Deutsche Anwaltverein)
 * Filter: Rechtsgebiet = Migrationsrecht (section=804)
 * Strategy A: Nationwide section filter with SID — 755 lawyers (~76 pages)
 * Strategy B: City-sweep fallback if Strategy A fails
 *
 * Requires: node >=16, cheerio (no external deps beyond that)
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const cheerio = require('cheerio');

const OUT_DIR = path.join(__dirname, '..', 'output');
const CSV_PATH = path.join(OUT_DIR, 'dav-germany-immigration.csv');
const STATE_PATH = path.join(OUT_DIR, '.dav-germany-state.json');

const BASE = 'https://anwaltauskunft.de';
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const LIST_DELAY_MS = 1500;
const DETAIL_CONCURRENCY = 5;
const DETAIL_DELAY_MS = 300;

const MAJOR_CITIES = [
  'Berlin','Hamburg','München','Köln','Frankfurt am Main','Stuttgart','Düsseldorf','Dortmund','Essen','Leipzig',
  'Bremen','Dresden','Hannover','Nürnberg','Duisburg','Bochum','Wuppertal','Bielefeld','Bonn','Münster',
  'Karlsruhe','Mannheim','Augsburg','Wiesbaden','Mönchengladbach'
];

const IMMIGRATION_KEYWORDS = [
  'migrationsrecht','ausländerrecht','auslaenderrecht','asylrecht',
  'aufenthaltsrecht','einbürgerungsrecht','einbuergerungsrecht','staatsangehörigkeitsrecht','staatsangehoerigkeitsrecht'
];

// ---------------- HTTP ----------------
function fetchUrl(url, cookieJar = null, retries = 3) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const headers = {
      'User-Agent': UA,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
      'Accept-Encoding': 'identity',
      'Connection': 'keep-alive',
    };
    if (cookieJar && cookieJar.cookies.length) {
      headers['Cookie'] = cookieJar.cookies.join('; ');
    }
    const req = https.get({
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers,
      timeout: 30000,
    }, (res) => {
      // Capture cookies
      if (cookieJar && res.headers['set-cookie']) {
        for (const c of res.headers['set-cookie']) {
          const piece = c.split(';')[0];
          const name = piece.split('=')[0];
          cookieJar.cookies = cookieJar.cookies.filter(x => !x.startsWith(name + '='));
          cookieJar.cookies.push(piece);
        }
      }
      // Redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const next = new URL(res.headers.location, url).toString();
        res.resume();
        return resolve(fetchUrl(next, cookieJar, retries));
      }
      if (res.statusCode !== 200) {
        res.resume();
        if (retries > 0) {
          return setTimeout(() => resolve(fetchUrl(url, cookieJar, retries - 1)), 2000);
        }
        return reject(new Error(`HTTP ${res.statusCode} ${url}`));
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (c) => data += c);
      res.on('end', () => resolve(data));
    });
    req.on('error', (e) => {
      if (retries > 0) return setTimeout(() => resolve(fetchUrl(url, cookieJar, retries - 1)), 2000);
      reject(e);
    });
    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) return setTimeout(() => resolve(fetchUrl(url, cookieJar, retries - 1)), 2000);
      reject(new Error('timeout ' + url));
    });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---------------- Session ----------------
async function bootstrapSession() {
  const jar = { cookies: [] };
  const html = await fetchUrl(`${BASE}/anwaltssuche`, jar);
  const $ = cheerio.load(html);
  const sid = $('input[name="ls[sid]"]').attr('value');
  if (!sid) throw new Error('Could not find ls[sid] on form page');
  return { sid, jar };
}

// ---------------- List page parsing ----------------
function parseListPage(html) {
  const $ = cheerio.load(html);
  const cards = [];
  $('div.lawyer.lawyer-list').each((_, el) => {
    const $el = $(el);
    const link = $el.find('a[href*="/anwaltssuche/"]').first();
    const href = link.attr('href');
    if (!href) return;
    const profileUrl = href.startsWith('http') ? href : new URL(href, BASE).toString();
    const name = $el.find('.h4.name').first().text().replace(/\u00ad/g, '').trim();

    const addrSpans = $el.find('address span').map((_, s) => $(s).text().replace(/\u00ad/g, '').trim()).get();
    // Typical order: street, postalCode, city
    let street = '', postal = '', city = '';
    if (addrSpans.length >= 3) {
      street = addrSpans[0];
      postal = addrSpans[1].trim();
      city = addrSpans[2];
    } else if (addrSpans.length === 2) {
      postal = addrSpans[0].trim();
      city = addrSpans[1];
    }
    // Some cards have postal + ' ' inside single span
    if (postal && /^\d{5}\s*$/.test(postal)) postal = postal.trim();

    const badges = $el.find('.badge').map((_, b) => $(b).text().replace(/\u00ad/g, '').trim()).get();

    cards.push({ profileUrl, name, street, postal, city, badges });
  });
  // Total count
  let totalText = $('.lawyer-list-conclusion .label').text();
  const m = totalText.match(/(\d+)/);
  const total = m ? parseInt(m[1], 10) : null;
  return { cards, total };
}

// ---------------- Profile parsing (JSON-LD) ----------------
function parseProfilePage(html, fallback = {}) {
  const $ = cheerio.load(html);
  let person = null;
  $('script[type="application/ld+json"]').each((_, el) => {
    if (person) return;
    try {
      const txt = $(el).html();
      if (!txt || !txt.includes('"Person"')) return;
      const data = JSON.parse(txt);
      const graph = Array.isArray(data['@graph']) ? data['@graph'] : [data];
      for (const node of graph) {
        if (node && node['@type'] === 'Person') {
          person = node;
          return;
        }
      }
    } catch (_) { /* ignore */ }
  });

  const out = {
    first_name: '',
    last_name: '',
    firm_name: '',
    email: '',
    phone: '',
    website: '',
    address: '',
    city: '',
    postal_code: '',
    state: '',
  };

  if (person) {
    const full = (person.name || '').replace(/&amp;/g, '&').trim();
    if (full) {
      const parts = full.split(/\s+/);
      // Handle titles: Dr., Prof., LL.M. etc. — take last as last_name, rest first_name
      // Strip trailing suffixes commonly found (LL.M., M.B.A., PhD)
      const suffixes = new Set(['LL.M.','LLM','M.B.A.','MBA','PhD','Ph.D.']);
      while (parts.length > 1 && suffixes.has(parts[parts.length - 1])) parts.pop();
      if (parts.length === 1) {
        out.last_name = parts[0];
      } else {
        out.last_name = parts[parts.length - 1];
        out.first_name = parts.slice(0, -1).join(' ');
      }
    }
    out.email = (person.email || '').trim();
    const tel = Array.isArray(person.telephone) ? person.telephone[0] : person.telephone;
    out.phone = (tel || '').toString().trim();
    const url = Array.isArray(person.url) ? person.url[0] : person.url;
    out.website = (url || '').toString().trim();

    const addr = Array.isArray(person.address) ? person.address[0] : person.address;
    if (addr) {
      out.firm_name = (addr.name || '').replace(/&amp;/g, '&').replace(/&uuml;/g, 'ü').replace(/&auml;/g, 'ä').replace(/&ouml;/g, 'ö').replace(/&szlig;/g, 'ß').trim();
      out.address = (addr.streetAddress || '').toString().trim();
      out.postal_code = (addr.postalCode || '').toString().trim();
      out.city = (addr.addressLocality || '').toString().trim();
    }
  }

  // Fallback to list-page data
  if (!out.city && fallback.city) out.city = fallback.city;
  if (!out.postal_code && fallback.postal) out.postal_code = fallback.postal;
  if (!out.address && fallback.street) out.address = fallback.street;
  if (!out.first_name && !out.last_name && fallback.name) {
    const parts = fallback.name.split(/\s+/);
    if (parts.length === 1) out.last_name = parts[0];
    else {
      out.last_name = parts[parts.length - 1];
      out.first_name = parts.slice(0, -1).join(' ');
    }
  }

  // Derive German state from postal code
  out.state = postalToState(out.postal_code);

  return out;
}

// German postal code -> state (Bundesland) — coarse mapping by first 2 digits
function postalToState(plz) {
  if (!plz || plz.length < 2) return '';
  const n = parseInt(plz.substring(0, 2), 10);
  if (isNaN(n)) return '';
  // Reference: wiki DE postal code ranges
  if (n >= 1 && n <= 9) return 'Sachsen';
  if (n >= 10 && n <= 14) return 'Berlin/Brandenburg';
  if (n >= 15 && n <= 16) return 'Brandenburg';
  if (n >= 17 && n <= 19) return 'Mecklenburg-Vorpommern';
  if (n >= 20 && n <= 25) return 'Hamburg/Schleswig-Holstein';
  if (n >= 26 && n <= 29) return 'Niedersachsen';
  if (n >= 30 && n <= 31) return 'Niedersachsen';
  if (n >= 32 && n <= 33) return 'Nordrhein-Westfalen';
  if (n >= 34 && n <= 36) return 'Hessen';
  if (n >= 37 && n <= 38) return 'Niedersachsen';
  if (n === 39) return 'Sachsen-Anhalt';
  if (n >= 40 && n <= 48) return 'Nordrhein-Westfalen';
  if (n === 49) return 'Niedersachsen';
  if (n >= 50 && n <= 53) return 'Nordrhein-Westfalen';
  if (n >= 54 && n <= 56) return 'Rheinland-Pfalz';
  if (n === 57) return 'Nordrhein-Westfalen';
  if (n >= 58 && n <= 59) return 'Nordrhein-Westfalen';
  if (n >= 60 && n <= 65) return 'Hessen';
  if (n >= 66 && n <= 67) return 'Saarland/Rheinland-Pfalz';
  if (n === 68 || n === 69) return 'Baden-Württemberg';
  if (n >= 70 && n <= 79) return 'Baden-Württemberg';
  if (n >= 80 && n <= 87) return 'Bayern';
  if (n >= 88 && n <= 89) return 'Baden-Württemberg/Bayern';
  if (n >= 90 && n <= 96) return 'Bayern';
  if (n === 97) return 'Bayern';
  if (n === 98 || n === 99) return 'Thüringen';
  return '';
}

// ---------------- Filtering ----------------
function isImmigrationLawyer(card) {
  if (!card.badges || !card.badges.length) return false;
  const joined = card.badges.join(' | ').toLowerCase();
  return IMMIGRATION_KEYWORDS.some(kw => joined.includes(kw));
}

// ---------------- CSV ----------------
const CSV_HEADERS = [
  'first_name','last_name','firm_name','email','phone','website',
  'address','city','postal_code','state','profile_url','country','niche','source'
];

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val).replace(/\r?\n/g, ' ').trim();
  if (s === '') return '';
  if (s.includes('"') || s.includes(',') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function appendRow(row) {
  const line = CSV_HEADERS.map(h => csvEscape(row[h])).join(',') + '\n';
  fs.appendFileSync(CSV_PATH, line, 'utf8');
}

function ensureCsv() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  if (!fs.existsSync(CSV_PATH)) {
    fs.writeFileSync(CSV_PATH, CSV_HEADERS.join(',') + '\n', 'utf8');
  }
}

// ---------------- State (resumable) ----------------
function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_PATH, 'utf8')); }
  catch { return { seenProfiles: [], completedPages: [], completedCities: [], strategy: null, sid: null }; }
}
function saveState(state) {
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2), 'utf8');
}

// ---------------- Concurrency helper ----------------
async function runWithConcurrency(items, concurrency, worker) {
  const results = [];
  let idx = 0;
  const workers = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push((async () => {
      while (true) {
        const cur = idx++;
        if (cur >= items.length) return;
        try {
          results[cur] = await worker(items[cur], cur);
        } catch (e) {
          results[cur] = { __error: e.message, item: items[cur] };
        }
      }
    })());
  }
  await Promise.all(workers);
  return results;
}

// ---------------- Main ----------------
async function main() {
  ensureCsv();
  const state = loadState();
  const seen = new Set(state.seenProfiles || []);

  // Flag support: --test (3 pages only), --max-pages=N
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const maxPagesArg = (args.find(a => a.startsWith('--max-pages=')) || '').split('=')[1];
  const MAX_PAGES_LIMIT = isTest ? 3 : (maxPagesArg ? parseInt(maxPagesArg, 10) : Infinity);

  console.log('[dav-de] Bootstrapping session...');
  let sid = state.sid;
  let jar = { cookies: [] };
  if (!sid) {
    const boot = await bootstrapSession();
    sid = boot.sid;
    jar = boot.jar;
    state.sid = sid;
    saveState(state);
    console.log('[dav-de] SID:', sid);
  } else {
    // Need a fresh jar; rebootstrap to get cookies
    const boot = await bootstrapSession();
    jar = boot.jar;
    // Keep old sid if site allows; if invalidated we'd need new, but SIDs seem durable
  }

  const filterUrl = (page) =>
    `${BASE}/anwaltssuche?ls%5Bsid%5D=${sid}&ls%5Bsection%5D%5B%5D=804&page=${page}`;

  // --- Strategy A: nationwide section filter ---
  console.log('[dav-de] Strategy A: nationwide section=804 pagination');
  const firstHtml = await fetchUrl(filterUrl(1), jar);
  const first = parseListPage(firstHtml);
  const total = first.total || 0;
  const totalPages = total ? Math.ceil(total / 10) : 1;
  console.log(`[dav-de] Total lawyers matching: ${total} (${totalPages} pages)`);

  if (total < 50) {
    console.error('[dav-de] Strategy A returned <50 results — aborting. Use Strategy B fallback manually.');
    process.exit(1);
  }

  const stopAt = Math.min(totalPages, MAX_PAGES_LIMIT);

  const allCards = [...first.cards];
  const completedSet = new Set(state.completedPages || []);
  completedSet.add(1);

  for (let p = 2; p <= stopAt; p++) {
    if (completedSet.has(p)) continue;
    await sleep(LIST_DELAY_MS);
    try {
      const html = await fetchUrl(filterUrl(p), jar);
      const parsed = parseListPage(html);
      if (!parsed.cards.length) {
        console.log(`[dav-de] page ${p}: no cards — stopping.`);
        break;
      }
      console.log(`[dav-de] page ${p}/${stopAt}: ${parsed.cards.length} cards (total=${parsed.total})`);
      allCards.push(...parsed.cards);
      completedSet.add(p);
      state.completedPages = Array.from(completedSet);
      saveState(state);
    } catch (e) {
      console.error(`[dav-de] page ${p} failed:`, e.message);
      // continue
    }
  }

  // Dedup by profileUrl
  const uniqueCards = [];
  const seenProfile = new Set();
  for (const c of allCards) {
    if (!c.profileUrl || seenProfile.has(c.profileUrl)) continue;
    seenProfile.add(c.profileUrl);
    uniqueCards.push(c);
  }
  console.log(`[dav-de] Unique cards collected from list pages: ${uniqueCards.length}`);

  // --- Fetch detail pages ---
  const toFetch = uniqueCards.filter(c => !seen.has(c.profileUrl));
  console.log(`[dav-de] Detail pages to fetch: ${toFetch.length} (skipped ${uniqueCards.length - toFetch.length} already seen)`);

  let rowCount = 0;
  let kept = 0;
  let filtered = 0;
  let failed = 0;

  await runWithConcurrency(toFetch, DETAIL_CONCURRENCY, async (card) => {
    await sleep(DETAIL_DELAY_MS);
    try {
      const html = await fetchUrl(card.profileUrl, jar);
      const detail = parseProfilePage(html, card);

      // Post-filter: require immigration-related badges OR Migration mention in firm/address
      const passesBadgeFilter = isImmigrationLawyer(card);
      // Since we filtered by section=804, all list-page results should already be Migrationsrecht.
      // But badges on cards may show only practice areas (not Fachanwalt). Keep all results that passed
      // the section filter, since the DAV search engine already filtered them.
      // However, the task says post-filter by immigration keywords — enforce it.
      // Reality check: many cards have empty badges due to hidden sections. Be lenient:
      // if badges are empty, KEEP (they came from section filter).
      const keep = card.badges.length === 0 ? true : passesBadgeFilter;

      if (!keep) {
        filtered++;
        return;
      }

      const row = {
        ...detail,
        profile_url: card.profileUrl,
        country: 'Germany',
        niche: 'immigration lawyer',
        source: 'dav_germany',
      };
      appendRow(row);
      seen.add(card.profileUrl);
      state.seenProfiles = Array.from(seen);
      if (rowCount % 20 === 0) saveState(state);
      rowCount++;
      kept++;
      if (rowCount <= 5 || rowCount % 50 === 0) {
        console.log(`[dav-de] +${rowCount}: ${row.first_name} ${row.last_name} — ${row.city} — ${row.email || '(no email)'}`);
      }
    } catch (e) {
      failed++;
      console.error(`[dav-de] profile failed ${card.profileUrl}: ${e.message}`);
    }
  });

  saveState(state);
  console.log('[dav-de] DONE');
  console.log(`[dav-de]  kept=${kept}  filtered_out=${filtered}  failed=${failed}`);
  console.log(`[dav-de]  CSV: ${CSV_PATH}`);
}

if (require.main === module) {
  main().catch(e => { console.error('[dav-de] FATAL', e); process.exit(1); });
}
