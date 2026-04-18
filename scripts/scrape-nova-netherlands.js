#!/usr/bin/env node
/**
 * NOvA (Nederlandse Orde van Advocaten) — Netherlands Bar immigration lawyer scraper
 *
 * Endpoint: https://zoekeenadvocaat.advocatenorde.nl/zoeken/fetch
 *   ?q=&type=advocaten&pagina=N&limiet=10&sortering=naam
 *   &filters[rechtsgebieden]=[68]  (68 = Vreemdelingenrecht)
 *   &weergave=lijst
 *
 * Returns JSON {html, count, filters}. Server caps page size at 10.
 * List cards include: name, firm, city, profile URL.
 * Profile page adds: email, phone, website, postal code, full address.
 *
 * Usage: node scripts/scrape-nova-netherlands.js [--limit=N]
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const cheerio = require('cheerio');

const BASE = 'https://zoekeenadvocaat.advocatenorde.nl';
const RECHTSGEBIED_ID = 68; // Vreemdelingenrecht
const PAGE_SIZE = 10;       // server-enforced
const DETAIL_CONCURRENCY = 5;
const PAGE_DELAY_MS = 1000;
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const OUTPUT_DIR = path.resolve(__dirname, '..', 'output');
const OUTPUT_CSV = path.join(OUTPUT_DIR, 'nova-netherlands-immigration.csv');
const STATE_FILE = path.join(OUTPUT_DIR, '.nova-netherlands-state.json');

const args = process.argv.slice(2).reduce((acc, a) => {
  const m = a.match(/^--([^=]+)=(.*)$/);
  if (m) acc[m[1]] = m[2];
  else if (a.startsWith('--')) acc[a.slice(2)] = true;
  return acc;
}, {});
const LIMIT = args.limit ? parseInt(args.limit, 10) : null;
const MAX_PAGES = args.pages ? parseInt(args.pages, 10) : null;

// ---------- tiny HTTP client with cookie jar ----------
const cookies = new Map();

function getCookieHeader() {
  return Array.from(cookies.entries()).map(([k, v]) => `${k}=${v}`).join('; ');
}

function storeSetCookie(header) {
  const arr = Array.isArray(header) ? header : [header];
  for (const line of arr) {
    if (!line) continue;
    const [pair] = line.split(';');
    const eq = pair.indexOf('=');
    if (eq > 0) cookies.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
  }
}

function fetchUrl(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const headers = {
      'User-Agent': UA,
      'Accept': opts.xhr ? 'application/json, text/javascript, */*; q=0.01' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
      'Connection': 'keep-alive',
    };
    if (opts.xhr) {
      headers['X-Requested-With'] = 'XMLHttpRequest';
      headers['Referer'] = `${BASE}/zoeken`;
    }
    const cookieHeader = getCookieHeader();
    if (cookieHeader) headers['Cookie'] = cookieHeader;

    const req = https.request({
      method: 'GET',
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers,
      timeout: 30000,
    }, res => {
      if (res.headers['set-cookie']) storeSetCookie(res.headers['set-cookie']);
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
        res.resume();
        const loc = res.headers.location;
        const next = loc.startsWith('http') ? loc : new URL(loc, urlStr).toString();
        return resolve(fetchUrl(next, opts));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 400) {
          return reject(new Error(`HTTP ${res.statusCode} for ${urlStr}`));
        }
        resolve({ body, status: res.statusCode, headers: res.headers });
      });
    });
    req.on('timeout', () => req.destroy(new Error('request timeout')));
    req.on('error', reject);
    req.end();
  });
}

async function fetchJson(urlStr) {
  const { body } = await fetchUrl(urlStr, { xhr: true });
  try { return JSON.parse(body); }
  catch (e) { throw new Error(`JSON parse failed (${body.slice(0, 120)}...)`); }
}

async function fetchHtml(urlStr) {
  const { body } = await fetchUrl(urlStr);
  return body;
}

// ---------- parsing ----------
function parseName(fullDisplay) {
  // e.g. "de heer mr. F.A. Alkilic", "mevrouw mr. R. Achttienribbe"
  // Strip titles/salutations; split initials vs surname.
  let s = String(fullDisplay || '').replace(/\s+/g, ' ').trim();
  s = s.replace(/^(de\s+heer|mevrouw)\s+/i, '');
  s = s.replace(/\bmr\.?\s*/i, '');
  s = s.replace(/\bdrs?\.?\s*/i, '');
  s = s.replace(/\bprof\.?\s*/i, '');
  s = s.trim();
  const parts = s.split(' ').filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: '', last_name: parts[0] };
  const first = parts[0];
  const last = parts.slice(1).join(' ');
  return { first_name: first, last_name: last };
}

function parseListCards(html) {
  const $ = cheerio.load(html);
  const out = [];
  $('.result.advocaten').each((_, el) => {
    const $el = $(el);
    const $nameA = $el.find('.heading .h4 a').first();
    const displayName = $nameA.text().trim();
    const profileHref = $nameA.attr('href') || '';
    const profileUrl = profileHref.startsWith('http') ? profileHref : `${BASE}${profileHref}`;
    const firmName = $el.find('.heading .icon-office').parent().find('strong').first().text().trim()
      || $el.find('.heading a.secondary strong').first().text().trim();
    const city = $el.find('.heading .icon-places').parent().find('strong').first().text().trim();

    const { first_name, last_name } = parseName(displayName);

    out.push({
      display_name: displayName,
      first_name,
      last_name,
      firm_name: firmName,
      city: city ? titleCase(city) : '',
      profile_url: profileUrl,
    });
  });
  return out;
}

function titleCase(s) {
  return String(s).toLowerCase().replace(/(^|[\s\-'])(\p{L})/gu, (_, p, c) => p + c.toUpperCase());
}

function parseProfile(html) {
  const $ = cheerio.load(html);
  let email = '', phone = '', website = '', postal_code = '', city = '';

  // Labels are in .lawyer-info rows: "Telefoon", "E-mail", "Website", "Bezoekadres"
  $('.lawyer-info .meta-label').each((_, el) => {
    const label = $(el).text().trim().toLowerCase();
    const $container = $(el).parent();
    // value is in sibling column
    const $row = $container.closest('.row');
    if (!$row.length) return;
    const valueEls = $row.find('a, span, p');

    if (label.startsWith('telefoon')) {
      const tel = $row.find('a[href^="tel:"]').first().attr('href') || '';
      phone = tel.replace(/^tel:/, '').trim() || $row.find('a[href^="tel:"]').first().text().trim();
    } else if (label.startsWith('e-mail') || label === 'email') {
      const m = $row.find('a[href^="mailto:"]').first().attr('href') || '';
      email = m.replace(/^mailto:/, '').trim();
    } else if (label.startsWith('website')) {
      const w = $row.find('a[target="_blank"]').first().attr('href') || '';
      if (w && !/google\.com\/maps/i.test(w)) website = w;
    } else if (label.startsWith('bezoekadres') || label.startsWith('postadres')) {
      // Address lines in the <p>; typical:  Street 31\n 3581 HD UTRECHT\n Nederland
      const ptxt = $container.find('p').text().replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
      // Postal: 4 digits + 2 letters
      const pcMatch = ptxt.match(/\b(\d{4}\s?[A-Z]{2})\b/);
      if (pcMatch) postal_code = pcMatch[1].replace(/\s/g, ' ').trim();
      // City: after postal code, usually uppercase, until "Nederland" or end
      const cMatch = ptxt.match(/\d{4}\s?[A-Z]{2}\s+([A-Z][A-Z\-']+(?:\s+[A-Z][A-Z\-']+)*)/);
      if (cMatch) {
        let cty = cMatch[1].trim();
        cty = cty.replace(/\s+Nederland\b.*$/i, '').trim();
        // Drop trailing stray single-letter tokens (artifacts of split lines)
        cty = cty.replace(/\s+[A-Z]$/, '').trim();
        city = titleCase(cty);
      }
    }
  });

  // Fallback: email/phone anywhere on page
  if (!email) {
    const m = html.match(/href="mailto:([^"]+)"/);
    if (m) email = m[1];
  }
  if (!phone) {
    const m = html.match(/href="tel:([^"]+)"/);
    if (m) phone = m[1];
  }

  return { email, phone, website, postal_code, city };
}

// ---------- CSV ----------
function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

const COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
  'city', 'postal_code', 'country', 'niche', 'source', 'profile_url',
];

function writeCsvRow(stream, row) {
  stream.write(COLUMNS.map(k => csvEscape(row[k] || '')).join(',') + '\n');
}

// ---------- state / resume ----------
function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }
  catch { return { completedProfiles: [], lastPage: 0, totalCount: null }; }
}
function saveState(st) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(st, null, 2));
}

// ---------- concurrency helper ----------
async function mapConcurrent(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      try { results[idx] = await worker(items[idx], idx); }
      catch (e) { results[idx] = { _error: e.message }; }
    }
  });
  await Promise.all(runners);
  return results;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------- main ----------
async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Prime cookies via the form page
  console.log('[nova] priming cookies...');
  await fetchHtml(`${BASE}/zoeken`);

  const state = loadState();
  const completed = new Set(state.completedProfiles);

  // Open CSV (append if resuming with existing rows, else fresh header)
  const fresh = !fs.existsSync(OUTPUT_CSV) || completed.size === 0;
  const csvStream = fs.createWriteStream(OUTPUT_CSV, { flags: fresh ? 'w' : 'a' });
  if (fresh) csvStream.write(COLUMNS.join(',') + '\n');

  // 1) Walk list pages, collect card stubs
  const allCards = [];
  let page = state.lastPage + 1;
  let totalCount = state.totalCount;

  while (true) {
    const qs = new URLSearchParams({
      q: '', type: 'advocaten',
      pagina: String(page),
      limiet: String(PAGE_SIZE),
      sortering: 'naam',
      'filters[rechtsgebieden]': `[${RECHTSGEBIED_ID}]`,
      weergave: 'lijst',
    }).toString();
    const url = `${BASE}/zoeken/fetch?${qs}`;

    let json;
    try { json = await fetchJson(url); }
    catch (e) { console.error(`[nova] page ${page} fetch failed: ${e.message}`); break; }

    if (totalCount == null) { totalCount = json.count; console.log(`[nova] total count: ${totalCount}`); }
    const cards = parseListCards(json.html);
    if (cards.length === 0) { console.log(`[nova] page ${page}: 0 cards, stopping`); break; }

    allCards.push(...cards);
    console.log(`[nova] page ${page}: +${cards.length} cards (cum=${allCards.length}/${totalCount})`);

    state.lastPage = page;
    state.totalCount = totalCount;
    saveState(state);

    if (MAX_PAGES && page >= MAX_PAGES) { console.log(`[nova] reached --pages=${MAX_PAGES}, stopping list walk`); break; }
    if (LIMIT && allCards.length >= LIMIT) { console.log(`[nova] reached --limit=${LIMIT}, stopping list walk`); break; }
    if (allCards.length >= totalCount) { console.log('[nova] reached total count, done listing'); break; }

    page += 1;
    await sleep(PAGE_DELAY_MS);
  }

  const queue = allCards
    .filter(c => !completed.has(c.profile_url))
    .slice(0, LIMIT || allCards.length);

  console.log(`[nova] enriching ${queue.length} profiles (concurrency=${DETAIL_CONCURRENCY})...`);

  let done = 0, withEmail = 0, withPhone = 0;
  await mapConcurrent(queue, DETAIL_CONCURRENCY, async (card) => {
    try {
      const html = await fetchHtml(card.profile_url);
      const p = parseProfile(html);

      const row = {
        first_name: card.first_name,
        last_name: card.last_name,
        firm_name: card.firm_name,
        email: p.email,
        phone: p.phone,
        website: p.website,
        city: p.city || card.city,
        postal_code: p.postal_code,
        country: 'Netherlands',
        niche: 'immigration lawyer',
        source: 'nova_netherlands',
        profile_url: card.profile_url,
      };
      writeCsvRow(csvStream, row);
      if (p.email) withEmail++;
      if (p.phone) withPhone++;
      completed.add(card.profile_url);
      done++;
      if (done % 20 === 0) {
        state.completedProfiles = Array.from(completed);
        saveState(state);
        console.log(`[nova] enriched ${done}/${queue.length} (email=${withEmail} phone=${withPhone})`);
      }
    } catch (e) {
      console.error(`[nova] profile failed ${card.profile_url}: ${e.message}`);
    }
  });

  state.completedProfiles = Array.from(completed);
  saveState(state);
  csvStream.end();

  console.log(`[nova] done. rows=${done} email=${withEmail} phone=${withPhone}`);
  console.log(`[nova] output: ${OUTPUT_CSV}`);
}

main().catch(e => { console.error(e); process.exit(1); });
