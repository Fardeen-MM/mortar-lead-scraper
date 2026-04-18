#!/usr/bin/env node
/**
 * NZ Law Society — Find a Lawyer, filtered by Immigration.
 *
 * Source: https://www.lawsociety.org.nz/for-the-public/find-a-lawyer/?PracticeAreas=IM&page=N
 * Pagination: querystring `?page=N`, 10 cards per page
 * Profile URLs: /register/<slug>/?glh=1 — server-rendered detail pages with
 *   mailto/tel links and a "Workplace" dt/dd giving firm + title.
 *
 * NOTE: the brief referenced `?AreasOfPractice=Immigration` (UI-style param) and
 * `/page/N/` path segments. Live inspection shows the actual working params are
 * `?PracticeAreas=IM` (internal code) and `?page=N`. Those are what this script uses.
 *
 * Expected total: ~100 immigration lawyers across 10 pages.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT   = path.join(__dirname, '..', 'output', 'nz-lawsociety-immigration.csv');
const STATE_FILE = path.join(__dirname, '..', 'output', '.nz-lawsociety-immigration.state.json');

const MAX_PAGES     = parseInt(process.argv[2]) || 20;
const PAGE_DELAY_MS = 1800;         // 1.8s between list pages
const DETAIL_DELAY_MS = 300;        // small jitter between detail requests within a batch
const DETAIL_CONCURRENCY = 5;
const BASE = 'https://www.lawsociety.org.nz';
const LIST_URL = BASE + '/for-the-public/find-a-lawyer/';

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-NZ,en;q=0.9',
      },
      timeout: 25000,
    }, (res) => {
      // Follow simple redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const next = res.headers.location.startsWith('http') ? res.headers.location : BASE + res.headers.location;
        res.resume();
        return resolve(httpGet(next));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} on ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`timeout on ${url}`)); });
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    const obj = JSON.parse(raw);
    return {
      done: new Set(obj.done || []),
      rows: obj.rows || [],
      lastPage: obj.lastPage || 0,
    };
  } catch {
    return { done: new Set(), rows: [], lastPage: 0 };
  }
}

function saveState(state) {
  const obj = {
    done: Array.from(state.done),
    rows: state.rows,
    lastPage: state.lastPage,
    savedAt: new Date().toISOString(),
  };
  fs.writeFileSync(STATE_FILE, JSON.stringify(obj, null, 2));
}

function splitName(fullName) {
  const parts = String(fullName || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function parseListPage(html) {
  const $ = cheerio.load(html);
  const cards = [];

  $('.c-lawyer-list-glh__item').each((_, el) => {
    const $el = $(el);

    const link = $el.find('a.c-lawyer-list-glh__link').first();
    const fullName = link.text().trim() || $el.find('.c-lawyer-list-glh__name').text().trim();
    const href = link.attr('href') || '';
    const profile_url = href ? (href.startsWith('http') ? href.split('?')[0] : BASE + href.split('?')[0]) : '';

    const locationText = $el.find('.c-lawyer-list-glh__location-name').first().text().trim();
    // Location formatted as "Region, City" (e.g., "Auckland, Downtown Auckland")
    let region = '', city = '';
    if (locationText) {
      const parts = locationText.split(',').map(s => s.trim()).filter(Boolean);
      if (parts.length >= 2) { region = parts[0]; city = parts.slice(1).join(', '); }
      else { region = parts[0] || ''; city = ''; }
    }

    if (fullName && profile_url) {
      cards.push({
        fullName,
        profile_url,
        city,
        region,
      });
    }
  });

  // Detect empty / end
  const showingMatch = html.match(/Showing\s+(\d+)\s+of\s+(\d+)/i);
  const total = showingMatch ? parseInt(showingMatch[2]) : null;

  return { cards, total };
}

function parseDetailPage(html) {
  const $ = cheerio.load(html);

  const emailAnchor = $('a[href^="mailto:"]').first();
  const email = (emailAnchor.attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();

  // Phone: prefer the first tel: that is labelled "Telephone" (not Fax)
  let phone = '';
  $('dt').each((_, dt) => {
    const dtText = $(dt).text().trim().toLowerCase();
    if (!phone && /telephone/.test(dtText)) {
      const dd = $(dt).next('dd');
      const telHref = dd.find('a[href^="tel:"]').first().attr('href') || '';
      if (telHref) phone = telHref.replace(/^tel:/i, '').trim();
    }
  });
  // Fallback: first tel: on page, skipping fax
  if (!phone) {
    $('a[href^="tel:"]').each((_, a) => {
      if (phone) return;
      const label = $(a).closest('dd').prev('dt').text().toLowerCase();
      if (/fax/.test(label)) return;
      phone = ($(a).attr('href') || '').replace(/^tel:/i, '').trim();
    });
  }

  // Website: any external non-mailto/non-tel link under the lawyer profile sections
  let website = '';
  $('a[href]').each((_, a) => {
    if (website) return;
    const href = $(a).attr('href') || '';
    if (!/^https?:\/\//i.test(href)) return;
    if (/lawsociety\.org\.nz|linkedin\.com|facebook\.com|twitter\.com|instagram\.com/i.test(href)) return;
    // Only accept links that appear within the lawyer page sections (c-layout or lawyer-page)
    const $a = $(a);
    if (!$a.closest('.c-layout__three-column, .lawyer-page__practice-areas, .c-definition-list').length) return;
    website = href.split('#')[0];
  });

  // Firm name: h4.c-definition-list__title within contact section
  let firm_name = $('.c-definition-list__title').first().text().trim();

  // Or from "Workplace" dt/dd (e.g., "Partner at Forest Harrison")
  if (!firm_name) {
    $('dt').each((_, dt) => {
      const dtText = $(dt).text().trim();
      if (/^\s*Workplace\s*$/i.test(dtText)) {
        const dd = $(dt).next('dd').text().trim();
        const match = dd.match(/(?:^|\bat\s+)(.+)$/i);
        firm_name = match ? match[1].trim() : dd;
      }
    });
  }

  // Full name from H1 inside main content
  const h1 = $('main h1, .o-container h1').first().text().trim()
    || $('h1').not('.c-header__index').first().text().trim();

  return { email, phone, website, firm_name, h1Name: h1 };
}

async function fetchWithRetry(url, attempts = 3) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try { return await httpGet(url); }
    catch (err) {
      lastErr = err;
      if (err.message.includes('HTTP 404')) throw err;
      await sleep(1500 * (i + 1));
    }
  }
  throw lastErr;
}

async function processDetail(card) {
  try {
    const html = await fetchWithRetry(card.profile_url);
    const detail = parseDetailPage(html);
    const name = detail.h1Name || card.fullName;
    const { first_name, last_name } = splitName(name);
    return {
      first_name,
      last_name,
      firm_name: detail.firm_name || '',
      email: detail.email || '',
      phone: detail.phone || '',
      website: detail.website || '',
      city: card.city || '',
      region: card.region || '',
      profile_url: card.profile_url,
      country: 'NZ',
      niche: 'immigration lawyer',
      source: 'nz_law_society',
    };
  } catch (err) {
    const { first_name, last_name } = splitName(card.fullName);
    return {
      first_name,
      last_name,
      firm_name: '',
      email: '',
      phone: '',
      website: '',
      city: card.city || '',
      region: card.region || '',
      profile_url: card.profile_url,
      country: 'NZ',
      niche: 'immigration lawyer',
      source: 'nz_law_society',
      _error: err.message,
    };
  }
}

async function runConcurrent(items, concurrency, worker) {
  const results = [];
  let idx = 0;
  async function loop() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
      if (DETAIL_DELAY_MS > 0) await sleep(DETAIL_DELAY_MS);
    }
  }
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, loop);
  await Promise.all(workers);
  return results;
}

function writeCsv(rows) {
  const cols = ['first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
                'city', 'region', 'profile_url', 'country', 'niche', 'source'];
  const out = [cols.join(',')];
  for (const r of rows) out.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUTPUT, out.join('\n') + '\n');
}

function dedupKey(row) {
  const name = `${row.first_name || ''} ${row.last_name || ''}`.trim().toLowerCase();
  const firm = (row.firm_name || '').trim().toLowerCase();
  return `${name}|${firm}`;
}

async function main() {
  console.log('=== NZ Law Society — Immigration Scraper ===');
  fs.mkdirSync(path.dirname(OUTPUT), { recursive: true });

  const state = loadState();
  const allRows = [...state.rows];
  const doneProfiles = state.done;
  const seenDedup = new Set(allRows.map(dedupKey));
  let emptyPages = 0;
  let totalAnnounced = null;

  const startPage = state.lastPage > 0 ? state.lastPage + 1 : 1;
  if (startPage > 1) console.log(`Resuming from page ${startPage} (${allRows.length} rows already in state)`);

  for (let page = startPage; page <= MAX_PAGES; page++) {
    const url = `${LIST_URL}?PracticeAreas=IM&page=${page}`;
    console.log(`\n[page ${page}/${MAX_PAGES}] ${url}`);

    let html;
    try { html = await fetchWithRetry(url); }
    catch (err) {
      console.error(`  ERROR: ${err.message}`);
      if (err.message.includes('HTTP 404')) break;
      continue;
    }

    const { cards, total } = parseListPage(html);
    if (total != null) totalAnnounced = total;
    console.log(`  ${cards.length} cards on page${total != null ? ` (total: ${total})` : ''}`);

    if (cards.length === 0) {
      emptyPages++;
      if (emptyPages >= 2) {
        console.log('  2 consecutive empty pages — stopping');
        break;
      }
      await sleep(PAGE_DELAY_MS);
      continue;
    }
    emptyPages = 0;

    // Filter out already-fetched profile URLs
    const todo = cards.filter(c => !doneProfiles.has(c.profile_url));
    console.log(`  fetching ${todo.length} detail pages (concurrency=${DETAIL_CONCURRENCY})`);

    const detailRows = await runConcurrent(todo, DETAIL_CONCURRENCY, processDetail);

    let added = 0, withEmail = 0, withPhone = 0, errors = 0;
    for (const row of detailRows) {
      doneProfiles.add(row.profile_url);
      if (row._error) errors++;
      const key = dedupKey(row);
      if (seenDedup.has(key)) continue;
      seenDedup.add(key);
      delete row._error;
      allRows.push(row);
      added++;
      if (row.email) withEmail++;
      if (row.phone) withPhone++;
    }
    console.log(`  added ${added} rows, +${withEmail} email, +${withPhone} phone, ${errors} errors`);

    state.rows = allRows;
    state.lastPage = page;
    saveState(state);
    writeCsv(allRows);

    // Stop if we've seen all announced results
    if (totalAnnounced && doneProfiles.size >= totalAnnounced) {
      console.log(`  reached announced total (${totalAnnounced}) — stopping`);
      break;
    }

    await sleep(PAGE_DELAY_MS);
  }

  writeCsv(allRows);

  const withEmail = allRows.filter(r => r.email).length;
  const withPhone = allRows.filter(r => r.phone).length;
  const withWebsite = allRows.filter(r => r.website).length;
  const withFirm = allRows.filter(r => r.firm_name).length;

  console.log('\n=== RESULTS ===');
  console.log(`Rows:          ${allRows.length}`);
  console.log(`With email:    ${withEmail}`);
  console.log(`With phone:    ${withPhone}`);
  console.log(`With website:  ${withWebsite}`);
  console.log(`With firm:     ${withFirm}`);
  console.log(`Output: ${OUTPUT}`);
  console.log(`State: ${STATE_FILE}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
