#!/usr/bin/env node
/**
 * ÖRAK Austria (Österreichischer Rechtsanwaltskammertag) — lawyer register.
 * Filter: Fremden- und Asylrecht (taetigkeitsgebiete=18). ~7,312 results claimed
 * but actually returns ALL Austrian lawyers; filter seems to be OR not AND.
 * Alternative approach: iterate all taetigkeitsgebiete codes individually.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'orak-austria-immigration.csv');
const STATE_FILE = OUTPUT.replace(/\.csv$/, '.state.json');
const BASE = 'https://www.rechtsanwaelte.at';
const FORM_URL = `${BASE}/buergerservice/servicecorner/rechtsanwalt-finden/?tx_rafinden_simplesearch%5Baction%5D=list&tx_rafinden_simplesearch%5Bcontroller%5D=LawyerSearch&cHash=10c149f8320ca3f1501cb65b0df4d67e`;
const CONCURRENCY = 8;

// Immigration task codes (multiple categories)
const IMMIGRATION_CODES = [
  '18:Fremden- und Asylrecht',
  '388:Fremdenrecht',
  '562:Ausländerhaftung, Aufenthaltsrecht',
  '385:Ausländerbeschäftigungsrecht',
  '538:Fremdenrecht und Personalentsendungen',
];

function httpRequest(url, method, body, cookies) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
      'Accept': 'text/html,application/xhtml+xml',
      'Cookie': (cookies || []).join('; '),
      'Referer': BASE + '/buergerservice/servicecorner/rechtsanwalt-finden/',
    };
    if (method === 'POST') {
      headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8';
      headers['Content-Length'] = Buffer.byteLength(body || '');
    }
    const req = https.request({
      method,
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers,
      timeout: 25000,
    }, res => {
      // Handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = res.headers.location.startsWith('http') ? res.headers.location : BASE + res.headers.location;
        return resolve(httpRequest(next, 'GET', null, cookies));
      }
      const setCookies = res.headers['set-cookie'] || [];
      const newCookies = [...(cookies || [])];
      for (const sc of setCookies) {
        const kv = sc.split(';')[0];
        const name = kv.split('=')[0];
        const existing = newCookies.findIndex(c => c.startsWith(name + '='));
        if (existing >= 0) newCookies.splice(existing, 1);
        newCookies.push(kv);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ body: Buffer.concat(chunks).toString('utf-8'), cookies: newCookies }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    if (body) req.write(body);
    req.end();
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parseResults(html) {
  const $ = cheerio.load(html);
  const results = [];
  $('.items.search_results .item.grey, .search_results .item').each((_, el) => {
    const $el = $(el);
    const last = $el.find('span.lastname').text().trim();
    const first = $el.find('span.firstname').text().trim();
    const link = $el.find('a[href*="lid"]').attr('href') || '';
    const addr = $el.find('.address, .address.center').text().trim().replace(/\s+/g, ' ');
    if (!last && !first) return;
    results.push({
      first_name: first,
      last_name: last,
      address: addr,
      profile_url: link ? (BASE + link.replace(/&amp;/g, '&')) : '',
    });
  });
  return results;
}

function parseDetail(html) {
  const $ = cheerio.load(html);
  const emails = [];
  const phones = [];
  let website = '';
  const badges = [];

  // Extract mailto + tel
  $('a[href^="mailto:"]').each((_, a) => {
    const e = ($(a).attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
    if (e && e.includes('@')) emails.push(e);
  });
  $('a[href^="tel:"]').each((_, a) => {
    const p = ($(a).attr('href') || '').replace(/^tel:/i, '').trim();
    if (p) phones.push(p);
  });
  $('a[href^="http"]').each((_, a) => {
    const h = $(a).attr('href') || '';
    if (/oerak\.at|anwaltsblatt|rechtsanwaelte\.at|justice\.gv|bmj\.gv|facebook|twitter|linkedin|youtube|google|googleapis/i.test(h)) return;
    if (!website) website = h;
  });

  // Specialty/Tätigkeitsgebiete extraction
  $('.taetigkeitsgebiete li, .specialities li').each((_, el) => {
    badges.push($(el).text().trim());
  });

  // Address
  const address = $('.address, .adresse').first().text().trim().replace(/\s+/g, ' ');

  // Firm name (if any)
  const firm = $('.kanzlei, .firm, h3').first().text().trim();

  return {
    email: emails[0] || '',
    phone: phones[0] || '',
    website,
    address,
    firm_name: firm,
    badges: badges.join('; '),
  };
}

async function runCategory(code, cookies) {
  console.log(`\n[Category: ${code}]`);
  const bodyFields = [
    `tx_rafinden_simplesearch[taetigkeitsgebiete][]=${encodeURIComponent(code)}`,
  ].join('&');

  let allResults = [];
  try {
    const res = await httpRequest(FORM_URL, 'POST', bodyFields, cookies);
    const results = parseResults(res.body);
    console.log(`  Page 1: ${results.length} results`);
    allResults = allResults.concat(results);

    // Pagination: try ?page=N
    let page = 2, emptyCount = 0;
    while (emptyCount < 2 && page <= 100) {
      const pagedUrl = FORM_URL + `&tx_rafinden_simplesearch%5Bpage%5D=${page}&tx_rafinden_simplesearch%5Blimit%5D=25`;
      try {
        const r = await httpRequest(pagedUrl, 'POST', bodyFields, res.cookies);
        const pageResults = parseResults(r.body);
        if (!pageResults.length) { emptyCount++; }
        else { emptyCount = 0; allResults = allResults.concat(pageResults); }
        console.log(`  Page ${page}: ${pageResults.length} results`);
      } catch (err) {
        console.error(`  Page ${page}: ${err.message}`);
        emptyCount++;
      }
      page++;
      await new Promise(r => setTimeout(r, 800));
    }
  } catch (err) {
    console.error(`  Error: ${err.message}`);
  }
  console.log(`  Total for ${code}: ${allResults.length}`);
  return allResults;
}

async function main() {
  console.log('=== ÖRAK Austria Immigration Scraper ===');

  // Load form to get cookies
  console.log('[1] Fetching form page for cookies...');
  const formRes = await httpRequest(BASE + '/buergerservice/servicecorner/rechtsanwalt-finden/', 'GET', null, []);
  let cookies = formRes.cookies;

  // Collect all results across immigration codes
  const allByLid = new Map();
  for (const code of IMMIGRATION_CODES) {
    const results = await runCategory(code, cookies);
    for (const r of results) {
      const lid = (r.profile_url.match(/lid%5D=(\d+)/) || [])[1] || r.profile_url;
      if (!allByLid.has(lid)) allByLid.set(lid, r);
    }
  }

  const unique = [...allByLid.values()];
  console.log(`\n[2] Unique lawyers across codes: ${unique.length}`);

  // Resume state
  let state = { done: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const done = new Set(state.done);

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'address', 'city', 'state', 'country', 'profile_url', 'niche', 'source'];
  if (!fs.existsSync(OUTPUT)) fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  let processed = 0, emails = 0, phones = 0;
  const start = Date.now();

  for (let i = 0; i < unique.length; i += CONCURRENCY) {
    const batch = unique.slice(i, i + CONCURRENCY).filter(r => !done.has(r.profile_url));
    if (!batch.length) { processed += CONCURRENCY; continue; }

    const results = await Promise.all(batch.map(async r => {
      try {
        const res = await httpRequest(r.profile_url, 'GET', null, cookies);
        const detail = parseDetail(res.body);
        return { ...r, ...detail };
      } catch (err) {
        return { ...r, _err: err.message };
      }
    }));

    for (const r of results) {
      const cityMatch = (r.address || '').match(/\b(\d{4})\s+([^,]+)/);
      const city = cityMatch ? cityMatch[2].trim() : '';
      const postal = cityMatch ? cityMatch[1] : '';
      const outRow = {
        first_name: r.first_name,
        last_name: r.last_name,
        email: r.email || '',
        phone: r.phone || '',
        firm_name: r.firm_name || '',
        website: r.website || '',
        address: r.address || '',
        city,
        state: postal,
        country: 'Austria',
        profile_url: r.profile_url,
        niche: 'immigration lawyer',
        source: 'orak_austria',
      };
      fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
      if (r.email) emails++;
      if (r.phone) phones++;
      done.add(r.profile_url);
    }
    processed += batch.length;

    state.done = [...done];
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));

    const elapsed = (Date.now() - start) / 1000;
    const rate = (processed / elapsed).toFixed(1);
    const eta = Math.round((unique.length - i - batch.length) / parseFloat(rate));
    if ((i / CONCURRENCY) % 5 === 0 || i + CONCURRENCY >= unique.length) {
      console.log(`  [${i + batch.length}/${unique.length}] emails=${emails} phones=${phones} ${rate}/s ETA ${eta}s`);
    }
  }

  console.log(`\n=== DONE — ${emails} emails, ${phones} phones from ${unique.length} Austrian immigration lawyers ===`);
  console.log(`Output: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
