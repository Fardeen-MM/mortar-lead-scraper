#!/usr/bin/env node
/**
 * LSAC Law Schools Scraper
 *
 * Pulls all 197 ABA-approved JD programs from LSAC's public AJAX endpoint,
 * then visits each school profile and extracts admissions@, phone, website,
 * city/state/zip.
 *
 * Data source: https://www.lsac.org/views/ajax
 *   view_name=graduate_law_schools_listing, view_display_id=embed_jd_us
 *
 * Resumable via scripts/state/lsac-law-schools.state.json
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const OUT_DIR = path.resolve(__dirname, '..', 'output');
const STATE_DIR = path.resolve(__dirname, 'state');
const OUT_CSV = path.join(OUT_DIR, 'lsac-law-schools.csv');
const STATE_FILE = path.join(STATE_DIR, 'lsac-law-schools.state.json');

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
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...headers,
      },
    };
    const req = https.request(opts, (res) => {
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
        const loc = res.headers.location;
        res.resume();
        const abs = loc.startsWith('http') ? loc : `https://${u.hostname}${loc}`;
        return resolve(httpsRequest(abs, { method: 'GET', headers }));
      }
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
    req.setTimeout(30000, () => {
      req.destroy(new Error('Request timeout'));
    });
    if (body) req.write(body);
    req.end();
  });
}

async function fetchSchoolList() {
  const url = 'https://www.lsac.org/views/ajax?view_name=graduate_law_schools_listing&view_display_id=embed_jd_us&_wrapper_format=drupal_ajax';
  const res = await httpsRequest(url, {
    method: 'POST',
    headers: {
      'X-Requested-With': 'XMLHttpRequest',
      'Accept': 'application/json, text/javascript, */*; q=0.01',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    },
    body: 'view_name=graduate_law_schools_listing&view_display_id=embed_jd_us',
  });
  if (res.statusCode !== 200) {
    throw new Error(`LSAC AJAX returned ${res.statusCode}`);
  }
  const data = JSON.parse(res.body);
  let html = '';
  for (const cmd of data) {
    if (cmd.command === 'insert' && cmd.data && cmd.data.includes('jd-programs') || (cmd.command === 'insert' && cmd.data && cmd.data.includes('/node/'))) {
      html += cmd.data;
    }
  }
  const $ = cheerio.load(html);
  const schools = [];
  const seen = new Set();
  $('a').each((_, el) => {
    const href = $(el).attr('href') || '';
    const name = $(el).text().trim();
    if (/\/(node\/\d+|choosing-law-school\/find-law-school\/jd-programs\/[a-z0-9-]+)/.test(href)) {
      if (!name || name.length < 3) return;
      const key = href.startsWith('http') ? href : 'https://www.lsac.org' + href;
      if (seen.has(key)) return;
      seen.add(key);
      schools.push({ name, url: key });
    }
  });
  return schools;
}

function parseSchoolProfile(html, fallbackName) {
  const $ = cheerio.load(html);
  // School name from <title> or h1
  let schoolName = ($('h1').first().text() || '').trim();
  if (!schoolName) {
    const title = $('title').text().trim();
    schoolName = title.replace(/\s*\|\s*The Law School Admission Council.*$/i, '').trim();
  }
  if (!schoolName) schoolName = fallbackName;

  // Email — prefer admissions@
  const allText = $.html();
  const emails = Array.from(new Set((allText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [])))
    .filter((e) => !e.toLowerCase().includes('@email.com') && !e.toLowerCase().includes('example.com'));
  let email = emails.find((e) => /^(admission|admissions|lawadmissions|admitlaw|law\.admissions)@/i.test(e));
  if (!email) email = emails.find((e) => /admission/i.test(e));
  if (!email) email = emails[0] || '';

  // Phone
  const phoneMatch = allText.match(/(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);
  const phone = phoneMatch ? phoneMatch[1] : '';

  // Address
  const street = ($('.address-street').first().text() || '').replace(/\s+/g, ' ').trim().replace(/,$/, '');
  const cityState = ($('.address-city-state').first().text() || '').replace(/\s+/g, ' ').trim();
  const country = ($('.address-country').first().text() || '').replace(/\s+/g, ' ').trim() || 'United States';
  let city = '', state = '', zip = '';
  // cityState like: "Akron, OH 44325,"
  const csMatch = cityState.replace(/,$/, '').match(/^(.+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?/);
  if (csMatch) {
    city = csMatch[1].trim();
    state = csMatch[2].trim();
    zip = (csMatch[3] || '').trim();
  } else {
    city = cityState.split(',')[0].trim();
  }

  // Website — external edu link
  let website = '';
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (!website && /^https?:\/\/(?!www\.lsac\.org)[^\/]+\.(edu|org)/i.test(href)) {
      website = href.split('?')[0].replace(/#.*$/, '');
      return false;
    }
  });

  return {
    school_name: schoolName,
    admissions_email: email,
    phone,
    website,
    city,
    state,
    zip,
    country: country || 'US',
  };
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function writeCsvHeader() {
  const header = 'school_name,admissions_email,phone,website,city,state,zip,country,niche,source\n';
  fs.writeFileSync(OUT_CSV, header);
}

function appendRow(row) {
  const line = [
    row.school_name,
    row.admissions_email,
    row.phone,
    row.website,
    row.city,
    row.state,
    row.zip,
    row.country,
    'law school',
    'lsac',
  ].map(csvEscape).join(',') + '\n';
  fs.appendFileSync(OUT_CSV, line);
}

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {}
  }
  return { processed: {}, startedAt: new Date().toISOString() };
}

function saveState(s) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2));
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  console.log('[LSAC] Fetching school list via AJAX…');
  const schools = await fetchSchoolList();
  console.log(`[LSAC] Found ${schools.length} schools`);

  const state = loadState();
  const fresh = !Object.keys(state.processed).length;
  if (fresh) writeCsvHeader();

  let count = 0, ok = 0, withEmail = 0;
  for (const s of schools) {
    count++;
    if (state.processed[s.url]) continue;
    try {
      const res = await httpsRequest(s.url);
      if (res.statusCode !== 200) {
        console.log(`[LSAC] ${count}/${schools.length} ${s.name} — HTTP ${res.statusCode}`);
        state.processed[s.url] = { ok: false, status: res.statusCode };
        saveState(state);
        continue;
      }
      const row = parseSchoolProfile(res.body, s.name);
      appendRow(row);
      ok++;
      if (row.admissions_email) withEmail++;
      const marker = row.admissions_email ? 'E' : '-';
      console.log(`[LSAC] ${count}/${schools.length} [${marker}] ${row.school_name} | ${row.admissions_email || '(no email)'} | ${row.city}, ${row.state}`);
      state.processed[s.url] = { ok: true, email: !!row.admissions_email };
      saveState(state);
    } catch (e) {
      console.log(`[LSAC] ${count}/${schools.length} ${s.name} — ERROR ${e.message}`);
      state.processed[s.url] = { ok: false, error: e.message };
      saveState(state);
    }
    await sleep(400);
  }
  console.log(`\n[LSAC] Done. Total: ${count}, OK: ${ok}, with admissions email: ${withEmail}. CSV: ${OUT_CSV}`);
}

main().catch((e) => {
  console.error('[LSAC] Fatal:', e);
  process.exit(1);
});
