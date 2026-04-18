#!/usr/bin/env node
/**
 * Medical School Coaching Company Tutor Scraper
 *
 * Combines three sources of medical admissions tutors:
 *   1. Elite Medical Prep  — https://elitemedicalprep.com/meet-the-tutors/
 *      Lists 50+ tutors, each with /tutors/{slug}/ profile page.
 *   2. MedSchoolCoach       — https://www.medschoolcoach.com/meet-our-team/
 *      Embedded JSON blob contains 454+ team members inline on the page.
 *   3. MedEdits              — https://mededits.com/about-mededits-medical-admissions-consulting
 *      19 faculty cards (Professors / Assistant Professors / Editors).
 *
 * Email is rarely present on these sites (most route through contact forms).
 * We capture it when it IS exposed and leave blank otherwise.
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const OUT_DIR = path.resolve(__dirname, '..', 'output');
const STATE_DIR = path.resolve(__dirname, 'state');
const OUT_CSV = path.join(OUT_DIR, 'med-prep-tutors.csv');
const STATE_FILE = path.join(STATE_DIR, 'med-prep-tutors.state.json');

for (const d of [OUT_DIR, STATE_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

function httpsRequest(url, { method = 'GET', headers = {}, body = null, redirects = 5 } = {}) {
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
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && redirects > 0) {
        const loc = res.headers.location;
        res.resume();
        const abs = loc.startsWith('http') ? loc : `https://${u.hostname}${loc}`;
        return resolve(httpsRequest(abs, { method: 'GET', headers, redirects: redirects - 1 }));
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({
        statusCode: res.statusCode,
        headers: res.headers,
        body: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => req.destroy(new Error('timeout')));
    if (body) req.write(body);
    req.end();
  });
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function splitName(full) {
  const s = full.replace(/^\s*(Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?|Professor)\s+/i, '').replace(/,\s*(MD|DO|PhD|MPH|JD|MS|MBA|DDS|RN|NP|MHS|MSc|BS|BA|MBBS).*$/i, '').replace(/\s+(MD|DO|PhD|MPH|JD|MS|MBA|DDS|RN|NP|MHS|MSc|MBBS|-\s*MD)\s*$/i, '').replace(/\s+/g, ' ').trim();
  const parts = s.split(/\s+/);
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts.slice(1).join(' ') };
}

function extractEmails(text) {
  const matches = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [];
  return Array.from(new Set(matches.filter(e => !/example\.com|email\.com|sentry\.io|wixpress\.com|wordpress|@2x|\.png|\.jpg/i.test(e))));
}

// ---------- Elite Medical Prep ----------
async function scrapeElite(rows) {
  console.log('[elite] Fetching meet-the-tutors list…');
  const list = await httpsRequest('https://elitemedicalprep.com/meet-the-tutors/');
  if (list.statusCode !== 200) {
    console.log(`[elite] list page HTTP ${list.statusCode} — skipping`);
    return;
  }
  const $ = cheerio.load(list.body);
  const tutorUrls = new Set();
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (/^https:\/\/elitemedicalprep\.com\/tutors\/[^/?#]+\/?$/i.test(href)) {
      tutorUrls.add(href);
    }
  });
  console.log(`[elite] found ${tutorUrls.size} tutor profile links`);
  let idx = 0;
  for (const url of tutorUrls) {
    idx++;
    try {
      const r = await httpsRequest(url);
      if (r.statusCode !== 200) { console.log(`[elite] ${idx}/${tutorUrls.size} ${url} HTTP ${r.statusCode}`); continue; }
      const $$ = cheerio.load(r.body);
      let rawName = ($$('h1.tutors-title').first().text() || '').trim();
      // Strip greeting prefix like "Hi! I'm Alexandria Foster, MD."
      rawName = rawName.replace(/^\s*Hi\s*!?\s*/i, '');
      rawName = rawName.replace(/^\s*I\s*['’]\s*m\s+/i, '');
      rawName = rawName.replace(/[.!?]+\s*$/, '').trim();
      const specialty = ($$('p.tutor-specialty').first().text() || '').trim();
      const bio = ($$('.tutor-bio').first().text() || '').trim().slice(0, 2000);
      const emails = extractEmails(r.body).filter(e => !/wpmudev|elitemedicalprep\.com/i.test(e) || /info@|contact@|hello@/i.test(e));
      const email = emails[0] || '';
      const { first, last } = splitName(rawName);
      let title = '';
      if (/MD/i.test(rawName)) title = 'MD';
      else if (/DO/i.test(rawName)) title = 'DO';
      else if (/PhD/i.test(rawName)) title = 'PhD';
      rows.push({
        first_name: first,
        last_name: last,
        title,
        specialty,
        company: 'Elite Medical Prep',
        website: url,
        email,
        country: 'US',
      });
      console.log(`[elite] ${idx}/${tutorUrls.size} ${first} ${last} — ${specialty || 'n/a'}${email ? ' <' + email + '>' : ''}`);
      await new Promise(r => setTimeout(r, 350));
    } catch (e) {
      console.log(`[elite] ${idx} ${url} ERROR: ${e.message}`);
    }
  }
}

// ---------- MedSchoolCoach ----------
async function scrapeMSC(rows) {
  console.log('[msc] Fetching meet-our-team page…');
  const r = await httpsRequest('https://www.medschoolcoach.com/meet-our-team/');
  if (r.statusCode !== 200) { console.log(`[msc] HTTP ${r.statusCode} — skipping`); return; }
  const html = r.body;
  // Extract JSON objects containing _first_name / _last_name
  // Each blob looks like: {"ID":NNN,...,"_first_name":"X","_last_name":"Y",...,"_job_title":"Z",...}
  // We walk the string balancing braces starting at each _first_name.
  const members = [];
  const needle = '"_first_name":';
  let pos = 0;
  while (true) {
    const i = html.indexOf(needle, pos);
    if (i === -1) break;
    // Back-scan to find enclosing '{'
    let depth = 0;
    let start = i;
    while (start > 0) {
      const ch = html[start];
      if (ch === '{') { depth++; if (depth === 1) break; }
      start--;
    }
    // Forward-scan to find balancing '}'
    let d = 0, end = start;
    let inStr = false, esc = false;
    for (let k = start; k < Math.min(html.length, start + 20000); k++) {
      const c = html[k];
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (c === '"') { inStr = !inStr; continue; }
      if (inStr) continue;
      if (c === '{') d++;
      else if (c === '}') { d--; if (d === 0) { end = k; break; } }
    }
    const frag = html.slice(start, end + 1);
    try {
      const obj = JSON.parse(frag);
      members.push(obj);
    } catch {}
    pos = end + 1;
  }
  console.log(`[msc] parsed ${members.length} members from embedded JSON`);
  const seen = new Set();
  for (const m of members) {
    const first = (m._first_name || '').trim();
    const last = (m._last_name || '').trim();
    const title = (m._nominal_letters || '').trim();
    const jobTitle = (m._job_title || '').replace(/<[^>]+>/g, '').trim();
    const url = (m._url || '').trim();
    const key = (first + '|' + last + '|' + jobTitle).toLowerCase();
    if (!first || seen.has(key)) continue;
    seen.add(key);
    rows.push({
      first_name: first,
      last_name: last,
      title,
      specialty: jobTitle,
      company: 'MedSchoolCoach',
      website: url || 'https://www.medschoolcoach.com/meet-our-team/',
      email: '',
      country: 'US',
    });
  }
}

// ---------- MedEdits ----------
async function scrapeMedEdits(rows) {
  console.log('[mededits] Fetching team page…');
  const r = await httpsRequest('https://mededits.com/about-mededits-medical-admissions-consulting');
  if (r.statusCode !== 200) { console.log(`[mededits] HTTP ${r.statusCode} — skipping`); return; }
  const $ = cheerio.load(r.body);
  $('.card').each((_, el) => {
    const name = ($(el).find('.mem-name').first().text() || '').trim();
    const desg = ($(el).find('.mem-desg').first().text() || '').trim();
    const desc = ($(el).find('.mem-desc').first().text() || '').trim().slice(0, 2000);
    if (!name) return;
    const { first, last } = splitName(name);
    rows.push({
      first_name: first,
      last_name: last,
      title: /Dr\.?/i.test(name) ? 'Dr' : '',
      specialty: desg,
      company: 'MedEdits',
      website: 'https://mededits.com/about-mededits-medical-admissions-consulting',
      email: '',
      country: 'US',
    });
  });
  console.log(`[mededits] captured ${rows.filter(r => r.company === 'MedEdits').length} faculty`);
}

function writeCsv(rows) {
  const header = 'first_name,last_name,title,specialty,company,website,email,country,niche,source\n';
  fs.writeFileSync(OUT_CSV, header);
  for (const r of rows) {
    const line = [
      r.first_name, r.last_name, r.title, r.specialty, r.company, r.website, r.email, r.country,
      'medical school admissions', r.company ? r.company.toLowerCase().replace(/\s+/g, '-') : 'med-prep',
    ].map(csvEscape).join(',') + '\n';
    fs.appendFileSync(OUT_CSV, line);
  }
}

async function main() {
  const rows = [];
  try { await scrapeElite(rows); } catch (e) { console.log('[elite] fatal:', e.message); }
  try { await scrapeMSC(rows); } catch (e) { console.log('[msc] fatal:', e.message); }
  try { await scrapeMedEdits(rows); } catch (e) { console.log('[mededits] fatal:', e.message); }
  writeCsv(rows);
  const byCo = rows.reduce((acc, r) => { acc[r.company] = (acc[r.company] || 0) + 1; return acc; }, {});
  fs.writeFileSync(STATE_FILE, JSON.stringify({ total: rows.length, byCompany: byCo, finishedAt: new Date().toISOString() }, null, 2));
  console.log(`\n[TUTORS] Done. Total rows: ${rows.length} — by company: ${JSON.stringify(byCo)}`);
  console.log(`[TUTORS] CSV: ${OUT_CSV}`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
