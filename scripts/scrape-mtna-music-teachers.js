#!/usr/bin/env node
/**
 * MTNA (Music Teachers National Association) — Find a Teacher directory.
 * The /cgi/page.cgi/_find-a-teacher.html?pro=advanced_search endpoint returns
 * a paged list (no pagination params; server seems to cap ~500 per state).
 * We iterate US states + Canadian provinces to collect teacher profiles.
 *
 * Fields: name, credential (NCTM), city, state, zip, teaching areas, profile URL.
 * Email/phone are hidden on the public profile page unless logged in — not scraped.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'mtna-music-teachers.csv');
const STATE_FILE = path.join(__dirname, '..', 'output', '.mtna-state.json');

const SEARCH_URL = 'https://www.mtna.org/cgi/page.cgi/_find-a-teacher.html?pro=advanced_search';

const US_STATES = ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','PR'];
const CA_PROVINCES = ['AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT'];

function httpPost(url, formData) {
  const u = new URL(url);
  const body = Object.entries(formData)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');

  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: u.hostname,
      path: u.pathname + u.search,
      method: 'POST',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
      },
      timeout: 40000,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
        resolve(Buffer.concat(chunks).toString('utf-8'));
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body);
    req.end();
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function decodeHtml(s) {
  if (!s) return '';
  return String(s).replace(/&amp;/g, '&').replace(/&#038;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ');
}

function parseResults(html, stateCode, country) {
  // Each record is <div class="MembershipMiniProfile"> ... </div></div>
  const re = /<div class="MembershipMiniProfile">([\s\S]*?)<\/div><\/div>/g;
  const out = [];
  let m;
  while ((m = re.exec(html))) {
    const block = m[1];
    const a = block.match(/<a href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/);
    if (!a) continue;
    const profilePath = a[1];
    const nameCred = decodeHtml(a[2].replace(/<[^>]+>/g, '').trim());
    // name may be "Jeanne Banta, NCTM" or just "Jane Doe"
    const credMatch = nameCred.match(/,\s*(NCTM|[A-Z]{2,5}(?:,\s*[A-Z]+)*)\s*$/);
    const credential = credMatch ? credMatch[1] : '';
    const fullName = credential ? nameCred.replace(/,\s*[A-Z]{2,5}.*$/, '').trim() : nameCred;

    // Location: "City," "ST" "zip" pattern
    const cityMatch = block.match(/<em>([^,<]+),<\/em>\s*<em>([^<]+)<\/em>\s*<em>([^<]+)<\/em>/);
    let city = '', state = stateCode, zip = '';
    if (cityMatch) {
      city = decodeHtml(cityMatch[1].trim());
      state = cityMatch[2].trim();
      zip = cityMatch[3].trim();
    } else {
      // fallback: look for city+state
      const simple = block.match(/<em>([^<]+)<\/em>\s*<em>([^<]+)<\/em>/);
      if (simple) { city = decodeHtml(simple[1].replace(/,$/, '').trim()); state = simple[2].trim(); }
    }

    // Teaching areas
    const taMatch = block.match(/Teaching Areas:<\/b>\s*([^<]+)/);
    const teachingAreas = taMatch ? decodeHtml(taMatch[1].trim()) : '';

    // Certified logo flag
    const isCertified = /mtna_cert_logo/.test(block) ? 'Yes' : '';

    // split first/last
    const parts = fullName.trim().split(/\s+/);
    const firstName = parts[0] || '';
    const lastName = parts.slice(1).join(' ') || '';

    out.push({
      name: fullName,
      first_name: firstName,
      last_name: lastName,
      credential,
      is_nctm_certified: isCertified,
      city,
      state,
      zip,
      country,
      teaching_areas: teachingAreas,
      profile_url: profilePath.startsWith('http') ? profilePath : ('https://www.mtna.org' + profilePath),
      niche: 'music teacher',
      source: 'mtna',
    });
  }
  return out;
}

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  return { done: [] };
}
function saveState(s) { fs.writeFileSync(STATE_FILE, JSON.stringify(s)); }

async function main() {
  console.log('=== MTNA Find-a-Teacher Scraper ===');

  const cols = ['name','first_name','last_name','credential','is_nctm_certified','city','state','zip','country','teaching_areas','profile_url','niche','source'];
  const state = loadState();
  const isResume = fs.existsSync(OUTPUT) && state.done.length > 0;
  const out = fs.createWriteStream(OUTPUT, { flags: isResume ? 'a' : 'w' });
  if (!isResume) out.write(cols.join(',') + '\n');

  let grandTotal = 0;
  const runs = [
    ...US_STATES.map(s => ({ state: s, country: 'United States' })),
    ...CA_PROVINCES.map(s => ({ state: s, country: 'Canada' })),
  ];

  for (const { state: stateCode, country } of runs) {
    if (state.done.includes(stateCode)) { console.log(`  skip ${stateCode} (already done)`); continue; }
    try {
      const html = await httpPost(SEARCH_URL, {
        'contact##provstate': stateCode,
        'contact##country': country,
      });
      const rows = parseResults(html, stateCode, country);
      for (const r of rows) {
        out.write(cols.map(c => escapeCsv(r[c])).join(',') + '\n');
      }
      grandTotal += rows.length;
      console.log(`  ${stateCode} (${country}): ${rows.length} teachers (total: ${grandTotal})`);
      state.done.push(stateCode);
      saveState(state);
    } catch (err) {
      console.error(`  ${stateCode} FAILED: ${err.message}`);
    }
    await new Promise(r => setTimeout(r, 1500));
  }

  out.end();
  await new Promise(r => out.on('close', r));

  console.log(`\n=== DONE — ${grandTotal} teacher records ===`);
  console.log(`Wrote: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
