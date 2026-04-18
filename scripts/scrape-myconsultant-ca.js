#!/usr/bin/env node
/**
 * MyConsultant.ca (CAPIC/CICC directory) — 3,714 RCIC immigration consultants.
 * Step 1: POST /Partial/ConsultantsList → single 4.9MB HTML blob with all consultants.
 * Step 2: Per-consultant detail page fetch → mailto/tel/website extraction.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'myconsultant-ca-rcic.csv');
const STATE_FILE = OUTPUT.replace(/\.csv$/, '.state.json');
const CONCURRENCY = 10;
const BASE = 'https://www.myconsultant.ca';

function httpGet(url, cookieJar) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Cookie': cookieJar.join('; '),
        'Referer': BASE + '/EN/Consultants',
      },
      timeout: 20000,
    }, res => {
      // Capture set-cookie
      const setCookies = res.headers['set-cookie'] || [];
      for (const sc of setCookies) {
        const kv = sc.split(';')[0];
        const name = kv.split('=')[0];
        cookieJar = cookieJar.filter(c => !c.startsWith(name + '='));
        cookieJar.push(kv);
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ body: Buffer.concat(chunks).toString('utf-8'), jar: cookieJar }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function httpPost(url, bodyStr, cookieJar, referer) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      method: 'POST',
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Content-Length': Buffer.byteLength(bodyStr),
        'Cookie': cookieJar.join('; '),
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': referer,
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parseConsultantList(html) {
  const $ = cheerio.load(html);
  const consultants = [];
  $('.consultants-item').each((_, el) => {
    const $el = $(el);
    const name = $el.find('.consultant-name').text().trim();
    const link = $el.find('.consultant-name').attr('href') || '';
    const cdnMatch = link.match(/cdn=(\d+)/);
    const cdn = cdnMatch ? cdnMatch[1] : '';
    const details = $el.find('.consultant-details').text();
    const cicc = (details.match(/CICC ID:\s*([A-Z0-9]+)/) || [])[1] || '';
    const location = (details.match(/Location:\s*([^\n]+?)(?=\s*Specialty|$)/) || [])[1]?.trim() || '';
    const specialty = (details.match(/Specialty:\s*([^\n]+?)(?=\s*Language|$)/) || [])[1]?.trim() || '';
    const languages = (details.match(/Language\(s\):\s*(.+)$/s) || [])[1]?.trim().replace(/\s+/g, ' ') || '';
    if (!name || !cdn) return;
    const parts = name.split(/\s+/);
    consultants.push({
      first_name: parts[0] || '',
      last_name: parts.slice(1).join(' '),
      cicc_id: cicc,
      cdn,
      location,
      specialty,
      languages,
      profile_url: BASE + link.replace(/&amp;/g, '&'),
    });
  });
  return consultants;
}

function extractFromDetail(html) {
  const $ = cheerio.load(html);
  const text = html;

  // Email: mailto: link
  const emailMatches = text.match(/mailto:([^"'\s?]+)/g) || [];
  const email = emailMatches.length ? emailMatches[0].replace('mailto:', '').split('?')[0].toLowerCase() : '';

  // Phone: tel: link
  const phoneMatches = text.match(/tel:([^"'\s]+)/g) || [];
  const phone = phoneMatches.length ? phoneMatches[0].replace('tel:', '').replace(/[^\d+]/g, '') : '';

  // Website: external href (not social, not gov)
  let website = '';
  $('a[href^="http"]').each((_, a) => {
    if (website) return;
    const h = $(a).attr('href') || '';
    if (/facebook|twitter|linkedin|instagram|youtube|myconsultant\.ca|college-ic\.ca|capic\.ca|canada\.ca|cbsa-asfc|irb-cisr|buzzsprout|fonts\.googleapis/i.test(h)) return;
    if (/capicconnect|cpd-pprd|gocsi|college/i.test(h)) return;
    website = h;
  });

  // Firm name
  const firmName = $('.firm-name, h2, .consultants-firm-name').first().text().trim();

  // Business/address
  const address = $('.consultant-address, .address').first().text().trim();

  return { email, phone, website, firm_name: firmName, address };
}

async function fetchDetailBatch(consultants, jar, startIdx, endIdx, done, cols) {
  const chunk = consultants.slice(startIdx, endIdx);
  const results = await Promise.all(chunk.map(async c => {
    if (done.has(c.cdn)) return null;
    try {
      const { body } = await httpGet(c.profile_url, [...jar]);
      const ext = extractFromDetail(body);
      return { ...c, ...ext };
    } catch (err) {
      return { ...c, _err: err.message };
    }
  }));
  return results.filter(Boolean);
}

async function main() {
  console.log('=== MyConsultant.ca RCIC Scraper ===');

  // Step 1: Load page, extract token
  console.log('[1/2] Fetching listing page for CSRF token...');
  let jar = [];
  const pageRes = await httpGet(BASE + '/EN/Consultants', jar);
  jar = pageRes.jar;
  const tokenMatch = pageRes.body.match(/__RequestVerificationToken[^>]*value="([^"]+)"/);
  if (!tokenMatch) throw new Error('Could not find CSRF token');
  const token = tokenMatch[1];
  console.log(`  Token: ${token.slice(0, 40)}... (jar has ${jar.length} cookies)`);

  // Step 2: POST to get all consultants
  console.log('[2/2] POSTing to /Partial/ConsultantsList...');
  const params = new URLSearchParams({
    language: 'English',
    keyword: '',
    location: '',
    spokenlang: '',
    service: '',
    __RequestVerificationToken: token,
  });
  const listHtml = await httpPost(BASE + '/Partial/ConsultantsList', params.toString(), jar, BASE + '/EN/Consultants');
  console.log(`  Got ${(listHtml.length / 1024).toFixed(0)}KB response`);

  const consultants = parseConsultantList(listHtml);
  console.log(`  Parsed ${consultants.length} consultants`);

  // Resume state
  let state = { done: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const done = new Set(state.done);

  const cols = ['first_name', 'last_name', 'cicc_id', 'email', 'phone', 'website',
                'firm_name', 'location', 'specialty', 'languages', 'address', 'profile_url',
                'country', 'niche', 'source'];
  if (!fs.existsSync(OUTPUT)) fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  let processed = 0, emails = 0, phones = 0, websites = 0;
  const start = Date.now();

  for (let i = 0; i < consultants.length; i += CONCURRENCY) {
    const batch = consultants.slice(i, i + CONCURRENCY).filter(c => !done.has(c.cdn));
    if (!batch.length) { processed += CONCURRENCY; continue; }

    const results = await Promise.all(batch.map(async c => {
      try {
        const { body } = await httpGet(c.profile_url, [...jar]);
        const ext = extractFromDetail(body);
        return { ...c, ...ext };
      } catch (err) {
        return { ...c, _err: err.message };
      }
    }));

    for (const r of results) {
      const outRow = {
        first_name: r.first_name,
        last_name: r.last_name,
        cicc_id: r.cicc_id,
        email: r.email || '',
        phone: r.phone || '',
        website: r.website || '',
        firm_name: r.firm_name || '',
        location: r.location,
        specialty: r.specialty,
        languages: r.languages,
        address: r.address || '',
        profile_url: r.profile_url,
        country: 'Canada',
        niche: 'immigration consultant',
        source: 'myconsultant_ca',
      };
      fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
      if (r.email) emails++;
      if (r.phone) phones++;
      if (r.website) websites++;
      done.add(r.cdn);
    }
    processed += batch.length;

    // Save state every batch
    state.done = [...done];
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));

    const elapsed = (Date.now() - start) / 1000;
    const rate = (processed / elapsed).toFixed(1);
    const eta = Math.round((consultants.length - i - batch.length) / parseFloat(rate));
    if ((i / CONCURRENCY) % 10 === 0 || i + CONCURRENCY >= consultants.length) {
      console.log(`  [${i + batch.length}/${consultants.length}] emails=${emails} phones=${phones} websites=${websites} ${rate}/s ETA ${eta}s`);
    }
  }

  console.log(`\n=== DONE — ${emails} emails, ${phones} phones, ${websites} websites from ${consultants.length} RCICs ===`);
  console.log(`Output: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
