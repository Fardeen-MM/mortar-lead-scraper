#!/usr/bin/env node
/**
 * CCNE (Commission on Collegiate Nursing Education) — Accredited Nursing Programs.
 *
 * Endpoint: https://directory.ccnecommunity.org/reports/rptAccreditedPrograms_New.asp
 *   ?sort=state&sProgramType=<id>
 *
 * Program IDs (from accprog.asp <select> options):
 *   1 = Baccalaureate (~856 emails)
 *   12 = Baccalaureate (extra)
 *   2 = Masters       (~544 emails)
 *   6,7,8,10 = Masters variants
 *   3 = DNP           (~392 emails)
 *   5 = Post-Graduate APRN Cert (~343 emails)
 *   4 = Employee-Based Entry-to-Practice Residency
 *   9 = Federally Funded Traineeship
 *   11 = NP Fellowship/Residency
 *
 * Classic ASP renders a giant HTML list grouped by state with:
 *   <h3> InstitutionName </h3>
 *   School of X<BR>AddressLine1<BR>City, ST ZIP
 *   <a href=WEBSITE>Link to Website</a>
 *   Chief Nurse Administrator: Name, Credentials
 *   Title: ...
 *   E-Mail: admin@school.edu
 *   Phone: 555-555-5555
 *   <table> Program | Dates | ... </table>
 *
 * Dedup by school_name+email+program_type so one institution may yield
 * multiple rows (one per program) and we keep them all since each row
 * is distinct accreditation data.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'nursing-ccne-schools.csv');
const STATE_FILE = path.join(__dirname, '..', 'output', 'nursing-ccne-schools.state.json');
const BASE = 'https://directory.ccnecommunity.org/reports/rptAccreditedPrograms_New.asp';

const PROGRAM_TYPES = [
  { id: '1',  label: 'Baccalaureate' },
  { id: '12', label: 'Baccalaureate' },
  { id: '2',  label: 'Masters' },
  { id: '6',  label: 'Masters' },
  { id: '7',  label: 'Masters' },
  { id: '8',  label: 'Masters' },
  { id: '10', label: 'Masters' },
  { id: '3',  label: 'Doctor of Nursing Practice (DNP)' },
  { id: '5',  label: 'Post-Graduate APRN Certificate' },
  { id: '4',  label: 'Entry-to-Practice Residency' },
  { id: '9',  label: 'Federally Funded Traineeship' },
  { id: '11', label: 'NP Fellowship/Residency' },
];

// ANSI state codes used in the report (US states + DC + PR)
const US_STATES = {
  AL:'Alabama', AK:'Alaska', AZ:'Arizona', AR:'Arkansas', CA:'California',
  CO:'Colorado', CT:'Connecticut', DC:'District of Columbia', DE:'Delaware',
  FL:'Florida', GA:'Georgia', HI:'Hawaii', IA:'Iowa', ID:'Idaho', IL:'Illinois',
  IN:'Indiana', KS:'Kansas', KY:'Kentucky', LA:'Louisiana', MA:'Massachusetts',
  MD:'Maryland', ME:'Maine', MI:'Michigan', MN:'Minnesota', MO:'Missouri',
  MS:'Mississippi', MT:'Montana', NC:'North Carolina', ND:'North Dakota',
  NE:'Nebraska', NH:'New Hampshire', NJ:'New Jersey', NM:'New Mexico',
  NV:'Nevada', NY:'New York', OH:'Ohio', OK:'Oklahoma', OR:'Oregon',
  PA:'Pennsylvania', PR:'Puerto Rico', RI:'Rhode Island', SC:'South Carolina',
  SD:'South Dakota', TN:'Tennessee', TX:'Texas', UT:'Utah', VA:'Virginia',
  VT:'Vermont', WA:'Washington', WI:'Wisconsin', WV:'West Virginia', WY:'Wyoming',
};

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
      },
      timeout: 60000,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function esc(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); }
  catch { return { completedPrograms: [], seenKeys: [] }; }
}
function saveState(state) { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); }

const STREET_SUFFIXES = /\b(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl|Highway|Hwy|Parkway|Pkwy|Circle|Cir|Plaza|Square|Sq|Building|Bldg|Box|Suite|Ste|Unit|Floor|Fl|Room|Rm|Hall)\.?\b/i;

function parseLocation(addrBlock) {
  // Find all "City, ST ZIP" patterns; use the LAST one (city appears right
  // before state/zip). Walk all matches and take last.
  const re = /((?:[A-Z][A-Za-z.'\-]+(?:\s+[A-Z][A-Za-z.'\-]+)*)),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/g;
  let m, last = null;
  while ((m = re.exec(addrBlock)) !== null) {
    // Only accept if the captured "city" portion doesn't contain a street suffix
    if (!STREET_SUFFIXES.test(m[1])) { last = m; continue; }
    // Otherwise trim off the street prefix: take only the last 1-3 title-case tokens
    const cleaned = m[1].replace(/.*\b(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl|Highway|Hwy|Parkway|Pkwy|Circle|Cir|Plaza|Square|Sq|Building|Bldg|Box|Suite|Ste|Unit|Floor|Fl|Room|Rm|Hall)\.?\s+/i, '').trim();
    if (cleaned && !STREET_SUFFIXES.test(cleaned)) last = [m[0], cleaned, m[2], m[3]];
  }
  if (last) return { city: last[1].trim(), state: last[2], zip: last[3] };
  return { city: '', state: '', zip: '' };
}

function parseProgramsHtml(html, programLabel) {
  // Classic ASP output has malformed HTML (double <html>, un-nested tables).
  // Don't use cheerio top-level — work with raw HTML and regex split on state anchors.
  const rows = [];
  const parts = html.split(/<a name='([A-Z]{2})'>/i);
  // parts[0] = preamble, then [stateCode, stateHtml, stateCode, stateHtml, ...]
  for (let i = 1; i < parts.length; i += 2) {
    const stateCode = parts[i];
    const stateHtml = parts[i + 1] || '';

    // Split the state block into institution blocks by <h3> ... </h3>
    // Each institution starts with <h3> InstitutionName </h3> and ends
    // before the next <h3> of an institution (not program headers).
    // Use a broad splitter: each institution block begins with
    //   <td valign="top" style="width: 100%">\s*<h3> NAME </h3>
    const instRe = /<td[^>]*>\s*<h3>\s*([^<]{3,140})\s*<\/h3>[\s\S]*?(?=<td[^>]*>\s*<h3>\s*[^<]{3,140}\s*<\/h3>|<a name='[A-Z]{2}'>|$)/g;
    let m;
    while ((m = instRe.exec(stateHtml)) !== null) {
      const name = m[1].trim();
      // Skip program-type rows
      if (/^(Program|Initial Accreditation|Most Recent|Accreditation Term|Last On-Site|Next On-Site|Baccalaureate|Masters|Master's|Doctor|Post|Employee|Federally|NP |Entry-to)/i.test(name)) continue;
      if (name.length < 4) continue;
      const block = m[0];

      // Strip HTML for field extraction but keep <a href="..."> visible
      const textOnly = block.replace(/<script[\s\S]*?<\/script>/gi, ' ')
                            .replace(/<br\s*\/?\s*>/gi, '\n')
                            .replace(/<[^>]+>/g, ' ')
                            .replace(/&nbsp;/g, ' ')
                            .replace(/&amp;/g, '&')
                            .replace(/\s+/g, ' ')
                            .trim();

      const email = ((textOnly.match(/E-?Mail:\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})/i) || [])[1] || '').toLowerCase();
      const phone = ((textOnly.match(/Phone:\s*([0-9().\-\s+]{7,25})/i) || [])[1] || '').trim();
      const fax = ((textOnly.match(/Fax:\s*([0-9().\-\s+]{7,25})/i) || [])[1] || '').trim();
      const admin = ((textOnly.match(/Chief Nurse Administrator:\s*([^]{3,160}?)\s+Title:/i) || [])[1] || '').replace(/\s+/g, ' ').trim();
      const adminTitle = ((textOnly.match(/Title:\s*([^]{3,160}?)\s+(?:E-?Mail|Phone|Fax)/i) || [])[1] || '').replace(/\s+/g, ' ').trim();

      // Address block usually: "School of X" or direct address with City, ST ZIP
      const loc = parseLocation(textOnly);

      // School dept name (e.g., School of Nursing)
      let department = '';
      const deptMatch = textOnly.match(/(School of [A-Za-z][^]{0,80}?|College of [A-Za-z][^]{0,80}?|Department of Nursing|Department of Health[^]{0,80}?)(?=\s+\d|\s+[A-Z][a-z]+,|\s+P\.?O\.?|$)/);
      if (deptMatch) department = deptMatch[1].trim();

      // Website from <a href=...>
      let website = '';
      const wMatch = block.match(/<a\s+href=["']?(https?:\/\/[^\s"'<>]+)["']?/i);
      if (wMatch) {
        const href = wMatch[1];
        if (!/ccnecommunity|aacnnursing|aacn\.nche|mailto:/i.test(href)) website = href;
      }

      // Street/address line: look for "<dept> <address> City, ST ZIP"
      // Use first occurrence of number+street before City,ST ZIP
      let streetAddr = '';
      const addrMatch = textOnly.match(/(\d{1,6}\s+[A-Z][^\n]{5,80}?)\s+[A-Z][a-z]+,\s*[A-Z]{2}\s+\d{5}/);
      if (addrMatch) streetAddr = addrMatch[1].replace(/\s+/g, ' ').trim();
      const fullAddr = [department, streetAddr].filter(Boolean).join(', ');

      rows.push({
        school_name: name,
        email,
        phone: phone.replace(/\s+/g, ' '),
        fax: fax.replace(/\s+/g, ' '),
        website,
        address: fullAddr,
        city: loc.city,
        state: loc.state || stateCode,
        zip: loc.zip,
        admin_name: admin,
        admin_title: adminTitle,
        program_type: programLabel,
        accreditation: 'CCNE',
        country: 'United States',
        niche: 'nursing school',
        source: 'ccne_directory',
      });
    }
  }
  return rows;
}

async function main() {
  console.log('=== CCNE Nursing Schools Scraper ===');

  const cols = ['school_name','email','phone','fax','website','address','city','state','zip','admin_name','admin_title','program_type','accreditation','country','niche','source'];
  const state = loadState();
  const seen = new Set(state.seenKeys);
  let total = 0, withEmail = 0;

  // Write header only on fresh run
  if (!fs.existsSync(OUTPUT) || state.completedPrograms.length === 0) {
    fs.writeFileSync(OUTPUT, cols.join(',') + '\n');
  }

  for (const pt of PROGRAM_TYPES) {
    if (state.completedPrograms.includes(pt.id)) {
      console.log(`  SKIP program ${pt.id} (${pt.label}) — already done`);
      continue;
    }
    const url = `${BASE}?sort=state&sProgramType=${pt.id}`;
    console.log(`\n> Program ${pt.id} (${pt.label}): ${url}`);
    let html;
    try {
      html = await httpGet(url);
    } catch (err) {
      console.log(`  ERROR: ${err.message}`);
      continue;
    }
    if (!html || html.length < 500) {
      console.log('  skipped — empty response');
      continue;
    }
    let rows;
    try { rows = parseProgramsHtml(html, pt.label); }
    catch (err) { console.log(`  parse error: ${err.message}`); continue; }

    let newInBatch = 0;
    for (const r of rows) {
      const key = [r.school_name, r.email || r.city, r.program_type].join('|').toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      fs.appendFileSync(OUTPUT, cols.map(c => esc(r[c])).join(',') + '\n');
      if (r.email) withEmail++;
      total++;
      newInBatch++;
    }
    console.log(`  -> ${rows.length} parsed, ${newInBatch} new. Total: ${total}, emails: ${withEmail}`);

    state.completedPrograms.push(pt.id);
    state.seenKeys = Array.from(seen);
    saveState(state);
    await new Promise(r => setTimeout(r, 1500));
  }

  console.log(`\n=== CCNE DONE — ${total} rows, ${withEmail} with email ===`);
  console.log(`Output: ${OUTPUT}`);
}

if (require.main === module) {
  main().catch(err => { console.error('FATAL:', err); process.exit(1); });
}
