#!/usr/bin/env node
/**
 * AACN (American Association of Colleges of Nursing) — Member School Directory.
 *
 * The public member list is served via a CVWeb iframe at:
 *   https://www.aacnnursing.org/cvweb/cgi-bin/utilities.dll/openpage?wrp=orgsearchNM.htm
 * which POSTs to:
 *   https://www.aacnnursing.org/cvweb/cgi-bin/organizationdll.dll/List
 *
 * The list endpoint returns HTML with rows:
 *   <a href="http://www.school.edu/nursing" id="aXXXXX">School Name</a>
 *   <td data-label="Location">City, ST</td>
 *   <td data-label="Categories"><ul>Bachelor's<BR>Master's<BR>DNP</ul></td>
 *   <img id="CCNEXXXXX" ...>      (present if CCNE accredited)
 *   <img id="NURSINGCASXXXXX" ...> (present if NursingCAS)
 *
 * The list itself does NOT expose email or dean contact — this is a thin
 * roster. It supplements CCNE by capturing schools that are AACN members
 * but not yet CCNE accredited (or independently want AACN outreach).
 *
 * Single POST with RANGE=1/2000 returns all 955 members in one shot.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'nursing-aacn-schools.csv');
const LIST_URL = 'https://www.aacnnursing.org/cvweb/cgi-bin/organizationdll.dll/List';
const REFERER = 'https://www.aacnnursing.org/cvweb/cgi-bin/utilities.dll/openpage?wrp=orgsearchNM.htm';

function httpPost(url, formData) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const body = Object.entries(formData)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&');
    const req = https.request({
      method: 'POST',
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
        'Referer': REFERER,
      },
      timeout: 60000,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body);
    req.end();
  });
}

function esc(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parseList(html) {
  const rows = [];
  // Match each <tr>...</tr> block that has a data-label="School"
  // The structure spans multiple <tr>s per school. Simpler: find each
  // <th data-label="School" scope="row"> ... </th> and its following cells.
  //
  // Each school row chunk:
  //   <tr>
  //     <th data-label="School" scope="row">
  //       <a href="URL" ... id="aNNNN" ...>NAME</a>
  //       <span class="hide" id="sNNNN">NAME</span>
  //       ...
  //     </th>
  //     <td data-label="Location">CITY, ST</td>
  //     <td data-label="Categories"><ul class="noFormat catList">CATS</ul></td>
  //     <td data-label="Affiliations">
  //       <img alt="CCNE" id="CCNENNN" ...>
  //       <img alt="CAS" id="NURSINGCASNNN" ...>
  //       <img alt="TV" id="AACNTVNNN" ...>
  //     </td>
  //   </tr>

  // Split on <tr> (pairs) then keep chunks that contain data-label="School"
  const blocks = html.split(/<tr>/i);
  for (const block of blocks) {
    if (!block.includes('data-label="School"')) continue;
    const nameMatch = block.match(/<span[^>]+id="s(\d+)"[^>]*>([^<]{2,200})<\/span>/i) ||
                      block.match(/<a[^>]*id="a\d+"[^>]*>([^<]{2,200})<\/a>/i);
    const idMatch = block.match(/id="a(\d+)"/i);
    if (!nameMatch) continue;
    const id = idMatch ? idMatch[1] : '';
    let name = (nameMatch[nameMatch.length - 1] || '').trim();
    // Decode common HTML entities
    name = name.replace(/&#39;/g, "'").replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#x27;/gi, "'");

    const websiteMatch = block.match(/<a\s+href="(https?:\/\/[^"]+)"[^>]+id="a\d+"/i);
    const website = websiteMatch ? websiteMatch[1].trim() : '';

    const locMatch = block.match(/data-label="Location">([^<]{2,100})</i);
    let city = '', state = '';
    if (locMatch) {
      const loc = locMatch[1].trim();
      const parts = loc.split(',').map(s => s.trim());
      city = parts[0] || '';
      state = (parts[1] || '').slice(0, 2).toUpperCase();
    }

    const catMatch = block.match(/data-label="Categories"[^>]*><ul[^>]*>([\s\S]*?)<\/ul>/i);
    let categories = '';
    if (catMatch) {
      categories = catMatch[1]
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .replace(/\s*\|\s*/g, ' | ')
        .trim();
    }

    // Affiliations flags come from a JS line: var data_NNN = $.trim('FLAGS');
    // where FLAGS is a concatenation of CCNE, NURSINGCAS, AACNTV.
    // We need to search the WHOLE html for the data_ID variable since it
    // may appear after </tr>.
    let affilFlags = '';
    if (id) {
      const dataMatch = html.match(new RegExp(`var data_${id}\\s*=\\s*\\$\\.trim\\('([^']*)'\\)`, 'i'));
      if (dataMatch) affilFlags = dataMatch[1];
    }
    const ccne = /CCNE/i.test(affilFlags) ? 'Y' : '';
    const cas = /NURSINGCAS/i.test(affilFlags) ? 'Y' : '';

    // Program type label — derive from category string
    const programType = categories || '';

    const accreditation = ccne ? 'AACN, CCNE' : 'AACN';

    rows.push({
      school_name: name,
      school_id: id,
      email: '',               // Not exposed in list view
      phone: '',               // Not exposed in list view
      website,
      city,
      state,
      program_type: programType,
      accreditation,
      nursing_cas: cas,
      country: 'United States',
      niche: 'nursing school',
      source: 'aacn_member_directory',
    });
  }
  return rows;
}

async function main() {
  console.log('=== AACN Nursing Schools Scraper ===');
  const cols = ['school_name','school_id','email','phone','website','city','state','program_type','accreditation','nursing_cas','country','niche','source'];

  const formData = {
    ORGNAME: '',
    RANGE: '1/2000',            // Pull all in one shot (total ~955)
    NOWEBFLG: '<>Y',
    SHOWSQL: 'N',
    SORT: 'ORGNAME',
    ORGTYPE: '|IN|INSTITUTE,CBRANCH,PRVISIONAL',
    ISMEMBERFLG: 'Y',
    WHP: 'schools.htm',
    WBP: 'schoollist.htm',
    WNR: 'schoollist_norecords.htm',
    UDEF2TXT: '',
    ORGNAME_field: '',
    STATECD: '',
    AFFIL: '',
    UDEF3TXT: '',
  };

  console.log('POST', LIST_URL);
  let html;
  try { html = await httpPost(LIST_URL, formData); }
  catch (err) { console.error('ERROR:', err.message); process.exit(1); }

  console.log(`  response size: ${html.length} bytes`);
  const matchMatch = html.match(/Member Schools\s*-\s*(\d+)\s*Match/i);
  if (matchMatch) console.log(`  reported matches: ${matchMatch[1]}`);

  const rows = parseList(html);
  console.log(`  parsed rows: ${rows.length}`);

  fs.writeFileSync(OUTPUT, cols.join(',') + '\n');
  const seen = new Set();
  let total = 0, withWebsite = 0, ccneCount = 0;
  for (const r of rows) {
    const key = (r.school_id || '') + '|' + r.school_name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    fs.appendFileSync(OUTPUT, cols.map(c => esc(r[c])).join(',') + '\n');
    total++;
    if (r.website) withWebsite++;
    if (r.accreditation.includes('CCNE')) ccneCount++;
  }

  console.log(`\n=== AACN DONE — ${total} member schools ===`);
  console.log(`  with website: ${withWebsite}`);
  console.log(`  CCNE-accredited: ${ccneCount}`);
  console.log(`Output: ${OUTPUT}`);
}

if (require.main === module) {
  main().catch(err => { console.error('FATAL:', err); process.exit(1); });
}
