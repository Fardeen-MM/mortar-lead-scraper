#!/usr/bin/env node
/**
 * APAC Immigration Agents Scraper
 *
 * Scrapes 3 sources:
 *   1. PIER Australia — Qualified Education Agent Counsellors (QEAC) via connect.pierapps.com
 *   2. Singapore MOM — Licensed Employment Agencies via employmentagency.com.sg
 *   3. Hong Kong — Licensed Employment Agencies via eaa.labour.gov.hk
 *
 * Output: output/apac-immigration-agents.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Pure HTTP (https module), NO Puppeteer.
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

// ── Config ──────────────────────────────────────────────────────────────────
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'apac-immigration-agents.csv');
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';
const DELAY_MS = 1500; // 1.5s between requests

// ── Helpers ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** URL-fetch returning {statusCode, headers, body} */
function fetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.request(url, {
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': options.accept || 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        ...(options.headers || {}),
      },
      timeout: 30000,
    }, (res) => {
      // Follow redirects (up to 3)
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const loc = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return resolve(fetch(loc, options));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({
        statusCode: res.statusCode,
        headers: res.headers,
        body: Buffer.concat(chunks).toString('utf-8'),
      }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    if (options.body) req.write(options.body);
    req.end();
  });
}

/** POST helper */
function fetchPost(url, formData, extraHeaders = {}) {
  const body = typeof formData === 'string' ? formData : querystring.stringify(formData);
  return fetch(url, {
    method: 'POST',
    body,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body).toString(),
      ...extraHeaders,
    },
  });
}

/** Escape a CSV field */
function csvEscape(val) {
  if (!val) return '';
  val = val.replace(/[\r\n]+/g, ' ').trim();
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

/** Format a lead row as CSV */
function toCsv(lead) {
  return [
    lead.email, lead.first_name, lead.last_name,
    lead.company, lead.phone, lead.city, lead.state,
    lead.country, lead.source,
  ].map(csvEscape).join(',');
}

/** Extract simple text from HTML */
function stripHtml(html) {
  if (!html) return '';
  return html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

/** Parse cookies from set-cookie headers */
function parseCookies(headers) {
  const raw = headers['set-cookie'] || [];
  return raw.map(c => c.split(';')[0]).join('; ');
}

/** Split "John Smith" into {first, last} */
function splitName(fullName) {
  if (!fullName) return { first: '', last: '' };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts.slice(1).join(' ') };
}

/** Clean phone number — keep digits, leading + */
function cleanPhone(raw) {
  if (!raw) return '';
  return raw.replace(/[^\d+\s()-]/g, '').replace(/\s+/g, ' ').trim();
}

// ── Source 1: PIER Australia ────────────────────────────────────────────────
// API: connect.pierapps.com DataTables 1.9 server-side endpoint
// Listing has name + address, detail page has phone/email/website

async function scrapePIER() {
  const leads = [];
  console.log('\n═══ PIER Australia (QEAC Education Agents) ═══');

  // Step 1: Get session cookie
  console.log('[PIER] Getting session...');
  const listPage = await fetch('https://connect.pierapps.com/widgets/qualification_types/1/agencies/list');
  const cookies = parseCookies(listPage.headers);

  // Step 2: Paginate through the agencies list, filter Australia
  const PAGE_SIZE = 100;
  let offset = 0;
  let total = null;
  const australianAgencies = []; // {id, name, address}

  console.log('[PIER] Fetching agency listings...');
  while (total === null || offset < total) {
    const params = new URLSearchParams({
      qualification_type_id: '1',
      sEcho: '1',
      iColumns: '4',
      sColumns: ',,,',
      iDisplayStart: offset.toString(),
      iDisplayLength: PAGE_SIZE.toString(),
      mDataProp_0: '0',
      mDataProp_1: '1',
      mDataProp_2: '2',
      mDataProp_3: '3',
      sSearch: '',
      bRegex: 'false',
      iSortCol_0: '0',
      sSortDir_0: 'asc',
      iSortingCols: '1',
    });

    const res = await fetch(
      'https://connect.pierapps.com/widgets/agencies/list.json?' + params.toString(),
      { headers: { Cookie: cookies, Accept: 'application/json' } }
    );

    let json;
    try { json = JSON.parse(res.body); } catch { break; }

    if (total === null) {
      total = json.iTotalDisplayRecords || 0;
      console.log(`[PIER] Total agencies globally: ${total}`);
    }

    const rows = json.aaData || [];
    if (rows.length === 0) break;

    for (const row of rows) {
      const address = stripHtml(row[2] || '');
      if (!address.toLowerCase().includes('australia')) continue;

      // Extract agency ID from View Profile link: /widgets/agencies/12345
      const idMatch = (row[3] || '').match(/\/widgets\/agencies\/(\d+)/);
      const id = idMatch ? idMatch[1] : null;

      // Extract name from anchor tag
      const nameMatch = (row[1] || '').match(/>([^<]+)</);
      const name = nameMatch ? stripHtml(nameMatch[1]) : '';

      if (id) australianAgencies.push({ id, name, address });
    }

    offset += PAGE_SIZE;
    const pct = Math.min(100, Math.round((offset / total) * 100));
    process.stdout.write(`\r[PIER] Listing progress: ${offset}/${total} (${pct}%) — AU agencies found: ${australianAgencies.length}`);

    await sleep(DELAY_MS);
  }

  console.log(`\n[PIER] Found ${australianAgencies.length} Australian agencies. Fetching detail pages...`);

  // Step 3: Fetch detail pages for each Australian agency
  let fetched = 0;
  let emailCount = 0;
  for (const agency of australianAgencies) {
    fetched++;
    if (fetched % 50 === 0) {
      console.log(`[PIER] Detail progress: ${fetched}/${australianAgencies.length} — emails: ${emailCount}`);
    }

    try {
      const detailRes = await fetch(
        `https://connect.pierapps.com/widgets/agencies/${agency.id}`,
        { headers: { Cookie: cookies } }
      );

      const html = detailRes.body;

      // Extract contact email — appears after "Contact Email:" label
      const emailMatch = html.match(/Contact Email:[\s\S]*?<\/strong>\s*([\w.+-]+@[\w.-]+\.\w+)/i)
        || html.match(/Email:[\s\S]*?<\/strong>\s*([\w.+-]+@[\w.-]+\.\w+)/i);
      let email = emailMatch ? emailMatch[1].toLowerCase().trim() : '';
      // Filter out tracker / script / asset emails
      if (email && (email.includes('noreply') || email.includes('example.com') || email.includes('.js'))) {
        email = '';
      }

      // Extract phone
      const phMatch = html.match(/Ph:\s*<\/strong>\s*([^<]+)/i)
        || html.match(/Phone:\s*<\/strong>\s*([^<]+)/i);
      const phone = phMatch ? cleanPhone(phMatch[1]) : '';

      // Parse address for city/state
      // Address format: "Street, City, STATE, PostCode, Australia"
      const addrParts = agency.address.split(',').map(s => s.trim());
      let city = '', state = '';
      if (addrParts.length >= 4) {
        // Second-to-last before "Australia" is usually state
        const auIdx = addrParts.findIndex(p => p.toLowerCase() === 'australia');
        if (auIdx >= 3) {
          state = addrParts[auIdx - 2] || '';
          city = addrParts[auIdx - 3] || '';
        } else if (addrParts.length >= 3) {
          city = addrParts[addrParts.length - 4] || addrParts[1] || '';
          state = addrParts[addrParts.length - 3] || '';
        }
      }

      if (email) emailCount++;

      leads.push({
        email,
        first_name: '',
        last_name: '',
        company: agency.name,
        phone,
        city,
        state,
        country: 'Australia',
        source: 'pier_australia',
      });
    } catch (err) {
      // Skip failed detail pages
    }

    await sleep(DELAY_MS);
  }

  console.log(`[PIER] Done. ${leads.length} agencies scraped, ${emailCount} with email.`);
  return leads;
}


// ── Source 2: Singapore MOM Licensed Employment Agencies ────────────────────
// Via employmentagency.com.sg — HTML pages with 100 entries each

async function scrapeSingaporeMOM() {
  const leads = [];
  console.log('\n═══ Singapore MOM (Licensed Employment Agencies) ═══');

  const TOTAL_PAGES = 20; // 1,976 agencies / ~100 per page

  for (let page = 1; page <= TOTAL_PAGES; page++) {
    const url = page === 1
      ? 'https://www.employmentagency.com.sg/employment-agency.html'
      : `https://www.employmentagency.com.sg/employment-agency-${page}.html`;

    console.log(`[SG] Fetching page ${page}/${TOTAL_PAGES}...`);

    try {
      const res = await fetch(url);
      if (res.statusCode !== 200) {
        console.log(`[SG] Page ${page} returned ${res.statusCode}, stopping.`);
        break;
      }

      // Parse entries from HTML
      // Structure: <h5><a href="...">COMPANY NAME (MOM Lic No: XXXXXX)</a></h5>
      //   followed by table with address and phone rows
      const html = res.body;

      // Split by agency entry containers
      const blocks = html.split('restaurant-list-item clearfix');
      for (let i = 1; i < blocks.length; i++) {
        const block = blocks[i];

        // Extract company name and license
        const nameMatch = block.match(/<h5[^>]*><a[^>]*>([^<]+(?:<[^>]*>[^<]*)*)<\/a><\/h5>/i);
        let company = '';
        let license = '';
        if (nameMatch) {
          const raw = stripHtml(nameMatch[1]);
          const licMatch = raw.match(/\(MOM Lic No:\s*([^)]+)\)/i);
          license = licMatch ? licMatch[1].trim() : '';
          company = raw.replace(/\s*\(MOM Lic No:[^)]*\)/, '').trim();
        }

        // Skip template/sample entries
        if (!company || company.toLowerCase() === 'sample agency') continue;

        // Extract address (after fa-map-marker)
        const addrMatch = block.match(/fa-map-marker[\s\S]*?<td[^>]*>([\s\S]*?)<\/td>/i);
        let address = addrMatch ? stripHtml(addrMatch[1]) : '';

        // Extract phone (after fa-phone)
        const phoneMatch = block.match(/fa-phone[\s\S]*?<td[^>]*>([\s\S]*?)<\/td>/i);
        let phone = phoneMatch ? cleanPhone(stripHtml(phoneMatch[1])) : '';
        // Take only the first phone if multiple
        if (phone.includes('/')) phone = phone.split('/')[0].trim();

        // Parse city from address — Singapore addresses end with "Singapore XXXXXX"
        let city = 'Singapore';

        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company,
          phone,
          city,
          state: '',
          country: 'Singapore',
          source: 'singapore_mom',
        });
      }

      console.log(`[SG] Page ${page} done. Running total: ${leads.length}`);
    } catch (err) {
      console.log(`[SG] Error on page ${page}: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  console.log(`[SG] Done. ${leads.length} agencies scraped.`);
  return leads;
}


// ── Source 3: Hong Kong Licensed Employment Agencies ────────────────────────
// POST to eaa.labour.gov.hk/en/result.php → list all agencies
// Then fetch each detail page at record.html?agency_id=X for phone/email

async function scrapeHongKong() {
  const leads = [];
  console.log('\n═══ Hong Kong (Licensed Employment Agencies) ═══');

  // Step 1: Get session + CSRF token from search page
  console.log('[HK] Getting session and CSRF token...');
  const searchPage = await fetch('https://www.eaa.labour.gov.hk/en/search.php');
  const hkCookies = parseCookies(searchPage.headers);
  const tokenMatch = searchPage.body.match(/name="token"\s+value="([^"]+)"/);
  if (!tokenMatch) {
    console.log('[HK] Failed to get CSRF token. Aborting.');
    return leads;
  }
  const token = tokenMatch[1];
  console.log('[HK] Session acquired.');

  // Step 2: POST to list all agencies, paginate through results
  // First page: POST with list_all_agencies=all
  // Subsequent pages: POST with page-no, filter-by etc.
  const allAgencies = []; // {agencyId, name, district, address}

  // Get first page to find total page count
  console.log('[HK] Fetching page 1...');
  let firstPageData = querystring.stringify({
    token,
    'list_all_agencies': 'all',
  });

  let pageRes = await fetchPost('https://www.eaa.labour.gov.hk/en/result.php', firstPageData, {
    Cookie: hkCookies,
    Referer: 'https://www.eaa.labour.gov.hk/en/search.php',
  });

  // Parse token for subsequent pages (may change)
  let currentToken = (pageRes.body.match(/name="token"\s+value="([^"]+)"/) || [])[1] || token;

  // Determine max page from pagination
  const pageNums = pageRes.body.match(/PageFormBtn\('(\d+)'\)/g) || [];
  const maxPage = pageNums.reduce((max, p) => {
    const n = parseInt(p.match(/\d+/)[0]);
    return n > max ? n : max;
  }, 1);
  console.log(`[HK] Total pages: ${maxPage} (at 10 per page)`);

  // Parse agencies from results page
  function parseResultPage(html) {
    const entries = [];
    const blocks = html.split('class="result"');
    for (let i = 1; i < blocks.length; i++) {
      const block = blocks[i];

      // English name (may be Chinese-only)
      const nameMatch = block.match(/class="en-name">([^<]+)/);
      const name = nameMatch ? nameMatch[1].trim() : '';

      // District
      const distMatch = block.match(/District:<\/strong>[\s\S]*?<p>([^<]+)/i);
      const district = distMatch ? distMatch[1].trim() : '';

      // Address
      const addrMatch = block.match(/Address:<\/strong>[\s\S]*?<p>([^<]+)/i);
      const address = addrMatch ? addrMatch[1].trim() : '';

      // Agency ID
      const idMatch = block.match(/agency_id=([^"&]+)/);
      const agencyId = idMatch ? idMatch[1] : '';

      if (agencyId) entries.push({ agencyId, name, district, address });
    }
    return entries;
  }

  // Parse first page
  let firstEntries = parseResultPage(pageRes.body);
  allAgencies.push(...firstEntries);
  console.log(`[HK] Page 1: ${firstEntries.length} agencies. Total: ${allAgencies.length}`);

  // Fetch remaining pages
  for (let pageNo = 2; pageNo <= maxPage; pageNo++) {
    await sleep(DELAY_MS);

    const formData = querystring.stringify({
      'en-name': '',
      'tc-name': '',
      'sc-name': '',
      'en-addr': '',
      'tc-addr': '',
      'sc-addr': '',
      'tel-no': '',
      'fax-no': '',
      'email': '',
      'types': '',
      'region': '',
      'location': '',
      'district': '',
      'search': '',
      'filter-by': '',
      'page-no': pageNo.toString(),
      token: currentToken,
    });

    try {
      pageRes = await fetchPost('https://www.eaa.labour.gov.hk/en/result.php', formData, {
        Cookie: hkCookies,
        Referer: 'https://www.eaa.labour.gov.hk/en/result.php',
      });

      // Update token
      currentToken = (pageRes.body.match(/name="token"\s+value="([^"]+)"/) || [])[1] || currentToken;

      const entries = parseResultPage(pageRes.body);
      allAgencies.push(...entries);

      if (pageNo % 10 === 0) {
        console.log(`[HK] Page ${pageNo}/${maxPage}: ${entries.length} agencies. Total: ${allAgencies.length}`);
      }
    } catch (err) {
      console.log(`[HK] Error on page ${pageNo}: ${err.message}`);
    }
  }

  console.log(`[HK] Listing complete. ${allAgencies.length} agencies found. Fetching detail pages...`);

  // Step 3: Fetch detail pages for phone/email
  let fetched = 0;
  let emailCount = 0;
  let phoneCount = 0;

  for (const agency of allAgencies) {
    fetched++;
    if (fetched % 100 === 0) {
      console.log(`[HK] Detail progress: ${fetched}/${allAgencies.length} — emails: ${emailCount}, phones: ${phoneCount}`);
    }

    try {
      const detailRes = await fetch(
        `https://www.eaa.labour.gov.hk/en/record.html?agency_id=${encodeURIComponent(agency.agencyId)}`,
        { headers: { Cookie: hkCookies, Referer: 'https://www.eaa.labour.gov.hk/en/result.php' } }
      );

      const html = detailRes.body;

      // Extract telephone
      const telMatch = html.match(/Telephone No\.:<\/strong>[\s\S]*?<p>\s*([^<]+)/i);
      const phone = telMatch ? cleanPhone(telMatch[1]) : '';

      // Extract email
      const emailMatch = html.match(/Email:<\/strong>[\s\S]*?<p>\s*([\w.+-]+@[\w.-]+\.\w+)/i);
      const email = emailMatch ? emailMatch[1].toLowerCase().trim() : '';

      // Extract English name from detail page (more reliable than listing)
      const detailNameMatch = html.match(/<h2 class="chi-name">([^<]+)/);
      let detailName = detailNameMatch ? detailNameMatch[1].trim() : agency.name;
      // If the h2 is Chinese, check for another name field
      if (/[\u4e00-\u9fff]/.test(detailName) && agency.name && !/[\u4e00-\u9fff]/.test(agency.name)) {
        detailName = agency.name; // Use the listing English name if available
      }

      if (email) emailCount++;
      if (phone) phoneCount++;

      // Parse city from district name
      const city = agency.district.replace(/\s*District$/i, '').trim() || 'Hong Kong';

      leads.push({
        email,
        first_name: '',
        last_name: '',
        company: detailName || agency.name,
        phone,
        city,
        state: '',
        country: 'Hong Kong',
        source: 'hongkong_ea',
      });
    } catch (err) {
      // Still record the agency even if detail fetch fails
      leads.push({
        email: '',
        first_name: '',
        last_name: '',
        company: agency.name,
        phone: '',
        city: agency.district.replace(/\s*District$/i, '').trim() || 'Hong Kong',
        state: '',
        country: 'Hong Kong',
        source: 'hongkong_ea',
      });
    }

    await sleep(DELAY_MS);
  }

  console.log(`[HK] Done. ${leads.length} agencies scraped, ${emailCount} with email, ${phoneCount} with phone.`);
  return leads;
}


// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔═══════════════════════════════════════════════════════╗');
  console.log('║   APAC Immigration Agents Scraper                    ║');
  console.log('║   Sources: PIER Australia | Singapore MOM | HK EA   ║');
  console.log('╚═══════════════════════════════════════════════════════╝');
  console.log(`Started: ${new Date().toISOString()}`);

  const allLeads = [];

  // Run each source sequentially
  try {
    const pierLeads = await scrapePIER();
    allLeads.push(...pierLeads);
  } catch (err) {
    console.error('[PIER] Fatal error:', err.message);
  }

  try {
    const sgLeads = await scrapeSingaporeMOM();
    allLeads.push(...sgLeads);
  } catch (err) {
    console.error('[SG] Fatal error:', err.message);
  }

  try {
    const hkLeads = await scrapeHongKong();
    allLeads.push(...hkLeads);
  } catch (err) {
    console.error('[HK] Fatal error:', err.message);
  }

  // Write CSV
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const rows = [CSV_HEADER, ...allLeads.map(toCsv)];
  fs.writeFileSync(OUTPUT_FILE, rows.join('\n'), 'utf-8');

  // Summary
  const bySource = {};
  const withEmail = {};
  const withPhone = {};
  for (const l of allLeads) {
    bySource[l.source] = (bySource[l.source] || 0) + 1;
    if (l.email) withEmail[l.source] = (withEmail[l.source] || 0) + 1;
    if (l.phone) withPhone[l.source] = (withPhone[l.source] || 0) + 1;
  }

  console.log('\n╔═══════════════════════════════════════════════════════╗');
  console.log('║   SCRAPE COMPLETE                                    ║');
  console.log('╠═══════════════════════════════════════════════════════╣');
  console.log(`║ Total leads:   ${String(allLeads.length).padStart(6)}                              ║`);
  console.log('╠───────────────────────────────────────────────────────╣');
  for (const src of Object.keys(bySource)) {
    const total = bySource[src];
    const emails = withEmail[src] || 0;
    const phones = withPhone[src] || 0;
    console.log(`║ ${src.padEnd(18)} ${String(total).padStart(5)} leads  ${String(emails).padStart(5)} emails  ${String(phones).padStart(5)} phones ║`);
  }
  console.log('╠───────────────────────────────────────────────────────╣');
  console.log(`║ Output: ${OUTPUT_FILE.split('/').slice(-2).join('/')}     ║`);
  console.log('╚═══════════════════════════════════════════════════════╝');
  console.log(`Finished: ${new Date().toISOString()}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
