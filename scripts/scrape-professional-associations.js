#!/usr/bin/env node
/**
 * Professional Association Directory Scraper
 *
 * Scrapes member/provider directories from major professional associations:
 *   1. IECA  — Independent Educational Consultants Association (1,132 consultants)
 *   2. SHRM  — Society for Human Resource Management Recertification Providers (3,530)
 *   3. NACAC Independent — College Admission Counselors (300 with EMAIL)
 *   4. NACAC Institution — College/University members (270 with phone+website)
 *   5. AMBA  — Association of MBAs Accredited Schools (300 schools with website)
 *
 * Output: output/professional-association-members.csv
 *
 * Usage:
 *   node scripts/scrape-professional-associations.js              # Scrape all
 *   node scripts/scrape-professional-associations.js --source ieca    # Single source
 *   node scripts/scrape-professional-associations.js --source shrm
 *   node scripts/scrape-professional-associations.js --source nacac-independent
 *   node scripts/scrape-professional-associations.js --source nacac-institution
 *   node scripts/scrape-professional-associations.js --source amba
 *   node scripts/scrape-professional-associations.js --test        # First 2 pages only
 */

const https = require('https');
const http = require('http');
const zlib = require('zlib');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// --- Config ---
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'professional-association-members.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'prof-assoc-progress.json');
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const DELAY_MS = 2000;

const CSV_HEADERS = [
  'first_name', 'last_name', 'name', 'company', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'address',
  'category', 'niche', 'source', 'profile_url'
];

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const sourceIdx = args.indexOf('--source');
const sourceFilter = sourceIdx >= 0 ? args[sourceIdx + 1] : null;

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function toCsvRow(obj) {
  return CSV_HEADERS.map(h => csvEscape(obj[h] || '')).join(',');
}

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const acceptEncoding = options.decompress ? 'gzip, deflate' : 'identity';
    const reqOptions = {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': acceptEncoding,
        ...(options.headers || {}),
      },
      timeout: 30000,
    };

    const req = mod.get(url, reqOptions, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return httpGet(redir, options).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks);
        let body;
        const encoding = res.headers['content-encoding'];
        try {
          if (encoding === 'gzip') body = zlib.gunzipSync(raw).toString();
          else if (encoding === 'br') body = zlib.brotliDecompressSync(raw).toString();
          else if (encoding === 'deflate') body = zlib.inflateSync(raw).toString();
          else body = raw.toString();
        } catch { body = raw.toString(); }
        resolve({ body, statusCode: res.statusCode, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function httpPost(url, body, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const urlObj = new URL(url);
    const postData = typeof body === 'string' ? body : new URLSearchParams(body).toString();

    const reqOptions = {
      hostname: urlObj.hostname,
      port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: {
        'User-Agent': USER_AGENT,
        'Content-Type': options.contentType || 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'identity',
        ...(options.headers || {}),
      },
      timeout: 30000,
    };

    const req = mod.request(reqOptions, (res) => {
      // Capture cookies
      const cookies = res.headers['set-cookie'] || [];
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ body: data, statusCode: res.statusCode, headers: res.headers, cookies }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postData);
    req.end();
  });
}

// ═══════════════════════════════════════════════════════════════
// 1. IECA SCRAPER — Independent Educational Consultants
//    2,587 entries via WordPress REST API (with EMAIL!)
//    ACF fields: name, company, phone, email, website, address
// ═══════════════════════════════════════════════════════════════
async function scrapeIECA() {
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  IECA — Independent Educational Consultants');
  console.log('  WordPress REST API — ~2,587 entries with EMAIL');
  console.log('═══════════════════════════════════════════════════\n');

  const leads = [];
  const PER_PAGE = 100;
  let page = 1;
  let totalPages = null;
  const maxPages = testMode ? 2 : 100;

  while (page <= maxPages) {
    const url = `https://www.iecaonline.com/wp-json/wp/v2/directory-entry?per_page=${PER_PAGE}&page=${page}`;
    try {
      console.log(`  [IECA] Fetching API page ${page}${totalPages ? '/' + totalPages : ''}...`);
      const { body, statusCode, headers } = await httpGet(url);

      if (statusCode !== 200) {
        console.log(`  [IECA] Got status ${statusCode}, stopping.`);
        break;
      }

      // Get total pages from headers on first request
      if (totalPages === null) {
        const total = headers['x-wp-total'];
        totalPages = parseInt(headers['x-wp-totalpages'] || '26');
        console.log(`  [IECA] Total entries: ${total}, Total pages: ${totalPages}`);
      }

      let entries;
      try {
        entries = JSON.parse(body);
      } catch (e) {
        console.log(`  [IECA] JSON parse error: ${e.message}`);
        break;
      }

      if (!Array.isArray(entries) || entries.length === 0) {
        console.log('  [IECA] No more results, stopping.');
        break;
      }

      for (const entry of entries) {
        const acf = entry.acf || {};
        const firstName = acf['member-directory-first-name-to-use'] || acf['member-directory-first-name'] || '';
        const lastName = acf['member-directory-last-name'] || '';
        const fullName = acf['member-directory-full-name'] || `${firstName} ${lastName}`.trim();
        const company = acf['member-directory-company'] || '';
        const phone = acf['member-directory-primary-phone'] || '';
        const email = acf['member-directory-primary-email'] || '';
        const website = acf['member-directory-website'] || '';
        const profileUrl = entry.link || '';

        // Extract address from first address entry
        const addresses = acf['member-directory-addresses'] || [];
        const addr = addresses[0] || {};
        const city = addr['member-directory-city'] || '';
        const state = addr['member-directory-state'] || '';
        const zip = addr['member-directory-zip'] || '';
        const country = addr['member-directory-country'] || '';

        // Extract membership type from class_list
        const classList = entry.class_list || [];
        const memberType = classList.find(c => c.startsWith('membership-type-'));
        const isProfessional = memberType === 'membership-type-professional';

        if (fullName) {
          leads.push({
            first_name: firstName,
            last_name: lastName,
            name: fullName,
            company,
            email,
            phone,
            website,
            domain: extractDomain(website),
            city,
            state,
            country: country === 'United States' ? 'US' : (country === 'Canada' ? 'CA' : country),
            address: [city, state, zip].filter(Boolean).join(', '),
            category: isProfessional ? 'Educational Consultant (Professional)' : 'Educational Consultant',
            niche: 'Education Consulting',
            source: 'IECA',
            profile_url: profileUrl,
          });
        }
      }

      console.log(`  [IECA] Page ${page}: ${entries.length} entries (total: ${leads.length})`);

      if (page >= totalPages) break;
      page++;
      await sleep(1500); // Polite delay
    } catch (err) {
      console.log(`  [IECA] Error on page ${page}: ${err.message}`);
      if (page > 3) break;
      page++;
      await sleep(DELAY_MS * 2);
    }
  }

  console.log(`  [IECA] DONE: ${leads.length} consultants scraped\n`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════
// 2. SHRM SCRAPER — Recertification Providers
//    3,530 providers with website, ASP.NET postback pagination
// ═══════════════════════════════════════════════════════════════
async function scrapeSHRM() {
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  SHRM — Recertification Provider Directory');
  console.log('  ~3,530 providers with website');
  console.log('═══════════════════════════════════════════════════\n');

  const leads = [];
  const baseUrl = 'https://portal.shrm.org/public/preferredproviderdirectory.aspx';

  try {
    // Step 1: Fetch page 1 to get ViewState + initial data
    console.log('  [SHRM] Fetching page 1 (initial)...');
    const { body: page1Body, headers: page1Headers } = await httpGet(baseUrl);
    const $1 = cheerio.load(page1Body);

    // Extract ASP.NET hidden fields
    let viewState = $1('#__VIEWSTATE').val() || '';
    let viewStateGen = $1('#__VIEWSTATEGENERATOR').val() || '';

    // Extract cookies
    const cookieHeader = page1Headers['set-cookie'];
    let cookies = '';
    if (cookieHeader) {
      cookies = (Array.isArray(cookieHeader) ? cookieHeader : [cookieHeader])
        .map(c => c.split(';')[0])
        .join('; ');
    }

    // Parse first page
    const totalMatch = page1Body.match(/(\d+)\s+items?\s+in\s+(\d+)\s+pages?/i);
    const totalItems = totalMatch ? parseInt(totalMatch[1]) : 3530;
    const totalPages = totalMatch ? parseInt(totalMatch[2]) : 353;
    console.log(`  [SHRM] Total: ${totalItems} providers across ${totalPages} pages`);

    // First, change page size to 50 to reduce requests
    console.log('  [SHRM] Setting page size to 50...');
    const pageSizeBody = new URLSearchParams({
      '__VIEWSTATE': viewState,
      '__VIEWSTATEGENERATOR': viewStateGen,
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTTARGET': 'ctl00$FormContentPlaceHolder$wizardPanel$ProviderList$ProviderList$ctl13$ctl10',
      '__EVENTARGUMENT': '',
      'ctl00$FormContentPlaceHolder$wizardPanel$ProviderList$ProviderList$ctl13$ctl10': '50',
    }).toString();

    await sleep(DELAY_MS);
    const { body: sizeBody, headers: sizeHeaders } = await httpPost(baseUrl, pageSizeBody, {
      headers: { 'Cookie': cookies, 'Referer': baseUrl },
    });

    // Update cookies
    if (sizeHeaders['set-cookie']) {
      const newCookies = (Array.isArray(sizeHeaders['set-cookie']) ? sizeHeaders['set-cookie'] : [sizeHeaders['set-cookie']])
        .map(c => c.split(';')[0]).join('; ');
      if (newCookies) cookies = newCookies;
    }

    // Update ViewState
    const $size = cheerio.load(sizeBody);
    viewState = $size('#__VIEWSTATE').val() || viewState;
    viewStateGen = $size('#__VIEWSTATEGENERATOR').val() || viewStateGen;

    // Parse function for SHRM table
    function parseSHRMPage(html) {
      const $ = cheerio.load(html);
      const rows = [];
      $('tr.RowStyle, tr.AltRowStyle').each((i, el) => {
        const tds = $(el).find('td');
        if (tds.length >= 2) {
          const nameLink = $(tds[0]).find('a');
          const name = nameLink.text().trim();
          const websiteLink = $(tds[1]).find('a');
          const website = websiteLink.text().trim();

          if (name) {
            rows.push({
              name,
              company: name,
              website: website || '',
              domain: extractDomain(website),
              category: 'HR/Training Provider',
              niche: 'HR Training & Certification',
              source: 'SHRM',
            });
          }
        }
      });
      return rows;
    }

    // Parse page 1 (after page size change)
    let pageLeads = parseSHRMPage(sizeBody);
    leads.push(...pageLeads);
    console.log(`  [SHRM] Page 1: ${pageLeads.length} providers (total: ${leads.length})`);

    // Calculate new total pages at 50/page
    const newTotalPages = Math.ceil(totalItems / 50);
    const maxPages = testMode ? 2 : newTotalPages;

    // Paginate through remaining pages
    for (let page = 2; page <= maxPages; page++) {
      await sleep(DELAY_MS);
      console.log(`  [SHRM] Fetching page ${page}/${maxPages}...`);

      try {
        const postBody = new URLSearchParams({
          '__VIEWSTATE': viewState,
          '__VIEWSTATEGENERATOR': viewStateGen,
          '__VIEWSTATEENCRYPTED': '',
          '__EVENTTARGET': 'ctl00$FormContentPlaceHolder$wizardPanel$ProviderList$ProviderList',
          '__EVENTARGUMENT': `Page$${page}`,
          'ctl00$FormContentPlaceHolder$wizardPanel$ProviderList$ProviderList$ctl13$ctl10': '50',
        }).toString();

        const { body: pageBody, headers: pageHeaders } = await httpPost(baseUrl, postBody, {
          headers: { 'Cookie': cookies, 'Referer': baseUrl },
        });

        // Update cookies
        if (pageHeaders['set-cookie']) {
          const newCookies = (Array.isArray(pageHeaders['set-cookie']) ? pageHeaders['set-cookie'] : [pageHeaders['set-cookie']])
            .map(c => c.split(';')[0]).join('; ');
          if (newCookies) cookies = newCookies;
        }

        // Update ViewState for next request
        const $p = cheerio.load(pageBody);
        viewState = $p('#__VIEWSTATE').val() || viewState;
        viewStateGen = $p('#__VIEWSTATEGENERATOR').val() || viewStateGen;

        pageLeads = parseSHRMPage(pageBody);
        leads.push(...pageLeads);
        console.log(`  [SHRM] Page ${page}: ${pageLeads.length} providers (total: ${leads.length})`);

        if (pageLeads.length === 0) {
          console.log('  [SHRM] Empty page, stopping.');
          break;
        }
      } catch (err) {
        console.log(`  [SHRM] Error on page ${page}: ${err.message}`);
        if (page > 5) break;
      }
    }
  } catch (err) {
    console.log(`  [SHRM] Fatal error: ${err.message}`);
  }

  console.log(`  [SHRM] DONE: ${leads.length} providers scraped\n`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════
// 3. NACAC INDEPENDENT — College Admission Counselors (with EMAIL!)
//    ~300 independent counselors, Salesforce Visualforce
// ═══════════════════════════════════════════════════════════════
async function scrapeNACACIndependent() {
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  NACAC — Independent Member Directory');
  console.log('  ~300 counselors with EMAIL + phone');
  console.log('═══════════════════════════════════════════════════\n');

  const leads = [];
  const baseUrl = 'https://hub.nacacnet.org/independentmemberdirectory';

  try {
    // Fetch page 1
    console.log('  [NACAC-Indep] Fetching page 1...');
    const { body, headers } = await httpGet(baseUrl);

    // Extract cookies
    let cookies = '';
    if (headers['set-cookie']) {
      cookies = (Array.isArray(headers['set-cookie']) ? headers['set-cookie'] : [headers['set-cookie']])
        .map(c => c.split(';')[0])
        .join('; ');
    }

    // Parse total pages
    const pageMatch = body.match(/Page\s+\d+\s+of\s+(\d+)/);
    const totalPages = pageMatch ? parseInt(pageMatch[1]) : 30;
    console.log(`  [NACAC-Indep] Total pages: ${totalPages}`);

    // Parse function for NACAC directory
    function parseNACACPage(html) {
      const $ = cheerio.load(html);
      const results = [];

      $('div.card').each((i, el) => {
        const card = $(el);
        const name = card.find('.card-heading span').first().text().trim();
        if (!name) return;

        // Extract fields from label/value pairs
        let email = '', phone = '', address = '', title = '';
        card.find('li').each((j, li) => {
          const label = $(li).find('label.card-detail-label').text().trim();
          const value = $(li).find('.card-detail-value').text().trim().replace(/\s+/g, ' ');
          const link = $(li).find('.card-detail-value a');

          if (label === 'Email') {
            email = link.attr('href')?.replace('mailto:', '') || value.replace(/\[email.*protected\]/, '');
          } else if (label === 'Account Phone') {
            phone = value;
          } else if (label === 'Directory Address') {
            address = value;
          } else if (label === 'Title') {
            title = value;
          }
        });

        // Parse location from address
        let city = '', state = '', country = 'US';
        if (address) {
          const parts = address.split('\n').map(s => s.trim()).filter(Boolean);
          const lastLine = parts[parts.length - 1] || '';
          // Try to parse "City, State ZIP" or "City, State ZIP Country"
          const locMatch = address.match(/([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5})?/);
          if (locMatch) {
            city = locMatch[1].trim();
            state = locMatch[2].trim();
          }
        }

        // Split name
        const nameParts = name.split(/\s+/);
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';

        results.push({
          first_name: firstName,
          last_name: lastName,
          name,
          title,
          email,
          phone,
          city,
          state,
          country,
          address,
          category: 'College Admission Counselor',
          niche: 'College Admissions Consulting',
          source: 'NACAC-Independent',
        });
      });

      return results;
    }

    // Parse page 1
    let pageLeads = parseNACACPage(body);
    leads.push(...pageLeads);
    console.log(`  [NACAC-Indep] Page 1: ${pageLeads.length} counselors (total: ${leads.length})`);

    // Extract JSF form data for Salesforce Visualforce pagination
    const $ = cheerio.load(body);
    const formId = $('form[id]').attr('id') || '';
    let vfViewState = $('input[name="com.salesforce.visualforce.ViewState"]').val() || '';
    let viewStateMac = $('input[name="com.salesforce.visualforce.ViewStateMAC"]').val() || '';
    let viewStateVer = $('input[name="com.salesforce.visualforce.ViewStateVersion"]').val() || '';
    let viewStateCsrf = $('input[name="com.salesforce.visualforce.ViewStateCSRF"]').val() || '';

    // Extract the pagination target pattern from JSF onclick handlers
    // Pattern: jsfcljs(form, 'TARGET_ID,TARGET_ID,pageNum,PAGE_NUM', '')
    // The target pattern is: j_id...:pagingToolbar:j_id402:INDEX:j_id404
    const paginationPattern = body.match(/(j_id[^,'"]+:pagingToolbar:j_id\d+):\d+:(j_id\d+)/);
    const paginationBase = paginationPattern ? paginationPattern[1] : null;
    const paginationSuffix = paginationPattern ? paginationPattern[2] : null;

    const maxPages = testMode ? 2 : totalPages;
    let currentBody = body;

    if (!paginationBase) {
      console.log('  [NACAC-Indep] Could not find pagination pattern. Only page 1 available.');
    }

    // Paginate using Salesforce Visualforce JSF form POSTs
    for (let page = 2; page <= maxPages && paginationBase; page++) {
      await sleep(DELAY_MS);
      console.log(`  [NACAC-Indep] Fetching page ${page}/${maxPages}...`);

      try {
        // Build the JSF pagination target: base:INDEX:suffix where INDEX = page-1 (0-based)
        const targetId = `${paginationBase}:${page - 1}:${paginationSuffix}`;

        // Salesforce JSF form POST: the target field is set as both key and value
        const postParams = new URLSearchParams();
        postParams.append(formId, formId);
        postParams.append('com.salesforce.visualforce.ViewState', vfViewState);
        postParams.append('com.salesforce.visualforce.ViewStateMAC', viewStateMac);
        postParams.append('com.salesforce.visualforce.ViewStateVersion', viewStateVer);
        if (viewStateCsrf) {
          postParams.append('com.salesforce.visualforce.ViewStateCSRF', viewStateCsrf);
        }
        postParams.append(targetId, targetId);
        postParams.append('pageNum', String(page));

        const { body: pageBody, headers: pageHeaders } = await httpPost(baseUrl, postParams.toString(), {
          headers: {
            'Cookie': cookies,
            'Referer': baseUrl,
            'Origin': 'https://hub.nacacnet.org',
          },
        });

        // Update cookies
        if (pageHeaders['set-cookie']) {
          const newCookies = (Array.isArray(pageHeaders['set-cookie']) ? pageHeaders['set-cookie'] : [pageHeaders['set-cookie']])
            .map(c => c.split(';')[0]).join('; ');
          if (newCookies) cookies = newCookies;
        }

        // Update ViewState for next request (Salesforce requires fresh state each time)
        const $p = cheerio.load(pageBody);
        const newVS = $p('input[name="com.salesforce.visualforce.ViewState"]').val();
        if (newVS) vfViewState = newVS;
        const newMAC = $p('input[name="com.salesforce.visualforce.ViewStateMAC"]').val();
        if (newMAC) viewStateMac = newMAC;
        const newVer = $p('input[name="com.salesforce.visualforce.ViewStateVersion"]').val();
        if (newVer) viewStateVer = newVer;
        const newCSRF = $p('input[name="com.salesforce.visualforce.ViewStateCSRF"]').val();
        if (newCSRF) viewStateCsrf = newCSRF;

        // Check if we actually got a different page
        const pageIndicator = pageBody.match(/Page\s+(\d+)\s+of\s+(\d+)/);
        if (pageIndicator && parseInt(pageIndicator[1]) !== page) {
          console.log(`  [NACAC-Indep] Expected page ${page}, got page ${pageIndicator[1]}. Pagination may require "Next" button.`);

          // Try using the "Next" button instead
          const nextBtnMatch = pageBody.match(/(j_id[^,'"]+:pagingToolbar:j_id409)/);
          if (nextBtnMatch) {
            console.log(`  [NACAC-Indep] Trying "Next" button...`);
            const nextParams = new URLSearchParams();
            nextParams.append(formId, formId);
            nextParams.append('com.salesforce.visualforce.ViewState', vfViewState);
            nextParams.append('com.salesforce.visualforce.ViewStateMAC', viewStateMac);
            nextParams.append('com.salesforce.visualforce.ViewStateVersion', viewStateVer);
            if (viewStateCsrf) nextParams.append('com.salesforce.visualforce.ViewStateCSRF', viewStateCsrf);
            nextParams.append(nextBtnMatch[1], nextBtnMatch[1]);

            const { body: nextBody, headers: nextHeaders } = await httpPost(baseUrl, nextParams.toString(), {
              headers: { 'Cookie': cookies, 'Referer': baseUrl, 'Origin': 'https://hub.nacacnet.org' },
            });

            if (nextHeaders['set-cookie']) {
              const nc = (Array.isArray(nextHeaders['set-cookie']) ? nextHeaders['set-cookie'] : [nextHeaders['set-cookie']])
                .map(c => c.split(';')[0]).join('; ');
              if (nc) cookies = nc;
            }

            const $n = cheerio.load(nextBody);
            const nvs = $n('input[name="com.salesforce.visualforce.ViewState"]').val();
            if (nvs) vfViewState = nvs;
            const nmac = $n('input[name="com.salesforce.visualforce.ViewStateMAC"]').val();
            if (nmac) viewStateMac = nmac;
            const nver = $n('input[name="com.salesforce.visualforce.ViewStateVersion"]').val();
            if (nver) viewStateVer = nver;
            const ncsrf = $n('input[name="com.salesforce.visualforce.ViewStateCSRF"]').val();
            if (ncsrf) viewStateCsrf = ncsrf;

            pageLeads = parseNACACPage(nextBody);
            currentBody = nextBody;
          } else {
            pageLeads = parseNACACPage(pageBody);
            currentBody = pageBody;
          }
        } else {
          pageLeads = parseNACACPage(pageBody);
          currentBody = pageBody;
        }

        leads.push(...pageLeads);
        console.log(`  [NACAC-Indep] Page ${page}: ${pageLeads.length} counselors (total: ${leads.length})`);

        if (pageLeads.length === 0) break;
      } catch (err) {
        console.log(`  [NACAC-Indep] Error on page ${page}: ${err.message}`);
        break;
      }
    }
  } catch (err) {
    console.log(`  [NACAC-Indep] Fatal error: ${err.message}`);
  }

  console.log(`  [NACAC-Indep] DONE: ${leads.length} counselors scraped\n`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════
// 4. NACAC INSTITUTION — College/University Members
//    ~270 institutions with phone + website
// ═══════════════════════════════════════════════════════════════
async function scrapeNACACInstitution() {
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  NACAC — Institution Member Directory');
  console.log('  ~270 institutions with phone + website');
  console.log('═══════════════════════════════════════════════════\n');

  const leads = [];
  const baseUrl = 'https://hub.nacacnet.org/institutionmemberdirectory';

  try {
    console.log('  [NACAC-Inst] Fetching page 1...');
    const { body, headers } = await httpGet(baseUrl);

    let cookies = '';
    if (headers['set-cookie']) {
      cookies = (Array.isArray(headers['set-cookie']) ? headers['set-cookie'] : [headers['set-cookie']])
        .map(c => c.split(';')[0]).join('; ');
    }

    const pageMatch = body.match(/Page\s+\d+\s+of\s+(\d+)/);
    const totalPages = pageMatch ? parseInt(pageMatch[1]) : 30;
    console.log(`  [NACAC-Inst] Total pages: ${totalPages}`);

    function parseNACACInstPage(html) {
      const $ = cheerio.load(html);
      const results = [];

      $('div.card').each((i, el) => {
        const card = $(el);
        const name = card.find('.card-heading span').first().text().trim();
        if (!name) return;

        let phone = '', website = '', address = '', category = '';
        let publicPrivate = '', yearType = '', fax = '';

        card.find('li').each((j, li) => {
          const label = $(li).find('label.card-detail-label').text().trim();
          const value = $(li).find('.card-detail-value').text().trim().replace(/\s+/g, ' ');
          const link = $(li).find('.card-detail-value a');

          if (label === 'Account Phone') phone = value;
          else if (label === 'Website') website = link.attr('href') || value;
          else if (label === 'Directory Address') address = value;
          else if (label === 'Category') category = value;
          else if (label === 'Public/Private') publicPrivate = value;
          else if (label === '2/4 Year') yearType = value;
          else if (label === 'Account Fax') fax = value;
        });

        // Parse city/state/country from address
        let city = '', state = '', country = '';
        if (address) {
          const locMatch = address.match(/([A-Za-z\s.'-]+),\s*([A-Z]{2})\s*(\d{5})?/);
          if (locMatch) {
            city = locMatch[1].trim();
            state = locMatch[2].trim();
            country = 'US';
          } else {
            // International - last word is country
            const parts = address.split(/\s+/);
            country = parts[parts.length - 1] || '';
          }
        }

        results.push({
          name,
          company: name,
          phone,
          website: website.replace(/\s+/g, ''),
          domain: extractDomain(website),
          city,
          state,
          country,
          address,
          category: category || 'Educational Institution',
          niche: 'Higher Education',
          source: 'NACAC-Institution',
        });
      });

      return results;
    }

    // Parse page 1
    let pageLeads = parseNACACInstPage(body);
    leads.push(...pageLeads);
    console.log(`  [NACAC-Inst] Page 1: ${pageLeads.length} institutions (total: ${leads.length})`);

    // NACAC Institution uses same Salesforce Visualforce pagination
    const maxPages = testMode ? 2 : totalPages;

    // Extract Visualforce state for pagination
    const $ = cheerio.load(body);
    const formId = $('form[id]').attr('id') || '';
    let vfViewState = $('input[name="com.salesforce.visualforce.ViewState"]').val() || '';
    let viewStateMac = $('input[name="com.salesforce.visualforce.ViewStateMAC"]').val() || '';
    let viewStateVer = $('input[name="com.salesforce.visualforce.ViewStateVersion"]').val() || '';
    let viewStateCsrf = $('input[name="com.salesforce.visualforce.ViewStateCSRF"]').val() || '';

    // Extract pagination target pattern
    const paginationPattern = body.match(/(j_id[^,'"]+:pagingToolbar:j_id\d+):\d+:(j_id\d+)/);
    const paginationBase = paginationPattern ? paginationPattern[1] : null;
    const paginationSuffix = paginationPattern ? paginationPattern[2] : null;

    if (!paginationBase) {
      console.log('  [NACAC-Inst] Could not find pagination pattern. Only page 1 available.');
    }

    for (let page = 2; page <= maxPages && paginationBase; page++) {
      await sleep(DELAY_MS);
      console.log(`  [NACAC-Inst] Fetching page ${page}/${maxPages}...`);

      try {
        const targetId = `${paginationBase}:${page - 1}:${paginationSuffix}`;

        const postParams = new URLSearchParams();
        postParams.append(formId, formId);
        postParams.append('com.salesforce.visualforce.ViewState', vfViewState);
        postParams.append('com.salesforce.visualforce.ViewStateMAC', viewStateMac);
        postParams.append('com.salesforce.visualforce.ViewStateVersion', viewStateVer);
        if (viewStateCsrf) postParams.append('com.salesforce.visualforce.ViewStateCSRF', viewStateCsrf);
        postParams.append(targetId, targetId);
        postParams.append('pageNum', String(page));

        const { body: pageBody, headers: pageHeaders } = await httpPost(baseUrl, postParams.toString(), {
          headers: {
            'Cookie': cookies,
            'Referer': baseUrl,
            'Origin': 'https://hub.nacacnet.org',
          },
        });

        if (pageHeaders['set-cookie']) {
          const newCookies = (Array.isArray(pageHeaders['set-cookie']) ? pageHeaders['set-cookie'] : [pageHeaders['set-cookie']])
            .map(c => c.split(';')[0]).join('; ');
          if (newCookies) cookies = newCookies;
        }

        // Update all ViewState fields
        const $p = cheerio.load(pageBody);
        const nvs = $p('input[name="com.salesforce.visualforce.ViewState"]').val();
        if (nvs) vfViewState = nvs;
        const nmac = $p('input[name="com.salesforce.visualforce.ViewStateMAC"]').val();
        if (nmac) viewStateMac = nmac;
        const nver = $p('input[name="com.salesforce.visualforce.ViewStateVersion"]').val();
        if (nver) viewStateVer = nver;
        const ncsrf = $p('input[name="com.salesforce.visualforce.ViewStateCSRF"]').val();
        if (ncsrf) viewStateCsrf = ncsrf;

        pageLeads = parseNACACInstPage(pageBody);
        leads.push(...pageLeads);
        console.log(`  [NACAC-Inst] Page ${page}: ${pageLeads.length} institutions (total: ${leads.length})`);

        if (pageLeads.length === 0) break;
      } catch (err) {
        console.log(`  [NACAC-Inst] Error on page ${page}: ${err.message}`);
        break;
      }
    }
  } catch (err) {
    console.log(`  [NACAC-Inst] Fatal error: ${err.message}`);
  }

  console.log(`  [NACAC-Inst] DONE: ${leads.length} institutions scraped\n`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════
// 5. AMBA — Association of MBAs Accredited Schools
//    ~300 schools, server-rendered HTML + profile page scraping
// ═══════════════════════════════════════════════════════════════
async function scrapeAMBA() {
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  AMBA — Accredited Business Schools');
  console.log('  ~300 schools with website (from profile pages)');
  console.log('═══════════════════════════════════════════════════\n');

  const leads = [];

  try {
    // Step 1: Fetch the main list page to get all school links
    console.log('  [AMBA] Fetching accredited schools list...');
    const { body } = await httpGet('https://www.associationofmbas.com/business-schools/accreditation/accredited-schools/', {
      decompress: true,
    });

    // Extract school links and country groupings
    // Links are in format: https://www.amba-bga.com/schools/school-slug
    const schoolRegex = /href="(https:\/\/www\.amba-bga\.com\/schools\/[^"]+)"[^>]*>([^<]+)<\/a>/gi;
    const schools = [];
    let match;
    while ((match = schoolRegex.exec(body)) !== null) {
      const url = match[1];
      const name = match[2].trim();
      if (name && !schools.find(s => s.url === url)) {
        schools.push({ url, name });
      }
    }

    console.log(`  [AMBA] Found ${schools.length} school links`);

    // Also try to extract country context from the HTML structure
    // Schools are grouped under country headings

    // Step 2: Fetch individual school profiles for website
    const maxSchools = testMode ? 5 : schools.length;
    let fetched = 0;

    for (let i = 0; i < maxSchools; i++) {
      const school = schools[i];
      fetched++;

      try {
        if (i > 0 && i % 10 === 0) {
          console.log(`  [AMBA] Progress: ${fetched}/${maxSchools} schools...`);
        }

        // Fetch profile page
        await sleep(1500); // Polite delay
        const { body: profileBody, statusCode } = await httpGet(school.url);

        if (statusCode !== 200) {
          leads.push({
            name: school.name,
            company: school.name,
            category: 'Business School',
            niche: 'MBA/Business Education',
            source: 'AMBA',
            profile_url: school.url,
          });
          continue;
        }

        // Extract website and location from profile
        // AMBA profiles use React, but often have SSR data or meta tags
        let website = '', country = '', city = '';

        // Look for website link in profile
        const websiteMatch = profileBody.match(/"website"\s*:\s*"(https?:\/\/[^"]+)"/i)
          || profileBody.match(/href="(https?:\/\/[^"]*)"[^>]*>[^<]*(?:website|visit|school site)/i)
          || profileBody.match(/"url"\s*:\s*"(https?:\/\/[^"]+\.edu[^"]*)/i);
        if (websiteMatch) website = websiteMatch[1];

        // Look for location
        const locationMatch = profileBody.match(/"location"\s*:\s*"([^"]+)"/i)
          || profileBody.match(/"city"\s*:\s*"([^"]+)"/i);
        const countryMatch = profileBody.match(/"country"\s*:\s*"([^"]+)"/i)
          || profileBody.match(/,"country":"([^"]+)"/i);

        if (locationMatch) city = locationMatch[1];
        if (countryMatch) country = countryMatch[1];

        leads.push({
          name: school.name,
          company: school.name,
          website,
          domain: extractDomain(website),
          city,
          country,
          category: 'Business School',
          niche: 'MBA/Business Education',
          source: 'AMBA',
          profile_url: school.url,
        });
      } catch (err) {
        // Still record the school even if profile fetch fails
        leads.push({
          name: school.name,
          company: school.name,
          category: 'Business School',
          niche: 'MBA/Business Education',
          source: 'AMBA',
          profile_url: school.url,
        });
      }
    }
  } catch (err) {
    console.log(`  [AMBA] Fatal error: ${err.message}`);
  }

  console.log(`  [AMBA] DONE: ${leads.length} schools scraped\n`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════
// MAIN — Run all scrapers and combine output
// ═══════════════════════════════════════════════════════════════
async function main() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║  Professional Association Directory Scraper              ║');
  console.log('║  IECA + SHRM + NACAC + AMBA                             ║');
  console.log(`║  Mode: ${testMode ? 'TEST (2 pages)' : 'FULL'}${sourceFilter ? ` | Source: ${sourceFilter}` : ''}                                  ║`);
  console.log('╚═══════════════════════════════════════════════════════════╝\n');

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const allLeads = [];
  const stats = {};

  // Run selected scrapers
  const scrapers = {
    'ieca': scrapeIECA,
    'shrm': scrapeSHRM,
    'nacac-independent': scrapeNACACIndependent,
    'nacac-institution': scrapeNACACInstitution,
    'amba': scrapeAMBA,
  };

  const toRun = sourceFilter
    ? { [sourceFilter]: scrapers[sourceFilter] }
    : scrapers;

  if (sourceFilter && !scrapers[sourceFilter]) {
    console.log(`Unknown source: ${sourceFilter}`);
    console.log(`Available: ${Object.keys(scrapers).join(', ')}`);
    process.exit(1);
  }

  for (const [name, fn] of Object.entries(toRun)) {
    try {
      const leads = await fn();
      stats[name] = {
        total: leads.length,
        withEmail: leads.filter(l => l.email).length,
        withPhone: leads.filter(l => l.phone).length,
        withWebsite: leads.filter(l => l.website).length,
      };
      allLeads.push(...leads);
    } catch (err) {
      console.log(`Error running ${name}: ${err.message}`);
      stats[name] = { total: 0, error: err.message };
    }
  }

  // Dedup by name+source
  const seen = new Set();
  const uniqueLeads = allLeads.filter(l => {
    const key = `${(l.name || '').toLowerCase()}|${l.source}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Write CSV
  const csvLines = [CSV_HEADERS.join(',')];
  for (const lead of uniqueLeads) {
    csvLines.push(toCsvRow(lead));
  }
  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');

  // Print summary
  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║  SCRAPING COMPLETE — SUMMARY                             ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');

  for (const [name, s] of Object.entries(stats)) {
    const pad = name.padEnd(20);
    if (s.error) {
      console.log(`║  ${pad} ERROR: ${s.error.substring(0, 35).padEnd(35)} ║`);
    } else {
      console.log(`║  ${pad} ${String(s.total).padStart(5)} leads | ${String(s.withEmail).padStart(4)} email | ${String(s.withPhone).padStart(4)} phone | ${String(s.withWebsite).padStart(4)} web ║`);
    }
  }

  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  TOTAL: ${String(uniqueLeads.length).padStart(5)} unique leads                               ║`);
  console.log(`║  With Email: ${String(uniqueLeads.filter(l => l.email).length).padStart(5)}                                        ║`);
  console.log(`║  With Phone: ${String(uniqueLeads.filter(l => l.phone).length).padStart(5)}                                        ║`);
  console.log(`║  With Website: ${String(uniqueLeads.filter(l => l.website).length).padStart(5)}                                      ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${OUTPUT_FILE.padEnd(47)} ║`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
