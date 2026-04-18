#!/usr/bin/env node
/**
 * Yoga / Fitness / Wellness Certification Directory Scraper
 *
 * Scrapes certified instructor/school data from wellness certification directories
 * for cold email outreach selling webinar funnels ("How to become a certified yoga
 * teacher" webinar pitch).
 *
 * === SOURCE 1: NPCP (National Pilates Certification Program) ===
 * URL: https://nationalpilatescertificationprogram.org/NPCP/NPCP/Directory/CertifiedTeachersList.aspx
 * Expected: ~4,117 certified Pilates teachers (NCPT + PMC)
 * Data: email, phone, website, company, city, state, country, cert type, cert dates
 * Method: HTTP GET + ASP.NET __doPostBack pagination (Telerik RadGrid, 10/page)
 * Profile: /NPCP/NPCP/Directory/Certified-Teachers.aspx?ID={code_id}
 * Note: Emails are Cloudflare-protected — decoded via hex decoding
 *
 * === SOURCE 2: Yoga Alliance (documented, NOT scraped here) ===
 * URL: https://app.yogaalliance.org/directoryregistrants?type=school
 * Expected: ~7,449 registered yoga schools (RYS 200/300/500)
 * Problem: Salesforce LWR SPA — requires Puppeteer + API reverse-engineering
 * Data: school name, website, phone, email, address, designation
 * Status: DEFERRED — needs separate Puppeteer-based scraper
 *
 * === SOURCE 3: ACE (American Council on Exercise) ===
 * URL: https://www.acefitness.org/resources/everyone/find-ace-pro/
 * Expected: ~90,000 certified pros
 * Problem: React SPA, API endpoints hidden in compiled bundles
 * Status: DEFERRED — needs network traffic interception
 *
 * === SOURCE 4: NASM (National Academy of Sports Medicine) ===
 * URL: https://directory.nasm.org/
 * Expected: 1.9M+ certified (directory subset unknown)
 * Problem: SPA, no visible API endpoint
 * Status: DEFERRED
 *
 * === SOURCE 5: PMA (Pilates Method Alliance) ===
 * URL: https://portal.pilatesmethodalliance.org/pilates-teacher-directory/
 * Expected: Unknown count
 * Method: ASP.NET form POST, I4A Enterprise platform
 * Data: name, city, state, country, cert type (no email/phone in listing)
 * Status: LOW PRIORITY — less data than NPCP, same teachers
 *
 * === SOURCE 6: NCCAOM (Acupuncture/Oriental Medicine) ===
 * URL: https://directory.nccaom.org/
 * Expected: ~20,000 practitioners
 * Problem: 403 Forbidden on HTTP requests
 * Status: DEFERRED — needs Puppeteer or proxy
 *
 * Output: output/yoga-fitness-schools.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Usage:
 *   node scripts/scrape-yoga-fitness.js                  # Full scrape (all pages + profiles)
 *   node scripts/scrape-yoga-fitness.js --test           # Test mode (first 3 pages, 10 profiles)
 *   node scripts/scrape-yoga-fitness.js --resume         # Resume from progress file
 *   node scripts/scrape-yoga-fitness.js --list-only      # Directory listing only (no profiles)
 *   node scripts/scrape-yoga-fitness.js --page-size=50   # 50 results per page (default 10)
 *   node scripts/scrape-yoga-fitness.js --start-page=50  # Start from page 50
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ============================================================================
// CONFIG
// ============================================================================

const BASE_URL = 'https://nationalpilatescertificationprogram.org';
const DIRECTORY_PATH = '/NPCP/NPCP/Directory/CertifiedTeachersList.aspx';
const PROFILE_PATH = '/NPCP/NPCP/Directory/Certified-Teachers.aspx';
const DIRECTORY_URL = `${BASE_URL}${DIRECTORY_PATH}`;
const PROFILE_URL_BASE = `${BASE_URL}${PROFILE_PATH}`;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'yoga-fitness-schools.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'npcp-progress.json');

const DELAY_MS = 1500;         // Between directory page requests
const PROFILE_DELAY_MS = 1200; // Between profile page requests
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const REQUEST_TIMEOUT = 30000;

const CSV_HEADERS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
  // Extra fields (not in required header but useful for enrichment)
  'website', 'cert_type', 'cert_id', 'cert_since', 'cert_expires',
  'modalities', 'formal_training', 'profile_url',
];

// The Telerik RadGrid control path for __doPostBack pagination
// Format: ctl01$TemplateBody$WebPartManager1$gwpciNewQueryMenuCommon$ciNewQueryMenuCommon$ResultsGrid$Grid1
const GRID_CONTROL_PREFIX = 'ctl01$TemplateBody$WebPartManager1$gwpciNewQueryMenuCommon$ciNewQueryMenuCommon$ResultsGrid$Grid1';

// ============================================================================
// CLI ARGS
// ============================================================================

const cliArgs = process.argv.slice(2);
const testMode = cliArgs.includes('--test');
const resumeMode = cliArgs.includes('--resume');
const listOnly = cliArgs.includes('--list-only');

function getArgValue(name, defaultVal) {
  const arg = cliArgs.find(a => a.startsWith(`--${name}=`));
  return arg ? arg.split('=')[1] : defaultVal;
}

const PAGE_SIZE = parseInt(getArgValue('page-size', '10'));
const START_PAGE = parseInt(getArgValue('start-page', '1'));

// ============================================================================
// HTTP HELPERS
// ============================================================================

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Make an HTTP GET request with redirect following.
 */
function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const req = mod.request({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(options.cookies ? { 'Cookie': options.cookies } : {}),
        ...(options.headers || {}),
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const redir = res.headers.location.startsWith('http')
            ? res.headers.location
            : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
          httpGet(redir, options).then(resolve).catch(reject);
          return;
        }
        resolve({ status: res.statusCode, body: data, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.setTimeout(REQUEST_TIMEOUT, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

/**
 * Make an HTTP POST request (for ASP.NET postback pagination).
 */
function httpPost(url, formFields, cookies = '') {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const postBody = Object.entries(formFields)
      .map(([k, v]) => encodeURIComponent(k) + '=' + encodeURIComponent(v || ''))
      .join('&');

    const req = mod.request({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postBody),
        'Origin': BASE_URL,
        'Referer': DIRECTORY_URL,
        ...(cookies ? { 'Cookie': cookies } : {}),
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, headers: res.headers }));
    });
    req.on('error', reject);
    req.setTimeout(REQUEST_TIMEOUT, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postBody);
    req.end();
  });
}

/**
 * Extract Set-Cookie headers into a single cookie string.
 */
function extractCookies(res, existingCookies = '') {
  const setCookies = res.headers['set-cookie'] || [];
  const newCookies = setCookies.map(c => c.split(';')[0]);

  // Merge with existing cookies
  const cookieMap = {};
  if (existingCookies) {
    for (const pair of existingCookies.split('; ')) {
      const [k, ...v] = pair.split('=');
      if (k) cookieMap[k.trim()] = v.join('=');
    }
  }
  for (const cookie of newCookies) {
    const [k, ...v] = cookie.split('=');
    if (k) cookieMap[k.trim()] = v.join('=');
  }

  return Object.entries(cookieMap).map(([k, v]) => `${k}=${v}`).join('; ');
}

// ============================================================================
// CLOUDFLARE EMAIL DECODING
// ============================================================================

/**
 * Decode Cloudflare email-protected strings.
 *
 * Cloudflare wraps emails in <a href="/cdn-cgi/l/email-protection" class="__cf_email__"
 * data-cfemail="HEXSTRING">. The HEXSTRING is XOR-encoded: first 2 hex chars are the key,
 * then each subsequent pair is XOR'd with that key to produce the real character.
 */
function decodeCfEmail(encodedHex) {
  if (!encodedHex || encodedHex.length < 4) return '';
  const key = parseInt(encodedHex.substring(0, 2), 16);
  let decoded = '';
  for (let i = 2; i < encodedHex.length; i += 2) {
    const charCode = parseInt(encodedHex.substring(i, i + 2), 16) ^ key;
    decoded += String.fromCharCode(charCode);
  }
  return decoded.trim();
}

/**
 * Extract and decode all Cloudflare-protected emails from HTML.
 */
function extractCfEmails(html) {
  const emails = [];
  // Pattern: data-cfemail="HEXSTRING"
  const regex = /data-cfemail="([a-f0-9]+)"/gi;
  let match;
  while ((match = regex.exec(html)) !== null) {
    const decoded = decodeCfEmail(match[1]);
    if (decoded && decoded.includes('@')) {
      emails.push(decoded);
    }
  }
  return emails;
}

// ============================================================================
// ASP.NET FORM STATE HELPERS
// ============================================================================

/**
 * Extract __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION from HTML.
 */
function extractFormState($) {
  return {
    viewState: $('#__VIEWSTATE').val() || '',
    viewStateGenerator: $('#__VIEWSTATEGENERATOR').val() || '',
    eventValidation: $('#__EVENTVALIDATION').val() || '',
  };
}

/**
 * Build the __EVENTTARGET string for Telerik RadGrid pagination.
 *
 * The NPCP directory uses a Telerik RadGrid with postback-based pagination.
 * The "View All" button submits the form. Page navigation uses __doPostBack
 * with the grid control path.
 *
 * Page size change: FireCommand on the grid with arguments like "FireCommand;Grid1;ChangePageSize;50"
 */
function buildPageChangeTarget(page) {
  // For page navigation, the event target is the grid's Page$Change command
  // But NPCP uses a different pattern: ctl01$...Grid1$ctl00$ctl03$ctl01$ctl{XX}
  // where XX increments. However, the simplest approach is to use the grid's
  // built-in Page command.
  return `${GRID_CONTROL_PREFIX}`;
}

// ============================================================================
// DIRECTORY PAGE PARSING
// ============================================================================

/**
 * Parse the directory listing page (Telerik RadGrid).
 * Extracts teacher summary data from the grid rows.
 *
 * Grid columns: Name | City | State | Zip | Country | Cert Type | Cert ID |
 *               Certified Since | Cert Expiration
 */
function parseDirectoryPage($) {
  const teachers = [];

  // Telerik RadGrid rows: .rgRow and .rgAltRow
  $('tr.rgRow, tr.rgAltRow').each((_, row) => {
    const cells = $(row).find('td');
    if (cells.length < 6) return;

    // The first cell contains a link to the profile
    const nameLink = cells.eq(0).find('a');
    const name = nameLink.text().trim();
    if (!name) return;

    // Extract profile ID from the JavaScript ShowDialog link
    const onclick = nameLink.attr('href') || nameLink.attr('onclick') || '';
    const idMatch = onclick.match(/ID=(\d+)/);
    const codeId = idMatch ? idMatch[1] : '';

    // Parse name
    const { first_name, last_name } = splitName(name);

    const teacher = {
      code_id: codeId,
      first_name,
      last_name,
      city: cells.eq(1).text().trim(),
      state: cells.eq(2).text().trim(),
      zip: cells.eq(3).text().trim(),
      country: cells.eq(4).text().trim(),
      cert_type: cells.eq(5).text().trim(),
      cert_id: cells.eq(6).text().trim(),
      cert_since: cells.eq(7).text().trim(),
      cert_expires: cells.eq(8).text().trim(),
      profile_url: codeId ? `${PROFILE_URL_BASE}?ID=${codeId}` : '',
    };

    teachers.push(teacher);
  });

  return teachers;
}

/**
 * Extract total item count and page count from the RadGrid pager.
 * Format: "4117 items in 412 pages"
 */
function extractPagingInfo($) {
  let totalItems = 0;
  let totalPages = 0;

  // Look for the paging info text
  const pagingText = $('.rgInfoPart, .rgWrap').text();
  const match = pagingText.match(/(\d[\d,]*)\s*items?\s+in\s+(\d[\d,]*)\s*pages?/i);
  if (match) {
    totalItems = parseInt(match[1].replace(/,/g, ''));
    totalPages = parseInt(match[2].replace(/,/g, ''));
  }

  // Also try "Displaying items X to Y of Z"
  if (!totalItems) {
    const altMatch = pagingText.match(/of\s+(\d[\d,]*)/);
    if (altMatch) {
      totalItems = parseInt(altMatch[1].replace(/,/g, ''));
    }
  }

  return { totalItems, totalPages };
}

// ============================================================================
// PROFILE PAGE PARSING
// ============================================================================

/**
 * Parse an individual teacher profile page.
 *
 * Profile fields (all server-rendered in HTML):
 *   - Name, City, State/Province, Country
 *   - Email (Cloudflare-protected)
 *   - Company, Website, Contact Number
 *   - NPCT/PMC certification IDs and dates
 *   - Formal Training, Modalities Taught, Professional Bio
 */
function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const data = {};

  // The profile data is rendered as label-value pairs in the page body.
  // We need to find them by text content since IDs are dynamic ASP.NET controls.

  const bodyText = $('body').text();

  // --- Email (Cloudflare-protected) ---
  const cfEmails = extractCfEmails(html);
  if (cfEmails.length > 0) {
    data.email = cfEmails[0].toLowerCase();
  }

  // Also check for plain mailto links
  if (!data.email) {
    $('a[href^="mailto:"]').each((_, el) => {
      const mailto = $(el).attr('href').replace('mailto:', '').trim();
      if (mailto && mailto.includes('@')) {
        data.email = mailto.toLowerCase();
      }
    });
  }

  // --- Parse label-value pairs ---
  // The NPCP profile uses a pattern where labels and values are in adjacent elements.
  // We'll scan all text nodes looking for known labels.

  // Strategy: get all text content, split by known labels, extract values
  const allText = $('body').html() || '';

  // Company/Studio
  const companyMatch = allText.match(/Company[:\s]*<[^>]*>([^<]+)/i);
  if (companyMatch) {
    data.company = companyMatch[1].trim();
  }

  // Website
  const websiteMatch = allText.match(/Website[:\s]*<[^>]*>([^<]+)/i);
  if (websiteMatch) {
    const ws = websiteMatch[1].trim();
    if (ws && ws !== 'N/A' && ws.length > 3) data.website = ws;
  }
  // Also check for href links near "Website" label
  if (!data.website) {
    $('a[href*="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const text = $(el).text().trim();
      if (href && !href.includes('nccaom') && !href.includes('nationalpilatescertification') &&
          !href.includes('cdn-cgi') && !href.includes('javascript:') &&
          (text.includes('http') || text.includes('.com') || text.includes('.org') || text.includes('.net'))) {
        data.website = href;
      }
    });
  }

  // Contact Number / Phone
  const phoneMatch = allText.match(/Contact\s*Number[:\s]*<[^>]*>([^<]+)/i);
  if (phoneMatch) {
    let phone = phoneMatch[1].trim().replace(/[^\d+()-\s]/g, '');
    if (phone.length >= 7) data.phone = phone;
  }

  // Modalities Taught
  const modalMatch = allText.match(/Modalities\s*Taught[:\s]*<[^>]*>([^<]+)/i);
  if (modalMatch) {
    const mod = modalMatch[1].trim();
    if (mod && mod.length > 2) data.modalities = mod;
  }

  // Formal Training
  const trainMatch = allText.match(/Formal\s*Training[:\s]*<[^>]*>([^<]+)/i);
  if (trainMatch) {
    const train = trainMatch[1].trim();
    if (train && train.length > 2) data.formal_training = train;
  }

  // --- Fallback: scan all spans/divs for label-value pairs ---
  // Some NPCP profiles use <span> elements with specific patterns
  $('span, div, td').each((_, el) => {
    const text = $(el).text().trim();

    // Look for phone numbers in text that weren't caught above
    if (!data.phone) {
      const phoneInText = text.match(/(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})/);
      if (phoneInText && !text.includes('Certification') && !text.includes('items')) {
        data.phone = phoneInText[1];
      }
      // International phone patterns (e.g., +34 911 131 619)
      const intlPhone = text.match(/(\+?\d{1,3}[\s.-]?\d{2,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4})/);
      if (intlPhone && !data.phone && text.length < 30) {
        data.phone = intlPhone[1];
      }
    }

    // Look for website URLs
    if (!data.website && text.match(/^https?:\/\//)) {
      const url = text.trim();
      if (!url.includes('nationalpilatescertification') && !url.includes('cdn-cgi')) {
        data.website = url;
      }
    }
  });

  // Clean phone: remove non-numeric except + and spaces
  if (data.phone) {
    data.phone = data.phone.replace(/[^\d+]/g, '');
    // Remove leading 1 for US numbers
    if (data.phone.length === 11 && data.phone.startsWith('1')) {
      data.phone = data.phone.substring(1);
    }
  }

  return data;
}

// ============================================================================
// UTILITY HELPERS
// ============================================================================

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/^(Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.)\s+/i, '')
    .trim()
    .replace(/\s+/g, ' ');

  const parts = cleaned.split(' ');
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  // Handle suffixes
  const suffixes = new Set(['Jr.', 'Jr', 'Sr.', 'Sr', 'II', 'III', 'IV', 'PhD', 'Ph.D.', 'DPT', 'PT', 'OCS']);
  let suffix = '';
  while (parts.length > 2 && suffixes.has(parts[parts.length - 1])) {
    suffix = ' ' + parts.pop() + suffix;
  }

  const lastName = parts.pop() + suffix;
  return { first_name: parts.join(' '), last_name: lastName.trim() };
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ============================================================================
// CSV OUTPUT
// ============================================================================

function writeCsv(teachers, outputPath) {
  if (!teachers.length) {
    console.log('No teachers to write.');
    return;
  }
  const header = CSV_HEADERS.join(',');
  const rows = teachers.map(t =>
    CSV_HEADERS.map(col => csvEscape(t[col])).join(',')
  );
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(outputPath, [header, ...rows].join('\n'), 'utf8');
  console.log(`\nWrote ${teachers.length} teachers to ${outputPath}`);
}

// ============================================================================
// PROGRESS MANAGEMENT
// ============================================================================

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      console.log(`  Loaded progress: ${data.directoryDone ? 'directory done' : 'directory in progress'}, ${Object.keys(data.teachers || {}).length} teachers, ${(data.profilesFetched || []).length} profiles fetched`);
      return data;
    }
  } catch (e) {
    console.warn('  Could not load progress, starting fresh');
  }
  return { directoryDone: false, lastPage: 0, teachers: {}, profilesFetched: [] };
}

function saveProgress(progress) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2), 'utf8');
}

// ============================================================================
// DIRECTORY SCRAPING (Phase 1: listing pages)
// ============================================================================

/**
 * Scrape the NPCP directory listing pages.
 *
 * Phase 1 approach:
 *   1. GET the directory page to establish session + extract form state
 *   2. Optionally change page size to 50 via postback
 *   3. Iterate through all pages using __doPostBack pagination
 *   4. Parse each page for teacher summary data (name, city, state, cert type, code_id)
 *
 * The Telerik RadGrid uses ASP.NET postback for pagination. The event target
 * for page navigation is the grid control, and the event argument encodes
 * the command (e.g., "Page$2" for page 2, or "Page$Next" / "Page$Prev").
 */
async function scrapeDirectory(progress) {
  console.log('\n=== Phase 1: Directory Listing ===\n');

  // Step 1: Initial GET to establish session
  console.log('Fetching directory page...');
  let res;
  try {
    res = await httpGet(DIRECTORY_URL);
  } catch (err) {
    console.error(`Failed to fetch directory: ${err.message}`);
    return [];
  }

  if (res.status !== 200) {
    console.error(`Directory HTTP ${res.status}`);
    return [];
  }

  let cookies = extractCookies(res);
  let $ = cheerio.load(res.body);
  let formState = extractFormState($);
  const { totalItems, totalPages: initialTotalPages } = extractPagingInfo($);

  console.log(`  Total items: ${totalItems}`);
  console.log(`  Initial pages: ${initialTotalPages} (at ${PAGE_SIZE}/page)`);

  // Step 2: If page size > 10, submit a page size change command
  // The RadGrid page size change uses FireCommand:
  // __EVENTTARGET: ctl01$...$Grid1
  // __EVENTARGUMENT: FireCommand;Grid1$ctl00;PageSize;50
  let effectivePageSize = 10; // default
  let effectiveTotalPages = initialTotalPages || Math.ceil(totalItems / 10);

  if (PAGE_SIZE !== 10 && totalItems > 0) {
    console.log(`\n  Changing page size to ${PAGE_SIZE}...`);
    await sleep(DELAY_MS);

    const pageSizeFields = {
      '__EVENTTARGET': GRID_CONTROL_PREFIX,
      '__EVENTARGUMENT': `FireCommand;${GRID_CONTROL_PREFIX.split('$').pop()}$ctl00;PageSize;${PAGE_SIZE}`,
      '__VIEWSTATE': formState.viewState,
      '__VIEWSTATEGENERATOR': formState.viewStateGenerator,
      '__EVENTVALIDATION': formState.eventValidation,
    };

    try {
      const pageSizeRes = await httpPost(DIRECTORY_URL, pageSizeFields, cookies);
      if (pageSizeRes.status === 200) {
        cookies = extractCookies(pageSizeRes, cookies);
        $ = cheerio.load(pageSizeRes.body);
        formState = extractFormState($);
        const newPaging = extractPagingInfo($);
        if (newPaging.totalPages > 0) {
          effectiveTotalPages = newPaging.totalPages;
          effectivePageSize = PAGE_SIZE;
          console.log(`  Page size changed. Now ${effectiveTotalPages} pages.`);
        } else {
          console.log(`  Page size change may not have worked. Continuing with default.`);
        }
      }
    } catch (err) {
      console.log(`  Page size change failed: ${err.message}. Continuing with default.`);
    }
  } else {
    effectiveTotalPages = initialTotalPages || Math.ceil(totalItems / 10);
  }

  // Step 3: Parse page 1 (already loaded)
  const startPage = Math.max(1, resumeMode ? (progress.lastPage || 1) : START_PAGE);
  const allTeachers = {};

  // Restore previously found teachers
  if (progress.teachers) {
    for (const [id, t] of Object.entries(progress.teachers)) {
      allTeachers[id] = t;
    }
    if (Object.keys(allTeachers).length > 0) {
      console.log(`  Restored ${Object.keys(allTeachers).length} teachers from progress`);
    }
  }

  // If we're starting from page 1 and haven't already fetched it
  if (startPage === 1) {
    const page1Teachers = parseDirectoryPage($);
    for (const t of page1Teachers) {
      if (t.code_id) allTeachers[t.code_id] = t;
    }
    console.log(`  Page 1: ${page1Teachers.length} teachers (${Object.keys(allTeachers).length} total)`);
    progress.lastPage = 1;
    progress.teachers = allTeachers;
    saveProgress(progress);
  }

  // Step 4: Paginate through remaining pages
  const maxPage = testMode ? Math.min(startPage + 2, effectiveTotalPages) : effectiveTotalPages;

  for (let page = Math.max(2, startPage + 1); page <= maxPage; page++) {
    await sleep(DELAY_MS);

    // Build postback for page navigation
    // The RadGrid Page command format: "Page${pageNumber}" (1-indexed)
    const pageFields = {
      '__EVENTTARGET': GRID_CONTROL_PREFIX,
      '__EVENTARGUMENT': `Page$${page}`,
      '__VIEWSTATE': formState.viewState,
      '__VIEWSTATEGENERATOR': formState.viewStateGenerator,
      '__EVENTVALIDATION': formState.eventValidation,
    };

    let pageRes;
    try {
      pageRes = await httpPost(DIRECTORY_URL, pageFields, cookies);
    } catch (err) {
      console.error(`  Page ${page} ERROR: ${err.message}`);
      // Save progress and continue
      progress.lastPage = page - 1;
      progress.teachers = allTeachers;
      saveProgress(progress);

      // Retry once after delay
      await sleep(DELAY_MS * 3);
      try {
        pageRes = await httpPost(DIRECTORY_URL, pageFields, cookies);
      } catch (retryErr) {
        console.error(`  Page ${page} RETRY FAILED: ${retryErr.message}`);
        continue;
      }
    }

    if (pageRes.status !== 200) {
      console.error(`  Page ${page} HTTP ${pageRes.status}`);
      continue;
    }

    // Update cookies and form state
    cookies = extractCookies(pageRes, cookies);
    $ = cheerio.load(pageRes.body);
    formState = extractFormState($);

    const pageTeachers = parseDirectoryPage($);
    let newCount = 0;
    for (const t of pageTeachers) {
      if (t.code_id && !allTeachers[t.code_id]) {
        allTeachers[t.code_id] = t;
        newCount++;
      }
    }

    // Progress output
    if (page % 10 === 0 || page === maxPage) {
      console.log(`  Page ${page}/${maxPage}: +${newCount} new (${Object.keys(allTeachers).length} total)`);
    } else {
      process.stdout.write('.');
    }

    // Save progress every 25 pages
    if (page % 25 === 0) {
      progress.lastPage = page;
      progress.teachers = allTeachers;
      saveProgress(progress);
    }
  }

  console.log('');
  progress.directoryDone = true;
  progress.lastPage = maxPage;
  progress.teachers = allTeachers;
  saveProgress(progress);

  const teacherList = Object.values(allTeachers);
  console.log(`\n  Directory complete: ${teacherList.length} unique teachers`);
  return teacherList;
}

// ============================================================================
// PROFILE SCRAPING (Phase 2: individual teacher pages)
// ============================================================================

/**
 * Fetch and parse individual teacher profile pages to get email, phone, website, company.
 *
 * Profile URL: /NPCP/NPCP/Directory/Certified-Teachers.aspx?ID={code_id}
 *
 * Data extracted from profiles:
 *   - Email (Cloudflare-protected, decoded)
 *   - Phone (Contact Number field)
 *   - Website
 *   - Company/Studio name
 *   - Modalities Taught
 *   - Formal Training
 */
async function scrapeProfiles(teachers, progress) {
  console.log('\n=== Phase 2: Profile Pages ===\n');

  const fetchedSet = new Set(progress.profilesFetched || []);
  const needProfile = teachers.filter(t => t.code_id && !fetchedSet.has(t.code_id));

  if (needProfile.length === 0) {
    console.log('  All profiles already fetched.');
    return;
  }

  const maxProfiles = testMode ? Math.min(10, needProfile.length) : needProfile.length;
  console.log(`  ${needProfile.length} profiles to fetch (${fetchedSet.size} already done)`);
  if (testMode) console.log(`  TEST MODE: limiting to ${maxProfiles} profiles`);

  let count = 0;
  let emailCount = 0;
  let phoneCount = 0;
  let websiteCount = 0;

  for (const teacher of needProfile.slice(0, maxProfiles)) {
    count++;
    await sleep(PROFILE_DELAY_MS);

    const profileUrl = teacher.profile_url || `${PROFILE_URL_BASE}?ID=${teacher.code_id}`;

    let profileData = {};
    try {
      const res = await httpGet(profileUrl);
      if (res.status === 200) {
        profileData = parseProfilePage(res.body);
      } else {
        console.error(`  Profile ${teacher.code_id} HTTP ${res.status}`);
        // Retry once
        await sleep(PROFILE_DELAY_MS * 2);
        try {
          const retryRes = await httpGet(profileUrl);
          if (retryRes.status === 200) {
            profileData = parseProfilePage(retryRes.body);
          }
        } catch { /* skip */ }
      }
    } catch (err) {
      console.error(`  Profile ${teacher.code_id} ERROR: ${err.message}`);
      // Retry once
      await sleep(PROFILE_DELAY_MS * 2);
      try {
        const retryRes = await httpGet(profileUrl);
        if (retryRes.status === 200) {
          profileData = parseProfilePage(retryRes.body);
        }
      } catch { /* skip */ }
    }

    // Merge profile data into teacher record
    if (profileData.email) { teacher.email = profileData.email; emailCount++; }
    if (profileData.phone) { teacher.phone = profileData.phone; phoneCount++; }
    if (profileData.website) { teacher.website = profileData.website; websiteCount++; }
    if (profileData.company) { teacher.company = profileData.company; }
    if (profileData.modalities) { teacher.modalities = profileData.modalities; }
    if (profileData.formal_training) { teacher.formal_training = profileData.formal_training; }

    // Set source
    teacher.source = 'npcp_pilates';

    fetchedSet.add(teacher.code_id);

    // Progress output
    if (count % 50 === 0) {
      const pct = ((count / maxProfiles) * 100).toFixed(1);
      console.log(`  ${count}/${maxProfiles} (${pct}%) — emails: ${emailCount}, phones: ${phoneCount}, websites: ${websiteCount}`);

      // Save progress
      progress.profilesFetched = Array.from(fetchedSet);
      saveProgress(progress);
    } else if (count % 10 === 0) {
      process.stdout.write('.');
    }
  }

  console.log('');
  progress.profilesFetched = Array.from(fetchedSet);
  saveProgress(progress);

  console.log(`\n  Profiles complete: ${count} fetched`);
  console.log(`    Emails: ${emailCount}/${count} (${((emailCount/count)*100).toFixed(1)}%)`);
  console.log(`    Phones: ${phoneCount}/${count} (${((phoneCount/count)*100).toFixed(1)}%)`);
  console.log(`    Websites: ${websiteCount}/${count} (${((websiteCount/count)*100).toFixed(1)}%)`);
}

// ============================================================================
// STATS
// ============================================================================

function printStats(teachers) {
  console.log('\n=== FINAL RESULTS ===');
  console.log(`Total teachers: ${teachers.length}`);
  console.log(`  With email: ${teachers.filter(t => t.email).length}`);
  console.log(`  With phone: ${teachers.filter(t => t.phone).length}`);
  console.log(`  With website: ${teachers.filter(t => t.website).length}`);
  console.log(`  With company: ${teachers.filter(t => t.company).length}`);

  // Cert type breakdown
  const certCounts = {};
  for (const t of teachers) {
    const ct = t.cert_type || 'Unknown';
    certCounts[ct] = (certCounts[ct] || 0) + 1;
  }
  console.log('\nCertification types:');
  for (const [type, count] of Object.entries(certCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${type}: ${count}`);
  }

  // Country breakdown
  const countryCounts = {};
  for (const t of teachers) {
    const c = t.country || 'Unknown';
    countryCounts[c] = (countryCounts[c] || 0) + 1;
  }
  console.log('\nTop countries:');
  for (const [country, count] of Object.entries(countryCounts).sort((a, b) => b[1] - a[1]).slice(0, 15)) {
    console.log(`  ${country}: ${count}`);
  }

  // State breakdown (US only)
  const stateCounts = {};
  for (const t of teachers) {
    if (t.country === 'United States' && t.state) {
      stateCounts[t.state] = (stateCounts[t.state] || 0) + 1;
    }
  }
  const topStates = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]).slice(0, 20);
  if (topStates.length > 0) {
    console.log('\nTop 20 US states:');
    for (const [st, cnt] of topStates) console.log(`  ${st}: ${cnt}`);
  }

  // Sample entries
  const withEmail = teachers.filter(t => t.email);
  if (withEmail.length > 0) {
    console.log('\nSample entries with email (first 10):');
    for (const t of withEmail.slice(0, 10)) {
      console.log(`  ${t.first_name} ${t.last_name} | ${t.company || '(no studio)'} | ${t.city}, ${t.state} ${t.country} | ${t.email} | ${t.phone || '(no phone)'}`);
    }
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('==========================================================');
  console.log('  YOGA / FITNESS / WELLNESS CERTIFICATION DIRECTORY SCRAPER');
  console.log('  Source: NPCP (National Pilates Certification Program)');
  console.log('  Expected: ~4,117 certified Pilates teachers');
  console.log('==========================================================\n');

  const startTime = Date.now();

  if (testMode) console.log('TEST MODE: limited pages and profiles\n');
  if (resumeMode) console.log('RESUME MODE: continuing from progress\n');
  if (listOnly) console.log('LIST ONLY: directory pages only, no profile fetching\n');

  // Load progress
  let progress = resumeMode ? loadProgress() : { directoryDone: false, lastPage: 0, teachers: {}, profilesFetched: [] };

  // Phase 1: Directory listing
  let teachers;
  if (progress.directoryDone && resumeMode) {
    teachers = Object.values(progress.teachers);
    console.log(`\nSkipping directory phase (already done, ${teachers.length} teachers)`);
  } else {
    teachers = await scrapeDirectory(progress);
  }

  if (teachers.length === 0) {
    console.error('\nNo teachers found. Check if the NPCP directory is accessible.');
    process.exit(1);
  }

  // Phase 2: Profile pages (unless --list-only)
  if (!listOnly) {
    await scrapeProfiles(teachers, progress);
  } else {
    // Set source for all teachers even without profiles
    for (const t of teachers) {
      t.source = 'npcp_pilates';
    }
  }

  // Phase 3: Write CSV
  writeCsv(teachers, OUTPUT_FILE);

  // Phase 4: Stats
  printStats(teachers);

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nCompleted in ${elapsed} minutes`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log(`Progress: ${PROGRESS_FILE}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
