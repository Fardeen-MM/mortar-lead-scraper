#!/usr/bin/env node
/**
 * AILA Lawyer Directory Scraper
 *
 * Scrapes the American Immigration Lawyers Association directory at
 * https://ailalawyer.com/ to extract immigration lawyer data across all US states.
 *
 * Strategy:
 *   1. Fetch homepage to extract state name->ID mappings from the dropdown
 *   2. For each state: GET search results page 1
 *   3. If multiple pages: use ASP.NET AJAX postback to paginate (25 per page)
 *   4. For each attorney: GET profile page to extract full details
 *   5. Dedup by PersonId, save progress for resume support
 *
 * Output: output/us-aila-immigration-lawyers.csv
 *
 * Usage:
 *   node scripts/scrape-aila.js                      # Full scrape
 *   node scripts/scrape-aila.js --test                # Test with small states only
 *   node scripts/scrape-aila.js --resume              # Resume from saved progress
 *   node scripts/scrape-aila.js --states Alaska,Alabama  # Specific states only
 *   node scripts/scrape-aila.js --skip-profiles       # Search results only (no profile pages)
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// --- Config ---
const BASE_URL = 'https://ailalawyer.com';
const SEARCH_URL = `${BASE_URL}/english/SearchResult.aspx`;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-aila-immigration-lawyers.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'aila-progress.json');
const DELAY_MS = 2000;
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'practice_areas', 'languages', 'law_school', 'member_since'
];

// US state abbreviation lookup
const STATE_ABBREVS = {
  'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
  'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
  'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
  'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
  'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
  'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
  'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
  'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
  'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
  'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI',
  'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
  'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
  'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
  'Puerto Rico': 'PR', 'US Virgin Islands': 'VI', 'Guam': 'GU',
  'American Samoa': 'AS', 'Northern Mariana Islands': 'MP',
  'Armed Forces Americas': 'AA', 'Armed Forces Europe': 'AE', 'Armed Forces Pacific': 'AP',
};

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const resumeMode = args.includes('--resume');
const skipProfiles = args.includes('--skip-profiles');
const statesIdx = args.indexOf('--states');
const statesFilter = statesIdx >= 0
  ? args[statesIdx + 1].split(',').map(s => s.trim())
  : null;

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url, extraHeaders = {}) {
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
        ...extraHeaders,
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const redir = res.headers.location.startsWith('http')
            ? res.headers.location
            : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
          httpGet(redir, extraHeaders).then(resolve).catch(reject);
          return;
        }
        resolve({ status: res.statusCode, body: data, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function httpPost(url, formFields, cookies) {
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
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postBody),
        'X-Requested-With': 'XMLHttpRequest',
        'X-MicrosoftAjax': 'Delta=true',
        ...(cookies ? { 'Cookie': cookies } : {}),
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, headers: res.headers }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postBody);
    req.end();
  });
}

function extractCookies(res) {
  const setCookies = res.headers['set-cookie'] || [];
  return setCookies.map(c => c.split(';')[0]).join('; ');
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function cleanWebsite(url) {
  if (!url) return '';
  // Fix common issues: http://https://... or http://http://...
  let cleaned = url.replace(/^https?:\/\/https?:\/\//i, 'https://');
  // Remove trailing slashes
  cleaned = cleaned.replace(/\/+$/, '');
  return cleaned;
}

function extractDomain(website) {
  if (!website) return '';
  try {
    const cleaned = cleanWebsite(website);
    const u = new URL(cleaned.startsWith('http') ? cleaned : `https://${cleaned}`);
    return u.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName.replace(/^(Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.)\s+/i, '').trim().replace(/\s+/g, ' ');

  const parts = cleaned.split(' ');
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  const suffixes = new Set(['Jr.', 'Jr', 'Sr.', 'Sr', 'II', 'III', 'IV', 'Esq.', 'Esq', 'Ph.D.', 'Ph.D']);
  let suffix = '';
  if (suffixes.has(parts[parts.length - 1])) {
    suffix = ' ' + parts.pop();
  }

  const lastName = parts.pop() + suffix;
  return { first_name: parts.join(' '), last_name: lastName };
}

function parseLocationString(location) {
  // "Anchorage, Alaska" -> { city: 'Anchorage', state: 'AK' }
  if (!location) return { city: '', state: '' };
  const match = location.match(/^(.+?),\s*(.+)$/);
  if (!match) return { city: location, state: '' };
  const city = match[1].trim();
  const stateStr = match[2].trim();
  const stateAbbrev = STATE_ABBREVS[stateStr] || stateStr;
  return { city, state: stateAbbrev };
}

// --- State ID extraction ---
async function fetchStateIds() {
  console.log('Fetching state dropdown from AILA homepage...');
  const res = await httpGet(BASE_URL);
  if (res.status !== 200) throw new Error(`Homepage HTTP ${res.status}`);

  const $ = cheerio.load(res.body);
  const states = [];

  $('select[name="ctl00$cphBody$drpState"] option').each((_, opt) => {
    const val = $(opt).attr('value');
    const text = $(opt).text().trim();
    if (val && text && val !== '' && val !== '0') {
      states.push({ id: val, name: text });
    }
  });

  // Filter to US states only (skip Canadian provinces, territories, etc.)
  const usStates = states.filter(s => STATE_ABBREVS[s.name]);
  console.log(`  Found ${states.length} total, ${usStates.length} US states/territories`);
  return usStates;
}

// --- Search Results Parsing ---
function parseSearchPage(body) {
  const $ = cheerio.load(body);
  const attorneys = [];

  $('tr.rgRow, tr.rgAltRow').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 3) return;

    const nameLink = tds.eq(0).find('a[href*="AttorneyDetail"]');
    if (nameLink.length === 0) return;

    const href = nameLink.attr('href') || '';
    const pMatch = href.match(/P=(\d+)/);
    const aMatch = href.match(/A=(\d+)/);
    if (!pMatch) return;

    const fullName = nameLink.text().trim().replace(/\s+/g, ' ');
    const location = tds.eq(1).text().trim();
    const firm = tds.eq(2).text().trim();
    const profileUrl = `${BASE_URL}/english/${href.replace(/^\.?\//, '')}`;

    const { city, state } = parseLocationString(location);
    const { first_name, last_name } = splitName(fullName);

    attorneys.push({
      personId: pMatch[1],
      attorneyId: aMatch ? aMatch[1] : '',
      first_name,
      last_name,
      firm_name: firm,
      city,
      state,
      profile_url: profileUrl,
    });
  });

  return attorneys;
}

function extractTotalCount(body) {
  // Parse "1 of 22, 1 - 25 of 536" from rgInfoPart
  const match = body.match(/\d+ of \d+, \d+ - \d+ of (\d+)/);
  return match ? parseInt(match[1], 10) : 0;
}

function extractPageLinks(body) {
  const $ = cheerio.load(body);
  const links = [];

  // Get page number links from the RadGrid pager
  $('.rgNumPart a').each((_, el) => {
    const href = $(el).attr('href') || '';
    const text = $(el).text().trim();
    const match = href.match(/__doPostBack\('([^']+)',\s*'([^']*)'\)/);
    if (match) {
      links.push({ page: text === '...' ? 'more' : parseInt(text), target: match[1], arg: match[2] });
    }
  });

  return links;
}

function extractFormState(body) {
  // Try AJAX pipe-delimited format first
  const vsMatch = body.match(/\|__VIEWSTATE\|([^|]+)\|/);
  const evMatch = body.match(/\|__EVENTVALIDATION\|([^|]+)\|/);

  if (vsMatch) {
    return {
      viewState: vsMatch[1],
      eventValidation: evMatch ? evMatch[1] : null,
    };
  }

  // Fall back to full HTML page
  const $ = cheerio.load(body);
  return {
    viewState: $('#__VIEWSTATE').val(),
    viewStateGen: $('#__VIEWSTATEGENERATOR').val(),
    eventValidation: $('#__EVENTVALIDATION').val(),
    previousPage: $('#__PREVIOUSPAGE').val(),
  };
}

// --- Profile Page Parsing ---
function parseProfilePage(body) {
  const $ = cheerio.load(body);
  const data = {};

  // Name (h3 inside #attorney)
  const rawName = $('h3').first().text().trim().replace(/\s+/g, ' ');
  if (rawName) data.fullName = rawName;

  // Website (firm link)
  const firmLink = $('a[id*="hlFirm"]');
  if (firmLink.length) {
    data.website = firmLink.attr('href') || '';
  }

  // Address from Map Location link
  const mapLink = $('a[id*="btnMapLocation"]');
  if (mapLink.length) {
    const mapHref = mapLink.attr('href') || '';
    // Format: https://maps.google.com/?q=+4141 B Street+Suite 205+Anchorage+AK+99503+USA
    // Not super reliable, parse from the address block instead
  }

  // Address block: inside #location > p
  const locationP = $('#location > p, #location p').first();
  if (locationP.length) {
    const addrHtml = locationP.html() || '';
    const parts = addrHtml.split(/<br\s*\/?>/i).map(p => cheerio.load(p).text().trim()).filter(Boolean);

    // First part is firm name, last is phone-like, middle parts are address
    if (parts.length > 0) {
      // Find the city, state zip line
      for (const part of parts) {
        const csMatch = part.match(/^(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
        if (csMatch) {
          data.city = csMatch[1].trim();
          data.state = csMatch[2];
          data.zip = csMatch[3];
        }
      }
      // Firm name is usually the first line
      if (parts[0] && !parts[0].match(/^\d/) && !parts[0].match(/^[A-Z]{2}\s+\d{5}/)) {
        data.firm_name = parts[0];
      }
    }
  }

  // H4 sections in the #right sidebar
  $('h4').each((_, h4) => {
    const label = $(h4).text().trim().toLowerCase();
    const nextP = $(h4).next('p');
    const nextUl = $(h4).next('ul');

    if (label.includes('member since')) {
      data.member_since = nextP.text().trim();
    } else if (label.includes('law school')) {
      data.law_school = nextP.text().trim();
    } else if (label.includes('bar admission')) {
      const items = [];
      nextUl.find('li').each((_, li) => items.push($(li).text().trim()));
      data.bar_admissions = items.join('; ');
    } else if (label.includes('cases handled') || label.includes('practice')) {
      const items = [];
      nextUl.find('li').each((_, li) => items.push($(li).text().trim()));
      data.practice_areas = items.join('; ');
    } else if (label.includes('language')) {
      const items = [];
      nextUl.find('li').each((_, li) => items.push($(li).text().trim()));
      if (items.length) data.languages = items.join('; ');
      else {
        const pText = nextP.text().trim();
        if (pText) data.languages = pText;
      }
    }
  });

  // Bio from #desc
  const desc = $('#desc > p').first().text().trim();
  if (desc && desc.length > 30) {
    data.bio = desc.substring(0, 500);
  }

  return data;
}

// --- Build search URL ---
function buildSearchUrl(stateId, stateName) {
  const params = new URLSearchParams({
    practice: '', prac: '', language: '0', lang: 'English',
    city: '', state: stateId, st: stateName,
    country: '', cy: '', zip: '', miles: '100', last: '',
  });
  return `${SEARCH_URL}?${params.toString()}`;
}

// --- Progress management ---
function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      console.log(`  Loaded progress: ${data.completedStates?.length || 0} states done, ${Object.keys(data.attorneys || {}).length} attorneys`);
      return data;
    }
  } catch (e) {
    console.warn('  Could not load progress, starting fresh');
  }
  return { completedStates: [], attorneys: {}, profilesFetched: [] };
}

function saveProgress(progress) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2), 'utf8');
}

// --- Scrape a single state (all pages) ---
async function scrapeState(stateId, stateName) {
  const searchUrl = buildSearchUrl(stateId, stateName);
  const stateAbbrev = STATE_ABBREVS[stateName] || stateName;

  // Page 1: plain GET
  console.log(`  Fetching page 1...`);
  await sleep(DELAY_MS);

  let res;
  try {
    res = await httpGet(searchUrl);
  } catch (err) {
    console.error(`  ERROR: ${err.message}`);
    return [];
  }

  if (res.status !== 200) {
    console.error(`  HTTP ${res.status}`);
    return [];
  }

  const cookies = extractCookies(res);
  let formState = extractFormState(res.body);

  const page1Attorneys = parseSearchPage(res.body);
  const totalCount = extractTotalCount(res.body);

  console.log(`  Page 1: ${page1Attorneys.length} results (total: ${totalCount})`);

  const allAttorneys = [...page1Attorneys];
  const seenPids = new Set(page1Attorneys.map(a => a.personId));

  // If there are more pages, paginate
  if (totalCount > 25) {
    const totalPages = Math.ceil(totalCount / 25);
    console.log(`  ${totalPages} pages to scrape...`);

    let currentBody = res.body;

    for (let pageNum = 2; pageNum <= totalPages; pageNum++) {
      await sleep(DELAY_MS);

      // Get page links from the current page
      const pageLinks = extractPageLinks(currentBody);
      let targetLink = pageLinks.find(l => l.page === pageNum);

      // If exact page not in links, use "more" to jump to next group
      if (!targetLink) {
        targetLink = pageLinks.find(l => l.page === 'more');
        if (!targetLink) {
          console.warn(`  No link for page ${pageNum}, stopping pagination`);
          break;
        }
      }

      const formData = {
        'ctl00$ctl00$sManager': `ctl00$ctl00$cphBody$cphBody$ctl00$ctl00$cphBody$cphBody$rgResultsPanel|${targetLink.target}`,
        '__EVENTTARGET': targetLink.target,
        '__EVENTARGUMENT': targetLink.arg,
        '__VIEWSTATE': formState.viewState,
        '__VIEWSTATEGENERATOR': formState.viewStateGen || '',
        '__EVENTVALIDATION': formState.eventValidation,
        '__PREVIOUSPAGE': formState.previousPage || '',
        '__ASYNCPOST': 'true',
      };

      let pageRes;
      try {
        pageRes = await httpPost(searchUrl, formData, cookies);
      } catch (err) {
        console.error(`  Page ${pageNum} ERROR: ${err.message}`);
        break;
      }

      // Update form state
      const newState = extractFormState(pageRes.body);
      if (newState.viewState) formState.viewState = newState.viewState;
      if (newState.eventValidation) formState.eventValidation = newState.eventValidation;

      const pageAttorneys = parseSearchPage(pageRes.body);
      let newCount = 0;
      for (const att of pageAttorneys) {
        if (!seenPids.has(att.personId)) {
          seenPids.add(att.personId);
          allAttorneys.push(att);
          newCount++;
        }
      }

      process.stdout.write(`  Page ${pageNum}/${totalPages}: +${newCount} new `);

      currentBody = pageRes.body;

      // Safety: if we got 0 new results, the page didn't change
      if (newCount === 0 && pageAttorneys.length > 0) {
        // Check if this is the "more" link result — it shows the next page group
        // We need to re-read page links and retry
        if (targetLink.page === 'more') {
          // After clicking "...", the current page should show new page links
          process.stdout.write('(loaded next group) ');
          // Don't count this as a data page, continue to next iteration
          pageNum--; // retry same page number with new links
        } else {
          console.log('\n  Pagination stuck, stopping');
          break;
        }
      }

      if (pageNum % 5 === 0) console.log('');
    }

    console.log('');
  }

  console.log(`  ${stateAbbrev}: ${allAttorneys.length} unique attorneys`);
  return allAttorneys;
}

// --- Fetch profile details ---
async function fetchProfile(attorney, retries = 2) {
  const url = attorney.profile_url;
  if (!url) return {};

  try {
    const res = await httpGet(url);
    if (res.status !== 200) {
      if (retries > 0) {
        await sleep(DELAY_MS * 2);
        return fetchProfile(attorney, retries - 1);
      }
      return {};
    }
    return parseProfilePage(res.body);
  } catch (err) {
    if (retries > 0) {
      await sleep(DELAY_MS * 2);
      return fetchProfile(attorney, retries - 1);
    }
    console.error(`    Profile FAILED for ${attorney.first_name} ${attorney.last_name}: ${err.message}`);
    return {};
  }
}

// --- Write CSV ---
function writeCsv(attorneys, outputPath) {
  const header = CSV_HEADERS.join(',');
  const rows = attorneys.map(a =>
    CSV_HEADERS.map(col => csvEscape(a[col])).join(',')
  );
  const csv = [header, ...rows].join('\n');
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(outputPath, csv, 'utf8');
  console.log(`\nWrote ${attorneys.length} attorneys to ${outputPath}`);
}

// --- Print stats ---
function printStats(attorneys) {
  console.log('\n=== FINAL RESULTS ===');
  console.log(`Total attorneys: ${attorneys.length}`);
  console.log(`  With website: ${attorneys.filter(a => a.website).length}`);
  console.log(`  With firm name: ${attorneys.filter(a => a.firm_name).length}`);
  console.log(`  With practice areas: ${attorneys.filter(a => a.practice_areas).length}`);
  console.log(`  With languages: ${attorneys.filter(a => a.languages).length}`);
  console.log(`  With law school: ${attorneys.filter(a => a.law_school).length}`);
  console.log(`  With member since: ${attorneys.filter(a => a.member_since).length}`);
  console.log(`  With domain: ${attorneys.filter(a => a.domain).length}`);

  const stateCounts = {};
  for (const a of attorneys) {
    if (a.state) stateCounts[a.state] = (stateCounts[a.state] || 0) + 1;
  }
  const topStates = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]).slice(0, 20);
  console.log('\nTop 20 states:');
  for (const [st, cnt] of topStates) console.log(`  ${st}: ${cnt}`);

  console.log('\nSample entries (first 10):');
  for (const a of attorneys.slice(0, 10)) {
    console.log(`  ${a.first_name} ${a.last_name} | ${a.firm_name || '(no firm)'} | ${a.city || '?'}, ${a.state || '?'} | ${a.website || '(no site)'}`);
  }
}

// --- Main ---
async function main() {
  console.log('=== AILA Immigration Lawyer Directory Scraper ===\n');
  const startTime = Date.now();

  if (testMode) console.log('TEST MODE: small states only\n');
  if (resumeMode) console.log('RESUME MODE: continuing from progress\n');
  if (skipProfiles) console.log('SKIP PROFILES: search results only\n');

  // Step 1: Get state IDs
  let states = await fetchStateIds();

  if (statesFilter) {
    states = states.filter(s =>
      statesFilter.some(f => s.name.toLowerCase().includes(f.toLowerCase()) || STATE_ABBREVS[s.name] === f.toUpperCase())
    );
    console.log(`Filtered to ${states.length} states: ${states.map(s => s.name).join(', ')}`);
  }

  if (testMode) {
    // Small states for testing
    const testNames = ['Alaska', 'Alabama', 'Wyoming'];
    states = states.filter(s => testNames.includes(s.name));
    console.log(`Test states: ${states.map(s => s.name).join(', ')}`);
  }

  // Step 2: Load progress
  let progress = resumeMode ? loadProgress() : { completedStates: [], attorneys: {}, profilesFetched: [] };

  // Step 3: Restore previously found attorneys
  const allAttorneys = {};
  if (progress.attorneys) {
    for (const [id, att] of Object.entries(progress.attorneys)) {
      allAttorneys[id] = att;
    }
    if (Object.keys(allAttorneys).length > 0) {
      console.log(`Restored ${Object.keys(allAttorneys).length} attorneys from progress\n`);
    }
  }

  // Step 4: Scrape each state
  for (const state of states) {
    if (progress.completedStates.includes(state.id)) {
      console.log(`\nSkipping ${state.name} (already completed)`);
      continue;
    }

    console.log(`\n--- ${state.name} (ID=${state.id}) ---`);

    const stateAttorneys = await scrapeState(state.id, state.name);

    let newCount = 0;
    for (const att of stateAttorneys) {
      if (!allAttorneys[att.personId]) {
        allAttorneys[att.personId] = {
          ...att,
          title: 'Immigration Attorney',
          email: '',
          phone: '',
          website: '',
          domain: '',
          country: 'US',
          niche: 'immigration lawyer',
          source: 'aila',
          practice_areas: '',
          languages: '',
          law_school: '',
          member_since: '',
        };
        newCount++;
      }
    }

    console.log(`  Added ${newCount} new (${Object.keys(allAttorneys).length} total)`);

    progress.completedStates.push(state.id);
    progress.attorneys = allAttorneys;
    saveProgress(progress);
  }

  console.log(`\n=== Search complete: ${Object.keys(allAttorneys).length} unique attorneys ===`);

  // Step 5: Fetch profile pages
  if (!skipProfiles) {
    const attorneyList = Object.values(allAttorneys);
    const fetchedSet = new Set(progress.profilesFetched || []);
    const needProfile = attorneyList.filter(a => !fetchedSet.has(a.personId) && a.profile_url);

    console.log(`\nFetching ${needProfile.length} profile pages (${fetchedSet.size} already done)...`);

    let count = 0;
    for (const att of needProfile) {
      count++;
      await sleep(DELAY_MS);

      const profileData = await fetchProfile(att);

      // Merge profile data
      if (profileData.website && !att.website) att.website = cleanWebsite(profileData.website);
      if (profileData.firm_name && (!att.firm_name || att.firm_name.length < profileData.firm_name.length)) {
        att.firm_name = profileData.firm_name;
      }
      if (profileData.practice_areas) att.practice_areas = profileData.practice_areas;
      if (profileData.languages) att.languages = profileData.languages;
      if (profileData.law_school) att.law_school = profileData.law_school;
      if (profileData.member_since) att.member_since = profileData.member_since;
      if (profileData.city && !att.city) att.city = profileData.city;
      if (profileData.state && !att.state) att.state = profileData.state;
      if (att.website) att.domain = extractDomain(att.website);

      fetchedSet.add(att.personId);
      allAttorneys[att.personId] = att;

      if (count % 25 === 0) {
        console.log(`  Profiles: ${count}/${needProfile.length} (${((count/needProfile.length)*100).toFixed(1)}%)`);
        progress.profilesFetched = Array.from(fetchedSet);
        progress.attorneys = allAttorneys;
        saveProgress(progress);
      } else if (count % 5 === 0) {
        process.stdout.write('.');
      }
    }

    console.log('');
    progress.profilesFetched = Array.from(fetchedSet);
    progress.attorneys = allAttorneys;
    saveProgress(progress);
  }

  // Step 6: Write CSV
  const finalList = Object.values(allAttorneys);
  writeCsv(finalList, OUTPUT_FILE);

  // Step 7: Stats
  printStats(finalList);

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nCompleted in ${elapsed} minutes`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
