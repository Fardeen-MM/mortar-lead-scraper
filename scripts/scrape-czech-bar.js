#!/usr/bin/env node
/**
 * Czech Bar Association (Česká advokátní komora / ČAK) Scraper
 *
 * Scrapes the Czech Bar lawyer directory at https://www.cak.cz/vyhledavani-advokatu
 * to extract all registered lawyers (~14K) with full contact details.
 *
 * Strategy:
 *   1. Iterate surname prefixes a-z (search is diacritic-insensitive, so 'c' includes 'č' etc.)
 *   2. For each letter: fetch page 1, extract total count + max pages from pagination
 *   3. Parse the embedded `const lawyers = [...]` JS array for GUIDs + basic data
 *   4. For each lawyer: fetch /advokat-detail/{guid} profile page for email/phone/website
 *   5. Dedup by GUID, save progress for resume support
 *
 * Output: output/czech-bar-lawyers.csv
 *
 * Usage:
 *   node scripts/scrape-czech-bar.js                  # Full scrape
 *   node scripts/scrape-czech-bar.js --test            # Test mode (2 letters, 2 pages each)
 *   node scripts/scrape-czech-bar.js --resume          # Resume from saved progress
 *   node scripts/scrape-czech-bar.js --letters a,b,c   # Specific letters only
 *   node scripts/scrape-czech-bar.js --skip-profiles   # List GUIDs only (no profile fetches)
 *   node scripts/scrape-czech-bar.js --profiles-only   # Only fetch profiles for already-collected GUIDs
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// --- Config ---
const BASE_URL = 'https://www.cak.cz';
const SEARCH_PATH = '/vyhledavani-advokatu';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'czech-bar-lawyers.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'czech-bar-progress.json');
const DELAY_MS = 1500;           // Delay between requests (ms)
const PROFILE_DELAY_MS = 1200;   // Delay between profile fetches
const PAGE_SIZE = 50;            // Results per page (server-side)
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

// Czech alphabet: a-z covers all diacritics (search is diacritic-insensitive)
const SURNAME_LETTERS = 'abcdefghijklmnopqrstuvwxyz'.split('');

const CSV_HEADERS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
  'website', 'specialization', 'languages', 'registration_number',
  'address', 'mobile', 'profile_url'
];

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const resumeMode = args.includes('--resume');
const skipProfiles = args.includes('--skip-profiles');
const profilesOnly = args.includes('--profiles-only');
const lettersIdx = args.indexOf('--letters');
const lettersFilter = lettersIdx >= 0
  ? args[lettersIdx + 1].split(',').map(s => s.trim().toLowerCase())
  : null;

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  console.log(`[${ts}] ${msg}`);
}

function httpGet(urlStr, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(urlStr);
    const req = https.request({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'cs,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'identity',
      },
    }, (res) => {
      // Handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
        httpGet(redir, retries).then(resolve).catch(reject);
        res.resume(); // drain response
        return;
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf-8');
        if (res.statusCode === 429 || res.statusCode >= 500) {
          if (retries > 0) {
            const backoff = (MAX_RETRIES - retries + 1) * 5000;
            log(`  HTTP ${res.statusCode}, retrying in ${backoff / 1000}s... (${retries - 1} retries left)`);
            setTimeout(() => httpGet(urlStr, retries - 1).then(resolve).catch(reject), backoff);
            return;
          }
        }
        resolve({ status: res.statusCode, body, headers: res.headers });
      });
    });
    req.on('error', (err) => {
      if (retries > 0) {
        const backoff = (MAX_RETRIES - retries + 1) * 5000;
        log(`  Request error: ${err.message}, retrying in ${backoff / 1000}s...`);
        setTimeout(() => httpGet(urlStr, retries - 1).then(resolve).catch(reject), backoff);
      } else {
        reject(err);
      }
    });
    req.setTimeout(REQUEST_TIMEOUT, () => {
      req.destroy();
      if (retries > 0) {
        const backoff = (MAX_RETRIES - retries + 1) * 5000;
        log(`  Timeout, retrying in ${backoff / 1000}s...`);
        setTimeout(() => httpGet(urlStr, retries - 1).then(resolve).catch(reject), backoff);
      } else {
        reject(new Error('Timeout'));
      }
    });
    req.end();
  });
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\xa0/g, ' ')
    .trim();
}

/**
 * Parse the name from the h1 heading.
 * Format: "Advokát - JUDr. ANTONÍN NOVÁK" or "Advokát - Mgr. ALEŠ NOVÁK, LL.M."
 * Returns { first_name, last_name } with title stripped and name in proper case.
 */
function parseName(rawName) {
  if (!rawName) return { first_name: '', last_name: '' };

  // Remove "Advokát - " or "Koncipient - " prefix
  let name = rawName.replace(/^(?:Advokát|Koncipient)\s*-\s*/i, '').trim();

  // Remove Czech academic titles: JUDr., Mgr., Ing., Bc., PhDr., MUDr., RNDr., Doc., Prof.
  // Loop to handle multiple stacked prefixes like "Ing. Mgr. PAVEL ..."
  let prevName;
  do {
    prevName = name;
    name = name.replace(/^(?:JUDr\.|Mgr\.|Ing\.|Bc\.|PhDr\.|MUDr\.|RNDr\.|Doc\.|Prof\.|Dr\.|ICDr\.|PaedDr\.|MVDr\.|ThDr\.|RSDr\.)\s*/i, '').trim();
  } while (name !== prevName);

  // Remove suffixes
  const titleSuffixes = /,?\s*(?:Ph\.?D\.?|LL\.?M\.?|LL\.?B\.?|MBA|MPA|MSc\.?|CSc\.?|DrSc\.?|DSc\.?|D\.E\.A\.?|DEA|MJur|MCL|BCL|BPP|dipl\.\s*\w+)\s*\.?\s*/gi;
  name = name.replace(titleSuffixes, '').trim();

  // Clean extra whitespace / commas
  name = name.replace(/,\s*$/, '').replace(/\s+/g, ' ').trim();

  // Convert from ALL CAPS to proper case
  function toProperCase(s) {
    return s.toLowerCase().replace(/(^|\s|-)(\S)/g, (_, sep, ch) => sep + ch.toUpperCase());
  }

  // Split into parts
  const parts = name.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: '', last_name: toProperCase(parts[0]) };

  // First word = first name, rest = last name
  const firstName = toProperCase(parts[0]);
  const lastName = toProperCase(parts.slice(1).join(' '));

  return { first_name: firstName, last_name: lastName };
}

/**
 * Extract city from address string.
 * Format: "Truhlářská 1108/3 , 11000 Praha" → "Praha"
 * Format: "U Nikolajky 833/5 , 15000 Praha" → "Praha"
 */
function extractCity(address) {
  if (!address) return '';
  // Pattern: postal code (5 digits) followed by city name
  const m = address.match(/\d{3}\s?\d{2}\s+(.+?)$/);
  if (m) return m[1].trim();
  // Fallback: last word after last comma
  const parts = address.split(',');
  if (parts.length > 1) return parts[parts.length - 1].trim();
  return '';
}

// --- Phase 1: Collect lawyer GUIDs from search pages ---

/**
 * Parse search results page HTML.
 * Extracts the `const lawyers = [...]` JS array and pagination info.
 * Returns { lawyers: [{guid, name, firm, address, status}], totalLawyers, totalPages }
 */
function parseSearchPage(html) {
  const result = { lawyers: [], totalLawyers: 0, totalPages: 1 };

  // Extract total count: Celkem <strong>132</strong> advokátů a <strong>21</strong> koncipientů
  const countMatch = html.match(/Celkem\s*<strong>(\d+)<\/strong>\s*advokátů/);
  if (countMatch) {
    result.totalLawyers = parseInt(countMatch[1], 10);
    result.totalPages = Math.ceil(result.totalLawyers / PAGE_SIZE);
  }

  // Extract last page from pagination links
  const pageNums = [...html.matchAll(/page=(\d+)/g)].map(m => parseInt(m[1], 10));
  if (pageNums.length > 0) {
    const maxPage = Math.max(...pageNums);
    // Pagination might not show all pages, but totalPages from count is more reliable
    result.totalPages = Math.max(result.totalPages, maxPage);
  }

  // Parse the const lawyers = [...] JavaScript array
  // Each entry: ['address', 'name', 'status', 'firm', '/advokat-detail/guid', 'photo', 'x', 'y']
  const arrMatch = html.match(/const lawyers = (\[[\s\S]*?\]);/);
  if (arrMatch) {
    try {
      // The array is valid JS but may have HTML entities - evaluate carefully
      // Use regex to extract individual entries instead of eval
      const arrStr = arrMatch[1];
      const entryRegex = /\[\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\]/g;
      let entry;
      while ((entry = entryRegex.exec(arrStr)) !== null) {
        const profilePath = decodeHtmlEntities(entry[5]);
        // Only collect advokat (lawyer) entries, not koncipient (trainee)
        if (!profilePath.startsWith('/advokat-detail/')) continue;
        const guid = profilePath.replace('/advokat-detail/', '');
        result.lawyers.push({
          guid,
          name: decodeHtmlEntities(entry[2]),
          status: decodeHtmlEntities(entry[3]),
          firm: decodeHtmlEntities(entry[4]),
          address: decodeHtmlEntities(entry[1]),
          profilePath,
        });
      }
    } catch (e) {
      log(`  Warning: Failed to parse lawyers array: ${e.message}`);
    }
  }

  // Also extract from the HTML table as a fallback / supplement
  // Pattern: <a href="/advokat-detail/GUID"><strong class="dark-blue">NAME</strong></a>
  const tableRegex = /<a\s+href="\/advokat-detail\/([a-f0-9-]+)"[^>]*>\s*<strong[^>]*>([^<]+)<\/strong>/g;
  let tableMatch;
  const existingGuids = new Set(result.lawyers.map(l => l.guid));
  while ((tableMatch = tableRegex.exec(html)) !== null) {
    const guid = tableMatch[1];
    if (!existingGuids.has(guid)) {
      existingGuids.add(guid);
      result.lawyers.push({
        guid,
        name: decodeHtmlEntities(tableMatch[2]),
        status: 'Aktivní',
        firm: '',
        address: '',
        profilePath: `/advokat-detail/${guid}`,
      });
    }
  }

  return result;
}

/**
 * Fetch all lawyer GUIDs for a given surname letter.
 */
async function collectLetterGuids(letter, maxPages = Infinity) {
  const lawyers = [];
  const seenGuids = new Set();

  // Fetch page 1 to get total count
  const url = `${BASE_URL}${SEARCH_PATH}?search_form%5Bsurname%5D=${encodeURIComponent(letter)}&page=1`;
  log(`Fetching letter "${letter}" page 1...`);
  const res = await httpGet(url);

  if (res.status !== 200) {
    log(`  ERROR: HTTP ${res.status} for letter "${letter}"`);
    return lawyers;
  }

  const parsed = parseSearchPage(res.body);
  log(`  Letter "${letter}": ${parsed.totalLawyers} lawyers, ${parsed.totalPages} pages`);

  // Add page 1 results
  for (const l of parsed.lawyers) {
    if (!seenGuids.has(l.guid)) {
      seenGuids.add(l.guid);
      lawyers.push(l);
    }
  }

  // Paginate through remaining pages
  const pagesToFetch = Math.min(parsed.totalPages, maxPages);
  for (let page = 2; page <= pagesToFetch; page++) {
    await sleep(DELAY_MS);
    const pageUrl = `${BASE_URL}${SEARCH_PATH}?search_form%5Bsurname%5D=${encodeURIComponent(letter)}&page=${page}`;
    log(`  Fetching letter "${letter}" page ${page}/${pagesToFetch}...`);

    try {
      const pageRes = await httpGet(pageUrl);
      if (pageRes.status !== 200) {
        log(`  WARNING: HTTP ${pageRes.status} on page ${page}, skipping`);
        continue;
      }

      const pageParsed = parseSearchPage(pageRes.body);
      let newCount = 0;
      for (const l of pageParsed.lawyers) {
        if (!seenGuids.has(l.guid)) {
          seenGuids.add(l.guid);
          lawyers.push(l);
          newCount++;
        }
      }
      log(`    Page ${page}: ${pageParsed.lawyers.length} entries, ${newCount} new`);

      if (pageParsed.lawyers.length === 0) {
        log(`    Empty page, stopping pagination for letter "${letter}"`);
        break;
      }
    } catch (err) {
      log(`  ERROR on page ${page}: ${err.message}`);
    }
  }

  return lawyers;
}

// --- Phase 2: Fetch profile pages ---

/**
 * Parse a lawyer profile page and extract contact details.
 * Returns { email, phone, mobile, website, address, city, firm, specialization, languages, regNumber }
 */
function parseProfilePage(html) {
  const data = {
    email: '',
    phone: '',
    mobile: '',
    website: '',
    address: '',
    city: '',
    firm: '',
    specialization: '',
    languages: '',
    regNumber: '',
    rawName: '',
  };

  // Extract name from h1: "Advokát - Mgr. ALEŠ NOVÁK, LL.M."
  const h1Match = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/);
  if (h1Match) {
    data.rawName = h1Match[1].replace(/<[^>]+>/g, '').trim();
  }

  // Registration number: badge with number
  const regMatch = html.match(/badge--simple[^>]*>\s*(\d{4,6})\s*</);
  if (regMatch) {
    data.regNumber = regMatch[1];
  }

  // Email: mailto: link
  const emailMatch = html.match(/mailto:([^"]+)/);
  if (emailMatch) {
    data.email = emailMatch[1].trim().toLowerCase();
  }

  // Phone: label "Telefon:" followed by value
  const phoneMatch = html.match(/Telefon:<\/span>\s*(?:<[^>]+>\s*)*([^<]+)/);
  if (phoneMatch) {
    data.phone = phoneMatch[1].trim();
  }

  // Mobile: label "Mobil:" or "Další telefony:"
  const mobileMatch = html.match(/(?:Mobil|Další telefony):<\/span>\s*(?:<[^>]+>\s*)*([^<]+)/);
  if (mobileMatch) {
    data.mobile = mobileMatch[1].trim();
  }

  // Website: label "WWW:" followed by link
  const wwwMatch = html.match(/WWW:<\/span>\s*(?:<[^>]+>\s*)*<a[^>]+href="([^"]+)"/);
  if (wwwMatch) {
    data.website = wwwMatch[1].trim();
  }

  // Address: label "Adresa:" followed by value
  const addrMatch = html.match(/Adresa:<\/span>\s*(?:<[^>]+>\s*)*([^<]+)/);
  if (addrMatch) {
    data.address = decodeHtmlEntities(addrMatch[1].trim());
    data.city = extractCity(data.address);
  }

  // Firm name: label "Název:" followed by value
  const firmMatch = html.match(/Název:<\/span>\s*(?:<[^>]+>\s*)*([^<]+)/);
  if (firmMatch) {
    data.firm = decodeHtmlEntities(firmMatch[1].trim());
  }

  // Specializations: numbered items like "01 generální praxe"
  const specs = [];
  const specRegex = />\s*(\d{2}\s+[^<]{3,})</g;
  let specMatch;
  while ((specMatch = specRegex.exec(html)) !== null) {
    const spec = specMatch[1].trim();
    // Avoid duplicates and non-spec matches
    if (!specs.includes(spec) && /^\d{2}\s/.test(spec)) {
      specs.push(spec);
    }
  }
  data.specialization = specs.join('; ');

  // Languages: in table under "Jazyk" header, values in <div class="text-bold">
  // Bound to </tbody> to avoid picking up specialization data from adjacent sections
  const langIdx = html.indexOf('>Jazyk<');
  if (langIdx >= 0) {
    let langSection = html.slice(langIdx, langIdx + 2000);
    const tbodyEnd = langSection.indexOf('</tbody>');
    if (tbodyEnd > 0) langSection = langSection.slice(0, tbodyEnd);
    const langs = [];
    const langRegex = /text-bold">([^<]+)</g;
    let langMatch;
    while ((langMatch = langRegex.exec(langSection)) !== null) {
      const lang = langMatch[1].trim();
      if (lang && !langs.includes(lang)) {
        langs.push(lang);
      }
    }
    data.languages = langs.join(', ');
  }

  return data;
}

// --- Progress management ---

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
    }
  } catch (e) {
    log(`Warning: Could not load progress file: ${e.message}`);
  }
  return {
    collectedGuids: {},     // letter -> [{guid, name, firm, address}]
    fetchedProfiles: {},    // guid -> profile data
    completedLetters: [],   // letters fully collected
    completedProfiles: [],  // guids fully fetched
    stats: { started: new Date().toISOString() },
  };
}

function saveProgress(progress) {
  try {
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
  } catch (e) {
    log(`Warning: Could not save progress: ${e.message}`);
  }
}

// --- CSV writing ---

function writeCSV(records) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const lines = [CSV_HEADERS.join(',')];
  for (const r of records) {
    const row = CSV_HEADERS.map(h => csvEscape(r[h] || ''));
    lines.push(row.join(','));
  }
  fs.writeFileSync(OUTPUT_FILE, lines.join('\n') + '\n', 'utf-8');
  log(`Wrote ${records.length} records to ${OUTPUT_FILE}`);
}

// --- Main ---

async function main() {
  log('=== Czech Bar Association (ČAK) Scraper ===');
  log(`Mode: ${testMode ? 'TEST' : 'FULL'} | Resume: ${resumeMode} | Skip profiles: ${skipProfiles} | Profiles only: ${profilesOnly}`);

  // Load or initialize progress
  const progress = resumeMode ? loadProgress() : loadProgress(); // always load for dedup
  if (!resumeMode) {
    // Reset if not resuming
    progress.collectedGuids = {};
    progress.fetchedProfiles = {};
    progress.completedLetters = [];
    progress.completedProfiles = [];
    progress.stats = { started: new Date().toISOString() };
  }

  // Determine which letters to process
  let letters = lettersFilter || SURNAME_LETTERS;
  if (testMode) {
    letters = letters.slice(0, 2);
    log(`Test mode: using letters [${letters.join(', ')}]`);
  }

  // ---- Phase 1: Collect GUIDs ----
  if (!profilesOnly) {
    log('\n--- Phase 1: Collecting lawyer GUIDs from search pages ---');
    const allGuids = new Set();

    // Load existing GUIDs from progress
    for (const letter of Object.keys(progress.collectedGuids)) {
      for (const l of progress.collectedGuids[letter]) {
        allGuids.add(l.guid);
      }
    }
    log(`Loaded ${allGuids.size} existing GUIDs from progress`);

    for (const letter of letters) {
      if (resumeMode && progress.completedLetters.includes(letter)) {
        const existing = (progress.collectedGuids[letter] || []).length;
        log(`Skipping letter "${letter}" (already completed, ${existing} GUIDs)`);
        continue;
      }

      const maxPages = testMode ? 2 : Infinity;
      const lawyers = await collectLetterGuids(letter, maxPages);

      // Store results
      progress.collectedGuids[letter] = lawyers;
      progress.completedLetters.push(letter);

      let newCount = 0;
      for (const l of lawyers) {
        if (!allGuids.has(l.guid)) {
          allGuids.add(l.guid);
          newCount++;
        }
      }

      log(`  Letter "${letter}" complete: ${lawyers.length} results, ${newCount} new unique`);
      log(`  Total unique GUIDs so far: ${allGuids.size}`);

      // Save progress after each letter
      saveProgress(progress);
      await sleep(DELAY_MS);
    }

    log(`\nPhase 1 complete. Total unique lawyer GUIDs: ${allGuids.size}`);
  }

  // ---- Phase 2: Fetch profile pages ----
  if (!skipProfiles) {
    log('\n--- Phase 2: Fetching lawyer profile pages ---');

    // Build complete GUID list from all letters
    const allLawyers = new Map(); // guid -> basic data
    for (const letter of Object.keys(progress.collectedGuids)) {
      for (const l of progress.collectedGuids[letter]) {
        if (!allLawyers.has(l.guid)) {
          allLawyers.set(l.guid, l);
        }
      }
    }

    const totalProfiles = allLawyers.size;
    const completedSet = new Set(progress.completedProfiles || []);
    const pendingGuids = [...allLawyers.keys()].filter(g => !completedSet.has(g));

    log(`Total profiles to fetch: ${totalProfiles}, already completed: ${completedSet.size}, pending: ${pendingGuids.length}`);

    if (testMode) {
      // In test mode, only fetch first 10 profiles
      pendingGuids.splice(10);
      log(`Test mode: limiting to ${pendingGuids.length} profiles`);
    }

    let fetched = 0;
    let errors = 0;
    let withEmail = 0;

    for (const guid of pendingGuids) {
      fetched++;
      const profileUrl = `${BASE_URL}/advokat-detail/${guid}`;

      if (fetched % 50 === 0 || fetched <= 3) {
        log(`  Fetching profile ${fetched}/${pendingGuids.length} (${withEmail} emails found so far)...`);
      }

      try {
        const res = await httpGet(profileUrl);
        if (res.status !== 200) {
          log(`  WARNING: HTTP ${res.status} for ${guid}`);
          errors++;
          continue;
        }

        const profileData = parseProfilePage(res.body);
        progress.fetchedProfiles[guid] = profileData;
        progress.completedProfiles.push(guid);

        if (profileData.email) withEmail++;

        // Save progress every 100 profiles
        if (fetched % 100 === 0) {
          saveProgress(progress);
          log(`  Progress saved. ${fetched}/${pendingGuids.length} profiles fetched, ${withEmail} with email`);
        }
      } catch (err) {
        log(`  ERROR fetching profile ${guid}: ${err.message}`);
        errors++;
      }

      await sleep(PROFILE_DELAY_MS);
    }

    log(`\nPhase 2 complete. Fetched: ${fetched}, Errors: ${errors}, With email: ${withEmail}`);
    saveProgress(progress);
  }

  // ---- Phase 3: Build CSV ----
  log('\n--- Phase 3: Building CSV output ---');

  const records = [];
  const seenEmails = new Set();
  const allLawyers = new Map();

  for (const letter of Object.keys(progress.collectedGuids)) {
    for (const l of progress.collectedGuids[letter]) {
      if (!allLawyers.has(l.guid)) {
        allLawyers.set(l.guid, l);
      }
    }
  }

  for (const [guid, basicData] of allLawyers) {
    const profile = progress.fetchedProfiles[guid] || {};
    const nameData = parseName(profile.rawName || basicData.name || '');

    // Use profile city if available, otherwise extract from basic address
    const city = profile.city || extractCity(basicData.address || '');
    const firm = profile.firm || basicData.firm || '';
    const email = (profile.email || '').toLowerCase();

    // Dedup by email (keep first occurrence)
    if (email && seenEmails.has(email)) continue;
    if (email) seenEmails.add(email);

    // Use the best phone available (prefer direct phone over mobile)
    const phone = profile.phone || profile.mobile || '';

    records.push({
      email,
      first_name: nameData.first_name,
      last_name: nameData.last_name,
      company: firm,
      phone,
      city,
      state: '',
      country: 'Czech Republic',
      source: 'czech_bar_cak',
      website: profile.website || '',
      specialization: profile.specialization || '',
      languages: profile.languages || '',
      registration_number: profile.regNumber || '',
      address: profile.address || basicData.address || '',
      mobile: profile.mobile || '',
      profile_url: `${BASE_URL}/advokat-detail/${guid}`,
    });
  }

  // Sort by last name
  records.sort((a, b) => (a.last_name || '').localeCompare(b.last_name || '', 'cs'));

  writeCSV(records);

  // --- Summary ---
  const withEmail = records.filter(r => r.email).length;
  const withPhone = records.filter(r => r.phone).length;
  const withWebsite = records.filter(r => r.website).length;

  log('\n=== SUMMARY ===');
  log(`Total records:  ${records.length}`);
  log(`With email:     ${withEmail} (${(100 * withEmail / Math.max(records.length, 1)).toFixed(1)}%)`);
  log(`With phone:     ${withPhone} (${(100 * withPhone / Math.max(records.length, 1)).toFixed(1)}%)`);
  log(`With website:   ${withWebsite} (${(100 * withWebsite / Math.max(records.length, 1)).toFixed(1)}%)`);
  log(`Output file:    ${OUTPUT_FILE}`);
  log(`Progress file:  ${PROGRESS_FILE}`);
  log('Done.');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
