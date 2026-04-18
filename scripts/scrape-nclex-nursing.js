#!/usr/bin/env node
/**
 * NCLEX Prep & Nursing School Lead Scraper
 *
 * Scrapes multiple authoritative sources for nursing schools, NCLEX prep
 * companies, and nursing education program contacts:
 *
 *   1. CCNE Directory (directory.ccnecommunity.org)
 *      — Commission on Collegiate Nursing Education accredited programs
 *      — ~1,000+ programs across all 50 states + DC + PR
 *      — Server-rendered HTML, email/phone/website/contact person per program
 *      — Iterates all states x all program types (BSN, MSN, DNP, etc.)
 *
 *   2. California BRN (rn.ca.gov)
 *      — Board of Registered Nursing approved pre-licensure RN programs
 *      — ~200+ programs with school name, city, phone, website
 *      — Clean HTML tables
 *
 *   3. NCLEX Prep Companies (team/about pages)
 *      — UWorld, Kaplan, Hurst, ATI, Archer, SimpleNursing, NurseAchieve,
 *        BoardVitals, Picmonic, Lecturio, NURSING.com, Feuer, TrueLearn,
 *        Mometrix, Princeton Review, Pocket Prep, Mark Klimek, NCSBN
 *      — Scrapes team/about/leadership pages for decision-maker contacts
 *
 *   4. NursingSchoolsAlmanac.com
 *      — Aggregated nursing school listings by state
 *      — School name, location, program types
 *
 * Skipped sources (not viable for HTTP scraping):
 *   - ACEN (acenursing.org): Webflow AJAX, no exposed API
 *   - AHPRA (portal.ahpra.gov.au): Salesforce Lightning SPA
 *   - NMC UK (nmc.org.uk): AJAX-driven, no static listings
 *   - AACN Member Directory: iframe-embedded, JS-driven
 *   - NursingCAS: Requires login to view programs
 *   - NCSBN Database: Aggregate stats only, no program-level data
 *   - Texas BON: SSL cert issue
 *   - Florida MQA: Dynamic search portal
 *
 * Output: output/nclex-nursing-schools.csv
 *
 * Usage:
 *   node scripts/scrape-nclex-nursing.js --test    # Test mode (2 states, limited pages)
 *   node scripts/scrape-nclex-nursing.js --all     # Full scrape all sources
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'nclex-nursing-schools.csv');

const CSV_COLUMNS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
  // Extra columns for enrichment
  'website', 'title', 'program_type', 'profile_url',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return rawPhone.trim();
}

/**
 * Split a full name with credentials into first/last name.
 * Handles formats like "Sherrill J. Smith, PhD, CNL, CNE, RN"
 */
function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Remove everything after the first comma (credentials)
  let cleaned = fullName;
  const commaIdx = cleaned.indexOf(',');
  if (commaIdx > 0) {
    cleaned = cleaned.substring(0, commaIdx).trim();
  }

  // Remove common nursing/academic credentials that might not have commas
  cleaned = cleaned
    .replace(/\s+(PhD|DNP|MSN|BSN|RN|LPN|APRN|FNP|ANP|CNE|CNL|CCRN|CPN|CNS|FAAN|FADLN|EdD|DrPH|DNSc|MBA|MPA|MPH|JD|LNC|NP|BC|AG|PPCNP|FNP-BC|ANP-BC|ACNP|PMHNP|WHNP|PNP|GNP|FNP-C|FACHE|FACNM|FNAP)[-,.\s]*$/gi, '')
    .trim();

  // Remove trailing periods, commas, spaces
  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  const parts = cleaned.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  // Handle "First M. Last" or "First Middle Last"
  return {
    first_name: parts[0],
    last_name: parts[parts.length - 1],
  };
}

function dedupKey(lead) {
  const email = (lead.email || '').toLowerCase().trim();
  const firstName = (lead.first_name || '').toLowerCase().trim();
  const lastName = (lead.last_name || '').toLowerCase().trim();
  const company = (lead.company || '').toLowerCase().trim();
  const city = (lead.city || '').toLowerCase().trim();

  if (email) return `email:${email}`;
  if (firstName && lastName && company) return `name:${firstName}|${lastName}|${company}`;
  if (firstName && lastName && city) return `name:${firstName}|${lastName}|${city}`;
  if (company && city) return `company:${company}|${city}`;
  if (company) return `company:${company}`;
  return `rand:${Math.random()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        ...extraHeaders,
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = mod.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries, extraHeaders).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── US States ────────────────────────────────────────────────────────────────

const US_STATES = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia',
  'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
  'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
  'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
  'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota',
  'MO': 'Missouri', 'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
  'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
  'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio',
  'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
  'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
  'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VT': 'Vermont',
  'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming',
};

// ══════════════════════════════════════════════════════════════════════════════
// Source 1: CCNE Directory (directory.ccnecommunity.org)
// ══════════════════════════════════════════════════════════════════════════════
//
// The CCNE accreditation directory is server-rendered ASP with ~1,000+ programs.
// Each state+programType combination returns a page with all programs listed.
//
// HTML structure per program:
//   <table id="finder"> wraps each institution
//     <h3> Institution Name </h3>
//     <br/> Department Name<br>Street Address<BR>City, ST ZIP <br/>
//     <a href=URL>Link to Website</a>
//     Chief Nurse Administrator: Name, Credentials <br>
//     Title: Role <br/>
//     <span class="email">E-Mail: address@school.edu</span><br/>
//     Phone: 123-456-7890 <br/>
//     Fax: ... (optional)
//   Programs are separated by <hr> tags
//
// URL pattern:
//   /reports/rptAccreditedPrograms_New.asp?state=XX&sFullName=State+Name&sProgramType=N
//
// Program types:
//   1,12 = Baccalaureate
//   2,6,7,8,10 = Masters
//   3 = Doctor of Nursing Practice (DNP)
//   4 = Employee-Based Entry-to-Practice Residency
//   5 = Post-Graduate APRN Certificate
//   9 = Federally Funded Traineeship Entry-to-Practice Residency
//   11 = NP Fellowship/Residency

const CCNE_BASE = 'https://directory.ccnecommunity.org/reports/rptAccreditedPrograms_New.asp';

const CCNE_PROGRAM_TYPES = [
  { value: '1', label: 'Baccalaureate' },
  { value: '2', label: 'Masters' },
  { value: '3', label: 'DNP' },
  { value: '5', label: 'Post-Graduate APRN Certificate' },
];
// Skipping types 4, 9, 11 — very few programs in residency/fellowship categories

/**
 * Parse a single CCNE state+programType page.
 * Each program is wrapped in a <table id="finder"> block.
 */
function parseCCNEPage(html, stateCode, programType) {
  const $ = cheerio.load(html);
  const leads = [];

  // The page has a messy structure — each institution is in its own <table id="finder">
  // We'll parse the raw HTML using regex-based extraction since the DOM is inconsistent
  // Split the HTML by <table...id="finder"> to isolate each institution block
  const blocks = html.split(/<table[^>]*id="finder"[^>]*>/i);

  for (let i = 1; i < blocks.length; i++) {
    const block = blocks[i];
    const $b = cheerio.load('<div>' + block + '</div>');

    try {
      // Institution name from <h3> tag (first one is the school name)
      const h3s = [];
      $b('h3').each((_, el) => {
        const text = $b(el).text().trim();
        if (text && !text.match(/^(Program|Initial|Most Recent|Accreditation|Last|Next|Baccalaureate|Master|Doctor|Post-Graduate|Employee|Federally|NP Fellowship)/i)) {
          h3s.push(text);
        }
      });

      const institution = h3s[0] || '';
      if (!institution) continue;

      // Skip the state header (e.g., "FLORIDA", "NEW YORK")
      if (institution === institution.toUpperCase() && institution.length < 30) continue;

      // Extract the full text content for regex parsing
      const fullText = $b.text();

      // Website URL
      let website = '';
      const linkEl = $b('a[href*="http"]').first();
      if (linkEl.length) {
        const href = linkEl.attr('href') || '';
        if (href && !href.includes('ccnecommunity') && !href.includes('accprog.asp')) {
          website = href;
        }
      }

      // Chief Nurse Administrator name + credentials
      let adminName = '';
      let adminTitle = '';
      const adminMatch = fullText.match(/Chief Nurse Administrator:\s*([^\n]+)/i);
      if (adminMatch) {
        adminName = adminMatch[1].trim();
      }

      const titleMatch = fullText.match(/Title:\s*([^\n]+)/i);
      if (titleMatch) {
        adminTitle = titleMatch[1].trim();
      }

      // Email — inside <span class="email">
      let email = '';
      const emailSpan = $b('span.email').text();
      if (emailSpan) {
        const emailMatch = emailSpan.match(/E-Mail:\s*(\S+@\S+\.\S+)/i);
        if (emailMatch) {
          email = emailMatch[1].trim().replace(/[<>]/g, '');
        }
      }
      // Fallback: regex on full text
      if (!email) {
        const emailRegex = fullText.match(/E-Mail:\s*(\S+@\S+\.\S+)/i);
        if (emailRegex) {
          email = emailRegex[1].trim().replace(/[<>]/g, '');
        }
      }

      // Phone
      let phone = '';
      const phoneMatch = fullText.match(/Phone:\s*([\d\-().+ ext]+)/i);
      if (phoneMatch) {
        phone = phoneMatch[1].trim();
      }

      // Address — extract from the HTML between institution name and "Link to Website"
      let city = '';
      let state = stateCode;
      const addressHtml = block.match(/<br\s*\/?>\s*([^<]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*<br\s*\/?>/i);
      if (addressHtml) {
        city = addressHtml[1].trim();
        state = addressHtml[2].trim();
      }

      // Split administrator name
      const { first_name, last_name } = splitName(adminName);

      leads.push({
        email: email.toLowerCase(),
        first_name,
        last_name,
        company: institution,
        phone: formatPhone(phone),
        city,
        state,
        country: 'US',
        source: 'ccne-directory',
        website,
        title: adminTitle,
        program_type: programType,
        profile_url: `${CCNE_BASE}?state=${stateCode}&sFullName=${encodeURIComponent(US_STATES[stateCode] || '')}&sProgramType=${programType === 'Baccalaureate' ? '1' : programType === 'Masters' ? '2' : programType === 'DNP' ? '3' : '5'}`,
      });
    } catch (err) {
      // Skip malformed blocks
    }
  }

  return leads;
}

async function scrapeCCNE(isTest) {
  const SOURCE = 'ccne-directory';
  const leads = [];
  const seenInstitutions = new Set();

  const states = isTest
    ? [['FL', 'Florida'], ['NY', 'New York']]
    : Object.entries(US_STATES).map(([code, name]) => [code, name]);

  const programTypes = isTest
    ? [CCNE_PROGRAM_TYPES[0]] // Baccalaureate only in test mode
    : CCNE_PROGRAM_TYPES;

  log(`\n=== CCNE Accredited Programs Directory ===`);
  log(`States: ${states.length}, Program types: ${programTypes.length}`);
  log(`Total requests: ${states.length * programTypes.length}`);

  let totalRequests = 0;
  for (const pType of programTypes) {
    log(`\n  --- Program type: ${pType.label} ---`);

    for (const [stateCode, stateName] of states) {
      const url = `${CCNE_BASE}?state=${stateCode}&sFullName=${encodeURIComponent(stateName)}&sProgramType=${pType.value}`;
      totalRequests++;

      try {
        const { statusCode, body } = await httpGet(url);
        if (statusCode !== 200 || !body) {
          log(`    [CCNE] ${stateCode} ${pType.label}: HTTP ${statusCode}`);
          await randomDelay();
          continue;
        }

        const pageLeads = parseCCNEPage(body, stateCode, pType.label);

        let added = 0;
        for (const lead of pageLeads) {
          // Dedup within CCNE: same institution may appear under multiple program types
          // We want to keep all entries since different program types = different contacts
          const key = `${lead.company}|${lead.email}|${pType.label}`.toLowerCase();
          if (seenInstitutions.has(key)) continue;
          seenInstitutions.add(key);
          leads.push(lead);
          added++;
        }

        log(`    [CCNE] ${stateCode} ${pType.label}: ${pageLeads.length} found, ${added} new`);
      } catch (err) {
        log(`    [CCNE] ${stateCode} ${pType.label}: ERROR - ${err.message}`);
      }

      await randomDelay();
    }
  }

  log(`\n  CCNE total: ${leads.length} leads from ${totalRequests} requests`);
  return leads;
}


// ══════════════════════════════════════════════════════════════════════════════
// Source 2: California BRN (rn.ca.gov)
// ══════════════════════════════════════════════════════════════════════════════
//
// The California Board of Registered Nursing publishes approved pre-licensure
// RN programs in a clean HTML table at /education/rnprograms.shtml
//
// Table columns: School Name (linked), City, Phone, Tuition, Costs, Enrollment, LVN
// ~200+ programs across Associate, Baccalaureate, and Entry Level Masters

const CA_BRN_URLS = [
  { url: 'https://www.rn.ca.gov/education/rnprograms.shtml', label: 'Pre-licensure RN Programs' },
  { url: 'https://www.rn.ca.gov/education/apprograms.shtml', label: 'Advanced Practice Programs' },
];

function parseCaliforniaBRN(html, sourceLabel) {
  const $ = cheerio.load(html);
  const leads = [];

  $('table.table tr').each((_, row) => {
    const cells = $(row).find('td');
    if (cells.length < 3) return;

    const schoolCell = cells.eq(0);
    const cityCell = cells.eq(1);
    const phoneCell = cells.eq(2);

    // School name and website
    const link = schoolCell.find('a').first();
    const company = (link.text() || schoolCell.text()).trim();
    const website = link.attr('href') || '';

    // Skip header rows and empty rows
    if (!company || company === 'School Name') return;

    // Remove asterisks and other markers
    const cleanName = company.replace(/^\*\s*/, '').trim();

    const city = cityCell.text().trim();
    const phone = phoneCell.text().trim();

    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company: cleanName,
      phone: formatPhone(phone),
      city,
      state: 'CA',
      country: 'US',
      source: 'california-brn',
      website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
      title: '',
      program_type: sourceLabel,
      profile_url: 'https://www.rn.ca.gov/education/rnprograms.shtml',
    });
  });

  return leads;
}

async function scrapeCaliforniaBRN(isTest) {
  const leads = [];

  log(`\n=== California BRN Approved Programs ===`);

  const urls = isTest ? [CA_BRN_URLS[0]] : CA_BRN_URLS;

  for (const { url, label } of urls) {
    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`  [CA-BRN] ${label}: HTTP ${statusCode}`);
        continue;
      }

      const pageLeads = parseCaliforniaBRN(body, label);
      leads.push(...pageLeads);
      log(`  [CA-BRN] ${label}: ${pageLeads.length} programs found`);
    } catch (err) {
      log(`  [CA-BRN] ${label}: ERROR - ${err.message}`);
    }
    await randomDelay();
  }

  log(`  CA-BRN total: ${leads.length} programs`);
  return leads;
}


// ══════════════════════════════════════════════════════════════════════════════
// Source 3: NCLEX Prep Companies (team/about pages)
// ══════════════════════════════════════════════════════════════════════════════
//
// Scrapes about/team/leadership pages of known NCLEX prep companies to find
// decision-maker contacts (founders, CEOs, marketing directors).

const PREP_COMPANIES = [
  {
    name: 'UWorld',
    url: 'https://nursing.uworld.com/',
    aboutUrl: 'https://www.uworld.com/about.aspx',
    website: 'https://nursing.uworld.com',
    city: 'Dallas', state: 'TX',
  },
  {
    name: 'Kaplan Nursing',
    url: 'https://www.kaptest.com/nclex',
    aboutUrl: 'https://www.kaptest.com/about',
    website: 'https://www.kaptest.com/nclex',
    city: 'New York', state: 'NY',
  },
  {
    name: 'Hurst Review Services',
    url: 'https://www.hurstreview.com/',
    aboutUrl: 'https://www.hurstreview.com/about',
    website: 'https://www.hurstreview.com',
    city: 'Brookhaven', state: 'MS',
  },
  {
    name: 'ATI Testing',
    url: 'https://www.atitesting.com/',
    aboutUrl: 'https://www.atitesting.com/about',
    website: 'https://www.atitesting.com',
    city: 'Leawood', state: 'KS',
  },
  {
    name: 'Archer Review',
    url: 'https://www.archerreview.com/',
    aboutUrl: 'https://www.archerreview.com/about-us',
    website: 'https://www.archerreview.com',
    city: '', state: '',
  },
  {
    name: 'SimpleNursing',
    url: 'https://simplenursing.com/',
    aboutUrl: 'https://simplenursing.com/about/',
    website: 'https://simplenursing.com',
    city: '', state: '',
  },
  {
    name: 'NurseAchieve',
    url: 'https://nurseachieve.com/',
    aboutUrl: 'https://nurseachieve.com/en-int/about',
    website: 'https://nurseachieve.com',
    city: 'Toronto', state: 'ON', country: 'CA',
  },
  {
    name: 'NURSING.com',
    url: 'https://nursing.com/',
    aboutUrl: 'https://nursing.com/about/',
    website: 'https://nursing.com',
    city: '', state: '',
  },
  {
    name: 'BoardVitals',
    url: 'https://www.boardvitals.com/',
    aboutUrl: 'https://www.boardvitals.com/about',
    website: 'https://www.boardvitals.com',
    city: 'New York', state: 'NY',
  },
  {
    name: 'Picmonic',
    url: 'https://www.picmonic.com/',
    aboutUrl: 'https://www.picmonic.com/pages/about/',
    website: 'https://www.picmonic.com',
    city: 'Tempe', state: 'AZ',
  },
  {
    name: 'Lecturio',
    url: 'https://www.lecturio.com/',
    aboutUrl: 'https://www.lecturio.com/about',
    website: 'https://www.lecturio.com',
    city: 'Leipzig', state: '', country: 'DE',
  },
  {
    name: 'TrueLearn',
    url: 'https://truelearn.com/',
    aboutUrl: 'https://truelearn.com/about/',
    website: 'https://truelearn.com',
    city: '', state: '',
  },
  {
    name: 'Feuer Nursing Review',
    url: 'https://www.feuernursingreview.com/',
    aboutUrl: 'https://www.feuernursingreview.com/about-us/',
    website: 'https://www.feuernursingreview.com',
    city: 'Hollywood', state: 'FL',
  },
  {
    name: 'Mometrix NCLEX Prep',
    url: 'https://www.mometrix.com/academy/nclex-rn-exam/',
    aboutUrl: 'https://www.mometrix.com/about/',
    website: 'https://www.mometrix.com',
    city: 'Beaumont', state: 'TX',
  },
  {
    name: 'Princeton Review NCLEX',
    url: 'https://www.princetonreview.com/professional/nclex-rn-selfguided-course',
    aboutUrl: 'https://www.princetonreview.com/about',
    website: 'https://www.princetonreview.com',
    city: 'New York', state: 'NY',
  },
  {
    name: 'Pocket Prep NCLEX',
    url: 'https://www.pocketprep.com/exams/nclex-rn/',
    aboutUrl: 'https://www.pocketprep.com/about-us/',
    website: 'https://www.pocketprep.com',
    city: 'Charlotte', state: 'NC',
  },
  {
    name: 'Mark Klimek NCLEX Reviews',
    url: 'https://klimekreviews.com/',
    aboutUrl: 'https://klimekreviews.com/about/',
    website: 'https://klimekreviews.com',
    city: '', state: '',
  },
  {
    name: 'NCSBN Learning Extension',
    url: 'https://www.ncsbn.org/',
    aboutUrl: 'https://www.ncsbn.org/about.page',
    website: 'https://www.ncsbn.org',
    city: 'Chicago', state: 'IL',
  },
  {
    name: 'Saunders NCLEX Review (Elsevier)',
    url: 'https://evolve.elsevier.com/',
    aboutUrl: 'https://www.elsevier.com/about',
    website: 'https://evolve.elsevier.com',
    city: 'Amsterdam', state: '', country: 'NL',
  },
  {
    name: 'NCLEX Bootcamp',
    url: 'https://nclexbootcamp.com/',
    aboutUrl: 'https://nclexbootcamp.com/about/',
    website: 'https://nclexbootcamp.com',
    city: '', state: '',
  },
  {
    name: 'Nurse Plus Academy',
    url: 'https://nurse.plus/',
    aboutUrl: 'https://nurse.plus/about/',
    website: 'https://nurse.plus',
    city: '', state: '',
  },
  {
    name: 'Osmosis (Elsevier)',
    url: 'https://www.osmosis.org/',
    aboutUrl: 'https://www.osmosis.org/about',
    website: 'https://www.osmosis.org',
    city: '', state: '',
  },
  {
    name: 'Study.com NCLEX',
    url: 'https://study.com/academy/course/next-gen-nclex-rn-course.html',
    aboutUrl: 'https://study.com/pages/About_Us.html',
    website: 'https://study.com',
    city: 'Mountain View', state: 'CA',
  },
  {
    name: "Kevin's Review",
    url: 'https://www.kevinsreview.com/',
    aboutUrl: 'https://www.kevinsreview.com/about/',
    website: 'https://www.kevinsreview.com',
    city: '', state: '',
  },
  {
    name: 'GoodNurse',
    url: 'https://www.goodnurse.com/',
    aboutUrl: 'https://www.goodnurse.com/about',
    website: 'https://www.goodnurse.com',
    city: '', state: '',
  },
  {
    name: 'Achievable NCLEX',
    url: 'https://achievable.me/nclex/',
    aboutUrl: 'https://achievable.me/about/',
    website: 'https://achievable.me',
    city: 'San Francisco', state: 'CA',
  },
  {
    name: 'FRESHRN',
    url: 'https://www.freshrn.com/',
    aboutUrl: 'https://www.freshrn.com/about/',
    website: 'https://www.freshrn.com',
    city: '', state: '',
  },
  {
    name: 'Nurse.org',
    url: 'https://nurse.org/',
    aboutUrl: 'https://nurse.org/about/',
    website: 'https://nurse.org',
    city: '', state: '',
  },
  {
    name: 'RegisteredNursing.org',
    url: 'https://www.registerednursing.org/',
    aboutUrl: 'https://www.registerednursing.org/about/',
    website: 'https://www.registerednursing.org',
    city: '', state: '',
  },
  {
    name: 'AllNurses.com',
    url: 'https://allnurses.com/',
    aboutUrl: 'https://allnurses.com/pages/about-us/',
    website: 'https://allnurses.com',
    city: '', state: '',
  },
  {
    name: 'NursingProcess.org',
    url: 'https://www.nursingprocess.org/',
    aboutUrl: 'https://www.nursingprocess.org/about/',
    website: 'https://www.nursingprocess.org',
    city: '', state: '',
  },
  {
    name: 'AllNursingSchools.com',
    url: 'https://www.allnursingschools.com/',
    aboutUrl: 'https://www.allnursingschools.com/about/',
    website: 'https://www.allnursingschools.com',
    city: '', state: '',
  },
  {
    name: 'NursingSchoolsAlmanac.com',
    url: 'https://www.nursingschoolsalmanac.com/',
    aboutUrl: 'https://www.nursingschoolsalmanac.com/about',
    website: 'https://www.nursingschoolsalmanac.com',
    city: '', state: '',
  },
];

/**
 * Scrape a company about/team page for contact information.
 * Extracts emails from page, looks for team member names + titles.
 */
function parseTeamPage(html, company) {
  const $ = cheerio.load(html);
  const leads = [];
  const fullText = $('body').text();

  // Extract all email addresses from the page
  const emailRegex = /[\w.+-]+@[\w-]+\.[\w.-]+/g;
  const emails = [];
  let match;
  while ((match = emailRegex.exec(fullText)) !== null) {
    const email = match[0].toLowerCase();
    // Filter out common non-person emails
    if (!email.match(/(noreply|no-reply|support|info@|sales@|help@|admin@|contact@|privacy@|legal@|abuse@|postmaster@|webmaster@|unsubscribe)/i)) {
      emails.push(email);
    }
  }

  // Try to find team members from structured patterns
  // Pattern: heading with name + paragraph with title
  // Common in about pages: "John Smith - CEO", "Jane Doe, Founder"
  const teamPatterns = [
    // JSON-LD structured data
    () => {
      const jsonLd = [];
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const data = JSON.parse($(el).html());
          if (data['@type'] === 'Person' || (Array.isArray(data) && data.some(d => d['@type'] === 'Person'))) {
            const people = Array.isArray(data) ? data.filter(d => d['@type'] === 'Person') : [data];
            for (const p of people) {
              if (p.name) {
                const { first_name, last_name } = splitName(p.name);
                jsonLd.push({ first_name, last_name, title: p.jobTitle || '', email: p.email || '' });
              }
            }
          }
        } catch {}
      });
      return jsonLd;
    },
    // Cards with images and name/title (common team page pattern)
    () => {
      const people = [];
      $('[class*="team"], [class*="member"], [class*="staff"], [class*="leader"], [class*="founder"], [class*="about"]').find('h2, h3, h4, h5, strong').each((_, el) => {
        const name = $(el).text().trim();
        if (name.length < 3 || name.length > 60) return;
        if (name.split(/\s+/).length < 2 || name.split(/\s+/).length > 4) return;
        // Check it looks like a person name
        if (!/^[A-Z][a-z]/.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (!first_name || !last_name) return;

        // Look for a title nearby
        const parent = $(el).parent();
        let title = '';
        const titleEl = parent.find('p, span, [class*="title"], [class*="role"], [class*="position"]').first();
        if (titleEl.length) {
          title = titleEl.text().trim().substring(0, 100);
        }

        people.push({ first_name, last_name, title, email: '' });
      });
      return people;
    },
  ];

  let foundPeople = [];
  for (const patternFn of teamPatterns) {
    try {
      const result = patternFn();
      if (result && result.length > 0) {
        foundPeople = result;
        break;
      }
    } catch {}
  }

  // If we found people, create leads for each
  if (foundPeople.length > 0) {
    for (const person of foundPeople) {
      leads.push({
        email: person.email || '',
        first_name: person.first_name,
        last_name: person.last_name,
        company: company.name,
        phone: '',
        city: company.city || '',
        state: company.state || '',
        country: company.country || 'US',
        source: 'nclex-prep-company',
        website: company.website,
        title: person.title,
        program_type: 'NCLEX Prep',
        profile_url: company.aboutUrl,
      });
    }
  }

  // If we found unique emails but no people, create company-level leads
  if (leads.length === 0 && emails.length > 0) {
    const uniqueEmails = [...new Set(emails)].slice(0, 3); // Max 3 emails per company
    for (const email of uniqueEmails) {
      leads.push({
        email,
        first_name: '',
        last_name: '',
        company: company.name,
        phone: '',
        city: company.city || '',
        state: company.state || '',
        country: company.country || 'US',
        source: 'nclex-prep-company',
        website: company.website,
        title: '',
        program_type: 'NCLEX Prep',
        profile_url: company.aboutUrl,
      });
    }
  }

  // Always add at least the company itself as a lead (for firms without team pages)
  if (leads.length === 0) {
    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company: company.name,
      phone: '',
      city: company.city || '',
      state: company.state || '',
      country: company.country || 'US',
      source: 'nclex-prep-company',
      website: company.website,
      title: '',
      program_type: 'NCLEX Prep',
      profile_url: company.url,
    });
  }

  return leads;
}

async function scrapeNCLEXPrepCompanies(isTest) {
  const leads = [];

  const companies = isTest ? PREP_COMPANIES.slice(0, 5) : PREP_COMPANIES;

  log(`\n=== NCLEX Prep Companies ===`);
  log(`Companies to scrape: ${companies.length}`);

  for (const company of companies) {
    try {
      // Try about/team page first
      const { statusCode, body } = await httpGet(company.aboutUrl);

      if (statusCode === 200 && body) {
        const pageLeads = parseTeamPage(body, company);
        leads.push(...pageLeads);
        const withEmail = pageLeads.filter(l => l.email).length;
        log(`  [Prep] ${company.name}: ${pageLeads.length} leads (${withEmail} with email)`);
      } else {
        // Fallback: add company as a lead anyway
        log(`  [Prep] ${company.name}: HTTP ${statusCode} — adding company only`);
        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company: company.name,
          phone: '',
          city: company.city || '',
          state: company.state || '',
          country: company.country || 'US',
          source: 'nclex-prep-company',
          website: company.website,
          title: '',
          program_type: 'NCLEX Prep',
          profile_url: company.url,
        });
      }
    } catch (err) {
      log(`  [Prep] ${company.name}: ERROR - ${err.message}`);
      // Still add the company
      leads.push({
        email: '',
        first_name: '',
        last_name: '',
        company: company.name,
        phone: '',
        city: company.city || '',
        state: company.state || '',
        country: company.country || 'US',
        source: 'nclex-prep-company',
        website: company.website,
        title: '',
        program_type: 'NCLEX Prep',
        profile_url: company.url,
      });
    }

    await randomDelay();
  }

  log(`  Prep companies total: ${leads.length} leads`);
  return leads;
}


// ══════════════════════════════════════════════════════════════════════════════
// Source 4: NursingSchoolsAlmanac.com — State-level nursing school listings
// ══════════════════════════════════════════════════════════════════════════════
//
// URL pattern: /articles/list-accredited-nursing-schools-{state-slug}
// Lists nursing schools with name, location, and program types.

const ALMANAC_BASE = 'https://www.nursingschoolsalmanac.com/articles/list-accredited-nursing-schools-';

const ALMANAC_STATE_SLUGS = {
  'AL': 'alabama', 'AK': 'alaska', 'AZ': 'arizona', 'AR': 'arkansas',
  'CA': 'california', 'CO': 'colorado', 'CT': 'connecticut', 'DE': 'delaware',
  'FL': 'florida', 'GA': 'georgia', 'HI': 'hawaii', 'ID': 'idaho',
  'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa', 'KS': 'kansas',
  'KY': 'kentucky', 'LA': 'louisiana', 'ME': 'maine', 'MD': 'maryland',
  'MA': 'massachusetts', 'MI': 'michigan', 'MN': 'minnesota', 'MS': 'mississippi',
  'MO': 'missouri', 'MT': 'montana', 'NE': 'nebraska', 'NV': 'nevada',
  'NH': 'new-hampshire', 'NJ': 'new-jersey', 'NM': 'new-mexico', 'NY': 'new-york',
  'NC': 'north-carolina', 'ND': 'north-dakota', 'OH': 'ohio', 'OK': 'oklahoma',
  'OR': 'oregon', 'PA': 'pennsylvania', 'RI': 'rhode-island', 'SC': 'south-carolina',
  'SD': 'south-dakota', 'TN': 'tennessee', 'TX': 'texas', 'UT': 'utah',
  'VT': 'vermont', 'VA': 'virginia', 'WA': 'washington', 'WV': 'west-virginia',
  'WI': 'wisconsin', 'WY': 'wyoming',
};

function parseAlmanacPage(html, stateCode) {
  const $ = cheerio.load(html);
  const leads = [];

  // The page lists schools in sections — look for school name headings
  // Each school has name, location, and list of programs
  $('h2, h3').each((_, heading) => {
    const text = $(heading).text().trim();

    // Skip navigation/section headings
    if (text.match(/^(Table of Contents|Related|Nursing Schools|About|Contact|Source|References|Looking for)/i)) return;
    if (text.length < 5 || text.length > 150) return;

    // Check if it looks like a school name (contains university/college/school/institute)
    if (!text.match(/(university|college|school|institute|academy|center|medical|health|nursing|tech)/i)) return;

    // Look for location info in the next sibling elements
    let city = '';
    const nextEl = $(heading).next();
    const nextText = nextEl.text().trim();

    // Try to extract city from text like "Location: City, ST" or "City, State"
    const locMatch = nextText.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*([A-Z]{2})/);
    if (locMatch) {
      city = locMatch[1];
    }

    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company: text,
      phone: '',
      city,
      state: stateCode,
      country: 'US',
      source: 'nursingschoolsalmanac',
      website: '',
      title: '',
      program_type: 'Nursing School',
      profile_url: `${ALMANAC_BASE}${ALMANAC_STATE_SLUGS[stateCode]}`,
    });
  });

  return leads;
}

async function scrapeNursingSchoolsAlmanac(isTest) {
  const leads = [];

  const states = isTest
    ? [['FL', 'florida'], ['NY', 'new-york']]
    : Object.entries(ALMANAC_STATE_SLUGS).map(([code, slug]) => [code, slug]);

  log(`\n=== NursingSchoolsAlmanac.com ===`);
  log(`States: ${states.length}`);

  for (const [stateCode, slug] of states) {
    const url = `${ALMANAC_BASE}${slug}`;

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`  [Almanac] ${stateCode}: HTTP ${statusCode}`);
        await randomDelay();
        continue;
      }

      const pageLeads = parseAlmanacPage(body, stateCode);
      leads.push(...pageLeads);
      log(`  [Almanac] ${stateCode}: ${pageLeads.length} schools found`);
    } catch (err) {
      log(`  [Almanac] ${stateCode}: ERROR - ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Almanac total: ${leads.length} schools`);
  return leads;
}


// ══════════════════════════════════════════════════════════════════════════════
// Main
// ══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const skipCCNE = args.includes('--skip-ccne');
  const skipBRN = args.includes('--skip-brn');
  const skipPrep = args.includes('--skip-prep');
  const skipAlmanac = args.includes('--skip-almanac');
  const ccneOnly = args.includes('--ccne-only');
  const prepOnly = args.includes('--prep-only');

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('='.repeat(70));
  log('NCLEX Prep & Nursing School Lead Scraper');
  log('='.repeat(70));
  log(`Mode: ${isTest ? 'TEST (limited states/pages)' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const seenKeys = new Set();
  const sourceStats = {};

  // Build source list based on flags
  const sources = [];
  if (!skipCCNE && !prepOnly) {
    sources.push({ name: 'CCNE Directory', fn: () => scrapeCCNE(isTest) });
  }
  if (!skipBRN && !prepOnly && !ccneOnly) {
    sources.push({ name: 'California BRN', fn: () => scrapeCaliforniaBRN(isTest) });
  }
  if (!skipPrep && !ccneOnly) {
    sources.push({ name: 'NCLEX Prep Companies', fn: () => scrapeNCLEXPrepCompanies(isTest) });
  }
  if (!skipAlmanac && !prepOnly && !ccneOnly) {
    sources.push({ name: 'NursingSchoolsAlmanac', fn: () => scrapeNursingSchoolsAlmanac(isTest) });
  }

  for (const source of sources) {
    try {
      const leads = await source.fn();

      let added = 0;
      for (const lead of leads) {
        const key = dedupKey(lead);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        allLeads.push(lead);
        added++;
      }
      sourceStats[source.name] = { total: leads.length, unique: added };
      log(`\n  >> ${source.name}: ${leads.length} found, ${added} unique added`);
    } catch (err) {
      log(`\n  ERROR in ${source.name}: ${err.message}`);
      sourceStats[source.name] = { total: 0, unique: 0, error: err.message };
    }

    // Save intermediate progress
    if (allLeads.length > 0) {
      writeCSV(allLeads, OUT_FILE);
    }
  }

  // Final write
  if (allLeads.length > 0) {
    writeCSV(allLeads, OUT_FILE);
  }

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const withCompany = allLeads.filter(l => l.company).length;

  log('');
  log('='.repeat(70));
  log('FINAL RESULTS');
  log('='.repeat(70));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With company name:  ${withCompany}`);
  log(`With contact name:  ${withName}`);
  log(`With email:         ${withEmail}`);
  log(`With phone:         ${withPhone}`);
  log(`With website:       ${withWebsite}`);
  log('');
  log('Source breakdown:');
  for (const [name, stats] of Object.entries(sourceStats)) {
    log(`  ${name}: ${stats.total} found, ${stats.unique} unique${stats.error ? ` (ERROR: ${stats.error})` : ''}`);
  }
  log('');
  log('By source:');
  const bySrc = {};
  for (const l of allLeads) {
    bySrc[l.source] = (bySrc[l.source] || 0) + 1;
  }
  for (const [src, count] of Object.entries(bySrc).sort((a, b) => b[1] - a[1])) {
    log(`  ${src}: ${count}`);
  }
  log('');
  log('By state (top 15):');
  const byState = {};
  for (const l of allLeads) {
    if (l.state) byState[l.state] = (byState[l.state] || 0) + 1;
  }
  const topStates = Object.entries(byState).sort((a, b) => b[1] - a[1]).slice(0, 15);
  for (const [st, count] of topStates) {
    log(`  ${st}: ${count}`);
  }
  log('');
  log(`Output: ${OUT_FILE}`);

  // Force exit to avoid dangling HTTP connection retries
  process.exit(0);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
