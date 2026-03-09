#!/usr/bin/env node
/**
 * UK SRA Immigration Solicitors Scraper
 *
 * Scrapes the SRA (Solicitors Regulation Authority) register at
 * https://www.sra.org.uk/consumers/register/ to find immigration law firms.
 *
 * Strategy:
 *   Phase 1: Search the SRA register with multiple immigration-related keywords
 *            to collect firm SRA numbers, names, and cities. Each search can
 *            return up to 1000 results (API cap). We use many specific terms
 *            to maximize coverage beyond the 67 firms returned by "immigration" alone.
 *
 *   Phase 2: Fetch each firm's detail page to extract:
 *            - Full address, phone, email, website
 *            - Named people (solicitors) at the firm
 *            - SRA regulatory status
 *
 *   Phase 3: Output CSV with one row per person (solicitor) at each firm,
 *            plus a firm-level row for firms where no individuals are listed.
 *
 * The SRA register is server-rendered HTML (no public REST API without a
 * subscription key). We parse HTML with Cheerio.
 *
 * Usage:
 *   node scripts/scrape-uk-sra-immigration.js                  # Full scrape
 *   node scripts/scrape-uk-sra-immigration.js --test            # Test mode (5 firms)
 *   node scripts/scrape-uk-sra-immigration.js --search-only     # Phase 1 only
 *   node scripts/scrape-uk-sra-immigration.js --max-details 50  # Limit detail fetches
 */

const https = require('https');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const BASE_URL = 'https://www.sra.org.uk';
const REGISTER_URL = `${BASE_URL}/consumers/register/`;
const DETAIL_URL = `${BASE_URL}/consumers/register/organisation/`;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-solicitors-sra.csv');
const DELAY_MIN = 1500;
const DELAY_MAX = 2500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 2;
const MAX_RESULTS_PER_SEARCH = 1000; // SRA caps at 1000

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

// Search terms that maximize coverage of immigration-related firms.
// Each term returns results where SRA name/description matches.
// We avoid compound terms like "immigration+solicitor" because "solicitor"
// matches nearly all firms (4600+), making results too broad.
const SEARCH_TERMS = [
  'immigration',
  'immigration lawyer',
  'immigration consultant',
  'immigration advice',
  'immigration barrister',
  'visa',
  'visa lawyer',
  'asylum',
  'refugee',
  'human rights',
  'right to remain',
  'sponsor licence',
  'tier 2',
  'work permit',
  'border',
  'leave to remain',
  'indefinite leave',
  'deportation',
  'nationality',
];

// Firms that are NOT immigration law firms (payment companies, government bodies, etc.)
const EXCLUDED_SRA_NUMBERS = new Set([
  '64973',    // Visa Europe — payment processing
  '8013208',  // Visa Europe Limited — payment processing
  '573134',   // Visa Middle East FZ-LLC — payment company
  '8002203',  // Visa Payments Limited — payment company
  '820317',   // Border to Coast Pensions Partnership Limited
  '665803',   // Border to Coast Pensions Partnership Ltd
  '833841',   // Surrey and Borders Partnership NHS Foundation Trust
  '611875',   // Upper Tribunal Immigration and Asylum Chamber (government)
]);

// UK counties to skip when parsing cities from addresses
const UK_COUNTIES = new Set([
  'middlesex', 'surrey', 'essex', 'kent', 'sussex', 'hampshire', 'berkshire',
  'hertfordshire', 'buckinghamshire', 'oxfordshire', 'cambridgeshire', 'norfolk',
  'suffolk', 'devon', 'dorset', 'somerset', 'wiltshire', 'gloucestershire',
  'warwickshire', 'staffordshire', 'lancashire', 'yorkshire', 'cheshire',
  'derbyshire', 'nottinghamshire', 'leicestershire', 'lincolnshire', 'shropshire',
  'herefordshire', 'worcestershire', 'northamptonshire', 'bedfordshire', 'cornwall',
  'cumbria', 'tyne and wear', 'merseyside', 'west midlands', 'greater manchester',
  'south yorkshire', 'west yorkshire', 'east sussex', 'west sussex',
  'north yorkshire', 'east riding of yorkshire', 'greater london',
]);

// Known UK cities for better extraction
const KNOWN_CITIES = new Set([
  'london', 'manchester', 'birmingham', 'leeds', 'liverpool', 'sheffield',
  'bristol', 'cardiff', 'edinburgh', 'glasgow', 'belfast', 'newcastle',
  'nottingham', 'leicester', 'coventry', 'bradford', 'stoke-on-trent',
  'wolverhampton', 'plymouth', 'derby', 'southampton', 'portsmouth',
  'oxford', 'cambridge', 'brighton', 'york', 'bath', 'exeter', 'norwich',
  'chester', 'lincoln', 'durham', 'carlisle', 'canterbury', 'winchester',
  'worcester', 'gloucester', 'sunderland', 'luton', 'reading', 'bolton',
  'bournemouth', 'blackburn', 'oldham', 'rochdale', 'blackpool', 'burnley',
  'harrow', 'ilford', 'croydon', 'ealing', 'barking', 'enfield', 'wembley',
  'slough', 'watford', 'woking', 'guildford', 'colchester', 'ipswich',
  'dewsbury', 'halifax', 'huddersfield', 'wakefield', 'crewe',
  'peterborough', 'northampton', 'swindon', 'cheltenham', 'bury',
  'stockport', 'warrington', 'wigan', 'doncaster', 'rotherham',
  'basildon', 'chelmsford', 'southend-on-sea', 'maidstone', 'crawley',
  'stevenage', 'hemel hempstead', 'st albans', 'bedford', 'milton keynes',
  'high wycombe', 'aylesbury', 'banbury', 'newbury',
  'wolverhampton', 'dudley', 'walsall', 'west bromwich', 'solihull',
  'stourbridge', 'halesowen', 'sutton coldfield', 'tamworth',
  'telford', 'shrewsbury', 'stafford', 'burton upon trent',
  'swansea', 'newport', 'wrexham', 'aberystwyth', 'bangor',
  'middlesbrough', 'darlington', 'hartlepool', 'stockton-on-tees',
  'scarborough', 'harrogate', 'ripon',
  'preston', 'lancaster', 'accrington', 'nelson', 'colne',
  'barrow-in-furness', 'kendal', 'penrith', 'workington',
  'grimsby', 'scunthorpe', 'boston', 'grantham', 'spalding',
  'norwich', 'great yarmouth', 'kings lynn', 'thetford',
  'bury st edmunds', 'lowestoft', 'felixstowe',
  'hereford', 'ross-on-wye', 'leominster',
  'taunton', 'yeovil', 'bridgwater', 'glastonbury', 'wells',
  'torquay', 'paignton', 'barnstaple', 'bideford', 'tiverton',
  'truro', 'falmouth', 'newquay', 'penzance', 'bodmin', 'st austell',
  'weymouth', 'dorchester', 'poole', 'christchurch', 'wimborne',
  'salisbury', 'devizes', 'chippenham', 'trowbridge', 'marlborough',
  'stroud', 'cirencester', 'tewkesbury',
  'leamington spa', 'nuneaton', 'rugby', 'stratford-upon-avon',
  'redditch', 'bromsgrove', 'kidderminster', 'evesham',
  'loughborough', 'melton mowbray', 'hinckley', 'coalville',
  'corby', 'kettering', 'wellingborough', 'daventry',
  'mansfield', 'newark', 'retford', 'worksop',
]);

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function getRandomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|\s|-)\S/g, c => c.toUpperCase())
    .replace(/\bLlp\b/g, 'LLP')
    .replace(/\bLtd\b/g, 'Ltd')
    .replace(/\bPlc\b/g, 'PLC');
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(leads, filePath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(filePath, header + '\n' + rows.join('\n') + '\n');
}

function extractDomain(website) {
  if (!website) return '';
  try {
    let url = website;
    if (!url.startsWith('http')) url = `https://${url}`;
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function cleanPhone(phone) {
  if (!phone) return '';
  let p = phone.replace(/[\s\-\(\)\.]/g, '');
  if (p.startsWith('0')) {
    p = '+44' + p.slice(1);
  } else if (!p.startsWith('+')) {
    p = '+44' + p;
  }
  return p;
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Remove titles and suffixes
  let name = fullName
    .replace(/^(Mr|Mrs|Ms|Miss|Dr|Prof|Professor|Sir|Dame|Lord|Lady|Rt Hon|Hon)\s+/i, '')
    .replace(/\s+(QC|KC|CBE|OBE|MBE|LLB|LLM|BA|MA|PhD|Esq|JP|FCIArb|FRICS)$/gi, '')
    .trim();

  const parts = name.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  return {
    first_name: titleCase(parts[0]),
    last_name: titleCase(parts.slice(1).join(' ')),
  };
}

function parseCityFromAddress(address) {
  if (!address) return '';

  const parts = address.split(',').map(p => p.trim());

  const skipPatterns = [
    /^england$/i, /^wales$/i, /^scotland$/i, /^northern ireland$/i,
    /^united kingdom$/i, /^uk$/i,
    /^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i, // UK postcode
  ];

  // First: look for known cities
  for (let i = 0; i < parts.length; i++) {
    if (KNOWN_CITIES.has(parts[i].toLowerCase())) {
      return titleCase(parts[i]);
    }
  }

  // Filter and pick best candidate
  const candidates = [];
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    if (skipPatterns.some(p => p.test(part))) continue;
    if (UK_COUNTIES.has(part.toLowerCase())) continue;
    if (/^\d+\s/.test(part) || /\d/.test(part)) continue;
    if (/\b(street|road|lane|way|drive|avenue|close|court|place|crescent|terrace|square|house|park|mill|gate|walk|row|precinct|floor|suite|unit|level|room|building|centre|center)\b/i.test(part)) continue;
    candidates.push(part);
  }

  if (candidates.length > 0) {
    return titleCase(candidates[0]);
  }

  return '';
}

// ── HTTP Client ──────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': getRandomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        return resolve(httpGet(redirectUrl, retries));
      }

      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });

    req.on('error', (err) => {
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(new Error('Request timed out'));
      }
    });

    req.end();
  });
}

// ── Phase 1: Search SRA Register ─────────────────────────────────────────────

/**
 * Search the SRA register for firms matching a search term.
 * Returns an array of { sraNumber, firmName, city, searchTerm }.
 */
async function searchSRA(searchTerm) {
  const encodedTerm = encodeURIComponent(searchTerm);
  const url = `${REGISTER_URL}?searchText=${encodedTerm}&searchBy=Organisation&numberOfResults=${MAX_RESULTS_PER_SEARCH}`;

  const { statusCode, body } = await httpGet(url);
  if (statusCode !== 200) {
    log(`    WARNING: Search returned status ${statusCode}`);
    return [];
  }

  const $ = cheerio.load(body);
  const firms = [];

  // Extract total count
  const countText = $('p.lead, h1.lead, .displayed-results-all').parent().text();
  const totalMatch = countText.match(/(\d+)\s+of\s+(\d+)\s+firm/);
  const shown = totalMatch ? totalMatch[1] : '?';
  const total = totalMatch ? totalMatch[2] : '?';

  // Each result has an onclick handler: goToOrgDetails(sraNumber)
  $('ul.lookup__search__result__list li a').each((_, el) => {
    const $a = $(el);
    const onclick = $a.attr('onclick') || '';
    const match = onclick.match(/goToOrgDetails\((\d+)\)/);
    if (!match) return;

    const sraNumber = match[1];
    if (EXCLUDED_SRA_NUMBERS.has(sraNumber)) return;

    const firmName = $a.find('h2').text().trim();
    const cityRaw = $a.find('.label__sm__block__location').text()
      .replace(/Head\s*office\s*/i, '')
      .replace(/in\s*/i, '')
      .trim();

    // Check if firm is closed or not regulated
    const statusText = $a.text();
    const isClosed = /firm has closed/i.test(statusText);
    const isNotRegulated = /not regulated/i.test(statusText);

    if (!isClosed) {
      firms.push({
        sraNumber,
        firmName,
        city: cityRaw,
        searchTerm,
        isNotRegulated,
      });
    }
  });

  log(`  "${searchTerm}": ${firms.length} active firms (${shown} of ${total} total)`);
  return firms;
}

// ── Phase 2: Fetch Firm Detail Pages ─────────────────────────────────────────

/**
 * Fetch the detail page for a firm and extract contact info + people.
 */
async function fetchFirmDetail(sraNumber) {
  const url = `${DETAIL_URL}?sraNumber=${sraNumber}`;
  const { statusCode, body } = await httpGet(url);

  if (statusCode !== 200) {
    log(`    WARNING: Detail page for ${sraNumber} returned ${statusCode}`);
    return null;
  }

  const $ = cheerio.load(body);
  const detail = {
    sraNumber,
    profileUrl: url,
  };

  // Firm name
  detail.firmName = $('h1.reg__detail__h1').text().trim();

  // Parse dt/dd details
  $('dl.reg__details .label__list__details, .label__list__details').each((_, el) => {
    const dt = $(el).find('dt').text().trim().toLowerCase();
    const dd = $(el).find('dd').text().trim();

    if (dt.includes('website')) {
      detail.website = dd;
    } else if (dt.includes('sra number')) {
      // Already have it
    } else if (dt.includes('type of firm')) {
      detail.firmType = dd;
    }
  });

  // Parse offices section
  // Structure: #collapseOffices > .result-icon-links > h4 + div.office*
  // Each div.office has: <small class="left"><i class="icon icon-X"></small>
  //                      <small class="right">value</small>
  const offices = [];
  $('#collapseOffices .result-icon-links').each((_, el) => {
    const office = {};

    // Office name/location
    office.title = $(el).find('h4').text().trim();

    // Parse each div.office within this block
    $(el).find('div.office, .office').each((_, officeDiv) => {
      const iconClass = $(officeDiv).find('i[class*="icon"]').attr('class') || '';
      const value = $(officeDiv).find('small.right, .right').text().trim();
      const linkHref = $(officeDiv).find('a').attr('href') || '';

      if (iconClass.includes('icon-location')) {
        // Address (also check for span.address-grey-text)
        office.address = $(officeDiv).find('.address-grey-text').text().trim() || value;
      } else if (iconClass.includes('icon-phone')) {
        office.phone = value;
      } else if (iconClass.includes('icon-desktop')) {
        office.website = linkHref || value;
      } else if (iconClass.includes('icon-envelope')) {
        if (linkHref && linkHref.startsWith('mailto:')) {
          office.email = linkHref.replace('mailto:', '');
        } else {
          office.email = value;
        }
      }
    });

    offices.push(office);
  });

  // Use head office for contact details
  if (offices.length > 0) {
    const head = offices[0];
    if (!detail.phone && head.phone) detail.phone = head.phone;
    if (!detail.email && head.email) detail.email = head.email;
    if (!detail.website && head.website) detail.website = head.website;
    if (head.address) detail.address = head.address;
  }

  // Parse people
  detail.people = [];
  $('.detail-person-item a').each((_, el) => {
    const $a = $(el);
    const name = $a.find('h2').text().trim();
    const role = $a.find('.label__sm__block__subline__positive').text().trim();
    const personHref = $a.attr('href') || '';

    if (name && !name.includes('Search') && !name.includes('filter')) {
      detail.people.push({
        name,
        role: role || 'Solicitor',
        profileUrl: personHref.startsWith('http') ? personHref : `${BASE_URL}${personHref}`,
      });
    }
  });

  return detail;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const searchOnly = args.includes('--search-only');

  // Parse --max-details flag
  let maxDetails = Infinity;
  const maxIdx = args.indexOf('--max-details');
  if (maxIdx >= 0 && args[maxIdx + 1]) {
    maxDetails = parseInt(args[maxIdx + 1], 10);
  }

  if (isTest) maxDetails = 5;

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  log('=== UK SRA Immigration Solicitors Scraper ===');
  log(`Test mode: ${isTest}`);
  log(`Search-only mode: ${searchOnly}`);
  log(`Max detail fetches: ${maxDetails === Infinity ? 'unlimited' : maxDetails}`);
  log(`Output: ${OUTPUT_FILE}`);

  // ═══════════════════════════════════════════════════════════════════════════
  // Phase 1: Collect firm SRA numbers from multiple search terms
  // ═══════════════════════════════════════════════════════════════════════════

  log('\n--- Phase 1: Searching SRA register ---');

  const firmsBySra = new Map(); // sraNumber → { sraNumber, firmName, city, ... }
  const searchTermsToUse = isTest ? SEARCH_TERMS.slice(0, 3) : SEARCH_TERMS;

  for (let i = 0; i < searchTermsToUse.length; i++) {
    const term = searchTermsToUse[i];
    try {
      const results = await searchSRA(term);
      let newCount = 0;
      for (const firm of results) {
        if (!firmsBySra.has(firm.sraNumber)) {
          firmsBySra.set(firm.sraNumber, firm);
          newCount++;
        }
      }
      log(`    ${newCount} new unique firms (running total: ${firmsBySra.size})`);
    } catch (err) {
      log(`  ERROR searching for "${term}": ${err.message}`);
    }

    // Polite delay between searches
    if (i < searchTermsToUse.length - 1) {
      await randomDelay();
    }
  }

  log(`\nPhase 1 complete: ${firmsBySra.size} unique active firms found`);

  if (firmsBySra.size === 0) {
    log('No firms found. Exiting.');
    return;
  }

  // If search-only mode, output firm-level CSV and exit
  if (searchOnly) {
    log('\n--search-only mode: creating firm-level CSV without detail pages');
    const leads = [];
    for (const firm of firmsBySra.values()) {
      leads.push({
        first_name: '',
        last_name: '',
        firm_name: titleCase(firm.firmName),
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: titleCase(firm.city),
        state: 'UK-EW',
        country: 'UK',
        niche: 'immigration',
        source: 'sra_api',
        profile_url: `${DETAIL_URL}?sraNumber=${firm.sraNumber}`,
      });
    }
    writeCSV(leads, OUTPUT_FILE);
    log(`Wrote ${leads.length} firm-level leads to ${OUTPUT_FILE}`);
    return;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Phase 2: Fetch detail pages for each firm
  // ═══════════════════════════════════════════════════════════════════════════

  const sraNumbers = [...firmsBySra.keys()];
  const totalToFetch = Math.min(sraNumbers.length, maxDetails);

  log(`\n--- Phase 2: Fetching detail pages (${totalToFetch} firms) ---`);

  const leads = [];
  let successCount = 0;
  let errorCount = 0;
  let emailCount = 0;
  let phoneCount = 0;
  let websiteCount = 0;
  let peopleCount = 0;
  let rateLimitCount = 0;

  for (let i = 0; i < totalToFetch; i++) {
    const sraNumber = sraNumbers[i];
    const searchFirm = firmsBySra.get(sraNumber);

    log(`  [${i + 1}/${totalToFetch}] ${searchFirm.firmName} (SRA: ${sraNumber})`);

    try {
      const detail = await fetchFirmDetail(sraNumber);

      if (!detail) {
        errorCount++;
        // Still create a minimal lead from search data
        leads.push({
          first_name: '',
          last_name: '',
          firm_name: titleCase(searchFirm.firmName),
          title: '',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: titleCase(searchFirm.city),
          state: 'UK-EW',
          country: 'UK',
          niche: 'immigration',
          source: 'sra_api',
          profile_url: `${DETAIL_URL}?sraNumber=${sraNumber}`,
        });
        continue;
      }

      successCount++;

      const firmName = titleCase(detail.firmName || searchFirm.firmName);
      const website = detail.website || '';
      const websiteUrl = website.startsWith('http') ? website : (website ? `https://${website}` : '');
      const domain = extractDomain(website);
      const phone = cleanPhone(detail.phone || '');
      const email = (detail.email || '').toLowerCase().trim();
      const city = parseCityFromAddress(detail.address) || titleCase(searchFirm.city);
      const profileUrl = detail.profileUrl;

      if (email) emailCount++;
      if (phone) phoneCount++;
      if (websiteUrl) websiteCount++;

      // If the firm has named people, create a lead per person
      if (detail.people && detail.people.length > 0) {
        const maxPeople = Math.min(detail.people.length, 20); // Cap at 20 people per firm
        for (let j = 0; j < maxPeople; j++) {
          const person = detail.people[j];
          const { first_name, last_name } = parseName(person.name);
          if (first_name || last_name) {
            leads.push({
              first_name,
              last_name,
              firm_name: firmName,
              title: person.role || 'Solicitor',
              email, // Firm-level email (individual emails not available from SRA)
              phone,
              website: websiteUrl,
              domain,
              city,
              state: 'UK-EW',
              country: 'UK',
              niche: 'immigration',
              source: 'sra_api',
              profile_url: person.profileUrl || profileUrl,
            });
            peopleCount++;
          }
        }
      } else {
        // No named people — create a firm-level lead
        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          title: '',
          email,
          phone,
          website: websiteUrl,
          domain,
          city,
          state: 'UK-EW',
          country: 'UK',
          niche: 'immigration',
          source: 'sra_api',
          profile_url: profileUrl,
        });
      }

      // Log summary for this firm
      const parts = [];
      if (email) parts.push(`email`);
      if (phone) parts.push(`phone`);
      if (websiteUrl) parts.push(`web`);
      parts.push(`${detail.people ? detail.people.length : 0} people`);
      log(`    -> ${city || '?'} | ${parts.join(', ')}`);

    } catch (err) {
      errorCount++;
      log(`    ERROR: ${err.message}`);

      // Create minimal lead from search data
      leads.push({
        first_name: '',
        last_name: '',
        firm_name: titleCase(searchFirm.firmName),
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: titleCase(searchFirm.city),
        state: 'UK-EW',
        country: 'UK',
        niche: 'immigration',
        source: 'sra_api',
        profile_url: `${DETAIL_URL}?sraNumber=${sraNumber}`,
      });
    }

    // Incremental save every 25 firms
    if ((i + 1) % 25 === 0 || i === totalToFetch - 1) {
      writeCSV(leads, OUTPUT_FILE);
      log(`  --- Saved ${leads.length} leads (${i + 1}/${totalToFetch} firms processed) ---`);
    }

    // Polite delay
    if (i < totalToFetch - 1) {
      await randomDelay();
    }
  }

  // Final save
  writeCSV(leads, OUTPUT_FILE);

  // ═══════════════════════════════════════════════════════════════════════════
  // Summary
  // ═══════════════════════════════════════════════════════════════════════════

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${leads.length}`);
  log(`  Firm-level leads: ${leads.filter(l => !l.first_name).length}`);
  log(`  Person leads: ${leads.filter(l => l.first_name).length} (${peopleCount} people)`);
  log(`Unique firms processed: ${successCount + errorCount}`);
  log(`  Successful: ${successCount}`);
  log(`  Errors: ${errorCount}`);
  log(`Firms with email: ${emailCount}`);
  log(`Firms with phone: ${phoneCount}`);
  log(`Firms with website: ${websiteCount}`);

  // City breakdown (top 15)
  const byCity = {};
  leads.forEach(l => {
    const c = l.city || 'Unknown';
    byCity[c] = (byCity[c] || 0) + 1;
  });
  log('\nTop cities:');
  Object.entries(byCity)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .forEach(([city, count]) => log(`  ${city}: ${count}`));

  log(`\nOutput: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
