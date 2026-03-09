#!/usr/bin/env node
/**
 * UK Extra Immigration Solicitors Scraper
 *
 * Scrapes two sources not covered by the existing OISC/IAA, SRA, ILPA, BSB,
 * or Law Society scrapers:
 *
 *   Source 1 — Solicitors.Guru  (~249 immigration firms)
 *     - Listing pages: firm name, phone, address, languages
 *     - Detail pages:  SRA ID, practice areas, additional info
 *     - Paginated: 10 firms/page, ~25 pages
 *     - URL: https://solicitors.guru/immigration-solicitors/?page=N
 *
 *   Source 2 — SASS Wales Legal Aid List  (~120 firms)
 *     - Single HTML page with structured entries
 *     - Fields: firm name, address, phone, website, email
 *     - URL: https://sass.wales/legal-aid-solicitors-uk/
 *
 * Usage:
 *   node scripts/scrape-uk-extra-immigration.js           # Full scrape (both sources)
 *   node scripts/scrape-uk-extra-immigration.js --test     # Test mode (page 1 + 5 detail pages)
 *   node scripts/scrape-uk-extra-immigration.js --all      # Full scrape (alias)
 *   node scripts/scrape-uk-extra-immigration.js --guru     # Solicitors.Guru only
 *   node scripts/scrape-uk-extra-immigration.js --sass     # SASS Wales only
 *   node scripts/scrape-uk-extra-immigration.js --resume   # Resume from last saved state
 */

const https = require('https');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-extra.csv');
const RESUME_FILE = path.join(OUTPUT_DIR, '.uk-extra-immigration-resume.json');
const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 2;

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

// UK counties to skip when parsing cities
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
  'high wycombe', 'aylesbury', 'banbury', 'newbury', 'chatham',
  'dudley', 'walsall', 'west bromwich', 'solihull',
  'stourbridge', 'halesowen', 'sutton coldfield', 'tamworth',
  'telford', 'shrewsbury', 'stafford', 'burton upon trent',
  'swansea', 'newport', 'wrexham', 'aberystwyth', 'bangor',
  'middlesbrough', 'darlington', 'hartlepool', 'stockton-on-tees',
  'scarborough', 'harrogate', 'ripon',
  'preston', 'lancaster', 'accrington', 'nelson', 'colne',
  'barrow-in-furness', 'kendal', 'penrith', 'workington',
  'grimsby', 'scunthorpe', 'boston', 'grantham', 'spalding',
  'great yarmouth', 'kings lynn', 'thetford',
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
  'haringey', 'tottenham', 'stanmore', 'wimbledon', 'southall',
  'hounslow', 'hackney', 'stratford', 'woolwich', 'greenwich',
  'lewisham', 'brixton', 'tooting', 'wandsworth', 'hammersmith',
  'fulham', 'islington', 'camden', 'paddington', 'brent',
  'moseley', 'handsworth', 'atherton',
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
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase()
    .replace(/(?:^|\s|-)\S/g, c => c.toUpperCase())
    .replace(/\bLlp\b/g, 'LLP')
    .replace(/\bLtd\b/g, 'Ltd')
    .replace(/\bPlc\b/g, 'PLC')
    .replace(/\bLlc\b/g, 'LLC');
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
  // Strip common prefixes and labels
  let p = phone
    .replace(/^Tel:\s*/i, '')
    .replace(/^Phone:\s*/i, '')
    .replace(/^CALL\s*/i, '')
    .replace(/[\s\u00a0\-\(\)\.]/g, '');
  if (p.startsWith('0')) {
    p = '+44' + p.slice(1);
  } else if (p.startsWith('44') && !p.startsWith('+')) {
    p = '+' + p;
  } else if (!p.startsWith('+')) {
    p = '+44' + p;
  }
  // Validate: UK numbers should be 10-11 digits after country code
  const digits = p.replace(/\D/g, '');
  if (digits.length < 10 || digits.length > 13) return '';
  return p;
}

function parseCityFromAddress(address) {
  if (!address) return '';

  // Clean up nbsp and extra whitespace
  const cleanAddr = address.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
  const parts = cleanAddr.split(',').map(p => p.trim()).filter(Boolean);

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

  // Filter and pick best candidate (skip street addresses, postcodes, countries, counties)
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

function determineState(address) {
  if (!address) return 'UK-EW';
  const lower = address.toLowerCase();
  if (/\bwales\b/.test(lower) || /\bcardiff\b/.test(lower) || /\bswansea\b/.test(lower) ||
      /\bnewport\b/.test(lower) || /\bwrexham\b/.test(lower)) return 'UK-EW';
  if (/\bscotland\b/.test(lower) || /\bedinburgh\b/.test(lower) || /\bglasgow\b/.test(lower)) return 'UK-SC';
  if (/\bnorthern ireland\b/.test(lower) || /\bbelfast\b/.test(lower)) return 'UK-NI';
  return 'UK-EW';
}

function saveResume(state) {
  fs.writeFileSync(RESUME_FILE, JSON.stringify(state, null, 2));
}

function loadResume() {
  try {
    if (fs.existsSync(RESUME_FILE)) {
      return JSON.parse(fs.readFileSync(RESUME_FILE, 'utf8'));
    }
  } catch {}
  return null;
}

function clearResume() {
  try {
    if (fs.existsSync(RESUME_FILE)) fs.unlinkSync(RESUME_FILE);
  } catch {}
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
          : `https://${parsed.hostname}${res.headers.location}`;
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

// ═════════════════════════════════════════════════════════════════════════════
// Source 1: Solicitors.Guru
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Scrape a single listing page from Solicitors.Guru.
 * Returns array of { name, phone, address, detailPath }.
 */
function parseGuruListingPage($) {
  const firms = [];

  $('a.office').each((_, el) => {
    const $a = $(el);
    const name = $a.find('.name').text().trim();
    const detailPath = $a.attr('href') || '';

    // Extract address from the block containing "Address:"
    let address = '';
    $a.find('.block').each((__, block) => {
      const text = $(block).text().trim();
      if (text.startsWith('Address:')) {
        address = text.replace(/^Address:\s*/i, '').trim();
      }
    });

    // Extract phone from .tel block
    const phoneText = $a.find('.tel').text().replace(/^Tel:\s*/i, '').trim();

    if (name) {
      firms.push({ name, phone: phoneText, address, detailPath });
    }
  });

  return firms;
}

/**
 * Fetch a Solicitors.Guru detail page and extract SRA ID + extra info.
 */
async function fetchGuruDetail(detailPath) {
  const url = `https://solicitors.guru${detailPath}`;
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200) return null;

    const $ = cheerio.load(body);
    const detail = {};

    // Extract SRA ID
    const legalInfoText = $('.business-details').text();
    const sraMatch = legalInfoText.match(/SRA\s*ID:\s*(\d+)/i);
    if (sraMatch) detail.sraId = sraMatch[1];

    // Extract phone from tel: link if not already known
    const telLink = $('a[href^="tel:"]').first();
    if (telLink.length) {
      // href="tel:0161 225 0777" — extract just the number
      detail.phone = telLink.attr('href').replace(/^tel:\s*/i, '').trim();
    }

    return detail;
  } catch (err) {
    log(`    Detail page error: ${err.message}`);
    return null;
  }
}

/**
 * Scrape all Solicitors.Guru immigration listings.
 */
async function scrapeGuruSource(options = {}) {
  const { isTest, resumeState } = options;
  const leads = [];
  const seenFirms = new Set();

  // Determine start page
  let startPage = 1;
  if (resumeState && resumeState.guruPage) {
    startPage = resumeState.guruPage;
    log(`  Resuming from page ${startPage}`);
  }

  const maxPages = isTest ? 2 : 30; // Safety cap
  let totalFirmsFound = 0;
  let detailFetched = 0;
  const maxDetailFetches = isTest ? 5 : Infinity;

  log('\n--- Source 1: Solicitors.Guru ---');
  log(`  URL: https://solicitors.guru/immigration-solicitors/`);

  for (let page = startPage; page <= maxPages; page++) {
    const url = page === 1
      ? 'https://solicitors.guru/immigration-solicitors/'
      : `https://solicitors.guru/immigration-solicitors/?page=${page}`;

    log(`  Page ${page}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200) {
        log(`    WARNING: Page returned ${statusCode}`);
        break;
      }

      const $ = cheerio.load(body);
      const firms = parseGuruListingPage($);

      if (firms.length === 0) {
        log(`    No firms found — end of listings`);
        break;
      }

      log(`    Found ${firms.length} firms`);
      totalFirmsFound += firms.length;

      for (const firm of firms) {
        // Dedup by name (normalized)
        const key = firm.name.toLowerCase().replace(/\s+/g, ' ').trim();
        if (seenFirms.has(key)) continue;
        seenFirms.add(key);

        const city = parseCityFromAddress(firm.address);
        const phone = cleanPhone(firm.phone);
        const state = determineState(firm.address);
        const profileUrl = firm.detailPath
          ? `https://solicitors.guru${firm.detailPath}`
          : '';

        // Fetch detail page for SRA ID (rate limited)
        let sraId = '';
        let detailPhone = '';
        if (detailFetched < maxDetailFetches && firm.detailPath) {
          await randomDelay();
          const detail = await fetchGuruDetail(firm.detailPath);
          if (detail) {
            sraId = detail.sraId || '';
            detailPhone = detail.phone || '';
          }
          detailFetched++;
        }

        // Use listing phone, fall back to detail page phone
        const finalPhone = phone || cleanPhone(detailPhone);

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: titleCase(firm.name),
          title: sraId ? `SRA: ${sraId}` : '',
          email: '',
          phone: finalPhone,
          website: '',
          domain: '',
          city,
          state,
          country: 'UK',
          niche: 'immigration',
          source: 'solicitors_guru',
          profile_url: profileUrl,
        });
      }

      // Save resume state
      saveResume({ guruPage: page + 1, guruLeads: leads.length });

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    // Polite delay between pages
    await randomDelay();
  }

  log(`  Solicitors.Guru complete: ${leads.length} leads from ${totalFirmsFound} firms`);
  return leads;
}

// ═════════════════════════════════════════════════════════════════════════════
// Source 2: SASS Wales Legal Aid List
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Scrape the SASS Wales legal aid solicitors list.
 * Single-page, no pagination needed.
 */
async function scrapeSassSource(options = {}) {
  const { isTest } = options;
  const leads = [];

  log('\n--- Source 2: SASS Wales Legal Aid List ---');
  log('  URL: https://sass.wales/legal-aid-solicitors-uk/');

  try {
    const { statusCode, body } = await httpGet('https://sass.wales/legal-aid-solicitors-uk/');
    if (statusCode !== 200) {
      log(`  WARNING: Page returned ${statusCode}`);
      return leads;
    }

    const $ = cheerio.load(body);

    // The page structure is:
    //   <p class="wp-block-paragraph"><strong>Firm Name</strong></p>
    //   <ul class="wp-block-list">
    //     <li><strong>Address:</strong> ...</li>
    //     <li><strong>Phone:</strong> ...</li>
    //     <li><strong>Website:</strong> <a href="...">...</a></li>
    //     <li><strong>Email/Contact:</strong> <a href="mailto:...">...</a></li>
    //   </ul>
    //   <p>---separator---</p>

    const entries = [];
    let currentEntry = null;

    // Get all direct children of the content area
    const contentArea = $('.entry-content, .post-content, .wp-block-group').first();
    const elements = contentArea.length ? contentArea.children() : $('body').children();

    elements.each((_, el) => {
      const $el = $(el);
      const tagName = el.tagName;

      // Check for firm name: <p><strong>Name</strong></p>
      if (tagName === 'p') {
        const strongText = $el.find('strong').text().trim().replace(/\u00a0/g, ' ').replace(/\s+/g, ' ');
        const plainText = $el.text().trim();

        // Skip separators and meta text
        if (plainText.startsWith('———') || plainText.startsWith('---')) return;
        if (plainText.includes('This list of legal aid') || plainText.includes('Downloadable')) return;
        if (!strongText || strongText.length < 3) return;

        // Skip field labels that appear as standalone paragraphs
        if (/^(Address|Phone|Website|Email|Contact):/i.test(strongText)) return;

        // This is likely a firm name
        if (currentEntry && currentEntry.name) {
          entries.push(currentEntry);
        }
        currentEntry = { name: strongText };
        return;
      }

      // Check for details list: <ul class="wp-block-list">
      if (tagName === 'ul' && currentEntry) {
        $el.find('li').each((__, li) => {
          const $li = $(li);
          const text = $li.text().trim().replace(/\u00a0/g, ' ');

          if (/^Address:/i.test(text)) {
            currentEntry.address = text.replace(/^Address:\s*/i, '').trim();
          } else if (/^Phone:/i.test(text)) {
            currentEntry.phone = text.replace(/^Phone:\s*/i, '').trim();
          } else if (/^Website:/i.test(text)) {
            const link = $li.find('a').attr('href');
            currentEntry.website = link || text.replace(/^Website:\s*/i, '').trim();
          } else if (/^(Email|Contact)/i.test(text)) {
            const mailto = $li.find('a[href^="mailto:"]').attr('href');
            if (mailto) {
              currentEntry.email = mailto.replace('mailto:', '').trim().toLowerCase();
            } else {
              const emailMatch = text.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
              if (emailMatch) currentEntry.email = emailMatch[0].toLowerCase();
            }
          }
        });
      }
    });

    // Push last entry
    if (currentEntry && currentEntry.name) {
      entries.push(currentEntry);
    }

    log(`  Parsed ${entries.length} firm entries`);

    // Convert to leads
    const maxEntries = isTest ? 10 : entries.length;
    for (let i = 0; i < Math.min(maxEntries, entries.length); i++) {
      const entry = entries[i];
      const phone = cleanPhone(entry.phone || '');
      const website = entry.website || '';
      const domain = extractDomain(website);
      const email = entry.email || '';
      const city = parseCityFromAddress(entry.address || '');
      const state = determineState(entry.address || '');

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: titleCase(entry.name),
        title: '',
        email,
        phone,
        website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
        domain,
        city,
        state,
        country: 'UK',
        niche: 'immigration',
        source: 'sass_legal_aid',
        profile_url: 'https://sass.wales/legal-aid-solicitors-uk/',
      });
    }

    // Stats
    const withPhone = leads.filter(l => l.phone);
    const withEmail = leads.filter(l => l.email);
    const withWebsite = leads.filter(l => l.website);
    log(`  SASS complete: ${leads.length} leads`);
    log(`    With phone: ${withPhone.length}`);
    log(`    With email: ${withEmail.length}`);
    log(`    With website: ${withWebsite.length}`);

  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }

  return leads;
}

// ═════════════════════════════════════════════════════════════════════════════
// Deduplication
// ═════════════════════════════════════════════════════════════════════════════

function deduplicateLeads(leads) {
  const seen = new Map(); // key -> lead (keep the one with more data)

  for (const lead of leads) {
    const normName = (lead.firm_name || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    const normCity = (lead.city || '').toLowerCase().replace(/[^a-z]/g, '');
    const key = `${normName}|${normCity}`;

    if (seen.has(key)) {
      // Merge: keep the entry with more data
      const existing = seen.get(key);
      if (!existing.email && lead.email) existing.email = lead.email;
      if (!existing.phone && lead.phone) existing.phone = lead.phone;
      if (!existing.website && lead.website) existing.website = lead.website;
      if (!existing.domain && lead.domain) existing.domain = lead.domain;
      if (!existing.title && lead.title) existing.title = lead.title;
      // Merge source
      if (lead.source && !existing.source.includes(lead.source)) {
        existing.source += ';' + lead.source;
      }
    } else {
      seen.set(key, { ...lead });
    }
  }

  return [...seen.values()];
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const guruOnly = args.includes('--guru');
  const sassOnly = args.includes('--sass');
  const resume = args.includes('--resume');

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  log('=== UK Extra Immigration Solicitors Scraper ===');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Sources: ${guruOnly ? 'Solicitors.Guru only' : sassOnly ? 'SASS only' : 'ALL'}`);
  log(`Output: ${OUTPUT_FILE}`);

  let allLeads = [];
  const resumeState = resume ? loadResume() : null;
  if (resumeState) {
    log(`Resume state found: ${JSON.stringify(resumeState)}`);
  }

  // Source 1: Solicitors.Guru
  if (!sassOnly) {
    const guruLeads = await scrapeGuruSource({ isTest, resumeState });
    allLeads.push(...guruLeads);
  }

  // Source 2: SASS Wales Legal Aid List
  if (!guruOnly) {
    const sassLeads = await scrapeSassSource({ isTest });
    allLeads.push(...sassLeads);
  }

  // Deduplicate across sources
  log('\n--- Deduplication ---');
  const beforeDedup = allLeads.length;
  allLeads = deduplicateLeads(allLeads);
  const removed = beforeDedup - allLeads.length;
  log(`  Before: ${beforeDedup}, After: ${allLeads.length}, Removed: ${removed}`);

  // Write final CSV
  writeCSV(allLeads, OUTPUT_FILE);
  clearResume();

  // ═══════════════════════════════════════════════════════════════════════════
  // Summary
  // ═══════════════════════════════════════════════════════════════════════════

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${allLeads.length}`);

  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  log(`  With phone: ${withPhone}`);
  log(`  With email: ${withEmail}`);
  log(`  With website: ${withWebsite}`);

  // Source breakdown
  const bySource = {};
  allLeads.forEach(l => {
    const s = l.source || 'unknown';
    bySource[s] = (bySource[s] || 0) + 1;
  });
  log('\nBy source:');
  Object.entries(bySource)
    .sort((a, b) => b[1] - a[1])
    .forEach(([source, count]) => log(`  ${source}: ${count}`));

  // City breakdown (top 15)
  const byCity = {};
  allLeads.forEach(l => {
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
