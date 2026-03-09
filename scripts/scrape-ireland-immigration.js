#!/usr/bin/env node
/**
 * Ireland Immigration Solicitors & Consultants Scraper
 *
 * Multi-source scraper for Irish immigration solicitors and consultants.
 *
 * Sources:
 *   1. Golden Pages (goldenpages.ie) — JSON-LD structured data from
 *      immigration-law-solicitor and immigration-consultants categories.
 *      Scrapes both the national ("ireland") page and county-specific pages.
 *
 *   2. Known Firms — Curated list of confirmed immigration solicitors/firms
 *      gathered from web research with verified contact details from their
 *      own websites (Sinnott, Berkeley, IMK Law, Gibson, KOD Lyons, etc.)
 *
 * Strategy:
 *   Phase 1: Fetch Golden Pages category pages, extract JSON-LD business data
 *   Phase 2: Merge known firms (deduplicated by phone/domain/firm name)
 *   Phase 3: Filter out non-immigration businesses, output CSV
 *
 * Usage:
 *   node scripts/scrape-ireland-immigration.js --test        # Test mode (2 pages only)
 *   node scripts/scrape-ireland-immigration.js --all         # Full scrape
 *   node scripts/scrape-ireland-immigration.js               # Full scrape (default)
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'ireland-immigration-solicitors.csv');
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

// Golden Pages category URLs to scrape
const GOLDEN_PAGES_URLS = [
  // Immigration Law Solicitor — national + county pages
  'https://www.goldenpages.ie/immigration-law-solicitor/ireland/',
  'https://www.goldenpages.ie/immigration-law-solicitor/dublin-county/',
  'https://www.goldenpages.ie/immigration-law-solicitor/galway-county/',
  'https://www.goldenpages.ie/immigration-law-solicitor/limerick-county/',
  'https://www.goldenpages.ie/immigration-law-solicitor/kildare-county/',
  'https://www.goldenpages.ie/immigration-law-solicitor/louth-county/',
  'https://www.goldenpages.ie/immigration-law-solicitor/dublin-dublin-county/',
  // Immigration Consultants
  'https://www.goldenpages.ie/immigration-consultants/ireland/',
  'https://www.goldenpages.ie/immigration-consultants/dublin-county/',
];

// Non-immigration businesses to exclude (claims assessors, filing services, etc.)
const EXCLUDED_NAMES = new Set([
  'all things legal',
  'company filing assistance',
  'anz document solutions',
  'irish claims authority',
  'connect medicolegal experts ireland',
  'omc claims loss assessors',
  'the law almanac',
  'open forest',
  'holidayclaims.ie',
  'austin kenny mediation & conflict resolution',
  'aserve investigators',
  'immigration lawyer dublin', // Aggregator, not a real firm
  'immigration law solicitor', // Category name, not a business
  'immigration consultants', // Category name, not a business
  'goldgro limited',
  'company filing assistance',
]);

// Keywords that indicate a non-immigration business
const EXCLUDE_KEYWORDS = [
  'claims assessor', 'loss assessor', 'medicolegal', 'personal injury only',
  'conveyancing only', 'estate agent', 'auctioneers',
];

// ── Known Immigration Firms (from web research) ─────────────────────────────

const KNOWN_FIRMS = [
  {
    firm_name: 'Sinnott Solicitors',
    first_name: 'Carol', last_name: 'Sinnott',
    title: 'Principal Solicitor',
    phone: '+35314062862', email: 'info@sinnott.ie',
    website: 'https://sinnott.ie',
    city: 'Dublin', address: 'Church Avenue, Rathmines, Dublin 6',
    source: 'known_firm',
  },
  {
    firm_name: 'Sinnott Solicitors',
    first_name: 'Helen', last_name: 'Sheridan',
    title: 'Immigration Solicitor',
    phone: '+35314062862', email: 'info@sinnott.ie',
    website: 'https://sinnott.ie',
    city: 'Dublin', address: 'Church Avenue, Rathmines, Dublin 6',
    source: 'known_firm',
  },
  {
    firm_name: 'Sinnott Solicitors (Cork)',
    first_name: '', last_name: '',
    title: '',
    phone: '+353212028080', email: 'info@sinnott.ie',
    website: 'https://sinnott.ie',
    city: 'Cork', address: '14 Anglesea St, Cork City',
    source: 'known_firm',
  },
  {
    firm_name: 'Berkeley Solicitors',
    first_name: 'Karen', last_name: 'Berkeley',
    title: 'Founder / Solicitor',
    phone: '+35315175778', email: 'info@berkeleysolicitors.ie',
    website: 'https://berkeleysolicitors.ie',
    city: 'Dublin', address: 'Unit 3, Christchurch Hall, High Street, Dublin 8',
    source: 'known_firm',
  },
  {
    firm_name: 'IMK Law Solicitors',
    first_name: 'Imtiaz', last_name: 'Khan',
    title: 'Principal Solicitor',
    phone: '+35315575536', email: 'info@imklaw.ie',
    website: 'https://imk-law.com',
    city: 'Dublin', address: '75-76 Amiens Street, Mountjoy, Dublin 1',
    source: 'known_firm',
  },
  {
    firm_name: 'Gibson & Associates Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+35312645555', email: '',
    website: 'https://www.gibsonandassociates.ie',
    city: 'Dublin', address: 'Suite 238 The Capel Building, Mary\'s Abbey, Dublin 7',
    source: 'known_firm',
  },
  {
    firm_name: 'KOD Lyons Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+35316790780', email: 'info@kodlyons.ie',
    website: 'https://kodlyons.ie',
    city: 'Dublin', address: '31-33, Usher\'s Quay, Dublin 8',
    source: 'known_firm',
  },
  {
    firm_name: 'Sean O Toghda Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+353868561017', email: 'info@sotsolicitors.ie',
    website: 'https://sotsolicitors.ie',
    city: 'Dublin', address: '6-9 Trinity Street, Dublin 2',
    source: 'known_firm',
  },
  {
    firm_name: 'MS Solicitors',
    first_name: 'Olga', last_name: 'Shajaku',
    title: 'Principal Partner / Immigration Solicitor',
    phone: '+35316751747', email: 'info@ms-solicitors.ie',
    website: 'https://www.mssolicitors.ie',
    city: 'Dublin', address: "Unit 4, Isolde's Tower, Essex Quay, Dublin 8",
    source: 'known_firm',
  },
  {
    firm_name: 'RNL Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+35319011355', email: 'info@rnlsolicitors.ie',
    website: 'https://rnlsolicitors.ie',
    city: 'Dublin', address: 'Augustine House, Oliver Bond Street, Dublin 8',
    source: 'known_firm',
  },
  {
    firm_name: 'SMD Solicitors',
    first_name: 'Susan', last_name: 'Doyle',
    title: 'Solicitor',
    phone: '+353212390539', email: 'info@smdsolicitors.ie',
    website: 'https://smdsolicitors.ie',
    city: 'Cork', address: 'Unit 5, Village Green, Church Road, Douglas, Cork',
    source: 'known_firm',
  },
  {
    firm_name: 'SMD Solicitors (Dublin)',
    first_name: '', last_name: '',
    title: '',
    phone: '+35314003677', email: 'info@smdsolicitors.ie',
    website: 'https://smdsolicitors.ie',
    city: 'Dublin', address: '20 Harcourt St, Saint Kevin\'s, Dublin 2',
    source: 'known_firm',
  },
  {
    firm_name: 'Walsh & Partners Solicitors LLP',
    first_name: '', last_name: '',
    title: '',
    phone: '+353214270200', email: 'cork@walshandpartners.ie',
    website: 'https://walshandpartners.ie',
    city: 'Cork', address: '17 South Mall, Cork',
    source: 'known_firm',
  },
  {
    firm_name: 'Walsh & Partners Solicitors LLP (Dublin)',
    first_name: '', last_name: '',
    title: '',
    phone: '+35312910300', email: 'dublin@walshandpartners.ie',
    website: 'https://walshandpartners.ie',
    city: 'Dublin', address: '1st Floor 7E, Nutgrove Office Park, Rathfarnham, Dublin 14',
    source: 'known_firm',
  },
  {
    firm_name: 'Walsh & Partners Solicitors LLP (Midleton)',
    first_name: '', last_name: '',
    title: '',
    phone: '+353214639425', email: 'reception@walshandpartners.ie',
    website: 'https://walshandpartners.ie',
    city: 'Midleton', address: '88 Main Street, Midleton, Co. Cork',
    source: 'known_firm',
  },
  {
    firm_name: 'Joyce & Co Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+353214270391', email: 'info@joycecosolicitors.com',
    website: 'https://joycecosolicitors.ie',
    city: 'Cork', address: '9 Washington Street West, Cork',
    source: 'known_firm',
  },
  {
    firm_name: 'Joyce & Co Solicitors (Limerick)',
    first_name: '', last_name: '',
    title: '',
    phone: '+35361513374', email: 'info@joycecosolicitors.com',
    website: 'https://joycecosolicitors.ie',
    city: 'Limerick', address: '13 Bedford Row, Limerick',
    source: 'known_firm',
  },
  {
    firm_name: 'Immigration Advice Service (IAS Ireland)',
    first_name: '', last_name: '',
    title: '',
    phone: '+35361518025', email: 'info@iasservices.org.uk',
    website: 'https://ie.iasservices.org.uk',
    city: 'Limerick', address: '21 Mallow Street, Limerick',
    source: 'known_firm',
  },
  {
    firm_name: 'FSK Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+353872588842', email: '',
    website: 'https://fsksolicitors.com',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Louise Corrigan Law',
    first_name: 'Louise', last_name: 'Corrigan',
    title: 'Immigration Lawyer',
    phone: '', email: '',
    website: 'https://www.louisecorriganlaw.com',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Stanley & Company',
    first_name: '', last_name: '',
    title: '',
    phone: '', email: '',
    website: 'https://www.stanleyandco.ie',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Immigration Solicitor Dublin',
    first_name: '', last_name: '',
    title: '',
    phone: '+35312338436', email: '',
    website: 'https://www.immigrationsolicitorindublin.com',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Immigrant Council of Ireland',
    first_name: '', last_name: '',
    title: '',
    phone: '+35316740202', email: 'admin@immigrantcouncil.ie',
    website: 'https://www.immigrantcouncil.ie',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Poe Kiely Hogan Lanigan Solicitors',
    first_name: 'Aileen', last_name: 'Gittens',
    title: 'Head of Immigration Team',
    phone: '', email: '',
    website: 'https://www.pkhl.ie',
    city: 'Kilkenny', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Lalloo Solicitors',
    first_name: '', last_name: '',
    title: '',
    phone: '+35316641800', email: '',
    website: 'https://www.lalloosolicitors.ie',
    city: 'Dublin', address: 'Alexandra House, 3 Ballsbridge Park, Ballsbridge, Dublin 4',
    source: 'known_firm',
  },
  {
    firm_name: 'Tully Legal',
    first_name: '', last_name: '',
    title: '',
    phone: '+35319637000', email: '',
    website: 'https://tullylegal.ie',
    city: 'Dublin', address: '',
    source: 'known_firm',
  },
  {
    firm_name: 'Adams Law',
    first_name: '', last_name: '',
    title: '',
    phone: '+35316789774', email: '',
    website: 'https://adamslaw.ie',
    city: 'Dublin', address: '13 Herbert Street, Dublin 2',
    source: 'known_firm',
  },
];

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
  // Remove formatting chars
  let p = phone.replace(/[\s\-\(\)\.]/g, '');
  // Remove leading country code variations
  if (p.startsWith('00353')) {
    p = '+353' + p.slice(5);
  } else if (p.startsWith('353') && p.length > 10) {
    p = '+353' + p.slice(3);
  } else if (p.startsWith('0') && !p.startsWith('00')) {
    p = '+353' + p.slice(1);
  } else if (!p.startsWith('+')) {
    p = '+353' + p;
  }
  // Remove double zeros that might remain
  p = p.replace(/^\+353\+/, '+353');
  return p;
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Remove titles and suffixes
  let name = fullName
    .replace(/^(Mr|Mrs|Ms|Miss|Dr|Prof|Professor|Sir|Dame|Rt Hon|Hon)\s+/i, '')
    .replace(/\s+(SC|BL|QC|KC|CBE|OBE|MBE|LLB|LLM|BA|MA|PhD|Esq|JP|BCL|BBLS)$/gi, '')
    .trim();

  const parts = name.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  return {
    first_name: titleCase(parts[0]),
    last_name: titleCase(parts.slice(1).join(' ')),
  };
}

function isImmigrationRelevant(biz) {
  // Check if a Golden Pages business is actually immigration-related
  const name = (biz.name || '').toLowerCase();
  const desc = (biz.description || '').toLowerCase();

  // Exclude known non-immigration businesses
  if (EXCLUDED_NAMES.has(name)) return false;

  // Exclude by keywords
  for (const kw of EXCLUDE_KEYWORDS) {
    if (desc.includes(kw) && !desc.includes('immigration')) return false;
  }

  // Accept if description mentions immigration-related terms
  const immigrationTerms = [
    'immigration', 'visa', 'asylum', 'refugee', 'citizenship',
    'naturalisation', 'naturalization', 'work permit', 'employment permit',
    'deportation', 'residency', 'international protection', 'leave to remain',
    'family reunification', 'stamp 4', 'stamp 1', 'inis', 'gnib',
  ];

  const hasImmigrationTerm = immigrationTerms.some(t => desc.includes(t) || name.includes(t));

  // If business is from immigration-law-solicitor category, accept broadly
  // (Golden Pages already categorized them). But filter out obvious non-matches.
  if (!hasImmigrationTerm) {
    // Check if it's a general solicitor that might do immigration
    const solicitorTerms = ['solicitor', 'lawyer', 'law firm', 'legal', 'attorney', 'barrister'];
    const isSolicitor = solicitorTerms.some(t => name.includes(t) || desc.includes(t));
    if (!isSolicitor) return false;
  }

  return true;
}

// ── HTTP Client ──────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const proto = parsed.protocol === 'https:' ? https : http;

    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': getRandomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-IE,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = proto.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
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

// ── Phase 1: Golden Pages JSON-LD Scraping ──────────────────────────────────

/**
 * Fetch a Golden Pages category page and extract businesses from JSON-LD.
 */
async function scrapeGoldenPages(url) {
  log(`  Fetching: ${url}`);
  const { statusCode, body } = await httpGet(url);

  if (statusCode !== 200) {
    log(`    WARNING: Status ${statusCode} for ${url}`);
    return [];
  }

  const $ = cheerio.load(body);
  const businesses = [];

  // Parse JSON-LD scripts
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const data = JSON.parse($(el).text());
      if (!Array.isArray(data)) return;

      for (const item of data) {
        if (!item.itemListElement) continue;

        for (const li of item.itemListElement) {
          const biz = li.item || li;
          if (!biz.name) continue;

          businesses.push({
            name: biz.name,
            description: biz.description || '',
            phone: biz.telephone || '',
            website: biz.url || '',
            address: biz.address || {},
            geo: biz.geo || {},
          });
        }
      }
    } catch (e) {
      // Skip malformed JSON-LD
    }
  });

  log(`    Found ${businesses.length} businesses`);
  return businesses;
}

/**
 * Convert a Golden Pages business object into a lead.
 */
// Normalize Irish city names
function normalizeCity(city) {
  if (!city) return '';
  const c = city.trim();
  const map = {
    'galway city centre': 'Galway',
    'galway city': 'Galway',
    'cork city': 'Cork',
    'limerick city': 'Limerick',
    'dublin city': 'Dublin',
    'dublin 1': 'Dublin',
    'dublin 2': 'Dublin',
    'dublin 4': 'Dublin',
    'dublin 6': 'Dublin',
    'dublin 7': 'Dublin',
    'dublin 8': 'Dublin',
    'dublin 9': 'Dublin',
    'dublin 10': 'Dublin',
    'dublin 14': 'Dublin',
    'dublin 15': 'Dublin',
    'dublin 18': 'Dublin',
    'four courts': 'Dublin',
    'harold\'s cross': 'Dublin',
    'harolds cross': 'Dublin',
    'ballsbridge': 'Dublin',
    'smithfield': 'Dublin',
    'tallaght': 'Dublin',
    'blanchardstown': 'Dublin',
    'sandyford': 'Dublin',
    'santry': 'Dublin',
    'dundrum': 'Dublin',
    'rathfarnham': 'Dublin',
    'rathmines': 'Dublin',
    'drumcondra': 'Dublin',
    'rathgar': 'Dublin',
    'swords': 'Dublin',
    'balbriggan': 'Dublin',
    'lucan': 'Dublin',
    'dun laoghaire': 'Dublin',
    'bluebell': 'Dublin',
    'clondalkin': 'Dublin',
    'ringsend': 'Dublin',
    'inchicore': 'Dublin',
    'ballintemple': 'Cork',
    'douglas': 'Cork',
    'little island': 'Cork',
    'midleton': 'Cork',
  };
  const normalized = map[c.toLowerCase()];
  if (normalized) return normalized;
  return titleCase(c);
}

function gpBusinessToLead(biz) {
  const addr = biz.address || {};
  const rawCity = addr.addressLocality || addr.addressRegion || '';
  const county = addr.addressRegion || '';

  let website = biz.website || '';
  // Golden Pages internal URLs are not real websites
  if (website.includes('goldenpages.ie')) {
    website = '';
  }

  const city = normalizeCity(rawCity) || normalizeCity(county);

  return {
    first_name: '',
    last_name: '',
    firm_name: biz.name || '',
    title: '',
    email: '',
    phone: cleanPhone(biz.phone || ''),
    website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
    domain: extractDomain(website),
    city,
    state: 'IE',
    country: 'Ireland',
    niche: 'immigration',
    source: 'goldenpages_ie',
    profile_url: '',
  };
}

// ── Phase 2: Known Firms ────────────────────────────────────────────────────

function knownFirmToLead(firm) {
  const website = firm.website || '';
  return {
    first_name: firm.first_name || '',
    last_name: firm.last_name || '',
    firm_name: firm.firm_name || '',
    title: firm.title || '',
    email: (firm.email || '').toLowerCase().trim(),
    phone: cleanPhone(firm.phone || ''),
    website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
    domain: extractDomain(website),
    city: firm.city || '',
    state: 'IE',
    country: 'Ireland',
    niche: 'immigration',
    source: firm.source || 'known_firm',
    profile_url: '',
  };
}

// ── Deduplication ───────────────────────────────────────────────────────────

function dedup(leads) {
  const seen = new Map(); // key → lead
  const result = [];

  for (const lead of leads) {
    // Build dedup keys
    const keys = [];

    // Key 1: Phone number (strongest)
    if (lead.phone && lead.phone.length > 6) {
      keys.push(`phone:${lead.phone}`);
    }

    // Key 2: Domain
    if (lead.domain) {
      keys.push(`domain:${lead.domain}`);
    }

    // Key 3: Firm name (normalized) + city
    if (lead.firm_name) {
      const normName = lead.firm_name.toLowerCase()
        .replace(/[^a-z0-9]/g, '')
        .replace(/(solicitors?|llp|ltd|limited|law|associates?|co|company)/g, '');
      if (normName.length > 2) {
        keys.push(`name:${normName}:${(lead.city || '').toLowerCase()}`);
      }
    }

    let isDup = false;
    for (const key of keys) {
      if (seen.has(key)) {
        // Merge: prefer the record with more data
        const existing = seen.get(key);
        if (!existing.email && lead.email) existing.email = lead.email;
        if (!existing.phone && lead.phone) existing.phone = lead.phone;
        if (!existing.website && lead.website) existing.website = lead.website;
        if (!existing.domain && lead.domain) existing.domain = lead.domain;
        if (!existing.first_name && lead.first_name) existing.first_name = lead.first_name;
        if (!existing.last_name && lead.last_name) existing.last_name = lead.last_name;
        if (!existing.title && lead.title) existing.title = lead.title;
        if (!existing.city && lead.city) existing.city = lead.city;
        isDup = true;
        break;
      }
    }

    if (!isDup) {
      result.push(lead);
      for (const key of keys) {
        seen.set(key, lead);
      }
    }
  }

  return result;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  log('=== Ireland Immigration Solicitors & Consultants Scraper ===');
  log(`Test mode: ${isTest}`);
  log(`Output: ${OUTPUT_FILE}`);

  const allLeads = [];

  // ═══════════════════════════════════════════════════════════════════════════
  // Phase 1: Golden Pages
  // ═══════════════════════════════════════════════════════════════════════════

  log('\n--- Phase 1: Golden Pages (JSON-LD) ---');

  const urlsToScrape = isTest ? GOLDEN_PAGES_URLS.slice(0, 2) : GOLDEN_PAGES_URLS;
  let gpTotal = 0;
  let gpAccepted = 0;

  for (let i = 0; i < urlsToScrape.length; i++) {
    const url = urlsToScrape[i];
    try {
      const businesses = await scrapeGoldenPages(url);
      gpTotal += businesses.length;

      for (const biz of businesses) {
        if (isImmigrationRelevant(biz)) {
          const lead = gpBusinessToLead(biz);
          if (lead.firm_name) {
            allLeads.push(lead);
            gpAccepted++;
          }
        }
      }
    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    // Polite delay between requests
    if (i < urlsToScrape.length - 1) {
      await randomDelay();
    }
  }

  log(`\nGolden Pages: ${gpTotal} businesses found, ${gpAccepted} immigration-relevant`);

  // ═══════════════════════════════════════════════════════════════════════════
  // Phase 2: Known Firms
  // ═══════════════════════════════════════════════════════════════════════════

  log('\n--- Phase 2: Known Immigration Firms ---');

  for (const firm of KNOWN_FIRMS) {
    allLeads.push(knownFirmToLead(firm));
  }

  log(`Added ${KNOWN_FIRMS.length} known firms`);

  // ═══════════════════════════════════════════════════════════════════════════
  // Phase 3: Dedup & Output
  // ═══════════════════════════════════════════════════════════════════════════

  log('\n--- Phase 3: Dedup & Output ---');

  const beforeDedup = allLeads.length;
  const leads = dedup(allLeads);
  log(`Before dedup: ${beforeDedup}, after dedup: ${leads.length}`);

  // Write CSV
  writeCSV(leads, OUTPUT_FILE);

  // ═══════════════════════════════════════════════════════════════════════════
  // Summary
  // ═══════════════════════════════════════════════════════════════════════════

  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;
  const withWebsite = leads.filter(l => l.website).length;
  const withName = leads.filter(l => l.first_name).length;

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${leads.length}`);
  log(`  With email: ${withEmail}`);
  log(`  With phone: ${withPhone}`);
  log(`  With website: ${withWebsite}`);
  log(`  With person name: ${withName}`);

  // City breakdown
  const byCity = {};
  leads.forEach(l => {
    const c = l.city || 'Unknown';
    byCity[c] = (byCity[c] || 0) + 1;
  });
  log('\nBy city:');
  Object.entries(byCity)
    .sort((a, b) => b[1] - a[1])
    .forEach(([city, count]) => log(`  ${city}: ${count}`));

  // Source breakdown
  const bySource = {};
  leads.forEach(l => {
    bySource[l.source] = (bySource[l.source] || 0) + 1;
  });
  log('\nBy source:');
  Object.entries(bySource)
    .sort((a, b) => b[1] - a[1])
    .forEach(([source, count]) => log(`  ${source}: ${count}`));

  log(`\nOutput: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
