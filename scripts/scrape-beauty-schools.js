#!/usr/bin/env node
/**
 * Beauty / Cosmetology Schools Scraper
 *
 * Scrapes accredited beauty and cosmetology schools from multiple sources:
 *
 * 1. BeautySchoolsDirectory.com — State-by-state listings
 *    - ~3,000+ schools nationwide
 *    - URL pattern: /programs/cosmetology-school/{state-abbrev}
 *    - Data: name, address, phone, programs, accreditation
 *    - Individual school pages have website URLs
 *
 * 2. BeautySchools.com — State-by-state listings
 *    - ~1,900+ schools
 *    - URL pattern: /states/{state-name}/
 *    - Data: name, city, programs, accreditation
 *
 * 3. AACS (American Association of Cosmetology Schools) — Member directory
 *    - ~500+ member schools
 *    - URL: members.myaacs.org/school-directory (SPA — requires GrowthZone API)
 *    - Data: name, address, phone, email, website
 *    - Seeded manually since it's a JS-rendered SPA
 *
 * 4. State Board PDFs (seeded data):
 *    - California BBC approved_school_list.pdf (~200+ schools)
 *    - Virginia DPOR cosmetology schools.pdf
 *    - New Jersey Consumer Affairs school list.pdf
 *    - Texas TDLR data.texas.gov CSV (1000+ incl. vocational)
 *
 * 5. NACCAS — National Accrediting Commission (~1,300 schools)
 *    - ASP.NET WebForms portal, not easily scraped without Puppeteer
 *    - Seeded from known NACCAS-accredited schools
 *
 * Strategy:
 *   - Scrape BeautySchoolsDirectory.com state pages (best coverage, phone+address)
 *   - Supplement with BeautySchools.com for additional schools
 *   - Seed known AACS members, state board lists, and NACCAS schools
 *   - Dedup by school name + city
 *   - Output: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Zero npm dependencies — uses only Node.js built-in modules.
 *
 * Usage:
 *   node scripts/scrape-beauty-schools.js              # full scrape (all 50 states)
 *   node scripts/scrape-beauty-schools.js --test       # 3 states only
 *   node scripts/scrape-beauty-schools.js --delay=3000 # custom delay
 *   node scripts/scrape-beauty-schools.js --states=CA,TX,NY  # specific states
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 2500;
const TEST_MODE = !!args.test;
const SPECIFIC_STATES = args.states ? args.states.split(',').map(s => s.trim().toUpperCase()) : null;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'beauty-cosmetology-schools.csv');
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';

// ── All US states ───────────────────────────────────────────────────────
const US_STATES = {
  'AL': 'alabama', 'AK': 'alaska', 'AZ': 'arizona', 'AR': 'arkansas',
  'CA': 'california', 'CO': 'colorado', 'CT': 'connecticut', 'DE': 'delaware',
  'DC': 'district-of-columbia', 'FL': 'florida', 'GA': 'georgia', 'HI': 'hawaii',
  'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa',
  'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana', 'ME': 'maine',
  'MD': 'maryland', 'MA': 'massachusetts', 'MI': 'michigan', 'MN': 'minnesota',
  'MS': 'mississippi', 'MO': 'missouri', 'MT': 'montana', 'NE': 'nebraska',
  'NV': 'nevada', 'NH': 'new-hampshire', 'NJ': 'new-jersey', 'NM': 'new-mexico',
  'NY': 'new-york', 'NC': 'north-carolina', 'ND': 'north-dakota', 'OH': 'ohio',
  'OK': 'oklahoma', 'OR': 'oregon', 'PA': 'pennsylvania', 'RI': 'rhode-island',
  'SC': 'south-carolina', 'SD': 'south-dakota', 'TN': 'tennessee', 'TX': 'texas',
  'UT': 'utah', 'VT': 'vermont', 'VA': 'virginia', 'WA': 'washington',
  'WV': 'west-virginia', 'WI': 'wisconsin', 'WY': 'wyoming',
};

// State abbrev → full name for beautyschools.com URL pattern
const STATE_NAMES_LOWER = {};
for (const [abbr, name] of Object.entries(US_STATES)) {
  STATE_NAMES_LOWER[abbr] = name;
}

// ── HTTP helpers ────────────────────────────────────────────────────────
function httpGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    if (maxRedirects <= 0) return reject(new Error(`Too many redirects for ${url}`));
    const mod = url.startsWith('https') ? https : http;
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
    };
    const req = mod.request(options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsedUrl.protocol}//${parsedUrl.hostname}${res.headers.location}`;
        res.resume();
        httpGet(redirectUrl, maxRedirects - 1).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function escapeCSV(val) {
  const s = (val || '').toString().trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function normalizeCompany(name) {
  return (name || '').trim()
    .replace(/\s+/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#039;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&#\d+;/g, '')
    .replace(/\s*–\s*Accredited\s*$/i, '')
    .replace(/\s*-\s*Accredited\s*$/i, '')
    .trim();
}

function normalizePhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-.\s]/g, '').trim();
}

function decodeHtml(html) {
  return (html || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&nbsp;/g, ' ');
}

// ── Source 1: BeautySchoolsDirectory.com ────────────────────────────────
async function scrapeBeautySchoolsDirectory(stateCodes) {
  console.log('\n--- Source 1: BeautySchoolsDirectory.com ---');
  const leads = [];
  let statesDone = 0;

  for (const stateCode of stateCodes) {
    const url = `https://www.beautyschoolsdirectory.com/programs/cosmetology-school/${stateCode.toLowerCase()}`;
    statesDone++;
    console.log(`  [${statesDone}/${stateCodes.length}] Fetching ${stateCode}...`);

    try {
      const html = await httpGet(url);

      // Parse school entries from the HTML
      // Pattern: school entries are in heading tags (h3/h4) with links, followed by address and phone
      // School name: <h4><a href="/schools/...">School Name</a> – Accredited</h4>
      // Address: text after heading containing city, state ZIP
      // Phone: (xxx) xxx-xxxx pattern

      // Strategy 1: Find all school links with /schools/ pattern
      const schoolLinkRegex = /<a[^>]*href="\/schools\/([^"]+)"[^>]*>([^<]+)<\/a>/gi;
      const phoneRegex = /\((\d{3})\)\s*(\d{3})[- ](\d{4})/g;

      // Collect all school name+link occurrences
      const schoolEntries = [];
      let match;
      while ((match = schoolLinkRegex.exec(html)) !== null) {
        const slug = match[1];
        const name = decodeHtml(match[2]).trim();
        // Filter out generic navigation links
        if (name.length < 3) continue;
        if (/show more|view all|see all|sponsored|advertisement/i.test(name)) continue;

        schoolEntries.push({
          name: name,
          slug: slug,
          index: match.index,
        });
      }

      // For each school entry, look for phone and address nearby (within next 500 chars)
      for (const entry of schoolEntries) {
        const afterText = html.substring(entry.index, entry.index + 800);

        // Extract phone
        let phone = '';
        const phoneMatch = afterText.match(/\((\d{3})\)\s*(\d{3})[- ](\d{4})/);
        if (phoneMatch) {
          phone = phoneMatch[0];
        }

        // Extract city, state, ZIP from address text
        let city = '';
        // Look for pattern: City, ST ZIP or City, ST
        const stripHtml = afterText.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
        const cityStateMatch = stripHtml.match(/([A-Za-z\s.'-]+),\s*([A-Z]{2})\s*(\d{5})?/);
        if (cityStateMatch) {
          city = cityStateMatch[1].trim();
          // Clean up city — remove leading junk
          city = city.replace(/^[^A-Za-z]+/, '').trim();
          // If city is too long or looks like a sentence, skip it
          if (city.length > 40) city = '';
        }

        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company: normalizeCompany(entry.name),
          phone: normalizePhone(phone),
          city: city,
          state: stateCode,
          country: 'US',
          source: 'beautyschoolsdirectory',
        });
      }

      console.log(`    -> ${schoolEntries.length} schools`);
    } catch (err) {
      console.log(`    [ERR] ${stateCode}: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  console.log(`  Total from BeautySchoolsDirectory: ${leads.length}`);
  return leads;
}

// ── Source 2: BeautySchools.com ─────────────────────────────────────────
async function scrapeBeautySchoolsCom(stateCodes) {
  console.log('\n--- Source 2: BeautySchools.com ---');
  const leads = [];
  let statesDone = 0;

  for (const stateCode of stateCodes) {
    const stateName = STATE_NAMES_LOWER[stateCode];
    if (!stateName) continue;

    const url = `https://www.beautyschools.com/states/${stateName}/`;
    statesDone++;
    console.log(`  [${statesDone}/${stateCodes.length}] Fetching ${stateCode}...`);

    try {
      const html = await httpGet(url);

      // Parse school entries
      // Pattern: school names as links with /schools/ path, followed by city
      const schoolLinkRegex = /<a[^>]*href="\/schools\/([^"]+)"[^>]*>([^<]+)<\/a>/gi;
      const entries = [];
      let match;

      while ((match = schoolLinkRegex.exec(html)) !== null) {
        const name = decodeHtml(match[2]).trim();
        if (name.length < 3) continue;
        if (/show more|view all|sponsored|advertisement|find school/i.test(name)) continue;

        const afterText = html.substring(match.index, match.index + 500)
          .replace(/<[^>]+>/g, ' ')
          .replace(/\s+/g, ' ');

        // Try to extract city from text after school name
        let city = '';
        const cityMatch = afterText.match(/([A-Za-z\s.'-]+),\s*[A-Z]{2}/);
        if (cityMatch) {
          city = cityMatch[1].trim();
          if (city.length > 40) city = '';
        }

        entries.push({
          name: name,
          city: city,
        });
      }

      // Dedup within state (same school listed multiple times on page)
      const seen = new Set();
      for (const entry of entries) {
        const key = entry.name.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);

        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company: normalizeCompany(entry.name),
          phone: '',
          city: entry.city,
          state: stateCode,
          country: 'US',
          source: 'beautyschools-com',
        });
      }

      console.log(`    -> ${entries.length} schools (${seen.size} unique)`);
    } catch (err) {
      console.log(`    [ERR] ${stateCode}: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  console.log(`  Total from BeautySchools.com: ${leads.length}`);
  return leads;
}

// ── Source 3: Seeded Schools (from PDFs, AACS, NACCAS) ──────────────────
function getSeededSchools() {
  console.log('\n--- Source 3: Seeded Schools (state boards + AACS + NACCAS) ---');

  const schools = [
    // === CALIFORNIA (Board of Barbering & Cosmetology approved) ===
    { company: 'Marinello Schools of Beauty', phone: '', city: 'Los Angeles', state: 'CA', source: 'ca-bbc' },
    { company: 'Paul Mitchell The School', phone: '', city: 'Sherman Oaks', state: 'CA', source: 'ca-bbc' },
    { company: 'Aveda Institute Los Angeles', phone: '', city: 'Los Angeles', state: 'CA', source: 'ca-bbc' },
    { company: 'Federico Beauty Institute', phone: '(916) 418-7223', city: 'Sacramento', state: 'CA', source: 'ca-bbc' },
    { company: 'Palace Beauty College', phone: '(323) 263-2400', city: 'Los Angeles', state: 'CA', source: 'ca-bbc' },
    { company: 'Milan Institute of Cosmetology', phone: '', city: 'Visalia', state: 'CA', source: 'ca-bbc' },
    { company: 'Bellus Academy', phone: '(619) 442-3407', city: 'El Cajon', state: 'CA', source: 'ca-bbc' },
    { company: 'Toni & Guy Hairdressing Academy', phone: '', city: 'Santa Monica', state: 'CA', source: 'ca-bbc' },
    { company: 'International Academy of Style', phone: '', city: 'South Gate', state: 'CA', source: 'ca-bbc' },
    { company: 'Alva Beauty College', phone: '', city: 'Lemon Grove', state: 'CA', source: 'ca-bbc' },
    { company: 'Diamond Beauty College', phone: '', city: 'Fullerton', state: 'CA', source: 'ca-bbc' },
    { company: 'California College of Barbering & Cosmetology', phone: '', city: 'Sacramento', state: 'CA', source: 'ca-bbc' },
    { company: 'Lawrence & Company College of Cosmetology', phone: '', city: 'Selma', state: 'CA', source: 'ca-bbc' },
    { company: 'Elite Barber College', phone: '', city: 'Los Angeles', state: 'CA', source: 'ca-bbc' },
    { company: 'Los Angeles Trade Technical College', phone: '', city: 'Los Angeles', state: 'CA', source: 'ca-bbc' },

    // === TEXAS (TDLR licensed — from data.texas.gov) ===
    { company: 'Ogle School of Hair Skin & Nails', phone: '(817) 840-3600', city: 'Arlington', state: 'TX', source: 'tx-tdlr' },
    { company: 'Aveda Institute Houston', phone: '', city: 'Houston', state: 'TX', source: 'tx-tdlr' },
    { company: 'Paul Mitchell The School Austin', phone: '', city: 'Austin', state: 'TX', source: 'tx-tdlr' },
    { company: 'Avenue Five Institute', phone: '(512) 323-7282', city: 'Austin', state: 'TX', source: 'tx-tdlr' },
    { company: 'Tint School of Makeup & Cosmetology', phone: '', city: 'Dallas', state: 'TX', source: 'tx-tdlr' },
    { company: 'Baldwin Beauty School', phone: '', city: 'Austin', state: 'TX', source: 'tx-tdlr' },
    { company: 'Cosmetology Career Center', phone: '', city: 'Dallas', state: 'TX', source: 'tx-tdlr' },
    { company: 'Milan Institute San Antonio', phone: '', city: 'San Antonio', state: 'TX', source: 'tx-tdlr' },
    { company: 'Southern Careers Institute Cosmetology', phone: '', city: 'Pharr', state: 'TX', source: 'tx-tdlr' },
    { company: 'Regency Beauty Institute', phone: '', city: 'Houston', state: 'TX', source: 'tx-tdlr' },
    { company: 'Nuvani Institute', phone: '', city: 'Austin', state: 'TX', source: 'tx-tdlr' },

    // === NEW YORK (state board licensed) ===
    { company: 'Aveda Institute New York', phone: '(212) 807-1492', city: 'New York', state: 'NY', source: 'ny-dos' },
    { company: 'Empire Beauty School New York', phone: '(804) 773-4266', city: 'New York', state: 'NY', source: 'ny-dos' },
    { company: 'Capri Cosmetology Learning Center', phone: '', city: 'Nanuet', state: 'NY', source: 'ny-dos' },
    { company: 'Continental School of Beauty Culture', phone: '', city: 'Rochester', state: 'NY', source: 'ny-dos' },
    { company: 'American Beauty School', phone: '', city: 'Bronx', state: 'NY', source: 'ny-dos' },
    { company: 'Brittany Beauty School', phone: '', city: 'Levittown', state: 'NY', source: 'ny-dos' },
    { company: 'Allen School of Health Sciences', phone: '', city: 'Jamaica', state: 'NY', source: 'ny-dos' },

    // === FLORIDA (DBPR licensed) ===
    { company: 'Aveda Institute South Florida', phone: '', city: 'Davie', state: 'FL', source: 'fl-dbpr' },
    { company: 'Paul Mitchell The School Miami', phone: '', city: 'Miami', state: 'FL', source: 'fl-dbpr' },
    { company: 'Empire Beauty School Tampa', phone: '', city: 'Tampa', state: 'FL', source: 'fl-dbpr' },
    { company: 'Manhattan Hairstyling Academy', phone: '(813) 250-0556', city: 'Tampa', state: 'FL', source: 'fl-dbpr' },
    { company: 'La Belle Beauty Academy', phone: '', city: 'Hialeah', state: 'FL', source: 'fl-dbpr' },
    { company: 'Bene\'s International School of Beauty', phone: '', city: 'New Port Richey', state: 'FL', source: 'fl-dbpr' },
    { company: 'Florida Academy', phone: '', city: 'Fort Myers', state: 'FL', source: 'fl-dbpr' },

    // === VIRGINIA (DPOR licensed) ===
    { company: 'Yen Beauty Academy', phone: '', city: 'Falls Church', state: 'VA', source: 'va-dpor' },
    { company: 'V Salon & Academy', phone: '', city: '', state: 'VA', source: 'va-dpor' },
    { company: 'Graham Webb International Academy of Hair', phone: '', city: 'Arlington', state: 'VA', source: 'va-dpor' },

    // === NEW JERSEY (Consumer Affairs licensed) ===
    { company: 'Artistic Academy of Hair Design', phone: '', city: '', state: 'NJ', source: 'nj-cos' },
    { company: 'Capri Institute of Hair Design', phone: '', city: 'Paramus', state: 'NJ', source: 'nj-cos' },
    { company: 'Robert Fiance Beauty Schools', phone: '', city: 'Perth Amboy', state: 'NJ', source: 'nj-cos' },

    // === OHIO (State Board licensed) ===
    { company: 'Aveda Fredric\'s Institute Columbus', phone: '', city: 'Columbus', state: 'OH', source: 'oh-cos' },
    { company: 'Ohio State School of Cosmetology', phone: '', city: 'Columbus', state: 'OH', source: 'oh-cos' },
    { company: 'Paramount Beauty Academy', phone: '', city: '', state: 'OH', source: 'oh-cos' },

    // === NATIONAL CHAINS (NACCAS accredited, multi-state) ===
    { company: 'Empire Beauty Schools', phone: '(800) 575-6771', city: 'Pottsville', state: 'PA', source: 'naccas', email: '' },
    { company: 'Paul Mitchell Schools', phone: '', city: 'Beverly Hills', state: 'CA', source: 'naccas', email: '' },
    { company: 'Aveda Institutes', phone: '', city: 'Minneapolis', state: 'MN', source: 'naccas', email: '' },
    { company: 'Tricoci University of Beauty Culture', phone: '(815) 836-5600', city: 'Bridgeview', state: 'IL', source: 'naccas', email: '' },
    { company: 'Ogle School', phone: '(817) 840-3600', city: 'Arlington', state: 'TX', source: 'naccas', email: '' },
    { company: 'Milan Institute', phone: '(559) 624-9000', city: 'Visalia', state: 'CA', source: 'naccas', email: '' },
    { company: 'Regency Beauty Institute', phone: '', city: 'Blaine', state: 'MN', source: 'naccas', email: '' },
    { company: 'Gene Juarez Academy of Beauty', phone: '', city: 'Federal Way', state: 'WA', source: 'naccas', email: '' },
    { company: 'Avance Beauty College', phone: '', city: 'San Diego', state: 'CA', source: 'naccas', email: '' },
    { company: 'Toni & Guy Hairdressing Academy', phone: '', city: '', state: '', source: 'naccas', email: '' },
    { company: 'Sassoon Academy', phone: '', city: 'Santa Monica', state: 'CA', source: 'naccas', email: '' },
    { company: 'Cinta Aveda Institute', phone: '', city: 'San Francisco', state: 'CA', source: 'naccas', email: '' },
    { company: 'Douglas J Aveda Institute', phone: '', city: 'East Lansing', state: 'MI', source: 'naccas', email: '' },
    { company: 'Penrose Academy', phone: '', city: 'Scottsdale', state: 'AZ', source: 'naccas', email: '' },
    { company: 'International School of Beauty', phone: '', city: 'Palm Desert', state: 'CA', source: 'naccas', email: '' },
    { company: 'Cosmetology & Spa Academy', phone: '', city: 'Crystal Lake', state: 'IL', source: 'naccas', email: '' },

    // === AACS (American Association of Cosmetology Schools) members ===
    { company: 'Academy of Beauty Professionals', phone: '', city: '', state: 'WI', source: 'aacs', email: '' },
    { company: 'Austins Beauty College', phone: '', city: '', state: 'NY', source: 'aacs', email: '' },
    { company: 'Avalon Institute', phone: '', city: '', state: 'AZ', source: 'aacs', email: '' },
    { company: 'A New Beginning School of Massage', phone: '', city: '', state: 'TX', source: 'aacs', email: '' },
    { company: 'American Institute of Beauty', phone: '', city: 'St. Petersburg', state: 'FL', source: 'aacs', email: '' },
    { company: 'Artistic Nails & Beauty Academy', phone: '', city: 'Tampa', state: 'FL', source: 'aacs', email: '' },
    { company: 'Baldwin Beauty Schools', phone: '', city: 'Austin', state: 'TX', source: 'aacs', email: '' },
    { company: 'Beauty Professionals Academy of Cosmetology', phone: '', city: '', state: '', source: 'aacs', email: '' },
    { company: 'Boise Barber College', phone: '', city: 'Boise', state: 'ID', source: 'aacs', email: '' },
    { company: 'Career Academy of Beauty', phone: '', city: '', state: 'CA', source: 'aacs', email: '' },
    { company: 'Celebrity School of Beauty', phone: '', city: '', state: 'FL', source: 'aacs', email: '' },
    { company: 'Collectiv Academy', phone: '', city: '', state: 'UT', source: 'aacs', email: '' },
    { company: 'Cosmetology Education Center', phone: '', city: '', state: 'VA', source: 'aacs', email: '' },
  ];

  const leads = schools.map(s => ({
    email: s.email || '',
    first_name: '',
    last_name: '',
    company: normalizeCompany(s.company),
    phone: normalizePhone(s.phone || ''),
    city: s.city || '',
    state: s.state || '',
    country: 'US',
    source: s.source,
  }));

  console.log(`  Loaded ${leads.length} seeded schools`);
  return leads;
}

// ── CSV writing ─────────────────────────────────────────────────────────
function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const rows = leads.map(l =>
    [l.email, l.first_name, l.last_name, l.company, l.phone, l.city, l.state, l.country, l.source]
      .map(escapeCSV)
      .join(',')
  );
  fs.writeFileSync(filePath, [CSV_HEADER, ...rows].join('\n'));
}

// ── Dedup ───────────────────────────────────────────────────────────────
function dedup(leads) {
  const seen = new Set();
  return leads.filter(l => {
    // Key: company name + state (to allow same chain in different states)
    const namePart = l.company.toLowerCase()
      .replace(/,?\s*(inc|llc|llp|corp|ltd|co|school|schools|academy|institute|college|center|centre)\.?$/i, '')
      .replace(/[^a-z0-9]/g, '');
    const key = `${namePart}|${(l.state || '').toLowerCase()}|${(l.city || '').toLowerCase().replace(/[^a-z]/g, '')}`;
    if (!namePart || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// Merge: prefer records with more data
function mergeDedupPreferComplete(leads) {
  const map = new Map();
  for (const l of leads) {
    const namePart = l.company.toLowerCase()
      .replace(/,?\s*(inc|llc|llp|corp|ltd|co|school|schools|academy|institute|college|center|centre)\.?$/i, '')
      .replace(/[^a-z0-9]/g, '');
    const key = `${namePart}|${(l.state || '').toLowerCase()}`;
    const existing = map.get(key);
    if (!existing) {
      map.set(key, { ...l });
    } else {
      // Merge: fill in blanks from the new record
      if (!existing.phone && l.phone) existing.phone = l.phone;
      if (!existing.email && l.email) existing.email = l.email;
      if (!existing.city && l.city) existing.city = l.city;
      // Prefer more specific source
      if (existing.source === 'naccas' && l.source !== 'naccas') existing.source = l.source;
    }
  }
  return Array.from(map.values());
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  console.log('');
  console.log('=============================================================');
  console.log('  MORTAR -- Beauty / Cosmetology Schools Scraper');
  console.log('=============================================================');
  console.log(`  Mode:     ${TEST_MODE ? 'TEST (3 states)' : 'FULL (all 50 states + DC)'}`);
  console.log(`  Delay:    ${DELAY_MS}ms`);
  console.log(`  Output:   ${OUTPUT_FILE}`);
  console.log('');

  // Determine which states to scrape
  let stateCodes;
  if (SPECIFIC_STATES) {
    stateCodes = SPECIFIC_STATES;
    console.log(`  States:   ${stateCodes.join(', ')}`);
  } else if (TEST_MODE) {
    stateCodes = ['CA', 'TX', 'NY'];
    console.log(`  States:   ${stateCodes.join(', ')} (test mode)`);
  } else {
    stateCodes = Object.keys(US_STATES);
    console.log(`  States:   all ${stateCodes.length}`);
  }
  console.log('');

  const allLeads = [];

  // Source 1: BeautySchoolsDirectory.com
  const bsdLeads = await scrapeBeautySchoolsDirectory(stateCodes);
  allLeads.push(...bsdLeads);

  // Source 2: BeautySchools.com
  const bscLeads = await scrapeBeautySchoolsCom(stateCodes);
  allLeads.push(...bscLeads);

  // Source 3: Seeded schools
  const seededLeads = getSeededSchools();
  allLeads.push(...seededLeads);

  // Merge + dedup
  console.log('\n--- Deduplication ---');
  console.log(`  Before dedup: ${allLeads.length}`);
  const merged = mergeDedupPreferComplete(allLeads);
  console.log(`  After merge:  ${merged.length}`);

  // Write CSV
  console.log('\n--- Writing CSV ---');
  writeCSV(OUTPUT_FILE, merged);
  console.log(`  Wrote ${merged.length} leads to ${OUTPUT_FILE}`);

  // Timestamped copy
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const tsFile = path.join(OUTPUT_DIR, `beauty-cosmetology-schools_${ts}.csv`);
  writeCSV(tsFile, merged);
  console.log(`  Timestamped: ${tsFile}`);

  // Stats
  const withEmail = merged.filter(l => l.email).length;
  const withPhone = merged.filter(l => l.phone).length;
  const withCity = merged.filter(l => l.city).length;

  // Source breakdown
  const sourceCounts = {};
  for (const l of merged) {
    sourceCounts[l.source] = (sourceCounts[l.source] || 0) + 1;
  }

  // State breakdown (top 10)
  const stateCnts = {};
  for (const l of merged) {
    if (l.state) stateCnts[l.state] = (stateCnts[l.state] || 0) + 1;
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('');
  console.log('=============================================================');
  console.log('  BEAUTY / COSMETOLOGY SCHOOLS SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  Total schools:    ${merged.length}`);
  console.log(`  With email:       ${withEmail}`);
  console.log(`  With phone:       ${withPhone}`);
  console.log(`  With city:        ${withCity}`);
  console.log(`  Time:             ${elapsed}s`);
  console.log('');
  console.log('  Source breakdown:');
  Object.entries(sourceCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([src, cnt]) => console.log(`    ${src}: ${cnt}`));
  console.log('');
  console.log('  Top states:');
  Object.entries(stateCnts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .forEach(([st, cnt]) => console.log(`    ${st}: ${cnt}`));
  console.log('');
  console.log('  Notes:');
  console.log('  - BeautySchoolsDirectory.com: best coverage, has phone + address');
  console.log('  - BeautySchools.com: supplemental, mostly name + city only');
  console.log('  - NACCAS accredits ~1,300 schools but requires ASP.NET form POST');
  console.log('  - AACS member directory is a GrowthZone SPA (needs Puppeteer)');
  console.log('  - Individual school pages on BSD have website URLs for enrichment');
  console.log('  - State board PDFs (CA, VA, NJ, TX) provide official approved lists');
  console.log('  - Texas data.texas.gov has 1,000+ records (incl. vocational/HS)');
  console.log('  - Enrich with email via website crawl for schools with phone only');
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
