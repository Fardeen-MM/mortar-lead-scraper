#!/usr/bin/env node
/**
 * UK Immigration Law Firm Scraper
 *
 * Scrapes immigration/visa/asylum law firms from the SRA (Solicitors Regulation Authority)
 * register at https://www.sra.org.uk/consumers/register/
 *
 * Strategy:
 *   1. Search SRA register for firms matching immigration-related keywords
 *   2. Fetch each firm's detail page for full contact info (phone, email, website, address)
 *   3. Deduplicate across search terms by SRA number
 *   4. Save to CSV
 *
 * Usage:
 *   node scripts/scrape-uk-immigration.js
 *   node scripts/scrape-uk-immigration.js --max-details 10   # Limit detail fetches (for testing)
 *   node scripts/scrape-uk-immigration.js --search-only       # Skip detail page fetches
 */

const https = require('https');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// --- Config ---
const SEARCH_TERMS = ['immigration', 'visa', 'asylum', 'border', 'nationality'];

// Firms that match search terms but are NOT immigration law firms
const EXCLUDED_SRA_NUMBERS = new Set([
  '64973',    // Visa Europe — payment processing company
  '8013208',  // Visa Europe Limited — payment processing company
  '573134',   // Visa Middle East FZ-LLC — payment company
  '8002203',  // Visa Payments Limited — payment company
  '820317',   // Border to Coast Pensions Partnership Limited
  '665803',   // Border to Coast Pensions Partnership Ltd
  '833841',   // Surrey and Borders Partnership NHS Foundation Trust
  '611875',   // Upper Tribunal Immigration and Asylum Chamber (government tribunal, not a firm)
]);
const BASE_URL = 'https://www.sra.org.uk';
const SEARCH_PATH = '/consumers/register/';
const DETAIL_PATH = '/consumers/register/organisation/';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-solicitors.csv');
const DELAY_MS = 2000; // Polite delay between requests
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// --- CLI args ---
const args = process.argv.slice(2);
const maxDetails = args.includes('--max-details')
  ? parseInt(args[args.indexOf('--max-details') + 1], 10)
  : Infinity;
const searchOnly = args.includes('--search-only');

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'X-Requested-With': 'XMLHttpRequest',
        ...headers,
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          // Follow redirect
          const redirectUrl = res.headers.location.startsWith('http')
            ? res.headers.location
            : `https://${parsed.hostname}${res.headers.location}`;
          httpGet(redirectUrl, headers).then(resolve).catch(reject);
          return;
        }
        resolve({ status: res.statusCode, body: data });
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function cleanPhone(phone) {
  if (!phone) return '';
  // Normalize UK phone: remove spaces, add +44 prefix
  let p = phone.replace(/[\s\-\(\)\.]/g, '');
  if (p.startsWith('0')) {
    p = '+44' + p.slice(1);
  } else if (!p.startsWith('+')) {
    p = '+44' + p;
  }
  return p;
}

function extractDomain(website) {
  if (!website) return '';
  let w = website.toLowerCase().trim();
  w = w.replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '');
  return w;
}

function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|\s|-)\S/g, c => c.toUpperCase());
}

// --- SRA Search ---
async function searchSRA(searchText) {
  const url = `${BASE_URL}${SEARCH_PATH}?searchText=${encodeURIComponent(searchText)}&searchBy=Organisation&numberOfResults=500`;
  console.log(`  Searching SRA for "${searchText}"...`);

  const { status, body } = await httpGet(url);
  if (status !== 200) {
    console.log(`  WARNING: Search returned status ${status}`);
    return [];
  }

  const $ = cheerio.load(body);
  const firms = [];

  // Extract firm names and SRA numbers from search results
  $('ul.lookup__search__result__list li a').each((i, el) => {
    const $a = $(el);
    const onclick = $a.attr('onclick') || '';
    const match = onclick.match(/goToOrgDetails\((\d+)\)/);
    if (!match) return;

    const sraNumber = match[1];
    const firmName = $a.find('h2').text().trim();
    const city = $a.find('.label__sm__block__location').text().replace(/Head office\s*/i, '').replace(/in\s*/i, '').trim();

    firms.push({
      sra_number: sraNumber,
      firm_name: firmName,
      city: city,
      search_term: searchText,
    });
  });

  // Also check total count
  const countText = $('h1.lead').first().text();
  const totalMatch = countText.match(/(\d+)\s+of\s+(\d+)\s+firm/);
  if (totalMatch) {
    console.log(`  Found ${totalMatch[1]} of ${totalMatch[2]} firms for "${searchText}"`);
  } else {
    console.log(`  Found ${firms.length} firms for "${searchText}"`);
  }

  return firms;
}

// --- SRA Firm Detail ---
async function fetchFirmDetail(sraNumber) {
  const url = `${BASE_URL}${DETAIL_PATH}?sraNumber=${sraNumber}`;

  const { status, body } = await httpGet(url);
  if (status !== 200) {
    console.log(`    WARNING: Detail page for ${sraNumber} returned status ${status}`);
    return null;
  }

  const $ = cheerio.load(body);
  const detail = {
    sra_number: sraNumber,
    profile_url: url,
  };

  // Firm name from the detail page
  detail.firm_name = $('h1.reg__detail__h1').text().trim();

  // Parse the details list (dt/dd pairs)
  $('dl.reg__details .label__list__details').each((i, el) => {
    const label = $(el).find('dt').text().trim().toLowerCase();
    const value = $(el).find('dd').text().trim();

    if (label.includes('website')) {
      detail.website = value;
    } else if (label.includes('sra number')) {
      detail.sra_number = value;
    }
  });

  // Parse offices section for contact details
  const offices = [];
  $('#collapseOffices .result-link').each((i, el) => {
    const office = {};

    // Office title/location
    office.title = $(el).find('h4').text().trim();

    // Address
    const addressEl = $(el).find('.icon-location').parent().next();
    if (addressEl.length) {
      office.address = addressEl.text().trim();
    }

    // Phone
    const phoneEl = $(el).find('.icon-phone').parent().next();
    if (phoneEl.length) {
      office.phone = phoneEl.text().trim();
    }

    // Website
    const websiteEl = $(el).find('.icon-desktop').parent().next();
    if (websiteEl.length) {
      const href = websiteEl.find('a').attr('href');
      office.website = href || websiteEl.text().trim();
    }

    // Email
    const emailEl = $(el).find('.icon-envelope-o').parent().next();
    if (emailEl.length) {
      const href = emailEl.find('a').attr('href');
      if (href && href.startsWith('mailto:')) {
        office.email = href.replace('mailto:', '');
      } else {
        office.email = emailEl.text().trim();
      }
    }

    offices.push(office);
  });

  // Use head office (first) for primary contact details
  if (offices.length > 0) {
    const head = offices[0];
    if (!detail.phone && head.phone) detail.phone = head.phone;
    if (!detail.email && head.email) detail.email = head.email;
    if (!detail.website && head.website) detail.website = head.website;
    if (head.address) detail.address = head.address;
  }

  // Parse people/solicitors in the firm
  detail.people = [];
  $('#peopleResults .detail-person-item a').each((i, el) => {
    const $a = $(el);
    const name = $a.find('h2').text().trim();
    const role = $a.find('.label__sm__block__subline__positive').text().trim();
    const personHref = $a.attr('href') || '';

    if (name && !name.includes('Search') && !name.includes('filter')) {
      detail.people.push({
        name,
        role: role || 'Solicitor',
        profile_url: personHref.startsWith('http') ? personHref : `${BASE_URL}${personHref}`,
      });
    }
  });

  return detail;
}

// --- Parse name ---
function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Remove titles
  let name = fullName
    .replace(/^(Mr|Mrs|Ms|Miss|Dr|Prof|Professor|Sir|Dame|Lord|Lady|Rt Hon|Hon)\s+/i, '')
    .replace(/\s+(QC|KC|CBE|OBE|MBE|LLB|LLM|BA|MA|PhD|Esq|JP)$/gi, '')
    .trim();

  const parts = name.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };

  return {
    first_name: titleCase(parts[0]),
    last_name: titleCase(parts.slice(1).join(' ')),
  };
}

// --- Parse city from address ---
function parseCityFromAddress(address) {
  if (!address) return '';
  // UK addresses typically: "Street, CITY, COUNTY, POSTCODE, Country"
  // or "Street, Street2, CITY, POSTCODE, Country"
  const parts = address.split(',').map(p => p.trim());

  // Work backward from end, skipping country and postcode
  // England/Wales/Scotland/Northern Ireland is always last
  // Postcode is second to last (matches pattern like "EC2A 4NE", "HA3 5AB")
  // City is typically the part before county or postcode

  const skipPatterns = [
    /^england$/i, /^wales$/i, /^scotland$/i, /^northern ireland$/i,
    /^united kingdom$/i, /^uk$/i,
    /^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i,  // UK postcode pattern
  ];

  // Known UK counties to skip
  const counties = new Set([
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

  // Known UK cities for matching
  const knownCities = new Set([
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
    'godalming', 'timperley', 'wednesbury',
  ]);

  // First pass: look for known cities anywhere in address parts
  for (let i = 0; i < parts.length; i++) {
    if (knownCities.has(parts[i].toLowerCase())) {
      return titleCase(parts[i]);
    }
  }

  // Filter out known non-city parts
  const candidates = [];
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    if (skipPatterns.some(p => p.test(part))) continue;
    if (counties.has(part.toLowerCase())) continue;
    // Skip parts that look like street addresses (contain numbers or "Street"/"Road"/etc)
    if (/^\d+\s/.test(part)) continue;
    if (/\d/.test(part)) continue; // Skip any part with numbers
    if (/\b(street|road|lane|way|drive|avenue|close|court|place|crescent|terrace|square|house|park|mill|gate|walk|row|precinct|floor|suite|unit|level|room)\b/i.test(part)) continue;
    candidates.push(part);
  }

  // The first candidate after the street address is usually the city/town
  if (candidates.length > 0) {
    return titleCase(candidates[0]);
  }

  return '';
}

// --- Main ---
async function main() {
  console.log('=== UK Immigration Law Firm Scraper (SRA Register) ===\n');

  // Ensure output dir exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Step 1: Search across all terms and deduplicate by SRA number
  const firmsBySra = new Map();

  for (const term of SEARCH_TERMS) {
    try {
      const results = await searchSRA(term);
      for (const firm of results) {
        if (EXCLUDED_SRA_NUMBERS.has(firm.sra_number)) {
          continue; // Skip non-immigration firms
        }
        if (!firmsBySra.has(firm.sra_number)) {
          firmsBySra.set(firm.sra_number, firm);
        }
      }
      await sleep(DELAY_MS);
    } catch (err) {
      console.log(`  ERROR searching for "${term}": ${err.message}`);
    }
  }

  console.log(`\nTotal unique firms found: ${firmsBySra.size}\n`);

  if (firmsBySra.size === 0) {
    console.log('No firms found. Exiting.');
    return;
  }

  // Step 2: Fetch detail pages for each firm
  const leads = [];
  const sraNumbers = [...firmsBySra.keys()];
  const totalToFetch = searchOnly ? 0 : Math.min(sraNumbers.length, maxDetails);

  if (searchOnly) {
    console.log('--search-only mode: skipping detail page fetches\n');
    // Create leads from search results only
    for (const firm of firmsBySra.values()) {
      leads.push({
        first_name: '',
        last_name: '',
        firm_name: titleCase(firm.firm_name),
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: titleCase(firm.city),
        state: 'UK-EW',
        country: 'UK',
        niche: 'immigration consultant',
        source: 'sra_register',
        profile_url: `${BASE_URL}${DETAIL_PATH}?sraNumber=${firm.sra_number}`,
        sra_number: firm.sra_number,
      });
    }
  } else {
    console.log(`Fetching detail pages for ${totalToFetch} firms...\n`);

    for (let i = 0; i < totalToFetch; i++) {
      const sraNumber = sraNumbers[i];
      const searchFirm = firmsBySra.get(sraNumber);

      console.log(`  [${i + 1}/${totalToFetch}] ${searchFirm.firm_name} (SRA: ${sraNumber})...`);

      try {
        const detail = await fetchFirmDetail(sraNumber);

        if (detail) {
          const firmName = titleCase(detail.firm_name || searchFirm.firm_name);
          const website = detail.website || '';
          const domain = extractDomain(website);
          const phone = cleanPhone(detail.phone || '');
          const email = (detail.email || '').toLowerCase().trim();
          const city = parseCityFromAddress(detail.address) || titleCase(searchFirm.city);

          // Create a lead for the firm itself
          leads.push({
            first_name: '',
            last_name: '',
            firm_name: firmName,
            title: '',
            email: email,
            phone: phone,
            website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
            domain: domain,
            city: city,
            state: 'UK-EW',
            country: 'UK',
            niche: 'immigration consultant',
            source: 'sra_register',
            profile_url: detail.profile_url,
            sra_number: sraNumber,
          });

          // Also create leads for named people at the firm
          if (detail.people && detail.people.length > 0) {
            for (const person of detail.people.slice(0, 10)) { // Max 10 people per firm
              const { first_name, last_name } = parseName(person.name);
              if (first_name || last_name) {
                leads.push({
                  first_name,
                  last_name,
                  firm_name: firmName,
                  title: person.role || 'Solicitor',
                  email: '',
                  phone: phone,
                  website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
                  domain: domain,
                  city: city,
                  state: 'UK-EW',
                  country: 'UK',
                  niche: 'immigration consultant',
                  source: 'sra_register',
                  profile_url: person.profile_url || detail.profile_url,
                  sra_number: sraNumber,
                });
              }
            }
          }

          const contactInfo = [
            email ? 'email' : null,
            phone ? 'phone' : null,
            website ? 'website' : null,
          ].filter(Boolean).join(', ');
          console.log(`    -> ${firmName} | ${city} | ${contactInfo || 'no contact info'}`);
        }
      } catch (err) {
        console.log(`    ERROR: ${err.message}`);
      }

      await sleep(DELAY_MS);
    }
  }

  // Step 3: Write CSV
  console.log(`\n=== Writing ${leads.length} leads to CSV ===\n`);

  const CSV_COLUMNS = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
    'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
    'profile_url', 'sra_number',
  ];

  const csvHeader = CSV_COLUMNS.join(',');
  const csvRows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col])).join(',')
  );
  const csvContent = [csvHeader, ...csvRows].join('\n');

  fs.writeFileSync(OUTPUT_FILE, csvContent, 'utf-8');
  console.log(`Saved to: ${OUTPUT_FILE}`);

  // Step 4: Summary
  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;
  const withWebsite = leads.filter(l => l.website).length;
  const firmLeads = leads.filter(l => !l.first_name);
  const personLeads = leads.filter(l => l.first_name);
  const uniqueFirms = new Set(leads.map(l => l.sra_number)).size;

  console.log(`\n=== Summary ===`);
  console.log(`Total leads:    ${leads.length}`);
  console.log(`  Firm leads:   ${firmLeads.length}`);
  console.log(`  Person leads: ${personLeads.length}`);
  console.log(`Unique firms:   ${uniqueFirms}`);
  console.log(`With email:     ${withEmail}`);
  console.log(`With phone:     ${withPhone}`);
  console.log(`With website:   ${withWebsite}`);
  console.log(`\nDone.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
