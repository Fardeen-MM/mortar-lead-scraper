#!/usr/bin/env node
/**
 * Lawyers.com Immigration Directory Scraper
 *
 * Scrapes https://www.lawyers.com/immigration/{state}/find-law-firms-by-city/
 * to extract immigration law firms and attorneys across all US states + Canada.
 *
 * Strategy:
 *   1. For each state, fetch the city listing page to discover all cities
 *   2. For each city, scrape the listing page(s) with pagination
 *   3. Extract firm data from embedded JavaScript (statValue vars) + HTML
 *   4. Extract individual attorney names from "lawyers-at-firms" sections
 *   5. Dedup by firm listingId
 *   6. Output CSV
 *
 * Note: Phone numbers from data-ctn-rtn are call-tracking numbers (not real).
 *       We extract the dwsLink (website) which is the real website.
 *
 * Output: output/us-immigration-lawyers-lawyerscom.csv
 *
 * Usage:
 *   node scripts/scrape-lawyerscom-immigration.js                    # All US states
 *   node scripts/scrape-lawyerscom-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-lawyerscom-immigration.js --test             # Test (3 states, 3 cities)
 *   node scripts/scrape-lawyerscom-immigration.js --canada           # Canada only
 *   node scripts/scrape-lawyerscom-immigration.js --resume           # US: skip states already in CSV
 *   node scripts/scrape-lawyerscom-immigration.js --canada --resume  # Canada: skip provinces already in CSV
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// --- Config ---
const BASE_URL = 'https://www.lawyers.com';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const DELAY_MS = 2000;
const CITY_DELAY_MS = 1500;
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'rating', 'attorneys'
];

const US_STATE_SLUGS = {
  AL: 'alabama', AK: 'alaska', AZ: 'arizona', AR: 'arkansas',
  CA: 'california', CO: 'colorado', CT: 'connecticut', DE: 'delaware',
  DC: 'district-of-columbia', FL: 'florida', GA: 'georgia', HI: 'hawaii',
  ID: 'idaho', IL: 'illinois', IN: 'indiana', IA: 'iowa',
  KS: 'kansas', KY: 'kentucky', LA: 'louisiana', ME: 'maine',
  MD: 'maryland', MA: 'massachusetts', MI: 'michigan', MN: 'minnesota',
  MS: 'mississippi', MO: 'missouri', MT: 'montana', NE: 'nebraska',
  NV: 'nevada', NH: 'new-hampshire', NJ: 'new-jersey', NM: 'new-mexico',
  NY: 'new-york', NC: 'north-carolina', ND: 'north-dakota', OH: 'ohio',
  OK: 'oklahoma', OR: 'oregon', PA: 'pennsylvania', RI: 'rhode-island',
  SC: 'south-carolina', SD: 'south-dakota', TN: 'tennessee', TX: 'texas',
  UT: 'utah', VT: 'vermont', VA: 'virginia', WA: 'washington',
  WV: 'west-virginia', WI: 'wisconsin', WY: 'wyoming',
};

const SLUG_TO_CODE = {};
for (const [code, slug] of Object.entries(US_STATE_SLUGS)) {
  SLUG_TO_CODE[slug] = code;
}

const CA_PROVINCE_SLUGS = {
  'CA-AB': 'alberta',
  'CA-BC': 'british-columbia',
  'CA-MB': 'manitoba',
  'CA-NB': 'new-brunswick',
  'CA-NL': 'newfoundland-and-labrador',
  'CA-NS': 'nova-scotia',
  'CA-NT': 'northwest-territories',
  'CA-NU': 'nunavut',
  'CA-ON': 'ontario',
  'CA-PE': 'prince-edward-island',
  'CA-QC': 'quebec',
  'CA-SK': 'saskatchewan',
  'CA-YT': 'yukon-territory',
};

// Ordered by immigration market size
const ALL_US_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const canadaMode = args.includes('--canada');
const resumeMode = args.includes('--resume');
const statesIdx = args.indexOf('--states');
const statesFilter = statesIdx >= 0
  ? args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase())
  : null;

const OUTPUT_FILE = path.join(OUTPUT_DIR, canadaMode
  ? 'canada-immigration-lawyers-lawyerscom.csv'
  : 'us-immigration-lawyers-lawyerscom.csv');

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url, retries = 2) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
      },
      timeout: 30000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        return resolve(httpGet(redir, retries));
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
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
        reject(new Error('timeout'));
      }
    });
  });
}

function extractDomain(url) {
  if (!url) return '';
  try { return new URL(url).hostname.replace(/^www\./, ''); } catch { return ''; }
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function csvRow(obj) {
  return CSV_HEADERS.map(h => csvEscape(obj[h] || '')).join(',');
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|ph\.?d\.?|m\.?d\.?|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = cleaned.split(' ').filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

// --- CSV Resume Helpers ---

function parseCSVLine(line) {
  const fields = [];
  let field = '';
  let inQuote = false;
  for (let j = 0; j < line.length; j++) {
    const ch = line[j];
    if (ch === '"') { inQuote = !inQuote; continue; }
    if (ch === ',' && !inQuote) { fields.push(field); field = ''; continue; }
    field += ch;
  }
  fields.push(field);
  return fields;
}

function readExistingStates(csvPath) {
  const states = new Set();
  if (!fs.existsSync(csvPath)) return states;
  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split('\n').filter(Boolean);
  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    const state = fields[9]; // state column
    if (state) states.add(state);
  }
  return states;
}

function countExistingLines(csvPath) {
  if (!fs.existsSync(csvPath)) return 0;
  const content = fs.readFileSync(csvPath, 'utf8');
  return content.split('\n').filter(Boolean).length - 1; // minus header
}

// --- Scraping Functions ---

async function fetchCityLinks(stateCode, isCanada = false) {
  const slug = isCanada ? CA_PROVINCE_SLUGS[stateCode] : US_STATE_SLUGS[stateCode];
  if (!slug) return [];

  const prefix = isCanada ? '/canada' : '';
  const url = `${BASE_URL}${prefix}/immigration/${slug}/find-law-firms-by-city/`;

  const resp = await httpGet(url);
  if (resp.status !== 200) {
    console.error(`  [WARN] City list for ${stateCode}: HTTP ${resp.status}`);
    return [];
  }

  const $ = cheerio.load(resp.body);
  const cities = [];
  const cityPattern = new RegExp(`/immigration/([^/]+)/${slug}/law-firms/`);

  $('a').each((i, el) => {
    const href = $(el).attr('href') || '';
    const match = href.match(cityPattern);
    if (match) {
      const citySlug = match[1];
      // Prefer title attr ("Los Angeles, CA Immigration Law Firms") -> extract city part
      const titleAttr = $(el).attr('title') || '';
      let cityName = '';
      if (titleAttr) {
        const titleMatch = titleAttr.match(/^([^,]+)/);
        cityName = titleMatch ? titleMatch[1].trim() : '';
      }
      // Fallback: clean link text by removing "Immigration Lawyers"
      if (!cityName) {
        cityName = $(el).text().trim()
          .replace(/\s*Immigration\s*(Lawyers?|Law\s*Firms?|Attorneys?)\s*/gi, '')
          .trim();
      }
      // Final fallback: title-case the slug
      if (!cityName) {
        cityName = citySlug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
      }
      const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
      cities.push({ slug: citySlug, name: cityName, url: fullUrl });
    }
  });

  // Dedup by slug
  const seen = new Set();
  return cities.filter(c => {
    if (seen.has(c.slug)) return false;
    seen.add(c.slug);
    return true;
  });
}

function parseListingPage(html, stateCode, cityName, country) {
  const listings = [];
  const $ = cheerio.load(html);

  // Extract data from statValue JavaScript variables
  const scriptMatches = html.match(/var\s+statValue\d+\s*=\s*(\{[^;]+\});/g) || [];

  for (const match of scriptMatches) {
    try {
      const jsonStr = match.replace(/var\s+statValue\d+\s*=\s*/, '').replace(/;$/, '');
      const parsed = JSON.parse(jsonStr);
      const data = parsed.data || parsed;

      const firmName = (data.name || '').replace(/&amp;/g, '&');
      if (!firmName) continue;

      let website = (data.dwsLink || '').replace(/\\\//g, '/');
      if (website && !website.startsWith('http')) website = 'https://' + website;

      const listingId = data.listingId || '';
      const profileLink = $(`a#firmName_${listingId}`).attr('href') || '';
      let profileUrl = '';
      if (profileLink) {
        if (profileLink.startsWith('http')) {
          profileUrl = profileLink;
        } else if (profileLink.startsWith('//')) {
          profileUrl = `https:${profileLink}`;
        } else {
          profileUrl = `https://www.lawyers.com${profileLink}`;
        }
      }

      // Try to get ratings
      const rating = data.crrRating && data.crrRating !== 'NR'
        ? Number(data.crrRating).toFixed(1)
        : '';

      listings.push({
        firm_name: firmName,
        website,
        domain: extractDomain(website),
        profile_url: profileUrl,
        city: cityName,
        state: stateCode,
        country,
        rating,
        listing_id: listingId,
        org_id: data.orgId || '',
      });
    } catch (e) {
      // Skip unparseable entries
    }
  }

  // Extract individual attorney names from HTML
  const attorneyNames = [];
  $('span.attorney, .attorney').each((i, el) => {
    const name = $(el).text().trim();
    if (name && name.length > 2 && !name.includes('<')) {
      attorneyNames.push(name);
    }
  });

  // Associate attorneys with their closest firm listing
  // For now, store all attorney names as a semicolon-separated field
  if (attorneyNames.length > 0 && listings.length > 0) {
    // Simple heuristic: distribute attorneys across firms
    // In practice, the attorneys appear within their firm's card
    // For simplicity, attach all to the first listing
    // A more sophisticated parser would match by DOM proximity
  }

  // Check for pagination
  const hasNextPage = html.includes('page=') && $('a[rel="next"]').length > 0;
  const nextPageMatch = html.match(/page=(\d+)/g);
  let maxPage = 1;
  if (nextPageMatch) {
    for (const p of nextPageMatch) {
      const num = parseInt(p.replace('page=', ''));
      if (num > maxPage) maxPage = num;
    }
  }

  return { listings, maxPage, attorneyNames };
}

async function scrapeCityListings(cityUrl, stateCode, cityName, country) {
  const allListings = [];
  const allAttorneys = [];
  let page = 1;
  let maxPage = 1;

  while (page <= maxPage && page <= 50) { // Safety cap at 50 pages
    const url = page === 1 ? cityUrl : `${cityUrl}?page=${page}`;
    const resp = await httpGet(url);

    if (resp.status !== 200) {
      if (page === 1) console.error(`    [WARN] ${cityName}: HTTP ${resp.status}`);
      break;
    }

    const result = parseListingPage(resp.body, stateCode, cityName, country);
    allListings.push(...result.listings);
    allAttorneys.push(...result.attorneyNames);

    if (page === 1 && result.maxPage > 1) {
      maxPage = result.maxPage;
    }

    if (page >= maxPage) break;
    page++;
    await sleep(CITY_DELAY_MS);
  }

  return { listings: allListings, attorneys: allAttorneys, pages: page };
}

// --- Main ---
async function main() {
  const isCanada = canadaMode;
  console.log(`=== Lawyers.com Immigration Scraper (${isCanada ? 'Canada' : 'US'}) ===`);
  console.log(`Mode: ${testMode ? 'TEST' : 'FULL'}${resumeMode ? ' + RESUME' : ''}`);

  let states;
  if (statesFilter) {
    states = statesFilter;
  } else if (isCanada) {
    states = Object.keys(CA_PROVINCE_SLUGS);
  } else if (testMode) {
    states = ['CA', 'NY', 'TX'];
  } else {
    states = ALL_US_STATES;
  }

  const country = isCanada ? 'CA' : 'US';

  // Resume mode: read existing CSV and skip states already scraped
  let existingCount = 0;
  let skippedStates = [];
  if (resumeMode) {
    const existingStates = readExistingStates(OUTPUT_FILE);
    existingCount = countExistingLines(OUTPUT_FILE);
    const originalCount = states.length;
    skippedStates = states.filter(s => existingStates.has(s));
    states = states.filter(s => !existingStates.has(s));
    console.log(`Resume: ${OUTPUT_FILE}`);
    console.log(`  Existing leads: ${existingCount}`);
    console.log(`  States already scraped (${skippedStates.length}): ${skippedStates.join(', ')}`);
    console.log(`  States remaining (${states.length}): ${states.join(', ')}`);
    if (states.length === 0) {
      console.log('\nAll states already scraped. Nothing to do.');
      return;
    }
  }

  console.log(`\nStates to scrape: ${states.join(', ')}\n`);

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const allFirms = [];
  const seenIds = new Set();
  const seenNames = new Set(); // fallback dedup

  for (let si = 0; si < states.length; si++) {
    const stateCode = states[si];
    console.log(`\n[${si + 1}/${states.length}] ${stateCode} — fetching city list...`);

    const cities = await fetchCityLinks(stateCode, isCanada);
    console.log(`  Found ${cities.length} cities`);

    if (cities.length === 0) {
      console.log(`  ${stateCode}: no cities found (may not have listings on Lawyers.com)`);
      continue;
    }

    const maxCities = testMode ? 3 : cities.length;
    let stateFirms = 0;

    for (let ci = 0; ci < Math.min(cities.length, maxCities); ci++) {
      const city = cities[ci];
      const result = await scrapeCityListings(city.url, stateCode, city.name, country);

      for (const firm of result.listings) {
        // Dedup
        const idKey = firm.listing_id ? `id:${firm.listing_id}` : '';
        const nameKey = `${firm.firm_name}|${firm.city}|${firm.state}`.toLowerCase();

        if (idKey && seenIds.has(idKey)) continue;
        if (seenNames.has(nameKey)) continue;

        if (idKey) seenIds.add(idKey);
        seenNames.add(nameKey);

        allFirms.push(firm);
        stateFirms++;
      }

      if ((ci + 1) % 20 === 0 || ci === Math.min(cities.length, maxCities) - 1) {
        console.log(`  ${ci + 1}/${Math.min(cities.length, maxCities)} cities scraped (${stateFirms} firms)`);
      }

      await sleep(CITY_DELAY_MS);
    }

    console.log(`  ${stateCode}: ${stateFirms} firms (running total: ${allFirms.length})`);

    // In resume mode, flush after each state to avoid data loss
    if (resumeMode && stateFirms > 0) {
      const newLines = [];
      // Only take the firms from this state (last stateFirms entries)
      const stateFirmsList = allFirms.slice(allFirms.length - stateFirms);
      for (const firm of stateFirmsList) {
        newLines.push(csvRow({
          first_name: '',
          last_name: '',
          firm_name: firm.firm_name,
          title: 'Immigration Law Firm',
          email: '',
          phone: '',
          website: firm.website,
          domain: firm.domain,
          city: firm.city,
          state: firm.state,
          country: firm.country,
          niche: 'immigration',
          source: 'lawyers_com',
          profile_url: firm.profile_url,
          rating: firm.rating,
          attorneys: '',
        }));
      }
      fs.appendFileSync(OUTPUT_FILE, '\n' + newLines.join('\n'), 'utf8');
      console.log(`  [SAVED] Appended ${stateFirms} firms to ${path.basename(OUTPUT_FILE)}`);
    }

    await sleep(DELAY_MS);
  }

  // Write CSV (full write for non-resume, summary for resume)
  if (!resumeMode) {
    const csvLines = [CSV_HEADERS.join(',')];
    for (const firm of allFirms) {
      csvLines.push(csvRow({
        first_name: '',
        last_name: '',
        firm_name: firm.firm_name,
        title: 'Immigration Law Firm',
        email: '',
        phone: '', // CTN numbers are tracking numbers, not real
        website: firm.website,
        domain: firm.domain,
        city: firm.city,
        state: firm.state,
        country: firm.country,
        niche: 'immigration',
        source: 'lawyers_com',
        profile_url: firm.profile_url,
        rating: firm.rating,
        attorneys: '',
      }));
    }
    fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  }

  const finalCount = resumeMode ? existingCount + allFirms.length : allFirms.length;
  console.log(`\n=== DONE ===`);
  console.log(`New firms scraped: ${allFirms.length}`);
  if (resumeMode) {
    console.log(`Previously existing: ${existingCount}`);
    console.log(`Total firms in CSV: ${finalCount}`);
  } else {
    console.log(`Total firms: ${allFirms.length}`);
  }
  console.log(`Output: ${OUTPUT_FILE}`);

  const withWebsite = allFirms.filter(f => f.website).length;
  console.log(`With website: ${withWebsite}`);
  console.log(`With rating: ${allFirms.filter(f => f.rating).length}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
