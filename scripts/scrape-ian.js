#!/usr/bin/env node
/**
 * Immigration Advocates Network (IAN) Legal Services Directory Scraper
 *
 * Scrapes https://www.immigrationadvocates.org/nonprofit/legaldirectory/
 * to extract nonprofit immigration legal services organizations across all US states.
 *
 * Strategy:
 *   1. For each US state, fetch the search results page
 *   2. Paginate through all result pages (20 per page)
 *   3. For each org, fetch the detail page to extract contact info
 *   4. Dedup by organization name + state
 *   5. Output CSV
 *
 * Data available: org name, address, phone, fax, website, services, languages, populations served
 *
 * Output: output/us-immigration-advocates-network.csv
 *
 * Usage:
 *   node scripts/scrape-ian.js                    # All 51 states+DC
 *   node scripts/scrape-ian.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-ian.js --test             # Test mode (3 states, no detail pages)
 *   node scripts/scrape-ian.js --skip-details     # Skip detail page fetching
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// --- Config ---
const BASE_URL = 'https://www.immigrationadvocates.org';
const SEARCH_PATH = '/nonprofit/legaldirectory/search';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-advocates-network.csv');
const DELAY_MS = 1500;
const DETAIL_DELAY_MS = 1000;
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'fax', 'street_address', 'zip', 'services', 'languages'
];

const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC'
];

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const skipDetails = args.includes('--skip-details');
const statesIdx = args.indexOf('--states');
const statesFilter = statesIdx >= 0
  ? args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase())
  : null;

// --- Helpers ---
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url) {
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
        return resolve(httpGet(redir));
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function extractDomain(url) {
  if (!url) return '';
  try { return new URL(url).hostname.replace(/^www\./, ''); } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
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

// --- Scraping Functions ---

async function fetchSearchPage(state, page = 1) {
  const url = `${BASE_URL}${SEARCH_PATH}?state=${state}&national=0&page=${page}`;
  const resp = await httpGet(url);
  if (resp.status !== 200) {
    console.error(`  [WARN] ${state} page ${page}: HTTP ${resp.status}`);
    return { orgs: [], hasNext: false, total: 0 };
  }

  const $ = cheerio.load(resp.body);
  const orgs = [];

  // Parse total count
  const showingText = $('span.shown').text(); // "Showing Results 1 - 20 of 214"
  const totalMatch = showingText.match(/of\s+(\d+)/);
  const total = totalMatch ? parseInt(totalMatch[1]) : 0;

  // Parse organization listings
  $('div.directory-organization-summary').each((i, el) => {
    const $el = $(el);
    const nameLink = $el.find('a').first();
    const name = nameLink.text().trim();
    const detailPath = nameLink.attr('href') || '';
    const profileUrl = detailPath ? `${BASE_URL}${detailPath}` : '';

    // Basic address info from summary
    const addressText = $el.find('.adr').text().trim();
    const cityMatch = addressText.match(/([^,]+),\s*([A-Z]{2})/);

    orgs.push({
      firm_name: name,
      profile_url: profileUrl,
      detail_path: detailPath,
      city: cityMatch ? cityMatch[1].trim() : '',
      state: state,
      country: 'US',
    });
  });

  // Check for next page
  const hasNext = $('a.next').length > 0;

  return { orgs, hasNext, total };
}

async function fetchOrgDetail(org) {
  if (!org.detail_path) return org;

  const url = `${BASE_URL}${org.detail_path}`;
  try {
    const resp = await httpGet(url);
    if (resp.status !== 200) return org;

    const $ = cheerio.load(resp.body);
    const detail = $('#directory-organization-details, #organization-detail-contact').first();

    // Address
    const street = $('div.street-address').first().text().trim();
    const extended = $('div.extended-address').first().text().trim();
    const city = $('span.locality').first().text().trim();
    const stateAbbr = $('abbr.region').first().attr('title') || $('abbr.region').first().text().trim();
    const zip = $('span.postal-code').first().text().trim();

    // Contact info
    let phone = '';
    let fax = '';
    let website = '';
    let email = '';

    // Parse the detail table/list
    const html = resp.body;

    // Phone
    const phoneMatch = html.match(/<strong>Phone:<\/strong><br\/>\s*([^<]+)/);
    if (phoneMatch) phone = cleanPhone(phoneMatch[1]);

    // Fax
    const faxMatch = html.match(/<strong>Fax:<\/strong><br\/>\s*([^<]+)/);
    if (faxMatch) fax = cleanPhone(faxMatch[1]);

    // Website
    const websiteMatch = html.match(/class="url external[^"]*"\s*>([^<]+)/);
    if (websiteMatch) {
      website = websiteMatch[1].trim();
      if (!website.startsWith('http')) website = 'https://' + website;
    }
    // Also try href
    const websiteHrefMatch = html.match(/<strong>Website:<\/strong><br \/>\s*<a[^>]*href="([^"]+)"/);
    if (websiteHrefMatch && !website) {
      website = websiteHrefMatch[1].trim();
    }

    // Email
    const emailMatch = html.match(/<strong>Email:<\/strong><br\/>\s*<a[^>]*href="mailto:([^"]+)"/);
    if (emailMatch) email = emailMatch[1].trim();

    // Services
    const servicesSection = html.match(/Legal Services Provided[\s\S]*?<ul[^>]*>([\s\S]*?)<\/ul>/);
    let services = '';
    if (servicesSection) {
      const serviceItems = servicesSection[1].match(/<li[^>]*>([^<]+)<\/li>/g);
      if (serviceItems) {
        services = serviceItems.map(s => s.replace(/<[^>]+>/g, '').trim()).join('; ');
      }
    }

    // Languages
    const langSection = html.match(/Languages[\s\S]*?<ul[^>]*>([\s\S]*?)<\/ul>/);
    let languages = '';
    if (langSection) {
      const langItems = langSection[1].match(/<li[^>]*>([^<]+)<\/li>/g);
      if (langItems) {
        languages = langItems.map(s => s.replace(/<[^>]+>/g, '').trim()).join('; ');
      }
    }

    Object.assign(org, {
      street_address: [street, extended].filter(Boolean).join(', '),
      city: city || org.city,
      state: org.state,
      zip,
      phone,
      fax,
      website,
      email,
      domain: extractDomain(website),
      services,
      languages,
    });
  } catch (err) {
    console.error(`  [WARN] Detail fetch failed for ${org.firm_name}: ${err.message}`);
  }

  return org;
}

// --- Main ---
async function main() {
  console.log('=== Immigration Advocates Network (IAN) Scraper ===');
  console.log(`Mode: ${testMode ? 'TEST' : 'FULL'}, Skip details: ${skipDetails}`);

  const states = statesFilter || (testMode ? ['CA', 'NY', 'TX'] : ALL_STATES);
  console.log(`States: ${states.join(', ')}\n`);

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const allOrgs = [];
  const seen = new Set();

  for (let si = 0; si < states.length; si++) {
    const state = states[si];
    console.log(`[${si + 1}/${states.length}] ${state} ...`);

    let page = 1;
    let stateOrgs = [];
    let total = 0;

    while (true) {
      const result = await fetchSearchPage(state, page);
      if (page === 1) {
        total = result.total;
        console.log(`  Found ${total} organizations`);
      }

      for (const org of result.orgs) {
        const key = `${org.firm_name}|${org.state}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        stateOrgs.push(org);
      }

      if (!result.hasNext || (testMode && page >= 2)) break;
      page++;
      await sleep(DELAY_MS);
    }

    // Fetch detail pages
    if (!skipDetails && !testMode) {
      console.log(`  Fetching ${stateOrgs.length} detail pages...`);
      for (let i = 0; i < stateOrgs.length; i++) {
        stateOrgs[i] = await fetchOrgDetail(stateOrgs[i]);
        if ((i + 1) % 10 === 0) {
          console.log(`    ${i + 1}/${stateOrgs.length} details fetched`);
        }
        await sleep(DETAIL_DELAY_MS);
      }
    }

    // Format for output
    for (const org of stateOrgs) {
      allOrgs.push({
        first_name: '',
        last_name: '',
        firm_name: org.firm_name,
        title: 'Immigration Legal Services',
        email: org.email || '',
        phone: org.phone || '',
        website: org.website || '',
        domain: org.domain || '',
        city: org.city || '',
        state: org.state,
        country: 'US',
        niche: 'immigration',
        source: 'immigration_advocates_network',
        profile_url: org.profile_url || '',
        fax: org.fax || '',
        street_address: org.street_address || '',
        zip: org.zip || '',
        services: org.services || '',
        languages: org.languages || '',
      });
    }

    console.log(`  ${state}: ${stateOrgs.length} orgs (running total: ${allOrgs.length})`);
    await sleep(DELAY_MS);
  }

  // Write CSV
  const csvLines = [CSV_HEADERS.join(',')];
  for (const org of allOrgs) {
    csvLines.push(csvRow(org));
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  console.log(`\n=== DONE ===`);
  console.log(`Total organizations: ${allOrgs.length}`);
  console.log(`Output: ${OUTPUT_FILE}`);

  // Stats
  const withPhone = allOrgs.filter(o => o.phone).length;
  const withWebsite = allOrgs.filter(o => o.website).length;
  const withEmail = allOrgs.filter(o => o.email).length;
  console.log(`With phone: ${withPhone}`);
  console.log(`With website: ${withWebsite}`);
  console.log(`With email: ${withEmail}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
