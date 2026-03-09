#!/usr/bin/env node
/**
 * FindLaw Immigration Lawyers Scraper
 *
 * Scrapes https://lawyers.findlaw.com/immigration-naturalization-law/{state}/{city}/
 * using Puppeteer + JSON-LD structured data (CollectionPage → LegalService items).
 *
 * Strategy:
 *   1. For each state, load the state page to discover city links
 *   2. For each city, extract JSON-LD items (20 per page) + paginate
 *   3. JSON-LD gives: firm name, phone, website, address (street, city, state, zip)
 *   4. Parse firm names to extract person names where possible
 *   5. Dedup by profile URL
 *   6. Output CSV
 *
 * FindLaw lists LAW FIRMS, not individuals. Firm names may contain attorney names
 * (e.g., "John Smith, Attorney at Law"). The parser attempts to extract person
 * names from solo practitioner firm names.
 *
 * Usage:
 *   node scripts/scrape-findlaw-immigration.js                    # All 50 states + DC
 *   node scripts/scrape-findlaw-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-findlaw-immigration.js --test             # Test mode (3 states, 3 cities each)
 *   node scripts/scrape-findlaw-immigration.js --append           # Append to existing CSV instead of overwriting
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const DELAY_BETWEEN_STATES = 4000;
const NAV_TIMEOUT = 30000;
const MAX_PAGES_PER_CITY = 20;

const STATE_SLUGS = {
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

const STATE_CODES = {};
for (const [code, slug] of Object.entries(STATE_SLUGS)) {
  STATE_CODES[slug] = code;
}

// Ordered by immigration market size
const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'street_address', 'city', 'state', 'zip',
  'country', 'niche', 'source', 'profile_url',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

/**
 * Parse a FindLaw firm name to extract person name if it's a solo practitioner.
 * FindLaw lists firms, not individuals. Many are "FirstName LastName, Attorney at Law"
 * or "Law Offices of FirstName LastName".
 */
function parseFirmName(firmName) {
  if (!firmName) return { first_name: '', last_name: '', firm_name: firmName, is_firm: true };

  const cleaned = firmName
    .replace(/&amp;/g, '&')
    .replace(/,?\s*(LLC|LLP|PLLC|PA|P\.?A\.?|PC|P\.?C\.?|Inc\.?|Attorney(s)?(\s+at\s+Law)?|Lawyer|Law\s*(Firm|Group|Office|Offices|Center|Practice))\.?\s*$/i, '')
    .trim();

  // Multi-partner firm: contains & or "and"
  if (cleaned.includes('&') || /\band\b/i.test(cleaned)) {
    return { first_name: '', last_name: '', firm_name: firmName.replace(/&amp;/g, '&'), is_firm: true };
  }

  // "Law Offices of John Smith" or "Law Office of John Smith"
  const lawOfficeMatch = cleaned.match(/^Law\s+Offices?\s+of\s+(.+)$/i);
  if (lawOfficeMatch) {
    const namePart = lawOfficeMatch[1].trim();
    const parts = namePart.split(/\s+/);
    if (parts.length === 2) {
      return { first_name: parts[0], last_name: parts[1], firm_name: firmName.replace(/&amp;/g, '&'), is_firm: false };
    }
    if (parts.length === 3) {
      return { first_name: parts[0], last_name: parts[2], firm_name: firmName.replace(/&amp;/g, '&'), is_firm: false };
    }
    return { first_name: '', last_name: '', firm_name: firmName.replace(/&amp;/g, '&'), is_firm: true };
  }

  const parts = cleaned.split(/\s+/);

  // "The Smith Firm" or starts with common firm words
  if (/^(the|law|legal|american|national|global|immigration|visa)\b/i.test(parts[0])) {
    return { first_name: '', last_name: '', firm_name: firmName.replace(/&amp;/g, '&'), is_firm: true };
  }

  // 2-word name: likely "First Last"
  if (parts.length === 2) {
    return { first_name: parts[0], last_name: parts[1], firm_name: '', is_firm: false };
  }

  // 3-word name: could be "First M. Last"
  if (parts.length === 3) {
    // If middle part is an initial (single letter or letter + period)
    if (parts[1].length <= 2 || /^[A-Z]\.?$/i.test(parts[1])) {
      return { first_name: parts[0], last_name: parts[2], firm_name: '', is_firm: false };
    }
    // Could still be a name like "Maria Del Carmen" - treat as name
    return { first_name: parts[0], last_name: parts[2], firm_name: '', is_firm: false };
  }

  // 4+ words: treat as firm
  return { first_name: '', last_name: '', firm_name: firmName.replace(/&amp;/g, '&'), is_firm: true };
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val);
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

function appendCSV(leads, outPath) {
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  if (rows.length > 0) {
    fs.appendFileSync(outPath, rows.join('\n') + '\n');
  }
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

/**
 * Get city links from a FindLaw state page.
 * State pages list all cities that have immigration lawyers.
 */
async function getCityLinks(page, stateSlug) {
  return page.evaluate((slug) => {
    const links = [];
    const seen = new Set();
    document.querySelectorAll('a').forEach(a => {
      const re = new RegExp(`/immigration-naturalization-law/${slug}/([a-z][a-z0-9-]+)/?$`);
      const m = a.href.match(re);
      if (m && !seen.has(m[1])) {
        seen.add(m[1]);
        links.push({
          city: a.textContent.trim(),
          slug: m[1],
          href: a.href.replace(/\/?$/, '/'),
        });
      }
    });
    return links;
  }, stateSlug);
}

/**
 * Extract JSON-LD items from a FindLaw city page.
 * Returns { items: LegalService[], total: number, hasNext: boolean }
 */
async function extractPageData(page) {
  return page.evaluate(() => {
    const result = { items: [], total: 0, hasNext: false };

    // Find JSON-LD
    document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
      try {
        const json = JSON.parse(s.textContent);
        if (json['@type'] === 'CollectionPage' && json.mainEntity) {
          result.total = parseInt(json.mainEntity.numberOfItems || '0', 10);
          result.items = (json.mainEntity.itemListElement || []).filter(
            item => item['@type'] === 'LegalService'
          );
        }
      } catch(e) {}
    });

    // Check for Next page
    document.querySelectorAll('a[href*="page="]').forEach(a => {
      if (a.textContent.trim().toLowerCase() === 'next' ||
          a.getAttribute('aria-label')?.toLowerCase() === 'next') {
        result.hasNext = true;
      }
    });

    // Also check if there are higher page numbers
    const currentPage = parseInt(new URL(window.location.href).searchParams.get('page') || '1');
    document.querySelectorAll('a[href*="page="]').forEach(a => {
      const m = a.href.match(/page=(\d+)/);
      if (m && parseInt(m[1]) > currentPage) {
        result.hasNext = true;
      }
    });

    return result;
  });
}

/**
 * Scrape all pages of a city on FindLaw.
 */
async function scrapeCityPages(page, cityInfo, stateCode, maxPages) {
  const allItems = [];
  let totalItems = 0;

  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    const url = pageNum === 1
      ? cityInfo.href
      : `${cityInfo.href}?page=${pageNum}`;

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`    ERROR navigating to ${cityInfo.city} page ${pageNum}: ${err.message}`);
      break;
    }

    // Check for Cloudflare
    const title = await page.title();
    if (title.includes('Just a moment') || title.includes('Attention Required')) {
      log(`    Cloudflare challenge on ${cityInfo.city} page ${pageNum}`);
      await sleep(5000);
      const stillBlocked = await page.evaluate(() => document.title.includes('Just a moment'));
      if (stillBlocked) {
        log(`    Cannot bypass Cloudflare for ${cityInfo.city}`);
        break;
      }
    }

    const data = await extractPageData(page);

    if (pageNum === 1) {
      totalItems = data.total;
    }

    if (data.items.length === 0) break;

    allItems.push(...data.items);

    if (!data.hasNext) break;

    await randomDelay();
  }

  return { items: allItems, total: totalItems };
}

/**
 * Convert a FindLaw JSON-LD LegalService item to a lead object.
 */
function itemToLead(item, cityName, stateCode) {
  const rawName = (item.name || '').replace(/&amp;/g, '&');
  const phone = item.telephone || '';
  const website = item.sameAs || '';
  const address = item.address || {};
  const profilePage = item.mainEntityOfPage || {};
  const profileUrl = profilePage['@id'] || profilePage.url || '';

  const nameInfo = parseFirmName(rawName);

  return {
    first_name: nameInfo.first_name || '',
    last_name: nameInfo.last_name || '',
    firm_name: nameInfo.is_firm ? rawName : (nameInfo.firm_name || ''),
    title: '',
    email: '',
    phone: cleanPhone(phone),
    website,
    domain: extractDomain(website),
    street_address: address.streetAddress || '',
    city: address.addressLocality || cityName,
    state: address.addressRegion || stateCode,
    zip: address.postalCode || '',
    country: address.addressCountry || 'US',
    niche: 'Immigration Law',
    source: 'findlaw',
    profile_url: profileUrl,
  };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAppend = args.includes('--append');
  let statesFilter = null;

  const statesArg = args.find(a => a.startsWith('--states='));
  if (statesArg) {
    statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
  }

  let states = statesFilter || ALL_STATES;
  const maxCitiesPerState = isTest ? 3 : 999;
  const maxPagesPerCity = isTest ? 2 : MAX_PAGES_PER_CITY;

  if (isTest && !statesFilter) {
    states = states.slice(0, 3); // CA, NY, TX in test mode
  }

  log(`FindLaw Immigration Lawyers Scraper`);
  log(`States: ${states.length} | Test mode: ${isTest} | Max cities/state: ${maxCitiesPerState}`);

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  const allLeads = [];
  const seenProfileUrls = new Set();
  let totalStatesProcessed = 0;

  for (const stateCode of states) {
    const stateSlug = STATE_SLUGS[stateCode];
    if (!stateSlug) {
      log(`Unknown state code: ${stateCode}`);
      continue;
    }

    totalStatesProcessed++;
    log(`\n[${'='.repeat(40)}]`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${stateSlug})`);

    // 1. Load state page to get city list
    const stateUrl = `https://lawyers.findlaw.com/immigration-naturalization-law/${stateSlug}`;
    try {
      await page.goto(stateUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`  ERROR loading state page for ${stateCode}: ${err.message}`);
      continue;
    }

    // Check Cloudflare
    const stateTitle = await page.title();
    if (stateTitle.includes('Just a moment')) {
      log(`  Cloudflare challenge on state page for ${stateCode}`);
      await sleep(8000);
      const stillBlocked = await page.evaluate(() => document.title.includes('Just a moment'));
      if (stillBlocked) {
        log(`  Cannot bypass Cloudflare for ${stateCode} — skipping`);
        continue;
      }
    }

    const cityLinks = await getCityLinks(page, stateSlug);
    log(`  ${stateCode}: ${cityLinks.length} cities found`);

    if (cityLinks.length === 0) {
      log(`  No cities found for ${stateCode} — skipping`);
      continue;
    }

    // 2. Scrape each city
    const citiesToScrape = cityLinks.slice(0, maxCitiesPerState);
    let stateLeadCount = 0;

    for (let ci = 0; ci < citiesToScrape.length; ci++) {
      const cityInfo = citiesToScrape[ci];
      log(`  City ${ci + 1}/${citiesToScrape.length}: ${cityInfo.city}`);

      await randomDelay();

      const { items, total } = await scrapeCityPages(page, cityInfo, stateCode, maxPagesPerCity);

      let cityNew = 0;
      for (const item of items) {
        const lead = itemToLead(item, cityInfo.city, stateCode);
        const dedupKey = lead.profile_url || `${lead.firm_name}-${lead.phone}-${lead.city}`;
        if (seenProfileUrls.has(dedupKey)) continue;
        seenProfileUrls.add(dedupKey);

        if (!lead.first_name && !lead.last_name && !lead.firm_name) continue;

        allLeads.push(lead);
        cityNew++;
        stateLeadCount++;
      }

      if (total > 0) {
        log(`    ${cityInfo.city}: ${total} total, ${items.length} scraped, ${cityNew} new`);
      }
    }

    log(`  ${stateCode} total: ${stateLeadCount} unique leads`);

    // Delay between states
    if (totalStatesProcessed < states.length) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 3000);
    }
  }

  await browser.close();

  // Write output
  const outDir = path.join(__dirname, '..', 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, 'us-immigration-lawyers-findlaw.csv');
  if (isAppend && fs.existsSync(outPath)) {
    appendCSV(allLeads, outPath);
    log(`Appended ${allLeads.length} leads to existing CSV`);
  } else {
    writeCSV(allLeads, outPath);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const firmsOnly = allLeads.filter(l => l.firm_name && !l.first_name).length;

  log(`\n${'='.repeat(60)}`);
  log(`DONE`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}/${states.length}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With person name: ${withName}`);
  log(`Firm-only (no person name): ${firmsOnly}`);
  log(`Output: ${outPath}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
