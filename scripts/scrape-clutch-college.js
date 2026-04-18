#!/usr/bin/env node
/**
 * Clutch.co College / Education Consultants Scraper
 *
 * Scrapes multiple Clutch.co education-related categories using Puppeteer + stealth plugin
 * (Cloudflare blocks plain HTTP requests).
 *
 * Categories tried:
 *   - https://clutch.co/consulting/education
 *   - https://clutch.co/tutoring
 *   - https://clutch.co/consulting/college-admissions
 *
 * Strategy:
 *   1. Load listing pages for each category URL (?page=N) with stealth Puppeteer
 *   2. Extract firm name, profile URL, location, website, rating from listing cards
 *   3. Visit each profile page for phone number + detailed location
 *   4. Parse location into city/state/country
 *   5. Dedup by firm name + city across all categories
 *   6. Output CSV
 *
 * Usage:
 *   node scripts/scrape-clutch-college.js --test    # Page 1 only per category, first 5 profiles
 *   node scripts/scrape-clutch-college.js --all     # All pages + all profiles
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const CATEGORY_URLS = [
  'https://clutch.co/consulting/education',
  'https://clutch.co/tutoring',
  'https://clutch.co/consulting/college-admissions',
];

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const PROFILE_DELAY_MIN = 1500;
const PROFILE_DELAY_MAX = 2500;
const PAGE_TIMEOUT = 60000;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-consultants-clutch.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'clutch-college-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── US State Abbreviations ───────────────────────────────────────────────────

const US_STATES = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
  'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
  'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
  'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
  'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
  'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
  'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
  'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
  'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
  'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
  'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
};

// Two-letter abbreviation → state (already abbreviated)
const US_STATE_ABBREVS = new Set(Object.values(US_STATES));
US_STATE_ABBREVS.add('DC');

// Canadian provinces
const CA_PROVINCES = new Set([
  'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
]);

// Country normalization
const COUNTRY_MAP = {
  'united states': 'US', 'usa': 'US', 'u.s.': 'US', 'u.s.a.': 'US',
  'united kingdom': 'UK', 'england': 'UK', 'scotland': 'UK', 'wales': 'UK',
  'northern ireland': 'UK', 'great britain': 'UK',
  'canada': 'CA',
  'australia': 'AU',
  'germany': 'DE', 'deutschland': 'DE',
  'france': 'FR',
  'italy': 'IT', 'italia': 'IT',
  'spain': 'ES', 'españa': 'ES',
  'netherlands': 'NL', 'holland': 'NL',
  'ireland': 'IE',
  'india': 'IN',
  'brazil': 'BR', 'brasil': 'BR',
  'mexico': 'MX', 'méxico': 'MX',
  'colombia': 'CO',
  'argentina': 'AR',
  'chile': 'CL',
  'poland': 'PL',
  'portugal': 'PT',
  'romania': 'RO',
  'turkey': 'TR', 'türkiye': 'TR',
  'israel': 'IL',
  'south africa': 'ZA',
  'united arab emirates': 'AE', 'uae': 'AE',
  'singapore': 'SG',
  'hong kong': 'HK',
  'japan': 'JP',
  'south korea': 'KR', 'korea': 'KR',
  'china': 'CN',
  'thailand': 'TH',
  'vietnam': 'VN',
  'philippines': 'PH',
  'indonesia': 'ID',
  'malaysia': 'MY',
  'new zealand': 'NZ',
  'nigeria': 'NG',
  'kenya': 'KE',
  'egypt': 'EG',
  'pakistan': 'PK',
  'bangladesh': 'BD',
  'ukraine': 'UA',
  'czech republic': 'CZ', 'czechia': 'CZ',
  'hungary': 'HU',
  'austria': 'AT',
  'switzerland': 'CH',
  'belgium': 'BE',
  'sweden': 'SE',
  'norway': 'NO',
  'denmark': 'DK',
  'finland': 'FI',
  'greece': 'GR',
  'croatia': 'HR',
  'serbia': 'RS',
  'bulgaria': 'BG',
  'peru': 'PE',
  'costa rica': 'CR',
  'panama': 'PA',
  'dominican republic': 'DO',
  'jamaica': 'JM',
  'trinidad and tobago': 'TT',
  'qatar': 'QA',
  'saudi arabia': 'SA',
  'bahrain': 'BH',
  'kuwait': 'KW',
  'oman': 'OM',
  'jordan': 'JO',
  'lebanon': 'LB',
  'morocco': 'MA',
  'tunisia': 'TN',
  'ghana': 'GH',
  'sri lanka': 'LK',
  'nepal': 'NP',
  'cambodia': 'KH',
  'myanmar': 'MM',
  'taiwan': 'TW',
  'slovakia': 'SK',
  'slovenia': 'SI',
  'lithuania': 'LT',
  'latvia': 'LV',
  'estonia': 'EE',
  'cyprus': 'CY',
  'malta': 'MT',
  'luxembourg': 'LU',
  'iceland': 'IS',
  'uruguay': 'UY',
  'ecuador': 'EC',
  'venezuela': 'VE',
  'bolivia': 'BO',
  'paraguay': 'PY',
  'guatemala': 'GT',
  'honduras': 'HN',
  'el salvador': 'SV',
  'nicaragua': 'NI',
  'cuba': 'CU',
  'puerto rico': 'PR',
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay(min = DELAY_MIN, max = DELAY_MAX) {
  return sleep(min + Math.random() * (max - min));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
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

function dedupKey(firmName, city) {
  return `${(firmName || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

/**
 * Parse a Clutch.co location string like "Cheyenne, WY" or "Firenze, Italy"
 * into { city, state, country }.
 */
function parseLocation(locStr) {
  if (!locStr) return { city: '', state: '', country: '' };

  const parts = locStr.split(',').map(s => s.trim()).filter(Boolean);

  if (parts.length === 0) return { city: '', state: '', country: '' };

  if (parts.length === 1) {
    // Could be just a country or just a city
    const lower = parts[0].toLowerCase();
    if (COUNTRY_MAP[lower]) {
      return { city: '', state: '', country: COUNTRY_MAP[lower] };
    }
    return { city: parts[0], state: '', country: '' };
  }

  if (parts.length === 2) {
    const city = parts[0];
    const second = parts[1].trim();

    // Check if second part is a US state abbreviation
    if (US_STATE_ABBREVS.has(second.toUpperCase())) {
      return { city, state: second.toUpperCase(), country: 'US' };
    }

    // Check if second part is a US state name
    if (US_STATES[second.toLowerCase()]) {
      return { city, state: US_STATES[second.toLowerCase()], country: 'US' };
    }

    // Check if second part is a Canadian province
    if (CA_PROVINCES.has(second.toUpperCase())) {
      return { city, state: second.toUpperCase(), country: 'CA' };
    }

    // Check if second part is a country
    if (COUNTRY_MAP[second.toLowerCase()]) {
      return { city, state: '', country: COUNTRY_MAP[second.toLowerCase()] };
    }

    // Default: treat second as country name (pass through)
    return { city, state: '', country: second };
  }

  // 3+ parts: "City, State/Province, Country"
  if (parts.length >= 3) {
    const city = parts[0];
    const stateOrRegion = parts[1].trim();
    const countryRaw = parts[parts.length - 1].trim();
    const country = COUNTRY_MAP[countryRaw.toLowerCase()] || countryRaw;

    // Check if middle part is a US state
    if (US_STATE_ABBREVS.has(stateOrRegion.toUpperCase())) {
      return { city, state: stateOrRegion.toUpperCase(), country: country || 'US' };
    }
    if (US_STATES[stateOrRegion.toLowerCase()]) {
      return { city, state: US_STATES[stateOrRegion.toLowerCase()], country: country || 'US' };
    }

    return { city, state: stateOrRegion, country };
  }

  return { city: '', state: '', country: '' };
}

/**
 * Extract a clean website URL from a Clutch redirect URL.
 */
function extractWebsiteFromRedirect(redirectUrl) {
  if (!redirectUrl) return '';
  try {
    const match = redirectUrl.match(/[&?]u=([^&]+)/);
    if (match) {
      let url = decodeURIComponent(match[1]);
      // Strip utm params
      try {
        const u = new URL(url);
        for (const key of [...u.searchParams.keys()]) {
          if (key.startsWith('utm_')) u.searchParams.delete(key);
        }
        url = u.toString();
        // Remove trailing ? if no params left
        if (url.endsWith('?')) url = url.slice(0, -1);
      } catch {}
      return url;
    }
  } catch {}
  return '';
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedPages: [], leads: [], profilesDone: [] };
}

function saveProgress(data) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    ...data,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Scraping Functions ───────────────────────────────────────────────────────

/**
 * Extract all provider listings from a listing page.
 */
async function extractListings(page) {
  return page.evaluate(() => {
    const cards = document.querySelectorAll('.provider-row');
    return Array.from(cards).map(card => {
      const titleLink = card.querySelector('.provider__title-link');
      const locationEl = card.querySelector('.provider__highlights-item.location');
      const ratingEl = card.querySelector('.sg-rating__number');
      const reviewsEl = card.querySelector('.sg-rating__reviews');
      const employeesEl = card.querySelector('.provider__highlights-item.employees-count');
      const minProjectEl = card.querySelector('.provider__highlights-item.min-project-size');
      const hourlyRateEl = card.querySelector('.provider__highlights-item.hourly-rate');
      const visitWebsite = card.querySelector('a[href*="r.clutch.co/redirect"]');
      const servicesEls = card.querySelectorAll('.provider__services-list-item');

      // Extract services
      const services = Array.from(servicesEls).map(s => s.textContent.trim()).filter(Boolean);

      // Get review count as number
      let reviewCount = 0;
      if (reviewsEl) {
        const match = reviewsEl.textContent.match(/(\d+)/);
        if (match) reviewCount = parseInt(match[1], 10);
      }

      return {
        name: titleLink ? titleLink.textContent.trim() : '',
        profileUrl: titleLink ? titleLink.href : '',
        location: locationEl ? locationEl.textContent.trim() : '',
        rating: ratingEl ? ratingEl.textContent.trim() : '',
        reviewCount,
        employees: employeesEl ? employeesEl.textContent.trim() : '',
        minProject: minProjectEl ? minProjectEl.textContent.trim() : '',
        hourlyRate: hourlyRateEl ? hourlyRateEl.textContent.trim() : '',
        websiteRedirectUrl: visitWebsite ? visitWebsite.href : '',
        services,
        pid: card.dataset.clutchPid || '',
      };
    });
  });
}

/**
 * Visit a profile page and extract phone number + detailed location.
 */
async function enrichFromProfile(page, profileUrl) {
  try {
    const resp = await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });
    if (!resp || resp.status() !== 200) {
      return {};
    }

    return page.evaluate(() => {
      // Phone
      const phoneLinks = document.querySelectorAll('a[href^="tel:"]');
      const phones = Array.from(phoneLinks).map(a => a.textContent.trim()).filter(Boolean);

      // Website — extract from profile page redirect link
      const websiteLink = document.querySelector('a[data-link_type="website"], a.website-link__item');
      let profileWebsite = '';
      if (websiteLink && websiteLink.href) {
        const uMatch = websiteLink.href.match(/[&?]u=([^&]+)/);
        if (uMatch) {
          profileWebsite = decodeURIComponent(uMatch[1]);
        } else if (websiteLink.href.indexOf('clutch.co') === -1) {
          profileWebsite = websiteLink.href;
        }
      }
      // Fallback: look for provider_website param
      if (!profileWebsite) {
        const allLinks = document.querySelectorAll('a[href*="provider_website="]');
        for (const a of allLinks) {
          const m = a.href.match(/provider_website=([^&]+)/);
          if (m) {
            profileWebsite = 'https://' + decodeURIComponent(m[1]);
            break;
          }
        }
      }

      // Location
      const locEls = document.querySelectorAll('.profile-summary__detail');
      let detailedLocation = '';
      for (const el of locEls) {
        const titleEl = el.querySelector('.profile-summary__detail-title');
        if (titleEl && titleEl.textContent.trim().toLowerCase().includes('location')) {
          // Get the location text after the title
          const valueEl = el.querySelector('.profile-summary__detail-value');
          if (valueEl) {
            detailedLocation = valueEl.textContent.trim();
          }
        }
      }

      // Tagline
      const taglineEl = document.querySelector('.profile-summary__tagline');

      return {
        phone: phones[0] || '',
        website: profileWebsite,
        detailedLocation,
        tagline: taglineEl ? taglineEl.textContent.trim() : '',
      };
    });
  } catch (e) {
    log(`  Profile error: ${e.message}`);
    return {};
  }
}

/**
 * Get total number of pages from the pagination nav.
 */
async function getTotalPages(page) {
  return page.evaluate(() => {
    const allPageLinks = document.querySelectorAll('.sg-pagination-v2-page-number');
    let maxPage = 1;
    for (const link of allPageLinks) {
      const pageNum = parseInt(link.dataset.page || link.textContent.trim(), 10);
      if (pageNum > maxPage) maxPage = pageNum;
    }
    return maxPage;
  });
}

/**
 * Convert a listing object to the standard CSV lead format.
 */
function listingToLead(listing) {
  let website = extractWebsiteFromRedirect(listing.websiteRedirectUrl);
  // Fallback to website from profile page
  if (!website && listing.profileWebsite) {
    website = listing.profileWebsite;
    // Strip utm params from profile website too
    try {
      const u = new URL(website);
      for (const key of [...u.searchParams.keys()]) {
        if (key.startsWith('utm_')) u.searchParams.delete(key);
      }
      website = u.toString();
      if (website.endsWith('?')) website = website.slice(0, -1);
    } catch {}
  }
  const { city, state, country } = parseLocation(listing.location);

  return {
    first_name: '',
    last_name: '',
    firm_name: listing.name || '',
    title: '',
    email: '',
    phone: formatPhone(listing.phone || ''),
    website,
    domain: extractDomain(website),
    city,
    state,
    country,
    niche: 'education',
    source: 'clutch.co',
    profile_url: listing.profileUrl || '',
  };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-clutch-college.js --test    # Page 1 only per category, first 5 profiles');
    console.log('  node scripts/scrape-clutch-college.js --all     # All pages + all profiles');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('Clutch.co College / Education Consultants Scraper');
  log(`Mode: ${isTest ? 'TEST (page 1 per category, 5 profiles)' : 'ALL pages'}`);
  log(`Categories: ${CATEGORY_URLS.length}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Launch browser
  log('Launching stealth browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  const allLeads = [];
  const seenKeys = new Set();
  let totalListings = 0;
  let profilesEnriched = 0;
  let profileErrors = 0;
  const allListings = [];
  const categoryStats = {};

  try {
    // ── Phase 1: Collect listings from all category pages ──

    log('Phase 1: Collecting listings from directory pages...');
    log('');

    for (let catIdx = 0; catIdx < CATEGORY_URLS.length; catIdx++) {
      const categoryUrl = CATEGORY_URLS[catIdx];
      log(`=== Category ${catIdx + 1}/${CATEGORY_URLS.length}: ${categoryUrl} ===`);
      log('');

      let categoryListingCount = 0;

      // Load page 1 to discover total pages (and check if category exists)
      try {
        log(`Loading page 1: ${categoryUrl}`);
        const resp = await page.goto(categoryUrl, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });

        // Check for 404 or other errors
        if (!resp || resp.status() === 404) {
          log(`  Category returned 404 — skipping.`);
          log('');
          categoryStats[categoryUrl] = { pages: 0, listings: 0, status: '404' };
          await randomDelay();
          continue;
        }

        if (resp.status() !== 200) {
          log(`  Category returned HTTP ${resp.status()} — skipping.`);
          log('');
          categoryStats[categoryUrl] = { pages: 0, listings: 0, status: `${resp.status()}` };
          await randomDelay();
          continue;
        }

        // Check if page has any provider rows (category might exist but be empty)
        const hasProviders = await page.evaluate(() => {
          return document.querySelectorAll('.provider-row').length > 0;
        });

        if (!hasProviders) {
          log(`  No provider listings found on this category page — skipping.`);
          log('');
          categoryStats[categoryUrl] = { pages: 0, listings: 0, status: 'empty' };
          await randomDelay();
          continue;
        }

        const totalPages = await getTotalPages(page);
        const pagesToScrape = isTest ? 1 : totalPages;
        log(`  Discovered ${totalPages} total pages. Scraping: ${pagesToScrape}`);
        log('');

        for (let pageNum = 1; pageNum <= pagesToScrape; pageNum++) {
          const url = pageNum === 1 ? categoryUrl : `${categoryUrl}?page=${pageNum}`;
          log(`  [Page ${pageNum}/${pagesToScrape}] ${url}`);

          if (pageNum > 1) {
            await page.goto(url, { waitUntil: 'networkidle2', timeout: PAGE_TIMEOUT });
          }

          const listings = await extractListings(page);
          log(`    Found ${listings.length} listings`);

          let newOnPage = 0;
          for (const listing of listings) {
            const key = dedupKey(listing.name, '');
            if (seenKeys.has(key)) continue;
            seenKeys.add(key);
            allListings.push(listing);
            newOnPage++;
          }

          log(`    New (deduped): ${newOnPage}`);
          totalListings += listings.length;
          categoryListingCount += listings.length;

          // Save intermediate CSV after each page
          if (allListings.length > 0) {
            const tempLeads = allListings.map(l => listingToLead(l));
            writeCSV(tempLeads, OUT_FILE);
          }

          if (pageNum < pagesToScrape) {
            await randomDelay();
          }
        }

        categoryStats[categoryUrl] = { pages: pagesToScrape, listings: categoryListingCount, status: 'ok' };
        log('');
        log(`  Category complete: ${categoryListingCount} listings scraped`);

      } catch (e) {
        log(`  Category error: ${e.message}`);
        categoryStats[categoryUrl] = { pages: 0, listings: 0, status: `error: ${e.message}` };
      }

      log('');

      // Delay between categories
      if (catIdx < CATEGORY_URLS.length - 1) {
        await randomDelay();
      }
    }

    log(`Phase 1 complete: ${allListings.length} unique listings from ${totalListings} total across ${CATEGORY_URLS.length} categories`);
    log('');

    // ── Phase 2: Enrich from profile pages (phone number) ──

    const listingsToEnrich = isTest ? allListings.slice(0, 5) : allListings;
    log(`Phase 2: Enriching ${listingsToEnrich.length} profiles for phone numbers...`);
    log('');

    for (let i = 0; i < listingsToEnrich.length; i++) {
      const listing = listingsToEnrich[i];
      log(`  [${i + 1}/${listingsToEnrich.length}] ${listing.name} -> ${listing.profileUrl}`);

      try {
        const profileData = await enrichFromProfile(page, listing.profileUrl);
        if (profileData.phone) {
          listing.phone = profileData.phone;
          log(`    Phone: ${profileData.phone}`);
        }
        if (profileData.website && !listing.websiteRedirectUrl) {
          listing.profileWebsite = profileData.website;
          log(`    Website: ${profileData.website}`);
        }
        if (profileData.detailedLocation && !listing.location) {
          listing.location = profileData.detailedLocation;
        }
        profilesEnriched++;
      } catch (e) {
        log(`    Error: ${e.message}`);
        profileErrors++;
      }

      // Save progress every 20 profiles
      if ((i + 1) % 20 === 0) {
        const tempLeads = listingsToEnrich.slice(0, i + 1).map(l => listingToLead(l));
        writeCSV(tempLeads, OUT_FILE);
        log(`    [Progress saved: ${i + 1} profiles]`);
      }

      await randomDelay(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX);
    }

    // Build final leads
    for (const listing of listingsToEnrich) {
      const lead = listingToLead(listing);
      allLeads.push(lead);
    }

    // Also add non-enriched listings (if test mode limited enrichment)
    if (isTest && allListings.length > 5) {
      for (let i = 5; i < allListings.length; i++) {
        allLeads.push(listingToLead(allListings[i]));
      }
    }

  } catch (e) {
    log(`FATAL ERROR: ${e.message}`);
  } finally {
    await browser.close();
    log('Browser closed.');
  }

  // ── Write final output ──

  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withCity = allLeads.filter(l => l.city).length;
  const withCountry = allLeads.filter(l => l.country).length;
  const countries = {};
  for (const l of allLeads) {
    if (l.country) countries[l.country] = (countries[l.country] || 0) + 1;
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log('');
  log('Category results:');
  for (const [url, stats] of Object.entries(categoryStats)) {
    log(`  ${url}: ${stats.status} (${stats.listings} listings, ${stats.pages} pages)`);
  }
  log('');
  log(`Total unique firms: ${allLeads.length}`);
  log(`Profiles enriched: ${profilesEnriched} (${profileErrors} errors)`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With city: ${withCity}`);
  log(`With country: ${withCountry}`);
  log('Countries:');
  for (const [country, count] of Object.entries(countries).sort((a, b) => b[1] - a[1])) {
    log(`  ${country}: ${count}`);
  }
  log(`Output: ${OUT_FILE}`);

  // Clean up progress
  try { fs.unlinkSync(PROGRESS_FILE); } catch {}
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
