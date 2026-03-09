#!/usr/bin/env node
/**
 * HG.org Immigration Lawyers Scraper
 *
 * Scrapes https://www.hg.org/lawyers/usa/{state}/immigration
 * using Puppeteer (Cloudflare protection requires real browser).
 *
 * Strategy:
 *   1. For each state, load the state listing page
 *   2. Parse all listing cards (div.listing with Schema.org LegalService markup)
 *   3. Paginate through ?page=2, ?page=3, etc.
 *   4. Extract: firm name, phone, website, city, state, address, profile URL
 *   5. Dedup by profile URL
 *   6. Save progress incrementally to CSV
 *
 * Data available per listing card:
 *   - Firm name (h2 > a > span[itemprop="name"])
 *   - Phone (span.mainphone > a.phoneneutral[href^="tel:"])
 *   - Website (a.blueunderline[rel="nofollow"])
 *   - City/State/Zip (meta tags with Schema.org PostalAddress)
 *   - Profile URL (/attorney/{slug}/{id})
 *   - Description (p tag)
 *   - Geo coordinates (meta.latitude, meta.longitude)
 *
 * Note: HG.org lists law FIRMS, not individual attorneys. The "name" field
 * is typically a firm name (e.g., "NPZ Law Group, P.C.") but sometimes
 * an individual attorney (e.g., "Mark I. Cohen, Esq."). We attempt to
 * parse individual names when the listing appears to be a solo practitioner.
 *
 * Usage:
 *   node scripts/scrape-hg-immigration.js                    # All 51 states
 *   node scripts/scrape-hg-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-hg-immigration.js --test             # Test mode (3 states, 2 pages each)
 *   node scripts/scrape-hg-immigration.js --all-states       # Explicit all states
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const DELAY_BETWEEN_STATES = 5000;
const CLOUDFLARE_WAIT = 10000;
const MAX_CLOUDFLARE_RETRIES = 5;
const NAV_TIMEOUT = 45000;
const MAX_PAGES_PER_STATE = 50; // safety limit

// HG.org DNS is currently broken (Cloudflare Error 1016).
// We use --host-resolver-rules to point to Cloudflare's edge IP.
// If DNS comes back, this still works fine.
const HG_IP = '104.26.5.141';

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

// Ordered by immigration market size (biggest first)
const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const OUT_PATH = path.join(__dirname, '..', 'output', 'us-immigration-lawyers-hg.csv');

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

/**
 * Attempt to parse a person's first/last name from a firm/attorney name.
 * HG.org lists mostly firms, but solo practitioners show as "John Smith, Esq."
 * We try to detect individual names vs firm names.
 */
function parseName(name) {
  if (!name) return { first_name: '', last_name: '', is_firm: true };

  // Firm indicators — if these appear, it's a firm not a person
  const firmIndicators = /\b(llp|llc|pllc|p\.?c\.?|plc|inc|group|associates|partners|firm|offices|law\s+office|legal|& |and )\b/i;
  if (firmIndicators.test(name)) {
    return { first_name: '', last_name: '', is_firm: true };
  }

  // Clean suffixes
  let cleaned = name
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|phd|md|llm|ii|iii|iv|jr\.?|sr\.?|p\.?a\.?)/gi, '')
    .trim();

  // If it looks like "Last, First" format
  if (cleaned.includes(',')) {
    const [last, ...rest] = cleaned.split(',').map(s => s.trim());
    const first = rest.join(' ').split(/\s+/)[0] || '';
    if (first && last && first.length > 1 && last.length > 1) {
      return { first_name: first, last_name: last, is_firm: false };
    }
  }

  // "First [Middle] Last" format
  const parts = cleaned.split(/\s+/).filter(p => p.length > 0);
  if (parts.length >= 2 && parts.length <= 4) {
    // Check if all parts look like name parts (capitalized, no weird chars)
    const looksLikeName = parts.every(p => /^[A-Z][a-z]+\.?$/.test(p) || /^[A-Z]\.?$/.test(p));
    if (looksLikeName) {
      return {
        first_name: parts[0],
        last_name: parts[parts.length - 1],
        is_firm: false,
      };
    }
  }

  return { first_name: '', last_name: '', is_firm: true };
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

function cleanWebsite(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(p => u.searchParams.delete(p));
    return u.toString();
  } catch { return url; }
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
  const dir = path.dirname(outPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

// Map state code back from slug for leads found
function stateCodeFromSlug(slug) {
  for (const [code, s] of Object.entries(STATE_SLUGS)) {
    if (s === slug) return code;
  }
  return '';
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

async function isCloudflareChallenge(page) {
  return page.evaluate(() => {
    const title = document.title || '';
    return title.includes('Just a moment') ||
      title.includes('Attention Required') ||
      title.includes('Origin DNS error') ||
      document.querySelector('#challenge-running') !== null;
  });
}

async function waitForCloudflare(page, maxRetries = MAX_CLOUDFLARE_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    const blocked = await isCloudflareChallenge(page);
    if (!blocked) return true;
    log(`  Cloudflare/DNS challenge detected, waiting ${CLOUDFLARE_WAIT / 1000}s (attempt ${i + 1}/${maxRetries})...`);
    await sleep(CLOUDFLARE_WAIT);
  }
  return !(await isCloudflareChallenge(page));
}

/**
 * Extract listing cards from the current page using Cheerio-style DOM parsing.
 * Each card is a div.listing with Schema.org LegalService itemtype.
 *
 * Data fields:
 *   - name: span[itemprop="name"] inside h2
 *   - phone: a.phoneneutral inside span.mainphone
 *   - website: a.blueunderline with rel="nofollow"
 *   - city: meta[itemprop="addressLocality"]
 *   - region: meta[itemprop="addressRegion"]
 *   - zip: meta[itemprop="postalCode"]
 *   - street: meta[itemprop="streetAddress"]
 *   - profileUrl: h2 > a[href*="/attorney/"]
 *   - description: p text
 */
async function extractListingsFromPage(page) {
  return page.evaluate(() => {
    const listings = document.querySelectorAll('div.listing');
    const results = [];

    listings.forEach(listing => {
      // Name (firm or attorney)
      const nameEl = listing.querySelector('h2 a span[itemprop="name"]');
      const name = nameEl ? nameEl.textContent.trim() : '';
      if (!name || name.length < 2) return;

      // Profile URL
      const profileLink = listing.querySelector('h2 a');
      let profileUrl = profileLink ? profileLink.getAttribute('href') : '';
      // Clean Wayback Machine prefix if present
      if (profileUrl && profileUrl.includes('/web/')) {
        const match = profileUrl.match(/https:\/\/www\.hg\.org\/attorney\/[^\s"]+/);
        if (match) profileUrl = match[0];
      }

      // Phone
      const phoneEl = listing.querySelector('span.mainphone a.phoneneutral');
      const phone = phoneEl ? phoneEl.textContent.trim() : '';

      // Website
      const websiteEl = listing.querySelector('a.blueunderline');
      let website = websiteEl ? websiteEl.getAttribute('href') : '';
      // Clean Wayback Machine prefix
      if (website && website.includes('/web/')) {
        const match = website.match(/https?:\/\/(?!web\.archive)[^\s"]+/);
        if (match) website = match[0];
      }

      // Address (Schema.org PostalAddress)
      const cityEl = listing.querySelector('meta[itemprop="addressLocality"]');
      const regionEl = listing.querySelector('meta[itemprop="addressRegion"]');
      const zipEl = listing.querySelector('meta[itemprop="postalCode"]');
      const streetEl = listing.querySelector('meta[itemprop="streetAddress"]');

      const city = cityEl ? cityEl.getAttribute('content') : '';
      const region = regionEl ? regionEl.getAttribute('content') : '';
      const zip = zipEl ? zipEl.getAttribute('content') : '';
      const street = streetEl ? streetEl.getAttribute('content') : '';

      // Subtitle with city (fallback) — "Immigration Lawyers in Albany, NY"
      const h3 = listing.querySelector('h3');
      let subtitleCity = '';
      if (h3) {
        const m = h3.textContent.match(/in\s+(.+?),\s*([A-Z]{2})/);
        if (m) subtitleCity = m[1].trim();
      }

      // Description
      const descEl = listing.querySelector('p');
      const description = descEl ? descEl.textContent.trim() : '';

      results.push({
        name,
        profileUrl,
        phone,
        website,
        city: city || subtitleCity,
        region,
        zip,
        street,
        description,
      });
    });

    return results;
  });
}

/**
 * Check how many pages exist for this state by looking at pagination links.
 * Returns the maximum page number found.
 */
async function getMaxPage(page) {
  return page.evaluate(() => {
    let maxPage = 1;
    document.querySelectorAll('a').forEach(a => {
      const href = a.getAttribute('href') || '';
      const match = href.match(/[?&]page=(\d+)/);
      if (match) {
        const p = parseInt(match[1]);
        if (p > maxPage) maxPage = p;
      }
    });
    return maxPage;
  });
}

/**
 * Scrape all pages for a given state.
 */
async function scrapeState(page, stateCode, stateSlug, maxPages) {
  const allListings = [];
  const baseUrl = `https://www.hg.org/lawyers/usa/${stateSlug}/immigration`;

  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    const url = pageNum === 1 ? baseUrl : `${baseUrl}?page=${pageNum}`;
    log(`  Page ${pageNum}: ${url}`);

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`    Navigation error: ${err.message}`);
      // If it's a DNS error, the site is still down
      if (err.message.includes('ERR_NAME_NOT_RESOLVED') || err.message.includes('DNS')) {
        log('    Site DNS is still broken. Aborting.');
        return { listings: allListings, aborted: true, reason: 'dns' };
      }
      break;
    }

    const passed = await waitForCloudflare(page);
    if (!passed) {
      const title = await page.title();
      if (title.includes('Origin DNS error')) {
        log('    Site DNS error (Cloudflare 1016). Site is down.');
        return { listings: allListings, aborted: true, reason: 'dns' };
      }
      log(`    Blocked by Cloudflare on page ${pageNum}`);
      break;
    }

    // Check for "no results" or error pages
    const pageTitle = await page.title();
    if (pageTitle.includes('Error') || pageTitle.includes('404') || pageTitle.includes('Not Found')) {
      log(`    Page returned error: "${pageTitle}"`);
      break;
    }

    const listings = await extractListingsFromPage(page);
    if (listings.length === 0) {
      log(`    No listings found on page ${pageNum}`);
      break;
    }

    allListings.push(...listings.map(l => ({ ...l, stateCode })));
    log(`    Found ${listings.length} listings (total: ${allListings.length})`);

    // On page 1, check how many pages exist
    if (pageNum === 1) {
      const totalPages = await getMaxPage(page);
      maxPages = Math.min(maxPages, totalPages);
      log(`    Total pages available: ${totalPages} (will scrape up to ${maxPages})`);
    }

    // Don't delay after the last page
    if (pageNum < maxPages) {
      await randomDelay();
    }
  }

  return { listings: allListings, aborted: false };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllStates = args.includes('--all-states');
  let statesFilter = null;

  const statesArg = args.find(a => a.startsWith('--states'));
  if (statesArg) {
    if (statesArg.includes('=')) {
      statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
    } else {
      const idx = args.indexOf(statesArg);
      if (idx < args.length - 1) {
        statesFilter = args[idx + 1].split(',').map(s => s.trim().toUpperCase());
      }
    }
  }

  let states = statesFilter || ALL_STATES;
  const maxPagesPerState = isTest ? 2 : MAX_PAGES_PER_STATE;

  if (isTest && !statesFilter) {
    states = states.slice(0, 3); // CA, NY, TX in test mode
  }

  log('='.repeat(60));
  log('HG.org Immigration Lawyers Scraper');
  log('='.repeat(60));
  log(`States: ${states.length} | Test mode: ${isTest} | Max pages/state: ${maxPagesPerState}`);
  log(`Output: ${OUT_PATH}`);
  log('');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--window-size=1920,1080',
      // DNS override for hg.org (Cloudflare Error 1016 workaround)
      `--host-resolver-rules=MAP www.hg.org ${HG_IP},MAP hg.org ${HG_IP}`,
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Warm up session
  log('Warming up session...');
  try {
    await page.goto('https://www.hg.org/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(3000);
    const warmupOk = !(await isCloudflareChallenge(page));
    const warmupTitle = await page.title();
    log(`Session warmup: ${warmupOk ? 'OK' : 'BLOCKED'} (title: "${warmupTitle}")`);

    if (!warmupOk) {
      if (warmupTitle.includes('Origin DNS error')) {
        log('');
        log('ERROR: HG.org is currently DOWN (Cloudflare Error 1016 - Origin DNS error).');
        log('The site\'s DNS records are misconfigured. This is a server-side issue.');
        log('The scraper is correctly built but the site needs to come back online.');
        log('Try again later.');
        log('');
        await browser.close();
        process.exit(1);
      }
      log('Waiting for Cloudflare challenge to resolve...');
      await sleep(15000);
      const stillBlocked = await isCloudflareChallenge(page);
      if (stillBlocked) {
        log('Still blocked by Cloudflare. The site may be down or blocking scrapers.');
        await browser.close();
        process.exit(1);
      }
    }
  } catch (err) {
    if (err.message.includes('ERR_NAME_NOT_RESOLVED') || err.message.includes('DNS')) {
      log('');
      log('ERROR: Cannot resolve www.hg.org DNS. The site appears to be DOWN.');
      log('The scraper is correctly built but the site needs to come back online.');
      log('');
      await browser.close();
      process.exit(1);
    }
    log(`Warmup error (non-fatal): ${err.message}`);
  }

  const allLeads = [];
  const seenProfileUrls = new Set();
  let totalStatesProcessed = 0;
  let consecutiveErrors = 0;
  let siteDown = false;

  // Load existing progress if any
  if (fs.existsSync(OUT_PATH)) {
    try {
      const existing = fs.readFileSync(OUT_PATH, 'utf-8').trim().split('\n');
      if (existing.length > 1) {
        log(`Found existing output with ${existing.length - 1} leads, will append new ones`);
        // Parse existing profile URLs for dedup
        const headerCols = existing[0].split(',');
        const profileIdx = headerCols.indexOf('profile_url');
        if (profileIdx >= 0) {
          for (let i = 1; i < existing.length; i++) {
            // Simple CSV parse for profile_url
            const cols = existing[i].split(',');
            if (cols[profileIdx]) {
              seenProfileUrls.add(cols[profileIdx].replace(/"/g, ''));
            }
          }
        }
        log(`Loaded ${seenProfileUrls.size} existing profile URLs for dedup`);
      }
    } catch (err) {
      log(`Could not load existing output: ${err.message}`);
    }
  }

  for (const stateCode of states) {
    if (siteDown) break;

    const stateSlug = STATE_SLUGS[stateCode];
    if (!stateSlug) {
      log(`Unknown state code: ${stateCode}`);
      continue;
    }

    totalStatesProcessed++;
    log('');
    log(`[${'='.repeat(50)}]`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${stateSlug})`);

    // Cool down after consecutive errors
    if (consecutiveErrors >= 3) {
      log(`  ${consecutiveErrors} consecutive errors — pausing 30s...`);
      await sleep(30000);
      consecutiveErrors = 0;
    }

    const { listings, aborted, reason } = await scrapeState(page, stateCode, stateSlug, maxPagesPerState);

    if (aborted && reason === 'dns') {
      log('');
      log('SITE IS DOWN — HG.org DNS is not resolving.');
      log('Cannot continue scraping. Try again when the site comes back online.');
      siteDown = true;
      break;
    }

    if (listings.length === 0) {
      consecutiveErrors++;
    } else {
      consecutiveErrors = 0;
    }

    let stateNewCount = 0;
    for (const listing of listings) {
      // Dedup by profile URL
      if (listing.profileUrl && seenProfileUrls.has(listing.profileUrl)) continue;
      if (listing.profileUrl) seenProfileUrls.add(listing.profileUrl);

      // Parse name — HG.org lists mostly firms, but some are individual attorneys
      const { first_name, last_name, is_firm } = parseName(listing.name);

      allLeads.push({
        first_name,
        last_name,
        firm_name: listing.name, // Always store the full name as firm_name
        title: '',
        email: '',
        phone: cleanPhone(listing.phone),
        website: cleanWebsite(listing.website),
        domain: extractDomain(listing.website),
        city: listing.city,
        state: listing.stateCode,
        country: 'US',
        niche: 'immigration',
        source: 'hg.org',
        profile_url: listing.profileUrl,
      });
      stateNewCount++;
    }

    log(`  ${stateCode}: ${listings.length} listings, ${stateNewCount} new unique`);

    // Save progress incrementally after each state
    if (allLeads.length > 0) {
      writeCSV(allLeads, OUT_PATH);
      log(`  Progress saved: ${allLeads.length} total leads → ${OUT_PATH}`);
    }

    // Delay between states
    if (totalStatesProcessed < states.length && !siteDown) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 2000);
    }
  }

  await browser.close();

  // Final write
  if (allLeads.length > 0) {
    writeCSV(allLeads, OUT_PATH);
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log('='.repeat(60));
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}/${states.length}`);
  log(`Leads with phone: ${allLeads.filter(l => l.phone).length}`);
  log(`Leads with website: ${allLeads.filter(l => l.website).length}`);
  if (siteDown) {
    log('');
    log('WARNING: Scrape was incomplete — HG.org is currently DOWN.');
    log('Re-run this script when the site comes back online.');
  }
  log(`Output: ${OUT_PATH}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
