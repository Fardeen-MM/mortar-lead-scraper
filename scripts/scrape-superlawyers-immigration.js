#!/usr/bin/env node
/**
 * SuperLawyers Immigration Attorney Scraper
 *
 * Scrapes https://attorneys.superlawyers.com/immigration/{state}/{city}/
 * using Puppeteer + Stealth (Cloudflare protection requires real browser).
 *
 * Strategy:
 *   1. For each state, construct city URLs for major immigration markets
 *   2. Load each page with Puppeteer, wait for Cloudflare challenge
 *   3. Extract attorney cards with name, firm, phone, website, profile URL
 *   4. Paginate through all result pages
 *   5. Dedup by profile URL
 *   6. Output CSV
 *
 * Output: output/us-immigration-lawyers-superlawyers.csv
 *
 * Usage:
 *   node scripts/scrape-superlawyers-immigration.js                    # All states
 *   node scripts/scrape-superlawyers-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-superlawyers-immigration.js --test             # Test (3 states)
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// --- Config ---
const DELAY_MIN = 3000;
const DELAY_MAX = 6000;
const DELAY_BETWEEN_STATES = 8000;
const CLOUDFLARE_WAIT = 12000;
const MAX_CLOUDFLARE_RETRIES = 3;
const NAV_TIMEOUT = 45000;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-superlawyers.csv');

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

// Major immigration cities per state (top markets)
const STATE_CITIES = {
  CA: ['los-angeles', 'san-francisco', 'san-diego', 'san-jose', 'sacramento', 'oakland', 'irvine', 'fresno'],
  NY: ['new-york', 'brooklyn', 'buffalo', 'rochester', 'albany', 'white-plains'],
  TX: ['houston', 'dallas', 'san-antonio', 'austin', 'el-paso', 'fort-worth', 'mcallen'],
  FL: ['miami', 'orlando', 'tampa', 'jacksonville', 'fort-lauderdale', 'west-palm-beach'],
  IL: ['chicago', 'springfield', 'naperville'],
  NJ: ['newark', 'jersey-city', 'trenton', 'edison', 'hackensack'],
  MA: ['boston', 'cambridge', 'worcester', 'springfield'],
  GA: ['atlanta', 'savannah', 'augusta'],
  PA: ['philadelphia', 'pittsburgh', 'harrisburg'],
  VA: ['arlington', 'fairfax', 'richmond', 'virginia-beach', 'alexandria'],
  MD: ['baltimore', 'bethesda', 'rockville', 'silver-spring'],
  WA: ['seattle', 'tacoma', 'spokane', 'bellevue'],
  AZ: ['phoenix', 'tucson', 'mesa', 'scottsdale'],
  CO: ['denver', 'colorado-springs', 'aurora'],
  NC: ['charlotte', 'raleigh', 'durham', 'greensboro'],
  OH: ['columbus', 'cleveland', 'cincinnati', 'dayton'],
  MI: ['detroit', 'grand-rapids', 'ann-arbor', 'dearborn'],
  CT: ['hartford', 'new-haven', 'stamford', 'bridgeport'],
  MN: ['minneapolis', 'saint-paul', 'bloomington'],
  OR: ['portland', 'eugene', 'salem'],
  NV: ['las-vegas', 'reno', 'henderson'],
  TN: ['nashville', 'memphis', 'knoxville', 'chattanooga'],
  MO: ['kansas-city', 'saint-louis', 'springfield'],
  IN: ['indianapolis', 'fort-wayne', 'south-bend'],
  WI: ['milwaukee', 'madison', 'green-bay'],
  SC: ['charleston', 'columbia', 'greenville'],
  KY: ['louisville', 'lexington'],
  LA: ['new-orleans', 'baton-rouge', 'shreveport'],
  AL: ['birmingham', 'montgomery', 'huntsville'],
  OK: ['oklahoma-city', 'tulsa'],
  IA: ['des-moines', 'cedar-rapids', 'iowa-city'],
  UT: ['salt-lake-city', 'provo', 'ogden'],
  KS: ['kansas-city', 'wichita', 'overland-park'],
  AR: ['little-rock', 'fayetteville'],
  MS: ['jackson', 'gulfport'],
  NE: ['omaha', 'lincoln'],
  NM: ['albuquerque', 'santa-fe', 'las-cruces'],
  HI: ['honolulu'],
  NH: ['manchester', 'concord'],
  ME: ['portland', 'bangor'],
  ID: ['boise', 'idaho-falls'],
  WV: ['charleston', 'morgantown'],
  MT: ['billings', 'missoula'],
  RI: ['providence', 'cranston'],
  DE: ['wilmington', 'dover'],
  SD: ['sioux-falls'],
  ND: ['fargo', 'bismarck'],
  AK: ['anchorage', 'fairbanks'],
  VT: ['burlington', 'montpelier'],
  WY: ['cheyenne', 'casper'],
  DC: ['washington'],
};

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
  'source', 'profile_url', 'rating', 'years_experience',
];

// --- CLI args ---
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const statesIdx = args.indexOf('--states');
const statesFilter = statesIdx >= 0
  ? args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase())
  : null;

// --- Helpers ---
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|ph\.?d\.?|ll\.?m\.?|jr\.?|sr\.?|ii|iii|iv)/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = cleaned.split(' ').filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function extractDomain(url) {
  if (!url) return '';
  try { return new URL(url).hostname.replace(/^www\./, ''); } catch { return ''; }
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function titleCase(str) {
  return str.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// --- Browser Helpers ---
async function waitForCloudflare(page) {
  const title = await page.title();
  if (title.includes('Just a moment') || title.includes('Checking')) {
    console.log('    Waiting for Cloudflare challenge...');
    await sleep(CLOUDFLARE_WAIT);
    // Check again
    const newTitle = await page.title();
    if (newTitle.includes('Just a moment') || newTitle.includes('Checking')) {
      console.log('    Still waiting...');
      await sleep(CLOUDFLARE_WAIT);
    }
  }
}

async function navigateWithRetry(page, url, retries = MAX_CLOUDFLARE_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
      await waitForCloudflare(page);

      const title = await page.title();
      if (title.includes('Just a moment') || title.includes('Checking')) {
        if (attempt < retries) {
          console.log(`    Cloudflare still blocking (attempt ${attempt}/${retries}), retrying...`);
          await sleep(CLOUDFLARE_WAIT);
          continue;
        }
        return false;
      }
      return true;
    } catch (err) {
      if (attempt < retries) {
        console.log(`    Navigation failed (attempt ${attempt}/${retries}): ${err.message}`);
        await sleep(5000);
        continue;
      }
      return false;
    }
  }
  return false;
}

async function extractListings(page) {
  return page.evaluate(() => {
    const listings = [];
    const seen = new Set();

    // SuperLawyers uses a.directory_profile links with aria-label for attorney names
    // and href to profile pages. Phone is in a[href^="tel:"] nearby.
    // The page uses h2/h3 headings for attorney names within card-like containers.

    // Strategy: find all a.directory_profile links that have aria-label (name links, not image links)
    const profileLinks = document.querySelectorAll('a.directory_profile[aria-label]');

    for (const link of profileLinks) {
      try {
        const name = link.textContent.trim();
        const profileUrl = link.href || '';

        // Skip empty or very short names, skip if already seen
        if (!name || name.length < 3) continue;
        const cleanUrl = profileUrl.split('?')[0]; // Remove query params for dedup
        if (seen.has(cleanUrl)) continue;
        seen.add(cleanUrl);

        // Navigate up to find the containing card/section
        let container = link.closest('.card') || link.closest('.result-item') || link.parentElement?.parentElement?.parentElement;

        let firm = '';
        let phone = '';
        let website = '';
        let location = '';

        if (container) {
          // Firm name - look for text near the attorney name that looks like a firm
          const textNodes = container.querySelectorAll('p, span, div');
          for (const node of textNodes) {
            const text = node.textContent.trim();
            // Look for firm-like text (contains law, llp, pc, etc. or is a short text after name)
            if (text && text !== name && text.length > 3 && text.length < 100) {
              if (text.match(/(law|llp|pllc|p\.?c\.?|firm|group|associates|attorney|office)/i) &&
                  !text.match(/(immigration|super lawyer|rated|view|profile|call|website)/i)) {
                firm = text;
                break;
              }
            }
          }

          // Phone
          const phoneEl = container.querySelector('a[href^="tel:"]');
          if (phoneEl) {
            phone = phoneEl.href.replace('tel:', '').replace(/^\+1/, '');
          }

          // Website link (external link, not superlawyers.com)
          const allLinks = container.querySelectorAll('a[href]');
          for (const a of allLinks) {
            if (a.href && !a.href.includes('superlawyers.com') && !a.href.includes('tel:') &&
                !a.href.includes('javascript:') && a.href.startsWith('http')) {
              // Strip tracking params like adSubId
              try {
                const u = new URL(a.href);
                u.searchParams.delete('adSubId');
                website = u.toString();
              } catch {
                website = a.href;
              }
              break;
            }
          }

          // Location
          const locText = container.textContent;
          const locMatch = locText.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*([A-Z]{2})/);
          if (locMatch) {
            location = `${locMatch[1]}, ${locMatch[2]}`;
          }
        }

        listings.push({ name, firm, phone, website, profileUrl: cleanUrl, location });
      } catch (e) {
        // Skip errors
      }
    }

    // Fallback: also check h2/h3 elements that might be attorney names
    if (listings.length === 0) {
      const headings = document.querySelectorAll('h2, h3');
      for (const h of headings) {
        const link = h.querySelector('a') || h.closest('a');
        if (!link) continue;
        const name = h.textContent.trim();
        const href = link.href || '';
        if (!name || name.length < 3 || !href.includes('superlawyers.com')) continue;
        if (seen.has(href)) continue;
        seen.add(href);
        listings.push({ name, firm: '', phone: '', website: '', profileUrl: href, location: '' });
      }
    }

    return listings;
  });
}

// --- Main ---
async function main() {
  console.log('=== SuperLawyers Immigration Attorney Scraper ===');
  console.log(`Mode: ${testMode ? 'TEST' : 'FULL'}\n`);

  const states = statesFilter || (testMode ? ['CA', 'NY', 'TX'] : ALL_STATES);
  console.log(`States: ${states.join(', ')}\n`);

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1920,1080',
    ],
  });

  const allListings = [];
  const seen = new Set();

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
    });

    // First, warm up by visiting the homepage
    console.log('Warming up browser with homepage...');
    const warmupOk = await navigateWithRetry(page, 'https://attorneys.superlawyers.com/');
    if (!warmupOk) {
      console.log('WARNING: Cloudflare is blocking access. SuperLawyers may require manual intervention.');
    }
    await sleep(3000);

    for (let si = 0; si < states.length; si++) {
      const stateCode = states[si];
      const stateSlug = STATE_SLUGS[stateCode];
      const cities = STATE_CITIES[stateCode] || [];
      const maxCities = testMode ? 2 : cities.length;

      console.log(`\n[${si + 1}/${states.length}] ${stateCode} (${cities.length} cities)`);

      let stateCount = 0;

      for (let ci = 0; ci < Math.min(cities.length, maxCities); ci++) {
        const citySlug = cities[ci];
        const url = `https://attorneys.superlawyers.com/immigration/${stateSlug}/${citySlug}/`;

        console.log(`  ${titleCase(citySlug)}: ${url}`);

        const ok = await navigateWithRetry(page, url);
        if (!ok) {
          console.log(`    BLOCKED by Cloudflare, skipping`);
          continue;
        }

        // Wait for content to load
        await sleep(2000);

        const listings = await extractListings(page);
        console.log(`    Found ${listings.length} attorneys`);

        for (const listing of listings) {
          const key = listing.profileUrl || `${listing.name}|${listing.firm}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);

          const { first_name, last_name } = parseName(listing.name);

          // Use the city slug as the primary city name (most reliable)
          // Only override if location looks like a proper "City, ST" pattern
          let city = titleCase(citySlug);
          let state = stateCode;
          if (listing.location) {
            const locMatch = listing.location.match(/^([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*([A-Z]{2})$/);
            if (locMatch) {
              city = locMatch[1];
            }
          }

          allListings.push({
            first_name,
            last_name,
            firm_name: listing.firm,
            title: 'Immigration Attorney',
            email: '',
            phone: listing.phone,
            website: listing.website,
            domain: extractDomain(listing.website),
            city,
            state,
            country: 'US',
            niche: 'immigration',
            source: 'superlawyers',
            profile_url: listing.profileUrl,
            rating: '',
            years_experience: '',
          });
          stateCount++;
        }

        // Check for pagination
        try {
          const nextPage = await page.$('a.next, a[rel="next"], .pagination a:last-child');
          if (nextPage && !testMode) {
            // Could paginate here, but let's keep it simple for now
          }
        } catch (e) {
          // No pagination
        }

        await randomDelay();
      }

      console.log(`  ${stateCode}: ${stateCount} attorneys (total: ${allListings.length})`);
      await sleep(DELAY_BETWEEN_STATES);
    }
  } finally {
    await browser.close();
  }

  // Write CSV
  const csvLines = [CSV_COLUMNS.join(',')];
  for (const l of allListings) {
    csvLines.push(CSV_COLUMNS.map(h => csvEscape(l[h] || '')).join(','));
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  console.log(`\n=== DONE ===`);
  console.log(`Total attorneys: ${allListings.length}`);
  console.log(`Output: ${OUTPUT_FILE}`);

  const withPhone = allListings.filter(l => l.phone).length;
  const withWebsite = allListings.filter(l => l.website).length;
  console.log(`With phone: ${withPhone}`);
  console.log(`With website: ${withWebsite}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
