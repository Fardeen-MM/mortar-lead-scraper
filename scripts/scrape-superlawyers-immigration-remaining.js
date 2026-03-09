#!/usr/bin/env node
/**
 * SuperLawyers Immigration Attorney Scraper — REMAINING STATES
 *
 * Scrapes all 41 states NOT covered in the initial run (CA, FL, GA, IL, MA, NJ, NY, PA, TX, VA already done).
 * APPENDS results to existing output/us-immigration-lawyers-superlawyers.csv.
 *
 * Usage:
 *   node scripts/scrape-superlawyers-immigration-remaining.js
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

// States already scraped in the first run
const ALREADY_DONE = new Set(['CA', 'FL', 'GA', 'IL', 'MA', 'NJ', 'NY', 'PA', 'TX', 'VA']);

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

// Major immigration cities per state
const STATE_CITIES = {
  AL: ['birmingham', 'montgomery', 'huntsville'],
  AK: ['anchorage', 'fairbanks'],
  AZ: ['phoenix', 'tucson', 'mesa', 'scottsdale'],
  AR: ['little-rock', 'fayetteville'],
  CO: ['denver', 'colorado-springs', 'aurora'],
  CT: ['hartford', 'new-haven', 'stamford', 'bridgeport'],
  DE: ['wilmington', 'dover'],
  DC: ['washington'],
  HI: ['honolulu'],
  ID: ['boise', 'idaho-falls'],
  IN: ['indianapolis', 'fort-wayne', 'south-bend'],
  IA: ['des-moines', 'cedar-rapids', 'iowa-city'],
  KS: ['kansas-city', 'wichita', 'overland-park'],
  KY: ['louisville', 'lexington'],
  LA: ['new-orleans', 'baton-rouge', 'shreveport'],
  ME: ['portland', 'bangor'],
  MD: ['baltimore', 'bethesda', 'rockville', 'silver-spring'],
  MI: ['detroit', 'grand-rapids', 'ann-arbor', 'dearborn'],
  MN: ['minneapolis', 'saint-paul', 'bloomington'],
  MS: ['jackson', 'gulfport'],
  MO: ['kansas-city', 'saint-louis', 'springfield'],
  MT: ['billings', 'missoula'],
  NE: ['omaha', 'lincoln'],
  NV: ['las-vegas', 'reno', 'henderson'],
  NH: ['manchester', 'concord'],
  NM: ['albuquerque', 'santa-fe', 'las-cruces'],
  NC: ['charlotte', 'raleigh', 'durham', 'greensboro'],
  ND: ['fargo', 'bismarck'],
  OH: ['columbus', 'cleveland', 'cincinnati', 'dayton'],
  OK: ['oklahoma-city', 'tulsa'],
  OR: ['portland', 'eugene', 'salem'],
  RI: ['providence', 'cranston'],
  SC: ['charleston', 'columbia', 'greenville'],
  SD: ['sioux-falls'],
  TN: ['nashville', 'memphis', 'knoxville', 'chattanooga'],
  UT: ['salt-lake-city', 'provo', 'ogden'],
  VT: ['burlington', 'montpelier'],
  WA: ['seattle', 'tacoma', 'spokane', 'bellevue'],
  WV: ['charleston', 'morgantown'],
  WI: ['milwaukee', 'madison', 'green-bay'],
  WY: ['cheyenne', 'casper'],
};

// Remaining states to scrape (all 51 minus the 10 already done)
const REMAINING_STATES = [
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

    const profileLinks = document.querySelectorAll('a.directory_profile[aria-label]');

    for (const link of profileLinks) {
      try {
        const name = link.textContent.trim();
        const profileUrl = link.href || '';

        if (!name || name.length < 3) continue;
        const cleanUrl = profileUrl.split('?')[0];
        if (seen.has(cleanUrl)) continue;
        seen.add(cleanUrl);

        let container = link.closest('.card') || link.closest('.result-item') || link.parentElement?.parentElement?.parentElement;

        let firm = '';
        let phone = '';
        let website = '';
        let location = '';

        if (container) {
          const textNodes = container.querySelectorAll('p, span, div');
          for (const node of textNodes) {
            const text = node.textContent.trim();
            if (text && text !== name && text.length > 3 && text.length < 100) {
              if (text.match(/(law|llp|pllc|p\.?c\.?|firm|group|associates|attorney|office)/i) &&
                  !text.match(/(immigration|super lawyer|rated|view|profile|call|website)/i)) {
                firm = text;
                break;
              }
            }
          }

          const phoneEl = container.querySelector('a[href^="tel:"]');
          if (phoneEl) {
            phone = phoneEl.href.replace('tel:', '').replace(/^\+1/, '');
          }

          const allLinks = container.querySelectorAll('a[href]');
          for (const a of allLinks) {
            if (a.href && !a.href.includes('superlawyers.com') && !a.href.includes('tel:') &&
                !a.href.includes('javascript:') && a.href.startsWith('http')) {
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

    // Fallback: check h2/h3 elements
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

// --- Load existing profile URLs for dedup ---
function loadExistingProfileUrls() {
  const urls = new Set();
  if (!fs.existsSync(OUTPUT_FILE)) return urls;
  const lines = fs.readFileSync(OUTPUT_FILE, 'utf8').split('\n');
  // profile_url is column index 13 (0-indexed)
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    // Simple CSV parse for profile_url column
    const cols = line.match(/(".*?"|[^,]*)/g) || [];
    if (cols.length > 13) {
      const url = cols[13]?.replace(/^"|"$/g, '').trim();
      if (url) urls.add(url);
    }
  }
  console.log(`Loaded ${urls.size} existing profile URLs for dedup`);
  return urls;
}

// --- Main ---
async function main() {
  console.log('=== SuperLawyers Immigration — REMAINING 41 STATES ===');
  console.log(`States: ${REMAINING_STATES.join(', ')}`);
  console.log(`Total states: ${REMAINING_STATES.length}\n`);

  // Verify none of the already-done states are in the list
  for (const s of REMAINING_STATES) {
    if (ALREADY_DONE.has(s)) {
      console.error(`ERROR: ${s} is already done!`);
      process.exit(1);
    }
  }

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Load existing for dedup
  const existingUrls = loadExistingProfileUrls();

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1920,1080',
    ],
  });

  const newListings = [];
  const seen = new Set(existingUrls); // Start with existing URLs to prevent duplicates
  let totalSkipped = 0;
  let lastCheckpointIndex = 0; // Track what we've already appended

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
    });

    // Warm up
    console.log('Warming up browser with homepage...');
    const warmupOk = await navigateWithRetry(page, 'https://attorneys.superlawyers.com/');
    if (!warmupOk) {
      console.log('WARNING: Cloudflare is blocking access. May require manual intervention.');
    }
    await sleep(3000);

    for (let si = 0; si < REMAINING_STATES.length; si++) {
      const stateCode = REMAINING_STATES[si];
      const stateSlug = STATE_SLUGS[stateCode];
      const cities = STATE_CITIES[stateCode] || [];

      console.log(`\n[${si + 1}/${REMAINING_STATES.length}] ${stateCode} (${cities.length} cities)`);

      let stateCount = 0;

      for (let ci = 0; ci < cities.length; ci++) {
        const citySlug = cities[ci];
        const url = `https://attorneys.superlawyers.com/immigration/${stateSlug}/${citySlug}/`;

        console.log(`  ${titleCase(citySlug)}: ${url}`);

        const ok = await navigateWithRetry(page, url);
        if (!ok) {
          console.log(`    BLOCKED by Cloudflare, skipping`);
          continue;
        }

        await sleep(2000);

        // Extract from current page
        let pageNum = 1;
        let totalPageListings = 0;

        while (true) {
          const listings = await extractListings(page);
          console.log(`    Page ${pageNum}: Found ${listings.length} attorneys`);

          for (const listing of listings) {
            const key = listing.profileUrl || `${listing.name}|${listing.firm}`.toLowerCase();
            if (seen.has(key)) {
              totalSkipped++;
              continue;
            }
            seen.add(key);

            const { first_name, last_name } = parseName(listing.name);

            let city = titleCase(citySlug);
            if (listing.location) {
              const locMatch = listing.location.match(/^([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*([A-Z]{2})$/);
              if (locMatch) {
                city = locMatch[1];
              }
            }

            newListings.push({
              first_name,
              last_name,
              firm_name: listing.firm,
              title: 'Immigration Attorney',
              email: '',
              phone: listing.phone,
              website: listing.website,
              domain: extractDomain(listing.website),
              city,
              state: stateCode,
              country: 'US',
              niche: 'immigration',
              source: 'superlawyers',
              profile_url: listing.profileUrl,
              rating: '',
              years_experience: '',
            });
            stateCount++;
            totalPageListings++;
          }

          // Try pagination — look for "Next" link
          const hasNext = await page.evaluate(() => {
            const nextLink = document.querySelector('a.next, a[rel="next"], li.next a, .pagination a.next');
            if (nextLink && nextLink.href) {
              return nextLink.href;
            }
            // Also check for numbered pagination
            const currentPage = document.querySelector('.pagination .active, .pagination .current');
            if (currentPage) {
              const nextSibling = currentPage.nextElementSibling;
              if (nextSibling && nextSibling.querySelector('a')) {
                return nextSibling.querySelector('a').href;
              }
            }
            return null;
          });

          if (hasNext && pageNum < 10) { // Safety limit of 10 pages per city
            pageNum++;
            console.log(`    Navigating to page ${pageNum}...`);
            const nextOk = await navigateWithRetry(page, hasNext);
            if (!nextOk) break;
            await sleep(2000);
          } else {
            break;
          }
        }

        await randomDelay();
      }

      console.log(`  ${stateCode}: ${stateCount} new attorneys (running total: ${newListings.length})`);

      // Periodic save every 5 states (only append new entries since last checkpoint)
      if ((si + 1) % 5 === 0 && newListings.length > lastCheckpointIndex) {
        const batch = newListings.slice(lastCheckpointIndex);
        appendToCsv(batch);
        console.log(`  [CHECKPOINT] Appended ${batch.length} leads to CSV (total new: ${newListings.length})`);
        lastCheckpointIndex = newListings.length;
      }

      await sleep(DELAY_BETWEEN_STATES);
    }
  } finally {
    await browser.close();
  }

  // Final append (only entries not yet saved by checkpoint)
  if (newListings.length > lastCheckpointIndex) {
    const remaining = newListings.slice(lastCheckpointIndex);
    appendToCsv(remaining);
  }

  console.log(`\n=== DONE ===`);
  console.log(`New attorneys found: ${newListings.length}`);
  console.log(`Duplicates skipped: ${totalSkipped}`);
  console.log(`Output (appended): ${OUTPUT_FILE}`);

  const withPhone = newListings.filter(l => l.phone).length;
  const withWebsite = newListings.filter(l => l.website).length;
  console.log(`With phone: ${withPhone}`);
  console.log(`With website: ${withWebsite}`);

  // Count total lines in CSV
  if (fs.existsSync(OUTPUT_FILE)) {
    const totalLines = fs.readFileSync(OUTPUT_FILE, 'utf8').split('\n').filter(l => l.trim()).length - 1;
    console.log(`Total leads in CSV: ${totalLines}`);
  }
}

// Append new listings to existing CSV (no header, just data rows)
function appendToCsv(listings) {
  const csvLines = [];
  for (const l of listings) {
    csvLines.push(CSV_COLUMNS.map(h => csvEscape(l[h] || '')).join(','));
  }

  // Check if file exists and has content
  if (fs.existsSync(OUTPUT_FILE)) {
    const existing = fs.readFileSync(OUTPUT_FILE, 'utf8');
    // Make sure existing ends with newline
    const separator = existing.endsWith('\n') ? '' : '\n';
    fs.appendFileSync(OUTPUT_FILE, separator + csvLines.join('\n') + '\n', 'utf8');
  } else {
    // Create new file with header
    const header = CSV_COLUMNS.join(',');
    fs.writeFileSync(OUTPUT_FILE, header + '\n' + csvLines.join('\n') + '\n', 'utf8');
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
