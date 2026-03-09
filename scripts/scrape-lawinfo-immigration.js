#!/usr/bin/env node
/**
 * LawInfo Immigration Lawyers Scraper
 *
 * Scrapes https://www.lawinfo.com/immigration/{state}/{city}/
 * using Puppeteer + Stealth (site is behind Cloudflare challenge).
 *
 * Strategy:
 *   1. For each state, load the state page to discover city links
 *   2. For each city, paginate through all listing pages
 *   3. Extract lawyer data from JSON-LD LegalService schema embedded in page
 *   4. Dedup by profile URL
 *   5. Output CSV with incremental saves
 *
 * Features:
 *   - Resume support: saves progress to a JSON file, skips completed states
 *   - Rate limiting: 2-3 second random delays between requests
 *   - Incremental CSV writes after each state
 *   - Cloudflare challenge handling with auto-wait
 *
 * Usage:
 *   node scripts/scrape-lawinfo-immigration.js --test             # 2 states, 2 pages each
 *   node scripts/scrape-lawinfo-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-lawinfo-immigration.js --all-states       # All 51 states+DC
 *   node scripts/scrape-lawinfo-immigration.js --no-resume        # Ignore saved progress
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2500;
const DELAY_MAX = 4500;
const DELAY_BETWEEN_STATES = 6000;
const DELAY_AFTER_CF_BLOCK = 15000; // cooldown after hitting a Cloudflare block
const CLOUDFLARE_WAIT = 8000;
const MAX_CLOUDFLARE_RETRIES = 4;
const NAV_TIMEOUT = 45000;
const MAX_PAGES_PER_CITY = 50;

const BASE_URL = 'https://www.lawinfo.com';

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

// Ordered by immigration legal market size
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

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'us-immigration-lawyers-lawinfo.csv');
const PROGRESS_FILE = path.join(OUT_DIR, '.lawinfo-immigration-progress.json');

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|ph\.?d\.?|md|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?|p\.?c\.?|p\.?a\.?|pllc|llc|llp|inc\.?)/gi, '')
    .trim();
  cleaned = cleaned.replace(/\.\s*$/, '').trim();
  if (!cleaned) return { first_name: '', last_name: '' };

  if (cleaned.includes(',')) {
    const [last, ...rest] = cleaned.split(',').map(s => s.trim());
    const first = rest.join(' ').split(/\s+/)[0] || '';
    return { first_name: first, last_name: last };
  }

  const parts = cleaned.split(/\s+/);
  if (parts.length >= 2) {
    return { first_name: parts[0], last_name: parts[parts.length - 1] };
  }
  return { first_name: '', last_name: cleaned };
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
  let p = phone.replace(/[^\d+()-\s]/g, '').trim();
  const digits = p.replace(/\D/g, '');
  if (digits.length < 7 || digits.length > 15) return '';
  return p;
}

/**
 * Extract a person's name from a firm name string.
 */
function extractPersonName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  // "Law Office(s) of FirstName LastName ..."
  let m = firmName.match(/Law\s+Offices?\s+of\s+(.+?)(?:\s+and\s+|\s*&\s*|\s*,|\s*-|\s+P\.?[ACL]\.?|\s+LLC|\s+LLP|\s+Inc|$)/i);
  if (m) return parseName(m[1].trim());

  // "The Law Firm of FirstName LastName"
  m = firmName.match(/(?:The\s+)?Law\s+Firm\s+of\s+(.+?)(?:\s+and\s+|\s*&\s*|\s*,|\s*-|\s+P\.?[ACL]\.?|\s+LLC|\s+LLP|\s+Inc|$)/i);
  if (m) return parseName(m[1].trim());

  // If the name looks like a person (2-4 words, no firm-like keywords)
  const cleaned = firmName
    .replace(/,?\s*(p\.?c\.?|p\.?a\.?|pllc|llc|llp|inc\.?|ltd\.?|esq\.?|attorney.*|lawyer.*)$/gi, '')
    .trim();
  const words = cleaned.split(/\s+/);
  const looksLikeFirm = /[&+|]|law|group|associates|partners|legal|services|counsel|firm|office|center|practice|clinic|solution|immigration|global|national|american|usa/i.test(cleaned);

  if (words.length >= 2 && words.length <= 4 && !looksLikeFirm) {
    return parseName(cleaned);
  }

  return { first_name: '', last_name: '' };
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

// ── Progress Management ──────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
    }
  } catch (e) {
    log(`Warning: Could not load progress file: ${e.message}`);
  }
  return { completedStates: [], leads: [] };
}

function saveProgress(progress) {
  try {
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
  } catch (e) {
    log(`Warning: Could not save progress: ${e.message}`);
  }
}

// ── Cloudflare Handling ──────────────────────────────────────────────────────

async function isCloudflareChallenge(page) {
  return page.evaluate(() => {
    return document.title.includes('Just a moment') ||
      document.title.includes('Attention Required') ||
      document.querySelector('#challenge-running') !== null ||
      document.querySelector('#challenge-form') !== null;
  });
}

async function waitForCloudflare(page, maxRetries = MAX_CLOUDFLARE_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    const blocked = await isCloudflareChallenge(page);
    if (!blocked) return true;
    log(`  Cloudflare challenge detected, waiting ${CLOUDFLARE_WAIT / 1000}s (attempt ${i + 1}/${maxRetries})...`);
    await sleep(CLOUDFLARE_WAIT);
  }
  return !(await isCloudflareChallenge(page));
}

/**
 * Navigate to a URL with Cloudflare handling. Returns true if page loaded successfully.
 */
async function navigateTo(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  } catch (err) {
    log(`  Navigation error: ${err.message}`);
    return false;
  }

  const passed = await waitForCloudflare(page);
  if (!passed) {
    log(`  BLOCKED by Cloudflare on ${url}`);
    return false;
  }

  return true;
}

// ── Page Data Extraction (runs in browser context) ───────────────────────────

/**
 * Extract city links from a state page. Runs in browser context via page.evaluate().
 */
async function extractCityLinks(page, stateSlug) {
  return page.evaluate((slug) => {
    const cities = [];
    const seen = new Set();
    const pattern = new RegExp(`/immigration/${slug}/([a-z][a-z0-9-]+)/?$`);

    document.querySelectorAll('a').forEach(a => {
      const href = a.href;
      if (!href) return;

      const m = href.match(pattern);
      if (m && !seen.has(m[1])) {
        const citySlug = m[1];
        if (['all-cities', 'all-counties', 'legal-aid', 'resources'].includes(citySlug)) return;
        seen.add(citySlug);
        cities.push({
          name: a.textContent.trim(),
          slug: citySlug,
          url: `https://www.lawinfo.com/immigration/${slug}/${citySlug}/`,
        });
      }
    });

    return cities;
  }, stateSlug);
}

/**
 * Extract lawyer data from the page using JSON-LD + HTML fallback.
 * JSON-LD is embedded as an array in a single <script> tag.
 * HTML profile links provide supplementary data for firms not in JSON-LD.
 * Runs in browser context via page.evaluate().
 */
async function extractLeadsFromPage(page, stateCode) {
  return page.evaluate((sc) => {
    const leads = [];
    const seenUUIDs = new Set();

    // Helper to extract UUID from a lawinfo profile URL
    function extractUUID(url) {
      if (!url) return '';
      const m = url.match(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/);
      return m ? m[1] : '';
    }

    // 1. Primary: Extract from JSON-LD (array or individual objects)
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    scripts.forEach(script => {
      try {
        const data = JSON.parse(script.textContent);
        const items = Array.isArray(data) ? data : [data];

        items.forEach(item => {
          if (item['@type'] !== 'LegalService') return;
          if (!item.name) return;

          const profileUrl = item['@id'] || '';
          const uuid = extractUUID(profileUrl);
          if (uuid) seenUUIDs.add(uuid);

          const addr = item.address || {};
          const phone = item.telephone || '';

          let website = item.url || '';
          if (website.includes('lawinfo.com')) website = '';

          leads.push({
            firmName: item.name,
            phone,
            website,
            city: addr.addressLocality || '',
            stateRegion: addr.addressRegion || sc,
            profileUrl: profileUrl.split('?')[0], // strip tracking params
          });
        });
      } catch (e) {
        // Skip malformed JSON-LD
      }
    });

    // 2. Supplementary: Extract from HTML profile links not already in JSON-LD
    document.querySelectorAll('a[href*="/lawfirm/"]').forEach(a => {
      const href = a.href;
      if (!href.includes('.html')) return;
      const text = a.textContent.trim();
      if (!text || text === 'Contact Us' || text === 'View Profile' || text.length < 2) return;

      const uuid = extractUUID(href);
      if (!uuid || seenUUIDs.has(uuid)) return;
      seenUUIDs.add(uuid);

      // Try to find phone in nearby elements
      let phone = '';
      const card = a.closest('div');
      if (card) {
        const phoneLink = card.querySelector('a[href^="tel:"]');
        if (phoneLink) {
          phone = phoneLink.textContent.replace(/call\s*now/i, '').trim();
        }
      }

      // Try to find website in nearby elements
      let website = '';
      if (card) {
        card.querySelectorAll('a').forEach(link => {
          const linkText = link.textContent.trim().toLowerCase();
          if (linkText.includes('visit website') && !link.href.includes('lawinfo.com')) {
            website = link.href;
          }
        });
      }

      leads.push({
        firmName: text,
        phone,
        website,
        city: '',
        stateRegion: sc,
        profileUrl: href.split('?')[0], // strip tracking params
      });
    });

    return leads;
  }, stateCode);
}

/**
 * Get the max page number from pagination links.
 * Runs in browser context.
 */
async function getMaxPage(page) {
  return page.evaluate(() => {
    let maxPage = 1;
    document.querySelectorAll('a[href*="page="]').forEach(a => {
      const m = a.href.match(/[?&]page=(\d+)/);
      if (m) {
        const p = parseInt(m[1], 10);
        if (p > maxPage) maxPage = p;
      }
    });
    return maxPage;
  });
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

/**
 * Scrape a single listing page. Returns { leads, maxPage, blocked }.
 */
async function scrapePage(page, url, stateCode) {
  const ok = await navigateTo(page, url);
  if (!ok) return { leads: [], maxPage: 1, blocked: true };

  // Check for error page
  const pageTitle = await page.title();
  if (pageTitle.includes("couldn't determine") || pageTitle.includes('Sorry')) {
    return { leads: [], maxPage: 1, blocked: false };
  }

  const rawLeads = await extractLeadsFromPage(page, stateCode);
  const maxPage = await getMaxPage(page);

  // Transform raw leads into our format
  const leads = rawLeads.map(raw => {
    const { first_name, last_name } = extractPersonName(raw.firmName);
    return {
      first_name,
      last_name,
      firm_name: raw.firmName,
      title: '',
      email: '',
      phone: cleanPhone(raw.phone),
      website: raw.website,
      domain: extractDomain(raw.website),
      city: raw.city,
      state: stateCode,
      country: 'US',
      niche: 'immigration',
      source: 'lawinfo',
      profile_url: raw.profileUrl,
    };
  });

  return { leads, maxPage, blocked: false };
}

/**
 * Scrape all pages for a given city.
 * Returns { leads, wasBlocked }.
 */
async function scrapeCity(page, cityUrl, stateCode, cityName, maxPages) {
  const allLeads = [];
  let discoveredMaxPage = maxPages;
  let wasBlocked = false;

  for (let pageNum = 1; pageNum <= discoveredMaxPage; pageNum++) {
    const url = pageNum === 1 ? cityUrl : `${cityUrl}?page=${pageNum}`;

    try {
      const { leads, maxPage, blocked } = await scrapePage(page, url, stateCode);

      if (blocked) {
        wasBlocked = true;
        break;
      }

      if (leads.length === 0) break;

      // Set city on leads that don't have one
      for (const lead of leads) {
        if (!lead.city) lead.city = cityName;
      }

      allLeads.push(...leads);

      // Update max page from pagination
      if (pageNum === 1 && maxPage > 1) {
        discoveredMaxPage = Math.min(maxPage, maxPages);
      }

      // Stop if we've reached the last page
      if (pageNum >= maxPage) break;

      await randomDelay();
    } catch (err) {
      log(`    ERROR on ${cityName} page ${pageNum}: ${err.message}`);
      break;
    }
  }

  return { leads: allLeads, wasBlocked };
}

/**
 * Scrape all cities for a given state.
 */
async function scrapeState(page, stateCode, maxPagesPerCity, maxCities) {
  const stateSlug = STATE_SLUGS[stateCode];
  if (!stateSlug) {
    log(`Unknown state code: ${stateCode}`);
    return [];
  }

  const stateUrl = `${BASE_URL}/immigration/${stateSlug}/`;
  log(`Loading state page: ${stateCode} -> ${stateUrl}`);

  const ok = await navigateTo(page, stateUrl);
  if (!ok) {
    log(`  Could not load state page for ${stateCode}`);
    return [];
  }

  // Check for error page
  const pageTitle = await page.title();
  if (pageTitle.includes("couldn't determine") || pageTitle.includes('Sorry')) {
    log(`  State page returned error for ${stateCode}`);
    return [];
  }

  const cities = await extractCityLinks(page, stateSlug);

  if (cities.length === 0) {
    log(`  No cities found for ${stateCode}`);
    return [];
  }

  log(`  ${stateCode}: found ${cities.length} cities`);

  const citiesToScrape = cities.slice(0, maxCities);
  const allLeads = [];
  const seenProfiles = new Set();

  for (let ci = 0; ci < citiesToScrape.length; ci++) {
    const city = citiesToScrape[ci];
    log(`  City ${ci + 1}/${citiesToScrape.length}: ${city.name}`);

    await randomDelay();

    try {
      const { leads: cityLeads, wasBlocked } = await scrapeCity(page, city.url, stateCode, city.name, maxPagesPerCity);
      let newCount = 0;

      for (const lead of cityLeads) {
        const key = lead.profile_url || `${lead.firm_name}|${lead.city}|${lead.state}`;
        if (seenProfiles.has(key)) continue;
        seenProfiles.add(key);
        allLeads.push(lead);
        newCount++;
      }

      log(`    ${city.name}: ${cityLeads.length} found, ${newCount} new${wasBlocked ? ' (CF block)' : ''}`);

      // Cooldown after Cloudflare block
      if (wasBlocked) {
        log(`    Cloudflare cooldown: ${DELAY_AFTER_CF_BLOCK / 1000}s...`);
        await sleep(DELAY_AFTER_CF_BLOCK);
      }
    } catch (err) {
      log(`    ERROR scraping ${city.name}: ${err.message}`);
    }
  }

  return allLeads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllStates = args.includes('--all-states');
  const noResume = args.includes('--no-resume');

  let statesFilter = null;
  const statesArg = args.find(a => a.startsWith('--states'));
  if (statesArg) {
    const val = statesArg.includes('=')
      ? statesArg.split('=')[1]
      : args[args.indexOf(statesArg) + 1];
    if (val) {
      statesFilter = val.split(',').map(s => s.trim().toUpperCase());
    }
  }

  // Determine which states to scrape
  let states;
  if (statesFilter) {
    states = statesFilter;
  } else if (isTest) {
    states = ['WY', 'VT']; // Small states for quick testing
  } else if (isAllStates) {
    states = ALL_STATES;
  } else {
    console.log('Usage:');
    console.log('  node scripts/scrape-lawinfo-immigration.js --test             # 2 small states, 2 pages/city');
    console.log('  node scripts/scrape-lawinfo-immigration.js --states CA,TX,NY  # Specific states');
    console.log('  node scripts/scrape-lawinfo-immigration.js --all-states       # All 51 states+DC');
    console.log('  node scripts/scrape-lawinfo-immigration.js --no-resume        # Ignore saved progress');
    process.exit(0);
  }

  const maxPagesPerCity = isTest ? 2 : MAX_PAGES_PER_CITY;
  const maxCities = isTest ? 3 : 999;

  // Ensure output directory exists
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Load progress (resume support)
  let progress = noResume ? { completedStates: [], leads: [] } : loadProgress();

  // Filter out already-completed states
  const remainingStates = states.filter(s => !progress.completedStates.includes(s));
  const skippedCount = states.length - remainingStates.length;

  log('');
  log('=== LawInfo Immigration Lawyers Scraper ===');
  log(`States: ${states.length} total, ${skippedCount} already done, ${remainingStates.length} remaining`);
  log(`Test mode: ${isTest} | Max pages/city: ${maxPagesPerCity} | Max cities/state: ${maxCities}`);
  log(`Output: ${OUT_FILE}`);
  log(`Progress: ${PROGRESS_FILE}`);
  log('');

  if (remainingStates.length === 0) {
    log('All states already completed! Use --no-resume to re-scrape.');
    if (progress.leads.length > 0) {
      writeCSV(progress.leads, OUT_FILE);
      log(`CSV written: ${progress.leads.length} leads -> ${OUT_FILE}`);
    }
    return;
  }

  // Launch Puppeteer
  log('Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--window-size=1920,1080',
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Warm up session to get Cloudflare cookies
  log('Warming up session (getting Cloudflare cookies)...');
  try {
    await page.goto('https://www.lawinfo.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(3000);
    const warmupOk = !(await isCloudflareChallenge(page));
    log(`Session warmup: ${warmupOk ? 'OK' : 'Cloudflare challenge detected'}`);
    if (!warmupOk) {
      log('Waiting for Cloudflare to resolve...');
      await sleep(15000);
      const resolved = !(await isCloudflareChallenge(page));
      log(`After wait: ${resolved ? 'OK' : 'STILL BLOCKED'}`);
    }
  } catch (err) {
    log(`Warmup error (non-fatal): ${err.message}`);
  }

  // Initialize allLeads from progress
  const allLeads = [...progress.leads];
  const seenProfiles = new Set(allLeads.map(l => l.profile_url || `${l.firm_name}|${l.city}|${l.state}`));

  let totalStatesProcessed = skippedCount;
  let consecutiveBlocks = 0;

  for (const stateCode of remainingStates) {
    totalStatesProcessed++;
    log(`\n${'='.repeat(60)}`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${STATE_SLUGS[stateCode]})`);

    // If too many consecutive Cloudflare blocks, pause
    if (consecutiveBlocks >= 3) {
      log(`  ${consecutiveBlocks} consecutive blocks — pausing 60s to cool down...`);
      await sleep(60000);
      consecutiveBlocks = 0;
    }

    const stateLeads = await scrapeState(page, stateCode, maxPagesPerCity, maxCities);

    if (stateLeads.length === 0) {
      consecutiveBlocks++;
    } else {
      consecutiveBlocks = 0;
    }

    // Dedup against global set
    let newCount = 0;
    for (const lead of stateLeads) {
      const key = lead.profile_url || `${lead.firm_name}|${lead.city}|${lead.state}`;
      if (seenProfiles.has(key)) continue;
      seenProfiles.add(key);
      allLeads.push(lead);
      newCount++;
    }

    log(`  ${stateCode} result: ${stateLeads.length} scraped, ${newCount} new unique leads`);
    log(`  Running total: ${allLeads.length} unique leads`);

    // Save progress incrementally
    progress.completedStates.push(stateCode);
    progress.leads = allLeads;
    saveProgress(progress);

    // Write CSV incrementally
    writeCSV(allLeads, OUT_FILE);
    log(`  CSV saved: ${allLeads.length} leads`);

    // Delay between states
    if (totalStatesProcessed < states.length) {
      const delay = DELAY_BETWEEN_STATES + Math.random() * 2000;
      log(`  Waiting ${(delay / 1000).toFixed(1)}s before next state...`);
      await sleep(delay);
    }
  }

  await browser.close();

  // Final stats
  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`States completed: ${progress.completedStates.length}/${states.length}`);
  log(`Leads with phone: ${allLeads.filter(l => l.phone).length}`);
  log(`Leads with website: ${allLeads.filter(l => l.website).length}`);
  log(`Leads with name: ${allLeads.filter(l => l.first_name && l.last_name).length}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on full completion
  if (progress.completedStates.length >= states.length) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
    log('Progress file cleaned up (all states done).');
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
