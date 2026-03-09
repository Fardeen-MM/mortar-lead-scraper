#!/usr/bin/env node
/**
 * Justia Immigration Lawyers Scraper
 *
 * Scrapes https://www.justia.com/lawyers/immigration-law/{state}/{city}
 * using Puppeteer (Cloudflare protection requires real browser).
 *
 * Strategy:
 *   1. For each state, load the state page to get city links + state-level cards
 *   2. For each city within the state, scrape all pages
 *   3. Extract from .jld-card elements: name, phone, website, location, rating, profile URL
 *   4. Dedup by profile URL
 *   5. Output CSV
 *
 * Usage:
 *   node scripts/scrape-justia-immigration.js                    # All 51 states
 *   node scripts/scrape-justia-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-justia-immigration.js --test             # Test mode (3 states, 2 cities each)
 *   node scripts/scrape-justia-immigration.js --append           # Append to existing CSV (load existing profile URLs for dedup)
 *   node scripts/scrape-justia-immigration.js --skip-states=CA,NY --append  # Skip already-scraped states + append
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 6000;
const DELAY_BETWEEN_STATES = 8000;
const CLOUDFLARE_WAIT = 10000;
const MAX_CLOUDFLARE_RETRIES = 3;
const NAV_TIMEOUT = 45000;

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

// Ordered by legal market size (biggest immigration markets first)
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
  'source', 'profile_url', 'rating',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|phd|md|llm|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .trim();
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

function extractCity(locationText, stateCode) {
  // "Immigration Lawyer Serving Los Angeles, CA" → "Los Angeles"
  if (!locationText) return '';
  const m = locationText.match(/Serving\s+(.+?),\s*[A-Z]{2}/);
  if (m) return m[1].trim();
  const m2 = locationText.match(/in\s+(.+?),\s*[A-Z]{2}/);
  if (m2) return m2[1].trim();
  return '';
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

function cleanWebsite(url) {
  if (!url) return '';
  // Strip tracking parameters
  try {
    const u = new URL(url);
    // Remove common tracking params
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
  fs.appendFileSync(outPath, rows.join('\n') + '\n');
}

function readExistingProfileUrls(csvPath) {
  if (!fs.existsSync(csvPath)) return new Set();
  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.trim().split('\n');
  const urls = new Set();
  // profile_url is column index 13 (0-based)
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    const profileUrl = cols[13]; // profile_url column
    if (profileUrl) urls.add(profileUrl);
  }
  return urls;
}

function parseCSVLine(line) {
  const cols = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        cols.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  cols.push(current);
  return cols;
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

async function isCloudflareChallenge(page) {
  return page.evaluate(() => {
    return document.title.includes('Just a moment') ||
      document.title.includes('Attention Required') ||
      document.querySelector('#challenge-running') !== null;
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

async function extractCardsFromPage(page, stateCode) {
  return page.evaluate((sc) => {
    const cards = document.querySelectorAll('.jld-card');
    const results = [];

    cards.forEach(card => {
      const nameEl = card.querySelector('.name a');
      const phoneEl = card.querySelector('a[href^="tel:"]');
      const websiteEl = card.querySelector('a[data-vars-action*="Website"]');
      const locationEl = card.querySelector('.outline');
      const ratingEl = card.querySelector('.rating strong');

      const name = nameEl ? (nameEl.title || nameEl.textContent.trim()) : '';
      if (!name || name.length < 2) return;

      results.push({
        name,
        profileUrl: nameEl ? nameEl.href : '',
        phone: phoneEl ? phoneEl.textContent.trim() : '',
        website: websiteEl ? websiteEl.href : '',
        location: locationEl ? locationEl.textContent.trim() : '',
        rating: ratingEl ? ratingEl.textContent.trim() : '',
        stateCode: sc,
      });
    });

    return results;
  }, stateCode);
}

async function getCityLinks(page, stateSlug) {
  return page.evaluate((slug) => {
    const links = [];
    const seen = new Set();
    document.querySelectorAll('a').forEach(a => {
      const m = a.href.match(new RegExp(`/immigration-law/${slug}/([a-z][a-z0-9-]+)$`));
      if (m && !seen.has(m[1]) && m[1] !== 'all-cities' && m[1] !== 'all-counties' &&
          !m[1].includes('legal-aid') && !a.textContent.includes('Show More')) {
        seen.add(m[1]);
        links.push({ city: a.textContent.trim(), slug: m[1], href: a.href });
      }
    });
    return links;
  }, stateSlug);
}

async function scrapeStatePage(page, stateCode, stateSlug) {
  const url = `https://www.justia.com/lawyers/immigration-law/${stateSlug}`;
  log(`State page: ${stateCode} -> ${url}`);

  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  } catch (err) {
    log(`  ERROR navigating to ${stateCode}: ${err.message}`);
    return { cards: [], cityLinks: [] };
  }

  const passed = await waitForCloudflare(page);
  if (!passed) {
    log(`  BLOCKED by Cloudflare for ${stateCode} state page`);
    return { cards: [], cityLinks: [] };
  }

  const cards = await extractCardsFromPage(page, stateCode);
  const cityLinks = await getCityLinks(page, stateSlug);

  log(`  ${stateCode} state page: ${cards.length} cards, ${cityLinks.length} cities`);
  return { cards, cityLinks };
}

async function scrapeCityPage(page, stateCode, cityInfo, maxPages = 10) {
  const allCards = [];

  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    const url = pageNum === 1
      ? cityInfo.href
      : `${cityInfo.href}?page=${pageNum}`;

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`    ERROR on ${cityInfo.city} page ${pageNum}: ${err.message}`);
      break;
    }

    const passed = await waitForCloudflare(page);
    if (!passed) {
      log(`    BLOCKED by Cloudflare on ${cityInfo.city} page ${pageNum}`);
      break;
    }

    const cards = await extractCardsFromPage(page, stateCode);
    if (cards.length === 0) break;

    allCards.push(...cards);

    // Check for next page link
    const hasNext = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href*="page="]');
      let maxPage = 0;
      links.forEach(a => {
        const m = a.href.match(/page=(\d+)/);
        if (m) maxPage = Math.max(maxPage, parseInt(m[1]));
      });
      return maxPage > 0;
    });

    const currentPageFromUrl = await page.evaluate(() => {
      const m = window.location.search.match(/page=(\d+)/);
      return m ? parseInt(m[1]) : 1;
    });

    // If we're on the last page, stop
    if (!hasNext || pageNum >= maxPages) break;

    await randomDelay();
  }

  return allCards;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAppend = args.includes('--append');
  let statesFilter = null;
  let skipStates = new Set();

  const statesArg = args.find(a => a.startsWith('--states='));
  if (statesArg) {
    statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
  }

  const skipArg = args.find(a => a.startsWith('--skip-states='));
  if (skipArg) {
    skipArg.split('=')[1].split(',').map(s => s.trim().toUpperCase()).forEach(s => skipStates.add(s));
  }

  let states = statesFilter || ALL_STATES;
  if (skipStates.size > 0) {
    states = states.filter(s => !skipStates.has(s));
  }
  const maxCitiesPerState = isTest ? 2 : 999;

  if (isTest && !statesFilter) {
    states = states.slice(0, 3); // CA, NY, TX in test mode
  }

  log(`Justia Immigration Lawyers Scraper`);
  log(`States: ${states.length} | Test mode: ${isTest} | Append: ${isAppend} | Max cities/state: ${maxCitiesPerState}`);
  if (skipStates.size > 0) log(`Skipping states: ${[...skipStates].join(', ')}`);

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

  let page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Warm up session — visit homepage first to get Cloudflare cookies
  log('Warming up session...');
  try {
    await page.goto('https://www.justia.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(3000);
    // Navigate to the lawyers directory to further warm up
    await page.goto('https://www.justia.com/lawyers/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(2000);
    const warmupOk = !(await isCloudflareChallenge(page));
    log(`Session warmup: ${warmupOk ? 'OK' : 'Cloudflare challenge'}`);
    if (!warmupOk) {
      log('Waiting for Cloudflare to resolve...');
      await sleep(15000);
    }
  } catch (err) {
    log(`Warmup navigation error (non-fatal): ${err.message}`);
  }

  const outDir = path.join(__dirname, '..', 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, 'us-immigration-lawyers-justia.csv');

  const allLeads = [];
  const seenProfileUrls = new Set();

  // In append mode, load existing profile URLs to avoid duplicates
  if (isAppend && fs.existsSync(outPath)) {
    const existingUrls = readExistingProfileUrls(outPath);
    existingUrls.forEach(u => seenProfileUrls.add(u));
    log(`Loaded ${existingUrls.size} existing profile URLs for dedup`);
  }

  let totalStatesProcessed = 0;
  let consecutiveBlocks = 0;

  // Helper to create a fresh page with all settings
  async function createFreshPage() {
    const newPage = await browser.newPage();
    await newPage.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    );
    await newPage.setViewport({ width: 1920, height: 1080 });
    await newPage.setDefaultNavigationTimeout(NAV_TIMEOUT);
    return newPage;
  }

  // Detect detached frame errors
  function isDetachedFrameError(err) {
    const msg = err.message || '';
    return msg.includes('detached Frame') || msg.includes('detached frame') ||
           msg.includes('Navigating frame was detached') || msg.includes('Target closed');
  }

  for (const stateCode of states) {
    const stateSlug = STATE_SLUGS[stateCode];
    if (!stateSlug) {
      log(`Unknown state code: ${stateCode}`);
      continue;
    }

    totalStatesProcessed++;
    log(`\n[${'='.repeat(40)}]`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${stateSlug})`);

    // If too many consecutive Cloudflare blocks, recreate browser page and pause
    if (consecutiveBlocks >= 3) {
      log(`  ${consecutiveBlocks} consecutive blocks — recreating page and pausing 30s...`);
      try { await page.close(); } catch {}
      page = await createFreshPage();
      // Warm up the new page
      try {
        await page.goto('https://www.justia.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
        await sleep(3000);
        await page.goto('https://www.justia.com/lawyers/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
        await sleep(2000);
      } catch (err) {
        log(`  Warmup error (non-fatal): ${err.message}`);
      }
      await sleep(30000);
      consecutiveBlocks = 0;
    }

    // 1. Scrape state-level page (gets ~50 featured cards + city links)
    let stateCards, cityLinks;
    try {
      const result = await scrapeStatePage(page, stateCode, stateSlug);
      stateCards = result.cards;
      cityLinks = result.cityLinks;
    } catch (err) {
      if (isDetachedFrameError(err)) {
        log(`  DETACHED FRAME — recreating page...`);
        try { await page.close(); } catch {}
        page = await createFreshPage();
        try {
          await page.goto('https://www.justia.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
          await sleep(5000);
        } catch {}
        // Retry this state
        try {
          const result = await scrapeStatePage(page, stateCode, stateSlug);
          stateCards = result.cards;
          cityLinks = result.cityLinks;
        } catch {
          stateCards = [];
          cityLinks = [];
        }
      } else {
        log(`  ERROR: ${err.message}`);
        stateCards = [];
        cityLinks = [];
      }
    }

    if (stateCards.length === 0 && cityLinks.length === 0) {
      consecutiveBlocks++;
    } else {
      consecutiveBlocks = 0;
    }

    let stateLeadCount = 0;

    // Add state-level cards
    for (const card of stateCards) {
      if (card.profileUrl && seenProfileUrls.has(card.profileUrl)) continue;
      if (card.profileUrl) seenProfileUrls.add(card.profileUrl);

      const { first_name, last_name } = parseName(card.name);
      const city = extractCity(card.location, stateCode);

      allLeads.push({
        first_name,
        last_name,
        firm_name: '',
        title: '',
        email: '',
        phone: cleanPhone(card.phone),
        website: cleanWebsite(card.website),
        domain: extractDomain(card.website),
        city,
        state: stateCode,
        country: 'US',
        niche: 'Immigration Law',
        source: 'justia',
        profile_url: card.profileUrl,
        rating: card.rating,
      });
      stateLeadCount++;
    }

    // 2. Scrape city pages
    const citiesToScrape = cityLinks.slice(0, maxCitiesPerState);
    let consecutiveZeroCities = 0;
    const MAX_ZERO_CITIES = 10; // Skip remaining cities if 10+ consecutive return 0 new

    for (let ci = 0; ci < citiesToScrape.length; ci++) {
      // Early exit: if many consecutive cities return 0 new, the rest likely will too
      if (consecutiveZeroCities >= MAX_ZERO_CITIES) {
        log(`  Skipping remaining ${citiesToScrape.length - ci} cities (${MAX_ZERO_CITIES} consecutive zero-new cities)`);
        break;
      }

      const cityInfo = citiesToScrape[ci];
      log(`  City ${ci + 1}/${citiesToScrape.length}: ${cityInfo.city}`);

      await randomDelay();

      let cityCards;
      try {
        cityCards = await scrapeCityPage(page, stateCode, cityInfo, isTest ? 2 : 10);
      } catch (err) {
        if (isDetachedFrameError(err)) {
          log(`    DETACHED FRAME on city ${cityInfo.city} — recreating page...`);
          try { await page.close(); } catch {}
          page = await createFreshPage();
          try {
            await page.goto('https://www.justia.com/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
            await sleep(5000);
          } catch {}
          // Retry this city
          try {
            cityCards = await scrapeCityPage(page, stateCode, cityInfo, isTest ? 2 : 10);
          } catch {
            cityCards = [];
          }
        } else {
          log(`    ERROR on city ${cityInfo.city}: ${err.message}`);
          cityCards = [];
        }
      }

      let cityNew = 0;

      for (const card of cityCards) {
        if (card.profileUrl && seenProfileUrls.has(card.profileUrl)) continue;
        if (card.profileUrl) seenProfileUrls.add(card.profileUrl);

        const { first_name, last_name } = parseName(card.name);
        const city = extractCity(card.location, stateCode) || cityInfo.city;

        allLeads.push({
          first_name,
          last_name,
          firm_name: '',
          title: '',
          email: '',
          phone: cleanPhone(card.phone),
          website: cleanWebsite(card.website),
          domain: extractDomain(card.website),
          city,
          state: stateCode,
          country: 'US',
          niche: 'Immigration Law',
          source: 'justia',
          profile_url: card.profileUrl,
          rating: card.rating,
        });
        cityNew++;
        stateLeadCount++;
      }

      if (cityNew === 0) {
        consecutiveZeroCities++;
      } else {
        consecutiveZeroCities = 0;
      }

      log(`    ${cityInfo.city}: ${(cityCards || []).length} cards, ${cityNew} new`);
    }

    log(`  ${stateCode} total: ${stateLeadCount} unique leads`);
    log(`  Running total: ${allLeads.length} leads`);

    // Periodic save: append leads for this state immediately so progress isn't lost
    if (isAppend && stateLeadCount > 0) {
      const stateLeads = allLeads.slice(allLeads.length - stateLeadCount);
      appendCSV(stateLeads, outPath);
      log(`  Saved ${stateLeadCount} leads for ${stateCode} to CSV`);
    }

    // Delay between states
    if (totalStatesProcessed < states.length) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 3000);
    }
  }

  await browser.close();

  // Write output (in append mode, leads are already saved per-state above)
  if (!isAppend) {
    writeCSV(allLeads, outPath);
  }

  log(`\n${'='.repeat(60)}`);
  log(`DONE`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}/${states.length}`);
  log(`Leads with phone: ${allLeads.filter(l => l.phone).length}`);
  log(`Leads with website: ${allLeads.filter(l => l.website).length}`);
  log(`Leads with rating: ${allLeads.filter(l => l.rating).length}`);
  log(`Output: ${outPath}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
