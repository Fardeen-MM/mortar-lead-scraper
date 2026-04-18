#!/usr/bin/env node
/**
 * Super Lawyers Immigration Attorney Scraper (Puppeteer + Stealth)
 *
 * Spec URL pattern: https://attorneys.superlawyers.com/immigration/{state}/?page=N
 *
 * IMPORTANT DISCOVERY (2026-04-17): The /immigration/{state}/ route is an SEO hub
 * with NO attorney listings (zero profile links). Actual attorney cards live at
 * /immigration/{state}/{city}/ — pages titled "Top Rated Immigration Lawyers in
 * {City}, {ST}". Profile URLs are on profiles.superlawyers.com/{state}/{city}/lawyer/...
 *
 * Cloudflare protection: bypassed successfully via puppeteer-extra-stealth +
 * AutomationControlled disabled + navigator.webdriver override.
 *
 * Strategy:
 *   - For each state, iterate curated city list (major immigration markets)
 *   - For each city page, extract h2.full-name + nearest anchor to profiles.superlawyers.com/.../lawyer/...
 *   - Paginate up to MAX_PAGES_PER_STATE per city via ?page=N
 *   - Visit each profile to scrape email/phone/website/firm/location
 *   - Resumable state file (saved after each state)
 *
 * Output: output/super-lawyers-immigration-us.csv
 * CSV: first_name, last_name, firm_name, email, phone, website, city, state,
 *      profile_url, country, niche, source
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// --- Config ---
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36';
const VIEWPORT = { width: 1366, height: 768 };
const CLOUDFLARE_WAIT_MS = 20000;
const CLOUDFLARE_POLL_MS = 1000;
const NAV_TIMEOUT_MS = 60000;
const INTER_PAGE_MIN_MS = 2500;
const INTER_PAGE_MAX_MS = 3500;
const MAX_PAGES_PER_CITY_DEFAULT = 20;

const ROOT = path.join(__dirname, '..');
const OUTPUT_DIR = path.join(ROOT, 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'super-lawyers-immigration-us.csv');
const STATE_FILE = path.join(OUTPUT_DIR, 'super-lawyers-immigration-us.state.json');

// State slug → 2-letter code. Caller-requested 10 states:
const STATE_MAP = {
  california: 'CA',
  texas: 'TX',
  'new-york': 'NY',
  florida: 'FL',
  illinois: 'IL',
  'new-jersey': 'NJ',
  massachusetts: 'MA',
  washington: 'WA',
  virginia: 'VA',
  georgia: 'GA',
};
const DEFAULT_STATES = Object.keys(STATE_MAP);

// Curated major immigration markets per state (for the targeted 10).
// Each state has 4-10 top cities; each city page paginates individually.
const STATE_CITIES = {
  california: [
    'los-angeles', 'san-francisco', 'san-diego', 'san-jose', 'sacramento',
    'oakland', 'irvine', 'fresno', 'beverly-hills', 'pasadena',
  ],
  texas: [
    'houston', 'dallas', 'san-antonio', 'austin', 'el-paso',
    'fort-worth', 'mcallen', 'laredo', 'plano',
  ],
  'new-york': [
    'new-york', 'brooklyn', 'buffalo', 'rochester', 'albany',
    'white-plains', 'queens', 'garden-city',
  ],
  florida: [
    'miami', 'orlando', 'tampa', 'jacksonville', 'fort-lauderdale',
    'west-palm-beach', 'coral-gables', 'boca-raton',
  ],
  illinois: [
    'chicago', 'springfield', 'naperville', 'skokie', 'rockford',
  ],
  'new-jersey': [
    'newark', 'jersey-city', 'trenton', 'edison', 'hackensack',
    'princeton', 'morristown',
  ],
  massachusetts: [
    'boston', 'cambridge', 'worcester', 'springfield', 'waltham',
  ],
  washington: [
    'seattle', 'tacoma', 'spokane', 'bellevue', 'everett',
  ],
  virginia: [
    'arlington', 'fairfax', 'richmond', 'virginia-beach', 'alexandria',
    'mclean', 'tysons-corner',
  ],
  georgia: [
    'atlanta', 'savannah', 'augusta', 'marietta', 'duluth',
  ],
};

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
  'city', 'state', 'profile_url', 'country', 'niche', 'source',
];

// --- CLI ---
const args = process.argv.slice(2);
function argVal(flag) { const i = args.indexOf(flag); return i >= 0 ? args[i + 1] : null; }
const TEST_MODE = args.includes('--test');
const PROBE_MODE = args.includes('--probe');
const TARGET_MAX_ATTORNEYS = parseInt(argVal('--max') || '50', 10);
const MAX_PAGES_PER_CITY = parseInt(argVal('--max-pages') || String(MAX_PAGES_PER_CITY_DEFAULT), 10);
const MAX_PROFILE_VISITS = parseInt(argVal('--profile-max') || '9999', 10);
const STATES_FILTER = argVal('--states')?.split(',').map(s => s.trim().toLowerCase());

// --- Helpers ---
const sleep = ms => new Promise(r => setTimeout(r, ms));
const rand = (min, max) => sleep(min + Math.random() * (max - min));

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|ph\.?d\.?|ll\.?m\.?|jr\.?|sr\.?|iii?|iv)$/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = cleaned.split(' ').filter(Boolean);
  if (!parts.length) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v).replace(/[\r\n]+/g, ' ').trim();
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function titleCase(s) {
  return String(s || '').replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {}
  return { completed_states: [], scraped_profile_urls: [] };
}

function saveState(state) {
  try {
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
  } catch (e) { console.warn('state save err:', e.message); }
}

function loadExistingRows() {
  if (!fs.existsSync(OUTPUT_FILE)) return { rows: [], urls: new Set() };
  const text = fs.readFileSync(OUTPUT_FILE, 'utf8').split(/\r?\n/).filter(Boolean);
  const rows = []; const urls = new Set();
  text.shift(); // header
  for (const line of text) {
    const parts = line.split(',');
    const url = (parts[8] || '').replace(/^"|"$/g, '').trim();
    if (url) urls.add(url);
    rows.push(line);
  }
  return { rows, urls };
}

function writeCsv(rows) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(OUTPUT_FILE, [CSV_COLUMNS.join(','), ...rows].join('\n') + '\n', 'utf8');
}

function rowToCsv(row) {
  return CSV_COLUMNS.map(c => csvEscape(row[c] || '')).join(',');
}

// --- Puppeteer bootstrap ---
async function launchBrowser() {
  const launchArgs = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-blink-features=AutomationControlled',
    `--user-agent=${USER_AGENT}`,
    '--window-size=1366,768',
    '--disable-dev-shm-usage',
  ];
  const launchOpts = { headless: 'new', args: launchArgs, defaultViewport: VIEWPORT };
  if (process.env.PUPPETEER_EXECUTABLE_PATH) {
    launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  }
  return puppeteer.launch(launchOpts);
}

async function hardenPage(page) {
  await page.setUserAgent(USER_AGENT);
  await page.setViewport(VIEWPORT);
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Upgrade-Insecure-Requests': '1',
  });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    try {
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
      Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
      window.chrome = window.chrome || { runtime: {} };
    } catch {}
  });
  page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
}

async function isChallengePage(page) {
  try {
    const title = (await page.title()) || '';
    if (/Just a moment|Checking your browser|Attention Required/i.test(title)) return true;
    const html = await page.content();
    if (/cf-browser-verification|cf_chl_opt|__cf_chl_rt_tk/i.test(html)) {
      const hasResults = /full-name|lawyer|profile|result-item|pagination/i.test(html);
      if (!hasResults) return true;
    }
  } catch {}
  return false;
}

async function waitPastCloudflare(page) {
  const deadline = Date.now() + CLOUDFLARE_WAIT_MS;
  while (Date.now() < deadline) {
    if (!(await isChallengePage(page))) return true;
    await sleep(CLOUDFLARE_POLL_MS);
  }
  return !(await isChallengePage(page));
}

async function navigate(page, url) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
  await waitPastCloudflare(page);
  try { await page.waitForNetworkIdle({ idleTime: 1200, timeout: 8000 }); } catch {}
}

// --- Extraction ---

/**
 * Extract attorney cards from a Super Lawyers city page.
 *
 * Selector strategy (live-inspected 2026-04-17):
 *   - <h2 class="full-name fw-bold ..."> Name </h2>
 *   - nearest ancestor container holds firm/city/profile link
 *   - profile link: <a href="https://profiles.superlawyers.com/{state}/{city}/lawyer/{slug}/{uuid}.html">
 *   - firm link:    <a href="https://profiles.superlawyers.com/{state}/{city}/lawfirm/{slug}/{uuid}.html">
 */
async function extractListPage(page) {
  return page.evaluate(() => {
    const out = [];
    const seen = new Set();
    const headings = document.querySelectorAll('h2.full-name');
    headings.forEach(h => {
      try {
        const name = (h.textContent || '').trim();
        if (!name) return;
        // walk up to the profile card (closest ancestor containing a profiles.superlawyers.com/lawyer link)
        let container = h.closest('article, .card, [class*="card"], [class*="result"], [class*="lawyer"]');
        if (!container) container = h.parentElement;
        // look up up to 6 levels for a profile anchor
        let anc = container;
        let profileAnchor = null;
        for (let depth = 0; depth < 6 && anc && !profileAnchor; depth++) {
          profileAnchor = anc.querySelector('a[href*="profiles.superlawyers.com"][href*="/lawyer/"]');
          if (!profileAnchor) anc = anc.parentElement;
        }
        const profileUrl = profileAnchor
          ? profileAnchor.href.split('?')[0]
          : null;
        if (!profileUrl) return;
        if (seen.has(profileUrl)) return;
        seen.add(profileUrl);

        // firm
        let firm = '';
        const firmAnchor = (anc || container)?.querySelector('a[href*="/lawfirm/"]');
        if (firmAnchor) firm = (firmAnchor.textContent || '').trim();

        // city/state from profile URL pattern /{state}/{city}/lawyer/
        let city = '', state = '';
        try {
          const u = new URL(profileUrl);
          const parts = u.pathname.split('/').filter(Boolean);
          if (parts.length >= 3 && parts[2] === 'lawyer') {
            state = parts[0].replace(/-/g, ' ');
            city = parts[1].replace(/-/g, ' ');
          }
        } catch {}

        // phone from nearest container
        let phone = '';
        const tel = (anc || container)?.querySelector('a[href^="tel:"]');
        if (tel) phone = tel.href.replace('tel:', '').replace(/^\+1/, '').trim();

        out.push({ name, firm, phone, profile_url: profileUrl, city, state });
      } catch {}
    });
    return out;
  });
}

async function detectPageCount(page) {
  return page.evaluate(() => {
    let max = 1;
    const candidates = document.querySelectorAll('.pagination a, .pagination li a, nav[aria-label*="Pag" i] a, a[href*="page="]');
    candidates.forEach(a => {
      const txt = (a.textContent || '').trim();
      const n = parseInt(txt, 10);
      if (Number.isFinite(n) && n > max) max = n;
      const m = (a.href || '').match(/[?&]page=(\d+)/);
      if (m) { const n2 = parseInt(m[1], 10); if (Number.isFinite(n2) && n2 > max) max = n2; }
    });
    return max;
  });
}

async function scrapeProfile(page, url) {
  try {
    await navigate(page, url);
  } catch { return {}; }
  return page.evaluate(() => {
    const out = { email: '', phone: '', website: '', firm_name: '', city: '', state: '' };

    // email: look for mailto OR "contact" link leading to a form, OR plain-text email
    const mailto = document.querySelector('a[href^="mailto:"]');
    if (mailto) {
      const e = mailto.href.replace('mailto:', '').split('?')[0].trim();
      if (e && /@/.test(e)) out.email = e;
    }
    if (!out.email) {
      const bodyText = document.body.innerText || '';
      const m = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}/);
      if (m) out.email = m[0];
    }
    // phone
    const tel = document.querySelector('a[href^="tel:"]');
    if (tel) out.phone = tel.href.replace('tel:', '').replace(/^\+1/, '').trim();

    // website — first outbound http(s) link that isn't superlawyers.com / social
    const bad = /(superlawyers\.com|findlaw|lawinfo|abogado|facebook|twitter|linkedin|instagram|youtube|internetbrands|onetrust|cookiepedia)/i;
    const anchors = document.querySelectorAll('a[href]');
    for (const a of anchors) {
      const h = a.href || '';
      if (!h.startsWith('http')) continue;
      if (bad.test(h)) continue;
      if (h.includes('tel:') || h.includes('mailto:') || h.includes('javascript:')) continue;
      try {
        const u = new URL(h);
        ['adSubId','utm_source','utm_medium','utm_campaign','utm_content','utm_term','lnasid','exp','src','tsid'].forEach(k => u.searchParams.delete(k));
        out.website = u.toString();
      } catch { out.website = h; }
      break;
    }

    // firm
    const firmAnchor = document.querySelector('a[href*="/lawfirm/"]');
    if (firmAnchor) out.firm_name = (firmAnchor.textContent || '').trim();
    if (!out.firm_name) {
      const firmEl = document.querySelector('.firm-name, [itemprop="worksFor"], .lawyer-firm');
      if (firmEl) out.firm_name = (firmEl.textContent || '').trim();
    }

    // city/state — try microdata, fall back to URL path
    const locEl = document.querySelector('[itemprop="addressLocality"]');
    const stEl = document.querySelector('[itemprop="addressRegion"]');
    if (locEl) out.city = (locEl.textContent || '').trim();
    if (stEl) out.state = (stEl.textContent || '').trim();
    if (!out.city || !out.state) {
      try {
        const parts = location.pathname.split('/').filter(Boolean);
        if (parts.length >= 3 && parts[2] === 'lawyer') {
          if (!out.state) out.state = parts[0].replace(/-/g, ' ');
          if (!out.city) out.city = parts[1].replace(/-/g, ' ');
        }
      } catch {}
    }
    return out;
  });
}

// --- Probe ---
async function probe() {
  const browser = await launchBrowser();
  const page = await browser.newPage();
  await hardenPage(page);
  // Use the city page (state hub has no attorneys)
  const url = 'https://attorneys.superlawyers.com/immigration/california/los-angeles/';
  console.log(`[probe] navigating ${url}`);
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
  } catch (e) { console.log(`[probe] goto err: ${e.message}`); }
  console.log('[probe] sleeping 15s for Cloudflare...');
  await sleep(15000);
  const title = await page.title().catch(() => '');
  const bypassed = !(await isChallengePage(page));
  const listings = bypassed ? await extractListPage(page) : [];
  const pageCount = bypassed ? await detectPageCount(page) : 0;
  const summary = bypassed ? await page.evaluate(() => ({
    h2FullNameCount: document.querySelectorAll('h2.full-name').length,
    lawyerHrefCount: document.querySelectorAll('a[href*="profiles.superlawyers.com"][href*="/lawyer/"]').length,
    title: document.title,
  })) : {};
  await browser.close();
  return { title, bypassed, summary, listings, pageCount };
}

// --- City scrape ---
async function scrapeCity(page, stateSlug, stateCode, citySlug, { resumeUrls, maxPages, cap }) {
  const listings = [];
  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    if (cap && listings.length >= cap) break;
    const url = `https://attorneys.superlawyers.com/immigration/${stateSlug}/${citySlug}/${pageNum > 1 ? `?page=${pageNum}` : ''}`;
    console.log(`    [p${pageNum}] ${url}`);
    try { await navigate(page, url); }
    catch (e) { console.log(`      nav err: ${e.message}`); break; }
    if (await isChallengePage(page)) { console.log('      cloudflare blocked, stopping city'); break; }
    const found = await extractListPage(page);
    console.log(`      found ${found.length} attorneys`);
    if (!found.length) break;
    let newThisPage = 0;
    for (const l of found) {
      if (resumeUrls && resumeUrls.has(l.profile_url)) continue;
      // overlay city/state from slugs where missing
      if (!l.state) l.state = stateCode;
      if (!l.city) l.city = titleCase(citySlug);
      listings.push(l);
      newThisPage++;
      if (cap && listings.length >= cap) break;
    }
    if (newThisPage === 0) break; // no new -> end of meaningful pagination
    await rand(INTER_PAGE_MIN_MS, INTER_PAGE_MAX_MS);
  }
  return listings;
}

// --- Main ---
async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // PROBE
  if (PROBE_MODE && !TEST_MODE) {
    console.log('=== Super Lawyers Cloudflare PROBE ===');
    const r = await probe();
    console.log(`title: ${r.title}`);
    console.log(`bypassed: ${r.bypassed}`);
    console.log(`selector summary: ${JSON.stringify(r.summary)}`);
    console.log(`listings found (p1): ${r.listings.length}`);
    console.log(`first 3 names: ${r.listings.slice(0, 3).map(l => l.name).join(' | ')}`);
    console.log(`pagination max detected: ${r.pageCount}`);
    if (!r.bypassed) {
      console.log('\nRESULT: Cloudflare blocked — NOT trying more bypasses. Moving on.');
    }
    process.exit(0);
  }

  // TEST (proof of concept)
  if (TEST_MODE) {
    console.log('=== Super Lawyers TEST (California, up to ' + TARGET_MAX_ATTORNEYS + ' attorneys) ===');
    const browser = await launchBrowser();
    const page = await browser.newPage();
    await hardenPage(page);

    // First: probe
    console.log('\n[probe] California / Los Angeles city page');
    try {
      await page.goto('https://attorneys.superlawyers.com/immigration/california/los-angeles/', { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
    } catch (e) { console.log(`goto err: ${e.message}`); }
    await sleep(15000);
    const probeTitle = await page.title();
    const probeBypass = !(await isChallengePage(page));
    console.log(`[probe] title: "${probeTitle}"`);
    console.log(`[probe] bypassed: ${probeBypass ? 'YES' : 'NO'}`);
    if (!probeBypass) {
      console.log('[probe] Cloudflare blocked — aborting.');
      await browser.close();
      process.exit(0);
    }
    const probeListings = await extractListPage(page);
    const probePages = await detectPageCount(page);
    console.log(`[probe] LA p1 listings: ${probeListings.length}`);
    console.log(`[probe] first 3 names: ${probeListings.slice(0, 3).map(l => l.name).join(' | ')}`);
    console.log(`[probe] LA pages detected: ${probePages}`);

    // Now iterate California cities until we hit target
    const stateSlug = 'california';
    const stateCode = 'CA';
    const cities = STATE_CITIES[stateSlug];
    const { rows: existingRows, urls: existingUrls } = loadExistingRows();
    const newRows = [];
    const cityTotals = {};
    const collected = [];

    for (const citySlug of cities) {
      if (collected.length >= TARGET_MAX_ATTORNEYS) break;
      console.log(`\n  city: ${citySlug}`);
      const remaining = TARGET_MAX_ATTORNEYS - collected.length;
      const cityListings = await scrapeCity(page, stateSlug, stateCode, citySlug, {
        resumeUrls: existingUrls,
        maxPages: 3, // in test mode, cap
        cap: remaining,
      });
      cityTotals[citySlug] = cityListings.length;
      collected.push(...cityListings);
    }

    console.log(`\n  scraped ${collected.length} cards; enriching ${Math.min(collected.length, MAX_PROFILE_VISITS)} profiles...`);
    for (let i = 0; i < Math.min(collected.length, MAX_PROFILE_VISITS); i++) {
      const l = collected[i];
      console.log(`   [${i+1}/${collected.length}] ${l.name}`);
      let enriched = {};
      try { enriched = await scrapeProfile(page, l.profile_url); }
      catch (e) { console.log(`     err: ${e.message}`); }
      const { first_name, last_name } = parseName(l.name);
      const cityDisp = enriched.city && /[a-z]/.test(enriched.city) ? titleCase(enriched.city) : titleCase(l.city || '');
      const stateDisp = (enriched.state && enriched.state.length === 2) ? enriched.state.toUpperCase() : (l.state || 'CA');
      const row = {
        first_name,
        last_name,
        firm_name: enriched.firm_name || l.firm || '',
        email: enriched.email || '',
        phone: enriched.phone || l.phone || '',
        website: enriched.website || '',
        city: cityDisp,
        state: stateDisp === 'CA' || stateDisp.length === 2 ? stateDisp : 'CA',
        profile_url: l.profile_url,
        country: 'US',
        niche: 'immigration attorney',
        source: 'super_lawyers',
      };
      newRows.push(rowToCsv(row));
      await rand(INTER_PAGE_MIN_MS, INTER_PAGE_MAX_MS);
    }

    writeCsv([...existingRows, ...newRows]);
    const withEmail = newRows.filter(r => r.split(',')[3]).length;
    const withPhone = newRows.filter(r => r.split(',')[4]).length;
    const withWebsite = newRows.filter(r => r.split(',')[5]).length;
    console.log(`\nTEST complete — wrote ${newRows.length} new rows to ${OUTPUT_FILE}`);
    console.log(`  per-city counts: ${JSON.stringify(cityTotals)}`);
    console.log(`  contact coverage: phone=${withPhone} website=${withWebsite} email=${withEmail}`);
    await browser.close();
    process.exit(0);
  }

  // FULL
  console.log('=== Super Lawyers Full Scrape ===');
  const statesToRun = (STATES_FILTER && STATES_FILTER.length)
    ? STATES_FILTER.filter(s => STATE_MAP[s])
    : DEFAULT_STATES;
  console.log(`States: ${statesToRun.join(', ')}`);

  const browser = await launchBrowser();
  const page = await browser.newPage();
  await hardenPage(page);

  const persisted = loadState();
  const completedSet = new Set(persisted.completed_states || []);
  const { rows: existingRows, urls: existingUrls } = loadExistingRows();
  const resumeUrls = new Set([...(persisted.scraped_profile_urls || []), ...existingUrls]);

  const newRows = [];

  for (const stateSlug of statesToRun) {
    if (completedSet.has(stateSlug)) { console.log(`[skip] ${stateSlug} already complete`); continue; }
    const stateCode = STATE_MAP[stateSlug];
    const cities = STATE_CITIES[stateSlug] || [];
    console.log(`\n[state] ${stateSlug} (${stateCode}) — ${cities.length} cities`);
    const stateListings = [];

    for (const citySlug of cities) {
      console.log(`  city: ${citySlug}`);
      try {
        const cityListings = await scrapeCity(page, stateSlug, stateCode, citySlug, {
          resumeUrls,
          maxPages: MAX_PAGES_PER_CITY,
        });
        stateListings.push(...cityListings);
      } catch (e) { console.log(`    err: ${e.message}`); }
      await rand(INTER_PAGE_MIN_MS, INTER_PAGE_MAX_MS);
    }

    // enrich
    console.log(`  collected ${stateListings.length}; enriching profiles...`);
    for (let i = 0; i < stateListings.length; i++) {
      const l = stateListings[i];
      if (resumeUrls.has(l.profile_url)) continue;
      try {
        const e = await scrapeProfile(page, l.profile_url);
        const { first_name, last_name } = parseName(l.name);
        const row = {
          first_name, last_name,
          firm_name: e.firm_name || l.firm || '',
          email: e.email || '',
          phone: e.phone || l.phone || '',
          website: e.website || '',
          city: (e.city && /[a-z]/.test(e.city)) ? titleCase(e.city) : titleCase(l.city || ''),
          state: (e.state && e.state.length === 2) ? e.state.toUpperCase() : stateCode,
          profile_url: l.profile_url,
          country: 'US',
          niche: 'immigration attorney',
          source: 'super_lawyers',
        };
        newRows.push(rowToCsv(row));
        resumeUrls.add(l.profile_url);
      } catch (err) {
        console.log(`    profile err: ${err.message}`);
      }
      if ((i + 1) % 25 === 0) {
        writeCsv([...existingRows, ...newRows]);
        saveState({
          completed_states: Array.from(completedSet),
          scraped_profile_urls: Array.from(resumeUrls),
          updated_at: new Date().toISOString(),
        });
      }
      await rand(INTER_PAGE_MIN_MS, INTER_PAGE_MAX_MS);
    }

    completedSet.add(stateSlug);
    saveState({
      completed_states: Array.from(completedSet),
      scraped_profile_urls: Array.from(resumeUrls),
      updated_at: new Date().toISOString(),
    });
    writeCsv([...existingRows, ...newRows]);
    console.log(`  state ${stateSlug} done. running total new rows: ${newRows.length}`);
  }

  await browser.close();
  console.log(`\nDone. Wrote ${newRows.length} new rows to ${OUTPUT_FILE}`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
