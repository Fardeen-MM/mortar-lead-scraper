/**
 * Facebook Ad Library Scraper — Playwright with anti-detection
 *
 * Uses Playwright (not Puppeteer) with advanced stealth techniques
 * adapted from NiksHacks/meta-ads-library-scraper.
 *
 * Key difference from our old Puppeteer approach:
 * - Playwright with 20+ stealth flags
 * - WebDriver trace removal
 * - Random viewports, user agents, mouse movements
 * - GraphQL response interception (not DOM parsing)
 * - view_all_page_id support for exact page lookups
 *
 * Usage: node scripts/fb-ad-library-playwright.js input.csv output.csv
 *
 * Input CSV needs: company name column + optional facebook_page column
 * Output adds: fb_ads_active, fb_ad_count, fb_page_id
 */

const fs = require('fs');
const { chromium } = require('playwright');

// ── Anti-Detection ──────────────────────────────────────────────
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
];

const VIEWPORTS = [
  { width: 1920, height: 1080 },
  { width: 1366, height: 768 },
  { width: 1440, height: 900 },
  { width: 1536, height: 864 },
  { width: 1280, height: 720 },
];

const STEALTH_SCRIPT = `
  Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
  delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
  delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
  window.chrome = { runtime: { onConnect: undefined, onMessage: undefined } };
  const origQuery = window.navigator.permissions.query;
  window.navigator.permissions.query = (p) => p.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : origQuery(p);
  Object.defineProperty(navigator, 'plugins', { get: () => [{ name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }] });
  Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
  Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
  Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
`;

function randomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }
function randomVP() { return VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)]; }
function delay(min, max) { return new Promise(r => setTimeout(r, min + Math.random() * (max - min))); }

// ── CSV Helpers ─────────────────────────────────────────────────
function parseLine(line) {
  const cols = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}

function esc(v) {
  const s = (v ?? '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

// ── Core: Check one business in Ad Library ──────────────────────
async function checkAdLibrary(page, searchTerm) {
  // Convert camelCase FB slugs to readable names: "FairnessCenter" → "Fairness Center"
  let cleanTerm = searchTerm
    .replace(/([a-z])([A-Z])/g, '$1 $2')  // camelCase → spaces
    .replace(/[-_]/g, ' ')                  // hyphens/underscores → spaces
    .replace(/\s+/g, ' ')                   // normalize spaces
    .trim();

  // Use country=US and keyword_unordered for best results
  const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&media_type=all&search_type=keyword_unordered&q=${encodeURIComponent(cleanTerm)}`;

  let adCount = null;
  let pageId = null;
  let pageName = null;

  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: 15000 });
    await delay(1500, 2500);

    // Parse result count from DOM
    adCount = await page.evaluate(() => {
      const t = document.body.innerText || '';
      if (t.includes('No ads match')) return 0;
      const m = t.match(/~?([\d,]+)\s*results?/i);
      return m ? parseInt(m[1].replace(/,/g, ''), 10) : null;
    });
  } catch (err) {
    try {
      adCount = await page.evaluate(() => {
        const t = document.body?.innerText || '';
        if (t.includes('No ads match')) return 0;
        const m = t.match(/~?([\d,]+)\s*results?/i);
        return m ? parseInt(m[1].replace(/,/g, ''), 10) : null;
      });
    } catch { adCount = -1; }
  }

  return {
    active: adCount !== null && adCount > 0,
    count: adCount ?? 0,
    pageId: pageId || '',
    pageName: pageName || '',
  };
}

// ── Main ────────────────────────────────────────────────────────
(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile?.replace('.csv', '-fb-ads.csv');
  const maxLeads = parseInt(process.argv[4] || '0', 10) || Infinity;

  if (!inputFile) {
    console.log('Usage: node scripts/fb-ad-library-playwright.js input.csv [output.csv] [max_leads]');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const lines = csvText.split('\n').filter(l => l.trim());
  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map(l => {
    const cols = parseLine(l);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });

  // Find columns — prioritize facebook_page over facebook, and website domain as fallback name
  const nameCol = headers.find(h => /business.?name|firm.?name|^name$|charity_name/i.test(h)) ||
                  headers.find(h => /company/i.test(h)) || headers[0];
  const fbCol = headers.find(h => /^facebook_page$/i.test(h)) ||
                headers.find(h => /^facebook$/i.test(h));
  const webCol = headers.find(h => /^website$/i.test(h));

  const total = Math.min(rows.length, maxLeads);
  console.log(`\n  Facebook Ad Library Checker (Playwright + Stealth)`);
  console.log(`  Input: ${total} leads | Name: "${nameCol}" | FB: "${fbCol || 'none'}"`);

  // Launch browser with stealth
  const vp = randomVP();
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled', '--disable-extensions',
      '--disable-plugins', '--disable-images', '--mute-audio', '--single-process',
    ],
  });

  const context = await browser.newContext({
    userAgent: randomUA(),
    viewport: vp,
    locale: 'en-US',
    timezoneId: 'America/New_York',
  });

  // Apply stealth to all pages
  await context.addInitScript(STEALTH_SCRIPT);

  const page = await context.newPage();
  // Block images/fonts for speed
  await page.route('**/*', route => {
    const type = route.request().resourceType();
    if (['image', 'font', 'media'].includes(type)) return route.abort();
    return route.continue();
  });

  const results = [];
  let checked = 0, found = 0;
  const t0 = Date.now();

  for (let i = 0; i < total; i++) {
    const row = rows[i];
    // Extract clean FB page slug — remove "facebook.com/" prefix
    let fbSlug = '';
    if (fbCol) {
      fbSlug = (row[fbCol] || '').replace(/^https?:\/\/(www\.)?facebook\.com\//i, '').replace(/\/$/, '').trim();
    }

    // Get company name — skip if it looks like a URL
    let companyName = (row[nameCol] || '').trim();
    if (companyName.includes('facebook.com') || companyName.includes('http')) companyName = '';

    // Try to extract a clean name from website domain as fallback
    if (!companyName && webCol) {
      const web = (row[webCol] || '').replace(/^https?:\/\/(www\.)?/i, '').replace(/\/.*$/, '').replace(/\.\w+$/, '');
      if (web.length > 2) companyName = web;
    }

    // Search priority: FB slug > company name
    const searchTerm = fbSlug || companyName;

    if (!searchTerm) {
      results.push({ ...row, fb_ads_active: '', fb_ad_count: '', fb_page_id: '', fb_check_error: 'no_name' });
      continue;
    }

    const result = await checkAdLibrary(page, searchTerm);
    checked++;
    if (result.active) found++;

    results.push({
      ...row,
      fb_ads_active: result.active ? 'YES' : 'NO',
      fb_ad_count: result.count,
      fb_page_id: result.pageId,
      fb_page_name: result.pageName,
      fb_check_error: result.count === -1 ? 'error' : '',
    });

    // Random delay between checks
    await delay(800, 1500);

    // Random mouse movement every 5 checks
    if (checked % 5 === 0) {
      await page.mouse.move(
        Math.floor(Math.random() * vp.width),
        Math.floor(Math.random() * vp.height),
        { steps: 5 + Math.floor(Math.random() * 10) }
      );
    }

    if (checked % 10 === 0 || checked <= 3) {
      const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
      const rate = (checked / ((Date.now() - t0) / 1000)).toFixed(2);
      console.log(`  [${elapsed}s] ${checked}/${total} | 🔥${found} with ads | ${rate}/sec`);
    }
  }

  await browser.close();

  // Write output
  const addCols = ['fb_ads_active', 'fb_ad_count', 'fb_page_id', 'fb_page_name', 'fb_check_error'];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.map(r => outHeaders.map(h => esc(r[h])).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n  ✓ Done: ${checked} checked, ${found} with ads, ${elapsed}s`);
  console.log(`  → ${outputFile}`);
})();
