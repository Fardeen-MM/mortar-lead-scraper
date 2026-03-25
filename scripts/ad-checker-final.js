/**
 * Ad Checker Final v2 — Production-ready Meta + Google ad detection
 *
 * Two-pass system:
 *   Pass 1 (HTTP, 50 concurrent, ~12/sec):
 *     - Meta Pixel, Google Ads tag, Google Analytics, TikTok, LinkedIn
 *     - Extracts Facebook page URL from website
 *     - Extracts pixel/tag IDs
 *
 *   Pass 2 (Puppeteer, 4 tabs, ~0.85/sec, negatives only):
 *     - Searches Facebook Ad Library by FB slug (if found) then business name
 *     - Tab crash recovery — auto-creates replacement tabs
 *     - Auto-saves progress every 50 checks
 *     - Graceful shutdown on SIGINT
 *
 * Usage: node scripts/ad-checker-final.js input.csv [output.csv]
 */

const fs = require('fs');
const https = require('https');
const http = require('http');
const path = require('path');

// ── Config ──────────────────────────────────────────────────────
const HTTP_CONCURRENCY = 50;
const PUPPETEER_CONCURRENCY = 4;
const HTTP_TIMEOUT = 8000;
const MAX_REDIRECTS = 5;
const PUPPETEER_NAV_TIMEOUT = 12000;
const PUPPETEER_WAIT = 1000;
const AD_LIBRARY_BASE = 'https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&media_type=all&search_type=keyword_exact_phrase&q=';

// ── Detection Patterns ──────────────────────────────────────────
const PATTERNS = {
  meta_pixel: [
    /fbq\s*\(/i,
    /facebook\.com\/tr\?/i,
    /connect\.facebook\.net\/[a-z_]+\/fbevents/i,
    /_fbp\s*=/i,
  ],
  google_ads: [
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-/i,
    /googleadservices\.com\/pagead\/conversion/i,
    /google_conversion_id\s*=/i,
    /googleads\.g\.doubleclick\.net\/pagead/i,
    /google_remarketing_only/i,
    /conversion_async/i,
  ],
  google_analytics: [
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-/i,
    /gtag\s*\(\s*['"]config['"]\s*,\s*['"]UA-/i,
    /google-analytics\.com\/analytics/i,
    /googletagmanager\.com\/gtag/i,
  ],
  gtm: [
    /googletagmanager\.com\/gtm\.js/i,
    /GTM-[A-Z0-9]{6,}/i,
  ],
  tiktok: [
    /analytics\.tiktok\.com/i,
    /ttq\.load/i,
  ],
  linkedin: [
    /snap\.licdn\.com\/li\.lms-analytics/i,
    /_linkedin_partner_id/i,
  ],
  snapchat: [
    /sc-static\.net\/scevent/i,
    /snaptr\s*\(/i,
  ],
  pinterest: [
    /pintrk\s*\(/i,
    /ct\.pinterest\.com/i,
  ],
};

const ID_EXTRACT = {
  meta_pixel_id: /fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i,
  google_ads_id: /['"]AW-([\d]+)['"]/i,
  ga4_id: /['"]G-([A-Z0-9]+)['"]/i,
  gtm_id: /GTM-([A-Z0-9]{6,})/i,
};

const FB_SLUG_SKIP = new Set([
  'sharer', 'sharer.php', 'share', 'share.php', 'dialog', 'tr',
  'plugins', 'pages', 'groups', 'events', 'hashtag', 'login',
  'help', 'business', 'privacy', 'policy', 'profile.php',
  'watch', 'reel', 'reels', 'marketplace', 'gaming', 'stories',
  'ads', 'about', 'legal', 'terms', 'settings', 'notifications',
  'messenger', 'fundraisers', 'offers', 'jobs', 'bookmarks',
]);

// ── CSV Parser ──────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map(line => {
    const cols = parseCsvLine(line);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });
  return { headers, rows };
}

function parseCsvLine(line) {
  const cols = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; continue; } // escaped quote
      inQ = !inQ;
      continue;
    }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}

function escCsv(v) {
  const s = (v || '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

// ── HTTP Fetch with redirect limit ──────────────────────────────
function fetchHttp(url, redirects = 0) {
  if (redirects > MAX_REDIRECTS) return Promise.resolve({ ok: false, body: '', error: 'too many redirects' });

  return new Promise(resolve => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: HTTP_TIMEOUT,
      rejectUnauthorized: false,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let r = res.headers.location;
        if (r.startsWith('/')) { try { const u = new URL(url); r = u.protocol + '//' + u.host + r; } catch { return resolve({ ok: false, body: '' }); } }
        if (!r.startsWith('http')) r = 'https://' + r;
        res.resume();
        return resolve(fetchHttp(r, redirects + 1));
      }
      let d = '';
      let b = 0;
      res.on('data', c => { b += c.length; if (b <= 500000) d += c; });
      res.on('end', () => resolve({ ok: res.statusCode === 200, body: d, status: res.statusCode }));
      res.on('error', () => resolve({ ok: false, body: '' }));
    });
    req.on('error', () => resolve({ ok: false, body: '', error: 'connect' }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, body: '', error: 'timeout' }); });
  });
}

// ── Extract Facebook slug from website HTML ─────────────────────
function extractFbSlug(html) {
  const re = /https?:\/\/(?:www\.)?facebook\.com\/([a-zA-Z0-9._-]{2,})/gi;
  let match;
  while ((match = re.exec(html)) !== null) {
    const slug = match[1];
    if (!FB_SLUG_SKIP.has(slug.toLowerCase()) && slug.length > 2 && !/^\d+$/.test(slug)) {
      return slug;
    }
  }
  return null;
}

// ── URL normalizer ──────────────────────────────────────────────
function normalizeUrl(raw) {
  if (!raw) return null;
  let url = raw.trim().replace(/^\/\//, 'https://');
  if (url.startsWith('www.')) url = 'https://' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

// ── Find column in CSV headers ──────────────────────────────────
function findCol(headers, patterns) {
  for (const p of patterns) {
    const found = headers.find(h => p.test(h));
    if (found) return found;
  }
  return null;
}

// ── PASS 1: HTTP Website Scan ───────────────────────────────────
async function pass1(rows, urlCol) {
  console.log('\n━━━ PASS 1: Website Scan (HTTP) ━━━');
  const results = new Array(rows.length);
  let idx = 0, checked = 0, stats = { pixel: 0, gads: 0, fb: 0, analytics: 0, errors: 0 };
  const t0 = Date.now();

  const ticker = setInterval(() => {
    const s = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`  [${s}s] ${checked}/${rows.length} scanned | Pixel:${stats.pixel} Google:${stats.gads} FB:${stats.fb} | ${rate}/sec`);
  }, 10000);

  async function worker() {
    while (idx < rows.length) {
      const i = idx++;
      const url = normalizeUrl(rows[i][urlCol]);

      if (!url) {
        results[i] = { meta_pixel: false, google_ads: false, analytics: false, fb_slug: null, pixel_id: '', gads_id: '', ga4_id: '', gtm_id: '', error: 'no_url' };
        checked++;
        continue;
      }

      const resp = await fetchHttp(url);
      checked++;

      if (!resp.ok) {
        results[i] = { meta_pixel: false, google_ads: false, analytics: false, fb_slug: null, pixel_id: '', gads_id: '', ga4_id: '', gtm_id: '', error: resp.error || 'http_' + (resp.status || 0) };
        stats.errors++;
        continue;
      }

      const html = resp.body;
      const r = {
        meta_pixel: PATTERNS.meta_pixel.some(p => p.test(html)),
        google_ads: PATTERNS.google_ads.some(p => p.test(html)),
        analytics: PATTERNS.google_analytics.some(p => p.test(html)) || PATTERNS.gtm.some(p => p.test(html)),
        tiktok: PATTERNS.tiktok.some(p => p.test(html)),
        linkedin: PATTERNS.linkedin.some(p => p.test(html)),
        snapchat: PATTERNS.snapchat.some(p => p.test(html)),
        pinterest: PATTERNS.pinterest.some(p => p.test(html)),
        fb_slug: extractFbSlug(html),
        pixel_id: '', gads_id: '', ga4_id: '', gtm_id: '',
      };

      // Extract IDs
      for (const [key, pattern] of Object.entries(ID_EXTRACT)) {
        const m = html.match(pattern);
        if (m) r[key] = m[1];
      }

      if (r.meta_pixel) stats.pixel++;
      if (r.google_ads) stats.gads++;
      if (r.fb_slug) stats.fb++;
      if (r.analytics) stats.analytics++;

      results[i] = r;
    }
  }

  await Promise.all(Array.from({ length: HTTP_CONCURRENCY }, () => worker()));
  clearInterval(ticker);

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  ✓ ${checked} scanned in ${elapsed}s | Pixel:${stats.pixel} Google:${stats.gads} FB Pages:${stats.fb} Analytics:${stats.analytics} Errors:${stats.errors}\n`);
  return results;
}

// ── PASS 2: Ad Library Check ────────────────────────────────────
async function pass2(rows, nameCol, pass1Results, outputFile) {
  const needsCheck = [];
  for (let i = 0; i < rows.length; i++) {
    if (!pass1Results[i]?.meta_pixel && (rows[i][nameCol] || pass1Results[i]?.fb_slug)) {
      needsCheck.push(i);
    }
  }

  if (needsCheck.length === 0) {
    console.log('━━━ PASS 2: Skipped (all detected via pixel) ━━━\n');
    return {};
  }

  console.log(`━━━ PASS 2: Ad Library Check — ${needsCheck.length} leads ━━━`);

  const puppeteer = require('puppeteer-extra');
  const StealthPlugin = require('puppeteer-extra-plugin-stealth');
  puppeteer.use(StealthPlugin());

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  // Create tab pool with crash recovery
  async function createTab() {
    const p = await browser.newPage();
    await p.setViewport({ width: 1280, height: 800 });
    await p.setRequestInterception(true);
    p.on('request', req => {
      ['image', 'font', 'media'].includes(req.resourceType()) ? req.abort() : req.continue();
    });
    return p;
  }

  const tabs = [];
  for (let i = 0; i < PUPPETEER_CONCURRENCY; i++) tabs.push(await createTab());

  const adResults = {};
  let idx = 0, checked = 0, found = 0;
  let shuttingDown = false;
  const t0 = Date.now();

  // Graceful shutdown handler
  process.on('SIGINT', () => {
    console.log('\n  ⚠ Shutting down gracefully — saving progress...');
    shuttingDown = true;
  });

  const ticker = setInterval(() => {
    const s = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(2);
    const eta = checked > 0 ? Math.round((needsCheck.length - checked) / (checked / ((Date.now() - t0) / 1000))) : '?';
    console.log(`  [${s}s] ${checked}/${needsCheck.length} | Found: ${found} | ${rate}/sec | ETA: ${eta}s`);
  }, 15000);

  async function searchAdLibrary(tab, searchTerm) {
    const url = AD_LIBRARY_BASE + encodeURIComponent(searchTerm);
    try {
      await tab.goto(url, { waitUntil: 'networkidle2', timeout: PUPPETEER_NAV_TIMEOUT });
      await new Promise(r => setTimeout(r, PUPPETEER_WAIT));
      return await tab.evaluate(() => {
        const text = document.body.innerText || '';
        if (text.includes('No ads match')) return 0;
        const m = text.match(/([\d,]+)\s*results?/i);
        return m ? parseInt(m[1].replace(/,/g, ''), 10) : 0;
      });
    } catch {
      return -1; // error
    }
  }

  async function worker(tabIdx) {
    while (idx < needsCheck.length && !shuttingDown) {
      const qi = idx++;
      const rowIdx = needsCheck[qi];
      const p1 = pass1Results[rowIdx] || {};
      const name = rows[rowIdx][nameCol] || '';
      const fbSlug = p1.fb_slug;

      // Try FB slug first (more accurate), then name as fallback
      let count = 0;
      let searchedBy = 'none';
      let searchTerm = '';

      if (fbSlug) {
        count = await searchAdLibrary(tabs[tabIdx], fbSlug);
        searchedBy = 'fb_slug';
        searchTerm = fbSlug;

        // If FB slug returned 0 or error, try business name as fallback
        if (count <= 0 && name && name !== fbSlug) {
          const nameCount = await searchAdLibrary(tabs[tabIdx], name);
          if (nameCount > 0) {
            count = nameCount;
            searchedBy = 'name_fallback';
            searchTerm = name;
          }
        }
      } else if (name) {
        count = await searchAdLibrary(tabs[tabIdx], name);
        searchedBy = 'name';
        searchTerm = name;
      }

      // Handle tab crash — recreate tab
      if (count === -1) {
        try {
          await tabs[tabIdx].close().catch(() => {});
          tabs[tabIdx] = await createTab();
        } catch {
          // Browser might have crashed entirely
        }
      }

      adResults[rowIdx] = { count: Math.max(0, count), searchedBy, searchTerm };
      if (count > 0) found++;
      checked++;

      // Auto-save progress
      if (checked % 50 === 0 && outputFile) {
        try {
          const progressFile = outputFile.replace('.csv', '.progress.json');
          fs.writeFileSync(progressFile, JSON.stringify({ checked, found, total: needsCheck.length, adResults }, null, 0));
        } catch {}
      }
    }
  }

  await Promise.all(tabs.map((_, i) => worker(i)));
  clearInterval(ticker);

  // Close browser safely
  try { await browser.close(); } catch {}

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  ✓ ${checked} checked in ${elapsed}s | Found: ${found} running ads\n`);
  return adResults;
}

// ── MAIN ────────────────────────────────────────────────────────
(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile.replace('.csv', '-ad-audit.csv');

  if (!inputFile) {
    console.log('Ad Checker Final v2 — Meta + Google ad detection');
    console.log('Usage: node scripts/ad-checker-final.js input.csv [output.csv]');
    console.log('\nInput CSV needs: website/url column + business name column');
    console.log('Output adds: runs_meta_ads, runs_google_ads, has_analytics, etc.');
    process.exit(1);
  }

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║        Ad Checker Final v2               ║');
  console.log('╚══════════════════════════════════════════╝');

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  const urlCol = findCol(headers, [/^website$/i, /^url$/i, /^domain$/i, /charity_contact_web/i, /website_url/i]) ||
                 findCol(headers, [/web/i, /site/i, /url/i, /domain/i]);
  const nameCol = findCol(headers, [/^business.?name$/i, /^firm.?name$/i, /^company$/i, /^name$/i, /^charity_name$/i, /^org/i]) ||
                  headers[0];

  console.log(`\n  Input:  ${rows.length} leads from ${path.basename(inputFile)}`);
  console.log(`  URL:    "${urlCol || 'NONE'}"`);
  console.log(`  Name:   "${nameCol}"`);
  console.log(`  Output: ${path.basename(outputFile)}`);

  const t0 = Date.now();

  // Pass 1
  const p1 = urlCol ? await pass1(rows, urlCol) : new Array(rows.length).fill({});

  // Pass 2
  const p2 = await pass2(rows, nameCol, p1, outputFile);

  // ── Merge Results ─────────────────────────────────────────────
  const results = rows.map((row, i) => {
    const s1 = p1[i] || {};
    const s2 = p2[i];

    let metaStatus = 'NO';
    let method = 'none';
    let adCount = '';

    if (s1.meta_pixel) {
      metaStatus = 'YES';
      method = 'pixel';
    } else if (s2 && s2.count > 0) {
      metaStatus = 'YES';
      method = 'ad_library_' + (s2.searchedBy || '');
      adCount = s2.count;
    }

    return {
      ...row,
      runs_meta_ads: metaStatus,
      detection_method: method,
      meta_ad_count: adCount,
      meta_pixel_id: s1.pixel_id || '',
      facebook_page: s1.fb_slug ? `facebook.com/${s1.fb_slug}` : '',
      runs_google_ads: s1.google_ads ? 'YES' : 'NO',
      google_ads_id: s1.gads_id || '',
      has_analytics: s1.analytics ? 'YES' : 'NO',
      ga4_id: s1.ga4_id || '',
      has_tiktok: s1.tiktok ? 'YES' : '',
      has_linkedin: s1.linkedin ? 'YES' : '',
    };
  });

  // ── Write CSV ─────────────────────────────────────────────────
  const addCols = [
    'runs_meta_ads', 'detection_method', 'meta_ad_count', 'meta_pixel_id',
    'facebook_page', 'runs_google_ads', 'google_ads_id', 'has_analytics',
    'ga4_id', 'has_tiktok', 'has_linkedin',
  ];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.map(r => outHeaders.map(h => escCsv(r[h])).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  // ── Summary ───────────────────────────────────────────────────
  const totalSec = ((Date.now() - t0) / 1000).toFixed(1);
  const metaYes = results.filter(r => r.runs_meta_ads === 'YES').length;
  const byPixel = results.filter(r => r.detection_method === 'pixel').length;
  const byAdLib = results.filter(r => r.detection_method.startsWith('ad_library')).length;
  const googleYes = results.filter(r => r.runs_google_ads === 'YES').length;
  const hasFb = results.filter(r => r.facebook_page).length;
  const analyticsOnly = results.filter(r => r.has_analytics === 'YES' && r.runs_meta_ads === 'NO' && r.runs_google_ads === 'NO').length;
  const tiktok = results.filter(r => r.has_tiktok === 'YES').length;
  const linkedin = results.filter(r => r.has_linkedin === 'YES').length;

  console.log('╔══════════════════════════════════════════╗');
  console.log('║              RESULTS                     ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Total leads:      ${String(rows.length).padStart(6)}               ║`);
  console.log(`║  Running Meta ads: ${String(metaYes).padStart(6)} (${byPixel} pixel + ${byAdLib} Ad Lib) ║`);
  console.log(`║  Running Google:   ${String(googleYes).padStart(6)}               ║`);
  console.log(`║  Has Facebook:     ${String(hasFb).padStart(6)}               ║`);
  console.log(`║  Analytics only:   ${String(analyticsOnly).padStart(6)} ← opportunity   ║`);
  if (tiktok) console.log(`║  TikTok ads:       ${String(tiktok).padStart(6)}               ║`);
  if (linkedin) console.log(`║  LinkedIn ads:     ${String(linkedin).padStart(6)}               ║`);
  console.log(`║  Time:             ${totalSec.padStart(5)}s              ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log(`  → ${outputFile}`);

  // Cleanup progress file
  try { fs.unlinkSync(outputFile.replace('.csv', '.progress.json')); } catch {}
})();
