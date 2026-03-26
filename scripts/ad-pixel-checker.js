#!/usr/bin/env node
/**
 * Ad Pixel Checker — Production-ready ad detection via website scanning
 *
 * Zero dependencies on Facebook API, Puppeteer, or any paid service.
 * Pure HTTP requests checking business websites for tracking pixels/tags.
 *
 * Speed: 50 concurrent, ~12/sec = 100K leads in ~2.5 hours
 *
 * Detects:
 *   META:     Pixel (fbq), Conversions API signals, Facebook SDK
 *   GOOGLE:   Ads conversion tag (AW-), remarketing, enhanced conversions
 *   ANALYTICS: GA4, Universal Analytics, GTM
 *   OTHER:    TikTok, LinkedIn, Snapchat, Pinterest, Twitter/X, Microsoft/Bing
 *
 * Also extracts:
 *   - Facebook page URL (from social links on website)
 *   - Pixel/tag IDs
 *   - Ad platform count (how many platforms they're on)
 *
 * Output categories for cold email:
 *   🔥 HOT:  Running paid ads (Meta Pixel or Google Ads tag found)
 *   🟡 WARM: Has analytics/tracking but no paid ads (opportunity pitch)
 *   ⬜ COLD: No tracking at all (needs everything)
 *
 * Usage:
 *   node scripts/ad-pixel-checker.js input.csv [output.csv] [concurrency]
 *
 * Input CSV needs a column named: website, url, domain, or similar
 */

const fs = require('fs');
const https = require('https');
const http = require('http');
const path = require('path');

// ── Config ──────────────────────────────────────────────────────
const CONCURRENCY = parseInt(process.argv[4] || '50', 10);
const TIMEOUT = 8000;
const MAX_REDIRECTS = 5;
const MAX_BODY = 500000; // Read first 500KB only

// ── Detection Patterns ──────────────────────────────────────────
const DETECTORS = {
  // ─── Meta / Facebook ───
  meta_pixel: {
    label: 'Meta Pixel',
    patterns: [
      /fbq\s*\(\s*['"]init/i,           // Standard pixel init
      /fbq\s*\(\s*['"]track/i,          // Pixel event tracking
      /connect\.facebook\.net\/[a-z_]+\/fbevents\.js/i,  // Pixel JS file
    ],
    idPattern: /fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i,
    idField: 'meta_pixel_id',
    category: 'paid',
  },
  meta_domain_verify: {
    label: 'Meta Domain Verification',
    patterns: [
      /facebook-domain-verification/i,   // <meta name="facebook-domain-verification" content="...">
    ],
    idPattern: /facebook-domain-verification['"]\s+content=['"]([a-z0-9]+)['"]/i,
    idField: 'meta_domain_verify_id',
    category: 'meta_signal', // Not paid by itself, but strong business ownership signal
  },
  meta_fbc: {
    label: 'Meta Click ID (_fbc/fbclid)',
    patterns: [
      /_fbc\b/,                         // _fbc cookie reference in code
      /fbclid/i,                        // fbclid parameter handling
      /facebook\.com\/tr\?.*&ev=/i,     // Server-side event tracking
    ],
    category: 'meta_signal', // Handling click IDs = they run FB ads that drive traffic
  },
  meta_capi: {
    label: 'Meta Conversions API',
    patterns: [
      /graph\.facebook\.com\/v\d+.*\/events/i,  // Conversions API endpoint
      /facebook.*conversions?\s*api/i,           // References to Conversions API
      /fb_event_name/i,                          // Server event parameter
    ],
    category: 'paid',
  },
  meta_sdk: {
    label: 'Facebook SDK',
    patterns: [
      /connect\.facebook\.net\/[a-z_]+\/sdk\.js/i,  // FB SDK
      /facebook\.com\/tr\?id=\d+/i,                  // Tracking pixel img
    ],
    category: 'tracking',
  },

  // ─── Google Ads ───
  google_ads: {
    label: 'Google Ads',
    patterns: [
      /gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-\d+/i,    // Gtag config AW-
      /googleadservices\.com\/pagead\/conversion/i,         // Conversion tracking
      /google_conversion_id\s*=\s*\d+/i,                   // Legacy conversion
      /googleads\.g\.doubleclick\.net\/pagead\/viewthroughconversion/i, // View-through
      /goog_report_conversion/i,                            // Report conversion
    ],
    idPattern: /['"]AW-(\d+)['"]/i,
    idField: 'google_ads_id',
    category: 'paid',
  },
  google_remarketing: {
    label: 'Google Remarketing',
    patterns: [
      /googleadservices\.com\/pagead\/conversion_async/i,
      /google_remarketing_only\s*=\s*true/i,
      /doubleclick\.net\/activity/i,
    ],
    category: 'paid',
  },

  // ─── Google Analytics ───
  ga4: {
    label: 'Google Analytics 4',
    patterns: [
      /gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-[A-Z0-9]+/i,
      /googletagmanager\.com\/gtag\/js\?id=G-/i,
    ],
    idPattern: /['"]G-([A-Z0-9]+)['"]/i,
    idField: 'ga4_id',
    category: 'analytics',
  },
  ua: {
    label: 'Universal Analytics',
    patterns: [
      /gtag\s*\(\s*['"]config['"]\s*,\s*['"]UA-\d+-\d+/i,
      /google-analytics\.com\/analytics\.js/i,
      /google-analytics\.com\/ga\.js/i,
    ],
    category: 'analytics',
  },
  gtm: {
    label: 'Google Tag Manager',
    patterns: [
      /googletagmanager\.com\/gtm\.js\?id=GTM-/i,
      /GTM-[A-Z0-9]{6,}/,
    ],
    idPattern: /GTM-([A-Z0-9]{6,})/,
    idField: 'gtm_id',
    category: 'analytics',
  },

  // ─── Other Ad Platforms ───
  tiktok: {
    label: 'TikTok Pixel',
    patterns: [
      /analytics\.tiktok\.com\/i18n\/pixel/i,
      /ttq\.load\s*\(/i,
    ],
    category: 'paid',
  },
  linkedin: {
    label: 'LinkedIn Insight',
    patterns: [
      /snap\.licdn\.com\/li\.lms-analytics/i,
      /_linkedin_partner_id\s*=/i,
      /linkedin\.com\/px/i,
    ],
    category: 'tracking', // Insight tag = tracking, NOT confirmed paid ads
  },
  snapchat: {
    label: 'Snapchat Pixel',
    patterns: [
      /sc-static\.net\/scevent/i,
      /snaptr\s*\(\s*['"]init/i,
    ],
    category: 'paid',
  },
  pinterest: {
    label: 'Pinterest Tag',
    patterns: [
      /pintrk\s*\(\s*['"]load/i,
      /ct\.pinterest\.com\/v3/i,
    ],
    category: 'paid',
  },
  twitter: {
    label: 'Twitter/X Pixel',
    patterns: [
      /static\.ads-twitter\.com\/uwt/i,
      /twq\s*\(\s*['"]init/i,
    ],
    category: 'paid',
  },
  bing_ads: {
    label: 'Bing Ads (UET)',
    patterns: [
      /bat\.bing\.com\/action/i,  // Actual Bing UET tag
      /uetq\s*=\s*uetq/i,        // UET queue
    ],
    category: 'paid',
  },
  microsoft_clarity: {
    label: 'Microsoft Clarity',
    patterns: [
      /clarity\.ms\/tag/i,
    ],
    category: 'tracking', // Clarity is FREE analytics, NOT paid ads
  },
  // Added from scrutiny findings — commonly used by SMBs
  linkedin_ads: {
    label: 'LinkedIn Ads',
    patterns: [
      /snap\.licdn\.com\/li\.lms-analytics\/insight/i,
      /_linkedin_data_partner_id/i,
      /dc\.ads\.linkedin\.com/i,
    ],
    category: 'paid',
  },
  callrail: {
    label: 'CallRail',
    patterns: [
      /calltrk\.com/i,
      /CallTrk/,
    ],
    category: 'paid',
  },
  hubspot_tracking: {
    label: 'HubSpot',
    patterns: [
      /\.hs-scripts\.com\//i,
      /_hsq/,
    ],
    category: 'tracking',
  },
};

// Facebook page URL skip list
const FB_SKIP = new Set([
  'sharer', 'sharer.php', 'share', 'share.php', 'dialog', 'tr',
  'plugins', 'pages', 'groups', 'events', 'hashtag', 'login',
  'help', 'business', 'privacy', 'policy', 'profile.php',
  'watch', 'reel', 'reels', 'marketplace', 'gaming', 'stories',
  'ads', 'about', 'legal', 'terms', 'settings', 'notifications',
  'messenger', 'fundraisers', 'offers', 'jobs', 'bookmarks',
  'flx', 'l.php', 'photo.php', 'video.php',
]);

// ── HTTP Fetch ──────────────────────────────────────────────────
function fetchUrl(url, redirects = 0) {
  if (redirects > MAX_REDIRECTS) return Promise.resolve({ ok: false, error: 'redirects' });
  return new Promise(resolve => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: TIMEOUT,
      rejectUnauthorized: false,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let r = res.headers.location;
        if (r.startsWith('/')) { try { const u = new URL(url); r = u.protocol + '//' + u.host + r; } catch { return resolve({ ok: false }); } }
        if (!r.startsWith('http')) r = 'https://' + r;
        res.resume();
        return resolve(fetchUrl(r, redirects + 1));
      }
      let d = '';
      let b = 0;
      res.on('data', c => { b += c.length; if (b <= MAX_BODY) d += c; });
      res.on('end', () => resolve({ ok: res.statusCode === 200, body: d, cookies: (res.headers['set-cookie'] || []).join('; ') }));
      res.on('error', () => resolve({ ok: false }));
    });
    req.on('error', () => resolve({ ok: false, error: 'connect' }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, error: 'timeout' }); });
  });
}

// ── Analyze HTML ────────────────────────────────────────────────
function analyzeWebsite(html, cookies = '') {
  const result = {
    detections: {},
    ids: {},
    facebook_page: '',
    paid_platforms: [],
    analytics_platforms: [],
    lead_category: 'COLD',
  };

  // Run all detectors
  for (const [key, detector] of Object.entries(DETECTORS)) {
    const found = detector.patterns.some(p => p.test(html));
    result.detections[key] = found;

    if (found && detector.idPattern && detector.idField) {
      const m = html.match(detector.idPattern);
      if (m) result.ids[detector.idField] = m[1];
    }

    if (found) {
      if (detector.category === 'paid') result.paid_platforms.push(detector.label);
      else if (detector.category === 'analytics') result.analytics_platforms.push(detector.label);
    }
  }

  // Extract Facebook page URL
  const fbRe = /https?:\/\/(?:www\.)?facebook\.com\/([a-zA-Z0-9._-]{2,})/gi;
  let m;
  while ((m = fbRe.exec(html)) !== null) {
    const slug = m[1];
    if (!FB_SKIP.has(slug.toLowerCase()) && !/^\d+$/.test(slug) && slug.length > 2) {
      result.facebook_page = 'facebook.com/' + slug;
      break;
    }
  }

  // Cookie-based detection (from Set-Cookie response headers)
  if (cookies) {
    if (/_fbp=/.test(cookies) && !result.detections.meta_pixel) {
      result.detections.meta_pixel = true;
      result.paid_platforms.push('Meta Pixel (cookie)');
    }
    if (/_gcl_aw=/.test(cookies) && !result.detections.google_ads) {
      result.detections.google_ads = true;
      result.paid_platforms.push('Google Ads (cookie)');
    }
  }

  // ── Composite Meta Confidence Score ─────────────────────────
  // Combines multiple signals for high-accuracy Meta ad detection
  let metaScore = 0;
  const metaSignals = [];
  if (result.detections.meta_pixel)        { metaScore += 50; metaSignals.push('pixel'); }
  if (result.detections.meta_capi)         { metaScore += 40; metaSignals.push('capi'); }
  if (result.detections.meta_domain_verify){ metaScore += 20; metaSignals.push('domain_verify'); }
  if (result.detections.meta_fbc)          { metaScore += 25; metaSignals.push('fbc_fbclid'); }
  if (result.detections.meta_sdk)          { metaScore += 10; metaSignals.push('sdk'); }
  if (result.facebook_page)                { metaScore += 5;  metaSignals.push('fb_page_link'); }

  // Check for Facebook UTM params in links (utm_source=facebook|fb|meta, utm_medium=paid|cpc|cpm)
  const hasFbUtm = /utm_source=(facebook|fb|meta|ig|instagram)/i.test(html) &&
                   /utm_medium=(paid|cpc|cpm|social|paidsocial)/i.test(html);
  if (hasFbUtm) { metaScore += 30; metaSignals.push('fb_utm'); }

  result.meta_confidence = Math.min(metaScore, 100);
  result.meta_signals = metaSignals;

  // ── Composite Google Confidence Score ─────────────────────
  let googleScore = 0;
  const googleSignals = [];
  if (result.detections.google_ads)        { googleScore += 60; googleSignals.push('ads_tag'); }
  if (result.detections.google_remarketing){ googleScore += 40; googleSignals.push('remarketing'); }
  // Google UTM params
  const hasGoogleUtm = /utm_source=(google|adwords|gclid)/i.test(html) &&
                       /utm_medium=(cpc|ppc|paid)/i.test(html);
  if (hasGoogleUtm) { googleScore += 25; googleSignals.push('google_utm'); }
  const hasGclid = /gclid/i.test(html);
  if (hasGclid && !result.detections.google_ads) { googleScore += 20; googleSignals.push('gclid'); }

  result.google_confidence = Math.min(googleScore, 100);
  result.google_signals = googleSignals;

  // Categorize lead
  const hasPaid = result.paid_platforms.length > 0;
  const hasMetaSignals = metaScore >= 40; // Strong enough meta signals even without pixel
  const hasAnalytics = result.analytics_platforms.length > 0;

  if (hasPaid || hasMetaSignals) {
    result.lead_category = 'HOT';  // Running paid ads (confirmed or high confidence)
  } else if (hasAnalytics || metaScore > 0 || googleScore > 0) {
    result.lead_category = 'WARM'; // Has tracking or weak signals
  } else {
    result.lead_category = 'COLD'; // Nothing
  }

  return result;
}

// ── CSV Helpers ─────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map(l => {
    const cols = parseLine(l);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });
  return { headers, rows };
}

function parseLine(line) {
  const cols = [];
  let cur = '';
  let inQ = false;
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

function normalizeUrl(raw) {
  if (!raw) return null;
  let url = raw.trim().replace(/^\/\//, 'https://');
  if (url.startsWith('www.')) url = 'https://' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

function findCol(headers, patterns) {
  for (const p of patterns) { const f = headers.find(h => p.test(h)); if (f) return f; }
  return null;
}

// ── Main ────────────────────────────────────────────────────────
(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile?.replace('.csv', '-ad-scan.csv');

  if (!inputFile) {
    console.log('Ad Pixel Checker — Lightning fast ad detection');
    console.log('Usage: node scripts/ad-pixel-checker.js input.csv [output.csv] [concurrency]');
    console.log('\nDetects: Meta Pixel, Google Ads, GA4, GTM, TikTok, LinkedIn, Snapchat, Pinterest, Twitter, Bing');
    console.log('Speed: ~12 websites/sec with 50 concurrent connections');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const { headers, rows } = parseCSV(csvText);

  const urlCol = findCol(headers, [/^website$/i, /^url$/i, /^domain$/i, /charity_contact_web/i]) ||
                 findCol(headers, [/web/i, /site/i, /url/i, /domain/i]);
  const nameCol = findCol(headers, [/^business.?name$/i, /^firm.?name$/i, /^company$/i, /^name$/i, /charity_name/i]) || headers[0];

  const withUrl = rows.filter(r => r[urlCol]?.trim()).length;

  console.log('\n╔════════════════════════════════════════╗');
  console.log('║         Ad Pixel Checker               ║');
  console.log('╚════════════════════════════════════════╝');
  console.log(`  Input:       ${rows.length} leads (${withUrl} with website)`);
  console.log(`  URL column:  "${urlCol || 'NONE'}"`);
  console.log(`  Name column: "${nameCol}"`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Output:      ${path.basename(outputFile)}\n`);

  if (!urlCol) { console.error('No website/URL column found.'); process.exit(1); }

  const results = new Array(rows.length);
  let idx = 0, checked = 0;
  let stats = { hot: 0, warm: 0, cold: 0, errors: 0 };
  const t0 = Date.now();

  const ticker = setInterval(() => {
    const s = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    const eta = checked > 0 ? Math.round((withUrl - checked) / parseFloat(rate)) : '?';
    console.log(`  [${s}s] ${checked}/${withUrl} | 🔥${stats.hot} 🟡${stats.warm} ⬜${stats.cold} ❌${stats.errors} | ${rate}/sec | ETA ${eta}s`);
  }, 5000);

  // Auto-save every 200 checks
  let lastSave = 0;
  function autoSave() {
    if (checked - lastSave < 200) return;
    lastSave = checked;
    try {
      const partial = results.filter(Boolean);
      const addCols = ['lead_category', 'runs_meta_ads', 'meta_confidence', 'meta_signals', 'runs_google_ads', 'google_confidence', 'google_signals', 'paid_platforms', 'has_analytics', 'facebook_page', 'meta_pixel_id', 'meta_domain_verify_id', 'google_ads_id', 'ga4_id'];
      const outH = [...headers, ...addCols];
      const outR = partial.map(r => outH.map(h => esc(r[h])).join(','));
      fs.writeFileSync(outputFile.replace('.csv', '.partial.csv'), outH.join(',') + '\n' + outR.join('\n'));
    } catch {}
  }

  async function worker() {
    while (idx < rows.length) {
      const i = idx++;
      const url = normalizeUrl(rows[i][urlCol]);

      if (!url) {
        results[i] = { ...rows[i], lead_category: '', runs_meta_ads: '', meta_confidence: '', meta_signals: '', runs_google_ads: '', google_confidence: '', google_signals: '', paid_platforms: '', has_analytics: '', facebook_page: '', meta_pixel_id: '', meta_domain_verify_id: '', google_ads_id: '', ga4_id: '', gtm_id: '', scan_error: 'no_url' };
        continue;
      }

      const resp = await fetchUrl(url);
      checked++;

      if (!resp.ok) {
        results[i] = { ...rows[i], lead_category: 'ERROR', runs_meta_ads: '', meta_confidence: '', meta_signals: '', runs_google_ads: '', google_confidence: '', google_signals: '', paid_platforms: '', has_analytics: '', facebook_page: '', meta_pixel_id: '', meta_domain_verify_id: '', google_ads_id: '', ga4_id: '', gtm_id: '', scan_error: resp.error || 'failed' };
        stats.errors++;
        autoSave();
        continue;
      }

      const analysis = analyzeWebsite(resp.body, resp.cookies || '');

      if (analysis.lead_category === 'HOT') stats.hot++;
      else if (analysis.lead_category === 'WARM') stats.warm++;
      else stats.cold++;

      results[i] = {
        ...rows[i],
        lead_category: analysis.lead_category,
        runs_meta_ads: analysis.detections.meta_pixel || analysis.meta_confidence >= 40 ? 'YES' : 'NO',
        meta_confidence: analysis.meta_confidence,
        meta_signals: analysis.meta_signals.join('+') || '',
        runs_google_ads: analysis.detections.google_ads || analysis.detections.google_remarketing ? 'YES' : 'NO',
        google_confidence: analysis.google_confidence,
        google_signals: analysis.google_signals.join('+') || '',
        paid_platforms: analysis.paid_platforms.join('; ') || '',
        has_analytics: analysis.analytics_platforms.length > 0 ? 'YES' : 'NO',
        facebook_page: analysis.facebook_page,
        meta_pixel_id: analysis.ids.meta_pixel_id || '',
        meta_domain_verify_id: analysis.ids.meta_domain_verify_id || '',
        google_ads_id: analysis.ids.google_ads_id || '',
        ga4_id: analysis.ids.ga4_id || '',
        gtm_id: analysis.ids.gtm_id || '',
        scan_error: '',
      };
      autoSave();
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  clearInterval(ticker);

  // ── Write Final CSV ───────────────────────────────────────────
  const addCols = ['lead_category', 'runs_meta_ads', 'meta_confidence', 'meta_signals', 'runs_google_ads', 'google_confidence', 'google_signals', 'paid_platforms', 'has_analytics', 'facebook_page', 'meta_pixel_id', 'meta_domain_verify_id', 'google_ads_id', 'ga4_id', 'gtm_id', 'scan_error'];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.filter(Boolean).map(r => outHeaders.map(h => esc(r[h])).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  // Cleanup partial file
  try { fs.unlinkSync(outputFile.replace('.csv', '.partial.csv')); } catch {}

  // ── Summary ───────────────────────────────────────────────────
  const totalSec = ((Date.now() - t0) / 1000).toFixed(1);
  const rate = (checked / parseFloat(totalSec)).toFixed(1);

  console.log('\n╔════════════════════════════════════════╗');
  console.log('║              RESULTS                   ║');
  console.log('╠════════════════════════════════════════╣');
  console.log(`║  Scanned:    ${String(checked).padStart(6)} websites          ║`);
  console.log(`║  🔥 HOT:     ${String(stats.hot).padStart(6)} (running paid ads) ║`);
  console.log(`║  🟡 WARM:    ${String(stats.warm).padStart(6)} (analytics only)  ║`);
  console.log(`║  ⬜ COLD:    ${String(stats.cold).padStart(6)} (no tracking)     ║`);
  console.log(`║  ❌ Errors:  ${String(stats.errors).padStart(6)}                  ║`);
  console.log(`║  Speed:      ${rate.padStart(6)}/sec              ║`);
  console.log(`║  Time:       ${totalSec.padStart(5)}s               ║`);
  console.log('╚════════════════════════════════════════╝');
  console.log(`\n  → ${outputFile}`);
  console.log(`\n  HOT leads: email them "We see you're running Facebook/Google ads — let us audit your campaigns for free"`);
  console.log(`  WARM leads: email them "You have analytics but no paid ads — you're leaving money on the table"`);
  console.log(`  COLD leads: email them "You have zero online marketing — let us set it all up"\n`);
})();
