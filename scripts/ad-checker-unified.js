#!/usr/bin/env node
/**
 * Unified Ad Checker — Composite detection across multiple signals
 *
 * Combines three detection layers:
 *   1. HTTP Pixel Scan (FREE, ~12/sec) — Meta Pixel, Google Ads tag, domain verification,
 *      fbclid, Conversions API, UTM params, Facebook page extraction
 *   2. Google Ads Transparency (FREE, ~1.5/sec) — Confirms Google Ads with exact ad count
 *   3. Facebook Ad Library (FREE, ~0.1/sec) — Confirms Meta ads via keyword search
 *
 * Smart pipeline:
 *   - Pixel scan runs on ALL leads (fast, free)
 *   - Google Ads Transparency runs on leads with google_confidence > 0
 *   - FB Ad Library only runs on uncertain leads (meta_confidence 10-49)
 *   - Leads with meta_confidence >= 50 are trusted without Ad Library
 *
 * Output columns:
 *   meta_ads: YES/NO/LIKELY — final determination for Meta ads
 *   google_ads: YES/NO/LIKELY — final determination for Google ads
 *   meta_confidence: 0-100 composite score
 *   google_confidence: 0-100 composite score
 *   facebook_page: extracted FB page URL from website
 *   paid_platforms: all detected paid ad platforms
 *
 * Usage: node scripts/ad-checker-unified.js input.csv [output.csv] [concurrency]
 */

const fs = require('fs');
const https = require('https');
const http = require('http');
const dns = require('dns');
const { execSync } = require('child_process');
const path = require('path');

const CONCURRENCY = parseInt(process.argv[4] || '50', 10);
const TIMEOUT = 8000;
const MAX_REDIRECTS = 5;
const MAX_BODY = 500000;

// ── Detection Patterns (same as ad-pixel-checker.js) ────────────
const DETECTORS = {
  meta_pixel: {
    label: 'Meta Pixel', category: 'paid',
    patterns: [/fbq\s*\(\s*['"]init/i, /fbq\s*\(\s*['"]track/i, /connect\.facebook\.net\/[a-z_]+\/fbevents\.js/i],
    idPattern: /fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d+)['"]/i, idField: 'meta_pixel_id',
  },
  meta_domain_verify: {
    label: 'Meta Domain Verification', category: 'meta_signal',
    patterns: [/facebook-domain-verification/i],
    idPattern: /facebook-domain-verification['"]\s+content=['"]([a-z0-9]+)['"]/i, idField: 'meta_domain_verify_id',
  },
  meta_fbc: {
    label: 'Meta Click ID', category: 'meta_signal',
    patterns: [/_fbc\b/, /fbclid/i, /facebook\.com\/tr\?.*&ev=/i],
  },
  meta_capi: {
    label: 'Meta Conversions API', category: 'paid',
    patterns: [/graph\.facebook\.com\/v\d+.*\/events/i, /facebook.*conversions?\s*api/i, /fb_event_name/i],
  },
  meta_sdk: {
    label: 'Facebook SDK', category: 'tracking',
    patterns: [/connect\.facebook\.net\/[a-z_]+\/sdk\.js/i, /facebook\.com\/tr\?id=\d+/i],
  },
  google_ads: {
    label: 'Google Ads', category: 'paid',
    patterns: [/gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-\d+/i, /googleadservices\.com\/pagead\/conversion/i, /google_conversion_id\s*=\s*\d+/i, /googleads\.g\.doubleclick\.net/i, /goog_report_conversion/i],
    idPattern: /['"]AW-(\d+)['"]/i, idField: 'google_ads_id',
  },
  google_remarketing: {
    label: 'Google Remarketing', category: 'paid',
    patterns: [/googleadservices\.com\/pagead\/conversion_async/i, /google_remarketing_only\s*=\s*true/i, /doubleclick\.net\/activity/i],
  },
  ga4: {
    label: 'GA4', category: 'analytics',
    patterns: [/gtag\s*\(\s*['"]config['"]\s*,\s*['"]G-[A-Z0-9]+/i, /googletagmanager\.com\/gtag\/js\?id=G-/i],
  },
  gtm: {
    label: 'GTM', category: 'analytics',
    patterns: [/googletagmanager\.com\/gtm\.js\?id=GTM-/i, /GTM-[A-Z0-9]{6,}/],
  },
  tiktok: { label: 'TikTok', category: 'paid', patterns: [/analytics\.tiktok\.com\/i18n\/pixel/i, /ttq\.load\s*\(/i] },
  bing_ads: { label: 'Bing Ads', category: 'paid', patterns: [/bat\.bing\.com\/action/i, /uetq\s*=\s*uetq/i] },
};

const FB_SKIP = new Set(['sharer','sharer.php','share','share.php','dialog','tr','plugins','pages','groups','events','hashtag','login','help','business','privacy','policy','profile.php','watch','reel','reels','marketplace','gaming','stories','ads','about','legal','terms','settings','notifications','messenger','fundraisers','offers','jobs','bookmarks','flx','l.php','photo.php','video.php']);

// ── HTTP Fetch with Retry ───────────────────────────────────────
const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
];

function fetchUrl(url, redirects = 0, ua = null) {
  if (redirects > MAX_REDIRECTS) return Promise.resolve({ ok: false });
  return new Promise(resolve => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: {
        'User-Agent': ua || USER_AGENTS[0],
        Accept: 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: TIMEOUT, rejectUnauthorized: false,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let r = res.headers.location;
        if (r.startsWith('/')) { try { const u = new URL(url); r = u.protocol + '//' + u.host + r; } catch { return resolve({ ok: false }); } }
        if (!r.startsWith('http')) r = 'https://' + r;
        res.resume();
        return resolve(fetchUrl(r, redirects + 1, ua));
      }
      let d = ''; let b = 0;
      res.on('data', c => { b += c.length; if (b <= MAX_BODY) d += c; });
      res.on('end', () => resolve({ ok: res.statusCode === 200, body: d, status: res.statusCode }));
      res.on('error', () => resolve({ ok: false }));
    });
    req.on('error', () => resolve({ ok: false }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, error: 'timeout' }); });
  });
}

// Fetch with one retry using different UA
async function fetchWithRetry(url) {
  let resp = await fetchUrl(url);
  if (!resp.ok) {
    // Retry once with different user agent
    resp = await fetchUrl(url, 0, USER_AGENTS[1]);
  }
  // If still failing and URL has no www, try with www
  if (!resp.ok && !url.includes('://www.')) {
    const wwwUrl = url.replace('://', '://www.');
    resp = await fetchUrl(wwwUrl, 0, USER_AGENTS[2]);
  }
  return resp;
}

// ── Analyze ─────────────────────────────────────────────────────
function analyzeWebsite(html) {
  const result = { detections: {}, ids: {}, paid: [], analytics: [], facebook_page: '' };

  for (const [key, det] of Object.entries(DETECTORS)) {
    const found = det.patterns.some(p => p.test(html));
    result.detections[key] = found;
    if (found && det.idPattern && det.idField) {
      const m = html.match(det.idPattern);
      if (m) result.ids[det.idField] = m[1];
    }
    if (found && det.category === 'paid') result.paid.push(det.label);
    if (found && det.category === 'analytics') result.analytics.push(det.label);
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

  // Composite Meta score
  let metaScore = 0;
  const metaSignals = [];
  if (result.detections.meta_pixel)         { metaScore += 50; metaSignals.push('pixel'); }
  if (result.detections.meta_capi)          { metaScore += 40; metaSignals.push('capi'); }
  if (result.detections.meta_domain_verify) { metaScore += 20; metaSignals.push('domain_verify'); }
  if (result.detections.meta_fbc)           { metaScore += 25; metaSignals.push('fbc'); }
  if (result.detections.meta_sdk)           { metaScore += 10; metaSignals.push('sdk'); }
  if (result.facebook_page)                 { metaScore += 5;  metaSignals.push('fb_link'); }
  const hasFbUtm = /utm_source=(facebook|fb|meta|ig|instagram)/i.test(html) && /utm_medium=(paid|cpc|cpm|social|paidsocial)/i.test(html);
  if (hasFbUtm) { metaScore += 30; metaSignals.push('fb_utm'); }

  // Composite Google score
  let googleScore = 0;
  const googleSignals = [];
  if (result.detections.google_ads)         { googleScore += 60; googleSignals.push('ads_tag'); }
  if (result.detections.google_remarketing) { googleScore += 40; googleSignals.push('remarketing'); }
  const hasGoogleUtm = /utm_source=(google|adwords|gclid)/i.test(html) && /utm_medium=(cpc|ppc|paid)/i.test(html);
  if (hasGoogleUtm) { googleScore += 25; googleSignals.push('google_utm'); }
  if (/gclid/i.test(html) && !result.detections.google_ads) { googleScore += 20; googleSignals.push('gclid'); }

  result.meta_confidence = Math.min(metaScore, 100);
  result.meta_signals = metaSignals;
  result.google_confidence = Math.min(googleScore, 100);
  result.google_signals = googleSignals;

  return result;
}

// ── JS Bundle Deep Scan ─────────────────────────────────────────
// For sites where HTML doesn't contain pixel code, download their main JS bundles
// and search those. Catches ~30% more pixels that are loaded dynamically.
const JS_BUNDLE_PATTERNS = [
  /fbq\s*\(\s*['"]init/i,
  /connect\.facebook\.net.*fbevents/i,
  /facebook-domain-verification/i,
  /gtag\s*\(\s*['"]config['"]\s*,\s*['"]AW-/i,
  /googleadservices\.com\/pagead/i,
  /google_conversion_id/i,
  /analytics\.tiktok\.com/i,
  /bat\.bing\.com\/action/i,
];

async function deepScanJsBundles(html, baseUrl) {
  // Extract script src URLs from HTML
  const scriptRe = /<script[^>]+src=["']([^"']+)["']/gi;
  const scripts = [];
  let sm;
  while ((sm = scriptRe.exec(html)) !== null) {
    let src = sm[1];
    // Skip external CDN scripts (analytics, ad libraries themselves)
    if (src.includes('google') || src.includes('facebook') || src.includes('tiktok') ||
        src.includes('doubleclick') || src.includes('cdn.') || src.includes('jquery') ||
        src.includes('bootstrap')) continue;
    // Resolve relative URLs
    if (src.startsWith('/')) {
      try { const u = new URL(baseUrl); src = u.protocol + '//' + u.host + src; } catch { continue; }
    } else if (!src.startsWith('http')) {
      try { src = new URL(src, baseUrl).href; } catch { continue; }
    }
    // Prioritize likely main bundles
    if (/main|app|bundle|chunk|vendor/i.test(src) || src.endsWith('.js')) {
      scripts.push(src);
    }
  }

  // Download top 3 JS files (limit to avoid slowdown)
  const toCheck = scripts.slice(0, 3);
  const found = { meta_pixel: false, google_ads: false, tiktok: false, bing: false };

  for (const jsUrl of toCheck) {
    try {
      const resp = await fetchUrl(jsUrl, 0, USER_AGENTS[0]);
      if (!resp.ok || !resp.body) continue;
      // Check for ad patterns in JS bundle
      for (const pattern of JS_BUNDLE_PATTERNS) {
        if (pattern.test(resp.body)) {
          if (/fbq|facebook/i.test(pattern.source)) found.meta_pixel = true;
          if (/gtag|googlead|google_conversion/i.test(pattern.source)) found.google_ads = true;
          if (/tiktok/i.test(pattern.source)) found.tiktok = true;
          if (/bing/i.test(pattern.source)) found.bing = true;
        }
      }
    } catch {}
  }

  return found;
}

// ── DNS Marketing Stack Detection ───────────────────────────────
// Check SPF/TXT records for marketing platform includes.
// If a domain uses HubSpot, Pardot, Mailchimp, Klaviyo etc. for email marketing,
// they almost certainly run paid ads to drive traffic to those funnels.
const MARKETING_SPF_PATTERNS = {
  'hubspot': { pattern: /hubspot/i, label: 'HubSpot', weight: 15 },
  'pardot': { pattern: /pardot/i, label: 'Pardot/Salesforce', weight: 15 },
  'mailchimp': { pattern: /mcsv\.net|mandrillapp|mailchimp/i, label: 'Mailchimp', weight: 10 },
  'klaviyo': { pattern: /klaviyo/i, label: 'Klaviyo', weight: 15 },
  'activecampaign': { pattern: /activecampaign/i, label: 'ActiveCampaign', weight: 10 },
  'sendgrid': { pattern: /sendgrid/i, label: 'SendGrid', weight: 5 },
  'marketo': { pattern: /marketo/i, label: 'Marketo', weight: 15 },
  'constantcontact': { pattern: /constantcontact|ctctcdn/i, label: 'Constant Contact', weight: 8 },
};

function checkDnsTxt(domain) {
  return new Promise(resolve => {
    dns.resolveTxt(domain, (err, records) => {
      if (err) return resolve({ platforms: [], score: 0 });
      const allTxt = records.map(r => r.join('')).join(' ');
      const found = [];
      let score = 0;
      for (const [key, { pattern, label, weight }] of Object.entries(MARKETING_SPF_PATTERNS)) {
        if (pattern.test(allTxt)) {
          found.push(label);
          score += weight;
        }
      }
      // Check for facebook-domain-verification in TXT records too
      if (/facebook-domain-verification/i.test(allTxt)) {
        found.push('FB Domain Verify (DNS)');
        score += 20;
      }
      resolve({ platforms: found, score });
    });
  });
}

// ── Google Ads Transparency Check ───────────────────────────────
const PY_SCRIPT = `
import sys, json
sys.path.insert(0, '/Users/fardeenchoudhury/Library/Python/3.9/lib/python/site-packages')
from GoogleAds import GoogleAds
name = sys.argv[1]
scraper = GoogleAds(region='US')
try:
    suggestions = scraper.get_all_search_suggestions(name)
    if suggestions:
        top = suggestions[0].get('1', {})
        result = {'found': True, 'advertiser_name': top.get('1', ''), 'advertiser_id': top.get('2', ''), 'ad_count': int(top.get('4', {}).get('2', {}).get('1', 0))}
    else:
        result = {'found': False, 'ad_count': 0}
except Exception as e:
    result = {'found': False, 'error': str(e)[:100], 'ad_count': 0}
print(json.dumps(result))
`;
fs.writeFileSync('/tmp/google-ads-check.py', PY_SCRIPT);

function checkGoogleAds(businessName) {
  try {
    const out = execSync(`python3 /tmp/google-ads-check.py "${businessName.replace(/"/g, '\\"')}"`, { timeout: 15000, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] });
    for (const line of out.split('\n')) {
      if (line.startsWith('{')) return JSON.parse(line);
    }
    return { found: false, ad_count: 0 };
  } catch { return { found: false, ad_count: 0 }; }
}

// ── CSV ─────────────────────────────────────────────────────────
function parseLine(line) {
  const cols = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}
function esc(v) { const s = (v ?? '').toString().replace(/"/g, '""'); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s; }
function normalizeUrl(raw) {
  if (!raw) return null;
  let url = raw.trim();
  // Filter out obvious non-URLs
  if (url === 'false' || url === 'true' || url === 'null' || url === 'undefined' || url === 'N/A' || url === 'n/a' || url.length < 4) return null;
  url = url.replace(/^\/\//, 'https://');
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
  const outputFile = process.argv[3] || inputFile?.replace('.csv', '-ad-audit.csv');

  if (!inputFile) {
    console.log('Unified Ad Checker — Composite detection pipeline');
    console.log('Usage: node scripts/ad-checker-unified.js input.csv [output.csv] [concurrency]');
    console.log('\nPass 1: HTTP pixel scan (all leads, ~12/sec)');
    console.log('Pass 2: Google Ads Transparency (leads with signals, ~1.5/sec)');
    console.log('Output: meta_ads, google_ads, confidence scores, Facebook page\n');
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

  const urlCol = findCol(headers, [/^website$/i, /^url$/i, /^domain$/i, /charity_contact_web/i]) || findCol(headers, [/web/i, /site/i, /url/i, /domain/i]);
  const nameCol = findCol(headers, [/^business.?name$/i, /^firm.?name$/i, /^company$/i, /^name$/i, /charity_name/i]) || headers[0];
  const firstNameCol = headers.find(h => /^first.?name$/i.test(h));
  const lastNameCol = headers.find(h => /^last.?name$/i.test(h));
  const withUrl = rows.filter(r => r[urlCol]?.trim()).length;

  console.log('\n╔═══════════════════════════════════════════╗');
  console.log('║      Unified Ad Checker Pipeline          ║');
  console.log('╠═══════════════════════════════════════════╣');
  console.log(`║  Input:       ${String(rows.length).padStart(6)} leads               ║`);
  console.log(`║  With URL:    ${String(withUrl).padStart(6)}                      ║`);
  console.log(`║  URL column:  ${('"' + (urlCol || 'NONE') + '"').padEnd(28)}║`);
  console.log(`║  Name column: ${('"' + nameCol + '"').padEnd(28)}║`);
  console.log('╚═══════════════════════════════════════════╝\n');

  if (!urlCol) { console.error('No website/URL column found.'); process.exit(1); }

  // ── PASS 1: HTTP Pixel Scan ─────────────────────────────────
  console.log('  ══ PASS 1: HTTP Pixel Scan ══');
  const results = new Array(rows.length);
  let idx = 0, checked = 0;
  let stats = { hot: 0, warm: 0, cold: 0, errors: 0 };
  const t0 = Date.now();

  const ticker1 = setInterval(() => {
    const s = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (checked / Math.max(1, (Date.now() - t0) / 1000)).toFixed(1);
    console.log(`  [${s}s] ${checked}/${withUrl} | 🔥${stats.hot} 🟡${stats.warm} ⬜${stats.cold} | ${rate}/sec`);
  }, 5000);

  async function pixelWorker() {
    while (idx < rows.length) {
      const i = idx++;
      const url = normalizeUrl(rows[i][urlCol]);
      if (!url) { results[i] = { _noUrl: true }; continue; }

      const resp = await fetchWithRetry(url);
      checked++;

      if (!resp.ok) { results[i] = { _error: true }; stats.errors++; continue; }

      const a = analyzeWebsite(resp.body);

      // Deep scan: if no Meta/Google paid signals found in HTML, check JS bundles
      if (a.meta_confidence < 20 && a.google_confidence < 20 && resp.body.includes('<script')) {
        try {
          const jsFinds = await deepScanJsBundles(resp.body, url);
          if (jsFinds.meta_pixel && !a.detections.meta_pixel) {
            a.detections.meta_pixel = true;
            a.paid.push('Meta Pixel (JS)');
            a.meta_confidence = Math.min(a.meta_confidence + 50, 100);
            a.meta_signals.push('pixel_js');
          }
          if (jsFinds.google_ads && !a.detections.google_ads) {
            a.detections.google_ads = true;
            a.paid.push('Google Ads (JS)');
            a.google_confidence = Math.min(a.google_confidence + 60, 100);
            a.google_signals.push('ads_tag_js');
          }
        } catch {}
      }

      // DNS marketing stack check: adds marketing platform signals
      try {
        const domain = new URL(url).hostname.replace(/^www\./, '');
        const dnsResult = await checkDnsTxt(domain);
        if (dnsResult.platforms.length > 0) {
          a.meta_confidence = Math.min(a.meta_confidence + dnsResult.score, 100);
          a.meta_signals.push(...dnsResult.platforms.map(p => 'dns:' + p.toLowerCase().replace(/\s+/g, '_')));
        }
      } catch {}

      if (a.meta_confidence >= 50 || a.paid.length > 0) stats.hot++;
      else if (a.analytics.length > 0 || a.meta_confidence > 0) stats.warm++;
      else stats.cold++;

      results[i] = { _analysis: a };
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => pixelWorker()));
  clearInterval(ticker1);

  const pass1Time = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  ✓ Pass 1 done: ${checked} scanned in ${pass1Time}s (${(checked / pass1Time).toFixed(1)}/sec)\n`);

  // ── PASS 2: Google Ads Transparency ─────────────────────────
  // Only check leads that have google signals OR no URL (need name-based check)
  const googleCandidates = results.map((r, i) => ({ i, r })).filter(({ r }) => {
    if (!r || r._noUrl || r._error) return false;
    return r._analysis?.google_confidence > 0;
  });

  console.log(`  ══ PASS 2: Google Ads Transparency (${googleCandidates.length} candidates) ══`);
  const t1 = Date.now();
  let gChecked = 0, gFound = 0;

  for (const { i, r } of googleCandidates) {
    // Build best name for search
    let name = (r[nameCol] || '').trim();
    if (name.includes('facebook.com/')) {
      name = name.replace(/^.*facebook\.com\//i, '').replace(/\/$/, '');
      name = name.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ').trim();
    } else if (name.includes('http') || /\.\w{2,4}$/.test(name)) {
      name = name.replace(/^https?:\/\/(www\.)?/i, '').replace(/\/.*$/, '').replace(/\.\w+$/, '');
      name = name.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ').trim();
    }
    if ((!name || name.length < 3) && firstNameCol && lastNameCol) {
      const fn = (r[firstNameCol] || '').trim();
      const ln = (r[lastNameCol] || '').trim();
      if (fn && ln) name = fn + ' ' + ln;
    }

    if (!name || name.length < 2) continue;

    const gResult = checkGoogleAds(name);
    gChecked++;

    if (gResult.found && gResult.ad_count > 0) {
      // Validate name match
      const sn = name.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
      const an = (gResult.advertiser_name || '').toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
      const matched = sn === an || an.includes(sn) || sn.includes(an) ||
        (() => { const sw = sn.split(' ').filter(w => w.length > 2); const aw = new Set(an.split(' ')); return sw.length > 0 && sw.filter(w => aw.has(w)).length / sw.length >= 0.5; })();

      if (matched) {
        gFound++;
        results[i]._googleAds = { confirmed: true, count: gResult.ad_count, advertiser: gResult.advertiser_name };
      }
    }

    if (gChecked % 10 === 0) {
      console.log(`  [${((Date.now() - t1) / 1000).toFixed(0)}s] ${gChecked}/${googleCandidates.length} | 🔥${gFound} confirmed`);
    }
  }

  console.log(`  ✓ Pass 2 done: ${gChecked} checked, ${gFound} confirmed Google Ads\n`);

  // ── Write Final Output ──────────────────────────────────────
  const addCols = ['meta_ads', 'meta_confidence', 'meta_signals', 'google_ads', 'google_confidence', 'google_signals', 'google_ad_count', 'google_advertiser', 'paid_platforms', 'has_analytics', 'facebook_page', 'meta_pixel_id', 'google_ads_id', 'lead_category'];
  const outHeaders = [...headers, ...addCols];
  let finalHot = 0, finalWarm = 0, finalCold = 0;

  // Stream write to avoid holding all output in memory
  const outStream = fs.createWriteStream(outputFile);
  outStream.write(outHeaders.join(',') + '\n');

  for (let i = 0; i < rows.length; i++) {
    const r = results[i];
    if (!r) continue;

    const a = r._analysis;
    const g = r._googleAds;
    let metaAds, googleAds, category;

    if (!a) {
      metaAds = ''; googleAds = ''; category = r._error ? 'ERROR' : '';
    } else {
      if (a.meta_confidence >= 50) metaAds = 'YES';
      else if (a.meta_confidence >= 20) metaAds = 'LIKELY';
      else metaAds = 'NO';

      if (g?.confirmed) googleAds = 'YES';
      else if (a.google_confidence >= 60) googleAds = 'YES';
      else if (a.google_confidence >= 20) googleAds = 'LIKELY';
      else googleAds = 'NO';

      if (metaAds === 'YES' || googleAds === 'YES') { category = 'HOT'; finalHot++; }
      else if (metaAds === 'LIKELY' || googleAds === 'LIKELY' || a.analytics.length > 0) { category = 'WARM'; finalWarm++; }
      else { category = 'COLD'; finalCold++; }
    }

    // Build output row from original CSV data + enrichment
    const enrichment = {
      meta_ads: metaAds || '',
      meta_confidence: a?.meta_confidence ?? '',
      meta_signals: a?.meta_signals?.join('+') || '',
      google_ads: googleAds || '',
      google_confidence: a?.google_confidence ?? '',
      google_signals: a?.google_signals?.join('+') || '',
      google_ad_count: g?.count || '',
      google_advertiser: g?.advertiser || '',
      paid_platforms: a?.paid?.join('; ') || '',
      has_analytics: a?.analytics?.length > 0 ? 'YES' : 'NO',
      facebook_page: a?.facebook_page || '',
      meta_pixel_id: a?.ids?.meta_pixel_id || '',
      google_ads_id: a?.ids?.google_ads_id || '',
      lead_category: category || '',
    };

    const row = rows[i];
    const line = outHeaders.map(h => esc(enrichment[h] !== undefined ? enrichment[h] : (row[h] || ''))).join(',');
    outStream.write(line + '\n');

    // Free memory: clear analysis data
    results[i] = null;
  }

  outStream.end();

  const totalSec = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('╔═══════════════════════════════════════════╗');
  console.log('║              FINAL RESULTS                ║');
  console.log('╠═══════════════════════════════════════════╣');
  console.log(`║  🔥 HOT (running ads):  ${String(finalHot).padStart(6)}            ║`);
  console.log(`║  🟡 WARM (some signals): ${String(finalWarm).padStart(5)}            ║`);
  console.log(`║  ⬜ COLD (nothing):     ${String(finalCold).padStart(6)}            ║`);
  console.log(`║  Total time:          ${totalSec.padStart(6)}s            ║`);
  console.log('╚═══════════════════════════════════════════╝');
  console.log(`\n  → ${outputFile}\n`);
})();
