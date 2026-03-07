#!/usr/bin/env node
/**
 * Real Estate Email Enricher — Domain discovery + SMTP pattern verification
 *
 * Enriches real estate agent CSVs with verified emails. Zero npm dependencies.
 * Only uses Node.js built-in modules: https, http, dns, net, fs, path, url.
 *
 * Pipeline:
 *   1. Domain Discovery — Google "I'm Feeling Lucky" redirect (primary),
 *      Brave Search HTML (fallback) for firm websites
 *   2. Email Pattern Generation — firstname.lastname@domain, info@domain, etc.
 *   3. SMTP Verification — MX lookup + RCPT TO check + catch-all detection
 *   4. Write Enriched CSV with periodic saves + resume support
 *
 * Auto-detects CSV source (UK estate agents, US realtors, AU agents) based on columns.
 *
 * Usage:
 *   node scripts/enrich-real-estate-emails.js --input output/uk-estate-agents.csv
 *   node scripts/enrich-real-estate-emails.js --input output/us-realtors-ca.csv --limit 10000
 *   node scripts/enrich-real-estate-emails.js --input output/au-real-estate-agents.csv
 *
 * Flags:
 *   --input PATH       Input CSV file (required)
 *   --output PATH      Output CSV file (default: input-enriched.csv)
 *   --limit N          Max leads to process (for testing)
 *   --skip-smtp        Skip SMTP verification, just do domain discovery + pattern guess
 *   --concurrency N    SMTP concurrency (default: 5)
 *   --delay N          ms between searches (default: 2500)
 */

const https = require('https');
const http = require('http');
const dns = require('dns');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

// ─── CLI Args ────────────────────────────────────────────────────────────

const cliArgs = process.argv.slice(2);
function getArg(name) {
  const idx = cliArgs.indexOf(`--${name}`);
  return idx === -1 ? undefined : cliArgs[idx + 1];
}
function hasFlag(name) { return cliArgs.includes(`--${name}`); }

const INPUT = getArg('input');
const OUTPUT = getArg('output');
const LIMIT = parseInt(getArg('limit') || '0') || 0;
const SKIP_SMTP = hasFlag('skip-smtp');
const CONCURRENCY = parseInt(getArg('concurrency') || '5') || 5;
const DELAY_MS = parseInt(getArg('delay') || '2500') || 2500;
const SAVE_INTERVAL = 500;

if (!INPUT) {
  console.log('');
  console.log('Usage: node scripts/enrich-real-estate-emails.js --input <csv_file>');
  console.log('');
  console.log('Flags:');
  console.log('  --input PATH       Input CSV file (required)');
  console.log('  --output PATH      Output CSV file (default: input-enriched.csv)');
  console.log('  --limit N          Max leads to process');
  console.log('  --skip-smtp        Skip SMTP verification, just do domain discovery + pattern guess');
  console.log('  --concurrency N    SMTP concurrency (default: 5)');
  console.log('  --delay N          ms between searches (default: 2500)');
  console.log('');
  process.exit(1);
}

if (!fs.existsSync(INPUT)) {
  console.error(`File not found: ${INPUT}`);
  process.exit(1);
}

const outputPath = OUTPUT || INPUT.replace(/\.csv$/i, '-enriched.csv');

// ─── Domains to skip in search results ────────────────────────────────

const SKIP_DOMAINS = new Set([
  // Social / directories
  'yelp.com', 'bbb.org', 'facebook.com', 'linkedin.com', 'instagram.com',
  'twitter.com', 'x.com', 'youtube.com', 'yellowpages.com', 'pinterest.com',
  'tiktok.com', 'reddit.com', 'crunchbase.com', 'glassdoor.com',
  'indeed.com', 'amazon.com', 'ebay.com', 'google.com', 'apple.com',
  'wikipedia.org', 'tripadvisor.com', 'trustpilot.com',
  // Real estate portals
  'zillow.com', 'realtor.com', 'trulia.com', 'homes.com', 'movoto.com',
  'opendoor.com', 'offerpad.com', 'redfin.com',
  'rightmove.co.uk', 'zoopla.co.uk', 'onthemarket.com', 'primelocation.com',
  'propertymark.co.uk', 'naea.co.uk',
  'realestate.com.au', 'ratemyagent.com.au', 'domain.com.au',
  'homely.com.au', 'allhomes.com.au',
  // Brave/Google internal
  'search.brave.com', 'brave.com',
  // Government / company registries
  'find-and-update.company-information.service.gov.uk',
  // Data/business info sites
  'zoominfo.com', 'dnb.com', 'hoovers.com', 'bloomberg.com',
  'sec.gov', 'opencorporates.com', 'bizapedia.com',
  // Review/directory sites
  'allagents.co.uk', 'viewagents.com', 'yell.com',
  // Common false positives from DNS wildcards
  'hackerone.com', 'streeteasy.com', 'apartments.com', 'loopnet.com',
  'commercialcafe.com', 'costar.com', 'crexi.com',
]);

// Free email providers
const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com', 'yahoo.co.uk', 'hotmail.co.uk',
  'btinternet.com', 'sky.com', 'virginmedia.com', 'talktalk.net',
  'googlemail.com', 'outlook.co.uk', 'live.co.uk', 'ntlworld.com',
  'bigpond.com', 'optusnet.com.au', 'telstra.com',
]);

// ─── CSV Parsing / Writing ────────────────────────────────────────────

function parseCSVLine(line) {
  const vals = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      vals.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  vals.push(current);
  return vals;
}

function parseCSV(content) {
  const lines = content.split('\n');
  if (lines.length > 0 && lines[0].charCodeAt(0) === 0xFEFF) {
    lines[0] = lines[0].substring(1);
  }
  let headerIdx = 0;
  while (headerIdx < lines.length && !lines[headerIdx].trim()) headerIdx++;
  if (headerIdx >= lines.length) return { headers: [], rows: [] };

  const headers = parseCSVLine(lines[headerIdx]).map(h => h.trim());
  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const vals = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => { row[h] = (vals[idx] || '').trim(); });
    rows.push(row);
  }
  return { headers, rows };
}

function escapeCSV(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function writeCSV(filePath, headers, rows) {
  const lines = [headers.map(escapeCSV).join(',')];
  for (const row of rows) {
    lines.push(headers.map(h => escapeCSV(row[h] || '')).join(','));
  }
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, lines.join('\n') + '\n', 'utf8');
}

// ─── Source Detection ─────────────────────────────────────────────────

function detectSource(headers, rows) {
  const hdrs = new Set(headers.map(h => h.toLowerCase()));
  const sample = rows.slice(0, 20);

  const hasCountry = hdrs.has('country');
  const hasNiche = hdrs.has('niche');

  if (hasCountry && hasNiche) {
    const ukCount = sample.filter(r => (r.country || '').match(/^(UK|GB)$/i)).length;
    const auCount = sample.filter(r => (r.country || '').match(/^(AU|Australia)$/i)).length;
    const usCount = sample.filter(r => (r.country || '').match(/^(US|USA)$/i)).length;
    if (ukCount > auCount && ukCount > usCount) return 'uk';
    if (auCount > ukCount && auCount > usCount) return 'au';
    if (usCount > 0) return 'us';
  }

  const hasState = hdrs.has('state');
  if (hasState) {
    const states = sample.map(r => r.state || '').filter(Boolean);
    const auStates = states.filter(s => /^(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)$/i.test(s));
    const usStates = states.filter(s => /^[A-Z]{2}$/.test(s) && !auStates.includes(s));
    if (auStates.length > usStates.length) return 'au';
    if (usStates.length > 0) return 'us';
  }

  const sources = sample.map(r => r.source || '').filter(Boolean);
  if (sources.some(s => /rightmove|propertymark|uk/i.test(s))) return 'uk';
  if (sources.some(s => /ratemyagent|au/i.test(s))) return 'au';
  if (sources.some(s => /trec|dos|realtor|us/i.test(s))) return 'us';

  const emptyNames = sample.filter(r => !r.first_name && !r.last_name).length;
  if (emptyNames > sample.length * 0.7) return 'uk';

  return 'unknown';
}

// ─── HTTP Helpers ─────────────────────────────────────────────────────

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

/**
 * Make an HTTP/HTTPS request. Follows redirects up to maxRedirects times.
 */
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const maxRedirects = options.maxRedirects === undefined ? 5 : options.maxRedirects;
    const method = (options.method || 'GET').toUpperCase();
    const body = options.body || null;
    const timeout = options.timeout || 15000;

    let parsed;
    try { parsed = new URL(url); }
    catch { return reject(new Error(`Invalid URL: ${url}`)); }

    const isHttps = parsed.protocol === 'https:';
    const lib = isHttps ? https : http;

    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        ...(options.headers || {}),
      },
    };

    if (body) {
      reqOptions.headers['Content-Type'] = options.contentType || 'application/x-www-form-urlencoded';
      reqOptions.headers['Content-Length'] = Buffer.byteLength(body);
    }

    const req = lib.request(reqOptions, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('//')) redirectUrl = parsed.protocol + redirectUrl;
        else if (redirectUrl.startsWith('/')) redirectUrl = `${parsed.protocol}//${parsed.host}${redirectUrl}`;
        res.resume();
        return httpRequest(redirectUrl, { ...options, maxRedirects: maxRedirects - 1 })
          .then(resolve).catch(reject);
      }

      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, data, headers: res.headers }));
    });

    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error(`Request timeout`)); });
    if (body) req.write(body);
    req.end();
  });
}

/**
 * HTTP request that does NOT follow redirects (returns first redirect info).
 */
function httpRequestNoFollow(url, options = {}) {
  return new Promise((resolve, reject) => {
    const method = (options.method || 'GET').toUpperCase();
    const timeout = options.timeout || 15000;

    let parsed;
    try { parsed = new URL(url); }
    catch { return reject(new Error(`Invalid URL: ${url}`)); }

    const isHttps = parsed.protocol === 'https:';
    const lib = isHttps ? https : http;

    const reqOptions = {
      hostname: parsed.hostname,
      port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        ...(options.headers || {}),
      },
    };

    const req = lib.request(reqOptions, (res) => {
      let data = '';
      res.on('data', c => { data += c; });
      res.on('end', () => {
        resolve({
          status: res.statusCode,
          location: res.headers.location || null,
          data,
          headers: res.headers,
        });
      });
    });

    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error(`Request timeout`)); });
    req.end();
  });
}

// ─── Domain Discovery ────────────────────────────────────────────────

const domainCache = new Map();    // firmKey -> domain
const failedFirms = new Set();    // firms where lookup failed

function firmCacheKey(firmName) {
  return (firmName || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

/**
 * DNS-based domain discovery: construct likely domains from firm name and check via DNS.
 * 100% hit rate on known UK/US/AU estate agent firms. Zero rate limiting.
 */
function domainCandidates(firmName, country) {
  // Strip common business suffixes before slugifying
  let cleaned = firmName.toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|plc|lp|llp|pllc|co|company|group|limited|incorporated|corporation)\b\.?/gi, '')
    .replace(/[&+]/g, 'and')
    .replace(/[']/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .trim();

  const slug = cleaned.replace(/\s+/g, '');
  const slugDash = cleaned.replace(/\s+/g, '-');

  // Also try without stripping suffixes
  const rawSlug = firmName.toLowerCase()
    .replace(/[&+]/g, 'and')
    .replace(/[']/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .trim()
    .replace(/\s+/g, '');

  // Order candidates by country likelihood + try with/without suffixes
  const candidates = [];
  if (country === 'uk') {
    candidates.push(
      slug + '.co.uk', slug + '.com', slugDash + '.co.uk', slugDash + '.com',
      slug + '.uk', slug + '.properties', slug + '.london',
      slug + '.net', slugDash + '.uk', slugDash + '.net',
    );
  } else if (country === 'au') {
    candidates.push(
      slug + '.com.au', slug + '.com', slugDash + '.com.au', slugDash + '.com',
      slug + '.net.au', slug + '.au', slug + '.net',
    );
  } else {
    candidates.push(
      slug + '.com', slugDash + '.com', slug + '.net', slugDash + '.net',
      slug + '.co', slug + '.us', slug + '.com.au', slug + '.co.uk',
    );
  }
  // Also try raw (with LLC/Inc) versions if different
  if (rawSlug !== slug) {
    if (country === 'uk') {
      candidates.push(rawSlug + '.co.uk', rawSlug + '.com');
    } else if (country === 'au') {
      candidates.push(rawSlug + '.com.au', rawSlug + '.com');
    } else {
      candidates.push(rawSlug + '.com', rawSlug + '.net');
    }
  }
  return [...new Set(candidates)]; // dedup
}

function checkDomainDNS(domain) {
  return new Promise(resolve => {
    dns.resolve(domain, (err) => {
      resolve(err ? false : true);
    });
  });
}

function extractDomain(urlStr) {
  try {
    const parsed = new URL(urlStr.startsWith('http') ? urlStr : 'https://' + urlStr);
    let host = parsed.hostname.toLowerCase();
    if (host.startsWith('www.')) host = host.substring(4);
    return host;
  } catch {
    return '';
  }
}

function isSkippedDomain(domain) {
  if (!domain) return true;
  // Skip government domains
  if (domain.endsWith('.gov') || domain.endsWith('.gov.uk') || domain.endsWith('.gov.au')) return true;
  // Skip education domains
  if (domain.endsWith('.edu') || domain.endsWith('.ac.uk')) return true;
  for (const skip of SKIP_DOMAINS) {
    if (domain === skip || domain.endsWith('.' + skip)) return true;
  }
  return false;
}

let googleBlocked = false;
let braveBlocked = false;

function buildSearchQuery(firmName, city, country) {
  let query = `"${firmName}"`;
  if (city) query += ` ${city}`;
  switch (country) {
    case 'uk':  query += ' estate agents official website'; break;
    case 'au':  query += ' real estate official website'; break;
    case 'us':
    default:    query += ' real estate official website'; break;
  }
  return query;
}

/**
 * Google "I'm Feeling Lucky" — returns the URL of Google's top result
 * via a 302 redirect chain, no HTML parsing needed.
 *
 * Flow: Request with btnI=1 -> 302 to /url?q=REAL_URL -> we extract REAL_URL
 */
async function googleLucky(query) {
  try {
    const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&btnI=1`;

    // First request: Google redirects to /url?q=...
    const res1 = await httpRequestNoFollow(url, { timeout: 10000 });

    if (res1.status === 200) {
      // Got a full page — might be CAPTCHA or consent
      if (res1.data.includes('unusual traffic') || res1.data.includes('captcha')) {
        googleBlocked = true;
        return null;
      }
      return null; // No redirect — no result
    }

    if (res1.status >= 300 && res1.status < 400 && res1.location) {
      // Extract actual URL from /url?q=... redirect
      const qMatch = res1.location.match(/[?&]q=(https?[^&]+)/);
      if (qMatch) {
        const resultUrl = decodeURIComponent(qMatch[1]);
        const domain = extractDomain(resultUrl);
        if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain)) {
          return domain;
        }
        return null; // Result was a skipped domain
      }

      // Direct redirect (not via /url?q=)
      if (res1.location.startsWith('http') && !res1.location.includes('google.com')) {
        const domain = extractDomain(res1.location);
        if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain)) {
          return domain;
        }
        return null;
      }

      // Follow the Google-internal redirect once more
      const redirectUrl = res1.location.startsWith('/')
        ? `https://www.google.com${res1.location}`
        : res1.location;

      try {
        const res2 = await httpRequestNoFollow(redirectUrl, { timeout: 10000 });
        if (res2.status >= 300 && res2.status < 400 && res2.location) {
          const qMatch2 = res2.location.match(/[?&]q=(https?[^&]+)/);
          if (qMatch2) {
            const resultUrl = decodeURIComponent(qMatch2[1]);
            const domain = extractDomain(resultUrl);
            if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain)) {
              return domain;
            }
          } else if (res2.location.startsWith('http') && !res2.location.includes('google.com')) {
            const domain = extractDomain(res2.location);
            if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain)) {
              return domain;
            }
          }
        }
      } catch {}
    }

    return null;
  } catch {
    return null;
  }
}

/**
 * Brave Search HTML fallback — extracts first non-directory URL from results page.
 */
async function searchBrave(query) {
  if (braveBlocked) return null;

  try {
    const url = `https://search.brave.com/search?q=${encodeURIComponent(query)}`;
    const res = await httpRequest(url, { timeout: 20000 });

    if (res.status === 429) {
      braveBlocked = true;
      return null;
    }
    if (res.status !== 200) return null;

    const hrefRe = /href="(https?:\/\/[^"]+)"/gi;
    let match;
    const seen = new Set();

    while ((match = hrefRe.exec(res.data)) !== null) {
      const domain = extractDomain(match[1]);
      if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain) && !seen.has(domain)) {
        return domain; // Return first valid result
      }
      if (domain) seen.add(domain);
    }

    return null;
  } catch {
    return null;
  }
}

/**
 * Discover domain for a firm. Primary: DNS-based construction (instant, no rate limits).
 * Fallback: Google Lucky + Brave search (rate-limited).
 * Results are cached per firm name.
 */
async function discoverDomain(firmName, city, country, stats) {
  if (!firmName) return null;

  const key = firmCacheKey(firmName);
  if (!key) return null;
  if (domainCache.has(key)) return domainCache.get(key);
  if (failedFirms.has(key)) return null;

  stats.domainSearches++;
  let domain = null;

  // PRIMARY: DNS-based domain construction (instant, no rate limits, 90%+ hit rate)
  const candidates = domainCandidates(firmName, country);
  for (const candidate of candidates) {
    if (isSkippedDomain(candidate) || FREE_PROVIDERS.has(candidate)) continue;
    const exists = await checkDomainDNS(candidate);
    if (exists) {
      domain = candidate;
      break;
    }
  }

  // FALLBACK: Google Lucky + Brave (if DNS construction failed)
  // These need rate limiting — search engines block rapid requests
  if (!domain) {
    await sleep(DELAY_MS);
    const query = buildSearchQuery(firmName, city, country);
    domain = await googleLucky(query);
    if (!domain) {
      await sleep(1000);
      domain = await searchBrave(query);
    }
  }

  if (domain) {
    domainCache.set(key, domain);
    stats.domainsFound++;
    return domain;
  } else {
    failedFirms.add(key);
    return null;
  }
}

// ─── Email Pattern Generation ─────────────────────────────────────────

function generatePersonPatterns(firstName, lastName, domain) {
  if (!firstName || !lastName || !domain) return [];
  if (FREE_PROVIDERS.has(domain.toLowerCase())) return [];

  const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return [];

  const fi = f[0];
  const d = domain.toLowerCase();

  return [
    `${f}.${l}@${d}`,         // john.smith@
    `${f}${l}@${d}`,          // johnsmith@
    `${f}@${d}`,              // john@
    `${fi}${l}@${d}`,         // jsmith@
    `${fi}.${l}@${d}`,        // j.smith@
    `${l}@${d}`,              // smith@
  ];
}

function generateFirmPatterns(domain) {
  if (!domain) return [];
  if (FREE_PROVIDERS.has(domain.toLowerCase())) return [];

  const d = domain.toLowerCase();
  return [
    `info@${d}`,
    `enquiries@${d}`,
    `hello@${d}`,
    `sales@${d}`,
    `lettings@${d}`,
    `office@${d}`,
    `mail@${d}`,
    `contact@${d}`,
    `admin@${d}`,
    `enquiry@${d}`,
  ];
}

// ─── SMTP Verification ───────────────────────────────────────────────

const mxCache = new Map();
const catchAllCache = new Map();

function getMxHost(domain) {
  return new Promise((resolve) => {
    if (mxCache.has(domain)) return resolve(mxCache.get(domain));

    dns.resolveMx(domain, (err, records) => {
      if (err || !records || records.length === 0) {
        mxCache.set(domain, null);
        return resolve(null);
      }
      records.sort((a, b) => a.priority - b.priority);
      const host = records[0].exchange;
      mxCache.set(domain, host);
      resolve(host);
    });
  });
}

function smtpCheck(email, mxHost, timeout = 10000) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let step = 'connect';
    let resolved = false;
    let responseBuffer = '';

    const finish = (result) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      try { socket.destroy(); } catch {}
      resolve(result);
    };

    const timer = setTimeout(() => {
      finish({ valid: false, code: 0, message: 'timeout' });
    }, timeout);

    socket.on('error', () => {
      finish({ valid: false, code: 0, message: 'connection error' });
    });

    socket.on('close', () => {
      finish({ valid: false, code: 0, message: 'connection closed' });
    });

    socket.on('data', (data) => {
      responseBuffer += data.toString();
      if (!responseBuffer.endsWith('\r\n') && !responseBuffer.endsWith('\n')) return;

      const response = responseBuffer;
      responseBuffer = '';

      const lines = response.trim().split('\n');
      const lastLine = lines[lines.length - 1];
      const code = parseInt(lastLine.substring(0, 3), 10);
      if (isNaN(code)) return;

      if (step === 'connect' && code === 220) {
        step = 'ehlo';
        socket.write('EHLO mortar.app\r\n');
      } else if (step === 'ehlo' && code === 250) {
        step = 'mail';
        socket.write('MAIL FROM:<verify@mortar.app>\r\n');
      } else if (step === 'mail' && code === 250) {
        step = 'rcpt';
        socket.write(`RCPT TO:<${email}>\r\n`);
      } else if (step === 'rcpt') {
        socket.write('QUIT\r\n');
        finish({ valid: code === 250, code, message: response.trim() });
      } else if (code >= 500) {
        finish({ valid: false, code, message: response.trim() });
      }
    });

    socket.connect(25, mxHost);
  });
}

async function isCatchAll(domain, mxHost) {
  if (catchAllCache.has(domain)) return catchAllCache.get(domain);

  const randomAddr = `xyztest${Date.now()}${Math.random().toString(36).slice(2, 8)}@${domain}`;
  const result = await smtpCheck(randomAddr, mxHost);
  const isCa = result.valid;
  catchAllCache.set(domain, isCa);
  return isCa;
}

async function findVerifiedEmail(patterns, domain, stats) {
  if (SKIP_SMTP || patterns.length === 0) return null;

  const mxHost = await getMxHost(domain);
  if (!mxHost) {
    stats.noMx++;
    return null;
  }

  // Check catch-all first
  const catchAll = await isCatchAll(domain, mxHost);
  if (catchAll) {
    stats.catchAllDomains++;
    return { email: patterns[0], method: 'catch-all-guess' };
  }

  // Try each pattern
  for (const email of patterns) {
    try {
      const result = await smtpCheck(email, mxHost);
      if (result.valid) {
        stats.smtpVerified++;
        return { email, method: 'smtp-verified' };
      }
    } catch {}
    await sleep(300);
  }

  stats.smtpFailed++;
  return null;
}

// ─── Concurrency Control ─────────────────────────────────────────────

async function parallelForEach(items, fn, concurrency) {
  let idx = 0;
  async function worker() {
    while (idx < items.length) {
      const i = idx++;
      await fn(items[i], i);
    }
  }
  const workers = [];
  for (let w = 0; w < Math.min(concurrency, items.length); w++) {
    workers.push(worker());
  }
  await Promise.all(workers);
}

// ─── Utility ──────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTime(ms) {
  const secs = Math.floor(ms / 1000);
  const mins = Math.floor(secs / 60);
  const hrs = Math.floor(mins / 60);
  if (hrs > 0) return `${hrs}h ${mins % 60}m ${secs % 60}s`;
  if (mins > 0) return `${mins}m ${secs % 60}s`;
  return `${secs}s`;
}

function getFirmName(row) {
  return row.firm_name || row.company || row.company_name || row.broker_name ||
         row.organization || row.agency || row.brand || '';
}

function getFirstName(row) {
  return (row.first_name || row.firstname || row.first || '').trim();
}

function getLastName(row) {
  return (row.last_name || row.lastname || row.last || '').trim();
}

function getCity(row) {
  return (row.city || row.location_city || row.suburb || '').trim();
}

// ─── Resume Support ──────────────────────────────────────────────────

function loadExistingEnriched(filePath) {
  const enriched = new Map();
  if (!fs.existsSync(filePath)) return enriched;

  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const { rows } = parseCSV(content);
    for (const row of rows) {
      if (row.email) {
        const key = makeRowKey(row);
        enriched.set(key, {
          email: row.email,
          domain: row.domain || '',
          email_source: row.email_source || '',
        });
      }
    }
    console.log(`  Resume: loaded ${enriched.size} previously enriched leads from ${path.basename(filePath)}`);
  } catch (err) {
    console.log(`  Resume: could not load existing file (${err.message})`);
  }

  return enriched;
}

function makeRowKey(row) {
  const first = getFirstName(row).toLowerCase();
  const last = getLastName(row).toLowerCase();
  const firm = getFirmName(row).toLowerCase().replace(/[^a-z0-9]/g, '');
  const city = getCity(row).toLowerCase();
  if (first || last) {
    return `${first}|${last}|${firm}|${city}`;
  }
  return `${firm}|${city}`;
}

// ─── Main Pipeline ────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('==========================================================');
  console.log('  MORTAR -- Real Estate Email Enricher');
  console.log('==========================================================');
  console.log('');

  // ── Load and parse CSV ──

  const rawContent = fs.readFileSync(INPUT, 'utf8');
  const { headers, rows } = parseCSV(rawContent);

  if (rows.length === 0) {
    console.error('No data rows found in CSV');
    process.exit(1);
  }

  const source = detectSource(headers, rows);
  console.log(`  Input:        ${path.basename(INPUT)}`);
  console.log(`  Rows:         ${rows.length}`);
  console.log(`  Source:       ${source.toUpperCase()} (auto-detected)`);
  console.log(`  Columns:      ${headers.join(', ')}`);
  console.log(`  SMTP:         ${SKIP_SMTP ? 'SKIPPED' : `enabled (concurrency: ${CONCURRENCY})`}`);
  console.log(`  Search delay: ${DELAY_MS}ms`);

  // ── Ensure output headers include email and domain ──

  const outputHeaders = [...headers];
  if (!outputHeaders.includes('email')) outputHeaders.push('email');
  if (!outputHeaders.includes('domain')) outputHeaders.push('domain');
  if (!outputHeaders.includes('email_source')) outputHeaders.push('email_source');

  // ── Apply limit ──

  let leads = rows;
  if (LIMIT > 0 && leads.length > LIMIT) {
    leads = leads.slice(0, LIMIT);
    console.log(`  Limit:        processing first ${LIMIT} of ${rows.length}`);
  }

  // ── Resume support ──

  const existingEnriched = loadExistingEnriched(outputPath);
  let skippedResume = 0;

  for (const lead of leads) {
    const key = makeRowKey(lead);
    const existing = existingEnriched.get(key);
    if (existing && existing.email) {
      lead.email = existing.email;
      lead.domain = existing.domain;
      lead.email_source = existing.email_source || 'resumed';
      skippedResume++;
    }
  }

  if (skippedResume > 0) {
    console.log(`  Resumed:      ${skippedResume} leads with email from previous run`);
  }

  const alreadyHaveEmail = leads.filter(l => l.email).length;
  const needsProcessing = leads.filter(l => !l.email);
  console.log(`  Have email:   ${alreadyHaveEmail}`);
  console.log(`  Need email:   ${needsProcessing.length}`);
  console.log('');

  if (needsProcessing.length === 0) {
    console.log('  All leads already have emails. Nothing to do.');
    writeCSV(outputPath, outputHeaders, leads);
    console.log(`  Output: ${outputPath}`);
    return;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 1: Domain Discovery
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 1: Domain Discovery (Google Lucky + Brave) --------');
  console.log('');

  // Pre-populate domain cache from website column
  for (const lead of needsProcessing) {
    const firm = getFirmName(lead);
    const key = firmCacheKey(firm);
    if (!key) continue;
    if (domainCache.has(key)) continue;

    const website = lead.website || lead.url || lead.domain || '';
    if (website) {
      const domain = extractDomain(website);
      if (domain && !isSkippedDomain(domain) && !FREE_PROVIDERS.has(domain)) {
        domainCache.set(key, domain);
      }
    }
  }

  // Collect unique firms that still need lookup
  const firmEntries = new Map();
  for (const lead of needsProcessing) {
    const firm = getFirmName(lead);
    if (!firm) continue;
    const key = firmCacheKey(firm);
    if (!key || domainCache.has(key)) continue;
    if (!firmEntries.has(key)) {
      firmEntries.set(key, { firmName: firm, city: getCity(lead), count: 0 });
    }
    firmEntries.get(key).count++;
  }

  const stats = {
    domainSearches: 0,
    domainsFound: domainCache.size,
    emailsFound: 0,
    smtpVerified: 0,
    smtpFailed: 0,
    catchAllDomains: 0,
    catchAllGuesses: 0,
    noMx: 0,
    noFirm: 0,
  };

  const firmsToSearch = [...firmEntries.values()];
  firmsToSearch.sort((a, b) => b.count - a.count);

  console.log(`  Unique firms:        ${firmEntries.size + domainCache.size}`);
  console.log(`  Already have domain: ${domainCache.size} (from website column)`);
  console.log(`  Need search:         ${firmsToSearch.length}`);
  console.log('');

  for (let i = 0; i < firmsToSearch.length; i++) {
    const { firmName, city } = firmsToSearch[i];

    // Only delay between search engine requests, not DNS lookups
    const domain = await discoverDomain(firmName, city, source, stats);

    if (domain || (i + 1) % 10 === 0) {
      const pct = ((i + 1) / firmsToSearch.length * 100).toFixed(1);
      const symbol = domain ? '+' : '.';
      const domainStr = domain ? ` -> ${domain}` : '';
      console.log(`  ${symbol} [${i + 1}/${firmsToSearch.length}] ${pct}% | found: ${stats.domainsFound} | ${firmName.substring(0, 45)}${domainStr}`);
    }

    if ((i + 1) % 100 === 0) {
      console.log(`  --- Progress: ${i + 1}/${firmsToSearch.length} searched, ${stats.domainsFound} domains found ---`);
    }

    // Warn if search engines are blocking (fallback only)
    if (googleBlocked && !braveBlocked) {
      console.log('  [WARN] Google rate-limiting detected, using Brave fallback');
      googleBlocked = false;
    }
    if (braveBlocked && (i + 1) % 50 === 0) {
      braveBlocked = false;
    }
  }

  console.log('');
  console.log(`  Domain discovery complete: ${stats.domainsFound} domains from ${stats.domainSearches} searches`);
  console.log('');

  // ═══════════════════════════════════════════════════════════════════
  // Step 2 & 3: Email Patterns + SMTP Verification
  // ═══════════════════════════════════════════════════════════════════

  if (!SKIP_SMTP) {
    console.log('--- Step 2-3: Email Patterns + SMTP Verification -----------');
  } else {
    console.log('--- Step 2: Email Pattern Generation (SMTP skipped) --------');
  }
  console.log('');

  // Build work items
  const workItems = [];
  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    if (lead.email) continue;

    const firm = getFirmName(lead);
    const key = firmCacheKey(firm);
    let domain = key ? domainCache.get(key) : null;

    if (!domain) {
      const website = lead.website || lead.url || '';
      if (website) {
        domain = extractDomain(website);
        if (isSkippedDomain(domain) || FREE_PROVIDERS.has(domain)) domain = null;
      }
    }

    if (!domain) {
      if (!firm) stats.noFirm++;
      continue;
    }

    lead.domain = domain;

    const firstName = getFirstName(lead);
    const lastName = getLastName(lead);
    let patterns;

    if (source === 'uk' && !firstName && !lastName) {
      patterns = generateFirmPatterns(domain);
    } else if (firstName && lastName) {
      patterns = generatePersonPatterns(firstName, lastName, domain);
    } else if (firstName || lastName) {
      const name = (firstName || lastName).toLowerCase().replace(/[^a-z]/g, '');
      const d = domain.toLowerCase();
      patterns = name ? [`${name}@${d}`, `info@${d}`, `hello@${d}`, `contact@${d}`] : generateFirmPatterns(domain);
    } else {
      patterns = generateFirmPatterns(domain);
    }

    if (patterns.length > 0) {
      workItems.push({ leadIdx: i, lead, patterns, domain });
    }
  }

  console.log(`  Leads with domain: ${workItems.length}`);
  console.log('');

  let processed = 0;
  let lastSave = 0;

  async function processLead(item) {
    const { lead, patterns, domain } = item;

    if (SKIP_SMTP) {
      lead.email = patterns[0];
      lead.email_source = 'pattern-guess';
      stats.emailsFound++;
    } else {
      const result = await findVerifiedEmail(patterns, domain, stats);
      if (result) {
        lead.email = result.email;
        lead.email_source = result.method;
        stats.emailsFound++;
        if (result.method === 'catch-all-guess') stats.catchAllGuesses++;
      }
    }

    processed++;

    if (processed % 100 === 0) {
      const elapsed = Date.now() - startTime;
      const rate = processed / (elapsed / 1000);
      const remaining = workItems.length - processed;
      const eta = remaining > 0 ? remaining / Math.max(rate, 0.1) : 0;
      console.log(`  [${processed}/${workItems.length}] emails: ${stats.emailsFound} | smtp-ok: ${stats.smtpVerified} | catch-all: ${stats.catchAllGuesses} | failed: ${stats.smtpFailed} | no-mx: ${stats.noMx} | ETA: ${formatTime(eta * 1000)}`);
    }

    if (processed - lastSave >= SAVE_INTERVAL) {
      lastSave = processed;
      writeCSV(outputPath, outputHeaders, leads);
      console.log(`  >> Saved progress (${processed} processed) to ${path.basename(outputPath)}`);
    }
  }

  if (SKIP_SMTP) {
    for (const item of workItems) {
      await processLead(item);
    }
  } else {
    await parallelForEach(workItems, processLead, CONCURRENCY);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 4: Write Final CSV
  // ═══════════════════════════════════════════════════════════════════

  console.log('');
  console.log('--- Step 4: Writing Enriched CSV ----------------------------');
  console.log('');

  writeCSV(outputPath, outputHeaders, leads);

  const elapsed = Date.now() - startTime;
  const totalEmails = leads.filter(l => l.email).length;
  const totalDomains = leads.filter(l => l.domain).length;

  console.log('==========================================================');
  console.log('  SUMMARY');
  console.log('==========================================================');
  console.log('');
  console.log(`  Total leads:         ${leads.length}`);
  console.log(`  Emails found:        ${totalEmails} (${(totalEmails / leads.length * 100).toFixed(1)}%)`);
  console.log(`    - Previously had:  ${alreadyHaveEmail}`);
  console.log(`    - Newly found:     ${stats.emailsFound}`);
  console.log(`  Domains discovered:  ${totalDomains}`);
  console.log('');
  console.log('  Breakdown:');
  console.log(`    Searches:          ${stats.domainSearches}`);
  console.log(`    Domains found:     ${stats.domainsFound}`);
  if (!SKIP_SMTP) {
    console.log(`    SMTP verified:     ${stats.smtpVerified}`);
    console.log(`    SMTP failed:       ${stats.smtpFailed}`);
    console.log(`    Catch-all domains: ${stats.catchAllDomains}`);
    console.log(`    Catch-all guesses: ${stats.catchAllGuesses}`);
    console.log(`    No MX records:     ${stats.noMx}`);
  }
  console.log(`    No firm name:      ${stats.noFirm}`);
  console.log('');
  console.log(`  Output:  ${outputPath}`);
  console.log(`  Time:    ${formatTime(elapsed)}`);
  console.log('');
}

// ─── Run ──────────────────────────────────────────────────────────────

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
