#!/usr/bin/env node
/**
 * Scrape MartialArtsSchoolsDirectory.com
 *
 * The site is an OptimizeCDN directory. Listing URLs follow the shape
 *   /{country-or-state-slug}/{city-slug}/martial-arts-schools/{business-slug}
 * with additional suffixes like `/connect` or `/reviews` that we ignore.
 *
 * Strategy
 *   1. Walk a set of top-level index pages (the 50 US state slugs plus a few
 *      country slugs) and collect every distinct listing URL visible in the
 *      HTML, following pagination via `?page=N`.
 *   2. For every unique listing URL, fetch the page and pull the JSON-LD
 *      LocalBusiness blob (name, phone, address, geo, etc.). Fall back to
 *      HTML selectors for any field missing from the JSON-LD.
 *   3. Write rows to output/martialarts-schools.csv with a checkpoint state
 *      file so the run is resumable.
 *
 * This script uses only the native https module and cheerio. No proxy or
 * browser is required because the site serves pre-rendered HTML without any
 * Cloudflare challenge.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const { URL } = require('url');
const cheerio = require('cheerio');

const BASE = 'https://www.martialartsschoolsdirectory.com';
const NICHE = 'martial-arts-school';
const SOURCE = 'martialartsschoolsdirectory.com';

const OUTPUT_CSV = path.join(__dirname, '..', 'output', 'martialarts-schools.csv');
const STATE_DIR = path.join(__dirname, '..', 'state');
const STATE_FILE = path.join(STATE_DIR, 'martialarts-directory.json');

const USER_AGENT =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const REQUEST_DELAY_MS = 1000;
const LISTING_CONCURRENCY = 5;
const MAX_PAGES_PER_INDEX = 25; // guard rail; real catalog is ~6 pages
const REQUEST_TIMEOUT_MS = 30000;

const US_STATE_SLUGS = [
  'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
  'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
  'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
  'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi',
  'missouri', 'montana', 'nebraska', 'nevada', 'new-hampshire', 'new-jersey',
  'new-mexico', 'new-york', 'north-carolina', 'north-dakota', 'ohio', 'oklahoma',
  'oregon', 'pennsylvania', 'rhode-island', 'south-carolina', 'south-dakota',
  'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
  'west-virginia', 'wisconsin', 'wyoming',
];

// A few national-level indexes exposed by the directory. These catch entries
// whose URL prefix is the country rather than a US state.
const COUNTRY_SLUGS = [
  'united-states', 'canada', 'united-kingdom', 'australia', 'new-zealand',
  'ireland', 'germany', 'france', 'italy', 'spain', 'mexico', 'brazil',
  'japan', 'south-africa', 'india', 'singapore', 'hong-kong',
];

// States sometimes appear as the first URL segment too (e.g. /florida/...),
// so we also walk the plain state slugs without country prefix. The index
// URLs below are the union of both lists.
const INDEX_SLUGS = Array.from(new Set([...US_STATE_SLUGS, ...COUNTRY_SLUGS]));

// ------------------------------ helpers ------------------------------

const US_STATE_ABBR = {
  alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA',
  colorado: 'CO', connecticut: 'CT', delaware: 'DE', florida: 'FL',
  georgia: 'GA', hawaii: 'HI', idaho: 'ID', illinois: 'IL', indiana: 'IN',
  iowa: 'IA', kansas: 'KS', kentucky: 'KY', louisiana: 'LA', maine: 'ME',
  maryland: 'MD', massachusetts: 'MA', michigan: 'MI', minnesota: 'MN',
  mississippi: 'MS', missouri: 'MO', montana: 'MT', nebraska: 'NE',
  nevada: 'NV', 'new-hampshire': 'NH', 'new-jersey': 'NJ', 'new-mexico': 'NM',
  'new-york': 'NY', 'north-carolina': 'NC', 'north-dakota': 'ND', ohio: 'OH',
  oklahoma: 'OK', oregon: 'OR', pennsylvania: 'PA', 'rhode-island': 'RI',
  'south-carolina': 'SC', 'south-dakota': 'SD', tennessee: 'TN', texas: 'TX',
  utah: 'UT', vermont: 'VT', virginia: 'VA', washington: 'WA',
  'west-virginia': 'WV', wisconsin: 'WI', wyoming: 'WY',
};

const COUNTRY_NAME_BY_SLUG = {
  'united-states': 'US', canada: 'CA', 'united-kingdom': 'GB',
  australia: 'AU', 'new-zealand': 'NZ', ireland: 'IE', germany: 'DE',
  france: 'FR', italy: 'IT', spain: 'ES', mexico: 'MX', brazil: 'BR',
  japan: 'JP', 'south-africa': 'ZA', india: 'IN', singapore: 'SG',
  'hong-kong': 'HK',
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function normalizePhone(raw) {
  if (!raw) return '';
  const digits = String(raw).replace(/[^0-9]/g, '');
  if (!digits) return '';
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return `+${digits}`;
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function httpGet(urlString, { attempt = 1, maxAttempts = 4 } = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(urlString);
    let settled = false;
    const done = (fn, arg) => {
      if (settled) return;
      settled = true;
      fn(arg);
    };
    // Hard ceiling so a single bad URL cannot hang the whole run.
    const hardTimer = setTimeout(() => {
      try { req.destroy(); } catch (_) {}
      done(reject, new Error(`hard timeout after ${REQUEST_TIMEOUT_MS + 5000}ms for ${urlString}`));
    }, REQUEST_TIMEOUT_MS + 5000);
    const req = https.request(
      {
        hostname: parsed.hostname,
        path: `${parsed.pathname}${parsed.search || ''}`,
        method: 'GET',
        headers: {
          'User-Agent': USER_AGENT,
          Accept:
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          Connection: 'keep-alive',
        },
        timeout: REQUEST_TIMEOUT_MS,
      },
      (res) => {
        // follow redirects manually so `?google=...` redirects still work
        if (
          res.statusCode &&
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          const next = new URL(res.headers.location, urlString).toString();
          res.resume();
          clearTimeout(hardTimer);
          done(resolve, httpGet(next, { attempt, maxAttempts }));
          return;
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf8');
          clearTimeout(hardTimer);
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 400) {
            done(resolve, { body, statusCode: res.statusCode });
          } else if (
            res.statusCode &&
            (res.statusCode === 429 || res.statusCode >= 500) &&
            attempt < maxAttempts
          ) {
            const backoff = 2000 * attempt;
            setTimeout(
              () => done(resolve, httpGet(urlString, { attempt: attempt + 1, maxAttempts })),
              backoff
            );
          } else {
            done(reject, new Error(`HTTP ${res.statusCode} for ${urlString}`));
          }
        });
      }
    );
    req.on('error', (err) => {
      clearTimeout(hardTimer);
      if (attempt < maxAttempts) {
        setTimeout(
          () => done(resolve, httpGet(urlString, { attempt: attempt + 1, maxAttempts })),
          2000 * attempt
        );
      } else {
        done(reject, err);
      }
    });
    req.on('timeout', () => {
      try { req.destroy(new Error(`timeout after ${REQUEST_TIMEOUT_MS}ms`)); } catch (_) {}
    });
    req.end();
  });
}

// -------------------------- listing URL discovery --------------------------

function extractListingUrls(html, slugPath) {
  const urls = new Set();
  const pattern = new RegExp(
    `/[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*/${slugPath}/[a-z0-9][a-z0-9-]*`,
    'g'
  );
  const matches = html.match(pattern) || [];
  for (const m of matches) {
    // filter out trailing action suffixes like /connect, /reviews
    const cleaned = m.replace(/\/(connect|reviews|review|share)$/i, '');
    // expect the shape /a/b/martial-arts-schools/c -> 5 segments once split on '/'
    if (cleaned.split('/').length === 5) urls.add(cleaned.toLowerCase());
  }
  return Array.from(urls);
}

async function collectListingUrls() {
  const discovered = new Set();
  for (const slug of INDEX_SLUGS) {
    for (let page = 1; page <= MAX_PAGES_PER_INDEX; page++) {
      const url =
        page === 1
          ? `${BASE}/${slug}`
          : `${BASE}/${slug}?page=${page}`;
      let body;
      try {
        const res = await httpGet(url);
        body = res.body;
      } catch (err) {
        console.error(`[index] ${url} -> ${err.message}`);
        break;
      }
      await sleep(REQUEST_DELAY_MS);
      const urls = extractListingUrls(body, 'martial-arts-schools');
      const before = discovered.size;
      for (const u of urls) discovered.add(u);
      const added = discovered.size - before;
      process.stdout.write(
        `[index] ${slug} p${page}: ${urls.length} listings (+${added} new, total ${discovered.size})\n`
      );
      // Stop paging when the page yields no listings or no new ones.
      if (urls.length === 0) break;
    }
  }
  return Array.from(discovered).map((p) => `${BASE}${p}`);
}

// -------------------------- listing parser --------------------------

function parseFirstJsonLdLocalBusiness(html) {
  const $ = cheerio.load(html);
  const scripts = $('script[type="application/ld+json"]').toArray();
  for (const s of scripts) {
    const raw = $(s).contents().text();
    if (!raw || !raw.trim()) continue;
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      continue;
    }
    const nodes = [];
    if (Array.isArray(parsed)) {
      nodes.push(...parsed);
    } else if (parsed['@graph'] && Array.isArray(parsed['@graph'])) {
      nodes.push(...parsed['@graph']);
    } else {
      nodes.push(parsed);
    }
    for (const node of nodes) {
      if (!node || typeof node !== 'object') continue;
      const type = node['@type'];
      const types = Array.isArray(type) ? type : [type];
      const isBusiness = types.some((t) =>
        /LocalBusiness|Organization|SportsActivityLocation|HealthClub|SportsClub|MartialArtsSchool|DanceStudio|EducationalOrganization|ExerciseGym/i.test(
          String(t || '')
        )
      );
      if (isBusiness && (node.name || node.telephone || node.address)) {
        return node;
      }
    }
  }
  return null;
}

function pickCountry($, jsonLd, indexSlug) {
  const addressCountry = jsonLd?.address?.addressCountry;
  if (typeof addressCountry === 'string' && addressCountry.trim()) {
    return addressCountry.trim().toUpperCase();
  }
  if (addressCountry && typeof addressCountry === 'object' && addressCountry.name) {
    return String(addressCountry.name).trim();
  }
  if (COUNTRY_NAME_BY_SLUG[indexSlug]) return COUNTRY_NAME_BY_SLUG[indexSlug];
  if (US_STATE_ABBR[indexSlug]) return 'US';
  return '';
}

function pickState(jsonLd, indexSlug, countryHint) {
  const region = jsonLd?.address?.addressRegion;
  if (typeof region === 'string' && region.trim()) return region.trim();
  if (region && typeof region === 'object' && region.name) {
    return String(region.name).trim();
  }
  if (countryHint === 'US' && US_STATE_ABBR[indexSlug]) {
    return US_STATE_ABBR[indexSlug];
  }
  return '';
}

function pickAddress(jsonLd) {
  const addr = jsonLd?.address;
  if (!addr || typeof addr !== 'object') return '';
  const parts = [];
  if (addr.streetAddress) parts.push(String(addr.streetAddress).trim());
  if (addr.addressLocality) parts.push(String(addr.addressLocality).trim());
  if (addr.addressRegion) parts.push(String(addr.addressRegion).trim());
  if (addr.postalCode) parts.push(String(addr.postalCode).trim());
  return parts.filter(Boolean).join(', ');
}

function firstEmailFromText(text) {
  if (!text) return '';
  const match = String(text).match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
  if (!match) return '';
  const email = match[0];
  if (/martialartsschoolsdirectory|dancestudiodirectory|optimizecdn|yoursite\.com|example\./i.test(email)) {
    return '';
  }
  return email.toLowerCase();
}

function parseListing(profileUrl, html, indexSlugHint) {
  const $ = cheerio.load(html);
  const jsonLd = parseFirstJsonLdLocalBusiness(html);

  let name = jsonLd?.name ? String(jsonLd.name).trim() : '';
  if (!name) {
    name = $('h1').first().text().trim();
  }

  let phone = '';
  if (jsonLd?.telephone) phone = normalizePhone(jsonLd.telephone);
  if (!phone) {
    const tel = $('a[href^="tel:"]').first().attr('href');
    if (tel) phone = normalizePhone(tel.replace(/^tel:/i, ''));
  }

  let website = '';
  const weblink = $('a.weblink[itemprop="url"]').first().attr('href');
  if (weblink && !/optimizecdn|martialartsschoolsdirectory|dancestudiodirectory/i.test(weblink)) {
    website = weblink.trim();
  }
  if (!website && jsonLd?.sameAs) {
    const sameAs = Array.isArray(jsonLd.sameAs) ? jsonLd.sameAs : [jsonLd.sameAs];
    for (const s of sameAs) {
      if (typeof s === 'string' && /^https?:/i.test(s) && !/facebook|instagram|twitter|youtube|tiktok|linkedin/i.test(s)) {
        website = s.trim();
        break;
      }
    }
  }

  const email = firstEmailFromText(jsonLd?.description) || firstEmailFromText($('body').text());

  const address = pickAddress(jsonLd);
  const countrySlug = (profileUrl.match(new RegExp(`${BASE.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&')}/([a-z0-9-]+)/`)) || [])[1] || indexSlugHint || '';
  const country = pickCountry($, jsonLd, countrySlug);
  const state = pickState(jsonLd, countrySlug, country);

  let city = '';
  if (jsonLd?.address?.addressLocality) {
    city = String(jsonLd.address.addressLocality).trim();
  } else {
    const segs = profileUrl.replace(`${BASE}/`, '').split('/');
    if (segs.length >= 2) {
      city = segs[1].replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
    }
  }

  return {
    name,
    phone,
    email,
    website,
    address,
    city,
    state,
    country,
    niche: NICHE,
    source: SOURCE,
    profile_url: profileUrl,
  };
}

// ---------------------------- CSV writer ----------------------------

const COLUMNS = [
  'name', 'phone', 'email', 'website', 'address', 'city', 'state', 'country',
  'niche', 'source', 'profile_url',
];

function appendRowToCsv(row, path) {
  const headerNeeded = !fs.existsSync(path);
  const line = COLUMNS.map((k) => csvEscape(row[k] || '')).join(',') + '\n';
  if (headerNeeded) {
    fs.writeFileSync(path, COLUMNS.join(',') + '\n');
  }
  fs.appendFileSync(path, line);
}

// ---------------------------- state helpers ----------------------------

function loadState() {
  if (!fs.existsSync(STATE_FILE)) {
    return { urls: [], done: {} };
  }
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch (e) {
    return { urls: [], done: {} };
  }
}

function saveState(state) {
  ensureDir(path.dirname(STATE_FILE));
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ---------------------------- concurrency ----------------------------

async function runPool(items, worker, concurrency) {
  const queue = items.slice();
  let active = 0;
  let done = 0;
  const total = items.length;
  return new Promise((resolve) => {
    const pump = () => {
      while (active < concurrency && queue.length) {
        const item = queue.shift();
        active++;
        Promise.resolve()
          .then(() => worker(item))
          .catch((err) => ({ error: err }))
          .then(() => {
            active--;
            done++;
            if (done % 10 === 0) {
              process.stdout.write(`  progress: ${done}/${total}\n`);
            }
            if (!queue.length && active === 0) resolve();
            else pump();
          });
      }
    };
    if (!items.length) resolve();
    else pump();
  });
}

// ---------------------------- driver ----------------------------

async function main() {
  const args = process.argv.slice(2);
  const limitStatesArg = args.find((a) => a.startsWith('--limit-states='));
  const limitStates = limitStatesArg ? Number(limitStatesArg.split('=')[1]) : null;
  const doRefreshIndex = args.includes('--refresh-index');
  const testMode = args.includes('--test');

  ensureDir(path.dirname(OUTPUT_CSV));
  ensureDir(STATE_DIR);

  let state = loadState();

  if (testMode) {
    // override index list for quick tests
    INDEX_SLUGS.length = 0;
    INDEX_SLUGS.push('california', 'new-york', 'texas', 'florida', 'united-states');
  } else if (limitStates) {
    INDEX_SLUGS.length = Math.min(INDEX_SLUGS.length, limitStates);
  }

  if (!state.urls.length || doRefreshIndex) {
    console.log(`[run] collecting listing URLs from ${INDEX_SLUGS.length} indexes`);
    const urls = await collectListingUrls();
    state.urls = urls;
    saveState(state);
  }
  console.log(`[run] ${state.urls.length} unique listing URLs queued`);

  const pending = state.urls.filter((u) => !state.done[u]);
  console.log(`[run] ${pending.length} pending (${Object.keys(state.done).length} already done)`);

  let success = 0;
  let failed = 0;

  await runPool(pending, async (url) => {
    try {
      const { body } = await httpGet(url);
      await sleep(REQUEST_DELAY_MS);
      const slug = url.replace(`${BASE}/`, '').split('/')[0];
      const row = parseListing(url, body, slug);
      if (row.name) {
        appendRowToCsv(row, OUTPUT_CSV);
        success++;
      } else {
        failed++;
      }
      state.done[url] = { ok: Boolean(row.name), ts: Date.now() };
      if ((success + failed) % 20 === 0) saveState(state);
    } catch (err) {
      failed++;
      state.done[url] = { ok: false, err: err.message, ts: Date.now() };
      console.error(`[fetch] ${url} -> ${err.message}`);
    }
  }, LISTING_CONCURRENCY);

  saveState(state);

  // Final pass: dedup CSV by profile_url to guard against rows written before
  // an interrupted run was resumed.
  if (fs.existsSync(OUTPUT_CSV)) {
    const raw = fs.readFileSync(OUTPUT_CSV, 'utf8');
    const lines = raw.split(/\r?\n/).filter(Boolean);
    if (lines.length > 1) {
      const header = lines[0];
      const seen = new Set();
      const kept = [header];
      for (let i = 1; i < lines.length; i++) {
        const row = lines[i];
        const profileIdx = row.lastIndexOf(',');
        const profile = row.slice(profileIdx + 1).trim();
        if (profile && !seen.has(profile)) {
          seen.add(profile);
          kept.push(row);
        }
      }
      fs.writeFileSync(OUTPUT_CSV, kept.join('\n') + '\n');
    }
  }

  console.log(`\n[run] done: ${success} rows written, ${failed} failed`);
  console.log(`[run] csv: ${OUTPUT_CSV}`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
