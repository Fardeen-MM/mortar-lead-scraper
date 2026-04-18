#!/usr/bin/env node
/**
 * Scrape English UK Member Centre Directory (accredited ESL schools).
 *
 * Source: https://www.englishuk.com/member-directory
 * Method: Server-rendered HTML pages (20/page), same h3 + p structure as agents directory
 *
 * Expected: ~278 accredited English language schools (British Council accredited)
 *
 * Features:
 *   - Cheerio HTML parsing (no Puppeteer needed)
 *   - Delay between requests (polite)
 *   - Incremental CSV saves
 *   - Test mode (--test): only 2 listing pages
 *
 * Usage:
 *   node scripts/scrape-english-uk-schools.js
 *   node scripts/scrape-english-uk-schools.js --test
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE      = !!args.test;
const SKIP_DETAILS   = !!args['skip-details'];
const DELAY_MS       = parseInt(args.delay) || 2000;
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const OUTPUT_FILE    = path.join(OUTPUT_DIR, 'uk-esl-schools-english-uk.csv');
const SAVE_EVERY     = 50;

const BASE_URL = 'https://www.englishuk.com/member-directory';

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'address', 'description',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  listingPages: 0,
  totalFromListing: 0,
  detailsFetched: 0,
  detailErrors: 0,
  withEmail: 0,
  withPhone: 0,
  withWebsite: 0,
  withContact: 0,
  startTime: Date.now(),
};

// ── Helpers ─────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

function escapeCSV(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function elapsed() {
  const s = Math.floor((Date.now() - stats.startTime) / 1000);
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'https://' + u;
    return new URL(u).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function httpGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        let loc = res.headers.location;
        if (loc.startsWith('/')) {
          const parsed = new URL(url);
          loc = parsed.protocol + '//' + parsed.host + loc;
        }
        return httpGet(loc, maxRedirects - 1).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ── Parse listing page ──────────────────────────────────────────────────
function parseListingPage(html) {
  const $ = cheerio.load(html);
  const schools = [];

  $('h3').each((_, h3) => {
    const $h3 = $(h3);
    const $link = $h3.find('a');
    if (!$link.length) return;

    const name = $link.text().trim();
    const href = $link.attr('href') || '';

    const idMatch = href.match(/[?&]id=(\d+)/);
    const schoolId = idMatch ? idMatch[1] : '';
    if (!schoolId) return;

    const profileUrl = `${BASE_URL}?id=${schoolId}`;

    const $p = $h3.next('p');
    if (!$p.length) return;

    const pHtml = $p.html() || '';

    const addressMatch = pHtml.match(/^([^<]+)/);
    const address = addressMatch ? addressMatch[1].trim() : '';

    const telMatch = pHtml.match(/Tel\s+([^<]+)/);
    const phone = telMatch ? telMatch[1].trim() : '';

    const websiteMatch = pHtml.match(/href="(https?:\/\/[^"]+)"[^>]*target="_blank"/);
    const website = websiteMatch ? websiteMatch[1].trim() : '';

    const emailMatch = pHtml.match(/mailto:([^"]+)"/);
    const email = emailMatch ? emailMatch[1].trim() : '';

    // Parse city from UK address: "...Road, City, County, POSTCODE"
    const addressParts = address.split(',').map(s => s.trim()).filter(Boolean);
    let city = '', state = '';
    if (addressParts.length >= 3) {
      // Typical UK: street, city, county, postcode
      // Postcode is last part and matches UK pattern
      const lastPart = addressParts[addressParts.length - 1];
      const isPostcode = /^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i.test(lastPart);
      if (isPostcode && addressParts.length >= 4) {
        city = addressParts[addressParts.length - 3] || '';
        state = addressParts[addressParts.length - 2] || '';
      } else if (isPostcode) {
        city = addressParts[addressParts.length - 2] || '';
      } else {
        city = addressParts[addressParts.length - 2] || '';
      }
    } else if (addressParts.length >= 2) {
      city = addressParts[0] || '';
    }

    schools.push({
      firm_name: name,
      address,
      phone,
      website,
      email,
      country: 'United Kingdom',
      city,
      state,
      domain: extractDomain(website),
      profile_url: profileUrl,
      school_id: schoolId,
    });
  });

  let totalCount = 0;
  const displayMatch = html.match(/Displaying\s+\d+\s*-\s*\d+\s+of\s+(\d+)/);
  if (displayMatch) totalCount = parseInt(displayMatch[1]);

  const maxPage = totalCount > 0 ? Math.ceil(totalCount / 20) - 1 : 0;

  return { schools, totalCount, maxPage };
}

// ── Parse detail page ───────────────────────────────────────────────────
function parseDetailPage(html) {
  const $ = cheerio.load(html);
  const result = {};

  const contentHtml = $('.content-list').html() || $('body').html() || '';
  const text = $('body').text();

  // Contact name (if present)
  const contactMatch = text.match(/Contact\s*\n?\s*([^\n]+)/);
  if (contactMatch) {
    const contactName = contactMatch[1].trim();
    if (contactName && !contactName.match(/^(Address|Country|Telephone|Website|Email|Description)/)) {
      result.contact_name = contactName;
      const nameParts = contactName.split(/\s+/);
      if (nameParts.length >= 2) {
        result.first_name = nameParts[0];
        result.last_name = nameParts.slice(1).join(' ');
      }
    }
  }

  // Description
  const descMatch = text.match(/Description of centre\s*(?:-->)?\s*\n?\s*([^\n]+(?:\n[^\n]+)*?)(?:\s*<\s*back|$)/i);
  if (descMatch) {
    result.description = descMatch[1].trim().substring(0, 500);
  }

  return result;
}

// ── Save CSV ────────────────────────────────────────────────────────────
function saveCSV(schools) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const header = CSV_COLUMNS.join(',');
  const rows = schools.map(s =>
    CSV_COLUMNS.map(col => escapeCSV(s[col] || '')).join(',')
  );

  fs.writeFileSync(OUTPUT_FILE, header + '\n' + rows.join('\n') + '\n');
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== English UK Member Centre Directory Scraper (ESL Schools) ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  if (SKIP_DETAILS) console.log('Skipping detail page fetches');
  console.log('');

  // ── Phase 1: Listing pages ──────────────────────────────────────────
  console.log('--- Phase 1: Fetching listing pages ---');

  let allSchools = [];
  let page = 0;
  let totalCount = 0;
  let maxPage = 0;

  while (true) {
    const url = `${BASE_URL}?letter_index=&keywords=&region_hq=0&page=${page}`;
    console.log(`  [${elapsed()}] Fetching page ${page + 1}...`);

    try {
      const html = await httpGet(url);
      const result = parseListingPage(html);

      if (page === 0) {
        totalCount = result.totalCount;
        maxPage = result.maxPage;
        console.log(`  Total schools: ${totalCount}, Pages: ${maxPage + 1}`);
      }

      if (result.schools.length === 0) {
        console.log(`  No schools on page ${page + 1}, stopping.`);
        break;
      }

      allSchools.push(...result.schools);
      stats.listingPages++;
      console.log(`  Got ${result.schools.length} schools (total: ${allSchools.length})`);

      if (page >= maxPage) {
        console.log('  Reached last page.');
        break;
      }

      if (TEST_MODE && page >= 1) {
        console.log('  TEST MODE: stopping after 2 pages');
        break;
      }

      page++;
      await sleep(DELAY_MS);
    } catch (err) {
      console.error(`  ERROR on page ${page}: ${err.message}`);
      if (page > 0) break;
      throw err;
    }
  }

  stats.totalFromListing = allSchools.length;
  console.log(`\nPhase 1 complete: ${allSchools.length} schools from ${stats.listingPages} pages`);

  // ── Phase 2: Detail pages (optional) ────────────────────────────────
  if (!SKIP_DETAILS) {
    console.log('\n--- Phase 2: Fetching detail pages for contact names ---');
    const limit = TEST_MODE ? 5 : allSchools.length;

    for (let i = 0; i < Math.min(limit, allSchools.length); i++) {
      const school = allSchools[i];
      const detailUrl = `${BASE_URL}?id=${school.school_id}`;

      try {
        const html = await httpGet(detailUrl);
        const detail = parseDetailPage(html);

        if (detail.first_name) school.first_name = detail.first_name;
        if (detail.last_name) school.last_name = detail.last_name;
        if (detail.description) school.description = detail.description;
        if (detail.contact_name) stats.withContact++;

        stats.detailsFetched++;

        if ((i + 1) % 10 === 0 || i === 0) {
          console.log(`  [${elapsed()}] ${i + 1}/${Math.min(limit, allSchools.length)} details (${stats.withContact} contacts)`);
        }

        if ((i + 1) % SAVE_EVERY === 0) {
          saveCSV(allSchools);
          console.log(`  Saved ${allSchools.length} schools to CSV`);
        }

        await sleep(DELAY_MS);
      } catch (err) {
        stats.detailErrors++;
        if (stats.detailErrors <= 5) console.error(`  ERROR ${school.firm_name}: ${err.message}`);
      }
    }

    console.log(`\nPhase 2 complete: ${stats.detailsFetched} details, ${stats.detailErrors} errors`);
  }

  // ── Finalize ────────────────────────────────────────────────────────
  for (const s of allSchools) {
    s.niche = 'ESL Language School';
    s.source = 'english-uk-schools';
    if (s.email) stats.withEmail++;
    if (s.phone) stats.withPhone++;
    if (s.website) stats.withWebsite++;
  }

  saveCSV(allSchools);

  console.log('\n=== RESULTS ===');
  console.log(`Total schools: ${allSchools.length}`);
  console.log(`With email:    ${stats.withEmail} (${(stats.withEmail / allSchools.length * 100).toFixed(1)}%)`);
  console.log(`With phone:    ${stats.withPhone} (${(stats.withPhone / allSchools.length * 100).toFixed(1)}%)`);
  console.log(`With website:  ${stats.withWebsite} (${(stats.withWebsite / allSchools.length * 100).toFixed(1)}%)`);
  console.log(`With contact:  ${stats.withContact}`);
  console.log(`Time: ${elapsed()}`);
  console.log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
