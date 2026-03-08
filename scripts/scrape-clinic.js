#!/usr/bin/env node
/**
 * Immigration Legal Services Scraper
 *
 * Source: ImmigrationLawHelp.org (Immigration Advocates Network / Pro Bono Net)
 *   — Directory of nonprofit immigration legal service providers in the USA
 *
 * CLINIC (cliniclegal.org) is behind Cloudflare protection, so we use
 * ImmigrationLawHelp.org instead, which is a larger and more comprehensive
 * directory of immigration legal service providers.
 *
 * Strategy:
 *   1. Iterate all 51 jurisdictions (50 states + DC)
 *   2. For each state, paginate listing pages (20 orgs/page)
 *   3. For each org, fetch detail page for email, contact name, phone, website
 *   4. Output CSV
 *
 * Usage:
 *   node scripts/scrape-clinic.js
 *   node scripts/scrape-clinic.js --state CA          # Single state
 *   node scripts/scrape-clinic.js --state CA,NY,TX     # Multiple states
 *   node scripts/scrape-clinic.js --skip-details        # Listing data only (fast)
 *   node scripts/scrape-clinic.js --max-per-state 20    # Limit per state
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ─── cheerio ─────────────────────────────────────────────────
let cheerio;
try {
  cheerio = require('cheerio');
} catch (e) {
  console.error('cheerio not found. Install: npm install cheerio');
  process.exit(1);
}

// ─── CLI Args ────────────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const STATE_FILTER = getArg('state') ? getArg('state').split(',').map(s => s.trim().toUpperCase()) : null;
const SKIP_DETAILS = hasFlag('skip-details');
const MAX_PER_STATE = parseInt(getArg('max-per-state') || '0') || 0;
const OUTPUT_PATH = getArg('output') || path.join(__dirname, '..', 'output', 'us-immigration-clinic-affiliates.csv');

// ─── Constants ───────────────────────────────────────────────
const BASE_URL = 'https://www.immigrationlawhelp.org';
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const DELAY_MIN = 2000;  // 2s min delay
const DELAY_MAX = 3500;  // 3.5s max delay
const DETAIL_DELAY_MIN = 1500;
const DETAIL_DELAY_MAX = 2500;

const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL',
  'GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
  'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
  'NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
  'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
];

// ─── HTTP helper ─────────────────────────────────────────────
function httpGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: 30000,
    }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && maxRedirects > 0) {
        let loc = res.headers.location;
        if (loc.startsWith('/')) loc = new URL(loc, url).href;
        return resolve(httpGet(loc, maxRedirects - 1));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString()));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay(min, max) { return sleep(min + Math.random() * (max - min)); }

// ─── Parse listing page ─────────────────────────────────────
function parseListingPage($) {
  const orgs = [];
  $('div.directory-organization-summary').each((i, el) => {
    const $el = $(el);
    const $link = $el.find('h4 a');
    const name = $link.text().trim();
    const profilePath = $link.attr('href') || '';
    const profileUrl = profilePath ? `${BASE_URL}${profilePath}` : '';

    // Location from listing
    let location = '';
    $el.find('td.label').each((j, td) => {
      if ($(td).text().trim() === 'Location:') {
        location = $(td).next('td').text().trim();
      }
    });

    // Contact from listing (phone + website)
    let contactText = '';
    $el.find('td.label').each((j, td) => {
      if ($(td).text().trim() === 'Contact:') {
        contactText = $(td).next('td').text().trim();
      }
    });

    // Services from listing
    let services = '';
    $el.find('td.label').each((j, td) => {
      if ($(td).text().trim() === 'Areas of legal assistance:') {
        services = $(td).next('td').text().trim();
      }
    });

    // Website from contact section link
    let website = '';
    $el.find('a.url.external').each((j, a) => {
      website = $(a).attr('href') || '';
    });

    // Extract phone from contact text (strip website URL)
    let phone = '';
    const phoneMatch = contactText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    if (phoneMatch) phone = phoneMatch[0];

    // Parse location into parts
    const locParts = parseLocation(location);

    orgs.push({
      firm_name: name,
      profile_url: profileUrl,
      profile_path: profilePath,
      phone,
      website,
      services,
      ...locParts,
    });
  });
  return orgs;
}

// ─── Parse location string ──────────────────────────────────
function parseLocation(loc) {
  if (!loc) return { address: '', city: '', state: '', zip: '' };

  // Format: "123 Street, Suite X, City, ST 12345"
  // or: "123 Street, City, ST 12345"
  const parts = loc.split(',').map(s => s.trim());
  const result = { address: '', city: '', state: '', zip: '' };

  if (parts.length === 0) return result;

  // Last part should contain state + zip
  const lastPart = parts[parts.length - 1];
  const stateZipMatch = lastPart.match(/([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
  if (stateZipMatch) {
    result.state = stateZipMatch[1];
    result.zip = stateZipMatch[2];
  }

  // Second-to-last part is city (when state+zip extracted from last part)
  if (parts.length >= 3 && stateZipMatch) {
    result.city = parts[parts.length - 2];
    result.address = parts.slice(0, parts.length - 2).join(', ');
  } else if (parts.length >= 2) {
    result.city = parts[parts.length - 2];
    result.address = parts.slice(0, parts.length - 2).join(', ');
  }

  return result;
}

// ─── Extract total pages from listing ────────────────────────
function extractTotalPages($) {
  let total = 1;
  $('span').each((i, el) => {
    const text = $(el).text().trim();
    const m = text.match(/Page\s+\d+\s+of\s+(\d+)/i);
    if (m) total = parseInt(m[1]);
  });
  return total;
}

// ─── Parse detail page ──────────────────────────────────────
function parseDetailPage($) {
  const detail = {
    first_name: '',
    last_name: '',
    email: '',
    phone: '',
    website: '',
    address: '',
    city: '',
    state: '',
    zip: '',
    title: '',
    description: '',
  };

  // Structured address
  const street = $('div.street-address').first().text().trim();
  const extended = $('div.extended-address').first().text().trim();
  const locality = $('span.locality').first().text().trim();
  const region = $('abbr.region').first().text().trim();
  const postalCode = $('span.postal-code').first().text().trim();

  detail.address = [street, extended].filter(Boolean).join(', ');
  detail.city = locality;
  detail.state = region;
  detail.zip = postalCode;

  // Website
  const websiteLink = $('a.url.external.text-break').first().attr('href');
  if (websiteLink) detail.website = websiteLink;

  // Phone from main contact section
  $('strong').each((i, el) => {
    const label = $(el).text().trim();
    if (label === 'Phone:') {
      // Phone is the next text node after the <br/>
      const parent = $(el).parent();
      const phoneText = parent.text().replace('Phone:', '').trim();
      const phoneMatch = phoneText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
      if (phoneMatch && !detail.phone) detail.phone = phoneMatch[0];
    }
  });

  // Contact person (first one found)
  const $contactSection = $('div.fn.n').first();
  if ($contactSection.length) {
    detail.first_name = $contactSection.find('span.given-name').text().trim();
    detail.last_name = $contactSection.find('span.family-name').text().trim();
  }

  // Contact person's email
  const emailLink = $('a.email[href^="mailto:"]').first();
  if (emailLink.length) {
    detail.email = emailLink.attr('href').replace('mailto:', '').trim();
    // Decode HTML entities
    detail.email = detail.email.replace(/&#64;/g, '@').replace(/&#46;/g, '.');
  }

  // Contact person's title/role (from the label in contacts table)
  $('td.label').each((i, td) => {
    const text = $(td).text().trim();
    // Typical labels: "Executive Director", "Immigration Program Manager", "Volunteer Coordinator"
    if ($contactSection.length && $(td).closest('tr').find('.fn').length) {
      detail.title = text;
    }
  });

  // Description
  const desc = $('#organization-detail-description').text().trim();
  if (desc) detail.description = desc.substring(0, 500);

  return detail;
}

// ─── Extract domain from URL ─────────────────────────────────
function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `http://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

// ─── Normalize phone ─────────────────────────────────────────
function normalizePhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits[0] === '1') return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
  return phone;
}

// ─── CSV escaping ────────────────────────────────────────────
function csvEscape(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ─── Main ────────────────────────────────────────────────────
async function main() {
  const states = STATE_FILTER || ALL_STATES;
  const allLeads = [];
  let detailsFetched = 0;
  let detailErrors = 0;
  let duplicateSkips = 0;
  const seenOrgs = new Set(); // Dedup by firm_name + state

  console.log(`\n=== Immigration Legal Services Scraper ===`);
  console.log(`Source: ImmigrationLawHelp.org`);
  console.log(`States: ${states.length}`);
  console.log(`Detail pages: ${SKIP_DETAILS ? 'SKIP' : 'YES'}`);
  console.log(`Max per state: ${MAX_PER_STATE || 'unlimited'}`);
  console.log(`Output: ${OUTPUT_PATH}\n`);

  for (let si = 0; si < states.length; si++) {
    const state = states[si];
    console.log(`\n[${si + 1}/${states.length}] Scraping ${state}...`);

    try {
      // Fetch first listing page
      const firstUrl = `${BASE_URL}/search?state=${state}`;
      const html = await httpGet(firstUrl);
      const $ = cheerio.load(html);

      const totalPages = extractTotalPages($);
      let stateOrgs = parseListingPage($);
      console.log(`  Page 1/${totalPages} — ${stateOrgs.length} orgs`);

      // Paginate
      for (let page = 2; page <= totalPages; page++) {
        if (MAX_PER_STATE && stateOrgs.length >= MAX_PER_STATE) break;

        await randomDelay(DELAY_MIN, DELAY_MAX);
        const pageUrl = `${BASE_URL}/search?&state=${state}&national=0&county=&legalArea=&legalService=&nonLegalService=&interestArea=&population=&legalNetwork=&language=&detentionFacility=&text=&zip=&interpreting=0&map=0&page=${page}`;
        try {
          const pageHtml = await httpGet(pageUrl);
          const $page = cheerio.load(pageHtml);
          const pageOrgs = parseListingPage($page);
          stateOrgs = stateOrgs.concat(pageOrgs);
          console.log(`  Page ${page}/${totalPages} — ${pageOrgs.length} orgs (total: ${stateOrgs.length})`);
        } catch (err) {
          console.log(`  Page ${page}/${totalPages} — ERROR: ${err.message}`);
        }
      }

      if (MAX_PER_STATE) stateOrgs = stateOrgs.slice(0, MAX_PER_STATE);

      // Fetch detail pages for each org
      for (let oi = 0; oi < stateOrgs.length; oi++) {
        const org = stateOrgs[oi];

        // Dedup by firm_name + state
        const dedupKey = `${(org.firm_name || '').toLowerCase()}|${state}`;
        if (seenOrgs.has(dedupKey)) {
          duplicateSkips++;
          continue;
        }
        seenOrgs.add(dedupKey);

        if (!SKIP_DETAILS && org.profile_path) {
          await randomDelay(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX);
          try {
            const detailUrl = `${BASE_URL}${org.profile_path}`;
            const detailHtml = await httpGet(detailUrl);
            const $detail = cheerio.load(detailHtml);
            const detail = parseDetailPage($detail);
            detailsFetched++;

            // Merge detail data (detail page takes precedence for structured fields)
            if (detail.first_name) org.first_name = detail.first_name;
            if (detail.last_name) org.last_name = detail.last_name;
            if (detail.email) org.email = detail.email;
            if (detail.phone) org.phone = detail.phone;
            if (detail.website) org.website = detail.website;
            if (detail.title) org.title = detail.title;
            if (detail.city) org.city = detail.city;
            if (detail.state) org.state = detail.state;
            if (detail.address) org.address = detail.address;
            if (detail.zip) org.zip = detail.zip;

            if ((oi + 1) % 10 === 0 || oi === stateOrgs.length - 1) {
              console.log(`  Details: ${oi + 1}/${stateOrgs.length} (${detailsFetched} fetched, ${detailErrors} errors)`);
            }
          } catch (err) {
            detailErrors++;
            if ((oi + 1) % 10 === 0) {
              console.log(`  Details: ${oi + 1}/${stateOrgs.length} — last error: ${err.message}`);
            }
          }
        }

        // Ensure state is set
        if (!org.state) org.state = state;

        // Build the lead row
        allLeads.push({
          first_name: org.first_name || '',
          last_name: org.last_name || '',
          firm_name: org.firm_name || '',
          title: org.title || '',
          email: org.email || '',
          phone: normalizePhone(org.phone || ''),
          website: org.website || '',
          domain: extractDomain(org.website || ''),
          city: org.city || '',
          state: org.state || state,
          country: 'US',
          niche: 'Immigration Legal Services',
          source: 'immigrationlawhelp',
          profile_url: org.profile_url || '',
        });
      }

      console.log(`  ${state} complete: ${stateOrgs.length} orgs`);

    } catch (err) {
      console.log(`  ERROR scraping ${state}: ${err.message}`);
    }
  }

  // ─── Write CSV ───────────────────────────────────────────────
  const columns = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
    'website', 'domain', 'city', 'state', 'country', 'niche', 'source', 'profile_url'
  ];

  // Ensure output directory exists
  const outDir = path.dirname(OUTPUT_PATH);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const csvLines = [columns.join(',')];
  for (const lead of allLeads) {
    csvLines.push(columns.map(c => csvEscape(lead[c])).join(','));
  }

  fs.writeFileSync(OUTPUT_PATH, csvLines.join('\n'), 'utf-8');

  // ─── Summary ─────────────────────────────────────────────────
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withContact = allLeads.filter(l => l.first_name || l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;

  console.log(`\n${'='.repeat(50)}`);
  console.log(`SCRAPE COMPLETE`);
  console.log(`${'='.repeat(50)}`);
  console.log(`Total organizations: ${allLeads.length}`);
  console.log(`Unique states:       ${uniqueStates}`);
  console.log(`With contact name:   ${withContact}`);
  console.log(`With email:          ${withEmail}`);
  console.log(`With phone:          ${withPhone}`);
  console.log(`With website:        ${withWebsite}`);
  console.log(`Duplicates skipped:  ${duplicateSkips}`);
  console.log(`Detail pages fetched:${detailsFetched}`);
  console.log(`Detail page errors:  ${detailErrors}`);
  console.log(`Output: ${OUTPUT_PATH}`);
  console.log(`${'='.repeat(50)}\n`);

  // State breakdown
  const stateCounts = {};
  for (const l of allLeads) {
    stateCounts[l.state] = (stateCounts[l.state] || 0) + 1;
  }
  const sorted = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
  console.log('Top states:');
  for (const [st, ct] of sorted.slice(0, 15)) {
    console.log(`  ${st}: ${ct}`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
