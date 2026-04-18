#!/usr/bin/env node
/**
 * HG.org Wayback Machine Immigration Lawyer Scraper
 *
 * HG.org was a massive lawyer directory that shut down in March 2026.
 * This scraper extracts immigration attorney data from archived pages
 * on the Wayback Machine (web.archive.org).
 *
 * Strategy:
 *   1. CDX API → discover all archived /law-firms/immigration/ listing pages
 *   2. Fetch listing pages → extract attorney profile URLs + inline data
 *   3. Fetch individual profile pages → extract structured schema.org data
 *   4. Deduplicate by firm ID and output to CSV
 *
 * Data available per profile:
 *   - Firm name, phone, fax, street address, city, state, zip, country
 *   - Geo coordinates, practice areas, description
 *   - No direct emails (HG.org used a contact form intermediary)
 *
 * Usage:
 *   node scripts/scrape-hg-wayback.js [options]
 *   --listings-only    Only fetch listing pages (skip profile pages)
 *   --profiles-only    Only fetch profiles (requires prior listing run)
 *   --max-listings N   Max listing pages to fetch (default: all)
 *   --max-profiles N   Max profile pages to fetch (default: all)
 *   --resume           Resume from where we left off
 *   --country FILTER   Filter listings by country (e.g. "usa", "united-kingdom")
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// === Configuration ===
const CDX_API = 'https://web.archive.org/cdx/search/cdx';
const WAYBACK_BASE = 'https://web.archive.org/web';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const CSV_FILE = path.join(OUTPUT_DIR, 'hg-org-immigration-wayback.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'hg-org-wayback-progress.json');
const PROFILES_CACHE = path.join(OUTPUT_DIR, 'hg-org-wayback-profiles.json');
const LISTING_URLS_CACHE = path.join(OUTPUT_DIR, 'hg-org-wayback-listing-urls.json');

const DELAY_MS = 1500;        // 1.5s between requests (be nice to archive.org)
const CDX_PAGE_SIZE = 5000;   // CDX API page size
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000;     // 5s retry delay
const REQUEST_TIMEOUT = 30000; // 30s timeout

const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source,address,zip,fax,latitude,longitude,practice_areas,description,hg_id,website\n';

// === Helpers ===

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      timeout: REQUEST_TIMEOUT,
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (!redirectUrl.startsWith('http')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        res.resume();
        return resolve(httpGet(redirectUrl, retries));
      }

      if (res.statusCode !== 200) {
        res.resume();
        if (retries > 0 && (res.statusCode === 429 || res.statusCode >= 500)) {
          return sleep(RETRY_DELAY).then(() => resolve(httpGet(url, retries - 1)));
        }
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }

      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        sleep(RETRY_DELAY).then(() => resolve(httpGet(url, retries - 1)));
      } else {
        reject(new Error(`Timeout for ${url}`));
      }
    });
    req.on('error', (err) => {
      if (retries > 0) {
        sleep(RETRY_DELAY).then(() => resolve(httpGet(url, retries - 1)));
      } else {
        reject(err);
      }
    });
  });
}

function csvEscape(val) {
  if (!val) return '';
  val = String(val).replace(/\r?\n/g, ' ').trim();
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Strip non-digits
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
  }
  return phone.trim();
}

function parseNameFromFirm(firmName) {
  if (!firmName) return { first: '', last: '' };

  // Try to extract a person name from firm name patterns
  // "Law Offices of John Smith" → John Smith
  // "John Smith, Attorney at Law" → John Smith
  // "The Law Firm of Jane Doe" → Jane Doe

  let name = '';

  const patterns = [
    /Law\s+Offic(?:e|es)\s+of\s+(.+?)(?:\s*,|\s+LLC|\s+LLP|\s+PC|\s+P\.?C\.?|\s+PLLC|\s+Inc|$)/i,
    /Law\s+Firm\s+of\s+(.+?)(?:\s*,|\s+LLC|\s+LLP|\s+PC|\s+P\.?C\.?|\s+PLLC|\s+Inc|$)/i,
    /(.+?)\s*,\s*(?:Attorney|Lawyer|Esq|P\.?A\.?|P\.?C\.?)/i,
    /(.+?)\s+(?:Law\s+(?:Firm|Group|Office)|Attorney|Lawyer|& Associates)/i,
  ];

  for (const pat of patterns) {
    const m = firmName.match(pat);
    if (m) {
      name = m[1].trim();
      break;
    }
  }

  if (!name) return { first: '', last: '' };

  // Clean up common prefixes
  name = name.replace(/^(?:The\s+)?(?:Law\s+)?(?:Offices?\s+of\s+)?/i, '').trim();

  // Remove suffixes
  name = name.replace(/\s*(?:LLC|LLP|PC|P\.C\.|PLLC|Inc\.?|& Associates|and Associates|Esq\.?)$/gi, '').trim();

  // Split into first/last
  const parts = name.split(/\s+/);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: '', last: parts[0] };

  // Handle "Dr." or "Mr." prefixes
  let startIdx = 0;
  if (/^(Dr|Mr|Mrs|Ms|Prof)\.?$/i.test(parts[0])) startIdx = 1;

  const first = parts[startIdx] || '';
  const last = parts[parts.length - 1] || '';

  // Validate it looks like a person name (not a business name)
  if (/\b(group|associates|partners|services|solutions|center|centre|legal|firm|office|law)\b/i.test(name)) {
    return { first: '', last: '' };
  }

  return { first, last };
}

function mapCountryFromUrl(urlPath) {
  if (!urlPath) return 'USA';
  const p = urlPath.toLowerCase();
  if (p.includes('usa-') || p.includes('/united-states')) return 'USA';
  if (p.includes('united-kingdom') || p.includes('england') || p.includes('scotland') || p.includes('northern-ireland')) return 'UK';
  if (p.includes('can-') || p.includes('/canada')) return 'Canada';
  if (p.includes('aus-') || p.includes('/australia')) return 'Australia';
  // Try to extract country from path
  const parts = p.split('/law-firms/immigration/');
  if (parts.length > 1) {
    const loc = parts[1].split('/')[0].replace('.html', '');
    if (loc.startsWith('usa-')) return 'USA';
    // Return title case
    return loc.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  }
  return '';
}

function mapStateFromUrl(urlPath) {
  if (!urlPath) return '';
  const p = urlPath.toLowerCase();
  const m = p.match(/usa-([a-z-]+)/);
  if (m) {
    return m[1].split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  }
  return '';
}

// === Phase 1: Discover listing pages via CDX API ===

async function discoverListingPages(countryFilter) {
  console.log('\n=== Phase 1: Discovering listing pages via CDX API ===\n');

  // Check cache
  if (fs.existsSync(LISTING_URLS_CACHE)) {
    console.log(`  Loading cached listing URLs from ${LISTING_URLS_CACHE}`);
    const cached = JSON.parse(fs.readFileSync(LISTING_URLS_CACHE, 'utf-8'));
    let urls = cached;
    if (countryFilter) {
      urls = urls.filter(u => u.original.toLowerCase().includes(countryFilter.toLowerCase()));
      console.log(`  Filtered to ${urls.length} URLs matching "${countryFilter}"`);
    }
    return urls;
  }

  const allUrls = [];
  let page = 0;

  // Query CDX for /law-firms/immigration/* listing pages
  // Use text output (more reliable than JSON for large results)
  const cdxUrl = `${CDX_API}?url=hg.org/law-firms/immigration/*&output=text&fl=original,timestamp,statuscode,length,mimetype&filter=statuscode:200&filter=mimetype:text/html&collapse=urlkey&limit=50000`;

  console.log(`  Fetching CDX index...`);

  try {
    const raw = await httpGet(cdxUrl);
    const lines = raw.trim().split('\n').filter(l => l.trim());

    for (const line of lines) {
      const parts = line.split(/\s+/);
      if (parts.length < 4) continue;

      const [original, timestamp, statuscode, length] = parts;

      // Skip CSS/JS/images
      if (/\.(css|js|png|jpg|gif|ico|svg)$/i.test(original)) continue;
      // Skip very small responses (likely Incapsula blocks or errors)
      if (parseInt(length) < 3000) continue;

      allUrls.push({ original, timestamp, length: parseInt(length) });
    }

    console.log(`    Found ${allUrls.length} listing page URLs`);
  } catch (err) {
    console.error(`  CDX error: ${err.message}`);
  }

  // For pages with multiple snapshots, try to find the best one (largest content)
  // Query again without collapse to get all snapshots for dedup later
  console.log('  Fetching additional snapshots for better coverage...');
  try {
    const cdxUrl2 = `${CDX_API}?url=hg.org/law-firms/immigration/usa*&output=text&fl=original,timestamp,statuscode,length&filter=statuscode:200&filter=mimetype:text/html&sort=reverse&limit=50000`;
    const raw2 = await httpGet(cdxUrl2);
    const lines2 = raw2.trim().split('\n').filter(l => l.trim());

    // Build map of urlkey -> best snapshot (largest)
    const existing = new Set(allUrls.map(u => u.original.toLowerCase().replace(/:80/g, '')));
    let added = 0;

    for (const line of lines2) {
      const parts = line.split(/\s+/);
      if (parts.length < 4) continue;
      const [original, timestamp, statuscode, length] = parts;
      if (/\.(css|js|png|jpg|gif|ico|svg)$/i.test(original)) continue;
      if (parseInt(length) < 3000) continue;

      const key = original.toLowerCase().replace(/:80/g, '');
      if (!existing.has(key)) {
        allUrls.push({ original, timestamp, length: parseInt(length) });
        existing.add(key);
        added++;
      }
    }

    console.log(`    Added ${added} additional US listing snapshots`);
  } catch (err) {
    console.error(`  CDX error (additional): ${err.message}`);
  }

  // Also query for individual attorney profile pages with "immigration" in the URL
  console.log('\n  Also fetching attorney profiles with "immigration" in URL...');
  try {
    const cdxUrl2 = `${CDX_API}?url=hg.org/attorney/immigration*&output=json&fl=original,timestamp,statuscode,length&filter=statuscode:200&filter=mimetype:text/html&collapse=urlkey&limit=${CDX_PAGE_SIZE}`;
    const raw2 = await httpGet(cdxUrl2);
    let data2;
    try { data2 = JSON.parse(raw2); } catch { data2 = []; }

    if (data2.length > 1) {
      const rows2 = data2.slice(1);
      let added = 0;
      for (const row of rows2) {
        const [original, timestamp, , length] = row;
        if (/\.(css|js|png|jpg)$/i.test(original)) continue;
        if (/\/(articles|preview|css|js)\//i.test(original)) continue;
        if (parseInt(length) < 2000) continue;
        if (!/\/\d+$/.test(original)) continue; // Must end with numeric ID
        allUrls.push({ original, timestamp, length: parseInt(length), isProfile: true });
        added++;
      }
      console.log(`    Added ${added} direct immigration attorney profiles`);
    }
  } catch (err) {
    console.error(`  CDX error for attorney profiles: ${err.message}`);
  }

  console.log(`\n  Total discovered: ${allUrls.length} URLs`);

  // Cache results
  fs.writeFileSync(LISTING_URLS_CACHE, JSON.stringify(allUrls, null, 2));
  console.log(`  Cached to ${LISTING_URLS_CACHE}`);

  if (countryFilter) {
    const cf = countryFilter.toLowerCase();
    const filtered = allUrls.filter(u => {
      const url = u.original.toLowerCase();
      // Match as a path segment (e.g. /usa- or /usa/ or /usa.)
      // This avoids "usa" matching "jerusalem" or "lausanne"
      return url.includes(`/${cf}-`) || url.includes(`/${cf}/`) ||
             url.includes(`/${cf}.`) || url.endsWith(`/${cf}`);
    });
    console.log(`  Filtered to ${filtered.length} URLs matching "${countryFilter}"`);
    return filtered;
  }

  return allUrls;
}

// === Phase 2: Fetch listing pages and extract attorney data ===

function parseListingPage(html, listingUrl) {
  const $ = cheerio.load(html);
  const attorneys = [];

  // Find all listing blocks
  // Pattern 1 (2010-2013 era): div.listing containing h3 > a[href*="/attorney/"]
  // Pattern 2 (2015+ era): div.profile with schema.org

  // Extract from listing blocks (both "listing" and "listingbasic" classes)
  $('div.listing, div.listingbasic, .fulllisting .listing').each((_, el) => {
    const $el = $(el);
    // 2013+ uses h3, 2010-2012 uses h1 inside listing divs
    const linkEl = $el.find('h3 a[href*="/attorney/"], h1 a[href*="/attorney/"]').first();
    if (!linkEl.length) return;

    const href = linkEl.attr('href') || '';
    const firmName = linkEl.text().trim();

    // Extract HG ID from URL
    const idMatch = href.match(/\/(\d+)$/);
    const hgId = idMatch ? idMatch[1] : '';

    // Extract location
    const locationText = $el.find('.location').text().trim();
    let city = '', state = '', zip = '';

    // Parse "City, ST 12345" or "City, State"
    const locMatch = locationText.match(/^([^,]+),\s*([A-Z]{2})\s*(\d{5})?/);
    if (locMatch) {
      city = locMatch[1].trim();
      state = locMatch[2];
      zip = locMatch[3] || '';
    } else {
      const locMatch2 = locationText.match(/^([^,]+),\s*(.+?)(?:\s*-.*)?$/);
      if (locMatch2) {
        city = locMatch2[1].trim();
        state = locMatch2[2].trim();
      }
    }

    // Extract phone - multiple patterns across HG.org eras
    let phoneText = $el.find('.mainphone').text().trim();
    if (!phoneText) phoneText = $el.find('.phone').text().trim();
    if (!phoneText) phoneText = $el.find('[itemprop="telephone"]').text().trim();
    if (!phoneText) phoneText = $el.find('a[href^="tel:"]').text().trim();
    // Last resort: look in the whole listing block text for a phone pattern
    if (!phoneText) phoneText = $el.text();
    const phoneMatch = phoneText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    const phone = phoneMatch ? normalizePhone(phoneMatch[0]) : '';

    // Extract description
    const desc = $el.find('p').first().text().trim().slice(0, 500);

    // Get country from URL path
    const country = mapCountryFromUrl(listingUrl);
    if (!state) state = mapStateFromUrl(listingUrl);

    const { first, last } = parseNameFromFirm(firmName);

    attorneys.push({
      hgId,
      company: firmName,
      first_name: first,
      last_name: last,
      phone,
      city,
      state,
      zip,
      country,
      description: desc,
      source: 'hg_org_wayback',
      profileUrl: href,
      fromListing: true,
    });
  });

  // If no listing blocks found, try extracting attorney profile links anyway
  if (attorneys.length === 0) {
    $('a[href*="/attorney/"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const idMatch = href.match(/\/attorney\/([^/]+)\/(\d+)/);
      if (!idMatch) return;
      if (/\/(articles|preview|css|js)\//.test(href)) return;

      const firmSlug = idMatch[1];
      const hgId = idMatch[2];
      const firmName = $(el).text().trim();

      if (!firmName || firmName.length < 3) return;

      const country = mapCountryFromUrl(listingUrl);
      const state = mapStateFromUrl(listingUrl);
      const { first, last } = parseNameFromFirm(firmName);

      attorneys.push({
        hgId,
        company: firmName,
        first_name: first,
        last_name: last,
        phone: '',
        city: '',
        state,
        country,
        description: '',
        source: 'hg_org_wayback',
        profileUrl: href,
        fromListing: true,
      });
    });
  }

  return attorneys;
}

function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const data = {};

  // Schema.org structured data (Attorney or LegalService)
  const profileEl = $('[itemscope][itemtype*="Attorney"], [itemscope][itemtype*="LegalService"]').first();
  if (!profileEl.length) {
    // Fallback: look for any profile-like structure
    const h1 = $('h1[itemprop="name"]').first();
    if (h1.length) {
      data.company = h1.text().trim();
    }
  }

  // Firm name
  data.company = $('[itemprop="name"]').first().text().trim() || $('h1').first().text().trim();

  // Address
  data.address = $('[itemprop="streetAddress"]').text().trim().replace(/\s+/g, ' ');
  data.city = $('[itemprop="addressLocality"]').text().trim();
  data.state = $('[itemprop="addressRegion"]').text().trim();
  data.zip = $('[itemprop="postalCode"]').text().trim();
  data.country = $('[itemprop="addressCountry"]').attr('content') ||
                 $('[itemprop="addressCountry"]').text().trim() || '';

  // Phone - multiple patterns across different eras
  const phoneEl = $('[itemprop="telephone"]');
  data.phone = phoneEl.text().trim() || '';
  if (!data.phone) {
    const telLink = $('a[href^="tel:"]').first();
    if (telLink.length) data.phone = telLink.text().trim();
  }
  if (!data.phone) {
    data.phone = $('.mainphone').first().text().trim();
  }
  if (!data.phone) {
    // Older era uses div.phone with "Call (xxx) xxx-xxxx" format
    const phoneDiv = $('.phone').first().text().trim();
    const phoneMatch = phoneDiv.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    if (phoneMatch) data.phone = phoneMatch[0];
  }
  if (!data.phone) {
    // Last resort: scan profile div for phone pattern
    const profileText = $('.profile, .fulllisting').first().text();
    const phoneMatch = profileText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
    if (phoneMatch) data.phone = phoneMatch[0];
  }

  // Fax
  data.fax = $('[itemprop="faxNumber"]').text().trim();

  // Geo
  data.latitude = $('[itemprop="latitude"]').attr('content') || '';
  data.longitude = $('[itemprop="longitude"]').attr('content') || '';

  // Website
  data.website = $('[itemprop="url"]').attr('href') || '';
  // Clean wayback prefix
  if (data.website && data.website.includes('web.archive.org')) {
    const wbMatch = data.website.match(/\/web\/\d+\/(https?:\/\/.+)/);
    if (wbMatch) data.website = wbMatch[1];
  }

  // Practice areas
  const areas = [];
  $('[itemprop="serviceType"]').each((_, el) => {
    const t = $(el).attr('content') || $(el).text().trim();
    if (t) areas.push(t);
  });
  if (areas.length === 0) {
    $('.subselection li a, .sectiontitle + div li a').each((_, el) => {
      areas.push($(el).text().trim());
    });
  }
  data.practice_areas = areas.join('; ');

  // Description
  data.description = $('[itemprop="Description"]').text().trim() ||
                     $('[itemprop="description"]').text().trim() ||
                     $('.listingoverview').next('p').text().trim() ||
                     '';
  data.description = data.description.slice(0, 500);

  // Extract HG ID from page URL
  const canonicalLink = $('link[rel="canonical"]').attr('href') || '';
  const idMatch = canonicalLink.match(/\/(\d+)$/);
  data.hgId = idMatch ? idMatch[1] : '';

  // Try title for more info
  if (!data.company) {
    const title = $('title').text();
    const tMatch = title.match(/^(.+?)\s*(?:\||[-–])\s*/);
    if (tMatch) data.company = tMatch[1].trim();
  }

  // Parse name from firm
  const { first, last } = parseNameFromFirm(data.company);
  data.first_name = first;
  data.last_name = last;

  data.source = 'hg_org_wayback';

  return data;
}

// === Phase 3: Main scrape orchestration ===

async function main() {
  const args = process.argv.slice(2);
  const listingsOnly = args.includes('--listings-only');
  const profilesOnly = args.includes('--profiles-only');
  const resume = args.includes('--resume');
  const countryFilter = args.includes('--country') ? args[args.indexOf('--country') + 1] : null;

  let maxListings = Infinity;
  if (args.includes('--max-listings')) {
    maxListings = parseInt(args[args.indexOf('--max-listings') + 1]);
  }
  let maxProfiles = Infinity;
  if (args.includes('--max-profiles')) {
    maxProfiles = parseInt(args[args.indexOf('--max-profiles') + 1]);
  }

  // Ensure output directory
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║   HG.org Wayback Machine Immigration Lawyer Scraper    ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log(`  Output:  ${CSV_FILE}`);
  console.log(`  Source:  web.archive.org cached pages`);
  if (countryFilter) console.log(`  Filter:  ${countryFilter}`);
  if (maxListings < Infinity) console.log(`  Max listings: ${maxListings}`);
  if (maxProfiles < Infinity) console.log(`  Max profiles: ${maxProfiles}`);

  // Load progress
  let progress = { fetchedListings: [], fetchedProfiles: [], attorneys: {} };
  if (resume && fs.existsSync(PROGRESS_FILE)) {
    progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
    console.log(`\n  Resuming: ${Object.keys(progress.attorneys).length} attorneys, ${progress.fetchedListings.length} listings, ${progress.fetchedProfiles.length} profiles already done`);
  }

  const fetchedListingsSet = new Set(progress.fetchedListings);
  const fetchedProfilesSet = new Set(progress.fetchedProfiles);

  // Phase 1: Discover URLs
  const listingUrls = await discoverListingPages(countryFilter);

  // Phase 2: Fetch listing pages
  if (!profilesOnly) {
    console.log('\n=== Phase 2: Fetching listing pages ===\n');

    // Filter to actual listing pages (not individual profiles)
    const listings = listingUrls.filter(u => !u.isProfile);
    const toFetch = listings.filter(u => !fetchedListingsSet.has(u.original));
    const limited = toFetch.slice(0, maxListings);

    console.log(`  Total listing pages: ${listings.length}`);
    console.log(`  Already fetched: ${fetchedListingsSet.size}`);
    console.log(`  To fetch: ${limited.length}`);

    let fetchedCount = 0;
    let errorCount = 0;
    let newAttorneys = 0;

    for (const urlInfo of limited) {
      const waybackUrl = `${WAYBACK_BASE}/${urlInfo.timestamp}/${urlInfo.original}`;

      try {
        process.stdout.write(`  [${fetchedCount + 1}/${limited.length}] ${urlInfo.original.slice(0, 80)}... `);

        const html = await httpGet(waybackUrl);

        // Skip Incapsula-blocked pages
        if (html.includes('Incapsula') || html.includes('_Incapsula_Resource?SWUDNSAI')) {
          process.stdout.write('BLOCKED\n');
          fetchedListingsSet.add(urlInfo.original);
          fetchedCount++;
          await sleep(DELAY_MS);
          continue;
        }

        const attorneys = parseListingPage(html, urlInfo.original);

        let added = 0;
        for (const atty of attorneys) {
          if (!atty.hgId || progress.attorneys[atty.hgId]) continue;
          progress.attorneys[atty.hgId] = atty;
          added++;
          newAttorneys++;
        }

        process.stdout.write(`${attorneys.length} found, ${added} new\n`);

        fetchedListingsSet.add(urlInfo.original);
        progress.fetchedListings.push(urlInfo.original);
        fetchedCount++;

        // Save progress every 50 pages
        if (fetchedCount % 50 === 0) {
          saveProgress(progress);
          console.log(`    [Progress saved: ${Object.keys(progress.attorneys).length} attorneys]`);
        }

        await sleep(DELAY_MS);
      } catch (err) {
        process.stdout.write(`ERROR: ${err.message}\n`);
        errorCount++;
        await sleep(DELAY_MS * 2);
      }
    }

    console.log(`\n  Listing phase complete: ${fetchedCount} pages fetched, ${errorCount} errors, ${newAttorneys} new attorneys`);
    console.log(`  Total unique attorneys: ${Object.keys(progress.attorneys).length}`);
    saveProgress(progress);
  }

  // Phase 2b: Also process direct immigration attorney profile URLs from CDX
  if (!profilesOnly) {
    const directProfiles = listingUrls.filter(u => u.isProfile);
    if (directProfiles.length > 0) {
      console.log(`\n=== Phase 2b: Processing ${directProfiles.length} direct immigration attorney profiles ===\n`);

      let fetchedCount = 0;
      let newCount = 0;

      for (const urlInfo of directProfiles) {
        const idMatch = urlInfo.original.match(/\/(\d+)$/);
        if (!idMatch) continue;
        const hgId = idMatch[1];

        if (progress.attorneys[hgId] && progress.attorneys[hgId].profileFetched) {
          continue;
        }

        if (fetchedCount >= maxProfiles) break;

        const waybackUrl = `${WAYBACK_BASE}/${urlInfo.timestamp}/${urlInfo.original}`;

        try {
          process.stdout.write(`  [${fetchedCount + 1}] ${urlInfo.original.slice(-60)}... `);

          const html = await httpGet(waybackUrl);

          if (html.includes('Incapsula') || html.includes('_Incapsula_Resource?SWUDNSAI')) {
            process.stdout.write('BLOCKED\n');
            fetchedCount++;
            await sleep(DELAY_MS);
            continue;
          }

          const data = parseProfilePage(html);
          data.hgId = hgId;
          data.profileFetched = true;

          if (data.company) {
            if (progress.attorneys[hgId]) {
              // Merge with existing
              Object.assign(progress.attorneys[hgId], data);
            } else {
              progress.attorneys[hgId] = data;
              newCount++;
            }
            process.stdout.write(`OK (${data.company.slice(0, 40)})\n`);
          } else {
            process.stdout.write('NO DATA\n');
          }

          fetchedCount++;
          fetchedProfilesSet.add(hgId);
          progress.fetchedProfiles.push(hgId);

          if (fetchedCount % 25 === 0) {
            saveProgress(progress);
          }

          await sleep(DELAY_MS);
        } catch (err) {
          process.stdout.write(`ERROR: ${err.message}\n`);
          await sleep(DELAY_MS * 2);
        }
      }

      console.log(`  Direct profiles: ${fetchedCount} fetched, ${newCount} new`);
      saveProgress(progress);
    }
  }

  // Phase 3: Fetch individual profile pages (for attorneys found in listings that lack data)
  if (!listingsOnly) {
    console.log('\n=== Phase 3: Enriching with individual profile pages ===\n');

    // Find attorneys that need enrichment (have profile URL but no phone/address)
    const needsEnrichment = Object.values(progress.attorneys).filter(a => {
      if (a.profileFetched) return false;
      if (!a.profileUrl) return false;
      // Already have good data
      if (a.phone && a.city) return false;
      return true;
    });

    const limited = needsEnrichment.slice(0, maxProfiles);
    console.log(`  Attorneys needing enrichment: ${needsEnrichment.length}`);
    console.log(`  Will fetch: ${limited.length}`);

    let enriched = 0;
    let errors = 0;

    for (const atty of limited) {
      if (fetchedProfilesSet.has(atty.hgId)) continue;

      // Build wayback URL for the profile
      let profileOriginal = atty.profileUrl;
      // Strip any existing wayback prefix
      const wbMatch = profileOriginal.match(/\/web\/\d+\/(https?:\/\/.+)/);
      if (wbMatch) profileOriginal = wbMatch[1];
      // Make absolute
      if (profileOriginal.startsWith('/')) {
        profileOriginal = `http://www.hg.org${profileOriginal}`;
      }
      if (!profileOriginal.startsWith('http')) {
        profileOriginal = `http://www.hg.org/attorney/${profileOriginal}`;
      }

      // Use CDX to find the best snapshot
      try {
        process.stdout.write(`  [${enriched + 1}/${limited.length}] ${atty.company?.slice(0, 50) || atty.hgId}... `);

        // Try multiple timestamps (newest usable first, fall back to older)
        const timestamps = ['20150101', '20130101', '20120101', '20110101', '20100101'];
        let html = null;

        for (const ts of timestamps) {
          const waybackUrl = `${WAYBACK_BASE}/${ts}/${profileOriginal}`;
          try {
            const resp = await httpGet(waybackUrl);
            if (resp.includes('Incapsula') || resp.includes('_Incapsula_Resource?SWUDNSAI')) {
              await sleep(DELAY_MS);
              continue; // Try older snapshot
            }
            html = resp;
            break;
          } catch {
            await sleep(1000);
            continue;
          }
        }

        if (!html) {
          process.stdout.write('BLOCKED (all snapshots)\n');
          atty.profileFetched = true;
          enriched++;
          await sleep(DELAY_MS);
          continue;
        }

        const data = parseProfilePage(html);

        // Merge data (don't overwrite existing good data)
        if (data.company && !atty.company) atty.company = data.company;
        if (data.phone && !atty.phone) atty.phone = data.phone;
        if (data.city && !atty.city) atty.city = data.city;
        if (data.state && !atty.state) atty.state = data.state;
        if (data.zip && !atty.zip) atty.zip = data.zip;
        if (data.country && !atty.country) atty.country = data.country;
        if (data.address && !atty.address) atty.address = data.address;
        if (data.fax && !atty.fax) atty.fax = data.fax;
        if (data.latitude && !atty.latitude) atty.latitude = data.latitude;
        if (data.longitude && !atty.longitude) atty.longitude = data.longitude;
        if (data.practice_areas && !atty.practice_areas) atty.practice_areas = data.practice_areas;
        if (data.description && !atty.description) atty.description = data.description;
        if (data.website && !atty.website) atty.website = data.website;
        if (data.first_name && !atty.first_name) atty.first_name = data.first_name;
        if (data.last_name && !atty.last_name) atty.last_name = data.last_name;

        atty.profileFetched = true;

        process.stdout.write(`OK (phone: ${atty.phone ? 'Y' : 'N'}, city: ${atty.city || '?'})\n`);

        enriched++;
        fetchedProfilesSet.add(atty.hgId);
        progress.fetchedProfiles.push(atty.hgId);

        if (enriched % 25 === 0) {
          saveProgress(progress);
          console.log(`    [Progress saved]`);
        }

        await sleep(DELAY_MS);
      } catch (err) {
        process.stdout.write(`ERROR: ${err.message}\n`);
        atty.profileFetched = true; // Mark to avoid retrying
        errors++;
        await sleep(DELAY_MS * 2);
      }
    }

    console.log(`\n  Enrichment complete: ${enriched} profiles fetched, ${errors} errors`);
    saveProgress(progress);
  }

  // Phase 4: Write CSV output
  writeCSV(progress);

  // Summary
  const all = Object.values(progress.attorneys);
  const withPhone = all.filter(a => a.phone);
  const withCity = all.filter(a => a.city);
  const withBoth = all.filter(a => a.phone && a.city);
  const countries = {};
  for (const a of all) {
    const c = a.country || 'Unknown';
    countries[c] = (countries[c] || 0) + 1;
  }

  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║                     Final Summary                       ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log(`  Total attorneys:    ${all.length}`);
  console.log(`  With phone:        ${withPhone.length}`);
  console.log(`  With city:         ${withCity.length}`);
  console.log(`  With phone+city:   ${withBoth.length}`);
  console.log(`  Countries:`);
  for (const [c, n] of Object.entries(countries).sort((a, b) => b[1] - a[1]).slice(0, 20)) {
    console.log(`    ${c}: ${n}`);
  }
  console.log(`\n  Output: ${CSV_FILE}`);
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress));
}

function writeCSV(progress) {
  console.log('\n=== Writing CSV ===\n');

  const attorneys = Object.values(progress.attorneys);

  // Sort by country, state, city
  attorneys.sort((a, b) => {
    const cmp1 = (a.country || '').localeCompare(b.country || '');
    if (cmp1) return cmp1;
    const cmp2 = (a.state || '').localeCompare(b.state || '');
    if (cmp2) return cmp2;
    return (a.city || '').localeCompare(b.city || '');
  });

  let csv = CSV_HEADER;
  let count = 0;

  for (const a of attorneys) {
    // Normalize phone
    const phone = normalizePhone(a.phone);

    // Normalize country
    let country = a.country || '';
    if (country === 'USA' || country === 'United States Of America') country = 'USA';

    csv += [
      csvEscape(''),  // email (not available on HG.org profiles)
      csvEscape(a.first_name),
      csvEscape(a.last_name),
      csvEscape(a.company),
      csvEscape(phone),
      csvEscape(a.city),
      csvEscape(a.state),
      csvEscape(country),
      csvEscape(a.source || 'hg_org_wayback'),
      csvEscape(a.address),
      csvEscape(a.zip),
      csvEscape(a.fax),
      csvEscape(a.latitude),
      csvEscape(a.longitude),
      csvEscape(a.practice_areas),
      csvEscape(a.description),
      csvEscape(a.hgId),
      csvEscape(a.website),
    ].join(',') + '\n';

    count++;
  }

  fs.writeFileSync(CSV_FILE, csv);
  console.log(`  Wrote ${count} attorneys to ${CSV_FILE}`);
}

// === Run ===
main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
