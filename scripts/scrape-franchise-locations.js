#!/usr/bin/env node
/**
 * Franchise Location Scraper
 *
 * Scrapes franchise directories for location data:
 * 1. Orangetheory Fitness — 1,463 US studios (EMAIL + phone + address + franchise entity)
 * 2. Club Pilates — 67+ studios (EMAIL + phone + address)
 * 3. Sylvan Learning — 750+ centers (phone + address via sitemap + page scrape)
 * 4. European Wax Center — 1,268 locations (phone + address via RLS/sitemap)
 * 5. The Learning Experience — 545 centers (phone + address via sitemap)
 *
 * Output: output/franchise-locations.csv
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'franchise-locations.csv');
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source,website\n';

// Rate limiting
const DELAY_MS = 1500; // 1.5s between requests
const MAX_CONCURRENT = 3;
const MAX_RETRIES = 2;

let totalLeads = 0;
let totalEmails = 0;
let writeStream;

// ─── HTTP Helper ──────────────────────────────────────────────────────────────

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const reqOptions = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...options.headers
      },
      timeout: 30000,
    };

    const req = mod.request(reqOptions, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
        return httpGet(redirectUrl, options).then(resolve).catch(reject);
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        } else {
          resolve({ body: data, statusCode: res.statusCode, headers: res.headers });
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

async function fetchWithRetry(url, options = {}, retries = MAX_RETRIES) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await httpGet(url, options);
    } catch (err) {
      if (i === retries) throw err;
      await sleep(2000 * (i + 1));
    }
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── CSV Helper ───────────────────────────────────────────────────────────────

function escCsv(val) {
  if (!val) return '';
  val = String(val).trim();
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function writeLead(lead) {
  const row = [
    escCsv(lead.email),
    escCsv(lead.first_name),
    escCsv(lead.last_name),
    escCsv(lead.company),
    escCsv(lead.phone),
    escCsv(lead.city),
    escCsv(lead.state),
    escCsv(lead.country),
    escCsv(lead.source),
    escCsv(lead.website),
  ].join(',');
  writeStream.write(row + '\n');
  totalLeads++;
  if (lead.email) totalEmails++;
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Remove all non-digit chars
  let digits = phone.replace(/\D/g, '');
  // Remove leading 1 for US numbers
  if (digits.length === 11 && digits.startsWith('1')) {
    digits = digits.slice(1);
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return phone.trim();
}

function extractStateFromAddress(address) {
  if (!address) return '';
  // Match US state codes in address
  const match = address.match(/,\s*([A-Z]{2})\s+\d{5}/);
  return match ? match[1] : '';
}

function extractCityFromAddress(address) {
  if (!address) return '';
  // Try to get city from "City, ST ZIP" pattern
  const match = address.match(/([A-Za-z\s.'-]+),\s*[A-Z]{2}\s+\d{5}/);
  return match ? match[1].trim() : '';
}

// ─── Batch processing helper ──────────────────────────────────────────────────

async function processBatch(items, fn, concurrency = MAX_CONCURRENT) {
  const results = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.allSettled(batch.map(fn));
    results.push(...batchResults);
    if (i + concurrency < items.length) {
      await sleep(DELAY_MS);
    }
  }
  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. ORANGETHEORY FITNESS — Sitemap → Individual page scrape
//    Each page has: studiomanagerNNNN@orangetheoryfitness.com, phone, address, entity
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeOrangetheory() {
  console.log('\n🟠 ORANGETHEORY FITNESS');
  console.log('─'.repeat(60));

  // Step 1: Get all studio URLs from sitemap
  console.log('  Fetching US sitemap...');
  const { body: sitemapXml } = await fetchWithRetry('https://www.orangetheory.com/en-us/sitemap.xml');
  const $ = cheerio.load(sitemapXml, { xmlMode: true });

  const studioUrls = [];
  $('url > loc').each((_, el) => {
    const url = $(el).text().trim();
    if (url.match(/\/en-us\/locations\/[a-z]+-[a-z]+-\d+$/)) {
      studioUrls.push(url);
    }
  });

  console.log(`  Found ${studioUrls.length} studio URLs in sitemap`);

  // Step 2: Scrape each studio page
  let scraped = 0;
  let errors = 0;

  await processBatch(studioUrls, async (url) => {
    try {
      const { body: html } = await fetchWithRetry(url);
      const page$ = cheerio.load(html);

      // Extract studio number from URL
      const studioNum = url.match(/-(\d+)$/)?.[1] || '';

      // Look for structured data in page
      let phone = '';
      let address = '';
      let city = '';
      let state = '';
      let studioName = '';
      let franchiseEntity = '';
      let email = '';

      // Predictable email pattern
      if (studioNum) {
        email = `studiomanager${studioNum}@orangetheoryfitness.com`;
      }

      // Extract from page text content
      const text = html;

      // Phone: look for phone numbers in the page
      const phoneMatch = text.match(/"telephone"\s*:\s*"([^"]+)"/i)
        || text.match(/"phone"\s*:\s*"([^"]+)"/i)
        || text.match(/tel:([0-9-()+ ]+)/i);
      if (phoneMatch) {
        phone = normalizePhone(phoneMatch[1]);
      }

      // Address from JSON-LD or page content
      const addressMatch = text.match(/"streetAddress"\s*:\s*"([^"]+)"/i);
      const cityMatch = text.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
      const stateMatch = text.match(/"addressRegion"\s*:\s*"([^"]+)"/i);

      if (addressMatch) address = addressMatch[1];
      if (cityMatch) city = cityMatch[1];
      if (stateMatch) state = stateMatch[1];

      // Franchise entity
      const entityMatch = text.match(/Franchise\s*Entity[:\s]*([A-Z][A-Z0-9\s,.'&-]+(?:LLC|INC|LP|CORP|LTD|PLLC))/i)
        || text.match(/"franchiseEntity"\s*:\s*"([^"]+)"/i);
      if (entityMatch) franchiseEntity = entityMatch[1].trim();

      // Studio name from title or heading
      const titleMatch = text.match(/<title>([^<]+)<\/title>/i);
      if (titleMatch) {
        studioName = titleMatch[1].replace(/\s*\|.*$/, '').replace(/Orangetheory Fitness\s*/i, '').trim();
      }

      // Parse city/state from URL if not found in page
      if (!city || !state) {
        const urlParts = url.match(/locations\/(.+)-(\w+)-\d+$/);
        if (urlParts) {
          const rawCity = urlParts[1].replace(/-/g, ' ');
          const rawState = urlParts[2];
          if (!city) city = rawCity.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
          if (!state) state = rawState.toUpperCase().slice(0, 2);
        }
      }

      const company = franchiseEntity
        ? `Orangetheory Fitness ${city || studioName} (${franchiseEntity})`
        : `Orangetheory Fitness ${city || studioName}`;

      writeLead({
        email,
        first_name: '',
        last_name: '',
        company: company.substring(0, 100),
        phone,
        city,
        state,
        country: 'US',
        source: 'orangetheory',
        website: url,
      });

      scraped++;
      if (scraped % 50 === 0) {
        console.log(`  Scraped ${scraped}/${studioUrls.length} studios...`);
      }
    } catch (err) {
      errors++;
    }
  }, 5); // 5 concurrent for OTF (generous rate limit)

  console.log(`  Done: ${scraped} studios scraped, ${errors} errors`);
  return scraped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. CLUB PILATES — Sitemap → Individual page scrape
//    Each page has: {slug}@clubpilates.com, phone, address
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeClubPilates() {
  console.log('\n🟣 CLUB PILATES');
  console.log('─'.repeat(60));

  // Step 1: Get all location URLs from sitemap
  console.log('  Fetching sitemap...');
  const { body: sitemapXml } = await fetchWithRetry('https://www.clubpilates.com/sitemap.xml');
  const $ = cheerio.load(sitemapXml, { xmlMode: true });

  const locationUrls = [];
  $('url > loc').each((_, el) => {
    const url = $(el).text().trim();
    if (url.match(/\/location\/[a-z0-9]+$/)) {
      locationUrls.push(url);
    }
  });

  console.log(`  Found ${locationUrls.length} studio URLs in sitemap`);

  // Step 2: Scrape each studio page
  let scraped = 0;
  let errors = 0;

  await processBatch(locationUrls, async (url) => {
    try {
      const { body: html } = await fetchWithRetry(url);
      const page$ = cheerio.load(html);

      const slug = url.match(/\/location\/([a-z0-9]+)$/)?.[1] || '';

      let phone = '';
      let email = '';
      let address = '';
      let city = '';
      let state = '';
      let studioName = '';

      // Email pattern: slug@clubpilates.com
      if (slug) {
        email = `${slug}@clubpilates.com`;
      }

      // Extract from embedded JSON data (Club Pilates uses JS objects, not JSON-LD)
      const text = html;

      // Try to extract from embedded JSON object with "city", "state", "address" keys
      const cpCityMatch = text.match(/"city"\s*:\s*"([^"]+)"/);
      const cpStateMatch = text.match(/"state"\s*:\s*"([^"]+)"/);
      const cpAddrMatch = text.match(/"address"\s*:\s*"([^"]+)"/);
      const cpCountryMatch = text.match(/"country"\s*:\s*"([^"]+)"/);

      if (cpCityMatch) city = cpCityMatch[1];
      if (cpStateMatch) state = cpStateMatch[1];
      if (cpAddrMatch) address = cpAddrMatch[1];

      // Determine country
      let country = '';
      if (cpCountryMatch) {
        const c = cpCountryMatch[1].toUpperCase();
        if (c === 'US' || c === 'USA' || c === 'UNITED STATES') country = 'US';
        else if (c === 'CA' || c === 'CANADA') country = 'CA';
        else if (c === 'AU' || c === 'AUSTRALIA') country = 'AU';
        else country = c;
      }

      // Fallback: try JSON-LD style
      if (!city) {
        const cityMatch = text.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
        if (cityMatch) city = cityMatch[1];
      }
      if (!state) {
        const stateMatch = text.match(/"addressRegion"\s*:\s*"([^"]+)"/i);
        if (stateMatch) state = stateMatch[1];
      }

      // Phone
      const phoneMatch = text.match(/tel:\+?1?([0-9-()+ ]+)/i)
        || text.match(/"telephone"\s*:\s*"([^"]+)"/i)
        || text.match(/"phone"\s*:\s*"([^"]+)"/i);
      if (phoneMatch) {
        phone = normalizePhone(phoneMatch[1]);
      }
      if (!phone) {
        const phoneMatch2 = text.match(/\(\d{3}\)\s*\d{3}-\d{4}/);
        if (phoneMatch2) phone = phoneMatch2[0];
      }

      // Studio name from title
      const titleMatch = text.match(/<title>([^<]+)<\/title>/i);
      if (titleMatch) {
        studioName = titleMatch[1].replace(/\s*\|.*$/, '').trim();
      }

      // Capitalize slug for company name if city not found
      const displayCity = city || slug.replace(/([a-z])([A-Z])/g, '$1 $2')
        .replace(/^./, c => c.toUpperCase());

      writeLead({
        email,
        first_name: '',
        last_name: '',
        company: `Club Pilates ${displayCity}`,
        phone,
        city,
        state,
        country: country || (state && state.length === 2 ? 'US' : ''),
        source: 'club-pilates',
        website: url,
      });

      scraped++;
      if (scraped % 20 === 0) {
        console.log(`  Scraped ${scraped}/${locationUrls.length} studios...`);
      }
    } catch (err) {
      errors++;
    }
  }, 3);

  console.log(`  Done: ${scraped} studios scraped, ${errors} errors`);
  return scraped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. SYLVAN LEARNING — WP REST API + Sitemap page scrape
//    Pages have: phone, address (no email in directory)
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeSylvan() {
  console.log('\n🔵 SYLVAN LEARNING');
  console.log('─'.repeat(60));

  // Step 1: Get all location URLs from both sitemaps
  console.log('  Fetching sitemaps...');
  const urls = new Set();

  for (const sitemapUrl of [
    'https://www.sylvanlearning.com/jpl_franchise-sitemap.xml',
    'https://www.sylvanlearning.com/jpl_franchise-sitemap2.xml'
  ]) {
    try {
      const { body: xml } = await fetchWithRetry(sitemapUrl);
      const $ = cheerio.load(xml, { xmlMode: true });
      $('url > loc').each((_, el) => {
        const url = $(el).text().trim();
        // Only individual center pages (4+ path segments with city name)
        if (url.match(/\/locations\/\w+\/\w+\/[a-z]+-tutoring\/[a-z]/)) {
          urls.add(url);
        }
      });
    } catch (err) {
      console.log(`  Warning: Failed to fetch ${sitemapUrl}: ${err.message}`);
    }
  }

  const locationUrls = [...urls];
  console.log(`  Found ${locationUrls.length} center URLs in sitemaps`);

  // Step 2: Scrape each center page for phone/address
  let scraped = 0;
  let errors = 0;

  await processBatch(locationUrls, async (url) => {
    try {
      const { body: html } = await fetchWithRetry(url);

      let phone = '';
      let city = '';
      let state = '';
      let country = '';
      let address = '';

      // Extract from JSON-LD or page content
      const phoneMatch = html.match(/"telephone"\s*:\s*"([^"]+)"/i)
        || html.match(/tel:([0-9-()+ ]+)/i)
        || html.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
      if (phoneMatch) {
        phone = normalizePhone(phoneMatch[0].replace(/tel:/i, ''));
      }

      const cityMatch = html.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
      const stateMatch = html.match(/"addressRegion"\s*:\s*"([^"]+)"/i);
      const countryMatch = html.match(/"addressCountry"\s*:\s*"([^"]+)"/i);

      if (cityMatch) city = cityMatch[1];
      if (stateMatch) state = stateMatch[1];
      if (countryMatch) country = countryMatch[1];

      // Parse country from URL
      const urlCountry = url.match(/\/locations\/(us|ca|international)\//)?.[1];
      if (!country && urlCountry) {
        if (urlCountry === 'us') country = 'US';
        else if (urlCountry === 'ca') country = 'CA';
        else if (urlCountry === 'international') {
          // Try to determine country from URL path
          const intlCountry = url.match(/\/international\/(\w+)\//)?.[1];
          country = intlCountry ? intlCountry.toUpperCase() : 'INTL';
        }
      }

      // Parse state from URL if not in page
      if (!state) {
        const urlState = url.match(/\/locations\/\w+\/(\w+)\//)?.[1];
        if (urlState) state = urlState.toUpperCase();
      }

      // Center name from URL
      const centerSlug = url.match(/\/([a-z-]+-tutoring)\//)?.[1] || '';
      const centerCity = centerSlug.replace(/-tutoring$/, '').replace(/-/g, ' ');
      if (!city) city = centerCity.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

      writeLead({
        email: '',
        first_name: '',
        last_name: '',
        company: `Sylvan Learning ${city}`,
        phone,
        city,
        state,
        country: country || '',
        source: 'sylvan-learning',
        website: url,
      });

      scraped++;
      if (scraped % 50 === 0) {
        console.log(`  Scraped ${scraped}/${locationUrls.length} centers...`);
      }
    } catch (err) {
      errors++;
    }
  }, 3);

  console.log(`  Done: ${scraped} centers scraped, ${errors} errors`);
  return scraped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. EUROPEAN WAX CENTER — RLS sitemap → Individual page scrape
//    Pages have: phone, address (no email)
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeEuropeanWax() {
  console.log('\n🟡 EUROPEAN WAX CENTER');
  console.log('─'.repeat(60));

  // Step 1: Get all location URLs from sitemap
  console.log('  Fetching sitemap...');
  const { body: sitemapXml } = await fetchWithRetry('https://locations.waxcenter.com/sitemap.xml');
  const $ = cheerio.load(sitemapXml, { xmlMode: true });

  const locationUrls = [];
  $('url > loc').each((_, el) => {
    const url = $(el).text().trim();
    // Only individual location pages (ending in .html)
    if (url.match(/\/[a-z]+\/[a-z-]+\/[a-z0-9-]+\.html$/)) {
      locationUrls.push(url);
    }
  });

  console.log(`  Found ${locationUrls.length} location URLs in sitemap`);

  // Step 2: Scrape each location page
  let scraped = 0;
  let errors = 0;

  await processBatch(locationUrls, async (url) => {
    try {
      const { body: html } = await fetchWithRetry(url);

      let phone = '';
      let city = '';
      let state = '';
      let address = '';
      let locationName = '';

      // Phone
      const phoneMatch = html.match(/"telephone"\s*:\s*"([^"]+)"/i)
        || html.match(/tel:\+?1?([0-9-()+ ]+)/i)
        || html.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
      if (phoneMatch) {
        phone = normalizePhone(phoneMatch[0].replace(/tel:\+?1?/i, ''));
      }

      // Address from JSON-LD
      const streetMatch = html.match(/"streetAddress"\s*:\s*"([^"]+)"/i);
      const cityMatch = html.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
      const stateMatch = html.match(/"addressRegion"\s*:\s*"([^"]+)"/i);

      if (streetMatch) address = streetMatch[1];
      if (cityMatch) city = cityMatch[1];
      if (stateMatch) state = stateMatch[1];

      // Location name - extract from JSON-LD or clean up from title
      const nameMatch = html.match(/"name"\s*:\s*"([^"]*European Wax Center[^"]*)"/i)
        || html.match(/<h1[^>]*>([^<]+)<\/h1>/i);
      if (nameMatch) {
        locationName = nameMatch[1].trim();
      }

      // Parse state from URL if not found
      if (!state) {
        const urlState = url.match(/waxcenter\.com\/([a-z]{2})\//)?.[1];
        if (urlState) state = urlState.toUpperCase();
      }

      // Parse city from URL
      if (!city) {
        const urlCity = url.match(/waxcenter\.com\/[a-z]{2}\/([a-z-]+)\//)?.[1];
        if (urlCity) city = urlCity.replace(/-/g, ' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
      }

      // Extract location suffix from URL slug
      const locSlug = url.match(/\/([a-z0-9-]+)\.html$/)?.[1] || '';
      const locSuffix = locSlug.replace(/-\d+$/, '').replace(city?.toLowerCase().replace(/\s+/g, '') || 'xxx', '').replace(/^-/, '').replace(/-/g, ' ').trim();
      const displayName = locSuffix
        ? `${city}${locSuffix ? ' - ' + locSuffix.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ') : ''}`
        : city;

      writeLead({
        email: '',
        first_name: '',
        last_name: '',
        company: `European Wax Center ${displayName || city}`,
        phone,
        city,
        state,
        country: 'US',
        source: 'european-wax-center',
        website: url,
      });

      scraped++;
      if (scraped % 100 === 0) {
        console.log(`  Scraped ${scraped}/${locationUrls.length} locations...`);
      }
    } catch (err) {
      errors++;
    }
  }, 5);

  console.log(`  Done: ${scraped} locations scraped, ${errors} errors`);
  return scraped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. THE LEARNING EXPERIENCE — Sitemap → Individual page scrape
//    Pages have: phone, address
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeTheLearningExperience() {
  console.log('\n🟢 THE LEARNING EXPERIENCE');
  console.log('─'.repeat(60));

  // Step 1: Get all center URLs from centers sitemap
  console.log('  Fetching centers sitemap...');
  const { body: sitemapXml } = await fetchWithRetry('https://thelearningexperience.com/centers-sitemap.xml');
  const $ = cheerio.load(sitemapXml, { xmlMode: true });

  const centerUrls = [];
  $('url > loc').each((_, el) => {
    const url = $(el).text().trim();
    if (url.includes('/centers/')) {
      centerUrls.push(url);
    }
  });

  console.log(`  Found ${centerUrls.length} center URLs in sitemap`);

  // Step 2: Scrape each center page
  let scraped = 0;
  let errors = 0;

  await processBatch(centerUrls, async (url) => {
    try {
      const { body: html } = await fetchWithRetry(url);

      let phone = '';
      let city = '';
      let state = '';
      let address = '';
      let centerName = '';

      // Phone
      const phoneMatch = html.match(/"telephone"\s*:\s*"([^"]+)"/i)
        || html.match(/tel:\+?1?([0-9-()+ ]+)/i)
        || html.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
      if (phoneMatch) {
        phone = normalizePhone(phoneMatch[0].replace(/tel:\+?1?/i, ''));
      }

      // Address from JSON-LD
      const cityMatch = html.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
      const stateMatch = html.match(/"addressRegion"\s*:\s*"([^"]+)"/i);

      if (cityMatch) city = cityMatch[1];
      if (stateMatch) state = stateMatch[1];

      // Center name
      const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
      if (titleMatch) {
        centerName = titleMatch[1]
          .replace(/The Learning Experience/i, '').replace(/\|/g, '').replace(/-/g, '').trim();
      }

      // Parse from URL
      const urlSlug = url.match(/\/centers\/([a-z-]+)\/?$/)?.[1] || '';
      const slugCity = urlSlug.replace(/-/g, ' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
      if (!city) city = slugCity;

      // Try to get state from URL if present (e.g., /centers/nj/city-name)
      const urlStateMatch = url.match(/\/centers\/([a-z]{2})\/[a-z]/);
      if (urlStateMatch && !state) {
        state = urlStateMatch[1].toUpperCase();
      }

      writeLead({
        email: '',
        first_name: '',
        last_name: '',
        company: `The Learning Experience ${city || slugCity}`,
        phone,
        city,
        state,
        country: 'US',
        source: 'the-learning-experience',
        website: url,
      });

      scraped++;
      if (scraped % 50 === 0) {
        console.log(`  Scraped ${scraped}/${centerUrls.length} centers...`);
      }
    } catch (err) {
      errors++;
    }
  }, 3);

  console.log(`  Done: ${scraped} centers scraped, ${errors} errors`);
  return scraped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═'.repeat(60));
  console.log('  FRANCHISE LOCATION SCRAPER');
  console.log('═'.repeat(60));
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log(`  Started: ${new Date().toISOString()}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Open write stream
  writeStream = fs.createWriteStream(OUTPUT_FILE);
  writeStream.write(CSV_HEADER);

  const results = {};

  // Run all scrapers sequentially (to be polite and avoid overwhelming any single target)
  try {
    results.orangetheory = await scrapeOrangetheory();
  } catch (err) {
    console.log(`  ERROR Orangetheory: ${err.message}`);
    results.orangetheory = 0;
  }

  try {
    results.clubPilates = await scrapeClubPilates();
  } catch (err) {
    console.log(`  ERROR Club Pilates: ${err.message}`);
    results.clubPilates = 0;
  }

  try {
    results.sylvan = await scrapeSylvan();
  } catch (err) {
    console.log(`  ERROR Sylvan: ${err.message}`);
    results.sylvan = 0;
  }

  try {
    results.europeanWax = await scrapeEuropeanWax();
  } catch (err) {
    console.log(`  ERROR European Wax: ${err.message}`);
    results.europeanWax = 0;
  }

  try {
    results.learningExperience = await scrapeTheLearningExperience();
  } catch (err) {
    console.log(`  ERROR The Learning Experience: ${err.message}`);
    results.learningExperience = 0;
  }

  // Close write stream
  writeStream.end();

  // Summary
  console.log('\n' + '═'.repeat(60));
  console.log('  FINAL SUMMARY');
  console.log('═'.repeat(60));
  console.log(`  Orangetheory Fitness:    ${results.orangetheory} locations`);
  console.log(`  Club Pilates:            ${results.clubPilates} locations`);
  console.log(`  Sylvan Learning:         ${results.sylvan} locations`);
  console.log(`  European Wax Center:     ${results.europeanWax} locations`);
  console.log(`  The Learning Experience: ${results.learningExperience} locations`);
  console.log('─'.repeat(60));
  console.log(`  TOTAL LEADS: ${totalLeads}`);
  console.log(`  WITH EMAIL:  ${totalEmails}`);
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log(`  Finished: ${new Date().toISOString()}`);
  console.log('═'.repeat(60));
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
