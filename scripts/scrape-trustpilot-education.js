#!/usr/bin/env node
/**
 * Trustpilot Education Consultants Scraper
 *
 * Scrapes education-related businesses from Trustpilot across 4 category feeds:
 *   - www.trustpilot.com/categories/educational_consultant (US, ~53 pages)
 *   - www.trustpilot.com/categories/education_services (US, ~461 pages)
 *   - uk.trustpilot.com/categories/educational_consultant (UK, ~57 pages)
 *   - uk.trustpilot.com/categories/education_services (UK, ~325 pages)
 *
 * Strategy:
 *   Phase 1: Scrape category listing pages to collect business profile slugs (domains)
 *   Phase 2: Visit each business profile page to extract JSON-LD structured data
 *            (phone, email, website, address, rating, review count)
 *   Dedup by domain across all categories
 *
 * Usage:
 *   node scripts/scrape-trustpilot-education.js --test    # 2 pages per category, 10 profiles
 *   node scripts/scrape-trustpilot-education.js --all     # All pages + all profiles
 *   node scripts/scrape-trustpilot-education.js --resume  # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'education-consultants-trustpilot.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'trustpilot-education-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'rating', 'review_count',
  'niche', 'source', 'profile_url',
];

// Category feeds to scrape
const CATEGORIES = [
  {
    name: 'US Educational Consultant',
    host: 'www.trustpilot.com',
    path: '/categories/educational_consultant',
    maxPages: 53,
  },
  {
    name: 'US Education Services',
    host: 'www.trustpilot.com',
    path: '/categories/education_services',
    maxPages: 100,  // Cap at 100 pages (~2000 businesses) — full 461 is too broad
  },
  {
    name: 'UK Educational Consultant',
    host: 'uk.trustpilot.com',
    path: '/categories/educational_consultant',
    maxPages: 57,
  },
  {
    name: 'UK Education Services',
    host: 'uk.trustpilot.com',
    path: '/categories/education_services',
    maxPages: 100,  // Cap at 100 pages (~2000 businesses)
  },
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  // Handle arrays (some JSON-LD returns arrays)
  if (Array.isArray(rawPhone)) rawPhone = rawPhone[0] || '';
  // If multiple phones separated by comma, take the first one
  let phone = String(rawPhone).split(',')[0].trim();
  const digits = phone.replace(/\D/g, '');

  // Reject obviously garbage numbers (too many digits = concatenated junk)
  if (digits.length > 15) return '';

  // US/CA: 10 digits
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  // US/CA with country code
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  // UK: starts with 44
  if (digits.length >= 10 && digits.length <= 13 && digits.startsWith('44')) {
    return '+44 ' + digits.slice(2);
  }
  // International: starts with +
  if (phone.startsWith('+') && digits.length >= 8 && digits.length <= 15) {
    return '+' + digits;
  }
  // At least 7 digits, return as-is
  if (digits.length >= 7 && digits.length <= 15) {
    return phone;
  }
  // Return empty for anything else
  return '';
}

function parseName(firmName) {
  // Education businesses rarely have person names as firm names.
  // Only attempt to extract names from very clear "Person Name Consulting" patterns.
  // This is deliberately conservative to avoid false positives.
  if (!firmName) return { first_name: '', last_name: '' };

  // Most education businesses are brands, not person-named firms.
  // Skip name extraction entirely -- it produces too many false positives
  // (e.g., "Divi Life" -> "Divi" "Life", "Industry Rockstar" -> "Industry" "Rockstar").
  return { first_name: '', last_name: '' };
}

function parseLocation(locationStr, addressObj) {
  let city = '', state = '', country = '';

  // From JSON-LD address object
  if (addressObj) {
    city = addressObj.addressLocality || '';
    state = addressObj.addressRegion || '';
    const countryCode = addressObj.addressCountry || '';
    if (typeof countryCode === 'object') {
      country = countryCode.name || countryCode['@id'] || '';
    } else {
      country = countryCode;
    }
  }

  // From listing location string (fallback)
  if (!city && locationStr) {
    const loc = locationStr.trim();
    // "Seattle, WA" or "London, UK" or just "United States"
    if (/^(United\s+States|US|USA)$/i.test(loc)) {
      country = 'US';
    } else if (/^(United\s+Kingdom|UK|Great\s+Britain|GB)$/i.test(loc)) {
      country = 'UK';
    } else {
      // Try "City, State/Country"
      const parts = loc.split(',').map(s => s.trim());
      if (parts.length >= 2) {
        city = parts[0];
        const rest = parts.slice(1).join(', ').trim();
        // Check if last part is country
        if (/United\s+States|US|USA/i.test(rest)) {
          country = 'US';
          // Check for state abbreviation
          const stateMatch = rest.match(/^([A-Z]{2})/);
          if (stateMatch) state = stateMatch[1];
        } else if (/United\s+Kingdom|UK|Great\s+Britain|GB/i.test(rest)) {
          country = 'UK';
        } else if (/Canada|CA/i.test(rest)) {
          country = 'CA';
        } else if (/Australia|AU/i.test(rest)) {
          country = 'AU';
        } else {
          // Could be "City, STATE" for US
          if (/^[A-Z]{2}$/.test(rest)) {
            state = rest;
            country = 'US';
          } else {
            country = rest;
          }
        }
      } else {
        city = loc;
      }
    }
  }

  // Normalize country
  if (/United\s+States|USA/i.test(country)) country = 'US';
  if (/United\s+Kingdom|Great\s+Britain|GB/i.test(country)) country = 'UK';
  if (/Canada/i.test(country)) country = 'CA';
  if (/Australia/i.test(country)) country = 'AU';

  return { city, state, country };
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpsGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);

    const options = {
      hostname: parsedUrl.hostname,
      port: 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(url, options, (res) => {
      // Handle redirects
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `https://${parsedUrl.hostname}${redirectUrl}`;
        }
        return httpsGet(redirectUrl, retries).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 8000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpsGet(url, retries - 1)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(3000).then(() => httpsGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpsGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Phase 1: Scrape Category Listing Pages ───────────────────────────────────

function parseListingPage(html) {
  const $ = cheerio.load(html);
  const businesses = [];

  // Extract business profile links: href="/review/domain.com"
  $('a[href^="/review/"]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;

    const domainSlug = href.replace('/review/', '').replace(/\/$/, '');
    // Skip duplicates within same page
    if (businesses.find(b => b.domainSlug === domainSlug)) return;

    // Try to extract name and location from card context
    const card = $(el);
    let name = '';

    // The business name is typically in a heading or strong element
    const nameEl = card.find('h3, h2, [class*="businessName"], [data-business-unit-name]').first();
    if (nameEl.length) {
      name = nameEl.text().trim();
    }

    // Try to get any text content for name if not found
    if (!name) {
      const allText = card.text().trim();
      const lines = allText.split('\n').map(l => l.trim()).filter(Boolean);
      if (lines.length > 0) {
        name = lines[0];
      }
    }

    businesses.push({
      domainSlug,
      name: name || domainSlug,
      profileUrl: `https://www.trustpilot.com/review/${domainSlug}`,
    });
  });

  // Detect if there's a next page
  const hasNextPage = $('a[href*="page="]').length > 0 ||
                      $('a[name="pagination-button-next"]').length > 0 ||
                      $('[class*="next"]').length > 0;

  return { businesses, hasNextPage };
}

async function scrapeCategory(category, maxPages) {
  const allBusinesses = [];
  const seenDomains = new Set();

  for (let page = 1; page <= maxPages; page++) {
    const pageParam = page > 1 ? `?page=${page}` : '';
    const url = `https://${category.host}${category.path}${pageParam}`;
    log(`  [Page ${page}/${maxPages}] ${url}`);

    try {
      const { statusCode, body } = await httpsGet(url);

      if (statusCode === 404) {
        log(`    404 - no more pages`);
        break;
      }

      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping page`);
        await randomDelay();
        continue;
      }

      const { businesses } = parseListingPage(body);
      let newCount = 0;

      for (const biz of businesses) {
        if (seenDomains.has(biz.domainSlug)) continue;
        seenDomains.add(biz.domainSlug);
        allBusinesses.push(biz);
        newCount++;
      }

      log(`    Found ${businesses.length} listings, ${newCount} new`);

      if (businesses.length === 0) {
        log(`    Empty page - stopping`);
        break;
      }

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  return allBusinesses;
}

// ── Phase 2: Scrape Individual Profile Pages ─────────────────────────────────

function parseProfilePage(html, domainSlug) {
  const $ = cheerio.load(html);
  const data = {
    firm_name: '',
    email: '',
    phone: '',
    website: '',
    city: '',
    state: '',
    country: '',
    rating: '',
    review_count: '',
    description: '',
  };

  // Extract JSON-LD structured data
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());

      // Helper to extract fields from a LocalBusiness object
      const extractLocalBusiness = (obj) => {
        if (!data.firm_name) data.firm_name = obj.name || '';
        if (!data.email) data.email = obj.email || '';
        if (!data.phone) data.phone = obj.telephone || '';
        if (!data.description) data.description = obj.description || '';

        // Website: prefer sameAs (actual business URL), NOT url (Trustpilot page)
        if (!data.website) {
          if (obj.sameAs) {
            data.website = typeof obj.sameAs === 'string' ? obj.sameAs : (Array.isArray(obj.sameAs) ? obj.sameAs[0] : '');
          }
        }

        if (obj.address && !data.city) {
          const loc = parseLocation('', obj.address);
          data.city = loc.city;
          data.state = loc.state;
          data.country = loc.country;
        }

        if (obj.aggregateRating && !data.rating) {
          data.rating = obj.aggregateRating.ratingValue || '';
          data.review_count = obj.aggregateRating.reviewCount || '';
        }
      };

      // Look for LocalBusiness type
      if (json['@type'] === 'LocalBusiness') {
        extractLocalBusiness(json);
      }

      // Also check @graph array
      if (Array.isArray(json['@graph'])) {
        for (const item of json['@graph']) {
          if (item['@type'] === 'LocalBusiness') {
            extractLocalBusiness(item);
          }
        }
      }
    } catch (e) {
      // Skip malformed JSON-LD
    }
  });

  // Fallback: try to extract from page meta/content if JSON-LD missed data
  if (!data.firm_name) {
    const ogTitle = $('meta[property="og:title"]').attr('content') || '';
    // "Company Name Reviews | Read Customer Service Reviews of domain.com"
    const titleMatch = ogTitle.match(/^(.+?)\s+Reviews/i);
    if (titleMatch) data.firm_name = titleMatch[1].trim();
  }

  if (!data.website) {
    // The domain slug IS the website domain
    data.website = `https://${domainSlug}`;
  }

  // If website still points to trustpilot, fallback to domain slug
  if (data.website && data.website.includes('trustpilot.com')) {
    data.website = `https://${domainSlug}`;
  }

  return data;
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedProfiles: [], leads: [], collectedDomains: [] };
}

function saveProgress(completedProfiles, leads, collectedDomains) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedProfiles,
    leads,
    collectedDomains,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const isResume = args.includes('--resume');

  if (!isTest && !isAll && !isResume) {
    console.log('Trustpilot Education Consultants Scraper');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-trustpilot-education.js --test     # Test (2 pages/category, 10 profiles)');
    console.log('  node scripts/scrape-trustpilot-education.js --all      # Full scrape (all pages + all profiles)');
    console.log('  node scripts/scrape-trustpilot-education.js --resume   # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('Trustpilot Education Consultants Scraper');
  log(`Mode: ${isTest ? 'TEST' : isResume ? 'RESUME' : 'FULL'}`);
  log('');

  let allLeads = [];
  let completedProfiles = [];
  let collectedDomains = [];
  const seenDomains = new Set();

  // Resume support
  if (isResume) {
    const progress = loadProgress();
    if (progress.completedProfiles.length > 0) {
      completedProfiles = progress.completedProfiles;
      allLeads = progress.leads || [];
      collectedDomains = progress.collectedDomains || [];
      for (const lead of allLeads) {
        seenDomains.add(lead.domain);
      }
      for (const d of completedProfiles) {
        seenDomains.add(d);
      }
      log(`Resuming: ${completedProfiles.length} profiles done, ${allLeads.length} leads`);
    }
  }

  // ── Phase 1: Collect business domains from category pages ──
  if (collectedDomains.length === 0 || !isResume) {
    log('=== PHASE 1: Collecting business listings from category pages ===');
    log('');

    const allBusinesses = [];
    const globalSeenDomains = new Set();

    for (const cat of CATEGORIES) {
      const maxPages = isTest ? 2 : cat.maxPages;
      log(`Category: ${cat.name} (up to ${maxPages} pages)`);

      const businesses = await scrapeCategory(cat, maxPages);

      let newCount = 0;
      for (const biz of businesses) {
        if (globalSeenDomains.has(biz.domainSlug)) continue;
        globalSeenDomains.add(biz.domainSlug);
        allBusinesses.push(biz);
        newCount++;
      }

      log(`  Total from ${cat.name}: ${businesses.length} (${newCount} new unique)`);
      log('');
    }

    collectedDomains = allBusinesses.map(b => b.domainSlug);
    log(`Phase 1 complete: ${collectedDomains.length} unique businesses found`);
    log('');
  }

  // ── Phase 2: Scrape individual profile pages ──
  log('=== PHASE 2: Scraping individual business profiles ===');
  log('');

  const domainsToScrape = collectedDomains.filter(d => !completedProfiles.includes(d));

  // In test mode, limit to 10 profiles
  const profileLimit = isTest ? 10 : domainsToScrape.length;
  const domainsSlice = domainsToScrape.slice(0, profileLimit);

  log(`Profiles to scrape: ${domainsSlice.length}${isTest ? ' (test limit)' : ''}`);
  log('');

  let processed = 0;
  let profileErrors = 0;

  for (const domainSlug of domainsSlice) {
    processed++;
    const profileUrl = `https://www.trustpilot.com/review/${domainSlug}`;
    log(`[${processed}/${domainsSlice.length}] ${domainSlug}`);

    try {
      const { statusCode, body } = await httpsGet(profileUrl);

      if (statusCode !== 200 || !body) {
        log(`  HTTP ${statusCode} - skipping`);
        profileErrors++;
        completedProfiles.push(domainSlug);
        await randomDelay();
        continue;
      }

      const data = parseProfilePage(body, domainSlug);

      if (!data.firm_name && !data.website) {
        log(`  No data found - skipping`);
        profileErrors++;
        completedProfiles.push(domainSlug);
        await randomDelay();
        continue;
      }

      const domain = extractDomain(data.website || domainSlug);
      const { first_name, last_name } = parseName(data.firm_name);

      const lead = {
        first_name,
        last_name,
        firm_name: data.firm_name,
        title: '',
        email: data.email,
        phone: formatPhone(data.phone),
        website: data.website || `https://${domainSlug}`,
        domain,
        city: data.city,
        state: data.state,
        country: data.country,
        rating: data.rating,
        review_count: data.review_count,
        niche: 'education',
        source: 'trustpilot',
        profile_url: profileUrl,
      };

      allLeads.push(lead);
      completedProfiles.push(domainSlug);

      const parts = [];
      if (data.firm_name) parts.push(data.firm_name);
      if (lead.phone) parts.push(`ph:${lead.phone}`);
      if (data.email) parts.push(`em:${data.email}`);
      if (data.rating) parts.push(`${data.rating}*`);
      if (data.review_count) parts.push(`${data.review_count} reviews`);
      if (data.city) parts.push(data.city);
      log(`  OK ${parts.join(' | ')}`);

      // Save progress every 20 profiles
      if (processed % 20 === 0 && !isTest) {
        saveProgress(completedProfiles, allLeads, collectedDomains);
        writeCSV(allLeads, OUT_FILE);
        log(`  [Progress saved: ${allLeads.length} total leads]`);
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
      profileErrors++;
      completedProfiles.push(domainSlug);
    }

    await randomDelay();
  }

  // ── Final Output ──
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) {
    saveProgress(completedProfiles, allLeads, collectedDomains);
  }

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withRating = allLeads.filter(l => l.rating).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueCountries = [...new Set(allLeads.map(l => l.country).filter(Boolean))];
  const uniqueCities = new Set(allLeads.filter(l => l.city).map(l => l.city)).size;

  // Rating distribution
  const rated = allLeads.filter(l => l.rating);
  const avgRating = rated.length > 0
    ? (rated.reduce((sum, l) => sum + parseFloat(l.rating), 0) / rated.length).toFixed(2)
    : 'N/A';

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Profiles scraped: ${processed} (${profileErrors} errors)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With rating: ${withRating} (avg: ${avgRating})`);
  log(`With parsed name: ${withName}`);
  log(`Countries: ${uniqueCountries.join(', ') || 'none'}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && profileErrors === 0 && domainsSlice.length === domainsToScrape.length) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
