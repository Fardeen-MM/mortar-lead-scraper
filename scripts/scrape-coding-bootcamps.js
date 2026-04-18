#!/usr/bin/env node
/**
 * Scrape coding bootcamps from Course Report (coursereport.com).
 *
 * Course Report is THE directory of coding bootcamps — 864 schools listed,
 * each profile page contains JSON-LD structured data with email, website,
 * rating, and price range.
 *
 * Strategy:
 *   Phase 1: Paginate /schools?page=1..35 to collect all school slugs
 *   Phase 2: Visit each /schools/{slug} profile page, extract JSON-LD + HTML data
 *   Phase 3: Write CSV
 *
 * Polite scraping:
 *   - robots.txt specifies Crawl-delay: 10
 *   - We honour a 10s delay between profile fetches (configurable via --delay)
 *   - Listing pages use a 3s delay (lighter pages)
 *
 * Features:
 *   - Auto-saves progress CSV every 50 profiles
 *   - Resume support (--resume skips already-scraped slugs)
 *   - Test mode (--test scrapes first 2 listing pages + 10 profiles)
 *   - Retry with exponential backoff on transient errors
 *
 * Usage:
 *   node scripts/scrape-coding-bootcamps.js
 *   node scripts/scrape-coding-bootcamps.js --test
 *   node scripts/scrape-coding-bootcamps.js --resume
 *   node scripts/scrape-coding-bootcamps.js --delay=12000
 *   node scripts/scrape-coding-bootcamps.js --start-page=10 --end-page=20
 *
 * Output: output/coding-bootcamps.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE = !!args.test;
const RESUME = !!args.resume;
const PROFILE_DELAY_MS = parseInt(args.delay) || 10000;  // 10s — honour robots.txt Crawl-delay
const LIST_DELAY_MS = 3000;                               // 3s between listing pages
const START_PAGE = parseInt(args['start-page']) || 1;
const END_PAGE = parseInt(args['end-page']) || 999;       // auto-stops when no results
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 5000;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'coding-bootcamps.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'coding-bootcamps-progress.csv');

const BASE_URL = 'https://www.coursereport.com';

const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';

// ---------------------------------------------------------------------------
// HTTP helper (Node.js built-in, zero deps beyond cheerio)
// ---------------------------------------------------------------------------

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const mod = urlObj.protocol === 'https:' ? https : require('http');

    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Referer': 'https://www.coursereport.com/',
      },
    }, (res) => {
      // Follow redirects (up to 5)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${urlObj.protocol}//${urlObj.host}${res.headers.location}`;
        fetchUrl(redirectUrl).then(resolve).catch(reject);
        res.resume(); // drain response
        return;
      }

      if (res.statusCode === 429) {
        res.resume();
        reject(new Error('RATE_LIMITED'));
        return;
      }

      if (res.statusCode >= 400) {
        res.resume();
        reject(new Error(`HTTP_${res.statusCode}`));
        return;
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('TIMEOUT')); });
  });
}

async function fetchWithRetry(url, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fetchUrl(url);
    } catch (err) {
      const isRetryable = ['RATE_LIMITED', 'TIMEOUT', 'ECONNRESET', 'ECONNREFUSED']
        .some(code => err.message.includes(code));

      if (attempt < retries && isRetryable) {
        const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1);
        log(`  [RETRY ${attempt}/${retries}] ${err.message} — waiting ${delay / 1000}s`);
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function csvEscape(val) {
  const str = (val || '').toString().trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const columns = CSV_HEADER.split(',');
  const rows = leads.map(lead =>
    columns.map(col => csvEscape(lead[col])).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [CSV_HEADER, ...rows].join('\n'));
}

function loadExistingSlugs() {
  if (!fs.existsSync(PROGRESS_FILE)) return new Set();
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return new Set();

    // The source column contains "coursereport" and company is the school name,
    // but we also store the slug in the profile_url-ish way via the source.
    // Actually, we just track which companies we've already processed.
    // Better approach: read existing leads to populate the set.
    const headers = lines[0].split(',');
    const sourceIdx = headers.indexOf('source');
    const companyIdx = headers.indexOf('company');

    const slugs = new Set();
    for (let i = 1; i < lines.length; i++) {
      // We store slug in the source field as "coursereport:{slug}"
      const parts = parseCSVLine(lines[i]);
      const source = parts[sourceIdx] || '';
      const match = source.match(/^coursereport:(.+)$/);
      if (match) slugs.add(match[1]);
    }
    return slugs;
  } catch {
    return new Set();
  }
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current);
  return result;
}

function loadExistingLeads() {
  if (!fs.existsSync(PROGRESS_FILE)) return [];
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return [];
    const headers = lines[0].split(',');
    const leads = [];
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
    }
    return leads;
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Phase 1: Collect all school slugs from listing pages
// ---------------------------------------------------------------------------

async function collectSlugs() {
  const slugs = [];
  const maxPage = TEST_MODE ? Math.min(2, END_PAGE) : END_PAGE;

  log('Phase 1: Collecting school slugs from listing pages...');

  for (let page = START_PAGE; page <= maxPage; page++) {
    const url = `${BASE_URL}/schools?page=${page}`;
    log(`  Fetching page ${page}...`);

    try {
      const html = await fetchWithRetry(url);
      const $ = cheerio.load(html);

      // Extract school slugs from links matching /schools/{slug}
      const pageSlugs = [];
      $('a[href^="/schools/"]').each((_, el) => {
        const href = $(el).attr('href');
        // Match /schools/{slug} but NOT /schools?page= or /schools/{slug}/admin etc.
        const match = href.match(/^\/schools\/([a-z0-9][a-z0-9\-]+[a-z0-9])$/);
        if (match && match[1] !== 'schools') {
          pageSlugs.push(match[1]);
        }
      });

      // Deduplicate slugs from this page (each school may appear in multiple links)
      const uniquePageSlugs = [...new Set(pageSlugs)];

      if (uniquePageSlugs.length === 0) {
        log(`  Page ${page}: no schools found — reached end of directory`);
        break;
      }

      slugs.push(...uniquePageSlugs);
      log(`  Page ${page}: found ${uniquePageSlugs.length} schools (total: ${slugs.length})`);

      // Check if we've reached the last page
      // Course Report shows "Displaying schools X-Y of Z in total"
      const displayText = $('body').text();
      const totalMatch = displayText.match(/of\s+(\d+)\s+in total/i);
      if (totalMatch) {
        const total = parseInt(totalMatch[1]);
        if (slugs.length >= total) {
          log(`  Reached all ${total} schools`);
          break;
        }
      }

    } catch (err) {
      log(`  [ERROR] Page ${page}: ${err.message}`);
      // Continue to next page on error
    }

    if (page < maxPage) await sleep(LIST_DELAY_MS);
  }

  // Final dedup across all pages
  const uniqueSlugs = [...new Set(slugs)];
  log(`Phase 1 complete: ${uniqueSlugs.length} unique school slugs collected`);
  return uniqueSlugs;
}

// ---------------------------------------------------------------------------
// Phase 2: Scrape individual school profiles
// ---------------------------------------------------------------------------

/**
 * Parse a school profile page and extract lead data.
 *
 * Data sources on each profile page:
 *   1. JSON-LD (schema.org LocalBusiness or EducationalOrganization)
 *      → email, name, aggregateRating, priceRange
 *   2. HTML elements
 *      → website link, phone, locations, social media
 */
function parseSchoolProfile($, slug) {
  const lead = {
    email: '',
    first_name: '',
    last_name: '',
    company: '',
    phone: '',
    city: '',
    state: '',
    country: '',
    source: `coursereport:${slug}`,
  };

  // --- Extract JSON-LD ---
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());

      // Handle single object or array
      const items = Array.isArray(json) ? json : [json];

      for (const item of items) {
        // Look for LocalBusiness or EducationalOrganization
        const type = item['@type'] || '';
        if (['LocalBusiness', 'EducationalOrganization', 'Organization', 'School']
            .some(t => type.includes(t))) {

          if (item.email && !lead.email) {
            lead.email = item.email.replace(/^mailto:/i, '').trim().toLowerCase();
          }
          if (item.name && !lead.company) {
            lead.company = item.name.trim();
          }
          if (item.telephone && !lead.phone) {
            lead.phone = item.telephone.trim();
          }
          if (item.address) {
            const addr = item.address;
            if (addr.addressLocality && !lead.city) lead.city = addr.addressLocality;
            if (addr.addressRegion && !lead.state) lead.state = addr.addressRegion;
            if (addr.addressCountry && !lead.country) lead.country = addr.addressCountry;
          }
        }
      }
    } catch {
      // Malformed JSON-LD — skip
    }
  });

  // --- Extract school name from page if not in JSON-LD ---
  if (!lead.company) {
    // Try h1 or og:title
    const h1 = $('h1').first().text().trim();
    if (h1) {
      lead.company = h1.replace(/\s*Reviews?\s*$/i, '').trim();
    }
  }

  // --- Extract website URL ---
  // Course Report links to school websites — look for external links near "Website" label
  // Pattern: an anchor tag with href pointing to the school's actual domain
  let website = '';

  // Look for explicit "Website" link
  $('a').each((_, el) => {
    const text = $(el).text().trim().toLowerCase();
    const href = $(el).attr('href') || '';
    if ((text === 'website' || text.includes('visit website') || text.includes('visit school'))
        && href.startsWith('http') && !href.includes('coursereport.com')) {
      if (!website) website = href;
    }
  });

  // Also look for links with the school's domain that aren't social media
  if (!website) {
    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes('coursereport.com')) return;
      if (href.includes('twitter.com') || href.includes('facebook.com') ||
          href.includes('linkedin.com') || href.includes('github.com') ||
          href.includes('youtube.com') || href.includes('instagram.com') ||
          href.includes('quora.com') || href.includes('reddit.com')) return;
      // Likely the school's website
      if (!website) website = href;
    });
  }

  // --- Extract phone from page text ---
  if (!lead.phone) {
    const bodyText = $('body').text();
    // US/CA phone pattern
    const phoneMatch = bodyText.match(/(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
    if (phoneMatch) {
      lead.phone = phoneMatch[0].replace(/[^\d+]/g, '');
      // Normalize: strip leading 1 if 11 digits
      if (lead.phone.length === 11 && lead.phone.startsWith('1')) {
        lead.phone = lead.phone.slice(1);
      }
      // Format as XXX-XXX-XXXX
      if (lead.phone.length === 10) {
        lead.phone = `${lead.phone.slice(0,3)}-${lead.phone.slice(3,6)}-${lead.phone.slice(6)}`;
      }
    }
  }

  // --- Extract locations ---
  // Course Report lists locations as links to /cities/{city-name}
  if (!lead.city) {
    const cities = [];
    $('a[href^="/cities/"]').each((_, el) => {
      const cityText = $(el).text().trim();
      if (cityText && cityText.toLowerCase() !== 'online') {
        cities.push(cityText);
      }
    });
    if (cities.length > 0) {
      // Use first physical location as primary
      const primary = cities[0];
      lead.city = primary;

      // Try to extract state/country from the city text
      // e.g., "New York City" → city=New York City, state guessed later
      // e.g., "London" → country=UK
    }
  }

  // --- Determine country if not set ---
  if (!lead.country && lead.city) {
    lead.country = guessCountry(lead.city);
  }

  // If still no country but the school exists, default to US (most bootcamps)
  if (!lead.country && lead.company) {
    // Check if "Online" is a location — many bootcamps are online-only
    const allText = $('body').text().toLowerCase();
    if (allText.includes('online') && !lead.city) {
      lead.city = 'Online';
    }
  }

  // --- Extract email from page text as fallback ---
  if (!lead.email) {
    const bodyText = $('body').text();
    const emailMatch = bodyText.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/);
    if (emailMatch) {
      const candidate = emailMatch[0].toLowerCase();
      // Skip coursereport.com emails and common false positives
      if (!candidate.includes('coursereport.com') &&
          !candidate.includes('example.com') &&
          !candidate.includes('@sentry')) {
        lead.email = candidate;
      }
    }
  }

  // --- Split company contact name if available ---
  // Some JSON-LD includes contactPoint with name — rare but worth checking
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());
      const items = Array.isArray(json) ? json : [json];
      for (const item of items) {
        if (item.contactPoint && item.contactPoint.name) {
          const parts = item.contactPoint.name.trim().split(/\s+/);
          if (parts.length >= 2) {
            lead.first_name = parts[0];
            lead.last_name = parts.slice(1).join(' ');
          }
        }
      }
    } catch { /* skip */ }
  });

  return lead;
}

/**
 * Guess country from city name for well-known bootcamp cities.
 */
function guessCountry(city) {
  if (!city) return '';
  const c = city.toLowerCase().trim();

  const usaCities = [
    'new york', 'san francisco', 'los angeles', 'chicago', 'austin', 'seattle',
    'denver', 'boston', 'atlanta', 'dallas', 'houston', 'miami', 'portland',
    'phoenix', 'san diego', 'philadelphia', 'washington', 'san jose', 'detroit',
    'minneapolis', 'charlotte', 'nashville', 'salt lake city', 'raleigh', 'orlando',
    'tampa', 'pittsburgh', 'columbus', 'indianapolis', 'milwaukee', 'boise',
    'birmingham', 'auburn', 'tulsa', 'provo', 'richmond', 'durham', 'burlington',
    'new york city', 'nyc', 'sf', 'la',
  ];
  const ukCities = ['london', 'manchester', 'birmingham uk', 'edinburgh', 'bristol', 'leeds', 'glasgow', 'cambridge', 'oxford', 'brighton'];
  const caCities = ['toronto', 'vancouver', 'montreal', 'calgary', 'ottawa', 'edmonton', 'winnipeg'];
  const auCities = ['sydney', 'melbourne', 'brisbane', 'perth', 'adelaide'];
  const deCities = ['berlin', 'munich', 'hamburg', 'frankfurt', 'cologne', 'dusseldorf', 'stuttgart'];
  const frCities = ['paris', 'lyon', 'marseille', 'toulouse', 'bordeaux', 'nantes', 'lille'];
  const esCities = ['barcelona', 'madrid', 'valencia', 'seville', 'malaga'];
  const nlCities = ['amsterdam', 'rotterdam', 'the hague', 'utrecht'];
  const otherEu = ['lisbon', 'dublin', 'prague', 'vienna', 'zurich', 'geneva', 'copenhagen', 'stockholm', 'oslo', 'helsinki', 'warsaw', 'budapest', 'rome', 'milan', 'athens'];
  const asiaCities = ['singapore', 'hong kong', 'tokyo', 'seoul', 'shanghai', 'beijing', 'mumbai', 'bangalore', 'jakarta', 'bangkok', 'kuala lumpur'];
  const safrCities = ['cape town', 'johannesburg'];
  const latamCities = ['mexico city', 'sao paulo', 'buenos aires', 'bogota', 'lima', 'santiago', 'medellin'];

  if (usaCities.some(x => c.includes(x))) return 'US';
  if (ukCities.some(x => c.includes(x))) return 'UK';
  if (caCities.some(x => c.includes(x))) return 'CA';
  if (auCities.some(x => c.includes(x))) return 'AU';
  if (deCities.some(x => c.includes(x))) return 'DE';
  if (frCities.some(x => c.includes(x))) return 'FR';
  if (esCities.some(x => c.includes(x))) return 'ES';
  if (nlCities.some(x => c.includes(x))) return 'NL';
  if (otherEu.some(x => c.includes(x))) return 'EU';
  if (asiaCities.some(x => c.includes(x))) return 'APAC';
  if (safrCities.some(x => c.includes(x))) return 'ZA';
  if (latamCities.some(x => c.includes(x))) return 'LATAM';

  return '';
}

// ---------------------------------------------------------------------------
// Phase 3: Main orchestration
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('+-----------------------------------------------------------+');
  console.log('|  MORTAR -- Coding Bootcamp Scraper (Course Report)        |');
  console.log('+-----------------------------------------------------------+');
  console.log('');
  console.log(`  Source:      coursereport.com (864 schools)`);
  console.log(`  Mode:        ${TEST_MODE ? 'TEST (2 pages, 10 profiles)' : 'FULL'}`);
  console.log(`  Resume:      ${RESUME ? 'YES' : 'NO'}`);
  console.log(`  Delay:       ${PROFILE_DELAY_MS / 1000}s between profiles (robots.txt: 10s)`);
  console.log(`  Output:      ${OUTPUT_FILE}`);
  console.log(`  Progress:    ${PROGRESS_FILE}`);
  console.log('');

  // Phase 1: Collect slugs
  const allSlugs = await collectSlugs();
  if (allSlugs.length === 0) {
    log('[ERROR] No school slugs found — aborting');
    process.exit(1);
  }

  // Phase 2: Load existing progress for resume
  let leads = [];
  let scrapedSlugs = new Set();

  if (RESUME) {
    leads = loadExistingLeads();
    scrapedSlugs = loadExistingSlugs();
    if (leads.length > 0) {
      log(`Resuming: loaded ${leads.length} existing leads (${scrapedSlugs.size} slugs)`);
    }
  }

  // Filter out already-scraped slugs
  const slugsToScrape = allSlugs.filter(s => !scrapedSlugs.has(s));
  const maxProfiles = TEST_MODE ? 10 : slugsToScrape.length;
  const profilesToScrape = slugsToScrape.slice(0, maxProfiles);

  log(`Phase 2: Scraping ${profilesToScrape.length} school profiles...`);
  if (RESUME && scrapedSlugs.size > 0) {
    log(`  (Skipping ${scrapedSlugs.size} already-scraped profiles)`);
  }

  const estMinutes = Math.round(profilesToScrape.length * PROFILE_DELAY_MS / 1000 / 60);
  log(`  Estimated time: ~${estMinutes} minutes`);
  console.log('');

  let scraped = 0;
  let withEmail = 0;
  let withPhone = 0;
  let errors = 0;

  for (const slug of profilesToScrape) {
    scraped++;
    const url = `${BASE_URL}/schools/${slug}`;

    try {
      const html = await fetchWithRetry(url);
      const $ = cheerio.load(html);
      const lead = parseSchoolProfile($, slug);

      if (lead.company) {
        leads.push(lead);
        if (lead.email) withEmail++;
        if (lead.phone) withPhone++;

        const emailTag = lead.email ? ` [${lead.email}]` : ' [no email]';
        const pct = ((scraped / profilesToScrape.length) * 100).toFixed(1);
        log(`  [${pct}%] ${scraped}/${profilesToScrape.length} — ${lead.company}${emailTag}`);
      } else {
        log(`  [SKIP] ${slug} — no company name found`);
      }

      // Save progress every 50 profiles
      if (scraped % 50 === 0) {
        writeCSV(PROGRESS_FILE, leads);
        log(`  [SAVE] Progress saved: ${leads.length} leads (${withEmail} emails, ${withPhone} phones)`);
      }

    } catch (err) {
      errors++;
      log(`  [ERROR] ${slug}: ${err.message}`);
    }

    // Rate limit between requests
    if (scraped < profilesToScrape.length) {
      await sleep(PROFILE_DELAY_MS);
    }
  }

  // Phase 3: Write final CSV
  console.log('');
  log('Phase 3: Writing final CSV...');

  // Write progress file (all data including empty emails for completeness)
  writeCSV(PROGRESS_FILE, leads);

  // Write final output file (only leads with email)
  const leadsWithEmail = leads.filter(l => l.email);
  writeCSV(OUTPUT_FILE, leadsWithEmail);

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  const minutes = (elapsed / 60).toFixed(1);

  console.log('');
  console.log('+-----------------------------------------------------------+');
  console.log('|  RESULTS                                                  |');
  console.log('+-----------------------------------------------------------+');
  console.log('');
  console.log(`  Total schools scraped:   ${leads.length}`);
  console.log(`  With email:              ${withEmail} (${(withEmail/leads.length*100).toFixed(1)}%)`);
  console.log(`  With phone:              ${withPhone} (${(withPhone/leads.length*100).toFixed(1)}%)`);
  console.log(`  Errors:                  ${errors}`);
  console.log(`  Time:                    ${minutes} minutes`);
  console.log('');
  console.log(`  Full output (all):       ${PROGRESS_FILE}`);
  console.log(`  Email leads:             ${OUTPUT_FILE} (${leadsWithEmail.length} rows)`);
  console.log('');

  // Show top 5 sample leads
  if (leadsWithEmail.length > 0) {
    console.log('  Sample leads:');
    for (const lead of leadsWithEmail.slice(0, 5)) {
      console.log(`    ${lead.company} — ${lead.email} — ${lead.city || 'Online'}`);
    }
    console.log('');
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});
