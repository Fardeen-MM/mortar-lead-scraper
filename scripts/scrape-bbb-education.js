#!/usr/bin/env node
/**
 * BBB.org Education Services Scraper
 *
 * Scrapes BBB category pages for education businesses: tutors, educational
 * consultants, and language schools.
 * Uses /us/{state}/category/{slug} and /ca/{province}/category/{slug} URLs,
 * which are server-rendered and accessible from any IP (unlike BBB search
 * which is geo-blocked).
 *
 * Strategy:
 *   1. Iterate all 50 US states + DC via category pages (no geo-blocking)
 *   2. Also iterate Canadian provinces (AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, SK)
 *   3. Three categories per region: educational-consultant, tutoring, language-school
 *   4. Each category page has embedded JSON data (webDigitalData.results) with
 *      name, phone, rating, zip + HTML profile links with city/state from URL
 *   5. Paginate up to 15 pages per state/category (BBB caps at page 15)
 *   6. Optionally enrich via profile pages for website, email, address, years, contacts
 *   7. 3-5 second delays between requests (polite to BBB)
 *   8. Resume support via progress file
 *
 * Data sources:
 *   - Category page JSON: business_name, business_phone, business_rating, zip_code,
 *     accredited_status, business_id
 *   - Category page HTML: profile_url (contains city + state in path)
 *   - Profile page (enrichment): website, email, address, years_in_business,
 *     categories, contact names
 *
 * Usage:
 *   node scripts/scrape-bbb-education.js --test          # 3 states, 1 page each, no profiles
 *   node scripts/scrape-bbb-education.js --all           # All states + profile enrichment
 *   node scripts/scrape-bbb-education.js --all --resume  # Resume interrupted scrape
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
const MAX_PAGES = 15; // BBB caps category pages at 15

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'education-services-bbb.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'bbb-education-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'zip', 'address', 'country',
  'niche', 'source', 'profile_url', 'bbb_rating', 'bbb_accredited',
  'years_in_business', 'categories',
];

// ── Category URLs to scrape ─────────────────────────────────────────────────
// Verified working BBB category slugs for education services:
//   educational-consultant  — 547 results in CA, 201 in NY, 73 in CA-ON
//   tutoring                — 729 results in CA, 418 in TX, 221 in NY
//   language-school         — 99 results in CA

const CATEGORIES = ['educational-consultant', 'tutoring', 'language-school'];

// US states (50 + DC)
const US_STATES = [
  'ak', 'al', 'ar', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl',
  'ga', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la', 'ma',
  'md', 'me', 'mi', 'mn', 'mo', 'ms', 'mt', 'nc', 'nd', 'ne',
  'nh', 'nj', 'nm', 'nv', 'ny', 'oh', 'ok', 'or', 'pa', 'ri',
  'sc', 'sd', 'tn', 'tx', 'ut', 'va', 'vt', 'wa', 'wi', 'wv',
  'wy',
];

// Canadian provinces (from BBB sitemap)
const CA_PROVINCES = [
  'ab', 'bc', 'mb', 'nb', 'nl', 'ns', 'nt', 'nu', 'on', 'pe', 'sk',
];

// Build all category page base URLs
function buildCategoryUrls() {
  const urls = [];

  for (const cat of CATEGORIES) {
    // US states
    for (const state of US_STATES) {
      urls.push({
        baseUrl: `https://www.bbb.org/us/${state}/category/${cat}`,
        country: 'US',
        stateCode: state.toUpperCase(),
        category: cat,
        key: `us/${state}/${cat}`,
      });
    }
    // Canadian provinces
    for (const prov of CA_PROVINCES) {
      urls.push({
        baseUrl: `https://www.bbb.org/ca/${prov}/category/${cat}`,
        country: 'CA',
        stateCode: prov.toUpperCase(),
        category: cat,
        key: `ca/${prov}/${cat}`,
      });
    }
  }

  return urls;
}

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

function titleCase(str) {
  if (!str) return '';
  return str.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  let cleaned = firmName;

  // "Academy of John Smith" / "School of John Smith" — skip these (org names)
  // Focus on simple personal-name patterns like "John Smith Tutoring"

  // Strip common suffixes
  cleaned = cleaned
    .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|APC|APLC|Corp\.?|Co\.?|Ltd\.?)/gi, '')
    .replace(/\s*(Tutoring|Tutor|Consulting|Consultants?|Education|Educational|Learning|Academy|School|Schools|Institute|Center|Centre|Language|College|Prep|Preparation|Services?|Group|Associates?|& Associates?|International|Online|Training|Programs?)/gi, '')
    .trim();

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  // Skip org-like names
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/^(A|An)\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  const orgWords = /^(tutoring|tutor|learning|academy|school|education|educational|center|centre|consulting|consultants?|language|college|prep|preparation|services?|group|associates?|international|online|training|programs?|institute|math|science|english|reading|study|smart|bright|elite|premier|world|global|national|american|canadian|best|pro|master|expert|top|kids|children|youth|student|scholars?|genius|university|universities|career|career|legacy|systems|intelligence|technology|solutions|edge|family|headstart|southeast|northwest|northeast|southwest|mobile|city|county|state|federal|dothan|sylvan|fortis|desi|visionary|leading|academic|creative|strategic|dynamic|innovative|advanced|foundational|southern|northern|eastern|western)$/i;

  // Match "First Last" or "First M. Last" patterns
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    const firstName = parts[0];
    if (orgWords.test(lastName) || orgWords.test(firstName)) return { first_name: '', last_name: '' };
    // Extra check: if the original firm name contains "University", "College", "School",
    // "Center", "Institute", "Academy" etc., it's an org, not a person
    if (/university|college|school|center|centre|institute|academy|headstart|career|systems/i.test(firmName)) {
      return { first_name: '', last_name: '' };
    }
    return { first_name: firstName, last_name: lastName };
  }

  return { first_name: '', last_name: '' };
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return rawPhone;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function csvEscape(val) {
  if (!val) return '';
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

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        'Cookie': 'iabbb_user_culture=en-us',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(url, options, (res) => {
      // Handle redirects
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `https://www.bbb.org${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 8000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
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
        sleep(5000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(5000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Category Page Parser ─────────────────────────────────────────────────────
// Extracts leads from BBB category pages using both:
//   1. Embedded JSON in webDigitalData.results (name, phone, rating, zip)
//   2. HTML profile links (profile URL with city/state in path)

function parseCategoryPage(html, catInfo) {
  const $ = cheerio.load(html);
  const results = [];

  // Extract total result count from webDigitalData
  let totalCount = 0;
  const totalMatch = html.match(/"total_results":(\d+)/);
  if (totalMatch) {
    totalCount = parseInt(totalMatch[1], 10);
  }

  // Extract JSON results from webDigitalData
  // Format: "results":[{...},{...},...]
  const jsonResults = [];
  const resultsMatch = html.match(/"results":\[(\{[^]*?\})\]/);
  if (resultsMatch) {
    try {
      const arr = JSON.parse('[' + resultsMatch[1] + ']');
      jsonResults.push(...arr);
    } catch (e) {
      // Try extracting individual objects if full parse fails
      const objMatches = resultsMatch[1].match(/\{[^{}]+\}/g) || [];
      for (const objStr of objMatches) {
        try {
          jsonResults.push(JSON.parse(objStr));
        } catch {}
      }
    }
  }

  // Build a map of business_id -> JSON data for matching with HTML links
  const jsonByName = {};
  const jsonById = {};
  for (const jr of jsonResults) {
    if (jr.business_name) {
      jsonByName[jr.business_name.toLowerCase()] = jr;
    }
    if (jr.business_id) {
      jsonById[jr.business_id] = jr;
    }
  }

  // Extract profile links from HTML
  const profilePattern = /\/(us|ca)\/([a-z]{2})\/([^/]+)\/profile\/[^/]+\/([^/]+)-(\d{4})-(\d+)/;
  const seen = new Set();

  $('a[href*="/profile/"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const match = href.match(profilePattern);
    if (!match) return;

    const [, countryCode, stateCode, citySlug, nameSlug, bbbId, businessId] = match;

    // Build clean profile URL (without addressId)
    let profileUrl = href.startsWith('http') ? href : `https://www.bbb.org${href}`;
    profileUrl = profileUrl.replace(/\/addressId\/\d+$/, '');
    // Also strip /details suffix
    profileUrl = profileUrl.replace(/\/details$/, '');

    // Dedup by businessId
    if (seen.has(businessId)) return;

    const name = $(el).text().trim();
    if (!name || name.length < 2 || name.length > 200) return;
    // Skip navigation/UI text
    if (/^(search|find|file|get|learn|start|sign|log|home|about|news|more\s*service)/i.test(name)) return;

    seen.add(businessId);

    // Match with JSON data by business_id or name
    const jsonData = jsonById[businessId] || jsonByName[name.toLowerCase()] || {};

    const city = titleCase(citySlug);
    const state = stateCode.toUpperCase();
    const country = countryCode.toUpperCase();
    const phone = formatPhone(jsonData.business_phone || '');
    const rating = jsonData.business_rating || '';
    const zip = jsonData.zip_code || '';
    const accredited = jsonData.accredited_status === 'AB' ? 'Yes' : 'No';

    results.push({
      firm_name: jsonData.business_name || name,
      phone,
      city,
      state,
      zip,
      country,
      bbb_rating: rating,
      bbb_accredited: accredited,
      profile_url: profileUrl,
      business_id: businessId,
    });
  });

  // If HTML parsing missed some, add from JSON-only results
  for (const jr of jsonResults) {
    if (!jr.business_id || seen.has(jr.business_id)) continue;
    seen.add(jr.business_id);

    results.push({
      firm_name: jr.business_name || '',
      phone: formatPhone(jr.business_phone || ''),
      city: '',
      state: catInfo.stateCode,
      zip: jr.zip_code || '',
      country: catInfo.country,
      bbb_rating: jr.business_rating || '',
      bbb_accredited: jr.accredited_status === 'AB' ? 'Yes' : 'No',
      profile_url: '',
      business_id: jr.business_id,
    });
  }

  return { results, totalCount };
}

// ── Profile Page Parser ──────────────────────────────────────────────────────

function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const data = {};
  const text = $('body').text();

  // Website URL - look for "Visit Website" or external links
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const linkText = $(el).text().trim().toLowerCase();

    // Prioritize "Visit Website" links
    if ((linkText.includes('visit website') || linkText === 'website') &&
        href.startsWith('http') && !href.includes('bbb.org')) {
      data.website = href;
      return false; // break
    }
  });

  // Fallback: find first external link that looks like a business website
  if (!data.website) {
    const skipDomains = ['bbb.org', 'google.com', 'facebook.com', 'twitter.com',
      'instagram.com', 'yelp.com', 'linkedin.com', 'youtube.com', 'pinterest.com',
      'tiktok.com', 'x.com', 'glassdoor.com', 'indeed.com', 'cloudflare.com',
      'mouseflow.com', 'adobedtm.com', 'googletagmanager.com', 'googleapis.com',
      'gstatic.com', 'doubleclick.net', 'googleadservices.com', 'livechatinc.com',
      'evergage.com', 'fontawesome.com', 'cookielaw.org', 'onetrust.com'];

    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (skipDomains.some(d => href.includes(d))) return;
      if (!data.website) {
        data.website = href;
      }
    });
  }

  // Years in business
  const yearsMatch = text.match(/Years?\s+in\s+Business:?\s*(\d+)/i)
    || text.match(/(\d+)\s+Years?\s+in\s+Business/i);
  if (yearsMatch) {
    data.years_in_business = yearsMatch[1];
  }

  // Categories - look for the category text pattern
  const catPatterns = [
    /(?:Categories?|Business\s+Category):?\s*([A-Z][A-Za-z\s,&]+(?:Tutoring|Tutor|Education|Educational|Consultant|Consulting|School|Language|Learning|Academy|College|Prep|Training|Services?))/i,
    /(?:Categories?|Business\s+Category):?\s*([^\n]+)/i,
  ];
  for (const pat of catPatterns) {
    const catMatch = text.match(pat);
    if (catMatch) {
      data.categories = catMatch[1].trim().replace(/\s+/g, ' ').substring(0, 200);
      break;
    }
  }

  // Contact name (Principal, Owner, Manager, President, Founder, etc.)
  const titlePatterns = [
    /(?:Principal|Owner|Manager|President|Founder|CEO|Director|Head)[:\s]+(?:Mr\.|Mrs\.|Ms\.|Dr\.)?\s*([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)/,
    /(?:Mr\.|Mrs\.|Ms\.|Dr\.)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)/,
  ];
  for (const pat of titlePatterns) {
    const contactMatch = text.match(pat);
    if (contactMatch) {
      const parts = contactMatch[1].trim().split(/\s+/);
      data.contact_first = parts[0];
      data.contact_last = parts[parts.length - 1];
      break;
    }
  }

  // Phone
  const phoneMatch = text.match(/\((\d{3})\)\s*(\d{3})[- ](\d{4})/);
  if (phoneMatch) {
    data.phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;
  }

  // Full address
  const addrMatch = text.match(/(\d+\s+[^,\n]{3,50}),\s*([A-Za-z\s.]{2,30}),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
  if (addrMatch) {
    data.address = addrMatch[1].trim();
    data.city = addrMatch[2].trim();
    data.state = addrMatch[3];
    data.zip = addrMatch[4];
  }

  // Email (rare on BBB but worth checking)
  const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
  if (emailMatch && !emailMatch[1].includes('bbb.org') && !emailMatch[1].includes('example.')) {
    data.email = emailMatch[1];
  }

  // BBB Rating
  const ratingMatch = text.match(/BBB\s+Rating:?\s*([A-F][+-]?)/i);
  if (ratingMatch) {
    data.bbb_rating = ratingMatch[1];
  }

  // Accreditation
  if (/BBB\s+Accredited\s+(?:Business|Since)/i.test(text)) {
    data.bbb_accredited = 'Yes';
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
  return { completedCategories: [], enrichedProfiles: [], leads: {} };
}

function saveProgress(state) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    ...state,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const isResume = args.includes('--resume');

  if (!isTest && !isAll) {
    console.log('BBB.org Education Services Scraper');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-bbb-education.js --test          # 3 states, 1 page each, no profiles');
    console.log('  node scripts/scrape-bbb-education.js --all           # All states + profile enrichment');
    console.log('  node scripts/scrape-bbb-education.js --all --resume  # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  const allCategoryUrls = buildCategoryUrls();

  // In test mode: 3 US states only, 1 page each, 1 category
  let categoryUrls = isTest
    ? allCategoryUrls.filter(c => c.country === 'US' && c.category === 'educational-consultant').slice(0, 3)
    : [...allCategoryUrls];
  const maxPagesPerCategory = isTest ? 1 : MAX_PAGES;
  const enrichProfiles = !isTest;

  // State
  let leadsMap = {}; // business_id -> lead
  let completedCategories = [];
  let enrichedProfiles = [];

  // Resume support
  if (isResume) {
    const progress = loadProgress();
    completedCategories = progress.completedCategories || [];
    enrichedProfiles = progress.enrichedProfiles || [];
    leadsMap = progress.leads || {};
    log(`Resuming: ${completedCategories.length} categories done, ${Object.keys(leadsMap).length} leads, ${enrichedProfiles.length} profiles enriched`);
  }

  const categoriesToDo = categoryUrls.filter(c => !completedCategories.includes(c.key));

  log('BBB.org Education Services Scraper');
  log(`Categories: ${categoryUrls.length} (${US_STATES.length} US states + ${CA_PROVINCES.length} CA provinces x ${CATEGORIES.length} categories)`);
  log(`Remaining: ${categoriesToDo.length} | Existing leads: ${Object.keys(leadsMap).length}`);
  log(`Max pages per category: ${maxPagesPerCategory}`);
  log(`Profile enrichment: ${enrichProfiles ? 'YES' : 'NO (test mode)'}`);
  log(`Delays: ${DELAY_MIN / 1000}-${DELAY_MAX / 1000}s between requests`);
  log(`Output: ${OUT_FILE}`);
  log('');

  let processed = 0;
  let totalNew = 0;
  let errors = 0;
  let noResults = 0;
  let pagesScraped = 0;

  // ── Phase 1: Category page scraping ────────────────────────────────────────

  for (const catInfo of categoriesToDo) {
    processed++;
    const label = `${catInfo.country}/${catInfo.stateCode} ${catInfo.category}`;

    log(`[${processed}/${categoriesToDo.length}] ${label}`);

    let page = 1;
    let totalForCategory = 0;
    let newForCategory = 0;
    let hasMore = true;

    while (hasMore && page <= maxPagesPerCategory) {
      const pageUrl = page === 1
        ? catInfo.baseUrl
        : `${catInfo.baseUrl}?page=${page}`;

      try {
        const { statusCode, body } = await httpGet(pageUrl);
        pagesScraped++;

        if (statusCode !== 200 || !body) {
          if (statusCode === 403 || statusCode === 429) {
            log(`  HTTP ${statusCode} on page ${page} - backing off`);
            await sleep(15000);
            errors++;
          } else if (statusCode === 404) {
            if (page === 1) log(`  404 - no category page`);
          } else {
            log(`  HTTP ${statusCode} on page ${page}`);
          }
          break;
        }

        // Check for "no results"
        if (/No results/i.test(body) && !/total_results":\s*[1-9]/.test(body)) {
          if (page === 1) {
            noResults++;
            log(`  No results`);
          }
          break;
        }

        const { results, totalCount } = parseCategoryPage(body, catInfo);

        if (results.length === 0) {
          if (page === 1) {
            noResults++;
            log(`  No listings parsed`);
          }
          break;
        }

        if (page === 1 && totalCount > 0) {
          const maxPages = Math.min(Math.ceil(totalCount / 15), MAX_PAGES);
          log(`  ${totalCount} total results (up to ${maxPages} pages)`);
        }

        let pageNew = 0;
        for (const result of results) {
          const dedupKey = result.business_id || result.profile_url || `${result.firm_name}|${result.zip}`;
          if (leadsMap[dedupKey]) continue;

          const { first_name, last_name } = parseName(result.firm_name);

          leadsMap[dedupKey] = {
            first_name,
            last_name,
            firm_name: result.firm_name,
            title: '',
            email: '',
            phone: result.phone,
            website: '',
            domain: '',
            city: result.city,
            state: result.state,
            zip: result.zip,
            address: '',
            country: result.country,
            niche: 'education',
            source: 'bbb.org',
            profile_url: result.profile_url,
            bbb_rating: result.bbb_rating,
            bbb_accredited: result.bbb_accredited,
            years_in_business: '',
            categories: '',
          };
          pageNew++;
          newForCategory++;
          totalNew++;
        }

        totalForCategory += results.length;
        log(`  Page ${page}: ${results.length} listings, ${pageNew} new`);

        // Check if more pages exist
        const maxPagesAvail = totalCount > 0 ? Math.min(Math.ceil(totalCount / 15), MAX_PAGES) : 1;
        if (page >= maxPagesAvail || results.length < 5) {
          hasMore = false;
        } else {
          page++;
          await randomDelay();
        }

      } catch (err) {
        log(`  ERROR page ${page}: ${err.message}`);
        errors++;
        break;
      }
    }

    if (totalForCategory > 0 && newForCategory > 0) {
      log(`  Subtotal: ${totalForCategory} listings, ${newForCategory} new`);
    }

    completedCategories.push(catInfo.key);

    // Save progress every 5 categories
    if (processed % 5 === 0) {
      saveProgress({ completedCategories, enrichedProfiles, leads: leadsMap });
      writeCSV(Object.values(leadsMap), OUT_FILE);
      log(`  [Progress saved: ${Object.keys(leadsMap).length} leads]`);
    }

    await randomDelay();
  }

  // Save after category phase
  saveProgress({ completedCategories, enrichedProfiles, leads: leadsMap });
  writeCSV(Object.values(leadsMap), OUT_FILE);
  log('');
  log(`Category scraping complete: ${Object.keys(leadsMap).length} unique leads`);

  // ── Phase 2: Profile Enrichment ────────────────────────────────────────────

  if (enrichProfiles) {
    const allLeads = Object.entries(leadsMap);
    const toEnrich = allLeads.filter(([key, lead]) =>
      lead.profile_url && !enrichedProfiles.includes(key)
    );

    log(`\n=== Profile Enrichment Phase: ${toEnrich.length} profiles to fetch ===`);

    let enriched = 0;
    let enrichErrors = 0;
    let websitesFound = 0;
    let emailsFound = 0;

    for (const [key, lead] of toEnrich) {
      enriched++;

      if (enriched % 25 === 0 || enriched === 1) {
        log(`  [${enriched}/${toEnrich.length}] Enriching... (${websitesFound} websites, ${emailsFound} emails found)`);
      }

      try {
        const { statusCode, body } = await httpGet(lead.profile_url);

        if (statusCode === 200 && body) {
          const profileData = parseProfilePage(body);

          // Merge profile data
          if (profileData.website) {
            lead.website = profileData.website;
            lead.domain = extractDomain(profileData.website);
            websitesFound++;
          }
          if (profileData.email && !lead.email) {
            lead.email = profileData.email;
            emailsFound++;
          }
          if (profileData.phone && !lead.phone) lead.phone = profileData.phone;
          if (profileData.address && !lead.address) lead.address = profileData.address;
          if (profileData.city && !lead.city) lead.city = profileData.city;
          if (profileData.state && !lead.state) lead.state = profileData.state;
          if (profileData.zip && !lead.zip) lead.zip = profileData.zip;
          if (profileData.bbb_rating) lead.bbb_rating = profileData.bbb_rating;
          if (profileData.bbb_accredited === 'Yes') lead.bbb_accredited = 'Yes';
          if (profileData.years_in_business) lead.years_in_business = profileData.years_in_business;
          if (profileData.categories) lead.categories = profileData.categories;
          if (profileData.contact_first && !lead.first_name) {
            lead.first_name = profileData.contact_first;
            lead.last_name = profileData.contact_last || '';
          }

          leadsMap[key] = lead;
        } else {
          enrichErrors++;
          if (statusCode === 429 || statusCode === 403) {
            log(`  Rate limited on profile, extra delay...`);
            await sleep(10000);
          }
        }
      } catch (err) {
        enrichErrors++;
      }

      enrichedProfiles.push(key);

      // Save progress every 50 profiles
      if (enriched % 50 === 0) {
        saveProgress({ completedCategories, enrichedProfiles, leads: leadsMap });
        writeCSV(Object.values(leadsMap), OUT_FILE);
        log(`  [Progress saved: ${enriched} profiles enriched]`);
      }

      await randomDelay();
    }

    log(`Profile enrichment done: ${enriched} fetched, ${enrichErrors} errors, ${websitesFound} websites, ${emailsFound} emails`);
  }

  // ── Final Output ───────────────────────────────────────────────────────────

  const allLeads = Object.values(leadsMap);
  writeCSV(allLeads, OUT_FILE);
  saveProgress({ completedCategories, enrichedProfiles, leads: leadsMap });

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const accredited = allLeads.filter(l => l.bbb_accredited === 'Yes').length;
  const uniqueStates = new Set(allLeads.map(l => l.state).filter(Boolean)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city).filter(Boolean)).size;
  const usLeads = allLeads.filter(l => l.country === 'US').length;
  const caLeads = allLeads.filter(l => l.country === 'CA').length;

  // Per-category breakdown
  const catCounts = {};
  for (const cat of CATEGORIES) {
    catCounts[cat] = 0;
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Categories scraped: ${processed}`);
  log(`Pages scraped: ${pagesScraped}`);
  log(`No-result categories: ${noResults}`);
  log(`Errors: ${errors}`);
  log('');
  log(`Total unique leads: ${allLeads.length} (${usLeads} US, ${caLeads} CA)`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With email: ${withEmail}`);
  log(`With parsed name: ${withName}`);
  log(`BBB Accredited: ${accredited}`);
  log(`Unique states/provinces: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on fully successful runs
  if (!isTest && errors === 0 && categoriesToDo.length === categoryUrls.length) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
