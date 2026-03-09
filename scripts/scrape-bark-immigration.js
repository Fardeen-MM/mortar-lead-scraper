#!/usr/bin/env node
/**
 * Bark.com Immigration Lawyers/Consultants Scraper
 *
 * Scrapes https://www.bark.com/en/{country}/immigration-lawyer/{region}/{city}/
 * using Puppeteer (Bark blocks plain HTTP with 403).
 *
 * Strategy:
 *   1. Visit main category page for each country to discover city URLs dynamically
 *      (city URLs include region prefixes that can't be hardcoded)
 *   2. For each city URL, collect company profile links
 *   3. Visit each profile page to extract: name, email, website, location
 *   4. Dedup by firm_name + city
 *   5. Resume support via progress file
 *   6. Output CSV
 *
 * Usage:
 *   node scripts/scrape-bark-immigration.js --test                 # 3 cities, 3 profiles per city
 *   node scripts/scrape-bark-immigration.js --country UK           # All UK cities
 *   node scripts/scrape-bark-immigration.js --country US           # All US cities
 *   node scripts/scrape-bark-immigration.js --country CA           # All CA cities
 *   node scripts/scrape-bark-immigration.js --country ALL          # All countries
 *   node scripts/scrape-bark-immigration.js --country ALL --resume # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const PROFILE_DELAY_MIN = 2000;
const PROFILE_DELAY_MAX = 3500;
const NAV_TIMEOUT = 30000;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-bark.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'bark-immigration-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Country configs — city URLs are discovered dynamically from the main listing page
const COUNTRY_CONFIG = {
  UK: { code: 'gb', country: 'UK', mainUrl: 'https://www.bark.com/en/gb/immigration-lawyer/' },
  US: { code: 'us', country: 'US', mainUrl: 'https://www.bark.com/en/us/immigration-lawyer/' },
  CA: { code: 'ca', country: 'CA', mainUrl: 'https://www.bark.com/en/ca/immigration-lawyer/' },
};

// State/province mapping for US and CA (extracted from URL region slugs)
const US_STATE_MAP = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'district-of-columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI',
  'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
  'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME',
  'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
  'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
  'nevada': 'NV', 'new-hampshire': 'NH', 'new-jersey': 'NJ', 'new-mexico': 'NM',
  'new-york': 'NY', 'north-carolina': 'NC', 'north-dakota': 'ND', 'ohio': 'OH',
  'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode-island': 'RI',
  'south-carolina': 'SC', 'south-dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX',
  'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
  'west-virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
};

const CA_PROVINCE_MAP = {
  'alberta': 'AB', 'british-columbia': 'BC', 'manitoba': 'MB',
  'new-brunswick': 'NB', 'newfoundland-and-labrador': 'NL',
  'northwest-territories': 'NT', 'nova-scotia': 'NS', 'nunavut': 'NU',
  'ontario': 'ON', 'prince-edward-island': 'PE', 'quebec': 'QC',
  'saskatchewan': 'SK', 'yukon': 'YT',
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay(min = DELAY_MIN, max = DELAY_MAX) {
  return sleep(min + Math.random() * (max - min));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function parseName(firmName) {
  if (!firmName) return { first_name: '', last_name: '' };

  let cleaned = firmName;

  // "Law Office(s) of John Smith" / "Law Firm of John Smith"
  const ofMatch = cleaned.match(/(?:Law\s+(?:Office|Offices|Firm|Group|Practice)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|Attorney.*|Law.*|&.*)/gi, '')
      .trim();
  } else {
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Ltd\.?|Limited|Esq\.?)/gi, '')
      .replace(/\s*(Law\s+(?:Firm|Group|Office|Offices|Practice|Center|Chamber)|Legal\s+(?:Group|Services?|Team|Associates?)|Immigration\s+(?:Law|Attorneys?|Lawyers?|Solicitors?|Consultants?|Advisers?|Advisors?)|Attorneys?\s+at\s+Law|Attorneys?|Lawyers?|Solicitors?|& Associates?|Associates?|Consultants?)/gi, '')
      .trim();
  }

  // Remove trailing punctuation/dashes
  cleaned = cleaned.replace(/[-,.\s]+$/, '').replace(/^[-,.\s]+/, '').trim();

  // Skip names starting with "The"
  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip if contains & or "and" (multi-partner firms)
  if (/\s[&+]\s|,\s+and\s+/i.test(cleaned)) return { first_name: '', last_name: '' };

  // Skip abbreviations / short "words"
  if (/^[A-Z]{2,5}$/i.test(cleaned)) return { first_name: '', last_name: '' };

  const lawWords = /^(law|legal|firm|group|office|pllc|plc|pc|immigration|attorneys?|lawyers?|solicitors?|counselors?|associates?|consultants?|services?|visa|uk|global|clear|first|pro|giga|smart)$/i;

  // If what remains looks like a person name (2-3 words)
  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    if (lawWords.test(lastName)) return { first_name: '', last_name: '' };
    return {
      first_name: parts[0],
      last_name: lastName,
    };
  }

  return { first_name: '', last_name: '' };
}

function formatPhoneUK(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 11 && digits[0] === '0') {
    if (digits.startsWith('020')) {
      return `${digits.slice(0, 3)} ${digits.slice(3, 7)} ${digits.slice(7)}`;
    }
    return `${digits.slice(0, 4)} ${digits.slice(4, 7)} ${digits.slice(7)}`;
  }
  if (digits.length === 12 && digits.startsWith('44')) {
    return formatPhoneUK('0' + digits.slice(2));
  }
  return rawPhone;
}

function formatPhoneUS(rawPhone) {
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

function formatPhone(rawPhone, country) {
  if (!rawPhone) return '';
  if (country === 'UK') return formatPhoneUK(rawPhone);
  return formatPhoneUS(rawPhone);
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let cleanUrl = url.trim();
    if (!cleanUrl.startsWith('http')) cleanUrl = 'https://' + cleanUrl;
    const u = new URL(cleanUrl);
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

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().replace(/[^a-z0-9]/g, '')}|${(city || '').toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

/**
 * Extract city name and state from a Bark URL path.
 * URL patterns:
 *   /en/gb/immigration-lawyer/london/                    -> city: London, state: ''
 *   /en/gb/immigration-lawyer/west-midlands/birmingham/  -> city: Birmingham, state: ''
 *   /en/us/immigration-lawyer/california/los-angeles/    -> city: Los Angeles, state: CA
 *   /en/ca/immigration-lawyer/ontario/toronto/           -> city: Toronto, state: ON
 */
function parseCityUrl(urlPath, countryKey) {
  // Remove base prefix to get the city parts
  const basePrefix = `/en/${COUNTRY_CONFIG[countryKey].code}/immigration-lawyer/`;
  let remainder = urlPath.replace(basePrefix, '').replace(/\/$/, '');

  // Skip non-city links (questions, costs, etc.)
  if (remainder.includes('question') || remainder.includes('cost') || remainder.includes('price') || remainder.includes('guide')) {
    return null;
  }

  const parts = remainder.split('/');

  if (parts.length === 2) {
    // Has region prefix: /region/city/
    const region = parts[0];
    const citySlug = parts[1];
    const city = slugToName(citySlug);

    let state = '';
    if (countryKey === 'US') {
      state = US_STATE_MAP[region] || '';
    } else if (countryKey === 'CA') {
      state = CA_PROVINCE_MAP[region] || '';
    }

    return { city, state, urlPath, slug: `${region}/${citySlug}` };
  } else if (parts.length === 1 && parts[0]) {
    // No region prefix: /city/
    const citySlug = parts[0];
    const city = slugToName(citySlug);
    return { city, state: '', urlPath, slug: citySlug };
  }

  return null;
}

function slugToName(slug) {
  return slug
    .split('-')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')
    .replace(/\bOf\b/g, 'of')
    .replace(/\bAnd\b/g, 'and')
    .replace(/\bOn\b/g, 'on')
    .replace(/\bUpon\b/g, 'upon')
    .replace(/\bThe\b/g, 'the')
    .replace(/^the /, 'The ');
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedCities: [], leads: [] };
}

function saveProgress(completedCities, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedCities,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Scraping Functions ───────────────────────────────────────────────────────

/**
 * Discover city URLs from the main listing page for a country.
 * Returns array of { city, state, url, slug }.
 */
async function discoverCityUrls(page, countryKey) {
  const config = COUNTRY_CONFIG[countryKey];
  log(`Discovering cities for ${countryKey} from ${config.mainUrl}`);

  try {
    const resp = await page.goto(config.mainUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    if (resp.status() !== 200) {
      log(`  HTTP ${resp.status()} loading main page`);
      return [];
    }
  } catch (err) {
    log(`  Navigation error: ${err.message.slice(0, 80)}`);
    return [];
  }

  await sleep(2000);

  const code = config.code;
  const linkPaths = await page.evaluate((countryCode) => {
    const paths = new Set();
    const basePrefix = `/en/${countryCode}/immigration-lawyer/`;
    document.querySelectorAll('a[href*="immigration-lawyer"]').forEach(a => {
      const href = a.getAttribute('href');
      if (!href) return;
      // Only city links (those with text like "Search X" or city name headings)
      if (href.startsWith(basePrefix) && href !== basePrefix) {
        // Must have at least one path segment after the base
        const remainder = href.replace(basePrefix, '').replace(/\/$/, '');
        if (remainder && !remainder.includes('question') && !remainder.includes('cost') && !remainder.includes('price') && !remainder.includes('guide')) {
          paths.add(href);
        }
      }
    });
    return [...paths];
  }, code);

  // Parse into city objects, filtering out region-only links
  const cities = [];
  const seenSlugs = new Set();

  for (const linkPath of linkPaths) {
    const parsed = parseCityUrl(linkPath, countryKey);
    if (!parsed) continue;

    // Skip region-only links (single slug without a second part that maps to a region)
    // For US/CA, single-slug links are regions (states/provinces), not cities
    if (countryKey === 'US' && parsed.slug.indexOf('/') === -1 && US_STATE_MAP[parsed.slug]) continue;
    if (countryKey === 'CA' && parsed.slug.indexOf('/') === -1 && CA_PROVINCE_MAP[parsed.slug]) continue;
    // For UK, single-slug region links (county names) — keep those that are also city names
    // (e.g., "london", "edinburgh", "cardiff" are both regions and cities)

    if (seenSlugs.has(parsed.slug)) continue;
    seenSlugs.add(parsed.slug);

    cities.push({
      city: parsed.city,
      state: parsed.state,
      url: `https://www.bark.com${linkPath}`,
      slug: parsed.slug,
    });
  }

  log(`  Discovered ${cities.length} city URLs`);
  return cities;
}

/**
 * Get list of company profile URLs from a city listing page.
 */
async function getProfileUrls(page, cityUrl) {
  log(`  Loading listing: ${cityUrl}`);

  try {
    const resp = await page.goto(cityUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    if (resp.status() === 404) {
      log(`  404 - no page`);
      return [];
    }
    if (resp.status() !== 200) {
      log(`  HTTP ${resp.status()} - skipping`);
      return [];
    }
  } catch (err) {
    log(`  Navigation error: ${err.message.slice(0, 80)}`);
    return [];
  }

  await sleep(2000);

  // Extract profile links
  const profileUrls = await page.evaluate(() => {
    const links = new Set();
    document.querySelectorAll('a[href*="/company/"]').forEach(a => {
      const href = a.getAttribute('href');
      if (href && !href.includes('linkedin') && !href.includes('facebook')) {
        const fullUrl = href.startsWith('http') ? href : `https://www.bark.com${href}`;
        links.add(fullUrl);
      }
    });
    return [...links];
  });

  log(`  Found ${profileUrls.length} profile links`);
  return profileUrls;
}

/**
 * Scrape a company profile page for contact details.
 */
async function scrapeProfile(page, profileUrl, cityInfo, country) {
  try {
    const resp = await page.goto(profileUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    if (resp.status() !== 200) {
      log(`    Profile ${resp.status()}: ${profileUrl.slice(-40)}`);
      return null;
    }
  } catch (err) {
    log(`    Profile error: ${err.message.slice(0, 60)}`);
    return null;
  }

  await sleep(1500);

  const data = await page.evaluate(() => {
    const result = {};

    // Company name from h1
    result.firmName = document.querySelector('h1')?.textContent?.trim() || '';

    // Email — look for the pro email link
    const emailEl = document.querySelector('.pro-email-link, a.js-profile-view-link-email');
    if (emailEl) {
      const href = emailEl.getAttribute('href');
      if (href && href.startsWith('mailto:')) {
        result.email = href.replace('mailto:', '').trim();
      } else {
        const text = emailEl.textContent.trim();
        if (text.includes('@')) {
          result.email = text;
        }
      }
    }
    // Fallback: look for any mailto link
    if (!result.email) {
      const mailtoEl = document.querySelector('a[href^="mailto:"]');
      if (mailtoEl) {
        result.email = mailtoEl.getAttribute('href').replace('mailto:', '').split('?')[0].trim();
      }
    }

    // Website
    const websiteEl = document.querySelector('.pro-website-link, a.js-profile-view-link-website');
    if (websiteEl) {
      result.website = websiteEl.getAttribute('href') || websiteEl.textContent.trim();
    }

    // Phone — check for visible phone number (some profiles show it without reveal)
    const phoneEls = document.querySelectorAll('.bj-seller-phone-number');
    for (const el of phoneEls) {
      const text = el.textContent.trim();
      if (text && text.length > 5 && !text.includes('Reveal') && /\d/.test(text)) {
        result.phone = text;
        break;
      }
    }
    // Also check tel: links
    if (!result.phone) {
      const telLink = document.querySelector('a[href^="tel:"]');
      if (telLink) {
        result.phone = telLink.getAttribute('href').replace('tel:', '').trim();
      }
    }

    // Location from profile
    const locationEl = document.querySelector('.seller-location');
    if (locationEl) {
      result.location = locationEl.textContent.trim();
    }

    return result;
  });

  if (!data.firmName) return null;

  // Parse location for city
  let city = cityInfo.city;
  if (data.location) {
    // Location format: "EC1V, London" or "London" or "M1, Manchester"
    const parts = data.location.split(',').map(s => s.trim());
    const locationCity = parts[parts.length - 1];
    if (locationCity && locationCity.length > 1) city = locationCity;
  }

  // Clean up website
  let website = data.website || '';
  if (website && !website.startsWith('http')) {
    website = 'https://' + website;
  }

  const { first_name, last_name } = parseName(data.firmName);

  return {
    first_name,
    last_name,
    firm_name: data.firmName,
    title: '',
    email: data.email || '',
    phone: formatPhone(data.phone || '', country),
    website,
    domain: extractDomain(website),
    city,
    state: cityInfo.state,
    country,
    niche: 'immigration',
    source: 'bark.com',
    profile_url: profileUrl,
  };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isResume = args.includes('--resume');
  const countryIdx = args.indexOf('--country');
  const countryArg = countryIdx >= 0 ? (args[countryIdx + 1] || '').toUpperCase() : '';

  if (!isTest && !countryArg) {
    console.log('Usage:');
    console.log('  node scripts/scrape-bark-immigration.js --test                 # Test mode (3 cities, 3 profiles/city)');
    console.log('  node scripts/scrape-bark-immigration.js --country UK           # All UK cities');
    console.log('  node scripts/scrape-bark-immigration.js --country US           # All US cities');
    console.log('  node scripts/scrape-bark-immigration.js --country CA           # All CA cities');
    console.log('  node scripts/scrape-bark-immigration.js --country ALL          # All countries');
    console.log('  node scripts/scrape-bark-immigration.js --country ALL --resume # Resume');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Determine countries to scrape
  let countries = [];
  if (isTest) {
    countries = ['UK'];
  } else if (countryArg === 'ALL') {
    countries = ['UK', 'US', 'CA'];
  } else if (COUNTRY_CONFIG[countryArg]) {
    countries = [countryArg];
  } else {
    console.error(`Unknown country: ${countryArg}. Use UK, US, CA, or ALL.`);
    process.exit(1);
  }

  // Load progress
  let allLeads = [];
  let completedCities = [];
  const seenKeys = new Set();

  if (isResume) {
    const progress = loadProgress();
    if (progress.completedCities.length > 0) {
      completedCities = progress.completedCities;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.firm_name, lead.city));
      }
      log(`Resuming: ${completedCities.length} cities done, ${allLeads.length} leads loaded`);
    }
  }

  log('Bark.com Immigration Lawyers/Consultants Scraper');
  log(`Countries: ${countries.join(', ')} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Launch Puppeteer
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1280, height: 800 });

  // Dismiss cookie banner
  try {
    await page.goto('https://www.bark.com/', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await sleep(2000);
    const acceptBtn = await page.$('#onetrust-accept-btn-handler');
    if (acceptBtn) {
      await acceptBtn.click();
      await sleep(1000);
      log('Dismissed cookie banner');
    }
  } catch {}

  let totalProcessed = 0;
  let totalNew = 0;
  let totalErrors = 0;
  let totalNoResults = 0;

  for (const countryKey of countries) {
    const config = COUNTRY_CONFIG[countryKey];
    log(`\n${'='.repeat(60)}`);
    log(`COUNTRY: ${countryKey} (${config.code})`);
    log('='.repeat(60));

    // Step 1: Discover city URLs from main page
    let cityJobs = await discoverCityUrls(page, countryKey);
    await randomDelay();

    if (cityJobs.length === 0) {
      log(`  No cities found for ${countryKey}, skipping`);
      continue;
    }

    // In test mode, limit to 3 cities
    if (isTest) {
      cityJobs = cityJobs.slice(0, 3);
      log(`  TEST MODE: limited to ${cityJobs.length} cities`);
    }

    // Filter out already-completed cities
    const citiesToScrape = cityJobs.filter(c => {
      const key = `${config.code}/${c.slug}`;
      return !completedCities.includes(key);
    });

    log(`  Total: ${cityJobs.length} cities | Remaining: ${citiesToScrape.length}`);

    // Step 2: Scrape each city
    for (let ci = 0; ci < citiesToScrape.length; ci++) {
      const cityJob = citiesToScrape[ci];
      const cityKey = `${config.code}/${cityJob.slug}`;
      totalProcessed++;
      log(`\n[${ci + 1}/${citiesToScrape.length}] ${cityJob.city}, ${countryKey}${cityJob.state ? ' (' + cityJob.state + ')' : ''}`);

      try {
        // Get profile URLs from listing page
        const profileUrls = await getProfileUrls(page, cityJob.url);

        if (profileUrls.length === 0) {
          totalNoResults++;
          completedCities.push(cityKey);
          await randomDelay();
          continue;
        }

        // In test mode, limit profiles per city
        const urlsToScrape = isTest ? profileUrls.slice(0, 3) : profileUrls;
        let newCount = 0;

        // Scrape each profile
        for (let i = 0; i < urlsToScrape.length; i++) {
          const profileUrl = urlsToScrape[i];
          log(`  [${i + 1}/${urlsToScrape.length}] Scraping profile...`);

          const lead = await scrapeProfile(page, profileUrl, cityJob, config.country);

          if (lead) {
            const key = dedupKey(lead.firm_name, lead.city);
            if (!seenKeys.has(key)) {
              seenKeys.add(key);
              allLeads.push(lead);
              newCount++;
              log(`    + ${lead.firm_name} | ${lead.email || 'no email'} | ${lead.website || 'no website'}`);
            } else {
              log(`    = Duplicate: ${lead.firm_name}`);
            }
          }

          await randomDelay(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX);
        }

        totalNew += newCount;
        log(`  City total: ${urlsToScrape.length} profiles, ${newCount} new`);

        completedCities.push(cityKey);

        // Save progress every 5 cities
        if (totalProcessed % 5 === 0 && !isTest) {
          saveProgress(completedCities, allLeads);
          writeCSV(allLeads, OUT_FILE);
          log(`  [Progress saved: ${allLeads.length} total leads]`);
        }

      } catch (err) {
        log(`  ERROR: ${err.message}`);
        totalErrors++;
      }

      await randomDelay();
    }
  }

  await browser.close();

  // Final write
  writeCSV(allLeads, OUT_FILE);
  if (!isTest) saveProgress(completedCities, allLeads);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueCountries = new Set(allLeads.map(l => l.country)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;

  const byCountry = {};
  allLeads.forEach(l => {
    byCountry[l.country] = (byCountry[l.country] || 0) + 1;
  });

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Cities scraped: ${totalProcessed} (${totalNoResults} empty, ${totalErrors} errors)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With email: ${withEmail}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Unique countries: ${uniqueCountries}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`By country: ${JSON.stringify(byCountry)}`);
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && totalErrors === 0) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
