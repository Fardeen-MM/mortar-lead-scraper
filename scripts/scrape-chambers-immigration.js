#!/usr/bin/env node
/**
 * Chambers & Partners Immigration Lawyers Scraper
 *
 * Scrapes https://chambers.com immigration rankings for US and UK.
 * Uses the Chambers ranking-tables API (pure HTTP, no Puppeteer needed)
 * to get ranked organisations and individuals, then visits department
 * profile pages to extract phone, website, address, and team members.
 *
 * Data flow:
 *   1. Fetch ranked organisations from ranking-tables API (bands 1-4)
 *   2. Fetch ranked individuals from ranking-tables API (all bands)
 *   3. For each organisation, visit department page to get contact info
 *   4. Merge individual lawyer data with firm contact info
 *   5. Output CSV
 *
 * Usage:
 *   node scripts/scrape-chambers-immigration.js --test              # 3 firms per country
 *   node scripts/scrape-chambers-immigration.js --country US        # US only
 *   node scripts/scrape-chambers-immigration.js --country UK        # UK only
 *   node scripts/scrape-chambers-immigration.js --country ALL       # Both US + UK (default)
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
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-chambers.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Chambers ranking subsection IDs for immigration
const RANKINGS = {
  US: {
    subsectionId: 539094,
    publicationTypeId: 5,
    practiceAreaId: 31,
    locationId: 12788,
    locationSlug: 'usa',
    country: 'US',
    countryName: 'USA',
  },
  UK: {
    subsectionId: 545124,
    publicationTypeId: 1,
    practiceAreaId: 132,
    locationId: 11805,
    locationSlug: 'uk',
    country: 'UK',
    countryName: 'UK',
  },
};

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

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const cleaned = rawPhone.trim();
  // Already formatted international numbers - return as-is
  if (cleaned.startsWith('+')) return cleaned;
  if (cleaned.startsWith('(0')) return cleaned; // UK format like (020) 7814 1200
  // US 10-digit
  const digits = cleaned.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return cleaned;
}

function parseName(displayName) {
  if (!displayName) return { first_name: '', last_name: '' };
  const parts = displayName.trim().split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  // Handle names like "H Ronald Klasko" or "Stephen W Yale-Loehr"
  const last = parts[parts.length - 1];
  const first = parts.slice(0, -1).join(' ');
  return { first_name: first, last_name: last };
}

function slugify(name) {
  return (name || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function dedupKey(firstName, lastName, firmName) {
  return `${(firstName || '').toLowerCase()}|${(lastName || '').toLowerCase()}|${(firmName || '').toLowerCase()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8',
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
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
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
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Origin': 'https://chambers.com',
        'Referer': 'https://chambers.com/',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return fetchJSON(redirectUrl).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        return resolve(null);
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch {
          resolve(null);
        }
      });
      res.on('error', reject);
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timed out'));
    });
  });
}

// ── API Fetchers ─────────────────────────────────────────────────────────────

async function fetchRankedOrganisations(subsectionId) {
  const url = `https://ranking-tables.chambers.com/api/subsections/${subsectionId}/ranked-organisations`;
  log(`  Fetching ranked organisations from API...`);
  const data = await fetchJSON(url);
  if (!data || !data.categories) return [];

  const orgs = [];
  for (const cat of data.categories) {
    for (const org of (cat.organisations || [])) {
      orgs.push({
        organisationId: org.organisationId,
        parentOrganisationId: org.parentOrganisationId,
        organisationName: org.displayName || org.organisationName,
        band: cat.description,
        rankedYearsCount: org.rankedYearsCount || 0,
        isPaid: org.isPaid || false,
      });
    }
  }
  return orgs;
}

async function fetchRankedIndividuals(subsectionId) {
  const url = `https://ranking-tables.chambers.com/api/subsections/${subsectionId}/ranked-individuals`;
  log(`  Fetching ranked individuals from API...`);
  let data = await fetchJSON(url);
  if (!data) return [];
  // API returns array wrapper: [{description, categories}]
  if (Array.isArray(data)) data = data[0];
  if (!data || !data.categories) return [];

  const individuals = [];
  for (const cat of data.categories) {
    for (const ind of (cat.individuals || [])) {
      individuals.push({
        personOrganisationId: ind.personOrganisationId,
        displayName: ind.displayName || ind.originalDisplayName,
        organisationId: ind.organisationId,
        organisationName: ind.organisationName,
        parentOrganisationId: ind.parentOrganisationId,
        band: cat.description,
        rankedYearsCount: ind.rankedYearsCount || 0,
        isDepartmentHead: ind.isDepartmentHead || false,
      });
    }
  }
  return individuals;
}

// ── Department Page Scraper ──────────────────────────────────────────────────

function buildDepartmentUrl(orgName, ranking, orgId) {
  const slug = slugify(orgName);
  const { publicationTypeId, practiceAreaId, locationId } = ranking;
  // Pattern: /department/{slug}-immigration-{location}-{pubTypeId}:{practiceAreaId}:{locationId}:1:{orgId}
  let locationPart;
  if (ranking.country === 'US') {
    locationPart = 'immigration-usa';
  } else {
    locationPart = 'immigration-business-uk';
  }
  return `https://chambers.com/department/${slug}-${locationPart}-${publicationTypeId}:${practiceAreaId}:${locationId}:1:${orgId}`;
}

function buildLawyerProfileUrl(displayName, ranking, personOrgId) {
  const slug = slugify(displayName);
  const { publicationTypeId } = ranking;
  return `https://chambers.com/lawyer/${slug}-${ranking.locationSlug}-${publicationTypeId}:${personOrgId}`;
}

async function scrapeDepartmentPage(url) {
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) return null;

    const $ = cheerio.load(body);
    const result = {
      phone: '',
      website: '',
      city: '',
      address: '',
      teamMembers: [],
    };

    // The Chambers pages are Angular SPAs, but some data is in the initial HTML
    // and much is in embedded JSON/script tags

    const text = body;

    // Extract phone - look for tel: links or phone patterns
    const telMatch = text.match(/href="tel:([^"]+)"/);
    if (telMatch) {
      result.phone = formatPhone(decodeURIComponent(telMatch[1]));
    }
    // Also try to find phone in visible text
    if (!result.phone) {
      // US phone pattern
      const usPhoneMatch = text.match(/(?:\+1\s?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/);
      if (usPhoneMatch) {
        result.phone = formatPhone(usPhoneMatch[0]);
      }
      // UK phone pattern
      if (!result.phone) {
        const ukPhoneMatch = text.match(/\(?0\d{2,4}\)?\s?\d{3,4}\s?\d{3,4}/);
        if (ukPhoneMatch) {
          result.phone = formatPhone(ukPhoneMatch[0]);
        }
      }
      // International +XX format
      if (!result.phone) {
        const intlPhoneMatch = text.match(/\+\d{1,3}\s?\d{3}\s?\d{3}\s?\d{3,4}/);
        if (intlPhoneMatch) {
          result.phone = formatPhone(intlPhoneMatch[0]);
        }
      }
    }

    // Extract website - look for external links or webLink in JSON
    const webLinkMatch = text.match(/"(?:organisationWebLink|webLink)"\s*:\s*"([^"]+)"/);
    if (webLinkMatch) {
      let w = webLinkMatch[1];
      if (w && !w.includes('chambers.com')) {
        result.website = w.replace(/\\u002F/g, '/');
      }
    }
    // Also try href with external URLs in "Visit website" type links
    if (!result.website) {
      const extLinkMatch = text.match(/href="(https?:\/\/(?!(?:www\.)?chambers\.com)[^"]+)"[^>]*>(?:[^<]*(?:website|visit|firm))/i);
      if (extLinkMatch) {
        result.website = extLinkMatch[1];
      }
    }

    // Extract city/location from address or JSON
    const addressMatch = text.match(/"(?:addressLocality|city)"\s*:\s*"([^"]+)"/);
    if (addressMatch) {
      result.city = addressMatch[1];
    }
    // Try to extract from visible content
    if (!result.city) {
      const locationMatch = text.match(/(?:Headquarters|Office)[^<]*?([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*,\s*([A-Z]{2}|\w+)/);
      if (locationMatch) {
        result.city = locationMatch[1];
      }
    }

    // Extract team members from the page
    // Look for lawyer profile links: /lawyer/{slug}-{location}-{pubTypeId}:{personOrgId}
    const lawyerLinks = text.match(/\/lawyer\/[a-z0-9-]+-(?:usa|uk)-\d+:\d+/g);
    if (lawyerLinks) {
      for (const link of lawyerLinks) {
        const match = link.match(/\/lawyer\/([a-z0-9-]+)-(?:usa|uk)-\d+:(\d+)/);
        if (match) {
          const nameParts = match[1].split('-').filter(p => p.length > 0);
          if (nameParts.length >= 2) {
            result.teamMembers.push({
              slug: match[1],
              personOrgId: match[2],
              nameFromSlug: nameParts.map(p => p.charAt(0).toUpperCase() + p.slice(1)).join(' '),
            });
          }
        }
      }
    }

    // Extract email if visible (rare on Chambers but sometimes in JSON)
    const emailMatch = text.match(/"email"\s*:\s*"([^"@\s]+@[^"@\s]+\.[^"]+)"/);
    if (emailMatch) {
      result.email = emailMatch[1];
    }

    // Extract publicPhone from profile-basics JSON
    const phoneJsonMatch = text.match(/"publicPhone"\s*:\s*"([^"]+)"/);
    if (phoneJsonMatch && phoneJsonMatch[1]) {
      result.phone = formatPhone(phoneJsonMatch[1]);
    }

    return result;
  } catch (err) {
    log(`    Error scraping department page: ${err.message}`);
    return null;
  }
}

async function scrapeLawyerProfilePage(url) {
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) return null;

    const result = {
      phone: '',
      email: '',
      website: '',
      city: '',
      title: '',
    };

    const text = body;

    // Extract phone
    const telMatch = text.match(/href="tel:([^"]+)"/);
    if (telMatch) {
      result.phone = formatPhone(decodeURIComponent(telMatch[1]));
    }
    const phoneJsonMatch = text.match(/"publicPhone"\s*:\s*"([^"]+)"/);
    if (phoneJsonMatch && phoneJsonMatch[1] && !result.phone) {
      result.phone = formatPhone(phoneJsonMatch[1]);
    }

    // Extract email
    const emailMatch = text.match(/"email"\s*:\s*"([^"@\s]+@[^"@\s]+\.[^"]+)"/);
    if (emailMatch) {
      result.email = emailMatch[1];
    }

    // Extract website
    const webMatch = text.match(/"(?:webLink|organisationWebLink)"\s*:\s*"([^"]+)"/);
    if (webMatch && !webMatch[1].includes('chambers.com')) {
      result.website = webMatch[1].replace(/\\u002F/g, '/');
    }

    // Extract city from location text or JSON
    const cityMatch = text.match(/"(?:addressLocality|city)"\s*:\s*"([^"]+)"/);
    if (cityMatch) {
      result.city = cityMatch[1];
    }

    return result;
  } catch (err) {
    log(`    Error scraping lawyer profile: ${err.message}`);
    return null;
  }
}

// ── Main Scrape Logic ────────────────────────────────────────────────────────

async function scrapeCountry(ranking, isTest) {
  const { country, countryName, subsectionId } = ranking;
  log(`\n${'='.repeat(60)}`);
  log(`Scraping ${countryName} Immigration Rankings (subsection ${subsectionId})`);
  log('='.repeat(60));

  // Step 1: Fetch ranked organisations (firms)
  const orgs = await fetchRankedOrganisations(subsectionId);
  log(`  Found ${orgs.length} ranked organisations`);
  await randomDelay();

  // Step 2: Fetch ranked individuals (lawyers)
  const individuals = await fetchRankedIndividuals(subsectionId);
  log(`  Found ${individuals.length} ranked individuals`);
  await randomDelay();

  // Build org lookup: orgId -> org data
  const orgMap = new Map();
  for (const org of orgs) {
    orgMap.set(org.organisationId, org);
  }

  // Build individual lookup by org: orgId -> [individuals]
  const indByOrg = new Map();
  for (const ind of individuals) {
    // Use parentOrganisationId to match, falling back to organisationId
    const orgId = ind.parentOrganisationId || ind.organisationId;
    if (!indByOrg.has(orgId)) indByOrg.set(orgId, []);
    indByOrg.get(orgId).push(ind);
    // Also map by direct organisationId if different
    if (ind.organisationId !== orgId) {
      if (!indByOrg.has(ind.organisationId)) indByOrg.set(ind.organisationId, []);
      indByOrg.get(ind.organisationId).push(ind);
    }
  }

  // Merge: get all unique org IDs from both orgs and individuals
  const allOrgIds = new Set();
  for (const org of orgs) {
    allOrgIds.add(org.organisationId);
  }
  for (const ind of individuals) {
    allOrgIds.add(ind.parentOrganisationId || ind.organisationId);
    allOrgIds.add(ind.organisationId);
  }

  // Step 3: Scrape department pages for contact details
  const leads = [];
  const seenKeys = new Set();
  const orgDetails = new Map(); // orgId -> { phone, website, city, ... }

  // Limit firms in test mode
  let orgList = orgs;
  if (isTest) {
    orgList = orgs.slice(0, 3);
    log(`  TEST MODE: limiting to ${orgList.length} firms`);
  }

  log(`\n--- Scraping ${orgList.length} firm department pages ---`);

  for (let i = 0; i < orgList.length; i++) {
    const org = orgList[i];
    const deptUrl = buildDepartmentUrl(org.organisationName, ranking, org.organisationId);
    log(`  [${i + 1}/${orgList.length}] ${org.organisationName} (${org.band})`);
    log(`    URL: ${deptUrl}`);

    const details = await scrapeDepartmentPage(deptUrl);
    if (details) {
      orgDetails.set(org.organisationId, details);
      // Also map parent org
      if (org.parentOrganisationId && org.parentOrganisationId !== org.organisationId) {
        orgDetails.set(org.parentOrganisationId, details);
      }
      log(`    Phone: ${details.phone || '-'} | Website: ${details.website || '-'} | City: ${details.city || '-'} | Team: ${details.teamMembers.length}`);
    } else {
      log(`    No data extracted from department page`);
    }

    await randomDelay();
  }

  // Step 4: Build lead records
  // First, add all ranked individuals as leads
  log(`\n--- Building lead records ---`);

  for (const ind of individuals) {
    const { first_name, last_name } = parseName(ind.displayName);
    const key = dedupKey(first_name, last_name, ind.organisationName);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    // Look up org details
    const orgDetail = orgDetails.get(ind.organisationId) || orgDetails.get(ind.parentOrganisationId) || {};
    const profileUrl = buildLawyerProfileUrl(ind.displayName, ranking, ind.personOrganisationId);

    const website = (orgDetail.website || '').replace(/\/$/, '');

    leads.push({
      first_name,
      last_name,
      firm_name: ind.organisationName || '',
      title: ind.isDepartmentHead ? 'Department Head' : ind.band || '',
      email: orgDetail.email || '',
      phone: orgDetail.phone || '',
      website,
      domain: extractDomain(website),
      city: orgDetail.city || '',
      state: '',
      country,
      niche: 'immigration',
      source: 'chambers',
      profile_url: profileUrl,
    });
  }

  // Also add firms without individual lawyers as firm-level leads
  for (const org of orgs) {
    // Check if any individuals were already added for this firm
    const firmIndividuals = indByOrg.get(org.organisationId) || indByOrg.get(org.parentOrganisationId) || [];
    if (firmIndividuals.length > 0) continue; // Already have individual leads

    const key = dedupKey('', '', org.organisationName);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    const orgDetail = orgDetails.get(org.organisationId) || {};
    const deptUrl = buildDepartmentUrl(org.organisationName, ranking, org.organisationId);
    const website = (orgDetail.website || '').replace(/\/$/, '');

    leads.push({
      first_name: '',
      last_name: '',
      firm_name: org.organisationName || '',
      title: org.band || '',
      email: orgDetail.email || '',
      phone: orgDetail.phone || '',
      website,
      domain: extractDomain(website),
      city: orgDetail.city || '',
      state: '',
      country,
      niche: 'immigration',
      source: 'chambers',
      profile_url: deptUrl,
    });
  }

  log(`  Built ${leads.length} leads for ${countryName}`);
  return leads;
}

// ── Enrichment Pass: visit individual lawyer profile pages ───────────────────

async function enrichLeadProfiles(leads, ranking, isTest) {
  // Only enrich leads that are missing phone/email
  const toEnrich = leads.filter(l => l.first_name && l.last_name && (!l.phone || !l.email));
  const limit = isTest ? Math.min(5, toEnrich.length) : toEnrich.length;

  if (limit === 0) {
    log(`  No individual profiles to enrich`);
    return;
  }

  log(`\n--- Enriching ${limit} lawyer profiles ---`);

  for (let i = 0; i < limit; i++) {
    const lead = toEnrich[i];
    log(`  [${i + 1}/${limit}] ${lead.first_name} ${lead.last_name} (${lead.firm_name})`);

    const profileData = await scrapeLawyerProfilePage(lead.profile_url);
    if (profileData) {
      if (profileData.phone && !lead.phone) lead.phone = profileData.phone;
      if (profileData.email && !lead.email) lead.email = profileData.email;
      if (profileData.website && !lead.website) {
        lead.website = profileData.website.replace(/\/$/, '');
        lead.domain = extractDomain(lead.website);
      }
      if (profileData.city && !lead.city) lead.city = profileData.city;
      log(`    Phone: ${lead.phone || '-'} | Email: ${lead.email || '-'} | City: ${lead.city || '-'}`);
    } else {
      log(`    No additional data found`);
    }

    await randomDelay();
  }
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');

  let countryArg = 'ALL';
  const countryIdx = args.indexOf('--country');
  if (countryIdx !== -1 && args[countryIdx + 1]) {
    countryArg = args[countryIdx + 1].toUpperCase();
  }

  if (args.includes('--help') || args.includes('-h')) {
    console.log('Usage:');
    console.log('  node scripts/scrape-chambers-immigration.js --test              # Test mode (3 firms)');
    console.log('  node scripts/scrape-chambers-immigration.js --country US        # US only');
    console.log('  node scripts/scrape-chambers-immigration.js --country UK        # UK only');
    console.log('  node scripts/scrape-chambers-immigration.js --country ALL       # Both (default)');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('Chambers & Partners Immigration Lawyers Scraper');
  log(`Country: ${countryArg} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const countries = countryArg === 'ALL' ? ['US', 'UK'] : [countryArg];

  for (const c of countries) {
    const ranking = RANKINGS[c];
    if (!ranking) {
      log(`Unknown country: ${c}, skipping`);
      continue;
    }

    try {
      const leads = await scrapeCountry(ranking, isTest);

      // Enrichment pass: visit individual lawyer profiles for more data
      await enrichLeadProfiles(leads, ranking, isTest);

      allLeads.push(...leads);
    } catch (err) {
      log(`ERROR scraping ${c}: ${err.message}`);
      console.error(err);
    }
  }

  // Deduplicate across countries (some firms appear in both US and UK)
  const finalLeads = [];
  const finalKeys = new Set();
  for (const lead of allLeads) {
    const key = dedupKey(lead.first_name, lead.last_name, lead.firm_name);
    if (finalKeys.has(key)) continue;
    finalKeys.add(key);
    finalLeads.push(lead);
  }

  // Write CSV
  writeCSV(finalLeads, OUT_FILE);

  // Stats
  const withPhone = finalLeads.filter(l => l.phone).length;
  const withEmail = finalLeads.filter(l => l.email).length;
  const withWebsite = finalLeads.filter(l => l.website).length;
  const withName = finalLeads.filter(l => l.first_name && l.last_name).length;
  const firmOnly = finalLeads.filter(l => !l.first_name).length;
  const usLeads = finalLeads.filter(l => l.country === 'US').length;
  const ukLeads = finalLeads.filter(l => l.country === 'UK').length;

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Total leads: ${finalLeads.length} (${usLeads} US, ${ukLeads} UK)`);
  log(`With name: ${withName} | Firm-only: ${firmOnly}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
