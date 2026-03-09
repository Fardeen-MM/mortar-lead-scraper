#!/usr/bin/env node
/**
 * IECA Education Consultants Scraper
 *
 * Scrapes Independent Educational Consultants Association (IECA) directory
 * via WordPress REST API at https://www.iecaonline.com
 *
 * NOTE: Expertise.com has no education category (only Legal, Home Improvement,
 * Finance, Insurance, Business, Health & Wellness, Lifestyle). IECA is the
 * premier directory for independent educational consultants in the US.
 *
 * Strategy:
 *   1. Fetch taxonomy terms (consulting areas, advising specialties) for labeling
 *   2. Paginate WP REST API: /wp-json/wp/v2/directory-entry?per_page=100&page=N
 *   3. Extract ACF fields: name, company, phone, email, address, website, social
 *   4. Resolve consulting area taxonomy IDs to human-readable names
 *   5. Dedup by email or name+city
 *   6. Output CSV
 *
 * Data source: 2,500+ directory entries (Professional + Associate members)
 *   - Professional members: 1,118 (fully credentialed)
 *   - Associate members: 1,414 (in training/newer)
 *   - ~2,277 specialize in College Admissions
 *
 * Usage:
 *   node scripts/scrape-expertise-education.js --test         # 2 pages (200 entries)
 *   node scripts/scrape-expertise-education.js --all-cities   # All 26 pages (~2,500 entries)
 *   node scripts/scrape-expertise-education.js --resume       # Resume interrupted scrape
 */

const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const PER_PAGE = 100;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'us-education-consultants-expertise.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'expertise-education-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'linkedin', 'city', 'state', 'zip', 'country',
  'consulting_areas', 'advising_specialties', 'membership_type',
  'niche', 'source', 'profile_url',
];

// Taxonomy ID → Name maps (populated at startup)
const CONSULTING_AREA_MAP = {};
const ADVISING_AREA_MAP = {};
const MEMBERSHIP_MAP = {};

// ── US State Name → Abbreviation ─────────────────────────────────────────────

const STATE_ABBREV = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'district of columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI',
  'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
  'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME',
  'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
  'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
  'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM',
  'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
  'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI',
  'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX',
  'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
  'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
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

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : 'http://' + url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function normalizeState(stateName) {
  if (!stateName) return '';
  // Already an abbreviation?
  if (/^[A-Z]{2}$/.test(stateName.trim())) return stateName.trim();
  const abbr = STATE_ABBREV[stateName.toLowerCase().trim()];
  return abbr || stateName.trim();
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

function dedupKey(email, firstName, lastName, city) {
  if (email) return `email:${email.toLowerCase().trim()}`;
  return `name:${(firstName || '').toLowerCase().trim()}|${(lastName || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

// ── JSON Fetcher (using built-in fetch) ──────────────────────────────────────

async function httpsGetJson(url, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

      const res = await fetch(url, {
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
        },
        signal: controller.signal,
        redirect: 'follow',
      });

      clearTimeout(timeout);

      if (res.status === 429 || res.status === 403) {
        if (attempt < retries) {
          const wait = (attempt + 1) * 5000;
          log(`  Rate limited (${res.status}), waiting ${wait / 1000}s...`);
          await sleep(wait);
          continue;
        }
        return { statusCode: res.status, data: null, totalPages: 0 };
      }

      if (res.status !== 200) {
        return { statusCode: res.status, data: null, totalPages: 0 };
      }

      const data = await res.json();
      const totalPages = parseInt(res.headers.get('x-wp-totalpages') || '0', 10);
      const totalItems = parseInt(res.headers.get('x-wp-total') || '0', 10);
      return { statusCode: 200, data, totalPages, totalItems };

    } catch (err) {
      if (attempt < retries) {
        log(`  Request error: ${err.message}, retrying (${attempt + 1}/${retries})...`);
        await sleep(3000);
        continue;
      }
      throw err;
    }
  }
}

// ── Taxonomy Loader ──────────────────────────────────────────────────────────

async function loadTaxonomies() {
  log('Loading taxonomy terms...');

  // Consulting areas
  const areasResp = await httpsGetJson('https://www.iecaonline.com/wp-json/wp/v2/consulting-area?per_page=100');
  if (areasResp.data) {
    for (const term of areasResp.data) {
      CONSULTING_AREA_MAP[term.id] = term.name;
    }
    log(`  Loaded ${Object.keys(CONSULTING_AREA_MAP).length} consulting area terms`);
  }

  await sleep(1000);

  // Additional advising areas
  const advisingResp = await httpsGetJson('https://www.iecaonline.com/wp-json/wp/v2/additional-advising-area?per_page=100');
  if (advisingResp.data) {
    for (const term of advisingResp.data) {
      ADVISING_AREA_MAP[term.id] = term.name;
    }
    log(`  Loaded ${Object.keys(ADVISING_AREA_MAP).length} advising area terms`);
  }

  await sleep(1000);

  // Membership types
  const memberResp = await httpsGetJson('https://www.iecaonline.com/wp-json/wp/v2/membership-type?per_page=100');
  if (memberResp.data) {
    for (const term of memberResp.data) {
      MEMBERSHIP_MAP[term.id] = term.name;
    }
    log(`  Loaded ${Object.keys(MEMBERSHIP_MAP).length} membership type terms`);
  }
}

// ── Entry Parser ─────────────────────────────────────────────────────────────

function parseEntry(entry) {
  const acf = entry.acf || {};

  const firstName = (acf['member-directory-first-name-to-use'] || acf['member-directory-first-name'] || '').trim();
  const lastName = (acf['member-directory-last-name'] || '').trim();
  const company = (acf['member-directory-company'] || '').trim();
  const phone = (acf['member-directory-primary-phone'] || '').trim();
  const email = (acf['member-directory-primary-email'] || '').trim();
  const website = (acf['member-directory-website'] || '').trim();
  const linkedin = (acf['member-directory-linkedin'] || '').trim();

  // Address (first address in array)
  const addresses = acf['member-directory-addresses'] || [];
  const addr = addresses[0] || {};
  const city = (addr['member-directory-city'] || '').trim();
  const stateRaw = (addr['member-directory-state'] || '').trim();
  const zip = (addr['member-directory-zip'] || '').trim();
  const country = (addr['member-directory-country'] || '').trim();
  const state = normalizeState(stateRaw);

  // Consulting areas (taxonomy IDs → names, deduped)
  const areaIds = entry['consulting-area'] || [];
  const consultingAreas = [...new Set(
    areaIds.map(id => CONSULTING_AREA_MAP[id]).filter(Boolean)
  )].join('; ');

  // Additional advising areas (deduped)
  const advisingIds = entry['additional-advising-area'] || [];
  const advisingAreas = [...new Set(
    advisingIds.map(id => ADVISING_AREA_MAP[id]).filter(Boolean)
  )].join('; ');

  // Membership type
  const memberIds = entry['membership-type'] || [];
  const memberType = memberIds
    .map(id => MEMBERSHIP_MAP[id])
    .filter(Boolean)
    .join('; ');

  const profileUrl = entry.link || '';

  if (!firstName && !lastName && !company) return null;

  return {
    first_name: firstName,
    last_name: lastName,
    firm_name: company,
    title: 'Independent Educational Consultant',
    email,
    phone,
    website,
    domain: extractDomain(website),
    linkedin,
    city,
    state,
    zip,
    country,
    consulting_areas: consultingAreas,
    advising_specialties: advisingAreas,
    membership_type: memberType,
    niche: 'education',
    source: 'ieca',
    profile_url: profileUrl,
  };
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedPages: [], leads: [] };
}

function saveProgress(completedPages, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedPages,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAllCities = args.includes('--all-cities');
  const isResume = args.includes('--resume');

  if (!isTest && !isAllCities && !isResume) {
    console.log('IECA Education Consultants Scraper');
    console.log('');
    console.log('NOTE: Expertise.com has no education category. This scraper uses IECA');
    console.log('(Independent Educational Consultants Association) which is the premier');
    console.log('directory for independent educational consultants (2,500+ members).');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-expertise-education.js --test         # Test (2 pages, ~200 entries)');
    console.log('  node scripts/scrape-expertise-education.js --all-cities   # All entries (~2,500)');
    console.log('  node scripts/scrape-expertise-education.js --resume       # Resume interrupted scrape');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Load taxonomies first
  await loadTaxonomies();

  let allLeads = [];
  let completedPages = [];
  const seenKeys = new Set();

  // Resume support
  if (isResume) {
    const progress = loadProgress();
    if (progress.completedPages.length > 0) {
      completedPages = progress.completedPages;
      allLeads = progress.leads || [];
      for (const lead of allLeads) {
        seenKeys.add(dedupKey(lead.email, lead.first_name, lead.last_name, lead.city));
      }
      log(`Resuming: ${completedPages.length} pages done, ${allLeads.length} leads loaded`);
    }
  }

  // Discover total pages
  log('Fetching page 1 to discover total pages...');
  const firstUrl = `https://www.iecaonline.com/wp-json/wp/v2/directory-entry?per_page=${PER_PAGE}&page=1`;
  const firstResp = await httpsGetJson(firstUrl);

  if (!firstResp.data) {
    log('ERROR: Failed to fetch first page');
    process.exit(1);
  }

  const totalPages = firstResp.totalPages;
  const totalItems = firstResp.totalItems;
  log(`Total entries: ${totalItems} across ${totalPages} pages`);

  let maxPages = totalPages;
  if (isTest) {
    maxPages = 2;
    log(`TEST MODE: limiting to ${maxPages} pages (~${maxPages * PER_PAGE} entries)`);
  }

  log(`Output: ${OUT_FILE}`);
  log('');

  let processed = 0;
  let totalNew = 0;
  let errors = 0;

  // Process first page data if not already done
  if (!completedPages.includes(1)) {
    processed++;
    const entries = firstResp.data;
    let pageNew = 0;

    for (const entry of entries) {
      const lead = parseEntry(entry);
      if (!lead) continue;

      const key = dedupKey(lead.email, lead.first_name, lead.last_name, lead.city);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      pageNew++;
    }

    totalNew += pageNew;
    completedPages.push(1);
    log(`[1/${maxPages}] Page 1: ${entries.length} entries, ${pageNew} new`);
  } else {
    log(`[1/${maxPages}] Page 1: already done (resumed)`);
  }

  // Fetch remaining pages
  for (let page = 2; page <= maxPages; page++) {
    if (completedPages.includes(page)) {
      log(`[${page}/${maxPages}] Page ${page}: already done (resumed)`);
      continue;
    }

    await randomDelay();
    processed++;

    const url = `https://www.iecaonline.com/wp-json/wp/v2/directory-entry?per_page=${PER_PAGE}&page=${page}`;
    log(`[${page}/${maxPages}] Fetching page ${page}...`);

    try {
      const resp = await httpsGetJson(url);

      if (!resp.data || resp.statusCode !== 200) {
        log(`  HTTP ${resp.statusCode} - skipping page ${page}`);
        errors++;
        continue;
      }

      const entries = resp.data;
      let pageNew = 0;

      for (const entry of entries) {
        const lead = parseEntry(entry);
        if (!lead) continue;

        const key = dedupKey(lead.email, lead.first_name, lead.last_name, lead.city);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        allLeads.push(lead);
        pageNew++;
      }

      totalNew += pageNew;
      completedPages.push(page);
      log(`  ${entries.length} entries, ${pageNew} new (total: ${allLeads.length})`);

      // Save progress every 5 pages
      if (page % 5 === 0 && !isTest) {
        saveProgress(completedPages, allLeads);
        writeCSV(allLeads, OUT_FILE);
        log(`  [Progress saved: ${allLeads.length} leads]`);
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
      errors++;
    }
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);
  saveProgress(completedPages, allLeads);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withLinkedin = allLeads.filter(l => l.linkedin).length;
  const usLeads = allLeads.filter(l => l.country === 'United States').length;
  const intlLeads = allLeads.length - usLeads;
  const uniqueStates = new Set(allLeads.filter(l => l.country === 'United States').map(l => l.state)).size;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;
  const professionals = allLeads.filter(l => l.membership_type.includes('Professional')).length;
  const associates = allLeads.filter(l => l.membership_type.includes('Associate')).length;

  // Top consulting areas
  const areaCounts = {};
  for (const lead of allLeads) {
    if (lead.consulting_areas) {
      for (const area of lead.consulting_areas.split('; ')) {
        areaCounts[area] = (areaCounts[area] || 0) + 1;
      }
    }
  }
  const topAreas = Object.entries(areaCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  // Top states
  const stateCounts = {};
  for (const lead of allLeads.filter(l => l.country === 'United States')) {
    const st = lead.state || 'Unknown';
    stateCounts[st] = (stateCounts[st] || 0) + 1;
  }
  const topStates = Object.entries(stateCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);

  log('');
  log('='.repeat(60));
  log('DONE - IECA Education Consultants');
  log(`Pages fetched: ${completedPages.length} (${errors} errors)`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`  US leads: ${usLeads} | International: ${intlLeads}`);
  log(`  Professional members: ${professionals}`);
  log(`  Associate members: ${associates}`);
  log(`With email: ${withEmail} (${(withEmail/allLeads.length*100).toFixed(1)}%)`);
  log(`With phone: ${withPhone} (${(withPhone/allLeads.length*100).toFixed(1)}%)`);
  log(`With website: ${withWebsite}`);
  log(`With LinkedIn: ${withLinkedin}`);
  log(`Unique US states: ${uniqueStates}`);
  log(`Unique cities: ${uniqueCities}`);
  log('');
  log('Top consulting areas:');
  for (const [area, count] of topAreas) {
    log(`  ${area}: ${count}`);
  }
  log('');
  log('Top US states:');
  for (const [state, count] of topStates) {
    log(`  ${state}: ${count}`);
  }
  log('');
  log(`Output: ${OUT_FILE}`);

  // Clean up progress file on successful complete run
  if (!isTest && errors === 0 && completedPages.length >= totalPages) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
    log('Progress file cleaned up (complete run)');
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
