#!/usr/bin/env node
/**
 * Scrape ALL registered real estate agents from BCFSA (BC Financial Services Authority).
 *
 * Source: https://www.bcfsa.ca/find-professional-or-organization
 * Method: Algolia Search API (index: crs_production, filter: bundle:re_licencee)
 *         Phase 1 — bulk fetch all agents via Algolia (name, firm, city, licence#)
 *         Phase 2 — fetch profile pages for phone numbers + full addresses
 *
 * Expected: ~29,000+ active licensees across British Columbia
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, fs, path).
 *
 * Features:
 *   - Algolia API pagination (1000 per page, ~30 pages total)
 *   - Profile page scraping for phone numbers and full postal addresses
 *   - Name splitting (first_name / last_name) with multi-word handling
 *   - Deduplication by licence_number
 *   - Auto-saves progress CSV every 500 new leads
 *   - Rate limiting between profile fetches (configurable, default 500ms)
 *   - Resume support (reads existing progress file)
 *   - Test mode (--test): only fetches 2 Algolia pages + 5 profiles
 *   - Skip profiles mode (--skip-profiles): only phase 1, no profile scraping
 *
 * Usage:
 *   node scripts/scrape-bcfsa-agents.js
 *   node scripts/scrape-bcfsa-agents.js --test
 *   node scripts/scrape-bcfsa-agents.js --skip-profiles
 *   node scripts/scrape-bcfsa-agents.js --delay=500
 *   node scripts/scrape-bcfsa-agents.js --resume
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS       = parseInt(args.delay) || 500;
const TEST_MODE      = !!args.test;
const SKIP_PROFILES  = !!args['skip-profiles'];
const RESUME         = !!args.resume;
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const OUTPUT_FILE    = path.join(OUTPUT_DIR, 'bcfsa-real-estate-agents.csv');
const SAVE_EVERY     = 500;

// ── Algolia config (public search-only key from bcfsa.ca frontend JS) ───
const ALGOLIA_APP_ID  = 'FV0XPB3EG4';
const ALGOLIA_API_KEY = 'f20efa4239f498465a006b0e4afa50dc';
const ALGOLIA_INDEX   = 'crs_production';
const ALGOLIA_HOST    = `${ALGOLIA_APP_ID}-dsn.algolia.net`;
const HITS_PER_PAGE   = 1000;

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'licence_number',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  algoliaPages: 0,
  algoliaHits: 0,
  profilesFetched: 0,
  profileErrors: 0,
  phonesFound: 0,
  saved: 0,
  dupes: 0,
  startTime: Date.now(),
};

// ── Lead storage + dedup ────────────────────────────────────────────────
const leads = [];
const seenLicences = new Set();

// ── Helpers ─────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

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
  const sec = s % 60;
  return `${m}m${sec}s`;
}

// ── HTTPS request helpers ───────────────────────────────────────────────
function httpsPost(hostname, urlPath, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(body);
    const options = {
      hostname,
      port: 443,
      path: urlPath,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
        ...headers,
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try { resolve(JSON.parse(data)); }
          catch (e) { reject(new Error(`JSON parse error: ${e.message}`)); }
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.write(postData);
    req.end();
  });
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      port: 443,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    };

    const req = https.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `https://${parsed.hostname}${res.headers.location}`;
        return httpsGet(redirectUrl).then(resolve).catch(reject);
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(data);
        } else {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

// ── Algolia search ──────────────────────────────────────────────────────
async function algoliaSearch(page) {
  const body = {
    query: '',
    filters: 'bundle:re_licencee',
    hitsPerPage: HITS_PER_PAGE,
    page,
    attributesToRetrieve: [
      'licence_number', 'name', 'secondary_name', 'business_name',
      'location', 'address', 'services', 'subtype', 'expiry_date',
      'objectID', 'bundle', 'status_flag', 'team_name',
    ],
    attributesToHighlight: [],
  };

  const headers = {
    'X-Algolia-Application-Id': ALGOLIA_APP_ID,
    'X-Algolia-API-Key': ALGOLIA_API_KEY,
  };

  return httpsPost(
    ALGOLIA_HOST,
    `/1/indexes/${ALGOLIA_INDEX}/query`,
    body,
    headers
  );
}

// ── Name splitting ──────────────────────────────────────────────────────
function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Clean up name: trim, strip leading dots/periods, collapse spaces
  const clean = fullName.trim().replace(/^[.\s]+/, '').replace(/\s+/g, ' ');

  // Handle "Last, First" format
  if (clean.includes(',')) {
    const [last, ...firstParts] = clean.split(',');
    return {
      first_name: firstParts.join(' ').trim(),
      last_name: last.trim(),
    };
  }

  // Handle "First Last" format — last word is last name
  const parts = clean.split(' ');
  if (parts.length === 1) {
    return { first_name: parts[0], last_name: '' };
  }

  // Use secondary_name (known as) if different — it often has better formatting
  return {
    first_name: parts.slice(0, -1).join(' '),
    last_name: parts[parts.length - 1],
  };
}

// ── Map subtype to title ────────────────────────────────────────────────
function mapTitle(subtype, services) {
  const svc = (services || []).join(', ');
  if (subtype === 'Managing Broker') return 'Managing Broker';
  if (subtype === 'Associate Broker') return 'Associate Broker';
  if (subtype === 'Broker') return 'Broker';
  if (subtype === 'Representative') {
    if (svc.includes('strata')) return 'Strata Management Representative';
    if (svc.includes('rental')) return 'Rental Property Management Representative';
    return 'Real Estate Agent';
  }
  return subtype || 'Real Estate Agent';
}

// ── Profile page parsing ────────────────────────────────────────────────
function parseProfilePage(html) {
  const result = { phone: '', address: '' };

  // Extract Business Number (phone)
  const phoneMatch = html.match(/Business Number:<\/dt>\s*<dd[^>]*>\s*([^<]+)/i);
  if (phoneMatch) {
    result.phone = phoneMatch[1].trim();
  }

  // Extract full Business Address
  const addrMatch = html.match(/Business Address:<\/dt>\s*<dd[^>]*>\s*([^<]+)/i);
  if (addrMatch) {
    result.address = addrMatch[1].trim();
  }

  return result;
}

// ── Normalize phone ─────────────────────────────────────────────────────
function normalizePhone(raw) {
  if (!raw) return '';
  // Strip everything except digits
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return raw.trim();
}

// ── Transform Algolia hit to lead ───────────────────────────────────────
function transformHit(hit) {
  // Use secondary_name (known as) for the display name if available
  const displayName = (hit.secondary_name && hit.secondary_name.trim())
    ? hit.secondary_name.trim()
    : (hit.name || '').trim();

  const { first_name, last_name } = splitName(displayName);

  return {
    first_name,
    last_name,
    firm_name: (hit.business_name || '').trim(),
    title: mapTitle(hit.subtype, hit.services),
    email: '',
    phone: '',
    website: '',
    domain: '',
    city: (hit.location || '').trim(),
    state: 'British Columbia',
    country: 'CA',
    niche: 'real estate agent',
    source: 'bcfsa',
    licence_number: (hit.licence_number || hit.objectID || '').replace('crs_entity/', ''),
    // Internal fields (not in CSV)
    _objectID: hit.objectID,
    _address: (hit.address || '').trim(),
    _subtype: hit.subtype,
    _services: (hit.services || []).join(', '),
  };
}

// ── CSV writing ─────────────────────────────────────────────────────────
function writeCSV() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );

  fs.writeFileSync(OUTPUT_FILE, header + '\n' + rows.join('\n') + '\n');
  stats.saved = leads.length;
}

// ── Resume: load existing CSV ───────────────────────────────────────────
function loadExisting() {
  if (!RESUME || !fs.existsSync(OUTPUT_FILE)) return 0;

  const content = fs.readFileSync(OUTPUT_FILE, 'utf8');
  const lines = content.trim().split('\n');
  if (lines.length <= 1) return 0;

  // Parse header
  const header = lines[0].split(',');
  const licIdx = header.indexOf('licence_number');
  if (licIdx === -1) return 0;

  let loaded = 0;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;

    // Simple CSV parse (handles quoted fields)
    const fields = [];
    let field = '';
    let inQuote = false;
    for (let j = 0; j < line.length; j++) {
      const ch = line[j];
      if (inQuote) {
        if (ch === '"' && line[j + 1] === '"') {
          field += '"';
          j++;
        } else if (ch === '"') {
          inQuote = false;
        } else {
          field += ch;
        }
      } else {
        if (ch === '"') {
          inQuote = true;
        } else if (ch === ',') {
          fields.push(field);
          field = '';
        } else {
          field += ch;
        }
      }
    }
    fields.push(field);

    const lead = {};
    for (let k = 0; k < header.length && k < fields.length; k++) {
      lead[header[k]] = fields[k];
    }

    if (lead.licence_number && !seenLicences.has(lead.licence_number)) {
      seenLicences.add(lead.licence_number);
      leads.push(lead);
      loaded++;
    }
  }

  console.log(`  Resumed ${loaded} leads from ${OUTPUT_FILE}`);
  return loaded;
}

// ── Phase 1: Bulk fetch from Algolia ────────────────────────────────────
async function phase1_algoliaFetch() {
  console.log('\n=== Phase 1: Algolia Bulk Fetch ===\n');

  // First page to get total count
  const firstResult = await algoliaSearch(0);
  const totalHits = firstResult.nbHits;
  const totalPages = firstResult.nbPages;

  console.log(`  Total licensees in index: ${totalHits.toLocaleString()}`);
  console.log(`  Pages to fetch: ${totalPages} (${HITS_PER_PAGE}/page)`);
  if (TEST_MODE) console.log('  TEST MODE: limiting to 2 pages\n');

  const maxPages = TEST_MODE ? Math.min(2, totalPages) : totalPages;
  let newCount = 0;

  for (let page = 0; page < maxPages; page++) {
    try {
      const result = page === 0 ? firstResult : await algoliaSearch(page);
      stats.algoliaPages++;

      for (const hit of result.hits) {
        stats.algoliaHits++;
        const licNum = (hit.licence_number || hit.objectID || '').replace('crs_entity/', '');

        if (seenLicences.has(licNum)) {
          stats.dupes++;
          continue;
        }
        seenLicences.add(licNum);

        const lead = transformHit(hit);
        leads.push(lead);
        newCount++;

        // Save progress
        if (newCount > 0 && newCount % SAVE_EVERY === 0) {
          writeCSV();
          console.log(`  [save] ${leads.length.toLocaleString()} leads saved (${elapsed()})`);
        }
      }

      const pct = ((page + 1) / maxPages * 100).toFixed(1);
      console.log(`  Page ${page + 1}/${maxPages} — ${result.hits.length} hits — ` +
        `total: ${leads.length.toLocaleString()} — ${pct}% (${elapsed()})`);

      // Rate limit between pages (be polite)
      if (page < maxPages - 1) await sleep(200);

    } catch (err) {
      console.error(`  ERROR on page ${page}: ${err.message}`);
      // Retry once after 2s
      await sleep(2000);
      try {
        const retry = await algoliaSearch(page);
        for (const hit of retry.hits) {
          const licNum = (hit.licence_number || hit.objectID || '').replace('crs_entity/', '');
          if (!seenLicences.has(licNum)) {
            seenLicences.add(licNum);
            leads.push(transformHit(hit));
            newCount++;
          }
        }
        console.log(`  Page ${page + 1} retry OK — ${retry.hits.length} hits`);
      } catch (retryErr) {
        console.error(`  RETRY FAILED page ${page}: ${retryErr.message}`);
      }
    }
  }

  // Final save after phase 1
  writeCSV();
  console.log(`\n  Phase 1 complete: ${leads.length.toLocaleString()} unique agents fetched`);
  console.log(`  Dupes skipped: ${stats.dupes}`);
  return newCount;
}

// ── Phase 2: Fetch profile pages for phone numbers ──────────────────────
async function phase2_profileFetch() {
  if (SKIP_PROFILES) {
    console.log('\n=== Phase 2: SKIPPED (--skip-profiles) ===\n');
    return;
  }

  console.log('\n=== Phase 2: Profile Page Scraping (phones + addresses) ===\n');

  // Count leads that need profile scraping (no phone yet)
  const needProfile = leads.filter(l => !l.phone);
  const total = needProfile.length;
  const maxProfiles = TEST_MODE ? Math.min(5, total) : total;

  console.log(`  Leads needing profile: ${total.toLocaleString()}`);
  if (TEST_MODE) console.log(`  TEST MODE: limiting to ${maxProfiles} profiles\n`);

  let fetched = 0;
  let errors = 0;
  let phonesFound = 0;

  for (let i = 0; i < maxProfiles; i++) {
    const lead = needProfile[i];
    const licNum = lead.licence_number;

    try {
      const url = `https://www.bcfsa.ca/re-licencee/${licNum}`;
      const html = await httpsGet(url);
      fetched++;
      stats.profilesFetched++;

      const profile = parseProfilePage(html);

      if (profile.phone) {
        lead.phone = normalizePhone(profile.phone);
        phonesFound++;
        stats.phonesFound++;
      }

      if (profile.address && !lead._address) {
        lead._address = profile.address;
      }

      // Progress logging
      if (fetched % 100 === 0 || fetched === maxProfiles) {
        const pct = (fetched / maxProfiles * 100).toFixed(1);
        console.log(`  ${fetched.toLocaleString()}/${maxProfiles.toLocaleString()} profiles — ` +
          `${phonesFound} phones — ${errors} errors — ${pct}% (${elapsed()})`);
      }

      // Save progress
      if (fetched % SAVE_EVERY === 0) {
        writeCSV();
        console.log(`  [save] progress saved at ${fetched} profiles`);
      }

    } catch (err) {
      errors++;
      stats.profileErrors++;
      if (errors <= 5) {
        console.error(`  Profile error [${licNum}]: ${err.message}`);
      } else if (errors === 6) {
        console.error('  (suppressing further profile errors)');
      }
    }

    // Rate limit
    if (i < maxProfiles - 1) await sleep(DELAY_MS);
  }

  // Final save
  writeCSV();
  console.log(`\n  Phase 2 complete: ${fetched} profiles fetched, ${phonesFound} phones found, ${errors} errors`);
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('BCFSA Real Estate Agent Scraper');
  console.log('================================');
  console.log(`Source:  https://www.bcfsa.ca/find-professional-or-organization`);
  console.log(`Output:  ${OUTPUT_FILE}`);
  console.log(`Mode:    ${TEST_MODE ? 'TEST' : 'FULL'}`);
  console.log(`Delay:   ${DELAY_MS}ms`);
  console.log(`Profiles: ${SKIP_PROFILES ? 'SKIP' : 'ENABLED'}`);
  console.log(`Resume:  ${RESUME ? 'YES' : 'NO'}`);

  // Ensure output dir exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Resume from existing file if requested
  if (RESUME) loadExisting();

  // Phase 1: Algolia bulk fetch
  await phase1_algoliaFetch();

  // Phase 2: Profile page scraping
  await phase2_profileFetch();

  // Final stats
  const totalTime = elapsed();
  const withPhone = leads.filter(l => l.phone).length;
  const withFirm = leads.filter(l => l.firm_name).length;
  const cities = new Set(leads.map(l => l.city).filter(Boolean));

  console.log('\n================================');
  console.log('FINAL RESULTS');
  console.log('================================');
  console.log(`Total agents:     ${leads.length.toLocaleString()}`);
  console.log(`With phone:       ${withPhone.toLocaleString()}`);
  console.log(`With firm:        ${withFirm.toLocaleString()}`);
  console.log(`Unique cities:    ${cities.size}`);
  console.log(`Dupes skipped:    ${stats.dupes}`);
  console.log(`Profile errors:   ${stats.profileErrors}`);
  console.log(`Time elapsed:     ${totalTime}`);
  console.log(`Output:           ${OUTPUT_FILE}`);

  // Show top cities
  const cityCounts = {};
  leads.forEach(l => {
    const c = l.city || 'Unknown';
    cityCounts[c] = (cityCounts[c] || 0) + 1;
  });
  const topCities = Object.entries(cityCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  console.log('\nTop 15 Cities:');
  for (const [city, count] of topCities) {
    console.log(`  ${city.padEnd(30)} ${count.toLocaleString()}`);
  }

  // Show licence type breakdown
  const typeCounts = {};
  leads.forEach(l => {
    const t = l.title || 'Unknown';
    typeCounts[t] = (typeCounts[t] || 0) + 1;
  });
  console.log('\nLicence Types:');
  for (const [type, count] of Object.entries(typeCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${type.padEnd(45)} ${count.toLocaleString()}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  // Save what we have
  if (leads.length > 0) {
    writeCSV();
    console.log(`Saved ${leads.length} leads before exit`);
  }
  process.exit(1);
});
