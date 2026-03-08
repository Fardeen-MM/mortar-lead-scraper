#!/usr/bin/env node
/**
 * UK Immigration Advice Authority (IAA, formerly OISC) Adviser Register Scraper
 *
 * Scrapes the IAA adviser register via the Salesforce Aura API.
 * Two endpoints give us everything:
 *   1. tempLocationController.getAllAdvisers  — 4000+ individual advisers
 *   2. tempLocationController.getFilteredLocations — 2000+ office locations with org details
 *
 * We merge advisers with their organisation's location data (address, website, OISC ref)
 * to produce a comprehensive CSV of immigration advisers in the UK.
 *
 * Usage:
 *   node scripts/scrape-uk-oisc.js                    # Full scrape
 *   node scripts/scrape-uk-oisc.js --test              # Quick test (first 20 results)
 *   node scripts/scrape-uk-oisc.js --orgs-only         # Only scrape organisations (locations)
 *   node scripts/scrape-uk-oisc.js --advisers-only     # Only scrape individual advisers
 */

const https = require('https');
const tls = require('tls');
const fs = require('fs');
const path = require('path');

// === Config ===
const BASE_URL = 'https://portal.immigrationadviceauthority.gov.uk';
const AURA_ENDPOINT = '/s/sfsites/aura';
const FWUID = 'YkVKdlZEd2t6eFplVFJNMGN2eVd5UTJEa1N5enhOU3R5QWl2VzNveFZTbGcxMy4tMjE0NzQ4MzY0OC45OTYxNDcy';
const APP_HASH = '1529_lI95rFcxq-le9BLgryC1ew';

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-advisers-oisc.csv');

// === CLI Args ===
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const orgsOnly = args.includes('--orgs-only');
const advisersOnly = args.includes('--advisers-only');

// === Helpers ===

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

/**
 * Make an Aura API call to the IAA Salesforce portal.
 */
function auraCall(classname, method, params = {}) {
  return new Promise((resolve, reject) => {
    const action = {
      id: '1;a',
      descriptor: 'aura://ApexActionController/ACTION$execute',
      callingDescriptor: 'UNKNOWN',
      params: {
        namespace: '',
        classname,
        method,
        ...(Object.keys(params).length > 0 ? { params } : {}),
        cacheable: true,
        isContinuation: false,
      },
    };

    const message = JSON.stringify({ actions: [action] });
    const context = JSON.stringify({
      mode: 'PROD',
      fwuid: FWUID,
      app: 'siteforce:communityApp',
      loaded: { 'APPLICATION@markup://siteforce:communityApp': APP_HASH },
      dn: [],
      globals: {},
      uad: true,
    });

    const body = new URLSearchParams({
      message,
      'aura.context': context,
      'aura.pageURI': '/s/adviser-register',
      'aura.token': 'null',
    }).toString();

    const url = new URL(`${BASE_URL}${AURA_ENDPOINT}?r=1&aura.ApexAction.execute=1`);

    const options = {
      hostname: url.hostname,
      port: 443,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        Accept: '*/*',
        Origin: BASE_URL,
        Referer: `${BASE_URL}/s/adviser-register`,
      },
      // Use system CA certs + skip verification for gov.uk intermediate certs
      secureContext: tls.createSecureContext(),
      rejectUnauthorized: false,
    };

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf-8');
        try {
          const data = JSON.parse(raw);
          const action = data.actions && data.actions[0];
          if (!action || action.state !== 'SUCCESS') {
            reject(new Error(`Aura call failed: ${action ? action.state : 'no action'} - ${JSON.stringify(action?.error || '')}`));
            return;
          }
          resolve(action.returnValue.returnValue);
        } catch (e) {
          reject(new Error(`Failed to parse Aura response: ${e.message}\nRaw: ${raw.substring(0, 500)}`));
        }
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

/**
 * Split a full name into first and last name.
 */
function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

/**
 * Format a UK phone number.
 */
function formatPhone(phone) {
  if (!phone) return '';
  let p = phone.toString().replace(/[^0-9+]/g, '');
  // Remove UK country code prefix
  if (p.startsWith('+44')) p = '0' + p.slice(3);
  if (p.startsWith('44') && p.length > 10) p = '0' + p.slice(2);
  return p;
}

/**
 * Extract domain from a URL.
 */
function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'http://' + u;
    return new URL(u).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

/**
 * Parse level number from level string like "Level 3".
 */
function parseLevel(levelStr) {
  if (!levelStr) return '';
  const m = levelStr.match(/Level\s*(\d)/i);
  return m ? m[1] : levelStr;
}

/**
 * Parse categories into a readable list.
 */
function parseCategories(cats) {
  if (!cats) return '';
  // Categories like "Level 3 Immigration;Level 3 Asylum and Protection"
  // Strip the "Level N " prefix from each and deduplicate
  const parts = cats.split(';').map((c) => c.trim());
  const cleaned = parts.map((c) => c.replace(/^Level\s*\d+\s*/i, '').trim()).filter(Boolean);
  return [...new Set(cleaned)].join('; ');
}

/**
 * Escape a CSV field.
 */
function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/**
 * Write rows to CSV.
 */
function writeCsv(filePath, headers, rows) {
  const headerLine = headers.map(csvEscape).join(',');
  const dataLines = rows.map((row) => headers.map((h) => csvEscape(row[h] || '')).join(','));
  fs.writeFileSync(filePath, [headerLine, ...dataLines].join('\n'), 'utf-8');
}

// === Main ===

async function main() {
  log('=== UK IAA (OISC) Adviser Register Scraper ===');
  log(`Mode: ${testMode ? 'TEST' : 'FULL'}${orgsOnly ? ' (orgs only)' : ''}${advisersOnly ? ' (advisers only)' : ''}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const results = [];
  const seen = new Set(); // Dedup key: adviser SF ID or org ref

  // --- Step 1: Fetch all advisers ---
  let advisers = [];
  if (!orgsOnly) {
    log('Fetching all advisers via Aura API...');
    try {
      advisers = await auraCall('tempLocationController', 'getAllAdvisers');
      log(`Got ${advisers.length} advisers`);
    } catch (e) {
      log(`ERROR fetching advisers: ${e.message}`);
      if (advisersOnly) {
        process.exit(1);
      }
    }
  }

  // --- Step 2: Fetch all locations (orgs with addresses) ---
  let locations = [];
  log('Fetching all locations via Aura API...');
  try {
    locations = await auraCall('tempLocationController', 'getFilteredLocations');
    log(`Got ${locations.length} locations`);
  } catch (e) {
    log(`ERROR fetching locations: ${e.message}`);
    if (orgsOnly) {
      process.exit(1);
    }
  }

  // Build a lookup: AccountId -> location data (use first/primary location per org)
  const orgLocationMap = new Map(); // AccountId -> location record
  const orgRefMap = new Map(); // AccountId -> org reference number
  for (const loc of locations) {
    const acctId = loc.Related_Account__c;
    const org = loc.Related_Account__r || {};

    // Store if we don't have this org yet, or if this location has more data
    if (!orgLocationMap.has(acctId)) {
      orgLocationMap.set(acctId, loc);
    }
    if (org.Organisation_Reference_Number__c) {
      orgRefMap.set(acctId, org.Organisation_Reference_Number__c);
    }
  }

  // --- Step 3: Process individual advisers ---
  if (!orgsOnly) {
    log('Processing advisers...');
    let processed = 0;
    for (const adv of advisers) {
      if (testMode && processed >= 20) break;

      const dedupKey = adv.Id;
      if (seen.has(dedupKey)) continue;
      seen.add(dedupKey);

      const { first_name, last_name } = splitName(adv.Name);
      const orgName = adv.Account?.BusinessName__c || '';
      const acctId = adv.AccountId;

      // Get location data from the org
      const loc = orgLocationMap.get(acctId);
      const org = loc?.Related_Account__r || {};
      const orgRef = orgRefMap.get(acctId) || '';

      const level = parseLevel(adv.Level__c);
      const categories = parseCategories(adv.Categories__c);

      // Prefer adviser-level contact info, fall back to org
      const phone = formatPhone(adv.Phone) || formatPhone(loc?.Phone_Number__c) || formatPhone(org.Phone);
      const email = adv.Email || loc?.Primary_Email__c || org.Primary_Email__c || '';
      const website = org.Website || '';
      const domain = extractDomain(website);

      const city = loc?.City__c || '';
      const county = loc?.County__c || '';
      const postcode = loc?.Postcode__c || '';
      const street = loc?.Street__c || '';
      const feeType = org.Fee_Paying_Type__c || '';

      const profileUrl = `${BASE_URL}/s/adviser-register`;

      results.push({
        first_name,
        last_name,
        firm_name: orgName,
        title: level ? `Level ${level} Immigration Adviser` : 'Immigration Adviser',
        email,
        phone,
        website,
        domain,
        city,
        state: county,
        postcode,
        street,
        country: 'UK',
        niche: 'Immigration Advisory',
        source: 'uk-iaa',
        profile_url: profileUrl,
        oisc_ref: orgRef,
        level,
        categories,
        fee_type: feeType,
        registration_status: org.Registration_Status__c || '',
        record_type: 'adviser',
      });

      processed++;
    }
    log(`Processed ${processed} advisers`);
  }

  // --- Step 4: Process organisations (from locations) ---
  if (!advisersOnly) {
    log('Processing organisations from locations...');
    const orgsSeen = new Set();
    let orgCount = 0;

    for (const loc of locations) {
      if (testMode && orgCount >= 20) break;

      const org = loc.Related_Account__r || {};
      const acctId = loc.Related_Account__c;
      const orgRef = org.Organisation_Reference_Number__c || '';

      // Dedup by account ID (multiple locations per org)
      if (orgsSeen.has(acctId)) continue;
      orgsSeen.add(acctId);

      // Skip if not registered
      if (org.Registration_Status__c && org.Registration_Status__c !== 'Registered') continue;

      const orgName = org.BusinessName__c || '';
      const level = parseLevel(org.Level__c);
      const categories = parseCategories(org.Categories__c);

      const phone = formatPhone(loc.Phone_Number__c) || formatPhone(org.Phone);
      const email = org.Primary_Email__c || loc.Primary_Email__c || '';
      const website = org.Website || '';
      const domain = extractDomain(website);
      const feeType = org.Fee_Paying_Type__c || '';

      const profileUrl = `${BASE_URL}/s/adviser-register`;

      // Don't add if we already have this org from adviser records
      const dedupKey = `org_${acctId}`;
      if (seen.has(dedupKey)) continue;
      seen.add(dedupKey);

      results.push({
        first_name: '',
        last_name: '',
        firm_name: orgName,
        title: level ? `Level ${level} Immigration Advisory Organisation` : 'Immigration Advisory Organisation',
        email,
        phone,
        website,
        domain,
        city: loc.City__c || '',
        state: loc.County__c || '',
        postcode: loc.Postcode__c || '',
        street: loc.Street__c || '',
        country: 'UK',
        niche: 'Immigration Advisory',
        source: 'uk-iaa',
        profile_url: profileUrl,
        oisc_ref: orgRef,
        level,
        categories,
        fee_type: feeType,
        registration_status: org.Registration_Status__c || '',
        record_type: 'organisation',
      });

      orgCount++;
    }
    log(`Processed ${orgCount} organisations`);
  }

  // --- Step 5: Write CSV ---
  const headers = [
    'first_name',
    'last_name',
    'firm_name',
    'title',
    'email',
    'phone',
    'website',
    'domain',
    'street',
    'city',
    'state',
    'postcode',
    'country',
    'niche',
    'source',
    'profile_url',
    'oisc_ref',
    'level',
    'categories',
    'fee_type',
    'registration_status',
    'record_type',
  ];

  writeCsv(OUTPUT_FILE, headers, results);
  log(`Wrote ${results.length} records to ${OUTPUT_FILE}`);

  // --- Stats ---
  const adviserCount = results.filter((r) => r.record_type === 'adviser').length;
  const orgCount = results.filter((r) => r.record_type === 'organisation').length;
  const withEmail = results.filter((r) => r.email).length;
  const withPhone = results.filter((r) => r.phone).length;
  const withWebsite = results.filter((r) => r.website).length;
  const withBoth = results.filter((r) => r.email && r.phone).length;

  log('');
  log('=== Summary ===');
  log(`Total records:    ${results.length}`);
  log(`  Advisers:       ${adviserCount}`);
  log(`  Organisations:  ${orgCount}`);
  const pct = (n) => results.length ? Math.round((100 * n) / results.length) : 0;
  log(`  With email:     ${withEmail} (${pct(withEmail)}%)`);
  log(`  With phone:     ${withPhone} (${pct(withPhone)}%)`);
  log(`  With website:   ${withWebsite} (${pct(withWebsite)}%)`);
  log(`  Gold (e+p):     ${withBoth} (${pct(withBoth)}%)`);

  // Level breakdown
  const levelCounts = {};
  for (const r of results) {
    const lv = r.level || 'unknown';
    levelCounts[lv] = (levelCounts[lv] || 0) + 1;
  }
  log('  By level:');
  for (const [lv, count] of Object.entries(levelCounts).sort()) {
    log(`    Level ${lv}: ${count}`);
  }

  // Fee type breakdown
  const feeCounts = {};
  for (const r of results) {
    const ft = r.fee_type || 'unknown';
    feeCounts[ft] = (feeCounts[ft] || 0) + 1;
  }
  log('  By fee type:');
  for (const [ft, count] of Object.entries(feeCounts).sort()) {
    log(`    ${ft}: ${count}`);
  }

  log('');
  log('Done!');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
