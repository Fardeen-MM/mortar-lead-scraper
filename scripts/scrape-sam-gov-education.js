#!/usr/bin/env node
/**
 * SAM.gov Education Entity Scraper
 *
 * Downloads education-related businesses from USASpending.gov (which sources from SAM.gov).
 * SAM.gov's Entity Management API requires a registered API key and POC emails/phones are
 * FOUO (For Official Use Only) restricted to federal users. USASpending.gov provides a free,
 * unauthenticated API with entity names, UEIs, addresses, and business types.
 *
 * NAICS 611* = Educational Services:
 *   611110 Elementary & Secondary Schools
 *   611210 Junior Colleges
 *   611310 Colleges, Universities & Professional Schools
 *   611410 Business & Secretarial Schools
 *   611420 Computer Training
 *   611430 Professional & Management Development Training
 *   611511 Cosmetology & Barber Schools
 *   611512 Flight Training
 *   611513 Apprenticeship Training
 *   611519 Other Technical & Trade Schools
 *   611610 Fine Arts Schools
 *   611620 Sports & Recreation Instruction
 *   611630 Language Schools
 *   611691 Exam Preparation & Tutoring
 *   611692 Automobile Driving Schools
 *   611699 All Other Misc Schools
 *   611710 Educational Support Services
 *
 * Output: output/sam-gov-education.csv
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// --- Config ---
const BASE_URL = 'https://api.usaspending.gov';
const NAICS_CODES = [
  '611110', '611210', '611310', '611410', '611420', '611430',
  '611511', '611512', '611513', '611519',
  '611610', '611620', '611630',
  '611691', '611692', '611699', '611710'
];
const AWARD_TYPE_CODES = ['A', 'B', 'C', 'D', '02', '03', '04', '05'];
const TIME_PERIOD = [{ start_date: '2020-01-01', end_date: '2026-12-31' }];
const PAGE_SIZE = 100;
const DETAIL_CONCURRENCY = 10; // parallel detail requests
const DETAIL_DELAY_MS = 100;  // polite delay between batches
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'sam-gov-education.csv');

// --- HTTP helpers ---
function httpPost(urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const url = new URL(urlPath, BASE_URL);
    const options = {
      hostname: url.hostname,
      port: 443,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'User-Agent': 'MortarLeadScraper/1.0',
        'Accept': 'application/json'
      }
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message} — body: ${body.slice(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.write(data);
    req.end();
  });
}

function httpGet(urlPath) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlPath, BASE_URL);
    const options = {
      hostname: url.hostname,
      port: 443,
      path: url.pathname + url.search,
      method: 'GET',
      headers: {
        'User-Agent': 'MortarLeadScraper/1.0',
        'Accept': 'application/json'
      }
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message} — body: ${body.slice(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// --- Step 1: Get all education recipients by NAICS ---
async function fetchAllRecipients() {
  const recipients = new Map(); // keyed by recipient_id to dedup
  let page = 1;
  let hasNext = true;

  console.log(`\n[Step 1] Fetching education recipients (NAICS 611*)...`);
  console.log(`  NAICS codes: ${NAICS_CODES.join(', ')}`);

  while (hasNext) {
    const body = {
      filters: {
        naics_codes: { require: NAICS_CODES },
        award_type_codes: AWARD_TYPE_CODES,
        time_period: TIME_PERIOD
      },
      category: 'recipient',
      page,
      limit: PAGE_SIZE
    };

    try {
      const data = await httpPost('/api/v2/search/spending_by_category/recipient/', body);
      const results = data.results || [];
      const meta = data.page_metadata || {};

      for (const r of results) {
        const rid = r.recipient_id;
        if (rid && !recipients.has(rid)) {
          recipients.set(rid, {
            recipient_id: rid,
            name: (r.name || '').trim(),
            amount: r.amount || 0,
            uei: r.uei || null
          });
        }
      }

      hasNext = meta.hasNext === true;
      const total = recipients.size;
      process.stdout.write(`\r  Page ${page}: ${results.length} results, ${total} unique recipients so far...`);
      page++;

      // Polite delay
      await sleep(200);
    } catch (err) {
      console.error(`\n  Error on page ${page}: ${err.message}`);
      if (page > 3) {
        // Retry once
        await sleep(2000);
        continue;
      }
      break;
    }
  }

  console.log(`\n  Total unique recipients: ${recipients.size}`);
  return Array.from(recipients.values());
}

// --- Step 2: Get detailed profiles (address, business types) ---
async function fetchRecipientDetails(recipients) {
  console.log(`\n[Step 2] Fetching detailed profiles for ${recipients.length} recipients...`);

  const detailed = [];
  let completed = 0;
  let errors = 0;

  // Process in batches for concurrency control
  for (let i = 0; i < recipients.length; i += DETAIL_CONCURRENCY) {
    const batch = recipients.slice(i, i + DETAIL_CONCURRENCY);

    const promises = batch.map(async (r) => {
      try {
        const data = await httpGet(`/api/v2/recipient/${r.recipient_id}/`);

        const loc = data.location || {};
        const businessTypes = data.business_types || [];

        return {
          name: data.name || r.name,
          uei: data.uei || r.uei || '',
          duns: data.duns || '',
          address: loc.address_line1 || '',
          city: loc.city_name || '',
          state: loc.state_code || '',
          zip: loc.zip || '',
          country: loc.country_code || loc.country_name || 'USA',
          business_types: businessTypes.join('; '),
          parent_name: data.parent_name || '',
          total_awards: data.total_transaction_amount || 0,
          award_count: data.total_transactions || 0,
          recipient_id: r.recipient_id
        };
      } catch (err) {
        errors++;
        return {
          name: r.name,
          uei: r.uei || '',
          duns: '',
          address: '',
          city: '',
          state: '',
          zip: '',
          country: 'USA',
          business_types: '',
          parent_name: '',
          total_awards: r.amount || 0,
          award_count: 0,
          recipient_id: r.recipient_id
        };
      }
    });

    const results = await Promise.all(promises);
    detailed.push(...results);
    completed += batch.length;

    process.stdout.write(`\r  ${completed}/${recipients.length} profiles fetched (${errors} errors)...`);

    if (i + DETAIL_CONCURRENCY < recipients.length) {
      await sleep(DETAIL_DELAY_MS);
    }
  }

  console.log(`\n  Fetched ${detailed.length} profiles, ${errors} errors`);
  return detailed;
}

// --- Step 3: Try to find POC info from SAM.gov entity pages ---
// SAM.gov entity pages at sam.gov/entity/{UEI}/coreData are SPAs,
// but we can try the internal API endpoint
async function trySamGovEntityLookup(uei) {
  if (!uei) return null;
  try {
    // This endpoint returns 500 without proper session but sometimes works
    const url = `https://sam.gov/api/prod/entity-registration/v1/api/entityDetails?ueiSAM=${uei}`;
    return new Promise((resolve) => {
      const options = {
        hostname: 'sam.gov',
        path: `/api/prod/entity-registration/v1/api/entityDetails?ueiSAM=${uei}`,
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
      };
      const req = https.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const data = JSON.parse(body);
            if (data.error || data.status >= 400) return resolve(null);
            resolve(data);
          } catch { resolve(null); }
        });
      });
      req.on('error', () => resolve(null));
      req.setTimeout(5000, () => { req.destroy(); resolve(null); });
      req.end();
    });
  } catch {
    return null;
  }
}

// --- Step 4: Write CSV ---
function writeCSV(records) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const header = 'email,first_name,last_name,company,phone,address,city,state,zip,country,uei,duns,business_types,parent_company,total_federal_awards,award_count,source';

  const lines = [header];
  let withEmail = 0;
  let withAddress = 0;

  for (const r of records) {
    const email = r.email || '';
    const phone = r.phone || '';
    const firstName = r.first_name || '';
    const lastName = r.last_name || '';

    if (email) withEmail++;
    if (r.city && r.state) withAddress++;

    const row = [
      csvEscape(email),
      csvEscape(firstName),
      csvEscape(lastName),
      csvEscape(r.name),
      csvEscape(phone),
      csvEscape(r.address),
      csvEscape(r.city),
      csvEscape(r.state),
      csvEscape(r.zip),
      csvEscape(r.country),
      csvEscape(r.uei),
      csvEscape(r.duns),
      csvEscape(r.business_types),
      csvEscape(r.parent_name),
      r.total_awards ? `${Math.round(r.total_awards)}` : '',
      r.award_count || '',
      'SAM.gov/USASpending'
    ].join(',');

    lines.push(row);
  }

  fs.writeFileSync(OUTPUT_FILE, lines.join('\n'), 'utf8');

  console.log(`\n[Step 4] CSV written to ${OUTPUT_FILE}`);
  console.log(`  Total records: ${records.length}`);
  console.log(`  With address: ${withAddress}`);
  console.log(`  With email: ${withEmail}`);
  console.log(`  File size: ${(fs.statSync(OUTPUT_FILE).size / 1024).toFixed(1)} KB`);
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// --- Step 5: Also try SAM.gov search API for exclusion data (public, no key needed) ---
async function fetchSamExclusionData() {
  console.log(`\n[Bonus] Checking SAM.gov exclusion search for education entities...`);

  // The SAM.gov internal search API at /api/prod/sgs/v1/search/ returns exclusion data
  // without requiring an API key. While this is exclusion data (banned entities),
  // it still provides addresses for verification/cross-reference.
  // We skip this since it's not what we want for lead gen.
  console.log('  Skipped — exclusion data not useful for lead generation.');
}

// --- Main ---
async function main() {
  const startTime = Date.now();

  console.log('='.repeat(60));
  console.log('  SAM.gov Education Entity Scraper');
  console.log('  Source: USASpending.gov (free public API, no key required)');
  console.log('='.repeat(60));
  console.log(`\nNAICS 611* Educational Services`);
  console.log(`Note: SAM.gov Entity Management API requires a registered API key.`);
  console.log(`      POC emails/phones are FOUO (restricted to federal users).`);
  console.log(`      Using USASpending.gov for entity data (names, addresses, UEIs).`);
  console.log(`      Emails can be enriched later via waterfall/website crawling.\n`);

  // Step 1: Get recipients
  const recipients = await fetchAllRecipients();
  if (recipients.length === 0) {
    console.error('No recipients found. Exiting.');
    process.exit(1);
  }

  // Step 2: Get detailed profiles
  const detailed = await fetchRecipientDetails(recipients);

  // Step 3: Try SAM.gov entity lookup for a sample (just to test)
  console.log(`\n[Step 3] Testing SAM.gov entity detail lookup...`);
  const sampleUei = detailed.find(d => d.uei)?.uei;
  if (sampleUei) {
    const samData = await trySamGovEntityLookup(sampleUei);
    if (samData && !samData.error) {
      console.log(`  SAM.gov entity detail API returned data for UEI ${sampleUei}`);
      console.log(`  Keys: ${Object.keys(samData).join(', ')}`);
    } else {
      console.log(`  SAM.gov entity detail API requires authentication (expected).`);
      console.log(`  POC emails/phones are restricted to federal system accounts.`);
    }
  }

  // Step 4: Write CSV
  writeCSV(detailed);

  // Stats
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const states = {};
  const types = {};
  for (const r of detailed) {
    if (r.state) states[r.state] = (states[r.state] || 0) + 1;
    for (const t of (r.business_types || '').split('; ')) {
      if (t) types[t] = (types[t] || 0) + 1;
    }
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log('  Summary');
  console.log('='.repeat(60));
  console.log(`  Total entities: ${detailed.length}`);
  console.log(`  With address: ${detailed.filter(d => d.city && d.state).length}`);
  console.log(`  With UEI: ${detailed.filter(d => d.uei).length}`);
  console.log(`  Unique states: ${Object.keys(states).length}`);
  console.log(`  Time: ${elapsed}s`);

  // Top states
  const topStates = Object.entries(states)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  console.log(`\n  Top states:`);
  for (const [st, count] of topStates) {
    console.log(`    ${st}: ${count}`);
  }

  // Top business types
  const topTypes = Object.entries(types)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  if (topTypes.length) {
    console.log(`\n  Top business types:`);
    for (const [t, count] of topTypes) {
      console.log(`    ${t}: ${count}`);
    }
  }

  console.log(`\n  Output: ${OUTPUT_FILE}`);
  console.log(`  Next step: Enrich with emails via waterfall (website crawl, SMTP patterns)`);
  console.log('');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
