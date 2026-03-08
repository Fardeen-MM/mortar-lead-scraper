#!/usr/bin/env node
/**
 * Scrape AIRC (American International Recruitment Council) certified education agencies.
 *
 * Source: https://airceducation.app.neoncrm.com/membership-directory/5
 * Method: NeonCRM API via Puppeteer (POST /nx/portal/membership-directory/list)
 *         Phase 1 — Bulk fetch all agencies via NeonCRM API (name, city, state, country, contact first name)
 *         Phase 2 — Fetch detail pages for website + certification dates
 *
 * Also scrapes: Institutional Members (directoryId=4), Service Providers (directoryId=6)
 *
 * Expected: ~108 certified agencies, ~268 institutions, ~31 service providers
 *
 * Features:
 *   - NeonCRM API pagination (fetches all in one request with large pageSize)
 *   - Detail page scraping for website and certification dates (Puppeteer)
 *   - Rate limiting between detail fetches (2-3s)
 *   - Test mode (--test): only fetches 5 detail pages
 *   - Skip details mode (--skip-details): only phase 1
 *
 * Usage:
 *   node scripts/scrape-airc.js
 *   node scripts/scrape-airc.js --test
 *   node scripts/scrape-airc.js --skip-details
 *   node scripts/scrape-airc.js --all        (includes institutions + service providers)
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE      = !!args.test;
const SKIP_DETAILS   = !!args['skip-details'];
const SCRAPE_ALL     = !!args.all;
const DELAY_MS       = parseInt(args.delay) || 2500;
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const OUTPUT_FILE    = path.join(OUTPUT_DIR, 'us-education-agents-airc.csv');
const SAVE_EVERY     = 50;

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'certification_start',
  'certification_end', 'member_type',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  apiRows: 0,
  detailsFetched: 0,
  detailErrors: 0,
  websitesFound: 0,
  saved: 0,
  startTime: Date.now(),
};

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

function extractDomain(url) {
  if (!url) return '';
  try {
    let clean = url.trim();
    if (!clean.startsWith('http')) clean = 'https://' + clean;
    const parsed = new URL(clean);
    return parsed.hostname.replace(/^www\./, '');
  } catch (e) {
    return '';
  }
}

function saveCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col] || '')).join(',')
  );
  fs.writeFileSync(OUTPUT_FILE, [header, ...rows].join('\n'), 'utf8');
  stats.saved = leads.length;
  console.log(`  [SAVE] ${leads.length} leads -> ${OUTPUT_FILE}`);
}

// ── Main ────────────────────────────────────────────────────────────────
(async () => {
  console.log('=== AIRC Education Agent Scraper ===');
  console.log(`Test mode: ${TEST_MODE}, Skip details: ${SKIP_DETAILS}, All types: ${SCRAPE_ALL}`);
  console.log(`Delay: ${DELAY_MS}ms\n`);

  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

  // Load the directory page to establish session
  console.log('[1/3] Loading NeonCRM directory page...');
  await page.goto('https://airceducation.app.neoncrm.com/membership-directory/5', {
    waitUntil: 'networkidle2',
    timeout: 30000,
  });
  console.log('  Page loaded. Title:', await page.title());

  // Define directories to scrape
  const directories = [
    { id: '5', name: 'Certified Agency', type: 'Certified Agency' },
  ];
  if (SCRAPE_ALL) {
    directories.push(
      { id: '4', name: 'Institution', type: 'Institution' },
      { id: '6', name: 'Service Provider', type: 'Service Provider' },
    );
  }

  const allLeads = [];

  for (const dir of directories) {
    console.log(`\n[2/3] Fetching ${dir.name} members (directoryId=${dir.id})...`);

    // Fetch all members via API
    const data = await page.evaluate(async (dirId) => {
      const res = await fetch('/nx/portal/membership-directory/list', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          directoryId: dirId,
          pageNo: 1,
          pageSize: 500,
          sortField: 'cuLogin desc, companyName0',
          sortOrder: 'ascending',
          searchCriteria: { directoryId: dirId, viewType: '0' },
        }),
      });
      return await res.json();
    }, dir.id);

    if (!data.success || !data.data?.rows) {
      console.log(`  ERROR: API returned ${JSON.stringify(data).slice(0, 200)}`);
      continue;
    }

    const rows = data.data.rows;
    console.log(`  Found ${rows.length} ${dir.name} members (total: ${data.data.total})`);
    stats.apiRows += rows.length;

    // Transform rows into leads
    for (const row of rows) {
      const lead = {
        first_name: (row.firstName0 || '').trim(),
        last_name: '',
        firm_name: (row.companyName0 || row.memberName || '').trim(),
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: (row.city || '').trim(),
        state: (row.state || '').trim(),
        country: (row.countryId || '').trim(),
        niche: 'Education Agent',
        source: 'airc',
        profile_url: `https://airceducation.app.neoncrm.com/membership-directory/details/5?memberId=${row.accountId}`,
        certification_start: '',
        certification_end: '',
        member_type: dir.type,
        _accountId: row.accountId,
        _directoryId: dir.id,
      };

      // The firstName0 field often has just a first name (contact person)
      // The lastName0 field is usually the company name repeated — not useful
      // Don't set last_name since lastName0 is the company name

      allLeads.push(lead);
    }
  }

  console.log(`\n  Total leads from API: ${allLeads.length}`);

  // Phase 2: Fetch detail pages for website + certification dates
  if (!SKIP_DETAILS) {
    console.log('\n[3/3] Fetching detail pages for websites + certification dates...');
    const maxDetails = TEST_MODE ? 5 : allLeads.length;
    let fetched = 0;

    for (let i = 0; i < Math.min(maxDetails, allLeads.length); i++) {
      const lead = allLeads[i];
      fetched++;

      try {
        const detailUrl = `https://airceducation.app.neoncrm.com/membership-directory/details/${lead._directoryId}?memberId=${lead._accountId}`;
        await page.goto(detailUrl, { waitUntil: 'networkidle2', timeout: 20000 });

        // Extract detail data from page
        const detail = await page.evaluate(() => {
          const result = {};
          const fields = document.querySelectorAll('.mdi-member-field-info-item');
          for (const field of fields) {
            const label = field.querySelector('.mdi-member-name-2');
            const value = field.querySelector('.mid-content-body');
            if (label && value) {
              const key = label.textContent.trim().toLowerCase();
              const val = value.textContent.trim();
              const link = value.querySelector('a');
              if (key.includes('website')) {
                result.website = link ? link.href : val;
              } else if (key.includes('certification start')) {
                result.certStart = val;
              } else if (key.includes('certification end')) {
                result.certEnd = val;
              } else if (key.includes('email')) {
                result.email = val;
              } else if (key.includes('phone')) {
                result.phone = val;
              }
            }
          }
          return result;
        });

        if (detail.website) {
          // Clean the website URL
          let website = detail.website;
          if (website.startsWith('https://www.')) website = website.replace('https://www.', '');
          else if (website.startsWith('http://www.')) website = website.replace('http://www.', '');
          else if (website.startsWith('https://')) website = website.replace('https://', '');
          else if (website.startsWith('http://')) website = website.replace('http://', '');
          // Remove trailing slash
          website = website.replace(/\/$/, '');

          lead.website = website;
          lead.domain = extractDomain(website);
          stats.websitesFound++;
        }
        if (detail.certStart) lead.certification_start = detail.certStart;
        if (detail.certEnd) lead.certification_end = detail.certEnd;
        if (detail.email) lead.email = detail.email;
        if (detail.phone) lead.phone = detail.phone;

        stats.detailsFetched++;
        process.stdout.write(`\r  Detail ${fetched}/${Math.min(maxDetails, allLeads.length)} | ${lead.firm_name.slice(0, 40)} | website: ${lead.website || 'none'}`);

      } catch (e) {
        stats.detailErrors++;
        process.stdout.write(`\r  Detail ${fetched}/${Math.min(maxDetails, allLeads.length)} | ${lead.firm_name.slice(0, 40)} | ERROR: ${e.message.slice(0, 50)}`);
      }

      // Save progress periodically
      if (fetched % SAVE_EVERY === 0) {
        console.log('');
        saveCSV(allLeads);
      }

      // Rate limit
      if (i < Math.min(maxDetails, allLeads.length) - 1) {
        await sleep(DELAY_MS);
      }
    }
    console.log('');
  } else {
    console.log('\n[3/3] Skipping detail pages (--skip-details)');
  }

  // Clean up internal fields
  for (const lead of allLeads) {
    delete lead._accountId;
    delete lead._directoryId;
  }

  // Final save
  saveCSV(allLeads);

  await browser.close();

  // Print stats
  console.log('\n=== AIRC Scrape Complete ===');
  console.log(`  Time: ${elapsed()}`);
  console.log(`  API rows: ${stats.apiRows}`);
  console.log(`  Details fetched: ${stats.detailsFetched}`);
  console.log(`  Detail errors: ${stats.detailErrors}`);
  console.log(`  Websites found: ${stats.websitesFound}`);
  console.log(`  Total leads saved: ${stats.saved}`);
  console.log(`  Output: ${OUTPUT_FILE}`);

  // Print country breakdown
  const countries = {};
  for (const lead of allLeads) {
    const c = lead.country || 'Unknown';
    countries[c] = (countries[c] || 0) + 1;
  }
  console.log('\n  Country breakdown:');
  for (const [country, count] of Object.entries(countries).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${country}: ${count}`);
  }
})();
