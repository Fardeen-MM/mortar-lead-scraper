#!/usr/bin/env node
/**
 * NZ IAA (Immigration Advisers Authority) Public Register Scraper
 *
 * Scrapes all current licensed immigration advisers from the MBIE Registers portal.
 * Uses Puppeteer because the site is a Catalyst/Vue.js SPA that requires JS rendering.
 *
 * The search results contain: name, licence number, status, last issued date, firm, address.
 * The site caps paginated results at 1500 (75 pages x 20), so this script also runs
 * supplementary alphabetical searches to capture the remaining advisers.
 *
 * Usage: node scripts/scrape-nz-immigration.js
 * Output: output/nz-immigration-advisers.csv
 *
 * ~1,630 current advisers expected as of March 2026.
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// ─── Config ────────────────────────────────────────────────────────────
const BASE_URL = 'https://app.mbieregisters.govt.nz/iaa/ui/start/searchForAnOccupationalRegistration';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'nz-immigration-advisers.csv');
const DELAY_MS = 2000;   // 2s between page navigations
const WAIT_MS = 5000;    // 5s wait for search results to load

// Country name → ISO code mapping
const COUNTRY_MAP = {
  'new zealand': 'NZ', 'australia': 'AU', 'india': 'IN', 'indonesia': 'ID',
  'china': 'CN', 'united kingdom': 'UK', 'england': 'UK', 'scotland': 'UK',
  'wales': 'UK', 'united states': 'US', 'usa': 'US', 'philippines': 'PH',
  'fiji': 'FJ', 'south africa': 'ZA', 'japan': 'JP', 'korea': 'KR',
  'south korea': 'KR', 'thailand': 'TH', 'malaysia': 'MY', 'singapore': 'SG',
  'vietnam': 'VN', 'pakistan': 'PK', 'nepal': 'NP', 'sri lanka': 'LK',
  'bangladesh': 'BD', 'iran': 'IR', 'samoa': 'WS', 'tonga': 'TO',
  'hong kong': 'HK', 'taiwan': 'TW', 'myanmar': 'MM', 'cambodia': 'KH',
  'brazil': 'BR', 'canada': 'CA', 'germany': 'DE', 'france': 'FR',
  'italy': 'IT', 'spain': 'ES', 'ireland': 'IE', 'poland': 'PL',
  'russia': 'RU', 'turkey': 'TR', 'egypt': 'EG', 'belize': 'BZ',
  'israel': 'IL', 'united arab emirates': 'AE', 'uae': 'AE',
  'saudi arabia': 'SA', 'qatar': 'QA', 'bahrain': 'BH', 'kuwait': 'KW',
  'oman': 'OM', 'jordan': 'JO', 'lebanon': 'LB',
  // Chinese provinces (address parsing sometimes captures these as "country")
  'beijing': 'CN', 'liaoning': 'CN', 'jiangsu': 'CN', 'guangdong': 'CN',
  'shanghai': 'CN', 'shandong': 'CN', 'fujian': 'CN', 'zhejiang': 'CN',
  'hebei': 'CN', 'sichuan': 'CN', 'hubei': 'CN', 'hunan': 'CN',
  // Thai/PH cities sometimes end up as country
  'bangkok': 'TH', 'patumthani': 'TH', 'cebu': 'PH', 'manila': 'PH',
  // NZ regions (sometimes address parsing captures these)
  'auckland': 'NZ', 'wellington': 'NZ', 'canterbury': 'NZ',
  'christchurch': 'NZ', 'otago': 'NZ', 'waikato': 'NZ',
  'bay of plenty': 'NZ', 'northland': 'NZ', 'taranaki': 'NZ',
  'hawke\'s bay': 'NZ', 'southland': 'NZ', 'nelson': 'NZ',
  'dubai': 'AE', 'abu dhabi': 'AE',
};

// ─── Helpers ───────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function escapeCSV(val) {
  if (!val) return '';
  val = String(val).trim();
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function normalizeCountry(raw) {
  if (!raw) return 'NZ';
  const lower = raw.toLowerCase().trim();
  return COUNTRY_MAP[lower] || raw;
}

/**
 * Parse a single adviser result block into a lead object.
 * Block format (from innerText):
 *   Line 1: "Full Name (Preferred Name) (LICENCE_NUMBER)"
 *   Line 2: "Current | Last issued on DD Month YYYY"
 *   Line 3: "Firm Name" (or "N/A")
 *   Line 4: "Street, Suburb, City, Region, Postcode, Country"
 */
function parseAdviserBlock(block) {
  const lines = block.split('\n').map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) return null;

  const result = {
    first_name: '', last_name: '', firm_name: '', title: '', email: '', phone: '',
    website: '', domain: '', city: '', state: '', country: 'NZ',
    niche: 'Immigration Advisory', source: 'nz-iaa', profile_url: '',
    licence_number: '', licence_type: ''
  };

  // Line 1: Name and licence number
  const licenceMatch = lines[0].match(/\((\d{9})\)/);
  if (!licenceMatch) return null;
  result.licence_number = licenceMatch[1];

  let nameStr = lines[0].replace(/\s*\(\d{9}\)\s*/, '').trim();
  // Remove alias/preferred name in parentheses
  const aliasMatch = nameStr.match(/^(.+?)\s*\(([^)]+)\)\s*$/);
  if (aliasMatch) nameStr = aliasMatch[1].trim();

  const nameParts = nameStr.split(/\s+/);
  result.first_name = nameParts[0] || '';
  result.last_name = nameParts.slice(1).join(' ') || '';

  // Line 3: Firm name
  if (lines.length >= 3) {
    const firm = lines[2].trim();
    result.firm_name = firm === 'N/A' ? '' : firm;
  }

  // Line 4+: Address
  if (lines.length >= 4) {
    const addressStr = lines.slice(3).join(', ');
    parseAddress(result, addressStr);
  }

  result.profile_url = `https://app.mbieregisters.govt.nz/iaa/ext/adviser/view/${result.licence_number}`;
  return result;
}

function parseAddress(result, address) {
  const parts = address.split(',').map(p => p.trim());
  if (parts.length < 2) return;

  // Last part is country
  const countryPart = parts[parts.length - 1];
  result.country = normalizeCountry(countryPart);

  // Extract city and region: ..., City, Region, Postcode, Country
  if (parts.length >= 4) {
    const postcode = parts[parts.length - 2];
    if (postcode && postcode.match(/^\d{3,6}$/)) {
      if (parts.length >= 5) {
        result.state = parts[parts.length - 3];
        result.city = parts[parts.length - 4];
      }
    } else {
      result.state = parts[parts.length - 2];
      if (parts.length >= 4) result.city = parts[parts.length - 3];
    }
  }
}

// Country names pattern for block termination
const COUNTRY_PATTERN = /(New Zealand|Australia|India|Indonesia|China|United Kingdom|Philippines|Fiji|South Africa|Japan|Korea|Thailand|Malaysia|Singapore|Vietnam|Pakistan|Nepal|Sri Lanka|Bangladesh|Iran|Samoa|Tonga|Myanmar|Cambodia|Hong Kong|Taiwan|Brazil|Canada|United States|United Arab Emirates|UAE|Saudi Arabia|Qatar|Egypt|Turkey|Germany|France|Italy|Spain|Netherlands|Sweden|Norway|Denmark|Finland|Switzerland|Ireland|Scotland|Wales|England|Russia|Ukraine|Poland|Czech Republic|Hungary|Romania|Bulgaria|Greece|Portugal|Belgium|Austria|Croatia|Serbia|Israel|Belize|Beijing|Liaoning|Jiangsu|Guangdong|Shanghai|Shandong|Fujian|Zhejiang|Bangkok|Patumthani|Cebu)$/i;

/**
 * Extract adviser result blocks from page text.
 * Each block starts with a name+licence pattern and ends at the next such pattern
 * or when a country name is matched at end of line.
 */
function extractResultBlocks(pageText) {
  const blocks = [];
  const lines = pageText.split('\n');
  let currentBlock = null;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (trimmed.match(/\(\d{9}\)\s*$/)) {
      if (currentBlock && currentBlock.length >= 2) {
        blocks.push(currentBlock.join('\n'));
      }
      currentBlock = [trimmed];
    } else if (currentBlock) {
      currentBlock.push(trimmed);
      if (COUNTRY_PATTERN.test(trimmed)) {
        blocks.push(currentBlock.join('\n'));
        currentBlock = null;
      }
    }
  }
  if (currentBlock && currentBlock.length >= 2) {
    blocks.push(currentBlock.join('\n'));
  }
  return blocks;
}

/**
 * Set up the search page: navigate, set status filter to Current.
 */
async function setupSearchPage(page) {
  await page.goto(BASE_URL, { waitUntil: 'networkidle0', timeout: 30000 });

  // Set status dropdown to "registered" (Current)
  await page.evaluate(() => {
    const selects = document.querySelectorAll('select');
    for (const sel of selects) {
      if ([...sel.options].some(o => o.value === 'registered')) {
        sel.value = 'registered';
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        break;
      }
    }
  });
  await sleep(2000);

  // Check "Show total number of results"
  await page.evaluate(() => {
    const cb = document.querySelector('input[type="checkbox"]');
    if (cb && !cb.checked) cb.click();
  });
  await sleep(1000);
}

/**
 * Execute a search with optional query text and scrape all result pages.
 */
async function executeSearch(page, query, seenLicences) {
  // Set search query if provided
  if (query) {
    await page.evaluate((q) => {
      const input = document.querySelector('input[aria-label="Name or number"]') ||
                    document.querySelector('input[placeholder="Name or number"]');
      if (input) {
        input.value = q;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, query);
    await sleep(1500);
  }

  // Click Search
  await page.evaluate(() => {
    const links = document.querySelectorAll('a.cat-btn');
    for (const a of links) {
      if (a.textContent.trim() === 'Search') { a.click(); break; }
    }
  });
  await sleep(query ? 8000 : WAIT_MS + 5000);

  // Get page count
  const pagesText = await page.evaluate(() => {
    const el = document.querySelector('.cat-pagination-pages');
    return el ? el.innerText.trim() : '';
  });
  const pagesMatch = pagesText.match(/of\s+(\d+)/i);
  const totalPages = pagesMatch ? parseInt(pagesMatch[1]) : 1;

  const newLeads = [];

  for (let p = 1; p <= totalPages; p++) {
    try {
      const pageText = await page.evaluate(() => {
        const s = document.querySelector('.cat-search') || document.body;
        return s.innerText;
      });

      const blocks = extractResultBlocks(pageText);
      for (const block of blocks) {
        const lead = parseAdviserBlock(block);
        if (lead && lead.licence_number && !seenLicences.has(lead.licence_number)) {
          seenLicences.add(lead.licence_number);
          newLeads.push(lead);
        }
      }

      if (p < totalPages) {
        await page.evaluate(() => {
          const btn = document.querySelector('a.pagination-pages-next');
          if (btn) btn.click();
        });
        await sleep(DELAY_MS);

        let attempts = 0;
        while (attempts < 15) {
          const curPage = await page.evaluate(() => {
            const inp = document.querySelector('input[aria-label="Page"]');
            return inp ? parseInt(inp.value) : -1;
          });
          if (curPage === p + 1) break;
          await sleep(1000);
          attempts++;
        }
        await sleep(DELAY_MS);
      }
    } catch (err) {
      console.log(`    Error on page ${p}: ${err.message}`);
      break;
    }
  }

  return { leads: newLeads, pages: totalPages };
}

// ─── Main ──────────────────────────────────────────────────────────────
async function scrape() {
  console.log('=== NZ IAA Immigration Advisers Scraper ===');
  console.log(`Started: ${new Date().toISOString()}\n`);

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    const allLeads = [];
    const seenLicences = new Set();

    // ─── Phase 1: Main search (empty query = all results) ─────────────
    console.log('[Phase 1] Main search (all current advisers)...');
    await setupSearchPage(page);
    const mainResult = await executeSearch(page, '', seenLicences);
    allLeads.push(...mainResult.leads);
    console.log(`  Scraped ${mainResult.leads.length} leads from ${mainResult.pages} pages\n`);

    // ─── Phase 2: Supplementary searches for truncated results ────────
    // The site caps paginated results at ~1500. Use alphabetical queries
    // to pick up remaining advisers (typically names starting X-Z).
    if (mainResult.pages >= 70) {
      console.log('[Phase 2] Supplementary searches (picking up truncated results)...');
      const supplementQueries = [
        'Xu', 'Xia', 'Xin', 'Xiu', 'Xue', 'Xiang', 'Xie', 'Xiong',
        'Ya', 'Yan', 'Yang', 'Yao', 'Yash',
        'Ye', 'Yi', 'Yin', 'Yip',
        'Yo', 'Yong', 'Yu', 'Yuan', 'Yun',
        'Za', 'Ze', 'Zh', 'Zhang', 'Zhao', 'Zheng', 'Zhou', 'Zhu',
        'Zi', 'Zo', 'Zu', 'Zul',
      ];

      for (const q of supplementQueries) {
        try {
          const beforeCount = seenLicences.size;
          const result = await executeSearch(page, q, seenLicences);
          allLeads.push(...result.leads);
          const newCount = seenLicences.size - beforeCount;
          if (newCount > 0) {
            console.log(`  "${q}": +${newCount} new (total: ${seenLicences.size})`);
          }
        } catch (err) {
          console.log(`  Error searching "${q}": ${err.message}`);
        }
      }
      console.log(`  Phase 2 complete. Total: ${allLeads.length}\n`);
    }

    // ─── Write CSV ────────────────────────────────────────────────────
    console.log(`=== Writing ${allLeads.length} leads to CSV ===`);

    const headers = [
      'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
      'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
      'profile_url', 'licence_number', 'licence_type'
    ];

    const csvLines = [headers.join(',')];
    for (const lead of allLeads) {
      csvLines.push(headers.map(h => escapeCSV(lead[h] || '')).join(','));
    }

    fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n') + '\n', 'utf8');
    console.log(`  Saved to: ${OUTPUT_FILE}`);

    // ─── Stats ────────────────────────────────────────────────────────
    const countryCounts = {};
    for (const lead of allLeads) {
      countryCounts[lead.country] = (countryCounts[lead.country] || 0) + 1;
    }

    console.log('\n=== Final Stats ===');
    console.log(`  Total advisers:    ${allLeads.length}`);
    console.log(`  With firm name:    ${allLeads.filter(l => l.firm_name).length}`);
    console.log(`  With city:         ${allLeads.filter(l => l.city).length}`);
    console.log(`  With region:       ${allLeads.filter(l => l.state).length}`);
    console.log(`  Unique licences:   ${seenLicences.size}`);
    console.log('\n  By country:');
    const sortedCountries = Object.entries(countryCounts).sort((a, b) => b[1] - a[1]);
    for (const [country, count] of sortedCountries) {
      console.log(`    ${country}: ${count}`);
    }

    console.log(`\nCompleted: ${new Date().toISOString()}`);

  } finally {
    await browser.close();
  }
}

// ─── Run ───────────────────────────────────────────────────────────────
scrape().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
