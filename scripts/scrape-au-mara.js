#!/usr/bin/env node
/**
 * Australia MARA (Migration Agents Registration Authority) Scraper
 *
 * Scrapes ALL registered migration agents from the OMARA Self-Service Portal.
 * Source: portal.mara.gov.au/search-the-register-of-migration-agents/
 *
 * The portal is a Microsoft Dynamics 365 / Power Apps site that uses an
 * encrypted entity-grid API. We use Puppeteer to:
 *   1. Load the search page and extract session tokens
 *   2. Search by state to segment the data
 *   3. Paginate through results, collecting detail page URLs
 *   4. Visit each detail page for full data (name, MARN, email, phone, etc.)
 *
 * Features:
 *   - State-by-state scraping (ACT, NSW, NT, QLD, SA, TAS, VIC, WA)
 *   - Auto-save progress CSV every 50 leads
 *   - Resume support (reads existing progress file)
 *   - Rate limiting (2-3s between detail page fetches)
 *   - Deduplication by ContactID
 *
 * Usage:
 *   node scripts/scrape-au-mara.js                    # Full scrape
 *   node scripts/scrape-au-mara.js --test              # Quick test (1 state, 2 pages)
 *   node scripts/scrape-au-mara.js --state=NSW          # Single state
 *   node scripts/scrape-au-mara.js --resume             # Resume from progress file
 *   node scripts/scrape-au-mara.js --list-only          # List mode: collect URLs only, no detail pages
 *   node scripts/scrape-au-mara.js --delay=3000         # Custom delay (ms)
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// === CLI Args ===
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE = !!args.test;
const LIST_ONLY = !!args['list-only'];
const SINGLE_STATE = args.state ? args.state.toUpperCase() : null;
const DELAY_MS = parseInt(args.delay) || 2500;
const MAX_PAGES = TEST_MODE ? 2 : 9999;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'au-mara-progress.csv');
const FINAL_FILE = path.join(OUTPUT_DIR, 'au-migration-agents.csv');
const URL_CACHE_FILE = path.join(OUTPUT_DIR, 'au-mara-urls.json');

// Australian states/territories
const STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA'];

const PORTAL_URL = 'https://portal.mara.gov.au/search-the-register-of-migration-agents/';

// === Helpers ===

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms + Math.random() * 1000));
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const columns = [
    'first_name', 'last_name', 'firm_name', 'title', 'email',
    'phone', 'website', 'domain', 'city', 'state', 'country',
    'niche', 'source', 'profile_url', 'registration_number',
    'bar_status', 'address', 'secondary_email', 'secondary_phone',
    'associated_businesses',
  ];
  const header = columns.join(',');
  const rows = leads.map(lead =>
    columns.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function loadExistingLeads() {
  if (!fs.existsSync(PROGRESS_FILE)) return [];
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return [];
    const headers = parseCSVLine(lines[0]);
    const leads = [];
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
    }
    return leads;
  } catch { return []; }
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

function extractDomain(website) {
  if (!website) return '';
  const m = website.match(/https?:\/\/(?:www\.)?([^\/\?#]+)/);
  return m ? m[1].toLowerCase() : '';
}

function cleanPhone(phone) {
  if (!phone) return '';
  let p = phone.replace(/[^0-9+]/g, '');
  // Normalize Australian numbers
  if (p.startsWith('+61')) p = '0' + p.slice(3);
  if (p.startsWith('61') && p.length > 10) p = '0' + p.slice(2);
  return p;
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

function titleCase(str) {
  if (!str) return '';
  return str.replace(/\b\w+/g, w => {
    if (['PTY', 'LTD', 'ABN', 'ACN', 'LLC', 'PO'].includes(w.toUpperCase())) return w.toUpperCase();
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  });
}

const STATE_MAP = {
  'australian capital territory': 'ACT',
  'new south wales': 'NSW',
  'northern territory': 'NT',
  'queensland': 'QLD',
  'south australia': 'SA',
  'tasmania': 'TAS',
  'victoria': 'VIC',
  'western australia': 'WA',
  'act': 'ACT', 'nsw': 'NSW', 'nt': 'NT', 'qld': 'QLD',
  'sa': 'SA', 'tas': 'TAS', 'vic': 'VIC', 'wa': 'WA',
};

function normalizeState(state) {
  if (!state) return '';
  const code = STATE_MAP[state.toLowerCase().trim()];
  return code || state.toUpperCase();
}

// === Main Scraper Logic ===

async function launchBrowser() {
  return puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
    ],
  });
}

/**
 * Collect all detail page URLs for a given state by paginating through search results.
 */
async function collectStateURLs(browser, state) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  const urls = []; // { contactId, familyName, firstName, businessName, suburb }

  try {
    log(`Loading search page for state: ${state}...`);
    await page.goto(PORTAL_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(2000);

    // Enter state in location field and search
    await page.evaluate((st) => {
      const inputs = document.querySelectorAll('.entitylist-filter-option-text input');
      // Location is the 5th field (index 4)
      if (inputs[4]) {
        inputs[4].value = st;
        inputs[4].dispatchEvent(new Event('input', { bubbles: true }));
        inputs[4].dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, state);

    const searchBtn = await page.$('.btn-entitylist-filter-submit');
    if (searchBtn) await searchBtn.click();
    await sleep(5000);

    // Get total pages
    const totalPages = await page.evaluate(() => {
      const pageLinks = document.querySelectorAll('.view-pagination li a');
      let max = 1;
      pageLinks.forEach(a => {
        const num = parseInt(a.getAttribute('data-page') || a.innerText);
        if (!isNaN(num) && num > max) max = num;
      });
      return max;
    });

    log(`  ${state}: ${totalPages} pages found`);

    let pageNum = 1;
    let consecutiveEmpty = 0;

    while (pageNum <= Math.min(totalPages, MAX_PAGES)) {
      // Extract rows from current page
      const rows = await page.evaluate(() => {
        const trs = document.querySelectorAll('.view-grid table tbody tr');
        return Array.from(trs).map(tr => {
          const link = tr.querySelector('a.details-link');
          const cells = tr.querySelectorAll('td');
          return {
            href: link ? link.href : null,
            familyName: cells[0] ? cells[0].innerText.trim() : '',
            firstName: cells[1] ? cells[1].innerText.trim() : '',
            businessName: cells[2] ? cells[2].innerText.trim() : '',
            suburb: cells[4] ? cells[4].innerText.trim() : '',
            state: cells[5] ? cells[5].innerText.trim() : '',
          };
        }).filter(r => r.href);
      });

      if (rows.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) {
          log(`  ${state}: 3 consecutive empty pages at page ${pageNum}, stopping`);
          break;
        }
      } else {
        consecutiveEmpty = 0;
      }

      for (const row of rows) {
        // Extract ContactID from URL
        const match = row.href.match(/ContactID=([a-f0-9-]+)/i);
        if (match) {
          urls.push({
            contactId: match[1],
            url: row.href,
            familyName: row.familyName,
            firstName: row.firstName,
            businessName: row.businessName,
            suburb: row.suburb,
            state: row.state || state,
          });
        }
      }

      if (pageNum % 10 === 0 || pageNum === 1) {
        log(`  ${state}: Page ${pageNum}/${totalPages} - ${urls.length} URLs collected`);
      }

      pageNum++;
      if (pageNum > Math.min(totalPages, MAX_PAGES)) break;

      // Click next page
      const clicked = await page.evaluate((targetPage) => {
        const pageLinks = document.querySelectorAll('.view-pagination li a');
        for (const link of pageLinks) {
          const pg = parseInt(link.getAttribute('data-page') || link.innerText);
          if (pg === targetPage) {
            link.click();
            return true;
          }
        }
        // Try the ">" next button
        const nextBtn = document.querySelector('.view-pagination li:last-child a');
        if (nextBtn) {
          nextBtn.click();
          return true;
        }
        return false;
      }, pageNum);

      if (!clicked) {
        log(`  ${state}: Could not navigate to page ${pageNum}, stopping`);
        break;
      }

      await sleep(3000 + Math.random() * 2000); // Wait for page load
    }

    log(`  ${state}: Collected ${urls.length} agent URLs across ${pageNum - 1} pages`);
  } catch (err) {
    log(`  ${state} ERROR: ${err.message}`);
  } finally {
    await page.close();
  }

  return urls;
}

/**
 * Fetch detail page for a single agent using Puppeteer.
 */
async function fetchDetailPage(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(1500);

    const data = await page.evaluate(() => {
      const text = document.body.innerText;

      // Extract MARN
      const marnMatch = text.match(/Migration agent registration number:\s*(\d+)/);
      const marn = marnMatch ? marnMatch[1] : '';

      // Extract status
      const statusMatch = text.match(/Current status:\s*(\w+)/);
      const status = statusMatch ? statusMatch[1] : '';

      // Extract full name: appears as standalone text line before MARN
      // Pattern: line just before "Migration agent registration number:"
      let fullName = '';
      const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].startsWith('Migration agent registration number')) {
          // Name is the line before this
          if (i > 0 && !lines[i - 1].includes('details') && !lines[i - 1].includes('Toggle')) {
            fullName = lines[i - 1];
          }
          break;
        }
      }

      // Parse the FIRST table (table.section) which has contact details as key-value rows
      // Structure: each <tr> has exactly 2 <td> cells: [label] [value]
      const details = {};
      const firstTable = document.querySelector('table.section');
      if (firstTable) {
        const trs = firstTable.querySelectorAll('tr');
        for (const tr of trs) {
          const cells = tr.querySelectorAll('td');
          if (cells.length === 2) {
            const key = cells[0].innerText.trim().toLowerCase();
            const val = cells[1].innerText.trim();
            if (key === 'business name') details.businessName = val;
            else if (key === 'business address') details.address = val;
            else if (key === 'phone') details.phone = val;
            else if (key === 'email address') {
              // Get email from mailto link or text
              const emailLink = cells[1].querySelector('a[href^="mailto:"]');
              details.email = emailLink ? emailLink.innerText.trim() : val;
            }
            else if (key === 'website') {
              const webLink = cells[1].querySelector('a#weburl, a[target="_blank"]');
              if (webLink && webLink.href && webLink.href !== 'http://' && webLink.href !== 'about:blank') {
                details.website = webLink.href;
              } else if (val && val.startsWith('http')) {
                details.website = val;
              }
              // Also try text-based extraction
              if (!details.website) {
                const webMatch = text.match(/Website\s+(https?:\/\/[^\s\n]+)/);
                if (webMatch) details.website = webMatch[1].trim();
              }
            }
          }
        }
      }

      // Fallback: parse from tab-separated text if table parsing failed
      if (!details.businessName) {
        const m = text.match(/^Business name\t(.+)$/m);
        if (m) details.businessName = m[1].trim();
      }
      if (!details.address) {
        const m = text.match(/^Business address\t(.+)$/m);
        if (m) details.address = m[1].trim();
      }
      if (!details.phone) {
        const m = text.match(/^Phone\t(.+)$/m);
        if (m) details.phone = m[1].trim();
      }
      if (!details.email) {
        const m = text.match(/^Email address\t(.+)$/m);
        if (m) details.email = m[1].trim();
      }

      // Extract associated businesses from the subgrid table(s)
      const associated = [];
      const subgrids = document.querySelectorAll('.subgrid table tbody tr, .entity-grid.subgrid table tbody tr');
      for (const row of subgrids) {
        const cells = row.querySelectorAll('td');
        // Only process rows with exactly 7 data cells (no TH)
        if (cells.length === 7 && cells[0].innerText.trim()) {
          const name = cells[0].innerText.trim();
          // Skip header-like text or empty rows
          if (name.includes('sort descending') || name.includes('sort ascending') || !name) continue;
          associated.push({
            name: name,
            email: cells[1].innerText.trim(),
            address: cells[2].innerText.trim(),
            suburb: cells[3].innerText.trim(),
            state: cells[4].innerText.trim(),
            country: cells[5].innerText.trim(),
            phone: cells[6].innerText.trim(),
          });
        }
      }

      return {
        fullName,
        marn,
        status,
        businessName: details.businessName || '',
        address: details.address || '',
        phone: details.phone || '',
        email: details.email || '',
        website: details.website || '',
        associated,
      };
    });

    return data;
  } catch (err) {
    return { error: err.message };
  }
}

/**
 * Parse address into city/state components.
 */
function parseAddress(address) {
  if (!address) return { city: '', state: '', postcode: '' };

  // Australian address format: "Street SUBURB STATE POSTCODE Country"
  // e.g., "152 Gladesville Blvd PATTERSON LAKES VIC 3197 Australia"
  const stateMatch = address.match(/\b(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b/);
  const postcodeMatch = address.match(/\b(\d{4})\b/);

  let city = '';
  if (stateMatch) {
    // Extract suburb: words before state code that are in UPPERCASE
    const beforeState = address.substring(0, stateMatch.index).trim();
    // Find the uppercase suburb part (often at the end before state)
    const parts = beforeState.split(/\s+/);
    const suburbParts = [];
    for (let i = parts.length - 1; i >= 0; i--) {
      if (parts[i] === parts[i].toUpperCase() && /[A-Z]/.test(parts[i])) {
        suburbParts.unshift(parts[i]);
      } else {
        break;
      }
    }
    city = suburbParts.join(' ');
  }

  return {
    city: city || '',
    state: stateMatch ? stateMatch[1] : '',
    postcode: postcodeMatch ? postcodeMatch[1] : '',
  };
}

async function main() {
  const startTime = Date.now();

  console.log('\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557');
  console.log('\u2551  MORTAR \u2014 Australia MARA Migration Agents Scraper         \u2551');
  console.log('\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n');
  console.log(`  Source:    portal.mara.gov.au`);
  console.log(`  States:    ${SINGLE_STATE || STATES.join(', ')}`);
  console.log(`  Delay:     ${DELAY_MS}ms between requests`);
  console.log(`  Mode:      ${TEST_MODE ? 'TEST (2 pages per state)' : LIST_ONLY ? 'LIST ONLY (URLs only)' : 'FULL SCRAPE'}`);
  console.log(`  Output:    ${FINAL_FILE}`);
  console.log('');

  // Resume support
  let allLeads = [];
  const seenContactIds = new Set();

  if (args.resume) {
    allLeads = loadExistingLeads();
    for (const lead of allLeads) {
      const idMatch = (lead.profile_url || '').match(/ContactID=([a-f0-9-]+)/i);
      if (idMatch) seenContactIds.add(idMatch[1]);
    }
    log(`Resumed: ${allLeads.length} existing leads, ${seenContactIds.size} contact IDs`);
  }

  const browser = await launchBrowser();
  const statesToScrape = SINGLE_STATE ? [SINGLE_STATE] : STATES;

  // Phase 1: Collect all URLs
  let allUrls = [];

  // Check for cached URLs
  if (args.resume && fs.existsSync(URL_CACHE_FILE)) {
    try {
      allUrls = JSON.parse(fs.readFileSync(URL_CACHE_FILE, 'utf8'));
      log(`Loaded ${allUrls.length} cached URLs from ${URL_CACHE_FILE}`);
    } catch {}
  }

  if (allUrls.length === 0) {
    log('Phase 1: Collecting agent URLs from search results...\n');

    for (const state of statesToScrape) {
      const stateUrls = await collectStateURLs(browser, state);
      allUrls.push(...stateUrls);
      log(`  Cumulative: ${allUrls.length} URLs\n`);
      await sleep(2000);
    }

    // Deduplicate URLs by contactId
    const uniqueUrls = [];
    const seenIds = new Set();
    for (const u of allUrls) {
      if (!seenIds.has(u.contactId)) {
        seenIds.add(u.contactId);
        uniqueUrls.push(u);
      }
    }
    allUrls = uniqueUrls;

    // Cache URLs
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(URL_CACHE_FILE, JSON.stringify(allUrls, null, 2));
    log(`Phase 1 complete: ${allUrls.length} unique agent URLs cached\n`);
  }

  if (LIST_ONLY) {
    log(`List-only mode. ${allUrls.length} URLs collected. Exiting.`);
    await browser.close();
    return;
  }

  // Phase 2: Fetch detail pages
  log(`Phase 2: Fetching ${allUrls.length} detail pages...\n`);

  // Filter out already-fetched contacts
  const urlsToFetch = allUrls.filter(u => !seenContactIds.has(u.contactId));
  log(`  ${urlsToFetch.length} new agents to fetch (${seenContactIds.size} already done)\n`);

  let fetched = 0;
  let errors = 0;
  let withEmail = 0;
  let withPhone = 0;
  let consecutiveErrors = 0;

  // Use a mutable page reference for error recovery
  let currentPage = await browser.newPage();
  await currentPage.setViewport({ width: 1280, height: 900 });

  async function getPage() {
    // Check if current page is still usable
    try {
      await currentPage.evaluate(() => true);
      return currentPage;
    } catch {
      // Page is dead, create a new one
      try { await currentPage.close(); } catch {}
      log('  Creating fresh browser page...');
      currentPage = await browser.newPage();
      await currentPage.setViewport({ width: 1280, height: 900 });
      return currentPage;
    }
  }

  for (const agentUrl of urlsToFetch) {
    fetched++;

    try {
      const pg = await getPage();
      const data = await fetchDetailPage(pg, agentUrl.url);

      if (data.error) {
        errors++;
        consecutiveErrors++;
        if (fetched <= 5 || consecutiveErrors <= 3) {
          log(`  ERROR ${agentUrl.contactId}: ${data.error}`);
        }

        if (consecutiveErrors >= 5) {
          log(`  ${consecutiveErrors} consecutive errors - recreating page...`);
          try { await currentPage.close(); } catch {}
          await sleep(5000);
          currentPage = await browser.newPage();
          await currentPage.setViewport({ width: 1280, height: 900 });
          consecutiveErrors = 0;
        }
        continue;
      }

      consecutiveErrors = 0;

      // Parse the name - use detail page fullName, fall back to search page data
      let firstName = '';
      let lastName = '';
      if (data.fullName) {
        const nameParts = splitName(data.fullName);
        firstName = nameParts.first_name;
        lastName = nameParts.last_name;
      }
      if (!firstName && agentUrl.firstName) firstName = agentUrl.firstName;
      if (!lastName && agentUrl.familyName) lastName = agentUrl.familyName;

      // Parse address for city/state
      const addrParts = parseAddress(data.address);
      const city = addrParts.city || agentUrl.suburb || '';
      const state = normalizeState(addrParts.state || agentUrl.state || '');

      // Build lead
      const lead = {
        first_name: titleCase(firstName),
        last_name: titleCase(lastName),
        firm_name: data.businessName || agentUrl.businessName || '',
        title: 'Registered Migration Agent',
        email: data.email || '',
        phone: cleanPhone(data.phone),
        website: data.website || '',
        domain: extractDomain(data.website),
        city: titleCase(city),
        state: state,
        country: 'AU',
        niche: 'Migration Agent',
        source: 'au-mara',
        profile_url: agentUrl.url,
        registration_number: data.marn || '',
        bar_status: data.status || '',
        address: data.address || '',
        secondary_email: '',
        secondary_phone: '',
        associated_businesses: '',
      };

      // Add associated business data
      if (data.associated && data.associated.length > 0) {
        lead.associated_businesses = data.associated.map(a => a.name).join('; ');
        // Use associated business email/phone if primary is empty
        if (!lead.email && data.associated[0].email) {
          lead.email = data.associated[0].email;
        }
        if (!lead.phone && data.associated[0].phone) {
          lead.phone = cleanPhone(data.associated[0].phone);
        }
        // Store secondary contact info
        const secEmails = data.associated
          .map(a => a.email)
          .filter(e => e && e !== lead.email);
        if (secEmails.length) lead.secondary_email = secEmails[0];

        const secPhones = data.associated
          .map(a => a.phone)
          .filter(p => p && cleanPhone(p) !== lead.phone);
        if (secPhones.length) lead.secondary_phone = cleanPhone(secPhones[0]);
      }

      if (lead.email) withEmail++;
      if (lead.phone) withPhone++;

      allLeads.push(lead);
      seenContactIds.add(agentUrl.contactId);

      // Progress logging
      if (fetched % 50 === 0 || fetched === 1) {
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = (fetched / elapsed * 60).toFixed(0);
        const pct = ((fetched / urlsToFetch.length) * 100).toFixed(1);
        const remaining = Math.round((urlsToFetch.length - fetched) / (fetched / elapsed) / 60);
        log(`  [${pct}%] ${fetched}/${urlsToFetch.length} | ${allLeads.length} leads (${withEmail} email, ${withPhone} phone) | ${rate}/min | ~${remaining}min left`);
      }

      // Auto-save every 50 leads
      if (allLeads.length % 50 === 0) {
        writeCSV(PROGRESS_FILE, allLeads);
      }

    } catch (err) {
      errors++;
      consecutiveErrors++;
      log(`  ERROR fetching ${agentUrl.contactId}: ${err.message}`);

      // If page is dead, recreate it
      if (err.message.includes('detached') || err.message.includes('destroyed') || err.message.includes('closed')) {
        try { await currentPage.close(); } catch {}
        await sleep(3000);
        currentPage = await browser.newPage();
        await currentPage.setViewport({ width: 1280, height: 900 });
        consecutiveErrors = 0;
      }
    }

    await sleep(DELAY_MS);
  }

  try { await currentPage.close(); } catch {}
  await browser.close();

  // Final save
  writeCSV(FINAL_FILE, allLeads);
  writeCSV(PROGRESS_FILE, allLeads);

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557');
  console.log('\u2551  SCRAPE COMPLETE                                          \u2551');
  console.log('\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n');
  console.log(`  Total leads:     ${allLeads.length}`);
  console.log(`  With email:      ${withEmail}`);
  console.log(`  With phone:      ${withPhone}`);
  console.log(`  Errors:          ${errors}`);
  console.log(`  Time:            ${elapsed} minutes`);
  console.log(`  Output:          ${FINAL_FILE}`);

  // State breakdown
  const stateCounts = {};
  for (const lead of allLeads) {
    const st = lead.state || 'Unknown';
    stateCounts[st] = (stateCounts[st] || 0) + 1;
  }
  console.log('\n  State breakdown:');
  for (const [st, count] of Object.entries(stateCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${st}: ${count}`);
  }
  console.log('');
}

main().catch(err => {
  console.error('FATAL ERROR:', err);
  process.exit(1);
});
