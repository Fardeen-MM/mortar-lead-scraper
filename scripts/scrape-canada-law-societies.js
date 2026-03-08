#!/usr/bin/env node
/**
 * Canadian Provincial Law Society Immigration Lawyer Scraper
 *
 * Scrapes three sources:
 *   1. Law Society of Ontario (LSO) — Certified Specialists in Immigration Law (Puppeteer)
 *   2. CBA British Columbia — Find-a-Lawyer by practice area "Immigration Law" (Puppeteer)
 *   3. Barreau du Québec — Find-a-Lawyer by immigration specialty code 575 (HTTP)
 *
 * Output: output/canada-immigration-lawyers-provincial.csv
 *
 * Usage:
 *   node scripts/scrape-canada-law-societies.js              # Full scrape (all 3)
 *   node scripts/scrape-canada-law-societies.js --lso        # Ontario only
 *   node scripts/scrape-canada-law-societies.js --lsbc       # BC only
 *   node scripts/scrape-canada-law-societies.js --barreau    # Quebec only
 *   node scripts/scrape-canada-law-societies.js --test       # Quick test (limited pages)
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ─── Config ───────────────────────────────────────────────────
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'canada-immigration-lawyers-provincial.csv');
const DELAY_MS = 2500;
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'bar_number', 'bar_status', 'admission_date',
];

// ─── CLI args ─────────────────────────────────────────────────
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const onlyLso = args.includes('--lso');
const onlyLsbc = args.includes('--lsbc');
const onlyBarreau = args.includes('--barreau');
const runAll = !onlyLso && !onlyLsbc && !onlyBarreau;

// ─── Helpers ──────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function httpGet(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...opts.headers,
      },
    };
    const req = mod.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const loc = res.headers.location.startsWith('http')
            ? res.headers.location
            : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
          httpGet(loc, opts).then(resolve).catch(reject);
          return;
        }
        resolve({ status: res.statusCode, body: data, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function csvEscape(val) {
  if (val == null) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col])).join(',')
  );
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function splitName(fullName) {
  const clean = (fullName || '').replace(/,\s*$/, '').trim();
  // Handle "Last, First" format
  if (clean.includes(',')) {
    const [last, ...rest] = clean.split(',').map(s => s.trim());
    const first = rest.join(' ').trim();
    return { first_name: first, last_name: last };
  }
  const parts = clean.split(/\s+/);
  if (parts.length === 1) return { first_name: '', last_name: parts[0] };
  return { first_name: parts.slice(0, -1).join(' '), last_name: parts[parts.length - 1] };
}

function extractDomain(website) {
  if (!website) return '';
  const m = website.match(/https?:\/\/(?:www\.)?([^\/\?#]+)/);
  return m ? m[1].toLowerCase() : '';
}

function normalizePhone(phone) {
  if (!phone) return '';
  let digits = phone.replace(/[^\d+]/g, '');
  if (digits.startsWith('+1')) digits = digits.slice(2);
  if (digits.startsWith('1') && digits.length === 11) digits = digits.slice(1);
  if (digits.length !== 10) return phone.trim();
  return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
}

async function launchBrowser() {
  const puppeteer = require('puppeteer');
  return puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });
}


// ═══════════════════════════════════════════════════════════════
//  SOURCE 1: Law Society of Ontario (LSO) — Puppeteer
// ═══════════════════════════════════════════════════════════════

async function scrapeLSO(browser) {
  console.log('\n  ── LSO (Ontario) ─ Certified Immigration Specialists ──');
  console.log('  Using Puppeteer to bypass Cloudflare...\n');

  const leads = [];

  try {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1280, height: 800 });

    // Try the main certified specialists page first
    console.log('  Loading lso.ca certified specialists page...');
    await page.goto('https://lso.ca/directory-of-certified-specialists/citizenship-and-immigration-law/immigration', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    // Wait for Cloudflare challenge
    const title = await page.title();
    if (title.includes('Just a moment')) {
      console.log('  Waiting for Cloudflare challenge...');
      try {
        await page.waitForFunction(() => !document.title.includes('Just a moment'), { timeout: 30000 });
        await sleep(3000);
      } catch {
        console.log('  Cloudflare challenge did not resolve. Trying JSP URL...');
      }
    }

    let html = await page.content();
    let $ = cheerio.load(html);

    // Parse the LSO specialist page - extract lawyer cards/entries
    let count = extractLSOLeads($, leads);
    console.log(`  Main page: ${count} lawyers extracted`);

    // If low results, try the JSP URL
    if (count < 5) {
      console.log('  Trying JSP directory URL...');
      try {
        await page.goto('https://www1.lso.ca/specialist/jsp/namelist1.jsp?code=CII&region=', {
          waitUntil: 'networkidle2',
          timeout: 60000,
        });

        const jspTitle = await page.title();
        if (jspTitle.includes('Just a moment')) {
          console.log('  Waiting for Cloudflare on JSP...');
          try {
            await page.waitForFunction(() => !document.title.includes('Just a moment'), { timeout: 30000 });
            await sleep(3000);
          } catch {
            console.log('  JSP Cloudflare did not resolve.');
          }
        }

        html = await page.content();
        $ = cheerio.load(html);
        count = extractLSOLeads($, leads);
        console.log(`  JSP page: ${count} additional lawyers`);
      } catch (err) {
        console.log(`  JSP error: ${err.message}`);
      }
    }

    // If still low results, try the Refugee Protection specialists too
    if (leads.length < 100) {
      console.log('  Also trying Refugee Protection specialists...');
      try {
        await page.goto('https://lso.ca/directory-of-certified-specialists/citizenship-and-immigration-law/refugee-protection', {
          waitUntil: 'networkidle2',
          timeout: 60000,
        });

        const rfTitle = await page.title();
        if (rfTitle.includes('Just a moment')) {
          try {
            await page.waitForFunction(() => !document.title.includes('Just a moment'), { timeout: 20000 });
            await sleep(3000);
          } catch {}
        }

        html = await page.content();
        $ = cheerio.load(html);
        const prevCount = leads.length;
        extractLSOLeads($, leads);
        console.log(`  Refugee protection: ${leads.length - prevCount} additional`);
      } catch (err) {
        console.log(`  Refugee page error: ${err.message}`);
      }
    }

    await page.close();
  } catch (err) {
    console.log(`  LSO error: ${err.message}`);
  }

  // Deduplicate
  const seen = new Set();
  const unique = [];
  for (const l of leads) {
    const key = `${l.first_name}|${l.last_name}`.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(l);
    }
  }

  console.log(`  LSO total: ${unique.length} immigration lawyers`);
  return unique;
}

function extractLSOLeads($, leads) {
  let count = 0;

  // Strategy 1: Look for table rows with lawyer data
  // Skip common false positives (cities, addresses, footer text)
  const falsePositiveNames = new Set([
    'north york', 'toronto', 'ottawa', 'mississauga', 'osgoode hall',
    'contact law', 'contact us', 'law society', 'certified specialist',
  ]);

  $('table tr').each((i, row) => {
    const cells = $(row).find('td');
    if (cells.length < 1) return;

    const firstCell = $(cells[0]).text().trim();
    // Skip header rows and short text
    if (!firstCell || firstCell.length < 3 || /^(name|lawyer|specialist|#)/i.test(firstCell)) return;

    const { first_name, last_name } = splitName(firstCell);
    if (!first_name || !last_name || last_name.length < 2) return;

    // Skip false positives (cities, addresses parsed as names)
    const nameKey = `${first_name} ${last_name}`.toLowerCase();
    if (falsePositiveNames.has(nameKey) || falsePositiveNames.has(first_name.toLowerCase())) return;
    // Skip if "name" looks like a street address or organization
    if (/\d{3,}|street|avenue|road|drive|boulevard|osgoode|society|ontario/i.test(nameKey)) return;

    const firmText = cells.length >= 2 ? $(cells[1]).text().trim() : '';
    const cityText = cells.length >= 3 ? $(cells[2]).text().trim() : '';
    const phoneText = cells.length >= 4 ? $(cells[3]).text().trim() : '';

    leads.push({
      first_name, last_name,
      firm_name: firmText,
      city: cityText,
      phone: normalizePhone(phoneText),
      state: 'ON', country: 'CA',
      niche: 'Immigration Law', source: 'lso',
      title: 'Certified Specialist',
    });
    count++;
  });

  // Strategy 2: Look for lawyer entries in div/list structures
  const contentArea = $('main, .content-area, .page-content, article, .field-items, #block-system-main');
  contentArea.find('a').each((i, el) => {
    const text = $(el).text().trim();
    const href = $(el).attr('href') || '';
    if (!text || text.length < 5 || text.length > 80) return;
    if (/^(home|about|contact|search|find|directory|back|next|prev|menu|skip)/i.test(text)) return;

    // Check if it looks like a person name (First Last or Last, First)
    if (text.match(/^[A-Z][a-z]+[\s,.]+[A-Z][a-z]+/)) {
      const { first_name, last_name } = splitName(text);
      if (first_name && last_name && last_name.length > 1) {
        // Check if already in leads
        const key = `${first_name}|${last_name}`.toLowerCase();
        const exists = leads.some(l => `${l.first_name}|${l.last_name}`.toLowerCase() === key);
        if (!exists) {
          leads.push({
            first_name, last_name,
            firm_name: '',
            city: '',
            state: 'ON', country: 'CA',
            niche: 'Immigration Law', source: 'lso',
            title: 'Certified Specialist',
            profile_url: href.startsWith('http') ? href : (href ? `https://lso.ca${href}` : ''),
          });
          count++;
        }
      }
    }
  });

  // Strategy 3 removed — the block-level parser produces too many false positives
  // from address fragments and footer text. Link-based extraction (Strategy 2) is sufficient.

  return count;
}


// ═══════════════════════════════════════════════════════════════
//  SOURCE 2: CBA British Columbia — Puppeteer
//  Uses CBA BC "Find a Lawyer" which has practice area filter
//  Unlike LSBC directory which only searches by name
// ═══════════════════════════════════════════════════════════════

async function scrapeLSBC(browser) {
  console.log('\n  ── CBA BC ─ Immigration Lawyers (British Columbia) ────');
  console.log('  Using CBA BC Find-a-Lawyer with Immigration filter...\n');

  const leads = [];
  const seen = new Set();

  try {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1280, height: 900 });

    // Load the search page
    console.log('  Loading CBA BC Find-a-Lawyer...');
    await page.goto('https://www.legacy.cbabc.org/Directory/Find-a-Lawyer', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    await sleep(2000);

    // Select "Immigration Law" from the Areas of Practice dropdown
    console.log('  Selecting Immigration Law practice area...');
    await page.select('#p_lt_ctl07_pageplaceholder_p_lt_ctl02_CBA_IMISFindALawyerFilter_ddlAreaOfLaw', 'IMM');
    await sleep(1000);

    // Click the search button
    console.log('  Submitting search...');
    const searchBtn = await page.$('input[value="Search"]');
    if (searchBtn) {
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {}),
        searchBtn.click(),
      ]);
    }

    await sleep(3000);

    // Paginate and extract all results
    let pageNum = 1;
    const maxPages = testMode ? 3 : 30;

    while (pageNum <= maxPages) {
      // Extract lawyer data from current page using page.evaluate
      const pageData = await page.evaluate(() => {
        const lawyerLinks = document.querySelectorAll('a[href*="Lawyers?ID="]');
        const results = [];
        lawyerLinks.forEach(a => {
          const card = a.closest('div') || a.parentElement?.parentElement;
          const fullText = card ? card.innerText : '';

          const phoneMatch = fullText.match(/Phone:\s*(.+)/);
          const websiteMatch = fullText.match(/Website:\s*(.+)/);
          const callMatch = fullText.match(/(?:First Year of Call|BC Year of Call):\s*(\d{4})/);
          const langMatch = fullText.match(/Languages:\s*(.+)/);

          results.push({
            name: a.textContent.trim(),
            href: a.getAttribute('href'),
            phone: phoneMatch ? phoneMatch[1].trim() : '',
            website: websiteMatch ? websiteMatch[1].trim() : '',
            yearOfCall: callMatch ? callMatch[1] : '',
            languages: langMatch ? langMatch[1].trim() : '',
          });
        });

        // Check for "next page" link
        const nextLink = document.querySelector('.page-next a, a[title*="next"], a span.NextPage');
        const hasNext = !!nextLink;

        // Get total count
        const pagerText = document.querySelector('.pager .display')?.textContent || '';
        const totalMatch = pagerText.match(/of\s+(\d+)/);
        const total = totalMatch ? parseInt(totalMatch[1]) : 0;

        return { results, hasNext, total };
      });

      if (pageNum === 1 && pageData.total > 0) {
        console.log(`  Total results: ${pageData.total}`);
      }

      let newCount = 0;
      for (const item of pageData.results) {
        // Clean name — remove honorifics and credentials
        let cleanName = item.name
          .replace(/^(Dr\.\s*|Mr\.\s*|Ms\.\s*|Mrs\.\s*)/, '')
          .replace(/,\s*(Q\.?C\.?|K\.?C\.?|J\.?D\.?\s*(\([^)]+\))?|LL\.?B\.?|LL\.?M\.?|B\.?C\.?L\.?|M\.?A\.?|Ph\.?D\.?|B\.?A\.?).*$/i, '')
          .trim();

        const { first_name, last_name } = splitName(cleanName);
        if (!first_name || !last_name || last_name.length < 2) continue;

        const key = `${first_name}|${last_name}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);

        let website = item.website || '';
        if (website && !website.startsWith('http')) website = 'https://' + website;

        const profileUrl = item.href
          ? (item.href.startsWith('http') ? item.href : `https://www.legacy.cbabc.org${item.href}`)
          : '';

        leads.push({
          first_name, last_name,
          phone: normalizePhone(item.phone),
          website,
          domain: extractDomain(website),
          admission_date: item.yearOfCall,
          state: 'BC', country: 'CA',
          niche: 'Immigration Law', source: 'lsbc',
          profile_url: profileUrl,
        });
        newCount++;
      }

      console.log(`  Page ${pageNum}: +${newCount} lawyers (${leads.length} total)`);

      if (!pageData.hasNext || newCount === 0) break;

      // Click next page
      try {
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {}),
          page.evaluate(() => {
            const next = document.querySelector('.page-next a') ||
                         document.querySelector('a span.NextPage')?.parentElement;
            if (next) next.click();
          }),
        ]);
        await sleep(DELAY_MS);
        pageNum++;
      } catch {
        break;
      }
    }

    await page.close();
  } catch (err) {
    console.log(`  CBA BC error: ${err.message}`);
  }

  console.log(`\n  BC total: ${leads.length} immigration lawyers`);
  return leads;
}

// No fallback functions needed — CBA BC Puppeteer approach is sufficient


// ═══════════════════════════════════════════════════════════════
//  SOURCE 3: Barreau du Québec — HTTP
// ═══════════════════════════════════════════════════════════════

async function scrapeBarreau() {
  console.log('\n  ── Barreau du Québec ─ Immigration Lawyers ────────────');
  console.log('  Searching by field of law: Immigration (code 575)...\n');

  const leads = [];
  const seen = new Set();
  const BASE = 'https://www.barreau.qc.ca';

  // First page to get total count and pages
  const firstUrl = `${BASE}/en/find-a-lawyer/results/?sop=575`;
  let totalPages = 1;

  try {
    const resp = await httpGet(firstUrl);
    if (resp.status !== 200) {
      console.log(`  HTTP ${resp.status} — cannot access Barreau`);
      return [];
    }

    const $ = cheerio.load(resp.body);

    const countText = $('p.mb-0').first().text().trim();
    const countMatch = countText.match(/(\d+)\s+results?/);
    const totalResults = countMatch ? parseInt(countMatch[1]) : 0;
    console.log(`  Total results: ${totalResults}`);

    // Get total pages from pagination
    const pageLinks = $('nav[aria-labelledby="paginationTitle"] a[href*="p="]');
    let maxPage = 1;
    pageLinks.each((i, el) => {
      const href = $(el).attr('href') || '';
      const pm = href.match(/p=(\d+)/);
      if (pm) maxPage = Math.max(maxPage, parseInt(pm[1]));
    });
    totalPages = maxPage;
    console.log(`  Total pages: ${totalPages}`);

    // Parse first page
    parseBarreauResultsPage($, leads, seen);
  } catch (err) {
    console.log(`  Error: ${err.message}`);
    return [];
  }

  // Paginate
  const maxPages = testMode ? Math.min(3, totalPages) : totalPages;

  for (let page = 2; page <= maxPages; page++) {
    if (page % 10 === 0 || page === 2) {
      console.log(`  Page ${page}/${maxPages} | ${leads.length} lawyers so far`);
    }

    try {
      const url = `${BASE}/en/find-a-lawyer/results/?p=${page}&sop=575`;
      const resp = await httpGet(url);

      if (resp.status !== 200) {
        console.log(`    HTTP ${resp.status}`);
        continue;
      }

      const $ = cheerio.load(resp.body);
      parseBarreauResultsPage($, leads, seen);
    } catch (err) {
      console.log(`    Page ${page} error: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  console.log(`\n  Barreau: ${leads.length} immigration lawyers found`);
  return leads;
}

function parseBarreauResultsPage($, leads, seen) {
  const rows = $('table tbody tr');

  rows.each((i, row) => {
    const cells = $(row).find('td');
    if (cells.length < 3) return;

    // Column 1: Name link "Last, First"
    const nameLink = $(cells[0]).find('a');
    const nameText = nameLink.find('.pf-button-label').text().trim() || nameLink.text().trim();
    const href = nameLink.attr('href') || '';

    // Column 2: City
    const citySpans = $(cells[1]).find('span.block');
    const city = $(citySpans[0]).text().trim();

    // Column 3: Company/employer
    const firmName = $(cells[2]).text().trim();

    if (!nameText || nameText.length < 3) return;

    const key = nameText.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);

    const { first_name, last_name } = splitName(nameText);
    if (!last_name) return;

    const profileUrl = href.startsWith('http') ? href : (href ? `https://www.barreau.qc.ca${href}` : '');
    // Clean backUrl from profile URL for cleaner output
    const cleanProfileUrl = profileUrl.replace(/&backUrl=[^&]*/, '');

    leads.push({
      first_name: first_name || '',
      last_name,
      firm_name: firmName || '',
      city: city || '',
      state: 'QC',
      country: 'CA',
      niche: 'Immigration Law',
      source: 'barreau-qc',
      profile_url: cleanProfileUrl,
    });
  });
}


// ═══════════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════════

async function main() {
  const startTime = Date.now();

  console.log('\n╔═══════════════════════════════════════════════════════════════╗');
  console.log('║  MORTAR — Canadian Provincial Law Society Scraper            ║');
  console.log('║  Immigration Lawyers: LSO + LSBC + Barreau du Québec        ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝');
  console.log(`  Mode: ${testMode ? 'TEST' : 'FULL'}`);
  console.log(`  Sources: ${runAll ? 'ALL' : [onlyLso && 'LSO', onlyLsbc && 'LSBC', onlyBarreau && 'Barreau'].filter(Boolean).join(', ')}`);

  const allLeads = [];
  let browser = null;

  // Launch browser once if needed for LSO or LSBC
  const needsBrowser = runAll || onlyLso || onlyLsbc;
  if (needsBrowser) {
    try {
      browser = await launchBrowser();
      console.log('  Browser launched.\n');
    } catch (err) {
      console.log(`  Cannot launch browser: ${err.message}`);
      console.log('  LSO and LSBC will use HTTP fallback.\n');
    }
  }

  // ─── Source 1: LSO (Ontario) ────────────────────────────
  if (runAll || onlyLso) {
    try {
      const lsoLeads = browser ? await scrapeLSO(browser) : [];
      allLeads.push(...lsoLeads);
      if (allLeads.length > 0) writeCSV(OUTPUT_FILE, allLeads);
    } catch (err) {
      console.log(`  LSO fatal error: ${err.message}`);
    }
  }

  // ─── Source 2: LSBC / CBA BC ───────────────────────────
  if (runAll || onlyLsbc) {
    try {
      const bcLeads = browser ? await scrapeLSBC(browser) : [];
      allLeads.push(...bcLeads);
      if (allLeads.length > 0) writeCSV(OUTPUT_FILE, allLeads);
    } catch (err) {
      console.log(`  LSBC fatal error: ${err.message}`);
    }
  }

  // ─── Source 3: Barreau du Québec ────────────────────────
  if (runAll || onlyBarreau) {
    try {
      const barreauLeads = await scrapeBarreau();
      allLeads.push(...barreauLeads);
      if (allLeads.length > 0) writeCSV(OUTPUT_FILE, allLeads);
    } catch (err) {
      console.log(`  Barreau fatal error: ${err.message}`);
    }
  }

  // Close browser
  if (browser) {
    try { await browser.close(); } catch {}
  }

  // ─── Summary ──────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  const lsoCount = allLeads.filter(l => l.source === 'lso').length;
  const lsbcCount = allLeads.filter(l => l.source === 'lsbc').length;
  const barreauCount = allLeads.filter(l => l.source === 'barreau-qc').length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const cities = new Set(allLeads.map(l => l.city).filter(Boolean));

  console.log('\n╔═══════════════════════════════════════════════════════════════╗');
  console.log('║  SCRAPE COMPLETE                                            ║');
  console.log('╠═══════════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:     ${String(allLeads.length).padEnd(42)}║`);
  console.log(`║  ├─ LSO (ON):     ${String(lsoCount).padEnd(42)}║`);
  console.log(`║  ├─ LSBC (BC):    ${String(lsbcCount).padEnd(42)}║`);
  console.log(`║  └─ Barreau (QC): ${String(barreauCount).padEnd(42)}║`);
  console.log(`║  With email:      ${String(withEmail).padEnd(42)}║`);
  console.log(`║  With phone:      ${String(withPhone).padEnd(42)}║`);
  console.log(`║  With firm:       ${String(withFirm).padEnd(42)}║`);
  console.log(`║  Unique cities:   ${String(cities.size).padEnd(42)}║`);
  console.log(`║  Time:            ${String(elapsed + 's').padEnd(42)}║`);
  console.log('╠═══════════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${OUTPUT_FILE}`);
  console.log('╚═══════════════════════════════════════════════════════════════╝\n');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
