#!/usr/bin/env node
/**
 * UK Law Society "Find a Solicitor" Immigration Scraper
 *
 * Scrapes https://solicitors.lawsociety.org.uk/ filtered to immigration solicitors.
 * Uses Puppeteer in headful mode (reCAPTCHA Enterprise blocks headless browsers).
 *
 * Strategy:
 *   1. Load homepage to establish session/cookies
 *   2. Submit Quick Search with "Immigration and changing countries" (LIUIMM)
 *   3. Paginate through all result pages, extracting firm cards
 *   4. Also run Pro Search for specific practice areas: IMM, IMA, IMN, IMG, IML
 *   5. Extract: firm name, address, phone, email, website, accreditations, people/offices links
 *   6. Optionally visit office detail pages for extra contact info
 *   7. Deduplicate by office ID
 *   8. Incremental save to CSV after each page
 *
 * Usage:
 *   node scripts/scrape-uk-lawsociety-immigration.js                 # Full run (Quick Search + Pro Search)
 *   node scripts/scrape-uk-lawsociety-immigration.js --test          # Test mode (Quick Search, 2 pages max)
 *   node scripts/scrape-uk-lawsociety-immigration.js --quick-only    # Only Quick Search (LIUIMM)
 *   node scripts/scrape-uk-lawsociety-immigration.js --pro-only      # Only Pro Search (IMM, IMA, IMN, IMG, IML)
 *   node scripts/scrape-uk-lawsociety-immigration.js --max-pages 10  # Limit pages per search
 *   node scripts/scrape-uk-lawsociety-immigration.js --details        # Also fetch office detail pages
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

// ── Config ────────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 4000;
const RECAPTCHA_WAIT = 10000;
const NAV_TIMEOUT = 30000;
const MAX_CAPTCHA_RETRIES = 3;

const BASE_URL = 'https://solicitors.lawsociety.org.uk';

// Quick Search category for immigration
const QUICK_SEARCH_CATEGORY = 'LIUIMM'; // "Immigration and changing countries"

// Pro Search practice area codes
const PRO_SEARCH_AREAS = [
  { code: 'IMM', label: 'Immigration - general' },
  { code: 'IMA', label: 'Immigration - asylum' },
  { code: 'IMN', label: 'Immigration - nationality and citizenship' },
  { code: 'IMG', label: 'Immigration - general - legal aid' },
  { code: 'IML', label: 'Immigration - asylum - legal aid' },
];

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-solicitors-lawsociety.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url',
];

// ── CLI Args ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const isTest = args.includes('--test');
const quickOnly = args.includes('--quick-only');
const proOnly = args.includes('--pro-only');
const fetchDetails = args.includes('--details');
const maxPagesArg = args.indexOf('--max-pages');
const maxPages = maxPagesArg >= 0 ? parseInt(args[maxPagesArg + 1], 10) : (isTest ? 2 : 200);

// ── Helpers ───────────────────────────────────────────────────────────────────

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
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  let p = phone.replace(/[\s\-\(\)\.]/g, '').trim();
  if (p.startsWith('0')) {
    p = '+44' + p.slice(1);
  } else if (p.match(/^\d{10,11}$/) && !p.startsWith('+')) {
    p = '+44' + p;
  }
  return p;
}

function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase()
    .replace(/(?:^|\s|-|')\S/g, c => c.toUpperCase())
    .replace(/\bLlp\b/g, 'LLP')
    .replace(/\bLtd\b/g, 'Ltd')
    .replace(/\bLimited\b/g, 'Limited');
}

function parseCityFromAddress(address) {
  if (!address) return '';
  const parts = address.split(',').map(p => p.trim());

  const skipPatterns = [
    /^england$/i, /^wales$/i, /^scotland$/i, /^northern ireland$/i,
    /^united kingdom$/i, /^uk$/i,
    /^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i, // UK postcode
  ];

  const counties = new Set([
    'middlesex', 'surrey', 'essex', 'kent', 'sussex', 'hampshire', 'berkshire',
    'hertfordshire', 'buckinghamshire', 'oxfordshire', 'cambridgeshire', 'norfolk',
    'suffolk', 'devon', 'dorset', 'somerset', 'wiltshire', 'gloucestershire',
    'warwickshire', 'staffordshire', 'lancashire', 'yorkshire', 'cheshire',
    'derbyshire', 'nottinghamshire', 'leicestershire', 'lincolnshire', 'shropshire',
    'herefordshire', 'worcestershire', 'northamptonshire', 'bedfordshire', 'cornwall',
    'cumbria', 'tyne and wear', 'merseyside', 'west midlands', 'greater manchester',
    'south yorkshire', 'west yorkshire', 'east sussex', 'west sussex',
    'north yorkshire', 'east riding of yorkshire', 'greater london',
  ]);

  const knownCities = new Set([
    'london', 'manchester', 'birmingham', 'leeds', 'liverpool', 'sheffield',
    'bristol', 'cardiff', 'edinburgh', 'glasgow', 'belfast', 'newcastle',
    'nottingham', 'leicester', 'coventry', 'bradford', 'stoke-on-trent',
    'wolverhampton', 'plymouth', 'derby', 'southampton', 'portsmouth',
    'oxford', 'cambridge', 'brighton', 'york', 'bath', 'exeter', 'norwich',
    'chester', 'lincoln', 'durham', 'carlisle', 'canterbury', 'winchester',
    'worcester', 'gloucester', 'sunderland', 'luton', 'reading', 'bolton',
    'bournemouth', 'blackburn', 'oldham', 'rochdale', 'blackpool', 'burnley',
    'harrow', 'ilford', 'croydon', 'ealing', 'barking', 'enfield', 'wembley',
    'slough', 'watford', 'woking', 'guildford', 'colchester', 'ipswich',
    'swansea', 'newport', 'wrexham', 'bangor', 'dundee', 'aberdeen', 'inverness',
    'peterborough', 'chelmsford', 'southend-on-sea', 'northampton', 'swindon',
    'crawley', 'basildon', 'chatham', 'maidstone', 'stevenage', 'stockport',
    'warrington', 'wigan', 'solihull', 'sutton coldfield', 'dudley', 'walsall',
    'telford', 'middlesbrough', 'doncaster', 'rotherham', 'barnsley', 'halifax',
    'huddersfield', 'wakefield', 'dewsbury', 'batley', 'keighley',
  ]);

  // First pass: look for known cities
  for (const part of parts) {
    if (knownCities.has(part.toLowerCase())) {
      return titleCase(part);
    }
  }

  // Second pass: filter out non-city parts
  const candidates = [];
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    if (skipPatterns.some(p => p.test(part))) continue;
    if (counties.has(part.toLowerCase())) continue;
    if (/\d/.test(part)) continue;
    if (/\b(street|road|lane|way|drive|avenue|close|court|place|crescent|terrace|square|house|park|mill|gate|walk|row|precinct|floor|suite|unit|level|room)\b/i.test(part)) continue;
    candidates.push(part);
  }

  if (candidates.length > 0) {
    return titleCase(candidates[0]);
  }

  return '';
}

function writeCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + rows.join('\n') + '\n', 'utf-8');
}

// ── Scraper Core ──────────────────────────────────────────────────────────────

async function initBrowser() {
  const browser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--window-size=1920,1080',
      '--disable-blink-features=AutomationControlled',
    ],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080 });
  return { browser, page };
}

async function warmUpSession(page) {
  log('Warming up session — loading homepage...');
  await page.goto(BASE_URL + '/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  await sleep(3000);
  log('Homepage loaded. Session ready.');
}

async function waitForResults(page) {
  // After form submit, we hit the reCAPTCHA intermediate page which auto-submits.
  // We need to wait for both navigations to complete.
  try {
    await page.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 });
  } catch(e) {}

  let url = page.url();

  // If we're on the search results page (with reCAPTCHA interstitial), wait for auto-submit
  if (url.includes('search/results') && !url.includes('failed-captcha')) {
    try {
      await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });
    } catch(e) {}
  }

  url = page.url();

  if (url.includes('failed-captcha')) {
    const score = url.split('/').pop();
    log(`  reCAPTCHA FAILED (score: ${score})`);
    return false;
  }

  return true;
}

async function parseResultCards(page) {
  return page.evaluate(() => {
    const cards = document.querySelectorAll('.solicitor-outer');
    const results = [];

    for (const card of cards) {
      const data = {};

      // Office ID from section id attribute (e.g., "solicitor31438")
      const sectionId = card.id || '';
      data.office_id = sectionId.replace('solicitor', '');

      // Latitude/longitude
      data.lat = card.getAttribute('data-latitude') || '';
      data.lng = card.getAttribute('data-longitude') || '';

      // Firm name + detail link
      const nameLink = card.querySelector('h2 a');
      data.firm_name = nameLink ? nameLink.textContent.trim() : '';
      data.detail_href = nameLink ? nameLink.getAttribute('href') : '';

      // Parse the details list
      const detailItems = card.querySelectorAll('.details li');
      for (const item of detailItems) {
        const label = item.querySelector('span');
        const labelText = label ? label.textContent.trim().toLowerCase() : '';

        if (labelText.includes('address') || labelText.includes('head office')) {
          // Address is the text content minus the label span
          data.address = item.textContent.replace(label.textContent, '').trim();
        } else if (labelText.includes('website')) {
          const link = item.querySelector('a');
          data.website = link ? (link.getAttribute('href') || link.textContent.trim()) : '';
        } else if (labelText.includes('email')) {
          const emailLink = item.querySelector('a[data-email]');
          if (emailLink) {
            data.email = emailLink.getAttribute('data-email') || '';
          } else {
            const mailLink = item.querySelector('a[href^="mailto:"]');
            if (mailLink) {
              data.email = mailLink.getAttribute('href').replace('mailto:', '');
            }
          }
        } else if (labelText.includes('telephone') || labelText.includes('phone')) {
          data.phone = item.textContent.replace(label.textContent, '').trim();
        }
      }

      // Accreditations
      const accredItems = card.querySelectorAll('.info-panel .initial li');
      data.accreditations = Array.from(accredItems).map(li => li.textContent.trim());

      // People/offices links
      const peopleLink = card.querySelector('a[href*="/organisation/people/"]');
      data.people_href = peopleLink ? peopleLink.getAttribute('href') : '';
      data.people_text = peopleLink ? peopleLink.textContent.trim() : '';

      const officesLink = card.querySelector('a[href*="/organisation/offices/"]');
      data.offices_href = officesLink ? officesLink.getAttribute('href') : '';
      data.offices_text = officesLink ? officesLink.textContent.trim() : '';

      results.push(data);
    }

    return results;
  });
}

async function getPageCount(page) {
  return page.evaluate(() => {
    const links = document.querySelectorAll('a[href*="Page="]');
    let max = 1;
    for (const link of links) {
      const m = link.href.match(/Page=(\d+)/);
      if (m) max = Math.max(max, parseInt(m[1]));
    }
    return max;
  });
}

async function navigateToPage(page, searchUrl, pageNum) {
  const separator = searchUrl.includes('?') ? '&' : '?';
  const url = `${searchUrl}${separator}Page=${pageNum}`;

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });

  // Wait for reCAPTCHA auto-submit
  const passed = await waitForResults(page);
  return passed;
}

// ── Search Functions ──────────────────────────────────────────────────────────

async function runQuickSearch(page, allLeads, seenIds) {
  log('\n=== Quick Search: Immigration and changing countries (LIUIMM) ===');

  // Navigate to homepage first
  await page.goto(BASE_URL + '/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  await sleep(2000);

  // Select immigration category in Quick Search
  await page.select('#Quick_UmbrellaLegalIssue', QUICK_SEARCH_CATEGORY);
  await sleep(500);

  // Submit the form
  await page.evaluate(() => {
    for (const form of document.querySelectorAll('form')) {
      if (form.querySelector('#Quick_Pro')) { form.submit(); break; }
    }
  });

  // Wait for results
  const passed = await waitForResults(page);
  if (!passed) {
    log('Quick Search failed reCAPTCHA. Retrying after delay...');
    await sleep(10000);
    await warmUpSession(page);
    await sleep(3000);

    await page.select('#Quick_UmbrellaLegalIssue', QUICK_SEARCH_CATEGORY);
    await sleep(500);
    await page.evaluate(() => {
      for (const form of document.querySelectorAll('form')) {
        if (form.querySelector('#Quick_Pro')) { form.submit(); break; }
      }
    });

    const retryPassed = await waitForResults(page);
    if (!retryPassed) {
      log('Quick Search failed reCAPTCHA on retry. Skipping.');
      return;
    }
  }

  // Get total pages
  const totalPages = Math.min(await getPageCount(page), maxPages);
  log(`Quick Search: ${totalPages} pages found`);

  // The search URL for pagination
  const searchUrl = `${BASE_URL}/search/results?Pro=False&UmbrellaLegalIssue=${QUICK_SEARCH_CATEGORY}&Location=`;

  // Parse page 1
  const page1Cards = await parseResultCards(page);
  let newCount = 0;
  for (const card of page1Cards) {
    if (card.office_id && seenIds.has(card.office_id)) continue;
    if (card.office_id) seenIds.add(card.office_id);
    allLeads.push(cardToLead(card));
    newCount++;
  }
  log(`  Page 1: ${page1Cards.length} cards, ${newCount} new (total: ${allLeads.length})`);
  writeCSV(allLeads);

  // Paginate
  for (let p = 2; p <= totalPages; p++) {
    await randomDelay();

    const ok = await navigateToPage(page, searchUrl, p);
    if (!ok) {
      log(`  Page ${p}: reCAPTCHA failed. Stopping pagination.`);
      break;
    }

    const cards = await parseResultCards(page);
    if (cards.length === 0) {
      log(`  Page ${p}: No cards. Stopping.`);
      break;
    }

    let pageNew = 0;
    for (const card of cards) {
      if (card.office_id && seenIds.has(card.office_id)) continue;
      if (card.office_id) seenIds.add(card.office_id);
      allLeads.push(cardToLead(card));
      pageNew++;
    }

    log(`  Page ${p}/${totalPages}: ${cards.length} cards, ${pageNew} new (total: ${allLeads.length})`);
    writeCSV(allLeads);
  }
}

async function runProSearch(page, areaCode, areaLabel, allLeads, seenIds) {
  log(`\n=== Pro Search: ${areaLabel} (${areaCode}) ===`);

  // Navigate to homepage
  await page.goto(BASE_URL + '/', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
  await sleep(2000);

  // Click the Pro Search tab
  await page.evaluate(() => {
    const tabs = document.querySelectorAll('#search-panel-tabs a, .nav-tabs a');
    for (const tab of tabs) {
      if (tab.id === 'pro' || tab.textContent.trim().toLowerCase().includes('advanced')) {
        tab.click();
        break;
      }
    }
  });
  await sleep(1000);

  // Select the practice area
  await page.select('#Pro_AreaOfPractice1', areaCode);
  await sleep(500);

  // Submit the pro search form
  await page.evaluate(() => {
    for (const form of document.querySelectorAll('form')) {
      if (form.querySelector('#Pro_Pro')) { form.submit(); break; }
    }
  });

  const passed = await waitForResults(page);
  if (!passed) {
    log(`  Pro Search ${areaCode} failed reCAPTCHA. Retrying...`);
    await sleep(10000);
    await warmUpSession(page);
    await sleep(3000);

    // Retry
    await page.evaluate(() => {
      const tabs = document.querySelectorAll('#search-panel-tabs a');
      for (const tab of tabs) {
        if (tab.id === 'pro') { tab.click(); break; }
      }
    });
    await sleep(1000);
    await page.select('#Pro_AreaOfPractice1', areaCode);
    await sleep(500);
    await page.evaluate(() => {
      for (const form of document.querySelectorAll('form')) {
        if (form.querySelector('#Pro_Pro')) { form.submit(); break; }
      }
    });

    const retryPassed = await waitForResults(page);
    if (!retryPassed) {
      log(`  Pro Search ${areaCode} failed on retry. Skipping.`);
      return;
    }
  }

  const totalPages = Math.min(await getPageCount(page), maxPages);
  log(`  Pro Search ${areaCode}: ${totalPages} pages`);

  const searchUrl = `${BASE_URL}/search/results?Pro=True&AreaOfPractice1=${areaCode}`;

  // Parse page 1
  const page1Cards = await parseResultCards(page);
  let newCount = 0;
  for (const card of page1Cards) {
    if (card.office_id && seenIds.has(card.office_id)) continue;
    if (card.office_id) seenIds.add(card.office_id);
    allLeads.push(cardToLead(card));
    newCount++;
  }
  log(`  Page 1: ${page1Cards.length} cards, ${newCount} new (total: ${allLeads.length})`);
  writeCSV(allLeads);

  // Paginate
  for (let p = 2; p <= totalPages; p++) {
    await randomDelay();

    const ok = await navigateToPage(page, searchUrl, p);
    if (!ok) {
      log(`  Page ${p}: reCAPTCHA failed. Stopping.`);
      break;
    }

    const cards = await parseResultCards(page);
    if (cards.length === 0) {
      log(`  Page ${p}: No cards. Stopping.`);
      break;
    }

    let pageNew = 0;
    for (const card of cards) {
      if (card.office_id && seenIds.has(card.office_id)) continue;
      if (card.office_id) seenIds.add(card.office_id);
      allLeads.push(cardToLead(card));
      pageNew++;
    }

    log(`  Page ${p}/${totalPages}: ${cards.length} cards, ${pageNew} new (total: ${allLeads.length})`);
    writeCSV(allLeads);
  }
}

function cardToLead(card) {
  const firmName = card.firm_name || '';
  const website = card.website || '';
  const address = card.address || '';
  const city = parseCityFromAddress(address);
  const phone = cleanPhone(card.phone || '');
  const email = (card.email || '').toLowerCase().trim();
  const profileUrl = card.detail_href
    ? (card.detail_href.startsWith('http') ? card.detail_href : `${BASE_URL}${card.detail_href}`)
    : '';

  return {
    first_name: '',
    last_name: '',
    firm_name: firmName,
    title: '',
    email: email,
    phone: phone,
    website: website.startsWith('http') ? website : (website ? `https://${website}` : ''),
    domain: extractDomain(website.startsWith('http') ? website : (website ? `https://${website}` : '')),
    city: city,
    state: 'UK-EW',
    country: 'UK',
    niche: 'immigration',
    source: 'law_society_uk',
    profile_url: profileUrl,
  };
}

// ── Detail Page Enrichment (optional) ─────────────────────────────────────────

async function fetchOfficeDetail(page, lead) {
  if (!lead.profile_url) return lead;

  try {
    await page.goto(lead.profile_url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
    const passed = await waitForResults(page);
    if (!passed) return lead;

    const detail = await page.evaluate(() => {
      const data = {};
      const items = document.querySelectorAll('.details li');
      for (const item of items) {
        const label = item.querySelector('span');
        const text = label ? label.textContent.trim().toLowerCase() : '';
        const value = item.textContent.replace(label ? label.textContent : '', '').trim();

        if (text.includes('telephone') && !data.phone) data.phone = value;
        if (text.includes('email')) {
          const emailEl = item.querySelector('a[data-email]');
          if (emailEl) data.email = emailEl.getAttribute('data-email');
        }
        if (text.includes('website') && !data.website) {
          const link = item.querySelector('a');
          data.website = link ? link.getAttribute('href') : '';
        }
      }
      return data;
    });

    // Fill in missing fields
    if (detail.phone && !lead.phone) lead.phone = cleanPhone(detail.phone);
    if (detail.email && !lead.email) lead.email = detail.email.toLowerCase().trim();
    if (detail.website && !lead.website) {
      lead.website = detail.website.startsWith('http') ? detail.website : `https://${detail.website}`;
      lead.domain = extractDomain(lead.website);
    }
  } catch(e) {
    log(`    Detail fetch error: ${e.message}`);
  }

  return lead;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  log('=== UK Law Society Immigration Solicitors Scraper ===');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'} | Max pages: ${maxPages} | Details: ${fetchDetails}`);
  log(`Quick Search: ${!proOnly} | Pro Search: ${!quickOnly}`);

  const { browser, page } = await initBrowser();

  const allLeads = [];
  const seenIds = new Set();

  try {
    // Warm up session
    await warmUpSession(page);

    // Run Quick Search (broader category: "Immigration and changing countries")
    if (!proOnly) {
      await runQuickSearch(page, allLeads, seenIds);
    }

    // Run Pro Search for each specific immigration practice area
    if (!quickOnly) {
      for (const area of PRO_SEARCH_AREAS) {
        await sleep(3000); // Extra delay between searches
        await runProSearch(page, area.code, area.label, allLeads, seenIds);
      }
    }

    // Optional: fetch detail pages for leads missing contact info
    if (fetchDetails) {
      const leadsNeedingDetail = allLeads.filter(l => !l.email && !l.phone);
      log(`\n=== Fetching detail pages for ${leadsNeedingDetail.length} leads without contact info ===`);

      const maxDetailFetches = isTest ? 5 : leadsNeedingDetail.length;
      for (let i = 0; i < Math.min(leadsNeedingDetail.length, maxDetailFetches); i++) {
        await randomDelay();
        const lead = leadsNeedingDetail[i];
        log(`  [${i+1}/${maxDetailFetches}] ${lead.firm_name}...`);
        await fetchOfficeDetail(page, lead);

        if ((i + 1) % 10 === 0) {
          writeCSV(allLeads);
          log(`  Saved checkpoint at ${i+1} details`);
        }
      }
      writeCSV(allLeads);
    }

  } finally {
    await browser.close();
  }

  // Final CSV write
  writeCSV(allLeads);

  // Summary
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const uniqueCities = new Set(allLeads.filter(l => l.city).map(l => l.city)).size;

  log('\n' + '='.repeat(60));
  log('DONE');
  log(`Total leads:     ${allLeads.length}`);
  log(`Unique firms:    ${seenIds.size}`);
  log(`With email:      ${withEmail}`);
  log(`With phone:      ${withPhone}`);
  log(`With website:    ${withWebsite}`);
  log(`Unique cities:   ${uniqueCities}`);
  log(`Output:          ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
