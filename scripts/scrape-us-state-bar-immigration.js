#!/usr/bin/env node
/**
 * US State Bar Certified Immigration Law Specialists Scraper
 *
 * Scrapes board-certified immigration & nationality law specialists from:
 *   1. California State Bar (calbar.ca.gov) — HTTP GET, ~250 specialists
 *   2. Texas Board of Legal Specialization (tbls.org) — Puppeteer (Angular SPA)
 *   3. Florida Bar (floridabar.org) — Puppeteer (Cloudflare protected)
 *
 * Usage:
 *   node scripts/scrape-us-state-bar-immigration.js                # All 3 sources
 *   node scripts/scrape-us-state-bar-immigration.js --source calbar   # California only
 *   node scripts/scrape-us-state-bar-immigration.js --source tbls     # Texas only
 *   node scripts/scrape-us-state-bar-immigration.js --source floridabar # Florida only
 *   node scripts/scrape-us-state-bar-immigration.js --test            # Quick test (5 per source)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ──────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const NAV_TIMEOUT = 30000;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-state-bar.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'bar_number',
];

// ── Helpers ─────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }

function extractDomain(url) {
  if (!url) return '';
  try { return new URL(url.startsWith('http') ? url : `https://${url}`).hostname.replace(/^www\./, ''); }
  catch { return ''; }
}

function csvEscape(val) {
  if (val == null) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(leads, filepath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(l => CSV_COLUMNS.map(c => csvEscape(l[c] || '')).join(','));
  fs.writeFileSync(filepath, [header, ...rows].join('\n'), 'utf8');
}

function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const proto = parsed.protocol === 'https:' ? https : http;
    const opts = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        ...headers,
      },
      timeout: 20000,
    };
    const req = proto.request(opts, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
        return resolve(httpGet(redirect, headers));
      }
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function httpPost(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const proto = parsed.protocol === 'https:' ? https : http;
    const postData = typeof body === 'string' ? body : JSON.stringify(body);
    const opts = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Content-Length': Buffer.byteLength(postData),
        ...headers,
      },
      timeout: 20000,
    };
    const req = proto.request(opts, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postData);
    req.end();
  });
}

function splitName(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: '', last_name: parts[0] };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function titleCase(s) {
  if (!s) return '';
  return s.toLowerCase().replace(/(?:^|\s|[-'])\S/g, c => c.toUpperCase());
}

// ═══════════════════════════════════════════════════════════════════════════
// SOURCE 1: California State Bar (calbar.ca.gov)
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeCalBar(testMode = false) {
  console.log('\n=== CALIFORNIA STATE BAR — Immigration & Nationality Law Specialists ===\n');

  // Step 1: Fetch the search results page (all immigration specialists in one page)
  const searchUrl = 'https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch?' +
    'LastNameOption=b&LastName=&FirstNameOption=b&FirstName=&MiddleNameOption=b&MiddleName=' +
    '&FirmNameOption=b&FirmName=&CityOption=b&City=&State=&Zip=&District=&County=' +
    '&LegalSpecialty=05&LanguageSpoken=&PracticeArea=';

  console.log('  Fetching search results...');
  const { body, statusCode } = await httpGet(searchUrl);
  if (statusCode !== 200) {
    console.log(`  ERROR: Got status ${statusCode}`);
    return [];
  }

  // Step 2: Parse search results to get bar numbers and basic info
  const $ = cheerio.load(body);
  const entries = [];

  $('table#tblAttorney tbody tr').each((_, el) => {
    const $tr = $(el);
    const $link = $tr.find('a[href*="Licensee/Detail"]');
    if (!$link.length) return;

    const href = $link.attr('href') || '';
    const barMatch = href.match(/Detail\/(\d+)/);
    if (!barMatch) return;
    const barNumber = barMatch[1];

    const rawName = $link.text().trim();
    const status = $tr.find('td').eq(1).text().trim();
    const city = $tr.find('td').eq(3).text().trim();

    // Parse name: "LastName, FirstName MiddleName" format
    const nameParts = rawName.split(',').map(s => s.trim());
    let lastName = titleCase(nameParts[0] || '');
    let firstName = '';
    if (nameParts.length > 1) {
      const firstMiddle = nameParts[1].trim().split(/\s+/);
      firstName = titleCase(firstMiddle[0] || '');
    }

    // Only include active attorneys
    if (status.toLowerCase().includes('active')) {
      entries.push({ barNumber, firstName, lastName, city: titleCase(city), status });
    }
  });

  console.log(`  Found ${entries.length} active immigration specialists in search results`);

  if (testMode) {
    entries.splice(5); // Test mode: only first 5
    console.log(`  [TEST MODE] Limited to ${entries.length} entries`);
  }

  // Step 3: Fetch detail pages for phone, email, website, firm
  const leads = [];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const detailUrl = `https://apps.calbar.ca.gov/attorney/Licensee/Detail/${entry.barNumber}`;

    console.log(`  [${i + 1}/${entries.length}] ${entry.firstName} ${entry.lastName} (#${entry.barNumber})...`);

    try {
      await randomDelay();
      const detail = await httpGet(detailUrl);
      if (detail.statusCode !== 200) {
        console.log(`    SKIP: status ${detail.statusCode}`);
        continue;
      }

      const d$ = cheerio.load(detail.body);
      const pageText = detail.body;

      // Extract address line (includes firm name)
      let firmName = '';
      let address = '';
      let state = 'CA';
      const addressMatch = pageText.match(/Address:\s*([^<]+)/);
      if (addressMatch) {
        address = addressMatch[1].replace(/&amp;/g, '&').trim();
        // Address format: "Firm Name, Street, City, State ZIP"
        const addrParts = address.split(',').map(s => s.trim());
        if (addrParts.length >= 4) {
          firmName = addrParts[0];
          // Check state from address
          const stateMatch = addrParts[addrParts.length - 1].match(/([A-Z]{2})\s+\d{5}/);
          if (stateMatch) state = stateMatch[1];
        }
      }

      // Extract phone
      let phone = '';
      const phoneMatch = pageText.match(/Phone:\s*([\d-]+)/);
      if (phoneMatch) phone = phoneMatch[1].trim();

      // Extract email (deobfuscation)
      // CalBar uses 20 spans (e0-e19) with CSS display:none on all EXCEPT the real one.
      // The span with display:inline contains the REAL email as displayed text.
      // The displayed text uses HTML entities: &#64; for @ and <span>&#46;</span> for .
      let email = '';
      const cssMatch = pageText.match(/#(e\d+)\{display:inline;\}/);
      if (cssMatch) {
        const visibleId = cssMatch[1]; // e.g., "e5"
        // Find the visible span's content
        // Pattern: <span id="e5" href="...">displayed&#64;text<span>&#46;</span>com</span>
        const spanRegex = new RegExp(
          `<span id="${visibleId}"[^>]*>([\\s\\S]*?)<\\/span>(?=<span id="e|$)`,
          'i'
        );
        const spanMatch = pageText.match(spanRegex);
        if (spanMatch) {
          email = spanMatch[1]
            .replace(/<span>/g, '').replace(/<\/span>/g, '')
            .replace(/&#64;/g, '@').replace(/&#46;/g, '.')
            .replace(/\s+/g, '')
            .trim();
        }
      }

      // Extract website
      let website = '';
      const websiteMatch = pageText.match(/var memberWebsite\s*=\s*'([^']+)'/);
      if (websiteMatch) website = websiteMatch[1].trim();

      const lead = {
        first_name: entry.firstName,
        last_name: entry.lastName,
        firm_name: firmName,
        title: 'Board Certified Specialist - Immigration & Nationality Law',
        email,
        phone,
        website,
        domain: extractDomain(website),
        city: entry.city,
        state,
        country: 'US',
        niche: 'Immigration Law',
        source: 'calbar',
        profile_url: detailUrl,
        bar_number: entry.barNumber,
      };

      leads.push(lead);
      const info = [email, phone, website].filter(Boolean).join(', ');
      console.log(`    OK: ${info || 'basic info only'}`);
    } catch (err) {
      console.log(`    ERROR: ${err.message}`);
    }
  }

  console.log(`\n  CalBar complete: ${leads.length} leads (${leads.filter(l => l.email).length} with email, ${leads.filter(l => l.phone).length} with phone)`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════
// SOURCE 2: Texas Board of Legal Specialization (tbls.org)
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeTBLS(testMode = false) {
  console.log('\n=== TEXAS BOARD OF LEGAL SPECIALIZATION — Immigration & Nationality Law ===\n');

  const leads = [];

  // TBLS has a public API at api.tbls.org
  // Step 1: Search for all Immigration & Nationality Law certified attorneys
  console.log('  Searching TBLS API for Immigration certified attorneys...');

  const searchUrl = 'https://api.tbls.org/api/repo/findlawyer';
  const searchBody = JSON.stringify({
    areaId: 'IM',
    firstName: '',
    lastName: '',
    city: '',
    zip: '',
    countyNum: 0,
  });

  let attorneys = [];
  try {
    const searchResult = await httpPost(searchUrl, searchBody, {
      'Content-Type': 'application/json',
      'Accept': 'application/json, text/plain, */*',
      'Origin': 'https://www.tbls.org',
      'Referer': 'https://www.tbls.org/findlawyer',
    });

    const data = JSON.parse(searchResult.body);
    if (data.failed) {
      console.log(`  API error: ${data.failureMessage}`);
      return [];
    }
    attorneys = data.data || [];
    console.log(`  Found ${attorneys.length} board-certified immigration specialists`);
  } catch (err) {
    console.log(`  Search API failed: ${err.message}`);
    return [];
  }

  if (testMode) {
    attorneys = attorneys.slice(0, 5);
    console.log(`  [TEST MODE] Limited to ${attorneys.length} entries`);
  }

  // Step 2: Fetch profile/bubble data for each attorney (phone, website, address)
  for (let i = 0; i < attorneys.length; i++) {
    const att = attorneys[i];
    const mid = att.id;
    console.log(`  [${i + 1}/${attorneys.length}] ${att.firstName || ''} ${att.lastName || ''} (${att.firmName || ''})...`);

    let phone = '', website = '', city = '', state = 'TX', barNumber = '', barUrl = '';

    try {
      await randomDelay();
      const bubbleUrl = `https://api.tbls.org/api/repo/findlawyerbubbledata/${mid}`;
      const bubbleResult = await httpGet(bubbleUrl, {
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://www.tbls.org',
        'Referer': 'https://www.tbls.org/findlawyer',
      });

      const bubble = JSON.parse(bubbleResult.body);
      if (!bubble.failed && bubble.data) {
        const d = bubble.data;
        phone = d.phone || '';
        website = d.website || '';
        city = d.city || '';
        state = d.state || 'TX';
        barNumber = String(d.barId || '');
        barUrl = d.barUrl || '';
      }
    } catch (err) {
      console.log(`    Profile fetch failed: ${err.message}`);
    }

    // Clean up name (remove Mr./Ms./Mrs. prefixes)
    let firstName = (att.firstName || '').replace(/^(Mr\.|Ms\.|Mrs\.|Dr\.)\s*/i, '').trim();
    let lastName = (att.lastName || '').trim();

    // If firstName has multiple parts, keep just the first
    const firstParts = firstName.split(/\s+/);
    if (firstParts.length > 1) {
      firstName = firstParts[0];
    }

    const lead = {
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      firm_name: att.firmName || '',
      title: 'Board Certified - Immigration & Nationality Law',
      email: '', // TBLS encrypts emails
      phone,
      website,
      domain: extractDomain(website),
      city: titleCase(city),
      state,
      country: 'US',
      niche: 'Immigration Law',
      source: 'tbls',
      profile_url: barUrl || `https://www.tbls.org/findlawyer/${mid}`,
      bar_number: barNumber,
    };

    leads.push(lead);
    const info = [phone, website].filter(Boolean).join(', ');
    console.log(`    OK: ${info || 'basic info only'}`);
  }

  console.log(`\n  TBLS complete: ${leads.length} leads (${leads.filter(l => l.email).length} with email, ${leads.filter(l => l.phone).length} with phone, ${leads.filter(l => l.website).length} with website)`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════
// SOURCE 3: Florida Bar (floridabar.org)
// ═══════════════════════════════════════════════════════════════════════════

async function scrapeFloridaBar(testMode = false) {
  console.log('\n=== FLORIDA BAR — Immigration & Nationality Law Certification ===\n');

  let puppeteerExtra, StealthPlugin;
  try {
    puppeteerExtra = require('puppeteer-extra');
    StealthPlugin = require('puppeteer-extra-plugin-stealth');
    puppeteerExtra.use(StealthPlugin());
  } catch {
    try { puppeteerExtra = require('puppeteer'); }
    catch { console.log('  ERROR: puppeteer not installed'); return []; }
  }

  const browser = await puppeteerExtra.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
           '--disable-blink-features=AutomationControlled'],
  });

  const leads = [];

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });

    // Step 1: Warm up — visit homepage first to get cookies
    console.log('  Warming up with Florida Bar homepage...');
    try {
      await page.goto('https://www.floridabar.org/', {
        waitUntil: 'networkidle2',
        timeout: 30000,
      });
      await sleep(5000);
    } catch (e) {
      console.log(`  Homepage load: ${e.message}`);
      await sleep(5000);
    }

    // Check if Cloudflare challenge passed
    let cfPassed = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      const title = await page.title();
      console.log(`  Page title (attempt ${attempt + 1}): "${title}"`);

      if (!title.includes('Cloudflare') && !title.includes('Rate Limit') &&
          !title.includes('Attention Required')) {
        cfPassed = true;
        break;
      }
      console.log('  Waiting for Cloudflare to clear...');
      await sleep(10000);
    }

    if (!cfPassed) {
      console.log('  WARNING: Could not pass Cloudflare protection.');
      console.log('  The Florida Bar website is actively blocking automated access.');
      console.log('  Tip: Try running again later, or use a residential IP / VPN.');
      await browser.close();
      return [];
    }

    // Step 2: Navigate to Find a Lawyer with immigration practice area
    // Florida Bar board certification codes: IM = Immigration & Nationality Law
    let pageNum = 1;
    const pageSize = 50;
    let totalFound = 0;
    let rateLimitRetries = 0;

    while (true) {
      const searchUrl = `https://www.floridabar.org/directories/find-mbr/?locType=S&locValue=FL&eligible=Y&pageNumber=${pageNum}&pageSize=${pageSize}&pracAreas=I01`;
      console.log(`  Loading page ${pageNum}...`);

      try {
        await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 30000 });
        await sleep(3000);
      } catch (e) {
        console.log(`  Page ${pageNum} navigation: ${e.message}`);
        break;
      }

      // Check for rate limit / Cloudflare
      const pageTitle = await page.title();
      if (pageTitle.includes('Rate Limit') || pageTitle.includes('Cloudflare') || pageTitle.includes('Attention')) {
        rateLimitRetries = (rateLimitRetries || 0) + 1;
        if (rateLimitRetries > 3) {
          console.log('  Rate limited too many times. The Florida Bar is blocking automated access.');
          console.log('  This source requires manual access or a different IP address.');
          break;
        }
        console.log(`  Rate limited (attempt ${rateLimitRetries}/3). Waiting 60s...`);
        await sleep(60000);
        continue;
      }
      rateLimitRetries = 0; // Reset on success

      // Parse results
      const attorneys = await page.evaluate(() => {
        const results = [];
        document.querySelectorAll('li.profile-compact').forEach(el => {
          const nameEl = el.querySelector('p.profile-name a, h4.profile-name a, .profile-name a');
          if (!nameEl) return;
          const name = nameEl.textContent.trim();
          const profileUrl = nameEl.href || '';
          const barMatch = profileUrl.match(/num=(\d+)/);

          // Contact info
          const contactBlock = el.querySelector('.profile-contact');
          const contactPs = contactBlock ? contactBlock.querySelectorAll('p') : [];

          let firm = '', city = '', state = 'FL', phone = '', email = '';

          // First p = firm + address
          if (contactPs.length > 0) {
            const lines = contactPs[0].innerHTML.split('<br>').map(s =>
              s.replace(/<[^>]+>/g, '').trim()
            ).filter(Boolean);
            if (lines.length >= 1) firm = lines[0];
            // City/state from last address line
            for (const line of lines) {
              const csz = line.match(/^([^,]+),\s*([A-Z]{2})\s+\d{5}/);
              if (csz) { city = csz[1]; state = csz[2]; }
            }
          }

          // Phone
          if (contactPs.length > 1) {
            const phoneMatch = contactPs[1].textContent.match(/([\d()-]+\s*[\d()-]+)/);
            if (phoneMatch) phone = phoneMatch[1].trim();
          }

          // Email (Cloudflare obfuscated)
          const emailEl = el.querySelector('a[href^="mailto:"]');
          if (emailEl) email = emailEl.href.replace('mailto:', '');
          // Also check for cf-protected email
          const cfEmail = el.querySelector('[data-cfemail]');
          if (cfEmail) {
            const encoded = cfEmail.getAttribute('data-cfemail');
            // Decode CF email
            const decoded = [];
            const key = parseInt(encoded.substring(0, 2), 16);
            for (let i = 2; i < encoded.length; i += 2) {
              decoded.push(String.fromCharCode(parseInt(encoded.substring(i, i + 2), 16) ^ key));
            }
            email = decoded.join('');
          }

          results.push({ name, profileUrl, barNumber: barMatch?.[1] || '', city, state, phone, firm, email });
        });
        return results;
      });

      if (attorneys.length === 0) {
        console.log(`  No results on page ${pageNum}. Done.`);
        break;
      }

      totalFound += attorneys.length;
      console.log(`  Found ${attorneys.length} on page ${pageNum} (total: ${totalFound})`);

      for (const att of attorneys) {
        const nameParts = att.name.split(/,\s*/);
        let lastName = nameParts[0] || '';
        let firstName = (nameParts[1] || '').split(/\s+/)[0] || '';

        leads.push({
          first_name: titleCase(firstName),
          last_name: titleCase(lastName),
          firm_name: att.firm || '',
          title: 'Board Certified - Immigration & Nationality Law',
          email: att.email || '',
          phone: att.phone || '',
          website: '',
          domain: '',
          city: titleCase(att.city || ''),
          state: att.state || 'FL',
          country: 'US',
          niche: 'Immigration Law',
          source: 'floridabar',
          profile_url: att.profileUrl || '',
          bar_number: att.barNumber || '',
        });
      }

      if (testMode || attorneys.length < pageSize) break;
      pageNum++;
      await randomDelay();
    }
  } catch (err) {
    console.log(`  Florida Bar error: ${err.message}`);
  } finally {
    await browser.close();
  }

  console.log(`\n  Florida Bar complete: ${leads.length} leads (${leads.filter(l => l.email).length} with email, ${leads.filter(l => l.phone).length} with phone)`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const testMode = args.includes('--test');
  const sourceArg = args.find(a => a.startsWith('--source='))?.split('=')[1]
    || (args.includes('--source') ? args[args.indexOf('--source') + 1] : null);

  console.log('============================================================');
  console.log(' US State Bar Certified Immigration Law Specialists Scraper');
  console.log('============================================================');
  if (testMode) console.log(' [TEST MODE — limited results]');
  if (sourceArg) console.log(` [SOURCE FILTER: ${sourceArg}]`);
  console.log('');

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  let allLeads = [];

  // Source 1: CalBar
  if (!sourceArg || sourceArg === 'calbar') {
    try {
      const calbarLeads = await scrapeCalBar(testMode);
      allLeads.push(...calbarLeads);
    } catch (err) {
      console.log(`\nCalBar failed: ${err.message}`);
    }
  }

  // Source 2: TBLS
  if (!sourceArg || sourceArg === 'tbls') {
    try {
      const tblsLeads = await scrapeTBLS(testMode);
      allLeads.push(...tblsLeads);
    } catch (err) {
      console.log(`\nTBLS failed: ${err.message}`);
    }
  }

  // Source 3: Florida Bar
  if (!sourceArg || sourceArg === 'floridabar') {
    try {
      const flLeads = await scrapeFloridaBar(testMode);
      allLeads.push(...flLeads);
    } catch (err) {
      console.log(`\nFlorida Bar failed: ${err.message}`);
    }
  }

  // Dedup by bar_number + source
  const seen = new Set();
  allLeads = allLeads.filter(l => {
    const key = `${l.source}:${l.bar_number || l.first_name + l.last_name + l.city}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Write CSV
  writeCsv(allLeads, OUTPUT_FILE);

  // Summary
  console.log('\n============================================================');
  console.log(' SUMMARY');
  console.log('============================================================');
  console.log(`  Total leads:     ${allLeads.length}`);
  console.log(`  With email:      ${allLeads.filter(l => l.email).length}`);
  console.log(`  With phone:      ${allLeads.filter(l => l.phone).length}`);
  console.log(`  With website:    ${allLeads.filter(l => l.website).length}`);
  console.log('');
  console.log('  By source:');
  const bySrc = {};
  allLeads.forEach(l => { bySrc[l.source] = (bySrc[l.source] || 0) + 1; });
  Object.entries(bySrc).sort((a, b) => b[1] - a[1]).forEach(([src, cnt]) => {
    console.log(`    ${src}: ${cnt}`);
  });
  console.log('');
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log('============================================================\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
