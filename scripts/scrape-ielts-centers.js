#!/usr/bin/env node
/**
 * IELTS / TOEFL Test Prep & English Language School Scraper
 *
 * Scrapes 3 high-value directories of English language schools,
 * IELTS/TOEFL prep centers, and education agencies worldwide.
 *
 * Sources:
 *   1. English UK Member Directory (englishuk.com/member-directory)
 *      - 278 British Council-accredited language schools in the UK
 *      - Fields: name, email, phone, website, address, city
 *      - Static HTML, paginated (20/page, 14 pages)
 *
 *   2. English UK Partner Agency Directory (englishuk.com/partner-agency-directory)
 *      - 249 international education agencies (study abroad consultants)
 *      - Fields: name, email, phone, website, address, city, country
 *      - Static HTML, paginated (20/page, 13 pages)
 *
 *   3. Languages Canada Directory (languagescanada.ca)
 *      - ~225 accredited ESL/FSL schools across Canada
 *      - 2-step: listing index -> individual detail pages for contact info
 *      - Fields: name, email, phone, website, address, city, province
 *
 * Research Notes (non-viable sources):
 *   - ielts.org: 4000+ centers but Angular SPA, no HTTP API discovered
 *   - ielts.idp.com: Angular SPA, dynamic JSON embedded in page
 *   - maprequest.ets.org (TOEFL): No exposed API, JS-driven
 *   - NEAS Australia: 404 on directory pages
 *   - IALC: 403 Forbidden
 *   - Oxford Seminars (66K schools): PHP errors, broken
 *   - CEA Accredit: JS-driven interface, no visible data
 *   - ESLbase: WordPress Grid Builder, 6000 schools but needs JS
 *
 * Output: output/ielts-test-prep-centers.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Usage:
 *   node scripts/scrape-ielts-centers.js
 *   node scripts/scrape-ielts-centers.js --test          # First 2 pages per source
 *   node scripts/scrape-ielts-centers.js --source=englishuk  # Only English UK schools
 *   node scripts/scrape-ielts-centers.js --source=agencies   # Only English UK agencies
 *   node scripts/scrape-ielts-centers.js --source=canada     # Only Languages Canada
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const m = arg.replace(/^--/, '').match(/^([^=]+)(?:=(.*))?$/);
  if (m) args[m[1]] = m[2] || true;
}

const TEST_MODE   = !!args.test;
const SOURCE_ONLY = args.source || ''; // 'englishuk', 'agencies', 'canada', or '' for all
const DELAY_MS    = parseInt(args.delay) || 1500;
const OUTPUT_DIR  = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'ielts-test-prep-centers.csv');

// ── CSV columns (requested format) ─────────────────────────────────────
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  totalLeads: 0,
  withEmail: 0,
  withPhone: 0,
  withWebsite: 0,
  bySource: {},
  byCountry: {},
  startTime: Date.now(),
};

// ── Helpers ─────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function elapsed() {
  const s = Math.floor((Date.now() - stats.startTime) / 1000);
  return `${Math.floor(s / 60)}m${s % 60}s`;
}

function escapeCSV(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/**
 * HTTP GET with redirect following, User-Agent spoofing, and timeout.
 * Works for both http and https URLs.
 */
function fetchUrl(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    if (maxRedirects <= 0) return reject(new Error('Too many redirects'));

    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const parsed = new URL(url);
          redirectUrl = `${parsed.protocol}//${parsed.host}${redirectUrl}`;
        }
        fetchUrl(redirectUrl, maxRedirects - 1).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`)));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Request timeout')); });
  });
}

/**
 * Minimal HTML entity decoder for common entities.
 */
function decodeEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)));
}

/**
 * Strip HTML tags and decode entities.
 */
function stripHTML(str) {
  if (!str) return '';
  return decodeEntities(str.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
}

/**
 * Extract email from mailto: link or raw text.
 */
function extractEmail(html) {
  if (!html) return '';
  // mailto: link
  const mailto = html.match(/href=["']mailto:([^"'?]+)/i);
  if (mailto) return mailto[1].trim().toLowerCase();
  // Raw email pattern
  const raw = html.match(/([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/);
  if (raw) return raw[1].trim().toLowerCase();
  return '';
}

/**
 * Extract phone number from text (after "Tel" prefix).
 */
function extractPhone(text) {
  if (!text) return '';
  // Look for phone after "Tel" or "Telephone" prefix
  const telMatch = text.match(/Tel(?:ephone)?[:\s]*([+\d\s\-().]+)/i);
  if (telMatch) return telMatch[1].replace(/[\s()]/g, '').trim();
  // Fallback: any international phone pattern
  const phoneMatch = text.match(/(\+?[\d\s\-().]{7,20})/);
  if (phoneMatch) return phoneMatch[1].replace(/[\s()]/g, '').trim();
  return '';
}

/**
 * Extract website URL from href attribute.
 */
function extractWebsite(html) {
  if (!html) return '';
  const href = html.match(/href=["'](https?:\/\/[^"']+)/i);
  if (href) return href[1].trim();
  // Look for www. pattern
  const www = html.match(/((?:https?:\/\/)?(?:www\.)?[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^\s<"']*)/i);
  if (www && !www[1].includes('mailto:') && !www[1].includes('englishuk.com')) return www[1].trim();
  return '';
}

/**
 * Try to split a company/school name into person name + company.
 * Most entries are company names, so we return empty first/last by default.
 */
function splitContactName(name) {
  // These are almost always company/school names, not person names
  return { first_name: '', last_name: '', company: name.trim() };
}

/**
 * Parse address string into city, state/region, country components.
 */
function parseAddress(address, defaultCountry) {
  if (!address) return { city: '', state: '', country: defaultCountry };

  const parts = address.split(',').map(p => p.trim()).filter(Boolean);
  if (parts.length === 0) return { city: '', state: '', country: defaultCountry };

  // For UK addresses: typically "Street, City, Region, POSTCODE"
  // For international: "Street, City, Postal, Country"
  let city = '';
  let state = '';
  let country = defaultCountry;

  if (parts.length >= 2) {
    // Last part is often country or postcode
    const lastPart = parts[parts.length - 1].trim();

    // Check if last part is a known country name
    const knownCountries = [
      'United Kingdom', 'UK', 'England', 'Scotland', 'Wales', 'Northern Ireland',
      'Italy', 'Turkey', 'Georgia', 'Venezuela', 'Colombia', 'Brazil', 'Mexico',
      'India', 'China', 'Japan', 'South Korea', 'Thailand', 'Vietnam', 'Indonesia',
      'Malaysia', 'Philippines', 'Taiwan', 'Pakistan', 'Bangladesh', 'Nepal',
      'Sri Lanka', 'Saudi Arabia', 'UAE', 'United Arab Emirates', 'Oman', 'Kuwait',
      'Qatar', 'Bahrain', 'Jordan', 'Egypt', 'Morocco', 'Tunisia', 'Algeria',
      'Nigeria', 'Ghana', 'Kenya', 'South Africa', 'Russia', 'Ukraine', 'Poland',
      'Czech Republic', 'Hungary', 'Romania', 'Bulgaria', 'Greece', 'Cyprus',
      'Spain', 'Portugal', 'France', 'Germany', 'Austria', 'Switzerland', 'Belgium',
      'Netherlands', 'Denmark', 'Sweden', 'Norway', 'Finland', 'Ireland',
      'Canada', 'United States', 'USA', 'Australia', 'New Zealand',
      'Argentina', 'Chile', 'Peru', 'Ecuador', 'Paraguay', 'Uruguay',
      'Iran', 'Iraq', 'Lebanon', 'Libya', 'Myanmar', 'Cambodia', 'Laos',
      'Kazakhstan', 'Uzbekistan', 'Azerbaijan', 'Armenia', 'Belarus',
    ];

    if (knownCountries.some(c => lastPart.toLowerCase() === c.toLowerCase())) {
      country = lastPart;
      // City is typically 2nd to last, or further back
      if (parts.length >= 3) {
        // Check if second-to-last is a postcode
        const secondLast = parts[parts.length - 2].trim();
        if (/^[A-Z0-9]{2,4}\s*[A-Z0-9]{2,4}$/i.test(secondLast) || /^\d{4,6}$/.test(secondLast)) {
          // It's a postcode, city is further back
          city = parts.length >= 4 ? parts[parts.length - 3].trim() : '';
          state = parts.length >= 5 ? parts[parts.length - 4].trim() : '';
        } else {
          city = secondLast;
          state = parts.length >= 4 ? parts[parts.length - 3].trim() : '';
        }
      }
    } else if (/^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i.test(lastPart)) {
      // UK postcode pattern - city is before it
      if (parts.length >= 3) {
        city = parts[parts.length - 2].trim();
        state = parts.length >= 4 ? parts[parts.length - 3].trim() : '';
      }
    } else {
      // Assume second part is city for simple "street, city" formats
      city = parts.length >= 2 ? parts[1].trim() : '';
      state = parts.length >= 3 ? parts[2].trim() : '';
    }
  }

  return { city, state, country };
}


// ═══════════════════════════════════════════════════════════════════════
// SOURCE 1: English UK Member Directory (278 language schools)
// ═══════════════════════════════════════════════════════════════════════

async function scrapeEnglishUKMembers() {
  console.log('\n━━━ Source 1: English UK Member Directory ━━━');
  console.log('  URL: https://www.englishuk.com/member-directory');
  console.log('  Expected: ~278 accredited English language schools in the UK');

  const leads = [];
  const maxPages = TEST_MODE ? 2 : 20; // 14 pages in practice, 20 as safety margin

  for (let page = 0; page < maxPages; page++) {
    const url = `https://www.englishuk.com/member-directory?letter_index=&keywords=&region_hq=0&page=${page}`;

    let html;
    try {
      html = await fetchUrl(url);
    } catch (err) {
      console.log(`  [WARN] Page ${page} failed: ${err.message}`);
      if (page > 0) break; // First page failure is fatal, later pages might just be end
      throw err;
    }

    // Check if we've gone past the last page (no results)
    if (!html.includes('member-directory') && !html.includes('read more')) {
      console.log(`  Page ${page}: no more results, stopping.`);
      break;
    }

    // Parse school listings from HTML
    // Each listing follows the pattern:
    //   <h3><a href="/member-directory?id=XXXX">School Name</a></h3>
    //   <p>Address<br>Tel PHONE<br>
    //   <a href="URL">website</a><br>
    //   <a href="mailto:EMAIL">EMAIL</a><br>
    //   <a href="/member-directory?id=XXXX">read more</a></p>
    //
    // We use regex since we don't want to add cheerio as a dependency for standalone scripts.

    // Split by school heading pattern
    const schoolBlocks = html.split(/<h3[^>]*>\s*<a\s+href=["']\/member-directory\?id=/i);

    let pageCount = 0;
    for (let i = 1; i < schoolBlocks.length; i++) { // Skip first block (before first <h3>)
      const block = schoolBlocks[i];

      // Extract school name
      const nameMatch = block.match(/^[^"]*["'][^>]*>([^<]+)<\/a>/i);
      const schoolName = nameMatch ? decodeEntities(nameMatch[1].trim()) : '';
      if (!schoolName) continue;

      // Extract the paragraph block after the heading
      const pBlock = block.match(/<\/h3>\s*<p[^>]*>([\s\S]*?)<\/p>/i);
      const content = pBlock ? pBlock[1] : block;

      // Extract email
      const email = extractEmail(content);

      // Extract phone
      const phone = extractPhone(stripHTML(content));

      // Extract website (first non-englishuk, non-mailto href)
      let website = '';
      const hrefMatches = content.match(/href=["'](https?:\/\/[^"']+)/gi) || [];
      for (const href of hrefMatches) {
        const urlMatch = href.match(/href=["'](https?:\/\/[^"']+)/i);
        if (urlMatch) {
          const u = urlMatch[1];
          if (!u.includes('englishuk.com') && !u.includes('mailto:')) {
            website = u;
            break;
          }
        }
      }

      // Extract address (text before "Tel" or before first <a>)
      const textContent = stripHTML(content);
      let address = '';
      const telIdx = textContent.indexOf('Tel');
      if (telIdx > 0) {
        address = textContent.substring(0, telIdx).trim();
      }
      // Clean trailing "read more" and similar
      address = address.replace(/read more\s*$/i, '').trim();

      const { city, state, country } = parseAddress(address, 'United Kingdom');
      const { first_name, last_name, company } = splitContactName(schoolName);

      const lead = {
        email: email,
        first_name: first_name,
        last_name: last_name,
        company: company,
        phone: phone,
        city: city,
        state: state,
        country: country,
        source: 'englishuk-member',
        website: website,
      };

      leads.push(lead);
      pageCount++;

      if (email) stats.withEmail++;
      if (phone) stats.withPhone++;
      if (website) stats.withWebsite++;
    }

    console.log(`  Page ${page + 1}: ${pageCount} schools parsed (${leads.length} total)`);

    if (pageCount === 0) {
      console.log(`  No schools found on page ${page + 1}, stopping.`);
      break;
    }

    await sleep(DELAY_MS);
  }

  console.log(`  [DONE] ${leads.length} English UK member schools scraped`);
  stats.bySource['englishuk-member'] = leads.length;
  return leads;
}


// ═══════════════════════════════════════════════════════════════════════
// SOURCE 2: English UK Partner Agency Directory (249 agencies)
// ═══════════════════════════════════════════════════════════════════════

async function scrapeEnglishUKAgencies() {
  console.log('\n━━━ Source 2: English UK Partner Agency Directory ━━━');
  console.log('  URL: https://www.englishuk.com/partner-agency-directory');
  console.log('  Expected: ~249 international education agencies');

  const leads = [];
  const maxPages = TEST_MODE ? 2 : 20; // 13 pages in practice

  for (let page = 0; page < maxPages; page++) {
    const url = `https://www.englishuk.com/partner-agency-directory?letter_index=&keywords=&page=${page}`;

    let html;
    try {
      html = await fetchUrl(url);
    } catch (err) {
      console.log(`  [WARN] Page ${page} failed: ${err.message}`);
      if (page > 0) break;
      throw err;
    }

    if (!html.includes('partner-agency-directory') && !html.includes('read more')) {
      console.log(`  Page ${page}: no more results, stopping.`);
      break;
    }

    // Partner agencies use the same HTML structure as member directory:
    //   <h3><a href="/partner-agency-directory?id=XXXX">Agency Name</a></h3>
    //   <p>Address<br>Tel PHONE<br>
    //   <a href="URL">website</a><br>
    //   <a href="mailto:EMAIL">EMAIL</a><br>
    //   <a href="/partner-agency-directory?id=XXXX">read more</a></p>
    const agencyBlocks = html.split(/<h3[^>]*>\s*<a\s+href=["']\/partner-agency-directory\?id=/i);

    let pageCount = 0;
    for (let i = 1; i < agencyBlocks.length; i++) {
      const block = agencyBlocks[i];

      const nameMatch = block.match(/^[^"]*["'][^>]*>([^<]+)<\/a>/i);
      const agencyName = nameMatch ? decodeEntities(nameMatch[1].trim()) : '';
      if (!agencyName) continue;

      const pBlock = block.match(/<\/h3>\s*<p[^>]*>([\s\S]*?)<\/p>/i);
      const content = pBlock ? pBlock[1] : block;

      const email = extractEmail(content);
      const phone = extractPhone(stripHTML(content));

      let website = '';
      const hrefMatches = content.match(/href=["'](https?:\/\/[^"']+)/gi) || [];
      for (const href of hrefMatches) {
        const urlMatch = href.match(/href=["'](https?:\/\/[^"']+)/i);
        if (urlMatch) {
          const u = urlMatch[1];
          if (!u.includes('englishuk.com') && !u.includes('mailto:')) {
            website = u;
            break;
          }
        }
      }

      // Address parsing - agencies include country in address
      const textContent = stripHTML(content);
      let address = '';
      const telIdx = textContent.indexOf('Tel');
      if (telIdx > 0) {
        address = textContent.substring(0, telIdx).trim();
      } else {
        // No phone - address is everything before website/email text
        address = textContent.replace(/read more\s*$/i, '').trim();
        // Remove website/email from address text
        if (website) {
          const domain = website.replace(/^https?:\/\//, '').replace(/\/$/, '');
          address = address.replace(domain, '').trim();
        }
        if (email) {
          address = address.replace(email, '').trim();
        }
      }
      address = address.replace(/read more\s*$/i, '').replace(/\s+/g, ' ').trim();

      const { city, state, country } = parseAddress(address, 'Unknown');
      const { first_name, last_name, company } = splitContactName(agencyName);

      const lead = {
        email: email,
        first_name: first_name,
        last_name: last_name,
        company: company,
        phone: phone,
        city: city,
        state: state,
        country: country,
        source: 'englishuk-agency',
        website: website,
      };

      leads.push(lead);
      pageCount++;

      if (email) stats.withEmail++;
      if (phone) stats.withPhone++;
      if (website) stats.withWebsite++;
    }

    console.log(`  Page ${page + 1}: ${pageCount} agencies parsed (${leads.length} total)`);

    if (pageCount === 0) {
      console.log(`  No agencies found on page ${page + 1}, stopping.`);
      break;
    }

    await sleep(DELAY_MS);
  }

  console.log(`  [DONE] ${leads.length} English UK partner agencies scraped`);
  stats.bySource['englishuk-agency'] = leads.length;
  return leads;
}


// ═══════════════════════════════════════════════════════════════════════
// SOURCE 3: Languages Canada Directory (~225 accredited ESL schools)
// ═══════════════════════════════════════════════════════════════════════

async function scrapeLanguagesCanada() {
  console.log('\n━━━ Source 3: Languages Canada Directory ━━━');
  console.log('  URL: https://www.languagescanada.ca/en/students-new');
  console.log('  Expected: ~225 accredited ESL/FSL schools across Canada');
  console.log('  Method: 2-step (listing index -> detail pages for contact info)');

  const leads = [];

  // Step 1: Collect all school listing URLs from the paginated index
  console.log('  [Step 1] Collecting school listing URLs...');
  const schoolUrls = [];
  const maxPages = TEST_MODE ? 2 : 25; // 19 pages observed

  for (let page = 1; page <= maxPages; page++) {
    const url = `https://www.languagescanada.ca/en/students-new?page=${page}`;

    let html;
    try {
      html = await fetchUrl(url);
    } catch (err) {
      console.log(`  [WARN] Index page ${page} failed: ${err.message}`);
      if (page > 1) break;
      throw err;
    }

    // Extract school detail page URLs
    // Pattern: /en/listing-directory/students/SCHOOL-SLUG
    const urlMatches = html.match(/href=["'](\/en\/listing-directory\/students\/[^"']+)/gi) || [];
    let pageCount = 0;

    for (const match of urlMatches) {
      const urlMatch = match.match(/href=["'](\/en\/listing-directory\/students\/[^"']+)/i);
      if (urlMatch) {
        const schoolPath = urlMatch[1];
        // Skip duplicate URLs and anchor links
        if (!schoolUrls.includes(schoolPath) && !schoolPath.includes('#')) {
          schoolUrls.push(schoolPath);
          pageCount++;
        }
      }
    }

    console.log(`    Index page ${page}: ${pageCount} new school URLs (${schoolUrls.length} total)`);

    if (pageCount === 0 && page > 1) {
      console.log(`    No new URLs on page ${page}, stopping index scan.`);
      break;
    }

    await sleep(DELAY_MS);
  }

  console.log(`  [Step 1 done] ${schoolUrls.length} school URLs collected`);

  // Step 2: Fetch each school's detail page for contact info
  console.log('  [Step 2] Fetching individual school detail pages...');
  let fetched = 0;
  let errors = 0;

  for (const schoolPath of schoolUrls) {
    const detailUrl = `https://www.languagescanada.ca${schoolPath}`;
    fetched++;

    let html;
    try {
      html = await fetchUrl(detailUrl);
    } catch (err) {
      errors++;
      if (errors <= 3) console.log(`    [WARN] Failed: ${schoolPath} - ${err.message}`);
      if (errors === 4) console.log(`    [WARN] Suppressing further error messages...`);
      continue;
    }

    // Extract school name from <h1> or <title>
    let schoolName = '';
    const h1Match = html.match(/<h1[^>]*>([^<]+)<\/h1>/i);
    if (h1Match) {
      schoolName = decodeEntities(h1Match[1].trim());
    } else {
      const titleMatch = html.match(/<title>([^<|]+)/i);
      if (titleMatch) schoolName = decodeEntities(titleMatch[1].trim());
    }
    // Clean up school name (remove " | Languages Canada" suffix)
    schoolName = schoolName.replace(/\s*\|\s*Languages Canada.*$/i, '').trim();
    if (!schoolName) {
      // Derive from URL slug
      const slug = schoolPath.split('/').pop();
      schoolName = slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }

    // Extract email
    const email = extractEmail(html);

    // Extract phone - look for phone patterns in the page
    let phone = '';
    // Languages Canada detail pages show phone as plain text, often with "1 XXX XXX-XXXX" format
    const phonePatterns = html.match(/(?:Phone|Tel|Telephone)[:\s]*([+\d\s\-().]+)/gi) || [];
    if (phonePatterns.length > 0) {
      const phoneClean = phonePatterns[0].replace(/^(?:Phone|Tel|Telephone)[:\s]*/i, '').trim();
      phone = phoneClean.replace(/[\s()]/g, '').replace(/^1-?/, '+1');
    }
    if (!phone) {
      // Try common Canadian phone format in the page
      const caPhone = html.match(/>(\+?1[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})</);
      if (caPhone) phone = caPhone[1].replace(/[\s().-]/g, '');
    }

    // Extract website
    let website = '';
    const visitWebsite = html.match(/href=["'](https?:\/\/[^"']+)["'][^>]*>\s*Visit\s+Website/i);
    if (visitWebsite) {
      website = visitWebsite[1];
    } else {
      // Look for website links that aren't languagescanada.ca or social media
      const allLinks = html.match(/href=["'](https?:\/\/[^"']+)/gi) || [];
      for (const link of allLinks) {
        const u = (link.match(/href=["'](https?:\/\/[^"']+)/i) || [])[1];
        if (u && !u.includes('languagescanada.ca') && !u.includes('facebook.com')
            && !u.includes('twitter.com') && !u.includes('instagram.com')
            && !u.includes('linkedin.com') && !u.includes('youtube.com')
            && !u.includes('google.com') && !u.includes('mailto:')
            && !u.includes('javascript:') && !u.includes('.gov.')
            && !u.includes('w3.org') && !u.includes('schema.org')
            && !u.includes('jquery') && !u.includes('cdn.')
            && !u.includes('cloudflare') && !u.includes('gstatic')) {
          website = u;
          break;
        }
      }
    }

    // Extract address - look for address block
    let address = '';
    let city = '';
    let province = '';

    // Try to find address in structured content
    const addressMatch = html.match(/(?:Address|Location)[:\s]*<[^>]*>([^<]+)/i);
    if (addressMatch) {
      address = decodeEntities(addressMatch[1].trim());
    }

    // Parse province from URL slug or address (many schools have city in name)
    // Languages Canada PDF gives us province mappings, use common ones
    const provinceCodes = {
      'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba',
      'NB': 'New Brunswick', 'NL': 'Newfoundland', 'NS': 'Nova Scotia',
      'NT': 'Northwest Territories', 'NU': 'Nunavut', 'ON': 'Ontario',
      'PE': 'Prince Edward Island', 'QC': 'Quebec', 'SK': 'Saskatchewan',
      'YT': 'Yukon',
    };

    // Try to find province/city from page content
    const provMatch = html.match(/(?:Province|State)[:\s]*<[^>]*>([^<]+)/i);
    if (provMatch) province = decodeEntities(provMatch[1].trim());

    const cityMatch = html.match(/(?:City)[:\s]*<[^>]*>([^<]+)/i);
    if (cityMatch) city = decodeEntities(cityMatch[1].trim());

    // Fallback: infer city from school name for known patterns
    if (!city) {
      const cityInName = schoolName.match(/[-–]\s*(Toronto|Vancouver|Montreal|Ottawa|Calgary|Edmonton|Victoria|Halifax|Winnipeg|Kelowna|London|Barrie|Kitchener|Surrey|Brampton|Mississauga|Markham|Richmond|Langley|Oakville|Windsor|Guelph|Waterloo|Welland|Thunder Bay|Burlington|Moncton|Charlottetown|Lunenburg|Glace Bay|Bathurst|Shippagan|Saint John|Regina|North York|Coquitlam|North Vancouver|La Pocatière|Pointe-Claire|Québec|Quebec|LaSalle)\s*$/i);
      if (cityInName) city = cityInName[1].trim();
    }

    // Contact person name (if available)
    let contactName = '';
    const contactMatch = html.match(/Contact\s*(?:Person)?[:\s]*<[^>]*>([^<]+)/i);
    if (contactMatch) contactName = decodeEntities(contactMatch[1].trim());

    let first_name = '';
    let last_name = '';
    if (contactName) {
      const nameParts = contactName.split(/\s+/);
      first_name = nameParts[0] || '';
      last_name = nameParts.slice(1).join(' ') || '';
    }

    const lead = {
      email: email,
      first_name: first_name,
      last_name: last_name,
      company: schoolName,
      phone: phone,
      city: city,
      state: province,
      country: 'Canada',
      source: 'languages-canada',
      website: website,
    };

    leads.push(lead);

    if (email) stats.withEmail++;
    if (phone) stats.withPhone++;
    if (website) stats.withWebsite++;

    if (fetched % 20 === 0) {
      process.stdout.write(`\r    Fetched ${fetched}/${schoolUrls.length} detail pages (${leads.length} leads, ${errors} errors)...`);
    }

    await sleep(DELAY_MS);
  }

  console.log(`\r    Fetched ${fetched}/${schoolUrls.length} detail pages (${leads.length} leads, ${errors} errors)    `);
  console.log(`  [DONE] ${leads.length} Languages Canada schools scraped`);
  stats.bySource['languages-canada'] = leads.length;
  return leads;
}


// ═══════════════════════════════════════════════════════════════════════
// Main Pipeline
// ═══════════════════════════════════════════════════════════════════════

function saveCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const csvColumns = ['email', 'first_name', 'last_name', 'company', 'phone', 'city', 'state', 'country', 'source'];
  const rows = leads.map(lead =>
    csvColumns.map(col => escapeCSV(lead[col] || '')).join(',')
  );

  fs.writeFileSync(OUTPUT_FILE, [CSV_HEADER, ...rows].join('\n'), 'utf8');
}

function dedup(leads) {
  const seen = new Set();
  const deduped = [];

  for (const lead of leads) {
    // Dedup by email (primary), then by company+city
    let key = '';
    if (lead.email) {
      key = `email:${lead.email.toLowerCase()}`;
    } else if (lead.company && lead.city) {
      key = `company:${lead.company.toLowerCase()}:${lead.city.toLowerCase()}`;
    } else if (lead.company) {
      key = `company:${lead.company.toLowerCase()}`;
    } else {
      continue; // Skip entries with no identifiable info
    }

    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  return deduped;
}

(async () => {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   IELTS/TOEFL Test Prep & Language School Scraper         ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`  Mode: ${TEST_MODE ? 'TEST (limited pages)' : 'FULL'}`);
  console.log(`  Source filter: ${SOURCE_ONLY || 'all'}`);
  console.log(`  Delay: ${DELAY_MS}ms between requests`);
  console.log(`  Output: ${OUTPUT_FILE}`);

  let allLeads = [];

  try {
    // Source 1: English UK Members
    if (!SOURCE_ONLY || SOURCE_ONLY === 'englishuk') {
      const ukLeads = await scrapeEnglishUKMembers();
      allLeads = allLeads.concat(ukLeads);
    }

    // Source 2: English UK Partner Agencies
    if (!SOURCE_ONLY || SOURCE_ONLY === 'agencies') {
      const agencyLeads = await scrapeEnglishUKAgencies();
      allLeads = allLeads.concat(agencyLeads);
    }

    // Source 3: Languages Canada
    if (!SOURCE_ONLY || SOURCE_ONLY === 'canada') {
      const canadaLeads = await scrapeLanguagesCanada();
      allLeads = allLeads.concat(canadaLeads);
    }

  } catch (err) {
    console.error(`\n[FATAL] ${err.message}`);
    console.error(err.stack);
    // Save whatever we have so far
    if (allLeads.length > 0) {
      console.log(`\n  Saving ${allLeads.length} leads collected before error...`);
    }
  }

  // Dedup
  const beforeDedup = allLeads.length;
  allLeads = dedup(allLeads);
  const dupesRemoved = beforeDedup - allLeads.length;

  // Count stats
  for (const lead of allLeads) {
    const c = lead.country || 'Unknown';
    stats.byCountry[c] = (stats.byCountry[c] || 0) + 1;
  }
  stats.totalLeads = allLeads.length;

  // Save CSV
  saveCSV(allLeads);

  // Print final report
  console.log('\n╔════════════════════════════════════════════════════════════╗');
  console.log('║   SCRAPE COMPLETE                                         ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`  Time elapsed: ${elapsed()}`);
  console.log(`  Total leads: ${stats.totalLeads}`);
  console.log(`  Duplicates removed: ${dupesRemoved}`);
  console.log(`  With email: ${stats.withEmail}`);
  console.log(`  With phone: ${stats.withPhone}`);
  console.log(`  With website: ${stats.withWebsite}`);
  console.log(`  Output: ${OUTPUT_FILE}`);

  // Source breakdown
  console.log('\n  Source breakdown:');
  for (const [source, count] of Object.entries(stats.bySource)) {
    console.log(`    ${source}: ${count}`);
  }

  // Country breakdown (top 20)
  const sortedCountries = Object.entries(stats.byCountry).sort((a, b) => b[1] - a[1]);
  console.log(`\n  Country breakdown (${sortedCountries.length} countries):`);
  for (const [country, count] of sortedCountries.slice(0, 20)) {
    console.log(`    ${country}: ${count}`);
  }
  if (sortedCountries.length > 20) {
    console.log(`    ... and ${sortedCountries.length - 20} more`);
  }

  // Email rate
  const emailRate = stats.totalLeads > 0 ? (stats.withEmail / stats.totalLeads * 100).toFixed(1) : 0;
  console.log(`\n  Email capture rate: ${emailRate}%`);

  console.log('\n  Done.');
})();
