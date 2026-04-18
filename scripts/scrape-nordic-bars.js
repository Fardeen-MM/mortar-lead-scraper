#!/usr/bin/env node
/**
 * Nordic Bar Association Scraper
 * Scrapes lawyer directories for Sweden, Norway, Denmark, Finland
 *
 * Sources:
 *   Finland  — findanattorney.fi  /api/searchforprint  (single JSON call, ~2400 lawyers)
 *   Sweden   — advokatsamfundet.se  listing page + company detail pages (~2950 firms)
 *   Denmark  — advokatnoeglen.dk  /sog.aspx paginated search + detail pages (~8000 lawyers)
 *   Norway   — advokatenhjelperdeg.no  paginated listing + profile pages (~6900 lawyers)
 *
 * Output: output/nordic-lawyers.csv
 * Usage:  node scripts/scrape-nordic-bars.js [--country=finland|sweden|denmark|norway] [--max-pages=N] [--skip-details]
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'nordic-lawyers.csv');
const DELAY_MS = 1500;            // 1.5s between requests
const DETAIL_DELAY_MS = 1000;     // 1s between detail page fetches
const MAX_RETRIES = 3;

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
const args = process.argv.slice(2).reduce((acc, arg) => {
  const [k, v] = arg.replace(/^--/, '').split('=');
  acc[k] = v || true;
  return acc;
}, {});

const ONLY_COUNTRY = args.country || null;   // e.g. "finland"
const MAX_PAGES = args['max-pages'] ? parseInt(args['max-pages'], 10) : Infinity;
const SKIP_DETAILS = !!args['skip-details']; // skip detail page fetching (faster, fewer emails)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Make an HTTPS GET request. Returns { statusCode, headers, body }.
 * Follows redirects. Retries on transient errors.
 */
function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const reqOpts = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...options.headers,
      },
      rejectUnauthorized: false, // Sweden has cert issues
      timeout: 30000,
    };

    const req = mod.get(url, reqOpts, (res) => {
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        resolve(httpGet(redir, options));
        return;
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks).toString('utf-8'),
        });
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

/** Retry wrapper */
async function fetchWithRetry(url, opts = {}, retries = MAX_RETRIES) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await httpGet(url, opts);
      if (res.statusCode === 200) return res;
      if (res.statusCode === 429 || res.statusCode >= 500) {
        console.log(`  [retry ${i + 1}/${retries}] HTTP ${res.statusCode} for ${url}`);
        await sleep(3000 * (i + 1));
        continue;
      }
      return res; // 4xx other than 429 — don't retry
    } catch (err) {
      console.log(`  [retry ${i + 1}/${retries}] Error: ${err.message} for ${url}`);
      if (i < retries - 1) await sleep(3000 * (i + 1));
    }
  }
  return null;
}

/** Decode HTML entities: &#123; &#x7B; &amp; etc. */
function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#xF6;/gi, 'ö')
    .replace(/&#xE4;/gi, 'ä')
    .replace(/&#xE5;/gi, 'å')
    .replace(/&#xC5;/gi, 'Å')
    .replace(/&#xC4;/gi, 'Ä')
    .replace(/&#xD6;/gi, 'Ö')
    .replace(/&#xE9;/gi, 'é')
    .replace(/&#xE8;/gi, 'è')
    .replace(/&#xF8;/gi, 'ø')
    .replace(/&#xE6;/gi, 'æ')
    .replace(/&#xD8;/gi, 'Ø')
    .replace(/&#xC6;/gi, 'Æ')
    .replace(/&nbsp;/g, ' ');
}

/** Escape CSV field */
function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/\r?\n/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/** Split "Last, First" or "First Last" into { first_name, last_name } */
function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const name = decodeHtmlEntities(fullName).trim();
  if (name.includes(',')) {
    const [last, ...first] = name.split(',').map(s => s.trim());
    return { first_name: first.join(' '), last_name: last };
  }
  const parts = name.split(/\s+/);
  if (parts.length === 1) return { first_name: '', last_name: parts[0] };
  return { first_name: parts.slice(0, -1).join(' '), last_name: parts[parts.length - 1] };
}

/** Clean phone: strip common artifacts */
function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/\s+/g, ' ').replace(/[^\d+\s()-]/g, '').trim();
}

// ---------------------------------------------------------------------------
// CSV Writer
// ---------------------------------------------------------------------------
const CSV_HEADERS = ['email', 'first_name', 'last_name', 'company', 'phone', 'city', 'state', 'country', 'source', 'website'];
let csvStream = null;
let totalWritten = 0;

function initCsv() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  csvStream = fs.createWriteStream(OUTPUT_FILE);
  csvStream.write(CSV_HEADERS.join(',') + '\n');
}

function writeLead(lead) {
  const row = CSV_HEADERS.map(h => csvEscape(lead[h] || '')).join(',');
  csvStream.write(row + '\n');
  totalWritten++;
}

function closeCsv() {
  return new Promise(resolve => {
    if (csvStream) csvStream.end(resolve);
    else resolve();
  });
}

// ---------------------------------------------------------------------------
// FINLAND — findanattorney.fi
// Single API call returns all lawyers grouped by municipality
// ---------------------------------------------------------------------------
async function scrapeFinland() {
  console.log('\n=== FINLAND (Asianajajaliitto) ===');
  console.log('Source: findanattorney.fi/api/searchforprint');

  const res = await fetchWithRetry('https://www.findanattorney.fi/api/searchforprint?lang=en', {
    headers: { 'Accept': 'application/json' },
  });
  if (!res || res.statusCode !== 200) {
    console.log('ERROR: Failed to fetch Finland API');
    return { total: 0, emails: 0 };
  }

  let data;
  try {
    data = JSON.parse(res.body);
  } catch (e) {
    console.log('ERROR: Invalid JSON from Finland API');
    return { total: 0, emails: 0 };
  }

  let count = 0;
  let emailCount = 0;

  for (const muni of data) {
    const city = muni.municipality || '';
    for (const wp of (muni.workplaces || [])) {
      const firmName = (wp.name || '').trim();
      const firmPhone = cleanPhone(wp.phone || '');
      const firmEmail = (wp.email || '').trim();
      const firmWeb = (wp.www || '').trim();
      const address = (wp.postaladdress || '').trim();

      for (const lawyer of (wp.lawyers || [])) {
        const { first_name, last_name } = splitName(lawyer.name);
        const email = (lawyer.email || firmEmail || '').trim().toLowerCase();
        const phone = cleanPhone(lawyer.phone || firmPhone);

        writeLead({
          email,
          first_name,
          last_name,
          company: firmName,
          phone,
          city,
          state: '',
          country: 'Finland',
          source: 'finland_bar',
          website: firmWeb,
        });
        count++;
        if (email) emailCount++;
      }
    }
  }

  console.log(`Finland: ${count} lawyers, ${emailCount} with email`);
  return { total: count, emails: emailCount };
}

// ---------------------------------------------------------------------------
// SWEDEN — advokatsamfundet.se
// Step 1: Fetch main listing (all firms on one page) → companyids + names + cities + phones
// Step 2: Fetch each company detail page → office email + person names
// ---------------------------------------------------------------------------
async function scrapeSweden() {
  console.log('\n=== SWEDEN (Advokatsamfundet) ===');
  console.log('Source: advokatsamfundet.se/Sok-advokat/Sokresultat/');

  // Step 1: Get all firms from listing page
  console.log('Step 1: Fetching main firm listing...');
  const listUrl = 'https://www.advokatsamfundet.se/Sok-advokat/Sokresultat/?LawFirmName=&LawyerLastname=&LawyerFirstname=&LawFirmCity=&LawyerCategory=&SubjectField=';
  const listRes = await fetchWithRetry(listUrl);
  if (!listRes || listRes.statusCode !== 200) {
    console.log('ERROR: Failed to fetch Sweden listing');
    return { total: 0, emails: 0 };
  }

  // Parse firm rows: each row has companyid, firm name, city, phone
  const firms = [];
  const rowRegex = /companyid=(\d+)">(.*?)<\/a>[\s\S]*?text-caps--first-letter">\s*(.*?)\s*<\/div>[\s\S]*?1-of-1--phone\s*\]?">\s*([\s\S]*?)\s*<\/div>/g;
  let match;
  while ((match = rowRegex.exec(listRes.body)) !== null) {
    firms.push({
      companyId: match[1],
      name: decodeHtmlEntities(match[2]).trim(),
      city: decodeHtmlEntities(match[3]).trim().replace(/^-$/, ''),
      phone: cleanPhone(match[4].replace(/<[^>]*>/g, '').trim()),
    });
  }

  console.log(`  Found ${firms.length} firms in listing`);
  if (firms.length === 0) {
    // Fallback: just extract companyids
    const ids = [...listRes.body.matchAll(/companyid=(\d+)/g)].map(m => m[1]);
    const uniqueIds = [...new Set(ids)];
    console.log(`  Fallback: found ${uniqueIds.length} company IDs`);
    for (const id of uniqueIds) {
      firms.push({ companyId: id, name: '', city: '', phone: '' });
    }
  }

  // Step 2: Fetch each company detail page
  let count = 0;
  let emailCount = 0;
  const totalFirms = SKIP_DETAILS ? 0 : Math.min(firms.length, MAX_PAGES === Infinity ? firms.length : MAX_PAGES * 20);

  if (SKIP_DETAILS) {
    // Just output firm-level data from the listing
    console.log('Step 2: Skipped (--skip-details). Writing firm-level data...');
    for (const firm of firms) {
      const { first_name, last_name } = splitName(firm.name);
      writeLead({
        email: '',
        first_name,
        last_name,
        company: firm.name,
        phone: firm.phone,
        city: firm.city,
        state: '',
        country: 'Sweden',
        source: 'sweden_bar',
        website: '',
      });
      count++;
    }
  } else {
    console.log(`Step 2: Fetching ${totalFirms} company detail pages...`);
    for (let i = 0; i < totalFirms; i++) {
      const firm = firms[i];
      if (i > 0 && i % 50 === 0) {
        console.log(`  Progress: ${i}/${totalFirms} firms (${count} lawyers, ${emailCount} emails)`);
      }

      const detailUrl = `https://www.advokatsamfundet.se/Sok-advokat/Sokresultat/Kontorsdetaljer/?companyid=${firm.companyId}`;
      const detailRes = await fetchWithRetry(detailUrl);
      await sleep(DETAIL_DELAY_MS);

      if (!detailRes || detailRes.statusCode !== 200) continue;

      const html = detailRes.body;

      // Extract office email (from Kontakt section, excluding bar association email)
      let officeEmail = '';
      const kontaktSection = html.substring(
        html.indexOf('Kontakt'),
        html.indexOf('Postadress') > 0 ? html.indexOf('Postadress') : html.indexOf('Jurister')
      );
      const emailMatches = (kontaktSection || '').match(/mailto:([^"]+)/g) || [];
      for (const em of emailMatches) {
        const addr = em.replace('mailto:', '').toLowerCase();
        if (!addr.includes('advokatsamfundet.se')) {
          officeEmail = addr;
          break;
        }
      }

      // Extract office phone
      let officePhone = firm.phone || '';
      const phoneMatch = (kontaktSection || '').match(/(?:Växel|Direkt|Telefon):\s*<\/span>([^<]+)/);
      if (phoneMatch) officePhone = cleanPhone(phoneMatch[1]);
      if (!officePhone) {
        const phoneMatch2 = (kontaktSection || '').match(/>(\d[\d\s-]{5,})</);
        if (phoneMatch2) officePhone = cleanPhone(phoneMatch2[1]);
      }

      // Extract website
      let website = '';
      const webMatch = html.match(/href="(https?:\/\/[^"]+)"[^>]*class="[^"]*website/i) ||
                        html.match(/href="(https?:\/\/(?!.*advokatsamfundet)[^"]+)"[^>]*>\s*(?:www\.|http)/i);
      if (webMatch) website = webMatch[1];

      // Extract firm name from h1 (more reliable than listing)
      const h1Match = html.match(/hero-heading[^>]*>(.*?)</);
      const firmName = h1Match ? decodeHtmlEntities(h1Match[1]).trim() : firm.name;

      // Extract city from address
      let city = firm.city;
      const addrMatch = html.match(/Postadress[\s\S]*?<div>([\s\S]*?)<\/div>/);
      if (addrMatch) {
        const addrLines = addrMatch[1].replace(/<br\s*\/?>/g, '\n').split('\n').map(l => l.trim()).filter(Boolean);
        // City is usually in the line with postal code (e.g. "113 24 Stockholm")
        for (const line of addrLines) {
          const cityMatch = line.match(/\d{3}\s*\d{2}\s+(.+)/);
          if (cityMatch) {
            city = cityMatch[1].replace(/Sverige$/i, '').trim();
            break;
          }
        }
      }

      // Extract persons from jurister section
      const personRegex = /Persondetaljer\/\?personid=(\d+)">(.*?)</g;
      const persons = [];
      let pm;
      while ((pm = personRegex.exec(html)) !== null) {
        persons.push({ personId: pm[1], name: decodeHtmlEntities(pm[2]).trim() });
      }

      if (persons.length === 0) {
        // Single-lawyer firm — use firm name as person
        const { first_name, last_name } = splitName(firmName);
        writeLead({
          email: officeEmail,
          first_name,
          last_name,
          company: firmName,
          phone: officePhone,
          city,
          state: '',
          country: 'Sweden',
          source: 'sweden_bar',
          website,
        });
        count++;
        if (officeEmail) emailCount++;
      } else {
        // Multiple lawyers — each gets the office email
        for (const person of persons) {
          const { first_name, last_name } = splitName(person.name);
          writeLead({
            email: officeEmail,
            first_name,
            last_name,
            company: firmName,
            phone: officePhone,
            city,
            state: '',
            country: 'Sweden',
            source: 'sweden_bar',
            website,
          });
          count++;
          if (officeEmail) emailCount++;
        }
      }
    }
  }

  console.log(`Sweden: ${count} lawyers, ${emailCount} with email`);
  return { total: count, emails: emailCount };
}

// ---------------------------------------------------------------------------
// DENMARK — advokatnoeglen.dk
// Paginated search: /sog.aspx?s=1&t=1&zf=1000&zt=9999&p=N
// Emails on detail pages (reversed/obfuscated)
// ---------------------------------------------------------------------------
async function scrapeDenmark() {
  console.log('\n=== DENMARK (Advokatsamfundet DK) ===');
  console.log('Source: advokatnoeglen.dk/sog.aspx');

  let allLawyers = [];
  let page = 0;
  let emptyPages = 0;
  const maxPages = Math.min(MAX_PAGES, 500); // safety cap

  // Step 1: Paginate through search results
  console.log('Step 1: Paginating search results...');
  while (page < maxPages && emptyPages < 3) {
    const url = `https://www.advokatnoeglen.dk/sog.aspx?s=1&t=1&zf=1000&zt=9999&p=${page}`;
    const res = await fetchWithRetry(url);
    await sleep(DELAY_MS);

    if (!res || res.statusCode !== 200) {
      emptyPages++;
      page++;
      continue;
    }

    // Parse table rows: onclick="location.href='/advokat/slug'" + td cells
    const rowRegex = /onclick="location\.href=&#39;(\/advokat\/[^']+)&#39;">\s*<td>(.*?)<\/td><td>([\s\S]*?)<\/td><td[^>]*>([\s\S]*?)<\/td>/g;
    let match;
    let pageCount = 0;

    while ((match = rowRegex.exec(res.body)) !== null) {
      const slug = match[1];
      const name = decodeHtmlEntities(match[2].replace(/<[^>]*>/g, '')).trim();
      const firmCell = decodeHtmlEntities(match[3].replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ')).trim();
      const areas = decodeHtmlEntities(match[4].replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ')).trim();

      // Parse firm and city from firmCell (format: "FIRM NAME, City")
      let company = '';
      let city = '';
      const firmParts = firmCell.split(',');
      if (firmParts.length >= 2) {
        company = firmParts.slice(0, -1).join(',').trim();
        city = firmParts[firmParts.length - 1].trim();
      } else {
        company = firmCell;
      }

      allLawyers.push({ slug, name, company, city, areas });
      pageCount++;
    }

    if (pageCount === 0) {
      emptyPages++;
    } else {
      emptyPages = 0;
    }

    if (page % 20 === 0) {
      console.log(`  Page ${page}: ${pageCount} results (total so far: ${allLawyers.length})`);
    }
    page++;
  }

  console.log(`  Listing complete: ${allLawyers.length} lawyers across ${page} pages`);

  // Step 2: Fetch detail pages for email + phone
  let count = 0;
  let emailCount = 0;

  if (SKIP_DETAILS) {
    console.log('Step 2: Skipped (--skip-details). Writing listing data...');
    for (const lawyer of allLawyers) {
      const { first_name, last_name } = splitName(lawyer.name);
      writeLead({
        email: '',
        first_name,
        last_name,
        company: lawyer.company,
        phone: '',
        city: lawyer.city,
        state: '',
        country: 'Denmark',
        source: 'denmark_bar',
        website: '',
      });
      count++;
    }
  } else {
    console.log(`Step 2: Fetching ${allLawyers.length} detail pages for email/phone...`);
    for (let i = 0; i < allLawyers.length; i++) {
      const lawyer = allLawyers[i];
      if (i > 0 && i % 100 === 0) {
        console.log(`  Progress: ${i}/${allLawyers.length} (${emailCount} emails found)`);
      }

      const detailUrl = `https://www.advokatnoeglen.dk${lawyer.slug}`;
      const res = await fetchWithRetry(detailUrl);
      await sleep(DETAIL_DELAY_MS);

      let email = '';
      let phone = '';
      let website = '';

      if (res && res.statusCode === 200) {
        const html = res.body;

        // Extract personal email (reversed/obfuscated)
        // Pattern: /email.aspx?e=REVERSED_EMAIL
        const emailMatch = html.match(/Beskikkelses[\s\S]*?email\.aspx\?e=([^'"&]+)/);
        if (emailMatch && emailMatch[1]) {
          email = emailMatch[1].split('').reverse().join('').toLowerCase().trim();
        }

        // Fallback: firm email
        if (!email) {
          const firmEmailMatch = html.match(/Email:[\s\S]*?email\.aspx\?e=([^'"&]+)/);
          if (firmEmailMatch && firmEmailMatch[1]) {
            email = firmEmailMatch[1].split('').reverse().join('').toLowerCase().trim();
          }
        }

        // Extract phone from firm section
        const phoneMatch = html.match(/Tlf\.:\s*(\d[\d\s]+)/);
        if (phoneMatch) phone = cleanPhone(phoneMatch[1]);

        // Extract website
        const webMatch = html.match(/href="(https?:\/\/[^"]+)"[^>]*style="color[^>]*>(www\.[^<]+)/);
        if (webMatch) website = webMatch[1];

        // Better city from detail page
        const cityMatch = html.match(/\d{4}\s+([A-ZÆØÅa-zæøå\s]+?)(?:<|$)/);
        if (cityMatch) lawyer.city = cityMatch[1].trim();
      }

      const { first_name, last_name } = splitName(lawyer.name);
      writeLead({
        email,
        first_name,
        last_name,
        company: lawyer.company,
        phone,
        city: lawyer.city,
        state: '',
        country: 'Denmark',
        source: 'denmark_bar',
        website,
      });
      count++;
      if (email) emailCount++;
    }
  }

  console.log(`Denmark: ${count} lawyers, ${emailCount} with email`);
  return { total: count, emails: emailCount };
}

// ---------------------------------------------------------------------------
// NORWAY — advokatenhjelperdeg.no
// Paginated WordPress/FacetWP listing + profile pages for contact info
// ---------------------------------------------------------------------------
async function scrapeNorway() {
  console.log('\n=== NORWAY (Advokatforeningen) ===');
  console.log('Source: advokatenhjelperdeg.no/finn-advokat/');

  let allProfiles = [];
  let page = 1;
  let emptyPages = 0;
  const maxPages = Math.min(MAX_PAGES, 800); // 694 pages estimated

  // Step 1: Paginate listing pages to collect profile URLs
  console.log('Step 1: Paginating listing pages...');
  while (page <= maxPages && emptyPages < 3) {
    const url = page === 1
      ? 'https://advokatenhjelperdeg.no/finn-advokat/'
      : `https://advokatenhjelperdeg.no/finn-advokat/?_ahd_pager=${page}`;
    const res = await fetchWithRetry(url);
    await sleep(DELAY_MS);

    if (!res || res.statusCode !== 200) {
      emptyPages++;
      page++;
      continue;
    }

    // Extract profile URLs and names from the facetwp-template section
    const templateStart = res.body.indexOf('facetwp-template');
    const templateEnd = res.body.indexOf('facetwp-facet', templateStart > 0 ? templateStart : 0);
    const section = templateStart > 0
      ? res.body.substring(templateStart, templateEnd > templateStart ? templateEnd : undefined)
      : res.body;

    // Each lawyer card: <h2><a href="/finn-advokat/REGION/NAME/">Lawyer Name</a></h2>
    //                    <h4><a href="/firma/SLUG/">Firm Name</a></h4>
    const cardRegex = /<h2>\s*<a\s+href="(https?:\/\/advokatenhjelperdeg\.no\/finn-advokat\/([^/]+)\/([^/]+)\/)"[^>]*>\s*([\s\S]*?)\s*<\/a>\s*<\/h2>[\s\S]*?(?:<h4>\s*<a[^>]*>\s*([\s\S]*?)\s*<\/a>\s*<\/h4>)?/g;
    let match;
    let pageCount = 0;

    while ((match = cardRegex.exec(section)) !== null) {
      allProfiles.push({
        profileUrl: match[1],
        region: decodeHtmlEntities(match[2]).replace(/-/g, ' '),
        nameSlug: match[3],
        name: decodeHtmlEntities(match[4]).trim(),
        firm: decodeHtmlEntities((match[5] || '').replace(/<[^>]*>/g, '')).trim(),
      });
      pageCount++;
    }

    if (pageCount === 0) {
      emptyPages++;
    } else {
      emptyPages = 0;
    }

    if (page % 50 === 0 || page === 1) {
      console.log(`  Page ${page}: ${pageCount} profiles (total so far: ${allProfiles.length})`);
    }
    page++;
  }

  console.log(`  Listing complete: ${allProfiles.length} profiles across ${page - 1} pages`);

  // Step 2: Fetch each profile page for email + phone
  let count = 0;
  let emailCount = 0;

  if (SKIP_DETAILS) {
    console.log('Step 2: Skipped (--skip-details). Writing listing data...');
    for (const profile of allProfiles) {
      const { first_name, last_name } = splitName(profile.name);
      // Derive city from region slug (e.g., "oslo-oslo" → "Oslo")
      const city = profile.region.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
      writeLead({
        email: '',
        first_name,
        last_name,
        company: profile.firm,
        phone: '',
        city,
        state: '',
        country: 'Norway',
        source: 'norway_bar',
        website: '',
      });
      count++;
    }
  } else {
    console.log(`Step 2: Fetching ${allProfiles.length} profile pages for email/phone...`);
    for (let i = 0; i < allProfiles.length; i++) {
      const profile = allProfiles[i];
      if (i > 0 && i % 200 === 0) {
        console.log(`  Progress: ${i}/${allProfiles.length} (${emailCount} emails found)`);
      }

      const res = await fetchWithRetry(profile.profileUrl);
      await sleep(DETAIL_DELAY_MS);

      let email = '';
      let phone = '';
      let website = '';
      let city = profile.region.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

      if (res && res.statusCode === 200) {
        const html = res.body;

        // Extract email (HTML-entity encoded in mailto: link)
        const emailMatch = html.match(/lawyer-mail[^>]*href="mailto:([^"]+)"/);
        if (emailMatch) {
          email = decodeHtmlEntities(emailMatch[1]).toLowerCase().trim();
        }

        // Extract phone (firm phone or mobile)
        const firmPhoneMatch = html.match(/land-firm-phone[^>]*>([^<]+)/);
        const mobileMatch = html.match(/lawyer-phone[^>]*>([^<]+)/);
        phone = cleanPhone((mobileMatch && mobileMatch[1]) || (firmPhoneMatch && firmPhoneMatch[1]) || '');

        // Extract website
        const webMatch = html.match(/title="Gå til nettsted[^"]*">\s*(https?:\/\/[^<\s]+)/);
        if (webMatch) website = webMatch[1].trim();

        // Extract city from address
        const addrMatch = html.match(/Besøksadresse:[\s\S]*?(\d{4})\s+([A-ZÆØÅ\s]+)/);
        if (addrMatch) {
          city = addrMatch[2].trim().split(' ').map(w =>
            w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
          ).join(' ');
        }

        // Better firm name from profile page
        const firmMatch = html.match(/class="land-name-company"[^>]*>(.*?)</);
        if (firmMatch) profile.firm = decodeHtmlEntities(firmMatch[1]).trim();
      }

      const { first_name, last_name } = splitName(profile.name);
      writeLead({
        email,
        first_name,
        last_name,
        company: profile.firm,
        phone,
        city,
        state: '',
        country: 'Norway',
        source: 'norway_bar',
        website,
      });
      count++;
      if (email) emailCount++;
    }
  }

  console.log(`Norway: ${count} lawyers, ${emailCount} with email`);
  return { total: count, emails: emailCount };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('='.repeat(60));
  console.log('Nordic Bar Association Scraper');
  console.log('='.repeat(60));
  if (ONLY_COUNTRY) console.log(`Country filter: ${ONLY_COUNTRY}`);
  if (MAX_PAGES < Infinity) console.log(`Max pages: ${MAX_PAGES}`);
  if (SKIP_DETAILS) console.log('Mode: skip-details (listing data only, no emails)');
  console.log(`Output: ${OUTPUT_FILE}`);

  const startTime = Date.now();
  initCsv();

  const results = {};
  const countries = [
    { name: 'finland', fn: scrapeFinland },
    { name: 'sweden', fn: scrapeSweden },
    { name: 'denmark', fn: scrapeDenmark },
    { name: 'norway', fn: scrapeNorway },
  ];

  for (const { name, fn } of countries) {
    if (ONLY_COUNTRY && ONLY_COUNTRY.toLowerCase() !== name) continue;
    try {
      results[name] = await fn();
    } catch (err) {
      console.error(`ERROR in ${name}: ${err.message}`);
      console.error(err.stack);
      results[name] = { total: 0, emails: 0 };
    }
  }

  await closeCsv();

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  for (const [country, { total, emails }] of Object.entries(results)) {
    console.log(`  ${country.padEnd(12)} ${String(total).padStart(6)} lawyers  ${String(emails).padStart(6)} emails`);
  }
  console.log(`  ${'TOTAL'.padEnd(12)} ${String(totalWritten).padStart(6)} lawyers`);
  console.log(`\nOutput: ${OUTPUT_FILE}`);
  console.log(`Time: ${elapsed} minutes`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
