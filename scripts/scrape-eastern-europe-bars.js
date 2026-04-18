#!/usr/bin/env node
/**
 * Eastern European Bar Association Scraper
 *
 * Scrapes lawyer directories for:
 *   1. Poland — KIRP (Krajowa Izba Radcow Prawnych) — 56K+ legal advisors
 *      Source: kirp.pl WordPress listing pages (name, city, district, reg#, status)
 *
 *   2. Romania — UNBR via cautavocat.ro — 22K+ lawyers
 *      Source: JSON API (getavocati.php) for listing + HTML profile pages for email/phone
 *
 *   3. Hungary — Regional bar associations (területi kamarák)
 *      Source: pecsiugyvedikamara.hu-style pages with full contact info
 *
 * Output: output/eastern-europe-lawyers.csv
 * Stack: Pure HTTP (https/http modules), no Puppeteer, Cheerio-free regex parsing
 * Encoding: UTF-8 throughout (handles Polish/Romanian/Hungarian diacritics)
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

// ─── Config ───────────────────────────────────────────────────────────────────

const OUTPUT = path.join(__dirname, '..', 'output', 'eastern-europe-lawyers.csv');
const DELAY_MS = 1500; // 1.5s between requests (polite)
const PROFILE_DELAY_MS = 1000; // 1s between profile fetches (Romania)
const REQUEST_TIMEOUT = 25000;
const MAX_RETRIES = 3;

// Romania: max profile pages to fetch for email/phone enrichment
// Set to 0 to skip profile enrichment and just use the listing API
const ROMANIA_MAX_PROFILES = 0; // listing-only mode (fast), set to Infinity for full enrichment

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function esc(v) {
  const s = (v ?? '').toString().trim().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

/**
 * Title-case a name, respecting Eastern European particles/prefixes.
 */
function titleCase(s) {
  if (!s) return '';
  return s.replace(/\b\w+/g, (w, i) => {
    const lower = w.toLowerCase();
    // Romanian/Hungarian/Polish particles
    if (i > 0 && ['de', 'di', 'von', 'van', 'el', 'al', 'bin', 'ben'].includes(lower)) {
      return lower;
    }
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  });
}

/**
 * HTTP(S) GET with redirect following, retry, and UTF-8 handling.
 */
function fetchUrl(url, options = {}) {
  const { method = 'GET', body = null, headers = {}, maxRedirects = 5 } = options;

  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;

    const reqHeaders = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/json,*/*',
      'Accept-Language': 'en-US,en;q=0.9,pl;q=0.8,ro;q=0.7,hu;q=0.6',
      'Accept-Encoding': 'identity',
      ...headers,
    };

    if (body && method === 'POST') {
      reqHeaders['Content-Type'] = 'application/x-www-form-urlencoded';
      reqHeaders['Content-Length'] = Buffer.byteLength(body);
    }

    const reqOpts = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers: reqHeaders,
      timeout: REQUEST_TIMEOUT,
      rejectUnauthorized: false, // Some Hungarian bars have bad SSL certs
    };

    const req = mod.request(reqOpts, res => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.host}${res.headers.location}`;
        return fetchUrl(redirectUrl, { ...options, maxRedirects: maxRedirects - 1 })
          .then(resolve).catch(reject);
      }

      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        resolve(buf.toString('utf8'));
      });
      res.on('error', reject);
    });

    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.on('error', reject);

    if (body && method === 'POST') req.write(body);
    req.end();
  });
}

/**
 * Fetch with retry logic.
 */
async function fetchWithRetry(url, options = {}, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fetchUrl(url, options);
    } catch (err) {
      if (attempt === retries) throw err;
      const waitMs = attempt * 2000;
      console.log(`    Retry ${attempt}/${retries} for ${url} (waiting ${waitMs}ms): ${err.message}`);
      await sleep(waitMs);
    }
  }
}

/**
 * Decode HTML entities.
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
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&nbsp;/g, ' ');
}

/**
 * Strip HTML tags from a string.
 */
function stripTags(html) {
  if (!html) return '';
  return decodeEntities(html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
}

// ─── POLAND: KIRP Scraper ─────────────────────────────────────────────────────
//
// kirp.pl/krajowa-wyszukiwarka-radcow-prawnych/page/{N}/
// WordPress site, 56,353 legal advisors, 2,818 pages of 20 each.
// Fields: surname, first name, reg number, district (OIRP), city, status.
// No email or phone in listing — but name+city+district is high-value for matching.

async function scrapePoland() {
  console.log('\n========================================');
  console.log('  POLAND — KIRP (Legal Advisors)');
  console.log('========================================\n');

  const leads = [];
  const BASE_URL = 'https://kirp.pl/krajowa-wyszukiwarka-radcow-prawnych';
  let page = 1;
  let totalPages = null;
  let consecutiveEmpty = 0;

  while (true) {
    const url = page === 1 ? `${BASE_URL}/` : `${BASE_URL}/page/${page}/`;

    try {
      const html = await fetchWithRetry(url);

      // Extract total results count on first page
      if (page === 1) {
        const countMatch = html.match(/(\d[\d\s,.]*)\s*wynik/i);
        if (countMatch) {
          const total = parseInt(countMatch[1].replace(/[\s,.]/g, ''));
          totalPages = Math.ceil(total / 20);
          console.log(`  Total results: ${total.toLocaleString()}, estimated ${totalPages} pages`);
        }
      }

      // Parse lawyer entries from the HTML table/list
      // The KIRP page renders a table with columns:
      // Nazwisko | Imie | Nr wpisu | Okrag | Miasto | Status
      //
      // Strategy: Find text blocks that match the pattern of table rows.
      // The page uses WordPress block layout with flex/grid, not a standard <table>.
      // We look for patterns of consecutive data that match known districts (OIRP).

      const pageLeads = parseKirpPage(html);

      if (pageLeads.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) {
          console.log(`  Page ${page}: 0 results (3 consecutive empty pages, stopping)`);
          break;
        }
      } else {
        consecutiveEmpty = 0;
      }

      for (const lead of pageLeads) {
        leads.push(lead);
      }

      if (page % 100 === 0 || page === 1) {
        console.log(`  Page ${page}/${totalPages || '?'}: +${pageLeads.length} leads (total: ${leads.length})`);
      }

      // Check if we've reached the last page
      if (totalPages && page >= totalPages) {
        console.log(`  Reached last page (${page})`);
        break;
      }

      // Safety valve
      if (page >= 3000) {
        console.log('  Safety limit reached (3000 pages)');
        break;
      }

      page++;
      await sleep(DELAY_MS);

    } catch (err) {
      console.log(`  Page ${page}: ERROR — ${err.message}`);
      consecutiveEmpty++;
      if (consecutiveEmpty >= 5) break;
      page++;
      await sleep(DELAY_MS * 2);
    }
  }

  console.log(`\n  Poland KIRP complete: ${leads.length} legal advisors`);
  return leads;
}

/**
 * Parse a KIRP listing page HTML into lead objects.
 * The page uses WordPress Gutenberg blocks, not a classic HTML table.
 * Each entry appears as text content in a repeated pattern with district codes like "OIRP".
 */
function parseKirpPage(html) {
  const leads = [];

  // Known OIRP (district) names for validation
  const oirpDistricts = [
    'Białystok', 'Bydgoszcz', 'Gdańsk', 'Katowice', 'Kielce', 'Koszalin',
    'Kraków', 'Lublin', 'Łódź', 'Olsztyn', 'Opole', 'Poznań', 'Rzeszów',
    'Szczecin', 'Toruń', 'Wałbrzych', 'Warszawa', 'Wrocław', 'Zielona Góra',
  ];

  // Strategy 1: Look for table rows (if standard HTML table is used)
  // Match <tr> blocks with 6 <td> cells
  const trPattern = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let trMatch;
  while ((trMatch = trPattern.exec(html)) !== null) {
    const row = trMatch[1];
    const cells = [];
    const tdPattern = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let tdMatch;
    while ((tdMatch = tdPattern.exec(row)) !== null) {
      cells.push(stripTags(tdMatch[1]));
    }

    // Expect: [surname, firstName, regNumber, district, city, status]
    if (cells.length >= 5) {
      const [surname, firstName, regNumber, district, city, ...rest] = cells;
      const status = rest.join(' ').trim();

      // Validate: district should contain "OIRP" or match known districts
      if (!district || (!district.includes('OIRP') && !oirpDistricts.some(d => district.includes(d)))) {
        // Check if this is a header row
        if (surname.toLowerCase().includes('nazwisko')) continue;
        // Might still be valid if other cells look right
        if (!regNumber || !/^[A-Z]{2}-\d+/.test(regNumber)) continue;
      }

      // Skip if surname looks like a header
      if (surname.toLowerCase() === 'nazwisko' || surname.toLowerCase() === 'imię') continue;
      if (!surname || surname.length < 2) continue;

      leads.push({
        email: '',
        first_name: titleCase(firstName || ''),
        last_name: titleCase(surname || ''),
        company: '',
        phone: '',
        city: (city || '').trim(),
        state: (district || '').replace('OIRP ', '').trim(),
        country: 'Poland',
        source: 'poland_bar',
      });
    }
  }

  // Strategy 2: If no <tr> found, parse from text patterns
  // The WordPress block layout uses divs with flex layout
  if (leads.length === 0) {
    // Look for registration number patterns (XX-NNNN) which are unique identifiers
    // and use them as anchors to find surrounding name/city data
    const regPattern = /([A-ZŁŚ]{2})-(\d{1,5})/g;
    let regMatch;
    const regPositions = [];

    while ((regMatch = regPattern.exec(html)) !== null) {
      regPositions.push({
        full: regMatch[0],
        prefix: regMatch[1],
        number: regMatch[2],
        index: regMatch.index,
      });
    }

    // For each registration number, look backwards for name and forwards for city
    for (const reg of regPositions) {
      // Get surrounding context (500 chars before and after)
      const contextStart = Math.max(0, reg.index - 500);
      const contextEnd = Math.min(html.length, reg.index + reg.full.length + 500);
      const context = html.substring(contextStart, contextEnd);
      const cleanContext = stripTags(context);

      // Try to extract name before the reg number
      // Pattern: "Surname FirstName ... XX-NNNN ... OIRP City ... City ... Status"
      const regPos = cleanContext.indexOf(reg.full);
      if (regPos < 0) continue;

      const before = cleanContext.substring(0, regPos).trim();
      const after = cleanContext.substring(regPos + reg.full.length).trim();

      // Name is typically the last 2-4 words before the reg number
      const beforeWords = before.split(/\s+/).filter(w => w.length > 0);
      if (beforeWords.length < 2) continue;

      // Take last 2-3 words as the name (surname + first name)
      const nameParts = beforeWords.slice(-3);
      let surname = '', firstName = '';

      // Check which parts are actually names (not pagination numbers, etc.)
      const nameFiltered = nameParts.filter(w =>
        w.length >= 2 && !/^\d+$/.test(w) && !/^(strona|wyniki|page)$/i.test(w)
      );

      if (nameFiltered.length >= 2) {
        surname = nameFiltered[0];
        firstName = nameFiltered.slice(1).join(' ');
      } else if (nameFiltered.length === 1) {
        surname = nameFiltered[0];
      } else {
        continue;
      }

      // Extract OIRP district and city from after the reg number
      let district = '';
      let city = '';
      const oirpMatch = after.match(/OIRP\s+(\S+(?:\s+\S+)?)/);
      if (oirpMatch) {
        district = oirpMatch[1].trim();
        // City is typically after the district
        const afterOirp = after.substring(after.indexOf(oirpMatch[0]) + oirpMatch[0].length).trim();
        const cityWords = afterOirp.split(/\s+/);
        // First word(s) before "Wykonuje" or "Nie" is the city
        const cityParts = [];
        for (const w of cityWords) {
          if (/^(Wykonuje|Nie|wykonuje|nie)/.test(w)) break;
          if (w.length < 2) continue;
          cityParts.push(w);
          if (cityParts.length >= 2) break; // cities are 1-2 words
        }
        city = cityParts.join(' ');
      }

      leads.push({
        email: '',
        first_name: titleCase(firstName),
        last_name: titleCase(surname),
        company: '',
        phone: '',
        city: city,
        state: district,
        country: 'Poland',
        source: 'poland_bar',
      });
    }
  }

  return leads;
}

// ─── ROMANIA: cautavocat.ro Scraper ───────────────────────────────────────────
//
// JSON API: POST https://cautavocat.ro/getavocati.php
// Params: page (1-based), barou (county slug or empty for all), domeniu, nume, etc.
// Returns: { cnt, pages, page, items: [{ id, nume, prenume, sex, judet, slug, definitiv, experienta, hasphoto }] }
//
// Profile pages: https://cautavocat.ro/avocat/{slug}
// Contains: phone, email (sometimes), address, firm, specializations, education
//
// Total: ~22,800 lawyers, 50 per page, ~457 pages

async function scrapeRomania() {
  console.log('\n========================================');
  console.log('  ROMANIA — UNBR (cautavocat.ro)');
  console.log('========================================\n');

  const leads = [];
  let page = 1;
  let totalPages = null;

  // Phase 1: Collect all lawyers from the listing API
  console.log('  Phase 1: Fetching lawyer listing via JSON API...');

  while (true) {
    try {
      const body = `page=${page}&barou=&domeniu=0&nume=&vechime=&limba=`;
      const json = await fetchWithRetry('https://cautavocat.ro/getavocati.php', {
        method: 'POST',
        body,
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Origin': 'https://cautavocat.ro',
          'Referer': 'https://cautavocat.ro/avocati',
        },
      });

      let data;
      try {
        data = JSON.parse(json);
      } catch (e) {
        console.log(`  Page ${page}: Invalid JSON response, skipping`);
        page++;
        await sleep(DELAY_MS);
        continue;
      }

      if (page === 1) {
        totalPages = data.pages || 1;
        console.log(`  Total lawyers: ${(data.cnt || 0).toLocaleString()}, ${totalPages} pages`);
      }

      const items = data.items || [];
      if (items.length === 0) {
        console.log(`  Page ${page}: 0 items (stopping)`);
        break;
      }

      for (const item of items) {
        // Split the name properly for Romanian naming conventions
        const lastName = titleCase((item.nume || '').trim());
        const firstName = titleCase((item.prenume || '').trim());
        const county = (item.judet || '').trim();
        const slug = (item.slug || '').trim();

        leads.push({
          email: '',
          first_name: firstName,
          last_name: lastName,
          company: '',
          phone: '',
          city: county, // judet is the county, closest we have to city from listing
          state: county,
          country: 'Romania',
          source: 'romania_bar',
          _slug: slug, // internal: for profile page enrichment
          _definitiv: item.definitiv, // 1 = definitiv (permanent), 0 = stagiar (trainee)
        });
      }

      if (page % 50 === 0 || page === 1) {
        console.log(`  Page ${page}/${totalPages}: +${items.length} leads (total: ${leads.length})`);
      }

      if (page >= totalPages) {
        console.log(`  Reached last page (${page})`);
        break;
      }

      page++;
      await sleep(DELAY_MS);

    } catch (err) {
      console.log(`  Page ${page}: ERROR — ${err.message}`);
      page++;
      await sleep(DELAY_MS * 2);
      if (page > (totalPages || 500) + 5) break;
    }
  }

  console.log(`  Phase 1 complete: ${leads.length} lawyers from listing`);

  // Phase 2: Optionally enrich from profile pages (email + phone)
  if (ROMANIA_MAX_PROFILES > 0) {
    console.log(`\n  Phase 2: Enriching profiles (max ${ROMANIA_MAX_PROFILES === Infinity ? 'ALL' : ROMANIA_MAX_PROFILES})...`);

    let enriched = 0;
    let emailsFound = 0;
    let phonesFound = 0;

    for (let i = 0; i < leads.length && enriched < ROMANIA_MAX_PROFILES; i++) {
      const lead = leads[i];
      if (!lead._slug) continue;

      try {
        const profileUrl = `https://cautavocat.ro/avocat/${lead._slug}`;
        const html = await fetchWithRetry(profileUrl);

        // Extract phone
        const phoneMatch = html.match(/(?:tel:|phone|telefon)[^>]*>[\s\S]*?([\d\s\-+().]{7,20})/i)
          || html.match(/(0\d{3}[\s.]?\d{3}[\s.]?\d{3})/);
        if (phoneMatch) {
          lead.phone = phoneMatch[1].replace(/\s+/g, ' ').trim();
          phonesFound++;
        }

        // Extract email
        const emailMatch = html.match(/mailto:([^"'\s]+@[^"'\s]+)/i)
          || html.match(/([\w.+-]+@[\w.-]+\.\w{2,})/);
        if (emailMatch) {
          const email = emailMatch[1].toLowerCase().trim();
          if (!email.includes('cautavocat') && !email.includes('unbr.ro')) {
            lead.email = email;
            emailsFound++;
          }
        }

        // Extract address (more specific than county)
        const addrMatch = html.match(/(?:Adresa|adresa|Sediu)[^<]*<[^>]*>([^<]+)/i)
          || html.match(/(?:str\.|Str\.|strada|Strada|Bd\.|bd\.)[^<]*/i);
        if (addrMatch) {
          const addr = stripTags(addrMatch[0]).trim();
          // Try to extract city from address
          const cityMatch = addr.match(/,\s*(?:Jud\.\s*)?([A-ZĂÂÎȘȚăâîșț][a-zăâîșț]+(?:\s+[A-ZĂÂÎȘȚăâîșț][a-zăâîșț]+)?)/);
          if (cityMatch) {
            lead.city = cityMatch[1].trim();
          }
        }

        // Extract firm name
        const firmMatch = html.match(/(?:Cabinet|Societate|SCA|SCPA)[^<]*(?:de Avocat|de Avocați|de avocatură)?[^<]*?([A-ZĂÂÎȘȚ][\wăâîșțĂÂÎȘȚ\s&.-]+)/i);
        if (firmMatch) {
          lead.company = stripTags(firmMatch[0]).trim().substring(0, 100);
        }

        enriched++;
        if (enriched % 100 === 0) {
          console.log(`  Enriched ${enriched}: ${emailsFound} emails, ${phonesFound} phones found`);
        }

        await sleep(PROFILE_DELAY_MS);

      } catch (err) {
        // Profile fetch failed — skip silently
        enriched++;
      }
    }

    console.log(`  Phase 2 complete: enriched ${enriched}, found ${emailsFound} emails, ${phonesFound} phones`);
  }

  // Clean up internal fields
  for (const lead of leads) {
    delete lead._slug;
    delete lead._definitiv;
  }

  console.log(`\n  Romania complete: ${leads.length} lawyers`);
  return leads;
}

// ─── HUNGARY: Regional Bar Association Scraper ────────────────────────────────
//
// The national MÜK search (magyarugyvedikamara.hu) is an iframe/Wix page
// that cannot be scraped via HTTP. Instead, we scrape individual regional
// bar (területi kamara) websites that expose full lawyer listings.
//
// Pécs model: /ugyvedek/aktiv/?limit=200&page={N}
// Fields: name, chamber ID, address, phone, email, specializations, languages
//
// There are 20 regional chambers. Not all have scrapeable websites.
// We target the ones with consistent HTML structures.

const HUNGARY_CHAMBERS = [
  {
    name: 'Pécs',
    baseUrl: 'https://www.pecsiugyvedikamara.hu',
    listPath: '/ugyvedek/aktiv/',
    limitParam: 200,
  },
  // Additional chambers can be added as they're verified to work.
  // Many have SSL issues or different page structures.
  // The Pécs chamber (Baranya county) is the most reliably structured.
];

async function scrapeHungary() {
  console.log('\n========================================');
  console.log('  HUNGARY — Regional Bar Associations');
  console.log('========================================\n');

  const leads = [];

  for (const chamber of HUNGARY_CHAMBERS) {
    console.log(`  --- ${chamber.name} Chamber ---`);

    let page = 1;
    let consecutiveEmpty = 0;

    while (true) {
      const url = `${chamber.baseUrl}${chamber.listPath}?limit=${chamber.limitParam}&page=${page}`;

      try {
        const html = await fetchWithRetry(url);

        // Extract total count on first page
        if (page === 1) {
          const countMatch = html.match(/(\d+)\s*(?:darab\s+)?találat/i);
          if (countMatch) {
            console.log(`  Total active lawyers: ${countMatch[1]}`);
          }
        }

        const pageLeads = parseHungaryPage(html, chamber.name);

        if (pageLeads.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            console.log(`  Page ${page}: 0 results, stopping`);
            break;
          }
        } else {
          consecutiveEmpty = 0;
        }

        for (const lead of pageLeads) {
          leads.push(lead);
        }

        console.log(`  Page ${page}: +${pageLeads.length} leads (total from ${chamber.name}: ${leads.length})`);

        // Check for next page link
        const hasNext = html.includes(`page=${page + 1}`) || html.includes(`>${page + 1}<`);
        if (!hasNext) {
          console.log(`  No more pages after ${page}`);
          break;
        }

        page++;
        await sleep(DELAY_MS);

      } catch (err) {
        console.log(`  Page ${page}: ERROR — ${err.message}`);
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) break;
        page++;
        await sleep(DELAY_MS * 2);
      }
    }
  }

  console.log(`\n  Hungary complete: ${leads.length} lawyers`);
  return leads;
}

/**
 * Parse a Hungarian regional bar association page.
 *
 * The Pécs-style pages list lawyers in blocks with these fields:
 *   - Name (with "Dr." prefix and "Ügyvéd" designation)
 *   - Kamarai azonosító szám (Chamber ID number)
 *   - Cím (Address)
 *   - Telefon (Phone)
 *   - E-mail cím (Email)
 *   - Szakterületek (Practice areas)
 *   - Nyelvtudás (Language skills)
 *
 * We parse these using field label anchors.
 */
function parseHungaryPage(html, chamberName) {
  const leads = [];

  // Strategy: Split the page into lawyer blocks.
  // Each lawyer entry starts with a name (typically "Dr. Firstname Lastname")
  // followed by structured fields.

  // Find all chamber ID numbers as entry anchors
  const idPattern = /(?:Kamarai azonos[ií]t[oó]\s*sz[aá]m|KASZ)[^:]*:\s*(\d{8})/gi;
  const ids = [];
  let idMatch;
  while ((idMatch = idPattern.exec(html)) !== null) {
    ids.push({ id: idMatch[1], index: idMatch.index });
  }

  if (ids.length === 0) {
    // Alternative: try to find entries by "Ügyvéd" markers
    const ugyvedPattern = /(?:Ügyvéd|ügyvéd)\s*(?:<|$)/gi;
    let um;
    while ((um = ugyvedPattern.exec(html)) !== null) {
      ids.push({ id: '', index: um.index });
    }
  }

  // For each ID, extract the surrounding block
  for (let i = 0; i < ids.length; i++) {
    const blockStart = Math.max(0, ids[i].index - 500);
    const blockEnd = i + 1 < ids.length
      ? ids[i + 1].index
      : Math.min(html.length, ids[i].index + 2000);

    const block = html.substring(blockStart, blockEnd);
    const text = stripTags(block);

    // Extract name: Look for "Dr." pattern before the ID
    let fullName = '';
    const nameMatch = text.match(/(?:Dr\.\s+)?([A-ZÁÉÍÓÖŐÚÜŰáéíóöőúüű][a-záéíóöőúüű]+(?:[-\s][A-ZÁÉÍÓÖŐÚÜŰa-záéíóöőúüű]+){1,4})\s+(?:Ügyvéd|ügyvéd|Kamarai)/);
    if (nameMatch) {
      fullName = nameMatch[1].trim();
    } else {
      // Try broader name pattern
      const nameMatch2 = text.match(/(?:Dr\.\s+)([A-ZÁÉÍÓÖŐÚÜŰáéíóöőúüű][\wáéíóöőúüűÁÉÍÓÖŐÚÜŰ\s-]{3,40}?)(?:\s+Ügyvéd|\s+Kamarai|\s+\d{8})/);
      if (nameMatch2) fullName = nameMatch2[1].trim();
    }

    if (!fullName || fullName.length < 3) continue;

    // Hungarian names: "Lastname Firstname" (reverse order from Western)
    const nameParts = fullName.split(/\s+/);
    let lastName = '', firstName = '';
    if (nameParts.length >= 2) {
      lastName = nameParts[0]; // Hungarian convention: surname first
      firstName = nameParts.slice(1).join(' ');
    } else {
      lastName = fullName;
    }

    // Extract phone
    let phone = '';
    const phoneMatch = text.match(/(?:Telefon|Tel\.?|Fax)[^:]*:\s*([\d\s\-+/().]{7,25})/i);
    if (phoneMatch) phone = phoneMatch[1].trim();

    // Extract email
    let email = '';
    const emailMatch = block.match(/mailto:([^"'\s]+@[^"'\s]+)/i)
      || block.match(/([\w.+-]+@[\w.-]+\.\w{2,})/);
    if (emailMatch) {
      email = emailMatch[1].toLowerCase().trim();
    }

    // Extract address/city
    let city = chamberName; // Default to chamber name
    let address = '';
    const addrMatch = text.match(/(?:Cím|C[ií]m)[^:]*:\s*"?(\d{4}\s+[^"]+?)(?:"|Telefon|Tel|E-mail|Szak|$)/i);
    if (addrMatch) {
      address = addrMatch[1].trim();
      // Hungarian addresses: "NNNN City, Street..."
      const cityMatch = address.match(/^\d{4}\s+([^,]+)/);
      if (cityMatch) city = cityMatch[1].trim();
    }

    // Extract firm/office name
    let company = '';
    const firmMatch = text.match(/(?:Iroda|iroda)[^:]*:\s*([^\n]{3,60})/i);
    if (firmMatch) company = firmMatch[1].trim();

    leads.push({
      email,
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      company,
      phone,
      city,
      state: chamberName,
      country: 'Hungary',
      source: 'hungary_bar',
    });
  }

  return leads;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  const startTime = Date.now();
  console.log('=================================================================');
  console.log('  Eastern European Bar Association Scraper');
  console.log('  Poland (KIRP) + Romania (UNBR) + Hungary (Regional Bars)');
  console.log('=================================================================');

  const allLeads = [];

  // Scrape all three countries
  try {
    const polandLeads = await scrapePoland();
    allLeads.push(...polandLeads);
  } catch (err) {
    console.error(`\n  POLAND FAILED: ${err.message}`);
  }

  try {
    const romaniaLeads = await scrapeRomania();
    allLeads.push(...romaniaLeads);
  } catch (err) {
    console.error(`\n  ROMANIA FAILED: ${err.message}`);
  }

  try {
    const hungaryLeads = await scrapeHungary();
    allLeads.push(...hungaryLeads);
  } catch (err) {
    console.error(`\n  HUNGARY FAILED: ${err.message}`);
  }

  // Deduplicate by first_name + last_name + country
  const seen = new Set();
  const deduped = [];
  for (const lead of allLeads) {
    const key = `${lead.first_name}|${lead.last_name}|${lead.country}|${lead.city}`.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  // Write CSV
  const header = 'email,first_name,last_name,company,phone,city,state,country,source';
  const rows = deduped.map(l => [
    esc(l.email), esc(l.first_name), esc(l.last_name), esc(l.company),
    esc(l.phone), esc(l.city), esc(l.state), esc(l.country), esc(l.source),
  ].join(','));

  // Ensure output directory exists
  const outputDir = path.dirname(OUTPUT);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  fs.writeFileSync(OUTPUT, '\ufeff' + header + '\n' + rows.join('\n'), 'utf8');
  // BOM (\ufeff) for Excel compatibility with UTF-8 diacritics

  // Summary statistics
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const byCountry = {};
  for (const l of deduped) {
    byCountry[l.country] = (byCountry[l.country] || 0) + 1;
  }

  const withEmail = deduped.filter(l => l.email).length;
  const withPhone = deduped.filter(l => l.phone).length;

  console.log('\n=================================================================');
  console.log('  SUMMARY');
  console.log('=================================================================');
  console.log(`  Total leads (deduped): ${deduped.length.toLocaleString()}`);
  console.log(`  With email: ${withEmail.toLocaleString()} (${deduped.length ? (withEmail/deduped.length*100).toFixed(1) : 0}%)`);
  console.log(`  With phone: ${withPhone.toLocaleString()} (${deduped.length ? (withPhone/deduped.length*100).toFixed(1) : 0}%)`);
  console.log('');
  for (const [country, count] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${country}: ${count.toLocaleString()}`);
  }
  console.log('');
  console.log(`  Time: ${elapsed} minutes`);
  console.log(`  Output: ${OUTPUT}`);
  console.log('=================================================================\n');
})();
