#!/usr/bin/env node
/**
 * DSAA (Driving School Association of America) Directory Scraper
 *
 * Iterates through detail pages at dsaa.org/Driving-School-Directory/details/[ID]
 * Extracts: school name, email, phone, website, address, city, state
 * Outputs CSV for cold email campaigns
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'dsaa-driving-schools.csv');
const BASE_URL = 'https://dsaa.org/Driving-School-Directory/details/';
const START_ID = 1;
const END_ID = 800;
const DELAY_MS = 1500;
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source,website';

// ─── HTTP fetch ─────────────────────────────────────────────────────
function fetch(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 15000,
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return resolve(null);
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

// ─── Junk email domains to skip ─────────────────────────────────────
const JUNK_EMAIL_DOMAINS = ['morweb.org', 'example.com', 'test.com'];

// ─── US state codes for validation ─────────────────────────────────
const US_STATES = new Set([
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
  'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
  'VA','WA','WV','WI','WY','DC',
]);
const CA_PROVINCES = new Set(['AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT']);

// ─── ZIP code to state mapping (first 3 digits) ────────────────────
// Source: USPS ZIP code prefix ranges
function zipToState(zip) {
  if (!zip) return '';
  const prefix = parseInt(zip.substring(0, 3), 10);
  if (prefix >= 995 && prefix <= 999) return 'AK';
  if (prefix >= 350 && prefix <= 369) return 'AL';
  if (prefix >= 716 && prefix <= 729) return 'AR';
  if (prefix >= 710 && prefix <= 715) return 'AR'; // also AR
  if (prefix >= 850 && prefix <= 865) return 'AZ';
  if (prefix >= 900 && prefix <= 961) return 'CA';
  if (prefix >= 800 && prefix <= 816) return 'CO';
  if (prefix >= 60 && prefix <= 69) return 'CT';
  if (prefix >= 200 && prefix <= 205) return 'DC';
  if (prefix >= 197 && prefix <= 199) return 'DE';
  if (prefix >= 320 && prefix <= 349) return 'FL';
  if (prefix >= 300 && prefix <= 319) return 'GA';
  if (prefix >= 967 && prefix <= 968) return 'HI';
  if (prefix >= 500 && prefix <= 528) return 'IA';
  if (prefix >= 832 && prefix <= 838) return 'ID';
  if (prefix >= 600 && prefix <= 629) return 'IL';
  if (prefix >= 460 && prefix <= 479) return 'IN';
  if (prefix >= 660 && prefix <= 679) return 'KS';
  if (prefix >= 400 && prefix <= 427) return 'KY';
  if (prefix >= 700 && prefix <= 714) return 'LA';
  if (prefix >= 10 && prefix <= 27) return 'MA';
  if (prefix >= 206 && prefix <= 219) return 'MD';
  if (prefix >= 39 && prefix <= 49) return 'ME';
  if (prefix >= 480 && prefix <= 499) return 'MI';
  if (prefix >= 550 && prefix <= 567) return 'MN';
  if (prefix >= 630 && prefix <= 658) return 'MO';
  if (prefix >= 386 && prefix <= 397) return 'MS';
  if (prefix >= 590 && prefix <= 599) return 'MT';
  if (prefix >= 270 && prefix <= 289) return 'NC';
  if (prefix >= 580 && prefix <= 588) return 'ND';
  if (prefix >= 680 && prefix <= 693) return 'NE';
  if (prefix >= 30 && prefix <= 38) return 'NH';
  if (prefix >= 70 && prefix <= 89) return 'NJ';
  if (prefix >= 870 && prefix <= 884) return 'NM';
  if (prefix >= 889 && prefix <= 898) return 'NV';
  if (prefix >= 100 && prefix <= 149) return 'NY';
  if (prefix >= 430 && prefix <= 459) return 'OH';
  if (prefix >= 730 && prefix <= 749) return 'OK';
  if (prefix >= 970 && prefix <= 979) return 'OR';
  if (prefix >= 150 && prefix <= 196) return 'PA';
  if (prefix >= 28 && prefix <= 29) return 'RI';
  if (prefix >= 290 && prefix <= 299) return 'SC';
  if (prefix >= 570 && prefix <= 577) return 'SD';
  if (prefix >= 370 && prefix <= 385) return 'TN';
  if (prefix >= 750 && prefix <= 799) return 'TX';
  if (prefix >= 840 && prefix <= 847) return 'UT';
  if (prefix >= 50 && prefix <= 59) return 'VT';
  if (prefix >= 220 && prefix <= 246) return 'VA';
  if (prefix >= 980 && prefix <= 994) return 'WA';
  if (prefix >= 530 && prefix <= 549) return 'WI';
  if (prefix >= 247 && prefix <= 268) return 'WV';
  if (prefix >= 820 && prefix <= 831) return 'WY';
  return '';
}

// ─── Parse a detail page ────────────────────────────────────────────
function parsePage(html, id) {
  if (!html) return null;

  // School name from h1 banner title
  const nameMatch = html.match(/_js-replace-file-title">\s*([^<]+?)\s*</);
  const name = nameMatch ? nameMatch[1].trim() : '';
  if (!name) return null; // Empty record — skip

  // Email from mailto: link inside the contact-info block (not the footer)
  let email = '';
  const contactBlock = html.match(/contact-info[\s\S]*?checkSocial/);
  if (contactBlock) {
    const emailMatch = contactBlock[0].match(/mailto:([^"]+)/);
    if (emailMatch && emailMatch[1] && emailMatch[1] !== '') {
      const candidate = emailMatch[1].trim().toLowerCase();
      const domain = candidate.split('@')[1] || '';
      // Skip junk/test emails and dsaa.org (the org's own email)
      if (!JUNK_EMAIL_DOMAINS.includes(domain) && domain !== 'dsaa.org') {
        email = candidate;
      }
    }
  }

  // Phone from <span class="isEmpty">
  let phone = '';
  const phoneMatch = html.match(/isPhone[\s\S]*?isEmpty">\s*([^<]*?)\s*<\/span>/);
  if (phoneMatch && phoneMatch[1]) {
    phone = phoneMatch[1].replace(/\D/g, '');
    if (phone.length === 10) {
      phone = `${phone.slice(0,3)}-${phone.slice(3,6)}-${phone.slice(6)}`;
    } else if (phone.length === 11 && phone.startsWith('1')) {
      phone = `${phone.slice(1,4)}-${phone.slice(4,7)}-${phone.slice(7)}`;
    } else if (phone.length === 0) {
      phone = '';
    }
  }

  // Website from isitEmpty link
  let website = '';
  const websiteMatch = html.match(/isitEmpty"><a href="([^"]+)"[^>]*target="_blank"/);
  if (websiteMatch && websiteMatch[1]) {
    website = websiteMatch[1].trim();
    if (website && !website.startsWith('http')) {
      website = 'https://' + website;
    }
  }

  // ─── Address parsing ────────────────────────────────────────────
  // The HTML address line format:
  //   "752 Blanding Blvd. Ste 107-B Orange Park 32065 \n FL US"
  // After collapsing whitespace: "752 Blanding Blvd. Ste 107-B Orange Park 32065 FL US"
  // Many entries have only city + state with no street or zip.
  let city = '', state = '', country = 'US';

  const addrMatch = html.match(/fa-map-marked-alt"><\/i>\s*([\s\S]*?)\s*<\/p>/);
  if (addrMatch) {
    const raw = addrMatch[1].replace(/\s+/g, ' ').trim();

    // Helper: extract city from the street+city portion of the address
    const extractCity = (streetCity) => {
      const parts = streetCity.split(/\s{2,}/);
      if (parts.length >= 2) return parts[parts.length - 1].trim();
      return extractCityFromStreetCity(streetCity);
    };

    let parsed = false;

    // === Strategy 1: US zip + 2-letter state + country suffix ===
    // "752 Blanding Blvd. Ste 107-B Orange Park 32065 FL US"
    const usFullMatch = raw.match(/^(.*?)\s+(\d{5}(?:-\d{4})?)\s+([A-Z]{2})\s+(US|USA)\s*$/i);
    if (usFullMatch) {
      state = usFullMatch[3].toUpperCase();
      country = 'US';
      city = extractCity(usFullMatch[1].trim());
      parsed = true;
    }

    // === Strategy 1b: US zip + 2-letter state code, no country ===
    // "8118-B S Memorial Dr  Tulsa 74133 OK"
    if (!parsed) {
      const usStateMatch = raw.match(/^(.*?)\s+(\d{5}(?:-\d{4})?)\s+([A-Z]{2})\s*$/i);
      if (usStateMatch && US_STATES.has(usStateMatch[3].toUpperCase())) {
        state = usStateMatch[3].toUpperCase();
        country = 'US';
        city = extractCity(usStateMatch[1].trim());
        parsed = true;
      }
    }

    // === Strategy 2: US zip only — derive state from ZIP ===
    // "8118-B S Memorial Dr  Tulsa 74133"
    if (!parsed) {
      const zipOnly = raw.match(/^(.*?)\s+(\d{5}(?:-\d{4})?)\s*$/);
      if (zipOnly) {
        const derivedState = zipToState(zipOnly[2]);
        if (derivedState) {
          state = derivedState;
        }
        city = extractCity(zipOnly[1].trim());
        country = 'US';
        parsed = true;
      }
    }

    // === Strategy 3: Canadian postal code (A1A 1A1) ===
    if (!parsed) {
      const caMatch = raw.match(/^(.*?)\s+([A-Z]\d[A-Z]\s?\d[A-Z]\d)(?:\s+([A-Z]{2}))?(?:\s+(CA|Canada))?\s*$/i);
      if (caMatch) {
        country = 'CA';
        if (caMatch[3] && CA_PROVINCES.has(caMatch[3].toUpperCase())) {
          state = caMatch[3].toUpperCase();
        }
        city = extractCity(caMatch[1].trim());
        parsed = true;
      }
    }

    // === Strategy 4: Fallback — use Region field + raw as city ===
    if (!parsed) {
      const regionMatch = html.match(/Region:<\/strong>\s*<span[^>]*>\s*([^<]*?)\s*<\/span>/);
      if (regionMatch && regionMatch[1]) {
        const regionVal = regionMatch[1].trim().toUpperCase();
        if (US_STATES.has(regionVal)) {
          state = regionVal;
          country = 'US';
        } else if (CA_PROVINCES.has(regionVal)) {
          state = regionVal;
          country = 'CA';
        } else {
          state = regionVal;
        }
      }
      city = extractCityFallback(raw);
    }
  }

  // If state empty, try Region field as last resort
  if (!state) {
    const regionMatch = html.match(/Region:<\/strong>\s*<span[^>]*>\s*([^<]*?)\s*<\/span>/);
    if (regionMatch && regionMatch[1]) {
      state = regionMatch[1].trim().toUpperCase();
    }
  }

  // Clean up state: remove region prefixes like "SOUTHEASTERN ", "WESTERN ", etc.
  if (state && state.length > 2) {
    // Known DSAA region names → map to empty (we have the state in the address already)
    const regionPrefixes = ['SOUTHEASTERN', 'NORTHEASTERN', 'CENTRAL', 'WESTERN', 'EASTERN', 'SOUTHERN', 'NORTHERN'];
    for (const prefix of regionPrefixes) {
      if (state.startsWith(prefix)) {
        // e.g., "SOUTHEASTERN US" → try to keep just the state part
        const remainder = state.replace(prefix, '').trim();
        if (US_STATES.has(remainder)) {
          state = remainder;
        } else if (remainder === 'US' || remainder === 'CA' || remainder === '') {
          state = ''; // Region only, no specific state
        }
        break;
      }
    }
  }

  // Set country based on state if not already set properly
  if (CA_PROVINCES.has(state)) {
    country = 'CA';
  }

  return { name, email, phone, website, city, state, country, id };
}

// ─── City extraction helpers ────────────────────────────────────────

// Given "752 Blanding Blvd. Ste 107-B Orange Park", extract "Orange Park"
// Heuristic: city starts after the last word that looks like a street number/suite
function extractCityFromStreetCity(streetCity) {
  if (!streetCity) return '';

  // Split on double spaces first (the HTML often has double spaces between street and city)
  const dsParts = streetCity.split(/\s{2,}/);
  if (dsParts.length >= 2) {
    return dsParts[dsParts.length - 1].trim();
  }

  // Look for common street suffixes to find where the city starts
  const streetSuffixes = /\b(St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Rd|Road|Way|Lane|Ln|Ct|Court|Pl|Place|Pkwy|Parkway|Hwy|Highway|Circle|Cir|Trail|Trl|Suite|Ste|Unit|Apt|Box|PO)\b/gi;
  let lastSuffixEnd = -1;
  let match;
  while ((match = streetSuffixes.exec(streetCity)) !== null) {
    // Check if there's a suite/unit number after it
    const afterSuffix = streetCity.substring(match.index + match[0].length).trim();
    const suiteNumMatch = afterSuffix.match(/^[#.\-]?\s*\d+\w*/);
    if (suiteNumMatch) {
      lastSuffixEnd = match.index + match[0].length + suiteNumMatch[0].length;
    } else {
      lastSuffixEnd = match.index + match[0].length;
    }
  }

  if (lastSuffixEnd > 0 && lastSuffixEnd < streetCity.length) {
    const city = streetCity.substring(lastSuffixEnd).trim();
    if (city) return city;
  }

  // Fallback: just take the last 1-2 words
  const words = streetCity.split(/\s+/);
  if (words.length >= 4) {
    // Check if last two words look like a city (both start with uppercase letter)
    const last = words[words.length - 1];
    const secondLast = words[words.length - 2];
    if (/^[A-Z]/.test(last) && /^[A-Z]/.test(secondLast) && !/^\d/.test(secondLast)) {
      return secondLast + ' ' + last;
    }
    return last;
  } else if (words.length >= 2) {
    return words[words.length - 1];
  }
  return streetCity;
}

// Fallback city extraction from a raw address with no structured zip/state
function extractCityFallback(raw) {
  if (!raw) return '';
  // If there's a zip code, take the word(s) before it
  const beforeZip = raw.match(/^(.*?)\s+\d{5}/);
  if (beforeZip) {
    return extractCityFromStreetCity(beforeZip[1].trim());
  }
  // Canadian postal code
  const beforePostal = raw.match(/^(.*?)\s+[A-Z]\d[A-Z]/i);
  if (beforePostal) {
    return extractCityFromStreetCity(beforePostal[1].trim());
  }
  // No zip at all — just return the raw (it's probably just a city name)
  return raw;
}

// ─── CSV escaping ───────────────────────────────────────────────────
function csvEscape(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function toCSVRow(rec) {
  // header: email,first_name,last_name,company,phone,city,state,country,source,website
  return [
    csvEscape(rec.email),
    '', // first_name — not available from directory
    '', // last_name — not available from directory
    csvEscape(rec.name),
    csvEscape(rec.phone),
    csvEscape(rec.city),
    csvEscape(rec.state),
    csvEscape(rec.country || 'US'),
    'DSAA Directory',
    csvEscape(rec.website),
  ].join(',');
}

// ─── Delay helper ───────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Main ───────────────────────────────────────────────────────────
async function main() {
  console.log('=== DSAA Driving School Directory Scraper ===');
  console.log(`Scanning IDs ${START_ID} to ${END_ID} with ${DELAY_MS}ms delay`);
  console.log(`Output: ${OUTPUT_FILE}\n`);

  // Ensure output dir exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Write CSV header
  fs.writeFileSync(OUTPUT_FILE, CSV_HEADER + '\n');

  let total = 0;
  let withEmail = 0;
  let withPhone = 0;
  let withWebsite = 0;
  let errors = 0;
  let empty = 0;

  for (let id = START_ID; id <= END_ID; id++) {
    const url = BASE_URL + id;

    try {
      const html = await fetch(url);
      const rec = parsePage(html, id);

      if (!rec) {
        empty++;
        if (id % 100 === 0 || id <= 5) {
          process.stdout.write(`  [${id}/${END_ID}] empty\r`);
        }
      } else {
        total++;
        if (rec.email) withEmail++;
        if (rec.phone) withPhone++;
        if (rec.website) withWebsite++;

        fs.appendFileSync(OUTPUT_FILE, toCSVRow(rec) + '\n');

        const flags = [
          rec.email ? 'E' : '-',
          rec.phone ? 'P' : '-',
          rec.website ? 'W' : '-',
        ].join('');
        console.log(`  [${id}/${END_ID}] [${flags}] ${rec.name} — ${rec.city}, ${rec.state}`);
      }
    } catch (err) {
      errors++;
      console.log(`  [${id}/${END_ID}] ERROR: ${err.message}`);
    }

    // Rate limit
    if (id < END_ID) {
      await sleep(DELAY_MS);
    }
  }

  console.log('\n=== SCRAPE COMPLETE ===');
  console.log(`Total records: ${total}`);
  console.log(`With email:    ${withEmail}`);
  console.log(`With phone:    ${withPhone}`);
  console.log(`With website:  ${withWebsite}`);
  console.log(`Empty pages:   ${empty}`);
  console.log(`Errors:        ${errors}`);
  console.log(`Output:        ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
