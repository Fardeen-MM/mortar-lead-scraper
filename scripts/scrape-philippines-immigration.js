#!/usr/bin/env node
/**
 * Scrape Philippines licensed recruitment agencies from WorkAbroad.ph
 *
 * Source: workabroad.ph/agency-directory (A-Z + 0-9)
 * Data: DMW (formerly POEA) licensed recruitment agencies
 *
 * Phase 1: Scrape directory listing pages (A-Z + 0-9) for:
 *   - Agency name, license number, profile URL
 *
 * Phase 2: Visit each profile page to extract:
 *   - License validity dates
 *   - About text (may contain address, specializations)
 *
 * Rate limit: 2-3s between requests
 * Output: output/philippines-recruitment-agencies.csv
 *
 * Usage:
 *   node scripts/scrape-philippines-immigration.js
 *   node scripts/scrape-philippines-immigration.js --test          # Only letter A
 *   node scripts/scrape-philippines-immigration.js --no-profiles   # Skip profile pages
 *   node scripts/scrape-philippines-immigration.js --resume        # Resume from progress file
 *   node scripts/scrape-philippines-immigration.js --letter=M      # Start from letter M
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// Parse CLI args
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE = !!args.test;
const SKIP_PROFILES = !!args['no-profiles'];
const RESUME = !!args.resume;
const START_LETTER = args.letter || null;
const DELAY_MS = parseInt(args.delay) || 2500;         // 2.5s between requests
const PROFILE_DELAY_MS = parseInt(args['profile-delay']) || 2000; // 2s between profile fetches
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'philippines-recruitment-agencies.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'ph-recruitment-progress.csv');

const BASE_URL = 'https://www.workabroad.ph';
const DIRECTORY_URL = `${BASE_URL}/agency-directory`;

// All letter tabs: 0-9, A-Z
const LETTERS = ['0-9', ...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')];

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'registration_number', 'license_valid_from',
  'license_valid_to', 'specializations', 'address',
];

// --- HTTP helpers ---

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': BASE_URL,
      },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        fetchUrl(redirectUrl).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => reject(new Error(`HTTP ${res.statusCode}: ${url}`)));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms + Math.random() * 1000));
}

// --- CSV helpers ---

function escapeCSV(val) {
  const str = (val || '').toString().trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function readExistingProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return [];
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return [];
    const headers = lines[0].split(',');
    const leads = [];
    for (let i = 1; i < lines.length; i++) {
      // Simple CSV parse - handle quoted fields
      const vals = [];
      let current = '';
      let inQuote = false;
      for (const ch of lines[i]) {
        if (ch === '"') { inQuote = !inQuote; continue; }
        if (ch === ',' && !inQuote) { vals.push(current); current = ''; continue; }
        current += ch;
      }
      vals.push(current);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
    }
    return leads;
  } catch (e) {
    console.log(`  Warning: could not read progress file: ${e.message}`);
    return [];
  }
}

// --- Parsers ---

/**
 * Parse the directory listing page (A-Z index).
 * Each page has a <table> with rows:
 *   <tr>
 *     <td><a href="profile_url"><img .../></a></td>
 *     <td><a href="profile_url" class="font-weight-bold word-link">AGENCY NAME</a></td>
 *     <td>LICENSE-NUMBER</td>
 *   </tr>
 */
function parseDirectoryPage(html) {
  const $ = cheerio.load(html);
  const agencies = [];

  $('table.table tbody tr').each((_, row) => {
    const tds = $(row).find('td');
    if (tds.length < 3) return;

    const nameLink = $(tds[1]).find('a.word-link');
    const name = nameLink.text().trim();
    let profileUrl = nameLink.attr('href') || '';
    const license = $(tds[2]).text().trim();

    if (!name) return;

    // Ensure full URL
    if (profileUrl && !profileUrl.startsWith('http')) {
      profileUrl = `${BASE_URL}${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`;
    }

    agencies.push({
      firm_name: name,
      registration_number: license,
      profile_url: profileUrl,
    });
  });

  return agencies;
}

/**
 * Parse an agency profile page for additional details.
 * Extracts: license validity dates, about text, address, specializations.
 */
function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const data = {};

  // License validity: "License Valid from <strong>Apr 30, 2025</strong> to <strong>Mar 04, 2031</strong>"
  const licenseText = $('span:contains("License Valid from")').text().trim();
  const validMatch = licenseText.match(/License Valid from\s+(\w+ \d+, \d{4})\s+to\s+(\w+ \d+, \d{4})/);
  if (validMatch) {
    data.license_valid_from = validMatch[1];
    data.license_valid_to = validMatch[2];
  }

  // About company section — extract text
  const aboutSection = $('h3:contains("About Company")').closest('.p-4, .card-body').find('.Section1, .Section2, .MsoNormal');
  let aboutText = '';
  if (aboutSection.length) {
    aboutText = aboutSection.text().replace(/\s+/g, ' ').trim();
  } else {
    // Fallback: grab text from the about card
    const aboutCard = $('h3:contains("About Company")').parent();
    if (aboutCard.length) {
      aboutText = aboutCard.text().replace(/About Company/i, '').replace(/\s+/g, ' ').trim();
    }
  }

  // Try to extract address from about text
  // Look for specific street address patterns (number + street name + city)
  const streetMatch = aboutText.match(/(\d+[A-Za-z]?\s+(?:(?:st|nd|rd|th)\s+)?(?:floor\s+)?[\w\s.,#-]+(?:Street|St\.|Avenue|Ave\.|Blvd|Boulevard|Road|Rd\.|Drive|Dr\.)[^.]*(?:Manila|Makati|Quezon|Pasay|Pasig|Mandaluyong|Parañaque|Paranaque|Caloocan|Taguig|Malate|Ermita)[^.]*)/i);
  if (streetMatch) {
    let addr = streetMatch[1].replace(/^[\s,]+|[\s,]+$/g, '').trim();
    // Cap at 200 chars to avoid narrative bleed
    if (addr.length > 200) addr = addr.substring(0, 200).replace(/[,\s]+$/, '');
    data.address = addr;
  }

  // Try to extract city from address or about text
  const phCities = [
    'Manila', 'Makati', 'Quezon City', 'Pasay', 'Pasig', 'Mandaluyong',
    'Parañaque', 'Paranaque', 'Caloocan', 'Taguig', 'Las Piñas', 'Las Pinas',
    'Marikina', 'Muntinlupa', 'Navotas', 'Pateros', 'San Juan', 'Valenzuela',
    'Ortigas', 'Malate', 'Ermita', 'Intramuros', 'Binondo', 'Sampaloc',
    'Cubao', 'Alabang', 'BGC', 'Bonifacio Global City',
    'Cebu', 'Davao', 'Iloilo', 'Bacolod', 'Zamboanga', 'Cagayan de Oro',
    'General Santos', 'Baguio', 'Angeles', 'Olongapo', 'Subic',
  ];
  for (const city of phCities) {
    if (aboutText.toLowerCase().includes(city.toLowerCase())) {
      data.city = city;
      break;
    }
  }

  // Extract specializations from Section2 list items
  const specs = [];
  $('.Section2 .MsoNormal, .Section2 p').each((_, el) => {
    let text = $(el).text().replace(/[*•\s]+/g, ' ').trim();
    // Strip MSO comment artifacts
    text = text.replace(/<!--\[if[^\]]*\]-->/g, '').replace(/<!--\[endif\]-->/g, '').trim();
    // Filter out junk
    if (text.length > 3 && text.length < 100) {
      specs.push(text);
    }
  });
  if (specs.length) {
    data.specializations = specs.join('; ');
  }

  // Try to find phone in about text
  // Match PH phone formats: (02) 8xxx-xxxx, 09xx-xxx-xxxx, (0xx) xxx-xxxx, +63 xxx xxx xxxx
  const phoneMatch = aboutText.match(/(?:phone|tel|telephone|mobile|contact(?:\s+(?:no|number|#))?)[:\s]*((?:\+63|0)[\d()\s.-]{7,18})/i);
  if (phoneMatch) {
    // Take only the first phone number (up to first space gap after 7+ digits)
    let phone = phoneMatch[1].replace(/[\s.-]+/g, '').trim();
    // If we captured multiple numbers concatenated, take first valid one
    const phoneNums = phone.match(/(?:\+63|0)\d{7,12}/);
    if (phoneNums) phone = phoneNums[0];
    if (phone.length >= 7 && phone.length <= 15) {
      data.phone = phone;
    }
  }

  // Try to find email in about text
  const emailMatch = aboutText.match(/[\w._%+-]+@[\w.-]+\.\w{2,}/);
  if (emailMatch) {
    data.email = emailMatch[0];
  }

  // Try to find website in about text
  const websiteMatch = aboutText.match(/(?:website|site|web|visit)[:\s]*(https?:\/\/[^\s,)]+|www\.[^\s,)]+)/i);
  if (websiteMatch) {
    let url = websiteMatch[1];
    if (!url.startsWith('http')) url = 'https://' + url;
    data.website = url;
  }

  return data;
}

// --- Main ---

async function main() {
  const startTime = Date.now();

  console.log('\n+-----------------------------------------------------------+');
  console.log('|  MORTAR -- Philippines Recruitment Agencies Scrape         |');
  console.log('|  Source: WorkAbroad.ph (DMW-licensed agencies)             |');
  console.log('+-----------------------------------------------------------+\n');

  let letters = [...LETTERS];
  if (TEST_MODE) {
    letters = ['A'];
    console.log('  Mode:    TEST (letter A only)');
  } else if (START_LETTER) {
    const idx = letters.indexOf(START_LETTER.toUpperCase());
    if (idx >= 0) {
      letters = letters.slice(idx);
      console.log(`  Start:   Letter ${START_LETTER.toUpperCase()}`);
    }
  }

  console.log(`  Letters: ${letters.length} (${letters[0]} - ${letters[letters.length - 1]})`);
  console.log(`  Delay:   ${DELAY_MS}ms (directory), ${PROFILE_DELAY_MS}ms (profiles)`);
  console.log(`  Profiles: ${SKIP_PROFILES ? 'SKIP' : 'YES'}`);
  console.log(`  Output:  ${OUTPUT_FILE}`);
  console.log('');

  // Load existing progress for resume
  let allAgencies = [];
  const seenNames = new Set();
  if (RESUME) {
    allAgencies = readExistingProgress();
    allAgencies.forEach(a => seenNames.add(a.firm_name));
    console.log(`  Resumed: ${allAgencies.length} agencies from progress file`);
  }

  // ============================
  // PHASE 1: Directory listings
  // ============================
  console.log('\n  --- Phase 1: Directory Listings ---\n');

  let newFromDirectory = 0;

  for (let i = 0; i < letters.length; i++) {
    const letter = letters[i];
    const url = `${DIRECTORY_URL}/${letter}`;
    const tag = `[${i + 1}/${letters.length}]`;

    try {
      const html = await fetchUrl(url);
      const agencies = parseDirectoryPage(html);

      let newCount = 0;
      for (const agency of agencies) {
        if (seenNames.has(agency.firm_name)) continue;
        seenNames.add(agency.firm_name);

        allAgencies.push({
          first_name: '',
          last_name: '',
          firm_name: agency.firm_name,
          title: '',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: '',
          state: '',
          country: 'PH',
          niche: 'Recruitment Agency',
          source: 'ph-workabroad',
          profile_url: agency.profile_url,
          registration_number: agency.registration_number,
          license_valid_from: '',
          license_valid_to: '',
          specializations: '',
          address: '',
        });
        newCount++;
        newFromDirectory++;
      }

      console.log(`  ${tag} Letter ${letter.padEnd(3)} => ${agencies.length} agencies (${newCount} new)  [total: ${allAgencies.length}]`);

    } catch (err) {
      console.log(`  ${tag} Letter ${letter} => ERROR: ${err.message}`);
    }

    // Save progress periodically
    if (allAgencies.length > 0 && i % 5 === 4) {
      writeCSV(PROGRESS_FILE, allAgencies);
    }

    if (i < letters.length - 1) {
      await delay(DELAY_MS);
    }
  }

  // Save after directory phase
  writeCSV(PROGRESS_FILE, allAgencies);
  console.log(`\n  Directory phase complete: ${allAgencies.length} agencies`);
  console.log(`  New from directory: ${newFromDirectory}`);

  // ============================
  // PHASE 2: Profile pages
  // ============================
  if (!SKIP_PROFILES) {
    console.log('\n  --- Phase 2: Profile Pages ---\n');

    // Only visit profiles we haven't enriched yet
    const toEnrich = allAgencies.filter(a => !a.license_valid_from && a.profile_url);
    console.log(`  Profiles to visit: ${toEnrich.length}`);
    console.log('');

    let enriched = 0;
    let errors = 0;
    let withCity = 0;
    let withSpecs = 0;

    for (let i = 0; i < toEnrich.length; i++) {
      const agency = toEnrich[i];
      const tag = `[${i + 1}/${toEnrich.length}]`;

      try {
        const html = await fetchUrl(agency.profile_url);
        const profileData = parseProfilePage(html);

        // Merge profile data into agency record
        if (profileData.license_valid_from) agency.license_valid_from = profileData.license_valid_from;
        if (profileData.license_valid_to) agency.license_valid_to = profileData.license_valid_to;
        if (profileData.address) agency.address = profileData.address;
        if (profileData.city) { agency.city = profileData.city; withCity++; }
        if (profileData.specializations) { agency.specializations = profileData.specializations; withSpecs++; }
        if (profileData.phone) agency.phone = profileData.phone;
        if (profileData.email) agency.email = profileData.email;
        if (profileData.website) {
          agency.website = profileData.website;
          try {
            agency.domain = new URL(profileData.website).hostname.replace('www.', '');
          } catch {}
        }

        enriched++;

        // Progress logging every 25 or for first 5
        if (i < 5 || (i + 1) % 25 === 0 || i === toEnrich.length - 1) {
          const extras = [];
          if (profileData.city) extras.push(`city=${profileData.city}`);
          if (profileData.specializations) extras.push('specs');
          if (profileData.phone) extras.push('phone');
          if (profileData.email) extras.push('email');
          const extraStr = extras.length ? ` (${extras.join(', ')})` : '';
          console.log(`  ${tag} ${agency.firm_name.substring(0, 50)}${extraStr}`);
        }

      } catch (err) {
        errors++;
        if (errors <= 5 || errors % 20 === 0) {
          console.log(`  ${tag} ERROR: ${agency.firm_name.substring(0, 40)} - ${err.message}`);
        }
      }

      // Save progress every 100 profiles
      if ((i + 1) % 100 === 0) {
        writeCSV(PROGRESS_FILE, allAgencies);
        console.log(`    ... saved progress (${i + 1}/${toEnrich.length})`);
      }

      if (i < toEnrich.length - 1) {
        await delay(PROFILE_DELAY_MS);
      }
    }

    console.log(`\n  Profile phase complete:`);
    console.log(`    Enriched:   ${enriched}`);
    console.log(`    Errors:     ${errors}`);
    console.log(`    With city:  ${withCity}`);
    console.log(`    With specs: ${withSpecs}`);
  }

  // ============================
  // Final output
  // ============================
  writeCSV(OUTPUT_FILE, allAgencies);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const withLicense = allAgencies.filter(a => a.registration_number).length;
  const withCity = allAgencies.filter(a => a.city).length;
  const withEmail = allAgencies.filter(a => a.email).length;
  const withPhone = allAgencies.filter(a => a.phone).length;
  const withSpecs = allAgencies.filter(a => a.specializations).length;
  const withAddress = allAgencies.filter(a => a.address).length;

  console.log('\n+-----------------------------------------------------------+');
  console.log('|  RESULTS                                                  |');
  console.log('+-----------------------------------------------------------+');
  console.log(`  Total agencies:    ${allAgencies.length}`);
  console.log(`  With license #:    ${withLicense}`);
  console.log(`  With city:         ${withCity}`);
  console.log(`  With address:      ${withAddress}`);
  console.log(`  With email:        ${withEmail}`);
  console.log(`  With phone:        ${withPhone}`);
  console.log(`  With specializations: ${withSpecs}`);
  console.log(`  Time:              ${elapsed}s`);
  console.log(`  Output:            ${OUTPUT_FILE}`);
  console.log('+-----------------------------------------------------------+\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
