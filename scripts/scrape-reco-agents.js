#!/usr/bin/env node
/**
 * Scrape ALL registered real estate agents from RECO (Real Estate Council of Ontario).
 *
 * Source: https://registrantsearch.reco.on.ca/
 * Method: POST form search by 2-letter last name prefixes (Aa-Zz), with auto-split
 *         to 3-letter prefixes when a 2-letter prefix returns 2000+ results.
 *         Dedup by Registration Number. Filter to REGISTERED status only.
 *
 * Expected: ~100,000+ active salespersons and brokers across Ontario
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, fs, path, querystring).
 * HTML parsing done with regex.
 *
 * Features:
 *   - 2-letter last name prefix iteration (676 combinations)
 *   - Auto-splits to 3-letter prefixes when results >= SPLIT_THRESHOLD
 *   - Deduplication by registration_number
 *   - Auto-saves progress CSV every 500 new leads
 *   - Rate limiting (configurable, default 500ms)
 *   - Segment support for parallel execution (--segment=A-F, G-L, M-R, S-Z)
 *   - Resume support (reads existing progress file)
 *   - City extraction from brokerage address
 *   - Domain extraction from brokerage email
 *
 * Usage:
 *   node scripts/scrape-reco-agents.js
 *   node scripts/scrape-reco-agents.js --segment=A-F
 *   node scripts/scrape-reco-agents.js --segment=G-L
 *   node scripts/scrape-reco-agents.js --segment=M-R
 *   node scripts/scrape-reco-agents.js --segment=S-Z
 *   node scripts/scrape-reco-agents.js --resume
 *   node scripts/scrape-reco-agents.js --delay=500
 *   node scripts/scrape-reco-agents.js --test   (only runs prefix "sm")
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 500;
const TEST_MODE = !!args.test;
const SEGMENT = (args.segment || '').toUpperCase();
const SPLIT_THRESHOLD = 2000; // If a 2-letter prefix returns >= this, split into 3-letter
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR,
  SEGMENT ? `reco-segment-${SEGMENT}.csv` : 'reco-real-estate-agents.csv'
);

// ── Segment letter ranges ───────────────────────────────────────────────
const SEGMENTS = {
  'A-F': 'ABCDEF',
  'G-L': 'GHIJKL',
  'M-R': 'MNOPQR',
  'S-Z': 'STUVWXYZ',
};

function getLetterRange() {
  if (TEST_MODE) return ['S']; // Only 'S' in test mode
  if (SEGMENT && SEGMENTS[SEGMENT]) return SEGMENTS[SEGMENT].split('');
  return 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
}

function buildPrefixes() {
  const letters = getLetterRange();
  const seconds = 'abcdefghijklmnopqrstuvwxyz'.split('');
  const prefixes = [];
  for (const first of letters) {
    if (TEST_MODE) {
      // In test mode, only first 2 prefixes (Sm, Sn)
      prefixes.push(first.toLowerCase() + 'm', first.toLowerCase() + 'n');
    } else {
      for (const second of seconds) {
        prefixes.push(first.toLowerCase() + second);
      }
    }
  }
  return prefixes;
}

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'registration_number',
];

// ── HTTP POST helper ────────────────────────────────────────────────────
function postSearch(lastName) {
  return new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      IsTerminated: 'false',
      FirstName: '',
      MiddleName: '',
      LastName: lastName,
      envValue: '',
      action: 'searchSalesperson',
    });

    const options = {
      hostname: 'registrantsearch.reco.on.ca',
      path: '/RegistrantSearch/Salesperson',
      method: 'POST',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://registrantsearch.reco.on.ca',
        'Referer': 'https://registrantsearch.reco.on.ca/',
      },
    };

    const req = https.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = new URL(res.headers.location, 'https://registrantsearch.reco.on.ca');
        https.get(redirectUrl.href, {
          headers: { 'User-Agent': options.headers['User-Agent'] },
        }, (res2) => {
          let data = '';
          res2.on('data', chunk => data += chunk);
          res2.on('end', () => resolve(data));
        }).on('error', reject);
        return;
      }

      if (res.statusCode !== 200) {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => reject(new Error(`HTTP ${res.statusCode} for "${lastName}"`)));
        return;
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });

    req.on('error', reject);
    req.setTimeout(120000, () => {
      req.destroy();
      reject(new Error(`Timeout fetching prefix "${lastName}"`));
    });
    req.write(postData);
    req.end();
  });
}

// ── Extract result count from HTML ──────────────────────────────────────
function extractResultCount(html) {
  const m = html.match(/(\d+)\s+results?\s+based\s+on\s+your\s+search/i);
  return m ? parseInt(m[1], 10) : 0;
}

// ── Parse results HTML using regex (no cheerio) ─────────────────────────
function parseResults(html) {
  const leads = [];

  // Check for no results
  if (html.includes('No results for Salesperson/Broker')) {
    return leads;
  }

  // Split HTML into individual cards.
  // Each card starts with <div class="card mt-2"> and ends with </div><!-- /card -->
  const cardRegex = /<div class="card mt-2">[\s\S]*?<\/div><!-- \/card -->/g;
  let cardMatch;

  while ((cardMatch = cardRegex.exec(html)) !== null) {
    const card = cardMatch[0];

    // Extract all <strong>Label:</strong> Value pairs
    // Pattern: <strong>Field Name :</strong> followed by value text
    const fields = {};
    const rawFields = {}; // Keep raw (non-collapsed) values for address parsing
    const fieldRegex = /<strong>([^<]+?)\s*:?\s*<\/strong>\s*([\s\S]*?)(?=<\/p>|<strong>)/g;
    let fMatch;

    while ((fMatch = fieldRegex.exec(card)) !== null) {
      const label = fMatch[1].replace(/\s*:\s*$/, '').trim();
      const rawValue = fMatch[2]
        .replace(/<[^>]*>/g, '')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#x27;/g, "'")
        .replace(/&#39;/g, "'")
        .trim();
      const value = rawValue.replace(/\s+/g, ' ').trim();
      if (label && value) {
        fields[label] = value;
        rawFields[label] = rawValue;
      }
    }

    // Only keep REGISTERED agents
    const status = (fields['Registration Status'] || '').toUpperCase();
    if (!status.includes('REGISTERED')) continue;

    // Parse legal name into first/last
    const legalName = fields['Legal Name'] || '';
    const nameParts = legalName.trim().split(/\s+/).filter(Boolean);
    let firstName = '', lastName = '';
    if (nameParts.length === 1) {
      lastName = nameParts[0];
    } else if (nameParts.length === 2) {
      firstName = nameParts[0];
      lastName = nameParts[1];
    } else if (nameParts.length >= 3) {
      // First name = first part, last name = last part, middle parts ignored
      firstName = nameParts[0];
      lastName = nameParts[nameParts.length - 1];
    }

    // Title from registration category
    const category = fields['Registration Category'] || '';
    let title = '';
    if (category === 'Salesperson') title = 'Real Estate Salesperson';
    else if (category === 'Broker') title = 'Real Estate Broker';
    else if (category.includes('Brokerage')) title = 'Brokerage';
    else title = category;

    // Brokerage info — prefer Trade Name over legal Name
    const tradeName = (fields['Brokerage Trade Name'] || '').trim();
    const brokerageLegalName = (fields['Brokerage Name'] || '').trim();
    const firmName = tradeName || brokerageLegalName;

    const brokerageEmail = (fields['Brokerage Email'] || '').trim().toLowerCase();
    const brokeragePhone = (fields['Brokerage Phone'] || '').trim();
    const brokerageAddress = (fields['Brokerage Address'] || '').trim();
    const regNumber = (fields['Registration Number'] || '').trim();

    if (!regNumber) continue; // Skip if no reg number (shouldn't happen)

    // Extract city from address using raw (non-collapsed) value
    // Address format: "STREET Unit: X  CITY, ON POSTAL Canada" (double/triple space before city)
    // Or: "STREET   CITY, ON POSTAL Canada" (no unit)
    let city = '';
    const rawAddress = rawFields['Brokerage Address'] || brokerageAddress;
    const onIdx = rawAddress.indexOf(', ON ');
    if (onIdx > 0) {
      // Find the double-space (or more) delimiter before the city
      // Look backwards from ", ON" for 2+ consecutive spaces
      const beforeOn = rawAddress.substring(0, onIdx);
      const dblSpaceIdx = beforeOn.lastIndexOf('  ');
      if (dblSpaceIdx > 0) {
        city = beforeOn.substring(dblSpaceIdx).trim();
      } else {
        // No double space found — take last word(s) that look like a city name
        // Fallback: everything after the last digit sequence
        const lastDigitMatch = beforeOn.match(/^.*\d\s+(.+)$/);
        if (lastDigitMatch) {
          city = lastDigitMatch[1].trim();
        }
      }
      // Clean up: remove leading floor/unit artifacts
      // e.g., "20th Floor Toronto" -> "Toronto", "Main Floor Toronto" -> "Toronto"
      // e.g., "100 MAIN FLOOR TORONTO" -> "Toronto", "2600 BAY ADELAIDE CENTRE EAST TOWER TORONTO" -> "Toronto"
      city = city.replace(/^\d+\w*\s+Floor\s+/i, '').trim();
      city = city.replace(/^(?:Main|Ground|Second|Third|Fourth|Fifth|Upper|Lower|Mezzanine)\s+Floor\s+/i, '').trim();
      city = city.replace(/^(?:\d+\s+)?(?:[A-Z]+\s+)*(?:TOWER|CENTRE|CENTER|PLAZA|BUILDING|COMPLEX)\s+/i, '').trim();
      // Remove P.O BOX prefixes
      city = city.replace(/^P\.?O\.?\s*BOX\s+\d+\s*/i, '').trim();
      // Remove stray leading numbers (unit numbers that leaked in)
      city = city.replace(/^\d+\s+/, '').trim();
      city = titleCase(city);
    }

    // Extract domain from email
    let domain = '';
    if (brokerageEmail && brokerageEmail.includes('@')) {
      domain = brokerageEmail.split('@')[1];
    }

    // Build website from domain
    let website = '';
    if (domain) {
      website = 'https://' + domain;
    }

    leads.push({
      first_name: titleCase(firstName),
      last_name: titleCase(lastName),
      firm_name: firmName,
      title: title,
      email: brokerageEmail,
      phone: normalizePhone(brokeragePhone),
      website: website,
      domain: domain,
      city: city,
      state: 'Ontario',
      country: 'CA',
      niche: 'real estate agent',
      source: 'reco',
      registration_number: regNumber,
    });
  }

  return leads;
}

// ── Helpers ─────────────────────────────────────────────────────────────
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|\s|[-'])\w/g, c => c.toUpperCase());
}

function normalizePhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return phone;
}

function escapeCSV(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
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

function loadExistingLeads() {
  if (!fs.existsSync(PROGRESS_FILE)) return { leads: [], seenRegNums: new Set() };

  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return { leads: [], seenRegNums: new Set() };

    const headers = lines[0].split(',');
    const regIdx = headers.indexOf('registration_number');
    const leads = [];
    const seenRegNums = new Set();

    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSVLine(lines[i]);
      const lead = {};
      headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
      leads.push(lead);
      if (regIdx >= 0 && vals[regIdx]) seenRegNums.add(vals[regIdx]);
    }

    return { leads, seenRegNums };
  } catch {
    return { leads: [], seenRegNums: new Set() };
  }
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
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

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  const prefixes = buildPrefixes();

  // Load existing progress for resume
  let { leads: allLeads, seenRegNums } = args.resume
    ? loadExistingLeads()
    : { leads: [], seenRegNums: new Set() };

  if (args.resume && allLeads.length > 0) {
    console.log(`  Resuming with ${allLeads.length} existing leads (${seenRegNums.size} unique reg numbers)`);
  }

  const segmentLabel = SEGMENT || 'ALL';
  const modeLabel = TEST_MODE ? ' (TEST MODE)' : '';

  console.log('');
  console.log('=============================================================');
  console.log(`  MORTAR -- RECO Real Estate Agent Scraper${modeLabel}`);
  console.log('=============================================================');
  console.log('');
  console.log(`  Segment:     ${segmentLabel}`);
  console.log(`  Prefixes:    ${prefixes.length} (${prefixes[0]} -> ${prefixes[prefixes.length - 1]})`);
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Split at:    ${SPLIT_THRESHOLD}+ results -> 3-letter prefixes`);
  console.log(`  Output:      ${PROGRESS_FILE}`);
  console.log('');

  let totalRequests = 0;
  let newLeads = 0;
  let duplicates = 0;
  let errors = 0;
  let consecutiveErrors = 0;
  let splitCount = 0;
  let withEmail = allLeads.filter(l => l.email).length;
  let withPhone = allLeads.filter(l => l.phone).length;
  let lastSaveCount = 0;

  async function processPrefix(prefix, depth) {
    totalRequests++;

    try {
      const html = await postSearch(prefix);
      consecutiveErrors = 0;

      const resultCount = extractResultCount(html);

      // If too many results and we haven't already split, try 3-letter prefixes
      if (resultCount >= SPLIT_THRESHOLD && prefix.length < 3) {
        splitCount++;
        console.log(`  [SPLIT] "${prefix}" has ${resultCount} results -> splitting into 3-letter prefixes`);
        const thirdLetters = 'abcdefghijklmnopqrstuvwxyz'.split('');
        for (const third of thirdLetters) {
          await processPrefix(prefix + third, depth + 1);
          await sleep(DELAY_MS);
        }
        // Also search the exact 2-letter prefix for names that are exactly 2 chars
        // (already covered by 3-letter combos since they match prefix)
        return;
      }

      const leads = parseResults(html);
      let prefixNew = 0;
      let prefixDup = 0;

      for (const lead of leads) {
        if (seenRegNums.has(lead.registration_number)) {
          prefixDup++;
          duplicates++;
          continue;
        }
        seenRegNums.add(lead.registration_number);
        allLeads.push(lead);
        newLeads++;
        prefixNew++;
        if (lead.email) withEmail++;
        if (lead.phone) withPhone++;
      }

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rate = totalRequests > 0 ? (totalRequests / (elapsed || 1) * 60).toFixed(0) : '?';

      if (leads.length > 0) {
        const indent = depth > 0 ? '    ' : '  ';
        console.log(`${indent}[${totalRequests}] "${prefix}" -> ${resultCount} results (${prefixNew} new, ${prefixDup} dup) | Total: ${allLeads.length} | ${rate} req/min`);
      } else if (totalRequests % 20 === 0) {
        // Log periodically even when empty to show progress
        console.log(`  [${totalRequests}] "${prefix}" -> 0 results | Total: ${allLeads.length} | ${rate} req/min`);
      }

      // Save progress every 500 new leads
      if (newLeads > 0 && newLeads - lastSaveCount >= 500) {
        writeCSV(PROGRESS_FILE, allLeads);
        lastSaveCount = newLeads;
        console.log(`  [SAVE] ${allLeads.length} leads saved to ${path.basename(PROGRESS_FILE)}`);
      }

    } catch (err) {
      errors++;
      consecutiveErrors++;
      console.log(`  [ERR] "${prefix}": ${err.message}`);

      if (consecutiveErrors >= 5) {
        console.log('  [PAUSE] 5 consecutive errors. Waiting 30 seconds...');
        await sleep(30000);
        consecutiveErrors = 0;
      }
    }
  }

  for (let i = 0; i < prefixes.length; i++) {
    await processPrefix(prefixes[i], 0);
    await sleep(DELAY_MS);
  }

  // ── Final save ──────────────────────────────────────────────────────
  if (allLeads.length > 0) {
    writeCSV(PROGRESS_FILE, allLeads);

    // Also save timestamped final file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const finalFile = SEGMENT
      ? `reco-agents-${SEGMENT}_${timestamp}.csv`
      : `reco-real-estate-agents_${timestamp}.csv`;
    const finalPath = path.join(OUTPUT_DIR, finalFile);
    writeCSV(finalPath, allLeads);
    console.log(`\n  Final file: ${finalPath}`);
  }

  // ── Summary ─────────────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const uniqueFirms = new Set(allLeads.filter(l => l.firm_name).map(l => l.firm_name)).size;
  const uniqueCities = new Set(allLeads.filter(l => l.city).map(l => l.city)).size;
  const brokers = allLeads.filter(l => l.title === 'Real Estate Broker').length;
  const salespeople = allLeads.filter(l => l.title === 'Real Estate Salesperson').length;

  console.log('');
  console.log('=============================================================');
  console.log('  RECO SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  Segment:       ${segmentLabel}`);
  console.log(`  Requests:      ${totalRequests} (${splitCount} prefixes auto-split to 3-letter)`);
  console.log(`  Total agents:  ${allLeads.length}`);
  console.log(`  Salespersons:  ${salespeople}`);
  console.log(`  Brokers:       ${brokers}`);
  console.log(`  With email:    ${withEmail} (${Math.round(withEmail / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  With phone:    ${withPhone} (${Math.round(withPhone / Math.max(allLeads.length, 1) * 100)}%)`);
  console.log(`  Unique firms:  ${uniqueFirms}`);
  console.log(`  Unique cities: ${uniqueCities}`);
  console.log(`  Duplicates:    ${duplicates}`);
  console.log(`  Errors:        ${errors}`);
  console.log(`  Time:          ${elapsed}s (${Math.round(elapsed / 60)}min)`);
  console.log(`  Output:        ${PROGRESS_FILE}`);
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
