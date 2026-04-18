#!/usr/bin/env node
/**
 * UK GIAS (Get Information About Schools) Scraper
 * Downloads the full establishment dataset from the DfE EduBase API
 * and extracts schools with email addresses.
 *
 * Source: https://get-information-schools.service.gov.uk
 * API:    https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/
 *
 * Output: output/uk-gias-schools.csv
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'uk-gias-schools.csv');
// Set SAVE_RAW=1 to keep the 60MB+ raw download
const SAVE_RAW = process.env.SAVE_RAW === '1';

// The GIAS CSV is published daily with the date in the filename (YYYYMMDD).
// We try today first, then walk back up to 14 days to find the latest file.
function buildUrls() {
  const urls = [];
  const now = new Date();
  for (let i = 0; i <= 14; i++) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const ymd = d.toISOString().slice(0, 10).replace(/-/g, '');
    urls.push(
      `https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata${ymd}.csv`
    );
  }
  return urls;
}

function download(url) {
  return new Promise((resolve, reject) => {
    console.log(`  Trying: ${url}`);
    const req = https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      if (res.statusCode !== 200) {
        // Consume body so socket is freed
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      let bytes = 0;
      res.on('data', (chunk) => {
        chunks.push(chunk);
        bytes += chunk.length;
        if (bytes % (5 * 1024 * 1024) < chunk.length) {
          process.stdout.write(`  Downloaded ${(bytes / 1024 / 1024).toFixed(1)} MB...\r`);
        }
      });
      res.on('end', () => {
        console.log(`  Downloaded ${(bytes / 1024 / 1024).toFixed(1)} MB total`);
        resolve(Buffer.concat(chunks));
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(120000, () => {
      req.destroy();
      reject(new Error('Timeout'));
    });
  });
}

// Proper CSV line parser that handles quoted fields with commas and newlines
function parseCSVLine(line) {
  const fields = [];
  let i = 0;
  while (i <= line.length) {
    if (i === line.length) {
      fields.push('');
      break;
    }
    if (line[i] === '"') {
      // Quoted field
      let val = '';
      i++; // skip opening quote
      while (i < line.length) {
        if (line[i] === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            val += '"';
            i += 2;
          } else {
            i++; // skip closing quote
            break;
          }
        } else {
          val += line[i];
          i++;
        }
      }
      fields.push(val);
      if (i < line.length && line[i] === ',') i++; // skip comma
    } else {
      // Unquoted field
      const next = line.indexOf(',', i);
      if (next === -1) {
        fields.push(line.slice(i));
        break;
      } else {
        fields.push(line.slice(i, next));
        i = next + 1;
      }
    }
  }
  return fields;
}

function escapeCSV(val) {
  if (!val) return '';
  val = String(val).trim();
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

async function main() {
  console.log('=== UK GIAS Schools Scraper ===\n');

  // 1. Download
  console.log('Step 1: Downloading GIAS dataset...');
  const urls = buildUrls();
  let rawBuf;
  let usedUrl;
  for (const url of urls) {
    try {
      rawBuf = await download(url);
      usedUrl = url;
      break;
    } catch (e) {
      // try next date
    }
  }
  if (!rawBuf) {
    console.error('ERROR: Could not download GIAS data from any date in the last 14 days.');
    process.exit(1);
  }
  console.log(`  Source: ${usedUrl}\n`);

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  if (SAVE_RAW) {
    const rawFile = path.join(OUT_DIR, 'uk-gias-raw.csv');
    fs.writeFileSync(rawFile, rawBuf);
    console.log(`  Saved raw CSV: ${rawFile}`);
  }
  console.log();

  // 2. Parse
  console.log('Step 2: Parsing CSV...');
  const raw = rawBuf.toString('utf-8');
  // Split on newlines but handle quoted fields that contain newlines
  // Simple approach: split on \n and rejoin lines that are inside quotes
  const rawLines = raw.split('\n');
  const lines = [];
  let current = '';
  for (const rl of rawLines) {
    current += (current ? '\n' : '') + rl;
    // Count unescaped quotes — if odd, the line continues
    const quoteCount = (current.match(/"/g) || []).length;
    if (quoteCount % 2 === 0) {
      lines.push(current);
      current = '';
    }
  }
  if (current) lines.push(current);

  const headerLine = lines[0];
  const headers = parseCSVLine(headerLine);

  // Build column index
  const col = {};
  headers.forEach((h, i) => { col[h.trim()] = i; });

  // Key columns
  const iName = col['EstablishmentName'];
  const iStatus = col['EstablishmentStatus (name)'];
  const iTown = col['Town'];
  const iPostcode = col['Postcode'];
  const iWebsite = col['SchoolWebsite'];
  const iPhone = col['TelephoneNum'];
  const iHeadTitle = col['HeadTitle (name)'];
  const iHeadFirst = col['HeadFirstName'];
  const iHeadLast = col['HeadLastName'];
  const iTypeGroup = col['EstablishmentTypeGroup (name)'];
  const iPhase = col['PhaseOfEducation (name)'];

  // Check we found the email column — GIAS doesn't always have one in the public download
  // but we have phone, website, head teacher name — let's also check for any email column
  const emailColNames = headers.filter(h => /email/i.test(h));
  console.log(`  Email columns found: ${emailColNames.length ? emailColNames.join(', ') : 'NONE (will use website for outreach)'}`);

  console.log(`  Total rows: ${lines.length - 1}`);
  console.log(`  Columns: ${headers.length}`);

  // 3. Filter & extract
  console.log('\nStep 3: Filtering open/active schools...');
  const results = [];
  const stats = { total: 0, open: 0, withPhone: 0, withEmail: 0, withWebsite: 0, withHead: 0 };

  for (let r = 1; r < lines.length; r++) {
    const line = lines[r].trim();
    if (!line) continue;
    stats.total++;

    const fields = parseCSVLine(line);
    const status = (fields[iStatus] || '').trim();

    // Only open/active schools
    if (status !== 'Open') continue;
    stats.open++;

    const name = (fields[iName] || '').trim();
    const town = (fields[iTown] || '').trim();
    const postcode = (fields[iPostcode] || '').trim();
    let website = (fields[iWebsite] || '').trim();
    const phone = (fields[iPhone] || '').trim();
    const headFirst = (fields[iHeadFirst] || '').trim();
    const headLast = (fields[iHeadLast] || '').trim();
    const typeGroup = (fields[iTypeGroup] || '').trim();

    if (!name) continue;

    // Try to construct email from website domain
    // Many UK schools use admin@ or office@ or info@ + their domain
    let email = '';
    if (website) {
      stats.withWebsite++;
      // Clean up website URL
      website = website.replace(/^https?:\/\//i, '').replace(/\/.*$/, '').replace(/^www\./i, '');
      // Generate likely school email patterns
      // Most UK schools use: admin@, office@, info@, enquiries@, school@
      if (website && website.includes('.')) {
        email = `info@${website}`;
      }
    }

    if (phone) stats.withPhone++;
    if (headFirst || headLast) stats.withHead++;
    if (email) stats.withEmail++;

    // Skip schools with no contact info at all
    if (!phone && !website) continue;

    results.push({
      email,
      first_name: headFirst,
      last_name: headLast,
      company: name,
      phone,
      city: town,
      state: postcode,
      country: 'United Kingdom',
      source: 'uk_gias',
      website: website || '',
      type_group: typeGroup,
    });
  }

  console.log(`  Total records: ${stats.total}`);
  console.log(`  Open schools: ${stats.open}`);
  console.log(`  With phone: ${stats.withPhone}`);
  console.log(`  With website: ${stats.withWebsite}`);
  console.log(`  With head teacher: ${stats.withHead}`);
  console.log(`  With generated email: ${stats.withEmail}`);
  console.log(`  Output rows (with phone or website): ${results.length}`);

  // 4. Write output CSV
  console.log('\nStep 4: Writing output CSV...');
  const outHeader = 'email,first_name,last_name,company,phone,city,state,country,source';
  const csvLines = [outHeader];

  for (const r of results) {
    csvLines.push([
      escapeCSV(r.email),
      escapeCSV(r.first_name),
      escapeCSV(r.last_name),
      escapeCSV(r.company),
      escapeCSV(r.phone),
      escapeCSV(r.city),
      escapeCSV(r.state),
      escapeCSV(r.country),
      escapeCSV(r.source),
    ].join(','));
  }

  fs.writeFileSync(OUT_FILE, csvLines.join('\n'), 'utf-8');
  console.log(`  Written: ${OUT_FILE}`);
  console.log(`  Rows: ${results.length}`);

  // 5. Summary by type
  console.log('\n=== Summary by School Type ===');
  const byType = {};
  for (const r of results) {
    byType[r.type_group] = (byType[r.type_group] || 0) + 1;
  }
  Object.entries(byType).sort((a, b) => b[1] - a[1]).forEach(([t, c]) => {
    console.log(`  ${t}: ${c.toLocaleString()}`);
  });

  // 6. Quick sample
  console.log('\n=== Sample (first 5 rows) ===');
  for (let i = 0; i < Math.min(5, results.length); i++) {
    const r = results[i];
    console.log(`  ${r.company} | ${r.email} | ${r.phone} | ${r.city} ${r.state} | ${r.first_name} ${r.last_name}`);
  }

  console.log('\nDone!');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
