#!/usr/bin/env node
/**
 * download-free-datasets.js
 * Downloads several free CSV datasets containing business/school contact info.
 * Saves them all to /Users/fardeenchoudhury/mortar-lead-scraper/output/
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const zlib = require('zlib');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');

// Ensure output dir exists
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

/**
 * Download a URL, following redirects (up to 5 hops).
 * Returns the response body as a string.
 */
function download(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
      }
    };
    proto.get(url, options, (res) => {
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        const next = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return resolve(download(next, maxRedirects - 1));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }

      // Handle gzip/deflate
      let stream = res;
      const encoding = res.headers['content-encoding'];
      if (encoding === 'gzip') stream = res.pipe(zlib.createGunzip());
      else if (encoding === 'deflate') stream = res.pipe(zlib.createInflate());

      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => resolve(Buffer.concat(chunks)));
      stream.on('error', reject);
    }).on('error', reject);
  });
}

/**
 * Download a URL to a file on disk (binary-safe, for zips).
 */
function downloadToFile(url, filePath, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
      }
    };
    proto.get(url, options, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        const next = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return resolve(downloadToFile(next, filePath, maxRedirects - 1));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const ws = fs.createWriteStream(filePath);
      res.pipe(ws);
      ws.on('finish', () => { ws.close(); resolve(); });
      ws.on('error', reject);
    }).on('error', reject);
  });
}

function countLines(csvStr) {
  const lines = csvStr.trim().split('\n');
  return lines.length - 1; // subtract header
}

function getFields(csvStr) {
  const firstLine = csvStr.split('\n')[0].trim();
  // Handle quoted fields
  const fields = [];
  let current = '';
  let inQuote = false;
  for (const ch of firstLine) {
    if (ch === '"') { inQuote = !inQuote; continue; }
    if (ch === ',' && !inQuote) { fields.push(current.trim()); current = ''; continue; }
    current += ch;
  }
  fields.push(current.trim());
  return fields;
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ============================================================
// Dataset download functions
// ============================================================

async function downloadMaRealEstateSchools() {
  const outFile = path.join(OUTPUT_DIR, 'ma-real-estate-schools.csv');
  if (fs.existsSync(outFile)) {
    const data = fs.readFileSync(outFile, 'utf8');
    const records = countLines(data);
    const fields = getFields(data);
    console.log(`  [SKIP] ma-real-estate-schools.csv already exists (${records} records)`);
    console.log(`  Fields: ${fields.join(', ')}`);
    return { name: 'MA Real Estate Schools', file: 'ma-real-estate-schools.csv', records, fields, skipped: true };
  }
  const url = 'https://www.mass.gov/files/csv/2025-08/Authorized-Real-Estate-Schools-List.csv';
  const buf = await download(url);
  const data = buf.toString('utf8');
  fs.writeFileSync(outFile, data);
  const records = countLines(data);
  const fields = getFields(data);
  console.log(`  [OK] ma-real-estate-schools.csv — ${records} records`);
  console.log(`  Fields: ${fields.join(', ')}`);
  return { name: 'MA Real Estate Schools', file: 'ma-real-estate-schools.csv', records, fields };
}

async function downloadNyDmvDrivingSchools() {
  const outFile = path.join(OUTPUT_DIR, 'ny-dmv-driving-schools.csv');
  const url = 'https://data.ny.gov/api/views/3p6i-cv8y/rows.csv?accessType=DOWNLOAD';
  console.log('  Downloading NY DMV Driving Schools...');
  const buf = await download(url);
  const data = buf.toString('utf8');
  fs.writeFileSync(outFile, data);
  const records = countLines(data);
  const fields = getFields(data);
  console.log(`  [OK] ny-dmv-driving-schools.csv — ${records} records`);
  console.log(`  Fields: ${fields.join(', ')}`);
  return { name: 'NY DMV Driving Schools', file: 'ny-dmv-driving-schools.csv', records, fields };
}

async function downloadTxTdlrDriverEducation() {
  const outFile = path.join(OUTPUT_DIR, 'tx-tdlr-driver-education.csv');
  const url = 'https://www.tdlr.texas.gov/dbproduction2/vsDriverEduProvider.csv';
  console.log('  Downloading TX TDLR Driver Education...');
  const buf = await download(url);
  const data = buf.toString('utf8');
  fs.writeFileSync(outFile, data);
  const records = countLines(data);
  const fields = getFields(data);
  console.log(`  [OK] tx-tdlr-driver-education.csv — ${records} records`);
  console.log(`  Fields: ${fields.join(', ')}`);
  return { name: 'TX TDLR Driver Education', file: 'tx-tdlr-driver-education.csv', records, fields };
}

async function downloadMaCosmetologySchools() {
  const outFile = path.join(OUTPUT_DIR, 'ma-cosmetology-schools.csv');
  const url = 'https://www.mass.gov/files/csv/2024-04/Approved-cosmetology-barbering-and-electrology-schools.csv';
  console.log('  Downloading MA Cosmetology Schools...');
  const buf = await download(url);
  const data = buf.toString('utf8');
  fs.writeFileSync(outFile, data);
  const records = countLines(data);
  const fields = getFields(data);
  console.log(`  [OK] ma-cosmetology-schools.csv — ${records} records`);
  console.log(`  Fields: ${fields.join(', ')}`);
  return { name: 'MA Cosmetology Schools', file: 'ma-cosmetology-schools.csv', records, fields };
}

async function downloadGetmanfredBootcamps() {
  const outFile = path.join(OUTPUT_DIR, 'getmanfred-bootcamps.csv');
  const url = 'https://raw.githubusercontent.com/getmanfred/bootcamps/main/bootcamps.json';
  console.log('  Downloading Getmanfred Bootcamps (JSON)...');
  const buf = await download(url);
  const json = JSON.parse(buf.toString('utf8'));

  // Parse JSON into CSV with requested header
  const header = 'email,first_name,last_name,company,phone,city,state,country,source';
  const rows = [header];
  for (const bc of json) {
    const org = bc.Organization || bc.organization || '';
    const contact = bc.Contact || bc.contact || '';
    const location = bc.Location || bc.location || '';
    const name = bc.Name || bc.name || '';

    // Try to extract email from Contact field
    const emailMatch = contact.match(/[\w.+-]+@[\w.-]+\.\w+/);
    const email = emailMatch ? emailMatch[0] : '';

    // Location might contain city info
    const city = location.split(',')[0] || '';
    const country = location.includes('Remote') ? '' : (location.split(',').pop() || '').trim();

    rows.push([
      escapeCsv(email),
      '',  // first_name — not in data
      '',  // last_name — not in data
      escapeCsv(org),
      '',  // phone — not in data
      escapeCsv(city),
      '',  // state — not in data
      escapeCsv(country),
      escapeCsv(`Getmanfred Bootcamps - ${name}`)
    ].join(','));
  }

  fs.writeFileSync(outFile, rows.join('\n') + '\n');
  const records = json.length;
  console.log(`  [OK] getmanfred-bootcamps.csv — ${records} records (parsed from JSON)`);
  console.log(`  Fields: ${header}`);
  return { name: 'Getmanfred Bootcamps', file: 'getmanfred-bootcamps.csv', records, fields: header.split(',') };
}

async function downloadDatascienceBootcamps() {
  const outFile = path.join(OUTPUT_DIR, 'datascience-bootcamps.csv');
  const url = 'https://raw.githubusercontent.com/ryanswanstrom/awesome-datascience-bootcamps/master/data_science_bootcamps.csv';
  console.log('  Downloading Data Science Bootcamps...');
  const buf = await download(url);
  const data = buf.toString('utf8');
  fs.writeFileSync(outFile, data);
  const records = countLines(data);
  const fields = getFields(data);
  console.log(`  [OK] datascience-bootcamps.csv — ${records} records`);
  console.log(`  Fields: ${fields.join(', ')}`);
  return { name: 'Data Science Bootcamps', file: 'datascience-bootcamps.csv', records, fields };
}

async function downloadUkEducationCharities() {
  const outFile = path.join(OUTPUT_DIR, 'uk-education-charities.csv');
  const tmpDir = path.join(OUTPUT_DIR, '_tmp_charity');

  console.log('  Downloading UK Charity Commission data (this may take a moment)...');

  // We need two files:
  // 1. Main charity data (charity details, contacts, emails)
  // 2. Classification data (to filter for education)
  const charityUrl = 'https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt/publicextract.charity.zip';
  const classificationUrl = 'https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt/publicextract.charity_classification.zip';

  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });

  const charityZip = path.join(tmpDir, 'charity.zip');
  const classZip = path.join(tmpDir, 'classification.zip');

  // Download both zips
  console.log('    Downloading charity main data zip...');
  await downloadToFile(charityUrl, charityZip);
  console.log('    Downloading classification data zip...');
  await downloadToFile(classificationUrl, classZip);

  // Unzip
  console.log('    Extracting...');
  execSync(`cd "${tmpDir}" && unzip -o charity.zip 2>/dev/null`, { stdio: 'pipe' });
  execSync(`cd "${tmpDir}" && unzip -o classification.zip 2>/dev/null`, { stdio: 'pipe' });

  // Find the extracted files
  const files = fs.readdirSync(tmpDir);
  const charityFile = files.find(f => f.startsWith('publicextract.charity') && !f.includes('classification') && f.endsWith('.txt'));
  const classFile = files.find(f => f.includes('classification') && f.endsWith('.txt'));

  if (!charityFile) throw new Error('Could not find extracted charity data file. Files: ' + files.join(', '));
  if (!classFile) throw new Error('Could not find extracted classification data file. Files: ' + files.join(', '));

  console.log(`    Charity file: ${charityFile}`);
  console.log(`    Classification file: ${classFile}`);

  // Read classification file to find education charity numbers
  // Tab-delimited format
  const classData = fs.readFileSync(path.join(tmpDir, classFile), 'utf8');
  const classLines = classData.split('\n');
  const classHeader = classLines[0].split('\t');
  console.log(`    Classification fields: ${classHeader.join(', ')}`);

  // Find column indices
  const orgNumIdx = classHeader.findIndex(h => h.match(/organisation_number|charity_number|registered_charity_number|numero/i) || h.trim().toLowerCase() === 'organisation_number');
  const classNameIdx = classHeader.findIndex(h => h.match(/classification_description|class_desc|what|classification_type/i));

  // If we can't find by name, try positional — let's log header for debugging
  console.log(`    Classification header (raw): ${classHeader.map((h,i) => `[${i}]=${h.trim()}`).join(' | ')}`);

  // Build set of education charity numbers
  const educationCharityNums = new Set();
  for (let i = 1; i < classLines.length; i++) {
    const line = classLines[i];
    if (!line.trim()) continue;
    const cols = line.split('\t');
    // Check if any column contains education/training keywords
    const lineStr = line.toLowerCase();
    if (lineStr.includes('education') || lineStr.includes('training')) {
      // Use first column as charity number (or the identified column)
      const num = cols[orgNumIdx >= 0 ? orgNumIdx : 0]?.trim();
      if (num) educationCharityNums.add(num);
    }
  }
  console.log(`    Found ${educationCharityNums.size} charity numbers with education/training classification`);

  // Read main charity file and filter
  const charityData = fs.readFileSync(path.join(tmpDir, charityFile), 'utf8');
  const charityLines = charityData.split('\n');
  const charityHeader = charityLines[0].split('\t');
  console.log(`    Charity fields: ${charityHeader.map(h => h.trim()).join(', ')}`);

  // Find org number column in charity data
  const charOrgIdx = charityHeader.findIndex(h => {
    const t = h.trim().toLowerCase();
    return t === 'organisation_number' || t === 'registered_charity_number' || t === 'charity_number';
  });
  console.log(`    Charity org number column index: ${charOrgIdx}`);

  // Build CSV output
  const outHeader = charityHeader.map(h => escapeCsv(h.trim())).join(',');
  const outRows = [outHeader];
  let totalCharity = 0;
  let matchedCount = 0;

  for (let i = 1; i < charityLines.length; i++) {
    const line = charityLines[i];
    if (!line.trim()) continue;
    totalCharity++;
    const cols = line.split('\t');
    const num = cols[charOrgIdx >= 0 ? charOrgIdx : 0]?.trim();
    if (educationCharityNums.has(num)) {
      matchedCount++;
      outRows.push(cols.map(c => escapeCsv(c.trim())).join(','));
    }
  }

  fs.writeFileSync(outFile, outRows.join('\n') + '\n');
  console.log(`  [OK] uk-education-charities.csv — ${matchedCount} education charities (filtered from ${totalCharity} total)`);
  console.log(`  Fields: ${charityHeader.map(h => h.trim()).join(', ')}`);

  // Cleanup temp files
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch (e) {
    console.log('  (Warning: could not clean up temp dir)');
  }

  return {
    name: 'UK Education Charities',
    file: 'uk-education-charities.csv',
    records: matchedCount,
    fields: charityHeader.map(h => h.trim()),
    totalBeforeFilter: totalCharity
  };
}

// ============================================================
// Main
// ============================================================

async function main() {
  console.log('=== Free Dataset Downloader ===');
  console.log(`Output directory: ${OUTPUT_DIR}\n`);

  const results = [];

  const tasks = [
    { fn: downloadMaRealEstateSchools, label: '1. MA Real Estate Schools' },
    { fn: downloadNyDmvDrivingSchools, label: '2. NY DMV Driving Schools' },
    { fn: downloadTxTdlrDriverEducation, label: '3. TX TDLR Driver Education' },
    { fn: downloadMaCosmetologySchools, label: '4. MA Cosmetology Schools' },
    { fn: downloadGetmanfredBootcamps, label: '5. Getmanfred Bootcamps (JSON → CSV)' },
    { fn: downloadDatascienceBootcamps, label: '6. Data Science Bootcamps' },
    { fn: downloadUkEducationCharities, label: '7. UK Education Charities' },
  ];

  for (const task of tasks) {
    console.log(`\n${task.label}:`);
    try {
      const result = await task.fn();
      results.push(result);
    } catch (err) {
      console.log(`  [FAIL] ${err.message}`);
      results.push({ name: task.label, error: err.message });
    }
  }

  // Summary
  console.log('\n\n========================================');
  console.log('SUMMARY');
  console.log('========================================');
  let totalRecords = 0;
  for (const r of results) {
    if (r.error) {
      console.log(`  FAIL  ${r.name}: ${r.error}`);
    } else {
      const skip = r.skipped ? ' (already existed)' : '';
      console.log(`  OK    ${r.name}: ${r.records} records → ${r.file}${skip}`);
      totalRecords += r.records;
    }
  }
  console.log(`\nTotal records across all datasets: ${totalRecords}`);
  console.log('Done.');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
