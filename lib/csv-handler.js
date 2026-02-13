/**
 * CSV Handler — read existing leads and write output CSVs
 *
 * Output columns match Instantly upload format:
 * first_name, last_name, firm_name, practice_area, city, state,
 * phone, website, email, bar_number, admission_date, bar_status, source
 */

const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

const OUTPUT_COLUMNS = [
  { id: 'first_name', title: 'first_name' },
  { id: 'last_name', title: 'last_name' },
  { id: 'firm_name', title: 'firm_name' },
  { id: 'practice_area', title: 'practice_area' },
  { id: 'city', title: 'city' },
  { id: 'state', title: 'state' },
  { id: 'phone', title: 'phone' },
  { id: 'website', title: 'website' },
  { id: 'email', title: 'email' },
  { id: 'bar_number', title: 'bar_number' },
  { id: 'admission_date', title: 'admission_date' },
  { id: 'bar_status', title: 'bar_status' },
  { id: 'source', title: 'source' },
];

/**
 * Read a CSV file and return an array of row objects.
 * Handles various column naming conventions (Instantly, Apollo, custom).
 */
function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    if (!fs.existsSync(filePath)) {
      return reject(new Error(`File not found: ${filePath}`));
    }
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row) => {
        rows.push(normalizeColumns(row));
      })
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

/**
 * Map various CSV column names to our standard format.
 */
function normalizeColumns(row) {
  // Build a lowercase key lookup
  const lk = {};
  for (const [key, val] of Object.entries(row)) {
    lk[key.toLowerCase().trim()] = val;
  }

  return {
    first_name: lk['first_name'] || lk['first name'] || lk['firstname'] || lk['first'] || '',
    last_name: lk['last_name'] || lk['last name'] || lk['lastname'] || lk['last'] || '',
    firm_name: lk['firm_name'] || lk['company'] || lk['company_name'] || lk['company name'] ||
               lk['firm'] || lk['organization'] || '',
    practice_area: lk['practice_area'] || lk['practice area'] || lk['industry'] || '',
    city: lk['city'] || lk['location_city'] || '',
    state: lk['state'] || lk['location_state'] || lk['province'] || '',
    phone: lk['phone'] || lk['phone_number'] || lk['office_phone'] || lk['direct_phone'] || '',
    website: lk['website'] || lk['company_url'] || lk['url'] || lk['domain'] || '',
    email: lk['email'] || lk['email_address'] || '',
    bar_number: lk['bar_number'] || '',
    admission_date: lk['admission_date'] || '',
    bar_status: lk['bar_status'] || '',
    source: lk['source'] || '',
  };
}

/**
 * Write leads to a CSV file.
 */
async function writeCSV(filePath, records) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  const writer = createObjectCsvWriter({
    path: filePath,
    header: OUTPUT_COLUMNS,
  });

  await writer.writeRecords(records);
  return filePath;
}

/**
 * Generate output filename with timestamp.
 */
function generateOutputPath(stateCode, practiceArea) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const practice = practiceArea
    ? practiceArea.toLowerCase().replace(/\s+/g, '-')
    : 'all';
  return path.join(
    __dirname, '..', 'data', 'output',
    `${stateCode.toLowerCase()}-${practice}-${ts}.csv`
  );
}

module.exports = { readCSV, writeCSV, generateOutputPath, OUTPUT_COLUMNS };
