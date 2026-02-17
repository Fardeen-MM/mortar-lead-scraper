/**
 * Test: lib/csv-handler.js
 */
const fs = require('fs');
const path = require('path');
const { readCSV, writeCSV, generateOutputPath, OUTPUT_COLUMNS } = require('../lib/csv-handler');

let passed = 0;
let failed = 0;

function assert(label, actual, expected) {
  if (actual === expected) {
    console.log(`  PASS: ${label}`);
    passed++;
  } else {
    console.log(`  FAIL: ${label}  —  expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    failed++;
  }
}

function assertIncludes(label, actual, substring) {
  if (actual && actual.includes(substring)) {
    console.log(`  PASS: ${label}`);
    passed++;
  } else {
    console.log(`  FAIL: ${label}  —  expected "${actual}" to include "${substring}"`);
    failed++;
  }
}

async function runTests() {
  const tmpDir = path.join(__dirname, '..', 'data', 'test-tmp');
  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });

  // ===================== OUTPUT_COLUMNS =====================
  console.log('\n=== CSV Handler: OUTPUT_COLUMNS ===');
  assert('Has output columns', OUTPUT_COLUMNS.length > 0, true);
  const colIds = OUTPUT_COLUMNS.map(c => c.id);
  assert('Has first_name column', colIds.includes('first_name'), true);
  assert('Has last_name column', colIds.includes('last_name'), true);
  assert('Has email column', colIds.includes('email'), true);
  assert('Has phone column', colIds.includes('phone'), true);
  assert('Has website column', colIds.includes('website'), true);
  assert('Has firm_name column', colIds.includes('firm_name'), true);
  assert('Has state column', colIds.includes('state'), true);
  assert('Has city column', colIds.includes('city'), true);
  assert('Has source column', colIds.includes('source'), true);

  // ===================== writeCSV =====================
  console.log('\n=== CSV Handler: writeCSV ===');

  const sampleLeads = [
    {
      first_name: 'John', last_name: 'Smith', firm_name: 'Smith Law LLC',
      practice_area: 'Personal Injury', city: 'Miami', state: 'FL',
      phone: '(305) 555-1234', website: 'https://smithlaw.com',
      email: 'jsmith@smithlaw.com', bar_number: '12345',
      admission_date: '2010-01-15', bar_status: 'Active', source: 'FL-bar',
      title: 'Partner', linkedin_url: '', bio: '', education: 'Harvard Law',
      languages: 'English, Spanish', practice_specialties: 'PI, Auto Accidents',
    },
    {
      first_name: 'Jane', last_name: 'Doe', firm_name: 'Doe Legal',
      practice_area: 'Criminal Defense', city: 'New York', state: 'NY',
      phone: '(212) 555-0100', website: 'https://doelaw.com',
      email: 'jdoe@doelaw.com', bar_number: '67890',
      admission_date: '2015-06-01', bar_status: 'Active', source: 'NY-bar',
      title: 'Associate', linkedin_url: 'https://linkedin.com/in/janedoe',
      bio: 'Expert in criminal defense', education: 'Yale Law',
      languages: 'English', practice_specialties: 'DUI, White Collar',
    },
    {
      first_name: 'Pierre', last_name: 'Dupont', firm_name: 'Cabinet Dupont',
      practice_area: 'Corporate', city: 'Paris', state: 'FR',
      phone: '+33 1 23 45 67 89', website: 'https://dupont-avocats.fr',
      email: 'pdupont@dupont-avocats.fr', bar_number: '',
      admission_date: '', bar_status: '', source: 'FR-bar',
      title: '', linkedin_url: '', bio: '', education: '',
      languages: 'French, English', practice_specialties: '',
    },
  ];

  const outFile = path.join(tmpDir, 'test-output.csv');
  await writeCSV(outFile, sampleLeads);
  assert('writeCSV created file', fs.existsSync(outFile), true);

  const rawContent = fs.readFileSync(outFile, 'utf-8');
  assertIncludes('CSV has header row', rawContent, 'first_name');
  assertIncludes('CSV has John', rawContent, 'John');
  assertIncludes('CSV has Jane', rawContent, 'Jane');
  assertIncludes('CSV has Pierre', rawContent, 'Pierre');

  // ===================== readCSV =====================
  console.log('\n=== CSV Handler: readCSV ===');

  const readBack = await readCSV(outFile);
  assert('readCSV returns 3 rows', readBack.length, 3);
  assert('Row 0 first_name', readBack[0].first_name, 'John');
  assert('Row 0 last_name', readBack[0].last_name, 'Smith');
  assert('Row 0 city', readBack[0].city, 'Miami');
  assert('Row 0 state', readBack[0].state, 'FL');
  assert('Row 0 email', readBack[0].email, 'jsmith@smithlaw.com');
  assert('Row 1 first_name', readBack[1].first_name, 'Jane');
  assert('Row 1 state', readBack[1].state, 'NY');
  assert('Row 2 first_name', readBack[2].first_name, 'Pierre');
  assert('Row 2 state', readBack[2].state, 'FR');

  // ===================== readCSV with alternative column names =====================
  console.log('\n=== CSV Handler: readCSV with alt column names ===');

  const altFile = path.join(tmpDir, 'test-alt-columns.csv');
  fs.writeFileSync(altFile, `First Name,Last Name,Company,Email Address,Phone Number,City,State\nBob,Jones,Jones Corp,bob@jones.com,555-1234,Chicago,IL\n`);
  const altRows = await readCSV(altFile);
  assert('Alt cols: 1 row', altRows.length, 1);
  assert('Alt cols: first_name mapped', altRows[0].first_name, 'Bob');
  assert('Alt cols: last_name mapped', altRows[0].last_name, 'Jones');
  assert('Alt cols: firm_name from Company', altRows[0].firm_name, 'Jones Corp');
  assert('Alt cols: email from Email Address', altRows[0].email, 'bob@jones.com');

  // ===================== readCSV error handling =====================
  console.log('\n=== CSV Handler: readCSV error handling ===');

  try {
    await readCSV('/nonexistent/path/file.csv');
    assert('readCSV on missing file throws', false, true);
  } catch (err) {
    assert('readCSV on missing file throws Error', err.message.includes('File not found'), true);
  }

  // ===================== generateOutputPath =====================
  console.log('\n=== CSV Handler: generateOutputPath ===');

  const outPath = generateOutputPath('FL', 'Personal Injury');
  assertIncludes('Output path has state code', outPath, 'fl-');
  assertIncludes('Output path has practice area', outPath, 'personal-injury');
  assertIncludes('Output path has .csv', outPath, '.csv');

  const outPath2 = generateOutputPath('NY', '');
  assertIncludes('Output path without practice area uses "all"', outPath2, '-all-');

  // ===================== Cleanup =====================
  try { fs.unlinkSync(outFile); } catch {}
  try { fs.unlinkSync(altFile); } catch {}
  try { fs.rmdirSync(tmpDir); } catch {}

  // ===================== Summary =====================
  console.log(`\n=== CSV HANDLER RESULTS: ${passed} passed, ${failed} failed ===\n`);
  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
