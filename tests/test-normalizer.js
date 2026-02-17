/**
 * Test: lib/normalizer.js
 */
const {
  normalizeState,
  normalizePhone,
  normalizeName,
  normalizeCity,
  normalizeFirmName,
  extractDomain,
  normalizeRecord,
} = require('../lib/normalizer');

let passed = 0;
let failed = 0;

function assert(label, actual, expected) {
  if (actual === expected) {
    console.log(`  PASS: ${label}`);
    passed++;
  } else {
    console.log(`  FAIL: ${label}  —  expected "${expected}", got "${actual}"`);
    failed++;
  }
}

// ===================== normalizeState =====================
console.log('\n=== normalizeState ===');

// US states (2-letter codes)
assert('US: FL', normalizeState('FL'), 'FL');
assert('US: NY', normalizeState('NY'), 'NY');
assert('US: CA', normalizeState('CA'), 'CA');
assert('US: TX', normalizeState('TX'), 'TX');
assert('US: fl lowercase', normalizeState('fl'), 'FL');

// Canadian provinces (hyphenated)
assert('CA: CA-ON', normalizeState('CA-ON'), 'CA-ON');
assert('CA: CA-BC', normalizeState('CA-BC'), 'CA-BC');
assert('CA: ca-on lowercase', normalizeState('ca-on'), 'CA-ON');
assert('CA: CA-AB', normalizeState('CA-AB'), 'CA-AB');
assert('CA: CA-QC', normalizeState('CA-QC'), 'CA-QC');

// UK (hyphenated)
assert('UK: UK-EW', normalizeState('UK-EW'), 'UK-EW');
assert('UK: UK-SC', normalizeState('UK-SC'), 'UK-SC');
assert('UK: uk-ew lowercase', normalizeState('uk-ew'), 'UK-EW');

// Australia (hyphenated)
assert('AU: AU-NSW', normalizeState('AU-NSW'), 'AU-NSW');
assert('AU: AU-VIC', normalizeState('AU-VIC'), 'AU-VIC');
assert('AU: AU-QLD', normalizeState('AU-QLD'), 'AU-QLD');
assert('AU: au-nsw lowercase', normalizeState('au-nsw'), 'AU-NSW');

// Europe
assert('EU: DE-BRAK', normalizeState('DE-BRAK'), 'DE-BRAK');
assert('EU: FR', normalizeState('FR'), 'FR');
assert('EU: IE', normalizeState('IE'), 'IE');
assert('EU: IT', normalizeState('IT'), 'IT');
assert('EU: ES', normalizeState('ES'), 'ES');

// International
assert('Int: NZ', normalizeState('NZ'), 'NZ');
assert('Int: SG', normalizeState('SG'), 'SG');
assert('Int: HK', normalizeState('HK'), 'HK');

// Edge cases
assert('Edge: empty string', normalizeState(''), '');
assert('Edge: null', normalizeState(null), '');
assert('Edge: undefined', normalizeState(undefined), '');
assert('Edge: whitespace', normalizeState('  FL  '), 'FL');
assert('Edge: DE (US Delaware, not Germany)', normalizeState('DE'), 'DE');

// ===================== normalizePhone =====================
console.log('\n=== normalizePhone ===');

// US/CA phones
assert('US: (305) 555-1234', normalizePhone('(305) 555-1234'), '3055551234');
assert('US: 305-555-1234', normalizePhone('305-555-1234'), '3055551234');
assert('US: +1 305 555 1234', normalizePhone('+1 305 555 1234'), '3055551234');
assert('US: 1-305-555-1234 (11 digits)', normalizePhone('1-305-555-1234'), '3055551234');
assert('US: 212.555.0100', normalizePhone('212.555.0100'), '2125550100');

// UK phones
assert('UK: +44 20 7946 0958', normalizePhone('+44 20 7946 0958'), '2079460958');
assert('UK: +44 (0)20 7946 0958', normalizePhone('+44 (0)20 7946 0958'), '2079460958');

// Australia phones
assert('AU: +61 2 9876 5432', normalizePhone('+61 2 9876 5432'), '298765432');
assert('AU: +61 (0)2 9876 5432', normalizePhone('+61 (0)2 9876 5432'), '298765432');

// France phones
assert('FR: +33 1 23 45 67 89', normalizePhone('+33 1 23 45 67 89'), '123456789');
assert('FR: +33 (0)1 23 45 67 89', normalizePhone('+33 (0)1 23 45 67 89'), '123456789');

// Germany phones
assert('DE: +49 30 1234567', normalizePhone('+49 30 1234567'), '301234567');
assert('DE: +49 (0)30 1234567', normalizePhone('+49 (0)30 1234567'), '301234567');

// Ireland phones
assert('IE: +353 1 234 5678', normalizePhone('+353 1 234 5678'), '12345678');

// New Zealand phones
assert('NZ: +64 9 123 4567', normalizePhone('+64 9 123 4567'), '91234567');

// Singapore phones
assert('SG: +65 6123 4567', normalizePhone('+65 6123 4567'), '61234567');

// Hong Kong phones
assert('HK: +852 2123 4567', normalizePhone('+852 2123 4567'), '21234567');

// Edge cases
assert('Edge: empty string', normalizePhone(''), '');
assert('Edge: null', normalizePhone(null), '');
assert('Edge: undefined', normalizePhone(undefined), '');

// ===================== normalizeName =====================
console.log('\n=== normalizeName ===');

assert('Name: John', normalizeName('John'), 'john');
assert('Name: JANE DOE', normalizeName('JANE DOE'), 'jane doe');
assert('Name: extra whitespace', normalizeName('  John   Doe  '), 'john doe');
assert('Name: empty', normalizeName(''), '');
assert('Name: null', normalizeName(null), '');

// ===================== normalizeCity =====================
console.log('\n=== normalizeCity ===');

assert('City: New York', normalizeCity('New York'), 'new york');
assert('City: St. Louis', normalizeCity('St. Louis'), 'st louis');
assert('City: empty', normalizeCity(''), '');
assert('City: null', normalizeCity(null), '');

// ===================== normalizeFirmName =====================
console.log('\n=== normalizeFirmName ===');

assert('Firm: Smith & Associates LLC', normalizeFirmName('Smith & Associates LLC'), 'smith');
assert('Firm: Jones Law Firm', normalizeFirmName('Jones Law Firm'), 'jones');
assert('Firm: empty', normalizeFirmName(''), '');
assert('Firm: null', normalizeFirmName(null), '');
assert('Firm: Brown Legal Group PC', normalizeFirmName('Brown Legal Group PC'), 'brown');

// ===================== extractDomain =====================
console.log('\n=== extractDomain ===');

assert('Domain: https://www.example.com/page', extractDomain('https://www.example.com/page'), 'example.com');
assert('Domain: http://example.com', extractDomain('http://example.com'), 'example.com');
assert('Domain: www.example.com', extractDomain('www.example.com'), 'example.com');
assert('Domain: example.com', extractDomain('example.com'), 'example.com');
assert('Domain: empty', extractDomain(''), '');
assert('Domain: null', extractDomain(null), '');

// ===================== normalizeRecord =====================
console.log('\n=== normalizeRecord ===');

const record = normalizeRecord({
  first_name: 'John',
  last_name: 'Smith',
  firm_name: 'Smith & Associates LLC',
  city: 'New York',
  state: 'NY',
  phone: '(212) 555-0100',
  website: 'https://www.smithlaw.com',
  email: 'jsmith@smithlaw.com',
});

assert('Record: first_name preserved', record.first_name, 'John');
assert('Record: _norm.firstName', record._norm.firstName, 'john');
assert('Record: _norm.lastName', record._norm.lastName, 'smith');
assert('Record: _norm.firm', record._norm.firm, 'smith');
assert('Record: _norm.city', record._norm.city, 'new york');
assert('Record: _norm.state', record._norm.state, 'NY');
assert('Record: _norm.phone', record._norm.phone, '2125550100');
assert('Record: _norm.domain', record._norm.domain, 'smithlaw.com');
assert('Record: _norm.email', record._norm.email, 'jsmith@smithlaw.com');

// ===================== Summary =====================
console.log(`\n=== NORMALIZER RESULTS: ${passed} passed, ${failed} failed ===\n`);
process.exit(failed > 0 ? 1 : 0);
