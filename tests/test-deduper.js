/**
 * Test: lib/deduper.js
 */
const Deduper = require('../lib/deduper');

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

// ===================== Setup =====================
console.log('\n=== Deduper: Construction with existing leads ===');

const existingLeads = [
  {
    first_name: 'John', last_name: 'Smith', firm_name: 'Smith & Associates LLC',
    city: 'Miami', state: 'FL', phone: '(305) 555-1234',
    website: 'https://www.smithlaw.com', email: 'jsmith@smithlaw.com',
  },
  {
    first_name: 'Jane', last_name: 'Doe', firm_name: 'Doe Legal Group',
    city: 'New York', state: 'NY', phone: '(212) 555-0100',
    website: 'https://www.doelaw.com', email: 'jdoe@doelaw.com',
  },
  {
    first_name: 'Alice', last_name: 'Brown', firm_name: 'Brown PC',
    city: 'Los Angeles', state: 'CA', phone: '+44 20 7946 0958',
    website: 'https://www.brownlaw.co.uk', email: 'alice@brownlaw.co.uk',
  },
];

const deduper = new Deduper(existingLeads);
assert('Constructor: 3 leads loaded', deduper.leads.length, 3);
assert('Constructor: domainSet has 3', deduper.domainSet.size, 3);
assert('Constructor: phoneSet has 3', deduper.phoneSet.size, 3);
assert('Constructor: emailSet has 3', deduper.emailSet.size, 3);
assert('Constructor: nameLocationSet has 3', deduper.nameLocationSet.size, 3);

// ===================== Duplicate Detection =====================
console.log('\n=== Deduper: Duplicate detection — domain match ===');

let result = deduper.check({
  first_name: 'Bob', last_name: 'Johnson',
  website: 'https://smithlaw.com', state: 'FL',
});
assert('Domain dup: isDuplicate', result.isDuplicate, true);
assert('Domain dup: matchReason contains domain', result.matchReason.includes('domain'), true);

console.log('\n=== Deduper: Duplicate detection — phone match ===');

result = deduper.check({
  first_name: 'Bob', last_name: 'Johnson',
  phone: '305-555-1234', state: 'FL',
});
assert('Phone dup: isDuplicate', result.isDuplicate, true);
assert('Phone dup: matchReason contains phone', result.matchReason.includes('phone'), true);

console.log('\n=== Deduper: Duplicate detection — email match ===');

result = deduper.check({
  first_name: 'Bob', last_name: 'Johnson',
  email: 'jsmith@smithlaw.com', state: 'FL',
});
assert('Email dup: isDuplicate', result.isDuplicate, true);
assert('Email dup: matchReason contains email', result.matchReason.includes('email'), true);

console.log('\n=== Deduper: Duplicate detection — name+city match ===');

result = deduper.check({
  first_name: 'John', last_name: 'Smith',
  city: 'Miami', state: 'FL',
});
assert('Name+city dup: isDuplicate', result.isDuplicate, true);
assert('Name+city dup: matchReason contains name+city', result.matchReason.includes('name+city'), true);

console.log('\n=== Deduper: Duplicate detection — firm+state fuzzy match ===');

result = deduper.check({
  firm_name: 'Smith and Associates', state: 'FL',
  first_name: 'X', last_name: 'Y', city: 'Tampa',
});
assert('Firm+state fuzzy dup: isDuplicate', result.isDuplicate, true);
assert('Firm+state fuzzy dup: matchReason contains firm+state', result.matchReason.includes('firm+state'), true);

console.log('\n=== Deduper: Unique lead (no match) ===');

result = deduper.check({
  first_name: 'Charlie', last_name: 'Wilson',
  firm_name: 'Wilson Law Group', city: 'Houston', state: 'TX',
  phone: '(713) 555-9999', website: 'https://www.wilsonlaw.com',
  email: 'charlie@wilsonlaw.com',
});
assert('Unique: isDuplicate', result.isDuplicate, false);
assert('Unique: matchReason is null', result.matchReason, null);

// ===================== addToKnown =====================
console.log('\n=== Deduper: addToKnown ===');

deduper.addToKnown({
  first_name: 'Charlie', last_name: 'Wilson',
  firm_name: 'Wilson Law Group', city: 'Houston', state: 'TX',
  phone: '(713) 555-9999', website: 'https://www.wilsonlaw.com',
  email: 'charlie@wilsonlaw.com',
});

result = deduper.check({
  first_name: 'Other', last_name: 'Person',
  website: 'wilsonlaw.com',
});
assert('addToKnown domain dup: isDuplicate', result.isDuplicate, true);

result = deduper.check({
  first_name: 'Other', last_name: 'Person',
  email: 'charlie@wilsonlaw.com',
});
assert('addToKnown email dup: isDuplicate', result.isDuplicate, true);

// ===================== Stats =====================
console.log('\n=== Deduper: Stats ===');

const stats = deduper.getStats();
assert('Stats: checked > 0', stats.checked > 0, true);
assert('Stats: duplicates > 0', stats.duplicates > 0, true);
assert('Stats: unique > 0', stats.unique > 0, true);
console.log(`  Stats detail: ${JSON.stringify(stats)}`);

// ===================== Empty deduper =====================
console.log('\n=== Deduper: Empty constructor ===');

const emptyDeduper = new Deduper([]);
result = emptyDeduper.check({
  first_name: 'Anyone', last_name: 'Anywhere',
  city: 'Nowhere', state: 'XX',
});
assert('Empty deduper: nothing is a dup', result.isDuplicate, false);

// ===================== Summary =====================
console.log(`\n=== DEDUPER RESULTS: ${passed} passed, ${failed} failed ===\n`);
process.exit(failed > 0 ? 1 : 0);
