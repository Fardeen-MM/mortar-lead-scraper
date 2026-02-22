#!/usr/bin/env node
/**
 * Comprehensive verification script — tests all scrapers, registry, metadata, normalizer, and pipeline.
 */

const { getRegistry, getScraperMetadata } = require('../lib/registry');
const { getStateName, getCountry } = require('../lib/state-metadata');
const { normalizeState, normalizePhone, normalizeFirmName, normalizeRecord, extractDomain } = require('../lib/normalizer');
const Deduper = require('../lib/deduper');
const metrics = require('../lib/metrics');

let totalPass = 0;
let totalFail = 0;
const failures = [];

function assert(label, actual, expected) {
  if (actual === expected) {
    totalPass++;
  } else {
    totalFail++;
    failures.push(`  FAIL: ${label} => "${actual}" (expected "${expected}")`);
  }
}

function assertTruthy(label, value) {
  if (value) {
    totalPass++;
  } else {
    totalFail++;
    failures.push(`  FAIL: ${label} => falsy`);
  }
}

// ============================================================
// 1. SCRAPER LOAD TEST
// ============================================================
console.log('\n=== 1. SCRAPER LOAD TEST ===');
const scrapers = getRegistry();
const codes = Object.keys(scrapers).sort();
let scraperPass = 0;
let scraperFail = 0;
const scraperErrors = [];

for (const code of codes) {
  try {
    const s = scrapers[code]();
    if (typeof s.name !== 'string' || s.name === '') throw new Error('missing name');
    if (typeof s.stateCode !== 'string') throw new Error('missing stateCode');
    if (typeof s.baseUrl !== 'string') throw new Error('missing baseUrl');
    if (typeof s.search !== 'function') throw new Error('missing search()');
    if (!s.defaultCities || s.defaultCities.length === 0) throw new Error('missing defaultCities');
    if (!s.practiceAreaCodes) throw new Error('missing practiceAreaCodes property');
    scraperPass++;
  } catch (e) {
    scraperFail++;
    scraperErrors.push(`  ${code}: ${e.message}`);
  }
}
console.log(`  Scrapers: ${scraperPass} passed, ${scraperFail} failed out of ${codes.length}`);
if (scraperErrors.length) scraperErrors.forEach(e => console.log(e));
totalPass += scraperPass;
totalFail += scraperFail;

// ============================================================
// 2. EXPECTED COVERAGE CHECK
// ============================================================
console.log('\n=== 2. JURISDICTION COVERAGE ===');
const expectedUS = [
  'AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN',
  'KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
  'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA',
  'WI','WV','WY'
];
const expectedCA = [
  'CA-AB','CA-BC','CA-MB','CA-NB','CA-NL','CA-NS','CA-NT','CA-NU','CA-ON','CA-PE','CA-SK','CA-YT'
];
const expectedUK = ['UK-EW','UK-EW-BAR','UK-NI','UK-SC'];
const allExpected = [...expectedUS, ...expectedCA, ...expectedUK];

const missingFromRegistry = allExpected.filter(c => !scrapers[c]);
const extraInRegistry = codes.filter(c => !allExpected.includes(c));

console.log(`  Expected: ${allExpected.length} jurisdictions`);
console.log(`  Registered: ${codes.length} scrapers`);
if (missingFromRegistry.length) {
  console.log(`  MISSING: ${missingFromRegistry.join(', ')}`);
  totalFail += missingFromRegistry.length;
} else {
  console.log('  All jurisdictions covered');
  totalPass++;
}
if (extraInRegistry.length) {
  console.log(`  Extra (unexpected): ${extraInRegistry.join(', ')}`);
}

// ============================================================
// 3. REGISTRY METADATA TEST
// ============================================================
console.log('\n=== 3. REGISTRY METADATA ===');
const meta = getScraperMetadata();
const metaEntries = Object.values(meta);
assert('metadata count', metaEntries.length, codes.length);

const countries = [...new Set(metaEntries.map(m => m.country))].sort();
console.log(`  Countries: ${countries.join(', ')}`);
assertTruthy('has US country', countries.includes('US'));
assertTruthy('has CA country', countries.includes('CA'));
assertTruthy('has UK country', countries.includes('UK'));

const noCountry = metaEntries.filter(m => !m.country);
if (noCountry.length) {
  console.log(`  Missing country: ${noCountry.map(m => m.stateCode).join(', ')}`);
  totalFail++;
} else {
  console.log('  All scrapers have country field');
  totalPass++;
}

// ============================================================
// 4. STATE METADATA TEST
// ============================================================
console.log('\n=== 4. STATE METADATA ===');
assert('getStateName FL', getStateName('FL'), 'Florida');
assert('getStateName CA-ON', getStateName('CA-ON'), 'Ontario');
assert('getStateName UK-EW', getStateName('UK-EW'), 'England & Wales');
assert('getStateName UK-EW-BAR', getStateName('UK-EW-BAR'), 'England & Wales (Barristers)');
assert('getStateName UK-SC', getStateName('UK-SC'), 'Scotland');
assert('getStateName UK-NI', getStateName('UK-NI'), 'Northern Ireland');
assert('getCountry FL', getCountry('FL'), 'US');
assert('getCountry CA-ON', getCountry('CA-ON'), 'CA');
assert('getCountry UK-EW', getCountry('UK-EW'), 'UK');
assert('getCountry NV', getCountry('NV'), 'US');
assert('getCountry SD', getCountry('SD'), 'US');
assert('getCountry WY', getCountry('WY'), 'US');

// ============================================================
// 5. NORMALIZER TESTS
// ============================================================
console.log('\n=== 5. NORMALIZER ===');
// normalizeState
assert('normalizeState FL', normalizeState('FL'), 'FL');
assert('normalizeState fl', normalizeState('fl'), 'FL');
assert('normalizeState ca-on', normalizeState('ca-on'), 'CA-ON');
assert('normalizeState UK-EW', normalizeState('UK-EW'), 'UK-EW');
assert('normalizeState UK-EW-BAR', normalizeState('UK-EW-BAR'), 'UK-EW-BAR');
assert('normalizeState empty', normalizeState(''), '');
assert('normalizeState null', normalizeState(null), '');

// normalizePhone
assert('normalizePhone US', normalizePhone('(305) 555-1234'), '3055551234');
assert('normalizePhone +1', normalizePhone('+1 305 555 1234'), '3055551234');
assert('normalizePhone 11-digit', normalizePhone('13055551234'), '3055551234');
assert('normalizePhone UK +44', normalizePhone('+44 20 7946 0958'), '2079460958');
assert('normalizePhone empty', normalizePhone(''), '');
assert('normalizePhone null', normalizePhone(null), '');

// normalizeFirmName
assert('normalizeFirmName LLC', normalizeFirmName('Smith LLC'), 'smith');
assert('normalizeFirmName LLP', normalizeFirmName('Baker McKenzie LLP'), 'baker mckenzie');
assert('normalizeFirmName empty', normalizeFirmName(''), '');
assert('normalizeFirmName null', normalizeFirmName(null), '');

// extractDomain
assert('extractDomain URL', extractDomain('https://www.smithlaw.com/about'), 'smithlaw.com');
assert('extractDomain bare', extractDomain('smithlaw.com'), 'smithlaw.com');
assert('extractDomain empty', extractDomain(''), '');

// normalizeRecord
const record = {
  first_name: 'John', last_name: 'Smith', firm_name: 'Smith Law LLC',
  city: 'Miami', state: 'FL', phone: '(305) 555-1234',
  website: 'https://www.smithlaw.com', email: 'John@SmithLaw.com',
};
const normalized = normalizeRecord(record);
assertTruthy('normalizeRecord has _norm', normalized._norm);
assert('normalizeRecord firstName', normalized._norm.firstName, 'john');
assert('normalizeRecord lastName', normalized._norm.lastName, 'smith');
assert('normalizeRecord phone', normalized._norm.phone, '3055551234');
assert('normalizeRecord domain', normalized._norm.domain, 'smithlaw.com');
assert('normalizeRecord email', normalized._norm.email, 'john@smithlaw.com');
assert('normalizeRecord state', normalized._norm.state, 'FL');

// ============================================================
// 6. DEDUPER TEST
// ============================================================
console.log('\n=== 6. DEDUPER ===');
const existing = [
  { first_name: 'Jane', last_name: 'Doe', firm_name: 'Doe Law LLC', city: 'Miami', state: 'FL', phone: '(305) 555-9999', website: 'https://doelaw.com', email: 'jane@doelaw.com' },
];
const deduper = new Deduper(existing);

// Should be duplicate (same email)
const dup1 = deduper.check({ first_name: 'Jane', last_name: 'Doe', email: 'jane@doelaw.com', city: 'Miami', state: 'FL' });
assertTruthy('dedup email match', dup1.isDuplicate);

// Should be unique
const unique1 = deduper.check({ first_name: 'Bob', last_name: 'Jones', email: 'bob@jones.com', city: 'Tampa', state: 'FL', phone: '(813) 555-0001' });
assert('dedup unique', unique1.isDuplicate, false);

// ============================================================
// 7. METRICS TEST
// ============================================================
console.log('\n=== 7. METRICS ===');
metrics.reset();
metrics.record('florida', 'request');
metrics.record('florida', 'success', { responseTime: 200 });
const summary = metrics.getSummary();
assert('metrics requests', summary.florida.requests, 1);
assert('metrics successes', summary.florida.successes, 1);
metrics.recordJob({ state: 'FL', leads: 50 });
const jobs = metrics.getRecentJobs();
assert('metrics job count', jobs.length, 1);
metrics.reset();

// ============================================================
// 8. LOGGER TEST
// ============================================================
console.log('\n=== 8. LOGGER ===');
try {
  const { createLogger, readLogTail } = require('../lib/logger');
  assertTruthy('createLogger exists', typeof createLogger === 'function');
  assertTruthy('readLogTail exists', typeof readLogTail === 'function');

  // Create a logger and test it doesn't crash
  const { EventEmitter } = require('events');
  const emitter = new EventEmitter();
  const log = createLogger(emitter);
  assertTruthy('logger has info', typeof log.info === 'function');
  assertTruthy('logger has error', typeof log.error === 'function');
  assertTruthy('logger has warn', typeof log.warn === 'function');
  assertTruthy('logger has success', typeof log.success === 'function');
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: Logger: ${e.message}`);
}

// ============================================================
// 9. CSV HANDLER TEST
// ============================================================
console.log('\n=== 9. CSV HANDLER ===');
try {
  const { generateOutputPath } = require('../lib/csv-handler');
  const path = generateOutputPath('FL', 'immigration');
  assertTruthy('generateOutputPath contains FL', path.includes('fl'));
  assertTruthy('generateOutputPath contains immigration', path.includes('immigration'));
  assertTruthy('generateOutputPath ends with .csv', path.endsWith('.csv'));
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: CSV Handler: ${e.message}`);
}

// ============================================================
// 10. PIPELINE MODULE TEST
// ============================================================
console.log('\n=== 10. PIPELINE MODULE ===');
try {
  const { runPipeline } = require('../lib/pipeline');
  assertTruthy('runPipeline exists', typeof runPipeline === 'function');

  // Test with invalid state (should emit error, not crash)
  const emitter = runPipeline({ state: 'INVALID' });
  assertTruthy('pipeline returns EventEmitter', typeof emitter.on === 'function');
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: Pipeline: ${e.message}`);
}

// ============================================================
// 11. SERVER MODULE LOAD TEST
// ============================================================
console.log('\n=== 11. SERVER MODULE ===');
try {
  // Just require the express app setup parts without starting the server
  const express = require('express');
  assertTruthy('express loaded', typeof express === 'function');

  const { readLogTail } = require('../lib/logger');
  assertTruthy('readLogTail for debug endpoint', typeof readLogTail === 'function');
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: Server module: ${e.message}`);
}

// ============================================================
// 12. LEAD DB MODULE
// ============================================================
console.log('\n=== 12. LEAD DB MODULE ===');
try {
  const leadDb = require('../lib/lead-db');
  assertTruthy('getStats function exists', typeof leadDb.getStats === 'function');
  assertTruthy('searchLeads function exists', typeof leadDb.searchLeads === 'function');
  assertTruthy('exportLeads function exists', typeof leadDb.exportLeads === 'function');
  assertTruthy('updateLead function exists', typeof leadDb.updateLead === 'function');
  assertTruthy('recordWaterfallRun function exists', typeof leadDb.recordWaterfallRun === 'function');
  assertTruthy('getWaterfallRuns function exists', typeof leadDb.getWaterfallRuns === 'function');
  assertTruthy('getWaterfallSummary function exists', typeof leadDb.getWaterfallSummary === 'function');
  assertTruthy('getEnrichmentStats function exists', typeof leadDb.getEnrichmentStats === 'function');
  assertTruthy('getDb function exists', typeof leadDb.getDb === 'function');

  // Test getStats returns expected shape
  const stats = leadDb.getStats();
  assertTruthy('getStats returns object with total', typeof stats.total === 'number');
  assertTruthy('getStats returns withEmail', typeof stats.withEmail === 'number');
  assertTruthy('getStats returns withPhone', typeof stats.withPhone === 'number');

  // Test searchLeads with enrichedAfter param
  const result = leadDb.searchLeads('', { limit: 1, enrichedAfter: '2020-01-01' });
  assertTruthy('searchLeads with enrichedAfter returns leads array', Array.isArray(result.leads));

  // Test exportLeads with hasWebsite param
  const exported = leadDb.exportLeads({ hasWebsite: true });
  assertTruthy('exportLeads with hasWebsite returns array', Array.isArray(exported));

  // Test waterfall run history
  const runs = leadDb.getWaterfallRuns(5);
  assertTruthy('getWaterfallRuns returns array', Array.isArray(runs));
  const summary = leadDb.getWaterfallSummary();
  assertTruthy('getWaterfallSummary returns totalRuns', typeof summary.totalRuns === 'number');
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: Lead DB module: ${e.message}`);
}

// ============================================================
// 13. WATERFALL MODULE
// ============================================================
console.log('\n=== 13. WATERFALL MODULE ===');
try {
  const { runWaterfall } = require('../lib/waterfall');
  assertTruthy('runWaterfall function exists', typeof runWaterfall === 'function');
} catch (e) {
  totalFail++;
  failures.push(`  FAIL: Waterfall module: ${e.message}`);
}

// ============================================================
// SUMMARY
// ============================================================
console.log('\n' + '='.repeat(50));
console.log(`TOTAL: ${totalPass} passed, ${totalFail} failed`);
if (failures.length) {
  console.log('\nFAILURES:');
  failures.forEach(f => console.log(f));
}
console.log('='.repeat(50));

process.exit(totalFail > 0 ? 1 : 0);
