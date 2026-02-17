/**
 * Test: lib/enricher.js — website derivation only (no Puppeteer/network)
 */
const Enricher = require('../lib/enricher');

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

// Create enricher with scraping disabled (no Puppeteer needed)
const enricher = new Enricher({
  deriveWebsite: true,
  scrapeWebsite: false,
  findLinkedIn: false,
  extractWithAI: false,
});

// ===================== deriveWebsiteFromEmail =====================
console.log('\n=== Enricher: deriveWebsiteFromEmail ===');

// Normal firm domain
assert('Firm email: jsmith@smithlaw.com', enricher.deriveWebsiteFromEmail('jsmith@smithlaw.com'), 'https://smithlaw.com');
assert('Firm email: alice@brownlaw.co.uk', enricher.deriveWebsiteFromEmail('alice@brownlaw.co.uk'), 'https://brownlaw.co.uk');
assert('Firm email: pierre@dupont-avocats.fr', enricher.deriveWebsiteFromEmail('pierre@dupont-avocats.fr'), 'https://dupont-avocats.fr');

// Freemail domains — should return null
assert('Freemail: gmail.com', enricher.deriveWebsiteFromEmail('john@gmail.com'), null);
assert('Freemail: yahoo.com', enricher.deriveWebsiteFromEmail('jane@yahoo.com'), null);
assert('Freemail: hotmail.com', enricher.deriveWebsiteFromEmail('bob@hotmail.com'), null);
assert('Freemail: outlook.com', enricher.deriveWebsiteFromEmail('sue@outlook.com'), null);
assert('Freemail: aol.com', enricher.deriveWebsiteFromEmail('old@aol.com'), null);
assert('Freemail: icloud.com', enricher.deriveWebsiteFromEmail('mac@icloud.com'), null);
assert('Freemail: protonmail.com', enricher.deriveWebsiteFromEmail('secure@protonmail.com'), null);
assert('Freemail: comcast.net', enricher.deriveWebsiteFromEmail('user@comcast.net'), null);

// Edge cases
assert('Edge: empty string', enricher.deriveWebsiteFromEmail(''), null);
assert('Edge: null', enricher.deriveWebsiteFromEmail(null), null);
assert('Edge: undefined', enricher.deriveWebsiteFromEmail(undefined), null);
assert('Edge: no @ sign', enricher.deriveWebsiteFromEmail('invalid-email'), null);

// Case handling
assert('Case: UPPERCASE@FIRM.COM', enricher.deriveWebsiteFromEmail('JOHN@SMITHLAW.COM'), 'https://smithlaw.com');

// ===================== enrichLead with deriveWebsite =====================
console.log('\n=== Enricher: enrichLead with deriveWebsite (sync portion) ===');

// Test that enrichLead fills in website from email when missing
const lead1 = {
  first_name: 'John', last_name: 'Smith',
  email: 'john@smithlaw.com', website: '',
};

// enrichLead is async (for Puppeteer tier) but website derivation is sync
// We can call it and check
(async () => {
  await enricher.enrichLead(lead1);
  assert('enrichLead: website derived from email', lead1.website, 'https://smithlaw.com');

  // Should NOT overwrite existing website
  const lead2 = {
    first_name: 'Jane', last_name: 'Doe',
    email: 'jane@doelaw.com', website: 'https://existing.com',
  };
  await enricher.enrichLead(lead2);
  assert('enrichLead: existing website preserved', lead2.website, 'https://existing.com');

  // Freemail should not derive website
  const lead3 = {
    first_name: 'Bob', last_name: 'Wilson',
    email: 'bob@gmail.com', website: '',
  };
  await enricher.enrichLead(lead3);
  assert('enrichLead: gmail does not derive website', lead3.website, '');

  // Stats check
  assert('Stats: websitesDerived is 1', enricher.stats.websitesDerived, 1);

  // ===================== Summary =====================
  console.log(`\n=== ENRICHER RESULTS: ${passed} passed, ${failed} failed ===\n`);
  process.exit(failed > 0 ? 1 : 0);
})();
