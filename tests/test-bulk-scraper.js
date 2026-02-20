const BulkScraper = require('../lib/bulk-scraper');

async function test() {
  const bulk = new BulkScraper();

  // Test with just 2 scrapers in test mode
  console.log('Starting bulk test with 2 scrapers...');
  const results = await bulk.run({
    test: true,
    scrapers: ['FL', 'GA'],
  });

  console.log('\n=== RESULTS ===');
  console.log('Completed:', results.completed);
  console.log('Total leads:', results.totalLeads);
  console.log('Total emails:', results.totalEmails);
  console.log('New in DB:', results.totalNew);
  console.log('\nPer scraper:');
  results.results.forEach(r => {
    console.log(`  ${r.code}: ${r.leads} leads, ${r.emails} emails, ${r.newInDb} new (${r.time}s)`);
  });

  // Check DB stats
  const leadDb = require('../lib/lead-db');
  const stats = leadDb.getStats();
  console.log(`\nMaster DB: ${stats.total} total, ${stats.withEmail} email, ${stats.withPhone} phone`);
}

test().catch(console.error);
