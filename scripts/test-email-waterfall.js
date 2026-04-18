/**
 * Comprehensive test of the email waterfall engine.
 * Tests Microsoft, Google, SMTP, and mixed domains.
 */
const { EmailWaterfall } = require('../lib/email-waterfall');

async function main() {
  const waterfall = new EmailWaterfall();

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║       Email Waterfall Engine — Comprehensive Test        ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');

  // 20 test leads across different provider types
  const testLeads = [
    // Microsoft 365 domains (should verify via GetCredentialType)
    { first_name: 'Satya', last_name: 'Nadella', domain: 'microsoft.com' },
    { first_name: 'Michael', last_name: 'Johnson', domain: 'mjlawgroup.com' },

    // Google Workspace custom domains (NEW: should verify via SMTP now!)
    { first_name: 'Larry', last_name: 'Page', domain: 'google.com' },
    { first_name: 'John', last_name: 'Morgan', domain: 'forthepeople.com' },
    { first_name: 'Ben', last_name: 'Crump', domain: 'bencrump.com' },

    // Law firms — mix of providers
    { first_name: 'Mark', last_name: 'Lanier', domain: 'lanierlawfirm.com' },
    { first_name: 'Tom', last_name: 'Girardi', domain: 'girardikeese.com' },
    { first_name: 'Thomas', last_name: 'Kline', domain: 'klinespecter.com' },

    // Large tech companies (various providers)
    { first_name: 'Tim', last_name: 'Cook', domain: 'apple.com' },
    { first_name: 'Jensen', last_name: 'Huang', domain: 'nvidia.com' },

    // Known Google Workspace law firms (SMTP should verify)
    { first_name: 'Maria', last_name: 'Garcia', domain: 'garcialaw.com' },
    { first_name: 'Robert', last_name: 'Smith', domain: 'smithlawfirm.com' },

    // Companies with email gateways (Proofpoint, Mimecast)
    { first_name: 'Brian', last_name: 'Moynihan', domain: 'bankofamerica.com' },
    { first_name: 'Jamie', last_name: 'Dimon', domain: 'jpmorgan.com' },

    // Zoho / other providers
    { first_name: 'Sarah', last_name: 'Jones', domain: 'joneslegalgroup.com' },
    { first_name: 'James', last_name: 'Wilson', domain: 'wilsonattorneys.com' },

    // Small firms — likely self-hosted SMTP
    { first_name: 'David', last_name: 'Parker', domain: 'parkerlawfirm.com' },
    { first_name: 'Lisa', last_name: 'Brown', domain: 'brownandassociates.com' },

    // Known real leads (sanity check)
    { first_name: 'Elon', last_name: 'Musk', domain: 'tesla.com' },
    { first_name: 'Mark', last_name: 'Zuckerberg', domain: 'meta.com' },
  ];

  console.log(`Testing ${testLeads.length} leads...\n`);

  const startTime = Date.now();
  const results = await waterfall.findEmailsBatch(testLeads, 3, (found, total) => {
    if (total % 5 === 0 || total === testLeads.length) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(`  [${total}/${testLeads.length}] ${found} found | ${elapsed}s`);
    }
  });

  // Print results table
  console.log('\n  RESULTS:');
  console.log('  ' + '─'.repeat(90));
  console.log(`  ${'Name'.padEnd(25)} ${'Domain'.padEnd(25)} ${'Email'.padEnd(30)} ${'Source'.padEnd(18)} Conf`);
  console.log('  ' + '─'.repeat(90));

  for (let i = 0; i < testLeads.length; i++) {
    const l = testLeads[i];
    const r = results[i];
    const name = `${l.first_name} ${l.last_name}`.padEnd(25);
    const domain = l.domain.padEnd(25);
    if (r) {
      const email = r.email.padEnd(30);
      const source = r.source.padEnd(18);
      const conf = `${r.confidence}%`;
      console.log(`  ${name} ${domain} ${email} ${source} ${conf}`);
    } else {
      console.log(`  ${name} ${domain} ${'NOT FOUND'.padEnd(30)} ${''.padEnd(18)}`);
    }
  }
  console.log('  ' + '─'.repeat(90));

  // Stats
  const stats = waterfall.getStats();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n  Total: ${stats.total} | Found: ${stats.found} (${Math.round(stats.found/stats.total*100)}%) | Time: ${elapsed}s`);

  if (Object.keys(stats.bySource).length > 0) {
    console.log('  By verification method:');
    for (const [src, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${src.padEnd(25)} ${count}`);
    }
  }
  if (Object.keys(stats.byProvider).length > 0) {
    console.log('  By email provider:');
    for (const [prov, count] of Object.entries(stats.byProvider).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${prov.padEnd(25)} ${count}`);
    }
  }

  // Pattern cache
  if (waterfall.patternCache.size > 0) {
    console.log('  Learned patterns:');
    for (const [domain, info] of waterfall.patternCache) {
      console.log(`    ${domain.padEnd(25)} ${info.pattern} (confidence: ${info.confidence}%, count: ${info.count})`);
    }
  }

  console.log('');
}

main().catch(e => console.error('Error:', e.message, e.stack));
