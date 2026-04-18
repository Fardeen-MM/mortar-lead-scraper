/**
 * Scale test of the email waterfall engine using real law firm leads.
 * Picks 50 enrichable leads from US-LAWFIRMS-MASTER.csv and runs waterfall.
 */
const fs = require('fs');
const { EmailWaterfall } = require('../lib/email-waterfall');

function parseCSVLine(line) {
  const vals = [];
  let current = '';
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"') { inQuotes = !inQuotes; continue; }
    if (ch === ',' && !inQuotes) { vals.push(current.trim()); current = ''; continue; }
    current += ch;
  }
  vals.push(current.trim());
  return vals;
}

async function main() {
  const content = fs.readFileSync('output/US-LAWFIRMS-MASTER.csv', 'utf8');
  const lines = content.trim().split('\n');
  const header = parseCSVLine(lines[0]);
  const fnIdx = header.indexOf('first_name');
  const lnIdx = header.indexOf('last_name');
  const emailIdx = header.indexOf('email');
  const websiteIdx = header.indexOf('website');
  const domainIdx = header.indexOf('domain');

  // Extract enrichable leads (no email, full name, has domain)
  const enrichable = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    const fn = (cols[fnIdx] || '').trim();
    const ln = (cols[lnIdx] || '').trim();
    const email = (cols[emailIdx] || '').trim();
    const website = (cols[websiteIdx] || '').trim();
    let domain = (cols[domainIdx] || '').trim();
    if (!domain && website) {
      const m = website.match(/https?:\/\/(?:www\.)?([^\/]+)/);
      if (m) domain = m[1];
    }
    if (!email && fn.length > 1 && ln.length > 2 && domain) {
      enrichable.push({ first_name: fn, last_name: ln, domain, website });
    }
  }

  // Take first N leads (all enrichable for full test, or set smaller for quick test)
  const N = parseInt(process.argv[2] || '50') || 50;
  const shuffled = enrichable.sort(() => Math.random() - 0.5);
  const testBatch = shuffled.slice(0, Math.min(N, enrichable.length));

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log(`║       Email Waterfall — Scale Test (${testBatch.length} Real Leads)${' '.repeat(Math.max(0, 9 - String(testBatch.length).length))}║`);
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Source:     US-LAWFIRMS-MASTER.csv`);
  console.log(`  Enrichable: ${enrichable.length} total`);
  console.log(`  Testing:    ${testBatch.length} leads`);
  console.log(`  Concurrency: 5\n`);

  const waterfall = new EmailWaterfall();
  const startTime = Date.now();

  const results = await waterfall.findEmailsBatch(testBatch, 5, (found, total) => {
    if (total % 10 === 0 || total === testBatch.length) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const pct = Math.round(found / Math.max(total, 1) * 100);
      console.log(`  [${total}/${testBatch.length}] ${found} found (${pct}%) | ${elapsed}s`);
    }
  });

  // Results table
  console.log('\n  RESULTS:');
  console.log('  ' + '─'.repeat(100));
  console.log(`  ${'Name'.padEnd(25)} ${'Domain'.padEnd(30)} ${'Email'.padEnd(35)} ${'Source'.padEnd(20)} Conf`);
  console.log('  ' + '─'.repeat(100));

  let verified = 0, guessed = 0, notFound = 0;
  for (let i = 0; i < testBatch.length; i++) {
    const l = testBatch[i];
    const r = results[i];
    const name = `${l.first_name} ${l.last_name}`.substring(0, 24).padEnd(25);
    const domain = l.domain.substring(0, 29).padEnd(30);
    if (r) {
      const email = r.email.substring(0, 34).padEnd(35);
      const source = r.source.padEnd(20);
      const conf = `${r.confidence}%`;
      console.log(`  ${name} ${domain} ${email} ${source} ${conf}`);
      if (r.confidence >= 80) verified++;
      else guessed++;
    } else {
      console.log(`  ${name} ${domain} ${'NOT FOUND'.padEnd(35)} ${''.padEnd(20)}`);
      notFound++;
    }
  }
  console.log('  ' + '─'.repeat(100));

  // Summary
  const stats = waterfall.getStats();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n  Total: ${stats.total} | Found: ${stats.found} (${Math.round(stats.found/stats.total*100)}%)`);
  console.log(`  Verified (80%+): ${verified} | Guessed (<80%): ${guessed} | Not Found: ${notFound}`);
  console.log(`  Time: ${elapsed}s (${(elapsed / testBatch.length).toFixed(1)}s per lead)\n`);

  if (Object.keys(stats.bySource).length > 0) {
    console.log('  By verification method:');
    for (const [src, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
      const pct = Math.round(count / stats.found * 100);
      console.log(`    ${src.padEnd(28)} ${String(count).padStart(3)} (${pct}%)`);
    }
  }
  if (Object.keys(stats.byProvider).length > 0) {
    console.log('  By email provider:');
    for (const [prov, count] of Object.entries(stats.byProvider).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${prov.padEnd(28)} ${String(count).padStart(3)}`);
    }
  }

  console.log(`\n  Domain patterns learned: ${waterfall.patternCache.size}`);
  console.log(`  Catch-all domains found: ${[...waterfall.catchAllCache.entries()].filter(([,v]) => v).length}`);
  console.log('');
}

main().catch(e => console.error('Error:', e.message, e.stack));
