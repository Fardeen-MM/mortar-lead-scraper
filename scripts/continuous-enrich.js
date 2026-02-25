#!/usr/bin/env node
/**
 * Continuous Enrichment Loop — runs enrichment scripts every 15 minutes
 * while the marathon scraper is running.
 *
 * Runs: decision-maker scorer, practice area inferrer, email generator
 * Then re-exports CSVs.
 */

const { execSync } = require('child_process');
const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = path.join(__dirname, '..', 'data', 'leads.db');
const INTERVAL = 15 * 60 * 1000; // 15 minutes

// Parse stop time
let stopTime = new Date();
stopTime.setHours(23, 0, 0, 0); // Default: 11 PM
const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--until' && args[i + 1]) {
    stopTime = new Date(args[i + 1]);
    i++;
  }
}

function getStats() {
  const db = new Database(DB_PATH, { readonly: true });
  const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
  const email = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
  const generated = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'generated_pattern'").get().c;
  const icp50 = db.prepare("SELECT COUNT(*) as c FROM leads WHERE icp_score >= 50").get().c;
  const practice = db.prepare("SELECT COUNT(*) as c FROM leads WHERE practice_area IS NOT NULL AND practice_area != ''").get().c;
  db.close();
  return { total, email, generated, icp50, practice };
}

function runScript(name) {
  try {
    const output = execSync(`node ${path.join(__dirname, name)}`, {
      encoding: 'utf8',
      maxBuffer: 10 * 1024 * 1024,
      cwd: path.join(__dirname, '..'),
      timeout: 5 * 60 * 1000, // 5 min max per script
    });
    // Extract key line from output
    const lines = output.split('\n').filter(l => l.includes('Done') || l.includes('Total') || l.includes('Generated') || l.includes('new emails'));
    return lines.join(' | ');
  } catch (err) {
    return `ERROR: ${err.message.substring(0, 100)}`;
  }
}

async function loop() {
  let cycle = 0;

  while (Date.now() < stopTime) {
    cycle++;
    const before = getStats();
    console.log(`\n${'='.repeat(60)}`);
    console.log(`ENRICHMENT CYCLE ${cycle} | ${new Date().toLocaleString()}`);
    console.log(`Before: ${before.total} leads, ${before.email} email, ${before.icp50} decision-makers`);
    console.log('='.repeat(60));

    // 1. Decision-maker scorer
    console.log('\n[1/4] Decision-maker scorer...');
    console.log('  ', runScript('decision-maker-scorer.js'));

    // 2. Practice area inferrer
    console.log('\n[2/4] Practice area inferrer...');
    console.log('  ', runScript('practice-area-inferrer.js'));

    // 3. Email generator
    console.log('\n[3/4] Email generator...');
    console.log('  ', runScript('email-generator.js'));

    // 4. Export CSVs
    console.log('\n[4/4] CSV export...');
    console.log('  ', runScript('export-cold-email.js'));

    const after = getStats();
    console.log(`\nAfter: ${after.total} leads, ${after.email} email (+${after.email - before.email}), ${after.icp50} DMs (+${after.icp50 - before.icp50})`);

    if (Date.now() >= stopTime) break;

    console.log(`\nSleeping ${INTERVAL / 60000} minutes until next cycle...`);
    await new Promise(resolve => setTimeout(resolve, INTERVAL));
  }

  console.log('\nContinuous enrichment complete.');
}

console.log('CONTINUOUS ENRICHMENT LOOP');
console.log(`Stop: ${stopTime.toLocaleString()}`);
loop().catch(err => {
  console.error('Enrichment loop error:', err);
  process.exit(1);
});
