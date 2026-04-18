#!/usr/bin/env node
/**
 * Australian Google Maps multi-niche scraper.
 * Scrapes 6 niches across Sydney, Melbourne, Brisbane.
 * Each niche runs in a child process to prevent memory buildup.
 * Saves CSV after each niche. Logs to stdout.
 *
 * Usage: node scripts/au-gmaps-scrape.js
 *   or:  nohup node scripts/au-gmaps-scrape.js > output/au-gmaps-scrape.log 2>&1 &
 */

const { fork } = require('child_process');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const WORKER = path.join(__dirname, 'au-gmaps-worker.js');
const niches = ['real estate agent', 'dentist', 'physiotherapist', 'accountant', 'plumber'];
const cities = ['Sydney, Australia', 'Melbourne, Australia', 'Brisbane, Australia'];

function runNicheInChild(niche) {
  return new Promise((resolve) => {
    const child = fork(WORKER, [], {
      env: { ...process.env, NICHE: niche, CITIES: JSON.stringify(cities), OUTPUT_DIR },
      stdio: ['pipe', 'inherit', 'inherit', 'ipc'],
    });

    child.on('message', (msg) => resolve(msg));
    child.on('error', (err) => {
      console.error(`  Child process error for ${niche}: ${err.message}`);
      resolve({ leads: 0, phones: 0, websites: 0, emails: 0, file: null });
    });
    child.on('exit', (code) => {
      if (code !== 0) {
        console.error(`  Child process for ${niche} exited with code ${code}`);
        resolve({ leads: 0, phones: 0, websites: 0, emails: 0, file: null });
      }
    });
  });
}

(async () => {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const grandTotal = { leads: 0, phones: 0, websites: 0, emails: 0, files: [] };
  const startTime = Date.now();

  for (const niche of niches) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`=== ${niche.toUpperCase()} — Starting (${cities.length} cities) ===`);
    console.log(`${'='.repeat(60)}`);

    const result = await runNicheInChild(niche);

    console.log(`\n  ${niche.toUpperCase()} SUMMARY: ${result.leads} leads | ${result.phones} phones | ${result.websites} websites | ${result.emails} emails`);

    if (result.file) {
      console.log(`  Written: ${result.file}`);
      grandTotal.files.push(path.basename(result.file));
    }

    grandTotal.leads += result.leads;
    grandTotal.phones += result.phones;
    grandTotal.websites += result.websites;
    grandTotal.emails += result.emails;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`=== ALL DONE in ${totalElapsed} minutes ===`);
  console.log(`Total: ${grandTotal.leads} leads | ${grandTotal.phones} phones | ${grandTotal.websites} websites | ${grandTotal.emails} emails`);
  console.log(`Files: ${grandTotal.files.join(', ')}`);
  console.log(`${'='.repeat(60)}`);
})();
