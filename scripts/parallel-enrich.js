#!/usr/bin/env node
/**
 * parallel-enrich.js — Run all enrichment steps concurrently, not sequentially
 *
 * Runs scorer + inferrer + email-gen in PARALLEL, then exports.
 * Cycles every 5 minutes instead of 15.
 *
 * Usage: node scripts/parallel-enrich.js [--until ISO_DATE]
 */

const { execSync } = require('child_process');
const path = require('path');

let stopTime = null;
const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--until' && args[i + 1]) {
    stopTime = new Date(args[i + 1]);
  }
}

const SCRIPTS_DIR = path.join(__dirname);
const ROOT_DIR = path.join(__dirname, '..');
const CYCLE_INTERVAL_MS = 5 * 60 * 1000;

function run(scriptName) {
  const scriptPath = path.join(SCRIPTS_DIR, scriptName);
  try {
    const output = execSync(`node "${scriptPath}"`, {
      timeout: 180_000,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      cwd: ROOT_DIR
    });
    const lines = output.split('\n');
    const keyLines = lines.filter(l =>
      l.includes('Total') || l.includes('Generated') || l.includes('Done') ||
      l.includes('scored') || l.includes('filled') || l.includes('rows')
    ).slice(0, 3);
    return keyLines.join(' | ') || 'OK';
  } catch (err) {
    return 'ERROR: ' + (err.stderr || err.message || '').slice(0, 150);
  }
}

function getStats() {
  try {
    return execSync('node scripts/_quick-stats.js', {
      encoding: 'utf8',
      timeout: 10000,
      cwd: ROOT_DIR
    }).trim();
  } catch {
    return '(stats unavailable)';
  }
}

async function runParallel(scripts) {
  return Promise.all(scripts.map(s =>
    new Promise(resolve => {
      const result = run(s);
      resolve({ script: s, result });
    })
  ));
}

async function cycle(num) {
  const now = new Date();
  console.log(`\n${'='.repeat(60)}`);
  console.log(`PARALLEL ENRICHMENT CYCLE ${num} | ${now.toLocaleString()}`);
  console.log(`Before: ${getStats()}`);
  console.log('='.repeat(60));

  // Run scorer + inferrer + email-gen IN PARALLEL
  console.log('\n[PARALLEL] Running scorer + inferrer + email-gen...');
  const t1 = Date.now();
  const results = await runParallel([
    'decision-maker-scorer.js',
    'practice-area-inferrer.js',
    'email-generator.js'
  ]);
  const elapsed1 = ((Date.now() - t1) / 1000).toFixed(0);
  for (const r of results) {
    console.log(`  ${r.script}: ${r.result}`);
  }
  console.log(`  Parallel step took ${elapsed1}s`);

  // Then export
  console.log('\n[EXPORT] Running CSV export...');
  const t2 = Date.now();
  const exportResult = run('export-cold-email.js');
  const elapsed2 = ((Date.now() - t2) / 1000).toFixed(0);
  console.log(`  ${exportResult}`);
  console.log(`  Export took ${elapsed2}s`);

  console.log(`\nAfter: ${getStats()}`);
}

async function main() {
  console.log('=== Parallel Enrichment Engine ===');
  console.log(`Stop time: ${stopTime ? stopTime.toLocaleString() : 'none (manual stop)'}`);

  let cycleNum = 1;
  while (true) {
    if (stopTime && new Date() >= stopTime) {
      console.log('\nReached stop time. Exiting.');
      break;
    }

    await cycle(cycleNum++);

    if (stopTime && new Date() >= stopTime) break;

    console.log(`\nSleeping 5 minutes until next cycle...`);
    await new Promise(r => setTimeout(r, CYCLE_INTERVAL_MS));
  }

  console.log('\nParallel enrichment complete.');
}

main();
