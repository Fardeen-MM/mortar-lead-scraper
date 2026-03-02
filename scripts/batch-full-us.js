#!/usr/bin/env node
/**
 * FULL MODE scrape — no --test flag.
 * Hits top 30 US cities with full geo-grid for maximum lead volume.
 * Each city should yield 100-500+ leads instead of ~35 in test mode.
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const LOCATIONS = [
  'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
  'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
  'Dallas, TX', 'Miami, FL', 'Atlanta, GA', 'Boston, MA',
  'Seattle, WA', 'Denver, CO', 'Nashville, TN', 'Charlotte, NC',
  'San Francisco, CA', 'Austin, TX', 'Portland, OR', 'Las Vegas, NV',
  'Tampa, FL', 'Orlando, FL', 'Minneapolis, MN', 'Cleveland, OH',
  'Pittsburgh, PA', 'St. Louis, MO', 'Jacksonville, FL', 'Salt Lake City, UT',
  'San Jose, CA', 'Indianapolis, IN',
];

const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 2;

const NICHE = 'law firms';
const logFile = path.join(__dirname, '..', 'output', 'batch-full-us-log.json');

let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function locationKey(city) {
  return city.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
}

function runLocation(city) {
  return new Promise((resolve) => {
    const key = locationKey(city);
    // NO --test flag = full mode
    const cmd = `node scripts/industry-scrape.js --niche "${NICHE}" --location "${city}"`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 45 * 60 * 1000, // 45 min timeout for full mode
      maxBuffer: 20 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[key] = { status: 'failed', city, error: errMsg, elapsed, timestamp: new Date().toISOString() };
        saveProgress();
        resolve({ city, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leads = parseInt((output.match(/Total leads:\s+(\d+)/) || [])[1] || '0');
      const emails = parseInt((output.match(/With email:\s+(\d+)/) || [])[1] || '0');
      const phones = parseInt((output.match(/With phone:\s+(\d+)/) || [])[1] || '0');
      const persons = parseInt((output.match(/With person name:\s+(\d+)/) || [])[1] || '0');
      const dms = parseInt((output.match(/Decision makers:\s+(\d+)/) || [])[1] || '0');

      progress[key] = { status: 'done', city, leads, emails, phones, persons, decisionMakers: dms, elapsed, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ city, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, completed = 0, failed = 0;

  const queue = [];
  for (const city of LOCATIONS) {
    const key = locationKey(city);
    if (resumeMode && progress[key] && progress[key].status === 'done') {
      totalLeads += progress[key].leads || 0;
      totalEmails += progress[key].emails || 0;
      completed++;
      continue;
    }
    queue.push(city);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  FULL MODE US SCRAPE — ${LOCATIONS.length} cities (NO test limit)`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    console.log(`  Starting: ${batch.join(' | ')}`);

    const results = await Promise.all(batch.map(city => runLocation(city)));

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const pct = r.leads > 0 ? Math.round(r.emails * 100 / r.leads) : 0;
        console.log(`  ✓ ${r.city}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.persons} ppl | ${r.dms} DMs | ${Math.round(r.elapsed/60)}min`);
      } else {
        failed++;
        console.log(`  ✗ ${r.city}: FAILED — ${r.error}`);
      }
    }
    queueIdx += CONCURRENCY;

    const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
    const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
    const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
    const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 600;
    const etaMin = Math.round(batchesLeft * avgBatchTime / 60);
    console.log(`  ── ${completed}/${LOCATIONS.length} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed | ETA: ~${etaMin}min ──`);
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  FULL US COMPLETE: ${completed}/${LOCATIONS.length} | ${totalLeads} leads | ${totalEmails} emails | ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
