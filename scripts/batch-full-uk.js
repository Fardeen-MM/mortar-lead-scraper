#!/usr/bin/env node
/**
 * FULL MODE UK scrape — no --test flag.
 * Top 10 UK cities with full geo-grid.
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const LOCATIONS = [
  { code: 'LONDON', city: 'London, UK' },
  { code: 'MANCHESTER', city: 'Manchester, UK' },
  { code: 'BIRMINGHAM', city: 'Birmingham, UK' },
  { code: 'LEEDS', city: 'Leeds, UK' },
  { code: 'GLASGOW', city: 'Glasgow, UK' },
  { code: 'EDINBURGH', city: 'Edinburgh, UK' },
  { code: 'BRISTOL', city: 'Bristol, UK' },
  { code: 'LIVERPOOL', city: 'Liverpool, UK' },
  { code: 'CARDIFF', city: 'Cardiff, UK' },
  { code: 'BELFAST', city: 'Belfast, UK' },
];

const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 2;

const NICHE = 'solicitors';
const logFile = path.join(__dirname, '..', 'output', 'batch-full-uk-log.json');

let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function runLocation(loc) {
  return new Promise((resolve) => {
    // NO --test flag
    const cmd = `node scripts/industry-scrape.js --niche "${NICHE}" --location "${loc.city}"`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 45 * 60 * 1000,
      maxBuffer: 20 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[loc.code] = { status: 'failed', city: loc.city, error: errMsg, elapsed, timestamp: new Date().toISOString() };
        saveProgress();
        resolve({ code: loc.code, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leads = parseInt((output.match(/Total leads:\s+(\d+)/) || [])[1] || '0');
      const emails = parseInt((output.match(/With email:\s+(\d+)/) || [])[1] || '0');
      const phones = parseInt((output.match(/With phone:\s+(\d+)/) || [])[1] || '0');
      const persons = parseInt((output.match(/With person name:\s+(\d+)/) || [])[1] || '0');
      const dms = parseInt((output.match(/Decision makers:\s+(\d+)/) || [])[1] || '0');

      progress[loc.code] = { status: 'done', city: loc.city, leads, emails, phones, persons, decisionMakers: dms, elapsed, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ code: loc.code, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, completed = 0, failed = 0;

  const queue = [];
  for (const loc of LOCATIONS) {
    if (resumeMode && progress[loc.code] && progress[loc.code].status === 'done') {
      totalLeads += progress[loc.code].leads || 0;
      totalEmails += progress[loc.code].emails || 0;
      completed++;
      continue;
    }
    queue.push(loc);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  FULL MODE UK SCRAPE — ${LOCATIONS.length} cities (NO test limit)`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    console.log(`  Starting: ${batch.map(s => s.code).join(', ')}`);

    const results = await Promise.all(batch.map(loc => runLocation(loc)));

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const pct = r.leads > 0 ? Math.round(r.emails * 100 / r.leads) : 0;
        console.log(`  ✓ ${r.code}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.persons} ppl | ${r.dms} DMs | ${Math.round(r.elapsed/60)}min`);
      } else {
        failed++;
        console.log(`  ✗ ${r.code}: FAILED — ${r.error}`);
      }
    }
    queueIdx += CONCURRENCY;
    console.log(`  ── ${completed}/${LOCATIONS.length} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed ──`);
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  FULL UK COMPLETE: ${completed}/${LOCATIONS.length} | ${totalLeads} leads | ${totalEmails} emails | ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
