#!/usr/bin/env node
/**
 * Scrape law firms by specific practice areas in top US markets.
 * Different practice area searches surface different firms that don't
 * appear in generic "law firms" results.
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const PRACTICE_AREAS = [
  'personal injury lawyer',
  'family law attorney',
  'criminal defense lawyer',
  'immigration lawyer',
  'bankruptcy attorney',
  'estate planning attorney',
  'real estate lawyer',
  'employment lawyer',
  'business attorney',
  'tax attorney',
];

const TOP_MARKETS = [
  'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
  'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
  'Dallas, TX', 'Miami, FL', 'Atlanta, GA', 'Boston, MA',
  'Seattle, WA', 'Denver, CO', 'Nashville, TN', 'Charlotte, NC',
  'San Francisco, CA', 'Austin, TX', 'Portland, OR', 'Las Vegas, NV',
];

const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 2;

const logFile = path.join(__dirname, '..', 'output', 'batch-practice-areas-log.json');

let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function jobKey(niche, city) {
  return `${niche}__${city}`.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
}

function runJob(niche, city) {
  return new Promise((resolve) => {
    const key = jobKey(niche, city);
    const cmd = `node scripts/industry-scrape.js --niche "${niche}" --location "${city}" --test`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 20 * 60 * 1000,
      maxBuffer: 10 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[key] = { status: 'failed', niche, city, error: errMsg, elapsed, timestamp: new Date().toISOString() };
        saveProgress();
        resolve({ niche, city, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leads = parseInt((output.match(/Total leads:\s+(\d+)/) || [])[1] || '0');
      const emails = parseInt((output.match(/With email:\s+(\d+)/) || [])[1] || '0');
      const phones = parseInt((output.match(/With phone:\s+(\d+)/) || [])[1] || '0');
      const persons = parseInt((output.match(/With person name:\s+(\d+)/) || [])[1] || '0');
      const dms = parseInt((output.match(/Decision makers:\s+(\d+)/) || [])[1] || '0');

      progress[key] = { status: 'done', niche, city, leads, emails, phones, persons, decisionMakers: dms, elapsed, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ niche, city, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, completed = 0, failed = 0;

  // Build job queue: every practice area × every market
  const allJobs = [];
  for (const niche of PRACTICE_AREAS) {
    for (const city of TOP_MARKETS) {
      allJobs.push({ niche, city });
    }
  }

  const queue = [];
  for (const job of allJobs) {
    const key = jobKey(job.niche, job.city);
    if (resumeMode && progress[key] && progress[key].status === 'done') {
      totalLeads += progress[key].leads || 0;
      totalEmails += progress[key].emails || 0;
      completed++;
      continue;
    }
    queue.push(job);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  PRACTICE AREA SCRAPE — ${PRACTICE_AREAS.length} niches × ${TOP_MARKETS.length} cities = ${allJobs.length} jobs`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    console.log(`  Starting: ${batch.map(j => `${j.niche} @ ${j.city}`).join(' | ')}`);

    const results = await Promise.all(batch.map(j => runJob(j.niche, j.city)));

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const pct = r.leads > 0 ? Math.round(r.emails * 100 / r.leads) : 0;
        console.log(`  ✓ ${r.niche} @ ${r.city}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.dms} DMs | ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`  ✗ ${r.niche} @ ${r.city}: FAILED — ${r.error}`);
      }
    }
    queueIdx += CONCURRENCY;

    const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
    const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
    const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
    const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 300;
    const etaMin = Math.round(batchesLeft * avgBatchTime / 60);
    console.log(`  ── ${completed}/${allJobs.length} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed | ETA: ~${etaMin}min ──`);
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  PRACTICE AREA COMPLETE: ${completed}/${allJobs.length} | ${totalLeads} leads | ${totalEmails} emails | ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
