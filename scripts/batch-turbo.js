#!/usr/bin/env node
/**
 * TURBO BATCH: Maximum leads per city by running multiple search terms.
 *
 * Instead of just "law firms", runs 6 practice-area queries per city:
 *   - law firms, personal injury lawyer, family law attorney,
 *   - criminal defense lawyer, immigration lawyer, bankruptcy attorney
 *
 * Each query finds different firms → 3-5x the leads per city.
 * Dedup across queries by domain/phone.
 *
 * Usage:
 *   node scripts/batch-turbo.js --concurrency 2
 *   node scripts/batch-turbo.js --concurrency 2 --resume
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// Search terms that find DIFFERENT law firms
const US_NICHES = [
  'law firms',
  'personal injury lawyer',
  'family law attorney',
  'criminal defense lawyer',
  'immigration lawyer',
  'estate planning attorney',
];

const UK_NICHES = [
  'solicitors',
  'personal injury solicitor',
  'family solicitor',
  'criminal solicitor',
  'immigration solicitor',
  'commercial solicitor',
];

// Cities — focus on top 50 US + key international
const US_CITIES = [
  'New York, NY','Los Angeles, CA','Chicago, IL','Houston, TX','Phoenix, AZ',
  'Philadelphia, PA','San Antonio, TX','San Diego, CA','Dallas, TX','San Jose, CA',
  'Austin, TX','Jacksonville, FL','Fort Worth, TX','Columbus, OH','Charlotte, NC',
  'Indianapolis, IN','San Francisco, CA','Seattle, WA','Denver, CO','Nashville, TN',
  'Oklahoma City, OK','El Paso, TX','Washington, DC','Boston, MA','Las Vegas, NV',
  'Portland, OR','Memphis, TN','Louisville, KY','Baltimore, MD','Milwaukee, WI',
  'Albuquerque, NM','Tucson, AZ','Sacramento, CA','Atlanta, GA','Kansas City, MO',
  'Raleigh, NC','Miami, FL','Minneapolis, MN','Tampa, FL','New Orleans, LA',
  'Cleveland, OH','Pittsburgh, PA','Cincinnati, OH','Orlando, FL','Salt Lake City, UT',
  'Richmond, VA','Birmingham, AL','Rochester, NY','Grand Rapids, MI','Knoxville, TN',
];

const UK_CITIES = [
  'London, UK','Manchester, UK','Birmingham, UK','Leeds, UK','Glasgow, UK',
  'Edinburgh, UK','Bristol, UK','Liverpool, UK','Cardiff, UK','Nottingham, UK',
];

const CA_CITIES = [
  'Toronto, ON, Canada','Vancouver, BC, Canada','Calgary, AB, Canada',
  'Montreal, QC, Canada','Ottawa, ON, Canada','Edmonton, AB, Canada',
  'Winnipeg, MB, Canada','Halifax, NS, Canada',
];

const AU_CITIES = [
  'Sydney, NSW, Australia','Melbourne, VIC, Australia','Brisbane, QLD, Australia',
  'Perth, WA, Australia','Adelaide, SA, Australia',
];

const IE_CITIES = [
  'Dublin, Ireland','Cork, Ireland','Galway, Ireland','Limerick, Ireland',
];

// Build all jobs: each city × each niche
const ALL_JOBS = [];
for (const city of US_CITIES) {
  for (const niche of US_NICHES) {
    ALL_JOBS.push({ city, niche });
  }
}
for (const city of UK_CITIES) {
  for (const niche of UK_NICHES) {
    ALL_JOBS.push({ city, niche });
  }
}
for (const city of CA_CITIES) {
  for (const niche of US_NICHES) {
    ALL_JOBS.push({ city, niche });
  }
}
for (const city of AU_CITIES) {
  for (const niche of US_NICHES) {
    ALL_JOBS.push({ city, niche });
  }
}
for (const city of IE_CITIES) {
  for (const niche of UK_NICHES) {
    ALL_JOBS.push({ city, niche });
  }
}

// CLI args
const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 2;

const logFile = path.join(__dirname, '..', 'output', 'batch-turbo-log.json');

let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function jobKey(city, niche) {
  return (city + '__' + niche).toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
}

function runJob(job) {
  return new Promise((resolve) => {
    const key = jobKey(job.city, job.niche);
    // Test mode (4 grid cells) + fast HTTP email scrape + skip CC/WHOIS
    const cmd = `node scripts/industry-scrape.js --niche "${job.niche}" --location "${job.city}" --test --fast --skip-cc --skip-whois`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 10 * 60 * 1000,
      maxBuffer: 10 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[key] = { status: 'failed', city: job.city, niche: job.niche, error: errMsg, elapsed, timestamp: new Date().toISOString() };
        saveProgress();
        resolve({ city: job.city, niche: job.niche, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leads = parseInt((output.match(/Total leads:\s+(\d+)/) || [])[1] || '0');
      const emails = parseInt((output.match(/With email:\s+(\d+)/) || [])[1] || '0');
      const dms = parseInt((output.match(/Decision makers:\s+(\d+)/) || [])[1] || '0');

      progress[key] = { status: 'done', city: job.city, niche: job.niche, leads, emails, dms, elapsed, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ city: job.city, niche: job.niche, status: 'done', leads, emails, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, totalDMs = 0, completed = 0, failed = 0;

  const queue = [];
  for (const job of ALL_JOBS) {
    const key = jobKey(job.city, job.niche);
    if (resumeMode && progress[key] && progress[key].status === 'done') {
      totalLeads += progress[key].leads || 0;
      totalEmails += progress[key].emails || 0;
      totalDMs += progress[key].dms || 0;
      completed++;
      continue;
    }
    queue.push(job);
  }

  const totalCities = new Set(ALL_JOBS.map(j => j.city)).size;
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  TURBO MODE — ${totalCities} cities × ${US_NICHES.length} niches = ${ALL_JOBS.length} jobs`);
  console.log(`  US: ${US_CITIES.length} | UK: ${UK_CITIES.length} | CA: ${CA_CITIES.length} | AU: ${AU_CITIES.length} | IE: ${IE_CITIES.length}`);
  console.log(`  Niches: ${US_NICHES.join(', ')}`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    const batchLabel = batch.map(j => `${j.city.split(',')[0]}/${j.niche.split(' ')[0]}`).join(' | ');
    console.log(`  [${completed+1}] ${batchLabel}`);

    const results = await Promise.all(batch.map(j => runJob(j)));

    for (const r of results) {
      const shortNiche = r.niche.split(' ').slice(0, 2).join(' ');
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        totalDMs += r.dms;
        const emailPct = r.leads > 0 ? Math.round(r.emails / r.leads * 100) : 0;
        console.log(`    ✓ ${r.city.split(',')[0]}/${shortNiche}: ${r.leads}L ${r.emails}E(${emailPct}%) ${r.dms}DM ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`    ✗ ${r.city.split(',')[0]}/${shortNiche}: ${(r.error || '').slice(0,50)}`);
      }
    }
    queueIdx += CONCURRENCY;

    if (queueIdx % (CONCURRENCY * 3) === 0 || queueIdx >= queue.length) {
      const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
      const emailRate = totalLeads > 0 ? Math.round(totalEmails / totalLeads * 100) : 0;
      const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
      const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
      const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 300;
      const etaMin = Math.round(batchesLeft * avgBatchTime / 60);
      console.log(`  ── ${completed}/${ALL_JOBS.length} | ${totalLeads}L ${totalEmails}E(${emailRate}%) ${totalDMs}DM | ${failed}fail | ETA ~${etaMin}m ──`);
    }
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  const emailRate = totalLeads > 0 ? Math.round(totalEmails / totalLeads * 100) : 0;
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  TURBO COMPLETE: ${totalLeads} leads | ${totalEmails} emails (${emailRate}%) | ${totalDMs} DMs`);
  console.log(`  ${completed} done / ${failed} failed / ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
