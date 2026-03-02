#!/usr/bin/env node
/**
 * Batch scrape law firms across all 50 US states.
 * Runs N states in parallel (default 3) with progress tracking.
 *
 * Usage:
 *   node scripts/batch-states.js                    # 3 concurrent, test mode
 *   node scripts/batch-states.js --concurrency 5    # 5 concurrent
 *   node scripts/batch-states.js --full              # Full grid (slow, deep)
 *   node scripts/batch-states.js --resume            # Skip already-completed states
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const STATES = [
  { code: 'AL', city: 'Birmingham, AL' },
  { code: 'AK', city: 'Anchorage, AK' },
  { code: 'AZ', city: 'Phoenix, AZ' },
  { code: 'AR', city: 'Little Rock, AR' },
  { code: 'CA', city: 'Los Angeles, CA' },
  { code: 'CO', city: 'Denver, CO' },
  { code: 'CT', city: 'Hartford, CT' },
  { code: 'DE', city: 'Wilmington, DE' },
  { code: 'FL', city: 'Miami, FL' },
  { code: 'GA', city: 'Atlanta, GA' },
  { code: 'HI', city: 'Honolulu, HI' },
  { code: 'ID', city: 'Boise, ID' },
  { code: 'IL', city: 'Chicago, IL' },
  { code: 'IN', city: 'Indianapolis, IN' },
  { code: 'IA', city: 'Des Moines, IA' },
  { code: 'KS', city: 'Wichita, KS' },
  { code: 'KY', city: 'Louisville, KY' },
  { code: 'LA', city: 'New Orleans, LA' },
  { code: 'ME', city: 'Portland, ME' },
  { code: 'MD', city: 'Baltimore, MD' },
  { code: 'MA', city: 'Boston, MA' },
  { code: 'MI', city: 'Detroit, MI' },
  { code: 'MN', city: 'Minneapolis, MN' },
  { code: 'MS', city: 'Jackson, MS' },
  { code: 'MO', city: 'St. Louis, MO' },
  { code: 'MT', city: 'Billings, MT' },
  { code: 'NE', city: 'Omaha, NE' },
  { code: 'NV', city: 'Las Vegas, NV' },
  { code: 'NH', city: 'Manchester, NH' },
  { code: 'NJ', city: 'Newark, NJ' },
  { code: 'NM', city: 'Albuquerque, NM' },
  { code: 'NY', city: 'New York, NY' },
  { code: 'NC', city: 'Charlotte, NC' },
  { code: 'ND', city: 'Fargo, ND' },
  { code: 'OH', city: 'Columbus, OH' },
  { code: 'OK', city: 'Oklahoma City, OK' },
  { code: 'OR', city: 'Portland, OR' },
  { code: 'PA', city: 'Philadelphia, PA' },
  { code: 'RI', city: 'Providence, RI' },
  { code: 'SC', city: 'Charleston, SC' },
  { code: 'SD', city: 'Sioux Falls, SD' },
  { code: 'TN', city: 'Nashville, TN' },
  { code: 'TX', city: 'Houston, TX' },
  { code: 'UT', city: 'Salt Lake City, UT' },
  { code: 'VT', city: 'Burlington, VT' },
  { code: 'VA', city: 'Richmond, VA' },
  { code: 'WA', city: 'Seattle, WA' },
  { code: 'WV', city: 'Charleston, WV' },
  { code: 'WI', city: 'Milwaukee, WI' },
  { code: 'WY', city: 'Cheyenne, WY' },
];

const args = process.argv.slice(2);
const fullMode = args.includes('--full');
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 3;

const NICHE = 'law firms';
const logFile = path.join(__dirname, '..', 'output', 'batch-lawfirms-log.json');

// Load existing progress
let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function runState(state, idx) {
  return new Promise((resolve) => {
    const testFlag = fullMode ? '' : ' --test';
    const cmd = `node scripts/industry-scrape.js --niche "${NICHE}" --location "${state.city}"${testFlag}`;

    const startTime = Date.now();
    const child = exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 30 * 60 * 1000,
      maxBuffer: 10 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[state.code] = {
          status: 'failed',
          error: errMsg,
          elapsed,
          timestamp: new Date().toISOString(),
        };
        saveProgress();
        resolve({ code: state.code, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leadsMatch = output.match(/Total leads:\s+(\d+)/);
      const emailMatch = output.match(/With email:\s+(\d+)/);
      const phoneMatch = output.match(/With phone:\s+(\d+)/);
      const personMatch = output.match(/With person name:\s+(\d+)/);
      const dmMatch = output.match(/Decision makers:\s+(\d+)/);
      const csvMatch = output.match(/Output:\s+(.+\.csv)/);

      const leads = leadsMatch ? parseInt(leadsMatch[1]) : 0;
      const emails = emailMatch ? parseInt(emailMatch[1]) : 0;
      const phones = phoneMatch ? parseInt(phoneMatch[1]) : 0;
      const persons = personMatch ? parseInt(personMatch[1]) : 0;
      const dms = dmMatch ? parseInt(dmMatch[1]) : 0;
      const csvPath = csvMatch ? csvMatch[1].trim() : '';

      progress[state.code] = {
        status: 'done',
        leads, emails, phones, persons,
        decisionMakers: dms,
        elapsed,
        csv: csvPath,
        timestamp: new Date().toISOString(),
      };
      saveProgress();

      resolve({ code: state.code, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0;
  let totalEmails = 0;
  let completed = 0;
  let failed = 0;
  let skipped = 0;

  // Build work queue (skip already-done states if --resume)
  const queue = [];
  for (const state of STATES) {
    if (resumeMode && progress[state.code] && progress[state.code].status === 'done') {
      totalLeads += progress[state.code].leads || 0;
      totalEmails += progress[state.code].emails || 0;
      completed++;
      skipped++;
      continue;
    }
    queue.push(state);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  LAW FIRMS — ALL 50 STATES ${fullMode ? '(FULL MODE)' : '(TEST MODE)'}`);
  console.log(`  Concurrency: ${CONCURRENCY} states at a time`);
  if (skipped > 0) console.log(`  Resuming: ${skipped} already done, ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  // Process in batches of CONCURRENCY
  let queueIdx = 0;
  const total = STATES.length;

  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    const batchCodes = batch.map(s => s.code).join(', ');
    console.log(`\n  Starting batch: ${batchCodes}`);

    const results = await Promise.all(
      batch.map((state, i) => runState(state, queueIdx + i))
    );

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const pct = r.leads > 0 ? Math.round(r.emails * 100 / r.leads) : 0;
        console.log(`  ✓ ${r.code}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.phones} phone | ${r.persons} people | ${r.dms} DMs | ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`  ✗ ${r.code}: FAILED — ${r.error}`);
      }
    }

    queueIdx += CONCURRENCY;

    const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
    const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
    const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
    const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 300;
    const etaMin = Math.round(batchesLeft * avgBatchTime / 60);

    console.log(`  ── ${completed}/${total} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed | ETA: ~${etaMin}min ──`);
  }

  // Final summary
  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  BATCH COMPLETE`);
  console.log(`${'═'.repeat(70)}`);
  console.log(`  States completed: ${completed}/50`);
  console.log(`  States failed:    ${failed}`);
  console.log(`  Total leads:      ${totalLeads}`);
  console.log(`  Total emails:     ${totalEmails} (${totalLeads > 0 ? Math.round(totalEmails * 100 / totalLeads) : 0}%)`);
  console.log(`  Total time:       ${Math.round(totalElapsed / 60)}min`);
  console.log(`  Progress log:     ${logFile}`);

  // Per-state breakdown
  console.log(`\n  Per-state results:`);
  for (const state of STATES) {
    const p = progress[state.code];
    if (p && p.status === 'done') {
      const pct = p.leads > 0 ? Math.round(p.emails * 100 / p.leads) : 0;
      console.log(`    ${state.code}: ${p.leads} leads | ${p.emails} email (${pct}%) | ${p.elapsed}s`);
    } else if (p && p.status === 'failed') {
      console.log(`    ${state.code}: FAILED`);
    }
  }
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
