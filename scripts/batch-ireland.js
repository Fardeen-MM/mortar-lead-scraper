#!/usr/bin/env node
/**
 * Batch scrape solicitors across Irish cities.
 * Uses "solicitors" niche (same as UK — Ireland uses solicitor/barrister system).
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const CITIES = [
  { code: 'DUBLIN', city: 'Dublin, Ireland' },
  { code: 'CORK', city: 'Cork, Ireland' },
  { code: 'GALWAY', city: 'Galway, Ireland' },
  { code: 'LIMERICK', city: 'Limerick, Ireland' },
  { code: 'WATERFORD', city: 'Waterford, Ireland' },
  { code: 'KILKENNY', city: 'Kilkenny, Ireland' },
  { code: 'DROGHEDA', city: 'Drogheda, Ireland' },
  { code: 'DUNDALK', city: 'Dundalk, Ireland' },
  { code: 'SLIGO', city: 'Sligo, Ireland' },
  { code: 'ATHLONE', city: 'Athlone, Ireland' },
  { code: 'WEXFORD', city: 'Wexford, Ireland' },
  { code: 'TRALEE', city: 'Tralee, Ireland' },
  // Northern Ireland (separate jurisdiction but same market)
  { code: 'BELFAST-NI', city: 'Belfast, Northern Ireland' },
  { code: 'DERRY', city: 'Derry, Northern Ireland' },
];

const args = process.argv.slice(2);
const fullMode = args.includes('--full');
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 3;

const NICHE = 'solicitors';
const logFile = path.join(__dirname, '..', 'output', 'batch-lawfirms-ireland-log.json');

let progress = {};
if (fs.existsSync(logFile)) {
  try { progress = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch {}
}

function saveProgress() {
  fs.writeFileSync(logFile, JSON.stringify(progress, null, 2));
}

function runLocation(loc) {
  return new Promise((resolve) => {
    const testFlag = fullMode ? '' : ' --test';
    const cmd = `node scripts/industry-scrape.js --niche "${NICHE}" --location "${loc.city}"${testFlag}`;

    const startTime = Date.now();
    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 30 * 60 * 1000,
      maxBuffer: 10 * 1024 * 1024,
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
      const csvPath = ((output.match(/Output:\s+(.+\.csv)/) || [])[1] || '').trim();

      progress[loc.code] = { status: 'done', city: loc.city, leads, emails, phones, persons, decisionMakers: dms, elapsed, csv: csvPath, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ code: loc.code, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, completed = 0, failed = 0;

  const queue = [];
  for (const loc of CITIES) {
    if (resumeMode && progress[loc.code] && progress[loc.code].status === 'done') {
      totalLeads += progress[loc.code].leads || 0;
      totalEmails += progress[loc.code].emails || 0;
      completed++;
      continue;
    }
    queue.push(loc);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  SOLICITORS — IRELAND (${CITIES.length} cities) ${fullMode ? '(FULL)' : '(TEST)'}`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    console.log(`\n  Starting: ${batch.map(s => s.code).join(', ')}`);

    const results = await Promise.all(batch.map(loc => runLocation(loc)));

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const pct = r.leads > 0 ? Math.round(r.emails * 100 / r.leads) : 0;
        console.log(`  ✓ ${r.code}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.persons} people | ${r.dms} DMs | ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`  ✗ ${r.code}: FAILED — ${r.error}`);
      }
    }
    queueIdx += CONCURRENCY;
    console.log(`  ── ${completed}/${CITIES.length} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed ──`);
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  IRELAND COMPLETE: ${completed}/${CITIES.length} | ${totalLeads} leads | ${totalEmails} emails | ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
