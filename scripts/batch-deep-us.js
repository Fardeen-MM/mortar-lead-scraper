#!/usr/bin/env node
/**
 * Deep scrape: Multiple cities per state for top US markets.
 * Uses test mode but hits many more cities for broader coverage.
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const LOCATIONS = [
  // California - 8 cities
  'Los Angeles, CA', 'San Francisco, CA', 'San Diego, CA', 'Sacramento, CA',
  'San Jose, CA', 'Fresno, CA', 'Oakland, CA', 'Irvine, CA',
  // Texas - 6 cities
  'Houston, TX', 'Dallas, TX', 'Austin, TX', 'San Antonio, TX',
  'Fort Worth, TX', 'El Paso, TX',
  // Florida - 6 cities
  'Miami, FL', 'Tampa, FL', 'Orlando, FL', 'Jacksonville, FL',
  'Fort Lauderdale, FL', 'West Palm Beach, FL',
  // New York - 5 cities
  'New York, NY', 'Buffalo, NY', 'Albany, NY', 'Rochester, NY', 'Syracuse, NY',
  // Illinois - 3 cities
  'Chicago, IL', 'Springfield, IL', 'Rockford, IL',
  // Pennsylvania - 4 cities
  'Philadelphia, PA', 'Pittsburgh, PA', 'Harrisburg, PA', 'Allentown, PA',
  // Ohio - 4 cities
  'Columbus, OH', 'Cleveland, OH', 'Cincinnati, OH', 'Toledo, OH',
  // Georgia - 3 cities
  'Atlanta, GA', 'Savannah, GA', 'Augusta, GA',
  // North Carolina - 4 cities
  'Charlotte, NC', 'Raleigh, NC', 'Greensboro, NC', 'Durham, NC',
  // Michigan - 3 cities
  'Detroit, MI', 'Grand Rapids, MI', 'Ann Arbor, MI',
  // New Jersey - 3 cities
  'Newark, NJ', 'Jersey City, NJ', 'Trenton, NJ',
  // Virginia - 3 cities
  'Richmond, VA', 'Virginia Beach, VA', 'Arlington, VA',
  // Washington - 3 cities
  'Seattle, WA', 'Spokane, WA', 'Tacoma, WA',
  // Massachusetts - 3 cities
  'Boston, MA', 'Worcester, MA', 'Springfield, MA',
  // Arizona - 3 cities
  'Phoenix, AZ', 'Tucson, AZ', 'Scottsdale, AZ',
  // Tennessee - 3 cities
  'Nashville, TN', 'Memphis, TN', 'Knoxville, TN',
  // Colorado - 3 cities
  'Denver, CO', 'Colorado Springs, CO', 'Boulder, CO',
  // Maryland - 2 cities
  'Baltimore, MD', 'Bethesda, MD',
  // Missouri - 3 cities
  'St. Louis, MO', 'Kansas City, MO', 'Springfield, MO',
  // Louisiana - 2 cities
  'New Orleans, LA', 'Baton Rouge, LA',
  // Minnesota - 2 cities
  'Minneapolis, MN', 'St. Paul, MN',
  // Wisconsin - 2 cities
  'Milwaukee, WI', 'Madison, WI',
  // Indiana - 2 cities
  'Indianapolis, IN', 'Fort Wayne, IN',
  // Connecticut - 2 cities
  'Hartford, CT', 'New Haven, CT',
  // Oregon - 2 cities
  'Portland, OR', 'Eugene, OR',
  // Nevada - 2 cities
  'Las Vegas, NV', 'Reno, NV',
  // South Carolina - 2 cities
  'Charleston, SC', 'Columbia, SC',
  // Oklahoma - 2 cities
  'Oklahoma City, OK', 'Tulsa, OK',
  // Kentucky - 2 cities
  'Louisville, KY', 'Lexington, KY',
  // Alabama - 2 cities
  'Birmingham, AL', 'Huntsville, AL',
  // Utah - 2 cities
  'Salt Lake City, UT', 'Provo, UT',
  // Iowa - 2 cities
  'Des Moines, IA', 'Cedar Rapids, IA',
  // Mississippi - 2 cities
  'Jackson, MS', 'Gulfport, MS',
  // Kansas - 2 cities
  'Wichita, KS', 'Overland Park, KS',
  // Arkansas - 2 cities
  'Little Rock, AR', 'Fayetteville, AR',
  // Nebraska - 2 cities
  'Omaha, NE', 'Lincoln, NE',
  // New Mexico - 2 cities
  'Albuquerque, NM', 'Santa Fe, NM',
  // Hawaii
  'Honolulu, HI',
  // DC
  'Washington, DC',
];

const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 3;

const NICHE = 'law firms';
const logFile = path.join(__dirname, '..', 'output', 'batch-deep-us-log.json');

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
    const cmd = `node scripts/industry-scrape.js --niche "${NICHE}" --location "${city}" --test`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 20 * 60 * 1000,
      maxBuffer: 10 * 1024 * 1024,
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
  console.log(`  DEEP US SCRAPE — ${LOCATIONS.length} cities (test mode)`);
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
        console.log(`  ✓ ${r.city}: ${r.leads} leads | ${r.emails} email (${pct}%) | ${r.persons} ppl | ${r.dms} DMs | ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`  ✗ ${r.city}: FAILED — ${r.error}`);
      }
    }
    queueIdx += CONCURRENCY;

    const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
    const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
    const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
    const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 300;
    const etaMin = Math.round(batchesLeft * avgBatchTime / 60);
    console.log(`  ── ${completed}/${LOCATIONS.length} done | ${totalLeads} leads | ${totalEmails} emails | ${failed} failed | ETA: ~${etaMin}min ──`);
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  DEEP US COMPLETE: ${completed}/${LOCATIONS.length} | ${totalLeads} leads | ${totalEmails} emails | ${Math.round(totalElapsed / 60)}min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
