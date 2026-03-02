#!/usr/bin/env node
/**
 * OPTIMIZED BATCH SCRAPE: Balanced speed + quality.
 *
 * Strategy — "Medium mode":
 *   1. Google Maps geo-grid (10 cells — 2.5x test, 40% of full)
 *   2. DuckDuckGo web search (100 results — full)
 *   3. Website crawl (30 sites — enough for ~50% email rate)
 *   4. Email pattern generation (for the rest)
 *   5. Skip CommonCrawl + WHOIS (slow, low-value)
 *
 * Expected: ~40-60 leads/city, ~50% email, ~5-8 min/city
 *
 * Usage:
 *   node scripts/batch-blitz.js --concurrency 2
 *   node scripts/batch-blitz.js --concurrency 2 --resume
 *   node scripts/batch-blitz.js --concurrency 2 --resume --skip-existing
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// ─── City Lists ───────────────────────────────────────────────────────

// 170 US cities
const US_CITIES = [
  'New York, NY','Los Angeles, CA','Chicago, IL','Houston, TX','Phoenix, AZ',
  'Philadelphia, PA','San Antonio, TX','San Diego, CA','Dallas, TX','San Jose, CA',
  'Austin, TX','Jacksonville, FL','Fort Worth, TX','Columbus, OH','Charlotte, NC',
  'Indianapolis, IN','San Francisco, CA','Seattle, WA','Denver, CO','Nashville, TN',
  'Oklahoma City, OK','El Paso, TX','Washington, DC','Boston, MA','Las Vegas, NV',
  'Portland, OR','Memphis, TN','Louisville, KY','Baltimore, MD','Milwaukee, WI',
  'Albuquerque, NM','Tucson, AZ','Fresno, CA','Mesa, AZ','Sacramento, CA',
  'Atlanta, GA','Kansas City, MO','Omaha, NE','Colorado Springs, CO','Raleigh, NC',
  'Long Beach, CA','Virginia Beach, VA','Miami, FL','Oakland, CA','Minneapolis, MN',
  'Tulsa, OK','Tampa, FL','Arlington, TX','New Orleans, LA','Wichita, KS',
  'Cleveland, OH','Bakersfield, CA','Aurora, CO','Anaheim, CA','Honolulu, HI',
  'Santa Ana, CA','Riverside, CA','Corpus Christi, TX','Lexington, KY','Stockton, CA',
  'Pittsburgh, PA','St. Paul, MN','Cincinnati, OH','Anchorage, AK','Henderson, NV',
  'Greensboro, NC','Plano, TX','Newark, NJ','Lincoln, NE','Orlando, FL',
  'Irvine, CA','Toledo, OH','Jersey City, NJ','Chula Vista, CA','Durham, NC',
  'Fort Wayne, IN','St. Petersburg, FL','Laredo, TX','Norfolk, VA','Madison, WI',
  'Chandler, AZ','Lubbock, TX','Scottsdale, AZ','Reno, NV','Glendale, AZ',
  'Buffalo, NY','Gilbert, AZ','Winston-Salem, NC','North Las Vegas, NV','Chesapeake, VA',
  'Fremont, CA','Irving, TX','Richmond, VA','Boise, ID','San Bernardino, CA',
  'Spokane, WA','Des Moines, IA','Birmingham, AL','Modesto, CA','Rochester, NY',
  'Tacoma, WA','Fontana, CA','Oxnard, CA','Moreno Valley, CA','Fayetteville, NC',
  'Glendale, CA','Yonkers, NY','Huntington Beach, CA','Salt Lake City, UT','Grand Rapids, MI',
  'Amarillo, TX','Little Rock, AR','Tallahassee, FL','Huntsville, AL','Augusta, GA',
  'Montgomery, AL','Akron, OH','Knoxville, TN','Mobile, AL','Shreveport, LA',
  'Grand Prairie, TX','Overland Park, KS','Chattanooga, TN','Providence, RI','Brownsville, TX',
  'Tempe, AZ','Fort Lauderdale, FL','Newport News, VA','Savannah, GA','West Palm Beach, FL',
  'Dayton, OH','Rockford, IL','Columbia, SC','Bridgeport, CT','Naperville, IL',
  'Hartford, CT','New Haven, CT','Baton Rouge, LA','Cedar Rapids, IA','Jackson, MS',
  'Provo, UT','Springfield, MO','Ann Arbor, MI','Sioux Falls, SD','Fargo, ND',
  'Charleston, SC','Wilmington, DE','Eugene, OR','Stamford, CT','Lakewood, CO',
  'Trenton, NJ','Albany, NY','Syracuse, NY','Bethesda, MD','Boulder, CO',
  'Santa Fe, NM','Worcester, MA','Springfield, MA','Duluth, MN','Billings, MT',
  'Cheyenne, WY','Burlington, VT','Concord, NH','Portland, ME','Bangor, ME',
  'Manchester, NH','Missoula, MT','Rapid City, SD','Bismarck, ND','Casper, WY',
];

// 30 Canadian cities
const CA_CITIES = [
  'Toronto, ON, Canada','Vancouver, BC, Canada','Calgary, AB, Canada',
  'Montreal, QC, Canada','Ottawa, ON, Canada','Edmonton, AB, Canada',
  'Winnipeg, MB, Canada','Hamilton, ON, Canada','Kitchener, ON, Canada',
  'London, ON, Canada','Victoria, BC, Canada','Halifax, NS, Canada',
  'Saskatoon, SK, Canada','Regina, SK, Canada','St. John\'s, NL, Canada',
  'Kelowna, BC, Canada','Barrie, ON, Canada','Abbotsford, BC, Canada',
  'Kingston, ON, Canada','Thunder Bay, ON, Canada','Fredericton, NB, Canada',
  'Moncton, NB, Canada','Charlottetown, PE, Canada','Yellowknife, NT, Canada',
  'Whitehorse, YT, Canada','Sudbury, ON, Canada','Guelph, ON, Canada',
  'Sherbrooke, QC, Canada','Lethbridge, AB, Canada','Red Deer, AB, Canada',
];

// 30 UK cities
const UK_CITIES = [
  'London, UK','Manchester, UK','Birmingham, UK','Leeds, UK','Glasgow, UK',
  'Edinburgh, UK','Bristol, UK','Liverpool, UK','Cardiff, UK','Nottingham, UK',
  'Sheffield, UK','Newcastle upon Tyne, UK','Belfast, UK','Southampton, UK',
  'Leicester, UK','Brighton, UK','Coventry, UK','Plymouth, UK','Reading, UK',
  'Derby, UK','Stoke, UK','Wolverhampton, UK','Swindon, UK','Swansea, UK',
  'Norwich, UK','Exeter, UK','Aberdeen, UK','Dundee, UK','Bath, UK','York, UK',
];

// 20 Australian cities
const AU_CITIES = [
  'Sydney, NSW, Australia','Melbourne, VIC, Australia','Brisbane, QLD, Australia',
  'Perth, WA, Australia','Adelaide, SA, Australia','Canberra, ACT, Australia',
  'Hobart, TAS, Australia','Darwin, NT, Australia','Gold Coast, QLD, Australia',
  'Newcastle, NSW, Australia','Wollongong, NSW, Australia','Geelong, VIC, Australia',
  'Townsville, QLD, Australia','Cairns, QLD, Australia','Toowoomba, QLD, Australia',
  'Ballarat, VIC, Australia','Bendigo, VIC, Australia','Launceston, TAS, Australia',
  'Mackay, QLD, Australia','Rockhampton, QLD, Australia',
];

// 15 Ireland cities
const IE_CITIES = [
  'Dublin, Ireland','Cork, Ireland','Galway, Ireland','Limerick, Ireland',
  'Waterford, Ireland','Kilkenny, Ireland','Drogheda, Ireland','Dundalk, Ireland',
  'Sligo, Ireland','Athlone, Ireland','Wexford, Ireland','Tralee, Ireland',
  'Ennis, Ireland','Letterkenny, Ireland','Carlow, Ireland',
];

const ALL_LOCATIONS = [
  ...US_CITIES.map(c => ({ city: c, niche: 'law firms' })),
  ...CA_CITIES.map(c => ({ city: c, niche: 'law firms' })),
  ...UK_CITIES.map(c => ({ city: c, niche: 'solicitors' })),
  ...AU_CITIES.map(c => ({ city: c, niche: 'law firms' })),
  ...IE_CITIES.map(c => ({ city: c, niche: 'solicitors' })),
];

// ─── CLI Args ─────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const resumeMode = args.includes('--resume');
const skipExisting = args.includes('--skip-existing');
const concurrencyIdx = args.indexOf('--concurrency');
const CONCURRENCY = concurrencyIdx >= 0 ? parseInt(args[concurrencyIdx + 1]) : 2;

// Tuning params — balanced for speed + quality
const GRID_CELLS = 6;      // 50% more than test (4), fast
const MAX_CRAWL = 20;      // Crawl 20 websites — good email rate without hogging resources
const MAX_MAPS = 40;       // Cap Maps results at 40 per city

const logFile = path.join(__dirname, '..', 'output', 'batch-blitz-log.json');

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

// Check which cities already have CSV output from previous runs
function getExistingCities() {
  const outputDir = path.join(__dirname, '..', 'output');
  const existing = new Set();
  try {
    const files = fs.readdirSync(outputDir);
    for (const f of files) {
      if (!f.endsWith('.csv')) continue;
      // Extract city key from filename: law-firms_new-york-ny_2026-...csv
      const match = f.match(/^(?:law-firms|solicitors)_(.+?)_\d{4}/);
      if (match) existing.add(match[1]);
    }
  } catch {}
  return existing;
}

function runLocation(loc) {
  return new Promise((resolve) => {
    const key = locationKey(loc.city);
    // Balanced mode: 6 grid cells, 20 website crawls, skip CC/WHOIS
    const cmd = `node scripts/industry-scrape.js --niche "${loc.niche}" --location "${loc.city}" --grid ${GRID_CELLS} --max-crawl ${MAX_CRAWL} --max-maps ${MAX_MAPS} --skip-cc --skip-whois`;
    const startTime = Date.now();

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 15 * 60 * 1000,  // 15 min timeout
      maxBuffer: 10 * 1024 * 1024,
    }, (err, stdout, stderr) => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      if (err) {
        const errMsg = stderr ? stderr.slice(-200).trim().split('\n').pop() : err.message;
        progress[key] = { status: 'failed', city: loc.city, error: errMsg, elapsed, timestamp: new Date().toISOString() };
        saveProgress();
        resolve({ city: loc.city, status: 'failed', error: errMsg, leads: 0, emails: 0, elapsed });
        return;
      }

      const output = stdout || '';
      const leads = parseInt((output.match(/Total leads:\s+(\d+)/) || [])[1] || '0');
      const emails = parseInt((output.match(/With email:\s+(\d+)/) || [])[1] || '0');
      const phones = parseInt((output.match(/With phone:\s+(\d+)/) || [])[1] || '0');
      const persons = parseInt((output.match(/With person name:\s+(\d+)/) || [])[1] || '0');
      const dms = parseInt((output.match(/Decision makers:\s+(\d+)/) || [])[1] || '0');

      progress[key] = { status: 'done', city: loc.city, leads, emails, phones, persons, decisionMakers: dms, elapsed, timestamp: new Date().toISOString() };
      saveProgress();
      resolve({ city: loc.city, status: 'done', leads, emails, phones, persons, dms, elapsed });
    });
  });
}

async function run() {
  const startTime = Date.now();
  let totalLeads = 0, totalEmails = 0, completed = 0, failed = 0;

  // Check existing CSV files if --skip-existing
  const existingCities = skipExisting ? getExistingCities() : new Set();

  const queue = [];
  let skipped = 0;
  for (const loc of ALL_LOCATIONS) {
    const key = locationKey(loc.city);

    // Skip if already done in this batch run
    if (resumeMode && progress[key] && progress[key].status === 'done') {
      totalLeads += progress[key].leads || 0;
      totalEmails += progress[key].emails || 0;
      completed++;
      continue;
    }

    // Skip if CSV already exists from a previous run
    if (skipExisting && existingCities.has(key)) {
      skipped++;
      continue;
    }

    queue.push(loc);
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  OPTIMIZED BATCH — ${ALL_LOCATIONS.length} cities`);
  console.log(`  US: ${US_CITIES.length} | CA: ${CA_CITIES.length} | UK: ${UK_CITIES.length} | AU: ${AU_CITIES.length} | IE: ${IE_CITIES.length}`);
  console.log(`  Grid: ${GRID_CELLS} cells | Crawl: ${MAX_CRAWL} sites | Maps cap: ${MAX_MAPS}`);
  console.log(`  Concurrency: ${CONCURRENCY} | ${queue.length} remaining | ${skipped} skipped (existing)`);
  console.log(`${'═'.repeat(70)}\n`);

  let queueIdx = 0;
  while (queueIdx < queue.length) {
    const batch = queue.slice(queueIdx, queueIdx + CONCURRENCY);
    const batchNames = batch.map(l => l.city.split(',')[0]).join(' | ');
    console.log(`  [${completed+1}-${Math.min(completed+batch.length, completed+CONCURRENCY)}] ${batchNames}`);

    const results = await Promise.all(batch.map(loc => runLocation(loc)));

    for (const r of results) {
      if (r.status === 'done') {
        completed++;
        totalLeads += r.leads;
        totalEmails += r.emails;
        const emailPct = r.leads > 0 ? Math.round(r.emails / r.leads * 100) : 0;
        console.log(`    ✓ ${r.city.split(',')[0]}: ${r.leads}L ${r.emails}E(${emailPct}%) ${r.dms}DM ${r.elapsed}s`);
      } else {
        failed++;
        console.log(`    ✗ ${r.city.split(',')[0]}: ${(r.error || '').slice(0,60)}`);
      }
    }
    queueIdx += CONCURRENCY;

    // Progress summary every 5 batches or at end
    if (queueIdx % (CONCURRENCY * 5) === 0 || queueIdx >= queue.length) {
      const elapsedTotal = Math.round((Date.now() - startTime) / 1000);
      const rate = completed > 0 ? totalLeads / (elapsedTotal / 60) : 0;
      const emailRate = totalLeads > 0 ? Math.round(totalEmails / totalLeads * 100) : 0;
      const batchesDone = Math.ceil(queueIdx / CONCURRENCY);
      const batchesLeft = Math.ceil((queue.length - queueIdx) / CONCURRENCY);
      const avgBatchTime = batchesDone > 0 ? elapsedTotal / batchesDone : 300;
      const etaMin = Math.round(batchesLeft * avgBatchTime / 60);
      console.log(`  ── ${completed} done | ${totalLeads} leads | ${totalEmails} emails (${emailRate}%) | ${failed} fail | ${Math.round(rate)}/min | ETA ~${etaMin}m ──`);
    }
  }

  const totalElapsed = Math.round((Date.now() - startTime) / 1000);
  const emailRate = totalLeads > 0 ? Math.round(totalEmails / totalLeads * 100) : 0;
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  BATCH COMPLETE: ${completed} done | ${totalLeads} leads | ${totalEmails} emails (${emailRate}%)`);
  console.log(`  Time: ${Math.round(totalElapsed / 60)}min | Rate: ${Math.round(totalLeads / (totalElapsed / 60))}/min`);
  console.log(`${'═'.repeat(70)}\n`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
