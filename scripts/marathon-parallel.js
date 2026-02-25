#!/usr/bin/env node
/**
 * Marathon Parallel Scraper — runs MULTIPLE scrapers simultaneously.
 *
 * Key difference from marathon-scrape.js: runs 5 scrapers at once
 * (each bar association is a separate server, so no rate limit conflicts).
 *
 * Usage:
 *   node scripts/marathon-parallel.js
 *   node scripts/marathon-parallel.js --concurrency 8
 *   node scripts/marathon-parallel.js --until "2026-02-25T18:00:00"
 *   node scripts/marathon-parallel.js --skip CA-AB,AZ
 */

const path = require('path');
const { runPipeline } = require('../lib/pipeline');
const { getScraperMetadata } = require('../lib/registry');
const leadDb = require('../lib/lead-db');
const Database = require('better-sqlite3');

const DB_PATH = path.join(__dirname, '..', 'data', 'leads.db');

// Parse args
const args = process.argv.slice(2);
let stopTime;
let concurrency = 5;
let skipScrapers = new Set();

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--until' && args[i + 1]) {
    stopTime = new Date(args[i + 1]);
    i++;
  } else if (args[i] === '--concurrency' && args[i + 1]) {
    concurrency = parseInt(args[i + 1]);
    i++;
  } else if (args[i] === '--skip' && args[i + 1]) {
    args[i + 1].split(',').forEach(s => skipScrapers.add(s.trim().toUpperCase()));
    i++;
  }
}

if (!stopTime) {
  stopTime = new Date();
  stopTime.setHours(18, 0, 0, 0); // Default: 6 PM today
}

const SKIP_IN_BULK = new Set(['MARTINDALE', 'LAWYERS-COM', 'GOOGLE-PLACES', 'JUSTIA', 'AVVO', 'FINDLAW', 'GOOGLE-MAPS']);
const SLOW_SCRAPERS = new Set(['AK', 'NC', 'SC', 'NM', 'AU-NSW', 'IE']);
const FAST_TIMEOUT = 45 * 60 * 1000;  // 45 min for fast scrapers
const SLOW_TIMEOUT = 2 * 60 * 60 * 1000;  // 2 hours for slow scrapers

function getDbStats() {
  try {
    const db = new Database(DB_PATH, { readonly: true });
    const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
    const email = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
    const phone = db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NOT NULL AND phone != ''").get().c;
    db.close();
    return { total, email, phone };
  } catch (err) {
    return { total: '?', email: '?', phone: '?' };
  }
}

function timeLeft() {
  const ms = stopTime - Date.now();
  if (ms <= 0) return '0m';
  const h = Math.floor(ms / 3600000);
  const m = Math.floor((ms % 3600000) / 60000);
  return `${h}h ${m}m`;
}

function runScraper(code) {
  const isSlow = SLOW_SCRAPERS.has(code);
  const timeout = isSlow ? SLOW_TIMEOUT : FAST_TIMEOUT;

  return new Promise((resolve) => {
    const startTime = Date.now();
    let leads = [];
    let resolved = false;

    function finish(extra = {}) {
      if (resolved) return;
      resolved = true;
      const time = Math.round((Date.now() - startTime) / 1000);

      let dbStats = { inserted: 0, updated: 0, unchanged: 0 };
      if (leads.length > 0) {
        try {
          dbStats = leadDb.batchUpsert(leads, `scraper:${code}`);
        } catch (err) {
          console.error(`  [${code}] DB save failed: ${err.message}`);
        }
      }

      resolve({
        code,
        leads: leads.length,
        newInDb: dbStats.inserted,
        updatedInDb: dbStats.updated,
        time,
        ...extra,
      });
    }

    try {
      const emitter = runPipeline({
        state: code,
        test: false,
        emailScrape: false,
        waterfall: {
          masterDbLookup: true,
          fetchProfiles: false,
          crossRefMartindale: false,
          crossRefLawyersCom: false,
          nameLookups: false,
          emailCrawl: false,
        },
      });

      emitter.on('lead', d => leads.push(d.data));
      emitter.on('complete', () => finish());
      emitter.on('error', (data) => finish({ error: data.message }));

      setTimeout(() => {
        emitter.emit('cancel');
        finish({ timedOut: true });
      }, timeout);
    } catch (err) {
      finish({ error: err.message });
    }
  });
}

// Run N scrapers concurrently using a worker pool pattern
async function runPool(scraperCodes, maxConcurrency) {
  const results = {};
  let index = 0;
  let completed = 0;
  const total = scraperCodes.length;

  async function worker() {
    while (index < total && Date.now() < stopTime) {
      const idx = index++;
      const code = scraperCodes[idx];
      const isSlow = SLOW_SCRAPERS.has(code);
      console.log(`  [${idx + 1}/${total}] ${isSlow ? '[SLOW]' : '[FAST]'} Starting ${code}... (${timeLeft()} left)`);

      const result = await runScraper(code);
      results[code] = result;
      completed++;

      if (result.error) {
        console.log(`  [${completed}/${total}] ${code}: FAILED — ${result.error} (${result.time}s)`);
      } else if (result.timedOut) {
        console.log(`  [${completed}/${total}] ${code}: TIMEOUT — saved ${result.leads} leads, ${result.newInDb} new (${result.time}s)`);
      } else {
        console.log(`  [${completed}/${total}] ${code}: ${result.leads} leads, ${result.newInDb} new, ${result.updatedInDb} updated (${result.time}s)`);
      }

      // Print DB stats every 5 completions
      if (completed % 5 === 0) {
        const stats = getDbStats();
        console.log(`  >>> DB: ${stats.total} total, ${stats.email} email, ${stats.phone} phone <<<`);
      }
    }
  }

  // Launch N workers
  const workers = [];
  for (let i = 0; i < maxConcurrency; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  return results;
}

async function main() {
  console.log('='.repeat(70));
  console.log(`MARATHON PARALLEL SCRAPER (${concurrency} concurrent)`);
  console.log(`Stop time: ${stopTime.toLocaleString()}`);
  console.log(`Time remaining: ${timeLeft()}`);
  console.log('='.repeat(70));

  const metadata = getScraperMetadata();
  let allCodes = Object.entries(metadata)
    .filter(([, m]) => m.working)
    .map(([code]) => code)
    .filter(code => !SKIP_IN_BULK.has(code))
    .filter(code => !skipScrapers.has(code));

  const fastCodes = allCodes.filter(c => !SLOW_SCRAPERS.has(c));
  const slowCodes = allCodes.filter(c => SLOW_SCRAPERS.has(c));
  const scraperCodes = [...fastCodes, ...slowCodes];

  if (skipScrapers.size > 0) {
    console.log(`\nSkipping: ${[...skipScrapers].join(', ')}`);
  }
  console.log(`Total: ${scraperCodes.length} scrapers (${fastCodes.length} fast, ${slowCodes.length} slow)`);
  console.log(`Concurrency: ${concurrency}`);

  let pass = 0;
  const globalResults = {};

  while (Date.now() < stopTime) {
    pass++;
    const stats = getDbStats();
    console.log(`\n${'='.repeat(70)}`);
    console.log(`PASS ${pass} | ${new Date().toLocaleString()} | DB: ${stats.total} leads | Time left: ${timeLeft()}`);
    console.log('='.repeat(70));

    let toRun;
    if (pass === 1) {
      toRun = [...scraperCodes];
    } else {
      // Retry failed/timed-out scrapers
      toRun = scraperCodes.filter(code => {
        const prev = globalResults[code];
        return !prev || prev.error || prev.timedOut || prev.leads === 0;
      });
      if (toRun.length === 0) {
        console.log('All scrapers completed! Done.');
        break;
      }
      console.log(`Retrying ${toRun.length} scrapers: ${toRun.join(', ')}`);
    }

    const passResults = await runPool(toRun, concurrency);
    Object.assign(globalResults, passResults);

    // Post-pass enrichment
    const passNew = Object.values(passResults).reduce((sum, r) => sum + (r.newInDb || 0), 0);
    if (passNew > 0) {
      console.log(`\n[Enrichment] Post-pass enrichment (${passNew} new leads)...`);
      try {
        delete require.cache[require.resolve('../lib/lead-db')];
        const freshDb = require('../lib/lead-db');
        const mergeResult = freshDb.mergeDuplicates({});
        const firmResult = freshDb.shareFirmData();
        const deduceResult = freshDb.deduceWebsitesFromEmail();
        freshDb.batchScoreLeads();
        console.log(`  Merged: ${mergeResult.merged}, Firm shared: ${firmResult.leadsUpdated}, Websites deduced: ${deduceResult.leadsUpdated}`);
      } catch (err) {
        console.error(`  Enrichment error: ${err.message}`);
      }
    }

    // Pass summary
    const endStats = getDbStats();
    const success = Object.values(passResults).filter(r => r.leads > 0 && !r.error && !r.timedOut);
    const failed = Object.values(passResults).filter(r => r.error);
    const timedOut = Object.values(passResults).filter(r => r.timedOut);
    console.log(`\n--- Pass ${pass} Summary ---`);
    console.log(`  Success: ${success.length} | Failed: ${failed.length} | Timed out: ${timedOut.length}`);
    console.log(`  New leads: ${passNew}`);
    console.log(`  DB: ${endStats.total} total | ${endStats.email} email | ${endStats.phone} phone`);
    if (failed.length > 0) console.log(`  Failed: ${failed.map(r => r.code).join(', ')}`);
    if (timedOut.length > 0) console.log(`  Timed out: ${timedOut.map(r => `${r.code}(${r.leads})`).join(', ')}`);
  }

  // Final summary
  const finalStats = getDbStats();
  console.log('\n' + '='.repeat(70));
  console.log('MARATHON COMPLETE');
  console.log('='.repeat(70));
  console.log(`  DB: ${finalStats.total} total | ${finalStats.email} email | ${finalStats.phone} phone`);

  console.log('\n--- Per-Scraper Results ---');
  Object.values(globalResults)
    .sort((a, b) => (b.newInDb || 0) - (a.newInDb || 0))
    .forEach(r => {
      const status = r.error ? `FAIL: ${r.error}` : r.timedOut ? `TIMEOUT (${r.leads} partial)` : `${r.leads} leads`;
      console.log(`  ${r.code.padEnd(12)} ${status.padEnd(35)} ${(r.newInDb || 0)} new  ${r.time || 0}s`);
    });

  process.exit(0);
}

main().catch(err => {
  console.error('Marathon error:', err);
  process.exit(1);
});
