#!/usr/bin/env node
/**
 * Marathon Scraper — runs all working scrapers until a target time.
 *
 * Strategy:
 *   - FAST scrapers first (pure HTML/API, no per-lead requests)
 *   - SLOW scrapers after (per-lead AJAX/profile fetches)
 *   - 2-hour timeout per scraper
 *   - After first pass, retry any that failed
 *   - Keep looping until the target stop time (default: 9 AM)
 *   - Re-run enrichment after each full pass
 *
 * Usage:
 *   node scripts/marathon-scrape.js
 *   node scripts/marathon-scrape.js --until "2026-02-25T14:00:00"
 *   node scripts/marathon-scrape.js --scrapers FL,GA,NY
 */

const path = require('path');
const { runPipeline } = require('../lib/pipeline');
const { getScraperMetadata } = require('../lib/registry');
const leadDb = require('../lib/lead-db');
const Database = require('better-sqlite3');

const DB_PATH = path.join(__dirname, '..', 'data', 'leads.db');

// Safe DB stats query (opens fresh read-only connection)
function getDbStats() {
  try {
    const db = new Database(DB_PATH, { readonly: true });
    const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
    const email = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
    const phone = db.prepare("SELECT COUNT(*) as c FROM leads WHERE phone IS NOT NULL AND phone != ''").get().c;
    db.close();
    return { total, email, phone };
  } catch (err) {
    return { total: '?', email: '?', phone: '?', error: err.message };
  }
}

// Parse args
const args = process.argv.slice(2);
let stopTime;
let onlyScrapers = null;
let skipScrapers = new Set();

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--until' && args[i + 1]) {
    stopTime = new Date(args[i + 1]);
    i++;
  } else if (args[i] === '--scrapers' && args[i + 1]) {
    onlyScrapers = args[i + 1].split(',').map(s => s.trim().toUpperCase());
    i++;
  } else if (args[i] === '--skip' && args[i + 1]) {
    args[i + 1].split(',').forEach(s => skipScrapers.add(s.trim().toUpperCase()));
    i++;
  }
}

// Default: 9 AM today (or tomorrow if already past 9 AM)
if (!stopTime) {
  const now = new Date();
  stopTime = new Date(now);
  if (now.getHours() < 9) {
    stopTime.setHours(9, 0, 0, 0);
  } else {
    stopTime.setDate(stopTime.getDate() + 1);
    stopTime.setHours(9, 0, 0, 0);
  }
}

const SKIP_IN_BULK = new Set(['MARTINDALE', 'LAWYERS-COM', 'GOOGLE-PLACES', 'JUSTIA', 'AVVO', 'FINDLAW', 'GOOGLE-MAPS']);

// Scrapers with inline per-lead AJAX/profile calls in search() — inherently slow
const SLOW_SCRAPERS = new Set([
  'AK',     // _fetchAddress AJAX per lead
  'NC',     // fetchProfilePage inline, max 20/city
  'SC',     // _fetchDetail POST per lead
  'NM',     // per-lead profile fetch (~8s each)
  'AU-NSW', // detail API per lead
  'IE',     // per-lead profile fetch
]);

// Timeouts: fast scrapers get 30 min, slow ones get 2 hours
const FAST_TIMEOUT = 30 * 60 * 1000;
const SLOW_TIMEOUT = 2 * 60 * 60 * 1000;

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

  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    let leads = [];
    let resolved = false;

    const emitter = runPipeline({
      state: code,
      test: false,
      emailScrape: false,
      waterfall: {
        masterDbLookup: true,
        fetchProfiles: false,  // Waterfall profile fetching disabled (slow scrapers do it inline anyway)
        crossRefMartindale: false,
        crossRefLawyersCom: false,
        nameLookups: false,
        emailCrawl: false,
      },
    });

    emitter.on('lead', d => leads.push(d.data));

    emitter.on('complete', (data) => {
      if (resolved) return;
      resolved = true;
      const time = Math.round((Date.now() - startTime) / 1000);

      let dbStats = { inserted: 0, updated: 0, unchanged: 0 };
      if (leads.length > 0) {
        try {
          dbStats = leadDb.batchUpsert(leads, `scraper:${code}`);
        } catch (err) {
          console.error(`  [DB] Save failed for ${code}: ${err.message}`);
        }
      }

      resolve({
        code,
        leads: leads.length,
        emails: data.stats?.emailsFound || 0,
        newInDb: dbStats.inserted,
        updatedInDb: dbStats.updated,
        time,
      });
    });

    emitter.on('error', (data) => {
      if (resolved) return;
      resolved = true;
      reject(new Error(data.message || 'Unknown error'));
    });

    setTimeout(() => {
      if (resolved) return;
      resolved = true;
      emitter.emit('cancel');

      // Save partial results
      let dbStats = { inserted: 0, updated: 0, unchanged: 0 };
      if (leads.length > 0) {
        try {
          dbStats = leadDb.batchUpsert(leads, `scraper:${code}`);
          console.log(`  [Timeout] Saved ${leads.length} partial leads for ${code}`);
        } catch (err) {
          console.error(`  [DB] Partial save failed: ${err.message}`);
        }
      }

      resolve({
        code,
        leads: leads.length,
        newInDb: dbStats.inserted,
        updatedInDb: dbStats.updated,
        time: Math.round((Date.now() - startTime) / 1000),
        timedOut: true,
      });
    }, timeout);
  });
}

async function main() {
  console.log('='.repeat(70));
  console.log('MARATHON SCRAPER v2 (fast-first strategy)');
  console.log(`Stop time: ${stopTime.toLocaleString()}`);
  console.log(`Time remaining: ${timeLeft()}`);
  console.log('='.repeat(70));

  const metadata = getScraperMetadata();
  let allCodes = onlyScrapers || Object.entries(metadata)
    .filter(([, m]) => m.working)
    .map(([code]) => code)
    .filter(code => !SKIP_IN_BULK.has(code))
    .filter(code => !skipScrapers.has(code));

  if (skipScrapers.size > 0) {
    console.log(`\nSkipping (already done): ${[...skipScrapers].join(', ')}`);
  }

  // Sort: fast scrapers first, slow scrapers last
  const fastCodes = allCodes.filter(c => !SLOW_SCRAPERS.has(c));
  const slowCodes = allCodes.filter(c => SLOW_SCRAPERS.has(c));
  const scraperCodes = [...fastCodes, ...slowCodes];

  console.log(`\nTotal scrapers: ${scraperCodes.length}`);
  console.log(`Fast (${fastCodes.length}): ${fastCodes.join(', ')}`);
  console.log(`Slow (${slowCodes.length}): ${slowCodes.join(', ')}`);

  let pass = 0;
  let totalNewLeads = 0;
  const allResults = {};

  while (Date.now() < stopTime) {
    pass++;
    console.log(`\n${'='.repeat(70)}`);
    console.log(`PASS ${pass} | ${new Date().toLocaleString()} | Time left: ${timeLeft()}`);
    console.log('='.repeat(70));

    let toRun;
    if (pass === 1) {
      toRun = [...scraperCodes];
    } else {
      toRun = scraperCodes.filter(code => {
        const prev = allResults[code];
        return !prev || prev.error || prev.timedOut || prev.leads === 0;
      });
      if (toRun.length === 0) {
        console.log('All scrapers completed successfully! No retries needed.');
        break;
      }
      console.log(`Retrying ${toRun.length} failed/timed-out scrapers: ${toRun.join(', ')}`);
    }

    let passNew = 0;

    for (let i = 0; i < toRun.length; i++) {
      if (Date.now() >= stopTime) {
        console.log('\n[Marathon] Stop time reached, ending scrape.');
        break;
      }

      const code = toRun[i];
      const isSlow = SLOW_SCRAPERS.has(code);
      const tag = isSlow ? '[SLOW]' : '[FAST]';
      const progress = `[${i + 1}/${toRun.length}]`;
      console.log(`\n${progress} ${tag} Scraping ${code}... (${timeLeft()} remaining)`);

      try {
        const result = await runScraper(code);
        allResults[code] = result;

        if (result.timedOut) {
          console.log(`  ${code}: TIMEOUT after ${result.time}s — saved ${result.leads} partial leads (${result.newInDb} new)`);
        } else if (result.leads === 0) {
          console.log(`  ${code}: 0 leads (empty or error)`);
        } else {
          console.log(`  ${code}: ${result.leads} leads, ${result.newInDb} new, ${result.updatedInDb} updated (${result.time}s)`);
          passNew += result.newInDb;
          totalNewLeads += result.newInDb;
        }
      } catch (err) {
        console.error(`  ${code}: FAILED — ${err.message}`);
        allResults[code] = { code, error: err.message, leads: 0, newInDb: 0 };
      }

      // Print running totals every 5 scrapers
      if ((i + 1) % 5 === 0) {
        const stats = getDbStats();
        console.log(`  --- Running total: ${stats.total} leads in DB, ${passNew} new this pass ---`);
      }
    }

    // Post-pass enrichment
    if (passNew > 0) {
      console.log(`\n[Enrichment] Running post-pass enrichment (${passNew} new leads this pass)...`);
      try {
        // Re-require lead-db to get a fresh instance in case the old one closed
        delete require.cache[require.resolve('../lib/lead-db')];
        const freshDb = require('../lib/lead-db');
        const mergeResult = freshDb.mergeDuplicates({});
        const firmResult = freshDb.shareFirmData();
        const deduceResult = freshDb.deduceWebsitesFromEmail();
        freshDb.batchScoreLeads();
        console.log(`  Merged: ${mergeResult.merged} dupes, Firm shared: ${firmResult.leadsUpdated}, Websites deduced: ${deduceResult.leadsUpdated}`);
      } catch (err) {
        console.error(`  Enrichment error: ${err.message}`);
      }
    }

    // Print pass summary
    const stats = getDbStats();
    console.log(`\n--- Pass ${pass} Summary ---`);
    console.log(`  New leads this pass: ${passNew}`);
    console.log(`  Total new leads (all passes): ${totalNewLeads}`);
    console.log(`  Master DB total: ${stats.total} | Email: ${stats.email} | Phone: ${stats.phone}`);
    console.log(`  Time remaining: ${timeLeft()}`);

    const success = Object.values(allResults).filter(r => r.leads > 0 && !r.error && !r.timedOut);
    const failed = Object.values(allResults).filter(r => r.error);
    const timedOut = Object.values(allResults).filter(r => r.timedOut);
    console.log(`\n  Success: ${success.length} | Failed: ${failed.length} | Timed out: ${timedOut.length}`);
    if (failed.length > 0) console.log(`  Failed: ${failed.map(r => r.code).join(', ')}`);
    if (timedOut.length > 0) console.log(`  Timed out: ${timedOut.map(r => `${r.code}(${r.leads})`).join(', ')}`);
  }

  // Final summary
  console.log('\n' + '='.repeat(70));
  console.log('MARATHON COMPLETE');
  console.log('='.repeat(70));

  const finalStats = getDbStats();
  console.log(`  Passes completed: ${pass}`);
  console.log(`  Total new leads: ${totalNewLeads}`);
  console.log(`  Master DB total: ${finalStats.total}`);
  console.log(`  Master DB with email: ${finalStats.email}`);
  console.log(`  Master DB with phone: ${finalStats.phone}`);

  console.log('\n--- Per-Scraper Results ---');
  Object.values(allResults)
    .sort((a, b) => (b.newInDb || 0) - (a.newInDb || 0))
    .forEach(r => {
      const status = r.error ? `FAIL: ${r.error}` : r.timedOut ? `TIMEOUT (${r.leads} partial)` : `${r.leads} leads`;
      console.log(`  ${r.code.padEnd(12)} ${status.padEnd(30)} ${(r.newInDb || 0)} new  ${r.time || 0}s`);
    });

  process.exit(0);
}

main().catch(err => {
  console.error('Marathon error:', err);
  process.exit(1);
});
