#!/usr/bin/env node
/**
 * Bulk find-leads — run find-leads.js across many (niche, location) pairs.
 *
 * Usage:
 *   node scripts/bulk-find-leads.js --cities cities.txt --niche "immigration consultant"
 *   node scripts/bulk-find-leads.js --cities cities.txt --niches "immigration lawyer,visa consultant"
 *
 * Cities file (one per line):
 *   Toronto, ON
 *   Vancouver, BC
 *   Calgary, AB
 *   New York, NY
 *   ...
 *
 * Output: output/bulk-leads-{niche}.csv with all results merged + deduped.
 *
 * Pacing: 30-60 sec delay between queries to respect rate limits.
 */

const fs = require('fs');
const path = require('path');
const multiEngine = require('../lib/multi-engine-search');
const { EmailWaterfall } = require('../lib/email-waterfall');

const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const s = a.replace(/^--/, '');
  if (s.includes('=')) { const [k, v] = s.split('='); args[k] = v; }
  else { const n = argv[i + 1]; if (n && !n.startsWith('--')) { args[s] = n; i++; } else args[s] = true; }
}

const CITIES_FILE = args.cities;
const NICHE = args.niche;
const NICHES = args.niches ? args.niches.split('|').map(s => s.trim()) : [NICHE].filter(Boolean);
const MAX_PER_QUERY = parseInt(args.max) || 20;
const DELAY_SEC = parseInt(args.delay) || 45;
const VERIFY = args.verify !== 'false' && !args['no-verify'];
const OUT = args.out || path.join(__dirname, '..', 'output', `bulk-leads-${(NICHES[0] || 'query').replace(/\W+/g, '-').toLowerCase()}.csv`);
const STATE_FILE = OUT.replace(/\.csv$/, '.state.json');

if (!CITIES_FILE || !NICHES.length) {
  console.error('Usage: node scripts/bulk-find-leads.js --cities cities.txt --niche "immigration consultant" [--max 20] [--delay 45]');
  process.exit(1);
}

if (!fs.existsSync(CITIES_FILE)) {
  console.error(`Cities file not found: ${CITIES_FILE}`);
  process.exit(1);
}

const cities = fs.readFileSync(CITIES_FILE, 'utf-8')
  .split(/\r?\n/)
  .map(s => s.trim())
  .filter(s => s && !s.startsWith('#'));

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log(`=== Bulk find-leads ===`);
  console.log(`Cities: ${cities.length}`);
  console.log(`Niches: ${NICHES.join(' | ')}`);
  console.log(`Max per query: ${MAX_PER_QUERY}`);
  console.log(`Delay between queries: ${DELAY_SEC}s`);
  console.log(`Output: ${OUT}`);
  console.log(`State: ${STATE_FILE}`);
  console.log(`Total queries: ${cities.length * NICHES.length}`);
  console.log('');

  const waterfall = new EmailWaterfall();
  const seenDomains = new Set();

  const cols = [
    'email', 'confidence', 'domain', 'company', 'title_snippet',
    'niche', 'location', 'source_engine', 'url', 'status',
  ];

  // Load state + existing CSV (resumable)
  let state = { completed: [] };
  if (fs.existsSync(STATE_FILE)) {
    try {
      state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
      console.log(`Resume: ${state.completed.length} (city, niche) combos already done`);
    } catch {}
  }
  const completedSet = new Set(state.completed);

  // Load existing CSV to seed seenDomains (dedup across resume)
  if (fs.existsSync(OUT)) {
    try {
      const existing = fs.readFileSync(OUT, 'utf-8').split('\n');
      for (let i = 1; i < existing.length; i++) {
        if (!existing[i]) continue;
        // Simple parse — domain is 3rd col
        const cols = existing[i].split(',');
        if (cols[2]) seenDomains.add(cols[2].replace(/^"|"$/g, ''));
      }
      console.log(`Loaded ${seenDomains.size} existing domains`);
    } catch {}
  } else {
    // Write header immediately
    fs.writeFileSync(OUT, cols.join(',') + '\n');
  }

  let totalQueries = 0;
  let totalDomains = seenDomains.size;
  let totalEmails = 0;
  let skipped = 0;
  const totalExpected = cities.length * NICHES.length;
  const start = Date.now();

  for (const city of cities) {
    for (const niche of NICHES) {
      totalQueries++;
      const comboKey = `${city}::${niche}`;
      if (completedSet.has(comboKey)) {
        skipped++;
        if (skipped % 50 === 0) console.log(`  (skipped ${skipped} already-done)`);
        continue;
      }
      const query = `${niche} ${city}`;
      console.log(`\n[${totalQueries}/${totalExpected}] ${query}`);

      let results;
      try {
        results = await multiEngine.search(query, { max: MAX_PER_QUERY, maxEngines: 2 });
      } catch (err) {
        console.log(`  discovery failed: ${err.message}`);
        continue;
      }

      console.log(`  Found ${results.length} domains (via multi-engine)`);

      let verified = 0;
      for (const r of results) {
        if (!r.domain || seenDomains.has(r.domain)) continue;
        seenDomains.add(r.domain);
        totalDomains++;

        // Quick generic-email verification (only if --verify; default on but --no-verify skips)
        let emailResult = { email: '', confidence: 0, status: 'not_verified' };
        if (VERIFY) {
          // Only try 'info@' (fastest, most common). Cap total wait to 8s.
          const candidate = `info@${r.domain}`;
          try {
            const v = await Promise.race([
              waterfall.verifyEmail(candidate),
              new Promise(r => setTimeout(() => r({ valid: null, reason: 'timeout' }), 8000)),
            ]);
            if (v && v.valid === true) {
              emailResult = {
                email: candidate,
                confidence: v.confidence || 70,
                status: 'verified',
              };
            } else {
              emailResult.status = v?.reason || 'not_verified';
            }
          } catch {}
        }

        if (emailResult.email) {
          verified++;
          totalEmails++;
        }

        const row = {
          email: emailResult.email,
          confidence: emailResult.confidence,
          domain: r.domain,
          company: (r.title || '').split(/[|•·\-–—]/)[0].trim().slice(0, 80),
          title_snippet: (r.snippet || '').slice(0, 200),
          niche,
          location: city,
          source_engine: r.source,
          url: r.url,
          status: emailResult.status,
        };
        // Append to CSV (streaming — no in-memory accumulation)
        fs.appendFileSync(OUT, cols.map(c => escapeCsv(row[c] || '')).join(',') + '\n');
      }

      console.log(`  ${verified} verified emails`);
      console.log(`  Totals: ${totalDomains} domains, ${totalEmails} emails`);

      // Save state after each combo (resumable)
      state.completed.push(comboKey);
      try { fs.writeFileSync(STATE_FILE, JSON.stringify(state)); } catch {}

      // Polite delay
      if (totalQueries < totalExpected) {
        await sleep(DELAY_SEC * 1000);
      }
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(0);
  console.log(`\n=== DONE in ${elapsed}s ===`);
  console.log(`Queries:         ${totalQueries}`);
  console.log(`Unique domains:  ${totalDomains}`);
  console.log(`Verified emails: ${totalEmails}`);
  console.log(`Output: ${OUT}`);

  // Engine status
  console.log('\nEngine status:');
  for (const s of multiEngine.status()) {
    console.log(`  ${s.name}: backed_off=${s.backed_off}, count=${s.backoff_count}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
