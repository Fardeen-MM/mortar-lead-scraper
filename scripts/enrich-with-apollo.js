#!/usr/bin/env node
/**
 * Bulk Email Enrichment via Apollo.io
 *
 * Reads a contractor CSV, searches Apollo for each lead by name/company/location,
 * attaches the best email found, and writes an enriched CSV.
 *
 * Rotates between multiple Apollo API keys to maximize credits.
 * Prioritizes owner/founder/president contacts when searching by org.
 *
 * Usage:
 *   node scripts/enrich-with-apollo.js output/dbpr-florida-all.csv
 *   node scripts/enrich-with-apollo.js output/dbpr-florida-all.csv --limit 1000
 *   node scripts/enrich-with-apollo.js output/dbpr-florida-all.csv --dry-run
 *   node scripts/enrich-with-apollo.js output/dbpr-florida-all.csv --limit 50 --delay 2000
 *
 * Options:
 *   --limit N      Cap total API calls (default: unlimited)
 *   --dry-run      Preview which leads would be enriched, no API calls
 *   --delay MS     Delay between API calls in ms (default: 1500)
 *   --offset N     Skip first N enrichable leads (for resuming)
 *   --output PATH  Custom output path (default: output/enriched-{filename}.csv)
 *
 * Env vars:
 *   APOLLO_API_KEYS=key1,key2   Comma-separated API keys (rotated round-robin)
 *   APOLLO_API_KEY=key           Single key fallback
 */

const fs = require('fs');
const path = require('path');

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const CSV_PATH = args.find(a => !a.startsWith('--') && a.endsWith('.csv'));
const LIMIT = parseInt(getArg('limit') || '0') || 0;
const DRY_RUN = hasFlag('dry-run');
const DELAY_MS = parseInt(getArg('delay') || '1500') || 1500;
const OFFSET = parseInt(getArg('offset') || '0') || 0;
const OUTPUT_PATH = getArg('output');

if (!CSV_PATH) {
  console.log('');
  console.log('Usage: node scripts/enrich-with-apollo.js <csv_file> [options]');
  console.log('');
  console.log('Options:');
  console.log('  --limit N      Cap total API calls');
  console.log('  --dry-run      Preview without API calls');
  console.log('  --delay MS     Delay between calls (default: 1500)');
  console.log('  --offset N     Skip first N enrichable leads');
  console.log('  --output PATH  Custom output path');
  console.log('');
  console.log('Env vars:');
  console.log('  APOLLO_API_KEYS=key1,key2   Comma-separated API keys');
  console.log('  APOLLO_API_KEY=key           Single key fallback');
  console.log('');
  process.exit(1);
}

// ─── CSV Parser (handles quoted fields with commas) ─────────────────

function parseCSVLine(line) {
  const vals = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      // Handle escaped quotes ""
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === ',' && !inQuotes) {
      vals.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  vals.push(current);
  return vals;
}

function parseCSV(content) {
  const lines = content.trim().split('\n');
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = parseCSVLine(lines[0]).map(h => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const vals = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => { row[h] = (vals[idx] || '').trim(); });
    row._lineIndex = i; // track original position
    rows.push(row);
  }
  return { headers, rows };
}

function escapeCSVField(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCSV(filePath, headers, rows) {
  const lines = [headers.map(escapeCSVField).join(',')];
  for (const row of rows) {
    lines.push(headers.map(h => escapeCSVField(row[h] || '')).join(','));
  }
  fs.writeFileSync(filePath, lines.join('\n'));
}

// ─── API Key Rotator ────────────────────────────────────────────────

class KeyRotator {
  constructor(keys) {
    this.keys = keys;
    this.index = 0;
    this.usagePerKey = {};
    for (const k of keys) {
      this.usagePerKey[k] = 0;
    }
  }

  next() {
    const key = this.keys[this.index % this.keys.length];
    this.usagePerKey[key]++;
    this.index++;
    return key;
  }

  getStats() {
    return this.keys.map((k, i) => ({
      key: `Key ${i + 1} (${k.slice(0, 6)}...)`,
      calls: this.usagePerKey[k],
    }));
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  // Load env
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  const ApolloClient = require('../lib/apollo-client');

  // ── Validate CSV ───────────────────────────────────────────────
  const csvFullPath = path.resolve(CSV_PATH);
  if (!fs.existsSync(csvFullPath)) {
    console.error(`File not found: ${csvFullPath}`);
    process.exit(1);
  }

  // ── Load API keys ─────────────────────────────────────────────
  const keysStr = process.env.APOLLO_API_KEYS || process.env.APOLLO_API_KEY || '';
  const apiKeys = keysStr.split(',').map(k => k.trim()).filter(Boolean);

  if (!DRY_RUN && apiKeys.length === 0) {
    console.error('No Apollo API keys found. Set APOLLO_API_KEYS or APOLLO_API_KEY in .env');
    process.exit(1);
  }

  const rotator = new KeyRotator(apiKeys.length > 0 ? apiKeys : ['dry-run-key']);

  // ── Parse CSV ─────────────────────────────────────────────────
  const csvContent = fs.readFileSync(csvFullPath, 'utf8');
  const { headers, rows } = parseCSV(csvContent);

  console.log('');
  console.log('==========================================================');
  console.log('   MORTAR — Apollo Email Enrichment');
  console.log('==========================================================');
  console.log('');
  console.log(`  Input:      ${path.basename(csvFullPath)} (${rows.length.toLocaleString()} leads)`);
  console.log(`  API Keys:   ${apiKeys.length} key(s) available`);
  console.log(`  Mode:       ${DRY_RUN ? 'DRY RUN (no API calls)' : 'LIVE'}`);
  if (LIMIT) console.log(`  Limit:      ${LIMIT} API calls`);
  if (OFFSET) console.log(`  Offset:     Skipping first ${OFFSET} enrichable leads`);
  console.log(`  Delay:      ${DELAY_MS}ms between calls`);

  // ── Identify enrichable leads ─────────────────────────────────
  // A lead is enrichable if it has no email and has either:
  //   a) a firm_name (search by org), or
  //   b) first_name + last_name + city (search by person)
  const alreadyHaveEmail = rows.filter(r => r.email).length;

  const enrichable = [];
  for (const row of rows) {
    if (row.email) continue; // already has email

    const hasFirm = !!(row.firm_name || '').trim();
    const hasName = !!((row.first_name || '').trim() && (row.last_name || '').trim());
    const hasLocation = !!((row.city || '').trim() || (row.state || '').trim());

    if (hasFirm || (hasName && hasLocation)) {
      enrichable.push(row);
    }
  }

  // Apply offset
  const enrichableSlice = OFFSET > 0 ? enrichable.slice(OFFSET) : enrichable;
  // Apply limit
  const toProcess = LIMIT > 0 ? enrichableSlice.slice(0, LIMIT) : enrichableSlice;

  const withFirm = toProcess.filter(r => (r.firm_name || '').trim()).length;
  const withoutFirm = toProcess.length - withFirm;

  console.log('');
  console.log(`  Already have email:   ${alreadyHaveEmail.toLocaleString()}`);
  console.log(`  Enrichable total:     ${enrichable.length.toLocaleString()}`);
  console.log(`  To process:           ${toProcess.length.toLocaleString()} (${withFirm} with firm, ${withoutFirm} person-only)`);
  console.log('');

  if (toProcess.length === 0) {
    console.log('  No leads to enrich.');
    return;
  }

  // ── Dry run preview ───────────────────────────────────────────
  if (DRY_RUN) {
    console.log('  --- DRY RUN PREVIEW (first 20 leads) ---');
    console.log('');
    const preview = toProcess.slice(0, 20);
    for (let i = 0; i < preview.length; i++) {
      const r = preview[i];
      const name = `${r.first_name || ''} ${r.last_name || ''}`.trim();
      const firm = r.firm_name || '(no firm)';
      const loc = [r.city, r.state].filter(Boolean).join(', ');
      const strategy = r.firm_name ? 'ORG search' : 'PERSON search';
      console.log(`  ${String(i + 1).padStart(3)}. ${name} | ${firm} | ${loc} | ${strategy}`);
    }
    if (toProcess.length > 20) {
      console.log(`  ... and ${toProcess.length - 20} more`);
    }
    console.log('');

    // Estimate credits
    const estimatedCredits = toProcess.length; // 1 credit per search
    console.log('  --- CREDIT ESTIMATE ---');
    console.log(`  API calls needed:     ~${estimatedCredits.toLocaleString()}`);
    console.log(`  Per key (${apiKeys.length} keys):    ~${Math.ceil(estimatedCredits / Math.max(apiKeys.length, 1)).toLocaleString()} each`);
    const creditsPerKey = 10000;
    const totalCredits = apiKeys.length * creditsPerKey;
    const budgetPct = totalCredits > 0 ? (estimatedCredits / totalCredits * 100).toFixed(1) : '0';
    console.log(`  Monthly budget:       ${totalCredits.toLocaleString()} credits (${apiKeys.length} x ${creditsPerKey.toLocaleString()})`)
    console.log(`  Budget usage:         ${budgetPct}% of monthly credits`);
    console.log('');
    console.log('  Remove --dry-run to execute.');
    console.log('');
    return;
  }

  // ── Run enrichment ────────────────────────────────────────────
  const startTime = Date.now();
  let apiCalls = 0;
  let emailsFound = 0;
  let errors = 0;
  let noResults = 0;
  let rateLimits = 0;
  let lastProgressLog = 0;

  // Progress file for resumability
  const progressPath = path.join(path.dirname(csvFullPath), `.apollo-progress-${path.basename(csvFullPath, '.csv')}.json`);

  // Load previous progress if exists
  const previousResults = {};
  if (fs.existsSync(progressPath)) {
    try {
      const saved = JSON.parse(fs.readFileSync(progressPath, 'utf8'));
      for (const r of saved.results || []) {
        const key = `${r.first_name}|${r.last_name}|${r.firm_name}|${r.city}`;
        previousResults[key] = r;
      }
      console.log(`  Loaded ${Object.keys(previousResults).length} cached results from previous run`);
    } catch {}
  }

  const allResults = [];

  for (let i = 0; i < toProcess.length; i++) {
    const row = toProcess[i];
    const name = `${row.first_name || ''} ${row.last_name || ''}`.trim();
    const firm = (row.firm_name || '').trim();
    const city = (row.city || '').trim();
    const state = (row.state || '').trim();
    const location = [city, state].filter(Boolean).join(', ');

    // Check cache
    const cacheKey = `${row.first_name}|${row.last_name}|${firm}|${city}`;
    if (previousResults[cacheKey]) {
      const cached = previousResults[cacheKey];
      if (cached.email) {
        row.email = cached.email;
        row.email_source = cached.email_source || 'apollo';
        row.apollo_title = cached.apollo_title || '';
        row.apollo_linkedin = cached.apollo_linkedin || '';
        emailsFound++;
      }
      allResults.push(cached);
      continue; // skip API call
    }

    // Pick API key (round-robin)
    const apiKey = rotator.next();
    const apollo = new ApolloClient({ apiKey });

    // Build search filters
    const filters = {};

    if (firm) {
      // Strategy A: Search by organization name
      filters.organization_name = firm;
      filters.person_seniorities = ['owner', 'founder', 'c_suite', 'partner', 'director'];
      if (location) {
        filters.person_locations = [location];
      }
    } else {
      // Strategy B: Search by person name + location
      filters.q_person_name = name;
      if (location) {
        filters.person_locations = [location];
      }
    }

    try {
      const result = await apollo.searchPeople(filters, 1, 1);
      apiCalls++;

      const person = (result.people || [])[0];

      const enrichResult = {
        first_name: row.first_name,
        last_name: row.last_name,
        firm_name: firm,
        city: city,
        email: '',
        email_source: '',
        apollo_title: '',
        apollo_linkedin: '',
        match_type: firm ? 'org' : 'person',
      };

      if (person && person.email) {
        row.email = person.email;
        row.email_source = 'apollo';
        row.apollo_title = person.title || '';
        row.apollo_linkedin = person.linkedin_url || '';
        // If no firm_name on the lead but Apollo found the org, backfill it
        if (!row.firm_name && person.firm_name) {
          row.firm_name = person.firm_name;
        }
        // Backfill website
        if (!row.website && person.website) {
          row.website = person.website;
        }

        enrichResult.email = person.email;
        enrichResult.email_source = 'apollo';
        enrichResult.apollo_title = person.title || '';
        enrichResult.apollo_linkedin = person.linkedin_url || '';
        emailsFound++;
      } else {
        noResults++;
      }

      allResults.push(enrichResult);

    } catch (err) {
      if (err.message.includes('429')) {
        rateLimits++;
        console.log(`  [!] Rate limited on key ${apiKey.slice(0, 6)}... — waiting 60s`);
        await sleep(60000);
        i--; // retry this lead
        continue;
      }
      errors++;
      allResults.push({
        first_name: row.first_name,
        last_name: row.last_name,
        firm_name: firm,
        city: city,
        error: err.message,
      });
    }

    // Progress logging
    const now = Date.now();
    if (now - lastProgressLog > 5000 || i === toProcess.length - 1) {
      const elapsed = ((now - startTime) / 1000).toFixed(1);
      const rate = elapsed > 0 ? (apiCalls / (elapsed / 60)).toFixed(0) : 0;
      const pct = ((i + 1) / toProcess.length * 100).toFixed(1);
      const hitRate = apiCalls > 0 ? (emailsFound / apiCalls * 100).toFixed(1) : '0.0';
      console.log(`  [${i + 1}/${toProcess.length}] ${pct}% | ${emailsFound} emails (${hitRate}% hit) | ${apiCalls} calls | ${rate}/min | ${elapsed}s`);
      lastProgressLog = now;

      // Save progress every 5s
      try {
        fs.writeFileSync(progressPath, JSON.stringify({
          timestamp: new Date().toISOString(),
          apiCalls,
          emailsFound,
          errors,
          results: allResults,
        }, null, 2));
      } catch {}
    }

    // Delay between calls
    if (i < toProcess.length - 1) {
      await sleep(DELAY_MS);
    }
  }

  // ── Write enriched CSV ────────────────────────────────────────
  // Add enrichment columns if not present
  const outputHeaders = [...headers];
  for (const col of ['email', 'email_source', 'apollo_title', 'apollo_linkedin', 'website']) {
    if (!outputHeaders.includes(col)) outputHeaders.push(col);
  }

  const outputFile = OUTPUT_PATH || path.join(
    path.dirname(csvFullPath),
    `enriched-${path.basename(csvFullPath)}`
  );

  // Ensure output directory exists
  const outputDir = path.dirname(outputFile);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  writeCSV(outputFile, outputHeaders, rows);

  // ── Summary ───────────────────────────────────────────────────
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalWithEmail = rows.filter(r => r.email).length;

  console.log('');
  console.log('==========================================================');
  console.log('   ENRICHMENT COMPLETE');
  console.log('==========================================================');
  console.log('');
  console.log(`  Total leads:          ${rows.length.toLocaleString()}`);
  console.log(`  Processed:            ${toProcess.length.toLocaleString()}`);
  console.log(`  API calls:            ${apiCalls.toLocaleString()}`);
  console.log(`  Emails found:         ${emailsFound.toLocaleString()}`);
  console.log(`  Hit rate:             ${apiCalls > 0 ? (emailsFound / apiCalls * 100).toFixed(1) : '0.0'}%`);
  console.log(`  No results:           ${noResults.toLocaleString()}`);
  console.log(`  Errors:               ${errors.toLocaleString()}`);
  console.log(`  Rate limits:          ${rateLimits.toLocaleString()}`);
  console.log(`  Time:                 ${elapsed}s`);
  console.log('');
  console.log(`  Total with email now: ${totalWithEmail.toLocaleString()} / ${rows.length.toLocaleString()} (${(totalWithEmail / rows.length * 100).toFixed(1)}%)`);
  console.log('');

  // Key usage breakdown
  console.log('  API Key Usage:');
  for (const stat of rotator.getStats()) {
    console.log(`    ${stat.key}: ${stat.calls} calls`);
  }
  console.log('');

  console.log(`  Output: ${outputFile}`);
  if (fs.existsSync(progressPath)) {
    console.log(`  Progress: ${progressPath}`);
  }
  console.log('');

  // Clean up progress file on successful completion
  if (errors === 0 && rateLimits === 0 && toProcess.length === (LIMIT || toProcess.length)) {
    try { fs.unlinkSync(progressPath); } catch {}
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
