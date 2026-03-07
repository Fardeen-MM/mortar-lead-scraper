#!/usr/bin/env node
/**
 * TRREB (Toronto Regional Real Estate Board) Agent Directory Scraper
 *
 * Scrapes ALL real estate agents from TRREB's public API.
 *
 * API: POST https://trebapi.torontomls.net/api/getrealtors/search
 * Body: { searchStr, page, pagesize, sortBy, sortAsc }
 * Hard cap: 100 results per page — must paginate
 * Strategy: Search a-z by last name initial, paginate all pages per letter.
 *           If a letter yields exactly totalCount == page*100 and there could be more,
 *           we sub-divide into 2-letter prefixes (Aa..Az), then 3-letter, etc.
 *
 * Usage:
 *   node scripts/scrape-trreb-agents.js               # Full run (a-z)
 *   node scripts/scrape-trreb-agents.js --resume       # Resume from checkpoint
 *   node scripts/scrape-trreb-agents.js --letters=S,T  # Only specific letters
 *   node scripts/scrape-trreb-agents.js --test         # Quick test (first 2 letters only)
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// === Configuration ===
const API_URL = 'https://trebapi.torontomls.net/api/getrealtors/search';
const RATE_LIMIT_MS = 200;
const PAGE_SIZE = 100;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const SAVE_EVERY = 500;
const MAX_PREFIX_DEPTH = 5; // max chars in prefix before firstname decomposition
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'trreb-real-estate-agents.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'trreb-real-estate-agents-progress.csv');
const CHECKPOINT_FILE = path.join(OUTPUT_DIR, '.trreb-checkpoint.json');

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz';

// === Parse CLI args ===
const args = process.argv.slice(2);
let resume = false;
let testMode = false;
let lettersOverride = null;
for (const arg of args) {
  if (arg === '--resume') resume = true;
  if (arg === '--test') testMode = true;
  if (arg.startsWith('--letters=')) {
    lettersOverride = arg.split('=')[1].toLowerCase().split(',').map(l => l.trim());
  }
}

let startLetters = lettersOverride || ALPHABET.split('');
if (testMode) startLetters = startLetters.slice(0, 2);

// === Stats ===
const stats = {
  totalRequests: 0,
  totalApiResults: 0,
  prefixesExpanded: 0,
  firstnameExpansions: 0,
  errors: 0,
  retries: 0,
  startTime: Date.now(),
  lastSaveCount: 0,
};

// === Dedup: key -> agent row ===
// Key = email (lowercase) || "firstname|lastname|firm" (lowercase)
const agents = new Map();
const seenKeys = new Set();

// === Checkpoint state ===
let completedLetters = new Set();

/**
 * Sleep for ms milliseconds
 */
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Make an HTTPS GET request with query params and retry logic
 * The TRREB API uses GET with query params: firstname, lastname, officename,
 * location, languages, designation, language, specialty, commercial
 */
function apiRequest(params, attempt = 1) {
  return new Promise((resolve, reject) => {
    const url = new URL(API_URL);
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v || '');
    }

    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
      },
    };

    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => (data += chunk));
      res.on('end', async () => {
        stats.totalRequests++;
        if (res.statusCode !== 200) {
          if (attempt < MAX_RETRIES) {
            stats.retries++;
            console.error(`  [WARN] HTTP ${res.statusCode} — retry ${attempt}/${MAX_RETRIES}...`);
            await sleep(RETRY_DELAY_MS * attempt);
            try {
              const result = await apiRequest(params, attempt + 1);
              resolve(result);
            } catch (e) {
              reject(e);
            }
            return;
          }
          stats.errors++;
          console.error(`  [ERR] HTTP ${res.statusCode} after ${MAX_RETRIES} retries`);
          resolve({ results: [], totalCount: 0 });
          return;
        }
        try {
          const json = JSON.parse(data);
          // API returns flat array of agents
          if (Array.isArray(json)) {
            resolve({ results: json, totalCount: json.length });
          } else if (json && Array.isArray(json.results)) {
            resolve({ results: json.results, totalCount: json.totalCount || json.results.length });
          } else {
            resolve({ results: [], totalCount: 0 });
          }
        } catch (e) {
          stats.errors++;
          console.error(`  [WARN] JSON parse error: ${e.message}`);
          resolve({ results: [], totalCount: 0 });
        }
      });
    });

    req.on('error', async e => {
      if (attempt < MAX_RETRIES) {
        stats.retries++;
        console.error(`  [WARN] Network error — retry ${attempt}/${MAX_RETRIES}: ${e.message}`);
        await sleep(RETRY_DELAY_MS * attempt);
        try {
          const result = await apiRequest(params, attempt + 1);
          resolve(result);
        } catch (err) {
          reject(err);
        }
        return;
      }
      stats.errors++;
      console.error(`  [ERR] Request failed after ${MAX_RETRIES} retries: ${e.message}`);
      resolve({ results: [], totalCount: 0 });
    });

    req.setTimeout(20000, () => {
      req.destroy();
      if (attempt < MAX_RETRIES) {
        stats.retries++;
        console.error(`  [WARN] Timeout — retry ${attempt}/${MAX_RETRIES}...`);
        sleep(RETRY_DELAY_MS * attempt).then(() => {
          apiRequest(params, attempt + 1).then(resolve).catch(reject);
        });
        return;
      }
      stats.errors++;
      console.error(`  [ERR] Timeout after ${MAX_RETRIES} retries`);
      resolve({ results: [], totalCount: 0 });
    });

    req.end();
  });
}

/**
 * Search TRREB API by lastname prefix
 * API is GET with query params: firstname, lastname, officename, location,
 * languages, designation, language, specialty, commercial
 * Returns flat array (no server-side pagination)
 */
async function searchByLastname(lastnamePrefix) {
  return apiRequest({
    firstname: '',
    lastname: lastnamePrefix,
    officename: '',
    location: '',
    languages: '',
    designation: '',
    language: '',
    specialty: '',
    commercial: '',
  });
}

/**
 * Search TRREB API with both lastname and firstname prefixes
 */
async function searchByName(lastname, firstname = '') {
  return apiRequest({
    firstname: firstname,
    lastname: lastname,
    officename: '',
    location: '',
    languages: '',
    designation: '',
    language: '',
    specialty: '',
    commercial: '',
  });
}

/**
 * Extract domain from URL or email
 */
function extractDomain(str) {
  if (!str) return '';
  // From email
  if (str.includes('@')) {
    const domain = str.split('@')[1];
    if (domain) return domain.toLowerCase().trim();
  }
  // From URL
  let d = str.replace(/^https?:\/\//i, '').replace(/^www\./i, '');
  d = d.split('/')[0].split('?')[0];
  return d.toLowerCase().trim();
}

/**
 * Clean phone number
 */
function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^0-9+\-() ]/g, '').trim();
}

/**
 * Build a dedup key for an agent
 */
function dedupKey(agent) {
  const email = (agent.email || agent.memberemail || '').trim().toLowerCase();
  if (email) return `email:${email}`;
  const fn = (agent.first_name || agent.firstname || '').trim().toLowerCase();
  const ln = (agent.last_name || agent.lastname || '').trim().toLowerCase();
  const firm = (agent.firm_name || agent.officename || '').trim().toLowerCase();
  return `name:${fn}|${ln}|${firm}`;
}

/**
 * Convert raw API agent object to our CSV row format
 */
function agentToRow(a) {
  const firstName = (a.firstname || a.first_name || '').trim();
  const lastName = (a.lastname || a.last_name || '').trim();
  const firm = (a.officename || a.firm_name || '').trim();
  const title = (a.title || '').trim();
  const email = (a.memberemail || a.email || '').trim().toLowerCase();
  const phone = cleanPhone(a.memberphone || a.phone || '');
  const website = (a.memberwebsite || a.website || '').trim();
  const brokerageWebsite = (a.brokeragewebsite || '').trim();
  const domain = extractDomain(email) || extractDomain(website) || extractDomain(brokerageWebsite);
  const city = (a.brokeragecity || a.location || a.city || '').trim();

  return {
    first_name: firstName,
    last_name: lastName,
    firm_name: firm,
    title: title,
    email: email,
    phone: phone,
    website: website,
    domain: domain,
    city: city,
    state: 'Ontario',
    country: 'CA',
    niche: 'real estate agent',
    source: 'trreb',
  };
}

/**
 * Add agents from API results to the dedup map
 * Returns count of new (non-duplicate) agents added
 */
function addAgents(results) {
  let newCount = 0;
  for (const a of results) {
    // Try memberId first for dedup (most reliable)
    const memberId = a.memberId || a.memberid;
    let key;
    if (memberId) {
      key = `id:${memberId}`;
    } else {
      key = dedupKey(a);
    }
    if (!seenKeys.has(key)) {
      seenKeys.add(key);
      // Also add secondary dedup keys
      const email = (a.memberemail || '').trim().toLowerCase();
      if (email) seenKeys.add(`email:${email}`);
      const fn = (a.firstname || '').trim().toLowerCase();
      const ln = (a.lastname || '').trim().toLowerCase();
      const firm = (a.officename || '').trim().toLowerCase();
      if (fn && ln) seenKeys.add(`name:${fn}|${ln}|${firm}`);

      agents.set(key, a);
      newCount++;
    }
  }
  stats.totalApiResults += results.length;
  return newCount;
}

/**
 * Search by lastname prefix, then expand by firstname if results are capped.
 * The API returns flat arrays — no server-side pagination.
 * Results appear to be capped at ~100 per query.
 */
async function searchPrefix(prefix, depth = 2) {
  await sleep(RATE_LIMIT_MS);

  const resp = await searchByLastname(prefix);
  let totalNew = addAgents(resp.results);

  if (resp.results.length > 0) {
    const rate = (stats.totalRequests / ((Date.now() - stats.startTime) / 1000)).toFixed(1);
    process.stdout.write(`  ${prefix}: ${resp.results.length} results (${totalNew} new) | total: ${agents.size} | ${rate} req/s\n`);
  }

  // If we got exactly PAGE_SIZE results, there may be more — expand deeper
  if (resp.results.length >= PAGE_SIZE && depth < MAX_PREFIX_DEPTH) {
    stats.prefixesExpanded++;
    for (const letter of ALPHABET) {
      const childNew = await searchPrefix(prefix + letter, depth + 1);
      totalNew += childNew;
      maybeSaveProgress();
    }
  } else if (resp.results.length >= PAGE_SIZE && depth >= MAX_PREFIX_DEPTH) {
    // At max depth — expand by firstname
    stats.prefixesExpanded++;
    console.log(`  ${prefix}: HIT CAP at max depth ${depth} — expanding by firstname...`);
    for (const letter of ALPHABET) {
      await sleep(RATE_LIMIT_MS);
      const fnResp = await searchByName(prefix, letter);
      const n = addAgents(fnResp.results);
      totalNew += n;
      stats.firstnameExpansions++;
      if (fnResp.results.length > 0) {
        process.stdout.write(`    fn="${letter}"+ln="${prefix}": ${fnResp.results.length} (${n} new)\n`);
      }
    }
  }

  return totalNew;
}

/**
 * Process a single letter (a-z) — iterate 2-letter prefixes
 */
async function processLetter(letter) {
  console.log(`\n${'─'.repeat(50)}`);
  console.log(`Processing letter: ${letter.toUpperCase()}`);
  console.log(`${'─'.repeat(50)}`);

  const letterStart = Date.now();
  let letterNew = 0;

  // Iterate 2-letter prefixes (aa..az)
  for (const second of ALPHABET) {
    const prefix = letter + second;
    const n = await searchPrefix(prefix, 2);
    letterNew += n;
    maybeSaveProgress();
  }

  const letterElapsed = Date.now() - letterStart;
  const rate = (stats.totalRequests / ((Date.now() - stats.startTime) / 1000)).toFixed(1);
  console.log(`Letter ${letter.toUpperCase()}: +${letterNew} new (${formatTime(letterElapsed)}) | total: ${agents.size} | ${rate} req/s`);

  return letterNew;
}

/**
 * Save progress CSV if threshold reached
 */
function maybeSaveProgress() {
  if (agents.size - stats.lastSaveCount >= SAVE_EVERY) {
    writeCSV(PROGRESS_FILE);
    stats.lastSaveCount = agents.size;
    console.log(`  [SAVE] Progress: ${agents.size} agents saved to ${path.basename(PROGRESS_FILE)}`);
  }
}

/**
 * Save checkpoint (completed letters + agent count)
 */
function saveCheckpoint() {
  const checkpoint = {
    completedLetters: [...completedLetters],
    agentCount: agents.size,
    stats: { ...stats, startTime: undefined },
    timestamp: new Date().toISOString(),
  };
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
}

/**
 * Load checkpoint and previously scraped CSV
 */
function loadCheckpoint() {
  // Load checkpoint metadata
  if (!fs.existsSync(CHECKPOINT_FILE)) {
    console.log('No checkpoint file found.');
    return false;
  }

  try {
    const data = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
    completedLetters = new Set(data.completedLetters || []);
    console.log(`Loaded checkpoint: ${completedLetters.size} letters completed (${[...completedLetters].join(', ')})`);
  } catch (e) {
    console.error(`Failed to load checkpoint: ${e.message}`);
    return false;
  }

  // Load existing progress CSV to rebuild dedup map
  const csvFile = fs.existsSync(PROGRESS_FILE) ? PROGRESS_FILE : OUTPUT_FILE;
  if (fs.existsSync(csvFile)) {
    try {
      const lines = fs.readFileSync(csvFile, 'utf8').split('\n').filter(l => l.trim());
      const headers = lines[0].split(',');
      let loaded = 0;

      for (let i = 1; i < lines.length; i++) {
        const fields = parseCSVLine(lines[i]);
        if (fields.length < headers.length) continue;

        const row = {};
        for (let j = 0; j < headers.length; j++) {
          row[headers[j]] = fields[j] || '';
        }

        // Map back to API-like format for agentToRow compatibility
        const agent = {
          firstname: row.first_name,
          lastname: row.last_name,
          officename: row.firm_name,
          title: row.title,
          memberemail: row.email,
          memberphone: row.phone,
          memberwebsite: row.website,
          brokeragecity: row.city,
        };

        const key = dedupKey(agent);
        if (!seenKeys.has(key)) {
          seenKeys.add(key);
          const email = (agent.memberemail || '').trim().toLowerCase();
          if (email) seenKeys.add(`email:${email}`);
          const fn = (agent.firstname || '').trim().toLowerCase();
          const ln = (agent.lastname || '').trim().toLowerCase();
          const firm = (agent.officename || '').trim().toLowerCase();
          if (fn && ln) seenKeys.add(`name:${fn}|${ln}|${firm}`);
          agents.set(key, agent);
          loaded++;
        }
      }

      console.log(`Loaded ${loaded} agents from ${path.basename(csvFile)}`);
      stats.lastSaveCount = agents.size;
    } catch (e) {
      console.error(`Failed to load CSV: ${e.message}`);
    }
  }

  return completedLetters.size > 0;
}

/**
 * Parse a CSV line respecting quoted fields
 */
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

/**
 * Escape a value for CSV
 */
function csvEscape(val) {
  const s = String(val || '');
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('`')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/**
 * Write all agents to a CSV file
 */
function writeCSV(filePath = OUTPUT_FILE) {
  const headers = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
    'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  ];

  const rows = [headers.join(',')];
  let emailCount = 0;
  let phoneCount = 0;
  let websiteCount = 0;

  for (const [, raw] of agents) {
    const row = agentToRow(raw);
    if (row.email) emailCount++;
    if (row.phone) phoneCount++;
    if (row.website) websiteCount++;
    rows.push(headers.map(h => csvEscape(row[h])).join(','));
  }

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(filePath, rows.join('\n') + '\n');

  return { emailCount, phoneCount, websiteCount };
}

/**
 * Format elapsed time
 */
function formatTime(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

/**
 * Print a stats summary line
 */
function printStats() {
  const elapsed = Date.now() - stats.startTime;
  const rate = (stats.totalRequests / (elapsed / 1000)).toFixed(1);
  console.log(`\n  Stats: ${agents.size} agents | ${stats.totalRequests} requests | ${rate} req/s | ${stats.errors} errors | ${stats.retries} retries | ${formatTime(elapsed)}`);
}

// === Main ===
async function main() {
  console.log('='.repeat(60));
  console.log('  TRREB Agent Directory Scraper');
  console.log('  Toronto Regional Real Estate Board');
  console.log('='.repeat(60));
  console.log(`  Letters:    ${startLetters.map(l => l.toUpperCase()).join(', ')}`);
  console.log(`  Rate limit: ${RATE_LIMIT_MS}ms between requests`);
  console.log(`  Page size:  ${PAGE_SIZE}`);
  console.log(`  Output:     ${OUTPUT_FILE}`);
  console.log(`  Progress:   ${PROGRESS_FILE}`);
  console.log(`  Resume:     ${resume ? 'YES' : 'NO'}`);
  console.log('');

  // Resume from checkpoint if requested
  if (resume) {
    loadCheckpoint();
    console.log(`Resuming with ${agents.size} agents already loaded.`);
    console.log(`Skipping completed letters: ${[...completedLetters].map(l => l.toUpperCase()).join(', ') || '(none)'}`);
  }

  // Probe the API
  console.log('\nProbing API...');
  const probe = await searchByLastname('sm');
  console.log(`API probe: "sm" returned ${probe.results.length} agents`);
  if (probe.results.length === 0) {
    console.error('API returned 0 results for "sm" — API may be down. Aborting.');
    process.exit(1);
  }
  addAgents(probe.results);
  console.log(`Sample: ${probe.results[0].firstname} ${probe.results[0].lastname} | ${probe.results[0].memberemail || 'no email'} | ${probe.results[0].officename}`);
  console.log(`Proceeding with 2-letter prefix expansion strategy.\n`);

  // Process each letter
  for (const letter of startLetters) {
    if (resume && completedLetters.has(letter)) {
      console.log(`\nSkipping letter ${letter.toUpperCase()} (already completed)`);
      continue;
    }

    await processLetter(letter);

    // Mark letter complete and save checkpoint + CSV
    completedLetters.add(letter);
    saveCheckpoint();
    writeCSV(PROGRESS_FILE);
    writeCSV(OUTPUT_FILE);

    printStats();
  }

  // Final save
  const { emailCount, phoneCount, websiteCount } = writeCSV(OUTPUT_FILE);

  // Clean up checkpoint on successful completion
  if (completedLetters.size === startLetters.length) {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      fs.unlinkSync(CHECKPOINT_FILE);
    }
    // Clean up progress file
    if (fs.existsSync(PROGRESS_FILE)) {
      fs.unlinkSync(PROGRESS_FILE);
    }
  }

  const elapsed = Date.now() - stats.startTime;
  console.log('\n' + '='.repeat(60));
  console.log('  SCRAPE COMPLETE');
  console.log('='.repeat(60));
  console.log(`  Total agents:       ${agents.size}`);
  console.log(`  With email:         ${emailCount}`);
  console.log(`  With phone:         ${phoneCount}`);
  console.log(`  With website:       ${websiteCount}`);
  console.log(`  API requests:       ${stats.totalRequests}`);
  console.log(`  Raw API results:    ${stats.totalApiResults}`);
  console.log(`  Prefixes expanded:  ${stats.prefixesExpanded}`);
  console.log(`  FN expansions:      ${stats.firstnameExpansions}`);
  console.log(`  Errors:             ${stats.errors}`);
  console.log(`  Retries:            ${stats.retries}`);
  console.log(`  Elapsed:            ${formatTime(elapsed)}`);
  console.log(`  Rate:               ${(stats.totalRequests / (elapsed / 1000)).toFixed(1)} req/s`);
  console.log(`  Output:             ${OUTPUT_FILE}`);
  console.log('='.repeat(60));
}

main().catch(err => {
  console.error('\nFatal error:', err);
  // Save progress before exiting
  try {
    saveCheckpoint();
    writeCSV(PROGRESS_FILE);
    writeCSV(OUTPUT_FILE);
    console.log(`Progress saved: ${agents.size} agents`);
  } catch (e) {
    console.error('Failed to save progress:', e.message);
  }
  process.exit(1);
});

// Graceful shutdown on Ctrl+C
process.on('SIGINT', () => {
  console.log('\n\nInterrupted! Saving progress...');
  try {
    saveCheckpoint();
    writeCSV(PROGRESS_FILE);
    writeCSV(OUTPUT_FILE);
    console.log(`Saved ${agents.size} agents. Use --resume to continue.`);
  } catch (e) {
    console.error('Failed to save:', e.message);
  }
  process.exit(0);
});
