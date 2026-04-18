#!/usr/bin/env node
/**
 * Apollo Batch Scraper — Pull lawyer leads from ALL major US/UK/CA/AU cities
 *
 * Runs apollo-scrape.js for each location × title combination.
 * Manages rate limits across jobs (Apollo has 600 req/day on free tier).
 *
 * Usage:
 *   # Cookie mode (recommended — gets emails)
 *   APOLLO_COOKIE="..." node scripts/apollo-batch.js
 *
 *   # API key mode (free search, scrapes websites for emails)
 *   APOLLO_API_KEY=xxx node scripts/apollo-batch.js
 *
 * Options:
 *   --max-per-search  Max results per search (default: 2000)
 *   --resume          Resume from progress log
 *   --test            Test mode (200 results per search)
 *   --preset          Preset: "lawyers", "all-lawyers", "dentists", "custom"
 *   --titles          Override titles (comma-separated)
 *   --seniorities     Override seniorities (comma-separated)
 *   --locations       Override locations (semicolon-separated)
 */

const path = require('path');
const fs = require('fs');
const { execSync, exec } = require('child_process');

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return undefined;
  return args[idx + 1];
}

function hasFlag(name) {
  return args.includes(`--${name}`);
}

const MAX_PER_SEARCH = parseInt(getArg('max-per-search') || '2000');
const RESUME = hasFlag('resume');
const TEST_MODE = hasFlag('test');
const PRESET = getArg('preset') || 'lawyers';

// ─── Presets ───────────────────────────────────────────────────────

const PRESETS = {
  lawyers: {
    titles: ['attorney', 'lawyer', 'partner', 'counsel', 'associate attorney'],
    seniorities: ['owner', 'founder', 'partner', 'c_suite', 'director', 'manager'],
    locations: [
      // Top 50 US states
      'Alabama, US', 'Alaska, US', 'Arizona, US', 'Arkansas, US', 'California, US',
      'Colorado, US', 'Connecticut, US', 'Delaware, US', 'Florida, US', 'Georgia, US',
      'Hawaii, US', 'Idaho, US', 'Illinois, US', 'Indiana, US', 'Iowa, US',
      'Kansas, US', 'Kentucky, US', 'Louisiana, US', 'Maine, US', 'Maryland, US',
      'Massachusetts, US', 'Michigan, US', 'Minnesota, US', 'Mississippi, US', 'Missouri, US',
      'Montana, US', 'Nebraska, US', 'Nevada, US', 'New Hampshire, US', 'New Jersey, US',
      'New Mexico, US', 'New York, US', 'North Carolina, US', 'North Dakota, US', 'Ohio, US',
      'Oklahoma, US', 'Oregon, US', 'Pennsylvania, US', 'Rhode Island, US', 'South Carolina, US',
      'South Dakota, US', 'Tennessee, US', 'Texas, US', 'Utah, US', 'Vermont, US',
      'Virginia, US', 'Washington, US', 'West Virginia, US', 'Wisconsin, US', 'Wyoming, US',
    ],
  },
  'all-lawyers': {
    titles: ['attorney', 'lawyer', 'partner', 'counsel', 'associate attorney', 'managing partner', 'senior partner', 'of counsel'],
    seniorities: ['owner', 'founder', 'partner', 'c_suite', 'director', 'manager', 'senior'],
    locations: [
      // US states
      'Alabama, US', 'Alaska, US', 'Arizona, US', 'Arkansas, US', 'California, US',
      'Colorado, US', 'Connecticut, US', 'Delaware, US', 'Florida, US', 'Georgia, US',
      'Hawaii, US', 'Idaho, US', 'Illinois, US', 'Indiana, US', 'Iowa, US',
      'Kansas, US', 'Kentucky, US', 'Louisiana, US', 'Maine, US', 'Maryland, US',
      'Massachusetts, US', 'Michigan, US', 'Minnesota, US', 'Mississippi, US', 'Missouri, US',
      'Montana, US', 'Nebraska, US', 'Nevada, US', 'New Hampshire, US', 'New Jersey, US',
      'New Mexico, US', 'New York, US', 'North Carolina, US', 'North Dakota, US', 'Ohio, US',
      'Oklahoma, US', 'Oregon, US', 'Pennsylvania, US', 'Rhode Island, US', 'South Carolina, US',
      'South Dakota, US', 'Tennessee, US', 'Texas, US', 'Utah, US', 'Vermont, US',
      'Virginia, US', 'Washington, US', 'West Virginia, US', 'Wisconsin, US', 'Wyoming, US',
      // UK
      'England, United Kingdom', 'Scotland, United Kingdom', 'Wales, United Kingdom', 'Northern Ireland, United Kingdom',
      // Canada
      'Ontario, Canada', 'Quebec, Canada', 'British Columbia, Canada', 'Alberta, Canada',
      'Manitoba, Canada', 'Saskatchewan, Canada', 'Nova Scotia, Canada', 'New Brunswick, Canada',
      // Australia
      'New South Wales, Australia', 'Victoria, Australia', 'Queensland, Australia',
      'Western Australia, Australia', 'South Australia, Australia',
      // Ireland
      'Ireland',
    ],
  },
};

// ─── Build job list ────────────────────────────────────────────────

function buildJobs() {
  const preset = PRESETS[PRESET];

  // Allow CLI overrides
  const titles = getArg('titles') ? getArg('titles').split(',').map(s => s.trim()) : (preset ? preset.titles : ['attorney']);
  const seniorities = getArg('seniorities') ? getArg('seniorities').split(',').map(s => s.trim()) : (preset ? preset.seniorities : []);
  const locations = getArg('locations') ? getArg('locations').split(';').map(s => s.trim()) : (preset ? preset.locations : []);

  if (locations.length === 0) {
    console.error('No locations specified. Use --preset or --locations.');
    process.exit(1);
  }

  // One job per location (Apollo handles multiple titles in one query)
  return locations.map(location => ({
    titles,
    seniorities,
    location,
  }));
}

// ─── Progress tracking ─────────────────────────────────────────────

const progressFile = path.join(__dirname, '..', 'output', 'apollo-batch-progress.json');

function loadProgress() {
  if (fs.existsSync(progressFile)) {
    try {
      return JSON.parse(fs.readFileSync(progressFile, 'utf8'));
    } catch {
      return {};
    }
  }
  return {};
}

function saveProgress(progress) {
  const dir = path.dirname(progressFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(progressFile, JSON.stringify(progress, null, 2));
}

function jobKey(job) {
  return `${job.location}`;
}

// ─── Run a single job ──────────────────────────────────────────────

function runJob(job) {
  return new Promise((resolve) => {
    const key = jobKey(job);
    const startTime = Date.now();

    const titlesArg = job.titles.join(',');
    const seniorArg = job.seniorities.length > 0 ? `--seniorities "${job.seniorities.join(',')}"` : '';
    const maxArg = TEST_MODE ? '--test' : `--max-results ${MAX_PER_SEARCH}`;

    // Build command — detect auth mode from env
    const authEnv = process.env.APOLLO_COOKIE
      ? `APOLLO_COOKIE="${process.env.APOLLO_COOKIE}"`
      : `APOLLO_API_KEY="${process.env.APOLLO_API_KEY}"`;

    const cmd = [
      authEnv,
      'node', path.join(__dirname, 'apollo-scrape.js'),
      `--titles "${titlesArg}"`,
      `--locations "${job.location}"`,
      seniorArg,
      maxArg,
    ].filter(Boolean).join(' ');

    exec(cmd, {
      cwd: path.join(__dirname, '..'),
      timeout: 15 * 60 * 1000, // 15 min timeout
      maxBuffer: 10 * 1024 * 1024,
      shell: true,
    }, (err, stdout, stderr) => {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);

      // Parse results from output
      const leadsMatch = stdout.match(/Total leads:\s+(\d+)/);
      const emailsMatch = stdout.match(/With email:\s+(\d+)/);
      const verifiedMatch = stdout.match(/Verified emails:\s+(\d+)/);
      const outputMatch = stdout.match(/Output:\s+(.+?)$/m);

      const leads = parseInt((leadsMatch || [])[1] || '0');
      const emails = parseInt((emailsMatch || [])[1] || '0');
      const verified = parseInt((verifiedMatch || [])[1] || '0');
      const outputFile = (outputMatch || [])[1] || '';

      if (err) {
        resolve({
          key,
          status: 'failed',
          error: err.message.slice(0, 200),
          leads: 0,
          emails: 0,
          elapsed,
        });
      } else {
        resolve({
          key,
          status: 'done',
          leads,
          emails,
          verified,
          outputFile: outputFile.trim(),
          elapsed,
        });
      }
    });
  });
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();
  const jobs = buildJobs();
  let progress = RESUME ? loadProgress() : {};

  // Filter out completed jobs
  const queue = [];
  let totalLeads = 0;
  let totalEmails = 0;

  for (const job of jobs) {
    const key = jobKey(job);
    if (RESUME && progress[key] && progress[key].status === 'done') {
      totalLeads += progress[key].leads || 0;
      totalEmails += progress[key].emails || 0;
    } else {
      queue.push(job);
    }
  }

  const doneCount = jobs.length - queue.length;

  console.log('');
  console.log('══════════════════════════════════════════════════════════════════════');
  console.log('  APOLLO BATCH — Scraping leads from Apollo.io B2B database');
  console.log(`  Preset: ${PRESET} | ${jobs.length} locations | ${queue.length} remaining`);
  console.log(`  Mode: ${process.env.APOLLO_COOKIE ? 'Cookie (emails in search)' : 'API Key (email scraping)'}`);
  if (doneCount > 0) console.log(`  Resumed: ${doneCount} already done (${totalLeads}L ${totalEmails}E)`);
  console.log('══════════════════════════════════════════════════════════════════════');
  console.log('');

  // Run jobs sequentially (Apollo rate limits are per-account, not per-request)
  // Running parallel would exhaust the daily limit faster
  for (let i = 0; i < queue.length; i++) {
    const job = queue[i];
    const key = jobKey(job);
    const remaining = queue.length - i;
    const eta = i > 0 ? Math.round(((Date.now() - startTime) / i) * remaining / 60000) : '?';

    console.log(`  [${doneCount + i + 1}/${jobs.length}] ${job.location} (ETA ~${eta}m)`);

    const result = await runJob(job);

    if (result.status === 'done') {
      totalLeads += result.leads;
      totalEmails += result.emails;
      console.log(`    ✓ ${result.leads}L ${result.emails}E ${result.verified || 0}V ${result.elapsed}s`);
    } else {
      console.log(`    ✗ FAILED: ${result.error}`);
    }

    progress[key] = { ...result, timestamp: new Date().toISOString() };
    saveProgress(progress);

    // Progress line
    if ((i + 1) % 5 === 0 || i === queue.length - 1) {
      const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
      console.log(`  ── ${doneCount + i + 1}/${jobs.length} | ${totalLeads}L ${totalEmails}E | ${elapsed}m elapsed ──`);
    }
  }

  // ─── Final summary ───────────────────────────────────────────

  const failedCount = Object.values(progress).filter(p => p.status === 'failed').length;
  const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);

  console.log('');
  console.log('══════════════════════════════════════════════════════════════════════');
  console.log(`  BATCH COMPLETE: ${totalLeads} leads | ${totalEmails} emails`);
  console.log(`  ${jobs.length} locations | ${failedCount} failed | ${elapsed}m`);
  console.log('══════════════════════════════════════════════════════════════════════');
  console.log('');

  // Merge all CSVs into one master file
  const outputDir = path.join(__dirname, '..', 'output');
  const apolloFiles = fs.readdirSync(outputDir).filter(f => f.startsWith('apollo_') && f.endsWith('.csv'));

  if (apolloFiles.length > 1) {
    console.log(`  Merging ${apolloFiles.length} CSV files...`);
    const allRows = [];
    const seenEmails = new Set();
    const seenNames = new Set();

    for (const file of apolloFiles) {
      const content = fs.readFileSync(path.join(outputDir, file), 'utf8');
      const lines = content.split('\n');
      const header = lines[0];

      for (let j = 1; j < lines.length; j++) {
        const line = lines[j].trim();
        if (!line) continue;

        // Simple dedup: by email or by name+firm
        // Parse first few columns (first_name, last_name, firm_name, ..., email)
        const cols = line.split(',');
        const email = (cols[9] || '').replace(/"/g, '').trim();
        const nameKey = `${(cols[0] || '').replace(/"/g, '')}|${(cols[1] || '').replace(/"/g, '')}|${(cols[2] || '').replace(/"/g, '')}`.toLowerCase();

        if (email && seenEmails.has(email)) continue;
        if (email) seenEmails.add(email);

        if (!email && seenNames.has(nameKey)) continue;
        seenNames.add(nameKey);

        allRows.push(line);
      }

      // Save header from first file
      if (allRows.length === 0 || !allRows._header) {
        allRows._header = header;
      }
    }

    const mergedPath = path.join(outputDir, `apollo_merged_${new Date().toISOString().slice(0, 10)}.csv`);
    fs.writeFileSync(mergedPath, [allRows._header || apolloFiles[0], ...allRows].join('\n'));
    console.log(`  Merged: ${allRows.length} unique leads → ${mergedPath}`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
