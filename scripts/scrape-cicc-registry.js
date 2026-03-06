#!/usr/bin/env node
/**
 * Scrape ALL registered immigration consultants from the CICC Public Registry.
 *
 * Source: register.college-ic.ca
 * Expected: ~12,000 RCICs across IDs 8000-30000
 *
 * Features:
 *   - Sequential profile enumeration (ID 8000 → 30000)
 *   - Cloudflare email decoding
 *   - Auto-saves progress CSV every 200 leads
 *   - Resume support (reads existing progress file to find last ID)
 *   - Rate limiting (400ms between requests)
 *
 * Usage:
 *   node scripts/scrape-cicc-registry.js
 *   node scripts/scrape-cicc-registry.js --start=10000 --end=15000
 *   node scripts/scrape-cicc-registry.js --resume
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// Parse CLI args
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const START_ID = parseInt(args.start) || 8000;
const END_ID = parseInt(args.end) || 30000;
const DELAY_MS = parseInt(args.delay) || 400;
const SEGMENT = args.segment || '';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PROGRESS_FILE = path.join(OUTPUT_DIR, SEGMENT ? `cicc-segment-${SEGMENT}.csv` : 'cicc-registry-progress.csv');

// Load the scraper module for its parser
const scraper = require('../scrapers/international/cicc-registry');

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        fetchUrl(res.headers.location).then(resolve).catch(reject);
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const columns = [
    'first_name', 'last_name', 'firm_name', 'title', 'email',
    'phone', 'website', 'domain', 'city', 'state', 'country',
    'college_id', 'bar_status', 'niche', 'source', 'profile_url',
  ];
  const header = columns.join(',');
  const rows = leads.map(lead =>
    columns.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function findResumeId() {
  if (!fs.existsSync(PROGRESS_FILE)) return null;
  try {
    const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
    const lines = content.trim().split('\n');
    if (lines.length < 2) return null;
    // Find profile_url column to extract last ID
    const headers = lines[0].split(',');
    const urlIdx = headers.indexOf('profile_url');
    if (urlIdx < 0) return null;
    const lastLine = lines[lines.length - 1];
    // Simple CSV parse for last line
    const match = lastLine.match(/ID=(\d+)/);
    if (match) return parseInt(match[1]) + 1;
  } catch { }
  return null;
}

async function main() {
  const startTime = Date.now();
  let startId = START_ID;

  // Resume support
  if (args.resume) {
    const resumeId = findResumeId();
    if (resumeId) {
      console.log(`  Resuming from ID ${resumeId} (found in progress file)`);
      startId = resumeId;
    } else {
      console.log(`  No progress file found, starting from ${startId}`);
    }
  }

  // Load existing progress if resuming
  let allLeads = [];
  if (args.resume && fs.existsSync(PROGRESS_FILE)) {
    try {
      const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
      const lines = content.trim().split('\n');
      const headers = lines[0].split(',');
      for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',');
        const lead = {};
        headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
        allLeads.push(lead);
      }
      console.log(`  Loaded ${allLeads.length} existing leads from progress file`);
    } catch { }
  }

  console.log('\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557');
  console.log('\u2551  MORTAR \u2014 CICC Registry Full Scrape                     \u2551');
  console.log('\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n');
  console.log(`  ID Range:    ${startId} \u2192 ${END_ID} (${END_ID - startId} to scan)`);
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Est. time:   ~${Math.round((END_ID - startId) * DELAY_MS / 1000 / 60)} minutes`);
  console.log(`  Output:      ${PROGRESS_FILE}`);
  console.log('');

  let found = 0;
  let scanned = 0;
  let errors = 0;
  let consecutiveErrors = 0;
  let consecutiveEmpty = 0;
  let withEmail = 0;
  let withPhone = 0;

  for (let id = startId; id <= END_ID; id++) {
    scanned++;

    try {
      const url = `https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/Licensee/Profile.aspx?ID=${id}&b9100e1006f6=2`;
      const html = await fetchUrl(url);

      if (!html || html.length < 500) {
        consecutiveEmpty++;
        // After 500 consecutive empty pages past ID 25000, we've likely hit the end
        if (consecutiveEmpty > 500 && id > 25000) {
          console.log(`\n  [STOP] 500 consecutive empty profiles at ID ${id}`);
          break;
        }
        continue;
      }

      consecutiveEmpty = 0;
      consecutiveErrors = 0;
      const lead = scraper._parseProfile(html, id);

      if (lead) {
        found++;
        allLeads.push(lead);
        if (lead.email) withEmail++;
        if (lead.phone) withPhone++;

        // Save progress every 100 leads
        if (found % 100 === 0) {
          writeCSV(PROGRESS_FILE, allLeads);
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
          const rate = (scanned / elapsed * 60).toFixed(0);
          const pctDone = ((id - startId) / (END_ID - startId) * 100).toFixed(1);
          const remaining = Math.round((END_ID - id) / (scanned / elapsed) / 60);
          console.log(`  [${pctDone}%] ID ${id} | ${found} active (${withEmail} email, ${withPhone} phone) | ${scanned} scanned | ${rate}/min | ~${remaining}min left`);
        }

        // Also log every 500 scanned
        if (scanned % 500 === 0) {
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
          const rate = (scanned / elapsed * 60).toFixed(0);
          const pctDone = ((id - startId) / (END_ID - startId) * 100).toFixed(1);
          const remaining = Math.round((END_ID - id) / (scanned / elapsed) / 60);
          console.log(`  [${pctDone}%] ID ${id} | ${found} found (${withEmail} email, ${withPhone} phone) | ${scanned} scanned | ${rate}/min | ~${remaining}min left`);
        }
      }
    } catch (err) {
      errors++;
      consecutiveErrors++;

      if (consecutiveErrors >= 10) {
        console.log(`  [PAUSE] ${consecutiveErrors} consecutive errors at ID ${id}: ${err.message}`);
        console.log('  Waiting 30 seconds...');
        await new Promise(r => setTimeout(r, 30000));
        consecutiveErrors = 0;
      }

      if (errors > 200) {
        console.log(`  [WARN] ${errors} total errors`);
      }
    }

    await new Promise(r => setTimeout(r, DELAY_MS));
  }

  // Final save
  if (allLeads.length > 0) {
    writeCSV(PROGRESS_FILE, allLeads);

    // Also save timestamped final file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const finalPath = path.join(OUTPUT_DIR, `cicc-registry_${timestamp}.csv`);
    writeCSV(finalPath, allLeads);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = allLeads.filter(l => l.email).length;
  const totalPhones = allLeads.filter(l => l.phone).length;
  const totalCompanies = allLeads.filter(l => l.firm_name).length;
  const eligible = allLeads.filter(l => l.bar_status && l.bar_status.includes('Eligible') && !l.bar_status.includes('NOT')).length;

  console.log('\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557');
  console.log('\u2551       CICC REGISTRY SCRAPE COMPLETE                      \u2551');
  console.log('\u2560\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2563');
  console.log(`\u2551  IDs scanned:   ${String(scanned).padEnd(40)}\u2551`);
  console.log(`\u2551  Total RCICs:   ${String(allLeads.length).padEnd(40)}\u2551`);
  console.log(`\u2551  Eligible:      ${String(eligible + ' (' + Math.round(eligible / allLeads.length * 100) + '%)').padEnd(40)}\u2551`);
  console.log(`\u2551  With email:    ${String(totalEmails + ' (' + Math.round(totalEmails / allLeads.length * 100) + '%)').padEnd(40)}\u2551`);
  console.log(`\u2551  With phone:    ${String(totalPhones + ' (' + Math.round(totalPhones / allLeads.length * 100) + '%)').padEnd(40)}\u2551`);
  console.log(`\u2551  With company:  ${String(totalCompanies + ' (' + Math.round(totalCompanies / allLeads.length * 100) + '%)').padEnd(40)}\u2551`);
  console.log(`\u2551  Errors:        ${String(errors).padEnd(40)}\u2551`);
  console.log(`\u2551  Time:          ${String(elapsed + 's (' + Math.round(elapsed / 60) + 'min)').padEnd(40)}\u2551`);
  console.log('\u2560\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2563');
  console.log(`\u2551  Output: ${PROGRESS_FILE}`);
  console.log('\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
