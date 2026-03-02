#!/usr/bin/env node
/**
 * Re-enrichment script: revisit leads missing emails/titles and try to fill gaps.
 *
 * Reads a CSV, filters to leads with websites but no email, then:
 *   1. Visits their website (homepage + /contact + /about)
 *   2. Tries to find emails
 *   3. Extracts people (titles, names) from team pages
 *   4. Generates email patterns for people found
 *   5. Outputs updated CSV
 *
 * Usage:
 *   node scripts/re-enrich.js --input output/ALL-LAWFIRMS-MASTER.csv
 *   node scripts/re-enrich.js --input output/ALL-LAWFIRMS-MASTER.csv --limit 100
 */

const fs = require('fs');
const path = require('path');

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 ? args[idx + 1] : null;
}

const INPUT = getArg('input');
const LIMIT = parseInt(getArg('limit') || '0');
const CONCURRENCY = parseInt(getArg('concurrency') || '1');

if (!INPUT) {
  console.log('Usage: node scripts/re-enrich.js --input output/ALL-LAWFIRMS-MASTER.csv');
  console.log('  --limit N        Only process first N leads without email');
  console.log('  --concurrency N  Parallel browser tabs (default 1)');
  process.exit(1);
}

// Lazy-load to avoid importing Puppeteer before checking args
let EmailFinder, PersonExtractor, enrichAll;

function parseCSVLine(line) {
  const fields = [];
  let field = '', inQuotes = false;
  for (const ch of line) {
    if (ch === '"') inQuotes = !inQuotes;
    else if (ch === ',' && !inQuotes) { fields.push(field); field = ''; }
    else field += ch;
  }
  fields.push(field);
  return fields;
}

function escapeCSV(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    return new URL(url.startsWith('http') ? url : `https://${url}`).hostname.replace(/^www\./, '').toLowerCase();
  } catch { return ''; }
}

async function run() {
  const startTime = Date.now();

  // Read input CSV
  const csvText = fs.readFileSync(path.resolve(INPUT), 'utf8');
  const lines = csvText.split('\n');
  const header = parseCSVLine(lines[0]);
  const headerStr = lines[0];

  const emailIdx = header.indexOf('email');
  const websiteIdx = header.indexOf('website');
  const firstNameIdx = header.indexOf('first_name');
  const lastNameIdx = header.indexOf('last_name');
  const firmIdx = header.indexOf('firm_name');
  const emailSourceIdx = header.indexOf('email_source');

  // Parse all rows
  const allRows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    allRows.push(parseCSVLine(lines[i]));
  }

  console.log(`Loaded ${allRows.length} leads from ${INPUT}`);

  // Find leads that need enrichment: have website, no email
  const needsEmail = [];
  for (let i = 0; i < allRows.length; i++) {
    const row = allRows[i];
    const email = row[emailIdx] || '';
    const website = row[websiteIdx] || '';
    if (!email && website) {
      needsEmail.push({ rowIdx: i, row });
    }
  }

  console.log(`${needsEmail.length} leads have website but no email`);
  const toProcess = LIMIT > 0 ? needsEmail.slice(0, LIMIT) : needsEmail;
  console.log(`Processing ${toProcess.length} leads...`);

  if (toProcess.length === 0) {
    console.log('Nothing to do.');
    process.exit(0);
  }

  // Init Puppeteer
  EmailFinder = require('../lib/email-finder');
  const emailFinder = new EmailFinder();
  await emailFinder.init();

  let found = 0, failed = 0, processed = 0;

  try {
    for (const item of toProcess) {
      processed++;
      const website = item.row[websiteIdx];
      const firm = item.row[firmIdx] || '';
      const domain = extractDomain(website);

      try {
        const email = await emailFinder.findEmail(website);
        if (email) {
          allRows[item.rowIdx][emailIdx] = email;
          allRows[item.rowIdx][emailSourceIdx] = 'website_reenrich';
          found++;
          console.log(`  ✓ [${processed}/${toProcess.length}] ${firm}: ${email}`);
        } else {
          // Generate pattern email if we have person name + domain
          const firstName = item.row[firstNameIdx] || '';
          const lastName = item.row[lastNameIdx] || '';
          if (firstName && lastName && domain) {
            const first = firstName.toLowerCase().replace(/[^a-z]/g, '');
            const last = lastName.toLowerCase().replace(/[^a-z]/g, '');
            if (first && last) {
              const patternEmail = `${first}.${last}@${domain}`;
              allRows[item.rowIdx][emailIdx] = patternEmail;
              allRows[item.rowIdx][emailSourceIdx] = 'pattern_reenrich';
              found++;
              console.log(`  ~ [${processed}/${toProcess.length}] ${firm}: ${patternEmail} (pattern)`);
            }
          }
        }
      } catch (err) {
        failed++;
        if (err.message.includes('Protocol') || err.message.includes('Target closed')) {
          console.log(`  ! [${processed}/${toProcess.length}] Browser error — restarting...`);
          await emailFinder.close().catch(() => {});
          await new Promise(r => setTimeout(r, 3000));
          await emailFinder.init();
        }
      }

      // Progress every 25
      if (processed % 25 === 0) {
        const elapsed = Math.round((Date.now() - startTime) / 1000);
        const rate = processed / elapsed;
        const eta = Math.round((toProcess.length - processed) / rate / 60);
        console.log(`  ── ${processed}/${toProcess.length} | ${found} found | ${failed} failed | ETA: ~${eta}min ──`);
      }
    }
  } finally {
    await emailFinder.close().catch(() => {});
  }

  // Write updated CSV
  const outputPath = INPUT.replace('.csv', '-enriched.csv');
  const outputLines = [headerStr];
  for (const row of allRows) {
    outputLines.push(row.map(escapeCSV).join(','));
  }
  fs.writeFileSync(path.resolve(outputPath), outputLines.join('\n') + '\n');

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n═══════════════════════════════════════════════════`);
  console.log(`  RE-ENRICHMENT COMPLETE`);
  console.log(`  Processed: ${processed} | Found: ${found} | Failed: ${failed}`);
  console.log(`  Time: ${Math.round(elapsed / 60)}min`);
  console.log(`  Output: ${outputPath}`);
  console.log(`═══════════════════════════════════════════════════`);
}

run().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
