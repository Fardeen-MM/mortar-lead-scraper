#!/usr/bin/env node
/**
 * Email Waterfall Enricher — Find verified emails for leads
 *
 * Clay-style waterfall: cascades through multiple email providers,
 * stops at the first verified email found per lead.
 * Only uses paid credits when email is actually found.
 *
 * Providers (in order, free → cheap → paid):
 *   1. Hunter.io    — 50 free/month
 *   2. Prospeo      — 75 free/month
 *   3. Snov.io      — 50 free/month
 *   4. Icypeas      — cheapest at scale ($0.005/email)
 *   5. LeadMagic    — best accuracy ($0.008/email)
 *   6. SMTP Pattern — free, pattern + SMTP verify (last resort)
 *
 * Setup — add API keys to .env:
 *   HUNTER_API_KEY=xxx
 *   PROSPEO_API_KEY=xxx
 *   SNOV_CLIENT_ID=xxx
 *   SNOV_CLIENT_SECRET=xxx
 *   ICYPEAS_API_KEY=xxx
 *   ICYPEAS_API_SECRET=xxx
 *   LEADMAGIC_API_KEY=xxx
 *
 * Usage:
 *   node scripts/enrich-emails.js --input output/apollo-turbo_attorney_florida.csv
 *   node scripts/enrich-emails.js --input output/leads.csv --concurrency 10
 *   node scripts/enrich-emails.js --input output/leads.csv --providers hunter,smtp_pattern
 *
 * Options:
 *   --input        Input CSV file (required)
 *   --output       Output CSV file (default: adds -enriched suffix)
 *   --concurrency  Parallel requests (default: 5)
 *   --providers    Comma-separated provider names to use (default: all configured)
 *   --max          Max leads to process
 *   --skip-has-email  Skip leads that already have email
 */

const fs = require('fs');
const path = require('path');
const { EmailWaterfall } = require('../lib/email-waterfall');

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const INPUT = getArg('input');
const OUTPUT = getArg('output');
const CONCURRENCY = parseInt(getArg('concurrency') || '5') || 5;
const MAX = parseInt(getArg('max') || '0') || 0;
const SKIP_HAS_EMAIL = hasFlag('skip-has-email');
const ONLY_PROVIDERS = getArg('providers') ? getArg('providers').split(',').map(s => s.trim()) : null;

if (!INPUT) {
  console.log('Usage: node scripts/enrich-emails.js --input <csv_file>');
  console.log('');
  console.log('Add API keys to .env:');
  console.log('  HUNTER_API_KEY=xxx       (50 free/month)');
  console.log('  PROSPEO_API_KEY=xxx      (75 free/month)');
  console.log('  SNOV_CLIENT_ID=xxx       (50 free/month)');
  console.log('  SNOV_CLIENT_SECRET=xxx');
  console.log('  ICYPEAS_API_KEY=xxx      ($0.005/email)');
  console.log('  ICYPEAS_API_SECRET=xxx');
  console.log('  LEADMAGIC_API_KEY=xxx    ($0.008/email, best accuracy)');
  process.exit(1);
}

// ─── Parse CSV ──────────────────────────────────────────────────────

function parseCSV(content) {
  const lines = content.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = parseCSVLine(lines[0]);
  return lines.slice(1).map(line => {
    const vals = parseCSVLine(line);
    const row = {};
    headers.forEach((h, i) => { row[h] = vals[i] || ''; });
    return row;
  });
}

function parseCSVLine(line) {
  const vals = [];
  let current = '';
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"') { inQuotes = !inQuotes; continue; }
    if (ch === ',' && !inQuotes) { vals.push(current.trim()); current = ''; continue; }
    current += ch;
  }
  vals.push(current.trim());
  return vals;
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  // Load env
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  // Load CSV
  if (!fs.existsSync(INPUT)) {
    console.error(`File not found: ${INPUT}`);
    process.exit(1);
  }
  let leads = parseCSV(fs.readFileSync(INPUT, 'utf8'));
  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       MORTAR — Email Waterfall Enricher                  ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Input:        ${path.basename(INPUT)} (${leads.length} leads)`);

  // Extract domain from website if no domain column
  for (const lead of leads) {
    if (!lead.domain && lead.website) {
      const m = lead.website.match(/https?:\/\/(?:www\.)?([^\/\?]+)/);
      if (m) lead.domain = m[1].toLowerCase();
    }
  }

  // Filter to leads that need enrichment
  const alreadyHaveEmail = leads.filter(l => l.email).length;
  let toEnrich = leads.filter(l => !l.email);
  // Must have first_name, last_name, domain, last_name > 2 chars
  toEnrich = toEnrich.filter(l => l.first_name && l.last_name && l.domain && l.last_name.length > 2);
  if (MAX > 0) toEnrich = toEnrich.slice(0, MAX);

  console.log(`  Already have: ${alreadyHaveEmail} emails`);
  console.log(`  To enrich:    ${toEnrich.length} leads (${leads.length - alreadyHaveEmail - toEnrich.length} skipped — missing name or domain)`);

  // Setup waterfall
  const config = {
    hunter_api_key: process.env.HUNTER_API_KEY,
    leadmagic_api_key: process.env.LEADMAGIC_API_KEY,
  };

  const waterfall = new EmailWaterfall(config);

  // Show methods
  console.log(`  Methods:      MS GetCredentialType → MS Autodiscover → SMTP → Spotify → Gravatar → Pattern`);
  console.log(`  Concurrency:  ${CONCURRENCY}`);
  console.log('');

  if (toEnrich.length === 0) {
    console.log('  No leads to enrich.');
    return;
  }

  // Run waterfall
  console.log('  Starting enrichment...');
  const startTime = Date.now();
  let lastLog = 0;

  const results = await waterfall.findEmailsBatch(toEnrich, CONCURRENCY, (found, total, lead) => {
    const now = Date.now();
    if (now - lastLog > 3000 || total === toEnrich.length) {
      const elapsed = ((now - startTime) / 1000).toFixed(1);
      const rate = elapsed > 0 ? Math.round(total / (elapsed / 60)) : 0;
      const pct = Math.round(found / Math.max(total, 1) * 100);
      console.log(`  [${total}/${toEnrich.length}] ${found} emails found (${pct}%) | ${rate}/min | ${elapsed}s`);
      lastLog = now;
    }
  });

  // Apply results back to leads
  for (let i = 0; i < toEnrich.length; i++) {
    const r = results[i];
    if (r && r.email) {
      toEnrich[i].email = r.email;
      toEnrich[i].email_source = r.source;
      toEnrich[i].email_confidence = r.status === 'verified' ? 'verified' :
                                      r.status === 'catchall' ? 'catchall' : 'likely';
    }
  }

  // Export
  const outputPath = OUTPUT || INPUT.replace(/(\.[^.]+)$/, '-enriched$1');
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  // Detect columns from input
  const columns = Object.keys(leads[0] || {});
  // Ensure enrichment columns exist
  if (!columns.includes('domain')) columns.push('domain');
  if (!columns.includes('email_source')) columns.push('email_source');
  if (!columns.includes('email_confidence')) columns.push('email_confidence');

  const csvHeader = columns.join(',');
  const csvRows = leads.map(lead => {
    return columns.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',');
  });
  // Sort: leads with email first
  csvRows.sort((a, b) => {
    const emailIdx = columns.indexOf('email');
    const aEmail = a.split(',')[emailIdx] || '';
    const bEmail = b.split(',')[emailIdx] || '';
    return (bEmail ? 1 : 0) - (aEmail ? 1 : 0);
  });
  fs.writeFileSync(outputPath, [csvHeader, ...csvRows].join('\n'));

  // Summary
  const stats = waterfall.getStats();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = leads.filter(l => l.email).length;

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       EMAIL WATERFALL COMPLETE                           ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:         ${String(leads.length).padStart(6)}                             ║`);
  console.log(`║  With email (total):  ${String(totalEmails).padStart(6)} (${Math.round(totalEmails/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log(`║  New emails found:    ${String(stats.found).padStart(6)}                             ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  if (Object.keys(stats.bySource).length > 0) {
    console.log('║  By Source:                                               ║');
    for (const [src, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
      console.log(`║    ${src.padEnd(24)} ${String(count).padStart(6)}                       ║`);
    }
    console.log('╠═══════════════════════════════════════════════════════════╣');
  }
  console.log(`║  Time:                ${elapsed}s                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  process.exit(1);
});
