#!/usr/bin/env node
/**
 * Instant Email Enrichment Engine
 *
 * Two-phase architecture:
 *   Phase 1 (SCAN):  Verify 1 email per unknown domain → learn pattern → persist to DB
 *   Phase 2 (APPLY): Apply cached patterns to all leads → microseconds per lead
 *
 * Speed:
 *   Phase 1: ~0.6s per NEW domain (only runs once per domain, ever)
 *   Phase 2: ~272,000 leads/sec (hash lookup + string concat)
 *   1M leads with warm cache: ~4 seconds
 *
 * Usage:
 *   # Full pipeline: scan unknown domains + enrich all leads
 *   node scripts/instant-enrich.js --input output/leads.csv
 *
 *   # Scan-only: just build the pattern database (no CSV output)
 *   node scripts/instant-enrich.js --input output/leads.csv --scan-only
 *
 *   # Apply-only: skip scanning, use cached patterns only (instant)
 *   node scripts/instant-enrich.js --input output/leads.csv --apply-only
 *
 *   # Show pattern database stats
 *   node scripts/instant-enrich.js --stats
 *
 * Options:
 *   --input          Input CSV file
 *   --output         Output CSV file (default: adds -enriched suffix)
 *   --scan-only      Only scan unknown domains, don't produce output
 *   --apply-only     Only apply cached patterns (instant, no network)
 *   --concurrency    Parallel verification for Phase 1 (default: 10)
 *   --min-confidence Minimum pattern confidence to apply (default: 60)
 *   --stats          Show pattern database statistics
 */

const fs = require('fs');
const path = require('path');
const { DomainPatternDB } = require('../lib/domain-pattern-db');
const { EmailWaterfall } = require('../lib/email-waterfall');

// ─── CLI Args ──────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const INPUT = getArg('input');
const OUTPUT = getArg('output');
const SCAN_ONLY = hasFlag('scan-only');
const APPLY_ONLY = hasFlag('apply-only');
const CONCURRENCY = parseInt(getArg('concurrency') || '10') || 10;
const MIN_CONFIDENCE = parseInt(getArg('min-confidence') || '60') || 60;
const SHOW_STATS = hasFlag('stats');

// ─── CSV Parsing ───────────────────────────────────────────────

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

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const columns = Object.keys(leads[0]);
  if (!columns.includes('domain')) columns.push('domain');
  if (!columns.includes('email_source')) columns.push('email_source');
  if (!columns.includes('email_confidence')) columns.push('email_confidence');

  const header = columns.join(',');
  const rows = leads.map(lead => {
    return columns.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',');
  });

  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

// ─── Main ──────────────────────────────────────────────────────

async function main() {
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  const patternDB = new DomainPatternDB();

  // ─── Stats mode ────────────────────────────────────────────
  if (SHOW_STATS) {
    const stats = patternDB.getStats();
    console.log('\n╔═══════════════════════════════════════════════════════════╗');
    console.log('║       MORTAR — Domain Pattern Database Stats             ║');
    console.log('╚═══════════════════════════════════════════════════════════╝\n');
    console.log(`  Total domains:      ${stats.total_domains}`);
    console.log(`  High confidence:    ${stats.high_confidence} (≥80%)`);
    console.log(`  Medium confidence:  ${stats.medium_confidence || 0} (60-79%)`);
    console.log(`  Low confidence:     ${stats.low_confidence || 0} (<60%)`);
    console.log(`  Avg confidence:     ${stats.avg_confidence || 0}%`);
    console.log(`  Total verifications: ${stats.total_verifications || 0}`);
    console.log(`  Catch-all domains:  ${stats.catch_all_domains || 0}`);
    if (stats.byProvider && stats.byProvider.length) {
      console.log('\n  By provider:');
      for (const row of stats.byProvider) {
        console.log(`    ${(row.provider || 'unknown').padEnd(20)} ${row.count}`);
      }
    }
    if (stats.byPattern && stats.byPattern.length) {
      console.log('\n  By pattern:');
      for (const row of stats.byPattern) {
        console.log(`    ${row.pattern.padEnd(20)} ${row.count}`);
      }
    }
    console.log('');
    patternDB.close();
    return;
  }

  // ─── Need input file ───────────────────────────────────────
  if (!INPUT) {
    console.log('Usage: node scripts/instant-enrich.js --input <csv_file>');
    console.log('       node scripts/instant-enrich.js --stats');
    process.exit(1);
  }

  if (!fs.existsSync(INPUT)) {
    console.error(`File not found: ${INPUT}`);
    process.exit(1);
  }

  const leads = parseCSV(fs.readFileSync(INPUT, 'utf8'));

  // Extract domain from website
  for (const lead of leads) {
    if (!lead.domain && lead.website) {
      const m = lead.website.match(/https?:\/\/(?:www\.)?([^\/\?]+)/);
      if (m) lead.domain = m[1].toLowerCase();
    }
  }

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║       MORTAR — Instant Email Enrichment Engine           ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Input:              ${path.basename(INPUT)} (${leads.length} leads)`);
  console.log(`  Pattern DB:         ${patternDB.size} domains cached`);

  const alreadyHaveEmail = leads.filter(l => l.email).length;
  const enrichable = leads.filter(l => !l.email && l.first_name && l.last_name && l.domain && l.last_name.length > 2);
  console.log(`  Already have email: ${alreadyHaveEmail}`);
  console.log(`  Enrichable:         ${enrichable.length}`);

  // ─── Phase 2: Instant Apply (cached patterns) ──────────────
  const applyStart = Date.now();
  const { enriched, cold } = patternDB.enrichBulk(enrichable);
  const applyMs = Date.now() - applyStart;

  // Apply enriched emails back to leads
  const enrichedMap = new Map();
  for (const e of enriched) {
    const key = `${e.first_name}|${e.last_name}|${e.domain}`;
    enrichedMap.set(key, e);
  }
  for (const lead of leads) {
    if (lead.email) continue;
    const key = `${lead.first_name}|${lead.last_name}|${lead.domain}`;
    const found = enrichedMap.get(key);
    if (found) {
      lead.email = found.email;
      lead.email_source = found.source;
      lead.email_confidence = found.status;
    }
  }

  const instantRate = enriched.length > 0 ? Math.round(enriched.length / (applyMs / 1000)) : 0;
  console.log(`\n  ── Phase 2: Instant Apply ────────────────────────────`);
  console.log(`  Cached patterns applied: ${enriched.length} emails`);
  console.log(`  Time: ${applyMs}ms (${instantRate.toLocaleString()} leads/sec)`);
  console.log(`  Remaining cold leads:    ${cold.length}`);

  if (APPLY_ONLY) {
    // Skip Phase 1, just output what we have
    const totalEmails = leads.filter(l => l.email).length;
    console.log(`\n  Total with email:    ${totalEmails} / ${leads.length} (${Math.round(totalEmails / leads.length * 100)}%)`);

    if (!SCAN_ONLY) {
      const outputPath = OUTPUT || INPUT.replace(/(\.[^.]+)$/, '-enriched$1');
      writeCSV(outputPath, leads);
      console.log(`  Output:              ${outputPath}\n`);
    }
    patternDB.close();
    return;
  }

  // ─── Phase 1: Scan Unknown Domains ─────────────────────────
  if (cold.length > 0) {
    console.log(`\n  ── Phase 1: Scanning ${cold.length} cold leads ─────────────────`);

    // Group by domain, pick 1 representative lead per unknown domain
    const byDomain = new Map();
    for (const lead of cold) {
      if (!byDomain.has(lead.domain)) {
        byDomain.set(lead.domain, []);
      }
      byDomain.get(lead.domain).push(lead);
    }

    const unknownDomains = [...byDomain.keys()].filter(d => !patternDB.has(d, MIN_CONFIDENCE));
    console.log(`  Unique cold domains: ${byDomain.size}`);
    console.log(`  Unknown domains:     ${unknownDomains.length} (need scanning)`);
    console.log(`  Concurrency:         ${CONCURRENCY}\n`);

    if (unknownDomains.length > 0) {
      // Pick 1 test lead per domain (prefer longer last names for better pattern detection)
      const testLeads = unknownDomains.map(domain => {
        const candidates = byDomain.get(domain);
        candidates.sort((a, b) => (b.last_name || '').length - (a.last_name || '').length);
        return candidates[0];
      });

      const waterfall = new EmailWaterfall();
      patternDB.seedWaterfall(waterfall);

      const scanStart = Date.now();
      let lastLog = 0;

      const results = await waterfall.findEmailsBatch(testLeads, CONCURRENCY, (found, total) => {
        const now = Date.now();
        if (now - lastLog > 3000 || total === testLeads.length) {
          const elapsed = ((now - scanStart) / 1000).toFixed(1);
          const rate = elapsed > 0 ? Math.round(total / (elapsed / 60)) : 0;
          console.log(`  [${total}/${testLeads.length}] ${found} patterns found | ${rate}/min | ${elapsed}s`);
          lastLog = now;
        }
      });

      const scanElapsed = ((Date.now() - scanStart) / 1000).toFixed(1);

      // Import learned patterns into persistent DB
      const imported = patternDB.importFromWaterfall(waterfall);
      console.log(`\n  Scan complete: ${scanElapsed}s`);
      console.log(`  New patterns learned: ${imported}`);

      // Now apply newly learned patterns to ALL cold leads
      const reapplyStart = Date.now();
      let newlyEnriched = 0;

      for (const lead of leads) {
        if (lead.email) continue;
        if (!lead.first_name || !lead.last_name || !lead.domain || lead.last_name.length <= 2) continue;

        const result = patternDB.applyWithMeta(lead.domain, lead.first_name, lead.last_name);
        if (result) {
          lead.email = result.email;
          lead.email_source = result.source;
          lead.email_confidence = result.status;
          newlyEnriched++;
        }
      }

      // Also apply results directly for test leads (they were verified, not just pattern-matched)
      for (let i = 0; i < testLeads.length; i++) {
        const r = results[i];
        if (!r) continue;
        const lead = testLeads[i];
        // Find original lead in the leads array
        for (const l of leads) {
          if (l.first_name === lead.first_name && l.last_name === lead.last_name && l.domain === lead.domain && !l.email) {
            l.email = r.email;
            l.email_source = r.source;
            l.email_confidence = r.status;
            break;
          }
        }
      }

      const reapplyMs = Date.now() - reapplyStart;
      console.log(`  Applied to remaining leads: ${newlyEnriched} new emails (${reapplyMs}ms)`);

      // Print waterfall stats
      const wfStats = waterfall.getStats();
      if (Object.keys(wfStats.bySource).length > 0) {
        console.log('\n  Scan verification methods:');
        for (const [src, count] of Object.entries(wfStats.bySource).sort((a, b) => b[1] - a[1])) {
          console.log(`    ${src.padEnd(28)} ${count}`);
        }
      }
    }
  }

  if (SCAN_ONLY) {
    console.log(`\n  Pattern DB now has ${patternDB.size} domains.\n`);
    patternDB.close();
    return;
  }

  // ─── Output ────────────────────────────────────────────────
  const outputPath = OUTPUT || INPUT.replace(/(\.[^.]+)$/, '-enriched$1');
  writeCSV(outputPath, leads);

  const totalEmails = leads.filter(l => l.email).length;
  const totalTime = ((Date.now() - applyStart) / 1000).toFixed(1);

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║       ENRICHMENT COMPLETE                                ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:         ${String(leads.length).padStart(6)}                             ║`);
  console.log(`║  With email:          ${String(totalEmails).padStart(6)} (${Math.round(totalEmails / leads.length * 100)}%)                            ║`);
  console.log(`║  New emails added:    ${String(totalEmails - alreadyHaveEmail).padStart(6)}                             ║`);
  console.log(`║  Pattern DB domains:  ${String(patternDB.size).padStart(6)}                             ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Time:                ${totalTime}s                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝\n');

  patternDB.close();
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
