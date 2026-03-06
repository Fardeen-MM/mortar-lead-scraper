#!/usr/bin/env node
/**
 * Universal Industry Scraper — "Give it an industry, get leads"
 *
 * One command to scrape any niche across any US location:
 *   node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL"
 *   node scripts/industry-scrape.js --niche "plumbers" --location "Texas" --zip-sweep
 *   node scripts/industry-scrape.js --niche "lawyers" --zip-sweep --max 5000
 *
 * Pipeline:
 *   1. DISCOVER — Google Maps scrape (city or zip-code-by-zip-code)
 *   2. EXTRACT  — Website crawl for people, emails, phones
 *   3. ENRICH   — Email waterfall (MS GetCredentialType, SMTP, Spotify, Gravatar)
 *   4. SCORE    — Decision maker scoring + dedup
 *   5. EXPORT   — CSV with all contact info
 *
 * Options:
 *   --niche        Business type to search (required)
 *   --location     City, State or just State (default: all US)
 *   --zip-sweep    Sweep all zip codes in the state/country
 *   --max          Max leads to collect (default: unlimited)
 *   --concurrency  Parallel scrape workers (default: 3)
 *   --output       Output CSV path
 *   --enrich       Run email waterfall enrichment (default: true)
 *   --no-enrich    Skip email enrichment
 *   --scan-db      Pre-scan pattern DB before enriching
 */

const fs = require('fs');
const path = require('path');

// ─── CLI Args ──────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const NICHE = getArg('niche');
const LOCATION = getArg('location') || '';
const ZIP_SWEEP = hasFlag('zip-sweep');
const MAX_LEADS = parseInt(getArg('max') || '0') || 0;
const CONCURRENCY = parseInt(getArg('concurrency') || '3') || 3;
const OUTPUT = getArg('output');
const DO_ENRICH = !hasFlag('no-enrich');
const SCAN_DB = hasFlag('scan-db');

if (!NICHE) {
  console.log(`
  Usage: node scripts/industry-scrape.js --niche <type> [options]

  Examples:
    node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL"
    node scripts/industry-scrape.js --niche "plumbers" --location "Texas" --zip-sweep
    node scripts/industry-scrape.js --niche "personal injury lawyers" --zip-sweep --max 5000
    node scripts/industry-scrape.js --niche "accountants" --location "New York, NY"

  Options:
    --niche        Business type (required)
    --location     City, State (default: major US cities)
    --zip-sweep    Cover all zip codes in state/country
    --max          Max leads to collect
    --concurrency  Parallel workers (default: 3)
    --output       Output CSV path
    --no-enrich    Skip email enrichment
  `);
  process.exit(1);
}

// ─── CSV Helpers ───────────────────────────────────────────────

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const columns = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'email_confidence',
    'email_source', 'phone', 'website', 'domain', 'city', 'state', 'country',
    'linkedin_url', 'niche', 'source', 'dm_score',
  ];
  for (const lead of leads.slice(0, 10)) {
    for (const key of Object.keys(lead)) {
      if (!columns.includes(key)) columns.push(key);
    }
  }

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

// ─── Main Pipeline ─────────────────────────────────────────────

async function main() {
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  const startTime = Date.now();
  const locationParts = LOCATION.split(',').map(s => s.trim());
  const city = locationParts[0] || '';
  const state = locationParts[1] || locationParts[0] || '';

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║       MORTAR — Universal Industry Scraper                ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Niche:       ${NICHE}`);
  console.log(`  Location:    ${LOCATION || 'Major US cities'}`);
  if (ZIP_SWEEP) {
    const zipcodes = require('../lib/us-zipcodes');
    const pts = state ? zipcodes.getSearchPoints({ state: state.length === 2 ? state.toUpperCase() : undefined }) : zipcodes.getSearchPoints();
    console.log(`  Mode:        ZIP code sweep (${pts.length} search points)`);
  } else {
    console.log(`  Mode:        City search`);
  }
  console.log(`  Max leads:   ${MAX_LEADS || 'unlimited'}`);
  console.log(`  Enrichment:  ${DO_ENRICH ? 'ON (email waterfall)' : 'OFF'}`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log('');

  // ─── Step 1: DISCOVER — Google Maps Scrape ─────────────────
  console.log('  ── Step 1: DISCOVER (Google Maps) ─────────────────────');

  let leads = [];
  const seen = new Set();

  // Helper: collect leads from a single Google Maps search run
  async function collectFromSearch(gmaps, options, label) {
    let count = 0;
    for await (const result of gmaps.search(NICHE, options)) {
      if (result._cityProgress || result._captcha) continue;

      const key = `${(result.firm_name || '').toLowerCase()}|${(result.city || '').toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);

      if (result.website && !result.domain) {
        const m = result.website.match(/https?:\/\/(?:www\.)?([^\/\?]+)/);
        if (m) result.domain = m[1].toLowerCase();
      }

      result.niche = NICHE;
      leads.push(result);
      count++;

      if (leads.length % 50 === 0) {
        console.log(`    ${leads.length} businesses found...`);
      }

      if (MAX_LEADS > 0 && leads.length >= MAX_LEADS) return true; // signal done
    }
    if (label) console.log(`    ${label}: +${count} businesses`);
    return false;
  }

  try {
    const gmaps = require('../scrapers/directories/google-maps');

    if (ZIP_SWEEP) {
      // ─── ZIP Code Sweep Mode ───────────────────────────────
      const zipcodes = require('../lib/us-zipcodes');

      // Resolve state code from location (e.g. "Texas" → "TX", "FL" → "FL")
      const { US_STATES } = require('../lib/state-metadata');
      let stateFilter = null;
      if (state) {
        // Check if it's already a 2-letter code
        if (US_STATES[state.toUpperCase()]) {
          stateFilter = state.toUpperCase();
        } else {
          // Try matching by name
          const entry = Object.entries(US_STATES).find(([, name]) =>
            name.toLowerCase() === state.toLowerCase()
          );
          if (entry) stateFilter = entry[0];
        }
      }

      const searchPoints = stateFilter
        ? zipcodes.getSearchPoints({ state: stateFilter })
        : zipcodes.getSearchPoints();

      console.log(`    ZIP sweep: ${searchPoints.length} search points` +
        (stateFilter ? ` (${stateFilter})` : ' (nationwide)'));

      for (let i = 0; i < searchPoints.length; i++) {
        const pt = searchPoints[i];
        const options = {
          niche: NICHE,
          lat: pt.lat,
          lng: pt.lng,
          radius: 25, // 25km radius per prefix
          maxPages: 3, // limit pages per point to keep speed reasonable
          maxCities: 1,
          gridCells: 4, // 4 grid cells per ZIP prefix center
          personExtract: true,
        };

        const label = `[${i + 1}/${searchPoints.length}] ${pt.city}, ${pt.state} (${pt.prefix}xx)`;
        if (i % 10 === 0) console.log(`    ${label}`);

        const done = await collectFromSearch(gmaps, options, null);
        if (done) {
          console.log(`    Reached max leads (${MAX_LEADS})`);
          break;
        }
      }

      console.log(`    ZIP sweep complete: ${leads.length} businesses\n`);
    } else {
      // ─── Standard City Mode ────────────────────────────────
      const options = {
        niche: NICHE,
        maxPages: MAX_LEADS > 0 ? Math.ceil(MAX_LEADS / 20) : 999,
        personExtract: true,
      };

      if (city) {
        options.city = city;
        options.maxCities = 1;
      }

      await collectFromSearch(gmaps, options, null);
      console.log(`    Total discovered: ${leads.length} businesses\n`);
    }
  } catch (err) {
    console.log(`    Google Maps scrape error: ${err.message}`);
    console.log('    Continuing with any leads found...\n');
  }

  if (leads.length === 0) {
    console.log('  No leads found. Try a different niche or location.\n');
    return;
  }

  // ─── Step 2: Stats Before Enrichment ───────────────────────
  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;
  const withWebsite = leads.filter(l => l.website).length;
  const withName = leads.filter(l => l.first_name && l.last_name && l.last_name.length > 2).length;

  console.log('  ── Pre-enrichment Stats ───────────────────────────────');
  console.log(`    Total leads:   ${leads.length}`);
  console.log(`    With name:     ${withName} (${Math.round(withName / leads.length * 100)}%)`);
  console.log(`    With email:    ${withEmail} (${Math.round(withEmail / leads.length * 100)}%)`);
  console.log(`    With phone:    ${withPhone} (${Math.round(withPhone / leads.length * 100)}%)`);
  console.log(`    With website:  ${withWebsite} (${Math.round(withWebsite / leads.length * 100)}%)`);
  console.log('');

  // ─── Step 3: ENRICH — Email Waterfall ──────────────────────
  if (DO_ENRICH) {
    console.log('  ── Step 3: ENRICH (Email Waterfall) ──────────────────');

    const { DomainPatternDB } = require('../lib/domain-pattern-db');
    const { EmailWaterfall } = require('../lib/email-waterfall');

    const patternDB = new DomainPatternDB();
    console.log(`    Pattern DB: ${patternDB.size} domains cached`);

    // Phase 2: Instant apply from cache
    const enrichable = leads.filter(l =>
      !l.email && l.first_name && l.last_name && l.domain && l.last_name.length > 2
    );

    const applyStart = Date.now();
    const { enriched, cold } = patternDB.enrichBulk(enrichable);
    const applyMs = Date.now() - applyStart;

    // Apply cached results
    const enrichedMap = new Map();
    for (const e of enriched) {
      enrichedMap.set(`${e.first_name}|${e.last_name}|${e.domain}`, e);
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

    console.log(`    Instant apply: ${enriched.length} emails (${applyMs}ms)`);

    // Scan cold leads with live waterfall
    if (cold.length > 0) {
      const waterfall = new EmailWaterfall();
      patternDB.seedWaterfall(waterfall);

      console.log(`    Verifying ${cold.length} cold leads (concurrency: ${CONCURRENCY})...`);
      const scanStart = Date.now();

      const results = await waterfall.findEmailsBatch(cold, CONCURRENCY, (found, total) => {
        if (total % 25 === 0 || total === cold.length) {
          const elapsed = ((Date.now() - scanStart) / 1000).toFixed(1);
          const pct = Math.round(found / Math.max(total, 1) * 100);
          console.log(`    [${total}/${cold.length}] ${found} found (${pct}%) | ${elapsed}s`);
        }
      });

      for (let i = 0; i < cold.length; i++) {
        const r = results[i];
        if (!r) continue;
        const lead = cold[i];
        for (const l of leads) {
          if (l.first_name === lead.first_name && l.last_name === lead.last_name && l.domain === lead.domain && !l.email) {
            l.email = r.email;
            l.email_source = r.source;
            l.email_confidence = r.status;
            break;
          }
        }
      }

      patternDB.importFromWaterfall(waterfall);
    }

    patternDB.close();

    const newEmailCount = leads.filter(l => l.email).length;
    console.log(`    Email coverage: ${newEmailCount} / ${leads.length} (${Math.round(newEmailCount / leads.length * 100)}%)\n`);
  }

  // ─── Step 4: SCORE — Decision Maker Scoring ────────────────
  console.log('  ── Step 4: SCORE ──────────────────────────────────────');

  for (const lead of leads) {
    let score = 0;
    const title = (lead.title || '').toLowerCase();

    if (/owner|founder|ceo|president|principal|managing/i.test(title)) score += 50;
    else if (/partner|director|vp|vice president/i.test(title)) score += 40;
    else if (/manager|supervisor|head of|lead/i.test(title)) score += 30;
    else if (/senior|sr\.|attorney|lawyer|dentist|doctor/i.test(title)) score += 20;
    else if (title) score += 10;

    if (lead.email) score += 20;
    if (lead.phone) score += 10;
    if (lead.linkedin_url) score += 5;
    if (lead.website) score += 5;

    lead.dm_score = score;
  }

  leads.sort((a, b) => (b.dm_score || 0) - (a.dm_score || 0));

  const dmCount = leads.filter(l => (l.dm_score || 0) >= 50).length;
  console.log(`    Decision makers (score ≥50): ${dmCount}`);
  console.log(`    Contactable (email+phone):   ${leads.filter(l => l.email && l.phone).length}`);
  console.log('');

  // ─── Step 5: EXPORT ────────────────────────────────────────
  const nicheSlug = NICHE.replace(/[^a-z0-9]/gi, '-').toLowerCase();
  const locationSlug = LOCATION ? LOCATION.replace(/[^a-z0-9]/gi, '-').toLowerCase() : 'us';
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outputPath = OUTPUT || path.join('output', `${nicheSlug}_${locationSlug}_${timestamp}.csv`);

  writeCSV(outputPath, leads);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = leads.filter(l => l.email).length;
  const totalPhones = leads.filter(l => l.phone).length;

  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       SCRAPE COMPLETE                                    ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Niche:          ${NICHE.padEnd(40)}║`);
  console.log(`║  Location:       ${(LOCATION || 'US').padEnd(40)}║`);
  console.log(`║  Total leads:    ${String(leads.length).padEnd(40)}║`);
  console.log(`║  With email:     ${String(totalEmails + ' (' + Math.round(totalEmails / leads.length * 100) + '%)').padEnd(40)}║`);
  console.log(`║  With phone:     ${String(totalPhones + ' (' + Math.round(totalPhones / leads.length * 100) + '%)').padEnd(40)}║`);
  console.log(`║  Decision makers: ${String(dmCount).padEnd(39)}║`);
  console.log(`║  Time:           ${String(elapsed + 's').padEnd(40)}║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
