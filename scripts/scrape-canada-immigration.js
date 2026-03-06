#!/usr/bin/env node
/**
 * Scrape immigration consultants across all major Canadian cities.
 * Runs sequentially (one city at a time) to avoid Puppeteer crashes.
 */

const fs = require('fs');
const path = require('path');

const CITIES = [
  { city: 'Toronto', state: 'ON' },
  { city: 'Vancouver', state: 'BC' },
  { city: 'Montreal', state: 'QC' },
  { city: 'Calgary', state: 'AB' },
  { city: 'Edmonton', state: 'AB' },
  { city: 'Ottawa', state: 'ON' },
  { city: 'Winnipeg', state: 'MB' },
  { city: 'Mississauga', state: 'ON' },
  { city: 'Brampton', state: 'ON' },
  { city: 'Surrey', state: 'BC' },
  { city: 'Halifax', state: 'NS' },
  { city: 'Markham', state: 'ON' },
  { city: 'Scarborough', state: 'ON' },
  { city: 'Richmond', state: 'BC' },
  { city: 'Burnaby', state: 'BC' },
];

const NICHE = 'immigration consultants';
const MAX_PER_CITY = 200;
const GRID_CELLS = 15; // more coverage per city

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

async function main() {
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  const startTime = Date.now();

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║  MORTAR — Canada Immigration Consultants Scrape          ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Cities:  ${CITIES.length}`);
  console.log(`  Niche:   ${NICHE}`);
  console.log(`  Grid:    ${GRID_CELLS} cells per city`);
  console.log('');

  const gmaps = require('../scrapers/directories/google-maps');
  const allLeads = [];
  const seen = new Set();

  for (let c = 0; c < CITIES.length; c++) {
    const { city, state } = CITIES[c];
    console.log(`  [${c + 1}/${CITIES.length}] ${city}, ${state}...`);

    const options = {
      niche: NICHE,
      city: city,
      maxCities: 1,
      gridCells: GRID_CELLS,
      personExtract: true,
    };

    let cityCount = 0;
    try {
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
        result.state = result.state || state;
        result.country = result.country || 'CA';
        allLeads.push(result);
        cityCount++;

        if (cityCount >= MAX_PER_CITY) break;
      }
    } catch (err) {
      console.log(`    Error: ${err.message}`);
    }

    console.log(`    +${cityCount} businesses (${allLeads.length} total)`);

    // Save intermediate results after each city
    if (allLeads.length > 0) {
      writeCSV(path.join('output', 'immigration-consultants-canada-progress.csv'), allLeads);
    }
  }

  console.log(`\n  Discovery complete: ${allLeads.length} businesses\n`);

  if (allLeads.length === 0) {
    console.log('  No leads found.\n');
    return;
  }

  // ─── Enrichment ─────────────────────────────────────────
  console.log('  ── ENRICH (Email Waterfall) ──────────────────────────');

  try {
    const { DomainPatternDB } = require('../lib/domain-pattern-db');
    const { EmailWaterfall } = require('../lib/email-waterfall');

    const patternDB = new DomainPatternDB();
    console.log(`    Pattern DB: ${patternDB.size} domains cached`);

    const enrichable = allLeads.filter(l =>
      !l.email && l.first_name && l.last_name && l.domain && l.last_name.length > 2
    );

    const { enriched, cold } = patternDB.enrichBulk(enrichable);

    // Apply cached results
    const enrichedMap = new Map();
    for (const e of enriched) {
      enrichedMap.set(`${e.first_name}|${e.last_name}|${e.domain}`, e);
    }
    for (const lead of allLeads) {
      if (lead.email) continue;
      const key = `${lead.first_name}|${lead.last_name}|${lead.domain}`;
      const found = enrichedMap.get(key);
      if (found) {
        lead.email = found.email;
        lead.email_source = found.source;
        lead.email_confidence = found.status;
      }
    }
    console.log(`    Instant apply: ${enriched.length} emails`);

    if (cold.length > 0) {
      const waterfall = new EmailWaterfall();
      patternDB.seedWaterfall(waterfall);

      console.log(`    Verifying ${cold.length} cold leads...`);
      const scanStart = Date.now();

      const results = await waterfall.findEmailsBatch(cold, 3, (found, total) => {
        if (total % 25 === 0 || total === cold.length) {
          const elapsed = ((Date.now() - scanStart) / 1000).toFixed(1);
          console.log(`    [${total}/${cold.length}] ${found} found | ${elapsed}s`);
        }
      });

      for (let i = 0; i < cold.length; i++) {
        const r = results[i];
        if (!r) continue;
        const lead = cold[i];
        for (const l of allLeads) {
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
  } catch (err) {
    console.log(`    Enrichment error: ${err.message}`);
  }

  // ─── Score ──────────────────────────────────────────────
  for (const lead of allLeads) {
    let score = 0;
    const title = (lead.title || '').toLowerCase();
    if (/owner|founder|ceo|president|principal|managing|director/i.test(title)) score += 50;
    else if (/partner|vp|vice president|consultant/i.test(title)) score += 40;
    else if (/manager|supervisor|head|lead|advisor/i.test(title)) score += 30;
    else if (/senior|sr\.|specialist|coordinator/i.test(title)) score += 20;
    else if (title) score += 10;
    if (lead.email) score += 20;
    if (lead.phone) score += 10;
    if (lead.linkedin_url) score += 5;
    if (lead.website) score += 5;
    lead.dm_score = score;
  }

  allLeads.sort((a, b) => (b.dm_score || 0) - (a.dm_score || 0));

  // ─── Export ─────────────────────────────────────────────
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outputPath = path.join('output', `immigration-consultants-canada_${timestamp}.csv`);
  writeCSV(outputPath, allLeads);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = allLeads.filter(l => l.email).length;
  const totalPhones = allLeads.filter(l => l.phone).length;
  const totalWebsites = allLeads.filter(l => l.website).length;
  const dmCount = allLeads.filter(l => (l.dm_score || 0) >= 50).length;

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║       SCRAPE COMPLETE                                    ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:    ${String(allLeads.length).padEnd(40)}║`);
  console.log(`║  With email:     ${String(totalEmails + ' (' + Math.round(totalEmails / allLeads.length * 100) + '%)').padEnd(40)}║`);
  console.log(`║  With phone:     ${String(totalPhones + ' (' + Math.round(totalPhones / allLeads.length * 100) + '%)').padEnd(40)}║`);
  console.log(`║  With website:   ${String(totalWebsites + ' (' + Math.round(totalWebsites / allLeads.length * 100) + '%)').padEnd(40)}║`);
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
