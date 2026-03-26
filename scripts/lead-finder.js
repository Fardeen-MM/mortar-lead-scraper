#!/usr/bin/env node
/**
 * Lead Finder — End-to-end B2B lead generation pipeline
 *
 * Given a niche + location, finds businesses, discovers emails,
 * detects ad activity, and outputs enriched CSV.
 *
 * Pipeline:
 *   1. DISCOVER: Google Maps scraper finds businesses (name, phone, website, address)
 *   2. ENRICH: Email waterfall finds work emails (Microsoft 365 API, SMTP, patterns)
 *   3. DETECT: Ad checker identifies Meta/Google ad activity (pixel scan + API checks)
 *   4. OUTPUT: Clean CSV with all data, ready for cold email
 *
 * Usage:
 *   node scripts/lead-finder.js --niche "dentist" --location "Miami, FL"
 *   node scripts/lead-finder.js --niche "lawyer" --location "New York, NY" --max 100
 *   node scripts/lead-finder.js --input existing-leads.csv --enrich --detect-ads
 *
 * Options:
 *   --niche       Business type to search (dentist, lawyer, plumber, etc.)
 *   --location    City/state to search
 *   --max         Maximum leads to find (default 100)
 *   --input       Skip discovery, start from existing CSV
 *   --enrich      Run email enrichment on leads
 *   --detect-ads  Run ad detection on leads
 *   --output      Custom output file path
 *   --skip-discovery  Skip Google Maps (use with --input)
 *   --skip-enrich     Skip email finding
 *   --skip-ads        Skip ad detection
 *
 * Environment:
 *   SERPER_API_KEY  — For website finding via Google search (2,500 free)
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// ── CLI Args ────────────────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const niche = getArg('niche') || '';
const location = getArg('location') || '';
const maxLeads = parseInt(getArg('max') || '100', 10);
const inputFile = getArg('input');
const outputFile = getArg('output') || `output/${niche || 'leads'}-${location?.replace(/[^a-zA-Z0-9]/g, '-') || 'all'}-${new Date().toISOString().split('T')[0]}.csv`;
const skipDiscovery = hasFlag('skip-discovery') || !!inputFile;
const skipEnrich = hasFlag('skip-enrich');
const skipAds = hasFlag('skip-ads');

if (!niche && !inputFile) {
  console.log(`
  Lead Finder — End-to-end B2B lead generation pipeline

  Usage:
    node scripts/lead-finder.js --niche "dentist" --location "Miami, FL"
    node scripts/lead-finder.js --input leads.csv --enrich --detect-ads

  Options:
    --niche       Business type (dentist, lawyer, plumber, etc.)
    --location    City/state
    --max         Max leads (default 100)
    --input       Start from existing CSV
    --output      Custom output path
    --skip-enrich Skip email finding
    --skip-ads    Skip ad detection

  Pipeline: Discover → Enrich (emails) → Detect (ads) → CSV
  `);
  process.exit(1);
}

// ── CSV Helpers ─────────────────────────────────────────────────
function parseLine(line) {
  const cols = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}

function esc(v) {
  const s = (v ?? '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

// ── Main Pipeline ───────────────────────────────────────────────
(async () => {
  const t0 = Date.now();
  console.log('\n╔═══════════════════════════════════════════════╗');
  console.log('║          Lead Finder Pipeline                 ║');
  console.log('╚═══════════════════════════════════════════════╝');

  let leads = [];
  let headers = [];

  // ── STEP 1: Discover businesses ─────────────────────────────
  if (!skipDiscovery) {
    console.log(`\n  ══ Step 1: Discover "${niche}" in "${location}" (max ${maxLeads}) ══`);

    if (!process.env.SERPER_API_KEY) {
      console.log('  ✗ SERPER_API_KEY not set. Get a free key at https://serper.dev');
      console.log('  Or use: node scripts/lead-finder.js --input your-leads.csv\n');
      process.exit(1);
    }

    try {
      const { SerperSearch } = require('../lib/serper-search');
      const search = new SerperSearch();
      const businesses = await search.findBusinesses(niche, location, maxLeads);

      console.log(`  Found ${businesses.length} businesses via Serper Maps API`);

      headers = ['name', 'address', 'phone', 'website', 'rating', 'ratingCount', 'category', 'latitude', 'longitude'];
      for (const biz of businesses) {
        leads.push({
          name: biz.title,
          address: biz.address,
          phone: biz.phone,
          website: biz.website,
          rating: biz.rating,
          ratingCount: biz.ratingCount,
          category: biz.category,
          latitude: biz.latitude,
          longitude: biz.longitude,
        });
      }

      // Split name into first/last for email finding (best effort)
      if (!headers.includes('first_name')) headers.push('first_name', 'last_name');
      // Note: Google Maps returns business names, not owner names
      // Email finding will use the domain from website instead
    } catch (e) {
      console.log(`  ✗ Discovery failed: ${e.message}`);
      process.exit(1);
    }
  }

  // ── Load existing CSV ───────────────────────────────────────
  if (inputFile) {
    console.log(`\n  ══ Loading leads from ${inputFile} ══`);
    const csvText = fs.readFileSync(inputFile, 'utf8');
    const lines = csvText.split('\n').filter(l => l.trim());
    headers = parseLine(lines[0]);
    for (let i = 1; i < lines.length; i++) {
      const cols = parseLine(lines[i]);
      const obj = {};
      headers.forEach((h, j) => { obj[h] = cols[j] || ''; });
      leads.push(obj);
    }
    console.log(`  Loaded ${leads.length} leads with ${headers.length} columns`);
  }

  // ── STEP 2: Find emails ─────────────────────────────────────
  if (!skipEnrich) {
    console.log(`\n  ══ Step 2: Email Enrichment ══`);

    // Find leads missing emails
    const emailCol = headers.find(h => /^email$/i.test(h));
    const firstNameCol = headers.find(h => /^first.?name$/i.test(h));
    const lastNameCol = headers.find(h => /^last.?name$/i.test(h));
    const websiteCol = headers.find(h => /^website$/i.test(h) || /^url$/i.test(h) || /^domain$/i.test(h));
    const nameCol = headers.find(h => /^name$/i.test(h) || /^business.?name$/i.test(h) || /^company$/i.test(h));

    if (!emailCol) {
      headers.push('email', 'email_source', 'email_confidence');
      leads.forEach(l => { l.email = ''; l.email_source = ''; l.email_confidence = ''; });
    }

    const missingEmail = leads.filter(l => !l[emailCol || 'email']?.includes('@'));
    const withDomain = missingEmail.filter(l => {
      const web = l[websiteCol] || '';
      return web && web !== 'false' && web.length > 3;
    });

    console.log(`  Total leads: ${leads.length}`);
    console.log(`  Missing email: ${missingEmail.length}`);
    console.log(`  With website (can enrich): ${withDomain.length}`);

    if (withDomain.length > 0) {
      try {
        const { EmailWaterfall } = require('../lib/email-waterfall');
        const wf = new EmailWaterfall();
        let found = 0;

        for (let i = 0; i < Math.min(withDomain.length, maxLeads); i++) {
          const lead = withDomain[i];
          const firstName = lead[firstNameCol] || lead.first_name || '';
          const lastName = lead[lastNameCol] || lead.last_name || '';

          if (!firstName || !lastName) continue;

          let domain = (lead[websiteCol] || '').replace(/^https?:\/\/(www\.)?/i, '').replace(/\/.*$/, '');
          if (!domain || domain === 'false') continue;

          try {
            const result = await wf.findEmail({ first_name: firstName, last_name: lastName, domain });
            if (result) {
              lead[emailCol || 'email'] = result.email;
              lead.email_source = result.source;
              lead.email_confidence = result.confidence;
              found++;
            }
          } catch {}

          if ((i + 1) % 10 === 0 || i < 3) {
            console.log(`  [${i + 1}/${withDomain.length}] ${found} emails found`);
          }
        }

        console.log(`  ✓ Email enrichment: ${found}/${Math.min(withDomain.length, maxLeads)} found (${(found / Math.min(withDomain.length, maxLeads) * 100).toFixed(0)}%)`);
      } catch (e) {
        console.log(`  ✗ Email enrichment failed: ${e.message}`);
      }
    }
  }

  // ── STEP 3: Detect ads ──────────────────────────────────────
  if (!skipAds) {
    console.log(`\n  ══ Step 3: Ad Detection ══`);

    const websiteCol = headers.find(h => /^website$/i.test(h) || /^url$/i.test(h) || /^domain$/i.test(h));
    const withWebsite = leads.filter(l => {
      const w = l[websiteCol] || '';
      return w && w !== 'false' && w.length > 3;
    });

    if (withWebsite.length > 0) {
      // Write temp CSV for ad checker
      const tempIn = '/tmp/lead-finder-ad-input.csv';
      const tempOut = '/tmp/lead-finder-ad-output.csv';
      const tempHeaders = [...headers];
      const tempRows = leads.map(l => tempHeaders.map(h => esc(l[h] || '')).join(','));
      fs.writeFileSync(tempIn, tempHeaders.join(',') + '\n' + tempRows.join('\n'));

      console.log(`  Running ad checker on ${withWebsite.length} leads with websites...`);

      try {
        execSync(`node --max-old-space-size=4096 scripts/ad-checker-unified.js "${tempIn}" "${tempOut}" 30`, {
          timeout: 600000,
          stdio: ['pipe', 'inherit', 'inherit'],
        });

        // Read enriched output back
        if (fs.existsSync(tempOut)) {
          const enrichedText = fs.readFileSync(tempOut, 'utf8');
          const enrichedLines = enrichedText.split('\n').filter(l => l.trim());
          const enrichedHeaders = parseLine(enrichedLines[0]);

          // Add new columns from ad checker
          const newCols = enrichedHeaders.filter(h => !headers.includes(h));
          headers.push(...newCols);

          // Update leads with ad data
          for (let i = 1; i < enrichedLines.length && i <= leads.length; i++) {
            const cols = parseLine(enrichedLines[i]);
            for (const col of newCols) {
              const idx = enrichedHeaders.indexOf(col);
              if (idx >= 0) leads[i - 1][col] = cols[idx] || '';
            }
          }

          console.log(`  ✓ Ad detection complete`);
        }
      } catch (e) {
        console.log(`  ✗ Ad detection failed: ${e.message}`);
      }

      // Cleanup
      try { fs.unlinkSync(tempIn); } catch {}
      try { fs.unlinkSync(tempOut); } catch {}
    }
  }

  // ── STEP 4: Output ──────────────────────────────────────────
  console.log(`\n  ══ Step 4: Output ══`);

  // Ensure output directory exists
  const outDir = path.dirname(outputFile);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const outRows = leads.map(l => headers.map(h => esc(l[h] || '')).join(','));
  fs.writeFileSync(outputFile, headers.join(',') + '\n' + outRows.join('\n'));

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  const emailCount = leads.filter(l => (l.email || l[headers.find(h => /^email$/i.test(h))] || '').includes('@')).length;
  const hotCount = leads.filter(l => l.lead_category === 'HOT').length;

  console.log(`\n╔═══════════════════════════════════════════════╗`);
  console.log(`║              RESULTS                          ║`);
  console.log(`╠═══════════════════════════════════════════════╣`);
  console.log(`║  Total leads:    ${String(leads.length).padStart(6)}                     ║`);
  console.log(`║  With email:     ${String(emailCount).padStart(6)}                     ║`);
  console.log(`║  Running ads:    ${String(hotCount).padStart(6)}                     ║`);
  console.log(`║  Time:           ${elapsed.padStart(5)}s                     ║`);
  console.log(`╚═══════════════════════════════════════════════╝`);
  console.log(`  → ${outputFile}\n`);
})();
