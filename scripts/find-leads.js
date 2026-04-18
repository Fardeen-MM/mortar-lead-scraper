#!/usr/bin/env node
/**
 * find-leads.js — Unified "Apollo clone" CLI
 *
 * Given a niche + location, returns verified emails by chaining:
 *   1. DDG/Serper search → business names + domains
 *   2. person-extractor (Puppeteer) → names from each website
 *   3. email-waterfall → verified emails per person
 *   4. CSV output
 *
 * Usage:
 *   node scripts/find-leads.js "immigration consultant" "Miami FL"
 *   node scripts/find-leads.js "immigration lawyer" "Toronto Canada" --count 50
 *   node scripts/find-leads.js "IELTS training" "London UK" --out london-ielts.csv
 *
 * Set SERPER_API_KEY env var to use Google (via Serper) instead of DDG.
 *
 * This is FREE with DDG. Serper costs $1/1K queries.
 */

const fs = require('fs');
const path = require('path');

const ddg = require('../lib/duckduckgo-scraper');
const searxng = require('../lib/searxng-scraper');
const multiEngine = require('../lib/multi-engine-search');
const { SerperSearch } = require('../lib/serper-search');
const { EmailWaterfall } = require('../lib/email-waterfall');
const PersonExtractor = require('../lib/person-extractor');

// ── CLI args ────────────────────────────────────────────────────────────
const positional = [];
const flags = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const [key, val] = arg.replace(/^--/, '').split('=');
    flags[key] = val === undefined ? true : val;
  } else {
    positional.push(arg);
  }
}

const NICHE = positional[0];
const LOCATION = positional[1] || '';
const COUNT = parseInt(flags.count) || 50;
const OUT = flags.out;
const USE_SERPER = !!process.env.SERPER_API_KEY && !flags['no-serper'];
const USE_SEARXNG = !!process.env.SEARXNG_URL && !flags['no-searxng'];
const SKIP_PEOPLE = !!flags['no-people']; // Skip person extraction (just use generic domain emails)

if (!NICHE) {
  console.error('Usage: node scripts/find-leads.js "<niche>" "<location>" [--count 50] [--out file.csv]');
  console.error('');
  console.error('Examples:');
  console.error('  node scripts/find-leads.js "immigration consultant" "Miami FL"');
  console.error('  node scripts/find-leads.js "IELTS training" "London UK" --count 100');
  console.error('  node scripts/find-leads.js "visa lawyer" "Toronto" --out visa-toronto.csv');
  process.exit(1);
}

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const outFile = path.resolve(OUT || path.join(OUTPUT_DIR,
  `find-leads_${NICHE.replace(/\W+/g, '-').toLowerCase()}_${LOCATION.replace(/\W+/g, '-').toLowerCase() || 'global'}.csv`
));

// ── Helpers ─────────────────────────────────────────────────────────────
function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(rows) {
  const cols = [
    'email', 'confidence', 'first_name', 'last_name', 'title',
    'company', 'domain', 'website', 'source', 'niche', 'location',
    'phone', 'snippet', 'status',
  ];
  const header = cols.join(',');
  const body = rows.map(r => cols.map(c => escapeCsv(r[c] || '')).join(',')).join('\n');
  fs.writeFileSync(outFile, header + '\n' + body + '\n');
}

function elapsed(start) {
  const s = Math.floor((Date.now() - start) / 1000);
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

// ── Stage 1: Discovery ─────────────────────────────────────────────────
async function discoverBusinesses(niche, location, maxResults) {
  // Priority: Serper (paid) → SearxNG (self-host) → Multi-engine (Brave+Startpage+Mojeek+DDG) → DDG
  const engine = USE_SERPER ? 'Serper/Google'
    : USE_SEARXNG ? `SearxNG (${process.env.SEARXNG_URL})`
    : 'Multi-Engine (Brave+Startpage+Mojeek+DDG)';
  console.log(`\n[Stage 1] Discovery (${engine})`);

  const t0 = Date.now();

  // Multi-engine path (default — uses Brave+Startpage+Mojeek+DDG rotation)
  if (!USE_SEARXNG && !USE_SERPER) {
    try {
      const queries = [
        `${niche} ${location || ''}`.trim(),
        location ? `"${niche}" "${location}"` : `"${niche}"`,
      ];
      const allResults = [];
      const seenDomains = new Set();

      for (const q of queries) {
        if (allResults.length >= maxResults) break;
        console.log(`  [multi-engine] "${q}"`);
        const results = await multiEngine.search(q, {
          max: maxResults,
          maxEngines: 2,
          verbose: true,
        });
        for (const r of results) {
          if (seenDomains.has(r.domain)) continue;
          seenDomains.add(r.domain);
          allResults.push({
            name: r.title ? r.title.split(/[|•·\-–—]/)[0].trim().slice(0, 80) : r.domain,
            url: r.url,
            domain: r.domain,
            snippet: r.snippet || '',
            phone: '',
            source: r.source,
          });
          if (allResults.length >= maxResults) break;
        }
      }

      console.log(`  [Stage 1] ${allResults.length} unique domains in ${elapsed(t0)}`);
      if (allResults.length > 0) return allResults;
    } catch (err) {
      console.error(`  [Stage 1] multi-engine failed: ${err.message} — falling back to DDG-only`);
    }
  }

  // SearxNG path
  if (USE_SEARXNG && !USE_SERPER) {
    try {
      const businesses = await searxng.search(niche, location, {
        maxResults,
        onProgress: (i, total, q) => console.log(`  [SearxNG ${i}/${total}] ${q}`),
      });
      console.log(`  [Stage 1] ${businesses.length} unique domains in ${elapsed(t0)}`);
      if (businesses.length > 0) return businesses;
      console.log('  [Stage 1] SearxNG returned 0 results — falling back to DDG');
    } catch (err) {
      console.error(`  [Stage 1] SearxNG failed: ${err.message} — falling back to DDG`);
    }
  }

  if (USE_SERPER) {
    const serper = new SerperSearch();
    const queries = [
      `${niche} in ${location}`,
      `"${niche}" "${location}"`,
      `${niche} ${location} contact`,
    ];

    const byDomain = new Map();
    for (const q of queries) {
      try {
        console.log(`  [Serper] "${q}"`);
        const results = await serper.search(q, Math.min(100, Math.ceil(maxResults / queries.length) + 10));
        for (const r of results) {
          if (!r.link) continue;
          let domain;
          try { domain = new URL(r.link).hostname.replace(/^www\./, ''); }
          catch { continue; }
          if (/yelp|bbb|yellowpages|facebook|linkedin|twitter|instagram|youtube|wikipedia/i.test(domain)) continue;
          if (byDomain.has(domain)) continue;
          byDomain.set(domain, {
            name: r.title.split('|')[0].split('-')[0].trim(),
            url: r.link,
            domain,
            snippet: r.snippet,
            source: 'serper',
          });
        }
        await new Promise(r => setTimeout(r, 500));
      } catch (e) {
        console.error(`  [Serper] error: ${e.message}`);
      }
    }
    const out = [...byDomain.values()].slice(0, maxResults);
    console.log(`  [Stage 1] ${out.length} unique domains in ${elapsed(t0)}`);
    return out;
  }

  // DDG path
  const businesses = await ddg.search(niche, location, {
    maxResults,
    onProgress: (i, total, q) => console.log(`  [DDG ${i}/${total}] ${q}`),
  });
  console.log(`  [Stage 1] ${businesses.length} unique domains in ${elapsed(t0)}`);
  return businesses;
}

// ── Stage 2: Person extraction ─────────────────────────────────────────
async function extractPeople(businesses, skip) {
  if (skip) {
    console.log('\n[Stage 2] Skipped (--no-people). Using generic email (info@, contact@) per domain.');
    return businesses.map(b => ({
      first_name: '',
      last_name: '',
      title: '',
      domain: b.domain,
      website: b.url,
      company: b.name,
      phone: b.phone || '',
      snippet: b.snippet || '',
    }));
  }

  console.log(`\n[Stage 2] Person extraction (Puppeteer) on ${businesses.length} websites`);
  const t0 = Date.now();
  const extractor = new PersonExtractor();

  try {
    await extractor.init();

    const people = [];
    let done = 0;

    for (const biz of businesses) {
      done++;
      try {
        const result = await extractor.extractPeople(biz.url);
        const list = (result && result.people) || [];
        if (list.length === 0) {
          people.push({
            first_name: '', last_name: '', title: '',
            domain: biz.domain, website: biz.url, company: biz.name,
            phone: biz.phone || '', snippet: biz.snippet || '',
          });
        } else {
          for (const p of list) {
            people.push({
              first_name: p.first_name || p.firstName || '',
              last_name: p.last_name || p.lastName || '',
              title: p.title || '',
              domain: biz.domain,
              website: biz.url,
              company: biz.name,
              phone: biz.phone || p.phone || '',
              snippet: biz.snippet || '',
            });
          }
        }
        console.log(`  [${done}/${businesses.length}] ${biz.name.slice(0, 40)} → ${list.length} people`);
      } catch (err) {
        console.error(`  [${done}/${businesses.length}] ${biz.domain} ERROR: ${err.message}`);
        people.push({
          first_name: '', last_name: '', title: '',
          domain: biz.domain, website: biz.url, company: biz.name,
          phone: biz.phone || '', snippet: biz.snippet || '',
        });
      }
    }

    console.log(`  [Stage 2] ${people.length} people from ${businesses.length} sites in ${elapsed(t0)}`);
    return people;
  } finally {
    try { await extractor.close(); } catch {}
  }
}

// ── Stage 3: Email waterfall ───────────────────────────────────────────
async function findEmails(people) {
  console.log(`\n[Stage 3] Email waterfall on ${people.length} candidates`);
  const t0 = Date.now();
  const waterfall = new EmailWaterfall();

  const results = [];
  let verified = 0, catchall = 0, notFound = 0;

  for (let i = 0; i < people.length; i++) {
    const p = people[i];
    let email = '', confidence = 0, status = 'not_found', source = '';

    if (p.first_name && p.last_name && p.domain) {
      try {
        const r = await waterfall.findEmail({
          first_name: p.first_name,
          last_name: p.last_name,
          domain: p.domain,
        });
        if (r && r.email) {
          email = r.email;
          confidence = r.confidence || 0;
          status = r.status || 'found';
          source = r.source || '';
        }
      } catch (err) {
        status = 'error';
      }
    } else if (p.domain) {
      // Fallback: try generic emails for the domain
      const candidates = ['info', 'contact', 'hello', 'admin', 'enquiries'];
      for (const prefix of candidates) {
        const candidate = `${prefix}@${p.domain}`;
        try {
          const v = await waterfall.verifyEmail(candidate);
          if (v && v.valid) {
            email = candidate;
            confidence = v.confidence || 50;
            status = v.status || 'verified';
            source = 'generic_prefix';
            break;
          }
        } catch {}
      }
    }

    if (status === 'verified' || confidence >= 70) verified++;
    else if (status === 'catchall' || status === 'risky') catchall++;
    else if (status === 'not_found') notFound++;

    results.push({
      email,
      confidence,
      first_name: p.first_name,
      last_name: p.last_name,
      title: p.title,
      company: p.company,
      domain: p.domain,
      website: p.website,
      source,
      niche: NICHE,
      location: LOCATION,
      phone: p.phone,
      snippet: p.snippet,
      status,
    });

    if ((i + 1) % 10 === 0 || i === people.length - 1) {
      console.log(`  [${i + 1}/${people.length}] verified=${verified} catchall=${catchall} not_found=${notFound} (${elapsed(t0)})`);
      // Incremental save
      writeCsv(results);
    }
  }

  console.log(`  [Stage 3] ${verified} verified, ${catchall} catch-all, ${notFound} not found in ${elapsed(t0)}`);
  return results;
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== find-leads: "${NICHE}" in "${LOCATION}" (target ${COUNT}) ===`);
  console.log(`Output: ${outFile}`);
  const t0 = Date.now();

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Stage 1
  const businesses = await discoverBusinesses(NICHE, LOCATION, COUNT);

  if (businesses.length === 0) {
    console.error('\nNo businesses found. Try different keywords or check DDG rate limits.');
    process.exit(1);
  }

  // Stage 2
  const people = await extractPeople(businesses, SKIP_PEOPLE);

  // Stage 3
  const results = await findEmails(people);

  // Final write
  writeCsv(results);

  const withEmail = results.filter(r => r.email).length;
  const verified = results.filter(r => r.confidence >= 70).length;

  console.log(`\n=== DONE ===`);
  console.log(`Time:         ${elapsed(t0)}`);
  console.log(`Businesses:   ${businesses.length}`);
  console.log(`People:       ${people.length}`);
  console.log(`With email:   ${withEmail}`);
  console.log(`Verified(70+): ${verified}`);
  console.log(`CSV:          ${outFile}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
