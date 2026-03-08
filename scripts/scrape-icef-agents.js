#!/usr/bin/env node
/**
 * Scrape ICEF Qualified Education Agent (QEA) agencies from Pier Connect.
 *
 * Source: https://www.icef.com/academy/qualified-education-agents-qea-list/
 *         (embeds iframe from https://connect.pierapps.com/widgets/agencies/list)
 *
 * Method: Pier Connect DataTables server-side API
 *         GET /widgets/agencies/list.json?sEcho=N&iDisplayStart=X&iDisplayLength=200
 *         Returns HTML fragments in JSON — parsed with cheerio for name, address, profile URL
 *         Then fetches individual profile pages for phone, email, website, principal agent, QEAC info
 *
 * Expected: ~9,500+ agencies globally
 *
 * Features:
 *   - DataTables server-side pagination (200 per page)
 *   - Profile page scraping for contact details (phone, email, website, principal agent)
 *   - 2-3s delay between requests
 *   - Incremental CSV saves every 200 agents
 *   - Test mode (--test): only fetches 2 listing pages + 10 profiles
 *   - Skip details mode (--skip-details): only listing data
 *   - Country filter (--country=Australia)
 *   - Profiles-only mode (--profiles-from=N): resume profile fetching from row N
 *
 * Usage:
 *   node scripts/scrape-icef-agents.js
 *   node scripts/scrape-icef-agents.js --test
 *   node scripts/scrape-icef-agents.js --skip-details
 *   node scripts/scrape-icef-agents.js --country=Australia
 *   node scripts/scrape-icef-agents.js --profiles-from=500
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE        = !!args.test;
const SKIP_DETAILS     = !!args['skip-details'];
const COUNTRY_FILTER   = (args.country || '').toLowerCase();
const PROFILES_FROM    = parseInt(args['profiles-from']) || 0;
const DELAY_MS         = parseInt(args.delay) || 2500;
const PAGE_SIZE        = 200;
const OUTPUT_DIR       = path.join(__dirname, '..', 'output');
const OUTPUT_FILE      = path.join(OUTPUT_DIR, 'education-agents-icef.csv');
const PROGRESS_FILE    = path.join(OUTPUT_DIR, 'icef-progress.json');
const SAVE_EVERY       = 200;

const LIST_API = 'https://connect.pierapps.com/widgets/agencies/list.json';
const PROFILE_BASE = 'https://connect.pierapps.com/widgets/agencies';

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'address', 'fax',
  'principal_agent', 'qeac_numbers', 'counselor_count',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  listingPages: 0,
  totalRecords: 0,
  totalFromListing: 0,
  detailsFetched: 0,
  detailErrors: 0,
  withEmail: 0,
  withPhone: 0,
  withWebsite: 0,
  withPrincipal: 0,
  saved: 0,
  startTime: Date.now(),
};

// ── Helpers ─────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function escapeCSV(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function elapsed() {
  const s = Math.floor((Date.now() - stats.startTime) / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`;
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://connect.pierapps.com/widgets/agencies/list',
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function httpsGetHtml(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': 'https://connect.pierapps.com/widgets/agencies/list',
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ── Parse listing page (DataTables JSON) ────────────────────────────────
function parseListingData(jsonStr) {
  const data = JSON.parse(jsonStr);
  const agents = [];

  stats.totalRecords = data.iTotalDisplayRecords || 0;

  for (const row of (data.aaData || [])) {
    // row[0] = logo HTML, row[1] = name link HTML, row[2] = address text, row[3] = view profile button
    const nameHtml = row[1] || '';
    const addressText = row[2] || '';

    // Extract name and profile URL from link HTML
    const $name = cheerio.load(nameHtml);
    const $a = $name('a');
    const name = $a.text().trim();
    const href = $a.attr('href') || '';

    // Extract agency ID from href: /widgets/agencies/982
    const idMatch = href.match(/\/agencies\/(\d+)/);
    const agencyId = idMatch ? idMatch[1] : '';

    if (!name || !agencyId) continue;

    // Parse address: " Ground Floor 262 Queen St, Melbourne, VIC, 3000, Australia"
    const addr = addressText.trim();
    const addrParts = addr.split(',').map(s => s.trim()).filter(Boolean);

    // Country is usually the last part
    const country = addrParts.length > 0 ? addrParts[addrParts.length - 1] : '';

    // City is usually 2nd or 3rd from end depending on format
    // Format: Street, City, State, PostCode, Country
    let city = '';
    let state = '';
    if (addrParts.length >= 4) {
      city = addrParts[addrParts.length - 4] || '';
      state = addrParts[addrParts.length - 3] || '';
    } else if (addrParts.length >= 3) {
      city = addrParts[addrParts.length - 3] || '';
    } else if (addrParts.length >= 2) {
      city = addrParts[0];
    }

    // The name often includes city: "Agency Name - City"
    let firmName = name;
    let nameCity = '';
    const dashIdx = name.lastIndexOf(' - ');
    if (dashIdx > 0) {
      firmName = name.substring(0, dashIdx).trim();
      nameCity = name.substring(dashIdx + 3).trim();
      if (!city) city = nameCity;
    }

    agents.push({
      firm_name: firmName,
      address: addr,
      city,
      state,
      country,
      profile_url: `${PROFILE_BASE}/${agencyId}`,
      agency_id: agencyId,
    });
  }

  return agents;
}

// ── Parse profile page ──────────────────────────────────────────────────
function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const result = {};
  const text = $('body').text();

  // Parse structured fields from the profile page
  // Format: "Address:\n value\n Ph:\n +61 03...\n Fax:\n...\n Contact Email:\n...\n Website:\n..."

  // Phone
  const phMatch = text.match(/Ph:\s*\n?\s*([^\n]+)/);
  if (phMatch) result.phone = phMatch[1].trim();

  // Fax
  const faxMatch = text.match(/Fax:\s*\n?\s*([^\n]+)/);
  if (faxMatch) result.fax = faxMatch[1].trim();

  // Email
  const emailMatch = text.match(/Contact Email:\s*\n?\s*([^\n]+)/);
  if (emailMatch) {
    const email = emailMatch[1].trim();
    if (email.includes('@')) result.email = email;
  }

  // Website
  const websiteMatch = text.match(/Website:\s*\n?\s*([^\n]+)/);
  if (websiteMatch) {
    const w = websiteMatch[1].trim();
    if (w && w !== '-' && w.length > 3) result.website = w;
  }

  // Principal Agent
  const principalMatch = text.match(/Principal Agent:\s*\n?\s*([^\n]+)/);
  if (principalMatch) {
    const name = principalMatch[1].trim();
    if (name && name !== '-' && !name.startsWith('Qualified')) {
      result.principal_agent = name;
      // Split name
      const parts = name.split(/\s+/);
      if (parts.length >= 2) {
        result.first_name = parts[0];
        result.last_name = parts.slice(1).join(' ');
      } else {
        result.first_name = name;
      }
    }
  }

  // Qualified Counsellors - count and QEAC numbers
  const qeacNumbers = [];
  const qeacMatches = text.matchAll(/QEAC\s+([A-Z]\d+)/g);
  for (const m of qeacMatches) {
    qeacNumbers.push(m[1]);
  }
  if (qeacNumbers.length > 0) {
    result.qeac_numbers = qeacNumbers.join('; ');
    result.counselor_count = String(qeacNumbers.length);
  }

  // Also try to get structured address from profile if available
  const addrMatch = text.match(/Address:\s*\n?\s*([\s\S]*?)(?=\n\s*(?:Ph:|Fax:|Contact Email:|Website:|Principal|Qualified|Back|$))/);
  if (addrMatch) {
    const addr = addrMatch[1].replace(/\n/g, ', ').replace(/,\s*,/g, ',').trim();
    if (addr.length > 5) result.address = addr;
  }

  return result;
}

// ── Save CSV ────────────────────────────────────────────────────────────
function saveCSV(agents) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const header = CSV_COLUMNS.join(',');
  const rows = agents.map(a =>
    CSV_COLUMNS.map(col => escapeCSV(a[col] || '')).join(',')
  );

  fs.writeFileSync(OUTPUT_FILE, header + '\n' + rows.join('\n') + '\n');
  stats.saved = agents.length;
}

// ── Save/load progress ─────────────────────────────────────────────────
function saveProgress(agents, lastIndex) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    lastIndex,
    totalAgents: agents.length,
    timestamp: new Date().toISOString(),
  }));
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== ICEF Qualified Education Agent (QEA) Scraper ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  if (COUNTRY_FILTER) console.log(`Country filter: ${COUNTRY_FILTER}`);
  if (SKIP_DETAILS) console.log('Skipping profile page fetches');
  if (PROFILES_FROM > 0) console.log(`Resuming profiles from row ${PROFILES_FROM}`);
  console.log('');

  // ── Phase 1: Fetch all listings via DataTables API ──────────────────
  console.log('--- Phase 1: Fetching agency listings via API ---');

  let allAgents = [];
  let start = 0;
  let echo = 1;
  let maxPages = TEST_MODE ? 2 : 1000;

  while (true) {
    const url = `${LIST_API}?sEcho=${echo}&iDisplayStart=${start}&iDisplayLength=${PAGE_SIZE}&sSearch=&iColumns=4`;
    console.log(`  [${elapsed()}] Fetching listings ${start + 1}-${start + PAGE_SIZE}...`);

    try {
      const json = await httpsGet(url);
      const agents = parseListingData(json);

      if (echo === 1) {
        console.log(`  Total agencies in database: ${stats.totalRecords}`);
      }

      if (agents.length === 0) {
        console.log(`  No more agents, stopping.`);
        break;
      }

      allAgents.push(...agents);
      stats.listingPages++;
      console.log(`  Got ${agents.length} agencies (total: ${allAgents.length}/${stats.totalRecords})`);

      if (allAgents.length >= stats.totalRecords) {
        console.log('  Fetched all records.');
        break;
      }

      echo++;
      start += PAGE_SIZE;
      maxPages--;
      if (maxPages <= 0) {
        if (TEST_MODE) console.log('  TEST MODE: stopping after 2 pages');
        break;
      }

      await sleep(DELAY_MS);
    } catch (err) {
      console.error(`  ERROR fetching listings at ${start}: ${err.message}`);
      if (allAgents.length > 0) break;
      throw err;
    }
  }

  stats.totalFromListing = allAgents.length;
  console.log(`\nPhase 1 complete: ${allAgents.length} agencies from ${stats.listingPages} pages`);

  // Apply country filter
  if (COUNTRY_FILTER) {
    const before = allAgents.length;
    allAgents = allAgents.filter(a =>
      (a.country || '').toLowerCase().includes(COUNTRY_FILTER)
    );
    console.log(`Country filter "${COUNTRY_FILTER}": ${before} -> ${allAgents.length}`);
  }

  // ── Phase 2: Fetch profile pages ────────────────────────────────────
  if (!SKIP_DETAILS) {
    console.log('\n--- Phase 2: Fetching agency profiles ---');
    const startIdx = PROFILES_FROM || 0;
    const limit = TEST_MODE ? Math.min(startIdx + 10, allAgents.length) : allAgents.length;
    console.log(`  Fetching profiles ${startIdx + 1} to ${limit} of ${allAgents.length}...`);

    for (let i = startIdx; i < limit; i++) {
      const agent = allAgents[i];
      const profileUrl = agent.profile_url;

      try {
        const html = await httpsGetHtml(profileUrl);
        const detail = parseProfilePage(html);

        // Merge detail data
        if (detail.phone) agent.phone = detail.phone;
        if (detail.fax) agent.fax = detail.fax;
        if (detail.email) agent.email = detail.email;
        if (detail.website) {
          agent.website = detail.website;
          agent.domain = extractDomain(detail.website);
        }
        if (detail.principal_agent) agent.principal_agent = detail.principal_agent;
        if (detail.first_name) agent.first_name = detail.first_name;
        if (detail.last_name) agent.last_name = detail.last_name;
        if (detail.qeac_numbers) agent.qeac_numbers = detail.qeac_numbers;
        if (detail.counselor_count) agent.counselor_count = detail.counselor_count;
        if (detail.address && detail.address.length > (agent.address || '').length) {
          agent.address = detail.address;
        }

        if (detail.phone) stats.withPhone++;
        if (detail.email) stats.withEmail++;
        if (detail.website) stats.withWebsite++;
        if (detail.principal_agent) stats.withPrincipal++;
        stats.detailsFetched++;

        if ((i + 1) % 50 === 0 || i === startIdx) {
          const pct = ((i + 1) / limit * 100).toFixed(1);
          console.log(`  [${elapsed()}] ${i + 1}/${limit} profiles (${pct}%) — ${stats.withEmail} emails, ${stats.withPhone} phones, ${stats.withWebsite} websites`);
        }

        // Save incrementally
        if ((i + 1) % SAVE_EVERY === 0) {
          saveCSV(allAgents);
          saveProgress(allAgents, i);
          console.log(`  [${elapsed()}] Saved progress at row ${i + 1}`);
        }

        await sleep(DELAY_MS);
      } catch (err) {
        stats.detailErrors++;
        if (stats.detailErrors % 10 === 0) {
          console.error(`  ERROR profile ${agent.firm_name} (${agent.agency_id}): ${err.message} (${stats.detailErrors} total errors)`);
        }
      }
    }

    console.log(`\nPhase 2 complete: ${stats.detailsFetched} profiles fetched, ${stats.detailErrors} errors`);
  }

  // ── Finalize and set niche/source ───────────────────────────────────
  for (const agent of allAgents) {
    agent.niche = 'Education Agent';
    agent.source = 'icef';
  }

  // Count final stats (for agents that already had data from listing)
  let finalWithEmail = 0, finalWithPhone = 0, finalWithWebsite = 0;
  for (const a of allAgents) {
    if (a.email) finalWithEmail++;
    if (a.phone) finalWithPhone++;
    if (a.website) finalWithWebsite++;
  }

  // ── Save final CSV ──────────────────────────────────────────────────
  saveCSV(allAgents);

  // ── Report ──────────────────────────────────────────────────────────
  console.log('\n=== RESULTS ===');
  console.log(`Total agencies: ${allAgents.length}`);
  console.log(`With email:     ${finalWithEmail} (${(finalWithEmail / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With phone:     ${finalWithPhone} (${(finalWithPhone / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With website:   ${finalWithWebsite} (${(finalWithWebsite / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With principal: ${stats.withPrincipal}`);
  console.log(`Profile errors: ${stats.detailErrors}`);
  console.log(`Time: ${elapsed()}`);
  console.log(`Output: ${OUTPUT_FILE}`);

  // Country breakdown
  const countries = {};
  for (const a of allAgents) {
    const c = a.country || 'Unknown';
    countries[c] = (countries[c] || 0) + 1;
  }
  const sorted = Object.entries(countries).sort((a, b) => b[1] - a[1]);
  console.log(`\nCountry breakdown (${sorted.length} countries):`);
  for (const [c, n] of sorted.slice(0, 25)) {
    console.log(`  ${c}: ${n}`);
  }
  if (sorted.length > 25) console.log(`  ... and ${sorted.length - 25} more`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
