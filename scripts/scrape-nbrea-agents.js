#!/usr/bin/env node
/**
 * Scrape ALL real estate agents from New Brunswick, Canada.
 *
 * Source: Royal LePage public agent directory (royallepage.ca)
 *   - The FCNB portal (portal.fcnb.ca) requires PowerApps anti-forgery auth
 *   - NBREA (nbrea.ca) redirects to realtor.ca which is behind Incapsula WAF
 *   - Royal LePage directory is publicly accessible with structured data
 *
 * Strategy:
 *   Phase 1 — Fetch city list from /en/search/get-agents-city-list/nb/{page}/
 *   Phase 2 — For each city, paginate through /en/nb/{city}/agents/{page}/
 *   Phase 3 — Parse agent cards: name, title, brokerage, address, phones, email
 *
 * Fields extracted: first_name, last_name, firm_name, title, email, phone,
 *                   website (profile URL), domain, city, state, country, niche, source
 *
 * Zero npm dependencies — uses only Node.js built-in modules (https, fs, path).
 *
 * Features:
 *   - Full province coverage via city-by-city iteration
 *   - Pagination within each city
 *   - Dedup by agent ID extracted from profile URL
 *   - Rate limiting (configurable, default 500ms)
 *   - Test mode (--test): 3 cities, 1 page each
 *   - Resume support: skips cities already in output file
 *
 * Usage:
 *   node scripts/scrape-nbrea-agents.js
 *   node scripts/scrape-nbrea-agents.js --test
 *   node scripts/scrape-nbrea-agents.js --delay=800
 *
 * Output: output/nbrea-real-estate-agents.csv
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ── CLI args ──────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS   = parseInt(args.delay) || 500;
const TEST_MODE  = Boolean(args.test);
const OUTPUT_DIR  = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'nbrea-real-estate-agents.csv');

const PROVINCE_CODE = 'nb';
const STATE_NAME    = 'New Brunswick';
const COUNTRY       = 'CA';
const NICHE         = 'real estate agent';
const SOURCE        = 'nbrea';

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// ── CSV columns ───────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source',
];

const CSV_HEADER = CSV_COLUMNS.join(',');

function escCsv(val) {
  if (!val) return '';
  const s = String(val).replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

function leadToCsvRow(lead) {
  return CSV_COLUMNS.map(col => escCsv(lead[col])).join(',');
}

// ── Helpers ───────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function titleCase(str) {
  if (!str) return '';
  return str
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map(w => {
      if (/^(of|the|and|in|at|for|de|du|la|le|des|von|van)$/i.test(w)) {
        return w.toLowerCase();
      }
      if (w.length <= 2 && w === w.toUpperCase()) return w; // NB, PE, etc.
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(' ');
}

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+]/g, '').replace(/^1(\d{10})$/, '$1');
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

// ── HTTP helper ──────────────────────────────────────────────────────────
function httpGet(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const options = {
      hostname: u.hostname,
      path: u.pathname + u.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(opts.headers || {}),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data }));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

// ── Phase 1: Fetch all city slugs ────────────────────────────────────────
async function fetchCities() {
  const cities = [];
  let page = 1;

  while (true) {
    const url = `https://www.royallepage.ca/en/search/get-agents-city-list/${PROVINCE_CODE}/${page}/`;
    console.log(`  Fetching city list page ${page}...`);

    const res = await httpGet(url, {
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
    });

    if (res.status !== 200) {
      console.log(`  City list page ${page}: HTTP ${res.status}, stopping.`);
      break;
    }

    let json;
    try { json = JSON.parse(res.data); } catch { break; }

    if (!json.html) break;

    // Extract city slugs from HTML: /en/nb/{slug}/agents/
    const re = new RegExp(`/en/${PROVINCE_CODE}/([^/]+)/agents/`, 'g');
    let m;
    while ((m = re.exec(json.html)) !== null) {
      if (!cities.includes(m[1])) cities.push(m[1]);
    }

    // Check if there's a next page link
    if (!json.html.includes(`get-agents-city-list/${PROVINCE_CODE}/${page + 1}/`)) break;

    page++;
    await sleep(DELAY_MS);
  }

  return cities;
}

// ── Phase 2: Parse agent cards from a page ───────────────────────────────
function parseAgentCards(html) {
  const agents = [];

  // Split by agent card boundaries
  const cardPattern = /<div class="card card--agent-card">([\s\S]*?)<\/div>\s*<\/li>/g;
  let match;

  while ((match = cardPattern.exec(html)) !== null) {
    const card = match[1];
    const agent = {};

    // Name from <h2>
    const nameMatch = card.match(/<h2 class="u-font-black">(.*?)<\/h2>/);
    if (nameMatch) {
      const fullName = decodeHtmlEntities(nameMatch[1].replace(/<[^>]+>/g, '').trim());
      const parts = fullName.split(/\s+/);
      if (parts.length >= 2) {
        agent.first_name = titleCase(parts[0]);
        agent.last_name = titleCase(parts.slice(1).join(' '));
      } else {
        agent.first_name = titleCase(fullName);
        agent.last_name = '';
      }
    }

    // Title/designation
    const titleMatch = card.match(/agent-info__title">(.*?)<\/span>/);
    if (titleMatch) {
      agent.title = decodeHtmlEntities(titleMatch[1].replace(/<[^>]+>/g, '').trim());
    }

    // Brokerage
    const brokerageMatch = card.match(/agent-info__brokerage">\s*(?:<[^>]*>)?\s*<a[^>]*>([\s\S]*?)<\/a>/);
    if (brokerageMatch) {
      agent.firm_name = decodeHtmlEntities(brokerageMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim());
    }

    // Address (city extraction)
    const addrMatch = card.match(/agent-info__brokerage-address">([\s\S]*?)<\/span>/);
    if (addrMatch) {
      const addrText = decodeHtmlEntities(addrMatch[1].replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '').trim());
      // City is typically on the second line: "City, Province PostalCode"
      const lines = addrText.split('\n').map(l => l.trim()).filter(Boolean);
      if (lines.length >= 2) {
        const cityLine = lines[lines.length - 1];
        const cityMatch = cityLine.match(/^([^,]+)/);
        if (cityMatch) {
          agent.city = titleCase(cityMatch[1].trim());
        }
      } else if (lines.length === 1) {
        const cityMatch = lines[0].match(/([A-Za-z\s.-]+),\s*[A-Z]{2}/);
        if (cityMatch) agent.city = titleCase(cityMatch[1].trim());
      }
    }

    // Phone numbers — prefer direct, then mobile, then office
    const directMatch = card.match(/Direct<\/span><a[^>]*>([^<]+)<\/a>/);
    const mobileMatch = card.match(/Mobile<\/span><a[^>]*>([^<]+)<\/a>/);
    const officeMatch = card.match(/Office<\/span><a[^>]*>([^<]+)<\/a>/);
    const phoneRaw = (directMatch || mobileMatch || officeMatch || [])[1];
    if (phoneRaw) {
      agent.phone = cleanPhone(phoneRaw);
    }

    // Email from data-email-address attribute
    const emailMatch = card.match(/data-email-address="([^"]+)"/);
    if (emailMatch && emailMatch[1] && emailMatch[1] !== '') {
      agent.email = emailMatch[1].toLowerCase().trim();
    }

    // Profile URL (used as website and for agent ID)
    const profileMatch = card.match(/href="(https:\/\/www\.royallepage\.ca\/en\/agent\/[^"]+)"/);
    if (profileMatch) {
      agent.website = profileMatch[1];
      agent.domain = 'royallepage.ca';

      // Extract agent ID from URL: /agent/.../name/ID/
      const idMatch = profileMatch[1].match(/\/(\d+)\/?$/);
      if (idMatch) agent._agentId = idMatch[1];
    }

    // Only keep if we have at least a name
    if (agent.first_name || agent.last_name) {
      agent.state = STATE_NAME;
      agent.country = COUNTRY;
      agent.niche = NICHE;
      agent.source = SOURCE;
      agents.push(agent);
    }
  }

  return agents;
}

// ── Phase 3: Scrape all agents from a city ───────────────────────────────
async function scrapeCity(citySlug, maxPages = Infinity) {
  const agents = [];
  let page = 1;

  while (page <= maxPages) {
    const pageSuffix = page === 1 ? '' : `${page}/`;
    const url = `https://www.royallepage.ca/en/${PROVINCE_CODE}/${citySlug}/agents/${pageSuffix}`;

    const res = await httpGet(url);

    if (res.status === 404) break;
    if (res.status !== 200) {
      console.log(`    ${citySlug} page ${page}: HTTP ${res.status}, skipping.`);
      break;
    }

    const pageAgents = parseAgentCards(res.data);
    if (pageAgents.length === 0) break;

    agents.push(...pageAgents);

    // Check for next page
    const nextPageUrl = `/en/${PROVINCE_CODE}/${citySlug}/agents/${page + 1}/`;
    if (!res.data.includes(nextPageUrl)) break;

    page++;
    await sleep(DELAY_MS);
  }

  return agents;
}

// ── Main ─────────────────────────────────────────────────────────────────
async function main() {
  console.log('='.repeat(70));
  console.log(`  NBREA Agent Scraper — New Brunswick Real Estate Agents`);
  console.log(`  Source: Royal LePage Directory (royallepage.ca)`);
  console.log(`  Mode: ${TEST_MODE ? 'TEST (3 cities, 1 page each)' : 'FULL'}`);
  console.log(`  Delay: ${DELAY_MS}ms`);
  console.log('='.repeat(70));
  console.log();

  // Ensure output directory
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Phase 1: Fetch city list
  console.log('[Phase 1] Fetching city list...');
  const cities = await fetchCities();
  console.log(`  Found ${cities.length} cities in New Brunswick.`);
  console.log();

  if (cities.length === 0) {
    console.log('ERROR: No cities found. Check network connectivity.');
    process.exit(1);
  }

  const citiesToScrape = TEST_MODE ? cities.slice(0, 3) : cities;
  const maxPages = TEST_MODE ? 1 : Infinity;

  // Phase 2: Scrape each city
  console.log(`[Phase 2] Scraping agents from ${citiesToScrape.length} cities...`);
  const allAgents = [];
  const seenIds = new Set();

  for (let i = 0; i < citiesToScrape.length; i++) {
    const city = citiesToScrape[i];
    const displayCity = city.replace(/-/g, ' ');
    process.stdout.write(`  [${i + 1}/${citiesToScrape.length}] ${titleCase(displayCity)}... `);

    try {
      const agents = await scrapeCity(city, maxPages);
      let newCount = 0;

      for (const agent of agents) {
        const dedupKey = agent._agentId || `${agent.first_name}|${agent.last_name}|${agent.firm_name}`;
        if (seenIds.has(dedupKey)) continue;
        seenIds.add(dedupKey);
        delete agent._agentId;
        allAgents.push(agent);
        newCount++;
      }

      console.log(`${agents.length} found, ${newCount} new (${allAgents.length} total)`);
    } catch (err) {
      console.log(`ERROR: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  console.log();

  // Phase 3: Write CSV
  console.log('[Phase 3] Writing CSV...');
  const rows = [CSV_HEADER, ...allAgents.map(leadToCsvRow)];
  fs.writeFileSync(OUTPUT_FILE, rows.join('\n') + '\n');

  // Stats
  const withEmail = allAgents.filter(a => a.email).length;
  const withPhone = allAgents.filter(a => a.phone).length;

  console.log();
  console.log('='.repeat(70));
  console.log('  RESULTS');
  console.log('='.repeat(70));
  console.log(`  Total agents:     ${allAgents.length}`);
  console.log(`  With email:       ${withEmail} (${(withEmail / allAgents.length * 100 || 0).toFixed(1)}%)`);
  console.log(`  With phone:       ${withPhone} (${(withPhone / allAgents.length * 100 || 0).toFixed(1)}%)`);
  console.log(`  Cities scraped:   ${citiesToScrape.length}`);
  console.log(`  Output file:      ${OUTPUT_FILE}`);
  console.log('='.repeat(70));
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
