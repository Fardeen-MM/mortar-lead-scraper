#!/usr/bin/env node
/**
 * Scrape English UK Partner Agency Directory.
 *
 * Source: https://www.englishuk.com/en/students/your-study-options/find-an-educational-agent/english-uk-partner-agency-directory
 * Method: Server-rendered HTML pages with pagination (?page=N, zero-indexed)
 *
 * Phase 1 — Fetch all listing pages (20 agents per page) to get names, addresses, phones, websites, emails, profile IDs
 * Phase 2 — Fetch detail pages for each agent to get contact person name, description, structured country
 *
 * Expected: ~248 partner education agents
 *
 * Features:
 *   - Cheerio HTML parsing (no Puppeteer needed)
 *   - 2-3s delay between requests
 *   - Incremental CSV saves every 50 agents
 *   - Test mode (--test): only fetches 2 listing pages + 5 detail pages
 *   - Skip details mode (--skip-details): only listing data
 *   - Country filter (--country=Turkey)
 *
 * Usage:
 *   node scripts/scrape-english-uk-agents.js
 *   node scripts/scrape-english-uk-agents.js --test
 *   node scripts/scrape-english-uk-agents.js --skip-details
 *   node scripts/scrape-english-uk-agents.js --country=Turkey
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE      = !!args.test;
const SKIP_DETAILS   = !!args['skip-details'];
const COUNTRY_FILTER = (args.country || '').toLowerCase();
const DELAY_MS       = parseInt(args.delay) || 2500;
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const OUTPUT_FILE    = path.join(OUTPUT_DIR, 'uk-education-agents-english-uk.csv');
const SAVE_EVERY     = 50;

const BASE_URL = 'https://www.englishuk.com/en/students/your-study-options/find-an-educational-agent/english-uk-partner-agency-directory';

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'address', 'description',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  listingPages: 0,
  totalFromListing: 0,
  detailsFetched: 0,
  detailErrors: 0,
  withEmail: 0,
  withPhone: 0,
  withWebsite: 0,
  withContact: 0,
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

function httpGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        let loc = res.headers.location;
        if (loc.startsWith('/')) {
          const parsed = new URL(url);
          loc = parsed.protocol + '//' + parsed.host + loc;
        }
        return httpGet(loc, maxRedirects - 1).then(resolve, reject);
      }
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

// ── Parse listing page ──────────────────────────────────────────────────
function parseListingPage(html) {
  const $ = cheerio.load(html);
  const agents = [];

  // Each agent is in an h3 > a tag followed by a <p> with address, phone, website, email
  $('h3').each((_, h3) => {
    const $h3 = $(h3);
    const $link = $h3.find('a');
    if (!$link.length) return;

    const name = $link.text().trim();
    const href = $link.attr('href') || '';

    // Extract ID from URL: ?id=572
    const idMatch = href.match(/[?&]id=(\d+)/);
    const agentId = idMatch ? idMatch[1] : '';
    if (!agentId) return;

    const profileUrl = `${BASE_URL}?id=${agentId}`;

    // The sibling <p> contains address, phone, website, email
    const $p = $h3.next('p');
    if (!$p.length) return;

    const pHtml = $p.html() || '';

    // Parse address (text before first <br>)
    const addressMatch = pHtml.match(/^([^<]+)/);
    const address = addressMatch ? addressMatch[1].trim() : '';

    // Parse phone (after "Tel ")
    const telMatch = pHtml.match(/Tel\s+([^<]+)/);
    const phone = telMatch ? telMatch[1].trim() : '';

    // Parse website
    const websiteMatch = pHtml.match(/href="(https?:\/\/[^"]+)"[^>]*target="_blank"/);
    const website = websiteMatch ? websiteMatch[1].trim() : '';

    // Parse email
    const emailMatch = pHtml.match(/mailto:([^"]+)"/);
    const email = emailMatch ? emailMatch[1].trim() : '';

    // Parse country from address (last part after last comma)
    const addressParts = address.split(',').map(s => s.trim());
    const lastPart = addressParts[addressParts.length - 1] || '';

    // Try to identify country from last or second-to-last part
    let country = '';
    let city = '';
    let state = '';

    // Common countries in the directory
    const countryKeywords = [
      'United Kingdom', 'UK', 'Turkey', 'Brazil', 'Italy', 'Spain', 'France',
      'Germany', 'Japan', 'China', 'India', 'Korea', 'Thailand', 'Vietnam',
      'Russia', 'Belarus', 'Ukraine', 'Poland', 'Hungary', 'Czech Republic',
      'Colombia', 'Mexico', 'Argentina', 'Chile', 'Peru', 'Ecuador',
      'Saudi Arabia', 'UAE', 'Kuwait', 'Oman', 'Qatar', 'Bahrain',
      'Iran', 'Iraq', 'Egypt', 'Nigeria', 'South Africa', 'Ghana',
      'USA', 'United States', 'Canada', 'Australia', 'New Zealand',
      'Indonesia', 'Malaysia', 'Philippines', 'Singapore', 'Taiwan',
      'Hong Kong', 'Kazakhstan', 'Uzbekistan', 'Azerbaijan', 'Georgia',
      'Sri Lanka', 'Bangladesh', 'Pakistan', 'Nepal',
      'Switzerland', 'Austria', 'Netherlands', 'Belgium', 'Sweden',
      'Norway', 'Denmark', 'Finland', 'Portugal', 'Greece', 'Romania',
      'Bulgaria', 'Croatia', 'Serbia', 'Slovenia', 'Slovakia',
      'Lithuania', 'Latvia', 'Estonia', 'Iceland', 'Ireland',
      'North Macedonia', 'Albania', 'Montenegro', 'Bosnia',
    ];

    // Check last part for country
    for (const c of countryKeywords) {
      if (lastPart.toLowerCase().includes(c.toLowerCase())) {
        country = lastPart;
        break;
      }
    }

    // If address has enough parts, try to extract city
    if (addressParts.length >= 3) {
      city = addressParts[addressParts.length - (country ? 3 : 2)] || '';
    } else if (addressParts.length >= 2) {
      city = addressParts[0] || '';
    }

    agents.push({
      firm_name: name,
      address,
      phone,
      website,
      email,
      country,
      city,
      state,
      domain: extractDomain(website),
      profile_url: profileUrl,
      agent_id: agentId,
    });
  });

  // Extract total count
  let totalCount = 0;
  const displayMatch = html.match(/Displaying\s+\d+\s*-\s*\d+\s+of\s+(\d+)/);
  if (displayMatch) {
    totalCount = parseInt(displayMatch[1]);
  }

  // Calculate max page from total count (20 per page, zero-indexed)
  let maxPage = totalCount > 0 ? Math.ceil(totalCount / 20) - 1 : 0;

  return { agents, totalCount, maxPage };
}

// ── Parse detail page ───────────────────────────────────────────────────
function parseDetailPage(html) {
  const $ = cheerio.load(html);
  const result = {};

  // The detail page has a content-list div with structured data
  const contentHtml = $('.content-list').html() || $('body').html() || '';
  const text = $('body').text();

  // Contact name
  const contactMatch = text.match(/Contact\s*\n?\s*([^\n]+)/);
  if (contactMatch) {
    const contactName = contactMatch[1].trim();
    if (contactName && !contactName.match(/^(Address|Country|Telephone|Website|Email|Description)/)) {
      result.contact_name = contactName;
      const nameParts = contactName.split(/\s+/);
      if (nameParts.length >= 2) {
        result.first_name = nameParts[0];
        result.last_name = nameParts.slice(1).join(' ');
      } else {
        result.first_name = contactName;
      }
    }
  }

  // Country (structured)
  const countryMatch = text.match(/Country\s*\n?\s*([^\n]+)/);
  if (countryMatch) {
    result.country = countryMatch[1].trim();
  }

  // Description
  const descMatch = text.match(/Description of centre\s*(?:-->)?\s*\n?\s*([^\n]+(?:\n[^\n]+)*?)(?:\s*<\s*back|$)/i);
  if (descMatch) {
    result.description = descMatch[1].trim().substring(0, 500);
  }

  // Also try parsing from HTML more carefully
  const detailHtml = contentHtml || html;

  // Structured fields from HTML
  const contactHtmlMatch = detailHtml.match(/Contact\s*<\/(?:h4|h3|strong|b)>\s*(?:<[^>]+>)*\s*([^<]+)/i);
  if (contactHtmlMatch && !result.contact_name) {
    const name = contactHtmlMatch[1].trim();
    if (name && name.length > 2) {
      result.contact_name = name;
      const parts = name.split(/\s+/);
      if (parts.length >= 2) {
        result.first_name = parts[0];
        result.last_name = parts.slice(1).join(' ');
      }
    }
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

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== English UK Partner Agency Directory Scraper ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  if (COUNTRY_FILTER) console.log(`Country filter: ${COUNTRY_FILTER}`);
  if (SKIP_DETAILS) console.log('Skipping detail page fetches');
  console.log('');

  // ── Phase 1: Fetch listing pages ────────────────────────────────────
  console.log('--- Phase 1: Fetching listing pages ---');

  let allAgents = [];
  let page = 0;
  let totalCount = 0;
  let maxPage = 0;

  while (true) {
    const url = `${BASE_URL}?letter_index=&keywords=&country=0&page=${page}`;
    console.log(`  [${elapsed()}] Fetching page ${page + 1}...`);

    try {
      const html = await httpGet(url);
      const result = parseListingPage(html);

      if (page === 0) {
        totalCount = result.totalCount;
        maxPage = result.maxPage;
        console.log(`  Total agents: ${totalCount}, Pages: ${maxPage + 1}`);
      }

      if (result.agents.length === 0) {
        console.log(`  No agents on page ${page + 1}, stopping.`);
        break;
      }

      allAgents.push(...result.agents);
      stats.listingPages++;
      console.log(`  Got ${result.agents.length} agents (total: ${allAgents.length})`);

      // Check if we've reached the last page
      if (page >= maxPage) {
        console.log('  Reached last page.');
        break;
      }

      // Test mode: limit pages
      if (TEST_MODE && page >= 1) {
        console.log('  TEST MODE: stopping after 2 pages');
        break;
      }

      page++;
      await sleep(DELAY_MS);
    } catch (err) {
      console.error(`  ERROR on page ${page}: ${err.message}`);
      if (page > 0) break; // Stop if we've already fetched some pages
      throw err;
    }
  }

  stats.totalFromListing = allAgents.length;
  console.log(`\nPhase 1 complete: ${allAgents.length} agents from ${stats.listingPages} pages`);

  // Apply country filter
  if (COUNTRY_FILTER) {
    const before = allAgents.length;
    allAgents = allAgents.filter(a =>
      (a.country || '').toLowerCase().includes(COUNTRY_FILTER)
    );
    console.log(`Country filter "${COUNTRY_FILTER}": ${before} -> ${allAgents.length}`);
  }

  // ── Phase 2: Fetch detail pages ─────────────────────────────────────
  if (!SKIP_DETAILS) {
    console.log('\n--- Phase 2: Fetching detail pages ---');
    const limit = TEST_MODE ? 5 : allAgents.length;
    console.log(`  Fetching ${Math.min(limit, allAgents.length)} detail pages...`);

    for (let i = 0; i < Math.min(limit, allAgents.length); i++) {
      const agent = allAgents[i];
      const detailUrl = `${BASE_URL}?id=${agent.agent_id}`;

      try {
        const html = await httpGet(detailUrl);
        const detail = parseDetailPage(html);

        // Merge detail data (only overwrite if we got better data)
        if (detail.first_name) agent.first_name = detail.first_name;
        if (detail.last_name) agent.last_name = detail.last_name;
        if (detail.country) agent.country = detail.country;
        if (detail.description) agent.description = detail.description;
        if (detail.contact_name) stats.withContact++;

        stats.detailsFetched++;

        if ((i + 1) % 10 === 0 || i === 0) {
          console.log(`  [${elapsed()}] ${i + 1}/${Math.min(limit, allAgents.length)} details fetched (${stats.withContact} contacts found)`);
        }

        // Save incrementally
        if ((i + 1) % SAVE_EVERY === 0) {
          saveCSV(allAgents);
          console.log(`  Saved ${allAgents.length} agents to CSV`);
        }

        await sleep(DELAY_MS);
      } catch (err) {
        stats.detailErrors++;
        console.error(`  ERROR detail ${agent.firm_name}: ${err.message}`);
      }
    }

    console.log(`\nPhase 2 complete: ${stats.detailsFetched} details fetched, ${stats.detailErrors} errors`);
  }

  // ── Finalize and set niche/source ───────────────────────────────────
  for (const agent of allAgents) {
    agent.niche = 'Education Agent';
    agent.source = 'english-uk';

    if (agent.email) stats.withEmail++;
    if (agent.phone) stats.withPhone++;
    if (agent.website) stats.withWebsite++;
  }

  // ── Save final CSV ──────────────────────────────────────────────────
  saveCSV(allAgents);

  // ── Report ──────────────────────────────────────────────────────────
  console.log('\n=== RESULTS ===');
  console.log(`Total agents: ${allAgents.length}`);
  console.log(`With email:   ${stats.withEmail} (${(stats.withEmail / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With phone:   ${stats.withPhone} (${(stats.withPhone / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With website: ${stats.withWebsite} (${(stats.withWebsite / allAgents.length * 100).toFixed(1)}%)`);
  console.log(`With contact: ${stats.withContact}`);
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
  for (const [c, n] of sorted.slice(0, 20)) {
    console.log(`  ${c}: ${n}`);
  }
  if (sorted.length > 20) console.log(`  ... and ${sorted.length - 20} more`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
