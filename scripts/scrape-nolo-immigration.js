#!/usr/bin/env node
/**
 * Nolo.com Immigration Lawyers Scraper
 *
 * Scrapes https://lawyers.nolo.com/immigration-law/{state}
 * using Cheerio (plain HTTP — no Cloudflare protection).
 *
 * Strategy:
 *   1. For each state, scrape all listing pages (50 results/page)
 *   2. For each lawyer, visit profile page to get phone, website, address from JSON-LD
 *   3. Dedup by profile URL
 *   4. Incremental save to CSV after each state
 *
 * Usage:
 *   node scripts/scrape-nolo-immigration.js                          # Default top immigration states
 *   node scripts/scrape-nolo-immigration.js --states CA,TX,NY        # Specific states
 *   node scripts/scrape-nolo-immigration.js --states CA --no-profiles # Skip profile pages (faster, less data)
 *   node scripts/scrape-nolo-immigration.js --test                   # Test mode (2 states, 1 page each)
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const https = require('https');
const http = require('http');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const DELAY_BETWEEN_STATES = 5000;
const PROFILE_DELAY_MIN = 1500;
const PROFILE_DELAY_MAX = 2500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const RESULTS_PER_PAGE = 50;

const STATE_SLUGS = {
  AL: 'alabama', AK: 'alaska', AZ: 'arizona', AR: 'arkansas',
  CA: 'california', CO: 'colorado', CT: 'connecticut', DE: 'delaware',
  DC: 'district-of-columbia', FL: 'florida', GA: 'georgia', HI: 'hawaii',
  ID: 'idaho', IL: 'illinois', IN: 'indiana', IA: 'iowa',
  KS: 'kansas', KY: 'kentucky', LA: 'louisiana', ME: 'maine',
  MD: 'maryland', MA: 'massachusetts', MI: 'michigan', MN: 'minnesota',
  MS: 'mississippi', MO: 'missouri', MT: 'montana', NE: 'nebraska',
  NV: 'nevada', NH: 'new-hampshire', NJ: 'new-jersey', NM: 'new-mexico',
  NY: 'new-york', NC: 'north-carolina', ND: 'north-dakota', OH: 'ohio',
  OK: 'oklahoma', OR: 'oregon', PA: 'pennsylvania', RI: 'rhode-island',
  SC: 'south-carolina', SD: 'south-dakota', TN: 'tennessee', TX: 'texas',
  UT: 'utah', VT: 'vermont', VA: 'virginia', WA: 'washington',
  WV: 'west-virginia', WI: 'wisconsin', WY: 'wyoming',
};

// Default: top immigration states by market size
const DEFAULT_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC',
];

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay(min = DELAY_MIN, max = DELAY_MAX) {
  return sleep(min + Math.random() * (max - min));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const doRequest = (attempt) => {
      const mod = url.startsWith('https') ? https : http;
      const req = mod.get(url, {
        headers: {
          'User-Agent': USER_AGENT,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: REQUEST_TIMEOUT,
      }, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const redirectUrl = res.headers.location.startsWith('http')
            ? res.headers.location
            : new URL(res.headers.location, url).toString();
          res.resume();
          return resolve(httpGet(redirectUrl, retries));
        }

        if (res.statusCode !== 200) {
          res.resume();
          if (attempt < retries) {
            return setTimeout(() => doRequest(attempt + 1), 3000 * attempt);
          }
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }

        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
        res.on('error', reject);
      });

      req.on('error', (err) => {
        if (attempt < retries) {
          setTimeout(() => doRequest(attempt + 1), 3000 * attempt);
        } else {
          reject(err);
        }
      });

      req.on('timeout', () => {
        req.destroy();
        if (attempt < retries) {
          setTimeout(() => doRequest(attempt + 1), 3000 * attempt);
        } else {
          reject(new Error(`Timeout for ${url}`));
        }
      });
    };

    doRequest(1);
  });
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  // Remove common suffixes
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney at law|attorney|lawyer|ph\.?d\.?|md|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?|p\.?a\.?|p\.?c\.?|p\.?l\.?l\.?c\.?|apc|llc)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim();

  // If it looks like a firm name (contains &, "Law", "Legal", "Group", "Office", etc.), skip name parsing
  if (/\b(law\s+(office|firm|group|center)|legal|&|associates|attorneys)\b/i.test(cleaned)) {
    return { first_name: '', last_name: '' };
  }

  if (cleaned.includes(',')) {
    const [last, ...rest] = cleaned.split(',').map(s => s.trim());
    const first = rest.join(' ').split(/\s+/)[0] || '';
    return { first_name: first, last_name: last };
  }

  const parts = cleaned.split(/\s+/);
  if (parts.length >= 2) {
    return { first_name: parts[0], last_name: parts[parts.length - 1] };
  }
  return { first_name: '', last_name: cleaned };
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `http://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  // Handle arrays (JSON-LD sometimes returns arrays)
  if (Array.isArray(phone)) phone = phone[0];
  if (typeof phone !== 'string') return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

function cleanWebsite(url) {
  if (!url) return '';
  if (Array.isArray(url)) url = url[0];
  if (typeof url !== 'string') return '';
  // Skip nolo.com URLs
  if (url.includes('nolo.com')) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `http://${url}`);
    // Remove common tracking params
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(p => u.searchParams.delete(p));
    return u.toString();
  } catch { return url; }
}

function parseLocation(locationText) {
  if (!locationText) return { city: '', state: '' };
  const cleaned = locationText.replace(/\t/g, ' ').replace(/\s+/g, ' ').trim();
  // "Hollywood, FL" or "Los Angeles, CA"
  const m = cleaned.match(/^(.+?),\s*([A-Z]{2})$/);
  if (m) {
    return { city: m[1].trim(), state: m[2] };
  }
  return { city: cleaned, state: '' };
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

// ── Scraper Core ─────────────────────────────────────────────────────────────

/**
 * Parse a listing page and extract lawyer entries.
 * Returns array of { name, profileUrl, location, tagline }
 */
function parseListingPage(html) {
  const $ = cheerio.load(html);
  const results = [];
  const seen = new Set();

  $('.lawyer-directory-profile').each((_, el) => {
    const card = $(el);

    // Name and profile URL
    const nameLink = card.find('.provider a').first();
    const name = nameLink.text().trim();
    const profileUrl = nameLink.attr('href') || '';

    if (!name || name.length < 2) return;
    if (seen.has(profileUrl)) return;
    if (profileUrl) seen.add(profileUrl);

    // Location
    const locationText = card.find('.location').first().text().trim();

    // Tagline / description
    const tagline = card.find('.tagline').first().text().trim();

    results.push({
      name,
      profileUrl: profileUrl.startsWith('http') ? profileUrl : `https://lawyers.nolo.com${profileUrl}`,
      location: locationText,
      tagline,
    });
  });

  return results;
}

/**
 * Get pagination info from listing page.
 * Returns the maximum page number found.
 */
function getMaxPage(html) {
  const $ = cheerio.load(html);
  let maxPage = 1;

  $('a[href*="page="]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const m = href.match(/page=(\d+)/);
    if (m) {
      const p = parseInt(m[1]);
      if (p > maxPage) maxPage = p;
    }
  });

  return maxPage;
}

/**
 * Parse a profile page to extract phone, website, address, firm name.
 * Extracts from JSON-LD (schema.org LegalService) and HTML fallbacks.
 */
function parseProfilePage(html) {
  const $ = cheerio.load(html);
  const result = {
    phone: '',
    website: '',
    domain: '',
    firm_name: '',
    first_name: '',
    last_name: '',
    city: '',
    state: '',
    title: '',
    email: '',
  };

  // 1. Try JSON-LD first (most reliable)
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const data = JSON.parse($(el).html());
      const graph = data['@graph'] || [data];
      for (const item of graph) {
        if (item['@type'] === 'LegalService' || item['@type'] === 'Attorney') {
          // Phone
          if (item.telephone) {
            result.phone = cleanPhone(item.telephone);
          }

          // Address
          if (item.address) {
            const addr = item.address;
            if (addr.addressLocality) {
              result.city = addr.addressLocality.replace(/\s+/g, ' ').trim();
              // Title-case the city if it's all uppercase
              if (result.city === result.city.toUpperCase()) {
                result.city = result.city.replace(/\w\S*/g, w =>
                  w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
                );
              }
            }
            if (addr.addressRegion) {
              result.state = addr.addressRegion.trim();
            }
          }

          // Listing name — could be person name or firm name
          const listingName = (item.name || '').trim();

          // Employees — extract the actual person's name
          // JSON-LD employees[0].name is concatenated like "ColleenDiSanto"
          if (item.employees && item.employees.length > 0) {
            const person = item.employees[0];
            if (person.name) {
              // Split camelCase concatenated name: "ColleenDiSanto" -> "Colleen DiSanto"
              const spaced = person.name.replace(/([a-z])([A-Z])/g, '$1 $2');
              const parts = spaced.split(/\s+/).filter(Boolean);
              if (parts.length >= 2) {
                result.first_name = parts[0];
                result.last_name = parts.slice(1).join(' ');
              } else if (parts.length === 1) {
                result.first_name = parts[0];
              }
            }
            // Note: jobTitle in Nolo JSON-LD is actually just the first name, not useful
          }

          // Determine if listing name is a firm name (distinct from person name)
          if (listingName) {
            const isFirmName = /\b(law\s+(office|firm|group|center)|legal|&|associates|attorneys|pllc|plc|llc|llp|p\.?c\.?|p\.?a\.?|apc|corp|inc)\b/i.test(listingName);
            if (isFirmName) {
              result.firm_name = listingName;
            } else if (result.first_name && result.last_name) {
              // Listing name is the person's name — we already have it from employees
              // Only set firm_name if listing name differs from the person's name
              const personFullName = `${result.first_name} ${result.last_name}`.toLowerCase();
              if (!listingName.toLowerCase().includes(result.last_name.toLowerCase())) {
                result.firm_name = listingName;
              }
            }
          }
        }
      }
    } catch (e) {
      // JSON parse error, skip
    }
  });

  // 2. Website from HTML (firm-websites section)
  const websiteLink = $('.firm-websites a[href]').first();
  if (websiteLink.length) {
    const href = websiteLink.attr('href') || '';
    if (href && !href.includes('nolo.com')) {
      result.website = cleanWebsite(href);
      result.domain = extractDomain(href);
    }
  }

  // 3. Fallback: phone from HTML
  if (!result.phone) {
    const phoneLink = $('a[href^="tel:"]').first();
    if (phoneLink.length) {
      result.phone = cleanPhone(phoneLink.text().trim());
    }
  }

  // 4. Email from HTML (mailto links)
  $('a[href^="mailto:"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const email = href.replace('mailto:', '').split('?')[0].trim();
    if (email && email.includes('@') && !email.includes('nolo.com')) {
      result.email = email;
    }
  });

  return result;
}

/**
 * Scrape all listing pages for a given state.
 * Returns array of card objects from listing pages.
 */
async function scrapeStateListings(stateCode, stateSlug, maxPages = 20) {
  const allCards = [];
  const baseUrl = `https://lawyers.nolo.com/immigration-law/${stateSlug}`;

  for (let page = 1; page <= maxPages; page++) {
    const url = page === 1 ? baseUrl : `${baseUrl}?page=${page}`;
    log(`  Page ${page}: ${url}`);

    try {
      const html = await httpGet(url);

      const cards = parseListingPage(html);
      if (cards.length === 0) {
        log(`  Page ${page}: 0 results, stopping pagination`);
        break;
      }

      allCards.push(...cards);
      log(`  Page ${page}: ${cards.length} lawyers found`);

      // Check if there are more pages
      if (page === 1) {
        const maxPage = getMaxPage(html);
        if (maxPage < 2) {
          log(`  Single page state — no pagination`);
          break;
        }
        log(`  Pagination: up to page ${maxPage}`);
        if (maxPages > maxPage) maxPages = maxPage;
      }

      if (page < maxPages) {
        await randomDelay();
      }
    } catch (err) {
      log(`  ERROR on page ${page}: ${err.message}`);
      if (page === 1) break; // If first page fails, skip state
      // Otherwise continue to next page
    }
  }

  return allCards;
}

/**
 * Enrich a lead by visiting its profile page.
 * Returns enriched fields { phone, website, domain, firm_name, city, state, email, title }.
 */
async function enrichFromProfile(profileUrl) {
  try {
    const html = await httpGet(profileUrl);
    return parseProfilePage(html);
  } catch (err) {
    log(`    Profile error: ${err.message}`);
    return null;
  }
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const skipProfiles = args.includes('--no-profiles');
  let statesFilter = null;

  const statesArg = args.find(a => a.startsWith('--states='));
  if (statesArg) {
    statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
  }

  let states = statesFilter || DEFAULT_STATES;
  let maxPagesPerState = isTest ? 1 : 20;

  if (isTest && !statesFilter) {
    states = states.slice(0, 2); // CA, NY in test mode
  }

  const outDir = path.join(__dirname, '..', 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, 'us-immigration-lawyers-nolo.csv');

  log(`Nolo Immigration Lawyers Scraper`);
  log(`States: ${states.join(', ')} (${states.length} total)`);
  log(`Test mode: ${isTest} | Skip profiles: ${skipProfiles} | Max pages/state: ${maxPagesPerState}`);
  log(`Output: ${outPath}`);
  log('');

  const allLeads = [];
  const seenProfileUrls = new Set();

  // Load existing leads if output file exists (resume support)
  if (fs.existsSync(outPath)) {
    const existing = fs.readFileSync(outPath, 'utf-8').split('\n').slice(1); // skip header
    for (const line of existing) {
      if (!line.trim()) continue;
      // Extract profile_url (last column)
      const cols = line.split(',');
      const profileUrl = cols[cols.length - 1]?.replace(/"/g, '').trim();
      if (profileUrl && profileUrl.startsWith('http')) {
        seenProfileUrls.add(profileUrl);
      }
    }
    if (seenProfileUrls.size > 0) {
      log(`Loaded ${seenProfileUrls.size} existing profile URLs for dedup`);
    }
  }

  let totalStatesProcessed = 0;

  for (const stateCode of states) {
    const stateSlug = STATE_SLUGS[stateCode];
    if (!stateSlug) {
      log(`Unknown state code: ${stateCode}, skipping`);
      continue;
    }

    totalStatesProcessed++;
    log(`\n${'='.repeat(60)}`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${stateSlug})`);

    // 1. Scrape listing pages
    const cards = await scrapeStateListings(stateCode, stateSlug, maxPagesPerState);

    if (cards.length === 0) {
      log(`  No results for ${stateCode}`);
      continue;
    }

    // 2. Dedup and build lead objects
    let stateNewLeads = 0;
    let stateSkipped = 0;

    for (let i = 0; i < cards.length; i++) {
      const card = cards[i];

      if (seenProfileUrls.has(card.profileUrl)) {
        stateSkipped++;
        continue;
      }
      seenProfileUrls.add(card.profileUrl);

      // Parse name from listing (fallback)
      const listingName = parseName(card.name);
      const loc = parseLocation(card.location);

      const lead = {
        first_name: listingName.first_name,
        last_name: listingName.last_name,
        firm_name: '',
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: loc.city,
        state: loc.state || stateCode,
        country: 'US',
        niche: 'immigration',
        source: 'nolo',
        profile_url: card.profileUrl,
      };

      // 3. Optionally enrich from profile page
      if (!skipProfiles) {
        log(`    [${i + 1}/${cards.length}] Enriching: ${card.name}`);
        const profile = await enrichFromProfile(card.profileUrl);

        if (profile) {
          lead.phone = profile.phone || '';
          lead.website = profile.website || '';
          lead.domain = profile.domain || '';
          lead.email = profile.email || '';

          // Use profile person name if extracted (more reliable than listing name parsing)
          if (profile.first_name) lead.first_name = profile.first_name;
          if (profile.last_name) lead.last_name = profile.last_name;

          // Use profile data for city/state (more reliable — from structured JSON-LD)
          if (profile.city) lead.city = profile.city;
          if (profile.state) lead.state = profile.state;

          // Firm name from profile (already filtered for firm-like names)
          if (profile.firm_name) lead.firm_name = profile.firm_name;

          if (profile.title) lead.title = profile.title;
        }

        await sleep(PROFILE_DELAY_MIN + Math.random() * (PROFILE_DELAY_MAX - PROFILE_DELAY_MIN));
      }

      allLeads.push(lead);
      stateNewLeads++;
    }

    log(`  ${stateCode}: ${stateNewLeads} new leads, ${stateSkipped} dupes skipped`);

    // 4. Incremental save after each state
    writeCSV(allLeads, outPath);
    log(`  Saved ${allLeads.length} total leads to CSV`);

    // Delay between states
    if (totalStatesProcessed < states.length) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 2000);
    }
  }

  // Final summary
  log(`\n${'='.repeat(60)}`);
  log(`DONE`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}/${states.length}`);
  log(`Leads with phone: ${allLeads.filter(l => l.phone).length}`);
  log(`Leads with email: ${allLeads.filter(l => l.email).length}`);
  log(`Leads with website: ${allLeads.filter(l => l.website).length}`);
  log(`Leads with firm name: ${allLeads.filter(l => l.firm_name).length}`);
  log(`Output: ${outPath}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
