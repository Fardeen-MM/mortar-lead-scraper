#!/usr/bin/env node
/**
 * Best Lawyers Immigration Lawyers Scraper
 *
 * Scrapes https://www.bestlawyers.com/united-states/{state}/immigration-law
 * using Puppeteer + Stealth (JS-rendered site).
 *
 * Strategy:
 *   1. For each state, load the state listing page
 *   2. Paginate through all result pages (~12 per page)
 *   3. Extract profile URLs from listing cards
 *   4. Visit each profile page to get full data (JSON-LD Person schema)
 *   5. JSON-LD gives: name, firm, phone, website, address, education, awards
 *   6. Dedup by profile URL
 *   7. Incremental save to CSV after each state
 *   8. Resume support — skips states already completed
 *
 * Data fields from Best Lawyers:
 *   - Listing page: name, firm, city, state, recognition year, profile URL
 *   - Profile page (JSON-LD): phone, website, education, awards, full address
 *
 * Output: output/us-immigration-lawyers-bestlawyers.csv
 *
 * Usage:
 *   node scripts/scrape-bestlawyers-immigration.js                    # Default 15 immigration states
 *   node scripts/scrape-bestlawyers-immigration.js --states CA,TX,NY  # Specific states
 *   node scripts/scrape-bestlawyers-immigration.js --all-states       # All 51 states
 *   node scripts/scrape-bestlawyers-immigration.js --test             # Test mode (2 states, 2 pages each)
 *   node scripts/scrape-bestlawyers-immigration.js --no-resume        # Ignore existing CSV, start fresh
 *   node scripts/scrape-bestlawyers-immigration.js --no-profiles      # Skip profile page visits (faster, less data)
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const DELAY_BETWEEN_STATES = 5000;
const DELAY_PROFILE = 2000;        // Delay between profile page visits
const NAV_TIMEOUT = 45000;
const RESULTS_PER_PAGE = 15;       // Best Lawyers shows ~12-15 per page
const MAX_PAGES_DEFAULT = 30;      // Safety cap per state

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-immigration-lawyers-bestlawyers.csv');
const PROGRESS_FILE = OUTPUT_FILE.replace('.csv', '-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// State code → Best Lawyers URL slug
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

// Default immigration-heavy states
const DEFAULT_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC',
];

// All 51 (50 states + DC) ordered by immigration market size
const ALL_STATES = [
  'CA', 'NY', 'TX', 'FL', 'IL', 'NJ', 'MA', 'GA', 'PA', 'VA',
  'MD', 'WA', 'AZ', 'CO', 'NC', 'OH', 'MI', 'CT', 'MN', 'OR',
  'NV', 'TN', 'MO', 'IN', 'WI', 'SC', 'KY', 'LA', 'AL', 'OK',
  'IA', 'UT', 'KS', 'AR', 'MS', 'NE', 'NM', 'HI', 'NH', 'ME',
  'ID', 'WV', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'DC',
];

// State slug → state code (reverse lookup)
const SLUG_TO_CODE = {};
for (const [code, slug] of Object.entries(STATE_SLUGS)) {
  SLUG_TO_CODE[slug] = code;
}

// Full state name → state code (for JSON-LD which returns "California" not "CA")
const STATE_NAME_TO_CODE = {};
for (const [code, slug] of Object.entries(STATE_SLUGS)) {
  // Convert slug like "new-york" to "New York"
  const name = slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  STATE_NAME_TO_CODE[name.toLowerCase()] = code;
}
// Special cases
STATE_NAME_TO_CODE['district of columbia'] = 'DC';

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

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    // Remove titles/suffixes
    .replace(/^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?)\s+/i, '')
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|ph\.?d\.?|m\.?d\.?|ll\.?m\.?|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .replace(/\s+/g, ' ')
    .trim();

  // Handle nicknames in quotes: 'J. Todd "Todd" Benson' → first=J. Todd, last=Benson
  cleaned = cleaned.replace(/\s*"[^"]+"\s*/g, ' ').replace(/\s+/g, ' ').trim();
  // Also handle parenthetical nicknames: 'Jose (Joe) Smith'
  cleaned = cleaned.replace(/\s*\([^)]+\)\s*/g, ' ').replace(/\s+/g, ' ').trim();

  const parts = cleaned.split(' ').filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts[parts.length - 1],
  };
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

/**
 * Normalize state to 2-letter code. Handles:
 *  - Already a code: "CA" → "CA"
 *  - Full name: "California" → "CA"
 *  - Slug: "new-york" → "NY"
 */
function normalizeStateCode(state) {
  if (!state) return '';
  const upper = state.toUpperCase().trim();
  // Already a valid 2-letter code?
  if (upper.length === 2 && STATE_SLUGS[upper]) return upper;
  // Try full name lookup
  const lower = state.toLowerCase().trim();
  if (STATE_NAME_TO_CODE[lower]) return STATE_NAME_TO_CODE[lower];
  // Try slug lookup
  if (SLUG_TO_CODE[lower]) return SLUG_TO_CODE[lower];
  // Return as-is (might be a 2-letter code we don't know)
  return upper.length <= 3 ? upper : state;
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).replace(/[\r\n]+/g, ' ').trim();
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

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      return new Set(data.statesCompleted || []);
    }
  } catch {}
  return new Set();
}

function saveProgress(statesCompleted) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    statesCompleted: [...statesCompleted],
    updatedAt: new Date().toISOString(),
  }, null, 2));
}

function loadExistingCSV(csvPath) {
  const result = { leads: [], seenProfileUrls: new Set() };
  if (!fs.existsSync(csvPath)) return result;

  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length <= 1) return result;

  const headers = lines[0].split(',');

  for (let i = 1; i < lines.length; i++) {
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (const ch of lines[i]) {
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === ',' && !inQuotes) {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
    fields.push(current);

    const lead = {};
    for (let j = 0; j < headers.length && j < fields.length; j++) {
      lead[headers[j]] = fields[j];
    }

    if (lead.profile_url) {
      result.seenProfileUrls.add(lead.profile_url);
    }
    result.leads.push(lead);
  }

  return result;
}

// ── Scraper: Listing Pages ───────────────────────────────────────────────────

/**
 * Extract lawyer cards from a Best Lawyers listing page.
 *
 * Best Lawyers card structure:
 *   - Cards use class "card-body d-flex flex-column p-3 text-center"
 *   - Profile links are overlays: a.ga-lawyer-card-click with aria-label="Name's Lawyer Profile"
 *   - The link text is EMPTY (overlay links) — name comes from aria-label or card innerText
 *   - Card innerText layout: Name \n Firm \n "Recognized since YYYY" \n "City, ST"
 *   - Pagination: a.card-grid-page and a.card-grid-next ("Next Button")
 *
 * Returns { lawyers: [], hasNextPage: boolean, totalText: string }
 */
async function extractListingPage(page) {
  return page.evaluate(() => {
    const result = { lawyers: [], hasNextPage: false, totalText: '' };

    // Look for result count text (e.g., "164 Results" or "1,200+ Results")
    const allText = document.body ? document.body.innerText : '';
    const countMatch = allText.match(/([\d,]+)\+?\s*Results?/i);
    if (countMatch) {
      result.totalText = countMatch[0];
    }

    // Strategy: find all card-body containers that have a profile link overlay
    const cardBodies = document.querySelectorAll('.card-body');
    const seen = new Set();

    for (const card of cardBodies) {
      // Find the profile link inside this card
      const profileLink = card.querySelector('a.ga-lawyer-card-click[href*="/lawyers/"]');
      if (!profileLink) continue;

      const href = profileLink.getAttribute('href') || '';
      const m = href.match(/\/lawyers\/([a-z0-9-]+)\/(\d+)/);
      if (!m) continue;

      const profileUrl = `https://www.bestlawyers.com${href}`;
      if (seen.has(profileUrl)) continue;
      seen.add(profileUrl);

      // Extract name from aria-label (e.g., "Regina Knoll's Lawyer Profile")
      let name = '';
      const ariaLabel = profileLink.getAttribute('aria-label') || '';
      const nameMatch = ariaLabel.match(/^(.+?)(?:'s|'s)?\s*Lawyer\s*Profile/i);
      if (nameMatch) {
        name = nameMatch[1].trim();
      }

      // Fallback: extract name from card innerText (first line)
      if (!name) {
        const cardText = card.innerText || '';
        const lines = cardText.split('\n').map(l => l.trim()).filter(Boolean);
        // First meaningful line (skip initials like "RW", "AM")
        for (const line of lines) {
          // Skip 1-3 char initials, "Practice Areas", "Immigration Law", etc.
          if (line.length <= 3) continue;
          if (/^(practice|immigration|recognized|unactivated)/i.test(line)) continue;
          name = line;
          break;
        }
      }

      if (!name || name.length < 3) continue;

      // Extract firm, location, recognition from card text
      const cardText = card.innerText || '';
      const lines = cardText.split('\n').map(l => l.trim()).filter(Boolean);

      let firm = '';
      let city = '';
      let stateText = '';
      let recognitionYear = '';

      // Also try firm link inside card
      const firmLink = card.querySelector('a[href*="/firm/"]');
      if (firmLink) {
        firm = firmLink.textContent.trim();
      }

      for (const line of lines) {
        // Skip the name itself and short initials
        if (line === name) continue;
        if (line.length <= 3) continue;

        // Recognition year
        if (/recognized\s+since\s+\d{4}/i.test(line)) {
          const ym = line.match(/(\d{4})/);
          if (ym) recognitionYear = ym[1];
          continue;
        }

        // Location: "City, ST" or "City, State"
        const locMatch = line.match(/^([A-Z][a-zA-Z\s]+),\s*([A-Z]{2})$/);
        if (locMatch) {
          city = locMatch[1].trim();
          stateText = locMatch[2];
          continue;
        }

        // If we haven't found a firm yet and it's not a known pattern, it could be the firm
        if (!firm && line !== name && !/^(practice|immigration|unactivated)/i.test(line)) {
          firm = line;
        }
      }

      result.lawyers.push({
        name,
        firm,
        city,
        state: stateText,
        recognitionYear,
        profileUrl,
      });
    }

    // Check pagination — look for "Next Button" link
    const nextBtn = document.querySelector('a.card-grid-next, .card-grid-next');
    if (nextBtn) {
      result.hasNextPage = true;
    }

    // Also check for numbered page links beyond current page
    if (!result.hasNextPage) {
      const paginationLinks = document.querySelectorAll('a.card-grid-page, a[href*="page="]');
      const currentUrl = window.location.href;
      const currentPageMatch = currentUrl.match(/[?&]page=(\d+)/);
      const currentPage = currentPageMatch ? parseInt(currentPageMatch[1]) : 1;

      for (const link of paginationLinks) {
        const pageMatch = (link.getAttribute('href') || '').match(/page=(\d+)/);
        if (pageMatch && parseInt(pageMatch[1]) > currentPage) {
          result.hasNextPage = true;
          break;
        }
      }
    }

    return result;
  });
}

/**
 * Scrape all listing pages for a state.
 * Returns array of { name, firm, city, state, profileUrl }
 */
async function scrapeStateListings(page, stateCode, maxPages) {
  const stateSlug = STATE_SLUGS[stateCode];
  if (!stateSlug) return [];

  const lawyers = [];
  const seenUrls = new Set();

  for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
    const pageParam = pageNum > 1 ? `?page=${pageNum}` : '';
    const url = `https://www.bestlawyers.com/united-states/${stateSlug}/immigration-law${pageParam}`;

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    } catch (err) {
      log(`    ERROR navigating to page ${pageNum}: ${err.message}`);
      break;
    }

    // Wait for content to render (JS-rendered site)
    await sleep(2000);

    // Check for Cloudflare / access issues
    const pageTitle = await page.title();
    if (pageTitle.includes('Just a moment') || pageTitle.includes('Attention Required')) {
      log(`    Cloudflare challenge on page ${pageNum}`);
      await sleep(10000);
      const stillBlocked = await page.evaluate(() =>
        document.title.includes('Just a moment') || document.title.includes('Attention Required')
      );
      if (stillBlocked) {
        log(`    Cannot bypass Cloudflare — stopping pagination`);
        break;
      }
    }

    // Check for 404
    if (pageTitle.includes('Page Not Found') || pageTitle.includes('404')) {
      if (pageNum === 1) log(`    State ${stateCode}: not found (404)`);
      break;
    }

    const data = await extractListingPage(page);

    if (pageNum === 1) {
      log(`    ${stateCode}: ${data.totalText || 'unknown count'}`);
    }

    if (data.lawyers.length === 0) {
      if (pageNum === 1) log(`    No lawyers found on page 1`);
      break;
    }

    let newCount = 0;
    for (const lawyer of data.lawyers) {
      if (seenUrls.has(lawyer.profileUrl)) continue;
      seenUrls.add(lawyer.profileUrl);
      lawyers.push(lawyer);
      newCount++;
    }

    log(`    Page ${pageNum}: ${data.lawyers.length} cards, ${newCount} new`);

    if (!data.hasNextPage) {
      log(`    No more pages`);
      break;
    }

    await randomDelay();
  }

  return lawyers;
}

// ── Scraper: Profile Pages ───────────────────────────────────────────────────

/**
 * Extract detailed data from a Best Lawyers profile page.
 * Best Lawyers profiles have JSON-LD Person schema with rich data.
 */
async function extractProfileData(page) {
  return page.evaluate(() => {
    const data = {
      name: '',
      firm: '',
      phone: '',
      website: '',
      email: '',
      city: '',
      state: '',
      zip: '',
      streetAddress: '',
      education: '',
      title: '',
      recognitionYear: '',
    };

    // 1. Try JSON-LD
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const script of scripts) {
      try {
        const json = JSON.parse(script.textContent);
        if (json['@type'] === 'Person' || json['@type'] === 'Attorney') {
          data.name = json.name || '';

          // Firm
          if (json.worksFor) {
            const wf = Array.isArray(json.worksFor) ? json.worksFor[0] : json.worksFor;
            data.firm = wf.name || '';
          }

          // Phone
          data.phone = json.telephone || '';

          // Website
          if (json.url) {
            // The Best Lawyers profile URL, not the firm website
          }
          if (json.sameAs) {
            const urls = Array.isArray(json.sameAs) ? json.sameAs : [json.sameAs];
            for (const u of urls) {
              if (u && !u.includes('bestlawyers.com') && !u.includes('linkedin.com') &&
                  !u.includes('twitter.com') && !u.includes('facebook.com')) {
                data.website = u;
                break;
              }
            }
          }

          // Address — can be on the Person or inside worksFor
          const addrSource = json.address ||
            (json.worksFor && (Array.isArray(json.worksFor) ? json.worksFor[0] : json.worksFor).address);
          if (addrSource) {
            const addr = Array.isArray(addrSource) ? addrSource[0] : addrSource;
            data.city = addr.addressLocality || '';
            data.state = addr.addressRegion || '';
            data.zip = addr.postalCode || '';
            data.streetAddress = addr.streetAddress || '';
          }

          // Education
          if (json.alumniOf) {
            const schools = Array.isArray(json.alumniOf) ? json.alumniOf : [json.alumniOf];
            data.education = schools.map(s => s.name || s).filter(Boolean).join('; ');
          }

          // Awards / title
          if (json.award) {
            const awards = Array.isArray(json.award) ? json.award : [json.award];
            data.title = awards[0] || '';
          }

          if (json.knowsAbout) {
            // Practice areas — not needed for CSV but useful for validation
          }
        }
      } catch (e) {
        // Skip unparseable JSON-LD
      }
    }

    // 2. Fallback: scrape from visible page elements
    const bodyText = document.body ? document.body.innerText : '';

    // Phone fallback
    if (!data.phone) {
      const phoneLinks = document.querySelectorAll('a[href^="tel:"]');
      for (const link of phoneLinks) {
        const tel = link.getAttribute('href').replace('tel:', '').trim();
        if (tel && tel.length >= 10) {
          data.phone = tel;
          break;
        }
      }
    }

    // Email fallback — only pick up emails that match the lawyer's firm domain
    // Best Lawyers pages have mailto links from ads/sidebars (other lawyers' emails)
    // so we must be strict: only accept emails whose domain matches the firm website
    if (!data.email && data.website) {
      const firmDomain = data.website.replace(/^https?:\/\/(www\.)?/, '').split('/')[0].toLowerCase();
      const firmDomainBase = firmDomain.split('.').slice(-2).join('.'); // e.g., "mclane.com"
      const emailLinks = document.querySelectorAll('a[href^="mailto:"]');
      for (const link of emailLinks) {
        const email = link.getAttribute('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
        if (!email || !email.includes('@') || email.includes('bestlawyers.com')) continue;
        const emailDomain = email.split('@')[1];
        if (emailDomain && (emailDomain === firmDomainBase || emailDomain.endsWith('.' + firmDomainBase))) {
          data.email = email;
          break;
        }
      }
    }

    // Website fallback — look for "Visit Website" link (not "Visit Firm Profile")
    if (!data.website) {
      const allLinks = document.querySelectorAll('a[href]');
      for (const link of allLinks) {
        const href = link.getAttribute('href') || '';
        const text = (link.textContent || '').toLowerCase().trim();
        // Match "Visit Website" but not "Visit Firm Profile" (which points to bestlawyers.com)
        if (text === 'visit website' &&
            href.startsWith('http') && !href.includes('bestlawyers.com')) {
          data.website = href;
          break;
        }
      }
    }

    // Name fallback from page heading
    if (!data.name) {
      const h1 = document.querySelector('h1');
      if (h1) data.name = h1.textContent.trim();
    }

    // Firm fallback
    if (!data.firm) {
      const firmLink = document.querySelector('a[href*="/firm/"]');
      if (firmLink) data.firm = firmLink.textContent.trim();
    }

    // Location fallback
    if (!data.city || !data.state) {
      const locMatch = bodyText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\s+(\d{5})/);
      if (locMatch) {
        if (!data.city) data.city = locMatch[1];
        if (!data.state) data.state = locMatch[2];
        if (!data.zip) data.zip = locMatch[3];
      }
    }

    return data;
  });
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const allStatesMode = args.includes('--all-states');
  const noResume = args.includes('--no-resume');
  const noProfiles = args.includes('--no-profiles');
  let statesFilter = null;

  // Parse --states flag
  const statesArg = args.find(a => a.startsWith('--states='));
  if (statesArg) {
    statesFilter = statesArg.split('=')[1].split(',').map(s => s.trim().toUpperCase());
  } else {
    const statesIdx = args.indexOf('--states');
    if (statesIdx >= 0 && args[statesIdx + 1]) {
      statesFilter = args[statesIdx + 1].split(',').map(s => s.trim().toUpperCase());
    }
  }

  let states;
  if (statesFilter) {
    states = statesFilter;
  } else if (allStatesMode) {
    states = ALL_STATES;
  } else if (isTest) {
    states = ['NH', 'VT'];  // Small states for quick testing
  } else {
    states = DEFAULT_STATES;
  }

  const maxPagesPerState = isTest ? 2 : MAX_PAGES_DEFAULT;
  const maxProfilesPerState = isTest ? 5 : 999;

  log('========================================');
  log('  Best Lawyers Immigration Scraper');
  log('========================================');
  log(`States: ${states.length} (${states.join(', ')})`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Max pages/state: ${maxPagesPerState}`);
  log(`Profile scraping: ${noProfiles ? 'DISABLED' : 'ENABLED'}`);

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Resume support
  let allLeads = [];
  let seenProfileUrls = new Set();
  let statesCompleted = new Set();

  if (!noResume) {
    const existing = loadExistingCSV(OUTPUT_FILE);
    statesCompleted = loadProgress();
    if (existing.leads.length > 0) {
      allLeads = existing.leads;
      seenProfileUrls = existing.seenProfileUrls;
      log(`RESUME: loaded ${allLeads.length} existing leads from CSV`);
      log(`RESUME: states already done: ${[...statesCompleted].join(', ') || 'none'}`);
    }
  } else {
    if (fs.existsSync(PROGRESS_FILE)) fs.unlinkSync(PROGRESS_FILE);
  }

  // Launch browser
  log('Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--window-size=1920,1080',
    ],
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Block images/fonts/media for faster loading
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (['image', 'font', 'media'].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  // Warm up session
  log('Warming up session...');
  try {
    await page.goto('https://www.bestlawyers.com', { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
    await sleep(3000);
    const title = await page.title();
    log(`Session warmup: page title = "${title}"`);
    if (title.includes('Just a moment') || title.includes('Attention Required')) {
      log('Cloudflare detected on warmup — waiting 15s...');
      await sleep(15000);
    }
  } catch (err) {
    log(`Warmup error (non-fatal): ${err.message}`);
  }

  let totalStatesProcessed = 0;

  for (const stateCode of states) {
    // Resume — skip already-completed states
    if (!noResume && statesCompleted.has(stateCode)) {
      log(`\nSkipping ${stateCode} (already completed)`);
      continue;
    }

    if (!STATE_SLUGS[stateCode]) {
      log(`Unknown state code: ${stateCode}`);
      continue;
    }

    totalStatesProcessed++;
    log(`\n${'='.repeat(50)}`);
    log(`State ${totalStatesProcessed}/${states.length}: ${stateCode} (${STATE_SLUGS[stateCode]})`);

    // Phase 1: Scrape listing pages to collect profile URLs
    const listings = await scrapeStateListings(page, stateCode, maxPagesPerState);
    log(`  Found ${listings.length} lawyers on listing pages`);

    if (listings.length === 0) {
      statesCompleted.add(stateCode);
      saveProgress(statesCompleted);
      continue;
    }

    // Phase 2: Visit profile pages for detailed data
    let stateLeadCount = 0;
    const profileQueue = listings.filter(l => !seenProfileUrls.has(l.profileUrl));
    const profilesToVisit = noProfiles ? [] : profileQueue.slice(0, maxProfilesPerState);

    if (!noProfiles && profilesToVisit.length > 0) {
      log(`  Visiting ${profilesToVisit.length} profile pages for detailed data...`);

      for (let i = 0; i < profilesToVisit.length; i++) {
        const listing = profilesToVisit[i];
        if (seenProfileUrls.has(listing.profileUrl)) continue;

        try {
          await page.goto(listing.profileUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
          await sleep(1500); // Let JS render

          // Check for Cloudflare
          const title = await page.title();
          if (title.includes('Just a moment')) {
            await sleep(8000);
            const stillBlocked = await page.evaluate(() => document.title.includes('Just a moment'));
            if (stillBlocked) {
              log(`    BLOCKED on profile ${i + 1}/${profilesToVisit.length} — falling back to listing data`);
              // Fall back to listing data
              const { first_name, last_name } = parseName(listing.name);
              allLeads.push({
                first_name,
                last_name,
                firm_name: listing.firm || '',
                title: '',
                email: '',
                phone: '',
                website: '',
                domain: '',
                city: listing.city || '',
                state: listing.state || stateCode,
                country: 'US',
                niche: 'immigration',
                source: 'best_lawyers',
                profile_url: listing.profileUrl,
              });
              seenProfileUrls.add(listing.profileUrl);
              stateLeadCount++;
              continue;
            }
          }

          const profileData = await extractProfileData(page);

          // Merge listing data with profile data (profile takes priority)
          const name = profileData.name || listing.name;
          const { first_name, last_name } = parseName(name);
          const website = profileData.website || '';
          const rawState = profileData.state || listing.state || stateCode;

          allLeads.push({
            first_name,
            last_name,
            firm_name: profileData.firm || listing.firm || '',
            title: profileData.title || '',
            email: profileData.email || '',
            phone: cleanPhone(profileData.phone),
            website,
            domain: extractDomain(website),
            city: profileData.city || listing.city || '',
            state: normalizeStateCode(rawState) || stateCode,
            country: 'US',
            niche: 'immigration',
            source: 'best_lawyers',
            profile_url: listing.profileUrl,
          });
          seenProfileUrls.add(listing.profileUrl);
          stateLeadCount++;

          if ((i + 1) % 10 === 0 || i === profilesToVisit.length - 1) {
            log(`    Profiles: ${i + 1}/${profilesToVisit.length} visited, ${stateLeadCount} leads`);
          }

        } catch (err) {
          log(`    ERROR on profile ${i + 1}: ${err.message}`);
          // Fall back to listing data
          const { first_name, last_name } = parseName(listing.name);
          allLeads.push({
            first_name,
            last_name,
            firm_name: listing.firm || '',
            title: '',
            email: '',
            phone: '',
            website: '',
            domain: '',
            city: listing.city || '',
            state: listing.state || stateCode,
            country: 'US',
            niche: 'immigration',
            source: 'best_lawyers',
            profile_url: listing.profileUrl,
          });
          seenProfileUrls.add(listing.profileUrl);
          stateLeadCount++;
        }

        // Rate limit between profile visits
        await sleep(DELAY_PROFILE + Math.random() * 1000);
      }
    }

    // Add remaining lawyers from listings (those not visited via profile)
    for (const listing of listings) {
      if (seenProfileUrls.has(listing.profileUrl)) continue;
      seenProfileUrls.add(listing.profileUrl);

      const { first_name, last_name } = parseName(listing.name);
      allLeads.push({
        first_name,
        last_name,
        firm_name: listing.firm || '',
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: listing.city || '',
        state: listing.state || stateCode,
        country: 'US',
        niche: 'immigration',
        source: 'best_lawyers',
        profile_url: listing.profileUrl,
      });
      stateLeadCount++;
    }

    log(`  ${stateCode} total: ${stateLeadCount} new leads`);

    // Mark state as completed and save progress
    statesCompleted.add(stateCode);
    saveProgress(statesCompleted);

    // Incremental save after each state
    writeCSV(allLeads, OUTPUT_FILE);
    log(`  Saved ${allLeads.length} total leads to CSV`);

    // Delay between states
    if (totalStatesProcessed < states.length) {
      await sleep(DELAY_BETWEEN_STATES + Math.random() * 3000);
    }
  }

  await browser.close();

  // Final write
  writeCSV(allLeads, OUTPUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`States processed: ${totalStatesProcessed}`);
  log(`With full name: ${withName}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With firm name: ${withFirm}`);
  log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
