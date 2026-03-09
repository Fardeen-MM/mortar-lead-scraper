#!/usr/bin/env node
/**
 * Legal 500 Immigration Lawyers Scraper
 *
 * Scrapes https://www.legal500.com immigration rankings for US and UK.
 * Extracts both ranked firms (with contact info from profile pages) and
 * individually ranked lawyers (Hall of Fame, Leading Partners, etc.).
 *
 * HTML Structure (Next.js server-rendered):
 *   Firms page: LI elements containing:
 *     - <a href="/rankings/ranking/.../{firmId}"> with firm name (prefixed by tier digit)
 *     - <a href="/firms/{id}-{slug}/...">Firm profile</a> (separate link)
 *   Lawyers page: H2 category headings, then H3 elements for each lawyer name.
 *     - Firm name in <span> within the same card div
 *     - Some lawyers have <a href="/firms/.../lawyers/{id}"> profile links
 *
 * Pages scraped:
 *   US: /c/united-states/labor-and-employment/immigration (firms + lawyers)
 *   UK: /c/london/employment/immigration-personal (firms + lawyers)
 *   UK: /c/london/employment/immigration-business (firms + lawyers)
 *
 * Usage:
 *   node scripts/scrape-legal500-immigration.js --test              # Test (3 firm contacts)
 *   node scripts/scrape-legal500-immigration.js --country US        # US only
 *   node scripts/scrape-legal500-immigration.js --country UK        # UK only
 *   node scripts/scrape-legal500-immigration.js --country ALL       # US + UK (default)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;
const BASE_URL = 'https://www.legal500.com';

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-legal500.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── Ranking Pages ────────────────────────────────────────────────────────────

const RANKING_PAGES = {
  US: [
    {
      label: 'US Immigration (National)',
      firmsUrl: '/c/united-states/labor-and-employment/immigration',
      lawyersUrl: '/c/united-states/labor-and-employment/immigration/lawyers',
      country: 'US',
      city: '',
      state: '',
    },
  ],
  UK: [
    {
      label: 'UK London Immigration: Personal',
      firmsUrl: '/c/london/employment/immigration-personal',
      lawyersUrl: '/c/london/employment/immigration-personal/lawyers',
      country: 'UK',
      city: 'London',
      state: '',
    },
    {
      label: 'UK London Immigration: Business',
      firmsUrl: '/c/london/employment/immigration-business',
      lawyersUrl: '/c/london/employment/immigration-business/lawyers',
      country: 'UK',
      city: 'London',
      state: '',
    },
  ],
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
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

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function dedupKey(firstName, lastName, firmName) {
  return `${(firstName || '').toLowerCase().trim()}|${(lastName || '').toLowerCase().trim()}|${(firmName || '').toLowerCase().trim()}`;
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  return rawPhone.replace(/\s+/g, ' ').trim();
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const fullUrl = url.startsWith('http') ? url : `${BASE_URL}${url}`;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = https.get(fullUrl, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `${BASE_URL}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 8000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(fullUrl, retries - 1)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(5000).then(() => httpGet(fullUrl, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(5000).then(() => httpGet(fullUrl, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Parsers ──────────────────────────────────────────────────────────────────

// Badge text to strip from firm names
const BADGE_TEXTS = [
  'Lawyer & team quality', 'Billing & efficiency', 'Sector knowledge',
  'NPS', 'NPS ®', 'Legal 500 Live', '®',
];

/**
 * Parse the firms ranking page.
 *
 * Structure: Each firm is in an <li> containing:
 *   - <a href="/rankings/ranking/.../{firmId}"> with text: "{tierDigit}{FirmName}{badges}"
 *   - <a href="/firms/{id}-{slug}/...">Firm profile</a>
 *
 * Firms without a Legal 500 profile have no /firms/ link but still have the
 * /rankings/ranking/ link.
 */
function parseFirmsPage(html) {
  const $ = cheerio.load(html);
  const firms = [];
  const seenFirms = new Set();

  $('li').each((_, li) => {
    const $li = $(li);

    // Look for ranking link: /rankings/ranking/c-united-states/.../
    const rankLink = $li.find('a[href*="/rankings/ranking/"]').first();
    if (!rankLink.length) return;

    // Extract firm name from ranking link text
    let linkText = rankLink.text().trim();
    if (!linkText) return;

    // Strip badge texts
    for (const badge of BADGE_TEXTS) {
      linkText = linkText.replace(new RegExp(badge.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'), '');
    }
    linkText = linkText.trim();

    // Extract leading tier digit(s)
    let tier = '';
    const tierMatch = linkText.match(/^(\d+)/);
    if (tierMatch) {
      tier = tierMatch[1];
      linkText = linkText.substring(tier.length).trim();
    }

    // Clean up remaining special chars (® sometimes sneaks through)
    const firmName = linkText.replace(/\s*®\s*/g, '').trim();
    if (!firmName || firmName.length < 2) return;
    if (seenFirms.has(firmName.toLowerCase())) return;
    seenFirms.add(firmName.toLowerCase());

    // Look for firm profile link
    const firmProfileLink = $li.find('a[href*="/firms/"]').first();
    let profilePath = '';
    if (firmProfileLink.length) {
      const href = firmProfileLink.attr('href') || '';
      // Extract base firm path: /firms/{id}-{slug}/{region}
      const match = href.match(/(\/firms\/\d+-[^/]+\/[^/]+)/);
      if (match) profilePath = match[1];
    }

    firms.push({
      firm_name: firmName,
      tier: tier,
      profile_path: profilePath,
    });
  });

  return firms;
}

/**
 * Parse the lawyers ranking page.
 *
 * Structure:
 *   <h2>Hall of Fame</h2>  (or Leading partners, Next Generation Partners, Leading associates)
 *   ... grid of cards ...
 *   Each card has <h3>LawyerName</h3> and <span>FirmName</span>
 *   Some cards are wrapped in <a href="/firms/.../lawyers/...">
 *
 * We walk the DOM: find each h2 category, then iterate siblings to find h3 lawyer names.
 */
function parseLawyersPage(html) {
  const $ = cheerio.load(html);
  const lawyers = [];
  const seenNames = new Set();

  // Category headings
  const CATEGORIES = ['hall of fame', 'leading partners', 'next generation partners', 'leading associates'];

  let currentCategory = '';

  $('h2').each((_, h2) => {
    const catText = $(h2).text().trim();
    const catLower = catText.toLowerCase();
    if (!CATEGORIES.some(c => catLower.includes(c))) return;

    currentCategory = catText;

    // Walk siblings after this h2 to find lawyer h3 elements
    let sibling = $(h2).next();
    while (sibling.length) {
      // Stop if we hit the next h2 category
      if (sibling.prop('tagName') === 'H2') break;

      // Find all h3 elements within this sibling (they're in grid cards)
      sibling.find('h3').each((_, h3) => {
        const name = $(h3).text().trim();
        if (!name || name.length < 3) return;
        // Skip navigation h3s
        if (/^(Rankings|Firms|In-House|Knowledge|About|Corporate)/i.test(name)) return;

        // Find firm name from span elements in the same card div
        const card = $(h3).closest('div');
        const allSpans = [];
        card.find('span').each((_, s) => {
          const t = $(s).text().trim();
          if (t && t !== name && t.length < 80) allSpans.push(t);
        });

        // Filter out badge texts to find the firm name
        const badgePatterns = ['hall of fame', 'leading partner', 'next generation', 'leading associate'];
        const firmSpans = allSpans.filter(s =>
          !badgePatterns.some(b => s.toLowerCase().includes(b))
        );
        const firmName = firmSpans[firmSpans.length - 1] || '';

        // Check for profile link
        const profileLink = $(h3).closest('a[href*="/lawyers/"]');
        let profileUrl = '';
        if (profileLink.length) {
          const href = profileLink.attr('href') || '';
          profileUrl = href.startsWith('/') ? `${BASE_URL}${href}` : href;
        }

        // Dedup by name + firm
        const key = `${name.toLowerCase()}|${firmName.toLowerCase()}`;
        if (seenNames.has(key)) return;
        seenNames.add(key);

        lawyers.push({
          full_name: name,
          firm_name: firmName,
          category: currentCategory,
          profile_url: profileUrl,
        });
      });

      sibling = sibling.next();
    }
  });

  return lawyers;
}

/**
 * Parse a firm's contact page for phone, email, website, city.
 *
 * Contact pages have:
 *   - <a href="tel:..."> for phone
 *   - <a href="mailto:..."> for email
 *   - External website link (text contains domain, href is external)
 *   - Address in <p> elements
 */
function parseFirmContactPage(html) {
  const $ = cheerio.load(html);
  const contact = { phone: '', email: '', website: '', city: '' };

  // Phone
  $('a[href^="tel:"]').each((_, el) => {
    if (!contact.phone) {
      contact.phone = $(el).text().trim() || $(el).attr('href').replace('tel:', '');
    }
  });

  // Email
  $('a[href^="mailto:"]').each((_, el) => {
    const email = $(el).attr('href').replace('mailto:', '').split('?')[0].trim();
    if (!contact.email && email) {
      contact.email = email;
    }
  });

  // Website — look for external links that aren't legal500/social
  const skipDomains = ['legal500.com', 'legalbusiness.co.uk', 'linkedin.com', 'twitter.com',
    'facebook.com', 'instagram.com', 'youtube.com', 'legal500.de', 'legal500.fr',
    'screenloop.com'];

  $('a[href^="http"]').each((_, el) => {
    if (contact.website) return;
    const href = $(el).attr('href') || '';
    const text = $(el).text().trim().toLowerCase();
    // Skip navigation/social links
    if (skipDomains.some(d => href.includes(d))) return;
    // Website links usually have domain-like text or the href is to a law firm domain
    if (text.includes('.com') || text.includes('.co.') || text.includes('.law') ||
        text.includes('.org') || text.includes('.net') || text.includes('.io') ||
        text.includes('.us') || text.includes('.uk')) {
      contact.website = href;
    }
  });

  // City — extract from address <p> elements near office headings
  // Legal 500 contact pages have office cards with city names in headings or address blocks
  // We look at non-footer content to avoid picking up "London" from Legal 500's own address
  const mainContent = $('main').length ? $('main').text() : '';
  // Look for the first office location text (often an "eyebrow" label)
  // The address cards typically have the city as a label above the address lines
  const cityPatterns = [
    { re: /\bNew York\b/i, city: 'New York' },
    { re: /\bWashington\b/i, city: 'Washington DC' },
    { re: /\bChicago\b/i, city: 'Chicago' },
    { re: /\bLos Angeles\b/i, city: 'Los Angeles' },
    { re: /\bSan Francisco\b/i, city: 'San Francisco' },
    { re: /\bHouston\b/i, city: 'Houston' },
    { re: /\bBoston\b/i, city: 'Boston' },
    { re: /\bAtlanta\b/i, city: 'Atlanta' },
    { re: /\bDallas\b/i, city: 'Dallas' },
    { re: /\bMiami\b/i, city: 'Miami' },
    { re: /\bPhiladelphia\b/i, city: 'Philadelphia' },
    { re: /\bLondon\b/i, city: 'London' },
    { re: /\bManchester\b/i, city: 'Manchester' },
    { re: /\bBirmingham\b/i, city: 'Birmingham' },
  ];

  if (mainContent) {
    for (const { re, city } of cityPatterns) {
      if (re.test(mainContent)) {
        contact.city = city;
        break;
      }
    }
  }

  return contact;
}

/**
 * Parse a firm's lawyers page to find immigration-practice lawyers.
 */
function parseFirmLawyersPage(html) {
  const $ = cheerio.load(html);
  const lawyers = [];

  $('a[href*="/lawyers/"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const $el = $(el);
    const allText = $el.text().trim();
    if (!allText) return;

    const h3 = $el.find('h3').first().text().trim();
    // Look for subtitle/position text in p or span
    let subtitle = '';
    $el.find('p, span').each((_, sub) => {
      const t = $(sub).text().trim();
      if (t && t !== h3 && t.length > 3 && t.length < 100) {
        subtitle = t;
      }
    });

    const name = h3 || allText.split('\n')[0]?.trim();
    if (!name || name.length < 3) return;

    // Only include if related to immigration
    if (subtitle && /immigration/i.test(subtitle)) {
      lawyers.push({
        full_name: name,
        title: subtitle,
        profile_url: href.startsWith('/') ? `${BASE_URL}${href}` : href,
      });
    }
  });

  return lawyers;
}

// ── Scraping Logic ───────────────────────────────────────────────────────────

/**
 * Scrape a single ranking page (firms + lawyers).
 */
async function scrapeRankingPage(pageInfo, seenKeys, firmContactCache, isTest) {
  const results = [];

  // ── Step 1: Scrape firms page ──────────────────────────────────────────
  log(`\n  [Firms] ${pageInfo.label}`);
  log(`  URL: ${BASE_URL}${pageInfo.firmsUrl}`);

  const firmsResp = await httpGet(pageInfo.firmsUrl);
  if (firmsResp.statusCode !== 200) {
    log(`  ERROR: HTTP ${firmsResp.statusCode} for firms page`);
  } else {
    const firms = parseFirmsPage(firmsResp.body);
    log(`  Found ${firms.length} firms (${firms.filter(f => f.profile_path).length} with profiles)`);

    let firmCount = 0;
    let contactsFetched = 0;

    for (const firm of firms) {
      const key = dedupKey('', '', firm.firm_name);
      if (seenKeys.has(key)) continue;

      let contact = { phone: '', email: '', website: '', city: '' };

      // Fetch contact page if we have a profile path
      if (firm.profile_path) {
        // Check cache first
        if (firmContactCache.has(firm.profile_path)) {
          contact = firmContactCache.get(firm.profile_path);
        } else if (!isTest || contactsFetched < 3) {
          const contactUrl = `${firm.profile_path}/contact`;
          log(`    Fetching contact: ${firm.firm_name}`);
          await randomDelay();

          try {
            const contactResp = await httpGet(contactUrl);
            if (contactResp.statusCode === 200) {
              contact = parseFirmContactPage(contactResp.body);
              firmContactCache.set(firm.profile_path, contact);
              contactsFetched++;
              const parts = [];
              if (contact.phone) parts.push(`phone=${contact.phone}`);
              if (contact.email) parts.push(`email=${contact.email}`);
              if (contact.website) parts.push(`web=${extractDomain(contact.website)}`);
              if (contact.city) parts.push(`city=${contact.city}`);
              if (parts.length) log(`      Got: ${parts.join(', ')}`);
              else log(`      No contact info found`);
            }
          } catch (err) {
            log(`      Contact fetch error: ${err.message}`);
          }

          // Also fetch firm's lawyers page to find immigration lawyers
          if (!isTest) {
            const lawyersUrl = `${firm.profile_path}/lawyers`;
            await randomDelay();
            try {
              const lawyersResp = await httpGet(lawyersUrl);
              if (lawyersResp.statusCode === 200) {
                const firmLawyers = parseFirmLawyersPage(lawyersResp.body);
                for (const fl of firmLawyers) {
                  const { first_name, last_name } = splitName(fl.full_name);
                  const lKey = dedupKey(first_name, last_name, firm.firm_name);
                  if (!seenKeys.has(lKey)) {
                    seenKeys.add(lKey);
                    results.push({
                      first_name,
                      last_name,
                      firm_name: firm.firm_name,
                      title: fl.title || '',
                      email: '',
                      phone: '',
                      website: contact.website || '',
                      domain: extractDomain(contact.website),
                      city: contact.city || pageInfo.city,
                      state: pageInfo.state,
                      country: pageInfo.country,
                      niche: 'immigration',
                      source: 'legal500',
                      profile_url: fl.profile_url,
                    });
                  }
                }
                if (firmLawyers.length > 0) {
                  log(`      Found ${firmLawyers.length} immigration lawyers at firm`);
                }
              }
            } catch (err) {
              // Silently skip
            }
          }
        }
      }

      // Add the firm as a lead
      seenKeys.add(key);
      results.push({
        first_name: '',
        last_name: '',
        firm_name: firm.firm_name,
        title: firm.tier ? `Tier ${firm.tier}` : '',
        email: contact.email || '',
        phone: formatPhone(contact.phone),
        website: contact.website || '',
        domain: extractDomain(contact.website),
        city: contact.city || pageInfo.city,
        state: pageInfo.state,
        country: pageInfo.country,
        niche: 'immigration',
        source: 'legal500',
        profile_url: firm.profile_path ? `${BASE_URL}${firm.profile_path}` : '',
      });

      firmCount++;
    }
  }

  await randomDelay();

  // ── Step 2: Scrape lawyers page ────────────────────────────────────────
  log(`\n  [Lawyers] ${pageInfo.label}`);
  log(`  URL: ${BASE_URL}${pageInfo.lawyersUrl}`);

  const lawyersResp = await httpGet(pageInfo.lawyersUrl);
  if (lawyersResp.statusCode !== 200) {
    log(`  ERROR: HTTP ${lawyersResp.statusCode} for lawyers page`);
  } else {
    const lawyers = parseLawyersPage(lawyersResp.body);
    log(`  Found ${lawyers.length} lawyers`);

    let newLawyers = 0;
    for (const lawyer of lawyers) {
      const { first_name, last_name } = splitName(lawyer.full_name);
      const key = dedupKey(first_name, last_name, lawyer.firm_name);

      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      // Map category to a title
      let title = '';
      const cat = (lawyer.category || '').toLowerCase();
      if (cat.includes('hall of fame')) title = 'Hall of Fame';
      else if (cat.includes('leading partner')) title = 'Leading Partner';
      else if (cat.includes('next generation')) title = 'Next Generation Partner';
      else if (cat.includes('leading associate')) title = 'Leading Associate';

      // Check if we have firm contact info from earlier scrape
      const existingFirm = results.find(r =>
        r.firm_name.toLowerCase() === lawyer.firm_name.toLowerCase() && !r.first_name
      );

      results.push({
        first_name,
        last_name,
        firm_name: lawyer.firm_name,
        title,
        email: '',
        phone: '',
        website: existingFirm?.website || '',
        domain: existingFirm?.domain || '',
        city: existingFirm?.city || pageInfo.city,
        state: pageInfo.state,
        country: pageInfo.country,
        niche: 'immigration',
        source: 'legal500',
        profile_url: lawyer.profile_url || '',
      });
      newLawyers++;
    }

    log(`  Added ${newLawyers} new lawyers (${lawyers.length - newLawyers} dupes)`);
  }

  return results;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const countryIdx = args.indexOf('--country');
  const countryFilter = countryIdx >= 0 ? (args[countryIdx + 1] || 'ALL').toUpperCase() : 'ALL';

  if (args.includes('--help') || args.includes('-h')) {
    console.log('Legal 500 Immigration Lawyers Scraper');
    console.log('');
    console.log('Usage:');
    console.log('  node scripts/scrape-legal500-immigration.js --test              # Test (limited contacts)');
    console.log('  node scripts/scrape-legal500-immigration.js --country US        # US only');
    console.log('  node scripts/scrape-legal500-immigration.js --country UK        # UK only');
    console.log('  node scripts/scrape-legal500-immigration.js --country ALL       # US + UK (default)');
    console.log('');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  // Build list of pages to scrape
  let pages = [];
  if (countryFilter === 'US' || countryFilter === 'ALL') {
    pages.push(...RANKING_PAGES.US);
  }
  if (countryFilter === 'UK' || countryFilter === 'ALL') {
    pages.push(...RANKING_PAGES.UK);
  }

  log('='.repeat(60));
  log('Legal 500 Immigration Lawyers Scraper');
  log(`Country: ${countryFilter} | Pages: ${pages.length} | Test: ${isTest}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));

  const seenKeys = new Set();
  const firmContactCache = new Map(); // cache firm contact info across pages
  const allLeads = [];
  let totalFirms = 0;
  let totalLawyers = 0;
  let errors = 0;

  for (const pageInfo of pages) {
    log(`\n${'─'.repeat(50)}`);
    log(`Scraping: ${pageInfo.label}`);
    log(`${'─'.repeat(50)}`);

    try {
      const results = await scrapeRankingPage(pageInfo, seenKeys, firmContactCache, isTest);

      const firms = results.filter(r => !r.first_name && !r.last_name);
      const lawyers = results.filter(r => r.first_name || r.last_name);

      totalFirms += firms.length;
      totalLawyers += lawyers.length;
      allLeads.push(...results);

      log(`\n  Subtotal: ${firms.length} firms, ${lawyers.length} individual lawyers`);

      // Save progress after each ranking page
      writeCSV(allLeads, OUT_FILE);
      log(`  [Progress saved: ${allLeads.length} total leads]`);

    } catch (err) {
      log(`  ERROR: ${err.message}`);
      errors++;
    }

    await randomDelay();
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const usLeads = allLeads.filter(l => l.country === 'US').length;
  const ukLeads = allLeads.filter(l => l.country === 'UK').length;
  const withTitle = allLeads.filter(l => l.title).length;

  log('');
  log('='.repeat(60));
  log('DONE');
  log('='.repeat(60));
  log(`Total leads: ${allLeads.length}`);
  log(`  US: ${usLeads} | UK: ${ukLeads}`);
  log(`  Firms: ${totalFirms} | Individual lawyers: ${totalLawyers}`);
  log(`  With name: ${withName}`);
  log(`  With title/category: ${withTitle}`);
  log(`  With phone: ${withPhone}`);
  log(`  With email: ${withEmail}`);
  log(`  With website: ${withWebsite}`);
  log(`  Errors: ${errors}`);
  log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
