#!/usr/bin/env node
/**
 * UK ILPA (Immigration Law Practitioners' Association) Member Directory Scraper
 *
 * ILPA's public member directory has been taken offline (returns 404 as of 2026).
 * However, the Wayback Machine has cached snapshots from July 2023 and December 2023.
 *
 * Strategy:
 *   1. Scrape listing pages from Wayback Machine archive (6 pages x 50 members)
 *   2. For each member, fetch the detail page from Wayback Machine for extra fields
 *   3. Parse: name, firm, type/title, address, phone, email, website, work covered, languages
 *   4. Deduplicate by Salesforce ID
 *   5. Output to CSV
 *
 * Data source: https://web.archive.org/web/20230703012202/https://ilpa.org.uk/members-directory/
 *
 * Usage:
 *   node scripts/scrape-uk-ilpa.js                    # Full scrape (listings + details)
 *   node scripts/scrape-uk-ilpa.js --test             # Quick test (first 2 pages, max 5 details)
 *   node scripts/scrape-uk-ilpa.js --listings-only    # Skip detail page fetches
 *   node scripts/scrape-uk-ilpa.js --max-details 20   # Limit number of detail page fetches
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// === Config ===
const WB_BASE = 'https://web.archive.org/web/20230703012202';
const ILPA_DIR_URL = 'https://ilpa.org.uk/members-directory/';
const DETAIL_BASE = 'https://ilpa.org.uk/members-directory/member/';
const DELAY_MS = 2500;       // Polite delay between Wayback Machine requests
const DETAIL_DELAY_MS = 2000; // Delay between detail page fetches
const MAX_PAGES = 10;         // Safety limit for listing pages
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-ilpa-immigration-practitioners.csv');

// === CLI Args ===
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const listingsOnly = args.includes('--listings-only');
const maxDetailsArg = args.includes('--max-details')
  ? parseInt(args[args.indexOf('--max-details') + 1], 10)
  : Infinity;
const maxDetails = testMode ? 5 : maxDetailsArg;
const maxListingPages = testMode ? 2 : MAX_PAGES;

// === Helpers ===

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * HTTP(S) GET with redirect following and Wayback Machine handling.
 */
function httpGet(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    if (maxRedirects <= 0) return reject(new Error('Too many redirects'));

    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
      },
    };

    const req = lib.request(options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redir = res.headers.location;
        if (redir.startsWith('/')) redir = `${parsed.protocol}//${parsed.hostname}${redir}`;
        return httpGet(redir, maxRedirects - 1).then(resolve, reject);
      }

      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, headers: res.headers }));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error(`Timeout: ${url}`));
    });
    req.end();
  });
}

/**
 * Fetch a URL from the Wayback Machine, trying multiple timestamps.
 */
async function fetchFromWayback(originalUrl, timestamps = ['20230703012202', '20231218005055', '20231203']) {
  for (const ts of timestamps) {
    const wbUrl = `https://web.archive.org/web/${ts}/${originalUrl}`;
    try {
      const res = await httpGet(wbUrl);
      if (res.status === 200 && res.body.length > 1000) {
        return res.body;
      }
    } catch (err) {
      // Try next timestamp
    }
  }
  return null;
}

/**
 * Parse address into city and postcode.
 * UK postcodes follow patterns like "SW1A 1AA", "EC1Y 4AG", "LS7 1AB".
 */
function parseUKAddress(address) {
  if (!address) return { city: '', postcode: '', fullAddress: '' };

  const cleaned = address.replace(/\n/g, ', ').replace(/\s+/g, ' ').trim();

  // Extract UK postcode
  const postcodeMatch = cleaned.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\s*$/i);
  const postcode = postcodeMatch ? postcodeMatch[1].trim().toUpperCase() : '';

  // City is typically the second-to-last comma-separated part (before postcode)
  const parts = cleaned.split(',').map(p => p.trim()).filter(Boolean);
  let city = '';
  if (parts.length >= 2) {
    // Usually: "street, city, postcode" or "line1, line2, city, postcode"
    const lastPart = parts[parts.length - 1].trim();
    const postcodeInLast = /[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}/i.test(lastPart);
    if (postcodeInLast && parts.length >= 2) {
      city = parts[parts.length - 2].trim();
    } else {
      city = parts[parts.length - 1].trim();
    }
  } else if (parts.length === 1) {
    city = parts[0].replace(/[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}/i, '').trim();
  }

  // Clean up city (remove postcode if still present)
  city = city.replace(/[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}/i, '').trim();
  // Remove trailing commas
  city = city.replace(/,\s*$/, '').trim();
  // Title case normalization (LONDON → London, etc.)
  if (city && city === city.toUpperCase() && city.length > 2) {
    city = city.charAt(0).toUpperCase() + city.slice(1).toLowerCase();
  }

  return { city, postcode, fullAddress: cleaned };
}

/**
 * Split a member name into first/last and firm.
 * Names like "Smith, John" are individuals → first=John, last=Smith
 * Names like "A Kay Pietron & Paluch Solicitors LLP" are firms → firm_name
 *
 * memberType is the ILPA member type string (e.g., "Solicitors firm with 1 to 5 fee earners").
 * If the type indicates a firm/org/chambers, treat the name as a firm name.
 */
function parseMemberName(rawName, memberType = '') {
  if (!rawName) return { first_name: '', last_name: '', firm_name: '' };

  const name = rawName.trim();
  const mtype = (memberType || '').toLowerCase();

  // If the member type says it's an org/firm/chambers, the name IS the firm name
  const orgTypeIndicators = [
    /firm/i, /chambers/i, /organisation/i, /organization/i,
    /not.for.profit/i, /oisc regulated organisation/i,
    /\d+\s+(fee earners|barristers|advisers)/i,
  ];
  const isOrgByType = orgTypeIndicators.some(re => re.test(mtype));
  if (isOrgByType) {
    return { first_name: '', last_name: '', firm_name: name };
  }

  // Check if it's a "Last, First" or "Last,First" format (individual)
  const commaMatch = name.match(/^([^,]+),\s*(.+)$/);
  if (commaMatch) {
    const last = commaMatch[1].trim();
    const first = commaMatch[2].trim();
    // Only treat as individual if the parts look like person names (not firm parts)
    // e.g., "Smith, John" is individual, "Bates, Wells & Braithwaite" is a firm
    const firmAfterComma = /[&]|solicitors|llp|ltd|chambers/i.test(first);
    if (!firmAfterComma) {
      return { first_name: first, last_name: last, firm_name: '' };
    }
  }

  // Firm indicators in the name itself
  const firmIndicators = [
    /\bllp\b/i, /\blp\b/i, /\bltd\b/i, /\blimited\b/i,
    /\bsolicitors?\b/i, /\bchambers?\b/i, /\bpartners?\b/i,
    /\b&\b/, /\blaw\s+(centre|center|firm|group|practice|society)\b/i,
    /\bconsultancy\b/i, /\bservices?\b/i, /\bassociates?\b/i,
    /\binternational\b/i, /\blegal\b/i, /\badvocates?\b/i,
    /\bcentre\b/i, /\bproject\b/i, /\bcouncil\b/i,
    /\bimmigration\b/i, /\bvisa\b/i, /\bconsulting\b/i,
    /\bthe\b/i,  // "The Aire Centre" etc.
  ];

  const isFirm = firmIndicators.some(re => re.test(name));
  if (isFirm) {
    return { first_name: '', last_name: '', firm_name: name };
  }

  // If the member type says it's an individual (solicitor, barrister, etc.)
  // then try to split the name
  const individualTypeIndicators = [
    /\bsolicitor\b/i, /\bbarrister\b/i, /\badviser\b/i,
    /\bacademic\b/i, /\bstudent\b/i, /\bother\b/i,
    /\byears?\b/i, /\bcall\b/i, /\broll\b/i,
  ];
  const isIndividualByType = individualTypeIndicators.some(re => re.test(mtype));

  if (isIndividualByType) {
    // Split by space - for names without comma format
    const parts = name.split(/\s+/);
    if (parts.length >= 2) {
      return {
        first_name: parts[0],
        last_name: parts.slice(1).join(' '),
        firm_name: '',
      };
    }
    return { first_name: name, last_name: '', firm_name: '' };
  }

  // Fallback: treat as firm name (safer for ILPA directory)
  return { first_name: '', last_name: '', firm_name: name };
}

/**
 * Determine title from member type string.
 */
function parseTitle(typeStr) {
  if (!typeStr) return '';
  const t = typeStr.toLowerCase();
  if (t.includes('barrister')) return 'Barrister';
  if (t.includes('solicitor')) return 'Solicitor';
  if (t.includes('oisc')) return 'OISC Adviser';
  if (t.includes('ilex')) return 'ILEX Adviser';
  if (t.includes('academic')) return 'Academic';
  if (t.includes('student')) return 'Student';
  return typeStr.trim();
}

/**
 * Extract domain from a website URL.
 */
function extractDomain(url) {
  if (!url) return '';
  try {
    const parsed = new URL(url.startsWith('http') ? url : `https://${url}`);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

/**
 * Normalize phone number for UK format.
 */
function normalizePhone(phone) {
  if (!phone) return '';
  let p = phone.replace(/[^\d+]/g, '');
  // Normalize UK prefixes
  if (p.startsWith('44')) p = '0' + p.slice(2);
  if (p.startsWith('+44')) p = '0' + p.slice(3);
  // Format landlines
  if (p.length === 11 && p.startsWith('0')) {
    if (p.startsWith('020')) {
      return `${p.slice(0, 3)} ${p.slice(3, 7)} ${p.slice(7)}`;
    }
    return `${p.slice(0, 4)} ${p.slice(4, 7)} ${p.slice(7)}`;
  }
  return phone.trim();
}

// =====================================================================
// Phase 1: Scrape listing pages
// =====================================================================

async function scrapeListingPage(pageNum) {
  const url = pageNum === 1
    ? ILPA_DIR_URL
    : `${ILPA_DIR_URL}?page-number=${pageNum}`;

  log(`Fetching listing page ${pageNum}: ${url}`);
  const html = await fetchFromWayback(url);
  if (!html) {
    log(`  No Wayback snapshot for page ${pageNum}`);
    return [];
  }

  const $ = cheerio.load(html);
  const members = [];

  // Extract total count
  const totalText = $('.m23__results-total').text();
  const totalMatch = totalText.match(/(\d+)\s+items?\s+of\s+(\d+)/i);
  if (totalMatch) {
    log(`  Page ${pageNum}: ${totalMatch[1]} items displayed, ${totalMatch[2]} total`);
  }

  $('article.single-member-item').each((i, el) => {
    const name = $(el).find('.single-member-item__heading, h2').text().trim();
    const type = $(el).find('.no-list i').text().trim();
    const address = $(el).find('.single-member-item__address').text().trim();
    const phone = $(el).find('.single-member-item__phone').text().trim();
    const profileLink = $(el).find('.single-member-item__button').attr('href') || '';

    // Extract Salesforce ID from profile link
    let sfId = '';
    const idMatch = profileLink.match(/[?&]id=([^&]+)/);
    if (idMatch) sfId = idMatch[1];

    // Sometimes WB rewrites the URL
    const cleanProfile = profileLink
      .replace(/^.*\/web\/\d+\//, '')
      .replace(/^https?:\/\/ilpa\.org\.uk\//, '');

    members.push({
      raw_name: name,
      type,
      address,
      phone: normalizePhone(phone),
      sfId,
      profile_path: cleanProfile,
    });
  });

  log(`  Found ${members.length} members on page ${pageNum}`);
  return members;
}

async function scrapeAllListings() {
  const allMembers = new Map(); // Keyed by sfId for dedup

  for (let page = 1; page <= maxListingPages; page++) {
    const members = await scrapeListingPage(page);
    if (members.length === 0) {
      log(`No more results on page ${page}, stopping.`);
      break;
    }

    for (const m of members) {
      const key = m.sfId || `${m.raw_name}|${m.address}`;
      if (!allMembers.has(key)) {
        allMembers.set(key, m);
      }
    }

    if (page < maxListingPages) {
      await sleep(DELAY_MS);
    }
  }

  log(`Total unique members from listings: ${allMembers.size}`);
  return [...allMembers.values()];
}

// =====================================================================
// Phase 2: Scrape detail pages
// =====================================================================

async function scrapeDetailPage(member) {
  if (!member.sfId) return {};

  const originalUrl = `${DETAIL_BASE}?id=${member.sfId}`;
  const html = await fetchFromWayback(originalUrl, [
    '20231218005055',
    '20230703012202',
    '20231203',
    '20230128',
    '2023',
  ]);

  if (!html) return {};

  const $ = cheerio.load(html);
  const details = {};

  // The detail page uses a dl with dt/dd pairs in .m18__detailed-list
  const dlPairs = {};
  $('dl.m18__detailed-list dt.m18__term').each((i, el) => {
    const key = $(el).text().trim();
    const val = $(el).next('dd.m18__description').text().trim();
    if (key && val) {
      dlPairs[key.toLowerCase()] = val;
    }
  });

  // Also check for h1 heading (may be more accurate than listing name)
  const detailName = $('h1.m18__heading, h1.h2.m18__heading').text().trim();
  if (detailName) details.detail_name = detailName;

  // Type from introduction
  const detailType = $('p.m18__introduction').text().trim();
  if (detailType) details.detail_type = detailType;

  // Map known field names
  if (dlPairs['address']) details.detail_address = dlPairs['address'];
  if (dlPairs['phone']) details.detail_phone = dlPairs['phone'];
  if (dlPairs['email']) details.detail_email = dlPairs['email'];
  if (dlPairs['e-mail']) details.detail_email = dlPairs['e-mail'];
  if (dlPairs['website']) details.detail_website = dlPairs['website'];
  if (dlPairs['web']) details.detail_website = dlPairs['web'];
  if (dlPairs['immigration work covered']) details.work_covered = dlPairs['immigration work covered'];
  if (dlPairs['immigration work covered - other']) {
    const extra = dlPairs['immigration work covered - other'];
    details.work_covered = details.work_covered
      ? `${details.work_covered}; ${extra}`
      : extra;
  }
  if (dlPairs['languages spoken']) details.languages = dlPairs['languages spoken'];
  if (dlPairs['legal aid']) details.legal_aid = dlPairs['legal aid'];

  // Also try to grab website from link elements
  if (!details.detail_website) {
    $('dd.m18__description a[href]').each((i, el) => {
      const href = $(el).attr('href');
      if (href && href.match(/^https?:\/\//) && !href.includes('ilpa.org') && !href.includes('archive.org')) {
        details.detail_website = href;
      }
    });
  }

  return details;
}

// =====================================================================
// Phase 3: Merge and output CSV
// =====================================================================

function escapeCSV(val) {
  if (!val) return '';
  const s = String(val).replace(/\r?\n/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function buildCSVRow(lead) {
  const columns = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
    'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
    'profile_url', 'address', 'postcode', 'member_type', 'work_covered',
    'languages', 'legal_aid', 'sf_id',
  ];
  return columns.map(c => escapeCSV(lead[c])).join(',');
}

function writeCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const header = 'first_name,last_name,firm_name,title,email,phone,website,domain,city,state,country,niche,source,profile_url,address,postcode,member_type,work_covered,languages,legal_aid,sf_id';
  const rows = leads.map(l => buildCSVRow(l));
  const csv = [header, ...rows].join('\n');

  fs.writeFileSync(OUTPUT_FILE, csv, 'utf8');
  log(`Wrote ${leads.length} leads to ${OUTPUT_FILE}`);
}

// =====================================================================
// Main
// =====================================================================

async function main() {
  log('=== ILPA Member Directory Scraper ===');
  log(`Mode: ${testMode ? 'TEST' : 'FULL'} | Details: ${listingsOnly ? 'SKIP' : `max ${maxDetails}`}`);
  log(`Source: Wayback Machine (archived July/Dec 2023)`);
  log('');

  // Phase 1: Listing pages
  log('--- Phase 1: Scraping listing pages ---');
  const rawMembers = await scrapeAllListings();

  if (rawMembers.length === 0) {
    log('No members found! The Wayback Machine snapshots may be unavailable.');
    process.exit(1);
  }

  // Phase 2: Detail pages
  let detailsFetched = 0;
  if (!listingsOnly) {
    log('');
    log('--- Phase 2: Scraping detail pages ---');
    const toFetch = rawMembers.filter(m => m.sfId).slice(0, maxDetails);
    log(`Will fetch ${toFetch.length} detail pages (of ${rawMembers.length} total members)`);

    for (const member of toFetch) {
      try {
        const details = await scrapeDetailPage(member);
        Object.assign(member, details);
        detailsFetched++;

        if (detailsFetched % 10 === 0) {
          log(`  Progress: ${detailsFetched}/${toFetch.length} detail pages`);
        }
      } catch (err) {
        log(`  Error fetching detail for ${member.raw_name}: ${err.message}`);
      }

      await sleep(DETAIL_DELAY_MS);
    }

    log(`Fetched ${detailsFetched} detail pages`);
  }

  // Phase 3: Build final leads
  log('');
  log('--- Phase 3: Building leads ---');

  const leads = rawMembers.map(m => {
    const memberType = (m.detail_type || m.type || '').trim();
    const { first_name, last_name, firm_name } = parseMemberName(m.detail_name || m.raw_name, memberType);
    const { city, postcode, fullAddress } = parseUKAddress(m.detail_address || m.address);
    const phone = normalizePhone(m.detail_phone || m.phone);
    const email = (m.detail_email || '').toLowerCase().trim();
    const website = m.detail_website || '';
    const domain = extractDomain(website);
    const title = parseTitle(m.detail_type || m.type);
    const profileUrl = m.sfId
      ? `https://ilpa.org.uk/members-directory/member/?id=${m.sfId}`
      : '';

    return {
      first_name,
      last_name,
      firm_name: firm_name || (first_name ? '' : m.raw_name), // If no firm parsed, use raw name
      title,
      email,
      phone,
      website,
      domain,
      city,
      state: '',
      country: 'UK',
      niche: 'Immigration Law',
      source: 'uk-ilpa',
      profile_url: profileUrl,
      address: fullAddress,
      postcode,
      member_type: memberType,
      work_covered: (m.work_covered || '').trim(),
      languages: (m.languages || '').trim(),
      legal_aid: (m.legal_aid || '').trim(),
      sf_id: m.sfId || '',
    };
  });

  // Phase 4: Write CSV
  log('');
  log('--- Phase 4: Writing CSV ---');
  writeCSV(leads);

  // Summary
  log('');
  log('=== Summary ===');
  log(`Total leads:       ${leads.length}`);
  log(`With phone:        ${leads.filter(l => l.phone).length}`);
  log(`With email:        ${leads.filter(l => l.email).length}`);
  log(`With website:      ${leads.filter(l => l.website).length}`);
  log(`Individuals:       ${leads.filter(l => l.first_name).length}`);
  log(`Firms/orgs:        ${leads.filter(l => l.firm_name && !l.first_name).length}`);
  log(`Detail pages:      ${detailsFetched} fetched`);
  log(`Work covered:      ${leads.filter(l => l.work_covered).length}`);
  log(`Languages:         ${leads.filter(l => l.languages).length}`);
  log(`Output file:       ${OUTPUT_FILE}`);

  // Show sample leads
  log('');
  log('--- Sample Leads ---');
  leads.slice(0, 5).forEach((l, i) => {
    const name = l.first_name
      ? `${l.first_name} ${l.last_name}`
      : l.firm_name;
    log(`  ${i + 1}. ${name} | ${l.title || 'N/A'} | ${l.city || 'N/A'} | ${l.phone || 'no phone'} | ${l.email || 'no email'}`);
  });
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
