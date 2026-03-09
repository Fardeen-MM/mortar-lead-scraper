#!/usr/bin/env node
/**
 * CILA (Canadian Immigration Lawyers Association) Directory Scraper
 *
 * Scrapes https://cila.co/directory/ — a WordPress/Elementor site with
 * server-rendered HTML, paginated via ?wpv_view_count=2510&wpv_paged=N.
 *
 * Strategy:
 *   1. Scrape all 6 directory pages to collect profile URLs + basic info
 *   2. Visit each profile page to extract email, phone, website, province, firm name
 *   3. Rate-limited: 2-3s between requests
 *   4. Incremental CSV save after each profile
 *
 * Usage:
 *   node scripts/scrape-cila.js               # Full scrape (~500 lawyers)
 *   node scripts/scrape-cila.js --test         # Test mode (page 1 only, max 5 profiles)
 *   node scripts/scrape-cila.js --profiles-only # Skip listing pages, resume from CSV
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const TOTAL_PAGES = 6;
const BASE_URL = 'https://cila.co';
const DIRECTORY_URL = `${BASE_URL}/directory/`;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'canada-immigration-lawyers-cila.csv');

// Province name → abbreviation mapping
const PROVINCE_MAP = {
  'alberta': 'AB',
  'british columbia': 'BC',
  'manitoba': 'MB',
  'new brunswick': 'NB',
  'newfoundland': 'NL',
  'newfoundland and labrador': 'NL',
  'northwest territories': 'NT',
  'nova scotia': 'NS',
  'nunavut': 'NU',
  'ontario': 'ON',
  'prince edward island': 'PE',
  'quebec': 'QC',
  'québec': 'QC',
  'saskatchewan': 'SK',
  'yukon': 'YT',
};

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

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

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: 30000,
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${BASE_URL}${res.headers.location}`;
        return httpGet(redirectUrl).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|phd|md|llm|ll\.?b\.?|q\.?c\.?|k\.?c\.?|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .trim();

  // Handle "Last, First" format
  if (cleaned.includes(',')) {
    const [last, ...rest] = cleaned.split(',').map(s => s.trim());
    const first = rest.join(' ').split(/\s+/)[0] || '';
    return { first_name: first, last_name: last };
  }

  const parts = cleaned.split(/\s+/);
  if (parts.length >= 2) {
    return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
  }
  return { first_name: '', last_name: cleaned };
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

function provinceToCode(provinceName) {
  if (!provinceName) return '';
  const lower = provinceName.toLowerCase().trim();
  return PROVINCE_MAP[lower] || '';
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

// ── Directory Page Parser ────────────────────────────────────────────────────

function parseDirectoryPage(html) {
  const $ = cheerio.load(html);
  const entries = [];

  // Each listing is in a div#member (note: they use id="member" on every card,
  // which is non-standard HTML but Cheerio handles it fine with [id="member"])
  $('div[id="member"]').each((i, el) => {
    const $el = $(el);
    const $nameLink = $el.find('h3.title a');
    if (!$nameLink.length) return;

    const profileUrl = $nameLink.attr('href') || '';
    // Name is the text content without the .member-type span
    const $clone = $nameLink.clone();
    $clone.find('.member-type').remove();
    const fullName = $clone.text().trim();

    // Member type (Lawyer, Student, etc.)
    const memberType = $el.find('span.member-type').text().trim();

    // Firm name from h5 within the wpv-view-layout div
    let firmName = '';
    const $firmH5 = $el.find('h5');
    $firmH5.each((_, h5) => {
      const text = $(h5).text().trim();
      if (text && text !== 'Featured') {
        firmName = text;
      }
    });

    // Practice areas from the <p> after the wpv-view-layout div
    const practiceAreas = $el.find('> p').text().trim();

    if (fullName) {
      entries.push({
        fullName,
        memberType,
        firmName,
        practiceAreas,
        profileUrl: profileUrl.startsWith('http') ? profileUrl : `${BASE_URL}${profileUrl}`,
      });
    }
  });

  return entries;
}

// ── Profile Page Parser ──────────────────────────────────────────────────────

function parseProfilePage(html, entry) {
  const $ = cheerio.load(html);

  // Province: from "CILA Member - Ontario" heading
  let province = '';
  const memberHeading = $('div.elementor-heading-title').filter((_, el) => {
    return $(el).text().includes('CILA Member');
  }).first().text().trim();
  if (memberHeading) {
    const m = memberHeading.match(/CILA Member\s*-\s*(.+)/i);
    if (m) province = m[1].trim();
  }

  // Title/type: from h2 span.type
  let title = $('h2 span.type').text().trim() || entry.memberType || '';

  // Firm name: from h5 inside #firm or toolset-view widget
  let firmName = entry.firmName || '';
  const $firmWidget = $('[id="firm"]');
  if ($firmWidget.length) {
    const firmText = $firmWidget.find('h5').text().trim();
    if (firmText) firmName = firmText;
  }
  // Fallback: look for h5 near "Firm Name" heading
  if (!firmName) {
    $('h4.elementor-heading-title').each((_, el) => {
      if ($(el).text().trim() === 'Firm Name') {
        const $container = $(el).closest('.elementor-widget').next();
        const h5Text = $container.find('h5').text().trim();
        if (h5Text) firmName = h5Text;
      }
    });
  }

  // Bar admission (used for province if not found above)
  let barAdmission = '';
  $('h4.elementor-heading-title').each((_, el) => {
    if ($(el).text().trim() === 'Bar Admission') {
      const $next = $(el).closest('.elementor-widget').next();
      barAdmission = $next.find('.elementor-widget-container').text().trim();
    }
  });
  if (!province && barAdmission) {
    province = barAdmission;
  }

  // Contact information
  let email = '';
  let phone = '';
  let website = '';

  // Email: mailto link
  const $mailto = $('a[href^="mailto:"]');
  if ($mailto.length) {
    email = $mailto.attr('href').replace('mailto:', '').trim();
  }

  // Phone: in elementor-shortcode div with id="red" or after "Contact Information"
  const $phoneWidget = $('[id="red"]');
  if ($phoneWidget.length) {
    phone = $phoneWidget.find('.elementor-shortcode').text().trim();
  }
  // Fallback: look for phone patterns in shortcode divs
  if (!phone) {
    $('div.elementor-shortcode').each((_, el) => {
      const text = $(el).text().trim();
      // Match phone patterns: (xxx) xxx-xxxx or xxx-xxx-xxxx or +1xxxxxxxxxx
      if (/^[\d(+][\d\s()+-]{7,}$/.test(text) && !text.includes('@')) {
        phone = text;
      }
    });
  }

  // Website: link with title="Website" or non-mailto, non-cila.co links in shortcodes
  $('div.elementor-shortcode a').each((_, el) => {
    const $a = $(el);
    const href = $a.attr('href') || '';
    const linkTitle = $a.attr('title') || '';
    const linkText = $a.text().trim();
    if (linkTitle === 'Website' || linkText === 'Website') {
      website = href;
    } else if (href && !href.includes('mailto:') && !href.includes('cila.co') &&
               href.startsWith('http') && !website) {
      website = href;
    }
  });

  // Normalize province to code
  const state = provinceToCode(province);

  return {
    firmName,
    title,
    email,
    phone: cleanPhone(phone),
    website,
    domain: extractDomain(website),
    state,
    province, // Keep the full name for debugging
  };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const profilesOnly = args.includes('--profiles-only');

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('CILA Immigration Lawyers Scraper');
  log(`Test mode: ${isTest} | Output: ${OUT_FILE}`);

  // ── Phase 1: Collect profile URLs from directory pages ───────────────────

  let allEntries = [];

  if (!profilesOnly) {
    const maxPages = isTest ? 1 : TOTAL_PAGES;
    log(`\nPhase 1: Scraping directory pages (1-${maxPages})...`);

    for (let page = 1; page <= maxPages; page++) {
      const url = page === 1
        ? DIRECTORY_URL
        : `${DIRECTORY_URL}?wpv_view_count=2510&wpv_paged=${page}`;

      log(`  Page ${page}/${maxPages}: ${url}`);

      try {
        const html = await httpGet(url);
        const entries = parseDirectoryPage(html);
        log(`    Found ${entries.length} entries`);
        allEntries.push(...entries);
      } catch (err) {
        log(`    ERROR: ${err.message}`);
      }

      if (page < maxPages) await randomDelay();
    }

    // Dedup by profile URL
    const seen = new Set();
    allEntries = allEntries.filter(e => {
      if (seen.has(e.profileUrl)) return false;
      seen.add(e.profileUrl);
      return true;
    });

    log(`\nPhase 1 complete: ${allEntries.length} unique entries collected`);
  } else {
    // Load existing entries from CSV to get profile URLs for re-scraping
    log('Profiles-only mode: skipping directory pages');
  }

  // ── Phase 2: Visit each profile page ─────────────────────────────────────

  const maxProfiles = isTest ? 5 : allEntries.length;
  const entriesToProcess = allEntries.slice(0, maxProfiles);

  log(`\nPhase 2: Scraping ${entriesToProcess.length} profile pages...`);

  const leads = [];
  let successCount = 0;
  let errorCount = 0;
  let emailCount = 0;
  let phoneCount = 0;

  for (let i = 0; i < entriesToProcess.length; i++) {
    const entry = entriesToProcess[i];
    const { first_name, last_name } = parseName(entry.fullName);

    log(`  [${i + 1}/${entriesToProcess.length}] ${entry.fullName}`);

    try {
      const html = await httpGet(entry.profileUrl);
      const profile = parseProfilePage(html, entry);

      const lead = {
        first_name,
        last_name,
        firm_name: profile.firmName || entry.firmName || '',
        title: profile.title || entry.memberType || '',
        email: profile.email || '',
        phone: profile.phone || '',
        website: profile.website || '',
        domain: profile.domain || '',
        city: '', // CILA doesn't have city data in structured form
        state: profile.state || '',
        country: 'CA',
        niche: 'immigration',
        source: 'cila',
        profile_url: entry.profileUrl,
      };

      leads.push(lead);
      successCount++;
      if (lead.email) emailCount++;
      if (lead.phone) phoneCount++;

      // Log extracted fields
      const fields = [];
      if (lead.email) fields.push(`email=${lead.email}`);
      if (lead.phone) fields.push(`phone=${lead.phone}`);
      if (lead.state) fields.push(`province=${lead.state}`);
      if (lead.firm_name) fields.push(`firm=${lead.firm_name.substring(0, 30)}`);
      log(`    OK: ${fields.join(' | ') || '(minimal data)'}`);

    } catch (err) {
      errorCount++;
      log(`    ERROR: ${err.message}`);

      // Still add a partial record
      leads.push({
        first_name,
        last_name,
        firm_name: entry.firmName || '',
        title: entry.memberType || '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: '',
        state: '',
        country: 'CA',
        niche: 'immigration',
        source: 'cila',
        profile_url: entry.profileUrl,
      });
    }

    // Incremental save after each profile
    writeCSV(leads, OUT_FILE);

    // Rate limit
    if (i < entriesToProcess.length - 1) {
      await randomDelay();
    }
  }

  // ── Summary ──────────────────────────────────────────────────────────────

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${leads.length}`);
  log(`Successful profiles: ${successCount}`);
  log(`Errors: ${errorCount}`);
  log(`Leads with email: ${emailCount}`);
  log(`Leads with phone: ${phoneCount}`);
  log(`Leads with website: ${leads.filter(l => l.website).length}`);
  log(`Leads with province: ${leads.filter(l => l.state).length}`);

  // Province breakdown
  const byProvince = {};
  leads.forEach(l => {
    const prov = l.state || 'unknown';
    byProvince[prov] = (byProvince[prov] || 0) + 1;
  });
  log(`\nProvince breakdown:`);
  Object.entries(byProvince)
    .sort((a, b) => b[1] - a[1])
    .forEach(([prov, count]) => log(`  ${prov}: ${count}`));

  log(`\nOutput: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
