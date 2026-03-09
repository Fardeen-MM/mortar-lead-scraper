#!/usr/bin/env node
/**
 * Canadian Immigration Extra Sources Scraper
 *
 * Multi-source scraper combining:
 *   Source 1: MyConsultant.ca — CAPIC directory of ~3,700 Regulated Canadian
 *             Immigration Consultants (RCICs). AJAX listing → profile pages.
 *   Source 2: Quebec Open Data — Government CSV of ~730 Quebec-registered
 *             immigration consultants with email addresses.
 *
 * Strategy:
 *   Phase 1 — Fetch MyConsultant.ca listing (single AJAX POST, all 3,700 cards)
 *   Phase 2 — Visit each profile page for email/phone/website (2-3s delays)
 *   Phase 3 — Download + parse Quebec open data CSV
 *   Phase 4 — Merge, dedup, write final CSV
 *
 * Usage:
 *   node scripts/scrape-canada-extra-immigration.js --test     # Test: 10 profiles + Quebec CSV
 *   node scripts/scrape-canada-extra-immigration.js --all      # Full scrape (all profiles)
 *   node scripts/scrape-canada-extra-immigration.js --resume   # Resume from existing CSV
 *   node scripts/scrape-canada-extra-immigration.js --quebec   # Quebec CSV only
 *   node scripts/scrape-canada-extra-immigration.js --listing  # Listing only (no profiles)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const MC_BASE = 'https://www.myconsultant.ca';
const MC_PAGE = `${MC_BASE}/EN/Consultants`;
const MC_API = `${MC_BASE}/Partial/ConsultantsList`;
const QUEBEC_CSV_URL = 'https://www.donneesquebec.ca/recherche/dataset/23f8d075-4574-4238-a3d1-f3ba5674fce0/resource/26d1f57f-1f1d-4856-af54-d7b3cdb57954/download/consultants20260209.csv';

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'canada-immigration-extra.csv');

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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function httpGet(url, cookies) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const headers = {
      'User-Agent': USER_AGENT,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
    };
    if (cookies) headers['Cookie'] = cookies;

    const req = proto.get(url, { headers, timeout: 30000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return httpGet(redir, cookies).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        // Return both body and set-cookie headers
        resolve({
          body: Buffer.concat(chunks).toString('utf-8'),
          cookies: (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; '),
        });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function httpPost(url, data, cookies) {
  return new Promise((resolve, reject) => {
    const body = typeof data === 'string' ? data
      : Object.entries(data).map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');

    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || 443,
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'User-Agent': USER_AGENT,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': MC_PAGE,
      },
      timeout: 60000,
    };
    if (cookies) options.headers['Cookie'] = cookies;

    const proto = url.startsWith('https') ? https : http;
    const req = proto.request(options, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} POST ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout POST: ${url}`)); });
    req.write(body);
    req.end();
  });
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|attorney|lawyer|phd|md|llm|ll\.?b\.?|q\.?c\.?|k\.?c\.?|rcic|risia|ii|iii|iv|jr\.?|sr\.?)/gi, '')
    .trim();
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
  try { return new URL(url.startsWith('http') ? url : `http://${url}`).hostname.replace(/^www\./, ''); }
  catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s]/g, '').trim();
}

function provinceToCode(name) {
  if (!name) return '';
  const lower = name.toLowerCase().trim();
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

function readExistingCSV(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const header = lines[0].split(',');
  const leads = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const lead = {};
    header.forEach((col, idx) => { lead[col] = values[idx] || ''; });
    leads.push(lead);
  }
  return leads;
}

function parseCSVLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"'; i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        values.push(current); current = '';
      } else {
        current += ch;
      }
    }
  }
  values.push(current);
  return values;
}

// ── Source 1: MyConsultant.ca ────────────────────────────────────────────────

async function fetchMCListingPage() {
  log('Fetching MyConsultant.ca page for CSRF token...');
  const { body, cookies } = await httpGet(MC_PAGE);

  // Extract CSRF token
  const $ = cheerio.load(body);
  const token = $('input[name="__RequestVerificationToken"]').attr('value');
  if (!token) throw new Error('Could not extract CSRF token from MyConsultant.ca');
  log(`  CSRF token: ${token.substring(0, 20)}...`);
  log(`  Cookies: ${cookies.substring(0, 40)}...`);
  return { token, cookies };
}

async function fetchMCConsultantList(token, cookies) {
  log('Fetching all consultants from MyConsultant.ca (AJAX POST)...');
  const html = await httpPost(MC_API, {
    language: 'EN',
    keyword: '',
    location: '',
    spokenlang: '',
    service: '',
    __RequestVerificationToken: token,
  }, cookies);

  log(`  Response size: ${(html.length / 1024 / 1024).toFixed(1)} MB`);
  return html;
}

function parseMCListingCards(html) {
  const $ = cheerio.load(html);
  const entries = [];

  // Hidden count field
  const count = $('#hdnConsultantsCount').val();
  log(`  Server reports ${count} consultants`);

  // Each consultant card
  $('.consultants-item').each((i, el) => {
    const $el = $(el);

    // Name from the link
    const $nameLink = $el.find('a.consultant-name');
    const fullName = $nameLink.text().trim();
    const profilePath = $nameLink.attr('href') || '';

    // CICC ID
    const $details = $el.find('.consultant-details');
    const detailText = $details.text();
    const ciccMatch = detailText.match(/CICC ID:\s*(R\d+)/);
    const ciccId = ciccMatch ? ciccMatch[1] : '';

    // Location
    const locMatch = detailText.match(/Location:\s*([^,\n]+(?:,\s*[^\n]+)?)/);
    const location = locMatch ? locMatch[1].trim() : '';

    // Specialty
    const specMatch = detailText.match(/Specialty:\s*([^\n]+)/);
    const specialty = specMatch ? specMatch[1].trim() : '';

    // Languages
    const langMatch = detailText.match(/Language\(s\):\s*([^\n]+)/);
    const languages = langMatch ? langMatch[1].trim() : '';

    if (fullName) {
      // Convert FR profile path to EN
      const profileUrl = profilePath
        ? `${MC_BASE}${profilePath.replace('/FR/', '/EN/')}`
        : '';

      entries.push({
        fullName,
        ciccId,
        location,
        specialty,
        languages,
        profileUrl,
      });
    }
  });

  return entries;
}

async function fetchMCProfile(url, cookies) {
  const { body } = await httpGet(url, cookies);
  const $ = cheerio.load(body);

  let phone = '';
  let email = '';
  let website = '';
  let city = '';
  let province = '';
  let title = '';
  let firmName = '';

  // Location: "City, Province, Country" from sidebar paragraph
  const locText = $('p.consultant-sidebar-paragraph').first().text().trim();
  if (locText) {
    const parts = locText.split(',').map(s => s.trim());
    if (parts.length >= 2) {
      city = parts[0];
      province = parts[1];
      // If "city" looks like a street address, clear it
      if (/^\d+\s/.test(city) || /\b(ave|st|rd|blvd|dr|cres|way|hwy|suite|unit)\b/i.test(city)) {
        city = '';
      }
    }
  }

  // Phone from list-phone
  const $phone = $('li.list-phone a[href^="tel:"]');
  if ($phone.length) {
    phone = $phone.first().text().trim();
  }
  // Fallback to cell
  if (!phone) {
    const $cell = $('li.list-cell a[href^="tel:"]');
    if ($cell.length) phone = $cell.first().text().trim();
  }

  // Email from list-mail
  const $mail = $('li.list-mail a[href^="mailto:"]');
  if ($mail.length) {
    email = $mail.first().attr('href').replace('mailto:', '').trim();
  }

  // Website from list-web
  const $web = $('li.list-web a');
  if ($web.length) {
    website = $web.first().attr('href') || '';
  }

  // Title: from heading sub span
  const $titleSpan = $('div.default-heading-sub span').first();
  if ($titleSpan.length) {
    title = $titleSpan.text().trim();
  }

  // Firm name: try to extract from description if it looks like a firm name
  const desc = $('div.consultant-description').text().trim();
  if (desc) {
    // Look for "XYZ Inc." or "XYZ Ltd." or "XYZ Immigration" at start
    const firmMatch = desc.match(/^([A-Z][^\n.]{3,60}(?:Inc\.|Ltd\.|Corp\.|LLC|Immigration|Consulting|Services|Group|Solutions|Agency|Associates))/i);
    if (firmMatch) {
      firmName = firmMatch[1].trim();
    }
  }

  return {
    phone: cleanPhone(phone),
    email,
    website,
    domain: extractDomain(website),
    city,
    province,
    state: provinceToCode(province),
    title: title || 'Immigration Consultant',
    firmName,
  };
}

// ── Source 2: Quebec Open Data CSV ───────────────────────────────────────────

async function fetchQuebecCSV() {
  log('Downloading Quebec immigration consultant registry...');
  const { body } = await httpGet(QUEBEC_CSV_URL);
  log(`  Downloaded ${(body.length / 1024).toFixed(0)} KB`);

  const lines = body.split('\n').filter(l => l.trim());
  log(`  ${lines.length - 1} records (including header)`);

  // Parse header
  const headerLine = lines[0];
  const headers = headerLine.split(',').map(h => h.trim());
  log(`  Columns: ${headers.join(', ')}`);

  const leads = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const record = {};
    headers.forEach((h, idx) => { record[h] = (values[idx] || '').trim(); });

    const status = record.STATUT || '';
    // Include both REV (revoked?) and REC (recognized?) — they are all registered consultants
    const firstName = record.PRENOM || '';
    const lastName = record.NOM || '';
    const email = record.COURRIEL || '';
    const city = record.ENTREPRISEVILLE || '';
    const province = record.ENTREPRISEPROVINCE || '';
    const regId = record.NOINSCRIPTION || '';

    if (!firstName && !lastName) continue;

    leads.push({
      first_name: firstName,
      last_name: lastName,
      firm_name: record.ENTREPRISEADRESSE1 || '', // Often firm name is in address line
      title: 'Immigration Consultant',
      email,
      phone: '',
      website: '',
      domain: '',
      city,
      state: provinceToCode(province) || 'QC',
      country: 'CA',
      niche: 'immigration',
      source: 'quebec-registry',
      profile_url: '',
      _regId: regId,
      _status: status,
    });
  }

  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');
  const isResume = args.includes('--resume');
  const isQuebecOnly = args.includes('--quebec');
  const isListingOnly = args.includes('--listing');

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('='.repeat(60));
  log('Canada Extra Immigration Sources Scraper');
  log(`Mode: ${isTest ? 'TEST' : isAll ? 'ALL' : isResume ? 'RESUME' : isQuebecOnly ? 'QUEBEC-ONLY' : isListingOnly ? 'LISTING-ONLY' : 'DEFAULT (use --test or --all)'}`);
  log(`Output: ${OUT_FILE}`);
  log('='.repeat(60));

  if (!isTest && !isAll && !isResume && !isQuebecOnly && !isListingOnly) {
    log('\nUsage:');
    log('  --test      Test mode (10 profiles + Quebec CSV)');
    log('  --all       Full scrape (all ~3,700 profiles + Quebec CSV)');
    log('  --resume    Resume profile scraping from existing CSV');
    log('  --quebec    Quebec open data CSV only');
    log('  --listing   MyConsultant listing only (no profile visits)');
    process.exit(0);
  }

  let allLeads = [];
  let existingProfileUrls = new Set();

  // Load existing CSV for resume
  if (isResume) {
    const existing = readExistingCSV(OUT_FILE);
    if (existing.length) {
      log(`\nLoaded ${existing.length} existing leads from CSV`);
      allLeads = existing;
      existing.forEach(l => {
        if (l.profile_url && l.source === 'myconsultant') {
          existingProfileUrls.add(l.profile_url);
        }
      });
      log(`  ${existingProfileUrls.size} MyConsultant profiles already scraped`);
    }
  }

  // ── Source 1: MyConsultant.ca ──────────────────────────────────────────────

  if (!isQuebecOnly) {
    log('\n--- Source 1: MyConsultant.ca ---');

    try {
      // Phase 1: Get CSRF token and cookies
      const { token, cookies } = await fetchMCListingPage();
      await sleep(1000);

      // Phase 2: Fetch all consultant cards
      const listingHtml = await fetchMCConsultantList(token, cookies);
      const entries = parseMCListingCards(listingHtml);
      log(`\nParsed ${entries.length} consultant cards from listing`);

      if (isListingOnly) {
        // Just save listing data without visiting profiles
        for (const entry of entries) {
          const { first_name, last_name } = parseName(entry.fullName);
          // Parse city from location string ("Markham, Canada" → "Markham")
          const locParts = entry.location.split(',').map(s => s.trim());
          const city = locParts[0] || '';

          allLeads.push({
            first_name,
            last_name,
            firm_name: '',
            title: 'Immigration Consultant',
            email: '',
            phone: '',
            website: '',
            domain: '',
            city,
            state: '',
            country: 'CA',
            niche: 'immigration',
            source: 'myconsultant',
            profile_url: entry.profileUrl,
          });
        }
        log(`Added ${entries.length} leads from listing (no profile visits)`);
      } else {
        // Phase 3: Visit profile pages
        let toProcess = entries;

        // Filter out already-scraped profiles in resume mode
        if (isResume) {
          toProcess = entries.filter(e => !existingProfileUrls.has(e.profileUrl));
          log(`${toProcess.length} profiles remaining after resume filter`);
        }

        // Limit in test mode
        const maxProfiles = isTest ? 10 : toProcess.length;
        const batch = toProcess.slice(0, maxProfiles);

        log(`\nPhase 2: Visiting ${batch.length} profile pages...`);

        let successCount = 0;
        let errorCount = 0;
        let emailCount = 0;
        let phoneCount = 0;
        let websiteCount = 0;

        for (let i = 0; i < batch.length; i++) {
          const entry = batch[i];
          const { first_name, last_name } = parseName(entry.fullName);

          log(`  [${i + 1}/${batch.length}] ${entry.fullName} (${entry.ciccId})`);

          try {
            const profile = await fetchMCProfile(entry.profileUrl, cookies);

            const lead = {
              first_name,
              last_name,
              firm_name: profile.firmName || '',
              title: profile.title || 'Immigration Consultant',
              email: profile.email || '',
              phone: profile.phone || '',
              website: profile.website || '',
              domain: profile.domain || '',
              city: profile.city || '',
              state: profile.state || '',
              country: 'CA',
              niche: 'immigration',
              source: 'myconsultant',
              profile_url: entry.profileUrl,
            };

            allLeads.push(lead);
            successCount++;
            if (lead.email) emailCount++;
            if (lead.phone) phoneCount++;
            if (lead.website) websiteCount++;

            const fields = [];
            if (lead.email) fields.push(`email=${lead.email}`);
            if (lead.phone) fields.push(`phone=${lead.phone}`);
            if (lead.city) fields.push(`city=${lead.city}`);
            if (lead.state) fields.push(`prov=${lead.state}`);
            log(`    OK: ${fields.join(' | ') || '(minimal data)'}`);

          } catch (err) {
            errorCount++;
            log(`    ERROR: ${err.message}`);

            // Add partial record
            const locParts = entry.location.split(',').map(s => s.trim());
            allLeads.push({
              first_name,
              last_name,
              firm_name: '',
              title: 'Immigration Consultant',
              email: '',
              phone: '',
              website: '',
              domain: '',
              city: locParts[0] || '',
              state: '',
              country: 'CA',
              niche: 'immigration',
              source: 'myconsultant',
              profile_url: entry.profileUrl,
            });
          }

          // Incremental save
          writeCSV(allLeads, OUT_FILE);

          // Rate limit
          if (i < batch.length - 1) await randomDelay();
        }

        log(`\nMyConsultant.ca results:`);
        log(`  Profiles scraped: ${successCount}`);
        log(`  Errors: ${errorCount}`);
        log(`  With email: ${emailCount}`);
        log(`  With phone: ${phoneCount}`);
        log(`  With website: ${websiteCount}`);
      }
    } catch (err) {
      log(`MyConsultant.ca ERROR: ${err.message}`);
    }
  }

  // ── Source 2: Quebec Open Data ─────────────────────────────────────────────

  log('\n--- Source 2: Quebec Open Data ---');

  try {
    const quebecLeads = await fetchQuebecCSV();
    log(`  Parsed ${quebecLeads.length} Quebec consultants`);

    // Filter out duplicates by email (if already have from MyConsultant)
    const existingEmails = new Set(
      allLeads.filter(l => l.email).map(l => l.email.toLowerCase())
    );
    const existingNames = new Set(
      allLeads.map(l => `${l.first_name} ${l.last_name}`.toLowerCase().trim())
    );

    let added = 0;
    let dupes = 0;
    for (const lead of quebecLeads) {
      const nameKey = `${lead.first_name} ${lead.last_name}`.toLowerCase().trim();
      const emailKey = lead.email ? lead.email.toLowerCase() : '';

      if ((emailKey && existingEmails.has(emailKey)) || existingNames.has(nameKey)) {
        dupes++;
        continue;
      }

      allLeads.push(lead);
      added++;
      if (emailKey) existingEmails.add(emailKey);
      existingNames.add(nameKey);
    }

    log(`  Added: ${added}, Skipped (dupes): ${dupes}`);
  } catch (err) {
    log(`Quebec CSV ERROR: ${err.message}`);
  }

  // ── Final Output ──────────────────────────────────────────────────────────

  writeCSV(allLeads, OUT_FILE);

  log(`\n${'='.repeat(60)}`);
  log('FINAL SUMMARY');
  log(`${'='.repeat(60)}`);
  log(`Total leads: ${allLeads.length}`);
  log(`With email: ${allLeads.filter(l => l.email).length}`);
  log(`With phone: ${allLeads.filter(l => l.phone).length}`);
  log(`With website: ${allLeads.filter(l => l.website).length}`);

  // Source breakdown
  const bySrc = {};
  allLeads.forEach(l => {
    bySrc[l.source] = (bySrc[l.source] || 0) + 1;
  });
  log('\nBy source:');
  Object.entries(bySrc)
    .sort((a, b) => b[1] - a[1])
    .forEach(([s, c]) => log(`  ${s}: ${c}`));

  // Province breakdown
  const byProv = {};
  allLeads.forEach(l => {
    const p = l.state || 'unknown';
    byProv[p] = (byProv[p] || 0) + 1;
  });
  log('\nBy province:');
  Object.entries(byProv)
    .sort((a, b) => b[1] - a[1])
    .forEach(([p, c]) => log(`  ${p}: ${c}`));

  log(`\nOutput: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
