#!/usr/bin/env node
/**
 * Canada Education Consultants / Agents Scraper
 *
 * Sources:
 *   1. EducationAgentsGuide.com — 500+ Canada-based education agents (listing + profile pages)
 *   2. StudyAbroadLists.com — 131 education agents across Canadian cities
 *   3. Languages Canada — ~180 accredited language programs with contact info
 *   4. University of Saskatchewan — 57 authorized international education agents
 *
 * Strategy:
 *   Phase 1: Scrape EducationAgentsGuide.com listing page → collect profile URLs
 *   Phase 2: Visit each EAG profile for email, phone, website, address, contact person
 *   Phase 3: Scrape StudyAbroadLists.com paginated directory → visit profiles
 *   Phase 4: Scrape Languages Canada paginated directory → visit profile pages
 *   Phase 5: Scrape University of Saskatchewan static agent table
 *
 * Usage:
 *   node scripts/scrape-canada-education-consultants.js --test     # Test mode (5 per source)
 *   node scripts/scrape-canada-education-consultants.js --all      # Full scrape
 *   node scripts/scrape-canada-education-consultants.js             # Full scrape (default)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'canada-education-consultants.csv');

const PROVINCE_MAP = {
  'alberta': 'AB', 'british columbia': 'BC', 'manitoba': 'MB',
  'new brunswick': 'NB', 'newfoundland': 'NL', 'newfoundland and labrador': 'NL',
  'northwest territories': 'NT', 'nova scotia': 'NS', 'nunavut': 'NU',
  'ontario': 'ON', 'prince edward island': 'PE', 'quebec': 'QC', 'québec': 'QC',
  'saskatchewan': 'SK', 'yukon': 'YT',
  // Abbreviation passthrough
  'ab': 'AB', 'bc': 'BC', 'mb': 'MB', 'nb': 'NB', 'nl': 'NL',
  'nt': 'NT', 'ns': 'NS', 'nu': 'NU', 'on': 'ON', 'pe': 'PE',
  'qc': 'QC', 'sk': 'SK', 'yt': 'YT',
};

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function httpGet(url, retries = 2) {
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
        const loc = res.headers.location;
        const redirectUrl = loc.startsWith('http') ? loc : new URL(loc, url).href;
        res.resume();
        return httpGet(redirectUrl, retries).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        if (retries > 0 && res.statusCode >= 500) {
          return sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
        }
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });
    req.on('error', (err) => {
      if (retries > 0) {
        return sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      }
      reject(err);
    });
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(esq\.?|esquire|j\.?d\.?|phd|md|llm|ll\.?b\.?|mba|m\.?ed\.?|b\.?ed\.?|b\.?a\.?|m\.?a\.?|rcic|cicc)/gi, '')
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
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+()-\s.]/g, '').replace(/\s+/g, ' ').trim();
}

function provinceToCode(str) {
  if (!str) return '';
  const lower = str.toLowerCase().trim();
  if (PROVINCE_MAP[lower]) return PROVINCE_MAP[lower];
  // Only match full province names (not abbreviations) as partial matches
  const fullNames = [
    'alberta', 'british columbia', 'manitoba', 'new brunswick',
    'newfoundland', 'newfoundland and labrador', 'northwest territories',
    'nova scotia', 'nunavut', 'ontario', 'prince edward island',
    'quebec', 'québec', 'saskatchewan', 'yukon',
  ];
  for (const name of fullNames) {
    if (lower.includes(name)) return PROVINCE_MAP[name];
  }
  return '';
}

function parseCanadianAddress(address) {
  if (!address) return { city: '', state: '' };
  // Pattern: "City, XX" or "City, Province" or full address with postal code
  // Try to find province code in address
  const parts = address.split(',').map(s => s.trim());
  let city = '';
  let state = '';

  for (const part of parts) {
    // Check if part is a province code or name
    const code = provinceToCode(part.replace(/[^a-zA-Z\s]/g, '').trim());
    if (code) {
      state = code;
    }
    // Check for "ON M4R1V2" pattern (province + postal code)
    const provPostal = part.match(/^([A-Z]{2})\s+[A-Z]\d[A-Z]\s?\d[A-Z]\d$/i);
    if (provPostal) {
      const maybeCode = provinceToCode(provPostal[1]);
      if (maybeCode) state = maybeCode;
    }
  }

  // City is usually the first or second part that isn't a number/postal code
  for (const part of parts) {
    const cleaned = part.trim();
    if (!cleaned) continue;
    // Skip if it looks like a street number, postal code, or country
    if (/^\d/.test(cleaned)) continue;
    if (/^[A-Z]\d[A-Z]\s?\d[A-Z]\d$/i.test(cleaned)) continue;
    if (/^canada$/i.test(cleaned)) continue;
    if (provinceToCode(cleaned.replace(/[^a-zA-Z\s]/g, '').trim())) continue;
    // Skip province + postal combo
    if (/^[A-Z]{2}\s+[A-Z]\d[A-Z]/i.test(cleaned)) continue;
    // This is likely the city — validate it
    if (!city && cleaned.length >= 3 && !/^(the|po box|box|unit|suite)$/i.test(cleaned)) {
      city = cleaned;
    }
  }

  return { city, state };
}

function csvEscape(val) {
  if (!val) return '';
  // Strip newlines/tabs and collapse whitespace
  const str = String(val).replace(/[\n\r\t]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (str.includes(',') || str.includes('"')) {
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

function normalizeUrl(url) {
  if (!url) return '';
  url = url.trim();
  if (url.startsWith('//')) url = 'https:' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  // Remove trailing slashes
  return url.replace(/\/+$/, '');
}

// ── Source 1: EducationAgentsGuide.com ───────────────────────────────────────

async function scrapeEAGListing() {
  log('Source 1: EducationAgentsGuide.com — fetching listing page...');
  const url = 'https://educationagentsguide.com/canada/index.htm';
  const html = await httpGet(url);
  const $ = cheerio.load(html);

  const entries = [];
  // Agent links are in <ul> <li> <a href="FILENAME.htm">Name</a>
  $('ul li a').each((_, el) => {
    const href = $(el).attr('href') || '';
    const name = $(el).text().trim();
    if (!href || !name) return;
    // Only .htm files that are agent profiles (not navigation/external links)
    if (!href.endsWith('.htm')) return;
    if (href === 'index.htm') return;
    if (href.startsWith('http') && !href.includes('educationagentsguide.com')) return;

    const profileUrl = href.startsWith('http')
      ? href
      : `https://educationagentsguide.com/canada/${href}`;

    entries.push({ name, profileUrl });
  });

  // Dedup by URL
  const seen = new Set();
  const unique = entries.filter(e => {
    if (seen.has(e.profileUrl)) return false;
    seen.add(e.profileUrl);
    return true;
  });

  log(`  Found ${unique.length} agent profile links`);
  return unique;
}

function parseEAGProfile(html, entry) {
  const $ = cheerio.load(html);
  const text = $('body').text();

  let email = '';
  let phone = '';
  let website = '';
  let address = '';
  let contactPerson = '';

  // Email: look for mailto links first, then text patterns
  const $mailto = $('a[href^="mailto:"]').first();
  if ($mailto.length) {
    email = $mailto.attr('href').replace('mailto:', '').split('?')[0].trim();
  }
  if (!email) {
    const emailMatch = text.match(/[\w.+-]+@[\w-]+\.[\w.-]+/);
    if (emailMatch) email = emailMatch[0];
  }

  // Phone: look for labeled fields — must be a real phone number (7+ digits)
  const phonePatterns = [
    /(?:Telephone|Phone|Tel)[:\s]*([+\d][\d\s().-]{7,})/i,
  ];
  for (const pat of phonePatterns) {
    const m = text.match(pat);
    if (m) {
      const digits = m[1].replace(/\D/g, '');
      if (digits.length >= 7) { phone = m[1].trim(); break; }
    }
  }

  // Website: look for labeled links
  $('a').each((_, el) => {
    const href = $(el).attr('href') || '';
    const linkText = $(el).text().trim().toLowerCase();
    if (website) return;
    if (href.startsWith('http') &&
        !href.includes('educationagentsguide.com') &&
        !href.includes('mailto:') &&
        !href.includes('facebook.com') &&
        !href.includes('twitter.com') &&
        !href.includes('linkedin.com') &&
        !href.includes('instagram.com') &&
        !href.includes('youtube.com')) {
      website = href;
    }
  });

  // Also check for website in text near "Website" label
  const webMatch = text.match(/Website[:\s]*(https?:\/\/[\S]+)/i);
  if (webMatch && !website) website = webMatch[1];
  const wwwMatch = text.match(/Website[:\s]*(www\.[\S]+)/i);
  if (wwwMatch && !website) website = 'https://' + wwwMatch[1];

  // Address: look for Canadian postal code pattern
  // EAG profiles have unstructured text with address block containing postal code
  const postalMatch = text.match(/([^\n]*?[A-Z]\d[A-Z]\s?\d[A-Z]\d[^\n]*)/);
  if (postalMatch) {
    address = postalMatch[1].trim();
  }
  // Fallback: "Address:" label
  if (!address) {
    const addrMatch = text.match(/Address[:\s]*([^\n]+)/i);
    if (addrMatch) address = addrMatch[1].trim();
  }

  // Contact person: "Principal Agent" label
  const agentMatch = text.match(/Principal Agent[:\s]*([^\n]+)/i);
  if (agentMatch) contactPerson = agentMatch[1].trim();
  // Fallback: "Contact" label
  if (!contactPerson) {
    const contactMatch = text.match(/Contact[:\s]*([A-Z][a-z]+ [A-Z][a-z]+[^\n]*)/);
    if (contactMatch) contactPerson = contactMatch[1].trim();
  }

  let { city, state } = parseCanadianAddress(address);

  // Validate city — reject junk
  if (city && (
    city.length > 40 || city.length < 3 ||
    /^(website|click|here|tel|fax|email|phone|contact|the)$/i.test(city) ||
    /^(PO Box|Box |Unit |Suite |\d)/i.test(city)
  )) {
    city = '';
  }

  return { email, phone: cleanPhone(phone), website: normalizeUrl(website), address, city, state, contactPerson };
}

async function scrapeEducationAgentsGuide(isTest) {
  const entries = await scrapeEAGListing();
  const maxProfiles = isTest ? 5 : entries.length;
  const toProcess = entries.slice(0, maxProfiles);

  log(`  Scraping ${toProcess.length} profiles...`);
  const leads = [];

  for (let i = 0; i < toProcess.length; i++) {
    const entry = toProcess[i];
    log(`  [${i + 1}/${toProcess.length}] ${entry.name}`);

    try {
      await randomDelay();
      const html = await httpGet(entry.profileUrl);
      const profile = parseEAGProfile(html, entry);

      // Parse contact person name if available
      const person = parseName(profile.contactPerson);

      leads.push({
        first_name: person.first_name,
        last_name: person.last_name,
        firm_name: entry.name,
        title: 'Education Agent',
        email: profile.email,
        phone: profile.phone,
        website: profile.website,
        domain: extractDomain(profile.website),
        city: profile.city,
        state: profile.state,
        country: 'CA',
        niche: 'education',
        source: 'educationagentsguide',
        profile_url: entry.profileUrl,
      });

      const fields = [];
      if (profile.email) fields.push(`email=${profile.email}`);
      if (profile.phone) fields.push(`phone=${profile.phone}`);
      if (profile.city) fields.push(`city=${profile.city}`);
      if (profile.state) fields.push(`prov=${profile.state}`);
      log(`    OK: ${fields.join(' | ') || '(minimal data)'}`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
      leads.push({
        first_name: '', last_name: '',
        firm_name: entry.name,
        title: 'Education Agent',
        email: '', phone: '', website: '', domain: '',
        city: '', state: '', country: 'CA',
        niche: 'education',
        source: 'educationagentsguide',
        profile_url: entry.profileUrl,
      });
    }
  }

  return leads;
}

// ── Source 2: StudyAbroadLists.com ───────────────────────────────────────────

async function scrapeSALListing(isTest) {
  log('Source 2: StudyAbroadLists.com — fetching paginated directory...');
  const baseUrl = 'https://www.studyabroadlists.com/canada/education-agents';
  const entries = [];
  const maxPages = isTest ? 1 : 20; // 131 agents / 9 per page ≈ 15 pages

  for (let page = 1; page <= maxPages; page++) {
    const url = page === 1 ? baseUrl : `${baseUrl}?page=${page}`;
    log(`  Page ${page}: ${url}`);

    try {
      const html = await httpGet(url);
      const $ = cheerio.load(html);
      let foundOnPage = 0;

      // Agent cards in grid elements — look for listing links
      // URL pattern: /province/city/education-agents/agent-slug or /canada/city/education-agents/agent-slug
      $('a').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim();
        // Match agent profile URLs
        if (href.match(/\/education-agents\/[a-z0-9-]+$/i) && text && text.length > 3) {
          // Skip navigation/category links
          if (text.toLowerCase() === 'view listing' || text.toLowerCase() === 'send message') return;
          if (href.includes('/connect')) return;
          const profileUrl = href.startsWith('http') ? href : `https://www.studyabroadlists.com${href}`;
          // Clean " - Education Agents" or " - Education Consulting" suffix
          const cleanName = text
            .replace(/\s*-\s*Education\s*(Agents?|Consulting)\s*$/i, '')
            .trim();
          entries.push({ name: cleanName || text, profileUrl });
          foundOnPage++;
        }
      });

      log(`    Found ${foundOnPage} entries`);
      if (foundOnPage === 0) break; // No more pages

      await randomDelay();
    } catch (err) {
      log(`    ERROR: ${err.message}`);
      break;
    }
  }

  // Dedup by URL
  const seen = new Set();
  return entries.filter(e => {
    if (seen.has(e.profileUrl)) return false;
    seen.add(e.profileUrl);
    return true;
  });
}

function parseSALProfile(html) {
  const $ = cheerio.load(html);
  const text = $('body').text();

  let phone = '';
  let email = '';
  let website = '';
  let city = '';
  let state = '';

  // Website: look for links that aren't internal/social — prefer links with agent's own domain
  const skipDomains = [
    'studyabroadlists.com', 'mailto:', 'facebook.com', 'twitter.com',
    'instagram.com', 'linkedin.com', 'youtube.com', 'google.com',
    'pinterest.', 'tiktok.com', 'pxf.io', 'knack-bags',
    'whatsapp.com', 'wa.me', 'bit.ly', 't.me',
    'wise.prf.hn', 'prf.hn', 'shareasale.com', 'anrdoezrs.net',
    'awin1.com', 'impact.com', 'go.skimresources',
    'ieltspass.com', 'ielts.org', 'blog.canada.idp.com',
  ];
  $('a').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (website) return;
    if (!href.startsWith('http')) return;
    const isSkip = skipDomains.some(d => href.includes(d));
    if (isSkip) return;
    website = href;
  });

  // Email: mailto links — but SKIP the site's own email
  $('a[href^="mailto:"]').each((_, el) => {
    if (email) return;
    const addr = $(el).attr('href').replace('mailto:', '').split('?')[0].trim();
    if (addr && !addr.includes('studyabroadlists.com')) {
      email = addr;
    }
  });
  if (!email) {
    const allEmails = text.match(/[\w.+-]+@[\w-]+\.[\w.-]+/g) || [];
    email = allEmails.find(e => !e.includes('studyabroadlists.com') && !e.includes('noreply')) || '';
  }

  // Phone: tel links or text patterns
  const $tel = $('a[href^="tel:"]').first();
  if ($tel.length) {
    phone = $tel.attr('href').replace('tel:', '').trim();
  }
  if (!phone) {
    const phoneMatch = text.match(/(?:Phone|Tel)[:\s]*([+\d][\d\s().-]{7,})/i);
    if (phoneMatch) phone = phoneMatch[1].trim();
  }

  // Location: look for Canadian city, province pattern
  const locMatch = text.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*(ON|BC|AB|SK|MB|QC|NB|NS|PE|NL|NT|NU|YT|Ontario|British Columbia|Alberta|Saskatchewan|Manitoba|Quebec|New Brunswick|Nova Scotia|Prince Edward Island|Newfoundland)/);
  if (locMatch) {
    city = locMatch[1].trim();
    state = provinceToCode(locMatch[2]);
  }

  // Fallback: address with postal code
  if (!city) {
    const addrMatch = text.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s*(ON|BC|AB|SK|MB|QC|NB|NS|PE|NL)\s+[A-Z]\d[A-Z]/);
    if (addrMatch) {
      city = addrMatch[1].trim();
      state = provinceToCode(addrMatch[2]);
    }
  }

  return { email, phone: cleanPhone(phone), website: normalizeUrl(website), city, state };
}

async function scrapeStudyAbroadLists(isTest) {
  const entries = await scrapeSALListing(isTest);
  const maxProfiles = isTest ? 5 : entries.length;
  const toProcess = entries.slice(0, maxProfiles);

  log(`  Scraping ${toProcess.length} profiles...`);
  const leads = [];

  for (let i = 0; i < toProcess.length; i++) {
    const entry = toProcess[i];
    log(`  [${i + 1}/${toProcess.length}] ${entry.name}`);

    try {
      await randomDelay();
      const html = await httpGet(entry.profileUrl);
      const profile = parseSALProfile(html);

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: entry.name,
        title: 'Education Agent',
        email: profile.email,
        phone: profile.phone,
        website: profile.website,
        domain: extractDomain(profile.website),
        city: profile.city,
        state: profile.state,
        country: 'CA',
        niche: 'education',
        source: 'studyabroadlists',
        profile_url: entry.profileUrl,
      });

      const fields = [];
      if (profile.email) fields.push(`email=${profile.email}`);
      if (profile.phone) fields.push(`phone=${profile.phone}`);
      if (profile.website) fields.push(`web=${extractDomain(profile.website)}`);
      if (profile.city) fields.push(`city=${profile.city}`);
      log(`    OK: ${fields.join(' | ') || '(minimal data)'}`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
      leads.push({
        first_name: '', last_name: '',
        firm_name: entry.name,
        title: 'Education Agent',
        email: '', phone: '', website: '', domain: '',
        city: '', state: '', country: 'CA',
        niche: 'education',
        source: 'studyabroadlists',
        profile_url: entry.profileUrl,
      });
    }
  }

  return leads;
}

// ── Source 3: Languages Canada ───────────────────────────────────────────────

async function scrapeLCListing(isTest) {
  log('Source 3: Languages Canada — fetching program directory...');
  const entries = [];
  const maxPages = isTest ? 1 : 25; // ~19 pages

  for (let page = 1; page <= maxPages; page++) {
    const url = page === 1
      ? 'https://www.languagescanada.ca/en/students-new'
      : `https://www.languagescanada.ca/en/students-new?listing_options_6%5Bitems%5D%5Bpage%5D=${page}`;
    log(`  Page ${page}: ${url}`);

    try {
      const html = await httpGet(url);
      const $ = cheerio.load(html);
      let foundOnPage = 0;

      // Links to /en/listing-directory/students/SCHOOL_NAME
      $('a').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim();
        if (href.includes('/listing-directory/students/') && text && text.length > 2) {
          // Skip navigation duplicates
          if (text.toLowerCase() === 'find a language program') return;
          if (text.toLowerCase() === 'list of lc members') return;
          const profileUrl = href.startsWith('http')
            ? href
            : `https://www.languagescanada.ca${href}`;
          entries.push({ name: text, profileUrl });
          foundOnPage++;
        }
      });

      log(`    Found ${foundOnPage} entries`);
      if (foundOnPage === 0) break;

      await randomDelay();
    } catch (err) {
      log(`    ERROR: ${err.message}`);
      break;
    }
  }

  // Dedup by URL
  const seen = new Set();
  return entries.filter(e => {
    if (seen.has(e.profileUrl)) return false;
    seen.add(e.profileUrl);
    return true;
  });
}

function parseLCProfile(html) {
  const $ = cheerio.load(html);

  // Remove cookie/consent/navigation noise before extracting text
  $('script, style, .cookie-notice, .cookie-consent, #cookie, nav, footer, header').remove();

  let email = '';
  let phone = '';
  let website = '';
  let city = '';
  let state = '';
  let address = '';

  // Languages Canada profiles use structured fields:
  // "Student Inquiries Email:", "Website:", "Address:" etc.

  // Email: look for labeled email field or mailto links
  const text = $('body').text();
  const emailLabeled = text.match(/(?:Student Inquiries Email|Inquiries Email|General Email|Email)\s*[:\s]\s*([\w.+-]+@[\w-]+\.[\w.-]+)/i);
  if (emailLabeled) email = emailLabeled[1].trim();
  if (!email) {
    $('a[href^="mailto:"]').each((_, el) => {
      if (email) return;
      const addr = $(el).attr('href').replace('mailto:', '').split('?')[0].trim();
      if (addr && !addr.includes('languagescanada.ca') && !addr.includes('noreply')) {
        email = addr;
      }
    });
  }
  if (!email) {
    const allEmails = text.match(/[\w.+-]+@[\w-]+\.[\w.-]+/g) || [];
    email = allEmails.find(e =>
      !e.includes('noreply') && !e.includes('no-reply') &&
      !e.includes('languagescanada.ca') && !e.includes('eventbrite') &&
      !e.includes('icptrack') && !e.includes('google')
    ) || '';
  }

  // Website: labeled field or first external link (exclude tracking/cdn links)
  const skipLinkDomains = [
    'languagescanada.ca', 'languescanada.ca', 'mailto:', 'facebook.com',
    'twitter.com', 'instagram.com', 'google.com', 'youtube.com', 'youtu.be',
    'linkedin.com', 'icptrack.com', 'click.icptrack', 'eventbrite',
    'addtoany.com', 'readspeaker.com', 'cookiebot.com', 'snapchat.com',
    'pinterest.com', 'tiktok.com', 'whatsapp.com', 'wa.me',
  ];
  $('a').each((_, el) => {
    const href = $(el).attr('href') || '';
    if (website) return;
    if (!href.startsWith('http')) return;
    const isSkip = skipLinkDomains.some(d => href.includes(d));
    if (isSkip) return;
    website = href;
  });

  // Address: structured field
  // The profile text often has "Address" followed by the street address on the next line
  const addrMatch = text.match(/(?:^|\n)\s*(?:Address|Location)\s*[:\s]*([^\n]+(?:[A-Z]\d[A-Z]\s?\d[A-Z]\d)[^\n]*)/im);
  if (addrMatch) address = addrMatch[1].trim();
  if (!address) {
    // Look for address with Canadian postal code
    const postalMatch = text.match(/([^\n]*\d+[^\n]*[A-Z]\d[A-Z]\s?\d[A-Z]\d[^\n]*)/);
    if (postalMatch) address = postalMatch[1].trim();
  }

  // Location from address
  if (address) {
    const parsed = parseCanadianAddress(address);
    city = parsed.city;
    state = parsed.state;
  }

  // Fallback: find City, Province pattern in the main content text
  // But be selective — only match in lines that look like address/location data
  if (!state) {
    const provPattern = /([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s*(ON|BC|AB|SK|MB|QC|NB|NS|PE|NL|NT|NU|YT)\s+[A-Z]\d[A-Z]/;
    const provMatch = text.match(provPattern);
    if (provMatch) {
      if (!city) city = provMatch[1].trim();
      state = provMatch[2];
    }
  }
  // Even simpler: city, PROVINCE (without postal code)
  if (!state) {
    const simpleMatch = text.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*(ON|BC|AB|SK|MB|QC|NB|NS|PE|NL|NT|NU|YT)\b/);
    if (simpleMatch) {
      if (!city) city = simpleMatch[1].trim();
      state = simpleMatch[2];
    }
  }

  // Validate city — reject if it looks like cookie/legal text or too short/generic
  if (city && (
    city.length > 40 || city.length < 3 ||
    /retarget|survey|cookie|statist|consent|analytic|^the$/i.test(city) ||
    /^PO Box|^Box |^Unit |^Suite |^\d/i.test(city)
  )) {
    city = '';
  }

  // Phone
  const phoneMatch = text.match(/(?:Phone|Telephone|Tel)[:\s]*([+\d][\d\s().-]{7,})/i);
  if (phoneMatch) phone = phoneMatch[1].trim();
  if (!phone) {
    const $tel = $('a[href^="tel:"]').first();
    if ($tel.length) phone = $tel.attr('href').replace('tel:', '').trim();
  }

  return { email, phone: cleanPhone(phone), website: normalizeUrl(website), city, state };
}

async function scrapeLanguagesCanada(isTest) {
  const entries = await scrapeLCListing(isTest);
  const maxProfiles = isTest ? 5 : entries.length;
  const toProcess = entries.slice(0, maxProfiles);

  log(`  Scraping ${toProcess.length} profiles...`);
  const leads = [];

  for (let i = 0; i < toProcess.length; i++) {
    const entry = toProcess[i];
    log(`  [${i + 1}/${toProcess.length}] ${entry.name}`);

    try {
      await randomDelay();
      const html = await httpGet(entry.profileUrl);
      const profile = parseLCProfile(html);

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: entry.name,
        title: 'Language School',
        email: profile.email,
        phone: profile.phone,
        website: profile.website,
        domain: extractDomain(profile.website),
        city: profile.city,
        state: profile.state,
        country: 'CA',
        niche: 'education',
        source: 'languages-canada',
        profile_url: entry.profileUrl,
      });

      const fields = [];
      if (profile.email) fields.push(`email=${profile.email}`);
      if (profile.phone) fields.push(`phone=${profile.phone}`);
      if (profile.website) fields.push(`web=${extractDomain(profile.website)}`);
      if (profile.city) fields.push(`city=${profile.city}`);
      log(`    OK: ${fields.join(' | ') || '(minimal data)'}`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
      leads.push({
        first_name: '', last_name: '',
        firm_name: entry.name,
        title: 'Language School',
        email: '', phone: '', website: '', domain: '',
        city: '', state: '', country: 'CA',
        niche: 'education',
        source: 'languages-canada',
        profile_url: entry.profileUrl,
      });
    }
  }

  return leads;
}

// ── Source 4: University of Saskatchewan Agent List ──────────────────────────

async function scrapeUSaskAgents(isTest) {
  log('Source 4: University of Saskatchewan — fetching agent table...');
  const url = 'https://admissions.usask.ca/requirements/agents.php';

  try {
    const html = await httpGet(url);
    const $ = cheerio.load(html);
    const leads = [];

    // Static HTML table with agency name (linked to website) and country
    $('table tr, tbody tr').each((_, row) => {
      const $row = $(row);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const $link = $(cells[0]).find('a');
      const agencyName = $(cells[0]).text().trim();
      const country = $(cells[1]).text().trim();
      let website = $link.attr('href') || '';

      if (!agencyName || agencyName.toLowerCase() === 'agency') return; // Skip header

      // Only include Canadian or Global agents
      // (Many agents listed are from India, China, etc. — include all as they serve Canadian education)

      if (website && !website.startsWith('http')) {
        website = 'https://' + website;
      }

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: agencyName,
        title: 'Education Agent',
        email: '',
        phone: '',
        website: normalizeUrl(website),
        domain: extractDomain(website),
        city: '',
        state: '',
        country: country === 'Global' ? 'CA' : country,
        niche: 'education',
        source: 'usask-agents',
        profile_url: url,
      });
    });

    const limit = isTest ? 5 : leads.length;
    log(`  Found ${leads.length} agents (returning ${limit})`);
    return leads.slice(0, limit);

  } catch (err) {
    log(`  ERROR: ${err.message}`);
    return [];
  }
}

// ── Deduplication ───────────────────────────────────────────────────────────

function deduplicateLeads(leads) {
  const seen = new Map();
  const unique = [];

  for (const lead of leads) {
    // Build dedup keys — only use strong identifiers
    const keys = [];

    // Key 1: email (strong — unique per entity)
    if (lead.email) {
      keys.push(`email:${lead.email.toLowerCase()}`);
    }

    // Key 2: profile URL (strong — unique per listing)
    if (lead.profile_url) {
      keys.push(`url:${lead.profile_url.toLowerCase()}`);
    }

    // Key 3: firm name + source (less aggressive — only dedup within same source)
    if (lead.firm_name) {
      const normFirm = lead.firm_name.toLowerCase()
        .replace(/\b(inc|ltd|llc|corp|co|pty|pvt|limited|incorporated)\b/g, '')
        .replace(/[^a-z0-9]/g, '');
      if (normFirm.length > 5) {
        keys.push(`firm+src:${normFirm}:${lead.source}`);
      }
    }

    // Check if any key was already seen
    let isDupe = false;
    for (const key of keys) {
      if (seen.has(key)) {
        isDupe = true;
        // Merge: fill in missing fields from the duplicate
        const existing = seen.get(key);
        if (!existing.email && lead.email) existing.email = lead.email;
        if (!existing.phone && lead.phone) existing.phone = lead.phone;
        if (!existing.website && lead.website) { existing.website = lead.website; existing.domain = lead.domain; }
        if (!existing.city && lead.city) existing.city = lead.city;
        if (!existing.state && lead.state) existing.state = lead.state;
        if (!existing.first_name && lead.first_name) { existing.first_name = lead.first_name; existing.last_name = lead.last_name; }
        break;
      }
    }

    if (!isDupe) {
      for (const key of keys) {
        seen.set(key, lead);
      }
      unique.push(lead);
    }
  }

  return unique;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('╔══════════════════════════════════════════════════════════════╗');
  log('║  Canada Education Consultants / Agents Scraper              ║');
  log('╚══════════════════════════════════════════════════════════════╝');
  log(`Mode: ${isTest ? 'TEST (5 per source)' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);

  const allLeads = [];

  // ── Source 1: EducationAgentsGuide.com ───────────────────────────────────
  try {
    const eagLeads = await scrapeEducationAgentsGuide(isTest);
    log(`\nSource 1 complete: ${eagLeads.length} leads`);
    allLeads.push(...eagLeads);
    writeCSV(allLeads, OUT_FILE);
  } catch (err) {
    log(`Source 1 FAILED: ${err.message}`);
  }

  // ── Source 2: StudyAbroadLists.com ──────────────────────────────────────
  try {
    const salLeads = await scrapeStudyAbroadLists(isTest);
    log(`\nSource 2 complete: ${salLeads.length} leads`);
    allLeads.push(...salLeads);
    writeCSV(allLeads, OUT_FILE);
  } catch (err) {
    log(`Source 2 FAILED: ${err.message}`);
  }

  // ── Source 3: Languages Canada ──────────────────────────────────────────
  try {
    const lcLeads = await scrapeLanguagesCanada(isTest);
    log(`\nSource 3 complete: ${lcLeads.length} leads`);
    allLeads.push(...lcLeads);
    writeCSV(allLeads, OUT_FILE);
  } catch (err) {
    log(`Source 3 FAILED: ${err.message}`);
  }

  // ── Source 4: University of Saskatchewan ────────────────────────────────
  try {
    const usaskLeads = await scrapeUSaskAgents(isTest);
    log(`\nSource 4 complete: ${usaskLeads.length} leads`);
    allLeads.push(...usaskLeads);
    writeCSV(allLeads, OUT_FILE);
  } catch (err) {
    log(`Source 4 FAILED: ${err.message}`);
  }

  // ── Dedup & Final Write ─────────────────────────────────────────────────

  log('\n── Deduplication ──');
  log(`Before dedup: ${allLeads.length}`);
  const finalLeads = deduplicateLeads(allLeads);
  log(`After dedup: ${finalLeads.length} (removed ${allLeads.length - finalLeads.length} dupes)`);

  writeCSV(finalLeads, OUT_FILE);

  // ── Summary ─────────────────────────────────────────────────────────────

  const emailCount = finalLeads.filter(l => l.email).length;
  const phoneCount = finalLeads.filter(l => l.phone).length;
  const websiteCount = finalLeads.filter(l => l.website).length;
  const cityCount = finalLeads.filter(l => l.city).length;
  const stateCount = finalLeads.filter(l => l.state).length;

  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${finalLeads.length}`);
  log(`With email: ${emailCount}`);
  log(`With phone: ${phoneCount}`);
  log(`With website: ${websiteCount}`);
  log(`With city: ${cityCount}`);
  log(`With province: ${stateCount}`);

  // Source breakdown
  const bySrc = {};
  finalLeads.forEach(l => { bySrc[l.source] = (bySrc[l.source] || 0) + 1; });
  log('\nBy source:');
  Object.entries(bySrc).sort((a, b) => b[1] - a[1]).forEach(([src, n]) => log(`  ${src}: ${n}`));

  // Province breakdown
  const byProv = {};
  finalLeads.forEach(l => { byProv[l.state || 'unknown'] = (byProv[l.state || 'unknown'] || 0) + 1; });
  log('\nBy province:');
  Object.entries(byProv).sort((a, b) => b[1] - a[1]).forEach(([prov, n]) => log(`  ${prov}: ${n}`));

  log(`\nOutput: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
