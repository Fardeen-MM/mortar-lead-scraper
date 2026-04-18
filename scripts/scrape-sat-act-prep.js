#!/usr/bin/env node
/**
 * SAT/ACT Test Prep & College Essay Consultant Scraper
 *
 * Scrapes multiple sources for individual tutors and consultants:
 *
 *   1. Wyzant tutor directory — SAT, ACT, PSAT, College Admissions, College Essay, GRE, GMAT
 *      25 featured tutors per subject page (SSR HTML with profile links)
 *   2. Thumbtack — SAT prep, ACT prep, college counseling across 30+ US cities
 *      JSON-LD LocalBusiness listings (10 per city page)
 *   3. Kaplan leadership team — team page with name + title
 *   4. Revolution Prep about page — founders and leadership
 *   5. College Essay Guy team page — Ethan Sawyer + team
 *   6. Prep company team pages — Applerouth, Compass, PrepMaven, etc.
 *
 * Skipped sources (not viable for HTTP scraping):
 *   - Varsity Tutors: 410 on old URLs, new Next.js SPA requires JS rendering
 *   - Niche.com: 404 on college-counselors path
 *   - Princeton Review: 404 on tutor pages
 *   - Superprof: Cloudflare CAPTCHA (403)
 *
 * Usage:
 *   node scripts/scrape-sat-act-prep.js --test    # Test mode (limited pages)
 *   node scripts/scrape-sat-act-prep.js --all     # Full scrape all sources
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'sat-act-prep-tutors.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

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
  const str = String(val).trim();
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
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return rawPhone.trim();
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/^(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?)\s+/i, '')
    .trim();

  const commaIdx = cleaned.indexOf(',');
  if (commaIdx > 0) {
    cleaned = cleaned.substring(0, commaIdx).trim();
  }

  cleaned = cleaned
    .replace(/\s+(CEP|PhD|EdD|MA|MEd|EdM|MS|MPA|JD|MBA|LCSW|LPC|LCPC|LMFT|MPP|Esq|Ph\.D\.|Ed\.D\.|M\.A\.|M\.Ed\.|Ed\.M\.|M\.S\.|M\.P\.A\.|J\.D\.|M\.B\.A\.|M\.P\.P\.?)\.?$/gi, '')
    .trim();

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  const parts = cleaned.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

function dedupKey(lead) {
  const firstName = (lead.first_name || '').toLowerCase().trim();
  const lastName = (lead.last_name || '').toLowerCase().trim();
  const name = `${firstName}|${lastName}`;
  const city = (lead.city || '').toLowerCase().trim();
  const email = (lead.email || '').toLowerCase().trim();
  const profileUrl = (lead.profile_url || '').toLowerCase().trim();
  const firmName = (lead.firm_name || '').toLowerCase().trim();
  if (email) return `email:${email}`;
  // Only use profile_url for dedup if it looks like a unique profile page (contains an ID)
  if (profileUrl && /\/\d+\/?$/.test(profileUrl)) return `url:${profileUrl}`;
  if (name !== '|' && city) return `name:${name}|${city}`;
  if (name !== '|' && firmName) return `name:${name}|${firmName}`;
  if (name !== '|') return `name:${name}`;
  if (firmName && city) return `firm:${firmName}|${city}`;
  if (firmName) return `firm:${firmName}`;
  return `rand:${Math.random()}`;
}

/**
 * Check if a string looks like a real person name (not a heading/slogan)
 * Rejects common English words that could be mistaken for names
 */
const NON_NAME_WORDS = new Set([
  'build', 'own', 'flexible', 'first', 'last', 'next', 'best', 'great', 'fast',
  'free', 'open', 'close', 'learn', 'grow', 'lead', 'find', 'make', 'take',
  'give', 'keep', 'just', 'more', 'less', 'high', 'low', 'new', 'old', 'big',
  'small', 'real', 'true', 'full', 'half', 'good', 'better', 'start', 'stop',
  'read', 'write', 'speak', 'think', 'dream', 'work', 'play', 'live', 'love',
  'act', 'create', 'bridge', 'bridges', 'test', 'prep', 'bold', 'brave',
  'smart', 'strong', 'deep', 'wide', 'long', 'short', 'hard', 'soft', 'safe',
  'pure', 'clear', 'clean', 'warm', 'cool', 'hot', 'cold', 'dark', 'light',
  'above', 'beyond', 'forward', 'always', 'every', 'never', 'together',
  // Academic institution words (often in image alts on education sites)
  'university', 'univ', 'college', 'institute', 'school', 'academy', 'center',
  'logo', 'icon', 'image', 'photo', 'banner', 'header', 'footer',
  // Short common words that aren't names
  'it', 'us', 'up', 'go', 'do', 'be', 'we', 'me', 'he', 'no', 'so',
  'on', 'in', 'at', 'to', 'by', 'or', 'an', 'if', 'am', 'is', 'as',
  // More slogans/values
  'commit', 'inspire', 'engage', 'impact', 'connect', 'empower', 'serve',
  'thrive', 'excel', 'succeed', 'achieve', 'innovate', 'explore', 'discover',
]);

function looksLikePersonName(name) {
  if (!name) return false;
  const words = name.split(/\s+/);
  if (words.length < 2 || words.length > 3) return false;
  // All words must start with uppercase
  if (!words.every(w => /^[A-Z]/.test(w))) return false;
  // First word (first name) must NOT be a common/blocklisted word
  if (NON_NAME_WORDS.has(words[0].toLowerCase())) return false;
  // Last word must NOT be a blocklisted word (catches "X University", "Y College")
  if (NON_NAME_WORDS.has(words[words.length - 1].toLowerCase())) return false;
  // First word should look like a first name (not too short)
  if (words[0].length < 2) return false;
  // Last word should be at least 2 chars
  if (words[words.length - 1].length < 2) return false;
  return true;
}

/**
 * Decode HTML entities (e.g. &amp; -> &, &apos; -> ')
 */
function decodeEntities(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&apos;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x2d;/g, '-')
    .replace(/&#\d+;/g, m => String.fromCharCode(parseInt(m.slice(2, -1))));
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        ...extraHeaders,
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = mod.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries, extraHeaders).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
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
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Source 1: Wyzant Tutor Directory ─────────────────────────────────────────
// URL pattern: https://www.wyzant.com/{SUBJECT}_tutors.aspx
// SSR HTML with 25 featured tutor links per page
// Profile URL: /Tutors/{STATE}/{City_Slug}/{ID}/
// Link text: "Name Rate/hour Headline Bio..."

async function scrapeWyzant(isTest) {
  const SOURCE = 'wyzant';
  const leads = [];
  const seenIds = new Set();

  const subjects = isTest
    ? ['SAT', 'ACT', 'College_Admissions']
    : ['SAT', 'ACT', 'PSAT', 'College_Admissions', 'College_Essay', 'GRE', 'GMAT'];

  log(`\n=== Wyzant Tutor Directory ===`);
  log(`Scraping ${subjects.length} subject pages...`);

  for (const subject of subjects) {
    const url = `https://www.wyzant.com/${subject}_tutors.aspx`;
    log(`  [Wyzant] ${subject}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Extract tutor profile links: /Tutors/STATE/City_Slug/ID/
      $('a[href*="/Tutors/"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const urlMatch = href.match(/\/Tutors\/([A-Z]{2})\/([^/]+)\/(\d+)/);
        if (!urlMatch) return;

        const tutorId = urlMatch[3];
        if (seenIds.has(tutorId)) return;
        seenIds.add(tutorId);

        const state = urlMatch[1];
        const city = urlMatch[2].replace(/_/g, ' ');

        // Parse name from link text: "FirstName LastInitial.Rate/hourHeadline..."
        const fullText = $(el).text().trim();
        const nameMatch = fullText.match(/^([A-Z][a-z]+)\s+([A-Z]\.?)/);
        if (!nameMatch) return;

        const firstName = nameMatch[1];
        const lastInitial = nameMatch[2].replace('.', '');

        // Extract headline (text between rate and bio)
        const headlineMatch = fullText.match(/\/hour(.+?)(?:I |My |With |As |A |The |For |Having |Getting )/);
        let title = '';
        if (headlineMatch) {
          title = headlineMatch[1].trim();
          if (title.length > 80) title = title.substring(0, 80);
        }
        if (!title) {
          // Try to get title from the text after rate
          const rateMatch = fullText.match(/\/hour(.{10,60})/);
          if (rateMatch) {
            title = rateMatch[1].trim();
            // Cut at first sentence boundary
            const sentEnd = title.match(/[.!]\s/);
            if (sentEnd) title = title.substring(0, sentEnd.index + 1);
            if (title.length > 60) title = title.substring(0, 60);
          }
        }
        if (!title) title = `${subject.replace(/_/g, ' ')} Tutor`;

        const profileUrl = href.startsWith('http') ? href : `https://www.wyzant.com${href}`;

        leads.push({
          first_name: firstName,
          last_name: lastInitial,
          firm_name: '',
          title,
          email: '',
          phone: '',
          website: profileUrl,
          domain: 'wyzant.com',
          city,
          state,
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: profileUrl,
        });
        pageLeads++;
      });

      log(`    Found ${pageLeads} tutors`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Wyzant total: ${leads.length} leads`);
  return leads;
}

// ── Source 2: Thumbtack SAT/ACT/College Prep ────────────────────────────────
// URL: https://www.thumbtack.com/{state-slug}/{city-slug}/{service}/
// Services: sat-prep, act-prep, college-counseling
// Data: JSON-LD LocalBusiness listings (10 per page)

const THUMBTACK_CITIES = [
  { city: 'New York', state: 'NY', slug: 'ny/new-york' },
  { city: 'Los Angeles', state: 'CA', slug: 'ca/los-angeles' },
  { city: 'Chicago', state: 'IL', slug: 'il/chicago' },
  { city: 'Houston', state: 'TX', slug: 'tx/houston' },
  { city: 'Phoenix', state: 'AZ', slug: 'az/phoenix' },
  { city: 'Philadelphia', state: 'PA', slug: 'pa/philadelphia' },
  { city: 'San Antonio', state: 'TX', slug: 'tx/san-antonio' },
  { city: 'San Diego', state: 'CA', slug: 'ca/san-diego' },
  { city: 'Dallas', state: 'TX', slug: 'tx/dallas' },
  { city: 'San Jose', state: 'CA', slug: 'ca/san-jose' },
  { city: 'Austin', state: 'TX', slug: 'tx/austin' },
  { city: 'Jacksonville', state: 'FL', slug: 'fl/jacksonville' },
  { city: 'Fort Worth', state: 'TX', slug: 'tx/fort-worth' },
  { city: 'Columbus', state: 'OH', slug: 'oh/columbus' },
  { city: 'Charlotte', state: 'NC', slug: 'nc/charlotte' },
  { city: 'Indianapolis', state: 'IN', slug: 'in/indianapolis' },
  { city: 'San Francisco', state: 'CA', slug: 'ca/san-francisco' },
  { city: 'Seattle', state: 'WA', slug: 'wa/seattle' },
  { city: 'Denver', state: 'CO', slug: 'co/denver' },
  { city: 'Washington', state: 'DC', slug: 'dc/washington' },
  { city: 'Nashville', state: 'TN', slug: 'tn/nashville' },
  { city: 'Oklahoma City', state: 'OK', slug: 'ok/oklahoma-city' },
  { city: 'Boston', state: 'MA', slug: 'ma/boston' },
  { city: 'Portland', state: 'OR', slug: 'or/portland' },
  { city: 'Las Vegas', state: 'NV', slug: 'nv/las-vegas' },
  { city: 'Miami', state: 'FL', slug: 'fl/miami' },
  { city: 'Atlanta', state: 'GA', slug: 'ga/atlanta' },
  { city: 'Minneapolis', state: 'MN', slug: 'mn/minneapolis' },
  { city: 'Raleigh', state: 'NC', slug: 'nc/raleigh' },
  { city: 'Tampa', state: 'FL', slug: 'fl/tampa' },
  { city: 'St. Louis', state: 'MO', slug: 'mo/saint-louis' },
  { city: 'Pittsburgh', state: 'PA', slug: 'pa/pittsburgh' },
  { city: 'Cincinnati', state: 'OH', slug: 'oh/cincinnati' },
  { city: 'Orlando', state: 'FL', slug: 'fl/orlando' },
  { city: 'Salt Lake City', state: 'UT', slug: 'ut/salt-lake-city' },
];

const THUMBTACK_SERVICES = [
  { service: 'sat-prep', label: 'SAT Prep' },
  { service: 'act-prep', label: 'ACT Prep' },
  { service: 'college-counseling', label: 'College Counseling' },
];

async function scrapeThumbtack(isTest) {
  const SOURCE = 'thumbtack';
  const leads = [];
  const seenNames = new Set();

  const cities = isTest ? THUMBTACK_CITIES.slice(0, 5) : THUMBTACK_CITIES;
  const services = isTest ? THUMBTACK_SERVICES.slice(0, 2) : THUMBTACK_SERVICES;

  log(`\n=== Thumbtack SAT/ACT/College Prep ===`);
  log(`Scraping ${cities.length} cities x ${services.length} services...`);

  for (const svc of services) {
    log(`  [Thumbtack] Service: ${svc.label}`);

    for (const ct of cities) {
      const url = `https://www.thumbtack.com/${ct.slug}/${svc.service}/`;
      log(`    ${ct.city}, ${ct.state}: ${url}`);

      try {
        const { statusCode, body } = await httpGet(url);
        if (statusCode !== 200 || !body) {
          log(`      HTTP ${statusCode} - skipping`);
          continue;
        }

        const $ = cheerio.load(body);
        let pageLeads = 0;

        // Extract JSON-LD LocalBusiness listings
        $('script[type="application/ld+json"]').each((_, el) => {
          try {
            const data = JSON.parse($(el).html());
            if (!data.itemListElement || data.itemListElement.length <= 4) return;

            for (const item of data.itemListElement) {
              const biz = item.item;
              if (!biz || biz['@type'] !== 'LocalBusiness') continue;

              let bizName = decodeEntities(biz.name || '');
              if (!bizName) continue;

              // Dedup by normalized name
              const nameKey = bizName.toLowerCase().replace(/[^a-z0-9]/g, '');
              if (seenNames.has(nameKey)) continue;
              seenNames.add(nameKey);

              // Try to extract person name from business name
              // Patterns: "FirstName LastName Tutoring", "FirstName | ...", "FirstName LastName - ..."
              let firstName = '', lastName = '', firmName = bizName;

              // "John Linneball Tutoring" or "Jeffrey Michael Tutoring"
              const namePattern = bizName.match(/^([A-Z][a-z]+)\s+([A-Z][a-z]+)\s+(Tutoring|Tutor|Prep|Learning|Education|Coaching|Counseling)/i);
              if (namePattern) {
                firstName = namePattern[1];
                lastName = namePattern[2];
                firmName = bizName;
              }

              // "Sebastian | VIRTUAL ONLY | SAT/ACT | 1540/34"
              const pipePattern = bizName.match(/^([A-Z][a-z]+)\s*\|/);
              if (!firstName && pipePattern) {
                firstName = pipePattern[1];
                firmName = bizName;
              }

              // "Stanford Grad - Jeffrey Michael Tutoring"
              const dashPattern = bizName.match(/- ([A-Z][a-z]+)\s+([A-Z][a-z]+)\s+(Tutoring|Prep)/i);
              if (!firstName && dashPattern) {
                firstName = dashPattern[1];
                lastName = dashPattern[2];
                firmName = bizName;
              }

              // Build profile URL
              const profilePath = biz.url || '';
              const profileUrl = profilePath.startsWith('http')
                ? profilePath
                : `https://www.thumbtack.com${profilePath}`;

              // Price
              const price = biz.priceRange || '';

              // Rating
              let rating = '';
              if (biz.aggregateRating) {
                rating = `${biz.aggregateRating.ratingValue} (${biz.aggregateRating.reviewCount} reviews)`;
              }

              leads.push({
                first_name: firstName,
                last_name: lastName,
                firm_name: firmName,
                title: svc.label + (price ? ` - ${price}` : ''),
                email: '',
                phone: '',
                website: profileUrl,
                domain: 'thumbtack.com',
                city: ct.city,
                state: ct.state,
                country: 'US',
                niche: 'education',
                source: SOURCE,
                profile_url: profileUrl,
              });
              pageLeads++;
            }
          } catch {}
        });

        log(`      Found ${pageLeads} businesses`);

      } catch (err) {
        log(`      ERROR: ${err.message}`);
      }

      await randomDelay();
    }
  }

  log(`  Thumbtack total: ${leads.length} leads`);
  return leads;
}

// ── Source 3: Kaplan Leadership Team ─────────────────────────────────────────
// URL: https://kaplan.com/about/our-team
// Structure: MUI personCard components with personName and personTitle classes

async function scrapeKaplan(isTest) {
  const SOURCE = 'kaplan';
  const leads = [];

  log(`\n=== Kaplan Leadership Team ===`);
  const url = 'https://kaplan.com/about/our-team';
  log(`  [Kaplan] ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`    HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);

    // Extract from personCard MUI components
    // Match only Card root elements (class starts with "Card personCard")
    $('div[class*="personCard"]').each((_, el) => {
      const card = $(el);
      // Only process the root card div — skip inner spans/p with personCard in class
      const className = card.attr('class') || '';
      if (!className.includes('Card ')) return;

      const name = card.find('[class*="personName"]').first().text().trim();
      const title = card.find('[class*="personTitle"]').first().text().trim();

      if (!name || name.length > 50) return;

      const { first_name, last_name } = splitName(name);
      if (!first_name) return;

      // Skip if already added
      if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) return;

      leads.push({
        first_name,
        last_name,
        firm_name: 'Kaplan',
        title: title || 'Leadership',
        email: '',
        phone: '',
        website: 'https://kaplan.com',
        domain: 'kaplan.com',
        city: 'Fort Lauderdale',
        state: 'FL',
        country: 'US',
        niche: 'education',
        source: SOURCE,
        profile_url: url,
      });
    });

    // Fallback: extract from headings
    if (leads.length === 0) {
      const knownNames = new Set();
      $('h3, h4, h5').each((_, el) => {
        const text = $(el).text().trim().replace(/\s+/g, ' ');
        if (text.length < 4 || text.length > 40) return;
        if (!/^[A-Z][a-z]+\s+[A-Z]/.test(text)) return;
        if (knownNames.has(text)) return;
        knownNames.add(text);

        const { first_name, last_name } = splitName(text);
        if (!first_name || !last_name) return;

        // Get title from next sibling
        let title = '';
        const next = $(el).next('p, span, div');
        const nextText = next.text().trim();
        if (nextText && nextText.length < 80) title = nextText;

        leads.push({
          first_name,
          last_name,
          firm_name: 'Kaplan',
          title: title || 'Leadership',
          email: '',
          phone: '',
          website: 'https://kaplan.com',
          domain: 'kaplan.com',
          city: 'Fort Lauderdale',
          state: 'FL',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
      });
    }

    log(`    Found ${leads.length} team members`);

  } catch (err) {
    log(`    ERROR: ${err.message}`);
  }

  log(`  Kaplan total: ${leads.length} leads`);
  return leads;
}

// ── Source 4: Revolution Prep ────────────────────────────────────────────────
// URL: https://www.revolutionprep.com/about/
// Structure: Headings for "Our Founders", "Our Academic Advisors", etc.
// Also scrape the /our-tutors/ page if available

async function scrapeRevolutionPrep(isTest) {
  const SOURCE = 'revolution-prep';
  const leads = [];

  log(`\n=== Revolution Prep ===`);

  const urls = [
    { url: 'https://www.revolutionprep.com/about/', type: 'about' },
    { url: 'https://www.revolutionprep.com/our-tutors/', type: 'tutors' },
    { url: 'https://www.revolutionprep.com/team/', type: 'team' },
  ];

  for (const { url, type } of urls) {
    log(`  [RevPrep] ${type}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);

      // Strategy 1: Look for person names in headings
      $('h2, h3, h4, h5').each((_, el) => {
        const name = $(el).text().trim().replace(/\s+/g, ' ');
        if (!name || name.length > 40 || name.length < 4) return;
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$/.test(name)) return;
        if (name.split(/\s+/).length > 3) return;
        if (!looksLikePersonName(name)) return;
        // Skip section headings
        if (/about|team|our|meet|staff|company|expert|approach|advisor|tutor|founder|cart|professional/i.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (!first_name || !last_name) return;
        if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) return;

        let title = 'SAT/ACT Tutor';
        const nextEl = $(el).next('p, span, div');
        const nextText = nextEl.text().trim();
        if (nextText && nextText.length < 80 && nextText.length > 3 &&
            /director|founder|manager|lead|head|chief|tutor|coach|counselor|president|ceo|coo|vp|advisor/i.test(nextText)) {
          title = nextText;
        }

        leads.push({
          first_name,
          last_name,
          firm_name: 'Revolution Prep',
          title,
          email: '',
          phone: '',
          website: 'https://www.revolutionprep.com',
          domain: 'revolutionprep.com',
          city: 'Los Angeles',
          state: 'CA',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
      });

      // Strategy 2: JSON-LD
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          const people = [];
          if (json.employee) people.push(...(Array.isArray(json.employee) ? json.employee : [json.employee]));
          if (json.founder) people.push(...(Array.isArray(json.founder) ? json.founder : [json.founder]));

          for (const person of people) {
            if (!person || !person.name) continue;
            const { first_name, last_name } = splitName(person.name);
            if (!first_name || !last_name) continue;
            if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) continue;

            leads.push({
              first_name,
              last_name,
              firm_name: 'Revolution Prep',
              title: person.jobTitle || 'Team Member',
              email: person.email || '',
              phone: person.telephone ? formatPhone(person.telephone) : '',
              website: 'https://www.revolutionprep.com',
              domain: 'revolutionprep.com',
              city: 'Los Angeles',
              state: 'CA',
              country: 'US',
              niche: 'education',
              source: SOURCE,
              profile_url: url,
            });
          }
        } catch {}
      });

      log(`    Found ${leads.length} team members so far`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Revolution Prep total: ${leads.length} leads`);
  return leads;
}

// ── Source 5: College Essay Guy Team ─────────────────────────────────────────
// URL: https://www.collegeessayguy.com/our-team (Squarespace)
// Founder: Ethan Sawyer
// Team page may have images with alt text or heading-based names

async function scrapeCollegeEssayGuy(isTest) {
  const SOURCE = 'college-essay-guy';
  const leads = [];

  log(`\n=== College Essay Guy ===`);

  const urls = [
    'https://www.collegeessayguy.com/our-team',
    'https://www.collegeessayguy.com/about',
  ];

  for (const url of urls) {
    log(`  [CEG] ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);

      // Look for person names in content blocks (Squarespace patterns)
      // Strategy 1: Headings that look like person names
      $('h2, h3, h4').each((_, el) => {
        const name = $(el).text().trim().replace(/\s+/g, ' ');
        if (!name || name.length > 40 || name.length < 4) return;
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$/.test(name)) return;
        if (name.split(/\s+/).length > 3) return;
        if (!looksLikePersonName(name)) return;
        if (/meet|team|our|check|core|value|work|resource|why|how|dare|guide|essay|college|personal|free|parent|counselor|partner/i.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (!first_name || !last_name) return;
        if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) return;

        let title = 'College Essay Consultant';
        const nextEl = $(el).next('p, span, div');
        const nextText = nextEl.text().trim();
        if (nextText && nextText.length < 80 && nextText.length > 3) {
          if (/founder|counselor|advisor|coach|director|consultant|editor|writer|tutor/i.test(nextText)) {
            title = nextText;
          }
        }

        leads.push({
          first_name,
          last_name,
          firm_name: 'College Essay Guy',
          title,
          email: '',
          phone: '',
          website: 'https://www.collegeessayguy.com',
          domain: 'collegeessayguy.com',
          city: 'Los Angeles',
          state: 'CA',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
      });

      // Strategy 2: Image alt attributes that look like person names
      $('img[alt]').each((_, el) => {
        const alt = $(el).attr('alt') || '';
        if (!alt || alt.length > 40 || alt.length < 4) return;
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+$/.test(alt)) return;
        if (!looksLikePersonName(alt)) return;
        if (/college|essay|guy|team|today|show/i.test(alt)) return;

        const { first_name, last_name } = splitName(alt);
        if (!first_name || !last_name) return;
        if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) return;

        leads.push({
          first_name,
          last_name,
          firm_name: 'College Essay Guy',
          title: 'College Essay Consultant',
          email: '',
          phone: '',
          website: 'https://www.collegeessayguy.com',
          domain: 'collegeessayguy.com',
          city: 'Los Angeles',
          state: 'CA',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
      });

      log(`    Found ${leads.length} team members so far`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  // Always include the founder
  if (!leads.some(l => l.first_name === 'Ethan' && l.last_name === 'Sawyer')) {
    leads.push({
      first_name: 'Ethan',
      last_name: 'Sawyer',
      firm_name: 'College Essay Guy',
      title: 'Founder, College Essay Consultant',
      email: '',
      phone: '',
      website: 'https://www.collegeessayguy.com',
      domain: 'collegeessayguy.com',
      city: 'Los Angeles',
      state: 'CA',
      country: 'US',
      niche: 'education',
      source: SOURCE,
      profile_url: 'https://www.collegeessayguy.com/about',
    });
  }

  log(`  College Essay Guy total: ${leads.length} leads`);
  return leads;
}

// ── Source 6: Prep Company Team Pages ────────────────────────────────────────
// Scrape about/team pages of notable SAT/ACT prep and college consulting firms

async function scrapePrepCompanies(isTest) {
  const SOURCE = 'prep-companies';
  const leads = [];

  const pages = [
    { url: 'https://www.compassprep.com/about/', firm: 'Compass Education Group', type: 'Education Consultant', city: 'San Francisco', state: 'CA' },
    { url: 'https://prepmaven.com/about/', firm: 'PrepMaven', type: 'College Prep Consultant', city: 'Westport', state: 'CT' },
    { url: 'https://www.applerouth.com/about/', firm: 'Applerouth', type: 'SAT/ACT Tutor', city: 'Atlanta', state: 'GA' },
    { url: 'https://www.collegewise.com/about', firm: 'Collegewise', type: 'College Admissions Counselor', city: 'Irvine', state: 'CA' },
    { url: 'https://www.testive.com/about/', firm: 'Testive', type: 'SAT/ACT Tutor', city: 'Boston', state: 'MA' },
    { url: 'https://www.door2doorcollegecounseling.com/about', firm: 'Door2Door College Counseling', type: 'College Admissions Consultant', city: '', state: '' },
    { url: 'https://www.methodtestprep.com/about/', firm: 'Method Test Prep', type: 'SAT/ACT Prep Instructor', city: '', state: '' },
    { url: 'https://www.prepexpert.com/about-us/', firm: 'Prep Expert', type: 'SAT/ACT Prep Instructor', city: 'Los Angeles', state: 'CA' },
    { url: 'https://www.arborbridge.com/about/', firm: 'ArborBridge', type: 'SAT/ACT Tutor', city: 'Los Angeles', state: 'CA' },
    { url: 'https://www.signetec.com/about', firm: 'Signet Education', type: 'College Admissions Consultant', city: 'Cambridge', state: 'MA' },
  ];

  const pagesToScrape = isTest ? pages.slice(0, 3) : pages;

  log(`\n=== Prep Company Team Pages ===`);

  for (const page of pagesToScrape) {
    log(`  [Prep] ${page.firm}: ${page.url}`);

    try {
      const { statusCode, body } = await httpGet(page.url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Strategy 1: Names in heading elements
      $('h2, h3, h4, h5').each((_, el) => {
        const name = $(el).text().trim().replace(/\s+/g, ' ');
        if (!name || name.length > 40 || name.length < 4) return;
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$/.test(name)) return;
        if (name.split(/\s+/).length > 3) return;
        if (!looksLikePersonName(name)) return;
        // Skip section headings and marketing copy
        if (/about|team|our|meet|staff|company|what|how|why|the|contact|sign|join|blog|real|focus|combat|excellent|employee|satisfaction|workplace|college|future|student|success|outcome|expert|approach|professional|founder|academic|cart/i.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (!first_name || !last_name) return;
        if (leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === page.firm)) return;

        let title = page.type;
        const nextEl = $(el).next('p, span, div');
        const nextText = nextEl.text().trim();
        if (nextText && nextText.length < 80 && nextText.length > 3 &&
            /director|founder|manager|lead|head|chief|tutor|coach|counselor|president|ceo|coo|vp|advisor|instructor/i.test(nextText)) {
          title = nextText;
        }

        leads.push({
          first_name,
          last_name,
          firm_name: page.firm,
          title,
          email: '',
          phone: '',
          website: page.url.replace(/\/about\/?.*$/, '/'),
          domain: extractDomain(page.url),
          city: page.city,
          state: page.state,
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: page.url,
        });
        pageLeads++;
      });

      // Strategy 2: Image alt attributes that look like names
      $('img[alt]').each((_, el) => {
        const alt = $(el).attr('alt') || '';
        if (!alt || alt.length > 40 || alt.length < 4) return;
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+$/.test(alt)) return;
        if (!looksLikePersonName(alt)) return;

        const { first_name, last_name } = splitName(alt);
        if (!first_name || !last_name) return;
        if (leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === page.firm)) return;

        leads.push({
          first_name,
          last_name,
          firm_name: page.firm,
          title: page.type,
          email: '',
          phone: '',
          website: page.url.replace(/\/about\/?.*$/, '/'),
          domain: extractDomain(page.url),
          city: page.city,
          state: page.state,
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: page.url,
        });
        pageLeads++;
      });

      // Strategy 3: JSON-LD for organization/employee data
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          const people = [];
          if (json.employee) people.push(...(Array.isArray(json.employee) ? json.employee : [json.employee]));
          if (json.founder) people.push(...(Array.isArray(json.founder) ? json.founder : [json.founder]));
          if (json.member) people.push(...(Array.isArray(json.member) ? json.member : [json.member]));

          for (const person of people) {
            if (!person || !person.name) continue;
            const { first_name, last_name } = splitName(person.name);
            if (!first_name || !last_name) continue;
            if (leads.some(l => l.first_name === first_name && l.last_name === last_name)) continue;

            leads.push({
              first_name,
              last_name,
              firm_name: page.firm,
              title: person.jobTitle || page.type,
              email: person.email || '',
              phone: person.telephone ? formatPhone(person.telephone) : '',
              website: page.url.replace(/\/about\/?.*$/, '/'),
              domain: extractDomain(page.url),
              city: page.city,
              state: page.state,
              country: 'US',
              niche: 'education',
              source: SOURCE,
              profile_url: page.url,
            });
            pageLeads++;
          }
        } catch {}
      });

      log(`    Found ${pageLeads} team members`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Prep companies total: ${leads.length} leads`);
  return leads;
}

// ── Source 7: Wyzant Profile Page Enrichment ─────────────────────────────────
// Visit individual Wyzant tutor profile pages to get JSON-LD details
// Only run in --all mode (adds ~2 min per 25 profiles)

async function enrichWyzantProfiles(wyzantLeads, isTest) {
  if (isTest) return wyzantLeads;

  const maxProfiles = 50; // Limit to avoid rate limiting
  const profilesToEnrich = wyzantLeads.slice(0, maxProfiles);

  log(`\n=== Wyzant Profile Enrichment ===`);
  log(`Enriching up to ${profilesToEnrich.length} profiles...`);

  let enriched = 0;

  for (const lead of profilesToEnrich) {
    if (!lead.profile_url) continue;

    try {
      const { statusCode, body } = await httpGet(lead.profile_url);
      if (statusCode !== 200 || !body) continue;

      const $ = cheerio.load(body);

      // Extract from JSON-LD ProfessionalService
      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const data = JSON.parse($(el).html());
          if (data['@type'] === 'ProfessionalService') {
            // Name is "FirstName LastInitial. Tutoring" — not very useful
            // But description can be a good title
            if (data.description && !lead.title) {
              lead.title = data.description.substring(0, 80);
            }

            // Address
            if (data.address) {
              if (data.address.addressLocality) lead.city = data.address.addressLocality;
              if (data.address.addressRegion) lead.state = data.address.addressRegion;
            }
          }
        } catch {}
      });

      // Extract education info from bio
      const title = $('title').text();
      // Title format: "FirstName LastInitial. - Subject, Subject Tutor in City, STATE | Wyzant"
      const titleMatch = title.match(/^([A-Z][a-z]+)\s+([A-Z])\.\s*-\s*(.+?)\s+Tutor/i);
      if (titleMatch) {
        lead.first_name = titleMatch[1];
        lead.last_name = titleMatch[2];
        if (!lead.title || lead.title.includes('Tutor')) {
          lead.title = titleMatch[3].trim() + ' Tutor';
        }
      }

      enriched++;

    } catch (err) {
      // Skip failed enrichments silently
    }

    await sleep(1500); // Gentler delay for profile pages
  }

  log(`  Enriched ${enriched}/${profilesToEnrich.length} profiles`);
  return wyzantLeads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-sat-act-prep.js --test    # Test mode (limited pages per source)');
    console.log('  node scripts/scrape-sat-act-prep.js --all     # Full scrape all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log(`SAT/ACT Test Prep & College Essay Consultant Scraper`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const seenKeys = new Set();
  const sourceStats = {};

  const sources = [
    { name: 'Wyzant', fn: () => scrapeWyzant(isTest) },
    { name: 'Thumbtack', fn: () => scrapeThumbtack(isTest) },
    { name: 'Kaplan', fn: () => scrapeKaplan(isTest) },
    { name: 'RevolutionPrep', fn: () => scrapeRevolutionPrep(isTest) },
    { name: 'CollegeEssayGuy', fn: () => scrapeCollegeEssayGuy(isTest) },
    { name: 'PrepCompanies', fn: () => scrapePrepCompanies(isTest) },
  ];

  for (const source of sources) {
    try {
      let leads = await source.fn();

      // Enrich Wyzant profiles in full mode
      if (source.name === 'Wyzant' && !isTest) {
        leads = await enrichWyzantProfiles(leads, isTest);
      }

      let added = 0;
      for (const lead of leads) {
        const key = dedupKey(lead);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        allLeads.push(lead);
        added++;
      }
      sourceStats[source.name] = { total: leads.length, unique: added };
      log(`\n  >> ${source.name}: ${leads.length} found, ${added} unique added`);
    } catch (err) {
      log(`\n  ERROR in ${source.name}: ${err.message}`);
      sourceStats[source.name] = { total: 0, unique: 0, error: err.message };
    }

    // Save intermediate progress
    writeCSV(allLeads, OUT_FILE);
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;

  log('');
  log('='.repeat(60));
  log('FINAL RESULTS');
  log('='.repeat(60));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With full name: ${withName}`);
  log(`With firm name: ${withFirm}`);
  log(`With email: ${withEmail}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log('');
  log('Source breakdown:');
  for (const [name, stats] of Object.entries(sourceStats)) {
    log(`  ${name}: ${stats.total} found, ${stats.unique} unique${stats.error ? ` (ERROR: ${stats.error})` : ''}`);
  }
  log('');
  log(`Output: ${OUT_FILE}`);

  // Force exit to avoid dangling HTTP connection retries
  process.exit(0);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
