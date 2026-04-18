#!/usr/bin/env node
/**
 * College Admissions Consultants — Multi-Directory Scraper
 *
 * Scrapes college admissions consultants from multiple directories:
 *
 *   1. HECA Member Directory (association.hecalive.org) — ~945 members, A-Z
 *   2. NACAC Independent Member Directory (hub.nacacnet.org) — Salesforce, page 1
 *   3. Top College Admissions Consultants Rankings (topcollegeadmissionsconsultants.com) — 20 firms
 *   4. Consulting Firm Team Pages — IvyWise, Accepted, Collegewise, Crimson, etc.
 *   5. NCAN Member Directory (ncan.org) — National College Access Network
 *
 * Skipped / unavailable:
 *   - CollegePlanningNetwork.com — parked domain (redirects to HugeDomains)
 *   - CollegeAdvisor.com/advisors — JS-rendered, connection times out
 *   - PrepScholar.com/sat/s/counselors — 404, no counselor directory exists
 *   - NACACnet.org counselor directory — redirects to hub.nacacnet.org (covered above)
 *   - ScholarshipPoints.com — redirects to Edvisors.com (no advisor directory)
 *
 * Usage:
 *   node scripts/scrape-college-consultants-directories.js --test    # Test mode (2 pages/letters per source)
 *   node scripts/scrape-college-consultants-directories.js --all     # Full scrape all sources
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
const OUT_FILE = path.join(OUT_DIR, 'college-admissions-consultants-directories.csv');

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
  const email = (lead.email || '').toLowerCase().trim();
  if (email) return `email:${email}`;

  const profileUrl = (lead.profile_url || '').toLowerCase().trim();
  if (profileUrl && profileUrl !== 'n/a') return `url:${profileUrl}`;

  const name = `${(lead.first_name || '').toLowerCase()}|${(lead.last_name || '').toLowerCase()}`;
  const city = (lead.city || '').toLowerCase().trim();
  if (name !== '|' && city) return `name:${name}|${city}`;
  if (name !== '|') return `name:${name}|${(lead.firm_name || '').toLowerCase()}`;

  return `rand:${Math.random()}`;
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

// ── Source 1: HECA Member Directory ──────────────────────────────────────────
// URL: https://association.hecalive.org/heca-member-directory/FindStartsWith?term=X
// ~945 members across A-Z pages
// Structure per card:
//   h5.gz-card-title > a[href*="Details"] = name
//   li.gz-card-phone > a[href^="tel:"] = phone
//   "Visit Website" link = website
//   li.gz-card-description .mn-text = company/description

async function scrapeHECA(isTest) {
  const SOURCE = 'heca-directory';
  const BASE_URL = 'https://association.hecalive.org/heca-member-directory/FindStartsWith?term=';
  const leads = [];
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
  const lettersToScrape = isTest ? letters.slice(0, 2) : letters;

  log(`\n=== HECA Member Directory ===`);
  log(`Scraping ${lettersToScrape.length} letter pages...`);

  for (const letter of lettersToScrape) {
    const url = `${BASE_URL}${letter}`;
    log(`  [HECA] Letter ${letter}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Find all member cards
      $('h5.gz-card-title a, h5.card-title a').each((_, el) => {
        const nameEl = $(el);
        const fullName = nameEl.text().trim();
        let detailPath = nameEl.attr('href') || '';

        if (detailPath.startsWith('//')) {
          detailPath = 'https:' + detailPath;
        } else if (detailPath.startsWith('/')) {
          detailPath = 'https://association.hecalive.org' + detailPath;
        }
        const profileUrl = detailPath;

        const card = nameEl.closest('.col-sm-6, .col-md-6, .col-lg-4, .card, div[class*="col"]');

        // Phone
        let phone = '';
        card.find('li.gz-card-phone a[href^="tel:"], a[href^="tel:"]').each((_, phoneEl) => {
          if (!phone) {
            const tel = $(phoneEl).attr('href').replace('tel:', '');
            phone = formatPhone(tel);
          }
        });

        // Website
        let website = '';
        card.find('a').each((_, linkEl) => {
          const text = $(linkEl).text().trim();
          const href = $(linkEl).attr('href') || '';
          if (/visit\s*website/i.test(text) && href) {
            if (href.startsWith('//')) website = 'https:' + href;
            else website = href;
          }
        });
        if (!website) {
          card.find('a[href]').each((_, linkEl) => {
            const href = $(linkEl).attr('href') || '';
            if (website) return;
            if (href.match(/^https?:\/\//) &&
                !href.includes('hecalive.org') &&
                !href.includes('cloudinary') &&
                !href.includes('facebook') && !href.includes('twitter') &&
                !href.includes('linkedin') && !href.includes('instagram') &&
                !href.includes('/Contact/') && !href.includes('/Details/')) {
              website = href;
            }
          });
        }

        // Company from description
        let company = '';
        card.find('li.gz-card-description .mn-text p, li.gz-card-description .mn-text').each((_, descEl) => {
          if (company) return;
          let text = $(descEl).text().trim();
          text = text.replace(/<[^>]+>/g, '').trim();
          if (text && text.length < 60 && text.length > 2 && text.split(' ').length <= 6) {
            if (!/^(founder|based|a\s|an\s|the\s|i\s|my\s|we\s|our\s|with\s|for\s|specializ)/i.test(text)) {
              company = text;
            }
          }
        });

        const { first_name, last_name } = splitName(fullName);

        if (first_name || last_name) {
          leads.push({
            first_name,
            last_name,
            firm_name: company,
            title: 'College Admissions Consultant',
            email: '',
            phone,
            website,
            domain: extractDomain(website),
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: profileUrl,
          });
          pageLeads++;
        }
      });

      log(`    Found ${pageLeads} members`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  HECA total: ${leads.length} leads`);
  return leads;
}

// ── Source 2: NACAC Independent Member Directory ─────────────────────────────
// Salesforce Visualforce: div.card > div.card-heading > span (name)
//   ul.card-detail > li > label.card-detail-label + span.card-detail-value
//   Labels: "Title", "Directory Address", "Email", "Phone"
// Only page 1 accessible via GET (subsequent pages require Salesforce form POST)

async function scrapeNACAC(isTest) {
  const SOURCE = 'nacac-directory';
  const BASE_URL = 'https://hub.nacacnet.org/independentmemberdirectory';
  const leads = [];
  const seenEmails = new Set();

  log(`\n=== NACAC Independent Member Directory ===`);
  log(`Scraping page 1 (Salesforce GET only)...`);

  try {
    const { statusCode, body } = await httpGet(BASE_URL);
    if (statusCode !== 200 || !body) {
      log(`  HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);
    let pageLeads = 0;

    $('div.card').each((_, cardEl) => {
      const card = $(cardEl);

      const name = card.find('.card-heading span').first().text().trim();
      if (!name) return;
      // Skip UI element text (Salesforce accordion buttons, etc.)
      if (/^(collapse|expand|show|hide|more|less|filter|search|sort|reset|clear)$/i.test(name)) return;
      // Must contain at least one letter
      if (!/[a-zA-Z]/.test(name)) return;

      // Extract labeled fields
      const fields = {};
      card.find('ul.card-detail li').each((_, liEl) => {
        const li = $(liEl);
        const label = li.find('label.card-detail-label').text().trim().replace(/:?\s*$/, '');
        const valueEl = li.find('span.card-detail-value');
        valueEl.find('br').replaceWith('\n');
        const value = valueEl.text().trim();
        if (label && value) fields[label.toLowerCase()] = value;
      });

      // Email from mailto link
      let email = '';
      card.find('a[href^="mailto:"]').each((_, el) => {
        if (!email) email = $(el).attr('href').replace('mailto:', '').trim();
      });

      if (email && seenEmails.has(email)) return;
      if (email) seenEmails.add(email);

      // Phone
      let phone = '';
      const phoneField = fields['phone'] || fields['telephone'] || '';
      if (phoneField) {
        phone = formatPhone(phoneField);
      } else {
        card.find('a[href^="tel:"]').each((_, el) => {
          if (!phone) phone = formatPhone($(el).text().trim());
        });
      }

      // Location
      let city = '', state = '', country = 'US';
      const addr = fields['directory address'] || '';
      const addrLines = addr.split('\n').map(l => l.trim()).filter(Boolean);
      for (const line of addrLines) {
        const locMatch = line.match(/^([A-Za-z\s.'-]+),\s*([A-Z]{2})\s+\d{5}/);
        if (locMatch) {
          city = locMatch[1].trim();
          state = locMatch[2];
          break;
        }
      }
      if (/Canada/i.test(addr)) country = 'CA';
      else if (/United Kingdom|UK/i.test(addr)) country = 'UK';
      else if (!/United States/i.test(addr) && !state) country = '';

      const title = fields['title'] || 'Independent Educational Consultant';
      const { first_name, last_name } = splitName(name);

      if (first_name || last_name) {
        leads.push({
          first_name,
          last_name,
          firm_name: '',
          title,
          email,
          phone,
          website: '',
          domain: email ? extractDomain(email.split('@')[1] || '') : '',
          city,
          state,
          country,
          niche: 'education',
          source: SOURCE,
          profile_url: BASE_URL,
        });
        pageLeads++;
      }
    });

    log(`  Found ${pageLeads} members on page 1`);

  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }

  log(`  NACAC total: ${leads.length} leads`);
  return leads;
}

// ── Source 3: Top College Admissions Consultants Rankings ─────────────────────
// URL: https://www.topcollegeadmissionsconsultants.com
// 20 ranked firms, each with a review page containing website links

async function scrapeTopRankings(isTest) {
  const SOURCE = 'top-college-rankings';
  const leads = [];
  const BASE_URL = 'https://www.topcollegeadmissionsconsultants.com';

  // Pre-populated from known rankings page data
  const firms = [
    { name: 'Deans of Admissions', city: 'New York', state: 'NY', reviewPath: '/deans-of-admissions-review', website: 'https://deansofadmissions.com' },
    { name: 'Prepory', city: 'Miami', state: 'FL', reviewPath: '/prepory-review', website: 'https://www.prepory.com' },
    { name: 'InGenius Prep', city: 'New Haven', state: 'CT', reviewPath: '/ingenius-prep-review', website: 'https://ingeniusprep.com' },
    { name: 'Signet Education', city: 'Boston', state: 'MA', reviewPath: '/signet-education-review', website: 'https://www.signeteducation.com' },
    { name: 'AcceptU', city: 'Newton', state: 'MA', reviewPath: '/acceptu-review', website: 'https://www.acceptu.com' },
    { name: 'HelloCollege', city: 'Oak Brook', state: 'IL', reviewPath: '/hello-college-review', website: 'https://sayhellocollege.com' },
    { name: 'Ivy Scholars', city: 'Houston', state: 'TX', reviewPath: '/ivy-scholars-review', website: 'https://www.ivyscholars.com' },
    { name: 'PrepScholar', city: 'Cambridge', state: 'MA', reviewPath: '/prepscholar-review', website: 'https://www.prepscholar.com' },
    { name: 'IvyWise', city: 'New York', state: 'NY', reviewPath: '/ivywise-review', website: 'https://www.ivywise.com' },
    { name: 'Solomon Admissions Consulting', city: 'New York', state: 'NY', reviewPath: '/solomon-admissions-consulting-review', website: 'https://www.solomonadmissions.com' },
    { name: 'CollegeAdvisor.com', city: 'New York', state: 'NY', reviewPath: '/collegeadvisor-review', website: 'https://www.collegeadvisor.com' },
    { name: 'College Coach', city: 'Newton', state: 'MA', reviewPath: '/college-coach-review', website: 'https://getintocollege.com' },
    { name: 'College Transitions', city: 'Charlotte', state: 'NC', reviewPath: '/college-transitions-review', website: 'https://www.collegetransitions.com' },
    { name: 'Crimson Education', city: 'New York', state: 'NY', reviewPath: '/crimson-education-review', website: 'https://www.crimsoneducation.org' },
    { name: 'College Zoom', city: 'Santa Monica', state: 'CA', reviewPath: '/collegezoom-review', website: 'https://www.collegezoom.com' },
    { name: 'Marks Education', city: 'Bethesda', state: 'MD', reviewPath: '/marks-education-review', website: 'https://www.markseducation.com' },
    { name: 'Preminente', city: 'New York', state: 'NY', reviewPath: '/preminente-review', website: 'https://www.preminente.com' },
    { name: 'Spark Admissions', city: 'Brookline', state: 'MA', reviewPath: '/spark-admissions-review', website: 'https://www.sparkadmissions.com' },
    { name: 'TopTier Admissions', city: 'Concord', state: 'MA', reviewPath: '/top-tier-admissions-review', website: 'https://www.toptieradmissions.com' },
    { name: 'Admissionado', city: 'Chicago', state: 'IL', reviewPath: '/admissionado-review', website: 'https://www.admissionado.com' },
  ];

  const firmsToScrape = isTest ? firms.slice(0, 5) : firms;

  log(`\n=== Top College Admissions Consultants Rankings ===`);
  log(`Processing ${firmsToScrape.length} ranked firms...`);

  for (const firm of firmsToScrape) {
    const reviewUrl = `${BASE_URL}${firm.reviewPath}`;
    log(`  [TopRankings] ${firm.name}: ${reviewUrl}`);

    try {
      // Try to scrape the review page for additional data (website, phone, etc.)
      const { statusCode, body } = await httpGet(reviewUrl);

      let phone = '';
      let websiteFromPage = firm.website;

      if (statusCode === 200 && body) {
        const $ = cheerio.load(body);

        // Note: phone numbers on review pages are typically Squarespace call-tracking
        // numbers (e.g. 615-405-xxxx), not real firm phone numbers. Skip extraction.

        // Look for website links if we don't have one
        if (!websiteFromPage) {
          $('a[href]').each((_, el) => {
            if (websiteFromPage) return;
            const href = $(el).attr('href') || '';
            const text = $(el).text().trim();
            if (/visit.*site|official.*site|website/i.test(text) && href.match(/^https?:\/\//)) {
              websiteFromPage = href;
            }
          });
        }
      }

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firm.name,
        title: 'College Admissions Consulting Firm',
        email: '',
        phone,
        website: websiteFromPage || '',
        domain: extractDomain(websiteFromPage || ''),
        city: firm.city,
        state: firm.state,
        country: 'US',
        niche: 'education',
        source: SOURCE,
        profile_url: reviewUrl,
      });

      log(`    Added: ${firm.name} (${firm.city}, ${firm.state})`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
      // Still add the firm with known data
      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firm.name,
        title: 'College Admissions Consulting Firm',
        email: '',
        phone: '',
        website: firm.website || '',
        domain: extractDomain(firm.website || ''),
        city: firm.city,
        state: firm.state,
        country: 'US',
        niche: 'education',
        source: SOURCE,
        profile_url: `${BASE_URL}${firm.reviewPath}`,
      });
    }

    await randomDelay();
  }

  log(`  TopRankings total: ${leads.length} leads`);
  return leads;
}

// ── Source 4: Consulting Firm Team Pages ──────────────────────────────────────
// Scrape team/about pages of well-known college admissions consulting firms

async function scrapeFirmTeamPages(isTest) {
  const SOURCE = 'firm-team-pages';
  const leads = [];

  const firmPages = [
    // IvyWise — counselors and tutors
    { url: 'https://www.ivywise.com/team/', firm: 'IvyWise', city: 'New York', state: 'NY', titleDefault: 'College Admissions Counselor', pathPatterns: ['/counselor/', '/tutors/', '/staff/'] },
    // Accepted.com — admissions consultants (MBA, Law, Med, College)
    { url: 'https://www.accepted.com/about/team/', firm: 'Accepted', city: 'Los Angeles', state: 'CA', titleDefault: 'Admissions Consultant', pathPatterns: null },
    // Collegewise — counselors
    { url: 'https://collegewise.com/our-counselors/', firm: 'Collegewise', city: 'Irvine', state: 'CA', titleDefault: 'College Counselor', pathPatterns: null },
    // Compass Education Group
    { url: 'https://www.compassprep.com/about/', firm: 'Compass Education Group', city: 'San Francisco', state: 'CA', titleDefault: 'Education Consultant', pathPatterns: null },
    // PrepMaven
    { url: 'https://prepmaven.com/about/', firm: 'PrepMaven', city: 'New York', state: 'NY', titleDefault: 'College Prep Consultant', pathPatterns: null },
    // Applerouth
    { url: 'https://www.applerouth.com/about/', firm: 'Applerouth', city: 'Atlanta', state: 'GA', titleDefault: 'SAT/ACT Tutor', pathPatterns: null },
    // College Essay Guy
    { url: 'https://www.collegeessayguy.com/about', firm: 'College Essay Guy', city: 'Los Angeles', state: 'CA', titleDefault: 'College Essay Consultant', pathPatterns: null },
    // Spark Admissions
    { url: 'https://www.sparkadmissions.com/about/', firm: 'Spark Admissions', city: 'Brookline', state: 'MA', titleDefault: 'Admissions Counselor', pathPatterns: null },
    // College Transitions
    { url: 'https://www.collegetransitions.com/about/', firm: 'College Transitions', city: 'Charlotte', state: 'NC', titleDefault: 'College Admissions Consultant', pathPatterns: null },
    // Marks Education
    { url: 'https://www.markseducation.com/about', firm: 'Marks Education', city: 'Bethesda', state: 'MD', titleDefault: 'Education Consultant', pathPatterns: null },
  ];

  const pagesToScrape = isTest ? firmPages.slice(0, 3) : firmPages;

  log(`\n=== Consulting Firm Team Pages ===`);
  log(`Scraping ${pagesToScrape.length} firm team pages...`);

  for (const page of pagesToScrape) {
    log(`  [FirmTeam] ${page.firm}: ${page.url}`);

    try {
      const { statusCode, body } = await httpGet(page.url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Strategy 1: If we have specific path patterns (like IvyWise)
      if (page.pathPatterns) {
        const selector = page.pathPatterns.map(p => `a[href*="${p}"]`).join(', ');
        $(selector).each((_, el) => {
          const linkEl = $(el);
          const href = linkEl.attr('href') || '';
          let text = linkEl.text().trim().replace(/\s+/g, ' ');

          if (!text || text.length > 60 || text.length < 3) return;
          if (/read more|learn more|back|home|about|meet our|our team/i.test(text) && text.length < 20) return;
          // Skip pure navigation labels
          if (/^(tutors|staff|counselors|view all|see all|browse|our team|find|search)$/i.test(text)) return;
          // Strip "Meet " prefix (IvyWise uses "Meet Scott, M.P.A." format)
          text = text.replace(/^meet\s+/i, '').trim();

          const lines = text.split('\n').map(s => s.trim()).filter(Boolean);
          const name = lines[0] || text;
          const profileUrl = href.startsWith('http') ? href : `https://www.ivywise.com${href}`;

          let title = page.titleDefault;
          if (href.includes('/counselor/')) title = 'College Admissions Counselor';
          else if (href.includes('/tutors/')) title = 'Master Tutor';
          else if (href.includes('/staff/')) title = 'Staff';

          const { first_name, last_name } = splitName(name);

          if ((first_name || last_name) && !leads.some(l => l.profile_url === profileUrl)) {
            leads.push({
              first_name,
              last_name,
              firm_name: page.firm,
              title,
              email: '',
              phone: '',
              website: page.url.replace(/\/(team|about|our-counselors)\/?.*$/, '/'),
              domain: extractDomain(page.url),
              city: page.city,
              state: page.state,
              country: 'US',
              niche: 'education',
              source: SOURCE,
              profile_url: profileUrl,
            });
            pageLeads++;
          }
        });
      }

      // Strategy 2: Look for person names in headings within team sections
      $('h2, h3, h4, h5').each((_, el) => {
        const name = $(el).text().trim().replace(/\s+/g, ' ');
        if (!name || name.length > 50 || name.length < 4) return;
        // Must look like a person name: 2-3 capitalized words
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$/.test(name)) return;
        if (name.split(/\s+/).length > 3) return;
        // Skip common headings
        if (/about|team|our|meet|staff|company|what|how|why|the|contact|sign|join|blog|real|focus|college|future|student|success|essay|application/i.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (first_name && last_name &&
            !leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === page.firm)) {

          let title = page.titleDefault;
          const nextEl = $(el).next('p, span, div');
          const nextText = nextEl.text().trim();
          if (nextText && nextText.length < 100 && nextText.length > 3 &&
              /director|founder|manager|lead|head|chief|tutor|coach|counselor|president|ceo|coo|vp|advisor|consultant/i.test(nextText)) {
            title = nextText.length < 80 ? nextText : title;
          }

          leads.push({
            first_name,
            last_name,
            firm_name: page.firm,
            title,
            email: '',
            phone: '',
            website: page.url.replace(/\/(team|about|our-counselors)\/?.*$/, '/'),
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
      });

      // Strategy 3: JSON-LD structured data
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
            if (first_name && last_name &&
                !leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === page.firm)) {
              leads.push({
                first_name,
                last_name,
                firm_name: page.firm,
                title: person.jobTitle || page.titleDefault,
                email: person.email || '',
                phone: person.telephone ? formatPhone(person.telephone) : '',
                website: page.url.replace(/\/(team|about|our-counselors)\/?.*$/, '/'),
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
          }
        } catch {}
      });

      log(`    Found ${pageLeads} team members`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Firm team pages total: ${leads.length} leads`);
  return leads;
}

// ── Source 5: HECA Detail Pages (Enrich) ─────────────────────────────────────
// Scrape HECA member detail pages for additional data: email, address, description
// URL: https://association.hecalive.org/heca-member-directory/Details/SLUG

async function scrapeHECADetails(hecaLeads, isTest) {
  const SOURCE = 'heca-directory';
  const maxDetails = isTest ? 10 : 200; // Limit detail page scraping
  let enriched = 0;

  const leadsToEnrich = hecaLeads.filter(l => l.profile_url && l.profile_url.includes('/Details/'));
  const batch = leadsToEnrich.slice(0, maxDetails);

  if (batch.length === 0) {
    log(`\n=== HECA Detail Page Enrichment (skipped — no detail URLs) ===`);
    return;
  }

  log(`\n=== HECA Detail Page Enrichment ===`);
  log(`Enriching up to ${batch.length} members...`);

  for (const lead of batch) {
    try {
      const { statusCode, body } = await httpGet(lead.profile_url);
      if (statusCode !== 200 || !body) continue;

      const $ = cheerio.load(body);

      // Look for email
      $('a[href^="mailto:"]').each((_, el) => {
        if (!lead.email) {
          const email = $(el).attr('href').replace('mailto:', '').trim();
          if (email && email.includes('@')) lead.email = email;
        }
      });

      // Look for phone (if not already found)
      if (!lead.phone) {
        $('a[href^="tel:"]').each((_, el) => {
          if (!lead.phone) {
            lead.phone = formatPhone($(el).attr('href').replace('tel:', ''));
          }
        });
      }

      // Look for address/location in detail fields
      const pageText = $('body').text();
      const stateMatch = pageText.match(/(?:^|\s)([A-Z]{2})\s+\d{5}(?:\s|$)/);
      if (stateMatch && !lead.state) {
        lead.state = stateMatch[1];
      }

      // Update domain if we got an email
      if (lead.email && !lead.domain) {
        lead.domain = extractDomain(lead.email.split('@')[1] || '');
      }

      enriched++;
      if (enriched % 20 === 0) {
        log(`  Enriched ${enriched}/${batch.length} detail pages`);
      }

    } catch (err) {
      // Silently skip errors on detail pages
    }

    await randomDelay();
  }

  log(`  Enriched ${enriched} detail pages`);
}

// ── Source 6: Accepted.com Team Page (Direct Parse) ──────────────────────────
// 19 team members with bios and specialties

async function scrapeAccepted(isTest) {
  const SOURCE = 'accepted-team';
  const leads = [];
  const url = 'https://www.accepted.com/about/team/';

  log(`\n=== Accepted.com Team ===`);
  log(`  ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`  HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);
    let pageLeads = 0;

    // Known team from page analysis
    const knownTeam = [
      { name: 'Alice Diamond', title: 'Admissions Consultant' },
      { name: 'Alicia Nimonkar', title: 'Medical School Admissions Consultant' },
      { name: 'Barry Rothman', title: 'Health Professions Advisor' },
      { name: 'Brigitte Suhr', title: 'Law School Admissions Consultant' },
      { name: 'Christie St-John', title: 'MBA Admissions Consultant' },
      { name: 'Esmeralda Cardenal', title: 'Graduate Admissions Consultant' },
      { name: 'Herman Gordon', title: 'Medical School Admissions Consultant' },
      { name: 'Joy Blaser', title: 'Law School Admissions Consultant' },
      { name: 'Karin Ash', title: 'MBA Admissions Consultant' },
      { name: 'Kelly Wilson', title: 'MBA Admissions Consultant' },
      { name: 'Madeline Rathbun', title: 'Test Prep Tutor' },
      { name: 'Marie Todd', title: 'College Admissions Consultant' },
      { name: 'Mary Mahoney', title: 'Medical School Admissions Consultant' },
      { name: 'Michelle Stockman', title: 'MBA Admissions Consultant' },
      { name: 'Natalie Grinblatt', title: 'MBA Admissions Consultant' },
      { name: 'Steven Tagle', title: 'College Essay Consultant' },
      { name: 'Sundas Ali', title: 'Graduate Admissions Consultant' },
      { name: 'Valerie Wherley', title: 'Medical School Admissions Consultant' },
      { name: 'Willa Tracy', title: 'Test Prep Tutor' },
    ];

    // Try to find names dynamically from the page first
    const foundNames = new Set();
    $('h3, h4').each((_, el) => {
      const text = $(el).text().trim().replace(/\s+/g, ' ');
      // Remove prefix titles
      const cleaned = text.replace(/^(Dr\.\s*|Prof\.\s*)/i, '').replace(/,.*$/, '').trim();
      if (cleaned && /^[A-Z][a-z]+\s+[A-Z]/.test(cleaned) && cleaned.length < 40) {
        foundNames.add(cleaned);
      }
    });

    // Use dynamic names if found, otherwise fall back to known list
    const teamToUse = foundNames.size > 5
      ? [...foundNames].map(n => ({ name: n, title: 'Admissions Consultant' }))
      : knownTeam;

    const batch = isTest ? teamToUse.slice(0, 5) : teamToUse;

    for (const member of batch) {
      const { first_name, last_name } = splitName(member.name);
      if (first_name && last_name) {
        leads.push({
          first_name,
          last_name,
          firm_name: 'Accepted',
          title: member.title,
          email: '',
          phone: '(310) 815-9553',
          website: 'https://www.accepted.com',
          domain: 'accepted.com',
          city: 'Los Angeles',
          state: 'CA',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
        pageLeads++;
      }
    }

    log(`  Found ${pageLeads} team members`);

  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }

  log(`  Accepted total: ${leads.length} leads`);
  return leads;
}

// ── Source 7: NCAN Member Directory ──────────────────────────────────────────
// National College Access Network — organizations focused on college access
// URL: https://www.ncan.org/general/custom.asp?page=MemberDirectory

async function scrapeNCAN(isTest) {
  const SOURCE = 'ncan-directory';
  const leads = [];
  const url = 'https://www.ncan.org/general/custom.asp?page=MemberDirectory';

  log(`\n=== NCAN Member Directory ===`);
  log(`  ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`  HTTP ${statusCode} - skipping (may require login)`);
      return leads;
    }

    const $ = cheerio.load(body);
    let pageLeads = 0;

    // NCAN uses Fonteva AMS or standard table-based directory
    // Try to find member organization cards/rows
    $('tr, .member-row, .directory-item, .result-item, .listing').each((_, el) => {
      const row = $(el);
      const text = row.text().trim();

      // Look for organization names in links
      row.find('a').each((_, linkEl) => {
        const href = $(linkEl).attr('href') || '';
        const orgName = $(linkEl).text().trim();

        if (!orgName || orgName.length < 3 || orgName.length > 100) return;
        if (/login|sign|register|about|home|privacy/i.test(orgName) && orgName.length < 20) return;

        // Must look like an organization name (not a nav link)
        if (href.includes('/directory/') || href.includes('/member/') || href.includes('Details')) {
          const profileUrl = href.startsWith('http') ? href : `https://www.ncan.org${href}`;

          leads.push({
            first_name: '',
            last_name: '',
            firm_name: orgName,
            title: 'College Access Organization',
            email: '',
            phone: '',
            website: '',
            domain: '',
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: profileUrl,
          });
          pageLeads++;
        }
      });
    });

    // Also try parsing structured data from the page
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const json = JSON.parse($(el).html());
        if (json.member || json.itemListElement) {
          const items = json.member || json.itemListElement || [];
          for (const item of (Array.isArray(items) ? items : [items])) {
            if (item.name || item.item?.name) {
              const name = item.name || item.item?.name;
              leads.push({
                first_name: '',
                last_name: '',
                firm_name: name,
                title: 'College Access Organization',
                email: '',
                phone: '',
                website: item.url || item.item?.url || '',
                domain: extractDomain(item.url || item.item?.url || ''),
                city: '',
                state: '',
                country: 'US',
                niche: 'education',
                source: SOURCE,
                profile_url: url,
              });
              pageLeads++;
            }
          }
        }
      } catch {}
    });

    log(`  Found ${pageLeads} organizations`);

  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }

  log(`  NCAN total: ${leads.length} leads`);
  return leads;
}

// ── Source 8: Additional Firm Websites (Scrape Contact Info) ─────────────────
// Scrape contact pages of well-known college admissions consulting firms
// to get phone, email, and confirm website

async function scrapeAdditionalFirms(isTest) {
  const SOURCE = 'consulting-firms';
  const leads = [];

  // Curated list of known college admissions consulting firms
  const firms = [
    { firm: 'Ivy Coach', website: 'https://www.ivycoach.com', city: 'New York', state: 'NY' },
    { firm: 'Empowerly', website: 'https://empowerly.com', city: 'San Jose', state: 'CA' },
    { firm: 'Command Education', website: 'https://www.commandeducation.com', city: 'New York', state: 'NY' },
    { firm: 'ScholarGrade', website: 'https://www.scholargrade.com', city: '', state: '' },
    { firm: 'Top Tier Admissions', website: 'https://www.toptieradmissions.com', city: 'Concord', state: 'MA' },
    { firm: 'Magellan College Counseling', website: 'https://www.magellancounseling.com', city: 'Tucson', state: 'AZ' },
    { firm: 'Great College Advice', website: 'https://greatcollegeadvice.com', city: 'Austin', state: 'TX' },
    { firm: 'College Kickstart', website: 'https://www.collegekickstart.com', city: '', state: '' },
    { firm: 'Door2Door College Counseling', website: 'https://www.door2doorcollegecounseling.com', city: '', state: '' },
    { firm: 'AdmissionSight', website: 'https://admissionsight.com', city: 'San Jose', state: 'CA' },
    { firm: 'Veritas Prep', website: 'https://www.veritasprep.com', city: 'Los Angeles', state: 'CA' },
    { firm: 'SupertutorTV', website: 'https://supertutortv.com', city: 'Los Angeles', state: 'CA' },
    { firm: 'Bright Horizons College Coach', website: 'https://getintocollege.com', city: 'Newton', state: 'MA' },
    { firm: 'Kaplan College Prep', website: 'https://www.kaptest.com', city: 'New York', state: 'NY' },
    { firm: 'Princeton Review', website: 'https://www.princetonreview.com', city: 'New York', state: 'NY' },
  ];

  const firmsToScrape = isTest ? firms.slice(0, 5) : firms;

  log(`\n=== Additional Consulting Firms ===`);
  log(`Processing ${firmsToScrape.length} firms...`);

  for (const firm of firmsToScrape) {
    log(`  [Firms] ${firm.firm}: ${firm.website}`);

    try {
      const { statusCode, body } = await httpGet(firm.website);
      let phone = '';
      let email = '';

      if (statusCode === 200 && body) {
        const $ = cheerio.load(body);

        // Extract phone from tel: links
        $('a[href^="tel:"]').each((_, el) => {
          if (!phone) {
            let raw = $(el).attr('href').replace('tel:', '');
            // Decode URL-encoded values
            try { raw = decodeURIComponent(raw); } catch {}
            raw = raw.replace(/^\s+/, '').trim();
            const digits = raw.replace(/\D/g, '');
            // Only accept valid-looking phone numbers (7+ digits)
            if (digits.length >= 7) {
              phone = formatPhone(raw);
            }
          }
        });

        // Extract email from mailto: links
        $('a[href^="mailto:"]').each((_, el) => {
          if (!email) {
            let mailto = $(el).attr('href').replace('mailto:', '').split('?')[0];
            // Decode URL-encoded values
            try { mailto = decodeURIComponent(mailto); } catch {}
            mailto = mailto.replace(/^\s+/, '').trim();
            if (mailto.includes('@') && !mailto.includes('example') && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(mailto)) {
              email = mailto;
            }
          }
        });

        // Try phone from text pattern
        if (!phone) {
          const bodyText = $('body').text();
          const phoneMatch = bodyText.match(/\(?[2-9]\d{2}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
          if (phoneMatch) phone = formatPhone(phoneMatch[0]);
        }
      }

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firm.firm,
        title: 'College Admissions Consulting Firm',
        email,
        phone,
        website: firm.website,
        domain: extractDomain(firm.website),
        city: firm.city,
        state: firm.state,
        country: 'US',
        niche: 'education',
        source: SOURCE,
        profile_url: firm.website,
      });

      log(`    Added: ${firm.firm}${phone ? ` (phone: ${phone})` : ''}${email ? ` (email: ${email})` : ''}`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
      // Still add with known data
      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firm.firm,
        title: 'College Admissions Consulting Firm',
        email: '',
        phone: '',
        website: firm.website,
        domain: extractDomain(firm.website),
        city: firm.city,
        state: firm.state,
        country: 'US',
        niche: 'education',
        source: SOURCE,
        profile_url: firm.website,
      });
    }

    await randomDelay();
  }

  log(`  Additional firms total: ${leads.length} leads`);
  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-college-consultants-directories.js --test    # Test mode (limited pages)');
    console.log('  node scripts/scrape-college-consultants-directories.js --all     # Full scrape all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log(`College Admissions Consultants — Multi-Directory Scraper`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const seenKeys = new Set();
  const sourceStats = {};

  // Source 1: HECA
  let hecaLeads = [];
  try {
    hecaLeads = await scrapeHECA(isTest);
    let added = 0;
    for (const lead of hecaLeads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    sourceStats['HECA'] = { total: hecaLeads.length, unique: added };
    log(`\n  >> HECA: ${hecaLeads.length} found, ${added} unique added`);
  } catch (err) {
    log(`\n  ERROR in HECA: ${err.message}`);
    sourceStats['HECA'] = { total: 0, unique: 0, error: err.message };
  }
  writeCSV(allLeads, OUT_FILE);

  // Source 1b: HECA detail page enrichment (uses leads from Source 1)
  try {
    await scrapeHECADetails(hecaLeads, isTest);
    // Re-write CSV since detail pages may have enriched data
    writeCSV(allLeads, OUT_FILE);
  } catch (err) {
    log(`\n  ERROR in HECA Details: ${err.message}`);
  }

  // Source 2: NACAC
  try {
    const leads = await scrapeNACAC(isTest);
    let added = 0;
    for (const lead of leads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    sourceStats['NACAC'] = { total: leads.length, unique: added };
    log(`\n  >> NACAC: ${leads.length} found, ${added} unique added`);
  } catch (err) {
    log(`\n  ERROR in NACAC: ${err.message}`);
    sourceStats['NACAC'] = { total: 0, unique: 0, error: err.message };
  }
  writeCSV(allLeads, OUT_FILE);

  // Source 3: Top Rankings
  try {
    const leads = await scrapeTopRankings(isTest);
    let added = 0;
    for (const lead of leads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    sourceStats['TopRankings'] = { total: leads.length, unique: added };
    log(`\n  >> TopRankings: ${leads.length} found, ${added} unique added`);
  } catch (err) {
    log(`\n  ERROR in TopRankings: ${err.message}`);
    sourceStats['TopRankings'] = { total: 0, unique: 0, error: err.message };
  }
  writeCSV(allLeads, OUT_FILE);

  // Source 4: Firm Team Pages
  try {
    const leads = await scrapeFirmTeamPages(isTest);
    let added = 0;
    for (const lead of leads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    sourceStats['FirmTeamPages'] = { total: leads.length, unique: added };
    log(`\n  >> FirmTeamPages: ${leads.length} found, ${added} unique added`);
  } catch (err) {
    log(`\n  ERROR in FirmTeamPages: ${err.message}`);
    sourceStats['FirmTeamPages'] = { total: 0, unique: 0, error: err.message };
  }
  writeCSV(allLeads, OUT_FILE);

  // Source 5: Additional Firms
  try {
    const leads = await scrapeAdditionalFirms(isTest);
    let added = 0;
    for (const lead of leads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    sourceStats['AdditionalFirms'] = { total: leads.length, unique: added };
    log(`\n  >> AdditionalFirms: ${leads.length} found, ${added} unique added`);
  } catch (err) {
    log(`\n  ERROR in AdditionalFirms: ${err.message}`);
    sourceStats['AdditionalFirms'] = { total: 0, unique: 0, error: err.message };
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const firms = allLeads.filter(l => l.firm_name && !l.first_name).length;

  log('');
  log('='.repeat(60));
  log('FINAL RESULTS');
  log('='.repeat(60));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With full name: ${withName}`);
  log(`Firm-only records: ${firms}`);
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
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
