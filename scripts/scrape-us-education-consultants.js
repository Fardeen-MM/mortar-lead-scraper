#!/usr/bin/env node
/**
 * US Education Consultants Extra Scraper
 *
 * Scrapes multiple education consultant directories beyond IECA/AIRC:
 *
 *   1. NACAC Independent Member Directory (hub.nacacnet.org) — ~300 consultants, 30 pages
 *   2. HECA Member Directory (association.hecalive.org) — ~945 members, A-Z
 *   3. IvyWise team page — counselors, tutors, staff (~50 people)
 *   4. EducationUSA advising centers (State Dept) — 436 centers, 44 pages
 *   5. Forum on Education Abroad providers — 130+ study abroad orgs
 *
 * Skipped sources (require JS rendering / Cloudflare):
 *   - CollegeAdvisor.com (times out, JS-rendered)
 *   - Collegewise (JS-rendered, placeholder data in SSR)
 *   - Wyzant (Next.js + Cloudflare, no SSR tutor data)
 *
 * Usage:
 *   node scripts/scrape-us-education-consultants.js --test    # Test mode (1-2 pages per source)
 *   node scripts/scrape-us-education-consultants.js --all     # Full scrape all sources
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
const OUT_FILE = path.join(OUT_DIR, 'us-education-consultants-extra.csv');

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
  // 1. Remove leading titles
  let cleaned = fullName
    .replace(/^(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?)\s+/i, '')
    .trim();

  // 2. If there's a comma, everything after it is likely credentials
  const commaIdx = cleaned.indexOf(',');
  if (commaIdx > 0) {
    cleaned = cleaned.substring(0, commaIdx).trim();
  }

  // 3. Remove trailing credential words (space-separated, must be standalone)
  cleaned = cleaned
    .replace(/\s+(CEP|PhD|EdD|MA|MEd|EdM|MS|MPA|JD|MBA|LCSW|LPC|LCPC|LMFT|MPP|Esq|Ph\.D\.|Ed\.D\.|M\.A\.|M\.Ed\.|Ed\.M\.|M\.S\.|M\.P\.A\.|J\.D\.|M\.B\.A\.|M\.P\.P\.?)\.?$/gi, '')
    .trim();

  // 4. Clean trailing punctuation
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
  const name = `${(lead.first_name || '').toLowerCase()}|${(lead.last_name || '').toLowerCase()}`;
  const firm = (lead.firm_name || '').toLowerCase().trim();
  const email = (lead.email || '').toLowerCase().trim();
  if (email) return `email:${email}`;
  if (name !== '|') return `name:${name}|${firm}`;
  if (firm) return `firm:${firm}`;
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

// ── Source 1: NACAC Independent Member Directory ─────────────────────────────
// Salesforce Visualforce app, server-rendered HTML
// Structure: div.card > div.card-heading > span (name)
//            ul.card-detail > li > label.card-detail-label + span.card-detail-value
//            Labels: "Title", "Directory Address", "Email", "Phone", "Fax"
// Pagination: Salesforce uses POST (same page form submit). Page 1 only via GET.
// NACAC pages 2+ require Salesforce form POST with ViewState -- we can only GET page 1
// but we can try ?page=N params

async function scrapeNACAC(isTest) {
  const SOURCE = 'nacac';
  const BASE_URL = 'https://hub.nacacnet.org/independentmemberdirectory';
  const leads = [];
  const maxPages = isTest ? 2 : 30;
  const seenEmails = new Set();

  log(`\n=== NACAC Independent Member Directory ===`);
  log(`Scraping up to ${maxPages} pages...`);

  for (let page = 0; page < maxPages; page++) {
    // NACAC Salesforce only serves page 1 via plain GET request
    // Subsequent pages require Salesforce form POST with ViewState
    // We'll try the GET approach and stop when we get duplicates
    const url = page === 0 ? BASE_URL : `${BASE_URL}?page=${page + 1}`;

    log(`  [NACAC] Page ${page + 1}/${maxPages}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        if (page === 0) break;
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;
      let allDupes = true;

      // Parse card structure:
      // div.card > div.card-heading span (name) + ul.card-detail li (label+value)
      $('div.card').each((_, cardEl) => {
        const card = $(cardEl);

        // Get name from card-heading
        const name = card.find('.card-heading span').first().text().trim();
        if (!name) return;

        // Extract labeled fields (preserve <br> as \n for address parsing)
        const fields = {};
        card.find('ul.card-detail li').each((_, liEl) => {
          const li = $(liEl);
          const label = li.find('label.card-detail-label').text().trim().replace(/:?\s*$/, '');
          const valueEl = li.find('span.card-detail-value');
          // Replace <br> with newlines before getting text
          valueEl.find('br').replaceWith('\n');
          const value = valueEl.text().trim();
          if (label && value) fields[label.toLowerCase()] = value;
        });

        // Extract email from mailto link
        let email = '';
        card.find('a[href^="mailto:"]').each((_, el) => {
          if (!email) email = $(el).attr('href').replace('mailto:', '').trim();
        });

        // Skip if already seen (duplicate from pagination not working)
        if (email && seenEmails.has(email)) return;
        if (email) {
          seenEmails.add(email);
          allDupes = false;
        }

        // Parse phone
        let phone = '';
        const phoneField = fields['phone'] || fields['telephone'] || '';
        if (phoneField) {
          phone = formatPhone(phoneField);
        } else {
          // Try to find phone from tel: links
          card.find('a[href^="tel:"]').each((_, el) => {
            if (!phone) phone = formatPhone($(el).text().trim());
          });
        }

        // Parse location from "Directory Address"
        // Format: "Street\nCity, ST ZIP\nCountry" (newlines from <br> tags)
        let city = '', state = '', country = 'US';
        const addr = fields['directory address'] || '';
        const addrLines = addr.split('\n').map(l => l.trim()).filter(Boolean);
        // Find the line with "City, ST ZIP" pattern
        for (const line of addrLines) {
          const locMatch = line.match(/^([A-Za-z\s.'-]+),\s*([A-Z]{2})\s+\d{5}/);
          if (locMatch) {
            city = locMatch[1].trim();
            state = locMatch[2];
            break;
          }
        }
        // Determine country from address
        if (/Canada/i.test(addr)) country = 'CA';
        else if (/United Kingdom|UK/i.test(addr)) country = 'UK';
        else if (!/United States/i.test(addr) && !state) {
          country = '';
        }

        // Title
        const title = fields['title'] || '';

        const { first_name, last_name } = splitName(name);

        if ((first_name || last_name) && email) {
          leads.push({
            first_name,
            last_name,
            firm_name: '',
            title,
            email,
            phone,
            website: '',
            domain: extractDomain(email.split('@')[1] || ''),
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

      log(`    Found ${pageLeads} members`);

      // If all entries were dupes, pagination isn't working -- stop
      if (allDupes && page > 0) {
        log(`    All duplicates -- Salesforce pagination requires POST. Stopping.`);
        break;
      }

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  NACAC total: ${leads.length} leads`);
  return leads;
}

// ── Source 2: HECA Member Directory ──────────────────────────────────────────
// URL: https://association.hecalive.org/heca-member-directory/FindStartsWith?term=X
// Structure per card:
//   h5.card-title.gz-card-title > a[href*="Details"] = name (with itemprop="name")
//   li.gz-card-description > div.mn-text = company/description
//   li.gz-card-phone > a[href^="tel:"] = phone
//   li with a[href*="/Contact/"] = email (form, not direct)
//   li with "Visit Website" text = website link
//   li with "More Details" = detail page link
// hrefs are protocol-relative: //association.hecalive.org/heca-member-directory/Details/slug

async function scrapeHECA(isTest) {
  const SOURCE = 'heca';
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

      // Find all member cards by the gz-card-title class
      $('h5.gz-card-title a, h5.card-title a').each((_, el) => {
        const nameEl = $(el);
        const fullName = nameEl.text().trim();
        let detailPath = nameEl.attr('href') || '';

        // Fix protocol-relative URLs: //association.hecalive.org/... -> https://...
        if (detailPath.startsWith('//')) {
          detailPath = 'https:' + detailPath;
        } else if (detailPath.startsWith('/')) {
          detailPath = 'https://association.hecalive.org' + detailPath;
        }
        const profileUrl = detailPath;

        // Navigate to the card container (the col-sm-6 or similar wrapping div)
        const card = nameEl.closest('.col-sm-6, .col-md-6, .col-lg-4, .card, div[class*="col"]');

        // Extract phone from gz-card-phone or tel: link
        let phone = '';
        card.find('li.gz-card-phone a[href^="tel:"], a[href^="tel:"]').each((_, phoneEl) => {
          if (!phone) {
            const tel = $(phoneEl).attr('href').replace('tel:', '');
            phone = formatPhone(tel);
          }
        });

        // Extract website from "Visit Website" link
        let website = '';
        card.find('a').each((_, linkEl) => {
          const text = $(linkEl).text().trim();
          const href = $(linkEl).attr('href') || '';
          if (/visit\s*website/i.test(text) && href) {
            // Fix protocol-relative
            if (href.startsWith('//')) website = 'https:' + href;
            else website = href;
          }
        });
        // Fallback: find any external link that's not HECA/social/contact
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

        // Extract company from gz-card-description
        // Only use short text (< 50 chars, <= 5 words) as company name
        // Longer text is a bio/description, not a company name
        let company = '';
        card.find('li.gz-card-description .mn-text p, li.gz-card-description .mn-text').each((_, descEl) => {
          if (company) return;
          let text = $(descEl).text().trim();
          text = text.replace(/<[^>]+>/g, '').trim();
          // Only use as company if it's very short (a company name, not a bio)
          if (text && text.length < 50 && text.length > 2 && text.split(' ').length <= 5) {
            // Skip if it starts with articles/prepositions that suggest a description
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
            title: 'Education Consultant',
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

// ── Source 3: IvyWise Team ───────────────────────────────────────────────────
// URL: https://www.ivywise.com/team/ — counselors and tutors
// URL: https://www.ivywise.com/about-ivywise/the-ivywise-team/staff/ — staff
// Structure: <a href="/counselor/name/"> or <a href="/tutors/name/"> or <a href="/staff/name/">

async function scrapeIvyWise(isTest) {
  const SOURCE = 'ivywise';
  const leads = [];

  log(`\n=== IvyWise Team ===`);

  const urls = [
    { url: 'https://www.ivywise.com/team/', type: 'counselors' },
    { url: 'https://www.ivywise.com/about-ivywise/the-ivywise-team/staff/', type: 'staff' },
  ];

  for (const { url, type } of urls) {
    log(`  [IvyWise] ${type}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // IvyWise team members in <a> with /counselor/, /tutors/, /staff/ paths
      $('a[href*="/counselor/"], a[href*="/tutors/"], a[href*="/staff/"]').each((_, el) => {
        const linkEl = $(el);
        const href = linkEl.attr('href') || '';
        let text = linkEl.text().trim();

        // Clean up whitespace from multi-line link text
        text = text.replace(/\s+/g, ' ').trim();

        if (!text || text.length > 80 || text.length < 2) return;
        if (/read more|learn more|back|home|about/i.test(text) && text.length < 15) return;

        // Extract just the name (first non-empty line)
        const lines = text.split('\n').map(s => s.trim()).filter(Boolean);
        let name = lines[0] || text;

        // Build profile URL
        const profileUrl = href.startsWith('http') ? href : `https://www.ivywise.com${href}`;

        // Determine role
        let title = '';
        if (href.includes('/counselor/')) title = 'College Admissions Counselor';
        else if (href.includes('/tutors/')) title = 'Master Tutor';
        else if (href.includes('/staff/')) title = 'Staff';

        // Look for a more specific title in child elements
        linkEl.find('span, p, .title, .role').each((_, titleEl) => {
          const t = $(titleEl).text().trim();
          if (t && t !== name && t.length < 80 && t.length > 3 &&
              (t.includes('Counselor') || t.includes('Tutor') || t.includes('Director') ||
               t.includes('Officer') || t.includes('Manager') || t.includes('Team Leader'))) {
            title = t;
          }
        });

        const { first_name, last_name } = splitName(name);

        if ((first_name || last_name) && !leads.some(l => l.profile_url === profileUrl)) {
          leads.push({
            first_name,
            last_name,
            firm_name: 'IvyWise',
            title,
            email: '',
            phone: '',
            website: 'https://www.ivywise.com',
            domain: 'ivywise.com',
            city: 'New York',
            state: 'NY',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: profileUrl,
          });
          pageLeads++;
        }
      });

      log(`    Found ${pageLeads} team members`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  IvyWise total: ${leads.length} leads`);
  return leads;
}

// ── Source 4: EducationUSA Advising Centers ──────────────────────────────────
// URL: https://educationusa.state.gov/find-advising-center?page=N (0-indexed)
// 436 centers across 44 pages (10 per page)
// Structure: a[href*="/centers/center-slug"] = center name + link
// Also scrape detail pages for email, phone, website

async function scrapeEducationUSA(isTest) {
  const SOURCE = 'educationusa';
  const leads = [];
  const maxPages = isTest ? 2 : 50;

  log(`\n=== EducationUSA Advising Centers ===`);
  log(`Scraping up to ${maxPages} pages...`);

  for (let page = 0; page < maxPages; page++) {
    const url = page === 0
      ? 'https://educationusa.state.gov/find-advising-center'
      : `https://educationusa.state.gov/find-advising-center?page=${page}`;
    log(`  [EducationUSA] Page ${page + 1}: ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping (server may be flaky)`);
        continue; // EducationUSA returns 500 intermittently, keep going
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Each center is a link to /centers/slug
      $('a[href*="/centers/"]').each((_, el) => {
        const linkEl = $(el);
        const name = linkEl.text().trim();
        const href = linkEl.attr('href') || '';

        if (!name || name.length > 100 || name.length < 3) return;
        // Skip the "Find an Advising Center" nav link
        if (/find|search|filter|about|home|advising center$/i.test(name) && name.length < 25) return;

        const profileUrl = href.startsWith('http') ? href : `https://educationusa.state.gov${href}`;

        // Skip duplicates
        if (leads.some(l => l.profile_url === profileUrl)) return;

        // Try to extract city and country from the table row or container
        const row = linkEl.closest('tr, .views-row, li, div');
        const rowText = row.text();

        // Parse location from row text (after removing the center name)
        let city = '', country = '';
        const afterName = rowText.substring(rowText.indexOf(name) + name.length).trim();
        const parts = afterName.split(/\s{2,}|\n|(?<=\w)\s+(?=[A-Z][a-z])/).map(s => s.trim()).filter(s => s && !/comprehensive|standard|reference/i.test(s));
        if (parts.length >= 2) {
          city = parts[0].replace(/^\s*,\s*/, '');
          country = parts[1].replace(/^\s*,\s*/, '');
        } else if (parts.length === 1) {
          city = parts[0].replace(/^\s*,\s*/, '');
        }
        // Clean up
        city = city.replace(/\s*(comprehensive|standard|reference)\s*/gi, '').trim();
        country = country.replace(/\s*(comprehensive|standard|reference)\s*/gi, '').trim();

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: name,
          title: 'EducationUSA Advising Center',
          email: '',
          phone: '',
          website: profileUrl,
          domain: 'educationusa.state.gov',
          city,
          state: '',
          country: country || '',
          niche: 'education',
          source: SOURCE,
          profile_url: profileUrl,
        });
        pageLeads++;
      });

      log(`    Found ${pageLeads} centers`);
      if (pageLeads === 0 && page > 0) {
        log(`    No more results, stopping`);
        break;
      }

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  EducationUSA total: ${leads.length} leads`);
  return leads;
}

// ── Source 5: Forum on Education Abroad Providers ────────────────────────────
// URL: https://web.forumea.org/Providers-(Ed-Abroad-Organizations)
// Structure: Organization names in h3/h4/strong headings, followed by "Visit Site" links
// 130+ study abroad organizations

async function scrapeForumEA(isTest) {
  const SOURCE = 'forumea';
  const leads = [];

  log(`\n=== Forum on Education Abroad Providers ===`);
  const url = 'https://web.forumea.org/Providers-(Ed-Abroad-Organizations)';
  log(`  [ForumEA] ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`    HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);
    let pageLeads = 0;
    const seenNames = new Set();

    // ForumEA structure:
    // div.ListingResults_All_CONTAINER contains each listing
    //   - .wl-local-results-listing-title or strong > span[itemprop="name"] = org name
    //   - .ListingResults_Level2_VISITSITE a = website link
    $('.ListingResults_All_CONTAINER').each((_, containerEl) => {
      const container = $(containerEl);

      // Get organization name
      let orgName = container.find('.wl-local-results-listing-title').first().text().trim();
      if (!orgName) {
        orgName = container.find('span[itemprop="name"]').first().text().trim();
      }
      if (!orgName) {
        orgName = container.find('.ListingResults_All_ENTRYTITLELEFTBOX strong').first().text().trim();
      }

      // Get website from "Visit Site" link
      let website = '';
      container.find('.ListingResults_Level2_VISITSITE a, a:contains("Visit Site")').each((_, linkEl) => {
        if (!website) {
          const href = $(linkEl).attr('href') || '';
          if (href.match(/^https?:\/\//)) website = href;
        }
      });

      if (orgName && !seenNames.has(orgName.toLowerCase())) {
        seenNames.add(orgName.toLowerCase());
        leads.push({
          first_name: '',
          last_name: '',
          firm_name: orgName,
          title: 'Study Abroad Organization',
          email: '',
          phone: '',
          website,
          domain: extractDomain(website),
          city: '',
          state: '',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
        pageLeads++;
      }
    });

    log(`    Found ${pageLeads} providers`);

  } catch (err) {
    log(`    ERROR: ${err.message}`);
  }

  log(`  ForumEA total: ${leads.length} leads`);
  return leads;
}

// ── Source 6: PrepScholar / Prep Company Team Pages ──────────────────────────
// Scrape about/team pages of large SAT/ACT prep and college consulting firms

async function scrapePrepCompanies(isTest) {
  const SOURCE = 'prep-companies';
  const leads = [];

  const pages = [
    { url: 'https://www.compassprep.com/about/', firm: 'Compass Education Group', type: 'Education Consultant' },
    { url: 'https://prepmaven.com/about/', firm: 'PrepMaven', type: 'College Prep Consultant' },
    { url: 'https://www.applerouth.com/about/', firm: 'Applerouth', type: 'SAT/ACT Tutor' },
    { url: 'https://www.collegewise.com/about', firm: 'Collegewise', type: 'College Admissions Counselor' },
    { url: 'https://www.collegeessayguy.com/about', firm: 'College Essay Guy', type: 'College Essay Consultant' },
    { url: 'https://www.door2doorcollegecounseling.com/about', firm: 'Door2Door College Counseling', type: 'College Admissions Consultant' },
  ];

  const pagesToScrape = isTest ? pages.slice(0, 2) : pages;

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

      // Strategy 1: Look for names in heading elements within team sections
      $('h2, h3, h4, h5').each((_, el) => {
        const name = $(el).text().trim().replace(/\s+/g, ' ');
        if (!name || name.length > 40 || name.length < 4) return;
        // Must look like a person name: "FirstName LastName" (2-3 words, no punctuation)
        if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$/.test(name)) return;
        // Must have exactly 2-3 parts
        if (name.split(/\s+/).length > 3) return;
        // Skip section headings and marketing copy
        if (/about|team|our|meet|staff|company|what|how|why|the|contact|sign|join|blog|real|focus|combat|excellent|employee|satisfaction|workplace|college|future|student|success|outcome/i.test(name)) return;

        const { first_name, last_name } = splitName(name);
        if (first_name && last_name &&
            !leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === page.firm)) {
          let title = page.type;
          const nextEl = $(el).next('p, span, div');
          const nextText = nextEl.text().trim();
          if (nextText && nextText.length < 80 && nextText.length > 3 &&
              /director|founder|manager|lead|head|chief|tutor|coach|counselor|president|ceo|coo|vp/i.test(nextText)) {
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
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: page.url,
          });
          pageLeads++;
        }
      });

      // Strategy 2: Check JSON-LD for organization/employee data
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
                !leads.some(l => l.first_name === first_name && l.last_name === last_name)) {
              leads.push({
                first_name,
                last_name,
                firm_name: page.firm,
                title: person.jobTitle || page.type,
                email: person.email || '',
                phone: person.telephone ? formatPhone(person.telephone) : '',
                website: page.url.replace(/\/about\/?.*$/, '/'),
                domain: extractDomain(page.url),
                city: '',
                state: '',
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

  log(`  Prep companies total: ${leads.length} leads`);
  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-us-education-consultants.js --test    # Test mode (1-2 pages per source)');
    console.log('  node scripts/scrape-us-education-consultants.js --all     # Full scrape all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log(`US Education Consultants Extra Scraper`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const seenKeys = new Set();
  const sourceStats = {};

  const sources = [
    { name: 'NACAC', fn: () => scrapeNACAC(isTest) },
    { name: 'HECA', fn: () => scrapeHECA(isTest) },
    { name: 'IvyWise', fn: () => scrapeIvyWise(isTest) },
    { name: 'EducationUSA', fn: () => scrapeEducationUSA(isTest) },
    { name: 'ForumEA', fn: () => scrapeForumEA(isTest) },
    { name: 'PrepCompanies', fn: () => scrapePrepCompanies(isTest) },
  ];

  for (const source of sources) {
    try {
      const leads = await source.fn();
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

  log('');
  log('='.repeat(60));
  log('FINAL RESULTS');
  log('='.repeat(60));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With name: ${withName}`);
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
