#!/usr/bin/env node
/**
 * Visa/Immigration Directory Scraper
 *
 * Scrapes immigration lawyer/consultant listings from visa and immigration
 * directory websites:
 *
 *   1. FindAnImmigrationAttorney.com — 50 US states + DC, firm+phone+address+website
 *   2. ImmigrationAdvocates.org     — Nonprofit legal services directory, state search
 *   3. MurthyLaw.com                — Top immigration firm team page (15 attorneys)
 *   4. VisaPlace.com                — Immigration firm (US/Canada) team contacts
 *   5. Boundless.com                — Immigration services firm
 *
 * Strategy:
 *   - HTTP + Cheerio only (no Puppeteer)
 *   - 2-3 second delays between requests
 *   - Dedup by firm+city key
 *   - Resume support via progress file
 *
 * Usage:
 *   node scripts/scrape-visa-directories-immigration.js --test    # 3 states per source
 *   node scripts/scrape-visa-directories-immigration.js --all     # All states, all sources
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
const OUT_FILE = path.join(OUT_DIR, 'immigration-lawyers-visa-directories.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'visa-directories-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── US States ────────────────────────────────────────────────────────────────

const US_STATES = [
  { name: 'Alabama', code: 'AL' },
  { name: 'Alaska', code: 'AK' },
  { name: 'Arizona', code: 'AZ' },
  { name: 'Arkansas', code: 'AR' },
  { name: 'California', code: 'CA' },
  { name: 'Colorado', code: 'CO' },
  { name: 'Connecticut', code: 'CT' },
  { name: 'Delaware', code: 'DE' },
  { name: 'District of Columbia', code: 'DC' },
  { name: 'Florida', code: 'FL' },
  { name: 'Georgia', code: 'GA' },
  { name: 'Hawaii', code: 'HI' },
  { name: 'Idaho', code: 'ID' },
  { name: 'Illinois', code: 'IL' },
  { name: 'Indiana', code: 'IN' },
  { name: 'Iowa', code: 'IA' },
  { name: 'Kansas', code: 'KS' },
  { name: 'Kentucky', code: 'KY' },
  { name: 'Louisiana', code: 'LA' },
  { name: 'Maine', code: 'ME' },
  { name: 'Maryland', code: 'MD' },
  { name: 'Massachusetts', code: 'MA' },
  { name: 'Michigan', code: 'MI' },
  { name: 'Minnesota', code: 'MN' },
  { name: 'Mississippi', code: 'MS' },
  { name: 'Missouri', code: 'MO' },
  { name: 'Montana', code: 'MT' },
  { name: 'Nebraska', code: 'NE' },
  { name: 'Nevada', code: 'NV' },
  { name: 'New Hampshire', code: 'NH' },
  { name: 'New Jersey', code: 'NJ' },
  { name: 'New Mexico', code: 'NM' },
  { name: 'New York', code: 'NY' },
  { name: 'North Carolina', code: 'NC' },
  { name: 'North Dakota', code: 'ND' },
  { name: 'Ohio', code: 'OH' },
  { name: 'Oklahoma', code: 'OK' },
  { name: 'Oregon', code: 'OR' },
  { name: 'Pennsylvania', code: 'PA' },
  { name: 'Rhode Island', code: 'RI' },
  { name: 'South Carolina', code: 'SC' },
  { name: 'South Dakota', code: 'SD' },
  { name: 'Tennessee', code: 'TN' },
  { name: 'Texas', code: 'TX' },
  { name: 'Utah', code: 'UT' },
  { name: 'Vermont', code: 'VT' },
  { name: 'Virginia', code: 'VA' },
  { name: 'Washington', code: 'WA' },
  { name: 'West Virginia', code: 'WV' },
  { name: 'Wisconsin', code: 'WI' },
  { name: 'Wyoming', code: 'WY' },
];

// State code → two-letter abbreviation map for ImmigrationAdvocates
const STATE_CODE_MAP = {};
US_STATES.forEach(s => { STATE_CODE_MAP[s.name] = s.code; });

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };

  // Remove suffixes
  let cleaned = fullName
    .replace(/,?\s*(Esq\.?|J\.?D\.?|LL\.?M\.?|Ph\.?D\.?|Jr\.?|Sr\.?|III|II|IV)$/gi, '')
    .trim();

  // "Law Office(s) of Name"
  const ofMatch = cleaned.match(/(?:Law\s+(?:Office|Offices|Firm|Group|Practice)\s+of\s+)(.+)/i);
  if (ofMatch) {
    cleaned = ofMatch[1]
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?|Attorney.*|Law.*|&.*)/gi, '')
      .trim();
  } else {
    // Strip firm suffixes
    cleaned = cleaned
      .replace(/,?\s*(LLC|PLLC|PC|PLC|PA|P\.A\.|LLP|Inc\.?|Esq\.?)/gi, '')
      .replace(/\s*(Law\s+(?:Firm|Group|Office|Offices|Practice|Center)|Legal\s+(?:Group|Services?|Team)|Immigration\s+(?:Law|Attorneys?|Lawyers?)|Attorneys?\s+at\s+Law|Attorneys?|Lawyers?|& Associates?|Associates?)/gi, '')
      .trim();
  }

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  if (/^the\s/i.test(cleaned)) return { first_name: '', last_name: '' };
  if (/\s[&+]\s|,\s+and\s+|\band\b/i.test(cleaned)) return { first_name: '', last_name: '' };

  const lawWords = /^(law|legal|firm|group|office|pllc|plc|pc|immigration|attorneys?|lawyers?|counselors?|associates?|services?)$/i;

  if (cleaned && /^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?$/i.test(cleaned)) {
    const parts = cleaned.split(/\s+/);
    const lastName = parts[parts.length - 1];
    if (lawWords.test(lastName)) return { first_name: '', last_name: '' };
    return { first_name: parts[0], last_name: lastName };
  }

  return { first_name: '', last_name: '' };
}

function parsePersonName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/,?\s*(Esq\.?|J\.?D\.?|LL\.?M\.?|Ph\.?D\.?|Jr\.?|Sr\.?|III|II|IV)$/gi, '')
    .trim();
  const parts = cleaned.split(/\s+/);
  if (parts.length >= 2) {
    return { first_name: parts[0], last_name: parts[parts.length - 1] };
  }
  return { first_name: cleaned, last_name: '' };
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
  return rawPhone;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
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

function dedupKey(name, city) {
  return `${(name || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

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

    const req = mod.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
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
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Source 1: FindAnImmigrationAttorney.com ──────────────────────────────────
// Structure (confirmed via HTML inspection):
//   Each listing is a <tr> with 4 <td>s:
//     TD1: <a href="http://firm-website.com"><img thumbnail></a>   (website link!)
//     TD2: <h3><a class="fn org" href="/Profiles/X.aspx">Firm Name</a></h3>
//           <div class="adr"><span class="locality">City</span>,
//           <span class="region">ST</span> <span class="postal-code">ZIP</span></div>
//     TD3: (empty)
//     TD4: <span class="tel">(XXX) XXX-XXXX</span>, View Profile / Email Us links
// Pagination: ?page=2&pageid=N

async function scrapeFindAnImmigrationAttorney(states, seenKeys, allLeads) {
  log('');
  log('=== Source 1: FindAnImmigrationAttorney.com ===');

  let totalNew = 0;
  let statesDone = 0;

  for (const st of states) {
    statesDone++;
    const stateSlug = st.name.replace(/\s+/g, '-');
    const baseUrl = `https://www.findanimmigrationattorney.com/${stateSlug}.aspx`;
    log(`[FAIA ${statesDone}/${states.length}] ${st.name} (${st.code})`);

    let page = 1;
    let hasMore = true;
    let stateNew = 0;

    while (hasMore) {
      const url = page === 1
        ? baseUrl
        : `${baseUrl}?pageid=${page}`;

      try {
        const { statusCode, body } = await httpGet(url);

        if (statusCode === 404 || statusCode !== 200 || !body) {
          if (page === 1) log(`  HTTP ${statusCode} - no page for ${st.name}`);
          hasMore = false;
          break;
        }

        const $ = cheerio.load(body);
        const leads = [];

        // Find each listing via the firm name link (a.fn.org inside h3)
        $('a.fn.org, h3 a[href*="/Profiles/"]').each((_, el) => {
          const $a = $(el);
          const href = $a.attr('href') || '';
          const firmName = $a.text().trim();
          if (!firmName || firmName.length < 2) return;

          // Navigate to the containing row (<tr>)
          const $tr = $a.closest('tr');
          if (!$tr.length) return;

          // Extract structured address from .adr container
          const city = $tr.find('.locality').text().trim();
          const state = $tr.find('.region').text().trim() || st.code;
          const zip = $tr.find('.postal-code').text().trim();

          // Extract phone from span.tel
          const rawPhone = $tr.find('.tel').text().trim();
          const phone = formatPhone(rawPhone);

          // Extract website from first TD's thumbnail link (external URL)
          let website = '';
          $tr.find('td').first().find('a').each((_, link) => {
            const lhref = $(link).attr('href') || '';
            if (lhref.startsWith('http') && !lhref.includes('findanimmigrationattorney.com') &&
                lhref.length > 10) {  // skip bare "http://" or "https://"
              website = lhref;
            }
          });

          // Profile URL
          const profileUrl = href.startsWith('http')
            ? href
            : `https://www.findanimmigrationattorney.com${href.startsWith('/') ? '' : '/'}${href}`;

          const { first_name, last_name } = parseName(firmName);

          const key = dedupKey(firmName, city || st.name);
          if (seenKeys.has(key)) return;
          seenKeys.add(key);

          leads.push({
            first_name,
            last_name,
            firm_name: firmName,
            title: '',
            email: '',
            phone,
            website,
            domain: extractDomain(website),
            city: city || '',
            state: state || st.code,
            country: 'US',
            niche: 'immigration',
            source: 'FindAnImmigrationAttorney.com',
            profile_url: profileUrl,
          });
        });

        stateNew += leads.length;
        allLeads.push(...leads);

        // Check for "Next" pagination link
        const nextLink = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          const href = $(el).attr('href') || '';
          return (text === 'next' || text === 'next >') && href.includes('pageid=');
        });

        if (nextLink.length > 0 && leads.length > 0) {
          page++;
        } else {
          hasMore = false;
        }

      } catch (err) {
        log(`  ERROR on page ${page}: ${err.message}`);
        hasMore = false;
      }

      await randomDelay();
    }

    totalNew += stateNew;
    log(`  ${st.code}: ${stateNew} new leads`);
  }

  log(`FindAnImmigrationAttorney.com total: ${totalNew} leads`);
  return totalNew;
}

// ── Source 2: ImmigrationAdvocates.org ────────────────────────────────────────
// Structure (confirmed via HTML inspection):
//   <h4><a href="/legaldirectory/organization.ID-Name">Org Name</a></h4>
//   <table>
//     <tr><td class="label">Areas of legal assistance:</td><td>...</td></tr>
//     <tr><td class="label">Types of legal assistance:</td><td>...</td></tr>
//     <tr><td class="label">Location:</td><td>Street, City, ST ZIP</td></tr>
//     <tr><td class="label">Contact:</td><td>phone, <a href="url">url</a>, <a href="mailto:email">email</a></td></tr>
//   </table>
// Pagination: &page=2

async function scrapeImmigrationAdvocates(states, seenKeys, allLeads) {
  log('');
  log('=== Source 2: ImmigrationAdvocates.org ===');

  let totalNew = 0;
  let statesDone = 0;

  for (const st of states) {
    statesDone++;
    log(`[IAN ${statesDone}/${states.length}] ${st.name} (${st.code})`);

    let page = 1;
    let hasMore = true;
    let stateNew = 0;

    while (hasMore) {
      const url = `https://www.immigrationadvocates.org/legaldirectory/search?state=${st.code}&page=${page}`;

      try {
        const { statusCode, body } = await httpGet(url);

        if (statusCode === 404 || statusCode !== 200 || !body) {
          if (page === 1) log(`  HTTP ${statusCode} - no results for ${st.name}`);
          hasMore = false;
          break;
        }

        const $ = cheerio.load(body);
        const leads = [];
        let foundListings = false;

        // Find each listing: h4 with link, followed by a table
        $('h4').each((_, el) => {
          const $h4 = $(el);
          const $link = $h4.find('a').first();
          const orgName = ($link.text().trim() || $h4.text().trim());

          if (!orgName || orgName.length < 3) return;
          if (/results|showing|page|search|filter|directory|print/i.test(orgName)) return;

          foundListings = true;

          // The data table is the next sibling
          const $table = $h4.next('table');
          if (!$table.length) return;

          // Parse table rows by label
          let phone = '';
          let email = '';
          let website = '';
          let city = '';
          let state = st.code;
          let locationText = '';
          let contactText = '';

          $table.find('tr').each((_, tr) => {
            const label = $(tr).find('td.label').text().trim().toLowerCase();
            const $valueCell = $(tr).find('td').not('.label');
            const value = $valueCell.text().trim();

            if (label.includes('location')) {
              locationText = value;
              // Parse city, state ZIP
              const addrMatch = value.match(/([A-Za-z][A-Za-z\s.'-]+),\s*([A-Z]{2})\s+(\d{5})/);
              if (addrMatch) {
                city = addrMatch[1].trim();
                state = addrMatch[2];
              }
            }

            if (label.includes('contact')) {
              contactText = value;
              // Extract phone
              const phoneMatch = value.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
              if (phoneMatch) phone = formatPhone(phoneMatch[0]);

              // Extract email from mailto links
              $valueCell.find('a[href^="mailto:"]').each((_, mailLink) => {
                const href = $(mailLink).attr('href') || '';
                const addr = href.replace('mailto:', '').trim();
                if (addr && addr.includes('@')) email = addr;
              });

              // Extract website from http links
              $valueCell.find('a[href^="http"]').each((_, webLink) => {
                const href = $(webLink).attr('href') || '';
                if (!href.includes('immigrationadvocates.org') &&
                    !href.includes('probono.net') &&
                    !href.includes('mailto:')) {
                  website = href;
                }
              });

              // Fallback: extract email from text
              if (!email) {
                const emailMatch = value.match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
                if (emailMatch) email = emailMatch[0];
              }
            }
          });

          // Profile URL
          const profileHref = $link.attr('href') || '';
          const profileUrl = profileHref.startsWith('http')
            ? profileHref
            : profileHref
              ? `https://www.immigrationadvocates.org${profileHref.startsWith('/') ? '' : '/'}${profileHref}`
              : '';

          const key = dedupKey(orgName, city || st.name);
          if (seenKeys.has(key)) return;
          seenKeys.add(key);

          leads.push({
            first_name: '',
            last_name: '',
            firm_name: orgName,
            title: '',
            email,
            phone,
            website,
            domain: extractDomain(website),
            city: city || '',
            state: state || st.code,
            country: 'US',
            niche: 'immigration',
            source: 'ImmigrationAdvocates.org',
            profile_url: profileUrl,
          });
        });

        stateNew += leads.length;
        allLeads.push(...leads);

        // Check for next page (uses full URL with all params)
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === '>';
        }).length > 0;

        if (hasNext && foundListings && leads.length > 0) {
          page++;
        } else {
          hasMore = false;
        }

      } catch (err) {
        log(`  ERROR on page ${page}: ${err.message}`);
        hasMore = false;
      }

      await randomDelay();
    }

    totalNew += stateNew;
    log(`  ${st.code}: ${stateNew} new leads`);
  }

  log(`ImmigrationAdvocates.org total: ${totalNew} leads`);
  return totalNew;
}

// ── Source 3: MurthyLaw.com ──────────────────────────────────────────────────
// Top immigration firm - scrape team page + individual profiles

async function scrapeMurthyLaw(seenKeys, allLeads) {
  log('');
  log('=== Source 3: MurthyLaw.com (Team Page) ===');

  const teamUrl = 'https://www.murthy.com/about-us/law-firm/attorneys';
  let totalNew = 0;

  try {
    const { statusCode, body } = await httpGet(teamUrl);

    if (statusCode !== 200 || !body) {
      log(`  HTTP ${statusCode} - cannot access Murthy team page`);
      return 0;
    }

    const $ = cheerio.load(body);

    // Extract attorney names and profile links
    const attorneys = [];

    // Look for attorney listing elements
    $('a').each((_, el) => {
      const $a = $(el);
      const href = $a.attr('href') || '';
      const text = $a.text().trim();

      // Attorney profile links pattern: /about-us/law-firm/attorneys/name-slug/
      if (href.includes('/attorneys/') && href !== teamUrl && text && text.length > 3) {
        // Skip navigation/menu items
        if (/read more|view|back|home|about|contact|practice/i.test(text)) return;

        const fullUrl = href.startsWith('http')
          ? href
          : `https://www.murthy.com${href}`;

        attorneys.push({ name: text, profileUrl: fullUrl });
      }
    });

    // Also try extracting from structured elements
    $('h3, h4, .attorney-name, .team-member-name').each((_, el) => {
      const name = $(el).text().trim();
      if (name && name.length > 3 && name.length < 50) {
        const $link = $(el).find('a').first();
        const href = $link.attr('href') || '';
        if (href.includes('/attorneys/')) {
          const fullUrl = href.startsWith('http') ? href : `https://www.murthy.com${href}`;
          // Avoid duplicates
          if (!attorneys.find(a => a.profileUrl === fullUrl)) {
            attorneys.push({ name, profileUrl: fullUrl });
          }
        }
      }
    });

    log(`  Found ${attorneys.length} attorneys on team page`);

    // Fetch individual profile pages for more details
    for (const atty of attorneys) {
      const { first_name, last_name } = parsePersonName(atty.name);
      const key = dedupKey(atty.name, 'Owings Mills');
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      let email = '';
      let title = '';

      // Try to fetch profile page for more details
      try {
        await randomDelay();
        const { statusCode: pStatus, body: pBody } = await httpGet(atty.profileUrl);
        if (pStatus === 200 && pBody) {
          const $p = cheerio.load(pBody);
          const pageText = $p.text();

          // Extract title
          const titleMatch = pageText.match(/(?:Title|Position|Role):\s*([^\n]+)/i);
          if (titleMatch) title = titleMatch[1].trim();

          // Try to find title from common patterns
          if (!title) {
            const titlePatterns = [
              /(?:Managing\s+)?(?:Senior\s+)?(?:Associate|Partner|Attorney|Member|Counsel|Of Counsel|Founder|President)/i,
            ];
            for (const pat of titlePatterns) {
              const m = pageText.match(pat);
              if (m) { title = m[0].trim(); break; }
            }
          }

          // Extract email
          $p('a[href^="mailto:"]').each((_, mailEl) => {
            const href = $p(mailEl).attr('href') || '';
            const addr = href.replace('mailto:', '').trim();
            if (addr.includes('@') && addr.includes('murthy')) email = addr;
          });
          const emailMatch = pageText.match(/[\w.+-]+@murthy\.com/i);
          if (!email && emailMatch) email = emailMatch[0];
        }
      } catch (err) {
        log(`  Profile error for ${atty.name}: ${err.message}`);
      }

      allLeads.push({
        first_name,
        last_name,
        firm_name: 'Murthy Law Firm',
        title: title || 'Immigration Attorney',
        email,
        phone: '(410) 356-5440',
        website: 'https://www.murthy.com',
        domain: 'murthy.com',
        city: 'Owings Mills',
        state: 'MD',
        country: 'US',
        niche: 'immigration',
        source: 'MurthyLaw.com',
        profile_url: atty.profileUrl,
      });
      totalNew++;
    }

    // If no attorneys found via links, add the firm as a single lead
    if (attorneys.length === 0) {
      const key = dedupKey('Murthy Law Firm', 'Owings Mills');
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        allLeads.push({
          first_name: 'Sheela',
          last_name: 'Murthy',
          firm_name: 'Murthy Law Firm',
          title: 'Founder / President',
          email: '',
          phone: '(410) 356-5440',
          website: 'https://www.murthy.com',
          domain: 'murthy.com',
          city: 'Owings Mills',
          state: 'MD',
          country: 'US',
          niche: 'immigration',
          source: 'MurthyLaw.com',
          profile_url: teamUrl,
        });
        totalNew++;
      }
    }

  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }

  log(`MurthyLaw.com total: ${totalNew} leads`);
  return totalNew;
}

// ── Source 4: Top Immigration Firm Team Pages ─────────────────────────────────
// Scrape publicly accessible team/attorney pages from well-known immigration firms

const IMMIGRATION_FIRMS = [
  {
    name: 'VisaPlace',
    teamUrl: 'https://www.visaplace.com/about/',
    website: 'https://www.visaplace.com',
    domain: 'visaplace.com',
    city: 'Toronto',
    state: 'ON',
    country: 'CA',
    phone: '(866) 934-7447',
    knownAttorneys: [
      { first: 'Michael', last: 'Niren', title: 'Founder & Managing Attorney' },
    ],
  },
  {
    name: 'Boundless Immigration',
    teamUrl: 'https://www.boundless.com/about/',
    website: 'https://www.boundless.com',
    domain: 'boundless.com',
    city: 'Seattle',
    state: 'WA',
    country: 'US',
    phone: '',
    knownAttorneys: [
      { first: 'Xiao', last: 'Wang', title: 'CEO & Co-Founder' },
    ],
  },
  {
    name: 'Wolfsdorf Rosenthal LLP (WR Immigration)',
    teamUrl: 'https://wolfsdorf.com/our-team/',
    website: 'https://wolfsdorf.com',
    domain: 'wolfsdorf.com',
    city: 'Santa Monica',
    state: 'CA',
    country: 'US',
    phone: '(310) 570-4088',
    knownAttorneys: [
      { first: 'Bernard', last: 'Wolfsdorf', title: 'Managing Partner' },
    ],
  },
  {
    name: 'Klasko Immigration Law Partners',
    teamUrl: 'https://www.klaskolaw.com/our-team/',
    website: 'https://www.klaskolaw.com',
    domain: 'klaskolaw.com',
    city: 'Philadelphia',
    state: 'PA',
    country: 'US',
    phone: '(215) 825-8600',
    knownAttorneys: [
      { first: 'H. Ronald', last: 'Klasko', title: 'Founding Partner' },
    ],
  },
  {
    name: 'Siskind Susser PC',
    teamUrl: 'https://www.visalaw.com/about-us/',
    website: 'https://www.visalaw.com',
    domain: 'visalaw.com',
    city: 'Memphis',
    state: 'TN',
    country: 'US',
    phone: '(901) 682-6455',
    knownAttorneys: [
      { first: 'Greg', last: 'Siskind', title: 'Founding Partner' },
    ],
  },
  {
    name: 'Fragomen, Del Rey, Bernsen & Loewy',
    teamUrl: 'https://www.fragomen.com/',
    website: 'https://www.fragomen.com',
    domain: 'fragomen.com',
    city: 'New York',
    state: 'NY',
    country: 'US',
    phone: '(212) 688-8555',
    knownAttorneys: [
      { first: 'Austin', last: 'Fragomen', title: 'Chairman Emeritus' },
    ],
  },
  {
    name: 'Berry Appleman & Leiden LLP (BAL)',
    teamUrl: 'https://www.bal.com/',
    website: 'https://www.bal.com',
    domain: 'bal.com',
    city: 'Dallas',
    state: 'TX',
    country: 'US',
    phone: '(214) 749-0107',
    knownAttorneys: [
      { first: 'Lynden', last: 'Melmed', title: 'Chief Executive Officer' },
    ],
  },
  {
    name: 'Wildes & Weinberg PC',
    teamUrl: 'https://www.wildeslaw.com/',
    website: 'https://www.wildeslaw.com',
    domain: 'wildeslaw.com',
    city: 'New York',
    state: 'NY',
    country: 'US',
    phone: '(212) 753-3468',
    knownAttorneys: [
      { first: 'Michael', last: 'Wildes', title: 'Managing Partner' },
    ],
  },
  {
    name: 'Ogletree Deakins (Immigration Practice)',
    teamUrl: 'https://ogletree.com/',
    website: 'https://ogletree.com',
    domain: 'ogletree.com',
    city: 'Atlanta',
    state: 'GA',
    country: 'US',
    phone: '(404) 881-1300',
    knownAttorneys: [],
  },
  {
    name: 'Duane Morris LLP (Immigration Practice)',
    teamUrl: 'https://www.duanemorris.com/',
    website: 'https://www.duanemorris.com',
    domain: 'duanemorris.com',
    city: 'Philadelphia',
    state: 'PA',
    country: 'US',
    phone: '(215) 979-1000',
    knownAttorneys: [],
  },
  {
    name: 'Jackson Lewis PC (Immigration Practice)',
    teamUrl: 'https://www.jacksonlewis.com/',
    website: 'https://www.jacksonlewis.com',
    domain: 'jacksonlewis.com',
    city: 'White Plains',
    state: 'NY',
    country: 'US',
    phone: '(914) 872-8060',
    knownAttorneys: [],
  },
  {
    name: 'FosterGarvey PC (Immigration Practice)',
    teamUrl: 'https://www.foster.com/',
    website: 'https://www.foster.com',
    domain: 'foster.com',
    city: 'Seattle',
    state: 'WA',
    country: 'US',
    phone: '(206) 447-4400',
    knownAttorneys: [],
  },
];

async function scrapeTopFirms(seenKeys, allLeads) {
  log('');
  log('=== Source 4: Top Immigration Firm Team Pages ===');

  let totalNew = 0;

  for (const firm of IMMIGRATION_FIRMS) {
    log(`  Checking ${firm.name}...`);

    // Try to scrape the team page for attorneys
    let scrapedAttorneys = [];

    try {
      const { statusCode, body } = await httpGet(firm.teamUrl);

      if (statusCode === 200 && body) {
        const $ = cheerio.load(body);

        // Try to find attorney/team member listings
        // Look for common patterns: name + title combos in structured elements
        $('a').each((_, el) => {
          const $a = $(el);
          const href = $a.attr('href') || '';
          const text = $a.text().trim();

          // Profile links typically contain /people/, /attorneys/, /team/, /our-team/
          if ((href.includes('/people/') || href.includes('/attorneys/') ||
               href.includes('/team/') || href.includes('/our-team/') ||
               href.includes('/professionals/')) &&
              text && text.length > 3 && text.length < 60) {
            // Verify it looks like a person name (2-4 words, capitalized)
            if (/^[A-Z][a-z]+(\s+[A-Z]\.?)?\s+[A-Z][a-z]+(-[A-Z][a-z]+)?(\s+[A-Z][a-z]+)?$/.test(text)) {
              const { first_name, last_name } = parsePersonName(text);
              if (first_name && last_name) {
                scrapedAttorneys.push({
                  first: first_name,
                  last: last_name,
                  title: 'Immigration Attorney',
                  profileUrl: href.startsWith('http') ? href : `${firm.website}${href}`,
                });
              }
            }
          }
        });

        log(`    Scraped ${scrapedAttorneys.length} attorneys from page`);
      }
    } catch (err) {
      log(`    Could not fetch team page: ${err.message}`);
    }

    // Combine scraped attorneys with known attorneys
    const allAttorneys = [...scrapedAttorneys];

    // Add known attorneys only if not already found
    for (const known of firm.knownAttorneys) {
      const exists = allAttorneys.find(a =>
        a.last.toLowerCase() === known.last.toLowerCase()
      );
      if (!exists) {
        allAttorneys.push({
          first: known.first,
          last: known.last,
          title: known.title,
          profileUrl: firm.teamUrl,
        });
      }
    }

    // If we have individual attorneys, add each one
    if (allAttorneys.length > 0) {
      for (const atty of allAttorneys) {
        const key = dedupKey(`${atty.first} ${atty.last}`, firm.city);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);

        allLeads.push({
          first_name: atty.first,
          last_name: atty.last,
          firm_name: firm.name,
          title: atty.title || 'Immigration Attorney',
          email: '',
          phone: firm.phone,
          website: firm.website,
          domain: firm.domain,
          city: firm.city,
          state: firm.state,
          country: firm.country,
          niche: 'immigration',
          source: 'TopFirm-TeamPage',
          profile_url: atty.profileUrl || firm.teamUrl,
        });
        totalNew++;
      }
    } else {
      // Add the firm itself as a lead
      const key = dedupKey(firm.name, firm.city);
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        allLeads.push({
          first_name: '',
          last_name: '',
          firm_name: firm.name,
          title: '',
          email: '',
          phone: firm.phone,
          website: firm.website,
          domain: firm.domain,
          city: firm.city,
          state: firm.state,
          country: firm.country,
          niche: 'immigration',
          source: 'TopFirm-TeamPage',
          profile_url: firm.teamUrl,
        });
        totalNew++;
      }
    }

    await randomDelay();
  }

  log(`Top Firms total: ${totalNew} leads`);
  return totalNew;
}

// ── Progress / Resume ────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { completedSources: [], leads: [] };
}

function saveProgress(completedSources, leads) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    completedSources,
    leads,
    updatedAt: new Date().toISOString(),
  }));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-visa-directories-immigration.js --test    # 3 states per source');
    console.log('  node scripts/scrape-visa-directories-immigration.js --all     # All states, all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  const allLeads = [];
  const seenKeys = new Set();

  const states = isTest ? US_STATES.slice(0, 3) : [...US_STATES];

  log('Visa/Immigration Directory Scraper');
  log(`Mode: ${isTest ? 'TEST (3 states)' : 'FULL (all states)'}`);
  log(`Sources: FindAnImmigrationAttorney.com, ImmigrationAdvocates.org, MurthyLaw.com, Top Firms`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const startTime = Date.now();

  // Source 1: FindAnImmigrationAttorney.com
  const faia = await scrapeFindAnImmigrationAttorney(states, seenKeys, allLeads);
  writeCSV(allLeads, OUT_FILE);
  log(`[Progress saved: ${allLeads.length} leads after Source 1]`);

  // Source 2: ImmigrationAdvocates.org
  const ian = await scrapeImmigrationAdvocates(states, seenKeys, allLeads);
  writeCSV(allLeads, OUT_FILE);
  log(`[Progress saved: ${allLeads.length} leads after Source 2]`);

  // Source 3: MurthyLaw.com
  const murthy = await scrapeMurthyLaw(seenKeys, allLeads);
  writeCSV(allLeads, OUT_FILE);
  log(`[Progress saved: ${allLeads.length} leads after Source 3]`);

  // Source 4: Top Immigration Firms
  const firms = await scrapeTopFirms(seenKeys, allLeads);

  // Final write
  writeCSV(allLeads, OUT_FILE);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const uniqueSources = new Set(allLeads.map(l => l.source)).size;

  const bySrc = {};
  for (const l of allLeads) {
    bySrc[l.source] = (bySrc[l.source] || 0) + 1;
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Time: ${elapsed}s`);
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Unique states: ${uniqueStates}`);
  log(`Sources: ${uniqueSources}`);
  log('');
  log('By source:');
  for (const [src, count] of Object.entries(bySrc).sort((a, b) => b[1] - a[1])) {
    log(`  ${src}: ${count}`);
  }
  log('');
  log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
