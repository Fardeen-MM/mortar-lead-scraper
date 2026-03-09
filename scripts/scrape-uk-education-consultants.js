#!/usr/bin/env node
/**
 * UK Education Consultants & Agents Scraper
 *
 * Scrapes multiple UK education consultant/agent directories:
 *
 *   Source 1: AEGIS (Association for the Education & Guardianship of International Students)
 *             ~364 orgs (66 guardianship orgs with full contact + ~298 schools/colleges)
 *             URL: https://aegisuk.net/organisations/
 *             Structure: .list-item cards with .card-title, p.email, p.phone-number,
 *                        p.website, .see-more-trigger[data-*] attributes
 *
 *   Source 2: English UK Partner Agency Directory
 *             248 partner agencies (study abroad / language education)
 *             URL: https://www.englishuk.com/.../english-uk-partner-agency-directory
 *             Structure: <li> with <h3><a>name</a></h3> + <p>address, tel, website, email</p>
 *
 *   Source 3: Education Agents Guide -- UK directory
 *             ~1900+ education agents with profile detail pages
 *             URL: https://educationagentsguide.com/united_kingdom/index.htm
 *             Structure: Index page with <a href="xxx.htm">name</a> links
 *                        Profile pages with contact details in free-form text
 *
 *   Source 4: UKCISA (UK Council for International Student Affairs)
 *             ~389 member institutions (HE, FE, students unions, corporate)
 *             URL: https://www.ukcisa.org.uk/Membership/List-of-current-members
 *             Structure: <ul><li>Institution Name</li></ul> grouped by alphabet
 *
 *   Source 5: QAA (Quality Assurance Agency) provider search
 *             ~130 registered higher education providers
 *             URL: https://www.qaa.ac.uk/.../provider-search
 *             Structure: Paginated list of <a href="/provider/...">Name</a> links
 *
 * Usage:
 *   node scripts/scrape-uk-education-consultants.js --test     # Test mode (few per source)
 *   node scripts/scrape-uk-education-consultants.js --all      # Full scrape all sources
 *   node scripts/scrape-uk-education-consultants.js --source aegis    # Single source
 *   node scripts/scrape-uk-education-consultants.js --source englishuk
 *   node scripts/scrape-uk-education-consultants.js --source edagents
 *   node scripts/scrape-uk-education-consultants.js --source ukcisa
 *   node scripts/scrape-uk-education-consultants.js --source qaa
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-education-consultants-extra.csv');
const DELAY_MIN = 2000;
const DELAY_MAX = 3000;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 2;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function getRandomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|\s|-)\S/g, c => c.toUpperCase())
    .replace(/\bLlp\b/g, 'LLP')
    .replace(/\bLtd\b/g, 'Ltd')
    .replace(/\bPlc\b/g, 'PLC');
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(leads, filePath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(filePath, header + '\n' + rows.join('\n') + '\n');
}

function extractDomain(website) {
  if (!website) return '';
  try {
    let url = website;
    if (!url.startsWith('http')) url = `https://${url}`;
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function cleanPhone(phone) {
  if (!phone) return '';
  // Take only the first phone number if multiple are separated by /
  phone = phone.split('/')[0].trim();
  let p = phone.replace(/[\s\-\(\)\.]/g, '');
  if (p.startsWith('0')) {
    p = '+44' + p.slice(1);
  } else if (p.startsWith('44') && !p.startsWith('+')) {
    p = '+' + p;
  } else if (!p.startsWith('+')) {
    if (p.length >= 10) p = '+' + p;
  }
  return p;
}

function cleanEmail(email) {
  if (!email) return '';
  return email.toLowerCase().trim().replace(/^mailto:/i, '');
}

function cleanWebsite(url) {
  if (!url) return '';
  url = url.trim();
  if (url.startsWith('//')) url = 'https:' + url;
  if (!url.startsWith('http')) url = 'https://' + url;
  return url;
}

function parseCityFromAddress(address) {
  if (!address) return '';
  // Try to extract city from UK address patterns
  const parts = address.split(',').map(p => p.trim());

  // Known UK cities
  const knownCities = new Set([
    'london', 'manchester', 'birmingham', 'leeds', 'liverpool', 'sheffield',
    'bristol', 'cardiff', 'edinburgh', 'glasgow', 'belfast', 'newcastle',
    'nottingham', 'leicester', 'coventry', 'bradford', 'southampton',
    'oxford', 'cambridge', 'brighton', 'york', 'bath', 'exeter', 'norwich',
    'cheltenham', 'malvern', 'ipswich', 'guildford', 'reading', 'canterbury',
  ]);

  // UK counties to skip
  const counties = new Set([
    'middlesex', 'surrey', 'essex', 'kent', 'sussex', 'hampshire', 'berkshire',
    'hertfordshire', 'buckinghamshire', 'oxfordshire', 'cambridgeshire', 'norfolk',
    'suffolk', 'devon', 'dorset', 'somerset', 'wiltshire', 'gloucestershire',
    'warwickshire', 'staffordshire', 'lancashire', 'yorkshire', 'cheshire',
    'worcestershire', 'shropshire', 'cornwall', 'cumbria',
  ]);

  for (const part of parts) {
    const lower = part.toLowerCase();
    if (knownCities.has(lower)) return titleCase(part);
  }

  // Try second-to-last part (after street, before postcode)
  for (let i = parts.length - 1; i >= 0; i--) {
    const part = parts[i].trim();
    const lower = part.toLowerCase();
    if (/^[A-Z]{1,2}\d/i.test(part)) continue; // Skip postcodes
    if (counties.has(lower)) continue;
    if (/^(england|wales|scotland|uk|united kingdom)$/i.test(part)) continue;
    if (/\d/.test(part)) continue; // Skip lines with numbers (addresses)
    if (/\b(road|street|lane|drive|close|way|avenue|house|floor|suite|unit)\b/i.test(part)) continue;
    if (part.length > 3 && part.length < 40) return titleCase(part);
  }

  return '';
}

// ── HTTP Client ──────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const transport = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': getRandomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = transport.request(options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
        return resolve(httpGet(redirectUrl, retries));
      }

      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });

    req.on('error', (err) => {
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        setTimeout(() => resolve(httpGet(url, retries - 1)), 3000);
      } else {
        reject(new Error('Request timed out'));
      }
    });

    req.end();
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Source 1: AEGIS — Accredited Guardianship Organisations + Member Schools
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeAEGIS(isTest) {
  log('\n=== Source 1: AEGIS Guardianship Organisations ===');
  const leads = [];

  const url = 'https://aegisuk.net/organisations/';
  log(`  Fetching ${url}`);

  const { statusCode, body } = await httpGet(url);
  if (statusCode !== 200) {
    log(`  ERROR: Got status ${statusCode}`);
    return leads;
  }

  const $ = cheerio.load(body);

  // Strategy A: Extract from .see-more-trigger data attributes (guardianship orgs with full data)
  const seeMoreLinks = $('.see-more-trigger');
  log(`  Found ${seeMoreLinks.length} guardianship orgs with full data (see-more-trigger)`);

  seeMoreLinks.each((i, el) => {
    if (isTest && i >= 10) return;

    const $el = $(el);
    const name = ($el.attr('data-title') || '').replace(/&nbsp;/g, ' ').trim();
    const address = $el.attr('data-address') || '';
    const website = $el.attr('data-website') || '';
    const phone = $el.attr('data-phone') || '';
    const email = $el.attr('data-email') || '';
    const info = $el.attr('data-info') || '';

    if (!name) return;

    const city = parseCityFromAddress(address);

    leads.push({
      first_name: '',
      last_name: '',
      firm_name: name,
      title: 'Education Guardian',
      email: cleanEmail(email),
      phone: cleanPhone(phone),
      website: website ? cleanWebsite(website) : '',
      domain: extractDomain(website),
      city,
      state: 'UK',
      country: 'UK',
      niche: 'education',
      source: 'aegis',
      profile_url: url,
    });
  });

  log(`  Guardianship orgs: ${leads.length}`);

  // Strategy B: Extract from .list-item cards (schools, colleges without see-more)
  const cards = $('.list-item');
  let schoolCount = 0;

  cards.each((i, el) => {
    if (isTest && schoolCount >= 10) return;

    const $card = $(el);
    // Skip cards that already have see-more (already processed above)
    if ($card.find('.see-more-trigger').length > 0) return;

    const name = $card.find('.card-title, .title').first().text().trim();
    if (!name || name.length < 3) return;

    // Address is in the h5 excerpt
    const address = $card.find('.uk-h5, .card-excerpt').first().text().trim();

    // Website from p.website text
    const websiteText = $card.find('p.website').text().trim();
    const website = websiteText || '';

    // Phone from p.phone-number text
    const phoneText = $card.find('p.phone-number').text().trim();
    const phone = phoneText || '';

    // Email from p.email text
    const emailText = $card.find('p.email').text().trim();
    const email = emailText || '';

    // Website from card link if not in p.website
    let websiteUrl = website;
    if (!websiteUrl) {
      const cardLink = $card.find('.card > a').first().attr('href');
      if (cardLink && !cardLink.includes('aegis')) {
        websiteUrl = cardLink;
      }
    }

    const city = parseCityFromAddress(address);

    leads.push({
      first_name: '',
      last_name: '',
      firm_name: name,
      title: 'Member School',
      email: cleanEmail(email),
      phone: cleanPhone(phone),
      website: websiteUrl ? cleanWebsite(websiteUrl) : '',
      domain: extractDomain(websiteUrl),
      city,
      state: 'UK',
      country: 'UK',
      niche: 'education',
      source: 'aegis',
      profile_url: url,
    });
    schoolCount++;
  });

  log(`  Schools/colleges: ${schoolCount}`);
  log(`  AEGIS total: ${leads.length} leads (${leads.filter(l => l.email).length} with email, ${leads.filter(l => l.phone).length} with phone)`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Source 2: English UK Partner Agency Directory
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeEnglishUK(isTest) {
  log('\n=== Source 2: English UK Partner Agency Directory ===');
  const leads = [];

  // 248 agencies, 20 per page, 13 pages (0-indexed)
  const baseUrl = 'https://www.englishuk.com/en/students/your-study-options/find-an-educational-agent/english-uk-partner-agency-directory';
  const maxPages = isTest ? 2 : 13;

  for (let page = 0; page < maxPages; page++) {
    const url = `${baseUrl}?letter_index=&keywords=&country=0&page=${page}`;
    log(`  Page ${page + 1}/${maxPages}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200) {
        log(`    WARNING: Status ${statusCode}`);
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);
      let pageCount = 0;

      // Structure: <li> containing <h3><a>Name</a></h3> then <p> with address, tel, website, email
      $('li').each((_, el) => {
        const $li = $(el);
        const $h3 = $li.find('h3');
        if ($h3.length === 0) return;

        const $nameLink = $h3.find('a');
        const firmName = $nameLink.text().trim();
        if (!firmName || firmName.length < 2) return;

        // Profile URL from the h3 link
        let profileUrl = $nameLink.attr('href') || '';
        if (profileUrl && !profileUrl.startsWith('http')) {
          profileUrl = `https://www.englishuk.com${profileUrl}`;
        }

        // Parse the <p> block for contact details
        const $p = $li.find('p');
        const pText = $p.text();
        const pHtml = $p.html() || '';

        // Email from mailto link
        let email = '';
        const $mailto = $p.find('a[href^="mailto:"]');
        if ($mailto.length > 0) {
          email = ($mailto.attr('href') || '').replace('mailto:', '').split('?')[0].trim();
        }

        // Website from <a href="http..."> that's not mailto
        let website = '';
        $p.find('a[href^="http"]').each((_, a) => {
          const href = $(a).attr('href') || '';
          if (!href.includes('englishuk') && !href.includes('mailto') && !website) {
            website = href;
          }
        });

        // Phone from "Tel " prefix
        const telMatch = pText.match(/Tel\s+(\+?[\d\s\-\(\)]+)/i);
        const phone = telMatch ? telMatch[1].trim() : '';

        // Address: everything before "Tel" in the <p>
        // Split by <br> tags and take lines before Tel/website/email
        const lines = pHtml.split(/<br\s*\/?>/i).map(l => cheerio.load(l).text().trim()).filter(l => l);
        let address = '';
        for (const line of lines) {
          if (/^Tel\s/i.test(line)) break;
          if (line.includes('@')) break;
          if (line.startsWith('www.') || line.startsWith('http')) break;
          if (line === 'read more') break;
          if (line.length > 3) {
            address = address ? address + ', ' + line : line;
          }
        }

        // Extract country from address (last part)
        let country = '';
        const addrParts = address.split(',').map(p => p.trim());
        const lastPart = addrParts[addrParts.length - 1] || '';
        const knownCountries = [
          'United Kingdom', 'UK', 'Italy', 'Turkey', 'Brazil', 'Spain', 'Russia',
          'South Korea', 'Japan', 'China', 'India', 'Pakistan', 'Saudi Arabia',
          'Colombia', 'Mexico', 'Germany', 'France', 'Portugal', 'Switzerland',
          'Poland', 'Kuwait', 'Qatar', 'Oman', 'UAE', 'Lebanon', 'Belarus',
          'Ukraine', 'Kazakhstan', 'Georgia', 'Vietnam', 'Thailand', 'Malaysia',
          'Indonesia', 'Philippines', 'Nigeria', 'Ghana', 'Kenya', 'Egypt',
          'Morocco', 'Iran', 'Iraq', 'Jordan', 'Azerbaijan', 'Uzbekistan',
          'Taiwan', 'Hong Kong', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Myanmar',
          'Cambodia', 'USA', 'Canada', 'Australia', 'New Zealand', 'Ireland',
          'Argentina', 'Chile', 'Peru', 'Ecuador', 'Venezuela',
        ];
        if (knownCountries.some(c => c.toLowerCase() === lastPart.toLowerCase())) {
          country = lastPart;
        }

        const city = parseCityFromAddress(address);

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          title: 'Education Agent',
          email: cleanEmail(email),
          phone: cleanPhone(phone),
          website: website ? cleanWebsite(website) : '',
          domain: extractDomain(website || email),
          city,
          state: (country === 'United Kingdom' || country === 'UK') ? 'UK' : '',
          country: country || '',
          niche: 'education',
          source: 'englishuk',
          profile_url: profileUrl || baseUrl,
        });
        pageCount++;
      });

      log(`    ${pageCount} agencies`);
      if (pageCount === 0 && page > 0) {
        log('    No more agencies, stopping pagination');
        break;
      }
    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  // Deduplicate by email or firm name
  const seen = new Set();
  const deduped = [];
  for (const lead of leads) {
    const key = lead.email ? lead.email.toLowerCase() : lead.firm_name.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  log(`  English UK total: ${deduped.length} leads (${deduped.filter(l => l.email).length} with email)`);
  return deduped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Source 3: Education Agents Guide — UK directory
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeEdAgentsGuide(isTest) {
  log('\n=== Source 3: Education Agents Guide (UK) ===');
  const leads = [];

  // Phase 1: Get list of agent profile URLs from index page
  const indexUrl = 'https://educationagentsguide.com/united_kingdom/index.htm';
  log(`  Phase 1: Fetching agent index`);

  const { statusCode, body } = await httpGet(indexUrl);
  if (statusCode !== 200) {
    log(`  ERROR: Index page returned ${statusCode}`);
    return leads;
  }

  const $ = cheerio.load(body);
  const agentLinks = [];

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const text = $(el).text().trim();
    // Profile pages are .htm files in the same directory (not index.htm, not directory.htm)
    if (href.endsWith('.htm') && !href.includes('index.htm') && !href.includes('directory.htm') &&
        text.length > 3 && text.length < 200 &&
        !text.includes('Add') && !text.includes('Contact') && !text.includes('Directory')) {
      const fullUrl = href.startsWith('http')
        ? href
        : `https://educationagentsguide.com/united_kingdom/${href}`;
      // Avoid duplicates in the link list
      if (!agentLinks.find(a => a.url === fullUrl)) {
        agentLinks.push({ url: fullUrl, name: text });
      }
    }
  });

  log(`  Found ${agentLinks.length} agent profile links`);

  // Phase 2: Fetch individual profile pages
  const maxProfiles = isTest ? 10 : agentLinks.length;
  log(`  Phase 2: Fetching ${maxProfiles} profile pages...`);

  let successCount = 0;
  let errorCount = 0;
  let emailCount = 0;

  for (let i = 0; i < maxProfiles; i++) {
    const { url: profileUrl, name: agentName } = agentLinks[i];

    if (i > 0 && i % 50 === 0) {
      log(`  Progress: ${i}/${maxProfiles} (${leads.length} leads, ${emailCount} emails, ${errorCount} errors)`);
    }

    try {
      const { statusCode: pStatus, body: pBody } = await httpGet(profileUrl);
      if (pStatus !== 200) {
        errorCount++;
        // Still create a minimal lead
        leads.push({
          first_name: '', last_name: '',
          firm_name: agentName,
          title: 'Education Agent', email: '', phone: '',
          website: '', domain: '', city: '', state: '',
          country: 'UK', niche: 'education',
          source: 'edagentsguide', profile_url: profileUrl,
        });
        await randomDelay();
        continue;
      }

      const $p = cheerio.load(pBody);
      const fullText = $p('body').text() || '';

      // Extract firm name from page heading
      let firmName = agentName;
      const h1 = $p('h1, h2').first().text().trim();
      if (h1 && h1.length > 3 && h1.length < 150) {
        firmName = h1;
      }

      // Email: mailto links first, then text pattern
      let email = '';
      $p('a[href^="mailto:"]').each((_, el) => {
        if (!email) {
          const href = $p(el).attr('href') || '';
          email = href.replace('mailto:', '').split('?')[0].trim();
        }
      });
      if (!email) {
        const emailMatch = fullText.match(/[\w.\-+]+@[\w.\-]+\.\w{2,}/);
        if (emailMatch) email = emailMatch[0];
      }

      // Phone
      let phone = '';
      $p('a[href^="tel:"]').each((_, el) => {
        if (!phone) phone = ($p(el).attr('href') || '').replace('tel:', '').trim();
      });
      if (!phone) {
        const phoneMatch = fullText.match(/(?:Landline|Phone|Tel|Mobile|WhatsApp)?:?\s*(\+?\d[\d\s\-\(\)]{8,})/i);
        if (phoneMatch) phone = (phoneMatch[1] || phoneMatch[0]).trim();
      }

      // Website: first external link not to social/directory sites
      let website = '';
      $p('a[href*="http"]').each((_, el) => {
        if (website) return;
        const href = $p(el).attr('href') || '';
        if (!href.includes('educationagentsguide') && !href.includes('mailto') &&
            !href.includes('facebook.com') && !href.includes('twitter.com') &&
            !href.includes('linkedin.com') && !href.includes('instagram.com') &&
            !href.includes('youtube.com') && !href.includes('google.com') &&
            !href.includes('skype:')) {
          website = href;
        }
      });

      // City
      let city = '';
      const cityMatch = fullText.match(/(?:City|Location)\s*:\s*(.+)/i);
      if (cityMatch) city = cityMatch[1].trim().split('\n')[0].trim();

      // Country - match only if the line contains a plausible country name
      let country = 'UK';
      const countryMatch = fullText.match(/Country\s*:\s*([A-Za-z\s]+)/i);
      if (countryMatch) {
        const rawCountry = countryMatch[1].trim().split('\n')[0].trim();
        // Only accept if it doesn't look like another field label
        if (rawCountry.length > 1 && rawCountry.length < 50 &&
            !/^(telephone|phone|fax|email|website|address|city|state|skype)/i.test(rawCountry)) {
          country = rawCountry;
        }
      }

      if (email) emailCount++;

      leads.push({
        first_name: '',
        last_name: '',
        firm_name: firmName.replace(/\s+/g, ' ').trim(),
        title: 'Education Agent',
        email: cleanEmail(email),
        phone: cleanPhone(phone),
        website: website ? cleanWebsite(website) : '',
        domain: extractDomain(website || email),
        city,
        state: (country === 'United Kingdom' || country === 'UK') ? 'UK' : '',
        country: country || 'UK',
        niche: 'education',
        source: 'edagentsguide',
        profile_url: profileUrl,
      });
      successCount++;

    } catch (err) {
      errorCount++;
      leads.push({
        first_name: '', last_name: '',
        firm_name: agentName,
        title: 'Education Agent', email: '', phone: '',
        website: '', domain: '', city: '', state: '',
        country: 'UK', niche: 'education',
        source: 'edagentsguide', profile_url: profileUrl,
      });
    }

    // Incremental save every 100
    if ((i + 1) % 100 === 0) {
      log(`    Checkpoint: ${leads.length} leads saved (${i + 1}/${maxProfiles})`);
    }

    await randomDelay();
  }

  log(`  EdAgentsGuide total: ${leads.length} leads (${emailCount} with email, ${errorCount} errors)`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Source 4: UKCISA — Member Institutions
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeUKCISA(isTest) {
  log('\n=== Source 4: UKCISA Member Institutions ===');
  const leads = [];

  const urls = [
    'https://www.ukcisa.org.uk/Membership/List-of-current-members',
    'https://www.ukcisa.org.uk/members-area/become-a-member-institution/our-members/',
  ];

  for (const url of urls) {
    log(`  Fetching ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200) {
        log(`    Status ${statusCode}, trying next URL...`);
        continue;
      }

      const $ = cheerio.load(body);
      let count = 0;

      // UKCISA lists members as simple <li> items within <ul>s
      $('ul li').each((_, el) => {
        const name = $(el).text().trim();
        if (!name || name.length < 4 || name.length > 200) return;
        // Skip navigation items
        if (/^(home|about|contact|login|membership|search|menu|privacy|cookie|terms|skip|close|toggle|show|hide|view|read|click|submit|sign|log)/i.test(name)) return;
        if (/^\d+$/.test(name)) return;
        if (name.includes('©') || name.includes('cookie') || name.includes('privacy')) return;
        // Skip items with URLs or special chars
        if (name.includes('http') || name.includes('www.')) return;

        // Must look like an institution name
        const isInstitution = /university|college|school|institute|academ|council|union|centre|trust|foundation|association|board|service|polytechnic|conservatoire|guild|society/i.test(name);
        if (!isInstitution && name.length < 20) return;

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: name,
          title: 'Member Institution',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: '',
          state: 'UK',
          country: 'UK',
          niche: 'education',
          source: 'ukcisa',
          profile_url: url,
        });
        count++;
      });

      if (count > 0) {
        log(`    Found ${count} member institutions`);
        break;
      }
    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  // Deduplicate by name
  const seen = new Set();
  const deduped = [];
  for (const lead of leads) {
    const key = lead.firm_name.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  if (isTest && deduped.length > 20) {
    log(`  UKCISA total: ${deduped.length} (returning 20 in test mode)`);
    return deduped.slice(0, 20);
  }

  log(`  UKCISA total: ${deduped.length} leads`);
  return deduped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Source 5: QAA — Provider Search
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeQAA(isTest) {
  log('\n=== Source 5: QAA Provider Search ===');
  const leads = [];

  const baseUrl = 'https://www.qaa.ac.uk/reviewing-higher-education/quality-assurance-reports/provider-search';
  const maxPages = isTest ? 2 : 15;

  for (let page = 0; page < maxPages; page++) {
    const url = page === 0 ? baseUrl : `${baseUrl}/Index/${page}/`;
    log(`  Page ${page + 1}/${maxPages}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200) {
        log(`    Status ${statusCode}`);
        if (page > 0) break;
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);
      let pageCount = 0;

      // Provider links are in .search-list-item with class .search-list-item__title
      // href pattern: /reviewing-higher-education/quality-assurance-reports/provider-search/Provider-Name
      $('.search-list-item__title, a[href*="provider-search/"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const name = $(el).text().trim();

        // Must be a provider detail link (not the search page itself or pagination)
        if (!href || href === '#' || href.endsWith('provider-search') || href.endsWith('provider-search/')) return;
        if (!name || name.length < 3 || name.length > 200) return;
        if (/^(home|about|contact|search|page|next|prev|\d+|index)$/i.test(name)) return;
        // Skip pagination links like Index/2/
        if (/\/Index\/\d+\/?$/.test(href)) return;

        const profileUrl = href.startsWith('http') ? href : `https://www.qaa.ac.uk${href}`;
        leads.push({
          first_name: '',
          last_name: '',
          firm_name: name,
          title: 'HE Provider',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: '',
          state: 'UK',
          country: 'UK',
          niche: 'education',
          source: 'qaa',
          profile_url: profileUrl,
        });
        pageCount++;
      });

      log(`    ${pageCount} providers`);
      if (pageCount === 0 && page > 0) break;
    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  // Deduplicate by name
  const seen = new Set();
  const deduped = [];
  for (const lead of leads) {
    const key = lead.firm_name.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  log(`  QAA total: ${deduped.length} leads`);
  return deduped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all') || (!isTest && !args.includes('--source'));

  // Parse --source flag
  let sourceFilter = null;
  const srcIdx = args.indexOf('--source');
  if (srcIdx >= 0 && args[srcIdx + 1]) {
    sourceFilter = args[srcIdx + 1].toLowerCase();
  }

  // Ensure output directory
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  log('=== UK Education Consultants & Agents Scraper ===');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Source filter: ${sourceFilter || 'all'}`);
  log(`Output: ${OUTPUT_FILE}`);

  const allLeads = [];

  const sources = {
    aegis: scrapeAEGIS,
    englishuk: scrapeEnglishUK,
    edagents: scrapeEdAgentsGuide,
    ukcisa: scrapeUKCISA,
    qaa: scrapeQAA,
  };

  for (const [name, fn] of Object.entries(sources)) {
    if (sourceFilter && sourceFilter !== name) continue;

    try {
      const leads = await fn(isTest);
      allLeads.push(...leads);
      log(`  -> ${name}: ${leads.length} leads`);

      // Incremental save after each source
      writeCSV(allLeads, OUTPUT_FILE);
      log(`  -> Saved ${allLeads.length} total leads to CSV`);
    } catch (err) {
      log(`  ERROR in ${name}: ${err.message}`);
      log(`  ${err.stack}`);
    }
  }

  // Final dedup across sources by email (if present) or firm_name+source
  const seen = new Set();
  const deduped = [];
  for (const lead of allLeads) {
    const key = lead.email
      ? lead.email.toLowerCase()
      : `${lead.firm_name.toLowerCase()}|${lead.source}`;
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  writeCSV(deduped, OUTPUT_FILE);

  // Summary
  log(`\n${'='.repeat(60)}`);
  log('DONE');
  log(`Total leads: ${deduped.length} (from ${allLeads.length} raw)`);
  log(`  With email: ${deduped.filter(l => l.email).length}`);
  log(`  With phone: ${deduped.filter(l => l.phone).length}`);
  log(`  With website: ${deduped.filter(l => l.website).length}`);

  // Source breakdown
  log('\nBy source:');
  const bySource = {};
  deduped.forEach(l => { bySource[l.source] = (bySource[l.source] || 0) + 1; });
  Object.entries(bySource).sort((a, b) => b[1] - a[1]).forEach(([src, count]) => {
    log(`  ${src}: ${count}`);
  });

  // Country breakdown
  log('\nBy country:');
  const byCountry = {};
  deduped.forEach(l => {
    const c = l.country || 'Unknown';
    byCountry[c] = (byCountry[c] || 0) + 1;
  });
  Object.entries(byCountry).sort((a, b) => b[1] - a[1]).slice(0, 15).forEach(([c, count]) => {
    log(`  ${c}: ${count}`);
  });

  log(`\nOutput: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
