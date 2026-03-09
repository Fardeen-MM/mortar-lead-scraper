#!/usr/bin/env node
/**
 * Australia Extra Immigration Agents/Lawyers Scraper
 *
 * Scrapes Australian immigration lawyers and migration agents from multiple
 * directories beyond the MARA portal (which already has ~1,350 agents).
 *
 * Sources:
 *   1. Lawzana — city-level listings with JSON-LD ItemList, then firm profile
 *      pages with JSON-LD LegalService (phone, email, address)
 *   2. Best Lawyers AU — listing pages with card elements, then profile pages
 *      with JSON-LD Person (phone, website, firm, address)
 *   3. Best Law Firms AU — listing page with 33 firms, then profile pages
 *      with JSON-LD LegalService (phone, website, address)
 *
 * All sources use HTTP + Cheerio (no Puppeteer needed — all SSR).
 *
 * Usage:
 *   node scripts/scrape-au-extra-immigration.js --test    # 2 cities + 1 BL page
 *   node scripts/scrape-au-extra-immigration.js --all     # Full scrape
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
const OUT_FILE = path.join(OUT_DIR, 'au-immigration-extra.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── Lawzana city pages ──────────────────────────────────────────────────────

const LAWZANA_CITIES = [
  { city: 'Sydney', slug: 'sydney', state: 'NSW' },
  { city: 'Melbourne', slug: 'melbourne', state: 'VIC' },
  { city: 'Brisbane', slug: 'brisbane', state: 'QLD' },
  { city: 'Perth', slug: 'perth', state: 'WA' },
  { city: 'Adelaide', slug: 'adelaide', state: 'SA' },
  { city: 'Canberra', slug: 'canberra', state: 'ACT' },
  { city: 'Gold Coast', slug: 'gold-coast', state: 'QLD' },
  { city: 'Hobart', slug: 'hobart', state: 'TAS' },
  { city: 'Darwin', slug: 'darwin', state: 'NT' },
  { city: 'Cairns', slug: 'cairns', state: 'QLD' },
  { city: 'Newcastle', slug: 'newcastle', state: 'NSW' },
  { city: 'Wollongong', slug: 'wollongong', state: 'NSW' },
  { city: 'Parramatta', slug: 'parramatta', state: 'NSW' },
  { city: 'Townsville', slug: 'townsville', state: 'QLD' },
  { city: 'Geelong', slug: 'geelong', state: 'VIC' },
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

function cleanPhone(raw) {
  if (!raw) return '';
  // Strip everything except digits
  let digits = raw.replace(/\D/g, '');
  // Australian phone: +61 X XXXX XXXX -> strip country code
  // Handles: 61292226100 (11 digits), 610292226100 (12 digits with leading 0 after 61)
  if (digits.startsWith('61') && digits.length >= 11 && !digits.startsWith('6113') && !digits.startsWith('6118')) {
    const rest = digits.slice(2);
    digits = rest.startsWith('0') ? rest : '0' + rest;
  }
  // Handle country code + 1300/1800: 611300950771 -> 1300950771
  if (digits.startsWith('611') && digits.length === 12) {
    digits = digits.slice(2);
  }
  // Handle 1300/1800 numbers (10 digits starting with 1)
  if (digits.length === 10 && digits.startsWith('1')) {
    return `${digits.slice(0, 4)} ${digits.slice(4, 7)} ${digits.slice(7)}`;
  }
  // Standard AU landline/mobile (10 digits starting with 0)
  if (digits.length === 10 && digits.startsWith('0')) {
    return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)} ${digits.slice(6)}`;
  }
  // 9 digits without leading 0 (some formats drop the leading 0)
  if (digits.length === 9 && !digits.startsWith('0')) {
    digits = '0' + digits;
    return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)} ${digits.slice(6)}`;
  }
  // 11 digits starting with 0 (e.g., 04 mobile + extra digit)
  if (digits.length === 11 && digits.startsWith('0')) {
    return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)} ${digits.slice(6)}`;
  }
  // Fallback: return cleaned raw without + prefix
  return raw.replace(/^\+/, '').trim();
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/\s*(Esq\.?|QC|SC|AM|AO|KC)\s*/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  const parts = cleaned.split(' ');
  if (parts.length >= 2) {
    return {
      first_name: parts[0],
      last_name: parts.slice(1).join(' '),
    };
  }
  return { first_name: cleaned, last_name: '' };
}

function dedupKey(firmOrName, city) {
  return `${(firmOrName || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}`;
}

function decodeHtml(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/');
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-AU,en;q=0.9',
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

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 1: LAWZANA
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeLawzana(cities, seenKeys) {
  const leads = [];
  log('');
  log('=== SOURCE 1: Lawzana ===');
  log(`Cities to scrape: ${cities.length}`);

  for (let i = 0; i < cities.length; i++) {
    const cityInfo = cities[i];
    const listUrl = `https://lawzana.com/immigration-lawyers/${cityInfo.slug}`;
    log(`[Lawzana ${i + 1}/${cities.length}] ${cityInfo.city} -> ${listUrl}`);

    try {
      const { statusCode, body } = await httpGet(listUrl);
      if (statusCode !== 200 || !body) {
        log(`  HTTP ${statusCode} - skipping`);
        await randomDelay();
        continue;
      }

      // Parse JSON-LD ItemList to get firm profile URLs
      const $ = cheerio.load(body);
      const firmUrls = [];

      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          if (json['@type'] === 'ItemList' && json.itemListElement) {
            for (const item of json.itemListElement) {
              if (item.url && item.name) {
                firmUrls.push({ url: item.url, name: item.name });
              }
            }
          }
        } catch {}
      });

      log(`  Found ${firmUrls.length} firms in listing`);

      // Visit each firm profile page
      for (let j = 0; j < firmUrls.length; j++) {
        const firm = firmUrls[j];
        const key = dedupKey(firm.name, cityInfo.city);
        if (seenKeys.has(key)) {
          log(`  [${j + 1}/${firmUrls.length}] SKIP (dupe): ${firm.name}`);
          continue;
        }

        await randomDelay();
        log(`  [${j + 1}/${firmUrls.length}] ${firm.name}`);

        try {
          const { statusCode: pStatus, body: pBody } = await httpGet(firm.url);
          if (pStatus !== 200 || !pBody) {
            log(`    HTTP ${pStatus}`);
            continue;
          }

          const p$ = cheerio.load(pBody);
          let firmData = null;

          // Parse JSON-LD LegalService from profile page
          p$('script[type="application/ld+json"]').each((_, el) => {
            try {
              const json = JSON.parse(p$(el).html());
              if (json['@type'] === 'LegalService') {
                firmData = json;
              }
            } catch {}
          });

          if (!firmData) {
            log(`    No JSON-LD LegalService found`);
            continue;
          }

          const address = firmData.address || {};
          const phone = cleanPhone(firmData.telephone || '');
          const email = (firmData.email || '').trim();
          const firmName = decodeHtml(firmData.name || firm.name);
          const city = address.addressLocality || cityInfo.city;
          const state = mapAuState(address.addressRegion || '') || cityInfo.state;

          // Extract website from JSON-LD sameAs array (skip social media)
          const socialPatterns = ['facebook.com', 'twitter.com', 'linkedin.com',
            'instagram.com', 'youtube.com', 'tiktok.com', 'pinterest.com',
            'x.com', 'wa.me', 'lawzana.com', 'google.com'];

          let finalWebsite = '';
          const sameAs = firmData.sameAs;
          if (Array.isArray(sameAs)) {
            for (const s of sameAs) {
              if (typeof s !== 'string') continue;
              const url = s.startsWith('http') ? s : `https://${s}`;
              const isSocial = socialPatterns.some(p => url.toLowerCase().indexOf(p) > -1);
              if (!isSocial && s.indexOf('.') > -1) {
                finalWebsite = url;
                break;
              }
            }
          } else if (typeof sameAs === 'string') {
            const isSocial = socialPatterns.some(p => sameAs.toLowerCase().indexOf(p) > -1);
            if (!isSocial) finalWebsite = sameAs.startsWith('http') ? sameAs : `https://${sameAs}`;
          }

          seenKeys.add(key);
          leads.push({
            first_name: '',
            last_name: '',
            firm_name: firmName,
            title: '',
            email,
            phone,
            website: finalWebsite,
            domain: extractDomain(finalWebsite),
            city,
            state,
            country: 'AU',
            niche: 'immigration',
            source: 'lawzana',
            profile_url: firm.url,
          });

          log(`    -> ${firmName} | ${phone || 'no phone'} | ${email || 'no email'} | ${finalWebsite || 'no website'}`);

        } catch (err) {
          log(`    ERROR: ${err.message}`);
        }
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`Lawzana: ${leads.length} leads collected`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 2: BEST LAWYERS AU
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeBestLawyers(maxPages, seenKeys) {
  const leads = [];
  log('');
  log('=== SOURCE 2: Best Lawyers AU ===');

  const BASE = 'https://www.bestlawyers.com';
  const profileUrls = [];

  // Step 1: Collect all profile URLs from listing pages
  for (let page = 1; page <= maxPages; page++) {
    const url = `${BASE}/australia/immigration-law` + (page > 1 ? `?page=${page}` : '');
    log(`[BestLawyers listing ${page}/${maxPages}] ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`  HTTP ${statusCode} - stopping`);
        break;
      }

      const $ = cheerio.load(body);

      // Extract total results on first page
      if (page === 1) {
        const totalMatch = body.match(/(\d+)\s*Result/i);
        if (totalMatch) {
          const total = parseInt(totalMatch[1]);
          const totalPages = Math.ceil(total / 14); // ~12-14 per page
          if (totalPages < maxPages) maxPages = totalPages;
          log(`  Total results: ${total}, pages: ${maxPages}`);
        }
      }

      // Parse card-lawyer elements (only the basic info cards, not detail/bio cards)
      const cards = $('.card-lawyer');
      let pageProfiles = 0;
      const seenOnPage = new Set();

      cards.each((_, card) => {
        const el = $(card);
        const name = el.find('.card-title').text().trim();
        const firm = el.find('.firm-profile-link').text().trim();
        const profileLink = el.find('a[href*="/lawyers/"]').first().attr('href') || '';

        if (!name || !profileLink || seenOnPage.has(profileLink)) return;
        seenOnPage.add(profileLink);

        // Extract city from card text
        let city = '';
        el.find('.card-text').each((_, t) => {
          const txt = $(t).text().trim();
          if (txt.match(/,\s*AU$/)) {
            city = txt.replace(/,\s*AU$/, '').trim();
          }
        });

        const fullUrl = profileLink.startsWith('http') ? profileLink : `${BASE}${profileLink}`;
        profileUrls.push({ name, firm, city, profileUrl: fullUrl });
        pageProfiles++;
      });

      log(`  Found ${pageProfiles} unique profiles`);

      // Check for next page
      const hasNext = $('a[href*="page="]').length > 0;
      if (!hasNext && page < maxPages) {
        log(`  No more pages`);
        break;
      }

    } catch (err) {
      log(`  ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  // Deduplicate profile URLs
  const uniqueProfiles = [];
  const seenProfileUrls = new Set();
  for (const p of profileUrls) {
    if (!seenProfileUrls.has(p.profileUrl)) {
      seenProfileUrls.add(p.profileUrl);
      uniqueProfiles.push(p);
    }
  }

  log(`Collected ${uniqueProfiles.length} unique profiles to visit`);

  // Step 2: Visit each profile page for JSON-LD data
  for (let i = 0; i < uniqueProfiles.length; i++) {
    const profile = uniqueProfiles[i];
    const key = dedupKey(profile.name, profile.city);
    if (seenKeys.has(key)) {
      log(`  [${i + 1}/${uniqueProfiles.length}] SKIP (dupe): ${profile.name}`);
      continue;
    }

    await randomDelay();
    log(`  [${i + 1}/${uniqueProfiles.length}] ${profile.name} (${profile.firm})`);

    try {
      const { statusCode, body } = await httpGet(profile.profileUrl);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode}`);
        continue;
      }

      const $ = cheerio.load(body);
      let personData = null;

      $('script[type="application/ld+json"]').each((_, el) => {
        try {
          const json = JSON.parse($(el).html());
          if (json['@type'] === 'Person') {
            personData = json;
          }
        } catch {}
      });

      if (!personData) {
        log(`    No JSON-LD Person found`);
        // Fallback: use listing data
        const { first_name, last_name } = parseName(profile.name);
        seenKeys.add(key);
        leads.push({
          first_name,
          last_name,
          firm_name: profile.firm,
          title: '',
          email: '',
          phone: '',
          website: '',
          domain: '',
          city: profile.city,
          state: guessAuStateFromCity(profile.city),
          country: 'AU',
          niche: 'immigration',
          source: 'bestlawyers',
          profile_url: profile.profileUrl,
        });
        continue;
      }

      const { first_name, last_name } = parseName(personData.name || profile.name);
      const worksFor = personData.worksFor || {};
      const address = (worksFor.address || {});
      const phone = cleanPhone(personData.telephone || '');
      const firmName = worksFor.name || profile.firm;
      const city = address.addressLocality || profile.city;
      const state = mapAuState(address.addressRegion || '') || guessAuStateFromCity(city);

      // External website from sameAs
      let website = '';
      const sameAs = personData.sameAs;
      if (Array.isArray(sameAs)) {
        for (const s of sameAs) {
          if (typeof s === 'string' && s.startsWith('http') && s.indexOf('bestlawyers') === -1) {
            website = s;
            break;
          }
        }
      } else if (typeof sameAs === 'string' && sameAs.indexOf('bestlawyers') === -1) {
        website = sameAs;
      }

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
        city,
        state,
        country: 'AU',
        niche: 'immigration',
        source: 'bestlawyers',
        profile_url: profile.profileUrl,
      });

      log(`    -> ${first_name} ${last_name} @ ${firmName} | ${phone || 'no phone'} | ${website || 'no website'}`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }
  }

  log(`Best Lawyers AU: ${leads.length} leads collected`);
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 3: BEST LAW FIRMS AU
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeBestLawFirms(seenKeys) {
  const leads = [];
  log('');
  log('=== SOURCE 3: Best Law Firms AU ===');

  const BASE = 'https://www.bestlawfirms.com';
  const listUrl = `${BASE}/australia/immigration-law`;

  log(`Fetching listing: ${listUrl}`);

  try {
    const { statusCode, body } = await httpGet(listUrl);
    if (statusCode !== 200 || !body) {
      log(`HTTP ${statusCode} - failed`);
      return leads;
    }

    const $ = cheerio.load(body);

    // Extract firm profile URLs from listing
    const firmUrls = [];
    const seenHrefs = new Set();

    $('a[href*="/firms/"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const text = $(el).text().trim();
      // Only take the firm name links (not "View Profile" buttons)
      if (href && text && text !== 'View Profile' && text.length > 2 &&
          !seenHrefs.has(href) && href.indexOf('/AU') > -1) {
        seenHrefs.add(href);
        const fullUrl = href.startsWith('http') ? href : `${BASE}${href}`;
        firmUrls.push({ name: text, url: fullUrl });
      }
    });

    log(`Found ${firmUrls.length} firms to visit`);

    // Visit each firm profile page
    for (let i = 0; i < firmUrls.length; i++) {
      const firm = firmUrls[i];
      const key = dedupKey(firm.name, '');  // Firms don't always have city in listing
      if (seenKeys.has(key)) {
        log(`  [${i + 1}/${firmUrls.length}] SKIP (dupe): ${firm.name}`);
        continue;
      }

      await randomDelay();
      log(`  [${i + 1}/${firmUrls.length}] ${firm.name}`);

      try {
        const { statusCode: pStatus, body: pBody } = await httpGet(firm.url);
        if (pStatus !== 200 || !pBody) {
          log(`    HTTP ${pStatus}`);
          continue;
        }

        const p$ = cheerio.load(pBody);
        let firmData = null;

        p$('script[type="application/ld+json"]').each((_, el) => {
          try {
            const json = JSON.parse(p$(el).html());
            if (json['@type'] === 'LegalService') {
              firmData = json;
            }
          } catch {}
        });

        if (!firmData) {
          log(`    No JSON-LD LegalService found`);
          continue;
        }

        const address = firmData.address || {};
        const phone = cleanPhone(firmData.telephone || '');
        const firmName = decodeHtml(firmData.name || firm.name);
        const city = address.addressLocality || '';
        const state = mapAuState(address.addressRegion || '') || guessAuStateFromCity(city);

        // Get website from JSON-LD url field
        let website = firmData.url || '';
        if (website.indexOf('bestlaw') > -1) website = '';

        // Also check for external links
        if (!website) {
          p$('a[target="_blank"]').each((_, el) => {
            const href = p$(el).attr('href') || '';
            if (href.startsWith('http') &&
                href.indexOf('bestlaw') === -1 &&
                href.indexOf('google') === -1 &&
                href.indexOf('facebook') === -1 &&
                href.indexOf('linkedin') === -1 &&
                href.indexOf('twitter') === -1 &&
                href.indexOf('cdn.') === -1) {
              if (!website) website = href;
            }
          });
        }

        // Also get individual lawyer names from the firm page
        const lawyerNames = [];
        p$('a[href*="/lawyers/"]').each((_, el) => {
          const name = p$(el).text().trim();
          if (name && name.length < 50 && name.indexOf('Best') === -1) {
            lawyerNames.push(name);
          }
        });

        // Deduplicate by firm name + city
        const firmKey = dedupKey(firmName, city);
        if (seenKeys.has(firmKey)) {
          log(`    SKIP (dupe): ${firmName}, ${city}`);
          continue;
        }
        seenKeys.add(firmKey);
        seenKeys.add(key);

        // Add firm lead
        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          title: '',
          email: '',
          phone,
          website,
          domain: extractDomain(website),
          city,
          state,
          country: 'AU',
          niche: 'immigration',
          source: 'bestlawfirms',
          profile_url: firm.url,
        });

        log(`    -> ${firmName} | ${city} | ${phone || 'no phone'} | ${website || 'no website'}`);

        // Add individual lawyers (deduped against Best Lawyers source)
        for (const lawyerName of lawyerNames) {
          const lKey = dedupKey(lawyerName, city);
          if (seenKeys.has(lKey)) continue;
          seenKeys.add(lKey);

          const { first_name, last_name } = parseName(lawyerName);
          leads.push({
            first_name,
            last_name,
            firm_name: firmName,
            title: '',
            email: '',
            phone,
            website,
            domain: extractDomain(website),
            city,
            state,
            country: 'AU',
            niche: 'immigration',
            source: 'bestlawfirms',
            profile_url: firm.url,
          });
        }

      } catch (err) {
        log(`    ERROR: ${err.message}`);
      }
    }

  } catch (err) {
    log(`ERROR: ${err.message}`);
  }

  log(`Best Law Firms AU: ${leads.length} leads collected`);
  return leads;
}

// ── State Mapping Helpers ────────────────────────────────────────────────────

function mapAuState(region) {
  if (!region) return '';
  const r = region.toLowerCase().trim();
  const map = {
    'new south wales': 'NSW',
    'nsw': 'NSW',
    'victoria': 'VIC',
    'vic': 'VIC',
    'queensland': 'QLD',
    'qld': 'QLD',
    'western australia': 'WA',
    'wa': 'WA',
    'south australia': 'SA',
    'sa': 'SA',
    'tasmania': 'TAS',
    'tas': 'TAS',
    'australian capital territory': 'ACT',
    'act': 'ACT',
    'northern territory': 'NT',
    'nt': 'NT',
  };
  return map[r] || region.toUpperCase();
}

function guessAuStateFromCity(city) {
  if (!city) return '';
  const c = city.toLowerCase().trim();
  const cityStateMap = {
    'sydney': 'NSW', 'parramatta': 'NSW', 'newcastle': 'NSW', 'wollongong': 'NSW',
    'north sydney': 'NSW', 'bondi': 'NSW', 'chatswood': 'NSW',
    'melbourne': 'VIC', 'geelong': 'VIC', 'richmond': 'VIC', 'north melbourne': 'VIC',
    'brisbane': 'QLD', 'gold coast': 'QLD', 'cairns': 'QLD', 'townsville': 'QLD',
    'fortitude valley': 'QLD',
    'perth': 'WA', 'fremantle': 'WA', 'scarborough': 'WA',
    'adelaide': 'SA',
    'hobart': 'TAS',
    'darwin': 'NT',
    'canberra': 'ACT',
  };
  return cityStateMap[c] || '';
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-au-extra-immigration.js --test    # Quick test (2 Lawzana cities + 1 BL page)');
    console.log('  node scripts/scrape-au-extra-immigration.js --all     # Full scrape all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  const seenKeys = new Set();
  let allLeads = [];

  log('AU Extra Immigration Agents/Lawyers Scraper');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  // Source 1: Lawzana
  const lawzanaCities = isTest ? LAWZANA_CITIES.slice(0, 2) : LAWZANA_CITIES;
  const lawzanaLeads = await scrapeLawzana(lawzanaCities, seenKeys);
  allLeads = allLeads.concat(lawzanaLeads);

  // Intermediate save
  if (allLeads.length > 0) {
    writeCSV(allLeads, OUT_FILE);
    log(`[Progress saved: ${allLeads.length} leads]`);
  }

  // Source 2: Best Lawyers AU
  const blMaxPages = isTest ? 1 : 10; // 5 pages exist, 10 is safety cap
  const blLeads = await scrapeBestLawyers(blMaxPages, seenKeys);
  allLeads = allLeads.concat(blLeads);

  // Intermediate save
  if (allLeads.length > 0) {
    writeCSV(allLeads, OUT_FILE);
    log(`[Progress saved: ${allLeads.length} leads]`);
  }

  // Source 3: Best Law Firms AU
  const blfLeads = await scrapeBestLawFirms(seenKeys);
  allLeads = allLeads.concat(blfLeads);

  // Final write
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const uniqueCities = new Set(allLeads.map(l => l.city)).size;
  const uniqueStates = new Set(allLeads.map(l => l.state)).size;
  const bySrc = {};
  for (const l of allLeads) {
    bySrc[l.source] = (bySrc[l.source] || 0) + 1;
  }

  log('');
  log('='.repeat(60));
  log('DONE');
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone: ${withPhone}`);
  log(`With email: ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With parsed name: ${withName}`);
  log(`Unique cities: ${uniqueCities}`);
  log(`Unique states: ${uniqueStates}`);
  log(`By source:`);
  for (const [src, count] of Object.entries(bySrc)) {
    log(`  ${src}: ${count}`);
  }
  log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
