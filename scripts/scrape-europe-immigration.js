#!/usr/bin/env node
/**
 * European Immigration Lawyers / Consultants Scraper
 *
 * Multi-source scraper covering Germany, France, Spain, Netherlands, Italy,
 * Portugal, Belgium, Switzerland, Poland, and more EU countries.
 *
 * Sources:
 *   1. Expat.com       — Immigration lawyers across 12+ EU countries
 *   2. Alexia.fr       — French immigration avocats by city (barreau IDs)
 *   3. IAmExpat.de     — Germany immigration lawyers
 *   4. IAmExpat.nl     — Netherlands immigration lawyers
 *   5. Expatica.com    — Immigration lawyers in DE/ES/NL/PT
 *
 * Usage:
 *   node scripts/scrape-europe-immigration.js --test                # Test mode (2 countries, 1 page each)
 *   node scripts/scrape-europe-immigration.js --all                 # Full scrape (all sources, all countries)
 *   node scripts/scrape-europe-immigration.js --country DE          # Germany only
 *   node scripts/scrape-europe-immigration.js --country FR          # France only
 *   node scripts/scrape-europe-immigration.js --country ES          # Spain only
 *   node scripts/scrape-europe-immigration.js --country NL          # Netherlands only
 *   node scripts/scrape-europe-immigration.js --country ALL         # All countries (same as --all)
 *   node scripts/scrape-europe-immigration.js --source expat        # Expat.com only
 *   node scripts/scrape-europe-immigration.js --source alexia       # Alexia.fr only
 *   node scripts/scrape-europe-immigration.js --source iamexpat-de  # IAmExpat.de only
 *   node scripts/scrape-europe-immigration.js --source iamexpat-nl  # IAmExpat.nl only
 *   node scripts/scrape-europe-immigration.js --source expat-cities  # Expat.com city pages only
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'europe-immigration-consultants.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, '.europe-immigration-progress.json');
const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
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

// ── Country → Code mapping ──────────────────────────────────────────────────

const COUNTRY_NAMES = {
  DE: 'Germany',
  FR: 'France',
  ES: 'Spain',
  NL: 'Netherlands',
  IT: 'Italy',
  PT: 'Portugal',
  BE: 'Belgium',
  CH: 'Switzerland',
  PL: 'Poland',
  AT: 'Austria',
  SE: 'Sweden',
  DK: 'Denmark',
  CZ: 'Czech Republic',
  GR: 'Greece',
  HU: 'Hungary',
  RO: 'Romania',
  IE: 'Ireland',
  FI: 'Finland',
  NO: 'Norway',
  LU: 'Luxembourg',
};

// ── Expat.com country slugs ─────────────────────────────────────────────────

const EXPAT_COM_COUNTRIES = {
  DE: 'germany',
  FR: 'france',
  ES: 'spain',
  NL: 'netherlands',
  IT: 'italy',
  PT: 'portugal',
  BE: 'belgium',
  CH: 'switzerland',
  PL: 'poland',
  AT: 'austria',
  SE: 'sweden',
  DK: 'denmark',
  CZ: 'czech-republic',
  GR: 'greece',
  HU: 'hungary',
  RO: 'romania',
  FI: 'finland',
  NO: 'norway',
  LU: 'luxembourg',
};

// ── Expatica country slugs ──────────────────────────────────────────────────

const EXPATICA_COUNTRIES = {
  DE: 'de',
  ES: 'es',
  NL: 'nl',
  PT: 'pt',
  BE: 'be',
  FR: 'fr',
  LU: 'lu',
  AT: 'at',
};

// ── Alexia.fr barreau IDs for French cities ─────────────────────────────────

const ALEXIA_CITIES = [
  { city: 'Paris', barreauId: 2 },
  { city: 'Lyon', barreauId: 46 },
  { city: 'Marseille', barreauId: 48 },
  { city: 'Bordeaux', barreauId: 23 },
  { city: 'Toulouse', barreauId: 86 },
  { city: 'Nantes', barreauId: 55 },
  { city: 'Strasbourg', barreauId: 82 },
  { city: 'Nice', barreauId: 56 },
  { city: 'Lille', barreauId: 44 },
  { city: 'Montpellier', barreauId: 52 },
  { city: 'Rennes', barreauId: 72 },
  { city: 'Grenoble', barreauId: 35 },
  { city: 'Rouen', barreauId: 74 },
  { city: 'Toulon', barreauId: 87 },
  { city: 'Dijon', barreauId: 28 },
  { city: 'Angers', barreauId: 4 },
  { city: 'Reims', barreauId: 70 },
  { city: 'Tours', barreauId: 88 },
  { city: 'Clermont-Ferrand', barreauId: 26 },
  { city: 'Metz', barreauId: 50 },
];

// ── IAmExpat.de German city pages ───────────────────────────────────────────

const IAMEXPAT_DE_CITIES = [
  { city: 'Berlin', slug: 'berlin' },
  { city: 'Munich', slug: 'munich' },
  { city: 'Frankfurt', slug: 'frankfurt' },
  { city: 'Hamburg', slug: 'hamburg' },
  { city: 'Cologne', slug: 'cologne' },
  { city: 'Dusseldorf', slug: 'dusseldorf' },
  { city: 'Stuttgart', slug: 'stuttgart' },
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }
function randomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }

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
    const u = new URL(url.startsWith('http') ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function cleanPhone(phone) {
  if (!phone) return '';
  return phone.replace(/\s+/g, ' ').trim();
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/\b(Me|Maître|Avv\.|Dr\.|Prof\.|RA|Rechtsanwalt|Rechtsanwältin|Mr\.?|Mrs\.?|Ms\.?|Mme\.?|Abogad[oa]|Advocaat)\b\.?/gi, '')
    .replace(/\s+/g, ' ')
    .trim();

  const parts = cleaned.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function dedupKey(lead) {
  // For leads with firm_name, dedup by firm+city+country
  // For leads without firm_name (individual lawyers), dedup by name+city+country
  // Also dedup by profile_url if available
  const name = lead.firm_name
    || `${lead.first_name} ${lead.last_name}`.trim()
    || lead.profile_url
    || '';
  return `${name.toLowerCase().trim()}|${(lead.city || '').toLowerCase().trim()}|${(lead.country || '').toLowerCase().trim()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const options = {
      headers: {
        'User-Agent': randomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,de;q=0.8,fr;q=0.7,es;q=0.6,nl;q=0.5',
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
      if (res.statusCode === 404) return resolve({ statusCode: 404, body: '' });
      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }
      if (res.statusCode !== 200) return resolve({ statusCode: res.statusCode, body: '' });

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else reject(err);
    });
    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1)).then(resolve).catch(reject);
      } else reject(new Error('Request timed out'));
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 1: Expat.com — Immigration lawyers across EU countries
// URL: https://www.expat.com/en/business/europe/{country}/8_legal-services/58_immigration-lawyers/
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeExpatCom(countryCode, countrySlug) {
  const countryName = COUNTRY_NAMES[countryCode] || countryCode;
  const url = `https://www.expat.com/en/business/europe/${countrySlug}/8_legal-services/58_immigration-lawyers/`;
  log(`[Expat.com] ${countryName} -> ${url}`);

  const leads = [];
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`  HTTP ${statusCode} — skipping`);
      return leads;
    }

    const $ = cheerio.load(body);

    // Each listing is a .default-card or similar container
    // Parse business entries from the listing
    $('.default-card, .business-card, .listing-item, .result-item, article').each((_, el) => {
      const $el = $(el);
      const text = $el.text();
      // Skip nav/header elements
      if (text.length < 20) return;

      // Try to extract firm name from title/heading
      const firmName = (
        $el.find('.default-card--title, .business-card--title, h2, h3, .title').first().text().trim() ||
        $el.find('a[href*="/business/"]').first().text().trim()
      );
      if (!firmName) return;

      // Extract contact person
      const contactText = $el.find('.default-card--misc, .contact, .person').text();

      // Extract phone from tel: links or text
      let phone = '';
      const telLink = $el.find('a[href^="tel:"]').first();
      if (telLink.length) {
        phone = telLink.attr('href').replace('tel:', '').trim();
      }
      if (!phone) {
        // Try to find phone in text
        const phoneMatch = text.match(/(\+?\d[\d\s\-()]{7,})/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // Extract city from address
      let city = '';
      const addressText = $el.find('.address, .location, .default-card--address').text().trim();
      if (addressText) {
        // Usually last significant part of address is city/region
        const parts = addressText.split(',').map(p => p.trim()).filter(Boolean);
        if (parts.length >= 1) {
          city = parts[parts.length - 1].replace(/\d{4,}/g, '').trim();
          if (!city && parts.length >= 2) city = parts[parts.length - 2].replace(/\d{4,}/g, '').trim();
        }
      }

      // Extract website
      let website = '';
      const webLink = $el.find('a[href^="http"]:not([href*="expat.com"])').first();
      if (webLink.length) {
        website = webLink.attr('href');
      }

      // Extract email
      let email = '';
      const mailLink = $el.find('a[href^="mailto:"]').first();
      if (mailLink.length) {
        email = mailLink.attr('href').replace('mailto:', '').trim();
      }

      // Parse the contact person name
      let personName = '';
      const contactMatch = contactText.match(/(?:Contact|By|Rep)[:\s]*([A-Z][a-zÀ-ÿ]+(?:\s+[A-Z][a-zÀ-ÿ]+)+)/i);
      if (contactMatch) personName = contactMatch[1].trim();

      const { first_name, last_name } = parseName(personName);

      leads.push({
        first_name,
        last_name,
        firm_name: firmName,
        title: '',
        email,
        phone: cleanPhone(phone),
        website,
        domain: extractDomain(website),
        city: city || '',
        state: '',
        country: countryCode,
        niche: 'immigration',
        source: 'expat.com',
        profile_url: url,
      });
    });

    // Alternative parsing: look for structured data / JSON-LD
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const json = JSON.parse($(el).html());
        const items = Array.isArray(json) ? json : (json['@graph'] || [json]);
        for (const item of items) {
          if (!item.name || item['@type'] === 'WebPage') continue;
          if (item['@type'] === 'LocalBusiness' || item['@type'] === 'LegalService' || item['@type'] === 'Attorney') {
            const addr = item.address || {};
            leads.push({
              first_name: '',
              last_name: '',
              firm_name: item.name,
              title: '',
              email: item.email || '',
              phone: cleanPhone(item.telephone || ''),
              website: item.url || '',
              domain: extractDomain(item.url || ''),
              city: addr.addressLocality || '',
              state: addr.addressRegion || '',
              country: countryCode,
              niche: 'immigration',
              source: 'expat.com',
              profile_url: url,
            });
          }
        }
      } catch {}
    });

    // Fallback: parse all tel: links on the page as potential business entries
    if (leads.length === 0) {
      const telLinks = $('a[href^="tel:"]');
      telLinks.each((_, el) => {
        const $tel = $(el);
        const phone = $tel.attr('href').replace('tel:', '').trim();
        // Walk up to find the parent card/container
        const $parent = $tel.closest('div, li, article, section').first();
        if (!$parent.length) return;

        // Get all text from parent
        const parentText = $parent.text().trim();
        // Try to find a heading/title
        const heading = $parent.find('h1, h2, h3, h4, h5, a').first().text().trim();
        if (!heading || heading.length < 3) return;

        // Skip if heading is just a phone number
        if (/^\+?\d[\d\s\-()]+$/.test(heading)) return;

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: heading,
          title: '',
          email: '',
          phone: cleanPhone(phone),
          website: '',
          domain: '',
          city: '',
          state: '',
          country: countryCode,
          niche: 'immigration',
          source: 'expat.com',
          profile_url: url,
        });
      });
    }

    log(`  Found ${leads.length} listings`);
  } catch (err) {
    log(`  ERROR: ${err.message}`);
  }
  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 2: Alexia.fr — French immigration avocats by city
// URL: https://www.alexia.fr/barreau-{id}-activite-3505/avocat-en-droit-des-etrangers-{city}.htm
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeAlexiaFr(cities, isTest) {
  const citiesToScrape = isTest ? cities.slice(0, 3) : cities;
  const leads = [];
  log(`[Alexia.fr] Scraping ${citiesToScrape.length} French cities`);

  for (const { city, barreauId } of citiesToScrape) {
    const citySlug = city.toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')  // Remove accents
      .replace(/\s+/g, '-')
      .replace(/[^a-z0-9-]/g, '');
    const url = `https://www.alexia.fr/barreau-${barreauId}-activite-3505/avocat-en-droit-des-etrangers-${citySlug}.htm`;
    log(`  ${city} -> ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode}`);
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);

      // Alexia.fr uses ._nnd_block_fiche for each lawyer card
      // Lawyer profile links match: /avocat-{id}/{name-slug}.htm
      // Each lawyer has 2 links with same URL - pricing link + name link
      // Strategy: extract unique URLs, parse name from URL slug
      const seenUrls = new Set();

      // Collect all unique /avocat-{digits}/ URLs
      $('a[href*="/avocat-"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        // Must match pattern /avocat-{digits}/{name-slug}.htm
        const urlMatch = href.match(/\/avocat-(\d+)\/([^.]+)\.htm/);
        if (!urlMatch) return;

        const fullUrl = href.startsWith('http') ? href : `https://www.alexia.fr${href}`;
        if (seenUrls.has(fullUrl)) return;
        seenUrls.add(fullUrl);

        // Extract name from URL slug: "emmanuel-fotso" -> "Emmanuel Fotso"
        const nameSlug = urlMatch[2];
        const nameParts = nameSlug.split('-').map(p =>
          p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()
        );

        let first_name = '';
        let last_name = '';
        if (nameParts.length >= 2) {
          first_name = nameParts[0];
          last_name = nameParts.slice(1).join(' ');
        } else if (nameParts.length === 1) {
          first_name = nameParts[0];
        }

        // Also try to get name from the second link text (the one with "Maitre")
        // by checking all links with same href for name content
        let textName = '';
        $(`a[href="${href}"]`).each((_, a2) => {
          const t = $(a2).text().trim();
          // Look for "Maitre" or "Ma\xEEtre" prefix
          const mMatch = t.match(/Ma.tre\s+([^\n]+?)(?:Avocat|Intervient|$)/i);
          if (mMatch && !textName) {
            textName = mMatch[1].trim();
          }
        });

        // If we got a text name, try to parse it (it has UPPERCASE last names)
        if (textName && textName.length >= 3 && textName.length <= 60) {
          const tParts = textName.split(/\s+/);
          if (tParts.length >= 2) {
            const capsIdx = tParts.findIndex(p => p === p.toUpperCase() && p.length > 1);
            if (capsIdx > 0) {
              first_name = tParts.slice(0, capsIdx).join(' ');
              last_name = tParts.slice(capsIdx).map(w =>
                w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
              ).join(' ');
            }
          }
        }

        // Try to get phone from parent fiche card
        let phone = '';
        const $parentCards = $(`a[href="${href}"]`).closest('._nnd_block_fiche, ._nnd_block_fiche_content');
        if ($parentCards.length) {
          const telLink = $parentCards.find('a[href^="tel:"]').first();
          if (telLink.length) phone = telLink.attr('href').replace('tel:', '');
        }

        leads.push({
          first_name: first_name.trim(),
          last_name: last_name.trim(),
          firm_name: '',
          title: 'Avocat',
          email: '',
          phone: cleanPhone(phone),
          website: '',
          domain: '',
          city,
          state: '',
          country: 'FR',
          niche: 'immigration',
          source: 'alexia.fr',
          profile_url: fullUrl,
        });
      });

      const cityLeads = leads.filter(l => l.source === 'alexia.fr' && l.city === city).length;
      log(`    Found ${cityLeads} avocats`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 3: IAmExpat.de — German immigration lawyers by city
// URL: https://www.iamexpat.de/expat-info/legal-advice/lawyers-germany/{city}/immigration-law
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeIAmExpatDe(cities, isTest) {
  const citiesToScrape = isTest ? cities.slice(0, 2) : cities;
  const leads = [];
  log(`[IAmExpat.de] Scraping ${citiesToScrape.length} German cities`);

  // First scrape the main page (all cities)
  const mainUrl = 'https://www.iamexpat.de/expat-info/legal-advice/lawyers-germany/immigration-law';
  const urls = [mainUrl, ...citiesToScrape.map(c =>
    `https://www.iamexpat.de/expat-info/legal-advice/lawyers-germany/${c.slug}/immigration-law`
  )];

  for (const url of urls) {
    log(`  -> ${url}`);
    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode}`);
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);

      // IAmExpat uses cards for each firm listing
      // Look for firm entries in various container classes
      $('.view-content .views-row, .lawyer-item, .listing-item, .node--type-lawyer, article, .card, .partner-item, .teaser').each((_, el) => {
        const $el = $(el);
        const text = $el.text().trim();
        if (text.length < 10) return;

        const firmName = (
          $el.find('h2, h3, .field--name-title, .title, .card-title').first().text().trim() ||
          $el.find('a').first().text().trim()
        );
        if (!firmName || firmName.length < 3) return;

        let phone = '';
        const telLink = $el.find('a[href^="tel:"]').first();
        if (telLink.length) phone = telLink.attr('href').replace('tel:', '');
        if (!phone) {
          const phoneMatch = text.match(/(\+49[\d\s\-()]{7,}|\b0\d{2,4}[\s\-]?\d{3,}[\s\-]?\d{2,})/);
          if (phoneMatch) phone = phoneMatch[1];
        }

        let website = '';
        $el.find('a[href^="http"]').each((_, a) => {
          const href = $(a).attr('href') || '';
          if (!href.includes('iamexpat.de') && !href.includes('facebook') && !href.includes('linkedin')) {
            if (!website) website = href;
          }
        });

        let email = '';
        const mailLink = $el.find('a[href^="mailto:"]').first();
        if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '');

        let profileUrl = '';
        const detailLink = $el.find('a[href*="/expat-info/"]').first();
        if (detailLink.length) {
          profileUrl = detailLink.attr('href');
          if (profileUrl && !profileUrl.startsWith('http')) {
            profileUrl = `https://www.iamexpat.de${profileUrl}`;
          }
        }

        // Try to extract city from the URL or text
        let city = '';
        const urlCityMatch = url.match(/lawyers-germany\/([^/]+)/);
        if (urlCityMatch && urlCityMatch[1] !== 'immigration-law') {
          city = urlCityMatch[1].charAt(0).toUpperCase() + urlCityMatch[1].slice(1);
        }
        // Also look in address text
        const addressText = $el.find('.field--name-field-address, .address, .location').text().trim();
        if (addressText && !city) {
          const cityGuess = addressText.split(',').pop().trim().replace(/\d{4,}/g, '').trim();
          if (cityGuess) city = cityGuess;
        }

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          title: '',
          email,
          phone: cleanPhone(phone),
          website,
          domain: extractDomain(website),
          city,
          state: '',
          country: 'DE',
          niche: 'immigration',
          source: 'iamexpat.de',
          profile_url: profileUrl || url,
        });
      });

      // Fallback: look for sponsored/featured listings with links
      if (leads.filter(l => l.source === 'iamexpat.de').length === 0) {
        $('a[href*="/expat-info/legal-advice/"]').each((_, el) => {
          const $a = $(el);
          const name = $a.text().trim();
          if (!name || name.length < 4 || name.length > 80) return;
          if (/lawyers|legal|immigration|advice|find|search|home|back/i.test(name)) return;

          let profileUrl = $a.attr('href') || '';
          if (profileUrl && !profileUrl.startsWith('http')) {
            profileUrl = `https://www.iamexpat.de${profileUrl}`;
          }

          leads.push({
            first_name: '',
            last_name: '',
            firm_name: name,
            title: '',
            email: '',
            phone: '',
            website: '',
            domain: '',
            city: '',
            state: '',
            country: 'DE',
            niche: 'immigration',
            source: 'iamexpat.de',
            profile_url: profileUrl,
          });
        });
      }

      const pageLeads = leads.filter(l => l.source === 'iamexpat.de').length;
      log(`    Total so far: ${pageLeads} firms`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 4: IAmExpat.nl — Netherlands immigration lawyers
// URL: https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/immigration-law
// ═══════════════════════════════════════════════════════════════════════════════

async function scrapeIAmExpatNl(isTest) {
  const leads = [];
  const urls = [
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/immigration-law',
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/amsterdam/immigration-law',
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/the-hague/immigration-law',
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/rotterdam/immigration-law',
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/utrecht/immigration-law',
    'https://www.iamexpat.nl/expat-info/legal-advice/lawyers-netherlands/eindhoven/immigration-law',
  ];

  const pagesToScrape = isTest ? urls.slice(0, 2) : urls;
  log(`[IAmExpat.nl] Scraping ${pagesToScrape.length} Netherlands pages`);

  for (const url of pagesToScrape) {
    log(`  -> ${url}`);
    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode}`);
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);

      // Same structure as IAmExpat.de
      $('.view-content .views-row, .lawyer-item, .listing-item, article, .card, .partner-item, .teaser').each((_, el) => {
        const $el = $(el);
        const text = $el.text().trim();
        if (text.length < 10) return;

        const firmName = (
          $el.find('h2, h3, .field--name-title, .title, .card-title').first().text().trim() ||
          $el.find('a').first().text().trim()
        );
        if (!firmName || firmName.length < 3) return;

        let phone = '';
        const telLink = $el.find('a[href^="tel:"]').first();
        if (telLink.length) phone = telLink.attr('href').replace('tel:', '');
        if (!phone) {
          const phoneMatch = text.match(/(\+31[\d\s\-()]{7,}|\b0\d{2,3}[\s\-]?\d{3,}[\s\-]?\d{2,})/);
          if (phoneMatch) phone = phoneMatch[1];
        }

        let website = '';
        $el.find('a[href^="http"]').each((_, a) => {
          const href = $(a).attr('href') || '';
          if (!href.includes('iamexpat.nl') && !href.includes('facebook') && !href.includes('linkedin')) {
            if (!website) website = href;
          }
        });

        let email = '';
        const mailLink = $el.find('a[href^="mailto:"]').first();
        if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '');

        let city = '';
        const urlCityMatch = url.match(/lawyers-netherlands\/([^/]+)/);
        if (urlCityMatch && urlCityMatch[1] !== 'immigration-law') {
          const rawCity = urlCityMatch[1].replace(/-/g, ' ');
          city = rawCity.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        }

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: firmName,
          title: '',
          email,
          phone: cleanPhone(phone),
          website,
          domain: extractDomain(website),
          city,
          state: '',
          country: 'NL',
          niche: 'immigration',
          source: 'iamexpat.nl',
          profile_url: url,
        });
      });

      // Fallback: links to firm pages
      if (leads.filter(l => l.source === 'iamexpat.nl').length === 0) {
        $('a[href*="/expat-info/legal-advice/"]').each((_, el) => {
          const $a = $(el);
          const name = $a.text().trim();
          if (!name || name.length < 4 || name.length > 80) return;
          if (/lawyers|legal|immigration|advice|find|search|home|back|netherlands/i.test(name)) return;

          let profileUrl = $a.attr('href') || '';
          if (profileUrl && !profileUrl.startsWith('http')) {
            profileUrl = `https://www.iamexpat.nl${profileUrl}`;
          }

          leads.push({
            first_name: '',
            last_name: '',
            firm_name: name,
            title: '',
            email: '',
            phone: '',
            website: '',
            domain: '',
            city: '',
            state: '',
            country: 'NL',
            niche: 'immigration',
            source: 'iamexpat.nl',
            profile_url: profileUrl,
          });
        });
      }

      log(`    Total so far: ${leads.filter(l => l.source === 'iamexpat.nl').length} firms`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 5: Expat.com city-level pages for key countries
// URL: https://www.expat.com/en/business/europe/{country}/{city}/8_legal-services/58_immigration-lawyers/
// ═══════════════════════════════════════════════════════════════════════════════

const EXPAT_COM_CITY_PAGES = {
  DE: [
    { city: 'Berlin', slug: 'berlin' },
    { city: 'Munich', slug: 'munich' },
    { city: 'Frankfurt', slug: 'frankfurt' },
    { city: 'Hamburg', slug: 'hamburg' },
    { city: 'Cologne', slug: 'cologne' },
    { city: 'Dusseldorf', slug: 'dusseldorf' },
    { city: 'Stuttgart', slug: 'stuttgart' },
  ],
  ES: [
    { city: 'Madrid', slug: 'madrid' },
    { city: 'Barcelona', slug: 'barcelona' },
    { city: 'Valencia', slug: 'valencia' },
    { city: 'Malaga', slug: 'malaga' },
    { city: 'Alicante', slug: 'alicante' },
    { city: 'Seville', slug: 'seville' },
  ],
  FR: [
    { city: 'Paris', slug: 'paris' },
    { city: 'Lyon', slug: 'lyon' },
    { city: 'Marseille', slug: 'marseille' },
    { city: 'Nice', slug: 'nice' },
    { city: 'Toulouse', slug: 'toulouse' },
    { city: 'Bordeaux', slug: 'bordeaux' },
  ],
  NL: [
    { city: 'Amsterdam', slug: 'amsterdam' },
    { city: 'The Hague', slug: 'the-hague' },
    { city: 'Rotterdam', slug: 'rotterdam' },
    { city: 'Utrecht', slug: 'utrecht' },
  ],
  PT: [
    { city: 'Lisbon', slug: 'lisbon' },
    { city: 'Porto', slug: 'porto' },
    { city: 'Algarve', slug: 'algarve' },
  ],
  IT: [
    { city: 'Rome', slug: 'rome' },
    { city: 'Milan', slug: 'milan' },
    { city: 'Florence', slug: 'florence' },
  ],
  BE: [
    { city: 'Brussels', slug: 'brussels' },
    { city: 'Antwerp', slug: 'antwerp' },
  ],
  CH: [
    { city: 'Zurich', slug: 'zurich' },
    { city: 'Geneva', slug: 'geneva' },
    { city: 'Basel', slug: 'basel' },
  ],
};

async function scrapeExpatComCities(countryCode, isTest) {
  const countrySlug = EXPAT_COM_COUNTRIES[countryCode];
  if (!countrySlug) return [];
  const cities = EXPAT_COM_CITY_PAGES[countryCode] || [];
  if (cities.length === 0) return [];
  const countryName = COUNTRY_NAMES[countryCode] || countryCode;

  const citiesToScrape = isTest ? cities.slice(0, 2) : cities;
  const leads = [];
  log(`[Expat.com Cities] ${countryName}: ${citiesToScrape.length} city pages`);

  for (const { city, slug } of citiesToScrape) {
    const url = `https://www.expat.com/en/business/europe/${countrySlug}/${slug}/8_legal-services/58_immigration-lawyers/`;
    log(`  ${city} -> ${url}`);

    try {
      const { statusCode, body } = await httpGet(url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode}`);
        await randomDelay();
        continue;
      }

      const $ = cheerio.load(body);
      let cityLeads = 0;

      // Same parsing logic as main expat.com but with city context
      $('a[href^="tel:"]').each((_, el) => {
        const $tel = $(el);
        const phone = $tel.attr('href').replace('tel:', '').trim();
        const $parent = $tel.closest('div, li, article, section').first();
        if (!$parent.length) return;

        const heading = (
          $parent.find('h1, h2, h3, h4, h5').first().text().trim() ||
          $parent.find('a').first().text().trim()
        );
        if (!heading || heading.length < 3 || /^\+?\d/.test(heading)) return;

        leads.push({
          first_name: '',
          last_name: '',
          firm_name: heading,
          title: '',
          email: '',
          phone: cleanPhone(phone),
          website: '',
          domain: '',
          city,
          state: '',
          country: countryCode,
          niche: 'immigration',
          source: 'expat.com',
          profile_url: url,
        });
        cityLeads++;
      });

      log(`    Found ${cityLeads} listings`);
    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  return leads;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Progress / Resume
// ═══════════════════════════════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all') || args.includes('--country') && args[args.indexOf('--country') + 1]?.toUpperCase() === 'ALL';
  const isResume = args.includes('--resume');

  // Parse --country flag
  let countryFilter = null;
  const countryIdx = args.indexOf('--country');
  if (countryIdx !== -1 && args[countryIdx + 1]) {
    countryFilter = args[countryIdx + 1].toUpperCase();
    if (countryFilter === 'ALL') countryFilter = null;
  }

  // Parse --source flag
  let sourceFilter = null;
  const sourceIdx = args.indexOf('--source');
  if (sourceIdx !== -1 && args[sourceIdx + 1]) {
    sourceFilter = args[sourceIdx + 1].toLowerCase();
  }

  if (!isTest && !isAll && !countryFilter && !sourceFilter && !isResume) {
    console.log('Usage:');
    console.log('  node scripts/scrape-europe-immigration.js --test                # Test mode');
    console.log('  node scripts/scrape-europe-immigration.js --all                 # Full scrape');
    console.log('  node scripts/scrape-europe-immigration.js --country DE          # Germany only');
    console.log('  node scripts/scrape-europe-immigration.js --country FR          # France only');
    console.log('  node scripts/scrape-europe-immigration.js --country ES          # Spain only');
    console.log('  node scripts/scrape-europe-immigration.js --country NL          # Netherlands only');
    console.log('  node scripts/scrape-europe-immigration.js --country ALL         # All countries');
    console.log('  node scripts/scrape-europe-immigration.js --source expat        # Expat.com only');
    console.log('  node scripts/scrape-europe-immigration.js --source alexia       # Alexia.fr only');
    console.log('  node scripts/scrape-europe-immigration.js --source iamexpat-de  # IAmExpat.de only');
    console.log('  node scripts/scrape-europe-immigration.js --source iamexpat-nl  # IAmExpat.nl only');
    console.log('  node scripts/scrape-europe-immigration.js --source expat-cities  # Expat.com city pages only');
    process.exit(0);
  }

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const allLeads = [];
  const seenKeys = new Set();
  const completedSources = [];

  // Resume support
  if (isResume) {
    const progress = loadProgress();
    if (progress.leads.length > 0) {
      allLeads.push(...progress.leads);
      completedSources.push(...progress.completedSources);
      for (const lead of progress.leads) {
        seenKeys.add(dedupKey(lead));
      }
      log(`Resumed: ${allLeads.length} leads from ${completedSources.length} completed sources`);
    }
  }

  function addLeads(leads) {
    let added = 0;
    for (const lead of leads) {
      const key = dedupKey(lead);
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      allLeads.push(lead);
      added++;
    }
    return added;
  }

  log('');
  log('='.repeat(70));
  log('European Immigration Lawyers / Consultants Scraper');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'} | Country: ${countryFilter || 'ALL'} | Source: ${sourceFilter || 'ALL'}`);
  log('='.repeat(70));
  log('');

  // Determine which countries to target
  const targetCountries = countryFilter
    ? [countryFilter]
    : (isTest ? ['DE', 'FR', 'ES', 'NL'] : Object.keys(COUNTRY_NAMES));

  // ── Source 1: Expat.com ──────────────────────────────────────────────────
  if (!sourceFilter || sourceFilter === 'expat') {
    const sourceKey = 'expat.com';
    if (!completedSources.includes(sourceKey)) {
      log('');
      log('━'.repeat(50));
      log('SOURCE 1: Expat.com');
      log('━'.repeat(50));

      const countries = isTest
        ? targetCountries.slice(0, 2)
        : targetCountries.filter(c => EXPAT_COM_COUNTRIES[c]);

      for (const cc of countries) {
        const slug = EXPAT_COM_COUNTRIES[cc];
        if (!slug) continue;
        const leads = await scrapeExpatCom(cc, slug);
        const added = addLeads(leads);
        log(`  ${COUNTRY_NAMES[cc]}: +${added} new (${leads.length} total)`);
        await randomDelay();
      }

      completedSources.push(sourceKey);
      saveProgress(completedSources, allLeads);
      writeCSV(allLeads, OUTPUT_FILE);
    }
  }

  // ── Source 2: Alexia.fr (France only) ─────────────────────────────────────
  if ((!sourceFilter || sourceFilter === 'alexia') && (!countryFilter || countryFilter === 'FR')) {
    const sourceKey = 'alexia.fr';
    if (!completedSources.includes(sourceKey)) {
      log('');
      log('━'.repeat(50));
      log('SOURCE 2: Alexia.fr (France)');
      log('━'.repeat(50));

      const leads = await scrapeAlexiaFr(ALEXIA_CITIES, isTest);
      const added = addLeads(leads);
      log(`  France (Alexia.fr): +${added} new avocats`);

      completedSources.push(sourceKey);
      saveProgress(completedSources, allLeads);
      writeCSV(allLeads, OUTPUT_FILE);
    }
  }

  // ── Source 3: IAmExpat.de (Germany only) ───────────────────────────────────
  if ((!sourceFilter || sourceFilter === 'iamexpat-de') && (!countryFilter || countryFilter === 'DE')) {
    const sourceKey = 'iamexpat.de';
    if (!completedSources.includes(sourceKey)) {
      log('');
      log('━'.repeat(50));
      log('SOURCE 3: IAmExpat.de (Germany)');
      log('━'.repeat(50));

      const leads = await scrapeIAmExpatDe(IAMEXPAT_DE_CITIES, isTest);
      const added = addLeads(leads);
      log(`  Germany (IAmExpat.de): +${added} new firms`);

      completedSources.push(sourceKey);
      saveProgress(completedSources, allLeads);
      writeCSV(allLeads, OUTPUT_FILE);
    }
  }

  // ── Source 4: IAmExpat.nl (Netherlands only) ──────────────────────────────
  if ((!sourceFilter || sourceFilter === 'iamexpat-nl') && (!countryFilter || countryFilter === 'NL')) {
    const sourceKey = 'iamexpat.nl';
    if (!completedSources.includes(sourceKey)) {
      log('');
      log('━'.repeat(50));
      log('SOURCE 4: IAmExpat.nl (Netherlands)');
      log('━'.repeat(50));

      const leads = await scrapeIAmExpatNl(isTest);
      const added = addLeads(leads);
      log(`  Netherlands (IAmExpat.nl): +${added} new firms`);

      completedSources.push(sourceKey);
      saveProgress(completedSources, allLeads);
      writeCSV(allLeads, OUTPUT_FILE);
    }
  }

  // (Source 5 removed — Expatica.com and Expat.com city pages are JS-rendered)

  // ── Final Output ──────────────────────────────────────────────────────────

  writeCSV(allLeads, OUTPUT_FILE);

  // Stats
  const bySource = {};
  const byCountry = {};
  for (const lead of allLeads) {
    bySource[lead.source] = (bySource[lead.source] || 0) + 1;
    byCountry[lead.country] = (byCountry[lead.country] || 0) + 1;
  }

  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name || l.last_name).length;

  log('');
  log('='.repeat(70));
  log('SCRAPE COMPLETE');
  log('='.repeat(70));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With phone:   ${withPhone}`);
  log(`With email:   ${withEmail}`);
  log(`With website: ${withWebsite}`);
  log(`With name:    ${withName}`);
  log('');
  log('By Source:');
  for (const [src, count] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
    log(`  ${src.padEnd(20)} ${count}`);
  }
  log('');
  log('By Country:');
  for (const [cc, count] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
    log(`  ${cc.padEnd(5)} ${(COUNTRY_NAMES[cc] || cc).padEnd(20)} ${count}`);
  }
  log('');
  log(`Output: ${OUTPUT_FILE}`);

  // Clean up progress on successful complete run
  if (!isTest && !countryFilter && !sourceFilter) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
