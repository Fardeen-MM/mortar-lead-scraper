#!/usr/bin/env node
/**
 * Global Education Agent Directory Scraper
 *
 * Multi-source scraper covering international education agent directories:
 *
 *   Source 1: Education Agents Guide (educationagentsguide.com)
 *             ~33,000 agents across 200+ countries. Index pages list agent links,
 *             then profile pages have phone, email, website, address.
 *
 *   Source 2: Education Agent Partnerships (educationagentpartnerships.com)
 *             350+ agents seeking institutional partnerships. Directory page + profiles.
 *
 *   Source 3: Study Abroad Lists (studyabroadlists.com)
 *             ~1,700 agents with phone, location, specialties.
 *             Paginated listing with "load more" style.
 *
 * Not scrapable via HTTP (login-gated, AJAX-only, or no public directory):
 *   - ICEF QEAC (already scraped via scrape-icef-agents.js - 9,566 agents)
 *   - Navitas (WordPress AJAX, no public listing)
 *   - StudyPortals, IDP, Shorelight, INTO, QS, UniversityHQ, MastersPortal
 *   - ENZ AgentLab (login required), PIER/EATC (redirects to ICEF)
 *
 * Usage:
 *   node scripts/scrape-global-education-agents.js --test    # Test mode (~3 countries, 10 profiles)
 *   node scripts/scrape-global-education-agents.js --all     # Full scrape
 *   node scripts/scrape-global-education-agents.js --source=eag    # Only Education Agents Guide
 *   node scripts/scrape-global-education-agents.js --source=eap    # Only Education Agent Partnerships
 *   node scripts/scrape-global-education-agents.js --source=sal    # Only Study Abroad Lists
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ────────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'global-education-agents.csv');
const PROGRESS_FILE = path.join(OUT_DIR, 'global-education-agents-progress.json');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url', 'address', 'fax',
];

// ── CLI args ──────────────────────────────────────────────────────────────────

const cliArgs = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  cliArgs[key] = val || true;
}

const TEST_MODE = !!cliArgs.test;
const SOURCE_FILTER = (cliArgs.source || '').toLowerCase();

// ── Helpers ───────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

const startTime = Date.now();
function elapsed() {
  const s = Math.floor((Date.now() - startTime) / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`;
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

function log(msg) {
  console.log(`[${elapsed()}] ${msg}`);
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'https://' + u;
    return new URL(u).hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function decodeHtml(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/').replace(/&nbsp;/g, ' ');
}

function cleanPhone(phone) {
  if (!phone) return '';
  let p = phone.trim();
  // Remove common labels
  p = p.replace(/^(tel|phone|ph|fax|mobile|cell|office|direct)[\s:.]+/i, '');
  p = p.replace(/\s+/g, ' ').trim();
  // Must have at least some digits
  if ((p.match(/\d/g) || []).length < 6) return '';
  return p;
}

function cleanEmail(email) {
  if (!email) return '';
  let e = email.trim().toLowerCase();
  // Extract email from "mailto:" or surrounding text
  const m = e.match(/([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i);
  return m ? m[1] : '';
}

function cleanWebsite(url) {
  if (!url) return '';
  let u = url.trim();
  if (u === '-' || u === 'N/A' || u.length < 4) return '';
  // Remove trailing slashes and clean up
  u = u.replace(/\/+$/, '');
  return u;
}

function parseName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const cleaned = fullName
    .replace(/\s*(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?)\s*/gi, '')
    .replace(/\s*(PhD|MBA|MA|BA|BSc|MSc|CPA|FAICD|OAM)\s*/gi, '')
    .trim();
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
  }
  if (parts.length === 1) {
    return { first_name: parts[0], last_name: '' };
  }
  return { first_name: '', last_name: '' };
}

function dedupKey(firmName, city, country) {
  return `${(firmName || '').toLowerCase().trim()}|${(city || '').toLowerCase().trim()}|${(country || '').toLowerCase().trim()}`;
}

function writeCSV(leads, outPath) {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

// ── HTTP Fetcher ──────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Cache-Control': 'no-cache',
      ...extraHeaders,
    };

    const req = mod.get(url, { headers, timeout: REQUEST_TIMEOUT }, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        } else if (!redirectUrl.startsWith('http')) {
          // Relative redirect
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}/${redirectUrl}`;
        }
        res.resume();
        return httpGet(redirectUrl, retries, extraHeaders).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        res.resume();
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        res.resume();
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        res.resume();
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Progress ──────────────────────────────────────────────────────────────────

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
    totalLeads: leads.length,
    updatedAt: new Date().toISOString(),
  }));
}


// =============================================================================
// SOURCE 1: Education Agents Guide (educationagentsguide.com)
// =============================================================================

const EAG_BASE = 'https://educationagentsguide.com';

// Top 40 countries by student recruitment volume
const EAG_COUNTRIES = [
  // Major source countries for international students
  { name: 'India', slug: 'india' },
  { name: 'China', slug: 'china' },
  { name: 'Nigeria', slug: 'nigeria' },
  { name: 'Bangladesh', slug: 'bangladesh' },
  { name: 'Pakistan', slug: 'pakistan' },
  { name: 'Vietnam', slug: 'vietnam' },
  { name: 'Nepal', slug: 'nepal' },
  { name: 'Philippines', slug: 'philippines' },
  { name: 'Sri Lanka', slug: 'sri_lanka' },
  { name: 'Indonesia', slug: 'indonesia' },
  { name: 'Malaysia', slug: 'malaysia' },
  { name: 'Thailand', slug: 'thailand' },
  { name: 'South Korea', slug: 'south_korea' },
  { name: 'Japan', slug: 'japan' },
  { name: 'Taiwan', slug: 'taiwan' },
  { name: 'Hong Kong', slug: 'hong_kong' },
  { name: 'Singapore', slug: 'singapore' },
  // Major destination countries (agents also based there)
  { name: 'Australia', slug: 'australia' },
  { name: 'United Kingdom', slug: 'united_kingdom' },
  { name: 'United States', slug: 'united_states' },
  { name: 'Canada', slug: 'canada' },
  { name: 'New Zealand', slug: 'new_zealand' },
  { name: 'Ireland', slug: 'ireland' },
  { name: 'Germany', slug: 'germany' },
  // Middle East
  { name: 'Dubai', slug: 'dubai' },
  { name: 'Saudi Arabia', slug: 'saudi_arabia' },
  { name: 'United Arab Emirates', slug: 'united_arab_emirates' },
  { name: 'Qatar', slug: 'qatar' },
  { name: 'Oman', slug: 'oman' },
  { name: 'Kuwait', slug: 'kuwait' },
  // Africa
  { name: 'Kenya', slug: 'kenya' },
  { name: 'Ghana', slug: 'ghana' },
  { name: 'South Africa', slug: 'south_africa' },
  { name: 'Egypt', slug: 'egypt' },
  // Latin America
  { name: 'Brazil', slug: 'brazil' },
  { name: 'Colombia', slug: 'colombia' },
  { name: 'Mexico', slug: 'mexico' },
  // Europe
  { name: 'Turkey', slug: 'turkey' },
  { name: 'Russia', slug: 'russia' },
  { name: 'France', slug: 'france' },
];

/**
 * Fetch the country index page and extract all agent profile links.
 * Returns array of { name, profileUrl }
 */
async function eagFetchCountryIndex(country) {
  const url = `${EAG_BASE}/${country.slug}/index.htm`;
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) return [];

    const $ = cheerio.load(body);
    const agents = [];

    // Agent links are in <li><a href="filename.htm">Agent Name</a></li>
    $('li a').each((_, el) => {
      const href = $(el).attr('href') || '';
      const name = $(el).text().trim();

      // Skip navigation/non-agent links
      if (!href.endsWith('.htm') && !href.endsWith('.html')) return;
      if (href.includes('index.htm') || href.includes('newsletter')) return;
      if (!name || name.length < 3) return;

      // Build full URL
      let profileUrl;
      if (href.startsWith('http')) {
        profileUrl = href;
      } else if (href.startsWith('/')) {
        profileUrl = `${EAG_BASE}${href}`;
      } else {
        profileUrl = `${EAG_BASE}/${country.slug}/${href}`;
      }

      agents.push({ name: decodeHtml(name), profileUrl, country: country.name });
    });

    return agents;
  } catch (err) {
    log(`  EAG error fetching ${country.name} index: ${err.message}`);
    return [];
  }
}

/**
 * Clean a firm name extracted from the page (remove tabs, newlines, trailing descriptors).
 */
function cleanFirmName(raw) {
  if (!raw) return '';
  return raw
    .replace(/[\t\r\n]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s*[-–]\s*(?:Study Abroad|Education(?:al)?|Overseas|Consultant|Agent|Advisor).*$/i, '')
    .replace(/\s+(?:Education\s+Agent|Study\s+Abroad\s+Agent|Educational\s+Consultant)s?\s*$/i, '')
    .trim();
}

/**
 * Fetch an individual agent profile page and extract contact details.
 * The site uses <strong>Label:</strong> text pattern for structured fields.
 */
async function eagFetchProfile(agent) {
  try {
    const { statusCode, body } = await httpGet(agent.profileUrl);
    if (statusCode !== 200 || !body) return null;

    const $ = cheerio.load(body);
    const text = $('body').text();
    const result = {
      firm_name: cleanFirmName(agent.name) || '',
      country: agent.country || '',
      profile_url: agent.profileUrl,
      source: 'educationagentsguide.com',
      niche: 'education',
    };

    // ── Strategy: Parse structured fields via <strong>Label:</strong> patterns ──
    // The site uses: <strong>Telephone:</strong> +91..., <strong>Email:</strong> <a href="mailto:...">
    // <strong>Website:</strong> <a href="http://...">, <strong>Address:</strong> text

    // Walk through all <strong> tags to find labeled fields
    $('strong, b').each((_, el) => {
      const label = $(el).text().trim().toLowerCase().replace(/:$/, '');
      // Get the next sibling content (text or link after the label)
      const parent = $(el).parent();
      const parentHtml = parent.html() || '';
      const elOuterHtml = $.html(el);
      // Get content after this <strong> tag
      const afterIdx = parentHtml.indexOf(elOuterHtml);
      if (afterIdx < 0) return;
      const afterHtml = parentHtml.substring(afterIdx + elOuterHtml.length);
      const $after = cheerio.load(afterHtml);
      const afterText = $after.text().trim();

      switch (label) {
        case 'telephone':
        case 'phone':
        case 'tel':
        case 'mobile':
        case 'landline': {
          if (!result.phone) {
            // Check for tel: link first
            const telLink = $after('a[href^="tel:"]').first();
            if (telLink.length) {
              result.phone = cleanPhone(telLink.text().trim() || telLink.attr('href').replace('tel:', ''));
            } else {
              // Plain text phone
              const ph = afterText.split('\n')[0].trim();
              result.phone = cleanPhone(ph);
            }
          }
          break;
        }
        case 'email':
        case 'e-mail':
        case 'contact email': {
          if (!result.email) {
            const mailLink = $after('a[href^="mailto:"]').first();
            if (mailLink.length) {
              result.email = cleanEmail(mailLink.attr('href').replace('mailto:', '') || mailLink.text().trim());
            } else {
              result.email = cleanEmail(afterText.split('\n')[0].trim());
            }
          }
          break;
        }
        case 'website':
        case 'web':
        case 'url': {
          if (!result.website) {
            const webLink = $after('a[href^="http"]').first();
            if (webLink.length) {
              const href = webLink.attr('href') || '';
              if (!href.includes('educationagentsguide') && !href.includes('educationmarketingsolutions')) {
                result.website = cleanWebsite(href);
                result.domain = extractDomain(href);
              }
            }
            if (!result.website) {
              const urlText = afterText.split('\n')[0].trim();
              if (urlText && /\.[a-z]{2,}/.test(urlText) && !urlText.includes('educationmarketingsolutions')) {
                result.website = cleanWebsite(urlText);
                result.domain = extractDomain(urlText);
              }
            }
          }
          break;
        }
        case 'address':
        case 'location':
        case 'office': {
          if (!result.address) {
            const addr = afterText.replace(/\n/g, ', ').replace(/,\s*,/g, ',').substring(0, 200).trim();
            if (addr.length > 5) result.address = addr;
          }
          break;
        }
        case 'fax': {
          if (!result.fax) {
            result.fax = cleanPhone(afterText.split('\n')[0].trim());
          }
          break;
        }
        case 'principal agent':
        case 'principal':
        case 'contact person':
        case 'contact':
        case 'director':
        case 'ceo':
        case 'manager': {
          if (!result.first_name) {
            const nameText = afterText.split('\n')[0].trim();
            // Only accept if it looks like a person name (2-4 words, capitalized)
            if (nameText && /^[A-Z][a-z]+(\s+[A-Z][a-z]+){0,3}$/.test(nameText) && nameText.length < 50) {
              const { first_name, last_name } = parseName(nameText);
              result.first_name = first_name;
              result.last_name = last_name;
            }
          }
          break;
        }
      }
    });

    // ── Fallback: tel: and mailto: links if not found yet ──
    if (!result.phone) {
      $('a[href^="tel:"]').each((_, el) => {
        if (!result.phone) {
          result.phone = cleanPhone($(el).text().trim() || $(el).attr('href').replace('tel:', ''));
        }
      });
    }

    if (!result.email) {
      $('a[href^="mailto:"]').each((_, el) => {
        if (!result.email) {
          const addr = $(el).attr('href').replace('mailto:', '') || $(el).text().trim();
          if (!addr.includes('educationagentsguide') && !addr.includes('educationmarketingsolutions')) {
            result.email = cleanEmail(addr);
          }
        }
      });
    }

    // ── Fallback: text regex for phone ──
    if (!result.phone) {
      const phoneMatch = text.match(/(?:telephone|phone|tel|ph)[:\s]+([+\d\s\-().]{7,20})/i);
      if (phoneMatch) result.phone = cleanPhone(phoneMatch[1]);
    }

    // ── Fallback: text regex for email ──
    if (!result.email) {
      const emailMatch = text.match(/(?:email|e-mail)[:\s]+([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i);
      if (emailMatch && !emailMatch[1].includes('educationagentsguide')) {
        result.email = cleanEmail(emailMatch[1]);
      }
    }

    // ── Fallback: website from text ──
    if (!result.website) {
      const webMatch = text.match(/(?:website|web|url)[:\s]+((?:https?:\/\/)?(?:www\.)?[a-z0-9][a-z0-9\-]+\.[a-z]{2,}[^\s,]*)/i);
      if (webMatch && !webMatch[1].includes('educationmarketingsolutions') && !webMatch[1].includes('educationagentsguide')) {
        result.website = cleanWebsite(webMatch[1]);
        result.domain = extractDomain(webMatch[1]);
      }
    }

    // ── Extract city from agent name ──
    // Pattern: "Agent Name - City Education Agent" or "Agent Name - City"
    const cityFromName = agent.name.match(/[-–]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*$/i);
    if (cityFromName && !result.city) result.city = cityFromName[1];

    // Parse city from address
    if (result.address && !result.city) {
      const parts = result.address.split(',').map(s => s.trim()).filter(Boolean);
      if (parts.length >= 3) {
        // Try to find a city (usually 2nd or 3rd from end)
        result.city = parts[Math.max(0, parts.length - 3)] || '';
      }
    }

    return result;
  } catch (err) {
    return null;
  }
}

/**
 * Source 1 main: Scrape Education Agents Guide
 */
async function scrapeEducationAgentsGuide(allLeads, seenKeys) {
  log('');
  log('=== SOURCE 1: Education Agents Guide (educationagentsguide.com) ===');

  const countries = TEST_MODE ? EAG_COUNTRIES.slice(0, 3) : EAG_COUNTRIES;
  let totalNew = 0;
  let profilesFetched = 0;
  let profileErrors = 0;

  for (let ci = 0; ci < countries.length; ci++) {
    const country = countries[ci];
    log(`[${ci + 1}/${countries.length}] Fetching ${country.name} index...`);

    const agents = await eagFetchCountryIndex(country);
    log(`  Found ${agents.length} agent links`);

    if (agents.length === 0) {
      await randomDelay();
      continue;
    }

    // In test mode, only fetch 5 profiles per country
    const profilesToFetch = TEST_MODE ? agents.slice(0, 5) : agents;
    let countryNew = 0;

    for (let pi = 0; pi < profilesToFetch.length; pi++) {
      const agent = profilesToFetch[pi];

      // Skip if we already have this agent
      const key = dedupKey(agent.name, '', country.name);
      if (seenKeys.has(key)) continue;

      try {
        const lead = await eagFetchProfile(agent);
        if (lead && lead.firm_name) {
          const dedupK = dedupKey(lead.firm_name, lead.city || '', lead.country || '');
          if (!seenKeys.has(dedupK)) {
            seenKeys.add(dedupK);
            seenKeys.add(key);
            allLeads.push(lead);
            countryNew++;
            totalNew++;
          }
          profilesFetched++;
        }
      } catch {
        profileErrors++;
      }

      // Progress logging every 25 profiles
      if ((pi + 1) % 25 === 0) {
        log(`  ${country.name}: ${pi + 1}/${profilesToFetch.length} profiles (${countryNew} new)`);
      }

      await randomDelay();
    }

    log(`  ${country.name}: ${countryNew} new agents from ${profilesToFetch.length} profiles`);

    // Incremental save every 5 countries
    if ((ci + 1) % 5 === 0) {
      writeCSV(allLeads, OUT_FILE);
      log(`  [Saved: ${allLeads.length} total leads]`);
    }
  }

  log(`EAG complete: ${totalNew} new agents, ${profilesFetched} profiles fetched, ${profileErrors} errors`);
  return totalNew;
}


// =============================================================================
// SOURCE 2: Education Agent Partnerships (educationagentpartnerships.com)
// =============================================================================

const EAP_BASE = 'https://educationagentpartnerships.com';

/**
 * Fetch the agent directory and get all agent profile links.
 */
async function eapFetchDirectory() {
  const url = `${EAP_BASE}/agent_directory.htm`;
  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) return [];

    const $ = cheerio.load(body);
    const agents = [];

    $('li a').each((_, el) => {
      const href = $(el).attr('href') || '';
      const name = $(el).text().trim();

      if (!href.endsWith('.htm') && !href.endsWith('.html')) return;
      if (href.includes('index') || href.includes('directory') || href.includes('database')) return;
      if (!name || name.length < 3) return;

      let profileUrl;
      if (href.startsWith('http')) {
        profileUrl = href;
      } else if (href.startsWith('/')) {
        profileUrl = `${EAP_BASE}${href}`;
      } else {
        profileUrl = `${EAP_BASE}/${href}`;
      }

      // Extract country hint from href path (e.g., "india/agentname.htm")
      const pathParts = href.split('/');
      let countryHint = '';
      if (pathParts.length >= 2) {
        countryHint = pathParts[pathParts.length - 2]
          .replace(/_/g, ' ')
          .replace(/\b\w/g, c => c.toUpperCase());
      }

      agents.push({
        name: decodeHtml(name).replace(/\s+seeks?\s+.*/i, '').trim(),
        profileUrl,
        countryHint,
      });
    });

    return agents;
  } catch (err) {
    log(`  EAP directory error: ${err.message}`);
    return [];
  }
}

/**
 * Fetch an EAP agent profile page (same site family as EAG, similar structure).
 */
async function eapFetchProfile(agent) {
  try {
    const { statusCode, body } = await httpGet(agent.profileUrl);
    if (statusCode !== 200 || !body) return null;

    const $ = cheerio.load(body);
    const text = $('body').text();
    const result = {
      firm_name: cleanFirmName(agent.name) || '',
      country: agent.countryHint || '',
      profile_url: agent.profileUrl,
      source: 'educationagentpartnerships.com',
      niche: 'education',
    };

    // Use the same <strong>Label:</strong> parsing approach as EAG
    $('strong, b').each((_, el) => {
      const label = $(el).text().trim().toLowerCase().replace(/:$/, '');
      const parent = $(el).parent();
      const parentHtml = parent.html() || '';
      const elOuterHtml = $.html(el);
      const afterIdx = parentHtml.indexOf(elOuterHtml);
      if (afterIdx < 0) return;
      const afterHtml = parentHtml.substring(afterIdx + elOuterHtml.length);
      const $after = cheerio.load(afterHtml);
      const afterText = $after.text().trim();

      switch (label) {
        case 'telephone':
        case 'phone':
        case 'tel':
        case 'mobile': {
          if (!result.phone) {
            const telLink = $after('a[href^="tel:"]').first();
            if (telLink.length) {
              result.phone = cleanPhone(telLink.text().trim() || telLink.attr('href').replace('tel:', ''));
            } else {
              result.phone = cleanPhone(afterText.split('\n')[0].trim());
            }
          }
          break;
        }
        case 'email':
        case 'e-mail': {
          if (!result.email) {
            const mailLink = $after('a[href^="mailto:"]').first();
            if (mailLink.length) {
              result.email = cleanEmail(mailLink.attr('href').replace('mailto:', '') || mailLink.text().trim());
            } else {
              result.email = cleanEmail(afterText.split('\n')[0].trim());
            }
          }
          break;
        }
        case 'website':
        case 'web':
        case 'url': {
          if (!result.website) {
            const webLink = $after('a[href^="http"]').first();
            if (webLink.length) {
              const href = webLink.attr('href') || '';
              if (!href.includes('educationagentpartnerships') && !href.includes('agentpartnerships') &&
                  !href.includes('educationmarketingsolutions')) {
                result.website = cleanWebsite(href);
                result.domain = extractDomain(href);
              }
            }
          }
          break;
        }
        case 'address':
        case 'location': {
          if (!result.address) {
            const addr = afterText.replace(/\n/g, ', ').replace(/,\s*,/g, ',').substring(0, 200).trim();
            if (addr.length > 5) {
              result.address = addr;
              const parts = addr.split(',').map(s => s.trim()).filter(Boolean);
              if (parts.length >= 2 && !result.city) {
                result.city = parts[parts.length >= 3 ? parts.length - 3 : 0] || '';
              }
            }
          }
          break;
        }
      }
    });

    // Fallback: tel: and mailto: links
    if (!result.phone) {
      $('a[href^="tel:"]').each((_, el) => {
        if (!result.phone) {
          result.phone = cleanPhone($(el).text().trim() || $(el).attr('href').replace('tel:', ''));
        }
      });
    }
    if (!result.email) {
      $('a[href^="mailto:"]').each((_, el) => {
        if (!result.email) {
          const addr = $(el).attr('href').replace('mailto:', '') || $(el).text().trim();
          if (!addr.includes('educationagentpartnerships') && !addr.includes('educationmarketingsolutions')) {
            result.email = cleanEmail(addr);
          }
        }
      });
    }

    // Text fallbacks
    if (!result.phone) {
      const phoneMatch = text.match(/(?:telephone|phone|tel|ph)[:\s]+([+\d\s\-().]{7,20})/i);
      if (phoneMatch) result.phone = cleanPhone(phoneMatch[1]);
    }
    if (!result.email) {
      const emailMatch = text.match(/(?:email|e-mail)[:\s]+([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})/i);
      if (emailMatch && !emailMatch[1].includes('educationagentpartnerships')) {
        result.email = cleanEmail(emailMatch[1]);
      }
    }

    return result;
  } catch {
    return null;
  }
}

/**
 * Source 2 main: Scrape Education Agent Partnerships
 */
async function scrapeEducationAgentPartnerships(allLeads, seenKeys) {
  log('');
  log('=== SOURCE 2: Education Agent Partnerships (educationagentpartnerships.com) ===');

  const agents = await eapFetchDirectory();
  log(`Found ${agents.length} agent links in directory`);

  if (agents.length === 0) return 0;

  const profilesToFetch = TEST_MODE ? agents.slice(0, 10) : agents;
  let totalNew = 0;

  for (let i = 0; i < profilesToFetch.length; i++) {
    const agent = profilesToFetch[i];

    try {
      const lead = await eapFetchProfile(agent);
      if (lead && lead.firm_name) {
        const key = dedupKey(lead.firm_name, lead.city || '', lead.country || '');
        if (!seenKeys.has(key)) {
          seenKeys.add(key);
          allLeads.push(lead);
          totalNew++;
        }
      }
    } catch {}

    if ((i + 1) % 25 === 0) {
      log(`  ${i + 1}/${profilesToFetch.length} profiles (${totalNew} new)`);
    }

    await randomDelay();
  }

  log(`EAP complete: ${totalNew} new agents from ${profilesToFetch.length} profiles`);
  return totalNew;
}


// =============================================================================
// SOURCE 3: Study Abroad Lists (studyabroadlists.com)
// =============================================================================

const SAL_BASE = 'https://www.studyabroadlists.com';

/**
 * Fetch a page of agent listings from Study Abroad Lists.
 * Uses ?page=N query parameter for pagination.
 * Agent cards use .grid_element > .search_result.grid structure.
 */
async function salFetchPage(page) {
  const url = page === 1
    ? `${SAL_BASE}/education-agents`
    : `${SAL_BASE}/education-agents?page=${page}`;

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) return { agents: [], hasMore: false, totalPages: 0 };

    const $ = cheerio.load(body);
    const agents = [];

    // Extract total results from "Showing X - Y of N Results" text
    let totalPages = 0;
    const bodyText = $('body').text().replace(/\s+/g, ' ');
    const showingMatch = bodyText.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (showingMatch) {
      const totalResults = parseInt(showingMatch[1].replace(/,/g, ''));
      totalPages = Math.ceil(totalResults / 9);  // 9 per page
    }
    // Also check for "page X of Y" pattern
    if (!totalPages) {
      const pageMatch = bodyText.match(/page\s+\d+\s+of\s+(\d+)/i);
      if (pageMatch) totalPages = parseInt(pageMatch[1]);
    }

    // Each agent card is in .grid_element
    // Structure: h2.h3 > b = name, .fa-map-marker text = location,
    //            .myphoneHide a[href^="tel:"] = phone
    $('.grid_element').each((_, el) => {
      const $el = $(el);
      const result = {
        source: 'studyabroadlists.com',
        niche: 'education',
      };

      // Agent name from h2 > b (the bold part, excludes "- Education Agents" span)
      const nameEl = $el.find('h2 b, h2.h3 b').first();
      if (nameEl.length) {
        result.firm_name = nameEl.text().trim();
      } else {
        // Fallback: h2 a text (strip "- Education Agents")
        const h2a = $el.find('h2 a, h2.h3 a').first();
        if (h2a.length) {
          result.firm_name = h2a.text().trim().replace(/\s*[-–]\s*Education\s+Agents?\s*$/i, '').trim();
        }
      }

      // Profile URL from the h2 link or the "View Listing" link
      const profileLink = $el.find('.mid_section > a[href], h2 a, a[href*="education-agents"]').first();
      if (profileLink.length) {
        const href = profileLink.attr('href') || '';
        result.profile_url = href.startsWith('http') ? href : SAL_BASE + href;

        // Extract country from URL path: /country/city/education-agents/name
        const pathParts = href.split('/').filter(Boolean);
        if (pathParts.length >= 1 && pathParts[0] !== 'education-agents') {
          // First part is country
          const countryFromUrl = decodeURIComponent(pathParts[0])
            .replace(/-/g, ' ')
            .replace(/\b\w/g, c => c.toUpperCase());
          if (!result.country) result.country = countryFromUrl;
        }
      }

      if (!result.firm_name) return;

      // Location: text after .fa-map-marker icon in the mid_section
      const midSection = $el.find('.mid_section');
      if (midSection.length) {
        const midHtml = midSection.html() || '';
        // Location appears after fa-map-marker icon as plain text
        const locMatch = midHtml.match(/fa-map-marker[^>]*>[\s\S]*?<\/i>\s*\n?\s*([^<\n]+)/);
        if (locMatch) {
          const loc = locMatch[1].trim();
          const parts = loc.split(',').map(s => s.trim()).filter(Boolean);
          if (parts.length >= 2) {
            result.city = parts[0];
            result.country = parts[parts.length - 1];
            if (parts.length >= 3) result.state = parts[1];
          } else if (parts.length === 1) {
            result.country = parts[0];
          }
        }
      }

      // Phone from hidden .myphoneHide a[href^="tel:"]
      const telLink = $el.find('.myphoneHide a[href^="tel:"], a[href^="tel:"]').first();
      if (telLink.length) {
        const phoneText = telLink.text().trim().replace(/^call:\s*/i, '');
        result.phone = cleanPhone(phoneText || telLink.attr('href').replace('tel:', ''));
      }

      // Specialties from table
      $el.find('table tr').each((_, row) => {
        const label = $(row).find('td.bold').text().trim().toLowerCase();
        const value = $(row).find('td:last-child').text().trim();
        if (label.includes('specialt')) {
          result.title = value;
        }
      });

      agents.push(result);
    });

    const hasMore = page < totalPages;
    return { agents, hasMore, totalPages };
  } catch (err) {
    log(`  SAL error page ${page}: ${err.message}`);
    return { agents: [], hasMore: false, totalPages: 0 };
  }
}

/**
 * Source 3 main: Scrape Study Abroad Lists
 */
async function scrapeStudyAbroadLists(allLeads, seenKeys) {
  log('');
  log('=== SOURCE 3: Study Abroad Lists (studyabroadlists.com) ===');

  let page = 1;
  const maxPages = TEST_MODE ? 3 : 200;
  let totalNew = 0;
  let totalFetched = 0;
  let knownTotalPages = 0;

  while (page <= maxPages) {
    log(`  Fetching page ${page}${knownTotalPages ? '/' + knownTotalPages : ''}...`);
    const { agents, hasMore, totalPages } = await salFetchPage(page);
    if (totalPages > 0) knownTotalPages = totalPages;

    if (agents.length === 0) {
      log(`  No agents on page ${page}, stopping.`);
      break;
    }

    totalFetched += agents.length;
    let pageNew = 0;

    for (const agent of agents) {
      const key = dedupKey(agent.firm_name, agent.city || '', agent.country || '');
      if (seenKeys.has(key)) continue;

      seenKeys.add(key);
      allLeads.push(agent);
      pageNew++;
      totalNew++;
    }

    log(`  Page ${page}: ${agents.length} agents, ${pageNew} new (total: ${totalNew})`);

    if (!hasMore) {
      log(`  No more pages.`);
      break;
    }

    page++;
    await randomDelay();

    // Save every 20 pages
    if (page % 20 === 0) {
      writeCSV(allLeads, OUT_FILE);
      log(`  [Saved: ${allLeads.length} total leads]`);
    }
  }

  log(`SAL complete: ${totalNew} new agents from ${totalFetched} listings across ${page} pages`);
  return totalNew;
}


// =============================================================================
// NOTE: Sources investigated but NOT scrapable via HTTP + Cheerio:
//   - Navitas (navitas.com): WordPress AJAX requires unknown action names/nonces
//   - ICEF QEAC (icef.com/academy): Dynamic Google Maps + AJAX (separate scraper exists)
//   - StudyPortals: No agent directory exists
//   - IDP (idp.com): Office locator only, no public agent listing
//   - Shorelight: Login-gated counselor portal
//   - INTO University Partnerships: Login-gated partner portal
//   - QS (qs.com): No public agent directory
//   - UniversityHQ.org: No agent directory
//   - MastersPortal/BachelorsPortal: No agent directory
//   - Education New Zealand AgentLab: Login required
//   - PIER/EATC: Redirects to ICEF (already scraped)
// =============================================================================


// =============================================================================
// MAIN
// =============================================================================

async function main() {
  console.log('=== Global Education Agent Directory Scraper ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  if (SOURCE_FILTER) console.log(`Source filter: ${SOURCE_FILTER}`);
  console.log(`Output: ${OUT_FILE}`);
  console.log('');

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  const allLeads = [];
  const seenKeys = new Set();
  const sourceResults = {};

  const runSource = (name) => !SOURCE_FILTER || SOURCE_FILTER === name;

  // Source 1: Education Agents Guide (~33,000 agents across 200+ countries)
  if (runSource('eag')) {
    try {
      const count = await scrapeEducationAgentsGuide(allLeads, seenKeys);
      sourceResults['Education Agents Guide'] = count;
      writeCSV(allLeads, OUT_FILE);
      log(`[Saved after EAG: ${allLeads.length} total leads]`);
    } catch (err) {
      log(`EAG ERROR: ${err.message}`);
      sourceResults['Education Agents Guide'] = 'ERROR';
    }
  }

  // Source 2: Education Agent Partnerships (~230 agents seeking partnerships)
  if (runSource('eap')) {
    try {
      const count = await scrapeEducationAgentPartnerships(allLeads, seenKeys);
      sourceResults['Education Agent Partnerships'] = count;
      writeCSV(allLeads, OUT_FILE);
      log(`[Saved after EAP: ${allLeads.length} total leads]`);
    } catch (err) {
      log(`EAP ERROR: ${err.message}`);
      sourceResults['Education Agent Partnerships'] = 'ERROR';
    }
  }

  // Source 3: Study Abroad Lists (~1,700 agents globally)
  if (runSource('sal')) {
    try {
      const count = await scrapeStudyAbroadLists(allLeads, seenKeys);
      sourceResults['Study Abroad Lists'] = count;
      writeCSV(allLeads, OUT_FILE);
      log(`[Saved after SAL: ${allLeads.length} total leads]`);
    } catch (err) {
      log(`SAL ERROR: ${err.message}`);
      sourceResults['Study Abroad Lists'] = 'ERROR';
    }
  }

  // Final save
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withPhone = allLeads.filter(l => l.phone).length;
  const withEmail = allLeads.filter(l => l.email).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;
  const countries = {};
  for (const l of allLeads) {
    const c = l.country || 'Unknown';
    countries[c] = (countries[c] || 0) + 1;
  }
  const sortedCountries = Object.entries(countries).sort((a, b) => b[1] - a[1]);

  console.log('');
  console.log('='.repeat(60));
  console.log('RESULTS');
  console.log('='.repeat(60));
  console.log(`Total unique leads: ${allLeads.length}`);
  console.log(`With phone:         ${withPhone} (${allLeads.length ? (withPhone / allLeads.length * 100).toFixed(1) : 0}%)`);
  console.log(`With email:         ${withEmail} (${allLeads.length ? (withEmail / allLeads.length * 100).toFixed(1) : 0}%)`);
  console.log(`With website:       ${withWebsite} (${allLeads.length ? (withWebsite / allLeads.length * 100).toFixed(1) : 0}%)`);
  console.log(`With contact name:  ${withName}`);
  console.log(`Time: ${elapsed()}`);
  console.log('');
  console.log('Source breakdown:');
  for (const [source, count] of Object.entries(sourceResults)) {
    console.log(`  ${source}: ${count}`);
  }
  console.log('');
  console.log(`Country breakdown (${sortedCountries.length} countries):`);
  for (const [c, n] of sortedCountries.slice(0, 30)) {
    console.log(`  ${c}: ${n}`);
  }
  if (sortedCountries.length > 30) console.log(`  ... and ${sortedCountries.length - 30} more`);
  console.log('');
  console.log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
