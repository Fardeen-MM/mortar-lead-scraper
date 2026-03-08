#!/usr/bin/env node
/**
 * Scrape British Council certified education agents/counsellors.
 *
 * Source: https://agent-counsellor-ukhub.britishcouncil.org/gal
 * Method: Fetches gzipped JSON data dump via Public/GetAgentUsers API
 *         The endpoint returns a URL to a ~4MB gzip file containing ALL agents
 *
 * Expected: ~18,000+ certified agents/counsellors worldwide
 *
 * Data fields (numeric keys from obfuscated API):
 *   2=firstName, 3=lastName, 4=fullName, 12=country, 13=email,
 *   16=agentType, 17=roleType, 18=gender, 19=languages,
 *   22=agencyName, 26=country2, 32=city, 36=address1, 37=address2,
 *   38=address3, 61=certificationDate, 66=providerTypes,
 *   79=XML(address,phone,email,jobTitle,website),
 *   80=XML(socialProfiles)
 *
 * Uses Puppeteer to bypass Akamai anti-bot protection on britishcouncil.org.
 *
 * Features:
 *   - Single HTTP request for entire dataset (gzip compressed ~4MB)
 *   - XML parsing for phone, job title, website from field 79
 *   - Country-based filtering (--country=India)
 *   - Test mode (--test): only processes first 50 agents
 *   - Full stats with country/agency breakdown
 *
 * Usage:
 *   node scripts/scrape-british-council-agents.js
 *   node scripts/scrape-british-council-agents.js --test
 *   node scripts/scrape-british-council-agents.js --country=India
 *   node scripts/scrape-british-council-agents.js --country="United Kingdom"
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const m = arg.replace(/^--/, '').match(/^([^=]+)(?:=(.*))?$/);
  if (m) args[m[1]] = m[2] || true;
}

const TEST_MODE      = !!args.test;
const COUNTRY_FILTER = args.country || '';
const OUTPUT_DIR     = path.join(__dirname, '..', 'output');
const OUTPUT_FILE    = path.join(OUTPUT_DIR, 'uk-education-agents-british-council.csv');

// ── CSV columns ─────────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'gender', 'languages',
  'agent_type', 'provider_types', 'certification_date',
];

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  totalRaw: 0,
  filtered: 0,
  withEmail: 0,
  withPhone: 0,
  withWebsite: 0,
  saved: 0,
  startTime: Date.now(),
};

// ── Helpers ─────────────────────────────────────────────────────────────
function escapeCSV(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function elapsed() {
  const s = Math.floor((Date.now() - stats.startTime) / 1000);
  return `${Math.floor(s / 60)}m${s % 60}s`;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let clean = url.trim();
    if (!clean.startsWith('http')) clean = 'https://' + clean;
    const parsed = new URL(clean);
    return parsed.hostname.replace(/^www\./, '');
  } catch (e) {
    return '';
  }
}

/**
 * Extract values from XML-like field 79 (address/contact details).
 * Format: <row><Address1>...</Address1><City>...</City><Phone>...</Phone>...</row>
 */
function parseXmlField(xml) {
  if (!xml) return {};
  const result = {};
  const tags = ['Address1', 'Address2', 'Address3', 'City', 'CountryName', 'StateName',
                'Email', 'JobTitle', 'Phone', 'PhoneCC', 'Webaddress'];
  for (const tag of tags) {
    const m = xml.match(new RegExp(`<${tag}>(.*?)</${tag}>`, 'i'));
    if (m && m[1] && m[1] !== "'" && m[1] !== '|NOT|') {
      // Clean values: strip |NOT| prefix (means "not public")
      let val = m[1].replace(/^\|NOT\|/, '').trim();
      if (val) result[tag] = val;
    }
  }
  return result;
}

/**
 * Extract social profiles from XML-like field 80.
 * Format: <rows><row><ProfileName>...</ProfileName><ProfileUrl>...</ProfileUrl><ProfileType>6</ProfileType></row></rows>
 * ProfileType: 0=Facebook, 2=Instagram, 6=LinkedIn, etc.
 */
function parseSocialField(xml) {
  if (!xml) return {};
  const result = {};
  const profiles = xml.match(/<row>.*?<\/row>/gi) || [];
  for (const profile of profiles) {
    const urlMatch = profile.match(/<ProfileUrl>(.*?)<\/ProfileUrl>/i);
    const typeMatch = profile.match(/<ProfileType>(\d+)<\/ProfileType>/i);
    if (urlMatch && typeMatch) {
      const type = parseInt(typeMatch[1]);
      if (type === 6) result.linkedin = urlMatch[1];
      else if (type === 0) result.facebook = urlMatch[1];
      else if (type === 2) result.instagram = urlMatch[1];
    }
  }
  return result;
}

function saveCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col] || '')).join(',')
  );
  fs.writeFileSync(OUTPUT_FILE, [header, ...rows].join('\n'), 'utf8');
  stats.saved = leads.length;
}

// ── Main ────────────────────────────────────────────────────────────────
(async () => {
  console.log('=== British Council Certified Education Agents Scraper ===');
  console.log(`Test mode: ${TEST_MODE}${COUNTRY_FILTER ? `, Country filter: ${COUNTRY_FILTER}` : ''}\n`);

  // Use Puppeteer to fetch data (site has Akamai anti-bot protection)
  console.log('[1/3] Launching browser and loading agent hub...');
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

  // Load the GAL page — this triggers the GetAgentUsers API call which returns the data URL
  await page.goto('https://agent-counsellor-ukhub.britishcouncil.org/gal', {
    waitUntil: 'networkidle2',
    timeout: 45000,
  });
  console.log('  Page loaded.');

  // Step 1: Get the data URL via the page's fetch context
  console.log('\n[2/3] Fetching agent data via browser context...');

  // Get the data URL
  const dataUrl = await page.evaluate(async () => {
    const apiRes = await fetch('/Public/GetAgentUsers');
    const apiJson = await apiRes.json();
    return apiJson.Url;
  });
  console.log(`  Data URL: ${dataUrl}`);

  // Fetch the gzipped data file using page.evaluate with DecompressionStream
  console.log('  Downloading and decompressing agent data...');
  const agents = await page.evaluate(async (url) => {
    const res = await fetch(url);
    // Use DecompressionStream to handle gzip
    const ds = new DecompressionStream('gzip');
    const decompressedStream = res.body.pipeThrough(ds);
    const reader = decompressedStream.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    // Combine chunks and decode as text
    const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
    const combined = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }
    const text = new TextDecoder().decode(combined);
    return JSON.parse(text);
  }, dataUrl);

  await browser.close();

  stats.totalRaw = agents.length;
  console.log(`  Total agents in database: ${agents.length}`);

  // Step 3: Transform into leads
  console.log('\n[3/3] Processing agents...');
  const leads = [];
  const limit = TEST_MODE ? 50 : agents.length;

  for (let i = 0; i < Math.min(limit, agents.length); i++) {
    const agent = agents[i];

    // Basic fields
    const firstName = (agent['2'] || '').trim();
    const lastName = (agent['3'] || '').trim();
    const country = (agent['12'] || agent['26'] || '').trim();
    const email = (agent['13'] || '').trim();
    const agencyName = (agent['22'] || '').trim();
    const city = (agent['32'] || '').trim();
    const agentType = (agent['16'] || '').trim();
    const gender = (agent['18'] || '').trim();
    const languages = (agent['19'] || '').trim();
    const certDate = (agent['61'] || '').trim();
    const providerTypes = (agent['66'] || []).join('; ');

    // Country filter
    if (COUNTRY_FILTER && country.toLowerCase() !== COUNTRY_FILTER.toLowerCase()) {
      continue;
    }

    // Parse XML fields for phone, website, job title
    const xmlData = parseXmlField(agent['79']);
    const socialData = parseSocialField(agent['80']);

    // Build phone number
    let phone = '';
    if (xmlData.Phone) {
      const phoneCC = xmlData.PhoneCC || '';
      phone = xmlData.Phone;
      // Add country code if not already present
      if (phoneCC && !phone.startsWith('+') && !phone.startsWith(phoneCC)) {
        phone = `+${phoneCC}${phone}`;
      }
    }

    // Build website
    let website = xmlData.Webaddress || '';
    // Skip social media URLs as websites
    if (website && (website.includes('facebook.com') || website.includes('instagram.com')
        || website.includes('linkedin.com') || website.includes('twitter.com'))) {
      website = '';
    }

    const domain = extractDomain(website);

    const lead = {
      first_name: firstName,
      last_name: lastName,
      firm_name: agencyName,
      title: xmlData.JobTitle || '',
      email: email,
      phone: phone,
      website: website,
      domain: domain,
      city: xmlData.City || city,
      state: xmlData.StateName || agent['74'] || agent['78'] || '',
      country: xmlData.CountryName || country,
      niche: 'Education Agent',
      source: 'british-council',
      profile_url: '',
      gender: gender,
      languages: languages,
      agent_type: agentType,
      provider_types: providerTypes,
      certification_date: certDate ? certDate.split('T')[0] : '',
    };

    // Clean state
    if (lead.state === "'" || lead.state === '|NOT|') lead.state = '';

    // Stats
    if (lead.email) stats.withEmail++;
    if (lead.phone) stats.withPhone++;
    if (lead.website) stats.withWebsite++;

    leads.push(lead);
    stats.filtered++;

    if (i % 1000 === 0 && i > 0) {
      process.stdout.write(`\r  Processed ${i}/${Math.min(limit, agents.length)} agents...`);
    }
  }
  console.log(`\r  Processed ${Math.min(limit, agents.length)} agents.          `);

  // Save CSV
  saveCSV(leads);
  console.log(`\n  [SAVE] ${leads.length} leads -> ${OUTPUT_FILE}`);

  // Print stats
  console.log('\n=== British Council Scrape Complete ===');
  console.log(`  Time: ${elapsed()}`);
  console.log(`  Total in database: ${stats.totalRaw}`);
  console.log(`  Leads processed: ${stats.filtered}`);
  console.log(`  With email: ${stats.withEmail} (${(stats.withEmail / stats.filtered * 100).toFixed(1)}%)`);
  console.log(`  With phone: ${stats.withPhone} (${(stats.withPhone / stats.filtered * 100).toFixed(1)}%)`);
  console.log(`  With website: ${stats.withWebsite} (${(stats.withWebsite / stats.filtered * 100).toFixed(1)}%)`);
  console.log(`  Total leads saved: ${stats.saved}`);
  console.log(`  Output: ${OUTPUT_FILE}`);

  // Print country breakdown (top 20)
  const countries = {};
  for (const lead of leads) {
    const c = lead.country || 'Unknown';
    countries[c] = (countries[c] || 0) + 1;
  }
  const sortedCountries = Object.entries(countries).sort((a, b) => b[1] - a[1]);
  console.log(`\n  Country breakdown (${sortedCountries.length} countries, top 20):`);
  for (const [country, count] of sortedCountries.slice(0, 20)) {
    console.log(`    ${country}: ${count}`);
  }
  if (sortedCountries.length > 20) {
    console.log(`    ... and ${sortedCountries.length - 20} more countries`);
  }

  // Print agency breakdown (top 15)
  const agencies = {};
  for (const lead of leads) {
    const a = lead.firm_name || 'Unknown';
    agencies[a] = (agencies[a] || 0) + 1;
  }
  const sortedAgencies = Object.entries(agencies).sort((a, b) => b[1] - a[1]);
  console.log(`\n  Top agencies (${sortedAgencies.length} unique, top 15):`);
  for (const [agency, count] of sortedAgencies.slice(0, 15)) {
    console.log(`    ${agency}: ${count}`);
  }
})();
