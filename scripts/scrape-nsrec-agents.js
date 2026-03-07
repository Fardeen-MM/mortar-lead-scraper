#!/usr/bin/env node
/**
 * NSREC (Nova Scotia Real Estate Commission) Agent Scraper
 * URL: https://licensees.nsrec.ns.ca/search
 * Platform: Laravel (HTML forms, POST search, detail pages via GET)
 * Strategy: Search by vowels (a, e, i, o) to cover all agents, dedup by licensee ID
 *           Then fetch each detail page for phone/email/city
 * Estimated: ~2,300 agents
 */

const https = require('https');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'nsrec-real-estate-agents.csv');
const DELAY_MS = 500;
const DETAIL_DELAY_MS = 300;

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source'
];

// Vowels cover all names (every name has at least one vowel)
const SEARCH_TERMS = ['a', 'e', 'i', 'o'];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', ...headers }
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return httpGet(res.headers.location, headers).then(resolve).catch(reject);
      }
      let data = '';
      const cookies = res.headers['set-cookie'] || [];
      res.on('data', c => data += c);
      res.on('end', () => resolve({ data, cookies, status: res.statusCode }));
    }).on('error', reject);
  });
}

function httpPost(hostname, path, postData, headers = {}) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname, path, method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        ...headers
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ data, status: res.statusCode, headers: res.headers }));
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

function parseName(fullName) {
  // Remove nickname in parentheses: "William (Billy) Reid" -> "William Reid"
  let clean = fullName.replace(/\s*\([^)]*\)\s*/g, ' ').trim();
  clean = clean.replace(/\s+/g, ' ');

  const parts = clean.split(' ');
  if (parts.length === 1) {
    return { first_name: parts[0], last_name: '' };
  }

  const lastName = parts[parts.length - 1];
  const firstName = parts.slice(0, -1).join(' ');
  return { first_name: firstName, last_name: lastName };
}

function parseCity(addressText) {
  // Address format: "13273 Highway 3,\n Upper Lahave, NS B4V 7C4"
  if (!addressText) return '';
  const cityMatch = addressText.match(/\n\s*([A-Za-z\s'-]+),\s*NS/);
  if (cityMatch) return cityMatch[1].trim();
  return '';
}

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

async function searchByTerm(term) {
  // Get fresh session with CSRF token
  const searchPage = await httpGet('https://licensees.nsrec.ns.ca/search');
  const tokenMatch = searchPage.data.match(/name="_token".*?value="([^"]+)"/);
  if (!tokenMatch) throw new Error('Could not extract CSRF token');

  const token = tokenMatch[1];
  const cookieStr = searchPage.cookies.map(c => c.split(';')[0]).join('; ');

  const result = await httpPost('licensees.nsrec.ns.ca', '/search/licensees',
    '_token=' + encodeURIComponent(token) + '&surname=' + encodeURIComponent(term),
    {
      'Cookie': cookieStr,
      'Referer': 'https://licensees.nsrec.ns.ca/search'
    }
  );

  const $ = cheerio.load(result.data);
  const countMatch = result.data.match(/(\d+) results matching/);
  const count = countMatch ? parseInt(countMatch[1]) : 0;

  const agents = [];
  $('table.search-result-table tbody tr').each((i, tr) => {
    const tds = $(tr).find('td');

    // Brokerage
    const brokerageLink = $(tds[0]).find('a').first();
    const brokerageName = brokerageLink.text().trim();
    const brokerageId = (brokerageLink.attr('href') || '').match(/details\/(\d+)/);

    // Branch info (phone in listing)
    const branchDiv = $(tds[0]).find('.search-result-branch');
    let listingPhone = '';
    if (branchDiv.length) {
      const phoneMatch = branchDiv.text().match(/Phone:\s*([^\n]+)/);
      if (phoneMatch) listingPhone = phoneMatch[1].trim();
    }

    // Licensee
    const licenseeLink = $(tds[1]).find('a');
    const licenseeName = licenseeLink.text().trim();
    const licenseeUrl = licenseeLink.attr('href') || '';
    const idMatch = licenseeUrl.match(/details\/(\d+)/);
    const licenseeId = idMatch ? idMatch[1] : null;

    // Designation
    const designation = $(tds[2]).text().trim();

    if (licenseeId) {
      agents.push({
        id: licenseeId,
        name: licenseeName,
        brokerage: brokerageName,
        brokerageId: brokerageId ? brokerageId[1] : null,
        designation,
        listingPhone,
        detailUrl: licenseeUrl
      });
    }
  });

  return { count, agents };
}

async function fetchDetail(detailUrl) {
  try {
    const result = await httpGet(detailUrl);
    if (result.status !== 200) return null;

    const $ = cheerio.load(result.data);
    const data = {};

    $('table.search-result-table tbody tr').each((i, tr) => {
      const label = $(tr).find('td.text-bold').text().trim();
      const value = $(tr).find('td').eq(1).text().trim();
      if (label && value) data[label] = value;
    });

    return {
      phone: data['Brokerage Phone'] || '',
      email: data['Broker Email'] || '',
      address: data['Brokerage Address'] || '',
      city: parseCity(data['Brokerage Address'] || ''),
      brokerageFull: data['Full and Exact Legal Name of Brokerage'] || '',
      broker: data['Broker'] || ''
    };
  } catch (e) {
    console.error(`  Error fetching ${detailUrl}: ${e.message}`);
    return null;
  }
}

async function main() {
  console.log('=== NSREC Nova Scotia Real Estate Commission Scraper ===\n');

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // Phase 1: Collect all unique agents from search results
  console.log('Phase 1: Collecting agents from search results...');
  const allAgents = new Map(); // id -> agent data

  for (const term of SEARCH_TERMS) {
    process.stdout.write(`  Searching "${term}"...`);
    const { count, agents } = await searchByTerm(term);

    let newCount = 0;
    for (const agent of agents) {
      if (!allAgents.has(agent.id)) {
        allAgents.set(agent.id, agent);
        newCount++;
      }
    }

    console.log(` ${count} results, ${newCount} new (total: ${allAgents.size})`);
    await sleep(DELAY_MS);
  }

  console.log(`\nTotal unique agents: ${allAgents.size}`);

  // Phase 2: Fetch detail pages for phone/email/city
  console.log('\nPhase 2: Fetching detail pages for contact info...');
  const agents = Array.from(allAgents.values());
  let detailCount = 0;
  let emailCount = 0;
  let phoneCount = 0;

  for (let i = 0; i < agents.length; i++) {
    const agent = agents[i];
    if ((i + 1) % 50 === 0 || i === 0) {
      process.stdout.write(`  [${i + 1}/${agents.length}] Fetching details...`);
    }

    const detail = await fetchDetail(agent.detailUrl);
    if (detail) {
      agent.phone = detail.phone;
      agent.email = detail.email;
      agent.city = detail.city;
      agent.brokerageFull = detail.brokerageFull;
      detailCount++;

      if (detail.email && detail.email !== '—') emailCount++;
      if (detail.phone && detail.phone !== '—' && detail.phone.toLowerCase() !== 'no phone') phoneCount++;
    }

    if ((i + 1) % 50 === 0) {
      console.log(` done (${emailCount} emails, ${phoneCount} phones so far)`);
    }

    await sleep(DETAIL_DELAY_MS);
  }

  console.log(`\nDetail pages fetched: ${detailCount}/${agents.length}`);
  console.log(`Emails found: ${emailCount}`);
  console.log(`Phones found: ${phoneCount}`);

  // Phase 3: Write CSV
  console.log('\nPhase 3: Writing CSV...');
  const csvLines = [CSV_HEADERS.join(',')];

  for (const agent of agents) {
    const { first_name, last_name } = parseName(agent.name);
    const phone = (agent.phone && agent.phone !== '—' && agent.phone.toLowerCase() !== 'no phone')
      ? agent.phone : '';
    const email = (agent.email && agent.email !== '—') ? agent.email : '';
    const firmName = agent.brokerageFull || agent.brokerage || '';

    // Extract domain from email
    let domain = '';
    if (email) {
      const emailDomain = email.split('@')[1];
      if (emailDomain && !emailDomain.includes('gmail.com') && !emailDomain.includes('yahoo.com')
          && !emailDomain.includes('hotmail.com') && !emailDomain.includes('outlook.com')) {
        domain = emailDomain;
      }
    }

    const row = [
      escapeCsv(first_name),
      escapeCsv(last_name),
      escapeCsv(firmName),
      escapeCsv(agent.designation || ''),
      escapeCsv(email),
      escapeCsv(phone),
      '',  // website
      escapeCsv(domain),
      escapeCsv(agent.city || ''),
      'Nova Scotia',
      'CA',
      'real estate agent',
      'NSREC'
    ];
    csvLines.push(row.join(','));
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  console.log(`\nCSV written to ${OUTPUT_FILE}`);
  console.log(`Total rows: ${agents.length}`);

  // Stats
  const designations = {};
  const cities = {};
  const brokerages = {};
  for (const a of agents) {
    designations[a.designation] = (designations[a.designation] || 0) + 1;
    if (a.city) cities[a.city] = (cities[a.city] || 0) + 1;
    brokerages[a.brokerage] = (brokerages[a.brokerage] || 0) + 1;
  }

  console.log('\nDesignations:');
  Object.entries(designations).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(`  ${k}: ${v}`));
  console.log(`\nTop 10 Cities:`);
  Object.entries(cities).sort((a, b) => b[1] - a[1]).slice(0, 10).forEach(([k, v]) => console.log(`  ${k}: ${v}`));
  console.log(`\nTop 10 Brokerages:`);
  Object.entries(brokerages).sort((a, b) => b[1] - a[1]).slice(0, 10).forEach(([k, v]) => console.log(`  ${k}: ${v}`));
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
