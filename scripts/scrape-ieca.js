#!/usr/bin/env node
/**
 * IECA (Independent Educational Consultants Association) Scraper
 *
 * Scrapes the IECA member directory via WordPress REST API.
 * Endpoint: /wp-json/wp/v2/directory-entry?per_page=100
 *
 * Total: ~2,500+ education consultants
 * Data: name, email, phone, company, website, address, specialties, education, social
 *
 * Usage:
 *   node scripts/scrape-ieca.js              # Full scrape (US only)
 *   node scripts/scrape-ieca.js --all        # All countries
 *   node scripts/scrape-ieca.js --test       # First 2 pages only
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// === Configuration ===
const BASE_URL = 'https://www.iecaonline.com/wp-json/wp/v2/directory-entry';
const PER_PAGE = 100;
const DELAY_MS = 2500; // 2.5s between requests
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'us-education-consultants-ieca.csv');

// === Taxonomy mappings (fetched from API) ===
const CONSULTING_AREAS = {
  276: 'Business',
  269: 'College Admissions',
  290: 'Education',
  289: 'Engineering, Architecture, Technology',
  291: 'Fine Arts (Visual and/or Performing Arts)',
  268: 'Graduate School Admissions',
  288: 'Humanities, Social Sciences',
  271: 'International',
  309: 'International',
  311: 'International',
  313: 'International',
  273: 'K-12 School Admissions',
  277: 'Law',
  270: 'Learning Differences/Neurodiversity',
  310: 'Learning Differences/Neurodiversity',
  312: 'Learning Differences/Neurodiversity',
  314: 'Learning Differences/Neurodiversity',
  278: 'Medicine, Health, Veterinary',
  298: 'Other',
  287: 'Psychology, Social Work',
  286: 'Sciences',
  275: 'Therapeutic Advising',
};

const ADDITIONAL_ADVISING = {
  299: 'Career Counseling',
  343: 'Executive Function Support',
  333: 'Financial Aid Advising',
  300: 'First-Generation College-Bound Students',
  318: 'Gap Year Programs',
  301: 'Gifted/Talented or Twice-Exceptional Students',
  302: 'Homeschooled Students',
  303: 'K-12 Day Schools',
  327: 'LGBTQ+ Students',
  304: 'Mental Health/Anxiety Concerns',
  305: 'Military Schools and Service Academies',
  306: 'Online Education',
  332: 'STEM',
  319: 'Student-Athletes',
  307: 'Substance Use/Recovery Support',
  308: 'Summer Programs',
  331: 'Transfer Students',
  317: 'Visual/Performing Arts',
};

const MEMBERSHIP_TYPES = {
  266: 'Associate',
  272: 'Professional',
};

// US state name → abbreviation
const STATE_ABBREV = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'district of columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI',
  'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
  'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME',
  'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
  'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
  'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM',
  'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
  'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI',
  'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX',
  'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
  'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
};

// === Helper functions ===

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: data,
        });
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Request timed out'));
    });
  });
}

function extractDomain(website) {
  if (!website) return '';
  try {
    const url = new URL(website.startsWith('http') ? website : `https://${website}`);
    return url.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Strip non-digits
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) {
    const area = digits.substring(1, 4);
    const mid = digits.substring(4, 7);
    const last = digits.substring(7);
    return `(${area}) ${mid}-${last}`;
  }
  if (digits.length === 10) {
    const area = digits.substring(0, 3);
    const mid = digits.substring(3, 6);
    const last = digits.substring(6);
    return `(${area}) ${mid}-${last}`;
  }
  return phone.trim();
}

function stateAbbrev(stateName) {
  if (!stateName) return '';
  const lower = stateName.toLowerCase().trim();
  return STATE_ABBREV[lower] || stateName.trim();
}

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const str = String(val).replace(/\r?\n/g, ' ').trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function parseEntry(entry) {
  const acf = entry.acf || {};

  // Name
  const firstName = (acf['member-directory-first-name-to-use'] || acf['member-directory-first-name'] || '').trim();
  const lastName = (acf['member-directory-last-name'] || '').trim();
  const secondLast = (acf['member-directory-second-last-name'] || '').trim();
  const fullLastName = secondLast ? `${lastName} ${secondLast}` : lastName;

  // Contact
  const email = (acf['member-directory-primary-email'] || '').trim().toLowerCase();
  const phone = normalizePhone(acf['member-directory-primary-phone'] || '');
  const website = (acf['member-directory-website'] || '').trim();
  const domain = extractDomain(website);

  // Company
  const company = (acf['member-directory-company'] || '').trim();

  // Address — use first address
  const addresses = acf['member-directory-addresses'] || [];
  let city = '', state = '', zip = '', country = '', address = '';
  if (addresses && addresses.length > 0) {
    const addr = addresses[0];
    city = (addr['member-directory-city'] || '').trim();
    state = (addr['member-directory-state'] || '').trim();
    zip = (addr['member-directory-zip'] || '').trim();
    country = (addr['member-directory-country'] || '').trim();
    address = (addr['member-directory-address'] || '').trim();
  }

  // State abbreviation for US
  const stateCode = country === 'United States' ? stateAbbrev(state) : state;

  // Consulting areas
  const consultingAreas = (entry['consulting-area'] || [])
    .map(id => CONSULTING_AREAS[id])
    .filter(Boolean);
  // Deduplicate (e.g., multiple "International" entries)
  const uniqueConsulting = [...new Set(consultingAreas)];

  // Additional advising areas
  const additionalAreas = (entry['additional-advising-area'] || [])
    .map(id => ADDITIONAL_ADVISING[id])
    .filter(Boolean);

  // All specialties combined
  const allSpecialties = [...uniqueConsulting, ...additionalAreas];

  // Membership type
  const membershipTypes = (entry['membership-type'] || [])
    .map(id => MEMBERSHIP_TYPES[id])
    .filter(Boolean);
  const membershipType = membershipTypes.join(', ');

  // Education
  const education = [
    acf['member-directory-bachelors-degree-institution'] ? `BA: ${acf['member-directory-bachelors-degree-institution']}` : '',
    acf['member-directory-masters-degree-institution'] ? `MA: ${acf['member-directory-masters-degree-institution']}` : '',
    acf['member-directory-doctoral-degree-institution'] ? `PhD: ${acf['member-directory-doctoral-degree-institution']}` : '',
    acf['member-directory-certificate-degree-institution'] ? `Cert: ${acf['member-directory-certificate-degree-institution']}` : '',
    acf['member-directory-other-postgraduate-degree-institution'] ? `Other: ${acf['member-directory-other-postgraduate-degree-institution']}` : '',
  ].filter(Boolean).join('; ');

  // Social
  const linkedin = (acf['member-directory-linkedin'] || '').trim();
  const facebook = (acf['member-directory-facebook'] || '').trim();
  const instagram = (acf['member-directory-instagram'] || '').trim();
  const twitter = (acf['member-directory-x-twitter'] || '').trim();

  // Profile URL
  const profileUrl = entry.link || '';

  return {
    first_name: firstName,
    last_name: fullLastName,
    firm_name: company,
    title: membershipType ? `${membershipType} Member` : 'Member',
    email,
    phone,
    website,
    domain,
    address,
    city,
    state: stateCode,
    zip,
    country,
    niche: 'Education Consultant',
    source: 'ieca',
    profile_url: profileUrl,
    specialties: allSpecialties.join('; '),
    education,
    linkedin,
    membership_type: membershipType,
  };
}

// === Main scraper ===

async function scrapeIECA(options = {}) {
  const { testMode = false, allCountries = false } = options;
  const maxPages = testMode ? 2 : 999;

  console.log('=== IECA Member Directory Scraper ===');
  console.log(`Mode: ${testMode ? 'TEST (2 pages)' : 'FULL'}`);
  console.log(`Filter: ${allCountries ? 'All countries' : 'US only'}`);
  console.log(`Delay: ${DELAY_MS}ms between requests`);
  console.log('');

  const allLeads = [];
  let page = 1;
  let totalEntries = 0;
  let totalPages = 0;

  while (page <= maxPages) {
    const url = `${BASE_URL}?per_page=${PER_PAGE}&page=${page}`;
    console.log(`Fetching page ${page}${totalPages ? `/${totalPages}` : ''}...`);

    try {
      const resp = await httpGet(url);

      if (resp.statusCode === 400) {
        // Past last page
        console.log('  Reached end of results (400 response).');
        break;
      }

      if (resp.statusCode !== 200) {
        console.error(`  HTTP ${resp.statusCode} — retrying in 10s...`);
        await sleep(10000);
        continue;
      }

      // Parse headers for totals (first page)
      if (page === 1) {
        totalEntries = parseInt(resp.headers['x-wp-total'], 10) || 0;
        totalPages = parseInt(resp.headers['x-wp-totalpages'], 10) || 0;
        console.log(`  Total entries: ${totalEntries} across ${totalPages} pages`);
      }

      const entries = JSON.parse(resp.body);
      if (!entries.length) {
        console.log('  No entries returned — done.');
        break;
      }

      let pageUS = 0;
      let pageOther = 0;

      for (const entry of entries) {
        const lead = parseEntry(entry);

        // Filter: US only unless --all
        if (!allCountries && lead.country !== 'United States') {
          pageOther++;
          continue;
        }

        allLeads.push(lead);
        pageUS++;
      }

      console.log(`  Parsed ${entries.length} entries (${pageUS} kept, ${pageOther} filtered)`);

      page++;

      // Don't delay after the last page
      if (page <= (testMode ? maxPages : totalPages)) {
        await sleep(DELAY_MS);
      }

    } catch (err) {
      console.error(`  Error on page ${page}: ${err.message}`);
      console.log('  Retrying in 10s...');
      await sleep(10000);
    }
  }

  console.log('');
  console.log(`=== Scraping complete ===`);
  console.log(`Total leads: ${allLeads.length}`);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withBoth = allLeads.filter(l => l.email && l.phone).length;

  console.log(`With email: ${withEmail} (${(100 * withEmail / allLeads.length).toFixed(1)}%)`);
  console.log(`With phone: ${withPhone} (${(100 * withPhone / allLeads.length).toFixed(1)}%)`);
  console.log(`With website: ${withWebsite} (${(100 * withWebsite / allLeads.length).toFixed(1)}%)`);
  console.log(`With email+phone: ${withBoth} (${(100 * withBoth / allLeads.length).toFixed(1)}%)`);

  // State breakdown
  const stateCounts = {};
  for (const l of allLeads) {
    const st = l.state || '(no state)';
    stateCounts[st] = (stateCounts[st] || 0) + 1;
  }
  const sorted = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
  console.log('\nTop states:');
  for (const [st, count] of sorted.slice(0, 15)) {
    console.log(`  ${st}: ${count}`);
  }

  // Write CSV
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const CSV_COLUMNS = [
    'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
    'website', 'domain', 'address', 'city', 'state', 'zip', 'country',
    'niche', 'source', 'profile_url', 'specialties', 'education',
    'linkedin', 'membership_type',
  ];

  const header = CSV_COLUMNS.join(',');
  const rows = allLeads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col])).join(',')
  );

  const csv = [header, ...rows].join('\n');
  fs.writeFileSync(OUTPUT_FILE, csv, 'utf-8');
  console.log(`\nCSV written: ${OUTPUT_FILE}`);
  console.log(`Rows: ${rows.length}`);

  return allLeads;
}

// === CLI ===
const args = process.argv.slice(2);
const testMode = args.includes('--test');
const allCountries = args.includes('--all');

scrapeIECA({ testMode, allCountries }).catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
