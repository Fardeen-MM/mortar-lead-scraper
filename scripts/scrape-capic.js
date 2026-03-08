#!/usr/bin/env node
/**
 * Scrape CAPIC (Canadian Association of Professional Immigration Consultants)
 * member directory + MyConsultant.ca profiles.
 *
 * Sources:
 *   - https://www.capic.ca/EN/ActiveMembersList  (3,700+ active members, name/city/province)
 *   - https://www.myconsultant.ca/Partial/ConsultantsList  (CICC IDs, CDN profile IDs)
 *   - https://www.myconsultant.ca/EN/Consultants?name=X&cdn=Y  (phone, email, firm, address)
 *
 * Strategy:
 *   1. Fetch CAPIC active members list (single HTML table, all members)
 *   2. Fetch MyConsultant.ca full listing (all members with CICC IDs + CDN IDs)
 *   3. Match CAPIC members to MyConsultant entries by name
 *   4. Scrape individual MyConsultant profile pages for contact details
 *
 * Usage:
 *   node scripts/scrape-capic.js
 *   node scripts/scrape-capic.js --skip-profiles        # Skip profile scraping (list only)
 *   node scripts/scrape-capic.js --resume                # Resume from progress file
 *   node scripts/scrape-capic.js --delay=3000            # Custom delay (ms)
 *   node scripts/scrape-capic.js --limit=100             # Limit profiles to scrape
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// --- CLI args ---
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 2500;
const PROFILE_LIMIT = parseInt(args.limit) || 0;
const SKIP_PROFILES = !!args['skip-profiles'];
const RESUME = !!args.resume;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'canada-capic-immigration-consultants.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'capic-progress.csv');

// --- HTTP helpers ---
function fetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        ...options.headers,
      },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirect = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).toString();
        fetch(redirect, options).then(resolve).catch(reject);
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function postForm(url, formData) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const body = typeof formData === 'string' ? formData
      : Object.entries(formData).map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');

    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.myconsultant.ca',
        'Referer': 'https://www.myconsultant.ca/EN/Consultants',
      },
    };

    const mod = parsed.protocol === 'https:' ? https : http;
    const req = mod.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(body);
    req.end();
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// --- Simple HTML parser (no cheerio dependency) ---
function extractTableRows(html) {
  const rows = [];
  const tbodyMatch = html.match(/<tbody>([\s\S]*?)<\/tbody>/i);
  if (!tbodyMatch) return rows;

  const tbody = tbodyMatch[1];
  const trRegex = /<tr>([\s\S]*?)<\/tr>/gi;
  let trMatch;
  while ((trMatch = trRegex.exec(tbody)) !== null) {
    const cells = [];
    const tdRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let tdMatch;
    while ((tdMatch = tdRegex.exec(trMatch[1])) !== null) {
      cells.push(tdMatch[1].trim().replace(/&amp;/g, '&').replace(/&#x27;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>'));
    }
    if (cells.length >= 4) {
      rows.push({
        first_name: cells[0],
        last_name: cells[1],
        city: cells[2],
        province: cells[3],
        country: cells[4] || 'Canada',
      });
    }
  }
  return rows;
}

function extractMyConsultantCards(html) {
  const cards = [];
  const cardRegex = /<div class="consultants-item">([\s\S]*?)<\/div>\s*<!-- \.consultants-item -->/gi;
  let cardMatch;
  while ((cardMatch = cardRegex.exec(html)) !== null) {
    const card = cardMatch[1];

    // Extract name + profile URL
    const nameMatch = card.match(/<a class="consultant-name" href="([^"]*)">\s*([\s\S]*?)\s*<\/a>/i);
    const ciccMatch = card.match(/CICC ID:\s*<\/span>([^<]+)/i);

    if (!nameMatch) continue;

    const profileHref = nameMatch[1].trim();
    const name = nameMatch[2].trim().replace(/\s+/g, ' ');

    // Extract CDN ID from URL: ?name=X&cdn=Y
    const cdnMatch = profileHref.match(/cdn=(\d+)/);
    const cdn = cdnMatch ? cdnMatch[1] : null;

    // Extract CICC ID
    const ciccId = ciccMatch ? ciccMatch[1].trim() : '';

    // Extract location
    const locMatch = card.match(/Location:\s*<\/span>([^<]+)/i);
    const location = locMatch ? locMatch[1].trim() : '';

    // Extract specialty
    const specMatch = card.match(/Specialty:\s*<\/span>([^<]*)/i);
    const specialty = specMatch ? specMatch[1].trim() : '';

    // Extract languages
    const langMatch = card.match(/Language\(s\):\s*<\/span>([^<]*)/i);
    const languages = langMatch ? langMatch[1].trim().replace(/,\s*,/g, ',').replace(/,\s*$/, '').trim() : '';

    cards.push({ name, cdn, ciccId, location, specialty, languages, profileHref });
  }
  return cards;
}

function parseProfilePage(html) {
  const result = {};

  // Firm name from h1.consultant-heading
  const firmMatch = html.match(/<h1 class="consultant-heading">([\s\S]*?)<\/h1>/i);
  if (firmMatch) {
    result.firm_name = firmMatch[1].trim().replace(/&amp;/g, '&').replace(/&#x27;/g, "'");
  }

  // Phone from list-phone
  const phoneMatch = html.match(/<li class="list-phone">\s*<a href="tel:([^"]+)"/i);
  if (phoneMatch) {
    result.phone = phoneMatch[1].trim();
  }

  // Email from list-mail
  const emailMatch = html.match(/<li class="list-mail">\s*<a href="mailto:([^"]+)"/i);
  if (emailMatch) {
    result.email = emailMatch[1].trim().toLowerCase();
  }

  // Website from list-web or list-link
  const webMatch = html.match(/<li class="list-(?:web|link)">\s*<a href="([^"]+)"/i);
  if (webMatch) {
    result.website = webMatch[1].trim();
  }

  // Address from consultant-sidebar-paragraph
  const addrMatch = html.match(/<p class="consultant-sidebar-paragraph" style="font-weight: bold;">\s*([\s\S]*?)\s*<\/p>/i);
  if (addrMatch) {
    const addrText = addrMatch[1].replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '').trim();
    result.address = addrText;

    // Parse city + province from address lines
    const lines = addrText.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length >= 2) {
      // Last line might be "Canada L3R 6G2" or "Canada"
      // Second-to-last is often "City, Province"
      const cityProvLine = lines.length >= 3 ? lines[1] : lines[0];
      const cityProvMatch = cityProvLine.match(/^([^,]+),\s*(.+)$/);
      if (cityProvMatch) {
        result.profile_city = cityProvMatch[1].trim();
        result.profile_province = cityProvMatch[2].trim();
      }
    }
  }

  // Title from default-heading-sub span
  const titleMatch = html.match(/<div class="default-heading-sub">\s*<span>([^<]+)<\/span>/i);
  if (titleMatch) {
    result.title = titleMatch[1].trim();
  }

  // LinkedIn
  const linkedinMatch = html.match(/href="(https?:\/\/(?:www\.)?linkedin\.com\/[^"]+)"/i);
  if (linkedinMatch) {
    result.linkedin = linkedinMatch[1];
  }

  return result;
}

// --- Province name → code mapping ---
const PROVINCE_MAP = {
  'alberta': 'AB', 'british columbia': 'BC', 'manitoba': 'MB',
  'new brunswick': 'NB', 'newfoundland and labrador': 'NL', 'newfoundland': 'NL',
  'northwest territories': 'NT', 'nova scotia': 'NS', 'nunavut': 'NU',
  'ontario': 'ON', 'prince edward island': 'PE', 'quebec': 'QC',
  'saskatchewan': 'SK', 'yukon': 'YT',
};

function provinceCode(name) {
  if (!name) return '';
  // Already a code?
  if (name.length <= 3 && name === name.toUpperCase()) return name;
  return PROVINCE_MAP[name.toLowerCase()] || name;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const hostname = new URL(url.startsWith('http') ? url : 'https://' + url).hostname;
    return hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return phone;
}

// --- CSV writer ---
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email',
  'phone', 'website', 'domain', 'city', 'state', 'country',
  'niche', 'source', 'profile_url', 'cicc_id', 'specialty', 'languages',
];

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

function loadProgressCSV(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.trim().split('\n');
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const leads = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSVLine(lines[i]);
    const lead = {};
    headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
    leads.push(lead);
  }
  return leads;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current);
  return result;
}

// --- Name normalization for matching ---
function normalizeName(name) {
  return (name || '').toLowerCase().replace(/[^a-z\s]/g, '').replace(/\s+/g, ' ').trim();
}

// --- Main ---
async function main() {
  const startTime = Date.now();
  console.log('\n====================================================================');
  console.log('  MORTAR -- CAPIC Immigration Consultants Scraper');
  console.log('====================================================================\n');

  // =====================================================================
  // PHASE 1: Fetch CAPIC active members list
  // =====================================================================
  console.log('  [Phase 1] Fetching CAPIC active members list...');
  const capicResp = await fetch('https://www.capic.ca/EN/ActiveMembersList');
  if (capicResp.status !== 200) {
    console.error(`  FATAL: CAPIC returned status ${capicResp.status}`);
    process.exit(1);
  }

  const capicMembers = extractTableRows(capicResp.body);
  console.log(`  Found ${capicMembers.length} active CAPIC members\n`);

  // =====================================================================
  // PHASE 2: Fetch MyConsultant.ca full listing (for CICC IDs + CDN IDs)
  // =====================================================================
  console.log('  [Phase 2] Fetching MyConsultant.ca consultant list...');
  const mcResp = await postForm('https://www.myconsultant.ca/Partial/ConsultantsList', {
    language: 'EN',
    keyword: '',
    location: '',
    spokenlang: '',
    service: '',
  });

  if (mcResp.status !== 200) {
    console.error(`  WARN: MyConsultant API returned status ${mcResp.status}`);
    console.log('  Proceeding with CAPIC data only...\n');
  }

  const mcCards = mcResp.status === 200 ? extractMyConsultantCards(mcResp.body) : [];
  const totalCountMatch = mcResp.body.match(/value="(\d+)"\s+id="hdnConsultantsCount"/i);
  const totalReported = totalCountMatch ? parseInt(totalCountMatch[1]) : mcCards.length;
  console.log(`  MyConsultant API reports ${totalReported} consultants`);
  console.log(`  Parsed ${mcCards.length} consultant cards\n`);

  // =====================================================================
  // PHASE 3: Build name-matching index and merge data
  // =====================================================================
  console.log('  [Phase 3] Matching CAPIC members to MyConsultant profiles...');

  // Build lookup by normalized name
  const mcByName = new Map();
  for (const card of mcCards) {
    const key = normalizeName(card.name);
    if (!mcByName.has(key)) mcByName.set(key, []);
    mcByName.get(key).push(card);
  }

  // Merge CAPIC members with MyConsultant data
  const allLeads = [];
  let matched = 0;
  let unmatched = 0;

  for (const member of capicMembers) {
    const fullName = `${member.first_name} ${member.last_name}`;
    const key = normalizeName(fullName);

    const lead = {
      first_name: member.first_name,
      last_name: member.last_name,
      firm_name: '',
      title: '',
      email: '',
      phone: '',
      website: '',
      domain: '',
      city: member.city,
      state: provinceCode(member.province),
      country: member.country === 'Canada' ? 'CA' : member.country,
      niche: 'Immigration Consultant',
      source: 'capic',
      profile_url: '',
      cicc_id: '',
      specialty: '',
      languages: '',
    };

    // Try to match with MyConsultant data
    const mcMatches = mcByName.get(key);
    if (mcMatches && mcMatches.length > 0) {
      // If multiple matches, prefer one with matching city
      let bestMatch = mcMatches[0];
      if (mcMatches.length > 1) {
        const cityNorm = (member.city || '').toLowerCase();
        const cityMatch = mcMatches.find(m => m.location.toLowerCase().includes(cityNorm));
        if (cityMatch) bestMatch = cityMatch;
      }

      lead.cicc_id = bestMatch.ciccId;
      lead.specialty = bestMatch.specialty;
      lead.languages = bestMatch.languages;
      lead.profile_url = `https://www.myconsultant.ca/EN/Consultants?name=${encodeURIComponent(bestMatch.name.replace(/\s+/g, '+'))}&cdn=${bestMatch.cdn}`;
      lead._cdn = bestMatch.cdn;
      lead._mcName = bestMatch.name;
      matched++;
    } else {
      unmatched++;
    }

    allLeads.push(lead);
  }

  console.log(`  Matched: ${matched}/${capicMembers.length} (${Math.round(matched / capicMembers.length * 100)}%)`);
  console.log(`  Unmatched: ${unmatched}\n`);

  // Also add MyConsultant-only members (not in CAPIC list)
  const capicNames = new Set(capicMembers.map(m => normalizeName(`${m.first_name} ${m.last_name}`)));
  let mcOnly = 0;
  for (const card of mcCards) {
    const key = normalizeName(card.name);
    if (!capicNames.has(key)) {
      // Split name into first/last
      const parts = card.name.trim().split(/\s+/);
      const lastName = parts.pop();
      const firstName = parts.join(' ');

      // Parse city from location
      const locParts = card.location.split(',').map(s => s.trim());
      const city = locParts[0] || '';
      const country = locParts.length > 1 ? locParts[locParts.length - 1] : 'Canada';

      allLeads.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        title: '',
        email: '',
        phone: '',
        website: '',
        domain: '',
        city: city,
        state: '',
        country: country === 'Canada' ? 'CA' : country,
        niche: 'Immigration Consultant',
        source: 'capic',
        profile_url: `https://www.myconsultant.ca/EN/Consultants?name=${encodeURIComponent(card.name.replace(/\s+/g, '+'))}&cdn=${card.cdn}`,
        cicc_id: card.ciccId,
        specialty: card.specialty,
        languages: card.languages,
        _cdn: card.cdn,
        _mcName: card.name,
      });
      mcOnly++;
    }
  }
  if (mcOnly > 0) {
    console.log(`  Added ${mcOnly} MyConsultant-only members (not in CAPIC list)\n`);
  }

  // =====================================================================
  // PHASE 4: Scrape individual profiles for contact details
  // =====================================================================
  if (SKIP_PROFILES) {
    console.log('  [Phase 4] SKIPPED (--skip-profiles flag)\n');
  } else {
    console.log('  [Phase 4] Scraping individual MyConsultant profiles...');

    // Filter to leads with profile URLs
    const profileLeads = allLeads.filter(l => l.profile_url && l._cdn);

    // Resume support: load existing progress
    let scraped = new Set();
    if (RESUME && fs.existsSync(PROGRESS_FILE)) {
      const existing = loadProgressCSV(PROGRESS_FILE);
      for (const lead of existing) {
        if (lead.profile_url) scraped.add(lead.profile_url);
        // Merge existing data back
        const idx = allLeads.findIndex(l => l.profile_url === lead.profile_url);
        if (idx >= 0) {
          if (lead.firm_name) allLeads[idx].firm_name = lead.firm_name;
          if (lead.phone) allLeads[idx].phone = lead.phone;
          if (lead.email) allLeads[idx].email = lead.email;
          if (lead.website) allLeads[idx].website = lead.website;
          if (lead.domain) allLeads[idx].domain = lead.domain;
          if (lead.title) allLeads[idx].title = lead.title;
          allLeads[idx]._scraped = true;
        }
      }
      console.log(`  Resuming: ${scraped.size} profiles already scraped\n`);
    }

    const toScrape = profileLeads.filter(l => !l._scraped);
    const limit = PROFILE_LIMIT > 0 ? Math.min(PROFILE_LIMIT, toScrape.length) : toScrape.length;
    const estMinutes = Math.round(limit * DELAY_MS / 1000 / 60);

    console.log(`  Profiles to scrape: ${limit}/${toScrape.length}`);
    console.log(`  Delay: ${DELAY_MS}ms between requests`);
    console.log(`  Estimated time: ~${estMinutes} minutes\n`);

    let profilesScraped = 0;
    let profileErrors = 0;
    let withEmail = 0;
    let withPhone = 0;
    let withFirm = 0;
    let consecutiveErrors = 0;

    for (let i = 0; i < limit; i++) {
      const lead = toScrape[i];
      profilesScraped++;

      try {
        const resp = await fetch(lead.profile_url);

        if (resp.status === 200 && resp.body.length > 500) {
          const profile = parseProfilePage(resp.body);

          if (profile.firm_name) { lead.firm_name = profile.firm_name; withFirm++; }
          if (profile.phone) { lead.phone = formatPhone(profile.phone); withPhone++; }
          if (profile.email) { lead.email = profile.email; withEmail++; }
          if (profile.website) {
            lead.website = profile.website;
            lead.domain = extractDomain(profile.website);
          }
          if (profile.title) lead.title = profile.title;
          // Update city/province from profile if available and more detailed
          if (profile.profile_city && !lead.city) lead.city = profile.profile_city;
          if (profile.profile_province) lead.state = provinceCode(profile.profile_province);

          consecutiveErrors = 0;
        } else {
          profileErrors++;
          consecutiveErrors++;
        }
      } catch (err) {
        profileErrors++;
        consecutiveErrors++;

        if (consecutiveErrors >= 10) {
          console.log(`\n  [PAUSE] ${consecutiveErrors} consecutive errors: ${err.message}`);
          console.log('  Waiting 30 seconds...');
          await sleep(30000);
          consecutiveErrors = 0;
        }
      }

      // Progress logging
      if (profilesScraped % 50 === 0 || profilesScraped === limit) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const pct = (profilesScraped / limit * 100).toFixed(1);
        const rate = (profilesScraped / (elapsed / 60)).toFixed(0);
        const remaining = Math.round((limit - profilesScraped) * DELAY_MS / 1000 / 60);
        console.log(`  [${pct}%] ${profilesScraped}/${limit} profiles | ${withEmail} email, ${withPhone} phone, ${withFirm} firm | ${rate}/min | ~${remaining}min left`);
      }

      // Save progress every 100 profiles
      if (profilesScraped % 100 === 0) {
        writeCSV(PROGRESS_FILE, allLeads.map(l => {
          const clean = { ...l };
          delete clean._cdn;
          delete clean._mcName;
          delete clean._scraped;
          return clean;
        }));
      }

      await sleep(DELAY_MS);
    }

    console.log(`\n  Profile scraping complete:`);
    console.log(`    Scraped: ${profilesScraped}`);
    console.log(`    Errors:  ${profileErrors}`);
    console.log(`    Email:   ${withEmail} (${(withEmail / profilesScraped * 100).toFixed(1)}%)`);
    console.log(`    Phone:   ${withPhone} (${(withPhone / profilesScraped * 100).toFixed(1)}%)`);
    console.log(`    Firm:    ${withFirm} (${(withFirm / profilesScraped * 100).toFixed(1)}%)\n`);
  }

  // =====================================================================
  // PHASE 5: Clean up and write final CSV
  // =====================================================================
  console.log('  [Phase 5] Writing output CSV...');

  // Clean up internal fields
  const finalLeads = allLeads.map(l => {
    const clean = { ...l };
    delete clean._cdn;
    delete clean._mcName;
    delete clean._scraped;

    // Ensure country code
    if (clean.country === 'Canada') clean.country = 'CA';

    // Extract domain from email if no website
    if (!clean.domain && clean.email) {
      const emailDomain = clean.email.split('@')[1];
      if (emailDomain && !emailDomain.match(/gmail|yahoo|hotmail|outlook|live|icloud/)) {
        clean.domain = emailDomain;
      }
    }

    return clean;
  });

  writeCSV(OUTPUT_FILE, finalLeads);

  // Also save progress file
  writeCSV(PROGRESS_FILE, finalLeads);

  // =====================================================================
  // Summary
  // =====================================================================
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = finalLeads.filter(l => l.email).length;
  const totalPhones = finalLeads.filter(l => l.phone).length;
  const totalFirms = finalLeads.filter(l => l.firm_name).length;
  const totalWebsites = finalLeads.filter(l => l.website).length;
  const totalCicc = finalLeads.filter(l => l.cicc_id).length;

  // Province breakdown
  const byProvince = {};
  for (const l of finalLeads) {
    const prov = l.state || 'Unknown';
    byProvince[prov] = (byProvince[prov] || 0) + 1;
  }
  const provinceList = Object.entries(byProvince).sort((a, b) => b[1] - a[1]);

  // Country breakdown
  const byCountry = {};
  for (const l of finalLeads) {
    const c = l.country || 'Unknown';
    byCountry[c] = (byCountry[c] || 0) + 1;
  }

  console.log('\n====================================================================');
  console.log('       CAPIC SCRAPE COMPLETE');
  console.log('====================================================================');
  console.log(`  Total leads:     ${finalLeads.length}`);
  console.log(`  With CICC ID:    ${totalCicc} (${Math.round(totalCicc / finalLeads.length * 100)}%)`);
  console.log(`  With email:      ${totalEmails} (${Math.round(totalEmails / finalLeads.length * 100)}%)`);
  console.log(`  With phone:      ${totalPhones} (${Math.round(totalPhones / finalLeads.length * 100)}%)`);
  console.log(`  With firm:       ${totalFirms} (${Math.round(totalFirms / finalLeads.length * 100)}%)`);
  console.log(`  With website:    ${totalWebsites} (${Math.round(totalWebsites / finalLeads.length * 100)}%)`);
  console.log(`  Errors:          ${0}`);
  console.log(`  Time:            ${elapsed}s (${Math.round(elapsed / 60)}min)`);
  console.log('--------------------------------------------------------------------');
  console.log('  Top provinces:');
  for (const [prov, count] of provinceList.slice(0, 10)) {
    console.log(`    ${prov.padEnd(25)} ${count}`);
  }
  console.log('  Countries:');
  for (const [country, count] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${country.padEnd(25)} ${count}`);
  }
  console.log('--------------------------------------------------------------------');
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log('====================================================================\n');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
