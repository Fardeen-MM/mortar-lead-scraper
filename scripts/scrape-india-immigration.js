#!/usr/bin/env node
/**
 * Scrape India's eMigrate Portal — Registry of Licensed Recruiting Agents
 *
 * Source: MEA PDF — "District and State wise list of Active RA (Recruiting Agents)"
 * URL:    https://www.mea.gov.in/Images/attach/03-List-4-2024.pdf
 *
 * The eMigrate web portal (emigrate.gov.in) is a JavaScript SPA with authenticated APIs,
 * so we parse the official MEA PDF which contains the complete registry.
 *
 * Extracts: agent name, registration number, firm name, address, city, state,
 *           phone, email, website, authorized signatory
 *
 * Output: output/india-recruiting-agents.csv
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const pdfParse = require('pdf-parse');

// ── Config ──────────────────────────────────────────────────────────────────────

const PDF_URL = 'https://www.mea.gov.in/Images/attach/03-List-4-2024.pdf';
const PDF_CACHE = path.join(__dirname, '..', 'data', 'india-ra-list.pdf');
const OUTPUT_FILE = path.join(__dirname, '..', 'output', 'india-recruiting-agents.csv');

const COUNTRY = 'IN';
const NICHE = 'Recruiting Agent';
const SOURCE = 'india-emigrate';

// Map PDF state names (no spaces) → proper state names
const STATE_MAP = {
  'ANDHRAPRADESH': 'Andhra Pradesh',
  'ARUNACHALPRADESH': 'Arunachal Pradesh',
  'ASSAM': 'Assam',
  'BIHAR': 'Bihar',
  'CHANDIGARH': 'Chandigarh',
  'CHHATTISGARH': 'Chhattisgarh',
  'DADRAANDNAGARHAVELI': 'Dadra and Nagar Haveli',
  'DELHI': 'Delhi',
  'GOA': 'Goa',
  'GUJARAT': 'Gujarat',
  'HARYANA': 'Haryana',
  'HIMACHALPRADESH': 'Himachal Pradesh',
  'JAMMUANDKASHMIR': 'Jammu and Kashmir',
  'JAMMU&KASHMIR': 'Jammu and Kashmir',
  'JAMMU&KASHMIR': 'Jammu and Kashmir',
  'JHARKHAND': 'Jharkhand',
  'KARNATAKA': 'Karnataka',
  'KERALA': 'Kerala',
  'MADHYAPRADESH': 'Madhya Pradesh',
  'MAHARASHTRA': 'Maharashtra',
  'MANIPUR': 'Manipur',
  'MEGHALAYA': 'Meghalaya',
  'MIZORAM': 'Mizoram',
  'NAGALAND': 'Nagaland',
  'ODISHA': 'Odisha',
  'PUDUCHERRY': 'Puducherry',
  'PUNJAB': 'Punjab',
  'RAJASTHAN': 'Rajasthan',
  'SIKKIM': 'Sikkim',
  'TAMILNADU': 'Tamil Nadu',
  'TELANGANA': 'Telangana',
  'TRIPURA': 'Tripura',
  'UTTARPRADESH': 'Uttar Pradesh',
  'UTTARAKHAND': 'Uttarakhand',
  'WESTBENGAL': 'West Bengal',
};

// ── CSV Writer ──────────────────────────────────────────────────────────────────

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url', 'registration_number', 'address', 'district',
  'authorized_signatory', 'rc_number',
];

function escapeCSV(val) {
  const s = (val || '').toString().trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

// ── PDF Downloader ──────────────────────────────────────────────────────────────

function downloadPDF(url, dest) {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    // Use cached version if less than 7 days old
    if (fs.existsSync(dest)) {
      const stat = fs.statSync(dest);
      const ageMs = Date.now() - stat.mtimeMs;
      if (ageMs < 7 * 24 * 60 * 60 * 1000) {
        console.log('  Using cached PDF (less than 7 days old)');
        return resolve(dest);
      }
    }

    console.log('  Downloading PDF from MEA...');
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        file.close();
        return downloadPDF(res.headers.location, dest).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        file.close();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const total = parseInt(res.headers['content-length'], 10);
      let downloaded = 0;
      res.on('data', (chunk) => {
        downloaded += chunk.length;
        if (total) {
          const pct = ((downloaded / total) * 100).toFixed(0);
          process.stdout.write(`\r  Downloaded: ${(downloaded / 1024).toFixed(0)} KB / ${(total / 1024).toFixed(0)} KB (${pct}%)`);
        }
      });
      res.pipe(file);
      file.on('finish', () => {
        file.close();
        console.log('');
        resolve(dest);
      });
    }).on('error', (err) => {
      file.close();
      fs.unlinkSync(dest);
      reject(err);
    });
  });
}

// ── Name Parser ─────────────────────────────────────────────────────────────────

function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|\s|\/|-)\S/g, m => m.toUpperCase());
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  const parts = fullName.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: titleCase(parts[0]), last_name: '' };
  return {
    first_name: titleCase(parts[0]),
    last_name: titleCase(parts.slice(1).join(' ')),
  };
}

// ── Phone Normalizer ────────────────────────────────────────────────────────────

function normalizePhone(raw) {
  if (!raw) return '';
  // Extract just the digits
  let digits = raw.replace(/[^\d]/g, '');
  // Strip leading country code: 0091 or 91
  if (digits.startsWith('0091')) digits = digits.substring(4);
  else if (digits.startsWith('91') && digits.length > 10) digits = digits.substring(2);
  // Strip leading 0 from STD code (e.g., 0877 → 877)
  if (digits.startsWith('0') && digits.length > 10) digits = digits.substring(1);
  // Must have at least 10 digits for an Indian number
  if (digits.length < 10) return '';
  // Format as +91-XXXX-XXXXXX or +91-XXXXX-XXXXX
  if (digits.length === 10) {
    // Mobile: +91-XXXXX-XXXXX
    return `+91-${digits.substring(0, 5)}-${digits.substring(5)}`;
  }
  // Landline with STD: +91-XXX-XXXXXXX or similar
  return `+91-${digits}`;
}

// ── Domain Extractor ────────────────────────────────────────────────────────────

function extractDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    return u.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

// ── Extract City from Address ───────────────────────────────────────────────────

// Known major city names often found in addresses
const KNOWN_CITIES = new Set([
  'MUMBAI', 'DELHI', 'BANGALORE', 'BENGALURU', 'CHENNAI', 'KOLKATA', 'HYDERABAD',
  'PUNE', 'AHMEDABAD', 'JAIPUR', 'LUCKNOW', 'KANPUR', 'NAGPUR', 'INDORE',
  'THANE', 'BHOPAL', 'VISAKHAPATNAM', 'PATNA', 'VADODARA', 'GHAZIABAD',
  'LUDHIANA', 'AGRA', 'NASHIK', 'FARIDABAD', 'MEERUT', 'RAJKOT', 'VARANASI',
  'SRINAGAR', 'KOCHI', 'COCHIN', 'COIMBATORE', 'MADURAI', 'CHANDIGARH',
  'GURGAON', 'GURUGRAM', 'NOIDA', 'TRIVANDRUM', 'THIRUVANANTHAPURAM',
  'VIJAYAWADA', 'MYSORE', 'MYSURU', 'RANCHI', 'JODHPUR', 'RAIPUR',
  'KOTA', 'GUWAHATI', 'TIRUPATI', 'SALEM', 'TIRUCHIRAPPALLI', 'BHUBANESWAR',
  'DEHRADUN', 'AMRITSAR', 'ALLAHABAD', 'PRAYAGRAJ', 'JALANDHAR', 'AURANGABAD',
  'CALICUT', 'KOZHIKODE', 'THRISSUR', 'ERNAKULAM', 'MALAPPURAM', 'PALAKKAD',
  'GWALIOR', 'JABALPUR', 'UDAIPUR', 'AJMER', 'BIKANER', 'BHILWARA',
  'NAVI MUMBAI', 'PANAJI', 'MARGAO', 'VASCO', 'JAMMU', 'SHIMLA',
  'PONDICHERRY', 'TIRUNELVELI', 'TIRUR', 'KAKINADA', 'GUNTUR', 'KURNOOL',
]);

function extractCity(address, district) {
  // First: check if address contains a known major city
  if (address) {
    const upper = address.toUpperCase();
    for (const city of KNOWN_CITIES) {
      if (upper.includes(city)) return titleCase(city);
    }
  }

  // Fallback: use district name as city (most reliable from PDF)
  return district ? titleCase(district) : '';
}

// ── PDF Parser ──────────────────────────────────────────────────────────────────

function parseEntries(text) {
  const leads = [];
  const lines = text.split('\n');

  // Remove page headers and footers
  const cleanLines = [];
  let skipNext = 0;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip page headers
    if (line.startsWith('District and State wise list')) { skipNext = 7; continue; }
    if (line === 'Any discrepancy in the content of the report') continue;
    if (line.startsWith('Any discrepancy')) continue;
    if (/^Page \d+ of$/.test(line)) continue;
    if (/^\d{1,3}$/.test(line) && i + 1 < lines.length && /^\d{1,3}$/.test(lines[i + 1].trim())) {
      // This is a page number (e.g. "291" after "Page X of")
      // But only skip if it matches the pattern: the previous line was "Page X of"
      const prevLine = (lines[i - 1] || '').trim();
      if (prevLine.startsWith('Page ') || prevLine === '') continue;
    }
    if (skipNext > 0) { skipNext--; continue; }

    // Skip column headers that appear at top of each page
    if (line === 'S. No.' || line === 'RAID(AS' || line === 'GIVEN IN' ||
        line === 'EMIGRATE' || line === 'SYSTEM)' || line === 'RA NAME AND' ||
        line === 'AUTHORISED' || line === 'SIGNATORY' || line === 'STATE AND' ||
        line === 'DISTRICT' || line === 'RC NUMBER AND CONTACT DETAILS' ||
        line === 'WEBSITE' || line === 'BRANCH OFFICE' || line === 'ADDRESS') {
      continue;
    }

    if (line) cleanLines.push(line);
  }

  // Now parse entries: each entry is a group starting with a serial number
  // followed by RA ID, then name, state/district, RC details, website, branch
  let i = 0;
  while (i < cleanLines.length) {
    const line = cleanLines[i];

    // Look for serial number followed by RA ID
    if (/^\d+$/.test(line) && i + 1 < cleanLines.length && /^RA\d+$/.test(cleanLines[i + 1])) {
      const serialNo = parseInt(line, 10);
      const raId = cleanLines[i + 1];
      i += 2;

      // Collect name line(s) — could be multi-line if firm name is long
      let nameLine = '';
      while (i < cleanLines.length && !cleanLines[i].startsWith('State :-')) {
        nameLine += (nameLine ? ' ' : '') + cleanLines[i];
        i++;
      }

      // State/District line
      let stateLine = '';
      if (i < cleanLines.length && cleanLines[i].startsWith('State :-')) {
        stateLine = cleanLines[i];
        i++;
      }

      // RC/contact details line(s) — continue until we hit a URL, dash, or next entry
      let contactLine = '';
      while (i < cleanLines.length) {
        const cl = cleanLines[i];
        // Stop if we see a URL line, a lone dash, or the next entry's serial number
        if (cl === '-') break;
        if (/^https?:\/\//.test(cl)) break;
        if (/^\d+$/.test(cl) && i + 1 < cleanLines.length && /^RA\d+$/.test(cleanLines[i + 1])) break;
        contactLine += (contactLine ? '' : '') + cl;
        i++;
      }

      // Website line (URL or dash)
      let website = '';
      if (i < cleanLines.length) {
        const wl = cleanLines[i];
        if (/^https?:\/\//.test(wl)) {
          website = wl.trim();
          i++;
        }
      }

      // Dash after website (or instead of website)
      if (i < cleanLines.length && cleanLines[i] === '-') {
        i++;
      }

      // Branch office line(s) — consume until we hit the next entry's serial number
      // (branch office is just informational, we skip it)
      while (i < cleanLines.length) {
        const bl = cleanLines[i];
        if (/^\d+$/.test(bl) && i + 1 < cleanLines.length && /^RA\d+$/.test(cleanLines[i + 1])) break;
        // If we see another State line or another RA, stop
        if (bl.startsWith('State :-')) break;
        if (/^RA\d+$/.test(bl)) break;
        i++;
      }

      // ── Parse extracted fields ──

      // Name and auth signatory
      let firmName = '';
      let authSign = '';
      const authMatch = nameLine.match(/(.+?)\s*;\s*AUTH-SIGN:\s*(.+)/i);
      if (authMatch) {
        firmName = authMatch[1].trim();
        authSign = authMatch[2].trim();
      } else {
        firmName = nameLine.trim();
      }
      // Clean firm name
      firmName = firmName.replace(/\s{2,}/g, ' ').trim();
      if (firmName.startsWith('M/S.')) firmName = firmName.substring(4).trim();
      if (firmName.startsWith('M/S')) firmName = firmName.substring(3).trim();

      // State and district
      let state = '';
      let district = '';
      const stateMatch = stateLine.match(/State\s*:-\s*(.+?)\s*;\s*District\s*:-\s*(.+)/i);
      if (stateMatch) {
        const rawState = stateMatch[1].trim().replace(/\s+/g, '');
        state = STATE_MAP[rawState] || titleCase(rawState);
        district = stateMatch[2].trim().replace(/\s+/g, ' ');
      }

      // Contact details: RC_NUMBER;ADDRESS;EMAIL;PHONE;
      const parts = contactLine.split(';').map(p => p.trim()).filter(Boolean);
      let rcNumber = '';
      let address = '';
      let email = '';
      let phone = '';

      if (parts.length >= 1) rcNumber = parts[0];
      if (parts.length >= 2) address = parts[1];

      // Find email and phone from remaining parts
      for (let p = 2; p < parts.length; p++) {
        const part = parts[p];
        if (part.includes('@') && !email) {
          email = part.toLowerCase().trim();
        } else if (/\d{5,}/.test(part.replace(/[-\s]/g, '')) && !phone) {
          phone = part;
        }
      }

      // Also check if email/phone is embedded in earlier parts
      if (!email) {
        const emailMatch = contactLine.match(/([\w.+-]+@[\w.-]+\.\w{2,})/i);
        if (emailMatch) email = emailMatch[1].toLowerCase();
      }
      if (!phone) {
        const phoneMatch = contactLine.match(/((?:0091|91)[-\s]?\d[\d-\s]{7,})/);
        if (phoneMatch) phone = phoneMatch[1];
      }

      // Skip generic emigrate website URLs
      if (website.includes('emigrate.gov.in')) website = '';

      // Extract city from address
      const city = extractCity(address, district);

      // Parse auth signatory name
      const { first_name, last_name } = splitName(authSign);

      // Build domain from website
      const domain = extractDomain(website);

      const lead = {
        first_name,
        last_name,
        firm_name: titleCase(firmName),
        title: 'Authorized Signatory',
        email: email || '',
        phone: normalizePhone(phone),
        website: website || '',
        domain,
        city,
        state,
        country: COUNTRY,
        niche: NICHE,
        source: SOURCE,
        profile_url: '',
        registration_number: raId,
        address: address ? titleCase(address.substring(0, 200)) : '',
        district: titleCase(district),
        authorized_signatory: titleCase(authSign),
        rc_number: rcNumber,
      };

      leads.push(lead);

      if (serialNo % 200 === 0) {
        console.log(`  Parsed ${serialNo} entries... (${state})`);
      }
    } else {
      i++;
    }
  }

  return leads;
}

// ── Main ────────────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('\n╔═══════════════════════════════════════════════════════════╗');
  console.log('║  MORTAR — India eMigrate Recruiting Agents Scrape       ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Source: MEA PDF (Official Government Registry)`);
  console.log(`  URL:    ${PDF_URL}`);
  console.log('');

  // Step 1: Download PDF
  console.log('  Step 1: Acquiring PDF...');
  await downloadPDF(PDF_URL, PDF_CACHE);
  const pdfSize = fs.statSync(PDF_CACHE).size;
  console.log(`  PDF size: ${(pdfSize / 1024).toFixed(0)} KB`);
  console.log('');

  // Step 2: Parse PDF
  console.log('  Step 2: Parsing PDF...');
  const buf = fs.readFileSync(PDF_CACHE);
  const data = await pdfParse(buf);
  console.log(`  Pages: ${data.numpages}`);
  console.log(`  Text length: ${data.text.length.toLocaleString()} chars`);
  console.log('');

  // Step 3: Extract entries
  console.log('  Step 3: Extracting entries...');
  const leads = parseEntries(data.text);
  console.log(`  Total entries extracted: ${leads.length}`);
  console.log('');

  // Step 4: Stats
  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;
  const withWebsite = leads.filter(l => l.website).length;
  const withName = leads.filter(l => l.first_name || l.last_name).length;
  const states = [...new Set(leads.map(l => l.state))].sort();

  console.log('  Step 4: Summary');
  console.log('  ─────────────────────────────────────');
  console.log(`  Total leads:        ${leads.length}`);
  console.log(`  With email:         ${withEmail} (${((withEmail / leads.length) * 100).toFixed(1)}%)`);
  console.log(`  With phone:         ${withPhone} (${((withPhone / leads.length) * 100).toFixed(1)}%)`);
  console.log(`  With website:       ${withWebsite} (${((withWebsite / leads.length) * 100).toFixed(1)}%)`);
  console.log(`  With name:          ${withName} (${((withName / leads.length) * 100).toFixed(1)}%)`);
  console.log(`  States/UTs:         ${states.length}`);
  console.log('');

  // State breakdown
  console.log('  State Breakdown:');
  const stateGroups = {};
  for (const lead of leads) {
    stateGroups[lead.state] = (stateGroups[lead.state] || 0) + 1;
  }
  const sortedStates = Object.entries(stateGroups).sort((a, b) => b[1] - a[1]);
  for (const [st, count] of sortedStates) {
    const bar = '█'.repeat(Math.ceil(count / 20));
    console.log(`    ${st.padEnd(22)} ${String(count).padStart(5)}  ${bar}`);
  }
  console.log('');

  // Step 5: Write CSV
  console.log('  Step 5: Writing CSV...');
  writeCSV(OUTPUT_FILE, leads);
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log('');

  // Sample leads
  console.log('  Sample Leads:');
  console.log('  ─────────────────────────────────────');
  for (const lead of leads.slice(0, 5)) {
    console.log(`    ${lead.registration_number} | ${lead.firm_name}`);
    console.log(`      ${lead.first_name} ${lead.last_name} | ${lead.email} | ${lead.phone}`);
    console.log(`      ${lead.city}, ${lead.state} | ${lead.website || '(no website)'}`);
    console.log('');
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Done in ${elapsed}s`);
}

main().catch(err => {
  console.error('\n  FATAL:', err.message);
  process.exit(1);
});
