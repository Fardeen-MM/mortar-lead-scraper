#!/usr/bin/env node
/**
 * SACAC College Consultant Scraper
 *
 * Scrapes the SACAC (Southern Association for College Admission Counseling)
 * member directory for Independent Educational Consultants using the
 * Cloudflare Browser Rendering API.
 *
 * Usage:
 *   node scripts/scrape-sacac-college.js            # full scrape
 *   node scripts/scrape-sacac-college.js --test      # first 10 entries only
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const TARGET_URL = 'https://members.sacac.org/activememberdirectory/Search/independent-educational-consultant-578047';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'college-consultants-sacac.csv');
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'address', 'city', 'state', 'zip', 'country',
  'niche', 'source', 'profile_url'
];

const TEST_MODE = process.argv.includes('--test');

// ---------------------------------------------------------------------------
// Load .env
// ---------------------------------------------------------------------------

function loadEnv() {
  const envPath = path.join(__dirname, '..', '.env');
  if (!fs.existsSync(envPath)) {
    console.error('[ERROR] .env file not found at', envPath);
    process.exit(1);
  }
  const lines = fs.readFileSync(envPath, 'utf8').split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) {
      process.env[key] = val;
    }
  }
}

loadEnv();

const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;

if (!CLOUDFLARE_ACCOUNT_ID || !CLOUDFLARE_API_TOKEN) {
  console.error('[ERROR] Missing CLOUDFLARE_ACCOUNT_ID or CLOUDFLARE_API_TOKEN in .env');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// HTTP helper (native https, returns Promise)
// ---------------------------------------------------------------------------

function cfRenderMarkdown(url) {
  return new Promise((resolve, reject) => {
    const endpoint = `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/browser-rendering/markdown`;
    const body = JSON.stringify({
      url,
      gotoOptions: { waitUntil: 'networkidle0', timeout: 30000 }
    });

    const parsed = new URL(endpoint);
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${CLOUDFLARE_API_TOKEN}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };

    console.log('[INFO] Fetching markdown from Cloudflare Browser Rendering API...');
    console.log('[INFO] Target URL:', url);

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8');
        try {
          const json = JSON.parse(raw);
          if (!json.success) {
            reject(new Error(`Cloudflare API error: ${JSON.stringify(json.errors || json)}`));
            return;
          }
          console.log(`[INFO] Received ${json.result.length.toLocaleString()} chars of markdown`);
          resolve(json.result);
        } catch (e) {
          reject(new Error(`Failed to parse Cloudflare response: ${e.message}\nRaw (first 500 chars): ${raw.slice(0, 500)}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error('Request timed out after 60s'));
    });
    req.write(body);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

function normalizePhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) {
    const d = digits.slice(1);
    return `(${d.slice(0, 3)}) ${d.slice(3, 6)}-${d.slice(6)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return raw.trim();
}

function stripTitle(name) {
  // Strip leading honorifics (with or without period)
  let cleaned = name
    .replace(/^(Mr\.?|Ms\.?|Mrs\.?|Dr\.?|Prof\.?|Rev\.?)\s+/i, '')
    .trim();
  // Strip trailing suffixes/credentials — require comma or space boundary before suffix
  // Use \b to avoid matching partial words (e.g., "Williams" ending with "ms")
  cleaned = cleaned
    .replace(/,\s*(Jr\.?|Sr\.?|II|III|IV|Esq\.?|Ph\.?D\.?|Ed\.?D\.?|M\.?Ed\.?|M\.?A\.?|M\.?S\.?|CEP|IECA|LLC|CPA|CFP|MBA)\s*$/gi, '')
    .trim();
  return cleaned;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const parsed = new URL(url.startsWith('http') ? url : `https://${url}`);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

/**
 * Fix malformed profile URLs from the source.
 * The markdown contains URLs like:
 *   https://members.sacac.org/activememberdirectory/Search/independent-educational-consultant-578047//members.sacac.org/activememberdirectory/Details/name-123
 * We need to extract just the Details path and build a clean URL.
 */
function fixProfileUrl(url) {
  if (!url) return '';
  // Look for the Details path buried inside the malformed URL
  const detailsMatch = url.match(/\/activememberdirectory\/Details\/[^\s)]+/);
  if (detailsMatch) {
    return 'https://members.sacac.org' + detailsMatch[0];
  }
  return url;
}

function parseAddress(addressText) {
  const result = { address: '', city: '', state: '', zip: '' };
  if (!addressText) return result;

  const cleaned = addressText.trim()
    .replace(/\s*,\s*United States\s*$/i, '')
    .replace(/\s*,\s*USA\s*$/i, '')
    .trim();

  // Match: ..., ST ZIP or ..., ST ZIPCODE-XXXX at the end
  const stateZipMatch = cleaned.match(/,\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
  if (stateZipMatch) {
    result.state = stateZipMatch[1];
    result.zip = stateZipMatch[2];
    const beforeStateZip = cleaned.slice(0, stateZipMatch.index).trim();
    const lastComma = beforeStateZip.lastIndexOf(',');
    if (lastComma !== -1) {
      result.address = beforeStateZip.slice(0, lastComma).trim();
      result.city = beforeStateZip.slice(lastComma + 1).trim();
    } else {
      result.city = beforeStateZip;
    }
  } else {
    // Try state-only pattern (no zip), e.g., "High Point, NC"
    const stateOnlyMatch = cleaned.match(/,\s*([A-Z]{2})$/);
    if (stateOnlyMatch) {
      result.state = stateOnlyMatch[1];
      const beforeState = cleaned.slice(0, stateOnlyMatch.index).trim();
      const lastComma = beforeState.lastIndexOf(',');
      if (lastComma !== -1) {
        result.address = beforeState.slice(0, lastComma).trim();
        result.city = beforeState.slice(lastComma + 1).trim();
      } else {
        result.city = beforeState;
      }
    } else {
      // Fallback: store the whole thing as address
      result.address = cleaned;
    }
  }
  return result;
}

/**
 * Detect if a "name" is actually an organization name, not a person.
 * Organization entries use the firm name in the name field (e.g., "PEAK Educational Consulting, LLC").
 */
function isOrgName(name) {
  if (!name) return false;
  return /\b(LLC|Inc|Corp|Ltd|LLP|PLLC|Group|Associates|Consulting|Services|Academy|Institute)\b/i.test(name)
    || /,\s*(LLC|Inc|Ltd)\.?\s*$/i.test(name);
}

function parseMemberBlock(block) {
  const lead = {
    first_name: '', last_name: '', firm_name: '', title: '', email: '',
    phone: '', website: '', domain: '', address: '', city: '', state: '',
    zip: '', country: 'US', niche: 'education', source: 'sacac', profile_url: ''
  };

  // --- Name from ##### heading ---
  const nameHeadingMatch = block.match(/#{5}\s*\[([^\]]+)\]\(([^)]*)\)/);
  if (!nameHeadingMatch) return null; // No valid entry in this block

  let rawName = nameHeadingMatch[1].trim();
  const rawProfileUrl = nameHeadingMatch[2].trim();

  // Fix malformed profile URL
  lead.profile_url = fixProfileUrl(rawProfileUrl);

  // Check if the name text contains an email (e.g., "naomi@ayeconline.com Naomi Steinberg")
  const emailInName = rawName.match(/([\w.+-]+@[\w.-]+\.\w{2,})/);
  if (emailInName) {
    lead.email = emailInName[1];
    rawName = rawName.replace(emailInName[0], '').trim();
  }

  // Check if this is an organization entry (not a person)
  if (isOrgName(rawName)) {
    lead.firm_name = rawName;
    // Don't set first/last name for orgs
  } else {
    rawName = stripTitle(rawName);
    const parts = rawName.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      lead.first_name = parts[0];
      lead.last_name = parts[parts.length - 1];
      // If there are middle parts that look like duplicated first name, skip them
      // e.g., "Liz Liz Agather" -> first=Liz, last=Agather
      // e.g., "Tammy Tammy Alt" -> first=Tammy, last=Alt
      // e.g., "Megan Meg Stiphany" -> first=Megan, last=Stiphany (keep as-is since different)
    } else if (parts.length === 1) {
      lead.first_name = parts[0];
    }
  }

  // --- Title and Firm from text between ##### heading and bullet list ---
  const headingIdx = block.indexOf('#####');
  if (headingIdx !== -1) {
    const headingLineEnd = block.indexOf('\n', headingIdx);
    if (headingLineEnd !== -1) {
      const bulletStart = block.indexOf('\n*', headingLineEnd);
      const endIdx = bulletStart !== -1 ? bulletStart : block.length;
      const middleText = block.slice(headingLineEnd + 1, endIdx)
        .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')  // flatten markdown links
        .replace(/#+/g, '')                          // strip stray heading marks
        .trim();

      if (middleText) {
        // Split by double-space (2+ spaces) — format is "Title  FirmName"
        const segments = middleText.split(/\s{2,}/).map(s => s.trim()).filter(Boolean);
        if (segments.length >= 2) {
          lead.title = segments[0];
          // Only set firm_name from this field if not already set by org detection
          if (!lead.firm_name) {
            lead.firm_name = segments.slice(1).join(' ');
          }
        } else if (segments.length === 1) {
          const val = segments[0];
          // If it matches common title keywords, treat as title
          if (/consultant|counselor|advisor|founder|owner|director|president|principal|ceo|partner|manager|specialist|coach|mentor/i.test(val)) {
            lead.title = val;
          } else if (!lead.firm_name) {
            lead.firm_name = val;
          }
        }
      }
    }
  }

  // --- Phone from tel: link ---
  const phoneMatch = block.match(/\[([^\]]*)\]\(tel:([^)]+)\)/);
  if (phoneMatch) {
    lead.phone = normalizePhone(phoneMatch[2]);
  }

  // --- Email from mailto: link ---
  if (!lead.email) {
    const mailtoMatch = block.match(/\[([^\]]*)\]\(mailto:([^)]+)\)/);
    if (mailtoMatch) {
      lead.email = mailtoMatch[2].trim();
    }
  }

  // --- Scan for bare email addresses (skip sacac.org and google.com) ---
  if (!lead.email) {
    const emailMatches = block.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    if (emailMatches) {
      const realEmail = emailMatches.find(e =>
        !e.endsWith('sacac.org') && !e.endsWith('google.com')
      );
      if (realEmail) lead.email = realEmail;
    }
  }

  // --- Website from "Visit Website" link ---
  const websiteMatch = block.match(/\[\s*Visit Website\s*\]\(([^)]+)\)/i);
  if (websiteMatch) {
    lead.website = websiteMatch[1].trim();
    lead.domain = extractDomain(lead.website);
  }

  // --- Address from Google Maps link ---
  const addressMatch = block.match(/\[\s*([^\]]+)\]\(https:\/\/www\.google\.com\/maps/);
  if (addressMatch) {
    const addrText = addressMatch[1].trim();
    const parsed = parseAddress(addrText);
    lead.address = parsed.address;
    lead.city = parsed.city;
    lead.state = parsed.state;
    lead.zip = parsed.zip;
  }

  return lead;
}

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

function escapeCSV(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function writeCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );
  const csv = [header, ...rows].join('\n') + '\n';
  fs.writeFileSync(OUTPUT_FILE, csv, 'utf8');
  return OUTPUT_FILE;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('SACAC College Consultant Scraper');
  console.log('='.repeat(60));
  if (TEST_MODE) console.log('[TEST MODE] Processing first 10 entries only\n');

  // Step 1: Fetch markdown via Cloudflare Browser Rendering API
  const markdown = await cfRenderMarkdown(TARGET_URL);

  // Step 2: Split into member blocks
  //
  // Each member entry in the markdown follows this pattern:
  //
  //   Independent Educational Consultant \n\n
  //   [ Name ](profile_url) \n\n
  //   ##### [Name](profile_url) \n\n
  //   Title  FirmName \n\n
  //   * [address](maps_url)
  //   * [phone](tel:xxx)
  //   * [Send Email](contact_url)
  //   * [ Visit Website ](url)
  //   * [ More Details ](profile_url)
  //
  // "Independent Educational Consultant" appears once as a header, then
  // once before EACH member entry. We split on it and skip blocks that
  // don't contain a ##### heading (header/preamble/footer junk).

  const blocks = markdown.split(/Independent Educational Consultant\s*/);

  // Filter to only blocks that contain a ##### member heading
  const memberBlocks = blocks.filter(b => /#{5}\s*\[/.test(b));
  console.log(`[INFO] Found ${memberBlocks.length} member entries\n`);

  const toProcess = TEST_MODE ? memberBlocks.slice(0, 10) : memberBlocks;

  // Step 3: Parse each block
  const leads = [];
  let skipped = 0;
  let orgEntries = 0;

  for (let i = 0; i < toProcess.length; i++) {
    const block = toProcess[i];
    const lead = parseMemberBlock(block);

    if (!lead) {
      skipped++;
      continue;
    }

    // Skip entries with no name AND no firm (garbage blocks)
    if (!lead.first_name && !lead.last_name && !lead.firm_name) {
      skipped++;
      continue;
    }

    if (!lead.first_name && !lead.last_name && lead.firm_name) {
      orgEntries++;
    }

    leads.push(lead);

    if ((i + 1) % 50 === 0 || i === toProcess.length - 1) {
      console.log(`[PROGRESS] Parsed ${i + 1}/${toProcess.length} blocks (${leads.length} leads extracted)`);
    }
  }

  // Step 4: Write CSV
  const outPath = writeCSV(leads);

  // Step 5: Summary stats
  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`Total member blocks:  ${memberBlocks.length}`);
  console.log(`Blocks processed:     ${toProcess.length}`);
  console.log(`Leads extracted:      ${leads.length}`);
  console.log(`  - People:           ${leads.length - orgEntries}`);
  console.log(`  - Organizations:    ${orgEntries}`);
  console.log(`Skipped (no data):    ${skipped}`);
  console.log(`With email:           ${leads.filter(l => l.email).length}`);
  console.log(`With phone:           ${leads.filter(l => l.phone).length}`);
  console.log(`With website:         ${leads.filter(l => l.website).length}`);
  console.log(`With firm name:       ${leads.filter(l => l.firm_name).length}`);
  console.log(`With address:         ${leads.filter(l => l.address || l.city).length}`);

  // State distribution
  const stateCounts = {};
  for (const l of leads) {
    if (l.state) stateCounts[l.state] = (stateCounts[l.state] || 0) + 1;
  }
  const sortedStates = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
  if (sortedStates.length > 0) {
    console.log(`\nTop states:`);
    for (const [st, count] of sortedStates.slice(0, 15)) {
      console.log(`  ${st}: ${count}`);
    }
  }

  console.log(`\nCSV written to: ${outPath}`);
  console.log('='.repeat(60));
}

main().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});
