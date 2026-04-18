#!/usr/bin/env node
/**
 * Parse EOIR Accredited Representatives Roster PDF into CSV.
 *
 * Source: https://www.justice.gov/eoir/page/file/942311/dl
 * Output: output/eoir-immigration-reps.csv
 *
 * EOIR accredits non-lawyer immigration representatives who can represent
 * clients before immigration courts. These are immigration service providers
 * at recognized organizations (nonprofits, legal aid). ~2,641 individuals.
 *
 * Each entry in the PDF typically contains:
 *   - Name
 *   - Accreditation level (Partial / Full)
 *   - Organization name
 *   - Address
 *   - Expiration date
 */

const fs = require('fs');
const path = require('path');
const pdf = require('pdf-parse');

const PDF_PATHS = [
  '/tmp/eoir-reps.bin',
  '/tmp/eoir-1398081.bin',
];

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'eoir-immigration-reps.csv');

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

async function parsePdf(filepath) {
  const buffer = fs.readFileSync(filepath);
  const data = await pdf(buffer);
  return data.text;
}

// Parse EOIR format. Typical entry block:
//   John Smith, Partial Accreditation
//   Catholic Charities of Atlanta
//   123 Main St
//   Atlanta, GA 30303
//   Expires: 10/31/2027
function parseEntries(text) {
  const lines = text.split(/\r?\n/).map(l => l.trim());
  const entries = [];

  // Try line-based parsing first — look for "Expires: MM/DD/YYYY" as entry terminator
  let currentBuffer = [];

  for (const line of lines) {
    if (!line) continue;

    // Skip header/footer noise
    if (/^(page \d+|accredited rep|number of|executive office|department of justice|u\.s\. department|updated)/i.test(line)) continue;
    if (/^\d+\s*$/.test(line)) continue;

    currentBuffer.push(line);

    // Entry boundary: line with "Expires" date OR paired "Partial"/"Full Accreditation"
    const isExpiry = /expires:?\s*\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/i.test(line);
    const isAccred = /\b(Partial|Full)\s+Accreditation\b/i.test(line);

    if (isExpiry && currentBuffer.length >= 2) {
      entries.push(currentBuffer.join('\n'));
      currentBuffer = [];
    }
  }

  if (currentBuffer.length) entries.push(currentBuffer.join('\n'));

  // Now parse each entry
  const parsed = [];
  for (const raw of entries) {
    const lines = raw.split('\n').filter(Boolean);

    let firstName = '', lastName = '', accreditation = '';
    let organization = '', address = '', city = '', state = '', zip = '', expires = '';

    // Look for accreditation marker
    const accIdx = lines.findIndex(l => /\b(Partial|Full)\s+Accreditation\b/i.test(l));

    if (accIdx >= 0) {
      const accLine = lines[accIdx];
      const accMatch = accLine.match(/^(.+?)[,\s]+(Partial|Full)\s+Accreditation/i);
      if (accMatch) {
        const fullName = accMatch[1].trim();
        accreditation = accMatch[2];

        const parts = fullName.split(/\s+/).filter(Boolean);
        if (parts.length >= 2) {
          firstName = parts[0];
          lastName = parts.slice(1).join(' ');
        } else {
          firstName = fullName;
        }
      }

      // Org is usually the next non-empty line after accreditation
      if (accIdx + 1 < lines.length) organization = lines[accIdx + 1].trim();

      // Address: usually 2-3 lines after org
      const addrLines = [];
      for (let i = accIdx + 2; i < lines.length; i++) {
        if (/expires:/i.test(lines[i])) {
          const m = lines[i].match(/expires:?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i);
          if (m) expires = m[1];
          break;
        }
        addrLines.push(lines[i]);
      }
      address = addrLines.join(', ');

      // Try to extract city, state, zip from address
      const cszMatch = address.match(/([A-Z][a-zA-Z\s.\-]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
      if (cszMatch) {
        city = cszMatch[1].trim();
        state = cszMatch[2];
        zip = cszMatch[3];
      }
    }

    if (!firstName && !organization) continue;

    parsed.push({
      first_name: firstName,
      last_name: lastName,
      accreditation,
      organization,
      address,
      city,
      state,
      zip,
      expires,
      raw_block: raw,
    });
  }

  return parsed;
}

async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('=== EOIR Accredited Immigration Reps Parser ===');

  const allEntries = [];
  for (const p of PDF_PATHS) {
    if (!fs.existsSync(p)) {
      console.log(`  Skip (not found): ${p}`);
      continue;
    }
    console.log(`  Parsing ${p}...`);
    const text = await parsePdf(p);
    console.log(`    Text length: ${text.length} chars`);
    const entries = parseEntries(text);
    console.log(`    Parsed ${entries.length} entries`);
    allEntries.push(...entries);
  }

  // Dedup by name + org
  const seen = new Set();
  const unique = [];
  for (const e of allEntries) {
    const key = (e.first_name + '|' + e.last_name + '|' + e.organization).toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(e);
    }
  }

  // Write CSV
  const cols = [
    'first_name', 'last_name', 'accreditation', 'organization',
    'address', 'city', 'state', 'zip', 'expires',
    'niche', 'source', 'country',
  ];
  const header = cols.join(',');
  const rows = unique.map(e => cols.map(c => {
    if (c === 'niche') return escapeCsv('immigration');
    if (c === 'source') return escapeCsv('eoir_accredited_reps');
    if (c === 'country') return escapeCsv('US');
    return escapeCsv(e[c] || '');
  }).join(','));
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + rows.join('\n') + '\n');

  console.log(`\n=== DONE ===`);
  console.log(`Total entries:  ${allEntries.length}`);
  console.log(`Unique:         ${unique.length}`);
  console.log(`With org:       ${unique.filter(e => e.organization).length}`);
  console.log(`With address:   ${unique.filter(e => e.address).length}`);
  console.log(`Output:         ${OUTPUT_FILE}`);

  // Sample
  console.log('\nSample (first 3):');
  for (const s of unique.slice(0, 3)) {
    console.log(`  ${s.first_name} ${s.last_name} (${s.accreditation}) — ${s.organization} — ${s.city}, ${s.state}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
