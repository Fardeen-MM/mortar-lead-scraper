#!/usr/bin/env node
/**
 * DOJ EOIR Accredited Representatives Scraper
 *
 * Downloads the DOJ "Recognition and Accreditation Roster" (Combined Roster by State and City)
 * and extracts all accredited representatives with their organization details.
 *
 * Source: https://www.justice.gov/eoir/media/1398081/dl?inline
 *
 * Output: output/doj-immigration-representatives.csv
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const pdfParse = require('pdf-parse');

const PDF_URL = 'https://www.justice.gov/eoir/media/1398081/dl?inline';
const PDF_PATH = '/tmp/doj-roster.pdf';
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_PATH = path.join(OUTPUT_DIR, 'doj-immigration-representatives.csv');

// CSV columns
const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source', 'profile_url'
];

// ============================================================================
// Download
// ============================================================================

function downloadPdf(url, dest) {
  return new Promise((resolve, reject) => {
    console.log(`Downloading PDF from ${url} ...`);
    const file = fs.createWriteStream(dest);
    const request = (reqUrl, redirects = 0) => {
      if (redirects > 5) return reject(new Error('Too many redirects'));
      const mod = reqUrl.startsWith('https') ? require('https') : require('http');
      mod.get(reqUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return request(res.headers.location, redirects + 1);
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
        res.pipe(file);
        file.on('finish', () => {
          file.close();
          const stats = fs.statSync(dest);
          console.log(`  Downloaded ${(stats.size / 1024).toFixed(0)} KB to ${dest}`);
          resolve();
        });
      }).on('error', reject);
    };
    request(url);
  });
}

// ============================================================================
// Parse Organizations (Section 1 of PDF)
// ============================================================================

function parseOrganizations(text, repSectionStart) {
  const orgText = text.substring(0, repSectionStart);
  const orgs = new Map(); // orgName -> { city, state, zip, phone, address }

  // Strategy 1: Find orgs by looking for lines above "Principal Office" / "Extension Office"
  const lines = orgText.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line === 'Principal Office' || /Extension Office$/.test(line)) {
      // Org name is the line above
      if (i > 0) {
        let orgName = lines[i - 1].trim();
        // Skip if it looks like a date/address/header
        if (!orgName || /^\d/.test(orgName) || /^P\.?O\.?\s*Box/i.test(orgName) ||
            /^\(/.test(orgName) || orgName.length < 4 ||
            /^Active/.test(orgName) || /^\d{2}\/\d{2}\//.test(orgName) ||
            orgName === 'Recognized' || orgName === 'Organization') {
          continue;
        }

        // Look downward for address, city/state/zip, phone
        let address = '';
        let city = '';
        let state = '';
        let zip = '';
        let phone = '';

        for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
          const addrLine = lines[j].trim();
          if (!addrLine) continue;

          // City, ST ZIP
          // Handle PDF artifacts: ZIP may be 4-5 digits, possibly concatenated with suite number
          // E.g., "191075812" = 19107-5812, "757025918" = 75702-5918, "5401" = 05401
          const csMatch = addrLine.match(/^([A-Za-z][A-Za-z\s.'()-]+),\s*([A-Z]{2})\s+(\d{4,10}(?:-\d{1,4})?)$/);
          if (csMatch) {
            city = csMatch[1].trim();
            state = csMatch[2];
            zip = csMatch[3];
            continue;
          }

          // Phone
          const phoneMatch = addrLine.match(/^\((\d{3})\)\s*(\d{3})[- ](\d{4})$/);
          if (phoneMatch) {
            phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;
            break; // Phone is the last field we need
          }

          // Date line (stop looking)
          if (/^\d{2}\/\d{2}\/\d{2}/.test(addrLine)) break;

          // Active/status line (stop)
          if (addrLine === 'Active' || /Active$/.test(addrLine)) break;

          // State header (stop)
          if (/^[A-Z]{2,}$/.test(addrLine) && addrLine.length <= 20) break;

          // Otherwise it's probably an address line
          if (!address) {
            address = addrLine;
          }
        }

        if (city && state) {
          // Only store the first occurrence (Principal Office preferred)
          if (!orgs.has(orgName)) {
            orgs.set(orgName, { orgName, address, city, state, zip, phone });
          }
        }
      }
    }
  }

  console.log(`  Parsed ${orgs.size} unique organizations from org section`);
  return orgs;
}

// ============================================================================
// Parse Representatives (Section 2 of PDF)
// ============================================================================

function parseRepresentatives(text, orgs) {
  // Find where the rep section starts
  const repStartIdx = text.indexOf('Accredited RepresentativeAccreditation Expiration');
  if (repStartIdx < 0) {
    console.error('ERROR: Could not find representative section in PDF');
    return [];
  }

  const repSection = text.substring(repStartIdx);

  // ---- STRATEGY: Use date+status as delimiters to extract rep entries ----
  // Every rep entry ends with a date pattern like "MM/DD/YYActive" or "MM/DD/YY* (Pending Renewal)Active"
  // We split the text at these boundaries to get one chunk per rep.

  // First, clean out page headers and interleaved org section data
  let cleaned = repSection
    .replace(/Recognized\nOrganization\n/g, '')
    .replace(/Accredited RepresentativeAccreditation Expiration\s*\nDate\nRepresentative\s*\nStatus\n/g, '');

  // Remove interleaved state org sections (they have addresses, date pairs, city headers)
  // In the Combined Roster PDF, some state org listings appear BETWEEN rep blocks.
  // Strategy: look ahead for "Principal Office" / "Extension Office" to detect org blocks.
  const lines = cleaned.split('\n');
  const filteredLines = [];

  // Pre-scan: find all line indices that are "Principal Office" or "Extension Office"
  const officeLineIndices = new Set();
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line === 'Principal Office' || /Extension Office$/.test(line)) {
      officeLineIndices.add(i);
      // Also mark the line above as an org name to skip
      if (i > 0) officeLineIndices.add(i - 1);
    }
  }

  let skipOrgBlock = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // If the NEXT non-empty line is "Principal Office" or "Extension Office",
    // this line is an org name - skip it and enter skip mode
    if (officeLineIndices.has(i)) {
      skipOrgBlock = true;
      continue;
    }

    // Detect start of interleaved org block
    if (line === 'Principal Office' || /Extension Office$/.test(line)) {
      skipOrgBlock = true;
      continue;
    }

    // In skip mode: skip org-section lines until we find a clear rep entry
    if (skipOrgBlock) {
      const isOrgLine = (
        /^\d{2}\/\d{2}\/\d{2}\d{2}\/\d{2}\/\d{2}/.test(line) ||  // date pair
        /^\(\d{3}\)\s*\d{3}[- ]\d{4}$/.test(line) ||              // phone
        /^[A-Za-z\s.'()-]+,\s*[A-Z]{2}\s+\d{5}/.test(line) ||     // city, ST ZIP (with or without 4-digit suffix)
        /^[A-Za-z\s.'()-]+,\s*[A-Z]{2}\s+\d{4,5}$/.test(line) ||  // city, ST ZIP (VT has 4-digit)
        /^\d+\s+/.test(line) ||                                     // street address
        /^P\.?O\.?\s*Box/i.test(line) ||                            // PO box
        /^[A-Z][A-Z\s]+$/.test(line) ||                             // state header (all caps)
        /^(Date|Recognition|Expiration Date|Organization|Status|Suite|Mailing)/.test(line) ||
        /^(Principal Office|Extension Office)/.test(line) ||
        officeLineIndices.has(i) ||                                 // pre-scanned as org name
        (line.match(/^\d{2}\/\d{2}\/\d{2}/) && !line.match(/[A-Za-z],/)) || // date without name
        // Org names in interleaved sections (no comma, no date, alphabetic)
        (!line.includes(',') && !line.match(/\d{2}\/\d{2}\/\d{2}/) && /^[A-Z]/.test(line) && line.length < 80)
      );

      if (isOrgLine) continue;

      // Check if this is a rep entry (has name comma pattern + date)
      if (line.match(/[A-Za-z]+,\s*[A-Z]/) || line.match(/\d{2}\/\d{2}\/\d{2}.*Active/)) {
        skipOrgBlock = false;
        filteredLines.push(line);
        continue;
      }

      // Still ambiguous - keep skipping
      continue;
    }

    // Skip standalone org-section artifacts that leak through
    if (/^\d{2}\/\d{2}\/\d{2}\d{2}\/\d{2}\/\d{2}/.test(line)) continue; // date pairs
    if (/^\(\d{3}\)\s*\d{3}[- ]\d{4}$/.test(line)) continue; // standalone phone
    if (/^[A-Za-z\s.'()-]+,\s*[A-Z]{2}\s+\d{5}/.test(line) && !line.match(/\d{2}\/\d{2}\/\d{2}/)) continue;
    if (/^\d+\s+[A-Z].*(?:Street|Avenue|Road|Drive|Blvd|Way|Ave|St|Rd|Dr)/i.test(line)) continue;
    if (/^P\.?O\.?\s*Box/i.test(line)) continue;
    if (/^(Date|Recognized|Recognition|Expiration Date|Organization|Status)$/.test(line)) continue;
    if (/^[A-Z][A-Z\s]+$/.test(line) && line.length < 25 && !line.includes(',')) continue;

    filteredLines.push(line);
  }

  // Now join all filtered lines into a single string and split by date+status markers
  const joinedText = filteredLines.join('\n');

  // Split at each rep entry boundary: the pattern MM/DD/YY[*] [(Pending Renewal)] Active/Inactive
  // Each match gives us one rep entry
  const entryPattern = /(\d{2}\/\d{2}\/\d{2})\*?\s*(?:\(Pending\s*Renewal\))?\s*(Active|Inactive)/g;
  const entries = [];
  let lastEnd = 0;
  let match;

  while ((match = entryPattern.exec(joinedText)) !== null) {
    const entryText = joinedText.substring(lastEnd, match.index + match[0].length).trim();
    const expDate = match[1];
    const status = match[2];
    entries.push({ text: entryText, expDate, status });
    lastEnd = match.index + match[0].length;
  }

  console.log(`  Found ${entries.length} rep entries by date+status markers`);

  // ---- Build org name lookup ----
  // Sort by length descending for greedy matching
  const orgNameList = Array.from(orgs.keys()).sort((a, b) => b.length - a.length);

  // Helper: find the split point between org name and rep last name.
  // In the PDF, they are concatenated without space, e.g. "ServicesVargas" or "VermontJenness"
  // Look for a lowercase-to-uppercase transition within a "word" (no space between).
  function findCaseTransitionSplit(text) {
    // Look for pattern: ...lowercaseUppercase... where the uppercase starts the last name
    // E.g., "Bet Tzedek Legal ServicesVargas, Lidia" -> split at "Services|Vargas"
    const commaIdx = text.indexOf(',');
    if (commaIdx < 0) return null;
    const beforeComma = text.substring(0, commaIdx);

    // Scan for lowercase-to-uppercase transition (within a word, no space)
    for (let i = beforeComma.length - 1; i >= 1; i--) {
      const prev = beforeComma[i - 1];
      const curr = beforeComma[i];
      // Transition: lowercase letter followed by uppercase letter, with no space between
      if (/[a-z]/.test(prev) && /[A-Z]/.test(curr)) {
        const possibleOrg = beforeComma.substring(0, i).trim();
        const possibleName = beforeComma.substring(i) + text.substring(commaIdx);
        // Validate: org should be long enough, name should have comma
        if (possibleOrg.length >= 3 && possibleName.includes(',') && /^[A-Z][a-z]/.test(possibleName)) {
          return { org: possibleOrg, name: possibleName };
        }
      }
    }

    // Also check for transition after special chars: ")" or '"'
    for (let i = beforeComma.length - 1; i >= 1; i--) {
      const prev = beforeComma[i - 1];
      const curr = beforeComma[i];
      if (/[)"\d]/.test(prev) && /[A-Z]/.test(curr) && i > 3) {
        const possibleOrg = beforeComma.substring(0, i).trim();
        const possibleName = beforeComma.substring(i) + text.substring(commaIdx);
        if (possibleOrg.length >= 3 && possibleName.includes(',')) {
          return { org: possibleOrg, name: possibleName };
        }
      }
    }

    return null;
  }

  // Helper: check if text looks like a pure person name (no org prefix)
  function looksLikePureName(text) {
    const commaIdx = text.indexOf(',');
    if (commaIdx < 0) return false;
    const beforeComma = text.substring(0, commaIdx).trim();
    const afterComma = text.substring(commaIdx + 1).trim();
    if (!afterComma || !/^[A-Z]/.test(afterComma)) return false;

    // Pure name: all words before comma are name-like (capitalized, reasonable length)
    const words = beforeComma.split(/\s+/);
    // Allow up to 4 words for compound names (e.g. "De La Cruz Garcia")
    if (words.length > 4) return false;
    // Each word should start with uppercase (or be a lowercase connector like "de", "van", "del")
    const nameConnectors = new Set(['de', 'del', 'la', 'van', 'von', 'los', 'las', 'di', 'da', 'el', 'al']);
    return words.every(w =>
      /^[A-Z]/.test(w) || nameConnectors.has(w.toLowerCase())
    );
  }

  const reps = [];
  let currentOrg = null;

  for (let ei = 0; ei < entries.length; ei++) {
    let { text: entryText, expDate, status } = entries[ei];

    // Normalize: collapse whitespace, remove the trailing date+status
    entryText = entryText.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();

    // Remove the date+status suffix
    entryText = entryText.replace(/\s*\d{2}\/\d{2}\/\d{2}\*?\s*(?:\(Pending\s*Renewal\))?\s*(Active|Inactive)\s*$/, '').trim();

    // Remove any leftover org-section text that leaked through
    entryText = entryText.replace(/^.*?[A-Z]{2}\s+\d{5}(?:-\d{4})?\s+/, '').trim();

    // Check for "(DHS only)" marker
    const isDhsOnly = /\(DHS only\)/.test(entryText);
    entryText = entryText.replace(/\s*\(DHS only\)\s*/g, ' ').trim();

    // Skip entries that don't look like names
    if (!entryText.includes(',')) continue;
    if (/^\d/.test(entryText) && !entryText.match(/[A-Za-z],/)) continue;

    // ---- Split org name from rep name ----
    let orgName = null;
    let nameStr = entryText;

    // Strategy 1: Match against known org names (longest match first)
    for (const knownOrg of orgNameList) {
      if (entryText.startsWith(knownOrg)) {
        const remainder = entryText.substring(knownOrg.length).trim();
        if (remainder && remainder.includes(',') && /^[A-Z]/.test(remainder)) {
          orgName = knownOrg;
          nameStr = remainder;
          break;
        }
      }
    }

    // Strategy 1b: Check if org name is concatenated without space (case transition)
    // E.g., "Bet Tzedek Legal ServicesVargas, Lidia"
    if (!orgName) {
      const caseSplit = findCaseTransitionSplit(entryText);
      if (caseSplit) {
        // Verify the org name is known (or close to known)
        let isKnownOrg = false;
        for (const knownOrg of orgNameList) {
          if (caseSplit.org === knownOrg || knownOrg.startsWith(caseSplit.org) || caseSplit.org.startsWith(knownOrg)) {
            isKnownOrg = true;
            break;
          }
        }
        if (isKnownOrg || caseSplit.org.length > 15) {
          orgName = caseSplit.org;
          nameStr = caseSplit.name;
        }
      }
    }

    // Strategy 2: Check if this is a pure person name (continuation rep)
    if (!orgName && currentOrg && looksLikePureName(entryText)) {
      nameStr = entryText;
    }

    // Strategy 3: Heuristic split at space boundaries for unknown orgs
    if (!orgName && !looksLikePureName(entryText)) {
      const commaIdx = entryText.indexOf(',');
      if (commaIdx > 0) {
        const beforeComma = entryText.substring(0, commaIdx);
        const afterComma = entryText.substring(commaIdx + 1).trim();

        // Try each space position (from right to left) as a potential split
        let bestSplit = null;
        for (let si = beforeComma.length - 1; si >= 1; si--) {
          if (beforeComma[si] === ' ') {
            const possibleLastName = beforeComma.substring(si + 1).trim();
            const possibleOrg = beforeComma.substring(0, si).trim();

            if (/^[A-Z][a-zA-Z'-]+$/.test(possibleLastName) && possibleLastName.length >= 2) {
              if (afterComma && /^[A-Z]/.test(afterComma) && possibleOrg.length > 5) {
                bestSplit = { org: possibleOrg, lastName: possibleLastName };
              }
            }
          }
        }

        if (bestSplit && bestSplit.org) {
          orgName = bestSplit.org;
          nameStr = bestSplit.lastName + ',' + entryText.substring(commaIdx + 1);
          // Add to known orgs for future lookups
          if (!orgNameList.includes(orgName)) {
            orgNameList.push(orgName);
            orgNameList.sort((a, b) => b.length - a.length);
          }
        }
      }
    }

    // Update current org
    if (orgName) {
      currentOrg = orgName;
    }

    // Parse the name: "LastName, FirstName MiddleName"
    const commaPos = nameStr.indexOf(',');
    if (commaPos < 0) continue;

    const lastName = nameStr.substring(0, commaPos).trim();
    let firstName = nameStr.substring(commaPos + 1).trim();

    // Clean up first name
    firstName = firstName.replace(/\s*\(DHS only\)\s*$/i, '').trim();
    firstName = firstName.replace(/\s+/g, ' ').trim();

    // Validate: both names should be reasonable
    if (!lastName || !firstName) continue;
    if (lastName.length < 2 || firstName.length < 2) continue;
    if (/^\d/.test(lastName)) continue;

    const title = isDhsOnly
      ? 'Partially Accredited Representative'
      : 'Accredited Representative';

    // Look up org data
    const orgData = currentOrg ? orgs.get(currentOrg) : null;

    // If org not in lookup, try fuzzy match
    let matchedOrgData = orgData;
    if (!matchedOrgData && currentOrg) {
      // Strategy 1: substring match
      for (const [name, data] of orgs.entries()) {
        if (currentOrg.includes(name) || name.includes(currentOrg)) {
          matchedOrgData = data;
          break;
        }
      }
      // Strategy 2: strip office suffix and try again
      if (!matchedOrgData) {
        const stripped = currentOrg
          .replace(/[-–]\s*(Principal|Extension|Branch|Regional|Satellite|Washington,?\s*DC)?\s*Office.*$/i, '')
          .trim();
        if (stripped !== currentOrg) {
          matchedOrgData = orgs.get(stripped);
          if (!matchedOrgData) {
            for (const [name, data] of orgs.entries()) {
              if (stripped.includes(name) || name.includes(stripped)) {
                matchedOrgData = data;
                break;
              }
            }
          }
        }
      }
      // Strategy 3: try first N significant words
      if (!matchedOrgData) {
        const words = currentOrg.split(/\s+/).slice(0, 4).join(' ');
        if (words.length >= 10) {
          for (const [name, data] of orgs.entries()) {
            if (name.startsWith(words)) {
              matchedOrgData = data;
              break;
            }
          }
        }
      }
    }

    reps.push({
      first_name: firstName,
      last_name: lastName,
      firm_name: currentOrg || '',
      title,
      email: '',
      phone: matchedOrgData ? matchedOrgData.phone : '',
      website: '',
      domain: '',
      city: matchedOrgData ? matchedOrgData.city : '',
      state: matchedOrgData ? matchedOrgData.state : '',
      country: 'US',
      niche: 'immigration consultant',
      source: 'doj_eoir',
      profile_url: ''
    });
  }

  return reps;
}

// ============================================================================
// CSV Output
// ============================================================================

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCsv(reps, outputPath) {
  const header = CSV_HEADERS.join(',');
  const rows = reps.map(r =>
    CSV_HEADERS.map(col => escapeCsv(r[col])).join(',')
  );
  const csv = [header, ...rows].join('\n');
  fs.writeFileSync(outputPath, csv, 'utf8');
  console.log(`\nWrote ${reps.length} representatives to ${outputPath}`);
}

// ============================================================================
// Stats
// ============================================================================

function printStats(reps) {
  console.log('\n=== RESULTS ===');
  console.log(`Total representatives: ${reps.length}`);

  const withPhone = reps.filter(r => r.phone).length;
  const withFirm = reps.filter(r => r.firm_name).length;
  const withCity = reps.filter(r => r.city).length;

  const fullAccred = reps.filter(r => r.title === 'Accredited Representative').length;
  const partialAccred = reps.filter(r => r.title === 'Partially Accredited Representative').length;

  console.log(`  Accredited Representatives: ${fullAccred}`);
  console.log(`  Partially Accredited (DHS only): ${partialAccred}`);
  console.log(`  With phone: ${withPhone}`);
  console.log(`  With firm name: ${withFirm}`);
  console.log(`  With city/state: ${withCity}`);

  // Top states
  const stateCounts = {};
  for (const r of reps) {
    if (r.state) stateCounts[r.state] = (stateCounts[r.state] || 0) + 1;
  }
  const topStates = Object.entries(stateCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  console.log('\nTop 15 states:');
  for (const [st, cnt] of topStates) {
    console.log(`  ${st}: ${cnt}`);
  }

  // Sample entries
  console.log('\nSample entries (first 10):');
  for (const r of reps.slice(0, 10)) {
    console.log(`  ${r.first_name} ${r.last_name} | ${r.firm_name} | ${r.city}, ${r.state} | ${r.phone} | ${r.title}`);
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log('=== DOJ EOIR Accredited Representatives Scraper ===\n');

  // Step 1: Download PDF
  if (!fs.existsSync(PDF_PATH) || process.argv.includes('--force-download')) {
    await downloadPdf(PDF_URL, PDF_PATH);
  } else {
    const stats = fs.statSync(PDF_PATH);
    const ageHours = (Date.now() - stats.mtimeMs) / 3600000;
    if (ageHours > 24) {
      console.log('PDF is older than 24 hours, re-downloading...');
      await downloadPdf(PDF_URL, PDF_PATH);
    } else {
      console.log(`Using cached PDF at ${PDF_PATH} (${(stats.size / 1024).toFixed(0)} KB, ${ageHours.toFixed(1)}h old)`);
    }
  }

  // Step 2: Parse PDF
  console.log('\nParsing PDF...');
  const buf = fs.readFileSync(PDF_PATH);
  const data = await pdfParse(buf);
  console.log(`  ${data.numpages} pages, ${data.text.length} chars extracted`);

  // Normalize: replace em dashes with regular dashes for consistent matching
  const text = data.text.replace(/\u2013/g, '-').replace(/\u2014/g, '-');

  // Step 3: Find where rep section starts
  const repStartIdx = text.indexOf('Accredited RepresentativeAccreditation Expiration');
  if (repStartIdx < 0) {
    console.error('ERROR: Could not find representative section in PDF.');
    console.error('The PDF format may have changed. Try the HTML fallback.');
    process.exit(1);
  }
  console.log(`  Org section: 0 - ${repStartIdx} (${repStartIdx} chars)`);
  console.log(`  Rep section: ${repStartIdx} - ${text.length} (${text.length - repStartIdx} chars)`);

  // Step 4: Parse organizations
  const orgs = parseOrganizations(text, repStartIdx);

  // Step 5: Parse representatives
  console.log('\nParsing representatives...');
  const reps = parseRepresentatives(text, orgs);

  // Step 6: Deduplicate (same name + same org)
  const seen = new Set();
  const uniqueReps = [];
  for (const r of reps) {
    const key = `${r.first_name}|${r.last_name}|${r.firm_name}`.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      uniqueReps.push(r);
    }
  }
  console.log(`  ${reps.length} total entries, ${uniqueReps.length} unique (${reps.length - uniqueReps.length} duplicates removed)`);

  // Step 7: Output
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
  writeCsv(uniqueReps, OUTPUT_PATH);
  printStats(uniqueReps);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
