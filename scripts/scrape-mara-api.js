#!/usr/bin/env node
/**
 * Australia MARA Migration Agent Scraper — via OData API
 *
 * Uses the exposed Power Apps OData endpoint at portal.mara.gov.au/_api/contacts
 * NO Puppeteer, NO auth, NO cookies needed. Pure HTTP GET.
 *
 * ~80,000+ contacts, 99% with email, ~7 minutes total.
 * Paginates via alphabet-split: startswith(fullname,'A'), startswith(fullname,'B'), etc.
 * Letters with >5000 results split into 2-letter prefixes (Aa, Ab, ... Az).
 */

const https = require('https');
const fs = require('fs');

const BASE = 'https://portal.mara.gov.au/_api/contacts';
const FIELDS = 'fullname,firstname,lastname,emailaddress1,telephone1,mobilephone,address1_line1,address1_city,address1_stateorprovince,address1_postalcode,address1_country,company,dlg_crn,statecode';
const OUTPUT = 'output/australia-mara-agents-full.csv';
const DELAY = 2000;

function fetchContacts(filter) {
  const url = `${BASE}?$top=5000&$select=${FIELDS}&$filter=${encodeURIComponent(filter)}`;
  return new Promise(resolve => {
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
      timeout: 30000,
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          resolve(data.value || []);
        } catch { resolve([]); }
      });
      res.on('error', () => resolve([]));
    }).on('error', () => resolve([]));
  });
}

function esc(v) {
  const s = (v ?? '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

(async () => {
  console.log('Scraping MARA via OData API (pure HTTP, no Puppeteer)...\n');

  const all = [];
  const seen = new Set();
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const bigLetters = new Set(['A', 'J', 'M', 'R', 'S']); // >5000 results

  // Build query list
  const queries = [];
  for (const letter of alphabet) {
    if (bigLetters.has(letter)) {
      // Split into 2-letter prefixes
      for (const letter2 of alphabet) {
        queries.push(`statecode eq 0 and startswith(fullname,'${letter}${letter2.toLowerCase()}')`);
      }
    } else {
      queries.push(`statecode eq 0 and startswith(fullname,'${letter}')`);
    }
  }

  console.log(`  ${queries.length} queries to run\n`);

  for (let i = 0; i < queries.length; i++) {
    const contacts = await fetchContacts(queries[i]);

    for (const c of contacts) {
      const key = c.emailaddress1?.toLowerCase() || c.fullname?.toLowerCase() || '';
      if (!key || seen.has(key)) continue;
      seen.add(key);
      all.push(c);
    }

    if ((i + 1) % 10 === 0 || i < 3) {
      console.log(`  [${i + 1}/${queries.length}] ${all.length} unique agents (${contacts.length} this batch)`);
    }

    await new Promise(r => setTimeout(r, DELAY));
  }

  // Write CSV
  const header = 'name,email,phone,mobile,company,crn,address,city,state,postal,country';
  const rows = all.map(c => [
    esc(c.fullname), esc(c.emailaddress1), esc(c.telephone1), esc(c.mobilephone),
    esc(c.company), esc(c.dlg_crn),
    esc(c.address1_line1), esc(c.address1_city), esc(c.address1_stateorprovince),
    esc(c.address1_postalcode), esc(c.address1_country),
  ].join(','));

  fs.writeFileSync(OUTPUT, header + '\n' + rows.join('\n'));

  const withEmail = all.filter(c => c.emailaddress1).length;
  const withPhone = all.filter(c => c.telephone1 || c.mobilephone).length;

  console.log(`\n✓ Done: ${all.length} unique agents`);
  console.log(`  With email: ${withEmail} (${(withEmail/all.length*100).toFixed(1)}%)`);
  console.log(`  With phone: ${withPhone} (${(withPhone/all.length*100).toFixed(1)}%)`);
  console.log(`  → ${OUTPUT}`);
})();
