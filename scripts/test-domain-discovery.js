#!/usr/bin/env node
const dns = require('dns');

function domainCandidates(firmName) {
  const slug = firmName.toLowerCase()
    .replace(/[&+]/g, 'and')
    .replace(/[^a-z0-9\s]/g, '')
    .trim()
    .replace(/\s+/g, '');

  const slugDash = firmName.toLowerCase()
    .replace(/[&+]/g, 'and')
    .replace(/[^a-z0-9\s]/g, '')
    .trim()
    .replace(/\s+/g, '-');

  return [
    slug + '.co.uk',
    slug + '.com',
    slugDash + '.co.uk',
    slugDash + '.com',
    slug + '.com.au',
    slug + '.net',
    slug + '.uk',
    slug + '.properties',
  ];
}

function checkDomain(domain) {
  return new Promise(resolve => {
    dns.resolve(domain, (err) => {
      resolve(err ? false : true);
    });
  });
}

const firms = [
  'Foxtons', 'Savills', 'Dexters', 'Purplebricks', 'Knight Frank',
  'Chestertons', 'Hunters', 'Connells', 'haart', 'Northwood',
  'Belvoir', 'OpenRent', 'Martin & Co', 'Reeds Rains', 'William H. Brown',
  'Leaders Lettings', 'Countrywide', 'Marsh & Parsons', 'Winkworth',
  'Jackson-Stops',
];

(async () => {
  let found = 0;
  for (const firm of firms) {
    const candidates = domainCandidates(firm);
    let domain = null;
    for (const d of candidates) {
      const exists = await checkDomain(d);
      if (exists) { domain = d; break; }
    }
    const status = domain ? '✓' : '✗';
    console.log(`  ${status} ${firm.padEnd(25)} -> ${domain || 'NOT FOUND'}`);
    if (domain) found++;
  }
  console.log(`\n  Found: ${found}/${firms.length} (${(found/firms.length*100).toFixed(0)}%)`);
})();
