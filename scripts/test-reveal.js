// Test if browser-based reveal works (uses UI credits, not API credits)
const https = require('https');
const fs = require('fs');
const path = require('path');

// Load cookie from .env
const env = fs.readFileSync(path.join(__dirname, '..', '.env'), 'utf8');
const cookieMatch = env.match(/^APOLLO_COOKIE=(.+)$/m);
if (!cookieMatch) { console.log('No APOLLO_COOKIE in .env'); process.exit(1); }
const cookie = cookieMatch[1];

const csrfMatch = cookie.match(/X-CSRF-TOKEN=([^;]+)/);
const csrf = csrfMatch ? decodeURIComponent(csrfMatch[1]) : '';
console.log('CSRF:', csrf ? 'found' : 'NOT FOUND');

// Step 1: Search for a person
function search() {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      q_keywords: 'attorney',
      person_locations: ['Florida, United States'],
      person_seniorities: ['owner'],
      page: 1,
      per_page: 3,
    });
    const req = https.request({
      hostname: 'app.apollo.io',
      path: '/api/v1/mixed_people/search',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Cookie': cookie,
        'X-CSRF-TOKEN': csrf,
        'Content-Length': Buffer.byteLength(data),
      },
      timeout: 15000,
    }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch { reject(body.slice(0, 200)); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// Step 2: Try to reveal email via add_to_my_prospects (browser endpoint)
function reveal(personId) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      entity_ids: [personId],
      skip_in_crm: true,
    });
    const req = https.request({
      hostname: 'app.apollo.io',
      path: '/api/v1/contacts/add_to_my_prospects',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Cookie': cookie,
        'X-CSRF-TOKEN': csrf,
        'Content-Length': Buffer.byteLength(data),
      },
      timeout: 15000,
    }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        console.log('  Status:', res.statusCode);
        console.log('  Body:', body.slice(0, 500));
        try { resolve(JSON.parse(body)); } catch { resolve({ raw: body.slice(0, 300) }); }
      });
    });
    req.on('error', e => { console.log('  Net error:', e.message); resolve({ error: e.message }); });
    req.on('timeout', () => { req.destroy(); console.log('  Timeout'); resolve({ error: 'timeout' }); });
    req.write(data);
    req.end();
  });
}

async function main() {
  console.log('\n1) Searching...');
  const searchResult = await search();

  if (!searchResult.people || searchResult.people.length === 0) {
    console.log('Search failed:', JSON.stringify(searchResult).slice(0, 300));
    return;
  }

  console.log(`Found ${searchResult.pagination?.total_entries} results`);
  const person = searchResult.people[0];
  console.log(`\nPerson: ${person.first_name} ${person.last_name}`);
  console.log(`Email (pre-reveal): ${person.email || '[locked]'}`);
  console.log(`ID: ${person.id}`);
  console.log(`Org: ${person.organization?.name}`);

  console.log('\n2) Attempting browser reveal...');
  const revealResult = await reveal(person.id);

  if (revealResult.contacts && revealResult.contacts.length > 0) {
    const c = revealResult.contacts[0];
    console.log(`\nREVEAL WORKED!`);
    console.log(`Email: ${c.email}`);
    console.log(`Phone: ${c.phone_numbers?.[0]?.sanitized_number || c.phone_number || 'none'}`);
  } else {
    console.log('Reveal response:', JSON.stringify(revealResult).slice(0, 500));
  }
}

main().catch(e => console.log('Error:', typeof e === 'string' ? e : e.message || JSON.stringify(e)));
