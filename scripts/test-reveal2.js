const https = require('https');
const fs = require('fs');
const path = require('path');

const env = fs.readFileSync(path.join(__dirname, '..', '.env'), 'utf8');
const cookie = env.match(/^APOLLO_COOKIE=(.+)$/m)?.[1];
const csrf = cookie.match(/X-CSRF-TOKEN=([^;]+)/)?.[1] ? decodeURIComponent(cookie.match(/X-CSRF-TOKEN=([^;]+)/)[1]) : '';

const personId = '66f3521cf5594e00017168c8'; // Shannon Sagan from search

// Try multiple reveal endpoints
const endpoints = [
  { path: '/api/v1/people/match', body: { id: personId, reveal_personal_emails: false } },
  { path: '/api/v1/people/bulk_match', body: { details: [{ id: personId }] } },
  { path: '/api/v1/mixed_people/add_to_my_prospects', body: { entity_ids: [personId] } },
  { path: '/api/v1/contacts', body: { person_id: personId } },
];

async function tryEndpoint(ep) {
  return new Promise(resolve => {
    const data = JSON.stringify(ep.body);
    const req = https.request({
      hostname: 'app.apollo.io', path: ep.path, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Cookie': cookie, 'X-CSRF-TOKEN': csrf, 'Content-Length': Buffer.byteLength(data) },
      timeout: 10000,
    }, res => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        const emailFound = body.includes('@') && !body.includes('not_unlocked') && !body.includes('fake.com');
        console.log(`  ${ep.path} → ${res.statusCode} | email: ${emailFound ? 'YES' : 'no'} | ${body.slice(0, 200)}`);
        resolve();
      });
    });
    req.on('error', e => { console.log(`  ${ep.path} → ERROR: ${e.message}`); resolve(); });
    req.on('timeout', () => { req.destroy(); console.log(`  ${ep.path} → TIMEOUT`); resolve(); });
    req.write(data);
    req.end();
  });
}

async function main() {
  console.log('Testing reveal endpoints...\n');
  for (const ep of endpoints) {
    await tryEndpoint(ep);
  }
}

main();
