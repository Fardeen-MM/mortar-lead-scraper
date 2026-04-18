const https = require('https');
const cookies = require('../data/apollo-cookies.json');
const cookie = cookies[0]?.cookie;
if (!cookie) { console.log('No cookies found'); process.exit(1); }

console.log('Using cookie from:', cookies[0].email);

const csrfMatch = cookie.match(/X-CSRF-TOKEN=([^;]+)/);
const csrf = csrfMatch ? decodeURIComponent(csrfMatch[1]) : '';
console.log('CSRF token:', csrf ? csrf.slice(0, 20) + '...' : 'NOT FOUND');

const data = JSON.stringify({
  q_keywords: 'attorney',
  person_locations: ['Florida, United States'],
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
    try {
      const j = JSON.parse(body);
      if (j.people && j.people.length > 0) {
        console.log('\nBROWSER SEARCH WORKS!');
        console.log('Total results:', j.pagination?.total_entries);
        for (const p of j.people) {
          console.log(`  ${p.first_name} ${p.last_name} | ${p.email || '[locked]'} | ${p.organization?.name || ''}`);
        }
      } else {
        console.log('Response:', JSON.stringify(j).slice(0, 500));
      }
    } catch(e) { console.log('Parse error:', body.slice(0, 300)); }
  });
});
req.on('error', e => console.log('Error:', e.message));
req.write(data);
req.end();
