#!/usr/bin/env node
/**
 * Test Apollo people/match API — does it return emails with a free API key?
 */
const https = require('https');
const API_KEY = 'dwab7r-Z57tEwqHc9SGkuw';

function match(first, last, domain) {
  return new Promise((resolve) => {
    const data = JSON.stringify({ first_name: first, last_name: last, domain: domain });
    const req = https.request({
      hostname: 'api.apollo.io',
      path: '/api/v1/people/match',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY, 'Content-Length': data.length }
    }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          const j = JSON.parse(body);
          const p = j.person || {};
          resolve({
            name: (p.first_name || '') + ' ' + (p.last_name || ''),
            email: p.email || 'none',
            status: p.email_status || '',
            title: p.title || '',
            matched: !!p.id,
          });
        } catch { resolve({ error: body.slice(0, 300) }); }
      });
    });
    req.on('error', e => resolve({ error: e.message }));
    req.write(data);
    req.end();
  });
}

async function main() {
  // Test with real attorney leads from our Florida scrape
  const tests = [
    ['Dalyla', 'Santos', 'santoslawpa.com'],
    ['Lorenzo', 'Jackson', 'thejacksonfirm.com'],
    ['Laura', 'Cabrera', '352immigration.com'],
    ['Brad', 'Gies', 'gieslaw.com'],
    ['Donna', 'Hung', 'donnahunglaw.com'],
    ['Cynthia', 'Swanson', 'swansonlawcenter.com'],
    ['Robyn', 'Lesser', 'robynlesserlaw.com'],
    ['Thomas', 'Nanna', 'tampabaydebtrelief.com'],
    ['Jon', 'Benjamin', 'benjaminforeclosurelaw.com'],
    ['Mark', 'Goldstein', 'bizavlaw.com'],
    ['Elizabeth', 'Wexler', 'wexlerlawfirm.com'],
    ['Tanya', 'Bell', 'belllawfirmflorida.com'],
    ['Andrea', 'Reyes', 'reyes-legal.com'],
    ['Brad', 'Fisher', 'bradgfisher.com'],
    ['David', 'Netburn', 'rolnicknetburn.com'],
  ];

  let found = 0;
  let matched = 0;
  for (const [f, l, d] of tests) {
    const r = await match(f, l, d);
    if (r.error) {
      console.log(`  ${f} ${l} @ ${d} → ERROR: ${r.error}`);
      continue;
    }
    if (r.matched) matched++;
    const hasEmail = r.email && r.email !== 'none' && !r.email.includes('not_unlocked');
    if (hasEmail) found++;
    const icon = hasEmail ? 'Y' : 'N';
    console.log(`  [${icon}] ${f} ${l} @ ${d} → ${r.email} [${r.status}]`);
    await new Promise(r => setTimeout(r, 600));
  }
  console.log(`\nResult: ${found}/${tests.length} emails found | ${matched}/${tests.length} matched in Apollo DB`);
}
main();
