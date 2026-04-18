#!/usr/bin/env node
/**
 * Stress test Apollo people/match API — how many can we do?
 * Tests rate limits and credit consumption.
 */
const https = require('https');
const fs = require('fs');
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
          if (j.error) {
            resolve({ error: j.error, message: j.message || '' });
            return;
          }
          const p = j.person || {};
          resolve({
            email: p.email || '',
            status: p.email_status || '',
            matched: !!p.id,
            rateHeaders: {
              limit: res.headers['x-rate-limit-limit'],
              remaining: res.headers['x-rate-limit-remaining'],
              reset: res.headers['x-rate-limit-reset'],
            },
            httpStatus: res.statusCode,
          });
        } catch { resolve({ error: 'parse', raw: body.slice(0, 200) }); }
      });
    });
    req.on('error', e => resolve({ error: e.message }));
    req.write(data);
    req.end();
  });
}

async function main() {
  // Read leads from latest CSV
  const csvFiles = fs.readdirSync('output').filter(f => f.startsWith('apollo-turbo')).sort();
  const latestCSV = 'output/' + csvFiles[csvFiles.length - 1];
  console.log('Reading leads from:', latestCSV);

  const content = fs.readFileSync(latestCSV, 'utf8').trim();
  const lines = content.split('\n');

  // Parse CSV properly (handle quoted fields with commas)
  function parseCSVLine(line) {
    const cols = [];
    let current = '';
    let inQuote = false;
    for (let i = 0; i < line.length; i++) {
      if (line[i] === '"') { inQuote = !inQuote; continue; }
      if (line[i] === ',' && !inQuote) { cols.push(current.trim()); current = ''; continue; }
      current += line[i];
    }
    cols.push(current.trim());
    return cols;
  }

  const header = parseCSVLine(lines[0]);
  const fnIdx = header.indexOf('first_name');
  const lnIdx = header.indexOf('last_name');
  const domIdx = header.indexOf('domain');
  console.log('Columns:', { fnIdx, lnIdx, domIdx });

  const leads = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    const fn = cols[fnIdx] || '';
    const ln = cols[lnIdx] || '';
    const dom = cols[domIdx] || '';
    if (fn && ln && dom && ln.length > 2) leads.push({ fn, ln, dom });
  }

  console.log(`${leads.length} leads with name+domain\n`);

  const startTime = Date.now();
  let found = 0;
  let matched = 0;
  let errors = 0;
  let rateLimited = 0;

  // Process in quick succession (test rate limits)
  const BATCH = Math.min(leads.length, 100); // Test first 100
  for (let i = 0; i < BATCH; i++) {
    const { fn, ln, dom } = leads[i];
    const r = await match(fn, ln, dom);

    if (r.error) {
      errors++;
      if (r.httpStatus === 429 || (r.message && r.message.includes('rate'))) {
        rateLimited++;
        console.log(`  [RATE LIMIT] at request ${i + 1}: ${r.message || r.error}`);
        // Wait and retry
        await new Promise(r => setTimeout(r, 5000));
        i--; // Retry
        continue;
      }
      if (i < 5 || i % 20 === 0) console.log(`  [ERR] ${fn} ${ln} @ ${dom} → ${r.error}: ${r.message || ''}`);
      continue;
    }

    if (r.matched) matched++;
    const hasEmail = r.email && !r.email.includes('not_unlocked');
    if (hasEmail) found++;

    if (i < 10 || i % 10 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = Math.round((i + 1) / (elapsed / 60));
      console.log(`  [${i + 1}/${BATCH}] ${fn} ${ln} → ${r.email || 'none'} [${r.status}] | ${found} emails, ${rate}/min`);
    }

    // Small delay to be polite but test speed
    await new Promise(r => setTimeout(r, 300));
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const rate = Math.round(BATCH / (elapsed / 60));

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`  Tested:        ${BATCH} leads`);
  console.log(`  Matched:       ${matched} (${Math.round(matched/BATCH*100)}%)`);
  console.log(`  Emails found:  ${found} (${Math.round(found/BATCH*100)}%)`);
  console.log(`  Errors:        ${errors}`);
  console.log(`  Rate limited:  ${rateLimited}`);
  console.log(`  Speed:         ${rate}/min`);
  console.log(`  Time:          ${elapsed}s`);
  console.log('═══════════════════════════════════════════════════════════');
}

main().catch(e => console.error('Error:', e.message));
