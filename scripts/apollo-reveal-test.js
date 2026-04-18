#!/usr/bin/env node
/**
 * Test Apollo's email reveal/unlock mechanisms.
 * Apollo has endpoints to "reveal" contact info — free tier gets some credits.
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: '/tmp/apollo-session-profile',
    args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check', '--disable-extensions', '--window-size=1280,900'],
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 90000 });
  await new Promise(r => setTimeout(r, 3000));

  if (page.url().includes('/login')) {
    console.log('Not logged in!');
    await browser.close();
    process.exit(1);
  }

  // First, get a person ID to test with
  console.log('--- Getting a test person ---');
  const searchResult = await xhrPost(page, '/api/v1/mixed_people/search', {
    person_titles: ['attorney'],
    person_locations: ['Florida, US'],
    person_seniorities: ['owner'],
    page: 1,
    per_page: 5,
  });

  if (searchResult.error) {
    console.log('Search failed:', searchResult.error);
    await browser.close();
    return;
  }

  const person = searchResult.data.people[0];
  console.log(`Test person: ${person.first_name} ${person.last_name} | ${person.id}`);
  console.log(`Email: ${person.email} | Status: ${person.email_status}`);
  console.log(`Org: ${(person.organization || {}).name}`);
  console.log('');

  // Test 1: Try the "match" endpoint (enrichment)
  console.log('--- Test 1: /api/v1/people/match (enrichment) ---');
  const match = await xhrPost(page, '/api/v1/people/match', {
    first_name: person.first_name,
    last_name: person.last_name,
    organization_name: (person.organization || {}).name,
    domain: (person.organization || {}).primary_domain,
  });
  logResult('match', match);

  // Test 2: Try reveal endpoint
  console.log('\n--- Test 2: /api/v1/contacts/reveal ---');
  const reveal = await xhrPost(page, '/api/v1/contacts/reveal', {
    id: person.id,
  });
  logResult('reveal', reveal);

  // Test 3: Try mixed_people reveal
  console.log('\n--- Test 3: /api/v1/mixed_people/reveal ---');
  const reveal2 = await xhrPost(page, '/api/v1/mixed_people/reveal', {
    id: person.id,
  });
  logResult('reveal2', reveal2);

  // Test 4: Try getting person details
  console.log('\n--- Test 4: /api/v1/people/' + person.id + ' (GET) ---');
  const detail = await xhrGet(page, '/api/v1/people/' + person.id);
  logResult('detail', detail);

  // Test 5: Try contacts endpoint
  console.log('\n--- Test 5: /api/v1/contacts/search ---');
  const contacts = await xhrPost(page, '/api/v1/contacts/search', {
    person_ids: [person.id],
  });
  logResult('contacts', contacts);

  // Test 6: Try the emailer/verify endpoint
  console.log('\n--- Test 6: /api/v1/emailer/verify ---');
  const verify = await xhrPost(page, '/api/v1/emailer/verify', {
    email: person.first_name.toLowerCase() + '.' + person.last_name.toLowerCase() + '@' + ((person.organization || {}).primary_domain || 'example.com'),
  });
  logResult('verify', verify);

  // Test 7: Try bulk reveal
  console.log('\n--- Test 7: /api/v1/mixed_people/add_to_my_prospects ---');
  const prospect = await xhrPost(page, '/api/v1/mixed_people/add_to_my_prospects', {
    entity_ids: [person.id],
  });
  logResult('prospect', prospect);

  // Test 8: Check account credits
  console.log('\n--- Test 8: Check credits ---');
  const credits = await xhrGet(page, '/api/v1/auth/check');
  if (credits.data) {
    const d = credits.data;
    // Look for credit-related fields
    const creditKeys = Object.keys(d).filter(k =>
      k.includes('credit') || k.includes('usage') || k.includes('limit') ||
      k.includes('quota') || k.includes('plan') || k.includes('reveal')
    );
    console.log('Credit-related keys:', creditKeys);
    for (const k of creditKeys) {
      console.log(`  ${k}: ${JSON.stringify(d[k])}`);
    }
    // Also check team/org info
    if (d.team) {
      const teamCreditKeys = Object.keys(d.team).filter(k =>
        k.includes('credit') || k.includes('usage') || k.includes('limit') || k.includes('quota')
      );
      console.log('Team credit keys:', teamCreditKeys);
      for (const k of teamCreditKeys) {
        console.log(`  team.${k}: ${JSON.stringify(d.team[k])}`);
      }
    }
  }

  // Test 9: After adding to prospects, search again for same person
  console.log('\n--- Test 9: Re-search after prospect add ---');
  const reSearch = await xhrPost(page, '/api/v1/mixed_people/search', {
    person_titles: ['attorney'],
    person_locations: ['Florida, US'],
    person_seniorities: ['owner'],
    page: 1,
    per_page: 3,
  });
  if (reSearch.data && reSearch.data.people) {
    const p = reSearch.data.people[0];
    console.log(`Re-search: ${p.first_name} ${p.last_name} | Email: ${p.email} | Status: ${p.email_status}`);
    // Check if contacts array has revealed data
    if (reSearch.data.contacts && reSearch.data.contacts.length > 0) {
      console.log('Contacts in response:', reSearch.data.contacts.length);
      const c = reSearch.data.contacts[0];
      console.log('First contact email:', c.email);
    }
  }

  await browser.close();
})().catch(e => { console.error('ERROR:', e.message); process.exit(1); });


function xhrPost(page, endpoint, body) {
  return page.evaluate((ep, bodyStr) => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', ep);
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.timeout = 15000;
      xhr.onload = () => {
        try { resolve({ status: xhr.status, data: JSON.parse(xhr.responseText) }); }
        catch (e) {
          const isHtml = xhr.responseText.trim().startsWith('<');
          resolve({ status: xhr.status, error: isHtml ? 'Cloudflare HTML' : 'JSON parse error', raw: xhr.responseText.slice(0, 300) });
        }
      };
      xhr.onerror = () => resolve({ error: 'network' });
      xhr.ontimeout = () => resolve({ error: 'timeout' });
      xhr.send(bodyStr);
    });
  }, endpoint, JSON.stringify(body));
}

function xhrGet(page, endpoint) {
  return page.evaluate((ep) => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('GET', ep);
      xhr.withCredentials = true;
      xhr.timeout = 15000;
      xhr.onload = () => {
        try { resolve({ status: xhr.status, data: JSON.parse(xhr.responseText) }); }
        catch (e) {
          const isHtml = xhr.responseText.trim().startsWith('<');
          resolve({ status: xhr.status, error: isHtml ? 'Cloudflare HTML' : 'JSON parse error', raw: xhr.responseText.slice(0, 300) });
        }
      };
      xhr.onerror = () => resolve({ error: 'network' });
      xhr.ontimeout = () => resolve({ error: 'timeout' });
      xhr.send();
    });
  }, endpoint);
}

function logResult(name, result) {
  console.log(`Status: ${result.status || 'N/A'}`);
  if (result.error) {
    console.log(`Error: ${result.error}`);
    if (result.raw) console.log(`Raw: ${result.raw.slice(0, 200)}`);
    return;
  }
  const d = result.data;
  if (d.person) {
    console.log(`Person email: ${d.person.email} | Status: ${d.person.email_status}`);
    if (d.person.phone_numbers) console.log(`Phones: ${JSON.stringify(d.person.phone_numbers.slice(0, 2))}`);
    if (d.person.personal_emails) console.log(`Personal emails: ${JSON.stringify(d.person.personal_emails)}`);
  }
  if (d.contact) {
    console.log(`Contact email: ${d.contact.email}`);
    if (d.contact.phone_numbers) console.log(`Contact phones: ${JSON.stringify(d.contact.phone_numbers.slice(0, 2))}`);
  }
  if (d.email) console.log(`Email: ${d.email}`);
  if (d.contacts) console.log(`Contacts count: ${d.contacts.length}`);
  if (d.error_message) console.log(`Apollo error: ${d.error_message}`);
  if (d.message) console.log(`Message: ${d.message}`);
  // Show all top-level keys
  console.log(`Keys: ${Object.keys(d).join(', ')}`);
}
