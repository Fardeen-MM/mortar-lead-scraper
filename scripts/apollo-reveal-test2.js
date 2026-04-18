#!/usr/bin/env node
/**
 * Test Apollo email reveal via add_to_my_prospects.
 * Check credit limits and reveal multiple contacts.
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

  // Step 1: Search and get 25 people
  console.log('--- Searching for people ---');
  const search = await xhrPost(page, '/api/v1/mixed_people/search', {
    person_titles: ['attorney'],
    person_locations: ['Florida, US'],
    person_seniorities: ['owner'],
    page: 1,
    per_page: 25,
  });

  const people = search.data.people || [];
  console.log(`Found ${people.length} people`);

  // Check how many already have contacts (previously prospected)
  const contacts = search.data.contacts || [];
  console.log(`Already prospected: ${contacts.length}`);
  for (const c of contacts.slice(0, 3)) {
    console.log(`  Prospected: ${c.first_name} ${c.last_name} | ${c.email} | ${c.email_status}`);
  }

  // Step 2: Find people NOT yet prospected (no matching contact)
  const prospectedIds = new Set(contacts.map(c => c.person_id || c.id));
  const unprospected = people.filter(p => {
    // Check if this person is in the contacts array
    return contacts.every(c => c.id !== p.id && c.person_id !== p.id);
  });
  console.log(`\nUnprospected: ${unprospected.length}`);

  // Step 3: Try to prospect 5 people at once
  const toReveal = unprospected.slice(0, 5);
  if (toReveal.length === 0) {
    console.log('All people already prospected. Trying page 2...');
    // Try a different page
    const search2 = await xhrPost(page, '/api/v1/mixed_people/search', {
      person_titles: ['attorney'],
      person_locations: ['Florida, US'],
      person_seniorities: ['owner'],
      page: 2,
      per_page: 25,
    });
    const people2 = search2.data.people || [];
    toReveal.push(...people2.slice(0, 5));
  }

  if (toReveal.length > 0) {
    console.log(`\n--- Revealing ${toReveal.length} contacts ---`);
    const ids = toReveal.map(p => p.id);
    console.log('IDs:', ids);

    const revealResult = await xhrPost(page, '/api/v1/mixed_people/add_to_my_prospects', {
      entity_ids: ids,
    });

    console.log(`Status: ${revealResult.status}`);
    if (revealResult.data) {
      const d = revealResult.data;
      console.log(`Prospected this cycle: ${d.num_prospected_people_this_billing_cycle}`);
      console.log(`Contact IDs returned: ${(d.contact_ids || []).length}`);
      console.log(`Contacts returned: ${(d.contacts || []).length}`);

      // Check the revealed contacts
      const revealedContacts = d.contacts || [];
      for (const c of revealedContacts) {
        const email = c.email || 'no email';
        const phones = (c.phone_numbers || []).map(p => p.sanitized_number || p.raw_number).join(', ');
        console.log(`  REVEALED: ${c.first_name} ${c.last_name} | ${email} | ${c.email_status} | phones: ${phones || 'none'}`);
      }

      if (d.error_message) console.log(`Error: ${d.error_message}`);
      if (d.message) console.log(`Message: ${d.message}`);

      // Show all keys
      console.log(`\nAll keys: ${Object.keys(d).join(', ')}`);
    }
  }

  // Step 4: Check account limits
  console.log('\n--- Checking account/plan info ---');
  const auth = await xhrGet(page, '/api/v1/auth/check');
  if (auth.data) {
    const d = auth.data;
    // Dig through all nested objects for credit info
    const findCredits = (obj, prefix = '') => {
      for (const [k, v] of Object.entries(obj || {})) {
        const key = prefix ? `${prefix}.${k}` : k;
        if (typeof v === 'object' && v !== null && !Array.isArray(v)) {
          if (k.includes('credit') || k.includes('plan') || k.includes('usage') || k.includes('limit') || k.includes('prospect')) {
            console.log(`  ${key}:`, JSON.stringify(v).slice(0, 200));
          } else {
            findCredits(v, key);
          }
        } else if (
          k.includes('credit') || k.includes('plan') || k.includes('usage') ||
          k.includes('limit') || k.includes('prospect') || k.includes('quota') ||
          k.includes('reveal') || k.includes('unlock') || k.includes('export') ||
          k.includes('billing') || k.includes('tier')
        ) {
          console.log(`  ${key}: ${JSON.stringify(v)}`);
        }
      }
    };
    findCredits(d);
  }

  // Step 5: Check usage/billing endpoint
  console.log('\n--- Checking usage ---');
  const usage = await xhrGet(page, '/api/v1/usage');
  if (usage.data) {
    console.log('Usage keys:', Object.keys(usage.data));
    console.log('Usage data:', JSON.stringify(usage.data).slice(0, 500));
  } else if (usage.error) {
    console.log('Usage error:', usage.error);
  }

  const credits2 = await xhrGet(page, '/api/v1/credits/usage');
  if (credits2.data) {
    console.log('\nCredits usage:', JSON.stringify(credits2.data).slice(0, 500));
  }

  const plan = await xhrGet(page, '/api/v1/plans/current');
  if (plan.data) {
    console.log('\nCurrent plan:', JSON.stringify(plan.data).slice(0, 500));
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
        catch (e) { resolve({ status: xhr.status, error: 'parse', raw: xhr.responseText.slice(0, 200) }); }
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
        catch (e) { resolve({ status: xhr.status, error: 'parse', raw: xhr.responseText.slice(0, 200) }); }
      };
      xhr.onerror = () => resolve({ error: 'network' });
      xhr.send();
    });
  }, endpoint);
}
