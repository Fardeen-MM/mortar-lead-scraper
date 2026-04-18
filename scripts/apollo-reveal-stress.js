#!/usr/bin/env node
/**
 * Apollo Reveal Stress Test — How many emails can we actually reveal?
 * Test if the 100 credit limit is hard or soft, and find the real ceiling.
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

  let totalRevealed = 0;
  let totalCreditsUsed = 0;
  let pageNum = 1;
  const allRevealed = [];

  console.log('Starting bulk reveal test...\n');

  // Keep revealing until we hit a wall
  for (let round = 0; round < 40; round++) { // 40 rounds × 25 = 1000 max
    // Search for people
    const search = await xhrPost(page, '/api/v1/mixed_people/search', {
      person_titles: ['attorney'],
      person_locations: ['Florida, US'],
      person_seniorities: ['owner'],
      page: pageNum,
      per_page: 25,
    });

    if (!search.data || !search.data.people || search.data.people.length === 0) {
      // Try next page or different query
      pageNum++;
      if (pageNum > 10) {
        // Switch to different state
        const states = ['New York, US', 'Texas, US', 'California, US', 'Georgia, US', 'Illinois, US'];
        const stateIdx = Math.floor((pageNum - 11) / 5);
        if (stateIdx >= states.length) { console.log('Ran out of search results'); break; }

        const altSearch = await xhrPost(page, '/api/v1/mixed_people/search', {
          person_titles: ['attorney'],
          person_locations: [states[stateIdx]],
          person_seniorities: ['owner'],
          page: ((pageNum - 11) % 5) + 1,
          per_page: 25,
        });

        if (!altSearch.data || !altSearch.data.people || altSearch.data.people.length === 0) {
          pageNum++;
          continue;
        }
        search.data = altSearch.data;
      } else {
        continue;
      }
    }

    const people = search.data.people || [];

    // Filter out already-prospected contacts
    const contacts = search.data.contacts || [];
    const prospectedIds = new Set(contacts.map(c => c.person_id || c.id));
    const newPeople = people.filter(p => !prospectedIds.has(p.id));

    if (newPeople.length === 0) {
      pageNum++;
      continue;
    }

    // Reveal this batch
    const ids = newPeople.map(p => p.id);
    const result = await xhrPost(page, '/api/v1/mixed_people/add_to_my_prospects', {
      entity_ids: ids,
    });

    if (!result.data) {
      console.log(`Round ${round + 1}: ERROR - ${result.error || 'unknown'}`);
      if (result.raw) console.log('  Raw:', result.raw.slice(0, 200));
      // This might be the credit limit
      break;
    }

    const d = result.data;

    // Check for error/limit message
    if (d.error_message) {
      console.log(`\n*** HIT LIMIT: ${d.error_message} ***`);
      break;
    }
    if (d.message) {
      console.log(`\n*** MESSAGE: ${d.message} ***`);
    }

    const revealedContacts = d.contacts || [];
    const emailsFound = revealedContacts.filter(c => c.email && !c.email.includes('not_unlocked')).length;
    const phonesFound = revealedContacts.filter(c => c.phone_numbers && c.phone_numbers.length > 0).length;

    totalRevealed += revealedContacts.length;
    totalCreditsUsed = d.num_prospected_people_this_billing_cycle || totalCreditsUsed;

    for (const c of revealedContacts) {
      allRevealed.push({
        name: `${c.first_name} ${c.last_name}`,
        email: c.email,
        phones: (c.phone_numbers || []).length,
      });
    }

    console.log(`Round ${round + 1} (page ${pageNum}): +${revealedContacts.length} contacts | ${emailsFound} emails | ${phonesFound} phones | Credits used: ${totalCreditsUsed} | Total: ${totalRevealed}`);

    // Show sample
    if (revealedContacts.length > 0) {
      const sample = revealedContacts[0];
      console.log(`  Sample: ${sample.first_name} ${sample.last_name} | ${sample.email} | ${(sample.phone_numbers||[]).map(p=>p.sanitized_number).join(',')}`);
    }

    pageNum++;
    await new Promise(r => setTimeout(r, 1500)); // Delay between batches
  }

  // Final summary
  const withEmail = allRevealed.filter(r => r.email && !r.email.includes('not_unlocked')).length;
  const withPhone = allRevealed.filter(r => r.phones > 0).length;

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`  Total revealed:    ${totalRevealed}`);
  console.log(`  With real email:   ${withEmail} (${Math.round(withEmail/Math.max(totalRevealed,1)*100)}%)`);
  console.log(`  With phone:        ${withPhone} (${Math.round(withPhone/Math.max(totalRevealed,1)*100)}%)`);
  console.log(`  Credits used:      ${totalCreditsUsed}`);
  console.log('═══════════════════════════════════════════════════════════');

  await browser.close();
})().catch(e => { console.error('ERROR:', e.message); process.exit(1); });


function xhrPost(page, endpoint, body) {
  return page.evaluate((ep, bodyStr) => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', ep);
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.timeout = 20000;
      xhr.onload = () => {
        const text = xhr.responseText;
        if (text.trim().startsWith('<')) {
          resolve({ error: 'cloudflare', raw: text.slice(0, 100) });
          return;
        }
        try { resolve({ status: xhr.status, data: JSON.parse(text) }); }
        catch (e) { resolve({ status: xhr.status, error: 'parse', raw: text.slice(0, 200) }); }
      };
      xhr.onerror = () => resolve({ error: 'network' });
      xhr.ontimeout = () => resolve({ error: 'timeout' });
      xhr.send(bodyStr);
    });
  }, endpoint, JSON.stringify(body));
}
