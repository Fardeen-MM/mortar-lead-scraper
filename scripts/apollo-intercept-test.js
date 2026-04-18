#!/usr/bin/env node
/**
 * Apollo Intercept Test — Navigate Apollo's UI and intercept API responses.
 * Instead of making our own fetch() calls (which Cloudflare blocks),
 * we trigger Apollo's own JavaScript to make the API calls and capture the data.
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const profileDir = '/tmp/apollo-session-profile';

async function test() {
  console.log('Launching Chrome with saved profile...');
  const browser = await puppeteer.launch({
    headless: false,  // Visible window — Cloudflare trusts this
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: profileDir,
    args: [
      '--no-sandbox',
      '--no-first-run',
      '--no-default-browser-check',
      '--disable-extensions',
      '--window-size=1280,900',
    ],
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();

  // Step 1: Check if logged in
  console.log('Loading Apollo...');
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 60000 });
  await new Promise(r => setTimeout(r, 2000));

  const url = page.url();
  const title = await page.title();
  console.log('Title:', title);
  console.log('URL:', url.slice(0, 80));

  if (url.includes('/login')) {
    console.log('\nNot logged in! Run: node scripts/apollo-login.js');
    await browser.close();
    process.exit(1);
  }

  // Step 2: Set up response interception for mixed_people API calls
  console.log('\n--- Setting up API response interception ---');

  const capturedData = [];

  page.on('response', async (response) => {
    const respUrl = response.url();
    if (respUrl.includes('mixed_people/search') && response.request().method() === 'POST') {
      try {
        const ct = response.headers()['content-type'] || '';
        const status = response.status();
        console.log(`  Intercepted: ${status} | ${ct} | ${respUrl.slice(0, 80)}`);

        if (ct.includes('json') && status === 200) {
          const json = await response.json();
          capturedData.push(json);
          console.log(`  → ${(json.people || []).length} people, total: ${json.total_entries || 0}`);
          if (json.people && json.people[0]) {
            const p = json.people[0];
            console.log(`  → First: ${p.first_name} ${p.last_name} | ${p.email || 'no email'} | ${p.title}`);
            console.log(`  → City: ${p.city}, ${p.state} | LinkedIn: ${p.linkedin_url ? 'YES' : 'no'}`);
            console.log(`  → Org: ${(p.organization || {}).name || 'N/A'} | Domain: ${(p.organization || {}).primary_domain || 'N/A'}`);
          }
        }
      } catch (e) {
        console.log(`  Error reading response: ${e.message}`);
      }
    }
  });

  // Step 3: Navigate with filters in URL hash (triggers Apollo's own API call)
  console.log('\nNavigating with filters (attorney + Florida + owner)...');

  // Apollo's hash-based URL params
  const searchUrl = 'https://app.apollo.io/#/people?' + [
    'page=1',
    'personTitles[]=attorney',
    'personLocations[]=Florida%2C%20US',
    'personSeniorities[]=owner',
  ].join('&');

  await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 60000 });
  console.log('Waiting for Apollo to make API call...');
  await new Promise(r => setTimeout(r, 8000));

  console.log(`\nCaptured ${capturedData.length} API responses`);

  if (capturedData.length === 0) {
    // Try triggering search differently — click search or change hash
    console.log('\nNo intercepted responses. Trying hash change...');

    await page.evaluate(() => {
      window.location.hash = '#/people?page=1&personTitles[]=attorney&personLocations[]=Florida%2C%20US&personSeniorities[]=owner&_t=' + Date.now();
    });
    await new Promise(r => setTimeout(r, 8000));
    console.log(`Captured ${capturedData.length} API responses after hash change`);
  }

  // Step 4: Try page 2 by navigating
  if (capturedData.length > 0) {
    console.log('\nTrying page 2...');
    await page.evaluate(() => {
      window.location.hash = '#/people?page=2&personTitles[]=attorney&personLocations[]=Florida%2C%20US&personSeniorities[]=owner';
    });
    await new Promise(r => setTimeout(r, 8000));
    console.log(`Total captured: ${capturedData.length} responses`);
  }

  // Step 5: Also try programmatic search using cdp or XMLHttpRequest via page context
  console.log('\n--- Test: XHR from page context (instead of fetch) ---');
  const xhrResult = await page.evaluate(() => {
    return new Promise((resolve) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/v1/mixed_people/search');
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.onload = () => {
        const isJson = xhr.responseText.trim().startsWith('{') || xhr.responseText.trim().startsWith('[');
        resolve({
          status: xhr.status,
          isJson,
          isHtml: xhr.responseText.includes('<html') || xhr.responseText.includes('cf-turnstile'),
          preview: xhr.responseText.slice(0, 500),
          length: xhr.responseText.length,
        });
      };
      xhr.onerror = () => resolve({ status: 0, error: 'XHR error' });
      xhr.send(JSON.stringify({
        person_titles: ['attorney'],
        person_locations: ['Florida, US'],
        person_seniorities: ['owner'],
        page: 1,
        per_page: 5,
      }));
    });
  });

  console.log('XHR status:', xhrResult.status);
  console.log('Is JSON:', xhrResult.isJson, '| Is HTML:', xhrResult.isHtml);
  if (xhrResult.isJson) {
    try {
      const data = JSON.parse(xhrResult.preview.length === xhrResult.length ? xhrResult.preview : '{}');
      console.log('People:', (data.people || []).length, '| Total:', data.total_entries || 0);
    } catch {}
  } else {
    console.log('Preview:', xhrResult.preview.slice(0, 200));
  }

  // Summary
  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`Total API responses captured: ${capturedData.length}`);
  let totalPeople = 0;
  for (const d of capturedData) {
    totalPeople += (d.people || []).length;
  }
  console.log(`Total people across responses: ${totalPeople}`);

  if (capturedData.length > 0 && capturedData[0].people) {
    console.log('\nSample of captured people:');
    for (const p of capturedData[0].people.slice(0, 5)) {
      const org = p.organization || {};
      console.log(`  ${p.first_name} ${p.last_name} | ${p.email || 'no email'} | ${p.title} | ${org.primary_domain || 'no domain'}`);
    }
  }

  console.log('\nDone. Closing browser in 3 seconds...');
  await new Promise(r => setTimeout(r, 3000));
  await browser.close();
}

test().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
