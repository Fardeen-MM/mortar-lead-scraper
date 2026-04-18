#!/usr/bin/env node
/**
 * Apollo Login Helper — Opens a real Chrome window for you to log in.
 * After login, saves session cookies to .env for the scraper to use.
 *
 * Usage:
 *   node scripts/apollo-login.js
 *
 * Then:
 *   1. Chrome opens → log into Apollo (Google sign-in works)
 *   2. Once you see the Apollo dashboard, come back here and press Enter
 *   3. Cookies are saved → you can now run apollo-scrape.js
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');
const readline = require('readline');

function askUser(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => rl.question(question, answer => { rl.close(); resolve(answer); }));
}

async function main() {
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  Apollo Login Helper');
  console.log('  A Chrome window will open — log into Apollo there.');
  console.log('  Google sign-in works. Come back here when done.');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  const profileDir = '/tmp/apollo-session-profile';

  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: profileDir,
    args: [
      '--no-sandbox',
      '--no-first-run',
      '--no-default-browser-check',
      '--window-size=1280,900',
      '--disable-backgrounding-occluded-windows',
    ],
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();
  await page.goto('https://app.apollo.io/#/login', { waitUntil: 'networkidle2', timeout: 60000 });

  console.log('  Chrome opened → Log into Apollo now (Google sign-in works)');
  console.log('  Waiting for you to log in...');
  console.log('');

  // Poll until logged in (up to 5 minutes)
  let loggedIn = false;
  for (let i = 0; i < 150; i++) {
    await new Promise(r => setTimeout(r, 2000));
    const url = page.url();
    const title = await page.title();
    if (!url.includes('/login') && !url.includes('/signup') && title.includes('Apollo')) {
      loggedIn = true;
      console.log(`  Detected login! Page: ${title}`);
      break;
    }
    if (i % 15 === 0 && i > 0) {
      console.log(`  Still waiting for login... (${i * 2}s)`);
    }
  }

  if (!loggedIn) {
    console.error('  Timed out waiting for login (5 min). Try again.');
    await browser.close();
    process.exit(1);
  }

  // Wait a bit more for the page to fully load
  await new Promise(r => setTimeout(r, 3000));

  // Extract cookies
  const cookies = await page.cookies('https://app.apollo.io');
  const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

  // Extract CSRF
  const csrfCookie = cookies.find(c => c.name === 'X-CSRF-TOKEN');
  const csrfToken = csrfCookie ? decodeURIComponent(csrfCookie.value) : '';

  // Quick test — make a search from within the browser
  console.log('');
  console.log('  Testing API access...');
  const testResult = await page.evaluate(async () => {
    try {
      const res = await fetch('/api/v1/mixed_people/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          person_titles: ['attorney'],
          person_locations: ['Florida, US'],
          person_seniorities: ['owner'],
          page: 1,
          per_page: 3,
        }),
        credentials: 'include',
      });
      if (!res.ok) return { error: `HTTP ${res.status}` };
      const data = await res.json();
      if (data.people && data.people[0]) {
        return {
          total: data.total_entries,
          first: `${data.people[0].first_name} ${data.people[0].last_name} | ${data.people[0].email || 'no email'} | ${data.people[0].title}`,
        };
      }
      return { error: 'No people in response' };
    } catch (e) {
      return { error: e.message };
    }
  });

  if (testResult.error) {
    console.log(`  API test FAILED: ${testResult.error}`);
    console.log('  The cookies might still work — saving anyway.');
  } else {
    console.log(`  API test PASSED! ${testResult.total} total results`);
    console.log(`  First: ${testResult.first}`);
  }

  // Save to .env
  const envPath = path.join(__dirname, '..', '.env');
  let envContent = fs.readFileSync(envPath, 'utf8');

  // Update or add APOLLO_CHROME_PROFILE
  if (envContent.includes('APOLLO_CHROME_PROFILE=')) {
    envContent = envContent.replace(/APOLLO_CHROME_PROFILE=.*/, `APOLLO_CHROME_PROFILE=${profileDir}`);
  } else {
    envContent += `\n# Apollo Chrome profile (created by apollo-login.js)\nAPOLLO_CHROME_PROFILE=${profileDir}\n`;
  }

  // Update session cookie
  if (envContent.includes('APOLLO_SESSION_COOKIE=')) {
    envContent = envContent.replace(/APOLLO_SESSION_COOKIE=.*/, `APOLLO_SESSION_COOKIE=${cookieStr.slice(0, 500)}`);
  } else {
    envContent += `\n# Apollo session cookie (auto-extracted)\nAPOLLO_SESSION_COOKIE=${cookieStr.slice(0, 500)}\n`;
  }

  if (csrfToken) {
    if (envContent.includes('APOLLO_CSRF_TOKEN=')) {
      envContent = envContent.replace(/APOLLO_CSRF_TOKEN=.*/, `APOLLO_CSRF_TOKEN=${csrfToken}`);
    }
  }

  fs.writeFileSync(envPath, envContent);

  console.log('');
  console.log('  Session saved! Profile dir: ' + profileDir);
  console.log('');
  console.log('  Now run:');
  console.log('    node scripts/apollo-scrape.js --titles "attorney,partner" --locations "Florida, US" --test');
  console.log('');

  await browser.close();
}

main().catch(e => {
  console.error('Error:', e.message);
  process.exit(1);
});
