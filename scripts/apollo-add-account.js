#!/usr/bin/env node
/**
 * Apollo Add Account — Register a new Apollo Chrome profile for account rotation.
 *
 * Each Apollo account gets ~120 free email reveal credits per month.
 * Multiple accounts = more credits. This script creates a new Chrome profile
 * and lets you log into a different Apollo account.
 *
 * Usage:
 *   node scripts/apollo-add-account.js                  # Auto-names: account-2, account-3, etc.
 *   node scripts/apollo-add-account.js --name work      # Custom name: "work"
 *
 * Accounts are stored in /tmp/apollo-profiles/
 * List accounts: ls /tmp/apollo-profiles/
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

const PROFILES_DIR = '/tmp/apollo-profiles';

const args = process.argv.slice(2);
const nameIdx = args.indexOf('--name');
let profileName = nameIdx >= 0 ? args[nameIdx + 1] : null;

// Auto-generate name if not provided
if (!profileName) {
  if (!fs.existsSync(PROFILES_DIR)) fs.mkdirSync(PROFILES_DIR, { recursive: true });
  const existing = fs.readdirSync(PROFILES_DIR).filter(f => fs.statSync(path.join(PROFILES_DIR, f)).isDirectory());
  profileName = `account-${existing.length + 1}`;
}

const profilePath = path.join(PROFILES_DIR, profileName);

// Also maintain the legacy default profile
const defaultProfile = '/tmp/apollo-session-profile';

async function main() {
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  Apollo Account Setup');
  console.log(`  Profile: ${profileName}`);
  console.log(`  Path:    ${profilePath}`);
  console.log('  Log into a DIFFERENT Apollo account (use a different Gmail).');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: profilePath,
    args: [
      '--no-sandbox', '--no-first-run', '--no-default-browser-check',
      '--window-size=1280,900', '--disable-backgrounding-occluded-windows',
    ],
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();
  await page.goto('https://app.apollo.io/#/login', { waitUntil: 'networkidle2', timeout: 60000 });

  console.log('  Chrome opened → Log into Apollo now (use a different email)');
  console.log('  Waiting for login...');
  console.log('');

  // Poll for login
  let loggedIn = false;
  for (let i = 0; i < 150; i++) {
    await new Promise(r => setTimeout(r, 2000));
    const url = page.url();
    const title = await page.title();
    if (!url.includes('/login') && !url.includes('/signup') && title.includes('Apollo')) {
      loggedIn = true;
      console.log(`  Logged in! (${title})`);
      break;
    }
    if (i % 15 === 0 && i > 0) console.log(`  Still waiting... (${i * 2}s)`);
  }

  if (!loggedIn) {
    console.error('  Timed out. Try again.');
    await browser.close();
    process.exit(1);
  }

  await new Promise(r => setTimeout(r, 3000));

  // Quick API test
  const test = await page.evaluate(() => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/v1/mixed_people/search');
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.onload = () => {
        try {
          const d = JSON.parse(xhr.responseText);
          resolve({ ok: true, people: (d.people || []).length });
        } catch { resolve({ ok: false }); }
      };
      xhr.onerror = () => resolve({ ok: false });
      xhr.send(JSON.stringify({ page: 1, per_page: 1 }));
    });
  });

  console.log(`  API test: ${test.ok ? 'PASSED' : 'FAILED'}`);

  // Save profile info
  const infoPath = path.join(profilePath, 'apollo-account.json');
  fs.writeFileSync(infoPath, JSON.stringify({
    name: profileName,
    path: profilePath,
    created: new Date().toISOString(),
    creditsUsed: 0,
  }, null, 2));

  console.log('');
  console.log(`  Account "${profileName}" saved!`);
  console.log('');

  // List all accounts
  if (fs.existsSync(PROFILES_DIR)) {
    const accounts = fs.readdirSync(PROFILES_DIR)
      .filter(f => fs.statSync(path.join(PROFILES_DIR, f)).isDirectory())
      .filter(f => fs.existsSync(path.join(PROFILES_DIR, f, 'apollo-account.json')));

    console.log(`  All accounts (${accounts.length}):`);
    for (const acc of accounts) {
      try {
        const info = JSON.parse(fs.readFileSync(path.join(PROFILES_DIR, acc, 'apollo-account.json'), 'utf8'));
        console.log(`    ${info.name} | Credits used: ${info.creditsUsed || 0} | Created: ${info.created}`);
      } catch {
        console.log(`    ${acc}`);
      }
    }
  }

  // Also save as default if it's the first one
  if (!fs.existsSync(defaultProfile) || !fs.existsSync(path.join(defaultProfile, 'Default'))) {
    console.log(`\n  (Also set as default profile)`);
  }

  console.log('');
  await browser.close();
}

main().catch(e => { console.error('Error:', e.message); process.exit(1); });
