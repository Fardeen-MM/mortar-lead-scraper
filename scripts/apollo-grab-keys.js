#!/usr/bin/env node
/**
 * Apollo Signup + API Key Grabber
 *
 * Signs up new accounts on Apollo via "Sign up with Microsoft",
 * skips onboarding, navigates to API settings, grabs the key.
 *
 * Usage:
 *   node scripts/apollo-grab-keys.js --max 3     # First 3 accounts
 *   node scripts/apollo-grab-keys.js              # All accounts
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

const CSV_PATH = '/Users/fardeenchoudhury/Downloads/smartlead_domains (6).csv';
const ENV_PATH = path.join(__dirname, '..', '.env');

const args = process.argv.slice(2);
const maxAccounts = parseInt(args[args.indexOf('--max') + 1]) || 0;
const skipAccounts = parseInt(args[args.indexOf('--skip') + 1]) || 0;

function parseCSV(content) {
  const lines = content.trim().split('\n');
  const accounts = [];
  for (let i = 1; i < lines.length; i++) {
    const match = lines[i].match(/^"([^"]+)","([^"]+)","([^"]+)"$/);
    if (match) accounts.push({ name: match[1], email: match[2], password: match[3] });
  }
  return accounts;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function signupAndGetKey(account) {
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: false,
      executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check', '--window-size=1280,900'],
      defaultViewport: null,
      protocolTimeout: 60000,
    });

    const page = (await browser.pages())[0] || await browser.newPage();

    // ── Step 1: Go to Apollo signup ──
    console.log('    Loading Apollo signup...');
    await page.goto('https://www.apollo.io/sign-up', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(3000);
    console.log('    Current URL: ' + page.url());

    // ── Step 2: Click "Sign Up with Microsoft" ──
    console.log('    Clicking Sign Up with Microsoft...');
    const msBtn = await page.evaluateHandle(() => {
      const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
      return btns.find(b => b.textContent.toLowerCase().includes('microsoft'));
    });

    if (!msBtn || !(await msBtn.asElement())) {
      // Try alternate selectors
      const altBtn = await page.$('[data-cy="microsoft-signup-btn"]') ||
                     await page.$('[data-cy="microsoft-login-btn"]') ||
                     await page.$('img[alt*="Microsoft" i]');
      if (altBtn) {
        await altBtn.click();
      } else {
        await browser.close();
        return { error: 'no_microsoft_button — may need to do first one manually' };
      }
    } else {
      await msBtn.click();
    }
    await sleep(5000);

    // ── Step 3: Microsoft OAuth — enter email ──
    console.log('    Microsoft login — entering email...');
    try {
      await page.waitForSelector('input[type="email"]', { timeout: 15000 });
    } catch {
      // Check if we're on a different page
      const url = page.url();
      if (!url.includes('microsoft') && !url.includes('login.live') && !url.includes('login.microsoftonline')) {
        await browser.close();
        return { error: 'microsoft_oauth_didnt_open: ' + url.split('?')[0] };
      }
    }

    const emailInput = await page.$('input[type="email"]');
    if (emailInput) {
      await emailInput.click({ clickCount: 3 });
      await emailInput.type(account.email, { delay: 30 });
      await sleep(500);

      // Click Next
      const nextBtn = await page.$('input[type="submit"]') || await page.$('#idSIButton9');
      if (nextBtn) await nextBtn.click();
      await sleep(4000);
    }

    // ── Step 4: Microsoft OAuth — enter password ──
    console.log('    Microsoft login — entering password...');
    try {
      await page.waitForSelector('input[type="password"]:not([style*="display: none"])', { timeout: 10000 });
    } catch {
      const html = await page.content();
      if (html.includes("doesn't exist") || html.includes('no account') || html.includes("couldn't find")) {
        await browser.close();
        return { error: 'microsoft_account_not_found' };
      }
    }

    const pwInput = await page.$('input[type="password"]');
    if (pwInput) {
      await pwInput.click({ clickCount: 3 });
      await pwInput.type(account.password, { delay: 30 });
      await sleep(500);

      const signInBtn = await page.$('input[type="submit"]') || await page.$('#idSIButton9');
      if (signInBtn) await signInBtn.click();
      await sleep(5000);
    }

    // ── Step 5: Handle Microsoft prompts + wait for Apollo ──
    console.log('    Handling Microsoft prompts + waiting for Apollo...');
    let landedOnApollo = false;
    for (let i = 0; i < 40; i++) {
      await sleep(2000);

      let url;
      try { url = page.url(); } catch { continue; } // Page may be navigating

      if (i % 5 === 0) console.log(`    ... waiting (${i}/40) — ${url.split('?')[0]}`);

      // If on Apollo login page, click "Log In with Microsoft" (account already exists)
      if (url.includes('apollo.io') && url.includes('/login')) {
        console.log('    Account already exists — clicking Log In with Microsoft...');
        try {
          const msLoginBtn = await page.evaluateHandle(() => {
            const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
            return btns.find(b => b.textContent.toLowerCase().includes('microsoft') && b.textContent.toLowerCase().includes('log'));
          });
          if (msLoginBtn && (await msLoginBtn.asElement())) {
            await msLoginBtn.click();
            await sleep(5000);
            continue; // Re-check URL after Microsoft login
          }
        } catch {}
      }

      // Check if we landed on Apollo (app or main site, post-signup/login)
      const isApollo = url.includes('app.apollo.io') || url.includes('apollo.io/');
      const isSignup = url.includes('/sign-up') || url.includes('#/signup');
      const isLogin = url.includes('/login') || url.includes('login');
      if (isApollo && !isSignup && !isLogin) {
        landedOnApollo = true;
        break;
      }

      // Click through "Stay signed in?" / "Accept" / consent prompts
      try {
        // Standard Microsoft buttons (#idSIButton9 = "Yes" for stay signed in)
        const submitBtn = await page.$('#idSIButton9');
        if (submitBtn) {
          await submitBtn.click();
          await sleep(2000);
        }
        // Consent page "Accept" button (different from #idSIButton9)
        const acceptBtn = await page.$('input[value="Accept"], button#idBtn_Accept');
        if (acceptBtn) {
          console.log('    Clicking Accept on consent page...');
          await acceptBtn.click();
          await sleep(3000);
        }
        // Generic accept/allow buttons on consent pages
        const genericAccept = await page.evaluateHandle(() => {
          const btns = [...document.querySelectorAll('button, input[type="submit"], input[type="button"]')];
          return btns.find(b => {
            const t = (b.textContent || b.value || '').toLowerCase().trim();
            return t === 'accept' || t === 'allow' || t === 'consent' || t === 'yes';
          });
        });
        if (genericAccept && (await genericAccept.asElement())) {
          console.log('    Clicking Accept/Allow...');
          await genericAccept.click();
          await sleep(3000);
        }
      } catch {} // Ignore errors from page navigations

      // Also check for pages in the browser (OAuth may open new tab)
      try {
        const pages = await browser.pages();
        for (const p of pages) {
          try {
            const pUrl = p.url();
            if (pUrl.includes('apollo.io') && !pUrl.includes('/login') && !pUrl.includes('/sign-up') && !pUrl.includes('#/signup')) {
              landedOnApollo = true;
              // Switch to this page
              if (p !== page) await p.bringToFront();
              break;
            }
          } catch {}
        }
        if (landedOnApollo) break;
      } catch {}
    }

    if (!landedOnApollo) {
      let finalUrl = '';
      try { finalUrl = page.url(); } catch {}
      // Take debug screenshot
      try {
        const ssPath = `/tmp/apollo-signup-debug-${Date.now()}.png`;
        const debugPage = (await browser.pages()).pop();
        await debugPage.screenshot({ path: ssPath, fullPage: true });
        console.log(`    Debug screenshot: ${ssPath}`);
      } catch {}
      await browser.close();
      return { error: 'never_reached_apollo: ' + finalUrl.split('?')[0] };
    }

    // Get the active Apollo page
    const allPages = await browser.pages();
    let apolloPage = page;
    for (const p of allPages) {
      try {
        if (p.url().includes('app.apollo.io')) { apolloPage = p; break; }
      } catch {}
    }
    await sleep(3000);

    // ── Step 7: Handle onboarding ──
    console.log('    Handling onboarding...');
    await sleep(3000);

    for (let attempt = 0; attempt < 20; attempt++) {
      const pageUrl = apolloPage.url();
      const pageTitle = await apolloPage.title().catch(() => '');
      const bodyText = await apolloPage.evaluate(() => document.body?.innerText?.slice(0, 500) || '').catch(() => '');

      // Check if we made it past onboarding — must be on a specific real Apollo page
      const isRealPage = pageUrl.includes('app.apollo.io') &&
                         (pageUrl.includes('#/home') || pageUrl.includes('#/people') ||
                          pageUrl.includes('#/settings') || pageUrl.includes('#/accounts') ||
                          pageUrl.includes('#/sequences') || pageUrl.includes('#/conversations') ||
                          pageUrl.includes('#/opportunities') || pageUrl.includes('#/enrichment'));
      if (isRealPage) {
        console.log('    Onboarding complete! URL: ' + pageUrl.split('?')[0]);
        break;
      }

      console.log(`    Onboarding step ${attempt + 1}: ${pageUrl.split('?')[0]}`);

      // If on signup-success interstitial, just wait for redirect
      if (pageUrl.includes('signup-success')) {
        await sleep(3000);
        continue;
      }

      // Screenshot every 5 steps for debugging
      if (attempt % 5 === 0 && attempt > 0) {
        const ssPath = `/tmp/apollo-onboard-${attempt}-${Date.now()}.png`;
        await apolloPage.screenshot({ path: ssPath });
        console.log(`    Debug: ${ssPath}`);
      }

      // Strategy: find all visible text inputs and fill them using Puppeteer .type()
      // This works better with React than setting .value directly
      const inputs = await apolloPage.$$('input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]):not([type="submit"])');
      for (let idx = 0; idx < inputs.length; idx++) {
        const input = inputs[idx];
        const info = await apolloPage.evaluate(el => ({
          value: el.value, placeholder: el.placeholder || '',
          visible: el.offsetParent !== null,
          parentText: el.parentElement?.textContent?.trim().slice(0, 80) || '',
        }), input);

        if (!info.visible || (info.value && info.value.trim())) continue;

        // Determine what to fill based on context
        const ctx = (info.placeholder + ' ' + info.parentText).toLowerCase();
        let fillValue = '';
        if (ctx.includes('full name') || ctx.includes('your name')) {
          fillValue = account.name;
        } else if (ctx.includes('company name') || ctx.includes('acme corp')) {
          fillValue = 'Mortar Metrics';
        } else if (ctx.includes('website') || ctx.includes('acme.com')) {
          fillValue = 'mortarmetrics.com';
        } else if (idx === 0 && !info.placeholder && bodyText.includes('Full name')) {
          // First input with no placeholder on a page that has "Full name" label
          fillValue = account.name;
        }

        if (fillValue) {
          console.log(`    Filling: "${fillValue}" (ctx: ${ctx.slice(0, 50)})`);
          await input.click({ clickCount: 3 });
          await input.type(fillValue, { delay: 20 });
          await sleep(300);
        }
      }

      // Handle role/pill selection screens (e.g., "Which best describes your role?")
      // Click a pill button if present (choose Founder/CEO or Marketing or first available)
      // IMPORTANT: Never click "Join" — that joins an existing workspace as non-admin
      const pillClicked = await apolloPage.evaluate(() => {
        // Look for pill-style buttons that aren't in a nav/header
        const pills = [...document.querySelectorAll('button, div[role="button"], span[role="button"]')].filter(el => {
          const t = el.textContent.trim().toLowerCase();
          const rect = el.getBoundingClientRect();
          // Pill buttons are usually small, in the main content area
          return rect.width > 50 && rect.width < 400 && rect.height > 20 && rect.height < 80 &&
                 rect.top > 200 && // Below header
                 !t.includes('continue') && !t.includes('skip') && !t.includes('next') &&
                 !t.includes('back') && !t.includes('sign') && t.length > 1 && t.length < 40 &&
                 t !== 'join' && !t.includes('join existing') && !t.includes('join team') &&
                 !t.includes('chrome') && !t.includes('extension') && !t.includes('install') &&
                 !t.includes('download') && !t.includes('upgrade') && !t.includes('log in') &&
                 !t.includes('sign up');
        });
        // Prefer "Founder/CEO" or "Marketing" or "Sales Leader" or "Create new"
        const preferred = pills.find(p => {
          const t = p.textContent.trim().toLowerCase();
          return t.includes('founder') || t.includes('marketing') || t.includes('sales leader') ||
                 t.includes('create new') || t.includes('create workspace') || t.includes('start fresh');
        });
        const target = preferred || pills[0];
        if (target) {
          target.click();
          return target.textContent.trim();
        }
        return null;
      });
      if (pillClicked) {
        console.log(`    Selected: "${pillClicked}"`);
        await sleep(1000);
      }

      // Handle "Join workspace" screen — look for "Create new" or "No thanks" alternatives
      const joinScreenHandled = await apolloPage.evaluate(() => {
        const text = document.body?.innerText || '';
        if (!text.toLowerCase().includes('join') || !text.toLowerCase().includes('workspace')) return false;
        // Look for "Create new workspace" or "No" or "Create my own" links/buttons
        const links = [...document.querySelectorAll('a, button, span')];
        const createOwn = links.find(l => {
          const t = l.textContent.trim().toLowerCase();
          return t.includes('create new') || t.includes('create my own') || t.includes('no thanks') ||
                 t.includes('start fresh') || t.includes('create a new') || t.includes("don't join") ||
                 t.includes('skip') || t.includes('create workspace');
        });
        if (createOwn) {
          createOwn.click();
          return true;
        }
        return false;
      });
      if (joinScreenHandled) {
        console.log('    Clicked "Create new workspace" instead of Join');
        await sleep(2000);
      }

      // Handle number input (e.g., "How many people are on your team?")
      const numberInput = await apolloPage.evaluateHandle(() => {
        const inputs = [...document.querySelectorAll('input[type="number"], input[type="text"]')];
        return inputs.find(i => {
          const label = (i.closest('label') || i.parentElement)?.textContent?.toLowerCase() || '';
          const placeholder = (i.placeholder || '').toLowerCase();
          return label.includes('how many') || label.includes('team size') ||
                 placeholder.includes('number') || label.includes('people');
        });
      });
      if (numberInput && (await numberInput.asElement())) {
        const numVal = await apolloPage.evaluate(el => el.value, numberInput);
        if (!numVal || numVal.trim() === '') {
          console.log('    Filling team size: 5');
          await numberInput.click({ clickCount: 3 });
          await numberInput.type('5', { delay: 30 });
          await sleep(500);
        }
      }

      // Click skip/continue/next buttons
      const actionBtn = await apolloPage.evaluateHandle(() => {
        const btns = [...document.querySelectorAll('button, a[role="button"], div[role="button"], a, span')];
        // Prefer skip/self-setup buttons first (fastest path through onboarding)
        const skip = btns.find(b => {
          const t = b.textContent.toLowerCase().trim();
          return t === 'skip' || t === 'skip for now' || t === 'skip this step' || t === 'maybe later' ||
                 t === "i'll setup on my own" || t === "i'll set up on my own" ||
                 t === 'set up on my own' || t === 'setup on my own' ||
                 t === 'go to homepage' || t === 'start searching';
        });
        if (skip) return skip;
        // Then continue/next
        return btns.find(b => {
          const t = b.textContent.toLowerCase().trim();
          return t === 'continue' || t === 'next' || t === 'get started' || t === 'submit' ||
                 t.includes('continue to') || t === 'start searching';
        });
      });

      if (actionBtn && (await actionBtn.asElement())) {
        const text = await apolloPage.evaluate(el => el.textContent.trim(), actionBtn);
        console.log(`    Clicking "${text}"...`);
        await actionBtn.click();
        await sleep(3000);
      } else {
        // No actionable button found — maybe page is loading or we're done
        await sleep(2000);
      }
    }

    // ── Step 7b: Force navigate past onboarding if stuck ──
    await sleep(2000);
    const currentUrl = apolloPage.url();
    if (currentUrl.includes('welcome') || currentUrl.includes('onboarding') || currentUrl.includes('sign-up')) {
      console.log('    Still on onboarding — force navigating to app...');
      await apolloPage.goto('https://app.apollo.io/#/settings/integrations/api', { waitUntil: 'networkidle2', timeout: 30000 });
      await sleep(5000);
    }

    // ── Step 8: Navigate to API Keys page ──
    console.log('    Navigating to API Developer Portal...');
    await apolloPage.goto('https://app.apollo.io/#/settings/integrations/api', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(5000);

    // Check if page loaded — if blank, try refreshing
    let pageContent = await apolloPage.evaluate(() => document.body?.innerText?.trim() || '');
    if (pageContent.length < 50) {
      console.log('    Page appears blank, refreshing...');
      await apolloPage.reload({ waitUntil: 'networkidle2', timeout: 30000 });
      await sleep(5000);
      pageContent = await apolloPage.evaluate(() => document.body?.innerText?.trim() || '');
    }

    // If still blank or redirected, try alternative navigation
    const currentApiUrl = apolloPage.url();
    console.log('    API page URL: ' + currentApiUrl.split('?')[0]);
    console.log('    Page content length: ' + pageContent.length);

    if (pageContent.length < 50 || !currentApiUrl.includes('settings')) {
      console.log('    Trying direct navigation to settings...');
      // Navigate to main app first, then settings
      await apolloPage.goto('https://app.apollo.io/#/home', { waitUntil: 'networkidle2', timeout: 30000 });
      await sleep(3000);
      await apolloPage.goto('https://app.apollo.io/#/settings/integrations/api', { waitUntil: 'networkidle2', timeout: 30000 });
      await sleep(5000);
    }

    // Click "API Keys" in the sidebar if we're on the Quick Start page
    const clickedApiKeys = await apolloPage.evaluate(() => {
      const links = [...document.querySelectorAll('a, div[role="button"], span, li')];
      const apiKeysLink = links.find(l => l.textContent.trim() === 'API Keys');
      if (apiKeysLink) { apiKeysLink.click(); return true; }
      return false;
    });
    if (clickedApiKeys) {
      console.log('    Clicked "API Keys" sidebar link...');
      await sleep(3000);
    }

    // ── Step 9: Extract API key ──
    console.log('    Looking for API key...');

    // Wait for API keys page to fully load (loading animation can take a while)
    for (let loadWait = 0; loadWait < 10; loadWait++) {
      const hasContent = await apolloPage.evaluate(() => {
        const text = document.body?.innerText || '';
        return text.includes('API key') || text.includes('Create new key') ||
               text.includes('No API keys') || text.includes('key_') ||
               text.length > 1000;
      });
      if (hasContent) break;
      await sleep(2000);
    }

    // Take a screenshot to see what we're working with
    const preScreenshot = `/tmp/apollo-apikeys-${Date.now()}.png`;
    await apolloPage.screenshot({ path: preScreenshot, fullPage: true });
    console.log(`    API Keys page screenshot: ${preScreenshot}`);

    // Blacklist of words that match the key regex but aren't keys
    const KEY_BLACKLIST = new Set([
      'HelpSupportDocumentation', 'QuickStart', 'OAuthRegistration',
      'OAuthPlayground', 'UpgradePlan', 'CreateNewKey', 'TermsOfService',
      'KnowledgeBase', 'GetStarted', 'Documentation',
    ]);

    // Helper: check if a string looks like an Apollo API key
    function looksLikeApiKey(text) {
      if (!text || text.length < 20 || text.length > 80) return false;
      if (!/^[a-zA-Z0-9_-]+$/.test(text)) return false;
      if (KEY_BLACKLIST.has(text)) return false;
      // Must have mixed case or special chars, not just camelCase words
      // Real Apollo keys look like: dwab7r-Z57tEwqHc9SGkuw or similar random strings
      if (/^[a-z]+([A-Z][a-z]+)+$/.test(text)) return false; // camelCase words
      if (/^[A-Z][a-z]+([A-Z][a-z]+)+$/.test(text)) return false; // PascalCase words
      // Must contain at least one digit or hyphen
      if (!/[\d-]/.test(text)) return false;
      return true;
    }

    // Step 9a: Click "Create new key" button if present
    const createBtn = await apolloPage.evaluateHandle(() => {
      const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
      return btns.find(b => {
        const t = b.textContent.toLowerCase().trim();
        return t.includes('create new key') || t.includes('create key') ||
               t.includes('generate key') || t.includes('new key');
      });
    });
    if (createBtn && (await createBtn.asElement())) {
      console.log('    Clicking "Create new key"...');
      await createBtn.click();
      await sleep(3000);

      // Fill in the key name in the modal
      const keyNameInput = await apolloPage.$('input[placeholder="Key name"], input[name="name"]');
      if (keyNameInput) {
        console.log('    Filling key name...');
        await keyNameInput.click({ clickCount: 3 });
        await keyNameInput.type('mortar-scraper', { delay: 20 });
        await sleep(500);
      } else {
        // Try any visible input in the modal
        const modalInput = await apolloPage.evaluateHandle(() => {
          const inputs = [...document.querySelectorAll('input[type="text"], input:not([type])')];
          return inputs.find(i => i.offsetParent !== null && !i.value);
        });
        if (modalInput && (await modalInput.asElement())) {
          console.log('    Filling key name (fallback)...');
          await modalInput.click({ clickCount: 3 });
          await modalInput.type('mortar-scraper', { delay: 20 });
          await sleep(500);
        }
      }

      // Click "Create API key" button in the modal
      const confirmBtn = await apolloPage.evaluateHandle(() => {
        const btns = [...document.querySelectorAll('button')];
        return btns.find(b => {
          const t = b.textContent.toLowerCase().trim();
          return t === 'create api key' || t === 'create key' || t === 'create';
        });
      });
      if (confirmBtn && (await confirmBtn.asElement())) {
        console.log('    Clicking "Create API key"...');
        await confirmBtn.click();
        await sleep(5000);
      }

      // Take screenshot after creating key
      const postCreate = `/tmp/apollo-postcreate-${Date.now()}.png`;
      await apolloPage.screenshot({ path: postCreate, fullPage: true });
      console.log(`    Post-create screenshot: ${postCreate}`);
    }

    // Step 9b: Extract session cookies (more useful than API key for free plan)
    console.log('    Extracting session cookies...');
    const cookies = await apolloPage.cookies();
    const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    const hasSession = cookies.some(c => c.name === '_leadgenie_session' || c.name === 'remember_token_leadgenie_v2');
    if (hasSession) {
      console.log(`    Session cookie extracted (${cookieStr.length} chars)`);
    }

    // Step 9c: Extract the API key
    let apiKey = await apolloPage.evaluate(() => {
      // Look in inputs first (highest confidence)
      const inputs = document.querySelectorAll('input, textarea');
      for (const el of inputs) {
        const val = (el.value || '').trim();
        if (val.length >= 20 && val.length <= 80 && /^[a-zA-Z0-9_-]+$/.test(val) && /[\d-]/.test(val)) {
          return val;
        }
      }
      // Look in code/pre/td elements
      const codeEls = document.querySelectorAll('code, pre, td, [class*="key"], [class*="token"], [class*="api"]');
      for (const el of codeEls) {
        if (el.children.length > 0) continue;
        const text = el.textContent.trim();
        if (text.length >= 20 && text.length <= 80 && /^[a-zA-Z0-9_-]+$/.test(text) && /[\d-]/.test(text)) {
          return text;
        }
      }
      // Look in any leaf text node
      const allEls = [...document.querySelectorAll('*')];
      for (const el of allEls) {
        if (el.children.length > 0) continue;
        const text = el.textContent.trim();
        if (text.length >= 20 && text.length <= 80 && /^[a-zA-Z0-9_-]+$/.test(text) && /[\d-]/.test(text)) {
          return text;
        }
      }
      return null;
    });

    if (apiKey && looksLikeApiKey(apiKey)) {
      console.log(`    Found API key: ${apiKey.slice(0, 10)}...`);
      await browser.close();
      return { key: apiKey, cookie: hasSession ? cookieStr : null };
    }

    // If still not found, try clicking "Copy" button or "Show" button
    const copyBtn = await apolloPage.evaluateHandle(() => {
      const btns = [...document.querySelectorAll('button')];
      return btns.find(b => {
        const t = b.textContent.toLowerCase().trim();
        return t === 'copy' || t.includes('copy key') || t.includes('show key');
      });
    });
    if (copyBtn && (await copyBtn.asElement())) {
      console.log('    Clicking copy/show button...');
      await copyBtn.click();
      await sleep(2000);
    }

    // If we have a cookie but no API key, that's still useful
    if (hasSession) {
      console.log('    No API key found, but session cookie is valid');
      await browser.close();
      return { key: null, cookie: cookieStr };
    }

    // Last resort: take a screenshot for debugging
    const ssPath = `/tmp/apollo-key-debug-${Date.now()}.png`;
    await apolloPage.screenshot({ path: ssPath, fullPage: true });
    console.log(`    Screenshot saved: ${ssPath}`);

    await browser.close();
    return { error: 'key_not_found_on_page' };

  } catch (e) {
    if (browser) try { await browser.close(); } catch {}
    return { error: e.message.slice(0, 120) };
  }
}

async function main() {
  const csv = fs.readFileSync(CSV_PATH, 'utf8');
  let accounts = parseCSV(csv);
  if (skipAccounts > 0) accounts = accounts.slice(skipAccounts);
  if (maxAccounts > 0) accounts = accounts.slice(0, maxAccounts);

  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  Apollo Signup + API Key Grabber (Microsoft OAuth)');
  console.log(`  ${accounts.length} accounts to process`);
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  const keys = [];
  const sessionCookies = [];
  const errors = [];

  // Run in parallel batches
  const CONCURRENCY = parseInt(args[args.indexOf('--parallel') + 1]) || 3;
  console.log(`  Running ${CONCURRENCY} browsers in parallel\n`);

  for (let batch = 0; batch < accounts.length; batch += CONCURRENCY) {
    const batchAccounts = accounts.slice(batch, batch + CONCURRENCY);
    const batchNum = Math.floor(batch / CONCURRENCY) + 1;
    console.log(`\n  ── Batch ${batchNum} (${batchAccounts.length} accounts) ──`);

    const promises = batchAccounts.map((acc, idx) => {
      const globalIdx = batch + idx;
      console.log(`  [${globalIdx + 1}/${accounts.length}] ${acc.email}`);
      return signupAndGetKey(acc).then(result => ({ acc, result, globalIdx }));
    });

    const results = await Promise.all(promises);

    let batchFailed = 0;
    for (const { acc, result, globalIdx } of results) {
      if (result.key) {
        console.log(`  [${globalIdx + 1}] ✓ API Key: ${result.key}`);
        keys.push(result.key);
      }
      if (result.cookie) {
        console.log(`  [${globalIdx + 1}] ✓ Session cookie captured`);
        sessionCookies.push({ email: acc.email, cookie: result.cookie });
      }
      if (!result.key && !result.cookie) {
        console.log(`  [${globalIdx + 1}] ✗ ${result.error}`);
        errors.push({ email: acc.email, error: result.error });
        batchFailed++;
      }
    }

    // If entire first batch fails, stop
    if (batch === 0 && batchFailed === batchAccounts.length) {
      console.log('\n  Entire first batch failed — stopping to debug.');
      break;
    }

    await sleep(2000);
  }

  // ── Save results ──
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  Results: ${keys.length} keys, ${sessionCookies.length} cookies, ${errors.length} errors`);
  console.log('═══════════════════════════════════════════════════════════');

  if (keys.length > 0) {
    const envContent = fs.readFileSync(ENV_PATH, 'utf8');
    const existingMatch = envContent.match(/^APOLLO_API_KEYS=(.*)$/m);
    const existingKeys = existingMatch ? existingMatch[1].split(',').filter(Boolean) : [];
    const allKeys = [...new Set([...existingKeys, ...keys])];

    const newEnv = envContent.replace(
      /^APOLLO_API_KEYS=.*$/m,
      `APOLLO_API_KEYS=${allKeys.join(',')}`
    );
    fs.writeFileSync(ENV_PATH, newEnv);
    console.log(`\n  API keys saved to .env (${allKeys.length} total)`);
  }

  // Save session cookies to a JSON file for browser-based scraping
  if (sessionCookies.length > 0) {
    const cookiePath = path.join(__dirname, '..', 'data', 'apollo-cookies.json');
    let existing = [];
    try { existing = JSON.parse(fs.readFileSync(cookiePath, 'utf8')); } catch {}
    const merged = [...existing, ...sessionCookies];
    // Dedup by email
    const seen = new Set();
    const unique = merged.filter(c => { if (seen.has(c.email)) return false; seen.add(c.email); return true; });
    fs.mkdirSync(path.dirname(cookiePath), { recursive: true });
    fs.writeFileSync(cookiePath, JSON.stringify(unique, null, 2));
    console.log(`  Session cookies saved to data/apollo-cookies.json (${unique.length} accounts)`);
    console.log(`  Each cookie enables browser-mode scraping with full access`);
  }

  if (errors.length > 0) {
    console.log('\n  Failed:');
    for (const e of errors) console.log(`    ${e.email} — ${e.error}`);
  }

  console.log('');
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
