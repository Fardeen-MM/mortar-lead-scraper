#!/usr/bin/env node
/**
 * Apollo Browser Reveal — Bulk email enrichment using browser-mode reveals.
 *
 * Logs into Apollo with Microsoft accounts, uses each account's 75 free UI
 * credits to reveal verified emails via the add_to_my_prospects endpoint.
 * Auto-rotates accounts when credits exhaust (422 = 0 credits left).
 *
 * Input:  CSV from apollo-turbo.js (or any CSV with apollo_id/first_name/last_name columns)
 * Output: Enriched CSV with verified emails added
 *
 * Usage:
 *   node scripts/apollo-reveal-emails.js --input output/apollo-turbo_attorney_florida_*.csv
 *   node scripts/apollo-reveal-emails.js --input output/leads.csv --max 500
 *   node scripts/apollo-reveal-emails.js --input output/leads.csv --skip-accounts 5
 *
 * Options:
 *   --input          Input CSV file path (required)
 *   --output         Output CSV file path (default: adds -revealed suffix)
 *   --max            Max reveals to attempt
 *   --skip-accounts  Skip first N accounts from the credentials CSV
 *   --max-accounts   Max accounts to use
 *   --titles         Search titles for re-search (default: from CSV or "attorney")
 *   --state          State for re-search (default: from CSV or "Florida")
 *   --seniorities    Seniorities for re-search (default: "owner")
 *   --search         Search mode: scrape leads + reveal in one shot
 *   --test           Test mode (1 account, max 10 reveals)
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');
const { normalizePhone } = require('../lib/normalizer');

const CREDS_CSV = '/Users/fardeenchoudhury/Downloads/smartlead_domains (6).csv';
const CREDITS_PER_ACCOUNT = 75;

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const INPUT_CSV = getArg('input');
const OUTPUT_CSV = getArg('output');
const MAX_REVEALS = parseInt(getArg('max') || '0') || 0;
const SKIP_ACCOUNTS = parseInt(getArg('skip-accounts') || '0') || 0;
const MAX_ACCOUNTS = parseInt(getArg('max-accounts') || '0') || 0;
const SEARCH_MODE = hasFlag('search');
const TEST_MODE = hasFlag('test');
const TITLES = getArg('titles') ? getArg('titles').split(',').map(s => s.trim()) : ['attorney'];
const STATE = getArg('state') || 'Florida';
const SENIORITIES = getArg('seniorities') ? getArg('seniorities').split(',').map(s => s.trim()) : ['owner'];
const KEYWORDS = getArg('keywords') || '';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Parse account credentials CSV ─────────────────────────────────

function parseCredsCSV(content) {
  const lines = content.trim().split('\n');
  const accounts = [];
  for (let i = 1; i < lines.length; i++) {
    const match = lines[i].match(/^"([^"]+)","([^"]+)","([^"]+)"$/);
    if (match) accounts.push({ name: match[1], email: match[2], password: match[3] });
  }
  return accounts;
}

// ─── Parse input CSV ────────────────────────────────────────────────

function parseLeadsCSV(content) {
  const lines = content.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"/, '').replace(/"$/, ''));
  const leads = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = [];
    let current = '';
    let inQuotes = false;
    for (const ch of lines[i]) {
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === ',' && !inQuotes) { vals.push(current.trim()); current = ''; continue; }
      current += ch;
    }
    vals.push(current.trim());
    const lead = {};
    headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
    leads.push(lead);
  }
  return leads;
}

// ─── Microsoft OAuth Login ──────────────────────────────────────────

async function loginToApollo(browser, account) {
  const page = (await browser.pages())[0] || await browser.newPage();

  console.log(`    Loading Apollo signup/login...`);
  await page.goto('https://www.apollo.io/sign-up', { waitUntil: 'networkidle2', timeout: 30000 });
  await sleep(3000);

  // Click "Sign Up with Microsoft"
  const msBtn = await page.evaluateHandle(() => {
    const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
    return btns.find(b => b.textContent.toLowerCase().includes('microsoft'));
  });
  if (!msBtn || !(await msBtn.asElement())) {
    return { error: 'no_microsoft_button' };
  }
  await msBtn.click();
  await sleep(5000);

  // Microsoft OAuth — enter email
  console.log(`    Microsoft login — entering email...`);
  try {
    await page.waitForSelector('input[type="email"]', { timeout: 15000 });
  } catch {
    const url = page.url();
    if (!url.includes('microsoft') && !url.includes('login.live') && !url.includes('login.microsoftonline')) {
      return { error: 'microsoft_oauth_didnt_open' };
    }
  }

  const emailInput = await page.$('input[type="email"]');
  if (emailInput) {
    await emailInput.click({ clickCount: 3 });
    await emailInput.type(account.email, { delay: 30 });
    await sleep(500);
    const nextBtn = await page.$('input[type="submit"]') || await page.$('#idSIButton9');
    if (nextBtn) await nextBtn.click();
    await sleep(4000);
  }

  // Microsoft OAuth — enter password
  console.log(`    Microsoft login — entering password...`);
  try {
    await page.waitForSelector('input[type="password"]:not([style*="display: none"])', { timeout: 10000 });
  } catch {
    const html = await page.content();
    if (html.includes("doesn't exist") || html.includes('no account')) {
      return { error: 'account_not_found' };
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

  // Handle Microsoft prompts + wait for Apollo
  console.log(`    Waiting for Apollo redirect...`);
  let landedOnApollo = false;
  let loginAttempts = 0;
  for (let i = 0; i < 40; i++) {
    await sleep(2000);
    let url;
    try { url = page.url(); } catch { continue; }
    if (i % 5 === 0) console.log(`    ... (${i}/40) ${url.split('?')[0]}`);

    // If redirected to Apollo login page, click "Log In with Microsoft"
    if (url.includes('app.apollo.io') && url.includes('#/login') && loginAttempts < 3) {
      loginAttempts++;
      console.log(`    Account exists — clicking Log In with Microsoft (attempt ${loginAttempts})...`);
      const msLoginBtn = await page.evaluateHandle(() => {
        const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
        return btns.find(b => b.textContent.toLowerCase().includes('microsoft'));
      });
      if (msLoginBtn && (await msLoginBtn.asElement())) {
        await msLoginBtn.click();
        await sleep(8000); // Give more time for MS OAuth round-trip
        continue;
      }
    }

    // Check if we're on a real Apollo page (past login/signup)
    const isRealApollo = url.includes('app.apollo.io') &&
      !url.includes('#/login') && !url.includes('#/signup') &&
      !url.includes('/sign-up') && !url.includes('/login');
    if (isRealApollo) {
      landedOnApollo = true;
      break;
    }

    // Click through Microsoft "Stay signed in?" / consent prompts
    try {
      const submitBtn = await page.$('#idSIButton9');
      if (submitBtn) { await submitBtn.click(); await sleep(2000); }
      const acceptBtn = await page.$('input[value="Accept"], button#idBtn_Accept');
      if (acceptBtn) { await acceptBtn.click(); await sleep(3000); }
      // Generic accept/allow buttons
      const genAccept = await page.evaluateHandle(() => {
        const btns = [...document.querySelectorAll('button, input[type="submit"]')];
        return btns.find(b => {
          const t = (b.textContent || b.value || '').toLowerCase().trim();
          return t === 'accept' || t === 'allow' || t === 'yes';
        });
      });
      if (genAccept && (await genAccept.asElement())) { await genAccept.click(); await sleep(3000); }
    } catch {}

    // Check if MS OAuth opened a new tab that landed on Apollo
    try {
      const pages = await page.browser().pages();
      for (const p of pages) {
        try {
          const pUrl = p.url();
          if (pUrl.includes('app.apollo.io') && !pUrl.includes('#/login') && !pUrl.includes('/login')) {
            landedOnApollo = true;
            if (p !== page) { await p.bringToFront(); page = p; }
            break;
          }
        } catch {}
      }
      if (landedOnApollo) break;
    } catch {}
  }

  if (!landedOnApollo) {
    // Last resort: if we're stuck on #/login, try force-navigating
    const finalUrl = page.url();
    if (finalUrl.includes('app.apollo.io')) {
      console.log('    Stuck on login — trying force navigation to #/people...');
      await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 30000 });
      await sleep(3000);
      const checkUrl = page.url();
      if (checkUrl.includes('#/people') || checkUrl.includes('#/home')) {
        landedOnApollo = true;
      }
    }
    if (!landedOnApollo) {
      return { error: 'never_reached_apollo: ' + (page.url() || '').split('?')[0] };
    }
  }

  // Handle onboarding if needed
  const apolloPage = page;
  for (let attempt = 0; attempt < 20; attempt++) {
    const pageUrl = apolloPage.url();
    const isRealPage = pageUrl.includes('app.apollo.io') &&
                       (pageUrl.includes('#/home') || pageUrl.includes('#/people') ||
                        pageUrl.includes('#/settings') || pageUrl.includes('#/accounts') ||
                        pageUrl.includes('#/sequences') || pageUrl.includes('#/enrichment'));
    if (isRealPage) break;

    if (pageUrl.includes('signup-success')) { await sleep(3000); continue; }

    // Fill onboarding form fields
    const inputs = await apolloPage.$$('input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]):not([type="submit"])');
    for (let idx = 0; idx < inputs.length; idx++) {
      const input = inputs[idx];
      const info = await apolloPage.evaluate(el => ({
        value: el.value, placeholder: el.placeholder || '',
        visible: el.offsetParent !== null,
        parentText: el.parentElement?.textContent?.trim().slice(0, 80) || '',
      }), input);
      if (!info.visible || (info.value && info.value.trim())) continue;
      const ctx = (info.placeholder + ' ' + info.parentText).toLowerCase();
      let fillValue = '';
      if (ctx.includes('full name') || ctx.includes('your name')) fillValue = account.name;
      else if (ctx.includes('company name') || ctx.includes('acme corp')) fillValue = 'Mortar Metrics';
      else if (ctx.includes('website') || ctx.includes('acme.com')) fillValue = 'mortarmetrics.com';
      if (fillValue) {
        await input.click({ clickCount: 3 });
        await input.type(fillValue, { delay: 20 });
        await sleep(300);
      }
    }

    // Handle pill/chip selections (role, goals, etc.)
    await apolloPage.evaluate(() => {
      const pills = [...document.querySelectorAll('button, div[role="button"], span[role="button"]')].filter(el => {
        const t = el.textContent.trim().toLowerCase();
        const rect = el.getBoundingClientRect();
        return rect.width > 50 && rect.width < 400 && rect.height > 20 && rect.height < 80 &&
               rect.top > 200 && !t.includes('continue') && !t.includes('skip') && !t.includes('next') &&
               !t.includes('back') && !t.includes('sign') && t.length > 1 && t.length < 40 &&
               t !== 'join' && !t.includes('join existing') && !t.includes('chrome') && !t.includes('extension') &&
               !t.includes('install') && !t.includes('download') && !t.includes('upgrade');
      });
      const preferred = pills.find(p => {
        const t = p.textContent.trim().toLowerCase();
        return t.includes('founder') || t.includes('marketing') || t.includes('sales leader') ||
               t.includes('create new') || t.includes('create workspace');
      });
      (preferred || pills[0])?.click();
    });

    // Handle "Join workspace" — prefer "Create new workspace"
    await apolloPage.evaluate(() => {
      const links = [...document.querySelectorAll('a, button, span')];
      const createOwn = links.find(l => {
        const t = l.textContent.trim().toLowerCase();
        return t.includes('create new') || t.includes('create my own') || t.includes('create a new') ||
               t.includes('start fresh') || t.includes('create workspace');
      });
      createOwn?.click();
    });

    // Click skip/continue/next
    const actionBtn = await apolloPage.evaluateHandle(() => {
      const btns = [...document.querySelectorAll('button, a[role="button"], div[role="button"], a, span')];
      const skip = btns.find(b => {
        const t = b.textContent.toLowerCase().trim();
        return t === 'skip' || t === 'skip for now' || t === "i'll setup on my own" ||
               t === "i'll set up on my own" || t === 'go to homepage' || t === 'start searching';
      });
      if (skip) return skip;
      return btns.find(b => {
        const t = b.textContent.toLowerCase().trim();
        return t === 'continue' || t === 'next' || t === 'get started' ||
               t.includes('continue to') || t === 'start searching';
      });
    });
    if (actionBtn && (await actionBtn.asElement())) {
      await actionBtn.click();
      await sleep(2500);
    } else {
      await sleep(2000);
    }
  }

  // Force navigate to people page if still stuck
  if (!apolloPage.url().includes('#/people') && !apolloPage.url().includes('#/home')) {
    await apolloPage.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(3000);
  }

  // Verify we're actually logged in — do a quick test search
  console.log('    Verifying API access...');
  const testResult = await apolloPage.evaluate(() => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/v1/mixed_people/search');
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.timeout = 10000;
      xhr.onload = () => {
        try {
          const data = JSON.parse(xhr.responseText);
          resolve({ ok: !!(data.people && data.people.length > 0), total: data.pagination?.total_entries || 0 });
        } catch { resolve({ ok: false, error: 'parse' }); }
      };
      xhr.onerror = () => resolve({ ok: false, error: 'network' });
      xhr.ontimeout = () => resolve({ ok: false, error: 'timeout' });
      xhr.send(JSON.stringify({
        person_titles: ['attorney'],
        person_locations: ['Florida, US'],
        page: 1, per_page: 1,
      }));
    });
  });

  if (!testResult.ok) {
    console.log(`    API test failed: ${testResult.error || 'no results'} — URL: ${apolloPage.url()}`);
    // Take debug screenshot
    const ssPath = `/tmp/apollo-reveal-debug-${Date.now()}.png`;
    await apolloPage.screenshot({ path: ssPath, fullPage: true });
    console.log(`    Debug screenshot: ${ssPath}`);
    return { error: 'api_not_working', page: apolloPage };
  }

  console.log(`    API verified! (${testResult.total} results available)`);
  return { page: apolloPage, success: true };
}

// ─── Browser-based search (reuse from turbo) ───────────────────────

async function browserSearch(page, filters, seenIds) {
  const people = [];
  let totalAvailable = 0;

  for (let pg = 1; pg <= 5; pg++) {
    let result;
    let retries = 0;

    while (retries < 3) {
      result = await page.evaluate((filtersStr, pageNum) => {
        return new Promise(resolve => {
          const xhr = new XMLHttpRequest();
          xhr.open('POST', '/api/v1/mixed_people/search');
          xhr.setRequestHeader('Content-Type', 'application/json');
          xhr.withCredentials = true;
          xhr.timeout = 20000;
          xhr.onload = () => {
            const text = xhr.responseText;
            if (text.trim().startsWith('<') || text.includes('cf-turnstile')) {
              resolve({ people: [], total: 0, totalPages: 0, cloudflare: true });
              return;
            }
            if (xhr.status === 429) {
              resolve({ people: [], total: 0, totalPages: 0, rateLimit: true });
              return;
            }
            try {
              const data = JSON.parse(text);
              const pag = data.pagination || {};
              resolve({
                people: (data.people || []).map(p => {
                  const org = p.organization || {};
                  return {
                    apollo_id: p.id,
                    first_name: p.first_name || '',
                    last_name: p.last_name || '',
                    title: p.title || '',
                    headline: p.headline || '',
                    seniority: p.seniority || '',
                    linkedin_url: p.linkedin_url || '',
                    city: p.city || '',
                    state: p.state || '',
                    country: p.country || '',
                    email: (p.email && !p.email.includes('not_unlocked')) ? p.email : '',
                    email_status: p.email_status || '',
                    firm_name: org.name || '',
                    website: org.website_url || '',
                    domain: org.primary_domain || '',
                    phone: org.sanitized_phone || org.primary_phone || '',
                  };
                }),
                total: pag.total_entries || 0,
                totalPages: pag.total_pages || 0,
              });
            } catch (e) { resolve({ people: [], total: 0, totalPages: 0, error: e.message }); }
          };
          xhr.onerror = () => resolve({ people: [], total: 0, totalPages: 0, error: 'network' });
          xhr.ontimeout = () => resolve({ people: [], total: 0, totalPages: 0, error: 'timeout' });
          const body = JSON.parse(filtersStr);
          body.page = pageNum;
          body.per_page = 25;
          xhr.send(JSON.stringify(body));
        });
      }, JSON.stringify(filters), pg);

      if (result.cloudflare) { retries++; await sleep(3000 * retries); continue; }
      if (result.rateLimit) { await sleep(30000); retries++; continue; }
      break;
    }

    if (!result || result.cloudflare) break;
    if (pg === 1) totalAvailable = result.total;
    if (result.people.length === 0) break;

    for (const p of result.people) {
      if (p.apollo_id && !seenIds.has(p.apollo_id)) {
        if (!p.domain && p.website) {
          try { p.domain = new URL(p.website).hostname.replace(/^www\./, ''); } catch {}
        }
        people.push(p);
        seenIds.add(p.apollo_id);
      }
    }

    if (pg >= result.totalPages) break;
    await sleep(800);
  }

  return { people, totalAvailable };
}

// ─── Browser-based email reveal ─────────────────────────────────────

async function revealEmail(page, apolloId) {
  return await page.evaluate((personId) => {
    return new Promise(resolve => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/v1/mixed_people/add_to_my_prospects');
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.withCredentials = true;
      xhr.timeout = 15000;
      xhr.onload = () => {
        try {
          const data = JSON.parse(xhr.responseText);
          if (xhr.status === 422) {
            // Check for credit exhaustion
            const msg = (data.message || data.error || JSON.stringify(data)).toLowerCase();
            if (msg.includes('credit') || msg.includes('limit') || msg.includes('upgrade')) {
              resolve({ credits_exhausted: true });
              return;
            }
            resolve({ error: msg });
            return;
          }
          if (data.contacts && data.contacts.length > 0) {
            const c = data.contacts[0];
            const email = (c.email && !c.email.includes('not_unlocked')) ? c.email : '';
            const phone = c.phone_numbers?.[0]?.sanitized_number || c.sanitized_phone || '';
            resolve({ email, phone, email_status: c.email_status || '' });
            return;
          }
          // Check if the person data is returned directly
          if (data.person) {
            const p = data.person;
            const email = (p.email && !p.email.includes('not_unlocked')) ? p.email : '';
            resolve({ email, email_status: p.email_status || '' });
            return;
          }
          resolve({ error: 'no_contact_in_response' });
        } catch (e) {
          resolve({ error: 'parse_error: ' + xhr.responseText.slice(0, 100) });
        }
      };
      xhr.onerror = () => resolve({ error: 'network_error' });
      xhr.ontimeout = () => resolve({ error: 'timeout' });
      xhr.send(JSON.stringify({
        entity_ids: [personId],
        skip_in_crm: true,
      }));
    });
  }, apolloId);
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  // Load credentials
  if (!fs.existsSync(CREDS_CSV)) {
    console.error(`Credentials CSV not found: ${CREDS_CSV}`);
    process.exit(1);
  }
  let accounts = parseCredsCSV(fs.readFileSync(CREDS_CSV, 'utf8'));
  if (SKIP_ACCOUNTS > 0) accounts = accounts.slice(SKIP_ACCOUNTS);
  if (MAX_ACCOUNTS > 0) accounts = accounts.slice(0, MAX_ACCOUNTS);
  if (TEST_MODE && accounts.length > 1) accounts = accounts.slice(0, 1);

  const maxReveals = TEST_MODE ? 10 : (MAX_REVEALS || accounts.length * CREDITS_PER_ACCOUNT);

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       MORTAR — Apollo Browser Email Reveal               ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Accounts:     ${accounts.length} (${accounts.length * CREDITS_PER_ACCOUNT} potential credits)`);
  console.log(`  Max reveals:  ${maxReveals}`);
  console.log(`  Mode:         ${SEARCH_MODE ? 'Search + Reveal' : 'Reveal from CSV'}`);
  console.log(`  Test mode:    ${TEST_MODE ? 'YES' : 'NO'}`);
  console.log('');

  // ─── Load leads ────────────────────────────────────────────────

  let leads = [];
  const seenIds = new Set();

  if (SEARCH_MODE) {
    // We'll search with the first account's session, then reveal
    console.log('  Will search for leads after first login...');
  } else if (INPUT_CSV) {
    if (!fs.existsSync(INPUT_CSV)) {
      console.error(`Input CSV not found: ${INPUT_CSV}`);
      process.exit(1);
    }
    leads = parseLeadsCSV(fs.readFileSync(INPUT_CSV, 'utf8'));
    console.log(`  Loaded ${leads.length} leads from ${path.basename(INPUT_CSV)}`);
    const withEmail = leads.filter(l => l.email).length;
    const needEmail = leads.filter(l => !l.email && l.apollo_id).length;
    console.log(`  Already have email: ${withEmail}`);
    console.log(`  Need email (have apollo_id): ${needEmail}`);
    if (needEmail === 0) {
      // If no apollo_ids, we need to re-search to get them
      const noId = leads.filter(l => !l.email && !l.apollo_id).length;
      if (noId > 0) {
        console.log(`  ${noId} leads without apollo_id — will re-search to match`);
      }
    }
    console.log('');
  } else {
    console.error('  Must specify --input <csv> or --search mode');
    process.exit(1);
  }

  // ─── Process accounts ──────────────────────────────────────────

  let totalRevealed = 0;
  let totalErrors = 0;
  let accountsUsed = 0;

  for (let accIdx = 0; accIdx < accounts.length; accIdx++) {
    if (totalRevealed >= maxReveals) break;

    const account = accounts[accIdx];
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`  Account ${accIdx + 1}/${accounts.length}: ${account.email}`);
    console.log('═══════════════════════════════════════════════════════════');

    let browser;
    try {
      browser = await puppeteer.launch({
        headless: false,
        executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check',
               '--disable-extensions', '--window-size=1280,900'],
        defaultViewport: null,
        protocolTimeout: 60000,
      });

      // Login
      const loginResult = await loginToApollo(browser, account);
      if (loginResult.error) {
        console.log(`    Login failed: ${loginResult.error}`);
        try { await browser.close(); } catch {}
        // If API not working, the account is still valid but can't search/reveal
        if (loginResult.error === 'api_not_working') {
          console.log('    Skipping this account — API not accessible');
        }
        continue;
      }

      const page = loginResult.page;
      accountsUsed++;
      console.log(`    Logged in! URL: ${page.url().split('?')[0]}`);

      // If search mode, do the search with this first account
      if (SEARCH_MODE && leads.length === 0) {
        console.log('');
        console.log('  ── Searching for leads ──');
        const stateLocation = `${STATE}, US`;
        const searchResult = await browserSearch(page, {
          person_titles: TITLES,
          person_locations: [stateLocation],
          person_seniorities: SENIORITIES,
          ...(KEYWORDS ? { q_keywords: KEYWORDS } : {}),
        }, seenIds);

        for (const p of searchResult.people) {
          leads.push(p);
          seenIds.add(p.apollo_id);
        }
        console.log(`  Found ${leads.length} leads (${searchResult.totalAvailable} available)`);

        // Filter to leads needing emails
        const withEmail = leads.filter(l => l.email).length;
        console.log(`  Already have email: ${withEmail}`);
        console.log(`  Need reveal: ${leads.filter(l => !l.email).length}`);
        console.log('');
      }

      // Navigate to people page (ensure we're in the app context for XHR)
      if (!page.url().includes('app.apollo.io')) {
        await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 30000 });
        await sleep(3000);
      }

      // ─── Reveal emails ─────────────────────────────────────────

      const needReveal = leads.filter(l => !l.email && (l.apollo_id || l.first_name));
      if (needReveal.length === 0) {
        console.log('    No leads need email reveal');
        try { await browser.close(); } catch {}
        break;
      }

      console.log(`    Starting reveals (${needReveal.length} leads need emails)...`);
      let accountReveals = 0;
      let accountErrors = 0;
      let creditsExhausted = false;
      const revealStart = Date.now();

      for (let i = 0; i < needReveal.length; i++) {
        if (totalRevealed >= maxReveals) break;
        if (creditsExhausted) break;

        const lead = needReveal[i];

        // If lead doesn't have apollo_id, we need to search for it first
        if (!lead.apollo_id && lead.first_name && lead.last_name) {
          // Quick search to find this person's apollo_id
          const searchResult = await browserSearch(page, {
            q_keywords: `${lead.first_name} ${lead.last_name}`,
            per_page: 5,
          }, new Set());

          if (searchResult.people.length > 0) {
            // Find best match by name
            const match = searchResult.people.find(p =>
              p.first_name.toLowerCase() === lead.first_name.toLowerCase() &&
              p.last_name.toLowerCase() === lead.last_name.toLowerCase()
            ) || searchResult.people[0];
            lead.apollo_id = match.apollo_id;
          }

          if (!lead.apollo_id) {
            accountErrors++;
            continue;
          }
          await sleep(500);
        }

        // Reveal
        const result = await revealEmail(page, lead.apollo_id);

        if (result.credits_exhausted) {
          console.log(`    Credits exhausted at reveal #${accountReveals + 1}`);
          creditsExhausted = true;
          break;
        }

        if (result.error) {
          accountErrors++;
          if (accountErrors > 5 && accountReveals === 0) {
            console.log(`    Too many errors without success — moving to next account`);
            break;
          }
          continue;
        }

        if (result.email) {
          lead.email = result.email;
          lead.email_status = result.email_status || 'verified';
          lead.email_source = 'apollo_browser_reveal';
          lead.email_confidence = result.email_status === 'verified' ? 'verified' : 'likely';
          accountReveals++;
          totalRevealed++;

          if (result.phone && !lead.phone) {
            lead.phone = result.phone;
          }
        } else {
          // Reveal succeeded but no email (person genuinely has no email)
          accountErrors++;
        }

        // Progress logging
        if ((accountReveals + accountErrors) % 10 === 0 || creditsExhausted) {
          const elapsed = ((Date.now() - revealStart) / 1000).toFixed(1);
          const rate = elapsed > 0 ? Math.round(accountReveals / (elapsed / 60)) : 0;
          console.log(`    [${accountReveals}/${accountReveals + accountErrors}] ${accountReveals} emails | ${rate}/min | ${elapsed}s`);
        }

        // Rate limit: 0.5s between reveals
        await sleep(500);
      }

      const revealElapsed = ((Date.now() - revealStart) / 1000).toFixed(1);
      console.log(`    Account ${accIdx + 1} done: ${accountReveals} emails revealed in ${revealElapsed}s (${accountErrors} errors)`);
      totalErrors += accountErrors;

      try { await browser.close(); } catch {}

      // Brief pause between accounts
      if (accIdx < accounts.length - 1 && totalRevealed < maxReveals) {
        console.log('    Switching to next account in 3s...');
        await sleep(3000);
      }

    } catch (e) {
      console.log(`    Fatal error: ${e.message.slice(0, 120)}`);
      if (browser) try { await browser.close(); } catch {}
    }
  }

  // ─── Export enriched CSV ─────────────────────────────────────────

  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  Exporting Results');
  console.log('═══════════════════════════════════════════════════════════');

  // Sort: email first
  const seniorityOrder = { owner: 0, founder: 1, c_suite: 2, partner: 3, vp: 4, director: 5, manager: 6, senior: 7, entry: 8 };
  leads.sort((a, b) => {
    const ae = a.email ? 0 : 1, be = b.email ? 0 : 1;
    if (ae !== be) return ae - be;
    return (seniorityOrder[a.seniority] ?? 9) - (seniorityOrder[b.seniority] ?? 9);
  });

  const COLUMNS = [
    'first_name', 'last_name', 'firm_name', 'title', 'seniority',
    'city', 'state', 'country', 'phone', 'email', 'email_confidence',
    'email_source', 'website', 'domain', 'linkedin_url', 'headline', 'source',
  ];

  let outputPath = OUTPUT_CSV;
  if (!outputPath) {
    if (INPUT_CSV) {
      const ext = path.extname(INPUT_CSV);
      outputPath = INPUT_CSV.replace(ext, `-revealed${ext}`);
    } else {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
      outputPath = path.join(__dirname, '..', 'output', `apollo-revealed_${timestamp}.csv`);
    }
  }

  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const csvHeader = COLUMNS.join(',');
  const csvRows = leads.map(lead => {
    return COLUMNS.map(col => {
      let val = (lead[col] || '').toString();
      if (col === 'phone' && val) val = normalizePhone(val);
      if (col === 'source' && !val) val = 'apollo';
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',');
  });
  fs.writeFileSync(outputPath, [csvHeader, ...csvRows].join('\n'));

  // ─── Summary ─────────────────────────────────────────────────────

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const emailCount = leads.filter(l => l.email).length;
  const phoneCount = leads.filter(l => l.phone).length;

  const bySource = {};
  for (const l of leads) {
    if (l.email_source) bySource[l.email_source] = (bySource[l.email_source] || 0) + 1;
  }

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       APOLLO BROWSER REVEAL COMPLETE                     ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:         ${String(leads.length).padStart(6)}                             ║`);
  console.log(`║  With email:          ${String(emailCount).padStart(6)} (${Math.round(emailCount/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log(`║  With phone:          ${String(phoneCount).padStart(6)} (${Math.round(phoneCount/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Browser reveals:     ${String(totalRevealed).padStart(6)}                             ║`);
  console.log(`║  Accounts used:       ${String(accountsUsed).padStart(6)}                             ║`);
  console.log(`║  Errors:              ${String(totalErrors).padStart(6)}                             ║`);
  if (Object.keys(bySource).length > 0) {
    console.log('╠═══════════════════════════════════════════════════════════╣');
    console.log('║  Email Sources:                                           ║');
    for (const [src, count] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
      console.log(`║    ${src.padEnd(28)} ${String(count).padStart(5)}                   ║`);
    }
  }
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total time:          ${totalElapsed}s                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  process.exit(1);
});
