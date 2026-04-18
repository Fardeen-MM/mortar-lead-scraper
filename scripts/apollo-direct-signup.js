#!/usr/bin/env node
/**
 * Apollo Direct Signup — email + password (NOT Microsoft OAuth)
 * Tests if direct signup gets the same API tier as the original account.
 *
 * Usage:
 *   node scripts/apollo-direct-signup.js --skip 10 --max 1
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

const CSV_PATH = '/Users/fardeenchoudhury/Downloads/smartlead_domains (6).csv';
const ENV_PATH = path.join(__dirname, '..', '.env');

const args = process.argv.slice(2);
const maxAccounts = parseInt(args[args.indexOf('--max') + 1]) || 1;
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

async function directSignup(account) {
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
    console.log('  Loading Apollo signup...');
    await page.goto('https://www.apollo.io/sign-up', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(3000);
    console.log('  URL: ' + page.url());

    // ── Step 2: Fill email + password directly (NOT Microsoft OAuth) ──
    console.log('  Filling signup form with email + password...');

    // First name + Last name or Full name
    const [firstName, ...lastParts] = account.name.split(' ');
    const lastName = lastParts.join(' ');

    // Look for signup form fields
    const filled = await page.evaluate((email, password, first, last) => {
      const results = [];
      const inputs = [...document.querySelectorAll('input:not([type="hidden"])')];

      for (const input of inputs) {
        const name = (input.name || '').toLowerCase();
        const placeholder = (input.placeholder || '').toLowerCase();
        const type = (input.type || '').toLowerCase();
        const label = input.closest('label')?.textContent?.toLowerCase() || '';
        const parentText = input.parentElement?.textContent?.toLowerCase() || '';
        const ctx = name + ' ' + placeholder + ' ' + label + ' ' + parentText;

        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;

        if (ctx.includes('first name') || name === 'first_name' || name === 'firstName') {
          setter.call(input, first);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          results.push('first:' + first);
        } else if (ctx.includes('last name') || name === 'last_name' || name === 'lastName') {
          setter.call(input, last);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          results.push('last:' + last);
        } else if ((ctx.includes('email') || type === 'email') && !ctx.includes('password')) {
          setter.call(input, email);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          results.push('email:' + email);
        } else if (ctx.includes('password') || type === 'password') {
          setter.call(input, password);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          results.push('password:***');
        }
      }
      return results;
    }, account.email, account.password, firstName, lastName);

    console.log('  Filled: ' + filled.join(', '));

    // Also try with Puppeteer .type() as fallback
    if (filled.length === 0) {
      console.log('  React fill failed, trying Puppeteer type()...');
      const allInputs = await page.$$('input:not([type="hidden"])');
      for (const input of allInputs) {
        const info = await page.evaluate(el => ({
          type: el.type, name: el.name, placeholder: el.placeholder,
        }), input);
        console.log('    Input:', JSON.stringify(info));
      }
    }

    // Take screenshot before clicking signup
    const preSignup = `/tmp/apollo-direct-presignup-${Date.now()}.png`;
    await page.screenshot({ path: preSignup });
    console.log('  Pre-signup screenshot: ' + preSignup);

    // Click "Sign Up" / "Create Account" button
    const signupBtn = await page.evaluateHandle(() => {
      const btns = [...document.querySelectorAll('button, input[type="submit"]')];
      return btns.find(b => {
        const t = (b.textContent || b.value || '').toLowerCase().trim();
        return t.includes('sign up') || t.includes('create account') || t.includes('get started') || t === 'submit';
      });
    });
    if (signupBtn && (await signupBtn.asElement())) {
      const btnText = await page.evaluate(el => (el.textContent || el.value || '').trim(), signupBtn);
      console.log('  Clicking "' + btnText + '"...');
      await signupBtn.click();
      await sleep(5000);
    } else {
      console.log('  No signup button found');
    }

    console.log('  Post-signup URL: ' + page.url());
    const postSignup = `/tmp/apollo-direct-postsignup-${Date.now()}.png`;
    await page.screenshot({ path: postSignup, fullPage: true });
    console.log('  Post-signup screenshot: ' + postSignup);

    // Check if we need email verification
    const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 500) || '');
    if (bodyText.includes('verify') || bodyText.includes('verification') || bodyText.includes('check your email')) {
      console.log('  ⚠ Email verification required — need to check inbox');
      await browser.close();
      return { error: 'email_verification_required' };
    }

    // If we made it to Apollo, try onboarding + key grab
    // (same flow as the Microsoft OAuth script from here)
    console.log('  Checking page state...');
    await sleep(5000);
    const finalUrl = page.url();
    const finalSs = `/tmp/apollo-direct-final-${Date.now()}.png`;
    await page.screenshot({ path: finalSs, fullPage: true });
    console.log('  Final URL: ' + finalUrl);
    console.log('  Final screenshot: ' + finalSs);

    await browser.close();
    return { error: 'manual_check_needed', url: finalUrl };

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
  console.log('  Apollo Direct Signup Test (email + password)');
  console.log(`  Testing with: ${accounts[0]?.email}`);
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  for (const acc of accounts) {
    console.log(`  Account: ${acc.email}`);
    const result = await directSignup(acc);
    console.log('  Result:', JSON.stringify(result));
  }
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
