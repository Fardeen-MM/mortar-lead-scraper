#!/usr/bin/env node
/**
 * NICEIC Session Acquirer — opens a browser for manual CAPTCHA solve
 *
 * This script opens Chrome, navigates to electricalcompetentperson.co.uk,
 * and waits for you to complete the search (solve CAPTCHA if needed).
 * It intercepts the API response to extract the searchSessionId.
 *
 * The session ID is printed to stdout and can be used with the main scraper:
 *   NICEIC_SESSION_ID=$(node scripts/niceic-get-session.js) node scripts/niceic-scrape.js
 *
 * Or copy-paste it:
 *   NICEIC_SESSION_ID=abc123... node scripts/niceic-scrape.js
 */

const puppeteer = require('puppeteer');

(async () => {
  console.error('Opening browser for NICEIC session acquisition...');
  console.error('1. The browser will open to the electrician search page');
  console.error('2. If prompted, solve the CAPTCHA');
  console.error('3. The session ID will be captured automatically');
  console.error('');

  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--window-size=1280,900'],
    defaultViewport: { width: 1280, height: 900 },
  });

  const page = await browser.newPage();

  // Listen for the API response
  let resolved = false;
  page.on('response', async (res) => {
    if (resolved) return;
    if (res.url().includes('/api/directory/search') && res.status() === 200) {
      try {
        const body = await res.text();
        const data = JSON.parse(body);
        if (data.searchSessionId) {
          resolved = true;
          console.error(`\nSession acquired! Results: ${data.totalCount || 0}`);
          console.error(`Session ID: ${data.searchSessionId.substring(0, 30)}...`);
          // Print ONLY the session ID to stdout (for command substitution)
          console.log(data.searchSessionId);
          await browser.close();
          process.exit(0);
        }
      } catch (e) { /* ignore */ }
    }
  });

  await page.goto('https://www.electricalcompetentperson.co.uk/search?postcode=SW1A+1AA&electricianType=Installer', {
    waitUntil: 'networkidle2',
    timeout: 30000,
  }).catch(() => {});

  console.error('Waiting for search results (solve CAPTCHA if needed)...');
  console.error('The browser will close automatically once a session is acquired.');

  // Wait up to 5 minutes for manual CAPTCHA solve
  setTimeout(async () => {
    if (!resolved) {
      console.error('\nTimeout — no session acquired in 5 minutes');
      await browser.close();
      process.exit(1);
    }
  }, 300000);
})();
