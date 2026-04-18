#!/usr/bin/env node
/**
 * Apollo Speed Test — Measure throughput of people search.
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

  console.log('Loading Apollo...');
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'domcontentloaded', timeout: 90000 });

  // Wait for page to stabilize (SPA loads asynchronously)
  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 1000));
    const url = page.url();
    if (!url.includes('/login') && !url.includes('/signup')) break;
  }

  const url = page.url();
  if (url.includes('/login')) {
    console.log('Not logged in! Run: node scripts/apollo-login.js');
    await browser.close();
    process.exit(1);
  }
  console.log('Logged in!\n');

  // Speed test: fetch 10 pages as fast as possible
  const start = Date.now();
  let totalPeople = 0;
  const allPeople = [];
  const PAGES = 10;
  const PER_PAGE = 100; // Request 100, Apollo may return 25-100

  for (let pg = 1; pg <= PAGES; pg++) {
    const pgStart = Date.now();
    const result = await page.evaluate((pageNum, perPage) => {
      return new Promise(resolve => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/v1/mixed_people/search');
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.withCredentials = true;
        xhr.timeout = 30000;
        xhr.onload = () => {
          try {
            const data = JSON.parse(xhr.responseText);
            resolve({
              numPeople: (data.people || []).length,
              totalEntries: data.pagination?.total_entries || 0,
              totalPages: data.pagination?.total_pages || 0,
              people: (data.people || []).map(p => ({
                name: p.first_name + ' ' + p.last_name,
                email: p.email,
                email_status: p.email_status,
                domain: (p.organization || {}).primary_domain,
                city: p.city,
                state: p.state,
                linkedin: p.linkedin_url ? 'Y' : 'N',
                seniority: p.seniority,
              })),
            });
          } catch (e) { resolve({ error: e.message, raw: xhr.responseText.slice(0, 200) }); }
        };
        xhr.onerror = () => resolve({ error: 'XHR error' });
        xhr.ontimeout = () => resolve({ error: 'timeout' });
        xhr.send(JSON.stringify({
          person_titles: ['attorney'],
          person_locations: ['Florida, US'],
          person_seniorities: ['owner'],
          page: pageNum,
          per_page: perPage,
        }));
      });
    }, pg, PER_PAGE);

    if (result.error) {
      console.log(`Page ${pg}: ERROR - ${result.error}`);
      if (result.raw) console.log('  Raw:', result.raw.slice(0, 100));
      break;
    }

    const elapsed = Date.now() - pgStart;
    totalPeople += result.numPeople;
    allPeople.push(...(result.people || []));
    const runRate = Math.round(totalPeople / ((Date.now() - start) / 60000));
    console.log(`Page ${pg}/${PAGES}: ${result.numPeople} people in ${elapsed}ms | Running: ${totalPeople} total @ ${runRate}/min | Available: ${result.totalEntries}`);

    // Very short delay — just enough to not trip rate limits
    await new Promise(r => setTimeout(r, 300));
  }

  const totalElapsed = (Date.now() - start) / 1000;
  const rate = Math.round(totalPeople / (totalElapsed / 60));

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`  Pages fetched:    ${PAGES}`);
  console.log(`  People fetched:   ${totalPeople}`);
  console.log(`  Time:             ${totalElapsed.toFixed(1)}s`);
  console.log(`  Rate:             ${rate} leads/minute`);

  // Coverage analysis
  const withDomain = allPeople.filter(p => p.domain).length;
  const withLinkedin = allPeople.filter(p => p.linkedin === 'Y').length;
  const verified = allPeople.filter(p => p.email_status === 'verified').length;
  const guessed = allPeople.filter(p => p.email_status === 'guessed').length;

  console.log('\n  Coverage:');
  console.log(`  With domain:      ${withDomain}/${totalPeople} (${Math.round(withDomain/Math.max(totalPeople,1)*100)}%)`);
  console.log(`  With LinkedIn:    ${withLinkedin}/${totalPeople} (${Math.round(withLinkedin/Math.max(totalPeople,1)*100)}%)`);
  console.log(`  Verified email:   ${verified}/${totalPeople} (emails locked on free tier)`);
  console.log(`  Guessed email:    ${guessed}/${totalPeople}`);
  console.log('═══════════════════════════════════════════════════════════');

  await browser.close();
})().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
