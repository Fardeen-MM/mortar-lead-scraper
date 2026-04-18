#!/usr/bin/env node
/**
 * Quick check: find active dentist detail page for email, and try report button
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';
async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1400,1200'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 1200 });

  // Enable download interception
  const client = await page.createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: '/tmp/mi-lara-downloads',
  });

  require('fs').mkdirSync('/tmp/mi-lara-downloads', { recursive: true });

  console.log('Loading search page...');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Search for Dentist with last name "Sm" to find common names
  console.log('Searching for Dentist, last name "Sm"...');
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);
  const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await lastNameInput.type('Sm');
  await delay(200);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Navigate to later pages to find active dentists (early licenses are all lapsed/deceased)
  // Each page has 50 results, so going to page 8-10 should get us past the old ones
  console.log('Navigating to later pages to find active dentists...');
  for (let i = 0; i < 8; i++) {
    const nextBtn = await page.$('a[href*="Page$Next"]');
    if (nextBtn) {
      await nextBtn.click();
      await delay(3000);
    } else {
      break;
    }
  }

  // Check current page for active dentists
  const activeInfo = await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    for (const t of tables) {
      if (!t.textContent.includes('License Type')) continue;
      const rows = t.querySelectorAll('tr');
      const actives = [];
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 8) {
          const status = cells[7].textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
          const name = [cells[2].textContent.trim(), cells[4].textContent.trim()].join(' ').replace(/[\u200B-\u200D\uFEFF]/g, '');
          const licNum = cells[1].textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
          const link = cells[1].querySelector('a');
          if (status.toLowerCase() === 'active' || status.toLowerCase() === 'renewed') {
            actives.push({ name, licNum, status, href: link ? link.href : null });
          }
        }
      }
      return actives;
    }
    return [];
  });

  console.log(`Found ${activeInfo.length} active dentists on current page.`);
  for (const a of activeInfo.slice(0, 5)) {
    console.log(`  ${a.name} (${a.licNum}) - ${a.status}`);
  }

  // Click into the first active dentist's detail page
  if (activeInfo.length > 0 && activeInfo[0].href) {
    console.log(`\nOpening detail page for: ${activeInfo[0].name}...`);
    await page.goto(activeInfo[0].href, { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(5000);

    await page.screenshot({ path: '/tmp/mi-lara-active-detail.png', fullPage: true });

    // Get ALL page text
    const detailText = await page.evaluate(() => document.body.innerText);

    // Check for email
    const emailMatches = detailText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    console.log(`\nEmails found in text: ${emailMatches ? emailMatches.join(', ') : 'NONE'}`);

    // Get all visible field labels and values
    const fieldPairs = await page.evaluate(() => {
      const pairs = [];
      const labels = document.querySelectorAll('label, span[id*="label"], div.ACA_Label');
      for (const label of labels) {
        const text = label.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
        if (text.length > 1 && text.length < 100) {
          // Try to find the associated value
          const parent = label.closest('div, td');
          if (parent) {
            const inputs = parent.querySelectorAll('input, span:not([id*="label"])');
            for (const inp of inputs) {
              const val = (inp.value || inp.textContent || '').trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
              if (val) {
                pairs.push({ label: text, value: val });
              }
            }
          }
        }
      }
      return pairs;
    });

    console.log('\nAll label/value pairs on detail page:');
    for (const p of fieldPairs) {
      console.log(`  ${p.label}: ${p.value}`);
    }

    // Print full text
    console.log('\n--- FULL DETAIL PAGE TEXT ---');
    console.log(detailText);
    console.log('--- END ---\n');

    // Also get page HTML to look for hidden email fields
    const pageHTML = await page.content();
    const emailInHTML = pageHTML.match(/email[^>]*>([^<]*@[^<]*)</gi);
    if (emailInHTML) {
      console.log('Email in HTML source:', emailInHTML.join(' | '));
    }

    // Check for hidden fields with email
    const hiddenEmails = await page.evaluate(() => {
      const inputs = document.querySelectorAll('input[type="hidden"]');
      const results = [];
      for (const inp of inputs) {
        if (inp.value && inp.value.includes('@')) {
          results.push({ name: inp.name, value: inp.value });
        }
      }
      return results;
    });
    if (hiddenEmails.length > 0) {
      console.log('Hidden email fields:', JSON.stringify(hiddenEmails));
    }
  }

  // Now try the report button
  console.log('\n\n=== Trying Report Button ===');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Search first
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);
  const ln2 = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await ln2.type('Sm');
  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Now try clicking the hidden report button
  const reportBtn = await page.$('#ctl00_HeaderNavigation_btnPostForReport');
  if (reportBtn) {
    console.log('Found report button, clicking...');
    await page.evaluate(() => {
      const btn = document.getElementById('ctl00_HeaderNavigation_btnPostForReport');
      if (btn) {
        btn.style.display = 'block';
        btn.click();
      }
    });
    await delay(10000);

    // Check for any new windows or downloads
    const pages = await browser.pages();
    console.log(`Open pages: ${pages.length}`);
    for (let i = 0; i < pages.length; i++) {
      console.log(`  Page ${i}: ${pages[i].url()}`);
    }

    await page.screenshot({ path: '/tmp/mi-lara-report.png', fullPage: true });

    // Check for downloads
    const downloads = require('fs').readdirSync('/tmp/mi-lara-downloads');
    console.log(`Downloads: ${downloads.length > 0 ? downloads.join(', ') : 'none'}`);

    // Check page content for any report
    const reportText = await page.evaluate(() => document.body.innerText.substring(0, 2000));
    console.log('Page after report click:', reportText.substring(0, 500));
  }

  await delay(5000);
  await browser.close();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
