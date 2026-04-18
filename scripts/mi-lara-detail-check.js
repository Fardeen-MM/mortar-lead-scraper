#!/usr/bin/env node
/**
 * Michigan LARA - Check detail page for email fields
 * Quick test: click on a license number and see what details are available
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1400,1000'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 1000 });

  console.log('Loading search page...');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Select Dentist and search for last name "Smith" (common name, guaranteed results)
  console.log('Searching for Dentist with last name "Smith"...');
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);

  const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await lastNameInput.type('Smith');
  await delay(300);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Take screenshot of results
  await page.screenshot({ path: '/tmp/mi-lara-results.png', fullPage: false });
  console.log('Results screenshot saved');

  // Now extract the actual table - find ALL tables and identify the results table
  const tableInfo = await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    const results = [];
    for (let i = 0; i < tables.length; i++) {
      const t = tables[i];
      const rows = t.querySelectorAll('tr');
      if (rows.length > 3) {
        const headerCells = Array.from(rows[0].querySelectorAll('th, td'))
          .map(c => c.textContent.trim())
          .filter(Boolean);
        const dataCells = rows.length > 1 ?
          Array.from(rows[1].querySelectorAll('td'))
            .map(c => c.textContent.trim()) : [];

        if (headerCells.length > 3) {
          results.push({
            index: i,
            id: t.id || 'no-id',
            className: t.className || 'no-class',
            rowCount: rows.length,
            headers: headerCells,
            firstRow: dataCells,
          });
        }
      }
    }
    return results;
  });

  console.log('\nTables found with data:');
  for (const t of tableInfo) {
    console.log(`\n  Table #${t.index} (id: ${t.id}, rows: ${t.rowCount})`);
    console.log(`  Headers: ${t.headers.join(' | ')}`);
    if (t.firstRow.length > 0) {
      console.log(`  First row: ${t.firstRow.join(' | ')}`);
    }
  }

  // Now click on the first license number link to see the detail page
  console.log('\n\nClicking on first license number to check detail page...');

  // Find the first clickable license number
  const licenseLink = await page.evaluate(() => {
    const links = document.querySelectorAll('a');
    for (const link of links) {
      const text = link.textContent.trim();
      if (/^\d{10}$/.test(text)) { // License numbers appear to be 10 digits
        return { href: link.href, text };
      }
    }
    return null;
  });

  if (licenseLink) {
    console.log(`Clicking license: ${licenseLink.text}`);

    // Click the link
    await page.evaluate(() => {
      const links = document.querySelectorAll('a');
      for (const link of links) {
        if (/^\d{10}$/.test(link.textContent.trim())) {
          link.click();
          return;
        }
      }
    });

    await delay(8000);

    // Take screenshot of detail page
    await page.screenshot({ path: '/tmp/mi-lara-detail.png', fullPage: true });
    console.log('Detail page screenshot saved');

    // Extract all text from the detail page to look for email
    const detailContent = await page.evaluate(() => {
      return document.body.innerText;
    });

    // Check for email
    const hasEmail = detailContent.toLowerCase().includes('email');
    const hasPhone = detailContent.toLowerCase().includes('phone');
    const hasAddress = detailContent.toLowerCase().includes('address');

    console.log(`\nDetail page contains:`);
    console.log(`  "email": ${hasEmail}`);
    console.log(`  "phone": ${hasPhone}`);
    console.log(`  "address": ${hasAddress}`);

    // Extract all label/value pairs
    const pairs = await page.evaluate(() => {
      const result = [];
      // Look for label-value patterns
      const allElements = document.querySelectorAll('span, label, div');
      for (const el of allElements) {
        const text = el.textContent.trim();
        if (text.length > 0 && text.length < 200 && text.includes(':')) {
          // Skip if it's a parent of many children
          if (el.children.length < 3) {
            result.push(text);
          }
        }
      }
      return result.slice(0, 100);
    });

    console.log('\nDetail page label/value pairs:');
    for (const p of pairs) {
      if (!p.includes('\n') || p.length < 100) {
        console.log(`  ${p}`);
      }
    }

    // Print a snippet of the page content
    console.log('\n\nDetail page text (first 3000 chars):');
    console.log(detailContent.substring(0, 3000));

  } else {
    console.log('No license number link found to click');
  }

  // Wait for user to inspect
  console.log('\n\nKeeping browser open for 10 seconds for inspection...');
  await delay(10000);

  await browser.close();
  console.log('Done.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
