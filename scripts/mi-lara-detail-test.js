#!/usr/bin/env node
/**
 * Simple test: open a known active dentist detail page and check ALL fields
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1400,1200'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 1200 });

  // We know license 2901012877 (Leland Zuidema) is active
  // Accela detail pages use a specific URL pattern
  // Let's search and click through to it
  const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';

  console.log('Loading search page...');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Search by license number directly
  console.log('Searching by license number 2901012877...');
  const licInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLicenseNumber"]');
  if (licInput) {
    await licInput.type('2901012877');
  }
  await delay(300);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  await page.screenshot({ path: '/tmp/mi-lara-lic-search.png', fullPage: false });

  // Click on the license number link
  const linkClicked = await page.evaluate(() => {
    const links = document.querySelectorAll('a');
    for (const link of links) {
      const text = link.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
      if (text === '2901012877') {
        link.click();
        return true;
      }
    }
    return false;
  });

  if (linkClicked) {
    console.log('Clicked license link, waiting for detail page...');
    await delay(8000);

    await page.screenshot({ path: '/tmp/mi-lara-active-detail-1.png', fullPage: true });

    // Get ALL page text
    const fullText = await page.evaluate(() => document.body.innerText);
    console.log('\n=== FULL DETAIL PAGE TEXT ===');
    console.log(fullText);
    console.log('=== END ===\n');

    // Check for email
    const emails = fullText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    console.log('Emails in text:', emails || 'NONE');

    // Check HTML source
    const html = await page.content();
    const htmlEmails = html.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    const unique = [...new Set(htmlEmails || [])].filter(e =>
      !e.includes('accela') && !e.includes('michigan.gov') && !e.includes('datadoghq') && !e.includes('walkme'));
    console.log('Emails in HTML:', unique.length > 0 ? unique.join(', ') : 'NONE');

    // Check for any address info
    console.log('\nLooking for address/contact fields...');
    const fields = fullText.split('\n').filter(l => l.trim().length > 0 && l.trim().length < 200);
    for (const f of fields) {
      console.log(`  ${f.trim()}`);
    }
  } else {
    console.log('Could not find license link to click');

    // Check what's on the page
    const pageText = await page.evaluate(() => document.body.innerText.substring(0, 3000));
    console.log('Page text:', pageText);
  }

  // Now check a second active dentist for comparison
  console.log('\n\n=== Checking second dentist (Cari Zupko, 2901015847) ===');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  const licInput2 = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLicenseNumber"]');
  if (licInput2) {
    await licInput2.type('2901015847');
  }
  await delay(300);
  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  await page.evaluate(() => {
    const links = document.querySelectorAll('a');
    for (const link of links) {
      if (link.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '') === '2901015847') {
        link.click();
        return;
      }
    }
  });
  await delay(8000);

  await page.screenshot({ path: '/tmp/mi-lara-active-detail-2.png', fullPage: true });

  const fullText2 = await page.evaluate(() => document.body.innerText);
  console.log('\n=== DETAIL PAGE 2 ===');
  console.log(fullText2);

  const emails2 = fullText2.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
  console.log('\nEmails:', emails2 || 'NONE');

  await delay(3000);
  await browser.close();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
