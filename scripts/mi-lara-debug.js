#!/usr/bin/env node
/**
 * Debug: understand why table extraction fails on non-first search
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';
async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function doSearch(page, letter) {
  await page.goto(BASE_URL, { waitUntil: 'networkidle0', timeout: 60000 });
  await delay(2000);

  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(500);
  await page.type('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]', letter);
  await delay(300);
  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');

  // Wait more aggressively
  await delay(10000);

  // Save HTML
  const html = await page.content();
  require('fs').writeFileSync(`/tmp/mi-lara-search-${letter}.html`, html);
  console.log(`Saved HTML for letter ${letter} (${html.length} bytes)`);

  // Check what tables exist
  const tableInfo = await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    const info = [];
    for (let i = 0; i < tables.length; i++) {
      const t = tables[i];
      const rows = t.querySelectorAll('tr');
      const firstRowText = rows[0] ? rows[0].textContent.substring(0, 200).trim() : '';
      info.push({
        index: i,
        id: t.id || 'none',
        rows: rows.length,
        firstRow: firstRowText
      });
    }
    return info;
  });

  console.log(`Tables found: ${tableInfo.length}`);
  for (const t of tableInfo) {
    if (t.rows > 2) {
      console.log(`  Table[${t.index}] id=${t.id} rows=${t.rows} first="${t.firstRow.substring(0, 100)}"`);
    }
  }

  // Check body text for result indicators
  const bodyText = await page.evaluate(() => document.body.innerText);
  const resultMatch = bodyText.match(/(\d+\+?\s+results?\s+found|Showing\s+\d+-\d+\s+of\s+\S+|no results)/i);
  console.log(`Result indicator: ${resultMatch ? resultMatch[0] : 'NOT FOUND'}`);

  // Try alternative approach: look for any table with license numbers
  const licenseData = await page.evaluate(() => {
    const allText = document.body.innerHTML;
    const licenseNums = allText.match(/290\d{7}/g);
    return {
      licenseNumberCount: licenseNums ? new Set(licenseNums).size : 0,
      sampleNums: licenseNums ? [...new Set(licenseNums)].slice(0, 5) : [],
    };
  });
  console.log(`License numbers in page: ${licenseData.licenseNumberCount}`);
  console.log(`Sample: ${licenseData.sampleNums.join(', ')}`);

  // Screenshot
  await page.screenshot({ path: `/tmp/mi-lara-debug-${letter}.png`, fullPage: false });
}

async function main() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--window-size=1400,900'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  // First search - A
  console.log('\n=== Search A ===');
  await doSearch(page, 'A');

  // Close and reopen page for B
  console.log('\n=== Search B (new page) ===');
  await page.close();
  const page2 = await browser.newPage();
  await page2.setViewport({ width: 1400, height: 900 });
  await doSearch(page2, 'B');

  // Try C on same page after fresh navigation
  console.log('\n=== Search C (same page, fresh nav) ===');
  await doSearch(page2, 'C');

  await delay(3000);
  await browser.close();
  console.log('\nDone.');
}

main().catch(console.error);
