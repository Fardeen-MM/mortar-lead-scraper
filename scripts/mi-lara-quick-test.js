#!/usr/bin/env node
/**
 * Michigan LARA - Quick test: scrape "Dentist" last name "S" and check detail page
 * for any email on ACTIVE licenses
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
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

  // Search Dentist, last name "S"
  console.log('Searching for active Dentist with last name "S"...');
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);

  const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await lastNameInput.type('S');
  await delay(200);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Get the count message
  const countMsg = await page.evaluate(() => {
    const body = document.body.innerText;
    const match = body.match(/(\d+\+?\s+results?\s+found|Showing\s+\d+-\d+\s+of\s+\d+)/i);
    return match ? match[0] : 'count not found';
  });
  console.log(`Results: ${countMsg}`);

  // Extract all rows from the first page, looking for ACTIVE licenses
  const data = await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    let resultTable = null;
    for (const t of tables) {
      const text = t.textContent;
      if (text.includes('License Type') && text.includes('License Number') && text.includes('Last Name')) {
        resultTable = t;
        break;
      }
    }
    if (!resultTable) return { rows: [], headers: [] };

    // Headers
    const headerRow = resultTable.querySelector('tr');
    const headers = [];
    headerRow.querySelectorAll('th, td').forEach(cell => {
      const a = cell.querySelector('a');
      headers.push(a ? a.textContent.trim() : cell.textContent.trim().split('\n')[0].trim());
    });

    // Data rows
    const rows = [];
    const allRows = resultTable.querySelectorAll('tr');
    for (let i = 1; i < allRows.length; i++) {
      const row = allRows[i];
      if (row.textContent.includes('Prev') && row.textContent.includes('Next')) continue;
      const cells = [];
      const links = [];
      row.querySelectorAll('td').forEach(cell => {
        const link = cell.querySelector('a');
        cells.push(cell.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
        links.push(link ? link.href : '');
      });
      if (cells.length > 3) {
        rows.push({ cells, links });
      }
    }

    return { headers, rows };
  });

  console.log(`\nHeaders: ${data.headers.join(' | ')}`);
  console.log(`Rows on first page: ${data.rows.length}`);

  // Find an ACTIVE dentist to check detail
  let activeDentist = null;
  for (const row of data.rows) {
    const status = row.cells[7] || '';
    if (status.toLowerCase().includes('active') || status.toLowerCase().includes('renewed')) {
      activeDentist = row;
      break;
    }
  }

  // Print some sample rows
  console.log('\nSample rows:');
  for (let i = 0; i < Math.min(5, data.rows.length); i++) {
    const r = data.rows[i];
    console.log(`  ${r.cells.join(' | ')}`);
  }

  if (activeDentist) {
    console.log(`\nFound active dentist: ${activeDentist.cells.join(' | ')}`);
    const detailLink = activeDentist.links.find(l => l && l.includes('accela'));

    if (detailLink) {
      console.log(`Detail link: ${detailLink}`);

      // Open detail page
      await page.goto(detailLink, { waitUntil: 'networkidle2', timeout: 30000 });
      await delay(5000);

      await page.screenshot({ path: '/tmp/mi-lara-active-detail.png', fullPage: true });

      // Extract ALL text from detail page
      const detailText = await page.evaluate(() => document.body.innerText);
      console.log('\n--- DETAIL PAGE TEXT ---');
      console.log(detailText.substring(0, 5000));
      console.log('--- END ---');

      // Specifically check for email patterns
      const emailMatch = detailText.match(/[\w.+-]+@[\w.-]+\.\w+/g);
      if (emailMatch) {
        console.log(`\n*** EMAILS FOUND: ${emailMatch.join(', ')} ***`);
      } else {
        console.log('\nNo email addresses found on detail page.');
      }

      // Check for any address/contact info
      const hasCity = detailText.toLowerCase().includes('city');
      const hasZip = detailText.toLowerCase().includes('zip');
      const hasCounty = detailText.toLowerCase().includes('county');
      console.log(`Has city: ${hasCity}, zip: ${hasZip}, county: ${hasCounty}`);

    } else {
      // Click through on the page instead
      console.log('No direct link found, clicking on license number...');
      const licNum = activeDentist.cells[1];
      await page.evaluate((num) => {
        const links = document.querySelectorAll('a');
        for (const link of links) {
          if (link.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, '') === num) {
            link.click();
            return;
          }
        }
      }, licNum);

      await delay(8000);
      await page.screenshot({ path: '/tmp/mi-lara-active-detail.png', fullPage: true });

      const detailText = await page.evaluate(() => document.body.innerText);
      console.log('\n--- DETAIL PAGE TEXT ---');
      console.log(detailText.substring(0, 5000));
      console.log('--- END ---');

      const emailMatch = detailText.match(/[\w.+-]+@[\w.-]+\.\w+/g);
      if (emailMatch) {
        console.log(`\n*** EMAILS FOUND: ${emailMatch.join(', ')} ***`);
      } else {
        console.log('\nNo email addresses found on detail page.');
      }
    }
  } else {
    console.log('\nNo active dentists found on first page of results.');
    // Let's try going to page 5 or so where there might be active ones
    console.log('Navigating forward to find active licenses...');
  }

  // Also try: check if there's a CSV export button after search
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Check for any hidden export buttons or links
  const exportInfo = await page.evaluate(() => {
    const allElements = document.querySelectorAll('a, button, input[type="submit"], input[type="button"]');
    const exportRelated = [];
    for (const el of allElements) {
      const text = (el.textContent || el.value || '').trim().toLowerCase();
      const id = el.id || '';
      const name = el.name || '';
      if (text.includes('export') || text.includes('download') || text.includes('csv') || text.includes('report') ||
          id.toLowerCase().includes('export') || id.toLowerCase().includes('report') || id.toLowerCase().includes('download') ||
          name.toLowerCase().includes('export') || name.toLowerCase().includes('report')) {
        exportRelated.push({
          tag: el.tagName,
          id: el.id,
          name: el.name,
          text: text.slice(0, 100),
          href: el.href || '',
          style: el.style.display || '',
        });
      }
    }
    return exportRelated;
  });

  console.log('\n\nExport/Report related elements:');
  for (const e of exportInfo) {
    console.log(`  ${e.tag} id="${e.id}" name="${e.name}" text="${e.text}" href="${e.href}" display="${e.style}"`);
  }

  await delay(3000);
  await browser.close();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
