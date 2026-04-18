#!/usr/bin/env node
/**
 * Michigan LARA License Scraper v2
 *
 * Fixed: correct table ID selector (gdvRefLicenseeList)
 * Each letter search opens a fresh browser page.
 * Paginates through all result pages.
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';
const TABLE_ID = 'ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList';

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

const PROFESSION = process.argv[2] || 'Dentist';

async function searchLetter(browser, profession, letter) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });
  const rows = [];

  try {
    await page.goto(BASE_URL, { waitUntil: 'networkidle0', timeout: 60000 });
    await delay(2000);

    // Select license type
    const matched = await page.evaluate((prof) => {
      const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
      if (!sel) return null;
      for (const o of sel.options) if (o.value === prof) return o.value;
      const pl = prof.toLowerCase();
      for (const o of sel.options) {
        if (o.value.toLowerCase() === pl || o.value.toLowerCase().startsWith(pl)) return o.value;
      }
      return null;
    }, profession);

    if (!matched) { await page.close(); return rows; }

    await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', matched);
    await delay(500);
    await page.type('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]', letter);
    await delay(300);

    // Click search
    await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
    await delay(8000);

    // Wait for results table
    try {
      await page.waitForSelector(`#${TABLE_ID}`, { timeout: 15000 });
    } catch (e) {
      // No results or timeout
      await page.close();
      return rows;
    }

    // Paginate
    let pageNum = 1;
    while (true) {
      const data = await page.evaluate((tableId) => {
        const table = document.getElementById(tableId);
        if (!table) return { rows: [], hasNext: false };

        const trs = table.querySelectorAll('tr');
        const dataRows = [];

        for (let i = 1; i < trs.length; i++) {
          const tr = trs[i];
          // Skip pager row (contains nested table with page numbers)
          if (tr.querySelector('table') &&
              (tr.textContent.includes('Next') || tr.textContent.includes('Prev'))) continue;

          const cells = Array.from(tr.querySelectorAll('td'))
            .map(c => c.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));

          // Valid data row has 9+ cells with a license number
          if (cells.length >= 8 && cells[1] && /^\d+$/.test(cells[1])) {
            dataRows.push(cells);
          }
        }

        const hasNext = !!document.querySelector(`a[href*="Page$Next"]`);
        return { rows: dataRows, hasNext };
      }, TABLE_ID);

      if (data.rows.length === 0) break;
      rows.push(...data.rows);

      if (data.hasNext && pageNum < 300) {
        try {
          await page.click('a[href*="Page$Next"]');
          await delay(3000);
          pageNum++;
        } catch (e) {
          break;
        }
      } else {
        break;
      }
    }

    if (pageNum > 1) {
      process.stdout.write(`(${pageNum}pg) `);
    }
  } catch (err) {
    process.stdout.write(`[err] `);
  }

  await page.close();
  return rows;
}

async function main() {
  console.log(`Michigan LARA Scraper v2 - ${PROFESSION}`);
  console.log('='.repeat(50));

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--window-size=1400,900'],
  });

  const allRows = [];

  for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    process.stdout.write(`${letter}: `);
    const rows = await searchLetter(browser, PROFESSION, letter);
    const active = rows.filter(r => r[7] && ['active', 'renewed'].includes(r[7].toLowerCase()));
    allRows.push(...rows);
    console.log(`${rows.length} (${active.length} active) [cumul: ${allRows.length}]`);
  }

  // Dedup
  const seen = new Set();
  const unique = allRows.filter(r => {
    if (seen.has(r[1])) return false;
    seen.add(r[1]);
    return true;
  });
  const active = unique.filter(r => ['active', 'renewed'].includes((r[7] || '').toLowerCase()));

  console.log(`\nTotal unique: ${unique.length}`);
  console.log(`Active: ${active.length}`);
  console.log(`Lapsed/Deceased: ${unique.length - active.length}`);

  // Write CSVs
  const safeName = PROFESSION.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const headers = 'name,license_number,license_type,organization,dba_trade_name,status,expiration_date,state';

  // All
  const allPath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ALL_${ts}.csv`);
  const allLines = [headers];
  for (const r of unique) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    allLines.push([name, r[1], r[0], r[5], r[6], r[7], r[8], 'MI']
      .map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
  }
  fs.writeFileSync(allPath, allLines.join('\n'));
  console.log(`\nAll: ${allPath}`);

  // Active only
  const actPath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ACTIVE_${ts}.csv`);
  const actLines = [headers];
  for (const r of active) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    actLines.push([name, r[1], r[0], r[5], r[6], r[7], r[8], 'MI']
      .map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
  }
  fs.writeFileSync(actPath, actLines.join('\n'));
  console.log(`Active: ${actPath}`);

  await browser.close();
  console.log('\nDone!');
}

main().catch(console.error);
