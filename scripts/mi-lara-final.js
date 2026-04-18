#!/usr/bin/env node
/**
 * Michigan LARA - Reliable scraper with visible browser and proper waits
 * Scrapes one profession at a time, two-letter prefixes for manageable result sets
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';
async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// Just do Dentist as proof of concept
const PROFESSION = process.argv[2] || 'Dentist';

async function searchAndCollect(browser, profession, prefix) {
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

    if (!matched) {
      await page.close();
      return rows;
    }

    await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', matched);
    await delay(500);

    // Type prefix
    await page.type('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]', prefix);
    await delay(300);

    // Click search and wait for response
    await Promise.all([
      page.click('#ctl00_PlaceHolderMain_btnNewSearch'),
      page.waitForNavigation({ waitUntil: 'networkidle0', timeout: 30000 }).catch(() => {}),
    ]);
    await delay(3000);

    // Wait for either results table or no-results message
    await page.waitForFunction(() => {
      const body = document.body.innerText;
      return body.includes('results found') || body.includes('no results') || body.includes('Showing');
    }, { timeout: 15000 }).catch(() => {});

    // Extract pages
    let pageNum = 1;
    while (true) {
      const data = await page.evaluate(() => {
        const tables = document.querySelectorAll('table');
        for (const t of tables) {
          const headerRow = t.querySelector('tr');
          if (!headerRow) continue;
          const text = headerRow.textContent || '';
          if (!text.includes('License Type') || !text.includes('License Number')) continue;

          const dataRows = [];
          const trs = t.querySelectorAll('tr');
          for (let i = 1; i < trs.length; i++) {
            const tr = trs[i];
            // Skip pager
            if (tr.querySelector('table')) continue;
            const cells = Array.from(tr.querySelectorAll('td'))
              .map(c => c.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
            if (cells.length >= 8 && cells[1]) {
              dataRows.push(cells);
            }
          }
          return { rows: dataRows, hasNext: !!document.querySelector('a[href*="Page$Next"]') };
        }
        return { rows: [], hasNext: false };
      });

      if (data.rows.length === 0) break;
      rows.push(...data.rows);

      if (data.hasNext && pageNum < 200) {
        await Promise.all([
          page.click('a[href*="Page$Next"]'),
          delay(3000),
        ]);
        pageNum++;
      } else {
        break;
      }
    }
  } catch (err) {
    // Ignore errors on individual searches
  }

  await page.close();
  return rows;
}

async function main() {
  console.log(`Michigan LARA Scraper - ${PROFESSION}`);
  console.log('='.repeat(50));
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--window-size=1400,900'],
  });

  const allRows = [];
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

  for (const letter of letters) {
    process.stdout.write(`${letter}: `);
    const rows = await searchAndCollect(browser, PROFESSION, letter);
    const active = rows.filter(r => r[7] && (r[7].toLowerCase() === 'active' || r[7].toLowerCase() === 'renewed'));
    allRows.push(...rows);
    console.log(`${rows.length} rows (${active.length} active) [total: ${allRows.length}]`);
  }

  // Dedup
  const seen = new Set();
  const unique = allRows.filter(r => {
    if (seen.has(r[1])) return false;
    seen.add(r[1]);
    return true;
  });

  const active = unique.filter(r => {
    const s = (r[7] || '').toLowerCase();
    return s === 'active' || s === 'renewed';
  });

  console.log(`\nTotal unique: ${unique.length}`);
  console.log(`Active: ${active.length}`);

  // Write CSV
  const safeName = PROFESSION.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  // All
  const allPath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ALL_${timestamp}.csv`);
  const headers = 'name,license_number,license_type,organization,dba_trade_name,status,expiration_date,state';
  const lines = [headers];
  for (const r of unique) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    lines.push([name, r[1], r[0], r[5], r[6], r[7], r[8], 'MI']
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(allPath, lines.join('\n'), 'utf8');
  console.log(`Wrote: ${allPath}`);

  // Active only
  const activePath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ACTIVE_${timestamp}.csv`);
  const activeLines = [headers];
  for (const r of active) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    activeLines.push([name, r[1], r[0], r[5], r[6], r[7], r[8], 'MI']
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(activePath, activeLines.join('\n'), 'utf8');
  console.log(`Wrote: ${activePath}`);

  await browser.close();
  console.log('Done!');
}

main().catch(console.error);
