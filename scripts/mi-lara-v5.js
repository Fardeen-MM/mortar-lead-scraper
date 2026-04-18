#!/usr/bin/env node
/**
 * Michigan LARA License Scraper v5
 *
 * Improvements:
 * - Saves progress after each letter (appends to CSV)
 * - Closes browser and reopens every 5 letters to prevent memory issues
 * - Proper error recovery
 * - Can run multiple professions
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

const ALL_PROFESSIONS = [
  'Dentist',
  'Dental Hygienist',
  'Chiropractor',
  'Optometrist',
  'Medical Doctor',
  'Osteopathic Physician',
  'Physical Therapist',
  'Physical Therapist Assistant',
  'Acupuncturist - Licensed',
  'Massage Therapist',
];

const PROFESSIONS = process.argv[2] ? [process.argv[2]] : ALL_PROFESSIONS;

async function launchBrowser() {
  return puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--single-process',
    ],
  });
}

async function extractPage(page) {
  return await page.evaluate((tableId) => {
    const table = document.getElementById(tableId);
    if (!table) return { rows: [], hasNext: false };

    const trs = table.querySelectorAll('tr');
    const dataRows = [];

    for (let i = 1; i < trs.length; i++) {
      const tr = trs[i];
      if (tr.querySelector('table')) continue;
      const cells = Array.from(tr.querySelectorAll('td'))
        .map(c => c.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
      if (cells.length >= 8 && cells[1] && /^\d+$/.test(cells[1])) {
        dataRows.push(cells);
      }
    }

    const links = document.querySelectorAll('a');
    let hasNext = false;
    for (const a of links) {
      if (a.textContent.trim().includes('Next') && a.href && a.href.includes('__doPostBack')) {
        hasNext = true;
        break;
      }
    }

    return { rows: dataRows, hasNext };
  }, TABLE_ID);
}

async function clickNext(page) {
  return await page.evaluate(() => {
    const links = document.querySelectorAll('a');
    for (const a of links) {
      if (a.textContent.trim().includes('Next') && a.href && a.href.includes('__doPostBack')) {
        a.click();
        return true;
      }
    }
    return false;
  });
}

async function searchLetter(browser, profession, letter) {
  let page;
  const rows = [];

  try {
    page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });

    await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await delay(2000);

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
    await page.evaluate(() => {
      document.querySelector('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]').value = '';
    });
    await page.type('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]', letter);
    await delay(300);

    await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
    await delay(8000);

    try {
      await page.waitForFunction((tid) => {
        return !!document.getElementById(tid) || document.body.innerText.includes('no results');
      }, { timeout: 15000 }, TABLE_ID);
    } catch (e) {}

    let pageNum = 1;
    while (true) {
      const data = await extractPage(page);
      if (data.rows.length === 0) break;
      rows.push(...data.rows);

      if (data.hasNext && pageNum < 300) {
        const clicked = await clickNext(page);
        if (!clicked) break;
        await delay(3500);
        pageNum++;
      } else {
        break;
      }
    }

    if (pageNum > 1) process.stdout.write(`(${pageNum}pg) `);
  } catch (err) {
    process.stdout.write(`[err] `);
  }

  try { if (page) await page.close(); } catch (e) {}
  return rows;
}

function appendToCsv(filepath, rows, profession) {
  const lines = [];
  for (const r of rows) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    lines.push([name, r[1], r[0], r[5], r[6], r[7], r[8], 'MI']
      .map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
  }
  fs.appendFileSync(filepath, lines.join('\n') + '\n');
}

async function scrapeProfession(profession) {
  const safeName = profession.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const headers = 'name,license_number,license_type,organization,dba_trade_name,status,expiration_date,state';

  const allPath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ALL_${ts}.csv`);
  fs.writeFileSync(allPath, headers + '\n');

  let browser = await launchBrowser();
  let totalRows = 0;
  let activeCount = 0;
  const allLicNums = new Set();

  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  for (let i = 0; i < letters.length; i++) {
    const letter = letters[i];
    process.stdout.write(`  ${letter}: `);

    // Restart browser every 8 letters to prevent memory leaks
    if (i > 0 && i % 8 === 0) {
      try { await browser.close(); } catch (e) {}
      browser = await launchBrowser();
      process.stdout.write('[restart] ');
    }

    try {
      const rows = await searchLetter(browser, profession, letter);

      // Dedup against already-seen license numbers
      const newRows = rows.filter(r => {
        if (allLicNums.has(r[1])) return false;
        allLicNums.add(r[1]);
        return true;
      });

      const active = newRows.filter(r => ['active', 'renewed'].includes((r[7] || '').toLowerCase()));
      activeCount += active.length;
      totalRows += newRows.length;

      if (newRows.length > 0) {
        appendToCsv(allPath, newRows, profession);
      }

      console.log(`${rows.length} (${active.length} active) [total: ${totalRows}, active: ${activeCount}]`);
    } catch (err) {
      console.log(`ERROR: ${err.message}`);
    }
  }

  try { await browser.close(); } catch (e) {}

  // Now create the active-only file from the full file
  const actPath = path.join(OUTPUT_DIR, `MI-LARA-${safeName}-ACTIVE_${ts}.csv`);
  const allData = fs.readFileSync(allPath, 'utf8');
  const allDataLines = allData.split('\n');
  const activeLines = [allDataLines[0]]; // header
  for (let i = 1; i < allDataLines.length; i++) {
    const line = allDataLines[i];
    if (line.toLowerCase().includes('"active"') || line.toLowerCase().includes('"renewed"')) {
      activeLines.push(line);
    }
  }
  fs.writeFileSync(actPath, activeLines.join('\n'));

  console.log(`\n  Total: ${totalRows} | Active: ${activeCount}`);
  console.log(`  ${allPath}`);
  console.log(`  ${actPath}`);

  return { profession, total: totalRows, active: activeCount, allPath, actPath };
}

async function main() {
  console.log('Michigan LARA License Scraper v5');
  console.log('================================');
  console.log(`Professions: ${PROFESSIONS.join(', ')}`);
  console.log(`Output: ${OUTPUT_DIR}`);
  console.log('NOTE: Email NOT available in public portal\n');

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const summary = [];

  for (const prof of PROFESSIONS) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`${prof}`);
    console.log('='.repeat(60));

    const result = await scrapeProfession(prof);
    summary.push(result);
  }

  // Final summary
  console.log('\n\n' + '='.repeat(70));
  console.log('FINAL SUMMARY');
  console.log('='.repeat(70));
  console.log(`${'Profession'.padEnd(30)} ${'Total'.padStart(8)} ${'Active'.padStart(8)}`);
  console.log('-'.repeat(50));
  for (const s of summary) {
    console.log(`${s.profession.padEnd(30)} ${String(s.total).padStart(8)} ${String(s.active).padStart(8)}`);
  }
  const totals = summary.reduce((a, b) => ({ total: a.total + b.total, active: a.active + b.active }), { total: 0, active: 0 });
  console.log('-'.repeat(50));
  console.log(`${'TOTAL'.padEnd(30)} ${String(totals.total).padStart(8)} ${String(totals.active).padStart(8)}`);
  console.log('\nNo email addresses available in public Accela portal.');
  console.log('Done!');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
