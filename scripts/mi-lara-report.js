#!/usr/bin/env node
/**
 * Michigan LARA - Try generating reports & check for email columns
 * Also scrape the full result set from search results
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// License types to check
const PROFESSIONS = [
  'Dentist',
  'Dental Hygienist',
  'Chiropractor',
  'Optometrist',
  'Medical Doctor',
  'Osteopathic Physician',
  'Physical Therapist',
  'Acupuncturist - Licensed',
  'Massage Therapist',
];

async function scrapeSearchResults(page, professionName) {
  const allRows = [];
  let headers = [];
  let pageNum = 1;

  while (true) {
    // Extract data from current page
    const data = await page.evaluate(() => {
      // Find the results table - look for the one with license data headers
      const tables = document.querySelectorAll('table');
      let resultTable = null;

      for (const t of tables) {
        const firstRow = t.querySelector('tr');
        if (firstRow) {
          const text = firstRow.textContent;
          if (text.includes('License Type') && text.includes('License Number') && text.includes('Last Name')) {
            resultTable = t;
            break;
          }
        }
      }

      if (!resultTable) return { headers: [], rows: [], hasNext: false };

      // Extract headers
      const headerRow = resultTable.querySelector('tr');
      const hdrs = [];
      if (headerRow) {
        headerRow.querySelectorAll('th, td').forEach(cell => {
          // Get only the visible text, not hidden sort elements
          const spans = cell.querySelectorAll('span[id*="Label"], a');
          let text = '';
          if (spans.length > 0) {
            text = spans[0].textContent.trim();
          } else {
            text = cell.textContent.trim().split('\n')[0].trim();
          }
          hdrs.push(text);
        });
      }

      // Extract data rows - skip first row (header)
      const rows = [];
      const allRows = resultTable.querySelectorAll('tr');
      for (let i = 1; i < allRows.length; i++) {
        const row = allRows[i];
        // Skip pager row
        if (row.querySelector('table') && row.textContent.includes('Prev') && row.textContent.includes('Next')) continue;

        const cells = [];
        row.querySelectorAll('td').forEach(cell => {
          const link = cell.querySelector('a');
          let text = cell.textContent.trim();
          // Clean up zero-width chars
          text = text.replace(/[\u200B-\u200D\uFEFF]/g, '');
          cells.push(text);
        });
        if (cells.length > 3 && cells.some(c => c.length > 0)) {
          rows.push(cells);
        }
      }

      // Check for next page
      const hasNext = !!document.querySelector('a[href*="Page$Next"]');

      return { headers: hdrs, rows, hasNext };
    });

    if (data.headers.length > 0 && headers.length === 0) {
      headers = data.headers.filter(h => h.length > 0);
      console.log(`  Headers: ${headers.join(' | ')}`);
    }

    if (data.rows.length === 0) break;

    allRows.push(...data.rows);

    // Navigate to next page if available
    if (data.hasNext && pageNum < 200) {
      try {
        await page.click('a[href*="Page$Next"]');
        await delay(3000);
        pageNum++;
        if (pageNum % 5 === 0) {
          console.log(`    Page ${pageNum}: ${allRows.length} rows so far...`);
        }
      } catch (e) {
        break;
      }
    } else {
      break;
    }
  }

  return { headers, rows: allRows, pages: pageNum };
}

async function scrapeOneProfession(browser, professionName) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Scraping: ${professionName}`);
  console.log('='.repeat(60));

  const allRows = [];
  let headers = [];

  for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });

    try {
      await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
      await delay(1500);

      // Select license type
      const matchedValue = await page.evaluate((prof) => {
        const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
        if (!sel) return null;
        for (const opt of sel.options) {
          if (opt.value === prof) return opt.value;
        }
        // Partial match
        const profLower = prof.toLowerCase();
        for (const opt of sel.options) {
          if (opt.value.toLowerCase() === profLower || opt.value.toLowerCase().startsWith(profLower)) return opt.value;
        }
        return null;
      }, professionName);

      if (!matchedValue) {
        console.log(`  "${professionName}" not found in dropdown. Skipping.`);
        await page.close();
        return { profession: professionName, headers: [], rows: [], error: 'Not in dropdown' };
      }

      await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', matchedValue);
      await delay(300);

      // Enter last name letter
      const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
      if (lastNameInput) {
        await lastNameInput.type(letter);
      }
      await delay(200);

      // Click search
      await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
      await delay(6000);

      // Scrape all pages for this letter
      const result = await scrapeSearchResults(page, professionName);

      if (result.headers.length > 0 && headers.length === 0) {
        headers = result.headers;
      }

      allRows.push(...result.rows);
      console.log(`  Letter ${letter}: ${result.rows.length} rows, ${result.pages} pages (total: ${allRows.length})`);

    } catch (err) {
      console.log(`  Letter ${letter} error: ${err.message}`);
    }

    await page.close();
  }

  return { profession: professionName, headers, rows: allRows };
}

function writeCsv(professionName, headers, rows) {
  const safeName = professionName.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  // Write full data
  const filename = `MI-LARA-${safeName}_${timestamp}.csv`;
  const filepath = path.join(OUTPUT_DIR, filename);

  // Clean headers
  const cleanHeaders = headers.length > 0 ? headers :
    ['License Type', 'License Number', 'First Name', 'Middle Initial', 'Last Name',
     'Organization Name', 'DBA/Trade Name', 'License Status', 'License Expiration Date'];

  const lines = [cleanHeaders.map(h => `"${h}"`).join(',')];
  for (const row of rows) {
    const escaped = row.map(cell => `"${(cell || '').replace(/"/g, '""')}"`);
    lines.push(escaped.join(','));
  }

  fs.writeFileSync(filepath, lines.join('\n'), 'utf8');
  console.log(`  Wrote ${rows.length} rows to ${filename}`);

  // Also write a clean CSV with just the key fields
  // Based on the screenshot, columns are:
  // 0: License Type, 1: License Number, 2: First Name, 3: Middle Initial, 4: Last Name
  // 5: Organization Name, 6: DBA/Trade Name, 7: License Status, 8: License Expiration Date
  const cleanFilename = `MI-LARA-${safeName}-CLEAN_${timestamp}.csv`;
  const cleanPath = path.join(OUTPUT_DIR, cleanFilename);

  // Filter to only active licenses
  const activeRows = rows.filter(r => {
    const status = (r[7] || '').toLowerCase();
    return status === 'active' || status === 'renewed' || status === '';
  });

  const cleanLines = ['"name","license_number","license_type","organization","city","state","status","expiration_date"'];
  for (const row of activeRows) {
    const firstName = (row[2] || '').trim();
    const middleInit = (row[3] || '').trim();
    const lastName = (row[4] || '').trim();
    const name = [firstName, middleInit, lastName].filter(Boolean).join(' ');
    const licNum = (row[1] || '').trim();
    const licType = (row[0] || professionName).trim();
    const org = (row[5] || '').trim();
    const status = (row[7] || '').trim();
    const expDate = (row[8] || '').trim();

    cleanLines.push([name, licNum, licType, org, '', 'MI', status, expDate]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }

  fs.writeFileSync(cleanPath, cleanLines.join('\n'), 'utf8');
  console.log(`  Clean export (active only): ${activeRows.length}/${rows.length} → ${cleanFilename}`);

  return { total: rows.length, active: activeRows.length };
}

async function main() {
  console.log('Michigan LARA License Scraper - Full Run');
  console.log('=========================================');
  console.log(`Professions: ${PROFESSIONS.length}`);
  console.log(`Output: ${OUTPUT_DIR}\n`);

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const summary = [];

  for (const prof of PROFESSIONS) {
    const result = await scrapeOneProfession(browser, prof);
    const stats = result.rows.length > 0 ?
      writeCsv(prof, result.headers, result.rows) :
      { total: 0, active: 0 };

    summary.push({
      profession: prof,
      total: stats.total,
      active: stats.active,
      hasEmail: false, // Accela search doesn't show email
      error: result.error || null,
    });
  }

  // Print summary
  console.log('\n\n' + '='.repeat(70));
  console.log('SUMMARY - Michigan LARA License Data');
  console.log('='.repeat(70));
  console.log('NOTE: The Accela search portal does NOT expose email addresses.');
  console.log('Email data is only available via FOIA request or MiPLUS login.\n');

  console.log(`${'Profession'.padEnd(30)} ${'Total'.padStart(8)} ${'Active'.padStart(8)}`);
  console.log('-'.repeat(50));

  for (const s of summary) {
    console.log(`${s.profession.padEnd(30)} ${String(s.total).padStart(8)} ${String(s.active).padStart(8)}`);
  }

  const totalLeads = summary.reduce((a, b) => a + b.total, 0);
  const totalActive = summary.reduce((a, b) => a + b.active, 0);

  console.log('-'.repeat(50));
  console.log(`${'TOTAL'.padEnd(30)} ${String(totalLeads).padStart(8)} ${String(totalActive).padStart(8)}`);

  await browser.close();
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
