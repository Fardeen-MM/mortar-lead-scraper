#!/usr/bin/env node
/**
 * Michigan LARA - Full scraper
 *
 * Strategy: Search letter by letter, paginate through ALL results.
 * Collect: License Type, License Number, First Name, Middle Initial, Last Name,
 *          Organization Name, DBA/Trade Name, License Status, License Expiration Date
 *
 * Then click into a few active license detail pages to check for email/address.
 * Also try the hidden report button.
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

const PROFESSIONS = [
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

async function extractTableData(page) {
  return await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    let resultTable = null;
    for (const t of tables) {
      const text = t.textContent || '';
      if (text.includes('License Type') && text.includes('License Number') && text.includes('Last Name')) {
        resultTable = t;
        break;
      }
    }
    if (!resultTable) return { rows: [], hasNext: false, totalText: '' };

    const rows = [];
    const allRows = resultTable.querySelectorAll('tr');
    for (let i = 1; i < allRows.length; i++) {
      const row = allRows[i];
      // Skip pager row
      if (row.querySelector('table') && row.textContent.includes('Next')) continue;
      const cells = [];
      row.querySelectorAll('td').forEach(cell => {
        cells.push(cell.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
      });
      if (cells.length >= 8 && cells.some(c => c.length > 0)) {
        rows.push(cells);
      }
    }

    const hasNext = !!document.querySelector('a[href*="Page$Next"]');

    // Get total count text
    const totalText = (document.body.innerText.match(/Showing\s+\d+-\d+\s+of\s+\S+/i) || [''])[0];

    return { rows, hasNext, totalText };
  });
}

async function searchAndPaginate(page, professionName, letter) {
  const allRows = [];

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
      const profLower = prof.toLowerCase();
      for (const opt of sel.options) {
        if (opt.value.toLowerCase() === profLower || opt.value.toLowerCase().startsWith(profLower)) return opt.value;
      }
      return null;
    }, professionName);

    if (!matchedValue) return allRows;

    await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', matchedValue);
    await delay(300);

    const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
    if (lastNameInput) {
      await lastNameInput.type(letter);
    }
    await delay(200);

    await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
    await delay(6000);

    // Paginate through all results
    let pageNum = 1;
    while (true) {
      const data = await extractTableData(page);

      if (data.rows.length === 0) break;
      allRows.push(...data.rows);

      if (pageNum === 1 && data.totalText) {
        process.stdout.write(`${data.totalText} `);
      }

      if (data.hasNext && pageNum < 300) {
        await page.click('a[href*="Page$Next"]');
        await delay(2500);
        pageNum++;
      } else {
        break;
      }
    }
  } catch (err) {
    process.stdout.write(`[ERR:${err.message.slice(0, 30)}] `);
  }

  return allRows;
}

function writeCsv(professionName, rows) {
  const safeName = professionName.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  const headers = ['License Type', 'License Number', 'First Name', 'Middle Initial', 'Last Name',
    'Organization Name', 'DBA/Trade Name', 'License Status', 'License Expiration Date'];

  // Full file
  const fullFilename = `MI-LARA-${safeName}-ALL_${timestamp}.csv`;
  const fullPath = path.join(OUTPUT_DIR, fullFilename);
  const lines = [headers.map(h => `"${h}"`).join(',')];
  for (const row of rows) {
    lines.push(row.map(cell => `"${(cell || '').replace(/"/g, '""')}"`).join(','));
  }
  fs.writeFileSync(fullPath, lines.join('\n'), 'utf8');

  // Active only - clean format
  const activeRows = rows.filter(r => {
    const status = (r[7] || '').toLowerCase();
    return status === 'active' || status === 'renewed';
  });

  const cleanFilename = `MI-LARA-${safeName}-ACTIVE_${timestamp}.csv`;
  const cleanPath = path.join(OUTPUT_DIR, cleanFilename);
  const cleanLines = ['"name","license_number","license_type","organization","status","expiration_date"'];
  for (const row of activeRows) {
    const name = [row[2], row[3], row[4]].filter(Boolean).join(' ').trim();
    cleanLines.push([name, row[1], row[0], row[5], row[7], row[8]]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(cleanPath, cleanLines.join('\n'), 'utf8');

  console.log(`  Written: ${fullFilename} (${rows.length} total)`);
  console.log(`  Written: ${cleanFilename} (${activeRows.length} active)`);

  return { total: rows.length, active: activeRows.length };
}

async function checkDetailPageForEmail(browser) {
  console.log('\n\nChecking detail pages for email/address fields...');
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 1000 });

  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Search for a common active dentist
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);

  const lastNameInput = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await lastNameInput.type('Johnson');
  await delay(200);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Navigate to page 5+ where active licenses are more likely
  for (let i = 0; i < 5; i++) {
    try {
      const hasNext = await page.$('a[href*="Page$Next"]');
      if (hasNext) {
        await hasNext.click();
        await delay(3000);
      }
    } catch (e) { break; }
  }

  // Find an active dentist
  const activeLink = await page.evaluate(() => {
    const tables = document.querySelectorAll('table');
    for (const t of tables) {
      if (!t.textContent.includes('License Type')) continue;
      const rows = t.querySelectorAll('tr');
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 8) {
          const status = cells[7].textContent.trim().toLowerCase();
          if (status === 'active' || status === 'renewed') {
            const link = cells[1].querySelector('a');
            if (link) return link.href;
          }
        }
      }
    }
    return null;
  });

  if (activeLink) {
    console.log(`Found active license detail link: ${activeLink}`);
    await page.goto(activeLink, { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(5000);

    await page.screenshot({ path: '/tmp/mi-lara-active-detail.png', fullPage: true });

    const detailText = await page.evaluate(() => document.body.innerText);

    // Check for email
    const emails = detailText.match(/[\w.+-]+@[\w.-]+\.\w+/g);
    console.log(`Emails found: ${emails ? emails.join(', ') : 'NONE'}`);

    // Extract all field labels
    const fields = detailText.split('\n')
      .filter(line => line.includes(':'))
      .map(line => line.trim())
      .filter(line => line.length < 200 && line.length > 3);

    console.log('\nAll fields on detail page:');
    for (const f of fields) {
      console.log(`  ${f}`);
    }
  } else {
    console.log('No active license found to check detail page.');
  }

  await page.close();
}

async function main() {
  console.log('Michigan LARA Full License Scraper');
  console.log('==================================\n');

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  // First, check an active license detail for email
  await checkDetailPageForEmail(browser);

  const summary = [];

  for (const prof of PROFESSIONS) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Scraping: ${prof}`);
    console.log('='.repeat(60));

    const allRows = [];

    for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
      process.stdout.write(`  ${letter}: `);
      const rows = await searchAndPaginate(await browser.newPage().catch(() => null) || (await browser.pages())[0], prof, letter);
      allRows.push(...rows);
      console.log(`${rows.length} rows (total: ${allRows.length})`);
    }

    const stats = allRows.length > 0 ? writeCsv(prof, allRows) : { total: 0, active: 0 };
    summary.push({ profession: prof, ...stats });
  }

  // Summary
  console.log('\n\n' + '='.repeat(70));
  console.log('SUMMARY');
  console.log('='.repeat(70));
  console.log(`${'Profession'.padEnd(30)} ${'Total'.padStart(8)} ${'Active'.padStart(8)}`);
  console.log('-'.repeat(50));
  for (const s of summary) {
    console.log(`${s.profession.padEnd(30)} ${String(s.total).padStart(8)} ${String(s.active).padStart(8)}`);
  }
  const totals = summary.reduce((a, b) => ({ total: a.total + b.total, active: a.active + b.active }), { total: 0, active: 0 });
  console.log('-'.repeat(50));
  console.log(`${'TOTAL'.padEnd(30)} ${String(totals.total).padStart(8)} ${String(totals.active).padStart(8)}`);

  console.log('\nNOTE: Michigan LARA Accela public portal does NOT expose email addresses.');
  console.log('Email is only available via FOIA request or authenticated MiPLUS reports.');

  await browser.close();
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
