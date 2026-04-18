#!/usr/bin/env node
/**
 * Michigan LARA - Full profession scraper
 *
 * Scrapes the Accela public portal for all target professions.
 * For each profession, searches A-Z by last name, paginates all results.
 * Each letter search opens a fresh page to avoid state issues.
 *
 * Output columns: name, license_number, license_type, city, state, county, zip,
 *                 organization, status, issue_date, expiration_date
 *
 * NOTE: Email is NOT available in the public Accela portal.
 *       City/State/ZIP only available on detail pages (too slow for bulk).
 *       Search results have: License Type, License Number, First Name, Middle Initial,
 *       Last Name, Organization Name, DBA/Trade Name, License Status, Expiration Date.
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

async function searchLetter(browser, professionName, letter) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });
  const allRows = [];

  try {
    await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await delay(1500);

    // Select license type
    const matched = await page.evaluate((prof) => {
      const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
      if (!sel) return null;
      for (const o of sel.options) if (o.value === prof) return o.value;
      const pl = prof.toLowerCase();
      for (const o of sel.options) if (o.value.toLowerCase() === pl || o.value.toLowerCase().startsWith(pl)) return o.value;
      return null;
    }, professionName);

    if (!matched) { await page.close(); return []; }

    await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', matched);
    await delay(300);

    // Type last name letter
    await page.type('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]', letter);
    await delay(200);

    // Search
    await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
    await delay(6000);

    // Paginate
    let pageNum = 1;
    while (true) {
      const data = await page.evaluate(() => {
        const tables = document.querySelectorAll('table');
        for (const t of tables) {
          const txt = t.textContent || '';
          if (!txt.includes('License Type') || !txt.includes('License Number') || !txt.includes('Last Name')) continue;

          const rows = [];
          const allTr = t.querySelectorAll('tr');
          for (let i = 1; i < allTr.length; i++) {
            const tr = allTr[i];
            // Skip pager rows
            if (tr.querySelector('table') && tr.textContent.includes('Next')) continue;
            if (tr.querySelector('table') && tr.textContent.includes('Prev')) continue;
            const cells = [];
            tr.querySelectorAll('td').forEach(c => {
              cells.push(c.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
            });
            if (cells.length >= 8 && cells.some(c => c.length > 0)) {
              rows.push(cells);
            }
          }

          const hasNext = !!document.querySelector('a[href*="Page$Next"]');
          return { rows, hasNext };
        }
        return { rows: [], hasNext: false };
      });

      if (data.rows.length === 0) break;
      allRows.push(...data.rows);

      if (data.hasNext && pageNum < 500) {
        try {
          await page.click('a[href*="Page$Next"]');
          await delay(2500);
          pageNum++;
        } catch (e) { break; }
      } else {
        break;
      }
    }
  } catch (err) {
    process.stdout.write(`[err:${err.message.slice(0, 25)}] `);
  }

  await page.close();
  return allRows;
}

function writeCsv(professionName, rows) {
  const safeName = professionName.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const headers = ['name', 'license_number', 'license_type', 'organization', 'dba_trade_name', 'status', 'expiration_date'];

  // Deduplicate by license number
  const seen = new Set();
  const unique = rows.filter(r => {
    const key = r[1]; // license number
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // All records
  const allFilename = `MI-LARA-${safeName}-ALL_${timestamp}.csv`;
  const allPath = path.join(OUTPUT_DIR, allFilename);
  const allLines = [headers.map(h => `"${h}"`).join(',')];
  for (const r of unique) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    allLines.push([name, r[1], r[0], r[5], r[6], r[7], r[8]]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(allPath, allLines.join('\n'), 'utf8');

  // Active only
  const active = unique.filter(r => {
    const s = (r[7] || '').toLowerCase();
    return s === 'active' || s === 'renewed';
  });

  const activeFilename = `MI-LARA-${safeName}-ACTIVE_${timestamp}.csv`;
  const activePath = path.join(OUTPUT_DIR, activeFilename);
  const activeLines = [headers.map(h => `"${h}"`).join(',')];
  for (const r of active) {
    const name = [r[2], r[3], r[4]].filter(Boolean).join(' ').trim();
    activeLines.push([name, r[1], r[0], r[5], r[6], r[7], r[8]]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(activePath, activeLines.join('\n'), 'utf8');

  console.log(`  ${allFilename}: ${unique.length} total`);
  console.log(`  ${activeFilename}: ${active.length} active`);

  return { total: unique.length, active: active.length };
}

async function main() {
  console.log('Michigan LARA Full Scraper');
  console.log('=========================');
  console.log(`Professions: ${PROFESSIONS.length}`);
  console.log(`Output: ${OUTPUT_DIR}`);
  console.log('NOTE: Email is NOT available in public Accela portal.\n');

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const summary = [];

  for (const prof of PROFESSIONS) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`${prof}`);
    console.log('='.repeat(60));

    const allRows = [];
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

    for (const letter of letters) {
      process.stdout.write(`  ${letter}: `);
      const rows = await searchLetter(browser, prof, letter);
      allRows.push(...rows);
      const active = rows.filter(r => (r[7] || '').toLowerCase() === 'active' || (r[7] || '').toLowerCase() === 'renewed');
      console.log(`${rows.length} (${active.length} active) [total: ${allRows.length}]`);
    }

    if (allRows.length > 0) {
      const stats = writeCsv(prof, allRows);
      summary.push({ profession: prof, ...stats });
    } else {
      summary.push({ profession: prof, total: 0, active: 0 });
    }
  }

  // Final summary
  console.log('\n\n' + '='.repeat(70));
  console.log('FINAL SUMMARY - Michigan LARA License Data');
  console.log('='.repeat(70));
  console.log(`${'Profession'.padEnd(30)} ${'Total'.padStart(8)} ${'Active'.padStart(8)}`);
  console.log('-'.repeat(50));
  for (const s of summary) {
    console.log(`${s.profession.padEnd(30)} ${String(s.total).padStart(8)} ${String(s.active).padStart(8)}`);
  }
  const totals = summary.reduce((a, b) => ({ total: a.total + b.total, active: a.active + b.active }), { total: 0, active: 0 });
  console.log('-'.repeat(50));
  console.log(`${'TOTAL'.padEnd(30)} ${String(totals.total).padStart(8)} ${String(totals.active).padStart(8)}`);

  console.log('\n*** IMPORTANT: No email addresses available in public data ***');
  console.log('Email only available via FOIA request or authenticated MiPLUS login.');
  console.log('Data includes: name, license_number, license_type, organization, status, expiration_date');

  await browser.close();
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
