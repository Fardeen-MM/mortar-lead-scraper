#!/usr/bin/env node
/**
 * Michigan LARA License List Scraper
 *
 * Scrapes the Accela Citizen Access portal for Michigan professional licenses.
 * Searches by license type, pages through results, and exports to CSV.
 * Checks for email column presence.
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');

puppeteer.use(StealthPlugin());

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const BASE_URL = 'https://aca-prod.accela.com/MILARA/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y&TabName=APO';

// Professions to scrape
const PROFESSIONS = [
  'Dentist',
  'Dental Hygienist',
  'Chiropractor',
  'Optometrist',
  'Medical Doctor',
  'Osteopathic Physician & Surgeon',
  'Physical Therapist',
  'Physical Therapist Assistant',
  'Acupuncturist Licensed',
  'Massage Therapist',
];

const SELECTORS = {
  licenseType: 'select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]',
  lastName: 'input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]',
  searchBtn: '#ctl00_PlaceHolderMain_btnNewSearch',
  resultsGrid: '.ACA_Grid_Alternate, .ACA_Grid_EvenRow, .ACA_Grid_OddRow, table[id*="gdvLicenseeList"]',
  resultTable: 'table[id*="gdvLicenseeList"]',
  noResults: '.ACA_NoPaddingCell',
  pagerNext: 'a[id*="gdvLicenseeList"][id*="Next"]',
  resultRows: 'table[id*="gdvLicenseeList"] tr',
};

async function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function scrapeOneProfession(browser, professionName) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Scraping: ${professionName}`);
  console.log('='.repeat(60));

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  try {
    // Navigate to search page
    console.log('  Loading search page...');
    await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await delay(2000);

    // Select license type
    console.log(`  Selecting license type: ${professionName}`);

    // Check if the exact option exists
    const optionExists = await page.evaluate((prof) => {
      const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
      if (!sel) return false;
      for (const opt of sel.options) {
        if (opt.value === prof || opt.text === prof) return true;
      }
      return false;
    }, professionName);

    if (!optionExists) {
      // Try partial match
      const matchedValue = await page.evaluate((prof) => {
        const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
        if (!sel) return null;
        const profLower = prof.toLowerCase();
        for (const opt of sel.options) {
          if (opt.value.toLowerCase().startsWith(profLower) || opt.text.toLowerCase().startsWith(profLower)) {
            return opt.value;
          }
        }
        return null;
      }, professionName);

      if (!matchedValue) {
        console.log(`  WARNING: License type "${professionName}" not found in dropdown. Skipping.`);
        await page.close();
        return { profession: professionName, error: 'Not found in dropdown', leads: [], hasEmail: false };
      }
      console.log(`  Matched to: ${matchedValue}`);
      await page.select(SELECTORS.licenseType, matchedValue);
    } else {
      await page.select(SELECTORS.licenseType, professionName);
    }

    await delay(500);

    // We need at least a last name to search. Let's try searching with wildcard or letter-by-letter
    // Accela typically requires some input. Let's try just clicking search with only the license type.
    console.log('  Submitting search (license type only)...');

    await page.click(SELECTORS.searchBtn);

    // Wait for results or error message
    await delay(5000);

    // Check if we got an error about needing more criteria
    const pageContent = await page.content();
    const needsMoreCriteria = pageContent.includes('Enter at least') ||
                              pageContent.includes('Please enter') ||
                              pageContent.includes('criteria') ||
                              pageContent.includes('required');

    if (needsMoreCriteria) {
      console.log('  System requires search criteria. Searching letter by letter (A-Z)...');
      return await scrapeByLetters(page, professionName);
    }

    // Try to find results
    return await extractResults(page, professionName);

  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    await page.close();
    return { profession: professionName, error: err.message, leads: [], hasEmail: false };
  }
}

async function scrapeByLetters(page, professionName) {
  const allLeads = [];
  let hasEmail = false;
  let headers = [];

  // Try A-Z for last name
  for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    try {
      console.log(`  Searching last name starting with: ${letter}...`);

      // Navigate fresh each time to avoid stale state
      await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
      await delay(2000);

      // Select license type
      const matchedValue = await page.evaluate((prof) => {
        const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
        if (!sel) return null;
        const profLower = prof.toLowerCase();
        for (const opt of sel.options) {
          if (opt.value === prof || opt.value.toLowerCase().startsWith(profLower)) {
            return opt.value;
          }
        }
        return null;
      }, professionName);

      if (matchedValue) {
        await page.select(SELECTORS.licenseType, matchedValue);
      }

      await delay(300);

      // Enter last name letter
      const lastNameInput = await page.$(SELECTORS.lastName);
      if (lastNameInput) {
        await lastNameInput.click({ clickCount: 3 });
        await lastNameInput.type(letter);
      }

      await delay(300);

      // Click search
      await page.click(SELECTORS.searchBtn);
      await delay(5000);

      // Wait for results table or timeout
      try {
        await page.waitForSelector('table[id*="gdvLicenseeList"], .ACA_NoPaddingCell, [id*="noDataMessage"]', { timeout: 15000 });
      } catch (e) {
        // timeout - check content
      }

      const result = await extractResults(page, professionName, letter);

      if (result.headers && result.headers.length > 0) {
        headers = result.headers;
      }
      if (result.hasEmail) hasEmail = true;

      allLeads.push(...result.leads);
      console.log(`    Letter ${letter}: ${result.leads.length} results (total: ${allLeads.length})`);

      // After first successful result, check if there's an email column
      if (result.leads.length > 0 && !hasEmail && result.headers) {
        const emailIdx = result.headers.findIndex(h =>
          h.toLowerCase().includes('email') || h.toLowerCase().includes('e-mail')
        );
        if (emailIdx >= 0) {
          hasEmail = true;
          console.log(`  *** EMAIL COLUMN FOUND at index ${emailIdx}! ***`);
        }
      }

    } catch (err) {
      console.log(`    Letter ${letter} error: ${err.message}`);
    }
  }

  return { profession: professionName, leads: allLeads, hasEmail, headers };
}

async function extractResults(page, professionName, letterPrefix = '') {
  const leads = [];
  let hasEmail = false;
  let headers = [];
  let pageNum = 1;

  while (true) {
    // Extract table headers and rows
    const pageData = await page.evaluate(() => {
      const table = document.querySelector('table[id*="gdvLicenseeList"]');
      if (!table) return { headers: [], rows: [], hasMore: false };

      const headerRow = table.querySelector('tr.ACA_Grid_Header, tr:first-child');
      const hdrs = [];
      if (headerRow) {
        headerRow.querySelectorAll('th, td').forEach(cell => {
          const text = (cell.textContent || '').trim();
          if (text && text !== '' && !text.includes('Sort') && text !== 'No.') {
            hdrs.push(text);
          }
        });
      }

      const dataRows = [];
      table.querySelectorAll('tr.ACA_Grid_EvenRow, tr.ACA_Grid_OddRow, tr.ACA_Grid_Alternate').forEach(row => {
        const cells = [];
        row.querySelectorAll('td').forEach(cell => {
          // Check for links too
          const link = cell.querySelector('a');
          cells.push((cell.textContent || '').trim());
        });
        if (cells.length > 0) {
          dataRows.push(cells);
        }
      });

      // Check for next page link
      const hasMore = !!document.querySelector('a[id*="gdvLicenseeList"][href*="Page$Next"]');

      return { headers: hdrs, rows: dataRows, hasMore };
    });

    if (pageData.headers.length > 0 && headers.length === 0) {
      headers = pageData.headers;
      console.log(`  Headers: ${headers.join(' | ')}`);

      // Check for email
      const emailIdx = headers.findIndex(h =>
        h.toLowerCase().includes('email') || h.toLowerCase().includes('e-mail')
      );
      if (emailIdx >= 0) {
        hasEmail = true;
        console.log(`  *** EMAIL COLUMN DETECTED! ***`);
      }
    }

    if (pageData.rows.length === 0) {
      if (pageNum === 1) {
        // Check for "no results" message
        const noResults = await page.evaluate(() => {
          const body = document.body.innerText;
          return body.includes('No records found') || body.includes('no record') || body.includes('0 result');
        });
        if (noResults) {
          console.log(`  No results found.`);
        }
      }
      break;
    }

    leads.push(...pageData.rows);

    if (pageNum === 1) {
      console.log(`  Page ${pageNum}: ${pageData.rows.length} rows (sample: ${pageData.rows[0] ? pageData.rows[0].slice(0, 3).join(', ') : 'N/A'})`);
    }

    // Try to go to next page
    if (pageData.hasMore) {
      try {
        await page.click('a[id*="gdvLicenseeList"][href*="Page$Next"]');
        await delay(3000);
        pageNum++;

        if (pageNum % 10 === 0) {
          console.log(`  Page ${pageNum}: ${leads.length} total rows so far...`);
        }

        // Safety limit
        if (pageNum > 500) {
          console.log('  Reached 500 page limit, stopping pagination.');
          break;
        }
      } catch (e) {
        break;
      }
    } else {
      break;
    }
  }

  return { profession: professionName, leads, hasEmail, headers };
}

function writeCsv(profession, headers, leads, hasEmail) {
  const safeName = profession.replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const filename = `MI-LARA-${safeName}_${timestamp}.csv`;
  const filepath = path.join(OUTPUT_DIR, filename);

  // Build CSV
  const csvHeaders = headers.length > 0 ? headers : ['Col1', 'Col2', 'Col3', 'Col4', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10'];

  const lines = [csvHeaders.map(h => `"${h}"`).join(',')];
  for (const row of leads) {
    const escaped = row.map(cell => `"${(cell || '').replace(/"/g, '""')}"`);
    lines.push(escaped.join(','));
  }

  fs.writeFileSync(filepath, lines.join('\n'), 'utf8');
  console.log(`  Wrote ${leads.length} rows to ${filename}`);

  // If has email, also write a clean version
  if (hasEmail) {
    const emailIdx = headers.findIndex(h =>
      h.toLowerCase().includes('email') || h.toLowerCase().includes('e-mail')
    );
    const nameIdx = headers.findIndex(h => h.toLowerCase().includes('name'));
    const cityIdx = headers.findIndex(h => h.toLowerCase().includes('city'));
    const stateIdx = headers.findIndex(h => h.toLowerCase().includes('state'));
    const licTypeIdx = headers.findIndex(h =>
      h.toLowerCase().includes('license type') || h.toLowerCase().includes('profession')
    );

    if (emailIdx >= 0) {
      const cleanFilename = `MI-LARA-${safeName}-EMAILS_${timestamp}.csv`;
      const cleanPath = path.join(OUTPUT_DIR, cleanFilename);

      const cleanLines = ['"name","email","city","state","license_type"'];
      let emailCount = 0;

      for (const row of leads) {
        const email = (row[emailIdx] || '').trim();
        if (email && email.includes('@')) {
          emailCount++;
          const name = (row[nameIdx] || row[0] || '').trim();
          const city = (row[cityIdx] || '').trim();
          const state = (row[stateIdx] || 'MI').trim();
          const licType = (row[licTypeIdx] || profession).trim();

          cleanLines.push([name, email, city, state, licType].map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
        }
      }

      fs.writeFileSync(cleanPath, cleanLines.join('\n'), 'utf8');
      console.log(`  Clean email export: ${emailCount}/${leads.length} have email → ${cleanFilename}`);
      return { emailCount, totalCount: leads.length };
    }
  }

  return { emailCount: 0, totalCount: leads.length };
}

async function main() {
  console.log('Michigan LARA Professional License Scraper');
  console.log('==========================================');
  console.log(`Output directory: ${OUTPUT_DIR}`);
  console.log(`Professions to scrape: ${PROFESSIONS.length}`);
  console.log();

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: false, // Use visible browser for debugging
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--window-size=1400,900',
    ],
  });

  const summary = [];

  // First, let's just check what columns are available by doing a quick test search
  console.log('Step 1: Quick test search to check column structure...');
  const testPage = await browser.newPage();
  await testPage.setViewport({ width: 1400, height: 900 });

  try {
    await testPage.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await delay(2000);

    // List all available license types
    const allTypes = await testPage.evaluate(() => {
      const sel = document.querySelector('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]');
      if (!sel) return [];
      return Array.from(sel.options).map(o => o.value).filter(v => v);
    });
    console.log(`\nAvailable license types (${allTypes.length} total):`);
    for (const t of allTypes) {
      if (PROFESSIONS.some(p => t.toLowerCase().includes(p.toLowerCase().split(' ')[0]))) {
        console.log(`  ✓ ${t}`);
      }
    }

    // Do a test search for "Dentist" with last name "A"
    console.log('\nTest search: Dentist, last name starts with "A"...');
    await testPage.select(SELECTORS.licenseType, 'Dentist');
    await delay(300);

    const lastNameInput = await testPage.$(SELECTORS.lastName);
    if (lastNameInput) {
      await lastNameInput.type('A');
    }

    await testPage.click(SELECTORS.searchBtn);
    await delay(8000);

    // Check for results
    const testResult = await testPage.evaluate(() => {
      const table = document.querySelector('table[id*="gdvLicenseeList"]');
      if (!table) {
        // Check all tables
        const tables = document.querySelectorAll('table');
        const info = [];
        for (const t of tables) {
          const firstRow = t.querySelector('tr');
          if (firstRow) {
            const cells = Array.from(firstRow.querySelectorAll('th, td')).map(c => c.textContent.trim()).filter(Boolean);
            if (cells.length > 3) {
              info.push(cells.join(' | '));
            }
          }
        }
        return { found: false, tables: info, bodySnippet: document.body.innerText.substring(0, 2000) };
      }

      // Get all header cells
      const allHeaders = [];
      table.querySelectorAll('tr').forEach((row, i) => {
        if (i < 3) {
          const cells = Array.from(row.querySelectorAll('th, td')).map(c => c.textContent.trim());
          allHeaders.push(cells);
        }
      });

      // Get first few data rows
      const firstRows = [];
      table.querySelectorAll('tr.ACA_Grid_EvenRow, tr.ACA_Grid_OddRow, tr.ACA_Grid_Alternate').forEach((row, i) => {
        if (i < 3) {
          const cells = Array.from(row.querySelectorAll('td')).map(c => c.textContent.trim());
          firstRows.push(cells);
        }
      });

      return { found: true, allHeaders, firstRows };
    });

    console.log('\nTest search results:');
    console.log(JSON.stringify(testResult, null, 2));

    // Also check the page HTML for any email-related content
    const pageHTML = await testPage.content();
    const hasEmailInPage = pageHTML.toLowerCase().includes('email');
    console.log(`\nPage contains "email" text: ${hasEmailInPage}`);

    // Screenshot for verification
    await testPage.screenshot({ path: '/tmp/mi-lara-test.png', fullPage: false });
    console.log('Screenshot saved to /tmp/mi-lara-test.png');

  } catch (err) {
    console.log(`Test search error: ${err.message}`);
  }

  await testPage.close();

  console.log('\n\nStep 2: Full scrape of all professions...');
  console.log('(This may take a while - 26 letters x multiple professions)\n');

  // For now, just do a quick check of each profession (letter A only) to see which have email
  for (const prof of PROFESSIONS) {
    try {
      const result = await scrapeOneProfession(browser, prof);

      const stats = result.leads.length > 0 && result.headers ?
        writeCsv(prof, result.headers, result.leads, result.hasEmail) :
        { emailCount: 0, totalCount: result.leads.length };

      summary.push({
        profession: prof,
        total: stats.totalCount,
        emails: stats.emailCount,
        hasEmail: result.hasEmail,
        error: result.error || null,
      });

    } catch (err) {
      summary.push({
        profession: prof,
        total: 0,
        emails: 0,
        hasEmail: false,
        error: err.message,
      });
    }
  }

  // Print summary
  console.log('\n\n' + '='.repeat(70));
  console.log('SUMMARY');
  console.log('='.repeat(70));
  console.log(`${'Profession'.padEnd(35)} ${'Total'.padStart(8)} ${'Emails'.padStart(8)} ${'Has Email?'.padStart(12)}`);
  console.log('-'.repeat(70));

  for (const s of summary) {
    const emailFlag = s.hasEmail ? 'YES' : (s.error ? `ERR: ${s.error.slice(0, 20)}` : 'NO');
    console.log(`${s.profession.padEnd(35)} ${String(s.total).padStart(8)} ${String(s.emails).padStart(8)} ${emailFlag.padStart(12)}`);
  }

  const totalLeads = summary.reduce((a, b) => a + b.total, 0);
  const totalEmails = summary.reduce((a, b) => a + b.emails, 0);
  const withEmail = summary.filter(s => s.hasEmail).length;

  console.log('-'.repeat(70));
  console.log(`${'TOTAL'.padEnd(35)} ${String(totalLeads).padStart(8)} ${String(totalEmails).padStart(8)} ${`${withEmail}/${summary.length}`.padStart(12)}`);

  await browser.close();
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
