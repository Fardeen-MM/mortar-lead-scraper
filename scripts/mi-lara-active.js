#!/usr/bin/env node
/**
 * Michigan LARA - Find active dentists and check detail pages for email
 * Strategy: Use specific page navigation to jump past old records
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
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1400,1200'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 1200 });

  console.log('Loading search page...');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await delay(2000);

  // Strategy: Search for a VERY specific last name that is common among active practitioners
  // Also try two-letter prefix to get fewer results and reach active ones faster
  console.log('Searching for Dentist, last name "Zu" (short list to find active)...');
  await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
  await delay(300);
  const ln = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
  await ln.type('Zu');
  await delay(200);

  await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
  await delay(8000);

  // Get all results from this small set
  const getAllData = async () => {
    const results = [];
    let pageNum = 1;

    while (true) {
      const data = await page.evaluate(() => {
        const tables = document.querySelectorAll('table');
        for (const t of tables) {
          if (!t.textContent.includes('License Type') || !t.textContent.includes('License Number')) continue;
          const rows = [];
          const allRows = t.querySelectorAll('tr');
          for (let i = 1; i < allRows.length; i++) {
            const row = allRows[i];
            if (row.querySelector('table') && row.textContent.includes('Next')) continue;
            const cells = [];
            const hrefs = [];
            row.querySelectorAll('td').forEach(cell => {
              cells.push(cell.textContent.trim().replace(/[\u200B-\u200D\uFEFF]/g, ''));
              const a = cell.querySelector('a');
              hrefs.push(a ? a.href : '');
            });
            if (cells.length >= 8) rows.push({ cells, hrefs });
          }
          const hasNext = !!document.querySelector('a[href*="Page$Next"]');
          return { rows, hasNext };
        }
        return { rows: [], hasNext: false };
      });

      results.push(...data.rows);
      if (data.hasNext && pageNum < 50) {
        await page.click('a[href*="Page$Next"]');
        await delay(3000);
        pageNum++;
      } else break;
    }
    return results;
  };

  const allResults = await getAllData();
  console.log(`Total "Zu" results: ${allResults.length}`);

  // Find active ones
  const activeResults = allResults.filter(r => {
    const status = r.cells[7].toLowerCase();
    return status === 'active' || status === 'renewed';
  });
  console.log(`Active "Zu" dentists: ${activeResults.length}`);

  for (const r of activeResults.slice(0, 5)) {
    console.log(`  ${r.cells[2]} ${r.cells[4]} | ${r.cells[1]} | ${r.cells[7]} | ${r.cells[8]}`);
  }

  // Now check detail pages for the first 3 active dentists
  const detailResults = [];
  for (const active of activeResults.slice(0, 3)) {
    const detailUrl = active.hrefs.find(h => h && h.includes('accela'));
    if (!detailUrl) continue;

    console.log(`\nChecking detail for ${active.cells[2]} ${active.cells[4]}...`);
    await page.goto(detailUrl, { waitUntil: 'networkidle2', timeout: 30000 });
    await delay(5000);

    // Screenshot
    await page.screenshot({
      path: `/tmp/mi-lara-detail-${active.cells[1]}.png`,
      fullPage: true
    });

    // Get ALL text
    const text = await page.evaluate(() => document.body.innerText);

    // Check for email
    const emails = text.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    console.log(`  Emails: ${emails ? emails.join(', ') : 'NONE'}`);

    // Parse all visible data
    const lines = text.split('\n').filter(l => l.trim().length > 0);
    const dataLines = lines.filter(l => {
      return l.includes(':') ||
             l.includes('Street') || l.includes('Address') || l.includes('City') ||
             l.includes('Email') || l.includes('Phone') || l.includes('email');
    });

    console.log('  Data fields:');
    for (const l of dataLines) {
      if (l.length < 200) console.log(`    ${l}`);
    }

    // Also check HTML source for hidden email
    const html = await page.content();
    const emailInSrc = html.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
    const uniqueEmails = [...new Set(emailInSrc || [])].filter(e => !e.includes('accela') && !e.includes('michigan.gov'));
    if (uniqueEmails.length > 0) {
      console.log(`  Emails in HTML source: ${uniqueEmails.join(', ')}`);
    }

    detailResults.push({
      name: `${active.cells[2]} ${active.cells[4]}`,
      licNum: active.cells[1],
      emailsInText: emails || [],
      emailsInHTML: uniqueEmails,
      fullText: text,
    });
  }

  // Now also scrape the full dataset for Dentist and other professions
  // (without email - just basic data)
  console.log('\n\n=== Full scrape of Dentist last name A-Z ===');

  const allDentists = [];
  for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
    process.stdout.write(`${letter}: `);

    try {
      await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
      await delay(1500);
      await page.select('select[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$ddlLicenseType"]', 'Dentist');
      await delay(300);
      const input = await page.$('input[name="ctl00$PlaceHolderMain$refLicenseeSearchForm$txtLastName"]');
      await input.type(letter);
      await delay(200);
      await page.click('#ctl00_PlaceHolderMain_btnNewSearch');
      await delay(6000);

      const results = await getAllData();
      allDentists.push(...results);
      const active = results.filter(r => r.cells[7].toLowerCase() === 'active' || r.cells[7].toLowerCase() === 'renewed');
      console.log(`${results.length} total, ${active.length} active (running total: ${allDentists.length})`);
    } catch (err) {
      console.log(`error: ${err.message}`);
    }
  }

  // Deduplicate by license number
  const seen = new Set();
  const uniqueDentists = allDentists.filter(r => {
    const key = r.cells[1];
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const activeDentists = uniqueDentists.filter(r => {
    const status = r.cells[7].toLowerCase();
    return status === 'active' || status === 'renewed';
  });

  console.log(`\nTotal unique dentists: ${uniqueDentists.length}`);
  console.log(`Active dentists: ${activeDentists.length}`);

  // Write CSV
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const csvPath = path.join(OUTPUT_DIR, `MI-LARA-Dentist-ALL_${timestamp}.csv`);
  const headers = ['name', 'license_number', 'license_type', 'organization', 'dba_trade_name', 'status', 'expiration_date'];
  const csvLines = [headers.map(h => `"${h}"`).join(',')];
  for (const r of uniqueDentists) {
    const name = [r.cells[2], r.cells[3], r.cells[4]].filter(Boolean).join(' ').trim();
    csvLines.push([name, r.cells[1], r.cells[0], r.cells[5], r.cells[6], r.cells[7], r.cells[8]]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(csvPath, csvLines.join('\n'), 'utf8');
  console.log(`Written: ${csvPath}`);

  // Active only
  const activePath = path.join(OUTPUT_DIR, `MI-LARA-Dentist-ACTIVE_${timestamp}.csv`);
  const activeLines = [headers.map(h => `"${h}"`).join(',')];
  for (const r of activeDentists) {
    const name = [r.cells[2], r.cells[3], r.cells[4]].filter(Boolean).join(' ').trim();
    activeLines.push([name, r.cells[1], r.cells[0], r.cells[5], r.cells[6], r.cells[7], r.cells[8]]
      .map(v => `"${(v || '').replace(/"/g, '""')}"`)
      .join(','));
  }
  fs.writeFileSync(activePath, activeLines.join('\n'), 'utf8');
  console.log(`Written: ${activePath}`);

  await delay(3000);
  await browser.close();
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
