#!/usr/bin/env node
/**
 * SREC (Saskatchewan Real Estate Commission) Agent Scraper
 * URL: https://ols.srec.ca/pubinquiry
 * Platform: ASP.NET WebForms with DevExpress callback controls
 * Strategy: Puppeteer-based - iterate A%-Z% last name prefixes, parse person grid
 *           For prefixes with >100 results (grid page size limit), split into
 *           two-letter prefixes (Xa%-Xz%) to avoid pagination issues
 * Estimated: ~2,000 agents
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'srec-real-estate-agents.csv');
const DELAY_MS = 500;
const BASE_URL = 'https://ols.srec.ca/pubinquiry';

const CSV_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source'
];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseName(fullName) {
  // Handle names like "Aaron Q. Alarcon" or "Brittany Marie Smith"
  // Also handles nicknames like "Aleksandar (Alek) Arsenic"
  let clean = fullName.replace(/\([^)]*\)/g, '').trim();
  clean = clean.replace(/\s+/g, ' ');

  const parts = clean.split(' ');
  if (parts.length === 1) {
    return { first_name: parts[0], last_name: '' };
  }

  const lastName = parts[parts.length - 1];
  const firstName = parts.slice(0, -1).join(' ');
  return { first_name: firstName, last_name: lastName };
}

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function extractAgentsFromGrid(page) {
  return page.evaluate(() => {
    const rows = document.querySelectorAll('#cp1_PanelResults_gvPerson tr[class*="dxgvDataRow"]');
    const agents = [];
    rows.forEach(row => {
      const cells = row.querySelectorAll('td');
      // Find cells with dxgv class (actual data cells)
      const dataCells = Array.from(cells).filter(c => c.classList.contains('dxgv'));

      if (dataCells.length >= 3) {
        // Cell 0: Name info (Registrant Name\nFull Name\n\nDesignation\nRole)
        const nameCell = dataCells[0].innerText.trim();
        const nameMatch = nameCell.match(/Registrant Name\n(.+)\n\nDesignation\n(.+)/s);

        let fullName = '';
        let designation = '';
        if (nameMatch) {
          fullName = nameMatch[1].trim();
          designation = nameMatch[2].trim();
        } else {
          fullName = nameCell;
        }

        // Cell 1: Brokerage
        const brokerage = dataCells[1] ? dataCells[1].innerText.trim() : '';

        // Cell 2: Specialties
        const specialties = dataCells[2] ? dataCells[2].innerText.trim() : '';

        if (fullName && fullName !== 'Registrant Name') {
          agents.push({ fullName, designation, brokerage, specialties });
        }
      }
    });

    const pager = document.querySelector('#cp1_PanelResults_gvPerson_DXPagerBottom');
    const pagerText = pager ? pager.innerText.trim() : '';
    const totalMatch = pagerText.match(/\((\d+) items\)/);
    const totalItems = totalMatch ? parseInt(totalMatch[1]) : agents.length;

    return { agents, pagerText, totalItems };
  });
}

async function scrapePrefix(page, prefix) {
  // Clear input and type new search
  await page.evaluate(() => {
    const input = document.querySelector('#cp1_PanelSearch_txtLastName_I');
    if (input) input.value = '';
  });
  await page.type('#cp1_PanelSearch_txtLastName_I', prefix);

  // Click search
  await page.click('#cp1_PanelSearch_cmdLastName');

  // Wait for results panel to appear
  try {
    await page.waitForFunction(() => {
      const panel = document.querySelector('#cp1_PanelResults');
      return panel && panel.style.display !== 'none';
    }, { timeout: 15000 });
  } catch (e) {
    return { agents: [], totalItems: 0 };
  }

  // Wait for grid to finish loading
  await sleep(1500);

  const data = await extractAgentsFromGrid(page);

  // Go back to search form
  try {
    await page.click('#cp1_PanelResults_cmdRequery');
    await sleep(800);
    await page.waitForFunction(() => {
      const panel = document.querySelector('#cp1_PanelSearch');
      return panel && panel.style.display !== 'none';
    }, { timeout: 10000 });
  } catch (e) {
    // Reload page if Search Again button fails
    await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 30000 });
    await page.waitForSelector('#cp1_PanelSearch_txtLastName_I', { timeout: 10000 });
  }

  return data;
}

async function main() {
  console.log('=== SREC Saskatchewan Real Estate Commission Scraper ===\n');

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

  console.log('Loading SREC search page...');
  await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: 30000 });
  await page.waitForSelector('#cp1_PanelSearch_txtLastName_I', { timeout: 10000 });
  console.log('Page loaded\n');

  const allAgents = [];
  const seen = new Set();  // Dedup by full name + brokerage
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

  function addAgents(agents, prefix) {
    let newCount = 0;
    for (const agent of agents) {
      const key = (agent.fullName + '|' + agent.brokerage).toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        allAgents.push(agent);
        newCount++;
      }
    }
    return newCount;
  }

  for (const letter of alphabet) {
    const prefix = letter + '%';
    process.stdout.write(`Searching "${prefix}"...`);

    const data = await scrapePrefix(page, prefix);
    await sleep(DELAY_MS);

    if (data.totalItems > 100) {
      // Grid page size is 100; need to split into two-letter prefixes
      console.log(` ${data.totalItems} total items (>${100}), splitting into sub-prefixes...`);

      for (const letter2 of alphabet) {
        const subPrefix = letter + letter2 + '%';
        const subData = await scrapePrefix(page, subPrefix);
        const newCount = addAgents(subData.agents, subPrefix);

        if (subData.agents.length > 0) {
          console.log(`  "${subPrefix}": ${subData.agents.length} found, ${newCount} new (total: ${allAgents.length})`);
        }

        if (subData.totalItems > 100) {
          // Extremely rare: need three-letter prefix
          console.log(`  WARNING: "${subPrefix}" has ${subData.totalItems} items, may be truncated`);
        }

        await sleep(DELAY_MS);
      }
    } else {
      const newCount = addAgents(data.agents, prefix);
      console.log(` ${data.agents.length} found, ${newCount} new (total: ${allAgents.length})`);
    }
  }

  console.log(`\nTotal unique agents: ${allAgents.length}`);

  // Write CSV
  const csvLines = [CSV_HEADERS.join(',')];

  for (const agent of allAgents) {
    const { first_name, last_name } = parseName(agent.fullName);
    const title = agent.designation || '';
    const firmName = agent.brokerage || '';

    const row = [
      escapeCsv(first_name),
      escapeCsv(last_name),
      escapeCsv(firmName),
      escapeCsv(title),
      '',  // email
      '',  // phone
      '',  // website
      '',  // domain
      '',  // city
      'Saskatchewan',
      'CA',
      'real estate agent',
      'SREC'
    ];
    csvLines.push(row.join(','));
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf8');
  console.log(`\nCSV written to ${OUTPUT_FILE}`);
  console.log(`Total rows: ${allAgents.length}`);

  // Stats
  const designations = {};
  const brokerages = {};
  for (const a of allAgents) {
    designations[a.designation] = (designations[a.designation] || 0) + 1;
    if (a.brokerage) brokerages[a.brokerage] = (brokerages[a.brokerage] || 0) + 1;
  }
  console.log('\nDesignations:');
  Object.entries(designations).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(`  ${k}: ${v}`));
  console.log(`\nTop 15 Brokerages:`);
  Object.entries(brokerages).sort((a, b) => b[1] - a[1]).slice(0, 15).forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  await browser.close();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
