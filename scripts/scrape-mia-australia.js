#!/usr/bin/env node
/**
 * MIA Australia (Migration Institute of Australia) Public Member Directory.
 *
 * Source: https://mia.associationonline.com.au/find-a-Member/search/
 * Platform: Sitebuilder / Membership3 CMS — form-driven, JS-rendered results.
 * Expected: ~3,500 registered migration agents (RMAs) + eligible legal practitioners.
 *
 * Members opt-in to public directory. Emails, phone, address are published
 * publicly (note: "opt-in to be included in the public directory" disclaimer).
 *
 * Method: Puppeteer — submit empty form with large radius to retrieve all results,
 * then paginate/scroll, then parse member cards.
 *
 * Usage:
 *   node scripts/scrape-mia-australia.js
 *   node scripts/scrape-mia-australia.js --test (first page only)
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const s = a.replace(/^--/, '');
  if (s.includes('=')) { const [k, v] = s.split('='); args[k] = v; }
  else args[s] = true;
}

const TEST_MODE = !!args.test;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'mia-australia-migration-agents.csv');
const SEARCH_URL = 'https://mia.associationonline.com.au/find-a-Member/search/';

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(rows) {
  const cols = [
    'first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
    'domain', 'city', 'state', 'country', 'niche', 'source', 'profile_url',
    'accreditation', 'address',
  ];
  const header = cols.join(',');
  const body = rows.map(r => cols.map(c => escapeCsv(r[c] || '')).join(',')).join('\n');
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + body + '\n');
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'https://' + u;
    return new URL(u).hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function splitName(fullName) {
  const parts = (fullName || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts.slice(1).join(' ') };
}

async function main() {
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  console.log('=== MIA Australia Migration Agents Scraper ===');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await page.setViewport({ width: 1400, height: 900 });

    console.log('Loading search page...');
    await page.goto(SEARCH_URL, { waitUntil: 'networkidle2', timeout: 60000 });

    // Submit empty form with largest radius
    console.log('Submitting empty search to retrieve all members...');

    // Set radius to max
    await page.evaluate(() => {
      // Set radius to 1000km
      const radiusSel = document.querySelector('select[name="LocationRadius"], input[name="LocationRadius"]');
      if (radiusSel) {
        if (radiusSel.tagName === 'SELECT') {
          // Find the max option
          const options = Array.from(radiusSel.options).map(o => parseInt(o.value) || 0);
          const max = Math.max(...options);
          for (const o of radiusSel.options) {
            if (parseInt(o.value) === max) { radiusSel.value = o.value; break; }
          }
        } else {
          radiusSel.value = '1000';
        }
      }
    });

    // Submit form
    const formSubmit = await page.evaluate(() => {
      const forms = Array.from(document.querySelectorAll('form')).filter(f => {
        const names = Array.from(f.querySelectorAll('input')).map(i => i.name);
        return names.includes('SearchLocation') || names.includes('LocationRadius');
      });
      if (forms.length === 0) return false;
      forms[0].submit();
      return true;
    });

    if (!formSubmit) {
      console.error('Could not find search form. MIA may have changed structure.');
      process.exit(1);
    }

    // Wait for results
    console.log('Waiting for results...');
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 60000 }).catch(() => {});

    // Give JS time to render
    await new Promise(r => setTimeout(r, 3000));

    // Extract all members
    console.log('Extracting member data from results page...');
    const results = await page.evaluate(() => {
      const out = [];
      // Try various card selectors — MIA uses Sitebuilder CMS
      const cardSelectors = [
        '.member-card', '.memberCard', '.directory-member', '.member-listing',
        '.search-result', '.result-item', '.w3-card', '.member',
        'div.row.w3-card-4', 'div[class*="member"]',
      ];

      let cards = [];
      for (const sel of cardSelectors) {
        const found = document.querySelectorAll(sel);
        if (found.length > 5) { // Need at least 5 for it to be the right selector
          cards = Array.from(found);
          break;
        }
      }

      // Fallback: any div containing a mailto link
      if (cards.length === 0) {
        const mailtos = document.querySelectorAll('a[href^="mailto:"]');
        const parents = new Set();
        for (const m of mailtos) {
          // Walk up to find a card-like parent
          let p = m.parentElement;
          for (let i = 0; i < 6 && p; i++) {
            const cls = (p.className || '').toString().toLowerCase();
            if (cls.includes('row') || cls.includes('card') || cls.includes('member') || cls.includes('result')) {
              parents.add(p);
              break;
            }
            p = p.parentElement;
          }
        }
        cards = [...parents];
      }

      for (const card of cards) {
        const text = card.innerText || '';
        // Extract email
        const mailtoEl = card.querySelector('a[href^="mailto:"]');
        const email = mailtoEl ? mailtoEl.href.replace(/^mailto:/i, '').split('?')[0].trim() : '';

        // Extract website
        const linkEls = Array.from(card.querySelectorAll('a[href^="http"]'));
        const websiteEl = linkEls.find(a => !a.href.includes('mia.associationonline') && !a.href.includes('mailto:'));
        const website = websiteEl ? websiteEl.href : '';

        // Extract phone (pattern: +61, 0, or ##)
        const phoneMatch = text.match(/(?:\+?61\s?)?\(?0?[1-9]\d{0,3}\)?[\s.-]?\d{3,4}[\s.-]?\d{3,4}/);
        const phone = phoneMatch ? phoneMatch[0] : '';

        // Extract name (first strong/h3/h4 text)
        const nameEl = card.querySelector('h2, h3, h4, strong, .name, [class*="name"]');
        const name = nameEl ? nameEl.textContent.trim() : '';

        // Extract firm/company (often second heading or paragraph)
        const allH = Array.from(card.querySelectorAll('h2, h3, h4, p'));
        let firm = '';
        for (const h of allH) {
          const t = h.textContent.trim();
          if (t && t !== name && t.length > 3 && t.length < 100 && !t.includes('@')) {
            firm = t;
            break;
          }
        }

        out.push({
          name,
          firm,
          email,
          phone,
          website,
          text: text.slice(0, 500),
        });
      }

      return out;
    });

    console.log(`Found ${results.length} raw member cards.`);

    // Parse into standard format
    const rows = results
      .filter(r => r.email || r.name)
      .map(r => {
        const { first, last } = splitName(r.name);
        return {
          first_name: first,
          last_name: last,
          firm_name: r.firm,
          email: r.email.toLowerCase(),
          phone: r.phone,
          website: r.website,
          domain: extractDomain(r.website),
          city: '',
          state: '',
          country: 'Australia',
          niche: 'migration agent',
          source: 'mia_australia',
          profile_url: SEARCH_URL,
          accreditation: 'MIA Member',
          address: '',
        };
      });

    console.log(`Parsed ${rows.length} rows`);
    console.log(`With email: ${rows.filter(r => r.email).length}`);

    writeCsv(rows);
    console.log(`Output: ${OUTPUT_FILE}`);

    // Sample
    console.log('\nSample (first 5):');
    for (const r of rows.slice(0, 5)) {
      console.log(`  ${r.first_name} ${r.last_name} | ${r.firm_name} | ${r.email} | ${r.phone}`);
    }
  } finally {
    await browser.close();
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
