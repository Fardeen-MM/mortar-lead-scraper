#!/usr/bin/env node
/**
 * Best Lawyers US Immigration Law — direct HTML scraper.
 *
 * Source: https://www.bestlawyers.com/united-states/immigration-law?page=N
 * Pagination: 1-20+ pages, `?page=N` param
 * Per-page: ~10-15 lawyer cards with mailto links
 *
 * Expected: 150-400 US immigration attorney emails
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'best-lawyers-immigration-us.csv');
const MAX_PAGES = parseInt(process.argv[2]) || 25;
const DELAY_MS = 4000;

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 20000,
    }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function parsePage(html) {
  const $ = cheerio.load(html);
  const results = [];

  $('.card-lawyer').each((_, el) => {
    const $el = $(el);

    // Extract mailto email
    const mailto = $el.find('a[href^="mailto:"]').first().attr('href') || '';
    const email = mailto.replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();

    // Extract phone
    const phone = $el.find('a[href^="tel:"]').first().attr('href') || '';
    const phoneClean = phone.replace(/^tel:/i, '').trim();

    // Name + firm from card
    const fullText = $el.text().trim();
    // Name usually in h3 or similar prominent heading
    const name = $el.find('h3, h4').first().text().trim()
      || $el.find('.lawyer-name, [class*="lawyer-name"]').first().text().trim();

    // Firm usually below name
    const firm = $el.find('[class*="firm"], [class*="law-firm"]').first().text().trim()
      || $el.find('h5, .subtitle').first().text().trim();

    // Location
    const location = $el.find('[class*="location"], [class*="city"]').first().text().trim();

    // Profile link
    const profileLink = $el.find('a[href*="/lawyers/"]').first().attr('href') || '';

    if (email || name) {
      const parts = name.split(/\s+/).filter(Boolean);
      results.push({
        first_name: parts[0] || '',
        last_name: parts.slice(1).join(' ') || '',
        email,
        phone: phoneClean,
        firm_name: firm,
        city: location,
        profile_url: profileLink ? (profileLink.startsWith('http') ? profileLink : 'https://www.bestlawyers.com' + profileLink) : '',
        country: 'US',
        niche: 'immigration attorney',
        source: 'best_lawyers',
      });
    }
  });

  return results;
}

async function main() {
  console.log('=== Best Lawyers US Immigration Scraper ===');

  const allRows = [];
  const seenEmails = new Set();
  let emptyPages = 0;

  for (let page = 1; page <= MAX_PAGES; page++) {
    const url = `https://www.bestlawyers.com/united-states/immigration-law?page=${page}`;
    console.log(`\n[page ${page}/${MAX_PAGES}] ${url}`);

    let html;
    try { html = await httpGet(url); }
    catch (err) {
      console.error(`  ERROR: ${err.message}`);
      if (err.message.includes('HTTP 404')) break;
      continue;
    }

    const rows = parsePage(html);
    let newRows = 0;
    for (const r of rows) {
      if (r.email && seenEmails.has(r.email)) continue;
      if (r.email) seenEmails.add(r.email);
      allRows.push(r);
      newRows++;
    }

    console.log(`  ${rows.length} cards, ${newRows} new (running total: ${allRows.length})`);

    if (rows.length === 0) {
      emptyPages++;
      if (emptyPages >= 2) {
        console.log('  2 consecutive empty pages — stopping');
        break;
      }
    } else {
      emptyPages = 0;
    }

    // Polite delay
    await new Promise(r => setTimeout(r, DELAY_MS));
  }

  // Write CSV
  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'city',
                'profile_url', 'country', 'niche', 'source'];
  const out = [cols.join(',')];
  for (const r of allRows) out.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUTPUT, out.join('\n') + '\n');

  const withEmail = allRows.filter(r => r.email).length;
  const withPhone = allRows.filter(r => r.phone).length;

  console.log('\n=== RESULTS ===');
  console.log(`Rows:         ${allRows.length}`);
  console.log(`With email:   ${withEmail}`);
  console.log(`With phone:   ${withPhone}`);
  console.log(`Output: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
