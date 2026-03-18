#!/usr/bin/env node
/**
 * Run Which? Trusted Traders scrape — fast direct-fetch version.
 * Extracts data from JSON-LD structured data on listing pages.
 * Exports to output/which-traders-uk.csv
 *
 * Usage: node scripts/run-which-traders.js
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const BASE_URL = 'https://trustedtraders.which.co.uk';
const OUTPUT_PATH = path.join(__dirname, '..', 'output', 'which-traders-uk.csv');
const DELAY_MS = 1500; // 1.5s between requests — polite but fast

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

// Trades and cities to scrape
const TRADES = ['plumbers', 'roofers', 'builders', 'electricians', 'painters-and-decorators'];
const CITIES = [
  'london', 'birmingham', 'leeds', 'glasgow',
  'edinburgh-and-lothian', 'bristol', 'sheffield', 'nottingham',
  'liverpool', 'brighton', 'southampton', 'reading', 'plymouth',
  'greater-manchester', 'west-midlands', 'surrey', 'hertfordshire',
  'west-yorkshire', 'south-yorkshire', 'merseyside',
  'leicestershire', 'northamptonshire', 'nottinghamshire',
  'east-sussex', 'west-sussex', 'warwickshire', 'worcestershire',
  'buckinghamshire', 'berkshire', 'fife',
  'north-ayrshire', 'south-ayrshire', 'south-lanarkshire',
  'stirling', 'peak-district', 'ribble-valley',
  'norfolk-broads', 'herefordshire',
];

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'identity',
      },
      timeout: 20000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) redirect = BASE_URL + redirect;
        return resolve(httpGet(redirect));
      }
      let body = '';
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function extractJsonLd(html) {
  const businesses = [];
  const regex = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/g;
  let match;
  while ((match = regex.exec(html)) !== null) {
    try {
      const data = JSON.parse(match[1]);
      if (data && data['@type'] === 'HomeAndConstructionBusiness') {
        businesses.push(data);
      }
    } catch (e) { /* skip malformed */ }
  }
  return businesses;
}

function extractTotal(html) {
  const m = html.match(/<span class="total">(\d+)<\/span>/);
  return m ? parseInt(m[1], 10) : 0;
}

function hasNextPage(html) {
  return html.includes('results-more') && html.includes('See more');
}

function extractPostcodes(html) {
  const postcodes = [];
  // UK postcodes near listing cards
  const re = /<span>([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})<\/span>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    postcodes.push(m[1].toUpperCase());
  }
  return postcodes;
}

function formatPhone(phone) {
  if (!phone) return '';
  const digits = String(phone).trim().replace(/[^\d+]/g, '');
  if (!digits) return '';
  if (digits.startsWith('+44')) return '0' + digits.slice(3);
  if (digits.startsWith('44') && digits.length > 10) return '0' + digits.slice(2);
  if (digits.startsWith('0')) return digits;
  return String(phone).trim();
}

function slugFromUrl(url) {
  if (!url) return '';
  const m = url.match(/\/businesses\/([^/]+)\/?$/);
  return m ? m[1] : '';
}

function escapeCsv(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return '"' + str.replace(/"/g, '""').replace(/[\r\n]+/g, ' ') + '"';
  }
  return str;
}

const COLUMNS = [
  'firm_name', 'phone', 'website', 'email', 'city', 'state',
  'zip', 'practice_area', 'profile_url', 'rating', 'review_count',
  'bar_number', 'bar_status', 'source', 'about',
];

async function scrapeTradCity(trade, city, seen) {
  const leads = [];
  let page = 1;
  const maxPages = 30;

  while (page <= maxPages) {
    const url = page === 1
      ? `${BASE_URL}/${trade}-in-${city}/`
      : `${BASE_URL}/${trade}-in-${city}/${page}/?continue=1&sort_type=distance`;

    let resp;
    try {
      await sleep(DELAY_MS);
      resp = await httpGet(url);
    } catch (err) {
      break;
    }

    if (resp.statusCode === 404 || resp.statusCode === 500) break;
    if (resp.statusCode === 429 || resp.statusCode === 403) {
      console.log(`  Rate limited (${resp.statusCode}) — backing off 30s`);
      await sleep(30000);
      continue;
    }
    if (resp.statusCode !== 200) break;

    const html = resp.body;
    const businesses = extractJsonLd(html);
    if (businesses.length === 0) break;

    const postcodes = extractPostcodes(html);
    const total = page === 1 ? extractTotal(html) : 0;

    if (page === 1 && total > 0) {
      process.stdout.write(`  ${trade} in ${city}: ${total} results `);
    }

    for (let i = 0; i < businesses.length; i++) {
      const biz = businesses[i];
      const slug = slugFromUrl(biz.url);
      if (slug && seen.has(slug)) continue;
      if (slug) seen.add(slug);

      const name = (biz.name || '').trim();
      if (!name) continue;

      let bizCity = '';
      let zip = postcodes[i] || '';
      let address = '';
      if (biz.address) {
        bizCity = (biz.address.addressLocality || '').trim();
        zip = zip || (biz.address.postalCode || '').trim();
        address = (biz.address.streetAddress || '').trim();
      }

      const tradeName = trade.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

      leads.push({
        firm_name: name,
        phone: formatPhone(biz.telephone),
        website: '', // Not on listing pages, would need profile fetch
        email: (biz.email || '').trim().toLowerCase(),
        city: bizCity || city.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        state: 'UK',
        zip: zip,
        practice_area: tradeName,
        profile_url: (biz.url || '').trim(),
        rating: biz.aggregateRating ? String(biz.aggregateRating.ratingValue || '') : '',
        review_count: biz.aggregateRating ? String(biz.aggregateRating.reviewCount || 0) : '0',
        bar_number: slug,
        bar_status: 'Which? Endorsed',
        source: 'which_traders',
        about: (biz.description || '').trim().substring(0, 500),
      });
    }

    if (!hasNextPage(html)) break;
    page++;
  }

  if (leads.length > 0) {
    console.log(`-> ${leads.length} unique`);
  }

  return leads;
}

async function main() {
  console.log('=== Which? Trusted Traders UK Scrape ===');
  console.log(`Trades: ${TRADES.join(', ')}`);
  console.log(`Cities: ${CITIES.length}`);
  console.log(`Delay: ${DELAY_MS}ms between requests`);
  console.log('');

  const allLeads = [];
  const seen = new Set();
  const stats = { byTrade: {} };

  for (const trade of TRADES) {
    console.log(`\n--- ${trade.toUpperCase()} ---`);
    stats.byTrade[trade] = 0;

    for (const city of CITIES) {
      const leads = await scrapeTradCity(trade, city, seen);
      for (const lead of leads) {
        allLeads.push(lead);
        stats.byTrade[trade]++;
      }
    }

    console.log(`  TOTAL ${trade}: ${stats.byTrade[trade]} leads`);
  }

  // Write CSV
  const header = COLUMNS.join(',');
  const rows = allLeads.map(lead => COLUMNS.map(c => escapeCsv(lead[c] || '')).join(','));
  const csv = [header, ...rows].join('\n');

  const outDir = path.dirname(OUTPUT_PATH);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, csv, 'utf8');

  // Summary
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;

  console.log('\n=== SUMMARY ===');
  console.log(`Total unique leads: ${allLeads.length}`);
  console.log(`With email: ${withEmail} (${(100 * withEmail / Math.max(allLeads.length, 1)).toFixed(1)}%)`);
  console.log(`With phone: ${withPhone} (${(100 * withPhone / Math.max(allLeads.length, 1)).toFixed(1)}%)`);
  console.log('');
  for (const [trade, count] of Object.entries(stats.byTrade)) {
    console.log(`  ${trade}: ${count}`);
  }
  console.log(`\nCSV: ${OUTPUT_PATH}`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
