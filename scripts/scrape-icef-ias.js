#!/usr/bin/env node
/**
 * ICEF IAS Directory Scraper
 *
 * Source: https://www.icef.com/ias-listing/ (public directory of ICEF-accredited education/migration agencies)
 * API:    https://www.icef.com/wp-content/plugins/icef-api-connector/endpoints/eias.php?t={timestamp}
 * Format: JSON with 2,216 records; ALL fields base64-encoded (trivial decode)
 *
 * Yields: ~2,216 global study-abroad / international-education / migration consultancies.
 * Country breakdown (top 10): India 314, UK 150, Australia 131, Nepal 111, Nigeria 99,
 *   Pakistan 95, Brazil 91, Canada 85, Turkey 83, Spain 65.
 *
 * Each agency has: name, city, country, website, description, facebook, iasCertNum.
 * NO EMAIL in API — but websites are publicly listed, so email-finder can crawl them.
 * This is the single highest-value immigration-adjacent source we haven't scraped:
 * every agency helps people move abroad for study → perfect webinar funnel TAM.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'icef-ias-global-agents.csv');

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, */*',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 60000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function decodeBase64Field(v) {
  if (!v || typeof v !== 'string') return '';
  try {
    return Buffer.from(v, 'base64').toString('utf-8');
  } catch {
    return v;
  }
}

function decodeRecord(rec) {
  const out = {};
  for (const [k, v] of Object.entries(rec)) {
    if (typeof v === 'string' && v) {
      // Try base64 decode; if result is non-printable garbage, keep raw
      try {
        const decoded = Buffer.from(v, 'base64').toString('utf-8');
        // Validate: has to be mostly printable
        const printable = decoded.split('').filter(c => {
          const code = c.charCodeAt(0);
          return (code >= 32 && code < 127) || code === 10 || code === 9 || code >= 160;
        }).length;
        if (printable / decoded.length > 0.8) {
          out[k] = decoded;
        } else {
          out[k] = v;
        }
      } catch {
        out[k] = v;
      }
    } else {
      out[k] = v;
    }
  }
  return out;
}

function normalizeDomain(website) {
  if (!website) return '';
  let w = website.trim().toLowerCase();
  w = w.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
  return w;
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

async function main() {
  console.log('=== ICEF IAS Global Education/Migration Agents ===');

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const timestamp = Date.now();
  const url = `https://www.icef.com/wp-content/plugins/icef-api-connector/endpoints/eias.php?t=${timestamp}`;

  console.log('Fetching ICEF API...');
  const body = await httpGet(url);
  console.log(`  Received ${(body.length / 1024).toFixed(0)} KB`);

  let data;
  try {
    data = JSON.parse(body);
  } catch (err) {
    console.error('JSON parse failed:', err.message);
    console.error('First 500 chars:', body.slice(0, 500));
    process.exit(1);
  }

  const records = data.records || [];
  console.log(`  Records: ${records.length}`);

  // Decode and flatten
  const rows = [];
  const countries = {};
  for (const rec of records) {
    const d = decodeRecord(rec);

    const website = d.website || '';
    const domain = normalizeDomain(website);

    const country = d.mailingCountry || '';
    countries[country] = (countries[country] || 0) + 1;

    rows.push({
      firm_name: d.name || '',
      city: d.mailingCity || '',
      country,
      country_iso2: d.countryIso2 || '',
      website: website && !/^https?:\/\//.test(website) ? `https://${website}` : website,
      domain,
      facebook: d.facebook || '',
      twitter: d.twitter || '',
      ias_cert_num: d.iasCertNum || '',
      ias_cert_url: d.iasCertUrl || '',
      ias_status: d.iasStatus || '',
      ias_listing_date: d.iasListingDate || '',
      ias_renewal_date: d.iasRenewalDate || '',
      ias_since_year: d.iasSinceYear || '',
      description: (d.description || '').slice(0, 500).replace(/\n/g, ' '),
      niche: 'study abroad / migration agent',
      source: 'icef_ias',
    });
  }

  // Write CSV
  const cols = ['firm_name', 'city', 'country', 'country_iso2', 'website', 'domain',
                'facebook', 'twitter', 'ias_cert_num', 'ias_cert_url', 'ias_status',
                'ias_listing_date', 'ias_renewal_date', 'ias_since_year',
                'description', 'niche', 'source'];
  const header = cols.join(',');
  const body2 = rows.map(r => cols.map(c => escapeCsv(r[c] || '')).join(',')).join('\n');
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + body2 + '\n');

  console.log(`\n=== RESULTS ===`);
  console.log(`Total agents: ${rows.length}`);
  console.log(`With website:    ${rows.filter(r => r.website).length}`);
  console.log(`With facebook:   ${rows.filter(r => r.facebook).length}`);
  console.log(`Unique domains:  ${new Set(rows.map(r => r.domain).filter(Boolean)).size}`);
  console.log(`Output: ${OUTPUT_FILE}`);

  console.log('\nTop 15 countries:');
  const sorted = Object.entries(countries).sort((a, b) => b[1] - a[1]);
  for (const [c, n] of sorted.slice(0, 15)) {
    console.log(`  ${c}: ${n}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
