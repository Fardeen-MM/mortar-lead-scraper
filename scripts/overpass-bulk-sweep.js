#!/usr/bin/env node
/**
 * Bulk Overpass sweep: iterate many cities × many business categories.
 * FREE — costs nothing. Typical yield: 30-100 businesses per city+category combo.
 */
const fs = require('fs');
const path = require('path');
const { searchCity, TAG_QUERIES } = require('../lib/osm-overpass');

const OUT_DIR = path.join(__dirname, '..', 'output');
const STATE_FILE = path.join(OUT_DIR, '.overpass-bulk-state.json');

// Parse args: --cities=file --categories=a,b,c --out=filename
const args = Object.fromEntries(
  process.argv.slice(2).map(a => a.startsWith('--') ? a.slice(2).split('=') : ['_', a])
);

const CITIES_FILE = args.cities || path.join(__dirname, '..', 'data', 'bulk-cities.txt');
const CATEGORIES = (args.categories || 'lawyer,dentist,realestate,accountant,insurance,chiropractor,optometrist').split(',');
const OUTPUT = path.join(OUT_DIR, args.out || 'overpass-bulk-businesses.csv');

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function getCities() {
  if (!fs.existsSync(CITIES_FILE)) {
    // Default to top US cities
    return [
      'New York, US', 'Los Angeles, US', 'Chicago, US', 'Houston, US', 'Phoenix, US',
      'Philadelphia, US', 'San Antonio, US', 'San Diego, US', 'Dallas, US', 'San Jose, US',
      'Austin, US', 'Jacksonville, US', 'Fort Worth, US', 'Columbus, US', 'Indianapolis, US',
      'Charlotte, US', 'San Francisco, US', 'Seattle, US', 'Denver, US', 'Washington, US',
      'Boston, US', 'El Paso, US', 'Nashville, US', 'Detroit, US', 'Oklahoma City, US',
      'Portland, US', 'Las Vegas, US', 'Memphis, US', 'Louisville, US', 'Baltimore, US',
      'Milwaukee, US', 'Albuquerque, US', 'Tucson, US', 'Fresno, US', 'Sacramento, US',
      'Kansas City, US', 'Mesa, US', 'Atlanta, US', 'Omaha, US', 'Colorado Springs, US',
      'Raleigh, US', 'Miami, US', 'Long Beach, US', 'Virginia Beach, US', 'Oakland, US',
      'Minneapolis, US', 'Tulsa, US', 'Tampa, US', 'Arlington, US', 'New Orleans, US',
      // Canada
      'Toronto, Canada', 'Montreal, Canada', 'Vancouver, Canada', 'Calgary, Canada',
      'Edmonton, Canada', 'Ottawa, Canada', 'Winnipeg, Canada', 'Hamilton, Canada',
      // UK
      'London, UK', 'Birmingham, UK', 'Manchester, UK', 'Leeds, UK', 'Glasgow, UK',
      // Australia
      'Sydney, Australia', 'Melbourne, Australia', 'Brisbane, Australia', 'Perth, Australia',
    ];
  }
  return fs.readFileSync(CITIES_FILE, 'utf-8')
    .split('\n').map(s => s.trim()).filter(Boolean);
}

async function main() {
  const cities = getCities();
  console.log(`=== Overpass Bulk Sweep ===`);
  console.log(`Cities: ${cities.length}, Categories: ${CATEGORIES.join(',')}`);
  console.log(`Output: ${OUTPUT}\n`);

  // Load state
  let state = { done: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const done = new Set(state.done);

  const cols = ['name', 'phone', 'email', 'website', 'address', 'city_searched', 'country_searched',
                'osm_city', 'osm_state', 'osm_postcode', 'osm_country', 'lat', 'lon',
                'category', 'source', 'osm_id'];
  if (!fs.existsSync(OUTPUT)) fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  let totalFound = 0, totalWithPhone = 0, totalWithEmail = 0, totalWithWebsite = 0;
  const start = Date.now();

  let cityIdx = 0;
  for (const cityStr of cities) {
    cityIdx++;
    const parts = cityStr.split(',').map(s => s.trim());
    const city = parts[0];
    const country = parts[1] || '';

    for (const category of CATEGORIES) {
      const key = `${city}|${country}|${category}`;
      if (done.has(key)) continue;

      try {
        const r = await searchCity(category, city, country);
        let added = 0;
        for (const b of r.results) {
          if (!b.name) continue;
          const outRow = {
            name: b.name,
            phone: b.phone || '',
            email: b.email || '',
            website: b.website || '',
            address: [b.addr_housenumber, b.addr_street].filter(Boolean).join(' '),
            city_searched: city,
            country_searched: country,
            osm_city: b.addr_city || '',
            osm_state: b.addr_state || '',
            osm_postcode: b.addr_postcode || '',
            osm_country: b.addr_country || '',
            lat: b.lat || '',
            lon: b.lon || '',
            category,
            source: 'osm_overpass',
            osm_id: b.osm_id,
          };
          fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
          if (b.phone) totalWithPhone++;
          if (b.email) totalWithEmail++;
          if (b.website) totalWithWebsite++;
          totalFound++;
          added++;
        }
        done.add(key);
        state.done = [...done];
        fs.writeFileSync(STATE_FILE, JSON.stringify(state));
        const elapsed = (Date.now() - start) / 1000;
        console.log(`  [${cityIdx}/${cities.length}] ${city} | ${category}: +${added} (total: ${totalFound}, emails: ${totalWithEmail}, sites: ${totalWithWebsite}) — ${elapsed.toFixed(0)}s`);
      } catch (err) {
        console.error(`  [${cityIdx}/${cities.length}] ${city} | ${category}: ERROR ${err.message}`);
      }
      // Rate limit: Overpass allows ~10K queries/day, we pace 2-3 per second
      await new Promise(r => setTimeout(r, 500));
    }
  }

  console.log(`\n=== DONE — ${totalFound} businesses, ${totalWithPhone} phones, ${totalWithEmail} emails, ${totalWithWebsite} websites ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
