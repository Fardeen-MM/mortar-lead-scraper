/**
 * OSM Overpass API — FREE business directory lookup by category + bounding box.
 * Perfect for "find all X in Y city" queries.
 */
const https = require('https');

const ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
];

// OSM tags for business categories
const TAG_QUERIES = {
  // Legal
  lawyer: ['node["office"="lawyer"]', 'way["office"="lawyer"]'],
  // Medical
  dentist: ['node["amenity"="dentist"]', 'way["amenity"="dentist"]', 'node["healthcare"="dentist"]'],
  doctor: ['node["amenity"="doctors"]', 'node["healthcare"="doctor"]'],
  clinic: ['node["amenity"="clinic"]', 'node["healthcare"="clinic"]'],
  pharmacy: ['node["amenity"="pharmacy"]', 'way["amenity"="pharmacy"]'],
  physiotherapist: ['node["healthcare"="physiotherapist"]'],
  chiropractor: ['node["healthcare"="chiropractor"]'],
  optometrist: ['node["healthcare"="optometrist"]'],
  // Real estate
  realestate: ['node["office"="estate_agent"]', 'way["office"="estate_agent"]'],
  // Financial
  accountant: ['node["office"="accountant"]', 'way["office"="accountant"]'],
  insurance: ['node["office"="insurance"]', 'way["office"="insurance"]'],
  financial: ['node["office"="financial"]', 'node["office"="financial_advisor"]'],
  bank: ['node["amenity"="bank"]'],
  // Beauty / fitness
  salon: ['node["shop"="beauty"]', 'node["shop"="hairdresser"]'],
  hairdresser: ['node["shop"="hairdresser"]'],
  gym: ['node["leisure"="fitness_centre"]', 'node["amenity"="gym"]'],
  yoga: ['node["sport"="yoga"]', 'node["amenity"="gym"]["sport"="yoga"]'],
  // Professional services
  consulting: ['node["office"="consulting"]', 'way["office"="consulting"]'],
  architect: ['node["office"="architect"]'],
  engineer: ['node["office"="engineer"]'],
  advertising: ['node["office"="advertising_agency"]'],
  marketing: ['node["office"="marketing"]'],
  itcompany: ['node["office"="it"]'],
  // Trades / home services
  electrician: ['node["craft"="electrician"]'],
  plumber: ['node["craft"="plumber"]'],
  hvac: ['node["craft"="hvac"]'],
  roofer: ['node["craft"="roofer"]'],
  carpenter: ['node["craft"="carpenter"]'],
  painter: ['node["craft"="painter"]'],
  // Auto
  carrepair: ['node["shop"="car_repair"]', 'node["amenity"="car_repair"]'],
  carwash: ['node["amenity"="car_wash"]'],
  // Food
  restaurant: ['node["amenity"="restaurant"]'],
  cafe: ['node["amenity"="cafe"]'],
  // Education
  school: ['node["amenity"="school"]'],
  kindergarten: ['node["amenity"="kindergarten"]'],
  driving_school: ['node["amenity"="driving_school"]'],
  music_school: ['node["amenity"="music_school"]'],
  language_school: ['node["amenity"="language_school"]'],
  // Veterinary
  veterinary: ['node["amenity"="veterinary"]'],
  // Other
  solar: ['node["shop"="solar"]', 'node["craft"="solar"]'],
  photographer: ['node["craft"="photographer"]', 'node["shop"="photo"]'],
  funeral: ['node["office"="funeral_director"]'],
  notary: ['node["office"="notary"]'],
  tax_advisor: ['node["office"="tax_advisor"]'],
};

function httpPost(url, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      method: 'POST',
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mortar-Lead-Scraper/1.0',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 35000,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body);
    req.end();
  });
}

/**
 * Search OSM Overpass for businesses in a bounding box.
 * @param {string} category - e.g. 'lawyer', 'dentist', 'realestate'
 * @param {number[]} bbox - [south, west, north, east]
 * @returns {Promise<Array<Object>>}
 */
async function searchOverpass(category, bbox) {
  const queries = TAG_QUERIES[category.toLowerCase()];
  if (!queries) throw new Error(`Unknown category: ${category}. Available: ${Object.keys(TAG_QUERIES).join(', ')}`);

  const bboxStr = bbox.join(',');
  const body = queries.map(q => `${q}(${bboxStr});`).join('');
  const overpassQuery = `[out:json][timeout:30];(${body});out center tags;`;
  const postBody = 'data=' + encodeURIComponent(overpassQuery);

  let lastErr;
  for (const ep of ENDPOINTS) {
    try {
      const raw = await httpPost(ep, postBody);
      const data = JSON.parse(raw);
      const results = [];
      for (const el of data.elements || []) {
        const tags = el.tags || {};
        if (!tags.name) continue;
        const lat = el.lat || (el.center && el.center.lat);
        const lon = el.lon || (el.center && el.center.lon);
        results.push({
          osm_id: `${el.type}/${el.id}`,
          name: tags.name,
          phone: tags.phone || tags['contact:phone'] || '',
          email: tags.email || tags['contact:email'] || '',
          website: tags.website || tags['contact:website'] || tags.url || '',
          addr_street: tags['addr:street'] || '',
          addr_housenumber: tags['addr:housenumber'] || '',
          addr_city: tags['addr:city'] || '',
          addr_state: tags['addr:state'] || '',
          addr_postcode: tags['addr:postcode'] || '',
          addr_country: tags['addr:country'] || '',
          lat, lon,
          opening_hours: tags.opening_hours || '',
          raw_tags: tags,
        });
      }
      return results;
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr || new Error('All Overpass endpoints failed');
}

// City → bounding box lookup (via Nominatim geocoding)
async function geocodeCity(city, country) {
  const query = encodeURIComponent(`${city}${country ? ', ' + country : ''}`);
  return new Promise((resolve, reject) => {
    https.get(`https://nominatim.openstreetmap.org/search?q=${query}&format=json&limit=1`, {
      headers: { 'User-Agent': 'Mortar-Lead-Scraper/1.0' },
      timeout: 15000,
    }, res => {
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
          if (!data || !data.length) return reject(new Error(`No geocoding result for ${city}`));
          const box = data[0].boundingbox.map(parseFloat);
          resolve({
            bbox: [box[0], box[2], box[1], box[3]], // [south, west, north, east]
            lat: parseFloat(data[0].lat),
            lon: parseFloat(data[0].lon),
            display_name: data[0].display_name,
          });
        } catch (err) { reject(err); }
      });
    }).on('error', reject);
  });
}

/**
 * Convenience: "find X in Y city"
 */
async function searchCity(category, city, country) {
  const geo = await geocodeCity(city, country);
  const results = await searchOverpass(category, geo.bbox);
  return {
    city,
    country: country || '',
    category,
    bbox: geo.bbox,
    count: results.length,
    results,
  };
}

module.exports = { searchOverpass, searchCity, geocodeCity, TAG_QUERIES };

// CLI usage: node lib/osm-overpass.js lawyer Miami US
if (require.main === module) {
  const [category, ...cityParts] = process.argv.slice(2);
  if (!category || !cityParts.length) {
    console.log('Usage: node lib/osm-overpass.js <category> <city> [country]');
    console.log('Categories:', Object.keys(TAG_QUERIES).join(', '));
    process.exit(1);
  }
  const country = cityParts.length > 1 ? cityParts.pop() : '';
  const city = cityParts.join(' ');
  searchCity(category, city, country).then(r => {
    console.log(`Found ${r.count} ${r.category} in ${r.city}${r.country ? ', ' + r.country : ''}`);
    console.log(JSON.stringify(r.results.slice(0, 5), null, 2));
  }).catch(err => { console.error('ERROR:', err.message); process.exit(1); });
}
