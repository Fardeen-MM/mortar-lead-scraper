#!/usr/bin/env node
/**
 * OpenStreetMap Driving Schools + Language Schools Scraper
 * Uses Overpass API to extract 27,911 driving schools + 10,052 language schools worldwide
 */
const https = require('https');
const http = require('http');
const fs = require('fs');

const OUTPUT_DIR = __dirname + '/../output';

function overpassQuery(query) {
  return new Promise((resolve, reject) => {
    const postData = 'data=' + encodeURIComponent(query);
    const req = https.request({
      hostname: 'overpass-api.de',
      path: '/api/interpreter',
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData) },
      timeout: 600000,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch(e) { reject(new Error('JSON parse error: ' + e.message)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postData);
    req.end();
  });
}

function esc(v) {
  const s = (v ?? '').toString().trim().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

function getTag(tags, ...keys) {
  for (const k of keys) if (tags[k]) return tags[k].trim();
  return '';
}

(async () => {
  console.log('Querying Overpass API for driving_school + language_school...');
  console.log('This may take 2-5 minutes for ~38K elements.\n');

  const query = `[out:json][timeout:600];
(
  nwr["amenity"="driving_school"];
  nwr["amenity"="language_school"];
);
out center tags;`;

  const data = await overpassQuery(query);
  console.log(`Got ${data.elements.length} total elements\n`);

  const driving = [];
  const language = [];

  for (const el of data.elements) {
    const t = el.tags || {};
    const amenity = t.amenity;
    const name = getTag(t, 'name', 'name:en');
    if (!name) continue;

    const record = {
      email: getTag(t, 'email', 'contact:email'),
      company: name,
      phone: getTag(t, 'phone', 'contact:phone'),
      city: getTag(t, 'addr:city', 'addr:suburb'),
      state: getTag(t, 'addr:state', 'addr:province'),
      country: getTag(t, 'addr:country', 'is_in:country'),
      website: getTag(t, 'website', 'contact:website', 'url'),
      lat: el.center?.lat || el.lat || '',
      lon: el.center?.lon || el.lon || '',
    };

    if (amenity === 'driving_school') driving.push(record);
    else if (amenity === 'language_school') language.push(record);
  }

  const header = 'email,first_name,last_name,company,phone,city,state,country,source,website,lat,lon';

  // Write driving schools
  const dRows = driving.map(r => [
    esc(r.email), '', '', esc(r.company), esc(r.phone), esc(r.city), esc(r.state),
    esc(r.country), 'osm_driving_school', esc(r.website), r.lat, r.lon
  ].join(','));
  fs.writeFileSync(`${OUTPUT_DIR}/osm-driving-schools.csv`, header + '\n' + dRows.join('\n'));

  // Write language schools
  const lRows = language.map(r => [
    esc(r.email), '', '', esc(r.company), esc(r.phone), esc(r.city), esc(r.state),
    esc(r.country), 'osm_language_school', esc(r.website), r.lat, r.lon
  ].join(','));
  fs.writeFileSync(`${OUTPUT_DIR}/osm-language-schools.csv`, header + '\n' + lRows.join('\n'));

  const dEmails = driving.filter(r => r.email).length;
  const dPhones = driving.filter(r => r.phone).length;
  const dWebs = driving.filter(r => r.website).length;
  const lEmails = language.filter(r => r.email).length;
  const lPhones = language.filter(r => r.phone).length;
  const lWebs = language.filter(r => r.website).length;

  console.log('=== DRIVING SCHOOLS ===');
  console.log(`  Total: ${driving.length}`);
  console.log(`  With email: ${dEmails} (${(dEmails/driving.length*100).toFixed(1)}%)`);
  console.log(`  With phone: ${dPhones} (${(dPhones/driving.length*100).toFixed(1)}%)`);
  console.log(`  With website: ${dWebs} (${(dWebs/driving.length*100).toFixed(1)}%)`);
  console.log(`  → ${OUTPUT_DIR}/osm-driving-schools.csv`);

  console.log('\n=== LANGUAGE SCHOOLS ===');
  console.log(`  Total: ${language.length}`);
  console.log(`  With email: ${lEmails} (${(lEmails/language.length*100).toFixed(1)}%)`);
  console.log(`  With phone: ${lPhones} (${(lPhones/language.length*100).toFixed(1)}%)`);
  console.log(`  With website: ${lWebs} (${(lWebs/language.length*100).toFixed(1)}%)`);
  console.log(`  → ${OUTPUT_DIR}/osm-language-schools.csv`);
})();
