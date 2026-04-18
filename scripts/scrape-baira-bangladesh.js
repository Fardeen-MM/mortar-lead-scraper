#!/usr/bin/env node
/**
 * BAIRA Bangladesh — Agency profiles. Iterate IDs 300-3200.
 * URL: https://baira.org.bd/dir/agency-information/{ID}
 * Sample fields: Licence, Proprietor, Address, Mobile, Email.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const cheerio = require('cheerio');

const OUTPUT = path.join(__dirname, '..', 'output', 'baira-bangladesh-agencies.csv');
const STATE_FILE = OUTPUT.replace(/\.csv$/, '.state.json');
const CONCURRENCY = 10;
const BASE = 'https://baira.org.bd/dir/agency-information';

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' },
      timeout: 15000,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = []; res.on('data', c => chunks.push(c));
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

function parseDetail(html) {
  const $ = cheerio.load(html);
  const fields = {};

  // Table format: Label | : | Value (3 cells per row), OR empty | Label | : | Value (4 cells)
  $('table tr').each((_, tr) => {
    const cells = $(tr).find('td').map((_, c) => $(c).text().trim()).get();
    // Find the "label : value" pattern within the row
    let label = '', value = '';
    for (let i = 0; i < cells.length - 2; i++) {
      if (cells[i] && cells[i + 1] === ':' && typeof cells[i + 2] !== 'undefined') {
        label = cells[i].toLowerCase().replace(/\s+/g, ' ').replace(/[:.*]/g, '').trim();
        value = cells[i + 2];
        break;
      }
    }
    if (label && value) fields[label] = value;
  });

  // Also try mailto
  const mailto = $('a[href^="mailto:"]').first().attr('href') || '';
  const emailFromLink = mailto.replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();

  // Extract license number from heading or body
  const licenceMatch = $('body').text().match(/Licen[cs]e No[.:\s]*([A-Z0-9\-\/]+)/i);

  return {
    name: fields['name of agency'] || fields['agency name'] || fields['name'] || fields['company name'] || '',
    licence: fields['recruiting licence no'] || fields['licence no'] || fields['license no'] || (licenceMatch && licenceMatch[1]) || '',
    proprietor: fields['contact person'] || fields['proprietor'] || fields['owner'] || fields['name of proprietor'] || '',
    address: fields['address'] || fields['registered address'] || fields['office address'] || '',
    mobile: fields['mobile'] || fields['cell'] || '',
    phone: fields['telephone no'] || fields['phone'] || fields['telephone'] || fields['tel'] || '',
    email: emailFromLink || (fields['email'] || '').toLowerCase(),
    city: fields['city'] || '',
  };
}

async function main() {
  console.log('=== BAIRA Bangladesh Scraper ===');

  const cols = ['id', 'licence', 'name', 'proprietor', 'email', 'mobile', 'phone', 'address', 'niche', 'country', 'source'];
  if (!fs.existsSync(OUTPUT)) fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  // Resume state
  let state = { done: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const done = new Set(state.done);

  const ids = [];
  for (let i = 300; i <= 3200; i++) ids.push(i);

  let total = 0, withEmail = 0, with404 = 0;
  const start = Date.now();

  for (let i = 0; i < ids.length; i += CONCURRENCY) {
    const batch = ids.slice(i, i + CONCURRENCY).filter(id => !done.has(id));
    if (!batch.length) continue;

    const results = await Promise.all(batch.map(async id => {
      try {
        const html = await httpGet(`${BASE}/${id}`);
        return { id, ...parseDetail(html) };
      } catch (err) {
        return { id, _err: err.message };
      }
    }));

    for (const r of results) {
      if (r._err || !r.name) {
        done.add(r.id);
        with404++;
        continue;
      }
      const outRow = {
        id: r.id,
        licence: r.licence,
        name: r.name,
        proprietor: r.proprietor,
        email: r.email,
        mobile: r.mobile,
        phone: r.phone,
        address: r.address,
        niche: 'recruitment agency',
        country: 'Bangladesh',
        source: 'baira_bangladesh',
      };
      fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
      if (r.email) withEmail++;
      total++;
      done.add(r.id);
    }

    state.done = [...done];
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));

    const elapsed = (Date.now() - start) / 1000;
    const processed = done.size - state.done.length + (i + batch.length);
    if ((i / CONCURRENCY) % 10 === 0) {
      const rate = ((i + batch.length) / elapsed).toFixed(1);
      const eta = Math.round((ids.length - i - batch.length) / parseFloat(rate));
      console.log(`  [${i + batch.length}/${ids.length}] agencies=${total} emails=${withEmail} 404s=${with404} ${rate}/s ETA ${eta}s`);
    }
  }

  console.log(`\n=== DONE — ${total} agencies, ${withEmail} with email, ${with404} not found ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
