#!/usr/bin/env node
/**
 * Email-waterfall the 2,169 Martindale rows that have name + domain.
 * Uses MS365 GetCredentialType (free, ~40% hit) + pattern generation.
 */
const fs = require('fs');
const path = require('path');
const { EmailWaterfall } = require('../lib/email-waterfall');

const IN = path.join(__dirname, '..', 'output', 'us-immigration-lawyers-martindale.csv');
const OUT = path.join(__dirname, '..', 'output', 'martindale-immigration-emails.csv');
const STATE_FILE = OUT.replace(/\.csv$/, '.state.json');

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i+1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else {
        if (c === ',') { out.push(cur); cur = ''; }
        else if (c === '"' && cur === '') inQ = true;
        else cur += c;
      }
    }
    out.push(cur);
    return out;
  }
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {}; headers.forEach((h, idx) => rec[h] = cols[idx] || '');
    rows.push(rec);
  }
  return rows;
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

async function main() {
  console.log('=== Email waterfall — Martindale 2,169 candidates ===');
  const rows = parseCsv(fs.readFileSync(IN, 'utf-8'));
  const targets = rows.filter(r => r.first_name && r.last_name && r.domain);
  console.log(`Enrichable: ${targets.length}`);

  // Resume state
  let state = { done: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const done = new Set(state.done);

  const cols = ['email', 'confidence', 'first_name', 'last_name', 'firm_name',
                'city', 'state', 'phone', 'website', 'domain', 'profile_url',
                'country', 'niche', 'source'];
  if (!fs.existsSync(OUT)) fs.writeFileSync(OUT, cols.join(',') + '\n');

  const waterfall = new EmailWaterfall();
  let verified = 0, notFound = 0;
  const start = Date.now();

  for (let i = 0; i < targets.length; i++) {
    const r = targets[i];
    const key = `${r.first_name}|${r.last_name}|${r.domain}`;
    if (done.has(key)) continue;

    try {
      const result = await Promise.race([
        waterfall.findEmail({ first_name: r.first_name, last_name: r.last_name, domain: r.domain }),
        new Promise(res => setTimeout(() => res({ email: '', status: 'timeout' }), 15000)),
      ]);

      const outRow = {
        email: result?.email || '',
        confidence: result?.confidence || 0,
        first_name: r.first_name,
        last_name: r.last_name,
        firm_name: r.firm_name || '',
        city: r.city || '',
        state: r.state || '',
        phone: r.phone || '',
        website: r.website || '',
        domain: r.domain,
        profile_url: r.profile_url || '',
        country: 'US',
        niche: 'immigration attorney',
        source: 'martindale_waterfall',
      };

      fs.appendFileSync(OUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');

      if (outRow.email) verified++;
      else notFound++;

      done.add(key);
      if ((i + 1) % 20 === 0 || i === targets.length - 1) {
        state.done = [...done];
        fs.writeFileSync(STATE_FILE, JSON.stringify(state));
        const elapsed = (Date.now() - start) / 1000;
        const rate = ((i + 1) / elapsed).toFixed(2);
        const eta = Math.round((targets.length - i - 1) / parseFloat(rate));
        console.log(`  [${i+1}/${targets.length}] verified=${verified} not_found=${notFound} ${rate}/s ETA ${eta}s`);
      }
    } catch (err) {
      // skip
    }
  }

  console.log(`\n=== DONE — ${verified} verified emails ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
