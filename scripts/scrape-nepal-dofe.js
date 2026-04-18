#!/usr/bin/env node
/**
 * Nepal DOFE (Department of Foreign Employment) — Recruitment Agencies.
 * Single JSON endpoint, caps 100 rows/query. Iterate by name prefix.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'nepal-dofe-recruitment-agencies.csv');
const BASE = 'https://foreignjob.dofe.gov.np/Home/Get_RecruitmentAgency';

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
      timeout: 20000,
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

async function main() {
  console.log('=== Nepal DOFE Scraper ===');

  const cols = ['id', 'name', 'permission_no', 'district', 'email', 'mobile', 'telephone', 'address', 'website', 'status', 'niche', 'country', 'source'];
  fs.writeFileSync(OUTPUT, cols.join(',') + '\n');

  // Generate name prefixes: a, b, c, ..., z, aa, ab, ..., zz (up to 2 chars)
  const prefixes = [];
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  for (const c of chars) prefixes.push(c);
  // Add 2-char for prefixes likely to hit cap
  for (const c1 of chars) for (const c2 of chars) prefixes.push(c1 + c2);

  const seen = new Set();
  let total = 0, withEmail = 0;

  for (const prefix of prefixes) {
    try {
      const url = `${BASE}?PermissionNo=&Name=${encodeURIComponent(prefix)}&StatusID=`;
      const body = await httpGet(url);
      let data;
      try { data = JSON.parse(body); } catch { continue; }
      const rows = Array.isArray(data) ? data : (data.data || data.Rows || data.rowData || []);
      if (!Array.isArray(rows)) continue;

      let newInBatch = 0;
      for (const r of rows) {
        const id = r.id || r.Id || r.ID;
        if (!id || seen.has(id)) continue;
        seen.add(id);
        const outRow = {
          id,
          name: r.name || r.Name || '',
          permission_no: r.permissionNo || r.PermissionNo || '',
          district: r.district || r.District || '',
          email: (r.email || r.Email || '').trim().toLowerCase(),
          mobile: r.mobileNo || r.MobileNo || r.mobile || '',
          telephone: r.telephone || r.Telephone || '',
          address: r.address || r.Address || '',
          website: r.website || r.Website || '',
          status: r.statusName || r.StatusName || '',
          niche: 'recruitment agency',
          country: 'Nepal',
          source: 'nepal_dofe',
        };
        fs.appendFileSync(OUTPUT, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
        if (outRow.email) withEmail++;
        total++;
        newInBatch++;
      }
      if (newInBatch > 0) {
        console.log(`  [${prefix}]: ${rows.length} rows (${newInBatch} new). Total: ${total}, emails: ${withEmail}`);
      }
    } catch (err) {
      // Skip
    }
    await new Promise(r => setTimeout(r, 400));
  }

  console.log(`\n=== DONE — ${total} agencies, ${withEmail} with email ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
