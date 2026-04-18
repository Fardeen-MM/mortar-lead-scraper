#!/usr/bin/env node
/**
 * College Scorecard API Nursing School Scraper
 *
 * Pulls US nursing schools from the Department of Education's College Scorecard API
 * by CIP program code. Outputs CSV with school metadata + URLs for downstream faculty
 * directory crawling.
 *
 * CIP codes targeted:
 *   5138 - Registered Nursing, Nursing Administration, Research, Clinical Nursing
 *   5139 - Practical Nursing, Vocational Nursing, Nursing Assistants
 *   5180 - Nurse Practitioner (older classification)
 *
 * Usage:
 *   node scripts/scrape-college-scorecard-nursing.js
 *
 * Output:
 *   output/college-scorecard-nursing-schools.csv
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const API_BASE = 'https://api.data.gov/ed/collegescorecard/v1/schools';
const API_KEY = process.env.COLLEGE_SCORECARD_API_KEY || 'DEMO_KEY';
const PER_PAGE = 100;
const RATE_LIMIT_MS = parseInt(process.env.CS_RATE_MS || '2500', 10);
const CIP_CODES = ['5138', '5139', '5180'];
const FIELDS = [
  'id',
  'school.name',
  'school.school_url',
  'school.city',
  'school.state',
  'school.zip',
].join(',');

const OUTPUT_DIR = path.resolve(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'college-scorecard-nursing-schools.csv');

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function httpsGetJsonOnce(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'mortar-nursing-scraper/1.0' } }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        if (res.statusCode !== 200) {
          const err = new Error(`HTTP ${res.statusCode}`);
          err.statusCode = res.statusCode;
          err.body = data;
          return reject(err);
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`JSON parse failed: ${e.message}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy(new Error('Request timeout'));
    });
  });
}

async function httpsGetJson(url, maxRetries = 8) {
  let attempt = 0;
  let delay = 2000;
  while (true) {
    try {
      return await httpsGetJsonOnce(url);
    } catch (err) {
      attempt++;
      if (attempt > maxRetries) throw err;
      const is429 = err.statusCode === 429;
      const wait = is429 ? delay : 1000;
      console.log(`  retry ${attempt}/${maxRetries} in ${wait}ms (${err.statusCode || err.message})`);
      await sleep(wait);
      if (is429) delay = Math.min(delay * 2, 60_000);
    }
  }
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

async function fetchCipPage(cipCode, page) {
  const params = new URLSearchParams({
    'programs.cip_4_digit.code': cipCode,
    fields: FIELDS,
    per_page: String(PER_PAGE),
    page: String(page),
    api_key: API_KEY,
  });
  const url = `${API_BASE}?${params.toString()}`;
  return httpsGetJson(url);
}

async function fetchAllForCip(cipCode, onPage) {
  const results = [];
  const first = await fetchCipPage(cipCode, 0);
  const total = first.metadata.total;
  const pages = Math.ceil(total / PER_PAGE);
  console.log(`[CIP ${cipCode}] total=${total} pages=${pages}`);
  for (const r of first.results) results.push({ cip_code: cipCode, ...r });
  if (onPage) onPage(first.results, cipCode, 0, pages);

  for (let p = 1; p < pages; p++) {
    await sleep(RATE_LIMIT_MS);
    try {
      const pageData = await fetchCipPage(cipCode, p);
      for (const r of pageData.results) results.push({ cip_code: cipCode, ...r });
      if (onPage) onPage(pageData.results, cipCode, p, pages);
      if ((p + 1) % 3 === 0 || p === pages - 1) {
        console.log(`[CIP ${cipCode}] page ${p + 1}/${pages} collected=${results.length}`);
      }
    } catch (err) {
      console.error(`[CIP ${cipCode}] page ${p} failed: ${err.message}`);
    }
  }
  return results;
}

function normalizeUrl(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let u = raw.trim();
  if (!u) return '';
  if (!/^https?:\/\//i.test(u)) u = 'http://' + u;
  try {
    const parsed = new URL(u);
    return parsed.origin;
  } catch {
    return '';
  }
}

async function main() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const byIpeds = new Map(); // dedup by ipeds_id; keep first CIP seen
  // Load partial state if exists, so we can resume
  const PARTIAL_FILE = path.join(OUTPUT_DIR, 'college-scorecard-nursing-schools.partial.json');
  if (fs.existsSync(PARTIAL_FILE)) {
    try {
      const saved = JSON.parse(fs.readFileSync(PARTIAL_FILE, 'utf-8'));
      for (const [k, v] of Object.entries(saved)) byIpeds.set(Number(k), v);
      console.log(`Resumed with ${byIpeds.size} schools from partial cache`);
    } catch {
      // ignore
    }
  }

  const saveIncremental = () => {
    const obj = {};
    for (const [k, v] of byIpeds.entries()) obj[k] = v;
    fs.writeFileSync(PARTIAL_FILE, JSON.stringify(obj));
  };

  for (const cip of CIP_CODES) {
    try {
      await fetchAllForCip(cip, (pageResults) => {
        for (const row of pageResults) {
          const ipedsId = row.id;
          if (!ipedsId) continue;
          if (byIpeds.has(ipedsId)) {
            const existing = byIpeds.get(ipedsId);
            if (!existing.cip_codes.includes(cip)) existing.cip_codes.push(cip);
            continue;
          }
          byIpeds.set(ipedsId, {
            ipeds_id: ipedsId,
            school_name: row['school.name'] || '',
            school_url: normalizeUrl(row['school.school_url']),
            city: row['school.city'] || '',
            state: row['school.state'] || '',
            zip: row['school.zip'] || '',
            cip_codes: [cip],
          });
        }
        saveIncremental();
      });
    } catch (err) {
      console.error(`[CIP ${cip}] aborted: ${err.message}`);
    }
    await sleep(RATE_LIMIT_MS);
  }

  const headers = [
    'ipeds_id',
    'school_name',
    'school_url',
    'city',
    'state',
    'zip',
    'cip_code',
    'country',
    'niche',
    'source',
  ];
  const lines = [headers.join(',')];
  let withUrl = 0;
  for (const row of byIpeds.values()) {
    if (row.school_url) withUrl++;
    lines.push(
      [
        csvEscape(row.ipeds_id),
        csvEscape(row.school_name),
        csvEscape(row.school_url),
        csvEscape(row.city),
        csvEscape(row.state),
        csvEscape(row.zip),
        csvEscape(row.cip_codes.join('|')),
        csvEscape('US'),
        csvEscape('nursing-education'),
        csvEscape('college-scorecard'),
      ].join(',')
    );
  }
  fs.writeFileSync(OUTPUT_FILE, lines.join('\n'));
  console.log(`\nWrote ${byIpeds.size} schools (${withUrl} with URL) -> ${OUTPUT_FILE}`);
  console.log(`By CIP:`);
  const cipCounts = {};
  for (const row of byIpeds.values()) {
    for (const c of row.cip_codes) cipCounts[c] = (cipCounts[c] || 0) + 1;
  }
  for (const [c, n] of Object.entries(cipCounts)) console.log(`  ${c}: ${n}`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error('FATAL:', err);
    process.exit(1);
  });
}
