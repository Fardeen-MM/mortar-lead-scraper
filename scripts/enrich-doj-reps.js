#!/usr/bin/env node
/**
 * Enrich DOJ Accredited Representatives (EOIR list).
 * 944 unique nonprofit firm names → guess .org domain → crawl for emails.
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const dns = require('dns').promises;

const IN_FILE = path.join(__dirname, '..', 'output', 'doj-immigration-representatives.csv');
const OUT_FILE = path.join(__dirname, '..', 'output', 'doj-reps-enriched.csv');
const STATE_FILE = OUT_FILE.replace(/\.csv$/, '.state.json');
const CONCURRENCY = 8;
const TIMEOUT = 8000;

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

function generateDomains(firmName) {
  const name = firmName
    .replace(/[,.]/g, ' ')
    .replace(/\(.*?\)/g, ' ')
    .replace(/\b(inc|incorporated|llc|llp|corp|corporation|the|a|an|of|for|and|d\/b\/a|dba)\b/gi, ' ')
    .replace(/[^a-z0-9\s]/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

  const words = name.split(/\s+/).filter(Boolean);
  if (!words.length) return [];

  const candidates = new Set();
  // Full name concatenated
  candidates.add(words.join('') + '.org');
  // Initials + first word
  if (words.length >= 2) {
    candidates.add(words.map(w => w[0]).join('') + '.org');
  }
  // First word only
  if (words[0].length >= 4) candidates.add(words[0] + '.org');
  // First two words
  if (words.length >= 2) candidates.add(words[0] + words[1] + '.org');
  // Acronyms like CHIRLA, RAICES, CARECEN often appear
  const acronymMatch = firmName.match(/\(([A-Z]{3,})\)/);
  if (acronymMatch) {
    candidates.add(acronymMatch[1].toLowerCase() + '.org');
  }
  return [...candidates];
}

async function checkDomain(domain) {
  try {
    await dns.resolve(domain, 'MX');
    return true;
  } catch {
    try {
      await dns.resolve(domain, 'A');
      return true;
    } catch {
      return false;
    }
  }
}

function httpGet(url, isHttps = true) {
  return new Promise((resolve) => {
    const mod = isHttps ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Accept': 'text/html',
      },
      timeout: TIMEOUT,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = res.headers.location.startsWith('http') ? res.headers.location : new URL(res.headers.location, url).href;
        return resolve(httpGet(next, next.startsWith('https')));
      }
      if (res.statusCode !== 200) { res.resume(); return resolve({ status: res.statusCode, body: '' }); }
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: 200, body: Buffer.concat(chunks).toString('utf-8') }));
    });
    req.on('error', () => resolve({ status: 0, body: '' }));
    req.on('timeout', () => { req.destroy(); resolve({ status: 0, body: '' }); });
  });
}

function extractEmails(html, firstName, lastName) {
  const emails = new Set();
  try {
    const $ = cheerio.load(html);
    $('a[href^="mailto:"]').each((_, a) => {
      const e = ($(a).attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
      if (e && e.includes('@') && /\.[a-z]{2,}$/.test(e)) emails.add(e);
    });
    // Also scan text for email regex
    const text = $('body').text();
    const matches = text.match(/[a-z0-9._+\-]+@[a-z0-9.\-]+\.[a-z]{2,}/gi) || [];
    for (const m of matches) {
      if (/(example|domain|yourdomain|yoursite|test@)/i.test(m)) continue;
      emails.add(m.toLowerCase());
    }
  } catch {}

  // If firstName/lastName given, prefer matching emails
  const list = [...emails];
  if (firstName && lastName) {
    const fn = firstName.toLowerCase().replace(/[^a-z]/g, '');
    const ln = lastName.toLowerCase().replace(/[^a-z]/g, '');
    const matched = list.filter(e => {
      const local = e.split('@')[0];
      return local.includes(fn) || local.includes(ln);
    });
    if (matched.length) return { matched, all: list };
  }
  return { matched: [], all: list };
}

async function findFirmDomain(firmName) {
  const candidates = generateDomains(firmName);
  for (const d of candidates) {
    if (await checkDomain(d)) return d;
  }
  return null;
}

async function crawlFirm(firm, domain) {
  const contactPaths = ['/', '/contact', '/about', '/staff', '/team', '/our-team', '/about-us', '/contact-us'];
  const allEmails = new Set();

  for (const p of contactPaths) {
    const url = `https://${domain}${p}`;
    const res = await httpGet(url, true);
    if (res.status === 200) {
      const { all } = extractEmails(res.body);
      for (const e of all) {
        // Filter out catch-all and non-firm emails
        const emailDomain = e.split('@')[1];
        if (emailDomain && (emailDomain === domain || emailDomain.endsWith('.' + domain))) {
          allEmails.add(e);
        }
      }
      if (allEmails.size >= 10) break;
    }
  }
  return [...allEmails];
}

async function main() {
  console.log('=== DOJ Reps Enrichment ===');

  const rows = parseCsv(fs.readFileSync(IN_FILE, 'utf-8'));
  console.log(`Total reps: ${rows.length}`);

  // Group by firm
  const byFirm = new Map();
  for (const r of rows) {
    const firm = (r.firm_name || '').trim();
    if (!firm) continue;
    if (!byFirm.has(firm)) byFirm.set(firm, []);
    byFirm.get(firm).push(r);
  }
  console.log(`Unique firms: ${byFirm.size}`);

  // Resume
  let state = { firmDomains: {}, doneFirms: [] };
  if (fs.existsSync(STATE_FILE)) {
    try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  const doneFirms = new Set(state.doneFirms);
  const firmDomains = new Map(Object.entries(state.firmDomains || {}));

  const cols = ['first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
                'domain', 'city', 'state', 'country', 'niche', 'title', 'source'];
  if (!fs.existsSync(OUT_FILE)) fs.writeFileSync(OUT_FILE, cols.join(',') + '\n');

  const firms = [...byFirm.keys()];
  let processed = 0, enriched = 0, totalEmails = 0;
  const start = Date.now();

  for (let i = 0; i < firms.length; i += CONCURRENCY) {
    const batch = firms.slice(i, i + CONCURRENCY).filter(f => !doneFirms.has(f));
    if (!batch.length) { processed += CONCURRENCY; continue; }

    const results = await Promise.all(batch.map(async firm => {
      try {
        let domain = firmDomains.get(firm);
        if (!domain) {
          domain = await findFirmDomain(firm);
          if (domain) firmDomains.set(firm, domain);
        }
        if (!domain) return { firm, emails: [] };
        const emails = await crawlFirm(firm, domain);
        return { firm, domain, emails };
      } catch (err) {
        return { firm, emails: [] };
      }
    }));

    for (const { firm, domain, emails } of results) {
      const reps = byFirm.get(firm) || [];
      if (emails && emails.length > 0) {
        enriched++;
        // For each rep in this firm, try to match email by name or use a representative
        for (const rep of reps) {
          const fn = (rep.first_name || '').toLowerCase().replace(/[^a-z]/g, '');
          const ln = (rep.last_name || '').toLowerCase().replace(/[^a-z]/g, '');
          let matched = emails.find(e => {
            const local = e.split('@')[0];
            return fn && local.includes(fn);
          }) || emails.find(e => {
            const local = e.split('@')[0];
            return ln && local.includes(ln);
          });
          if (!matched) matched = emails[0]; // fallback: use org-level email

          const outRow = {
            first_name: rep.first_name || '',
            last_name: rep.last_name || '',
            firm_name: firm,
            email: matched,
            phone: rep.phone || '',
            website: `https://${domain}`,
            domain,
            city: rep.city || '',
            state: rep.state || '',
            country: 'US',
            niche: 'immigration nonprofit',
            title: rep.title || '',
            source: 'doj_eoir_enriched',
          };
          fs.appendFileSync(OUT_FILE, cols.map(c => escapeCsv(outRow[c] || '')).join(',') + '\n');
          totalEmails++;
        }
      }
      doneFirms.add(firm);
    }
    processed += batch.length;

    state.firmDomains = Object.fromEntries(firmDomains);
    state.doneFirms = [...doneFirms];
    fs.writeFileSync(STATE_FILE, JSON.stringify(state));

    const elapsed = (Date.now() - start) / 1000;
    const rate = (processed / elapsed).toFixed(1);
    const eta = Math.round((firms.length - i - batch.length) / parseFloat(rate));
    if ((i / CONCURRENCY) % 5 === 0) {
      console.log(`  [${i + batch.length}/${firms.length}] enriched=${enriched} emails=${totalEmails} ${rate}/s ETA ${eta}s`);
    }
  }

  console.log(`\n=== DONE — ${enriched}/${firms.length} firms enriched, ${totalEmails} email rows ===`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
