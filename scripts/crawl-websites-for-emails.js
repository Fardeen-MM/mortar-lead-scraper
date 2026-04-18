#!/usr/bin/env node
/**
 * HTTP-based website email crawler.
 *
 * For each domain/website in a CSV, fetches:
 *   - The homepage
 *   - /contact, /contact-us, /about, /about-us (common contact page paths)
 * Then extracts all emails via:
 *   - mailto: links
 *   - Cloudflare email protection (data-cfemail hex decode)
 *   - HTML entity decoded (&#64;, [at], (dot))
 *   - Plain regex in HTML
 *
 * Pure HTTP + cheerio. No Puppeteer. No SMTP. ~200ms per domain on good day.
 * For 2,216 domains at concurrency 15: ~5-10 minutes.
 *
 * Usage:
 *   node scripts/crawl-websites-for-emails.js --in output/icef-ias-global-agents.csv
 *   node scripts/crawl-websites-for-emails.js --in input.csv --out crawled.csv --concurrency 20
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { URL } = require('url');

// Parse args
const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const s = a.replace(/^--/, '');
  if (s.includes('=')) { const [k, v] = s.split('='); args[k] = v; }
  else { const n = argv[i + 1]; if (n && !n.startsWith('--')) { args[s] = n; i++; } else args[s] = true; }
}

const INPUT_FILE = args.in;
const OUTPUT_FILE = args.out || (INPUT_FILE ? INPUT_FILE.replace(/\.csv$/, '-emails.csv') : null);
const CONCURRENCY = parseInt(args.concurrency) || 15;
const TIMEOUT = parseInt(args.timeout) || 12000;
const LIMIT = parseInt(args.limit) || 0;
const PATHS = (args.paths || '/,/contact,/contact-us,/about,/about-us').split(',');

if (!INPUT_FILE) {
  console.error('Usage: node scripts/crawl-websites-for-emails.js --in input.csv [--out output.csv] [--concurrency 15]');
  process.exit(1);
}

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

const BAD_EMAIL_DOMAINS = new Set([
  'example.com','example.org','test.com','domain.com','yoursite.com','yourdomain.com',
  'wixpress.com','sentry.io','sentry-next.wixpress.com','googleapis.com','google.com',
  'facebook.com','twitter.com','linkedin.com','instagram.com','youtube.com','tiktok.com',
  'w3.org','schema.org','wordpress.com','gravatar.com','cloudflare.com','jquery.com',
  'bootstrapcdn.com','fontawesome.com','googletagmanager.com','sentry.wixpress.com',
]);

const BAD_EMAIL_PREFIXES = new Set([
  'noreply','no-reply','donotreply','do-not-reply','postmaster','mailer-daemon','webmaster',
  'abuse','bounce','wordpress','sentry','hello@sentry','security','spam',
]);

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
];

function decodeCfEmail(hex) {
  if (!hex || hex.length < 4 || hex.length % 2 !== 0) return '';
  try {
    const key = parseInt(hex.substr(0, 2), 16);
    let out = '';
    for (let i = 2; i < hex.length; i += 2) {
      out += String.fromCharCode(parseInt(hex.substr(i, 2), 16) ^ key);
    }
    return out;
  } catch { return ''; }
}

function extractEmails(html) {
  const emails = new Set();

  // 1. mailto: links
  const mailtoMatches = html.match(/mailto:[^"'\s)>]+/gi) || [];
  for (const m of mailtoMatches) {
    const clean = m.replace(/^mailto:/i, '').split('?')[0].split('&')[0].trim().toLowerCase();
    if (clean) emails.add(clean);
  }

  // 2. Cloudflare email protection
  const cfMatches = html.match(/data-cfemail="([a-f0-9]+)"/gi) || [];
  for (const m of cfMatches) {
    const hex = m.match(/"([a-f0-9]+)"/i)?.[1] || '';
    const decoded = decodeCfEmail(hex);
    if (decoded && EMAIL_RE.test(decoded)) emails.add(decoded.toLowerCase());
  }

  // 3. Entity / obfuscation decode
  const decoded = html
    .replace(/&#64;/gi, '@').replace(/&#46;/gi, '.')
    .replace(/\s*\[at\]\s*/gi, '@').replace(/\s*\(at\)\s*/gi, '@')
    .replace(/\s+at\s+/gi, '@').replace(/\s*\[dot\]\s*/gi, '.').replace(/\s*\(dot\)\s*/gi, '.');

  // 4. Plain email regex
  const plainMatches = decoded.match(EMAIL_RE) || [];
  for (const e of plainMatches) emails.add(e.toLowerCase());

  // Filter out bad ones
  const result = [];
  for (const e of emails) {
    const [local, domain] = e.split('@');
    if (!local || !domain) continue;
    if (BAD_EMAIL_DOMAINS.has(domain)) continue;
    if (BAD_EMAIL_PREFIXES.has(local)) continue;
    if (e.length < 5 || e.length > 100) continue;
    if (/\.(jpg|jpeg|png|gif|svg|pdf|css|js|ico|woff|woff2|ttf)$/i.test(e)) continue;
    // Skip file-wrapped emails
    if (/\/|\\|\?|#/.test(e)) continue;
    result.push(e);
  }
  return result;
}

function httpFetch(url) {
  return new Promise((resolve, reject) => {
    let parsed;
    try { parsed = new URL(url); } catch { return reject(new Error('invalid url')); }
    const mod = parsed.protocol === 'https:' ? https : http;
    const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    const req = mod.get({
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: TIMEOUT,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        // Follow one redirect
        let loc = res.headers.location;
        if (loc.startsWith('/')) loc = `${parsed.protocol}//${parsed.hostname}${loc}`;
        res.resume();
        return httpFetch(loc).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      // Only accept text/html or text
      const ct = (res.headers['content-type'] || '').toLowerCase();
      if (ct && !ct.includes('text/html') && !ct.includes('text/plain') && !ct.includes('application/xhtml')) {
        res.resume();
        return reject(new Error(`Non-HTML content-type: ${ct}`));
      }
      const chunks = [];
      let total = 0;
      res.on('data', c => {
        chunks.push(c);
        total += c.length;
        if (total > 1500000) { // 1.5MB cap
          res.destroy();
        }
      });
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function crawlDomain(domain) {
  if (!domain) return { emails: [], tried: 0 };
  const base = domain.startsWith('http') ? domain : `https://${domain.replace(/^https?:\/\//, '').replace(/\/$/, '')}`;
  const emails = new Set();
  let tried = 0;

  for (const pth of PATHS) {
    const url = base.replace(/\/$/, '') + pth;
    tried++;
    try {
      const html = await httpFetch(url);
      const found = extractEmails(html);
      for (const e of found) emails.add(e);
      // Early exit if we have enough
      if (emails.size >= 5) break;
    } catch (err) {
      // skip
    }
  }
  return { emails: [...emails], tried };
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
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
  if (!lines.length) return { headers: [], rows: [] };
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {};
    headers.forEach((h, idx) => { rec[h] = cols[idx] || ''; });
    rows.push(rec);
  }
  return { headers, rows };
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function writeCsv(headers, rows) {
  const out = [headers.join(',')];
  for (const r of rows) out.push(headers.map(h => escapeCsv(r[h] || '')).join(','));
  fs.writeFileSync(OUTPUT_FILE, out.join('\n') + '\n');
}

async function main() {
  console.log(`=== HTTP Website Email Crawler ===`);
  console.log(`Input:       ${INPUT_FILE}`);
  console.log(`Output:      ${OUTPUT_FILE}`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Paths:       ${PATHS.join(' ')}`);

  const text = fs.readFileSync(INPUT_FILE, 'utf-8');
  const { headers, rows } = parseCsv(text);
  const workingRows = LIMIT > 0 ? rows.slice(0, LIMIT) : rows;
  console.log(`Rows: ${workingRows.length} of ${rows.length}`);

  const newHeaders = [...headers];
  for (const c of ['crawled_emails', 'crawled_email_count', 'primary_email']) {
    if (!newHeaders.includes(c)) newHeaders.push(c);
  }

  const queue = [...workingRows.map((r, i) => ({ row: r, idx: i }))];
  let done = 0, withEmail = 0;
  const start = Date.now();

  const saveTimer = setInterval(() => writeCsv(newHeaders, rows), 20000);

  async function worker() {
    while (queue.length) {
      const item = queue.shift();
      if (!item) return;
      const domain = item.row.domain || item.row.Domain || item.row.website || '';
      try {
        const r = await crawlDomain(domain);
        const uniq = [...new Set(r.emails)];
        item.row.crawled_emails = uniq.join('; ');
        item.row.crawled_email_count = uniq.length;
        item.row.primary_email = uniq[0] || '';
        if (uniq.length) withEmail++;
      } catch (err) {
        item.row.crawled_emails = '';
        item.row.crawled_email_count = 0;
        item.row.primary_email = '';
      }
      done++;
      if (done % 25 === 0 || done === workingRows.length) {
        const elapsed = (Date.now() - start) / 1000;
        const rate = (done / elapsed).toFixed(1);
        const eta = workingRows.length > done ? Math.round((workingRows.length - done) / (parseFloat(rate) || 1)) : 0;
        console.log(`  [${(done / workingRows.length * 100).toFixed(1)}%] ${done}/${workingRows.length}, emails=${withEmail}, ${rate}/s, ETA ${eta}s`);
      }
    }
  }

  const workers = Array(CONCURRENCY).fill().map(() => worker());
  await Promise.all(workers);

  clearInterval(saveTimer);
  writeCsv(newHeaders, rows);

  console.log(`\n=== DONE ===`);
  console.log(`Processed:   ${workingRows.length}`);
  console.log(`With emails: ${withEmail} (${(withEmail / workingRows.length * 100).toFixed(1)}%)`);
  console.log(`Total email instances: ${workingRows.reduce((n, r) => n + parseInt(r.crawled_email_count || 0), 0)}`);
  console.log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
