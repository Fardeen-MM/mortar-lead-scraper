#!/usr/bin/env node
/**
 * CrossFit Affiliates Scraper
 * Strategy:
 *  1) Fetch bulk GeoJSON: https://www.crossfit.com/wp-content/uploads/affiliates.json
 *     (9,140 gyms — name/city/state/country/slug, no contact info)
 *  2) Optional: fetch per-gym SSR HTML to extract email/phone/website
 *     (costly: 9K requests — gated behind --enrich flag)
 *
 * Output: output/crossfit-affiliates.csv
 * Columns: affiliate_name, location, city, state, country, website, email, phone, niche, source
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'crossfit-affiliates.csv');
const STATE_FILE = path.join(OUT_DIR, '.crossfit.state.json');
const BULK_URL = 'https://www.crossfit.com/wp-content/uploads/affiliates.json';

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (_) { /* ignore */ }
  }
  return { phase: 'bulk', enriched: {}, index: 0 };
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function httpGet(url, tries = 4, timeout = 30000) {
  return new Promise((resolve, reject) => {
    const attempt = (n) => {
      const opts = {
        headers: {
          'User-Agent': USER_AGENT,
          'Accept': 'text/html,application/json,application/xhtml+xml,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
        }
      };
      const req = https.get(url, opts, (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (c) => body += c);
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) resolve(body);
          else if ((res.statusCode === 429 || res.statusCode >= 500) && n < tries) {
            setTimeout(() => attempt(n + 1), 1500 * n);
          } else {
            reject(new Error(`HTTP ${res.statusCode}`));
          }
        });
      });
      req.on('error', (e) => {
        if (n < tries) setTimeout(() => attempt(n + 1), 1500 * n);
        else reject(e);
      });
      req.setTimeout(timeout, () => req.destroy(new Error('timeout')));
    };
    attempt(1);
  });
}

function toCsvField(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function featureToRow(feat) {
  const p = feat.properties || {};
  const location = [p.city, p.state, p.country].filter(Boolean).join(', ');
  return {
    id: p.id || '',
    slug: p.slug || '',
    affiliate_name: p.name || '',
    location,
    city: p.city || '',
    state: p.state || '',
    country: p.country || '',
    website: '',
    email: '',
    phone: '',
    niche: 'crossfit-gym',
    source: 'crossfit-affiliates'
  };
}

function extractContactFromHtml(html) {
  const out = { email: '', website: '', phone: '' };
  // email: <a data-id="affiliate-email-link" href="mailto:...">
  const mEmail = html.match(/data-id=["']affiliate-email-link["'][^>]*href=["']mailto:([^"']+)["']/);
  if (mEmail) out.email = mEmail[1].trim();
  // website: <a data-id="affiliate-website-link" href="https://...">
  const mWeb = html.match(/data-id=["']affiliate-website-link["'][^>]*href=["']([^"']+)["']/);
  if (mWeb) out.website = mWeb[1].trim();
  // phone: <a data-id="affiliate-phone-link" href="tel:...">
  const mPhone = html.match(/data-id=["']affiliate-phone-link["'][^>]*href=["']tel:([^"']+)["']/);
  if (mPhone) out.phone = mPhone[1].trim();
  return out;
}

async function main() {
  const args = process.argv.slice(2);
  const TEST = args.includes('--test');
  const ENRICH = args.includes('--enrich');
  const MAX_ENRICH_ARG = args.find(a => a.startsWith('--max-enrich='));
  const MAX_ENRICH = MAX_ENRICH_ARG ? parseInt(MAX_ENRICH_ARG.split('=')[1], 10) : (TEST ? 5 : Infinity);

  const header = 'affiliate_name,location,city,state,country,website,email,phone,niche,source\n';
  if (!fs.existsSync(OUT_FILE)) fs.writeFileSync(OUT_FILE, header);

  const state = loadState();

  // PHASE 1: bulk download
  console.log(`Fetching bulk GeoJSON from ${BULK_URL}...`);
  const rawBulk = await httpGet(BULK_URL);
  const geo = JSON.parse(rawBulk);
  const features = geo.features || [];
  console.log(`Received ${features.length} features.`);

  // Build rows (keep id map for enrichment)
  const rows = features.map(featureToRow);
  const idToIdx = {};
  rows.forEach((r, i) => { if (r.id) idToIdx[r.id] = i; });

  // PHASE 2: enrich (optional)
  if (ENRICH) {
    const startIdx = state.index || 0;
    let enrichedCount = 0;
    for (let i = startIdx; i < rows.length; i++) {
      if (enrichedCount >= MAX_ENRICH) break;
      const r = rows[i];
      if (!r.slug) continue;
      if (state.enriched[r.id]) {
        Object.assign(r, state.enriched[r.id]);
        continue;
      }
      const url = `https://www.crossfit.com${r.slug}`;
      try {
        const html = await httpGet(url, 3, 20000);
        const c = extractContactFromHtml(html);
        r.email = c.email;
        r.website = c.website;
        r.phone = c.phone;
        state.enriched[r.id] = { email: r.email, website: r.website, phone: r.phone };
        enrichedCount++;
        process.stdout.write(`[${i + 1}/${rows.length}] ${r.affiliate_name} -> email=${r.email ? 'Y' : '-'} web=${r.website ? 'Y' : '-'} phone=${r.phone ? 'Y' : '-'}\n`);
      } catch (e) {
        process.stdout.write(`[${i + 1}/${rows.length}] ${r.affiliate_name} ERR ${e.message}\n`);
      }
      if (enrichedCount % 25 === 0) {
        state.index = i + 1;
        saveState(state);
      }
      // Rate limit
      await new Promise(res => setTimeout(res, 300));
    }
    state.index = startIdx + enrichedCount;
    saveState(state);
  }

  // Write CSV (rewrite fully for atomic run)
  const out = fs.createWriteStream(OUT_FILE);
  out.write(header);
  for (const r of rows) {
    const line = [
      r.affiliate_name, r.location, r.city, r.state, r.country,
      r.website, r.email, r.phone, r.niche, r.source
    ].map(toCsvField).join(',') + '\n';
    out.write(line);
  }
  out.end();

  console.log(`\nDONE. ${rows.length} affiliates written to ${OUT_FILE}`);
  if (!ENRICH) {
    console.log(`Tip: run with --enrich to scrape emails/websites from per-gym pages (adds ~50min at 300ms/gym).`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
