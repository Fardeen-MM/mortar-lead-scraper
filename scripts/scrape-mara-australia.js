#!/usr/bin/env node
/**
 * MARA Australia — Registered Migration Agents Scraper (Puppeteer)
 *
 * Source: https://portal.mara.gov.au/search-the-register-of-migration-agents/
 * Platform: Microsoft Power Apps / Dynamics 365 Portal
 *
 * Strategy (discovered via recon):
 *   1. Launch headless Chromium. Load the portal page — this initialises an
 *      authenticated portal session (cookie + __RequestVerificationToken) and
 *      fires the first grid POST automatically.
 *   2. Intercept the first grid POST body — it contains an opaque
 *      `base64SecureConfiguration` token that embeds the entity/view IDs and
 *      authorisation for the query. We REPLAY that exact request via
 *      page.evaluate(fetch) with only the `page` field incremented.
 *   3. Grid response gives first_name (firstname), last_name (actually the
 *      family name field), business_name, state, city, country, contactid —
 *      but NOT email/phone/MARN. These live on the per-agent detail page.
 *   4. For each agent, fetch the detail page:
 *      /search-the-register-of-migration-agents/register-of-migration-agent-details/?ContactID={id}
 *      and parse the contact-details table to extract MARN, phone, email,
 *      website, and postcode from the address block.
 *
 * Output columns:
 *   first_name, last_name, marn, business_name, email, phone,
 *   city, state, postcode, country, niche, source
 *
 * Resume: writes .progress-mara-australia.json every 50 records with
 * lastPage + completed contactIds, so a re-run resumes where it left off.
 *
 * Usage:
 *   node scripts/scrape-mara-australia.js              # full scrape
 *   node scripts/scrape-mara-australia.js --test       # first 10-20 agents only
 *   node scripts/scrape-mara-australia.js --max=500    # cap detail enrichment
 *   node scripts/scrape-mara-australia.js --no-details # grid data only
 *   node scripts/scrape-mara-australia.js --delay=1500 # override per-request delay
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// ─── CLI ──────────────────────────────────────────────────────────────────────
const args = {};
for (const a of process.argv.slice(2)) {
  const [k, v] = a.replace(/^--/, '').split('=');
  args[k] = v === undefined ? true : v;
}
const TEST_MODE = !!args.test;
const NO_DETAILS = !!args['no-details'];
const MAX_RECORDS = args.max ? parseInt(args.max, 10) : (TEST_MODE ? 20 : Infinity);
const API_DELAY_MS = parseInt(args.delay || '1000', 10);
const DETAIL_DELAY_MS = parseInt(args['detail-delay'] || '500', 10);

// ─── Paths ────────────────────────────────────────────────────────────────────
const OUT_DIR = path.join(__dirname, '..', 'output');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
const OUTPUT_CSV = path.join(OUT_DIR, 'mara-australia-migration-agents.csv');
const PROGRESS_FILE = path.join(OUT_DIR, '.progress-mara-australia.json');

// ─── Constants ────────────────────────────────────────────────────────────────
const PORTAL_URL = 'https://portal.mara.gov.au/search-the-register-of-migration-agents/';
const GRID_ENDPOINT = 'https://portal.mara.gov.au/_services/entity-grid-data.json/7b138792-1090-45b6-9241-8f8d96d8c372';
const DETAIL_URL_BASE = 'https://portal.mara.gov.au/search-the-register-of-migration-agents/register-of-migration-agent-details/?ContactID=';
const NAV_TIMEOUT = 60000;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'marn', 'business_name', 'email', 'phone',
  'city', 'state', 'postcode', 'country', 'niche', 'source',
];

// ─── Helpers ──────────────────────────────────────────────────────────────────
function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function csvEscape(v) {
  const s = (v == null ? '' : String(v));
  return /[,"\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function writeCsv(rows) {
  const header = CSV_COLUMNS.join(',');
  const body = rows.map(r => CSV_COLUMNS.map(c => csvEscape(r[c])).join(','));
  fs.writeFileSync(OUTPUT_CSV, [header, ...body].join('\n'));
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return null;
  try { return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8')); }
  catch { return null; }
}
function saveProgress(state) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(state, null, 2));
}

// Pull out scalar from the grid attribute shape
function attrVal(rec, name) {
  const a = (rec.Attributes || []).find(x => x.Name === name);
  if (!a) return '';
  // DisplayValue is the human-readable string
  if (a.DisplayValue && typeof a.DisplayValue === 'string') return a.DisplayValue.trim();
  if (typeof a.Value === 'string') return a.Value.trim();
  if (a.Value && typeof a.Value === 'object' && a.Value.Name) return String(a.Value.Name).trim();
  return '';
}

function parseRecord(rec) {
  const contactid = attrVal(rec, 'contactid') || rec.Id || '';
  const lastname = attrVal(rec, 'lastname');          // Note: the grid uses "lastname" to store the full name
  const stateProvince = attrVal(rec, 'a_65f8adef9182ea11a811000d3a6aaf7c.address1_stateorprovince');
  const city = attrVal(rec, 'a_65f8adef9182ea11a811000d3a6aaf7c.address1_city');
  const country = attrVal(rec, 'a_65f8adef9182ea11a811000d3a6aaf7c.address1_country');
  const businessName = attrVal(rec, 'dlg_contactcompanyname');
  const statusCode = attrVal(rec, 'statuscode');

  // Try to split "lastname" into first/last. The grid labels "lastname" but
  // real data ranges from a single token ("Yasmine") to 3-word names.
  let first_name = '', last_name = lastname || '';
  const parts = (lastname || '').trim().split(/\s+/);
  if (parts.length >= 2) {
    first_name = parts[0];
    last_name = parts.slice(1).join(' ');
  } else if (parts.length === 1) {
    first_name = parts[0];
    last_name = '';
  }

  return {
    contactid,
    first_name,
    last_name,
    business_name: businessName,
    city,
    state: stateProvince,
    country: country || 'Australia',
    status: statusCode,
  };
}

function parseAddressBlock(raw) {
  // Typical format:
  //   "55 Rockefeller way\nHarrisdale WA 6112\nAustralia"
  // We want to extract state + postcode + city fallback from the second line.
  const out = { city: '', state: '', postcode: '' };
  if (!raw) return out;
  const lines = raw.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  // The second-to-last line is usually "Suburb STATE POSTCODE"
  for (const line of lines) {
    const m = line.match(/^(.+?)\s+([A-Z]{2,3})\s+(\d{4})\s*$/);
    if (m) {
      out.city = m[1].trim();
      out.state = m[2];
      out.postcode = m[3];
      return out;
    }
  }
  // Fallback: look for postcode anywhere
  const pc = raw.match(/\b(\d{4})\b/);
  if (pc) out.postcode = pc[1];
  const st = raw.match(/\b(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\b/);
  if (st) out.state = st[1];
  return out;
}

// ─── Step 1: Load the portal, capture the first grid request ──────────────────
async function captureGridRequest(page) {
  let captured = null;
  const handler = (req) => {
    if (req.url() === GRID_ENDPOINT && req.method() === 'POST' && !captured) {
      captured = {
        headers: req.headers(),
        body: req.postData(),
      };
    }
  };
  page.on('request', handler);

  log(`loading ${PORTAL_URL}`);
  await page.goto(PORTAL_URL, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });

  // The grid loads via AJAX after page ready. Wait up to 12s.
  const start = Date.now();
  while (!captured && Date.now() - start < 12000) {
    await sleep(250);
  }
  page.off('request', handler);

  if (!captured) {
    throw new Error('Failed to capture initial grid POST request within 12s.');
  }
  return captured;
}

// ─── Step 2: Fetch a specific grid page, re-using captured config ─────────────
async function fetchGridPage(page, captured, pageNum) {
  // Parse the captured body, swap page, re-send via browser fetch
  const body = JSON.parse(captured.body);
  body.page = pageNum;
  body.pageSize = body.pageSize || 17;
  // pagingCookie may come from previous response; leave as null for ordered paging
  const bodyStr = JSON.stringify(body);

  const headers = {
    'Accept': captured.headers['accept'] || 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': captured.headers['content-type'] || 'application/json; charset=UTF-8',
    '__RequestVerificationToken': captured.headers['__requestverificationtoken'] || '',
    'X-Requested-With': 'XMLHttpRequest',
  };

  const result = await page.evaluate(async (url, hdrs, bodyJson) => {
    try {
      const r = await fetch(url, {
        method: 'POST',
        credentials: 'include',
        headers: hdrs,
        body: bodyJson,
      });
      const text = await r.text();
      return { status: r.status, text };
    } catch (e) {
      return { error: e.message };
    }
  }, GRID_ENDPOINT, headers, bodyStr);

  if (result.error) throw new Error(`fetch error: ${result.error}`);
  if (result.status !== 200) throw new Error(`HTTP ${result.status}: ${(result.text || '').slice(0, 300)}`);
  let parsed;
  try { parsed = JSON.parse(result.text); }
  catch (e) { throw new Error(`invalid JSON from grid: ${result.text.slice(0, 300)}`); }
  return parsed;
}

// ─── Step 3: Fetch detail page and extract MARN, email, phone, postcode ───────
async function fetchDetail(detailPage, contactId) {
  const url = DETAIL_URL_BASE + encodeURIComponent(contactId);
  await detailPage.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
  // Give SSR a moment (actual data is already in HTML server-side)
  await sleep(300);

  const info = await detailPage.evaluate(() => {
    const out = {};
    // MARN + status
    const labels = [...document.querySelectorAll('.resgister-label, .register-label')];
    const values = [...document.querySelectorAll('.resgister-value, .register-value')];
    for (let i = 0; i < Math.min(labels.length, values.length); i++) {
      const k = labels[i].textContent.trim().replace(/[:\s]+$/, '');
      const v = values[i].textContent.trim();
      if (/registration number/i.test(k)) out.marn = v;
      if (/current status/i.test(k)) out.status = v;
    }

    // Contact details table
    document.querySelectorAll('table tr').forEach(tr => {
      const tds = tr.querySelectorAll('td');
      if (tds.length < 2) return;
      const k = tds[0].textContent.trim();
      const v = tds[1].textContent.trim();
      if (!k || !v) return;
      if (/^phone$/i.test(k)) out.phone = v.replace(/\s+/g, ' ').trim();
      else if (/^email/i.test(k)) {
        const mail = tds[1].querySelector('a[href^="mailto:"]');
        out.email = mail ? mail.getAttribute('href').replace(/^mailto:/i, '').trim() : v;
      }
      else if (/^business name/i.test(k)) out.business_name_detail = v;
      else if (/^business address/i.test(k)) out.address = v;
      else if (/^website/i.test(k)) {
        const a = tds[1].querySelector('a[href]');
        out.website = a ? a.getAttribute('href') : v;
      }
      else if (/^first name/i.test(k)) out.first_name_detail = v;
      else if (/^family name|^last name|^surname/i.test(k)) out.last_name_detail = v;
    });

    return out;
  });

  return info;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
(async () => {
  log(`MARA Australia scraper starting (TEST=${TEST_MODE}, MAX=${MAX_RECORDS}, NO_DETAILS=${NO_DETAILS})`);
  const progress = loadProgress() || { page: 1, doneIds: [], records: [] };
  const doneIds = new Set(progress.doneIds || []);
  const collected = progress.records || [];
  log(`resume: ${collected.length} records, starting at grid page ${progress.page}`);

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await page.setViewport({ width: 1280, height: 900 });

    // (a) Puppeteer OK
    log('(a) Puppeteer launched OK');

    // Capture initial grid request
    const captured = await captureGridRequest(page);
    log('(b) Portal page loaded, grid rendered — captured POST body + __RequestVerificationToken');
    log(`(c) API endpoint identified: POST ${GRID_ENDPOINT}`);

    // Second page (detail) — reuse same browser cookies
    const detailPage = await browser.newPage();
    await detailPage.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await detailPage.setRequestInterception(true);
    detailPage.on('request', (req) => {
      const rt = req.resourceType();
      if (rt === 'image' || rt === 'font' || rt === 'media' || rt === 'stylesheet') return req.abort();
      return req.continue();
    });

    // Iterate grid pages
    let gridPage = progress.page || 1;
    let totalItems = null, totalPages = null;
    let newThisRun = 0;

    while (collected.length < MAX_RECORDS) {
      let data;
      try {
        data = await fetchGridPage(page, captured, gridPage);
      } catch (e) {
        log(`grid page ${gridPage} failed: ${e.message}. Refreshing token...`);
        await sleep(3000);
        try {
          const refreshed = await captureGridRequest(page);
          Object.assign(captured, refreshed);
          data = await fetchGridPage(page, captured, gridPage);
        } catch (e2) {
          log(`retry failed: ${e2.message}. Stopping.`);
          break;
        }
      }

      if (totalItems === null) {
        totalItems = data.ItemCount || 0;
        totalPages = data.PageCount || 0;
        log(`(e) Estimated total: ${totalItems} records across ${totalPages} pages`);
      }

      const records = (data.Records || []).map(parseRecord);
      if (!records.length) {
        log(`grid page ${gridPage} returned zero records — done.`);
        break;
      }

      for (const r of records) {
        if (!r.contactid || doneIds.has(r.contactid)) continue;

        let detail = {};
        if (!NO_DETAILS) {
          try {
            detail = await fetchDetail(detailPage, r.contactid);
          } catch (e) {
            log(`  detail fail ${r.contactid}: ${e.message}`);
          }
          await sleep(DETAIL_DELAY_MS);
        }

        const addr = parseAddressBlock(detail.address || '');
        const row = {
          first_name: detail.first_name_detail || r.first_name || '',
          last_name: detail.last_name_detail || r.last_name || '',
          marn: detail.marn || '',
          business_name: detail.business_name_detail || r.business_name || '',
          email: detail.email || '',
          phone: detail.phone || '',
          city: r.city || addr.city || '',
          state: r.state || addr.state || '',
          postcode: addr.postcode || '',
          country: r.country || 'Australia',
          niche: 'migration agent',
          source: 'mara_australia',
        };
        collected.push(row);
        doneIds.add(r.contactid);
        newThisRun++;

        if (collected.length <= 10 || collected.length % 25 === 0) {
          log(`  [${collected.length}/${totalItems || '?'}] ${row.first_name} ${row.last_name} | MARN=${row.marn} | ${row.email || '-'} | ${row.city}, ${row.state}`);
        }
        if (collected.length >= MAX_RECORDS) break;
      }

      // Flush progress
      saveProgress({ page: gridPage, doneIds: [...doneIds], records: collected });
      writeCsv(collected);

      if (!data.MoreRecords) {
        log(`grid MoreRecords=false — final page reached (${gridPage}).`);
        break;
      }
      if (totalPages && gridPage >= totalPages) {
        log(`reached reported PageCount (${totalPages}).`);
        break;
      }

      gridPage++;
      await sleep(API_DELAY_MS);
    }

    writeCsv(collected);
    saveProgress({ page: gridPage, doneIds: [...doneIds], records: collected });

    // (d) First 10 records report
    const sample = collected.slice(0, 10);
    log('(d) First records parsed (sample of up to 10):');
    sample.forEach((r, i) => log(`  ${i + 1}. ${r.first_name} ${r.last_name} | MARN=${r.marn || '-'} | ${r.email || '-'} | ${r.phone || '-'} | ${r.business_name} | ${r.city}, ${r.state} ${r.postcode}`));

    log(`\nDone. ${collected.length} records collected (${newThisRun} new this run). CSV: ${OUTPUT_CSV}`);
  } catch (e) {
    console.error('FATAL:', e);
    process.exitCode = 1;
  } finally {
    if (browser) await browser.close();
  }
})();
