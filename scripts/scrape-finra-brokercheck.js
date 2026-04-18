#!/usr/bin/env node
/**
 * FINRA BrokerCheck API scraper
 *
 * Endpoint: https://api.brokercheck.finra.org/search/firm?query={Q}&hl=true&nrows=100&start={N}&r=25&sort=score+desc
 * Pagination is capped at 10,000 results per query (Elasticsearch default).
 * Strategy: iterate all single letters a-z and 0-9; for any query exceeding 10K results,
 * expand to 2-letter combinations (aa, ab, ac, ...).
 *
 * Output columns: firm_name, firm_id, bd_sec_number, city, state, country, firm_type, niche, source
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUT_CSV = path.join(OUTPUT_DIR, 'finra-brokercheck.csv');
const STATE_FILE = path.join(OUTPUT_DIR, 'finra-brokercheck.state.json');
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const NROWS = 100; // max per page
const MAX_OFFSET = 10000 - NROWS; // 9900 is the last safe start

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function csvEscape(v) {
    if (v === null || v === undefined) return '';
    const s = String(v);
    if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
        return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
}

function httpGetJson(url, attempt = 0) {
    return new Promise((resolve, reject) => {
        const u = new URL(url);
        const req = https.request({
            hostname: u.hostname,
            port: 443,
            path: u.pathname + u.search,
            method: 'GET',
            headers: {
                'User-Agent': UA,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://brokercheck.finra.org',
                'Referer': 'https://brokercheck.finra.org/',
                'Connection': 'keep-alive',
            },
            timeout: 30000,
        }, (res) => {
            const chunks = [];
            res.on('data', (c) => chunks.push(c));
            res.on('end', () => {
                const body = Buffer.concat(chunks).toString('utf8');
                if (res.statusCode !== 200) {
                    if (attempt < 3) {
                        setTimeout(() => resolve(httpGetJson(url, attempt + 1)), 2000 * (attempt + 1));
                        return;
                    }
                    return reject(new Error(`HTTP ${res.statusCode} for ${url}: ${body.substring(0, 200)}`));
                }
                try { resolve(JSON.parse(body)); }
                catch (e) { reject(new Error(`parse: ${e.message} | body: ${body.substring(0, 300)}`)); }
            });
        });
        req.on('error', (e) => {
            if (attempt < 3) setTimeout(() => resolve(httpGetJson(url, attempt + 1)), 2000 * (attempt + 1));
            else reject(e);
        });
        req.on('timeout', () => {
            req.destroy();
            if (attempt < 3) setTimeout(() => resolve(httpGetJson(url, attempt + 1)), 2000 * (attempt + 1));
            else reject(new Error('timeout'));
        });
        req.end();
    });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function loadState() {
    try {
        return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (_) {
        return { completed_queries: [], seen_ids: [] };
    }
}

function saveState(state) {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function appendCsvRow(row) {
    const cols = ['firm_name', 'firm_id', 'bd_sec_number', 'city', 'state', 'country', 'firm_type', 'niche', 'source'];
    if (!fs.existsSync(OUT_CSV)) {
        fs.writeFileSync(OUT_CSV, cols.join(',') + '\n');
    }
    fs.appendFileSync(OUT_CSV, cols.map((c) => csvEscape(row[c])).join(',') + '\n');
}

function parseAddress(raw) {
    // firm_ia_address_details is a JSON-encoded string
    if (!raw) return { city: '', state: '', country: '' };
    if (typeof raw === 'string') {
        try { raw = JSON.parse(raw); } catch (_) { return { city: '', state: '', country: '' }; }
    }
    if (Array.isArray(raw)) raw = raw[0] || {};
    const oa = raw.officeAddress || raw.mainOfficeAddress || raw;
    if (!oa || typeof oa !== 'object') return { city: '', state: '', country: '' };
    return {
        city: (oa.city || '').trim(),
        state: (oa.state || '').trim(),
        country: (oa.country || '').trim(),
    };
}

function firmTypeFromHit(s) {
    const t = [];
    if (s.firm_bd_sec_number || s.firm_bd_full_sec_number || s.firm_scope) t.push('Broker-Dealer');
    if (s.firm_ia_sec_number || s.firm_ia_full_sec_number || s.firm_ia_scope) t.push('Investment Adviser');
    return t.join(' + ') || 'Unknown';
}

function hitToRow(hit) {
    const s = hit._source || {};
    const addr = parseAddress(s.firm_ia_address_details);
    return {
        firm_name: (s.firm_name || '').trim(),
        firm_id: s.firm_source_id || '',
        bd_sec_number: s.firm_bd_full_sec_number || s.firm_bd_sec_number || s.firm_ia_full_sec_number || s.firm_ia_sec_number || '',
        city: addr.city,
        state: addr.state,
        country: addr.country,
        firm_type: firmTypeFromHit(s),
        niche: 'Securities Firm (Broker-Dealer / Investment Adviser)',
        source: 'FINRA BrokerCheck',
    };
}

async function queryTotal(query) {
    const url = `https://api.brokercheck.finra.org/search/firm?query=${encodeURIComponent(query)}&hl=true&nrows=1&start=0&r=25&sort=score+desc`;
    const j = await httpGetJson(url);
    return (j.hits && j.hits.total) || 0;
}

async function harvestQuery(query, seen, onRow) {
    // Returns { fetched, added, total, exceededCap }
    let fetched = 0, added = 0, total = 0, exceededCap = false;

    for (let start = 0; start <= MAX_OFFSET; start += NROWS) {
        const url = `https://api.brokercheck.finra.org/search/firm?query=${encodeURIComponent(query)}&hl=true&nrows=${NROWS}&start=${start}&r=25&sort=score+desc`;
        let j;
        try { j = await httpGetJson(url); }
        catch (e) { console.error(`  [err] ${query} @ ${start}: ${e.message}`); break; }
        total = (j.hits && j.hits.total) || 0;
        const hits = (j.hits && j.hits.hits) || [];
        if (start === 0) {
            console.log(`  query="${query}" total=${total}`);
            if (total > 10000) exceededCap = true;
        }
        if (hits.length === 0) break;
        for (const h of hits) {
            const row = hitToRow(h);
            if (!row.firm_id) continue;
            if (seen.has(row.firm_id)) continue;
            seen.add(row.firm_id);
            onRow(row);
            added++;
        }
        fetched += hits.length;
        if (hits.length < NROWS) break;
        if (start + NROWS >= total) break;
        await sleep(700 + Math.floor(Math.random() * 600));
    }
    return { fetched, added, total, exceededCap };
}

function twoLetterQueries() {
    const base = 'abcdefghijklmnopqrstuvwxyz'.split('');
    const out = [];
    for (const a of base) for (const b of base) out.push(a + b);
    // digits too
    const digits = '0123456789'.split('');
    for (const d of digits) out.push(d);
    for (const d of digits) for (const c of base) out.push(d + c);
    return out;
}

async function run() {
    const state = loadState();
    const seen = new Set(state.seen_ids);

    // Phase 1: try single letters a-z and 0-9 first
    const singleQueries = 'abcdefghijklmnopqrstuvwxyz0123456789'.split('');
    const needsExpansion = [];

    for (const q of singleQueries) {
        const key = `single:${q}`;
        if (state.completed_queries.includes(key)) {
            console.log(`[skip] ${key}`);
            continue;
        }
        console.log(`[query] ${q} (single letter)`);
        let res;
        try {
            res = await harvestQuery(q, seen, (row) => appendCsvRow(row));
        } catch (e) {
            console.error(`[error] ${q}: ${e.message}`);
            await sleep(5000);
            continue;
        }
        console.log(`  → +${res.added} new, ${seen.size} total seen`);
        if (res.exceededCap) {
            console.log(`  ⚠ ${q} total ${res.total} > 10K, will expand to 2-letter`);
            needsExpansion.push(q);
        }
        state.completed_queries.push(key);
        state.seen_ids = Array.from(seen);
        saveState(state);
        await sleep(1200);
    }

    // Phase 2: two-letter expansion for letters that hit the 10K cap
    for (const prefix of needsExpansion) {
        for (const c of 'abcdefghijklmnopqrstuvwxyz0123456789'.split('')) {
            const q = prefix + c;
            const key = `pair:${q}`;
            if (state.completed_queries.includes(key)) {
                console.log(`[skip] ${key}`);
                continue;
            }
            console.log(`[query] ${q} (expansion of ${prefix})`);
            let res;
            try {
                res = await harvestQuery(q, seen, (row) => appendCsvRow(row));
            } catch (e) {
                console.error(`[error] ${q}: ${e.message}`);
                await sleep(5000);
                continue;
            }
            console.log(`  → +${res.added} new, ${seen.size} total seen`);
            state.completed_queries.push(key);
            state.seen_ids = Array.from(seen);
            saveState(state);
            await sleep(1000);
        }
    }

    console.log(`[done] ${seen.size} unique firms total`);
}

if (require.main === module) {
    run().catch((e) => { console.error(e); process.exit(1); });
}

module.exports = { hitToRow, parseAddress };
