#!/usr/bin/env node
/**
 * International Franchise Association (IFA) — franchisor directory + member directory.
 * Uses their public Algolia search API (credentials extracted from franchise.org).
 * Algolia caps ~1000 pages * hitsPerPage; we pull hits in batches up to nbHits.
 * Two indexes: franprod_franchises (~1,037 franchise brands) + franprod_members (~2,110 supplier members).
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'ifa-franchisors.csv');

const ALGOLIA_HOST = '01jx580af9-dsn.algolia.net';
const ALGOLIA_APP_ID = '01JX580AF9';
const ALGOLIA_API_KEY = '5af69fce67b0477681b636c4f68869b7';

function httpPostJson(pathname, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: ALGOLIA_HOST,
      path: pathname,
      method: 'POST',
      headers: {
        'X-Algolia-API-Key': ALGOLIA_API_KEY,
        'X-Algolia-Application-Id': ALGOLIA_APP_ID,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        'Content-Length': Buffer.byteLength(payload),
      },
      timeout: 25000,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf-8');
        if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}: ${text.slice(0, 200)}`));
        try { resolve(JSON.parse(text)); } catch (e) { reject(new Error('Invalid JSON: ' + text.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(payload);
    req.end();
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function decodeHtml(s) {
  if (!s) return '';
  return String(s).replace(/&amp;/g, '&').replace(/&#038;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&rsquo;/g, "'").replace(/&nbsp;/g, ' ');
}

async function fetchQuery(indexName, query, filters, outStream, sourceSlug, recordType, seen) {
  const HITS_PER_PAGE = 1000;
  const body = { query, hitsPerPage: HITS_PER_PAGE, page: 0 };
  if (filters) body.filters = filters;
  let data;
  try {
    data = await httpPostJson(`/1/indexes/${indexName}/query`, body);
  } catch (err) {
    console.error(`  error q="${query}" f="${filters||''}": ${err.message}`);
    return 0;
  }
  const hits = data.hits || [];
  let added = 0;
  for (const h of hits) {
    const id = h.objectID;
    if (seen.has(id)) continue;
    seen.add(id);

    const title = decodeHtml(h.title || '');
    const permalink = h.permalink || '';
    const description = decodeHtml(Array.isArray(h.description) ? h.description[0] : (h.description || ''));
    const category = decodeHtml((h.franchise_category || [])[0] || (h.member_type || [])[0] || '');
    const subcategory = decodeHtml((h.franchise_subcategory || []).join('; '));
    const minInv = h.min_total_investment || '';
    const maxInv = h.max_total_investment || '';
    const startupMin = h.startup_costs_min || '';
    const advertisor = decodeHtml((h.advertisor_type || [])[0] || '');
    const urlMatch = description.match(/(?:https?:\/\/)?(?:www\.)?([a-z0-9][a-z0-9-]*\.[a-z]{2,}(?:\/[^\s,.)"'<]*)?)\b/i);
    const derivedWebsite = urlMatch ? urlMatch[1] : '';

    const row = {
      record_type: recordType,
      name: title,
      category,
      subcategory,
      min_total_investment: minInv,
      max_total_investment: maxInv,
      startup_costs_min: startupMin,
      advertisor_tier: advertisor,
      permalink,
      derived_website: derivedWebsite,
      description: description.slice(0, 1000),
      object_id: id,
      niche: recordType === 'franchisor' ? 'franchise' : (recordType === 'supplier_member' ? 'franchise supplier' : recordType.replace('_', ' ')),
      country: 'US',
      source: sourceSlug,
    };
    outStream.write(Object.values(row).map(escapeCsv).join(',') + '\n');
    added++;
  }
  return added;
}

async function fetchFranchises(outStream, seen) {
  console.log(`\n--- franprod_franchises (1037 reported) ---`);
  // Use facet values to shard queries below 1000 cap
  const data = await httpPostJson(`/1/indexes/franprod_franchises/query`, {
    query: '', hitsPerPage: 0, facets: ['franchise_category'],
  });
  const categories = Object.keys((data && data.facets && data.facets.franchise_category) || {});
  console.log(`  ${categories.length} categories to shard`);

  let total = 0;
  // First: unfiltered pull
  const baseAdded = await fetchQuery('franprod_franchises', '', null, outStream, 'ifa_franchisors', 'franchisor', seen);
  total += baseAdded;
  console.log(`  base: +${baseAdded} (seen ${seen.size})`);

  // Any extras via facet filter
  for (const cat of categories) {
    // Algolia filter syntax uses "attr:\"value\""
    const filt = `franchise_category:"${cat.replace(/&amp;/g, '&').replace(/"/g, '\\"')}"`;
    const added = await fetchQuery('franprod_franchises', '', filt, outStream, 'ifa_franchisors', 'franchisor', seen);
    if (added > 0) {
      console.log(`  [${cat}]: +${added} (seen ${seen.size})`);
      total += added;
    }
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`  franchises total unique: ${total}`);
  return total;
}

async function fetchMembers(outStream, seen) {
  console.log(`\n--- franprod_members (2110 reported) ---`);
  const types = ['Franchisor', 'Supplier', 'Organization'];
  let total = 0;
  for (const t of types) {
    const filt = `member_type:"${t}"`;
    const recordType = t === 'Supplier' ? 'supplier_member' : (t === 'Franchisor' ? 'franchisor_member' : 'org_member');
    const added = await fetchQuery('franprod_members', '', filt, outStream, 'ifa_members', recordType, seen);
    console.log(`  [${t}]: +${added} (seen ${seen.size})`);
    total += added;
    await new Promise(r => setTimeout(r, 200));
  }

  // Fallback: also paginate by first letter to catch any rows missed (e.g. Franchisor=1304, needs sharding)
  const letters = 'abcdefghijklmnopqrstuvwxyz0123456789';
  for (const l of letters) {
    for (const t of types) {
      const filt = `member_type:"${t}"`;
      const added = await fetchQuery('franprod_members', l, filt, outStream, 'ifa_members', t === 'Supplier' ? 'supplier_member' : (t === 'Franchisor' ? 'franchisor_member' : 'org_member'), seen);
      if (added > 0) total += added;
      await new Promise(r => setTimeout(r, 100));
    }
  }

  console.log(`  members total unique: ${total}`);
  return total;
}

async function main() {
  console.log('=== IFA (franchise.org) Algolia Scraper ===');

  const cols = ['record_type','name','category','subcategory','min_total_investment','max_total_investment','startup_costs_min','advertisor_tier','permalink','derived_website','description','object_id','niche','country','source'];
  const out = fs.createWriteStream(OUTPUT);
  out.write(cols.join(',') + '\n');

  const seen = new Set();
  const a = await fetchFranchises(out, seen);
  const seen2 = new Set();
  const b = await fetchMembers(out, seen2);

  out.end();
  await new Promise(r => out.on('close', r));

  console.log(`\n=== DONE — ${a} franchisors + ${b} supplier members = ${a + b} total ===`);
  console.log(`Wrote: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
