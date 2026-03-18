#!/usr/bin/env node
/**
 * BuildZoom Fast Scrape — Search pages only (no profile fetching)
 * Collects profile URLs + basic info from search listings, skips individual profile pages.
 * Profile URLs can be used later for batch enrichment.
 *
 * 7 trades x 15 cities x 5 pages = 525 search pages
 * Run: node scripts/run-buildzoom-fast.js
 */
const cheerio = require('cheerio');
const fs = require('fs');
const https = require('https');
const { normalizeTrade } = require('../lib/normalizer');
const { log } = require('../lib/logger');

const BASE_URL = 'https://www.buildzoom.com';

function buildSearchUrl(trade, city, page) {
  const location = city.replace(/,\s*/g, ' ').trim();
  const keywords = `${trade} ${location}`.trim();
  const encoded = encodeURIComponent(keywords);
  let url = `${BASE_URL}/search?keywords=${encoded}`;
  if (page > 1) url += `&page=${page}`;
  return url;
}

function randomDelay(min, max) {
  return new Promise(resolve => setTimeout(resolve, min + Math.random() * (max - min)));
}

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

function httpGet(url) {
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 30000,
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return httpGet(res.headers.location).then(resolve).catch(reject);
      }
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

/**
 * Parse search results page — extract contractor info from listing cards.
 * Also try to extract basic info (name, location) from the search listing text.
 */
function parseSearchPage(html) {
  const $ = cheerio.load(html);
  const results = [];
  const seen = new Set();

  // Extract total count
  let totalCount = 0;
  const text = $.text();
  const countMatch = text.match(/of the best\s+([\d,]+)\s+experts/i) || text.match(/([\d,]+)\s+experts/i);
  if (countMatch) totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);

  // Find all /contractor/ links
  $('a[href^="/contractor/"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const match = href.match(/^\/contractor\/([a-z0-9][\w-]+)$/i);
    if (!match) return;

    const slug = match[1].toLowerCase();
    if (['search', 'new', 'login', 'signup', 'register', 'claim'].includes(slug)) return;
    if (seen.has(slug)) return;
    seen.add(slug);

    // Try to extract business name from link text
    const linkText = $(el).text().trim();
    let firmName = '';
    if (linkText && linkText.length >= 3 && linkText.length <= 200 &&
        !/^(view|read|see|get|search|find|browse|compare)/i.test(linkText)) {
      firmName = linkText;
    }

    // Walk up to find containing block for address/phone
    let $container = $(el).parent();
    for (let i = 0; i < 6; i++) {
      const t = $container.text();
      if (/\(\d{3}\)\s*\d{3}-\d{4}/.test(t) || /[A-Z]{2}\s+\d{5}/.test(t)) break;
      const $p = $container.parent();
      if ($p.length === 0) break;
      $container = $p;
    }

    const containerText = $container.text();

    // Extract phone
    let phone = '';
    const phoneMatch = containerText.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
    if (phoneMatch) phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;

    // Extract city, state from "City, ST ZIP" pattern
    let city = '', state = '', zip = '';
    const addrMatch = containerText.match(/([A-Za-z\s.]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
    if (addrMatch) {
      city = addrMatch[1].trim();
      state = addrMatch[2];
      zip = addrMatch[3];
    }

    // Try to extract from slug if no city/state
    if (!city) {
      // Slugs often end with city-state like "acme-roofing-miami-fl"
      const slugParts = slug.split('-');
      if (slugParts.length >= 2) {
        const lastPart = slugParts[slugParts.length - 1].toUpperCase();
        if (/^[A-Z]{2}$/.test(lastPart)) {
          state = lastPart;
          city = slugParts[slugParts.length - 2].replace(/^\w/, c => c.toUpperCase());
        }
      }
    }

    const profileUrl = `${BASE_URL}/contractor/${slug}`;
    results.push({
      first_name: '',
      last_name: '',
      firm_name: firmName,
      phone,
      email: '',
      website: '',
      city,
      state,
      zip,
      bar_number: '',
      bar_status: '',
      profile_url: profileUrl,
      source: 'buildzoom_contractors',
    });
  });

  return { results, totalCount };
}

(async () => {
  const leads = [];
  const trades = ['roofing', 'painting', 'plumbing', 'hvac', 'electrical', 'general contractor', 'landscaping'];
  const cities = ['Miami, FL', 'Houston, TX', 'Dallas, TX', 'Atlanta, GA', 'Phoenix, AZ', 'Tampa, FL', 'Orlando, FL', 'Charlotte, NC', 'Denver, CO', 'Nashville, TN', 'San Antonio, TX', 'Jacksonville, FL', 'Austin, TX', 'Las Vegas, NV', 'Raleigh, NC'];

  const startTime = Date.now();
  const seenUrls = new Set();
  let consecutive429 = 0;
  const maxPages = 3; // Conservative: 3 pages per city (75 results)

  for (const trade of trades) {
    for (const city of cities) {
      for (let page = 1; page <= maxPages; page++) {
        try {
          // Longer delays to avoid rate limiting (8-15s between requests)
          const baseDelay = consecutive429 > 0 ? 15000 : 8000;
          const maxExtraDelay = consecutive429 > 0 ? 10000 : 7000;
          await randomDelay(baseDelay, baseDelay + maxExtraDelay);

          const url = buildSearchUrl(trade, city, page);
          const res = await httpGet(url);

          if (res.statusCode === 429 || res.statusCode === 403) {
            consecutive429++;
            // Cap backoff at 5 minutes, but also skip to next city after 3 consecutive 429s
            if (consecutive429 >= 3) {
              console.log(`429/403 x${consecutive429} on ${trade} ${city} — skipping city`);
              break;
            }
            const backoff = Math.min(30000 * consecutive429, 120000);
            console.log(`429/403 on ${trade} ${city} p${page} — backing off ${(backoff/1000).toFixed(0)}s`);
            await randomDelay(backoff, backoff + 10000);
            continue;
          }

          if (res.statusCode !== 200) {
            console.log(`HTTP ${res.statusCode} on ${trade} ${city} p${page}`);
            if (res.statusCode === 404) break;
            continue;
          }

          // Reset on success
          consecutive429 = 0;

          const { results, totalCount } = parseSearchPage(res.body);

          if (results.length === 0) {
            if (page === 1) console.log(`No results: ${trade} in ${city}`);
            break;
          }

          // Dedup and add trade category
          let newCount = 0;
          for (const r of results) {
            if (seenUrls.has(r.profile_url)) continue;
            seenUrls.add(r.profile_url);
            r.trade_category = normalizeTrade(trade);
            leads.push(r);
            newCount++;
          }

          const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
          if (page === 1 && totalCount > 0) {
            console.log(`[${elapsed}s] ${trade} | ${city}: ${totalCount} total avail, +${newCount} new (${leads.length} total)`);
          } else {
            console.log(`[${elapsed}s] ${trade} | ${city} p${page}: +${newCount} new (${leads.length} total)`);
          }

          // Check if this was a partial page (last page)
          if (results.length < 20) break;

        } catch(e) {
          console.log(`Error: ${trade} ${city} p${page}: ${e.message.slice(0, 60)}`);
        }
      }
    }
  }

  // Write CSV
  const headers = 'first_name,last_name,firm_name,email,phone,website,city,state,trade_category,bar_status,profile_url,source\n';
  const rows = leads.map(l => [
    l.first_name||'', l.last_name||'', l.firm_name||'', l.email||'', l.phone||'',
    l.website||'', l.city||'', l.state||'', l.trade_category||'', l.bar_status||'',
    l.profile_url||'', l.source||'buildzoom_contractors'
  ].map(v => {
    const s = (v||'').toString().replace(/"/g, '""');
    return s.includes(',') ? '"' + s + '"' : s;
  }).join(',')).join('\n');

  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/buildzoom-massive.csv', headers + rows);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  console.log(`\n=== BUILDZOOM COMPLETE in ${elapsed}s ===`);
  console.log(`TOTAL: ${leads.length} leads (${seenUrls.size} unique)`);
  console.log(`Phone: ${leads.filter(l=>l.phone).length}`);
  console.log(`Website: ${leads.filter(l=>l.website).length}`);
  console.log(`Profile URL: ${leads.filter(l=>l.profile_url).length}`);
  console.log(`With firm name: ${leads.filter(l=>l.firm_name).length}`);
  console.log(`Output: output/buildzoom-massive.csv`);
})().catch(e => console.error(e.stack));
