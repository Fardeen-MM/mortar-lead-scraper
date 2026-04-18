#!/usr/bin/env node
/**
 * Immigration Podcast Host Email Harvester.
 *
 * Searches iTunes Podcasts for immigration/visa/study-abroad keywords across
 * English-market country storefronts, then fetches each podcast's RSS feed and
 * extracts the itunes:owner email + name.
 *
 * Every immigration podcast host is almost certainly:
 *   - An immigration consultant / lawyer
 *   - OR a study-abroad expert
 *   - OR an expat coach who runs a service
 * = PERFECT webinar funnel prospect, HIGH INTENT.
 *
 * Method:
 *   1. Query iTunes Search API for each keyword × each country (~200 results max per query)
 *   2. Collect unique feed URLs
 *   3. Fetch each RSS feed (polite 500ms delay)
 *   4. Extract itunes:owner/itunes:email + itunes:owner/itunes:name
 *   5. Fallback: managingEditor, webMaster, generic email regex
 *   6. Dedupe by email, save CSV
 *
 * Expected yield: 500-3000 immigration-podcast host emails
 *
 * Usage:
 *   node scripts/scrape-immigration-podcasts.js
 *   node scripts/scrape-immigration-podcasts.js --test (only 5 keywords × 2 countries)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE = !!args.test;
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'immigration-podcast-hosts.csv');
const FEEDS_FILE = path.join(OUTPUT_DIR, 'immigration-podcast-feeds.json');

// iTunes search keywords (immigration + adjacent high-TAM)
const KEYWORDS = [
  'immigration', 'visa', 'green card', 'citizenship',
  'move to canada', 'move to usa', 'move to uk', 'move to australia',
  'express entry', 'permanent residency', 'PR canada',
  'study abroad', 'international student', 'study in canada',
  'migrate', 'migration', 'expat life',
  'work visa', 'H1B', 'skilled worker visa',
  'IELTS', 'TOEFL', 'language test',
  'NCLEX', 'nursing abroad', 'credential evaluation',
  'digital nomad', 'living abroad',
];

// English-market country storefronts
const COUNTRIES = [
  'us', 'ca', 'gb', 'au', 'nz', 'ie',  // primary English markets
  'in', 'pk', 'bd', 'ph', 'ng', 'za',  // large emigrant sources
  'ae', 'sg', 'hk',                     // migration hubs
];

const TEST_KEYWORDS = KEYWORDS.slice(0, 5);
const TEST_COUNTRIES = ['us', 'ca'];

const activeKeywords = TEST_MODE ? TEST_KEYWORDS : KEYWORDS;
const activeCountries = TEST_MODE ? TEST_COUNTRIES : COUNTRIES;

// ── Helpers ────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

function httpGet(url, maxRedirects = 3, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/xml, application/rss+xml, */*',
      },
      timeout: timeoutMs,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
        let loc = res.headers.location;
        if (loc.startsWith('/')) {
          const p = new URL(url);
          loc = p.protocol + '//' + p.host + loc;
        }
        return httpGet(loc, maxRedirects - 1, timeoutMs).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function escapeCSV(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function extractDomain(email) {
  if (!email || !email.includes('@')) return '';
  return email.split('@')[1].toLowerCase();
}

// ── Phase 1: Discover feed URLs via iTunes Search API ─────────────────
async function discoverFeeds() {
  console.log(`\n=== Phase 1: iTunes Search (${activeKeywords.length} keywords × ${activeCountries.length} countries) ===`);

  const feeds = new Map(); // feedUrl → {keyword, country, trackName, collectionName, artistName, trackId}

  let queries = 0;
  const total = activeKeywords.length * activeCountries.length;

  for (const keyword of activeKeywords) {
    for (const country of activeCountries) {
      queries++;
      const term = encodeURIComponent(keyword);
      const url = `https://itunes.apple.com/search?media=podcast&term=${term}&country=${country}&limit=200`;

      try {
        const json = await httpGet(url, 3, 15000);
        const data = JSON.parse(json);
        const results = data.results || [];

        for (const r of results) {
          if (!r.feedUrl) continue;
          const feedUrl = r.feedUrl.trim();
          if (!feeds.has(feedUrl)) {
            feeds.set(feedUrl, {
              feedUrl,
              collectionName: r.collectionName || '',
              artistName: r.artistName || '',
              trackId: r.trackId || '',
              discoveryKeyword: keyword,
              discoveryCountry: country,
              primaryGenre: r.primaryGenreName || '',
              artworkUrl: r.artworkUrl100 || '',
            });
          }
        }

        if (queries % 10 === 0 || queries === total) {
          console.log(`  [${queries}/${total}] ${keyword} (${country}): ${results.length} results, ${feeds.size} unique feeds so far`);
        }

        // iTunes rate limit: 20 req/min → 3 sec delay
        await sleep(3100);
      } catch (err) {
        console.error(`  ERROR ${keyword}/${country}: ${err.message}`);
        await sleep(5000);
      }
    }
  }

  console.log(`\nPhase 1 complete: ${feeds.size} unique podcast feeds discovered`);

  // Save feed list for resumability
  fs.writeFileSync(FEEDS_FILE, JSON.stringify([...feeds.values()], null, 2));
  console.log(`Saved feed list to ${FEEDS_FILE}`);

  return [...feeds.values()];
}

// ── Phase 2: Fetch each RSS feed and extract owner email ──────────────
async function extractOwnerInfo(feed, delayMs = 500) {
  try {
    const xml = await httpGet(feed.feedUrl, 3, 15000);

    // Extract itunes:owner block
    const ownerBlockMatch = xml.match(/<itunes:owner[^>]*>([\s\S]*?)<\/itunes:owner>/i);
    let email = '', ownerName = '';

    if (ownerBlockMatch) {
      const block = ownerBlockMatch[1];
      const emailMatch = block.match(/<itunes:email[^>]*>\s*(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?\s*<\/itunes:email>/i);
      const nameMatch = block.match(/<itunes:name[^>]*>\s*(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?\s*<\/itunes:name>/i);
      if (emailMatch) email = emailMatch[1].trim();
      if (nameMatch) ownerName = nameMatch[1].trim();
    }

    // Fallback: managingEditor "email (Name)" format
    if (!email) {
      const me = xml.match(/<managingEditor>\s*([^<]+)\s*<\/managingEditor>/i);
      if (me) {
        const mev = me[1].trim();
        const m = mev.match(/([^\s(]+@[^\s)]+)\s*(?:\(([^)]+)\))?/);
        if (m) {
          email = m[1];
          if (!ownerName && m[2]) ownerName = m[2];
        }
      }
    }

    // Fallback: webMaster
    if (!email) {
      const wm = xml.match(/<webMaster>\s*([^<]+)\s*<\/webMaster>/i);
      if (wm) {
        const wmv = wm[1].trim();
        const m = wmv.match(/([^\s(]+@[^\s)]+)/);
        if (m) email = m[1];
      }
    }

    // Fallback: first plausible email in channel description/copyright
    if (!email) {
      const channelHead = xml.split('<item')[0] || xml.slice(0, 20000);
      const generic = channelHead.match(/\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/);
      if (generic) email = generic[1];
    }

    // Extract additional info: link, description (first 300 chars)
    const linkMatch = xml.match(/<channel>[\s\S]*?<link>\s*(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?\s*<\/link>/i);
    const link = linkMatch ? linkMatch[1].trim() : '';

    const descMatch = xml.match(/<description>\s*(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?\s*<\/description>/i);
    let description = descMatch ? descMatch[1].trim().replace(/<[^>]+>/g, '') : '';
    if (description.length > 300) description = description.slice(0, 297) + '...';

    return {
      email: email.toLowerCase(),
      ownerName,
      link,
      description,
    };
  } catch (err) {
    return { email: '', ownerName: '', link: '', description: '', error: err.message };
  }
}

async function harvestFeeds(feeds) {
  console.log(`\n=== Phase 2: Parsing ${feeds.length} RSS feeds ===`);

  const leads = [];
  let done = 0, withEmail = 0, errors = 0;
  const startTime = Date.now();

  // Save incrementally
  const saveCsv = () => {
    const cols = [
      'email', 'first_name', 'last_name', 'company', 'podcast_name',
      'primary_genre', 'website', 'description', 'domain',
      'discovery_keyword', 'discovery_country', 'feed_url',
    ];
    const rows = [cols.join(',')];
    for (const l of leads) {
      rows.push(cols.map(c => escapeCSV(l[c] || '')).join(','));
    }
    fs.writeFileSync(OUTPUT_FILE, rows.join('\n') + '\n');
  };

  for (const feed of feeds) {
    done++;
    try {
      const info = await extractOwnerInfo(feed);
      if (info.email) {
        withEmail++;
        // Parse first/last from ownerName or artistName
        const name = info.ownerName || feed.artistName || '';
        const parts = name.split(/\s+/).filter(Boolean);
        const first = parts[0] || '';
        const last = parts.slice(1).join(' ') || '';

        leads.push({
          email: info.email,
          first_name: first,
          last_name: last,
          company: feed.artistName || '',
          podcast_name: feed.collectionName,
          primary_genre: feed.primaryGenre,
          website: info.link,
          description: info.description,
          domain: extractDomain(info.email),
          discovery_keyword: feed.discoveryKeyword,
          discovery_country: feed.discoveryCountry,
          feed_url: feed.feedUrl,
        });
      } else if (info.error) {
        errors++;
      }

      if (done % 25 === 0 || done === feeds.length) {
        const elapsed = Math.round((Date.now() - startTime) / 1000);
        const rate = Math.round(done / (elapsed || 1) * 60);
        const remaining = Math.round((feeds.length - done) / (rate / 60));
        console.log(`  [${done}/${feeds.length}] emails=${withEmail} errors=${errors} rate=${rate}/min ~${remaining}s left`);
      }

      if (done % 100 === 0) saveCsv();

      await sleep(300); // polite
    } catch (err) {
      errors++;
    }
  }

  saveCsv();
  console.log(`\nPhase 2 complete: ${withEmail} emails from ${feeds.length} feeds (${errors} errors)`);
  return leads;
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== Immigration Podcast Host Harvester ===');
  console.log(`Mode: ${TEST_MODE ? 'TEST' : 'FULL'}`);
  console.log(`Keywords: ${activeKeywords.length}, Countries: ${activeCountries.length}`);

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  let feeds;
  // Resume: if we already have a feeds file, skip discovery
  if (!args.fresh && fs.existsSync(FEEDS_FILE)) {
    try {
      feeds = JSON.parse(fs.readFileSync(FEEDS_FILE));
      console.log(`Resume: loaded ${feeds.length} feeds from ${FEEDS_FILE}`);
    } catch {
      feeds = await discoverFeeds();
    }
  } else {
    feeds = await discoverFeeds();
  }

  if (!feeds.length) {
    console.error('No feeds discovered. Exiting.');
    process.exit(1);
  }

  const leads = await harvestFeeds(feeds);

  // Summary
  const byKeyword = {};
  for (const l of leads) {
    byKeyword[l.discovery_keyword] = (byKeyword[l.discovery_keyword] || 0) + 1;
  }

  console.log('\n=== RESULTS ===');
  console.log(`Total feeds:   ${feeds.length}`);
  console.log(`With email:    ${leads.length} (${Math.round(leads.length / feeds.length * 100)}%)`);
  console.log(`Unique domains: ${new Set(leads.map(l => l.domain)).size}`);
  console.log(`\nTop keywords by lead count:`);
  const sorted = Object.entries(byKeyword).sort((a, b) => b[1] - a[1]);
  for (const [k, n] of sorted.slice(0, 15)) {
    console.log(`  ${k}: ${n}`);
  }
  console.log(`\nOutput: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
