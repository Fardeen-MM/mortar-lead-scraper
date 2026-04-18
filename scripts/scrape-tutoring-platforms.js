#!/usr/bin/env node
/**
 * Tutoring Platform Scraper — Multi-source scraper for course providers,
 * tutors, and training businesses.
 *
 * === SOURCE 1: italki (api.italki.com) ===
 * Open JSON API, no auth, no rate limiting
 * Endpoint: GET /api/v2/teachers?page_size=50&page={n}
 * Fields: name, country, city, timezone, sessions, rating, education, certs,
 *         teaching experience, intro video (YouTube), languages taught/spoken
 * Scale: ~2,000 unique teachers (API wraps at page 41, filters are no-ops)
 * Note: No email/phone — but names + profile URLs enable enrichment
 *
 * === SOURCE 2: Preply (preply.com) ===
 * SSR Next.js site with __NEXT_DATA__ JSON embedded in HTML
 * URL: /en/online/{subject}-tutors?page={n}  (10 per page)
 * Fields: name, country, headline, experience, rating, reviews, total lessons,
 *         active students, video intro, price, certification, profile URL
 * Scale: 39,890 English + 13,671 Spanish + 80,000+ across all subjects
 * Note: No email/phone — but profile URLs + names enable enrichment
 *
 * === DEAD ENDS (researched, not viable) ===
 * Thumbtack: Partnership API required (approval-gated)
 * Superprof: 403 Forbidden on HTTP
 * Bark.com: 403 Forbidden
 * Wyzant: Data feed requires affiliate account approval
 * ClassPass: JS SPA, needs HAR capture
 * MindBody: JS SPA, no static API
 * Care.com: Gated enrollment flow, no public directory
 * Varsity Tutors: HTML cards, no contact info, no API
 *
 * Output: output/tutoring-platforms.csv
 *
 * Usage:
 *   node scripts/scrape-tutoring-platforms.js                    # Full run (all sources)
 *   node scripts/scrape-tutoring-platforms.js --italki-only      # italki only
 *   node scripts/scrape-tutoring-platforms.js --preply-only      # Preply only
 *   node scripts/scrape-tutoring-platforms.js --test             # Test mode (2 pages/source)
 *   node scripts/scrape-tutoring-platforms.js --max-pages 100    # Limit pages per subject
 *   node scripts/scrape-tutoring-platforms.js --resume           # Resume from progress file
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ============================================================================
// CONFIG
// ============================================================================

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'tutoring-platforms.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'tutoring-platforms-progress.json');

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const REQUEST_TIMEOUT = 30000;

const CSV_HEADERS = [
  'name', 'first_name', 'last_name', 'email', 'phone', 'company',
  'website', 'city', 'state', 'country', 'source', 'subject',
  'rating', 'total_sessions', 'experience_years', 'profile_url',
  'video_url', 'headline', 'price_usd',
];

// Preply subjects to scrape (sorted by volume, largest first)
const PREPLY_SUBJECTS = [
  'english',    // 39,890
  'spanish',    // 13,671
  'chinese',    // 7,379
  'french',     // 5,743
  'math',       // 4,444
  'japanese',   // 4,421
  'german',     // 2,509
  'korean',     // 2,196
  'biology',    // 1,519
  'music',      // 1,077
  'chemistry',  // 886
  'physics',    // 778
  'art',        // 308
  'business',   // 157
  'writing',    // 118
  'photography',// 32
];

// ============================================================================
// CLI ARGS
// ============================================================================

const args = process.argv.slice(2);
const ITALKI_ONLY = args.includes('--italki-only');
const PREPLY_ONLY = args.includes('--preply-only');
const TEST_MODE = args.includes('--test');
const RESUME = args.includes('--resume');
const MAX_PAGES = (() => {
  const idx = args.indexOf('--max-pages');
  if (idx >= 0) return parseInt(args[idx + 1]) || 100;
  return TEST_MODE ? 2 : 4000;  // 4000 pages max per Preply subject (10/page = 40K tutors)
})();

const DELAY_MS = 400;           // 400ms between italki requests (no rate limiting)
const PREPLY_DELAY_MS = 700;    // 700ms between Preply requests (HTML pages, be polite)
const ITALKI_PAGE_SIZE = 50;
const ITALKI_MAX_PAGES = 42;    // API wraps/repeats after page 40-41
const PREPLY_PER_PAGE = 10;

// ============================================================================
// UTILS
// ============================================================================

const sleep = ms => new Promise(r => setTimeout(r, ms));

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const lib = urlObj.protocol === 'https:' ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': options.json ? 'application/json' : 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(options.headers || {}),
      },
      timeout: REQUEST_TIMEOUT,
    }, res => {
      // Handle redirects
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${urlObj.protocol}//${urlObj.host}${res.headers.location}`;
        return httpGet(redirectUrl, options).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf-8');
        if (options.json) {
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(new Error(`JSON parse error: ${e.message}`)); }
        } else {
          resolve(body);
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  // Remove emojis and special chars from name
  const clean = fullName.replace(/[\u{1F300}-\u{1F9FF}]/gu, '').trim();
  const parts = clean.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first_name: fullName.trim(), last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

function csvEscape(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCsvRow(fd, row) {
  const line = CSV_HEADERS.map(h => csvEscape(row[h] || '')).join(',');
  fs.writeSync(fd, line + '\n');
}

// Country code to name mapping
const COUNTRY_NAMES = {
  US: 'United States', GB: 'United Kingdom', CA: 'Canada', AU: 'Australia',
  DE: 'Germany', FR: 'France', ES: 'Spain', IT: 'Italy', BR: 'Brazil',
  MX: 'Mexico', JP: 'Japan', CN: 'China', KR: 'South Korea', IN: 'India',
  RU: 'Russia', TR: 'Turkey', PL: 'Poland', NL: 'Netherlands', SE: 'Sweden',
  NO: 'Norway', DK: 'Denmark', FI: 'Finland', PT: 'Portugal', GR: 'Greece',
  CZ: 'Czech Republic', HU: 'Hungary', RO: 'Romania', UA: 'Ukraine',
  PH: 'Philippines', ZA: 'South Africa', AR: 'Argentina', CL: 'Chile',
  CO: 'Colombia', PE: 'Peru', VE: 'Venezuela', EC: 'Ecuador',
  TH: 'Thailand', VN: 'Vietnam', ID: 'Indonesia', MY: 'Malaysia',
  SG: 'Singapore', HK: 'Hong Kong', TW: 'Taiwan', NZ: 'New Zealand',
  IE: 'Ireland', AT: 'Austria', CH: 'Switzerland', BE: 'Belgium',
  IL: 'Israel', EG: 'Egypt', SA: 'Saudi Arabia', AE: 'UAE',
  NG: 'Nigeria', KE: 'Kenya', GH: 'Ghana', RS: 'Serbia', HR: 'Croatia',
  BG: 'Bulgaria', SK: 'Slovakia', LT: 'Lithuania', LV: 'Latvia',
  EE: 'Estonia', SI: 'Slovenia', BA: 'Bosnia', ME: 'Montenegro',
  MK: 'North Macedonia', AL: 'Albania', GE: 'Georgia', AM: 'Armenia',
  AZ: 'Azerbaijan', KZ: 'Kazakhstan', UZ: 'Uzbekistan', BY: 'Belarus',
  MD: 'Moldova', PA: 'Panama', CR: 'Costa Rica', DO: 'Dominican Republic',
  PR: 'Puerto Rico', JM: 'Jamaica', TT: 'Trinidad', CU: 'Cuba',
  GT: 'Guatemala', HN: 'Honduras', SV: 'El Salvador', NI: 'Nicaragua',
  UY: 'Uruguay', PY: 'Paraguay', BO: 'Bolivia',
};

// ============================================================================
// PROGRESS TRACKING
// ============================================================================

let progress = { italki: { page: 0 }, preply: {}, totals: { scraped: 0, written: 0 } };

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
      console.log(`[RESUME] Loaded progress: ${progress.totals.written} written so far`);
    }
  } catch (e) {
    console.warn('[RESUME] Could not load progress file, starting fresh');
  }
}

function saveProgress() {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ============================================================================
// DEDUP
// ============================================================================

const seenIds = new Set();       // Track by platform-specific ID
const seenNames = new Set();     // Track by normalized name

function isDuplicate(record, uniqueId) {
  // Primary dedup: by unique ID (user_id for italki, tutor id for Preply)
  if (uniqueId) {
    const idKey = `${record.source}:${uniqueId}`;
    if (seenIds.has(idKey)) return true;
    seenIds.add(idKey);
  }

  // Secondary dedup: by profile URL
  if (record.profile_url) {
    const urlKey = record.profile_url.toLowerCase();
    if (seenIds.has(urlKey)) return true;
    seenIds.add(urlKey);
  }

  return false;
}

// ============================================================================
// SOURCE 1: italki
// ============================================================================

async function scrapeItalki(csvFd) {
  const maxPages = Math.min(TEST_MODE ? 2 : ITALKI_MAX_PAGES, MAX_PAGES);

  console.log('\n' + '='.repeat(70));
  console.log('  SOURCE 1: italki -- Language Tutoring Platform');
  console.log('  API: api.italki.com/api/v2/teachers (open, no auth)');
  console.log(`  Pages: 1-${maxPages} (50/page, API wraps at ~41)`);
  console.log('  Expected: ~2,000 unique teachers');
  console.log('='.repeat(70));

  let totalScraped = 0;
  let totalWritten = 0;
  const startPage = (RESUME && progress.italki.page) ? progress.italki.page + 1 : 1;

  if (RESUME && startPage > 1) {
    console.log(`  Resuming from page ${startPage}`);
  }

  let newThisPage = 0;
  let consecutiveZeroNew = 0;

  for (let page = startPage; page <= maxPages; page++) {
    try {
      const url = `https://api.italki.com/api/v2/teachers?page_size=${ITALKI_PAGE_SIZE}&page=${page}`;
      const data = await httpGet(url, { json: true });

      const teachers = data.data || [];
      if (teachers.length === 0) break;

      newThisPage = 0;

      for (const t of teachers) {
        const ui = t.user_info || {};
        const ti = t.teacher_info || {};
        const ci = t.course_info || {};
        const stats = t.teacher_statistics || {};

        const name = ui.nickname || '';
        const { first_name, last_name } = splitName(name);
        const countryCode = ui.living_country_id || ui.origin_country_id || '';
        const country = COUNTRY_NAMES[countryCode] || countryCode;
        const city = ui.living_city_name && ui.living_city_name !== 'Other'
          ? ui.living_city_name : '';

        // Build teaching subjects string
        const teachLangs = (ti.teach_language || []).map(l => l.language).join(', ');
        // Build spoken languages string
        const spokenLangs = (ti.also_speak || []).map(l => l.language).join(', ');

        const record = {
          name,
          first_name,
          last_name,
          email: '',
          phone: '',
          company: '',
          website: '',
          city,
          state: '',
          country,
          source: 'italki',
          subject: teachLangs || '',
          rating: ti.overall_rating || '',
          total_sessions: ti.session_count || 0,
          experience_years: '',
          profile_url: `https://www.italki.com/en/teacher/${ui.user_id}`,
          video_url: ti.video_url || '',
          headline: (ti.short_signature || '').substring(0, 200),
          price_usd: ci.min_price || '',
        };

        if (!isDuplicate(record, ui.user_id)) {
          writeCsvRow(csvFd, record);
          totalWritten++;
          newThisPage++;
        }
        totalScraped++;
      }

      // Detect wrap-around: if we get 0 new records, the API is cycling
      if (newThisPage === 0) {
        consecutiveZeroNew++;
        if (consecutiveZeroNew >= 2) {
          console.log(`\n  Detected wrap-around at page ${page} (0 new records x2). Stopping.`);
          break;
        }
      } else {
        consecutiveZeroNew = 0;
      }

      process.stdout.write(`\r  Page ${page}/${maxPages} -- ${totalWritten} unique, ${newThisPage} new this page    `);

      progress.italki.page = page;
      if (page % 10 === 0) saveProgress();

      await sleep(DELAY_MS);
    } catch (err) {
      console.error(`\n  Error page ${page}: ${err.message}`);
      progress.italki.page = page;
      saveProgress();
      await sleep(2000);
    }
  }

  console.log(`\n  italki DONE: ${totalWritten} unique / ${totalScraped} total scraped`);
  return { scraped: totalScraped, written: totalWritten };
}

// ============================================================================
// SOURCE 2: Preply
// ============================================================================

async function scrapePreply(csvFd) {
  console.log('\n' + '='.repeat(70));
  console.log('  SOURCE 2: Preply -- Online Tutoring Marketplace');
  console.log('  Method: SSR __NEXT_DATA__ extraction');
  console.log(`  Subjects: ${PREPLY_SUBJECTS.length} | Max pages: ${MAX_PAGES}`);
  console.log('  Expected: ~85,000 tutors across all subjects');
  console.log('='.repeat(70));

  let totalScraped = 0;
  let totalWritten = 0;
  let consecutiveErrors = 0;

  for (const subject of PREPLY_SUBJECTS) {
    const startPage = (RESUME && progress.preply[subject]) ? progress.preply[subject] + 1 : 1;

    if (RESUME && startPage > 1) {
      console.log(`  [${subject}] Resuming from page ${startPage}`);
    }

    let subjectTotal = 0;
    let subjectCount = 0;
    let emptyPages = 0;
    let maxPagesForSubject = MAX_PAGES;

    for (let page = startPage; page <= maxPagesForSubject; page++) {
      try {
        const url = `https://preply.com/en/online/${subject}-tutors?page=${page}`;
        const html = await httpGet(url, { json: false });

        // Extract __NEXT_DATA__
        const match = html.match(/<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
        if (!match) {
          console.error(`\n  [${subject}] No __NEXT_DATA__ on page ${page}`);
          emptyPages++;
          if (emptyPages >= 3) break;
          await sleep(PREPLY_DELAY_MS);
          continue;
        }

        const nextData = JSON.parse(match[1]);
        const pageProps = nextData.props?.pageProps || {};
        const ssr = pageProps.ssrAllTutors || {};
        const tutors = ssr.tutors || [];

        // On first page, get total and calculate max pages
        if (page === startPage && ssr.totalResults) {
          subjectTotal = ssr.totalResults;
          maxPagesForSubject = Math.min(
            MAX_PAGES,
            Math.ceil(subjectTotal / PREPLY_PER_PAGE)
          );
          console.log(`  [${subject}] ${subjectTotal.toLocaleString()} total tutors, scraping up to page ${maxPagesForSubject.toLocaleString()}`);
        }

        if (tutors.length === 0) {
          emptyPages++;
          if (emptyPages >= 3) {
            console.log(`  [${subject}] No more results after page ${page}`);
            break;
          }
          await sleep(PREPLY_DELAY_MS);
          continue;
        }
        emptyPages = 0;
        consecutiveErrors = 0;

        for (const t of tutors) {
          const user = t.user || {};
          const fullName = user.fullName || '';
          const { first_name, last_name } = splitName(fullName);
          const cob = t.countryOfBirth || {};

          const record = {
            name: fullName,
            first_name,
            last_name,
            email: '',
            phone: '',
            company: '',
            website: '',
            city: '',
            state: '',
            country: cob.name || '',
            source: 'preply',
            subject,
            rating: t.averageScore || '',
            total_sessions: t.totalLessons || 0,
            experience_years: t.yearsOfExperience || '',
            profile_url: t.publicUrl || `https://preply.com/en/tutor/${t.id}`,
            video_url: t.videoIntro || '',
            headline: (t.headline || '').substring(0, 200),
            price_usd: t.price?.value || '',
          };

          if (!isDuplicate(record, t.id)) {
            writeCsvRow(csvFd, record);
            totalWritten++;
            subjectCount++;
          }
          totalScraped++;
        }

        if (page % 50 === 0 || page <= 3 || page === maxPagesForSubject) {
          process.stdout.write(`\r  [${subject}] Page ${page}/${maxPagesForSubject} -- ${subjectCount} unique    `);
        }

        progress.preply[subject] = page;
        if (page % 100 === 0) saveProgress();

        await sleep(PREPLY_DELAY_MS);
      } catch (err) {
        console.error(`\n  [${subject}] Error page ${page}: ${err.message}`);
        consecutiveErrors++;
        progress.preply[subject] = page;
        saveProgress();

        // On 403/429, back off significantly
        if (err.message.includes('403') || err.message.includes('429')) {
          console.error(`  [${subject}] Rate limited/blocked. Waiting 30s...`);
          await sleep(30000);
          consecutiveErrors++;
          if (consecutiveErrors >= 5) {
            console.error(`  [${subject}] Too many errors, skipping to next subject`);
            break;
          }
        } else {
          await sleep(3000);
          if (consecutiveErrors >= 10) {
            console.error(`  [${subject}] Too many errors, skipping to next subject`);
            break;
          }
        }
      }
    }

    console.log(`\n  [${subject}] Done -- ${subjectCount.toLocaleString()} unique / ${subjectTotal.toLocaleString() || '?'} total`);
    saveProgress();
  }

  console.log(`\n  Preply DONE: ${totalWritten.toLocaleString()} unique / ${totalScraped.toLocaleString()} total scraped`);
  return { scraped: totalScraped, written: totalWritten };
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const startTime = Date.now();

  console.log('='.repeat(70));
  console.log('  TUTORING PLATFORM SCRAPER');
  console.log('  Sources: italki (~2K teachers) + Preply (~85K tutors)');
  console.log(`  Mode: ${TEST_MODE ? 'TEST (2 pages/source)' : 'FULL'}`);
  console.log(`  Resume: ${RESUME ? 'YES' : 'NO'}`);
  console.log(`  Output: ${OUTPUT_FILE}`);
  console.log('='.repeat(70));

  // Ensure output dir exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Load progress if resuming
  if (RESUME) loadProgress();

  // Open CSV file
  const append = RESUME && fs.existsSync(OUTPUT_FILE);
  const csvFd = fs.openSync(OUTPUT_FILE, append ? 'a' : 'w');

  if (!append) {
    fs.writeSync(csvFd, CSV_HEADERS.join(',') + '\n');
  }

  const results = { italki: null, preply: null };

  try {
    // Source 1: italki (fast, ~2K results, ~20 seconds)
    if (!PREPLY_ONLY) {
      results.italki = await scrapeItalki(csvFd);
    }

    // Source 2: Preply (slower, ~85K results, hours for full scrape)
    if (!ITALKI_ONLY) {
      results.preply = await scrapePreply(csvFd);
    }
  } catch (err) {
    console.error(`\nFATAL ERROR: ${err.message}`);
    console.error(err.stack);
  } finally {
    fs.closeSync(csvFd);
    saveProgress();
  }

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalWritten = (results.italki?.written || 0) + (results.preply?.written || 0);
  const totalScraped = (results.italki?.scraped || 0) + (results.preply?.scraped || 0);

  console.log('\n' + '='.repeat(70));
  console.log('  SCRAPE COMPLETE');
  console.log('='.repeat(70));

  if (results.italki) {
    console.log(`  italki:  ${results.italki.written.toLocaleString()} unique / ${results.italki.scraped.toLocaleString()} scraped`);
  }
  if (results.preply) {
    console.log(`  Preply:  ${results.preply.written.toLocaleString()} unique / ${results.preply.scraped.toLocaleString()} scraped`);
  }

  console.log(`  ----------------------------------------`);
  console.log(`  TOTAL:   ${totalWritten.toLocaleString()} unique leads written`);
  console.log(`  Output:  ${OUTPUT_FILE}`);
  console.log(`  Time:    ${elapsed}s`);
  console.log('='.repeat(70));

  // Count lines in output
  try {
    const content = fs.readFileSync(OUTPUT_FILE, 'utf-8');
    const lines = content.split('\n').filter(l => l.trim()).length - 1;
    console.log(`  CSV rows: ${lines.toLocaleString()}`);
  } catch {}

  // Cleanup progress file on successful completion (not test mode)
  if (!TEST_MODE) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
