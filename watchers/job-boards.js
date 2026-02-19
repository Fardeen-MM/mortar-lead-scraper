/**
 * Job Board Watcher — scans Indeed for law firm hiring signals
 *
 * Primary: JobSpy Python bridge (works on Railway with python-jobspy installed)
 * Fallback: Puppeteer scraping (works locally without Python deps)
 *
 * Searches multiple countries (US, UK, Canada, Australia) for law firms
 * hiring marketing, intake, and business development roles.
 */

const path = require('path');
const { execFileSync } = require('child_process');
const signalDb = require('../lib/signal-db');
const telegram = require('../lib/telegram');

// Search terms grouped by region
// Each entry: { query, location, country }
const SEARCHES = [
  // --- US ---
  { query: '"law firm" "marketing coordinator"', location: 'United States', country: 'US' },
  { query: '"law firm" "marketing manager"', location: 'United States', country: 'US' },
  { query: '"law firm" "marketing director"', location: 'United States', country: 'US' },
  { query: '"law firm" "digital marketing"', location: 'United States', country: 'US' },
  { query: '"law firm" "intake specialist"', location: 'United States', country: 'US' },
  { query: '"law firm" "intake coordinator"', location: 'United States', country: 'US' },
  { query: '"law firm" "business development"', location: 'United States', country: 'US' },
  { query: '"personal injury" "marketing manager"', location: 'United States', country: 'US' },
  { query: '"personal injury" "marketing coordinator"', location: 'United States', country: 'US' },

  // --- UK ---
  { query: '"law firm" "marketing manager"', location: 'United Kingdom', country: 'UK' },
  { query: '"law firm" "marketing coordinator"', location: 'United Kingdom', country: 'UK' },
  { query: '"law firm" "business development"', location: 'United Kingdom', country: 'UK' },
  { query: 'solicitors "marketing manager"', location: 'United Kingdom', country: 'UK' },

  // --- Canada ---
  { query: '"law firm" "marketing manager"', location: 'Canada', country: 'CA' },
  { query: '"law firm" "marketing coordinator"', location: 'Canada', country: 'CA' },
  { query: '"law firm" "business development"', location: 'Canada', country: 'CA' },

  // --- Australia ---
  { query: '"law firm" "marketing manager"', location: 'Australia', country: 'AU' },
  { query: '"law firm" "marketing coordinator"', location: 'Australia', country: 'AU' },
  { query: '"law firm" "business development"', location: 'Australia', country: 'AU' },
];

// --- Law Firm Detection ---
// Tight matching: only words that almost always mean "law firm"
const LAW_FIRM_STRONG = [
  'law firm', 'law office', 'law group', 'law center', 'law centre',
  'llp', 'pllc', 'p.c.', 'p.a.',
  '& associates', 'attorneys at law', 'attorneys-at-law',
  'barristers', 'solicitors',
];

// Weaker signals — need to appear alongside a legal context word
const LAW_FIRM_WEAK = [
  'injury', 'personal injury', 'family law', 'criminal defense',
  'criminal defence', 'immigration law', 'estate planning',
  'bankruptcy', 'employment law', 'intellectual property law',
];

function isLawFirm(companyName) {
  if (!companyName) return false;
  const lower = companyName.toLowerCase();

  // Strong match: company name contains a definitive law firm indicator
  if (LAW_FIRM_STRONG.some(w => lower.includes(w))) return true;

  // "law" as a standalone word (not "outlaw", "lawn", "flaw")
  if (/\blaw\b/.test(lower)) return true;

  // "legal" as standalone (not "illegal")
  if (/\blegal\b/.test(lower) && !lower.includes('illegal')) return true;

  // "attorney" or "lawyer" in name
  if (/\battorney|lawyer\b/.test(lower)) return true;

  // Weak match: practice area in company name (these are almost always law firms)
  if (LAW_FIRM_WEAK.some(w => lower.includes(w))) return true;

  return false;
}

// --- Marketing Role Detection ---
// The job TITLE must be a marketing/intake/BD role, not an attorney/paralegal position
const MARKETING_TITLE_WORDS = [
  'marketing', 'brand', 'content', 'social media', 'creative',
  'intake', 'business development', 'biz dev', 'sales',
  'communications', 'public relations', 'pr manager', 'pr coordinator',
  'growth', 'demand gen', 'seo', 'sem', 'ppc',
  'copywriter', 'graphic design', 'web design',
];

const NON_MARKETING_TITLES = [
  'attorney', 'lawyer', 'paralegal', 'legal assistant', 'legal secretary',
  'associate attorney', 'partner', 'counsel', 'litigator',
  'receptionist', 'office manager', 'bookkeeper', 'accountant',
  'it support', 'software engineer', 'developer',
];

function isMarketingRole(jobTitle) {
  if (!jobTitle) return false;
  const lower = jobTitle.toLowerCase();

  // Reject if it's clearly a non-marketing role
  if (NON_MARKETING_TITLES.some(w => lower.includes(w))) return false;

  // Must contain a marketing-related word
  return MARKETING_TITLE_WORDS.some(w => lower.includes(w));
}

/**
 * Search Indeed via JobSpy Python bridge.
 * Returns array of { title, company, city, state, job_url, description }.
 */
function searchIndeedJobSpy(searchTerm, location = 'United States', hoursOld = 72) {
  try {
    const bridgePath = path.join(__dirname, '..', 'jobspy_bridge.py');
    const stdout = execFileSync('python3', [
      bridgePath, searchTerm, location, String(hoursOld)
    ], { timeout: 120000, encoding: 'utf-8' });
    return JSON.parse(stdout);
  } catch (err) {
    return null; // Signal to try fallback
  }
}

/**
 * Fallback: Search Indeed via Puppeteer scraping.
 */
async function searchIndeedPuppeteer(query, location = 'United States', hoursOld = 72) {
  let browser;
  try {
    const puppeteer = require('puppeteer');
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );

    // Pick the right Indeed domain based on location
    const domain = getIndeedDomain(location);
    const days = Math.ceil(hoursOld / 24);
    const url = `https://${domain}/jobs?q=${encodeURIComponent(query)}&l=${encodeURIComponent(location)}&fromage=${days}&sort=date`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    await page.waitForSelector('.job_seen_beacon, .jobsearch-ResultsList', { timeout: 10000 }).catch(() => {});

    const jobs = await page.evaluate(() => {
      const results = [];
      const cards = document.querySelectorAll('.job_seen_beacon, [data-jk]');
      cards.forEach(card => {
        const titleEl = card.querySelector('h2.jobTitle a, .jobTitle > a, a[data-jk]');
        const companyEl = card.querySelector('[data-testid="company-name"], .companyName, .company');
        const locationEl = card.querySelector('[data-testid="text-location"], .companyLocation, .location');
        const snippetEl = card.querySelector('.job-snippet, .underShelfFooter, [class*="snippet"]');
        const linkEl = card.querySelector('a[href*="/viewjob"], a[data-jk], h2.jobTitle a');

        const title = titleEl?.textContent?.trim() || '';
        const company = companyEl?.textContent?.trim() || '';
        const loc = locationEl?.textContent?.trim() || '';
        const description = snippetEl?.textContent?.trim() || '';
        const href = linkEl?.getAttribute('href') || '';

        if (!title || !company) return;
        const locParts = loc.split(',').map(s => s.trim());
        const city = locParts[0] || '';
        const state = (locParts[1] || '').replace(/\d+/g, '').trim();
        let job_url = '';
        if (href.startsWith('http')) job_url = href;
        else if (href) job_url = `https://www.indeed.com${href}`;

        results.push({ title, company, city, state, job_url, description: description.substring(0, 500) });
      });
      return results;
    });

    return jobs;
  } catch (err) {
    console.error(`[Signal Engine] Puppeteer scrape error: ${err.message}`);
    return [];
  } finally {
    if (browser) await browser.close();
  }
}

function getIndeedDomain(location) {
  const loc = location.toLowerCase();
  if (loc.includes('united kingdom') || loc.includes('uk')) return 'uk.indeed.com';
  if (loc.includes('canada')) return 'ca.indeed.com';
  if (loc.includes('australia')) return 'au.indeed.com';
  return 'www.indeed.com';
}

/**
 * Search Indeed — try JobSpy first, fall back to Puppeteer.
 */
async function searchIndeed(searchTerm, location, hoursOld) {
  // Try JobSpy Python bridge first (preferred — better results on Railway)
  const jobspyResult = searchIndeedJobSpy(searchTerm, location, hoursOld);
  if (jobspyResult !== null) {
    if (jobspyResult.error) {
      console.warn(`[Signal Engine] JobSpy error: ${jobspyResult.error}`);
      return [];
    }
    return jobspyResult;
  }

  // Fallback to Puppeteer
  return searchIndeedPuppeteer(searchTerm, location, hoursOld);
}

/**
 * Run one full scan cycle.
 */
async function run() {
  console.log('[Signal Engine] Starting job board scan...');
  let totalNew = 0;

  for (const search of SEARCHES) {
    console.log(`[Signal Engine] Searching [${search.country}]: ${search.query}`);

    const jobs = await searchIndeed(search.query, search.location, 72);
    if (!jobs || jobs.length === 0) {
      console.log(`[Signal Engine]   0 results`);
      await sleep(5000 + Math.random() * 5000);
      continue;
    }

    let matched = 0;
    for (const job of jobs) {
      // Two-gate filter: must be a law firm AND a marketing/intake/BD role
      if (!isLawFirm(job.company)) continue;
      if (!isMarketingRole(job.title)) continue;

      matched++;

      const isNew = signalDb.insertSignal({
        firm_name: job.company,
        job_title: job.title,
        city: job.city,
        state: job.state,
        country: search.country,
        job_url: job.job_url,
        description: job.description || '',
      });

      if (isNew) {
        totalNew++;
        await telegram.sendAlert({
          firm_name: job.company,
          job_title: job.title,
          city: job.city,
          state: job.state,
          country: search.country,
          job_url: job.job_url,
        });
      }
    }

    console.log(`[Signal Engine]   ${jobs.length} results, ${matched} matched filters`);

    // Be polite — wait 5-10s between searches
    await sleep(5000 + Math.random() * 5000);
  }

  console.log(`[Signal Engine] Scan complete — ${totalNew} new signals`);
  return totalNew;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

module.exports = { run };
