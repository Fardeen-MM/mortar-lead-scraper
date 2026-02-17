/**
 * Job Board Watcher — scans Indeed for law firm hiring signals
 *
 * Primary: JobSpy Python bridge (works on Railway with python-jobspy installed)
 * Fallback: Puppeteer scraping (works locally without Python deps)
 */

const path = require('path');
const { execFileSync } = require('child_process');
const signalDb = require('../lib/signal-db');
const telegram = require('../lib/telegram');

// 14 search terms covering marketing, intake, and BD roles
const SEARCHES = [
  // Direct marketing hires
  '"law firm" "marketing coordinator"',
  '"law firm" "marketing manager"',
  '"law firm" "marketing director"',
  '"law firm" "digital marketing"',
  '"law firm" "VP marketing"',
  '"law firm" "head of marketing"',

  // Intake hires
  '"law firm" "intake specialist"',
  '"law firm" "intake coordinator"',

  // Business development
  '"law firm" "business development"',
  '"law firm" "VP sales"',

  // Practice-area + marketing
  '"personal injury" "marketing"',
  '"family law" "marketing"',
  '"criminal defense" "marketing"',
  '"immigration" "marketing"',
];

// Law firm detection — check company name only
const LAW_FIRM_WORDS = [
  'law', 'legal', 'attorney', 'lawyer', 'llp', 'pllc', 'p.c.',
  '& associates', 'law group', 'law office', 'law center',
  'injury', 'defense', 'family law', 'immigration', 'estate',
];

function isLawFirm(companyName) {
  if (!companyName) return false;
  const lower = companyName.toLowerCase();
  return LAW_FIRM_WORDS.some(word => lower.includes(word));
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

    const days = Math.ceil(hoursOld / 24);
    const url = `https://www.indeed.com/jobs?q=${encodeURIComponent(query)}&l=${encodeURIComponent(location)}&fromage=${days}&sort=date`;
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

        results.push({ title, company, city, state, job_url, description: description.substring(0, 300) });
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

  for (const searchTerm of SEARCHES) {
    console.log(`[Signal Engine] Searching: ${searchTerm}`);

    const jobs = await searchIndeed(searchTerm, 'United States', 72);
    if (!jobs || jobs.length === 0) {
      console.log(`[Signal Engine]   0 results`);
      // Polite delay between searches
      await sleep(5000 + Math.random() * 5000);
      continue;
    }

    console.log(`[Signal Engine]   ${jobs.length} results`);

    for (const job of jobs) {
      if (!isLawFirm(job.company)) continue;

      const isNew = signalDb.insertSignal({
        firm_name: job.company,
        job_title: job.title,
        city: job.city,
        state: job.state,
        job_url: job.job_url,
        description: job.description,
      });

      if (isNew) {
        totalNew++;
        await telegram.sendAlert({
          firm_name: job.company,
          job_title: job.title,
          city: job.city,
          state: job.state,
          job_url: job.job_url,
        });
      }
    }

    // Be polite to Indeed — wait 5-10s between searches
    await sleep(5000 + Math.random() * 5000);
  }

  console.log(`[Signal Engine] Scan complete — ${totalNew} new signals`);
  return totalNew;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

module.exports = { run };
