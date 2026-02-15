#!/usr/bin/env node
/**
 * API Integration Test — tests every scraper through the actual server pipeline.
 * Starts a scrape job in test mode for each working scraper and verifies results.
 *
 * Usage: node tests/api-test-all.js [--base=http://localhost:3000] [--concurrency=3]
 */

const http = require('http');
const https = require('https');

const BASE = process.argv.find(a => a.startsWith('--base='))?.split('=')[1] || 'http://localhost:3000';
const CONCURRENCY = parseInt(process.argv.find(a => a.startsWith('--concurrency='))?.split('=')[1] || '3', 10);
const TIMEOUT_MS = parseInt(process.argv.find(a => a.startsWith('--timeout='))?.split('=')[1] || '120000', 10);
const ONLY = process.argv.find(a => a.startsWith('--scrapers='))?.split('=')[1]?.split(',') || null;

// Working scrapers we expect to return leads (from smoke test)
const WORKING_SCRAPERS = [
  'AU-NSW', 'AU-QLD', 'AU-VIC', 'AU-WA',
  'CA', 'CA-AB', 'CA-BC', 'CA-NL', 'CA-PE', 'CA-YT',
  'CT', 'FL', 'FR', 'GA',
  'HK', 'ID', 'IE', 'IL',
  'IT', 'MARTINDALE', 'MD', 'MN',
  'NC', 'NY', 'OH', 'OR', 'PA',
  'SG', 'TX', 'UK-EW-BAR', 'UK-SC',
];

// Scrapers that may or may not work (flaky, slow, etc.)
const FLAKY = new Set(['AU-SA', 'AU-TAS', 'DE-BRAK', 'NZ']);

function fetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    const method = options.method || 'GET';
    const parsed = new URL(url);

    const reqOpts = {
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      timeout: 10000,
    };

    const req = proto.request(reqOpts, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, data: data });
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });

    if (options.body) {
      req.write(JSON.stringify(options.body));
    }
    req.end();
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function testScraper(state) {
  const start = Date.now();
  const result = { state, status: 'unknown', leads: 0, error: null, timeMs: 0, hasWaterfall: false };

  try {
    // Start scrape job in test mode with waterfall enabled
    const startRes = await fetch(`${BASE}/api/scrape/start`, {
      method: 'POST',
      body: {
        state,
        test: true,
        waterfall: {
          fetchProfiles: true,
          crossRefMartindale: true,
          crossRefLawyersCom: false,  // Skip lawyers.com (often has CAPTCHA)
          emailCrawl: false,  // Skip Puppeteer email crawl in tests
        },
        emailScrape: false,  // Skip Puppeteer in tests
      },
    });

    if (startRes.status !== 200 || !startRes.data.jobId) {
      result.status = 'start-failed';
      result.error = startRes.data.error || `HTTP ${startRes.status}`;
      result.timeMs = Date.now() - start;
      return result;
    }

    const jobId = startRes.data.jobId;

    // Poll for completion
    let polls = 0;
    const maxPolls = Math.ceil(TIMEOUT_MS / 3000);

    while (polls < maxPolls) {
      await sleep(3000);
      polls++;

      const statusRes = await fetch(`${BASE}/api/scrape/${jobId}/status`);
      if (statusRes.status !== 200) {
        result.status = 'poll-failed';
        result.error = `Status poll returned ${statusRes.status}`;
        break;
      }

      const job = statusRes.data;

      if (job.status === 'complete') {
        result.status = 'ok';
        result.leads = job.leadCount;
        result.stats = job.stats;
        result.hasWaterfall = !!(job.stats && job.stats.waterfall);
        break;
      } else if (job.status === 'error') {
        result.status = 'error';
        result.error = 'Job errored';
        break;
      } else if (job.status === 'cancelled') {
        result.status = 'cancelled';
        result.leads = job.leadCount;
        break;
      }
      // Still running...
    }

    if (result.status === 'unknown') {
      result.status = 'timeout';
      result.error = `Timed out after ${TIMEOUT_MS}ms`;

      // Cancel the job
      try {
        await fetch(`${BASE}/api/scrape/${jobId}/cancel`, { method: 'POST' });
      } catch {}
    }
  } catch (err) {
    result.status = 'error';
    result.error = err.message;
  }

  result.timeMs = Date.now() - start;
  return result;
}

async function main() {
  const scrapers = ONLY || WORKING_SCRAPERS;

  // Verify server is up
  try {
    const configRes = await fetch(`${BASE}/api/config`);
    if (configRes.status !== 200) throw new Error(`Config returned ${configRes.status}`);
    console.log(`Server at ${BASE} is up. API key: ${configRes.data.hasAnthropicKey ? 'YES' : 'NO'}`);
    console.log(`Testing ${scrapers.length} scrapers (concurrency=${CONCURRENCY}, timeout=${TIMEOUT_MS}ms)\n`);
  } catch (err) {
    console.error(`Cannot reach server at ${BASE}: ${err.message}`);
    process.exit(1);
  }

  console.log('='.repeat(90));

  const allResults = [];

  // Process in batches
  for (let i = 0; i < scrapers.length; i += CONCURRENCY) {
    const batch = scrapers.slice(i, i + CONCURRENCY);
    const batchResults = await Promise.all(batch.map(s => testScraper(s)));

    for (const r of batchResults) {
      allResults.push(r);

      const icon = r.status === 'ok' ? (r.leads > 0 ? 'PASS' : 'EMPT')
        : r.status === 'timeout' ? 'TIME'
        : 'FAIL';

      const waterfall = r.hasWaterfall ? ' [waterfall]' : '';
      const wfFields = r.stats?.waterfall?.totalFieldsFilled || 0;
      const wfDetail = wfFields > 0 ? ` (+${wfFields} fields)` : '';

      const detail = r.status === 'ok'
        ? `${r.leads} leads, ${r.stats?.emailsFound || 0} emails${waterfall}${wfDetail}`
        : `${r.error || r.status}`;

      const time = (r.timeMs / 1000).toFixed(1) + 's';
      console.log(`  [${icon}] ${r.state.padEnd(12)} ${time.padEnd(8)} ${detail}`);
    }
  }

  // Summary
  console.log('\n' + '='.repeat(90));
  const ok = allResults.filter(r => r.status === 'ok' && r.leads > 0);
  const empty = allResults.filter(r => r.status === 'ok' && r.leads === 0);
  const timeout = allResults.filter(r => r.status === 'timeout');
  const errors = allResults.filter(r => !['ok', 'timeout'].includes(r.status));

  console.log(`\nSUMMARY:`);
  console.log(`  PASS (returned leads): ${ok.length}`);
  console.log(`  EMPTY (0 results):     ${empty.length}`);
  console.log(`  TIMEOUT:               ${timeout.length}`);
  console.log(`  ERROR:                 ${errors.length}`);
  console.log(`  TOTAL:                 ${allResults.length}`);

  // Total leads
  const totalLeads = allResults.reduce((sum, r) => sum + r.leads, 0);
  const totalEmails = allResults.reduce((sum, r) => sum + (r.stats?.emailsFound || 0), 0);
  const waterfallCount = allResults.filter(r => r.hasWaterfall).length;
  const waterfallFields = allResults.reduce((sum, r) => sum + (r.stats?.waterfall?.totalFieldsFilled || 0), 0);
  console.log(`\n  Total leads:           ${totalLeads}`);
  console.log(`  Total emails:          ${totalEmails}`);
  console.log(`  Waterfall ran:         ${waterfallCount}/${allResults.length}`);
  console.log(`  Waterfall fields filled: ${waterfallFields}`);

  if (errors.length > 0) {
    console.log(`\nFAILURES:`);
    for (const r of errors) {
      console.log(`  ${r.state}: ${r.status} — ${r.error}`);
    }
  }

  if (empty.length > 0) {
    console.log(`\nEMPTY (expected leads but got 0):`);
    for (const r of empty) {
      console.log(`  ${r.state}`);
    }
  }

  console.log('\n' + '='.repeat(90));

  // Exit code
  const failures = errors.length + empty.length;
  process.exit(failures > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
