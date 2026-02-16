#!/usr/bin/env node
/**
 * Live smoke test — hits every registered scraper's real endpoint.
 * Tests that search() yields at least one result or a known signal (_captcha).
 *
 * Usage: node tests/smoke-test.js [--concurrency=5] [--timeout=30000]
 */

const { getRegistry } = require('../lib/registry');

const CONCURRENCY = parseInt(process.argv.find(a => a.startsWith('--concurrency='))?.split('=')[1] || '5', 10);
const TIMEOUT_MS = parseInt(process.argv.find(a => a.startsWith('--timeout='))?.split('=')[1] || '30000', 10);

// Known placeholders that don't have public directories or require browser/auth
const KNOWN_PLACEHOLDERS = new Set([
  // US — CAPTCHA/login/auth required
  'NH',   // Login required
  'DC',   // Salesforce + reCAPTCHA
  'LA',   // Image CAPTCHA
  'MA',   // Salesforce Lightning
  'WI',   // reCAPTCHA
  'WV',   // 403 Forbidden
  'SD',   // React SPA referral only
  // US — inaccessible/unknown format
  'AK',   // CV5 response format unknown
  'AL',   // ASP.NET AJAX UpdatePanels
  'AR',   // DNS not found
  'HI',   // Algolia key not found
  'IA',   // Lucee CMS form fields unknown
  'IN',   // 403 on fetch
  'KS',   // SPA 403
  'KY',   // Iframe-embedded search
  'ME',   // Response format unknown
  'MS',   // 403 Forbidden
  'MT',   // LicensedLawyer.org JS SPA
  'ND',   // CMS search fields unknown
  'NE',   // Vue.js SPA
  'NJ',   // Incapsula WAF
  'NM',   // Needs inspection
  'NV',   // CV5 memberdll system
  'OK',   // eWeb/iMIS form fields unknown
  'RI',   // Response format unknown
  'UT',   // DNN iframe
  'VT',   // Login required
  'WY',   // WordPress directory plugin
  'DE',   // DOE Legal ASP.NET — field names need browser inspection
  // Canada — inaccessible
  'CA-MB',  // reCAPTCHA v3
  'CA-NB',  // Alinity CAPTCHA
  'CA-NS',  // Login required
  'CA-NT',  // Thentia Cloud JS app
  'CA-NU',  // Response format unknown
  'CA-ON',  // SSL connectivity issues
  'CA-SK',  // Alinity CAPTCHA
  // UK — inaccessible
  'UK-EW',  // SRA API endpoint unknown
  'UK-NI',  // Client-side rendering
  // Australia — may need browser/special handling
  'AU-NT',  // PDF scraper triggers CAPTCHA on some requests
  'AU-ACT',   // Bond MCRM — requires Puppeteer
  // Europe — placeholder
  'ES',       // Spain CGAE — Liferay/Angular SPA
  // Asia-Pacific — placeholder
  'IN-DL',    // India BCI — no reliable public API
  // Africa — placeholder
  'ZA',       // South Africa LSSA — no public directory
  // Directories — may have CAPTCHA
  'LAWYERS-COM',  // Lawyers.com — CAPTCHA detected
]);

async function testScraper(code, loader) {
  const start = Date.now();
  const result = {
    code,
    status: 'unknown',
    leads: 0,
    captcha: false,
    firstLead: null,
    error: null,
    timeMs: 0,
  };

  try {
    const scraper = loader();
    const city = scraper.defaultCities?.[0] || null;

    // Wrap in a timeout
    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error('TIMEOUT')), TIMEOUT_MS)
    );

    const searchPromise = (async () => {
      // Skip profile fetching in smoke test to avoid timeouts (NZ, etc.)
      for await (const item of scraper.search(null, { maxPages: 1, city, skipProfiles: true })) {
        if (item._cityProgress) continue;

        if (item._captcha) {
          result.captcha = true;
          result.status = 'captcha';
          return;
        }

        // It's a lead
        result.leads++;
        if (!result.firstLead) {
          result.firstLead = {
            name: `${item.first_name || ''} ${item.last_name || ''}`.trim(),
            city: item.city || '',
            firm: item.firm_name || '',
            hasPhone: !!(item.phone),
            hasEmail: !!(item.email),
            hasWebsite: !!(item.website),
          };
        }

        // Stop after collecting a few leads to avoid hammering sites
        if (result.leads >= 5) return;
      }
    })();

    await Promise.race([searchPromise, timeoutPromise]);

    if (result.leads > 0) {
      result.status = 'ok';
    } else if (!result.captcha) {
      result.status = 'empty';
    }
  } catch (err) {
    result.error = err.message;
    if (err.message === 'TIMEOUT') {
      result.status = 'timeout';
    } else if (err.message.includes('CAPTCHA') || err.message.includes('captcha')) {
      result.status = 'captcha';
      result.captcha = true;
    } else {
      result.status = 'error';
    }
  }

  result.timeMs = Date.now() - start;
  return result;
}

async function runBatch(batch) {
  return Promise.all(batch.map(([code, loader]) => testScraper(code, loader)));
}

async function main() {
  const registry = getRegistry();
  const codes = Object.keys(registry).sort();

  console.log(`\nSmoke Testing ${codes.length} scrapers (concurrency=${CONCURRENCY}, timeout=${TIMEOUT_MS}ms)\n`);
  console.log('='.repeat(80));

  const allResults = [];
  const batches = [];

  // Build batches
  for (let i = 0; i < codes.length; i += CONCURRENCY) {
    batches.push(codes.slice(i, i + CONCURRENCY).map(c => [c, registry[c]]));
  }

  for (const batch of batches) {
    const results = await runBatch(batch);
    for (const r of results) {
      allResults.push(r);

      const icon = r.status === 'ok' ? 'PASS'
        : r.status === 'captcha' ? 'CAPT'
        : r.status === 'timeout' ? 'TIME'
        : r.status === 'empty' ? 'EMPT'
        : 'FAIL';

      const isPlaceholder = KNOWN_PLACEHOLDERS.has(r.code);
      const tag = isPlaceholder ? ' [placeholder]' : '';

      const detail = r.status === 'ok'
        ? `${r.leads} leads — ${r.firstLead?.name || '?'} (${r.firstLead?.city || '?'})${r.firstLead?.hasPhone ? ' +phone' : ''}${r.firstLead?.hasEmail ? ' +email' : ''}${r.firstLead?.hasWebsite ? ' +web' : ''}`
        : r.status === 'captcha' ? 'CAPTCHA / login required'
        : r.status === 'timeout' ? `Timed out after ${r.timeMs}ms`
        : r.status === 'empty' ? 'No results returned'
        : `Error: ${r.error}`;

      console.log(`  [${icon}] ${r.code.padEnd(8)} ${(r.timeMs + 'ms').padEnd(8)} ${detail}${tag}`);
    }
  }

  // Summary
  console.log('\n' + '='.repeat(80));
  const ok = allResults.filter(r => r.status === 'ok');
  const captcha = allResults.filter(r => r.status === 'captcha');
  const empty = allResults.filter(r => r.status === 'empty');
  const timeout = allResults.filter(r => r.status === 'timeout');
  const errors = allResults.filter(r => r.status === 'error');

  console.log(`\nSUMMARY:`);
  console.log(`  OK (returned leads):  ${ok.length}`);
  console.log(`  CAPTCHA/login:        ${captcha.length}`);
  console.log(`  Empty (0 results):    ${empty.length}`);
  console.log(`  Timeout:              ${timeout.length}`);
  console.log(`  Error:                ${errors.length}`);
  console.log(`  TOTAL:                ${allResults.length}`);

  // Breakdown of failures
  const realFailures = allResults.filter(r =>
    r.status !== 'ok' && !KNOWN_PLACEHOLDERS.has(r.code)
  );

  if (realFailures.length > 0) {
    console.log(`\nNON-PLACEHOLDER ISSUES (${realFailures.length}):`);
    for (const r of realFailures) {
      console.log(`  ${r.code}: ${r.status} — ${r.error || 'no error message'}`);
    }
  }

  // Field coverage for successful scrapers
  if (ok.length > 0) {
    const withPhone = ok.filter(r => r.firstLead?.hasPhone).length;
    const withEmail = ok.filter(r => r.firstLead?.hasEmail).length;
    const withWeb = ok.filter(r => r.firstLead?.hasWebsite).length;
    console.log(`\nFIELD COVERAGE (of ${ok.length} working scrapers):`);
    console.log(`  Has phone:   ${withPhone} (${Math.round(100*withPhone/ok.length)}%)`);
    console.log(`  Has email:   ${withEmail} (${Math.round(100*withEmail/ok.length)}%)`);
    console.log(`  Has website: ${withWeb} (${Math.round(100*withWeb/ok.length)}%)`);
  }

  console.log('\n' + '='.repeat(80));

  // Exit code: fail only if non-placeholder scrapers have errors
  const unexpectedErrors = errors.filter(r => !KNOWN_PLACEHOLDERS.has(r.code));
  process.exit(unexpectedErrors.length > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
