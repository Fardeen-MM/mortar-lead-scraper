/**
 * Smoke Tests — verify each registered scraper can reach its base URL.
 * Run separately via: npm run test:smoke
 */

const { getRegistry } = require('../../lib/registry');
const https = require('https');
const http = require('http');

const TIMEOUT = 15000;

function checkUrl(url) {
  return new Promise((resolve) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, {
      timeout: TIMEOUT,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
      },
    }, (res) => {
      res.resume();
      resolve({ ok: res.statusCode < 500, statusCode: res.statusCode });
    });
    req.on('error', (err) => resolve({ ok: false, error: err.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, error: 'timeout' }); });
  });
}

describe('Scraper Connectivity Smoke Tests', () => {
  const registry = getRegistry();

  for (const [code, loader] of Object.entries(registry)) {
    const scraper = loader();
    test(`${code} (${scraper.name}) — ${scraper.baseUrl}`, async () => {
      const result = await checkUrl(scraper.baseUrl);
      expect(result.ok).toBe(true);
    }, 20000);
  }
});
