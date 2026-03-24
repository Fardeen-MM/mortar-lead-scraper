/**
 * MCS v3: Intercept AJAX requests to find the actual data API
 */
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });

  const page = await browser.newPage();
  
  // Intercept all XHR/fetch requests
  const apiCalls = [];
  page.on('response', async (response) => {
    const url = response.url();
    const type = response.request().resourceType();
    if ((type === 'xhr' || type === 'fetch') && !url.includes('google') && !url.includes('analytics') && !url.includes('cookie')) {
      const status = response.status();
      let body = '';
      try { body = await response.text(); } catch {}
      apiCalls.push({ url: url.substring(0, 150), status, bodyLen: body.length, bodyPreview: body.substring(0, 300) });
    }
  });

  console.log('Loading MCS...');
  await page.goto('https://mcscertified.com/find-an-installer/', { waitUntil: 'networkidle2', timeout: 45000 });
  await new Promise(r => setTimeout(r, 5000));

  console.log('\n=== AJAX calls captured ===');
  for (const c of apiCalls) {
    console.log(`${c.status} ${c.url} (${c.bodyLen} bytes)`);
    if (c.bodyLen > 0 && c.bodyLen < 1000) {
      console.log('  Preview:', c.bodyPreview.substring(0, 200));
    }
  }

  // Also check for wp-admin AJAX endpoint with installer data
  console.log('\n=== Trying wp-admin AJAX ===');
  const result = await page.evaluate(async () => {
    try {
      const resp = await fetch('/wp-admin/admin-ajax.php', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: 'action=get_installers&page=1&per_page=10'
      });
      return await resp.text();
    } catch (e) { return 'Error: ' + e.message; }
  });
  console.log('AJAX response:', result.substring(0, 500));

  await browser.close();
})();
