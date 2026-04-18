const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

async function test() {
  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: '/tmp/apollo-session-profile',
    args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check', '--disable-extensions'],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080 });

  console.log('Loading Apollo...');
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 60000 });
  await new Promise(r => setTimeout(r, 3000));

  console.log('Title:', await page.title());
  console.log('URL:', page.url().slice(0, 80));

  // Test 1: Direct fetch from page context
  console.log('\n--- TEST 1: fetch() from page context ---');
  const r1 = await page.evaluate(async () => {
    const res = await fetch('/api/v1/mixed_people/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        person_titles: ['attorney'],
        person_locations: ['Florida, US'],
        person_seniorities: ['owner'],
        page: 1,
        per_page: 3,
      }),
      credentials: 'include',
    });
    const text = await res.text();
    const isHtml = text.trim().startsWith('<');
    const isJson = text.trim().startsWith('{') || text.trim().startsWith('[');
    return {
      status: res.status,
      contentType: res.headers.get('content-type'),
      isHtml,
      isJson,
      bodyPreview: text.slice(0, 500),
      bodyLength: text.length,
    };
  });
  console.log('Status:', r1.status);
  console.log('Content-Type:', r1.contentType);
  console.log('Is HTML:', r1.isHtml, '| Is JSON:', r1.isJson);
  console.log('Body length:', r1.bodyLength);
  console.log('Body preview:', r1.bodyPreview.slice(0, 300));

  if (r1.isJson) {
    try {
      const data = JSON.parse(r1.bodyPreview.length === r1.bodyLength ? r1.bodyPreview : '{}');
      console.log('People:', (data.people || []).length);
      console.log('Total:', data.total_entries);
      if (data.people && data.people[0]) {
        const p = data.people[0];
        console.log('First:', p.first_name, p.last_name, '|', p.email, '|', p.title);
      }
    } catch (e) {
      console.log('Parse error:', e.message);
    }
  }

  // Test 2: Try with CSRF token from cookie
  console.log('\n--- TEST 2: fetch() with CSRF token ---');
  const csrfToken = await page.evaluate(() => {
    const cookies = document.cookie.split(';');
    for (const c of cookies) {
      const [name, ...val] = c.trim().split('=');
      if (name === 'X-CSRF-TOKEN') return decodeURIComponent(val.join('='));
    }
    return null;
  });
  console.log('CSRF from cookie:', csrfToken ? csrfToken.slice(0, 30) + '...' : 'NONE');

  const r2 = await page.evaluate(async (csrf) => {
    const headers = { 'Content-Type': 'application/json' };
    if (csrf) headers['x-csrf-token'] = csrf;
    const res = await fetch('/api/v1/mixed_people/search', {
      method: 'POST',
      headers,
      body: JSON.stringify({
        person_titles: ['attorney'],
        person_locations: ['Florida, US'],
        person_seniorities: ['owner'],
        page: 1,
        per_page: 3,
      }),
      credentials: 'include',
    });
    const text = await res.text();
    return { status: res.status, isHtml: text.trim().startsWith('<'), body: text.slice(0, 500) };
  }, csrfToken);
  console.log('Status:', r2.status, '| HTML:', r2.isHtml);
  if (!r2.isHtml) {
    console.log('Body:', r2.body.slice(0, 300));
  } else {
    console.log('Got HTML (Cloudflare)');
  }

  // Test 3: Intercept a real search by navigating
  console.log('\n--- TEST 3: Navigate + intercept real search ---');
  const intercepted = [];
  page.on('response', async (res) => {
    if (res.url().includes('mixed_people')) {
      const ct = res.headers()['content-type'] || '';
      intercepted.push({ url: res.url().slice(0, 80), status: res.status(), ct });
    }
  });

  await page.goto('https://app.apollo.io/#/people?page=1&personTitles[]=attorney&personLocations[]=Florida%2C%20US&personSeniorities[]=owner', {
    waitUntil: 'networkidle2',
    timeout: 30000,
  }).catch(() => {});
  await new Promise(r => setTimeout(r, 5000));

  console.log('Intercepted requests:', intercepted.length);
  for (const r of intercepted) {
    console.log(`  ${r.status} | ${r.ct} | ${r.url}`);
  }

  await browser.close();
}

test().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
