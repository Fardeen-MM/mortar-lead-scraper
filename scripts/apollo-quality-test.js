const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: '/tmp/apollo-session-profile',
    args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check', '--disable-extensions', '--window-size=1280,900'],
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 90000 });
  await new Promise(r => setTimeout(r, 3000));

  const tests = [
    'Florida, US', 'New York, US', 'Texas, US', 'Illinois, US', 'Georgia, US',
    'Ohio, US', 'Michigan, US', 'Pennsylvania, US', 'North Carolina, US', 'Arizona, US',
    'Colorado, US', 'Washington, US', 'Oregon, US', 'United Kingdom', 'California, US',
  ];

  for (const loc of tests) {
    const result = await page.evaluate((location) => {
      return new Promise(resolve => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/v1/mixed_people/search');
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.withCredentials = true;
        xhr.onload = () => {
          try {
            const data = JSON.parse(xhr.responseText);
            const p = (data.people || [])[0];
            if (p === undefined) { resolve({ location, total: 0, data: 'none' }); return; }
            resolve({
              location,
              total: (data.pagination || {}).total_entries,
              fullName: p.last_name && p.last_name.length > 2,
              hasLinkedin: p.linkedin_url ? true : false,
              sample: p.first_name + ' ' + p.last_name,
            });
          } catch (e) { resolve({ location, error: 'parse: ' + e.message }); }
        };
        xhr.onerror = () => resolve({ location, error: 'network' });
        xhr.send(JSON.stringify({
          person_titles: ['attorney'],
          person_locations: [location],
          person_seniorities: ['owner'],
          page: 1,
          per_page: 3,
        }));
      });
    }, loc);

    const status = result.error ? 'ERROR' : (result.fullName ? 'FULL' : 'OBFUSCATED');
    const linkedin = result.hasLinkedin ? ' [LinkedIn]' : '';
    console.log(loc.padEnd(25) + '| ' + status.padEnd(12) + '| total: ' + String(result.total || 0).padStart(5) + ' | ' + (result.sample || '') + linkedin);
    await new Promise(r => setTimeout(r, 800));
  }

  await browser.close();
})().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
