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
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 60000 });
  await new Promise(r => setTimeout(r, 3000));

  // Broader search
  const result = await page.evaluate(async () => {
    const res = await fetch('/api/v1/mixed_people/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        person_titles: ['attorney', 'lawyer', 'partner', 'counsel'],
        person_locations: ['Florida, US'],
        person_seniorities: ['owner', 'founder', 'partner', 'c_suite', 'director'],
        page: 1,
        per_page: 25,
      }),
      credentials: 'include',
    });
    const data = await res.json();
    const people = data.people || [];
    const withEmail = people.filter(p => p.email && !p.email.includes('not_unlocked'));
    const withLinkedin = people.filter(p => p.linkedin_url);
    const sample = people.slice(0, 8).map(p => ({
      name: p.first_name + ' ' + p.last_name,
      email: p.email || 'none',
      email_status: p.email_status,
      title: (p.title || '').slice(0, 35),
      firm: ((p.organization || {}).name || '').slice(0, 30),
      city: (p.city || '') + ', ' + (p.state || ''),
      linkedin: p.linkedin_url ? 'YES' : 'no',
      seniority: p.seniority,
      domain: (p.organization || {}).primary_domain || '',
      phone: (p.phone_numbers && p.phone_numbers[0]) ? p.phone_numbers[0].sanitized_number : '',
    }));
    return { total: data.total_entries, returned: people.length, withRealEmail: withEmail.length, withLinkedin: withLinkedin.length, sample };
  });

  console.log('TOTAL MATCHES:', result.total);
  console.log('RETURNED:', result.returned);
  console.log('WITH REAL EMAIL:', result.withRealEmail + '/' + result.returned);
  console.log('WITH LINKEDIN:', result.withLinkedin + '/' + result.returned);
  console.log('');
  for (const p of result.sample) {
    console.log(`  ${p.name.padEnd(28)} | ${(p.seniority||'').padEnd(10)} | ${p.title.padEnd(35)} | ${p.firm.padEnd(30)} | ${p.city.padEnd(22)} | LI:${p.linkedin.padEnd(3)} | ${p.domain.padEnd(25)} | ${p.email_status || ''}`);
  }

  await browser.close();
}

test().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
