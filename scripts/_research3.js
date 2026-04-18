const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

  // Check Texas page
  console.log('=== JUSTIA TEXAS ===');
  await page.goto('https://www.justia.com/lawyers/immigration-law/texas', { waitUntil: 'networkidle2', timeout: 30000 });

  const txData = await page.evaluate(() => {
    return {
      title: document.title,
      h1: document.querySelector('h1')?.textContent?.trim(),
      cards: document.querySelectorAll('.jld-card').length,
      allLinks: Array.from(document.querySelectorAll('a')).filter(a =>
        a.href.includes('/immigration-law/texas/')
      ).map(a => ({ text: a.textContent.trim(), href: a.href })).filter(l => l.text.length > 1),
      isChallenge: document.title.includes('Just a moment'),
    };
  });

  console.log('Title:', txData.title);
  console.log('H1:', txData.h1);
  console.log('Cards:', txData.cards);
  console.log('Is Cloudflare:', txData.isChallenge);
  console.log('City links:', txData.allLinks.length, txData.allLinks.slice(0, 10));

  // If TX has city links, try one
  if (txData.allLinks.length > 0) {
    const houstonLink = txData.allLinks.find(l => l.text.toLowerCase().includes('houston'));
    if (houstonLink) {
      console.log('\n=== JUSTIA HOUSTON TX ===');
      await page.goto(houstonLink.href, { waitUntil: 'networkidle2', timeout: 30000 });
      const hData = await page.evaluate(() => ({
        cards: document.querySelectorAll('.jld-card').length,
        title: document.title
      }));
      console.log('Houston cards:', hData.cards, 'Title:', hData.title);
    }
  }

  // FindLaw - check state page for cities in California
  console.log('\n=== FINDLAW CALIFORNIA CITY COUNT ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/california', { waitUntil: 'networkidle2', timeout: 30000 });

  const flCities = await page.evaluate(() => {
    const links = [];
    document.querySelectorAll('a').forEach(a => {
      const m = a.href.match(/\/immigration-naturalization-law\/california\/([a-z][a-z0-9-]+)\/?$/);
      if (m) {
        links.push({ city: a.textContent.trim(), slug: m[1] });
      }
    });
    // Dedupe
    const seen = new Set();
    return links.filter(l => {
      if (seen.has(l.slug)) return false;
      seen.add(l.slug);
      return true;
    });
  });
  console.log('CA cities on FindLaw:', flCities.length);
  console.log('First 10:', flCities.slice(0, 10));

  // FindLaw Texas
  console.log('\n=== FINDLAW TEXAS CITIES ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/texas', { waitUntil: 'networkidle2', timeout: 30000 });

  const flTxCities = await page.evaluate(() => {
    const links = [];
    document.querySelectorAll('a').forEach(a => {
      const m = a.href.match(/\/immigration-naturalization-law\/texas\/([a-z][a-z0-9-]+)\/?$/);
      if (m) {
        links.push({ city: a.textContent.trim(), slug: m[1] });
      }
    });
    const seen = new Set();
    return links.filter(l => {
      if (seen.has(l.slug)) return false;
      seen.add(l.slug);
      return true;
    });
  });
  console.log('TX cities on FindLaw:', flTxCities.length);
  console.log('First 10:', flTxCities.slice(0, 10));

  // Justia - check a small state to see fewer results
  console.log('\n=== JUSTIA WYOMING ===');
  await page.goto('https://www.justia.com/lawyers/immigration-law/wyoming', { waitUntil: 'networkidle2', timeout: 30000 });
  const wyData = await page.evaluate(() => ({
    cards: document.querySelectorAll('.jld-card').length,
    title: document.title,
  }));
  console.log('WY cards:', wyData.cards, 'Title:', wyData.title);

  await browser.close();
})().catch(e => console.error(e));
