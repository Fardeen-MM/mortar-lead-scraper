const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });
  
  console.log('Navigating to TrustPilot search...');
  await page.goto('https://www.trustpilot.com/search?query=coding+bootcamp', { 
    waitUntil: 'networkidle2', timeout: 30000 
  });
  
  // Wait a bit for any challenge to resolve
  await new Promise(r => setTimeout(r, 3000));
  
  const title = await page.title();
  console.log('Page title:', title);
  
  const url = await page.url();
  console.log('Current URL:', url);
  
  // Check for __NEXT_DATA__
  const nextData = await page.evaluate(() => {
    const el = document.getElementById('__NEXT_DATA__');
    if (el) return el.textContent.substring(0, 3000);
    return null;
  });
  if (nextData) {
    console.log('\n=== __NEXT_DATA__ (first 3000 chars) ===');
    console.log(nextData);
  }
  
  // Get the HTML structure of search results
  const html = await page.content();
  console.log('\n=== PAGE HTML LENGTH ===', html.length);
  
  // Look for search result patterns
  const resultInfo = await page.evaluate(() => {
    // Common TrustPilot patterns
    const results = [];
    
    // Try various selectors
    const selectors = [
      '[class*="businessUnit"]', '[class*="search-result"]', '[class*="result"]',
      '[data-business-unit]', 'a[href*="/review/"]', 'section', 'article',
      '[class*="card"]', '[class*="listing"]'
    ];
    
    for (const sel of selectors) {
      const els = document.querySelectorAll(sel);
      if (els.length > 0) {
        results.push({ selector: sel, count: els.length, sample: els[0].outerHTML.substring(0, 500) });
      }
    }
    
    // Also get all links that look like business pages
    const links = [...document.querySelectorAll('a[href*="/review/"]')];
    const bizLinks = links.map(a => ({ href: a.href, text: a.textContent.trim().substring(0, 100) }));
    
    return { results, bizLinks: bizLinks.slice(0, 10) };
  });
  
  console.log('\n=== RESULT SELECTORS ===');
  console.log(JSON.stringify(resultInfo.results, null, 2));
  console.log('\n=== BUSINESS LINKS ===');
  console.log(JSON.stringify(resultInfo.bizLinks, null, 2));
  
  await browser.close();
})().catch(e => { console.error(e.message); process.exit(1); });
