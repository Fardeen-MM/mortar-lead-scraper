const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  
  // Check high page numbers to find where results dry up
  for (const pg of [15, 20, 25, 30]) {
    await page.goto(`https://www.trustpilot.com/search?query=driving+school&page=${pg}`, { 
      waitUntil: 'networkidle2', timeout: 30000 
    });
    await new Promise(r => setTimeout(r, 2000));
    
    const info = await page.evaluate(() => {
      const el = document.getElementById('__NEXT_DATA__');
      if (!el) return { error: 'no __NEXT_DATA__' };
      const d = JSON.parse(el.textContent);
      const pp = d.props?.pageProps;
      return {
        hasMore: pp?.hasMore,
        count: pp?.businessUnits?.length || 0,
        firstBiz: pp?.businessUnits?.[0]?.displayName || 'none',
        lastBiz: pp?.businessUnits?.[pp?.businessUnits?.length - 1]?.displayName || 'none',
      };
    });
    console.log(`Page ${pg}: ${JSON.stringify(info)}`);
    
    if (info.count === 0) break;
  }

  await browser.close();
})().catch(e => { console.error(e.message); process.exit(1); });
