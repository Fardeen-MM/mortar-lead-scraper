const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  
  // Check very high pages
  for (const pg of [40, 50, 75, 100]) {
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
        count: pp?.businessUnits?.length || 0,
        firstBiz: pp?.businessUnits?.[0]?.displayName || 'none',
      };
    });
    console.log(`Page ${pg}: ${JSON.stringify(info)}`);
    
    if (info.count === 0) {
      console.log(`Results empty at page ${pg}`);
      break;
    }
  }

  await browser.close();
})().catch(e => { console.error(e.message); process.exit(1); });
