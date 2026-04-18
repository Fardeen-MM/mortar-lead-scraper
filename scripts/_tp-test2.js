const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  
  // Check pagination - page 2
  console.log('=== Testing page 2 for "driving school" ===');
  await page.goto('https://www.trustpilot.com/search?query=driving+school&page=2', { 
    waitUntil: 'networkidle2', timeout: 30000 
  });
  await new Promise(r => setTimeout(r, 2000));
  
  const data = await page.evaluate(() => {
    const el = document.getElementById('__NEXT_DATA__');
    if (el) return JSON.parse(el.textContent);
    return null;
  });
  
  if (data && data.props && data.props.pageProps) {
    const pp = data.props.pageProps;
    console.log('hasMore:', pp.hasMore);
    console.log('businessUnits count:', pp.businessUnits ? pp.businessUnits.length : 0);
    
    // Show first business fully
    if (pp.businessUnits && pp.businessUnits.length > 0) {
      console.log('\nFull first business:');
      console.log(JSON.stringify(pp.businessUnits[0], null, 2));
      console.log('\nAll business names:');
      pp.businessUnits.forEach((b, i) => {
        console.log(`  ${i+1}. ${b.displayName} - ${b.contact?.website || 'no website'} - ${b.contact?.email || 'no email'} - ${b.location?.city || ''}, ${b.location?.country || ''}`);
      });
    }
  }
  
  // Check page 3
  console.log('\n=== Testing page 3 for "driving school" ===');
  await page.goto('https://www.trustpilot.com/search?query=driving+school&page=3', { 
    waitUntil: 'networkidle2', timeout: 30000 
  });
  await new Promise(r => setTimeout(r, 2000));
  
  const data3 = await page.evaluate(() => {
    const el = document.getElementById('__NEXT_DATA__');
    if (el) return JSON.parse(el.textContent);
    return null;
  });
  
  if (data3 && data3.props && data3.props.pageProps) {
    console.log('hasMore:', data3.props.pageProps.hasMore);
    console.log('businessUnits count:', data3.props.pageProps.businessUnits ? data3.props.pageProps.businessUnits.length : 0);
    if (data3.props.pageProps.businessUnits) {
      data3.props.pageProps.businessUnits.forEach((b, i) => {
        console.log(`  ${i+1}. ${b.displayName} - ${b.contact?.website || 'no website'}`);
      });
    }
  }

  // Test page 10 (does it have results?)
  console.log('\n=== Testing page 10 for "driving school" ===');
  await page.goto('https://www.trustpilot.com/search?query=driving+school&page=10', { 
    waitUntil: 'networkidle2', timeout: 30000 
  });
  await new Promise(r => setTimeout(r, 2000));
  
  const data10 = await page.evaluate(() => {
    const el = document.getElementById('__NEXT_DATA__');
    if (el) return JSON.parse(el.textContent);
    return null;
  });
  
  if (data10 && data10.props && data10.props.pageProps) {
    console.log('hasMore:', data10.props.pageProps.hasMore);
    console.log('businessUnits count:', data10.props.pageProps.businessUnits ? data10.props.pageProps.businessUnits.length : 0);
  }

  await browser.close();
})().catch(e => { console.error(e.message); process.exit(1); });
