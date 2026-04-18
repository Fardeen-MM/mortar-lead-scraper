const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    
    console.log('Navigating to Udemy course page...');
    await page.goto('https://www.udemy.com/course/ielts-band-7-preparation-course/', {
      waitUntil: 'networkidle2',
      timeout: 30000
    });
    
    const title = await page.title();
    console.log('Page title:', title);
    
    // Check if we got past Cloudflare
    const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 500));
    console.log('Body text (first 500):', bodyText);
    
    // Look for instructor info
    const instructorData = await page.evaluate(() => {
      // Check for JSON-LD
      const ldScripts = [...document.querySelectorAll('script[type="application/ld+json"]')];
      const ldData = ldScripts.map(s => { try { return JSON.parse(s.textContent); } catch(e) { return null; } }).filter(Boolean);
      
      // Check for instructor links
      const instructorLinks = [...document.querySelectorAll('a[href*="/user/"]')].map(a => ({
        href: a.href,
        text: a.textContent.trim()
      }));
      
      // Check for instructor section
      const instructorSection = document.querySelector('[data-purpose="instructor-bio"]');
      const instructorName = instructorSection ? instructorSection.textContent.trim().substring(0, 200) : null;
      
      // Look for any data in window.__NEXT_DATA__ or similar
      const nextData = window.__NEXT_DATA__ ? JSON.stringify(window.__NEXT_DATA__).substring(0, 1000) : null;
      
      // Check for UD app data
      const udData = window.UD ? JSON.stringify(window.UD).substring(0, 1000) : null;
      
      return { ldData, instructorLinks, instructorName, nextData: nextData?.substring(0,500), udData: udData?.substring(0,500) };
    });
    
    console.log('\nInstructor data:', JSON.stringify(instructorData, null, 2));
    
    // Also try the API from within the browser context (same origin)
    const apiResult = await page.evaluate(async () => {
      try {
        const resp = await fetch('/api-2.0/courses/?search=IELTS&page_size=3&fields[course]=title,url,visible_instructors&fields[user]=title,url,display_name');
        if (resp.ok) {
          return await resp.json();
        }
        return { status: resp.status, statusText: resp.statusText };
      } catch(e) {
        return { error: e.message };
      }
    });
    
    console.log('\nAPI from browser:', JSON.stringify(apiResult, null, 2).substring(0, 1000));
    
  } catch(e) {
    console.error('Error:', e.message);
  } finally {
    await browser.close();
  }
})();
