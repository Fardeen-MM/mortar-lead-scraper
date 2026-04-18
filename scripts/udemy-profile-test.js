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
    
    console.log('Navigating to instructor profile...');
    await page.goto('https://www.udemy.com/user/keinocampbell/', {
      waitUntil: 'networkidle2',
      timeout: 30000
    });
    
    const title = await page.title();
    console.log('Page title:', title);
    
    const profileData = await page.evaluate(() => {
      const bodyText = document.body.innerText.substring(0, 2000);
      
      // Look for social/website links
      const allLinks = [...document.querySelectorAll('a')].map(a => ({
        href: a.href,
        text: a.textContent.trim().substring(0, 100),
        className: a.className
      })).filter(l => !l.href.includes('udemy.com') || l.href.includes('/user/'));
      
      // Look for bio text
      const bioElements = document.querySelectorAll('[data-purpose*="bio"], [class*="bio"], [class*="instructor"]');
      const bios = [...bioElements].map(e => e.textContent.trim().substring(0, 300));
      
      // JSON-LD
      const ldScripts = [...document.querySelectorAll('script[type="application/ld+json"]')];
      const ldData = ldScripts.map(s => { try { return JSON.parse(s.textContent); } catch(e) { return null; } }).filter(Boolean);
      
      // Look for website/social icons
      const socialLinks = [...document.querySelectorAll('a[href*="linkedin"], a[href*="twitter"], a[href*="youtube"], a[href*="facebook"], a[href*="instagram"], a[href*="github"]')].map(a => a.href);
      
      // External links
      const externalLinks = [...document.querySelectorAll('a')].filter(a => {
        const href = a.href;
        return href && !href.includes('udemy.com') && !href.includes('javascript:') && !href.includes('#');
      }).map(a => ({ href: a.href, text: a.textContent.trim().substring(0, 100) }));
      
      return { bodyText, allLinks: allLinks.slice(0, 20), bios, ldData, socialLinks, externalLinks: externalLinks.slice(0, 20) };
    });
    
    console.log('\n=== BODY TEXT (first 2000 chars) ===');
    console.log(profileData.bodyText);
    console.log('\n=== EXTERNAL LINKS ===');
    console.log(JSON.stringify(profileData.externalLinks, null, 2));
    console.log('\n=== SOCIAL LINKS ===');
    console.log(JSON.stringify(profileData.socialLinks, null, 2));
    console.log('\n=== JSON-LD ===');
    console.log(JSON.stringify(profileData.ldData, null, 2).substring(0, 1000));
    console.log('\n=== BIOS ===');
    console.log(JSON.stringify(profileData.bios, null, 2));
    
  } catch(e) {
    console.error('Error:', e.message);
  } finally {
    await browser.close();
  }
})();
