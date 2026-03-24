const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  
  await page.goto('https://mcscertified.com/find-an-installer/', { waitUntil: 'networkidle2', timeout: 45000 });
  await new Promise(r => setTimeout(r, 3000));

  // Get ALL pagination-related HTML
  const paginationHTML = await page.evaluate(() => {
    // Find pagination elements
    const pag = document.querySelector('.pagination, .page-numbers, nav[aria-label*="pagination"], .wp-pagenavi');
    if (pag) return 'FOUND: ' + pag.outerHTML.substring(0, 2000);
    
    // Search for any element with page links
    const allLinks = document.querySelectorAll('a[href*="page"], a[href*="paged"]');
    if (allLinks.length) {
      return 'LINKS: ' + Array.from(allLinks).map(a => a.outerHTML).join('\n').substring(0, 2000);
    }
    
    // Search for numbered links that look like pagination
    const nums = [];
    document.querySelectorAll('a').forEach(a => {
      const t = a.textContent.trim();
      if (/^\d+$/.test(t) && parseInt(t) <= 500) {
        nums.push({ text: t, href: a.href, class: a.className, parent: a.parentElement?.className });
      }
    });
    if (nums.length) return 'NUMBERED: ' + JSON.stringify(nums.slice(0, 20));
    
    // Check for "Next" text
    const nexts = [];
    document.querySelectorAll('a, button, span').forEach(el => {
      const t = el.textContent.trim().toLowerCase();
      if (t === 'next' || t === '›' || t === '»' || t.includes('next page')) {
        nexts.push({ tag: el.tagName, text: t, href: el.href || '', class: el.className, visible: el.offsetParent !== null });
      }
    });
    if (nexts.length) return 'NEXT BUTTONS: ' + JSON.stringify(nexts);
    
    return 'NO PAGINATION FOUND';
  });
  
  console.log(paginationHTML);
  
  // Also get the "show X results" buttons
  const showButtons = await page.evaluate(() => {
    const btns = [];
    document.querySelectorAll('a, button, span').forEach(el => {
      const t = el.textContent.trim();
      if (['12', '30', '60', '90'].includes(t)) {
        btns.push({ tag: el.tagName, text: t, class: el.className, href: el.href || '', onclick: el.getAttribute('onclick') || '' });
      }
    });
    return btns;
  });
  console.log('\nSHOW BUTTONS:', JSON.stringify(showButtons, null, 2));

  await browser.close();
})();
