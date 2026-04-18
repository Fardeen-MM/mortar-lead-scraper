const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

  await page.goto('https://www.justia.com/lawyers/immigration-law/california', { waitUntil: 'networkidle2', timeout: 30000 });

  // Get location text from ALL cards
  const data = await page.evaluate(() => {
    const cards = document.querySelectorAll('.jld-card');
    return Array.from(cards).slice(0, 5).map(card => {
      const outline = card.querySelector('.outline');
      const name = card.querySelector('.name a');
      return {
        name: name ? name.textContent.trim() : '',
        location: outline ? outline.textContent.trim() : '',
        locationHTML: outline ? outline.innerHTML : '',
      };
    });
  });

  console.log('State-level card locations:');
  data.forEach((d, i) => console.log(`  ${i+1}. "${d.name}" -> "${d.location}"`));

  // Now check a city page
  await page.goto('https://www.justia.com/lawyers/immigration-law/california/fresno', { waitUntil: 'networkidle2', timeout: 30000 });

  const cityData = await page.evaluate(() => {
    const cards = document.querySelectorAll('.jld-card');
    return Array.from(cards).slice(0, 5).map(card => {
      const outline = card.querySelector('.outline');
      const name = card.querySelector('.name a');
      return {
        name: name ? name.textContent.trim() : '',
        location: outline ? outline.textContent.trim() : '',
      };
    });
  });

  console.log('\nCity-level card locations (Fresno):');
  cityData.forEach((d, i) => console.log(`  ${i+1}. "${d.name}" -> "${d.location}"`));

  await browser.close();
})().catch(e => console.error(e));
