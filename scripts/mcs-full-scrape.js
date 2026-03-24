const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
puppeteer.use(StealthPlugin());

const BASE = 'https://mcscertified.com/find-an-installer/';

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
  
  const leads = [];
  const seenEmails = new Set();
  let page = 1;
  const maxPages = 470; // ~5630/12 = ~469 pages
  
  console.log('MCS scrape starting...');
  
  const tab = await browser.newPage();
  await tab.setViewport({ width: 1280, height: 900 });
  
  while (page <= maxPages) {
    try {
      await tab.goto(`${BASE}?page=${page}`, { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 1500));
      
      const html = await tab.content();
      const $ = cheerio.load(html);
      
      // Get total on first page
      if (page === 1) {
        const totalMatch = $('body').text().match(/Showing:\s*\d+\s*of\s*([\d,]+)/i);
        if (totalMatch) console.log('Total installers:', totalMatch[1]);
      }
      
      // Extract emails from mailto links
      let pageCount = 0;
      $('a[href^="mailto:"]').each((_, el) => {
        const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
        if (!email || email.includes('mcscertified') || seenEmails.has(email)) return;
        seenEmails.add(email);
        
        // Walk up to find container
        let $c = $(el).parent();
        for (let i = 0; i < 8; i++) { if ($c.find('h2,h3').length) break; $c = $c.parent(); }
        
        const firm = $c.find('h2,h3').first().text().trim();
        let phone = '';
        const $tel = $c.find('a[href^="tel:"]');
        if ($tel.length) phone = $tel.attr('href').replace('tel:', '').trim();
        
        const text = $c.text().replace(/\s+/g, ' ');
        const certMatch = text.match(/Certification Number:\s*([A-Z]+-\d+)/);
        const addrMatch = text.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
        let address = addrMatch ? addrMatch[1].trim().replace(/,\s*$/, '') : '';
        let postcode = '';
        const pcMatch = (address || text).match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        if (pcMatch) postcode = pcMatch[1].toUpperCase();
        
        leads.push({
          firm_name: firm, email, phone, address,
          zip: postcode, state: 'UK', country: 'UK',
          bar_number: certMatch ? certMatch[1] : '',
          source: 'mcs_certified',
          practice_area: 'Solar/Renewable Energy',
        });
        pageCount++;
      });
      
      if (pageCount === 0 && page > 1) {
        console.log(`Page ${page}: 0 new emails - stopping`);
        break;
      }
      
      if (page % 10 === 0 || page <= 3) {
        console.log(`Page ${page}: ${pageCount} new (${leads.length} total, ${seenEmails.size} emails)`);
      }
    } catch (err) {
      console.error(`Page ${page} error: ${err.message}`);
      if (page > 5) break; // Only retry early pages
    }
    page++;
  }
  
  await browser.close();
  
  // Write CSV
  const h = ['firm_name','email','phone','address','zip','state','country','bar_number','source','practice_area'];
  const rows = leads.map(l => h.map(k => {
    const v = (l[k]||'').replace(/"/g,'""');
    return v.includes(',') || v.includes('"') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync('output/mcs-solar-installers.csv', [h.join(','), ...rows].join('\n'));
  fs.writeFileSync('output/mcs-emails.txt', leads.map(l => l.email).join('\n'));
  
  console.log(`\n=== MCS COMPLETE: ${leads.length} leads, ${seenEmails.size} emails ===`);
  console.log(`CSV: output/mcs-solar-installers.csv`);
  console.log(`Emails: output/mcs-emails.txt`);
})();
