const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  const leads = [];
  const seenEmails = new Set();
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  try {
    let pageNum = 1;
    let maxPage = 470;
    
    while (pageNum <= maxPage) {
      const url = 'https://mcscertified.com/find-an-installer/?page=' + pageNum;
      console.log('Page ' + pageNum + ': ' + url);
      
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 2000));
      
      const html = await page.content();
      const $ = cheerio.load(html);
      
      // Get total on first page
      if (pageNum === 1) {
        const totalText = $('body').text();
        const m = totalText.match(/of\s*([\d,]+)\s*results/i);
        if (m) {
          const total = parseInt(m[1].replace(/,/g, ''), 10);
          maxPage = Math.ceil(total / 12);
          console.log('Total: ' + total + ' (' + maxPage + ' pages)');
        }
      }
      
      let newCount = 0;
      $('a[href^="mailto:"]').each((_, el) => {
        const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
        if (!email || email.includes('mcscertified') || seenEmails.has(email)) return;
        seenEmails.add(email);
        
        let $c = $(el).parent();
        for (let i = 0; i < 10; i++) { if ($c.find('h2,h3').length) break; $c = $c.parent(); }
        
        const firm = $c.find('h2,h3').first().text().trim();
        const $tel = $c.find('a[href^="tel:"]');
        const phone = $tel.length ? $tel.attr('href').replace('tel:', '').trim() : '';
        const text = $c.text().replace(/\s+/g, ' ');
        const certMatch = text.match(/Certification Number:\s*([A-Z]+-\d+)/);
        const addrMatch = text.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
        const addr = addrMatch ? addrMatch[1].trim().replace(/,\s*$/, '') : '';
        const pcMatch = addr.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        
        leads.push({
          firm, email, phone, addr,
          postcode: pcMatch ? pcMatch[1].toUpperCase() : '',
          cert: certMatch ? certMatch[1] : '',
        });
        newCount++;
      });
      
      if (pageNum % 25 === 0 || pageNum <= 3) {
        console.log('  -> ' + newCount + ' new (' + leads.length + ' total)');
      }
      
      if (newCount === 0 && pageNum > 1) {
        console.log('  -> 0 new emails — stopping');
        break;
      }
      
      pageNum++;
    }
  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await browser.close();
  }

  const h = 'firm_name,email,phone,address,postcode,certification_number,source';
  const rows = leads.map(l => [l.firm, l.email, l.phone, l.addr, l.postcode, l.cert, 'mcs_certified']
    .map(v => { const s=(v||'').replace(/"/g,'""'); return s.includes(',')||s.includes('"')?`"${s}"`:s; }).join(','));
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-solar-installers.csv', h + '\n' + rows.join('\n'));
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-emails.txt', leads.map(l => l.email).join('\n'));
  console.log('\n=== MCS COMPLETE: ' + leads.length + ' leads, ' + seenEmails.size + ' emails ===');
})();
