/**
 * MCS Full Scrape — Puppeteer with in-page pagination
 * Single browser, single tab, click page numbers to paginate.
 * Outputs: output/mcs-solar-installers.csv + output/mcs-emails.txt
 */
const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
puppeteer.use(StealthPlugin());

const BASE = 'https://mcscertified.com/find-an-installer/';

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--single-process'],
  });
  
  const leads = [];
  const seenEmails = new Set();
  
  try {
    const tab = await browser.newPage();
    await tab.setViewport({ width: 1280, height: 900 });
    
    // Load initial page
    console.log('Loading MCS find-an-installer...');
    await tab.goto(BASE, { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));
    
    // Click "Show 90" to get max results per page
    const show90 = await tab.evaluate(() => {
      const btns = document.querySelectorAll('button, a');
      for (const b of btns) {
        if (b.textContent.trim() === '90') { b.click(); return true; }
      }
      return false;
    });
    if (show90) {
      console.log('Set page size to 90');
      await new Promise(r => setTimeout(r, 3000));
    }
    
    // Get total count
    const total = await tab.evaluate(() => {
      const m = document.body.innerText.match(/Showing:\s*\d+\s*of\s*([\d,]+)/i);
      return m ? parseInt(m[1].replace(/,/g, ''), 10) : 0;
    });
    console.log('Total installers:', total);
    
    const maxPages = Math.ceil(total / 90) + 1;
    let currentPage = 1;
    let consecutiveEmpty = 0;
    
    while (currentPage <= maxPages && consecutiveEmpty < 3) {
      const html = await tab.content();
      const $ = cheerio.load(html);
      
      let pageNew = 0;
      $('a[href^="mailto:"]').each((_, el) => {
        const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
        if (!email || email.includes('mcscertified') || seenEmails.has(email)) return;
        seenEmails.add(email);
        
        let $c = $(el).parent();
        for (let i = 0; i < 8; i++) { if ($c.find('h2,h3').length) break; $c = $c.parent(); }
        
        const firm = $c.find('h2,h3').first().text().trim();
        let phone = '';
        const $tel = $c.find('a[href^="tel:"]');
        if ($tel.length) phone = $tel.attr('href').replace('tel:', '').trim();
        
        const text = $c.text().replace(/\s+/g, ' ');
        const certMatch = text.match(/Certification Number:\s*([A-Z]+-\d+)/);
        const addrMatch = text.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
        const address = addrMatch ? addrMatch[1].trim().replace(/,\s*$/, '') : '';
        const pcMatch = (address || text).match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
        const postcode = pcMatch ? pcMatch[1].toUpperCase() : '';
        
        leads.push({ firm_name: firm, email, phone, address, zip: postcode, state: 'UK', country: 'UK', bar_number: certMatch ? certMatch[1] : '', source: 'mcs_certified', practice_area: 'Solar/Renewable Energy' });
        pageNew++;
      });
      
      console.log(`Page ${currentPage}: ${pageNew} new (${leads.length} total, ${seenEmails.size} emails)`);
      
      if (pageNew === 0) {
        consecutiveEmpty++;
      } else {
        consecutiveEmpty = 0;
      }
      
      // Click next page
      const hasNext = await tab.evaluate(() => {
        const links = document.querySelectorAll('.pagination a, a[href*="page="]');
        for (const l of links) {
          if (l.textContent.trim().toLowerCase() === 'next' || l.textContent.trim() === '›') {
            l.click();
            return true;
          }
        }
        // Try clicking next page number
        const pageNums = document.querySelectorAll('.pagination a');
        let maxPage = 0;
        let currentActive = null;
        for (const l of pageNums) {
          const t = l.textContent.trim();
          if (l.classList.contains('active') || l.classList.contains('current')) {
            currentActive = parseInt(t, 10);
          }
        }
        if (currentActive) {
          for (const l of pageNums) {
            if (parseInt(l.textContent.trim(), 10) === currentActive + 1) {
              l.click();
              return true;
            }
          }
        }
        return false;
      });
      
      if (!hasNext) {
        console.log('No next page button found — stopping');
        break;
      }
      
      await new Promise(r => setTimeout(r, 2000));
      currentPage++;
    }
  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await browser.close();
  }
  
  // Write outputs
  const h = ['firm_name','email','phone','address','zip','state','country','bar_number','source','practice_area'];
  const rows = leads.map(l => h.map(k => {
    const v = (l[k]||'').replace(/"/g,'""');
    return v.includes(',') || v.includes('"') ? `"${v}"` : v;
  }).join(','));
  fs.writeFileSync('output/mcs-solar-installers.csv', [h.join(','), ...rows].join('\n'));
  fs.writeFileSync('output/mcs-emails.txt', leads.map(l => l.email).join('\n'));
  
  console.log(`\n=== MCS COMPLETE: ${leads.length} leads, ${seenEmails.size} emails ===`);
})();
