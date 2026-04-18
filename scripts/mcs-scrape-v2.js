/**
 * MCS Solar Installer Scrape v2
 * Proper Puppeteer pagination: click Next, wait for content change.
 * Single browser, single tab, thorough cleanup.
 */
const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  const leads = [];
  const seenEmails = new Set();
  let totalPages = 0;

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });
    
    console.log('Loading MCS...');
    await page.goto('https://mcscertified.com/find-an-installer/', { waitUntil: 'networkidle2', timeout: 45000 });
    await new Promise(r => setTimeout(r, 3000));

    // Click "90" button to show max results per page
    const clicked90 = await page.evaluate(() => {
      const btns = document.querySelectorAll('button, a, span');
      for (const b of btns) {
        if (b.textContent.trim() === '90' && b.offsetParent !== null) {
          b.click();
          return true;
        }
      }
      return false;
    });
    console.log('Clicked 90 per page:', clicked90);
    await new Promise(r => setTimeout(r, 4000));

    // Get total
    const total = await page.evaluate(() => {
      const t = document.body.innerText;
      const m = t.match(/of\s*([\d,]+)\s*results/i) || t.match(/Showing[:\s]*([\d,]+)/i);
      return m ? parseInt(m[1].replace(/,/g, ''), 10) : 0;
    });
    console.log('Total installers:', total || 'unknown');

    // Extract function
    async function extractPage() {
      return await page.evaluate(() => {
        const results = [];
        const mailLinks = document.querySelectorAll('a[href^="mailto:"]');
        mailLinks.forEach(el => {
          const email = el.href.replace('mailto:', '').trim().toLowerCase();
          if (!email || email.includes('mcscertified')) return;
          
          // Walk up to find container
          let c = el;
          for (let i = 0; i < 10; i++) {
            c = c.parentElement;
            if (!c) break;
            if (c.querySelector('h2,h3')) break;
          }
          if (!c) return;
          
          const heading = c.querySelector('h2,h3');
          const firm = heading ? heading.textContent.trim() : '';
          const telEl = c.querySelector('a[href^="tel:"]');
          const phone = telEl ? telEl.href.replace('tel:', '').trim() : '';
          const text = c.innerText.replace(/\s+/g, ' ');
          const certMatch = text.match(/Certification Number:\s*([A-Z]+-\d+)/);
          const addrMatch = text.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
          const addr = addrMatch ? addrMatch[1].trim().replace(/,\s*$/, '') : '';
          const pcMatch = addr.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
          
          results.push({
            firm, email, phone, addr,
            postcode: pcMatch ? pcMatch[1].toUpperCase() : '',
            cert: certMatch ? certMatch[1] : '',
          });
        });
        return results;
      });
    }

    // Scrape pages
    let pageNum = 1;
    let consecutiveEmpty = 0;

    while (consecutiveEmpty < 3) {
      const pageResults = await extractPage();
      let newCount = 0;
      
      for (const r of pageResults) {
        if (seenEmails.has(r.email)) continue;
        seenEmails.add(r.email);
        leads.push(r);
        newCount++;
      }

      totalPages = pageNum;
      console.log(`Page ${pageNum}: ${pageResults.length} found, ${newCount} new (${leads.length} total)`);

      if (newCount === 0) {
        consecutiveEmpty++;
      } else {
        consecutiveEmpty = 0;
      }

      // Try to click Next
      const oldFirst = leads.length > 0 ? leads[leads.length - newCount]?.email : '';
      
      const clickedNext = await page.evaluate(() => {
        // Find "Next" or "›" pagination link
        const links = document.querySelectorAll('a, button');
        for (const l of links) {
          const text = l.textContent.trim();
          if (text === 'Next' || text === '›' || text === '»' || text === '>') {
            if (l.offsetParent !== null && !l.classList.contains('disabled')) {
              l.click();
              return true;
            }
          }
        }
        // Try clicking the next page number
        const active = document.querySelector('.pagination .active, .pagination .current');
        if (active) {
          const next = active.nextElementSibling;
          if (next && next.querySelector('a')) {
            next.querySelector('a').click();
            return true;
          } else if (next) {
            next.click();
            return true;
          }
        }
        return false;
      });

      if (!clickedNext) {
        console.log('No Next button found — done');
        break;
      }

      // Wait for page content to change
      await new Promise(r => setTimeout(r, 3000));
      pageNum++;
      
      if (pageNum > 100) break; // Safety cap
    }

  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await browser.close();
    console.log('Browser closed');
  }

  // Write output
  const h = 'firm_name,email,phone,address,postcode,certification_number,source';
  const rows = leads.map(l => {
    return [l.firm, l.email, l.phone, l.addr, l.postcode, l.cert, 'mcs_certified']
      .map(v => { const s=(v||'').replace(/"/g,'""'); return s.includes(',')||s.includes('"')?`"${s}"`:s; })
      .join(',');
  });
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-solar-installers.csv', h + '\n' + rows.join('\n'));
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-emails.txt', leads.map(l => l.email).join('\n'));
  
  console.log(`\n=== MCS COMPLETE: ${leads.length} leads, ${seenEmails.size} emails, ${totalPages} pages ===`);
})();
