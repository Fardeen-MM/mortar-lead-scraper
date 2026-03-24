const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });

  const leads = [];
  const seenEmails = new Set();
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  try {
    // Load page ONCE
    console.log('Loading MCS...');
    await page.goto('https://mcscertified.com/find-an-installer/', { waitUntil: 'networkidle2', timeout: 45000 });
    await new Promise(r => setTimeout(r, 3000));

    let pageNum = 1;
    
    while (pageNum <= 500) {
      // Extract current page emails
      const html = await page.content();
      const $ = cheerio.load(html);
      
      if (pageNum === 1) {
        const m = $('body').text().match(/of\s*([\d,]+)\s*results/i);
        if (m) console.log('Total:', m[1], 'installers');
      }
      
      let newCount = 0;
      $('a[href^="mailto:"]').each((_, el) => {
        const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
        if (!email || email.includes('mcscertified') || seenEmails.has(email)) return;
        seenEmails.add(email);
        
        let $c = $(el).parent();
        for (let i = 0; i < 10; i++) { if ($c.find('h2,h3').length) break; $c = $c.parent(); }
        
        leads.push({
          firm: $c.find('h2,h3').first().text().trim(),
          email,
          phone: $c.find('a[href^="tel:"]').attr('href')?.replace('tel:', '').trim() || '',
          text: $c.text().replace(/\s+/g, ' ').substring(0, 500),
        });
        newCount++;
      });
      
      if (pageNum % 10 === 0 || pageNum <= 5) {
        console.log('Page ' + pageNum + ': ' + newCount + ' new (' + leads.length + ' total, ' + seenEmails.size + ' emails)');
      }

      // Click "Next" pagination link — use .pagination-link with "Next" text
      const firstEmailBefore = seenEmails.size;
      
      const clickResult = await page.evaluate(() => {
        const links = document.querySelectorAll('a.pagination-link');
        for (const l of links) {
          if (l.textContent.trim().includes('Next')) {
            l.click();
            return 'clicked-next';
          }
        }
        // Try page number
        const current = document.querySelector('a.pagination-link.bg-blue-950, a.pagination-link[aria-current="page"]');
        if (current) {
          const next = current.parentElement?.nextElementSibling?.querySelector('a');
          if (next) { next.click(); return 'clicked-page-' + next.textContent.trim(); }
        }
        return 'no-next';
      });
      
      if (clickResult === 'no-next') {
        console.log('No more pages');
        break;
      }
      
      // Wait for content to update
      await new Promise(r => setTimeout(r, 2500));
      
      // Verify content changed by checking if new emails appear
      const htmlAfter = await page.content();
      const $after = cheerio.load(htmlAfter);
      let hasNew = false;
      $after('a[href^="mailto:"]').each((_, el) => {
        const e = $(el).attr('href')?.replace('mailto:', '').trim().toLowerCase();
        if (e && !seenEmails.has(e)) hasNew = true;
      });
      
      if (!hasNew && pageNum > 3) {
        // Content didn't change — pagination might be broken
        await new Promise(r => setTimeout(r, 3000)); // Wait more
        const htmlRetry = await page.content();
        const $retry = cheerio.load(htmlRetry);
        let retryHasNew = false;
        $retry('a[href^="mailto:"]').each((_, el) => {
          const e = $retry(el).attr('href')?.replace('mailto:', '').trim().toLowerCase();
          if (e && !seenEmails.has(e)) retryHasNew = true;
        });
        if (!retryHasNew) {
          console.log('Content not changing after page ' + pageNum + ' — stopping');
          break;
        }
      }
      
      pageNum++;
    }
  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await browser.close();
  }

  // Parse addresses from stored text
  const finalLeads = leads.map(l => {
    const certMatch = l.text.match(/Certification Number:\s*([A-Z]+-\d+)/);
    const addrMatch = l.text.match(/Address:\s*(.+?)(?=Regions|Boiler|Certification Body|$)/);
    const addr = addrMatch ? addrMatch[1].trim().replace(/,\s*$/, '') : '';
    const pcMatch = addr.match(/([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})/i);
    return {
      firm_name: l.firm, email: l.email, phone: l.phone,
      address: addr, postcode: pcMatch ? pcMatch[1].toUpperCase() : '',
      cert: certMatch ? certMatch[1] : '', source: 'mcs_certified',
    };
  });

  const h = 'firm_name,email,phone,address,postcode,certification_number,source';
  const rows = finalLeads.map(l => [l.firm_name,l.email,l.phone,l.address,l.postcode,l.cert,l.source]
    .map(v => { const s=(v||'').replace(/"/g,'""'); return s.includes(',')||s.includes('"')?`"${s}"`:s; }).join(','));
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-solar-installers.csv', h+'\n'+rows.join('\n'));
  fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/mcs-emails.txt', finalLeads.map(l=>l.email).join('\n'));
  console.log('\n=== MCS COMPLETE: ' + finalLeads.length + ' leads, ' + seenEmails.size + ' emails ===');
})();
