
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
puppeteer.use(StealthPlugin());

const LETTER = process.argv[2];
const SEARCH_URL = 'https://register.college-ic.ca/Public-Register-EN/RCIC_Search.aspx';

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  const results = [];
  
  try {
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(60000);
    page.setDefaultTimeout(30000);
    await page.setViewport({ width: 1280, height: 900 });
    
    await page.goto(SEARCH_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    await new Promise(r => setTimeout(r, 2000));
    
    // Type letter into Last Name field
    await page.type('input[id*="Input1_TextBox1"]', LETTER);
    await page.click('input[id*="SubmitButton"]');
    
    try {
      await page.waitForSelector('table[id*="ResultsGrid_Grid1"] tr.rgRow', { timeout: 20000 });
    } catch { process.stdout.write(JSON.stringify([])); process.exit(0); }
    
    await new Promise(r => setTimeout(r, 2000));
    
    // Set page size to 50
    try {
      const combo = await page.$('div[id*="PageSizeComboBox"]');
      if (combo) {
        await combo.click();
        await new Promise(r => setTimeout(r, 500));
        const items = await page.$$('li[class*="rcbItem"]');
        for (const item of items) {
          const text = await page.evaluate(el => el.textContent.trim(), item);
          if (text === '50') { await item.click(); break; }
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    } catch {}
    
    // Extract rows
    const rows = await page.evaluate(() => {
      const results = [];
      document.querySelectorAll('table[id*="ResultsGrid_Grid1"] tr.rgRow, table[id*="ResultsGrid_Grid1"] tr.rgAltRow').forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 5) return;
        let profileId = null;
        const link = row.querySelector('a[href*="ID="]');
        if (link) { const m = link.href.match(/ID=(\d+)/); if (m) profileId = m[1]; }
        if (!profileId) {
          for (const c of cells) { if (c.style.display === 'none' && /^\d{3,8}$/.test(c.textContent.trim())) { profileId = c.textContent.trim(); break; } }
        }
        if (!profileId) return;
        const vis = Array.from(cells).filter(c => c.style.display !== 'none').map(c => c.textContent.trim());
        results.push({ id: profileId, collegeId: vis[1]||'', name: vis[2]||'', company: vis[3]||'', type: vis[4]||'' });
      });
      return results;
    });
    
    // Fetch each profile
    for (const row of rows) {
      try {
        await new Promise(r => setTimeout(r, 400));
        const ppage = await browser.newPage();
        ppage.setDefaultNavigationTimeout(45000);
        await ppage.goto('https://register.college-ic.ca/Pr_Profile_RCIC?ID=' + row.id, { waitUntil: 'networkidle2', timeout: 45000 });
        await new Promise(r => setTimeout(r, 800));
        
        // Click Licensee Details tab
        const tabSel = await ppage.evaluate(() => {
          const tabs = document.querySelectorAll('.rtsLI a, .rtsLink');
          for (const t of tabs) { if (t.textContent.trim().toLowerCase().includes('licensee details')) { t.setAttribute('data-tab','ld'); return '[data-tab="ld"]'; } }
          return null;
        });
        
        let email = '', phone = '', city = '', province = '', company = '';
        if (tabSel) {
          await Promise.all([ppage.waitForNavigation({ waitUntil: 'networkidle2', timeout: 20000 }), ppage.click(tabSel)]);
          await new Promise(r => setTimeout(r, 500));
          
          const emp = await ppage.evaluate(() => {
            const rows = document.querySelectorAll('table[id*="Employment"] tr.rgRow, table[id*="Employment"] tr.rgAltRow');
            if (!rows.length) return null;
            const cells = rows[0].querySelectorAll('td');
            if (cells.length < 7) return null;
            const cfEl = cells[5].querySelector('[data-cfemail]');
            let email = '';
            if (cfEl) {
              const enc = cfEl.getAttribute('data-cfemail');
              const key = parseInt(enc.substring(0,2),16);
              for (let i=2; i<enc.length; i+=2) email += String.fromCharCode(parseInt(enc.substring(i,i+2),16)^key);
            } else {
              const t = cells[5].textContent.trim();
              if (t.includes('@')) email = t;
            }
            return { company: cells[0].textContent.trim(), city: cells[4].textContent.trim(), province: cells[3].textContent.trim(), email: email.toLowerCase(), phone: cells[6].textContent.trim() };
          });
          if (emp) { email = emp.email; phone = emp.phone; city = emp.city; province = emp.province; company = emp.company; }
        }
        
        const name = (row.name || '').replace(/\s*[-.]$/, '').trim();
        const parts = name.split(/\s+/);
        
        results.push({
          first_name: parts[0] || '', last_name: parts.slice(1).join(' ') || '',
          firm_name: company || row.company || '', city, state: province, country: 'CA',
          phone, email, bar_number: row.collegeId, bar_status: row.type,
          profile_url: 'https://register.college-ic.ca/Public-Register-EN/Licensee/Profile.aspx?ID=' + row.id,
          source: 'cicc_register'
        });
        
        await ppage.close();
      } catch (e) { /* skip profile */ }
    }
  } catch (e) { console.error(e.message); }
  
  await browser.close();
  process.stdout.write(JSON.stringify(results));
})();
