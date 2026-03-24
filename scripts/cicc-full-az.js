const fs = require('fs');
const scraper = require('../scrapers/contractors/cicc');

(async () => {
  const leads = [];
  let count = 0, withEmail = 0, withPhone = 0;
  const t0 = Date.now();
  
  try {
    for await (const lead of scraper.search('all', { maxPages: 9999 })) {
      if (lead._cityProgress || lead._captcha) continue;
      leads.push(lead);
      count++;
      if (lead.email) withEmail++;
      if (lead.phone) withPhone++;
      if (count % 100 === 0) console.log('Progress:', count, '|', withEmail, 'emails |', withPhone, 'phones');
    }
  } catch (err) { console.error('Error:', err.message); }

  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);
  console.log('=== CICC COMPLETE:', count, 'leads,', withEmail, 'emails (' + (count?Math.round(100*withEmail/count):0) + '%),', elapsed, 'min ===');

  if (leads.length > 0) {
    const h = ['first_name','last_name','firm_name','city','state','country','phone','email','bar_number','bar_status','profile_url','practice_area','trade_category','license_type','source'];
    const rows = leads.map(l => h.map(k => { const v=(l[k]||'').toString().replace(/"/g,'""'); return v.includes(',')||v.includes('"')||v.includes('\n')?'"'+v+'"':v; }).join(','));
    fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/cicc-immigration-consultants.csv', [h.join(','), ...rows].join('\n'));
    fs.writeFileSync('/Users/fardeenchoudhury/mortar-lead-scraper/output/cicc-emails.txt', leads.filter(l=>l.email).map(l=>l.email).join('\n'));
    console.log('CSV:', leads.length, 'rows | Emails:', leads.filter(l=>l.email).length);
  }
})();
