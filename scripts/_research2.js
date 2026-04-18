const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

  // FindLaw - get full JSON-LD item structure
  console.log('=== FINDLAW JSON-LD STRUCTURE ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/california/los-angeles/', { waitUntil: 'networkidle2', timeout: 30000 });

  const flData = await page.evaluate(() => {
    const result = { items: [], total: 0, pagination: [] };
    document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
      try {
        const json = JSON.parse(s.textContent);
        if (json['@type'] === 'CollectionPage' && json.mainEntity) {
          result.total = parseInt(json.mainEntity.numberOfItems || '0', 10);
          result.items = json.mainEntity.itemListElement || [];
        }
      } catch(e) {}
    });
    // Pagination
    document.querySelectorAll('a[href*="page="]').forEach(a => {
      const m = a.href.match(/page=(\d+)/);
      if (m) result.pagination.push(parseInt(m[1]));
    });
    return result;
  });

  console.log('Total items:', flData.total);
  console.log('Items on page:', flData.items.length);
  console.log('Pagination pages:', [...new Set(flData.pagination)]);
  console.log('\nFirst 3 items (full):');
  flData.items.slice(0, 3).forEach((item, i) => {
    console.log(`\n--- Item ${i+1} ---`);
    console.log(JSON.stringify(item, null, 2));
  });

  // FindLaw page 2
  console.log('\n=== FINDLAW PAGE 2 ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/california/los-angeles/?page=2', { waitUntil: 'networkidle2', timeout: 30000 });

  const flPage2 = await page.evaluate(() => {
    const result = { items: [], total: 0 };
    document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
      try {
        const json = JSON.parse(s.textContent);
        if (json['@type'] === 'CollectionPage' && json.mainEntity) {
          result.total = parseInt(json.mainEntity.numberOfItems || '0', 10);
          result.items = json.mainEntity.itemListElement || [];
        }
      } catch(e) {}
    });
    return result;
  });
  console.log('Page 2 items:', flPage2.items.length);

  // FindLaw - get state listing page to understand total states
  console.log('\n=== FINDLAW MAIN PAGE - STATE LINKS ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/', { waitUntil: 'networkidle2', timeout: 30000 });

  const flStates = await page.evaluate(() => {
    const links = [];
    document.querySelectorAll('a').forEach(a => {
      const m = a.href.match(/\/immigration-naturalization-law\/([a-z-]+)\/?$/);
      if (m && m[1] !== 'california') {
        links.push({ state: m[1], text: a.textContent.trim() });
      }
    });
    // Dedupe
    const seen = new Set();
    return links.filter(l => {
      if (seen.has(l.state)) return false;
      seen.add(l.state);
      return true;
    });
  });
  console.log('States found:', flStates.length);
  console.log('All states:', flStates.map(s => s.state));

  // Justia - get full card extraction
  console.log('\n=== JUSTIA FULL CARD EXTRACTION ===');
  await page.goto('https://www.justia.com/lawyers/immigration-law/california/los-angeles', { waitUntil: 'networkidle2', timeout: 30000 });

  const justiaCards = await page.evaluate(() => {
    const cards = document.querySelectorAll('.jld-card');
    const results = [];
    cards.forEach((card, i) => {
      if (i >= 5) return;
      const nameEl = card.querySelector('.name a');
      const phoneEl = card.querySelector('a[href^="tel:"]');
      const websiteEl = card.querySelector('a[data-vars-action*="Website"]');
      const locationEl = card.querySelector('.outline');
      const ratingEl = card.querySelector('.rating strong');
      const descEl = card.querySelector('.description');

      // Get all text content cleanly
      results.push({
        name: nameEl ? nameEl.title || nameEl.textContent.trim() : '',
        profileUrl: nameEl ? nameEl.href : '',
        phone: phoneEl ? phoneEl.textContent.trim() : '',
        phoneHref: phoneEl ? phoneEl.href : '',
        website: websiteEl ? websiteEl.href : '',
        location: locationEl ? locationEl.textContent.trim() : '',
        rating: ratingEl ? ratingEl.textContent.trim() : '',
        description: descEl ? descEl.textContent.trim().substring(0, 200) : '',
        isPremium: card.classList.contains('-premium'),
      });
    });
    return { total: cards.length, results };
  });

  console.log('Total cards:', justiaCards.total);
  justiaCards.results.forEach((r, i) => {
    console.log(`\n--- Justia Card ${i+1} ---`);
    console.log(JSON.stringify(r, null, 2));
  });

  // Justia - check how many pages for a bigger state (Texas)
  console.log('\n=== JUSTIA TEXAS - SIZE CHECK ===');
  await page.goto('https://www.justia.com/lawyers/immigration-law/texas', { waitUntil: 'networkidle2', timeout: 30000 });

  const txData = await page.evaluate(() => {
    const cards = document.querySelectorAll('.jld-card').length;
    const cityLinks = [];
    document.querySelectorAll('a').forEach(a => {
      if (a.href.match(/\/immigration-law\/texas\/[a-z]/)) {
        cityLinks.push(a.textContent.trim());
      }
    });
    return { cards, cities: [...new Set(cityLinks)] };
  });
  console.log('TX state page cards:', txData.cards);
  console.log('TX cities:', txData.cities.length, txData.cities.slice(0, 10));

  await browser.close();
})().catch(e => console.error(e));
