const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

  // Check if state page has pagination
  console.log('=== JUSTIA STATE PAGE 2 ===');
  await page.goto('https://www.justia.com/lawyers/immigration-law/california?page=2', { waitUntil: 'networkidle2', timeout: 30000 });

  const data1 = await page.evaluate(() => {
    const cards = document.querySelectorAll('.jld-card');
    return { cardCount: cards.length, title: document.title };
  });
  console.log('State page 2 cards:', data1.cardCount, 'Title:', data1.title);

  // Now analyze FindLaw state page
  console.log('\n=== FINDLAW CALIFORNIA ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/california', { waitUntil: 'networkidle2', timeout: 30000 });

  const flStateData = await page.evaluate(() => {
    const result = { title: document.title, jsonLD: [], cardSelectors: {}, cityLinks: [], pagination: [], allLinks: [] };

    // JSON-LD
    document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
      try { result.jsonLD.push(JSON.parse(s.textContent)); } catch(e) {}
    });

    // Look for listing cards
    const possibleSelectors = [
      '.lawyer-card', '.attorney-card', '.listing-item', '.search-result',
      '[data-listing-id]', '.fl-card', '.fl-listing', '.result-item',
      '.attorney-listing', 'article', '.profile-card', '.serp-item',
      '.v-lawyer-result', '.legal-service', '.law-firm-listing'
    ];

    for (const sel of possibleSelectors) {
      const els = document.querySelectorAll(sel);
      if (els.length > 0) {
        result.cardSelectors[sel] = els.length;
      }
    }

    // All links containing findlaw
    document.querySelectorAll('a').forEach(a => {
      const href = a.href;
      const text = a.textContent.trim();
      if (href.includes('/immigration-naturalization-law/california/') && text.length > 1) {
        result.cityLinks.push({ text, href });
      }
    });

    // Pagination
    document.querySelectorAll('a[href*="page="], .pagination a, nav.pagination a').forEach(a => {
      result.pagination.push({ text: a.textContent.trim(), href: a.href });
    });

    return result;
  });

  console.log('Title:', flStateData.title);
  console.log('JSON-LD count:', flStateData.jsonLD.length);
  if (flStateData.jsonLD.length > 0) {
    flStateData.jsonLD.forEach((j, i) => {
      const items = j.mainEntity?.itemListElement?.length || j.itemListElement?.length || 'none';
      console.log(`  JSON-LD ${i} - type: ${j['@type']}, items: ${items}`);
      if (j.mainEntity && j.mainEntity.itemListElement) {
        const first = j.mainEntity.itemListElement[0];
        console.log('  First item:', JSON.stringify(first).substring(0, 500));
      }
    });
  }
  console.log('Card selectors:', flStateData.cardSelectors);
  console.log('City links:', flStateData.cityLinks.length);
  console.log('Sample cities:', flStateData.cityLinks.slice(0, 8));
  console.log('Pagination:', flStateData.pagination);

  // Now try a city-level FindLaw page
  console.log('\n=== FINDLAW LOS ANGELES ===');
  await page.goto('https://lawyers.findlaw.com/immigration-naturalization-law/california/los-angeles', { waitUntil: 'networkidle2', timeout: 30000 });

  const flCityData = await page.evaluate(() => {
    const result = { title: document.title, jsonLD: [], cards: 0, pagination: [], sampleHTML: '' };

    document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
      try { result.jsonLD.push(JSON.parse(s.textContent)); } catch(e) {}
    });

    // Try various selectors
    const selectors = ['.serp-item', '.result-item', '.listing-item', 'article', '.v-lawyer-result'];
    for (const sel of selectors) {
      const els = document.querySelectorAll(sel);
      if (els.length > 0) {
        result.cards = els.length;
        result.cardSelector = sel;
        break;
      }
    }

    // Pagination
    document.querySelectorAll('a[href*="page="]').forEach(a => {
      result.pagination.push({ text: a.textContent.trim(), href: a.href });
    });

    // Sample of body
    result.sampleHTML = document.body.innerHTML.substring(0, 5000);

    return result;
  });

  console.log('Title:', flCityData.title);
  console.log('JSON-LD:', flCityData.jsonLD.length);
  if (flCityData.jsonLD.length > 0) {
    flCityData.jsonLD.forEach((j, i) => {
      const items = j.mainEntity?.itemListElement?.length || j.itemListElement?.length || 'none';
      console.log(`  JSON-LD ${i} - type: ${j['@type']}, items: ${items}, numberOfItems: ${j.mainEntity?.numberOfItems || 'N/A'}`);
      if (j.mainEntity && j.mainEntity.itemListElement) {
        const first = j.mainEntity.itemListElement[0];
        console.log('  First item:', JSON.stringify(first).substring(0, 500));
      }
    });
  }
  console.log('Cards:', flCityData.cards, 'selector:', flCityData.cardSelector);
  console.log('Pagination:', flCityData.pagination.slice(0, 5));

  await browser.close();
})().catch(e => console.error(e));
