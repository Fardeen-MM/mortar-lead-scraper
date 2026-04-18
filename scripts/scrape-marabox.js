#!/usr/bin/env node
/**
 * Marabox.com.au Migration Agent Scraper
 * Scrapes ALL Australian MARA agents (~7,000+) with email, phone, address
 */
const https = require('https');
const fs = require('fs');

const BASE_URL = 'https://www.marabox.com.au/lists/agents';
const OUTPUT = 'output/australia-mara-agents.csv';

function fetchPage(page) {
  const url = page === 1 ? BASE_URL : `${BASE_URL}?page=${page}`;
  return new Promise(resolve => {
    https.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', Accept: 'text/html' },
      timeout: 15000,
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => resolve(body));
      res.on('error', () => resolve(''));
    }).on('error', () => resolve(''));
  });
}

function extractAgents(html) {
  const agents = [];
  // Find all agent JSON blocks using the known structure
  // Pattern: "mara_number":"XXXX" ... "name":{"en":"XXX"} ... "email":"XXX" ... "phone":"XXX"

  // Extract the __NEXT_DATA__ script tag
  const nextDataMatch = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (!nextDataMatch) return [];

  try {
    const data = JSON.parse(nextDataMatch[1]);
    const queries = data?.props?.pageProps?.dehydratedState?.queries || [];

    for (const q of queries) {
      const pages = q?.state?.data?.pages || [q?.state?.data];
      for (const page of pages) {
        const items = page?.data || page?.items || page || [];
        if (!Array.isArray(items)) continue;
        for (const agent of items) {
          if (!agent?.mara_number) continue;
          const name = agent.name?.en || '';
          const marn = agent.mara_number || '';
          const agency = agent.agencies?.[0] || {};
          const contact = agency.contact || {};
          agents.push({
            name,
            marn,
            email: contact.email || '',
            phone: contact.phone || '',
            address: (contact.full_address || '').replace(/\n/g, ', '),
            firm: agency.title?.en || agency._key || '',
            city: agency.city || '',
            state: agency.state || '',
            country: agency.country || 'Australia',
          });
        }
      }
    }
  } catch (e) {
    // Fallback: regex extraction
    const blocks = html.match(/"mara_number":"(\d+)"[\s\S]*?"en":"([^"]+)"[\s\S]*?"email":"([^"]*)"[\s\S]*?"phone":"([^"]*)"/g) || [];
    for (const block of blocks) {
      const marn = block.match(/"mara_number":"(\d+)"/)?.[1] || '';
      const name = block.match(/"en":"([^"]+)"/)?.[1] || '';
      const email = block.match(/"email":"([^"]*)"/)?.[1] || '';
      const phone = block.match(/"phone":"([^"]*)"/)?.[1] || '';
      if (marn) agents.push({ name, marn, email, phone, address: '', firm: '', city: '', state: '', country: 'Australia' });
    }
  }

  return agents;
}

function esc(v) {
  const s = (v ?? '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

(async () => {
  console.log('Scraping Australian MARA agents from marabox.com.au...\n');

  const all = [];
  const seen = new Set();
  let page = 1;
  let emptyStreak = 0;

  while (emptyStreak < 5) {
    const html = await fetchPage(page);
    const agents = extractAgents(html);

    if (agents.length === 0) {
      emptyStreak++;
    } else {
      emptyStreak = 0;
      for (const a of agents) {
        if (!seen.has(a.marn)) {
          seen.add(a.marn);
          all.push(a);
        }
      }
    }

    if (page % 10 === 0) console.log(`  Page ${page} | ${all.length} agents (${agents.length} this page)`);
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  // Write CSV
  const header = 'name,marn,email,phone,firm,address,city,state,country';
  const rows = all.map(a => [esc(a.name), a.marn, esc(a.email), esc(a.phone), esc(a.firm), esc(a.address), esc(a.city), a.state, a.country].join(','));
  fs.writeFileSync(OUTPUT, header + '\n' + rows.join('\n'));

  console.log(`\n✓ Done: ${all.length} agents → ${OUTPUT}`);
  console.log(`  With email: ${all.filter(a => a.email).length}`);
  console.log(`  With phone: ${all.filter(a => a.phone).length}`);
})();
