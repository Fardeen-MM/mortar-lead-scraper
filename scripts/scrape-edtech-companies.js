#!/usr/bin/env node
/**
 * EdTech Company Scraper — Multi-source scraper for education technology companies
 *
 * Sources:
 *   1. BuiltIn.com — 3,133 EdTech companies (listing pages with JSON-LD structured data)
 *   2. Y Combinator — Education startups via Algolia API (includes websites)
 *
 * Strategy:
 *   - BuiltIn listing pages return 200 (detail pages are 403-blocked)
 *   - JSON-LD on listing pages gives company name + description
 *   - data-company-alias attributes give slugs and IDs
 *   - For companies without websites, we guess domain from name and verify via HTTP HEAD
 *   - YC Algolia API gives full structured data including websites
 *
 * Usage:
 *   node scripts/scrape-edtech-companies.js                         # All sources, all pages
 *   node scripts/scrape-edtech-companies.js --max-pages 10          # Limit BuiltIn pages
 *   node scripts/scrape-edtech-companies.js --source builtin        # Only BuiltIn
 *   node scripts/scrape-edtech-companies.js --source yc             # Only YC
 *   node scripts/scrape-edtech-companies.js --verify-domains        # HTTP-check guessed domains
 *   node scripts/scrape-edtech-companies.js --enrich                # Crawl websites for emails
 *   node scripts/scrape-edtech-companies.js --concurrency 5         # Parallel workers
 *
 * Output: output/edtech-companies.csv
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ─── CLI Args ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const MAX_PAGES = parseInt(getArg('max-pages') || '0') || 0;
const SOURCE = getArg('source') || 'all';
const CONCURRENCY = parseInt(getArg('concurrency') || '5') || 5;
const DO_ENRICH = hasFlag('enrich');
const VERIFY_DOMAINS = hasFlag('verify-domains');
const OUTPUT = getArg('output') || path.join(__dirname, '..', 'output', 'edtech-companies.csv');

// ─── HTTP Helpers ──────────────────────────────────────────────────────────────

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === 'https:' ? https : http;
    const reqOpts = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': UA,
        'Accept': options.accept || 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        ...(options.headers || {}),
      },
      timeout: options.timeout || 15000,
    };

    const req = mod.request(reqOpts, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        const redirectUrl = new URL(res.headers.location, url).href;
        return resolve({ status: res.statusCode, redirect: redirectUrl, headers: res.headers });
      }
      let data = '';
      res.setEncoding('utf-8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, headers: res.headers }));
    });

    req.on('error', err => resolve({ status: 0, body: '', error: err.message }));
    req.on('timeout', () => { req.destroy(); resolve({ status: 0, body: '', error: 'timeout' }); });

    if (options.body) req.write(options.body);
    req.end();
  });
}

function httpPost(url, body, headers = {}) {
  return httpGet(url, {
    method: 'POST',
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json', ...headers },
    accept: 'application/json',
  });
}

function httpHead(url) {
  return httpGet(url, { method: 'HEAD', timeout: 8000 });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── CSV Helpers ───────────────────────────────────────────────────────────────

function writeCSV(filePath, leads) {
  if (!leads.length) { console.log('No leads to write.'); return; }
  const columns = [
    'company_name', 'website', 'domain', 'website_confirmed', 'description', 'headquarters',
    'employee_count', 'year_founded', 'industry_tags', 'source',
    'profile_url', 'email', 'phone', 'batch', 'status',
  ];
  const escape = (v) => {
    if (v == null) return '';
    const s = String(v).replace(/"/g, '""');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
  };
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const rows = [columns.join(',')];
  for (const lead of leads) {
    rows.push(columns.map(c => escape(lead[c])).join(','));
  }
  fs.writeFileSync(filePath, rows.join('\n'), 'utf-8');
  console.log(`\n  Wrote ${leads.length} leads to ${filePath}`);
}

// ─── Dedup ─────────────────────────────────────────────────────────────────────

const seen = new Set();
function isDupe(company) {
  const key = (company.company_name || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!key || key.length < 2 || seen.has(key)) return true;
  // Also dedup by domain
  if (company.domain) {
    const domKey = company.domain.toLowerCase().replace(/^www\./, '');
    if (seen.has(`dom:${domKey}`)) return true;
    seen.add(`dom:${domKey}`);
  }
  seen.add(key);
  return false;
}

// ─── Domain Guesser ────────────────────────────────────────────────────────────

function guessDomain(companyName) {
  if (!companyName) return null;
  // Clean the name
  let clean = companyName
    .replace(/\(.*?\)/g, '')             // Remove parenthetical
    .replace(/[®™©]/g, '')              // Remove trademark symbols
    .replace(/[^a-zA-Z0-9\s-]/g, '')     // Remove special chars
    .trim()
    .toLowerCase();

  // Generate candidates
  const words = clean.split(/\s+/).filter(w => w.length > 0);
  if (words.length === 0) return null;

  const candidates = [];

  // Single word: word.com
  if (words.length === 1) {
    candidates.push(`${words[0]}.com`);
    candidates.push(`${words[0]}.io`);
  }
  // Two words: combine or hyphenate
  else if (words.length === 2) {
    candidates.push(`${words.join('')}.com`);
    candidates.push(`${words.join('-')}.com`);
    candidates.push(`${words.join('')}.io`);
  }
  // Three+ words: combine first 2-3
  else {
    candidates.push(`${words.join('')}.com`);
    candidates.push(`${words.slice(0, 2).join('')}.com`);
    candidates.push(`${words.join('-')}.com`);
  }

  return candidates;
}

async function verifyDomain(domain) {
  try {
    const res = await httpHead(`https://${domain}`);
    if (res.status >= 200 && res.status < 400) return `https://${domain}`;
    if (res.redirect) return res.redirect;
    // Try www prefix
    const res2 = await httpHead(`https://www.${domain}`);
    if (res2.status >= 200 && res2.status < 400) return `https://www.${domain}`;
    return null;
  } catch (_) {
    return null;
  }
}

// ─── Source 1: BuiltIn.com ─────────────────────────────────────────────────────

function parseBuiltInListPage(html) {
  const $ = cheerio.load(html);
  const companies = [];

  // Primary: JSON-LD structured data (most reliable)
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const json = JSON.parse($(el).html());
      const graph = json['@graph'] || [json];
      for (const item of graph) {
        if (item['@type'] === 'ItemList' && item.itemListElement) {
          for (const entry of item.itemListElement) {
            if (entry.name && entry.url) {
              const slug = (entry.url || '').match(/\/company\/([a-z0-9-]+)/i)?.[1] || '';
              companies.push({
                company_name: entry.name.trim(),
                description: (entry.description || '').replace(/\r\n/g, ' ').substring(0, 500),
                slug,
                profile_url: entry.url,
                source: 'builtin',
              });
            }
          }
        }
      }
    } catch (_) {}
  });

  // If JSON-LD didn't work, fall back to data attributes
  if (companies.length === 0) {
    const seenIds = new Set();
    $('[data-track-id="profile"][data-company-id]').each((_, el) => {
      const $el = $(el);
      const companyId = $el.attr('data-company-id');
      if (seenIds.has(companyId)) return;
      seenIds.add(companyId);

      const alias = $el.attr('data-company-alias') || '';
      const slug = alias.replace('/company/', '');
      const name = $el.find('[data-track-id="title"]').first().text().trim() ||
                   slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

      if (name) {
        companies.push({
          company_name: name,
          slug,
          profile_url: `https://builtin.com/company/${slug}`,
          source: 'builtin',
        });
      }
    });
  }

  // Extract website URLs from descriptions (e.g., "Learn more at www.skillsoft.com")
  const excludeDomains = ['calendly.com', 'bit.ly', 'linktr.ee', 'youtube.com', 'youtu.be',
    'twitter.com', 'x.com', 'facebook.com', 'instagram.com', 'linkedin.com', 'tiktok.com',
    'github.com', 'medium.com', 'notion.so', 'docs.google.com', 'forms.gle'];
  for (const company of companies) {
    if (company.description) {
      const urlMatch = company.description.match(/(?:www\.|https?:\/\/)([a-z0-9][-a-z0-9]*\.)+[a-z]{2,}/i);
      if (urlMatch) {
        let url = urlMatch[0];
        if (!url.startsWith('http')) url = `https://${url}`;
        const host = url.replace(/https?:\/\//, '').replace(/^www\./, '').split('/')[0].toLowerCase();
        if (!excludeDomains.some(d => host.includes(d))) {
          company.website = url;
          try {
            company.domain = new URL(url).hostname.replace(/^www\./, '');
          } catch (_) {}
        }
      }
    }
  }

  // Check for next page
  const hasMore = html.includes('rel="next"') ||
                  $('a[rel="next"]').length > 0 ||
                  $('[class*="pager"] a, [class*="pagination"] a').length > 0;

  return { companies, hasMore };
}

async function scrapeBuiltIn() {
  console.log('\n═══ Source: BuiltIn.com EdTech Directory ═══');
  console.log('  Total available: ~3,133 companies, 157 pages\n');

  const allCompanies = [];
  let page = 1;
  const maxPages = MAX_PAGES || 157;
  let consecutiveEmpty = 0;

  while (page <= maxPages) {
    const url = `https://builtin.com/companies/type/edtech-companies?page=${page}`;
    process.stdout.write(`  [BuiltIn] Page ${page}... `);

    try {
      const res = await httpGet(url);
      if (res.status !== 200) {
        console.log(`HTTP ${res.status}`);
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3) break;
        page++;
        await sleep(2000);
        continue;
      }

      const { companies, hasMore } = parseBuiltInListPage(res.body);
      consecutiveEmpty = companies.length === 0 ? consecutiveEmpty + 1 : 0;

      if (companies.length > 0) {
        allCompanies.push(...companies);
        const withSite = companies.filter(c => c.website).length;
        console.log(`${companies.length} companies (${withSite} with website, total: ${allCompanies.length})`);
      } else {
        console.log('0 companies');
      }

      if (consecutiveEmpty >= 3) {
        console.log('  [BuiltIn] 3 consecutive empty pages — stopping.');
        break;
      }

      if (!hasMore && page > 5) break;  // Trust hasMore after first few pages
      page++;
      await sleep(1500 + Math.random() * 1000);
    } catch (err) {
      console.log(`Error: ${err.message}`);
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) break;
      page++;
      await sleep(3000);
    }
  }

  console.log(`\n  [BuiltIn] Scraped ${allCompanies.length} companies from ${page - 1} pages`);

  // Domain guessing for companies without websites
  const needDomain = allCompanies.filter(c => !c.website);
  const confirmed = allCompanies.filter(c => c.website);
  console.log(`  [BuiltIn] ${confirmed.length} confirmed websites, ${needDomain.length} need domain guessing`);

  if (needDomain.length > 0 && VERIFY_DOMAINS) {
    console.log(`  [BuiltIn] Verifying guessed domains for ${needDomain.length} companies (concurrency: ${CONCURRENCY})...`);
    let verified = 0;

    for (let i = 0; i < needDomain.length; i += CONCURRENCY) {
      const batch = needDomain.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(async (company) => {
        const candidates = guessDomain(company.company_name);
        if (!candidates) return;
        for (const domain of candidates) {
          const url = await verifyDomain(domain);
          if (url) {
            company.website = url;
            company.website_confirmed = true;
            try { company.domain = new URL(url).hostname.replace(/^www\./, ''); } catch (_) {}
            verified++;
            break;
          }
        }
      }));

      if ((i + CONCURRENCY) % 50 === 0 || i + CONCURRENCY >= needDomain.length) {
        process.stdout.write(`  [BuiltIn] Verified ${Math.min(i + CONCURRENCY, needDomain.length)}/${needDomain.length} (${verified} found)\r`);
      }
      await sleep(200);
    }
    console.log(`\n  [BuiltIn] Domain verification: ${verified} websites found`);
  } else if (needDomain.length > 0) {
    // Assign guessed domains (marked as unconfirmed)
    for (const company of needDomain) {
      const candidates = guessDomain(company.company_name);
      if (candidates && candidates[0]) {
        company.domain = candidates[0];
        company.website = `https://${candidates[0]}`;
        company.website_guessed = true;
      }
    }
    console.log(`  [BuiltIn] Guessed domains for ${needDomain.length} companies (use --verify-domains to HTTP-check)`);
  }

  // Mark confirmed websites
  for (const c of confirmed) c.website_confirmed = true;

  return allCompanies;
}

// ─── Source 2: Y Combinator Algolia API ────────────────────────────────────────

async function scrapeYCombinator() {
  console.log('\n═══ Source: Y Combinator Education Startups (Algolia API) ═══');

  const ALGOLIA_APP_ID = '45BWZJ1SGC';
  const ALGOLIA_API_KEY = 'NzllNTY5MzJiZGM2OTY2ZTQwMDEzOTNhYWZiZGRjODlhYzVkNjBmOGRjNzJiMWM4ZTU0ZDlhYTZjOTJiMjlhMWFuYWx5dGljc1RhZ3M9eWNkYyZyZXN0cmljdEluZGljZXM9WUNDb21wYW55X3Byb2R1Y3Rpb24lMkNZQ0NvbXBhbnlfQnlfTGF1bmNoX0RhdGVfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVE';
  const INDEX = 'YCCompany_production';

  const allCompanies = [];
  let page = 0;
  const hitsPerPage = 100;

  // Search for Education industry + targeted edtech keyword searches
  const searches = [
    { facet: '["industry:Education"]', query: '' },         // Primary: all education-industry companies
    { facet: '', query: 'edtech education technology' },    // Broad edtech catch
    { facet: '', query: 'online learning platform' },       // LMS/course platforms
    { facet: '', query: 'tutoring education students' },    // Tutoring
    { facet: '', query: 'course creator training' },        // Course creation tools
  ];

  for (const search of searches) {
    page = 0;
    let totalPages = 1;
    const label = search.facet ? `industry:Education` : `"${search.query}"`;

    while (page < totalPages) {
      process.stdout.write(`  [YC] ${label} page ${page + 1}... `);

      try {
        let params = `query=${encodeURIComponent(search.query)}&page=${page}&hitsPerPage=${hitsPerPage}`;
        if (search.facet) params += `&facetFilters=[${search.facet}]`;

        const searchBody = { requests: [{ indexName: INDEX, params }] };

        const res = await httpPost(
          `https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries`,
          searchBody,
          {
            'x-algolia-application-id': ALGOLIA_APP_ID,
            'x-algolia-api-key': ALGOLIA_API_KEY,
          }
        );

        if (res.status !== 200) {
          console.log(`HTTP ${res.status}`);
          break;
        }

        const data = JSON.parse(res.body);
        const result = data.results?.[0];
        if (!result?.hits?.length) {
          console.log('0 hits');
          break;
        }

        totalPages = Math.min(result.nbPages || 1, MAX_PAGES || 20);

        let newCount = 0;
        for (const hit of result.hits) {
          const company = {
            company_name: hit.name || '',
            website: hit.website || '',
            description: (hit.one_liner || hit.long_description || '').replace(/\r?\n/g, ' ').substring(0, 500),
            headquarters: [hit.all_locations || ''].flat().join('; '),
            employee_count: hit.team_size ? String(hit.team_size) : '',
            year_founded: '',
            industry_tags: (hit.tags || []).join('; '),
            source: 'yc',
            profile_url: hit.slug ? `https://www.ycombinator.com/companies/${hit.slug}` : '',
            batch: hit.batch || '',
            status: hit.status || '',
          };

          // Parse year from batch (e.g., "W21" → 2021, "S19" → 2019)
          if (hit.batch) {
            const batchMatch = hit.batch.match(/[WSF](\d{2})/);
            if (batchMatch) {
              const yr = parseInt(batchMatch[1]);
              company.year_founded = String(yr >= 50 ? 1900 + yr : 2000 + yr);
            }
          }

          if (company.website) {
            if (!company.website.startsWith('http')) company.website = `https://${company.website}`;
            try { company.domain = new URL(company.website).hostname.replace(/^www\./, ''); } catch (_) {}
          }

          allCompanies.push(company);
          newCount++;
        }

        console.log(`${result.hits.length} hits (${newCount} new, total: ${allCompanies.length}/${result.nbHits})`);
        page++;
        await sleep(300);
      } catch (err) {
        console.log(`Error: ${err.message}`);
        break;
      }
    }
  }

  console.log(`\n  [YC] Scraped ${allCompanies.length} education/edtech startups`);
  return allCompanies;
}

// ─── Email Extraction (optional) ───────────────────────────────────────────────

async function extractEmailFromWebsite(company) {
  if (!company.website) return company;
  try {
    const res = await httpGet(company.website, { timeout: 10000 });
    if (res.status !== 200 || !res.body) return company;

    // Extract emails
    const emailPattern = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    const emails = (res.body.match(emailPattern) || [])
      .filter(e => !e.match(/\.(png|jpg|gif|svg|css|js|webp|jpeg|ico|woff|ttf|eot|map)$/i) &&
                   !e.match(/@[0-9]x\./i) &&   // @2x.webp, @3x.png etc
                   !e.includes('example.com') && !e.includes('sentry.io') &&
                   !e.includes('wixpress') && !e.includes('cloudflare') &&
                   !e.includes('webpack') && !e.includes('localhost') &&
                   !e.includes('@import') && !e.includes('email@') &&
                   !e.match(/^[a-f0-9]{8,}@/i));

    if (emails.length > 0) {
      const priority = ['info@', 'contact@', 'hello@', 'sales@', 'support@', 'team@', 'hi@'];
      const best = emails.find(e => priority.some(p => e.toLowerCase().startsWith(p))) || emails[0];
      company.email = best.toLowerCase();
    }

    // Extract phone
    const phonePattern = /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
    const phones = (res.body.match(phonePattern) || [])
      .filter(p => !p.match(/^\d{10}$/) || true);  // Keep all
    if (phones.length > 0) {
      company.phone = phones[0].replace(/\s+/g, ' ').trim();
    }
  } catch (_) {}
  return company;
}

// ─── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`
╔════════════════════════════════════════════════════════════╗
║          EdTech Company Scraper — Multi-Source              ║
║     BuiltIn.com (3,133) + Y Combinator (125+ via API)      ║
╚════════════════════════════════════════════════════════════╝`);

  const startTime = Date.now();
  let allLeads = [];

  // Scrape sources
  if (SOURCE === 'all' || SOURCE === 'builtin') {
    const leads = await scrapeBuiltIn();
    allLeads.push(...leads);
  }

  if (SOURCE === 'all' || SOURCE === 'yc') {
    const leads = await scrapeYCombinator();
    allLeads.push(...leads);
  }

  // Dedup
  const before = allLeads.length;
  allLeads = allLeads.filter(c => !isDupe(c));
  console.log(`\n═══ Deduplication ═══`);
  console.log(`  Before: ${before} → After: ${allLeads.length} (removed ${before - allLeads.length} dupes)`);

  // Optional email enrichment (only crawl confirmed websites, not guessed domains)
  if (DO_ENRICH && allLeads.length > 0) {
    console.log(`\n═══ Email Enrichment ═══`);
    const withWebsite = allLeads.filter(c => c.website && !c.website_guessed);
    const guessedCount = allLeads.filter(c => c.website_guessed).length;
    console.log(`  ${withWebsite.length} confirmed websites to crawl (skipping ${guessedCount} guessed domains)...`);

    let emailCount = 0;
    for (let i = 0; i < withWebsite.length; i += CONCURRENCY) {
      const batch = withWebsite.slice(i, i + CONCURRENCY);
      await Promise.allSettled(batch.map(c => extractEmailFromWebsite(c)));
      emailCount = allLeads.filter(c => c.email).length;
      if ((i + CONCURRENCY) % 50 === 0 || i + CONCURRENCY >= withWebsite.length) {
        process.stdout.write(`  Crawled ${Math.min(i + CONCURRENCY, withWebsite.length)}/${withWebsite.length} (${emailCount} emails found)\r`);
      }
      await sleep(300);
    }
    console.log('');
  }

  // Stats
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const withWebsite = allLeads.filter(c => c.website).length;
  const withDomain = allLeads.filter(c => c.domain).length;
  const withEmail = allLeads.filter(c => c.email).length;
  const withPhone = allLeads.filter(c => c.phone).length;
  const withDesc = allLeads.filter(c => c.description && c.description.length > 10).length;
  const bySource = {};
  for (const lead of allLeads) {
    bySource[lead.source] = (bySource[lead.source] || 0) + 1;
  }

  console.log(`
╔════════════════════════════════════════════════════════════╗
║                     SCRAPE COMPLETE                        ║
╠════════════════════════════════════════════════════════════╣
║  Total companies:  ${String(allLeads.length).padEnd(38)}║
║  With website:     ${String(withWebsite).padEnd(38)}║
║  With domain:      ${String(withDomain).padEnd(38)}║
║  With description: ${String(withDesc).padEnd(38)}║
║  With email:       ${String(withEmail).padEnd(38)}║
║  With phone:       ${String(withPhone).padEnd(38)}║
║  Time elapsed:     ${String(elapsed + 's').padEnd(38)}║
╠════════════════════════════════════════════════════════════╣`);
  for (const [src, count] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
    console.log(`║  ${src.padEnd(19)} ${String(count).padEnd(38)}║`);
  }
  console.log(`╚════════════════════════════════════════════════════════════╝`);

  // Write CSV
  writeCSV(OUTPUT, allLeads);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
