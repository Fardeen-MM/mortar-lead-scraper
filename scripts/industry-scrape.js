#!/usr/bin/env node
/**
 * Universal Industry Scraper — give it an industry, get leads
 *
 * The single entry point for niche-agnostic business lead generation.
 * Chains together: Google Maps → DuckDuckGo → Website Crawl → Common Crawl → WHOIS → Enrichment → CSV
 *
 * Usage:
 *   node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL"
 *   node scripts/industry-scrape.js --niche "plumbers" --location "London, UK"
 *   node scripts/industry-scrape.js --niche "accountants" --location "Sydney, AU" --radius 50
 *   node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL" --test
 *   node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL" --skip-maps --skip-ddg
 *
 * Options:
 *   --niche         Business type (required)
 *   --location      City + state/country (required)
 *   --radius        Search radius in km (default 25)
 *   --concurrency   Parallel website crawls (default 3)
 *   --output        Custom CSV output path
 *   --test          Test mode — limits all steps for quick testing
 *   --skip-maps     Skip Google Maps step
 *   --skip-ddg      Skip DuckDuckGo step
 *   --skip-cc       Skip Common Crawl step
 *   --skip-whois    Skip WHOIS step
 *   --skip-crawl    Skip website crawl step
 *   --skip-enrich   Skip enrichment step
 */

const path = require('path');
const https = require('https');
const fs = require('fs');
const { log } = require('../lib/logger');
const { writeCSV } = require('../lib/csv-handler');
const { extractDomain, normalizePhone, titleCase } = require('../lib/normalizer');
const ddg = require('../lib/duckduckgo-scraper');
const cc = require('../lib/commoncrawl-client');
const whois = require('../lib/whois-client');
const { mergeLeads, enrichAll, isLikelyPersonName } = require('../lib/industry-enricher');

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1) return undefined;
  return args[idx + 1];
}

function hasFlag(name) {
  return args.includes(`--${name}`);
}

const NICHE = getArg('niche');
const LOCATION = getArg('location');
const RADIUS = parseInt(getArg('radius') || '25');
const CONCURRENCY = parseInt(getArg('concurrency') || '3');
const OUTPUT = getArg('output');
const TEST_MODE = hasFlag('test');
const SKIP_MAPS = hasFlag('skip-maps');
const SKIP_DDG = hasFlag('skip-ddg');
const SKIP_CC = hasFlag('skip-cc');
const SKIP_WHOIS = hasFlag('skip-whois');
const SKIP_CRAWL = hasFlag('skip-crawl');
const SKIP_ENRICH = hasFlag('skip-enrich');

if (!NICHE || !LOCATION) {
  console.log('Usage: node scripts/industry-scrape.js --niche "dentists" --location "Miami, FL"');
  console.log('');
  console.log('Options:');
  console.log('  --niche         Business type (required)');
  console.log('  --location      City + state/country (required)');
  console.log('  --radius        Search radius in km (default 25)');
  console.log('  --concurrency   Parallel website crawls (default 3)');
  console.log('  --output        Custom CSV output path');
  console.log('  --test          Test mode (limits all steps)');
  console.log('  --skip-maps     Skip Google Maps');
  console.log('  --skip-ddg      Skip DuckDuckGo');
  console.log('  --skip-cc       Skip Common Crawl');
  console.log('  --skip-whois    Skip WHOIS');
  console.log('  --skip-crawl    Skip website crawl');
  console.log('  --skip-enrich   Skip enrichment');
  process.exit(1);
}

// ─── Geocode ────────────────────────────────────────────────────────

/**
 * Geocode a location string to lat/lng using Nominatim (free, no key).
 * Retries up to 3 times with 2s delay (Nominatim rate limits to 1 req/sec).
 */
async function geocode(location, retries = 3) {
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(location)}&format=json&limit=1`;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const result = await new Promise((resolve, reject) => {
        const req = https.get(url, {
          headers: { 'User-Agent': 'MortarLeadScraper/1.0 (contact@mortarmetrics.com)' },
          timeout: 10000,
        }, (res) => {
          let body = '';
          res.on('data', c => body += c);
          res.on('end', () => {
            if (res.statusCode !== 200) {
              return reject(new Error(`HTTP ${res.statusCode} from Nominatim`));
            }
            try {
              const data = JSON.parse(body);
              if (data && data[0]) {
                resolve({
                  lat: parseFloat(data[0].lat),
                  lng: parseFloat(data[0].lon),
                  displayName: data[0].display_name,
                  boundingbox: data[0].boundingbox ? data[0].boundingbox.map(Number) : null,
                });
              } else {
                reject(new Error(`No results found for location: ${location}`));
              }
            } catch (err) {
              reject(new Error(`Failed to parse Nominatim response (attempt ${attempt}): ${err.message}`));
            }
          });
        });

        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Geocode timeout')); });
      });
      return result;
    } catch (err) {
      if (attempt < retries) {
        log.warn(`Geocode attempt ${attempt} failed: ${err.message} — retrying in 2s...`);
        await new Promise(r => setTimeout(r, 2000));
      } else {
        throw err;
      }
    }
  }
}

// ─── Step 1: Google Maps Discovery ──────────────────────────────────

async function runGoogleMaps(niche, location, geo) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 1A: Google Maps Discovery');
  log.info('═══════════════════════════════════════════════════════════');

  try {
    // Dynamic import — Google Maps scraper uses Puppeteer
    const { getRegistry } = require('../lib/registry');
    const SCRAPERS = getRegistry();

    if (!SCRAPERS['GOOGLE-MAPS']) {
      log.warn('[Step 1A] Google Maps scraper not found — skipping');
      return [];
    }

    const scraper = SCRAPERS['GOOGLE-MAPS']();
    const leads = [];

    const searchOpts = {
      niche,
      city: location.split(',')[0].trim(), // Just the city name
      maxPages: TEST_MODE ? 1 : null,
      maxCities: 1,
      // Pass geocoded lat/lng/radius to avoid redundant Nominatim call
      lat: geo.lat,
      lng: geo.lng,
      radius: RADIUS,
    };

    for await (const result of scraper.search('', searchOpts)) {
      if (result._cityProgress || result._captcha) continue;

      leads.push({
        first_name: result.first_name || '',
        last_name: result.last_name || '',
        firm_name: result.firm_name || '',
        city: result.city || '',
        state: result.state || '',
        phone: result.phone || '',
        website: result.website || '',
        email: result.email || '',
        domain: extractDomain(result.website || ''),
        source: result.source || 'google_maps',
        profile_url: result.profile_url || '',
        _rating: result._rating || '',
        _rating_count: result._rating_count || 0,
        _snippet: '',
      });

      if (TEST_MODE && leads.length >= 20) break;
    }

    // Close browser
    if (scraper._closeBrowser) {
      await scraper._closeBrowser();
    }

    log.info(`[Step 1A] Google Maps: ${leads.length} businesses found`);
    return leads;
  } catch (err) {
    log.error(`[Step 1A] Google Maps error: ${err.message}`);
    return [];
  }
}

// ─── Step 1B: DuckDuckGo Discovery ─────────────────────────────────

async function runDuckDuckGo(niche, location) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 1B: DuckDuckGo Web Search Discovery');
  log.info('═══════════════════════════════════════════════════════════');

  try {
    const results = await ddg.search(niche, location, {
      maxResults: TEST_MODE ? 20 : 100,
      onProgress: (i, total, query) => {
        log.info(`[Step 1B] Query ${i}/${total}: "${query}"`);
      },
    });

    // Convert DDG results to lead format
    const leads = results.map(r => ({
      first_name: '',
      last_name: '',
      firm_name: r.name || '',
      city: '',
      state: '',
      phone: r.phone || '',
      website: r.website || r.url || '',
      email: '',
      domain: r.domain || '',
      source: r.source || 'ddg',
      profile_url: '',
      _rating: '',
      _rating_count: 0,
      _snippet: r.snippet || '',
    }));

    log.info(`[Step 1B] DuckDuckGo: ${leads.length} businesses found`);
    return leads;
  } catch (err) {
    log.error(`[Step 1B] DuckDuckGo error: ${err.message}`);
    return [];
  }
}

// ─── Step 2: Website Crawl + Person Extraction ─────────────────────

async function runWebsiteCrawl(leads) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 2: Website Crawl + Person Extraction');
  log.info('═══════════════════════════════════════════════════════════');

  const withWebsite = leads.filter(l => l.website && l.domain);
  if (withWebsite.length === 0) {
    log.warn('[Step 2] No leads with websites — skipping');
    return { leads, people: [] };
  }

  const limit = TEST_MODE ? Math.min(10, withWebsite.length) : withWebsite.length;
  log.info(`[Step 2] Crawling ${limit} websites for people + emails...`);

  let PersonExtractor, EmailFinder;
  try {
    PersonExtractor = require('../lib/person-extractor');
    EmailFinder = require('../lib/email-finder');
  } catch (err) {
    log.error(`[Step 2] Failed to load modules: ${err.message}`);
    return { leads, people: [] };
  }

  const extractor = new PersonExtractor();
  const emailFinder = new EmailFinder();
  const people = [];

  try {
    await extractor.init();
    await emailFinder.init();

    const subset = withWebsite.slice(0, limit);

    for (let i = 0; i < subset.length; i++) {
      const lead = subset[i];
      const pctDone = Math.round((i / subset.length) * 100);
      log.info(`[Step 2] [${pctDone}%] Crawling: ${lead.domain} (${lead.firm_name})`);

      try {
        // Extract people
        const extracted = await extractor.extractPeople(lead.website);
        for (const person of extracted) {
          people.push({
            first_name: person.first_name || '',
            last_name: person.last_name || '',
            title: person.title || '',
            email: person.email || '',
            phone: person.phone || '',
            linkedin_url: person.linkedin_url || '',
            firm_name: lead.firm_name || '',
            city: lead.city || '',
            state: lead.state || '',
            website: lead.website || '',
            domain: lead.domain || '',
            source: 'website_crawl',
            _rating: lead._rating || '',
            _rating_count: lead._rating_count || 0,
            _snippet: lead._snippet || '',
          });
        }

        // Find email for the business if none found yet
        if (!lead.email) {
          const email = await emailFinder.findEmail(lead.website);
          if (email) {
            lead.email = email;
            lead.email_source = 'website';
          }
        }
      } catch (err) {
        log.warn(`[Step 2] Error crawling ${lead.domain}: ${err.message}`);
      }
    }
  } catch (err) {
    log.error(`[Step 2] Browser error: ${err.message}`);
  } finally {
    await extractor.close().catch(() => {});
    await emailFinder.close().catch(() => {});
  }

  log.info(`[Step 2] Extracted ${people.length} people from ${limit} websites`);
  const emailCount = leads.filter(l => l.email).length + people.filter(p => p.email).length;
  log.info(`[Step 2] Emails found so far: ${emailCount}`);

  return { leads, people };
}

// ─── Step 3A: Common Crawl ──────────────────────────────────────────

async function runCommonCrawl(leads) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 3A: Common Crawl Archive Search');
  log.info('═══════════════════════════════════════════════════════════');

  // Get unique domains from leads that don't have emails
  const domainsWithoutEmail = [...new Set(
    leads
      .filter(l => !l.email && l.domain)
      .map(l => l.domain)
  )];

  if (domainsWithoutEmail.length === 0) {
    log.info('[Step 3A] All leads have emails — skipping Common Crawl');
    return {};
  }

  const limit = TEST_MODE ? Math.min(5, domainsWithoutEmail.length) : domainsWithoutEmail.length;
  const domains = domainsWithoutEmail.slice(0, limit);
  log.info(`[Step 3A] Searching ${domains.length} domains in Common Crawl...`);

  const emailMap = await cc.batchFindEmails(domains, {
    onProgress: (i, total, domain) => {
      if (i % 10 === 0 || i === total) {
        log.info(`[Step 3A] Progress: ${i}/${total} (${domain})`);
      }
    },
  });

  // Apply found emails back to leads
  let applied = 0;
  for (const lead of leads) {
    if (!lead.email && lead.domain && emailMap.has(lead.domain)) {
      const emails = emailMap.get(lead.domain);
      if (emails.length > 0) {
        lead.email = emails[0];
        lead.email_source = 'commoncrawl';
        applied++;
      }
    }
  }

  log.info(`[Step 3A] Common Crawl: ${emailMap.size} domains had emails, ${applied} applied to leads`);
  return emailMap;
}

// ─── Step 3B: WHOIS Lookup ──────────────────────────────────────────

async function runWhois(leads) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 3B: WHOIS Domain Owner Lookup');
  log.info('═══════════════════════════════════════════════════════════');

  // Skip social media / major platform domains (WHOIS data is useless for these)
  const SKIP_DOMAINS = new Set([
    'facebook.com', 'fb.com', 'instagram.com', 'twitter.com', 'x.com',
    'linkedin.com', 'youtube.com', 'tiktok.com', 'pinterest.com',
    'yelp.com', 'google.com', 'apple.com', 'amazon.com', 'microsoft.com',
    'squarespace.com', 'wix.com', 'weebly.com', 'wordpress.com', 'godaddy.com',
    'shopify.com', 'toasttab.com', 'booksy.com', 'mindbodyonline.com',
    'vagaro.com', 'schedulicity.com', 'acuityscheduling.com',
  ]);

  // Get unique domains (excluding platform domains)
  const domains = [...new Set(leads.filter(l => l.domain && !SKIP_DOMAINS.has(l.domain)).map(l => l.domain))];

  if (domains.length === 0) {
    log.info('[Step 3B] No domains to look up — skipping');
    return {};
  }

  const limit = TEST_MODE ? Math.min(5, domains.length) : Math.min(100, domains.length);
  const subset = domains.slice(0, limit);
  log.info(`[Step 3B] Looking up WHOIS for ${subset.length} domains...`);

  const whoisMap = await whois.batchLookup(subset, {
    onProgress: (i, total, domain) => {
      if (i % 10 === 0 || i === total) {
        log.info(`[Step 3B] Progress: ${i}/${total} (${domain})`);
      }
    },
  });

  // Apply WHOIS data to leads
  let emailsApplied = 0;
  let namesApplied = 0;
  for (const lead of leads) {
    if (!lead.domain || !whoisMap.has(lead.domain)) continue;
    const info = whoisMap.get(lead.domain);

    // Apply registrant email if lead has none
    if (!lead.email && info.registrant_email) {
      lead.email = info.registrant_email;
      lead.email_source = 'whois';
      emailsApplied++;
    }

    // Apply registrant name if lead has no person name
    // Validate it looks like a real person name (not "Registration Private" etc.)
    if (!lead.first_name && info.registrant_name) {
      const parts = info.registrant_name.split(/\s+/);
      if (parts.length >= 2 && isLikelyPersonName(parts[0])) {
        lead.first_name = titleCase(parts[0]);
        lead.last_name = titleCase(parts.slice(1).join(' '));
        lead.title = lead.title || 'Owner';
        namesApplied++;
      }
    }

    // Apply organization as firm name
    if (!lead.firm_name && info.organization) {
      lead.firm_name = info.organization;
    }
  }

  log.info(`[Step 3B] WHOIS: ${whoisMap.size} domains had info, ${emailsApplied} emails + ${namesApplied} names applied`);
  return whoisMap;
}

// ─── Step 4: Enrich + Score + Export ────────────────────────────────

async function runEnrichAndExport(allLeads, niche) {
  log.info('');
  log.info('═══════════════════════════════════════════════════════════');
  log.info('  STEP 4: Enrich + Score + Export');
  log.info('═══════════════════════════════════════════════════════════');

  // Run enrichment
  const enrichStats = enrichAll(allLeads, niche, {
    onProgress: (i, total, name) => {
      if (i % 50 === 0 || i === total) {
        log.info(`[Step 4] Enriching: ${i}/${total}`);
      }
    },
  });

  log.info(`[Step 4] Enrichment: ${enrichStats.titlesInferred} titles, ${enrichStats.specialtiesDetected} specialties, ${enrichStats.linkedInBuilt} LinkedIn, ${enrichStats.emailPatternsGenerated} email patterns`);

  // Sort by DM score (decision makers first)
  allLeads.sort((a, b) => (b.dm_score || 0) - (a.dm_score || 0));

  // Sanitize emails: decode URL encoding, strip whitespace
  for (const lead of allLeads) {
    if (lead.email) {
      try { lead.email = decodeURIComponent(lead.email); } catch {}
      lead.email = lead.email.replace(/^\s+|\s+$/g, '').replace(/[\x00-\x1f]/g, '');
    }
  }

  // Format for CSV output
  const csvLeads = allLeads.map(lead => ({
    first_name: lead.first_name || '',
    last_name: lead.last_name || '',
    firm_name: lead.firm_name || '',
    practice_area: '',
    city: lead.city || '',
    state: lead.state || '',
    phone: lead.phone || '',
    website: lead.website || '',
    email: lead.email || '',
    bar_number: '',
    admission_date: '',
    bar_status: '',
    source: lead.source || '',
    profile_url: lead.profile_url || '',
    title: lead.title || '',
    linkedin_url: lead.linkedin_url || '',
    bio: '',
    education: '',
    languages: '',
    practice_specialties: lead.practice_specialties || '',
    email_source: lead.email_source || '',
    phone_source: lead.phone_source || '',
    website_source: lead.website_source || '',
  }));

  // Generate output path
  const nicheSlug = niche.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
  const locationSlug = LOCATION.toLowerCase().replace(/[\s,]+/g, '-').replace(/[^a-z0-9-]/g, '');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outputPath = OUTPUT || path.join(__dirname, '..', 'output', `${nicheSlug}_${locationSlug}_${timestamp}.csv`);

  // Ensure output directory exists
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  await writeCSV(outputPath, csvLeads);
  log.info(`[Step 4] CSV written: ${outputPath}`);

  return { outputPath, csvLeads, enrichStats };
}

// ─── Main Orchestrator ──────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║          MORTAR — Universal Industry Scraper              ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Niche:      ${NICHE}`);
  console.log(`  Location:   ${LOCATION}`);
  console.log(`  Radius:     ${RADIUS}km`);
  console.log(`  Test mode:  ${TEST_MODE ? 'YES (limited)' : 'NO (full)'}`);
  console.log('');

  // Geocode location
  log.info('Geocoding location...');
  let geo;
  try {
    geo = await geocode(LOCATION);
    log.info(`Location resolved: ${geo.displayName} (${geo.lat}, ${geo.lng})`);
    // Wait 1.5s before hitting Nominatim again (Google Maps scraper also uses it)
    await new Promise(r => setTimeout(r, 1500));
  } catch (err) {
    log.error(`Failed to geocode "${LOCATION}": ${err.message}`);
    process.exit(1);
  }

  // ─── Step 1: Business Discovery (parallel) ─────────────────────

  const discoveryPromises = [];

  if (!SKIP_MAPS) {
    discoveryPromises.push(runGoogleMaps(NICHE, LOCATION, geo));
  }

  if (!SKIP_DDG) {
    discoveryPromises.push(runDuckDuckGo(NICHE, LOCATION));
  }

  const discoveryResults = await Promise.all(discoveryPromises);

  // Merge all discovery results
  let allBusinesses;
  if (discoveryResults.length > 1) {
    allBusinesses = mergeLeads(...discoveryResults);
  } else if (discoveryResults.length === 1) {
    allBusinesses = discoveryResults[0];
  } else {
    allBusinesses = [];
  }

  log.info('');
  log.info(`═══ Discovery Complete: ${allBusinesses.length} unique businesses ═══`);

  if (allBusinesses.length === 0) {
    log.error('No businesses found from any source. Try a different niche or location.');
    log.info('Tips: Make sure Google Maps is enabled (don\'t use --skip-maps) for best results.');
    log.info('DuckDuckGo may rate-limit — Google Maps is the primary discovery source.');
    process.exit(1);
  }

  // ─── Step 2: Website Crawl + Person Extraction ──────────────────

  let people = [];
  if (!SKIP_CRAWL) {
    const crawlResult = await runWebsiteCrawl(allBusinesses);
    people = crawlResult.people;
  }

  // Combine businesses + extracted people
  // People are individual leads (not merged by domain — each person is unique)
  const allLeads = [...allBusinesses];
  const seenPeople = new Set();
  for (const person of people) {
    // Dedup people by name + domain
    const personKey = `${(person.first_name || '').toLowerCase()}|${(person.last_name || '').toLowerCase()}|${person.domain || ''}`;
    if (seenPeople.has(personKey)) continue;
    seenPeople.add(personKey);
    allLeads.push(person);
  }
  log.info(`Total leads after website crawl: ${allLeads.length} (${allBusinesses.length} businesses + ${allLeads.length - allBusinesses.length} people)`);

  // ─── Step 3: Deep Enrichment (parallel) ─────────────────────────

  const enrichPromises = [];

  if (!SKIP_CC) {
    enrichPromises.push(runCommonCrawl(allLeads));
  }

  if (!SKIP_WHOIS) {
    enrichPromises.push(runWhois(allLeads));
  }

  if (enrichPromises.length > 0) {
    await Promise.all(enrichPromises);
  }

  // ─── Step 4: Enrich + Score + Export ────────────────────────────

  let result;
  if (!SKIP_ENRICH) {
    result = await runEnrichAndExport(allLeads, NICHE);
  } else {
    // Just export without enrichment
    result = await runEnrichAndExport(allLeads, NICHE);
  }

  // ─── Summary ────────────────────────────────────────────────────

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const emailCount = allLeads.filter(l => l.email).length;
  const phoneCount = allLeads.filter(l => l.phone).length;
  const websiteCount = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name).length;
  const dmCount = allLeads.filter(l => (l.dm_score || 0) >= 30).length;

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║                    SCRAPE COMPLETE                        ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:        ${String(allLeads.length).padStart(6)}                              ║`);
  console.log(`║  With email:         ${String(emailCount).padStart(6)} (${Math.round(emailCount/allLeads.length*100)}%)                          ║`);
  console.log(`║  With phone:         ${String(phoneCount).padStart(6)} (${Math.round(phoneCount/allLeads.length*100)}%)                          ║`);
  console.log(`║  With website:       ${String(websiteCount).padStart(6)} (${Math.round(websiteCount/allLeads.length*100)}%)                          ║`);
  console.log(`║  With person name:   ${String(withName).padStart(6)} (${Math.round(withName/allLeads.length*100)}%)                          ║`);
  console.log(`║  Decision makers:    ${String(dmCount).padStart(6)} (DM score >= 30)               ║`);
  console.log(`║  Time elapsed:       ${elapsed}s                              ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${result.outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
