#!/usr/bin/env node
/**
 * Apollo Lead Scraper — Pull leads from Apollo.io's B2B database
 *
 * Two modes:
 *   1. API key mode: Free search (names, titles, companies, LinkedIn) but NO emails
 *      → Then runs fast HTTP email scraper on their websites
 *   2. Cookie mode: Gets emails directly from Apollo (like Apify "Leads Finder")
 *
 * Usage:
 *   # API key mode (free, no emails from Apollo — scrapes websites for emails)
 *   APOLLO_API_KEY=xxx node scripts/apollo-scrape.js --titles "attorney,partner" --locations "California, US"
 *
 *   # Cookie mode (gets emails directly — needs session cookie from browser)
 *   APOLLO_COOKIE="..." node scripts/apollo-scrape.js --titles "attorney,partner" --locations "California, US"
 *
 * Options:
 *   --titles        Job titles, comma-separated (e.g. "attorney,partner,lawyer")
 *   --locations     Locations, comma-separated (e.g. "California, US;New York, US")
 *   --seniorities   Seniority levels (e.g. "owner,partner,c_suite,director")
 *   --keywords      Free-text keyword search (e.g. "personal injury")
 *   --industries    Company industry filter (e.g. "law practice")
 *   --employees     Employee range (e.g. "1,10;11,50;51,200")
 *   --email-status  Email verification status (e.g. "verified" or "verified,guessed")
 *   --max-pages     Max pages to fetch (default: 500, each page = 100 results)
 *   --max-results   Max total results (default: 0 = no limit)
 *   --output        Custom output CSV path
 *   --enrich-emails Run fast HTTP email scraper on results (auto if API key mode)
 *   --skip-emails   Skip email scraping even in API key mode
 *   --test          Test mode: 2 pages max
 */

const path = require('path');
const fs = require('fs');
const ApolloClient = require('../lib/apollo-client');
const { writeCSV } = require('../lib/csv-handler');
const { extractDomain, normalizePhone, titleCase } = require('../lib/normalizer');

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

const TITLES = getArg('titles') ? getArg('titles').split(',').map(s => s.trim()) : [];
const LOCATIONS = getArg('locations') ? getArg('locations').split(';').map(s => s.trim()) : [];
const SENIORITIES = getArg('seniorities') ? getArg('seniorities').split(',').map(s => s.trim()) : [];
const KEYWORDS = getArg('keywords') || '';
const INDUSTRIES = getArg('industries') ? getArg('industries').split(',').map(s => s.trim()) : [];
const EMPLOYEES = getArg('employees') ? getArg('employees').split(';').map(s => s.trim()) : [];
const EMAIL_STATUS = getArg('email-status') ? getArg('email-status').split(',').map(s => s.trim()) : [];
const MAX_PAGES = parseInt(getArg('max-pages') || '500');
const MAX_RESULTS = parseInt(getArg('max-results') || '0');
const OUTPUT = getArg('output');
const ENRICH_EMAILS = hasFlag('enrich-emails');
const SKIP_EMAILS = hasFlag('skip-emails');
const TEST_MODE = hasFlag('test');

// Auth: prefer Chrome profile (from apollo-login.js), then email/password, then cookie, then API key
const CHROME_PROFILE = process.env.APOLLO_CHROME_PROFILE || '/tmp/apollo-session-profile';
const HAS_CHROME_PROFILE = require('fs').existsSync(CHROME_PROFILE);
const APOLLO_EMAIL = process.env.APOLLO_EMAIL || '';
const APOLLO_PASSWORD = process.env.APOLLO_PASSWORD || '';
const COOKIE = process.env.APOLLO_COOKIE || '';
const API_KEY = process.env.APOLLO_API_KEY || '';
const CSRF_TOKEN = process.env.APOLLO_CSRF_TOKEN || '';

if (!HAS_CHROME_PROFILE && !APOLLO_EMAIL && !COOKIE && !API_KEY) {
  console.log('');
  console.log('Apollo Lead Scraper — Pull leads from Apollo.io B2B database');
  console.log('');
  console.log('Set one of these environment variables:');
  console.log('  APOLLO_COOKIE="..."  — Session cookie from browser (gets emails)');
  console.log('  APOLLO_API_KEY=xxx   — API key from Apollo settings (no emails in search)');
  console.log('');
  console.log('To get a session cookie:');
  console.log('  1. Log into app.apollo.io in Chrome');
  console.log('  2. Open DevTools (F12) → Application → Cookies → app.apollo.io');
  console.log('  3. Copy ALL cookies as a single string (name=value; name2=value2; ...)');
  console.log('  4. Or: DevTools → Network → any request → copy "Cookie" header value');
  console.log('');
  console.log('Usage:');
  console.log('  APOLLO_COOKIE="..." node scripts/apollo-scrape.js \\');
  console.log('    --titles "attorney,partner,lawyer" \\');
  console.log('    --locations "California, US;New York, US" \\');
  console.log('    --seniorities "owner,partner,c_suite" \\');
  console.log('    --keywords "personal injury"');
  console.log('');
  console.log('Options:');
  console.log('  --titles        Job titles, comma-separated');
  console.log('  --locations     Locations, semicolon-separated');
  console.log('  --seniorities   Seniority levels, comma-separated');
  console.log('  --keywords      Free-text keyword search');
  console.log('  --industries    Industry filter, comma-separated');
  console.log('  --employees     Employee ranges, semicolon-separated (e.g. "1,10;11,50")');
  console.log('  --email-status  Filter by email status (verified, guessed, etc.)');
  console.log('  --max-pages     Max pages to fetch (default: 500)');
  console.log('  --max-results   Max results (default: unlimited)');
  console.log('  --output        Custom CSV output path');
  console.log('  --enrich-emails Also run HTTP email scraper on websites');
  console.log('  --skip-emails   Skip email scraping in API key mode');
  console.log('  --test          Test mode (2 pages max)');
  process.exit(1);
}

if (TITLES.length === 0 && !KEYWORDS && LOCATIONS.length === 0) {
  console.error('Error: Provide at least --titles, --keywords, or --locations to search.');
  process.exit(1);
}

// ─── Build Filters ─────────────────────────────────────────────────

function buildFilters() {
  const filters = {};

  if (TITLES.length > 0) filters.person_titles = TITLES;
  if (LOCATIONS.length > 0) filters.person_locations = LOCATIONS;
  if (SENIORITIES.length > 0) filters.person_seniorities = SENIORITIES;
  if (KEYWORDS) filters.q_keywords = KEYWORDS;
  if (INDUSTRIES.length > 0) filters.organization_industry_tag_ids = INDUSTRIES;
  if (EMPLOYEES.length > 0) filters.organization_num_employees_ranges = EMPLOYEES;
  if (EMAIL_STATUS.length > 0) filters.contact_email_status = EMAIL_STATUS;

  return filters;
}

// ─── Email Enrichment ──────────────────────────────────────────────

async function enrichWithEmails(leads) {
  const { scrapeEmailsBatch } = require('../lib/fast-email-scraper');

  // Get leads that have a website but no email
  const needsEmail = leads.filter(l => l.domain && !l.email);
  if (needsEmail.length === 0) {
    console.log('  All leads already have emails — skipping HTTP scrape');
    return 0;
  }

  console.log(`  Scraping ${needsEmail.length} domains for emails (10 parallel)...`);
  const emailMap = await scrapeEmailsBatch(needsEmail, 10, 3);

  let found = 0;
  for (const lead of leads) {
    if (lead.email || !lead.domain) continue;
    const emails = emailMap.get(lead.domain);
    if (emails && emails.length > 0) {
      lead.email = emails[0];
      lead.email_source = 'website_http';
      found++;
    }
  }

  console.log(`  Found emails for ${found}/${needsEmail.length} domains`);
  return found;
}

// ─── SMTP Email Verification (generate patterns for leads with name + domain but no email) ──

async function generateEmailPatterns(leads) {
  const EmailVerifier = require('../lib/email-verifier');
  const verifier = new EmailVerifier({ timeout: 8000, concurrency: 5 });

  const needsEmail = leads.filter(l => l.first_name && l.last_name && l.domain && !l.email);
  if (needsEmail.length === 0) return 0;

  console.log(`  Generating + verifying email patterns for ${needsEmail.length} leads...`);

  let found = 0;
  let idx = 0;
  const CONCURRENCY = 5;

  const workers = [];
  for (let w = 0; w < Math.min(CONCURRENCY, needsEmail.length); w++) {
    workers.push((async () => {
      while (idx < needsEmail.length) {
        const lead = needsEmail[idx++];
        try {
          const email = await verifier.findBestEmail(lead.first_name, lead.last_name, lead.domain);
          if (email) {
            lead.email = email;
            lead.email_source = 'smtp_pattern';
            found++;
          }
        } catch {
          // Skip verification errors
        }

        if (idx % 50 === 0) {
          console.log(`  Pattern verify: ${idx}/${needsEmail.length} checked, ${found} found`);
        }
      }
    })());
  }

  await Promise.all(workers);
  console.log(`  SMTP pattern verification: ${found}/${needsEmail.length} emails found`);
  return found;
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();
  const mode = (HAS_CHROME_PROFILE || APOLLO_EMAIL || COOKIE) ? 'browser' : 'api';

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║         MORTAR — Apollo Lead Scraper                      ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Mode:         ${mode === 'browser' ? 'Browser (Chrome profile)' : 'API Key (limited data)'}`);
  if (TITLES.length > 0) console.log(`  Titles:       ${TITLES.join(', ')}`);
  if (LOCATIONS.length > 0) console.log(`  Locations:    ${LOCATIONS.join('; ')}`);
  if (SENIORITIES.length > 0) console.log(`  Seniorities:  ${SENIORITIES.join(', ')}`);
  if (KEYWORDS) console.log(`  Keywords:     ${KEYWORDS}`);
  if (INDUSTRIES.length > 0) console.log(`  Industries:   ${INDUSTRIES.join(', ')}`);
  if (EMPLOYEES.length > 0) console.log(`  Employees:    ${EMPLOYEES.join('; ')}`);
  if (EMAIL_STATUS.length > 0) console.log(`  Email status: ${EMAIL_STATUS.join(', ')}`);
  console.log(`  Max pages:    ${TEST_MODE ? 2 : MAX_PAGES}`);
  console.log(`  Test mode:    ${TEST_MODE ? 'YES' : 'NO'}`);
  console.log('');

  // Initialize client
  let apollo;
  if (HAS_CHROME_PROFILE) {
    apollo = new ApolloClient({ chromeProfile: CHROME_PROFILE });
  } else if (APOLLO_EMAIL && APOLLO_PASSWORD) {
    apollo = new ApolloClient({ email: APOLLO_EMAIL, password: APOLLO_PASSWORD });
  } else if (COOKIE) {
    apollo = new ApolloClient({ cookie: COOKIE, csrfToken: CSRF_TOKEN });
  } else {
    apollo = new ApolloClient({ apiKey: API_KEY });
  }

  // Browser mode: launch Puppeteer
  if (apollo.mode === 'browser') {
    await apollo.init();
  }

  const filters = buildFilters();
  const maxPages = TEST_MODE ? 2 : MAX_PAGES;
  const maxResults = TEST_MODE ? 200 : MAX_RESULTS;

  // ─── Fetch leads ─────────────────────────────────────────────

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  STEP 1: Apollo People Search');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  const leads = [];
  const seen = new Set(); // Dedup by apollo_id

  const searchStart = Date.now();

  for await (const person of apollo.searchAll(filters, {
    maxPages,
    maxResults,
    onPage: (page, totalPages, count, totalEntries) => {
      const pct = totalPages > 0 ? Math.round((page / totalPages) * 100) : 0;
      const elapsed = ((Date.now() - searchStart) / 1000).toFixed(0);
      const rate = elapsed > 0 ? Math.round(leads.length / (elapsed / 60)) : 0;
      console.log(`  [Page ${page}/${totalPages || '?'}] ${pct}% | ${leads.length}/${totalEntries || '?'} leads | ${rate}/min | +${count} | ${elapsed}s`);
    },
  })) {
    // Dedup
    if (person.apollo_id && seen.has(person.apollo_id)) continue;
    if (person.apollo_id) seen.add(person.apollo_id);

    // Extract domain from website if not present
    if (!person.domain && person.website) {
      person.domain = extractDomain(person.website);
    }

    leads.push(person);
  }

  // Close browser after search is done (free up memory for email enrichment)
  await apollo.close();

  console.log('');
  console.log(`  Apollo search complete: ${leads.length} leads`);
  console.log(`  API stats: ${apollo.stats.requests} requests, ${apollo.stats.pages} pages, ${apollo.stats.errors} errors, ${apollo.stats.rateLimits} rate limits`);

  if (leads.length === 0) {
    console.error('No leads found. Try broader filters.');
    process.exit(1);
  }

  // ─── Email enrichment ────────────────────────────────────────

  const emailsBefore = leads.filter(l => l.email).length;
  const needsEmailScrape = (mode === 'api' && !SKIP_EMAILS) || ENRICH_EMAILS;

  if (needsEmailScrape) {
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  STEP 2: Email Enrichment');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');

    // First: scrape websites for emails
    const websiteFound = await enrichWithEmails(leads);

    // Second: generate + verify email patterns via SMTP for remaining leads
    const patternFound = await generateEmailPatterns(leads);

    const emailsAfter = leads.filter(l => l.email).length;
    console.log('');
    console.log(`  Email enrichment: ${emailsBefore} → ${emailsAfter} (+${emailsAfter - emailsBefore})`);
    console.log(`    Website scrape: +${websiteFound}`);
    console.log(`    SMTP patterns:  +${patternFound}`);
  }

  // ─── Export CSV ──────────────────────────────────────────────

  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  STEP ${needsEmailScrape ? '3' : '2'}: Export CSV`);
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  // Sort: leads with email first, then by seniority
  const seniorityOrder = { owner: 0, founder: 1, c_suite: 2, partner: 3, vp: 4, director: 5, manager: 6, senior: 7, entry: 8 };
  leads.sort((a, b) => {
    // Email first
    const aHasEmail = a.email ? 0 : 1;
    const bHasEmail = b.email ? 0 : 1;
    if (aHasEmail !== bHasEmail) return aHasEmail - bHasEmail;
    // Then by seniority
    const aSen = seniorityOrder[a.seniority] ?? 9;
    const bSen = seniorityOrder[b.seniority] ?? 9;
    return aSen - bSen;
  });

  // Map to CSV format
  const csvLeads = leads.map(lead => ({
    first_name: lead.first_name || '',
    last_name: lead.last_name || '',
    firm_name: lead.firm_name || '',
    practice_area: lead.industry || '',
    city: lead.city || '',
    state: lead.state || '',
    country: lead.country || '',
    phone: lead.phone ? normalizePhone(lead.phone) : '',
    website: lead.website || '',
    email: lead.email || '',
    bar_number: '',
    admission_date: '',
    bar_status: '',
    source: lead.source || 'apollo',
    profile_url: lead.linkedin_url || '',
    title: lead.title || '',
    linkedin_url: lead.linkedin_url || '',
    bio: lead.headline || '',
    education: '',
    languages: '',
    practice_specialties: lead.keywords || '',
    email_source: lead.email_source || '',
    phone_source: lead.phone_source || '',
    website_source: '',
  }));

  // Generate output path
  const titleSlug = (TITLES[0] || KEYWORDS || 'search').toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 30);
  const locationSlug = (LOCATIONS[0] || 'all').toLowerCase().replace(/[\s,]+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 30);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outputPath = OUTPUT || path.join(__dirname, '..', 'output', `apollo_${titleSlug}_${locationSlug}_${timestamp}.csv`);

  // Ensure output directory exists
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  await writeCSV(outputPath, csvLeads);

  // ─── Summary ─────────────────────────────────────────────────

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const emailCount = leads.filter(l => l.email).length;
  const verifiedCount = leads.filter(l => l.email_status === 'verified').length;
  const phoneCount = leads.filter(l => l.phone).length;
  const linkedinCount = leads.filter(l => l.linkedin_url).length;
  const withName = leads.filter(l => l.first_name && l.last_name).length;
  const seniorLeads = leads.filter(l => ['owner', 'founder', 'c_suite', 'partner'].includes(l.seniority)).length;

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║                  APOLLO SCRAPE COMPLETE                   ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:        ${String(leads.length).padStart(6)}                              ║`);
  console.log(`║  With email:         ${String(emailCount).padStart(6)} (${leads.length > 0 ? Math.round(emailCount/leads.length*100) : 0}%)                          ║`);
  console.log(`║  Verified emails:    ${String(verifiedCount).padStart(6)}                              ║`);
  console.log(`║  With phone:         ${String(phoneCount).padStart(6)}                              ║`);
  console.log(`║  With LinkedIn:      ${String(linkedinCount).padStart(6)}                              ║`);
  console.log(`║  With name:          ${String(withName).padStart(6)}                              ║`);
  console.log(`║  Senior (owner/C/VP):${String(seniorLeads).padStart(6)}                              ║`);
  console.log(`║  Time elapsed:       ${elapsed}s                              ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
}

main().catch(async err => {
  console.error('Fatal error:', err);
  // Attempt cleanup
  try {
    const puppeteer = require('puppeteer');
    // kill any dangling browsers
  } catch {}
  process.exit(1);
});
