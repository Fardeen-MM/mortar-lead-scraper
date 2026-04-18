#!/usr/bin/env node
/**
 * Scholarship Advisors & Financial Aid Consultants Scraper
 *
 * Scrapes scholarship advisors and financial aid consultants from multiple directories:
 *
 *   1. HEAG (Higher Education Assistance Group) team page — ~68 financial aid consultants
 *   2. Bright Horizons College Coach — Finance experts + Admissions experts (~70+ people)
 *   3. Financial Aid Consulting Firms — Team/about pages of known firms
 *   4. NASFAA Certification Commission — Commission member bios
 *   5. University Financial Aid Office directories — Staff listed on public pages
 *   6. Independent FA consultants — Known solo practitioners with public profiles
 *
 * Skipped sources (inaccessible):
 *   - NASFAA member directory (login required)
 *   - NSPA member directory (members only)
 *   - NICCP advisor directory (broken/404)
 *   - Fastweb, Scholarships.com, ScholarshipOwl (no advisor directories)
 *   - CollegeAbacus (no public consultant directory)
 *
 * Usage:
 *   node scripts/scrape-scholarship-advisors.js --test    # Test mode (limited pages)
 *   node scripts/scrape-scholarship-advisors.js --all     # Full scrape all sources
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const cheerio = require('cheerio');

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 2000;
const DELAY_MAX = 3500;
const REQUEST_TIMEOUT = 30000;
const MAX_RETRIES = 3;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'scholarship-financial-aid-advisors.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch { return ''; }
}

function formatPhone(rawPhone) {
  if (!rawPhone) return '';
  const digits = rawPhone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return rawPhone.trim();
}

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/^(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?)\s+/i, '')
    .trim();

  const commaIdx = cleaned.indexOf(',');
  if (commaIdx > 0) {
    cleaned = cleaned.substring(0, commaIdx).trim();
  }

  cleaned = cleaned
    .replace(/\s+(CEP|PhD|EdD|MA|MEd|EdM|MS|MPA|JD|MBA|LCSW|LPC|LCPC|LMFT|MPP|Esq|FAAC|CPA|CFP|CCPS|ChFC|CLU|RICP|CDFA|AIF|CRPC|CIMA|Ph\.D\.|Ed\.D\.|M\.A\.|M\.Ed\.|Ed\.M\.|M\.S\.|M\.P\.A\.|J\.D\.|M\.B\.A\.|M\.P\.P\.?|FAAC®?)\.?$/gi, '')
    .trim();

  cleaned = cleaned.replace(/[,.\s]+$/, '').trim();

  const parts = cleaned.split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

// Common two-word phrases that match "Firstname Lastname" pattern but aren't people
const FALSE_NAME_WORDS = new Set([
  'financial', 'aid', 'scholarship', 'student', 'college', 'university', 'award',
  'letter', 'mission', 'statement', 'physical', 'address', 'mailing', 'review',
  'provide', 'payment', 'solutions', 'research', 'loan', 'repayment', 'counseling',
  'consulting', 'services', 'about', 'contact', 'office', 'staff', 'team',
  'welcome', 'home', 'directory', 'find', 'meet', 'our', 'acceptances', 'learn',
  'more', 'read', 'view', 'back', 'next', 'previous', 'page', 'search',
  'apply', 'submit', 'request', 'information', 'resources', 'events', 'news',
  'program', 'programs', 'academic', 'federal', 'state', 'private', 'work',
  'study', 'graduate', 'undergraduate', 'enrollment', 'admissions', 'tuition',
  'cost', 'budget', 'family', 'parent', 'parents', 'important', 'dates',
  'deadline', 'deadlines', 'policy', 'policies', 'form', 'forms', 'annual',
  'report', 'reports', 'consumer', 'disclosure', 'verification', 'satisfactory',
  'progress', 'types', 'eligibility', 'requirements', 'quick', 'links',
  'vision', 'values', 'goals', 'strategic', 'plan', 'overview', 'history',
  'benefits', 'membership', 'join', 'register', 'login', 'sign', 'subscribe',
  'donate', 'volunteer', 'sponsor', 'partner', 'become', 'member',
  'award', 'letters', 'appeals', 'negotiations', 'compare', 'offers',
  'free', 'consultation', 'schedule', 'book', 'call', 'start', 'begin',
  'complete', 'guide', 'step', 'steps', 'tips', 'advice', 'blog', 'post',
  'article', 'articles', 'posts', 'latest', 'recent', 'featured', 'popular',
  'board', 'executive', 'committee', 'members', 'archives', 'merchandise', 'shop',
  'collegiate', 'productions', 'annual', 'conference', 'training', 'workshop',
  'event', 'webinar', 'session', 'presentation', 'speaker', 'speakers',
  'registration', 'agenda', 'schedule', 'vendor', 'exhibit', 'exhibitor',
  'spring', 'fall', 'winter', 'summer', 'national', 'regional', 'local',
  'prospective', 'iron', 'bridge', 'council', 'representatives', 'representative',
  'institutional', 'associate', 'corporate', 'sector', 'advisory', 'past',
  'current', 'incoming', 'outgoing', 'elect', 'appointed', 'ex', 'officio',
]);

function isFalseName(text) {
  if (!text) return true;
  const words = text.toLowerCase().split(/\s+/);
  // If all words are in the false-name set, it's probably not a person
  if (words.every(w => FALSE_NAME_WORDS.has(w))) return true;
  // If the first word is a common non-name word and short phrase
  if (FALSE_NAME_WORDS.has(words[0]) && words.length <= 3) return true;
  // If the last word is a common non-name word and short phrase
  if (words.length <= 3 && FALSE_NAME_WORDS.has(words[words.length - 1]) &&
      FALSE_NAME_WORDS.has(words[0])) return true;
  // Specific known false positive patterns
  const lower = text.toLowerCase();
  if (/^(our |the |my |your |get |how |why |what |view |find |meet |read |see |more |all )/i.test(lower)) return true;
  // Phrases that end with non-name words
  if (/\s+(members|resources|services|information|committee|council|registration|representatives|productions|archives|shop|center|office|department)$/i.test(text)) return true;
  return false;
}

function dedupKey(lead) {
  const name = `${(lead.first_name || '').toLowerCase()}|${(lead.last_name || '').toLowerCase()}`;
  const firm = (lead.firm_name || '').toLowerCase().trim();
  const email = (lead.email || '').toLowerCase().trim();
  if (email) return `email:${email}`;
  if (name !== '|') return `name:${name}|${firm}`;
  if (firm) return `firm:${firm}`;
  return `rand:${Math.random()}`;
}

// ── HTTP Fetcher ─────────────────────────────────────────────────────────────

function httpGet(url, retries = MAX_RETRIES, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        ...extraHeaders,
      },
      timeout: REQUEST_TIMEOUT,
    };

    const req = mod.get(url, options, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const u = new URL(url);
          redirectUrl = `${u.protocol}//${u.host}${redirectUrl}`;
        }
        return httpGet(redirectUrl, retries, extraHeaders).then(resolve).catch(reject);
      }

      if (res.statusCode === 404) {
        return resolve({ statusCode: 404, body: '' });
      }

      if (res.statusCode === 429 || res.statusCode === 403) {
        if (retries > 0) {
          const wait = (MAX_RETRIES - retries + 1) * 5000;
          log(`  Rate limited (${res.statusCode}), waiting ${wait / 1000}s...`);
          return sleep(wait).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
        }
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      if (res.statusCode !== 200) {
        return resolve({ statusCode: res.statusCode, body: '' });
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ statusCode: 200, body }));
      res.on('error', reject);
    });

    req.on('error', (err) => {
      if (retries > 0) {
        log(`  Request error: ${err.message}, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(err);
      }
    });

    req.on('timeout', () => {
      req.destroy();
      if (retries > 0) {
        log(`  Timeout, retrying...`);
        sleep(3000).then(() => httpGet(url, retries - 1, extraHeaders)).then(resolve).catch(reject);
      } else {
        reject(new Error('Request timed out'));
      }
    });
  });
}

// ── Source 1: HEAG (Higher Education Assistance Group) Team ──────────────────
// URL: https://heag.us/our-team/
// 68+ financial aid consultants, many with emails
// Structure: .sptp-member cards with .sptp-member-name, .sptp-member-profession,
//   .sptp-member-email, .sptp-member-desc

async function scrapeHEAG(isTest) {
  const SOURCE = 'heag';
  const leads = [];

  log(`\n=== HEAG (Higher Education Assistance Group) Team ===`);
  const url = 'https://heag.us/our-team/';
  log(`  [HEAG] ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`    HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);
    let count = 0;

    // Parse team member cards
    // Try multiple selector strategies for the SP Team Pro plugin
    const memberSelectors = [
      '.sptp-member',
      '.sp-team-pro-item',
      'article.sptp-member',
      '.sptp-row .sptp-col-lg-4',
      '.sptp-row .sptp-col-md-3',
    ];

    let memberEls = $();
    for (const sel of memberSelectors) {
      const found = $(sel);
      if (found.length > 0) {
        memberEls = found;
        log(`    Using selector: ${sel} (${found.length} elements)`);
        break;
      }
    }

    if (memberEls.length === 0) {
      // Fallback: try to find name elements directly
      log(`    No member cards found with standard selectors, trying name-based extraction`);

      // Look for .sptp-member-name elements
      $('.sptp-member-name, [class*="member-name"]').each((_, el) => {
        const nameEl = $(el);
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 3 || fullName.length > 60) return;

        const container = nameEl.closest('div, article, section');

        let email = '';
        container.find('a[href^="mailto:"]').each((_, a) => {
          if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
        });
        // Also check for .sptp-member-email
        if (!email) {
          container.find('[class*="member-email"] a').each((_, a) => {
            if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
          });
        }

        let title = '';
        container.find('.sptp-member-profession, [class*="member-profession"]').each((_, t) => {
          if (!title) title = $(t).text().trim();
        });

        const { first_name, last_name } = splitName(fullName);
        if (first_name || last_name) {
          leads.push({
            first_name,
            last_name,
            firm_name: 'The Higher Education Assistance Group (HEAG)',
            title: title || 'Financial Aid Consultant',
            email,
            phone: '',
            website: 'https://heag.us',
            domain: 'heag.us',
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: url,
          });
          count++;
        }
      });
    } else {
      memberEls.each((_, el) => {
        const member = $(el);

        let fullName = '';
        member.find('.sptp-member-name, [class*="member-name"] a, [class*="member-name"]').each((_, n) => {
          if (!fullName) fullName = $(n).text().trim();
        });
        if (!fullName) {
          // Try h3/h4 heading
          member.find('h3, h4, h5').each((_, n) => {
            if (!fullName) fullName = $(n).text().trim();
          });
        }
        if (!fullName || fullName.length < 3 || fullName.length > 80) return;

        let email = '';
        member.find('a[href^="mailto:"]').each((_, a) => {
          if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
        });
        member.find('.sptp-member-email a, [class*="member-email"] a').each((_, a) => {
          if (!email) {
            const href = $(a).attr('href') || '';
            if (href.includes('mailto:')) {
              email = href.replace('mailto:', '').trim().toLowerCase();
            }
          }
        });

        let title = '';
        member.find('.sptp-member-profession, [class*="member-profession"]').each((_, t) => {
          if (!title) title = $(t).text().trim();
        });

        let phone = '';
        member.find('a[href^="tel:"]').each((_, a) => {
          if (!phone) phone = formatPhone($(a).text().trim());
        });

        const { first_name, last_name } = splitName(fullName);
        if (first_name || last_name) {
          leads.push({
            first_name,
            last_name,
            firm_name: 'The Higher Education Assistance Group (HEAG)',
            title: title || 'Financial Aid Consultant',
            email,
            phone,
            website: 'https://heag.us',
            domain: 'heag.us',
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: url,
          });
          count++;
        }
      });
    }

    // Additional fallback: scan raw HTML for email pattern @heag.us and extract names
    if (count === 0) {
      log(`    Trying raw HTML extraction fallback...`);
      const emailMatches = body.match(/[a-zA-Z0-9._%+-]+@heag\.us/gi) || [];
      const uniqueEmails = [...new Set(emailMatches.map(e => e.toLowerCase()))];

      for (const email of uniqueEmails) {
        // Try to derive name from email prefix (e.g., mkerstein@heag.us)
        const prefix = email.split('@')[0];
        let first_name = '', last_name = '';

        // Common patterns: firstlast, first.last, flast
        const dotMatch = prefix.match(/^([a-z]+)\.([a-z]+)$/i);
        if (dotMatch) {
          first_name = dotMatch[1].charAt(0).toUpperCase() + dotMatch[1].slice(1);
          last_name = dotMatch[2].charAt(0).toUpperCase() + dotMatch[2].slice(1);
        }

        leads.push({
          first_name,
          last_name,
          firm_name: 'The Higher Education Assistance Group (HEAG)',
          title: 'Financial Aid Consultant',
          email,
          phone: '',
          website: 'https://heag.us',
          domain: 'heag.us',
          city: '',
          state: '',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
        count++;
      }
    }

    log(`    Found ${count} team members`);

  } catch (err) {
    log(`    ERROR: ${err.message}`);
  }

  log(`  HEAG total: ${leads.length} leads`);
  return leads;
}

// ── Source 2: Bright Horizons College Coach ──────────────────────────────────
// Finance experts: https://getintocollege.com/Our-Experts/Finance-Experts
// Admissions experts: https://getintocollege.com/Our-Experts/Admissions-Experts
// Each has individual profile pages with bio details

async function scrapeBrightHorizons(isTest) {
  const SOURCE = 'bright-horizons-college-coach';
  const leads = [];

  log(`\n=== Bright Horizons College Coach ===`);

  const pages = [
    { url: 'https://getintocollege.com/Our-Experts/Finance-Experts', category: 'Financial Aid Advisor' },
    { url: 'https://getintocollege.com/Our-Experts/Admissions-Experts', category: 'College Admissions Consultant' },
  ];

  const pagesToScrape = isTest ? [pages[0]] : pages;

  for (const page of pagesToScrape) {
    log(`  [CollegeCoach] ${page.category}: ${page.url}`);

    try {
      const { statusCode, body } = await httpGet(page.url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Find profile links: /our-experts/finance-experts/name-slug or /our-experts/admissions-experts/name-slug
      const profileLinks = new Set();
      $('a[href]').each((_, el) => {
        const href = ($(el).attr('href') || '').toLowerCase();
        if (href.includes('/our-experts/finance-experts/') || href.includes('/our-experts/admissions-experts/')) {
          // Exclude the category page itself
          const slug = href.split('/').filter(Boolean).pop();
          if (slug && slug !== 'finance-experts' && slug !== 'admissions-experts') {
            let fullUrl = href;
            if (href.startsWith('/')) fullUrl = 'https://getintocollege.com' + href;
            else if (!href.startsWith('http')) fullUrl = 'https://getintocollege.com/' + href;
            profileLinks.add(fullUrl);
          }
        }
      });

      log(`    Found ${profileLinks.size} profile links`);

      // Also try to extract names directly from the listing page
      // Names are typically in link text pointing to profile pages
      $('a[href*="/our-experts/"]').each((_, el) => {
        const linkEl = $(el);
        const href = (linkEl.attr('href') || '').toLowerCase();
        const text = linkEl.text().trim();

        // Skip navigation/category links
        if (!text || text.length > 50 || text.length < 4) return;
        if (/learn more|view|our experts|finance|admissions|back|about/i.test(text) && text.length < 20) return;

        const slug = href.split('/').filter(Boolean).pop();
        if (!slug || slug === 'finance-experts' || slug === 'admissions-experts' || slug === 'our-experts') return;

        // Extract name from slug as fallback: "alex-bickford" -> "Alex Bickford"
        let name = text;
        if (!/^[A-Z]/.test(name)) {
          // Use slug-derived name
          name = slug.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        }

        if (isFalseName(name)) return;

        let profileUrl = href;
        if (href.startsWith('/')) profileUrl = 'https://getintocollege.com' + href;
        else if (!href.startsWith('http')) profileUrl = 'https://getintocollege.com/' + href;

        const { first_name, last_name } = splitName(name);
        if ((first_name || last_name) && !leads.some(l =>
          l.first_name === first_name && l.last_name === last_name && l.source === SOURCE)) {
          leads.push({
            first_name,
            last_name,
            firm_name: 'Bright Horizons College Coach',
            title: page.category,
            email: '',
            phone: '',
            website: 'https://getintocollege.com',
            domain: 'getintocollege.com',
            city: '',
            state: '',
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: profileUrl,
          });
          pageLeads++;
        }
      });

      log(`    Extracted ${pageLeads} experts from listing`);

      // Optionally scrape individual profile pages for more details
      const profilesToScrape = isTest ? [...profileLinks].slice(0, 3) : [...profileLinks];
      let profilesScraped = 0;

      for (const profileUrl of profilesToScrape) {
        try {
          await randomDelay();
          const { statusCode: pStatus, body: pBody } = await httpGet(profileUrl);
          if (pStatus !== 200 || !pBody) continue;

          const p$ = cheerio.load(pBody);

          // Look for more detailed info on profile page
          // Typically has bio text, location, specialties
          const bodyText = p$('body').text();

          // Extract location from bio text
          const locationMatch = bodyText.match(/(?:based in|located in|lives in|from)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*([A-Z]{2})?/i);

          if (locationMatch) {
            // Find and update the lead for this profile
            const existingLead = leads.find(l => l.profile_url === profileUrl);
            if (existingLead) {
              if (locationMatch[1]) existingLead.city = locationMatch[1].trim();
              if (locationMatch[2]) existingLead.state = locationMatch[2].trim();
            }
          }

          profilesScraped++;
        } catch (err) {
          // Skip individual profile errors
        }
      }

      if (profilesScraped > 0) {
        log(`    Enriched ${profilesScraped} profiles`);
      }

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Bright Horizons total: ${leads.length} leads`);
  return leads;
}

// ── Source 3: Financial Aid Consulting Firms ─────────────────────────────────
// Scrape team/about pages from known financial aid consulting firms

async function scrapeFinancialAidFirms(isTest) {
  const SOURCE = 'financial-aid-firms';
  const leads = [];

  log(`\n=== Financial Aid Consulting Firms ===`);

  // List of known financial aid consulting firms with public team pages
  const firms = [
    {
      name: 'College Financing Group',
      urls: [
        'https://www.collegefinancinggroup.com/',
        'https://www.collegefinancinggroup.com/about-us/our-team/',
      ],
      defaultTitle: 'Financial Aid Consultant',
    },
    {
      name: 'College Financial Aid Advisors',
      urls: ['https://collegefinancialaidadvisors.com/', 'https://collegefinancialaidadvisors.com/about-us/'],
      defaultTitle: 'Financial Aid Advisor',
    },
    {
      name: 'College Aid Consulting Services',
      urls: ['https://morecollegemoney.com/', 'https://morecollegemoney.com/about/'],
      defaultTitle: 'Financial Aid Consultant',
    },
    {
      name: 'Financial Aid Coach',
      urls: ['https://www.financialaidcoach.com/'],
      defaultTitle: 'Financial Aid Coach',
    },
    {
      name: 'College Career Consulting',
      urls: ['https://collegecareerconsulting.com/'],
      defaultTitle: 'College Career Consultant',
    },
    {
      name: 'College Financial Consultants',
      urls: ['https://collegefinancial-consultants.com/'],
      defaultTitle: 'College Financial Consultant',
    },
    {
      name: 'American College Planning Service',
      urls: ['https://americancollegeplanningservice.com/'],
      defaultTitle: 'Financial Aid Advisor',
    },
    {
      name: 'The FAFSA Guru',
      urls: ['https://www.thefafsaguru.com/', 'https://www.thefafsaguru.com/consulting/'],
      defaultTitle: 'FAFSA Consultant',
    },
    {
      name: 'College Aid Smart',
      urls: ['https://collegeaidsmart.com/'],
      defaultTitle: 'Financial Aid Consultant',
    },
    {
      name: 'College Aid Pro',
      urls: ['https://collegeaidpro.com/'],
      defaultTitle: 'College Financial Planner',
    },
    {
      name: 'Financial Aid Consulting',
      urls: ['https://www.financialaidconsulting.com/'],
      defaultTitle: 'Financial Aid Consultant',
    },
    {
      name: 'HelloCollege',
      urls: ['https://sayhellocollege.com/'],
      defaultTitle: 'Scholarship Services Consultant',
    },
    {
      name: 'Scholarship Advisor',
      urls: ['https://scholarship-advisor.com/'],
      defaultTitle: 'Scholarship Advisor',
    },
    {
      name: 'College Aid Services',
      urls: ['https://www.collegeaidservices.net/'],
      defaultTitle: 'College Aid Consultant',
    },
  ];

  const firmsToScrape = isTest ? firms.slice(0, 4) : firms;

  for (const firm of firmsToScrape) {
    for (const url of firm.urls) {
      log(`  [Firms] ${firm.name}: ${url}`);

      try {
        const { statusCode, body } = await httpGet(url);
        if (statusCode !== 200 || !body) {
          log(`    HTTP ${statusCode} - skipping`);
          continue;
        }

        const $ = cheerio.load(body);
        let pageLeads = 0;

        // Strategy 1: Extract email addresses
        const emails = new Set();
        $('a[href^="mailto:"]').each((_, el) => {
          const email = $(el).attr('href').replace('mailto:', '').trim().toLowerCase();
          if (email && email.includes('@') && !email.includes('example') &&
              !/info@|contact@|support@|help@|admin@|sales@|office@|hello@|team@/i.test(email)) {
            emails.add(email);
          }
        });

        // Strategy 2: Extract phone numbers
        const phones = new Set();
        $('a[href^="tel:"]').each((_, el) => {
          const phone = formatPhone($(el).text().trim());
          if (phone) phones.add(phone);
        });

        // Strategy 3: Look for people names in headings and team sections
        const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$/;
        $('h1, h2, h3, h4, h5').each((_, el) => {
          const text = $(el).text().trim().replace(/\s+/g, ' ');
          if (text.length > 50 || text.length < 4) return;

          // Check if it looks like a person name
          if (!namePattern.test(text)) return;

          // Skip common section headings and false positives
          if (/about|team|our|meet|staff|contact|sign|blog|why|how|services|resources|home|what|the\s/i.test(text)) return;
          if (isFalseName(text)) return;

          const { first_name, last_name } = splitName(text);
          if (first_name && last_name) {
            // Try to find title near the name
            let title = firm.defaultTitle;
            const nextEl = $(el).next('p, span, div, h6');
            const nextText = nextEl.text().trim();
            if (nextText && nextText.length < 100 && nextText.length > 3 &&
                /director|founder|president|ceo|coo|vp|advisor|consultant|coach|counselor|manager|specialist|officer|expert|planner/i.test(nextText)) {
              title = nextText.split('.')[0].trim(); // Take first sentence
              if (title.length > 80) title = firm.defaultTitle;
            }

            if (!leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === firm.name)) {
              leads.push({
                first_name,
                last_name,
                firm_name: firm.name,
                title,
                email: '',
                phone: '',
                website: url.replace(/\/about.*|\/team.*|\/consulting\/?$/i, '/'),
                domain: extractDomain(url),
                city: '',
                state: '',
                country: 'US',
                niche: 'education',
                source: SOURCE,
                profile_url: url,
              });
              pageLeads++;
            }
          }
        });

        // Strategy 4: JSON-LD for organization/founder data
        $('script[type="application/ld+json"]').each((_, el) => {
          try {
            const json = JSON.parse($(el).html());
            const people = [];
            if (json.founder) people.push(...(Array.isArray(json.founder) ? json.founder : [json.founder]));
            if (json.employee) people.push(...(Array.isArray(json.employee) ? json.employee : [json.employee]));
            if (json.member) people.push(...(Array.isArray(json.member) ? json.member : [json.member]));
            // Check for author
            if (json.author && json.author.name) people.push(json.author);

            for (const person of people) {
              if (!person || !person.name) continue;
              const { first_name, last_name } = splitName(person.name);
              if (first_name && last_name &&
                  !leads.some(l => l.first_name === first_name && l.last_name === last_name)) {
                leads.push({
                  first_name,
                  last_name,
                  firm_name: firm.name,
                  title: person.jobTitle || firm.defaultTitle,
                  email: person.email || '',
                  phone: person.telephone ? formatPhone(person.telephone) : '',
                  website: url.replace(/\/about.*|\/team.*|\/consulting\/?$/i, '/'),
                  domain: extractDomain(url),
                  city: '',
                  state: '',
                  country: 'US',
                  niche: 'education',
                  source: SOURCE,
                  profile_url: url,
                });
                pageLeads++;
              }
            }
          } catch {}
        });

        // Strategy 5: If we found contact emails but no names, create a lead with firm info
        if (pageLeads === 0 && emails.size > 0) {
          for (const email of emails) {
            // Skip generic addresses
            if (/info@|contact@|support@|help@|admin@|sales@|office@|hello@|team@/i.test(email)) continue;

            // Try to extract name from email
            const prefix = email.split('@')[0];
            let first_name = '', last_name = '';
            const dotParts = prefix.split('.');
            if (dotParts.length >= 2) {
              first_name = dotParts[0].charAt(0).toUpperCase() + dotParts[0].slice(1);
              last_name = dotParts[1].charAt(0).toUpperCase() + dotParts[1].slice(1);
            }

            if (!leads.some(l => l.email === email)) {
              leads.push({
                first_name,
                last_name,
                firm_name: firm.name,
                title: firm.defaultTitle,
                email,
                phone: [...phones][0] || '',
                website: url.replace(/\/about.*|\/team.*|\/consulting\/?$/i, '/'),
                domain: extractDomain(url),
                city: '',
                state: '',
                country: 'US',
                niche: 'education',
                source: SOURCE,
                profile_url: url,
              });
              pageLeads++;
            }
          }
        }

        log(`    Found ${pageLeads} leads`);

      } catch (err) {
        log(`    ERROR: ${err.message}`);
      }

      await randomDelay();
    }
  }

  log(`  Financial Aid Firms total: ${leads.length} leads`);
  return leads;
}

// ── Source 4: University Financial Aid Office Staff ──────────────────────────
// Many large universities publicly list their financial aid office staff

async function scrapeUniversityFAStaff(isTest) {
  const SOURCE = 'university-fa-staff';
  const leads = [];

  log(`\n=== University Financial Aid Office Staff ===`);

  const universities = [
    { url: 'https://fa.nmsu.edu/financial-aid-advisors/', name: 'New Mexico State University', state: 'NM' },
    { url: 'https://www.finaid.ucsb.edu/staff', name: 'UC Santa Barbara', state: 'CA' },
    { url: 'https://www.k-state.edu/sfa/about/advisor/', name: 'Kansas State University', state: 'KS' },
    { url: 'https://www.du.edu/admission-aid/financial-aid-scholarships/meet-our-team', name: 'University of Denver', state: 'CO' },
    { url: 'https://www.lindenwood.edu/student-financial-services/find-your-financial-aid-advisor/', name: 'Lindenwood University', state: 'MO' },
    { url: 'https://www.chabotcollege.edu/finaid/team.php', name: 'Chabot College', state: 'CA' },
    { url: 'https://financialaid.iastate.edu/about-us/meet-our-team/', name: 'Iowa State University', state: 'IA' },
    { url: 'https://www.odu.edu/finaidoffice/directory', name: 'Old Dominion University', state: 'VA' },
    { url: 'https://finaid.umich.edu/about-us/staff-directory/', name: 'University of Michigan', state: 'MI' },
    { url: 'https://financialaid.uoregon.edu/staff', name: 'University of Oregon', state: 'OR' },
    { url: 'https://www.bu.edu/finaid/about/staff/', name: 'Boston University', state: 'MA' },
    { url: 'https://financialaid.arizona.edu/about/staff', name: 'University of Arizona', state: 'AZ' },
    { url: 'https://www.vanderbilt.edu/financialaid/about/staff/', name: 'Vanderbilt University', state: 'TN' },
    { url: 'https://finaid.emory.edu/about/staff.html', name: 'Emory University', state: 'GA' },
    { url: 'https://www.unh.edu/financial-aid/staff', name: 'University of New Hampshire', state: 'NH' },
  ];

  const unisToScrape = isTest ? universities.slice(0, 3) : universities;

  for (const uni of unisToScrape) {
    log(`  [Uni] ${uni.name}: ${uni.url}`);

    try {
      const { statusCode, body } = await httpGet(uni.url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Strategy 1: Look for staff cards/rows with names, titles, emails, phones
      // Common patterns across university sites
      const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?$/;

      // Scan headings for person names
      $('h2, h3, h4, h5, strong, b, .staff-name, .name, .person-name, [class*="name"]').each((_, el) => {
        const nameEl = $(el);
        const text = nameEl.text().trim().replace(/\s+/g, ' ');
        if (text.length > 50 || text.length < 4) return;

        // Must look like a person name
        if (!namePattern.test(text)) return;
        // Skip headings and false positives
        if (/about|team|meet|staff|contact|our|office|financial|student|scholarship|welcome|directory/i.test(text)) return;
        if (isFalseName(text)) return;

        const { first_name, last_name } = splitName(text);
        if (!first_name || !last_name) return;

        // Look for title and email in nearby elements
        let title = 'Financial Aid Advisor';
        let email = '';
        let phone = '';

        const container = nameEl.closest('div, tr, li, article, section, .card, .staff-member, [class*="staff"], [class*="person"]');

        if (container.length) {
          // Look for title/position
          container.find('p, span, .title, .position, .role, [class*="title"], [class*="position"], [class*="role"], td').each((_, titleEl) => {
            const t = $(titleEl).text().trim();
            if (t && t !== text && t.length < 100 && t.length > 3 &&
                /director|advisor|counselor|coordinator|specialist|manager|officer|assistant|associate|dean|analyst|supervisor/i.test(t) &&
                !namePattern.test(t)) {
              // Take the first matching title
              if (title === 'Financial Aid Advisor') {
                title = t.split('\n')[0].trim();
                if (title.length > 80) title = title.substring(0, 80);
              }
            }
          });

          // Look for email
          container.find('a[href^="mailto:"]').each((_, a) => {
            if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
          });

          // Look for phone
          container.find('a[href^="tel:"]').each((_, a) => {
            if (!phone) phone = formatPhone($(a).text().trim());
          });
        }

        // Also search next sibling elements
        if (!email) {
          const nextEls = nameEl.nextAll('p, div, span, a').slice(0, 5);
          nextEls.each((_, nextEl) => {
            if (!email) {
              $(nextEl).find('a[href^="mailto:"]').each((_, a) => {
                if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
              });
              // Also check the element itself
              const href = $(nextEl).attr('href') || '';
              if (href.startsWith('mailto:') && !email) {
                email = href.replace('mailto:', '').trim().toLowerCase();
              }
            }
          });
        }

        if (!leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === uni.name)) {
          leads.push({
            first_name,
            last_name,
            firm_name: uni.name,
            title,
            email,
            phone,
            website: uni.url,
            domain: extractDomain(uni.url),
            city: '',
            state: uni.state,
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: uni.url,
          });
          pageLeads++;
        }
      });

      // Strategy 2: Look for table rows with staff info
      $('table tr, table tbody tr').each((_, row) => {
        const cells = $(row).find('td');
        if (cells.length < 2) return;

        const firstCellText = $(cells[0]).text().trim().replace(/\s+/g, ' ');
        if (firstCellText.length > 50 || firstCellText.length < 4) return;
        if (!namePattern.test(firstCellText)) return;
        if (isFalseName(firstCellText)) return;

        const { first_name, last_name } = splitName(firstCellText);
        if (!first_name || !last_name) return;

        let title = 'Financial Aid Advisor';
        let email = '';
        let phone = '';

        cells.each((i, cell) => {
          if (i === 0) return; // Skip name cell
          const cellText = $(cell).text().trim();

          // Check for email
          $(cell).find('a[href^="mailto:"]').each((_, a) => {
            if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
          });

          // Check for phone
          $(cell).find('a[href^="tel:"]').each((_, a) => {
            if (!phone) phone = formatPhone($(a).text().trim());
          });

          // Check for title
          if (!email && !phone && cellText.length > 5 && cellText.length < 100 &&
              /director|advisor|counselor|coordinator|specialist|manager|officer|assistant|associate/i.test(cellText)) {
            title = cellText;
          }
        });

        if (!leads.some(l => l.first_name === first_name && l.last_name === last_name && l.firm_name === uni.name)) {
          leads.push({
            first_name,
            last_name,
            firm_name: uni.name,
            title,
            email,
            phone,
            website: uni.url,
            domain: extractDomain(uni.url),
            city: '',
            state: uni.state,
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: uni.url,
          });
          pageLeads++;
        }
      });

      log(`    Found ${pageLeads} staff`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  University FA Staff total: ${leads.length} leads`);
  return leads;
}

// ── Source 5: NASFAA Certification Commission Bios ──────────────────────────
// URL: https://www.nasfaa.org/certification_commission_bios
// Commission members who help set standards for CFAA certification

async function scrapeNASFAACommission(isTest) {
  const SOURCE = 'nasfaa-commission';
  const leads = [];

  log(`\n=== NASFAA Certification Commission ===`);
  const url = 'https://www.nasfaa.org/certification_commission_bios';
  log(`  [NASFAA] ${url}`);

  try {
    const { statusCode, body } = await httpGet(url);
    if (statusCode !== 200 || !body) {
      log(`    HTTP ${statusCode} - skipping`);
      return leads;
    }

    const $ = cheerio.load(body);
    let count = 0;

    // Commission bios typically have names in h2/h3/strong and institutions after
    const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?(?:,?\s*(?:FAAC|Ph\.D\.|Ed\.D\.|M\.Ed\.|MBA|CPA)®?)?$/;

    $('h2, h3, h4, strong, b').each((_, el) => {
      let text = $(el).text().trim().replace(/\s+/g, ' ');
      if (text.length > 80 || text.length < 5) return;

      // Check if it looks like a person name
      if (!namePattern.test(text) && !/^[A-Z][a-z]+ [A-Z]/.test(text)) return;
      // Skip section headings
      if (/commission|about|certification|program|overview|eligibility|resources|contact|search/i.test(text)) return;

      const { first_name, last_name } = splitName(text);
      if (!first_name || !last_name) return;

      // Look for institution/title in nearby elements
      let title = 'Financial Aid Administrator';
      let firmName = '';

      const nextEls = $(el).nextAll('p, div, span').slice(0, 3);
      nextEls.each((_, nextEl) => {
        const nextText = $(nextEl).text().trim();
        if (nextText.length < 5 || nextText.length > 200) return;

        // Look for institution name (typically "Title at Institution" or just "Institution")
        if (!firmName) {
          const atMatch = nextText.match(/^(.+?)\s+at\s+(.+?)(?:\.|$)/i);
          if (atMatch) {
            title = atMatch[1].trim();
            firmName = atMatch[2].trim();
          } else if (/university|college|institute|school|association/i.test(nextText)) {
            firmName = nextText.split('.')[0].trim();
          }
        }
      });

      if (!leads.some(l => l.first_name === first_name && l.last_name === last_name)) {
        leads.push({
          first_name,
          last_name,
          firm_name: firmName || 'NASFAA',
          title,
          email: '',
          phone: '',
          website: 'https://www.nasfaa.org',
          domain: 'nasfaa.org',
          city: '',
          state: '',
          country: 'US',
          niche: 'education',
          source: SOURCE,
          profile_url: url,
        });
        count++;
      }
    });

    log(`    Found ${count} commission members`);

  } catch (err) {
    log(`    ERROR: ${err.message}`);
  }

  log(`  NASFAA Commission total: ${leads.length} leads`);
  return leads;
}

// ── Source 6: Known Independent Financial Aid Professionals ──────────────────
// Manually curated list of publicly known scholarship advisors and FA consultants
// from web research with publicly available contact info

async function scrapeKnownProfessionals(isTest) {
  const SOURCE = 'known-professionals';
  const leads = [];

  log(`\n=== Known Financial Aid Professionals ===`);

  // These professionals have public websites and are well-known in the FA industry
  const professionals = [
    {
      first_name: 'Jodi', last_name: 'Okun',
      firm_name: 'College Financial Aid Advisors',
      title: 'Founder & Financial Aid Advisor',
      website: 'https://collegefinancialaidadvisors.com',
      domain: 'collegefinancialaidadvisors.com',
    },
    {
      first_name: 'Ronald', last_name: 'Ramsdell',
      firm_name: 'College Aid Consulting Services',
      title: 'Founder & Financial Aid Consultant',
      website: 'https://morecollegemoney.com',
      domain: 'morecollegemoney.com',
    },
    {
      first_name: 'Andy', last_name: 'Leardini',
      firm_name: 'College Financing Group',
      title: 'Founder & Financial Aid Consultant',
      email: 'andy@collegefinancinggroup.com',
      phone: '(716) 745-7286',
      website: 'https://www.collegefinancinggroup.com',
      domain: 'collegefinancinggroup.com',
    },
    {
      first_name: 'Bonnie', last_name: 'Rabin',
      firm_name: 'College Career Consulting',
      title: 'Founder & College Advisor',
      website: 'https://collegecareerconsulting.com',
      domain: 'collegecareerconsulting.com',
    },
    {
      first_name: 'Milton', last_name: 'Kerstein',
      firm_name: 'The Higher Education Assistance Group (HEAG)',
      title: 'President & Founder',
      email: 'mkerstein@heag.us',
      website: 'https://heag.us',
      domain: 'heag.us',
    },
    {
      first_name: 'Karyn', last_name: 'Wright-Moore',
      firm_name: 'The Higher Education Assistance Group (HEAG)',
      title: 'VP of Compliance and Quality Assurance',
      email: 'kwrightmoore@heag.us',
      website: 'https://heag.us',
      domain: 'heag.us',
    },
    {
      first_name: 'Karen', last_name: 'Van Dyne',
      firm_name: 'The Higher Education Assistance Group (HEAG)',
      title: 'Director of Business Affairs',
      email: 'kvandyne@heag.us',
      website: 'https://heag.us',
      domain: 'heag.us',
    },
    {
      first_name: 'Amanda', last_name: 'Manuel',
      firm_name: 'The Higher Education Assistance Group (HEAG)',
      title: 'Quality Assurance Specialist & Client Liaison',
      email: 'amanuel@heag.us',
      website: 'https://heag.us',
      domain: 'heag.us',
    },
    {
      first_name: 'Shannon', last_name: 'Vasconcelos',
      firm_name: 'Bright Horizons College Coach',
      title: 'Senior Financial Aid Advisor',
      website: 'https://getintocollege.com',
      domain: 'getintocollege.com',
    },
    {
      first_name: 'Melanie', last_name: 'Storey',
      firm_name: 'NASFAA',
      title: 'President and CEO',
      website: 'https://www.nasfaa.org',
      domain: 'nasfaa.org',
    },
    {
      first_name: 'Kristi', last_name: 'Jovell',
      firm_name: 'Johns Hopkins University',
      title: 'Director of Strategic Financial Aid Initiatives',
      website: 'https://www.jhu.edu',
      domain: 'jhu.edu',
      state: 'MD',
    },
    {
      first_name: 'Blaine', last_name: 'Blontz',
      firm_name: 'Financial Aid Coach',
      title: 'Founder & Financial Aid Coach',
      website: 'https://www.financialaidcoach.com',
      domain: 'financialaidcoach.com',
    },
    {
      first_name: 'Mark', last_name: 'Kantrowitz',
      firm_name: 'Kantrowitz Consulting',
      title: 'Financial Aid Expert & Author',
      website: 'https://www.kantrowitz.com',
      domain: 'kantrowitz.com',
    },
    {
      first_name: 'Jeff', last_name: 'Levy',
      firm_name: 'Levy Financial Aid Consulting',
      title: 'Financial Aid Consultant',
      website: 'https://www.levyfinancialaid.com',
      domain: 'levyfinancialaid.com',
    },
    {
      first_name: 'Kalman', last_name: 'Chany',
      firm_name: 'Campus Consultants',
      title: 'President & Financial Aid Consultant',
      website: 'https://www.campusconsultants.com',
      domain: 'campusconsultants.com',
      city: 'New York',
      state: 'NY',
    },
  ];

  const profToUse = isTest ? professionals.slice(0, 5) : professionals;

  for (const prof of profToUse) {
    leads.push({
      first_name: prof.first_name,
      last_name: prof.last_name,
      firm_name: prof.firm_name,
      title: prof.title,
      email: prof.email || '',
      phone: prof.phone || '',
      website: prof.website || '',
      domain: prof.domain || extractDomain(prof.website),
      city: prof.city || '',
      state: prof.state || '',
      country: 'US',
      niche: 'education',
      source: SOURCE,
      profile_url: prof.website || '',
    });
  }

  log(`  Known Professionals total: ${leads.length} leads`);
  return leads;
}

// ── Source 7: Scholarship / FA Directory Sites ──────────────────────────────
// Scrape Finaid.org professional associations page for org contacts
// Also check NASFAA Directory of Associations for state association contacts

async function scrapeAssociationContacts(isTest) {
  const SOURCE = 'fa-associations';
  const leads = [];

  log(`\n=== Financial Aid Association Contacts ===`);

  // State/regional FA association websites to scan for leadership/staff contacts
  const associations = [
    { url: 'https://www.fasfaa.org/about_fasfaa.php', name: 'FASFAA (Florida)', state: 'FL' },
    { url: 'https://www.casfaa.org/', name: 'CASFAA (California)', state: 'CA' },
    { url: 'https://www.easfaa.org/', name: 'EASFAA (Eastern)', state: '' },
    { url: 'https://www.masfaaweb.org/', name: 'MASFAA (Midwest)', state: '' },
    { url: 'https://www.rmasfaa.org/', name: 'RMASFAA (Rocky Mountain)', state: '' },
    { url: 'https://www.sasfaa.org/', name: 'SASFAA (Southern)', state: '' },
    { url: 'https://www.swasfaa.org/', name: 'SWASFAA (Southwest)', state: '' },
    { url: 'https://www.wasfaa.org/', name: 'WASFAA (Western)', state: '' },
    { url: 'https://www.ncasfaa.com/', name: 'NCASFAA (North Carolina)', state: 'NC' },
    { url: 'https://www.dedcmdasfaa.org/', name: 'Tristate (DE-DC-MD)', state: '' },
    { url: 'https://www.njasfaa.com/', name: 'NJASFAA (New Jersey)', state: 'NJ' },
    { url: 'https://www.nysfaaa.org/', name: 'NYSFAAA (New York)', state: 'NY' },
    { url: 'https://www.pasfaa.org/', name: 'PASFAA (Pennsylvania)', state: 'PA' },
    { url: 'https://www.tasfaa.org/', name: 'TASFAA (Texas)', state: 'TX' },
    { url: 'https://www.oasfaa.org/', name: 'OASFAA (Ohio)', state: 'OH' },
  ];

  const assocToScrape = isTest ? associations.slice(0, 3) : associations;

  for (const assoc of assocToScrape) {
    log(`  [Assoc] ${assoc.name}: ${assoc.url}`);

    try {
      const { statusCode, body } = await httpGet(assoc.url);
      if (statusCode !== 200 || !body) {
        log(`    HTTP ${statusCode} - skipping`);
        continue;
      }

      const $ = cheerio.load(body);
      let pageLeads = 0;

      // Look for leadership/board/officer sections
      // Common patterns: "Board of Directors", "Officers", "Executive Committee"
      const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?$/;

      // Extract from headings
      $('h2, h3, h4, h5, strong, b, .name, [class*="officer"], [class*="board"]').each((_, el) => {
        const text = $(el).text().trim().replace(/\s+/g, ' ');
        if (text.length > 50 || text.length < 4) return;
        if (!namePattern.test(text)) return;
        if (/about|board|officer|director|committee|executive|president|chair|contact|membership/i.test(text) && text.split(' ').length <= 2) return;
        if (isFalseName(text)) return;

        const { first_name, last_name } = splitName(text);
        if (!first_name || !last_name) return;

        // Look for title and email nearby
        let title = 'Financial Aid Administrator';
        let email = '';

        const container = $(el).closest('div, li, tr, p, section');
        if (container.length) {
          container.find('a[href^="mailto:"]').each((_, a) => {
            if (!email) email = $(a).attr('href').replace('mailto:', '').trim().toLowerCase();
          });

          const containerText = container.text();
          const titleMatch = containerText.match(/(?:president|chair|treasurer|secretary|vice\s*president|past\s*president|director|coordinator)/i);
          if (titleMatch) {
            title = titleMatch[0].trim();
            // Capitalize
            title = title.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
          }
        }

        if (!leads.some(l => l.first_name === first_name && l.last_name === last_name)) {
          leads.push({
            first_name,
            last_name,
            firm_name: assoc.name,
            title,
            email,
            phone: '',
            website: assoc.url,
            domain: extractDomain(assoc.url),
            city: '',
            state: assoc.state,
            country: 'US',
            niche: 'education',
            source: SOURCE,
            profile_url: assoc.url,
          });
          pageLeads++;
        }
      });

      // Also look for board/leadership pages linked from the homepage
      const leadershipLinks = [];
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim().toLowerCase();
        if (/(board|officer|leadership|executive|committee|staff|team|about)/i.test(text) ||
            /(board|officer|leadership|executive|committee|staff|team)/i.test(href)) {
          let fullUrl = href;
          if (href.startsWith('/')) {
            const u = new URL(assoc.url);
            fullUrl = `${u.protocol}//${u.host}${href}`;
          } else if (!href.startsWith('http')) {
            fullUrl = assoc.url.replace(/\/$/, '') + '/' + href;
          }
          if (!leadershipLinks.includes(fullUrl) && fullUrl.includes(extractDomain(assoc.url))) {
            leadershipLinks.push(fullUrl);
          }
        }
      });

      // Scrape up to 2 leadership subpages
      const subpagesToScrape = isTest ? leadershipLinks.slice(0, 1) : leadershipLinks.slice(0, 2);
      for (const subUrl of subpagesToScrape) {
        if (subUrl === assoc.url) continue;
        log(`    -> Subpage: ${subUrl}`);

        try {
          await randomDelay();
          const { statusCode: sStatus, body: sBody } = await httpGet(subUrl);
          if (sStatus !== 200 || !sBody) continue;

          const s$ = cheerio.load(sBody);

          s$('h2, h3, h4, h5, strong, b, .name, [class*="name"]').each((_, el) => {
            const text = s$(el).text().trim().replace(/\s+/g, ' ');
            if (text.length > 50 || text.length < 4) return;
            if (!namePattern.test(text)) return;
            if (isFalseName(text)) return;

            const { first_name, last_name } = splitName(text);
            if (!first_name || !last_name) return;

            let email = '';
            const container = s$(el).closest('div, li, tr, p, section');
            if (container.length) {
              container.find('a[href^="mailto:"]').each((_, a) => {
                if (!email) email = s$(a).attr('href').replace('mailto:', '').trim().toLowerCase();
              });
            }

            if (!leads.some(l => l.first_name === first_name && l.last_name === last_name)) {
              leads.push({
                first_name,
                last_name,
                firm_name: assoc.name,
                title: 'Financial Aid Administrator',
                email,
                phone: '',
                website: assoc.url,
                domain: extractDomain(assoc.url),
                city: '',
                state: assoc.state,
                country: 'US',
                niche: 'education',
                source: SOURCE,
                profile_url: subUrl,
              });
              pageLeads++;
            }
          });

        } catch (err) {
          // Skip subpage errors
        }
      }

      log(`    Found ${pageLeads} contacts`);

    } catch (err) {
      log(`    ERROR: ${err.message}`);
    }

    await randomDelay();
  }

  log(`  Association Contacts total: ${leads.length} leads`);
  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-scholarship-advisors.js --test    # Test mode (limited pages)');
    console.log('  node scripts/scrape-scholarship-advisors.js --all     # Full scrape all sources');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log(`Scholarship Advisors & Financial Aid Consultants Scraper`);
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log('');

  const allLeads = [];
  const seenKeys = new Set();
  const sourceStats = {};

  const sources = [
    { name: 'HEAG', fn: () => scrapeHEAG(isTest) },
    { name: 'BrightHorizons', fn: () => scrapeBrightHorizons(isTest) },
    { name: 'FinancialAidFirms', fn: () => scrapeFinancialAidFirms(isTest) },
    { name: 'UniversityFAStaff', fn: () => scrapeUniversityFAStaff(isTest) },
    { name: 'NASFAACommission', fn: () => scrapeNASFAACommission(isTest) },
    { name: 'KnownProfessionals', fn: () => scrapeKnownProfessionals(isTest) },
    { name: 'AssociationContacts', fn: () => scrapeAssociationContacts(isTest) },
  ];

  for (const source of sources) {
    try {
      const leads = await source.fn();
      let added = 0;
      for (const lead of leads) {
        const key = dedupKey(lead);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        allLeads.push(lead);
        added++;
      }
      sourceStats[source.name] = { total: leads.length, unique: added };
      log(`\n  >> ${source.name}: ${leads.length} found, ${added} unique added`);
    } catch (err) {
      log(`\n  ERROR in ${source.name}: ${err.message}`);
      sourceStats[source.name] = { total: 0, unique: 0, error: err.message };
    }

    // Save intermediate progress
    writeCSV(allLeads, OUT_FILE);
  }

  // Final write
  writeCSV(allLeads, OUT_FILE);

  // Stats
  const withEmail = allLeads.filter(l => l.email).length;
  const withPhone = allLeads.filter(l => l.phone).length;
  const withWebsite = allLeads.filter(l => l.website).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;

  log('');
  log('='.repeat(60));
  log('FINAL RESULTS');
  log('='.repeat(60));
  log(`Total unique leads: ${allLeads.length}`);
  log(`With name: ${withName}`);
  log(`With email: ${withEmail}`);
  log(`With phone: ${withPhone}`);
  log(`With website: ${withWebsite}`);
  log('');
  log('Source breakdown:');
  for (const [name, stats] of Object.entries(sourceStats)) {
    log(`  ${name}: ${stats.total} found, ${stats.unique} unique${stats.error ? ` (ERROR: ${stats.error})` : ''}`);
  }
  log('');
  log(`Output: ${OUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
