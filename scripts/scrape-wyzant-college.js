#!/usr/bin/env node
/**
 * Wyzant College Admissions Tutor Scraper
 *
 * Scrapes college admissions tutors/consultants from Wyzant.com across
 * top 50+ US metro areas. Wyzant uses Next.js + Cloudflare, so we use
 * Puppeteer with stealth to bypass bot detection and extract the
 * __NEXT_DATA__ JSON payload embedded in each search results page.
 *
 * Subjects scraped:
 *   - College Counseling
 *   - College Essays
 *   - SAT
 *   - ACT
 *   - Admissions
 *
 * Usage:
 *   node scripts/scrape-wyzant-college.js --test    # Test mode (2 cities, 1 page per subject)
 *   node scripts/scrape-wyzant-college.js --all     # Full scrape (50+ cities, all pages)
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

// ── Config ───────────────────────────────────────────────────────────────────

const DELAY_MIN = 3000;
const DELAY_MAX = 6000;
const PAGE_SIZE = 10;
const MAX_PAGES_PER_SEARCH = 50; // 500 tutors max per subject+city combo
const NAV_TIMEOUT = 45000;

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT_FILE = path.join(OUT_DIR, 'college-tutors-wyzant.csv');

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// Subjects to search
const SUBJECTS = [
  { kw: 'College Counseling', title: 'College Admissions Counselor' },
  { kw: 'College Essays', title: 'College Essay Tutor' },
  { kw: 'SAT', title: 'SAT Tutor' },
  { kw: 'ACT', title: 'ACT Tutor' },
  { kw: 'Admissions', title: 'Admissions Consultant' },
];

// Top 60 US metro areas by population (zip codes for city centers)
const METROS = [
  { city: 'New York', state: 'NY', zip: '10001' },
  { city: 'Los Angeles', state: 'CA', zip: '90001' },
  { city: 'Chicago', state: 'IL', zip: '60601' },
  { city: 'Houston', state: 'TX', zip: '77001' },
  { city: 'Phoenix', state: 'AZ', zip: '85001' },
  { city: 'Philadelphia', state: 'PA', zip: '19101' },
  { city: 'San Antonio', state: 'TX', zip: '78201' },
  { city: 'San Diego', state: 'CA', zip: '92101' },
  { city: 'Dallas', state: 'TX', zip: '75201' },
  { city: 'San Jose', state: 'CA', zip: '95101' },
  { city: 'Austin', state: 'TX', zip: '78701' },
  { city: 'Jacksonville', state: 'FL', zip: '32099' },
  { city: 'Fort Worth', state: 'TX', zip: '76101' },
  { city: 'Columbus', state: 'OH', zip: '43201' },
  { city: 'Charlotte', state: 'NC', zip: '28201' },
  { city: 'Indianapolis', state: 'IN', zip: '46201' },
  { city: 'San Francisco', state: 'CA', zip: '94101' },
  { city: 'Seattle', state: 'WA', zip: '98101' },
  { city: 'Denver', state: 'CO', zip: '80201' },
  { city: 'Washington', state: 'DC', zip: '20001' },
  { city: 'Nashville', state: 'TN', zip: '37201' },
  { city: 'Oklahoma City', state: 'OK', zip: '73101' },
  { city: 'El Paso', state: 'TX', zip: '79901' },
  { city: 'Boston', state: 'MA', zip: '02101' },
  { city: 'Portland', state: 'OR', zip: '97201' },
  { city: 'Las Vegas', state: 'NV', zip: '89101' },
  { city: 'Memphis', state: 'TN', zip: '38101' },
  { city: 'Louisville', state: 'KY', zip: '40201' },
  { city: 'Baltimore', state: 'MD', zip: '21201' },
  { city: 'Milwaukee', state: 'WI', zip: '53201' },
  { city: 'Albuquerque', state: 'NM', zip: '87101' },
  { city: 'Tucson', state: 'AZ', zip: '85701' },
  { city: 'Fresno', state: 'CA', zip: '93701' },
  { city: 'Sacramento', state: 'CA', zip: '95801' },
  { city: 'Mesa', state: 'AZ', zip: '85201' },
  { city: 'Kansas City', state: 'MO', zip: '64101' },
  { city: 'Atlanta', state: 'GA', zip: '30301' },
  { city: 'Omaha', state: 'NE', zip: '68101' },
  { city: 'Colorado Springs', state: 'CO', zip: '80901' },
  { city: 'Raleigh', state: 'NC', zip: '27601' },
  { city: 'Long Beach', state: 'CA', zip: '90801' },
  { city: 'Virginia Beach', state: 'VA', zip: '23450' },
  { city: 'Miami', state: 'FL', zip: '33101' },
  { city: 'Oakland', state: 'CA', zip: '94601' },
  { city: 'Minneapolis', state: 'MN', zip: '55401' },
  { city: 'Tampa', state: 'FL', zip: '33601' },
  { city: 'Tulsa', state: 'OK', zip: '74101' },
  { city: 'Arlington', state: 'TX', zip: '76001' },
  { city: 'New Orleans', state: 'LA', zip: '70112' },
  { city: 'Cleveland', state: 'OH', zip: '44101' },
  { city: 'Honolulu', state: 'HI', zip: '96801' },
  { city: 'Pittsburgh', state: 'PA', zip: '15201' },
  { city: 'Cincinnati', state: 'OH', zip: '45201' },
  { city: 'St. Louis', state: 'MO', zip: '63101' },
  { city: 'Orlando', state: 'FL', zip: '32801' },
  { city: 'Salt Lake City', state: 'UT', zip: '84101' },
  { city: 'Detroit', state: 'MI', zip: '48201' },
  { city: 'Hartford', state: 'CT', zip: '06101' },
  { city: 'Providence', state: 'RI', zip: '02901' },
  { city: 'Richmond', state: 'VA', zip: '23219' },
];

// Well-known US city → state mapping for state inference
const CITY_TO_STATE = {};
for (const m of METROS) {
  CITY_TO_STATE[m.city.toLowerCase()] = m.state;
}
// Add common cities not in metro list
Object.assign(CITY_TO_STATE, {
  'brooklyn': 'NY', 'queens': 'NY', 'bronx': 'NY', 'manhattan': 'NY', 'staten island': 'NY',
  'west lafayette': 'IN', 'naperville': 'IL', 'evanston': 'IL', 'aurora': 'IL',
  'san leandro': 'CA', 'san mateo': 'CA', 'palo alto': 'CA', 'berkeley': 'CA', 'irvine': 'CA',
  'pasadena': 'CA', 'glendale': 'CA', 'burbank': 'CA', 'santa monica': 'CA', 'torrance': 'CA',
  'woodland hills': 'CA', 'encino': 'CA', 'venice': 'CA', 'beverly hills': 'CA',
  'silver spring': 'MD', 'bethesda': 'MD', 'rockville': 'MD', 'columbia': 'MD',
  'alexandria': 'VA', 'arlington': 'VA', 'fairfax': 'VA', 'reston': 'VA', 'mclean': 'VA',
  'pittsford': 'NY', 'buffalo': 'NY', 'white plains': 'NY', 'scarsdale': 'NY',
  'forest hills': 'NY', 'great neck': 'NY', 'garden city': 'NY', 'ithaca': 'NY',
  'cambridge': 'MA', 'newton': 'MA', 'brookline': 'MA', 'wellesley': 'MA',
  'stamford': 'CT', 'greenwich': 'CT', 'new haven': 'CT', 'west hartford': 'CT',
  'hoboken': 'NJ', 'princeton': 'NJ', 'montclair': 'NJ', 'morristown': 'NJ',
  'alpharetta': 'GA', 'kennesaw': 'GA', 'marietta': 'GA', 'decatur': 'GA', 'roswell': 'GA',
  'plano': 'TX', 'frisco': 'TX', 'round rock': 'TX', 'sugar land': 'TX', 'the woodlands': 'TX',
  'centerton': 'AR', 'coatesville': 'PA', 'clarks summit': 'PA', 'farmington': 'CT',
  'worcester': 'MA', 'spartanburg': 'SC', 'haines city': 'FL', 'riverview': 'FL',
  'federal way': 'WA', 'bellevue': 'WA', 'redmond': 'WA', 'tacoma': 'WA',
  'lees summit': 'MO', 'kansas city': 'MO', 'centralia': 'WA', 'boulder': 'CO',
  'scottsdale': 'AZ', 'tempe': 'AZ', 'chandler': 'AZ', 'gilbert': 'AZ',
  'chapel hill': 'NC', 'durham': 'NC', 'cary': 'NC', 'greensboro': 'NC',
  'ann arbor': 'MI', 'troy': 'MI', 'birmingham': 'MI',
  'overland park': 'KS', 'lawrence': 'KS',
  'madison': 'WI', 'waukesha': 'WI',
  'boca raton': 'FL', 'coral gables': 'FL', 'fort lauderdale': 'FL', 'sarasota': 'FL',
  'newport beach': 'CA', 'laguna beach': 'CA', 'san rafael': 'CA', 'walnut creek': 'CA',
});

/**
 * Infer state from city name using known mapping
 */
function inferState(city) {
  if (!city) return '';
  return CITY_TO_STATE[city.toLowerCase().trim()] || '';
}

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

function splitName(fullName) {
  if (!fullName) return { first_name: '', last_name: '' };
  let cleaned = fullName
    .replace(/^(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?)\s+/i, '')
    .trim();
  // Remove trailing credentials after comma
  const commaIdx = cleaned.indexOf(',');
  if (commaIdx > 0) {
    cleaned = cleaned.substring(0, commaIdx).trim();
  }
  // Remove trailing credential abbreviations
  cleaned = cleaned
    .replace(/\s+(CEP|PhD|EdD|MA|MEd|EdM|MS|MPA|JD|MBA|LCSW|LPC|LCPC|LMFT|MPP|Esq|Ph\.D\.|Ed\.D\.|M\.A\.|M\.Ed\.|Ed\.M\.|M\.S\.|M\.P\.A\.|J\.D\.|M\.B\.A\.|M\.P\.P\.?)\.?$/gi, '')
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

/**
 * Build a dedup key from tutor UserID (most reliable) or name+city
 */
function dedupKey(lead) {
  if (lead._userId) return `uid:${lead._userId}`;
  const name = `${(lead.first_name || '').toLowerCase()}|${(lead.last_name || '').toLowerCase()}`;
  const city = (lead.city || '').toLowerCase().trim();
  if (name !== '|') return `name:${name}|${city}`;
  return `rand:${Math.random()}`;
}

/**
 * Determine the best title based on subjects
 */
function inferTitle(subjects, defaultTitle) {
  if (!subjects || !subjects.length) return defaultTitle;
  const subjectStr = subjects.join(', ').toLowerCase();
  if (subjectStr.includes('college counseling') || subjectStr.includes('admissions')) {
    return 'College Admissions Consultant';
  }
  if (subjectStr.includes('college essay')) {
    return 'College Essay Consultant';
  }
  if (subjectStr.includes('sat') && subjectStr.includes('act')) {
    return 'SAT/ACT Tutor';
  }
  if (subjectStr.includes('sat')) return 'SAT Tutor';
  if (subjectStr.includes('act')) return 'ACT Tutor';
  return defaultTitle;
}

// ── Puppeteer Scraper ────────────────────────────────────────────────────────

/**
 * Extract __NEXT_DATA__ JSON from the page
 */
async function extractNextData(page) {
  try {
    const data = await page.evaluate(() => {
      const el = document.getElementById('__NEXT_DATA__');
      if (el) {
        try { return JSON.parse(el.textContent); } catch { return null; }
      }
      // Fallback: try window.__NEXT_DATA__
      if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
      return null;
    });
    return data;
  } catch {
    return null;
  }
}

/**
 * Parse tutors from __NEXT_DATA__ JSON
 */
function parseTutorsFromNextData(nextData, defaultTitle) {
  const tutors = [];
  if (!nextData) return tutors;

  // Navigate the Next.js data structure to find tutor arrays
  // Primary path: pageProps.serverSideKeywordBackfillSearchResults.Tutors
  const pageProps = nextData.props?.pageProps || {};
  const searchResults = pageProps.serverSideKeywordBackfillSearchResults || {};

  // Try multiple possible paths where tutor data might live
  const candidates = [
    searchResults.Tutors,
    pageProps.tutors,
    pageProps.searchResults,
    pageProps.results,
    pageProps.data?.tutors,
    pageProps.data?.searchResults,
    pageProps.initialData?.tutors,
  ];

  let tutorList = null;
  for (const c of candidates) {
    if (Array.isArray(c) && c.length > 0) {
      tutorList = c;
      break;
    }
  }

  // Also try to find tutor count for pagination
  let totalCount = 0;
  const pricingBuckets = searchResults.PricingBuckets || [];
  if (pricingBuckets.length > 0) {
    totalCount = pricingBuckets.reduce((sum, b) => sum + (b.num_tutors || 0), 0);
  }
  if (totalCount === 0) {
    const countCandidates = [
      searchResults.TutorCount,
      pageProps.tutorCount,
      pageProps.totalCount,
      pageProps.TutorCount,
      pageProps.data?.tutorCount,
      pageProps.data?.TutorCount,
    ];
    for (const c of countCandidates) {
      if (typeof c === 'number' && c > 0) {
        totalCount = c;
        break;
      }
    }
  }

  if (!tutorList) {
    // Deep search: look for any array of objects with UserID or Name fields
    const searchObj = (obj, depth = 0) => {
      if (depth > 5 || !obj || typeof obj !== 'object') return null;
      if (Array.isArray(obj)) {
        if (obj.length > 0 && obj[0] && (obj[0].UserID || obj[0].Name || obj[0].userId || obj[0].name)) {
          return obj;
        }
        for (const item of obj) {
          const found = searchObj(item, depth + 1);
          if (found) return found;
        }
      } else {
        for (const key of Object.keys(obj)) {
          const found = searchObj(obj[key], depth + 1);
          if (found) return found;
        }
      }
      return null;
    };
    tutorList = searchObj(pageProps);
  }

  if (!tutorList || !Array.isArray(tutorList)) {
    return { tutors, totalCount };
  }

  for (const t of tutorList) {
    const name = t.Name || t.name || '';
    const userId = t.UserID || t.userId || t.Id || t.id || '';
    const city = t.City || t.city || '';
    const state = t.State || t.state || '';
    const zipCode = t.ZipCode || t.zipCode || t.Zip || t.zip || '';
    const fee = t.FeePerHour || t.feePerHour || t.Rate || t.rate || '';
    const url = t.URL || t.url || t.ProfileUrl || t.profileUrl || '';
    const rating = t.StarRating || t.starRating || t.Rating || '';
    const numHours = t.NumHours || t.numHours || 0;
    const description = t.Description || t.description || t.AdTitle || t.adTitle || '';
    const college = t.GraduateCollege || t.graduateCollege || '';
    const degree = t.GraduateDegree || t.graduateDegree || '';
    const subjects = t.Subjects || t.subjects || [];
    const exactSubject = t.ExactSubject || t.exactSubject || {};

    if (!name) continue;

    // Parse city and state from multiple sources
    let tutorCity = '', tutorState = '';

    // 1. Try explicit State field
    if (state) {
      tutorState = state;
      tutorCity = city;
    }
    // 2. Parse "City, ST" format from City field
    else if (city.includes(',')) {
      const parts = city.split(',').map(s => s.trim());
      tutorCity = parts[0];
      tutorState = parts[1] || '';
      // Clean state (might have extra text like "available for...")
      tutorState = (tutorState.match(/^([A-Z]{2})\b/) || [])[1] || tutorState;
    }
    // 3. Try extracting state from profile URL (/Tutors/ST/City/ID/)
    else if (url) {
      tutorCity = city;
      const urlMatch = url.match(/\/Tutors\/([A-Z]{2})\//);
      if (urlMatch) tutorState = urlMatch[1];
    }
    else {
      tutorCity = city;
    }

    // 4. Fallback: infer state from well-known city names
    if (!tutorState && tutorCity) {
      tutorState = inferState(tutorCity);
    }

    // Build profile URL
    let profileUrl = url;
    if (profileUrl && !profileUrl.startsWith('http')) {
      profileUrl = 'https://www.wyzant.com' + profileUrl;
    }

    // Build firm_name from description/ad title
    let firmName = '';
    // Wyzant tutors are typically independent, no firm

    const { first_name, last_name } = splitName(name);

    // Collect all known subjects for this tutor
    const subjectList = [];
    if (exactSubject && exactSubject.SubjectName) {
      subjectList.push(exactSubject.SubjectName);
    }
    if (Array.isArray(subjects)) {
      for (const s of subjects) {
        const sName = typeof s === 'string' ? s : (s.SubjectName || s.Name || s.name || '');
        if (sName && !subjectList.includes(sName)) subjectList.push(sName);
      }
    }

    const title = inferTitle(subjectList, defaultTitle);

    tutors.push({
      first_name,
      last_name: last_name || '',
      firm_name: firmName,
      title,
      email: '',
      phone: '',
      website: profileUrl,
      domain: 'wyzant.com',
      city: tutorCity,
      state: tutorState,
      country: 'US',
      niche: 'education',
      source: 'wyzant',
      profile_url: profileUrl,
      _userId: userId,
      _rate: fee,
      _rating: rating,
      _hours: numHours,
      _college: college,
      _degree: degree,
      _subjects: subjectList,
    });
  }

  return { tutors, totalCount };
}

/**
 * Scrape tutor cards directly from the DOM (fallback if __NEXT_DATA__ is empty)
 */
async function scrapeTutorCardsFromDOM(page, defaultTitle) {
  const tutors = await page.evaluate(() => {
    const results = [];

    // Strategy 1: Look for tutor card links with /Tutors/ or /match/tutor/ patterns
    const profileLinks = document.querySelectorAll('a[href*="/Tutors/"], a[href*="/match/tutor/"]');
    const seen = new Set();

    for (const link of profileLinks) {
      const href = link.getAttribute('href') || '';
      if (seen.has(href)) continue;
      seen.add(href);

      // Get the card container (walk up to find the wrapping element)
      let card = link;
      for (let i = 0; i < 8; i++) {
        if (!card.parentElement) break;
        card = card.parentElement;
        // Stop if we find something that looks like a card container
        if (card.querySelector('a[href*="/Tutors/"], a[href*="/match/tutor/"]') !== link) {
          card = card.querySelector('a[href*="/Tutors/"], a[href*="/match/tutor/"]') === link ? card : link.parentElement;
          break;
        }
      }

      // Extract text content from the card area
      const cardText = card ? card.textContent : link.textContent;
      const linkText = link.textContent.trim();

      // Try to find name (usually the first prominent text)
      let name = '';
      // The link text itself often contains the name
      const nameMatch = linkText.match(/^([A-Z][a-z]+\s+[A-Z]\.?)/);
      if (nameMatch) {
        name = nameMatch[1];
      } else if (linkText.length < 40 && linkText.length > 2) {
        name = linkText.split('\n')[0].trim();
      }

      // Try to extract rate from card text
      let rate = '';
      const rateMatch = (cardText || '').match(/\$(\d+)\s*\/\s*h(?:ou)?r/);
      if (rateMatch) rate = rateMatch[1];

      // Try to extract location
      let location = '';
      const locMatch = (cardText || '').match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})/);
      if (locMatch) location = `${locMatch[1]}, ${locMatch[2]}`;

      if (name && name.length > 2) {
        results.push({
          name,
          href,
          rate,
          location,
        });
      }
    }

    return results;
  });

  return tutors.map(t => {
    let tutorCity = '', tutorState = '';
    if (t.location) {
      const parts = t.location.split(',').map(s => s.trim());
      tutorCity = parts[0] || '';
      tutorState = parts[1] || '';
    }

    const profileUrl = t.href.startsWith('http') ? t.href : `https://www.wyzant.com${t.href}`;
    const { first_name, last_name } = splitName(t.name);

    return {
      first_name,
      last_name: last_name || '',
      firm_name: '',
      title: defaultTitle,
      email: '',
      phone: '',
      website: profileUrl,
      domain: 'wyzant.com',
      city: tutorCity,
      state: tutorState,
      country: 'US',
      niche: 'education',
      source: 'wyzant',
      profile_url: profileUrl,
      _userId: (t.href.match(/\/(\d+)\/?$/) || [])[1] || '',
      _rate: t.rate,
    };
  });
}

/**
 * Visit a tutor profile page to extract additional info
 */
async function enrichFromProfile(page, lead) {
  if (!lead.profile_url || !lead.profile_url.includes('wyzant.com')) return lead;

  try {
    await page.goto(lead.profile_url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
    await sleep(1500 + Math.random() * 1500);

    const extra = await page.evaluate(() => {
      const data = {};

      // Try __NEXT_DATA__ first
      const nextEl = document.getElementById('__NEXT_DATA__');
      if (nextEl) {
        try {
          const nd = JSON.parse(nextEl.textContent);
          const pp = nd.props?.pageProps || {};
          const tutor = pp.tutor || pp.profile || pp.data?.tutor || pp;

          if (tutor.Subjects || tutor.subjects) {
            const subs = tutor.Subjects || tutor.subjects || [];
            data.subjects = Array.isArray(subs) ? subs.map(s => typeof s === 'string' ? s : (s.SubjectName || s.Name || s.name || '')).filter(Boolean) : [];
          }
          if (tutor.Description || tutor.description || tutor.About || tutor.about) {
            data.bio = (tutor.Description || tutor.description || tutor.About || tutor.about || '').slice(0, 500);
          }
          if (tutor.Education || tutor.education) {
            data.education = JSON.stringify(tutor.Education || tutor.education);
          }
          if (tutor.Website || tutor.website) {
            data.website = tutor.Website || tutor.website;
          }
        } catch {}
      }

      // Fallback: look for website links in the page
      if (!data.website) {
        const links = document.querySelectorAll('a[href]');
        for (const link of links) {
          const href = link.getAttribute('href') || '';
          const text = link.textContent.trim().toLowerCase();
          if ((text.includes('website') || text.includes('visit')) &&
              href.startsWith('http') && !href.includes('wyzant.com')) {
            data.website = href;
            break;
          }
        }
      }

      return data;
    });

    // Update lead with enriched data
    if (extra.website && extra.website !== lead.website) {
      lead.website = extra.website;
      lead.domain = '';
      try {
        let u = extra.website;
        if (!u.startsWith('http')) u = 'https://' + u;
        lead.domain = new URL(u).hostname.replace(/^www\./, '');
      } catch {}
    }
    if (extra.subjects && extra.subjects.length > 0) {
      lead.title = inferTitle(extra.subjects, lead.title);
      lead._subjects = extra.subjects;
    }

  } catch (err) {
    // Profile enrichment is best-effort
    log(`    Profile error for ${lead.first_name} ${lead.last_name}: ${err.message}`);
  }

  return lead;
}

// ── Main Scraping Logic ──────────────────────────────────────────────────────

async function scrapeWyzant(isTest) {
  const metros = isTest ? METROS.slice(0, 2) : METROS;
  const maxPagesPerSearch = isTest ? 1 : MAX_PAGES_PER_SEARCH;
  const enrichProfiles = !isTest; // Skip profile enrichment in test mode

  log('Launching Puppeteer with stealth...');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--disable-infobars',
      '--window-size=1920,1080',
    ],
    defaultViewport: { width: 1920, height: 1080 },
  });

  const page = await browser.newPage();

  // Set realistic headers
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
  });

  // Block images/fonts/css for speed
  await page.setRequestInterception(true);
  page.on('request', req => {
    const type = req.resourceType();
    if (['image', 'font', 'stylesheet', 'media'].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  const allLeads = [];
  const seenKeys = new Set();
  const subjectStats = {};

  // Navigate to Wyzant homepage first to establish cookies
  log('Establishing session with Wyzant homepage...');
  try {
    await page.goto('https://www.wyzant.com', { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
    await sleep(2000 + Math.random() * 2000);
  } catch (err) {
    log(`  Warning: homepage load issue: ${err.message}`);
  }

  for (const subject of SUBJECTS) {
    const subjectLeads = [];
    log(`\n${'='.repeat(60)}`);
    log(`Subject: ${subject.kw}`);
    log(`${'='.repeat(60)}`);

    for (const metro of metros) {
      log(`\n  [${subject.kw}] ${metro.city}, ${metro.state} (zip: ${metro.zip})`);

      let pageNum = 0;
      let totalForSearch = 0;
      let consecutiveEmpty = 0;

      while (pageNum < maxPagesPerSearch) {
        const searchUrl = `https://www.wyzant.com/match/search?kw=${encodeURIComponent(subject.kw)}&z=${metro.zip}&page_number=${pageNum}&page_size=${PAGE_SIZE}`;

        try {
          await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
          await sleep(1500 + Math.random() * 1500);

          // Check for Cloudflare challenge
          const pageContent = await page.content();
          if (pageContent.includes('Just a moment') || pageContent.includes('cf-browser-verification')) {
            log(`    Cloudflare challenge on page ${pageNum + 1}, waiting...`);
            await sleep(8000 + Math.random() * 5000);
            // Try to wait for challenge to resolve
            try {
              await page.waitForSelector('#__NEXT_DATA__, a[href*="/Tutors/"], a[href*="/match/tutor/"]', { timeout: 20000 });
            } catch {
              log(`    Challenge not resolved, skipping city`);
              break;
            }
          }

          // Try __NEXT_DATA__ first
          const nextData = await extractNextData(page);
          let { tutors, totalCount } = parseTutorsFromNextData(nextData, subject.title);

          if (totalCount > 0 && totalForSearch === 0) {
            totalForSearch = totalCount;
            const maxPg = Math.min(Math.ceil(totalCount / PAGE_SIZE), maxPagesPerSearch);
            log(`    Total tutors for this search: ${totalCount} (up to ${maxPg} pages)`);
          }

          // Fallback to DOM scraping if __NEXT_DATA__ is empty
          if (tutors.length === 0) {
            tutors = await scrapeTutorCardsFromDOM(page, subject.title);
          }

          if (tutors.length === 0) {
            consecutiveEmpty++;
            if (consecutiveEmpty >= 2) {
              log(`    No tutors on page ${pageNum + 1}, stopping city`);
              break;
            }
            log(`    Page ${pageNum + 1}: 0 tutors (retrying next page)`);
            pageNum++;
            await randomDelay();
            continue;
          }

          consecutiveEmpty = 0;
          let newCount = 0;

          for (const tutor of tutors) {
            const key = dedupKey(tutor);
            if (seenKeys.has(key)) continue;
            seenKeys.add(key);

            // Remove internal fields before storing
            const clean = { ...tutor };
            delete clean._userId;
            delete clean._rate;
            delete clean._rating;
            delete clean._hours;
            delete clean._college;
            delete clean._degree;
            delete clean._subjects;

            subjectLeads.push(clean);
            allLeads.push(clean);
            newCount++;
          }

          log(`    Page ${pageNum + 1}: ${tutors.length} tutors, ${newCount} new unique`);

          // Stop paginating if we've seen all
          if (totalForSearch > 0 && (pageNum + 1) * PAGE_SIZE >= totalForSearch) {
            break;
          }

        } catch (err) {
          log(`    ERROR on page ${pageNum + 1}: ${err.message}`);
          consecutiveEmpty++;
          if (consecutiveEmpty >= 3) break;
        }

        pageNum++;
        await randomDelay();
      }

      // Save intermediate progress after each city
      if (allLeads.length > 0) {
        writeCSV(allLeads, OUT_FILE);
      }

      await randomDelay();
    }

    subjectStats[subject.kw] = subjectLeads.length;
    log(`\n  >> ${subject.kw} total new: ${subjectLeads.length}`);
  }

  // Optional: enrich top leads by visiting profile pages
  if (enrichProfiles && allLeads.length > 0) {
    log(`\n${'='.repeat(60)}`);
    log(`Profile Enrichment (sampling up to 100 profiles)`);
    log(`${'='.repeat(60)}`);

    // Sample leads that have profile URLs but no external website
    const toEnrich = allLeads
      .filter(l => l.profile_url && l.domain === 'wyzant.com')
      .slice(0, 100);

    let enriched = 0;
    for (const lead of toEnrich) {
      try {
        await enrichFromProfile(page, lead);
        enriched++;
        if (enriched % 10 === 0) {
          log(`  Enriched ${enriched}/${toEnrich.length} profiles`);
          writeCSV(allLeads, OUT_FILE);
        }
      } catch (err) {
        log(`  Enrich error: ${err.message}`);
      }
      await sleep(2000 + Math.random() * 2000);
    }
    log(`  Enrichment complete: ${enriched} profiles visited`);
  }

  await browser.close();

  // Final write
  writeCSV(allLeads, OUT_FILE);

  return { allLeads, subjectStats };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const isAll = args.includes('--all');

  if (!isTest && !isAll) {
    console.log('Usage:');
    console.log('  node scripts/scrape-wyzant-college.js --test    # Test mode (2 cities, 1 page per subject)');
    console.log('  node scripts/scrape-wyzant-college.js --all     # Full scrape (50+ cities, all pages)');
    process.exit(0);
  }

  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  log('Wyzant College Admissions Tutor Scraper');
  log(`Mode: ${isTest ? 'TEST' : 'FULL'}`);
  log(`Output: ${OUT_FILE}`);
  log(`Subjects: ${SUBJECTS.map(s => s.kw).join(', ')}`);
  log(`Cities: ${isTest ? '2 (test)' : METROS.length}`);
  log('');

  const startTime = Date.now();

  try {
    const { allLeads, subjectStats } = await scrapeWyzant(isTest);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const withName = allLeads.filter(l => l.first_name && l.last_name).length;
    const withWebsite = allLeads.filter(l => l.website && l.domain !== 'wyzant.com').length;
    const cities = new Set(allLeads.map(l => `${l.city},${l.state}`));

    log('');
    log('='.repeat(60));
    log('FINAL RESULTS');
    log('='.repeat(60));
    log(`Total unique leads: ${allLeads.length}`);
    log(`With name: ${withName}`);
    log(`With external website: ${withWebsite}`);
    log(`Unique cities: ${cities.size}`);
    log(`Elapsed: ${elapsed}s`);
    log('');
    log('Subject breakdown:');
    for (const [subject, count] of Object.entries(subjectStats)) {
      log(`  ${subject}: ${count} new unique`);
    }
    log('');
    log(`Output: ${OUT_FILE}`);

  } catch (err) {
    log(`FATAL: ${err.message}`);
    console.error(err);
    process.exit(1);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
