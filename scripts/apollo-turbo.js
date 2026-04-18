#!/usr/bin/env node
/**
 * Apollo Turbo Scraper — High-speed B2B lead extraction from Apollo.io
 *
 * Bypasses Apollo's 125-result-per-query limit by segmenting searches by city.
 * 3-phase email enrichment: Apollo Reveal → Website scraping → Smart pattern generation.
 * Account rotation for maximal reveal credits across multiple free accounts.
 *
 * Usage:
 *   node scripts/apollo-turbo.js --titles "attorney" --state "Florida" --seniorities "owner"
 *   node scripts/apollo-turbo.js --titles "dentist" --state "California" --seniorities "owner,founder"
 *   node scripts/apollo-turbo.js --titles "plumber" --state "Texas" --seniorities "owner" --keywords "residential"
 *   node scripts/apollo-turbo.js --titles "solicitor" --state "UK" --seniorities "owner,partner"
 *   node scripts/apollo-turbo.js --titles "attorney" --state "Canada" --seniorities "partner"
 *   node scripts/apollo-turbo.js --titles "attorney" --state "all-us" --seniorities "owner,partner"
 *
 * Options:
 *   --titles       Job titles to search (comma-separated)
 *   --state        US state, "all-us", "UK", "Canada", or "Australia"
 *   --seniorities  Seniority levels (owner,founder,partner,c_suite,vp,director,manager)
 *   --keywords     Additional keyword filter
 *   --max          Max leads to collect
 *   --max-reveal   Max Apollo reveal credits to use
 *   --test         Test mode (1 state, 5 cities max)
 *   --skip-emails  Skip all email enrichment
 *   --skip-reveal  Skip Apollo reveal (save credits)
 *   --reveal-skip N     Skip first N Microsoft accounts for browser reveal
 *   --reveal-accounts N Max Microsoft accounts to use for browser reveal
 *   --output       Custom output file path
 *
 * First run: node scripts/apollo-login.js  (log into Apollo once)
 * Add accounts: node scripts/apollo-add-account.js  (for more reveal credits)
 */

const path = require('path');
const fs = require('fs');
const { normalizePhone } = require('../lib/normalizer');

const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
]);

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const TITLES = getArg('titles') ? getArg('titles').split(',').map(s => s.trim()) : ['attorney'];
const STATE = getArg('state') || 'Florida';
const SENIORITIES = getArg('seniorities') ? getArg('seniorities').split(',').map(s => s.trim()) : ['owner'];
const KEYWORDS = getArg('keywords') || '';
const OUTPUT = getArg('output');
const SKIP_EMAILS = hasFlag('skip-emails');
const TEST_MODE = hasFlag('test');
const MAX_LEADS = parseInt(getArg('max') || '0') || 0;
const MAX_REVEAL = parseInt(getArg('max-reveal') || '0') || 0;

// ─── US State Cities (top 50+ cities per state for segmentation) ────

const STATE_CITIES = {
  'Florida': [
    'Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Fort Lauderdale', 'St. Petersburg',
    'Hialeah', 'Tallahassee', 'Cape Coral', 'Fort Myers', 'Pembroke Pines', 'Hollywood',
    'Gainesville', 'Miramar', 'Coral Springs', 'Palm Bay', 'West Palm Beach', 'Clearwater',
    'Lakeland', 'Pompano Beach', 'Davie', 'Miami Gardens', 'Boca Raton', 'Plantation',
    'Sunrise', 'Doral', 'Deltona', 'Palm Coast', 'Melbourne', 'Boynton Beach',
    'Lauderhill', 'Weston', 'Kissimmee', 'Homestead', 'Delray Beach', 'Tamarac',
    'Daytona Beach', 'North Miami', 'Wellington', 'Jupiter', 'Ocala', 'Port St. Lucie',
    'Sarasota', 'Pensacola', 'Bradenton', 'Naples', 'Aventura', 'Key West',
  ],
  'California': [
    'Los Angeles', 'San Francisco', 'San Diego', 'San Jose', 'Sacramento', 'Fresno',
    'Long Beach', 'Oakland', 'Bakersfield', 'Anaheim', 'Santa Ana', 'Riverside',
    'Stockton', 'Irvine', 'Chula Vista', 'Fremont', 'San Bernardino', 'Modesto',
    'Fontana', 'Moreno Valley', 'Glendale', 'Huntington Beach', 'Santa Clarita',
    'Garden Grove', 'Oceanside', 'Rancho Cucamonga', 'Ontario', 'Santa Rosa',
    'Elk Grove', 'Corona', 'Lancaster', 'Palmdale', 'Salinas', 'Pomona',
    'Escondido', 'Torrance', 'Pasadena', 'Orange', 'Fullerton', 'Thousand Oaks',
    'Roseville', 'Concord', 'Simi Valley', 'Santa Maria', 'Visalia', 'Berkeley',
    'El Monte', 'Downey', 'Costa Mesa', 'Carlsbad', 'Palo Alto', 'Walnut Creek',
    'Newport Beach', 'Beverly Hills', 'Burbank', 'Redwood City',
  ],
  'New York': [
    'New York', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse', 'Albany',
    'New Rochelle', 'Mount Vernon', 'Schenectady', 'Utica', 'White Plains',
    'Troy', 'Niagara Falls', 'Binghamton', 'Freeport', 'Valley Stream',
    'Long Beach', 'Ithaca', 'Poughkeepsie', 'Saratoga Springs', 'Garden City',
    'Mineola', 'Hempstead', 'Huntington', 'Riverhead', 'Brooklyn', 'Queens',
    'Bronx', 'Staten Island', 'Manhattan', 'Westchester',
  ],
  'Texas': [
    'Houston', 'San Antonio', 'Dallas', 'Austin', 'Fort Worth', 'El Paso',
    'Arlington', 'Corpus Christi', 'Plano', 'Laredo', 'Lubbock', 'Garland',
    'Irving', 'Amarillo', 'Grand Prairie', 'Brownsville', 'McKinney', 'Frisco',
    'Pasadena', 'Mesquite', 'Killeen', 'McAllen', 'Waco', 'Midland',
    'Beaumont', 'Round Rock', 'Odessa', 'Abilene', 'Pearland', 'Richardson',
    'Sugar Land', 'Tyler', 'College Station', 'League City', 'Allen', 'Edinburg',
    'San Marcos', 'Temple', 'Flower Mound', 'The Woodlands',
  ],
  'Illinois': [
    'Chicago', 'Aurora', 'Joliet', 'Naperville', 'Rockford', 'Springfield',
    'Elgin', 'Peoria', 'Champaign', 'Waukegan', 'Cicero', 'Bloomington',
    'Arlington Heights', 'Evanston', 'Schaumburg', 'Decatur', 'Bolingbrook',
    'Palatine', 'Skokie', 'Oak Lawn', 'Berwyn', 'Des Plaines', 'Tinley Park',
    'Orland Park', 'Oak Park', 'Downers Grove', 'Wheaton',
  ],
  'Pennsylvania': [
    'Philadelphia', 'Pittsburgh', 'Allentown', 'Erie', 'Reading', 'Scranton',
    'Bethlehem', 'Lancaster', 'Harrisburg', 'York', 'Wilkes-Barre', 'Chester',
    'State College', 'Easton', 'Norristown', 'Pottstown', 'West Chester',
    'King of Prussia', 'Doylestown', 'Media', 'Wayne', 'Conshohocken',
  ],
  'Georgia': [
    'Atlanta', 'Augusta', 'Columbus', 'Savannah', 'Athens', 'Sandy Springs',
    'Macon', 'Roswell', 'Johns Creek', 'Albany', 'Warner Robins', 'Alpharetta',
    'Marietta', 'Stonecrest', 'Smyrna', 'Valdosta', 'Brookhaven', 'Dunwoody',
    'Peachtree City', 'Newnan', 'Gainesville', 'Duluth', 'Kennesaw', 'Lawrenceville',
  ],
  'Ohio': [
    'Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron', 'Dayton',
    'Parma', 'Canton', 'Youngstown', 'Lorain', 'Hamilton', 'Springfield',
    'Kettering', 'Elyria', 'Lakewood', 'Cuyahoga Falls', 'Dublin', 'Westerville',
    'Mason', 'Fairfield', 'Strongsville', 'Mentor', 'Boardman', 'Hudson',
  ],
  'North Carolina': [
    'Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem', 'Fayetteville',
    'Cary', 'Wilmington', 'High Point', 'Concord', 'Asheville', 'Greenville',
    'Gastonia', 'Jacksonville', 'Chapel Hill', 'Huntersville', 'Apex', 'Mooresville',
    'Hickory', 'Kannapolis', 'Wake Forest', 'Indian Trail',
  ],
  'Michigan': [
    'Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Ann Arbor',
    'Lansing', 'Flint', 'Dearborn', 'Livonia', 'Troy', 'Westland',
    'Kalamazoo', 'Canton', 'Southfield', 'Farmington Hills', 'Rochester Hills',
    'Novi', 'Pontiac', 'Royal Oak', 'St. Clair Shores', 'Midland',
  ],
  'Arizona': [
    'Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Scottsdale', 'Glendale',
    'Gilbert', 'Tempe', 'Peoria', 'Surprise', 'Goodyear', 'Avondale',
    'Flagstaff', 'Yuma', 'Lake Havasu City', 'Prescott', 'Casa Grande',
  ],
  'Colorado': [
    'Denver', 'Colorado Springs', 'Aurora', 'Fort Collins', 'Lakewood',
    'Thornton', 'Arvada', 'Westminster', 'Pueblo', 'Boulder', 'Greeley',
    'Longmont', 'Loveland', 'Broomfield', 'Castle Rock', 'Parker',
  ],
  'Virginia': [
    'Virginia Beach', 'Norfolk', 'Chesapeake', 'Richmond', 'Newport News',
    'Alexandria', 'Hampton', 'Roanoke', 'Portsmouth', 'Lynchburg',
    'Manassas', 'Fredericksburg', 'Charlottesville', 'Fairfax', 'Arlington',
  ],
  'Washington': [
    'Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue', 'Kent',
    'Everett', 'Renton', 'Federal Way', 'Yakima', 'Bellingham', 'Kirkland',
    'Kennewick', 'Redmond', 'Olympia', 'Lakewood', 'Issaquah',
  ],
  'Massachusetts': [
    'Boston', 'Worcester', 'Springfield', 'Cambridge', 'Lowell', 'Brockton',
    'New Bedford', 'Quincy', 'Lynn', 'Newton', 'Somerville', 'Lawrence',
    'Framingham', 'Brookline', 'Plymouth', 'Medford', 'Waltham',
  ],
  'Maryland': [
    'Baltimore', 'Columbia', 'Germantown', 'Silver Spring', 'Waldorf',
    'Frederick', 'Ellicott City', 'Glen Burnie', 'Bethesda', 'Rockville',
    'Bowie', 'Hagerstown', 'Annapolis', 'College Park', 'Towson',
  ],
  'Indiana': [
    'Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend', 'Carmel',
    'Fishers', 'Bloomington', 'Hammond', 'Gary', 'Lafayette',
    'Muncie', 'Terre Haute', 'Noblesville', 'Greenwood', 'Anderson',
  ],
  'Tennessee': [
    'Nashville', 'Memphis', 'Knoxville', 'Chattanooga', 'Clarksville',
    'Murfreesboro', 'Franklin', 'Jackson', 'Johnson City', 'Bartlett',
    'Hendersonville', 'Kingsport', 'Smyrna', 'Brentwood', 'Germantown',
  ],
  'Missouri': [
    'Kansas City', 'St. Louis', 'Springfield', 'Columbia', 'Independence',
    'Lee\'s Summit', 'O\'Fallon', 'St. Joseph', 'St. Charles', 'Blue Springs',
    'Joplin', 'Jefferson City', 'Florissant', 'Chesterfield', 'Ballwin',
  ],
  'Wisconsin': [
    'Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine', 'Appleton',
    'Waukesha', 'Oshkosh', 'Eau Claire', 'Janesville', 'West Allis',
    'La Crosse', 'Sheboygan', 'Wauwatosa', 'Fond du Lac', 'Brookfield',
  ],
  'Minnesota': [
    'Minneapolis', 'St. Paul', 'Rochester', 'Duluth', 'Bloomington',
    'Brooklyn Park', 'Plymouth', 'Woodbury', 'Lakeville', 'Maple Grove',
    'Eagan', 'St. Cloud', 'Eden Prairie', 'Burnsville', 'Minnetonka',
  ],
  'Oregon': [
    'Portland', 'Salem', 'Eugene', 'Gresham', 'Hillsboro', 'Beaverton',
    'Bend', 'Medford', 'Springfield', 'Corvallis', 'Albany', 'Tigard',
    'Lake Oswego', 'Tualatin', 'Oregon City', 'Grants Pass',
  ],
  'Connecticut': [
    'Bridgeport', 'New Haven', 'Hartford', 'Stamford', 'Waterbury',
    'Norwalk', 'Danbury', 'New Britain', 'Greenwich', 'Fairfield',
    'Hamden', 'Meriden', 'West Hartford', 'Milford', 'Stratford',
  ],
  'South Carolina': [
    'Charleston', 'Columbia', 'North Charleston', 'Greenville', 'Rock Hill',
    'Mount Pleasant', 'Spartanburg', 'Summerville', 'Myrtle Beach', 'Florence',
    'Hilton Head Island', 'Bluffton', 'Aiken', 'Anderson', 'Clemson',
  ],
  'Nevada': [
    'Las Vegas', 'Henderson', 'Reno', 'North Las Vegas', 'Sparks',
    'Carson City', 'Fernley', 'Elko', 'Mesquite', 'Boulder City',
  ],
  'Louisiana': [
    'New Orleans', 'Baton Rouge', 'Shreveport', 'Lafayette', 'Lake Charles',
    'Kenner', 'Bossier City', 'Monroe', 'Alexandria', 'Houma',
    'Metairie', 'Mandeville', 'Slidell', 'Covington', 'Hammond',
  ],
  'Alabama': [
    'Birmingham', 'Montgomery', 'Huntsville', 'Mobile', 'Tuscaloosa',
    'Hoover', 'Dothan', 'Decatur', 'Auburn', 'Madison',
    'Florence', 'Gadsden', 'Vestavia Hills', 'Prattville', 'Phenix City',
  ],
  'New Jersey': [
    'Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Edison',
    'Woodbridge', 'Lakewood', 'Toms River', 'Hamilton', 'Trenton',
    'Clifton', 'Camden', 'Passaic', 'Cherry Hill', 'Hackensack',
    'Morristown', 'Princeton', 'Hoboken', 'New Brunswick', 'Wayne',
  ],
};

// UK, Canada, Australia city lists
const INTL_CITIES = {
  'United Kingdom': [
    'London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 'Liverpool',
    'Bristol', 'Edinburgh', 'Sheffield', 'Newcastle', 'Nottingham', 'Cardiff',
    'Leicester', 'Belfast', 'Brighton', 'Southampton', 'Cambridge', 'Oxford',
    'York', 'Reading', 'Exeter', 'Bath', 'Aberdeen', 'Dundee',
  ],
  'Canada': [
    'Toronto', 'Montreal', 'Vancouver', 'Calgary', 'Edmonton', 'Ottawa',
    'Winnipeg', 'Quebec City', 'Hamilton', 'Kitchener', 'London', 'Victoria',
    'Halifax', 'Saskatoon', 'Regina', 'St. John\'s', 'Kelowna', 'Barrie',
  ],
  'Australia': [
    'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Gold Coast',
    'Newcastle', 'Canberra', 'Hobart', 'Darwin', 'Cairns', 'Townsville',
    'Wollongong', 'Geelong', 'Toowoomba', 'Ballarat',
  ],
};

// All 50 US states for --state all-us
const ALL_US_STATES = [
  'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
  'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
  'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
  'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
  'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
  'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
  'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
  'Wisconsin', 'Wyoming',
];

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  // Load .env
  try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }); } catch {}

  // Load API keys (comma-separated for multi-account rotation)
  const API_KEYS = (process.env.APOLLO_API_KEYS || process.env.APOLLO_API_KEY || '').split(',').map(k => k.trim()).filter(Boolean);

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║       MORTAR — Apollo Turbo Scraper                      ║');
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Titles:       ${TITLES.join(', ')}`);
  console.log(`  State:        ${STATE}`);
  console.log(`  Seniorities:  ${SENIORITIES.join(', ')}`);
  if (KEYWORDS) console.log(`  Keywords:     ${KEYWORDS}`);
  console.log(`  API keys:     ${API_KEYS.length} account(s) loaded`);
  console.log(`  Test mode:    ${TEST_MODE ? 'YES (1 state, max 250 leads)' : 'NO'}`);
  console.log(`  Skip emails:  ${SKIP_EMAILS ? 'YES' : 'NO'}`);
  console.log('');

  // Launch browser
  const puppeteer = require('puppeteer-extra');
  const StealthPlugin = require('puppeteer-extra-plugin-stealth');
  puppeteer.use(StealthPlugin());

  console.log('  Launching Chrome...');
  let browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    userDataDir: '/tmp/apollo-session-profile',
    args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check', '--disable-extensions', '--window-size=1280,900'],
    defaultViewport: null,
  });

  let page = (await browser.pages())[0] || await browser.newPage();

  console.log('  Loading Apollo...');
  await page.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 90000 });
  await sleep(3000);

  if (page.url().includes('/login')) {
    console.error('\n  Not logged in! Run first: node scripts/apollo-login.js\n');
    await browser.close();
    process.exit(1);
  }
  console.log('  Logged in!\n');

  // ─── Build search segments ─────────────────────────────────────

  const isIntl = ['united kingdom', 'uk', 'canada', 'australia'].includes(STATE.toLowerCase());
  const normalizedState = STATE.toLowerCase() === 'uk' ? 'United Kingdom' : STATE;
  const states = STATE.toLowerCase() === 'all-us' ? ALL_US_STATES : [normalizedState];
  if (TEST_MODE && states.length > 1) states.length = 1;

  const leads = [];
  const seenIds = new Set();
  const seenEmails = new Set();
  let totalAvailable = 0;

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  STEP 1: Apollo People Search');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  for (const state of states) {
    if (MAX_LEADS > 0 && leads.length >= MAX_LEADS) break;

    // First: state/country-level query (gets first 125 results)
    const stateLocation = isIntl ? state : `${state}, US`;
    console.log(`  ── ${state} ──`);

    const stateResult = await searchAllPages(page, {
      person_titles: TITLES,
      person_locations: [stateLocation],
      person_seniorities: SENIORITIES,
      ...(KEYWORDS ? { q_keywords: KEYWORDS } : {}),
    }, seenIds);

    const stateBefore = leads.length;
    for (const p of stateResult.people) {
      leads.push(p);
      if (p.apollo_id) seenIds.add(p.apollo_id);
    }
    totalAvailable += stateResult.totalAvailable;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = Math.round(leads.length / (elapsed / 60));
    console.log(`  State-level: +${stateResult.people.length} leads (${leads.length} total | ${rate}/min | ${stateResult.totalAvailable} available)`);

    // If there are more results than we got, segment by city
    if (stateResult.totalAvailable > stateResult.people.length && !(MAX_LEADS > 0 && leads.length >= MAX_LEADS)) {
      const cities = STATE_CITIES[state] || INTL_CITIES[state] || [];
      if (cities.length > 0) {
        const testCities = TEST_MODE ? cities.slice(0, 5) : cities;
        console.log(`  Drilling into ${testCities.length} cities...`);

        // Process cities sequentially but with minimal delay
        for (const city of testCities) {
          if (MAX_LEADS > 0 && leads.length >= MAX_LEADS) break;

          const cityLocation = isIntl ? `${city}, ${state}` : `${city}, ${state}, US`;
          const cityResult = await searchAllPages(page, {
            person_titles: TITLES,
            person_locations: [cityLocation],
            person_seniorities: SENIORITIES,
            ...(KEYWORDS ? { q_keywords: KEYWORDS } : {}),
          }, seenIds);

          for (const p of cityResult.people) {
            leads.push(p);
            if (p.apollo_id) seenIds.add(p.apollo_id);
          }

          if (cityResult.people.length > 0) {
            const e2 = ((Date.now() - startTime) / 1000).toFixed(0);
            const r2 = Math.round(leads.length / (e2 / 60));
            console.log(`    ${city}: +${cityResult.people.length} new (${leads.length} total | ${r2}/min)`);
          }

          await sleep(800); // 0.8s delay between city queries (prevents Cloudflare)
        }
      }
    }
  }

  const searchElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const searchRate = Math.round(leads.length / (searchElapsed / 60));
  console.log('');
  console.log(`  Search complete: ${leads.length} unique leads in ${searchElapsed}s (${searchRate}/min)`);
  console.log(`  Total available in Apollo: ${totalAvailable}`);

  if (leads.length === 0) {
    await browser.close();
    console.error('  No leads found. Try different filters.');
    process.exit(1);
  }

  // ─── Email Enrichment ──────────────────────────────────────────

  if (!SKIP_EMAILS) {
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  STEP 2: Email Enrichment');
    console.log('═══════════════════════════════════════════════════════════');

    // Phase A: Apollo People Match API — returns verified emails directly
    // Uses /api/v1/people/match with name+domain. Free tier: 10,000 credits/month.
    // This is how Apify and other scrapers get verified emails cheaply.
    console.log('');
    console.log('  Phase A: Apollo People Match API (verified emails via API)');

    const needsEmail = leads.filter(l => !l.email && l.first_name && l.domain && l.last_name && l.last_name.length > 2);
    const matchLimit = MAX_REVEAL > 0 ? MAX_REVEAL : needsEmail.length;

    if (needsEmail.length > 0 && !hasFlag('skip-reveal') && API_KEYS.length > 0) {
      let apiFound = 0;
      let apiErrors = 0;
      let keyIdx = 0;
      const CONCURRENCY = 5;
      const enrichStart = Date.now();

      console.log(`  ${needsEmail.length} leads need emails | ${API_KEYS.length} API key(s) | limit: ${matchLimit}`);

      let i = 0;
      while (i < Math.min(needsEmail.length, matchLimit) && keyIdx < API_KEYS.length) {
        const currentKey = API_KEYS[keyIdx];
        const keyLabel = API_KEYS.length > 1 ? ` [key ${keyIdx + 1}/${API_KEYS.length}]` : '';
        let creditsExhausted = false;

        while (i < Math.min(needsEmail.length, matchLimit) && !creditsExhausted) {
          const batch = needsEmail.slice(i, i + CONCURRENCY).filter(l => !l.email);
          if (batch.length === 0) { i += CONCURRENCY; continue; }

          const promises = batch.map(lead => apolloMatch(currentKey, lead.first_name, lead.last_name, lead.domain));
          const results = await Promise.all(promises);

          for (let j = 0; j < results.length; j++) {
            const r = results[j];
            const lead = batch[j];

            if (r.error) {
              if (r.error.includes('insufficient credits') || r.error.includes('Upgrade your plan')) {
                creditsExhausted = true;
                break;
              }
              apiErrors++;
              continue;
            }

            if (r.email) {
              lead.email = r.email;
              lead.email_status = r.status || 'verified';
              lead.email_source = 'apollo_api';
              lead.email_confidence = r.status === 'verified' ? 'verified' : 'likely';
              apiFound++;
            }
          }

          i += batch.length;

          if (i % 50 < CONCURRENCY || creditsExhausted) {
            const elapsed = ((Date.now() - enrichStart) / 1000).toFixed(1);
            const rate = elapsed > 0 ? Math.round(i / (elapsed / 60)) : 0;
            console.log(`  ${keyLabel} [${i}/${Math.min(needsEmail.length, matchLimit)}] ${apiFound} emails | ${rate}/min`);
          }

          if (!creditsExhausted) await sleep(400);
        }

        if (creditsExhausted) {
          keyIdx++;
          if (keyIdx < API_KEYS.length) {
            console.log(`  Key ${keyIdx} credits exhausted → switching to key ${keyIdx + 1}...`);
          } else {
            console.log(`  All ${API_KEYS.length} API key(s) exhausted at lead ${i}`);
          }
        }
      }

      const enrichElapsed = ((Date.now() - enrichStart) / 1000).toFixed(1);
      console.log(`  Apollo API: ${apiFound} verified emails in ${enrichElapsed}s (${apiErrors} errors)`);
    } else if (API_KEYS.length === 0) {
      console.log('  No API keys configured — add APOLLO_API_KEYS to .env');
    } else {
      console.log('  Skipping (all leads have emails or --skip-reveal set)');
    }

    // Phase A2: Browser-based email reveal (uses UI credits from Microsoft accounts)
    // Each new account gets 75 free reveals. Auto-rotates when credits exhaust.
    console.log('');
    console.log('  Phase A2: Browser reveal (UI credits from Microsoft accounts)');

    const CREDS_CSV_PATH = '/Users/fardeenchoudhury/Downloads/smartlead_domains (6).csv';
    const needsBrowserReveal = leads.filter(l => !l.email && l.apollo_id);
    let browserRevealTotal = 0;

    if (needsBrowserReveal.length > 0 && !hasFlag('skip-reveal') && fs.existsSync(CREDS_CSV_PATH)) {
      // Parse Microsoft account credentials
      const credsContent = fs.readFileSync(CREDS_CSV_PATH, 'utf8');
      const credsLines = credsContent.trim().split('\n');
      let msAccounts = [];
      for (let ci = 1; ci < credsLines.length; ci++) {
        const m = credsLines[ci].match(/^"([^"]+)","([^"]+)","([^"]+)"$/);
        if (m) msAccounts.push({ name: m[1], email: m[2], password: m[3] });
      }

      // Apply skip/limit from env or args
      const revealSkip = parseInt(getArg('reveal-skip') || '0') || 0;
      const revealMaxAccounts = parseInt(getArg('reveal-accounts') || '0') || 0;
      if (revealSkip > 0) msAccounts = msAccounts.slice(revealSkip);
      if (revealMaxAccounts > 0) msAccounts = msAccounts.slice(0, revealMaxAccounts);

      console.log(`  ${needsBrowserReveal.length} leads need emails | ${msAccounts.length} Microsoft accounts available`);

      // Close main browser before launching reveal browsers
      await browser.close();
      let browserClosed = true;

      for (let accIdx = 0; accIdx < msAccounts.length; accIdx++) {
        const remaining = leads.filter(l => !l.email && l.apollo_id);
        if (remaining.length === 0) break;

        const acc = msAccounts[accIdx];
        console.log(`  Account ${accIdx + 1}/${msAccounts.length}: ${acc.email}`);

        let revBrowser;
        try {
          revBrowser = await puppeteer.launch({
            headless: false,
            executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            args: ['--no-sandbox', '--no-first-run', '--no-default-browser-check',
                   '--disable-extensions', '--window-size=1280,900'],
            defaultViewport: null,
            protocolTimeout: 60000,
          });

          const revPage = (await revBrowser.pages())[0] || await revBrowser.newPage();

          // ── Microsoft OAuth login ──
          console.log('    Signing in via Microsoft...');
          await revPage.goto('https://www.apollo.io/sign-up', { waitUntil: 'networkidle2', timeout: 30000 });
          await sleep(3000);

          // Click "Sign Up with Microsoft"
          const msBtn = await revPage.evaluateHandle(() => {
            const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
            return btns.find(b => b.textContent.toLowerCase().includes('microsoft'));
          });
          if (!msBtn || !(await msBtn.asElement())) {
            console.log('    No Microsoft button — skipping');
            await revBrowser.close();
            continue;
          }
          await msBtn.click();
          await sleep(5000);

          // Enter email
          try { await revPage.waitForSelector('input[type="email"]', { timeout: 15000 }); } catch {}
          const emailInput = await revPage.$('input[type="email"]');
          if (emailInput) {
            await emailInput.click({ clickCount: 3 });
            await emailInput.type(acc.email, { delay: 30 });
            await sleep(500);
            const nextBtn = await revPage.$('input[type="submit"]') || await revPage.$('#idSIButton9');
            if (nextBtn) await nextBtn.click();
            await sleep(4000);
          }

          // Enter password
          try { await revPage.waitForSelector('input[type="password"]:not([style*="display: none"])', { timeout: 10000 }); } catch {}
          const pwInput = await revPage.$('input[type="password"]');
          if (pwInput) {
            await pwInput.click({ clickCount: 3 });
            await pwInput.type(acc.password, { delay: 30 });
            await sleep(500);
            const signInBtn = await revPage.$('input[type="submit"]') || await revPage.$('#idSIButton9');
            if (signInBtn) await signInBtn.click();
            await sleep(5000);
          }

          // Wait for Apollo redirect + handle Microsoft prompts
          let landedOnApollo = false;
          for (let wi = 0; wi < 30; wi++) {
            await sleep(2000);
            let url;
            try { url = revPage.url(); } catch { continue; }

            // Login page → click Login with Microsoft
            if (url.includes('apollo.io') && url.includes('/login')) {
              const msLoginBtn = await revPage.evaluateHandle(() => {
                const btns = [...document.querySelectorAll('button, a, div[role="button"]')];
                return btns.find(b => b.textContent.toLowerCase().includes('microsoft'));
              });
              if (msLoginBtn && (await msLoginBtn.asElement())) {
                await msLoginBtn.click();
                await sleep(5000);
                continue;
              }
            }

            const isApollo = url.includes('app.apollo.io');
            const isAuth = url.includes('/sign-up') || url.includes('/login') || url.includes('#/signup');
            if (isApollo && !isAuth) { landedOnApollo = true; break; }

            // Click through Microsoft prompts
            try {
              const submitBtn = await revPage.$('#idSIButton9');
              if (submitBtn) { await submitBtn.click(); await sleep(2000); }
              const acceptBtn = await revPage.$('input[value="Accept"], button#idBtn_Accept');
              if (acceptBtn) { await acceptBtn.click(); await sleep(3000); }
            } catch {}
          }

          if (!landedOnApollo) {
            console.log('    Failed to reach Apollo — skipping');
            await revBrowser.close();
            continue;
          }

          // Handle onboarding quickly
          for (let oa = 0; oa < 15; oa++) {
            const oUrl = revPage.url();
            const isReal = oUrl.includes('#/home') || oUrl.includes('#/people') || oUrl.includes('#/settings');
            if (isReal) break;
            if (oUrl.includes('signup-success')) { await sleep(3000); continue; }

            // Fill form fields
            const inputs = await revPage.$$('input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]):not([type="submit"])');
            for (const inp of inputs) {
              const info = await revPage.evaluate(el => ({
                value: el.value, placeholder: el.placeholder || '', visible: el.offsetParent !== null,
                parentText: el.parentElement?.textContent?.trim().slice(0, 80) || '',
              }), inp);
              if (!info.visible || (info.value && info.value.trim())) continue;
              const ctx = (info.placeholder + ' ' + info.parentText).toLowerCase();
              let fv = '';
              if (ctx.includes('full name') || ctx.includes('your name')) fv = acc.name;
              else if (ctx.includes('company') || ctx.includes('acme corp')) fv = 'Mortar Metrics';
              else if (ctx.includes('website') || ctx.includes('acme.com')) fv = 'mortarmetrics.com';
              if (fv) { await inp.click({ clickCount: 3 }); await inp.type(fv, { delay: 20 }); await sleep(300); }
            }

            // Pill selection
            await revPage.evaluate(() => {
              const pills = [...document.querySelectorAll('button, div[role="button"]')].filter(el => {
                const t = el.textContent.trim().toLowerCase();
                const r = el.getBoundingClientRect();
                return r.width > 50 && r.width < 400 && r.height > 20 && r.height < 80 && r.top > 200 &&
                       !t.includes('continue') && !t.includes('skip') && !t.includes('back') && !t.includes('sign') &&
                       t.length > 1 && t.length < 40 && t !== 'join' && !t.includes('join') && !t.includes('chrome') &&
                       !t.includes('extension') && !t.includes('install') && !t.includes('upgrade');
              });
              const pref = pills.find(p => {
                const t = p.textContent.trim().toLowerCase();
                return t.includes('founder') || t.includes('marketing') || t.includes('create new') || t.includes('create workspace');
              });
              (pref || pills[0])?.click();
            });

            // Create workspace instead of join
            await revPage.evaluate(() => {
              const links = [...document.querySelectorAll('a, button, span')];
              const own = links.find(l => {
                const t = l.textContent.trim().toLowerCase();
                return t.includes('create new') || t.includes('create my own') || t.includes('create a new') || t.includes('create workspace');
              });
              own?.click();
            });

            // Click skip/continue
            const actBtn = await revPage.evaluateHandle(() => {
              const btns = [...document.querySelectorAll('button, a, span')];
              const skip = btns.find(b => {
                const t = b.textContent.toLowerCase().trim();
                return t === 'skip' || t === 'skip for now' || t.includes("setup on my own") || t.includes("set up on my own") ||
                       t === 'go to homepage' || t === 'start searching';
              });
              if (skip) return skip;
              return btns.find(b => {
                const t = b.textContent.toLowerCase().trim();
                return t === 'continue' || t === 'next' || t === 'get started' || t.includes('continue to') || t === 'start searching';
              });
            });
            if (actBtn && (await actBtn.asElement())) { await actBtn.click(); await sleep(2500); }
            else { await sleep(2000); }
          }

          // Force to people page
          if (!revPage.url().includes('#/people') && !revPage.url().includes('#/home')) {
            await revPage.goto('https://app.apollo.io/#/people', { waitUntil: 'networkidle2', timeout: 30000 });
            await sleep(3000);
          }

          console.log('    Logged in! Starting reveals...');

          // ── Reveal emails ──
          let accReveals = 0;
          let accErrors = 0;
          let creditsOut = false;
          const accStart = Date.now();

          for (const lead of remaining) {
            if (lead.email || !lead.apollo_id) continue;
            if (creditsOut) break;

            const result = await revPage.evaluate((personId) => {
              return new Promise(resolve => {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', '/api/v1/mixed_people/add_to_my_prospects');
                xhr.setRequestHeader('Content-Type', 'application/json');
                xhr.withCredentials = true;
                xhr.timeout = 15000;
                xhr.onload = () => {
                  try {
                    const data = JSON.parse(xhr.responseText);
                    if (xhr.status === 422) {
                      const msg = (data.message || data.error || JSON.stringify(data)).toLowerCase();
                      if (msg.includes('credit') || msg.includes('limit') || msg.includes('upgrade')) {
                        resolve({ credits_exhausted: true });
                        return;
                      }
                      resolve({ error: msg });
                      return;
                    }
                    if (data.contacts && data.contacts.length > 0) {
                      const c = data.contacts[0];
                      resolve({
                        email: (c.email && !c.email.includes('not_unlocked')) ? c.email : '',
                        phone: c.phone_numbers?.[0]?.sanitized_number || c.sanitized_phone || '',
                        status: c.email_status || '',
                      });
                      return;
                    }
                    if (data.person) {
                      const p = data.person;
                      resolve({
                        email: (p.email && !p.email.includes('not_unlocked')) ? p.email : '',
                        status: p.email_status || '',
                      });
                      return;
                    }
                    resolve({ error: 'no_contact' });
                  } catch (e) { resolve({ error: 'parse_error' }); }
                };
                xhr.onerror = () => resolve({ error: 'network' });
                xhr.ontimeout = () => resolve({ error: 'timeout' });
                xhr.send(JSON.stringify({ entity_ids: [personId], skip_in_crm: true }));
              });
            }, lead.apollo_id);

            if (result.credits_exhausted) {
              console.log(`    Credits exhausted after ${accReveals} reveals`);
              creditsOut = true;
              break;
            }

            if (result.error) { accErrors++; continue; }

            if (result.email) {
              lead.email = result.email;
              lead.email_status = result.status || 'verified';
              lead.email_source = 'apollo_browser_reveal';
              lead.email_confidence = result.status === 'verified' ? 'verified' : 'likely';
              accReveals++;
              browserRevealTotal++;
              if (result.phone && !lead.phone) lead.phone = result.phone;
            } else {
              accErrors++;
            }

            if ((accReveals + accErrors) % 10 === 0) {
              const e = ((Date.now() - accStart) / 1000).toFixed(1);
              console.log(`    [${accReveals + accErrors}] ${accReveals} emails | ${e}s`);
            }

            await sleep(500);
          }

          const accElapsed = ((Date.now() - accStart) / 1000).toFixed(1);
          console.log(`    Account done: ${accReveals} emails in ${accElapsed}s (${accErrors} errors)`);

          await revBrowser.close();

          if (accIdx < msAccounts.length - 1) await sleep(3000);

        } catch (e) {
          console.log(`    Error: ${e.message.slice(0, 100)}`);
          if (revBrowser) try { await revBrowser.close(); } catch {}
        }
      }

      console.log(`  Browser reveal total: ${browserRevealTotal} verified emails`);
    } else if (!fs.existsSync(CREDS_CSV_PATH)) {
      console.log(`  No Microsoft accounts CSV found at ${CREDS_CSV_PATH}`);
      await browser.close();
    } else {
      console.log('  No leads need browser reveal (all have emails or no apollo_ids)');
      await browser.close();
    }

    // Close browser if not already closed
    try { await browser.close(); } catch {}

    // Phase B: HTTP website scraping for remaining leads without email
    console.log('');
    console.log('  Phase B: Website email scraping (HTTP fallback)');

    const domainPatterns = new Map();
    const domainsNeedEmail = leads.filter(l => l.domain && !l.email);

    if (domainsNeedEmail.length > 0) {
      try {
        const { scrapeEmailsBatch } = require('../lib/fast-email-scraper');
        const uniqueDomains = [...new Set(domainsNeedEmail.map(l => l.domain))];
        console.log(`  Scraping ${uniqueDomains.length} unique domains...`);
        const enrichStart = Date.now();
        const emailMap = await scrapeEmailsBatch(domainsNeedEmail, 20, 4);

        let found = 0;
        // Detect domain patterns from all leads (including API-enriched ones)
        for (const lead of leads) {
          if (!lead.email || !lead.domain) continue;
          if (domainPatterns.has(lead.domain)) continue;
          const f = (lead.first_name || '').toLowerCase().replace(/[^a-z]/g, '');
          const l = (lead.last_name || '').toLowerCase().replace(/[^a-z]/g, '');
          if (!f || !l || l.length <= 2) continue;
          const local = lead.email.split('@')[0].toLowerCase();
          if (local === `${f}.${l}`) domainPatterns.set(lead.domain, 'first.last');
          else if (local === `${f}${l}`) domainPatterns.set(lead.domain, 'firstlast');
          else if (local === `${f[0]}${l}`) domainPatterns.set(lead.domain, 'flast');
          else if (local === `${f[0]}.${l}`) domainPatterns.set(lead.domain, 'f.last');
          else if (local === `${f}_${l}`) domainPatterns.set(lead.domain, 'first_last');
          else if (local === `${l}.${f}`) domainPatterns.set(lead.domain, 'last.first');
        }

        // Match website emails to leads
        for (const [domain, emails] of emailMap) {
          const domainLeads = leads.filter(l => l.domain === domain && !l.email);
          if (domainLeads.length === 0) continue;

          for (const email of emails) {
            const local = email.split('@')[0].toLowerCase();
            for (const lead of domainLeads) {
              if (lead.email) continue;
              const f = (lead.first_name || '').toLowerCase().replace(/[^a-z]/g, '');
              const l = (lead.last_name || '').toLowerCase().replace(/[^a-z]/g, '');
              if (!f || !l || l.length <= 2) continue;
              if ((local === `${f}.${l}` || local === `${f}${l}` || local === `${f[0]}${l}` ||
                  local === `${f}_${l}` || local === `${f[0]}.${l}` || local === `${l}.${f}`) && isValidEmail(email)) {
                lead.email = email;
                lead.email_source = 'website_name_match';
                found++;
                break;
              }
            }
          }

          for (const lead of domainLeads) {
            if (lead.email) continue;
            const valid = emails.filter(e => e.endsWith('@' + domain) && isValidEmail(e));
            if (valid.length > 0) { lead.email = valid[0]; lead.email_source = 'website_domain'; found++; }
          }
        }

        const enrichElapsed = ((Date.now() - enrichStart) / 1000).toFixed(1);
        console.log(`  Website scrape: ${found} emails found in ${enrichElapsed}s | ${domainPatterns.size} patterns detected`);
      } catch (e) {
        console.log(`  Website scrape skipped: ${e.message}`);
      }
    } else {
      console.log('  No more domains to scrape');
    }

    // Phase C: Email Waterfall — cascade through multiple email providers
    // Hunter (50 free/mo) → Prospeo (75 free/mo) → Snov (50 free/mo) → paid APIs → SMTP
    console.log('');
    console.log('  Phase C: Email Waterfall (multi-provider cascade)');

    const { EmailWaterfall } = require('../lib/email-waterfall');
    const waterfall = new EmailWaterfall({
      hunter_api_key: process.env.HUNTER_API_KEY,
      prospeo_api_key: process.env.PROSPEO_API_KEY,
      snov_client_id: process.env.SNOV_CLIENT_ID,
      snov_client_secret: process.env.SNOV_CLIENT_SECRET,
      icypeas_api_key: process.env.ICYPEAS_API_KEY,
      icypeas_api_secret: process.env.ICYPEAS_API_SECRET,
      leadmagic_api_key: process.env.LEADMAGIC_API_KEY,
    });

    const needsWaterfall = leads.filter(l =>
      !l.email && l.first_name && l.last_name && l.domain &&
      l.last_name.length > 2 && !FREE_PROVIDERS.has((l.domain || '').toLowerCase())
    );

    const activeProviders = waterfall.providers.map(p => p.name);
    console.log(`  ${needsWaterfall.length} leads need emails | Providers: ${activeProviders.join(' → ')}`);

    if (needsWaterfall.length > 0) {
      const wfStart = Date.now();
      let lastLog = 0;

      const wfResults = await waterfall.findEmailsBatch(needsWaterfall, 5, (found, total, lead) => {
        const now = Date.now();
        if (now - lastLog > 5000 || total === needsWaterfall.length) {
          const elapsed = ((now - wfStart) / 1000).toFixed(1);
          const rate = elapsed > 0 ? Math.round(total / (elapsed / 60)) : 0;
          console.log(`  [${total}/${needsWaterfall.length}] ${found} emails found | ${rate}/min | ${elapsed}s`);
          lastLog = now;
        }
      });

      // Apply results to leads
      for (let wi = 0; wi < needsWaterfall.length; wi++) {
        const r = wfResults[wi];
        if (r && r.email) {
          needsWaterfall[wi].email = r.email;
          needsWaterfall[wi].email_source = r.source;
          needsWaterfall[wi].email_confidence = r.status === 'verified' ? 'verified' :
            r.status === 'catchall' ? 'catchall' : 'likely';
        }
      }

      const wfStats = waterfall.getStats();
      const wfElapsed = ((Date.now() - wfStart) / 1000).toFixed(1);
      console.log(`  Waterfall: ${wfStats.found} emails in ${wfElapsed}s`);
      if (Object.keys(wfStats.bySource).length > 0) {
        for (const [src, count] of Object.entries(wfStats.bySource).sort((a, b) => b[1] - a[1])) {
          console.log(`    ${src}: ${count}`);
        }
      }
    }
  }

  // ─── Export CSV ────────────────────────────────────────────────

  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  STEP 3: Export CSV');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');

  // Sort: email first, then by seniority
  const seniorityOrder = { owner: 0, founder: 1, c_suite: 2, partner: 3, vp: 4, director: 5, manager: 6, senior: 7, entry: 8 };
  leads.sort((a, b) => {
    const ae = a.email ? 0 : 1, be = b.email ? 0 : 1;
    if (ae !== be) return ae - be;
    return (seniorityOrder[a.seniority] ?? 9) - (seniorityOrder[b.seniority] ?? 9);
  });

  const csvLeads = leads.map(lead => ({
    first_name: lead.first_name || '',
    last_name: lead.last_name || '',
    firm_name: lead.firm_name || '',
    title: lead.title || '',
    seniority: lead.seniority || '',
    city: lead.city || '',
    state: lead.state || '',
    country: lead.country || '',
    phone: lead.phone ? normalizePhone(lead.phone) : '',
    email: lead.email || '',
    email_confidence: lead.email_confidence || (lead.email_source === 'apollo_reveal' ? 'verified' : lead.email_source === 'website_name_match' ? 'high' : lead.email_source === 'website_domain' ? 'medium' : lead.email ? 'low' : ''),
    email_source: lead.email_source || '',
    website: lead.website || '',
    domain: lead.domain || '',
    linkedin_url: lead.linkedin_url || '',
    headline: lead.headline || '',
    source: 'apollo',
  }));

  const titleSlug = (TITLES[0] || 'search').toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 20);
  const stateSlug = STATE.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 20);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outputPath = OUTPUT || path.join(__dirname, '..', 'output', `apollo-turbo_${titleSlug}_${stateSlug}_${timestamp}.csv`);

  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  // Write CSV directly with Apollo-specific columns (no empty lawyer fields)
  const APOLLO_COLUMNS = [
    'first_name', 'last_name', 'firm_name', 'title', 'seniority',
    'city', 'state', 'country', 'phone', 'email', 'email_confidence',
    'email_source', 'website', 'domain', 'linkedin_url', 'headline', 'source',
  ];
  const csvHeader = APOLLO_COLUMNS.join(',');
  const csvRows = csvLeads.map(row =>
    APOLLO_COLUMNS.map(col => {
      const val = (row[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',')
  );
  fs.writeFileSync(outputPath, [csvHeader, ...csvRows].join('\n'));

  // ─── Summary ───────────────────────────────────────────────────

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const emailCount = leads.filter(l => l.email).length;
  const phoneCount = leads.filter(l => l.phone).length;
  const linkedinCount = leads.filter(l => l.linkedin_url).length;
  const fullNameCount = leads.filter(l => l.last_name && l.last_name.length > 2).length;
  const obfuscatedCount = leads.length - fullNameCount;
  const withDomain = leads.filter(l => l.domain).length;

  // Email source breakdown
  const bySource = {};
  for (const l of leads) {
    if (l.email_source) bySource[l.email_source] = (bySource[l.email_source] || 0) + 1;
  }

  console.log('');
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║            APOLLO TURBO SCRAPE COMPLETE                  ║');
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Total leads:         ${String(leads.length).padStart(6)}                             ║`);
  console.log(`║  Full name:           ${String(fullNameCount).padStart(6)} (${Math.round(fullNameCount/Math.max(leads.length,1)*100)}%)${obfuscatedCount > 0 ? ` [${obfuscatedCount} obfuscated]` : ''}                   ║`);
  console.log(`║  With email:          ${String(emailCount).padStart(6)} (${Math.round(emailCount/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log(`║  With phone:          ${String(phoneCount).padStart(6)} (${Math.round(phoneCount/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log(`║  With LinkedIn:       ${String(linkedinCount).padStart(6)} (${Math.round(linkedinCount/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log(`║  With domain:         ${String(withDomain).padStart(6)} (${Math.round(withDomain/Math.max(leads.length,1)*100)}%)                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  if (Object.keys(bySource).length > 0) {
    console.log('║  Email Sources:                                           ║');
    for (const [src, count] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
      console.log(`║    ${src.padEnd(28)} ${String(count).padStart(5)}                   ║`);
    }
    console.log('╠═══════════════════════════════════════════════════════════╣');
  }
  console.log(`║  Search speed:        ${searchRate} leads/min                     ║`);
  console.log(`║  Total time:          ${totalElapsed}s                            ║`);
  console.log('╠═══════════════════════════════════════════════════════════╣');
  console.log(`║  Output: ${outputPath}`);
  console.log('╚═══════════════════════════════════════════════════════════╝');
  console.log('');
}

// ─── Helpers ──────────────────────────────────────────────────────

/**
 * Search all available pages for a query (max 5 pages = 125 results due to free tier cap).
 */
async function searchAllPages(page, filters, seenIds) {
  const people = [];
  let totalAvailable = 0;

  for (let pg = 1; pg <= 5; pg++) {
    let result;
    let retries = 0;

    while (retries < 3) {
      result = await page.evaluate((filtersStr, pageNum) => {
        return new Promise(resolve => {
          const xhr = new XMLHttpRequest();
          xhr.open('POST', '/api/v1/mixed_people/search');
          xhr.setRequestHeader('Content-Type', 'application/json');
          xhr.withCredentials = true;
          xhr.timeout = 20000;
          xhr.onload = () => {
            const text = xhr.responseText;
            // Detect Cloudflare challenge
            if (text.trim().startsWith('<') || text.includes('cf-turnstile') || text.includes('challenges.cloudflare')) {
              resolve({ people: [], total: 0, totalPages: 0, cloudflare: true });
              return;
            }
            if (xhr.status === 429) {
              resolve({ people: [], total: 0, totalPages: 0, rateLimit: true });
              return;
            }
            try {
              const data = JSON.parse(text);
              const pag = data.pagination || {};
              resolve({
                people: (data.people || []).map(p => {
                  const org = p.organization || {};
                  return {
                    apollo_id: p.id,
                    first_name: p.first_name || '',
                    last_name: p.last_name || '',
                    title: p.title || '',
                    headline: p.headline || '',
                    seniority: p.seniority || '',
                    linkedin_url: p.linkedin_url || '',
                    city: p.city || '',
                    state: p.state || '',
                    country: p.country || '',
                    email: (p.email && !p.email.includes('not_unlocked')) ? p.email : '',
                    email_status: p.email_status || '',
                    firm_name: org.name || '',
                    website: org.website_url || '',
                    domain: org.primary_domain || '',
                    phone: org.sanitized_phone || org.primary_phone || '',
                  };
                }),
                total: pag.total_entries || 0,
                totalPages: pag.total_pages || 0,
              });
            } catch (e) { resolve({ people: [], total: 0, totalPages: 0, error: e.message }); }
          };
          xhr.onerror = () => resolve({ people: [], total: 0, totalPages: 0, error: 'network' });
          xhr.ontimeout = () => resolve({ people: [], total: 0, totalPages: 0, error: 'timeout' });

          const body = JSON.parse(filtersStr);
          body.page = pageNum;
          body.per_page = 25;
          xhr.send(JSON.stringify(body));
        });
      }, JSON.stringify(filters), pg);

      if (result.cloudflare) {
        retries++;
        if (retries < 3) {
          await sleep(3000 * retries); // Back off: 3s, 6s, 9s
          continue;
        }
        break; // Give up after 3 retries
      }
      if (result.rateLimit) {
        await sleep(30000); // Wait 30s on rate limit
        retries++;
        continue;
      }
      break; // Success
    }

    if (!result || result.cloudflare) break;
    if (pg === 1) totalAvailable = result.total;
    if (result.people.length === 0) break;

    // Add only new (unseen) leads
    for (const p of result.people) {
      if (p.apollo_id && !seenIds.has(p.apollo_id)) {
        if (!p.domain && p.website) {
          try { p.domain = new URL(p.website).hostname.replace(/^www\./, ''); } catch {}
        }
        people.push(p);
        seenIds.add(p.apollo_id);
      }
    }

    if (pg >= result.totalPages) break;
    await sleep(800); // 0.8s between pages within same query
  }

  return { people, totalAvailable };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Validate an email address — rejects malformed/concatenated strings.
 */
function isValidEmail(email) {
  if (!email || email.length > 60) return false;
  const parts = email.split('@');
  if (parts.length !== 2) return false;
  const [local, domain] = parts;
  if (!local || !domain || local.length > 40) return false;
  // Domain must have at least one dot and only valid chars
  if (!/^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/.test(domain)) return false;
  // Reject double extensions (.comwant, .comfoo, etc.)
  const tld = domain.split('.').pop();
  if (tld.length > 6) return false; // longest valid TLD is ~6 chars
  return true;
}

/**
 * Apollo People Match API — enriches a person with verified email via REST API.
 * Uses /api/v1/people/match (1 credit per match, free tier: 10,000/month).
 */
function apolloMatch(apiKey, firstName, lastName, domain) {
  const https = require('https');
  return new Promise((resolve) => {
    const data = JSON.stringify({ first_name: firstName, last_name: lastName, domain: domain });
    const req = https.request({
      hostname: 'api.apollo.io',
      path: '/api/v1/people/match',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'Content-Length': Buffer.byteLength(data) },
      timeout: 15000,
    }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          const j = JSON.parse(body);
          if (j.error || (typeof j === 'string' && j.includes('insufficient'))) {
            resolve({ error: j.error || j });
            return;
          }
          const p = j.person || {};
          const email = (p.email && !p.email.includes('not_unlocked')) ? p.email : '';
          resolve({ email, status: p.email_status || '', matched: !!p.id });
        } catch { resolve({ error: 'parse: ' + body.slice(0, 100) }); }
      });
    });
    req.on('error', e => resolve({ error: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ error: 'timeout' }); });
    req.write(data);
    req.end();
  });
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  process.exit(1);
});
