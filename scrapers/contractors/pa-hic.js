/**
 * Pennsylvania Home Improvement Contractor (HIC) Scraper
 *
 * Source: PA Attorney General — Home Improvement Contractor Search
 * URL: https://hicsearch.attorneygeneral.gov/
 * Records: 50K+ registered contractors with phone numbers
 * Method: POST JSON to /Search/Search (Syncfusion Grid + custom adaptor)
 *
 * Available fields per record:
 *   businessId             — Internal GUID
 *   businessName           — Business/DBA name
 *   registrationNumber     — PA registration number (e.g., "PA000123")
 *   expirationDateClean    — Registration expiration date
 *   address                — Street address line 1
 *   address2               — Street address line 2 (optional)
 *   address3               — Street address line 3 (optional)
 *   city                   — City name
 *   stateCode              — State abbreviation
 *   stateName              — State full name (hidden column)
 *   zip                    — ZIP code
 *   phone                  — Phone number
 *   phoneClean             — Phone digits only (hidden column)
 *   businessTypeOfWorkSet  — Type(s) of work (hidden column)
 *   recordId               — Numeric record ID (hidden column)
 *
 * Search strategy:
 *   - Iterates through all 67 PA counties (each county has a GUID in the form)
 *   - POSTs to /Search/Search with county GUID + Skip/Take for pagination
 *   - Page size: 25 (server default from Syncfusion grid config)
 *
 * Anti-bot: Cloudflare Turnstile CAPTCHA is required. The server validates the
 *   token and returns empty results {} when invalid. The scraper attempts
 *   requests without a token first and yields a _captcha signal if blocked.
 *
 * Type of Work categories (50+ in the dropdown):
 *   95-5 Carpentry, 95-10 Carpet Installation, 95-15 Central Vacuum Systems,
 *   95-16 Chimney Sweep, 95-20 Cleaning & Janitorial, 95-45 Concrete,
 *   95-46A Construction (Conventional), 95-46B Construction (Pole-framing),
 *   95-47 Countertops, 95-50 Decks/Patios/Porches, 95-93 Demolition,
 *   95-55 Disaster-Related, 95-56 Doors, 95-57 Drywall/Plaster,
 *   95-60 Electrical, 95-65 Excavation, 95-70 Extermination, 95-75 Fencing,
 *   95-80 Flooring (Non-Carpet), 95-82 Gutters & Downspouts,
 *   95-85 Handyman, 95 Home Improvement, 95-90 Home Improvement - General,
 *   95-95 HVAC, 95-100 Insulation, 95-105 Interior Decorators,
 *   95-110 Landscaping & Hardscaping, 95-115 Lawn & Garden,
 *   95-120 Locksmith, 95-125 Masonry, 95-130 Painting & Staining,
 *   95-135 Paving - Asphalt, 95-140 Plumbing, 95-145 Pressure Washing,
 *   95-150 Radon & Carbon Monoxide, 95-155 Remediation & Restoration,
 *   95-157 Remodeling & Renovation, 95-160 Roofing,
 *   95-165 Security & Fire Alarm, 95-167 Sewage/Septic,
 *   95-170 Siding, 95-172 Stucco, 95-175 Swimming Pools (Above Ground),
 *   95-180 Swimming Pools (In-Ground), 95-185 Third Party Referral,
 *   95-190 Tree Services, 95-197 Water Conditioning,
 *   95-195 Waterproofing, 95-200 Windows & Glazing
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_HOST = 'hicsearch.attorneygeneral.gov';
const SEARCH_PATH = '/Search/Search';
const PAGE_SIZE = 25;  // Server default from Syncfusion grid config

// All 67 PA counties with their GUIDs from the search form dropdown
const PA_COUNTIES = [
  { name: 'Adams', guid: '95f85c95-6444-4f32-b4d0-9d8fae0a2684' },
  { name: 'Allegheny', guid: 'dafe1cd6-26dd-4308-bee3-305a73dd0418' },
  { name: 'Armstrong', guid: '5c0b8ae8-a236-407c-9772-28a2e07c5e1c' },
  { name: 'Beaver', guid: '367bda22-dfb7-44ee-8426-3b9a622d9709' },
  { name: 'Bedford', guid: '71fe0ff9-76d8-430c-9ba3-b9e955e9c6ce' },
  { name: 'Berks', guid: '66592884-2047-446e-9e69-620af3cd790a' },
  { name: 'Blair', guid: '8e16ab88-f18a-498d-8bd6-0f3c4775accd' },
  { name: 'Bradford', guid: '2ac94cc3-c583-446b-a51e-6859002e54e5' },
  { name: 'Bucks', guid: '893dc108-ba39-40a1-b3ef-5d40e74005a1' },
  { name: 'Butler', guid: '84318bbb-5517-4005-b088-9219971e2567' },
  { name: 'Cambria', guid: 'b5715dd2-2216-4c15-91fd-84e6dba9afb8' },
  { name: 'Cameron', guid: '6892a610-c01c-43e7-9e0e-9a294fa7b5a1' },
  { name: 'Carbon', guid: '2fa4edec-5574-4b87-b6d4-635b01eb2a7b' },
  { name: 'Centre', guid: 'd569a88d-fa66-4c6e-9716-d5f72718f91a' },
  { name: 'Chester', guid: '209c83df-91b6-42b1-8e51-09024b76e40c' },
  { name: 'Clarion', guid: '4fd09da0-cbce-40b0-bc24-fe62ef28789d' },
  { name: 'Clearfield', guid: 'da781742-574a-47cf-a4af-56dacd008d78' },
  { name: 'Clinton', guid: 'c331b2f7-0aba-4a21-ab75-a70b0c0146e8' },
  { name: 'Columbia', guid: '76e7ba34-7f14-4e9d-a3dd-316c8751d9f4' },
  { name: 'Crawford', guid: 'fab0bdde-1400-4035-aca8-b469d79c5709' },
  { name: 'Cumberland', guid: '75adeb66-985b-4cc5-9444-a0e16e165039' },
  { name: 'Dauphin', guid: '6d27568c-3a66-4fba-bdba-09fec53539d0' },
  { name: 'Delaware', guid: '57108669-fbd7-48e6-890f-ecd3d657cac2' },
  { name: 'Elk', guid: 'ec22b4d4-4e8c-4f93-bad5-f701acde208f' },
  { name: 'Erie', guid: 'fc61e2c1-1dc3-4ac9-b65a-5060361e75bf' },
  { name: 'Fayette', guid: 'ca05a8e7-9b8c-4631-98da-aa1dca53dc9b' },
  { name: 'Forest', guid: 'f8617a76-2c4a-4098-b680-824306436d43' },
  { name: 'Franklin', guid: '27b1a9ae-d9f3-41d1-944e-dcaa5a9fc9b2' },
  { name: 'Fulton', guid: 'f3c0102e-35c3-47d4-98a7-09a7bdfcac88' },
  { name: 'Greene', guid: '69762013-75be-4c12-a839-6a7939cd51b1' },
  { name: 'Huntingdon', guid: 'ba9baaf6-3171-4f16-8217-7f5f4340654b' },
  { name: 'Indiana', guid: 'f9e55c58-3508-4252-a100-21c54a4bc57a' },
  { name: 'Jefferson', guid: '82d5ece1-f629-435c-b8b9-e47aef941656' },
  { name: 'Juniata', guid: '5ac48138-cb8a-4289-a373-5738814b6a04' },
  { name: 'Lackawanna', guid: '763d63d6-b318-479f-823f-eb922aa86bc5' },
  { name: 'Lancaster', guid: '24fb7cdb-086d-4b4e-ba7d-4f0432fba616' },
  { name: 'Lawrence', guid: 'f252832c-addc-4488-a343-3c9160865e28' },
  { name: 'Lebanon', guid: '756b562e-126a-4435-9fa8-037008696fe6' },
  { name: 'Lehigh', guid: '160a38f2-5b1c-40bf-9727-44c3ebb89c16' },
  { name: 'Luzerne', guid: '8a9ae751-80a8-493a-812c-e1e4b2e2d6be' },
  { name: 'Lycoming', guid: '16585831-8170-4ddd-a57e-14e173d6df07' },
  { name: 'McKean', guid: 'f4e45fde-f921-485a-ad55-9e438487815b' },
  { name: 'Mercer', guid: 'cdb4581b-61a1-4c1f-ac94-33473c92c992' },
  { name: 'Mifflin', guid: 'cc0eb8b2-71ce-4375-b3ec-b80817560186' },
  { name: 'Monroe', guid: '7f019bd6-b87f-42e0-a07d-5fa1845978a2' },
  { name: 'Montgomery', guid: 'b052c44f-769e-46bf-b02e-3136194c467c' },
  { name: 'Montour', guid: 'a14d7255-da3e-4164-9af8-627cd092c083' },
  { name: 'Northampton', guid: '6e8c38be-e6fc-48b9-95c8-3981b2a61c63' },
  { name: 'Northumberland', guid: 'e64d7eae-f1f2-4f21-8ffc-e3baa5e804a2' },
  { name: 'Perry', guid: 'e84657c5-b236-422b-bccc-5968bfe874e9' },
  { name: 'Philadelphia', guid: '72cb339d-adca-42b3-9f65-b883ef742016' },
  { name: 'Pike', guid: '5f81451a-8dbc-4e56-b6f8-80fd3624268f' },
  { name: 'Potter', guid: '8f83f521-dbab-4940-a1a7-fe6f75de52c4' },
  { name: 'Schuylkill', guid: 'b533aa00-b759-4347-bd88-aa90ba6cfb2b' },
  { name: 'Snyder', guid: 'bfc4f886-3df4-43c1-8168-7f9eb4a46b52' },
  { name: 'Somerset', guid: 'f5457104-ffd8-43e3-a9dd-a2c82dfc91c3' },
  { name: 'Sullivan', guid: '4f973eb1-2a83-44f8-b034-ae5fea7c7fad' },
  { name: 'Susquehanna', guid: '90cb4258-0133-4936-b1e8-67291fb7eac8' },
  { name: 'Tioga', guid: 'bf24a080-b373-44b0-bf91-b4bd82642f6c' },
  { name: 'Union', guid: 'dfb52e47-35c6-4226-ad6e-c4b413fdcd9e' },
  { name: 'Venango', guid: '25b86620-6870-4866-ac7f-1482814c54e4' },
  { name: 'Warren', guid: 'b2f7f918-dc0d-4c89-adc4-9ef5a108ff53' },
  { name: 'Washington', guid: 'ad4f2e94-fcb4-4827-bb68-43f3946e1288' },
  { name: 'Wayne', guid: '04e90913-eac9-4822-ba45-a6576f466dd7' },
  { name: 'Westmoreland', guid: '8a57224b-da53-449d-b80e-eee449ba6e35' },
  { name: 'Wyoming', guid: '74dac8c9-45a9-450a-b682-37d83e495880' },
  { name: 'York', guid: '5eadc9d8-f991-4230-aa52-84e225356325' },
];

// Type of Work codes from the search form dropdown
const TYPE_OF_WORK_CODES = {
  'carpentry': '95-5',
  'carpet installation': '95-10',
  'central vacuum': '95-15',
  'chimney sweep': '95-16',
  'cleaning': '95-20',
  'janitorial': '95-20',
  'concrete': '95-45',
  'construction': '95-46A',
  'additions': '95-46A',
  'pole framing': '95-46B',
  'countertops': '95-47',
  'decks': '95-50',
  'patios': '95-50',
  'porches': '95-50',
  'demolition': '95-93',
  'disaster': '95-55',
  'doors': '95-56',
  'drywall': '95-57',
  'plaster': '95-57',
  'electrical': '95-60',
  'excavation': '95-65',
  'extermination': '95-70',
  'pest control': '95-70',
  'fencing': '95-75',
  'flooring': '95-80',
  'gutters': '95-82',
  'downspouts': '95-82',
  'handyman': '95-85',
  'home improvement': '95',
  'general contractor': '95-90',
  'general': '95-90',
  'hvac': '95-95',
  'heating': '95-95',
  'cooling': '95-95',
  'insulation': '95-100',
  'interior decorating': '95-105',
  'landscaping': '95-110',
  'hardscaping': '95-110',
  'lawn': '95-115',
  'garden': '95-115',
  'locksmith': '95-120',
  'masonry': '95-125',
  'brick': '95-125',
  'stone': '95-125',
  'painting': '95-130',
  'staining': '95-130',
  'paving': '95-135',
  'asphalt': '95-135',
  'plumbing': '95-140',
  'pressure washing': '95-145',
  'radon': '95-150',
  'carbon monoxide': '95-150',
  'remediation': '95-155',
  'restoration': '95-155',
  'remodeling': '95-157',
  'renovation': '95-157',
  'roofing': '95-160',
  'security': '95-165',
  'fire alarm': '95-165',
  'sewage': '95-167',
  'septic': '95-167',
  'siding': '95-170',
  'stucco': '95-172',
  'swimming pools': '95-175',
  'pools above ground': '95-175',
  'pools in ground': '95-180',
  'tree services': '95-190',
  'tree removal': '95-190',
  'water conditioning': '95-197',
  'waterproofing': '95-195',
  'windows': '95-200',
  'glazing': '95-200',
};

// Business name keywords indicating a company (not a person's name)
const BUSINESS_KEYWORDS = [
  'inc', 'llc', 'corp', 'ltd', 'co', 'company', 'plumbing', 'electric',
  'electrical', 'heating', 'cooling', 'hvac', 'roofing', 'construction',
  'contracting', 'contractors', 'contractor', 'renovations', 'renovation',
  'remodeling', 'services', 'service', 'enterprises', 'enterprise',
  'builders', 'building', 'group', 'associates', 'solutions', 'systems',
  'maintenance', 'management', 'restoration', 'improvements', 'improvement',
  'home', 'general', 'sons', 'brothers', 'bros', 'industries', 'supply',
  'design', 'designs', 'interiors', 'exteriors', 'painting', 'floors',
  'flooring', 'kitchens', 'bathrooms', 'windows', 'doors', 'insulation',
  'mechanical', 'dba', 'd/b/a', 'pllc', 'pllp', 'lp', 'llp',
];

/**
 * Determine if a business name looks like a person's name vs. a company name.
 * Returns { firstName, lastName, firmName } with best-guess split.
 */
function splitBusinessName(raw) {
  if (!raw) return { firstName: '', lastName: '', firmName: '' };

  const name = raw.trim();
  const lower = name.toLowerCase();

  // If it contains business keywords, treat entire thing as firm name
  for (const kw of BUSINESS_KEYWORDS) {
    const pattern = new RegExp(`\\b${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    if (pattern.test(lower)) {
      return { firstName: '', lastName: '', firmName: titleCase(name) };
    }
  }

  // If it contains numbers, punctuation (beyond apostrophe/hyphen/comma), treat as business
  if (/\d/.test(name) || /[&@#!%]/.test(name)) {
    return { firstName: '', lastName: '', firmName: titleCase(name) };
  }

  // Check for "LAST, FIRST" or "LAST, FIRST MIDDLE" format
  const commaMatch = name.match(/^([A-Za-z'-]+)\s*,\s*(.+)$/);
  if (commaMatch) {
    const last = commaMatch[1].trim();
    const firstParts = commaMatch[2].trim().split(/\s+/);
    const first = firstParts[0] || '';
    if (firstParts.length <= 2 && first.length > 1) {
      return {
        firstName: titleCase(first),
        lastName: titleCase(last),
        firmName: '',
      };
    }
  }

  const words = name.split(/\s+/).filter(Boolean);

  // 2-3 words without business keywords — likely a person's name
  if (words.length >= 2 && words.length <= 3) {
    return {
      firstName: titleCase(words[0]),
      lastName: titleCase(words[words.length - 1]),
      firmName: '',
    };
  }

  // Single word or 4+ words — treat as business
  return { firstName: '', lastName: '', firmName: titleCase(name) };
}

/**
 * Title-case a string (UPPERCASE -> Title Case).
 */
function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|[\s\-'/(])(\w)/g, (match) => match.toUpperCase());
}

/**
 * Format a phone string. The API returns formatted like "(717) 555-1234" or "717-555-1234".
 * Normalize to (XXX) XXX-XXXX.
 */
function formatPhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return digits.length >= 7 ? raw.trim() : '';
}

/**
 * Parse expiration date to YYYY-MM-DD.
 * Server returns dates as "MM/DD/YYYY" or ISO format.
 */
function formatDate(dateStr) {
  if (!dateStr) return '';
  // Try ISO format first
  const isoMatch = dateStr.match(/^(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];
  // Try MM/DD/YYYY format
  const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (usMatch) {
    const mm = usMatch[1].padStart(2, '0');
    const dd = usMatch[2].padStart(2, '0');
    return `${usMatch[3]}-${mm}-${dd}`;
  }
  return dateStr;
}


class PaHicScraper extends BaseScraper {
  constructor() {
    super({
      name: 'pa_hic_contractors',
      stateCode: 'PA-HIC',
      baseUrl: `https://${API_HOST}`,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: TYPE_OF_WORK_CODES,
      defaultCities: PA_COUNTIES.map(c => c.name),
    });
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override detectCaptcha — the PA HIC API uses Cloudflare Turnstile.
   * An empty response {} or {"result":[],"count":0} with no data signals CAPTCHA block.
   */
  detectCaptcha(body) {
    // The server returns {} when Turnstile token is invalid/missing
    if (body === '{}') return true;
    // Check for standard CAPTCHA indicators
    return body.includes('turnstile') || body.includes('challenge-form') ||
           body.includes('cf-turnstile');
  }

  /**
   * Override transformResult to use our source name.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'pa_hic_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTPS POST to the PA HIC search API.
   * Returns { statusCode, body }.
   */
  _apiPost(payload, rateLimiter) {
    return new Promise((resolve, reject) => {
      const data = JSON.stringify(payload);

      const req = https.request({
        hostname: API_HOST,
        path: SEARCH_PATH,
        method: 'POST',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
          'Origin': `https://${API_HOST}`,
          'Referer': `https://${API_HOST}/`,
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      }, (res) => {
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('PA HIC API request timed out')); });
      req.write(data);
      req.end();
    });
  }

  /**
   * Build the search payload for a county + type of work + pagination.
   *
   * @param {object} params
   * @param {string} params.countyGuid — County GUID from PA_COUNTIES
   * @param {string} params.typeOfWork — Type of work code (e.g., "95-160" for Roofing)
   * @param {number} params.skip — Records to skip (pagination offset)
   * @param {number} params.take — Records per page
   * @returns {object} JSON payload matching the server's expected format
   */
  _buildPayload({ countyGuid, typeOfWork, skip, take }) {
    return {
      RegistrationNumber: '',
      BusinessName: '',
      PrimaryApplicant: '',
      BusinessAddress: '',
      BusinessCity: '',
      BusinessState: 'PA',
      BusinessZip: '',
      BusinessCounty: countyGuid || '',
      BusinessPhone: '',
      BusinessTypeOfWork: typeOfWork || '',
      includeExpired: false,
      Skip: skip,
      Take: take,
      Token: '',  // Cloudflare Turnstile token — empty without browser
    };
  }

  /**
   * Map a PA HIC API result record to a normalized lead object.
   */
  _mapRecord(rec, countyName) {
    const businessName = (rec.businessName || '').trim();
    const { firstName, lastName, firmName } = splitBusinessName(businessName);

    const address = (rec.address || '').trim();
    const address2 = (rec.address2 || '').trim();
    const fullAddress = [address, address2].filter(Boolean).join(', ');

    const city = titleCase((rec.city || '').trim());
    const stateCode = (rec.stateCode || 'PA').trim();
    const zip = (rec.zip || '').trim();
    const phone = formatPhone(rec.phone || rec.phoneClean || '');
    const regNumber = (rec.registrationNumber || '').trim();
    const expDate = formatDate(rec.expirationDateClean || '');
    const typeOfWork = (rec.businessTypeOfWorkSet || '').trim();

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName || (firstName ? '' : titleCase(businessName)),
      address: fullAddress,
      city,
      state: stateCode,
      zip,
      county: countyName || '',
      phone,
      email: '',    // Not available in this dataset
      website: '',  // Not available in this dataset
      bar_number: regNumber,
      bar_status: expDate ? 'Active' : '',
      license_type: typeOfWork,
      license_expiry: expDate,
      source: 'pa_hic_contractors',
      practice_area: typeOfWork || 'Home Improvement Contractor',
    };
  }

  /**
   * Async generator that yields contractor leads from PA HIC registry.
   *
   * Strategy:
   * 1. Iterate through PA counties (67 total), each identified by GUID
   * 2. For each county, POST to /Search/Search with county filter
   * 3. Paginate via Skip/Take until no more results
   * 4. If Turnstile CAPTCHA blocks results, yield _captcha signal
   *
   * The Turnstile CAPTCHA is enforced server-side. When the token is empty,
   * the server returns {} (empty object). The scraper detects this and
   * signals CAPTCHA. If the CAPTCHA is ever relaxed or a token provider
   * is added, the scraper will work without code changes.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Resolve type of work code from practice area
    const typeOfWork = this.resolvePracticeCode(practiceArea) || '';

    // Determine which counties to search
    let counties = PA_COUNTIES;
    if (options.city) {
      // Match county by name (case-insensitive)
      const target = options.city.toLowerCase().trim();
      const match = PA_COUNTIES.find(c => c.name.toLowerCase() === target);
      if (match) {
        counties = [match];
      } else {
        log.warn(`County "${options.city}" not found in PA counties — searching all counties`);
      }
    }
    if (options.maxCities) {
      counties = counties.slice(0, options.maxCities);
    }

    log.scrape(`PA HIC Contractor search — ${practiceArea || 'all types'}, counties=${counties.length}`);

    // Track seen registration numbers to deduplicate across counties
    const seenRegNumbers = new Set();
    let totalYielded = 0;
    let totalWithPhone = 0;
    let captchaBlocked = false;

    for (let ci = 0; ci < counties.length; ci++) {
      const county = counties[ci];
      yield { _cityProgress: { current: ci + 1, total: counties.length } };
      log.scrape(`Searching PA HIC contractors in ${county.name} County`);

      let skip = 0;
      let pagesFetched = 0;
      let countyYielded = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${county.name} County`);
          break;
        }

        const payload = this._buildPayload({
          countyGuid: county.guid,
          typeOfWork,
          skip,
          take: PAGE_SIZE,
        });

        log.info(`Page ${pagesFetched + 1} (skip ${skip}) — ${county.name} County`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiPost(payload, rateLimiter);
        } catch (err) {
          log.error(`PA HIC API request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429) {
          log.warn('PA HIC API rate limit hit (429)');
          const shouldRetry = await rateLimiter.handleBlock(429);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 403) {
          log.warn('PA HIC API returned 403 — blocked');
          const shouldRetry = await rateLimiter.handleBlock(403);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`PA HIC API returned ${response.statusCode} for ${county.name} County`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for Turnstile CAPTCHA block (server returns {} when token invalid)
        if (this.detectCaptcha(response.body)) {
          if (!captchaBlocked) {
            log.warn('PA HIC: Cloudflare Turnstile CAPTCHA required — server returned empty response');
            captchaBlocked = true;
          }
          yield {
            _captcha: true,
            city: county.name,
            reason: 'Cloudflare Turnstile CAPTCHA required — server validates token server-side',
          };
          break;
        }

        // Parse response — expected format: {"result": [...], "count": N}
        let data;
        try {
          data = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse PA HIC API response: ${err.message}`);
          break;
        }

        // Validate response shape
        const results = data.result || data.Result || [];
        const totalCount = data.count || data.Count || 0;

        if (!Array.isArray(results)) {
          log.error(`Unexpected PA HIC response shape for ${county.name} County`);
          break;
        }

        // Log count on first page
        if (pagesFetched === 0 && totalCount > 0) {
          const totalPages = Math.ceil(totalCount / PAGE_SIZE);
          log.success(`Found ${totalCount.toLocaleString()} contractors in ${county.name} County (${totalPages} pages)`);
        }

        if (results.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            if (countyYielded > 0) {
              log.info(`No more results for ${county.name} County`);
            }
            break;
          }
          skip += PAGE_SIZE;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const rec of results) {
          // Deduplicate by registration number
          const regNum = (rec.registrationNumber || '').trim();
          if (regNum && seenRegNumbers.has(regNum)) continue;
          if (regNum) seenRegNumbers.add(regNum);

          const lead = this._mapRecord(rec, county.name);

          // Skip records with no business name at all
          if (!lead.firm_name && !lead.first_name && !lead.last_name) continue;

          if (lead.phone) totalWithPhone++;

          yield this.transformResult(lead, practiceArea);
          countyYielded++;
          totalYielded++;
        }

        log.info(`Got ${results.length} records for ${county.name} County (${countyYielded} leads from this county so far)`);

        // Check if we've exhausted results
        if (results.length < PAGE_SIZE) {
          log.success(`Completed ${county.name} County: ${countyYielded} leads`);
          break;
        }

        // Check if we've reached the total count
        if (totalCount > 0 && skip + PAGE_SIZE >= totalCount) {
          log.success(`Completed ${county.name} County: ${countyYielded} leads (${totalCount} total)`);
          break;
        }

        skip += PAGE_SIZE;
        pagesFetched++;
      }

      // If first county was CAPTCHA-blocked, stop early
      if (captchaBlocked && countyYielded === 0 && ci === 0) {
        log.warn('PA HIC: Stopping search — Cloudflare Turnstile CAPTCHA required for all queries');
        break;
      }
    }

    log.success([
      `PA HIC scrape complete:`,
      `${seenRegNumbers.size.toLocaleString()} unique registrations`,
      `${totalYielded.toLocaleString()} leads yielded`,
      `${totalWithPhone.toLocaleString()} with phone`,
      captchaBlocked ? '(CAPTCHA blocked — Turnstile token required)' : '',
    ].filter(Boolean).join(' | '));
  }
}

module.exports = new PaHicScraper();
