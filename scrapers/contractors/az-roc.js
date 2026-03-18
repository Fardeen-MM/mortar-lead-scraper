/**
 * Arizona ROC (Registrar of Contractors) — Contractor Scraper
 *
 * Source: https://azroc.my.site.com/AZRoc/s/contractor-search
 * Records: 40K+ licensed contractors with classification codes
 * Method: Salesforce Experience Cloud Aura API (guest user access)
 *
 * The Arizona ROC runs on Salesforce Experience Cloud. The Aura API endpoint
 * at /s/sfsites/aura is accessible to guest (unauthenticated) users and
 * exposes the License__c custom object with contractor data.
 *
 * API approach:
 *   1. getItems — paginate through License__c records (up to 2000 per run)
 *      Returns: license number, type, subtype (classification), status, firm name
 *   2. getRecordWithFields — fetch individual records with Account relationship
 *      Returns: phone, email, website, address (when available)
 *
 * The list view is capped at 2000 records by Salesforce, sorted by license number.
 * Records include both active and inactive licenses. The scraper filters by
 * status (Active only by default) and classification code.
 *
 * Classification codes follow Arizona ROC conventions:
 *   R-class: Residential (R-37 Plumbing, R-11 Electrical, etc.)
 *   C-class: Commercial (C-37 Plumbing, C-11 Electrical, etc.)
 *   CR-class: Combined Residential/Commercial
 *   B-class: General contractors (B General Residential, B-1 General Commercial)
 *   A-class: General engineering
 *   K-class: Dual-licensed (KA Engineering, KB Building)
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Salesforce Aura API endpoint
const AURA_URL = 'https://azroc.my.site.com/AZRoc/s/sfsites/aura';

// Aura context for guest user requests
const AURA_CONTEXT = JSON.stringify({
  mode: 'PROD',
  fwuid: '',
  app: 'siteforce:communityApp',
});

// Fields to request for individual license records (includes Account relationship)
const LICENSE_FIELDS = [
  'License__c.Name',
  'License__c.License_Type__c',
  'License__c.Status__c',
  'License__c.license_subtype__c',
  'License__c.Primary_Licensee__c',
  'License__c.Primary_Licensee__r.Name',
  'License__c.Primary_Licensee__r.Phone',
  'License__c.Primary_Licensee__r.Email__c',
  'License__c.Primary_Licensee__r.Website',
  'License__c.Primary_Licensee__r.BillingCity',
  'License__c.Primary_Licensee__r.BillingState',
  'License__c.Primary_Licensee__r.BillingPostalCode',
  'License__c.Primary_Licensee__r.BillingStreet',
];

// Practice area -> classification code prefix mapping
// Users select a friendly name, scraper filters by subtype prefix
const PRACTICE_AREA_CODES = {
  'general contractor': 'B',
  'general commercial': 'B-1',
  'general residential': 'B',
  'roofing': 'CR-42',
  'plumbing': 'R-37',
  'painting': 'CR-34',
  'hvac': 'R-39',
  'electrical': 'R-11',
  'landscaping': 'CR-21',
  'masonry': 'CR-31',
  'concrete': 'R-9',
  'drywall': 'CR-10',
  'fencing': 'CR-14',
  'carpentry': 'CR-61',
  'tile': 'CR-48',
  'insulation': 'CR-40',
  'septic': 'CR-41',
  'swimming pools': 'A-9',
  'sheet metal': 'CR-45',
  'glazing': 'CR-65',
  'welding': 'CR-56',
  'fire protection': 'CR-16',
  'engineering': 'A',
  'floor covering': 'CR-8',
};

// Major Arizona cities for reference (not used for filtering since API is statewide)
const DEFAULT_CITIES = [
  'Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Scottsdale',
  'Glendale', 'Gilbert', 'Tempe', 'Peoria', 'Surprise',
  'Flagstaff', 'Yuma', 'Prescott', 'Lake Havasu City', 'Goodyear',
];


class AzRocScraper extends BaseScraper {
  constructor() {
    super({
      name: 'az_roc_contractors',
      stateCode: 'AZ-ROC',
      baseUrl: 'https://azroc.my.site.com',
      pageSize: 200, // Salesforce list view page size
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() { throw new Error(`${this.name}: search() is overridden`); }
  parseResultsPage() { throw new Error(`${this.name}: search() is overridden`); }
  extractResultCount() { throw new Error(`${this.name}: search() is overridden`); }

  /**
   * POST to the Salesforce Aura API endpoint.
   * All requests go through this method.
   */
  _auraPost(message, rateLimiter) {
    return new Promise((resolve, reject) => {
      const body = new URLSearchParams({
        'message': JSON.stringify(message),
        'aura.context': AURA_CONTEXT,
        'aura.pageURI': '/AZRoc/s/contractor-search',
        'aura.token': 'undefined', // Guest user token
      }).toString();

      const url = new URL(AURA_URL);
      const options = {
        hostname: url.hostname,
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            const parsed = JSON.parse(data);
            resolve(parsed);
          } catch (err) {
            reject(new Error(`Failed to parse Aura response: ${err.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Aura request timed out')); });
      req.write(body);
      req.end();
    });
  }

  /**
   * Fetch a page of License records using the list data provider.
   * Returns up to pageSize records per page, max 2000 total (10 pages at 200).
   */
  async _getListPage(page, rateLimiter) {
    const message = {
      actions: [{
        id: `${page};a`,
        descriptor: 'serviceComponent://ui.force.components.controllers.lists.selectableListDataProvider.SelectableListDataProviderController/ACTION$getItems',
        callingDescriptor: 'UNKNOWN',
        params: {
          entityNameOrId: 'License__c',
          layoutType: 'FULL',
          pageSize: this.pageSize,
          currentPage: page,
          useTimeout: false,
          getCount: page === 0, // Only get count on first page
          enableRowActions: false,
        },
      }],
    };

    const response = await this._auraPost(message, rateLimiter);
    const action = response.actions && response.actions.find(a => a.id === `${page};a`);

    if (!action || action.state !== 'SUCCESS') {
      const errMsg = action && action.error && action.error[0]
        ? action.error[0].event.attributes.values.error.message
        : 'Unknown error';
      throw new Error(`Aura getItems failed (page ${page}): ${errMsg}`);
    }

    return action.returnValue;
  }

  /**
   * Fetch a single License record with full Account relationship fields.
   * Used for enrichment when we need phone/email/website.
   */
  async _getRecordDetail(recordId, rateLimiter) {
    const message = {
      actions: [{
        id: `d;a`,
        descriptor: 'aura://RecordUiController/ACTION$getRecordWithFields',
        callingDescriptor: 'UNKNOWN',
        params: {
          recordId,
          fields: LICENSE_FIELDS,
        },
      }],
    };

    const response = await this._auraPost(message, rateLimiter);
    const action = response.actions && response.actions.find(a => a.id === 'd;a');

    if (!action || action.state !== 'SUCCESS') {
      return null;
    }

    return action.returnValue;
  }

  /**
   * Map a list record to a lead object.
   * List records have license info + firm name but no phone/email/address.
   */
  _mapListRecord(record) {
    const licenseName = record.Name || '';
    const licenseType = record.License_Type__c || '';
    const subtype = record.license_subtype__c || '';
    const status = record.Status__c || '';
    const licensee = record.Primary_Licensee__r || {};
    const firmName = licensee.Name || '';
    const licenseeId = record.Primary_Licensee__c || '';

    // Extract ROC number from "ROC 123456" format
    const rocMatch = licenseName.match(/ROC\s*(\d+)/i);
    const licenseNumber = rocMatch ? rocMatch[1] : licenseName;

    // Extract classification code from subtype (e.g., "CR-42 Roofing" -> "CR-42")
    const classMatch = subtype.match(/^([A-Z]+-?\d*[A-Z]?)\s/);
    const classCode = classMatch ? classMatch[1] : '';

    return {
      first_name: '',
      last_name: '',
      firm_name: firmName,
      city: '',
      state: 'AZ',
      phone: '',
      email: '',
      website: '',
      bar_number: licenseNumber,
      bar_status: status,
      license_type: licenseType,
      license_subtype: subtype,
      classification_code: classCode,
      source: 'az_roc_contractors',
      profile_url: `https://azroc.my.site.com/AZRoc/s/contractor-search?licenseId=${record.Id}`,
      _licenseeId: licenseeId, // Internal: for enrichment
      _recordId: record.Id,    // Internal: for enrichment
    };
  }

  /**
   * Enrich a lead with Account data (phone, email, website, address).
   * Fetches the License record with relationship fields to get Account data.
   */
  async _enrichWithDetail(lead, rateLimiter) {
    if (!lead._recordId) return lead;

    try {
      const detail = await this._getRecordDetail(lead._recordId, rateLimiter);
      if (!detail || !detail.fields) return lead;

      const fields = detail.fields;
      const acct = fields.Primary_Licensee__r && fields.Primary_Licensee__r.value
        ? fields.Primary_Licensee__r.value.fields
        : null;

      if (acct) {
        if (acct.Phone && acct.Phone.value) lead.phone = acct.Phone.value;
        if (acct.Email__c && acct.Email__c.value) lead.email = acct.Email__c.value;
        if (acct.Website && acct.Website.value) lead.website = acct.Website.value;
        if (acct.BillingCity && acct.BillingCity.value) lead.city = acct.BillingCity.value;
        if (acct.BillingState && acct.BillingState.value) lead.state = acct.BillingState.value;
        if (acct.BillingPostalCode && acct.BillingPostalCode.value) lead.zip = acct.BillingPostalCode.value;
        if (acct.BillingStreet && acct.BillingStreet.value) lead.address = acct.BillingStreet.value;
      }
    } catch (err) {
      log.warn(`AZ-ROC enrichment failed for ${lead.bar_number}: ${err.message}`);
    }

    return lead;
  }

  /**
   * Check if a license subtype matches the requested practice area filter.
   * Matches by classification code prefix.
   */
  _matchesClassification(subtype, practiceCode) {
    if (!practiceCode) return true;
    if (!subtype) return false;

    const subtypeUpper = subtype.toUpperCase();
    const codeUpper = practiceCode.toUpperCase();

    // Match by prefix: "B" matches "B General Residential Contractor" and "B-2 General Small Commercial"
    // "CR-42" matches "CR-42 Roofing" exactly
    // "R-37" matches "R-37 Plumbing, Including Solar" and "R-37R Plumbing"
    if (subtypeUpper.startsWith(codeUpper + ' ') || subtypeUpper.startsWith(codeUpper + 'R ')) {
      return true;
    }

    // Also check if the subtype contains the code followed by a space
    // e.g., for "B" matching "B General Residential Contractor"
    if (codeUpper.length <= 2 && subtypeUpper.startsWith(codeUpper + ' ')) {
      return true;
    }

    return false;
  }

  /**
   * Check if a record's city matches the city filter (case-insensitive).
   */
  _matchesCity(recordCity, filterCity) {
    if (!filterCity) return true;
    if (!recordCity) return false;
    const rc = recordCity.toLowerCase().trim();
    const fc = filterCity.toLowerCase().trim();
    return rc === fc || rc.includes(fc) || fc.includes(rc);
  }

  /**
   * Override transformResult to use contractor source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'az_roc_contractors';
    lead.practice_area = practiceArea || lead.classification_code || '';
    // Remove internal fields before yielding
    delete lead._licenseeId;
    delete lead._recordId;
    return lead;
  }

  /**
   * Main search generator — fetches License records via Salesforce Aura API.
   *
   * Approach:
   * 1. Paginate through License__c list view (up to 2000 records, 200/page)
   * 2. Filter by classification code (practice area) and status (Active)
   * 3. Optionally enrich each record with Account data (phone/email/address)
   * 4. Filter by city if requested
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape(`Arizona ROC Contractor Search — ${practiceArea || 'all trades'}`);
    if (practiceCode) {
      log.info(`Classification filter: ${practiceCode}`);
    }

    yield { _cityProgress: { current: 1, total: 1 } };

    // Determine page limits
    const maxPages = options.maxPages || 10; // 10 pages x 200 = 2000 max
    const enrichDetail = !options.maxPages || options.maxPages > 2; // Skip enrichment in test mode

    let totalCount = 0;
    let yieldedCount = 0;
    let activeCount = 0;
    let matchedClassification = 0;
    let matchedCity = 0;
    let enrichedCount = 0;
    let withPhone = 0;
    let withEmail = 0;

    for (let page = 0; page < maxPages; page++) {
      log.info(`Fetching page ${page + 1}/${maxPages} (records ${page * this.pageSize + 1}-${(page + 1) * this.pageSize})`);

      let pageData;
      try {
        await rateLimiter.wait();
        pageData = await this._getListPage(page, rateLimiter);
      } catch (err) {
        log.error(`Failed to fetch page ${page}: ${err.message}`);
        // If first page fails, abort. Otherwise, stop pagination.
        if (page === 0) {
          log.error('AZ-ROC: First page failed — cannot proceed');
          return;
        }
        break;
      }

      if (page === 0 && pageData.totalCount) {
        totalCount = pageData.totalCount;
        const pages = Math.ceil(totalCount / this.pageSize);
        log.success(`AZ-ROC: ${totalCount.toLocaleString()} records available (${pages} pages, limited to ${maxPages})`);
      }

      const results = pageData.result || [];
      if (results.length === 0) {
        log.info(`Page ${page + 1} empty — stopping pagination`);
        break;
      }

      log.info(`Page ${page + 1}: ${results.length} records`);

      for (const item of results) {
        const record = item.record;
        if (!record) continue;

        // Filter by status — only Active licenses by default
        const status = record.Status__c || '';
        if (status !== 'Active') continue;
        activeCount++;

        // Filter by classification code
        const subtype = record.license_subtype__c || '';
        if (!this._matchesClassification(subtype, practiceCode)) continue;
        matchedClassification++;

        // Map to lead object
        const lead = this._mapListRecord(record);

        // Enrich with Account data if not in test mode
        if (enrichDetail) {
          try {
            await rateLimiter.wait();
            await this._enrichWithDetail(lead, rateLimiter);
            enrichedCount++;
          } catch (err) {
            log.warn(`Enrichment failed for ${lead.bar_number}: ${err.message}`);
          }
        }

        // Filter by city (after enrichment, since city comes from Account)
        if (options.city && !this._matchesCity(lead.city, options.city)) continue;
        matchedCity++;

        // Track stats
        if (lead.phone) withPhone++;
        if (lead.email) withEmail++;

        yield this.transformResult(lead, practiceArea || lead.classification_code);
        yieldedCount++;
      }
    }

    log.success([
      `AZ-ROC scrape complete:`,
      `${totalCount.toLocaleString()} total in list view`,
      `${activeCount} active`,
      matchedClassification !== activeCount ? `${matchedClassification} matched classification` : '',
      `${yieldedCount} yielded`,
      enrichedCount > 0 ? `(${enrichedCount} enriched)` : '',
      withPhone > 0 ? `(${withPhone} with phone)` : '',
      withEmail > 0 ? `(${withEmail} with email)` : '',
    ].filter(Boolean).join(' | '));
  }
}

module.exports = new AzRocScraper();
