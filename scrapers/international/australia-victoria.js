/**
 * Victoria (AU-VIC) Legal Services Board + Commissioner — Lawyer Register Scraper
 *
 * Source: https://lsbc.vic.gov.au/register-of-lawyers
 * Method: HTTP GET + Cheerio (Drupal 10 server-rendered HTML)
 *
 * The register uses a GET-based search at:
 *   /register-of-lawyers?query=&type=lawyer&lawyer=&lookup={suburb}&area={practiceArea}&...&start_rank={offset}
 *
 * Pagination uses `start_rank` (1-indexed offset), 10 results per page.
 * ~30,500 individual lawyers registered.
 *
 * Available data per lawyer:
 *   - Name, practising certificate type, firm/employer ("Practising at"),
 *     suburb/postcode address, areas of practice, accredited specialisations,
 *     languages spoken, and any disciplinary action.
 *
 * No individual profile URLs, phone numbers, emails, or bar numbers are exposed
 * in the public register — only the fields listed above.
 */

const BaseScraper = require('../base-scraper');

class VictoriaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-victoria',
      stateCode: 'AU-VIC',
      baseUrl: 'https://lsbc.vic.gov.au/register-of-lawyers',
      pageSize: 10, // The register returns 10 results per page
      practiceAreaCodes: {
        'administrative law':              'Administrative Law',
        'advocacy':                        'Advocacy',
        'alternative dispute resolution':  'Alternative Dispute Resolution',
        'banking':                         'Banking/Finance',
        'banking/finance':                 'Banking/Finance',
        'building':                        'Building/Construction',
        'building/construction':           'Building/Construction',
        'construction':                    'Building/Construction',
        'business':                        'Business/Corporate Law',
        'business/corporate law':          'Business/Corporate Law',
        'corporate':                       'Business/Corporate Law',
        'charity':                         'Charity/Not-For-Profit',
        'not-for-profit':                  'Charity/Not-For-Profit',
        'child protection':                'Child Protection/Children\'s Law',
        'children':                        'Child Protection/Children\'s Law',
        'civil litigation':                'Civil Litigation',
        'commercial':                      'Commercial Law',
        'commercial law':                  'Commercial Law',
        'competition':                     'Competition/Consumer Law',
        'consumer law':                    'Competition/Consumer Law',
        'conveyancing':                    'Conveyancing/Real Property',
        'real property':                   'Conveyancing/Real Property',
        'property':                        'Conveyancing/Real Property',
        'costs':                           'Costs',
        'criminal':                        'Criminal Law',
        'criminal law':                    'Criminal Law',
        'debts':                           'Debts/Insolvency',
        'insolvency':                      'Debts/Insolvency',
        'discrimination':                  'Discrimination/Human Rights',
        'human rights':                    'Discrimination/Human Rights',
        'education':                       'Education',
        'employment':                      'Employment/Industrial Law',
        'industrial law':                  'Employment/Industrial Law',
        'energy':                          'Energy/Resources',
        'resources':                       'Energy/Resources',
        'entertainment':                   'Entertainment/Media/Sports',
        'media':                           'Entertainment/Media/Sports',
        'sports':                          'Entertainment/Media/Sports',
        'environmental':                   'Environmental Law',
        'environmental law':               'Environmental Law',
        'equity':                          'Equity/Trusts/Finance',
        'trusts':                          'Equity/Trusts/Finance',
        'family':                          'Family Law',
        'family law':                      'Family Law',
        'family violence':                 'Family Violence/Statutory Offences',
        'financial services':              'Financial Services/Superannuation',
        'superannuation':                  'Financial Services/Superannuation',
        'government':                      'Government',
        'health':                          'Health/Disability Law',
        'disability':                      'Health/Disability Law',
        'immigration':                     'Immigration Law',
        'immigration law':                 'Immigration Law',
        'information technology':          'Information technology/Telecommunications',
        'telecommunications':              'Information technology/Telecommunications',
        'insurance':                       'Insurance',
        'intellectual property':           'Intellectual Property',
        'international law':               'International Law',
        'it/cybersecurity':                'IT/Cybersecurity',
        'cybersecurity':                   'IT/Cybersecurity',
        'leasing':                         'Leasing Law',
        'leasing law':                     'Leasing Law',
        'legislation':                     'Legislation/legal drafting',
        'legal drafting':                  'Legislation/legal drafting',
        'liquor':                          'Liquor/Gaming/Hospitality Law',
        'gaming':                          'Liquor/Gaming/Hospitality Law',
        'hospitality':                     'Liquor/Gaming/Hospitality Law',
        'litigation':                      'Litigation - general',
        'general litigation':              'Litigation - general',
        'native title':                    'Native Title/Indigenous law',
        'indigenous':                      'Native Title/Indigenous law',
        'personal injury':                 'Personal Injury',
        'planning':                        'Planning/Local Government',
        'local government':                'Planning/Local Government',
        'privacy':                         'Privacy',
        'regulation':                      'Regulation/Compliance/Ethics',
        'compliance':                      'Regulation/Compliance/Ethics',
        'ethics':                          'Regulation/Compliance/Ethics',
        'road':                            'Road/Traffic',
        'traffic':                         'Road/Traffic',
        'small business':                  'Small Business',
        'taxation':                        'Taxation',
        'tax':                             'Taxation',
        'trade practices':                 'Trade Practices Law',
        'transport':                       'Transport/Logistics',
        'logistics':                       'Transport/Logistics',
        'wills':                           'Wills and Estates',
        'estates':                         'Wills and Estates',
        'wills and estates':               'Wills and Estates',
      },
      defaultCities: [
        'Melbourne', 'Geelong', 'Ballarat', 'Bendigo',
        'Frankston', 'Dandenong', 'Ringwood', 'Footscray',
        'Box Hill', 'Sunshine', 'Heidelberg', 'Moorabbin',
        'Werribee', 'Broadmeadows', 'Shepparton', 'Mildura',
        'Warrnambool', 'Traralgon', 'Wangaratta', 'Horsham',
      ],
      maxConsecutiveEmpty: 3,
    });
  }

  /**
   * Build the search URL for the VLSB+C register.
   *
   * The working GET URL format is:
   *   /register-of-lawyers?query=&type=lawyer&lawyer=&lookup={suburb}&area={area}&...&start_rank={offset}
   *
   * - `type=lawyer` restricts results to individual lawyers (not law practices)
   * - `lookup` is the suburb/postcode filter
   * - `area` is the practice area filter (exact string from the register)
   * - `start_rank` is the 1-indexed result offset for pagination
   * - Page size is always 10 results
   */
  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    params.set('query', '');
    params.set('type', 'lawyer');
    params.set('lawyer', '');
    params.set('lookup', city || '');
    params.set('action', '');
    params.set('area', practiceCode || '');
    params.set('language', '');
    params.set('accreditation', '0');
    params.set('page', '0');

    // start_rank is 1-indexed; page 1 = rank 1 (no param needed), page 2 = rank 11, etc.
    if (page > 1) {
      const startRank = (page - 1) * this.pageSize + 1;
      params.set('start_rank', String(startRank));
    }

    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse the VLSB+C search results page with Cheerio.
   *
   * Each lawyer card is a `div.search-content.card` containing:
   *   - h3.mt-5: full name
   *   - span.type: practising certificate type (Barrister, Employee without Trust, etc.)
   *   - "Practising at:" li: firm/employer name
   *   - address tag: "SUBURB VIC POSTCODE" or "POSTCODE AUSTRALIA VIC"
   *   - "Areas of practice:" li: comma-separated practice areas
   *   - "Accredited specialisations:" li: specialisations or "Not applicable"
   *   - "Languages:" li: languages spoken or "Not applicable"
   */
  parseResultsPage($) {
    const attorneys = [];

    $('div.search-content.card').each((_, el) => {
      const $el = $(el);

      // --- Name ---
      const fullName = ($el.find('h3').first().text() || '').trim();
      if (!fullName) return;

      const { firstName, lastName } = this.splitName(fullName);

      // --- Practising certificate type (used as bar_status) ---
      const typeText = $el.find('span.type').text() || '';
      const typeMatch = typeText.match(/Type:\s*(.+)/);
      const lawyerType = typeMatch ? typeMatch[1].trim() : '';

      // --- Firm name ("Practising at:") ---
      let firmName = '';
      $el.find('li').each((_, li) => {
        const text = $(li).text();
        if (text.includes('Practising at:')) {
          firmName = text.replace(/Practising at:\s*/, '').trim();
        }
      });

      // --- Address (suburb, state, postcode) ---
      let city = '';
      let postcode = '';
      const addressText = ($el.find('address').text() || '').trim();
      if (addressText) {
        // Format can be either:
        //   "SUBURB VIC POSTCODE"  (from filtered/lawyer results)
        //   "POSTCODE AUSTRALIA VIC" (from unfiltered GET results)
        const parsed = this._parseVicAddress(addressText);
        city = parsed.city;
        postcode = parsed.postcode;
      }

      // --- Areas of practice ---
      let areasOfPractice = '';
      $el.find('li').each((_, li) => {
        const text = $(li).text();
        if (text.includes('Areas of practice:')) {
          areasOfPractice = text.replace(/Areas of practice:\s*/, '').trim();
        }
      });

      // --- Accredited specialisations ---
      let specialisations = '';
      $el.find('li').each((_, li) => {
        const text = $(li).text();
        if (text.includes('Accredited specialisations:')) {
          const raw = text.replace(/Accredited specialisations:\s*/, '').trim();
          if (raw && raw !== 'Not applicable') {
            specialisations = raw;
          }
        }
      });

      // --- Languages ---
      let languages = '';
      $el.find('li').each((_, li) => {
        const text = $(li).text();
        if (text.includes('Languages:')) {
          const raw = text.replace(/Languages:\s*/, '').trim();
          if (raw && raw !== 'Not applicable') {
            languages = raw;
          }
        }
      });

      // --- Disciplinary action (if present) ---
      let disciplinaryAction = '';
      const disciplinarySection = $el.find('.load-more-content');
      if (disciplinarySection.length) {
        const parts = [];
        disciplinarySection.find('.more-content').each((_, mc) => {
          const mcText = $(mc).text().trim().replace(/\s+/g, ' ');
          if (mcText && !mcText.startsWith('Disciplinary action') && !mcText.startsWith('See')) {
            parts.push(mcText);
          }
        });
        if (parts.length) {
          disciplinaryAction = parts.join(' | ');
        }
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'VIC',
        zip: postcode,
        country: 'Australia',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: lawyerType,
        profile_url: '',
        practice_areas: areasOfPractice,
        specialisations: specialisations,
        languages: languages,
        disciplinary_action: disciplinaryAction,
      });
    });

    return attorneys;
  }

  /**
   * Parse a Victorian address string from the register.
   *
   * Two formats observed:
   *   1. "MELBOURNE VIC 3000" — suburb first, then state and postcode
   *   2. "3000 AUSTRALIA VIC" — postcode first (older/unfiltered results)
   *
   * @param {string} text - The address text from the <address> tag
   * @returns {{ city: string, postcode: string }}
   */
  _parseVicAddress(text) {
    if (!text) return { city: '', postcode: '' };

    const cleaned = text.replace(/AUSTRALIA/gi, '').replace(/\s+/g, ' ').trim();

    // Format 1: "MELBOURNE VIC 3000" or "SOUTH MELBOURNE VIC 3205"
    const format1 = cleaned.match(/^(.+?)\s+VIC\s+(\d{4})$/i);
    if (format1) {
      return {
        city: this._titleCase(format1[1].trim()),
        postcode: format1[2],
      };
    }

    // Format 2: "3000 VIC" (postcode only, no suburb name)
    const format2 = cleaned.match(/^(\d{4})\s+VIC$/i);
    if (format2) {
      return {
        city: '',
        postcode: format2[1],
      };
    }

    // Format 3: "VIC 3000" or just "3000"
    const format3 = cleaned.match(/(\d{4})/);
    if (format3) {
      const beforePostcode = cleaned.replace(/VIC/i, '').replace(format3[1], '').trim();
      return {
        city: beforePostcode ? this._titleCase(beforePostcode) : '',
        postcode: format3[1],
      };
    }

    return { city: cleaned, postcode: '' };
  }

  /**
   * Convert an UPPERCASE string to Title Case.
   * e.g. "SOUTH MELBOURNE" => "South Melbourne"
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  /**
   * Extract total result count from the page.
   *
   * The register displays: "Showing X - Y of Z results"
   * inside a <strong class="title"> element.
   */
  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/Showing\s+[\d,]+\s*-\s*[\d,]+\s+of\s+([\d,]+)\s+results/i);
    if (match) {
      return parseInt(match[1].replace(/,/g, ''), 10);
    }

    // Fallback: look in the strong.title element specifically
    const titleText = $('strong.title').text();
    const titleMatch = titleText.match(/of\s+([\d,]+)\s+results/i);
    if (titleMatch) {
      return parseInt(titleMatch[1].replace(/,/g, ''), 10);
    }

    return 0;
  }
}

module.exports = new VictoriaScraper();
