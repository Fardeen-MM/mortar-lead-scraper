/**
 * Jurisdiction Metadata — code-to-name lookup for US, Canada, UK
 */

const US_STATES = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas',
  CA: 'California', CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware',
  DC: 'District of Columbia', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii',
  ID: 'Idaho', IL: 'Illinois', IN: 'Indiana', IA: 'Iowa',
  KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine',
  MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota',
  MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska',
  NV: 'Nevada', NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico',
  NY: 'New York', NC: 'North Carolina', ND: 'North Dakota', OH: 'Ohio',
  OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island',
  SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas',
  UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington',
  WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
};

const CA_PROVINCES = {
  'CA-AB': 'Alberta',
  'CA-BC': 'British Columbia',
  'CA-MB': 'Manitoba',
  'CA-NB': 'New Brunswick',
  'CA-NL': 'Newfoundland and Labrador',
  'CA-NS': 'Nova Scotia',
  'CA-NT': 'Northwest Territories',
  'CA-NU': 'Nunavut',
  'CA-ON': 'Ontario',
  'CA-PE': 'Prince Edward Island',
  'CA-QC': 'Quebec',
  'CA-SK': 'Saskatchewan',
  'CA-YT': 'Yukon',
};

const UK_JURISDICTIONS = {
  'UK-EW': 'England & Wales',
  'UK-EW-BAR': 'England & Wales (Barristers)',
  'UK-SC': 'Scotland',
  'UK-NI': 'Northern Ireland',
};

const AU_STATES = {
  'AU-NSW': 'New South Wales',
  'AU-VIC': 'Victoria',
  'AU-QLD': 'Queensland',
  'AU-WA': 'Western Australia',
  'AU-SA': 'South Australia',
  'AU-TAS': 'Tasmania',
  'AU-NT': 'Northern Territory',
  'AU-ACT': 'Australian Capital Territory',
};

const EU_JURISDICTIONS = {
  'FR': 'France',
  'DE-BRAK': 'Germany',
  'IE': 'Ireland',
  'NL-EU': 'Netherlands',
  'IT': 'Italy',
  'ES': 'Spain',
};

const INTL_JURISDICTIONS = {
  'NZ': 'New Zealand',
  'IN-DL': 'India (Delhi)',
  'IN-MH': 'India (Maharashtra)',
  'SG': 'Singapore',
  'HK': 'Hong Kong',
  'ZA': 'South Africa',
};

// US-wide lawyer directories (not tied to a single state)
const DIRECTORY_JURISDICTIONS = {
  'MARTINDALE': 'Martindale.com (US-wide)',
  'LAWYERS-COM': 'Lawyers.com (US-wide)',
};

// Combined lookup
const ALL_JURISDICTIONS = { ...US_STATES, ...CA_PROVINCES, ...UK_JURISDICTIONS, ...AU_STATES, ...EU_JURISDICTIONS, ...INTL_JURISDICTIONS, ...DIRECTORY_JURISDICTIONS };

// Country mapping
const COUNTRY_MAP = {};
for (const code of Object.keys(US_STATES)) COUNTRY_MAP[code] = 'US';
for (const code of Object.keys(CA_PROVINCES)) COUNTRY_MAP[code] = 'CA';
for (const code of Object.keys(UK_JURISDICTIONS)) COUNTRY_MAP[code] = 'UK';
for (const code of Object.keys(AU_STATES)) COUNTRY_MAP[code] = 'AU';
for (const code of Object.keys(EU_JURISDICTIONS)) COUNTRY_MAP[code] = 'EU';
for (const code of Object.keys(INTL_JURISDICTIONS)) COUNTRY_MAP[code] = 'INTL';
for (const code of Object.keys(DIRECTORY_JURISDICTIONS)) COUNTRY_MAP[code] = 'US';

function getStateName(code) {
  return ALL_JURISDICTIONS[code] || code;
}

function getCountry(code) {
  return COUNTRY_MAP[code] || 'US';
}

// Backward compatibility
const STATES = ALL_JURISDICTIONS;

module.exports = { STATES, US_STATES, CA_PROVINCES, UK_JURISDICTIONS, AU_STATES, EU_JURISDICTIONS, INTL_JURISDICTIONS, DIRECTORY_JURISDICTIONS, ALL_JURISDICTIONS, COUNTRY_MAP, getStateName, getCountry };
