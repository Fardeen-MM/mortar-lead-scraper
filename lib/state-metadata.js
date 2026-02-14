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

// Combined lookup
const ALL_JURISDICTIONS = { ...US_STATES, ...CA_PROVINCES, ...UK_JURISDICTIONS };

// Country mapping
const COUNTRY_MAP = {};
for (const code of Object.keys(US_STATES)) COUNTRY_MAP[code] = 'US';
for (const code of Object.keys(CA_PROVINCES)) COUNTRY_MAP[code] = 'CA';
for (const code of Object.keys(UK_JURISDICTIONS)) COUNTRY_MAP[code] = 'UK';

function getStateName(code) {
  return ALL_JURISDICTIONS[code] || code;
}

function getCountry(code) {
  return COUNTRY_MAP[code] || 'US';
}

// Backward compatibility
const STATES = ALL_JURISDICTIONS;

module.exports = { STATES, US_STATES, CA_PROVINCES, UK_JURISDICTIONS, ALL_JURISDICTIONS, COUNTRY_MAP, getStateName, getCountry };
