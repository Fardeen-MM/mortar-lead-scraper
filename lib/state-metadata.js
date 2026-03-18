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
  'CA-CICC': 'CICC Registry (All Canada)',
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

// Directories (worldwide or US-wide, not tied to a single state)
const DIRECTORY_JURISDICTIONS = {
  'MARTINDALE': 'Martindale.com (US-wide)',
  'LAWYERS-COM': 'Lawyers.com (US-wide)',
  'GOOGLE-PLACES': 'Google Places (Worldwide)',
  'GOOGLE-MAPS': 'Google Maps (Worldwide, Free)',
  'JUSTIA': 'Justia.com (US-wide)',
  'AVVO': 'Avvo.com (US-wide)',
  'FINDLAW': 'FindLaw.com (US-wide)',
  'GOOGLE-ADS': 'Google Ads Transparency (Worldwide)',
  'LINKEDIN-ADS': 'LinkedIn Ad Library (Worldwide)',
};

// Contractor licensing boards (home services)
const CONTRACTOR_JURISDICTIONS = {
  'CSLB': 'California CSLB (700K+ Contractors)',
  'CT-HIC': 'Connecticut HIC (Socrata API)',
  'DBPR': 'Florida DBPR (Contractors w/ Email)',
  'TDLR': 'Texas TDLR (949K Licenses)',
  'WA-LNI': 'Washington L&I (160K Contractors)',
  'NYC-HIC': 'NYC Home Improvement (69K)',
  'RBQ': 'Quebec RBQ (All Trades w/ Email)',
  'ESA': 'Ontario ESA (10K+ Electricians w/ Email)',
  'TRUSTMARK': 'TrustMark UK (18K Trades, API)',
  'ATLANTIC-HW': 'Atlantic Home Warranty (NS/NB/PE/NL)',
  'HOUZZ': 'Houzz (US-wide, 2M+ Pros)',
  'BBB': 'BBB (US/CA, Emails)',
  'BUILDZOOM': 'BuildZoom (US-wide, 4M+ Contractors)',
  'MANTA': 'Manta.com (US, Emails)',
  'YELP': 'Yelp (US-wide, Millions of Home Service Pros)',
  'ANGI': 'Angi (US-wide, 200K+ Service Providers)',
  'PA-HIC': 'Pennsylvania HIC (50K+ Contractors)',
  'NJ-HIC': 'New Jersey HIC (30K+ Contractors)',
  'GAS-SAFE': 'Gas Safe Register UK (130K)',
  'AZ-ROC': 'Arizona ROC (40K+ Contractors)',
  'OR-CCB': 'Oregon CCB (55K+ Contractors)',
  'NC-GC': 'North Carolina NCLBGC (General Contractors)',
  'SC-GC': 'South Carolina CLB (35 Trade Classifications)',
  'CHECKATRADE': 'Checkatrade UK (60K+ Vetted Trades)',
  'WHICH-TRADERS': 'Which? Trusted Traders UK (Endorsed Trades)',
  'FMB': 'FMB UK (6,200 Vetted Builders)',
  'NICEIC': 'NICEIC UK (40K+ Registered Electricians)',
};

// Combined lookup
const ALL_JURISDICTIONS = { ...US_STATES, ...CA_PROVINCES, ...UK_JURISDICTIONS, ...AU_STATES, ...EU_JURISDICTIONS, ...INTL_JURISDICTIONS, ...DIRECTORY_JURISDICTIONS, ...CONTRACTOR_JURISDICTIONS };

// Country mapping
const COUNTRY_MAP = {};
for (const code of Object.keys(US_STATES)) COUNTRY_MAP[code] = 'US';
for (const code of Object.keys(CA_PROVINCES)) COUNTRY_MAP[code] = 'CA';
for (const code of Object.keys(UK_JURISDICTIONS)) COUNTRY_MAP[code] = 'UK';
for (const code of Object.keys(AU_STATES)) COUNTRY_MAP[code] = 'AU';
for (const code of Object.keys(EU_JURISDICTIONS)) COUNTRY_MAP[code] = 'EU';
for (const code of Object.keys(INTL_JURISDICTIONS)) COUNTRY_MAP[code] = 'INTL';
for (const code of Object.keys(DIRECTORY_JURISDICTIONS)) COUNTRY_MAP[code] = 'DIRECTORY';
for (const code of Object.keys(CONTRACTOR_JURISDICTIONS)) COUNTRY_MAP[code] = 'CONTRACTOR';

function getStateName(code) {
  return ALL_JURISDICTIONS[code] || code;
}

function getCountry(code) {
  return COUNTRY_MAP[code] || 'US';
}

// Backward compatibility
const STATES = ALL_JURISDICTIONS;

module.exports = { STATES, US_STATES, CA_PROVINCES, UK_JURISDICTIONS, AU_STATES, EU_JURISDICTIONS, INTL_JURISDICTIONS, DIRECTORY_JURISDICTIONS, CONTRACTOR_JURISDICTIONS, ALL_JURISDICTIONS, COUNTRY_MAP, getStateName, getCountry };
