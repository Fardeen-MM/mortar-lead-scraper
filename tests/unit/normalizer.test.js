const {
  normalizeFirmName,
  normalizePhone,
  extractDomain,
  normalizeName,
  normalizeCity,
  normalizeState,
  normalizeRecord,
} = require('../../lib/normalizer');

describe('normalizeFirmName', () => {
  test('strips LLC suffix', () => {
    expect(normalizeFirmName('Smith & Jones LLC')).toBe('smith & jones');
  });

  test('strips nested suffixes', () => {
    expect(normalizeFirmName('Smith Law Firm LLC')).toBe('smith');
  });

  test('strips LLP', () => {
    expect(normalizeFirmName('Baker McKenzie LLP')).toBe('baker mckenzie');
  });

  test('handles empty/null', () => {
    expect(normalizeFirmName('')).toBe('');
    expect(normalizeFirmName(null)).toBe('');
    expect(normalizeFirmName(undefined)).toBe('');
  });

  test('collapses whitespace', () => {
    expect(normalizeFirmName('Smith   &   Jones')).toBe('smith & jones');
  });
});

describe('normalizePhone', () => {
  test('strips formatting from US number', () => {
    expect(normalizePhone('(305) 555-1234')).toBe('3055551234');
  });

  test('removes US country code', () => {
    expect(normalizePhone('+1 (305) 555-1234')).toBe('3055551234');
    expect(normalizePhone('13055551234')).toBe('3055551234');
  });

  test('handles Canadian numbers same as US', () => {
    expect(normalizePhone('+1 (416) 555-1234')).toBe('4165551234');
  });

  test('handles UK numbers with +44', () => {
    expect(normalizePhone('+44 20 7946 0958')).toBe('2079460958');
  });

  test('handles UK numbers with 44 prefix', () => {
    expect(normalizePhone('442079460958')).toBe('2079460958');
  });

  test('handles empty/null', () => {
    expect(normalizePhone('')).toBe('');
    expect(normalizePhone(null)).toBe('');
  });
});

describe('normalizeState', () => {
  test('uppercases and trims 2-letter US code', () => {
    expect(normalizeState('fl')).toBe('FL');
    expect(normalizeState('  ny  ')).toBe('NY');
  });

  test('preserves hyphenated Canadian codes', () => {
    expect(normalizeState('CA-ON')).toBe('CA-ON');
    expect(normalizeState('ca-on')).toBe('CA-ON');
  });

  test('preserves hyphenated UK codes', () => {
    expect(normalizeState('UK-EW')).toBe('UK-EW');
    expect(normalizeState('uk-sc')).toBe('UK-SC');
    expect(normalizeState('UK-EW-BAR')).toBe('UK-EW-BAR');
  });

  test('truncates long non-hyphenated to 2 chars', () => {
    expect(normalizeState('Florida')).toBe('FL');
  });

  test('handles empty/null', () => {
    expect(normalizeState('')).toBe('');
    expect(normalizeState(null)).toBe('');
  });
});

describe('extractDomain', () => {
  test('extracts from URL', () => {
    expect(extractDomain('https://www.smithlaw.com/about')).toBe('smithlaw.com');
  });

  test('strips www prefix', () => {
    expect(extractDomain('http://www.example.com')).toBe('example.com');
  });

  test('handles bare domain', () => {
    expect(extractDomain('smithlaw.com')).toBe('smithlaw.com');
  });

  test('handles empty/null', () => {
    expect(extractDomain('')).toBe('');
    expect(extractDomain(null)).toBe('');
  });
});

describe('normalizeCity', () => {
  test('lowercases and strips periods', () => {
    expect(normalizeCity('St. Petersburg')).toBe('st petersburg');
  });

  test('handles empty', () => {
    expect(normalizeCity('')).toBe('');
  });
});

describe('normalizeRecord', () => {
  test('creates _norm object with all fields', () => {
    const record = {
      first_name: 'John',
      last_name: 'Smith',
      firm_name: 'Smith Law LLC',
      city: 'Miami',
      state: 'FL',
      phone: '(305) 555-1234',
      website: 'https://www.smithlaw.com',
      email: 'John@SmithLaw.com',
    };
    const result = normalizeRecord(record);
    expect(result._norm.firstName).toBe('john');
    expect(result._norm.lastName).toBe('smith');
    expect(result._norm.firm).toBe('smith law');
    expect(result._norm.city).toBe('miami');
    expect(result._norm.state).toBe('FL');
    expect(result._norm.phone).toBe('3055551234');
    expect(result._norm.domain).toBe('smithlaw.com');
    expect(result._norm.email).toBe('john@smithlaw.com');
  });
});
