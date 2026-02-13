/**
 * Normalizer — clean and normalize lead data for dedup matching
 *
 * Strips legal suffixes from firm names, normalizes phones to digits,
 * extracts domains from URLs, lowercases everything.
 */

const LEGAL_SUFFIXES = [
  'llc', 'llp', 'pa', 'pllc', 'pc', 'plc', 'inc', 'corp', 'ltd',
  'law firm', 'law group', 'law office', 'law offices',
  'attorney at law', 'attorneys at law',
  '& associates', 'and associates',
  'legal group', 'legal services', 'legal',
  'law center', 'law practice',
  'professional association', 'professional corporation',
];

// Sort longest first so "law offices" matches before "law"
const SORTED_SUFFIXES = LEGAL_SUFFIXES.sort((a, b) => b.length - a.length);

function normalizeFirmName(name) {
  if (!name) return '';
  let clean = name.toLowerCase().trim();
  // Strip punctuation except spaces and alphanumeric
  clean = clean.replace(/[.,'"]/g, '');
  // Strip legal suffixes (repeat to catch nested like "Smith Law Firm LLC")
  for (let i = 0; i < 2; i++) {
    for (const suffix of SORTED_SUFFIXES) {
      // Match suffix at end or preceded by space/comma
      const re = new RegExp(`\\s*\\b${suffix.replace(/[&]/g, '\\&')}\\s*$`, 'i');
      clean = clean.replace(re, '');
    }
  }
  // Collapse whitespace
  clean = clean.replace(/\s+/g, ' ').trim();
  return clean;
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Strip everything except digits
  const digits = phone.replace(/\D/g, '');
  // Remove leading 1 (US country code) if 11 digits
  if (digits.length === 11 && digits.startsWith('1')) {
    return digits.slice(1);
  }
  return digits;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let clean = url.toLowerCase().trim();
    if (!clean.startsWith('http')) clean = 'https://' + clean;
    const parsed = new URL(clean);
    // Strip www. prefix
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    // Fallback: try to extract domain from string
    return url.toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0]
      .trim();
  }
}

function normalizeName(name) {
  if (!name) return '';
  return name.toLowerCase().trim().replace(/\s+/g, ' ');
}

function normalizeCity(city) {
  if (!city) return '';
  return city.toLowerCase().trim()
    .replace(/\./g, '')      // "St." -> "St"
    .replace(/\s+/g, ' ');
}

function normalizeState(state) {
  if (!state) return '';
  return state.toUpperCase().trim().slice(0, 2);
}

/**
 * Build a normalized record for matching purposes.
 * Keeps original data intact, adds normalized_ fields.
 */
function normalizeRecord(record) {
  return {
    ...record,
    _norm: {
      firm: normalizeFirmName(record.firm_name || record.company || ''),
      firstName: normalizeName(record.first_name || ''),
      lastName: normalizeName(record.last_name || ''),
      city: normalizeCity(record.city || ''),
      state: normalizeState(record.state || ''),
      phone: normalizePhone(record.phone || ''),
      domain: extractDomain(record.website || ''),
      email: (record.email || '').toLowerCase().trim(),
    }
  };
}

module.exports = {
  normalizeFirmName,
  normalizePhone,
  extractDomain,
  normalizeName,
  normalizeCity,
  normalizeState,
  normalizeRecord,
};
