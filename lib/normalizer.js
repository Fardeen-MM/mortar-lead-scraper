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
      const re = new RegExp(`\\s*(?:\\b|(?<=\\s))${suffix.replace(/[&]/g, '\\&')}\\s*$`, 'i');
      clean = clean.replace(re, '');
    }
  }
  // Collapse whitespace
  clean = clean.replace(/\s+/g, ' ').trim();
  return clean;
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Strip phone extensions before normalizing (ext, x, #, or repeated trailing digits)
  let noExt = String(phone).replace(/\s*(ext\.?|x|#)\s*\d+\s*$/i, '');
  // Strip repeated trailing digits e.g. "(505) 483-1856       1856" → "(505) 483-1856"
  noExt = noExt.replace(/(\d{4})\s+\1\s*$/, '$1');
  // Strip everything except digits and leading +
  const cleaned = noExt.replace(/[^\d+]/g, '');
  // UK: strip +44 prefix, handle 0 prefix
  if (cleaned.startsWith('+44')) {
    return cleaned.slice(3).replace(/^0/, '');
  }
  if (cleaned.startsWith('44') && cleaned.length === 12) {
    return cleaned.slice(2).replace(/^0/, '');
  }
  // Australia: strip +61 prefix
  if (cleaned.startsWith('+61')) {
    return cleaned.slice(3).replace(/^0/, '');
  }
  if (cleaned.startsWith('61') && cleaned.length === 11) {
    return cleaned.slice(2).replace(/^0/, '');
  }
  // France: strip +33 prefix
  if (cleaned.startsWith('+33')) {
    return cleaned.slice(3).replace(/^0/, '');
  }
  if (cleaned.startsWith('33') && cleaned.length === 11) {
    return cleaned.slice(2).replace(/^0/, '');
  }
  // Germany: strip +49 prefix
  if (cleaned.startsWith('+49')) {
    return cleaned.slice(3).replace(/^0/, '');
  }
  // Ireland: strip +353 prefix
  if (cleaned.startsWith('+353')) {
    return cleaned.slice(4).replace(/^0/, '');
  }
  // New Zealand: strip +64 prefix
  if (cleaned.startsWith('+64')) {
    return cleaned.slice(3).replace(/^0/, '');
  }
  // Singapore: strip +65 prefix
  if (cleaned.startsWith('+65')) {
    return cleaned.slice(3);
  }
  // Hong Kong: strip +852 prefix
  if (cleaned.startsWith('+852')) {
    return cleaned.slice(4);
  }
  const digits = cleaned.replace(/\D/g, '');
  // Reject too-short phones (< 7 digits is not a valid number)
  if (digits.length < 7) return '';
  // US/CA: Remove leading 1 (country code) if 11 digits
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
  const upper = state.toUpperCase().trim();
  // Preserve hyphenated codes like CA-ON, UK-EW, AU-NSW, DE-BRAK
  if (upper.includes('-')) return upper;
  // Preserve short international codes (FR, IE, NZ, SG, HK, IT, ES)
  if (upper.length <= 2) return upper;
  // For longer strings without hyphens, try to extract a 2-letter code
  return upper.slice(0, 2);
}

/**
 * Smart title-case for names from ALL CAPS data sources.
 * Handles Mc/Mac prefixes, apostrophes, hyphens, multi-word names.
 *
 *   "MCDONALD"    → "McDonald"
 *   "O'BRIEN"     → "O'Brien"
 *   "DE LA CRUZ"  → "De La Cruz"
 *   "VAN DER BERG"→ "Van Der Berg"
 *   "SMITH-JONES" → "Smith-Jones"
 *   "MACARTHUR"   → "MacArthur"
 */
function titleCase(str) {
  if (!str) return '';
  const s = str.trim();
  if (!s) return '';

  // Title-case a single word (no spaces/hyphens)
  function capWord(w) {
    if (!w) return w;
    const lower = w.toLowerCase();

    // Mc prefix: McD...
    if (lower.length > 2 && lower.startsWith('mc')) {
      return 'Mc' + lower.charAt(2).toUpperCase() + lower.slice(3);
    }

    // Mac prefix: only for 5+ letter words to avoid mangling "Macy", "Mace", etc.
    // Common Mac names: MacArthur, MacDonald, MacGregor, MacKenzie, etc.
    if (lower.length >= 5 && lower.startsWith('mac') && /^mac[a-z]/.test(lower)) {
      const afterMac = lower.slice(3);
      // Avoid false positives: mache, machine, macro, macon, macy, etc.
      const macExceptions = ['he', 'hi', 'ho', 'hu', 'ro', 'on', 'e', 'ula', 'ram', 'omb', 'key', 'hin'];
      const isFalsePositive = macExceptions.some(ex => afterMac.startsWith(ex));
      if (!isFalsePositive) {
        return 'Mac' + afterMac.charAt(0).toUpperCase() + afterMac.slice(1);
      }
    }

    // O' prefix: O'Brien, O'Connor, O'Reilly
    if (lower.length > 2 && (w[0] === 'O' || w[0] === 'o') && w[1] === "'") {
      return "O'" + lower.charAt(2).toUpperCase() + lower.slice(3);
    }

    // Default: capitalize first letter
    return lower.charAt(0).toUpperCase() + lower.slice(1);
  }

  // Split on spaces, then handle hyphens within each part
  return s.split(/\s+/).map(part => {
    // Handle hyphenated names: "SMITH-JONES" → "Smith-Jones"
    if (part.includes('-')) {
      return part.split('-').map(capWord).join('-');
    }
    return capWord(part);
  }).join(' ');
}

/**
 * Build a normalized record for matching purposes.
 * Keeps original data intact, adds normalized_ fields.
 */
function normalizeRecord(record) {
  if (!record) record = {};
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

// ---- Trade / niche normalization for contractor leads ----

const TRADE_MAP = {
  // General Contractor
  'general contractor': 'General Contractor', 'general building': 'General Contractor',
  'general engineering': 'General Contractor', 'general': 'General Contractor',
  'building': 'General Contractor', 'entrepreneur general': 'General Contractor',
  'general builder': 'General Contractor', 'cbc': 'General Contractor', 'cgc': 'General Contractor',
  'b': 'General Contractor',

  // Roofing
  'roofing': 'Roofing', 'roofer': 'Roofing', 'couvreur': 'Roofing',
  'crc': 'Roofing', 'c-39': 'Roofing', 'roofng': 'Roofing',

  // Painting
  'painting': 'Painting', 'painting and decorating': 'Painting', 'painter': 'Painting',
  'peintre': 'Painting', 'c-33': 'Painting', 'painting/wallcovering': 'Painting',

  // Plumbing
  'plumbing': 'Plumbing', 'plumber': 'Plumbing', 'plombier': 'Plumbing',
  'cpc': 'Plumbing', 'c-36': 'Plumbing', 'pc': 'Plumbing', 'rp': 'Plumbing',
  'master plumber': 'Plumbing', 'journeyman plumber': 'Plumbing',

  // HVAC
  'hvac': 'HVAC', 'heating': 'HVAC', 'air conditioning': 'HVAC',
  'warm-air heating': 'HVAC', 'chauffage': 'HVAC', 'cmc': 'HVAC',
  'c-20': 'HVAC', 'hvac/rfrg': 'HVAC', 'a/c and refrigeration': 'HVAC',
  'a/c and refrigeration contractor': 'HVAC', 'a/c and refrigeration technician': 'HVAC',
  'refrigeration': 'HVAC',

  // Electrical
  'electrical': 'Electrical', 'electrician': 'Electrical', 'electricien': 'Electrical',
  'ec': 'Electrical', 'c-10': 'Electrical', 'master electrician': 'Electrical',
  'journeyman electrician': 'Electrical', 'electrical contractor': 'Electrical',
  'residential wireman': 'Electrical', 'maintenance electrician': 'Electrical',
  'residential': 'Electrical', 'commercial': 'Electrical',

  // Landscaping
  'landscaping': 'Landscaping', 'landscaper': 'Landscaping', 'landscape': 'Landscaping',
  'c-27': 'Landscaping', 'landscp': 'Landscaping',

  // Solar
  'solar': 'Solar', 'c-46': 'Solar', 'solar/renewable': 'Solar',

  // Remodeling
  'remodeling': 'Remodeling', 'residential remodeling': 'Remodeling',
  'renovation': 'Remodeling', 'renovator': 'Remodeling', 'home improvement': 'Remodeling',
  'kitchen': 'Remodeling', 'bathroom': 'Remodeling', 'kitchen fitting': 'Remodeling',
  'bathroom fitting': 'Remodeling',

  // Concrete
  'concrete': 'Concrete', 'c-8': 'Concrete',

  // Fencing
  'fencing': 'Fencing', 'c-13': 'Fencing',

  // Flooring
  'flooring': 'Flooring', 'c-15': 'Flooring',

  // Tile
  'tile': 'Tile', 'c-54': 'Tile',

  // Framing / Carpentry
  'framing': 'Carpentry', 'carpentry': 'Carpentry', 'cabinet': 'Carpentry',
  'charpentier': 'Carpentry',

  // Masonry
  'masonry': 'Masonry', 'maconnerie': 'Masonry',

  // Insulation
  'insulation': 'Insulation', 'c-2': 'Insulation',

  // Drywall
  'drywall': 'Drywall', 'c-9': 'Drywall',

  // Excavation
  'excavation': 'Excavation', 'earthwork': 'Excavation', 'c-12': 'Excavation',

  // Demolition
  'demolition': 'Demolition',

  // Glazing / Windows
  'glazing': 'Windows & Doors', 'glass': 'Windows & Doors', 'window': 'Windows & Doors',

  // Sheet Metal
  'sheet metal': 'Sheet Metal', 'c-43': 'Sheet Metal',

  // Tree Service
  'tree service': 'Tree Service', 'tree': 'Tree Service', 'c-61/d-49': 'Tree Service',

  // Swimming Pool
  'swimming pool': 'Pool & Spa', 'pool': 'Pool & Spa', 'ccc': 'Pool & Spa',

  // Fire Protection
  'fire protection': 'Fire Protection', 'fire alarm': 'Fire Protection',

  // Elevator
  'elevator': 'Elevator',

  // Sign
  'sign': 'Signage',

  // Low Voltage / Security
  'low voltage': 'Low Voltage', 'security': 'Low Voltage',

  // Welding
  'welding': 'Welding',
};

/**
 * Normalize a trade/practice area string to a standard category.
 * Returns a clean, filterable trade category like "Roofing", "Plumbing", etc.
 *
 *   normalizeTrade('C-39')                → 'Roofing'
 *   normalizeTrade('painting and decorating') → 'Painting'
 *   normalizeTrade('Master Electrician')   → 'Electrical'
 *   normalizeTrade('couvreur')             → 'Roofing'
 */
function normalizeTrade(trade) {
  if (!trade) return '';
  const key = trade.toLowerCase().trim();
  // Direct match
  if (TRADE_MAP[key]) return TRADE_MAP[key];
  // Partial match — check if any map key is contained in the input
  for (const [pattern, category] of Object.entries(TRADE_MAP)) {
    if (key.includes(pattern) || pattern.includes(key)) return category;
  }
  // Title case the original if no match
  return trade.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

module.exports = {
  titleCase,
  normalizeFirmName,
  normalizePhone,
  extractDomain,
  normalizeName,
  normalizeCity,
  normalizeState,
  normalizeRecord,
  normalizeTrade,
};
