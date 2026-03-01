/**
 * Industry Enricher — niche-agnostic lead enrichment
 *
 * Consolidates scoring + classification + email pattern generation
 * for non-lawyer leads (dentists, plumbers, accountants, etc.).
 *
 * Features:
 *   1. Decision maker scoring (owner/director/manager/staff)
 *   2. Title inference from Google rating count as size proxy
 *   3. Specialty classification from niche keywords
 *   4. LinkedIn URL construction
 *   5. Email pattern generation (reuses email-verifier patterns)
 *   6. Cross-source lead merging
 */

const { normalizePhone, extractDomain, normalizeFirmName, normalizeCity, titleCase } = require('./normalizer');
const { log } = require('./logger');

// ─── Decision Maker Scoring ────────────────────────────────────────

const DM_SCORES = {
  // Highest value: owners/founders
  owner:        50,
  'co-owner':   50,
  founder:      50,
  'co-founder': 50,
  principal:    50,
  proprietor:   50,
  president:    45,
  ceo:          45,
  chairman:     40,

  // High value: directors/VPs
  director:     35,
  vp:           30,
  'vice president': 30,
  'managing partner': 40,
  partner:      35,
  'general manager': 30,

  // Medium value: managers
  manager:      20,
  supervisor:   15,
  lead:         15,
  head:         20,
  chief:        25,
  coordinator:  10,

  // Lower value: staff
  associate:    5,
  specialist:   5,
  assistant:    5,
  technician:   5,
  analyst:      5,
};

/**
 * Score a lead as a decision maker (0-100).
 * Higher score = more likely to be the business decision maker.
 *
 * @param {object} lead
 * @param {string} [lead.title] - Job title
 * @param {string} [lead.first_name]
 * @param {string} [lead.last_name]
 * @param {string} [lead.firm_name] - Business name
 * @param {number} [lead._rating_count] - Google review count (size proxy)
 * @returns {number} Score 0-100
 */
function scoreDM(lead) {
  let score = 0;
  const title = (lead.title || '').toLowerCase();
  const name = `${lead.first_name || ''} ${lead.last_name || ''}`.toLowerCase().trim();
  const firm = (lead.firm_name || '').toLowerCase();

  // Score from explicit title
  for (const [keyword, points] of Object.entries(DM_SCORES)) {
    if (title.includes(keyword)) {
      score = Math.max(score, points);
    }
  }

  // Doctor/dentist/professional titles = likely owner of small practice
  if (/^(dr\.?|dds|dmd|md|do|dc|od|dvm)\b/i.test(title) || /\b(dds|dmd|md|do)\b/i.test(title)) {
    score = Math.max(score, 35);
  }

  // Name in business name = owner signal
  if (name && firm) {
    const lastName = (lead.last_name || '').toLowerCase();
    if (lastName && lastName.length > 2 && firm.includes(lastName)) {
      score = Math.max(score, 45);
    }
  }

  // Small business (few reviews) = contact is likely the owner
  const ratingCount = lead._rating_count || 0;
  if (ratingCount > 0 && ratingCount < 20 && score < 30) {
    score = Math.max(score, 30); // Small business, probably owner
  }

  // No title but only person at business = likely owner
  if (!title && !lead._is_staff) {
    score = Math.max(score, 15);
  }

  return Math.min(score, 100);
}

// ─── Title Inference ────────────────────────────────────────────────

/**
 * Infer a title for a person based on business context.
 *
 * @param {object} lead
 * @param {string} niche - Business type
 * @returns {string} Inferred title or ''
 */
function inferTitle(lead, niche) {
  if (lead.title) return lead.title;

  const firm = (lead.firm_name || '').toLowerCase();
  const lastName = (lead.last_name || '').toLowerCase();
  const ratingCount = lead._rating_count || 0;

  // Name in business name = owner
  if (lastName && lastName.length > 2 && firm.includes(lastName)) {
    return 'Owner';
  }

  // Very small business (< 10 reviews) and this is the only contact
  if (ratingCount > 0 && ratingCount < 10) {
    return 'Owner';
  }

  // Medium business
  if (ratingCount >= 10 && ratingCount < 50) {
    return 'Manager';
  }

  // Large business (many reviews)
  if (ratingCount >= 50) {
    return '';  // Can't infer for large businesses
  }

  // Niche-specific defaults
  const nicheL = (niche || '').toLowerCase();
  if (/dentist|dental/i.test(nicheL)) return 'Dentist';
  if (/doctor|physician|medical/i.test(nicheL)) return 'Physician';
  if (/chiropract/i.test(nicheL)) return 'Chiropractor';
  if (/veterinar/i.test(nicheL)) return 'Veterinarian';
  if (/optometri/i.test(nicheL)) return 'Optometrist';
  if (/plumb/i.test(nicheL)) return 'Plumber';
  if (/electric/i.test(nicheL)) return 'Electrician';
  if (/account/i.test(nicheL)) return 'Accountant';
  if (/architect/i.test(nicheL)) return 'Architect';
  if (/real\s*estate|realtor/i.test(nicheL)) return 'Real Estate Agent';

  return '';
}

// ─── Specialty Classification ───────────────────────────────────────

// Industry-specific specialty patterns
const SPECIALTY_PATTERNS = {
  // Dental
  'General Dentistry': /\b(general\s+dentist|family\s+dentist)/i,
  'Pediatric Dentistry': /\b(pediatric|children|kids)\s*(dentist|dental)/i,
  'Orthodontics': /\b(orthodont|braces|invisalign)/i,
  'Cosmetic Dentistry': /\b(cosmetic\s+dentist|veneers|teeth\s+whitening)/i,
  'Oral Surgery': /\b(oral\s+surg|wisdom\s+teeth|extraction)/i,
  'Periodontics': /\b(periodont|gum\s+disease)/i,
  'Endodontics': /\b(endodont|root\s+canal)/i,
  'Prosthodontics': /\b(prosthodont|dental\s+implant|denture)/i,

  // Medical
  'Family Medicine': /\b(family\s+(medicine|practice|doctor))/i,
  'Internal Medicine': /\b(internal\s+medicine|internist)/i,
  'Pediatrics': /\b(pediatric|children.*doctor)/i,
  'Dermatology': /\b(dermatolog|skin\s+(doctor|care|clinic))/i,
  'Cardiology': /\b(cardiolog|heart\s+(doctor|specialist))/i,
  'Orthopedics': /\b(orthoped|bone|joint\s+(doctor|specialist))/i,
  'Ophthalmology': /\b(ophthalmolog|eye\s+(doctor|surgeon))/i,

  // Trades
  'Residential Plumbing': /\b(residential|home)\s*plumb/i,
  'Commercial Plumbing': /\b(commercial|industrial)\s*plumb/i,
  'Emergency Plumbing': /\b(emergency|24.hour)\s*plumb/i,
  'Residential Electrical': /\b(residential|home)\s*electric/i,
  'Commercial Electrical': /\b(commercial|industrial)\s*electric/i,
  'HVAC': /\b(hvac|heating\s+(and|&)\s+cooling|air\s+condition|furnace)/i,
  'Roofing': /\b(roofing|roof\s*(repair|install|replace|contract))/i,
  'Landscaping': /\b(landscap|lawn\s+care|garden\s*(service|design|maintenance|care))/i,

  // Professional Services
  'Tax Accounting': /\b(tax\s+(account|prep|service)|cpa|bookkeep)/i,
  'Audit': /\b(audit\s+(service|firm)|assurance\s+service)/i,
  'Financial Planning': /\b(financial\s+plan|wealth\s+manage|investment\s+advi)/i,
  'Architecture': /\b(architect|building\s+design)/i,
  'Interior Design': /\b(interior\s+design)/i,
  'Real Estate Residential': /\b(residential\s+real\s+estate|home\s+sale)/i,
  'Real Estate Commercial': /\b(commercial\s+real\s+estate|office\s+space)/i,
};

/**
 * Detect specialty from business name + snippet.
 */
function detectSpecialty(firmName, snippet) {
  const text = ((firmName || '') + ' ' + (snippet || '')).trim();
  if (!text) return '';

  for (const [specialty, pattern] of Object.entries(SPECIALTY_PATTERNS)) {
    if (pattern.test(text)) return specialty;
  }

  return '';
}

// ─── Person Name Validation ─────────────────────────────────────────

// Common first names — subset for quick validation (from person-extractor.js pattern)
const COMMON_FIRST_NAMES = new Set([
  'james','robert','john','michael','david','william','richard','joseph','thomas','charles',
  'christopher','daniel','matthew','anthony','mark','donald','steven','paul','andrew','joshua',
  'kenneth','kevin','brian','george','timothy','ronald','edward','jason','jeffrey','ryan',
  'jacob','gary','nicholas','eric','jonathan','stephen','larry','justin','scott','brandon',
  'benjamin','samuel','raymond','gregory','frank','alexander','patrick','jack','dennis','jerry',
  'tyler','aaron','jose','adam','nathan','henry','peter','zachary','douglas','harold',
  'kyle','noah','carl','gerald','keith','roger','arthur','terry','sean','austin',
  'christian','albert','joe','ethan','jesse','ralph','roy','louis','eugene','philip',
  'mary','patricia','jennifer','linda','barbara','elizabeth','susan','jessica','sarah','karen',
  'lisa','nancy','betty','margaret','sandra','ashley','dorothy','kimberly','emily','donna',
  'michelle','carol','amanda','melissa','deborah','stephanie','rebecca','sharon','laura','cynthia',
  'kathleen','amy','angela','shirley','anna','brenda','pamela','emma','nicole','helen','samantha',
  'katherine','christine','debra','rachel','carolyn','janet','catherine','maria','heather','diane',
  'ruth','julie','olivia','joyce','virginia','victoria','kelly','lauren','christina','joan',
  'sophia','grace','denise','amber','doris','marilyn','danielle','beverly','isabella','theresa',
  'diana','natalie','brittany','charlotte','marie','kayla','alexis','lori','alyssa','rosa',
  'mohammed','ahmed','ali','wei','chen','raj','priya','carlos','miguel','antonio','pablo',
  'marco','luca','hans','lars','sven','ivan','dmitri','yuki','hiroshi','kenji',
  'alejandro','ricardo','diego','luis','jorge','sofia','elena','lucia','ana','carmen',
  'fatima','aisha','omar','hassan','ibrahim',
  'sebastian','milton','ximena','ingrid','gigi','cesar','rafael','gabriel','fernando',
  'victor','hector','oscar','ruben','felix','mario','sergio','angel','pedro','raul',
  'gloria','rosa','teresa','blanca','yolanda','silvia','veronica','adriana','claudia',
  'tiffany','tracy','wendy','kristen','megan','courtney','holly','jenna',
  'derek','troy','blake','spencer','logan','mason','liam','owen','luke','caleb',
  'dylan','cole','chase','hunter','connor','cameron','garrett','trevor','landon',
]);

/**
 * Check if a first_name looks like a real human first name (not a business word).
 */
function isLikelyPersonName(firstName) {
  if (!firstName) return false;
  return COMMON_FIRST_NAMES.has(firstName.toLowerCase());
}

// ─── LinkedIn URL Construction ──────────────────────────────────────

/**
 * Build a best-guess LinkedIn profile URL.
 */
function buildLinkedInUrl(firstName, lastName) {
  if (!firstName || !lastName) return '';

  // Normalize: lowercase, remove accents, strip non-alphanumeric
  const clean = (s) => s.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // Strip accents
    .replace(/[^a-z0-9]/g, '');

  const first = clean(firstName);
  const last = clean(lastName);

  if (!first || !last) return '';

  return `https://www.linkedin.com/in/${first}-${last}`;
}

// ─── Email Pattern Generation ───────────────────────────────────────

/**
 * Generate likely email patterns for a person at a domain.
 * Same patterns as email-verifier.js.
 */
function generateEmailPatterns(firstName, lastName, domain) {
  if (!firstName || !lastName || !domain) return [];

  const first = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const last = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!first || !last) return [];

  const initial = first[0];

  return [
    `${first}.${last}@${domain}`,
    `${first}${last}@${domain}`,
    `${initial}${last}@${domain}`,
    `${initial}.${last}@${domain}`,
    `${first}@${domain}`,
    `${last}@${domain}`,
    `${first}_${last}@${domain}`,
    `${last}.${first}@${domain}`,
    `${first}${initial}@${domain}`,
    `${last}${initial}@${domain}`,
    `${initial}${first[1] || ''}${last}@${domain}`,
    `${first}-${last}@${domain}`,
  ];
}

// ─── Cross-Source Merging ───────────────────────────────────────────

/**
 * Merge leads from multiple sources, deduplicating by domain + name.
 *
 * @param {object[][]} leadArrays - Arrays of leads from different sources
 * @returns {object[]} Merged, deduplicated leads
 */
function mergeLeads(...leadArrays) {
  const byKey = new Map();

  for (const leads of leadArrays) {
    for (const lead of leads) {
      // Build merge key: domain + normalized name (or domain + phone)
      const domain = lead.domain || extractDomain(lead.website || '');
      const name = normalizeFirmName(lead.firm_name || lead.name || '');
      const phone = (lead.phone || '').replace(/\D/g, '');

      let key = '';
      if (domain && name) key = `${domain}|${name}`;
      else if (phone && name) key = `${phone}|${name}`;
      else if (domain) key = domain;
      else if (phone && phone.length >= 7) key = `phone:${phone}`;
      else key = `name:${name}:${lead.city || ''}`;

      if (!key || key === '|') {
        // Can't merge — just add as unique
        byKey.set(`uniq:${Math.random()}`, lead);
        continue;
      }

      if (byKey.has(key)) {
        // Merge: prefer non-empty fields
        const existing = byKey.get(key);
        for (const [field, val] of Object.entries(lead)) {
          if (val && !existing[field]) {
            existing[field] = val;
          }
        }
        // Track sources
        if (lead.source && existing._sources) {
          existing._sources.add(lead.source);
        }
      } else {
        lead._sources = new Set([lead.source || 'unknown']);
        byKey.set(key, lead);
      }
    }
  }

  // Convert _sources Set to comma-separated string
  const results = [];
  for (const lead of byKey.values()) {
    if (lead._sources) {
      lead.source = [...lead._sources].join(', ');
      delete lead._sources;
    }
    results.push(lead);
  }

  return results;
}

// ─── Full Enrichment Pass ───────────────────────────────────────────

/**
 * Run enrichment on a batch of leads.
 *
 * @param {object[]} leads - Array of leads to enrich (mutated in place)
 * @param {string} niche - Business niche
 * @param {object} [options]
 * @param {function} [options.onProgress] - Progress callback
 * @returns {object} Stats
 */
function enrichAll(leads, niche, options = {}) {
  const onProgress = options.onProgress || (() => {});
  const stats = {
    titlesInferred: 0,
    specialtiesDetected: 0,
    linkedInBuilt: 0,
    emailPatternsGenerated: 0,
    dmScored: 0,
  };

  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    onProgress(i + 1, leads.length, `${lead.first_name} ${lead.last_name}`.trim() || lead.firm_name);

    // Infer title
    if (!lead.title) {
      const title = inferTitle(lead, niche);
      if (title) {
        lead.title = title;
        stats.titlesInferred++;
      }
    }

    // Detect specialty
    if (!lead.practice_specialties) {
      const specialty = detectSpecialty(lead.firm_name, lead._snippet || '');
      if (specialty) {
        lead.practice_specialties = specialty;
        stats.specialtiesDetected++;
      }
    }

    // Only generate LinkedIn + email patterns for real person names
    // (avoid nonsense like linkedin.com/in/level-chiropractic)
    const hasRealPersonName = isLikelyPersonName(lead.first_name);

    // Build LinkedIn URL
    if (!lead.linkedin_url && lead.first_name && lead.last_name && hasRealPersonName) {
      lead.linkedin_url = buildLinkedInUrl(lead.first_name, lead.last_name);
      if (lead.linkedin_url) stats.linkedInBuilt++;
    }

    // Generate email patterns (if no email and has website)
    if (!lead.email && lead.website && hasRealPersonName) {
      const domain = extractDomain(lead.website);
      if (domain && lead.first_name && lead.last_name) {
        const patterns = generateEmailPatterns(lead.first_name, lead.last_name, domain);
        if (patterns.length > 0) {
          lead._email_patterns = patterns;
          lead.email = patterns[0]; // Best guess: first.last@domain
          lead.email_source = 'pattern';
          stats.emailPatternsGenerated++;
        }
      }
    }

    // Score as decision maker
    lead.dm_score = scoreDM(lead);
    stats.dmScored++;
  }

  return stats;
}

module.exports = {
  scoreDM,
  inferTitle,
  detectSpecialty,
  buildLinkedInUrl,
  generateEmailPatterns,
  isLikelyPersonName,
  mergeLeads,
  enrichAll,
};
