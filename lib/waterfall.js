/**
 * Waterfall Enrichment Engine — Clay-style data enrichment pipeline
 *
 * Takes scraped leads and runs them through multiple data sources in sequence,
 * filling missing fields at each step. First source that returns a field wins.
 * Each step only runs for fields still missing.
 *
 * Waterfall steps (in order):
 *   1. Profile page fetch (same source detail pages)
 *   2. Cross-reference Martindale (city batch + name match)
 *   3. Cross-reference Lawyers.com (city batch + name match)
 *   4. Name-based lookups (CA/NY/AU-NSW APIs)
 *   5. Firm website email crawl (Puppeteer, name-matched)
 *
 * Source tracking: every field gets a _source tag (email_source, phone_source, etc.)
 */

const { log } = require('./logger');
const { RateLimiter } = require('./rate-limiter');
const { getRegistry } = require('./registry');

// Fields we track sources for
const TRACKED_FIELDS = ['email', 'phone', 'website'];

/**
 * Merge enrichment data into a lead without overwriting existing values.
 * Tracks the source of each newly-filled field.
 *
 * @param {object} lead - The lead to enrich
 * @param {object} newData - New fields to merge
 * @param {string} sourceName - Source identifier (e.g., 'profile', 'martindale')
 * @returns {string[]} List of fields that were actually filled
 */
function mergeFields(lead, newData, sourceName) {
  const filled = [];
  for (const [key, value] of Object.entries(newData)) {
    if (!value) continue;
    // Only fill if the lead doesn't already have this field
    if (!lead[key] || lead[key].trim() === '') {
      lead[key] = value;
      filled.push(key);
      // Track source for tracked fields
      if (TRACKED_FIELDS.includes(key)) {
        lead[`${key}_source`] = sourceName;
      }
    }
  }
  return filled;
}

/**
 * Check if a lead is missing critical fields that waterfall can fill.
 */
function needsEnrichment(lead) {
  return !lead.email || !lead.phone || !lead.website;
}

/**
 * Run the waterfall enrichment pipeline on an array of leads.
 *
 * @param {object[]} leads - Array of lead objects to enrich
 * @param {object} options
 * @param {boolean} [options.fetchProfiles=true] - Fetch profile pages
 * @param {boolean} [options.crossRefMartindale=true] - Cross-ref Martindale
 * @param {boolean} [options.crossRefLawyersCom=true] - Cross-ref Lawyers.com
 * @param {boolean} [options.nameLookups=true] - Name-based API lookups
 * @param {boolean} [options.emailCrawl=true] - Crawl firm websites for emails
 * @param {string} [options.proxy] - Proxy URL
 * @param {EventEmitter} [options.emitter] - Event emitter for progress
 * @param {function} [options.isCancelled] - Returns true if cancelled
 * @param {object} [options.emailFinder] - Existing EmailFinder instance
 * @param {object} [options.domainCache] - DomainEmailCache instance
 * @returns {object} Waterfall stats
 */
async function runWaterfall(leads, options = {}) {
  const {
    fetchProfiles = true,
    crossRefMartindale = true,
    crossRefLawyersCom = true,
    nameLookups = true,
    emailCrawl = true,
    emitter,
    isCancelled = () => false,
    emailFinder,
    domainCache,
  } = options;

  const stats = {
    profilesFetched: 0,
    profileFieldsFilled: 0,
    crossRefMatches: 0,
    crossRefFieldsFilled: 0,
    nameLookupsRun: 0,
    nameFieldsFilled: 0,
    emailsCrawled: 0,
    totalFieldsFilled: 0,
  };

  const emitProgress = (step, current, total, detail) => {
    if (emitter) {
      emitter.emit('waterfall-progress', { step, current, total, detail });
    }
  };

  const SCRAPERS = getRegistry();

  // --- Step 1: Profile Page Fetch ---
  if (fetchProfiles) {
    const maxProfiles = options.maxProfileFetches || 100;
    let allLeadsWithProfiles = leads.filter(l => l.profile_url && needsEnrichment(l));
    if (allLeadsWithProfiles.length > maxProfiles) {
      log.info(`Capping profile fetches at ${maxProfiles} (${allLeadsWithProfiles.length} eligible)`);
      allLeadsWithProfiles = allLeadsWithProfiles.slice(0, maxProfiles);
    }
    const leadsWithProfiles = allLeadsWithProfiles;
    if (leadsWithProfiles.length > 0) {
      log.info(`Waterfall Step 1: Fetching ${leadsWithProfiles.length} profile pages`);
      emitProgress('profiles', 0, leadsWithProfiles.length, 'Starting profile page fetch');

      const rateLimiter = new RateLimiter();

      // Group leads by source to use the right scraper's parseProfilePage
      const bySource = new Map();
      for (const lead of leadsWithProfiles) {
        const src = lead.source || '';
        if (!bySource.has(src)) bySource.set(src, []);
        bySource.get(src).push(lead);
      }

      let processed = 0;
      for (const [source, sourceLeads] of bySource) {
        // Find the scraper for this source
        const scraper = findScraperForSource(source, SCRAPERS);
        if (!scraper) {
          processed += sourceLeads.length;
          continue;
        }

        // Skip scrapers that don't have parseProfilePage implemented
        if (!scraper.hasProfileParser) {
          log.info(`Skipping profile fetch for ${source} — no parseProfilePage`);
          processed += sourceLeads.length;
          continue;
        }

        for (const lead of sourceLeads) {
          if (isCancelled()) break;

          const profileData = await scraper.enrichFromProfile(lead, rateLimiter);
          const filled = mergeFields(lead, profileData, 'profile');
          if (filled.length > 0) {
            stats.profilesFetched++;
            stats.profileFieldsFilled += filled.length;
            stats.totalFieldsFilled += filled.length;
          }
          processed++;
          emitProgress('profiles', processed, leadsWithProfiles.length,
            `${lead.first_name} ${lead.last_name}`);
        }
      }

      log.success(`Step 1 done: ${stats.profilesFetched} profiles enriched, ${stats.profileFieldsFilled} fields filled`);
    }
  }

  if (isCancelled()) return stats;

  // Lazy require to avoid circular dependency
  const CrossReference = require('./cross-reference');

  // --- Step 2: Cross-Reference Martindale ---
  if (crossRefMartindale && SCRAPERS['MARTINDALE']) {
    const leadsNeedingData = leads.filter(l => needsEnrichment(l));
    if (leadsNeedingData.length > 0) {
      log.info(`Waterfall Step 2: Cross-referencing ${leadsNeedingData.length} leads against Martindale`);
      emitProgress('martindale', 0, leadsNeedingData.length, 'Starting Martindale cross-reference');

      try {
        const crossRef = new CrossReference({
          scraperCode: 'MARTINDALE',
          scrapers: SCRAPERS,
        });

        const crStats = await crossRef.batchCrossReference(leadsNeedingData, {
          sourceName: 'martindale',
          onProgress: (current, total, detail) => {
            emitProgress('martindale', current, total, detail);
          },
          isCancelled,
        });

        stats.crossRefMatches += crStats.matches;
        stats.crossRefFieldsFilled += crStats.fieldsFilled;
        stats.totalFieldsFilled += crStats.fieldsFilled;

        log.success(`Step 2 done: ${crStats.matches} Martindale matches, ${crStats.fieldsFilled} fields filled`);
      } catch (err) {
        log.error(`Martindale cross-ref failed: ${err.message}`);
      }
    }
  }

  if (isCancelled()) return stats;

  // --- Step 3: Cross-Reference Lawyers.com ---
  if (crossRefLawyersCom && SCRAPERS['LAWYERS-COM']) {
    const leadsNeedingData = leads.filter(l => needsEnrichment(l));
    if (leadsNeedingData.length > 0) {
      log.info(`Waterfall Step 3: Cross-referencing ${leadsNeedingData.length} leads against Lawyers.com`);
      emitProgress('lawyers-com', 0, leadsNeedingData.length, 'Starting Lawyers.com cross-reference');

      try {
        const crossRef = new CrossReference({
          scraperCode: 'LAWYERS-COM',
          scrapers: SCRAPERS,
        });

        const crStats = await crossRef.batchCrossReference(leadsNeedingData, {
          sourceName: 'lawyers-com',
          onProgress: (current, total, detail) => {
            emitProgress('lawyers-com', current, total, detail);
          },
          isCancelled,
        });

        stats.crossRefMatches += crStats.matches;
        stats.crossRefFieldsFilled += crStats.fieldsFilled;
        stats.totalFieldsFilled += crStats.fieldsFilled;

        log.success(`Step 3 done: ${crStats.matches} Lawyers.com matches, ${crStats.fieldsFilled} fields filled`);
      } catch (err) {
        log.error(`Lawyers.com cross-ref failed: ${err.message}`);
      }
    }
  }

  if (isCancelled()) return stats;

  // --- Step 4: Name-Based Lookups ---
  if (nameLookups) {
    const nameSearchable = {
      CA: { method: 'lookupByName', stateCode: 'CA' },
      NY: { method: 'lookupByName', stateCode: 'NY' },
      'AU-NSW': { method: 'lookupByName', stateCode: 'AU-NSW' },
    };

    // For each name-searchable source, find leads from OTHER sources that
    // might be found via name lookup
    for (const [code, config] of Object.entries(nameSearchable)) {
      if (isCancelled()) break;
      if (!SCRAPERS[code]) continue;

      // Find leads that: need enrichment AND are in a region this API covers
      const eligibleLeads = leads.filter(l => {
        if (!needsEnrichment(l)) return false;
        // Don't look up leads that came FROM this source
        if (l.source && l.source.includes(code.toLowerCase().replace('-', '_'))) return false;

        // CA API covers California leads from any source
        if (code === 'CA') return l.state === 'CA';
        // NY API covers New York leads
        if (code === 'NY') return l.state === 'NY';
        // AU-NSW covers all Australian leads
        if (code === 'AU-NSW') return (l.state || '').startsWith('AU-') || (l.country || '').toUpperCase() === 'AU';

        return false;
      });

      if (eligibleLeads.length === 0) continue;

      log.info(`Waterfall Step 4: Name lookups on ${code} for ${eligibleLeads.length} leads`);
      emitProgress('name-lookup', 0, eligibleLeads.length, `Looking up names on ${code}`);

      const scraper = SCRAPERS[code]();
      const rateLimiter = new RateLimiter();

      // Only proceed if the scraper has a lookupByName method
      if (typeof scraper.lookupByName !== 'function') {
        log.info(`${code} scraper doesn't have lookupByName — skipping`);
        continue;
      }

      let processed = 0;
      for (const lead of eligibleLeads) {
        if (isCancelled()) break;

        try {
          const result = await scraper.lookupByName(
            lead.first_name, lead.last_name, lead.city, rateLimiter
          );
          if (result) {
            const filled = mergeFields(lead, result, code.toLowerCase());
            if (filled.length > 0) {
              stats.nameLookupsRun++;
              stats.nameFieldsFilled += filled.length;
              stats.totalFieldsFilled += filled.length;
            }
          }
        } catch (err) {
          log.warn(`Name lookup failed for ${lead.first_name} ${lead.last_name} on ${code}: ${err.message}`);
        }

        processed++;
        emitProgress('name-lookup', processed, eligibleLeads.length,
          `${lead.first_name} ${lead.last_name}`);
      }

      log.success(`Step 4 (${code}) done: ${stats.nameLookupsRun} lookups, ${stats.nameFieldsFilled} fields filled`);
    }
  }

  if (isCancelled()) return stats;

  // --- Step 5: Firm Website Email Crawl ---
  if (emailCrawl && emailFinder) {
    const leadsNeedingEmail = leads.filter(l => !l.email && l.website);
    if (leadsNeedingEmail.length > 0) {
      log.info(`Waterfall Step 5: Crawling ${leadsNeedingEmail.length} firm websites for emails`);
      emitProgress('email-crawl', 0, leadsNeedingEmail.length, 'Starting email crawl');

      const crawlStats = await emailFinder.batchFindEmails(
        leadsNeedingEmail,
        (current, total, name) => {
          emitProgress('email-crawl', current, total, name);
        },
        isCancelled,
        domainCache
      );

      stats.emailsCrawled = crawlStats.emailsFound;
      stats.totalFieldsFilled += crawlStats.emailsFound;

      log.success(`Step 5 done: ${crawlStats.emailsFound} emails found from ${crawlStats.websitesVisited} websites`);
    }
  }

  return stats;
}

/**
 * Build a reverse lookup from scraper name → registry code.
 * Done once, then cached. Maps source strings like "florida_bar" or "florida" → "FL".
 */
let _sourceToCode = null;
function getSourceToCodeMap(SCRAPERS) {
  if (_sourceToCode) return _sourceToCode;
  _sourceToCode = {};
  for (const [code, loader] of Object.entries(SCRAPERS)) {
    try {
      const scraper = loader();
      if (scraper.name) {
        // Map both "name_bar" and "name" to the code
        _sourceToCode[`${scraper.name}_bar`] = code;
        _sourceToCode[scraper.name] = code;
      }
    } catch {
      // Skip scrapers that fail to instantiate
    }
  }
  return _sourceToCode;
}

/**
 * Find the right scraper instance for a given source string.
 * Dynamically maps source names to scraper codes using the registry.
 */
function findScraperForSource(source, SCRAPERS) {
  if (!source) return null;
  const sourceLower = source.toLowerCase().trim();

  const map = getSourceToCodeMap(SCRAPERS);

  // Direct match first (most common: "florida_bar" → FL)
  if (map[sourceLower] && SCRAPERS[map[sourceLower]]) {
    return SCRAPERS[map[sourceLower]]();
  }

  // Partial match fallback (e.g., "texas" matches "texas_bar")
  for (const [key, code] of Object.entries(map)) {
    if (sourceLower.includes(key) && SCRAPERS[code]) {
      return SCRAPERS[code]();
    }
  }

  return null;
}

module.exports = { runWaterfall, mergeFields, needsEnrichment };
