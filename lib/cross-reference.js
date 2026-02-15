/**
 * Cross-Reference Engine — batch city-level directory lookups + name matching
 *
 * For directory sources that can't search by name (Martindale, Lawyers.com):
 * 1. Collect unique cities from scrape results
 * 2. Search each directory ONCE per city → cache all results in memory
 * 3. Match each lead against cache by firstName + lastName (exact + fuzzy)
 * 4. Merge found data: phone, website, firm, email
 *
 * This is efficient — searching Martindale for "Houston, TX" once gives us
 * data for potentially hundreds of our leads.
 */

const { log } = require('./logger');

// Inline mergeFields to avoid circular dependency with waterfall.js
const TRACKED_FIELDS = ['email', 'phone', 'website'];
function mergeFields(lead, newData, sourceName) {
  const filled = [];
  for (const [key, value] of Object.entries(newData)) {
    if (!value) continue;
    if (!lead[key] || lead[key].trim() === '') {
      lead[key] = value;
      filled.push(key);
      if (TRACKED_FIELDS.includes(key)) {
        lead[`${key}_source`] = sourceName;
      }
    }
  }
  return filled;
}

// Simple string similarity for fuzzy name matching (Dice coefficient)
function similarity(a, b) {
  if (!a || !b) return 0;
  a = a.toLowerCase().trim();
  b = b.toLowerCase().trim();
  if (a === b) return 1;

  const bigrams = (str) => {
    const result = new Set();
    for (let i = 0; i < str.length - 1; i++) {
      result.add(str.substring(i, i + 2));
    }
    return result;
  };

  const aBigrams = bigrams(a);
  const bBigrams = bigrams(b);
  let intersection = 0;
  for (const bg of aBigrams) {
    if (bBigrams.has(bg)) intersection++;
  }

  return (2 * intersection) / (aBigrams.size + bBigrams.size);
}

/**
 * Match a lead against a cache of directory results by name.
 * Returns the best match or null.
 *
 * @param {object} lead - The lead to match
 * @param {object[]} directoryResults - Array of results from the directory
 * @param {number} [threshold=0.8] - Minimum name similarity to accept
 * @returns {object|null} Best matching directory entry
 */
function findNameMatch(lead, directoryResults, threshold = 0.8) {
  if (!lead.first_name || !lead.last_name) return null;

  const leadFirst = lead.first_name.toLowerCase().trim();
  const leadLast = lead.last_name.toLowerCase().trim();

  let bestMatch = null;
  let bestScore = 0;

  for (const entry of directoryResults) {
    const entryFirst = (entry.first_name || '').toLowerCase().trim();
    const entryLast = (entry.last_name || '').toLowerCase().trim();

    if (!entryFirst || !entryLast) continue;

    // Last name must match closely
    const lastSim = similarity(leadLast, entryLast);
    if (lastSim < threshold) continue;

    // First name similarity
    const firstSim = similarity(leadFirst, entryFirst);
    if (firstSim < 0.6) continue; // First name can be less strict (nicknames, abbreviations)

    const score = lastSim * 0.6 + firstSim * 0.4;
    if (score > bestScore) {
      bestScore = score;
      bestMatch = entry;
    }
  }

  return bestScore >= threshold ? bestMatch : null;
}

class CrossReference {
  /**
   * @param {object} config
   * @param {string} config.scraperCode - Registry code for the directory (e.g., 'MARTINDALE')
   * @param {object} config.scrapers - The scraper registry (from getRegistry())
   */
  constructor(config) {
    this.scraperCode = config.scraperCode;
    this.scrapers = config.scrapers;
    this.cityCache = new Map(); // city+state → results[]
  }

  /**
   * Batch cross-reference leads against this directory.
   * Groups by city, searches each city once, then matches by name.
   *
   * @param {object[]} leads - Array of leads to cross-reference
   * @param {object} options
   * @param {string} options.sourceName - Source name for field tracking
   * @param {function} [options.onProgress] - Progress callback(current, total, detail)
   * @param {function} [options.isCancelled] - Returns true to stop
   * @returns {object} Stats: { matches, fieldsFilled, citiesSearched }
   */
  async batchCrossReference(leads, options = {}) {
    const { sourceName, onProgress, isCancelled = () => false } = options;
    const stats = { matches: 0, fieldsFilled: 0, citiesSearched: 0 };

    if (!this.scrapers[this.scraperCode]) {
      log.warn(`CrossReference: No scraper found for ${this.scraperCode}`);
      return stats;
    }

    // Collect unique city+state combinations from leads
    const citySet = new Map(); // "city|state" → { city, stateCode }
    for (const lead of leads) {
      if (!lead.city) continue;
      const key = `${lead.city.toLowerCase()}|${(lead.state || '').toUpperCase()}`;
      if (!citySet.has(key)) {
        citySet.set(key, { city: lead.city, stateCode: lead.state || '' });
      }
    }

    log.info(`CrossReference (${this.scraperCode}): ${citySet.size} unique cities to search`);

    // Search each city in the directory
    let citiesProcessed = 0;
    for (const [cacheKey, { city, stateCode }] of citySet) {
      if (isCancelled()) break;

      // Check cache
      if (!this.cityCache.has(cacheKey)) {
        const results = await this._searchCity(city, stateCode);
        this.cityCache.set(cacheKey, results);
        stats.citiesSearched++;
        log.info(`CrossRef ${this.scraperCode}: ${city}, ${stateCode} → ${results.length} results`);
      }

      citiesProcessed++;
    }

    // Now match each lead against the cache
    let processed = 0;
    for (const lead of leads) {
      if (isCancelled()) break;

      if (!lead.city) {
        processed++;
        continue;
      }

      const cacheKey = `${lead.city.toLowerCase()}|${(lead.state || '').toUpperCase()}`;
      const directoryResults = this.cityCache.get(cacheKey) || [];

      if (directoryResults.length > 0) {
        const match = findNameMatch(lead, directoryResults);
        if (match) {
          const filled = mergeFields(lead, {
            phone: match.phone,
            website: match.website,
            email: match.email,
            firm_name: match.firm_name,
          }, sourceName);

          if (filled.length > 0) {
            stats.matches++;
            stats.fieldsFilled += filled.length;
          }
        }
      }

      processed++;
      if (onProgress) onProgress(processed, leads.length, `${lead.first_name} ${lead.last_name}`);
    }

    return stats;
  }

  /**
   * Search a directory for a single city.
   * Returns an array of results from the directory scraper.
   */
  async _searchCity(city, stateCode) {
    const results = [];

    try {
      const scraper = this.scrapers[this.scraperCode]();

      // Build search options based on directory type
      const searchOptions = {
        city,
        state: stateCode,
        stateCode,
        maxPages: 3, // Limit pages for cross-reference (don't need ALL results)
      };

      for await (const result of scraper.search('', searchOptions)) {
        // Skip internal signals
        if (result._cityProgress || result._captcha) continue;

        results.push({
          first_name: result.first_name || '',
          last_name: result.last_name || '',
          full_name: result.full_name || '',
          firm_name: result.firm_name || '',
          phone: result.phone || '',
          email: result.email || '',
          website: result.website || '',
          city: result.city || city,
          state: result.state || stateCode,
        });
      }
    } catch (err) {
      log.warn(`CrossRef ${this.scraperCode}: Failed to search ${city}, ${stateCode}: ${err.message}`);
    }

    return results;
  }
}

module.exports = CrossReference;
