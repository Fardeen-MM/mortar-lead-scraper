/**
 * Scraper Registry — auto-discovers state bar scrapers from filesystem
 *
 * Scans scrapers/bars/*.js, loads each module, and builds a registry
 * keyed by stateCode (e.g., { FL: () => floridaScraper, 'CA-ON': () => ontarioScraper })
 */

const fs = require('fs');
const path = require('path');
const { getStateName, getCountry } = require('./state-metadata');

const SCRAPERS_DIR = path.join(__dirname, '..', 'scrapers', 'bars');

let _registry = null;
let _metadata = null;

/**
 * Discover all scrapers from the bars directory.
 * Returns { stateCode: () => scraperInstance }
 */
function getRegistry() {
  if (_registry) return _registry;

  _registry = {};
  const files = fs.readdirSync(SCRAPERS_DIR)
    .filter(f => f.endsWith('.js'));

  for (const file of files) {
    try {
      const scraper = require(path.join(SCRAPERS_DIR, file));
      if (scraper && scraper.stateCode) {
        const code = scraper.stateCode;
        _registry[code] = () => scraper;
      }
    } catch (err) {
      console.error(`Failed to load scraper ${file}: ${err.message}`);
    }
  }

  return _registry;
}

/**
 * Get metadata for all discovered scrapers.
 * Used by /api/config to populate the UI.
 * Includes country field for grouping.
 */
function getScraperMetadata() {
  if (_metadata) return _metadata;

  const registry = getRegistry();
  _metadata = {};

  for (const [code, loader] of Object.entries(registry)) {
    const scraper = loader();
    _metadata[code] = {
      name: getStateName(code),
      stateCode: code,
      country: getCountry(code),
      practiceAreas: Object.keys(scraper.practiceAreaCodes || scraper.PRACTICE_AREA_CODES || {}),
      defaultCities: scraper.defaultCities || scraper.DEFAULT_CITIES || [],
    };
  }

  return _metadata;
}

/**
 * Force re-discovery (useful after hot-loading scrapers).
 */
function resetRegistry() {
  _registry = null;
  _metadata = null;
}

module.exports = { getRegistry, getScraperMetadata, resetRegistry };
