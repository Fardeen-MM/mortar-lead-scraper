/**
 * Scraper Registry — auto-discovers scrapers from filesystem
 *
 * Scans multiple scraper directories (bars/, directories/, federal/, international/)
 * and builds a registry keyed by stateCode.
 */

const fs = require('fs');
const path = require('path');
const { getStateName, getCountry } = require('./state-metadata');

// Scrapers confirmed working via smoke/API tests (return real leads)
const WORKING_SCRAPERS = new Set([
  'AU-NSW', 'AU-QLD', 'AU-SA', 'AU-TAS', 'AU-VIC', 'AU-WA',
  'CA', 'CA-AB', 'CA-BC', 'CA-NL', 'CA-PE', 'CA-YT',
  'CO', 'CT', 'FL', 'FR', 'GA', 'HK', 'ID', 'IE', 'IL', 'IT',
  'MARTINDALE', 'MD', 'MI', 'MN', 'NC', 'NZ', 'NY', 'OH', 'OR', 'PA',
  'SG', 'TX', 'UK-EW-BAR', 'UK-SC', 'VA',
]);

const SCRAPER_DIRS = [
  path.join(__dirname, '..', 'scrapers', 'bars'),
  path.join(__dirname, '..', 'scrapers', 'directories'),
  path.join(__dirname, '..', 'scrapers', 'federal'),
  path.join(__dirname, '..', 'scrapers', 'international'),
];

let _registry = null;
let _metadata = null;

/**
 * Discover all scrapers from all scraper directories.
 * Returns { stateCode: () => scraperInstance }
 */
function getRegistry() {
  if (_registry) return _registry;

  _registry = {};

  for (const dir of SCRAPER_DIRS) {
    if (!fs.existsSync(dir)) continue;
    const files = fs.readdirSync(dir).filter(f => f.endsWith('.js'));

    for (const file of files) {
      try {
        const scraper = require(path.join(dir, file));
        if (scraper && scraper.stateCode) {
          const code = scraper.stateCode;
          _registry[code] = () => scraper;
        }
      } catch (err) {
        console.error(`Failed to load scraper ${file}: ${err.message}`);
      }
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
      working: WORKING_SCRAPERS.has(code),
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
