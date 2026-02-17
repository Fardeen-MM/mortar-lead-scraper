# Mortar Lead Scraper

## Overview

Lawyer/attorney lead scraper covering US states, Canadian provinces, UK, Australia, Europe, and international jurisdictions. Scrapes bar association directories and legal directories, deduplicates against existing leads, optionally enriches via waterfall pipeline, and outputs CSV.

**Stack:** Node.js, Express, Cheerio, WebSocket, vanilla JS frontend (5-step wizard)

## Architecture

```
server.js              — Express + WebSocket server, scrape job management
scrape.js              — CLI entry point (commander)
public/                — Frontend wizard (index.html, app.js, styles.css)
lib/
  pipeline.js          — Orchestrates: Scrape → Dedup → Waterfall → Enrich → CSV
  waterfall.js         — Clay-style multi-source enrichment (profile fetch, cross-ref, email crawl)
  cross-reference.js   — City-batch directory lookups + fuzzy name matching (Fuse.js)
  registry.js          — Discovers scrapers from bars/, directories/, federal/, international/
  normalizer.js        — State codes, phone numbers, firm names, domains
  state-metadata.js    — Jurisdiction codes grouped by country (US/CA/UK/AU/EU/INTL)
  csv-handler.js       — Read/write CSV with column mapping
  deduper.js           — Dedup by email, phone, domain, name+city, firm+state
  email-finder.js      — Puppeteer website crawler for emails (currently disabled)
  enricher.js          — Puppeteer website scraper for title/bio/LinkedIn/education
  domain-email-cache.js— Domain-level email caching for firm websites
  rate-limiter.js      — 5-10s random delays, exponential backoff on 429/403
  logger.js            — Console + EventEmitter logging
  metrics.js           — Request/job metrics tracking
scrapers/
  base-scraper.js      — Abstract base class all scrapers extend
  bars/                — US state + Canadian province + UK scrapers
  directories/         — Martindale, Lawyers.com
  federal/             — (reserved)
  international/       — AU, NZ, FR, IE, IT, HK, SG, etc.
tests/
  verify-load.js       — Confirm all scrapers register without errors
  verify-all.js        — Comprehensive: load test, metadata, normalizer, deduper, pipeline
  api-test-all.js      — HTTP integration test: starts job per scraper, polls for completion
  smoke-test.js        — Direct scraper invocation test (not via server)
  test-normalizer.js   — Unit tests for normalizer
  test-csv-handler.js  — Unit tests for CSV handler
  test-deduper.js      — Unit tests for deduper
  test-enricher.js     — Unit tests for enricher
```

## Pipeline Flow

```
1. Load existing leads (CSV upload or path) → Deduper
2. Initialize scraper from registry by state code
3. Run scraper.search(practiceArea, options) — async generator yields leads
4. For each lead: dedup check → skip or keep
5. Waterfall enrichment (if enabled):
   a. Fetch profile pages (bar directory detail pages)
   b. Cross-reference Martindale (city batch + name match)
   c. Cross-reference Lawyers.com (city batch + name match)
   d. Name-based lookups (CA/NY/AU-NSW APIs)
   e. Firm website email crawl (Puppeteer)
6. Optional enrichment (title, bio, LinkedIn, education)
7. Write CSV output with source tracking columns
```

## How Scrapers Work

All scrapers extend `BaseScraper` from `scrapers/base-scraper.js`.

### Required Properties

```javascript
class MyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'my-state',           // Identifier used in logs and source field
      stateCode: 'XX',            // 2+ letter code (e.g., 'FL', 'CA-AB', 'UK-SC')
      baseUrl: 'https://...',     // Bar directory URL
      pageSize: 50,               // Results per page (default 50)
      practiceAreaCodes: {},       // Map of practice area name → site-specific code
      defaultCities: ['City1'],   // Cities to iterate when no city filter given
    });
  }
}
module.exports = new MyScraper();  // Export singleton instance
```

### Standard Pattern (HTML scraping)

Override these three methods and the base class handles pagination:

```javascript
buildSearchUrl({ city, practiceCode, page }) → URL string
parseResultsPage($) → array of lead objects
extractResultCount($) → total results number
```

### Custom Pattern (API-based or complex sites)

Override `search()` directly as an async generator:

```javascript
async *search(practiceArea, options = {}) {
  // options.maxPages — limit pages in test mode (usually 2)
  // options.maxCities — limit cities in test mode
  // options.city — specific city filter

  yield { _cityProgress: { current: 1, total: 5 } };  // Progress signal

  // Yield leads:
  yield this.transformResult({
    first_name, last_name, firm_name, city, state, phone, email,
    website, bar_number, bar_status, admission_date, source,
    profile_url,  // Optional: enables waterfall profile enrichment
  }, practiceArea);

  // Signal CAPTCHA/inaccessible:
  yield { _captcha: true, city: 'all', reason: 'CAPTCHA detected' };
}
```

### Profile Page Enrichment

Scrapers can optionally implement profile page parsing:

```javascript
// Option A: HTML profile pages accessible via GET
parseProfilePage($) → { phone, email, website, firm_name, ... }

// Option B: Custom enrichment (POST required, API-based, etc.)
async enrichFromProfile(lead, rateLimiter) → { phone, email, ... }
get hasProfileParser() { return true; }
```

### Registration

`lib/registry.js` auto-discovers all `.js` files in the scraper directories. A scraper is registered if it exports an object with a `stateCode` property. The `WORKING_SCRAPERS` Set controls which scrapers appear as "working" in the UI.

## Testing

### Quick Load Check
```bash
node tests/verify-load.js
```

### Comprehensive Verification (no network)
```bash
node tests/verify-all.js
```

### Unit Tests
```bash
node tests/test-normalizer.js
node tests/test-csv-handler.js
node tests/test-deduper.js
node tests/test-enricher.js
```

### Test Single Scraper via CLI
```bash
node scrape.js --state FL --test --no-email-scrape
```

### Test Single Scraper via API (requires server running)
```bash
node server.js &
node tests/api-test-all.js --scrapers=FL --timeout=120000
```

### Full API Regression
```bash
node tests/api-test-all.js --concurrency=3 --timeout=180000
```

### Smoke Test (direct scraper invocation)
```bash
node tests/smoke-test.js
```

## Key Files

| File | Purpose |
|------|---------|
| `server.js` | Express server, WebSocket, job management, API endpoints |
| `scrape.js` | CLI entry point |
| `lib/pipeline.js` | Main pipeline orchestrator |
| `lib/waterfall.js` | Multi-source enrichment engine |
| `lib/registry.js` | Scraper discovery + WORKING_SCRAPERS set |
| `lib/normalizer.js` | Data normalization (phones, states, firms) |
| `lib/rate-limiter.js` | Polite scraping delays (5-10s between requests) |
| `scrapers/base-scraper.js` | Abstract base class for all scrapers |

## Conventions

- **State codes**: US uses 2-letter (`FL`), Canada uses `CA-XX` (`CA-AB`), UK uses `UK-XX` (`UK-SC`), Australia uses `AU-XX` (`AU-NSW`), Europe uses ISO (`FR`, `IE`, `IT`), directories use names (`MARTINDALE`, `LAWYERS-COM`)
- **Source field**: `{scraper_name}_bar` (e.g., `florida_bar`, `martindale`)
- **Placeholder scrapers**: Yield `{ _captcha: true }` for inaccessible sites
- **Test mode**: `maxPages=2`, `maxCities=2` — limits pagination for quick testing
- **Phone normalization**: Strips country codes (+1, +44, +61, etc.) and formatting
- **DE collision**: Germany is `DE-BRAK`, US Delaware is `DE`

## Common Pitfalls

- CV5/cvweb sites may have reCAPTCHA in HTML comments → override `detectCaptcha()`
- ASP.NET ViewState tokens expire → need session refresh logic
- Some APIs return arrays where strings expected (Martindale)
- MN MARS CSV is UTF-16LE encoded
- CT requires `ctl00$ContentPlaceHolder1$` prefix on ASP.NET form fields
- Server must be restarted after scraper code changes (no hot reload)
- Rate limiter delays (5-10s) make tests slow — this is intentional for politeness
