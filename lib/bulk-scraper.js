/**
 * Bulk Scraper — runs all working scrapers sequentially to populate the master DB
 *
 * Features:
 *   - Queues all 50 working scrapers (or a filtered subset)
 *   - Runs them one at a time (or with limited concurrency)
 *   - Tracks progress per scraper
 *   - Saves results to master lead DB
 *   - Emits events for real-time UI updates
 *   - Can be scheduled to run daily via cron
 *
 * Usage:
 *   const bulk = new BulkScraper();
 *   const results = await bulk.run({ test: true });
 */

const { EventEmitter } = require('events');
const { runPipeline } = require('./pipeline');
const { getRegistry, getScraperMetadata } = require('./registry');
const leadDb = require('./lead-db');

class BulkScraper extends EventEmitter {
  constructor(options = {}) {
    super();
    this.concurrency = options.concurrency || 1; // Sequential by default for politeness
    this.running = false;
    this.cancelled = false;
    this.progress = {
      total: 0,
      completed: 0,
      failed: 0,
      skipped: 0,
      totalLeads: 0,
      totalEmails: 0,
      totalNew: 0,
      currentScraper: '',
      results: [],
      startedAt: null,
      completedAt: null,
    };
  }

  /**
   * Run bulk scrape across all working scrapers.
   *
   * @param {object} options
   * @param {boolean} [options.test=false] - Test mode (2 pages per city)
   * @param {string[]} [options.scrapers] - Specific scraper codes to run (default: all working)
   * @param {string[]} [options.countries] - Filter by country ('US', 'CA', 'UK', 'AU', etc.)
   * @param {boolean} [options.emailScrape=false] - Enable website email crawling
   * @param {object} [options.waterfall] - Waterfall enrichment options
   * @returns {object} Final results
   */
  async run(options = {}) {
    if (this.running) {
      throw new Error('Bulk scrape already running');
    }

    this.running = true;
    this.cancelled = false;

    const metadata = getScraperMetadata();

    // Determine which scrapers to run
    let scraperCodes = options.scrapers || Object.entries(metadata)
      .filter(([, m]) => m.working)
      .map(([code]) => code);

    // Filter by country if specified
    if (options.countries) {
      const countries = new Set(options.countries.map(c => c.toUpperCase()));
      scraperCodes = scraperCodes.filter(code => {
        const m = metadata[code];
        return m && countries.has(m.country);
      });
    }

    // Skip directory scrapers in bulk mode (they're for cross-reference, not standalone)
    const skipInBulk = new Set(['MARTINDALE', 'LAWYERS-COM', 'GOOGLE-PLACES', 'JUSTIA', 'AVVO', 'FINDLAW']);
    scraperCodes = scraperCodes.filter(code => !skipInBulk.has(code));

    this.progress = {
      total: scraperCodes.length,
      completed: 0,
      failed: 0,
      skipped: 0,
      totalLeads: 0,
      totalEmails: 0,
      totalNew: 0,
      currentScraper: '',
      results: [],
      startedAt: new Date().toISOString(),
      completedAt: null,
    };

    this.emit('start', { total: scraperCodes.length, scrapers: scraperCodes });
    console.log(`[Bulk] Starting bulk scrape of ${scraperCodes.length} scrapers (test=${!!options.test})`);

    for (const code of scraperCodes) {
      if (this.cancelled) {
        console.log('[Bulk] Cancelled by user');
        break;
      }

      this.progress.currentScraper = code;
      this.emit('scraper-start', { code, index: this.progress.completed + 1, total: this.progress.total });

      try {
        const result = await this._runSingleScraper(code, options);
        this.progress.results.push(result);

        if (result.leads > 0) {
          this.progress.completed++;
          this.progress.totalLeads += result.leads;
          this.progress.totalEmails += result.emails;
          this.progress.totalNew += result.newInDb;
        } else {
          this.progress.skipped++;
        }

        this.emit('scraper-complete', {
          code,
          ...result,
          progress: { ...this.progress },
        });

        console.log(`[Bulk] ${code}: ${result.leads} leads, ${result.emails} emails, ${result.newInDb} new in DB (${result.time}s)`);
      } catch (err) {
        this.progress.failed++;
        const errorResult = { code, leads: 0, emails: 0, newInDb: 0, error: err.message, time: 0 };
        this.progress.results.push(errorResult);
        this.emit('scraper-error', { code, error: err.message });
        console.error(`[Bulk] ${code} FAILED: ${err.message}`);
      }
    }

    this.progress.completedAt = new Date().toISOString();
    this.progress.currentScraper = '';
    this.running = false;

    // Record bulk run
    leadDb.recordScrapeRun({
      state: 'BULK',
      source: 'bulk-scraper',
      leadsFound: this.progress.totalLeads,
      leadsNew: this.progress.totalNew,
      emailsFound: this.progress.totalEmails,
    });

    this.emit('complete', { ...this.progress });
    console.log(`[Bulk] Complete: ${this.progress.completed} scrapers, ${this.progress.totalLeads} leads, ${this.progress.totalEmails} emails, ${this.progress.totalNew} new`);

    return this.progress;
  }

  /**
   * Run a single scraper and save results to master DB.
   */
  _runSingleScraper(code, options = {}) {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();

      const emitter = runPipeline({
        state: code,
        test: !!options.test,
        emailScrape: options.emailScrape || false,
        waterfall: options.waterfall || {
          masterDbLookup: true, // Instant SQLite lookup — always enabled
          fetchProfiles: true,
          crossRefMartindale: false, // Skip cross-ref in bulk (too slow per scraper)
          crossRefLawyersCom: false,
          nameLookups: false,
          emailCrawl: false,
        },
      });

      let leads = [];

      emitter.on('lead', d => leads.push(d.data));

      emitter.on('complete', (data) => {
        const time = Math.round((Date.now() - startTime) / 1000);

        // Save to master DB
        let dbStats = { inserted: 0, updated: 0, unchanged: 0 };
        if (leads.length > 0) {
          try {
            dbStats = leadDb.batchUpsert(leads, `scraper:${code}`);
          } catch (err) {
            console.error(`[Bulk] DB save failed for ${code}: ${err.message}`);
          }
        }

        resolve({
          code,
          leads: leads.length,
          emails: data.stats.emailsFound || 0,
          waterfall: data.stats.waterfall || null,
          newInDb: dbStats.inserted,
          updatedInDb: dbStats.updated,
          time,
        });
      });

      emitter.on('error', (data) => {
        reject(new Error(data.message));
      });

      // Timeout per scraper (5 min in test mode, 30 min in full mode)
      const timeout = options.test ? 5 * 60 * 1000 : 30 * 60 * 1000;
      setTimeout(() => {
        emitter.emit('cancel');
        reject(new Error('Timeout'));
      }, timeout);
    });
  }

  /**
   * Cancel the current bulk scrape.
   */
  cancel() {
    this.cancelled = true;
  }

  /**
   * Get current progress.
   */
  getProgress() {
    return { running: this.running, ...this.progress };
  }
}

module.exports = BulkScraper;
