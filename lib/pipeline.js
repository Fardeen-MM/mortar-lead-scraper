/**
 * Pipeline — extracted scrape pipeline callable by both CLI and server
 *
 * Returns an EventEmitter that fires:
 *   'log'      → { level, message }
 *   'progress' → { totalScraped, dupes, netNew, emails }
 *   'lead'     → { data: { first_name, ... } }
 *   'complete' → { stats, outputFile }
 *   'error'    → { message }
 */

const { EventEmitter } = require('events');
const { readCSV, writeCSV, generateOutputPath } = require('./csv-handler');
const Deduper = require('./deduper');
const EmailFinder = require('./email-finder');
const Enricher = require('./enricher');
const { createLogger } = require('./logger');
const { getRegistry } = require('./registry');
const metrics = require('./metrics');
const { runWaterfall } = require('./waterfall');
const DomainEmailCache = require('./domain-email-cache');
const PersonExtractor = require('./person-extractor');

/**
 * Run the full scrape pipeline.
 *
 * @param {object} options
 * @param {string} options.state          - State code (e.g., 'FL')
 * @param {string} [options.practice]     - Practice area name
 * @param {string} [options.city]         - City filter
 * @param {boolean} [options.test]        - Test mode (2 pages per city)
 * @param {boolean} [options.emailScrape] - Enable website email scraping (default true)
 * @param {string} [options.proxy]        - Proxy URL
 * @param {number} [options.minYear]      - Min bar admission year
 * @param {string} [options.output]       - Custom output path
 * @param {Array}  [options.existingLeads] - Pre-loaded existing leads array (from upload)
 * @param {string} [options.existingPath]  - Path to existing leads CSV
 * @param {boolean} [options.enrich]       - Enable lead enrichment phase
 * @param {object} [options.enrichOptions] - Enrichment feature toggles
 * @param {object} [options.waterfall]     - Waterfall enrichment options
 * @param {boolean} [options.waterfall.masterDbLookup]      - Cross-ref master lead DB (default true)
 * @param {boolean} [options.waterfall.fetchProfiles]      - Fetch profile pages (default true)
 * @param {boolean} [options.waterfall.crossRefMartindale] - Cross-ref Martindale (default true)
 * @param {boolean} [options.waterfall.crossRefLawyersCom] - Cross-ref Lawyers.com (default true)
 * @param {boolean} [options.waterfall.nameLookups]        - Name-based API lookups (default true)
 * @param {boolean} [options.waterfall.emailCrawl]         - Crawl firm websites for emails (default true)
 * @param {string} [options.niche]                         - Business niche (e.g. "dentists", "plumbers")
 * @param {boolean} [options.personExtract]                - Extract people from business websites
 * @returns {EventEmitter} emitter — listen for 'log', 'progress', 'lead', 'complete', 'error'
 */
function runPipeline(options = {}) {
  const emitter = new EventEmitter();

  // Create a logger that both prints to console and emits events
  const log = createLogger(emitter);

  // Run async pipeline, forwarding everything through the emitter
  (async () => {
    const stateCode = (options.state || '').toUpperCase().trim();

    const SCRAPERS = getRegistry();
    if (!SCRAPERS[stateCode]) {
      log.error(`No scraper available for state: ${stateCode}`);
      log.info(`Available states: ${Object.keys(SCRAPERS).join(', ')}`);
      emitter.emit('error', { message: `No scraper for state: ${stateCode}` });
      return;
    }

    log.info(`Mortar Lead Scraper — ${stateCode} ${options.practice || 'all practice areas'}`);
    if (options.test) log.warn('TEST MODE — limited to 2 pages per city');
    if (options.city) log.info(`City filter: ${options.city}`);
    if (options.minYear) log.info(`Min admission year: ${options.minYear}`);

    // Step 1: Load existing leads for dedup
    let deduper;
    if (options.existingLeads && options.existingLeads.length > 0) {
      deduper = new Deduper(options.existingLeads);
      log.success(`Loaded ${options.existingLeads.length.toLocaleString()} existing leads for dedup`);
    } else if (options.existingPath) {
      log.info(`Loading existing leads from: ${options.existingPath}`);
      try {
        const existingLeads = await readCSV(options.existingPath);
        deduper = new Deduper(existingLeads);
        log.success(`Loaded ${existingLeads.length.toLocaleString()} existing leads for dedup`);
      } catch (err) {
        log.error(`Failed to load existing leads: ${err.message}`);
        emitter.emit('error', { message: `Failed to load existing leads: ${err.message}` });
        return;
      }
    } else {
      deduper = new Deduper([]);
      log.info('No existing leads — skipping dedup');
    }

    // Step 2: Initialize scraper
    const scraper = SCRAPERS[stateCode]();
    log.info(`Scraper: ${scraper.name} (${scraper.baseUrl})`);

    // Step 3: Initialize email finder (unless disabled)
    let emailFinder = null;
    if (options.emailScrape !== false) {
      log.info('Email finder enabled — will visit firm websites for net new leads');
      try {
        emailFinder = new EmailFinder({ proxy: options.proxy });
        await emailFinder.init();
      } catch (err) {
        log.error(`Email finder failed to initialize: ${err.message} — continuing without email finding`);
        emailFinder = null;
      }
    }

    // Step 4: Scrape
    const stats = {
      totalScraped: 0,
      duplicatesSkipped: 0,
      netNew: 0,
      emailsFound: 0,
      captchaSkipped: 0,
      errorSkipped: 0,
    };

    const newLeads = [];

    const searchOptions = {
      city: options.city,
      minYear: options.minYear,
      maxPages: options.test ? 2 : null,
      maxCities: options.test ? 2 : null,
      maxPrefixes: options.test ? 2 : null,
      proxy: options.proxy,
      niche: options.niche || undefined,
    };

    // Determine if this is a lawyer-specific scrape
    const nicheVal = (options.niche || '').trim().toLowerCase();
    const isLawyerScrape = !nicheVal || /^(lawyers?|law\s*firms?|attorneys?)$/.test(nicheVal);

    // Cancel support
    let cancelled = false;
    emitter.on('cancel', () => { cancelled = true; });

    // City progress tracking
    let cityIndex = 0;
    let totalCities = 0;

    try {
      for await (const result of scraper.search(options.practice, searchOptions)) {
        // Check cancel flag at top of loop
        if (cancelled) {
          log.warn('Scrape cancelled by user');
          break;
        }

        // Track city progress via _cityProgress signals from scraper
        if (result._cityProgress) {
          cityIndex = result._cityProgress.current;
          totalCities = result._cityProgress.total;
          continue;
        }

        // Handle special signals
        if (result._captcha) {
          stats.captchaSkipped++;
          continue;
        }

        stats.totalScraped++;

        // Dedup check
        const { isDuplicate, matchReason } = deduper.check(result);

        if (isDuplicate) {
          stats.duplicatesSkipped++;
          log.skip(`DUP: ${result.first_name} ${result.last_name} (${matchReason})`);
          emitter.emit('progress', {
            totalScraped: stats.totalScraped,
            dupes: stats.duplicatesSkipped,
            netNew: stats.netNew,
            emails: stats.emailsFound,
            cityIndex,
            totalCities,
          });
          continue;
        }

        // Add to known set for within-run dedup
        deduper.addToKnown(result);

        if (result.email) stats.emailsFound++;
        stats.netNew++;

        // Format output record
        const lead = {
          first_name: result.first_name || '',
          last_name: result.last_name || '',
          firm_name: result.firm_name || '',
          practice_area: result.practice_area || '',
          city: result.city || '',
          state: result.state || stateCode,
          phone: result.phone || '',
          website: result.website || '',
          email: result.email || '',
          bar_number: result.bar_number || '',
          admission_date: result.admission_date || '',
          bar_status: result.bar_status || '',
          source: result.source || '',
          profile_url: result.profile_url || '',
          title: '',
          linkedin_url: '',
          bio: '',
          education: '',
          languages: '',
          practice_specialties: '',
          email_source: result.email ? 'bar' : '',
          phone_source: result.phone ? 'bar' : '',
          website_source: result.website ? 'bar' : '',
        };

        newLeads.push(lead);

        // Emit lead event
        emitter.emit('lead', { data: lead });

        // Emit progress
        emitter.emit('progress', {
          totalScraped: stats.totalScraped,
          dupes: stats.duplicatesSkipped,
          netNew: stats.netNew,
          emails: stats.emailsFound,
          cityIndex,
          totalCities,
        });

        // Console progress every 50 leads
        if (stats.netNew % 50 === 0) {
          log.progress(stats.netNew, stats.totalScraped,
            `${stats.netNew} new / ${stats.totalScraped} scraped / ${stats.duplicatesSkipped} dupes`);
        }
      }
    } catch (err) {
      log.error(`Scraper error: ${err.message}`);
      console.error('[pipeline] Scraper error stack:', err.stack);
      stats.errorSkipped++;
    }

    // If cancelled, write partial CSV and emit cancelled-complete
    if (cancelled) {
      if (emailFinder) await emailFinder.close().catch(() => {});
      let outputFile = null;
      if (newLeads.length > 0) {
        outputFile = options.output || generateOutputPath(stateCode, options.practice);
        await writeCSV(outputFile, newLeads);
        stats.outputFile = outputFile;
        log.info(`Wrote ${newLeads.length} partial leads to: ${outputFile}`);
      }
      emitter.emit('cancelled-complete', { stats, outputFile, leads: newLeads });
      return;
    }

    // Step 5: Waterfall enrichment (if we have leads and waterfall options)
    const waterfallOpts = options.waterfall || {};
    const waterfallEnabled = waterfallOpts.masterDbLookup !== false ||
                             waterfallOpts.fetchProfiles !== false ||
                             waterfallOpts.crossRefMartindale !== false ||
                             waterfallOpts.crossRefLawyersCom !== false ||
                             waterfallOpts.nameLookups !== false ||
                             waterfallOpts.emailCrawl !== false;

    if (waterfallEnabled && newLeads.length > 0) {
      log.info(`Starting waterfall enrichment for ${newLeads.length} leads...`);

      const domainCache = new DomainEmailCache();

      try {
        const waterfallStats = await runWaterfall(newLeads, {
          masterDbLookup: waterfallOpts.masterDbLookup !== false,
          fetchProfiles: isLawyerScrape && waterfallOpts.fetchProfiles !== false,
          crossRefMartindale: isLawyerScrape && waterfallOpts.crossRefMartindale !== false,
          crossRefLawyersCom: isLawyerScrape && waterfallOpts.crossRefLawyersCom !== false,
          nameLookups: isLawyerScrape && waterfallOpts.nameLookups !== false,
          emailCrawl: waterfallOpts.emailCrawl !== false && options.emailScrape !== false,
          maxProfileFetches: options.test ? 10 : (waterfallOpts.maxProfileFetches || 200),
          proxy: options.proxy,
          emitter,
          isCancelled: () => cancelled,
          emailFinder: (options.emailScrape !== false) ? emailFinder : null,
          domainCache,
          isLawyerScrape,
        });

        stats.waterfall = waterfallStats;

        // Recount emails after waterfall (some leads may have gained emails)
        stats.emailsFound = newLeads.filter(l => l.email).length;

        if (cancelled) {
          log.warn('Waterfall enrichment cancelled by user');
        } else {
          log.success(
            `Waterfall complete — ${waterfallStats.totalFieldsFilled} fields filled ` +
            `(${waterfallStats.dbLookups || 0} from DB, ${waterfallStats.profilesFetched} profiles, ` +
            `${waterfallStats.crossRefMatches} cross-refs, ${waterfallStats.emailsCrawled} emails crawled)`
          );
        }
      } catch (err) {
        log.error(`Waterfall error: ${err.message}`);
        console.error('[pipeline] Waterfall error stack:', err.stack);
      }

      // Check if cancelled during waterfall
      if (cancelled) {
        if (emailFinder) await emailFinder.close().catch(() => {});
        let outputFile = null;
        if (newLeads.length > 0) {
          outputFile = options.output || generateOutputPath(stateCode, options.practice);
          await writeCSV(outputFile, newLeads);
          stats.outputFile = outputFile;
        }
        emitter.emit('cancelled-complete', { stats, outputFile, leads: newLeads });
        return;
      }
    }

    // Step 5b: Person extraction (for non-lawyer niches with business websites)
    if (options.personExtract && newLeads.length > 0 && !cancelled) {
      const businessesWithWebsite = newLeads.filter(l => l.website && !l.first_name);
      if (businessesWithWebsite.length > 0) {
        log.info(`Starting person extraction for ${businessesWithWebsite.length} businesses...`);
        emitter.emit('person-extract-progress', { current: 0, total: businessesWithWebsite.length, detail: 'Launching browser' });

        const extractor = new PersonExtractor({ proxy: options.proxy });
        try {
          await extractor.init();

          const extractResult = await extractor.batchExtract(
            businessesWithWebsite,
            (current, total, name) => {
              emitter.emit('person-extract-progress', { current, total, detail: name });
            },
            () => cancelled
          );

          stats.personExtract = {
            peopleFound: extractResult.peopleFound,
            websitesVisited: extractResult.websitesVisited,
          };

          // Convert extracted people into leads (each person becomes a separate lead)
          for (const person of extractResult.results) {
            const personLead = {
              first_name: person.first_name || '',
              last_name: person.last_name || '',
              firm_name: person.firm_name || '',
              practice_area: '',
              city: person.city || '',
              state: person.state || stateCode,
              phone: person.phone || '',
              website: person.website || '',
              email: person.email || '',
              bar_number: '',
              admission_date: '',
              bar_status: '',
              source: `person_extract`,
              profile_url: person.linkedin_url || '',
              title: person.title || '',
              linkedin_url: person.linkedin_url || '',
              bio: '',
              education: '',
              languages: '',
              practice_specialties: '',
              email_source: person.email ? 'website' : '',
              phone_source: person.phone ? 'website' : '',
              website_source: person.website ? 'inherited' : '',
            };

            // Dedup the person lead
            const { isDuplicate } = deduper.check(personLead);
            if (!isDuplicate) {
              deduper.addToKnown(personLead);
              newLeads.push(personLead);
              if (personLead.email) stats.emailsFound++;
              stats.netNew++;
              emitter.emit('lead', { data: personLead });
            }
          }

          if (!cancelled) {
            log.success(`Person extraction complete — ${extractResult.peopleFound} people from ${extractResult.websitesVisited} websites`);
          }
        } catch (err) {
          log.error(`Person extraction error: ${err.message}`);
          console.error('[pipeline] Person extraction error stack:', err.stack);
        } finally {
          await extractor.close();
        }

        // Check if cancelled during person extraction
        if (cancelled) {
          if (emailFinder) await emailFinder.close().catch(() => {});
          let outputFile = null;
          if (newLeads.length > 0) {
            outputFile = options.output || generateOutputPath(stateCode, options.practice);
            await writeCSV(outputFile, newLeads);
            stats.outputFile = outputFile;
          }
          emitter.emit('cancelled-complete', { stats, outputFile, leads: newLeads });
          return;
        }
      }
    }

    // Step 6: Deep enrichment phase (if enabled and we have leads)
    if (options.enrich && newLeads.length > 0) {
      log.info(`Starting enrichment for ${newLeads.length} leads...`);

      const enrichOpts = options.enrichOptions || {};
      const enricher = new Enricher({
        deriveWebsite: enrichOpts.deriveWebsite !== false,
        scrapeWebsite: enrichOpts.scrapeWebsite !== false,
        findLinkedIn: enrichOpts.findLinkedIn !== false,
        extractWithAI: enrichOpts.extractWithAI === true,
        proxy: options.proxy,
        emitter,
      });

      try {
        await enricher.init();
        const enrichStats = await enricher.enrichAll(newLeads, () => cancelled);
        stats.enrichment = enrichStats;

        if (cancelled) {
          log.warn('Enrichment cancelled by user');
        } else {
          log.success(`Enrichment complete — ${enrichStats.titlesFound} titles, ${enrichStats.linkedInFound} LinkedIn, ${enrichStats.websitesDerived} websites derived`);
        }
      } catch (err) {
        log.error(`Enrichment error: ${err.message}`);
        console.error('[pipeline] Enrichment error stack:', err.stack);
      } finally {
        await enricher.close();
      }

      // Check if cancelled during enrichment
      if (cancelled) {
        if (emailFinder) await emailFinder.close().catch(() => {});
        let outputFile = null;
        if (newLeads.length > 0) {
          outputFile = options.output || generateOutputPath(stateCode, options.practice);
          await writeCSV(outputFile, newLeads);
          stats.outputFile = outputFile;
        }
        emitter.emit('cancelled-complete', { stats, outputFile, leads: newLeads });
        return;
      }
    }

    // Cleanup: close email finder browser
    if (emailFinder) {
      await emailFinder.close().catch(() => {});
    }

    // Step 7: Write output CSV
    let outputFile = null;
    if (newLeads.length > 0) {
      outputFile = options.output || generateOutputPath(stateCode, options.practice);
      await writeCSV(outputFile, newLeads);
      stats.outputFile = outputFile;
      log.success(`Wrote ${newLeads.length} leads to: ${outputFile}`);
    } else {
      log.warn('No new leads found — no output file created');
    }

    // Record metrics
    metrics.recordJob({
      state: stateCode,
      practice: options.practice || 'all',
      leads: newLeads.length,
      stats,
    });

    // Emit complete
    emitter.emit('complete', { stats, outputFile, leads: newLeads });
  })().catch(err => {
    console.error('[pipeline] Fatal pipeline error:', err.stack || err);
    emitter.emit('log', { level: 'error', message: `Fatal: ${err.message}` });
    emitter.emit('error', { message: err.message });
  });

  return emitter;
}

module.exports = { runPipeline, getRegistry };
