#!/usr/bin/env node
/**
 * Mortar Lead Scraper — CLI entry point
 *
 * Scrapes law firm leads from bar association directories,
 * deduplicates against existing leads, optionally finds emails
 * from firm websites, and outputs Instantly-compatible CSV.
 *
 * Usage:
 *   node scrape.js --state FL --practice "immigration" --existing ./data/existing_leads.csv
 *   node scrape.js --state FL --practice "immigration" --test
 *   node scrape.js --state FL --practice "immigration" --city Miami --existing ./data/existing_leads.csv
 */

require('dotenv').config();
const { Command } = require('commander');
const { log, printSummary } = require('./lib/logger');
const { readCSV, writeCSV, generateOutputPath } = require('./lib/csv-handler');
const Deduper = require('./lib/deduper');
const EmailFinder = require('./lib/email-finder');

// Scraper registry — add new states here
const SCRAPERS = {
  FL: () => require('./scrapers/bars/florida'),
};

const program = new Command();
program
  .name('mortar-lead-scraper')
  .description('Scrape law firm leads from bar associations and legal directories')
  .requiredOption('--state <code>', 'State code (e.g., FL)')
  .option('--practice <area>', 'Practice area (e.g., "immigration", "family", "criminal")')
  .option('--city <name>', 'Filter by specific city')
  .option('--existing <path>', 'Path to existing leads CSV for deduplication')
  .option('--min-year <year>', 'Minimum bar admission year', parseInt)
  .option('--test', 'Test mode — only first 2 pages per city')
  .option('--no-email-scrape', 'Skip website email scraping (bar emails only)')
  .option('--proxy <url>', 'Proxy URL (http://user:pass@host:port)')
  .option('--output <path>', 'Custom output file path')
  .parse(process.argv);

const opts = program.opts();

async function main() {
  const stateCode = opts.state.toUpperCase();

  // Validate state
  if (!SCRAPERS[stateCode]) {
    log.error(`No scraper available for state: ${stateCode}`);
    log.info(`Available states: ${Object.keys(SCRAPERS).join(', ')}`);
    process.exit(1);
  }

  log.info(`Mortar Lead Scraper — ${stateCode} ${opts.practice || 'all practice areas'}`);
  if (opts.test) log.warn('TEST MODE — limited to 2 pages per city');
  if (opts.city) log.info(`City filter: ${opts.city}`);
  if (opts.minYear) log.info(`Min admission year: ${opts.minYear}`);

  // Step 1: Load existing leads for dedup
  let deduper;
  if (opts.existing) {
    log.info(`Loading existing leads from: ${opts.existing}`);
    try {
      const existingLeads = await readCSV(opts.existing);
      deduper = new Deduper(existingLeads);
      log.success(`Loaded ${existingLeads.length.toLocaleString()} existing leads for dedup`);
    } catch (err) {
      log.error(`Failed to load existing leads: ${err.message}`);
      process.exit(1);
    }
  } else {
    deduper = new Deduper([]);
    log.info('No existing leads file — skipping dedup');
  }

  // Step 2: Initialize scraper
  const scraper = SCRAPERS[stateCode]();
  log.info(`Scraper: ${scraper.name} (${scraper.baseUrl})`);

  // Step 3: Initialize email finder (unless disabled)
  let emailFinder = null;
  if (opts.emailScrape !== false) {
    log.info('Email finder enabled — will visit firm websites for net new leads');
    emailFinder = new EmailFinder({ proxy: opts.proxy });
    await emailFinder.init();
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
    city: opts.city,
    minYear: opts.minYear,
    maxPages: opts.test ? 2 : null,
    proxy: opts.proxy,
  };

  try {
    for await (const result of scraper.search(opts.practice, searchOptions)) {
      // Handle special signals
      if (result._captcha) {
        stats.captchaSkipped++;
        continue;
      }

      stats.totalScraped++;

      // Step 3: Dedup check IMMEDIATELY before any further work
      const { isDuplicate, matchReason } = deduper.check(result);

      if (isDuplicate) {
        stats.duplicatesSkipped++;
        log.skip(`DUP: ${result.first_name} ${result.last_name} (${matchReason})`);
        continue;
      }

      // Step 4: For net new leads without email, try website scraping
      if (!result.email && emailFinder && result.firm_name) {
        // Try to construct a website URL from the firm name
        // The Florida Bar doesn't provide websites, so we skip email-finding
        // unless we have a website URL from another source
        // For bar scraping, we already get the bar email if they have one listed
      }

      // Add to known set to dedup within this scrape run
      deduper.addToKnown(result);

      if (result.email) stats.emailsFound++;
      stats.netNew++;

      // Clean up output record
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
      };

      newLeads.push(lead);

      // Progress log every 50 leads
      if (stats.netNew % 50 === 0) {
        log.progress(stats.netNew, stats.totalScraped,
          `${stats.netNew} new / ${stats.totalScraped} scraped / ${stats.duplicatesSkipped} dupes`);
      }
    }
  } catch (err) {
    log.error(`Scraper error: ${err.message}`);
    stats.errorSkipped++;
  } finally {
    if (emailFinder) await emailFinder.close();
  }

  // Step 5: Write output CSV
  if (newLeads.length > 0) {
    const outputPath = opts.output || generateOutputPath(stateCode, opts.practice);
    await writeCSV(outputPath, newLeads);
    stats.outputFile = outputPath;
    log.success(`Wrote ${newLeads.length} leads to: ${outputPath}`);
  } else {
    log.warn('No new leads found — no output file created');
  }

  // Print summary
  printSummary(stats);
}

main().catch(err => {
  log.error(`Fatal: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
