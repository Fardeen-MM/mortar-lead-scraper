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
const { printSummary } = require('./lib/logger');
const { runPipeline } = require('./lib/pipeline');

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

const emitter = runPipeline({
  state: opts.state,
  practice: opts.practice,
  city: opts.city,
  test: opts.test,
  emailScrape: opts.emailScrape,
  proxy: opts.proxy,
  minYear: opts.minYear,
  output: opts.output,
  existingPath: opts.existing,
});

emitter.on('complete', ({ stats }) => {
  printSummary(stats);
});

emitter.on('error', ({ message }) => {
  console.error(`Fatal: ${message}`);
  process.exit(1);
});
