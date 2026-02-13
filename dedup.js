#!/usr/bin/env node
/**
 * Standalone Dedup Tool — compare any CSV against existing leads
 *
 * Usage:
 *   node dedup.js --input raw_leads.csv --existing existing_leads.csv
 *   node dedup.js --input raw_leads.csv --existing existing_leads.csv --output clean_leads.csv
 */

require('dotenv').config();
const { Command } = require('commander');
const { log, printSummary } = require('./lib/logger');
const { readCSV, writeCSV } = require('./lib/csv-handler');
const Deduper = require('./lib/deduper');

const program = new Command();
program
  .name('dedup')
  .description('Deduplicate a CSV of leads against an existing leads CSV')
  .requiredOption('--input <path>', 'Path to input CSV to deduplicate')
  .requiredOption('--existing <path>', 'Path to existing leads CSV')
  .option('--output <path>', 'Output file path (default: input file with -deduped suffix)')
  .parse(process.argv);

const opts = program.opts();

async function main() {
  // Load existing leads
  log.info(`Loading existing leads from: ${opts.existing}`);
  const existingLeads = await readCSV(opts.existing);
  log.success(`Loaded ${existingLeads.length.toLocaleString()} existing leads`);

  // Load input leads
  log.info(`Loading input leads from: ${opts.input}`);
  const inputLeads = await readCSV(opts.input);
  log.success(`Loaded ${inputLeads.length.toLocaleString()} input leads`);

  // Initialize deduper
  const deduper = new Deduper(existingLeads);

  // Dedup
  const cleanLeads = [];
  let dupeCount = 0;

  for (const lead of inputLeads) {
    const { isDuplicate, matchReason } = deduper.check(lead);
    if (isDuplicate) {
      dupeCount++;
      log.skip(`DUP: ${lead.first_name} ${lead.last_name} — ${lead.firm_name} (${matchReason})`);
    } else {
      cleanLeads.push(lead);
      deduper.addToKnown(lead); // Prevent intra-file dupes
    }
  }

  // Write output
  const outputPath = opts.output || opts.input.replace(/\.csv$/, '-deduped.csv');
  await writeCSV(outputPath, cleanLeads);

  // Summary
  const stats = {
    totalScraped: inputLeads.length,
    duplicatesSkipped: dupeCount,
    netNew: cleanLeads.length,
    emailsFound: cleanLeads.filter(l => l.email).length,
    outputFile: outputPath,
  };

  printSummary(stats);
}

main().catch(err => {
  log.error(`Fatal: ${err.message}`);
  process.exit(1);
});
