/**
 * Logger — colored console output and summary stats
 */

const chalk = require('chalk');

const startTime = Date.now();

function timestamp() {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  return chalk.gray(`[${elapsed}s]`);
}

const log = {
  info: (msg) => console.log(`${timestamp()} ${chalk.blue('ℹ')} ${msg}`),
  success: (msg) => console.log(`${timestamp()} ${chalk.green('✓')} ${msg}`),
  warn: (msg) => console.log(`${timestamp()} ${chalk.yellow('⚠')} ${msg}`),
  error: (msg) => console.log(`${timestamp()} ${chalk.red('✗')} ${msg}`),
  skip: (msg) => console.log(`${timestamp()} ${chalk.gray('↷')} ${msg}`),
  scrape: (msg) => console.log(`${timestamp()} ${chalk.cyan('🔍')} ${msg}`),
  email: (msg) => console.log(`${timestamp()} ${chalk.magenta('📧')} ${msg}`),
  dedup: (msg) => console.log(`${timestamp()} ${chalk.yellow('🔄')} ${msg}`),
  progress: (current, total, msg) => {
    const pct = total > 0 ? Math.round((current / total) * 100) : 0;
    const bar = '█'.repeat(Math.floor(pct / 5)) + '░'.repeat(20 - Math.floor(pct / 5));
    console.log(`${timestamp()} ${chalk.cyan(bar)} ${pct}% ${msg}`);
  },
};

function printSummary(stats) {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n' + chalk.bold('═══════════════════════════════════════'));
  console.log(chalk.bold('  SCRAPE SUMMARY'));
  console.log(chalk.bold('═══════════════════════════════════════'));
  console.log(`  ${chalk.cyan('Total scraped:')}      ${stats.totalScraped || 0}`);
  console.log(`  ${chalk.yellow('Duplicates skipped:')} ${stats.duplicatesSkipped || 0}`);
  console.log(`  ${chalk.green('Net new leads:')}      ${stats.netNew || 0}`);
  console.log(`  ${chalk.magenta('Emails found:')}       ${stats.emailsFound || 0}`);
  if (stats.captchaSkipped) {
    console.log(`  ${chalk.red('CAPTCHA skipped:')}    ${stats.captchaSkipped}`);
  }
  if (stats.errorSkipped) {
    console.log(`  ${chalk.red('Errors skipped:')}     ${stats.errorSkipped}`);
  }
  if (stats.rateimited) {
    console.log(`  ${chalk.red('Rate limited:')}       ${stats.rateLimited}`);
  }
  console.log(`  ${chalk.gray('Time elapsed:')}       ${elapsed}s`);
  if (stats.outputFile) {
    console.log(`  ${chalk.green('Output file:')}        ${stats.outputFile}`);
  }
  console.log(chalk.bold('═══════════════════════════════════════\n'));
}

module.exports = { log, printSummary };
