/**
 * Logger — colored console output, summary stats, and event emitter mode
 *
 * createLogger(emitter?) returns a log object that:
 *   - Always prints to console (colored with chalk)
 *   - If an emitter is provided, also emits { level, message } events
 */

const chalk = require('chalk');

const startTime = Date.now();

function timestamp() {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  return chalk.gray(`[${elapsed}s]`);
}

/**
 * Create a logger. If an EventEmitter is passed, log events are also
 * emitted as { type: 'log', level, message } on that emitter.
 */
function createLogger(emitter) {
  function emit(level, message) {
    if (emitter) {
      emitter.emit('log', { level, message });
    }
  }

  return {
    info: (msg) => {
      console.log(`${timestamp()} ${chalk.blue('ℹ')} ${msg}`);
      emit('info', msg);
    },
    success: (msg) => {
      console.log(`${timestamp()} ${chalk.green('✓')} ${msg}`);
      emit('success', msg);
    },
    warn: (msg) => {
      console.log(`${timestamp()} ${chalk.yellow('⚠')} ${msg}`);
      emit('warn', msg);
    },
    error: (msg) => {
      console.log(`${timestamp()} ${chalk.red('✗')} ${msg}`);
      emit('error', msg);
    },
    skip: (msg) => {
      console.log(`${timestamp()} ${chalk.gray('↷')} ${msg}`);
      emit('skip', msg);
    },
    scrape: (msg) => {
      console.log(`${timestamp()} ${chalk.cyan('🔍')} ${msg}`);
      emit('scrape', msg);
    },
    email: (msg) => {
      console.log(`${timestamp()} ${chalk.magenta('📧')} ${msg}`);
      emit('email', msg);
    },
    dedup: (msg) => {
      console.log(`${timestamp()} ${chalk.yellow('🔄')} ${msg}`);
      emit('dedup', msg);
    },
    progress: (current, total, msg) => {
      const pct = total > 0 ? Math.round((current / total) * 100) : 0;
      const bar = '█'.repeat(Math.floor(pct / 5)) + '░'.repeat(20 - Math.floor(pct / 5));
      console.log(`${timestamp()} ${chalk.cyan(bar)} ${pct}% ${msg}`);
      emit('progress', msg);
    },
  };
}

// Default logger (no emitter — console only, backwards compatible)
const log = createLogger(null);

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
  if (stats.rateLimited) {
    console.log(`  ${chalk.red('Rate limited:')}       ${stats.rateLimited}`);
  }
  console.log(`  ${chalk.gray('Time elapsed:')}       ${elapsed}s`);
  if (stats.outputFile) {
    console.log(`  ${chalk.green('Output file:')}        ${stats.outputFile}`);
  }
  console.log(chalk.bold('═══════════════════════════════════════\n'));
}

module.exports = { log, createLogger, printSummary };
