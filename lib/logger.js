/**
 * Logger — colored console output, JSON file transport, and event emitter mode
 *
 * createLogger(emitter?) returns a log object that:
 *   - Always prints to console (colored with chalk)
 *   - If an emitter is provided, also emits { level, message } events
 *   - Writes JSON Lines to data/logs/mortar-YYYY-MM-DD.log
 */

const chalk = require('chalk');
const fs = require('fs');
const path = require('path');

const LOG_DIR = path.join(__dirname, '..', 'data', 'logs');
const MAX_LOG_AGE_DAYS = 7;

const startTime = Date.now();

// Ensure log directory exists
try { fs.mkdirSync(LOG_DIR, { recursive: true }); } catch {}

// Clean old log files on startup
try {
  const cutoff = Date.now() - MAX_LOG_AGE_DAYS * 86400000;
  const files = fs.readdirSync(LOG_DIR).filter(f => f.startsWith('mortar-') && f.endsWith('.log'));
  for (const file of files) {
    const stat = fs.statSync(path.join(LOG_DIR, file));
    if (stat.mtimeMs < cutoff) {
      fs.unlinkSync(path.join(LOG_DIR, file));
    }
  }
} catch {}

function getLogFilePath() {
  const date = new Date().toISOString().slice(0, 10);
  return path.join(LOG_DIR, `mortar-${date}.log`);
}

function writeToFile(level, module, message, meta) {
  try {
    const entry = JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      module: module || 'app',
      message,
      meta: meta || undefined,
    }) + '\n';
    fs.appendFileSync(getLogFilePath(), entry);
  } catch {}
}

function timestamp() {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  return chalk.gray(`[${elapsed}s]`);
}

/**
 * Create a logger. If an EventEmitter is passed, log events are also
 * emitted as { type: 'log', level, message } on that emitter.
 */
function createLogger(emitter, module) {
  function emit(level, message) {
    if (emitter) {
      emitter.emit('log', { level, message });
    }
  }

  return {
    info: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.blue('\u2139')} ${msg}`);
      emit('info', msg);
      writeToFile('info', module, msg, meta);
    },
    success: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.green('\u2713')} ${msg}`);
      emit('success', msg);
      writeToFile('success', module, msg, meta);
    },
    warn: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.yellow('\u26A0')} ${msg}`);
      emit('warn', msg);
      writeToFile('warn', module, msg, meta);
    },
    error: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.red('\u2717')} ${msg}`);
      emit('error', msg);
      writeToFile('error', module, msg, meta);
    },
    skip: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.gray('\u21B7')} ${msg}`);
      emit('skip', msg);
      writeToFile('skip', module, msg, meta);
    },
    scrape: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.cyan('🔍')} ${msg}`);
      emit('scrape', msg);
      writeToFile('scrape', module, msg, meta);
    },
    email: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.magenta('📧')} ${msg}`);
      emit('email', msg);
      writeToFile('email', module, msg, meta);
    },
    dedup: (msg, meta) => {
      console.log(`${timestamp()} ${chalk.yellow('🔄')} ${msg}`);
      emit('dedup', msg);
      writeToFile('dedup', module, msg, meta);
    },
    progress: (current, total, msg) => {
      const pct = total > 0 ? Math.round((current / total) * 100) : 0;
      const bar = '\u2588'.repeat(Math.floor(pct / 5)) + '\u2591'.repeat(20 - Math.floor(pct / 5));
      console.log(`${timestamp()} ${chalk.cyan(bar)} ${pct}% ${msg}`);
      emit('progress', msg);
      writeToFile('progress', module, `${pct}% ${msg}`);
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

  writeToFile('summary', 'pipeline', 'Scrape complete', stats);
}

/**
 * Read today's log file tail.
 */
function readLogTail(lines = 100) {
  try {
    const logFile = getLogFilePath();
    if (!fs.existsSync(logFile)) return [];
    const content = fs.readFileSync(logFile, 'utf-8');
    const allLines = content.trim().split('\n').filter(Boolean);
    return allLines.slice(-lines).map(line => {
      try { return JSON.parse(line); } catch { return { raw: line }; }
    });
  } catch {
    return [];
  }
}

module.exports = { log, createLogger, printSummary, readLogTail, LOG_DIR };
