/**
 * Rate Limiter — polite scraping with exponential backoff
 *
 * - Random 5-10s delays between requests
 * - Exponential backoff on 429/403 (30s → 60s → 120s)
 * - 5-minute pause after 3 consecutive blocks
 * - User agent rotation
 */

const { log } = require('./logger');

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

class RateLimiter {
  constructor(options = {}) {
    this.minDelay = options.minDelay || 5000;  // 5 seconds
    this.maxDelay = options.maxDelay || 10000;  // 10 seconds
    this.consecutiveBlocks = 0;
    this.backoffMultiplier = 1;
    this.uaIndex = Math.floor(Math.random() * USER_AGENTS.length);
  }

  /**
   * Wait a random delay between requests.
   */
  async wait() {
    const delay = this.minDelay + Math.random() * (this.maxDelay - this.minDelay);
    await sleep(delay);
  }

  /**
   * Handle a blocked response (429 or 403).
   * Returns true if we should retry, false if we should give up.
   */
  async handleBlock(statusCode) {
    this.consecutiveBlocks++;

    if (this.consecutiveBlocks >= 3) {
      log.warn(`3 consecutive blocks — pausing for 5 minutes`);
      await sleep(300_000); // 5 minutes
      this.consecutiveBlocks = 0;
      this.backoffMultiplier = 1;
      return true;
    }

    const backoff = 30_000 * this.backoffMultiplier; // 30s, 60s, 120s
    log.warn(`Got ${statusCode} — backing off ${backoff / 1000}s (attempt ${this.consecutiveBlocks}/3)`);
    await sleep(backoff);
    this.backoffMultiplier *= 2;
    return true;
  }

  /**
   * Reset backoff after a successful request.
   */
  resetBackoff() {
    this.consecutiveBlocks = 0;
    this.backoffMultiplier = 1;
  }

  /**
   * Get a random user agent.
   */
  getUserAgent() {
    this.uaIndex = (this.uaIndex + 1) % USER_AGENTS.length;
    return USER_AGENTS[this.uaIndex];
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { RateLimiter, sleep, USER_AGENTS };
