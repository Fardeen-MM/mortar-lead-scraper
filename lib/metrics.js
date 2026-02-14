/**
 * Scraper Metrics — track per-scraper performance and errors
 *
 * Singleton that records events (request, success, failure, captcha, rate_limit)
 * per scraper. Flushes summary to logger at end of each job.
 */

class ScraperMetrics {
  constructor() {
    this.scrapers = {}; // { scraperName: { requests, successes, failures, captchas, rateLimits, totalResponseTime, errors: [] } }
    this.jobs = []; // recent job summaries (keep last 50)
  }

  _ensure(name) {
    if (!this.scrapers[name]) {
      this.scrapers[name] = {
        requests: 0,
        successes: 0,
        failures: 0,
        captchas: 0,
        rateLimits: 0,
        totalResponseTime: 0,
        lastError: null,
        lastRequestAt: null,
      };
    }
    return this.scrapers[name];
  }

  record(scraperName, event, meta = {}) {
    const s = this._ensure(scraperName);
    s.lastRequestAt = new Date().toISOString();

    switch (event) {
      case 'request':
        s.requests++;
        break;
      case 'success':
        s.successes++;
        if (meta.responseTime) s.totalResponseTime += meta.responseTime;
        break;
      case 'failure':
        s.failures++;
        s.lastError = meta.error || 'unknown';
        break;
      case 'captcha':
        s.captchas++;
        break;
      case 'rate_limit':
        s.rateLimits++;
        break;
    }
  }

  getSummary() {
    const summary = {};
    for (const [name, data] of Object.entries(this.scrapers)) {
      summary[name] = {
        ...data,
        avgResponseTime: data.requests > 0
          ? Math.round(data.totalResponseTime / data.successes) || 0
          : 0,
      };
    }
    return summary;
  }

  recordJob(jobInfo) {
    this.jobs.push({
      ...jobInfo,
      completedAt: new Date().toISOString(),
    });
    if (this.jobs.length > 50) this.jobs.shift();
  }

  getRecentJobs() {
    return this.jobs.slice(-20);
  }

  reset(scraperName) {
    if (scraperName) {
      delete this.scrapers[scraperName];
    } else {
      this.scrapers = {};
    }
  }
}

// Singleton
const metrics = new ScraperMetrics();

module.exports = metrics;
