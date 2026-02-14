const metrics = require('../../lib/metrics');

describe('ScraperMetrics', () => {
  beforeEach(() => {
    metrics.reset();
  });

  test('records requests and successes', () => {
    metrics.record('florida', 'request');
    metrics.record('florida', 'success', { responseTime: 200 });
    metrics.record('florida', 'request');
    metrics.record('florida', 'success', { responseTime: 400 });

    const summary = metrics.getSummary();
    expect(summary.florida.requests).toBe(2);
    expect(summary.florida.successes).toBe(2);
    expect(summary.florida.avgResponseTime).toBe(300);
  });

  test('records failures', () => {
    metrics.record('texas', 'failure', { error: 'timeout' });
    const summary = metrics.getSummary();
    expect(summary.texas.failures).toBe(1);
    expect(summary.texas.lastError).toBe('timeout');
  });

  test('records captchas and rate limits', () => {
    metrics.record('california', 'captcha');
    metrics.record('california', 'rate_limit');
    const summary = metrics.getSummary();
    expect(summary.california.captchas).toBe(1);
    expect(summary.california.rateLimits).toBe(1);
  });

  test('records jobs', () => {
    metrics.recordJob({ state: 'FL', leads: 50 });
    metrics.recordJob({ state: 'TX', leads: 100 });
    const jobs = metrics.getRecentJobs();
    expect(jobs).toHaveLength(2);
    expect(jobs[0].state).toBe('FL');
  });

  test('reset clears data', () => {
    metrics.record('florida', 'request');
    metrics.reset();
    expect(metrics.getSummary()).toEqual({});
  });

  test('reset specific scraper', () => {
    metrics.record('florida', 'request');
    metrics.record('texas', 'request');
    metrics.reset('florida');
    const summary = metrics.getSummary();
    expect(summary.florida).toBeUndefined();
    expect(summary.texas.requests).toBe(1);
  });
});
