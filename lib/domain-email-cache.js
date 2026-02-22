/**
 * Domain Email Cache — caches all emails found per domain
 *
 * When multiple attorneys work at the same firm, we only need to crawl
 * the firm website once. The cache stores all emails found for a domain,
 * and subsequent lookups match by name from the cache.
 *
 * Prevents crawling the same firm website 20x for 20 attorneys.
 */

class DomainEmailCache {
  constructor(maxSize = 500) {
    // domain → { emails: string[], pages_crawled: string[], timestamp: number }
    this._cache = new Map();
    this._maxSize = maxSize;
  }

  /**
   * Get cached data for a domain.
   * @param {string} domain - Domain name (e.g., 'smithlaw.com')
   * @returns {object|null} Cache entry or null if not cached
   */
  get(domain) {
    const key = this._normalizeKey(domain);
    return this._cache.get(key) || null;
  }

  /**
   * Store email data for a domain.
   * @param {string} domain - Domain name
   * @param {object} data - { emails: string[], pages_crawled?: string[], timestamp?: number }
   */
  set(domain, data) {
    const key = this._normalizeKey(domain);
    // Evict oldest entry if at capacity
    if (this._cache.size >= this._maxSize && !this._cache.has(key)) {
      const oldest = this._cache.keys().next().value;
      this._cache.delete(oldest);
    }
    this._cache.set(key, {
      emails: data.emails || [],
      pages_crawled: data.pages_crawled || [],
      timestamp: data.timestamp || Date.now(),
    });
  }

  /**
   * Check if a domain is cached.
   */
  has(domain) {
    return this._cache.has(this._normalizeKey(domain));
  }

  /**
   * Get cache stats.
   */
  getStats() {
    let totalEmails = 0;
    for (const entry of this._cache.values()) {
      totalEmails += entry.emails.length;
    }
    return {
      domains: this._cache.size,
      totalEmails,
    };
  }

  /**
   * Clear the cache.
   */
  clear() {
    this._cache.clear();
  }

  /**
   * Normalize a domain key: lowercase, strip www.
   */
  _normalizeKey(domain) {
    return (domain || '').toLowerCase().replace(/^www\./, '').trim();
  }
}

module.exports = DomainEmailCache;
