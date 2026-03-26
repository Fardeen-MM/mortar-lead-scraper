/**
 * Serper.dev Search Client — Google Search API
 *
 * 2,500 free searches (one-time), then $1/1K queries.
 * Returns real Google search results as structured JSON.
 *
 * Used for:
 *   - Finding business websites from name + location
 *   - Discovering LinkedIn company pages
 *   - General web research
 *
 * Setup: Set SERPER_API_KEY env var or pass apiKey to constructor.
 * Get your free key at: https://serper.dev (no credit card required)
 *
 * Usage:
 *   const { SerperSearch } = require('./lib/serper-search');
 *   const search = new SerperSearch();
 *   const results = await search.search('Cellino Law New York');
 *   console.log(results[0].link); // https://www.cellinolaw.com
 */

const https = require('https');

const EXCLUDED_DOMAINS = new Set([
  'yelp.com', 'bbb.org', 'manta.com', 'yellowpages.com', 'mapquest.com',
  'facebook.com', 'twitter.com', 'instagram.com', 'tiktok.com', 'pinterest.com',
  'linkedin.com', 'youtube.com', 'reddit.com',
  'google.com', 'bing.com', 'yahoo.com',
  'wikipedia.org', 'wikidata.org',
  'avvo.com', 'justia.com', 'findlaw.com', 'martindale.com', 'lawyers.com',
  'nolo.com', 'superlawyers.com', 'hg.org', 'lawinfo.com',
]);

class SerperSearch {
  constructor(apiKey) {
    this.apiKey = apiKey || process.env.SERPER_API_KEY || '';
    if (!this.apiKey) {
      console.warn('SerperSearch: No API key set. Set SERPER_API_KEY env var or get one free at https://serper.dev');
    }
  }

  /**
   * Search Google via Serper API
   * @param {string} query - Search query
   * @param {number} num - Number of results (default 5)
   * @returns {Array} - [{title, link, snippet, position}, ...]
   */
  async search(query, num = 5) {
    if (!this.apiKey) throw new Error('No Serper API key');

    return new Promise((resolve, reject) => {
      const data = JSON.stringify({ q: query, num });
      const req = https.request({
        hostname: 'google.serper.dev',
        path: '/search',
        method: 'POST',
        headers: {
          'X-API-KEY': this.apiKey,
          'Content-Type': 'application/json',
          'Content-Length': data.length,
        },
      }, res => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => {
          try {
            const json = JSON.parse(body);
            if (json.organic) {
              resolve(json.organic.map(r => ({
                title: r.title || '',
                link: r.link || '',
                snippet: r.snippet || '',
                position: r.position || 0,
              })));
            } else {
              resolve([]);
            }
          } catch (e) { reject(new Error('Failed to parse Serper response')); }
        });
      });
      req.on('error', reject);
      req.write(data);
      req.end();
    });
  }

  /**
   * Find a business website from name + location
   * @param {string} businessName - "Cellino Law"
   * @param {string} location - "New York" (optional)
   * @returns {string|null} - Website URL or null
   */
  async findWebsite(businessName, location) {
    const query = location ? `${businessName} ${location}` : businessName;
    try {
      const results = await this.search(query, 5);
      // Find first result that's not a directory/social media site
      for (const r of results) {
        try {
          const domain = new URL(r.link).hostname.replace(/^www\./, '');
          if (!EXCLUDED_DOMAINS.has(domain) && !domain.endsWith('.gov')) {
            return r.link;
          }
        } catch {}
      }
      return null;
    } catch { return null; }
  }

  /**
   * Find a business's LinkedIn company page
   * @param {string} businessName
   * @returns {string|null} - LinkedIn URL or null
   */
  async findLinkedIn(businessName) {
    try {
      const results = await this.search(`site:linkedin.com/company "${businessName}"`, 3);
      for (const r of results) {
        if (r.link.includes('linkedin.com/company/')) return r.link;
      }
      return null;
    } catch { return null; }
  }
}

module.exports = { SerperSearch };
