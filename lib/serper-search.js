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

  /**
   * Search Google Maps for businesses via Serper Maps API
   * @param {string} query - "dentists in Miami" or "lawyers in New York"
   * @param {number} num - Number of results (default 20, max ~20 per page)
   * @returns {Array} - [{title, address, phone, website, rating, ratingCount, category, cid, latitude, longitude}, ...]
   */
  async searchMaps(query, num = 20) {
    if (!this.apiKey) throw new Error('No Serper API key');

    return new Promise((resolve, reject) => {
      const data = JSON.stringify({ q: query, num, type: 'places' });
      const req = https.request({
        hostname: 'google.serper.dev',
        path: '/places',
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
            if (json.places) {
              resolve(json.places.map(p => ({
                title: p.title || '',
                address: p.address || '',
                phone: p.phoneNumber || '',
                website: p.website || '',
                rating: p.rating || 0,
                ratingCount: p.ratingCount || 0,
                category: p.category || '',
                cid: p.cid || '',
                latitude: p.latitude || 0,
                longitude: p.longitude || 0,
                placeId: p.placeId || '',
              })));
            } else {
              resolve([]);
            }
          } catch (e) { reject(new Error('Failed to parse Serper Maps response')); }
        });
      });
      req.on('error', reject);
      req.write(data);
      req.end();
    });
  }

  /**
   * Find businesses by niche and location
   * @param {string} niche - "dentist", "lawyer", "plumber"
   * @param {string} location - "Miami, FL", "New York, NY"
   * @param {number} maxResults - Max businesses to find (default 100)
   * @returns {Array} - Array of business objects
   */
  async findBusinesses(niche, location, maxResults = 100) {
    const businesses = [];
    const seen = new Set();

    // Serper Maps returns ~20 results per query. For more, vary the query.
    const queries = [
      `${niche} in ${location}`,
      `best ${niche} in ${location}`,
      `${niche} near ${location}`,
      `top ${niche} ${location}`,
    ];

    for (const query of queries) {
      if (businesses.length >= maxResults) break;
      try {
        const results = await this.searchMaps(query);
        for (const biz of results) {
          const key = (biz.title + biz.address).toLowerCase().replace(/[^a-z0-9]/g, '');
          if (!seen.has(key) && biz.title) {
            seen.add(key);
            businesses.push(biz);
          }
        }
        // Small delay between queries
        await new Promise(r => setTimeout(r, 500));
      } catch {}
    }

    return businesses.slice(0, maxResults);
  }
}

module.exports = { SerperSearch };
