/**
 * Google search via apify/impit (TLS fingerprint impersonation).
 * Bypasses Google's basic bot detection by matching real Chrome TLS JA3/JA4.
 * For scale, layer residential proxy rotation on top.
 */
const { Impit } = require('impit');
const cheerio = require('cheerio');

const BROWSERS = ['chrome', 'firefox', 'safari'];
let impitInstances = new Map();

function getImpit(browser = 'chrome') {
  if (!impitInstances.has(browser)) {
    impitInstances.set(browser, new Impit({ browser, ignoreTlsErrors: true }));
  }
  return impitInstances.get(browser);
}

/**
 * Google Search HTML scrape via TLS fingerprint spoofing.
 * Returns array of {title, url, snippet}.
 */
async function googleSearch(query, options = {}) {
  const { hl = 'en', gl = 'us', num = 100, location = '', start = 0 } = options;
  const browser = BROWSERS[Math.floor(Math.random() * BROWSERS.length)];
  const impit = getImpit(browser);

  const params = new URLSearchParams({ q: query, hl, gl, num: String(num), start: String(start) });
  if (location) params.set('uule', `w+CAIQICI${Buffer.from(location).toString('base64').replace(/=/g, '')}`);

  const url = `https://www.google.com/search?${params}`;
  const res = await impit.fetch(url);
  if (!res.ok) throw new Error(`Google HTTP ${res.status}`);
  const html = await res.text();

  const $ = cheerio.load(html);
  const results = [];
  $('div.g, div.tF2Cxc, div[data-hveid]').each((_, el) => {
    const $el = $(el);
    const title = $el.find('h3').first().text().trim();
    const url = $el.find('a').first().attr('href') || '';
    const snippet = $el.find('[data-sncf="1"], .VwiC3b, span.aCOpRe').first().text().trim();
    if (title && url && url.startsWith('http')) {
      results.push({ title, url, snippet });
    }
  });

  // Detect block
  if (!results.length) {
    if (html.includes('unusual traffic') || html.includes('captcha')) {
      throw new Error('Google blocked: CAPTCHA/unusual traffic');
    }
  }

  return { query, results, raw_html_size: html.length };
}

/**
 * Search for email with domain constraint.
 * E.g., findEmailForPerson('John Smith', 'acme.com')
 */
async function findEmailForPerson(fullName, domain) {
  const query = `"${fullName}" "@${domain}" OR "${domain}" email contact`;
  const { results } = await googleSearch(query);
  const emailRegex = new RegExp(`[a-z0-9._+\\-]+@${domain.replace('.', '\\.')}`, 'gi');
  const found = new Set();
  for (const r of results) {
    const matches = (r.snippet + ' ' + r.title).match(emailRegex) || [];
    for (const m of matches) found.add(m.toLowerCase());
  }
  return { query, candidates: [...found], result_count: results.length };
}

/**
 * Search for businesses in a location: "immigration lawyers Miami"
 */
async function findBusinesses(niche, city, options = {}) {
  const query = `${niche} ${city}${options.country ? ' ' + options.country : ''}`;
  const { results } = await googleSearch(query, { location: `${city}, ${options.country || 'United States'}` });
  return results.map(r => {
    try {
      const host = new URL(r.url).hostname.replace(/^www\./, '');
      return { ...r, domain: host };
    } catch { return r; }
  });
}

module.exports = { googleSearch, findEmailForPerson, findBusinesses };

// CLI: node lib/google-impit.js "immigration lawyers Miami"
if (require.main === module) {
  const q = process.argv.slice(2).join(' ') || 'immigration lawyers Miami';
  googleSearch(q).then(r => {
    console.log(`Query: ${r.query}`);
    console.log(`Results: ${r.results.length}`);
    console.log(`Raw HTML: ${r.raw_html_size} bytes`);
    console.log('\nFirst 5:');
    r.results.slice(0, 5).forEach((x, i) => {
      console.log(`${i+1}. ${x.title}`);
      console.log(`   ${x.url}`);
      console.log(`   ${x.snippet.slice(0, 100)}`);
    });
  }).catch(err => { console.error('ERROR:', err.message); process.exit(1); });
}
