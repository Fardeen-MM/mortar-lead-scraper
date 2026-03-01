/**
 * WHOIS Client — domain owner lookup via raw TCP
 *
 * Zero dependencies — uses Node.js built-in `net` module only.
 * Same pattern as email-verifier.js.
 *
 * Supports .com/.net (Verisign), .org (PIR), .co.uk (Nominet),
 * .com.au (auDA), and others via IANA root WHOIS.
 *
 * Usage:
 *   const whois = require('./lib/whois-client');
 *   const info = await whois.lookup('smithdental.com');
 *   // { registrant_name, registrant_email, registrant_phone, organization, ... }
 */

const net = require('net');
const { log } = require('./logger');

// TLD → WHOIS server mapping
const WHOIS_SERVERS = {
  'com':    'whois.verisign-grs.com',
  'net':    'whois.verisign-grs.com',
  'org':    'whois.pir.org',
  'info':   'whois.afilias.net',
  'biz':    'whois.biz',
  'us':     'whois.nic.us',
  'co':     'whois.nic.co',
  'io':     'whois.nic.io',
  'co.uk':  'whois.nic.uk',
  'org.uk': 'whois.nic.uk',
  'uk':     'whois.nic.uk',
  'com.au': 'whois.auda.org.au',
  'au':     'whois.auda.org.au',
  'ca':     'whois.cira.ca',
  'de':     'whois.denic.de',
  'fr':     'whois.nic.fr',
  'nl':     'whois.sidn.nl',
  'eu':     'whois.eu',
  'nz':     'whois.srs.net.nz',
};

// Cache results per domain
const _cache = new Map();
const CACHE_MAX = 500;

/**
 * Send a raw WHOIS query over TCP.
 *
 * @param {string} domain - Domain to query
 * @param {string} server - WHOIS server hostname
 * @param {number} [timeout=10000] - Connection timeout in ms
 * @returns {Promise<string>} Raw WHOIS response text
 */
function rawQuery(domain, server, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let data = '';

    socket.setTimeout(timeout);

    socket.connect(43, server, () => {
      socket.write(domain + '\r\n');
    });

    socket.on('data', chunk => {
      data += chunk.toString('utf-8');
    });

    socket.on('end', () => {
      socket.destroy();
      resolve(data);
    });

    socket.on('timeout', () => {
      socket.destroy();
      reject(new Error(`WHOIS timeout connecting to ${server}`));
    });

    socket.on('error', (err) => {
      socket.destroy();
      reject(err);
    });
  });
}

/**
 * Determine the WHOIS server for a domain based on its TLD.
 */
function getWhoisServer(domain) {
  const parts = domain.split('.');

  // Try compound TLD first (co.uk, com.au)
  if (parts.length >= 3) {
    const compoundTld = parts.slice(-2).join('.');
    if (WHOIS_SERVERS[compoundTld]) return WHOIS_SERVERS[compoundTld];
  }

  // Try simple TLD
  const tld = parts[parts.length - 1];
  return WHOIS_SERVERS[tld] || null;
}

/**
 * Parse WHOIS response to extract registrant information.
 *
 * @param {string} raw - Raw WHOIS response text
 * @returns {object} Parsed registrant info
 */
function parseWhoisResponse(raw) {
  const result = {
    registrant_name: '',
    registrant_email: '',
    registrant_phone: '',
    organization: '',
    registrant_city: '',
    registrant_state: '',
    registrant_country: '',
    creation_date: '',
    expiration_date: '',
    registrar: '',
    raw_length: raw.length,
  };

  // Privacy protection indicators — skip these values
  const PRIVACY_INDICATORS = [
    'redacted', 'privacy', 'data protected', 'private registration',
    'whoisguard', 'domains by proxy', 'contactprivacy', 'perfect privacy',
    'withheld for privacy', 'not disclosed', 'identity protect',
  ];

  function isPrivate(val) {
    if (!val) return true;
    const lower = val.toLowerCase();
    return PRIVACY_INDICATORS.some(p => lower.includes(p));
  }

  const lines = raw.split('\n');

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('%') || trimmed.startsWith('#')) continue;

    // Extract key: value
    const colonIdx = trimmed.indexOf(':');
    if (colonIdx === -1) continue;
    const key = trimmed.substring(0, colonIdx).trim().toLowerCase();
    const val = trimmed.substring(colonIdx + 1).trim();
    if (!val) continue;

    // Registrant Name
    if (key === 'registrant name' && !isPrivate(val)) {
      result.registrant_name = val;
    }

    // Registrant Organization
    if ((key === 'registrant organization' || key === 'registrant org' || key === 'org') && !isPrivate(val)) {
      result.organization = val;
    }

    // Registrant Email
    if (key === 'registrant email' && val.includes('@') && !isPrivate(val)) {
      result.registrant_email = val.toLowerCase();
    }

    // Registrant Phone
    if (key === 'registrant phone' && !isPrivate(val) && /\d/.test(val)) {
      result.registrant_phone = val.replace(/\s+/g, '');
    }

    // Location
    if (key === 'registrant city' && !isPrivate(val)) result.registrant_city = val;
    if ((key === 'registrant state/province' || key === 'registrant state') && !isPrivate(val)) result.registrant_state = val;
    if (key === 'registrant country' && !isPrivate(val)) result.registrant_country = val;

    // Dates
    if (key === 'creation date' || key === 'created') result.creation_date = val;
    if (/^(registry )?expir(y|ation) date$/.test(key) || key === 'expires') result.expiration_date = val;

    // Registrar
    if (key === 'registrar' && !result.registrar) result.registrar = val;
  }

  return result;
}

/**
 * Look up WHOIS information for a domain.
 *
 * For .com/.net domains, Verisign returns a "thin" WHOIS that points to
 * the registrar's WHOIS server. We follow that referral automatically.
 *
 * @param {string} domain - Domain to look up (e.g., "smithdental.com")
 * @returns {Promise<object>} Parsed registrant info
 */
async function lookup(domain) {
  domain = domain.toLowerCase().replace(/^www\./, '');

  // Check cache
  if (_cache.has(domain)) return _cache.get(domain);

  const server = getWhoisServer(domain);
  if (!server) {
    log.warn(`[WHOIS] No WHOIS server known for domain: ${domain}`);
    return null;
  }

  try {
    let raw = await rawQuery(domain, server);

    // Verisign thin WHOIS: follow referral to registrar's WHOIS
    const referralMatch = raw.match(/Registrar WHOIS Server:\s*(.+)/i);
    if (referralMatch && referralMatch[1].trim()) {
      const referralServer = referralMatch[1].trim();
      if (referralServer !== server) {
        try {
          const detailed = await rawQuery(domain, referralServer);
          if (detailed.length > raw.length) {
            raw = detailed;
          }
        } catch {
          // Use thin WHOIS if referral fails
        }
      }
    }

    const result = parseWhoisResponse(raw);
    result.domain = domain;

    // Cache result
    if (_cache.size >= CACHE_MAX) {
      const oldest = _cache.keys().next().value;
      _cache.delete(oldest);
    }
    _cache.set(domain, result);

    return result;
  } catch (err) {
    log.warn(`[WHOIS] Lookup failed for ${domain}: ${err.message}`);
    return null;
  }
}

/**
 * Batch WHOIS lookup for multiple domains.
 *
 * @param {string[]} domains - Array of domains
 * @param {object} [options]
 * @param {function} [options.onProgress] - Progress callback (current, total, domain)
 * @returns {Promise<Map<string, object>>} Map of domain → WHOIS info
 */
async function batchLookup(domains, options = {}) {
  const onProgress = options.onProgress || (() => {});
  const results = new Map();

  for (let i = 0; i < domains.length; i++) {
    const domain = domains[i];
    onProgress(i + 1, domains.length, domain);

    const info = await lookup(domain);
    if (info && (info.registrant_name || info.registrant_email || info.organization)) {
      results.set(domain, info);
    }

    // Rate limit: ~1 req/sec
    if (i < domains.length - 1) {
      await new Promise(r => setTimeout(r, 1100));
    }
  }

  return results;
}

module.exports = { lookup, batchLookup, rawQuery, parseWhoisResponse, getWhoisServer };
