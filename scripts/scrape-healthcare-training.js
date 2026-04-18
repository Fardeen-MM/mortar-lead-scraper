#!/usr/bin/env node
/**
 * Healthcare Training Program Scraper
 *
 * Sources:
 *   1. CAAHEP  — 2,100+ accredited allied health programs (POST API)
 *   2. CMS Nursing Home Compare — 14,700+ nursing homes (JSON API)
 *   3. CMS Home Health Agencies — 12,200+ agencies (JSON API)
 *   4. PTCAS/APTA — 318 accredited PT programs (HTML scrape)
 *
 * Output: output/healthcare-training-programs.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'healthcare-training-programs.csv');
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';

// ─── Helpers ───────────────────────────────────────────────────────────────────

function fetchJSON(url, options = {}) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const urlObj = new URL(url);
    const reqOpts = {
      hostname: urlObj.hostname,
      port: urlObj.port || (url.startsWith('https') ? 443 : 80),
      path: urlObj.pathname + urlObj.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        ...(options.headers || {}),
      },
    };

    const req = mod.request(reqOpts, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          resolve({ data: parsed, headers: res.headers });
        } catch (e) {
          reject(new Error(`JSON parse error from ${url}: ${e.message} — body: ${data.slice(0, 200)}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });

    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

function fetchHTML(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const urlObj = new URL(url);
    const reqOpts = {
      hostname: urlObj.hostname,
      port: urlObj.port || (url.startsWith('https') ? 443 : 80),
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
      },
    };

    const req = mod.request(reqOpts, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirect = res.headers.location.startsWith('http')
          ? res.headers.location
          : `https://${urlObj.hostname}${res.headers.location}`;
        return fetchHTML(redirect).then(resolve).catch(reject);
      }
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve(data));
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

function formatPhone(raw) {
  if (!raw) return '';
  const digits = String(raw).replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return raw;
}

function csvEscape(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function csvRow(obj) {
  return [
    obj.email, obj.first_name, obj.last_name, obj.company,
    obj.phone, obj.city, obj.state, obj.country, obj.source,
  ].map(csvEscape).join(',');
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function titleCase(s) {
  if (!s) return '';
  return s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
}

// ─── 1. CAAHEP Programs (POST API) ────────────────────────────────────────────

const CAAHEP_API = 'https://search-api.caahep.org/api/programs?code=jTWtFM5JdJXTkfWQKs7aoKS2tDadlFZnSuLT0V1N2CbIr70RiqjabA==';
const CAAHEP_PROFESSIONS_API = 'https://search-api.caahep.org/api/professions?code=jTWtFM5JdJXTkfWQKs7aoKS2tDadlFZnSuLT0V1N2CbIr70RiqjabA==';

async function scrapeCAAHEP() {
  const leads = [];
  console.log('\n[CAAHEP] Fetching profession list...');

  let professions;
  try {
    const { data } = await fetchJSON(CAAHEP_PROFESSIONS_API);
    professions = data;
    console.log(`[CAAHEP] Found ${professions.length} professions`);
  } catch (e) {
    console.error(`[CAAHEP] Failed to fetch professions: ${e.message}`);
    return leads;
  }

  // Fetch all programs at once with large page size
  let page = 1;
  const pageSize = 200;
  let totalFetched = 0;
  let totalCount = 0;

  console.log('[CAAHEP] Fetching all programs...');

  while (true) {
    const body = JSON.stringify({
      searchText: '',
      professionIds: [],
      sortStatuses: [1],  // Accredited
      degreeDiploma: true,
      degreeAssociate: true,
      degreeBaccalaureate: true,
      degreeCertificate: true,
      degreeMasters: true,
      addOnTracks: [],
      concentrations: [],
      levels: [],
      isInternational: false,
      paging: { pageNumber: page, maxPageSize: pageSize, pageSize: pageSize },
    });

    try {
      const { data, headers } = await fetchJSON(CAAHEP_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      });

      if (!data || !Array.isArray(data) || data.length === 0) break;

      // Parse X-Pagination header for total count
      if (headers['x-pagination'] && totalCount === 0) {
        try {
          const pagination = JSON.parse(headers['x-pagination']);
          totalCount = pagination.totalCount || 0;
          console.log(`[CAAHEP] Total programs: ${totalCount}`);
        } catch (e) { /* ignore */ }
      }

      for (const prog of data) {
        // Institution name is the company; programName is the degree name
        const institution = prog.institutionName || '';
        const programName = prog.programName || prog.name || '';
        const professionName = prog.professionName || '';

        // Program Director has the best contact info
        const pd = prog.programDirector || {};
        const addr = prog.programAddress || {};

        const firstName = pd.firstName || '';
        const lastName = pd.lastName || '';
        const email = (pd.email || '').toLowerCase().trim();
        const phone = formatPhone(pd.phone || addr.phone || '');
        const city = pd.city || addr.city || '';
        const state = pd.state || addr.state || '';
        const country = pd.country || addr.country || '';

        // Determine country code
        let countryCode = 'US';
        if (country && country.toLowerCase().includes('canada')) countryCode = 'CA';
        else if (country && country.toLowerCase().includes('new zealand')) countryCode = 'NZ';
        else if (country && country.toLowerCase().includes('australia')) countryCode = 'AU';
        else if (country && country.toLowerCase().includes('united kingdom')) countryCode = 'UK';
        else if (prog.isInternational) countryCode = 'INTL';

        // Company = "Institution Name — Program Name" for clarity
        const company = institution
          ? (programName && programName !== institution ? `${institution} — ${programName}` : institution)
          : programName;

        leads.push({
          email,
          first_name: firstName,
          last_name: lastName,
          company,
          phone,
          city: titleCase(city),
          state: (state || '').toUpperCase(),
          country: countryCode,
          source: `CAAHEP-${professionName}`,
        });
      }

      totalFetched += data.length;
      process.stdout.write(`\r[CAAHEP] Fetched ${totalFetched}/${totalCount || '?'} programs...`);

      if (data.length < pageSize) break;
      page++;
      await sleep(300);
    } catch (e) {
      console.error(`\n[CAAHEP] Error on page ${page}: ${e.message}`);
      break;
    }
  }

  console.log(`\n[CAAHEP] Done: ${leads.length} programs scraped`);
  return leads;
}

// ─── 2. CMS Nursing Home Compare (JSON API) ──────────────────────────────────

const CMS_NURSING_HOME_API = 'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0';

async function scrapeCMSNursingHomes() {
  const leads = [];
  const batchSize = 1000;
  let offset = 0;
  let total = 0;

  console.log('\n[CMS-NursingHomes] Starting fetch...');

  while (true) {
    const url = `${CMS_NURSING_HOME_API}?limit=${batchSize}&offset=${offset}`;
    try {
      const { data } = await fetchJSON(url);
      if (!data.results || data.results.length === 0) break;
      if (total === 0) {
        total = data.count || 0;
        console.log(`[CMS-NursingHomes] Total records: ${total}`);
      }

      for (const rec of data.results) {
        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company: titleCase(rec.provider_name || ''),
          phone: formatPhone(rec.telephone_number || ''),
          city: titleCase(rec.citytown || ''),
          state: (rec.state || '').toUpperCase(),
          country: 'US',
          source: 'CMS-NursingHome',
        });
      }

      offset += data.results.length;
      process.stdout.write(`\r[CMS-NursingHomes] Fetched ${offset}/${total}...`);

      if (data.results.length < batchSize) break;
      await sleep(200);
    } catch (e) {
      console.error(`\n[CMS-NursingHomes] Error at offset ${offset}: ${e.message}`);
      // Retry once
      await sleep(2000);
      try {
        const { data } = await fetchJSON(url);
        if (!data.results || data.results.length === 0) break;
        for (const rec of data.results) {
          leads.push({
            email: '',
            first_name: '',
            last_name: '',
            company: titleCase(rec.provider_name || ''),
            phone: formatPhone(rec.telephone_number || ''),
            city: titleCase(rec.citytown || ''),
            state: (rec.state || '').toUpperCase(),
            country: 'US',
            source: 'CMS-NursingHome',
          });
        }
        offset += data.results.length;
      } catch (e2) {
        console.error(`\n[CMS-NursingHomes] Retry failed: ${e2.message}`);
        break;
      }
    }
  }

  console.log(`\n[CMS-NursingHomes] Done: ${leads.length} nursing homes`);
  return leads;
}

// ─── 3. CMS Home Health Agencies (JSON API) ──────────────────────────────────

const CMS_HOME_HEALTH_API = 'https://data.cms.gov/provider-data/api/1/datastore/query/6jpm-sxkc/0';

async function scrapeCMSHomeHealth() {
  const leads = [];
  const batchSize = 1000;
  let offset = 0;
  let total = 0;

  console.log('\n[CMS-HomeHealth] Starting fetch...');

  while (true) {
    const url = `${CMS_HOME_HEALTH_API}?limit=${batchSize}&offset=${offset}`;
    try {
      const { data } = await fetchJSON(url);
      if (!data.results || data.results.length === 0) break;
      if (total === 0) {
        total = data.count || 0;
        console.log(`[CMS-HomeHealth] Total records: ${total}`);
      }

      for (const rec of data.results) {
        leads.push({
          email: '',
          first_name: '',
          last_name: '',
          company: titleCase(rec.provider_name || ''),
          phone: formatPhone(rec.telephone_number || ''),
          city: titleCase(rec.citytown || ''),
          state: (rec.state || '').toUpperCase(),
          country: 'US',
          source: 'CMS-HomeHealth',
        });
      }

      offset += data.results.length;
      process.stdout.write(`\r[CMS-HomeHealth] Fetched ${offset}/${total}...`);

      if (data.results.length < batchSize) break;
      await sleep(200);
    } catch (e) {
      console.error(`\n[CMS-HomeHealth] Error at offset ${offset}: ${e.message}`);
      await sleep(2000);
      try {
        const { data } = await fetchJSON(url);
        if (!data.results || data.results.length === 0) break;
        for (const rec of data.results) {
          leads.push({
            email: '',
            first_name: '',
            last_name: '',
            company: titleCase(rec.provider_name || ''),
            phone: formatPhone(rec.telephone_number || ''),
            city: titleCase(rec.citytown || ''),
            state: (rec.state || '').toUpperCase(),
            country: 'US',
            source: 'CMS-HomeHealth',
          });
        }
        offset += data.results.length;
      } catch (e2) {
        console.error(`\n[CMS-HomeHealth] Retry failed: ${e2.message}`);
        break;
      }
    }
  }

  console.log(`\n[CMS-HomeHealth] Done: ${leads.length} home health agencies`);
  return leads;
}

// ─── 4. PTCAS / APTA PT Programs (HTML Scrape) ──────────────────────────────

const PTCAS_BASE = 'https://ptcasdirectory.apta.org/39/List-of-PTCAS-Programs';

async function scrapePTCAS() {
  const leads = [];
  let page = 1;
  const pageSize = 100;

  console.log('\n[PTCAS] Scraping PT programs...');

  while (true) {
    const url = `${PTCAS_BASE}?pn=${page}&ps=${pageSize}`;
    try {
      const html = await fetchHTML(url);

      // Extract program entries using regex on the HTML
      // Programs are in card/list format with institution name, city, state, phone, email

      // Look for program blocks - each program has institution name, location, contact info
      // Pattern: institution name in heading/link, then address, phone, email

      const programs = [];

      // Extract email addresses
      const emailPattern = /[\w.+-]+@[\w.-]+\.\w{2,}/g;
      const emails = [...new Set((html.match(emailPattern) || []).map(e => e.toLowerCase()))];

      // Filter out non-program emails (analytics, tracking, etc.)
      const programEmails = emails.filter(e =>
        !e.includes('google') && !e.includes('analytics') && !e.includes('tracking') &&
        !e.includes('sitecore') && !e.includes('coveo') && !e.includes('cookie') &&
        !e.includes('noreply') && !e.includes('webmaster') && !e.includes('tealium')
      );

      // Extract program entries - look for institution blocks
      // The PTCAS directory has structured cards with: name, department, address, phone, email, website

      // Try to parse structured blocks
      // Each program appears as a block with institution name followed by contact details
      const blockPattern = /class="[^"]*program[^"]*"[^>]*>[\s\S]*?<\/(?:div|li|article)>/gi;
      const blocks = html.match(blockPattern) || [];

      // Alternative: parse by looking for phone+email pairs near institution names
      // Phone pattern: xxx-xxx-xxxx or (xxx) xxx-xxxx
      const phonePattern = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const phones = html.match(phonePattern) || [];

      // Parse the structured directory entries
      // Each entry has: institution name (in bold/heading), location line, phone, email
      // The actual format has them in a list with links

      // Use a simpler approach: extract all "institution-like" entries
      // Look for the pattern: Name ... City, ST ... phone ... email

      // Split by institution markers - common patterns in the HTML
      const entryPattern = /<h[2-5][^>]*>([^<]+)<\/h[2-5]>/gi;
      let match;
      const institutions = [];
      while ((match = entryPattern.exec(html)) !== null) {
        institutions.push({ name: match[1].trim(), pos: match.index });
      }

      // Also try: links that look like institution names (with .edu domains)
      const linkPattern = /<a[^>]*href="[^"]*"[^>]*>([^<]{10,})<\/a>/gi;
      while ((match = linkPattern.exec(html)) !== null) {
        const text = match[1].trim();
        if (text.match(/university|college|institute|school|medical|health/i)) {
          institutions.push({ name: text, pos: match.index });
        }
      }

      // If we found structured entries, great. Otherwise fall back to regex extraction
      if (institutions.length === 0 && programEmails.length === 0) {
        // Check if page has no results (end of pagination)
        if (html.includes('No results') || html.includes('0 of 0') || !html.includes('ptcas')) {
          break;
        }
      }

      // More targeted extraction: look for structured data in the directory
      // The PTCAS format seems to have program names followed by details in spans/divs

      // Extract all unique email-bearing entries
      // Each email typically belongs to one program

      // Let's parse the full text and extract entries between known separators
      const textOnly = html.replace(/<[^>]+>/g, '\n').replace(/\n{3,}/g, '\n\n');
      const lines = textOnly.split('\n').map(l => l.trim()).filter(Boolean);

      // Walk through lines looking for institution name + email + phone patterns
      let currentInst = null;
      let currentCity = '';
      let currentState = '';
      let currentPhone = '';
      let currentEmail = '';

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // Institution name detection
        if (line.match(/university|college|institute|school|medical center/i) &&
            line.length > 10 && line.length < 120 &&
            !line.match(/^(the |a |an |search|select|display|filter|about)/i)) {

          // Save previous entry
          if (currentInst && (currentEmail || currentPhone)) {
            leads.push({
              email: currentEmail,
              first_name: '',
              last_name: '',
              company: currentInst,
              phone: formatPhone(currentPhone),
              city: titleCase(currentCity),
              state: currentState.toUpperCase(),
              country: 'US',
              source: 'PTCAS-PT-Program',
            });
          }

          currentInst = line;
          currentCity = '';
          currentState = '';
          currentPhone = '';
          currentEmail = '';
        }

        // Email detection
        const emailMatch = line.match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
        if (emailMatch && currentInst) {
          const em = emailMatch[0].toLowerCase();
          if (!em.includes('google') && !em.includes('analytics')) {
            currentEmail = em;
          }
        }

        // Phone detection
        const phoneMatch = line.match(/\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
        if (phoneMatch && currentInst) {
          currentPhone = phoneMatch[0];
        }

        // City, State detection (e.g., "Chicago, IL" or "Chicago IL 60601")
        const locMatch = line.match(/^([A-Za-z\s.'-]+),?\s+([A-Z]{2})\b/);
        if (locMatch && currentInst && !currentCity) {
          currentCity = locMatch[1].trim();
          currentState = locMatch[2];
        }
      }

      // Save last entry
      if (currentInst && (currentEmail || currentPhone)) {
        leads.push({
          email: currentEmail,
          first_name: '',
          last_name: '',
          company: currentInst,
          phone: formatPhone(currentPhone),
          city: titleCase(currentCity),
          state: currentState.toUpperCase(),
          country: 'US',
          source: 'PTCAS-PT-Program',
        });
      }

      const beforeCount = leads.length;
      process.stdout.write(`\r[PTCAS] Page ${page}: found entries (total so far: ${leads.length})...`);

      // Check for end of pagination
      if (html.includes(`Displaying`) && html.includes(`of `)) {
        const pagMatch = html.match(/Displaying\s+\d+\s*-\s*(\d+)\s+of\s+(\d+)/i);
        if (pagMatch) {
          const shown = parseInt(pagMatch[1]);
          const totalPrograms = parseInt(pagMatch[2]);
          if (shown >= totalPrograms) break;
        }
      }

      // If no new leads were found on this page, we may have reached the end
      if (leads.length === beforeCount && page > 1) break;

      page++;
      await sleep(500);

      // Safety: max 10 pages
      if (page > 10) break;
    } catch (e) {
      console.error(`\n[PTCAS] Error on page ${page}: ${e.message}`);
      break;
    }
  }

  console.log(`\n[PTCAS] Done: ${leads.length} PT programs scraped`);
  return leads;
}

// ─── 5. Alternative PTCAS approach: direct list with known pagination ────────

async function scrapePTCASv2() {
  const leads = [];
  console.log('\n[PTCAS-v2] Trying structured page scrape with 100 per page...');

  for (let page = 1; page <= 4; page++) {
    const url = `${PTCAS_BASE}?pn=${page}&ps=100`;
    try {
      const html = await fetchHTML(url);

      // The PTCAS pages contain program data with emails and phones
      // Extract all emails and phones and try to pair them with institution names

      const allEmails = [];
      const emailRe = /[\w.+-]+@[\w.-]+\.(?:edu|com|org|net)/gi;
      let m;
      while ((m = emailRe.exec(html)) !== null) {
        const em = m[0].toLowerCase();
        if (!em.includes('google') && !em.includes('sitecore') && !em.includes('tealium') &&
            !em.includes('noreply') && !em.includes('webmaster') && !em.includes('analytics') &&
            !em.includes('cookie') && !em.includes('jquery') && !em.includes('bootstrap')) {
          allEmails.push({ email: em, pos: m.index });
        }
      }

      const allPhones = [];
      const phoneRe = /(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}|\d{3}-\d{3}-\d{4})/g;
      while ((m = phoneRe.exec(html)) !== null) {
        allPhones.push({ phone: m[0], pos: m.index });
      }

      // Extract institution names - look for .edu links or known university patterns
      const instRe = /(?:University|College|Institute|School|Medical Center|A&amp;M|SUNY)[^<,\n]{0,80}/gi;
      const instMatches = [];
      while ((m = instRe.exec(html)) !== null) {
        const name = m[0].replace(/&amp;/g, '&').replace(/<[^>]+>/g, '').trim();
        if (name.length > 8 && name.length < 120) {
          instMatches.push({ name, pos: m.index });
        }
      }

      // Now for each email, find the nearest institution name that precedes it
      for (const emailObj of allEmails) {
        // Find nearest institution before this email
        let nearestInst = null;
        for (const inst of instMatches) {
          if (inst.pos < emailObj.pos) {
            if (!nearestInst || inst.pos > nearestInst.pos) {
              nearestInst = inst;
            }
          }
        }

        // Find nearest phone near this email (within 500 chars)
        let nearestPhone = null;
        for (const ph of allPhones) {
          const dist = Math.abs(ph.pos - emailObj.pos);
          if (dist < 500) {
            if (!nearestPhone || Math.abs(ph.pos - emailObj.pos) < Math.abs(nearestPhone.pos - emailObj.pos)) {
              nearestPhone = ph;
            }
          }
        }

        if (nearestInst) {
          // Try to extract city/state near the institution
          const surroundingText = html.slice(Math.max(0, nearestInst.pos - 50), emailObj.pos + 100);
          const locMatch = surroundingText.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+([A-Z]{2})\b/);

          leads.push({
            email: emailObj.email,
            first_name: '',
            last_name: '',
            company: nearestInst.name,
            phone: nearestPhone ? formatPhone(nearestPhone.phone) : '',
            city: locMatch ? titleCase(locMatch[1]) : '',
            state: locMatch ? locMatch[2].toUpperCase() : '',
            country: 'US',
            source: 'PTCAS-PT-Program',
          });
        }
      }

      process.stdout.write(`\r[PTCAS-v2] Page ${page}: ${leads.length} programs so far...`);

      // Check if we've reached the end
      const pagMatch = html.match(/Displaying\s+\d+\s*-\s*(\d+)\s+of\s+(\d+)/i);
      if (pagMatch) {
        const shown = parseInt(pagMatch[1]);
        const totalPrograms = parseInt(pagMatch[2]);
        if (shown >= totalPrograms) break;
      }

      await sleep(500);
    } catch (e) {
      console.error(`\n[PTCAS-v2] Error on page ${page}: ${e.message}`);
      break;
    }
  }

  console.log(`\n[PTCAS-v2] Done: ${leads.length} PT programs`);
  return leads;
}

// ─── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== Healthcare Training Program Scraper ===');
  console.log(`Output: ${OUTPUT_FILE}\n`);

  const allLeads = [];

  // Run all scrapers
  try {
    // 1. CAAHEP
    const caahepLeads = await scrapeCAAHEP();
    allLeads.push(...caahepLeads);

    // 2. CMS Nursing Homes
    const nursingLeads = await scrapeCMSNursingHomes();
    allLeads.push(...nursingLeads);

    // 3. CMS Home Health
    const homeHealthLeads = await scrapeCMSHomeHealth();
    allLeads.push(...homeHealthLeads);

    // 4. PTCAS PT Programs (try v2 first as it's more structured)
    const ptcasLeads = await scrapePTCASv2();
    if (ptcasLeads.length < 50) {
      // Fallback to v1 if v2 didn't get enough
      console.log('[PTCAS] v2 got few results, trying v1 approach...');
      const ptcasV1 = await scrapePTCAS();
      allLeads.push(...(ptcasV1.length > ptcasLeads.length ? ptcasV1 : ptcasLeads));
    } else {
      allLeads.push(...ptcasLeads);
    }
  } catch (e) {
    console.error(`\nFatal error: ${e.message}`);
  }

  // Dedup by company name (case-insensitive)
  const seen = new Set();
  const deduped = [];
  for (const lead of allLeads) {
    const key = (lead.company || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    if (key && !seen.has(key)) {
      seen.add(key);
      deduped.push(lead);
    }
  }

  // Stats
  const withEmail = deduped.filter(l => l.email).length;
  const withPhone = deduped.filter(l => l.phone).length;
  const sources = {};
  for (const l of deduped) {
    const src = l.source.split('-').slice(0, 2).join('-');
    sources[src] = (sources[src] || 0) + 1;
  }

  console.log('\n=== Summary ===');
  console.log(`Total leads (before dedup): ${allLeads.length}`);
  console.log(`Total leads (after dedup):  ${deduped.length}`);
  console.log(`With email: ${withEmail}`);
  console.log(`With phone: ${withPhone}`);
  console.log('\nBy source:');
  for (const [src, count] of Object.entries(sources).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${src}: ${count}`);
  }

  // Write CSV
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const rows = [CSV_HEADER, ...deduped.map(csvRow)];
  fs.writeFileSync(OUTPUT_FILE, rows.join('\n'), 'utf-8');
  console.log(`\nCSV written: ${OUTPUT_FILE}`);
  console.log(`Rows: ${deduped.length}`);
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
