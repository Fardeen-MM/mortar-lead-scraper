#!/usr/bin/env node
/**
 * Scrape the Portugal Bar Association (Ordem dos Advogados / OA) directory.
 *
 * Two-phase approach:
 *   Phase 1 — Law firms (Sociedades de Advogados)
 *     URL: portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/
 *     ~1,400 firms across 7 regional councils, emails directly in HTML.
 *     10 results per page, full pagination.
 *
 *   Phase 2 — Individual lawyers (Pesquisa de Advogados)
 *     URL: portal.oa.pt/advogados/pesquisa-de-advogados/
 *     ~33,000+ lawyers across 7 councils. The directory caps results at 56
 *     per query, so we iterate through 2-letter name prefixes (aa..zz) per
 *     council. Emails are behind CAPTCHA — we extract name, phone, city,
 *     address, postal code, license number (cedula), status.
 *
 * Output: output/portugal-bar-lawyers.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Usage:
 *   node scripts/scrape-portugal-bar.js                  # both phases
 *   node scripts/scrape-portugal-bar.js --firms-only     # law firms only
 *   node scripts/scrape-portugal-bar.js --lawyers-only   # individual lawyers only
 *   node scripts/scrape-portugal-bar.js --resume         # resume from progress
 *   node scripts/scrape-portugal-bar.js --delay=2000     # custom delay (ms)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// ─── CLI args ────────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const DELAY_MS = parseInt(args.delay) || 1500;
const FIRMS_ONLY = !!args['firms-only'];
const LAWYERS_ONLY = !!args['lawyers-only'];
const RESUME = !!args.resume;

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'portugal-bar-lawyers.csv');
const PROGRESS_FILE = path.join(OUTPUT_DIR, 'portugal-bar-progress.json');

// ─── Constants ───────────────────────────────────────────────────────────────
const COUNCILS = [
  { code: 'L', name: 'Lisboa' },
  { code: 'P', name: 'Porto' },
  { code: 'C', name: 'Coimbra' },
  { code: 'E', name: 'Evora' },
  { code: 'F', name: 'Faro' },
  { code: 'M', name: 'Madeira' },
  { code: 'A', name: 'Acores' },
];

const FIRM_BASE = 'https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/';
const LAWYER_BASE = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/';

// 2-letter name prefixes for exhaustive iteration (bypasses 56-result cap)
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz';
const NAME_PREFIXES = [];
for (const a of ALPHABET) {
  for (const b of ALPHABET) {
    NAME_PREFIXES.push(a + b);
  }
}

// ─── HTTP helper ─────────────────────────────────────────────────────────────
function fetchPage(url) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-PT,pt;q=0.9,en;q=0.8',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
    };

    const req = https.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `https://${parsedUrl.hostname}${redirectUrl}`;
        }
        fetchPage(redirectUrl).then(resolve).catch(reject);
        res.resume();
        return;
      }

      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }

      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        // Decode as UTF-8 (handles Portuguese characters: ã, õ, ç, é, etc.)
        resolve(buf.toString('utf8'));
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ─── HTML parsing (no Cheerio — pure regex/string parsing) ───────────────────

/**
 * Extract the total result count from the page.
 * Pattern: "Foram encontrados X resultados" or "X de Y resultados"
 */
function extractTotalResults(html) {
  // Check "X de Y resultados" pattern FIRST (lawyer search: "56 de 31077 resultados")
  const m2 = html.match(/Foram encontrados\s+(\d+)\s+de\s+(\d+)\s+resultados/i);
  if (m2) return parseInt(m2[1]); // return visible count, not DB total

  // "Foram encontrados 784 resultados" (firm search: simple count)
  const m1 = html.match(/Foram encontrados\s+(\d+)\s+resultados/i);
  if (m1) return parseInt(m1[1]);

  return 0;
}

/**
 * Parse law firm entries from HTML.
 * Structure:
 *   <h4>FIRM NAME</h4>
 *   <ul>
 *     <li>Conselho Regional XXXX</li>
 *     <li>Registo XX/XX</li>
 *     <li>Localidade XXXX</li>
 *     <li>Email xxxx@xxx.xx</li>
 *     <li>Morada XXXX</li>
 *     <li>Codigo Postal XXXX</li>
 *     <li>Telefone +351XXXX</li>
 *     <li>Fax +351XXXX</li>
 *   </ul>
 */
function parseFirmEntries(html) {
  const entries = [];

  // Split by <h4> tags to get each entry block
  // Each firm entry starts with an <h4> containing the firm name
  const blocks = html.split(/<h4[^>]*>/i);

  for (let i = 1; i < blocks.length; i++) {
    const block = blocks[i];

    // Extract firm name (text before </h4>)
    const nameMatch = block.match(/^([^<]+)<\/h4>/i);
    if (!nameMatch) continue;

    const firmName = decodeEntities(nameMatch[1].trim());

    // Skip if this is clearly not a firm entry (navigation headers, etc.)
    if (firmName.length < 3) continue;
    if (/pesquisa|resultados|ordem|advogados.*portal/i.test(firmName) && firmName.length < 30) continue;

    // Extract all <li> items from this block
    const liItems = [];
    const liRegex = /<li[^>]*>([\s\S]*?)<\/li>/gi;
    let liMatch;
    while ((liMatch = liRegex.exec(block)) !== null) {
      liItems.push(stripTags(liMatch[1]).trim());
    }

    const entry = { company: firmName };

    for (const li of liItems) {
      if (/^Conselho Regional\s+/i.test(li)) {
        entry.council = li.replace(/^Conselho Regional\s+/i, '').trim();
      } else if (/^Registo\s+/i.test(li)) {
        entry.registration = li.replace(/^Registo\s+/i, '').trim();
      } else if (/^Localidade\s+/i.test(li)) {
        entry.city = li.replace(/^Localidade\s+/i, '').trim();
      } else if (/^Email\s+/i.test(li)) {
        entry.email = li.replace(/^Email\s+/i, '').trim().toLowerCase();
      } else if (/^Morada\s+/i.test(li)) {
        entry.address = li.replace(/^Morada\s+/i, '').trim();
      } else if (/^C[oó]digo Postal\s+/i.test(li)) {
        entry.postalCode = li.replace(/^C[oó]digo Postal\s+/i, '').trim();
      } else if (/^Telefone\s+/i.test(li)) {
        entry.phone = normalizePhone(li.replace(/^Telefone\s+/i, '').trim());
      } else if (/^Fax\s+/i.test(li)) {
        entry.fax = li.replace(/^Fax\s+/i, '').trim();
      } else if (/^Data de Constitui/i.test(li)) {
        entry.established = li.replace(/^Data de Constitui[çc][ãa]o\s+/i, '').trim();
      }
    }

    // Basic email validation
    if (entry.email && !entry.email.includes('@')) {
      entry.email = '';
    }

    entries.push(entry);
  }

  return entries;
}

/**
 * Parse individual lawyer entries from HTML.
 * Structure:
 *   <h4>LAWYER NAME</h4>
 *   <p>Activo</p> (or Inativo)
 *   <ul>
 *     <li>Conselho Regional XXX</li>
 *     <li>Cedula XXXXL</li>
 *     <li>Localidade XXX</li>
 *     <li>Morada XXX</li>
 *     <li>Codigo Postal XXX</li>
 *     <li>Telefone XXX</li>
 *     <li>Telemovel XXX</li>
 *     <li>Fax XXX</li>
 *   </ul>
 */
function parseLawyerEntries(html) {
  const entries = [];

  const blocks = html.split(/<h4[^>]*>/i);

  for (let i = 1; i < blocks.length; i++) {
    const block = blocks[i];

    const nameMatch = block.match(/^([^<]+)<\/h4>/i);
    if (!nameMatch) continue;

    const fullName = decodeEntities(nameMatch[1].trim());
    if (fullName.length < 3) continue;
    if (/pesquisa|resultados|ordem|advogados.*portal/i.test(fullName) && fullName.length < 30) continue;

    // Detect status (Activo / Inativo)
    const statusMatch = block.match(/>(Activo|Inativo)</i);
    const status = statusMatch ? statusMatch[1] : '';

    // Skip inactive lawyers
    if (/inativo/i.test(status)) continue;

    // Extract <li> items
    const liItems = [];
    const liRegex = /<li[^>]*>([\s\S]*?)<\/li>/gi;
    let liMatch;
    while ((liMatch = liRegex.exec(block)) !== null) {
      liItems.push(stripTags(liMatch[1]).trim());
    }

    const entry = { fullName, status };

    for (const li of liItems) {
      if (/^Conselho Regional\s+/i.test(li)) {
        entry.council = li.replace(/^Conselho Regional\s+/i, '').trim();
      } else if (/^C[eé]dula\s+/i.test(li)) {
        entry.cedula = li.replace(/^C[eé]dula\s+/i, '').trim();
      } else if (/^Localidade\s+/i.test(li)) {
        entry.city = li.replace(/^Localidade\s+/i, '').trim();
      } else if (/^Morada\s+/i.test(li)) {
        entry.address = li.replace(/^Morada\s+/i, '').trim();
      } else if (/^C[oó]digo Postal\s+/i.test(li)) {
        entry.postalCode = li.replace(/^C[oó]digo Postal\s+/i, '').trim();
      } else if (/^Telefone\s+/i.test(li)) {
        entry.phone = normalizePhone(li.replace(/^Telefone\s+/i, '').trim());
      } else if (/^Telem[oó]vel\s+/i.test(li)) {
        const mobile = normalizePhone(li.replace(/^Telem[oó]vel\s+/i, '').trim());
        // Prefer mobile over landline
        if (mobile) entry.phone = mobile;
      } else if (/^Fax\s+/i.test(li)) {
        entry.fax = li.replace(/^Fax\s+/i, '').trim();
      } else if (/^Data de Inscri/i.test(li)) {
        entry.registrationDate = li.replace(/^Data de Inscri[çc][ãa]o\s+/i, '').trim();
      }
    }

    entries.push(entry);
  }

  return entries;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function stripTags(html) {
  return html.replace(/<[^>]+>/g, '');
}

function decodeEntities(str) {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
    .replace(/&nbsp;/g, ' ');
}

function normalizePhone(phone) {
  if (!phone) return '';
  // Remove duplicate country codes like "+351(+351)"
  phone = phone.replace(/\+351\s*\(\+351\)/g, '+351');
  // Remove spaces, dashes, parens
  phone = phone.replace(/[\s\-\(\)\.]/g, '');
  // Ensure +351 prefix
  if (phone && !phone.startsWith('+') && phone.length >= 9) {
    phone = '+351' + phone;
  }
  return phone;
}

/**
 * Split a full Portuguese name into first_name and last_name.
 * Portuguese names often have multiple given names and family names.
 * Strategy: first word = first_name, rest = last_name.
 */
function splitName(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

/**
 * Determine how many pages exist for a result set.
 * The firm search shows all results (no cap), so total / 10.
 * The lawyer search caps at 56 visible results = 6 pages max.
 */
function computeFirmPages(totalResults) {
  return Math.ceil(totalResults / 10);
}

// ─── CSV output ──────────────────────────────────────────────────────────────

const CSV_COLUMNS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
];

function escapeCSV(val) {
  const s = (val || '').toString();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function writeCSV(filePath, leads) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col])).join(',')
  );

  fs.writeFileSync(filePath, '\ufeff' + [header, ...rows].join('\n'), 'utf8');
}

// ─── Progress tracking ───────────────────────────────────────────────────────

function saveProgress(state) {
  const dir = path.dirname(PROGRESS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(state, null, 2));
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return null;
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
  } catch { return null; }
}

// ─── Deduplication ───────────────────────────────────────────────────────────

class DeduplicatorSet {
  constructor() {
    this.seen = new Set();
  }

  /**
   * Returns true if this lead is a duplicate.
   */
  isDuplicate(lead) {
    // Dedup by email if present
    if (lead.email) {
      const key = `email:${lead.email.toLowerCase()}`;
      if (this.seen.has(key)) return true;
      this.seen.add(key);
      return false;
    }

    // Dedup by name + city
    if (lead.first_name && lead.last_name && lead.city) {
      const key = `name:${lead.first_name.toLowerCase()}|${lead.last_name.toLowerCase()}|${lead.city.toLowerCase()}`;
      if (this.seen.has(key)) return true;
      this.seen.add(key);
      return false;
    }

    // Dedup by company + city (for firms)
    if (lead.company && lead.city) {
      const key = `firm:${lead.company.toLowerCase()}|${lead.city.toLowerCase()}`;
      if (this.seen.has(key)) return true;
      this.seen.add(key);
      return false;
    }

    return false; // Can't dedup — keep it
  }
}

// ─── Phase 1: Law Firms ─────────────────────────────────────────────────────

async function scrapeFirms(allLeads, dedup, progress) {
  console.log('\n  ┌─────────────────────────────────────────────────────┐');
  console.log('  │  PHASE 1: Law Firms (Sociedades de Advogados)      │');
  console.log('  └─────────────────────────────────────────────────────┘\n');

  let totalFirms = 0;
  let totalEmails = 0;
  let totalPhones = 0;
  let errors = 0;

  // Determine resume point
  const startCouncilIdx = progress.firmCouncilIdx || 0;
  const startPage = progress.firmPage || 1;

  for (let ci = startCouncilIdx; ci < COUNCILS.length; ci++) {
    const council = COUNCILS[ci];
    const firstPageUrl = `${FIRM_BASE}?cg=${council.code}&r=&n=&lo=&m=&cp=&op=&o=0&page=1`;

    let totalResults = 0;
    let totalPages = 1;

    // Fetch first page to get total count
    try {
      const html = await fetchPage(firstPageUrl);
      totalResults = extractTotalResults(html);
      totalPages = computeFirmPages(totalResults);

      console.log(`  [${council.name}] ${totalResults} firms, ${totalPages} pages`);

      // Parse first page (unless resuming past it)
      if (ci === startCouncilIdx && startPage > 1) {
        // Skip first page if resuming
      } else {
        const entries = parseFirmEntries(html);
        for (const entry of entries) {
          const lead = {
            email: entry.email || '',
            first_name: '',
            last_name: '',
            company: entry.company || '',
            phone: entry.phone || '',
            city: entry.city || '',
            state: entry.council || council.name,
            country: 'Portugal',
            source: 'portugal_bar',
          };
          if (!dedup.isDuplicate(lead)) {
            allLeads.push(lead);
            totalFirms++;
            if (lead.email) totalEmails++;
            if (lead.phone) totalPhones++;
          }
        }
      }

      await sleep(DELAY_MS);
    } catch (err) {
      console.log(`  [${council.name}] ERROR on page 1: ${err.message}`);
      errors++;
      await sleep(DELAY_MS * 2);
      continue;
    }

    // Fetch remaining pages
    const pageStart = (ci === startCouncilIdx && startPage > 1) ? startPage : 2;
    for (let page = pageStart; page <= totalPages; page++) {
      try {
        const url = `${FIRM_BASE}?cg=${council.code}&r=&n=&lo=&m=&cp=&op=&o=0&page=${page}`;
        const html = await fetchPage(url);
        const entries = parseFirmEntries(html);

        for (const entry of entries) {
          const lead = {
            email: entry.email || '',
            first_name: '',
            last_name: '',
            company: entry.company || '',
            phone: entry.phone || '',
            city: entry.city || '',
            state: entry.council || council.name,
            country: 'Portugal',
            source: 'portugal_bar',
          };
          if (!dedup.isDuplicate(lead)) {
            allLeads.push(lead);
            totalFirms++;
            if (lead.email) totalEmails++;
            if (lead.phone) totalPhones++;
          }
        }

        if (page % 10 === 0) {
          console.log(`    [${council.name}] page ${page}/${totalPages} — ${totalFirms} firms (${totalEmails} email)`);
        }

        // Save progress periodically
        if (totalFirms % 100 === 0 && totalFirms > 0) {
          progress.firmCouncilIdx = ci;
          progress.firmPage = page + 1;
          progress.firmsDone = false;
          saveProgress(progress);
          writeCSV(OUTPUT_FILE, allLeads);
        }
      } catch (err) {
        console.log(`    [${council.name}] ERROR page ${page}: ${err.message}`);
        errors++;
        await sleep(DELAY_MS * 2);
      }

      await sleep(DELAY_MS);
    }

    console.log(`  [${council.name}] done — ${totalFirms} cumulative firms`);
  }

  progress.firmsDone = true;
  progress.firmCouncilIdx = 0;
  progress.firmPage = 1;
  saveProgress(progress);

  console.log(`\n  Phase 1 complete: ${totalFirms} firms (${totalEmails} email, ${totalPhones} phone, ${errors} errors)`);
  return { totalFirms, totalEmails, totalPhones, errors };
}

// ─── Phase 2: Individual Lawyers ─────────────────────────────────────────────

async function scrapeLawyers(allLeads, dedup, progress) {
  console.log('\n  ┌─────────────────────────────────────────────────────┐');
  console.log('  │  PHASE 2: Individual Lawyers (Pesquisa de Advogados)│');
  console.log('  └─────────────────────────────────────────────────────┘\n');
  console.log('  Strategy: iterate 2-letter name prefixes x 7 councils');
  console.log(`  Prefixes: ${NAME_PREFIXES.length} (aa..zz) x ${COUNCILS.length} councils`);
  console.log(`  Max results per query: 56 (6 pages x ~10)\n`);

  let totalLawyers = 0;
  let totalPhones = 0;
  let errors = 0;
  let queriesRun = 0;
  let emptyQueries = 0;

  const startCouncilIdx = progress.lawyerCouncilIdx || 0;
  const startPrefixIdx = progress.lawyerPrefixIdx || 0;

  const totalCombinations = COUNCILS.length * NAME_PREFIXES.length;
  let combinationsDone = startCouncilIdx * NAME_PREFIXES.length + startPrefixIdx;

  for (let ci = startCouncilIdx; ci < COUNCILS.length; ci++) {
    const council = COUNCILS[ci];
    let councilLawyers = 0;

    const prefixStart = (ci === startCouncilIdx) ? startPrefixIdx : 0;

    for (let pi = prefixStart; pi < NAME_PREFIXES.length; pi++) {
      const prefix = NAME_PREFIXES[pi];
      queriesRun++;
      combinationsDone++;

      // Fetch first page to see if there are results
      try {
        const url = `${LAWYER_BASE}?cg=${council.code}&r=&n=${encodeURIComponent(prefix)}&lo=&m=&cp=&op=&o=0&page=1`;
        const html = await fetchPage(url);
        const visibleCount = extractTotalResults(html);

        if (visibleCount === 0) {
          emptyQueries++;
          await sleep(DELAY_MS);
          continue;
        }

        // Parse first page
        const firstPageEntries = parseLawyerEntries(html);
        for (const entry of firstPageEntries) {
          const { first_name, last_name } = splitName(entry.fullName);
          const lead = {
            email: '',
            first_name,
            last_name,
            company: '',
            phone: entry.phone || '',
            city: entry.city || '',
            state: entry.council || council.name,
            country: 'Portugal',
            source: 'portugal_bar',
          };
          if (!dedup.isDuplicate(lead)) {
            allLeads.push(lead);
            totalLawyers++;
            councilLawyers++;
            if (lead.phone) totalPhones++;
          }
        }

        // Calculate how many pages (max 6 pages of ~10 = 56 results cap)
        const pages = Math.min(6, Math.ceil(visibleCount / 10));

        // Fetch remaining pages
        for (let page = 2; page <= pages; page++) {
          try {
            const pageUrl = `${LAWYER_BASE}?cg=${council.code}&r=&n=${encodeURIComponent(prefix)}&lo=&m=&cp=&op=&o=0&page=${page}`;
            const pageHtml = await fetchPage(pageUrl);
            const entries = parseLawyerEntries(pageHtml);

            for (const entry of entries) {
              const { first_name, last_name } = splitName(entry.fullName);
              const lead = {
                email: '',
                first_name,
                last_name,
                company: '',
                phone: entry.phone || '',
                city: entry.city || '',
                state: entry.council || council.name,
                country: 'Portugal',
                source: 'portugal_bar',
              };
              if (!dedup.isDuplicate(lead)) {
                allLeads.push(lead);
                totalLawyers++;
                councilLawyers++;
                if (lead.phone) totalPhones++;
              }
            }

            await sleep(DELAY_MS);
          } catch (err) {
            errors++;
            await sleep(DELAY_MS);
          }
        }

        // Log progress periodically
        if (queriesRun % 50 === 0) {
          const pct = ((combinationsDone / totalCombinations) * 100).toFixed(1);
          console.log(`  [${pct}%] ${council.name}/${prefix} — ${totalLawyers} lawyers (${totalPhones} phone) | ${queriesRun} queries | ${emptyQueries} empty`);
        }

        // Save progress periodically
        if (queriesRun % 100 === 0) {
          progress.lawyerCouncilIdx = ci;
          progress.lawyerPrefixIdx = pi + 1;
          progress.lawyersDone = false;
          saveProgress(progress);
          writeCSV(OUTPUT_FILE, allLeads);
          console.log(`    [SAVE] ${allLeads.length} total leads saved`);
        }
      } catch (err) {
        errors++;
        if (errors % 20 === 0) {
          console.log(`  [WARN] ${errors} errors so far (latest: ${err.message})`);
          await sleep(DELAY_MS * 3);
        }
        await sleep(DELAY_MS);
      }

      await sleep(DELAY_MS);
    }

    console.log(`  [${council.name}] done — ${councilLawyers} lawyers from this council`);
  }

  progress.lawyersDone = true;
  progress.lawyerCouncilIdx = 0;
  progress.lawyerPrefixIdx = 0;
  saveProgress(progress);

  console.log(`\n  Phase 2 complete: ${totalLawyers} lawyers (${totalPhones} phone, ${errors} errors)`);
  console.log(`  Queries: ${queriesRun} total, ${emptyQueries} empty`);
  return { totalLawyers, totalPhones, errors };
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('\n  ╔═══════════════════════════════════════════════════════════╗');
  console.log('  ║  MORTAR — Portugal Bar Association (Ordem dos Advogados) ║');
  console.log('  ╚═══════════════════════════════════════════════════════════╝\n');
  console.log(`  Delay:       ${DELAY_MS}ms between requests`);
  console.log(`  Output:      ${OUTPUT_FILE}`);
  console.log(`  Firms only:  ${FIRMS_ONLY}`);
  console.log(`  Lawyers only:${LAWYERS_ONLY}`);
  console.log(`  Resume:      ${RESUME}`);

  // Load progress or start fresh
  let progress = {};
  if (RESUME) {
    const saved = loadProgress();
    if (saved) {
      progress = saved;
      console.log(`\n  Resuming from saved progress:`);
      console.log(`    Firms done:    ${progress.firmsDone || false}`);
      console.log(`    Lawyers done:  ${progress.lawyersDone || false}`);
    } else {
      console.log('\n  No progress file found, starting fresh');
    }
  }

  // Load existing leads from CSV if resuming
  const allLeads = [];
  const dedup = new DeduplicatorSet();

  if (RESUME && fs.existsSync(OUTPUT_FILE)) {
    try {
      const content = fs.readFileSync(OUTPUT_FILE, 'utf8');
      // Strip BOM if present
      const clean = content.replace(/^\ufeff/, '');
      const lines = clean.trim().split('\n');
      if (lines.length > 1) {
        const headers = lines[0].split(',');
        for (let i = 1; i < lines.length; i++) {
          // Simple CSV parse (handles quoted fields)
          const vals = parseCSVLine(lines[i]);
          const lead = {};
          headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
          allLeads.push(lead);
          dedup.isDuplicate(lead); // Register in dedup set
        }
        console.log(`  Loaded ${allLeads.length} existing leads from CSV`);
      }
    } catch (err) {
      console.log(`  Could not load existing CSV: ${err.message}`);
    }
  }

  // Phase 1: Law firms
  let firmStats = { totalFirms: 0, totalEmails: 0, totalPhones: 0, errors: 0 };
  if (!LAWYERS_ONLY && !progress.firmsDone) {
    firmStats = await scrapeFirms(allLeads, dedup, progress);
    // Save after phase 1
    writeCSV(OUTPUT_FILE, allLeads);
    console.log(`  Saved ${allLeads.length} leads to ${OUTPUT_FILE}`);
  } else if (progress.firmsDone) {
    console.log('\n  Phase 1 (firms) already complete, skipping');
  }

  // Phase 2: Individual lawyers
  let lawyerStats = { totalLawyers: 0, totalPhones: 0, errors: 0 };
  if (!FIRMS_ONLY && !progress.lawyersDone) {
    lawyerStats = await scrapeLawyers(allLeads, dedup, progress);
  } else if (progress.lawyersDone) {
    console.log('\n  Phase 2 (lawyers) already complete, skipping');
  }

  // Final save
  if (allLeads.length > 0) {
    writeCSV(OUTPUT_FILE, allLeads);

    // Also save timestamped final file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const finalPath = path.join(OUTPUT_DIR, `portugal-bar-lawyers_${timestamp}.csv`);
    writeCSV(finalPath, allLeads);
  }

  // Clean up progress file on completion
  if (progress.firmsDone !== false && progress.lawyersDone !== false) {
    try { fs.unlinkSync(PROGRESS_FILE); } catch {}
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalEmails = allLeads.filter(l => l.email).length;
  const totalPhones = allLeads.filter(l => l.phone).length;
  const totalFirms = allLeads.filter(l => l.company).length;

  console.log('\n  ╔═══════════════════════════════════════════════════════════╗');
  console.log('  ║       PORTUGAL BAR SCRAPE COMPLETE                       ║');
  console.log('  ╠═══════════════════════════════════════════════════════════╣');
  console.log(`  ║  Total leads:   ${String(allLeads.length).padEnd(42)}║`);
  console.log(`  ║  With email:    ${String(totalEmails + ' (' + (allLeads.length ? Math.round(totalEmails / allLeads.length * 100) : 0) + '%)').padEnd(42)}║`);
  console.log(`  ║  With phone:    ${String(totalPhones + ' (' + (allLeads.length ? Math.round(totalPhones / allLeads.length * 100) : 0) + '%)').padEnd(42)}║`);
  console.log(`  ║  Law firms:     ${String(totalFirms).padEnd(42)}║`);
  console.log(`  ║  Time:          ${String(elapsed + 's (' + Math.round(elapsed / 60) + 'min)').padEnd(42)}║`);
  console.log('  ╠═══════════════════════════════════════════════════════════╣');
  console.log(`  ║  Output: ${OUTPUT_FILE}`);
  console.log('  ╚═══════════════════════════════════════════════════════════╝\n');
}

// ─── Simple CSV line parser (handles quoted fields) ──────────────────────────

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current);
  return result;
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
