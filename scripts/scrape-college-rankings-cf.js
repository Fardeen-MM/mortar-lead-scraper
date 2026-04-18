#!/usr/bin/env node
/**
 * College Admissions Rankings Scraper (Cloudflare Browser Rendering)
 *
 * Scrapes multiple college admissions consultant ranking/review sites using the
 * Cloudflare Browser Rendering API (markdown endpoint) and extracts firm data.
 *
 * Sources:
 *   1. topcollegeadmissionsconsultants.com — Top 20 ranked firms
 *   2. bestcollegeadmissionconsultants.com — 7+ reviewed firms
 *   3. research.com — Best college admissions consultants ranking
 *   4. examstrategist.com — Best college admissions consultants
 *   5. collegeconsensus.com — Best college admissions consultants
 *
 * After extracting firms from rankings, optionally crawls each firm's website
 * for phone, email, and address data.
 *
 * Usage:
 *   node scripts/scrape-college-rankings-cf.js            # full scrape + enrichment
 *   node scripts/scrape-college-rankings-cf.js --test      # skip firm website enrichment
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'college-consultants-rankings.csv');
const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche', 'source',
  'profile_url'
];

const TEST_MODE = process.argv.includes('--test');
const CF_DELAY_MS = 5000; // 5s between Cloudflare API calls
const CF_ENRICH_DELAY_MS = 3000; // 3s between firm enrichment calls
const CF_RETRY_DELAY_MS = 12000; // 12s before retry on rate limit
const CF_MAX_RETRIES = 2;

const SOURCES = [
  {
    name: 'topcollegeadmissionsconsultants',
    url: 'https://www.topcollegeadmissionsconsultants.com',
    label: 'TopCollegeAdmissionsConsultants.com'
  },
  {
    name: 'bestcollegeadmissionconsultants',
    url: 'https://bestcollegeadmissionconsultants.com/',
    label: 'BestCollegeAdmissionConsultants.com'
  },
  {
    name: 'research-com',
    url: 'https://research.com/universities-colleges/best-college-admissions-consultants-ranking',
    label: 'Research.com'
  },
  {
    name: 'examstrategist',
    url: 'https://examstrategist.com/best-college-admissions-consultants/',
    label: 'ExamStrategist.com'
  },
  {
    name: 'collegeconsensus',
    url: 'https://www.collegeconsensus.com/rankings/best-college-admissions-consultants/',
    label: 'CollegeConsensus.com'
  }
];

// ---------------------------------------------------------------------------
// Load .env
// ---------------------------------------------------------------------------

function loadEnv() {
  const envPath = path.join(__dirname, '..', '.env');
  if (!fs.existsSync(envPath)) {
    console.error('[ERROR] .env file not found at', envPath);
    process.exit(1);
  }
  const lines = fs.readFileSync(envPath, 'utf8').split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) {
      process.env[key] = val;
    }
  }
}

loadEnv();

const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;

if (!CLOUDFLARE_ACCOUNT_ID || !CLOUDFLARE_API_TOKEN) {
  console.error('[ERROR] Missing CLOUDFLARE_ACCOUNT_ID or CLOUDFLARE_API_TOKEN in .env');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url;
    if (!u.startsWith('http')) u = 'https://' + u;
    const parsed = new URL(u);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function normalizePhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) {
    const d = digits.slice(1);
    return `(${d.slice(0, 3)}) ${d.slice(3, 6)}-${d.slice(6)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return raw.trim();
}

function escapeCSV(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

// ---------------------------------------------------------------------------
// Cloudflare Browser Rendering API
// ---------------------------------------------------------------------------

function cfRenderMarkdown(url) {
  return new Promise((resolve, reject) => {
    const endpoint = `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/browser-rendering/markdown`;
    const body = JSON.stringify({
      url,
      gotoOptions: { waitUntil: 'networkidle0', timeout: 30000 }
    });

    const parsed = new URL(endpoint);
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${CLOUDFLARE_API_TOKEN}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };

    log(`Fetching markdown via CF Browser Rendering: ${url}`);

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8');
        try {
          const json = JSON.parse(raw);
          if (!json.success) {
            reject(new Error(`CF API error: ${JSON.stringify(json.errors || json)}`));
            return;
          }
          log(`Received ${json.result.length.toLocaleString()} chars of markdown`);
          resolve(json.result);
        } catch (e) {
          reject(new Error(`Failed to parse CF response: ${e.message}\nRaw (500 chars): ${raw.slice(0, 500)}`));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error('CF API request timed out after 60s'));
    });
    req.write(body);
    req.end();
  });
}

/**
 * Wrapper with retry on rate limit (error code 2001)
 */
async function cfRenderMarkdownWithRetry(url) {
  for (let attempt = 0; attempt <= CF_MAX_RETRIES; attempt++) {
    try {
      return await cfRenderMarkdown(url);
    } catch (err) {
      if (err.message.includes('2001') && attempt < CF_MAX_RETRIES) {
        log(`  Rate limited — waiting ${CF_RETRY_DELAY_MS / 1000}s before retry ${attempt + 1}/${CF_MAX_RETRIES}...`);
        await sleep(CF_RETRY_DELAY_MS);
        continue;
      }
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Generic HTTPS GET (for firm website enrichment)
// ---------------------------------------------------------------------------

function httpsGet(url, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : require('http');
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    };

    const req = mod.request(options, (res) => {
      // Follow redirects (up to 3)
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `${parsed.protocol}//${parsed.hostname}${redirectUrl}`;
        }
        return httpsGet(redirectUrl, timeoutMs).then(resolve).catch(reject);
      }

      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        resolve(Buffer.concat(chunks).toString('utf8'));
      });
    });

    req.on('error', reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      reject(new Error(`Timeout fetching ${url}`));
    });
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Source Parsers
// ---------------------------------------------------------------------------

/**
 * Source 1: topcollegeadmissionsconsultants.com
 * Pattern: `# 01` through `# 20`
 * Structure per block:
 *   # 01
 *   [ ![Alt](image_url) ](firm_website)
 *   City, ST
 *   ### **The #1 ranked...** [**Firm Name**](url)**.**
 *   OR: ### **The #4 ranked...** **Signet Education** **.**
 */
function parseTopCollegeAdmissions(md) {
  const firms = [];

  // Split by rank headers like "# 01", "# 02", etc.
  const rankBlocks = md.split(/(?=^# \d{2}\b)/m);

  for (const block of rankBlocks) {
    // Match rank number (2 digits only, not # followed by arbitrary text)
    const rankMatch = block.match(/^# (\d{2})\s*$/m);
    if (!rankMatch) continue;

    const rank = parseInt(rankMatch[1], 10);
    if (rank < 1 || rank > 25) continue; // sanity check

    // Extract location — line like "City, ST" standing alone
    const locationMatch = block.match(/\n([A-Z][a-zA-Z\s]+,\s*[A-Z]{2})\s*\n/);
    let city = '', state = '';
    if (locationMatch) {
      const parts = locationMatch[1].split(',').map(s => s.trim());
      city = parts[0] || '';
      state = parts[1] || '';
    }

    // Primary: Extract firm name and website from the ### ranked heading
    // Pattern: ### **The #N ranked...** [**Firm Name**](url)**.**
    let firmName = '', website = '';

    const rankedHeadingLink = block.match(/###\s+\*{2}The #\d+ ranked[^[]*\[\*{2}([^\]*]+?)\*{2}\]\((https?:\/\/[^)]+)\)/);
    if (rankedHeadingLink) {
      firmName = rankedHeadingLink[1].replace(/\*+/g, '').replace(/[.*]+$/, '').trim();
      website = rankedHeadingLink[2].trim();
    }

    // Fallback: firm name is bold inline (no link), e.g. **Signet Education**
    if (!firmName) {
      const rankedHeadingBold = block.match(/###\s+\*{2}The #\d+ ranked[^*]*\*{2}\s+\*{2}([^*]+)\*{2}/);
      if (rankedHeadingBold) {
        firmName = rankedHeadingBold[1].replace(/[.*]+$/, '').trim();
      }
    }

    // If still no firm name from ### heading, try the image-wrapped link for the website
    // Pattern: [ ![...](image) ](firm_website)
    if (!website) {
      const imageLink = block.match(/\[\s*!\[.*?\]\(.*?\)\s*\]\((https?:\/\/[^)]+)\)/);
      if (imageLink) {
        website = imageLink[1].trim();
      }
    }

    // If still no firm name, try extracting from the image alt text in the logo link
    if (!firmName) {
      const imgAlt = block.match(/!\[([^\]]+)\]\(https:\/\/images\.squarespace/);
      if (imgAlt) {
        let alt = imgAlt[1]
          .replace(/\s*-\s*top college admissions consultants\s*/gi, '')
          .replace(/logo\.?\s*/gi, '')
          .trim();
        if (alt && alt.length > 1) firmName = alt;
      }
    }

    if (!firmName) continue;

    // Clean trailing period/dot from firm name
    firmName = firmName.replace(/\.\s*$/, '').trim();

    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city,
      state,
      rank,
      profile_url: 'https://www.topcollegeadmissionsconsultants.com'
    });
  }

  return firms;
}

/**
 * Source 2: bestcollegeadmissionconsultants.com
 * Structure:
 *   Table of Contents with headings like ### Admissionado, ### HelloCollege, etc.
 *   Review sections with ### FirmName headings
 *   Visit links: [Visit Admissionado](https://bestcollegeadmissionconsultants.com/visit/admissionado/)
 */
function parseBestCollegeAdmission(md) {
  const firms = [];
  const seen = new Set();

  // Strategy 1: Find ### review section headings for known firm names
  // These are the actual review sections (not TOC entries)
  const reviewHeadings = md.matchAll(/^### ([A-Z][^\n]+?)$/gm);
  for (const m of reviewHeadings) {
    let firmName = m[1].trim();

    // Skip non-firm headings (FAQs, editorial, etc.)
    if (/pros?\s+and\s+cons?|pricing|prices|services|what|how|why|our|tips/i.test(firmName)) continue;
    if (/^(are|can|do|does|is|should|when|how|what|why|related|faq)\b/i.test(firmName)) continue;

    // Clean markdown
    firmName = firmName.replace(/\*+/g, '').trim();
    const key = firmName.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seen.has(key) || key.length < 3) continue;
    seen.add(key);

    // Find a visit link in the block below
    const startIdx = m.index + m[0].length;
    const nextHeadingIdx = md.indexOf('\n##', startIdx + 1);
    const blockEnd = nextHeadingIdx > -1 ? nextHeadingIdx : Math.min(md.length, startIdx + 5000);
    const blockText = md.slice(startIdx, blockEnd);

    let website = '';
    // Primary: visit redirect link
    const visitMatch = blockText.match(/\[Visit\s+[^\]]*\]\((https?:\/\/bestcollegeadmission[^\s)]+\/visit\/[^)]+)\)/i);
    if (visitMatch) {
      website = visitMatch[1];
    }

    // Visit/redirect links are not real firm URLs — just track them as profile_url
    const domain = extractDomain(website);
    const isRedirectLink = domain && domain.includes('bestcollegeadmission');

    firms.push({
      firm_name: firmName,
      website: isRedirectLink ? '' : website,
      domain: isRedirectLink ? '' : domain,
      city: '',
      state: '',
      profile_url: website || 'https://bestcollegeadmissionconsultants.com/'
    });
  }

  // Strategy 2: Also extract from the comparison table row for "Consultant"
  const consultantRow = md.match(/\|\s*\*\*Consultant\*\*\s*\|(.+)\|/);
  if (consultantRow) {
    const cells = consultantRow[1].split('|').map(c => c.trim()).filter(Boolean);
    for (const cell of cells) {
      const name = cell.replace(/\*+/g, '').trim();
      const key = name.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (key.length < 3 || seen.has(key)) continue;
      seen.add(key);
      firms.push({
        firm_name: name,
        website: '',
        domain: '',
        city: '',
        state: '',
        profile_url: 'https://bestcollegeadmissionconsultants.com/'
      });
    }
  }

  return firms;
}

/**
 * Source 3: research.com rankings
 * Extract firm names and websites from ranking entries
 */
function parseResearchCom(md) {
  const firms = [];

  // Check if page is Cloudflare blocked
  if (md.includes('you have been blocked') || md.includes('Sorry, you have been blocked') || md.length < 1000) {
    return firms; // Blocked page, return empty
  }

  // Look for numbered entries or heading-based patterns
  // Typical patterns: "### N. Firm Name" or "**N. Firm Name**" or table rows
  const patterns = [
    /^#{2,4}\s*(?:\d+[\.\)]\s*)?(.+?)$/gm,
    /\*{2}(\d+[\.\)]\s*.+?)\*{2}/g,
    /\|\s*(\d+)\s*\|\s*\[([^\]]+)\]\(([^)]+)\)/g
  ];

  // Try table format first
  const tableMatches = md.matchAll(/\|\s*(\d+)\s*\|\s*\[([^\]]+)\]\(([^)]+)\)/g);
  for (const m of tableMatches) {
    const firmName = m[2].trim();
    const website = m[3].trim();
    if (firmName.length < 2) continue;
    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city: '',
      state: '',
      profile_url: 'https://research.com/universities-colleges/best-college-admissions-consultants-ranking'
    });
  }

  if (firms.length > 0) return firms;

  // Try heading pattern with links
  const headingLinkMatches = md.matchAll(/^#{2,4}\s*(?:\d+[\.\)]\s*)?\[([^\]]+)\]\(([^)]+)\)/gm);
  for (const m of headingLinkMatches) {
    const firmName = m[1].replace(/\*+/g, '').trim();
    const website = m[2].trim();
    if (firmName.length < 2) continue;
    // Skip navigation/internal links
    if (website.includes('research.com') && !website.includes('redirect')) continue;
    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city: '',
      state: '',
      profile_url: 'https://research.com/universities-colleges/best-college-admissions-consultants-ranking'
    });
  }

  if (firms.length > 0) return firms;

  // Broad approach: look for any linked firm names that seem like consultancies
  // Find sections that mention consulting/admissions firms with links
  const allLinks = md.matchAll(/\[([^\]]+)\]\((https?:\/\/(?!research\.com)[^)]+)\)/g);
  const seen = new Set();
  for (const m of allLinks) {
    const text = m[1].replace(/\*+/g, '').trim();
    const url = m[2].trim();
    const domain = extractDomain(url);

    // Filter: must look like a firm (has a domain, text is not too short, not a generic link)
    if (!domain || text.length < 3) continue;
    if (/^(here|click|read more|learn more|visit|link|source|see|view)$/i.test(text)) continue;
    if (domain.includes('facebook.com') || domain.includes('twitter.com') || domain.includes('linkedin.com')) continue;
    if (domain.includes('youtube.com') || domain.includes('instagram.com')) continue;
    if (seen.has(domain)) continue;
    seen.add(domain);

    // Check if text contains admissions/college/consulting keywords nearby
    const idx = md.indexOf(m[0]);
    const context = md.slice(Math.max(0, idx - 300), Math.min(md.length, idx + 300)).toLowerCase();
    if (context.includes('admissions') || context.includes('consulting') || context.includes('college') ||
        context.includes('counseling') || context.includes('prep') || context.includes('rank')) {
      firms.push({
        firm_name: text,
        website: url,
        domain,
        city: '',
        state: '',
        profile_url: 'https://research.com/universities-colleges/best-college-admissions-consultants-ranking'
      });
    }
  }

  return firms;
}

/**
 * Source 4: examstrategist.com
 * Structure:
 *   ## 1\. Admissionado College Admissions Consultants
 *   ## 2\. Admit Advantage College Admissions Consultants
 *   ...up to ## 19\. College Zoom
 *   Also: ## Quad Education (BS/MD Admissions Experts)
 *   Each section has a "Learn More" link with the firm's URL
 */
function parseExamStrategist(md) {
  const firms = [];
  const seen = new Set();

  // Pattern: ## N\. Firm Name ... (with optional "College Admissions Consultants/Consulting" suffix)
  // The backslash-dot is markdown escaped period
  const numberedMatches = md.matchAll(/^## (\d+)\\?\.\s+(.+?)$/gm);
  for (const m of numberedMatches) {
    const rank = parseInt(m[1], 10);
    let rawHeading = m[2].trim();

    // Strip common suffixes like "College Admissions Consultants", "College Admissions Consulting"
    let firmName = rawHeading
      .replace(/\s+College\s+Admissions?\s+(Consultant|Consulting|Counseling)s?\s*$/i, '')
      .replace(/\s+Academic\s+Counseling\s*.*$/i, '')
      .replace(/\s+Admissions\s+Consulting\s*.*$/i, '')
      .replace(/\s+College\s+Admissions?\s*$/i, '')
      .replace(/\*+/g, '')
      .trim();

    // Skip FAQ / methodology / non-firm headings
    if (/^(is|how|what|why|when|our|frequently|last|best|should|does|do|are|who|can)\b/i.test(firmName)) continue;
    if (/^college admissions?\b/i.test(firmName)) continue; // "College Admissions Consultant Quiz" but not "College Zoom"
    if (firmName.length < 2) continue;

    const key = firmName.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seen.has(key)) continue;
    seen.add(key);

    // Find the website link in the block below (typically "Learn More About ..." links)
    const startIdx = m.index + m[0].length;
    const nextH2 = md.indexOf('\n## ', startIdx + 1);
    const blockEnd = nextH2 > -1 ? nextH2 : Math.min(md.length, startIdx + 5000);
    const blockText = md.slice(startIdx, blockEnd);

    let website = '';
    // Affiliate/tracker domains to skip
    const affiliateDomains = ['shrsl.com', 'shareasale.com', 'anrdoezrs.net', 'awin1.com',
      'jdoqocy.com', 'kqzyfj.com', 'tkqlhce.com', 'dpbolvw.net', 'nacacnet.org'];

    // Only use "Learn More About ..." links that point to the actual firm
    // These are the most reliable indicators of the firm's URL
    const learnMoreMatch = blockText.match(/\[\s*Learn More[^\]]*\]\((https?:\/\/(?!examstrategist\.com)[^)]+)\)/i);
    if (learnMoreMatch) {
      const d = extractDomain(learnMoreMatch[1]);
      if (!affiliateDomains.some(ad => d.includes(ad))) {
        website = learnMoreMatch[1];
      }
    }
    // Note: We intentionally skip fallback link extraction because the block text
    // often contains cross-references to OTHER firms (e.g., "check out Admissionado's...")
    // which would assign the wrong website.

    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city: '',
      state: '',
      rank,
      profile_url: 'https://examstrategist.com/best-college-admissions-consultants/'
    });
  }

  // Also look for non-numbered firm headings like "## Quad Education (BS/MD Admissions Experts)"
  const unnumberedFirm = md.matchAll(/^## ([A-Z][^\n]+?)\s*(?:\([^)]*\))?\s*$/gm);
  for (const m of unnumberedFirm) {
    let heading = m[1].trim();
    // Must NOT start with generic/editorial words
    if (/^(is|how|what|why|when|our|frequently|last|best|should|does|do|are|who|can|honorable|methodology|review|comparison|faq|pricing|conclusion|disclaimer|about|contact)\b/i.test(heading)) continue;
    if (/^college admissions?\b/i.test(heading)) continue;
    // Must look like a firm name (no question marks, not too long)
    if (heading.includes('?') || heading.length > 60) continue;

    let firmName = heading
      .replace(/\s*\([^)]*\)\s*$/, '') // strip parenthetical
      .replace(/\s+College\s+Admissions?\s+(Consultant|Consulting|Counseling)s?\s*$/i, '')
      .replace(/\*+/g, '')
      .trim();

    const key = firmName.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (seen.has(key) || key.length < 3) continue;
    seen.add(key);

    const startIdx = m.index + m[0].length;
    const nextH2 = md.indexOf('\n## ', startIdx + 1);
    const blockEnd = nextH2 > -1 ? nextH2 : Math.min(md.length, startIdx + 3000);
    const blockText = md.slice(startIdx, blockEnd);

    let website = '';
    const learnMoreMatch = blockText.match(/\[\s*Learn More[^\]]*\]\((https?:\/\/(?!examstrategist\.com)[^)]+)\)/i);
    if (learnMoreMatch) website = learnMoreMatch[1];

    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city: '',
      state: '',
      profile_url: 'https://examstrategist.com/best-college-admissions-consultants/'
    });
  }

  return firms;
}

/**
 * Source 5: collegeconsensus.com
 * May return empty (Cloudflare protection or dynamic content)
 */
function parseCollegeConsensus(md) {
  const firms = [];

  // Check if page is Cloudflare blocked
  if (md.includes('you have been blocked') || md.includes('enable cookies') || md.length < 1000) {
    return firms; // Blocked page, return empty
  }

  // Try numbered headings
  const numberedMatches = md.matchAll(/^#{2,4}\s*(\d+)[\.\)]\s*(.+?)$/gm);
  for (const m of numberedMatches) {
    let firmName = m[2].trim();
    const linkInHeading = firmName.match(/\[([^\]]+)\]\(([^)]+)\)/);
    let website = '';
    if (linkInHeading) {
      firmName = linkInHeading[1].replace(/\*+/g, '').trim();
      website = linkInHeading[2].trim();
    } else {
      firmName = firmName.replace(/\*+/g, '').trim();
    }

    if (firmName.length < 2) continue;
    if (/^(what|how|why|when|the|our|best|top|conclusion|faq)/i.test(firmName)) continue;

    if (!website) {
      const startIdx = m.index + m[0].length;
      const nextHeadingIdx = md.indexOf('\n##', startIdx + 1);
      const blockEnd = nextHeadingIdx > -1 ? nextHeadingIdx : startIdx + 3000;
      const blockText = md.slice(startIdx, blockEnd);
      const linkMatch = blockText.match(/\[(?:[^\]]*)\]\((https?:\/\/(?!collegeconsensus\.com)[^)]+)\)/);
      if (linkMatch) website = linkMatch[1];
    }

    firms.push({
      firm_name: firmName,
      website,
      domain: extractDomain(website),
      city: '',
      state: '',
      profile_url: 'https://www.collegeconsensus.com/rankings/best-college-admissions-consultants/'
    });
  }

  // Also try link-based extraction
  if (firms.length === 0) {
    const allLinks = md.matchAll(/\[([^\]]+)\]\((https?:\/\/(?!collegeconsensus\.com)[^)]+)\)/g);
    const seen = new Set();
    for (const m of allLinks) {
      const text = m[1].replace(/\*+/g, '').trim();
      const url = m[2].trim();
      const domain = extractDomain(url);
      if (!domain || text.length < 3 || text.length > 80) continue;
      if (seen.has(domain)) continue;
      if (/^(here|click|read more|learn more|visit|link|source)$/i.test(text)) continue;
      if (domain.includes('facebook.com') || domain.includes('twitter.com') ||
          domain.includes('linkedin.com') || domain.includes('youtube.com')) continue;
      seen.add(domain);

      const idx = md.indexOf(m[0]);
      const context = md.slice(Math.max(0, idx - 300), Math.min(md.length, idx + 300)).toLowerCase();
      if (context.includes('admissions') || context.includes('consulting') || context.includes('college') ||
          context.includes('counseling') || context.includes('prep') || context.includes('rank')) {
        firms.push({
          firm_name: text,
          website: url,
          domain,
          city: '',
          state: '',
          profile_url: 'https://www.collegeconsensus.com/rankings/best-college-admissions-consultants/'
        });
      }
    }
  }

  return firms;
}

// ---------------------------------------------------------------------------
// Firm Website Enrichment
// ---------------------------------------------------------------------------

/**
 * Crawl a firm's website homepage via CF Browser Rendering to extract
 * phone, email, and address information from the markdown.
 */
async function enrichFirmFromWebsite(firm) {
  if (!firm.website) return firm;

  let url = firm.website;
  // Ensure it starts with http
  if (!url.startsWith('http')) url = 'https://' + url;

  // Don't crawl ranking sites themselves or visit/redirect links
  const domain = extractDomain(url);
  if (!domain) return firm;
  const skipDomains = [
    'bestcollegeadmissionconsultants.com', 'topcollegeadmissionsconsultants.com',
    'research.com', 'examstrategist.com', 'collegeconsensus.com',
    'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'instagram.com'
  ];
  if (skipDomains.some(d => domain.includes(d))) return firm;

  try {
    log(`  Enriching: ${domain}`);
    const md = await cfRenderMarkdownWithRetry(url);

    // Extract phone
    if (!firm.phone) {
      // Look for tel: links
      const telMatch = md.match(/\[([^\]]*)\]\(tel:([^)]+)\)/);
      if (telMatch) {
        firm.phone = normalizePhone(telMatch[2]);
      }
      // Look for phone patterns in text
      if (!firm.phone) {
        const phonePatterns = md.match(/(?:\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/g);
        if (phonePatterns && phonePatterns.length > 0) {
          firm.phone = normalizePhone(phonePatterns[0]);
        }
      }
      // Look for +1 format
      if (!firm.phone) {
        const intlPhone = md.match(/\+1[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
        if (intlPhone) {
          firm.phone = normalizePhone(intlPhone[0]);
        }
      }
    }

    // Extract email
    if (!firm.email) {
      // Look for mailto: links
      const mailtoMatch = md.match(/\[([^\]]*)\]\(mailto:([^)]+)\)/);
      if (mailtoMatch) {
        firm.email = mailtoMatch[2].trim().split('?')[0]; // Strip ?subject= etc
      }
      // Look for bare email addresses
      if (!firm.email) {
        const emailMatches = md.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g);
        if (emailMatches) {
          // Prefer info@, contact@, hello@, admissions@ over random addresses
          const preferred = emailMatches.find(e =>
            /^(info|contact|hello|admissions|team|office|support|inquiry|inquiries)@/i.test(e)
          );
          firm.email = preferred || emailMatches[0];
        }
      }
    }

    // Extract address/location from the page
    if (!firm.city && !firm.state) {
      // Look for US address patterns
      const addressMatch = md.match(/([A-Z][a-zA-Z\s]+),\s*([A-Z]{2})\s+(\d{5})/);
      if (addressMatch) {
        firm.city = addressMatch[1].trim();
        firm.state = addressMatch[2].trim();
      }
    }

  } catch (err) {
    log(`  [WARN] Failed to enrich ${domain}: ${err.message}`);
  }

  return firm;
}

// ---------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------

/**
 * Known name aliases for dedup (map variant -> canonical).
 */
const NAME_ALIASES = {
  'alisteducation': 'testive',
  'alisteducationtestivecom': 'testive',
  'testiveacademic': 'testive',
  'brightorizonsollegecoach': 'collegecoach',
  'brighthorizonscollegecoach': 'collegecoach',
  'varsitytutors': 'varsitytutors',
  'solomon': 'solomonadmissions',
  'toptier': 'toptieradmissions',
  'toptierad': 'toptieradmissions',
};

/**
 * Normalize firm name for dedup comparison.
 * Strips common suffixes like "Admissions Consulting", "College Admissions", etc.
 */
function normalizeFirmName(name) {
  let key = name.toLowerCase()
    .replace(/\s*\([^)]*\)\s*$/i, '') // strip trailing parenthetical
    .replace(/\s+(college\s+)?(admissions?\s*)?(consulting|consultants?|counseling|advisors?)?\s*$/i, '')
    .replace(/[^a-z0-9]/g, '');
  // Apply known aliases
  if (NAME_ALIASES[key]) key = NAME_ALIASES[key];
  return key;
}

function deduplicateByDomain(allFirms) {
  const byDomain = new Map(); // domain -> firm
  const byName = new Map();   // normalized name -> firm
  const deduped = [];

  for (const firm of allFirms) {
    const domain = firm.domain;
    const nameKey = normalizeFirmName(firm.firm_name);

    // Check if this firm already exists by domain OR by name
    let existing = null;
    if (domain && byDomain.has(domain)) {
      existing = byDomain.get(domain);
    } else if (nameKey && byName.has(nameKey)) {
      existing = byName.get(nameKey);
    }

    if (existing) {
      // Merge: fill in missing fields from the new record
      if (!existing.city && firm.city) existing.city = firm.city;
      if (!existing.state && firm.state) existing.state = firm.state;
      if (!existing.email && firm.email) existing.email = firm.email;
      if (!existing.phone && firm.phone) existing.phone = firm.phone;
      if (!existing.website && firm.website) {
        existing.website = firm.website;
        existing.domain = firm.domain;
      }
      if (!existing.profile_url && firm.profile_url) existing.profile_url = firm.profile_url;
      // Append source if different
      if (firm.source && !existing.source.includes(firm.source)) {
        existing.source += ',' + firm.source;
      }
      // Update domain mapping if we just got a domain
      if (firm.domain && !byDomain.has(firm.domain)) {
        byDomain.set(firm.domain, existing);
      }
      continue;
    }

    // New firm
    if (domain) byDomain.set(domain, firm);
    if (nameKey) byName.set(nameKey, firm);
    deduped.push(firm);
  }

  return deduped;
}

// ---------------------------------------------------------------------------
// CSV Output
// ---------------------------------------------------------------------------

function writeCSV(leads) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => escapeCSV(lead[col] || '')).join(',')
  );
  const csv = [header, ...rows].join('\n') + '\n';
  fs.writeFileSync(OUTPUT_FILE, csv, 'utf8');
  return OUTPUT_FILE;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(65));
  console.log('College Admissions Rankings Scraper (Cloudflare Browser Rendering)');
  console.log('='.repeat(65));
  if (TEST_MODE) console.log('[TEST MODE] Skipping firm website enrichment\n');
  console.log(`Sources: ${SOURCES.length}`);
  console.log(`Output:  ${OUTPUT_FILE}\n`);

  const parsers = {
    'topcollegeadmissionsconsultants': parseTopCollegeAdmissions,
    'bestcollegeadmissionconsultants': parseBestCollegeAdmission,
    'research-com': parseResearchCom,
    'examstrategist': parseExamStrategist,
    'collegeconsensus': parseCollegeConsensus
  };

  const allFirms = [];
  const sourceCounts = {};

  // Step 1: Fetch and parse each source
  for (let i = 0; i < SOURCES.length; i++) {
    const source = SOURCES[i];
    console.log(`\n${'─'.repeat(65)}`);
    console.log(`[${i + 1}/${SOURCES.length}] ${source.label}`);
    console.log(`${'─'.repeat(65)}`);

    try {
      const md = await cfRenderMarkdownWithRetry(source.url);

      // Debug: save markdown for inspection
      const debugDir = path.join(OUTPUT_DIR, 'debug');
      if (!fs.existsSync(debugDir)) fs.mkdirSync(debugDir, { recursive: true });
      fs.writeFileSync(path.join(debugDir, `rankings-${source.name}.md`), md, 'utf8');
      log(`Debug markdown saved to output/debug/rankings-${source.name}.md`);

      const parser = parsers[source.name];
      const firms = parser(md);

      // Tag each firm with source info
      for (const firm of firms) {
        firm.source = `rankings-${source.name}`;
        firm.niche = 'education';
        firm.country = 'US';
        firm.title = firm.title || 'College Admissions Consulting Firm';
        firm.first_name = firm.first_name || '';
        firm.last_name = firm.last_name || '';
      }

      log(`Extracted ${firms.length} firms from ${source.label}`);
      sourceCounts[source.label] = firms.length;
      allFirms.push(...firms);

      // Log firm names for verification
      if (firms.length > 0) {
        for (const f of firms) {
          const extra = f.domain ? ` (${f.domain})` : '';
          const loc = f.city && f.state ? ` — ${f.city}, ${f.state}` : '';
          console.log(`    ${f.rank ? '#' + f.rank + ' ' : ''}${f.firm_name}${extra}${loc}`);
        }
      }

    } catch (err) {
      log(`[ERROR] Failed to scrape ${source.label}: ${err.message}`);
      sourceCounts[source.label] = 0;
    }

    // 3s delay between CF API calls
    if (i < SOURCES.length - 1) {
      log(`Waiting ${CF_DELAY_MS / 1000}s before next source...`);
      await sleep(CF_DELAY_MS);
    }
  }

  // Step 2: Deduplicate by domain
  console.log(`\n${'─'.repeat(65)}`);
  log(`Total firms before dedup: ${allFirms.length}`);
  const deduped = deduplicateByDomain(allFirms);
  log(`Total firms after dedup:  ${deduped.length} (removed ${allFirms.length - deduped.length} dupes)`);

  // Step 3: Enrich from firm websites (unless --test)
  if (!TEST_MODE && deduped.length > 0) {
    console.log(`\n${'─'.repeat(65)}`);
    console.log('Enriching firms from their websites...');
    console.log(`${'─'.repeat(65)}`);

    let enriched = 0;
    for (let i = 0; i < deduped.length; i++) {
      const firm = deduped[i];
      const before = { email: firm.email, phone: firm.phone, city: firm.city };

      await enrichFirmFromWebsite(firm);

      const gained = [];
      if (!before.email && firm.email) gained.push('email');
      if (!before.phone && firm.phone) gained.push('phone');
      if (!before.city && firm.city) gained.push('city');
      if (gained.length > 0) {
        enriched++;
        log(`  +${gained.join(', ')} for ${firm.firm_name}`);
      }

      // delay between CF API calls for enrichment
      if (i < deduped.length - 1) {
        await sleep(CF_ENRICH_DELAY_MS);
      }
    }

    log(`Enriched ${enriched}/${deduped.length} firms with new data`);
  } else if (TEST_MODE) {
    log('Skipping firm website enrichment (--test mode)');
  }

  // Step 4: Write CSV
  const outPath = writeCSV(deduped);

  // Step 5: Summary
  console.log('\n' + '='.repeat(65));
  console.log('SUMMARY');
  console.log('='.repeat(65));

  console.log('\nFirms per source:');
  for (const [label, count] of Object.entries(sourceCounts)) {
    console.log(`  ${label}: ${count}`);
  }

  console.log(`\nTotal unique firms: ${deduped.length}`);
  console.log(`With website:       ${deduped.filter(f => f.website).length}`);
  console.log(`With email:         ${deduped.filter(f => f.email).length}`);
  console.log(`With phone:         ${deduped.filter(f => f.phone).length}`);
  console.log(`With city/state:    ${deduped.filter(f => f.city || f.state).length}`);

  // Top domains
  console.log('\nFirms:');
  for (const f of deduped) {
    const parts = [f.firm_name];
    if (f.domain) parts.push(f.domain);
    if (f.email) parts.push(f.email);
    if (f.phone) parts.push(f.phone);
    if (f.city && f.state) parts.push(`${f.city}, ${f.state}`);
    console.log(`  ${parts.join(' | ')}`);
  }

  console.log(`\nCSV written to: ${outPath}`);
  console.log('='.repeat(65));
}

main().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});
