#!/usr/bin/env node
/**
 * Nursing Faculty Directory Email Crawler
 *
 * Reads output/college-scorecard-nursing-schools.csv and for each school tries a set
 * of common nursing-faculty directory path patterns. Parses mailto: links AND
 * text-embedded emails; associates names from surrounding anchor text / card markup.
 *
 * Resumable via output/nursing-faculty-emails.state.json.
 *
 * Usage:
 *   node scripts/crawl-nursing-faculty-emails.js [--limit N] [--concurrency N]
 *
 * Output:
 *   output/nursing-faculty-emails.csv
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const INPUT_CSV = path.resolve(__dirname, '..', 'output', 'college-scorecard-nursing-schools.csv');
const OUTPUT_DIR = path.resolve(__dirname, '..', 'output');
const OUTPUT_CSV = path.join(OUTPUT_DIR, 'nursing-faculty-emails.csv');
const STATE_FILE = path.join(OUTPUT_DIR, 'nursing-faculty-emails.state.json');

const args = process.argv.slice(2);
function argVal(flag, fallback) {
  const i = args.indexOf(flag);
  if (i === -1) return fallback;
  return args[i + 1];
}
const LIMIT = parseInt(argVal('--limit', '0'), 10) || 0;
const CONCURRENCY = parseInt(argVal('--concurrency', '10'), 10) || 10;
const PER_SCHOOL_RATE_MS = 1000;

const FACULTY_PATHS = [
  '/nursing/faculty',
  '/nursing/directory',
  '/nursing/home/directory',
  '/son/faculty',
  '/academics/nursing/faculty',
  '/nursing/about/directory',
  '/nursing/people',
  '/nursing/staff',
  '/about/faculty',
  '/con/faculty',
  '/nursing/faculty-staff',
  '/nursing/faculty-and-staff',
  '/nursing/about/faculty',
  '/nursing/about/people',
  '/nursing',
];

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
    } catch {
      return { completed: {} };
    }
  }
  return { completed: {} };
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  const headers = splitCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitCsvLine(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => (obj[h] = cols[idx] || ''));
    rows.push(obj);
  }
  return rows;
}

function splitCsvLine(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') {
        inQ = false;
      } else {
        cur += c;
      }
    } else {
      if (c === '"') inQ = true;
      else if (c === ',') {
        out.push(cur);
        cur = '';
      } else cur += c;
    }
  }
  out.push(cur);
  return out;
}

function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function extractDomain(urlStr) {
  try {
    const u = new URL(urlStr);
    return u.hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function rootDomain(hostname) {
  // .edu is common for US schools; for our purposes "uab.edu" is root of "nursing.uab.edu"
  const parts = hostname.split('.');
  if (parts.length >= 2) return parts.slice(-2).join('.');
  return hostname;
}

function httpGet(urlStr, redirectsLeft = 5) {
  return new Promise((resolve) => {
    let u;
    try {
      u = new URL(urlStr);
    } catch {
      return resolve({ status: 0, body: '', error: 'bad-url', finalUrl: urlStr });
    }
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.get(
      urlStr,
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
          Accept: 'text/html,application/xhtml+xml',
          'Accept-Language': 'en-US,en;q=0.9',
        },
        timeout: 15000,
      },
      (res) => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
          const next = new URL(res.headers.location, urlStr).toString();
          res.resume();
          return resolve(httpGet(next, redirectsLeft - 1));
        }
        let data = '';
        res.on('data', (c) => {
          data += c;
          if (data.length > 5_000_000) {
            req.destroy();
            resolve({ status: res.statusCode, body: data, finalUrl: urlStr });
          }
        });
        res.on('end', () => resolve({ status: res.statusCode, body: data, finalUrl: urlStr }));
      }
    );
    req.on('error', (err) => resolve({ status: 0, body: '', error: err.message, finalUrl: urlStr }));
    req.on('timeout', () => {
      req.destroy();
      resolve({ status: 0, body: '', error: 'timeout', finalUrl: urlStr });
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Find emails in HTML plus attempt name association.
 *
 * Returns [{email, firstName, lastName}]
 */
function extractEmailsFromHtml(html, rootDom) {
  const found = new Map(); // email -> {firstName, lastName}

  // 1) mailto: anchors with inner text
  const mailtoRe = /<a[^>]*href\s*=\s*["']mailto:([^"'?]+)(?:\?[^"']*)?["'][^>]*>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = mailtoRe.exec(html)) !== null) {
    const email = m[1].trim().toLowerCase();
    const inner = stripTags(m[2]).trim();
    if (!isValidEmail(email)) continue;
    if (!matchesRootDomain(email, rootDom)) continue;
    const existing = found.get(email) || { firstName: '', lastName: '' };
    const nameFromAnchor = inner && !/@/.test(inner) ? splitNameStrict(inner) : null;
    if (nameFromAnchor && (!existing.firstName || !existing.lastName)) {
      if (nameFromAnchor.firstName) existing.firstName = existing.firstName || nameFromAnchor.firstName;
      if (nameFromAnchor.lastName) existing.lastName = existing.lastName || nameFromAnchor.lastName;
    }
    if (!existing.firstName && !existing.lastName) {
      // Try headshot image filename first (most reliable on university CON pages)
      const handle = email.split('@')[0].toLowerCase();
      const nameFromImg = findNameFromNearbyImage(html, m.index, handle);
      if (nameFromImg) {
        existing.firstName = nameFromImg.firstName;
        existing.lastName = nameFromImg.lastName;
      }
      if (!existing.firstName && !existing.lastName) {
        // Text-based fallback
        const ctx = getSurroundingText(html, m.index, 600);
        const nameFromCtx = findNameNearEmail(ctx, email);
        if (nameFromCtx) {
          existing.firstName = nameFromCtx.firstName;
          existing.lastName = nameFromCtx.lastName;
        }
      }
    }
    found.set(email, existing);
  }

  // 2) Plain-text emails in page body
  const textEmailRe = /([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})/g;
  const seenText = new Set();
  let tm;
  while ((tm = textEmailRe.exec(html)) !== null) {
    const email = tm[1].trim().toLowerCase();
    if (seenText.has(email)) continue;
    seenText.add(email);
    if (!isValidEmail(email)) continue;
    if (!matchesRootDomain(email, rootDom)) continue;
    if (found.has(email)) continue;
    const entry = { firstName: '', lastName: '' };
    const handle = email.split('@')[0].toLowerCase();
    const nameFromImg = findNameFromNearbyImage(html, tm.index, handle);
    if (nameFromImg) {
      entry.firstName = nameFromImg.firstName;
      entry.lastName = nameFromImg.lastName;
    }
    if (!entry.firstName && !entry.lastName) {
      const ctx = getSurroundingText(html, tm.index, 600);
      const nameFromCtx = findNameNearEmail(ctx, email);
      if (nameFromCtx) {
        entry.firstName = nameFromCtx.firstName;
        entry.lastName = nameFromCtx.lastName;
      }
    }
    // Try to infer from email handle if no contextual name
    if (!entry.firstName && !entry.lastName) {
      const handle = email.split('@')[0];
      const inferred = inferNameFromHandle(handle);
      if (inferred) {
        entry.firstName = inferred.firstName;
        entry.lastName = inferred.lastName;
      }
    }
    found.set(email, entry);
  }

  // Post-process: title-case names
  return Array.from(found.entries()).map(([email, v]) => ({
    email,
    firstName: titleCase(v.firstName),
    lastName: titleCase(v.lastName),
  }));
}

function titleCase(s) {
  if (!s) return '';
  const cleaned = String(s).trim();
  if (!cleaned) return '';
  // Keep known honorifics/suffixes uppercase
  if (/^(PhD|DNP|MSN|RN|BSN|FNP|FAAN|FACN|MSc|MD)$/i.test(cleaned)) return cleaned.toUpperCase();
  return cleaned
    .split(/\s+/)
    .map((w) => {
      if (!w) return '';
      // Preserve hyphens: "Smith-Jones" -> "Smith-Jones"
      return w
        .split('-')
        .map((p) => (p[0] ? p[0].toUpperCase() + p.slice(1).toLowerCase() : p))
        .join('-');
    })
    .join(' ');
}

function stripTags(html) {
  return html
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function getSurroundingText(html, idx, radius) {
  const start = Math.max(0, idx - radius);
  const end = Math.min(html.length, idx + radius);
  return stripTags(html.slice(start, end));
}

function isValidEmail(e) {
  // Reject common junk, images, tracking
  if (!/^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/.test(e)) return false;
  if (/\.(png|jpg|jpeg|gif|svg|webp|ico|pdf|css|js)$/i.test(e)) return false;
  if (/^(info|example|test|no-?reply|noreply|donotreply|webmaster|postmaster|admin|support|help|contact|mail|email|office|hello|sales|marketing)@/i.test(e)) return false;
  if (/sentry|cloudflare|wixpress|googlemail|png$|jpg$/i.test(e)) return false;
  // Reject emails that are clearly generic department addresses
  if (/^(nursing|graduate-nursing|undergraduate-nursing|mailto|webadmin|it|hr|registrar|admissions|admissions-nursing|dnp|msn|phd-nursing|student-nursing|faculty-nursing)@/i.test(e)) return false;
  // Reject literal "mailto@" that can appear if a broken <a href="mailto:"></a> exists
  if (/^mailto@/i.test(e)) return false;
  return true;
}

function matchesRootDomain(email, rootDom) {
  if (!rootDom) return false;
  const dom = email.split('@')[1].toLowerCase();
  return dom === rootDom || dom.endsWith('.' + rootDom);
}

// Split "Jane Doe, PhD, RN" -> {firstName: "Jane", lastName: "Doe"}
function splitName(raw) {
  if (!raw) return null;
  // strip degree suffixes
  let s = raw.replace(/\s+/g, ' ').trim();
  s = s.split(',')[0].trim();
  s = s.replace(/\b(Dr|Prof|Professor|Mr|Mrs|Ms|Mx)\.?\s+/i, '');
  const parts = s.split(' ').filter(Boolean);
  if (parts.length < 2) return null;
  if (parts.length > 4) return null; // probably a sentence
  const first = parts[0];
  const last = parts[parts.length - 1];
  if (!/^[A-Za-z][A-Za-z'.-]+$/.test(first)) return null;
  if (!/^[A-Za-z][A-Za-z'.-]+$/.test(last)) return null;
  return { firstName: first, lastName: last };
}

// Reject common non-name phrases (department / title words)
const NON_NAME_WORDS = new Set(
  [
    'health','systems','continuing','care','academic','affairs','global','partnerships','student','success',
    'alumni','relations','nursing','school','college','department','dean','associate','assistant',
    'professor','clinical','instructor','chair','director','center','institute','research',
    'phd','dnp','msn','rn','fnp','pmhnp','wocn','ccrn','facn','faan','fno','faaohn','emertia','emeritus',
    'office','office of','university','state','regional','administrative','program','faculty','staff',
    'graduate','undergraduate','academic','advising','outreach','services','admin','administration',
    'office for','office of the','about','about us','contact','contact us','directory','people',
    'position','title','credentials','degrees','education','biography','bio','profile','phone',
    'email','phone:','email:','view','read more','learn more','info','information',
  ].map((s) => s.toLowerCase())
);

function looksLikePersonName(parts) {
  if (parts.length < 2) return false;
  if (parts.length > 4) return false;
  for (const p of parts) {
    if (NON_NAME_WORDS.has(p.toLowerCase())) return false;
    // Require mostly alpha + possibly '.'/hyphen
    if (!/^[A-Z][a-zA-Z'.-]{1,}$/.test(p)) return false;
    // Reject all-caps credentials (PHD, RN, DNP)
    if (p.length <= 5 && p.toUpperCase() === p) return false;
  }
  return true;
}

function splitNameStrict(raw) {
  if (!raw) return null;
  let s = raw.replace(/&amp;/g, '&').replace(/\s+/g, ' ').trim();
  // Cut off credentials at first comma
  s = s.split(/[,|]/)[0].trim();
  s = s.replace(/\b(Dr|Prof|Professor|Mr|Mrs|Ms|Mx)\.?\s+/i, '');
  const parts = s.split(' ').filter(Boolean);
  if (!looksLikePersonName(parts)) return null;
  return { firstName: parts[0], lastName: parts[parts.length - 1] };
}

// Find a headshot image filename BEFORE an email (pattern "Berrien_Samantha.jpg").
// Verify it actually belongs to this email by matching handle pieces.
function findNameFromNearbyImage(html, emailIdx, emailHandle) {
  // Shorter window — directly-before-email pattern
  const before = html.slice(Math.max(0, emailIdx - 1500), emailIdx);
  const imgRe = /(?:src|alt)\s*=\s*["']([^"']*\/)?([A-Z][a-zA-Z]+)[ _-]([A-Z][a-zA-Z]+)(?:[ _-][A-Z][a-zA-Z]+)?\.(?:jpg|jpeg|png|webp)["']/gi;
  let m, last;
  while ((m = imgRe.exec(before)) !== null) last = m;
  if (!last) return null;
  const a = last[2];
  const b = last[3];
  if (!a || !b) return null;
  const aLow = a.toLowerCase();
  const bLow = b.toLowerCase();
  if (NON_NAME_WORDS.has(aLow) || NON_NAME_WORDS.has(bLow)) return null;
  // Verify the image belongs to this email. Compute handle-relatedness score for each side.
  const h = (emailHandle || '').toLowerCase().replace(/\d+$/, '');

  // Score: higher = more likely this side matches the handle
  const scoreAgainst = (side) => {
    if (!side) return 0;
    if (h === side) return 100;
    if (h.startsWith(side)) return 80;
    if (h.endsWith(side)) return 75;
    if (h.includes(side)) return 60;
    if (side.startsWith(h.replace(/\./g, ''))) return 40;
    if (side[0] === h[0]) return 10;
    return 0;
  };

  // Most universities use "first.last" or flast or firstl patterns in email
  // Infer order from what each side matches
  const aScore = scoreAgainst(aLow);
  const bScore = scoreAgainst(bLow);
  const maxScore = Math.max(aScore, bScore);
  if (maxScore < 10) return null;

  // The side with the higher match-score for the SECOND half of a "first.last" pattern is the
  // last name. Conversely, the side matching the FIRST portion is the first name.
  // Example: handle=jennifer.bail, a=Jennifer b=Bail → aScore on "jennifer"=100, bScore on "bail"=100
  // Example: handle=keakins, a=Akins b=Kristina → aScore ("akins") h=keakins, endsWith=75; bScore ("kristina") h[0]=k, side[0]=k → 10.
  //   So Akins wins -> lastName.
  // Example: handle=abarmstrong, a=Armstrong b=Alexandra → h starts with 'a' for both; a ends with 'armstrong'? handle is 'abarmstrong' -> endsWith('armstrong')=true (75). b 'alexandra' not included (0+10 initial).
  //   So Armstrong wins -> lastName.

  // If handle has a dot, the part before dot = first name, after = last name
  if (h.includes('.')) {
    const [firstPart, lastPart] = h.split('.');
    const aMatchesFirst = aLow === firstPart || firstPart.startsWith(aLow.slice(0, 3));
    const aMatchesLast = aLow === lastPart || lastPart.startsWith(aLow.slice(0, 3));
    const bMatchesFirst = bLow === firstPart || firstPart.startsWith(bLow.slice(0, 3));
    const bMatchesLast = bLow === lastPart || lastPart.startsWith(bLow.slice(0, 3));
    if (aMatchesFirst && bMatchesLast) {
      return { firstName: a, lastName: b, altOrder: { firstName: b, lastName: a } };
    }
    if (bMatchesFirst && aMatchesLast) {
      return { firstName: b, lastName: a, altOrder: { firstName: a, lastName: b } };
    }
  }

  // Pick whichever side has a higher endsWith/contains score → that's the last name (handle
  // usually ends with lastname in flast / firstl / lastf patterns).
  const aEnds = h.endsWith(aLow) || h.slice(-aLow.length) === aLow;
  const bEnds = h.endsWith(bLow) || h.slice(-bLow.length) === bLow;
  if (aEnds && !bEnds) {
    return { firstName: b, lastName: a, altOrder: { firstName: a, lastName: b } };
  }
  if (bEnds && !aEnds) {
    return { firstName: a, lastName: b, altOrder: { firstName: b, lastName: a } };
  }
  // Default: Lastname_Firstname (academic photo library convention)
  return { firstName: b, lastName: a, altOrder: { firstName: a, lastName: b } };
}

function findNameNearEmail(ctx, email) {
  // Search text BEFORE the email for person-name-like runs.
  const idx = ctx.toLowerCase().lastIndexOf(email.toLowerCase());
  if (idx === -1) return null;
  const before = ctx.slice(Math.max(0, idx - 600), idx);

  // Find all candidate "two-to-four cap word" runs (e.g. "Samantha Berrien", "Anne Marie Smith")
  // A candidate must:
  //  - consist only of cap words, each >= 2 chars
  //  - not include any NON_NAME_WORDS
  //  - have distinct plausible first-and-last
  const candidates = [];
  const re = /\b([A-Z][a-zA-Z'.-]{1,})(?:\s+([A-Z][a-zA-Z'.-]{1,})){1,3}\b/g;
  let m;
  while ((m = re.exec(before)) !== null) {
    const phrase = m[0];
    const parts = phrase.split(/\s+/);
    let bad = false;
    for (const p of parts) {
      const low = p.toLowerCase().replace(/[.']/g, '');
      if (NON_NAME_WORDS.has(low)) {
        bad = true;
        break;
      }
      if (/^(phd|dnp|msn|rn|bsn|fnp|facnp|faan|facn|pmhnp|ccrn|ma|mph|edd|dns|aprn|wocn|crnp|whnpbc|mba)$/i.test(p)) {
        bad = true;
        break;
      }
      if (p.length <= 4 && p.toUpperCase() === p) {
        bad = true;
        break;
      }
    }
    if (bad) continue;
    if (parts.length < 2 || parts.length > 4) continue;
    candidates.push({ phrase, offset: m.index });
  }
  if (candidates.length === 0) return null;
  // Pick the candidate closest to (and before) the email.
  candidates.sort((a, b) => b.offset - a.offset);
  for (const c of candidates) {
    const result = splitNameStrict(c.phrase);
    if (result) return result;
  }
  return null;
}

function inferNameFromHandle(handle) {
  // Patterns: first.last
  if (handle.includes('.')) {
    const parts = handle.split('.');
    if (parts.length === 2 && parts[0].length > 1 && parts[1].length > 1) {
      // strip trailing digits
      const first = parts[0].replace(/\d+$/, '');
      const last = parts[1].replace(/\d+$/, '');
      if (first.length >= 2 && last.length >= 2 && /^[a-z]+$/i.test(first) && /^[a-z]+$/i.test(last)) {
        return { firstName: cap(first), lastName: cap(last) };
      }
    }
  }
  return null;
}

function cap(s) {
  if (!s) return '';
  return s[0].toUpperCase() + s.slice(1).toLowerCase();
}

async function crawlSchool(school) {
  const rootUrl = school.school_url;
  if (!rootUrl) return { emails: [], patternsHit: [] };
  const rootDom = rootDomain(extractDomain(rootUrl));
  if (!rootDom) return { emails: [], patternsHit: [] };

  const allEmails = new Map();
  const patternsHit = [];

  // Build list of (baseUrl, path) pairs:
  //  1. Subdomain hosts like nursing.<root>, son.<root>, con.<root>
  //  2. Paths on the main host
  const SUBDOMAIN_HOSTS = ['nursing', 'son', 'con', 'nurs'];
  const baseUrls = [rootUrl];
  for (const sub of SUBDOMAIN_HOSTS) {
    baseUrls.push(`https://${sub}.${rootDom}`);
  }
  const subPaths = ['/', '/faculty', '/directory', '/people', '/about/faculty', '/faculty-and-staff', '/about/directory', '/faculty-staff', '/staff'];

  const urls = [];
  for (const base of baseUrls) {
    // If it's a subdomain host, try the sub-paths (root-relative)
    if (base !== rootUrl) {
      for (const p of subPaths) urls.push(base + p);
    }
  }
  // Plus traditional /nursing/* paths on the main host
  for (const p of FACULTY_PATHS) urls.push(new URL(p, rootUrl).toString());

  for (const url of urls) {
    const res = await httpGet(url);
    await sleep(PER_SCHOOL_RATE_MS);
    if (res.status !== 200 || !res.body) continue;
    // Heuristic: must look like faculty/directory content
    const lower = res.body.toLowerCase();
    const looksLikeDirectory =
      /faculty|directory|professor|instructor|dean|chair|nursing/.test(lower) && res.body.length > 2000;
    if (!looksLikeDirectory) continue;

    const emails = extractEmailsFromHtml(res.body, rootDom);
    if (emails.length === 0) continue;
    patternsHit.push({ pattern: url, count: emails.length });
    for (const e of emails) {
      if (!allEmails.has(e.email)) allEmails.set(e.email, e);
    }
  }

  return {
    emails: Array.from(allEmails.values()),
    patternsHit,
    rootDom,
  };
}

async function main() {
  if (!fs.existsSync(INPUT_CSV)) {
    console.error(`Missing input: ${INPUT_CSV}`);
    console.error('Run scripts/scrape-college-scorecard-nursing.js first.');
    process.exit(1);
  }
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const csvText = fs.readFileSync(INPUT_CSV, 'utf-8');
  const rows = parseCsv(csvText);
  let schools = rows.filter((r) => r.school_url && r.school_url.startsWith('http'));
  console.log(`Loaded ${schools.length} schools with URLs (of ${rows.length} total).`);

  const state = loadState();

  // Open CSV in append mode; write header if not present
  let needsHeader = !fs.existsSync(OUTPUT_CSV) || fs.statSync(OUTPUT_CSV).size === 0;
  const out = fs.openSync(OUTPUT_CSV, 'a');
  if (needsHeader) {
    const headers = [
      'email',
      'first_name',
      'last_name',
      'school_name',
      'school_url',
      'domain',
      'cip_code',
      'state',
      'country',
      'niche',
      'source',
    ];
    fs.writeSync(out, headers.join(',') + '\n');
  }

  if (LIMIT > 0) schools = schools.slice(0, LIMIT);

  let completed = 0;
  let totalEmails = 0;
  let idx = 0;

  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= schools.length) return;
      const s = schools[i];
      if (state.completed[s.ipeds_id]) {
        completed++;
        continue;
      }
      try {
        const result = await crawlSchool(s);
        for (const e of result.emails) {
          const line = [
            csvEscape(e.email),
            csvEscape(e.firstName || ''),
            csvEscape(e.lastName || ''),
            csvEscape(s.school_name),
            csvEscape(s.school_url),
            csvEscape(result.rootDom || ''),
            csvEscape(s.cip_code),
            csvEscape(s.state),
            csvEscape('US'),
            csvEscape('nursing-education'),
            csvEscape('college-scorecard+faculty-crawl'),
          ].join(',');
          fs.writeSync(out, line + '\n');
        }
        totalEmails += result.emails.length;
        state.completed[s.ipeds_id] = {
          emails: result.emails.length,
          patterns: result.patternsHit.map((p) => p.pattern),
          ts: Date.now(),
        };
      } catch (err) {
        state.completed[s.ipeds_id] = { error: err.message, ts: Date.now() };
      }
      completed++;
      if (completed % 5 === 0 || completed === schools.length) {
        saveState(state);
        console.log(
          `[${completed}/${schools.length}] ${s.school_name} (${s.state}) -> ` +
            `${(state.completed[s.ipeds_id] || {}).emails || 0} emails | cumulative=${totalEmails}`
        );
      }
    }
  }

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);

  saveState(state);
  fs.closeSync(out);
  console.log(`\nDone. ${completed} schools processed, ${totalEmails} emails written -> ${OUTPUT_CSV}`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error('FATAL:', err);
    process.exit(1);
  });
}
