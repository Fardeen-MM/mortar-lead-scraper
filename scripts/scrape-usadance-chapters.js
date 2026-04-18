#!/usr/bin/env node
/**
 * USA Dance Chapter Directory scraper.
 * Chapter listings page: https://usadance.org/page/USADanceChapters
 * Links out to https://membership.usadance.org/members/group.aspx?id=XXXX
 * Each chapter page exposes chapter name, contact info, social links.
 *
 * NOTE: USA Dance is a nonprofit chapter network, not 40K studios. Scope is ~100 US chapters.
 * For the 40K dance studios goal, those registries are BLOCKED (Dance/USA behind login; DSA dead DNS).
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const OUTPUT = path.join(__dirname, '..', 'output', 'dance-studios.csv');
const LIST_URL = 'https://usadance.org/page/USADanceChapters';
const STATE_FILE = path.join(__dirname, '..', 'output', '.usadance-state.json');

function httpGet(url) {
  const u = new URL(url);
  return new Promise((resolve, reject) => {
    const req = https.get({
      hostname: u.hostname,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
      },
      timeout: 40000,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(httpGet(new URL(res.headers.location, url).href));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val).replace(/\r?\n/g, ' ').replace(/\s+/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function decodeHtml(s) {
  if (!s) return '';
  return String(s).replace(/&amp;/g, '&').replace(/&#038;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ');
}

function extractEmail(html) {
  const m = html.match(/mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
  if (m) return m[1].toLowerCase();
  const m2 = html.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
  return m2 ? m2[1].toLowerCase() : '';
}

function extractPhone(html) {
  const m = html.match(/tel:([+0-9\-\s()]+)/);
  if (m) return m[1].trim();
  const m2 = html.match(/\(?([2-9]\d{2})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})/);
  return m2 ? `(${m2[1]}) ${m2[2]}-${m2[3]}` : '';
}

function extractLinkedUrls(html) {
  const urls = new Set();
  const re = /href="(https?:\/\/[^"]+)"/gi;
  let m;
  while ((m = re.exec(html))) {
    const u = m[1];
    if (u.includes('usadance.org') || u.includes('ymaws') || u.includes('ymcomm') || u.includes('linkedin.com') || u.includes('twitter.com') || u.includes('facebook.com') || u.includes('youtube.com') || u.includes('instagram.com')) continue;
    urls.add(u);
  }
  return [...urls];
}

function parseChapterPage(html, chapterId, chapterUrl) {
  // The chapter name lives in an ASP.NET span: ctl00_PageContent_lblPageSummaryTitle
  const summaryMatch = html.match(/id="ctl00_PageContent_lblPageSummaryTitle"[^>]*>([^<]+)</);
  let chapterName = summaryMatch ? decodeHtml(summaryMatch[1]).trim() : '';
  // Format: "Chapter: Los Angeles County, CA - #4031"
  let city = '', state = '', chapterNumber = '';
  if (chapterName) {
    const parse = chapterName.match(/Chapter:\s*([^,]+?),\s*([A-Z]{2})\s*-\s*#?(\d{3,5})/);
    if (parse) {
      city = parse[1].trim();
      state = parse[2].trim();
      chapterNumber = parse[3];
    } else {
      const altNum = chapterName.match(/#?(\d{3,5})/);
      if (altNum) chapterNumber = altNum[1];
      const altState = chapterName.match(/\b([A-Z]{2})\b/);
      if (altState) state = altState[1];
    }
  }

  // Isolate the chapter description region (after the summary title up to ~2000 chars or next major tag)
  const sumIdx = html.indexOf('ctl00_PageContent_lblPageSummaryTitle');
  let mainBlock = html;
  if (sumIdx > 0) {
    const end = html.indexOf('<div class="irailcontent"', sumIdx);
    mainBlock = html.slice(sumIdx, end > 0 ? end : sumIdx + 10000);
  }

  const email = extractEmail(mainBlock);
  const phone = extractPhone(mainBlock);
  // external websites from chapter region only (avoids pulling CDN/shared links)
  const websites = extractLinkedUrls(mainBlock).filter(u => !/cdnjs|jquery|bootstrap|google\.com|schema\.org|googleapis|w3\.org|usadancenationals|lev-vesnovskiy|o2cm\.com|ballroomcompexpress|americandancer|worlddancesport|myshopify|docs\.google/.test(u));
  const firstWebsite = websites[0] || '';

  return {
    chapter_id: chapterId,
    chapter_number: chapterNumber,
    chapter_name: chapterName,
    city,
    state,
    email,
    phone,
    website: firstWebsite,
    other_websites: websites.slice(1, 5).join('; '),
    profile_url: chapterUrl,
    niche: 'dance chapter',
    country: 'US',
    source: 'usadance',
  };
}

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8')); } catch {}
  }
  return { doneIds: [] };
}
function saveState(s) { fs.writeFileSync(STATE_FILE, JSON.stringify(s)); }

async function main() {
  console.log('=== USA Dance Chapter Scraper ===');

  const listHtml = await httpGet(LIST_URL);
  const linkRe = /href="(https?:\/\/membership\.usadance\.org\/members\/group\.aspx\?id=(\d+))"/gi;
  const seen = new Set();
  const chapters = [];
  let m;
  while ((m = linkRe.exec(listHtml))) {
    const id = m[2];
    if (seen.has(id)) continue;
    seen.add(id);
    chapters.push({ id, url: m[1] });
  }
  console.log(`  Found ${chapters.length} chapter URLs`);

  const cols = ['chapter_id','chapter_number','chapter_name','city','state','email','phone','website','other_websites','profile_url','niche','country','source'];
  const state = loadState();
  const isResume = fs.existsSync(OUTPUT) && state.doneIds.length > 0;
  const out = fs.createWriteStream(OUTPUT, { flags: isResume ? 'a' : 'w' });
  if (!isResume) out.write(cols.join(',') + '\n');

  let withEmail = 0, withWebsite = 0, total = 0;

  for (const { id, url } of chapters) {
    if (state.doneIds.includes(id)) continue;
    try {
      const html = await httpGet(url);
      const row = parseChapterPage(html, id, url);
      out.write(cols.map(c => escapeCsv(row[c])).join(',') + '\n');
      total++;
      if (row.email) withEmail++;
      if (row.website) withWebsite++;
      if (total % 10 === 0 || row.email) {
        console.log(`  [${total}/${chapters.length}] ${row.chapter_name} — email: ${row.email || '-'}, web: ${row.website || '-'}`);
      }
      state.doneIds.push(id);
      if (total % 5 === 0) saveState(state);
    } catch (err) {
      console.error(`  ${id} FAILED: ${err.message}`);
    }
    await new Promise(r => setTimeout(r, 900));
  }

  saveState(state);
  out.end();
  await new Promise(r => out.on('close', r));

  console.log(`\n=== DONE — ${total} chapters (${withEmail} emails, ${withWebsite} websites) ===`);
  console.log(`Wrote: ${OUTPUT}`);
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
