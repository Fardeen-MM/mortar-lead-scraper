/**
 * Google Ads Transparency Checker
 *
 * Uses the free Python Google-Ads-Transparency-Scraper to check
 * if businesses run Google Ads. Calls Python via child_process.
 *
 * Zero cost. Searches Google Ads Transparency Center for each business.
 * Returns: advertiser name, ad count, advertiser ID.
 *
 * Usage: node scripts/google-ads-checker.js input.csv [output.csv] [concurrency]
 */

const fs = require('fs');
const { execSync } = require('child_process');
const path = require('path');

const CONCURRENCY = parseInt(process.argv[4] || '5', 10); // Lower concurrency — Python subprocess is heavier

// Python script that checks one business
const PY_SCRIPT = `
import sys, json
sys.path.insert(0, '/Users/fardeenchoudhury/Library/Python/3.9/lib/python/site-packages')
from GoogleAds import GoogleAds

name = sys.argv[1]
scraper = GoogleAds(region='US')

try:
    suggestions = scraper.get_all_search_suggestions(name)
    if suggestions:
        top = suggestions[0].get('1', {})
        result = {
            'found': True,
            'advertiser_name': top.get('1', ''),
            'advertiser_id': top.get('2', ''),
            'country': top.get('3', ''),
            'ad_count': int(top.get('4', {}).get('2', {}).get('1', 0)),
        }
    else:
        result = {'found': False, 'ad_count': 0}
except Exception as e:
    result = {'found': False, 'error': str(e)[:100], 'ad_count': 0}

print(json.dumps(result))
`;

fs.writeFileSync('/tmp/google-ads-check.py', PY_SCRIPT);

function parseLine(line) {
  const cols = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (c === ',' && !inQ) { cols.push(cur.trim()); cur = ''; continue; }
    cur += c;
  }
  cols.push(cur.trim());
  return cols;
}

function esc(v) {
  const s = (v ?? '').toString().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

function checkOne(businessName) {
  try {
    const result = execSync(`python3 /tmp/google-ads-check.py "${businessName.replace(/"/g, '\\"')}"`, {
      timeout: 15000,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    // Find the JSON line in output (skip warnings)
    const lines = result.split('\n');
    for (const line of lines) {
      if (line.startsWith('{')) return JSON.parse(line);
    }
    return { found: false, ad_count: 0 };
  } catch (err) {
    return { found: false, ad_count: 0, error: 'timeout' };
  }
}

(async () => {
  const inputFile = process.argv[2];
  const outputFile = process.argv[3] || inputFile?.replace('.csv', '-google-ads.csv');
  const maxLeads = parseInt(process.argv[5] || '0', 10) || Infinity;

  if (!inputFile) {
    console.log('Google Ads Transparency Checker (FREE — uses Python scraper)');
    console.log('Usage: node scripts/google-ads-checker.js input.csv [output.csv] [concurrency] [max_leads]');
    process.exit(1);
  }

  const csvText = fs.readFileSync(inputFile, 'utf8');
  const lines = csvText.split('\n').filter(l => l.trim());
  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map(l => {
    const cols = parseLine(l);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ''; });
    return obj;
  });

  const nameCol = headers.find(h => /business.?name|firm.?name|company|^name$|charity_name/i.test(h)) || headers[0];
  const firstNameCol = headers.find(h => /^first.?name$/i.test(h));
  const lastNameCol = headers.find(h => /^last.?name$/i.test(h));
  const webCol = headers.find(h => /^website$/i.test(h) || /^url$/i.test(h) || /^domain$/i.test(h));
  const total = Math.min(rows.length, maxLeads);

  console.log(`\n  Google Ads Transparency Checker (FREE)`);
  console.log(`  Input: ${total} leads | Name column: "${nameCol}"`);
  console.log(`  Output: ${path.basename(outputFile)}\n`);

  const results = [];
  let checked = 0, found = 0;
  const t0 = Date.now();

  for (let i = 0; i < total; i++) {
    const row = rows[i];
    let name = (row[nameCol] || '').trim();

    // Handle facebook.com/slug URLs — extract the slug as the name
    if (name.includes('facebook.com/')) {
      const slug = name.replace(/^.*facebook\.com\//i, '').replace(/\/$/, '').trim();
      if (slug && slug.length > 2) {
        name = slug.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ').trim();
      } else {
        name = '';
      }
    }
    // Handle other URLs/domains
    else if (name.includes('http') || /\.\w{2,4}$/.test(name)) {
      name = name.replace(/^https?:\/\/(www\.)?/i, '').replace(/\/.*$/, '').replace(/\.\w+$/, '');
      name = name.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ').trim();
    }

    // If name is empty or too short, try first+last name
    if ((!name || name.length < 3) && firstNameCol && lastNameCol) {
      const fn = (row[firstNameCol] || '').trim();
      const ln = (row[lastNameCol] || '').trim();
      if (fn && ln) name = fn + ' ' + ln;
    }

    // If still empty, try extracting from website domain
    if (!name && webCol) {
      const web = (row[webCol] || '').replace(/^https?:\/\/(www\.)?/i, '').replace(/\/.*$/, '').replace(/\.\w+$/, '');
      if (web.length > 2) {
        name = web.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ').trim();
      }
    }

    if (!name) {
      results.push({ ...row, google_ads_active: '', google_ad_count: '', google_advertiser: '', google_check_error: 'no_name' });
      continue;
    }

    const result = checkOne(name);
    checked++;

    // Validate: does the returned advertiser name actually match our search?
    // Must be strict to avoid false positives like "Cozen" matching "Paul Cozens"
    let isValidMatch = false;
    if (result.found && result.ad_count > 0 && result.advertiser_name) {
      const searchNorm = name.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
      const advNorm = result.advertiser_name.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();

      // Exact match
      if (searchNorm === advNorm) {
        isValidMatch = true;
      }
      // One contains the other as whole words (word boundary check)
      else if (new RegExp('\\b' + searchNorm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(advNorm) ||
               new RegExp('\\b' + advNorm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(searchNorm)) {
        isValidMatch = true;
      }
      // Word-level match: require EXACT word matches (not substrings)
      else {
        const sWords = searchNorm.split(' ').filter(w => w.length > 2);
        const aWords = new Set(advNorm.split(' '));
        // Each search word must appear as an EXACT word in advertiser name
        const matches = sWords.filter(w => aWords.has(w)).length;
        isValidMatch = sWords.length > 0 && matches / sWords.length >= 0.5;
      }
    }

    if (isValidMatch) found++;

    results.push({
      ...row,
      google_ads_active: isValidMatch ? 'YES' : 'NO',
      google_ad_count: isValidMatch ? (result.ad_count || '') : '',
      google_advertiser: result.advertiser_name || '',
      google_advertiser_id: isValidMatch ? (result.advertiser_id || '') : '',
      google_check_error: result.error || (result.found && !isValidMatch ? 'name_mismatch' : ''),
    });

    if (checked % 5 === 0 || checked <= 3) {
      const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
      const rate = (checked / ((Date.now() - t0) / 1000)).toFixed(2);
      console.log(`  [${elapsed}s] ${checked}/${total} | 🔥${found} with Google Ads | ${rate}/sec`);
    }
  }

  // Write output
  const addCols = ['google_ads_active', 'google_ad_count', 'google_advertiser', 'google_advertiser_id', 'google_check_error'];
  const outHeaders = [...headers, ...addCols];
  const outRows = results.map(r => outHeaders.map(h => esc(r[h])).join(','));
  fs.writeFileSync(outputFile, outHeaders.join(',') + '\n' + outRows.join('\n'));

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n  ✓ Done: ${checked} checked, ${found} with Google Ads, ${elapsed}s`);
  console.log(`  → ${outputFile}`);
})();
