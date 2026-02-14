#!/usr/bin/env node
/**
 * Debug script for the France lawyer CSV scraper.
 *
 * 1. Tries to discover the latest CSV URL via the data.gouv.fr API.
 * 2. Downloads a chunk of the CSV (first ~200KB).
 * 3. Prints raw headers, sample rows, delimiter analysis, and city column values.
 */

const https = require('https');
const http = require('http');

const DATASET_API = 'https://www.data.gouv.fr/api/1/datasets/annuaire-des-avocats-de-france/';
const HARDCODED_URL = 'https://static.data.gouv.fr/resources/annuaire-des-avocats-de-france/20260114-162250/annuaire-avocats-20260114.csv';

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        ...options.headers,
      },
      timeout: 30000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        let redirect = res.headers.location;
        if (redirect.startsWith('/')) {
          const u = new URL(url);
          redirect = `${u.protocol}//${u.host}${redirect}`;
        }
        return resolve(httpGet(redirect, options));
      }

      const chunks = [];
      let totalBytes = 0;
      const maxBytes = options.maxBytes || Infinity;

      res.on('data', (chunk) => {
        totalBytes += chunk.length;
        chunks.push(chunk);
        if (totalBytes >= maxBytes) {
          res.destroy();
        }
      });
      res.on('end', () => resolve({
        statusCode: res.statusCode,
        body: Buffer.concat(chunks),
        headers: res.headers,
      }));
      res.on('close', () => resolve({
        statusCode: res.statusCode,
        body: Buffer.concat(chunks),
        headers: res.headers,
      }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

async function discoverCsvUrl() {
  console.log('\n=== STEP 1: Discover latest CSV URL from data.gouv.fr API ===\n');
  try {
    const resp = await httpGet(DATASET_API);
    const data = JSON.parse(resp.body.toString('utf-8'));

    console.log(`Dataset title: ${data.title}`);
    console.log(`Number of resources: ${data.resources?.length || 0}`);

    const csvResources = (data.resources || []).filter(r =>
      r.format === 'csv' || (r.url && r.url.endsWith('.csv')) || (r.title && r.title.toLowerCase().includes('csv'))
    );

    console.log(`\nCSV resources found: ${csvResources.length}`);
    for (const r of csvResources) {
      console.log(`  - Title: ${r.title}`);
      console.log(`    URL: ${r.url}`);
      console.log(`    Format: ${r.format}`);
      console.log(`    Size: ${r.filesize ? (r.filesize / 1024 / 1024).toFixed(1) + ' MB' : 'unknown'}`);
      console.log(`    Last modified: ${r.last_modified}`);
      console.log('');
    }

    // Return the first (latest) CSV URL
    if (csvResources.length > 0) {
      return csvResources[0].url;
    }

    // If no CSV found, list all resources
    console.log('\nAll resources:');
    for (const r of data.resources || []) {
      console.log(`  - [${r.format}] ${r.title}: ${r.url}`);
    }

    return null;
  } catch (err) {
    console.error(`API request failed: ${err.message}`);
    return null;
  }
}

async function fetchAndAnalyzeCsv(url) {
  console.log(`\n=== STEP 2: Fetch CSV from ${url} ===\n`);

  try {
    // Download ~200KB
    const resp = await httpGet(url, { maxBytes: 200 * 1024 });
    console.log(`Status: ${resp.statusCode}`);
    console.log(`Content-Type: ${resp.headers['content-type']}`);
    console.log(`Downloaded: ${(resp.body.length / 1024).toFixed(1)} KB`);

    // Try multiple encodings
    const encodings = ['utf-8', 'latin1'];
    for (const enc of encodings) {
      const text = resp.body.toString(enc);
      const lines = text.split('\n').filter(l => l.trim());

      console.log(`\n--- Decoding as ${enc} ---`);
      console.log(`Lines found: ${lines.length}`);

      if (lines.length === 0) {
        console.log('No lines found with this encoding');
        continue;
      }

      // Raw first line (headers)
      console.log(`\nRaw header line (first 500 chars):`);
      console.log(lines[0].substring(0, 500));

      // Detect delimiter
      const headerLine = lines[0];
      const semicolonCount = (headerLine.match(/;/g) || []).length;
      const commaCount = (headerLine.match(/,/g) || []).length;
      const tabCount = (headerLine.match(/\t/g) || []).length;
      const pipeCount = (headerLine.match(/\|/g) || []).length;

      console.log(`\nDelimiter analysis (in header):`);
      console.log(`  Semicolons: ${semicolonCount}`);
      console.log(`  Commas: ${commaCount}`);
      console.log(`  Tabs: ${tabCount}`);
      console.log(`  Pipes: ${pipeCount}`);

      // Determine likely delimiter
      const maxDelim = Math.max(semicolonCount, commaCount, tabCount, pipeCount);
      let delimiter = ';';
      if (maxDelim === commaCount) delimiter = ',';
      else if (maxDelim === tabCount) delimiter = '\t';
      else if (maxDelim === pipeCount) delimiter = '|';

      console.log(`  Likely delimiter: "${delimiter === '\t' ? 'TAB' : delimiter}"`);

      // Split headers
      const headers = headerLine.split(delimiter).map(h => h.trim().replace(/^"/, '').replace(/"$/, ''));
      console.log(`\nHeaders (${headers.length} columns):`);
      headers.forEach((h, i) => {
        console.log(`  [${i}] "${h}"`);
      });

      // Print sample rows
      console.log(`\nSample rows (up to 10):`);
      const sampleRows = lines.slice(1, 11);
      for (let i = 0; i < sampleRows.length; i++) {
        const cols = sampleRows[i].split(delimiter).map(c => c.trim().replace(/^"/, '').replace(/"$/, ''));
        console.log(`\n  Row ${i + 1}:`);
        cols.forEach((val, j) => {
          const headerName = headers[j] || `col_${j}`;
          console.log(`    ${headerName}: "${val}"`);
        });
      }

      // Look for city-like columns
      console.log(`\n\nCity-related column analysis:`);
      const cityColIndices = headers.reduce((acc, h, i) => {
        if (/ville|city|commune|localite|localité/i.test(h)) {
          acc.push(i);
        }
        return acc;
      }, []);

      if (cityColIndices.length === 0) {
        console.log('  No column found with city-like name!');
        console.log('  Looking for columns containing "Paris" or "PARIS" in data...');

        // Check all columns for Paris mentions
        const dataLines = lines.slice(1, Math.min(lines.length, 200));
        for (let colIdx = 0; colIdx < headers.length; colIdx++) {
          let parisCount = 0;
          for (const line of dataLines) {
            const cols = line.split(delimiter);
            if (cols[colIdx] && /paris/i.test(cols[colIdx])) {
              parisCount++;
            }
          }
          if (parisCount > 0) {
            console.log(`  Column [${colIdx}] "${headers[colIdx]}" has ${parisCount} rows containing "Paris"`);
          }
        }
      } else {
        for (const idx of cityColIndices) {
          console.log(`\n  Column [${idx}] "${headers[idx]}" — sample unique values:`);
          const dataLines = lines.slice(1, Math.min(lines.length, 200));
          const uniqueVals = new Set();
          for (const line of dataLines) {
            const cols = line.split(delimiter);
            if (cols[idx]) {
              uniqueVals.add(cols[idx].trim().replace(/^"/, '').replace(/"$/, ''));
            }
          }
          const sorted = [...uniqueVals].sort();
          // Show first 30 unique values
          sorted.slice(0, 30).forEach(v => console.log(`    "${v}"`));
          if (sorted.length > 30) console.log(`    ... and ${sorted.length - 30} more`);

          // Check for target cities
          const targets = ['paris', 'lyon', 'marseille', 'toulouse', 'bordeaux', 'nantes', 'strasbourg', 'lille'];
          console.log(`\n  Target city matches in "${headers[idx]}":`);
          for (const city of targets) {
            const matches = sorted.filter(v => v.toLowerCase().includes(city));
            if (matches.length > 0) {
              console.log(`    "${city}" matched by: ${matches.join(', ')}`);
            } else {
              console.log(`    "${city}" — NO MATCH`);
            }
          }
        }
      }

      // BOM detection
      if (resp.body[0] === 0xEF && resp.body[1] === 0xBB && resp.body[2] === 0xBF) {
        console.log('\nBOM detected: UTF-8 BOM');
      } else if (resp.body[0] === 0xFF && resp.body[1] === 0xFE) {
        console.log('\nBOM detected: UTF-16 LE BOM');
      } else {
        console.log(`\nNo BOM detected. First 3 bytes: 0x${resp.body[0]?.toString(16)} 0x${resp.body[1]?.toString(16)} 0x${resp.body[2]?.toString(16)}`);
      }

      // Only analyze with the first encoding that has reasonable results
      if (lines.length > 5 && semicolonCount >= 3) {
        break;
      }
    }
  } catch (err) {
    console.error(`CSV fetch failed: ${err.message}`);
  }
}

async function main() {
  console.log('=== France Lawyer CSV Debug Tool ===');

  // Step 1: Discover URL from API
  const apiUrl = await discoverCsvUrl();

  // Step 2: Try the API-discovered URL
  if (apiUrl) {
    console.log(`\nUsing API-discovered URL: ${apiUrl}`);
    await fetchAndAnalyzeCsv(apiUrl);
  }

  // Step 3: Also try the hardcoded URL if different
  if (!apiUrl || apiUrl !== HARDCODED_URL) {
    console.log(`\n\n=== Also trying hardcoded URL: ${HARDCODED_URL} ===`);
    await fetchAndAnalyzeCsv(HARDCODED_URL);
  }
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
