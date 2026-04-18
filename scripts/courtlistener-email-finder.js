#!/usr/bin/env node
/**
 * CourtListener Attorney Email Finder
 * 
 * Searches federal court filing records for attorney email addresses.
 * FREE API, 5000 queries/hour, no API key needed for basic access.
 * 
 * Usage: node scripts/courtlistener-email-finder.js input.csv output.csv
 */
const https = require('https');
const fs = require('fs');

function searchAttorney(name) {
  return new Promise(resolve => {
    const url = `https://www.courtlistener.com/api/rest/v3/attorneys/?name=${encodeURIComponent(name)}&format=json`;
    https.get(url, {
      headers: { 'User-Agent': 'MortarMetrics/1.0 (lead enrichment)' },
      timeout: 10000,
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          if (data.results && data.results.length > 0) {
            // Find first result with an email
            for (const att of data.results) {
              if (att.email) {
                resolve({ email: att.email, source: 'courtlistener' });
                return;
              }
              // Check contact info
              if (att.contact_raw && att.contact_raw.includes('@')) {
                const emailMatch = att.contact_raw.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
                if (emailMatch) {
                  resolve({ email: emailMatch[0], source: 'courtlistener_contact' });
                  return;
                }
              }
            }
          }
          resolve(null);
        } catch { resolve(null); }
      });
      res.on('error', () => resolve(null));
    }).on('error', () => resolve(null));
  });
}

// Quick test
(async () => {
  const testNames = ['John Smith immigration', 'Maria Garcia immigration lawyer'];
  for (const name of testNames) {
    const result = await searchAttorney(name);
    console.log(`${name}: ${result ? result.email : 'NOT FOUND'}`);
  }
})();
