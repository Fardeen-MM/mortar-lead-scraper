#!/usr/bin/env node
/**
 * Belgium Advocaat.be Immigration Lawyer Scraper
 *
 * Single HTTP request to print page gives ALL 209 immigration lawyers with emails.
 * URL: https://www.advocaat.be/en/search-lawyer?theme=immigration-law&print=true
 */

const https = require('https');
const fs = require('fs');

const URL = 'https://www.advocaat.be/en/search-lawyer?theme=immigration-law&print=true';
const OUTPUT = 'output/belgium-immigration-lawyers.csv';

function fetch(url) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 30000,
    }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve).catch(reject);
      }
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => resolve(body));
      res.on('error', reject);
    }).on('error', reject);
  });
}

function esc(v) {
  const s = (v ?? '').toString().trim().replace(/"/g, '""');
  return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
}

function titleCase(s) {
  return s.replace(/\b\w+/g, w => {
    if (['de', 'van', 'den', 'der', 'le', 'la', 'du', 'des', 'et'].includes(w.toLowerCase())) return w.toLowerCase();
    return w[0].toUpperCase() + w.slice(1).toLowerCase();
  });
}

(async () => {
  console.log('Fetching Belgium advocaat.be immigration lawyers (print page)...\n');

  const html = await fetch(URL);
  console.log(`  Got ${(html.length / 1024).toFixed(0)}KB of HTML`);

  const leads = [];

  // Parse cards - each lawyer is in a card div
  // Name is in h3 > a tags
  // Email and phone are in hidden print:block divs
  // Address is in <address> tags

  // Strategy: split by card boundaries and extract per card
  const cardPattern = /<div[^>]*class="[^"]*card[^"]*bg-primary[^"]*"[^>]*>([\s\S]*?)(?=<div[^>]*class="[^"]*card[^"]*bg-primary|$)/gi;

  let match;
  while ((match = cardPattern.exec(html)) !== null) {
    const card = match[1];

    // Extract name from h3 > a
    const nameMatch = card.match(/<h3[^>]*>[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
    const fullName = nameMatch ? nameMatch[1].replace(/<[^>]+>/g, '').trim() : '';

    if (!fullName) continue;

    // Extract email - look for @ in hidden divs or any mailto
    let email = '';
    const mailtoMatch = card.match(/mailto:([^"'\s]+)/i);
    if (mailtoMatch) {
      email = mailtoMatch[1];
    } else {
      // Look for email pattern in hidden print divs
      const hiddenDivs = card.match(/<div[^>]*class="[^"]*hidden[^"]*print[^"]*"[^>]*>([\s\S]*?)<\/div>/gi) || [];
      for (const div of hiddenDivs) {
        const content = div.replace(/<[^>]+>/g, '').trim();
        if (content.includes('@') && content.includes('.')) {
          email = content;
          break;
        }
      }
    }
    // Also try plain text email patterns in the card
    if (!email) {
      const emailPattern = card.match(/[\w.+-]+@[\w.-]+\.\w{2,}/);
      if (emailPattern) email = emailPattern[0];
    }

    // Extract phone
    let phone = '';
    const telMatch = card.match(/tel:([^"'\s]+)/i);
    if (telMatch) {
      phone = telMatch[1].replace(/%20/g, ' ');
    } else {
      const hiddenDivs = card.match(/<div[^>]*class="[^"]*hidden[^"]*print[^"]*"[^>]*>([\s\S]*?)<\/div>/gi) || [];
      for (const div of hiddenDivs) {
        const content = div.replace(/<[^>]+>/g, '').trim();
        if (/^\+?\d[\d\s\-/.]{6,}$/.test(content)) {
          phone = content;
          break;
        }
      }
    }

    // Extract address
    const addrMatch = card.match(/<address[^>]*>([\s\S]*?)<\/address>/i);
    let city = '', address = '';
    if (addrMatch) {
      const addrText = addrMatch[1].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      address = addrText;
      // Belgian postal codes are 4 digits
      const cityMatch = addrText.match(/\d{4}\s+(.+?)$/);
      if (cityMatch) city = cityMatch[1].trim();
    }

    // Extract practice areas
    const practiceMatch = card.match(/overflow-ellipsis[^>]*>([\s\S]*?)<\/div>/i);
    const practiceAreas = practiceMatch ? practiceMatch[1].replace(/<[^>]+>/g, '').trim() : '';

    // Split name
    let firstName = '', lastName = '';
    const nameParts = fullName.split(/\s+/);
    if (nameParts.length >= 2) {
      firstName = nameParts[0];
      lastName = nameParts.slice(1).join(' ');
    } else {
      firstName = fullName;
    }

    leads.push({
      email: email.toLowerCase().trim(),
      first_name: firstName,
      last_name: lastName,
      company: '',
      phone: phone,
      city: city,
      state: '',
      country: 'Belgium',
      source: 'belgium_advocaat',
    });
  }

  // If card regex didn't work well, try alternative parsing
  if (leads.length < 10) {
    console.log('  Card regex got few results, trying alternative parsing...');

    // Find all email addresses
    const emails = [...html.matchAll(/[\w.+-]+@[\w.-]+\.\w{2,}/g)].map(m => m[0]);
    // Find all names near lawyer links
    const names = [...html.matchAll(/<a[^>]*search-a-lawyer\/lawyer[^>]*>([\s\S]*?)<\/a>/gi)]
      .map(m => m[1].replace(/<[^>]+>/g, '').trim())
      .filter(n => n.length > 2);

    console.log(`  Found ${emails.length} emails, ${names.length} names via alt parsing`);

    // If we have more from alt parsing, use those
    if (names.length > leads.length) {
      leads.length = 0;
      const seenEmails = new Set();

      for (let i = 0; i < names.length; i++) {
        const name = names[i];
        const nameParts = name.split(/\s+/);
        let firstName = nameParts[0] || '';
        let lastName = nameParts.slice(1).join(' ') || '';

        // Try to match with nearby email
        const email = emails[i] || '';
        if (email && seenEmails.has(email.toLowerCase())) continue;
        if (email) seenEmails.add(email.toLowerCase());

        leads.push({
          email: (email || '').toLowerCase().trim(),
          first_name: firstName,
          last_name: lastName,
          company: '',
          phone: '',
          city: '',
          state: '',
          country: 'Belgium',
          source: 'belgium_advocaat',
        });
      }
    }
  }

  console.log(`\n  Parsed ${leads.length} lawyers`);

  // Write CSV
  const header = 'email,first_name,last_name,company,phone,city,state,country,source';
  const rows = leads.map(l => [
    esc(l.email), esc(l.first_name), esc(l.last_name), esc(l.company),
    esc(l.phone), esc(l.city), esc(l.state), esc(l.country), esc(l.source),
  ].join(','));

  fs.writeFileSync(OUTPUT, header + '\n' + rows.join('\n'));

  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;

  console.log(`\n✓ Done: ${leads.length} Belgium immigration lawyers`);
  console.log(`  With email: ${withEmail} (${leads.length ? (withEmail/leads.length*100).toFixed(1) : 0}%)`);
  console.log(`  With phone: ${withPhone} (${leads.length ? (withPhone/leads.length*100).toFixed(1) : 0}%)`);
  console.log(`  → ${OUTPUT}`);
})();
