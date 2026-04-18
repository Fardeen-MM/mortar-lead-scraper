#!/usr/bin/env node
/**
 * Netherlands SVMA Immigration Lawyer Scraper
 *
 * ~156 firms across 16 pages. Emails on profile pages.
 * List: https://www.svma.nl/voor-clienten/zoek-een-advocaat/page/{N}
 * Profiles: https://www.svma.nl/voor-clienten/zoek-een-advocaat/{slug}
 */

const https = require('https');
const fs = require('fs');

const BASE = 'https://www.svma.nl';
const LIST_URL = '/voor-clienten/zoek-een-advocaat';
const OUTPUT = 'output/netherlands-svma-lawyers.csv';
const DELAY = 1500;
const PAGES = 16;

function fetch(url) {
  const fullUrl = url.startsWith('http') ? url : BASE + url;
  return new Promise((resolve, reject) => {
    https.get(fullUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9,nl;q=0.8',
      },
      timeout: 20000,
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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

(async () => {
  console.log('Scraping Netherlands SVMA immigration lawyers...\n');

  // Phase 1: Collect all firm profile URLs from listing pages
  const firms = [];

  for (let page = 1; page <= PAGES; page++) {
    const url = page === 1 ? LIST_URL : `${LIST_URL}/page/${page}`;
    try {
      const html = await fetch(url);

      // Extract firm cards with profile links and member names
      const links = [...html.matchAll(/<h4[^>]*>[\s\S]*?<a[^>]*href="([^"]*zoek-een-advocaat\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)];

      for (const m of links) {
        const profileUrl = m[1];
        const firmName = m[2].replace(/<[^>]+>/g, '').trim();

        // Skip pagination links
        if (profileUrl.includes('/page/')) continue;

        // Extract member names from the card
        const cardStart = html.indexOf(m[0]);
        const cardEnd = html.indexOf('</article>', cardStart);
        const cardHtml = cardEnd > cardStart ? html.substring(cardStart, cardEnd) : '';

        const members = [...cardHtml.matchAll(/organisation-member-name[^>]*>([\s\S]*?)<\//gi)]
          .map(n => n[1].replace(/<[^>]+>/g, '').trim())
          .filter(n => n.length > 2);

        firms.push({ profileUrl, firmName, members });
      }

      console.log(`  Page ${page}/${PAGES}: found ${links.length} firms (total: ${firms.length})`);
    } catch (e) {
      console.log(`  Page ${page} error: ${e.message}`);
    }

    await sleep(DELAY);
  }

  console.log(`\n  Total firms: ${firms.length}. Fetching profiles for emails...\n`);

  // Phase 2: Fetch each profile page for email, phone, website, address
  const leads = [];

  for (let i = 0; i < firms.length; i++) {
    const firm = firms[i];
    try {
      const html = await fetch(firm.profileUrl);

      // Extract email
      const emailMatch = html.match(/mailto:([^"'\s]+)/i);
      const email = emailMatch ? emailMatch[1].toLowerCase().trim() : '';

      // Extract phone
      const phoneMatch = html.match(/tel:([^"'\s]+)/i);
      let phone = phoneMatch ? phoneMatch[1].replace(/%20/g, ' ') : '';
      if (!phone) {
        const phoneTextMatch = html.match(/(\+31[\s\d\-/.]{8,})/);
        if (phoneTextMatch) phone = phoneTextMatch[1].trim();
      }

      // Extract website
      const websiteMatch = html.match(/<a[^>]*href="(https?:\/\/(?!www\.svma\.nl)[^"]+)"[^>]*target="_blank"/i);
      const website = websiteMatch ? websiteMatch[1] : '';

      // Extract address
      const addrMatch = html.match(/(\d{4}\s*[A-Z]{2})\s*([\w\s]+?)(?:<|$)/i);
      const city = addrMatch ? addrMatch[2].trim() : '';

      // Create leads for each member, or one lead per firm if no members listed
      const members = firm.members.length > 0 ? firm.members : [firm.firmName];

      for (const member of members) {
        // Parse name - Dutch names often have "mr." prefix
        let cleanName = member.replace(/^(mr\.|mrs\.|ms\.|dr\.|prof\.)\s*/gi, '').trim();
        const nameParts = cleanName.split(/\s+/);

        let firstName = '', lastName = '';
        if (nameParts.length >= 2) {
          // Handle initials like "L.J."
          let firstIdx = 0;
          while (firstIdx < nameParts.length - 1 && /^[A-Z]\.?$/.test(nameParts[firstIdx])) {
            firstIdx++;
          }
          if (firstIdx === nameParts.length - 1) {
            // All but last are initials
            firstName = nameParts.slice(0, firstIdx).join(' ');
            lastName = nameParts[firstIdx];
          } else {
            firstName = nameParts[0];
            lastName = nameParts.slice(1).join(' ');
          }
        } else {
          firstName = cleanName;
        }

        leads.push({
          email,
          first_name: firstName,
          last_name: lastName,
          company: firm.firmName,
          phone,
          city,
          state: '',
          country: 'Netherlands',
          source: 'netherlands_svma',
        });
      }

      if ((i + 1) % 20 === 0 || i < 3) {
        console.log(`  [${i + 1}/${firms.length}] ${leads.length} leads (${email ? 'email' : 'no email'})`);
      }
    } catch (e) {
      console.log(`  Profile ${i + 1} error: ${e.message}`);
    }

    await sleep(DELAY);
  }

  // Write CSV
  const header = 'email,first_name,last_name,company,phone,city,state,country,source';
  const rows = leads.map(l => [
    esc(l.email), esc(l.first_name), esc(l.last_name), esc(l.company),
    esc(l.phone), esc(l.city), esc(l.state), esc(l.country), esc(l.source),
  ].join(','));

  fs.writeFileSync(OUTPUT, header + '\n' + rows.join('\n'));

  const withEmail = leads.filter(l => l.email).length;
  const withPhone = leads.filter(l => l.phone).length;

  console.log(`\n✓ Done: ${leads.length} Netherlands SVMA immigration lawyers`);
  console.log(`  With email: ${withEmail} (${leads.length ? (withEmail/leads.length*100).toFixed(1) : 0}%)`);
  console.log(`  With phone: ${withPhone} (${leads.length ? (withPhone/leads.length*100).toFixed(1) : 0}%)`);
  console.log(`  → ${OUTPUT}`);
})();
