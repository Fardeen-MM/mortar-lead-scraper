#!/usr/bin/env node
/**
 * Direct waterfall test — scrape a few CA leads and run waterfall on them.
 */
const { getRegistry } = require('../lib/registry');
const { runWaterfall } = require('../lib/waterfall');

async function main() {
  const SCRAPERS = getRegistry();
  const scraper = SCRAPERS['CA']();

  console.log('Scraping 1 page of CA (Beverly Hills)...');
  const leads = [];
  let count = 0;

  for await (const result of scraper.search('', { city: 'Beverly Hills', maxPages: 1 })) {
    if (result._cityProgress || result._captcha) continue;
    leads.push({
      first_name: result.first_name || '',
      last_name: result.last_name || '',
      firm_name: result.firm_name || '',
      city: result.city || '',
      state: result.state || 'CA',
      phone: result.phone || '',
      website: result.website || '',
      email: result.email || '',
      bar_number: result.bar_number || '',
      source: result.source || '',
      profile_url: result.profile_url || '',
      email_source: '',
      phone_source: '',
      website_source: '',
    });
    count++;
    if (count >= 10) break; // Only 10 leads for quick test
  }

  console.log(`Got ${leads.length} leads`);
  console.log('Profile URLs:', leads.filter(l => l.profile_url).length);
  console.log('Sample profile_url:', leads[0]?.profile_url);
  console.log('Needs enrichment (no email/phone/website):', leads.filter(l => !l.email || !l.phone || !l.website).length);

  console.log('\nRunning waterfall (profiles only)...');
  const stats = await runWaterfall(leads, {
    fetchProfiles: true,
    crossRefMartindale: false,
    crossRefLawyersCom: false,
    nameLookups: false,
    emailCrawl: false,
  });

  console.log('\nWaterfall stats:', JSON.stringify(stats, null, 2));
  console.log('\nLead sample after waterfall:');
  for (const l of leads.slice(0, 3)) {
    console.log(`  ${l.first_name} ${l.last_name}: phone=${l.phone || '(none)'} email=${l.email || '(none)'} website=${l.website || '(none)'} phone_src=${l.phone_source} email_src=${l.email_source}`);
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
