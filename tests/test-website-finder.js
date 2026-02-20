const { generateDomainGuesses, findByDomainGuessing } = require('../lib/website-finder');

async function test() {
  // Test domain guessing patterns
  console.log('=== Domain Pattern Generation ===');
  const guesses = generateDomainGuesses('Smith & Jones LLP', 'Houston');
  console.log('Smith & Jones LLP:');
  guesses.forEach(g => console.log('  ' + g));

  const guesses2 = generateDomainGuesses('Morgan Lewis', 'Philadelphia');
  console.log('\nMorgan Lewis:');
  guesses2.forEach(g => console.log('  ' + g));

  // Test actual DNS resolution
  console.log('\n=== DNS Resolution ===');
  const website = await findByDomainGuessing('Morgan Lewis', 'Philadelphia');
  console.log('Morgan Lewis:', website || '(not found)');

  const website2 = await findByDomainGuessing('Baker McKenzie', 'Chicago');
  console.log('Baker McKenzie:', website2 || '(not found)');

  const website3 = await findByDomainGuessing('Jones Day', 'Cleveland');
  console.log('Jones Day:', website3 || '(not found)');

  console.log('\nWebsite finder loaded OK');
}

test().catch(console.error);
