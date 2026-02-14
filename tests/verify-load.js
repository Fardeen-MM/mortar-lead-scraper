#!/usr/bin/env node
const { getRegistry } = require('../lib/registry');
const registry = getRegistry();
const codes = Object.keys(registry).sort();
console.log('Total scrapers registered:', codes.length);

let errors = 0;
for (const code of codes) {
  try {
    const s = registry[code]();
    if (!s.stateCode) throw new Error('no stateCode');
  } catch (e) {
    console.error('ERROR', code, ':', e.message);
    errors++;
  }
}

console.log('Load errors:', errors);
const auCodes = codes.filter(c => c.startsWith('AU-'));
const euCodes = codes.filter(c => ['FR', 'IE', 'DE-BRAK', 'NZ', 'SG', 'HK'].includes(c));
console.log('Australian scrapers:', auCodes.length, '-', auCodes.join(', '));
console.log('EU/International:', euCodes.length, '-', euCodes.join(', '));
console.log('All loaded OK:', errors === 0);
