#!/usr/bin/env node
/**
 * Smart company name inference from email domains
 * Splits concatenated domain names into readable company names
 */
const fs = require('fs');

const INPUT = 'output/IMMIGRATION-FINAL-CLEAN.csv';

const freeProviders = new Set(['gmail','yahoo','hotmail','outlook','aol','icloud','live','msn','mail','protonmail','zoho','ymail','googlemail','fastmail','me','mac','yandex','qq','163','126','rediffmail','naver','web','gmx','libero','foxmail','bigpond','optusnet','tpg','iinet','westnet','internode','dodo','iprimus','pipex','btinternet','sky','virginmedia','talktalk','ntlworld','blueyonder','tiscali','freeserve','compuserve','comcast','cox','earthlink','verizon','att','centurylink','y7mail','rogers','shaw','telus','bell','sympatico','cogeco']);

// Split words sorted longest first
const splitWords = [
  'international','partnership','immigration','consultants','consultant','consulting',
  'associates','management','foundation','solicitors','enterprise','counseling',
  'community','australia','attorneys','solutions','barrister','migration',
  'advocates','education','solicitor','services','practice','advisory',
  'advisors','national','partners','chambers','mobility','frontier',
  'resource','counsel','project','network','justice','freedom','liberty',
  'support','protect','refugee','lawyers','society','council','welfare',
  'central','general','capital','venture','harbour','pacific','premier',
  'western','eastern','golden','bridge','global','london','family',
  'trust','legal','youth','house','cross','world','first','north','south',
  'group','human','royal','haven','swift','grace','metro','crown','green',
  'elite','crest','field','vista','point','right','voice','power',
  'care','hope','help','star','link','base','gate','zone','edge','core',
  'rise','firm','tech','info','team','plus','bond','path','nest',
  'dawn','land','town','city','home','work','life','plan','move',
  'lead','view','mind','form','visa','law','aid',
];

function smartSplit(domain) {
  if (!domain) return '';
  let name = domain.split('.')[0].toLowerCase();
  if (freeProviders.has(name)) return '';
  if (name.length < 4 || name.length > 30) return '';

  // Replace hyphens with spaces
  name = name.replace(/[-_]/g, ' ');
  if (name.includes(' ')) return titleCase(name);

  // Try splitting on known words
  for (const word of splitWords) {
    if (word.length < 3) continue;

    // Word at the end
    if (name.endsWith(word) && name.length > word.length) {
      const before = name.substring(0, name.length - word.length);
      if (before.length >= 2) return titleCase(before + ' ' + word);
    }

    // Word at the start
    if (name.startsWith(word) && name.length > word.length) {
      const after = name.substring(word.length);
      if (after.length >= 2) {
        // Try splitting the remainder too
        for (const w2 of splitWords) {
          if (after.endsWith(w2) && after.length > w2.length) {
            const mid = after.substring(0, after.length - w2.length);
            if (mid.length >= 2) return titleCase(word + ' ' + mid + ' ' + w2);
            return titleCase(word + ' ' + w2);
          }
          if (after.startsWith(w2) && after.length > w2.length) {
            const rest = after.substring(w2.length);
            if (rest.length >= 2) return titleCase(word + ' ' + w2 + ' ' + rest);
          }
        }
        return titleCase(word + ' ' + after);
      }
    }

    // Word in the middle
    const idx = name.indexOf(word);
    if (idx > 1 && idx + word.length < name.length) {
      const before = name.substring(0, idx);
      const after = name.substring(idx + word.length);
      if (before.length >= 2 && after.length >= 2) return titleCase(before + ' ' + word + ' ' + after);
      if (before.length >= 2) return titleCase(before + ' ' + word);
    }
  }

  // No split found — if short enough, title case as-is
  if (name.length <= 12) return titleCase(name);
  return '';
}

function titleCase(s) {
  return s.replace(/\b\w+/g, w => {
    if (['and','of','the','for','in','at','by','to','or','de'].includes(w.toLowerCase())) return w.toLowerCase();
    return w[0].toUpperCase() + w.slice(1).toLowerCase();
  }).replace(/\bUk\b/g, 'UK').replace(/\bUs\b/g, 'US');
}

// Test
console.log('SPLIT TESTS:');
const tests = [
  'islingtonlaw', 'sableinternational', 'freedomfromtorture', 'redcross',
  'zenithlawyers', 'horizoncare', 'fjmlaw', 'nathansonpartnership',
  'crestsolicitors', 'sabzsolicitors', 'globalimmigration', 'visaservices',
  'communitylaw', 'refugeeaid', 'justicecentre', 'migrationpartners',
  'islingtonlawcentre', 'smithimmigration', 'pacificvisa', 'goldengate',
];
tests.forEach(t => console.log('  ' + t.padEnd(25) + ' → "' + smartSplit(t + '.com') + '"'));

// Process file
const lines = fs.readFileSync(INPUT, 'utf8').split('\n');
const out = [lines[0]];
let inferred = 0;

for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const c = lines[i].split(',');
  let co = (c[3] || '').replace(/^"|"$/g, '').trim();
  const email = (c[0] || '').trim();
  const domain = email.split('@')[1];

  if (!co && domain) {
    const name = smartSplit(domain);
    if (name && name.includes(' ') && name.length >= 6) {
      co = name;
      inferred++;
    }
  }

  c[3] = co.includes(',') ? '"' + co + '"' : co;
  out.push(c.join(','));
}

fs.writeFileSync(INPUT, out.join('\n'));

let hasCo = 0;
for (let i = 1; i < out.length; i++) {
  if (out[i].split(',')[3]?.replace(/^"|"$/g, '').trim()) hasCo++;
}
console.log('\nInferred: ' + inferred);
console.log('Total with company: ' + hasCo + '/' + (out.length - 1) + ' (' + (hasCo / (out.length - 1) * 100).toFixed(0) + '%)');
