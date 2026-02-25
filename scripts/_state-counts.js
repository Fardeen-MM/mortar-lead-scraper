const Database = require('better-sqlite3');
const db = new Database('./data/leads.db', { readonly: true });
const states = db.prepare("SELECT state, COUNT(*) as c FROM leads GROUP BY state ORDER BY c DESC").all();
const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
console.log(`Total: ${total}`);
states.forEach(r => console.log(`  ${r.state.padEnd(10)} ${r.c}`));
db.close();
