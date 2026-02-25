const Database = require('better-sqlite3');
const db = new Database('./data/leads.db', { readonly: true });
const total = db.prepare("SELECT COUNT(*) as c FROM leads").get().c;
const withEmail = db.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
const recent = db.prepare("SELECT COUNT(*) as c FROM leads WHERE created_at > datetime('now', '-1 hour')").get().c;
const byState = db.prepare("SELECT state, COUNT(*) as c FROM leads WHERE created_at > datetime('now', '-1 hour') GROUP BY state ORDER BY c DESC LIMIT 10").all();
console.log(`Total: ${total} | Email: ${withEmail} | New (last hour): ${recent}`);
if (byState.length > 0) console.log('New by state:', byState.map(r => `${r.state}(${r.c})`).join(', '));
db.close();
