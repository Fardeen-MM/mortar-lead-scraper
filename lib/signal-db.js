/**
 * Signal DB — SQLite storage for job board signals with dedup
 */

const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.SIGNAL_DB_PATH || path.join(__dirname, '..', 'data', 'signals.db');

let _db = null;

function getDb() {
  if (_db) return _db;

  _db = new Database(DB_PATH);
  _db.pragma('journal_mode = WAL');

  _db.exec(`
    CREATE TABLE IF NOT EXISTS signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      firm_name TEXT NOT NULL,
      job_title TEXT NOT NULL,
      city TEXT,
      state TEXT,
      job_url TEXT,
      description TEXT,
      source TEXT DEFAULT 'indeed',
      detected_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_firm_job ON signals(firm_name, job_title);
  `);

  return _db;
}

/**
 * Try to insert a signal. Returns true if new (inserted), false if duplicate.
 */
function insertSignal({ firm_name, job_title, city, state, job_url, description }) {
  const db = getDb();
  const result = db.prepare(`
    INSERT OR IGNORE INTO signals (firm_name, job_title, city, state, job_url, description)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(firm_name, job_title, city, state, job_url, description);
  return result.changes > 0;
}

/**
 * Get recent signals.
 */
function getRecent(limit = 50) {
  const db = getDb();
  return db.prepare('SELECT * FROM signals ORDER BY detected_at DESC LIMIT ?').all(limit);
}

/**
 * Get total signal count.
 */
function getCount() {
  const db = getDb();
  return db.prepare('SELECT COUNT(*) as count FROM signals').get().count;
}

module.exports = { insertSignal, getRecent, getCount };
