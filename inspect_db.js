// Simple inspector for the app's runtime SQLite DB
// Usage: node inspect_db.js "C:\Users\Admin\AppData\Roaming\whasend\patients.db"
// If no path provided, defaults to userData patients.db location commonly used by the app.

const path = require('path');
const fs = require('fs');

const Database = require('better-sqlite3');

const arg = process.argv[2];
let dbPath = arg;
if (!dbPath) {
  const os = require('os');
  const user = os.userInfo().username;
  // default guess (Windows AppData Roaming)
  dbPath = path.join(process.env.APPDATA || path.join('C:', 'Users', user, 'AppData', 'Roaming'), 'whasend', 'patients.db');
}

console.log('Inspecting DB at:', dbPath);
if (!fs.existsSync(dbPath)) {
  console.error('DB file not found. Provide full path as first arg or ensure the default path exists.');
  process.exit(2);
}

let db;
try {
  db = new Database(dbPath, { readonly: true });
} catch (e) {
  console.error('Failed to open DB:', e && e.message ? e.message : e);
  process.exit(3);
}

try {
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all().map(r => r.name);
  console.log('Tables:', tables.join(', '));

  if (tables.includes('message_logs')) {
    const cnt = db.prepare('SELECT COUNT(*) as cnt FROM message_logs').get();
    console.log('message_logs row count:', cnt ? cnt.cnt : 0);

    const sample = db.prepare('SELECT id, job_id, unique_id, name, phone, profile, template, status, sent_at, error FROM message_logs ORDER BY sent_at DESC LIMIT 20').all();
    console.log('Recent message_logs rows (up to 20):');
    console.table(sample);
  } else {
    console.log('message_logs table not found in this DB.');
  }

  // show last few updates to patients table for reference
  if (tables.includes('patients')) {
    const p = db.prepare('SELECT unique_id, name, phone, profile, Last_Msgsent_date, last_template FROM patients ORDER BY mod_date DESC LIMIT 10').all();
    console.log('Recent patients (up to 10):');
    console.table(p);
  }
} catch (e) {
  console.error('Query failed:', e && e.message ? e.message : e);
} finally {
  try { db.close(); } catch (e) {}
}

process.exit(0);
