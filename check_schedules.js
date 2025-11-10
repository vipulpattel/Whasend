// Check schedules in the database
const path = require('path');
const fs = require('fs');

const dbPath = path.join(__dirname, 'Assets', 'db', 'patients.db');
console.log('Checking DB at:', dbPath);

if (!fs.existsSync(dbPath)) {
  console.error('DB file not found.');
  process.exit(1);
}

// Use Node.js built-in sqlite3 instead of better-sqlite3 to avoid version issues
const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY);

db.serialize(() => {
  console.log('\n=== SCHEDULES TABLE ===');
  db.all("SELECT * FROM schedules WHERE is_active = 1 ORDER BY days ASC", (err, rows) => {
    if (err) {
      console.error('Error querying schedules:', err.message);
    } else {
      console.log('Active Schedules:');
      rows.forEach((s, i) => {
        console.log(`${i}: ${s.name} - ${s.days} days - Template: ${s.templateName}`);
      });
      
      console.log('\nFull schedule details:');
      console.table(rows);
    }
    
    console.log('\n=== TEMPLATES TABLE ===');
    db.all("SELECT name, LENGTH(message) as msg_length FROM templates WHERE is_delete = 0", (err, templates) => {
      if (err) {
        console.error('Error querying templates:', err.message);
      } else {
        console.log('Active Templates:');
        console.table(templates);
      }
      
      db.close();
    });
  });
});