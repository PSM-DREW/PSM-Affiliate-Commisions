const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const dataDir = (process.env.RAILWAY_VOLUME_MOUNT_PATH || '').trim() || path.join(__dirname, 'data');
const dbPath = path.join(dataDir, 'commissions.db');
fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS affiliates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    telegram_chat_id TEXT,
    email TEXT,
    commission_rate_override REAL,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS weekly_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliate_id INTEGER NOT NULL,
    week_label TEXT NOT NULL,
    week_range TEXT NOT NULL,
    week_start TEXT NOT NULL,
    week_end TEXT NOT NULL,
    active_players INTEGER NOT NULL DEFAULT 0,
    referred_players INTEGER NOT NULL DEFAULT 0,
    net_sc REAL NOT NULL DEFAULT 0,
    sold_usd REAL NOT NULL DEFAULT 0,
    processing_fees REAL NOT NULL DEFAULT 0,
    bonuses REAL NOT NULL DEFAULT 0,
    adjustment REAL NOT NULL DEFAULT 0,
    adjustment_note TEXT,
    extra_expenses TEXT DEFAULT '[]',
    total_expenses REAL NOT NULL DEFAULT 0,
    carryover_in REAL NOT NULL DEFAULT 0,
    net REAL NOT NULL DEFAULT 0,
    payout_net REAL NOT NULL DEFAULT 0,
    commission_rate REAL NOT NULL DEFAULT 0,
    total_commission REAL NOT NULL DEFAULT 0,
    carryover_out REAL NOT NULL DEFAULT 0,
    rate_override_reason TEXT,
    status TEXT NOT NULL DEFAULT 'unpaid',
    sent_via_telegram INTEGER NOT NULL DEFAULT 0,
    telegram_sent_at TEXT,
    pdf_path TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (affiliate_id) REFERENCES affiliates(id)
  );

  CREATE TABLE IF NOT EXISTS player_weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliate_id INTEGER NOT NULL,
    week_start TEXT NOT NULL,
    player_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (affiliate_id) REFERENCES affiliates(id),
    UNIQUE(affiliate_id, week_start)
  );
`);

// Migrations for existing databases
const migrations = [
  'ALTER TABLE weekly_reports ADD COLUMN referred_players INTEGER NOT NULL DEFAULT 0',
  'ALTER TABLE weekly_reports ADD COLUMN adjustment REAL NOT NULL DEFAULT 0',
  'ALTER TABLE weekly_reports ADD COLUMN adjustment_note TEXT',
  "ALTER TABLE weekly_reports ADD COLUMN extra_expenses TEXT DEFAULT '[]'",
];
for (const sql of migrations) {
  try { db.exec(sql); } catch (e) {}
}

module.exports = db;
