const fs = require('fs');
const path = require('path');
const db = require('./db');

const csvFile = process.argv[2];
if (!csvFile) {
  console.error('Usage: node import.js <csv-file>');
  process.exit(1);
}

const raw = fs.readFileSync(csvFile, 'utf8');
const lines = raw.trim().split('\n');
const header = lines[0];

// Parse CSV line handling quoted fields with commas
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      fields.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  fields.push(current.trim());
  return fields;
}

function parseMoney(str) {
  if (!str) return 0;
  return parseFloat(str.replace(/[$,]/g, '')) || 0;
}

function parseRate(str) {
  if (!str) return 0;
  return parseFloat(str.replace('%', '')) / 100;
}

// Parse week range into dates
// "March 16- 22 Week 3" → week_start=2026-03-16, week_end=2026-03-22
function parseWeek(weekStr) {
  // Normalize: replace en-dash/em-dash with hyphen, remove "TH"/"ST"/"ND"/"RD" suffixes
  const normalized = weekStr.replace(/[–—]/g, '-').replace(/(\d+)(ST|ND|RD|TH)/gi, '$1');
  const match = normalized.match(/(\w+)\s+(\d+)\s*-\s*(\d+)\s+Week\s+(\d+)/i);
  if (!match) return null;

  const monthName = match[1];
  const startDay = parseInt(match[2]);
  const endDay = parseInt(match[3]);
  const weekNum = match[4];

  const months = {
    january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
    july: 6, august: 7, september: 8, october: 9, november: 10, december: 11
  };
  const monthIdx = months[monthName.toLowerCase()];
  if (monthIdx === undefined) return null;

  // Assume current year unless that doesn't make sense
  const year = 2025;

  const weekStart = `${year}-${String(monthIdx + 1).padStart(2, '0')}-${String(startDay).padStart(2, '0')}`;
  const weekEnd = `${year}-${String(monthIdx + 1).padStart(2, '0')}-${String(endDay).padStart(2, '0')}`;

  return {
    weekStart,
    weekEnd,
    weekLabel: `Week ${weekNum}`,
    weekRange: `${weekStart} to ${weekEnd}`,
  };
}

const headers = parseCSVLine(header);
console.log('Headers:', headers);

// Detect if adjustment column exists
const hasAdjustment = headers.some(h => h.toLowerCase().includes('adjustment'));
console.log('Has adjustment column:', hasAdjustment);
console.log('---');

let imported = 0;
let skipped = 0;

for (let i = 1; i < lines.length; i++) {
  const fields = parseCSVLine(lines[i]);
  if (fields.length < 13) {
    console.log(`Skipping line ${i + 1}: not enough fields`);
    skipped++;
    continue;
  }

  const weekRaw = fields[0];
  const affiliateName = fields[1].trim().replace(/\s*\(.*\)/, ''); // Remove parenthetical like "(GILBERTO)"
  const displayName = fields[1].trim(); // Keep original for notes
  const soldUsd = parseMoney(fields[2]);
  const netSc = parseMoney(fields[3]);
  const bonuses = parseMoney(fields[4]);
  const commissionRate = parseRate(fields[5]);
  const activePlayers = parseInt(fields[6]) || 0;

  let adjustment = 0;
  let adjustmentNote = null;
  let offset = 0;

  if (hasAdjustment) {
    adjustment = parseMoney(fields[7]);
    adjustmentNote = adjustment !== 0 ? '1.75% processing backtrack' : null;
    offset = 1;
  }

  const carryoverIn = parseMoney(fields[7 + offset]);
  const processingFees = parseMoney(fields[8 + offset]);
  const totalExpenses = parseMoney(fields[9 + offset]);
  const net = parseMoney(fields[10 + offset]);
  const netCommission = parseMoney(fields[11 + offset]);
  const carryoverOut = parseMoney(fields[12 + offset]);

  const week = parseWeek(weekRaw);
  if (!week) {
    console.log(`Skipping line ${i + 1}: can't parse week "${weekRaw}"`);
    skipped++;
    continue;
  }

  // Create affiliate if doesn't exist
  let affiliate = db.prepare('SELECT * FROM affiliates WHERE LOWER(username) = LOWER(?)').get(affiliateName);
  if (!affiliate) {
    const notes = displayName !== affiliateName ? `Also known as: ${displayName}` : null;
    db.prepare('INSERT INTO affiliates (username, notes) VALUES (?, ?)').run(affiliateName, notes);
    affiliate = db.prepare('SELECT * FROM affiliates WHERE LOWER(username) = LOWER(?)').get(affiliateName);
    console.log(`Created affiliate: ${affiliateName}` + (notes ? ` (${notes})` : ''));
  }

  // Check for duplicate report
  const existing = db.prepare(
    'SELECT id FROM weekly_reports WHERE affiliate_id = ? AND week_start = ? AND week_label = ?'
  ).get(affiliate.id, week.weekStart, week.weekLabel);

  if (existing) {
    console.log(`Skipping duplicate: ${affiliateName} ${week.weekLabel}`);
    skipped++;
    continue;
  }

  // Determine payout net (commission is on positive net only)
  const payoutNet = Math.max(0, net + carryoverIn);
  const rateOverrideReason = 'imported from spreadsheet';

  // Save player_weekly
  db.prepare(`
    INSERT OR REPLACE INTO player_weekly (affiliate_id, week_start, player_count)
    VALUES (?, ?, ?)
  `).run(affiliate.id, week.weekStart, activePlayers);

  // Save report — use the spreadsheet's calculated values to preserve exact numbers
  db.prepare(`
    INSERT INTO weekly_reports (
      affiliate_id, week_label, week_range, week_start, week_end,
      active_players, referred_players, net_sc, sold_usd, processing_fees, bonuses,
      total_expenses, carryover_in, net, payout_net,
      commission_rate, total_commission, carryover_out,
      adjustment, adjustment_note,
      rate_override_reason, status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unpaid')
  `).run(
    affiliate.id, week.weekLabel, week.weekRange, week.weekStart, week.weekEnd,
    0, activePlayers, netSc, soldUsd, processingFees, bonuses,
    totalExpenses, carryoverIn, net, payoutNet,
    commissionRate, netCommission, carryoverOut,
    adjustment, adjustmentNote,
    rateOverrideReason
  );

  const adjStr = adjustment !== 0 ? ` | Adj: $${adjustment}` : '';
  console.log(`Imported: ${affiliateName} | ${week.weekLabel} | Sold: $${soldUsd} | Commission: $${netCommission} | Carryover: $${carryoverOut}${adjStr}`);
  imported++;
}

console.log('---');
console.log(`Done. Imported: ${imported}, Skipped: ${skipped}`);
console.log(`Total affiliates: ${db.prepare('SELECT COUNT(*) as c FROM affiliates').get().c}`);
console.log(`Total reports: ${db.prepare('SELECT COUNT(*) as c FROM weekly_reports').get().c}`);
