const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const { formatCurrency } = require('./telegram');
const { getNextTier, TIERS } = require('./commission');
const db = require('./db');

const logoPath = path.join(__dirname, 'public', 'images', 'logo.png');

function generatePDF(report, affiliate) {
  return new Promise((resolve, reject) => {
    const dir = path.join(__dirname, 'reports');
    fs.mkdirSync(dir, { recursive: true });

    const filename = `${affiliate.username}_${report.week_label.replace(/\s+/g, '_')}.pdf`;
    const filepath = path.join(dir, filename);
    const doc = new PDFDocument({ size: 'LETTER', margin: 50 });
    const stream = fs.createWriteStream(filepath);
    doc.pipe(stream);

    // Colors - PSM branded theme
    const bg = '#0a0a12';
    const surface = '#12121f';
    const purple = '#7B4FFF';
    const teal = '#00D4C8';
    const white = '#ffffff';
    const gray = '#aaa';
    const red = '#ff6b6b';

    // Background
    doc.rect(0, 0, doc.page.width, doc.page.height).fill(bg);

    let y = 30;

    // Logo in header
    if (fs.existsSync(logoPath)) {
      const logoWidth = 80;
      const logoX = (doc.page.width - logoWidth) / 2;
      doc.image(logoPath, logoX, y, { width: logoWidth });
      y += 85;
    }

    // Header
    doc.fillColor(purple).fontSize(22).font('Helvetica-Bold')
      .text('PlaySlotsMobile Commission Report', 50, y, { align: 'center' });
    y += 32;

    doc.fillColor(white).fontSize(12).font('Helvetica')
      .text(`${affiliate.username}  ·  ${report.week_range}  ·  ${report.week_label}`, 50, y, { align: 'center' });
    y += 28;

    // Status stamp
    const statusColor = report.status === 'paid' ? teal : red;
    doc.fillColor(statusColor).fontSize(14).font('Helvetica-Bold')
      .text(report.status.toUpperCase(), 50, y, { align: 'center' });
    y += 30;

    // Helper: section header
    function sectionHeader(title) {
      doc.roundedRect(50, y, doc.page.width - 100, 28, 4).fill(purple);
      doc.fillColor(white).fontSize(12).font('Helvetica-Bold')
        .text(title, 60, y + 7);
      y += 38;
    }

    // Helper: row
    function row(label, value, valueColor) {
      doc.fillColor(gray).fontSize(10).font('Helvetica')
        .text(label, 60, y);
      doc.fillColor(valueColor || white).fontSize(10).font('Helvetica-Bold')
        .text(value, 300, y, { width: 210, align: 'right' });
      y += 18;
    }

    // PERFORMANCE
    sectionHeader('PERFORMANCE');
    const playerRows = db.prepare(`
      SELECT week_start, player_count FROM player_weekly
      WHERE affiliate_id = ? AND week_start <= ?
      ORDER BY week_start DESC LIMIT 4
    `).all(report.affiliate_id, report.week_start).reverse();
    const counts = playerRows.map(r => r.player_count);
    const avg = counts.length > 0 ? counts.reduce((a, b) => a + b, 0) / counts.length : 0;

    row('Active Players (this week)', report.active_players.toString());
    row('Referred Players', (report.referred_players || 0).toString());
    row('Players (4-wk avg)', (Math.round(avg * 100) / 100).toString());
    row('Sold USD', formatCurrency(report.sold_usd));
    row('NET SC', formatCurrency(report.net_sc));
    y += 5;

    // EXPENSES
    sectionHeader('EXPENSES');
    row('Processing Fees (6.25%)', formatCurrency(report.processing_fees));
    row('Bonuses', formatCurrency(report.bonuses));
    if (report.adjustment) {
      row('Adjustment' + (report.adjustment_note ? ' (' + report.adjustment_note + ')' : ''), formatCurrency(report.adjustment));
    }
    let extras = [];
    try { extras = JSON.parse(report.extra_expenses || '[]'); } catch (e) {}
    extras.forEach(e => row(e.label, formatCurrency(e.amount)));
    row('Total Expenses', formatCurrency(report.total_expenses), purple);
    y += 5;

    // NET CALCULATION
    sectionHeader('NET CALCULATION');
    row('Net (NET SC - Expenses)', formatCurrency(report.net));
    row('Carryover In', report.carryover_in !== 0 ? formatCurrency(report.carryover_in) : 'None');
    row('Payout Net', formatCurrency(report.payout_net), teal);
    y += 5;

    // COMMISSION
    sectionHeader('COMMISSION');
    const ratePercent = Math.round(report.commission_rate * 100);
    const rateBasis = report.rate_override_reason || `${ratePercent}% tier`;
    row('Commission Rate', `${ratePercent}% (${rateBasis})`);
    row('Total Commission', formatCurrency(report.total_commission), purple);
    if (report.carryover_out < 0) {
      row('⚠️ Carryover Out', formatCurrency(report.carryover_out), red);
    }
    y += 10;

    // TIER QUALIFICATION TABLE
    sectionHeader('TIER QUALIFICATION');
    // Table header
    doc.fillColor(gray).fontSize(9).font('Helvetica-Bold');
    doc.text('Rate', 60, y);
    doc.text('Players Req', 140, y);
    doc.text('Sold Req', 240, y);
    doc.text('Status', 380, y);
    y += 16;

    for (const tier of [...TIERS].reverse()) {
      const meetsPlayers = avg >= tier.players;
      const meetsSold = report.sold_usd >= tier.sold;
      const meetsBoth = meetsPlayers && meetsSold;
      const isCurrentTier = report.commission_rate === tier.rate;

      if (isCurrentTier) {
        doc.roundedRect(55, y - 3, doc.page.width - 110, 16, 2).fill('#2a1f5c');
      }

      doc.fillColor(isCurrentTier ? purple : white).fontSize(9).font(isCurrentTier ? 'Helvetica-Bold' : 'Helvetica');
      doc.text(`${Math.round(tier.rate * 100)}%`, 60, y);
      doc.fillColor(meetsPlayers ? teal : red).text(`${tier.players}+`, 140, y);
      doc.fillColor(meetsSold ? teal : red).text(`$${tier.sold.toLocaleString()}+`, 240, y);
      doc.fillColor(meetsBoth ? teal : gray).text(meetsBoth ? '✓ Qualified' : '✗', 380, y);
      y += 16;
    }
    y += 10;

    // PLAYER HISTORY
    if (y > 620) { doc.addPage(); doc.rect(0, 0, doc.page.width, doc.page.height).fill(bg); y = 40; }
    sectionHeader('PLAYER HISTORY (Last 4 Weeks)');
    doc.fillColor(gray).fontSize(9).font('Helvetica-Bold');
    doc.text('Week', 60, y);
    doc.text('Players', 300, y, { width: 210, align: 'right' });
    y += 16;
    playerRows.forEach((r, i) => {
      doc.fillColor(white).fontSize(9).font('Helvetica');
      doc.text(`Week ${i + 1} (${r.week_start})`, 60, y);
      doc.text(r.player_count.toString(), 300, y, { width: 210, align: 'right' });
      y += 16;
    });
    doc.fillColor(purple).fontSize(9).font('Helvetica-Bold');
    doc.text('4-week average', 60, y);
    doc.text((Math.round(avg * 100) / 100).toString(), 300, y, { width: 210, align: 'right' });
    y += 20;

    // NEXT TIER MOTIVATOR
    const nextTierInfo = getNextTier(avg, report.sold_usd);
    if (nextTierInfo) {
      const parts = [];
      if (nextTierInfo.playersNeeded > 0) parts.push(`${Math.ceil(nextTierInfo.playersNeeded)} more avg players`);
      if (nextTierInfo.soldNeeded > 0) parts.push(`${formatCurrency(nextTierInfo.soldNeeded)} more in sales`);
      doc.fillColor(purple).fontSize(10).font('Helvetica-Bold')
        .text(`Next tier: ${parts.join(' & ')} to reach ${Math.round(nextTierInfo.tier.rate * 100)}%`, 60, y, { align: 'center' });
    } else {
      doc.fillColor(teal).fontSize(10).font('Helvetica-Bold')
        .text('You are at the maximum commission tier!', 60, y, { align: 'center' });
    }
    y += 25;

    // DEFINITION
    doc.fillColor(gray).fontSize(8).font('Helvetica')
      .text('Active player = purchased AND wagered ≥ $5.00 within the week (Mon–Sun CST)', 50, y, { align: 'center' });
    y += 20;

    // TIMESTAMP
    const now = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago' });
    doc.fillColor(gray).fontSize(8).font('Helvetica')
      .text(`Generated ${now} CST · PlaySlotsMobile`, 50, y, { align: 'center' });

    doc.end();
    stream.on('finish', () => {
      db.prepare('UPDATE weekly_reports SET pdf_path = ? WHERE id = ?').run(filepath, report.id);
      resolve(filepath);
    });
    stream.on('error', reject);
  });
}

module.exports = { generatePDF };
