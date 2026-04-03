process.env.TZ = 'America/Chicago';

const express = require('express');
const path = require('path');
const fs = require('fs');
const db = require('./db');
const { calculate, getCarryoverIn, getPlayerHistory, TIERS } = require('./commission');
const { initBot, sendReport, formatCurrency, testConnection } = require('./telegram');
const { generatePDF } = require('./pdf');

const app = express();
const PORT = process.env.PORT || 3000;

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/reports', express.static(path.join(__dirname, 'reports')));

app.locals.formatCurrency = formatCurrency;

// ─── Helper: parse extra expenses from form ───
function parseExtras(body) {
  const extras = [];
  if (body.extra_label) {
    const labels = Array.isArray(body.extra_label) ? body.extra_label : [body.extra_label];
    const amounts = Array.isArray(body.extra_amount) ? body.extra_amount : [body.extra_amount];
    for (let i = 0; i < labels.length; i++) {
      const label = (labels[i] || '').trim();
      const amount = parseFloat(amounts[i]) || 0;
      if (label && amount !== 0) extras.push({ label, amount });
    }
  }
  return extras;
}

// ─── Helper: build report data from form body ───
function parseReportBody(body) {
  return {
    affiliateId: parseInt(body.affiliate_id),
    weekLabel: body.week_label,
    weekStart: body.week_start,
    weekEnd: body.week_end,
    activePlayers: parseInt(body.active_players) || 0,
    referredPlayers: parseInt(body.referred_players) || 0,
    netSc: parseFloat(body.net_sc) || 0,
    soldUsd: parseFloat(body.sold_usd) || 0,
    bonuses: parseFloat(body.bonuses) || 0,
    adjustment: parseFloat(body.adjustment) || 0,
    adjustmentNote: body.adjustment_note || null,
    extraExpenses: parseExtras(body),
    rateOverride: body.rate_override ? parseFloat(body.rate_override) / 100 : null,
    rateOverrideReason: body.rate_override_reason || null,
  };
}

// ─── Helper: save or update a report ───
function saveReport(data, existingId) {
  const calc = calculate({
    affiliateId: data.affiliateId,
    weekStart: data.weekStart,
    activePlayers: data.activePlayers,
    netSc: data.netSc,
    soldUsd: data.soldUsd,
    bonuses: data.bonuses,
    adjustment: data.adjustment,
    extraExpenses: data.extraExpenses,
    rateOverride: data.rateOverride,
    excludeReportId: existingId || null,
  });

  const weekRange = `${data.weekStart} to ${data.weekEnd}`;
  const extrasJson = JSON.stringify(data.extraExpenses);

  // Save player_weekly
  db.prepare(`
    INSERT OR REPLACE INTO player_weekly (affiliate_id, week_start, player_count)
    VALUES (?, ?, ?)
  `).run(data.affiliateId, data.weekStart, data.activePlayers);

  if (existingId) {
    db.prepare(`
      UPDATE weekly_reports SET
        week_label=?, week_range=?, week_start=?, week_end=?,
        active_players=?, referred_players=?, net_sc=?, sold_usd=?,
        processing_fees=?, bonuses=?, adjustment=?, adjustment_note=?,
        extra_expenses=?, total_expenses=?, carryover_in=?, net=?,
        payout_net=?, commission_rate=?, total_commission=?,
        carryover_out=?, rate_override_reason=?
      WHERE id=?
    `).run(
      data.weekLabel || `Week of ${data.weekStart}`, weekRange, data.weekStart, data.weekEnd,
      data.activePlayers, data.referredPlayers, data.netSc, data.soldUsd,
      calc.processingFees, data.bonuses, data.adjustment, data.adjustmentNote,
      extrasJson, calc.totalExpenses, calc.carryoverIn, calc.net,
      calc.payoutNet, calc.commissionRate, calc.totalCommission,
      calc.carryoverOut, data.rateOverrideReason,
      existingId
    );
    return existingId;
  } else {
    const result = db.prepare(`
      INSERT INTO weekly_reports (
        affiliate_id, week_label, week_range, week_start, week_end,
        active_players, referred_players, net_sc, sold_usd,
        processing_fees, bonuses, adjustment, adjustment_note,
        extra_expenses, total_expenses, carryover_in, net,
        payout_net, commission_rate, total_commission,
        carryover_out, rate_override_reason, status
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'unpaid')
    `).run(
      data.affiliateId, data.weekLabel || `Week of ${data.weekStart}`, weekRange, data.weekStart, data.weekEnd,
      data.activePlayers, data.referredPlayers, data.netSc, data.soldUsd,
      calc.processingFees, data.bonuses, data.adjustment, data.adjustmentNote,
      extrasJson, calc.totalExpenses, calc.carryoverIn, calc.net,
      calc.payoutNet, calc.commissionRate, calc.totalCommission,
      calc.carryoverOut, data.rateOverrideReason
    );
    return result.lastInsertRowid;
  }
}

// ─── Dashboard ───
app.get('/', (req, res) => {
  const affiliates = db.prepare('SELECT * FROM affiliates ORDER BY username').all();

  const data = affiliates.map(a => {
    const lastReport = db.prepare(`
      SELECT * FROM weekly_reports WHERE affiliate_id = ?
      ORDER BY week_start DESC LIMIT 1
    `).get(a.id);

    const playerRows = db.prepare(`
      SELECT player_count FROM player_weekly
      WHERE affiliate_id = ? ORDER BY week_start DESC LIMIT 4
    `).all(a.id);
    const counts = playerRows.map(r => r.player_count);
    const playerAvg = counts.length > 0
      ? Math.round((counts.reduce((s, c) => s + c, 0) / counts.length) * 100) / 100
      : 0;

    return {
      ...a,
      carryover: lastReport ? lastReport.carryover_out : 0,
      lastCommission: lastReport ? lastReport.total_commission : 0,
      lastSoldUsd: lastReport ? lastReport.sold_usd : 0,
      lastReferredPlayers: lastReport ? (lastReport.referred_players || 0) : 0,
      lastStatus: lastReport ? lastReport.status : 'n/a',
      playerAvg,
    };
  });

  const top = data.filter(a => a.lastSoldUsd >= 5000).sort((a, b) => b.lastSoldUsd - a.lastSoldUsd);
  const mid = data.filter(a => a.lastSoldUsd >= 1000 && a.lastSoldUsd < 5000).sort((a, b) => b.lastSoldUsd - a.lastSoldUsd);
  const misc = data.filter(a => a.lastSoldUsd < 1000).sort((a, b) => b.lastSoldUsd - a.lastSoldUsd);

  res.render('dashboard', { top, mid, misc });
});

// ─── Affiliates ───
app.post('/affiliate/new', (req, res) => {
  const { username, email, telegram_chat_id, notes } = req.body;
  try {
    db.prepare('INSERT INTO affiliates (username, email, telegram_chat_id, notes) VALUES (?, ?, ?, ?)')
      .run(username, email || null, telegram_chat_id || null, notes || null);
  } catch (e) {
    if (e.message.includes('UNIQUE')) return res.redirect('/?error=Username already exists');
    throw e;
  }
  res.redirect('/');
});

app.get('/affiliate/:id', (req, res) => {
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(req.params.id);
  if (!affiliate) return res.status(404).send('Affiliate not found');

  const reports = db.prepare('SELECT * FROM weekly_reports WHERE affiliate_id = ? ORDER BY week_start DESC').all(affiliate.id);
  const playerHistory = db.prepare('SELECT * FROM player_weekly WHERE affiliate_id = ? ORDER BY week_start DESC LIMIT 12').all(affiliate.id);

  res.render('affiliate', { affiliate, reports, playerHistory, formatCurrency });
});

app.post('/affiliate/:id/update', (req, res) => {
  const { email, telegram_chat_id, commission_rate_override, notes } = req.body;
  const rateOverride = commission_rate_override ? parseFloat(commission_rate_override) / 100 : null;
  db.prepare('UPDATE affiliates SET email=?, telegram_chat_id=?, commission_rate_override=?, notes=? WHERE id=?')
    .run(email || null, telegram_chat_id || null, rateOverride, notes || null, req.params.id);
  res.redirect(`/affiliate/${req.params.id}`);
});

app.post('/affiliate/:id/telegram-verify', async (req, res) => {
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(req.params.id);
  if (!affiliate) return res.status(404).json({ error: 'Affiliate not found' });
  if (!affiliate.telegram_chat_id) return res.status(400).json({ error: 'No chat ID set' });
  try {
    await testConnection(affiliate.telegram_chat_id, affiliate.username);
    res.json({ success: true });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ─── New Report ───
app.get('/report/new', (req, res) => {
  const affiliates = db.prepare('SELECT * FROM affiliates ORDER BY username').all();
  const selectedId = req.query.affiliate_id || '';
  let preload = null;

  if (selectedId) {
    const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(selectedId);
    if (affiliate) {
      const now = new Date();
      const dayOfWeek = now.getDay();
      const lastMonday = new Date(now);
      lastMonday.setDate(now.getDate() - dayOfWeek - 6);
      lastMonday.setHours(0, 0, 0, 0);
      const lastSunday = new Date(lastMonday);
      lastSunday.setDate(lastMonday.getDate() + 6);

      const weekStart = lastMonday.toISOString().split('T')[0];
      preload = {
        affiliate,
        carryoverIn: getCarryoverIn(parseInt(selectedId), weekStart),
        playerData: getPlayerHistory(parseInt(selectedId), weekStart, 0),
        weekStart,
        weekEnd: lastSunday.toISOString().split('T')[0],
      };
    }
  }

  res.render('report_form', { affiliates, preload, selectedId, tiers: TIERS, report: null });
});

// ─── Edit Report ───
app.get('/report/:id/edit', (req, res) => {
  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(req.params.id);
  if (!report) return res.status(404).send('Report not found');

  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(report.affiliate_id);
  const affiliates = db.prepare('SELECT * FROM affiliates ORDER BY username').all();

  // Parse stored extra_expenses JSON
  report.extraExpenses = [];
  try { report.extraExpenses = JSON.parse(report.extra_expenses || '[]'); } catch (e) {}

  const preload = {
    affiliate,
    carryoverIn: report.carryover_in,
    playerData: getPlayerHistory(report.affiliate_id, report.week_start, report.active_players),
    weekStart: report.week_start,
    weekEnd: report.week_end,
  };

  res.render('report_form', { affiliates, preload, selectedId: report.affiliate_id, tiers: TIERS, report });
});

// ─── API: Calculate (live preview) ───
app.post('/api/calculate', (req, res) => {
  const { affiliate_id, week_start, active_players, net_sc, sold_usd, bonuses, adjustment, extra_expenses, rate_override, exclude_report_id } = req.body;
  let extras = [];
  try { extras = JSON.parse(extra_expenses || '[]'); } catch (e) {}

  const result = calculate({
    affiliateId: parseInt(affiliate_id),
    weekStart: week_start,
    activePlayers: parseInt(active_players) || 0,
    netSc: parseFloat(net_sc) || 0,
    soldUsd: parseFloat(sold_usd) || 0,
    bonuses: parseFloat(bonuses) || 0,
    adjustment: parseFloat(adjustment) || 0,
    extraExpenses: extras,
    rateOverride: rate_override ? parseFloat(rate_override) / 100 : null,
    excludeReportId: exclude_report_id ? parseInt(exclude_report_id) : null,
  });
  res.json(result);
});

// ─── Save Report (new or edit) ───
app.post('/report/save', async (req, res) => {
  const data = parseReportBody(req.body);
  const existingId = req.body.report_id ? parseInt(req.body.report_id) : null;
  const reportId = saveReport(data, existingId);

  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(reportId);
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(data.affiliateId);

  try { await generatePDF(report, affiliate); } catch (e) { console.error('PDF error:', e); }

  res.redirect(`/report/${reportId}/send`);
});

// ─── Report View / Send ───
app.get('/report/:id/send', (req, res) => {
  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(req.params.id);
  if (!report) return res.status(404).send('Report not found');
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(report.affiliate_id);

  const playerRows = db.prepare(`
    SELECT week_start, player_count FROM player_weekly
    WHERE affiliate_id = ? AND week_start <= ?
    ORDER BY week_start DESC LIMIT 4
  `).all(report.affiliate_id, report.week_start).reverse();
  const counts = playerRows.map(r => r.player_count);
  const avg = counts.length > 0 ? counts.reduce((a, b) => a + b, 0) / counts.length : 0;

  let extras = [];
  try { extras = JSON.parse(report.extra_expenses || '[]'); } catch (e) {}

  res.render('report_send', { report, affiliate, playerRows, avg, extras, formatCurrency });
});

app.post('/report/:id/telegram', async (req, res) => {
  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(req.params.id);
  if (!report) return res.status(404).json({ error: 'Report not found' });
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(report.affiliate_id);
  try {
    await sendReport(report, affiliate);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/report/:id/status', (req, res) => {
  db.prepare('UPDATE weekly_reports SET status = ? WHERE id = ?').run(req.body.status, req.params.id);
  res.json({ success: true });
});

app.get('/report/:id/pdf', async (req, res) => {
  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(req.params.id);
  if (!report) return res.status(404).send('Report not found');
  const affiliate = db.prepare('SELECT * FROM affiliates WHERE id = ?').get(report.affiliate_id);
  let pdfPath = report.pdf_path;
  if (!pdfPath || !fs.existsSync(pdfPath)) pdfPath = await generatePDF(report, affiliate);
  res.download(pdfPath);
});

app.post('/report/:id/delete', (req, res) => {
  const report = db.prepare('SELECT * FROM weekly_reports WHERE id = ?').get(req.params.id);
  if (!report) return res.status(404).send('Report not found');
  if (report.pdf_path) try { fs.unlinkSync(report.pdf_path); } catch (e) {}
  db.prepare('DELETE FROM player_weekly WHERE affiliate_id = ? AND week_start = ?').run(report.affiliate_id, report.week_start);
  db.prepare('DELETE FROM weekly_reports WHERE id = ?').run(report.id);
  res.redirect(`/affiliate/${report.affiliate_id}`);
});


// Start
initBot();
app.listen(PORT, () => console.log(`PSM Commissions running on port ${PORT}`));
