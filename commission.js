const db = require('./db');

const TIERS = [
  { rate: 0.40, players: 50, sold: 20000 },
  { rate: 0.35, players: 35, sold: 14000 },
  { rate: 0.30, players: 25, sold: 8000 },
  { rate: 0.25, players: 15, sold: 4000 },
  { rate: 0.20, players: 10, sold: 3000 },
];

function getPlayerHistory(affiliateId, currentWeekStart, currentWeekPlayers) {
  const rows = db.prepare(`
    SELECT week_start, player_count FROM player_weekly
    WHERE affiliate_id = ? AND week_start < ?
    ORDER BY week_start DESC LIMIT 3
  `).all(affiliateId, currentWeekStart);

  const history = rows.reverse().map(r => ({
    week_start: r.week_start,
    count: r.player_count
  }));
  history.push({ week_start: currentWeekStart, count: currentWeekPlayers });

  const counts = history.map(h => h.count);
  const average = counts.reduce((a, b) => a + b, 0) / counts.length;
  return { history, average };
}

function determineTier(playerAvg, soldUsd) {
  for (const tier of TIERS) {
    if (playerAvg >= tier.players && soldUsd >= tier.sold) return tier;
  }
  return { rate: 0, players: 0, sold: 0 };
}

function getNextTier(playerAvg, soldUsd) {
  const current = determineTier(playerAvg, soldUsd);
  for (let i = TIERS.length - 1; i >= 0; i--) {
    if (TIERS[i].rate > current.rate) {
      return {
        tier: TIERS[i],
        playersNeeded: Math.max(0, TIERS[i].players - playerAvg),
        soldNeeded: Math.max(0, TIERS[i].sold - soldUsd),
      };
    }
  }
  return null;
}

function getCarryoverIn(affiliateId, weekStart, excludeReportId) {
  let sql = `SELECT carryover_out FROM weekly_reports
    WHERE affiliate_id = ? AND week_start < ?`;
  const params = [affiliateId, weekStart];
  if (excludeReportId) {
    sql += ' AND id != ?';
    params.push(excludeReportId);
  }
  sql += ' ORDER BY week_start DESC LIMIT 1';
  const prev = db.prepare(sql).get(...params);
  return prev ? prev.carryover_out : 0;
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

function calculate(inputs) {
  const {
    affiliateId, weekStart, activePlayers,
    netSc, soldUsd, bonuses, adjustment,
    extraExpenses, rateOverride, excludeReportId
  } = inputs;

  const playerData = getPlayerHistory(affiliateId, weekStart, activePlayers);
  const playerAvg = playerData.average;
  const carryoverIn = getCarryoverIn(affiliateId, weekStart, excludeReportId);

  const processingFees = round2(soldUsd * 0.0625);
  const adj = adjustment || 0;
  const extras = (extraExpenses || []).reduce((sum, e) => sum + (e.amount || 0), 0);
  const totalExpenses = round2(processingFees + bonuses + adj + extras);
  const net = round2(netSc - totalExpenses);
  const adjustedNet = round2(net + carryoverIn);
  const payoutNet = Math.max(0, adjustedNet);

  let commissionRate, tierBasis;
  if (rateOverride != null && rateOverride > 0) {
    commissionRate = rateOverride;
    tierBasis = 'manual override';
  } else {
    const affiliate = db.prepare('SELECT commission_rate_override FROM affiliates WHERE id = ?').get(affiliateId);
    if (affiliate && affiliate.commission_rate_override) {
      commissionRate = affiliate.commission_rate_override;
      tierBasis = 'affiliate override';
    } else {
      const tier = determineTier(playerAvg, soldUsd);
      commissionRate = tier.rate;
      tierBasis = tier.rate === 0
        ? 'below minimum tier'
        : `${Math.round(tier.rate * 100)}% tier (${tier.players}+ players, $${tier.sold.toLocaleString()}+ sold)`;
    }
  }

  const totalCommission = round2(payoutNet * commissionRate);
  const carryoverOut = round2(Math.min(0, adjustedNet));

  return {
    playerAvg: round2(playerAvg),
    playerHistory: playerData.history,
    carryoverIn,
    processingFees,
    totalExpenses,
    net,
    adjustedNet,
    payoutNet: round2(payoutNet),
    commissionRate,
    tierBasis,
    totalCommission,
    carryoverOut,
    nextTier: getNextTier(playerAvg, soldUsd),
    tiers: TIERS,
  };
}

module.exports = { calculate, getCarryoverIn, getPlayerHistory, determineTier, getNextTier, TIERS };
