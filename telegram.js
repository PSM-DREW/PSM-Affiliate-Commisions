const TelegramBot = require('node-telegram-bot-api');
const db = require('./db');
const fs = require('fs');

let bot = null;

function initBot() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) {
    console.log('TELEGRAM_BOT_TOKEN not set — Telegram bot disabled');
    return;
  }

  bot = new TelegramBot(token, { polling: false });

  bot.on('message', (msg) => {
    const chatId = msg.chat.id.toString();
    const username = msg.from.username || msg.from.first_name || '';
    const text = msg.text || '';

    console.log(`[Telegram] Message from @${username} (chat_id: ${chatId}): "${text}"`);

    // Try to match to existing affiliate by username
    const affiliate = db.prepare(
      'SELECT id, username FROM affiliates WHERE LOWER(username) = LOWER(?)'
    ).get(username);

    if (affiliate) {
      db.prepare('UPDATE affiliates SET telegram_chat_id = ? WHERE id = ?')
        .run(chatId, affiliate.id);
      console.log(`[Telegram] Linked chat_id ${chatId} to affiliate "${affiliate.username}" (id: ${affiliate.id})`);
      bot.sendMessage(chatId,
        `✅ You're connected to PSM Commissions as ${affiliate.username}. You'll receive your weekly reports here.`
      );
    } else {
      // Check if already linked by chat_id
      const existing = db.prepare(
        'SELECT id, username FROM affiliates WHERE telegram_chat_id = ?'
      ).get(chatId);

      if (existing) {
        console.log(`[Telegram] Already linked: chat_id ${chatId} → "${existing.username}"`);
        bot.sendMessage(chatId,
          `✅ You're already connected as ${existing.username}. You'll receive your weekly reports here.`
        );
      } else {
        console.log(`[Telegram] No affiliate match for @${username}. Chat ID ${chatId} saved for manual linking.`);
        bot.sendMessage(chatId,
          `✅ You're connected to PSM Commissions. Your chat ID (${chatId}) has been recorded. An admin will link your account.`
        );
      }
    }
  });

  console.log('Telegram bot started');
}

function formatCurrency(val) {
  if (val == null) return '$0.00';
  const neg = val < 0;
  const abs = Math.abs(val);
  const formatted = '$' + abs.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return neg ? `-${formatted}` : formatted;
}

async function sendReport(report, affiliate) {
  if (!bot) throw new Error('Telegram bot not initialized');
  if (!affiliate.telegram_chat_id) throw new Error('Affiliate has no Telegram chat ID');

  const carryoverInText = report.carryover_in !== 0 ? formatCurrency(report.carryover_in) : 'None';
  const ratePercent = Math.round(report.commission_rate * 100);
  const rateBasis = report.rate_override_reason || `${ratePercent}% tier`;

  // Build player history from player_weekly
  const playerRows = db.prepare(`
    SELECT week_start, player_count FROM player_weekly
    WHERE affiliate_id = ? AND week_start <= ?
    ORDER BY week_start DESC LIMIT 4
  `).all(report.affiliate_id, report.week_start).reverse();

  const playerLines = playerRows.map((r, i) => `Wk${i + 1}: ${r.player_count}`).join(' · ');

  // Calculate average
  const counts = playerRows.map(r => r.player_count);
  const avg = counts.length > 0 ? (counts.reduce((a, b) => a + b, 0) / counts.length) : 0;

  // Next tier info
  const { getNextTier } = require('./commission');
  const nextTierInfo = getNextTier(avg, report.sold_usd);
  let nextTierLine = 'At maximum tier!';
  if (nextTierInfo) {
    const parts = [];
    if (nextTierInfo.playersNeeded > 0) parts.push(`${Math.ceil(nextTierInfo.playersNeeded)} more players avg`);
    if (nextTierInfo.soldNeeded > 0) parts.push(`${formatCurrency(nextTierInfo.soldNeeded)} more sold`);
    nextTierLine = `Next tier: ${parts.join(' & ')} needed for ${Math.round(nextTierInfo.tier.rate * 100)}% rate`;
  }

  const carryoverLine = report.carryover_out < 0
    ? `\n⚠️ Balance carried forward: ${formatCurrency(report.carryover_out)}`
    : '';

  const now = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago' });

  const message = `🎰 *PLAYSLOTSMOBILE*
📊 *Commission Report*
${affiliate.username} · ${report.week_range} ${report.week_label}

*PERFORMANCE*
Players (4-wk avg): ${Math.round(avg * 100) / 100}
Sold USD: ${formatCurrency(report.sold_usd)}
NET SC: ${formatCurrency(report.net_sc)}

*EXPENSES*
Processing Fees (6.25%): ${formatCurrency(report.processing_fees)}
Bonuses: ${formatCurrency(report.bonuses)}
Total Expenses: ${formatCurrency(report.total_expenses)}

*NET CALCULATION*
Net: ${formatCurrency(report.net)}
Carryover In: ${carryoverInText}
Payout Net: ${formatCurrency(report.payout_net)}

*COMMISSION*
Rate: ${ratePercent}% (${rateBasis})
Total Commission: ${formatCurrency(report.total_commission)}${carryoverLine}

*PLAYER AVERAGE*
${playerLines}
4-week avg: ${Math.round(avg * 100) / 100} players
${nextTierLine}

_Generated ${now} CST · PlaySlotsMobile_`;

  await bot.sendMessage(affiliate.telegram_chat_id, message, { parse_mode: 'Markdown' });

  // Send PDF if exists
  if (report.pdf_path && fs.existsSync(report.pdf_path)) {
    await bot.sendDocument(affiliate.telegram_chat_id, report.pdf_path, {
      caption: `${affiliate.username} - ${report.week_label} Commission Report`
    });
  }

  // Mark as sent
  db.prepare(`
    UPDATE weekly_reports SET sent_via_telegram = 1, telegram_sent_at = datetime('now')
    WHERE id = ?
  `).run(report.id);
}

module.exports = { initBot, sendReport, formatCurrency };
