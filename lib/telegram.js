/**
 * Telegram — send alert messages
 */

const https = require('https');

/**
 * Send a message via Telegram Bot API.
 * Falls back to console.log if credentials not set.
 */
async function sendAlert(signal) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.log(`[SIGNAL] ${signal.firm_name} — ${signal.job_title} (${signal.city || '?'}, ${signal.state || '?'})`);
    console.log(`         ${signal.job_url || ''}`);
    return;
  }

  // Truncate fields to prevent exceeding Telegram's 4096 char limit
  const firm = (signal.firm_name || '').slice(0, 200);
  const job = (signal.job_title || '').slice(0, 200);

  const text = [
    `\u{1F525} Law firm hiring`,
    ``,
    `Firm: ${firm}`,
    `Job: ${job}`,
    `City: ${signal.city || '?'}, ${signal.state || '?'}`,
    signal.job_url ? `URL: ${signal.job_url}` : '',
  ].filter(Boolean).join('\n');

  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ chat_id: chatId, text });
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${token}/sendMessage`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          console.error(`[Telegram] API error ${res.statusCode}: ${data.substring(0, 200)}`);
        }
        resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => {
      req.destroy();
      reject(new Error('Telegram request timed out'));
    });
    req.write(body);
    req.end();
  });
}

module.exports = { sendAlert };
