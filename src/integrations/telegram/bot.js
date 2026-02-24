const TelegramBot = require('node-telegram-bot-api');
const commands = require('./commands');

/**
 * Create and start the Telegram bot.
 */
function createBot(config, watcher, router) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.warn('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env — Telegram disabled');
    return null;
  }

  const bot = new TelegramBot(token, { polling: true });
  const allowedChatId = String(chatId);

  // Auth middleware -- only respond to the configured user
  function auth(msg) {
    return String(msg.chat.id) === allowedChatId;
  }

  // Helper to send a message (with error handling)
  function send(text, opts = {}) {
    return bot.sendMessage(allowedChatId, text, opts)
      .catch(err => {
        console.error('Send error:', err.message);
        // Retry without formatting if it fails
        return bot.sendMessage(allowedChatId, text.replace(/<[^>]+>/g, '')).catch(() => {});
      });
  }

  // Helper to edit an existing message
  function edit(messageId, text, opts = {}) {
    return bot.editMessageText(text, { chat_id: allowedChatId, message_id: messageId, ...opts })
      .catch(err => {
        // Ignore "message is not modified" errors (content unchanged)
        if (err.message && err.message.includes('not modified')) return;
        console.error('Edit error:', err.message);
      });
  }

  // Wrap command handlers with error catching
  function safe(fn) {
    return async (...args) => {
      try {
        await fn(...args);
      } catch (err) {
        console.error('Command error:', err.message);
        send(`Error: ${err.message}`).catch(() => {});
      }
    };
  }

  // -- Command routing ------------------------------------------------

  bot.onText(/\/start$/, (msg) => {
    if (!auth(msg)) return;
    send([
      '<b>trhive</b> -- AI Fleet Command',
      '',
      '<code>/status</code> -- all sessions at a glance',
      '<code>/idle</code> -- list idle sessions',
      '<code>/working</code> -- list working sessions',
      '<code>/session N</code> -- detailed session status',
      '<code>/peek N</code> -- last output from Claude',
      '<code>/ask N msg</code> -- send message, get response',
      '<code>/tell N msg</code> -- fire and forget',
      '<code>/restart N</code> -- restart Claude',
      '<code>/kill N</code> -- kill session',
      '<code>/prs</code> -- all open PRs',
    ].join('\n'), { parse_mode: 'HTML' });
  });

  bot.onText(/\/status$/, (msg) => {
    if (!auth(msg)) return;
    safe(commands.status)(config, send, router);
  });

  bot.onText(/\/idle$/, (msg) => {
    if (!auth(msg)) return;
    safe(commands.idle)(config, send, router);
  });

  bot.onText(/\/working$/, (msg) => {
    if (!auth(msg)) return;
    safe(commands.working)(config, send, router);
  });

  bot.onText(/\/session\s+(\S+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.session)(config, send, router, match[1]);
  });

  bot.onText(/\/peek\s+(\S+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.peek)(config, send, router, match[1]);
  });

  bot.onText(/\/ask\s+(\S+)\s+(.+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.ask)(config, send, edit, router, match[1], match[2]);
  });

  bot.onText(/\/tell\s+(\S+)\s+(.+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.tell)(config, send, router, match[1], match[2]);
  });

  bot.onText(/\/restart\s+(\S+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.restart)(config, send, router, match[1]);
  });

  bot.onText(/\/kill\s+(\S+)/, (msg, match) => {
    if (!auth(msg)) return;
    safe(commands.kill)(config, send, router, match[1]);
  });

  bot.onText(/\/prs$/, (msg) => {
    if (!auth(msg)) return;
    safe(commands.prs)(config, send, router);
  });

  // -- Watcher notifications ------------------------------------------

  watcher.on('session:idle', ({ name, num }) => {
    send(`Session ${num} finished: ${name}`).catch(() => {});
  });

  watcher.on('ci:changed', ({ name, num, from, to, pr }) => {
    const icon = to === 'SUCCESS' ? 'PASS' : to === 'FAILURE' ? 'FAIL' : to;
    send(`CI ${icon} for session ${num} PR #${pr}: ${from} -> ${to}`).catch(() => {});
  });

  // -- Error handling -------------------------------------------------

  bot.on('polling_error', (err) => {
    // Ignore conflict errors during startup
    if (err.message && err.message.includes('409')) return;
    console.error('Telegram polling error:', err.message);
  });

  // Catch unhandled promise rejections from the bot
  process.on('unhandledRejection', (err) => {
    console.error('Unhandled rejection:', err.message || err);
  });

  console.log('Telegram bot started. Listening for commands...');
  return bot;
}

module.exports = { createBot };
