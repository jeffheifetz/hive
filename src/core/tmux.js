const { execSync } = require('child_process');

/**
 * Execute a command and return trimmed stdout, or null on failure.
 */
function exec(cmd, opts = {}) {
  try {
    return execSync(cmd, { encoding: 'utf8', timeout: 10000, ...opts }).trim();
  } catch {
    return null;
  }
}

/**
 * List all tmux sessions, returns array of session name strings.
 */
function listSessions() {
  const out = exec("tmux list-sessions -F '#S' 2>/dev/null");
  if (!out) return [];
  return out.split('\n').filter(Boolean).sort((a, b) => {
    const na = parseInt(a), nb = parseInt(b);
    if (!isNaN(na) && !isNaN(nb)) return na - nb;
    return a.localeCompare(b);
  });
}

/**
 * Capture pane content.
 * @param {string} target - tmux target (e.g. "6-branch:.1")
 * @param {object} opts
 * @param {number} opts.lines - number of scrollback lines (default: visible only)
 */
function capturePane(target, { lines } = {}) {
  const scrollback = lines ? `-S -${lines}` : '';
  const out = exec(`tmux capture-pane -t "${target}" -p ${scrollback} 2>/dev/null`);
  return out || '';
}

/**
 * Send keys to a tmux pane.
 * @param {string} target - tmux target
 * @param {string} keys - text to send
 * @param {boolean} enter - whether to press Enter after
 */
function sendKeys(target, keys, enter = true) {
  // Replace newlines with " — " so the entire message is sent as one line
  const oneLine = keys.replace(/\r?\n+/g, ' — ');
  // Escape single quotes in the message
  const escaped = oneLine.replace(/'/g, "'\\''");
  // Use -l for literal text (prevents key name interpretation)
  exec(`tmux send-keys -t "${target}" -l '${escaped}'`);
  if (enter) exec(`tmux send-keys -t "${target}" Enter`);
}

/**
 * Check if a tmux session exists.
 */
function hasSession(name) {
  return exec(`tmux has-session -t "${name}" 2>/dev/null`) !== null;
}

/**
 * Detect Claude's state from a pane capture.
 * @param {string} paneContent - raw pane capture text
 * @param {object} config - hive config with idlePatterns/offPatterns
 * @returns {'idle'|'working'|'off'}
 */
function detectState(paneContent, config) {
  const lines = paneContent.split('\n').filter(l => l.trim());
  if (lines.length === 0) return 'off';

  // Check raw lines for Claude Code idle/waiting indicators.
  // Must check raw content because ❯ (U+276F) is stripped by ASCII filter.
  const recentRaw = lines.slice(-15);
  for (let i = recentRaw.length - 1; i >= 0; i--) {
    const line = recentRaw[i];
    // Empty input prompt: ❯ with no text after it
    if (/❯\s*$/.test(line) && !/❯\s+\S/.test(line)) return 'idle';
    // Tool approval prompt: ❯ pointing at a numbered option
    if (/❯\s*\d+\./.test(line)) return 'idle';
    // Permission/approval prompt footer
    if (/Esc to cancel/.test(line)) return 'idle';
    // Question UI with "Do you want to proceed"
    if (/Do you want to proceed/.test(line)) return 'idle';
  }

  // Check last few lines (stripped) for config patterns
  const recentClean = recentRaw.map(l => l.replace(/[^\x20-\x7E]/g, ''));
  for (const line of recentClean) {
    for (const pat of config.idlePatterns) {
      if (pat.test(line)) return 'idle';
    }
  }
  for (const pat of config.offPatterns) {
    const lastClean = recentClean[recentClean.length - 1];
    if (pat.test(lastClean)) return 'off';
  }
  return 'working';
}

/**
 * Get git info for a repo directory.
 * @returns {{ branch, staged, modified, untracked }}
 */
function gitInfo(repoDir) {
  const branch = exec(`git -C "${repoDir}" branch --show-current 2>/dev/null`) || '';
  const staged = parseInt(exec(`git -C "${repoDir}" diff --cached --shortstat 2>/dev/null | sed -E 's/^ *([0-9]+) file.*/\\1/'`) || '0') || 0;
  const modified = parseInt(exec(`git -C "${repoDir}" diff --shortstat 2>/dev/null | sed -E 's/^ *([0-9]+) file.*/\\1/'`) || '0') || 0;
  const untracked = parseInt(exec(`git -C "${repoDir}" ls-files --others --exclude-standard 2>/dev/null | wc -l`) || '0') || 0;
  return { branch, staged, modified, untracked };
}

/**
 * Kill a tmux session.
 */
function killSession(name) {
  return exec(`tmux kill-session -t "${name}:" 2>/dev/null`) !== null;
}

// Claude Code TUI chrome patterns (status bars, prompt, UI elements)
const TUI_CHROME = [
  /\$[\d.]+/,                    // cost: $186.73
  /bypass permissions/,
  /shift\+tab/,
  /ctrl-g to edit/,
  /ctrl\+o to expand/,
  /no JIRA ticket/i,
  /^\s*>\s*$/,                   // bare prompt ">"
  /^\s*copy\s*$/,                // TUI "copy" button
  /-- INSERT --/,
  /Cogitated for/,               // "Cogitated for 1m 19s"
  /Baked for/,                   // "Baked for 3m 23s"
  /^\s*\d+\s*tokens/,            // token count
  /^\s*CI\s+(no build|PASS|FAIL)/i, // CI status line
  /^\s*approve,?\s*(next|merge)/i,  // "approve, next"
  /^Waiting/,                    // "Waitingpr diff..." tool calls
  /^Explore\(/,                  // "Explore(..." tool calls
  /^Reading\(/,                  // "Reading(..." tool calls
];

/**
 * Strip Claude Code TUI chrome from pane content.
 * Removes status bars, prompts, and UI elements from top and bottom.
 * Returns cleaned content string.
 */
function stripTUIChrome(content, config) {
  if (!content) return '';
  const lines = content.split('\n');

  function isChrome(line) {
    const clean = line.replace(/[^\x20-\x7E]/g, '').trim();
    if (!clean) return true;
    // Config patterns
    if (config) {
      for (const pat of (config.idlePatterns || [])) {
        if (pat.test(clean)) return true;
      }
      for (const pat of (config.offPatterns || [])) {
        if (pat.test(clean)) return true;
      }
    }
    // Built-in patterns
    for (const pat of TUI_CHROME) {
      if (pat.test(clean)) return true;
    }
    return false;
  }

  // Strip from bottom
  let end = lines.length;
  while (end > 0 && isChrome(lines[end - 1])) end--;

  // Strip from top
  let start = 0;
  while (start < end && isChrome(lines[start])) start++;

  return lines.slice(start, end)
    .map(l => l.replace(/[^\x20-\x7E]/g, '').trimEnd())
    .join('\n')
    .trim();
}

module.exports = {
  exec,
  listSessions,
  capturePane,
  sendKeys,
  hasSession,
  detectState,
  gitInfo,
  killSession,
  stripTUIChrome,
};
