const os = require('os');
const path = require('path');

module.exports = {
  // ── Session discovery ───────────────────────────────────
  sessions: {
    // Regex to match tmux session names that are part of your fleet
    // Default: numbered sessions (1-Reviews, 6-DEV-43966-..., etc.)
    pattern: /^\d+/,

    // Map session number → repo directory
    repoDir: (n) => {
      if (n === 11) return path.join(os.homedir(), 'Coding/Agentic/webplatform-pr-checks');
      return path.join(os.homedir(), `Coding/webplatform${n}`);
    },

    // Which pane index runs Claude Code (depends on your tmux layout)
    claudePane: 1,

    // Fixed session roles (optional, for display)
    roles: {
      1: 'Reviews',
      2: 'Ideas',
      3: 'Urgent',
      4: 'Tests',
      11: 'PR-Checks',
    },
  },

  // ── Idle detection ──────────────────────────────────────
  // Patterns that indicate Claude is waiting for input (idle)
  idlePatterns: [
    /bypass permissions/,
    /shift\+tab/,
    /ctrl-g to edit/,
  ],

  // Patterns that indicate Claude isn't running
  offPatterns: [
    /conversation\./,
  ],

  // ── Cache (from tmux dashboard scripts) ─────────────────
  cache: {
    // Directory for PR/CI/review cache files (written by cache-warmer.sh)
    statusPrefix: '/tmp/tmux-status-',
    // Directory for Claude state files (written by idle-watcher.sh)
    stateDir: '/tmp/tmux-claude-states',
  },

  // ── Relay settings ──────────────────────────────────────
  relay: {
    // How often to poll for response (ms)
    pollInterval: 2000,
    // How long to wait before declaring done after idle detected (ms)
    cooldown: 5000,
    // Max time to wait for a response (ms)
    timeout: 5 * 60 * 1000,
  },

  // ── Watcher settings ────────────────────────────────────
  watcher: {
    // How often to check for state changes (ms)
    interval: 10000,
  },

  // ── Web dashboard links ───────────────────────────────
  // URL templates for PR and CI links in the dashboard.
  // Use ${prNum} and ${ciBuild} as placeholders.
  // Set to null to disable linking.
  links: {
    pr: 'https://github.com/mavencare/webplatform/pull/${prNum}',
    ci: 'https://jenkins.vivtechnologies.com/job/webplatform/job/PR-${prNum}/${ciBuild}/',
  },
};
