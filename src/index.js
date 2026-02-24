#!/usr/bin/env node

const path = require('path');
const fs = require('fs');

// Load .env from project root
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq > 0) {
      process.env[trimmed.substring(0, eq)] = trimmed.substring(eq + 1);
    }
  }
}

// Load config
const configPath = path.join(__dirname, '..', 'hive.config.js');
if (!fs.existsSync(configPath)) {
  console.error('Missing hive.config.js -- copy from hive.config.example.js and customize.');
  process.exit(1);
}
const config = require(configPath);

// Create node router with local node
const LocalNode = require('./core/local-node');
const NodeRouter = require('./core/node-router');
const router = new NodeRouter();
router.addNode(new LocalNode('local'));
console.log('Node router initialized (local node)');

// Start core watcher
const Watcher = require('./core/watcher');
const watcher = new Watcher(config, router);
watcher.start().then(() => {
  console.log(`Watcher started (polling every ${config.watcher.interval / 1000}s)`);
}).catch(err => {
  console.error('Watcher failed to start:', err.message);
});

// Start Telegram integration
const { createBot } = require('./integrations/telegram/bot');
createBot(config, watcher, router);

// Start task queue
const TaskQueue = require('./core/taskqueue');
const taskQueue = new TaskQueue(config, watcher, router);
console.log('Task queue initialized');

// Patch config.sessions.repoDir to check spawned agents first
const originalRepoDir = config.sessions.repoDir;
config.sessions.repoDir = (n) => {
  const spawned = taskQueue.getSpawnedAgent(n);
  if (spawned) return spawned.repoDir;
  return originalRepoDir(n);
};

// Start project managers
const ProjectManager = require('./core/pm');
const pmManager = new ProjectManager(taskQueue);
// Load PM state from the state file (taskQueue already loaded it)
try {
  const stateFile = path.join(__dirname, '..', '.hive-state.json');
  const state = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  if (state.pms) pmManager.loadState(state.pms);
} catch {
  // No PM state yet
}

// Start Web dashboard
const { createWebServer } = require('./integrations/web/server');
const webServer = createWebServer(config, watcher, taskQueue, pmManager, router);

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  pmManager.stopAll();
  watcher.stop();
  if (webServer) webServer.close();
  process.exit(0);
});

process.on('SIGTERM', () => {
  pmManager.stopAll();
  watcher.stop();
  if (webServer) webServer.close();
  process.exit(0);
});
