const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');
const express = require('express');
const { WebSocketServer } = require('ws');
const fleet = require('../../core/fleet');
const relay = require('../../core/relay');
const git = require('../../core/git');
const RemoteNode = require('../../core/remote-node');

/**
 * Scan a directory for Claude command .md files and parse frontmatter.
 * Returns array of { name, description }.
 */
function scanCommands(dir) {
  const cmds = [];
  try {
    const files = fs.readdirSync(dir).filter(f => f.endsWith('.md'));
    for (const file of files) {
      const content = fs.readFileSync(path.join(dir, file), 'utf8');
      const m = content.match(/^---\n([\s\S]*?)\n---/);
      if (m) {
        const nameMatch = m[1].match(/^name:\s*(.+)/m);
        const descMatch = m[1].match(/^description:\s*(.+)/m);
        cmds.push({
          name: nameMatch ? nameMatch[1].trim() : file.replace('.md', ''),
          description: descMatch ? descMatch[1].trim() : '',
        });
      } else {
        cmds.push({ name: file.replace('.md', ''), description: '' });
      }
    }
  } catch {
    // Directory doesn't exist or not readable -- that's fine
  }
  return cmds;
}

/**
 * Discover all available Claude slash commands.
 * Checks global ~/.claude/commands/ and project-level .claude/commands/.
 */
function discoverCommands(config) {
  const globalDir = path.join(os.homedir(), '.claude', 'commands');
  const globalCmds = scanCommands(globalDir);

  // Check project-level commands from the first session's repo
  let projectCmds = [];
  if (config.sessions && config.sessions.repoDir) {
    const projectDir = path.join(config.sessions.repoDir(1), '.claude', 'commands');
    projectCmds = scanCommands(projectDir);
  }

  // Merge: project commands override global ones with same name
  const byName = new Map();
  for (const c of globalCmds) byName.set(c.name, c);
  for (const c of projectCmds) byName.set(c.name, c);
  return Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Capture pane with ANSI escape sequences preserved.
 * Uses -e flag for raw terminal colors that xterm.js can render.
 * -S -500 captures 500 lines of scrollback history.
 */
async function capturePaneAnsi(node, target) {
  const content = await node.exec(`tmux capture-pane -e -p -S -500 -t "${target}" 2>/dev/null`) || '';
  const colsStr = await node.exec(`tmux display-message -p -t "${target}" "#{pane_width}" 2>/dev/null`);
  const cols = parseInt(colsStr) || 0;
  return { content, cols };
}

/**
 * Create and start the web dashboard server.
 * @param {object} config - hive config
 * @param {Watcher} watcher - core watcher instance
 * @param {TaskQueue} taskQueue - task queue instance
 * @param {ProjectManager} pmManager
 * @param {NodeRouter} router
 * @returns {{ app, server, wss }}
 */
function createWebServer(config, watcher, taskQueue, pmManager, router) {
  const port = parseInt(process.env.WEB_PORT) || 3000;
  const token = process.env.WEB_TOKEN;
  const workerSecret = process.env.HIVE_WORKER_SECRET;

  if (!token) {
    console.warn('WEB_TOKEN not set in .env -- web dashboard disabled');
    return null;
  }

  const app = express();
  app.use(express.static(path.join(__dirname, 'public')));

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server });

  // Discover available slash commands
  const commands = discoverCommands(config);

  // Track authenticated clients
  const clients = new Set();
  // Per-client terminal subscriptions: ws -> { interval, session, lastContent }
  const termSubs = new Map();
  // Track worker connections: ws -> RemoteNode
  const workers = new Map();

  // -- WebSocket handling -----------------------------------------------

  wss.on('connection', (ws) => {
    let authenticated = false;
    let isWorker = false;

    // Auth timeout -- must authenticate within 5s
    const authTimeout = setTimeout(() => {
      if (!authenticated) {
        ws.send(JSON.stringify({ type: 'error', message: 'Auth timeout' }));
        ws.close();
      }
    }, 5000);

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch {
        return;
      }

      // First message must be auth or worker registration
      if (!authenticated) {
        if (msg.type === 'auth' && msg.token === token) {
          // Dashboard client
          authenticated = true;
          clearTimeout(authTimeout);
          clients.add(ws);
          ws.send(JSON.stringify({ type: 'auth', ok: true }));
          // Send config, available commands, and initial fleet status
          ws.send(JSON.stringify({ type: 'config', links: config.links || {} }));
          ws.send(JSON.stringify({ type: 'commands:list', commands }));
          sendFleetStatus(ws);
          // Send task queue initial state
          if (taskQueue) {
            ws.send(JSON.stringify({ type: 'tasks:list', tasks: taskQueue.getTasksList() }));
            ws.send(JSON.stringify({ type: 'auto:status', sessions: taskQueue.getAutoSessions() }));
            ws.send(JSON.stringify({ type: 'approvals:list', approvals: taskQueue.getPendingApprovals() }));
            const feedData = taskQueue.getFeed(null, 50);
            ws.send(JSON.stringify({ type: 'feed:entries', entries: feedData.entries, hasMore: feedData.hasMore }));
            ws.send(JSON.stringify({ type: 'rules:list', rules: taskQueue.getRules() }));
            ws.send(JSON.stringify({ type: 'designations:status', designations: taskQueue.getDesignations() }));
            ws.send(JSON.stringify({ type: 'vim:status', enabled: taskQueue.vimMode }));
            const spawnRange = taskQueue.getSpawnRange();
            ws.send(JSON.stringify({ type: 'spawn:config', min: spawnRange.min, max: spawnRange.max, tmuxDir: spawnRange.tmuxDir }));
            ws.send(JSON.stringify({ type: 'spawn:debug', enabled: taskQueue.spawnDebug }));
          }
          if (pmManager) {
            ws.send(JSON.stringify({ type: 'pm:list', pms: pmManager.getAll() }));
          }
          // Send connected worker nodes info
          const workerNodes = Array.from(workers.values()).map(n => ({
            id: n.id, type: n.type, connected: n.connected,
          }));
          ws.send(JSON.stringify({ type: 'nodes:list', nodes: workerNodes }));
        } else if (msg.type === 'worker:register' && workerSecret && msg.secret === workerSecret) {
          // Worker node registration
          authenticated = true;
          isWorker = true;
          clearTimeout(authTimeout);
          const node = new RemoteNode(msg.nodeId, ws);
          router.addNode(node);
          workers.set(ws, node);
          ws.send(JSON.stringify({ type: 'worker:registered', nodeId: msg.nodeId }));
          console.log(`Worker "${msg.nodeId}" connected`);
          // Notify dashboard clients about the new node
          broadcast({ type: 'node:connected', nodeId: msg.nodeId });
        } else {
          ws.send(JSON.stringify({ type: 'auth', ok: false }));
          ws.close();
        }
        return;
      }

      // Worker messages: handle RPC responses and heartbeats
      if (isWorker) {
        if (msg.type === 'rpc:response') {
          const node = workers.get(ws);
          if (node) node.handleResponse(msg);
        }
        // Heartbeats are handled implicitly (connection stays alive)
        return;
      }

      // Dashboard client message routing
      handleMessage(ws, msg).catch(err => {
        console.error('Message handler error:', err.message);
        if (ws.readyState === 1) {
          ws.send(JSON.stringify({ type: 'error', message: err.message }));
        }
      });
    });

    ws.on('close', () => {
      authenticated = false;
      clients.delete(ws);
      clearTermSub(ws);
      clearTimeout(authTimeout);
      // Clean up worker
      const node = workers.get(ws);
      if (node) {
        node.disconnect();
        router.removeNode(node.id);
        workers.delete(ws);
        console.log(`Worker "${node.id}" disconnected`);
        broadcast({ type: 'node:disconnected', nodeId: node.id });
      }
    });

    ws.on('error', () => {
      clients.delete(ws);
      clearTermSub(ws);
      const node = workers.get(ws);
      if (node) {
        node.disconnect();
        router.removeNode(node.id);
        workers.delete(ws);
      }
    });
  });

  // -- Message handlers --------------------------------------------------

  async function handleMessage(ws, msg) {
    switch (msg.type) {
      case 'fleet:get':
        await sendFleetStatus(ws);
        break;

      case 'peek': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const paneTarget = `${name}:.${config.sessions.claudePane}`;
        const { content: peekContent, cols: peekCols } = await capturePaneAnsi(node, paneTarget);
        ws.send(JSON.stringify({ type: 'terminal:data', session: msg.session, content: peekContent, cols: peekCols }));
        break;
      }

      case 'fleet:search': {
        const query = (msg.query || '').trim();
        if (!query) { ws.send(JSON.stringify({ type: 'fleet:search:result', results: [] })); break; }
        const sessions = await fleet.getFleetStatus(config, router);
        const results = [];
        const queryLower = query.toLowerCase();
        await Promise.all(sessions.map(async (s) => {
          const node = router.nodeFor(s.name);
          if (!node) return;
          const paneTarget = `${s.name}:.${config.sessions.claudePane}`;
          try {
            const { content: searchContent } = await capturePaneAnsi(node, paneTarget);
            const plain = searchContent.replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '');
            if (plain.toLowerCase().includes(queryLower)) {
              // Extract matching lines for context
              const matchLines = plain.split('\n')
                .filter(l => l.toLowerCase().includes(queryLower))
                .slice(0, 3)
                .map(l => l.trim());
              results.push({ num: s.num, name: s.name, branch: s.branch, state: s.state, matchLines });
            }
          } catch {}
        }));
        ws.send(JSON.stringify({ type: 'fleet:search:result', query, results }));
        break;
      }

      case 'terminal:subscribe': {
        // Unsubscribe from any previous session
        clearTermSub(ws);
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const paneTarget = `${name}:.${config.sessions.claudePane}`;
        // Send immediately
        const { content: subContent, cols: subCols } = await capturePaneAnsi(node, paneTarget);
        ws.send(JSON.stringify({ type: 'terminal:data', session: msg.session, content: subContent, cols: subCols }));
        const sub = { interval: null, session: msg.session, lastContent: subContent };
        // Poll every 2s — only send if content changed
        sub.interval = setInterval(async () => {
          if (ws.readyState !== 1) { clearTermSub(ws); return; }
          try {
            const { content: pollContent, cols: pollCols } = await capturePaneAnsi(node, paneTarget);
            if (pollContent === sub.lastContent) return;
            sub.lastContent = pollContent;
            ws.send(JSON.stringify({ type: 'terminal:data', session: msg.session, content: pollContent, cols: pollCols }));
          } catch {
            // Node may have disconnected
          }
        }, 2000);
        termSubs.set(ws, sub);
        break;
      }

      case 'terminal:unsubscribe':
        clearTermSub(ws);
        break;

      case 'ask': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        relay.ask(config, node, name, msg.message, {
          onStream: (content, isFinal) => {
            if (ws.readyState !== 1) return;
            ws.send(JSON.stringify({ type: 'ask:stream', session: msg.session, content, final: isFinal }));
          },
          vimMode: taskQueue ? taskQueue.vimMode : false,
        }).then((result) => {
          if (ws.readyState !== 1) return;
          ws.send(JSON.stringify({
            type: 'ask:done',
            session: msg.session,
            success: result.success,
            response: result.response,
            error: result.error,
            duration: result.duration,
          }));
        });
        break;
      }

      case 'tell': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        relay.tell(config, node, name, msg.message, { vimMode: taskQueue ? taskQueue.vimMode : false }).then((result) => {
          if (ws.readyState !== 1) return;
          ws.send(JSON.stringify({
            type: 'tell:done',
            session: msg.session,
            success: result.success,
            error: result.error,
          }));
        }).catch((err) => {
          console.error('tell error:', err);
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'tell:done', session: msg.session, success: false, error: err.message }));
          }
        });
        break;
      }

      case 'keys': {
        // Send raw tmux keys (Enter, Up, Down, Escape, Tab, etc.)
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const paneTarget = `${name}:.${config.sessions.claudePane}`;
        // msg.keys is an array of tmux key names, e.g. ["Enter"], ["Up"], ["Escape"]
        // Keys bar buttons always send raw — vim preamble only applies to typed text (ask/tell)
        for (const key of (msg.keys || [])) {
          await node.exec(`tmux send-keys -t "${paneTarget}" ${key}`);
        }
        ws.send(JSON.stringify({ type: 'keys:done', session: msg.session }));
        break;
      }

      case 'restart': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const paneTarget = `${name}:.${config.sessions.claudePane}`;
        // Send Escape, then /exit, wait, then claude --resume
        await node.exec(`tmux send-keys -t "${paneTarget}" Escape`);
        setTimeout(async () => {
          await node.exec(`tmux send-keys -t "${paneTarget}" -l '/exit'`);
          await node.exec(`tmux send-keys -t "${paneTarget}" Enter`);
          setTimeout(async () => {
            await node.exec(`tmux send-keys -t "${paneTarget}" -l 'claude --resume'`);
            await node.exec(`tmux send-keys -t "${paneTarget}" Enter`);
            if (ws.readyState === 1) {
              ws.send(JSON.stringify({ type: 'restart:done', session: msg.session }));
            }
          }, 3000);
        }, 500);
        break;
      }

      // -- Git info messages -----------------------------------------------
      case 'git:info': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const num = fleet.sessionNum(name);
        const nc = fleet.getNodeConfig(config, nodeId);
        const repoDir = num ? nc.sessions.repoDir(num) : null;
        if (!repoDir) {
          ws.send(JSON.stringify({ type: 'git:info', session: msg.session, log: [], diffStat: [], stagedStat: [], changedFiles: [], branchDiff: null }));
          return;
        }
        const [log, diffStat, stagedStat, changedFiles, branchDiff] = await Promise.all([
          git.getLog(node, repoDir),
          git.getDiffStat(node, repoDir),
          git.getStagedStat(node, repoDir),
          git.getChangedFiles(node, repoDir),
          git.getBranchDiff(node, repoDir),
        ]);
        ws.send(JSON.stringify({ type: 'git:info', session: msg.session, log, diffStat, stagedStat, changedFiles, branchDiff }));
        break;
      }

      case 'git:diff': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const num = fleet.sessionNum(name);
        const nc = fleet.getNodeConfig(config, nodeId);
        const repoDir = num ? nc.sessions.repoDir(num) : null;
        let diff = '';
        if (repoDir) {
          if (msg.commit) {
            diff = await git.getCommitFileDiff(node, repoDir, msg.commit, msg.file);
          } else {
            diff = await git.getFileDiff(node, repoDir, msg.file, msg.base);
          }
        }
        ws.send(JSON.stringify({ type: 'git:diff', session: msg.session, file: msg.file, diff }));
        break;
      }

      case 'git:commit': {
        const found = await fleet.findSession(config, router, msg.session);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: `No session matching "${msg.session}"` }));
          return;
        }
        const { name, nodeId } = found;
        const node = router.getNode(nodeId);
        const num = fleet.sessionNum(name);
        const nc = fleet.getNodeConfig(config, nodeId);
        const repoDir = num ? nc.sessions.repoDir(num) : null;
        const files = repoDir ? await git.getCommitFiles(node, repoDir, msg.hash) : [];
        ws.send(JSON.stringify({ type: 'git:commit', session: msg.session, hash: msg.hash, files }));
        break;
      }

      // -- Task queue messages ----------------------------------------------
      case 'task:create': {
        if (!taskQueue) break;
        const task = taskQueue.createTask(msg.text, msg.mode, msg.targetSession, msg.designation);
        ws.send(JSON.stringify({ type: 'task:created', task }));
        break;
      }

      case 'task:attach': {
        if (!taskQueue) break;
        const task = taskQueue.attachTask(msg.text, msg.session, msg.meta);
        broadcast({ type: 'task:created', task });
        broadcast({ type: 'task:dispatched', task });
        ws.send(JSON.stringify({ type: 'task:attached', task }));
        break;
      }

      case 'task:update': {
        if (!taskQueue) break;
        const updatedTask = taskQueue.updateTask(msg.taskId, msg.updates || {});
        if (updatedTask) broadcast({ type: 'task:updated', task: updatedTask });
        break;
      }

      case 'task:snapshot': {
        if (!taskQueue) break;
        const snapTask = taskQueue.tasks.get(msg.taskId);
        if (snapTask && snapTask.snapshot) {
          ws.send(JSON.stringify({ type: 'task:snapshot', taskId: msg.taskId, content: snapTask.snapshot, cols: snapTask.snapshotCols || 0 }));
        }
        break;
      }

      case 'task:cancel': {
        if (!taskQueue) break;
        const cancelledTask = taskQueue.cancelTask(msg.taskId);
        if (cancelledTask) broadcast({ type: 'task:cancelled', task: cancelledTask });
        break;
      }

      case 'task:complete': {
        if (!taskQueue) break;
        const task = taskQueue.completeTask(msg.taskId, msg.result || 'Manually completed');
        if (task) broadcast({ type: 'task:completed', task });
        break;
      }

      case 'task:park': {
        if (!taskQueue) break;
        // Capture snapshot of the session before parking
        const parkTaskObj = taskQueue.tasks.get(msg.taskId);
        let parkSnapshot = null, parkCols = 0;
        if (parkTaskObj && parkTaskObj.assignedTo) {
          try {
            const found = await fleet.findSession(config, router, parkTaskObj.assignedTo);
            if (found) {
              const node = router.getNode(found.nodeId);
              const paneTarget = `${found.name}:.${config.sessions.claudePane}`;
              const { content, cols } = await capturePaneAnsi(node, paneTarget);
              parkSnapshot = content;
              parkCols = cols;
            }
          } catch { /* best-effort snapshot */ }
        }
        const parkedTask = taskQueue.parkTask(msg.taskId, parkSnapshot, parkCols);
        if (parkedTask) broadcast({ type: 'task:parked', task: parkedTask });
        break;
      }

      case 'task:unpark': {
        if (!taskQueue) break;
        const unparkedTask = taskQueue.unparkTask(msg.taskId);
        if (unparkedTask) broadcast({ type: 'task:unparked', task: unparkedTask });
        break;
      }

      case 'task:start': {
        if (!taskQueue) break;
        taskQueue.startTask(msg.taskId, msg.session || null).then((task) => {
          if (task) ws.send(JSON.stringify({ type: 'task:started', task }));
        }).catch((err) => {
          ws.send(JSON.stringify({ type: 'error', message: err.message }));
        });
        break;
      }

      case 'auto:toggle': {
        if (!taskQueue) break;
        taskQueue.toggleAutoSession(msg.session);
        break;
      }

      case 'auto:set': {
        if (!taskQueue) break;
        taskQueue.setAutoSessions(msg.sessions || []);
        break;
      }

      case 'broadcast': {
        if (!taskQueue) break;
        taskQueue.broadcast(msg.message, msg.target, msg.sessions).then((result) => {
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'broadcast:done', sent: result.sent, failed: result.failed }));
          }
        });
        break;
      }

      case 'approval:respond': {
        if (!taskQueue) break;
        const approval = await taskQueue.resolveApproval(msg.approvalId, msg.approved);
        if (approval) ws.send(JSON.stringify({ type: 'approval:resolved', approval }));
        break;
      }

      case 'feed:get': {
        if (!taskQueue) break;
        const feedData = taskQueue.getFeed(msg.before, msg.limit);
        ws.send(JSON.stringify({ type: 'feed:entries', entries: feedData.entries, hasMore: feedData.hasMore }));
        break;
      }

      case 'rule:toggle': {
        if (!taskQueue) break;
        taskQueue.toggleRule(msg.ruleId);
        break;
      }

      // -- Designation messages ---------------------------------------------
      case 'designation:set': {
        if (!taskQueue) break;
        taskQueue.setDesignation(msg.session, msg.designation);
        break;
      }

      // -- VIM mode messages -------------------------------------------------
      case 'vim:toggle': {
        if (!taskQueue) break;
        taskQueue.setVimMode(msg.enabled);
        broadcast({ type: 'vim:status', enabled: taskQueue.vimMode });
        break;
      }

      // -- Spawn config messages -----------------------------------------------
      case 'spawn:config:update': {
        if (!taskQueue) break;
        try {
          taskQueue.setSpawnRange(msg.min, msg.max, msg.tmuxDir);
          // broadcast happens via spawn:config:changed event bridge
        } catch (err) {
          ws.send(JSON.stringify({ type: 'error', message: err.message }));
        }
        break;
      }

      case 'spawn:debug': {
        if (!taskQueue) break;
        taskQueue.spawnDebug = !!msg.enabled;
        broadcast({ type: 'spawn:debug', enabled: taskQueue.spawnDebug });
        break;
      }

      // -- Spawn messages ---------------------------------------------------
      case 'spawn:slots': {
        if (!taskQueue) break;
        const slots = await taskQueue.getAvailableSlots();
        ws.send(JSON.stringify({ type: 'spawn:slots', slots }));
        break;
      }

      case 'spawn': {
        if (!taskQueue) break;
        taskQueue.spawnSession({
          num: msg.num,
          baseDir: msg.baseDir,
          name: msg.name,
          gitUrl: msg.gitUrl,
          skipPermissions: msg.skipPermissions,
        }).then((result) => {
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'spawn:done', success: true, num: result.num, repoDir: result.repoDir }));
          }
          // Refresh fleet for all clients after a delay
          setTimeout(() => {
            broadcastFleetStatus().catch(() => {});
          }, 3000);
        }).catch((err) => {
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'spawn:done', success: false, error: err.message }));
          }
        });
        break;
      }

      // -- PM messages -------------------------------------------------------
      case 'pm:create': {
        if (!pmManager) break;
        const pm = pmManager.create(msg.config);
        ws.send(JSON.stringify({ type: 'pm:created', pm }));
        broadcast({ type: 'pm:list', pms: pmManager.getAll() });
        break;
      }

      case 'pm:update': {
        if (!pmManager) break;
        pmManager.update(msg.id, msg.updates);
        broadcast({ type: 'pm:list', pms: pmManager.getAll() });
        break;
      }

      case 'pm:delete': {
        if (!pmManager) break;
        pmManager.remove(msg.id);
        broadcast({ type: 'pm:list', pms: pmManager.getAll() });
        break;
      }

      case 'pm:toggle': {
        if (!pmManager) break;
        pmManager.toggle(msg.id);
        broadcast({ type: 'pm:list', pms: pmManager.getAll() });
        break;
      }

      case 'pm:list': {
        if (!pmManager) break;
        ws.send(JSON.stringify({ type: 'pm:list', pms: pmManager.getAll() }));
        break;
      }
    }
  }

  function clearTermSub(ws) {
    const sub = termSubs.get(ws);
    if (sub) {
      clearInterval(sub.interval);
      termSubs.delete(ws);
    }
  }

  // -- Fleet status broadcast --------------------------------------------

  // Noise patterns to strip from previews (already shown in badges or not useful)
  const PREVIEW_NOISE = [
    /^https?:\/\//i,
    /^PR[:#]\s*\d/i,
    /^CI\s/i,
    /^Jenkins/i,
    /^Slack:/i,
    /^Commit\s+[a-f0-9]/i,
    /^Branch:/i,
    /^merge\s+PR\s/i,
    /^\s*no JIRA/i,
    /^\s*In Review$/i,
    /^\s*PASS|FAIL|SUCCESS|FAILURE$/i,
    /^Try\s+"/,
    /^Press Ctrl/i,
    /^No conversations found/i,
    /^claude\s+--resume/i,
    /default interactive shell/i,
    /support\.apple\.com/i,
  ];

  function cleanPreview(content) {
    if (!content) return '';
    const lines = content.split('\n')
      .map(l => l.trimEnd())
      .filter(l => {
        const t = l.trim();
        if (!t) return false;
        for (const pat of PREVIEW_NOISE) {
          if (pat.test(t)) return false;
        }
        return true;
      });
    return lines.slice(-5).join('\n');
  }

  async function getFleetWithPreviews() {
    const sessions = await fleet.getFleetStatus(config, router);
    for (const s of sessions) {
      try {
        const node = router.nodeFor(s.name);
        if (node) {
          const content = await fleet.peekSession(config, node, s.name);
          s.preview = cleanPreview(content);
        } else {
          s.preview = '';
        }
      } catch {
        s.preview = '';
      }
      // Lightweight git summary for card rendering
      try {
        const node = router.nodeFor(s.name);
        const nc = fleet.getNodeConfig(config, s.nodeId);
        const repoDir = s.num ? nc.sessions.repoDir(s.num) : null;
        if (repoDir && node) {
          const topLog = await git.getLog(node, repoDir, 1);
          s.gitSummary = {
            lastCommit: topLog.length ? topLog[0].message : '',
            lastCommitTime: topLog.length ? topLog[0].relative : '',
            totalChanges: (s.git ? s.git.staged + s.git.modified + s.git.untracked : 0),
          };
        }
      } catch {
        // git summary is optional
      }
    }
    return sessions;
  }

  async function sendFleetStatus(ws) {
    const sessions = await getFleetWithPreviews();
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'fleet:status', sessions }));
    }
  }

  async function broadcastFleetStatus() {
    const sessions = await getFleetWithPreviews();
    const msg = JSON.stringify({ type: 'fleet:status', sessions });
    for (const ws of clients) {
      if (ws.readyState === 1) ws.send(msg);
    }
  }

  // Broadcast fleet status every 10s
  const fleetInterval = setInterval(() => {
    broadcastFleetStatus().catch(err => console.error('Fleet broadcast error:', err.message));
  }, 10000);

  // -- Watcher event bridge -----------------------------------------------

  function broadcast(data) {
    const msg = JSON.stringify(data);
    for (const ws of clients) {
      if (ws.readyState === 1) ws.send(msg);
    }
  }

  watcher.on('session:idle', ({ session, name, num }) => {
    broadcast({ type: 'notify', event: 'session:idle', session: num, name });
    broadcastFleetStatus().catch(() => {});
  });

  watcher.on('session:working', ({ session, name, num }) => {
    broadcast({ type: 'notify', event: 'session:working', session: num, name });
  });

  watcher.on('ci:changed', ({ name, num, from, to, pr }) => {
    broadcast({ type: 'notify', event: 'ci:changed', session: num, name, from, to, pr });
    broadcastFleetStatus().catch(() => {});
  });

  // -- TaskQueue event bridge ---------------------------------------------

  if (taskQueue) {
    taskQueue.on('task:created', (task) => broadcast({ type: 'task:created', task }));
    taskQueue.on('task:dispatched', (task) => broadcast({ type: 'task:dispatched', task }));
    taskQueue.on('task:completed', (task) => broadcast({ type: 'task:completed', task }));
    taskQueue.on('task:failed', (task) => broadcast({ type: 'task:failed', task }));
    taskQueue.on('task:cancelled', (task) => broadcast({ type: 'task:cancelled', task }));
    taskQueue.on('task:parked', (task) => broadcast({ type: 'task:parked', task }));
    taskQueue.on('task:unparked', (task) => broadcast({ type: 'task:unparked', task }));
    taskQueue.on('task:updated', (task) => broadcast({ type: 'task:updated', task }));
    taskQueue.on('auto:changed', (sessions) => broadcast({ type: 'auto:status', sessions }));
    taskQueue.on('feed:new', (entry) => broadcast({ type: 'feed:new', entry }));
    taskQueue.on('approval:new', (approval) => broadcast({ type: 'approval:new', approval }));
    taskQueue.on('approval:resolved', (approval) => broadcast({ type: 'approval:resolved', approval }));
    taskQueue.on('rules:changed', (rules) => broadcast({ type: 'rules:list', rules }));
    taskQueue.on('designations:changed', (designations) => broadcast({ type: 'designations:status', designations }));
    taskQueue.on('vim:changed', (enabled) => broadcast({ type: 'vim:status', enabled }));
    taskQueue.on('spawn:config:changed', ({ min, max, tmuxDir }) => broadcast({ type: 'spawn:config', min, max, tmuxDir }));
    taskQueue.on('spawn:log', (message) => broadcast({ type: 'spawn:log', message }));
  }

  if (pmManager) {
    pmManager.on('pm:error', (data) => broadcast({ type: 'pm:error', id: data.id, error: data.error }));
    pmManager.on('pm:changed', () => broadcast({ type: 'pm:list', pms: pmManager.getAll() }));
  }

  // -- Start server -------------------------------------------------------

  server.listen(port, () => {
    console.log(`Web dashboard: http://localhost:${port}`);
    if (workerSecret) {
      console.log(`Worker registration enabled (workers connect to ws://localhost:${port})`);
    }
  });

  // Cleanup helper
  server.on('close', () => {
    clearInterval(fleetInterval);
    for (const ws of clients) {
      clearTermSub(ws);
      ws.close();
    }
    clients.clear();
    // Clean up worker connections
    for (const [ws, node] of workers) {
      node.disconnect();
      router.removeNode(node.id);
      ws.close();
    }
    workers.clear();
  });

  return { app, server, wss };
}

module.exports = { createWebServer };
