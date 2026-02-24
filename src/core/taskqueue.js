const EventEmitter = require('events');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { execSync, spawn: spawnChild } = require('child_process');
const relay = require('./relay');
const fleet = require('./fleet');

const STATE_FILE = path.join(__dirname, '..', '..', '.hive-state.json');

// Task statuses
const TASK = {
  QUEUED: 'queued',
  DISPATCHED: 'dispatched',
  COMPLETED: 'completed',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
  PARKED: 'parked',
};

// Approval statuses
const APPROVAL = {
  PENDING: 'pending',
  APPROVED: 'approved',
  DENIED: 'denied',
};

let nextTaskId = 1;
let nextApprovalId = 1;

class TaskQueue extends EventEmitter {
  constructor(config, watcher, router) {
    super();
    this.config = config;
    this.watcher = watcher;
    this.router = router;

    // State
    this.tasks = new Map();           // id -> Task
    this.autoSessions = new Set();    // session numbers opted into auto-mode
    this.designations = new Map();    // session num -> designation string
    this.feed = [];                   // ring buffer, max 200
    this.approvals = new Map();       // id -> Approval
    this.dispatchLock = new Set();    // session numbers currently being dispatched to
    this.activeTaskBySession = new Map(); // session num -> task id
    this.spawnedAgents = new Map();  // slot num -> { repoDir, name }
    this.vimMode = false;

    // Auto-pilot rules
    this.rules = [
      { id: 'ci-fail-fix', name: 'Auto-fix CI failures', enabled: false,
        trigger: 'ci:fail', action: 'dispatch-fix' },
      { id: 'review-changes', name: 'Auto-address review changes', enabled: false,
        trigger: 'review:changes_requested', action: 'dispatch-fix' },
      { id: 'idle-next-task', name: 'Auto-pick next task on idle', enabled: true,
        trigger: 'session:idle', action: 'auto-dispatch' },
    ];

    // Load persisted state
    this._loadState();

    // Wire watcher events
    this._wireWatcher();

    // Delayed dispatch after startup -- give sessions time to boot (60s)
    // then check once. Ongoing dispatch is event-driven (session:idle, designation change, etc.)
    setTimeout(() => {
      this._tryAutoDispatch().catch(err => console.error('Auto-dispatch error:', err.message));
    }, 60000);
  }

  // -- Task lifecycle -----------------------------------------------

  createTask(text, mode, targetSession, designation, meta) {
    const task = {
      id: String(nextTaskId++),
      text,
      mode, // 'auto' or 'manual'
      targetSession: targetSession || null,
      designation: designation || null,
      status: TASK.QUEUED,
      assignedTo: null,
      createdAt: Date.now(),
      dispatchedAt: null,
      completedAt: null,
      result: null,
      source: (meta && meta.source) || null,   // e.g. 'ci-fail', 'review-changes'
      sourcePR: (meta && meta.pr) || null,      // PR number that triggered this
      sourceSession: (meta && meta.session) || null, // session that triggered this
    };
    this.tasks.set(task.id, task);
    this.emit('task:created', task);
    this.pushFeed('task', null, `Task created: "${text}" (${mode})`);

    if (mode === 'manual' && targetSession) {
      this._dispatchTask(task, targetSession).catch(err =>
        console.error('Dispatch error:', err.message));
    } else if (mode === 'auto') {
      // Try to dispatch immediately to an idle auto-session
      this._tryAutoDispatch().catch(err =>
        console.error('Auto-dispatch error:', err.message));
    }

    return task;
  }

  /**
   * Attach a tracking task to an already-working session.
   * No dispatch, no /clear, no relay — just bookkeeping.
   */
  attachTask(text, sessionNum, meta) {
    const task = {
      id: String(nextTaskId++),
      text,
      mode: 'manual',
      targetSession: sessionNum,
      designation: null,
      status: TASK.DISPATCHED,
      assignedTo: sessionNum,
      createdAt: Date.now(),
      dispatchedAt: Date.now(),
      lastActivityAt: Date.now(),
      completedAt: null,
      result: null,
      source: (meta && meta.source) || 'attached',
      sourcePR: (meta && meta.pr) || null,
      sourceSession: sessionNum,
    };
    this.tasks.set(task.id, task);
    this.activeTaskBySession.set(sessionNum, task.id);
    this.dispatchLock.add(sessionNum);
    this.emit('task:created', task);
    this.emit('task:dispatched', task);
    this.pushFeed('task', sessionNum, `Task attached to session ${sessionNum}: "${text}"`);
    this._saveState();
    return task;
  }

  updateTask(taskId, updates) {
    const task = this.tasks.get(taskId);
    if (!task || task.status !== TASK.QUEUED) return null;

    const allowed = ['text', 'mode', 'targetSession', 'designation'];
    for (const key of allowed) {
      if (key in updates) task[key] = updates[key];
    }
    this.emit('task:updated', task);
    this._saveState();
    return task;
  }

  cancelTask(taskId) {
    const task = this.tasks.get(taskId);
    if (!task || task.status === TASK.COMPLETED || task.status === TASK.FAILED) return null;

    if (task.status === TASK.DISPATCHED && task.assignedTo) {
      this.activeTaskBySession.delete(task.assignedTo);
    }
    task.status = TASK.CANCELLED;
    this.emit('task:cancelled', task);
    this.pushFeed('task', task.assignedTo, `Task cancelled: "${task.text}"`);
    return task;
  }

  completeTask(taskId, result, snapshot, snapshotCols) {
    const task = this.tasks.get(taskId);
    if (!task || task.status !== TASK.DISPATCHED) return null;

    task.status = TASK.COMPLETED;
    task.completedAt = Date.now();
    task.result = result || null;
    task.snapshot = snapshot || null;
    task.snapshotCols = snapshotCols || 0;

    if (task.assignedTo) {
      this.activeTaskBySession.delete(task.assignedTo);
      this.dispatchLock.delete(task.assignedTo);
    }

    const duration = task.dispatchedAt
      ? Math.round((task.completedAt - task.dispatchedAt) / 60000)
      : 0;
    this.emit('task:completed', task);
    this.pushFeed('task', task.assignedTo,
      `Task completed: "${task.text}" (${duration}m)`);
    this._saveState();
    // Dispatch next queued task now that a session is free
    this._tryAutoDispatch().catch(err =>
      console.error('Auto-dispatch error:', err.message));
    return task;
  }

  failTask(taskId, error) {
    const task = this.tasks.get(taskId);
    if (!task || task.status !== TASK.DISPATCHED) return null;

    task.status = TASK.FAILED;
    task.completedAt = Date.now();
    task.result = error;

    if (task.assignedTo) {
      this.activeTaskBySession.delete(task.assignedTo);
      this.dispatchLock.delete(task.assignedTo);
    }

    this.emit('task:failed', task);
    this.pushFeed('task', task.assignedTo,
      `Task failed: "${task.text}" -- ${error}`);
    return task;
  }

  parkTask(taskId, snapshot, snapshotCols) {
    const task = this.tasks.get(taskId);
    if (!task || task.status !== TASK.DISPATCHED) return null;

    task.status = TASK.PARKED;
    task.parkedAt = Date.now();
    task.parkedFrom = task.assignedTo;
    task.snapshot = snapshot || null;
    task.snapshotCols = snapshotCols || 0;

    if (task.assignedTo) {
      this.activeTaskBySession.delete(task.assignedTo);
      this.dispatchLock.delete(task.assignedTo);
    }
    task.assignedTo = null;

    this.emit('task:parked', task);
    this.pushFeed('task', task.parkedFrom,
      `Task parked: "${task.text}" (was session ${task.parkedFrom})`);
    this._saveState();
    // Session is free now — try dispatching queued tasks
    this._tryAutoDispatch().catch(err =>
      console.error('Auto-dispatch error:', err.message));
    return task;
  }

  unparkTask(taskId) {
    const task = this.tasks.get(taskId);
    if (!task || task.status !== TASK.PARKED) return null;

    task.status = TASK.QUEUED;
    task.unparkedAt = Date.now();

    this.emit('task:unparked', task);
    this.pushFeed('task', null, `Task unparked: "${task.text}"`);
    this._saveState();
    // Try auto-dispatching the freshly unparked task
    this._tryAutoDispatch().catch(err =>
      console.error('Auto-dispatch error:', err.message));
    return task;
  }

  /**
   * Manually dispatch a queued or parked task.
   * If sessionNum is provided, dispatch to that session.
   * Otherwise, find any idle session.
   */
  async startTask(taskId, sessionNum) {
    const task = this.tasks.get(taskId);
    if (!task || (task.status !== TASK.QUEUED && task.status !== TASK.PARKED)) return null;

    // If parked, move back to queued first
    if (task.status === TASK.PARKED) {
      task.status = TASK.QUEUED;
      task.unparkedAt = Date.now();
    }

    if (sessionNum) {
      const dispatched = await this._dispatchTask(task, sessionNum);
      if (!dispatched) throw new Error(`Session ${sessionNum} is not idle or not found`);
      return task;
    }

    // No session specified — find any idle session
    const sessions = await fleet.getFleetStatus(this.config, this.router);
    const idle = sessions.filter(s =>
      s.state === 'idle'
      && !this.dispatchLock.has(s.num)
      && !this.activeTaskBySession.has(s.num)
    );
    if (!idle.length) throw new Error('No idle sessions available');

    const dispatched = await this._dispatchTask(task, idle[0].num);
    if (!dispatched) throw new Error('Dispatch failed — session may have become busy');
    return task;
  }

  async _dispatchTask(task, sessionNum) {
    if (this.dispatchLock.has(sessionNum)) return false;

    const found = await fleet.findSession(this.config, this.router, sessionNum);
    if (!found) {
      this.failTask(task.id, `Session ${sessionNum} not found`);
      return false;
    }

    const { name: sessionName, nodeId } = found;
    const node = this.router.getNode(nodeId);

    // Double-check session is actually idle right now (fresh read)
    const sessions = await fleet.getFleetStatus(this.config, this.router);
    const session = sessions.find(s => s.num === sessionNum);
    if (!session || session.state !== 'idle') {
      return false; // silently skip -- don't fail the task, just don't dispatch yet
    }

    this.dispatchLock.add(sessionNum);
    task.status = TASK.DISPATCHED;
    task.assignedTo = sessionNum;
    task.dispatchedAt = Date.now();
    task.lastActivityAt = Date.now();
    this.activeTaskBySession.set(sessionNum, task.id);

    this.emit('task:dispatched', task);
    this.pushFeed('task', sessionNum,
      `Task dispatched to session ${sessionNum}: "${task.text}"`);
    this._saveState();

    // Fire-and-forget: send the task text to Claude
    // For auto-dispatched tasks, clear context first so the agent starts fresh
    const sendTask = async () => {
      if (task.mode === 'auto') {
        const clearResult = await relay.tell(this.config, node, sessionName, '/clear', { vimMode: this.vimMode });
        if (clearResult.success) {
          await new Promise(r => setTimeout(r, 2500));
        }
      }
      return relay.tell(this.config, node, sessionName, task.text, { vimMode: this.vimMode });
    };

    sendTask().then((result) => {
      if (!result.success) {
        this.failTask(task.id, result.error || 'Tell failed');
      }
      // Don't unlock dispatchLock here -- wait for session to go idle
    }).catch((err) => {
      this.failTask(task.id, err.message);
    });

    return true;
  }

  _handleSessionIdle(num, preview, paneCols) {
    // Complete active task for this session
    const taskId = this.activeTaskBySession.get(num);
    if (taskId) {
      this.completeTask(taskId, null, preview || null, paneCols);
    }
    this.dispatchLock.delete(num);
  }

  async _tryAutoDispatch() {
    const queuedTasks = Array.from(this.tasks.values())
      .filter(t => t.status === TASK.QUEUED && t.mode === 'auto');
    if (!queuedTasks.length) return;

    const sessions = await fleet.getFleetStatus(this.config, this.router);
    const idleAuto = sessions.filter(s =>
      s.state === 'idle'
      && this.autoSessions.has(s.num)
      && !this.dispatchLock.has(s.num)
      && !this.activeTaskBySession.has(s.num)
    );
    if (!idleAuto.length) return;

    // Dispatch one task per idle session (not all at once)
    for (const session of idleAuto) {
      const task = queuedTasks.find(t => {
        if (t.status !== TASK.QUEUED) return false;
        if (t.designation) {
          return this.designations.get(session.num) === t.designation;
        }
        return true; // no designation -- any session
      });
      if (task) {
        await this._dispatchTask(task, session.num);
      }
    }
  }

  // -- Auto-mode ----------------------------------------------------

  toggleAutoSession(num) {
    if (this.autoSessions.has(num)) {
      this.autoSessions.delete(num);
    } else {
      this.autoSessions.add(num);
    }
    this._saveState();
    this.emit('auto:changed', this.getAutoSessions());
    // Re-evaluate dispatch with new auto-session set
    this._tryAutoDispatch().catch(err =>
      console.error('Auto-dispatch error:', err.message));
    return this.autoSessions.has(num);
  }

  setAutoSessions(nums) {
    this.autoSessions.clear();
    for (const n of nums) this.autoSessions.add(n);
    this._saveState();
    this.emit('auto:changed', this.getAutoSessions());
  }

  getAutoSessions() {
    return Array.from(this.autoSessions).sort((a, b) => a - b);
  }

  // -- VIM mode -----------------------------------------------------

  setVimMode(enabled) {
    this.vimMode = !!enabled;
    this._saveState();
    this.emit('vim:changed', this.vimMode);
  }

  // -- Designations -------------------------------------------------

  setDesignation(num, designation) {
    if (designation) {
      this.designations.set(num, designation);
    } else {
      this.designations.delete(num);
    }
    this._saveState();
    this.emit('designations:changed', this.getDesignations());
    // Re-evaluate dispatch with new designation mapping
    this._tryAutoDispatch().catch(err =>
      console.error('Auto-dispatch error:', err.message));
  }

  getDesignations() {
    const obj = {};
    for (const [num, des] of this.designations) obj[num] = des;
    return obj;
  }

  // -- Spawn -------------------------------------------------------

  async getAvailableSlots() {
    const sessions = await fleet.getFleetStatus(this.config, this.router);
    const occupied = new Set(sessions.map(s => s.num));
    const slots = [];
    for (let i = 17; i <= 32; i++) {
      if (!occupied.has(i)) slots.push(i);
    }
    return slots;
  }

  getSpawnedAgent(num) {
    return this.spawnedAgents.get(num) || null;
  }

  getRepoDir(num) {
    const spawned = this.spawnedAgents.get(num);
    if (spawned) return spawned.repoDir;
    return this.config.sessions.repoDir(num);
  }

  async spawnSession({ num, baseDir, name, gitUrl, skipPermissions } = {}) {
    if (!name) throw new Error('Agent name is required');

    // Resolve base directory
    baseDir = (baseDir || '~/Coding').replace(/^~/, os.homedir());

    // Pick slot
    if (num === undefined || num === null) {
      const slots = await this.getAvailableSlots();
      if (!slots.length) throw new Error('No available slots (17-32 all occupied)');
      num = slots[0];
    }
    if (num < 17 || num > 32) throw new Error('Spawn slots must be 17-32');

    const sessions = await fleet.getFleetStatus(this.config, this.router);
    const existing = sessions.find(s => s.num === num);

    // Build repo path: baseDir/name+num (e.g. ~/Coding/ios17)
    const repoDir = path.join(baseDir, `${name}${num}`);

    // Re-spawn: if session exists in tmux, reuse it
    if (existing) {
      const prev = this.spawnedAgents.get(num);
      if (prev && prev.repoDir === repoDir) {
        // Same slot, same dir — already running, just update metadata
        this.pushFeed('state', num, `Agent "${name}" already running in slot ${num}`);
        return { num, repoDir };
      }
      // Different agent in this slot — can't overwrite a live session
      throw new Error(`Slot ${num} is occupied by "${existing.name}". Kill it first or pick another slot.`);
    }

    // Prepare directory (skip if it already exists from a previous spawn)
    const dirExists = fs.existsSync(repoDir);
    if (!dirExists) {
      if (gitUrl) {
        try {
          execSync(`git clone ${gitUrl} "${repoDir}"`, { timeout: 60000, stdio: 'pipe' });
        } catch (err) {
          throw new Error(`Git clone failed: ${err.message}`);
        }
      } else {
        try {
          fs.mkdirSync(repoDir, { recursive: true });
        } catch (err) {
          throw new Error(`Failed to create directory: ${err.message}`);
        }
      }
    }

    // Start tmux session in background using agent.yml template
    const agentYml = path.join(os.homedir(), 'dev', 'agents', 'tmux', 'agent.yml');
    const env = { ...process.env };
    if (skipPermissions) env.SKIP_PERMISSIONS = '1';

    try {
      const child = spawnChild('tmuxinator', ['start', agentYml, `N=${num}`, `ROOT=${repoDir}`], {
        stdio: 'ignore',
        detached: true,
        env,
      });
      child.unref();
    } catch (err) {
      throw new Error(`Failed to start session ${num}: ${err.message}`);
    }

    // Register spawned agent
    this.spawnedAgents.set(num, { repoDir, name });
    this._saveState();

    // Wait for init then rename (in background, don't block response)
    setTimeout(() => {
      try {
        const renameScript = path.join(os.homedir(), 'dev', 'agents', 'tmux', 'rename.sh');
        execSync(`bash "${renameScript}"`, { timeout: 10000, stdio: 'pipe' });
      } catch {
        // Rename is best-effort
      }
    }, 2000);

    const action = dirExists ? 're-spawned (reused directory)' : 'spawned';
    this.pushFeed('state', num, `Agent "${name}" ${action} in slot ${num}`);
    return { num, repoDir };
  }

  // -- Broadcast ----------------------------------------------------

  async broadcast(message, target, specificSessions) {
    const sessions = await fleet.getFleetStatus(this.config, this.router);
    let targets;

    if (specificSessions && specificSessions.length) {
      targets = sessions.filter(s => specificSessions.includes(s.num));
    } else if (target === 'idle') {
      targets = sessions.filter(s => s.state === 'idle');
    } else if (target === 'working') {
      targets = sessions.filter(s => s.state === 'working');
    } else {
      targets = sessions.filter(s => s.state !== 'off');
    }

    let sent = 0, failed = 0;
    for (const s of targets) {
      try {
        const node = this.router.nodeFor(s.name);
        if (!node) { failed++; continue; }
        const result = await relay.tell(this.config, node, s.name, message, { vimMode: this.vimMode });
        if (result.success) sent++;
        else failed++;
      } catch {
        failed++;
      }
    }

    this.pushFeed('broadcast', null,
      `Broadcast to ${target || 'all'}: "${message}" (${sent} sent, ${failed} failed)`);
    return { sent, failed };
  }

  // -- Approvals ----------------------------------------------------

  createApproval(sessionNum, prompt) {
    // Don't create duplicate pending approvals for the same session
    for (const a of this.approvals.values()) {
      if (a.session === sessionNum && a.status === APPROVAL.PENDING) return a;
    }

    const approval = {
      id: String(nextApprovalId++),
      session: sessionNum,
      prompt,
      status: APPROVAL.PENDING,
      createdAt: Date.now(),
      resolvedAt: null,
    };
    this.approvals.set(approval.id, approval);
    this.emit('approval:new', approval);
    this.pushFeed('approval', sessionNum, `Approval requested: "${prompt}"`);
    return approval;
  }

  async resolveApproval(approvalId, approved) {
    const approval = this.approvals.get(approvalId);
    if (!approval || approval.status !== APPROVAL.PENDING) return null;

    approval.status = approved ? APPROVAL.APPROVED : APPROVAL.DENIED;
    approval.resolvedAt = Date.now();

    // Send y or n key to the session
    const found = await fleet.findSession(this.config, this.router, approval.session);
    if (found) {
      const { name: sessionName, nodeId } = found;
      const node = this.router.getNode(nodeId);
      if (node) {
        const paneTarget = `${sessionName}:.${this.config.sessions.claudePane}`;
        const key = approved ? 'y' : 'n';
        await node.exec(`tmux send-keys -t "${paneTarget}" ${key}`);
      }
    }

    this.emit('approval:resolved', approval);
    this.pushFeed('approval', approval.session,
      `Approval ${approved ? 'approved' : 'denied'}: "${approval.prompt}"`);
    return approval;
  }

  getPendingApprovals() {
    return Array.from(this.approvals.values())
      .filter(a => a.status === APPROVAL.PENDING);
  }

  // -- Feed ---------------------------------------------------------

  pushFeed(type, session, detail, extra) {
    const entry = {
      id: Date.now() + '-' + Math.random().toString(36).slice(2, 6),
      type, // 'state' | 'task' | 'ci' | 'approval' | 'broadcast' | 'user'
      session,
      detail,
      timestamp: Date.now(),
      ...extra,
    };

    this.feed.push(entry);
    if (this.feed.length > 200) this.feed.shift();

    this.emit('feed:new', entry);
    return entry;
  }

  getFeed(before, limit = 50) {
    let entries = this.feed;
    if (before) {
      const idx = entries.findIndex(e => e.id === before);
      if (idx > 0) entries = entries.slice(0, idx);
    }
    const slice = entries.slice(-limit);
    return {
      entries: slice,
      hasMore: entries.length > slice.length,
    };
  }

  // -- Auto-pilot rules --------------------------------------------

  getRules() {
    return this.rules;
  }

  toggleRule(ruleId) {
    const rule = this.rules.find(r => r.id === ruleId);
    if (rule) {
      rule.enabled = !rule.enabled;
      this._saveState();
      this.emit('rules:changed', this.rules);
    }
    return rule;
  }

  evaluateRules(trigger, data) {
    for (const rule of this.rules) {
      if (!rule.enabled || rule.trigger !== trigger) continue;

      switch (rule.action) {
        case 'auto-dispatch':
          this._tryAutoDispatch().catch(err =>
            console.error('Auto-dispatch error:', err.message));
          break;

        case 'dispatch-fix':
          if (data && data.num && this.autoSessions.has(data.num)) {
            const pr = data.pr ? ` PR #${data.pr}` : '';
            const message = trigger === 'ci:fail'
              ? `CI failed on${pr}. Run /ci-status ${data.pr || ''} to see failures, then fix them.`
              : `Review changes requested on${pr}. Check the PR review comments and address the feedback.`;
            this.createTask(message, 'manual', data.num, null, {
              source: trigger === 'ci:fail' ? 'ci-fail' : 'review-changes',
              pr: data.pr || null,
              session: data.num,
            });
          }
          break;
      }
    }
  }

  // -- Persistence ---------------------------------------------------

  _loadState() {
    try {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      if (Array.isArray(data.autoSessions)) {
        for (const n of data.autoSessions) this.autoSessions.add(n);
      }
      if (Array.isArray(data.rules)) {
        for (const saved of data.rules) {
          const rule = this.rules.find(r => r.id === saved.id);
          if (rule) rule.enabled = saved.enabled;
        }
      }
      if (data.designations && typeof data.designations === 'object') {
        for (const [num, des] of Object.entries(data.designations)) {
          this.designations.set(Number(num), des);
        }
      }
      if (data.spawnedAgents && typeof data.spawnedAgents === 'object') {
        for (const [num, info] of Object.entries(data.spawnedAgents)) {
          this.spawnedAgents.set(Number(num), info);
        }
      }
      if (data.vimMode !== undefined) this.vimMode = data.vimMode;
      // Restore tasks
      if (Array.isArray(data.tasks)) {
        for (const t of data.tasks) {
          // Preserve dispatched tasks and their session assignments across restarts.
          // The session is still running in tmux — don't reset to queued or send /clear.
          if (t.status === TASK.DISPATCHED && t.assignedTo) {
            this.activeTaskBySession.set(t.assignedTo, t.id);
            this.dispatchLock.add(t.assignedTo);
          }
          this.tasks.set(t.id, t);
          if (Number(t.id) >= nextTaskId) nextTaskId = Number(t.id) + 1;
        }
      }
      console.log(`Loaded state: ${this.autoSessions.size} auto-sessions, ${this.designations.size} designations, ${this.spawnedAgents.size} spawned agents`);
      if (this.tasks.size) console.log(`Restored ${this.tasks.size} tasks`);
    } catch {
      // No state file yet -- that's fine
    }
  }

  _saveState() {
    const spawnedObj = {};
    for (const [num, info] of this.spawnedAgents) spawnedObj[num] = info;
    // Persist active tasks (queued + dispatched + parked, not completed/cancelled/failed)
    const tasksArr = Array.from(this.tasks.values())
      .filter(t => t.status === TASK.QUEUED || t.status === TASK.DISPATCHED || t.status === TASK.PARKED)
      .map(t => ({ ...t }));
    const data = {
      autoSessions: Array.from(this.autoSessions),
      rules: this.rules.map(r => ({ id: r.id, enabled: r.enabled })),
      designations: this.getDesignations(),
      spawnedAgents: spawnedObj,
      tasks: tasksArr,
      vimMode: this.vimMode,
    };
    // Merge PM data if pmManager is attached
    if (this._pmManager) {
      data.pms = this._pmManager.serialize();
    }
    try {
      fs.writeFileSync(STATE_FILE, JSON.stringify(data, null, 2));
    } catch (err) {
      console.error('Failed to save state:', err.message);
    }
  }

  // -- Watcher integration ------------------------------------------

  _wireWatcher() {
    this.watcher.on('session:idle', (data) => {
      // Include terminal preview so feed entries show what Claude finished / is asking
      const extra = {};
      if (data.preview) {
        // Grab last ~20 lines of meaningful content for the feed
        const lines = data.preview.split('\n');
        extra.preview = lines.slice(-20).join('\n');
      }
      this.pushFeed('state', data.num, `Session ${data.num} went idle`, extra);
      this._handleSessionIdle(data.num, data.ansiSnapshot || data.preview, data.paneCols);

      // Evaluate auto-pilot rules
      this.evaluateRules('session:idle', data);
    });

    // Note: task completion is handled by 'session:idle' event above,
    // which now requires two consecutive polls confirming idle state.
    // No periodic fallback needed — the watcher confirmation prevents
    // false positives from brief idle flickers between tool calls.

    this.watcher.on('session:working', (data) => {
      this.pushFeed('state', data.num, `Session ${data.num} started working`);
      // Update lastActivityAt on the active task for this session
      const taskId = this.activeTaskBySession.get(data.num);
      if (taskId) {
        const task = this.tasks.get(taskId);
        if (task) {
          task.lastActivityAt = Date.now();
          this.emit('task:updated', task);
        }
      }
    });

    this.watcher.on('ci:changed', (data) => {
      const label = data.to === 'SUCCESS' ? 'PASS' : data.to === 'FAILURE' ? 'FAIL' : data.to;
      this.pushFeed('ci', data.num,
        `CI ${label} -- session ${data.num} PR#${data.pr}`);

      if (data.to === 'FAILURE') {
        this.evaluateRules('ci:fail', data);
      }
    });

    this.watcher.on('approval:requested', (data) => {
      this.createApproval(data.num, data.prompt);
    });

    this.watcher.on('review:changed', (data) => {
      this.pushFeed('ci', data.num,
        `Review: ${data.to} -- session ${data.num} PR#${data.pr}`);

      if (data.to === 'CHANGES_REQUESTED') {
        this.evaluateRules('review:changes_requested', data);
      }
    });
  }

  // -- Serialization (for sending to clients) -----------------------

  getTasksList() {
    return Array.from(this.tasks.values())
      .filter(t => t.status !== TASK.CANCELLED)
      .sort((a, b) => b.createdAt - a.createdAt)
      .map(t => {
        const { snapshot, snapshotCols, ...rest } = t;
        return rest;
      });
  }
}

module.exports = TaskQueue;
module.exports.TASK = TASK;
module.exports.APPROVAL = APPROVAL;
