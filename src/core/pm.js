const EventEmitter = require('events');
const https = require('https');
const http = require('http');

let nextPmId = 1;

class ProjectManager extends EventEmitter {
  constructor(taskQueue) {
    super();
    this.taskQueue = taskQueue;
    this.pms = new Map(); // id → PM config
    this.timers = new Map(); // id → interval handle
    // Let taskQueue know about us so _saveState() includes PM data
    taskQueue._pmManager = this;
  }

  // ── CRUD ────────────────────────────────────────────

  create(cfg) {
    const id = String(nextPmId++);
    const pm = {
      id,
      name: cfg.name || 'Untitled PM',
      source: cfg.source || { type: 'jira', jql: '' },
      designation: cfg.designation || null,
      instructions: cfg.instructions || '',
      autoThreshold: cfg.autoThreshold != null ? cfg.autoThreshold : 3,
      pollInterval: cfg.pollInterval || 60000,
      taskFormat: cfg.taskFormat || null,
      enabled: false,
      seenKeys: [],
      tasksCreated: 0,
      lastPoll: null,
      lastError: null,
    };
    this.pms.set(id, pm);
    this._save();
    this.emit('pm:changed');
    return pm;
  }

  update(id, updates) {
    const pm = this.pms.get(id);
    if (!pm) return null;
    const wasEnabled = pm.enabled;
    for (const [k, v] of Object.entries(updates)) {
      if (k === 'id' || k === 'seenKeys' || k === 'tasksCreated' || k === 'lastPoll' || k === 'lastError') continue;
      pm[k] = v;
    }
    // Restart polling if interval changed or was re-enabled
    if (wasEnabled && pm.enabled) {
      this._stopPolling(id);
      this._startPolling(id);
    }
    this._save();
    this.emit('pm:changed');
    return pm;
  }

  remove(id) {
    this._stopPolling(id);
    this.pms.delete(id);
    this._save();
    this.emit('pm:changed');
  }

  toggle(id) {
    const pm = this.pms.get(id);
    if (!pm) return;
    pm.enabled = !pm.enabled;
    if (pm.enabled) {
      this._startPolling(id);
    } else {
      this._stopPolling(id);
    }
    this._save();
    this.emit('pm:changed');
  }

  getAll() {
    return Array.from(this.pms.values());
  }

  get(id) {
    return this.pms.get(id) || null;
  }

  // ── Serialization ───────────────────────────────────

  serialize() {
    return this.getAll().map(pm => ({
      ...pm,
      seenKeys: pm.seenKeys.slice(-500), // cap at 500
    }));
  }

  loadState(pmsData) {
    if (!Array.isArray(pmsData)) return;
    for (const data of pmsData) {
      const id = data.id || String(nextPmId++);
      if (Number(id) >= nextPmId) nextPmId = Number(id) + 1;
      const pm = {
        id,
        name: data.name || 'Untitled PM',
        source: data.source || { type: 'jira', jql: '' },
        designation: data.designation || null,
        instructions: data.instructions || '',
        autoThreshold: data.autoThreshold != null ? data.autoThreshold : 3,
        pollInterval: data.pollInterval || 60000,
        taskFormat: data.taskFormat || null,
        enabled: data.enabled || false,
        seenKeys: Array.isArray(data.seenKeys) ? data.seenKeys : [],
        tasksCreated: data.tasksCreated || 0,
        lastPoll: data.lastPoll || null,
        lastError: data.lastError || null,
      };
      this.pms.set(id, pm);
      if (pm.enabled) {
        this._startPolling(id);
      }
    }
    console.log(`Loaded ${this.pms.size} project managers`);
  }

  // ── Polling ─────────────────────────────────────────

  _startPolling(id) {
    const pm = this.pms.get(id);
    if (!pm) return;
    this._stopPolling(id); // clear any existing

    // Manual source: create one task immediately, no polling
    if (pm.source.type === 'manual') {
      this._createManualTask(id);
      return;
    }

    // Poll immediately, then on interval
    this._poll(id);
    const interval = setInterval(() => this._poll(id), pm.pollInterval);
    this.timers.set(id, interval);
  }

  _stopPolling(id) {
    const timer = this.timers.get(id);
    if (timer) {
      clearInterval(timer);
      this.timers.delete(id);
    }
  }

  _createManualTask(id) {
    const pm = this.pms.get(id);
    if (!pm) return;

    const text = (pm.source.text || '').trim();
    if (!text) {
      pm.lastError = 'No task text provided';
      this._save();
      this.emit('pm:changed');
      return;
    }

    const key = `manual-${id}-${Date.now()}`;
    pm.seenKeys.push(key);
    const mode = 'manual'; // manual tasks always go to manual queue
    let taskText = text;
    if (pm.taskFormat) {
      taskText = pm.taskFormat.replace('{key}', key).replace('{summary}', text);
    }
    const fullText = pm.instructions ? `${taskText}\n\nInstructions: ${pm.instructions}` : taskText;
    this.taskQueue.createTask(fullText, mode, null, pm.designation);
    pm.tasksCreated++;
    pm.lastPoll = Date.now();
    pm.lastError = null;

    // Auto-disable after creating the task
    pm.enabled = false;
    this._stopPolling(id);

    this.taskQueue.pushFeed('task', null, `PM "${pm.name}" created manual task`);
    this._save();
    this.emit('pm:changed');
  }

  async _poll(id) {
    const pm = this.pms.get(id);
    if (!pm || !pm.enabled) return;

    try {
      let issues;
      switch (pm.source.type) {
        case 'jira':    issues = await this._fetchJira(pm.source); break;
        case 'github-issues': issues = await this._fetchGithubIssues(pm.source); break;
        case 'github-prs':   issues = await this._fetchGithubPrs(pm.source); break;
        case 'jenkins': issues = await this._fetchJenkins(pm.source); break;
        case 'zoho':    issues = await this._fetchZoho(pm.source); break;
        default: throw new Error(`Unsupported source type: ${pm.source.type}`);
      }
      pm.lastPoll = Date.now();
      pm.lastError = null;

      const seenSet = new Set(pm.seenKeys);
      let created = 0;

      for (const issue of issues) {
        if (seenSet.has(issue.key)) continue;

        // Add to seen
        pm.seenKeys.push(issue.key);
        seenSet.add(issue.key);

        // Evaluate complexity
        const mode = this._evaluateComplexity(issue, pm.autoThreshold);
        let text;
        if (pm.taskFormat) {
          text = pm.taskFormat.replace('{key}', issue.key).replace('{summary}', issue.summary);
        } else {
          text = `[${issue.key}] ${issue.summary}`;
        }
        const fullText = pm.instructions ? `${text}\n\nInstructions: ${pm.instructions}` : text;
        this.taskQueue.createTask(fullText, mode, null, pm.designation);
        created++;
        pm.tasksCreated++;
      }

      // Cap seenKeys at 500 (FIFO)
      if (pm.seenKeys.length > 500) {
        pm.seenKeys = pm.seenKeys.slice(-500);
      }

      if (created > 0) {
        this.taskQueue.pushFeed('task', null,
          `PM "${pm.name}" created ${created} task${created > 1 ? 's' : ''}`);
      }

      this._save();
      this.emit('pm:changed');
    } catch (err) {
      pm.lastPoll = Date.now();
      pm.lastError = err.message;
      this._save();
      this.emit('pm:error', { id, error: err.message });
      this.emit('pm:changed');
    }
  }

  _evaluateComplexity(issue, threshold) {
    const storyPoints = issue.storyPoints;
    const issueType = (issue.issueType || '').toLowerCase();

    if (storyPoints != null && storyPoints !== '') {
      const points = Number(storyPoints);
      return points <= threshold ? 'auto' : 'manual';
    }

    // No story points — decide by issue type
    const autoTypes = ['bug', 'task', 'sub-task', 'subtask', 'pr', 'issue', 'ticket'];
    if (autoTypes.includes(issueType)) return 'auto';
    return 'manual'; // Story, Epic, etc.
  }

  // ── Source integrations ─────────────────────────────

  async _fetchJira(source) {
    const baseUrl = process.env.JIRA_BASE_URL;
    const email = process.env.JIRA_EMAIL;
    const apiToken = process.env.JIRA_API_TOKEN;

    if (!baseUrl || !email || !apiToken) {
      throw new Error('JIRA credentials not configured (JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN)');
    }

    const jql = source.jql || '';
    const fields = 'summary,issuetype,customfield_10016,priority,labels';
    const encodedJql = encodeURIComponent(jql);
    const urlStr = `${baseUrl}/rest/api/2/search?jql=${encodedJql}&fields=${fields}&maxResults=50`;
    const auth = Buffer.from(`${email}:${apiToken}`).toString('base64');

    const data = await this._httpRequest(urlStr, {
      'Authorization': `Basic ${auth}`,
      'Accept': 'application/json',
    });
    return (data.issues || []).map(i => ({
      key: i.key,
      summary: i.fields.summary,
      issueType: i.fields.issuetype ? i.fields.issuetype.name : '',
      storyPoints: i.fields.customfield_10016,
    }));
  }

  _githubHeaders() {
    const token = process.env.GITHUB_TOKEN;
    if (!token) throw new Error('GITHUB_TOKEN not configured');
    return { 'Authorization': `Bearer ${token}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'hive-pm' };
  }

  async _fetchGithubIssues(source) {
    if (!source.repo) throw new Error('GitHub repo not configured');
    const params = new URLSearchParams({ per_page: '50' });
    if (source.labels) params.set('labels', source.labels);
    if (source.state) params.set('state', source.state);
    const urlStr = `https://api.github.com/repos/${source.repo}/issues?${params}`;

    const data = await this._httpRequest(urlStr, this._githubHeaders());
    return (Array.isArray(data) ? data : [])
      .filter(i => !i.pull_request) // exclude PRs from issues endpoint
      .map(i => ({
        key: `${source.repo}#${i.number}`,
        summary: i.title,
        issueType: 'issue',
        storyPoints: null,
      }));
  }

  async _fetchGithubPrs(source) {
    if (!source.repo) throw new Error('GitHub repo not configured');
    const allowedBases = source.base
      ? [source.base]
      : ['main', 'master'];

    const allPrs = [];
    for (const base of allowedBases) {
      const params = new URLSearchParams({ per_page: '50', base });
      if (source.state) params.set('state', source.state);
      if (source.labels) params.set('labels', source.labels);
      const urlStr = `https://api.github.com/repos/${source.repo}/pulls?${params}`;
      const data = await this._httpRequest(urlStr, this._githubHeaders());
      if (Array.isArray(data)) allPrs.push(...data);
    }

    // Double-check: filter out any PRs not targeting allowed bases
    const baseSet = new Set(allowedBases);
    return allPrs
      .filter(pr => baseSet.has(pr.base && pr.base.ref) && !pr.draft)
      .map(pr => ({
        key: `${source.repo}#${pr.number}`,
        summary: pr.title,
        issueType: 'pr',
        storyPoints: null,
      }));
  }

  async _fetchJenkins(source) {
    const baseUrl = process.env.JENKINS_URL;
    const user = process.env.JENKINS_USER;
    const token = process.env.JENKINS_API_TOKEN;
    if (!baseUrl || !user || !token) throw new Error('Jenkins credentials not configured (JENKINS_URL, JENKINS_USER, JENKINS_API_TOKEN)');
    if (!source.jobPath) throw new Error('Jenkins job path not configured');

    const urlStr = `${baseUrl}/job/${source.jobPath}/api/json?tree=builds[number,result,timestamp,url]{0,20}`;
    const auth = Buffer.from(`${user}:${token}`).toString('base64');

    const data = await this._httpRequest(urlStr, {
      'Authorization': `Basic ${auth}`,
      'Accept': 'application/json',
    });
    return (data.builds || [])
      .filter(b => b.result === 'FAILURE')
      .map(b => ({
        key: `jenkins-${b.number}`,
        summary: `Build #${b.number} failed — ${source.jobPath}`,
        issueType: 'bug',
        storyPoints: null,
      }));
  }

  async _fetchZoho(source) {
    const orgId = process.env.ZOHO_DESK_ORG_ID;
    const token = process.env.ZOHO_DESK_API_TOKEN;
    if (!orgId || !token) throw new Error('Zoho Desk credentials not configured (ZOHO_DESK_ORG_ID, ZOHO_DESK_API_TOKEN)');

    let urlStr;
    if (source.query) {
      const params = new URLSearchParams({ searchStr: source.query, limit: '50' });
      if (source.department) params.set('departmentId', source.department);
      if (source.status) params.set('status', source.status);
      urlStr = `https://desk.zoho.com/api/v1/tickets/search?${params}`;
    } else {
      const params = new URLSearchParams({ limit: '50' });
      if (source.department) params.set('departmentId', source.department);
      if (source.status) params.set('status', source.status);
      urlStr = `https://desk.zoho.com/api/v1/tickets?${params}`;
    }

    const data = await this._httpRequest(urlStr, {
      'Authorization': `Zoho-oauthtoken ${token}`,
      'orgId': orgId,
      'Accept': 'application/json',
    });
    const tickets = data.data || data || [];
    return (Array.isArray(tickets) ? tickets : []).map(t => ({
      key: `zoho-${t.ticketNumber || t.id}`,
      summary: t.subject || t.description || '',
      issueType: 'ticket',
      storyPoints: null,
    }));
  }

  _httpRequest(urlStr, headers) {
    return new Promise((resolve, reject) => {
      const url = new URL(urlStr);
      const mod = url.protocol === 'https:' ? https : http;

      const req = mod.get(urlStr, {
        headers: { ...headers },
        timeout: 15000,
      }, (res) => {
        let body = '';
        res.on('data', (chunk) => body += chunk);
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(body));
            } catch (e) {
              reject(new Error(`Invalid JSON response: ${e.message}`));
            }
          } else {
            reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
          }
        });
      });

      req.on('error', (err) => reject(new Error(`Request failed: ${err.message}`)));
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  // ── Helpers ─────────────────────────────────────────

  _save() {
    this.taskQueue._saveState();
  }

  stopAll() {
    for (const id of this.timers.keys()) {
      this._stopPolling(id);
    }
  }
}

module.exports = ProjectManager;
