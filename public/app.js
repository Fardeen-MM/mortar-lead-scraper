/**
 * Mortar Lead Scraper — Client-side UI logic
 */

const app = {
  currentStep: 1,
  uploadId: null,
  jobId: null,
  ws: null,
  leads: [],
  stats: null,
  sortCol: null,
  sortAsc: true,
  config: null,
  wsReconnectAttempts: 0,
  wsReconnectTimer: null,

  // --- Initialization ---

  async init() {
    // Cache frequently used DOM elements
    this.$statScraped = document.getElementById('stat-scraped');
    this.$statDupes = document.getElementById('stat-dupes');
    this.$statNew = document.getElementById('stat-new');
    this.$statEmails = document.getElementById('stat-emails');
    this.$progressBar = document.getElementById('progress-bar');

    this.setupDropZone();
    this.setupBeforeUnload();
    await this.loadConfig();
    this.loadDbStats();

    // Try to restore a previous session from localStorage
    const savedJobId = localStorage.getItem('mortar-jobId');
    if (savedJobId) {
      await this.restoreSession(savedJobId);
    }
  },

  async loadConfig() {
    try {
      const res = await fetch('/api/config');
      this.config = await res.json();
      this.populateDropdowns();
      this.setupEnrichmentToggles();
    } catch (err) {
      console.error('[mortar] Failed to load config:', err);
    }
  },

  populateDropdowns() {
    const stateSelect = document.getElementById('select-state');
    stateSelect.innerHTML = '';

    // Group by country
    const groups = { US: [], CA: [], UK: [], AU: [], EU: [], INTL: [], DIRECTORY: [] };
    const groupLabels = {
      US: 'United States', CA: 'Canada', UK: 'United Kingdom',
      AU: 'Australia', EU: 'Europe', INTL: 'International', DIRECTORY: 'Directories',
    };

    for (const [code, meta] of Object.entries(this.config.states)) {
      const country = meta.country || 'US';
      if (!groups[country]) groups[country] = [];
      groups[country].push({ code, name: meta.name, working: meta.working });
    }

    // Sort each group: working first, then alphabetically by name
    for (const country of Object.keys(groups)) {
      groups[country].sort((a, b) => {
        if (a.working !== b.working) return a.working ? -1 : 1;
        return a.name.localeCompare(b.name);
      });
    }

    // Build optgroups in display order
    for (const country of ['US', 'CA', 'UK', 'AU', 'EU', 'INTL', 'DIRECTORY']) {
      if (!groups[country] || groups[country].length === 0) continue;
      const optgroup = document.createElement('optgroup');
      optgroup.label = groupLabels[country] || country;
      for (const { code, name, working } of groups[country]) {
        const opt = document.createElement('option');
        opt.value = code;
        if (working) {
          opt.textContent = `${code} — ${name}`;
        } else {
          opt.textContent = `${code} — ${name} (unavailable)`;
          opt.disabled = true;
          opt.style.color = '#999';
        }
        optgroup.appendChild(opt);
      }
      stateSelect.appendChild(optgroup);
    }

    // Fetch health status for states (non-blocking)
    this.fetchHealthStatus();

    // Listen for state change to update practice areas and cities
    stateSelect.addEventListener('change', () => { this.onStateChange(); this.updateStateInfo(); });

    // Listen for niche input changes to update waterfall visibility
    const nicheInput = document.getElementById('input-niche');
    if (nicheInput) {
      nicheInput.addEventListener('input', () => {
        const code = document.getElementById('select-state').value;
        const isGoogleScraper = code === 'GOOGLE-PLACES' || code === 'GOOGLE-MAPS';
        if (isGoogleScraper) {
          const nicheVal = nicheInput.value.trim().toLowerCase();
          const isNonLawyer = nicheVal && !/^(lawyers?|law\s*firms?|attorneys?)$/.test(nicheVal);
          this._updateWaterfallVisibility(isNonLawyer);
        }
      });
    }

    // Initialize with first state
    this.onStateChange();
  },

  onStateChange() {
    const code = document.getElementById('select-state').value;
    const stateMeta = this.config.states[code];
    if (!stateMeta) return;

    // Detect if this is a Google scraper (supports niche input)
    const isGoogleScraper = code === 'GOOGLE-PLACES' || code === 'GOOGLE-MAPS';

    // Show/hide niche input
    const nicheGroup = document.getElementById('niche-group');
    if (nicheGroup) nicheGroup.style.display = isGoogleScraper ? '' : 'none';

    // Show/hide person extraction toggle
    const personExtractGroup = document.getElementById('person-extract-group');
    if (personExtractGroup) personExtractGroup.style.display = isGoogleScraper ? '' : 'none';

    // Show/hide practice area (not relevant for Google scrapers)
    const practiceGroup = document.getElementById('practice-group');
    if (practiceGroup) practiceGroup.style.display = isGoogleScraper ? 'none' : '';

    // Switch city input: text input for Google scrapers, dropdown for bar scrapers
    const citySelectGroup = document.getElementById('city-select-group');
    const cityInputGroup = document.getElementById('city-input-group');
    if (citySelectGroup) citySelectGroup.style.display = isGoogleScraper ? 'none' : '';
    if (cityInputGroup) cityInputGroup.style.display = isGoogleScraper ? '' : 'none';

    // Populate city suggestions datalist for Google scrapers
    if (isGoogleScraper) {
      const datalist = document.getElementById('city-suggestions');
      if (datalist) {
        datalist.innerHTML = '';
        for (const city of (stateMeta.defaultCities || [])) {
          const opt = document.createElement('option');
          opt.value = city;
          datalist.appendChild(opt);
        }
      }
    }

    // Update hint text based on scraper type
    const hint = document.getElementById('configure-hint');
    if (hint) {
      hint.textContent = isGoogleScraper
        ? 'Search Google Maps for any business type in any city worldwide.'
        : 'Choose what to scrape. The scraper will search bar association directories for matching attorneys.';
    }

    // Show/hide lawyer-specific waterfall options
    const nicheVal = (document.getElementById('input-niche')?.value || '').trim().toLowerCase();
    const isNonLawyer = isGoogleScraper && nicheVal && !/^(lawyers?|law\s*firms?|attorneys?)$/.test(nicheVal);
    this._updateWaterfallVisibility(isNonLawyer);

    // Update practice areas
    const practiceSelect = document.getElementById('select-practice');
    if (!practiceSelect) return;
    practiceSelect.innerHTML = '<option value="">All practice areas</option>';

    // Deduplicate practice areas (some states have aliases like 'family' and 'family law')
    const seen = new Set();
    for (const area of (stateMeta.practiceAreas || [])) {
      if (seen.has(area)) continue;
      seen.add(area);
      const opt = document.createElement('option');
      opt.value = area;
      opt.textContent = area.charAt(0).toUpperCase() + area.slice(1);
      practiceSelect.appendChild(opt);
    }

    // Update cities dropdown (for bar scrapers)
    const citySelect = document.getElementById('select-city');
    citySelect.innerHTML = '<option value="">All major cities</option>';
    for (const city of (stateMeta.defaultCities || [])) {
      const opt = document.createElement('option');
      opt.value = city;
      opt.textContent = city;
      citySelect.appendChild(opt);
    }
  },

  _updateWaterfallVisibility(isNonLawyer) {
    // Lawyer-specific waterfall toggles
    const lawyerToggles = [
      'toggle-fetch-profiles',
      'toggle-crossref-martindale',
      'toggle-crossref-lawyerscom',
      'toggle-name-lookups',
    ];
    for (const id of lawyerToggles) {
      const el = document.getElementById(id);
      if (el) {
        const group = el.closest('.form-group');
        if (group) {
          group.style.display = isNonLawyer ? 'none' : '';
          if (isNonLawyer) {
            el.checked = false;
          } else {
            // Re-check when showing (restore defaults)
            el.checked = true;
          }
        }
      }
    }
  },

  // --- DB Stats Banner ---

  async loadDbStats() {
    try {
      const res = await fetch('/api/leads/stats');
      if (!res.ok) return;
      const d = await res.json();
      const banner = document.getElementById('db-stats-banner');
      if (!banner) return;
      banner.style.display = '';
      document.getElementById('db-total').textContent = (d.total || 0).toLocaleString();
      document.getElementById('db-email').textContent = (d.withEmail || 0).toLocaleString();
      document.getElementById('db-phone').textContent = (d.withPhone || 0).toLocaleString();
    } catch (err) { console.warn('loadDbStats:', err.message); }
  },

  async updateStateInfo() {
    const code = document.getElementById('select-state').value;
    const infoEl = document.getElementById('db-state-info');
    if (!infoEl || !code) return;
    try {
      const res = await fetch('/api/leads/coverage');
      if (!res.ok) return;
      const data = await res.json();
      const stateData = data.find(s => s.state === code);
      if (stateData) {
        infoEl.textContent = `${code}: ${stateData.total} leads in DB (${stateData.email_pct}% email, ${stateData.phone_pct}% phone)`;
      } else {
        infoEl.textContent = `${code}: 0 leads in DB`;
      }
    } catch (err) { console.warn('updateStateInfo:', err.message); }
  },

  // --- Health Status ---

  async fetchHealthStatus() {
    try {
      const res = await fetch('/api/health');
      if (!res.ok) return;
      const health = await res.json();
      const stateSelect = document.getElementById('select-state');
      for (const opt of stateSelect.options) {
        const status = health[opt.value];
        if (status) {
          const dot = status === 'green' ? '\u{1F7E2}' : status === 'yellow' ? '\u{1F7E1}' : '\u{1F534}';
          opt.textContent = `${dot} ${opt.textContent}`;
          opt.title = status === 'green' ? 'Online — scraper is accessible'
            : status === 'yellow' ? 'Degraded — server responded with errors'
            : 'Offline — server unreachable';
        }
      }
    } catch (err) {
      console.debug('[mortar] Health check failed:', err.message);
    }
  },

  // --- Enrichment Preview ---

  async showEnrichPreview() {
    if (!this.jobId) return;
    const previewEl = document.getElementById('enrich-preview');
    if (!previewEl) return;

    try {
      const res = await fetch(`/api/scrape/${this.jobId}/enrich-preview`);
      if (!res.ok) return;
      const data = await res.json();

      if (data.samples.length === 0) {
        previewEl.innerHTML = '<p class="hint">No leads available for preview.</p>';
        previewEl.classList.remove('hidden');
        return;
      }

      let html = `<h4>Enrichment Preview (${data.enrichableCount}/${data.totalLeads} leads enrichable)</h4><ul>`;
      for (const s of data.samples) {
        const potential = (s.potentialEnrichment || '').replace(/^, /, '').replace(/, $/, '');
        html += `<li><strong>${esc(s.name)}</strong> — ${potential ? 'can find: ' + esc(potential) : 'already complete'}</li>`;
      }
      html += '</ul>';
      previewEl.innerHTML = html;
      previewEl.classList.remove('hidden');
    } catch (err) {
      console.debug('[mortar] Enrich preview failed:', err.message);
    }
  },

  // --- Step Navigation ---

  goToStep(step) {
    // Hide current
    document.getElementById(`step-${this.currentStep}`).classList.remove('active');

    // Update step indicators
    document.querySelectorAll('.step').forEach(el => {
      const s = parseInt(el.dataset.step);
      el.classList.remove('active', 'done');
      if (s === step) el.classList.add('active');
      else if (s < step) el.classList.add('done');
    });

    // Show new
    document.getElementById(`step-${step}`).classList.add('active');
    this.currentStep = step;

    // Step-specific logic
    if (step === 4) this.renderTable();
    if (step === 5) this.renderDownloadPage();
  },

  // --- Step 1: Upload ---

  setupDropZone() {
    const zone = document.getElementById('drop-zone');
    const input = document.getElementById('file-input');

    zone.addEventListener('click', () => input.click());

    zone.addEventListener('dragover', (e) => {
      e.preventDefault();
      zone.classList.add('drag-over');
    });

    zone.addEventListener('dragleave', () => {
      zone.classList.remove('drag-over');
    });

    zone.addEventListener('drop', (e) => {
      e.preventDefault();
      zone.classList.remove('drag-over');
      if (e.dataTransfer.files.length) {
        this.uploadFile(e.dataTransfer.files[0]);
      }
    });

    input.addEventListener('change', () => {
      if (input.files.length) {
        this.uploadFile(input.files[0]);
      }
    });
  },

  async uploadFile(file) {
    if (!file.name.toLowerCase().endsWith('.csv')) {
      this.showNotification('Please upload a CSV file', 'error');
      return;
    }

    // Frontend file size validation (50MB max, matches server limit)
    if (file.size > 50 * 1024 * 1024) {
      this.showNotification('File is too large. Maximum size is 50MB.', 'error');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/upload', { method: 'POST', body: formData });
      const data = await res.json();

      if (!res.ok) {
        console.error('[mortar] Upload failed:', res.status, data);
        this.showNotification(data.error || 'Upload failed', 'error');
        return;
      }

      this.uploadId = data.uploadId;

      const status = document.getElementById('upload-status');
      const message = document.getElementById('upload-message');
      status.classList.remove('hidden');
      message.textContent = `${data.originalName} — ${data.count.toLocaleString()} leads loaded for deduplication`;

      document.getElementById('btn-upload-next').disabled = false;
    } catch (err) {
      console.error('[mortar] Upload network error:', err);
      this.showNotification('Upload failed: ' + err.message, 'error');
    }
  },

  skipUpload() {
    this.uploadId = null;
    this.goToStep(2);
  },

  // --- Step 2: Configure ---

  setupEnrichmentToggles() {
    if (!this.config) return;
    // Disable AI toggle if no API key — but don't show warning yet
    if (!this.config.hasAnthropicKey) {
      const aiToggle = document.getElementById('toggle-ai-fallback');
      aiToggle.disabled = true;
      document.getElementById('label-ai-toggle').classList.add('disabled');
    }
  },

  toggleEnrichment() {
    const enabled = document.getElementById('toggle-enrich').checked;
    const options = document.getElementById('enrich-options');
    if (enabled) {
      options.classList.remove('hidden');
      // Show AI warning only when enrichment is on and no API key
      if (!this.config.hasAnthropicKey) {
        document.getElementById('ai-warning').classList.remove('hidden');
      }
    } else {
      options.classList.add('hidden');
      // Hide AI warning when enrichment is off
      document.getElementById('ai-warning').classList.add('hidden');
    }
  },

  // --- Step 3: Scrape ---

  async startScrape() {
    const state = document.getElementById('select-state').value;
    const practice = document.getElementById('select-practice').value;
    const test = document.getElementById('toggle-test').checked;
    const isGoogleScraper = state === 'GOOGLE-PLACES' || state === 'GOOGLE-MAPS';
    // Google scrapers use text input for city; bar scrapers use dropdown
    const city = isGoogleScraper
      ? (document.getElementById('input-city')?.value || '').trim()
      : document.getElementById('select-city').value;
    const niche = isGoogleScraper ? (document.getElementById('input-niche')?.value || '').trim() : '';
    const personExtract = isGoogleScraper && (document.getElementById('toggle-person-extract')?.checked || false);

    if (!state) {
      this.showNotification('Please select a state', 'error');
      return;
    }

    // Move to scrape step
    this.goToStep(3);

    // Show/hide test mode badge
    const badge = document.getElementById('test-mode-badge');
    if (test) {
      badge.classList.remove('hidden');
    } else {
      badge.classList.add('hidden');
    }

    // Reset heading
    const headingText = niche && !/^lawyers?$/i.test(niche)
      ? `Scraping ${niche}...`
      : 'Scraping...';
    document.getElementById('scrape-heading').textContent = headingText;

    // Gather waterfall options
    const waterfall = {
      masterDbLookup: document.getElementById('toggle-master-db').checked,
      fetchProfiles: document.getElementById('toggle-fetch-profiles').checked,
      crossRefMartindale: document.getElementById('toggle-crossref-martindale').checked,
      crossRefLawyersCom: document.getElementById('toggle-crossref-lawyerscom').checked,
      nameLookups: document.getElementById('toggle-name-lookups').checked,
      emailCrawl: document.getElementById('toggle-email-crawl').checked,
    };

    // Gather enrichment options
    const enrich = document.getElementById('toggle-enrich').checked;
    const enrichOptions = enrich ? {
      deriveWebsite: document.getElementById('toggle-derive-website').checked,
      scrapeWebsite: document.getElementById('toggle-scrape-website').checked,
      findLinkedIn: document.getElementById('toggle-find-linkedin').checked,
      extractWithAI: document.getElementById('toggle-ai-fallback').checked,
    } : {};

    // Clear previous state
    this.leads = [];
    this.stats = null;
    document.getElementById('log-container').innerHTML = '';
    this.$statScraped.textContent = '0';
    this.$statDupes.textContent = '0';
    this.$statNew.textContent = '0';
    this.$statEmails.textContent = '0';
    this.$progressBar.style.width = '0%';
    this.$progressBar.classList.remove('progress-bar-cancelled');
    document.getElementById('btn-view-results').disabled = true;
    document.getElementById('btn-stop-scrape').hidden = false;
    document.getElementById('btn-stop-scrape').disabled = false;
    document.getElementById('btn-stop-scrape').textContent = 'Stop Scrape';
    document.getElementById('waterfall-section').classList.add('hidden');
    document.getElementById('waterfall-progress-bar').style.width = '0%';
    document.getElementById('person-extract-section').classList.add('hidden');
    document.getElementById('person-extract-progress-bar').style.width = '0%';
    document.getElementById('enrichment-section').classList.add('hidden');
    document.getElementById('enrich-progress-bar').style.width = '0%';

    // Start the job
    try {
      const res = await fetch('/api/scrape/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ state, practice, city, test, uploadId: this.uploadId, enrich, enrichOptions, waterfall, emailScrape: waterfall.emailCrawl, niche: niche || undefined, personExtract }),
      });
      const data = await res.json();

      if (!res.ok) {
        this.addLog('error', data.error || 'Failed to start scrape');
        document.getElementById('btn-stop-scrape').hidden = true;
        return;
      }

      this.jobId = data.jobId;
      localStorage.setItem('mortar-jobId', this.jobId);
      console.debug('[mortar] Job started:', this.jobId);
      this.connectWebSocket();
    } catch (err) {
      console.error('[mortar] Start scrape error:', err);
      this.addLog('error', 'Failed to start scrape: ' + err.message);
      document.getElementById('btn-stop-scrape').hidden = true;
    }
  },

  // --- Session Restore ---

  async restoreSession(jobId) {
    console.debug('[mortar] Attempting session restore for', jobId);
    try {
      const res = await fetch(`/api/scrape/${jobId}/status`);
      if (!res.ok) {
        console.debug('[mortar] Session restore: job gone (status', res.status + ')');
        localStorage.removeItem('mortar-jobId');
        return;
      }

      const data = await res.json();
      console.debug('[mortar] Session restore: job status =', data.status, 'leads =', data.leadCount);
      this.jobId = data.jobId;
      this.leads = data.leads || [];

      if (data.status === 'running') {
        // Restore to Step 3 and reconnect WS
        this.goToStep(3);
        document.getElementById('scrape-heading').textContent = 'Scraping...';
        this.$statNew.textContent = this.leads.length.toLocaleString();
        document.getElementById('btn-stop-scrape').hidden = false;
        if (data.test) {
          document.getElementById('test-mode-badge').classList.remove('hidden');
        }
        this.connectWebSocket();
        this.addLog('info', 'Session restored — reconnected to running scrape');
      } else if (data.status === 'complete' || data.status === 'cancelled') {
        // Restore to Step 4 (preview) with leads and stats
        this.stats = data.stats;
        this.goToStep(4);
      } else {
        // Error or unknown status — clear
        localStorage.removeItem('mortar-jobId');
      }
    } catch (err) {
      console.error('[mortar] Session restore failed:', err);
      localStorage.removeItem('mortar-jobId');
    }
  },

  // --- Cancel Scrape ---

  async cancelScrape() {
    if (!this.jobId) return;

    const btn = document.getElementById('btn-stop-scrape');
    btn.disabled = true;
    btn.textContent = 'Stopping...';

    try {
      console.debug('[mortar] Cancelling job:', this.jobId);
      const res = await fetch(`/api/scrape/${this.jobId}/cancel`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json();
        console.error('[mortar] Cancel failed:', res.status, data);
        this.showNotification(data.error || 'Failed to cancel', 'error');
        btn.disabled = false;
        btn.textContent = 'Stop Scrape';
      }
      // Success — wait for cancelled-complete WS message
    } catch (err) {
      console.error('[mortar] Cancel network error:', err);
      this.showNotification('Failed to cancel: ' + err.message, 'error');
      btn.disabled = false;
      btn.textContent = 'Stop Scrape';
    }
  },

  // --- WebSocket ---

  connectWebSocket() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.ws = new WebSocket(`${protocol}//${location.host}/ws`);

    this.ws.onopen = () => {
      console.debug('[mortar] WS connected, subscribing to', this.jobId);
      this.ws.send(JSON.stringify({ type: 'subscribe', jobId: this.jobId }));
      this.wsReconnectAttempts = 0;
      this.showConnectionStatus('connected');
      this.addLog('info', 'Connected — scrape started');
    };

    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        this.handleWSMessage(msg);
      } catch (err) {
        console.error('[mortar] WS message parse error:', err, 'raw:', event.data.substring(0, 200));
      }
    };

    this.ws.onclose = (event) => {
      console.debug('[mortar] WS closed — code:', event.code, 'reason:', event.reason || 'none', 'jobId:', this.jobId, 'hasStats:', !!this.stats);
      // Reconnect only if job is still running (no stats yet = still running)
      if (this.jobId && !this.stats) {
        this.showConnectionStatus('reconnecting');
        this.reconnectWebSocket();
      } else {
        this.showConnectionStatus(null);
      }
    };

    this.ws.onerror = (event) => {
      console.error('[mortar] WS error:', event);
    };
  },

  manualReconnect() {
    const btn = document.getElementById('btn-reconnect');
    if (btn) btn.classList.add('hidden');
    this.wsReconnectAttempts = 0;
    this.addLog('info', 'Attempting to reconnect...');
    this.connectWebSocket();
  },

  reconnectWebSocket() {
    if (this.wsReconnectAttempts >= 5) {
      this.showConnectionStatus('disconnected');
      this.addLog('error', 'Lost connection to server. Falling back to HTTP polling...');
      const btn = document.getElementById('btn-reconnect');
      if (btn) btn.classList.remove('hidden');
      // Start HTTP polling fallback so user still sees progress
      this.startHttpPolling();
      return;
    }

    const delays = [1000, 2000, 4000, 8000, 10000];
    const delay = delays[this.wsReconnectAttempts] || 10000;
    this.wsReconnectAttempts++;

    this.wsReconnectTimer = setTimeout(() => {
      this.addLog('info', `Reconnecting... (attempt ${this.wsReconnectAttempts}/5)`);
      this.connectWebSocket();
    }, delay);
  },

  // HTTP polling fallback when WebSocket dies mid-scrape
  startHttpPolling() {
    if (this._httpPollTimer) return;
    this._httpPollTimer = setInterval(async () => {
      if (!this.jobId) { clearInterval(this._httpPollTimer); this._httpPollTimer = null; return; }
      try {
        const res = await fetch(`/api/scrape/${this.jobId}/status`);
        if (!res.ok) return;
        const data = await res.json();
        this.leads = data.leads || this.leads;
        this.$statNew.textContent = this.leads.length.toLocaleString();
        if (data.status === 'complete' || data.status === 'cancelled') {
          clearInterval(this._httpPollTimer);
          this._httpPollTimer = null;
          this.stats = data.stats || {};
          this.addLog('success', `Scrape complete! ${this.leads.length} new leads found.`);
          document.getElementById('btn-view-results').disabled = false;
          document.getElementById('btn-stop-scrape').hidden = true;
          this.$progressBar.style.width = '100%';
          document.getElementById('scrape-heading').textContent = 'Scrape Complete';
          this.showConnectionStatus(null);
        }
      } catch {}
    }, 5000);
  },

  showConnectionStatus(status) {
    const el = document.getElementById('connection-status');
    const text = document.getElementById('connection-text');
    if (!status) {
      el.classList.add('hidden');
      return;
    }
    el.classList.remove('hidden', 'connected', 'reconnecting', 'disconnected');
    el.classList.add(status);
    const labels = {
      connected: 'Connected',
      reconnecting: 'Reconnecting...',
      disconnected: 'Disconnected',
    };
    text.textContent = labels[status] || status;
  },

  handleWSMessage(msg) {
    switch (msg.type) {
      case 'log':
        this.addLog(msg.level, msg.message);
        break;

      case 'progress':
        this.$statScraped.textContent = msg.totalScraped.toLocaleString();
        this.$statDupes.textContent = msg.dupes.toLocaleString();
        this.$statNew.textContent = msg.netNew.toLocaleString();
        this.$statEmails.textContent = msg.emails.toLocaleString();

        // Progress bar: use city progress if multiple cities, else asymptotic
        if (msg.totalCities > 1 && msg.cityIndex > 0) {
          const pct = Math.min(95, Math.round((msg.cityIndex / msg.totalCities) * 95));
          this.$progressBar.style.width = pct + '%';
        } else if (msg.totalScraped > 0) {
          // Asymptotic: approaches 90% but never reaches it
          // Works well for single-city geo-grid scrapes where we don't know total count
          const pct = Math.min(90, Math.round(90 * (1 - Math.exp(-msg.totalScraped / 200))));
          this.$progressBar.style.width = pct + '%';
        }
        break;

      case 'lead':
        this.leads.push(msg.data);
        break;

      case 'waterfall-progress': {
        const section = document.getElementById('waterfall-section');
        if (section) {
          section.classList.remove('hidden');
          const pct = msg.total > 0 ? Math.round((msg.current / msg.total) * 100) : 0;
          document.getElementById('waterfall-progress-bar').style.width = pct + '%';
          const stepLabels = {
            'master-db': 'Cross-referencing master database',
            profiles: 'Fetching profile pages',
            martindale: 'Cross-referencing Martindale',
            'lawyers-com': 'Cross-referencing Lawyers.com',
            'name-lookup': 'Name lookups',
            'website-find': 'Finding firm websites',
            'email-crawl': 'Crawling websites for emails',
            'smtp-patterns': 'SMTP email pattern matching',
          };
          const stepLabel = stepLabels[msg.step] || msg.step;
          document.getElementById('waterfall-status-text').textContent =
            `${stepLabel}: ${msg.current}/${msg.total} — ${msg.detail || ''}`;
        }
        break;
      }

      case 'person-extract-progress': {
        const peSection = document.getElementById('person-extract-section');
        if (peSection) {
          peSection.classList.remove('hidden');
          const pePct = msg.total > 0 ? Math.round((msg.current / msg.total) * 100) : 0;
          document.getElementById('person-extract-progress-bar').style.width = pePct + '%';
          document.getElementById('person-extract-status-text').textContent =
            `Extracting: ${msg.current}/${msg.total} — ${msg.detail || ''}`;
        }
        break;
      }

      case 'enrichment-progress': {
        const section = document.getElementById('enrichment-section');
        if (!section) break;
        section.classList.remove('hidden');
        const pct = msg.total > 0 ? Math.round((msg.current / msg.total) * 100) : 0;
        document.getElementById('enrich-progress-bar').style.width = pct + '%';
        document.getElementById('enrich-status-text').textContent =
          `Enriching: ${msg.current}/${msg.total} — ${msg.leadName}`;
        break;
      }

      case 'complete':
        this.stats = msg.stats;
        this.addLog('success', `Scrape complete! ${this.leads.length} new leads found.`);
        document.getElementById('btn-view-results').disabled = false;
        document.getElementById('btn-stop-scrape').hidden = true;
        this.$progressBar.style.width = '100%';
        document.getElementById('scrape-heading').textContent = 'Scrape Complete';
        this.showConnectionStatus(null);
        // Mark waterfall as done if it was running
        if (msg.stats.waterfall) {
          const wfSection = document.getElementById('waterfall-section');
          if (wfSection) {
            document.getElementById('waterfall-progress-bar').style.width = '100%';
            document.getElementById('waterfall-status-text').textContent = 'Waterfall enrichment complete';
          }
        }
        // Mark person extraction as done if it was running
        if (msg.stats.personExtract) {
          const peSection = document.getElementById('person-extract-section');
          if (peSection) {
            document.getElementById('person-extract-progress-bar').style.width = '100%';
            document.getElementById('person-extract-status-text').textContent =
              `Complete — ${msg.stats.personExtract.peopleFound} people from ${msg.stats.personExtract.websitesVisited} websites`;
          }
        }
        // Mark enrichment as done if it was running
        if (msg.stats.enrichment) {
          document.getElementById('enrich-progress-bar').style.width = '100%';
          document.getElementById('enrich-status-text').textContent = 'Enrichment complete';
        }
        // Show enrichment preview if enrichment wasn't already done
        if (!msg.stats.enrichment) {
          this.showEnrichPreview();
        }
        if (this.ws) this.ws.close();
        break;

      case 'cancelled-complete':
        this.stats = msg.stats;
        this.addLog('warn', `Scrape stopped. ${this.leads.length} leads collected.`);
        document.getElementById('btn-view-results').disabled = false;
        document.getElementById('btn-stop-scrape').hidden = true;
        this.$progressBar.classList.add('progress-bar-cancelled');
        document.getElementById('scrape-heading').textContent = 'Scrape Stopped';
        this.showConnectionStatus(null);
        if (this.ws) this.ws.close();
        break;

      case 'error':
        this.addLog('error', msg.message);
        break;
    }
  },

  addLog(level, message) {
    const container = document.getElementById('log-container');
    const line = document.createElement('div');
    line.className = `log-line log-${level}`;

    const icons = {
      info: 'i',
      success: '✓',
      warn: '!',
      error: '✗',
      skip: '↷',
      scrape: '🔍',
      progress: '▶',
      enrich: '✦',
    };

    const icon = icons[level] || '·';
    line.textContent = `${icon} ${message}`;
    container.appendChild(line);

    // Auto-scroll to bottom
    container.scrollTop = container.scrollHeight;
  },

  // --- Step 4: Preview ---

  renderTable() {
    const body = document.getElementById('results-body');
    const filtered = this.getFilteredLeads();
    body.innerHTML = '';

    // Empty state
    if (this.leads.length === 0) {
      const dupesSkipped = this.stats?.duplicatesSkipped || 0;
      const dupesHint = dupesSkipped > 0
        ? `<p>${dupesSkipped.toLocaleString()} duplicate(s) were removed during deduplication. Try disabling the dedup CSV upload or using a different jurisdiction.</p>`
        : '<p>Try adjusting your search criteria — select a different practice area, city, or jurisdiction.</p>';
      body.innerHTML = `
        <tr>
          <td colspan="9">
            <div class="empty-state">
              <span class="empty-state-icon">🔍</span>
              <h3>No leads found</h3>
              ${dupesHint}
            </div>
          </td>
        </tr>
      `;
      document.getElementById('result-count').textContent = '0 leads';
      return;
    }

    for (const lead of filtered) {
      const tr = document.createElement('tr');
      const safeLinkedIn = lead.linkedin_url && /^https?:\/\//i.test(lead.linkedin_url) ? lead.linkedin_url : '';
      const linkedInCell = safeLinkedIn
        ? `<a href="${esc(safeLinkedIn)}" target="_blank" rel="noopener" class="link-view">View</a>`
        : '';
      const completeness = this.calcCompleteness(lead);
      const compClass = completeness >= 70 ? 'completeness-high' : completeness >= 40 ? 'completeness-med' : 'completeness-low';
      tr.innerHTML = `
        <td title="${esc(lead.first_name)}">${esc(lead.first_name)}</td>
        <td title="${esc(lead.last_name)}">${esc(lead.last_name)}</td>
        <td title="${esc(lead.firm_name)}">${esc(lead.firm_name)}</td>
        <td title="${esc(lead.title)}">${esc(lead.title)}</td>
        <td title="${esc(lead.city)}">${esc(lead.city)}</td>
        <td title="${esc(lead.email)}">${esc(lead.email)}</td>
        <td title="${esc(lead.phone)}">${esc(lead.phone)}</td>
        <td>${linkedInCell}</td>
        <td><span class="completeness-badge ${compClass}">${completeness}%</span></td>
      `;
      body.appendChild(tr);
    }

    document.getElementById('result-count').textContent = `${filtered.length} of ${this.leads.length} leads`;
  },

  getFilteredLeads() {
    const query = (document.getElementById('table-search').value || '').toLowerCase();
    let results = this.leads;

    if (query) {
      results = results.filter(l =>
        Object.values(l).some(v => String(v || '').toLowerCase().includes(query))
      );
    }

    if (this.sortCol) {
      results = [...results].sort((a, b) => {
        if (this.sortCol === '_completeness') {
          const va = this.calcCompleteness(a);
          const vb = this.calcCompleteness(b);
          return this.sortAsc ? va - vb : vb - va;
        }
        const va = (a[this.sortCol] || '').toLowerCase();
        const vb = (b[this.sortCol] || '').toLowerCase();
        if (va < vb) return this.sortAsc ? -1 : 1;
        if (va > vb) return this.sortAsc ? 1 : -1;
        return 0;
      });
    }

    return results;
  },

  sortTable(col) {
    if (this.sortCol === col) {
      this.sortAsc = !this.sortAsc;
    } else {
      this.sortCol = col;
      this.sortAsc = true;
    }
    this.renderTable();
    this.updateSortArrows();
  },

  updateSortArrows() {
    document.querySelectorAll('.sort-arrow').forEach(el => { el.textContent = ''; });
    if (!this.sortCol) return;
    const colMap = {
      first_name: 0, last_name: 1, firm_name: 2, title: 3,
      city: 4, email: 5, phone: 6, _completeness: 8,
    };
    const idx = colMap[this.sortCol];
    if (idx === undefined) return;
    const th = document.querySelectorAll('#results-table thead th')[idx];
    if (th) {
      const arrow = th.querySelector('.sort-arrow');
      if (arrow) arrow.textContent = this.sortAsc ? ' \u25B2' : ' \u25BC';
    }
  },

  _filterDebounceTimer: null,
  filterTable() {
    clearTimeout(this._filterDebounceTimer);
    this._filterDebounceTimer = setTimeout(() => this.renderTable(), 300);
  },

  // --- Step 5: Download ---

  renderDownloadPage() {
    // Empty state: no leads
    if (this.leads.length === 0) {
      document.getElementById('download-filename').textContent = '';
      document.getElementById('download-summary').textContent = '';
      const card = document.querySelector('.download-card');
      card.innerHTML = `
        <div class="empty-state">
          <span class="empty-state-icon">📭</span>
          <h3>No new leads to download</h3>
          <p>The scrape didn't find any new leads. Try a different practice area, city, or disable deduplication.</p>
        </div>
      `;
    } else {
      const count = this.leads.length;
      const emails = this.leads.filter(l => l.email).length;

      document.getElementById('download-filename').textContent =
        this.jobId ? `${this.jobId.replace('job-', 'leads-')}.csv` : 'leads.csv';
      document.getElementById('download-summary').textContent =
        `${count.toLocaleString()} leads with ${emails.toLocaleString()} emails`;

      // Render column selection checkboxes
      const colSelector = document.getElementById('column-selector');
      if (colSelector) {
        colSelector.innerHTML = '';
        for (const col of this.allColumns) {
          const label = document.createElement('label');
          label.className = 'col-select-label';
          label.innerHTML = `<input type="checkbox" class="col-select-checkbox" value="${col.key}" ${col.default ? 'checked' : ''}> ${esc(col.label)}`;
          colSelector.appendChild(label);
        }
      }
    }

    // Summary
    const grid = document.getElementById('summary-grid');
    grid.innerHTML = '';

    const stats = this.stats || {};
    const rows = [
      ['Total Scraped', stats.totalScraped || 0],
      ['Duplicates Skipped', stats.duplicatesSkipped || 0],
      ['Net New Leads', stats.netNew || 0],
      ['Emails Found', stats.emailsFound || 0],
    ];

    if (stats.captchaSkipped) rows.push(['CAPTCHA Skipped', stats.captchaSkipped]);
    if (stats.errorSkipped) rows.push(['Errors', stats.errorSkipped]);

    // Waterfall stats
    if (stats.waterfall) {
      const w = stats.waterfall;
      if (w.profilesFetched) rows.push(['Profiles Fetched', w.profilesFetched]);
      if (w.crossRefMatches) rows.push(['Cross-Ref Matches', w.crossRefMatches]);
      if (w.nameLookupsRun) rows.push(['Name Lookups', w.nameLookupsRun]);
      if (w.emailsCrawled) rows.push(['Emails from Websites', w.emailsCrawled]);
      if (w.totalFieldsFilled) rows.push(['Fields Filled (Waterfall)', w.totalFieldsFilled]);
    }

    // Person extraction stats
    if (stats.personExtract) {
      rows.push(['People Extracted', stats.personExtract.peopleFound || 0]);
      rows.push(['Websites Visited', stats.personExtract.websitesVisited || 0]);
    }

    // Enrichment stats
    if (stats.enrichment) {
      const e = stats.enrichment;
      if (e.websitesDerived) rows.push(['Websites Derived', e.websitesDerived]);
      if (e.titlesFound) rows.push(['Titles Found', e.titlesFound]);
      if (e.linkedInFound) rows.push(['LinkedIn Found', e.linkedInFound]);
      if (e.educationFound) rows.push(['Education Found', e.educationFound]);
      if (e.llmCalls) rows.push(['AI Calls', e.llmCalls]);
    }

    for (const [label, value] of rows) {
      const div = document.createElement('div');
      div.className = 'summary-row';
      div.innerHTML = `<span class="label">${label}</span><span class="value">${value.toLocaleString()}</span>`;
      grid.appendChild(div);
    }
  },

  async downloadCSV() {
    if (!this.jobId) {
      this.showNotification('No job to download. Start a scrape first.', 'error');
      return;
    }

    const btn = document.getElementById('btn-download');
    const originalText = btn.textContent;
    btn.textContent = 'Preparing...';
    btn.disabled = true;

    try {
      console.debug('[mortar] Downloading CSV for', this.jobId);
      const res = await fetch(`/api/scrape/${this.jobId}/download`);
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        console.error('[mortar] Download failed:', res.status, data);
        this.showNotification(data.error || 'Download failed', 'error');
        return;
      }

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = this.jobId ? `${this.jobId.replace('job-', 'leads-')}.csv` : 'leads.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('[mortar] Download network error:', err);
      this.showNotification('Download failed: ' + err.message, 'error');
    } finally {
      btn.textContent = originalText;
      btn.disabled = false;
    }
  },

  // --- Notification System ---

  showNotification(message, type = 'info') {
    const area = document.getElementById('notification-area');
    const el = document.createElement('div');
    el.className = `notification ${type}`;
    el.innerHTML = `
      <span>${esc(message)}</span>
      <button class="notification-dismiss" onclick="this.parentElement.remove()">&times;</button>
    `;
    area.appendChild(el);

    // Auto-dismiss after 5 seconds
    setTimeout(() => {
      if (el.parentElement) el.remove();
    }, 5000);
  },

  // --- beforeunload Protection ---

  setupBeforeUnload() {
    window.addEventListener('beforeunload', (e) => {
      // Only warn when scrape is actively running (Step 3, no stats yet)
      if (this.currentStep === 3 && this.jobId && !this.stats) {
        e.preventDefault();
        e.returnValue = '';
      }
    });
  },

  // --- Lead Completeness Scoring ---

  calcCompleteness(lead) {
    let score = 0;
    if (lead.first_name) score += 10;
    if (lead.last_name) score += 10;
    if (lead.email) score += 25;
    if (lead.phone) score += 15;
    if (lead.firm_name) score += 10;
    if (lead.city) score += 5;
    if (lead.bar_number) score += 5;
    if (lead.admission_date) score += 5;
    if (lead.website) score += 10;
    if (lead.linkedin_url) score += 5;
    return score;
  },

  // --- Column Selection Export ---

  allColumns: [
    { key: 'first_name', label: 'First Name', default: true },
    { key: 'last_name', label: 'Last Name', default: true },
    { key: 'firm_name', label: 'Firm', default: true },
    { key: 'title', label: 'Title', default: true },
    { key: 'city', label: 'City', default: true },
    { key: 'state', label: 'State', default: true },
    { key: 'email', label: 'Email', default: true },
    { key: 'phone', label: 'Phone', default: true },
    { key: 'website', label: 'Website', default: true },
    { key: 'linkedin_url', label: 'LinkedIn', default: true },
    { key: 'bar_number', label: 'Bar Number', default: false },
    { key: 'admission_date', label: 'Admission Date', default: false },
    { key: 'bar_status', label: 'Bar Status', default: false },
    { key: 'practice_area', label: 'Practice Area', default: true },
    { key: 'bio', label: 'Bio', default: false },
    { key: 'education', label: 'Education', default: false },
    { key: 'languages', label: 'Languages', default: false },
    { key: 'email_source', label: 'Email Source', default: false },
    { key: 'phone_source', label: 'Phone Source', default: false },
    { key: 'website_source', label: 'Website Source', default: false },
  ],

  getSelectedColumns() {
    const checkboxes = document.querySelectorAll('.col-select-checkbox');
    if (checkboxes.length === 0) return this.allColumns.filter(c => c.default).map(c => c.key);
    const selected = [];
    checkboxes.forEach(cb => { if (cb.checked) selected.push(cb.value); });
    return selected;
  },

  exportCSVCustom() {
    if (this.leads.length === 0) {
      this.showNotification('No leads to export', 'error');
      return;
    }
    const cols = this.getSelectedColumns();
    const header = cols.map(c => `"${c}"`).join(',');
    const rows = this.leads.map(lead =>
      cols.map(c => {
        const val = (lead[c] || '').toString().replace(/"/g, '""');
        return `"${val}"`;
      }).join(',')
    );
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = this.jobId ? `${this.jobId.replace('job-', 'leads-')}.csv` : 'leads.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  },

  // --- Reset ---

  reset() {
    // Confirmation dialog if leads exist
    if (this.leads.length > 0) {
      if (!confirm(`You have ${this.leads.length} leads. Start a new scrape? (Your current results will be lost)`)) {
        return;
      }
    }

    this.uploadId = null;
    this.jobId = null;
    this.leads = [];
    this.stats = null;
    this.sortCol = null;
    this.sortAsc = true;
    localStorage.removeItem('mortar-jobId');

    if (this.wsReconnectTimer) {
      clearTimeout(this.wsReconnectTimer);
      this.wsReconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    // Reset upload UI
    document.getElementById('upload-status').classList.add('hidden');
    document.getElementById('btn-upload-next').disabled = true;
    document.getElementById('file-input').value = '';

    // Reset text inputs
    const nicheInput = document.getElementById('input-niche');
    if (nicheInput) nicheInput.value = '';
    const cityInput = document.getElementById('input-city');
    if (cityInput) cityInput.value = '';

    // Reset Step 3 UI
    document.getElementById('btn-stop-scrape').hidden = true;
    document.getElementById('btn-view-results').disabled = true;
    document.getElementById('test-mode-badge').classList.add('hidden');
    document.getElementById('scrape-heading').textContent = 'Scraping...';
    this.$progressBar.classList.remove('progress-bar-cancelled');
    this.$progressBar.style.width = '0%';
    this.showConnectionStatus(null);

    // Reset stats counters
    document.getElementById('stat-scraped').textContent = '0';
    document.getElementById('stat-dupes').textContent = '0';
    document.getElementById('stat-new').textContent = '0';
    document.getElementById('stat-emails').textContent = '0';

    // Clear scrape log
    document.getElementById('log-container').innerHTML = '<div class="log-line log-info">Waiting for scrape to start...</div>';

    // Re-hide enrichment/progress sections
    document.getElementById('waterfall-section').classList.add('hidden');
    document.getElementById('waterfall-progress-bar').style.width = '0%';
    document.getElementById('person-extract-section').classList.add('hidden');
    document.getElementById('person-extract-progress-bar').style.width = '0%';
    document.getElementById('enrichment-section').classList.add('hidden');
    document.getElementById('enrich-progress-bar').style.width = '0%';
    document.getElementById('enrich-preview').classList.add('hidden');

    // Clear Step 4 table
    document.getElementById('results-body').innerHTML = '';
    document.getElementById('table-search').value = '';
    document.getElementById('result-count').textContent = '';

    // Restore download card HTML in case it was replaced by empty state
    const downloadCard = document.querySelector('.download-card');
    downloadCard.innerHTML = `
      <div class="download-icon">📥</div>
      <h3 id="download-filename">leads.csv</h3>
      <p id="download-summary"></p>
      <button class="btn btn-primary btn-large" id="btn-download" onclick="app.downloadCSV()">Download CSV</button>
      <p class="hint">Ready to upload to Instantly</p>
    `;

    this.goToStep(1);
  },
};

// Escape HTML
function esc(str) {
  if (!str && str !== 0) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Boot
app.init();
