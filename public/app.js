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

  // --- Initialization ---

  async init() {
    this.setupDropZone();
    await this.loadConfig();
  },

  async loadConfig() {
    try {
      const res = await fetch('/api/config');
      this.config = await res.json();
      this.populateDropdowns();
      this.setupEnrichmentToggles();
    } catch (err) {
      console.error('Failed to load config:', err);
    }
  },

  populateDropdowns() {
    const stateSelect = document.getElementById('select-state');
    stateSelect.innerHTML = '';
    for (const state of this.config.states) {
      const opt = document.createElement('option');
      opt.value = state;
      opt.textContent = state;
      stateSelect.appendChild(opt);
    }

    const practiceSelect = document.getElementById('select-practice');
    practiceSelect.innerHTML = '<option value="">All practice areas</option>';
    for (const area of this.config.practiceAreas) {
      const opt = document.createElement('option');
      opt.value = area;
      opt.textContent = area.charAt(0).toUpperCase() + area.slice(1);
      practiceSelect.appendChild(opt);
    }

    const citySelect = document.getElementById('select-city');
    citySelect.innerHTML = '<option value="">All major cities</option>';
    for (const city of this.config.floridaCities) {
      const opt = document.createElement('option');
      opt.value = city;
      opt.textContent = city;
      citySelect.appendChild(opt);
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
    if (!file.name.endsWith('.csv')) {
      alert('Please upload a CSV file');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/upload', { method: 'POST', body: formData });
      const data = await res.json();

      if (!res.ok) {
        alert(data.error || 'Upload failed');
        return;
      }

      this.uploadId = data.uploadId;

      const status = document.getElementById('upload-status');
      const message = document.getElementById('upload-message');
      status.classList.remove('hidden');
      message.textContent = `${data.originalName} — ${data.count.toLocaleString()} leads loaded for deduplication`;

      document.getElementById('btn-upload-next').disabled = false;
    } catch (err) {
      alert('Upload failed: ' + err.message);
    }
  },

  skipUpload() {
    this.uploadId = null;
    this.goToStep(2);
  },

  // --- Step 2: Configure ---

  setupEnrichmentToggles() {
    if (!this.config) return;
    // Disable AI toggle if no API key
    if (!this.config.hasAnthropicKey) {
      const aiToggle = document.getElementById('toggle-ai-fallback');
      aiToggle.disabled = true;
      document.getElementById('ai-warning').classList.remove('hidden');
      document.getElementById('label-ai-toggle').classList.add('disabled');
    }
  },

  toggleEnrichment() {
    const enabled = document.getElementById('toggle-enrich').checked;
    const options = document.getElementById('enrich-options');
    if (enabled) {
      options.classList.remove('hidden');
    } else {
      options.classList.add('hidden');
    }
  },

  // --- Step 3: Scrape ---

  async startScrape() {
    const state = document.getElementById('select-state').value;
    const practice = document.getElementById('select-practice').value;
    const city = document.getElementById('select-city').value;
    const test = document.getElementById('toggle-test').checked;

    if (!state) {
      alert('Please select a state');
      return;
    }

    // Move to scrape step
    this.goToStep(3);

    // Gather enrichment options
    const enrich = document.getElementById('toggle-enrich').checked;
    const enrichOptions = enrich ? {
      deriveWebsite: document.getElementById('toggle-derive-website').checked,
      scrapeWebsite: document.getElementById('toggle-scrape-website').checked,
      findLinkedIn: document.getElementById('toggle-scrape-website').checked, // Depends on website scraping
      extractWithAI: document.getElementById('toggle-ai-fallback').checked,
    } : {};

    // Clear previous state
    this.leads = [];
    this.stats = null;
    document.getElementById('log-container').innerHTML = '';
    document.getElementById('stat-scraped').textContent = '0';
    document.getElementById('stat-dupes').textContent = '0';
    document.getElementById('stat-new').textContent = '0';
    document.getElementById('stat-emails').textContent = '0';
    document.getElementById('progress-bar').style.width = '0%';
    document.getElementById('btn-view-results').disabled = true;
    document.getElementById('enrichment-section').classList.add('hidden');
    document.getElementById('enrich-progress-bar').style.width = '0%';

    // Start the job
    try {
      const res = await fetch('/api/scrape/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ state, practice, city, test, uploadId: this.uploadId, enrich, enrichOptions }),
      });
      const data = await res.json();

      if (!res.ok) {
        this.addLog('error', data.error || 'Failed to start scrape');
        return;
      }

      this.jobId = data.jobId;
      this.connectWebSocket();
    } catch (err) {
      this.addLog('error', 'Failed to start scrape: ' + err.message);
    }
  },

  connectWebSocket() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.ws = new WebSocket(`${protocol}//${location.host}/ws`);

    this.ws.onopen = () => {
      this.ws.send(JSON.stringify({ type: 'subscribe', jobId: this.jobId }));
      this.addLog('info', 'Connected — scrape started');
    };

    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      this.handleWSMessage(msg);
    };

    this.ws.onclose = () => {
      // Reconnect only if job is still running
    };

    this.ws.onerror = () => {
      this.addLog('error', 'WebSocket connection error');
    };
  },

  handleWSMessage(msg) {
    switch (msg.type) {
      case 'log':
        this.addLog(msg.level, msg.message);
        break;

      case 'progress':
        document.getElementById('stat-scraped').textContent = msg.totalScraped.toLocaleString();
        document.getElementById('stat-dupes').textContent = msg.dupes.toLocaleString();
        document.getElementById('stat-new').textContent = msg.netNew.toLocaleString();
        document.getElementById('stat-emails').textContent = msg.emails.toLocaleString();

        // Progress bar: use netNew / totalScraped ratio (capped at 100%)
        if (msg.totalScraped > 0) {
          const pct = Math.min(100, Math.round((msg.netNew / msg.totalScraped) * 100));
          document.getElementById('progress-bar').style.width = pct + '%';
        }
        break;

      case 'lead':
        this.leads.push(msg.data);
        break;

      case 'enrichment-progress': {
        const section = document.getElementById('enrichment-section');
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
        document.getElementById('progress-bar').style.width = '100%';
        // Mark enrichment as done if it was running
        if (msg.stats.enrichment) {
          document.getElementById('enrich-progress-bar').style.width = '100%';
          document.getElementById('enrich-status-text').textContent = 'Enrichment complete';
        }
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

    for (const lead of filtered) {
      const tr = document.createElement('tr');
      const linkedInCell = lead.linkedin_url
        ? `<a href="${esc(lead.linkedin_url)}" target="_blank" rel="noopener" class="link-view">View</a>`
        : '';
      tr.innerHTML = `
        <td title="${esc(lead.first_name)}">${esc(lead.first_name)}</td>
        <td title="${esc(lead.last_name)}">${esc(lead.last_name)}</td>
        <td title="${esc(lead.firm_name)}">${esc(lead.firm_name)}</td>
        <td title="${esc(lead.title)}">${esc(lead.title)}</td>
        <td title="${esc(lead.city)}">${esc(lead.city)}</td>
        <td title="${esc(lead.email)}">${esc(lead.email)}</td>
        <td title="${esc(lead.phone)}">${esc(lead.phone)}</td>
        <td>${linkedInCell}</td>
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
        Object.values(l).some(v => (v || '').toLowerCase().includes(query))
      );
    }

    if (this.sortCol) {
      results = [...results].sort((a, b) => {
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
  },

  filterTable() {
    this.renderTable();
  },

  // --- Step 5: Download ---

  renderDownloadPage() {
    const count = this.leads.length;
    const emails = this.leads.filter(l => l.email).length;

    document.getElementById('download-filename').textContent =
      this.jobId ? `${this.jobId.replace('job-', 'leads-')}.csv` : 'leads.csv';
    document.getElementById('download-summary').textContent =
      `${count.toLocaleString()} leads with ${emails.toLocaleString()} emails`;

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

  downloadCSV() {
    if (!this.jobId) return;
    window.open(`/api/scrape/${this.jobId}/download`, '_blank');
  },

  // --- Reset ---

  reset() {
    this.uploadId = null;
    this.jobId = null;
    this.leads = [];
    this.stats = null;
    this.sortCol = null;
    this.sortAsc = true;

    // Reset upload UI
    document.getElementById('upload-status').classList.add('hidden');
    document.getElementById('btn-upload-next').disabled = true;
    document.getElementById('file-input').value = '';

    this.goToStep(1);
  },
};

// Escape HTML
function esc(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Boot
app.init();
