// leads-table.js — Lead table: CRUD, search, filters, modals, keyboard shortcuts, views, columns
// Dependencies: utils.js, app.js

// --- Tab Switching ---
function switchTab(tab, contentId) {
  const parent = tab.closest('.panel');
  parent.querySelectorAll('.panel-tab').forEach(t => t.classList.remove('active'));
  parent.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  tab.classList.add('active');
  document.getElementById(contentId).classList.add('active');
  if (contentId === 'tab-duplicates') loadDuplicates();
  if (contentId === 'tab-health') loadScraperHealth();
}

// --- Duplicates ---
async function loadDuplicates() {
  try {
    const res = await fetch('/api/leads/duplicates?limit=50');
    const data = await safeJSON(res);
    const list = document.getElementById('dupe-list');
    const empty = document.getElementById('dupe-empty');
    const badge = document.getElementById('dupe-count-badge');

    if (!data || data.length === 0) {
      list.innerHTML = '';
      empty.style.display = '';
      badge.style.display = 'none';
      return;
    }

    empty.style.display = 'none';
    badge.style.display = 'inline';
    badge.textContent = data.length;

    list.innerHTML = data.map((d, i) => {
      const matchCls = d.match_type === 'email' ? 'email' : d.match_type === 'phone' ? 'phone' : 'name-city';
      const side = (fn, ln, city, st, email, phone, firm) => `
        <strong>${esc(fn)} ${esc(ln)}</strong>
        <div style="font-size:0.75rem;color:var(--text-muted);">
          ${city ? esc(city) + ', ' + esc(st) : esc(st) || ''}
          ${firm ? '<br>' + esc(firm) : ''}
          ${email ? '<br>' + esc(email) : ''}
          ${phone ? '<br>' + esc(phone) : ''}
        </div>`;
      return `<div class="dupe-card">
        <div class="dupe-header">
          <span class="dupe-match-type ${matchCls}">${d.match_type.replace('+', ' + ')}</span>
          <span style="font-size:0.72rem;color:var(--text-muted);">#${d.id1} vs #${d.id2}</span>
        </div>
        <div class="dupe-compare">
          <div class="dupe-side">${side(d.fn1, d.ln1, d.city1, d.state1, d.email1, d.phone1, d.firm1)}</div>
          <div class="dupe-vs">VS</div>
          <div class="dupe-side">${side(d.fn2, d.ln2, d.city2, d.state2, d.email2, d.phone2, d.firm2)}</div>
        </div>
        <div class="dupe-actions">
          <button class="btn btn-secondary" style="font-size:0.72rem;padding:0.2rem 0.5rem;" onclick="mergeDupe(${d.id1}, ${d.id2}, this)">Keep #${d.id1}</button>
          <button class="btn btn-secondary" style="font-size:0.72rem;padding:0.2rem 0.5rem;" onclick="mergeDupe(${d.id2}, ${d.id1}, this)">Keep #${d.id2}</button>
          <button class="btn btn-secondary" style="font-size:0.72rem;padding:0.2rem 0.5rem;opacity:0.5;" onclick="this.closest('.dupe-card').remove()">Skip</button>
        </div>
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load duplicates:', err);
  }
}

async function mergeDupe(keepId, deleteId, btn) {
  btn.disabled = true;
  btn.textContent = '...';
  try {
    const res = await fetch('/api/leads/merge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keepId, deleteId }),
    });
    const d = await safeJSON(res);
    if (d.merged) {
      btn.closest('.dupe-card').remove();
      const badge = document.getElementById('dupe-count-badge');
      const current = parseInt(badge.textContent) || 0;
      badge.textContent = Math.max(0, current - 1);
    }
  } catch (err) {
    btn.textContent = 'Error';
  }
}

// --- Lead Lists / Campaigns ---
let _lists = [];
let _activeListId = null;

async function loadLists() {
  try {
    const res = await fetch('/api/lists');
    _lists = await safeJSON(res);
    renderLists();
  } catch (err) {
    console.error('Failed to load lists:', err);
  }
}

function renderLists() {
  const el = document.getElementById('list-cards');
  if (_lists.length === 0) {
    el.innerHTML = '<span style="font-size:0.82rem;color:var(--text-muted);">No lists yet. Create one to organize leads for campaigns.</span>';
    return;
  }
  el.innerHTML = _lists.map(l => `
    <div class="list-card ${_activeListId === l.id ? 'active' : ''}" onclick="toggleListFilter(${l.id})">
      <span class="list-dot" style="background:${esc(l.color)};"></span>
      <span class="list-card-name">${esc(l.name)}</span>
      <span class="list-card-count">${l.member_count}</span>
      <span class="list-x" onclick="event.stopPropagation();deleteListUI(${l.id})">&times;</span>
    </div>
  `).join('');
}

async function createNewList() {
  const name = prompt('List name (e.g. "NYC Corporate Campaign"):');
  if (!name || !name.trim()) return;
  const colors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899'];
  const color = colors[_lists.length % colors.length];
  try {
    const res = await fetch('/api/lists', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim(), color }),
    });
    await safeJSON(res);
    loadLists();
  } catch (err) { toast('Failed to create list: ' + err.message, 'error'); }
}

async function deleteListUI(id) {
  if (!confirm('Delete this list? Leads will NOT be deleted.')) return;
  try {
    await fetch('/api/lists/' + id, { method: 'DELETE' });
    if (_activeListId === id) _activeListId = null;
    loadLists();
  } catch (err) { toast('Failed to delete list: ' + err.message, 'error'); }
}

function toggleListFilter(id) {
  _activeListId = _activeListId === id ? null : id;
  renderLists();
  // TODO: filter leads by list membership (would need server-side support)
}

// Add selected leads to a list (from FAB)
async function addSelectedToList() {
  if (_selectedIds.size === 0) return;
  if (_lists.length === 0) {
    alert('Create a list first');
    return;
  }
  const options = _lists.map(l => l.name).join(', ');
  const name = prompt(`Add to which list? (${options})`);
  if (!name) return;
  const list = _lists.find(l => l.name.toLowerCase() === name.toLowerCase());
  if (!list) { alert('List not found: ' + name); return; }
  try {
    const res = await fetch(`/api/lists/${list.id}/add`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: [..._selectedIds] }),
    });
    const d = await safeJSON(res);
    document.getElementById('bulk-status').textContent = `Added ${d.added} leads to "${list.name}"`;
    toast(`Added ${d.added} leads to "${list.name}"`);
    clearSelection();
    loadLists();
  } catch (err) { toast('Failed to add to list: ' + err.message, 'error'); }
}

// --- Scraper Health ---
async function loadScraperHealth() {
  try {
    const res = await fetch('/api/scrapers/health');
    const data = await safeJSON(res);
    const el = document.getElementById('health-grid');
    if (!data || data.length === 0) {
      el.innerHTML = '<div style="color:var(--text-muted);font-size:0.82rem;">No scrape runs yet</div>';
      return;
    }
    el.innerHTML = data.map(s => {
      const healthCls = s.success_rate >= 80 ? 'health-good' : s.success_rate >= 50 ? 'health-warn' : 'health-bad';
      return `<div class="health-card">
        <span class="health-indicator ${healthCls}"></span>
        <div class="health-info">
          <div>
            <span class="health-state">${esc(s.state)}</span>
            <span class="health-stats">${s.total_runs} runs, avg ${s.avg_leads} leads</span>
          </div>
          <span style="font-size:0.72rem;font-weight:600;color:${s.success_rate >= 80 ? '#22c55e' : s.success_rate >= 50 ? '#f59e0b' : '#ef4444'};">${s.success_rate}%</span>
        </div>
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load scraper health:', err);
  }
}

// --- Command Palette (Cmd+K / Ctrl+K) ---
let _cmdActive = false;
let _cmdSelectedIdx = 0;

const CMD_ACTIONS = [
  { label: 'Scrape All', desc: 'Run all working scrapers', icon: '&#x26A1;', action: () => startBulkScrape() },
  { label: 'Test Mode', desc: 'Quick test scrape all scrapers', icon: '&#x1F9EA;', action: () => startBulkScrape(true) },
  { label: 'Enrich All', desc: 'Run waterfall enrichment pipeline', icon: '&#x2728;', action: () => enrichAll() },
  { label: 'Find Websites', desc: 'Google search for firm websites', icon: '&#x1F310;', action: () => startWebsiteFinder() },
  { label: 'Find Emails', desc: 'SMTP verification for emails', icon: '&#x1F4E7;', action: () => startVerify() },
  { label: 'Score All', desc: 'Recompute lead quality scores', icon: '&#x1F4CA;', action: () => scoreLeads() },
  { label: 'Merge Duplicates', desc: 'Auto-merge duplicate leads', icon: '&#x1F501;', action: () => mergeDuplicates() },
  { label: 'Share Firm Data', desc: 'Propagate firm info across leads', icon: '&#x1F3E2;', action: () => shareFirmData() },
  { label: 'Deduce Websites', desc: 'Infer websites from email domains', icon: '&#x1F517;', action: () => deduceWebsites() },
  { label: 'Export All', desc: 'Download all leads as CSV', icon: '&#x1F4E5;', action: () => { window.location.href = '/api/leads/export'; } },
  { label: 'Export Gold', desc: 'Export leads with email+phone', icon: '&#x1F31F;', action: () => { window.location.href = '/api/leads/export?hasEmail=true&hasPhone=true'; } },
  { label: 'Export Instantly', desc: 'Export in Instantly.ai format', icon: '&#x1F4E8;', action: () => { window.location.href = '/api/leads/export/instantly'; } },
  { label: 'New List', desc: 'Create a new lead list/campaign', icon: '&#x1F4CB;', action: () => createNewList() },
  { label: 'Export SmartLead', desc: 'Export in SmartLead format', icon: '&#x1F4AC;', action: () => { window.location.href = '/api/leads/export/smartlead'; } },
  { label: 'Pre-populate DB', desc: 'Quick scrape all for database seeding', icon: '&#x1F4BE;', action: () => startPrepopulate() },
  { label: 'Copy Emails', desc: 'Copy filtered lead emails to clipboard', icon: '&#x1F4CB;', action: () => copyAllEmails() },
  { label: 'Compare Selected', desc: 'Side-by-side compare selected leads', icon: '&#x1F50D;', action: () => compareSelectedLeads() },
  { label: 'Quality Check', desc: 'Scan for data quality issues', icon: '&#x1F50D;', action: () => runQualityCheck() },
  { label: 'AI Brain', desc: 'Ask questions about your leads', icon: '&#x1F9E0;', action: () => toggleAiBrain() },
  { label: 'AI Classify', desc: 'Auto-tag practice areas with AI', icon: '&#x1F3AF;', action: () => runAiClassify() },
  { label: 'AI Quality Audit', desc: 'AI-powered data quality check', icon: '&#x1F50D;', action: () => runAiAudit() },
  { label: 'Dark Mode', desc: 'Toggle dark/light theme', icon: '&#x1F319;', action: () => toggleDarkMode() },
  { label: 'Clear Filters', desc: 'Reset all search filters', icon: '&#x1F6AB;', action: () => { document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' })); } },
  { label: 'Refresh Data', desc: 'Reload all dashboard data', icon: '&#x1F504;', action: () => refreshAll() },
  { label: 'Switch to Overview', desc: 'Go to overview tab', icon: '&#x1F4CA;', action: () => switchMainTab('overview') },
  { label: 'Switch to Leads', desc: 'Go to leads explorer', icon: '&#x1F465;', action: () => switchMainTab('leads') },
  { label: 'Switch to Analytics', desc: 'Go to analytics tab', icon: '&#x1F4C8;', action: () => switchMainTab('analytics') },
  { label: 'Switch to Quality', desc: 'Go to data quality tab', icon: '&#x2705;', action: () => switchMainTab('quality') },
  { label: 'Switch to Outreach', desc: 'Go to outreach tab', icon: '&#x1F4E7;', action: () => switchMainTab('outreach') },
  { label: 'Switch to Settings', desc: 'Go to settings tab', icon: '&#x2699;&#xFE0F;', action: () => switchMainTab('settings') },
];

function openCmdPalette() {
  _cmdActive = true;
  _cmdSelectedIdx = 0;
  document.getElementById('cmd-palette').classList.add('active');
  const input = document.getElementById('cmd-input');
  input.value = '';
  input.focus();
  renderCmdResults('');
}

function closeCmdPalette() {
  _cmdActive = false;
  document.getElementById('cmd-palette').classList.remove('active');
}

// State name map for command palette search
const STATE_NAMES = {
  AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',CO:'Colorado',CT:'Connecticut',DE:'Delaware',FL:'Florida',GA:'Georgia',HI:'Hawaii',ID:'Idaho',IL:'Illinois',IN:'Indiana',IA:'Iowa',KS:'Kansas',KY:'Kentucky',LA:'Louisiana',ME:'Maine',MD:'Maryland',MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',MS:'Mississippi',MO:'Missouri',MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',NJ:'New Jersey',NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',OH:'Ohio',OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',SC:'South Carolina',SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',VA:'Virginia',WA:'Washington',WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming',
  'CA-AB':'Alberta','CA-BC':'British Columbia','CA-NL':'Newfoundland','CA-PE':'Prince Edward Island','CA-YT':'Yukon',
  'UK-SC':'Scotland','UK-EW-BAR':'England Wales Bar',
  'AU-NSW':'New South Wales','AU-QLD':'Queensland','AU-SA':'South Australia','AU-TAS':'Tasmania','AU-VIC':'Victoria','AU-WA':'Western Australia',
  FR:'France',IE:'Ireland',IT:'Italy',HK:'Hong Kong',NZ:'New Zealand',SG:'Singapore',
};

let _cmdDynamicActions = [];

function renderCmdResults(query) {
  const q = query.toLowerCase();
  let items = CMD_ACTIONS;
  _cmdDynamicActions = [];

  if (q) {
    items = items.filter(a =>
      a.label.toLowerCase().includes(q) ||
      a.desc.toLowerCase().includes(q)
    );

    // Add dynamic state search results
    for (const [code, name] of Object.entries(STATE_NAMES)) {
      if (code.toLowerCase().includes(q) || name.toLowerCase().includes(q)) {
        _cmdDynamicActions.push(
          { label: `View ${code} leads`, desc: name, icon: '&#x1F50D;', action: () => { document.getElementById('filter-state').value = code; loadLeads(); } },
          { label: `Scrape ${code}`, desc: `Quick scrape ${name}`, icon: '&#x26A1;', action: () => quickScrape(code) },
          { label: `${code} Analytics`, desc: `View ${name} analytics`, icon: '&#x1F4CA;', action: () => openStateModal(code) },
        );
      }
    }
  }

  const allItems = [...items, ..._cmdDynamicActions].slice(0, 15);
  _cmdSelectedIdx = Math.min(_cmdSelectedIdx, allItems.length - 1);
  if (_cmdSelectedIdx < 0) _cmdSelectedIdx = 0;

  document.getElementById('cmd-results').innerHTML = allItems.map((item, i) =>
    `<div class="cmd-item ${i === _cmdSelectedIdx ? 'active' : ''}" data-idx="${i}"
      onmouseenter="_cmdSelectedIdx=${i};renderCmdResults(document.getElementById('cmd-input').value)"
      onclick="_cmdAllItems[${i}].action();closeCmdPalette();">
      <span class="cmd-icon">${item.icon}</span>
      <span class="cmd-label">${esc(item.label)}<small>${esc(item.desc)}</small></span>
    </div>`
  ).join('') || '<div style="padding:1rem;text-align:center;color:var(--text-muted);font-size:0.85rem;">No matching actions</div>';

  window._cmdAllItems = allItems;
}

document.getElementById('cmd-input').addEventListener('input', (e) => {
  _cmdSelectedIdx = 0;
  renderCmdResults(e.target.value);
});

document.getElementById('cmd-input').addEventListener('keydown', (e) => {
  const items = document.querySelectorAll('.cmd-item');
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    _cmdSelectedIdx = Math.min(_cmdSelectedIdx + 1, items.length - 1);
    renderCmdResults(document.getElementById('cmd-input').value);
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    _cmdSelectedIdx = Math.max(_cmdSelectedIdx - 1, 0);
    renderCmdResults(document.getElementById('cmd-input').value);
  } else if (e.key === 'Enter' && items[_cmdSelectedIdx]) {
    items[_cmdSelectedIdx].click();
  } else if (e.key === 'Escape') {
    closeCmdPalette();
  }
});

// --- Quick Scrape per state ---
async function quickScrape(state) {
  const btn = document.getElementById('cov-scrape-' + state);
  if (!btn || btn.classList.contains('scraping')) return;
  btn.classList.add('scraping');
  btn.textContent = '...';

  try {
    const res = await fetch('/api/scrape/quick', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state, test: true }),
    });
    const d = await safeJSON(res);
    if (d.jobId) {
      pollQuickScrape(d.jobId, state, btn);
    }
  } catch {
    btn.classList.remove('scraping');
    btn.textContent = 'Scrape';
  }
}

function pollQuickScrape(jobId, state, btn) {
  const interval = setInterval(async () => {
    try {
      const res = await fetch('/api/scrape/quick/' + jobId);
      const d = await safeJSON(res);
      if (d.status === 'complete' || d.status === 'error') {
        clearInterval(interval);
        btn.classList.remove('scraping');
        btn.textContent = d.leads > 0 ? `+${d.leads}` : 'Done';
        setTimeout(() => { btn.textContent = 'Scrape'; }, 3000);
        if (d.leads > 0) refreshAll();
      } else {
        btn.textContent = d.leads > 0 ? d.leads + '...' : '...';
      }
    } catch {
      clearInterval(interval);
      btn.classList.remove('scraping');
      btn.textContent = 'Scrape';
    }
  }, 3000);
}

// --- Score Tier Filter ---
function filterByScore(min, max) {
  document.getElementById('filter-score').value = min + '-' + max;
  loadLeads();
  // Scroll to lead explorer
  document.getElementById('leads-table').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// --- Leads (enhanced with sort, pagination, selection) ---
async function loadLeads(append = false) {
  const q = document.getElementById('search-input').value.trim();
  const country = document.getElementById('filter-country').value;
  const state = document.getElementById('filter-state').value;
  const hasEmail = document.getElementById('filter-email').checked;
  const hasPhone = document.getElementById('filter-phone').checked;
  const hasWebsite = document.getElementById('filter-website').checked;
  const practice = document.getElementById('filter-practice').value;
  const scoreRange = document.getElementById('filter-score').value;
  const tag = document.getElementById('filter-tags').value;
  const source = document.getElementById('filter-source').value;

  if (!append) currentOffset = 0;

  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: currentOffset, sort: _currentSort, order: _currentOrder });
  if (q) params.set('q', q);
  if (country) params.set('country', country);
  if (state) params.set('state', state);
  if (hasEmail) params.set('hasEmail', 'true');
  if (hasPhone) params.set('hasPhone', 'true');
  if (hasWebsite) params.set('hasWebsite', 'true');
  if (practice) params.set('practiceArea', practice);
  if (tag) params.set('tag', tag);
  if (source) params.set('source', source);
  if (scoreRange) {
    const [min, max] = scoreRange.split('-').map(Number);
    params.set('minScore', min);
    params.set('maxScore', max);
  }
  // Apply extra filters (e.g., enrichedAfter from quick filter)
  for (const [k, v] of Object.entries(_extraFilters)) {
    if (v) params.set(k, v);
  }

  try {
    const res = await fetch('/api/leads?' + params);
    const d = await safeJSON(res);

    _totalResults = d.total || 0;
    _currentLeads = d.leads;

    const tbody = document.getElementById('leads-body');
    if (!append) tbody.innerHTML = '';

    for (const lead of d.leads) {
      const score = lead.lead_score || 0;
      const scoreColor = score >= 80 ? '#166534' : score >= 55 ? '#1e40af' : score >= 30 ? '#92400e' : '#991b1b';
      const scoreBg = score >= 80 ? '#dcfce7' : score >= 55 ? '#dbeafe' : score >= 30 ? '#fef3c7' : '#fee2e2';
      // Compute completeness dot
      const cFields = [lead.email, lead.phone, lead.website, lead.firm_name, lead.city, lead.bar_number, lead.practice_area, lead.title].filter(Boolean).length;
      const cPct = Math.round(100 * cFields / 8);
      const cColor = cPct >= 75 ? '#22c55e' : cPct >= 50 ? '#3b82f6' : cPct >= 25 ? '#f59e0b' : '#ef4444';
      const isSelected = _selectedIds.has(lead.id);
      const tr = document.createElement('tr');
      tr.dataset.id = lead.id;
      if (isSelected) tr.classList.add('selected');
      tr.innerHTML = `
        <td><input type="checkbox" class="lead-cb" data-id="${lead.id}" ${isSelected ? 'checked' : ''}></td>
        <td style="cursor:pointer;"><span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${cColor};margin-right:3px;" title="${cPct}% complete (${cFields}/8 fields)"></span><strong>${esc(lead.first_name)} ${esc(lead.last_name)}</strong>${lead.email && lead.phone ? '<span title="Gold: email+phone" style="margin-left:3px;font-size:0.55rem;color:#d97706;">&#x2605;</span>' : ''}</td>
        <td>${lead.firm_name ? '<span onclick="event.stopPropagation();openFirmDetail(\'' + esc(lead.firm_name).replace(/'/g, "\\'") + '\')" style="cursor:pointer;color:#2563eb;text-decoration:underline dotted;" title="View firm details">' + esc(lead.firm_name) + '</span>' : ''}</td>
        <td>${esc(lead.city)}</td>
        <td>${esc(lead.state)}</td>
        <td><span style="display:inline-flex;align-items:center;gap:0.2rem;padding:0.1rem 0.4rem;border-radius:4px;font-size:0.72rem;font-weight:600;background:${scoreBg};color:${scoreColor};"><span style="display:inline-block;width:3px;height:12px;border-radius:1px;background:${scoreColor};opacity:0.3;position:relative;overflow:hidden;"><span style="position:absolute;bottom:0;width:100%;height:${score}%;background:${scoreColor};border-radius:1px;"></span></span>${score}</span></td>
        <td class="${lead.email ? 'has-data' : 'no-data'}">${lead.email ? esc(lead.email) + (lead.email_verified === 1 ? ' <span title="Verified" style="color:#16a34a;">&#x2713;</span>' : lead.email_verified === -1 ? ' <span title="Invalid" style="color:#dc2626;">&#x2717;</span>' : '') : '-'}</td>
        <td class="${lead.phone ? 'has-data' : 'no-data'}">${lead.phone ? esc(lead.phone) : '-'}</td>
        <td class="${lead.website ? 'has-data' : 'no-data'}">${lead.website ? '<a href="' + (lead.website.startsWith('http') ? '' : 'https://') + esc(lead.website) + '" target="_blank" rel="noopener">' + esc(lead.website).substring(0, 30) + '</a>' : '-'}</td>
        <td>
          <span>${esc(lead.primary_source)}</span>
          <span class="lead-row-actions">
            ${lead.email ? `<button onclick="event.stopPropagation();navigator.clipboard.writeText('${esc(lead.email)}');toast('Email copied','success');" title="Copy email">Copy</button>` : ''}
            ${lead.phone ? `<button onclick="event.stopPropagation();navigator.clipboard.writeText('${esc(lead.phone)}');toast('Phone copied','success');" title="Copy phone">Phone</button>` : ''}
            <button onclick="event.stopPropagation();quickTagLead(${lead.id})" title="Add tag">Tag</button>
          </span>
        </td>
      `;
      // Click to open detail, double-click to inline edit
      tr.querySelectorAll('td:not(:first-child)').forEach(td => {
        td.style.cursor = 'pointer';
        td.addEventListener('click', () => showLeadDetail(lead));
      });
      // Inline edit: double-click on email, phone, or firm cells
      const editableCells = tr.querySelectorAll('td:nth-child(3), td:nth-child(7), td:nth-child(8)');
      editableCells.forEach((td, ci) => {
        const fieldMap = ['firm_name', 'email', 'phone'];
        td.addEventListener('dblclick', (e) => {
          e.stopPropagation();
          const field = fieldMap[ci];
          const currentVal = lead[field] || '';
          td.innerHTML = `<input type="text" value="${esc(currentVal)}" style="width:100%;padding:0.2rem 0.3rem;font-size:0.78rem;border:2px solid #6366f1;border-radius:4px;outline:none;background:#fff;" autofocus>`;
          const input = td.querySelector('input');
          input.focus();
          input.select();
          const save = () => {
            const newVal = input.value.trim();
            if (newVal !== currentVal) {
              fetch('/api/leads/' + lead.id, {
                method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ [field]: newVal })
              }).then(safeJSON).then(updated => {
                lead[field] = newVal;
                toast(`Updated ${field.replace('_', ' ')}`, 'success');
                loadLeads();
              }).catch(err => { toast('Update failed: ' + err.message, 'error'); loadLeads(); });
            } else { loadLeads(); }
          };
          input.addEventListener('blur', save);
          input.addEventListener('keydown', (ke) => { if (ke.key === 'Enter') { ke.preventDefault(); input.blur(); } if (ke.key === 'Escape') { loadLeads(); } });
        });
      });
      tbody.appendChild(tr);
    }

    if (!append) currentOffset = d.leads.length;
    else currentOffset += d.leads.length;

    // Pagination info
    const showing = Math.min(currentOffset, _totalResults);
    const pageStart = currentOffset - d.leads.length + 1;
    document.getElementById('page-info').textContent = _totalResults > 0
      ? `Showing ${fmt(pageStart)}-${fmt(showing)} of ${fmt(_totalResults)} leads`
      : 'No leads found';
    document.getElementById('prev-page-btn').disabled = (currentOffset - d.leads.length) <= 0;
    document.getElementById('next-page-btn').disabled = currentOffset >= _totalResults;

    document.getElementById('result-count').textContent = `${fmt(_totalResults)} total`;

    // Render active filters bar
    const afBar = document.getElementById('active-filters-bar');
    const chips = [];
    if (q) chips.push(`<span style="background:#e0e7ff;color:#3730a3;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('search-input').value='';loadLeads();">"${esc(q)}" &times;</span>`);
    if (country) chips.push(`<span style="background:#dbeafe;color:#1e40af;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-country').value='';loadLeads();">${country} &times;</span>`);
    if (state) chips.push(`<span style="background:#dbeafe;color:#1e40af;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-state').value='';loadLeads();">${state} &times;</span>`);
    if (hasEmail) chips.push(`<span style="background:#dcfce7;color:#166534;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-email').checked=false;loadLeads();">Has Email &times;</span>`);
    if (hasPhone) chips.push(`<span style="background:#dcfce7;color:#166534;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-phone').checked=false;loadLeads();">Has Phone &times;</span>`);
    if (hasWebsite) chips.push(`<span style="background:#dcfce7;color:#166534;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-website').checked=false;loadLeads();">Has Website &times;</span>`);
    if (practice) chips.push(`<span style="background:#fef3c7;color:#92400e;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-practice').value='';loadLeads();">${esc(practice)} &times;</span>`);
    if (scoreRange) chips.push(`<span style="background:#fef3c7;color:#92400e;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-score').value='';loadLeads();">Score ${scoreRange} &times;</span>`);
    if (tag) chips.push(`<span style="background:#ede9fe;color:#5b21b6;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-tags').value='';loadLeads();">${esc(tag)} &times;</span>`);
    if (source) chips.push(`<span style="background:#ede9fe;color:#5b21b6;padding:0.1rem 0.5rem;border-radius:10px;cursor:pointer;" onclick="document.getElementById('filter-source').value='';loadLeads();">${esc(source)} &times;</span>`);
    afBar.style.display = chips.length > 0 ? 'flex' : 'none';
    afBar.innerHTML = chips.length > 0 ? '<span style="color:var(--text-muted);font-weight:500;">Active:</span>' + chips.join('') : '';

    // Update checkbox listeners
    document.querySelectorAll('.lead-cb').forEach(cb => {
      cb.addEventListener('change', () => {
        const id = parseInt(cb.dataset.id);
        if (cb.checked) _selectedIds.add(id);
        else _selectedIds.delete(id);
        cb.closest('tr').classList.toggle('selected', cb.checked);
        updateFab();
      });
    });
  } catch (err) {
    console.error('Failed to load leads:', err);
  }
}

function nextPage() {
  loadLeads(true);
}
function prevPage() {
  currentOffset = Math.max(0, currentOffset - PAGE_SIZE * 2);
  loadLeads(true);
}
function loadMore() { loadLeads(true); }

// --- Sortable columns ---
document.querySelectorAll('.leads-table th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const sort = th.dataset.sort;
    if (_currentSort === sort) {
      _currentOrder = _currentOrder === 'asc' ? 'desc' : 'asc';
    } else {
      _currentSort = sort;
      _currentOrder = 'desc';
    }
    // Update header styles
    document.querySelectorAll('.leads-table th.sortable').forEach(h => h.classList.remove('active'));
    th.classList.add('active');
    th.querySelector('.sort-arrow').innerHTML = _currentOrder === 'asc' ? '&#x25B2;' : '&#x25BC;';
    loadLeads();
  });
});

// --- Select All ---
document.getElementById('select-all').addEventListener('change', (e) => {
  const checked = e.target.checked;
  document.querySelectorAll('.lead-cb').forEach(cb => {
    cb.checked = checked;
    const id = parseInt(cb.dataset.id);
    if (checked) _selectedIds.add(id);
    else _selectedIds.delete(id);
    cb.closest('tr').classList.toggle('selected', checked);
  });
  updateFab();
});

// --- Floating Action Bar ---
function updateFab() {
  const fab = document.getElementById('fab');
  const count = _selectedIds.size;
  if (count > 0) {
    fab.classList.add('active');
    document.getElementById('fab-count').textContent = `${count} selected`;
  } else {
    fab.classList.remove('active');
  }
}

function clearSelection() {
  _selectedIds.clear();
  document.querySelectorAll('.lead-cb').forEach(cb => {
    cb.checked = false;
    cb.closest('tr').classList.remove('selected');
  });
  document.getElementById('select-all').checked = false;
  updateFab();
}

async function generatePersonalization() {
  if (_selectedIds.size === 0) { toast('Select leads first', 'info'); return; }

  const template = prompt('Template: default, referral, or value', 'default');
  if (!template) return;

  try {
    toast('Generating personalization...', 'info');
    const res = await fetch('/api/leads/generate-personalization', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: [..._selectedIds], template }),
    });
    const d = await safeJSON(res);

    // Show results in state modal (reuse)
    const leads = d.leads.filter(l => l.email);
    let html = `<p style="font-size:0.75rem;color:var(--text-muted);margin-bottom:0.5rem;">${leads.length} leads with email (${d.count} total selected)</p>`;

    if (leads.length > 0) {
      html += '<div style="max-height:400px;overflow-y:auto;">';
      leads.forEach(l => {
        html += `<div style="padding:0.4rem 0;border-bottom:1px solid var(--border,#f3f4f6);">
          <div style="font-size:0.75rem;font-weight:600;">${esc(l.first_name)} ${esc(l.last_name)} <span style="color:var(--text-muted);font-weight:400;">(${esc(l.email || '')})</span></div>
          <div style="font-size:0.72rem;color:var(--text-secondary,#374151);margin-top:0.2rem;font-style:italic;">${esc(l.personalization)}</div>
        </div>`;
      });
      html += '</div>';

      // Export as Instantly CSV button
      html += `<div style="margin-top:0.75rem;display:flex;gap:0.5rem;">
        <button class="btn btn-primary" style="font-size:0.78rem;" onclick="downloadPersonalizationCSV()">Download Instantly CSV</button>
        <button class="btn btn-secondary" style="font-size:0.78rem;" onclick="copyPersonalizationText()">Copy All Text</button>
      </div>`;

      // Store for download
      window._lastPersonalization = leads;
    } else {
      html += '<p style="color:#ef4444;">No selected leads have email addresses.</p>';
    }

    document.getElementById('state-modal-title').textContent = 'Personalized Outreach';
    document.getElementById('state-modal-body').innerHTML = html;
    document.getElementById('state-modal').classList.add('active');
    toast(`Generated personalization for ${leads.length} leads`);
  } catch (err) {
    toast('Failed to generate personalization', 'error');
  }
}

function downloadPersonalizationCSV() {
  const leads = window._lastPersonalization;
  if (!leads || !leads.length) return;
  const headers = ['email', 'first_name', 'last_name', 'company_name', 'personalization'];
  const rows = [headers.join(',')];
  leads.forEach(l => {
    rows.push([
      l.email,
      l.first_name || '',
      l.last_name || '',
      `"${(l.company_name || '').replace(/"/g, '""')}"`,
      `"${(l.personalization || '').replace(/"/g, '""')}"`,
    ].join(','));
  });
  const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `instantly-personalized-${leads.length}-leads.csv`;
  a.click();
  toast(`Downloaded ${leads.length} personalized leads`);
}

async function copyPersonalizationText() {
  const leads = window._lastPersonalization;
  if (!leads || !leads.length) return;
  const text = leads.map(l => `${l.email}: ${l.personalization}`).join('\n');
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
  }
  toast(`Copied ${leads.length} personalization texts`);
}

function compareSelected() {
  if (_selectedIds.size !== 2) {
    toast('Select exactly 2 leads to compare & merge', 'info');
    return;
  }
  const ids = [..._selectedIds];
  openCompareModal(ids[0], ids[1]);
}

function exportSelected() {
  if (_selectedIds.size === 0) return;
  // Build CSV from selected leads in current page
  const selected = _currentLeads.filter(l => _selectedIds.has(l.id));
  if (selected.length === 0) return;

  const headers = ['first_name','last_name','firm_name','city','state','country','email','phone','website','bar_number','bar_status','practice_area','lead_score','primary_source'];
  const rows = [headers.join(',')];
  for (const l of selected) {
    rows.push(headers.map(h => {
      const v = (l[h] || '').toString().replace(/"/g, '""');
      return v.includes(',') || v.includes('"') ? '"' + v + '"' : v;
    }).join(','));
  }
  const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `mortar-selected-${_selectedIds.size}-leads.csv`;
  a.click();
}

function exportSelectedInstantly() {
  if (_selectedIds.size === 0) return;
  const selected = _currentLeads.filter(l => _selectedIds.has(l.id) && l.email);
  if (selected.length === 0) { toast('No selected leads have email addresses', 'info'); return; }

  const headers = ['email','first_name','last_name','company_name','personalization'];
  const rows = [headers.join(',')];
  for (const l of selected) {
    const firm = (l.firm_name || '').replace(/"/g, '""');
    const person = `${l.first_name || ''} is a lawyer${l.practice_area ? ' specializing in ' + l.practice_area : ''} at ${l.firm_name || 'their firm'} in ${l.city || l.state || ''}`.replace(/"/g, '""');
    rows.push([
      l.email,
      l.first_name || '',
      l.last_name || '',
      firm.includes(',') ? '"' + firm + '"' : firm,
      '"' + person + '"'
    ].join(','));
  }
  const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `instantly-${selected.length}-leads.csv`;
  a.click();
  toast(`Exported ${selected.length} leads in Instantly format`);
}

async function copySelectedEmails() {
  const selected = _currentLeads.filter(l => _selectedIds.has(l.id) && l.email);
  if (selected.length === 0) { toast('No selected leads have email addresses', 'info'); return; }
  const emails = selected.map(l => l.email);
  try {
    await navigator.clipboard.writeText(emails.join('\n'));
  } catch {
    const ta = document.createElement('textarea');
    ta.value = emails.join('\n');
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
  }
  toast(`Copied ${emails.length} emails to clipboard`);
}

function quickTagLead(leadId) {
  const tag = prompt('Enter tag name:');
  if (!tag || !tag.trim()) return;
  fetch('/api/leads/tag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ leadIds: [leadId], tag: tag.trim() }),
  })
    .then(safeJSON)
    .then(() => toast(`Tagged with "${tag.trim()}"`, 'success'))
    .catch(err => toast('Failed to tag: ' + err.message, 'error'));
}

function tagSelected() {
  if (_selectedIds.size === 0) return;
  const tag = prompt('Enter tag name:');
  if (!tag || !tag.trim()) return;
  const btn = document.getElementById('btn-tag');
  if (btn) { btn.disabled = true; btn.textContent = 'Tagging...'; }
  fetch('/api/leads/tag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ leadIds: [..._selectedIds], tag: tag.trim() }),
  })
    .then(safeJSON)
    .then(d => {
      document.getElementById('bulk-status').textContent = `Tagged ${d.updated} leads with "${tag.trim()}"`;
      toast(`Tagged ${d.updated} leads with "${tag.trim()}"`);
      clearSelection();
    })
    .catch(err => toast('Failed to tag leads: ' + err.message, 'error'))
    .finally(() => { if (btn) { btn.disabled = false; btn.textContent = 'Tag'; } });
}

function deleteSelected() {
  if (_selectedIds.size === 0) return;
  if (!confirm(`Delete ${_selectedIds.size} leads? This cannot be undone.`)) return;
  const btn = document.getElementById('btn-delete');
  if (btn) { btn.disabled = true; btn.textContent = 'Deleting...'; }
  fetch('/api/leads/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ leadIds: [..._selectedIds] }),
  })
    .then(safeJSON)
    .then(d => {
      document.getElementById('bulk-status').textContent = `Deleted ${d.deleted} leads`;
      toast(`Deleted ${d.deleted} leads`);
      addNotification('&#x1F5D1;', `Deleted <strong>${d.deleted}</strong> leads`, 'warning');
      clearSelection();
      refreshAll();
    })
    .catch(err => toast('Failed to delete leads: ' + err.message, 'error'))
    .finally(() => { if (btn) { btn.disabled = false; btn.textContent = 'Delete'; } });
}


// --- Lead Detail Modal ---
let _currentModalLead = null;
let _editMode = false;

const openLeadDetail = (id) => openLeadById(id);
async function openLeadById(id) {
  try {
    const res = await fetch('/api/leads/' + id);
    if (!res.ok) { toast('Lead not found', 'error'); return; }
    const lead = await safeJSON(res);
    if (lead && lead.id) showLeadDetail(lead);
    else toast('Lead not found', 'error');
  } catch (err) { toast('Failed to load lead: ' + err.message, 'error'); }
}

function showLeadDetail(lead) {
  _currentModalLead = lead;
  _editMode = false;
  document.getElementById('modal-view').style.display = '';
  document.getElementById('modal-edit').style.display = 'none';
  document.getElementById('modal-edit-btn').textContent = 'Edit';
  document.getElementById('modal-name').textContent = `${lead.first_name} ${lead.last_name}`;

  // Completeness meter
  const completenessFields = ['email','phone','website','firm_name','city','bar_number','practice_area','title','linkedin_url','education'];
  const filled = completenessFields.filter(f => lead[f] && lead[f].trim()).length;
  const pct = Math.round(100 * filled / completenessFields.length);
  const meterColor = pct >= 80 ? '#22c55e' : pct >= 50 ? '#3b82f6' : pct >= 30 ? '#f59e0b' : '#ef4444';

  const score = lead.lead_score || 0;
  const scoreColor = score >= 80 ? '#166534' : score >= 55 ? '#1e40af' : score >= 30 ? '#92400e' : '#991b1b';
  const scoreBg = score >= 80 ? '#dcfce7' : score >= 55 ? '#dbeafe' : score >= 30 ? '#fef3c7' : '#fee2e2';

  const fields = [
    ['Firm', lead.firm_name],
    ['City', lead.city],
    ['State', lead.state],
    ['Country', lead.country],
    ['Score', `<span style="padding:0.1rem 0.4rem;border-radius:4px;font-size:0.8rem;font-weight:600;background:${scoreBg};color:${scoreColor};">${score}/100</span>`],
    ['Email', lead.email ? `<a href="mailto:${esc(lead.email)}">${esc(lead.email)}</a>` : '-'],
    ['Phone', lead.phone || '-'],
    ['Website', lead.website ? `<a href="${lead.website.startsWith('http') ? '' : 'https://'}${esc(lead.website)}" target="_blank">${esc(lead.website)}</a>` : '-'],
    ['Bar Number', lead.bar_number || '-'],
    ['Bar Status', lead.bar_status || '-'],
    ['Admitted', lead.admission_date || '-'],
    ['Practice Area', lead.practice_area || '-'],
    ['Title', lead.title || '-'],
    ['LinkedIn', lead.linkedin_url ? `<a href="${esc(lead.linkedin_url)}" target="_blank">View</a>` : '-'],
    ['Education', lead.education || '-'],
    ['Tags', lead.tags || '-'],
    ['Email Source', lead.email_source || '-'],
    ['Phone Source', lead.phone_source || '-'],
    ['Website Src', lead.website_source || '-'],
    ['Primary Source', lead.primary_source || '-'],
  ].filter(([, v]) => v && v !== '-');

  document.getElementById('modal-details').innerHTML =
    `<div style="grid-column:1/-1;margin-bottom:0.3rem;">
      <div style="display:flex;align-items:center;gap:0.5rem;">
        <span style="font-size:0.72rem;color:var(--text-muted,#6b7280);font-weight:500;">Completeness</span>
        <div style="flex:1;height:6px;background:var(--bg-primary,#f3f4f6);border-radius:3px;">
          <div style="width:${pct}%;height:100%;background:${meterColor};border-radius:3px;transition:width 0.3s;"></div>
        </div>
        <span style="font-size:0.72rem;font-weight:600;color:${meterColor};">${filled}/${completenessFields.length}</span>
      </div>
    </div>` +
    fields.map(([label, value]) =>
      `<div class="detail-label">${label}</div><div class="detail-value">${value}</div>`
    ).join('');

  document.getElementById('modal-notes').textContent = lead.notes || 'No notes yet. Click Edit to add.';

  // Quick Action Buttons
  const actBtnStyle = 'padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #d1d5db;border-radius:6px;cursor:pointer;background:#fff;display:flex;align-items:center;gap:0.3rem;';
  document.getElementById('modal-quick-actions').innerHTML = `<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin:0.5rem 0;padding:0.6rem 0;border-top:1px solid var(--border,#e5e7eb);border-bottom:1px solid var(--border,#e5e7eb);">
    ${lead.email ? `<button onclick="navigator.clipboard.writeText('${esc(lead.email)}');toast('Email copied','success');" style="${actBtnStyle}">&#x1F4CB; Copy Email</button>` : ''}
    ${lead.email ? `<button onclick="window.location.href='mailto:${esc(lead.email)}';" style="${actBtnStyle}">&#x2709;&#xFE0F; Send Email</button>` : ''}
    ${lead.phone ? `<button onclick="navigator.clipboard.writeText('${esc(lead.phone)}');toast('Phone copied','success');" style="${actBtnStyle}">&#x1F4DE; Copy Phone</button>` : ''}
    ${lead.website ? `<button onclick="window.open('${(lead.website.startsWith('http') ? '' : 'https://') + esc(lead.website)}','_blank');" style="${actBtnStyle}">&#x1F310; Visit Site</button>` : ''}
    ${lead.linkedin_url ? `<button onclick="window.open('${esc(lead.linkedin_url)}','_blank');" style="${actBtnStyle}">&#x1F517; LinkedIn</button>` : ''}
    <button onclick="loadAiEmail(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #818cf8;border-radius:6px;cursor:pointer;background:#eef2ff;color:#4338ca;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x2728; AI Email</button>
    <button onclick="loadAiScoreExplain(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #818cf8;border-radius:6px;cursor:pointer;background:#eef2ff;color:#4338ca;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x1F4CA; Explain Score</button>
    <button onclick="loadAiTalkingPoints(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #818cf8;border-radius:6px;cursor:pointer;background:#eef2ff;color:#4338ca;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x1F4AC; Talking Points</button>
    <button onclick="loadAiCallScript(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #10b981;border-radius:6px;cursor:pointer;background:#ecfdf5;color:#065f46;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x1F4DE; Call Script</button>
    <button onclick="loadAiWinProb(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #f59e0b;border-radius:6px;cursor:pointer;background:#fffbeb;color:#92400e;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x1F3AF; Win Prob</button>
    <button onclick="openReplyAnalyzer(${lead.id})" style="padding:0.3rem 0.7rem;font-size:0.75rem;border:1px solid #ec4899;border-radius:6px;cursor:pointer;background:#fdf2f8;color:#9d174d;display:flex;align-items:center;gap:0.3rem;font-weight:500;">&#x1F4E8; Analyze Reply</button>
    <button onclick="quickTagLead(${lead.id})" style="${actBtnStyle}">&#x1F3F7;&#xFE0F; Tag</button>
  </div>`;

  // Data provenance pipeline
  const pipelineSteps = [
    { key: 'bar', label: 'Bar Scrape', icon: '\u{1F3DB}\uFE0F' },
    { key: 'profile', label: 'Profile Page', icon: '\u{1F517}' },
    { key: 'martindale', label: 'Martindale', icon: '\u{1F4D6}' },
    { key: 'lawyers-com', label: 'Lawyers.com', icon: '\u{1F4CB}' },
    { key: 'website-crawl', label: 'Website Crawl', icon: '\u{1F310}' },
    { key: 'email-domain', label: 'Email Domain', icon: '\u2709\uFE0F' },
    { key: 'firm-share', label: 'Firm Share', icon: '\u{1F3E2}' },
    { key: 'domain-guess', label: 'Domain Guess', icon: '\u{1F50E}' },
  ];
  const activeSources = new Set([lead.primary_source, lead.email_source, lead.phone_source, lead.website_source].filter(Boolean));
  const enrichSteps = (lead.enrichment_steps || '').split(',').filter(Boolean);
  enrichSteps.forEach(s => activeSources.add(s.trim()));

  let sourcesHtml = '<div style="font-size:0.82rem;color:var(--text-muted);margin-bottom:0.4rem;font-weight:600;">Data Pipeline</div>';
  sourcesHtml += '<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.5rem;">';
  pipelineSteps.forEach(step => {
    const active = activeSources.has(step.key);
    sourcesHtml += `<span style="display:inline-flex;align-items:center;gap:0.2rem;padding:0.15rem 0.5rem;border-radius:12px;font-size:0.7rem;font-weight:500;
      ${active ? 'background:#dcfce7;color:#166534;border:1px solid #86efac;' : 'background:var(--bg-secondary,#f3f4f6);color:var(--text-muted,#9ca3af);border:1px solid var(--border,#e5e7eb);opacity:0.5;'}">
      <span style="font-size:0.8rem;">${step.icon}</span> ${step.label}
    </span>`;
  });
  sourcesHtml += '</div>';

  // Source attribution per field
  const fieldSources = [
    lead.email_source && { field: 'Email', source: lead.email_source },
    lead.phone_source && { field: 'Phone', source: lead.phone_source },
    lead.website_source && { field: 'Website', source: lead.website_source },
  ].filter(Boolean);
  if (fieldSources.length > 0) {
    sourcesHtml += '<div style="font-size:0.7rem;color:var(--text-secondary,#6b7280);margin-top:0.3rem;">';
    fieldSources.forEach(fs => {
      sourcesHtml += `<span style="margin-right:0.75rem;">${fs.field}: <strong>${esc(fs.source)}</strong></span>`;
    });
    sourcesHtml += '</div>';
  }
  if (lead.last_enriched_at) {
    const enrichDate = new Date(lead.last_enriched_at + (lead.last_enriched_at.endsWith('Z') ? '' : 'Z'));
    sourcesHtml += `<div style="font-size:0.68rem;color:var(--text-muted,#9ca3af);margin-top:0.3rem;">Last enriched: ${enrichDate.toLocaleDateString()} ${enrichDate.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</div>`;
  }
  if (lead.last_enrichment_error) {
    sourcesHtml += `<div style="font-size:0.68rem;color:#dc2626;margin-top:0.2rem;background:#fef2f2;padding:0.2rem 0.4rem;border-radius:4px;">Enrichment error (attempt ${lead.enrichment_attempts || 1}): ${esc(lead.last_enrichment_error)}</div>`;
  }

  document.getElementById('modal-sources').innerHTML = sourcesHtml;

  document.getElementById('lead-modal').classList.add('active');

  // Pipeline stage badge
  const stageHtml = `<div style="margin-top:0.5rem;display:flex;align-items:center;gap:0.5rem;">
    <span style="font-size:0.72rem;font-weight:600;color:var(--text-muted);">Stage:</span>
    <select onchange="quickChangeStage(${lead.id}, this.value)" style="padding:0.2rem 0.5rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.75rem;">
      ${['new','contacted','replied','meeting','client'].map(s => `<option value="${s}" ${lead.pipeline_stage === s ? 'selected' : ''}>${s.charAt(0).toUpperCase() + s.slice(1)}</option>`).join('')}
    </select>
    <button onclick="addNoteToLead(${lead.id})" style="font-size:0.72rem;padding:0.15rem 0.5rem;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:4px;cursor:pointer;">+ Note</button>
  </div>`;
  document.getElementById('modal-sources').insertAdjacentHTML('beforeend', stageHtml);

  // Load contact timeline
  loadContactTimeline(lead.id);
  // Wire up log contact button
  document.getElementById('log-contact-btn').onclick = () => {
    document.getElementById('log-contact-form').style.display = document.getElementById('log-contact-form').style.display === 'none' ? '' : 'none';
  };
  document.getElementById('save-contact-btn').onclick = () => saveContactLog(lead.id);

  // Load changelog, similar leads, and timeline async
  Promise.all([
    loadLeadChangelog(lead.id),
    loadSimilarLeads(lead.id),
  ]).then(([changelogHtml, similarHtml]) => {
    const container = document.getElementById('modal-sources');
    if (changelogHtml) container.insertAdjacentHTML('beforeend', changelogHtml);
    if (similarHtml) container.insertAdjacentHTML('beforeend', similarHtml);
    // Add timeline section
    container.insertAdjacentHTML('beforeend', '<div style="margin-top:0.75rem;"><div style="font-size:0.82rem;font-weight:600;color:var(--text-muted);margin-bottom:0.4rem;">Activity Timeline</div><div id="lead-timeline" style="max-height:200px;overflow-y:auto;"></div></div>');
    loadLeadTimeline(lead.id, document.getElementById('lead-timeline'));
    // Add AI Insights section
    container.insertAdjacentHTML('beforeend', '<div id="ai-lead-insights" style="margin-top:0.75rem;"></div>');
    loadAiInsights(lead.id);
  });
}

function toggleEditMode() {
  _editMode = !_editMode;
  const lead = _currentModalLead;
  if (!lead) return;

  document.getElementById('modal-view').style.display = _editMode ? 'none' : '';
  document.getElementById('modal-edit').style.display = _editMode ? '' : 'none';
  document.getElementById('modal-edit-btn').textContent = _editMode ? 'View' : 'Edit';

  if (_editMode) {
    const editFields = [
      { key: 'first_name', label: 'First Name', type: 'text' },
      { key: 'last_name', label: 'Last Name', type: 'text' },
      { key: 'email', label: 'Email', type: 'email' },
      { key: 'phone', label: 'Phone', type: 'text' },
      { key: 'firm_name', label: 'Firm', type: 'text' },
      { key: 'city', label: 'City', type: 'text' },
      { key: 'state', label: 'State', type: 'text' },
      { key: 'website', label: 'Website', type: 'url' },
      { key: 'practice_area', label: 'Practice Area', type: 'text' },
      { key: 'title', label: 'Title', type: 'text' },
      { key: 'linkedin_url', label: 'LinkedIn', type: 'url' },
      { key: 'tags', label: 'Tags', type: 'text' },
      { key: 'notes', label: 'Notes', type: 'textarea' },
    ];
    document.getElementById('modal-edit-fields').innerHTML = editFields.map(f => {
      const val = esc(lead[f.key] || '');
      if (f.type === 'textarea') {
        return `<div class="edit-field"><label>${f.label}</label><textarea id="edit-${f.key}">${val}</textarea></div>`;
      }
      return `<div class="edit-field"><label>${f.label}</label><input type="${f.type}" id="edit-${f.key}" value="${val}"></div>`;
    }).join('');
  }
}

async function saveLeadEdit() {
  if (!_currentModalLead) return;
  const updates = {};
  const fields = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'city', 'state', 'website', 'practice_area', 'title', 'linkedin_url', 'tags', 'notes'];
  for (const f of fields) {
    const el = document.getElementById('edit-' + f);
    if (el) {
      const newVal = el.value.trim();
      if (newVal !== (_currentModalLead[f] || '').trim()) {
        updates[f] = newVal;
      }
    }
  }
  if (Object.keys(updates).length === 0) {
    toggleEditMode();
    return;
  }

  try {
    const res = await fetch(`/api/leads/${_currentModalLead.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    const updated = await safeJSON(res);
    if (updated.id) {
      _currentModalLead = { ..._currentModalLead, ...updates, lead_score: updated.lead_score };
      toggleEditMode();
      showLeadDetail(_currentModalLead);
      toast('Lead updated successfully');
      loadLeads();
    }
  } catch (err) {
    toast('Failed to save lead', 'error');
    console.error('Failed to save lead:', err);
  }
}

function closeModal() {
  _editMode = false;
  document.getElementById('lead-modal').classList.remove('active');
}

// --- State Drilldown Modal ---
async function openStateModal(stateCode) {
  document.getElementById('state-modal-title').textContent = stateCode + ' Analytics';
  document.getElementById('state-modal-body').innerHTML = '<p style="text-align:center;padding:2rem;color:var(--text-muted);">Loading...</p>';
  document.getElementById('state-modal').classList.add('active');

  try {
    const res = await fetch(`/api/leads/state-details/${encodeURIComponent(stateCode)}`);
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);

    const pctBar = (cnt, total, color) => {
      const p = Math.round(100 * cnt / Math.max(total, 1));
      return `<div style="display:flex;align-items:center;gap:0.4rem;">
        <div style="flex:1;height:8px;background:var(--bg-primary,#f3f4f6);border-radius:4px;">
          <div style="width:${p}%;height:100%;background:${color};border-radius:4px;"></div>
        </div>
        <span style="font-size:0.72rem;min-width:60px;text-align:right;">${cnt.toLocaleString()} (${p}%)</span>
      </div>`;
    };

    const sd = d.scoreDistribution;
    const sdTotal = (sd.excellent || 0) + (sd.good || 0) + (sd.fair || 0) + (sd.poor || 0);

    let html = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem;">
        <div style="text-align:center;padding:0.75rem;background:var(--bg-secondary,#f9fafb);border-radius:8px;">
          <div style="font-size:1.5rem;font-weight:700;color:var(--text-primary,#111);">${d.total.toLocaleString()}</div>
          <div style="font-size:0.72rem;color:var(--text-muted,#6b7280);">Total Leads</div>
        </div>
        <div style="text-align:center;padding:0.75rem;background:var(--bg-secondary,#f9fafb);border-radius:8px;">
          <div style="font-size:1.5rem;font-weight:700;color:${d.fields.avg_score >= 55 ? '#22c55e' : '#f59e0b'};">${d.fields.avg_score || 0}</div>
          <div style="font-size:0.72rem;color:var(--text-muted,#6b7280);">Avg Score</div>
        </div>
      </div>

      <div style="margin-bottom:1rem;">
        <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.5rem;">Field Coverage</div>
        <div style="display:grid;gap:0.3rem;">
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Email</span>${pctBar(d.fields.email, d.total, '#22c55e')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Phone</span>${pctBar(d.fields.phone, d.total, '#3b82f6')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Website</span>${pctBar(d.fields.website, d.total, '#8b5cf6')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Firm</span>${pctBar(d.fields.firm, d.total, '#f59e0b')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Practice</span>${pctBar(d.fields.practice, d.total, '#ec4899')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Profile URL</span>${pctBar(d.fields.profile_url, d.total, '#14b8a6')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">Title</span>${pctBar(d.fields.title, d.total, '#6366f1')}</div>
          <div style="display:flex;align-items:center;gap:0.5rem;"><span style="font-size:0.72rem;min-width:70px;">LinkedIn</span>${pctBar(d.fields.linkedin, d.total, '#0077b5')}</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem;">
        <div>
          <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.3rem;">Score Distribution</div>
          <div style="display:grid;gap:0.2rem;">
            <div style="display:flex;align-items:center;gap:0.3rem;font-size:0.72rem;"><span style="width:8px;height:8px;border-radius:2px;background:#22c55e;"></span>Excellent: ${sd.excellent || 0}</div>
            <div style="display:flex;align-items:center;gap:0.3rem;font-size:0.72rem;"><span style="width:8px;height:8px;border-radius:2px;background:#3b82f6;"></span>Good: ${sd.good || 0}</div>
            <div style="display:flex;align-items:center;gap:0.3rem;font-size:0.72rem;"><span style="width:8px;height:8px;border-radius:2px;background:#f59e0b;"></span>Fair: ${sd.fair || 0}</div>
            <div style="display:flex;align-items:center;gap:0.3rem;font-size:0.72rem;"><span style="width:8px;height:8px;border-radius:2px;background:#ef4444;"></span>Poor: ${sd.poor || 0}</div>
          </div>
        </div>
        <div>
          <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.3rem;">Top Cities</div>
          <div style="display:grid;gap:0.15rem;">
            ${d.topCities.map(c => `<div style="display:flex;justify-content:space-between;font-size:0.72rem;"><span>${esc(c.city)}</span><span style="color:var(--text-muted,#9ca3af);">${c.cnt}</span></div>`).join('')}
          </div>
        </div>
      </div>

      <div style="margin-bottom:1rem;">
        <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.3rem;">Top Firms</div>
        <div style="display:flex;flex-wrap:wrap;gap:0.3rem;">
          ${d.topFirms.map(f => `<span style="font-size:0.68rem;padding:0.15rem 0.4rem;background:var(--bg-secondary,#f3f4f6);border:1px solid var(--border,#e5e7eb);border-radius:4px;">${esc(f.firm_name)} <strong>${f.cnt}</strong></span>`).join('')}
        </div>
      </div>

      ${d.recentRuns.length > 0 ? `
        <div>
          <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.3rem;">Recent Scrape Runs</div>
          <div style="display:grid;gap:0.2rem;">
            ${d.recentRuns.map(r => `<div style="display:flex;justify-content:space-between;font-size:0.72rem;padding:0.2rem 0;border-bottom:1px solid var(--border,#f3f4f6);">
              <span>${new Date(r.started_at + 'Z').toLocaleDateString()}</span>
              <span>${r.leads_found} found, ${r.leads_new} new${r.emails_found ? ', ' + r.emails_found + ' emails' : ''}</span>
            </div>`).join('')}
          </div>
        </div>
      ` : ''}

      <div style="margin-top:1rem;display:flex;gap:0.5rem;">
        <button class="btn btn-primary" style="font-size:0.78rem;" onclick="document.getElementById('filter-state').value='${esc(stateCode)}';loadLeads();closeStateModal();">View Leads</button>
        <a href="/api/leads/export/state/${encodeURIComponent(stateCode)}" class="btn btn-secondary" style="font-size:0.78rem;text-decoration:none;">Export CSV</a>
        <button class="btn btn-secondary" style="font-size:0.78rem;" onclick="quickScrape('${esc(stateCode)}');closeStateModal();">Scrape Now</button>
      </div>
    `;

    document.getElementById('state-modal-body').innerHTML = html;
  } catch (err) {
    document.getElementById('state-modal-body').innerHTML = `<p style="color:#ef4444;text-align:center;padding:1rem;">${err.message}</p>`;
  }
}

function closeStateModal() {
  document.getElementById('state-modal').classList.remove('active');
}

// Close modal on Escape
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') { closeModal(); closeStateModal(); }
});

// --- Event Listeners ---
// --- Smart Search Parser ---
// Detects patterns like "in Florida", "with email", "score 80+" and auto-applies filters
// STATE_NAMES already declared above (in command palette section) — reuse it
const STATE_LOOKUP = {};
for (const [code, name] of Object.entries(STATE_NAMES)) { STATE_LOOKUP[name.toLowerCase()] = code; STATE_LOOKUP[code.toLowerCase()] = code; }

function parseSmartSearch(query) {
  let remaining = query;
  const actions = {};

  // "with email" / "has email"
  if (/\b(with|has)\s+email/i.test(remaining)) { actions.hasEmail = true; remaining = remaining.replace(/\b(with|has)\s+email/gi, ''); }
  if (/\b(with|has)\s+phone/i.test(remaining)) { actions.hasPhone = true; remaining = remaining.replace(/\b(with|has)\s+phone/gi, ''); }
  if (/\b(with|has)\s+website/i.test(remaining)) { actions.hasWebsite = true; remaining = remaining.replace(/\b(with|has)\s+website/gi, ''); }
  if (/\b(no|without|missing)\s+email/i.test(remaining)) { actions.noEmail = true; remaining = remaining.replace(/\b(no|without|missing)\s+email/gi, ''); }

  // "score 80+" / "excellent" / "gold"
  const scoreMatch = remaining.match(/\bscore\s*(\d+)\+?/i);
  if (scoreMatch) { actions.minScore = parseInt(scoreMatch[1]); remaining = remaining.replace(scoreMatch[0], ''); }
  if (/\bexcellent/i.test(remaining)) { actions.scoreRange = '80-100'; remaining = remaining.replace(/\bexcellent/gi, ''); }
  if (/\bgold/i.test(remaining)) { actions.hasEmail = true; actions.hasPhone = true; remaining = remaining.replace(/\bgold/gi, ''); }

  // "in Florida" / "in FL" / "in New York"
  const inMatch = remaining.match(/\bin\s+([a-z][a-z\s]+)/i);
  if (inMatch) {
    const place = inMatch[1].trim().toLowerCase();
    if (STATE_LOOKUP[place]) { actions.state = STATE_LOOKUP[place]; remaining = remaining.replace(inMatch[0], ''); }
  }

  actions.text = remaining.trim();
  return actions;
}

document.getElementById('search-input').addEventListener('input', () => {
  clearTimeout(searchTimeout);
  const val = document.getElementById('search-input').value.trim();
  // Check if input looks like a smart search (has keywords)
  const actions = parseSmartSearch(val);
  if (actions.hasEmail || actions.hasPhone || actions.hasWebsite || actions.state || actions.scoreRange || actions.minScore || actions.noEmail) {
    // Apply smart filters
    if (actions.hasEmail) document.getElementById('filter-email').checked = true;
    if (actions.hasPhone) document.getElementById('filter-phone').checked = true;
    if (actions.hasWebsite) document.getElementById('filter-website').checked = true;
    if (actions.state) document.getElementById('filter-state').value = actions.state;
    if (actions.scoreRange) document.getElementById('filter-score').value = actions.scoreRange;
    if (actions.minScore) document.getElementById('filter-score').value = actions.minScore >= 80 ? '80-100' : actions.minScore >= 55 ? '55-79' : actions.minScore >= 30 ? '30-54' : '0-29';
    // Replace search text with just the remaining text
    document.getElementById('search-input').value = actions.text;
    toast('Smart search: applied ' + Object.keys(actions).filter(k => k !== 'text').join(', '), 'info');
  }
  searchTimeout = setTimeout(() => loadLeads(), 300);
});
document.getElementById('filter-country').addEventListener('change', () => loadLeads());
document.getElementById('filter-state').addEventListener('change', () => loadLeads());
document.getElementById('filter-email').addEventListener('change', () => loadLeads());
document.getElementById('filter-phone').addEventListener('change', () => loadLeads());
document.getElementById('filter-website').addEventListener('change', () => loadLeads());
document.getElementById('filter-practice').addEventListener('change', () => loadLeads());
document.getElementById('filter-score').addEventListener('change', () => loadLeads());
document.getElementById('filter-tags').addEventListener('change', () => loadLeads());
document.getElementById('filter-source').addEventListener('change', () => loadLeads());

// Populate tags and source dropdowns
async function loadFilterOptions() {
  try {
    const [tagsRes, sourcesRes] = await Promise.all([
      fetch('/api/leads/tags'),
      fetch('/api/leads/sources'),
    ]);
    if (tagsRes.ok) {
      const tags = await tagsRes.json();
      const sel = document.getElementById('filter-tags');
      tags.forEach(t => {
        const o = document.createElement('option');
        o.value = t;
        o.textContent = t;
        sel.appendChild(o);
      });
    }
    if (sourcesRes.ok) {
      const sources = await sourcesRes.json();
      const sel = document.getElementById('filter-source');
      sources.forEach(s => {
        const o = document.createElement('option');
        o.value = s;
        o.textContent = s;
        sel.appendChild(o);
      });
    }
  } catch (err) {
    console.error('Failed to load filter options:', err);
  }
}
loadFilterOptions();

// --- CSV Import ---
const importZone = document.getElementById('import-zone');
const importFile = document.getElementById('csv-import-file');

importZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  importZone.classList.add('dragover');
});
importZone.addEventListener('dragleave', () => {
  importZone.classList.remove('dragover');
});
importZone.addEventListener('drop', (e) => {
  e.preventDefault();
  importZone.classList.remove('dragover');
  if (e.dataTransfer.files.length > 0) {
    handleImportWithPreview(e.dataTransfer.files[0]);
  }
});
importFile.addEventListener('change', () => {
  if (importFile.files.length > 0) {
    handleImportWithPreview(importFile.files[0]);
    importFile.value = '';
  }
});

// --- Keyboard Shortcuts ---
let _focusedRow = -1;
let _kbdVisible = false;

document.addEventListener('keydown', (e) => {
  // Command palette: Cmd+K / Ctrl+K (works everywhere)
  if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
    e.preventDefault();
    if (_cmdActive) closeCmdPalette();
    else openCmdPalette();
    return;
  }

  // Don't handle shortcuts when typing in inputs
  const tag = e.target.tagName;
  if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') {
    if (e.key === 'Escape') {
      e.target.blur();
      e.target.value = '';
      loadLeads();
    }
    return;
  }

  // Modal open check
  if (document.getElementById('lead-modal').classList.contains('active')) {
    if (e.key === 'Escape') closeModal();
    return;
  }

  switch (e.key) {
    case '/':
      e.preventDefault();
      document.getElementById('search-input').focus();
      break;
    case '?':
      _kbdVisible = !_kbdVisible;
      document.getElementById('kbd-hint').classList.toggle('active', _kbdVisible);
      break;
    case 'Escape':
      // Reset all filters
      document.getElementById('search-input').value = '';
      document.getElementById('filter-country').value = '';
      document.getElementById('filter-state').value = '';
      document.getElementById('filter-practice').value = '';
      document.getElementById('filter-score').value = '';
      document.getElementById('filter-email').checked = false;
      document.getElementById('filter-phone').checked = false;
      document.getElementById('filter-website').checked = false;
      _focusedRow = -1;
      clearSelection();
      loadLeads();
      break;
    case 'j': // Next row
      e.preventDefault();
      navigateRow(1);
      break;
    case 'k': // Prev row
      e.preventDefault();
      navigateRow(-1);
      break;
    case 'Enter': // Open detail
      if (_focusedRow >= 0 && _currentLeads[_focusedRow]) {
        showLeadDetail(_currentLeads[_focusedRow]);
      }
      break;
    case 'x': // Toggle select
      if (_focusedRow >= 0) {
        const rows = document.querySelectorAll('#leads-body tr');
        if (rows[_focusedRow]) {
          const cb = rows[_focusedRow].querySelector('.lead-cb');
          if (cb) {
            cb.checked = !cb.checked;
            cb.dispatchEvent(new Event('change'));
          }
        }
      }
      break;
    case 'r': // Refresh
      refreshAll();
      toast('Refreshing data...', 'info');
      break;
    case 'e': // Export
      window.location.href = '/api/leads/export';
      break;
    case 'c': // Copy emails
      copyAllEmails();
      break;
    case 'd': // Toggle dark mode
      toggleDarkMode();
      break;
    case 'a': // Select all visible
      e.preventDefault();
      document.querySelectorAll('.lead-cb').forEach(cb => {
        if (!cb.checked) { cb.checked = true; cb.dispatchEvent(new Event('change')); }
      });
      break;
    case 't': // Tag selected
      if (_selectedIds.size > 0) tagSelected();
      break;
    case 'm': // Move stage
      if (_selectedIds.size > 0) moveSelectedToStage();
      break;
    case 'n': // New scrape
      window.location.href = '/';
      break;
    case 'f': // Toggle filter panel
      e.preventDefault();
      { const fp = document.querySelector('.filter-panel');
        if (fp) fp.style.display = fp.style.display === 'none' ? '' : 'none'; }
      break;
    case 'p': // Scroll to pipeline
      document.getElementById('pipeline-board').scrollIntoView({ behavior: 'smooth' });
      break;
    case 'g': // Scroll to geographic map
      document.querySelector('.panel h2')?.closest('.panel')?.scrollIntoView({ behavior: 'smooth' });
      break;
    case 'b': // Open AI Brain
      e.preventDefault();
      if (!_aiBrainOpen) toggleAiBrain();
      document.getElementById('ai-brain-input').focus();
      break;
    case 's': // Toggle sort
      e.preventDefault();
      _currentOrder = _currentOrder === 'desc' ? 'asc' : 'desc';
      toast(`Sort: ${_currentSort} ${_currentOrder}`, 'info');
      loadLeads();
      break;
    case 'v': // Cycle view
      e.preventDefault();
      { const views = ['table', 'cards', 'kanban'];
        const ci = views.indexOf(_currentView);
        const next = views[(ci + 1) % views.length];
        switchView(next);
        toast(`View: ${next}`, 'info');
      }
      break;
    case 'Delete': case 'Backspace': // Delete selected
      if (_selectedIds.size > 0) {
        e.preventDefault();
        deleteSelected();
      }
      break;
    case 'G': // Jump to top/bottom
      e.preventDefault();
      if (e.shiftKey) {
        // Shift+G → last row
        const rows = document.querySelectorAll('#leads-body tr');
        if (rows.length > 0) { _focusedRow = rows.length - 1; navigateRow(0); }
      }
      break;
    case 'H': // Jump to start
      e.preventDefault();
      _focusedRow = 0; navigateRow(0);
      break;
    case '[': // Previous page
      e.preventDefault();
      if (currentOffset > 0) {
        currentOffset = Math.max(0, currentOffset - PAGE_SIZE);
        loadLeads();
        toast(`Page ${Math.floor(currentOffset / PAGE_SIZE) + 1}`, 'info');
      }
      break;
    case ']': // Next page
      e.preventDefault();
      if (currentOffset + PAGE_SIZE < _totalResults) {
        currentOffset += PAGE_SIZE;
        loadLeads();
        toast(`Page ${Math.floor(currentOffset / PAGE_SIZE) + 1}`, 'info');
      }
      break;
    case 'i': // Inline edit focused row
      if (_focusedRow >= 0 && _currentLeads[_focusedRow]) {
        e.preventDefault();
        showLeadDetail(_currentLeads[_focusedRow]);
      }
      break;
    case '1': case '2': case '3': case '4': case '5': case '6': {
      // Jump to tab
      const tabs = ['overview', 'leads', 'analytics', 'quality', 'outreach', 'settings'];
      const idx = parseInt(e.key) - 1;
      if (tabs[idx]) {
        switchMainTab(tabs[idx]);
        toast('Switched to ' + tabs[idx], 'info');
      }
      break;
    }
  }
});

function navigateRow(delta) {
  const rows = document.querySelectorAll('#leads-body tr');
  if (rows.length === 0) return;

  // Remove previous highlight
  if (_focusedRow >= 0 && rows[_focusedRow]) {
    rows[_focusedRow].style.outline = '';
  }

  _focusedRow = Math.max(0, Math.min(rows.length - 1, _focusedRow + delta));

  if (rows[_focusedRow]) {
    rows[_focusedRow].style.outline = '2px solid var(--indigo)';
    rows[_focusedRow].style.outlineOffset = '-2px';
    rows[_focusedRow].scrollIntoView({ block: 'nearest' });
  }
}

// --- Saved Views (localStorage) ---
const VIEWS_KEY = 'mortar_saved_views';
let _savedViews = (() => { try { return JSON.parse(localStorage.getItem(VIEWS_KEY) || '[]'); } catch { return []; } })();

function renderViews() {
  const bar = document.getElementById('views-bar');
  if (_savedViews.length === 0) { bar.innerHTML = ''; return; }
  bar.innerHTML = _savedViews.map((v, i) =>
    `<span class="view-chip" onclick="applyView(${i})" title="${esc(v.description || '')}">
      ${esc(v.name)}
      <span class="x-btn" onclick="event.stopPropagation();deleteView(${i})">&times;</span>
    </span>`
  ).join('');
}

function saveCurrentView() {
  const name = prompt('View name:');
  if (!name || !name.trim()) return;
  const view = {
    name: name.trim(),
    q: document.getElementById('search-input').value,
    country: document.getElementById('filter-country').value,
    state: document.getElementById('filter-state').value,
    practice: document.getElementById('filter-practice').value,
    score: document.getElementById('filter-score').value,
    hasEmail: document.getElementById('filter-email').checked,
    hasPhone: document.getElementById('filter-phone').checked,
    hasWebsite: document.getElementById('filter-website').checked,
    sort: _currentSort,
    order: _currentOrder,
  };
  _savedViews.push(view);
  localStorage.setItem(VIEWS_KEY, JSON.stringify(_savedViews));
  renderViews();
}

function applyView(idx) {
  const v = _savedViews[idx];
  if (!v) return;
  document.getElementById('search-input').value = v.q || '';
  document.getElementById('filter-country').value = v.country || '';
  document.getElementById('filter-state').value = v.state || '';
  document.getElementById('filter-practice').value = v.practice || '';
  document.getElementById('filter-score').value = v.score || '';
  document.getElementById('filter-email').checked = !!v.hasEmail;
  document.getElementById('filter-phone').checked = !!v.hasPhone;
  document.getElementById('filter-website').checked = !!v.hasWebsite;
  _currentSort = v.sort || 'score';
  _currentOrder = v.order || 'desc';
  // Highlight active view
  document.querySelectorAll('.view-chip').forEach((c, i) => c.classList.toggle('active', i === idx));
  loadLeads();
}

function deleteView(idx) {
  _savedViews.splice(idx, 1);
  localStorage.setItem(VIEWS_KEY, JSON.stringify(_savedViews));
  renderViews();
}

// --- Column Visibility (Explorer Table) ---
const COLS_KEY = 'mortar_visible_cols';
const EXPLORER_COLUMNS = [
  { key: 'select', label: 'Select', default: true },
  { key: 'name', label: 'Name', default: true },
  { key: 'firm', label: 'Firm', default: true },
  { key: 'city', label: 'City', default: true },
  { key: 'state', label: 'State', default: true },
  { key: 'score', label: 'Score', default: true },
  { key: 'email', label: 'Email', default: true },
  { key: 'phone', label: 'Phone', default: true },
  { key: 'website', label: 'Website', default: true },
  { key: 'source', label: 'Source', default: true },
];
let _visibleCols = (() => { try { return JSON.parse(localStorage.getItem(COLS_KEY)) || EXPLORER_COLUMNS.filter(c => c.default).map(c => c.key); } catch { return EXPLORER_COLUMNS.filter(c => c.default).map(c => c.key); } })();
let _colPickerVisible = false;

function toggleExplorerColumnPicker() {
  _colPickerVisible = !_colPickerVisible;
  const el = document.getElementById('col-toggles');
  if (_colPickerVisible) {
    el.style.display = 'flex';
    el.innerHTML = EXPLORER_COLUMNS.map(c =>
      `<span class="col-toggle ${_visibleCols.includes(c.key) ? 'active' : ''}" onclick="toggleExplorerColumn('${c.key}', this)">${c.label}</span>`
    ).join('');
  } else {
    el.style.display = 'none';
  }
}

function toggleExplorerColumn(key, chip) {
  if (_visibleCols.includes(key)) {
    _visibleCols = _visibleCols.filter(k => k !== key);
  } else {
    _visibleCols.push(key);
  }
  chip.classList.toggle('active');
  localStorage.setItem(COLS_KEY, JSON.stringify(_visibleCols));
  applyColumnVisibility();
}

function applyColumnVisibility() {
  const table = document.getElementById('leads-table');
  table.querySelectorAll('tr').forEach(tr => {
    const cells = tr.querySelectorAll('th, td');
    EXPLORER_COLUMNS.forEach((col, i) => {
      if (cells[i]) {
        cells[i].style.display = _visibleCols.includes(col.key) ? '' : 'none';
      }
    });
  });
}

// --- Dark Mode ---
function toggleDarkMode() {
  document.body.classList.toggle('dark');
  const isDark = document.body.classList.contains('dark');
  localStorage.setItem('mortar_dark_mode', isDark ? '1' : '0');
  document.getElementById('dark-toggle').textContent = isDark ? 'Light Mode' : 'Dark Mode';
}
// Restore dark mode from localStorage
if (localStorage.getItem('mortar_dark_mode') === '1') {
  document.body.classList.add('dark');
  document.getElementById('dark-toggle').textContent = 'Light Mode';
}

// --- Quick Filter Presets ---
let _activeQuickFilter = null;
let _extraFilters = {};

function applyQuickFilter(preset) {
  // Clear previous
  document.querySelectorAll('.qf-chip').forEach(c => c.classList.remove('active'));
  _activeQuickFilter = preset === 'clear' ? null : preset;

  // Reset all filters first
  document.getElementById('search-input').value = '';
  document.getElementById('filter-country').value = '';
  document.getElementById('filter-state').value = '';
  document.getElementById('filter-practice').value = '';
  document.getElementById('filter-score').value = '';
  document.getElementById('filter-email').checked = false;
  document.getElementById('filter-phone').checked = false;
  document.getElementById('filter-website').checked = false;
  _extraFilters = {};

  if (preset === 'clear') { loadLeads(); return; }

  // Highlight active chip
  event.target.classList.add('active');

  switch (preset) {
    case 'gold':
      document.getElementById('filter-email').checked = true;
      document.getElementById('filter-phone').checked = true;
      break;
    case 'missing-email':
      // Search for leads WITHOUT email — we use score filter for poor leads
      document.getElementById('filter-score').value = '0-29';
      break;
    case 'has-website-no-email':
      document.getElementById('filter-website').checked = true;
      // Note: we can't filter "no email" directly, but website+low score approximates it
      break;
    case 'excellent':
      document.getElementById('filter-score').value = '80-100';
      break;
    case 'needs-enrichment':
      document.getElementById('filter-score').value = '0-29';
      break;
    case 'missing-phone':
      // Search for leads with email but no phone
      document.getElementById('filter-email').checked = true;
      break;
    case 'recently-enriched':
      _extraFilters = { enrichedAfter: new Date(Date.now() - 24*60*60*1000).toISOString() };
      _currentSort = 'updated';
      _currentOrder = 'desc';
      break;
    case 'has-linkedin':
      _extraFilters = { hasLinkedin: 'true' };
      break;
    case 'enrichment-failed':
      _extraFilters = { hasEnrichmentError: 'true' };
      break;
    case 'new-today':
      document.getElementById('search-input').value = '';
      _currentSort = 'created';
      _currentOrder = 'desc';
      break;
    case 'new-week':
      _extraFilters = { createdAfter: new Date(Date.now() - 7*24*60*60*1000).toISOString() };
      _currentSort = 'created';
      _currentOrder = 'desc';
      break;
  }
  loadLeads();
}

// --- Copy All Emails ---
async function copyAllEmails() {
  const emails = _currentLeads.filter(l => l.email).map(l => l.email);
  if (emails.length === 0) {
    toast('No emails to copy in current page', 'info');
    return;
  }
  try {
    await navigator.clipboard.writeText(emails.join('\n'));
    toast(`Copied ${emails.length} emails to clipboard`);
  } catch {
    const ta = document.createElement('textarea');
    ta.value = emails.join('\n');
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    toast(`Copied ${emails.length} emails to clipboard`);
  }
}
