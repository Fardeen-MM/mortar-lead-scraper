// quality.js — Quality tab: email validation, ICP scoring, saved searches, classification, staleness
// Dependencies: utils.js, app.js

// === Email Validation ===
async function startEmailValidation() {
  const btn = document.getElementById('validate-btn');
  btn.disabled = true;
  btn.textContent = 'Validating...';
  document.getElementById('validation-progress').style.display = '';

  try {
    const result = await fetch('/api/leads/validate-emails', { method: 'POST' }).then(safeJSON);
    document.getElementById('validation-bar').style.width = '100%';
    document.getElementById('validation-status').textContent = 'Complete!';
    document.getElementById('validation-results').innerHTML = `
      <div style="text-align:center;flex:1;"><div style="font-size:1.1rem;font-weight:700;color:#22c55e;">${fmt(result.valid)}</div><div style="font-size:0.68rem;color:#6b7280;">Valid (MX OK)</div></div>
      <div style="text-align:center;flex:1;"><div style="font-size:1.1rem;font-weight:700;color:#ef4444;">${fmt(result.invalid)}</div><div style="font-size:0.68rem;color:#6b7280;">Invalid</div></div>
      <div style="text-align:center;flex:1;"><div style="font-size:1.1rem;font-weight:700;color:#f59e0b;">${fmt(result.disposable)}</div><div style="font-size:0.68rem;color:#6b7280;">Disposable</div></div>
      <div style="text-align:center;flex:1;"><div style="font-size:1.1rem;font-weight:700;color:#3b82f6;">${fmt(result.domainsChecked)}</div><div style="font-size:0.68rem;color:#6b7280;">Domains Checked</div></div>
    `;
    toast('Email validation complete: ' + result.valid + ' valid, ' + result.invalid + ' invalid', 'success');
    loadVerificationStats();
  } catch (err) {
    toast('Validation error: ' + err.message, 'error');
  }
  btn.disabled = false;
  btn.textContent = 'Validate All Emails';
}

// Listen for validation progress via WebSocket
// (handled in ws.onmessage — look for type: 'validation-progress')

// === ICP Scoring (Apollo-style) ===
async function loadIcpPanel() {
  try {
    const [criteria, dist] = await Promise.all([
      fetch('/api/icp/criteria').then(safeJSON),
      fetch('/api/icp/distribution').then(safeJSON),
    ]);

    // Render criteria
    const cList = document.getElementById('icp-criteria-list');
    if (criteria.length === 0) {
      cList.innerHTML = '<div style="font-size:0.78rem;color:#9ca3af;">No ICP criteria defined. Add criteria to score leads against your ideal customer profile.</div>';
    } else {
      cList.innerHTML = criteria.map(c => `
        <div style="display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0;font-size:0.78rem;border-bottom:1px solid #f3e8ff;">
          <span style="font-weight:600;color:#7c3aed;">${c.weight}pts</span>
          <span>${esc(c.label || c.field)} ${esc(c.operator)} ${esc(c.value)}</span>
          <button onclick="deleteIcpCriterion(${c.id})" style="margin-left:auto;background:none;border:none;color:#ef4444;cursor:pointer;font-size:0.8rem;">&times;</button>
        </div>
      `).join('');
    }

    // Render distribution
    if (dist.total > 0) {
      const segs = [
        { label: 'Perfect (90+)', count: dist.perfect, color: '#7c3aed' },
        { label: 'Great (70-89)', count: dist.great, color: '#22c55e' },
        { label: 'Good (50-69)', count: dist.good, color: '#3b82f6' },
        { label: 'Fair (25-49)', count: dist.fair, color: '#f59e0b' },
        { label: 'Poor (<25)', count: dist.poor, color: '#ef4444' },
      ];
      document.getElementById('icp-distribution').innerHTML = segs.map(s => {
        const pct = ((s.count / dist.total) * 100).toFixed(1);
        return `<div style="flex:1;min-width:60px;text-align:center;">
          <div style="font-size:1rem;font-weight:700;color:${s.color};">${fmt(s.count)}</div>
          <div style="font-size:0.65rem;color:#6b7280;">${s.label}</div>
        </div>`;
      }).join('');
    }
  } catch (err) { console.error('ICP error:', err); }
}

function openIcpBuilder() {
  const field = prompt('Field (practice_area, state, city, firm_name, title, email, phone):');
  if (!field) return;
  const operator = prompt('Operator (contains, equals, is_not_empty, in_list):');
  if (!operator) return;
  const value = prompt('Value (for contains/equals/in_list):') || '';
  const weight = parseInt(prompt('Weight (1-50):') || '10');
  const label = prompt('Label (description):') || '';

  fetch('/api/icp/criteria', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ field, operator, value, weight, label }),
  }).then(() => {
    toast('ICP criterion added!', 'success');
    loadIcpPanel();
  });
}

async function deleteIcpCriterion(id) {
  await fetch('/api/icp/criteria/' + id, { method: 'DELETE' });
  toast('Criterion removed', 'success');
  loadIcpPanel();
}

async function scoreAllIcp() {
  toast('Computing ICP scores...', 'info');
  const result = await fetch('/api/icp/score-all', { method: 'POST' }).then(safeJSON);
  toast('Scored ' + fmt(result.updated) + ' leads!', 'success');
  loadIcpPanel();
}

async function loadAiICP() {
  const el = document.getElementById('icp-ai-result');
  if (!el) return;
  el.style.display = 'block';
  el.innerHTML = '<div style="font-size:0.72rem;color:#7c3aed;">AI analyzing your best leads to build ICP...</div>';
  try {
    const res = await fetch('/api/ai/build-icp', { method: 'POST' });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    let html = '<div style="font-size:0.68rem;font-weight:600;color:#7c3aed;margin-bottom:0.3rem;">AI-GENERATED ICP</div>';
    if (d.icp_summary) html += `<div style="font-size:0.75rem;color:#374151;margin-bottom:0.4rem;line-height:1.4;background:#f3e8ff;padding:0.3rem 0.5rem;border-radius:6px;">${esc(d.icp_summary)}</div>`;
    if (d.attributes && d.attributes.length > 0) {
      html += '<div style="font-size:0.62rem;font-weight:600;color:#7c3aed;margin-bottom:0.2rem;">KEY ATTRIBUTES</div>';
      d.attributes.forEach(attr => {
        const wColor = attr.weight === 'high' ? '#7c3aed' : attr.weight === 'medium' ? '#f59e0b' : '#94a3b8';
        html += `<div style="display:flex;align-items:flex-start;gap:0.3rem;padding:0.15rem 0;border-bottom:1px solid #f3e8ff;">
          <span style="font-size:0.55rem;padding:0.05rem 0.2rem;border-radius:3px;background:${wColor};color:#fff;flex-shrink:0;margin-top:0.1rem;">${esc(attr.weight || '')}</span>
          <div style="min-width:0;">
            <div style="font-size:0.68rem;font-weight:600;color:#1f2937;">${esc(attr.dimension || '')}: <span style="font-weight:400;color:#6b7280;">${esc(attr.ideal_value || '')}</span></div>
            <div style="font-size:0.6rem;color:#9ca3af;">${esc(attr.reasoning || '')}</div>
          </div>
        </div>`;
      });
    }
    if (d.anti_patterns && d.anti_patterns.length > 0) {
      html += '<div style="font-size:0.62rem;font-weight:600;color:#dc2626;margin-top:0.3rem;margin-bottom:0.1rem;">ANTI-PATTERNS (avoid)</div>';
      html += d.anti_patterns.map(p => `<span style="display:inline-block;font-size:0.6rem;padding:0.1rem 0.3rem;background:#fef2f2;border:1px solid #fca5a5;border-radius:4px;margin:0.1rem 0.1rem 0.1rem 0;color:#dc2626;">${esc(p)}</span>`).join('');
    }
    if (d.recommendations && d.recommendations.length > 0) {
      html += '<div style="font-size:0.62rem;font-weight:600;color:#7c3aed;margin-top:0.3rem;margin-bottom:0.1rem;">RECOMMENDATIONS</div>';
      html += d.recommendations.map(r => `<div style="font-size:0.62rem;color:#374151;padding:0.05rem 0;">&#x2192; ${esc(r)}</div>`).join('');
    }
    el.innerHTML = html;
  } catch (err) { el.innerHTML = `<div style="color:#dc2626;font-size:0.72rem;">Error: ${esc(err.message)}</div>`; }
}

// === Saved Searches (Apollo-style) ===
async function loadSavedSearches() {
  try {
    const searches = await fetch('/api/saved-searches').then(safeJSON);
    const container = document.getElementById('saved-searches-list');
    if (searches.length === 0) {
      container.innerHTML = '<span style="font-size:0.78rem;color:#9ca3af;">No saved searches yet. Apply filters and click Save Current.</span>';
      return;
    }
    container.innerHTML = searches.map(s => {
      let filters;
      try { filters = JSON.parse(s.filters); } catch { filters = {}; }
      const desc = Object.entries(filters).filter(([k,v]) => v).map(([k,v]) => k + ':' + v).join(', ');
      return `<span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.25rem 0.6rem;background:#eef2ff;border:1px solid #c7d2fe;border-radius:12px;font-size:0.72rem;cursor:pointer;" onclick="applySavedSearch(${s.id},'${esc(s.filters)}')">
        ${s.alert_enabled ? '<span style="color:#f59e0b;">&#x1F514;</span>' : ''}
        <strong>${esc(s.name)}</strong>
        <span style="color:#6b7280;">(${fmt(s.last_count)})</span>
        <span style="color:#ef4444;margin-left:0.2rem;opacity:0.5;" onclick="event.stopPropagation();deleteSavedSearch(${s.id})">&times;</span>
      </span>`;
    }).join('');

    // Check for alerts
    const alerts = await fetch('/api/saved-searches/alerts').then(safeJSON);
    const alertDiv = document.getElementById('search-alerts');
    if (alerts.length > 0) {
      alertDiv.innerHTML = alerts.map(a => `
        <div style="font-size:0.72rem;color:#c2410c;padding:0.2rem 0;">
          <strong>${esc(a.search.name)}</strong>: ${fmt(a.newCount)} new leads since last check!
        </div>
      `).join('');
    } else {
      alertDiv.innerHTML = '';
    }
  } catch (err) { console.error('Saved searches error:', err); }
}

async function saveCurrentSearch() {
  const name = prompt('Name for this search:');
  if (!name) return;
  const filters = {
    search: document.getElementById('search-input').value,
    state: document.getElementById('filter-state').value,
    country: document.getElementById('filter-country').value,
    practice_area: document.getElementById('filter-practice').value,
    hasEmail: document.getElementById('filter-email').checked,
    hasPhone: document.getElementById('filter-phone').checked,
    minScore: document.getElementById('filter-score').value,
  };
  // Remove empty filters
  Object.keys(filters).forEach(k => { if (!filters[k]) delete filters[k]; });
  if (Object.keys(filters).length === 0) {
    toast('Apply some filters first', 'info');
    return;
  }
  const alertEnabled = confirm('Enable alerts for new matching leads?');
  await fetch('/api/saved-searches', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, filters, alertEnabled }),
  });
  toast('Search saved: ' + name, 'success');
  loadSavedSearches();
}

function applySavedSearch(id, filtersJson) {
  const filters = JSON.parse(filtersJson);
  if (filters.search) document.getElementById('search-input').value = filters.search;
  if (filters.state) document.getElementById('filter-state').value = filters.state;
  if (filters.country) document.getElementById('filter-country').value = filters.country;
  if (filters.practice_area) document.getElementById('filter-practice').value = filters.practice_area;
  if (filters.hasEmail) document.getElementById('filter-email').checked = true;
  if (filters.hasPhone) document.getElementById('filter-phone').checked = true;
  if (filters.minScore) document.getElementById('filter-score').value = filters.minScore;
  loadLeads();
  toast('Applied saved search', 'info');
}

async function deleteSavedSearch(id) {
  await fetch('/api/saved-searches/' + id, { method: 'DELETE' });
  toast('Search deleted', 'success');
  loadSavedSearches();
}

// === Buying Signals: Recent Admissions ===
async function loadAdmissionSignals() {
  try {
    const stats = await fetch('/api/signals/admission-stats').then(safeJSON);
    const panel = document.getElementById('admissions-panel');
    if (stats.recent6m === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    document.getElementById('admissions-count').textContent = fmt(stats.recent6m) + ' in 6mo';
    document.getElementById('admissions-list').innerHTML = stats.byState.map(s =>
      `<span onclick="filterByAdmission('${esc(s.state)}')" style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.2rem 0.5rem;background:#fff;border:1px solid #fed7aa;border-radius:8px;font-size:0.72rem;cursor:pointer;">
        <strong>${esc(s.state)}</strong> <span style="color:#c2410c;">${fmt(s.cnt)}</span>
      </span>`
    ).join('');
  } catch (err) { console.error('Admissions error:', err); }
}

function filterByAdmission(state) {
  document.getElementById('filter-state').value = state;
  document.getElementById('search-input').value = 'admitted:recent';
  loadLeads();
  toast('Showing recently admitted in ' + state, 'info');
}

// === Column Visibility Toggle ===
const ALL_COLUMNS = [
  { key: 'first_name', label: 'First Name', default: true },
  { key: 'last_name', label: 'Last Name', default: true },
  { key: 'email', label: 'Email', default: true },
  { key: 'phone', label: 'Phone', default: true },
  { key: 'firm_name', label: 'Firm', default: true },
  { key: 'city', label: 'City', default: true },
  { key: 'state', label: 'State', default: true },
  { key: 'practice_area', label: 'Practice', default: false },
  { key: 'title', label: 'Title', default: false },
  { key: 'website', label: 'Website', default: false },
  { key: 'bar_number', label: 'Bar #', default: false },
  { key: 'bar_status', label: 'Status', default: false },
  { key: 'linkedin_url', label: 'LinkedIn', default: false },
  { key: 'lead_score', label: 'Score', default: true },
  { key: 'confidence_score', label: 'Confidence', default: false },
  { key: 'email_type', label: 'Email Type', default: false },
  { key: 'tags', label: 'Tags', default: false },
  { key: 'pipeline_stage', label: 'Stage', default: false },
  { key: 'source', label: 'Source', default: false },
];
let _visibleColumns = ALL_COLUMNS.filter(c => c.default).map(c => c.key);

// Load saved column config
(async () => {
  try {
    const config = await fetch('/api/table-config').then(safeJSON);
    if (config && config.columns) _visibleColumns = config.columns;
  } catch (err) { console.warn('table-config:', err.message); }
})();

function toggleColumnPicker() {
  const existing = document.getElementById('col-picker-dropdown');
  if (existing) { existing.remove(); return; }

  const btn = document.getElementById('col-picker-btn');
  const rect = btn.getBoundingClientRect();
  const div = document.createElement('div');
  div.id = 'col-picker-dropdown';
  div.style.cssText = 'position:fixed;top:' + (rect.bottom + 4) + 'px;left:' + rect.left + 'px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:0.5rem;box-shadow:0 4px 12px rgba(0,0,0,0.1);z-index:100;max-height:300px;overflow-y:auto;min-width:180px;';

  div.innerHTML = ALL_COLUMNS.map(c => `
    <label style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;font-size:0.78rem;cursor:pointer;">
      <input type="checkbox" ${_visibleColumns.includes(c.key) ? 'checked' : ''} onchange="toggleColumn('${c.key}', this.checked)">
      ${c.label}
    </label>
  `).join('');

  document.body.appendChild(div);
  setTimeout(() => {
    document.addEventListener('click', function closeCol(e) {
      if (!div.contains(e.target) && e.target !== btn) {
        div.remove();
        document.removeEventListener('click', closeCol);
      }
    });
  }, 10);
}

function toggleColumn(key, visible) {
  if (visible && !_visibleColumns.includes(key)) {
    _visibleColumns.push(key);
  } else if (!visible) {
    _visibleColumns = _visibleColumns.filter(k => k !== key);
  }
  // Save config
  fetch('/api/table-config', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ columns: _visibleColumns }),
  });
  loadLeads();
}

// === Email Classification (Apollo-style) ===
async function loadEmailClassification() {
  try {
    const data = await fetch('/api/leads/email-classification').then(safeJSON);
    const panel = document.getElementById('email-class-panel');
    if (!data.breakdown || data.total === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    const colors = { professional: '#22c55e', personal: '#f59e0b', role_based: '#ef4444', generic: '#6b7280', '': '#d1d5db' };
    const labels = { professional: 'Professional', personal: 'Personal', role_based: 'Role-Based', generic: 'Generic', '': 'Unclassified' };
    document.getElementById('email-class-bars').innerHTML = data.breakdown.map(b => {
      const pct = ((b.count / data.total) * 100).toFixed(1);
      const color = colors[b.email_type] || '#6b7280';
      return `<div style="flex:1;min-width:80px;text-align:center;">
        <div style="font-size:1.1rem;font-weight:700;color:${color};">${fmt(b.count)}</div>
        <div style="font-size:0.68rem;color:#6b7280;">${labels[b.email_type] || b.email_type}</div>
        <div style="font-size:0.65rem;color:#9ca3af;">${pct}%</div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Email classification error:', err); }
}

async function classifyEmails() {
  const btn = event ? event.target.closest('button') : null;
  if (btn) { btn.disabled = true; btn.textContent = 'Classifying...'; }
  try {
    toast('Classifying emails...', 'info');
    const res = await fetch('/api/leads/classify-emails', { method: 'POST' });
    if (!res.ok) throw new Error('Server error');
    toast('Emails classified!', 'success');
    loadEmailClassification();
  } catch (err) { toast('Classification failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Classify'; } }
}

// === Confidence Scoring ===
async function loadConfidencePanel() {
  try {
    const data = await fetch('/api/leads/confidence').then(safeJSON);
    const panel = document.getElementById('confidence-panel');
    if (data.total === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    const segments = [
      { label: 'High (80+)', count: data.high, color: '#22c55e' },
      { label: 'Medium (50-79)', count: data.medium, color: '#3b82f6' },
      { label: 'Low (20-49)', count: data.low, color: '#f59e0b' },
      { label: 'Very Low (<20)', count: data.veryLow, color: '#ef4444' },
    ];
    document.getElementById('confidence-bars').innerHTML = segments.map(s => {
      const pct = ((s.count / data.total) * 100).toFixed(1);
      return `<div style="flex:1;min-width:80px;">
        <div style="font-size:1.1rem;font-weight:700;color:${s.color};">${fmt(s.count)}</div>
        <div style="font-size:0.68rem;color:#6b7280;">${s.label}</div>
        <div style="height:4px;background:#e5e7eb;border-radius:2px;margin-top:0.3rem;"><div style="height:100%;background:${s.color};border-radius:2px;width:${pct}%;"></div></div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Confidence error:', err); }
}

async function recomputeConfidence() {
  const btn = event ? event.target.closest('button') : null;
  if (btn) { btn.disabled = true; btn.textContent = 'Computing...'; }
  try {
    toast('Computing confidence scores...', 'info');
    const res = await fetch('/api/leads/compute-confidence', { method: 'POST' });
    if (!res.ok) throw new Error('Server error');
    toast('Confidence scores updated!', 'success');
    loadConfidencePanel();
  } catch (err) { toast('Confidence scoring failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Recompute'; } }
}

// === Data Staleness (ZoomInfo-style) ===
async function loadStalenessPanel() {
  try {
    const data = await fetch('/api/leads/staleness').then(safeJSON);
    const panel = document.getElementById('staleness-panel');
    if (data.total === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    const segments = [
      { label: 'Fresh (<7d)', count: data.fresh, color: '#22c55e' },
      { label: 'Recent (7-30d)', count: data.recent, color: '#3b82f6' },
      { label: 'Aging (30-90d)', count: data.aging, color: '#f59e0b' },
      { label: 'Stale (90d+)', count: data.stale, color: '#ef4444' },
    ];
    document.getElementById('staleness-bars').innerHTML = segments.map(s => {
      const pct = data.total > 0 ? ((s.count / data.total) * 100).toFixed(1) : 0;
      return `<div style="flex:1;min-width:80px;">
        <div style="font-size:1.1rem;font-weight:700;color:${s.color};">${fmt(s.count)}</div>
        <div style="font-size:0.68rem;color:#6b7280;">${s.label}</div>
        <div style="height:4px;background:#e5e7eb;border-radius:2px;margin-top:0.3rem;"><div style="height:100%;background:${s.color};border-radius:2px;width:${pct}%;"></div></div>
      </div>`;
    }).join('');
    // Stale states
    if (data.staleByState.length > 0) {
      document.getElementById('staleness-states').innerHTML = 'Stalest: ' + data.staleByState.slice(0, 5).map(s => `${s.state} (${fmt(s.cnt)})`).join(', ');
    }
  } catch (err) { console.error('Staleness error:', err); }
}
