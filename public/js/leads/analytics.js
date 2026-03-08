// analytics.js — Analytics tab: pipeline funnel, source analytics, verification stats
// Dependencies: utils.js, app.js

// --- Pipeline Funnel & Source Analytics ---
async function loadAnalytics() {
  try {
    const [funnel, sources] = await Promise.all([
      fetch('/api/analytics/funnel').then(safeJSON),
      fetch('/api/analytics/source-effectiveness').then(safeJSON),
    ]);

    // Funnel chart
    const funnelEl = document.getElementById('funnel-chart');
    const maxCount = Math.max(...funnel.map(f => f.count), 1);
    funnelEl.innerHTML = funnel.map(f => {
      const w = (f.count / maxCount * 100).toFixed(0);
      const stageColors = { new: '#6366f1', contacted: '#0ea5e9', replied: '#f59e0b', meeting: '#8b5cf6', client: '#10b981' };
      return `<div style="margin-bottom:0.4rem;">
        <div style="display:flex;justify-content:space-between;font-size:0.75rem;margin-bottom:0.15rem;">
          <span style="font-weight:600;">${f.stage.charAt(0).toUpperCase() + f.stage.slice(1)}</span>
          <span>${f.count} ${f.dropoff !== null ? '<span style="color:#ef4444;font-size:0.65rem;">(-' + f.dropoff + '%)</span>' : ''}</span>
        </div>
        <div style="height:16px;background:#f3f4f6;border-radius:3px;overflow:hidden;">
          <div style="height:100%;width:${w}%;background:${stageColors[f.stage] || '#6b7280'};border-radius:3px;transition:width 0.5s;"></div>
        </div>
      </div>`;
    }).join('');

    // Source effectiveness
    const srcEl = document.getElementById('source-effectiveness');
    srcEl.innerHTML = sources.map(s => {
      const emailPct = s.total > 0 ? (s.with_email / s.total * 100).toFixed(0) : 0;
      const engagedPct = s.total > 0 ? (s.engaged / s.total * 100).toFixed(0) : 0;
      return `<div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0;border-bottom:1px solid #f3f4f6;font-size:0.75rem;">
        <span style="font-weight:600;min-width:100px;">${esc(s.source)}</span>
        <span style="color:#6b7280;">${s.total} leads</span>
        <span style="color:#059669;">${emailPct}% email</span>
        <span style="color:#4f46e5;">${engagedPct}% engaged</span>
        <span style="margin-left:auto;font-weight:600;color:#374151;">${Math.round(s.avg_score)} avg</span>
      </div>`;
    }).join('');
  } catch (err) { console.error('Analytics error:', err); }
}

// --- Search Autocomplete ---
let _suggestTimer = null;
const searchInput = document.getElementById('search-input');
const suggestBox = document.getElementById('search-suggestions');

searchInput.addEventListener('input', function() {
  clearTimeout(_suggestTimer);
  const q = this.value.trim();
  if (q.length < 2) { suggestBox.style.display = 'none'; return; }
  _suggestTimer = setTimeout(async () => {
    try {
      const suggestions = await fetch('/api/leads/suggest?q=' + encodeURIComponent(q)).then(safeJSON);
      if (suggestions.length === 0) { suggestBox.style.display = 'none'; return; }
      const icons = { name: '&#x1F464;', firm: '&#x1F3E2;', city: '&#x1F4CD;' };
      suggestBox.innerHTML = suggestions.map(s => `
        <div onclick="applySuggestion('${esc(s.label)}', '${s.type}')" style="padding:0.4rem 0.75rem;cursor:pointer;font-size:0.8rem;border-bottom:1px solid #f9fafb;display:flex;align-items:center;gap:0.4rem;" onmouseover="this.style.background='#f3f4f6'" onmouseout="this.style.background=''">
          <span>${icons[s.type] || ''}</span>
          <span>${esc(s.label)}</span>
          <span style="font-size:0.65rem;color:#9ca3af;margin-left:auto;">${s.type}</span>
        </div>
      `).join('');
      suggestBox.style.display = '';
    } catch (err) { suggestBox.style.display = 'none'; }
  }, 200);
});

searchInput.addEventListener('blur', () => {
  setTimeout(() => { suggestBox.style.display = 'none'; }, 200);
});

function applySuggestion(label, type) {
  searchInput.value = label;
  suggestBox.style.display = 'none';
  loadLeads();
  // Save to recent searches
  let recent; try { recent = JSON.parse(localStorage.getItem('mortar-recent-searches') || '[]'); } catch { recent = []; }
  recent.unshift({ label, type, time: Date.now() });
  localStorage.setItem('mortar-recent-searches', JSON.stringify(recent.slice(0, 10)));
}

// --- Quick Actions ---
function startBulkOp(op, btnEl) {
  const endpoints = {
    'find-websites': '/api/leads/find-websites',
    'find-emails': '/api/leads/verify-emails',
    'enrich': '/api/leads/enrich-all',
  };
  const url = endpoints[op];
  if (!url) return;
  // Find the clicked button via event or passed element
  const btn = btnEl || event.target.closest('button');
  const origText = btn ? btn.textContent : '';
  if (btn) { btn.disabled = true; btn.textContent = 'Starting...'; }
  fetch(url, { method: 'POST' }).then(safeJSON).then(d => {
    toast(`${op} started`, 'info');
    setTimeout(loadEnrichmentQueue, 2000);
  }).catch(() => toast('Failed to start ' + op, 'error'))
    .finally(() => { if (btn) { btn.disabled = false; btn.textContent = origText; } });
}

function exportGold() {
  window.location.href = '/api/leads/export?hasEmail=true&hasPhone=true';
  toast('Downloading gold leads (email + phone)', 'success');
}

// --- Email Verification ---
async function loadVerificationStats() {
  try {
    const stats = await fetch('/api/leads/verification-stats').then(safeJSON);
    const panel = document.getElementById('verification-panel');
    if (stats.total === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    const bars = document.getElementById('verification-bars');
    const verPct = (stats.verified / stats.total * 100).toFixed(0);
    const invPct = (stats.invalid / stats.total * 100).toFixed(0);
    const caPct = (stats.catchAll / stats.total * 100).toFixed(0);
    const unvPct = (stats.unverified / stats.total * 100).toFixed(0);
    bars.innerHTML = `
      <div style="text-align:center;"><div style="font-weight:700;font-size:1rem;color:#16a34a;">${stats.verified}</div><div style="font-size:0.7rem;color:#6b7280;">Verified (${verPct}%)</div></div>
      <div style="text-align:center;"><div style="font-weight:700;font-size:1rem;color:#dc2626;">${stats.invalid}</div><div style="font-size:0.7rem;color:#6b7280;">Invalid (${invPct}%)</div></div>
      <div style="text-align:center;"><div style="font-weight:700;font-size:1rem;color:#f59e0b;">${stats.catchAll}</div><div style="font-size:0.7rem;color:#6b7280;">Catch-All (${caPct}%)</div></div>
      <div style="text-align:center;"><div style="font-weight:700;font-size:1rem;color:#6b7280;">${stats.unverified}</div><div style="font-size:0.7rem;color:#6b7280;">Unverified (${unvPct}%)</div></div>
    `;
  } catch (err) { /* hide panel on error */ }
}

async function importVerificationCSV(file) {
  if (!file) return;
  const formData = new FormData();
  formData.append('file', file);
  try {
    const result = await fetch('/api/leads/import-verification', { method: 'POST', body: formData }).then(safeJSON);
    toast(`Updated ${result.updated} of ${result.total} emails`, 'success');
    loadVerificationStats();
  } catch (err) { toast('Import failed', 'error'); }
}
