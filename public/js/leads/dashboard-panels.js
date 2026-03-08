// dashboard-panels.js — All dashboard panels (batches 17-42)
// Dependencies: utils.js, app.js

// === Lead Notes ===
async function showLeadNotes(leadId) {
  try {
    const notes = await fetch(`/api/leads/${leadId}/notes`).then(safeJSON);
    const timeline = await fetch(`/api/leads/${leadId}/timeline`).then(safeJSON);

    const modal = document.createElement('div');
    modal.id = 'notes-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
    modal.innerHTML = `
      <div style="background:#fff;border-radius:12px;padding:1.5rem;width:550px;max-width:90vw;max-height:80vh;overflow-y:auto;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">
          <h3 style="margin:0;font-size:1rem;">Lead Notes & Timeline</h3>
          <button onclick="document.getElementById('notes-modal').remove()" style="border:none;background:none;font-size:1.2rem;cursor:pointer;">&times;</button>
        </div>

        <div style="margin-bottom:1rem;">
          <textarea id="new-note-content" rows="2" placeholder="Add a note..." style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.82rem;font-family:inherit;resize:vertical;"></textarea>
          <button onclick="addLeadNote(${leadId})" style="margin-top:0.3rem;font-size:0.75rem;padding:0.25rem 0.6rem;background:#4f46e5;color:#fff;border:none;border-radius:4px;cursor:pointer;">Add Note</button>
        </div>

        <h4 style="font-size:0.85rem;margin:0 0 0.5rem;">Notes (${notes.length})</h4>
        ${notes.map(n => `
          <div style="padding:0.4rem;border-left:3px solid ${n.pinned ? '#f59e0b' : '#e5e7eb'};margin-bottom:0.3rem;background:#f9fafb;border-radius:0 4px 4px 0;">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <span style="font-size:0.72rem;color:#6b7280;">${esc(n.author)} &bull; ${new Date(n.created_at).toLocaleDateString()}</span>
              <div>
                <button onclick="pinNote(${n.id}, ${leadId})" style="border:none;background:none;cursor:pointer;font-size:0.7rem;">${n.pinned ? '&#x1F4CC;' : '&#x1F4CE;'}</button>
                <button onclick="deleteNoteById(${n.id}, ${leadId})" style="border:none;background:none;cursor:pointer;font-size:0.7rem;color:#dc2626;">&times;</button>
              </div>
            </div>
            <div style="font-size:0.78rem;margin-top:0.2rem;white-space:pre-wrap;">${esc(n.content)}</div>
          </div>
        `).join('') || '<p style="font-size:0.72rem;color:#9ca3af;">No notes yet.</p>'}

        <h4 style="font-size:0.85rem;margin:1rem 0 0.5rem;">Timeline (${timeline.length} events)</h4>
        <div style="max-height:250px;overflow-y:auto;">
          ${timeline.slice(0, 30).map(e => `
            <div style="display:flex;gap:0.5rem;padding:0.25rem 0;border-bottom:1px solid #f3f4f6;font-size:0.72rem;">
              <span style="color:#9ca3af;min-width:60px;">${new Date(e.created_at).toLocaleDateString()}</span>
              <span style="font-weight:500;color:${getTimelineColor(e.type)};">${esc(e.event)}</span>
              ${e.details ? '<span style="color:#6b7280;">' + esc(e.details.substring(0, 80)) + '</span>' : ''}
            </div>
          `).join('')}
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

function getTimelineColor(type) {
  return { activity: '#7c3aed', note: '#0369a1', system: '#6b7280', change: '#ea580c', sequence: '#be185d' }[type] || '#6b7280';
}

async function addLeadNote(leadId) {
  const content = document.getElementById('new-note-content').value.trim();
  if (!content) return;
  await fetch(`/api/leads/${leadId}/notes`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content }),
  });
  document.getElementById('notes-modal').remove();
  showLeadNotes(leadId);
  loadRecentNotes();
}

async function deleteNoteById(noteId, leadId) {
  await fetch('/api/notes/' + noteId, { method: 'DELETE' });
  document.getElementById('notes-modal').remove();
  showLeadNotes(leadId);
  loadRecentNotes();
}

async function pinNote(noteId, leadId) {
  await fetch('/api/notes/' + noteId + '/pin', { method: 'POST' });
  document.getElementById('notes-modal').remove();
  showLeadNotes(leadId);
}

async function loadRecentNotes() {
  try {
    const notes = await fetch('/api/notes/recent').then(safeJSON);
    const list = document.getElementById('recent-notes-list');
    if (notes.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#94a3b8;">No notes yet. Click a lead to add notes.</span>';
      return;
    }
    list.innerHTML = notes.slice(0, 10).map(n => `
      <div style="display:flex;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #e2e8f0;font-size:0.72rem;">
        <div>
          <span style="font-weight:500;">${esc(n.first_name)} ${esc(n.last_name)}</span>
          <span style="color:#6b7280;margin-left:0.3rem;">${esc(n.content.substring(0, 60))}${n.content.length > 60 ? '...' : ''}</span>
        </div>
        <span style="color:#9ca3af;font-size:0.65rem;">${new Date(n.created_at).toLocaleDateString()}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Notes error:', err); }
}

// === Smart Lists (Enhanced) ===
let _activeSmartListId = null;
const SL_TEMPLATES = [
  { name: 'Gold Leads (Email + Phone)', icon: '⭐', filters: { logic: 'AND', conditions: [{ field: 'email', operator: 'is_not_empty', value: '' }, { field: 'phone', operator: 'is_not_empty', value: '' }] } },
  { name: 'High Scorers (80+)', icon: '🏆', filters: { logic: 'AND', conditions: [{ field: 'lead_score', operator: 'greater_than', value: '79' }] } },
  { name: 'Has Email, No Phone', icon: '✉️', filters: { logic: 'AND', conditions: [{ field: 'email', operator: 'is_not_empty', value: '' }, { field: 'phone', operator: 'is_empty', value: '' }] } },
  { name: 'Has Website, No Email', icon: '🌐', filters: { logic: 'AND', conditions: [{ field: 'website', operator: 'is_not_empty', value: '' }, { field: 'email', operator: 'is_empty', value: '' }] } },
  { name: 'Personal Injury Lawyers', icon: '⚖️', filters: { logic: 'AND', conditions: [{ field: 'practice_area', operator: 'contains', value: 'injury' }] } },
  { name: 'Florida Leads', icon: '🌴', filters: { logic: 'AND', conditions: [{ field: 'state', operator: 'equals', value: 'FL' }] } },
  { name: 'New York Leads', icon: '🗽', filters: { logic: 'AND', conditions: [{ field: 'state', operator: 'equals', value: 'NY' }] } },
  { name: 'Needs Enrichment', icon: '🔍', filters: { logic: 'AND', conditions: [{ field: 'email', operator: 'is_empty', value: '' }, { field: 'website', operator: 'is_not_empty', value: '' }] } },
];

async function loadSmartLists() {
  try {
    const lists = await fetch('/api/smart-lists').then(safeJSON);
    const container = document.getElementById('smart-lists-list');
    if (lists.length === 0) {
      container.innerHTML = '<span style="font-size:0.72rem;color:#a16207;">No smart lists yet. Click <b>Templates</b> for quick presets or <b>+ New</b> to build custom filters.</span>';
      return;
    }
    container.innerHTML = lists.map(l => {
      const isActive = _activeSmartListId === l.id;
      return `<span onclick="viewSmartList(${l.id})" style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.25rem 0.6rem;background:${isActive ? '#a16207' : '#fff'};color:${isActive ? '#fff' : 'inherit'};border:1px solid ${isActive ? '#a16207' : '#fde047'};border-radius:8px;font-size:0.72rem;cursor:pointer;transition:all 0.15s;" onmouseover="if(!${isActive})this.style.borderColor='#a16207'" onmouseout="if(!${isActive})this.style.borderColor='#fde047'">
        <strong style="color:${isActive ? '#fff' : '#1f2937'};">${esc(l.name)}</strong>
        <span style="background:${isActive ? 'rgba(255,255,255,0.2)' : '#fef9c3'};padding:0.05rem 0.3rem;border-radius:6px;font-size:0.65rem;font-weight:600;color:${isActive ? '#fff' : '#a16207'};">${fmt(l.last_count)}</span>
        <button onclick="event.stopPropagation();deleteSmartList(${l.id})" style="border:none;background:none;cursor:pointer;color:${isActive ? 'rgba(255,255,255,0.7)' : '#dc2626'};font-size:0.7rem;padding:0;" title="Delete">&times;</button>
      </span>`;
    }).join('');
  } catch (err) { console.error('Smart lists error:', err); }
}

function showSmartListTemplates() {
  const modal = document.createElement('div');
  modal.id = 'sl-templates-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:500px;max-width:90vw;">
      <h3 style="margin:0 0 0.5rem;font-size:1rem;color:#a16207;">Quick Start Templates</h3>
      <p style="font-size:0.75rem;color:#6b7280;margin-bottom:0.75rem;">One-click to create a smart list from these common filters.</p>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.4rem;">
        ${SL_TEMPLATES.map((t, i) => `
          <button onclick="createFromTemplate(${i})" style="display:flex;align-items:center;gap:0.4rem;padding:0.5rem;background:#fffbeb;border:1px solid #fde047;border-radius:8px;cursor:pointer;text-align:left;transition:border-color 0.15s;" onmouseover="this.style.borderColor='#a16207'" onmouseout="this.style.borderColor='#fde047'">
            <span style="font-size:1rem;">${t.icon}</span>
            <span style="font-size:0.75rem;font-weight:500;color:#1f2937;">${esc(t.name)}</span>
          </button>
        `).join('')}
      </div>
      <div style="display:flex;justify-content:flex-end;margin-top:0.75rem;">
        <button onclick="document.getElementById('sl-templates-modal').remove()" style="padding:0.35rem 0.7rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;font-size:0.78rem;">Close</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createFromTemplate(idx) {
  const t = SL_TEMPLATES[idx];
  if (!t) return;
  try {
    await fetch('/api/smart-lists', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: t.name, description: '', filters: t.filters }),
    });
    const m = document.getElementById('sl-templates-modal');
    if (m) m.remove();
    toast('Created "' + t.name + '"', 'success');
    loadSmartLists();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

const SL_FIELD_OPTIONS = `
  <option value="state">State</option><option value="city">City</option><option value="practice_area">Practice Area</option>
  <option value="firm_name">Firm</option><option value="email">Email</option><option value="phone">Phone</option>
  <option value="website">Website</option><option value="lead_score">Score</option><option value="pipeline_stage">Stage</option>
  <option value="tags">Tags</option><option value="email_type">Email Type</option><option value="country">Country</option>
  <option value="bar_status">Bar Status</option><option value="title">Title</option>
`;
const SL_OP_OPTIONS = `
  <option value="equals">Equals</option><option value="not_equals">Not Equals</option><option value="contains">Contains</option>
  <option value="starts_with">Starts With</option><option value="is_not_empty">Has Value</option><option value="is_empty">Is Empty</option>
  <option value="greater_than">Greater Than</option><option value="less_than">Less Than</option>
`;

function showCreateSmartListModal() {
  const modal = document.createElement('div');
  modal.id = 'smart-list-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:520px;max-width:90vw;">
      <h3 style="margin:0 0 0.75rem;font-size:1rem;color:#a16207;">Create Smart List</h3>
      <div style="margin-bottom:0.6rem;">
        <label style="font-size:0.78rem;font-weight:600;">Name</label>
        <input id="sl-name" type="text" placeholder="e.g., FL attorneys with email" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;box-sizing:border-box;">
      </div>
      <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;">
        <label style="font-size:0.78rem;font-weight:600;">Logic</label>
        <select id="sl-logic" onchange="slPreviewCount()" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
          <option value="AND">Match ALL (AND)</option>
          <option value="OR">Match ANY (OR)</option>
        </select>
        <span id="sl-preview" style="margin-left:auto;font-size:0.72rem;font-weight:600;color:#a16207;"></span>
      </div>
      <div id="sl-conditions"></div>
      <div style="display:flex;gap:0.3rem;margin-bottom:0.75rem;">
        <button onclick="addSmartListCondition()" style="font-size:0.68rem;padding:0.2rem 0.5rem;background:#fef9c3;color:#a16207;border:1px solid #fde047;border-radius:4px;cursor:pointer;">+ Add Filter</button>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('smart-list-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createSmartListFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#a16207;color:#fff;cursor:pointer;font-weight:600;">Create List</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  addSmartListCondition(); // Start with one condition row
}

function addSmartListCondition() {
  const container = document.getElementById('sl-conditions');
  const row = document.createElement('div');
  row.className = 'sl-condition';
  row.style.cssText = 'display:grid;grid-template-columns:1fr 1fr 1fr 24px;gap:0.3rem;margin-bottom:0.3rem;align-items:center;';
  row.innerHTML = `
    <select class="sl-field" onchange="slPreviewCount()" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">${SL_FIELD_OPTIONS}</select>
    <select class="sl-op" onchange="slPreviewCount()" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">${SL_OP_OPTIONS}</select>
    <input class="sl-value" type="text" placeholder="Value" oninput="slPreviewDebounced()" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
    <button onclick="this.parentElement.remove();slPreviewCount();" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.85rem;padding:0;" title="Remove">&times;</button>
  `;
  container.appendChild(row);
}

let _slPreviewTimer = null;
function slPreviewDebounced() { clearTimeout(_slPreviewTimer); _slPreviewTimer = setTimeout(slPreviewCount, 400); }

async function slPreviewCount() {
  const el = document.getElementById('sl-preview');
  if (!el) return;
  const filters = _gatherSlFilters();
  if (!filters.conditions.length) { el.textContent = ''; return; }
  el.textContent = 'counting...';
  try {
    const res = await fetch('/api/smart-lists/preview', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ filters }) });
    const d = await safeJSON(res);
    el.textContent = fmt(d.count) + ' leads match';
    el.style.color = d.count > 0 ? '#059669' : '#dc2626';
  } catch { el.textContent = ''; }
}

function _gatherSlFilters() {
  const logic = document.getElementById('sl-logic')?.value || 'AND';
  const rows = document.querySelectorAll('.sl-condition');
  const conditions = [];
  rows.forEach(row => {
    const field = row.querySelector('.sl-field')?.value;
    const operator = row.querySelector('.sl-op')?.value;
    const value = row.querySelector('.sl-value')?.value?.trim() || '';
    if (field && operator) conditions.push({ field, operator, value });
  });
  return { logic, conditions };
}

async function createSmartListFromModal() {
  const name = document.getElementById('sl-name').value.trim();
  if (!name) { toast('Name required', 'error'); return; }
  const filters = _gatherSlFilters();
  if (!filters.conditions.length) { toast('Add at least one filter', 'error'); return; }
  await fetch('/api/smart-lists', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description: '', filters }),
  });
  document.getElementById('smart-list-modal').remove();
  toast('Smart list "' + esc(name) + '" created', 'success');
  loadSmartLists();
}

async function viewSmartList(id) {
  try {
    if (_activeSmartListId === id) {
      // Toggle off — clear filter
      _activeSmartListId = null;
      document.getElementById('smart-list-drilldown').style.display = 'none';
      loadSmartLists();
      loadLeads();
      return;
    }
    _activeSmartListId = id;
    const [leads, lists] = await Promise.all([
      fetch(`/api/smart-lists/${id}/leads?limit=100`).then(safeJSON),
      fetch('/api/smart-lists').then(safeJSON),
    ]);
    const list = lists.find(l => l.id === id);
    const filters = list ? JSON.parse(list.filters) : {};
    const dd = document.getElementById('smart-list-drilldown');
    dd.style.display = 'block';
    const condText = (filters.conditions || []).map(c => `<span style="background:#fef9c3;padding:0.1rem 0.3rem;border-radius:4px;font-size:0.65rem;">${esc(c.field)} ${esc(c.operator)} ${esc(c.value || '—')}</span>`).join(` <span style="font-size:0.6rem;color:#a16207;">${filters.logic || 'AND'}</span> `);
    const emailCount = leads.filter(l => l.email).length;
    const phoneCount = leads.filter(l => l.phone).length;
    const avgScore = leads.length ? Math.round(leads.reduce((s, l) => s + (l.lead_score || 0), 0) / leads.length) : 0;
    dd.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.3rem;">
        <span style="font-size:0.78rem;font-weight:600;color:#a16207;">${esc(list?.name || 'List')} — ${leads.length} leads</span>
        <button onclick="_activeSmartListId=null;document.getElementById('smart-list-drilldown').style.display='none';loadSmartLists();loadLeads();" style="font-size:0.65rem;padding:0.1rem 0.3rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:4px;cursor:pointer;">Clear Filter</button>
      </div>
      <div style="margin-bottom:0.3rem;">${condText}</div>
      <div style="display:flex;gap:0.6rem;font-size:0.68rem;color:#78716c;margin-bottom:0.3rem;">
        <span>✉️ ${emailCount} emails</span>
        <span>📞 ${phoneCount} phones</span>
        <span>📊 Avg score: ${avgScore}</span>
      </div>
      <div style="display:flex;gap:0.3rem;">
        <button onclick="exportSmartListCSV(${id})" style="font-size:0.65rem;padding:0.15rem 0.4rem;background:#fef9c3;color:#a16207;border:1px solid #fde047;border-radius:4px;cursor:pointer;">Export CSV</button>
        <button onclick="batchAiEmailsForList(${id})" style="font-size:0.65rem;padding:0.15rem 0.4rem;background:#eef2ff;color:#4f46e5;border:1px solid #c7d2fe;border-radius:4px;cursor:pointer;">AI Emails</button>
      </div>
    `;
    loadSmartLists(); // re-render to highlight active
    _currentLeads = leads;
    renderLeadTable();
    toast(`Filtered to "${list?.name}" — ${leads.length} leads`, 'info');
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

async function deleteSmartList(id) {
  if (!confirm('Delete this smart list?')) return;
  await fetch('/api/smart-lists/' + id, { method: 'DELETE' });
  if (_activeSmartListId === id) {
    _activeSmartListId = null;
    document.getElementById('smart-list-drilldown').style.display = 'none';
    loadLeads();
  }
  toast('Smart list deleted', 'success');
  loadSmartLists();
}

async function exportSmartListCSV(listId) {
  try {
    const leads = await fetch(`/api/smart-lists/${listId}/leads?limit=500`).then(safeJSON);
    if (!leads.length) { toast('No leads to export', 'error'); return; }
    const headers = ['first_name','last_name','email','phone','firm_name','city','state','practice_area','website','lead_score'];
    const rows = [headers.join(',')];
    leads.forEach(l => {
      rows.push(headers.map(h => `"${(l[h] || '').toString().replace(/"/g, '""')}"`).join(','));
    });
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'smart-list-export.csv';
    a.click();
    toast('Exported ' + leads.length + ' leads', 'success');
  } catch (err) { toast('Export error: ' + err.message, 'error'); }
}

async function batchAiEmailsForList(listId) {
  try {
    const leads = await fetch(`/api/smart-lists/${listId}/leads?limit=10`).then(safeJSON);
    const withEmail = leads.filter(l => l.email);
    if (!withEmail.length) { toast('No leads with email in this list', 'error'); return; }
    // Switch to outreach tab and trigger batch email
    switchMainTab('outreach');
    setTimeout(() => {
      const result = document.getElementById('ai-command-result');
      if (result) {
        result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Generating AI emails for ' + withEmail.length + ' leads from smart list...</div><div id="ai-cmd-emails"></div>';
        const emailEl = document.getElementById('ai-cmd-emails');
        (async () => {
          for (const lead of withEmail.slice(0, 5)) {
            try {
              const eRes = await fetch('/api/ai/write-email', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ leadId: lead.id }) });
              const email = await safeJSON(eRes);
              if (email.error) throw new Error(email.error);
              emailEl.innerHTML += `<div style="background:#1e293b;border:1px solid #334155;border-radius:6px;padding:0.35rem 0.5rem;margin-bottom:0.3rem;">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                  <span style="font-size:0.68rem;color:#e2e8f0;font-weight:500;">${esc(lead.first_name || '')} ${esc(lead.last_name || '')} <span style="color:#64748b;">(${esc(lead.email || '')})</span></span>
                  <button onclick="navigator.clipboard.writeText(this.parentElement.parentElement.querySelector('.cmd-email-body').textContent);toast('Copied!','success');" style="font-size:0.55rem;padding:0.1rem 0.25rem;background:#4f46e5;border:none;border-radius:3px;color:#fff;cursor:pointer;">Copy</button>
                </div>
                <div style="font-size:0.68rem;color:#818cf8;margin-top:0.1rem;">Subject: ${esc(email.subject || '')}</div>
                <div class="cmd-email-body" style="font-size:0.65rem;color:#94a3b8;white-space:pre-wrap;max-height:60px;overflow-y:auto;margin-top:0.1rem;">${esc(email.body || '')}</div>
              </div>`;
            } catch (err) {
              emailEl.innerHTML += `<div style="font-size:0.65rem;color:#fca5a5;">Failed: ${esc(err.message)}</div>`;
            }
          }
        })();
      }
    }, 200);
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === KPI Dashboard (Batch 17) ===
async function loadKpiMetrics() {
  try {
    const m = await fetch('/api/leads/kpi-metrics').then(safeJSON);
    const el = document.getElementById('kpi-cards');
    const cards = [
      { label: 'Total Leads', value: fmt(m.total), color: '#059669' },
      { label: 'Email Rate', value: m.emailRate + '%', color: m.emailRate >= 50 ? '#16a34a' : '#dc2626' },
      { label: 'Phone Rate', value: m.phoneRate + '%', color: m.phoneRate >= 30 ? '#16a34a' : '#f59e0b' },
      { label: 'Avg Score', value: m.avgScore, color: m.avgScore >= 50 ? '#16a34a' : '#f59e0b' },
      { label: 'Added (7d)', value: fmt(m.addedThisWeek), color: '#2563eb' },
      { label: 'Campaign Ready', value: fmt(m.campaignReady), color: '#7c3aed' },
      { label: 'DNC Blocked', value: fmt(m.dncBlocked), color: '#dc2626' },
      { label: 'Active Campaigns', value: fmt(m.activeCampaigns), color: '#4338ca' },
      { label: 'Unique Firms', value: fmt(m.uniqueFirms), color: '#0891b2' },
      { label: 'States Covered', value: fmt(m.statesCovered), color: '#059669' },
    ];
    el.innerHTML = cards.map(c => `
      <div style="background:#fff;border-radius:6px;padding:0.4rem 0.6rem;text-align:center;border:1px solid #e5e7eb;">
        <div style="font-size:1.1rem;font-weight:700;color:${c.color};">${c.value}</div>
        <div style="font-size:0.65rem;color:#6b7280;">${c.label}</div>
      </div>
    `).join('');
  } catch (err) { console.error('KPI error:', err); }
}

// === Scoring Models (Batch 17) ===
async function loadScoringModels() {
  try {
    const models = await fetch('/api/scoring-models').then(safeJSON);
    const el = document.getElementById('scoring-models-list');
    if (models.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#9a3412;">No custom models. Using default scoring.</span>';
      return;
    }
    el.innerHTML = models.map(m => {
      const weights = JSON.parse(m.weights);
      const fields = Object.entries(weights).map(([k, v]) => `${k}:${v}`).join(', ');
      return `
        <div style="display:flex;align-items:center;gap:0.3rem;padding:0.2rem 0.6rem;background:#fff;border:1px solid ${m.is_active ? '#f97316' : '#fed7aa'};border-radius:8px;font-size:0.72rem;">
          ${m.is_active ? '<span style="color:#f97316;font-weight:700;">&#x2713;</span>' : ''}
          <strong>${esc(m.name)}</strong>
          <span style="color:#9a3412;font-size:0.65rem;">${fields}</span>
          ${!m.is_active ? `<button onclick="activateScoringModel(${m.id})" style="border:none;background:none;cursor:pointer;color:#c2410c;font-size:0.65rem;padding:0;">[activate]</button>` : ''}
          <button onclick="deleteScoringModel(${m.id})" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.7rem;padding:0;">&times;</button>
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Scoring models error:', err); }
}

function showCreateScoringModelModal() {
  const modal = document.createElement('div');
  modal.id = 'scoring-model-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:420px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#c2410c;">Create Scoring Model</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Model Name</label>
        <input id="sm-name" type="text" placeholder="e.g., Email-First, Phone-Priority" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <p style="font-size:0.72rem;color:#6b7280;margin-bottom:0.5rem;">Set weights for each field (higher = more important):</p>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.4rem;margin-bottom:0.75rem;">
        <label style="font-size:0.75rem;">Email <input id="sm-email" type="number" value="20" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">Phone <input id="sm-phone" type="number" value="15" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">Website <input id="sm-website" type="number" value="10" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">Firm <input id="sm-firm" type="number" value="10" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">LinkedIn <input id="sm-linkedin" type="number" value="10" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">Practice <input id="sm-practice" type="number" value="5" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">Bar Status <input id="sm-bar-status" type="number" value="5" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
        <label style="font-size:0.75rem;">City <input id="sm-city" type="number" value="5" min="0" max="50" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;margin-left:0.3rem;"></label>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('scoring-model-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createScoringModelFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#c2410c;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createScoringModelFromModal() {
  const name = document.getElementById('sm-name').value.trim();
  if (!name) { alert('Name required'); return; }
  const weights = {
    email: parseInt(document.getElementById('sm-email').value) || 0,
    phone: parseInt(document.getElementById('sm-phone').value) || 0,
    website: parseInt(document.getElementById('sm-website').value) || 0,
    firm_name: parseInt(document.getElementById('sm-firm').value) || 0,
    linkedin_url: parseInt(document.getElementById('sm-linkedin').value) || 0,
    practice_area: parseInt(document.getElementById('sm-practice').value) || 0,
    bar_status: parseInt(document.getElementById('sm-bar-status').value) || 0,
    city: parseInt(document.getElementById('sm-city').value) || 0,
  };
  await fetch('/api/scoring-models', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, weights }),
  });
  document.getElementById('scoring-model-modal').remove();
  toast('Scoring model "' + esc(name) + '" created', 'success');
  loadScoringModels();
}

async function activateScoringModel(id) {
  await fetch('/api/scoring-models/' + id + '/activate', { method: 'POST' });
  toast('Scoring model activated', 'success');
  loadScoringModels();
}

async function applyScoringModel() {
  const res = await fetch('/api/scoring-models/apply', { method: 'POST' }).then(safeJSON);
  toast('Rescored ' + fmt(res.updated) + ' leads', 'success');
  refreshAll();
}

async function deleteScoringModel(id) {
  await fetch('/api/scoring-models/' + id, { method: 'DELETE' });
  toast('Scoring model deleted', 'success');
  loadScoringModels();
}

// === Campaign Management (Batch 17) ===
async function loadCampaigns() {
  try {
    const campaigns = await fetch('/api/campaigns').then(safeJSON);
    const el = document.getElementById('campaigns-list');
    if (campaigns.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#6366f1;">No campaigns yet. Create one to start tracking outreach.</span>';
      return;
    }
    el.innerHTML = campaigns.map(c => {
      const statusColors = { draft: '#6b7280', active: '#16a34a', paused: '#f59e0b', completed: '#7c3aed', archived: '#94a3b8' };
      const color = statusColors[c.status] || '#6b7280';
      return `
        <div style="display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0.6rem;background:#fff;border:1px solid #c7d2fe;border-radius:6px;font-size:0.72rem;">
          <span style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0;"></span>
          <strong style="flex:1;">${esc(c.name)}</strong>
          <span style="color:#6b7280;">${fmt(c.lead_count || 0)} leads</span>
          <span style="font-size:0.65rem;color:${color};font-weight:600;">${c.status}</span>
          <select onchange="updateCampaignStatus(${c.id}, this.value)" style="font-size:0.65rem;padding:0.1rem;border:1px solid #d1d5db;border-radius:3px;">
            ${['draft','active','paused','completed','archived'].map(s => `<option value="${s}" ${s === c.status ? 'selected' : ''}>${s}</option>`).join('')}
          </select>
          <button onclick="viewCampaignLeads(${c.id})" style="border:none;background:none;cursor:pointer;color:#4338ca;font-size:0.65rem;padding:0;">[view]</button>
          <button onclick="deleteCampaign(${c.id})" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.7rem;padding:0;">&times;</button>
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Campaigns error:', err); }
}

function showCreateCampaignModal() {
  const modal = document.createElement('div');
  modal.id = 'campaign-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:400px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#4338ca;">Create Campaign</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Campaign Name</label>
        <input id="camp-name" type="text" placeholder="e.g., FL Immigration Q1 2026" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Description</label>
        <textarea id="camp-desc" placeholder="Campaign goals and notes..." style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;height:60px;resize:vertical;"></textarea>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('campaign-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createCampaignFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#4338ca;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createCampaignFromModal() {
  const name = document.getElementById('camp-name').value.trim();
  if (!name) { alert('Name required'); return; }
  const description = document.getElementById('camp-desc').value.trim();
  await fetch('/api/campaigns', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description }),
  });
  document.getElementById('campaign-modal').remove();
  toast('Campaign "' + esc(name) + '" created', 'success');
  loadCampaigns();
}

async function updateCampaignStatus(id, status) {
  await fetch('/api/campaigns/' + id + '/status', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  });
  toast('Campaign status updated to ' + status, 'success');
  loadCampaigns();
}

async function viewCampaignLeads(id) {
  try {
    const leads = await fetch('/api/campaigns/' + id + '/leads?limit=50').then(safeJSON);
    toast('Campaign has ' + leads.length + ' leads', 'info');
    _currentLeads = leads;
    renderLeadTable();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

async function enrollSelectedInCampaign(campaignId) {
  const ids = [..._selectedIds];
  if (ids.length === 0) { toast('Select leads first', 'warn'); return; }
  await fetch('/api/campaigns/' + campaignId + '/leads', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ leadIds: ids }),
  });
  toast('Enrolled ' + ids.length + ' leads', 'success');
  loadCampaigns();
}

async function deleteCampaign(id) {
  await fetch('/api/campaigns/' + id, { method: 'DELETE' });
  toast('Campaign deleted', 'success');
  loadCampaigns();
}

// === Cross-Source Duplicates (Batch 17) ===
async function loadCrossSourceDuplicates() {
  try {
    const dupes = await fetch('/api/leads/cross-source-duplicates?limit=20').then(safeJSON);
    const el = document.getElementById('cross-source-list');
    if (dupes.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#a21caf;">No cross-source duplicates found.</span>';
      return;
    }
    el.innerHTML = dupes.map(d => `
      <div style="display:flex;align-items:center;gap:0.5rem;padding:0.25rem 0;border-bottom:1px solid #f3e8ff;font-size:0.72rem;">
        <strong style="flex:1;">${esc(d.name)}</strong>
        <span style="color:#6b7280;">${esc(d.city || '')}, ${esc(d.state || '')}</span>
        <span style="background:#fae8ff;color:#a21caf;padding:0.1rem 0.4rem;border-radius:4px;font-size:0.65rem;font-weight:600;">${d.source_count} sources</span>
        <span style="color:#9333ea;font-size:0.65rem;">${esc(d.sources || '')}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Cross-source error:', err); }
}

// === Contact Timeline (Batch 20) ===
async function loadRecentContacts() {
  try {
    const contacts = await fetch('/api/contacts/recent?limit=15').then(safeJSON);
    const el = document.getElementById('contacts-list');
    if (contacts.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#7c3aed;">No contacts logged yet. Track outreach via the contact timeline.</span>';
      return;
    }
    const channelIcons = { email: '&#x2709;', phone: '&#x260E;', linkedin: '&#x1F310;', meeting: '&#x1F4C5;', sms: '&#x1F4AC;', other: '&#x2022;' };
    el.innerHTML = contacts.map(c => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ede9fe;font-size:0.7rem;">
        <span style="font-size:0.9rem;">${channelIcons[c.channel] || channelIcons.other}</span>
        <strong>${esc(c.first_name || '')} ${esc(c.last_name || '')}</strong>
        <span style="color:#6b7280;">${esc(c.channel)}</span>
        <span style="color:#9ca3af;font-size:0.6rem;">${c.direction === 'inbound' ? '&larr;' : '&rarr;'}</span>
        ${c.subject ? `<span style="flex:1;color:#4b5563;">${esc(c.subject)}</span>` : '<span style="flex:1;"></span>'}
        ${c.outcome ? `<span style="font-size:0.6rem;padding:0.05rem 0.3rem;background:#ede9fe;color:#7c3aed;border-radius:3px;">${esc(c.outcome)}</span>` : ''}
        <span style="color:#9ca3af;font-size:0.6rem;">${new Date(c.contact_at).toLocaleDateString()}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Contacts error:', err); }
}

// === Warm-Up Scoring (Batch 20) ===
async function loadWarmUpScores() {
  try {
    const leads = await fetch('/api/leads/warm-up-batch?limit=15').then(safeJSON);
    const el = document.getElementById('warmup-list');
    if (leads.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#e11d48;">No warm-up data. Log contacts to see warm-up scores.</span>';
      return;
    }
    el.innerHTML = leads.map(l => {
      const s = l.warmUp;
      const color = s.score >= 60 ? '#dc2626' : s.score >= 30 ? '#f59e0b' : '#3b82f6';
      const label = s.score >= 60 ? 'Hot' : s.score >= 30 ? 'Warm' : 'Cold';
      return `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ffe4e6;font-size:0.7rem;">
          <span style="width:32px;text-align:center;font-weight:700;color:${color};">${s.score}</span>
          <strong style="flex:1;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</strong>
          <span style="font-size:0.6rem;padding:0.05rem 0.3rem;background:${color}20;color:${color};border-radius:3px;font-weight:600;">${label}</span>
          <span style="color:#6b7280;font-size:0.6rem;">${s.contactsLast30d} contacts/30d</span>
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Warm-up error:', err); }
}

// === Multi-View (Batch 20) ===
let _currentView = 'table';

function switchView(view) {
  _currentView = view;
  ['table', 'cards', 'kanban'].forEach(v => {
    const btn = document.getElementById('view-btn-' + v);
    if (btn) { btn.style.background = v === view ? '#4f46e5' : '#e5e7eb'; btn.style.color = v === view ? '#fff' : '#374151'; }
  });

  // Toggle containers
  const tableEl = document.querySelector('.leads-table-container') || document.querySelector('table');
  if (tableEl) tableEl.style.display = view === 'table' ? '' : 'none';
  document.getElementById('card-view-container').style.display = view === 'cards' ? 'grid' : 'none';
  document.getElementById('kanban-view-container').style.display = view === 'kanban' ? 'flex' : 'none';

  if (view === 'cards') loadCardView();
  if (view === 'kanban') loadKanbanView();
}

async function loadCardView() {
  try {
    const leads = await fetch('/api/leads/card-view?limit=30').then(safeJSON);
    const el = document.getElementById('card-view-container');
    el.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:0.6rem;';
    el.innerHTML = leads.map(l => {
      const scoreBg = l.lead_score >= 60 ? '#dcfce7' : l.lead_score >= 30 ? '#fef3c7' : '#fee2e2';
      const scoreFg = l.lead_score >= 60 ? '#16a34a' : l.lead_score >= 30 ? '#b45309' : '#dc2626';
      const borderColor = l.lead_score >= 60 ? '#22c55e' : l.lead_score >= 30 ? '#f59e0b' : '#e5e7eb';
      return `
      <div onclick="openLeadById(${l.id})" style="background:#fff;border:1px solid #e5e7eb;border-left:3px solid ${borderColor};border-radius:8px;padding:0.7rem 0.8rem;font-size:0.75rem;cursor:pointer;transition:box-shadow 0.15s,transform 0.1s;" onmouseover="this.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)';this.style.transform='translateY(-2px)'" onmouseout="this.style.boxShadow='';this.style.transform=''">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem;">
          <strong style="font-size:0.78rem;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</strong>
          <span style="font-size:0.65rem;padding:0.1rem 0.35rem;background:${scoreBg};color:${scoreFg};border-radius:6px;font-weight:700;">${l.lead_score || 0}</span>
        </div>
        ${l.firm_name ? `<div style="color:#6b7280;font-size:0.7rem;margin-bottom:0.15rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${esc(l.firm_name)}</div>` : ''}
        <div style="color:#9ca3af;font-size:0.65rem;">${esc(l.city || '')}${l.city && l.state ? ', ' : ''}${esc(l.state || '')}</div>
        <div style="display:flex;gap:0.3rem;margin-top:0.35rem;flex-wrap:wrap;">
          ${l.email ? '<span style="font-size:0.6rem;padding:0.08rem 0.25rem;background:#dbeafe;color:#2563eb;border-radius:4px;">&#x2709; email</span>' : ''}
          ${l.phone ? '<span style="font-size:0.6rem;padding:0.08rem 0.25rem;background:#dcfce7;color:#16a34a;border-radius:4px;">&#x260E; phone</span>' : ''}
          ${l.website ? '<span style="font-size:0.6rem;padding:0.08rem 0.25rem;background:#fef3c7;color:#b45309;border-radius:4px;">&#x1F310; web</span>' : ''}
          ${l.linkedin_url ? '<span style="font-size:0.6rem;padding:0.08rem 0.25rem;background:#e0e7ff;color:#4338ca;border-radius:4px;">in</span>' : ''}
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Card view error:', err); }
}

async function loadKanbanView() {
  try {
    const data = await fetch('/api/leads/kanban?limit=20').then(safeJSON);
    const el = document.getElementById('kanban-view-container');
    el.style.cssText = 'display:flex;gap:0.5rem;overflow-x:auto;padding:0.5rem 0;min-height:300px;';
    const stageColors = { new: '#3b82f6', contacted: '#f59e0b', qualified: '#22c55e', proposal: '#8b5cf6', negotiation: '#ec4899', won: '#059669', lost: '#6b7280' };

    el.innerHTML = data.stages.map(stage => {
      const leads = data.data[stage] || [];
      const color = stageColors[stage] || '#6b7280';
      return `
        <div class="kanban-col" data-stage="${stage}"
          ondragover="event.preventDefault();this.classList.add('kanban-dragover')"
          ondragleave="this.classList.remove('kanban-dragover')"
          ondrop="handleKanbanDrop(event,'${stage}');this.classList.remove('kanban-dragover')"
          style="min-width:200px;flex:1;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:0.5rem;transition:background 0.15s,border-color 0.15s;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;padding-bottom:0.3rem;border-bottom:2px solid ${color};">
            <span style="font-weight:600;font-size:0.78rem;color:${color};text-transform:capitalize;">${stage}</span>
            <span style="font-size:0.65rem;padding:0.1rem 0.4rem;background:${color}15;color:${color};border-radius:8px;font-weight:600;">${leads.length}</span>
          </div>
          <div style="display:flex;flex-direction:column;gap:0.3rem;min-height:40px;">
            ${leads.slice(0, 15).map(l => {
              const scoreBg = l.lead_score >= 60 ? '#dcfce7' : l.lead_score >= 30 ? '#fef3c7' : '#fee2e2';
              const scoreFg = l.lead_score >= 60 ? '#16a34a' : l.lead_score >= 30 ? '#b45309' : '#dc2626';
              return `
              <div draggable="true" data-lead-id="${l.id}" class="kanban-card"
                ondragstart="event.dataTransfer.setData('text/plain','${l.id}');this.classList.add('dragging')"
                ondragend="this.classList.remove('dragging')"
                onclick="openLeadById(${l.id})"
                style="background:#fff;border:1px solid #e5e7eb;border-radius:6px;padding:0.4rem 0.5rem;cursor:grab;font-size:0.72rem;transition:box-shadow 0.15s,transform 0.1s;">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                  <strong style="font-size:0.72rem;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</strong>
                  <span style="font-size:0.6rem;padding:0.05rem 0.25rem;background:${scoreBg};color:${scoreFg};border-radius:4px;font-weight:600;">${l.lead_score || 0}</span>
                </div>
                ${l.firm_name ? `<div style="color:#6b7280;font-size:0.65rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${esc(l.firm_name)}</div>` : ''}
                <div style="display:flex;gap:0.2rem;margin-top:0.2rem;">
                  ${l.email ? '<span style="font-size:0.55rem;padding:0.02rem 0.15rem;background:#dbeafe;color:#2563eb;border-radius:2px;">&#x2709;</span>' : ''}
                  ${l.phone ? '<span style="font-size:0.55rem;padding:0.02rem 0.15rem;background:#dcfce7;color:#16a34a;border-radius:2px;">&#x260E;</span>' : ''}
                  ${l.website ? '<span style="font-size:0.55rem;padding:0.02rem 0.15rem;background:#fef3c7;color:#b45309;border-radius:2px;">&#x1F310;</span>' : ''}
                </div>
              </div>`;
            }).join('')}
            ${leads.length === 0 ? '<div style="padding:0.5rem;text-align:center;color:#d1d5db;font-size:0.7rem;">Drop leads here</div>' : ''}
          </div>
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Kanban view error:', err); }
}

async function handleKanbanDrop(event, stage) {
  event.preventDefault();
  const leadId = event.dataTransfer.getData('text/plain');
  if (!leadId) return;
  try {
    await fetch('/api/pipeline/move', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId: parseInt(leadId), stage })
    });
    toast(`Moved to ${stage}`, 'success');
    addNotification('&#x1F4CB;', `Lead moved to <strong>${esc(stage)}</strong> stage`, 'info');
    loadKanbanView();
    loadPipeline();
  } catch (err) { toast('Failed to move lead', 'error'); }
}

// === Search Typeahead (Batch 20) ===
let _typeaheadTimer = null;

function handleTypeahead(query) {
  clearTimeout(_typeaheadTimer);
  const dropdown = document.getElementById('typeahead-dropdown');
  if (!query || query.length < 2) { dropdown.style.display = 'none'; return; }

  _typeaheadTimer = setTimeout(async () => {
    try {
      const results = await fetch('/api/leads/typeahead?q=' + encodeURIComponent(query) + '&limit=8').then(safeJSON);
      if (results.length === 0) { dropdown.style.display = 'none'; return; }

      const matchColors = { name: '#4f46e5', firm: '#059669', email: '#dc2626', city: '#b45309' };
      dropdown.innerHTML = results.map(r => `
        <div onclick="selectTypeaheadResult(${r.id})" style="display:flex;align-items:center;gap:0.4rem;padding:0.4rem 0.8rem;cursor:pointer;border-bottom:1px solid #f3f4f6;font-size:0.78rem;" onmouseover="this.style.background='#f3f4f6'" onmouseout="this.style.background=''">
          <span style="font-size:0.6rem;padding:0.05rem 0.3rem;background:${matchColors[r.match_type]}20;color:${matchColors[r.match_type]};border-radius:3px;">${r.match_type}</span>
          <strong>${esc(r.first_name || '')} ${esc(r.last_name || '')}</strong>
          <span style="color:#6b7280;flex:1;">${esc(r.firm_name || '')}</span>
          <span style="color:#9ca3af;font-size:0.68rem;">${esc(r.city || '')}${r.city && r.state ? ', ' : ''}${esc(r.state || '')}</span>
          <span style="font-size:0.65rem;font-weight:600;color:${r.lead_score >= 60 ? '#16a34a' : '#f59e0b'};">${r.lead_score}</span>
        </div>
      `).join('');
      dropdown.style.display = 'block';
    } catch (err) { dropdown.style.display = 'none'; }
  }, 250);
}

function selectTypeaheadResult(leadId) {
  document.getElementById('typeahead-dropdown').style.display = 'none';
  document.getElementById('global-search').value = '';
  // Focus on this lead in the table
  _currentLeads = _currentLeads || [];
  const lead = _currentLeads.find(l => l.id === leadId);
  if (lead) {
    toast('Found: ' + (lead.first_name || '') + ' ' + (lead.last_name || ''), 'info');
  } else {
    fetch('/api/leads/' + leadId).then(safeJSON).then(l => {
      toast('Lead: ' + (l.first_name || '') + ' ' + (l.last_name || '') + ' (score: ' + l.lead_score + ')', 'info');
    }).catch(err => console.warn('typeahead lookup:', err.message));
  }
}

// Hide typeahead on click outside
document.addEventListener('click', (e) => {
  if (!e.target.closest('#global-search') && !e.target.closest('#typeahead-dropdown')) {
    document.getElementById('typeahead-dropdown').style.display = 'none';
  }
});

// === Filter Facets (Batch 20) ===
async function loadFilterFacets() {
  try {
    const facets = await fetch('/api/leads/filter-facets').then(safeJSON);
    const el = document.getElementById('filter-facets');

    let html = '';

    // States
    html += `<div><strong style="font-size:0.72rem;color:#4f46e5;">States</strong><div style="max-height:120px;overflow-y:auto;margin-top:0.2rem;">`;
    html += facets.states.slice(0, 15).map(s => `
      <div onclick="applyFacetFilter('state','${esc(s.state)}')" style="display:flex;justify-content:space-between;padding:0.15rem 0;cursor:pointer;font-size:0.68rem;" onmouseover="this.style.background='#eef2ff'" onmouseout="this.style.background=''">
        <span>${esc(s.state)}</span><span style="color:#6b7280;">${fmt(s.count)}</span>
      </div>
    `).join('');
    html += `</div></div>`;

    // Practice Areas
    html += `<div><strong style="font-size:0.72rem;color:#4f46e5;">Practice Areas</strong><div style="max-height:120px;overflow-y:auto;margin-top:0.2rem;">`;
    html += facets.practiceAreas.slice(0, 10).map(p => `
      <div onclick="applyFacetFilter('practice_area','${esc(p.practice_area)}')" style="display:flex;justify-content:space-between;padding:0.15rem 0;cursor:pointer;font-size:0.68rem;" onmouseover="this.style.background='#eef2ff'" onmouseout="this.style.background=''">
        <span>${esc(p.practice_area).substring(0, 25)}</span><span style="color:#6b7280;">${fmt(p.count)}</span>
      </div>
    `).join('');
    html += `</div></div>`;

    // Score Ranges
    html += `<div><strong style="font-size:0.72rem;color:#4f46e5;">Score Ranges</strong><div style="margin-top:0.2rem;">`;
    html += facets.scoreRanges.map(r => `
      <div onclick="applyFacetFilter('score','${r.min}-${r.max}')" style="display:flex;justify-content:space-between;padding:0.15rem 0;cursor:pointer;font-size:0.68rem;" onmouseover="this.style.background='#eef2ff'" onmouseout="this.style.background=''">
        <span>${r.label}</span><span style="color:#6b7280;">${fmt(r.count)}</span>
      </div>
    `).join('');
    html += `</div></div>`;

    // Pipeline Stages
    html += `<div><strong style="font-size:0.72rem;color:#4f46e5;">Pipeline Stages</strong><div style="margin-top:0.2rem;">`;
    html += facets.stages.map(s => `
      <div onclick="applyFacetFilter('pipeline_stage','${esc(s.stage)}')" style="display:flex;justify-content:space-between;padding:0.15rem 0;cursor:pointer;font-size:0.68rem;" onmouseover="this.style.background='#eef2ff'" onmouseout="this.style.background=''">
        <span style="text-transform:capitalize;">${esc(s.stage)}</span><span style="color:#6b7280;">${fmt(s.count)}</span>
      </div>
    `).join('');
    html += `</div></div>`;

    el.innerHTML = html;
  } catch (err) { console.error('Facets error:', err); }
}

function applyFacetFilter(field, value) {
  toast('Filtering by ' + field + ': ' + value, 'info');
  // Apply filter to lead table search
  const searchInput = document.getElementById('global-search');
  if (searchInput) searchInput.value = value;
  // Reload with filter
  fetch('/api/leads?limit=200&' + field + '=' + encodeURIComponent(value)).then(safeJSON).then(d => {
    _currentLeads = d.leads || d;
    renderLeadTable();
  }).catch(err => toast('Filter failed: ' + err.message, 'error'));
}

// === Enrichment Queue (Batch 21) ===
async function loadEnrichQueue() {
  try {
    const data = await fetch('/api/enrichment-queue/status').then(safeJSON);
    const statsEl = document.getElementById('enrich-queue-stats');
    statsEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#db2777;">${data.pending || 0}</strong> pending</span>
      <span style="font-size:0.72rem;"><strong style="color:#f59e0b;">${data.processing || 0}</strong> processing</span>
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${data.completed || 0}</strong> completed</span>
      <span style="font-size:0.72rem;"><strong style="color:#ef4444;">${data.failed || 0}</strong> failed</span>
    `;
    const itemsEl = document.getElementById('enrich-queue-items');
    if (!data.recent || data.recent.length === 0) {
      itemsEl.innerHTML = '<span style="font-size:0.72rem;color:#db2777;">Queue empty. Select leads and add them to the enrichment queue.</span>';
    } else {
      itemsEl.innerHTML = data.recent.map(item => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fce7f3;font-size:0.72rem;">
          <span style="width:50px;text-align:center;padding:0.1rem 0.3rem;border-radius:4px;font-size:0.6rem;font-weight:600;background:${item.status === 'pending' ? '#fce7f3;color:#db2777' : item.status === 'completed' ? '#ecfdf5;color:#059669' : '#fef3c7;color:#f59e0b'};">${item.status}</span>
          <strong style="flex:1;">${esc(item.first_name || '')} ${esc(item.last_name || '')}</strong>
          <span style="color:#6b7280;font-size:0.65rem;">${esc(item.source || '')}</span>
          <span style="color:#db2777;font-size:0.65rem;">priority: ${item.priority || 0}</span>
        </div>
      `).join('');
    }
  } catch (err) { console.error('Enrich queue error:', err); }
}

async function processEnrichQueue() {
  const btn = event ? event.target.closest('button') : null;
  if (btn) { btn.disabled = true; btn.textContent = 'Processing...'; }
  try {
    const res = await fetch('/api/enrichment-queue/process', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ batchSize: 10 }) }).then(safeJSON);
    toast('Processed ' + (res.processed || 0) + ' items', 'success');
    loadEnrichQueue();
  } catch (err) { toast('Process error: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Process Batch'; } }
}

async function clearEnrichQueue() {
  try {
    const res = await fetch('/api/enrichment-queue/clear', { method: 'DELETE' }).then(safeJSON);
    toast('Cleared ' + (res.deleted || 0) + ' completed items', 'success');
    loadEnrichQueue();
  } catch (err) { toast('Clear error: ' + err.message, 'error'); }
}

// === Firm Intelligence (Batch 21) ===
async function loadFirmIntelligence() {
  try {
    const firms = await fetch('/api/firms?limit=30').then(safeJSON);
    const el = document.getElementById('firm-intel-list');
    if (!firms || firms.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#2563eb;">No firm data yet.</span>';
      return;
    }
    el.innerHTML = firms.map(f => `
      <div onclick="showFirmDetail('${esc(f.firm_name)}')" style="display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0;border-bottom:1px solid #dbeafe;font-size:0.72rem;cursor:pointer;" onmouseover="this.style.background='#eff6ff'" onmouseout="this.style.background=''">
        <strong style="flex:1;color:#1e40af;">${esc(f.firm_name)}</strong>
        <span style="color:#6b7280;">${f.headcount} attorneys</span>
        <span style="color:#059669;font-size:0.65rem;">${f.email_rate || 0}% email</span>
        <span style="color:#2563eb;font-size:0.65rem;">${f.states_count || 1} states</span>
      </div>
    `).join('');
  } catch (err) { console.error('Firm intel error:', err); }
}

async function showFirmDetail(firmName) {
  try {
    const d = await fetch('/api/firms/' + encodeURIComponent(firmName)).then(safeJSON);
    let html = `<h4 style="margin:0 0 0.3rem;color:#1e40af;">${esc(d.firm_name || firmName)}</h4>`;
    html += `<div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:0.3rem;">`;
    html += `<span style="font-size:0.72rem;"><strong>${d.headcount || 0}</strong> attorneys</span>`;
    html += `<span style="font-size:0.72rem;"><strong>${d.email_count || 0}</strong> emails</span>`;
    html += `<span style="font-size:0.72rem;"><strong>${d.phone_count || 0}</strong> phones</span>`;
    html += `</div>`;
    if (d.states && d.states.length > 0) {
      html += `<div style="font-size:0.68rem;color:#6b7280;">States: ${d.states.map(s => esc(s)).join(', ')}</div>`;
    }
    if (d.practice_areas && d.practice_areas.length > 0) {
      html += `<div style="font-size:0.68rem;color:#6b7280;margin-top:0.2rem;">Practice: ${d.practice_areas.slice(0,5).map(p => esc(p)).join(', ')}</div>`;
    }
    document.getElementById('firm-intel-list').innerHTML = html + '<br><button onclick="loadFirmIntelligence()" style="font-size:0.68rem;padding:0.15rem 0.4rem;background:#dbeafe;color:#2563eb;border:1px solid #60a5fa;border-radius:4px;cursor:pointer;">Back to List</button>';
  } catch (err) { toast('Firm detail error: ' + err.message, 'error'); }
}

// === Dedup Merge Queue (Batch 21) ===
async function loadDedupQueue() {
  try {
    const stats = await fetch('/api/dedup/stats').then(safeJSON);
    const statsEl = document.getElementById('dedup-queue-stats');
    statsEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#a16207;">${stats.pending || 0}</strong> pending</span>
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${stats.resolved || 0}</strong> resolved</span>
      ${stats.byType ? Object.entries(stats.byType).map(([k,v]) => `<span style="font-size:0.65rem;color:#6b7280;">${k}: ${v}</span>`).join('') : ''}
    `;
    const items = await fetch('/api/dedup/queue?limit=20').then(safeJSON);
    const el = document.getElementById('dedup-queue-items');
    if (!items || items.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#a16207;">No duplicates in queue. Click "Scan Now" to detect duplicates.</span>';
      return;
    }
    el.innerHTML = items.map(item => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.25rem 0;border-bottom:1px solid #fef9c3;font-size:0.72rem;">
        <span style="padding:0.1rem 0.3rem;border-radius:4px;font-size:0.6rem;font-weight:600;background:#fef9c3;color:#a16207;">${item.confidence || 0}%</span>
        <span style="color:#6b7280;font-size:0.6rem;">${esc(item.match_type || '')}</span>
        <strong style="flex:1;">${esc(item.lead1_name || '')} &harr; ${esc(item.lead2_name || '')}</strong>
        <button onclick="resolveDedupItem(${item.id},'merge',${item.lead1_id})" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#059669;color:#fff;border:none;border-radius:3px;cursor:pointer;">Merge</button>
        <button onclick="resolveDedupItem(${item.id},'skip')" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#f9fafb;color:#6b7280;border:1px solid #d1d5db;border-radius:3px;cursor:pointer;">Skip</button>
      </div>
    `).join('');
  } catch (err) { console.error('Dedup queue error:', err); }
}

async function scanDuplicates() {
  try {
    const res = await fetch('/api/dedup/scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ limit: 200 }) }).then(safeJSON);
    toast('Found ' + (res.found || 0) + ' potential duplicates', 'success');
    loadDedupQueue();
  } catch (err) { toast('Scan error: ' + err.message, 'error'); }
}

async function resolveDedupItem(id, resolution, keepId) {
  try {
    await fetch('/api/dedup/resolve/' + id, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ resolution, keepId }) }).then(safeJSON);
    toast('Resolved: ' + resolution, 'success');
    loadDedupQueue();
  } catch (err) { toast('Resolve error: ' + err.message, 'error'); }
}

// === Audit Log (Batch 21) ===
async function loadAuditLog() {
  try {
    const filter = document.getElementById('audit-filter-action')?.value || '';
    const [log, stats] = await Promise.all([
      fetch('/api/audit/log?limit=30' + (filter ? '&action=' + filter : '')).then(safeJSON),
      fetch('/api/audit/stats').then(safeJSON),
    ]);
    const statsEl = document.getElementById('audit-stats');
    statsEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#7c3aed;">${stats.total || 0}</strong> total</span>
      <span style="font-size:0.72rem;"><strong style="color:#f59e0b;">${stats.today || 0}</strong> today</span>
      ${stats.byAction ? Object.entries(stats.byAction).slice(0,4).map(([k,v]) => `<span style="font-size:0.65rem;color:#6b7280;">${k}: ${v}</span>`).join('') : ''}
    `;
    const el = document.getElementById('audit-log-list');
    if (!log || log.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#7c3aed;">No audit events yet.</span>';
      return;
    }
    el.innerHTML = log.map(e => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ede9fe;font-size:0.72rem;">
        <span style="padding:0.1rem 0.3rem;border-radius:4px;font-size:0.6rem;font-weight:600;background:#ede9fe;color:#7c3aed;">${esc(e.action || '')}</span>
        <span style="color:#6b7280;font-size:0.6rem;">${esc(e.entity_type || '')}</span>
        <strong style="flex:1;">${esc(e.details || '').substring(0, 60)}</strong>
        <span style="color:#9ca3af;font-size:0.6rem;">${e.created_at ? new Date(e.created_at).toLocaleString() : ''}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Audit log error:', err); }
}

async function exportAuditLog() {
  try {
    const data = await fetch('/api/audit/export').then(safeJSON);
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'audit-log-export.json'; a.click();
    URL.revokeObjectURL(url);
    toast('Exported audit log', 'success');
  } catch (err) { toast('Export error: ' + err.message, 'error'); }
}

// === Predictive Scoring (Batch 25) ===
async function loadPredictiveScores() {
  try {
    const data = await fetch('/api/predictive-scores?limit=20').then(safeJSON);
    const msEl = document.getElementById('predictive-model-stats');
    const ms = data.modelStats || {};
    msEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#7c3aed;">${ms.totalLeads || 0}</strong> leads analyzed</span>
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${ms.convertedLeads || 0}</strong> converted</span>
      <span style="font-size:0.72rem;"><strong style="color:#d97706;">${ms.conversionRate || 0}%</strong> conversion rate</span>
    `;
    const impEl = document.getElementById('predictive-importance');
    if (data.featureImportance) {
      const sorted = Object.entries(data.featureImportance).sort((a, b) => b[1].lift - a[1].lift);
      impEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#7c3aed;">Feature Importance:</span> ' +
        sorted.map(([f, v]) => `<span style="display:inline-block;padding:0.1rem 0.3rem;background:#ede9fe;border-radius:4px;font-size:0.6rem;margin:0.1rem;">${f}: ${v.lift}x lift (${v.baseRate}% base)</span>`).join('');
    }
    const el = document.getElementById('predictive-list');
    if (data.predictions && data.predictions.length > 0) {
      el.innerHTML = data.predictions.slice(0, 15).map(p => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ede9fe;font-size:0.72rem;">
          <span style="width:40px;text-align:center;padding:0.1rem 0.3rem;border-radius:4px;font-size:0.65rem;font-weight:700;background:${p.conversionProbability > 60 ? '#dcfce7;color:#16a34a' : p.conversionProbability > 30 ? '#fef9c3;color:#ca8a04' : '#fef2f2;color:#ef4444'};">${p.conversionProbability}%</span>
          <strong style="flex:1;">${esc(p.first_name || '')} ${esc(p.last_name || '')}</strong>
          <span style="color:#6b7280;font-size:0.65rem;">${esc(p.city || '')}, ${esc(p.state || '')}</span>
          <span style="color:#7c3aed;font-size:0.6rem;">score: ${p.lead_score}</span>
        </div>
      `).join('');
    }
  } catch (err) { console.error('Predictive scores error:', err); }
}

// === Team Performance (Batch 25) ===
async function loadTeamPerformance() {
  try {
    const data = await fetch('/api/team/performance').then(safeJSON);
    document.getElementById('team-perf-summary').innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#0284c7;">${data.teamSize || 0}</strong> team members</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${fmt(data.totalAssigned || 0)}</strong> leads assigned</span>
    `;
    const el = document.getElementById('team-perf-list');
    if (!data.members || data.members.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#0284c7;">No team members yet. Assign leads to owners to see performance.</span>';
      return;
    }
    el.innerHTML = data.members.map(m => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #e0f2fe;font-size:0.72rem;">
        <strong style="flex:1;color:#0369a1;">${esc(m.owner)}</strong>
        <span style="color:#6b7280;">${m.assigned} assigned</span>
        <span style="color:#059669;font-size:0.65rem;">${m.contacts} contacts</span>
        <span style="color:#2563eb;font-size:0.65rem;">${m.conversionRate}% conv</span>
        <span style="color:#d97706;font-size:0.65rem;">${m.contactRate}% reached</span>
        <span style="padding:0.1rem 0.3rem;background:#e0f2fe;border-radius:4px;font-size:0.6rem;color:#0284c7;">avg ${m.avg_score}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Team performance error:', err); }
}

// === Email Deliverability (Batch 25) ===
async function loadEmailDeliverability() {
  try {
    const data = await fetch('/api/email/deliverability').then(safeJSON);
    const s = data.summary || {};
    document.getElementById('email-intel-summary').innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${fmt(s.totalEmails || 0)}</strong> emails</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${s.emailRate || 0}%</strong> coverage</span>
      <span style="font-size:0.72rem;"><strong style="color:#2563eb;">${fmt(s.corporateEmail || 0)}</strong> corporate</span>
      <span style="font-size:0.72rem;"><strong style="color:#d97706;">${fmt(s.freeEmail || 0)}</strong> free</span>
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${s.corporateRate || 0}%</strong> corporate rate</span>
    `;
    const el = document.getElementById('email-intel-domains');
    if (data.topDomains && data.topDomains.length > 0) {
      el.innerHTML = data.topDomains.slice(0, 15).map(d => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #d1fae5;font-size:0.68rem;">
          <strong style="flex:1;">${esc(d.domain)}</strong>
          <span style="color:#059669;">${d.count} leads</span>
        </div>
      `).join('');
    }
  } catch (err) { console.error('Email deliverability error:', err); }
}

// === Priority Inbox (Batch 24) ===
async function loadPriorityInbox() {
  try {
    const [leads, recs] = await Promise.all([
      fetch('/api/priority-inbox?limit=15').then(safeJSON),
      fetch('/api/smart-recommendations').then(safeJSON),
    ]);
    const recEl = document.getElementById('smart-recs');
    recEl.innerHTML = recs.map(r => `
      <span style="display:inline-block;padding:0.15rem 0.4rem;border-radius:6px;font-size:0.65rem;background:${r.priority === 'high' ? '#fef2f2;color:#dc2626;border:1px solid #f87171' : r.priority === 'medium' ? '#fef9c3;color:#ca8a04;border:1px solid #eab308' : '#f0f9ff;color:#0369a1;border:1px solid #38bdf8'};">
        ${esc(r.message)}
      </span>
    `).join('');
    const el = document.getElementById('priority-inbox-list');
    if (!leads || leads.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#dc2626;">No priority leads found. Scrape some leads with email first.</span>';
      return;
    }
    el.innerHTML = leads.map((l, i) => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.25rem 0;border-bottom:1px solid #fee2e2;font-size:0.72rem;">
        <span style="width:20px;text-align:center;font-weight:600;color:#dc2626;font-size:0.65rem;">#${i+1}</span>
        <strong style="flex:1;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</strong>
        <span style="color:#6b7280;font-size:0.65rem;">${esc(l.firm_name || '').substring(0, 20)}</span>
        <span style="color:#0d9488;font-size:0.6rem;">${esc(l.city || '')}</span>
        <span style="padding:0.1rem 0.3rem;background:#fee2e2;border-radius:4px;font-size:0.6rem;font-weight:600;color:#dc2626;">${l.priority}</span>
        <span style="color:#6b7280;font-size:0.6rem;">${esc(l.reason || '')}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Priority inbox error:', err); }
}


// === Practice Area Analytics (Batch 24) ===
async function loadPracticeAnalytics() {
  try {
    const data = await fetch('/api/practice-areas/analytics?limit=20').then(safeJSON);
    const el = document.getElementById('practice-areas-list');
    if (data.areas && data.areas.length > 0) {
      el.innerHTML = data.areas.slice(0, 12).map(a => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #d1fae5;font-size:0.68rem;">
          <strong style="flex:1;color:#059669;">${esc(a.practice_area).substring(0, 35)}</strong>
          <span style="color:#6b7280;">${a.lead_count} leads</span>
          <span style="color:#2563eb;font-size:0.6rem;">${a.with_email} email</span>
          <span style="color:#0d9488;font-size:0.6rem;">${a.states} states</span>
          <span style="color:#d97706;font-size:0.6rem;">avg ${a.avg_score}</span>
        </div>
      `).join('');
    } else {
      el.innerHTML = '<span style="font-size:0.72rem;color:#059669;">No practice area data yet.</span>';
    }
    const csEl = document.getElementById('practice-crosssell');
    if (data.crossSell && data.crossSell.length > 0) {
      csEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#059669;display:block;margin-bottom:0.2rem;">Cross-Sell Opportunities</span>' +
        data.crossSell.slice(0, 8).map(c => `
          <div style="font-size:0.65rem;padding:0.1rem 0;color:#6b7280;">
            ${esc(c.area_a).substring(0, 20)} &harr; ${esc(c.area_b).substring(0, 20)} <strong style="color:#059669;">(${c.shared_firms} firms)</strong>
          </div>
        `).join('');
    }
  } catch (err) { console.error('Practice analytics error:', err); }
}

async function loadAiPracticeMarket() {
  const el = document.getElementById('practice-ai-insights');
  if (!el) return;
  el.style.display = 'block';
  el.innerHTML = '<div style="font-size:0.72rem;color:#059669;">Analyzing practice area market...</div>';
  try {
    const res = await fetch('/api/ai/practice-market', { method: 'POST' });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    let html = '<div style="font-size:0.68rem;font-weight:600;color:#059669;margin-bottom:0.3rem;">AI MARKET INTELLIGENCE</div>';
    if (d.market_overview) html += `<div style="font-size:0.72rem;color:#374151;margin-bottom:0.3rem;line-height:1.4;">${esc(d.market_overview)}</div>`;
    if (d.top_opportunities && d.top_opportunities.length > 0) {
      html += '<div style="font-size:0.65rem;font-weight:600;color:#059669;margin-bottom:0.2rem;">TOP OPPORTUNITIES</div>';
      d.top_opportunities.forEach(opp => {
        const qualColor = opp.lead_quality === 'high' ? '#22c55e' : opp.lead_quality === 'medium' ? '#f59e0b' : '#94a3b8';
        html += `<div style="padding:0.2rem 0;border-bottom:1px solid #d1fae5;">
          <div style="display:flex;align-items:center;gap:0.3rem;">
            <span style="font-size:0.72rem;font-weight:600;color:#1f2937;">${esc(opp.practice_area || '')}</span>
            <span style="font-size:0.55rem;padding:0.05rem 0.2rem;border-radius:4px;background:${qualColor};color:#fff;">${esc(opp.lead_quality || '')}</span>
          </div>
          <div style="font-size:0.65rem;color:#6b7280;">${esc(opp.opportunity || '')}</div>
          <div style="font-size:0.62rem;color:#059669;">${esc(opp.recommended_action || '')}</div>
        </div>`;
      });
    }
    if (d.underserved_areas && d.underserved_areas.length > 0) {
      html += '<div style="font-size:0.65rem;font-weight:600;color:#dc2626;margin-top:0.3rem;margin-bottom:0.1rem;">UNDERSERVED AREAS</div>';
      html += d.underserved_areas.map(a => `<span style="display:inline-block;font-size:0.62rem;padding:0.1rem 0.3rem;background:#fef2f2;border:1px solid #fca5a5;border-radius:4px;margin:0.1rem 0.15rem 0.1rem 0;color:#dc2626;">${esc(a)}</span>`).join('');
    }
    if (d.recommendations && d.recommendations.length > 0) {
      html += '<div style="font-size:0.65rem;font-weight:600;color:#059669;margin-top:0.3rem;margin-bottom:0.1rem;">RECOMMENDATIONS</div>';
      html += d.recommendations.map(r => `<div style="font-size:0.65rem;color:#374151;padding:0.05rem 0;">&#x2192; ${esc(r)}</div>`).join('');
    }
    el.innerHTML = html;
  } catch (err) { el.innerHTML = `<div style="color:#dc2626;font-size:0.72rem;">Error: ${esc(err.message)}</div>`; }
}

// === Source ROI (Batch 24) ===
async function loadSourceROI() {
  try {
    const sources = await fetch('/api/source-roi').then(safeJSON);
    const el = document.getElementById('source-roi-list');
    if (!sources || sources.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#ca8a04;">No source data yet.</span>';
      return;
    }
    el.innerHTML = sources.map(s => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fef9c3;font-size:0.72rem;">
        <strong style="flex:1;color:#a16207;">${esc(s.primary_source)}</strong>
        <span style="color:#6b7280;">${fmt(s.total_leads)} leads</span>
        <span style="color:#059669;font-size:0.65rem;">${s.email_rate}% email</span>
        <span style="color:#2563eb;font-size:0.65rem;">${s.phone_rate}% phone</span>
        <span style="color:#d97706;font-size:0.65rem;">avg ${s.avg_score}</span>
        <span style="padding:0.1rem 0.3rem;background:${s.completeness > 50 ? '#dcfce7' : s.completeness > 20 ? '#fef9c3' : '#fef2f2'};border-radius:4px;font-size:0.6rem;font-weight:600;">${s.completeness}% complete</span>
      </div>
    `).join('');
  } catch (err) { console.error('Source ROI error:', err); }
}

async function loadAiSourceROI() {
  const el = document.getElementById('source-roi-ai');
  if (!el) return;
  el.style.display = 'block';
  el.innerHTML = '<div style="font-size:0.72rem;color:#ca8a04;">AI analyzing source performance...</div>';
  try {
    const res = await fetch('/api/ai/source-roi', { method: 'POST' });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    let html = '<div style="font-size:0.68rem;font-weight:600;color:#ca8a04;margin-bottom:0.3rem;">AI SOURCE ANALYSIS</div>';
    if (d.summary) html += `<div style="font-size:0.72rem;color:#374151;margin-bottom:0.3rem;line-height:1.4;">${esc(d.summary)}</div>`;
    const rankings = d.rankings || [];
    if (rankings.length > 0) {
      rankings.forEach(r => {
        const gradeColors = { A: '#22c55e', B: '#3b82f6', C: '#f59e0b', D: '#f97316', F: '#dc2626' };
        const color = gradeColors[r.grade] || '#6b7280';
        const recColors = { expand: '#22c55e', maintain: '#3b82f6', deprioritize: '#dc2626' };
        const recColor = recColors[r.recommendation] || '#6b7280';
        html += `<div style="display:flex;align-items:center;gap:0.3rem;padding:0.2rem 0;border-bottom:1px solid #fef9c3;">
          <span style="width:22px;height:22px;border-radius:50%;background:${color};color:#fff;font-size:0.65rem;display:flex;align-items:center;justify-content:center;font-weight:700;flex-shrink:0;">${esc(r.grade || '?')}</span>
          <div style="flex:1;min-width:0;">
            <div style="font-size:0.7rem;font-weight:600;color:#1f2937;">${esc(r.source || '')}</div>
            <div style="font-size:0.6rem;color:#6b7280;">${esc(r.strengths || '')}</div>
          </div>
          <span style="font-size:0.55rem;padding:0.1rem 0.3rem;border-radius:4px;background:${recColor}20;color:${recColor};font-weight:600;flex-shrink:0;">${esc(r.recommendation || '')}</span>
        </div>`;
      });
    }
    if (d.strategy && d.strategy.length > 0) {
      html += '<div style="font-size:0.62rem;font-weight:600;color:#ca8a04;margin-top:0.3rem;margin-bottom:0.1rem;">STRATEGY</div>';
      html += d.strategy.map(s => `<div style="font-size:0.62rem;color:#374151;padding:0.05rem 0;">&#x2192; ${esc(s)}</div>`).join('');
    }
    el.innerHTML = html;
  } catch (err) { el.innerHTML = `<div style="color:#dc2626;font-size:0.72rem;">Error: ${esc(err.message)}</div>`; }
}

// === Compliance Dashboard (Batch 24) ===
async function loadComplianceDashboard() {
  try {
    const data = await fetch('/api/compliance/dashboard').then(safeJSON);
    document.getElementById('compliance-score').innerHTML = `
      <div style="display:inline-flex;align-items:center;gap:0.4rem;">
        <span style="font-size:1.2rem;font-weight:700;color:${data.complianceScore > 80 ? '#16a34a' : data.complianceScore > 50 ? '#ca8a04' : '#dc2626'};">${data.complianceScore}</span>
        <span style="font-size:0.72rem;color:#6b7280;">/ 100 compliance score</span>
      </div>
    `;
    document.getElementById('compliance-stats').innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#475569;">${data.optOuts?.total || 0}</strong> opt-outs</span>
      <span style="font-size:0.72rem;"><strong style="color:#ef4444;">${data.optOuts?.blocked || 0}</strong> blocked leads</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${data.dnc?.total || 0}</strong> DNC entries</span>
      <span style="font-size:0.72rem;"><strong style="color:#ca8a04;">${data.retention?.older_1yr || 0}</strong> older than 1yr</span>
    `;
    const el = document.getElementById('compliance-optouts');
    if (data.optOuts?.recent && data.optOuts.recent.length > 0) {
      el.innerHTML = data.optOuts.recent.map(o => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #f1f5f9;font-size:0.68rem;">
          <strong>${esc(o.email)}</strong>
          <span style="flex:1;color:#6b7280;">${esc(o.reason || 'No reason')}</span>
          <span style="color:#9ca3af;font-size:0.6rem;">${o.opted_out_at ? new Date(o.opted_out_at).toLocaleDateString() : ''}</span>
          <button onclick="removeOptOutEmail('${esc(o.email)}')" style="font-size:0.6rem;padding:0.1rem 0.2rem;background:#f1f5f9;color:#475569;border:1px solid #94a3b8;border-radius:3px;cursor:pointer;">Remove</button>
        </div>
      `).join('');
    } else {
      el.innerHTML = '<span style="font-size:0.72rem;color:#059669;">No opt-outs recorded. All clear.</span>';
    }
  } catch (err) { console.error('Compliance error:', err); }
}

function showOptOutForm() {
  const email = prompt('Enter email to opt-out:');
  if (!email) return;
  const reason = prompt('Reason (optional):') || '';
  fetch('/api/compliance/opt-out', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email, reason }) })
    .then(safeJSON).then(() => { toast('Opt-out recorded', 'success'); loadComplianceDashboard(); })
    .catch(err => toast('Error: ' + err.message, 'error'));
}

function removeOptOutEmail(email) {
  fetch('/api/compliance/opt-out', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email }) })
    .then(safeJSON).then(() => { toast('Opt-out removed', 'success'); loadComplianceDashboard(); })
    .catch(err => toast('Error: ' + err.message, 'error'));
}

// === Firm Network (Batch 23) ===
async function loadFirmNetwork() {
  try {
    const firms = await fetch('/api/firm-network?limit=20').then(safeJSON);
    const el = document.getElementById('firm-network-list');
    if (!firms || firms.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#a21caf;">No multi-attorney firms found yet.</span>';
      return;
    }
    el.innerHTML = firms.map(f => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fae8ff;font-size:0.72rem;">
        <strong style="flex:1;color:#7c3aed;">${esc(f.firm_name)}</strong>
        <span style="color:#6b7280;">${f.headcount} attorneys</span>
        <span style="color:#0d9488;font-size:0.65rem;">${f.cities} cities</span>
        <span style="color:#2563eb;font-size:0.65rem;">${f.states} states</span>
        <span style="color:#059669;font-size:0.65rem;">${f.with_email} emails</span>
        <span style="padding:0.1rem 0.3rem;background:#fae8ff;border-radius:4px;font-size:0.6rem;color:#a21caf;">avg ${f.avg_score}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Firm network error:', err); }
}

// === Data Freshness Monitor (Batch 23) ===
async function loadFreshnessMonitor() {
  try {
    const data = await fetch('/api/freshness/report?staleDays=90&limit=50').then(safeJSON);
    const sumEl = document.getElementById('freshness-summary');
    const o = data.overall || {};
    sumEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#0d9488;">${o.total || 0}</strong> total leads</span>
      <span style="font-size:0.72rem;"><strong style="color:#059669;">${o.fresh_30d || 0}</strong> fresh (30d)</span>
      <span style="font-size:0.72rem;"><strong style="color:#ef4444;">${o.stale_90d || 0}</strong> stale (90d+)</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${o.avg_age_days || 0}</strong> avg age (days)</span>
    `;
    const resEl = document.getElementById('freshness-rescrape');
    if (data.rescrape && data.rescrape.length > 0) {
      resEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#ef4444;">Re-scrape needed:</span> ' +
        data.rescrape.map(r => `<span style="display:inline-block;padding:0.1rem 0.3rem;background:#fef2f2;border:1px solid #fca5a5;border-radius:4px;font-size:0.65rem;margin:0.1rem;">${esc(r.state)} (${r.stalePct}% stale, ${r.staleCount}/${r.totalCount})</span>`).join('');
    } else {
      resEl.innerHTML = '<span style="font-size:0.68rem;color:#059669;">All states have fresh data.</span>';
    }
    const stEl = document.getElementById('freshness-states');
    if (data.byState && data.byState.length > 0) {
      stEl.innerHTML = data.byState.slice(0, 15).map(s => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #ccfbf1;font-size:0.68rem;">
          <strong style="width:50px;">${esc(s.state)}</strong>
          <span style="flex:1;">${s.total} total</span>
          <span style="color:#059669;">${s.fresh_30d} fresh</span>
          <span style="color:#ef4444;">${s.stale_90d} stale</span>
          <span style="color:#6b7280;">${s.avg_age_days}d avg</span>
        </div>
      `).join('');
    }
  } catch (err) { console.error('Freshness error:', err); }
}

// === Scoring Model Rankings (Batch 23) ===
async function loadScoringRankings() {
  try {
    const models = await fetch('/api/scoring-models/rankings').then(safeJSON);
    const el = document.getElementById('scoring-rankings-list');
    if (!models || models.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#d97706;">No scoring models created yet. Create one in the Scoring Models panel.</span>';
      return;
    }
    el.innerHTML = models.map(m => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fef3c7;font-size:0.72rem;">
        <span style="padding:0.1rem 0.3rem;border-radius:4px;font-size:0.6rem;font-weight:600;background:${m.is_active ? '#dcfce7;color:#16a34a' : '#f3f4f6;color:#6b7280'};">${m.is_active ? 'active' : 'draft'}</span>
        <strong style="flex:1;color:#b45309;">${esc(m.name)}</strong>
        <span style="color:#6b7280;font-size:0.65rem;">${m.fieldCount} fields</span>
        <span style="color:#d97706;font-size:0.65rem;">max: ${m.maxPossible}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Scoring rankings error:', err); }
}

// === Geographic Clustering (Batch 23) ===
async function loadGeoClusters() {
  try {
    const data = await fetch('/api/geographic/clusters?minSize=3&limit=30').then(safeJSON);
    const sumEl = document.getElementById('geo-summary');
    const s = data.summary || {};
    sumEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#4f46e5;">${fmt(s.totalLeads || 0)}</strong> leads</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${s.totalCities || 0}</strong> cities</span>
      <span style="font-size:0.72rem;"><strong style="color:#0d9488;">${s.totalStates || 0}</strong> states</span>
      <span style="font-size:0.72rem;"><strong style="color:#d97706;">${s.avgPerCity || 0}</strong> avg/city</span>
    `;
    const clEl = document.getElementById('geo-clusters-list');
    if (data.cityClusters && data.cityClusters.length > 0) {
      clEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#4f46e5;margin-bottom:0.2rem;display:block;">Top Markets</span>' +
        data.cityClusters.slice(0, 15).map(c => `
          <div onclick="showMarketDetail('${esc(c.state)}')" style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #e0e7ff;font-size:0.68rem;cursor:pointer;" onmouseover="this.style.background='#eef2ff'" onmouseout="this.style.background=''">
            <strong style="flex:1;">${esc(c.city)}, ${esc(c.state)}</strong>
            <span style="color:#4f46e5;">${c.lead_count} leads</span>
            <span style="color:#059669;font-size:0.6rem;">${c.with_email} email</span>
            <span style="color:#6b7280;font-size:0.6rem;">${c.unique_firms} firms</span>
          </div>
        `).join('');
    }
    const upEl = document.getElementById('geo-underpen');
    if (data.underPenetrated && data.underPenetrated.length > 0) {
      upEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#ef4444;margin-bottom:0.2rem;display:block;">Under-Penetrated</span>' +
        data.underPenetrated.slice(0, 10).map(u => `
          <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #fef2f2;font-size:0.68rem;">
            <strong style="flex:1;">${esc(u.city)}, ${esc(u.state)}</strong>
            <span style="color:#ef4444;">${u.lead_count} leads (${u.pct_of_state}% of state)</span>
          </div>
        `).join('');
    }
  } catch (err) { console.error('Geo clusters error:', err); }
}

async function showMarketDetail(state) {
  try {
    const data = await fetch('/api/geographic/penetration/' + encodeURIComponent(state)).then(safeJSON);
    toast(state + ': ' + data.totalLeads + ' leads across ' + (data.cities?.length || 0) + ' cities', 'info');
  } catch (err) { toast('Market error: ' + err.message, 'error'); }
}

// === Lifecycle Analytics (Batch 22) ===
async function loadLifecycleAnalytics() {
  try {
    const data = await fetch('/api/lifecycle/analytics').then(safeJSON);
    const velEl = document.getElementById('lifecycle-velocity');
    velEl.innerHTML = `
      <span style="font-size:0.72rem;"><strong style="color:#16a34a;">${data.velocity?.avg_total_hours || 0}h</strong> avg velocity</span>
      <span style="font-size:0.72rem;"><strong style="color:#6b7280;">${data.velocity?.leads_completed || 0}</strong> leads completed</span>
    `;
    const bottEl = document.getElementById('lifecycle-bottlenecks');
    if (data.bottlenecks && data.bottlenecks.length > 0) {
      bottEl.innerHTML = '<span style="font-size:0.68rem;font-weight:600;color:#16a34a;">Bottlenecks:</span> ' +
        data.bottlenecks.map(b => `<span style="display:inline-block;padding:0.1rem 0.3rem;background:#dcfce7;border-radius:4px;font-size:0.65rem;margin:0.1rem;">${esc(b.stage)} <strong>${b.avg_hours}h</strong> avg (${b.transitions} moves)</span>`).join('');
    } else {
      bottEl.innerHTML = '<span style="font-size:0.68rem;color:#16a34a;">No stage bottlenecks detected yet.</span>';
    }
    const recEl = document.getElementById('lifecycle-recent');
    if (data.recent && data.recent.length > 0) {
      recEl.innerHTML = data.recent.map(t => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #dcfce7;font-size:0.72rem;">
          <span style="color:#16a34a;font-size:0.6rem;">${esc(t.from_stage || 'new')}</span>
          <span style="color:#9ca3af;">&#x2192;</span>
          <span style="font-weight:600;color:#16a34a;font-size:0.6rem;">${esc(t.to_stage)}</span>
          <strong style="flex:1;">${esc(t.first_name || '')} ${esc(t.last_name || '')}</strong>
          <span style="color:#6b7280;font-size:0.6rem;">${t.duration_hours > 0 ? t.duration_hours + 'h' : 'instant'}</span>
        </div>
      `).join('');
    } else {
      recEl.innerHTML = '<span style="font-size:0.72rem;color:#16a34a;">No stage transitions recorded yet.</span>';
    }
  } catch (err) { console.error('Lifecycle error:', err); }
}

// === Sequence Performance (Batch 22) ===
async function loadSequencePerformance() {
  try {
    const seqs = await fetch('/api/sequences/performance').then(safeJSON);
    const el = document.getElementById('sequence-performance-list');
    if (!seqs || seqs.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#9333ea;">No sequences with events yet.</span>';
      return;
    }
    el.innerHTML = seqs.map(s => `
      <div style="display:flex;align-items:center;gap:0.5rem;padding:0.25rem 0;border-bottom:1px solid #f3e8ff;font-size:0.72rem;">
        <strong style="flex:1;color:#7c3aed;">${esc(s.name || 'Seq #' + s.id)}</strong>
        <span style="color:#6b7280;">${s.leads || 0} leads</span>
        <span style="color:#059669;">${s.sent || 0} sent</span>
        <span style="color:#2563eb;">${s.reply_rate || 0}% reply</span>
        <span style="color:#ef4444;font-size:0.65rem;">${s.bounced || 0} bounced</span>
        <button onclick="showSequenceAnalytics(${s.id})" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#f3e8ff;color:#9333ea;border:1px solid #c084fc;border-radius:3px;cursor:pointer;">Detail</button>
      </div>
    `).join('');
  } catch (err) { console.error('Sequence perf error:', err); }
}

async function showSequenceAnalytics(seqId) {
  try {
    const data = await fetch('/api/sequences/' + seqId + '/analytics').then(safeJSON);
    const el = document.getElementById('sequence-performance-list');
    let html = `<h4 style="margin:0 0 0.3rem;color:#7c3aed;">Sequence #${seqId} Analytics</h4>`;
    if (data.totals) {
      html += `<div style="display:flex;gap:0.8rem;margin-bottom:0.3rem;font-size:0.72rem;">
        <span><strong>${data.totals.total_sent || 0}</strong> sent</span>
        <span><strong>${data.totals.total_opened || 0}</strong> opened</span>
        <span><strong>${data.totals.total_replied || 0}</strong> replied</span>
        <span><strong>${data.totals.total_bounced || 0}</strong> bounced</span>
      </div>`;
    }
    if (data.steps && data.steps.length > 0) {
      html += data.steps.map(s => `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.15rem 0;border-bottom:1px solid #f3e8ff;font-size:0.68rem;">
          <span style="font-weight:600;width:60px;">Step ${s.step_number}</span>
          <span style="color:#059669;">${s.open_rate}% open</span>
          <span style="color:#2563eb;">${s.reply_rate}% reply</span>
          <span style="color:#ef4444;">${s.bounce_rate}% bounce</span>
          <span style="color:#6b7280;">${s.sent} sent</span>
        </div>
      `).join('');
    }
    html += '<br><button onclick="loadSequencePerformance()" style="font-size:0.68rem;padding:0.15rem 0.4rem;background:#f3e8ff;color:#9333ea;border:1px solid #c084fc;border-radius:4px;cursor:pointer;">Back</button>';
    el.innerHTML = html;
  } catch (err) { toast('Sequence detail error: ' + err.message, 'error'); }
}

// === Activity Scoring (Batch 22) ===
async function loadActivityScores() {
  try {
    const scores = await fetch('/api/activity-scores/batch?limit=20').then(safeJSON);
    const el = document.getElementById('activity-scores-list');
    if (!scores || scores.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#ea580c;">No activity data yet. Log contacts and interactions to see scores.</span>';
      return;
    }
    el.innerHTML = scores.map(s => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ffedd5;font-size:0.72rem;">
        <span style="padding:0.1rem 0.3rem;border-radius:4px;font-size:0.65rem;font-weight:600;background:${s.score > 50 ? '#dcfce7;color:#16a34a' : s.score > 20 ? '#ffedd5;color:#ea580c' : '#fef2f2;color:#ef4444'};">${s.score}</span>
        <strong style="flex:1;">${esc(s.first_name || '')} ${esc(s.last_name || '')}</strong>
        <span style="color:#6b7280;font-size:0.65rem;">${s.totalActivities} activities</span>
        <span style="color:#059669;font-size:0.65rem;">${s.recentActivities} recent</span>
        <span style="color:#9ca3af;font-size:0.6rem;">${Object.entries(s.breakdown || {}).map(([k,v]) => k + ':' + v).join(', ')}</span>
      </div>
    `).join('');
  } catch (err) { console.error('Activity scores error:', err); }
}

// === Bulk Enrichment (Batch 22) ===
async function loadBulkEnrichmentRuns() {
  try {
    const runs = await fetch('/api/bulk-enrichment/runs?limit=10').then(safeJSON);
    const el = document.getElementById('bulk-enrichment-runs');
    if (!runs || runs.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#0891b2;">No enrichment runs yet. Select leads and click "Enrich Selected" to start.</span>';
      return;
    }
    el.innerHTML = runs.map(r => {
      const fields = JSON.parse(r.fields_filled || '{}');
      const fieldStr = Object.entries(fields).map(([k,v]) => k + ':' + v).join(', ');
      return `
        <div style="display:flex;align-items:center;gap:0.4rem;padding:0.25rem 0;border-bottom:1px solid #cffafe;font-size:0.72rem;">
          <span style="padding:0.1rem 0.3rem;border-radius:4px;font-size:0.6rem;font-weight:600;background:${r.status === 'completed' ? '#dcfce7;color:#16a34a' : r.status === 'processing' ? '#cffafe;color:#0891b2' : '#fef3c7;color:#f59e0b'};">${r.status}</span>
          <strong style="flex:1;">${r.total_leads} leads</strong>
          <span style="color:#059669;font-size:0.65rem;">${r.enriched}/${r.processed} enriched</span>
          <span style="color:#6b7280;font-size:0.6rem;">${fieldStr || 'none yet'}</span>
          ${r.status === 'processing' ? `<button onclick="processEnrichRun(${r.id})" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#0891b2;color:#fff;border:none;border-radius:3px;cursor:pointer;">Process</button>` : ''}
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Bulk enrichment error:', err); }
}

async function startBulkEnrichment() {
  const ids = [..._selectedIds];
  if (ids.length === 0) { toast('Select leads first using checkboxes', 'warn'); return; }
  try {
    const res = await fetch('/api/bulk-enrichment/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ leadIds: ids }) }).then(safeJSON);
    toast('Created enrichment run #' + res.runId + ' (' + res.queued + ' queued)', 'success');
    // Auto-process first batch
    await fetch('/api/bulk-enrichment/process/' + res.runId, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ batchSize: 20 }) });
    loadBulkEnrichmentRuns();
    refreshAll();
  } catch (err) { toast('Enrichment error: ' + err.message, 'error'); }
}

async function processEnrichRun(runId) {
  try {
    const res = await fetch('/api/bulk-enrichment/process/' + runId, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ batchSize: 20 }) }).then(safeJSON);
    toast('Processed ' + res.processed + ' leads (' + res.enriched + ' enriched)', 'success');
    loadBulkEnrichmentRuns();
  } catch (err) { toast('Process error: ' + err.message, 'error'); }
}

// === Dedup Intelligence (Batch 40) ===
async function loadDedupIntelligence() {
  try {
    const r = await fetch('/api/dedup-intelligence');
    const d = await r.json();
    const s = d.stats||{};
    document.getElementById('dedup-intel-stats').innerHTML = '<strong>Duplicates:</strong> ' +
      `${s.exactDupeGroups||0} name groups (${s.totalDupeLeads||0} leads) · ${s.phoneDupeGroups||0} phone dupes · ~${s.estimatedSavings||0} leads to merge`;
    const items = (d.exactDupes||[]).slice(0,10).map(dup =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${dup.first_name} ${dup.last_name} (${dup.city}): ${dup.count}x [IDs: ${dup.ids}]</div>`
    );
    document.getElementById('dedup-intel-list').innerHTML = items.join('') || '<em>No duplicates found</em>';
  } catch(e) { console.warn('dedup intelligence:', e); }
}

// === Outbound Readiness (Batch 40) ===
async function loadOutboundReadiness() {
  try {
    const r = await fetch('/api/outbound-readiness');
    const d = await r.json();
    const rd = d.readiness||{};
    document.getElementById('outbound-score').innerHTML = '<strong>Readiness Score:</strong> ' +
      `${d.score||0}% · ${rd.total||0} total leads`;
    document.getElementById('outbound-channels').innerHTML = '<strong>Channels:</strong> ' +
      `Email ready: ${rd.email_ready||0} · Phone ready: ${rd.phone_ready||0} · Multi-channel: ${rd.multi_channel||0} · No contact: ${rd.no_contact_info||0}`;
    document.getElementById('outbound-actions').innerHTML = '<strong>Actions:</strong> ' +
      (d.actions||[]).map(a => `${a.action}: ~${a.potential} leads (${a.channel})`).join(' · ');
  } catch(e) { console.warn('outbound readiness:', e); }
}

// === Aging Report (Batch 40) ===
async function loadAgingReport() {
  try {
    const r = await fetch('/api/aging-report');
    const d = await r.json();
    document.getElementById('aging-categories').innerHTML = '<strong>Data Age:</strong> ' +
      (d.categories||[]).map(c => `${c.age_category}: ${c.count} (avg ${c.avg_score})`).join(' · ');
    document.getElementById('aging-suggestions').innerHTML = '<strong>Re-engagement:</strong> ' +
      (d.suggestions||[]).map(s => `[${s.priority}] ${s.action}: ${s.count} leads`).join(' · ');
  } catch(e) { console.warn('aging report:', e); }
}

// === Growth Analytics (Batch 40) ===
async function loadGrowthAnalytics2() {
  try {
    const r = await fetch('/api/growth-analytics');
    const d = await r.json();
    const p = d.projections||{};
    document.getElementById('growth-projections').innerHTML = '<strong>Projections:</strong> ' +
      `${p.current||0} leads · ${p.dailyRate||0}/day` +
      (p.to20k ? ` · 20K in ${p.to20k}d` : '') +
      (p.to50k ? ` · 50K in ${p.to50k}d` : '') +
      (p.to100k ? ` · 100K in ${p.to100k}d` : '');
    document.getElementById('growth-weekly').innerHTML = '<strong>Weekly:</strong> ' +
      (d.weeklyGrowth||[]).slice(0,6).map(w => `${w.week}: +${w.new_leads} (${w.with_email} w/email)`).join(' · ');
    document.getElementById('growth-quality').innerHTML = '<strong>Quality Trend:</strong> ' +
      (d.qualityTrend||[]).slice(0,6).map(q => `${q.week}: ${q.email_pct}% email, avg ${q.avg_score}`).join(' · ');
  } catch(e) { console.warn('growth analytics:', e); }
}

// === Revenue Attribution (Batch 39) ===
async function loadRevenueAttribution() {
  try {
    const r = await fetch('/api/revenue-attribution');
    const d = await r.json();
    document.getElementById('revenue-segments').innerHTML = '<strong>High-Value Segments:</strong> ' +
      (d.segments||[]).slice(0,6).map(s => `${s.practice_area}: $${(s.total_value||0).toLocaleString()} (${s.contactable} contacts)`).join(' · ');
    document.getElementById('revenue-sources').innerHTML = (d.sourceROI||[]).map(s =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${s.primary_source}: ${s.leads} leads, ${s.contactable} contactable, ROI score ${s.roi_score}</div>`
    ).join('');
  } catch(e) { console.warn('revenue attribution:', e); }
}

// === Saturation Heatmap (Batch 39) ===
async function loadSaturationHeatmap2() {
  try {
    const r = await fetch('/api/saturation-heatmap');
    const d = await r.json();
    document.getElementById('saturation-cities').innerHTML = (d.heatmap||[]).map(c =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${c.city}, ${c.state}: ${c.our_leads}/${c.estimated_market} (${c.penetration}%) [${c.saturation}]</div>`
    ).join('');
  } catch(e) { console.warn('saturation heatmap:', e); }
}

// === Smart List Builder (Batch 39) ===
async function loadSmartListBuilder() {
  try {
    const r = await fetch('/api/smart-list-builder');
    const d = await r.json();
    document.getElementById('smart-list-templates').innerHTML = '<strong>Lists:</strong> ' +
      (d.templates||[]).map(t => `${t.name}: ${t.count} leads`).join(' · ');
    const fc = d.filterCounts||{};
    document.getElementById('smart-list-filters').innerHTML = '<strong>Filters:</strong> ' +
      `Email: ${fc.withEmail||0} · Phone: ${fc.withPhone||0} · Website: ${fc.withWebsite||0} · LinkedIn: ${fc.withLinkedin||0} · Total: ${fc.total||0}`;
  } catch(e) { console.warn('smart list builder:', e); }
}

// === Quality Scorecard (Batch 39) ===
async function loadQualityScorecard() {
  try {
    const r = await fetch('/api/quality-scorecard');
    const d = await r.json();
    const o = d.overall||{};
    document.getElementById('quality-overall').innerHTML = '<strong>Database Grade:</strong> ' +
      `${o.grade||'?'} (${o.quality_score||0}/100) · ${o.total||0} leads · ${o.email_pct||0}% email · ${o.phone_pct||0}% phone`;
    document.getElementById('quality-sources').innerHTML = (d.sources||[]).map(s =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">[${s.grade}] ${s.primary_source}: ${s.quality_score}/100 · ${s.email_pct}% email · ${s.phone_pct}% phone · ${s.total} leads</div>`
    ).join('');
  } catch(e) { console.warn('quality scorecard:', e); }
}

// === Affinity Scoring (Batch 38) ===
async function loadAffinityScoring() {
  try {
    const r = await fetch('/api/affinity-scoring');
    const d = await r.json();
    document.getElementById('affinity-combos').innerHTML = '<strong>Top Combos:</strong> ' +
      (d.topCombos||[]).slice(0,8).map(c => `${c.practice_area}@${c.city}: ${c.count}`).join(' · ');
    document.getElementById('affinity-leads').innerHTML = (d.leads||[]).slice(0,15).map(l =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${l.first_name} ${l.last_name} (${l.practice_area||'?'}@${l.city||'?'}) — affinity <strong>${l.affinity_score}</strong> cluster:${l.cluster_size}</div>`
    ).join('');
  } catch(e) { console.warn('affinity scoring:', e); }
}

// === Scraper Gaps (Batch 38) ===
async function loadScraperGaps() {
  try {
    const r = await fetch('/api/scraper-gaps');
    const d = await r.json();
    document.getElementById('scraper-gaps-recs').innerHTML = '<strong>Issues:</strong> ' +
      (d.recommendations||[]).map(r => `[${r.type}] ${r.message}`).join(' · ') || 'All good';
    document.getElementById('scraper-gaps-quality').innerHTML = (d.sourceCoverage||[]).map(s =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${s.primary_source}: ${s.total} leads, quality ${s.quality_score}, ${s.emails} emails, ${s.cities} cities</div>`
    ).join('');
  } catch(e) { console.warn('scraper gaps:', e); }
}

// === Freshness Index (Batch 38) ===
async function loadFreshnessIndex() {
  try {
    const r = await fetch('/api/freshness-index');
    const d = await r.json();
    document.getElementById('freshness-idx-sources').innerHTML = '<strong>By Source:</strong> ' +
      (d.bySource||[]).slice(0,8).map(s => `${s.primary_source}: ${s.avg_freshness}% fresh (avg ${s.avg_age}d)`).join(' · ');
    document.getElementById('freshness-idx-rescrape').innerHTML = '<strong>Re-scrape Batches:</strong><br>' +
      (d.rescrapeBatches||[]).map(b => `${b.state}: ${b.stale_count} stale (avg ${b.avg_age}d)`).join('<br>');
  } catch(e) { console.warn('freshness index:', e); }
}

// === Firm Growth (Batch 38) ===
async function loadFirmGrowthPanel() {
  try {
    const r = await fetch('/api/firm-growth');
    const d = await r.json();
    document.getElementById('firm-growth-dist').innerHTML = '<strong>Size Distribution:</strong> ' +
      (d.sizeDistribution||[]).map(s => `${s.size_bucket}: ${s.firm_count} firms`).join(' · ');
    document.getElementById('firm-growth-list').innerHTML = (d.firms||[]).slice(0,15).map(f =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${f.firm_name}: ${f.headcount} people, ${f.offices} offices, avg ${f.avg_score}</div>`
    ).join('');
  } catch(e) { console.warn('firm growth:', e); }
}

// === Cadence Optimizer (Batch 37) ===
async function loadCadenceOptimizer() {
  try {
    const r = await fetch('/api/cadence-optimizer');
    const d = await r.json();
    const nc = d.needsContact||{};
    document.getElementById('cadence-needs').innerHTML = '<strong>Contact Status:</strong> ' +
      `Never contacted: ${nc.never_contacted||0} · Needs follow-up: ${nc.needs_followup||0} · Over-contacted: ${nc.over_contacted||0}`;
    document.getElementById('cadence-day-perf').innerHTML = '<strong>Day Performance:</strong> ' +
      (d.dayPerformance||[]).map(dp => `${dp.day_name}: ${dp.contacts} contacts, ${dp.responses} responses`).join(' · ');
    document.getElementById('cadence-recs').innerHTML = '<strong>Recommendations:</strong> ' +
      (d.recommendations||[]).map(r => `[${r.priority}] ${r.rule}`).join(' · ');
  } catch(e) { console.warn('cadence optimizer:', e); }
}

// === Scoring Calibration (Batch 37) ===
async function loadScoringCalibration() {
  try {
    const r = await fetch('/api/scoring-calibration');
    const d = await r.json();
    const s = d.stats||{};
    document.getElementById('scoring-cal-stats').innerHTML = '<strong>Score Stats:</strong> ' +
      `Mean: ${s.mean||0} · Median: ${s.median||0} · Min: ${s.min_score||0} · Max: ${s.max_score||0} · Calibration: ${d.calibration||'?'} (${d.lowScorePercent||0}% below 30)`;
    document.getElementById('scoring-cal-dist').innerHTML = '<strong>Distribution:</strong> ' +
      (d.distribution||[]).map(b => `${b.bucket}: ${b.count}`).join(' · ');
    document.getElementById('scoring-cal-outcome').innerHTML = '<strong>Score→Outcome:</strong> ' +
      (d.scoreOutcome||[]).map(o => `${o.score_group}: ${o.total} leads, ${o.with_email} email, ${o.progressed} progressed`).join(' · ');
  } catch(e) { console.warn('scoring calibration:', e); }
}

// === Practice Market Size (Batch 37) ===
async function loadPracticeMarketSize() {
  try {
    const r = await fetch('/api/practice-market-size');
    const d = await r.json();
    const o = d.overall||{};
    document.getElementById('practice-market-overview').innerHTML = '<strong>Market:</strong> ' +
      `${o.unique_practices||0} practice areas · ${o.unique_cities||0} cities · ${o.total||0} leads with practice data`;
    document.getElementById('practice-market-list').innerHTML = (d.practiceAreas||[]).map(p =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${p.practice_area}: ${p.total} leads in ${p.cities} cities, ${p.with_email} emails, avg ${p.avg_score}</div>`
    ).join('');
  } catch(e) { console.warn('practice market size:', e); }
}

// === Pipeline Health (Batch 37) ===
async function loadPipelineHealthPanel() {
  try {
    const r = await fetch('/api/pipeline-health');
    const d = await r.json();
    const q = d.quality||{};
    document.getElementById('pipeline-health-quality').innerHTML = '<strong>Data Quality:</strong> ' +
      `Email: ${q.email_rate||0}% · Phone: ${q.phone_rate||0}% · Website: ${q.website_rate||0}% · Firm: ${q.firm_rate||0}%`;
    document.getElementById('pipeline-health-degrading').innerHTML = '<strong>Degrading Sources:</strong> ' +
      ((d.degrading||[]).length ? d.degrading.map(s => `${s.source}: ${s.fields_per_lead} fields/lead`).join(' · ') : 'None detected');
    document.getElementById('pipeline-health-sources').innerHTML = (d.sourceHealth||[]).map(s =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${s.primary_source}: ${s.total} leads, ${s.avg_fields_per_lead} fields/lead, avg ${s.avg_score}</div>`
    ).join('');
  } catch(e) { console.warn('pipeline health:', e); }
}

// === Prioritization Matrix (Batch 36) ===
async function loadPrioritizationMatrix() {
  try {
    const r = await fetch('/api/prioritization-matrix');
    const d = await r.json();
    const q = d.quadrants||{};
    document.getElementById('priority-quadrants').innerHTML = '<strong>Quadrants:</strong> ' +
      `Stars: ${q.star||0} · Nurture: ${q.nurture||0} · Enrich: ${q.enrich||0} · Deprioritize: ${q.deprioritize||0}`;
    document.getElementById('priority-leads').innerHTML = (d.leads||[]).slice(0,15).map(l =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${l.first_name} ${l.last_name} (${l.city||''}) — <strong>${l.priority_score}</strong> pts [${l.quadrant}] R${l.recency} C${l.completeness} M${l.marketValue} E${l.engagement}</div>`
    ).join('');
  } catch(e) { console.warn('prioritization matrix:', e); }
}

// === Firm Aggregation (Batch 36) ===
async function loadFirmAggregation() {
  try {
    const r = await fetch('/api/firm-aggregation');
    const d = await r.json();
    const t = d.totals||{};
    document.getElementById('firm-agg-totals').innerHTML = '<strong>Firms:</strong> ' +
      `${t.unique_firms||0} unique · ${t.total_leads||0} total leads · ${t.no_firm||0} without firm`;
    document.getElementById('firm-agg-list').innerHTML = (d.firms||[]).slice(0,15).map(f =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${f.firm_name}: ${f.headcount} people, ${f.offices} offices, avg ${f.avg_score} · ${f.with_email}✉ ${f.with_phone}📞</div>`
    ).join('');
  } catch(e) { console.warn('firm aggregation:', e); }
}

// === Improvement Recommendations (Batch 36) ===
async function loadImprovementRecs() {
  try {
    const r = await fetch('/api/improvement-recs');
    const d = await r.json();
    const g = d.gaps||{};
    document.getElementById('improve-gaps').innerHTML = '<strong>Missing Fields:</strong> ' +
      `Email: ${g.missing_email||0} · Phone: ${g.missing_phone||0} · Website: ${g.missing_website||0} · Firm: ${g.missing_firm||0} · LinkedIn: ${g.missing_linkedin||0} (of ${g.total||0})`;
    document.getElementById('improve-actions').innerHTML = '<strong>Actions:</strong> ' +
      (d.actions||[]).map(a => `${a.action}: could fill ~${a.potential} ${a.field}s`).join(' · ');
    document.getElementById('improve-quickwins').innerHTML = '<strong>Quick Wins (need 1 field):</strong><br>' +
      (d.quickWins||[]).map(q => `${q.first_name} ${q.last_name} (${q.city}) score ${q.lead_score} — needs ${q.missing_field}`).join('<br>');
  } catch(e) { console.warn('improvement recs:', e); }
}

// === Lifecycle Funnel (Batch 36) ===
async function loadLifecycleFunnel() {
  try {
    const r = await fetch('/api/lifecycle-funnel');
    const d = await r.json();
    const f = d.funnel||{};
    document.getElementById('funnel-stages').innerHTML = '<strong>Funnel:</strong> ' +
      `Raw: ${f.total||0} → Enriched: ${f.enriched||0} → Contacted: ${f.contacted||0} → Responded: ${f.responded||0} → Qualified: ${f.qualified||0} → Converted: ${f.converted||0}`;
    const rt = d.rates||{};
    document.getElementById('funnel-rates').innerHTML = '<strong>Conversion Rates:</strong> ' +
      `Raw→Enriched: ${rt.raw_to_enriched}% · Enriched→Contacted: ${rt.enriched_to_contacted}% · Contacted→Responded: ${rt.contacted_to_responded}%`;
    document.getElementById('funnel-sources').innerHTML = '<strong>By Source:</strong><br>' +
      (d.bySource||[]).slice(0,10).map(s => `${s.primary_source}: ${s.total} total, ${s.enriched} enriched, ${s.progressed} progressed`).join('<br>');
  } catch(e) { console.warn('lifecycle funnel:', e); }
}

// === Relationship Graph (Batch 35) ===
async function loadRelationshipGraph() {
  try {
    const r = await fetch('/api/relationship-graph');
    const d = await r.json();
    document.getElementById('rel-firm-bridges').innerHTML = '<strong>Firm Bridges:</strong> ' +
      (d.firmBridges||[]).slice(0,8).map(b => `${b.firm_name}: ${b.city1}↔${b.city2}`).join(' · ');
    document.getElementById('rel-practice-groups').innerHTML = '<strong>Practice Clusters:</strong> ' +
      (d.practiceGroups||[]).slice(0,8).map(p => `${p.practice_area}: ${p.count} (${p.cities} cities)`).join(' · ');
    document.getElementById('rel-city-pairs').innerHTML = '<strong>Connected Cities:</strong><br>' +
      (d.cityPairs||[]).map(c => `${c.city1} ↔ ${c.city2}: ${c.shared_firms} shared firms`).join('<br>');
  } catch(e) { console.warn('relationship graph:', e); }
}

// === Enrichment ROI (Batch 35) ===
async function loadEnrichmentROI() {
  try {
    const r = await fetch('/api/enrichment-roi');
    const d = await r.json();
    const o = d.overall||{};
    document.getElementById('eroi-overall').innerHTML = '<strong>Overall:</strong> ' +
      `${o.total||0} leads · ${o.emails||0} emails · ${o.phones||0} phones · ${o.websites||0} websites · avg score ${o.avg_score||0}`;
    document.getElementById('eroi-efficiency').innerHTML = (d.efficiency||[]).map(e =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${e.source}: ${e.fieldsPerLead} fields/lead · ${e.emailRate}% email · ${e.phoneRate}% phone (n=${e.total})</div>`
    ).join('');
  } catch(e) { console.warn('enrichment roi:', e); }
}

// === Engagement Prediction (Batch 35) ===
async function loadEngagementPrediction() {
  try {
    const r = await fetch('/api/engagement-prediction');
    const d = await r.json();
    const dist = d.distribution||{};
    document.getElementById('engagement-dist').innerHTML = '<strong>Prediction:</strong> ' +
      `High: ${dist.high||0} · Medium: ${dist.medium||0} · Low: ${dist.low||0}`;
    document.getElementById('engagement-top').innerHTML = (d.leads||[]).slice(0,15).map(l =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${l.first_name} ${l.last_name} (${l.city||''}) — <strong>${l.engagement_prob}%</strong> T${l.city_tier} ${l.email?'✉':''}${l.phone?'📞':''}</div>`
    ).join('');
  } catch(e) { console.warn('engagement prediction:', e); }
}

// === Campaign Performance (Batch 35) ===
async function loadCampaignPerformance2() {
  try {
    const r = await fetch('/api/campaign-performance');
    const d = await r.json();
    document.getElementById('camp-perf-practice').innerHTML = '<strong>Top Practice Areas:</strong> ' +
      (d.byPractice||[]).slice(0,8).map(p => `${p.practice_area}: ${p.total} (avg ${p.avg_score})`).join(' · ');
    document.getElementById('camp-perf-cities').innerHTML = '<strong>Top Cities:</strong> ' +
      (d.byCityPerf||[]).slice(0,8).map(c => `${c.city}: ${c.total} (avg ${c.avg_score})`).join(' · ');
    document.getElementById('camp-perf-sources').innerHTML = '<strong>Source Performance:</strong><br>' +
      (d.bySourcePerf||[]).map(s => `${s.primary_source}: ${s.total} leads, ${s.emails} emails, avg ${s.avg_score}`).join('<br>');
  } catch(e) { console.warn('campaign performance:', e); }
}

// === Merge Candidates (Batch 34) ===
async function loadMergeCandidates() {
  try {
    const r = await fetch('/api/merge-candidates');
    const d = await r.json();
    document.getElementById('merge-name-dupes').innerHTML = '<strong>Name+City Dupes:</strong> ' + (d.nameDupes||[]).length + ' pairs found';
    document.getElementById('merge-email-dupes').innerHTML = '<strong>Email Dupes:</strong> ' + (d.emailDupes||[]).length + ' pairs found';
    const items = (d.nameDupes||[]).slice(0,10).map(m =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${m.first_name} ${m.last_name} (${m.city}) — IDs #${m.id1} vs #${m.id2} | scores ${m.score1}/${m.score2}</div>`
    );
    document.getElementById('merge-list').innerHTML = items.join('') || '<em>No duplicates</em>';
  } catch(e) { console.warn('merge candidates:', e); }
}

// === Outreach Analytics (Batch 34) ===
async function loadOutreachAnalytics() {
  try {
    const r = await fetch('/api/outreach-analytics');
    const d = await r.json();
    const cs = d.contactStats||{};
    document.getElementById('outreach-contact-stats').innerHTML = '<strong>Total Contacts:</strong> ' + (cs.total||0) +
      (cs.byMethod||[]).length ? (' · ' + cs.byMethod.map(m => `${m.contact_method}: ${m.count} (${m.response_rate}% response)`).join(' · ')) : '';
    document.getElementById('outreach-by-day').innerHTML = '<strong>By Day:</strong> ' +
      (cs.byDayOfWeek||[]).map(d => `${d.day_name}: ${d.count}`).join(' · ');
    document.getElementById('outreach-attempts').innerHTML = '<strong>Attempt Distribution:</strong> ' +
      (d.attemptDistribution||[]).map(a => `${a.bucket}: ${a.count}`).join(' · ');
  } catch(e) { console.warn('outreach analytics:', e); }
}

// === ICP Scoring (Batch 34) ===
async function loadIcpScoring() {
  try {
    const r = await fetch('/api/icp-scoring');
    const d = await r.json();
    const dist = d.distribution||{};
    document.getElementById('icp-scoring-distribution').innerHTML = '<strong>ICP Fit:</strong> ' +
      `Excellent: ${dist.excellent||0} · Good: ${dist.good||0} · Fair: ${dist.fair||0} · Poor: ${dist.poor||0} (${d.total||0} total)`;
    document.getElementById('icp-top-leads').innerHTML = (d.leads||[]).slice(0,15).map(l =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${l.first_name} ${l.last_name} (${l.city||''}) — ICP <strong>${l.icp_score}</strong> | T${l.city_tier} ${l.email?'✉':''}${l.phone?'📞':''}${l.website?'🌐':''}</div>`
    ).join('');
  } catch(e) { console.warn('icp scoring:', e); }
}

// === Pipeline Velocity (Batch 34) ===
async function loadPipelineVelocity() {
  try {
    const r = await fetch('/api/pipeline-velocity');
    const d = await r.json();
    document.getElementById('velocity-stages').innerHTML = '<strong>Stages:</strong> ' +
      (d.stages||[]).map(s => `${s.pipeline_stage||'unknown'}: ${s.count} (avg ${s.avg_score})`).join(' · ');
    document.getElementById('velocity-bottlenecks').innerHTML = '<strong>Bottlenecks:</strong> ' +
      (d.bottlenecks||[]).map(b => `${b.pipeline_stage}: ${b.stuck_count} stuck avg ${b.avg_days_stuck}d`).join(' · ');
    document.getElementById('velocity-weekly').innerHTML = '<strong>Weekly:</strong> ' +
      (d.weeklyVelocity||[]).slice(0,6).map(w => `${w.week}: ${w.new_leads} new (${w.with_email} w/email)`).join(' · ');
  } catch(e) { console.warn('pipeline velocity:', e); }
}

// === Journey Mapping (Batch 33) ===
async function loadJourneyMapping() {
  try {
    const r = await fetch('/api/journey-mapping');
    const d = await r.json();
    document.getElementById('journey-stages').innerHTML = '<strong>Stages:</strong> ' +
      (d.stages||[]).map(s => `${s.pipeline_stage||'unknown'}: ${s.count} (avg ${s.avg_score})`).join(' · ');
    document.getElementById('journey-touchpoints').innerHTML = '<strong>Enrichment:</strong> ' +
      (d.touchpoints||[]).map(t => `${t.enrichment_stage}: ${t.count}`).join(' · ');
    const src = {};
    (d.bySource||[]).forEach(s => { if(!src[s.primary_source]) src[s.primary_source]=[]; src[s.primary_source].push(`${s.pipeline_stage}:${s.count}`); });
    document.getElementById('journey-by-source').innerHTML = '<strong>By Source:</strong><br>' +
      Object.entries(src).map(([k,v]) => `${k}: ${v.join(', ')}`).join('<br>');
  } catch(e) { console.warn('journey mapping:', e); }
}

// === Scoring Audit (Batch 33) ===
async function loadScoringAudit() {
  try {
    const r = await fetch('/api/scoring-audit');
    const d = await r.json();
    document.getElementById('scoring-histogram').innerHTML = '<strong>Distribution:</strong> ' +
      (d.histogram||[]).map(h => `${h.bucket}: ${h.count}`).join(' · ');
    const f = d.factors||{};
    document.getElementById('scoring-factors').innerHTML = '<strong>Coverage:</strong> ' +
      `Email ${f.have_email||0}/${f.total||0} · Phone ${f.have_phone||0}/${f.total||0} · Website ${f.have_website||0}/${f.total||0} · LinkedIn ${f.have_linkedin||0}/${f.total||0}`;
    document.getElementById('scoring-leads').innerHTML = (d.leads||[]).slice(0,15).map(l =>
      `<div style="padding:2px 0;border-bottom:1px solid #e5e7eb;">${l.first_name} ${l.last_name} — <strong>${l.lead_score}</strong> pts (email:${l.email_pts} phone:${l.phone_pts} web:${l.website_pts} firm:${l.firm_pts})</div>`
    ).join('');
  } catch(e) { console.warn('scoring audit:', e); }
}

// === Geographic Expansion (Batch 33) ===
async function loadGeoExpansion() {
  try {
    const r = await fetch('/api/geo-expansion');
    const d = await r.json();
    document.getElementById('geo-state-coverage').innerHTML = '<strong>Coverage:</strong> ' +
      (d.stateCoverage||[]).slice(0,10).map(s => `${s.state}: ${s.total} (${s.cities_covered} cities)`).join(' · ');
    document.getElementById('geo-underserved').innerHTML = '<strong>Underserved:</strong> ' +
      (d.underserved||[]).map(u => `${u.state}: only ${u.total} leads`).join(' · ');
    document.getElementById('geo-success').innerHTML = '<strong>Best Email Rates:</strong><br>' +
      (d.successPatterns||[]).slice(0,10).map(s => `${s.state}: ${s.email_rate}% email, ${s.phone_rate}% phone (n=${s.total})`).join('<br>');
  } catch(e) { console.warn('geo expansion:', e); }
}

// === Freshness Alerts (Batch 33) ===
async function loadFreshnessAlerts2() {
  try {
    const r = await fetch('/api/freshness-alerts');
    const d = await r.json();
    document.getElementById('freshness-staleness').innerHTML = '<strong>Data Age:</strong> ' +
      (d.staleness||[]).map(s => `${s.freshness}: ${s.count}`).join(' · ');
    document.getElementById('freshness-source-age').innerHTML = '<strong>Source Age:</strong> ' +
      (d.sourceAge||[]).slice(0,8).map(s => `${s.primary_source}: ${s.days_ago}d ago (${s.total})`).join(' · ');
    document.getElementById('freshness-alerts-rescrape').innerHTML = '<strong>Re-scrape Priority:</strong><br>' +
      (d.rescrapePriority||[]).slice(0,10).map(r => `${r.state}: avg ${r.avg_age_days}d old (${r.total} leads)`).join('<br>');
  } catch(e) { console.warn('freshness alerts:', e); }
}

// === Deal Estimation (Batch 32) ===
async function loadDealEstimates() {
  try {
    const data = await fetch('/api/deals?limit=20').then(safeJSON);
    const sEl = document.getElementById('deal-summary');
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#059669;">$${(data.totalPipeline / 1000).toFixed(0)}k</div><div style="font-size:0.65rem;">Total Pipeline</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#f59e0b;">$${(data.weightedPipeline / 1000).toFixed(0)}k</div><div style="font-size:0.65rem;">Weighted</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#2563eb;">$${data.avgDeal}</div><div style="font-size:0.65rem;">Avg Deal</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#6b7280;">${data.count}</div><div style="font-size:0.65rem;">Deals</div></div>
    `;
    const lEl = document.getElementById('deal-list');
    lEl.innerHTML = data.leads.slice(0, 15).map(l => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #d1fae5;">
      <span style="font-size:0.72rem;">${l.first_name} ${l.last_name} <span style="color:#78716c;">${l.city || ''}</span></span>
      <div style="display:flex;gap:0.4rem;font-size:0.65rem;">
        <span style="color:#059669;font-weight:600;">$${l.dealEstimate}</span>
        <span style="color:#f59e0b;">wt: $${l.weightedValue}</span>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Deal error:', err); }
}

// === Outreach Calendar (Batch 32) ===
async function loadOutreachCal() {
  try {
    const data = await fetch('/api/outreach-calendar').then(safeJSON);
    const bEl = document.getElementById('outreach-backlog');
    bEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#0284c7;">${data.backlog.emailReady}</div><div style="font-size:0.65rem;">Email Ready</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#059669;">${data.backlog.phoneReady}</div><div style="font-size:0.65rem;">Call Ready</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${data.backlog.emailDays}d</div><div style="font-size:0.65rem;">Email Backlog</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#dc2626;">${data.backlog.callDays}d</div><div style="font-size:0.65rem;">Call Backlog</div></div>
    `;
    const pEl = document.getElementById('outreach-plan');
    pEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#0284c7;margin-bottom:0.2rem;">7-Day Plan</div>' +
      data.plan.map(d => `<div style="display:flex;align-items:center;gap:0.3rem;padding:0.1rem 0;${d.isWeekend ? 'opacity:0.5;' : ''}">
        <span style="font-size:0.68rem;width:60px;">${d.dayName} ${d.date.slice(5)}</span>
        <span style="font-size:0.62rem;color:#0284c7;">${d.emails} emails</span>
        <span style="font-size:0.62rem;color:#059669;">${d.calls} calls</span>
      </div>`).join('');
  } catch (err) { console.error('Outreach cal error:', err); }
}

async function generateAiWeekPlan() {
  const el = document.getElementById('outreach-ai-plan');
  if (!el) return;
  el.style.display = 'block';
  el.innerHTML = '<div style="font-size:0.72rem;color:#0284c7;">Generating AI weekly outreach plan...</div>';
  try {
    const res = await fetch('/api/ai/outreach-plan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ goal: 'Maximize meetings booked this week' }) });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const dayColors = ['#3b82f6','#22c55e','#f59e0b','#ec4899','#8b5cf6'];
    let html = '<div style="font-size:0.68rem;font-weight:600;color:#0284c7;margin-bottom:0.3rem;">AI WEEKLY OUTREACH PLAN</div>';
    if (d.summary) html += `<div style="font-size:0.72rem;color:#374151;margin-bottom:0.3rem;line-height:1.4;">${esc(d.summary)}</div>`;
    const days = d.daily_plan || [];
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:0.4rem;margin-bottom:0.3rem;">';
    days.forEach((day, i) => {
      const color = dayColors[i % dayColors.length];
      html += `<div style="background:#fff;border:1px solid #bfdbfe;border-radius:8px;padding:0.4rem;border-top:3px solid ${color};">
        <div style="font-size:0.72rem;font-weight:600;color:${color};margin-bottom:0.2rem;">Day ${day.day}</div>
        <div style="font-size:0.62rem;color:#6b7280;margin-bottom:0.2rem;">${esc(day.focus || '')}</div>`;
      (day.leads || []).forEach(l => {
        const icon = l.action === 'call' ? '📞' : l.action === 'linkedin' ? '🔗' : '✉️';
        html += `<div style="font-size:0.62rem;padding:0.1rem 0;color:#1f2937;">${icon} ${esc(l.name || '')}</div>`;
      });
      html += '</div>';
    });
    html += '</div>';
    if (d.tips && d.tips.length > 0) {
      html += d.tips.map(t => `<div style="font-size:0.62rem;color:#0284c7;padding:0.05rem 0;">&#x1F4A1; ${esc(t)}</div>`).join('');
    }
    el.innerHTML = html;
  } catch (err) { el.innerHTML = `<div style="color:#dc2626;font-size:0.72rem;">Error: ${esc(err.message)}</div>`; }
}

// === Risk Scoring (Batch 32) ===
async function loadRiskScores() {
  try {
    const data = await fetch('/api/risk-scores?limit=20').then(safeJSON);
    const dEl = document.getElementById('risk-dist');
    dEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#dc2626;">${data.distribution.high}</div><div style="font-size:0.65rem;">High Risk</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#f59e0b;">${data.distribution.medium}</div><div style="font-size:0.65rem;">Medium</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#16a34a;">${data.distribution.low}</div><div style="font-size:0.65rem;">Low Risk</div></div>
    `;
    const lEl = document.getElementById('risk-list');
    if (!data.leads.length) { lEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;">No at-risk leads in active pipeline.</div>'; return; }
    const colors = { high: '#dc2626', medium: '#f59e0b', low: '#16a34a' };
    lEl.innerHTML = data.leads.map(l => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #fee2e2;">
      <span style="font-size:0.72rem;">${l.first_name} ${l.last_name} <span style="color:#78716c;">${l.pipeline_stage}</span></span>
      <div style="display:flex;gap:0.3rem;align-items:center;">
        <span style="font-size:0.65rem;font-weight:600;color:${colors[l.riskLevel]};">${l.riskScore}% risk</span>
        <span style="font-size:0.6rem;color:#78716c;">${l.daysSinceContact}d ago</span>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Risk error:', err); }
}

// === Network Map (Batch 32) ===
async function loadNetworkMap() {
  try {
    const data = await fetch('/api/network-map?limit=20').then(safeJSON);
    const cEl = document.getElementById('network-connectors');
    if (data.connectors && data.connectors.length) {
      cEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#a21caf;margin-bottom:0.2rem;">Key Connectors (Multi-City Firms)</div>' +
        data.connectors.slice(0, 10).map(c => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.15rem 0;border-bottom:1px solid #fae8ff;">
          <span style="font-size:0.72rem;">${c.first_name} ${c.last_name} <span style="color:#78716c;">${c.firm_name}</span></span>
          <span style="font-size:0.62rem;color:#a21caf;">${c.firm_cities} cities, ${c.firm_size} attorneys</span>
        </div>`).join('');
    }
    const fEl = document.getElementById('network-clusters');
    if (data.firmClusters.length) {
      fEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#a21caf;margin-bottom:0.2rem;">Firm Clusters</div>' +
        data.firmClusters.slice(0, 12).map(f => `<div style="font-size:0.68rem;padding:0.1rem 0;border-bottom:1px solid #fae8ff;">
          <strong>${f.firm_name}</strong>: ${f.members} members, ${f.cities} | Avg: ${f.avg_score}
        </div>`).join('');
    }
  } catch (err) { console.error('Network error:', err); }
}

// === Propensity Model (Batch 31) ===
async function loadPropensity() {
  try {
    const data = await fetch('/api/propensity?limit=30').then(safeJSON);
    const dEl = document.getElementById('propensity-dist');
    const colors = { high: '#16a34a', medium: '#f59e0b', low: '#dc2626' };
    dEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#16a34a;">${data.distribution.high}</div><div style="font-size:0.65rem;">High Propensity</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#f59e0b;">${data.distribution.medium}</div><div style="font-size:0.65rem;">Medium</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#dc2626;">${data.distribution.low}</div><div style="font-size:0.65rem;">Low</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#7c3aed;">${data.avgPropensity}%</div><div style="font-size:0.65rem;">Avg Score</div></div>
    `;
    const lEl = document.getElementById('propensity-list');
    lEl.innerHTML = data.leads.slice(0, 15).map(l => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #f3e8ff;">
      <span style="font-size:0.72rem;">${l.first_name} ${l.last_name} <span style="color:#78716c;">${l.city || ''}</span></span>
      <div style="display:flex;gap:0.3rem;align-items:center;">
        <div style="width:40px;background:#e5e7eb;border-radius:2px;height:8px;"><div style="width:${l.propensity}%;background:${colors[l.likelihood]};height:100%;border-radius:2px;"></div></div>
        <span style="font-size:0.65rem;font-weight:600;color:${colors[l.likelihood]};">${l.propensity}%</span>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Propensity error:', err); }
}

// === Cohort Analysis (Batch 31) ===
async function loadCohorts() {
  try {
    const data = await fetch('/api/cohorts').then(safeJSON);
    const cEl = document.getElementById('cohort-comparison');
    if (data.comparison) {
      const c = data.comparison;
      cEl.innerHTML = `<div style="display:flex;gap:0.8rem;font-size:0.72rem;flex-wrap:wrap;">
        <span>Score trend: <strong style="color:${c.scoreImprovement >= 0 ? '#16a34a' : '#dc2626'};">${c.scoreImprovement >= 0 ? '+' : ''}${c.scoreImprovement}</strong></span>
        <span>Email rate: <strong style="color:${c.emailRateChange >= 0 ? '#16a34a' : '#dc2626'};">${c.emailRateChange >= 0 ? '+' : ''}${c.emailRateChange}%</strong></span>
        <span>Volume: <strong style="color:${c.volumeChange >= 0 ? '#16a34a' : '#dc2626'};">${c.volumeChange >= 0 ? '+' : ''}${c.volumeChange}</strong></span>
      </div>`;
    }
    const tEl = document.getElementById('cohort-table');
    if (!data.cohorts.length) { tEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;">No cohort data.</div>'; return; }
    let html = '<table style="width:100%;font-size:0.65rem;border-collapse:collapse;"><thead><tr>';
    html += '<th style="text-align:left;padding:0.15rem;">Cohort</th><th>Leads</th><th>Email%</th><th>Active%</th><th>Avg Score</th></tr></thead><tbody>';
    data.cohorts.forEach(c => {
      html += `<tr><td style="padding:0.15rem;border-bottom:1px solid #d1fae5;">${c.cohort_week}</td>
        <td style="text-align:center;border-bottom:1px solid #d1fae5;">${c.total}</td>
        <td style="text-align:center;border-bottom:1px solid #d1fae5;">${c.emailRate}%</td>
        <td style="text-align:center;border-bottom:1px solid #d1fae5;">${c.activeRate}%</td>
        <td style="text-align:center;border-bottom:1px solid #d1fae5;">${c.avg_score}</td></tr>`;
    });
    html += '</tbody></table>';
    tEl.innerHTML = html;
  } catch (err) { console.error('Cohort error:', err); }
}

// === Channel Preferences (Batch 31) ===
async function loadChannelPrefs() {
  try {
    const data = await fetch('/api/channel-preferences').then(safeJSON);
    const sEl = document.getElementById('channel-stats');
    if (data.channels.length) {
      sEl.innerHTML = data.channels.map(c => `<div style="text-align:center;padding:0.2rem 0.5rem;background:rgba(255,255,255,0.6);border-radius:4px;">
        <div style="font-size:0.85rem;font-weight:700;color:#c2410c;">${c.channel}</div>
        <div style="font-size:0.65rem;">${c.total_touches} touches | ${c.responseRate}% resp.</div>
      </div>`).join('');
    } else { sEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;">No contact data yet.</div>'; }
    const rEl = document.getElementById('channel-recs');
    const r = data.recommendations;
    rEl.innerHTML = `<div style="font-size:0.68rem;display:flex;gap:0.6rem;flex-wrap:wrap;">
      <span>Email only: <strong>${r.withEmailOnly}</strong></span>
      <span>Phone only: <strong>${r.withPhoneOnly}</strong></span>
      <span>Both: <strong style="color:#16a34a;">${r.withBoth}</strong></span>
      <span>Neither: <strong style="color:#dc2626;">${r.withNeither}</strong></span>
    </div>`;
  } catch (err) { console.error('Channel prefs error:', err); }
}

// === Jurisdiction Benchmarks (Batch 31) ===
async function loadBenchmarks() {
  try {
    const data = await fetch('/api/benchmarks').then(safeJSON);
    const gEl = document.getElementById('benchmark-global');
    const g = data.globalAvg;
    gEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#2563eb;">${g.emailRate}%</div><div style="font-size:0.65rem;">Global Email Rate</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#059669;">${g.phoneRate}%</div><div style="font-size:0.65rem;">Global Phone Rate</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${g.avgScore}</div><div style="font-size:0.65rem;">Global Avg Score</div></div>
    `;
    const tEl = document.getElementById('benchmark-table');
    let html = '<table style="width:100%;font-size:0.62rem;border-collapse:collapse;"><thead><tr>';
    html += '<th style="text-align:left;padding:0.15rem;">State</th><th>Leads</th><th>Email%</th><th>Phone%</th><th>Web%</th><th>Score</th><th>Quality</th></tr></thead><tbody>';
    data.benchmarks.slice(0, 25).forEach(b => {
      const qColor = b.dataQuality >= 40 ? '#16a34a' : b.dataQuality >= 20 ? '#f59e0b' : '#dc2626';
      html += `<tr><td style="padding:0.15rem;border-bottom:1px solid #dbeafe;">${b.state}</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;">${b.total_leads}</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;">${b.emailRate}%</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;">${b.phoneRate}%</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;">${b.websiteRate}%</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;">${b.avg_score}</td>
        <td style="text-align:center;border-bottom:1px solid #dbeafe;font-weight:600;color:${qColor};">${b.dataQuality}</td></tr>`;
    });
    html += '</tbody></table>';
    tEl.innerHTML = html;
  } catch (err) { console.error('Benchmarks error:', err); }
}

// === Sequence Templates (Batch 30) ===
async function loadSeqTemplates() {
  try {
    const templates = await fetch('/api/sequence-templates').then(safeJSON);
    const el = document.getElementById('seq-templates-list');
    if (!templates.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No sequences. Create one above.</div>'; return; }
    el.innerHTML = templates.map(t => `<div style="padding:0.3rem 0;border-bottom:1px solid #e0e7ff;">
      <div style="display:flex;align-items:center;justify-content:space-between;">
        <span style="font-size:0.75rem;font-weight:500;">${t.name}</span>
        <div style="display:flex;gap:0.2rem;align-items:center;">
          <span style="font-size:0.6rem;background:#e0e7ff;padding:0.05rem 0.2rem;border-radius:3px;">${t.steps.length} steps</span>
          <span style="font-size:0.6rem;color:#6b7280;">${t.status}</span>
          <button onclick="deleteSeqTemplateItem(${t.id})" style="font-size:0.6rem;padding:0.05rem 0.2rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">x</button>
        </div>
      </div>
      ${t.description ? '<div style="font-size:0.65rem;color:#78716c;">' + t.description + '</div>' : ''}
    </div>`).join('');
  } catch (err) { console.error('Seq templates error:', err); }
}
async function createSeqTemplateFromUI() {
  const name = document.getElementById('seq-name').value.trim();
  if (!name) { toast('Enter sequence name', 'error'); return; }
  const steps = [
    { step: 1, delay: 0, subject: 'Hi {first_name} — quick question about {firm_name}', body: 'Hi {first_name},\n\nI noticed you practice in {city}, {state}...' },
    { step: 2, delay: 3, subject: 'Following up — {first_name}', body: 'Hi {first_name},\n\nJust wanted to follow up on my previous email...' },
    { step: 3, delay: 7, subject: 'Last try — {first_name} at {firm_name}', body: 'Hi {first_name},\n\nI know you are busy...' }
  ];
  try {
    await fetch('/api/sequence-templates', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, steps }) });
    document.getElementById('seq-name').value = '';
    toast('Sequence created with 3 default steps', 'success');
    loadSeqTemplates();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteSeqTemplateItem(id) {
  try { await fetch('/api/sequence-templates/' + id, { method: 'DELETE' }); toast('Deleted', 'success'); loadSeqTemplates(); } catch (err) { toast('Error', 'error'); }
}

// === Data Quality Rules (Batch 30) ===
async function loadQualityRulesPanel() {
  try {
    const rules = await fetch('/api/quality-rules').then(safeJSON);
    const el = document.getElementById('quality-rules-list');
    if (!rules.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No quality rules. Create one above.</div>'; return; }
    el.innerHTML = rules.map(r => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #fef3c7;">
      <div>
        <span style="font-size:0.72rem;font-weight:500;">${r.name}</span>
        <span style="font-size:0.62rem;color:#78716c;margin-left:0.2rem;">${r.field} ${r.check_type} ${r.check_value || ''}</span>
        ${r.flag_tag ? '<span style="background:#a16207;color:#fff;padding:0.02rem 0.2rem;border-radius:2px;font-size:0.58rem;margin-left:0.2rem;">' + r.flag_tag + '</span>' : ''}
      </div>
      <button onclick="deleteQualityRuleItem(${r.id})" style="font-size:0.6rem;padding:0.05rem 0.2rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">x</button>
    </div>`).join('');
  } catch (err) { console.error('Quality rules error:', err); }
}
async function createQualityRuleFromUI() {
  const name = document.getElementById('qr-name').value.trim();
  const field = document.getElementById('qr-field').value;
  const checkType = document.getElementById('qr-check').value;
  const checkValue = document.getElementById('qr-value').value.trim();
  const flagTag = document.getElementById('qr-tag').value.trim();
  if (!name) { toast('Rule name required', 'error'); return; }
  try {
    await fetch('/api/quality-rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, field, checkType, checkValue, flagTag }) });
    document.getElementById('qr-name').value = ''; document.getElementById('qr-value').value = ''; document.getElementById('qr-tag').value = '';
    toast('Quality rule created', 'success');
    loadQualityRulesPanel();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteQualityRuleItem(id) {
  try { await fetch('/api/quality-rules/' + id, { method: 'DELETE' }); toast('Deleted', 'success'); loadQualityRulesPanel(); } catch (err) { toast('Error', 'error'); }
}
async function runAllQualityRules() {
  try {
    const res = await fetch('/api/quality-rules/run', { method: 'POST' }).then(safeJSON);
    toast('Flagged ' + res.totalFlagged + ' leads across ' + res.rulesRun + ' rules', 'success');
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Unified Timeline (Batch 30) ===
async function loadTimelinePanel() {
  const el = document.getElementById('timeline-events');
  el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">Enter a lead ID to view their timeline.</div>';
}
async function showLeadTimeline() {
  const leadId = document.getElementById('timeline-lead-id').value;
  if (!leadId) { toast('Enter a lead ID', 'error'); return; }
  try {
    const events = await fetch('/api/leads/' + leadId + '/timeline').then(safeJSON);
    const el = document.getElementById('timeline-events');
    if (!events.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;">No events for this lead.</div>'; return; }
    const typeIcons = { created: '●', contact: '✉', stage_change: '→', audit: '✎', note: '📝' };
    const typeColors = { created: '#16a34a', contact: '#2563eb', stage_change: '#f59e0b', audit: '#6b7280', note: '#8b5cf6' };
    el.innerHTML = events.map(e => `<div style="display:flex;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #e2e8f0;">
      <span style="color:${typeColors[e.type] || '#6b7280'};font-size:0.8rem;min-width:16px;">${typeIcons[e.type] || '·'}</span>
      <div style="flex:1;">
        <div style="font-size:0.72rem;">${e.details}</div>
        <div style="font-size:0.6rem;color:#9ca3af;">${e.timestamp || ''} ${e.author ? '— ' + e.author : ''}</div>
      </div>
    </div>`).join('');
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Export Scheduler (Batch 30) ===
async function loadExportScheduler() {
  try {
    const schedules = await fetch('/api/export-schedules').then(safeJSON);
    const el = document.getElementById('export-sched-list');
    if (!schedules.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No export schedules. Create one above.</div>'; return; }
    el.innerHTML = schedules.map(s => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #dcfce7;">
      <div>
        <span style="font-size:0.72rem;font-weight:500;">${s.name}</span>
        <span style="font-size:0.62rem;color:#78716c;margin-left:0.2rem;">${s.frequency}</span>
        <span style="font-size:0.62rem;color:#15803d;margin-left:0.2rem;">${s.export_count} exports</span>
        ${s.next_export ? '<span style="font-size:0.6rem;color:#9ca3af;margin-left:0.2rem;">Next: ' + s.next_export.slice(0, 10) + '</span>' : ''}
      </div>
      <div style="display:flex;gap:0.2rem;">
        <button onclick="runExportSchedItem(${s.id})" style="font-size:0.6rem;padding:0.05rem 0.2rem;background:#dcfce7;color:#15803d;border:1px solid #86efac;border-radius:3px;cursor:pointer;">Run</button>
        <button onclick="deleteExportSchedItem(${s.id})" style="font-size:0.6rem;padding:0.05rem 0.2rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">x</button>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Export scheduler error:', err); }
}
async function createExportSchedFromUI() {
  const name = document.getElementById('es-name').value.trim();
  const frequency = document.getElementById('es-freq').value;
  if (!name) { toast('Name required', 'error'); return; }
  try {
    await fetch('/api/export-schedules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, frequency, filters: { hasEmail: true } }) });
    document.getElementById('es-name').value = '';
    toast('Export schedule created', 'success');
    loadExportScheduler();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteExportSchedItem(id) {
  try { await fetch('/api/export-schedules/' + id, { method: 'DELETE' }); toast('Deleted', 'success'); loadExportScheduler(); } catch (err) { toast('Error', 'error'); }
}
async function runExportSchedItem(id) {
  try {
    const res = await fetch('/api/export-schedules/' + id + '/run', { method: 'POST' }).then(safeJSON);
    toast('Exported ' + res.leadsExported + ' leads', 'success');
    loadExportScheduler();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Response Time SLA (Batch 29) ===
async function loadSLATracker() {
  try {
    const data = await fetch('/api/sla').then(safeJSON);
    const sEl = document.getElementById('sla-summary');
    const s = data.summary;
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#6b7280;">${s.total}</div><div style="font-size:0.65rem;color:#78716c;">Contacted</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#16a34a;">${s.withinSla}</div><div style="font-size:0.65rem;color:#78716c;">Within SLA</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#dc2626;">${s.overdue}</div><div style="font-size:0.65rem;color:#78716c;">Overdue</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${s.slaThreshold}h</div><div style="font-size:0.65rem;color:#78716c;">SLA Threshold</div></div>
    `;
    const lEl = document.getElementById('sla-list');
    if (!data.leads.length) { lEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No contacted leads to track.</div>'; return; }
    lEl.innerHTML = data.leads.slice(0, 15).map(l => {
      const statusColor = l.slaStatus === 'overdue' ? '#dc2626' : l.slaStatus === 'within_sla' ? '#16a34a' : '#6b7280';
      return `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #fee2e2;">
        <span style="font-size:0.72rem;">${l.first_name} ${l.last_name} <span style="color:#78716c;">${l.city || ''}</span></span>
        <div style="display:flex;gap:0.3rem;align-items:center;">
          <span style="font-size:0.62rem;color:${statusColor};font-weight:600;">${l.slaStatus.replace('_', ' ')}</span>
          ${l.hoursSinceOutbound != null ? '<span style="font-size:0.6rem;color:#78716c;">' + l.hoursSinceOutbound + 'h ago</span>' : ''}
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('SLA error:', err); }
}

// === Market Saturation (Batch 29) ===
async function loadSaturation() {
  try {
    const data = await fetch('/api/saturation').then(safeJSON);
    const uEl = document.getElementById('saturation-underserved');
    if (data.underServed && data.underServed.length) {
      uEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#7c3aed;margin-bottom:0.2rem;">Under-Served Markets (Growth Opportunity)</div>' +
        data.underServed.map(c => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.15rem 0;border-bottom:1px solid #ede9fe;">
          <span style="font-size:0.72rem;">${c.city}, ${c.state}</span>
          <div style="display:flex;gap:0.3rem;align-items:center;">
            <span style="font-size:0.65rem;color:#78716c;">${c.our_leads} leads</span>
            <span style="font-size:0.65rem;color:#7c3aed;font-weight:600;">${c.penetration}% penetration</span>
          </div>
        </div>`).join('');
    }
    const cEl = document.getElementById('market-saturation-cities');
    cEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#7c3aed;margin-bottom:0.2rem;">Top Markets by Volume</div>' +
      data.cities.slice(0, 20).map(c => {
        const barWidth = Math.max(5, c.penetration);
        return `<div style="display:flex;align-items:center;gap:0.3rem;padding:0.1rem 0;">
          <span style="font-size:0.68rem;width:120px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${c.city}</span>
          <div style="flex:1;background:#e5e7eb;border-radius:2px;height:10px;"><div style="width:${barWidth}%;background:#8b5cf6;height:100%;border-radius:2px;"></div></div>
          <span style="font-size:0.62rem;width:60px;text-align:right;">${c.our_leads} (${c.penetration}%)</span>
        </div>`;
      }).join('');
  } catch (err) { console.error('Saturation error:', err); }
}

// === Enrichment Waterfall (Batch 29) ===
async function loadEnrichWaterfall() {
  try {
    const data = await fetch('/api/enrichment-waterfall').then(safeJSON);
    const cEl = document.getElementById('enrich-waterfall-coverage');
    cEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#0d9488;">${data.total}</div><div style="font-size:0.65rem;">Total Leads</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#059669;">${data.coverage.email}</div><div style="font-size:0.65rem;">Emails (${data.total > 0 ? Math.round(data.coverage.email / data.total * 100) : 0}%)</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#2563eb;">${data.coverage.phone}</div><div style="font-size:0.65rem;">Phones (${data.total > 0 ? Math.round(data.coverage.phone / data.total * 100) : 0}%)</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${data.coverage.website}</div><div style="font-size:0.65rem;">Websites (${data.total > 0 ? Math.round(data.coverage.website / data.total * 100) : 0}%)</div></div>
    `;
    const sEl = document.getElementById('enrich-waterfall-steps');
    const stepColors = ['#3b82f6', '#8b5cf6', '#f59e0b', '#22c55e'];
    sEl.innerHTML = data.steps.map((s, i) => {
      const totalFilled = s.fills.email + s.fills.phone + s.fills.website;
      return `<div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #ccfbf1;">
        <span style="width:5px;height:20px;background:${stepColors[i]};border-radius:2px;"></span>
        <div style="flex:1;">
          <div style="font-size:0.72rem;font-weight:500;">${s.step}</div>
          <div style="font-size:0.62rem;color:#78716c;">${s.description}</div>
        </div>
        <div style="display:flex;gap:0.4rem;font-size:0.65rem;">
          <span style="color:#059669;">${s.fills.email} email</span>
          <span style="color:#2563eb;">${s.fills.phone} phone</span>
          <span style="color:#f59e0b;">${s.fills.website} web</span>
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Enrichment waterfall error:', err); }
}

// === Waterfall Run History (Batch 41) ===
async function loadWaterfallHistory() {
  try {
    const [summary, runs] = await Promise.all([
      fetch('/api/leads/waterfall/summary').then(safeJSON),
      fetch('/api/leads/waterfall/history?limit=15').then(safeJSON),
    ]);
    const sEl = document.getElementById('waterfall-history-summary');
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#059669;">${summary.totalRuns || 0}</div><div style="font-size:0.65rem;">Total Runs</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#0d9488;">${summary.total_enriched || 0}</div><div style="font-size:0.65rem;">Leads Enriched</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#2563eb;">${summary.total_fields || 0}</div><div style="font-size:0.65rem;">Fields Filled</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${summary.total_websites || 0}</div><div style="font-size:0.65rem;">Websites Found</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#8b5cf6;">${summary.total_emails || 0}</div><div style="font-size:0.65rem;">Emails Found</div></div>
    `;
    const tEl = document.getElementById('waterfall-history-table');
    if (!runs.length) {
      tEl.innerHTML = '<div style="font-size:0.72rem;color:#78716c;text-align:center;padding:1rem;">No waterfall runs yet. Click "Waterfall Enrich" to start.</div>';
      return;
    }
    tEl.innerHTML = `<table style="width:100%;font-size:0.68rem;border-collapse:collapse;">
      <thead><tr style="border-bottom:2px solid #a7f3d0;">
        <th style="text-align:left;padding:0.2rem;">When</th>
        <th style="text-align:left;padding:0.2rem;">State</th>
        <th style="text-align:right;padding:0.2rem;">Leads</th>
        <th style="text-align:right;padding:0.2rem;">Enriched</th>
        <th style="text-align:right;padding:0.2rem;">Fields</th>
        <th style="text-align:right;padding:0.2rem;">Profiles</th>
        <th style="text-align:right;padding:0.2rem;">Duration</th>
        <th style="text-align:center;padding:0.2rem;">Trigger</th>
      </tr></thead>
      <tbody>${runs.map(r => {
        const d = r.started_at ? new Date(r.started_at + 'Z') : null;
        const when = d ? d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '—';
        const dur = r.duration_secs > 0 ? (r.duration_secs >= 60 ? Math.round(r.duration_secs/60) + 'm' : r.duration_secs + 's') : '—';
        const trigBadge = r.trigger === 'auto-cron' ? '<span style="background:#dbeafe;color:#2563eb;padding:0.1rem 0.3rem;border-radius:3px;">auto</span>' : r.trigger === 'multi-state' ? '<span style="background:#e0e7ff;color:#4338ca;padding:0.1rem 0.3rem;border-radius:3px;">multi</span>' : '<span style="background:#fef3c7;color:#92400e;padding:0.1rem 0.3rem;border-radius:3px;">manual</span>';
        const failCount = r.profiles_failed || 0;
        const profText = (r.profiles_fetched || 0) + (failCount > 0 ? ' <span style="color:#dc2626;">(' + failCount + ' fail)</span>' : '');
        return `<tr style="border-bottom:1px solid #d1fae5;">
          <td style="padding:0.2rem;">${when}</td>
          <td style="padding:0.2rem;font-weight:500;">${r.state_filter || 'all'}</td>
          <td style="text-align:right;padding:0.2rem;">${r.leads_processed}</td>
          <td style="text-align:right;padding:0.2rem;color:#059669;font-weight:600;">${r.leads_enriched}</td>
          <td style="text-align:right;padding:0.2rem;color:#2563eb;">${r.fields_filled}</td>
          <td style="text-align:right;padding:0.2rem;">${profText}</td>
          <td style="text-align:right;padding:0.2rem;">${dur}</td>
          <td style="text-align:center;padding:0.2rem;">${trigBadge}</td>
        </tr>`;
      }).join('')}</tbody>
    </table>`;
  } catch (err) { console.error('Waterfall history error:', err); }
}

// === Recently Enriched Leads ===
async function loadRecentlyEnriched() {
  try {
    const hours = document.getElementById('enriched-hours')?.value || 24;
    const res = await fetch(`/api/leads/recently-enriched?hours=${hours}&limit=50`);
    const d = await safeJSON(res);
    const sEl = document.getElementById('recently-enriched-stats');
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#16a34a;">${d.stats?.total || 0}</div><div style="font-size:0.65rem;">Enriched</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#2563eb;">${d.stats?.withEmail || 0}</div><div style="font-size:0.65rem;">With Email</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#7c3aed;">${d.stats?.withPhone || 0}</div><div style="font-size:0.65rem;">With Phone</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${d.stats?.withWebsite || 0}</div><div style="font-size:0.65rem;">With Website</div></div>
    `;
    const tEl = document.getElementById('recently-enriched-table');
    if (!d.leads || !d.leads.length) {
      tEl.innerHTML = '<div style="font-size:0.72rem;color:#78716c;text-align:center;padding:1rem;">No leads enriched in the last ' + hours + ' hours.</div>';
      return;
    }
    tEl.innerHTML = `<table style="width:100%;font-size:0.68rem;border-collapse:collapse;">
      <thead><tr style="border-bottom:2px solid #bbf7d0;">
        <th style="text-align:left;padding:0.2rem;">Name</th>
        <th style="text-align:left;padding:0.2rem;">Firm</th>
        <th style="text-align:left;padding:0.2rem;">State</th>
        <th style="text-align:center;padding:0.2rem;">Email</th>
        <th style="text-align:center;padding:0.2rem;">Phone</th>
        <th style="text-align:center;padding:0.2rem;">Web</th>
        <th style="text-align:left;padding:0.2rem;">Steps</th>
        <th style="text-align:right;padding:0.2rem;">Score</th>
      </tr></thead>
      <tbody>${d.leads.slice(0, 30).map(l => {
        const sc = l.lead_score || 0;
        const scColor = sc >= 80 ? '#16a34a' : sc >= 55 ? '#2563eb' : sc >= 30 ? '#d97706' : '#dc2626';
        const check = '&#10003;', cross = '&#8212;';
        const steps = (l.enrichment_steps || '').split(',').map(s => {
          const colors = {profile:'#3b82f6',martindale:'#8b5cf6',lawyerscom:'#ec4899',smtp:'#f59e0b','email-crawl':'#10b981','name-lookup':'#f97316'};
          return '<span style="background:' + (colors[s] || '#94a3b8') + ';color:#fff;padding:0.1rem 0.25rem;border-radius:3px;font-size:0.6rem;">' + s + '</span>';
        }).join(' ');
        return '<tr style="border-bottom:1px solid #dcfce7;cursor:pointer;" onclick="showLeadDetail(' + l.id + ')">' +
          '<td style="padding:0.2rem;">' + (l.first_name || '') + ' ' + (l.last_name || '') + '</td>' +
          '<td style="padding:0.2rem;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + (l.firm_name || '—') + '</td>' +
          '<td style="padding:0.2rem;">' + (l.state || '') + '</td>' +
          '<td style="text-align:center;padding:0.2rem;color:' + (l.email ? '#16a34a' : '#d1d5db') + ';">' + (l.email ? check : cross) + '</td>' +
          '<td style="text-align:center;padding:0.2rem;color:' + (l.phone ? '#16a34a' : '#d1d5db') + ';">' + (l.phone ? check : cross) + '</td>' +
          '<td style="text-align:center;padding:0.2rem;color:' + (l.website ? '#16a34a' : '#d1d5db') + ';">' + (l.website ? check : cross) + '</td>' +
          '<td style="padding:0.2rem;">' + steps + '</td>' +
          '<td style="text-align:right;padding:0.2rem;color:' + scColor + ';font-weight:600;">' + sc + '</td>' +
        '</tr>';
      }).join('')}</tbody>
    </table>`;
  } catch (err) { console.error('Recently enriched error:', err); }
}

// === Competitive Intelligence (Batch 29) ===
function exportRecentlyEnriched() {
  const hours = document.getElementById('enriched-hours')?.value || 24;
  const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  window.open('/api/leads/export?enrichedAfter=' + encodeURIComponent(cutoff), '_blank');
}

async function loadEnrichmentPriority() {
  try {
    const data = await fetch('/api/leads/enrichment-priority').then(safeJSON);
    const tEl = document.getElementById('enrichment-priority-table');
    if (!data || !data.length) {
      tEl.innerHTML = '<div style="font-size:0.72rem;color:#78716c;text-align:center;padding:1rem;">No enrichable leads found.</div>';
      return;
    }
    const maxEnrichable = Math.max(...data.map(d => d.enrichable));
    tEl.innerHTML = `<table style="width:100%;font-size:0.68rem;border-collapse:collapse;">
      <thead><tr style="border-bottom:2px solid #bfdbfe;">
        <th style="text-align:left;padding:0.2rem;">State</th>
        <th style="text-align:right;padding:0.2rem;">Total</th>
        <th style="text-align:right;padding:0.2rem;">Enrichable</th>
        <th style="text-align:right;padding:0.2rem;">Missing Phone</th>
        <th style="text-align:right;padding:0.2rem;">Missing Email</th>
        <th style="padding:0.2rem;width:30%;">Progress</th>
        <th style="padding:0.2rem;"></th>
      </tr></thead>
      <tbody>${data.slice(0, 20).map(d => {
        const pct = d.total > 0 ? Math.round((1 - d.enrichable / d.total) * 100) : 100;
        const barWidth = maxEnrichable > 0 ? Math.round(d.enrichable / maxEnrichable * 100) : 0;
        return '<tr style="border-bottom:1px solid #dbeafe;">' +
          '<td style="padding:0.2rem;font-weight:600;">' + d.state + '</td>' +
          '<td style="text-align:right;padding:0.2rem;">' + d.total + '</td>' +
          '<td style="text-align:right;padding:0.2rem;color:#1d4ed8;font-weight:600;">' + d.enrichable + '</td>' +
          '<td style="text-align:right;padding:0.2rem;color:#dc2626;">' + d.missing_phone + '</td>' +
          '<td style="text-align:right;padding:0.2rem;color:#d97706;">' + d.missing_email + '</td>' +
          '<td style="padding:0.2rem;"><div style="background:#e2e8f0;border-radius:4px;height:10px;"><div style="background:linear-gradient(90deg,#3b82f6,#1d4ed8);height:100%;border-radius:4px;width:' + barWidth + '%;"></div></div></td>' +
          '<td style="padding:0.2rem;"><button onclick="startStateWaterfall(\'' + d.state + '\')" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#3b82f6;color:#fff;border:none;border-radius:3px;cursor:pointer;">Enrich</button></td>' +
        '</tr>';
      }).join('')}</tbody>
    </table>`;
  } catch (err) { console.error('Enrichment priority error:', err); }
}

async function startStateWaterfall(state) {
  if (!confirm('Start waterfall enrichment for ' + state + ' (up to 500 leads)?')) return;
  try {
    const res = await fetch('/api/leads/waterfall', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state, limit: 500, fetchProfiles: true, smtpPatterns: true, crossRefMartindale: false, crossRefLawyersCom: false, nameLookups: false, emailCrawl: false }),
    });
    const d = await safeJSON(res);
    if (d.error) { alert('Error: ' + d.error); return; }
    // Start live progress polling
    pollWaterfallStatus();
  } catch (err) { alert('Failed: ' + err.message); }
}

async function loadEnrichmentCoverage() {
  try {
    const data = await fetch('/api/leads/enrichment-coverage').then(safeJSON);
    const t = data.totals || {};
    const tEl = document.getElementById('enrichment-coverage-totals');
    tEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#7c3aed;">${fmt(t.enriched || 0)}</div><div style="font-size:0.65rem;">Enriched</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#2563eb;">${t.total > 0 ? Math.round(t.enriched / t.total * 100) : 0}%</div><div style="font-size:0.65rem;">Coverage</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#059669;">${fmt(t.withPhone || 0)}</div><div style="font-size:0.65rem;">Phones</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#dc2626;">${fmt(t.withEmail || 0)}</div><div style="font-size:0.65rem;">Emails</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${fmt(t.withWebsite || 0)}</div><div style="font-size:0.65rem;">Websites</div></div>
    `;
    const table = document.getElementById('enrichment-coverage-table');
    if (!data.states || !data.states.length) {
      table.innerHTML = '<div style="font-size:0.72rem;color:#78716c;text-align:center;padding:1rem;">No data.</div>';
      return;
    }
    table.innerHTML = `<table style="width:100%;font-size:0.68rem;border-collapse:collapse;">
      <thead><tr style="border-bottom:2px solid #e9d5ff;">
        <th style="text-align:left;padding:0.2rem;">State</th>
        <th style="text-align:right;padding:0.2rem;">Total</th>
        <th style="text-align:right;padding:0.2rem;">Enriched</th>
        <th style="padding:0.2rem;width:15%;">Coverage</th>
        <th style="text-align:right;padding:0.2rem;">Phone</th>
        <th style="text-align:right;padding:0.2rem;">Email</th>
        <th style="text-align:right;padding:0.2rem;">Web</th>
        <th style="text-align:right;padding:0.2rem;">Avg Score</th>
      </tr></thead>
      <tbody>${data.states.filter(s => s.total > 10).map(s => {
        const pct = s.total > 0 ? Math.round(s.enriched / s.total * 100) : 0;
        const phonePct = s.total > 0 ? Math.round(s.withPhone / s.total * 100) : 0;
        const barColor = pct >= 80 ? '#22c55e' : pct >= 40 ? '#f59e0b' : pct > 0 ? '#3b82f6' : '#e2e8f0';
        return '<tr style="border-bottom:1px solid #f3e8ff;">' +
          '<td style="padding:0.2rem;font-weight:600;">' + s.state + '</td>' +
          '<td style="text-align:right;padding:0.2rem;">' + s.total + '</td>' +
          '<td style="text-align:right;padding:0.2rem;color:#7c3aed;">' + s.enriched + '</td>' +
          '<td style="padding:0.2rem;"><div style="display:flex;align-items:center;gap:0.3rem;"><div style="flex:1;background:#f3e8ff;border-radius:3px;height:8px;"><div style="height:100%;border-radius:3px;background:' + barColor + ';width:' + pct + '%;"></div></div><span style="font-size:0.6rem;min-width:25px;text-align:right;">' + pct + '%</span></div></td>' +
          '<td style="text-align:right;padding:0.2rem;color:' + (phonePct > 50 ? '#059669' : '#dc2626') + ';">' + s.withPhone + '</td>' +
          '<td style="text-align:right;padding:0.2rem;">' + s.withEmail + '</td>' +
          '<td style="text-align:right;padding:0.2rem;">' + s.withWebsite + '</td>' +
          '<td style="text-align:right;padding:0.2rem;font-weight:500;">' + (s.avgScore || 0) + '</td>' +
        '</tr>';
      }).join('')}</tbody>
    </table>`;
  } catch (err) { console.error('Enrichment coverage error:', err); }
}

async function loadEnrichmentFailures() {
  try {
    const data = await fetch('/api/leads/enrichment-failures').then(safeJSON);
    const statsEl = document.getElementById('enrichment-failures-stats');
    const s = data.stats || {};
    statsEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#dc2626;">${fmt(s.total || 0)}</div><div style="font-size:0.65rem;">Failed</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${fmt(s.exhausted || 0)}</div><div style="font-size:0.65rem;">3+ Attempts</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#6b7280;">${(s.byState || []).length}</div><div style="font-size:0.65rem;">States</div></div>
    `;
    const table = document.getElementById('enrichment-failures-table');
    if (!data.failures || !data.failures.length) {
      table.innerHTML = '<div style="font-size:0.72rem;color:#78716c;text-align:center;padding:1rem;">No enrichment failures recorded.</div>';
      return;
    }
    // Show error breakdown + recent failures
    let html = '';
    if (s.byError && s.byError.length) {
      html += '<div style="font-size:0.68rem;font-weight:600;color:#dc2626;margin-bottom:0.2rem;">Error Types</div>';
      html += s.byError.slice(0, 8).map(e =>
        '<div style="display:flex;justify-content:space-between;padding:0.15rem 0;border-bottom:1px solid #fecaca;font-size:0.68rem;">' +
          '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:260px;" title="' + (e.error || '').replace(/"/g, '&quot;') + '">' + (e.error || 'Unknown') + '</span>' +
          '<span style="font-weight:600;color:#dc2626;min-width:30px;text-align:right;">' + e.cnt + '</span>' +
        '</div>'
      ).join('');
    }
    if (s.byState && s.byState.length) {
      html += '<div style="font-size:0.68rem;font-weight:600;color:#dc2626;margin:0.4rem 0 0.2rem;">By State</div>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:0.3rem;">';
      html += s.byState.slice(0, 15).map(e =>
        '<span style="font-size:0.65rem;background:#fecaca;color:#dc2626;padding:0.1rem 0.3rem;border-radius:3px;">' + e.state + ': ' + e.cnt + '</span>'
      ).join('');
      html += '</div>';
    }
    html += '<div style="font-size:0.68rem;font-weight:600;color:#dc2626;margin:0.4rem 0 0.2rem;">Recent Failures</div>';
    html += '<table style="width:100%;font-size:0.68rem;border-collapse:collapse;"><thead><tr style="border-bottom:2px solid #fecaca;">' +
      '<th style="text-align:left;padding:0.2rem;">Name</th><th style="text-align:left;padding:0.2rem;">State</th>' +
      '<th style="text-align:right;padding:0.2rem;">Attempts</th><th style="text-align:left;padding:0.2rem;">Error</th>' +
      '</tr></thead><tbody>';
    html += data.failures.slice(0, 20).map(f =>
      '<tr style="border-bottom:1px solid #fee2e2;">' +
        '<td style="padding:0.2rem;cursor:pointer;color:#2563eb;text-decoration:underline;" onclick="openLeadDetail(' + f.id + ')">' + (f.first_name || '') + ' ' + (f.last_name || '') + '</td>' +
        '<td style="padding:0.2rem;">' + (f.state || '') + '</td>' +
        '<td style="text-align:right;padding:0.2rem;font-weight:600;color:' + (f.enrichment_attempts >= 3 ? '#dc2626' : '#f59e0b') + ';">' + (f.enrichment_attempts || 0) + '</td>' +
        '<td style="padding:0.2rem;font-size:0.62rem;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + (f.last_enrichment_error || '').replace(/"/g, '&quot;') + '">' + (f.last_enrichment_error || '') + '</td>' +
      '</tr>'
    ).join('');
    html += '</tbody></table>';
    table.innerHTML = html;
  } catch (err) { console.error('Enrichment failures error:', err); }
}

async function clearEnrichmentFailures() {
  if (!confirm('Clear all enrichment error flags? This allows these leads to be retried.')) return;
  try {
    await fetch('/api/leads/enrichment-failures/clear', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
    toast('Enrichment errors cleared');
    loadEnrichmentFailures();
  } catch (err) {
    toast('Error clearing: ' + err.message, 'error');
  }
}

async function loadCompetitiveIntel() {
  try {
    const data = await fetch('/api/competitive').then(safeJSON);
    const fEl = document.getElementById('competitive-firms');
    if (data.multiStateFirms.length) {
      fEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#c2410c;margin-bottom:0.2rem;">Multi-Jurisdiction Firms</div>' +
        data.multiStateFirms.slice(0, 15).map(f => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #fed7aa;">
          <div style="flex:1;">
            <span style="font-size:0.72rem;font-weight:500;">${f.firm_name}</span>
            <span style="font-size:0.62rem;color:#78716c;margin-left:0.2rem;">${f.attorneys} attorneys</span>
          </div>
          <span style="font-size:0.62rem;color:#c2410c;">${f.states} states: ${f.state_list}</span>
        </div>`).join('');
    } else { fEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;">No multi-state firms found.</div>'; }
    const pEl = document.getElementById('competitive-practices');
    const practices = Object.entries(data.practiceLeaders).slice(0, 8);
    if (practices.length) {
      pEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#c2410c;margin-bottom:0.2rem;">Practice Area Leaders</div>' +
        practices.map(([practice, firms]) => `<div style="font-size:0.68rem;padding:0.1rem 0;"><strong>${practice}:</strong> ${firms.map(f => f.firm + ' (' + f.attorneys + ')').join(', ')}</div>`).join('');
    }
  } catch (err) { console.error('Competitive intel error:', err); }
}

// === Lead Clustering (Batch 28) ===
async function loadLeadClusters() {
  try {
    const data = await fetch('/api/clusters').then(safeJSON);
    const tEl = document.getElementById('cluster-tiers');
    const tierColors = { solo: '#6b7280', small: '#3b82f6', mid: '#f59e0b', large: '#16a34a' };
    tEl.innerHTML = Object.entries(data.clusters).map(([tier, info]) => `<div style="text-align:center;padding:0.3rem 0.6rem;background:rgba(255,255,255,0.6);border-radius:6px;">
      <div style="font-size:1rem;font-weight:700;color:${tierColors[tier] || '#6b7280'};">${info.leads || info.count || 0}</div>
      <div style="font-size:0.65rem;color:#78716c;">${info.label || tier}</div>
      <div style="font-size:0.6rem;color:#9ca3af;">${info.count || 0} firms</div>
    </div>`).join('');
    const oEl = document.getElementById('cluster-opportunities');
    if (data.topOpportunities && data.topOpportunities.length) {
      oEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#a16207;margin-bottom:0.2rem;">Top Revenue Opportunities</div>' +
        data.topOpportunities.map(o => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #fef9c3;">
          <span style="font-size:0.72rem;">${o.firm} <span style="color:#78716c;">(${o.headcount} attorneys)</span></span>
          <span style="font-size:0.65rem;color:#16a34a;font-weight:600;">$${(o.revenueEstimate / 1000).toFixed(1)}k est.</span>
        </div>`).join('');
    }
  } catch (err) { console.error('Clusters error:', err); }
}

// === A/B Test Framework (Batch 28) ===
async function loadAbTests() {
  try {
    const tests = await fetch('/api/ab-tests').then(safeJSON);
    const el = document.getElementById('ab-tests-list');
    if (!tests.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No A/B tests. Create one above.</div>'; return; }
    el.innerHTML = tests.map(t => {
      const winnerBadge = t.winner === 'A' ? '<span style="background:#16a34a;color:#fff;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;">A wins</span>' :
        t.winner === 'B' ? '<span style="background:#2563eb;color:#fff;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;">B wins</span>' :
        t.winner === 'tie' ? '<span style="background:#6b7280;color:#fff;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;">Tie</span>' : '';
      return `<div style="padding:0.3rem 0;border-bottom:1px solid #e0f2fe;">
        <div style="display:flex;align-items:center;justify-content:space-between;">
          <span style="font-size:0.75rem;font-weight:500;">${t.name}</span>
          <div style="display:flex;gap:0.2rem;align-items:center;">
            <span style="font-size:0.6rem;background:#e0f2fe;padding:0.05rem 0.2rem;border-radius:3px;">${t.status}</span>
            ${winnerBadge}
            <button onclick="deleteAbTestItem(${t.id})" style="font-size:0.6rem;padding:0.05rem 0.2rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">x</button>
          </div>
        </div>
        <div style="font-size:0.68rem;color:#78716c;margin-top:0.1rem;">
          A: "${t.variant_a}" (${t.variantA.count} leads, ${t.variantA.rate}% resp.) |
          B: "${t.variant_b}" (${t.variantB.count} leads, ${t.variantB.rate}% resp.)
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('AB tests error:', err); }
}
async function createAbTestFromUI() {
  const name = document.getElementById('ab-name').value.trim();
  const variantA = document.getElementById('ab-variant-a').value.trim();
  const variantB = document.getElementById('ab-variant-b').value.trim();
  if (!name || !variantA || !variantB) { toast('Fill in all fields', 'error'); return; }
  try {
    await fetch('/api/ab-tests', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, variantA, variantB }) });
    document.getElementById('ab-name').value = ''; document.getElementById('ab-variant-a').value = ''; document.getElementById('ab-variant-b').value = '';
    toast('A/B test created', 'success');
    loadAbTests();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteAbTestItem(id) {
  try {
    await fetch('/api/ab-tests/' + id, { method: 'DELETE' });
    toast('Test deleted', 'success');
    loadAbTests();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Re-engagement Scoring (Batch 28) ===
async function loadReengagement() {
  try {
    const data = await fetch('/api/reengagement?limit=20').then(safeJSON);
    const sEl = document.getElementById('reengage-summary');
    const s = data.summary;
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#e11d48;">${s.totalDormant}</div><div style="font-size:0.65rem;color:#78716c;">Dormant</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#f59e0b;">${s.neverContacted}</div><div style="font-size:0.65rem;color:#78716c;">Never Contacted</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#6b7280;">${s.avgScore}</div><div style="font-size:0.65rem;color:#78716c;">Avg Score</div></div>
      <div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:#3b82f6;">${s.avgDaysDormant}d</div><div style="font-size:0.65rem;color:#78716c;">Avg Dormant</div></div>
    `;
    const lEl = document.getElementById('reengage-list');
    if (!data.leads.length) { lEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;">No leads need re-engagement.</div>'; return; }
    lEl.innerHTML = data.leads.map(l => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #ffe4e6;">
      <div style="flex:1;">
        <span style="font-size:0.72rem;font-weight:500;">${l.first_name} ${l.last_name}</span>
        <span style="font-size:0.62rem;color:#78716c;margin-left:0.2rem;">Score: ${l.lead_score}</span>
      </div>
      <span style="font-size:0.62rem;color:#e11d48;max-width:40%;text-align:right;">${l.strategy}</span>
    </div>`).join('');
  } catch (err) { console.error('Reengagement error:', err); }
}

// === Attribution Model (Batch 28) ===
async function loadAttribution() {
  try {
    const data = await fetch('/api/attribution').then(safeJSON);
    const qEl = document.getElementById('attribution-quality');
    if (data.sourceQuality && data.sourceQuality.length) {
      qEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#059669;margin-bottom:0.2rem;">Source Quality Ranking</div>' +
        data.sourceQuality.slice(0, 15).map((s, i) => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #d1fae5;">
          <span style="font-size:0.72rem;"><span style="color:#9ca3af;">#${i + 1}</span> ${s.source}</span>
          <div style="display:flex;gap:0.4rem;font-size:0.65rem;">
            <span>${s.leads} leads</span>
            <span style="color:#059669;">${s.emailRate}% email</span>
            <span style="color:#2563eb;">score ${s.avgScore}</span>
            <span style="font-weight:600;color:#b45309;">Q: ${s.quality}</span>
          </div>
        </div>`).join('');
    }
    const eEl = document.getElementById('attribution-enrichment');
    if (data.enrichAttribution) {
      const ea = data.enrichAttribution;
      let html = '<div style="font-size:0.68rem;font-weight:600;color:#059669;margin-bottom:0.2rem;">Enrichment Attribution</div><div style="display:flex;gap:1rem;flex-wrap:wrap;">';
      for (const [field, sources] of Object.entries(ea)) {
        if (sources.length) {
          html += `<div><span style="font-size:0.65rem;font-weight:600;">${field}:</span> ${sources.slice(0, 3).map(s => `<span style="font-size:0.62rem;">${s.source} (${s.count})</span>`).join(', ')}</div>`;
        }
      }
      html += '</div>';
      eEl.innerHTML = html;
    }
  } catch (err) { console.error('Attribution error:', err); }
}

// === Lookalike Finder (Batch 27) ===
async function loadLookalikes() {
  // Default: show top-scored lead's lookalikes
  try {
    const stats = await fetch('/api/leads/stats').then(safeJSON);
    if (stats.total > 0) {
      const leads = await fetch('/api/leads?limit=1&sort=lead_score&order=desc').then(safeJSON);
      if (leads.leads && leads.leads.length > 0) {
        document.getElementById('lookalike-lead-id').value = leads.leads[0].id;
        searchLookalikes();
      }
    }
  } catch (err) { console.error('Lookalike load error:', err); }
}
async function searchLookalikes() {
  const leadId = document.getElementById('lookalike-lead-id').value;
  if (!leadId) { toast('Enter a lead ID', 'error'); return; }
  try {
    const data = await fetch('/api/leads/' + leadId + '/lookalikes?limit=15').then(safeJSON);
    const el = document.getElementById('lookalike-results');
    if (!data.target) { el.innerHTML = '<div style="font-size:0.72rem;color:#dc2626;">Lead not found.</div>'; return; }
    const t = data.target;
    let html = `<div style="background:rgba(255,255,255,0.6);padding:0.3rem 0.5rem;border-radius:4px;margin-bottom:0.3rem;font-size:0.72rem;">
      <strong>Target:</strong> ${t.first_name} ${t.last_name} | ${t.city || '?'}, ${t.state || '?'} | ${t.practice_area || 'N/A'} | Score: ${t.lead_score || 0}
    </div>`;
    if (!data.matches.length) { html += '<div style="font-size:0.72rem;color:#9ca3af;">No similar leads found.</div>'; }
    else {
      html += data.matches.map(m => {
        const simColor = m.similarity >= 60 ? '#16a34a' : m.similarity >= 40 ? '#f59e0b' : '#6b7280';
        return `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #dbeafe;">
          <span style="font-size:0.72rem;">${m.first_name} ${m.last_name} <span style="color:#78716c;">${m.city || ''}, ${m.state || ''}</span></span>
          <span style="font-size:0.65rem;font-weight:600;color:${simColor};">${m.similarity}% match</span>
        </div>`;
      }).join('');
    }
    el.innerHTML = html;
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Conversion Funnel (Batch 27) ===
async function loadConversionFunnel() {
  try {
    const data = await fetch('/api/funnel').then(safeJSON);
    const el = document.getElementById('conversion-funnel-stages');
    const maxCount = Math.max(...data.funnel.map(f => f.count), 1);
    el.innerHTML = data.funnel.map(f => {
      const width = Math.max(5, Math.round((f.count / maxCount) * 100));
      const stageColors = { new: '#3b82f6', contacted: '#8b5cf6', qualified: '#f59e0b', proposal: '#ec4899', won: '#22c55e', lost: '#ef4444' };
      const color = stageColors[f.stage] || '#6b7280';
      return `<div style="margin-bottom:0.2rem;">
        <div style="display:flex;align-items:center;gap:0.4rem;">
          <span style="font-size:0.7rem;width:60px;text-align:right;color:#374151;">${f.stage}</span>
          <div style="flex:1;background:#e5e7eb;border-radius:3px;height:16px;position:relative;">
            <div style="width:${width}%;background:${color};border-radius:3px;height:100%;"></div>
          </div>
          <span style="font-size:0.68rem;color:#374151;width:45px;">${f.count}</span>
          ${f.dropOff > 0 ? '<span style="font-size:0.6rem;color:#dc2626;">-' + f.dropOff + '%</span>' : ''}
        </div>
      </div>`;
    }).join('');
    // Source breakdown
    const sEl = document.getElementById('conversion-funnel-sources');
    const srcEntries = Object.entries(data.bySource).slice(0, 10);
    if (srcEntries.length) {
      sEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#16a34a;margin-bottom:0.2rem;">Top Sources by Stage</div>' +
        srcEntries.map(([src, stages]) => {
          const total = Object.values(stages).reduce((a, b) => a + b, 0);
          return `<div style="font-size:0.68rem;padding:0.15rem 0;border-bottom:1px solid #dcfce7;">${src}: ${total} leads (${Object.entries(stages).map(([s, c]) => s + ':' + c).join(', ')})</div>`;
        }).join('');
    }
  } catch (err) { console.error('Funnel error:', err); }
}

// === Lead Velocity (Batch 27) ===
async function loadLeadVelocity() {
  try {
    const data = await fetch('/api/velocity?days=30').then(safeJSON);
    const sEl = document.getElementById('velocity-stats');
    const trendColor = data.trend > 0 ? '#16a34a' : data.trend < 0 ? '#dc2626' : '#6b7280';
    const trendIcon = data.trend > 0 ? '&uarr;' : data.trend < 0 ? '&darr;' : '&rarr;';
    sEl.innerHTML = `
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#a21caf;">${data.totalNew}</div><div style="font-size:0.65rem;color:#78716c;">New (${data.days}d)</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#a21caf;">${data.avgDaily}</div><div style="font-size:0.65rem;color:#78716c;">Avg/Day</div></div>
      <div style="text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:${trendColor};">${trendIcon} ${Math.abs(data.trend)}%</div><div style="font-size:0.65rem;color:#78716c;">Trend</div></div>
    `;
    const cEl = document.getElementById('velocity-chart');
    if (data.weekly.length) {
      const maxW = Math.max(...data.weekly.map(w => w.count), 1);
      cEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#a21caf;margin-bottom:0.2rem;">Weekly Acquisition</div>' +
        data.weekly.map(w => `<div style="display:flex;align-items:center;gap:0.3rem;margin-bottom:0.1rem;">
          <span style="font-size:0.65rem;width:60px;color:#78716c;">${w.week}</span>
          <div style="flex:1;background:#f3e8ff;border-radius:2px;height:12px;"><div style="width:${Math.round((w.count / maxW) * 100)}%;background:#d946ef;height:100%;border-radius:2px;"></div></div>
          <span style="font-size:0.65rem;width:30px;text-align:right;">${w.count}</span>
        </div>`).join('');
    } else {
      cEl.innerHTML = data.bySource.slice(0, 8).map(s => `<div style="font-size:0.68rem;padding:0.1rem 0;">${s.source}: <strong>${s.count}</strong></div>`).join('');
    }
  } catch (err) { console.error('Velocity error:', err); }
}

// === Completeness Matrix (Batch 27) ===
async function loadCompletenessMatrix() {
  try {
    const data = await fetch('/api/completeness-matrix').then(safeJSON);
    // Gaps
    const gEl = document.getElementById('matrix-gaps');
    if (data.gaps.length) {
      gEl.innerHTML = '<div style="font-size:0.68rem;font-weight:600;color:#c2410c;margin-bottom:0.2rem;">Data Gaps</div>' +
        data.gaps.map(g => `<div style="font-size:0.68rem;padding:0.1rem 0;border-bottom:1px solid #ffedd5;">
          <strong>${g.field}</strong>: ${g.fillRate}% filled (${g.totalMissing} missing) — ${g.recommendation}
        </div>`).join('');
    } else { gEl.innerHTML = '<div style="font-size:0.72rem;color:#16a34a;">All fields above 50% fill rate!</div>'; }
    // Matrix grid
    const mEl = document.getElementById('matrix-grid');
    if (!data.matrix.length) { mEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;">No source data.</div>'; return; }
    const shortFields = data.fields.map(f => f.replace('practice_area', 'practice').replace('admission_date', 'admit').replace('bar_number', 'bar#').replace('bar_status', 'status').replace('firm_name', 'firm'));
    let tableHtml = '<table style="width:100%;font-size:0.62rem;border-collapse:collapse;"><thead><tr><th style="text-align:left;padding:0.15rem;border-bottom:2px solid #fdba74;">Source</th>';
    shortFields.forEach(f => { tableHtml += `<th style="padding:0.15rem;border-bottom:2px solid #fdba74;text-align:center;">${f}</th>`; });
    tableHtml += '<th style="padding:0.15rem;border-bottom:2px solid #fdba74;">Total</th></tr></thead><tbody>';
    data.matrix.slice(0, 20).forEach(row => {
      tableHtml += `<tr><td style="padding:0.15rem;border-bottom:1px solid #ffedd5;white-space:nowrap;">${row.source}</td>`;
      data.fields.forEach(f => {
        const r = row.fields[f];
        const bg = r.rate >= 80 ? '#dcfce7' : r.rate >= 50 ? '#fef9c3' : r.rate >= 20 ? '#ffedd5' : '#fee2e2';
        tableHtml += `<td style="text-align:center;padding:0.15rem;border-bottom:1px solid #ffedd5;background:${bg};">${r.rate}%</td>`;
      });
      tableHtml += `<td style="text-align:center;padding:0.15rem;border-bottom:1px solid #ffedd5;font-weight:600;">${row.total}</td></tr>`;
    });
    tableHtml += '</tbody></table>';
    mEl.innerHTML = tableHtml;
  } catch (err) { console.error('Completeness matrix error:', err); }
}

// === Tagging Rules Engine (Batch 26) ===
async function loadTagRulesEngine() {
  try {
    const rules = await fetch('/api/tag-rules').then(safeJSON);
    const el = document.getElementById('tag-rules-list');
    if (!rules.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No tagging rules defined. Create one above.</div>'; return; }
    el.innerHTML = rules.map(r => {
      const conds = JSON.parse(r.conditions || '[]');
      const condStr = conds.map(c => `<span style="background:#fce7f3;padding:0.1rem 0.3rem;border-radius:3px;font-size:0.65rem;">${c.field} ${c.operator} ${c.value || ''}</span>`).join(` <span style="font-size:0.6rem;color:#9ca3af;">${r.logic || 'AND'}</span> `);
      return `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.3rem 0;border-bottom:1px solid #fce7f3;">
        <div style="flex:1;">
          <span style="font-size:0.75rem;font-weight:500;">${r.name}</span>
          <span style="background:#be185d;color:#fff;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;margin-left:0.3rem;">${r.tag}</span>
          ${r.enabled ? '<span style="color:#16a34a;font-size:0.6rem;margin-left:0.2rem;">active</span>' : '<span style="color:#9ca3af;font-size:0.6rem;margin-left:0.2rem;">disabled</span>'}
          <div style="margin-top:0.15rem;">${condStr}</div>
        </div>
        <div style="display:flex;gap:0.2rem;">
          <button onclick="toggleTagRuleItem(${r.id})" style="font-size:0.62rem;padding:0.1rem 0.3rem;background:#fce7f3;border:1px solid #f9a8d4;border-radius:3px;cursor:pointer;">${r.enabled ? 'Disable' : 'Enable'}</button>
          <button onclick="deleteTagRuleItem(${r.id})" style="font-size:0.62rem;padding:0.1rem 0.3rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">Del</button>
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Tag rules error:', err); }
}
async function createTagRuleFromUI() {
  const name = document.getElementById('tag-rule-name').value.trim();
  const tag = document.getElementById('tag-rule-tag').value.trim();
  const field = document.getElementById('tag-rule-field').value;
  const operator = document.getElementById('tag-rule-op').value;
  const value = document.getElementById('tag-rule-value').value.trim();
  if (!name || !tag) { toast('Rule name and tag required', 'error'); return; }
  try {
    await fetch('/api/tag-rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, tag, conditions: [{ field, operator, value }], logic: 'AND' }) });
    document.getElementById('tag-rule-name').value = '';
    document.getElementById('tag-rule-tag').value = '';
    document.getElementById('tag-rule-value').value = '';
    toast('Tag rule created', 'success');
    loadTagRulesEngine();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function toggleTagRuleItem(id) {
  try {
    await fetch('/api/tag-rules/' + id + '/toggle', { method: 'POST' });
    loadTagRulesEngine();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteTagRuleItem(id) {
  try {
    await fetch('/api/tag-rules/' + id, { method: 'DELETE' });
    toast('Rule deleted', 'success');
    loadTagRulesEngine();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function runAllTagRules() {
  try {
    const res = await fetch('/api/tag-rules/run', { method: 'POST' }).then(safeJSON);
    toast('Applied ' + res.totalApplied + ' tags across ' + res.results.length + ' rules', 'success');
    loadTagRulesEngine();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Nurture Cadence (Batch 26) ===
async function loadNurtureCadence() {
  try {
    const [leads, analytics] = await Promise.all([
      fetch('/api/nurture/cadence?limit=30').then(safeJSON),
      fetch('/api/nurture/analytics').then(safeJSON)
    ]);
    const aEl = document.getElementById('cadence-analytics');
    const statusColors = { never: '#dc2626', dormant: '#9ca3af', cold: '#3b82f6', cooling: '#f59e0b', warm: '#22c55e', active: '#16a34a' };
    if (analytics.gapAnalysis) {
      aEl.innerHTML = analytics.gapAnalysis.map(g => `<div style="text-align:center;"><div style="font-size:1rem;font-weight:700;color:${statusColors[g.cadenceStatus] || '#6b7280'};">${g.count}</div><div style="font-size:0.65rem;color:#78716c;">${g.cadenceStatus}</div></div>`).join('');
    }
    const el = document.getElementById('nurture-cadence-list');
    if (!leads.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No leads to nurture.</div>'; return; }
    el.innerHTML = leads.slice(0, 20).map(l => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #f3e8ff;">
      <div style="flex:1;"><span style="font-size:0.75rem;font-weight:500;">${l.first_name} ${l.last_name}</span> <span style="font-size:0.65rem;color:#78716c;">${l.city || ''}</span></div>
      <span style="background:${statusColors[l.cadenceStatus] || '#9ca3af'};color:#fff;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;">${l.cadenceStatus}</span>
      <span style="font-size:0.65rem;color:#78716c;margin-left:0.3rem;">${l.daysSinceContact != null ? l.daysSinceContact + 'd ago' : 'never'}</span>
    </div>`).join('');
  } catch (err) { console.error('Nurture cadence error:', err); }
}

// === Custom Fields (Batch 26) ===
async function loadCustomFields() {
  try {
    const stats = await fetch('/api/custom-fields/stats').then(safeJSON);
    const el = document.getElementById('custom-fields-list');
    if (!stats.length) { el.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No custom fields defined. Create one above.</div>'; return; }
    el.innerHTML = stats.map(f => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.3rem 0;border-bottom:1px solid #ccfbf1;">
      <div>
        <span style="font-size:0.75rem;font-weight:500;">${f.field_name}</span>
        <span style="background:#ccfbf1;padding:0.05rem 0.3rem;border-radius:3px;font-size:0.6rem;margin-left:0.3rem;">${f.field_type}</span>
        ${f.required ? '<span style="color:#dc2626;font-size:0.6rem;margin-left:0.2rem;">required</span>' : ''}
        ${f.default_value ? '<span style="font-size:0.6rem;color:#78716c;margin-left:0.2rem;">default: ' + f.default_value + '</span>' : ''}
      </div>
      <div style="display:flex;align-items:center;gap:0.3rem;">
        <span style="font-size:0.65rem;color:#0d9488;font-weight:500;">${f.filledCount || 0} filled</span>
        <button onclick="deleteCustomFieldItem('${f.field_name}')" style="font-size:0.62rem;padding:0.1rem 0.3rem;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;border-radius:3px;cursor:pointer;">Del</button>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Custom fields error:', err); }
}
async function createCustomFieldFromUI() {
  const fieldName = document.getElementById('cf-name').value.trim();
  const fieldType = document.getElementById('cf-type').value;
  const defaultValue = document.getElementById('cf-default').value.trim() || null;
  if (!fieldName) { toast('Field name required', 'error'); return; }
  try {
    await fetch('/api/custom-fields', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ fieldName, fieldType, defaultValue }) });
    document.getElementById('cf-name').value = '';
    document.getElementById('cf-default').value = '';
    toast('Custom field created', 'success');
    loadCustomFields();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}
async function deleteCustomFieldItem(name) {
  try {
    await fetch('/api/custom-fields/' + encodeURIComponent(name), { method: 'DELETE' });
    toast('Field deleted', 'success');
    loadCustomFields();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Score Decay Config (Batch 26) ===
async function loadScoreDecayConfig() {
  try {
    const [config, preview] = await Promise.all([
      fetch('/api/score-decay/config').then(safeJSON),
      fetch('/api/score-decay/preview?limit=10').then(safeJSON)
    ]);
    const cEl = document.getElementById('decay-config-display');
    cEl.innerHTML = `<div style="display:flex;gap:1rem;flex-wrap:wrap;font-size:0.72rem;">
      <div><strong>Status:</strong> <span style="color:${config.enabled ? '#16a34a' : '#dc2626'};">${config.enabled ? 'Enabled' : 'Disabled'}</span></div>
      <div><strong>Type:</strong> ${config.decayType || 'linear'}</div>
      <div><strong>Rate:</strong> ${config.decayRate || 0}${config.decayType === 'exponential' ? '%' : ' pts'}</div>
      <div><strong>Interval:</strong> ${config.decayIntervalDays || 30} days</div>
      <div><strong>Min Score:</strong> ${config.minScore || 0}</div>
      ${config.exemptStages ? '<div><strong>Exempt:</strong> ' + config.exemptStages + '</div>' : ''}
    </div>`;
    const pEl = document.getElementById('decay-preview-list');
    if (!preview.length) { pEl.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No leads eligible for decay.</div>'; return; }
    pEl.innerHTML = preview.map(p => `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #fef9c3;">
      <span style="font-size:0.75rem;">${p.first_name} ${p.last_name}</span>
      <div style="display:flex;gap:0.5rem;align-items:center;">
        <span style="font-size:0.7rem;color:#78716c;">Score: ${p.currentScore}</span>
        <span style="font-size:0.7rem;color:#b45309;">→ ${p.projectedScore}</span>
        <span style="font-size:0.6rem;color:#9ca3af;">${p.daysSinceUpdate || '?'}d idle</span>
      </div>
    </div>`).join('');
  } catch (err) { console.error('Score decay error:', err); }
}
async function runScoreDecayNow() {
  try {
    const res = await fetch('/api/score-decay/run', { method: 'POST' }).then(safeJSON);
    toast('Decayed ' + res.affected + ' leads', 'success');
    loadScoreDecayConfig();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Leaderboard (Batch 19) ===
async function loadLeaderboard() {
  try {
    const metric = document.getElementById('leaderboard-metric')?.value || 'lead_score';
    const leads = await fetch('/api/leads/leaderboard?limit=15&metric=' + metric).then(safeJSON);
    const el = document.getElementById('leaderboard-list');

    if (leads.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#b45309;">No leads to rank.</span>';
    } else {
      el.innerHTML = leads.map((l, i) => {
        const medal = i === 0 ? '&#x1F947;' : i === 1 ? '&#x1F948;' : i === 2 ? '&#x1F949;' : `<span style="color:#9ca3af;font-size:0.65rem;">#${i+1}</span>`;
        return `
          <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fef3c7;font-size:0.72rem;">
            <span style="width:24px;text-align:center;">${medal}</span>
            <strong style="flex:1;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</strong>
            <span style="color:#6b7280;font-size:0.65rem;">${esc(l.city || '')}, ${esc(l.state || '')}</span>
            <span style="background:#fef3c7;color:#b45309;padding:0.1rem 0.3rem;border-radius:4px;font-size:0.65rem;font-weight:600;">${l.lead_score}</span>
            <span style="color:#059669;font-size:0.6rem;">${l.completeness}%</span>
          </div>
        `;
      }).join('');
    }

    // State leaderboard
    const states = await fetch('/api/leads/leaderboard-by-state?limit=8').then(safeJSON);
    document.getElementById('leaderboard-states').innerHTML = states.map(s => `
      <span style="display:inline-flex;align-items:center;gap:0.2rem;padding:0.15rem 0.4rem;background:#fff;border:1px solid #fbbf24;border-radius:6px;font-size:0.65rem;">
        <strong>${esc(s.state)}</strong>
        <span style="color:#b45309;">avg ${s.avg_score}</span>
        <span style="color:#6b7280;">${fmt(s.total)}</span>
      </span>
    `).join('');
  } catch (err) { console.error('Leaderboard error:', err); }
}

// === Automation Rules (Batch 19) ===
async function loadAutomationRules() {
  try {
    const rules = await fetch('/api/automation-rules').then(safeJSON);
    const el = document.getElementById('automation-rules-list');
    if (rules.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#059669;">No automation rules. Create rules to automate lead management.</span>';
      return;
    }
    el.innerHTML = rules.map(r => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.25rem 0.6rem;background:#fff;border:1px solid ${r.enabled ? '#34d399' : '#d1d5db'};border-radius:6px;font-size:0.72rem;">
        <span style="width:8px;height:8px;border-radius:50%;background:${r.enabled ? '#22c55e' : '#9ca3af'};flex-shrink:0;"></span>
        <strong style="flex:1;">${esc(r.name)}</strong>
        <span style="font-size:0.65rem;color:#6b7280;">on ${esc(r.trigger_event)}</span>
        <span style="font-size:0.65rem;color:#059669;">${r.action_type}: ${esc(r.action_value)}</span>
        <span style="font-size:0.6rem;color:#9ca3af;">${fmt(r.run_count)} runs</span>
        <button onclick="toggleAutomation(${r.id})" style="border:none;background:none;cursor:pointer;color:#059669;font-size:0.65rem;padding:0;">[${r.enabled ? 'disable' : 'enable'}]</button>
        <button onclick="deleteAutomation(${r.id})" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.7rem;padding:0;">&times;</button>
      </div>
    `).join('');
  } catch (err) { console.error('Automation error:', err); }
}

function showCreateAutomationModal() {
  const modal = document.createElement('div');
  modal.id = 'automation-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:440px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#059669;">Create Automation Rule</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Rule Name</label>
        <input id="auto-name" type="text" placeholder="e.g., Tag high-score FL leads" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem;margin-bottom:0.75rem;">
        <div>
          <label style="font-size:0.78rem;font-weight:600;">Trigger Event</label>
          <select id="auto-trigger" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.78rem;">
            <option value="lead_added">Lead Added</option>
            <option value="lead_updated">Lead Updated</option>
            <option value="score_changed">Score Changed</option>
            <option value="email_found">Email Found</option>
          </select>
        </div>
        <div>
          <label style="font-size:0.78rem;font-weight:600;">Action</label>
          <select id="auto-action" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.78rem;">
            <option value="tag">Add Tag</option>
            <option value="move_stage">Move to Stage</option>
            <option value="assign_owner">Assign Owner</option>
          </select>
        </div>
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Action Value</label>
        <input id="auto-value" type="text" placeholder="e.g., priority, qualified, John" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('automation-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createAutomationFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#059669;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createAutomationFromModal() {
  const name = document.getElementById('auto-name').value.trim();
  if (!name) { alert('Name required'); return; }
  await fetch('/api/automation-rules', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      triggerEvent: document.getElementById('auto-trigger').value,
      actionType: document.getElementById('auto-action').value,
      actionValue: document.getElementById('auto-value').value.trim(),
      conditions: {},
    }),
  });
  document.getElementById('automation-modal').remove();
  toast('Automation rule created', 'success');
  loadAutomationRules();
}

async function toggleAutomation(id) {
  await fetch('/api/automation-rules/' + id + '/toggle', { method: 'POST' });
  loadAutomationRules();
}

async function deleteAutomation(id) {
  await fetch('/api/automation-rules/' + id, { method: 'DELETE' });
  toast('Rule deleted', 'success');
  loadAutomationRules();
}

// === Data Quality (Batch 19) ===
async function loadDataQuality() {
  try {
    const [summary, leads] = await Promise.all([
      fetch('/api/leads/data-quality-summary').then(safeJSON),
      fetch('/api/leads/data-quality?limit=15').then(safeJSON),
    ]);

    const sumEl = document.getElementById('quality-summary');
    sumEl.innerHTML = [
      { label: 'Complete', val: summary.completeRate + '%', color: summary.completeRate >= 50 ? '#16a34a' : '#dc2626' },
      { label: 'Missing Email', val: fmt(summary.missingEmail), color: '#dc2626' },
      { label: 'Missing Phone', val: fmt(summary.missingPhone), color: '#f59e0b' },
      { label: 'Missing Website', val: fmt(summary.missingWebsite), color: '#6b7280' },
    ].map(c => `
      <span style="font-size:0.72rem;padding:0.15rem 0.5rem;background:#fff;border:1px solid #fca5a5;border-radius:6px;">
        <span style="color:${c.color};font-weight:700;">${c.val}</span> ${c.label}
      </span>
    `).join('');

    const leadsEl = document.getElementById('quality-leads');
    leadsEl.innerHTML = leads.map(l => `
      <div style="display:flex;align-items:center;gap:0.4rem;padding:0.2rem 0;border-bottom:1px solid #fee2e2;font-size:0.7rem;">
        <span style="width:32px;text-align:center;font-weight:700;color:${l.qualityScore >= 70 ? '#16a34a' : l.qualityScore >= 40 ? '#f59e0b' : '#dc2626'};">${l.qualityScore}%</span>
        <strong style="flex:1;">${esc(l.name)}</strong>
        <span style="color:#6b7280;font-size:0.65rem;">${esc(l.state || '')}</span>
        ${l.missing.slice(0, 3).map(m => `<span style="font-size:0.6rem;padding:0.05rem 0.3rem;background:#fee2e2;color:#dc2626;border-radius:3px;">${m}</span>`).join('')}
        ${l.suggestions.length ? `<span style="font-size:0.6rem;color:#059669;" title="${esc(l.suggestions[0].action)}">💡</span>` : ''}
      </div>
    `).join('');
  } catch (err) { console.error('Quality error:', err); }
}

async function loadAiDataFixes() {
  const el = document.getElementById('quality-ai-fixes');
  if (!el) return;
  el.style.display = 'block';
  el.innerHTML = '<div style="font-size:0.72rem;color:#dc2626;">AI analyzing data quality issues...</div>';
  try {
    const res = await fetch('/api/ai/data-fixes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    let html = `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.3rem;">
      <span style="font-size:0.68rem;font-weight:600;color:#dc2626;">AI DATA FIX SUGGESTIONS</span>
      <span style="font-size:0.62rem;color:#6b7280;">${d.issues_found || 0} issues, ${d.auto_fixable || 0} auto-fixable</span>
    </div>`;
    const fixes = d.fixes || [];
    if (fixes.length > 0) {
      fixes.slice(0, 10).forEach(fix => {
        const confColor = fix.confidence === 'high' ? '#22c55e' : fix.confidence === 'medium' ? '#f59e0b' : '#94a3b8';
        html += `<div style="display:flex;align-items:center;gap:0.3rem;padding:0.15rem 0;border-bottom:1px solid #fee2e2;font-size:0.65rem;">
          <span style="width:6px;height:6px;border-radius:50%;background:${confColor};flex-shrink:0;"></span>
          <span style="color:#374151;min-width:0;">
            <strong>Lead #${fix.lead_id}</strong> ${esc(fix.field)}:
            <span style="color:#dc2626;text-decoration:line-through;">${esc((fix.current_value || '').substring(0, 30))}</span>
            &#x2192; <span style="color:#059669;">${esc((fix.suggested_fix || '').substring(0, 40))}</span>
          </span>
          <span style="flex-shrink:0;font-size:0.55rem;padding:0.05rem 0.2rem;border-radius:3px;background:${confColor};color:#fff;">${esc(fix.confidence || '')}</span>
        </div>`;
      });
    }
    if (d.patterns && d.patterns.length > 0) {
      html += '<div style="font-size:0.62rem;font-weight:600;color:#dc2626;margin-top:0.3rem;">PATTERNS</div>';
      html += d.patterns.map(p => `<div style="font-size:0.62rem;color:#6b7280;padding:0.05rem 0;">&#x2022; ${esc(p)}</div>`).join('');
    }
    el.innerHTML = html;
  } catch (err) { el.innerHTML = `<div style="color:#dc2626;font-size:0.72rem;">Error: ${esc(err.message)}</div>`; }
}

// === Export Profiles (Batch 19) ===
async function loadExportProfiles() {
  try {
    const profiles = await fetch('/api/export-profiles').then(safeJSON);
    const el = document.getElementById('export-profiles-list');
    if (profiles.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#2563eb;">No export profiles. Save filter+column presets for quick exports.</span>';
      return;
    }
    el.innerHTML = profiles.map(p => `
      <span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.2rem 0.6rem;background:#fff;border:1px solid #93c5fd;border-radius:8px;font-size:0.72rem;">
        <strong>${esc(p.name)}</strong>
        <span style="color:#6b7280;font-size:0.65rem;">${fmt(p.export_count)} exports</span>
        <button onclick="runProfile(${p.id})" style="border:none;background:none;cursor:pointer;color:#2563eb;font-size:0.65rem;padding:0;">[run]</button>
        <button onclick="deleteProfile(${p.id})" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.7rem;padding:0;">&times;</button>
      </span>
    `).join('');
  } catch (err) { console.error('Export profiles error:', err); }
}

function showCreateExportProfileModal() {
  const modal = document.createElement('div');
  modal.id = 'export-profile-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:440px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#2563eb;">Create Export Profile</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Profile Name</label>
        <input id="ep-name" type="text" placeholder="e.g., FL Immigration w/ Email" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem;margin-bottom:0.75rem;">
        <div><label style="font-size:0.72rem;">State</label><input id="ep-state" type="text" placeholder="FL" style="width:100%;padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;"></div>
        <div><label style="font-size:0.72rem;">Min Score</label><input id="ep-score" type="number" placeholder="0" style="width:100%;padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;"></div>
        <div><label style="font-size:0.72rem;">Practice Area</label><input id="ep-practice" type="text" placeholder="" style="width:100%;padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;"></div>
        <div><label style="font-size:0.72rem;display:flex;align-items:center;gap:0.3rem;margin-top:1rem;"><input type="checkbox" id="ep-email-only"> Email only</label></div>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('export-profile-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createProfileFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#2563eb;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createProfileFromModal() {
  const name = document.getElementById('ep-name').value.trim();
  if (!name) { alert('Name required'); return; }
  const filters = {};
  const state = document.getElementById('ep-state').value.trim();
  if (state) filters.state = state;
  const score = document.getElementById('ep-score').value;
  if (score) filters.minScore = parseInt(score);
  const practice = document.getElementById('ep-practice').value.trim();
  if (practice) filters.practiceArea = practice;
  if (document.getElementById('ep-email-only').checked) filters.hasEmail = true;

  await fetch('/api/export-profiles', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, filters }),
  });
  document.getElementById('export-profile-modal').remove();
  toast('Export profile created', 'success');
  loadExportProfiles();
}

async function runProfile(id) {
  try {
    const res = await fetch('/api/export-profiles/' + id + '/run').then(safeJSON);
    toast('Export profile: ' + fmt(res.count) + ' leads matching', 'info');
    if (res.leads) { _currentLeads = res.leads; renderLeadTable(); }
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

async function deleteProfile(id) {
  await fetch('/api/export-profiles/' + id, { method: 'DELETE' });
  toast('Export profile deleted', 'success');
  loadExportProfiles();
}

// === Lead Import (Batch 18) ===
async function runImport() {
  const fileInput = document.getElementById('import-file');
  const resultEl = document.getElementById('import-result');
  if (!fileInput.files.length) { toast('Select a CSV file first', 'warn'); return; }

  resultEl.innerHTML = '<span style="color:#a16207;">Parsing CSV...</span>';
  const file = fileInput.files[0];
  const text = await file.text();
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) { resultEl.innerHTML = '<span style="color:#dc2626;">CSV is empty or has no data rows.</span>'; return; }

  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const leads = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = lines[i].split(',').map(v => v.trim().replace(/^"|"$/g, ''));
    const lead = {};
    headers.forEach((h, idx) => { lead[h] = vals[idx] || ''; });
    leads.push(lead);
  }

  resultEl.innerHTML = `<span style="color:#a16207;">Importing ${leads.length} leads...</span>`;
  const options = {
    dedup: document.getElementById('import-dedup').checked,
    overwriteEmpty: document.getElementById('import-fill').checked,
    source: 'csv-import',
  };

  try {
    const res = await fetch('/api/leads/import', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leads, options }),
    }).then(safeJSON);

    resultEl.innerHTML = `
      <span style="color:#16a34a;font-weight:600;">${fmt(res.added)} added</span>,
      <span style="color:#2563eb;">${fmt(res.updated)} updated</span>,
      <span style="color:#6b7280;">${fmt(res.skipped)} skipped</span>
      ${res.errors?.length ? `<span style="color:#dc2626;"> (${res.errors.length} errors)</span>` : ''}
    `;
    if (res.added > 0 || res.updated > 0) refreshAll();
  } catch (err) {
    resultEl.innerHTML = '<span style="color:#dc2626;">Import failed: ' + esc(err.message) + '</span>';
  }
}

// === Engagement Heatmap (Batch 18) ===
async function loadEngagementHeatmap() {
  try {
    const data = await fetch('/api/leads/engagement-heatmap').then(safeJSON);
    const gridEl = document.getElementById('engagement-heatmap-grid');
    const actionsEl = document.getElementById('heatmap-actions');
    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    if (!data.grid || data.grid.every(row => row.every(v => v === 0))) {
      gridEl.innerHTML = '<span style="font-size:0.72rem;color:#be185d;">No engagement data yet. Track activities to see the heatmap.</span>';
      actionsEl.innerHTML = '';
      return;
    }

    const maxVal = Math.max(...data.grid.flat(), 1);
    let html = '<table style="border-collapse:collapse;font-size:0.6rem;"><tr><td></td>';
    for (let h = 0; h < 24; h++) html += `<td style="text-align:center;color:#9ca3af;padding:0 1px;">${h}</td>`;
    html += '</tr>';
    for (let d = 0; d < 7; d++) {
      html += `<tr><td style="color:#9ca3af;padding:0 4px 0 0;text-align:right;">${days[d]}</td>`;
      for (let h = 0; h < 24; h++) {
        const val = data.grid[d][h];
        const intensity = val / maxVal;
        const bg = val === 0 ? '#fce7f3' : `rgba(190, 24, 93, ${0.15 + intensity * 0.85})`;
        const color = intensity > 0.5 ? '#fff' : '#be185d';
        html += `<td style="width:14px;height:14px;background:${bg};color:${color};text-align:center;border:1px solid #fdf2f8;border-radius:2px;font-size:0.55rem;">${val || ''}</td>`;
      }
      html += '</tr>';
    }
    html += '</table>';
    gridEl.innerHTML = html;

    actionsEl.innerHTML = data.topActions.map(a => `
      <span style="font-size:0.65rem;padding:0.1rem 0.4rem;background:#fff;border:1px solid #f9a8d4;border-radius:4px;">${esc(a.action)}: <strong>${fmt(a.count)}</strong></span>
    `).join('');
  } catch (err) { console.error('Heatmap error:', err); }
}

// === Owners (Batch 18) ===
async function loadOwners() {
  try {
    const owners = await fetch('/api/owners').then(safeJSON);
    const el = document.getElementById('owners-list');
    if (owners.length === 0) {
      el.innerHTML = '<span style="font-size:0.72rem;color:#0369a1;">No owners assigned. Use bulk actions to assign leads to team members.</span>';
      return;
    }
    el.innerHTML = owners.map(o => `
      <span onclick="viewOwnerLeads('${esc(o.owner)}')" style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.2rem 0.6rem;background:#fff;border:1px solid #7dd3fc;border-radius:8px;font-size:0.72rem;cursor:pointer;">
        <strong>${esc(o.owner)}</strong>
        <span style="color:#0369a1;">${fmt(o.lead_count)} leads</span>
        <span style="color:#6b7280;font-size:0.65rem;">avg ${o.avg_score}</span>
      </span>
    `).join('');
  } catch (err) { console.error('Owners error:', err); }
}

async function viewOwnerLeads(owner) {
  try {
    const leads = await fetch('/api/owners/' + encodeURIComponent(owner) + '/leads?limit=50').then(safeJSON);
    toast(owner + ' has ' + leads.length + ' leads', 'info');
    _currentLeads = leads;
    renderLeadTable();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Lead Comparison (Batch 18) ===
async function compareSelectedLeads() {
  const ids = [..._selectedIds];
  if (ids.length < 2) { toast('Select at least 2 leads to compare', 'warn'); return; }
  if (ids.length > 5) { toast('Maximum 5 leads for comparison', 'warn'); return; }

  try {
    const data = await fetch('/api/leads/compare', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: ids }),
    }).then(safeJSON);

    const el = document.getElementById('comparison-result');
    if (!data.leads || data.leads.length === 0) {
      el.innerHTML = '<span style="color:#dc2626;">No leads found.</span>';
      return;
    }

    let html = '<table style="width:100%;border-collapse:collapse;font-size:0.7rem;">';
    html += '<tr style="border-bottom:1px solid #e9d5ff;"><th style="text-align:left;padding:2px 4px;color:#7c3aed;">Field</th>';
    for (const lead of data.leads) {
      html += `<th style="text-align:left;padding:2px 4px;color:#7c3aed;">ID ${lead.id}: ${esc(lead.first_name || '')} ${esc(lead.last_name || '')}</th>`;
    }
    html += '</tr>';

    for (const field of data.fields) {
      if (!field.hasData) continue;
      const bgColor = field.hasDifference ? '#faf5ff' : '#fff';
      html += `<tr style="background:${bgColor};border-bottom:1px solid #f3e8ff;">`;
      html += `<td style="padding:2px 4px;font-weight:600;color:#6b21a8;">${esc(field.name)}</td>`;
      for (const v of field.values) {
        const val = v.value || '<em style="color:#d1d5db;">empty</em>';
        html += `<td style="padding:2px 4px;">${typeof val === 'string' && val.startsWith('<') ? val : esc(String(val))}</td>`;
      }
      html += '</tr>';
    }
    html += '</table>';

    // Merge button
    html += `<div style="margin-top:0.5rem;display:flex;gap:0.3rem;">`;
    for (const lead of data.leads) {
      html += `<button onclick="mergeIntoLead(${lead.id}, [${ids.filter(i => i !== lead.id).join(',')}])" style="font-size:0.68rem;padding:0.2rem 0.5rem;background:#7c3aed;color:#fff;border:none;border-radius:4px;cursor:pointer;">Merge into ID ${lead.id}</button>`;
    }
    html += `</div>`;

    el.innerHTML = html;
  } catch (err) { toast('Error comparing leads: ' + err.message, 'error'); }
}

async function mergeIntoLead(targetId, sourceIds) {
  if (!confirm('Merge leads ' + sourceIds.join(', ') + ' into lead ' + targetId + '? Source leads will be deleted.')) return;
  try {
    const res = await fetch('/api/leads/merge-with-picks', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetId, sourceIds, fieldPicks: {} }),
    }).then(safeJSON);
    toast('Merged ' + res.merged + ' leads into ID ' + targetId, 'success');
    document.getElementById('comparison-result').innerHTML = '';
    refreshAll();
  } catch (err) { toast('Merge error: ' + err.message, 'error'); }
}

