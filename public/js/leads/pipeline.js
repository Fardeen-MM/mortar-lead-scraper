// pipeline.js — Pipeline kanban board, smart segments, export templates
// Dependencies: utils.js, app.js

// --- Pipeline Kanban Board ---
const STAGE_META = {
  new: { label: 'New', color: '#6366f1', icon: '&#x2728;' },
  contacted: { label: 'Contacted', color: '#0ea5e9', icon: '&#x1F4E7;' },
  replied: { label: 'Replied', color: '#f59e0b', icon: '&#x1F4AC;' },
  meeting: { label: 'Meeting', color: '#8b5cf6', icon: '&#x1F4C5;' },
  client: { label: 'Client', color: '#10b981', icon: '&#x2705;' },
};
let _pipelineExpanded = false;

async function loadPipeline() {
  try {
    const data = await fetch('/api/pipeline/stats').then(safeJSON);
    const container = document.getElementById('pipeline-columns');
    document.getElementById('pipeline-total').textContent = `${data.total} leads in pipeline`;
    let html = '';
    for (const stage of data.stageOrder) {
      const meta = STAGE_META[stage];
      const s = data.stages[stage];
      const pct = data.total > 0 ? ((s.count / data.total) * 100).toFixed(0) : 0;
      html += `<div class="pipeline-col" data-stage="${stage}" style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:0.75rem;min-height:80px;border-top:3px solid ${meta.color};"
        ondragover="event.preventDefault();this.style.background='#f0f4ff'" ondragleave="this.style.background='#fff'"
        ondrop="handlePipelineDrop(event,'${stage}');this.style.background='#fff'">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
          <span style="font-weight:600;font-size:0.85rem;">${meta.icon} ${meta.label}</span>
          <span style="background:${meta.color}15;color:${meta.color};font-size:0.75rem;padding:0.1rem 0.4rem;border-radius:10px;font-weight:600;">${s.count}</span>
        </div>
        <div style="font-size:0.75rem;color:#9ca3af;">${pct}% of total</div>
        <div style="font-size:0.7rem;color:#6b7280;margin-top:0.25rem;">${s.withEmail} with email</div>
        <div id="pipeline-cards-${stage}" style="margin-top:0.5rem;display:${_pipelineExpanded ? 'flex' : 'none'};flex-direction:column;gap:0.35rem;"></div>
      </div>`;
    }
    container.innerHTML = html;
    if (_pipelineExpanded) loadPipelineCards();
  } catch (err) { console.error('Pipeline load error:', err); }
}

async function loadPipelineCards() {
  for (const stage of Object.keys(STAGE_META)) {
    try {
      const leads = await fetch(`/api/pipeline/stage/${stage}?limit=10`).then(safeJSON);
      const container = document.getElementById('pipeline-cards-' + stage);
      if (!container) continue;
      container.innerHTML = leads.map(l => `
        <div draggable="true" data-lead-id="${l.id}" ondragstart="event.dataTransfer.setData('text/plain','${l.id}')"
          style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:0.4rem 0.5rem;cursor:grab;font-size:0.75rem;"
          onclick="openLeadById(${l.id})">
          <div style="font-weight:600;">${esc(l.first_name)} ${esc(l.last_name)}</div>
          <div style="color:#6b7280;">${esc(l.firm_name || '')} &middot; ${esc(l.city || '')}</div>
          ${l.email ? '<div style="color:#059669;">&#x2709; has email</div>' : ''}
        </div>
      `).join('');
    } catch (err) { /* skip */ }
  }
}

function togglePipelineExpand() {
  _pipelineExpanded = !_pipelineExpanded;
  loadPipeline();
}

async function handlePipelineDrop(event, stage) {
  event.preventDefault();
  const leadId = event.dataTransfer.getData('text/plain');
  if (!leadId) return;
  try {
    await fetch('/api/pipeline/move', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId: parseInt(leadId), stage })
    });
    toast(`Moved to ${STAGE_META[stage].label}`, 'success');
    loadPipeline();
  } catch (err) { toast('Failed to move lead', 'error'); }
}

async function moveSelectedToStage() {
  const stages = Object.entries(STAGE_META).map(([k, v]) => `${v.icon} ${v.label}`);
  const choice = prompt('Move to stage:\\n1. New\\n2. Contacted\\n3. Replied\\n4. Meeting\\n5. Client\\n\\nEnter number:');
  if (!choice) return;
  const stageKeys = Object.keys(STAGE_META);
  const idx = parseInt(choice) - 1;
  if (idx < 0 || idx >= stageKeys.length) return;
  const stage = stageKeys[idx];
  const ids = Array.from(_selectedLeads);
  const btn = document.getElementById('btn-move-stage');
  if (btn) { btn.disabled = true; btn.textContent = 'Moving...'; }
  try {
    await fetch('/api/pipeline/bulk-move', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: ids, stage })
    });
    toast(`Moved ${ids.length} leads to ${STAGE_META[stage].label}`, 'success');
    clearSelection();
    loadPipeline();
  } catch (err) { toast('Failed to move leads', 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Move Stage'; } }
}

// --- Smart Segments ---
let _segments = [];
let _editingSegmentId = null;

async function loadSegments() {
  try {
    _segments = await fetch('/api/segments').then(safeJSON);
    const container = document.getElementById('segments-list');
    if (_segments.length === 0) {
      container.innerHTML = '<span style="font-size:0.8rem;color:#9ca3af;">No segments yet. Create one to save filter combinations.</span>';
      return;
    }
    container.innerHTML = _segments.map(seg => {
      const condText = seg.filters.map(f => `${f.field} ${f.operator} ${f.value || ''}`).join(', ');
      return `<div onclick="applySegment(${seg.id})" style="display:inline-flex;align-items:center;gap:0.4rem;padding:0.35rem 0.75rem;background:${seg.color}15;border:1px solid ${seg.color}40;border-radius:20px;cursor:pointer;font-size:0.8rem;transition:all 0.15s;" onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">
        <span style="width:8px;height:8px;border-radius:50%;background:${seg.color};"></span>
        <strong style="color:${seg.color};">${esc(seg.name)}</strong>
        <span style="color:#6b7280;font-size:0.7rem;" title="${esc(condText)}">${seg.filters.length} rules</span>
        <span onclick="event.stopPropagation();deleteSegmentUI(${seg.id})" style="color:#9ca3af;font-size:0.7rem;cursor:pointer;" title="Delete">&times;</span>
      </div>`;
    }).join('');
  } catch (err) { console.error('Segments load error:', err); }
}

function openSegmentBuilder(segId) {
  _editingSegmentId = segId || null;
  const modal = document.getElementById('segment-modal');
  modal.style.display = 'flex';
  document.getElementById('seg-name').value = '';
  document.getElementById('seg-desc').value = '';
  document.getElementById('seg-conditions').innerHTML = '';
  document.getElementById('seg-preview-count').textContent = '';
  if (segId) {
    const seg = _segments.find(s => s.id === segId);
    if (seg) {
      document.getElementById('seg-name').value = seg.name;
      document.getElementById('seg-desc').value = seg.description || '';
      for (const f of seg.filters) addSegCondition(f);
      return;
    }
  }
  addSegCondition();
}

function closeSegmentModal() {
  document.getElementById('segment-modal').style.display = 'none';
}

const SEG_FIELDS = [
  { value: 'state', label: 'State' },
  { value: 'country', label: 'Country' },
  { value: 'city', label: 'City' },
  { value: 'firm_name', label: 'Firm' },
  { value: 'practice_area', label: 'Practice Area' },
  { value: 'email', label: 'Email' },
  { value: 'phone', label: 'Phone' },
  { value: 'website', label: 'Website' },
  { value: 'tags', label: 'Tags' },
  { value: 'primary_source', label: 'Source' },
  { value: 'pipeline_stage', label: 'Pipeline Stage' },
  { value: 'lead_score', label: 'Score' },
  { value: 'title', label: 'Title' },
  { value: 'bar_status', label: 'Bar Status' },
];
const SEG_OPS = [
  { value: 'equals', label: '=' },
  { value: 'not_equals', label: '!=' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'is_empty', label: 'is empty' },
  { value: 'is_not_empty', label: 'is not empty' },
  { value: 'greater_than', label: '>' },
  { value: 'less_than', label: '<' },
];

function addSegCondition(preset) {
  const container = document.getElementById('seg-conditions');
  const row = document.createElement('div');
  row.style.cssText = 'display:flex;gap:0.4rem;align-items:center;';
  const fieldSel = `<select class="seg-field" style="padding:0.35rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.8rem;" onchange="segOpChanged(this)">
    ${SEG_FIELDS.map(f => `<option value="${f.value}" ${preset && preset.field === f.value ? 'selected' : ''}>${f.label}</option>`).join('')}
  </select>`;
  const opSel = `<select class="seg-op" style="padding:0.35rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.8rem;">
    ${SEG_OPS.map(o => `<option value="${o.value}" ${preset && preset.operator === o.value ? 'selected' : ''}>${o.label}</option>`).join('')}
  </select>`;
  const valInput = `<input type="text" class="seg-val" value="${preset ? esc(preset.value || '') : ''}" placeholder="value" style="flex:1;padding:0.35rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.8rem;">`;
  row.innerHTML = `${fieldSel} ${opSel} ${valInput} <button onclick="this.parentElement.remove()" style="background:none;border:none;color:#ef4444;cursor:pointer;font-size:1rem;">&times;</button>`;
  container.appendChild(row);
}

function segOpChanged(sel) {
  const valInput = sel.parentElement.querySelector('.seg-val');
  const op = sel.parentElement.querySelector('.seg-op').value;
  if (op === 'is_empty' || op === 'is_not_empty') {
    valInput.style.display = 'none';
  } else {
    valInput.style.display = '';
  }
}

function getSegFilters() {
  const rows = document.querySelectorAll('#seg-conditions > div');
  const filters = [];
  for (const row of rows) {
    const field = row.querySelector('.seg-field').value;
    const operator = row.querySelector('.seg-op').value;
    const value = row.querySelector('.seg-val').value;
    filters.push({ field, operator, value });
  }
  return filters;
}

async function previewSegment() {
  const filters = getSegFilters();
  if (filters.length === 0) return;
  try {
    const data = await fetch('/api/segments/query', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filters })
    }).then(safeJSON);
    document.getElementById('seg-preview-count').textContent = `${data.count} leads match`;
  } catch (err) { toast('Preview failed', 'error'); }
}

async function saveSegment() {
  const name = document.getElementById('seg-name').value.trim();
  if (!name) return toast('Name required', 'error');
  const filters = getSegFilters();
  if (filters.length === 0) return toast('Add at least one condition', 'error');
  const description = document.getElementById('seg-desc').value.trim();
  try {
    if (_editingSegmentId) {
      await fetch('/api/segments/' + _editingSegmentId, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, description, filters })
      });
    } else {
      await fetch('/api/segments', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, description, filters })
      });
    }
    closeSegmentModal();
    toast('Segment saved', 'success');
    loadSegments();
  } catch (err) { toast('Failed to save segment', 'error'); }
}

async function applySegment(segId) {
  const seg = _segments.find(s => s.id === segId);
  if (!seg) return;
  try {
    const leads = await fetch('/api/segments/query/leads', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filters: seg.filters, limit: 200 })
    }).then(safeJSON);
    _allLeads = leads;
    _currentPage = 1;
    renderLeadsTable();
    toast(`Segment "${seg.name}": ${leads.length} leads`, 'info');
  } catch (err) { toast('Failed to apply segment', 'error'); }
}

async function deleteSegmentUI(segId) {
  if (!confirm('Delete this segment?')) return;
  await fetch('/api/segments/' + segId, { method: 'DELETE' });
  toast('Segment deleted', 'success');
  loadSegments();
}

// --- Export Templates ---
async function loadExportTemplates() {
  try {
    const templates = await fetch('/api/export-templates').then(safeJSON);
    const container = document.getElementById('export-templates-list');
    const colors = ['#4f46e5', '#059669', '#0ea5e9', '#d97706', '#7c3aed', '#ec4899'];
    container.innerHTML = templates.map((t, i) => `
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:0.6rem;border-left:3px solid ${colors[i % colors.length]};">
        <div style="font-weight:600;font-size:0.82rem;margin-bottom:0.3rem;">${esc(t.name)}</div>
        <div style="font-size:0.7rem;color:#6b7280;margin-bottom:0.4rem;">${t.columns.length} columns</div>
        <div style="display:flex;gap:0.3rem;">
          <button onclick="exportWithTemplate(${t.id})" style="font-size:0.68rem;padding:0.15rem 0.4rem;background:${colors[i % colors.length]};color:#fff;border:none;border-radius:3px;cursor:pointer;">Export</button>
          <button onclick="deleteTemplateUI(${t.id})" style="font-size:0.68rem;padding:0.15rem 0.4rem;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:3px;cursor:pointer;">&times;</button>
        </div>
      </div>
    `).join('');
  } catch (err) { console.error('Templates load error:', err); }
}

function exportWithTemplate(templateId) {
  const form = document.createElement('form');
  form.method = 'POST';
  form.action = '/api/export-templates/' + templateId + '/export';
  document.body.appendChild(form);
  form.submit();
  document.body.removeChild(form);
  toast('Downloading...', 'success');
}

async function deleteTemplateUI(id) {
  if (!confirm('Delete this template?')) return;
  await fetch('/api/export-templates/' + id, { method: 'DELETE' });
  toast('Template deleted', 'success');
  loadExportTemplates();
}

function openTemplateBuilder() {
  const name = prompt('Template name:');
  if (!name) return;
  const allCols = ['email','first_name','last_name','firm_name','phone','website','city','state','country','practice_area','title','bar_number','bar_status','lead_score','pipeline_stage','tags','primary_source','linkedin_url','bio','education'];
  const cols = prompt('Columns (comma-separated):\\n\\nAvailable: ' + allCols.join(', '), 'email,first_name,last_name,firm_name,phone,city,state');
  if (!cols) return;
  const columns = cols.split(',').map(c => c.trim()).filter(Boolean);
  fetch('/api/export-templates', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, columns })
  }).then(() => { toast('Template created', 'success'); loadExportTemplates(); })
    .catch(() => toast('Failed', 'error'));
}
