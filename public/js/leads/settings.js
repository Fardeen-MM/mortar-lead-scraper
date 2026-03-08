// settings.js — Settings tab: scoring rules, webhooks, timeline, tags, compare/merge, import
// Dependencies: utils.js, app.js

// --- Scoring Rules ---
let _scoringVisible = false;

function toggleScoringPanel() {
  _scoringVisible = !_scoringVisible;
  document.getElementById('scoring-rules-body').style.display = _scoringVisible ? '' : 'none';
  document.getElementById('scoring-toggle').innerHTML = _scoringVisible ? '&#x25BC;' : '&#x25B6;';
  if (_scoringVisible) loadScoringRules();
}

async function loadScoringRules() {
  try {
    const rules = await fetch('/api/scoring/rules').then(safeJSON);
    const container = document.getElementById('scoring-rules-body');
    container.innerHTML = rules.map(r => `
      <div style="display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0;border-bottom:1px solid #f3f4f6;">
        <label style="cursor:pointer;"><input type="checkbox" ${r.enabled ? 'checked' : ''} onchange="toggleScoringRule(${r.id}, this.checked)"></label>
        <span style="font-size:0.8rem;font-weight:600;min-width:100px;">${r.field}</span>
        <span style="font-size:0.75rem;color:#6b7280;">${r.condition}</span>
        <input type="number" value="${r.points}" onchange="updateScoringPoints(${r.id}, this.value)" style="width:50px;padding:0.2rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.8rem;text-align:center;" min="0" max="100">
        <span style="font-size:0.7rem;color:#9ca3af;">pts</span>
        <button onclick="deleteScoringRuleUI(${r.id})" style="background:none;border:none;color:#ef4444;cursor:pointer;margin-left:auto;">&times;</button>
      </div>
    `).join('') +
    `<div style="margin-top:0.5rem;display:flex;gap:0.5rem;">
      <button onclick="addScoringRuleUI()" style="font-size:0.72rem;padding:0.25rem 0.6rem;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:4px;cursor:pointer;">+ Add Rule</button>
      <button onclick="rescoreAll()" style="font-size:0.72rem;padding:0.25rem 0.6rem;background:#4f46e5;color:#fff;border:none;border-radius:4px;cursor:pointer;">Re-score All</button>
    </div>`;
  } catch (err) { console.error('Scoring rules load error:', err); }
}

async function toggleScoringRule(id, enabled) {
  await fetch('/api/scoring/rules/' + id, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled }) });
}

async function updateScoringPoints(id, points) {
  await fetch('/api/scoring/rules/' + id, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ points: parseInt(points) }) });
}

async function deleteScoringRuleUI(id) {
  await fetch('/api/scoring/rules/' + id, { method: 'DELETE' });
  loadScoringRules();
}

async function addScoringRuleUI() {
  const field = prompt('Field name (e.g., linkedin_url, title, bio):');
  if (!field) return;
  const points = parseInt(prompt('Points (0-100):', '10'));
  if (isNaN(points)) return;
  await fetch('/api/scoring/rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ field, points, condition: 'is_not_empty' }) });
  loadScoringRules();
}

async function rescoreAll() {
  try {
    const result = await fetch('/api/leads/score', { method: 'POST' }).then(safeJSON);
    toast(`Scored ${result.scored} leads (avg: ${result.avgScore})`, 'success');
    loadScores();
    loadLeads();
  } catch (err) { toast('Scoring failed', 'error'); }
}

async function quickChangeStage(leadId, stage) {
  try {
    await fetch('/api/pipeline/move', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId, stage })
    });
    toast(`Stage changed to ${stage}`, 'success');
    loadPipeline();
  } catch (err) { toast('Failed to change stage', 'error'); }
}

// --- Webhooks ---
async function loadWebhooks() {
  try {
    const hooks = await fetch('/api/webhooks').then(safeJSON);
    const container = document.getElementById('webhooks-list');
    if (hooks.length === 0) {
      container.innerHTML = '<span style="font-size:0.8rem;color:#9ca3af;">No webhooks configured.</span>';
      return;
    }
    container.innerHTML = hooks.map(h => {
      const evtBadges = h.events.map(e => `<span style="background:#ede9fe;color:#5b21b6;font-size:0.65rem;padding:0.1rem 0.3rem;border-radius:3px;">${esc(e)}</span>`).join(' ');
      return `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.5rem 0;border-bottom:1px solid #f3f4f6;">
        <div>
          <div style="font-size:0.8rem;font-weight:600;word-break:break-all;">${esc(h.url)}</div>
          <div style="display:flex;gap:0.3rem;margin-top:0.2rem;flex-wrap:wrap;">${evtBadges}</div>
        </div>
        <div style="display:flex;gap:0.4rem;align-items:center;">
          <label style="cursor:pointer;"><input type="checkbox" ${h.enabled ? 'checked' : ''} onchange="toggleWebhook(${h.id}, this.checked)"></label>
          <button onclick="testWebhookUI(${h.id}, '${esc(h.url)}')" style="background:none;border:1px solid #d1d5db;border-radius:4px;padding:0.15rem 0.4rem;font-size:0.7rem;cursor:pointer;">Test</button>
          <button onclick="deleteWebhookUI(${h.id})" style="background:none;border:none;color:#ef4444;cursor:pointer;">&times;</button>
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Webhooks load error:', err); }
}

function openWebhookBuilder() {
  document.getElementById('webhook-modal').style.display = 'flex';
  document.getElementById('wh-url').value = '';
  document.getElementById('wh-secret').value = '';
  document.querySelectorAll('.wh-event').forEach(c => c.checked = false);
}
function closeWebhookModal() { document.getElementById('webhook-modal').style.display = 'none'; }

async function saveWebhook() {
  const url = document.getElementById('wh-url').value.trim();
  if (!url) return toast('URL required', 'error');
  const events = Array.from(document.querySelectorAll('.wh-event:checked')).map(c => c.value);
  if (events.length === 0) return toast('Select at least one event', 'error');
  const secret = document.getElementById('wh-secret').value.trim();
  try {
    await fetch('/api/webhooks', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, events, secret })
    });
    closeWebhookModal();
    toast('Webhook created', 'success');
    loadWebhooks();
  } catch (err) { toast('Failed to create webhook', 'error'); }
}

async function toggleWebhook(id, enabled) {
  await fetch('/api/webhooks/' + id, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled })
  });
}

async function testWebhookUI(id, url) {
  try {
    await fetch('/api/webhooks/test', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url })
    });
    toast('Test webhook sent', 'success');
  } catch (err) { toast('Test failed', 'error'); }
}

async function deleteWebhookUI(id) {
  if (!confirm('Delete this webhook?')) return;
  await fetch('/api/webhooks/' + id, { method: 'DELETE' });
  toast('Webhook deleted', 'success');
  loadWebhooks();
}

// --- Lead Notes & Timeline ---
async function loadLeadTimeline(leadId, container) {
  try {
    const timeline = await fetch('/api/leads/' + leadId + '/timeline').then(safeJSON);
    if (timeline.length === 0) {
      container.innerHTML = '<div style="color:#9ca3af;font-size:0.8rem;">No activity yet.</div>';
      return;
    }
    container.innerHTML = timeline.map(item => {
      const time = timeAgo(new Date(item.created_at));
      if (item.type === 'note') {
        return `<div style="padding:0.4rem 0;border-bottom:1px solid #f3f4f6;">
          <div style="display:flex;justify-content:space-between;align-items:start;">
            <div style="font-size:0.8rem;">&#x1F4DD; <strong>${esc(item.author)}</strong> added a note</div>
            <span style="font-size:0.7rem;color:#9ca3af;">${time}</span>
          </div>
          <div style="font-size:0.8rem;color:#374151;margin-top:0.2rem;padding-left:1.2rem;">${esc(item.content)}</div>
        </div>`;
      } else {
        const icons = { update: '&#x270F;', stage_change: '&#x1F4CB;', bulk_update: '&#x1F4E6;', note_added: '&#x1F4DD;', create: '&#x2795;' };
        const icon = icons[item.action] || '&#x1F504;';
        return `<div style="padding:0.3rem 0;border-bottom:1px solid #f9fafb;font-size:0.75rem;color:#6b7280;">
          ${icon} <strong>${item.field || item.action}</strong> ${item.old_value ? esc(item.old_value) + ' → ' : ''}${item.new_value ? esc(item.new_value) : ''} <span style="color:#9ca3af;">${time}</span>
        </div>`;
      }
    }).join('');
  } catch (err) { container.innerHTML = '<div style="color:#ef4444;font-size:0.8rem;">Failed to load timeline</div>'; }
}

async function loadContactTimeline(leadId) {
  const container = document.getElementById('modal-timeline');
  if (!container) return;
  container.innerHTML = '<div class="skeleton skeleton-line" style="width:80%;"></div><div class="skeleton skeleton-line" style="width:60%;"></div>';
  try {
    const contacts = await fetch('/api/leads/' + leadId + '/contacts?limit=20').then(safeJSON);
    if (!contacts || contacts.length === 0) {
      container.innerHTML = '<div style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:0.5rem;">No contacts logged yet. Click "+ Log Contact" to record outreach.</div>';
      return;
    }
    const channelIcons = { email: '\u{2709}\uFE0F', phone: '\u{1F4DE}', linkedin: '\u{1F517}', meeting: '\u{1F91D}', sms: '\u{1F4AC}', other: '\u{1F4CB}' };
    const outcomeColors = { replied: '#22c55e', interested: '#22c55e', meeting_set: '#7c3aed', not_interested: '#ef4444', no_answer: '#f59e0b', voicemail: '#f59e0b' };
    container.innerHTML = '<div style="position:relative;padding-left:16px;">' +
      '<div style="position:absolute;left:5px;top:0;bottom:0;width:2px;background:#e5e7eb;"></div>' +
      contacts.map(c => {
        const icon = channelIcons[c.channel] || '\u{1F4CB}';
        const dir = c.direction === 'inbound' ? '\u{2B05}\uFE0F' : '\u{27A1}\uFE0F';
        const time = typeof timeAgo === 'function' ? timeAgo(c.contact_at) : new Date(c.contact_at).toLocaleDateString();
        const outColor = outcomeColors[c.outcome] || '#6b7280';
        return `<div style="position:relative;padding:0.25rem 0 0.25rem 0.5rem;border-bottom:1px solid #f9fafb;">
          <div style="position:absolute;left:-7px;top:8px;width:10px;height:10px;border-radius:50%;background:${c.direction === 'inbound' ? '#3b82f6' : '#6366f1'};border:2px solid #fff;"></div>
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="font-size:0.72rem;">${icon} ${dir} ${esc(c.channel)} ${c.subject ? '— ' + esc(c.subject) : ''}</span>
            <span style="font-size:0.62rem;color:#9ca3af;">${time}</span>
          </div>
          ${c.outcome ? '<span style="font-size:0.6rem;padding:0.05rem 0.3rem;border-radius:8px;background:' + outColor + '20;color:' + outColor + ';font-weight:600;">' + esc(c.outcome.replace(/_/g, ' ')) + '</span>' : ''}
          ${c.notes ? '<div style="font-size:0.68rem;color:#6b7280;margin-top:0.1rem;padding-left:0.3rem;">' + esc(c.notes.substring(0, 150)) + '</div>' : ''}
        </div>`;
      }).join('') + '</div>';
  } catch (err) {
    container.innerHTML = '<div style="font-size:0.72rem;color:#ef4444;">Failed to load contacts</div>';
  }
}

async function saveContactLog(leadId) {
  const channel = document.getElementById('log-channel').value;
  const direction = document.getElementById('log-direction').value;
  const outcome = document.getElementById('log-outcome').value;
  const subject = document.getElementById('log-subject').value.trim();
  const notes = document.getElementById('log-notes').value.trim();
  try {
    await fetch('/api/leads/' + leadId + '/contacts', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel, direction, subject, notes, outcome }),
    });
    toast('Contact logged', 'success');
    document.getElementById('log-contact-form').style.display = 'none';
    document.getElementById('log-subject').value = '';
    document.getElementById('log-notes').value = '';
    loadContactTimeline(leadId);
    addNotification('\u{1F4DE}', `Contact logged for lead #${leadId}`, 'success');
  } catch (err) { toast('Failed to log: ' + err.message, 'error'); }
}

async function addNoteToLead(leadId) {
  const content = prompt('Add a note:');
  if (!content) return;
  try {
    await fetch('/api/leads/' + leadId + '/notes', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content })
    });
    toast('Note added', 'success');
    // Reload timeline if modal is open
    const timelineEl = document.getElementById('lead-timeline');
    if (timelineEl) loadLeadTimeline(leadId, timelineEl);
  } catch (err) { toast('Failed to add note', 'error'); }
}

// --- Bulk Edit ---
function openBulkEditModal() {
  const count = _selectedLeads.size;
  if (count === 0) return toast('No leads selected', 'info');
  document.getElementById('bulk-edit-modal').style.display = 'flex';
  document.getElementById('bulk-edit-count').textContent = `(${count} leads)`;
  document.getElementById('be-stage').value = '';
  document.getElementById('be-tags').value = '';
  document.getElementById('be-practice').value = '';
  document.getElementById('be-title').value = '';
}
function closeBulkEditModal() { document.getElementById('bulk-edit-modal').style.display = 'none'; }

async function applyBulkEdit() {
  const ids = Array.from(_selectedLeads);
  const updates = {};
  const stage = document.getElementById('be-stage').value;
  const tags = document.getElementById('be-tags').value.trim();
  const practice = document.getElementById('be-practice').value.trim();
  const title = document.getElementById('be-title').value.trim();
  if (stage) updates.pipeline_stage = stage;
  if (tags) updates.tags = tags;
  if (practice) updates.practice_area = practice;
  if (title) updates.title = title;
  if (Object.keys(updates).length === 0) return toast('No changes to apply', 'info');
  try {
    const result = await fetch('/api/leads/bulk-update', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: ids, updates })
    }).then(safeJSON);
    closeBulkEditModal();
    toast(`Updated ${result.updated} leads`, 'success');
    clearSelection();
    loadLeads();
    loadPipeline();
  } catch (err) { toast('Bulk edit failed', 'error'); }
}

// --- Enrichment Queue ---
async function loadEnrichmentQueue() {
  try {
    const [stats, wsStatus, emailStatus, enrichStatus, waterfallStatus] = await Promise.all([
      fetch('/api/leads/enrichment-stats').then(safeJSON),
      fetch('/api/leads/find-websites/status').then(safeJSON).catch(() => null),
      fetch('/api/leads/verify-status').then(safeJSON).catch(() => null),
      fetch('/api/leads/enrich-all/status').then(safeJSON).catch(() => null),
      fetch('/api/leads/waterfall/status').then(safeJSON).catch(() => null),
    ]);
    const container = document.getElementById('enrichment-queue');
    const jobs = [];

    if (wsStatus && wsStatus.status === 'running') {
      jobs.push({ name: 'Find Websites', status: 'running', progress: wsStatus.processed || 0, total: wsStatus.total || 0 });
    }
    if (emailStatus && emailStatus.status === 'running') {
      jobs.push({ name: 'Find Emails', status: 'running', progress: emailStatus.processed || 0, total: emailStatus.total || 0 });
    }
    if (enrichStatus && enrichStatus.status === 'running') {
      jobs.push({ name: 'Enrich All', status: 'running', progress: enrichStatus.processed || 0, total: enrichStatus.total || 0 });
    }
    if (waterfallStatus && waterfallStatus.running) {
      const wp = waterfallStatus.progress || {};
      jobs.push({ name: 'Waterfall Enrich', status: 'running', progress: wp.current || 0, total: wp.total || 0 });
    }

    if (jobs.length === 0) {
      container.innerHTML = '<span style="font-size:0.8rem;color:#9ca3af;">No enrichment jobs running.</span>';
      document.getElementById('eq-status').textContent = 'Idle';
      document.getElementById('eq-status').style.background = '#dcfce7';
      document.getElementById('eq-status').style.color = '#166534';
    } else {
      document.getElementById('eq-status').textContent = `${jobs.length} Running`;
      document.getElementById('eq-status').style.background = '#dbeafe';
      document.getElementById('eq-status').style.color = '#1e40af';
      container.innerHTML = jobs.map(j => {
        const pct = j.total > 0 ? Math.round(100 * j.progress / j.total) : 0;
        return `<div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:0.5rem 0.75rem;">
          <div style="display:flex;justify-content:space-between;margin-bottom:0.3rem;">
            <span style="font-weight:600;font-size:0.82rem;">${j.name}</span>
            <span style="font-size:0.75rem;color:#6b7280;">${j.progress}/${j.total} (${pct}%)</span>
          </div>
          <div style="height:4px;background:#e5e7eb;border-radius:2px;overflow:hidden;">
            <div style="height:100%;width:${pct}%;background:linear-gradient(90deg,#6366f1,#8b5cf6);border-radius:2px;transition:width 0.5s;"></div>
          </div>
        </div>`;
      }).join('');
    }

    // Enrichment opportunities
    const opps = [];
    if (stats.opportunities) {
      if (stats.opportunities.needsEmail > 0) opps.push({ action: 'Find Emails', count: stats.opportunities.needsEmail, btn: 'verifyEmails()' });
      if (stats.opportunities.needsPhone > 0) opps.push({ action: 'Find Phones', count: stats.opportunities.needsPhone, btn: null });
      if (stats.opportunities.hasWebsiteNoEmail > 0) opps.push({ action: 'Crawl Websites', count: stats.opportunities.hasWebsiteNoEmail, btn: 'startEmailVerify()' });
    }
    const oppContainer = document.getElementById('eq-opportunities');
    if (opps.length > 0) {
      oppContainer.innerHTML = '<div style="font-size:0.8rem;font-weight:600;margin-bottom:0.3rem;color:#4338ca;">Opportunities</div>' +
        opps.map(o => `<div style="display:flex;justify-content:space-between;align-items:center;padding:0.3rem 0;border-bottom:1px solid #f3f4f6;">
          <span style="font-size:0.8rem;">${o.action}: <strong>${o.count}</strong> leads</span>
          ${o.btn ? `<button onclick="${o.btn}" style="font-size:0.7rem;padding:0.15rem 0.5rem;background:#4f46e5;color:#fff;border:none;border-radius:4px;cursor:pointer;">Run</button>` : ''}
        </div>`).join('');
    } else {
      oppContainer.innerHTML = '';
    }
  } catch (err) { console.error('Enrichment queue error:', err); }
}

function startEmailVerify() {
  fetch('/api/leads/verify-emails', { method: 'POST' })
    .then(r => { if (!r.ok) throw new Error('Server error'); return r.json(); })
    .then(() => { toast('Email verification started', 'info'); setTimeout(loadEnrichmentQueue, 2000); })
    .catch(err => toast('Email verification failed: ' + err.message, 'error'));
}
function verifyEmails() {
  fetch('/api/leads/verify-emails', { method: 'POST' })
    .then(r => { if (!r.ok) throw new Error('Server error'); return r.json(); })
    .then(() => { toast('Finding emails from firm websites...', 'info'); setTimeout(loadEnrichmentQueue, 2000); })
    .catch(err => toast('Email finding failed: ' + err.message, 'error'));
}

// --- Scheduled Scrapes ---
async function loadSchedules() {
  try {
    const schedules = await fetch('/api/schedules').then(safeJSON);
    const container = document.getElementById('schedules-list');
    if (schedules.length === 0) {
      container.innerHTML = '<span style="font-size:0.8rem;color:#9ca3af;">No scheduled scrapes. Add one to auto-refresh lead data.</span>';
      return;
    }
    const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    container.innerHTML = schedules.map(s => {
      const lastRun = s.last_run_at ? timeAgo(new Date(s.last_run_at)) : 'Never';
      const nextRun = s.next_run_at ? new Date(s.next_run_at).toLocaleDateString() + ' ' + new Date(s.next_run_at).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '—';
      return `<div style="display:flex;align-items:center;justify-content:space-between;padding:0.5rem 0.75rem;background:#fff;border:1px solid #e5e7eb;border-radius:8px;">
        <div style="display:flex;align-items:center;gap:0.75rem;">
          <label style="cursor:pointer;display:flex;align-items:center;">
            <input type="checkbox" ${s.enabled ? 'checked' : ''} onchange="toggleSchedule(${s.id}, this.checked)" style="cursor:pointer;">
          </label>
          <div>
            <div style="font-weight:600;font-size:0.85rem;">${esc(s.state)}</div>
            <div style="font-size:0.75rem;color:#6b7280;">${s.frequency} &middot; ${DAYS[s.day_of_week]} ${s.hour}:00 UTC</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:1rem;">
          <div style="text-align:right;">
            <div style="font-size:0.7rem;color:#9ca3af;">Last: ${lastRun}</div>
            <div style="font-size:0.7rem;color:#6b7280;">Next: ${nextRun}</div>
          </div>
          <button onclick="deleteScheduleUI(${s.id})" style="background:none;border:none;color:#ef4444;cursor:pointer;font-size:0.9rem;" title="Delete">&times;</button>
        </div>
      </div>`;
    }).join('');
  } catch (err) { console.error('Schedules load error:', err); }
}

function openScheduleBuilder() {
  const modal = document.getElementById('schedule-modal');
  modal.style.display = 'flex';
  // Populate state dropdown from existing config
  const sel = document.getElementById('sched-state');
  if (sel.options.length <= 1) {
    fetch('/api/config').then(safeJSON).then(data => {
      sel.innerHTML = data.scrapers.filter(s => s.working).map(s => `<option value="${s.stateCode}">${s.stateCode} — ${s.name}</option>`).join('');
    }).catch(err => console.warn('schedule config:', err.message));
  }
}

function closeScheduleModal() {
  document.getElementById('schedule-modal').style.display = 'none';
}

async function saveSchedule() {
  const state = document.getElementById('sched-state').value;
  const frequency = document.getElementById('sched-freq').value;
  const dayOfWeek = parseInt(document.getElementById('sched-dow').value);
  const hour = parseInt(document.getElementById('sched-hour').value);
  try {
    await fetch('/api/schedules', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state, frequency, dayOfWeek, hour })
    });
    closeScheduleModal();
    toast(`Scheduled ${state} scrape (${frequency})`, 'success');
    loadSchedules();
  } catch (err) { toast('Failed to create schedule', 'error'); }
}

async function toggleSchedule(id, enabled) {
  await fetch('/api/schedules/' + id, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: enabled ? 1 : 0 })
  });
  toast(enabled ? 'Schedule enabled' : 'Schedule paused', 'info');
}

async function deleteScheduleUI(id) {
  if (!confirm('Delete this schedule?')) return;
  await fetch('/api/schedules/' + id, { method: 'DELETE' });
  toast('Schedule deleted', 'success');
  loadSchedules();
}

// === Tag Management (Clay-style) ===
let _tagColor = '#6366f1';

async function loadTagDefinitions() {
  try {
    const tags = await fetch('/api/tag-definitions').then(safeJSON);
    const container = document.getElementById('tags-list');
    if (tags.length === 0) {
      container.innerHTML = '<span style="font-size:0.78rem;color:#9ca3af;">No tags defined yet. Create tags to organize leads.</span>';
      return;
    }
    container.innerHTML = tags.map(t => `
      <span class="tag-chip" style="background:${t.color}20;color:${t.color};border:1px solid ${t.color}40;" onclick="filterByTag('${esc(t.name)}')">
        ${esc(t.name)} <span class="tag-count">${fmt(t.lead_count)}</span>
        <span class="tag-delete" onclick="event.stopPropagation();deleteTagDef(${t.id},'${esc(t.name)}')">&times;</span>
      </span>
    `).join('');
  } catch (err) { console.error('Tag defs error:', err); }
}

function openTagBuilder() {
  document.getElementById('tag-modal').style.display = 'flex';
  document.getElementById('tag-name').focus();
}
function closeTagModal() { document.getElementById('tag-modal').style.display = 'none'; }

function pickTagColor(el) {
  document.querySelectorAll('.tag-color-opt').forEach(e => e.classList.remove('selected'));
  el.classList.add('selected');
  _tagColor = el.dataset.color;
}

async function saveTag() {
  const name = document.getElementById('tag-name').value.trim();
  if (!name) { toast('Tag name required', 'error'); return; }
  const color = _tagColor;
  const description = document.getElementById('tag-desc').value.trim();
  const ruleField = document.getElementById('tag-def-field').value;
  const autoRule = ruleField ? {
    field: ruleField,
    operator: document.getElementById('tag-def-op').value,
    value: document.getElementById('tag-rule-val').value || '1',
  } : undefined;
  try {
    await fetch('/api/tag-definitions', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, color, description, autoRule }),
    });
    closeTagModal();
    toast('Tag created: ' + name, 'success');
    document.getElementById('tag-name').value = '';
    document.getElementById('tag-desc').value = '';
    loadTagDefinitions();
  } catch (err) { toast('Failed to create tag', 'error'); }
}

async function deleteTagDef(id, name) {
  if (!confirm('Delete tag "' + name + '"?')) return;
  await fetch('/api/tag-definitions/' + id, { method: 'DELETE' });
  toast('Tag deleted', 'success');
  loadTagDefinitions();
}

function filterByTag(tag) {
  document.getElementById('search-input').value = 'tag:' + tag;
  loadLeads();
}

async function runAutoTagging() {
  toast('Running auto-tagging rules...', 'info');
  const result = await fetch('/api/leads/auto-tag', { method: 'POST' }).then(safeJSON);
  toast('Auto-tagged ' + fmt(result.tagged) + ' leads across ' + result.rules + ' rules', 'success');
  loadTagDefinitions();
}

// === Change Detection / Signals ===
async function loadChangeSignals() {
  try {
    const [changes, firmChanges] = await Promise.all([
      fetch('/api/leads/changes?limit=20').then(safeJSON),
      fetch('/api/leads/firm-changes?limit=10').then(safeJSON),
    ]);
    const panel = document.getElementById('changes-panel');
    const allChanges = [...firmChanges.map(f => ({ ...f, _type: 'firm' })), ...changes.map(c => ({ ...c, _type: 'field' }))];
    if (allChanges.length === 0) { panel.style.display = 'none'; return; }
    panel.style.display = '';
    document.getElementById('changes-badge').textContent = allChanges.length + ' changes';
    const html = allChanges.slice(0, 15).map(c => {
      if (c._type === 'firm') {
        return `<div style="padding:0.4rem 0;border-bottom:1px solid #f3f4f6;font-size:0.78rem;">
          <span style="color:#dc2626;font-weight:600;">Firm Change</span>
          <strong>${esc(c.first_name)} ${esc(c.last_name)}</strong> (${esc(c.state)})
          moved from <em>${esc(c.previous_firm)}</em> to <em>${esc(c.firm_name)}</em>
        </div>`;
      }
      return `<div style="padding:0.4rem 0;border-bottom:1px solid #f3f4f6;font-size:0.78rem;">
        <span style="color:#f59e0b;font-weight:600;">${esc(c.field_name)}</span>
        <strong>${esc(c.first_name)} ${esc(c.last_name)}</strong> (${esc(c.state)}):
        <span style="text-decoration:line-through;color:#9ca3af;">${esc(c.old_value || '')}</span> &rarr; ${esc(c.new_value || '')}
      </div>`;
    }).join('');
    document.getElementById('changes-list').innerHTML = html;
  } catch (err) { console.error('Changes error:', err); }
}

// === Lead Comparison & Merge Tool ===
let _compareChoices = {};

async function openCompareModal(id1, id2) {
  _compareChoices = {};
  document.getElementById('compare-modal').style.display = 'flex';
  const content = document.getElementById('compare-content');
  content.innerHTML = '<p style="color:#6b7280;">Loading comparison...</p>';
  try {
    const data = await fetch('/api/leads/compare?id1=' + id1 + '&id2=' + id2).then(safeJSON);
    if (data.error) { content.innerHTML = '<p style="color:#ef4444;">' + esc(data.error) + '</p>'; return; }

    let html = `
      <div style="display:flex;gap:1rem;margin-bottom:1rem;">
        <div style="flex:1;text-align:center;padding:0.5rem;background:#f0fdf4;border-radius:8px;">
          <div style="font-weight:700;">${esc(data.lead1.name)}</div>
          <div style="font-size:0.75rem;color:#6b7280;">ID: ${data.lead1.id} | Score: ${data.lead1.score}</div>
          <div style="font-size:0.7rem;color:#9ca3af;">Sources: ${data.lead1.sources.join(', ') || 'none'}</div>
        </div>
        <div style="display:flex;align-items:center;font-size:0.9rem;font-weight:600;color:#6b7280;">vs</div>
        <div style="flex:1;text-align:center;padding:0.5rem;background:#eff6ff;border-radius:8px;">
          <div style="font-weight:700;">${esc(data.lead2.name)}</div>
          <div style="font-size:0.75rem;color:#6b7280;">ID: ${data.lead2.id} | Score: ${data.lead2.score}</div>
          <div style="font-size:0.7rem;color:#9ca3af;">Sources: ${data.lead2.sources.join(', ') || 'none'}</div>
        </div>
      </div>
      <div style="display:flex;gap:0.5rem;margin-bottom:0.75rem;font-size:0.78rem;">
        <span style="padding:0.2rem 0.5rem;background:#f0fdf4;border:1px solid #86efac;border-radius:4px;">${data.matchPercentage}% match</span>
        <span style="padding:0.2rem 0.5rem;background:#fef2f2;border:1px solid #fca5a5;border-radius:4px;">${data.conflicts} conflicts</span>
        <span style="padding:0.2rem 0.5rem;background:#fffbeb;border:1px solid #fde68a;border-radius:4px;">${data.complementary} complementary</span>
      </div>
      <table class="compare-table">
        <thead><tr><th>Field</th><th>Lead A</th><th></th><th>Lead B</th></tr></thead>
        <tbody>
    `;

    for (const row of data.comparison) {
      const cls = row.match ? 'match' : row.conflict ? 'conflict' : (row.onlyIn1 || row.onlyIn2) ? 'only-one' : '';
      html += `<tr class="${cls}">
        <th>${esc(row.field)}</th>
        <td>
          ${row.value1 ? esc(row.value1) : '<span style="color:#d1d5db;">—</span>'}
          ${row.conflict ? '<button class="pick-btn" onclick="pickMergeField(\'' + row.field + '\',1,this)">Use A</button>' : ''}
        </td>
        <td style="text-align:center;">
          ${row.match ? '<span style="color:#22c55e;">&#x2714;</span>' : row.conflict ? '<span style="color:#ef4444;">&#x2716;</span>' : ''}
        </td>
        <td>
          ${row.value2 ? esc(row.value2) : '<span style="color:#d1d5db;">—</span>'}
          ${row.conflict ? '<button class="pick-btn" onclick="pickMergeField(\'' + row.field + '\',2,this)">Use B</button>' : ''}
        </td>
      </tr>`;
    }

    html += `</tbody></table>
      <div style="display:flex;gap:0.5rem;margin-top:1rem;">
        <button onclick="executeMerge(${data.lead1.id},${data.lead2.id})" style="flex:1;padding:0.5rem;background:#22c55e;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;">Merge (Keep A, merge B into A)</button>
        <button onclick="executeMerge(${data.lead2.id},${data.lead1.id})" style="flex:1;padding:0.5rem;background:#3b82f6;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;">Merge (Keep B, merge A into B)</button>
      </div>`;

    content.innerHTML = html;
  } catch (err) { content.innerHTML = '<p style="color:#ef4444;">Error: ' + esc(err.message) + '</p>'; }
}

function closeCompareModal() { document.getElementById('compare-modal').style.display = 'none'; }

function pickMergeField(field, choice, btn) {
  _compareChoices[field] = choice;
  // Visual feedback
  const row = btn.closest('tr');
  row.querySelectorAll('.pick-btn').forEach(b => b.classList.remove('picked'));
  btn.classList.add('picked');
}

async function executeMerge(keepId, mergeId) {
  if (!confirm('Merge lead #' + mergeId + ' into #' + keepId + '? The merged lead will be deleted.')) return;
  try {
    const result = await fetch('/api/leads/merge-with-choices', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keepId, mergeId, fieldChoices: _compareChoices }),
    }).then(safeJSON);
    if (result.success) {
      toast('Leads merged successfully!', 'success');
      closeCompareModal();
      refreshAll();
    } else {
      toast('Merge failed: ' + (result.error || 'Unknown error'), 'error');
    }
  } catch (err) { toast('Merge error: ' + err.message, 'error'); }
}

// === Enhanced Import with Preview ===
let _importFile = null;
let _importMapping = {};

async function handleImportWithPreview(file) {
  if (!file.name.endsWith('.csv')) { alert('Please select a CSV file'); return; }
  _importFile = file;

  // Get preview
  const formData = new FormData();
  formData.append('file', file);

  try {
    const preview = await fetch('/api/leads/import-preview', { method: 'POST', body: formData }).then(safeJSON);
    if (preview.error) { toast('Preview failed: ' + preview.error, 'error'); return; }

    _importMapping = { ...preview.mapping };
    const modal = document.getElementById('import-modal');
    modal.style.display = 'flex';

    const availFields = preview.availableFields;
    let html = `
      <div style="margin-bottom:1rem;">
        <div style="font-size:0.85rem;font-weight:600;margin-bottom:0.3rem;">File: ${esc(file.name)}</div>
        <div style="font-size:0.78rem;color:#6b7280;">${preview.totalHeaders} columns detected, ${preview.mappedCount} auto-mapped</div>
      </div>
      <div style="font-size:0.82rem;font-weight:600;margin-bottom:0.5rem;">Column Mapping</div>
      <div style="display:flex;flex-direction:column;gap:0.3rem;margin-bottom:1rem;">
    `;

    for (const header of preview.headers) {
      const mappedTo = Object.entries(preview.mapping).find(([k, v]) => v === header);
      const currentField = mappedTo ? mappedTo[0] : '';
      html += `<div style="display:flex;align-items:center;gap:0.5rem;">
        <span style="flex:1;font-size:0.78rem;font-weight:500;padding:0.3rem 0.5rem;background:#f3f4f6;border-radius:4px;">${esc(header)}</span>
        <span style="font-size:0.75rem;color:#9ca3af;">&rarr;</span>
        <select class="import-map-sel" data-header="${esc(header)}" onchange="updateImportMapping(this)" style="flex:1;padding:0.3rem;font-size:0.78rem;border:1px solid #d1d5db;border-radius:4px;">
          <option value="">-- Skip --</option>
          ${availFields.map(f => `<option value="${f}" ${currentField === f ? 'selected' : ''}>${f}</option>`).join('')}
          <option value="_full_name" ${currentField === '_full_name' ? 'selected' : ''}>Full Name (split)</option>
        </select>
      </div>`;
    }

    html += `</div>`;

    // Preview table
    if (preview.sampleRows.length > 0) {
      html += `<div style="font-size:0.82rem;font-weight:600;margin-bottom:0.3rem;">Preview (first ${preview.sampleRows.length} rows)</div>`;
      html += `<div style="overflow-x:auto;margin-bottom:1rem;"><table style="width:100%;font-size:0.72rem;border-collapse:collapse;">`;
      html += '<thead><tr>' + preview.headers.map(h => `<th style="padding:0.3rem;border:1px solid #e5e7eb;background:#f9fafb;white-space:nowrap;">${esc(h)}</th>`).join('') + '</tr></thead><tbody>';
      for (const row of preview.sampleRows) {
        html += '<tr>' + preview.headers.map(h => `<td style="padding:0.3rem;border:1px solid #e5e7eb;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(row[h] || '')}</td>`).join('') + '</tr>';
      }
      html += '</tbody></table></div>';
    }

    html += `<button onclick="executeImport()" style="width:100%;padding:0.5rem;background:#4f46e5;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;">Import ${esc(file.name)}</button>`;

    document.getElementById('import-preview-content').innerHTML = html;
  } catch (err) { toast('Preview error: ' + err.message, 'error'); }
}

function closeImportModal() { document.getElementById('import-modal').style.display = 'none'; }

function updateImportMapping(sel) {
  const header = sel.dataset.header;
  const field = sel.value;
  // Remove this header from any existing mapping
  for (const [k, v] of Object.entries(_importMapping)) {
    if (v === header) delete _importMapping[k];
  }
  if (field) _importMapping[field] = header;
}

async function executeImport() {
  if (!_importFile) return;
  const resultEl = document.getElementById('csv-import-result');
  resultEl.style.display = '';
  resultEl.innerHTML = 'Importing...';
  closeImportModal();

  const formData = new FormData();
  formData.append('file', _importFile);
  formData.append('source', 'csv-import');

  try {
    const res = await fetch('/api/leads/import/csv', { method: 'POST', body: formData });
    const d = await safeJSON(res);
    if (d.error) {
      resultEl.innerHTML = '<span style="color:#dc2626;">Import failed: ' + esc(d.error) + '</span>';
      return;
    }
    resultEl.innerHTML = '<strong>Import complete</strong> | ' + fmt(d.total) + ' rows processed | ' + fmt(d.imported) + ' new | ' + fmt(d.updated) + ' updated | ' + fmt(d.skipped) + ' skipped';
    toast('Imported ' + fmt(d.imported) + ' new leads', 'success');
    refreshAll();
  } catch (err) {
    resultEl.innerHTML = '<span style="color:#dc2626;">Import error: ' + esc(err.message) + '</span>';
  }
}
