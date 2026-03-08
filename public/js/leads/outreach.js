// outreach.js — Outreach tab: sequences, AI emails, firm directory, engagement, DNC, territories
// Dependencies: utils.js, app.js

// === Outreach Sequences ===
async function loadSequences() {
  try {
    const seqs = await fetch('/api/sequences').then(safeJSON);
    const list = document.getElementById('sequences-list');
    if (seqs.length === 0) {
      list.innerHTML = `<div style="text-align:center;padding:0.8rem;">
        <div style="font-size:1.4rem;margin-bottom:0.3rem;">&#x1F4E7;</div>
        <span style="font-size:0.75rem;color:#a78bfa;">No sequences yet. Create your first outreach sequence.</span>
      </div>`;
      return;
    }
    list.innerHTML = seqs.map(s => {
      const stepCount = s.steps?.length || 0;
      const uniqueSteps = [...new Set((s.steps || []).map(st => st.step_number))].length;
      const channels = [...new Set((s.steps || []).map(st => st.channel))];
      const channelIcons = { email: '\u{2709}\uFE0F', phone: '\u{1F4DE}', linkedin: '\u{1F517}', sms: '\u{1F4AC}' };
      // Mini step dots
      let dots = '';
      for (let i = 0; i < Math.min(uniqueSteps, 7); i++) {
        dots += '<span style="width:8px;height:8px;border-radius:50%;background:#7c3aed;display:inline-block;"></span>';
        if (i < uniqueSteps - 1 && i < 6) dots += '<span style="width:12px;height:1px;background:#d8b4fe;display:inline-block;vertical-align:middle;"></span>';
      }
      if (uniqueSteps > 7) dots += '<span style="font-size:0.6rem;color:#9ca3af;">+' + (uniqueSteps - 7) + '</span>';
      return `
      <div style="background:#fff;border:1px solid #e9d5ff;border-radius:8px;padding:0.5rem 0.6rem;cursor:pointer;transition:box-shadow 0.15s;" onclick="showSequenceDetail(${s.id})" onmouseover="this.style.boxShadow='0 2px 8px rgba(124,58,237,0.15)'" onmouseout="this.style.boxShadow='none'">
        <div style="display:flex;align-items:center;justify-content:space-between;">
          <div>
            <strong style="font-size:0.8rem;color:#7c3aed;">${esc(s.name)}</strong>
            <span style="font-size:0.65rem;color:#9ca3af;margin-left:0.3rem;">${channels.map(c => channelIcons[c] || c).join(' ')}</span>
          </div>
          <div style="display:flex;align-items:center;gap:0.4rem;">
            <span style="font-size:0.62rem;padding:0.1rem 0.35rem;border-radius:8px;background:#dcfce7;color:#166534;font-weight:600;">${fmt(s.active)} active</span>
            <span style="font-size:0.62rem;padding:0.1rem 0.35rem;border-radius:8px;background:#ede9fe;color:#7c3aed;font-weight:600;">${fmt(s.enrolled)} total</span>
            <button onclick="event.stopPropagation();deleteSequence(${s.id})" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:none;color:#dc2626;border:1px solid #fecaca;border-radius:3px;cursor:pointer;">&times;</button>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:0;margin-top:0.3rem;">${dots}</div>
        ${s.description ? '<div style="font-size:0.65rem;color:#9ca3af;margin-top:0.15rem;">' + esc(s.description) + '</div>' : ''}
      </div>`;
    }).join('');
  } catch (err) { console.error('Sequences error:', err); }
}

function showCreateSequenceModal() {
  const modal = document.createElement('div');
  modal.id = 'sequence-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:520px;max-width:90vw;max-height:85vh;overflow-y:auto;">
      <h3 style="margin:0 0 0.75rem;font-size:1.1rem;color:#7c3aed;">Create Outreach Sequence</h3>
      <div style="margin-bottom:0.6rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.2rem;">Sequence Name</label>
        <input id="modal-seq-name" type="text" placeholder="e.g., PI Attorney Cold Outreach" style="width:100%;padding:0.35rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.82rem;">
      </div>
      <div style="margin-bottom:0.6rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.2rem;">Description</label>
        <input id="seq-desc" type="text" placeholder="Optional description" style="width:100%;padding:0.35rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.82rem;">
      </div>

      <!-- Quick Start Templates -->
      <div style="background:#f5f3ff;border:1px solid #e9d5ff;border-radius:8px;padding:0.5rem;margin-bottom:0.6rem;">
        <div style="font-size:0.72rem;font-weight:600;color:#7c3aed;margin-bottom:0.3rem;">Quick Start Templates</div>
        <div style="display:flex;gap:0.3rem;flex-wrap:wrap;">
          <button onclick="applySeqTemplate('cold')" class="seq-template-btn" style="font-size:0.68rem;padding:0.2rem 0.5rem;border:1px solid #d8b4fe;border-radius:4px;background:#fff;cursor:pointer;color:#7c3aed;">Cold Outreach (3-step)</button>
          <button onclick="applySeqTemplate('followup')" class="seq-template-btn" style="font-size:0.68rem;padding:0.2rem 0.5rem;border:1px solid #d8b4fe;border-radius:4px;background:#fff;cursor:pointer;color:#7c3aed;">Follow-up (2-step)</button>
          <button onclick="applySeqTemplate('breakup')" class="seq-template-btn" style="font-size:0.68rem;padding:0.2rem 0.5rem;border:1px solid #d8b4fe;border-radius:4px;background:#fff;cursor:pointer;color:#7c3aed;">Break-up (1-step)</button>
        </div>
      </div>

      <h4 style="font-size:0.82rem;margin:0.5rem 0 0.4rem;">Step 1: Initial Email</h4>
      <div style="margin-bottom:0.4rem;">
        <input id="seq-subject-1" type="text" placeholder="Hi {{first_name}}, quick question about {{firm_name}}" style="width:100%;padding:0.3rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.78rem;">
      </div>
      <div style="margin-bottom:0.4rem;">
        <textarea id="seq-body-1" rows="4" placeholder="Hi {{first_name}},\n\nI noticed you're a {{practice_area}} attorney in {{city}}..." style="width:100%;padding:0.3rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.78rem;font-family:inherit;resize:vertical;"></textarea>
      </div>
      <p style="font-size:0.65rem;color:#9ca3af;margin-bottom:0.5rem;">Variables: {{first_name}}, {{last_name}}, {{firm_name}}, {{city}}, {{state}}, {{practice_area}}, {{title}}</p>
      <div style="display:flex;justify-content:flex-end;gap:0.4rem;">
        <button onclick="document.getElementById('sequence-modal').remove()" style="padding:0.35rem 0.7rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;font-size:0.8rem;">Cancel</button>
        <button onclick="createSequence()" style="padding:0.35rem 0.7rem;border:none;border-radius:6px;background:#7c3aed;color:#fff;cursor:pointer;font-size:0.8rem;font-weight:500;">Create Sequence</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

function applySeqTemplate(type) {
  const subj = document.getElementById('seq-subject-1');
  const body = document.getElementById('seq-body-1');
  const name = document.getElementById('modal-seq-name');
  const templates = {
    cold: {
      name: 'Cold Outreach',
      subject: 'Quick question about {{firm_name}}\'s growth',
      body: 'Hi {{first_name}},\n\nI noticed {{firm_name}} focuses on {{practice_area}} in {{city}}. We help firms like yours generate 15-30 qualified consultations per month through targeted digital marketing.\n\nWould you be open to a quick 15-minute chat this week to see if there\'s a fit?\n\nBest,\n[Your Name]',
    },
    followup: {
      name: 'Follow-up',
      subject: 'Re: {{firm_name}} marketing',
      body: 'Hi {{first_name}},\n\nJust following up on my previous email. I know {{practice_area}} attorneys are busy, so I\'ll keep this brief.\n\nWe recently helped a {{practice_area}} firm in {{state}} increase their qualified leads by 40% in 90 days. Happy to share the case study if you\'re interested.\n\nBest,\n[Your Name]',
    },
    breakup: {
      name: 'Break-up',
      subject: 'Should I close your file?',
      body: 'Hi {{first_name}},\n\nI\'ve reached out a few times about helping {{firm_name}} with marketing. I don\'t want to be a pest, so this will be my last email.\n\nIf you ever want to explore how other {{practice_area}} firms are growing their practice, feel free to reach out.\n\nWishing you continued success,\n[Your Name]',
    },
  };
  const t = templates[type];
  if (t) {
    if (!name.value) name.value = t.name;
    subj.value = t.subject;
    body.value = t.body;
  }
}

async function createSequence() {
  const name = document.getElementById('modal-seq-name').value.trim();
  if (!name) { alert('Name required'); return; }
  const desc = document.getElementById('seq-desc').value.trim();
  const subject = document.getElementById('seq-subject-1').value.trim();
  const body = document.getElementById('seq-body-1').value.trim();

  try {
    const result = await fetch('/api/sequences', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, description: desc }),
    }).then(safeJSON);

    if (result.id && (subject || body)) {
      await fetch(`/api/sequences/${result.id}/steps`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stepNumber: 1, channel: 'email', subject, body, delayDays: 0 }),
      });
    }

    document.getElementById('sequence-modal').remove();
    toast('Sequence "' + esc(name) + '" created', 'success');
    loadSequences();
  } catch (err) { toast('Error creating sequence: ' + err.message, 'error'); }
}

async function deleteSequence(id) {
  if (!confirm('Delete this sequence and all its steps?')) return;
  await fetch('/api/sequences/' + id, { method: 'DELETE' });
  toast('Sequence deleted', 'success');
  loadSequences();
}

async function showSequenceDetail(id) {
  try {
    const seqs = await fetch('/api/sequences').then(safeJSON);
    const seq = seqs.find(s => s.id === id);
    if (!seq) return;
    const enrollments = await fetch(`/api/sequences/${id}/enrollments`).then(safeJSON);
    const steps = seq.steps || [];
    // Group steps by step_number to show variants
    const stepGroups = {};
    steps.forEach(st => {
      if (!stepGroups[st.step_number]) stepGroups[st.step_number] = [];
      stepGroups[st.step_number].push(st);
    });
    const stepNums = Object.keys(stepGroups).map(Number).sort((a, b) => a - b);

    const modal = document.createElement('div');
    modal.id = 'sequence-detail-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
    let stepsHtml = '';
    if (stepNums.length === 0) {
      stepsHtml = '<p style="font-size:0.72rem;color:#9ca3af;text-align:center;padding:1rem 0;">No steps yet. Add a step below or let AI generate a full sequence.</p>';
    } else {
      // Visual timeline
      stepsHtml += '<div style="position:relative;padding-left:24px;">';
      stepsHtml += '<div style="position:absolute;left:10px;top:0;bottom:0;width:2px;background:linear-gradient(to bottom,#c4b5fd,#e9d5ff);"></div>';
      stepNums.forEach((num, idx) => {
        const variants = stepGroups[num];
        const delay = variants[0].delay_days;
        const channel = variants[0].channel || 'email';
        const channelIcons = { email: '\u{2709}\uFE0F', phone: '\u{1F4DE}', linkedin: '\u{1F517}', sms: '\u{1F4AC}' };
        stepsHtml += `<div style="position:relative;margin-bottom:0.8rem;">`;
        stepsHtml += `<div style="position:absolute;left:-18px;top:4px;width:14px;height:14px;border-radius:50%;background:#7c3aed;border:2px solid #e9d5ff;z-index:1;"></div>`;
        stepsHtml += `<div style="display:flex;align-items:center;gap:0.4rem;margin-bottom:0.3rem;">`;
        stepsHtml += `<span style="font-size:0.72rem;font-weight:700;color:#7c3aed;">Step ${num}</span>`;
        stepsHtml += `<span style="font-size:0.65rem;padding:0.1rem 0.35rem;border-radius:8px;background:#ede9fe;color:#7c3aed;">${channelIcons[channel] || ''} ${channel}</span>`;
        if (delay > 0 || idx > 0) stepsHtml += `<span style="font-size:0.62rem;color:#9ca3af;">wait ${delay}d</span>`;
        stepsHtml += `</div>`;
        // Variant tabs
        if (variants.length > 1) {
          stepsHtml += `<div style="display:flex;gap:0.2rem;margin-bottom:0.2rem;">`;
          variants.forEach(v => {
            stepsHtml += `<span style="font-size:0.6rem;padding:0.08rem 0.35rem;border-radius:4px;background:${v.variant === 'A' ? '#7c3aed' : v.variant === 'B' ? '#3b82f6' : '#10b981'};color:#fff;font-weight:600;">Variant ${v.variant}</span>`;
          });
          stepsHtml += `</div>`;
        }
        variants.forEach(v => {
          stepsHtml += `<div style="background:#f5f3ff;border:1px solid #e9d5ff;border-radius:6px;padding:0.4rem 0.5rem;margin-bottom:0.25rem;${variants.length > 1 ? 'border-left:3px solid ' + (v.variant === 'A' ? '#7c3aed' : v.variant === 'B' ? '#3b82f6' : '#10b981') : ''}">`;
          if (variants.length > 1) stepsHtml += `<span style="font-size:0.6rem;color:#9ca3af;font-weight:600;">Variant ${v.variant}</span> `;
          stepsHtml += `<div style="font-size:0.76rem;font-weight:500;color:#1e1b4b;">${esc(v.subject || '(no subject)')}</div>`;
          stepsHtml += `<div style="font-size:0.7rem;color:#6b7280;margin-top:0.1rem;white-space:pre-wrap;max-height:60px;overflow-y:auto;">${esc(v.body || '(no body)')}</div>`;
          stepsHtml += `</div>`;
        });
        stepsHtml += `</div>`;
      });
      stepsHtml += '</div>';
    }

    modal.innerHTML = `
      <div style="background:#fff;border-radius:12px;padding:1.5rem;width:680px;max-width:92vw;max-height:85vh;overflow-y:auto;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
          <div>
            <h3 style="margin:0;font-size:1.1rem;color:#7c3aed;">${esc(seq.name)}</h3>
            ${seq.description ? '<p style="font-size:0.72rem;color:#6b7280;margin:0.15rem 0 0;">' + esc(seq.description) + '</p>' : ''}
          </div>
          <div style="display:flex;align-items:center;gap:0.5rem;">
            <span style="font-size:0.68rem;padding:0.15rem 0.5rem;border-radius:10px;background:#ede9fe;color:#7c3aed;font-weight:600;">${stepNums.length} steps</span>
            <span style="font-size:0.68rem;padding:0.15rem 0.5rem;border-radius:10px;background:#dcfce7;color:#166534;font-weight:600;">${fmt(seq.enrolled)} enrolled</span>
            <button onclick="document.getElementById('sequence-detail-modal').remove()" style="border:none;background:none;font-size:1.4rem;cursor:pointer;color:#9ca3af;line-height:1;">&times;</button>
          </div>
        </div>

        <!-- Steps Timeline -->
        <div style="margin:0.75rem 0;">${stepsHtml}</div>

        <!-- AI Generate Variants -->
        <div style="background:linear-gradient(135deg,#eef2ff,#e0e7ff);border:1px solid #c7d2fe;border-radius:8px;padding:0.6rem;margin-bottom:0.75rem;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.3rem;">
            <span style="font-size:0.78rem;font-weight:600;color:#4338ca;">AI Variant Generator</span>
          </div>
          <div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin-bottom:0.3rem;">
            <select id="ai-variant-step" style="padding:0.25rem;border:1px solid #c7d2fe;border-radius:4px;font-size:0.72rem;">
              ${stepNums.length > 0 ? stepNums.map(n => '<option value="' + n + '">Step ' + n + '</option>').join('') : '<option value="1">Step 1 (new)</option>'}
            </select>
            <input id="ai-variant-practice" type="text" placeholder="Practice area..." value="personal injury" style="padding:0.25rem;border:1px solid #c7d2fe;border-radius:4px;font-size:0.72rem;width:120px;">
            <select id="ai-variant-purpose" style="padding:0.25rem;border:1px solid #c7d2fe;border-radius:4px;font-size:0.72rem;">
              <option value="initial cold outreach">Cold outreach</option>
              <option value="follow-up after no reply">Follow-up</option>
              <option value="break-up / last attempt">Break-up</option>
              <option value="value-add / case study share">Value-add</option>
              <option value="meeting request">Meeting request</option>
            </select>
            <button id="ai-gen-variants-btn" onclick="aiGenerateVariants(${id})" style="padding:0.25rem 0.6rem;background:#4f46e5;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:0.72rem;font-weight:500;">Generate 3 Variants</button>
          </div>
          <div id="ai-variant-results"></div>
        </div>

        <!-- Add Step Manually -->
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:0.6rem;margin-bottom:0.75rem;">
          <div style="font-size:0.78rem;font-weight:600;color:#374151;margin-bottom:0.3rem;">Add Step Manually</div>
          <div style="display:grid;grid-template-columns:1fr auto auto;gap:0.3rem;margin-bottom:0.3rem;">
            <input id="new-step-subject" type="text" placeholder="Subject line..." style="padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.74rem;">
            <select id="new-step-channel" style="padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.74rem;">
              <option value="email">Email</option><option value="phone">Phone</option><option value="linkedin">LinkedIn</option><option value="sms">SMS</option>
            </select>
            <select id="new-step-delay" style="padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.74rem;">
              <option value="0">No delay</option><option value="1">1 day</option><option value="2">2 days</option><option value="3">3 days</option><option value="5">5 days</option><option value="7">7 days</option><option value="14">14 days</option>
            </select>
          </div>
          <textarea id="new-step-body" rows="2" placeholder="Email body... Use {{first_name}}, {{firm_name}}, {{city}}, {{practice_area}}" style="width:100%;padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.74rem;font-family:inherit;resize:vertical;margin-bottom:0.3rem;"></textarea>
          <button onclick="addStepToSequence(${id})" style="font-size:0.72rem;padding:0.25rem 0.6rem;background:#7c3aed;color:#fff;border:none;border-radius:4px;cursor:pointer;">Add Step</button>
        </div>

        <!-- Enrollments -->
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:0.6rem;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem;">
            <span style="font-size:0.78rem;font-weight:600;color:#374151;">Enrolled Leads (${enrollments.length})</span>
            ${enrollments.length > 0 ? '<span style="font-size:0.65rem;color:#16a34a;font-weight:600;">' + enrollments.filter(e => e.status === 'active').length + ' active</span>' : ''}
          </div>
          ${enrollments.length > 0 ? `
            <div style="max-height:120px;overflow-y:auto;">
              ${enrollments.slice(0, 30).map(e => `
                <div style="display:flex;justify-content:space-between;padding:0.15rem 0;border-bottom:1px solid #f3f4f6;font-size:0.7rem;">
                  <span style="color:#374151;">${esc(e.first_name || '')} ${esc(e.last_name || '')} ${e.email ? '<span style="color:#6b7280;">(' + esc(e.email) + ')</span>' : ''}</span>
                  <span style="padding:0.05rem 0.3rem;border-radius:8px;font-size:0.6rem;font-weight:600;${e.status === 'active' ? 'background:#dcfce7;color:#166534;' : e.status === 'completed' ? 'background:#dbeafe;color:#1e40af;' : 'background:#f3f4f6;color:#6b7280;'}">${e.status}</span>
                </div>
              `).join('')}
              ${enrollments.length > 30 ? '<p style="font-size:0.65rem;color:#9ca3af;text-align:center;">...and ' + (enrollments.length - 30) + ' more</p>' : ''}
            </div>
          ` : '<p style="font-size:0.72rem;color:#9ca3af;">No leads enrolled. Select leads in the table and use "Enroll" to add them.</p>'}
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

async function aiGenerateVariants(seqId) {
  const stepNum = parseInt(document.getElementById('ai-variant-step').value) || 1;
  const practice = document.getElementById('ai-variant-practice').value.trim() || 'general';
  const purpose = document.getElementById('ai-variant-purpose').value;
  const resultEl = document.getElementById('ai-variant-results');
  const btn = document.getElementById('ai-gen-variants-btn');
  btn.disabled = true; btn.textContent = 'Generating...';
  resultEl.innerHTML = '<div style="font-size:0.72rem;color:#6366f1;">AI is writing variants...</div>';
  try {
    const res = await fetch('/api/ai/sequence-variants', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stepContext: { stepNumber: stepNum, channel: 'email', purpose, practiceArea: practice }, numVariants: 3 }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const variants = d.variants || [];
    if (variants.length === 0) { resultEl.innerHTML = '<div style="font-size:0.72rem;color:#fca5a5;">No variants generated.</div>'; return; }
    let html = '';
    variants.forEach((v, i) => {
      const varLabel = String.fromCharCode(65 + i);
      html += `<div style="background:#fff;border:1px solid #c7d2fe;border-left:3px solid ${['#7c3aed','#3b82f6','#10b981'][i] || '#6366f1'};border-radius:6px;padding:0.4rem 0.5rem;margin-top:0.3rem;">`;
      html += `<div style="display:flex;justify-content:space-between;align-items:center;">`;
      html += `<span style="font-size:0.68rem;font-weight:600;color:${['#7c3aed','#3b82f6','#10b981'][i] || '#6366f1'};">${esc(v.label || 'Variant ' + varLabel)}</span>`;
      html += `<button onclick="saveAiVariant(${seqId}, ${stepNum}, '${varLabel}', this)" style="font-size:0.62rem;padding:0.1rem 0.4rem;background:#7c3aed;color:#fff;border:none;border-radius:3px;cursor:pointer;">Save as Variant ${varLabel}</button>`;
      html += `</div>`;
      html += `<div style="font-size:0.74rem;font-weight:500;margin-top:0.1rem;">${esc(v.subject || '')}</div>`;
      html += `<div style="font-size:0.7rem;color:#6b7280;margin-top:0.05rem;white-space:pre-wrap;">${esc(v.body || '')}</div>`;
      html += `<input type="hidden" class="ai-variant-subject" value="${esc(v.subject || '')}"><input type="hidden" class="ai-variant-body" value="${esc(v.body || '')}">`;
      html += `</div>`;
    });
    resultEl.innerHTML = html;
  } catch (err) {
    resultEl.innerHTML = `<div style="font-size:0.72rem;color:#fca5a5;">Error: ${esc(err.message)}</div>`;
  } finally { btn.disabled = false; btn.textContent = 'Generate 3 Variants'; }
}

async function saveAiVariant(seqId, stepNumber, variant, btn) {
  const card = btn.closest('div[style*="border-left"]');
  const subject = card.querySelector('.ai-variant-subject').value;
  const body = card.querySelector('.ai-variant-body').value;
  btn.disabled = true; btn.textContent = 'Saving...';
  try {
    await fetch(`/api/sequences/${seqId}/steps`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stepNumber, channel: 'email', subject, body, delayDays: 0, variant }),
    });
    btn.textContent = 'Saved!';
    btn.style.background = '#059669';
    toast(`Variant ${variant} saved to step ${stepNumber}`, 'success');
  } catch (err) { btn.textContent = 'Error'; toast('Save failed: ' + err.message, 'error'); }
}

async function addStepToSequence(seqId) {
  const subject = document.getElementById('new-step-subject').value.trim();
  const body = document.getElementById('new-step-body').value.trim();
  const delayDays = parseInt(document.getElementById('new-step-delay').value) || 0;
  if (!subject && !body) { alert('Subject or body required'); return; }

  const seqs = await fetch('/api/sequences').then(safeJSON);
  const seq = seqs.find(s => s.id === seqId);
  const nextStep = (seq?.steps?.length || 0) + 1;

  await fetch(`/api/sequences/${seqId}/steps`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stepNumber: nextStep, channel: 'email', subject, body, delayDays }),
  });
  toast('Step added', 'success');
  document.getElementById('sequence-detail-modal').remove();
  showSequenceDetail(seqId);
}

async function enrollSelectedInSequence() {
  const selected = [..._selectedIds];
  if (selected.length === 0) { toast('Select leads first', 'error'); return; }

  const seqs = await fetch('/api/sequences').then(safeJSON);
  if (seqs.length === 0) { toast('Create a sequence first', 'error'); return; }

  const seqId = seqs.length === 1 ? seqs[0].id : parseInt(prompt('Enter sequence ID:\n' + seqs.map(s => s.id + ': ' + s.name).join('\n')));
  if (!seqId) return;

  const btn = document.getElementById('btn-enroll');
  if (btn) { btn.disabled = true; btn.textContent = 'Enrolling...'; }
  try {
    const result = await fetch(`/api/sequences/${seqId}/enroll`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadIds: selected }),
    }).then(safeJSON);
    toast(`Enrolled ${result.enrolled} leads in sequence`, 'success');
    loadSequences();
  } catch (err) { toast('Failed to enroll: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Enroll'; } }
}

function findLookalikesSelected() {
  const selected = [..._selectedIds];
  if (selected.length === 0) { toast('Select a lead first', 'error'); return; }
  findLookalikes(selected[0]);
}

async function batchAiEmails() {
  const selected = [..._selectedIds];
  if (selected.length === 0) { toast('Select leads first', 'error'); return; }
  if (selected.length > 20) { toast('Max 20 leads at a time for AI emails', 'error'); return; }
  const leads = _currentLeads.filter(l => selected.includes(l.id));
  if (leads.length === 0) return;

  // Show progress modal
  const modal = document.createElement('div');
  modal.id = 'batch-ai-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:700px;max-width:92vw;max-height:85vh;overflow-y:auto;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.75rem;">
        <h3 style="margin:0;font-size:1rem;color:#4f46e5;">AI Batch Email Generator</h3>
        <button onclick="document.getElementById('batch-ai-modal').remove()" style="border:none;background:none;font-size:1.3rem;cursor:pointer;color:#9ca3af;">&times;</button>
      </div>
      <div style="margin-bottom:0.5rem;">
        <div style="display:flex;gap:0.3rem;margin-bottom:0.3rem;">
          <select id="batch-email-tone" style="padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.75rem;">
            <option value="professional">Professional</option><option value="friendly">Friendly</option><option value="casual">Casual</option><option value="urgent">Urgent</option>
          </select>
          <select id="batch-email-purpose" style="padding:0.25rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.75rem;">
            <option value="cold outreach">Cold Outreach</option><option value="follow-up">Follow-up</option><option value="case study share">Case Study</option><option value="meeting request">Meeting Request</option>
          </select>
        </div>
        <div style="font-size:0.7rem;color:#6b7280;">Generating for ${leads.length} lead${leads.length > 1 ? 's' : ''}...</div>
      </div>
      <div style="height:4px;background:#e5e7eb;border-radius:2px;margin-bottom:0.5rem;"><div id="batch-progress" style="height:100%;background:#6366f1;border-radius:2px;width:0;transition:width 0.3s;"></div></div>
      <div id="batch-results" style="display:flex;flex-direction:column;gap:0.3rem;"></div>
      <div style="display:flex;gap:0.3rem;margin-top:0.5rem;">
        <button id="batch-start-btn" onclick="startBatchAiEmails()" style="padding:0.3rem 0.8rem;background:#4f46e5;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:500;">Generate All</button>
        <button onclick="copyAllBatchEmails()" style="padding:0.3rem 0.8rem;background:#059669;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:500;">Copy All</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  window._batchLeads = leads;
}

async function startBatchAiEmails() {
  const leads = window._batchLeads || [];
  const tone = document.getElementById('batch-email-tone').value;
  const purpose = document.getElementById('batch-email-purpose').value;
  const bar = document.getElementById('batch-progress');
  const results = document.getElementById('batch-results');
  const btn = document.getElementById('batch-start-btn');
  btn.disabled = true; btn.textContent = 'Generating...';
  results.innerHTML = '';

  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    bar.style.width = ((i + 1) / leads.length * 100) + '%';
    try {
      const res = await fetch('/api/ai/write-email', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ leadId: lead.id, tone, context: purpose }),
      });
      const d = await safeJSON(res);
      if (d.error) throw new Error(d.error);
      results.innerHTML += `
        <div class="batch-email-result" style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:6px;padding:0.4rem 0.5rem;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.15rem;">
            <span style="font-size:0.72rem;font-weight:600;color:#374151;">${esc(lead.first_name || '')} ${esc(lead.last_name || '')} ${lead.email ? '<span style="color:#6b7280;font-weight:400;">(' + esc(lead.email) + ')</span>' : ''}</span>
            <button onclick="navigator.clipboard.writeText(this.closest('.batch-email-result').querySelector('.email-body').textContent);toast('Copied!','success');" style="font-size:0.6rem;padding:0.1rem 0.3rem;border:1px solid #d1d5db;border-radius:3px;background:#fff;cursor:pointer;">Copy</button>
          </div>
          <div style="font-size:0.72rem;font-weight:500;color:#4338ca;">${esc(d.subject || '')}</div>
          <div class="email-body" style="font-size:0.7rem;color:#4b5563;white-space:pre-wrap;max-height:80px;overflow-y:auto;">${esc(d.body || '')}</div>
        </div>`;
    } catch (err) {
      results.innerHTML += `<div style="font-size:0.72rem;color:#ef4444;padding:0.2rem;">Failed: ${esc(lead.first_name || '')} ${esc(lead.last_name || '')} — ${esc(err.message)}</div>`;
    }
  }
  btn.textContent = 'Done!'; btn.style.background = '#059669';
}

function copyAllBatchEmails() {
  const results = document.querySelectorAll('.batch-email-result');
  if (results.length === 0) { toast('Generate emails first', 'error'); return; }
  const texts = [...results].map(r => {
    const name = r.querySelector('span').textContent;
    const subj = r.querySelector('div[style*="color:#4338ca"]')?.textContent || '';
    const body = r.querySelector('.email-body')?.textContent || '';
    return `To: ${name}\nSubject: ${subj}\n\n${body}`;
  }).join('\n\n---\n\n');
  navigator.clipboard.writeText(texts);
  toast(`${results.length} emails copied to clipboard`, 'success');
}

// --- AI Command Center ---
async function aiCommandAction(action) {
  const result = document.getElementById('ai-command-result');
  if (!result) return;

  if (action === 'top-leads') {
    result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Analyzing your best leads...</div>';
    try {
      const res = await fetch('/api/leads?sort=lead_score&order=desc&limit=10&minScore=50');
      const d = await safeJSON(res);
      const leads = d.leads || [];
      if (leads.length === 0) { result.innerHTML = '<div style="color:#94a3b8;font-size:0.75rem;padding:0.5rem;">No high-score leads found.</div>'; return; }
      let html = '<div style="font-size:0.68rem;color:#818cf8;font-weight:600;margin-bottom:0.3rem;">TOP LEADS TO CONTACT TODAY</div>';
      html += leads.map((l, i) => {
        const scoreColor = l.lead_score >= 80 ? '#22c55e' : l.lead_score >= 60 ? '#f59e0b' : '#6366f1';
        return `<div style="display:flex;align-items:center;gap:0.4rem;padding:0.25rem 0;border-bottom:1px solid #1e293b;">
          <span style="width:18px;height:18px;border-radius:50%;background:${scoreColor};color:#fff;font-size:0.55rem;display:flex;align-items:center;justify-content:center;font-weight:700;flex-shrink:0;">${i + 1}</span>
          <div style="flex:1;min-width:0;">
            <div style="font-size:0.72rem;font-weight:500;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${esc(l.first_name || '')} ${esc(l.last_name || '')} <span style="color:#64748b;">— ${esc(l.firm_name || '?')}</span></div>
            <div style="font-size:0.62rem;color:#64748b;">${esc(l.city || '')} ${esc(l.state || '')} &middot; ${esc(l.practice_area || '')} &middot; Score: ${l.lead_score}</div>
          </div>
          <div style="display:flex;gap:0.2rem;flex-shrink:0;">
            ${l.email ? '<button onclick="navigator.clipboard.writeText(\'' + esc(l.email) + '\');toast(\'Copied!\',\'success\');" style="font-size:0.55rem;padding:0.1rem 0.25rem;background:#1e293b;border:1px solid #334155;border-radius:3px;color:#818cf8;cursor:pointer;">Email</button>' : ''}
            ${l.phone ? '<button onclick="navigator.clipboard.writeText(\'' + esc(l.phone) + '\');toast(\'Copied!\',\'success\');" style="font-size:0.55rem;padding:0.1rem 0.25rem;background:#1e293b;border:1px solid #334155;border-radius:3px;color:#818cf8;cursor:pointer;">Phone</button>' : ''}
            <button onclick="openLeadModal(${l.id})" style="font-size:0.55rem;padding:0.1rem 0.25rem;background:#4f46e5;border:none;border-radius:3px;color:#fff;cursor:pointer;">View</button>
          </div>
        </div>`;
      }).join('');
      result.innerHTML = html;
    } catch (err) { result.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">Error: ${esc(err.message)}</div>`; }
  }

  else if (action === 'batch-email') {
    result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Fetching leads with email for batch generation...</div>';
    try {
      const res = await fetch('/api/leads?sort=lead_score&order=desc&limit=5&hasEmail=true&minScore=40');
      const d = await safeJSON(res);
      const leads = d.leads || [];
      if (leads.length === 0) { result.innerHTML = '<div style="color:#94a3b8;font-size:0.75rem;">No leads with email found.</div>'; return; }
      let html = '<div style="font-size:0.68rem;color:#818cf8;font-weight:600;margin-bottom:0.3rem;">AI EMAILS FOR TOP LEADS</div>';
      html += '<div style="font-size:0.65rem;color:#64748b;margin-bottom:0.3rem;">Generating for ' + leads.length + ' leads...</div>';
      html += '<div id="ai-cmd-emails"></div>';
      result.innerHTML = html;
      const emailEl = document.getElementById('ai-cmd-emails');
      for (const lead of leads) {
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
          emailEl.innerHTML += `<div style="font-size:0.65rem;color:#fca5a5;">Failed for ${esc(lead.first_name || '')}: ${esc(err.message)}</div>`;
        }
      }
    } catch (err) { result.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">Error: ${esc(err.message)}</div>`; }
  }

  else if (action === 'call-list') {
    result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Building your call list...</div>';
    try {
      const res = await fetch('/api/leads?sort=lead_score&order=desc&limit=10&hasPhone=true&minScore=30');
      const d = await safeJSON(res);
      const leads = d.leads || [];
      if (leads.length === 0) { result.innerHTML = '<div style="color:#94a3b8;font-size:0.75rem;">No leads with phone found.</div>'; return; }
      let html = '<div style="font-size:0.68rem;color:#818cf8;font-weight:600;margin-bottom:0.3rem;">CALL LIST — ' + leads.length + ' leads with phone</div>';
      html += '<table style="width:100%;font-size:0.68rem;border-collapse:collapse;">';
      html += '<tr style="color:#64748b;border-bottom:1px solid #334155;"><th style="text-align:left;padding:0.15rem 0.3rem;">#</th><th style="text-align:left;padding:0.15rem 0.3rem;">Name</th><th style="text-align:left;padding:0.15rem 0.3rem;">Firm</th><th style="text-align:left;padding:0.15rem 0.3rem;">Phone</th><th style="text-align:left;padding:0.15rem 0.3rem;">Score</th></tr>';
      leads.forEach((l, i) => {
        html += `<tr style="border-bottom:1px solid #1e293b;cursor:pointer;" onclick="openLeadModal(${l.id})">
          <td style="padding:0.15rem 0.3rem;color:#94a3b8;">${i + 1}</td>
          <td style="padding:0.15rem 0.3rem;color:#e2e8f0;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</td>
          <td style="padding:0.15rem 0.3rem;color:#94a3b8;">${esc(l.firm_name || '')}</td>
          <td style="padding:0.15rem 0.3rem;"><a href="tel:${esc(l.phone || '')}" style="color:#818cf8;">${esc(l.phone || '')}</a></td>
          <td style="padding:0.15rem 0.3rem;color:${l.lead_score >= 60 ? '#22c55e' : '#f59e0b'};">${l.lead_score}</td>
        </tr>`;
      });
      html += '</table>';
      html += '<div style="margin-top:0.3rem;font-size:0.62rem;color:#64748b;">Click any lead to open their profile and generate a call script.</div>';
      result.innerHTML = html;
    } catch (err) { result.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">Error: ${esc(err.message)}</div>`; }
  }

  else if (action === 'audit') {
    result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Running AI data quality audit...</div>';
    try {
      const res = await fetch('/api/ai/audit', { method: 'POST' });
      const d = await safeJSON(res);
      if (d.error) throw new Error(d.error);
      let html = '<div style="font-size:0.68rem;color:#818cf8;font-weight:600;margin-bottom:0.3rem;">DATA QUALITY AUDIT</div>';
      if (d.overall_grade) html += `<div style="font-size:0.82rem;font-weight:700;color:${d.overall_grade === 'A' ? '#22c55e' : d.overall_grade === 'B' ? '#3b82f6' : '#f59e0b'};margin-bottom:0.3rem;">Grade: ${esc(d.overall_grade)}</div>`;
      if (d.summary) html += `<div style="font-size:0.72rem;color:#cbd5e1;margin-bottom:0.3rem;">${esc(d.summary)}</div>`;
      if (d.issues && d.issues.length > 0) {
        html += d.issues.map(issue => `<div style="font-size:0.68rem;color:#fbbf24;padding:0.1rem 0;">&#x26A0; ${esc(issue)}</div>`).join('');
      }
      if (d.recommendations && d.recommendations.length > 0) {
        html += '<div style="font-size:0.62rem;color:#818cf8;font-weight:600;margin-top:0.3rem;">Recommendations:</div>';
        html += d.recommendations.map(r => `<div style="font-size:0.68rem;color:#94a3b8;padding:0.05rem 0;">&#x2192; ${esc(r)}</div>`).join('');
      }
      result.innerHTML = html;
    } catch (err) { result.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">Audit error: ${esc(err.message)}</div>`; }
  }

  else if (action === 'plan') {
    result.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;padding:0.5rem;">Building your 5-day outreach plan...</div>';
    try {
      const res = await fetch('/api/ai/outreach-plan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
      const d = await safeJSON(res);
      if (d.error) throw new Error(d.error);
      let html = '<div style="font-size:0.68rem;color:#818cf8;font-weight:600;margin-bottom:0.3rem;">5-DAY OUTREACH PLAN</div>';
      if (d.summary) html += `<div style="font-size:0.72rem;color:#cbd5e1;margin-bottom:0.4rem;">${esc(d.summary)}</div>`;
      const days = d.daily_plan || [];
      days.forEach(day => {
        const dayColors = ['#818cf8','#22c55e','#f59e0b','#ec4899','#06b6d4'];
        const color = dayColors[(day.day || 1) - 1] || '#818cf8';
        html += `<div style="margin-bottom:0.4rem;border-left:3px solid ${color};padding-left:0.4rem;">
          <div style="font-size:0.72rem;font-weight:600;color:${color};">Day ${day.day}: ${esc(day.focus || '')}</div>`;
        const leads = day.leads || [];
        leads.forEach(l => {
          const prioColor = l.priority === 'high' ? '#ef4444' : l.priority === 'medium' ? '#f59e0b' : '#64748b';
          html += `<div style="display:flex;gap:0.3rem;align-items:center;padding:0.15rem 0;">
            <span style="width:6px;height:6px;border-radius:50%;background:${prioColor};flex-shrink:0;"></span>
            <span style="font-size:0.68rem;color:#e2e8f0;font-weight:500;">${esc(l.name || '')}</span>
            <span style="font-size:0.62rem;color:#94a3b8;">— ${esc(l.action || '')}</span>
          </div>`;
        });
        html += '</div>';
      });
      if (d.tips && d.tips.length > 0) {
        html += '<div style="font-size:0.62rem;color:#818cf8;font-weight:600;margin-top:0.3rem;">Tips:</div>';
        html += d.tips.map(t => `<div style="font-size:0.65rem;color:#94a3b8;padding:0.05rem 0;">&#x1F4A1; ${esc(t)}</div>`).join('');
      }
      result.innerHTML = html;
    } catch (err) { result.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">Plan error: ${esc(err.message)}</div>`; }
  }
}

// === Firm Directory ===
async function loadFirmDirectory() {
  try {
    const firms = await fetch('/api/firms/directory?limit=30').then(safeJSON);
    const list = document.getElementById('firm-directory-list');
    if (firms.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#7dd3fc;">Run firm enrichment to populate this directory.</span>';
      return;
    }
    list.innerHTML = `
      <table style="width:100%;font-size:0.72rem;border-collapse:collapse;">
        <thead><tr style="text-align:left;color:#6b7280;border-bottom:1px solid #e5e7eb;">
          <th style="padding:0.2rem 0.4rem;">Firm</th><th style="padding:0.2rem 0.4rem;">Size</th><th style="padding:0.2rem 0.4rem;">Emails</th><th style="padding:0.2rem 0.4rem;">Phones</th><th style="padding:0.2rem 0.4rem;">Locations</th>
        </tr></thead>
        <tbody>${firms.map(f => `
          <tr onclick="openFirmDetail('${esc(f.firm_name).replace(/'/g, "\\'")}')" style="cursor:pointer;border-bottom:1px solid #f3f4f6;" onmouseover="this.style.background='#f0f9ff'" onmouseout="this.style.background=''">
            <td style="padding:0.25rem 0.4rem;font-weight:500;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(f.firm_name)}</td>
            <td style="padding:0.25rem 0.4rem;"><strong>${f.size}</strong></td>
            <td style="padding:0.25rem 0.4rem;color:${f.emails > 0 ? '#15803d' : '#d1d5db'};">${f.emails}</td>
            <td style="padding:0.25rem 0.4rem;color:${f.phones > 0 ? '#0369a1' : '#d1d5db'};">${f.phones}</td>
            <td style="padding:0.25rem 0.4rem;font-size:0.68rem;color:#9ca3af;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(f.locations || '')}</td>
          </tr>
        `).join('')}</tbody>
      </table>
    `;
  } catch (err) { console.error('Firm directory error:', err); }
}

async function runFirmEnrichment() {
  try {
    const result = await fetch('/api/firms/enrich', { method: 'POST' }).then(safeJSON);
    toast(`Enriched ${result.firmsEnriched} firms`, 'success');
    loadFirmDirectory();
  } catch (err) { toast('Enrichment error: ' + err.message, 'error'); }
}

// === Firm Detail Modal ===
async function openFirmDetail(firmName) {
  const modal = document.getElementById('firm-detail-modal');
  modal.style.display = 'flex';
  document.getElementById('firm-modal-name').textContent = firmName;
  document.getElementById('firm-modal-stats').innerHTML = '<div style="font-size:0.72rem;color:#78716c;">Loading...</div>';
  document.getElementById('firm-modal-leads').innerHTML = '';
  try {
    const data = await fetch('/api/firms/' + encodeURIComponent(firmName)).then(safeJSON);
    const leads = data.leads || [];
    const emails = leads.filter(l => l.email).length;
    const phones = leads.filter(l => l.phone).length;
    const websites = leads.filter(l => l.website).length;
    const states = [...new Set(leads.map(l => l.state).filter(Boolean))];
    const avgScore = leads.length ? Math.round(leads.reduce((s, l) => s + (l.lead_score || 0), 0) / leads.length) : 0;
    document.getElementById('firm-modal-stats').innerHTML = `
      <div style="background:#f0fdf4;padding:0.4rem 0.8rem;border-radius:6px;text-align:center;">
        <div style="font-size:1.2rem;font-weight:700;color:#15803d;">${leads.length}</div>
        <div style="font-size:0.65rem;color:#6b7280;">Attorneys</div>
      </div>
      <div style="background:#eff6ff;padding:0.4rem 0.8rem;border-radius:6px;text-align:center;">
        <div style="font-size:1.2rem;font-weight:700;color:#2563eb;">${emails}</div>
        <div style="font-size:0.65rem;color:#6b7280;">Emails</div>
      </div>
      <div style="background:#faf5ff;padding:0.4rem 0.8rem;border-radius:6px;text-align:center;">
        <div style="font-size:1.2rem;font-weight:700;color:#7c3aed;">${phones}</div>
        <div style="font-size:0.65rem;color:#6b7280;">Phones</div>
      </div>
      <div style="background:#fefce8;padding:0.4rem 0.8rem;border-radius:6px;text-align:center;">
        <div style="font-size:1.2rem;font-weight:700;color:#a16207;">${avgScore}</div>
        <div style="font-size:0.65rem;color:#6b7280;">Avg Score</div>
      </div>
      <div style="background:#f8fafc;padding:0.4rem 0.8rem;border-radius:6px;text-align:center;">
        <div style="font-size:0.85rem;font-weight:600;color:#475569;">${states.join(', ')}</div>
        <div style="font-size:0.65rem;color:#6b7280;">${states.length} Location${states.length !== 1 ? 's' : ''}</div>
      </div>
    `;
    if (leads.length === 0) {
      document.getElementById('firm-modal-leads').innerHTML = '<div style="text-align:center;color:#9ca3af;padding:1rem;">No attorneys found.</div>';
      return;
    }
    document.getElementById('firm-modal-leads').innerHTML = `
      <table style="width:100%;font-size:0.75rem;border-collapse:collapse;">
        <thead><tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
          <th style="padding:0.3rem;">Name</th><th style="padding:0.3rem;">Title</th>
          <th style="padding:0.3rem;">Email</th><th style="padding:0.3rem;">Phone</th>
          <th style="padding:0.3rem;">City</th><th style="padding:0.3rem;">Score</th>
        </tr></thead>
        <tbody>${leads.map(l => `
          <tr style="border-bottom:1px solid #f3f4f6;cursor:pointer;" onclick="closeFirmModal();openLeadDetail(${l.id})" onmouseover="this.style.background='#f0f9ff'" onmouseout="this.style.background=''">
            <td style="padding:0.3rem;font-weight:500;">${esc(l.first_name || '')} ${esc(l.last_name || '')}</td>
            <td style="padding:0.3rem;color:#6b7280;font-size:0.7rem;">${esc(l.title || '—')}</td>
            <td style="padding:0.3rem;color:${l.email ? '#15803d' : '#d1d5db'};font-size:0.7rem;">${l.email ? esc(l.email) : '—'}</td>
            <td style="padding:0.3rem;color:${l.phone ? '#2563eb' : '#d1d5db'};font-size:0.7rem;">${l.phone ? esc(l.phone) : '—'}</td>
            <td style="padding:0.3rem;font-size:0.7rem;">${esc(l.city || '')}${l.state ? ', ' + l.state : ''}</td>
            <td style="padding:0.3rem;font-weight:600;color:${(l.lead_score||0) >= 80 ? '#15803d' : (l.lead_score||0) >= 55 ? '#2563eb' : '#dc2626'};">${l.lead_score || 0}</td>
          </tr>
        `).join('')}</tbody>
      </table>
    `;
  } catch (err) {
    document.getElementById('firm-modal-leads').innerHTML = '<div style="color:#dc2626;padding:1rem;">Error: ' + err.message + '</div>';
  }
}

function closeFirmModal() {
  document.getElementById('firm-detail-modal').style.display = 'none';
}

// === Most Engaged Leads ===
async function loadEngagementPanel() {
  try {
    const leads = await fetch('/api/leads/most-engaged?limit=10').then(safeJSON);
    const list = document.getElementById('engaged-leads-list');
    const countEl = document.getElementById('engagement-count');
    if (leads.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#fbbf24;">No activity tracked yet. View, email, or tag leads to build engagement.</span>';
      countEl.textContent = '';
      return;
    }
    countEl.textContent = leads.length + ' engaged leads';
    list.innerHTML = leads.map((l, i) => `
      <div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0;border-bottom:1px solid #fef3c7;font-size:0.72rem;">
        <div>
          <span style="font-weight:500;">${esc(l.first_name)} ${esc(l.last_name)}</span>
          <span style="color:#6b7280;margin-left:0.3rem;">${esc(l.firm_name || '')}${l.city ? ', ' + esc(l.city) : ''}</span>
        </div>
        <div style="display:flex;align-items:center;gap:0.4rem;">
          <span style="font-size:0.65rem;color:#b45309;font-weight:600;">${l.activity_count} actions</span>
          <button onclick="showLeadActivities(${l.id})" style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#fef3c7;border:1px solid #fcd34d;border-radius:3px;cursor:pointer;">View</button>
        </div>
      </div>
    `).join('');
  } catch (err) { console.error('Engagement error:', err); }
}

async function showLeadActivities(leadId) {
  try {
    const activities = await fetch(`/api/leads/${leadId}/activities`).then(safeJSON);
    const engagement = await fetch(`/api/leads/${leadId}/engagement`).then(safeJSON);

    const modal = document.createElement('div');
    modal.id = 'activity-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
    modal.innerHTML = `
      <div style="background:#fff;border-radius:12px;padding:1.5rem;width:450px;max-width:90vw;max-height:70vh;overflow-y:auto;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">
          <h3 style="margin:0;font-size:1rem;">Lead Activity (Score: ${engagement.score}/100)</h3>
          <button onclick="document.getElementById('activity-modal').remove()" style="border:none;background:none;font-size:1.2rem;cursor:pointer;">&times;</button>
        </div>
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.75rem;">
          <div style="flex:1;height:6px;background:#e5e7eb;border-radius:3px;overflow:hidden;">
            <div style="height:100%;width:${engagement.score}%;background:${engagement.score > 50 ? '#22c55e' : engagement.score > 20 ? '#f59e0b' : '#d1d5db'};border-radius:3px;"></div>
          </div>
          <span style="font-size:0.75rem;font-weight:600;">${engagement.score}/100</span>
        </div>
        ${activities.length > 0 ? activities.map(a => `
          <div style="display:flex;justify-content:space-between;padding:0.3rem 0;border-bottom:1px solid #f3f4f6;font-size:0.75rem;">
            <div>
              <span style="font-weight:500;color:${getActivityColor(a.action)};">${esc(a.action)}</span>
              ${a.details ? '<span style="color:#6b7280;margin-left:0.3rem;">' + esc(a.details) + '</span>' : ''}
            </div>
            <span style="color:#9ca3af;font-size:0.68rem;">${new Date(a.created_at).toLocaleDateString()}</span>
          </div>
        `).join('') : '<p style="font-size:0.78rem;color:#9ca3af;">No activities recorded.</p>'}
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

function getActivityColor(action) {
  const colors = { viewed: '#6b7280', emailed: '#7c3aed', called: '#15803d', exported: '#0369a1', tagged: '#b45309', enriched: '#059669', stage_changed: '#dc2626', note_added: '#6366f1' };
  return colors[action] || '#6b7280';
}

// === Lookalike Finder ===
async function findLookalikes(leadId) {
  try {
    const results = await fetch(`/api/leads/${leadId}/lookalikes?limit=15`).then(safeJSON);
    if (results.length === 0) { toast('No lookalikes found', 'info'); return; }

    const modal = document.createElement('div');
    modal.id = 'lookalike-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
    modal.innerHTML = `
      <div style="background:#fff;border-radius:12px;padding:1.5rem;width:600px;max-width:90vw;max-height:80vh;overflow-y:auto;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">
          <h3 style="margin:0;font-size:1rem;">Lookalike Leads (${results.length} found)</h3>
          <button onclick="document.getElementById('lookalike-modal').remove()" style="border:none;background:none;font-size:1.2rem;cursor:pointer;">&times;</button>
        </div>
        <p style="font-size:0.72rem;color:#6b7280;margin-bottom:0.75rem;">Leads with similar practice area, city, state, and firm size.</p>
        <table style="width:100%;font-size:0.72rem;border-collapse:collapse;">
          <thead><tr style="text-align:left;color:#6b7280;border-bottom:2px solid #e5e7eb;">
            <th style="padding:0.3rem;">Name</th><th style="padding:0.3rem;">Firm</th><th style="padding:0.3rem;">Location</th><th style="padding:0.3rem;">Email</th><th style="padding:0.3rem;">Match</th>
          </tr></thead>
          <tbody>${results.map(r => `
            <tr style="border-bottom:1px solid #f3f4f6;">
              <td style="padding:0.3rem;font-weight:500;">${esc(r.first_name)} ${esc(r.last_name)}</td>
              <td style="padding:0.3rem;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(r.firm_name || '-')}</td>
              <td style="padding:0.3rem;">${esc(r.city || '')}${r.state ? ', ' + esc(r.state) : ''}</td>
              <td style="padding:0.3rem;color:${r.email ? '#15803d' : '#d1d5db'};">${r.email ? esc(r.email) : '-'}</td>
              <td style="padding:0.3rem;"><span style="display:inline-block;padding:0.1rem 0.4rem;border-radius:8px;font-weight:600;font-size:0.68rem;background:${r.similarity_score >= 40 ? '#dcfce7;color:#15803d' : r.similarity_score >= 25 ? '#fef3c7;color:#b45309' : '#f3f4f6;color:#6b7280'};">${r.similarity_score}pts</span></td>
            </tr>
          `).join('')}</tbody>
        </table>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  } catch (err) { toast('Error finding lookalikes: ' + err.message, 'error'); }
}

// === Do Not Contact ===
async function loadDncPanel() {
  try {
    const entries = await fetch('/api/dnc').then(safeJSON);
    const check = await fetch('/api/dnc/check').then(safeJSON);
    const countEl = document.getElementById('dnc-blocked-count');
    countEl.textContent = check.blocked > 0 ? check.blocked + ' blocked' : entries.length + ' rules';

    const list = document.getElementById('dnc-list');
    if (entries.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#fca5a5;">No DNC entries. Add emails, domains, or phones to block.</span>';
      return;
    }
    list.innerHTML = entries.slice(0, 30).map(e => `
      <span style="display:inline-flex;align-items:center;gap:0.3rem;padding:0.15rem 0.5rem;background:#fff;border:1px solid #fca5a5;border-radius:8px;font-size:0.68rem;">
        <span style="color:#dc2626;font-weight:600;">${esc(e.type)}</span>
        <span>${esc(e.value)}</span>
        <button onclick="removeDnc(${e.id})" style="border:none;background:none;cursor:pointer;color:#dc2626;font-size:0.7rem;padding:0;">&times;</button>
      </span>
    `).join('');
  } catch (err) { console.error('DNC error:', err); }
}

function showAddDncModal() {
  const modal = document.createElement('div');
  modal.id = 'dnc-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:400px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#dc2626;">Add to Do Not Contact</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Type</label>
        <select id="dnc-type" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
          <option value="email">Email Address</option>
          <option value="domain">Email Domain</option>
          <option value="phone">Phone Number</option>
        </select>
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Value</label>
        <input id="dnc-value" type="text" placeholder="e.g., spam@example.com or example.com" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Reason (optional)</label>
        <input id="dnc-reason" type="text" placeholder="e.g., Unsubscribed, Bounced" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('dnc-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="addDncEntry()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#dc2626;color:#fff;cursor:pointer;">Block</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function addDncEntry() {
  const type = document.getElementById('dnc-type').value;
  const value = document.getElementById('dnc-value').value.trim();
  const reason = document.getElementById('dnc-reason').value.trim();
  if (!value) { alert('Value required'); return; }
  await fetch('/api/dnc', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, value, reason }),
  });
  document.getElementById('dnc-modal').remove();
  toast('Added to DNC list', 'success');
  loadDncPanel();
}

async function removeDnc(id) {
  await fetch('/api/dnc/' + id, { method: 'DELETE' });
  toast('Removed from DNC', 'success');
  loadDncPanel();
}

// === Smart Duplicates ===
async function loadSmartDuplicates() {
  try {
    const data = await fetch('/api/leads/smart-duplicates').then(safeJSON);
    const list = document.getElementById('duplicates-list');
    if (data.total === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#fdba74;">No duplicates found. Your data is clean!</span>';
      return;
    }
    list.innerHTML = `
      <div style="font-size:0.72rem;color:#ea580c;font-weight:600;margin-bottom:0.3rem;">${data.total} duplicate groups found</div>
      ${data.groups.slice(0, 20).map(g => `
        <div style="display:flex;align-items:center;justify-content:space-between;padding:0.25rem 0.4rem;background:#fff;border:1px solid #fed7aa;border-radius:4px;margin-bottom:0.2rem;font-size:0.72rem;">
          <div>
            <span style="font-weight:600;color:#ea580c;">${esc(g.type)}</span>
            <span style="color:#6b7280;margin-left:0.3rem;">${esc(g.matchValue)}</span>
          </div>
          <span style="font-size:0.68rem;color:#9ca3af;">${g.count} records</span>
        </div>
      `).join('')}
    `;
  } catch (err) { console.error('Duplicates error:', err); }
}

async function runAutoMerge(dryRun = true) {
  if (!dryRun && !confirm('Auto-merge duplicates? This will permanently combine duplicate records.')) return;
  try {
    const result = await fetch('/api/leads/auto-merge', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dryRun }),
    }).then(safeJSON);
    if (dryRun) {
      toast(`Found ${result.candidates} merge candidates. Click "Auto-Merge" again to execute.`, 'info');
      // Re-call with dryRun=false after confirmation
    } else {
      toast(`Merged ${result.merged} duplicate pairs`, 'success');
      refreshAll();
    }
    loadSmartDuplicates();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Territory Management ===
async function loadTerritories() {
  try {
    const territories = await fetch('/api/territories').then(safeJSON);
    const list = document.getElementById('territory-list');
    if (territories.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#5eead4;">No territories yet. Create one to organize leads by geography.</span>';
      return;
    }
    list.innerHTML = territories.map(t => `
      <div style="display:flex;align-items:center;justify-content:space-between;padding:0.3rem 0.5rem;background:#fff;border:1px solid #99f6e4;border-radius:6px;">
        <div>
          <strong style="font-size:0.8rem;color:#0d9488;">${esc(t.name)}</strong>
          ${t.owner ? '<span style="font-size:0.68rem;color:#6b7280;margin-left:0.4rem;">(' + esc(t.owner) + ')</span>' : ''}
          <br><span style="font-size:0.68rem;color:#9ca3af;">${esc(t.states || '')}${t.cities ? ' | ' + esc(t.cities) : ''}</span>
        </div>
        <div style="display:flex;align-items:center;gap:0.4rem;">
          <span style="font-size:0.7rem;font-weight:600;color:#0d9488;">${fmt(t.leadCount)} leads</span>
          <button onclick="assignTerritory(${t.id})" style="font-size:0.65rem;padding:0.15rem 0.4rem;background:#ccfbf1;color:#0d9488;border:1px solid #5eead4;border-radius:4px;cursor:pointer;">Assign</button>
          <button onclick="deleteTerritory(${t.id})" style="font-size:0.65rem;padding:0.15rem 0.4rem;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;border-radius:4px;cursor:pointer;">Del</button>
        </div>
      </div>
    `).join('');
  } catch (err) { console.error('Territories error:', err); }
}

function showCreateTerritoryModal() {
  const modal = document.createElement('div');
  modal.id = 'territory-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:450px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#0d9488;">Create Territory</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Name</label>
        <input id="terr-name" type="text" placeholder="e.g., Southeast US" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">States (comma-separated)</label>
        <input id="terr-states" type="text" placeholder="e.g., FL, GA, NC, SC" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Cities (optional, comma-separated)</label>
        <input id="terr-cities" type="text" placeholder="e.g., Miami, Atlanta, Charlotte" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;display:block;margin-bottom:0.25rem;">Owner / Rep</label>
        <input id="terr-owner" type="text" placeholder="e.g., John" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('territory-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createTerritory()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#0d9488;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createTerritory() {
  const name = document.getElementById('terr-name').value.trim();
  if (!name) { alert('Name required'); return; }
  await fetch('/api/territories', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      states: document.getElementById('terr-states').value.trim(),
      cities: document.getElementById('terr-cities').value.trim(),
      owner: document.getElementById('terr-owner').value.trim(),
    }),
  });
  document.getElementById('territory-modal').remove();
  toast('Territory "' + esc(name) + '" created', 'success');
  loadTerritories();
}

async function assignTerritory(id) {
  const result = await fetch('/api/territories/' + id + '/assign', { method: 'POST' }).then(safeJSON);
  toast(`Assigned ${result.assigned} leads to territory`, 'success');
  loadTerritories();
}

async function deleteTerritory(id) {
  if (!confirm('Delete this territory?')) return;
  await fetch('/api/territories/' + id, { method: 'DELETE' });
  toast('Territory deleted', 'success');
  loadTerritories();
}

// === Source Attribution ===
async function loadSourceAttribution() {
  try {
    const sources = await fetch('/api/leads/source-attribution').then(safeJSON);
    const list = document.getElementById('source-attribution-list');
    if (sources.length === 0) { list.innerHTML = '<span style="font-size:0.72rem;color:#d946ef;">No source data yet.</span>'; return; }
    list.innerHTML = `
      <table style="width:100%;font-size:0.72rem;border-collapse:collapse;">
        <thead><tr style="text-align:left;color:#6b7280;border-bottom:1px solid #e5e7eb;">
          <th style="padding:0.2rem 0.3rem;">Source</th><th style="padding:0.2rem 0.3rem;">Leads</th><th style="padding:0.2rem 0.3rem;">Email%</th><th style="padding:0.2rem 0.3rem;">Phone%</th><th style="padding:0.2rem 0.3rem;">Avg Score</th>
        </tr></thead>
        <tbody>${sources.slice(0, 20).map(s => `
          <tr style="border-bottom:1px solid #f3f4f6;" onclick="document.getElementById('search-input').value='source:${esc(s.source)}';loadLeads();" onmouseover="this.style.background='#fdf4ff'" onmouseout="this.style.background=''">
            <td style="padding:0.25rem 0.3rem;font-weight:500;">${esc(s.source)}</td>
            <td style="padding:0.25rem 0.3rem;">${fmt(s.total)}</td>
            <td style="padding:0.25rem 0.3rem;"><span style="color:${s.emailRate > 50 ? '#15803d' : s.emailRate > 20 ? '#b45309' : '#dc2626'};">${s.emailRate}%</span></td>
            <td style="padding:0.25rem 0.3rem;"><span style="color:${s.phoneRate > 50 ? '#15803d' : s.phoneRate > 20 ? '#b45309' : '#dc2626'};">${s.phoneRate}%</span></td>
            <td style="padding:0.25rem 0.3rem;">${s.avg_score}</td>
          </tr>
        `).join('')}</tbody>
      </table>
    `;
  } catch (err) { console.error('Source attribution error:', err); }
}

// === Score Decay ===
async function loadDecayPreview() {
  try {
    const data = await fetch('/api/leads/decay-preview').then(safeJSON);
    document.getElementById('decay-preview-count').textContent = fmt(data.wouldDecay) + ' inactive';
  } catch (err) { console.error('Decay error:', err); }
}

async function runScoreDecay() {
  if (!confirm('Apply score decay? Leads inactive for 30+ days will lose 5 points.')) return;
  try {
    const result = await fetch('/api/leads/apply-decay', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decayPercent: 5, inactiveDays: 30 }),
    }).then(safeJSON);
    toast(`Decay applied to ${result.decayed} leads (-${result.decayPercent} pts)`, 'success');
    loadDecayPreview();
    loadScores();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Intent Signals ===
async function loadIntentSignals() {
  try {
    const signals = await fetch('/api/leads/intent-signals').then(safeJSON);
    const list = document.getElementById('intent-list');
    const highIntent = signals.filter(s => s.intentScore >= 30);
    document.getElementById('intent-high-count').textContent = highIntent.length + ' high-intent';

    if (signals.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#f9a8d4;">No intent signals yet. Build engagement and scrape more data.</span>';
      return;
    }
    list.innerHTML = signals.slice(0, 15).map(s => `
      <div style="display:flex;align-items:center;justify-content:space-between;padding:0.2rem 0;border-bottom:1px solid #fce7f3;font-size:0.72rem;">
        <div>
          <span style="font-weight:500;">${esc(s.first_name)} ${esc(s.last_name)}</span>
          <span style="color:#6b7280;margin-left:0.3rem;">${esc(s.firm_name || '')}${s.city ? ', ' + esc(s.city) : ''}</span>
        </div>
        <div style="display:flex;align-items:center;gap:0.3rem;">
          ${s.new_admission_pts > 0 ? '<span style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#fef3c7;border-radius:3px;color:#b45309;">New Admission</span>' : ''}
          ${s.firm_change_pts > 0 ? '<span style="font-size:0.6rem;padding:0.1rem 0.3rem;background:#fee2e2;border-radius:3px;color:#dc2626;">Firm Changed</span>' : ''}
          <span style="font-size:0.65rem;font-weight:600;color:${s.intentScore >= 50 ? '#be185d' : s.intentScore >= 30 ? '#ea580c' : '#6b7280'};">${s.intentScore}</span>
        </div>
      </div>
    `).join('');
  } catch (err) { console.error('Intent signals error:', err); }
}

// === Routing Rules ===
async function loadRoutingRules() {
  try {
    const rules = await fetch('/api/routing-rules').then(safeJSON);
    const list = document.getElementById('routing-list');
    if (rules.length === 0) {
      list.innerHTML = '<span style="font-size:0.72rem;color:#6ee7b7;">No routing rules. Create one to auto-process leads.</span>';
      return;
    }
    list.innerHTML = rules.map(r => {
      const conds = JSON.parse(r.conditions);
      return `
        <div style="display:flex;align-items:center;justify-content:space-between;padding:0.3rem 0.5rem;background:#fff;border:1px solid #a7f3d0;border-radius:6px;">
          <div>
            <strong style="font-size:0.78rem;color:#059669;">${esc(r.name)}</strong>
            <span style="font-size:0.68rem;color:#6b7280;margin-left:0.3rem;">
              IF ${conds.map(c => esc(c.field) + ' ' + esc(c.operator) + ' ' + esc(c.value || '')).join(' AND ')}
              THEN ${esc(r.action_type)}: ${esc(r.action_value)}
            </span>
          </div>
          <button onclick="deleteRoutingRule(${r.id})" style="font-size:0.65rem;padding:0.15rem 0.4rem;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;border-radius:4px;cursor:pointer;">Del</button>
        </div>
      `;
    }).join('');
  } catch (err) { console.error('Routing error:', err); }
}

function showCreateRoutingModal() {
  const modal = document.createElement('div');
  modal.id = 'routing-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:#fff;border-radius:12px;padding:1.5rem;width:500px;max-width:90vw;">
      <h3 style="margin:0 0 1rem;font-size:1rem;color:#059669;">Create Routing Rule</h3>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Rule Name</label>
        <input id="route-name" type="text" placeholder="e.g., Tag FL leads as Southeast" style="width:100%;padding:0.4rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.85rem;">
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Condition</label>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.3rem;">
          <select id="route-field" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
            <option value="state">State</option><option value="city">City</option><option value="practice_area">Practice Area</option>
            <option value="lead_score">Lead Score</option><option value="email">Email</option><option value="phone">Phone</option>
            <option value="pipeline_stage">Pipeline Stage</option><option value="email_type">Email Type</option>
          </select>
          <select id="route-op" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
            <option value="equals">Equals</option><option value="contains">Contains</option><option value="starts_with">Starts With</option>
            <option value="is_empty">Is Empty</option><option value="is_not_empty">Is Not Empty</option>
            <option value="greater_than">Greater Than</option><option value="less_than">Less Than</option>
          </select>
          <input id="route-value" type="text" placeholder="Value" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
        </div>
      </div>
      <div style="margin-bottom:0.75rem;">
        <label style="font-size:0.78rem;font-weight:600;">Action</label>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.3rem;">
          <select id="route-action-type" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
            <option value="tag">Add Tag</option><option value="pipeline">Move to Pipeline Stage</option>
          </select>
          <input id="route-action-value" type="text" placeholder="Tag name or stage" style="padding:0.3rem;border:1px solid #d1d5db;border-radius:4px;font-size:0.78rem;">
        </div>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:0.5rem;">
        <button onclick="document.getElementById('routing-modal').remove()" style="padding:0.4rem 0.8rem;border:1px solid #d1d5db;border-radius:6px;background:#fff;cursor:pointer;">Cancel</button>
        <button onclick="createRoutingRuleFromModal()" style="padding:0.4rem 0.8rem;border:none;border-radius:6px;background:#059669;color:#fff;cursor:pointer;">Create</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
}

async function createRoutingRuleFromModal() {
  const name = document.getElementById('route-name').value.trim();
  if (!name) { alert('Name required'); return; }
  await fetch('/api/routing-rules', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      conditions: [{ field: document.getElementById('route-field').value, operator: document.getElementById('route-op').value, value: document.getElementById('route-value').value.trim() }],
      actionType: document.getElementById('route-action-type').value,
      actionValue: document.getElementById('route-action-value').value.trim(),
    }),
  });
  document.getElementById('routing-modal').remove();
  toast('Routing rule created', 'success');
  loadRoutingRules();
}

async function deleteRoutingRule(id) {
  await fetch('/api/routing-rules/' + id, { method: 'DELETE' });
  toast('Rule deleted', 'success');
  loadRoutingRules();
}

async function executeRoutingRules() {
  try {
    const result = await fetch('/api/routing-rules/run', { method: 'POST' }).then(safeJSON);
    toast(`Routing: ${result.rulesRun} rules run, ${result.leadsAffected} leads affected`, 'success');
    refreshAll();
  } catch (err) { toast('Error: ' + err.message, 'error'); }
}

// === Completeness Heatmap ===
async function loadCompletenessHeatmap() {
  try {
    const data = await fetch('/api/leads/completeness-heatmap').then(safeJSON);
    const grid = document.getElementById('heatmap-grid');
    if (data.length === 0) { grid.innerHTML = '<span style="font-size:0.72rem;color:#93c5fd;">No data yet.</span>'; return; }

    const fields = ['email', 'phone', 'website', 'firm_name', 'practice_area', 'title'];
    const heatColor = (pct) => pct >= 60 ? '#dcfce7' : pct >= 30 ? '#fef3c7' : pct >= 10 ? '#fee2e2' : '#fef2f2';
    const textColor = (pct) => pct >= 60 ? '#15803d' : pct >= 30 ? '#b45309' : '#dc2626';

    grid.innerHTML = `
      <table style="width:100%;font-size:0.68rem;border-collapse:collapse;">
        <thead><tr style="color:#6b7280;">
          <th style="padding:0.2rem 0.3rem;text-align:left;">State</th><th style="padding:0.2rem;text-align:center;">Total</th>
          ${fields.map(f => `<th style="padding:0.2rem;text-align:center;">${f.replace('_', ' ')}</th>`).join('')}
        </tr></thead>
        <tbody>${data.slice(0, 15).map(s => `
          <tr style="border-bottom:1px solid #f3f4f6;">
            <td style="padding:0.2rem 0.3rem;font-weight:600;">${esc(s.state)}</td>
            <td style="padding:0.2rem;text-align:center;">${fmt(s.total)}</td>
            ${fields.map(f => `<td style="padding:0.2rem;text-align:center;background:${heatColor(s.fields[f])};color:${textColor(s.fields[f])};font-weight:500;">${s.fields[f]}%</td>`).join('')}
          </tr>
        `).join('')}</tbody>
      </table>
    `;

    // Enrichment recommendations
    const recs = await fetch('/api/leads/enrichment-recommendations').then(safeJSON);
    const recsDiv = document.getElementById('enrichment-recs');
    recsDiv.innerHTML = `
      <div style="display:flex;gap:0.5rem;flex-wrap:wrap;font-size:0.7rem;">
        <span style="padding:0.15rem 0.5rem;background:#dbeafe;border-radius:4px;color:#2563eb;">${fmt(recs.crawlCandidates)} leads with website, no email (crawl candidates)</span>
        <span style="padding:0.15rem 0.5rem;background:#dcfce7;border-radius:4px;color:#15803d;">${fmt(recs.domainCandidates)} leads with email, no website (derive domain)</span>
      </div>
    `;
  } catch (err) { console.error('Heatmap error:', err); }
}

// === Instantly Export ===
async function previewInstantlyExport() {
  try {
    const state = document.getElementById('instantly-state').value;
    const minScore = document.getElementById('instantly-min-score').value;
    const qs = new URLSearchParams();
    if (state) qs.set('state', state);
    if (minScore) qs.set('minScore', minScore);
    const data = await fetch('/api/leads/export/instantly/preview?' + qs.toString()).then(safeJSON);
    document.getElementById('instantly-preview').innerHTML = `
      <strong>${fmt(data.count)}</strong> leads ready for Instantly export
      ${data.sample.length > 0 ? '<br>Sample: ' + data.sample.slice(0, 3).map(s => esc(s.first_name) + ' ' + esc(s.last_name) + ' &lt;' + esc(s.email) + '&gt;').join(', ') : ''}
    `;
  } catch (err) { document.getElementById('instantly-preview').textContent = 'Error: ' + err.message; }
}

function downloadInstantlyExport() {
  const state = document.getElementById('instantly-state').value;
  const minScore = document.getElementById('instantly-min-score').value;
  const qs = new URLSearchParams();
  if (state) qs.set('state', state);
  if (minScore) qs.set('minScore', minScore);
  window.location.href = '/api/leads/export/instantly?' + qs.toString();
}

// Populate Instantly state dropdown
(async () => {
  try {
    const states = await fetch('/api/leads/coverage').then(safeJSON);
    const sel = document.getElementById('instantly-state');
    if (sel && states && Array.isArray(states)) {
      for (const s of states.sort((a, b) => b.count - a.count)) {
        const opt = document.createElement('option');
        opt.value = s.state; opt.textContent = s.state + ' (' + s.count + ')';
        sel.appendChild(opt);
      }
    }
  } catch (err) { console.warn('state-dropdown:', err.message); }
})();
