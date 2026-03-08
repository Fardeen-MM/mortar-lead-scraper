// ai-brain.js — AI intelligence: brain panel, insights, email writer, score explain, call script
// Dependencies: utils.js, app.js

// ============================================================
// === AI INTELLIGENCE ===
// ============================================================

let _aiBrainOpen = false;

function toggleAiBrain() {
  _aiBrainOpen = !_aiBrainOpen;
  document.getElementById('ai-brain-panel').style.display = _aiBrainOpen ? '' : 'none';
  if (_aiBrainOpen) document.getElementById('ai-brain-input').focus();
}

async function aiAsk(q) {
  const input = document.getElementById('ai-brain-input');
  const question = q || input.value.trim();
  if (!question) return;
  input.value = '';
  if (!_aiBrainOpen) toggleAiBrain();

  const msgs = document.getElementById('ai-brain-messages');
  // User message
  msgs.innerHTML += `<div style="align-self:flex-end;background:#4f46e5;color:white;padding:0.4rem 0.7rem;border-radius:8px 8px 2px 8px;font-size:0.78rem;max-width:85%;">${esc(question)}</div>`;
  // Thinking indicator
  const thinkId = 'ai-think-' + Date.now();
  msgs.innerHTML += `<div id="${thinkId}" style="align-self:flex-start;color:#818cf8;font-size:0.72rem;padding:0.3rem;">Thinking...</div>`;
  msgs.scrollTop = msgs.scrollHeight;

  try {
    const res = await fetch('/api/ai/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question }),
    });
    const data = await safeJSON(res);
    const el = document.getElementById(thinkId);
    if (!el) return;

    if (data.error) {
      el.outerHTML = `<div style="align-self:flex-start;background:rgba(239,68,68,0.15);color:#fca5a5;padding:0.5rem 0.7rem;border-radius:8px;font-size:0.75rem;max-width:90%;">${esc(data.error)}</div>`;
    } else {
      let html = `<div style="align-self:flex-start;background:rgba(99,102,241,0.12);border-radius:8px;padding:0.5rem 0.7rem;max-width:95%;font-size:0.75rem;color:#e0e7ff;">`;
      html += `<div style="color:#818cf8;font-size:0.65rem;margin-bottom:0.3rem;">${esc(data.explanation || '')} (${data.count} result${data.count !== 1 ? 's' : ''})</div>`;

      if (data.results && data.results.length > 0) {
        const keys = Object.keys(data.results[0]).filter(k => !['id','created_at','updated_at','bio','education','enrichment_steps','profile_url','notes','bar_number'].includes(k));
        const displayKeys = keys.slice(0, 8);

        if (data.results[0].count !== undefined || data.results[0].total !== undefined || data.results.length <= 5) {
          // Aggregate or small result — show as list
          html += `<div style="display:flex;flex-direction:column;gap:0.2rem;">`;
          for (const r of data.results.slice(0, 20)) {
            const vals = displayKeys.map(k => r[k] != null ? `<span style="color:#c7d2fe;">${esc(String(r[k]))}</span>` : '').filter(Boolean);
            html += `<div style="padding:0.15rem 0;border-bottom:1px solid rgba(99,102,241,0.15);font-size:0.72rem;">${vals.join(' &middot; ')}</div>`;
          }
          html += `</div>`;
        } else {
          // Table for multiple results
          html += `<div style="overflow-x:auto;max-height:200px;overflow-y:auto;"><table style="width:100%;border-collapse:collapse;font-size:0.68rem;"><thead><tr>`;
          for (const k of displayKeys) html += `<th style="padding:0.2rem 0.4rem;text-align:left;color:#a5b4fc;border-bottom:1px solid #4338ca;white-space:nowrap;">${esc(k)}</th>`;
          html += `</tr></thead><tbody>`;
          for (const r of data.results.slice(0, 30)) {
            html += `<tr>`;
            for (const k of displayKeys) {
              const v = r[k] != null ? String(r[k]).substring(0, 60) : '';
              const style = k === 'lead_score' ? `color:${parseInt(v) >= 70 ? '#4ade80' : parseInt(v) >= 40 ? '#fbbf24' : '#f87171'}` : 'color:#e0e7ff';
              html += `<td style="padding:0.2rem 0.4rem;border-bottom:1px solid rgba(67,56,202,0.3);${style};white-space:nowrap;">${esc(v)}</td>`;
            }
            html += `</tr>`;
          }
          html += `</tbody></table></div>`;
          if (data.count > 30) html += `<div style="color:#818cf8;font-size:0.65rem;margin-top:0.2rem;">... and ${data.count - 30} more</div>`;
        }
      }
      html += `</div>`;
      el.outerHTML = html;
    }
  } catch (err) {
    const el = document.getElementById(thinkId);
    if (el) el.outerHTML = `<div style="align-self:flex-start;color:#fca5a5;font-size:0.72rem;">Error: ${esc(err.message)}</div>`;
  }
  msgs.scrollTop = msgs.scrollHeight;
}

// AI Dashboard Brief
async function loadAiBrief() {
  const panel = document.getElementById('ai-brief-panel');
  const btn = document.getElementById('ai-brief-refresh');
  try {
    const statusRes = await fetch('/api/ai/status');
    const status = await safeJSON(statusRes);
    if (!status.available) { panel.style.display = 'none'; return; }

    panel.style.display = '';
    btn.textContent = 'Loading...';
    btn.disabled = true;
    const res = await fetch('/api/ai/dashboard-brief');
    const d = await safeJSON(res);
    if (d.error) {
      if (d.error.includes('credit') || d.error.includes('depleted')) {
        document.getElementById('ai-brief-headline').textContent = 'AI Ready — Add API credits to activate';
        document.getElementById('ai-brief-highlights').innerHTML = '<div style="margin-bottom:0.2rem;">&#x2022; AI features are fully built and ready</div><div>&#x2022; Add credits at console.anthropic.com</div>';
        document.getElementById('ai-brief-opportunities').innerHTML = '<div>&#x2714; Ask Brain: natural language queries</div><div>&#x2714; AI Lead Insights per attorney</div><div>&#x2714; AI Email Writer</div>';
        document.getElementById('ai-brief-recommendation').textContent = 'Top up Anthropic API credits to unlock AI intelligence across the entire app.';
        document.getElementById('ai-brief-warnings').innerHTML = '';
        return;
      }
      throw new Error(d.error);
    }

    document.getElementById('ai-brief-headline').textContent = d.headline || 'AI brief loaded';
    document.getElementById('ai-brief-highlights').innerHTML = (d.highlights || []).map(h => `<div style="margin-bottom:0.2rem;">&#x2022; ${esc(h)}</div>`).join('');
    document.getElementById('ai-brief-opportunities').innerHTML = (d.opportunities || []).map(o => `<div style="margin-bottom:0.2rem;">&#x2714; ${esc(o)}</div>`).join('');
    document.getElementById('ai-brief-recommendation').textContent = d.recommendation || '';
    document.getElementById('ai-brief-warnings').innerHTML = (d.warnings || []).map(w => `<div>&#x26A0; ${esc(w)}</div>`).join('');
  } catch (err) {
    panel.style.display = '';
    document.getElementById('ai-brief-headline').textContent = err.message.includes('credit') ? 'AI Ready — Add API credits to activate' : 'AI brief unavailable';
    console.error('AI brief error:', err);
  } finally {
    btn.textContent = 'Refresh';
    btn.disabled = false;
  }
}

// AI Practice Area Classifier
async function runAiClassify() {
  const status = document.getElementById('ai-classify-status');
  status.style.display = '';
  status.textContent = 'Starting AI practice area classification...';
  try {
    await fetch('/api/ai/classify-practice-areas', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
    // Poll for status
    const poll = setInterval(async () => {
      try {
        const r = await fetch('/api/ai/classify-practice-areas/status');
        const d = await safeJSON(r);
        if (d.progress) {
          status.textContent = d.progress.done
            ? `Done! Tagged ${d.progress.updated} leads with practice areas.`
            : `Classifying... ${d.progress.processed}/${d.progress.total} (${d.progress.updated} tagged)`;
          if (d.progress.done || d.progress.error) {
            clearInterval(poll);
            if (d.progress.error) status.textContent = 'Error: ' + d.progress.error;
            setTimeout(() => refreshAll(), 2000);
          }
        }
      } catch {}
    }, 2000);
  } catch (err) {
    status.textContent = 'Error: ' + err.message;
  }
}

// AI Data Quality Audit
async function runAiAudit() {
  const container = document.getElementById('ai-audit-results');
  container.style.display = '';
  container.innerHTML = '<div style="font-size:0.72rem;color:#a5b4fc;">Running AI quality audit...</div>';
  try {
    const res = await fetch('/api/ai/audit-quality', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ limit: 50 }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);

    let html = `<div style="font-size:0.78rem;color:#e0e7ff;font-weight:600;margin-bottom:0.3rem;">Quality Score: <span style="color:${d.score >= 70 ? '#4ade80' : d.score >= 40 ? '#fbbf24' : '#f87171'}">${d.score || '?'}/100</span></div>`;
    html += `<div style="font-size:0.72rem;color:#a5b4fc;margin-bottom:0.4rem;">${esc(d.summary || '')}</div>`;
    if (d.issues && d.issues.length > 0) {
      html += `<div style="max-height:150px;overflow-y:auto;">`;
      for (const issue of d.issues.slice(0, 15)) {
        const sev = issue.severity === 'high' ? '#ef4444' : issue.severity === 'medium' ? '#f59e0b' : '#6b7280';
        html += `<div style="padding:0.2rem 0;border-bottom:1px solid rgba(99,102,241,0.15);font-size:0.7rem;">
          <span style="color:${sev};font-weight:600;">${esc(issue.severity || '?').toUpperCase()}</span>
          <span style="color:#c7d2fe;cursor:pointer;" onclick="openLeadById(${issue.id})">Lead #${issue.id}</span>:
          <span style="color:#e0e7ff;">${esc(issue.field)}</span> — ${esc(issue.issue)}
          <span style="color:#818cf8;font-style:italic;"> ${esc(issue.suggestion || '')}</span>
        </div>`;
      }
      html += `</div>`;
    }
    container.innerHTML = html;
  } catch (err) {
    container.innerHTML = `<div style="font-size:0.72rem;color:#fca5a5;">Error: ${esc(err.message)}</div>`;
  }
}

// AI Lead Insights (called from lead detail modal)
async function loadAiInsights(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  container.style.display = '';
  container.innerHTML = '<div style="color:#818cf8;font-size:0.75rem;">Analyzing lead with AI...</div>';
  try {
    const res = await fetch('/api/ai/lead-insights', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const i = d.insights;
    const priorityColor = i.priority === 'high' ? '#4ade80' : i.priority === 'medium' ? '#fbbf24' : '#6b7280';

    let html = `<div style="background:linear-gradient(135deg,#1e1b4b,#312e81);border-radius:8px;padding:0.7rem;color:#e0e7ff;">`;
    html += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem;">
      <span style="font-weight:700;color:#c7d2fe;font-size:0.82rem;">AI Insights</span>
      <span style="background:${priorityColor}20;color:${priorityColor};padding:0.1rem 0.4rem;border-radius:4px;font-size:0.68rem;font-weight:600;">${esc((i.priority || '').toUpperCase())} PRIORITY</span>
    </div>`;
    html += `<div style="font-size:0.74rem;color:#a5b4fc;margin-bottom:0.4rem;">${esc(i.quality_assessment || '')}</div>`;
    html += `<div style="font-size:0.74rem;margin-bottom:0.4rem;"><strong style="color:#c7d2fe;">Outreach Angle:</strong> ${esc(i.outreach_angle || '')}</div>`;
    html += `<div style="background:rgba(99,102,241,0.15);border-radius:6px;padding:0.5rem;margin-bottom:0.4rem;">
      <div style="font-size:0.68rem;color:#a5b4fc;margin-bottom:0.2rem;">PERSONALIZED OPENER</div>
      <div style="font-size:0.78rem;color:#e0e7ff;font-style:italic;line-height:1.4;">"${esc(i.personalized_opener || '')}"</div>
      <button onclick="navigator.clipboard.writeText('${esc((i.personalized_opener || '').replace(/'/g, "\\'"))}');this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500)" style="margin-top:0.3rem;font-size:0.62rem;padding:0.1rem 0.4rem;background:#4f46e5;color:white;border:none;border-radius:3px;cursor:pointer;">Copy</button>
    </div>`;
    html += `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.4rem;margin-bottom:0.4rem;">
      <div><div style="font-size:0.65rem;color:#a5b4fc;margin-bottom:0.15rem;">Pain Points</div>${(i.pain_points || []).map(p => `<div style="font-size:0.7rem;color:#fbbf24;">&#x2022; ${esc(p)}</div>`).join('')}</div>
      <div><div style="font-size:0.65rem;color:#a5b4fc;margin-bottom:0.15rem;">Recommended Services</div>${(i.recommended_services || []).map(s => `<div style="font-size:0.7rem;color:#4ade80;">&#x2713; ${esc(s)}</div>`).join('')}</div>
    </div>`;
    html += `<div style="font-size:0.65rem;color:#a5b4fc;margin-bottom:0.15rem;">Next Steps</div>`;
    html += (i.next_steps || []).map(s => `<div style="font-size:0.7rem;color:#c7d2fe;">&#x2192; ${esc(s)}</div>`).join('');
    html += `<button onclick="loadAiEmail(${leadId})" style="margin-top:0.5rem;width:100%;padding:0.4rem;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:white;border:none;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600;">&#x2709; Generate Cold Email</button>`;
    html += `</div>`;
    container.innerHTML = html;
  } catch (err) {
    container.innerHTML = `<div style="color:#fca5a5;font-size:0.75rem;">AI Error: ${esc(err.message)}</div>`;
  }
}

// AI Email Writer (called from lead detail)
async function loadAiEmail(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  const prev = container.innerHTML;
  container.innerHTML += '<div id="ai-email-loading" style="color:#818cf8;font-size:0.75rem;margin-top:0.4rem;">Generating personalized email...</div>';
  try {
    const res = await fetch('/api/ai/write-email', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const e = d.email;
    const loadEl = document.getElementById('ai-email-loading');
    if (!loadEl) return;

    let html = `<div style="background:rgba(99,102,241,0.1);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
    html += `<div style="font-size:0.68rem;color:#a5b4fc;margin-bottom:0.3rem;">AI-GENERATED EMAIL</div>`;
    html += `<div style="font-size:0.78rem;color:#c7d2fe;font-weight:600;margin-bottom:0.3rem;">Subject: ${esc(e.subject || '')}</div>`;
    html += `<div style="font-size:0.76rem;color:#e0e7ff;white-space:pre-line;line-height:1.5;margin-bottom:0.3rem;">${esc(e.body || '')}</div>`;
    if (e.ps) html += `<div style="font-size:0.72rem;color:#a5b4fc;font-style:italic;">P.S. ${esc(e.ps)}</div>`;
    html += `<div style="display:flex;gap:0.3rem;margin-top:0.4rem;">`;
    const fullEmail = `Subject: ${e.subject || ''}\n\n${e.body || ''}${e.ps ? '\n\nP.S. ' + e.ps : ''}`;
    html += `<button onclick="navigator.clipboard.writeText(${JSON.stringify(fullEmail).replace(/'/g, '&#39;')});this.textContent='Copied!';setTimeout(()=>this.textContent='Copy Email',1500)" style="padding:0.3rem 0.6rem;background:#4f46e5;color:white;border:none;border-radius:4px;cursor:pointer;font-size:0.72rem;">Copy Email</button>`;
    html += `<button onclick="loadAiEmail(${leadId})" style="padding:0.3rem 0.6rem;background:rgba(99,102,241,0.2);color:#c7d2fe;border:1px solid #4f46e5;border-radius:4px;cursor:pointer;font-size:0.72rem;">Regenerate</button>`;
    html += `</div>`;
    if (e.follow_up) {
      html += `<div style="margin-top:0.4rem;padding-top:0.3rem;border-top:1px solid rgba(99,102,241,0.2);">
        <div style="font-size:0.65rem;color:#818cf8;">FOLLOW-UP (3 days later)</div>
        <div style="font-size:0.72rem;color:#a5b4fc;margin-top:0.15rem;">${esc(e.follow_up)}</div>
      </div>`;
    }
    html += `</div>`;
    loadEl.outerHTML = html;
  } catch (err) {
    const loadEl = document.getElementById('ai-email-loading');
    if (loadEl) loadEl.outerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Email generation error: ${esc(err.message)}</div>`;
  }
}

// --- AI Score Explanation ---
async function loadAiScoreExplain(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  container.innerHTML += '<div id="ai-score-loading" style="color:#818cf8;font-size:0.75rem;margin-top:0.4rem;">Analyzing score factors...</div>';
  try {
    const res = await fetch('/api/ai/explain-score', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const loadEl = document.getElementById('ai-score-loading');
    if (!loadEl) return;
    let html = `<div style="background:rgba(99,102,241,0.1);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
    html += `<div style="font-size:0.68rem;color:#a5b4fc;margin-bottom:0.3rem;">AI SCORE ANALYSIS &middot; Grade: <strong>${esc(d.grade || '?')}</strong></div>`;
    html += `<div style="font-size:0.76rem;color:#e0e7ff;margin-bottom:0.3rem;line-height:1.4;">${esc(d.explanation || '')}</div>`;
    if (d.strengths && d.strengths.length > 0) {
      html += `<div style="font-size:0.68rem;color:#86efac;margin-bottom:0.2rem;">Strengths:</div>`;
      html += d.strengths.map(s => `<div style="font-size:0.72rem;color:#a5b4fc;padding-left:0.5rem;">&#x2713; ${esc(s)}</div>`).join('');
    }
    if (d.gaps && d.gaps.length > 0) {
      html += `<div style="font-size:0.68rem;color:#fbbf24;margin-top:0.2rem;margin-bottom:0.2rem;">Gaps:</div>`;
      html += d.gaps.map(g => `<div style="font-size:0.72rem;color:#a5b4fc;padding-left:0.5rem;">&#x2022; ${esc(g)}</div>`).join('');
    }
    if (d.suggestions && d.suggestions.length > 0) {
      html += `<div style="font-size:0.68rem;color:#c7d2fe;margin-top:0.2rem;margin-bottom:0.2rem;">Suggestions:</div>`;
      html += d.suggestions.map(s => `<div style="font-size:0.72rem;color:#e0e7ff;padding-left:0.5rem;">&#x2192; ${esc(s)}</div>`).join('');
    }
    html += `</div>`;
    loadEl.outerHTML = html;
  } catch (err) {
    const loadEl = document.getElementById('ai-score-loading');
    if (loadEl) loadEl.outerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Score analysis error: ${esc(err.message)}</div>`;
  }
}

// --- AI Talking Points ---
async function loadAiTalkingPoints(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  container.innerHTML += '<div id="ai-tp-loading" style="color:#818cf8;font-size:0.75rem;margin-top:0.4rem;">Generating talking points...</div>';
  try {
    const res = await fetch('/api/ai/talking-points', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const loadEl = document.getElementById('ai-tp-loading');
    if (!loadEl) return;
    let html = `<div style="background:rgba(99,102,241,0.1);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
    html += `<div style="font-size:0.68rem;color:#a5b4fc;margin-bottom:0.3rem;">AI TALKING POINTS</div>`;
    if (d.icebreaker) html += `<div style="font-size:0.78rem;color:#fbbf24;font-weight:600;margin-bottom:0.3rem;">Icebreaker: ${esc(d.icebreaker)}</div>`;
    if (d.points && d.points.length > 0) {
      html += d.points.map((p, i) => `<div style="font-size:0.76rem;color:#e0e7ff;padding:0.15rem 0;line-height:1.4;">${i + 1}. ${esc(p)}</div>`).join('');
    }
    if (d.cta) html += `<div style="font-size:0.72rem;color:#86efac;margin-top:0.3rem;font-weight:500;">CTA: ${esc(d.cta)}</div>`;
    html += `<button onclick="navigator.clipboard.writeText('${esc(JSON.stringify(d).replace(/'/g, "\\'"))}');toast('Copied!','success');" style="margin-top:0.3rem;padding:0.2rem 0.5rem;background:#4f46e5;color:white;border:none;border-radius:4px;cursor:pointer;font-size:0.68rem;">Copy All</button>`;
    html += `</div>`;
    loadEl.outerHTML = html;
  } catch (err) {
    const loadEl = document.getElementById('ai-tp-loading');
    if (loadEl) loadEl.outerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Talking points error: ${esc(err.message)}</div>`;
  }
}

// --- AI Call Script Generator ---
async function loadAiCallScript(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  container.innerHTML += '<div id="ai-call-loading" style="color:#10b981;font-size:0.75rem;margin-top:0.4rem;">Generating call script...</div>';
  try {
    const res = await fetch('/api/ai/call-script', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const loadEl = document.getElementById('ai-call-loading');
    if (!loadEl) return;
    let html = `<div style="background:rgba(16,185,129,0.1);border:1px solid rgba(16,185,129,0.2);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
    html += `<div style="font-size:0.68rem;color:#6ee7b7;margin-bottom:0.4rem;font-weight:600;">COLD CALL SCRIPT</div>`;
    if (d.opener) html += `<div style="margin-bottom:0.4rem;"><div style="font-size:0.62rem;color:#34d399;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Opener</div><div style="font-size:0.76rem;color:#e0e7ff;line-height:1.4;">${esc(d.opener)}</div></div>`;
    if (d.value_prop) html += `<div style="margin-bottom:0.4rem;"><div style="font-size:0.62rem;color:#34d399;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Value Prop</div><div style="font-size:0.76rem;color:#e0e7ff;line-height:1.4;">${esc(d.value_prop)}</div></div>`;
    if (d.discovery_questions && d.discovery_questions.length > 0) {
      html += `<div style="margin-bottom:0.4rem;"><div style="font-size:0.62rem;color:#34d399;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Discovery Questions</div>`;
      html += d.discovery_questions.map(q => `<div style="font-size:0.74rem;color:#c7d2fe;padding:0.1rem 0 0.1rem 0.5rem;">&#x2022; ${esc(q)}</div>`).join('');
      html += `</div>`;
    }
    if (d.objection_responses) {
      html += `<div style="margin-bottom:0.4rem;"><div style="font-size:0.62rem;color:#fbbf24;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Objection Responses</div>`;
      Object.entries(d.objection_responses).forEach(([key, val]) => {
        const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        html += `<div style="font-size:0.72rem;padding:0.15rem 0 0.15rem 0.5rem;border-left:2px solid #fbbf24;margin:0.15rem 0;"><strong style="color:#fcd34d;">"${esc(label)}"</strong><br><span style="color:#e0e7ff;">${esc(val)}</span></div>`;
      });
      html += `</div>`;
    }
    if (d.closing) html += `<div style="margin-bottom:0.4rem;"><div style="font-size:0.62rem;color:#34d399;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Closing</div><div style="font-size:0.76rem;color:#86efac;font-weight:500;line-height:1.4;">${esc(d.closing)}</div></div>`;
    if (d.voicemail) html += `<div style="margin-bottom:0.3rem;"><div style="font-size:0.62rem;color:#a78bfa;font-weight:600;text-transform:uppercase;margin-bottom:0.1rem;">Voicemail</div><div style="font-size:0.74rem;color:#c4b5fd;line-height:1.4;">${esc(d.voicemail)}</div></div>`;
    html += `<button onclick="navigator.clipboard.writeText(document.getElementById('ai-call-text').innerText);toast('Script copied!','success');" style="margin-top:0.3rem;padding:0.2rem 0.5rem;background:#059669;color:white;border:none;border-radius:4px;cursor:pointer;font-size:0.68rem;">Copy Script</button>`;
    html += `</div><div id="ai-call-text" style="display:none;">${esc(d.opener || '')}\\n${esc(d.value_prop || '')}\\n${(d.discovery_questions || []).map(q => '- ' + q).join('\\n')}\\n${esc(d.closing || '')}\\n\\nVoicemail: ${esc(d.voicemail || '')}</div>`;
    loadEl.outerHTML = html;
  } catch (err) {
    const loadEl = document.getElementById('ai-call-loading');
    if (loadEl) loadEl.outerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Call script error: ${esc(err.message)}</div>`;
  }
}

// --- AI Win Probability ---
async function loadAiWinProb(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  container.innerHTML += '<div id="ai-win-loading" style="color:#f59e0b;font-size:0.75rem;margin-top:0.4rem;">Calculating win probability...</div>';
  try {
    const res = await fetch('/api/ai/win-probability', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const loadEl = document.getElementById('ai-win-loading');
    if (!loadEl) return;
    const prob = d.probability || 0;
    const tierColors = { hot: '#ef4444', warm: '#f59e0b', cool: '#3b82f6', cold: '#94a3b8' };
    const tierColor = tierColors[d.tier] || '#94a3b8';
    let html = `<div style="background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.2);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
    html += `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.4rem;">`;
    html += `<div style="font-size:0.68rem;color:#fbbf24;font-weight:600;">WIN PROBABILITY</div>`;
    html += `<div style="display:flex;align-items:center;gap:0.4rem;">`;
    html += `<span style="font-size:1.3rem;font-weight:700;color:${tierColor};">${prob}%</span>`;
    html += `<span style="padding:0.1rem 0.4rem;border-radius:10px;font-size:0.62rem;font-weight:600;background:${tierColor};color:white;text-transform:uppercase;">${esc(d.tier || '?')}</span>`;
    html += `</div></div>`;
    // Progress bar
    html += `<div style="height:6px;background:rgba(255,255,255,0.1);border-radius:3px;overflow:hidden;margin-bottom:0.4rem;"><div style="height:100%;width:${prob}%;background:${tierColor};border-radius:3px;transition:width 0.5s;"></div></div>`;
    if (d.recommended_channel) html += `<div style="font-size:0.72rem;color:#e0e7ff;margin-bottom:0.3rem;">Best channel: <strong style="color:#fbbf24;">${esc(d.recommended_channel)}</strong>${d.best_time ? ` &middot; ${esc(d.best_time)}` : ''}</div>`;
    if (d.factors_positive && d.factors_positive.length > 0) {
      html += d.factors_positive.map(f => `<div style="font-size:0.72rem;color:#86efac;padding:0.05rem 0;">&#x2191; ${esc(f)}</div>`).join('');
    }
    if (d.factors_negative && d.factors_negative.length > 0) {
      html += d.factors_negative.map(f => `<div style="font-size:0.72rem;color:#fca5a5;padding:0.05rem 0;">&#x2193; ${esc(f)}</div>`).join('');
    }
    html += `</div>`;
    loadEl.outerHTML = html;
  } catch (err) {
    const loadEl = document.getElementById('ai-win-loading');
    if (loadEl) loadEl.outerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Win prob error: ${esc(err.message)}</div>`;
  }
}

// --- AI Reply Analyzer ---
function openReplyAnalyzer(leadId) {
  const container = document.getElementById('ai-lead-insights');
  if (!container) return;
  let html = `<div id="reply-analyzer" style="background:rgba(236,72,153,0.1);border:1px solid rgba(236,72,153,0.2);border-radius:8px;padding:0.7rem;margin-top:0.4rem;">`;
  html += `<div style="font-size:0.68rem;color:#f9a8d4;font-weight:600;margin-bottom:0.3rem;">REPLY ANALYZER</div>`;
  html += `<textarea id="reply-text-input" placeholder="Paste the prospect's email reply here..." style="width:100%;min-height:80px;padding:0.4rem;border:1px solid rgba(236,72,153,0.3);border-radius:6px;background:rgba(0,0,0,0.2);color:#e0e7ff;font-size:0.75rem;resize:vertical;font-family:inherit;"></textarea>`;
  html += `<button onclick="analyzeReplyText(${leadId})" style="margin-top:0.3rem;padding:0.3rem 0.7rem;background:#db2777;color:white;border:none;border-radius:5px;cursor:pointer;font-size:0.72rem;font-weight:500;">Analyze Reply</button>`;
  html += `<div id="reply-analysis-result"></div>`;
  html += `</div>`;
  container.innerHTML += html;
  document.getElementById('reply-text-input').focus();
}

async function analyzeReplyText(leadId) {
  const text = document.getElementById('reply-text-input').value.trim();
  if (!text) { toast('Paste a reply first', 'error'); return; }
  const resultEl = document.getElementById('reply-analysis-result');
  resultEl.innerHTML = '<div style="color:#f9a8d4;font-size:0.72rem;margin-top:0.3rem;">Analyzing reply...</div>';
  try {
    const res = await fetch('/api/ai/analyze-reply', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ replyText: text, leadId }),
    });
    const d = await safeJSON(res);
    if (d.error) throw new Error(d.error);
    const interestColors = { high: '#22c55e', medium: '#f59e0b', low: '#f97316', none: '#ef4444' };
    const sentColors = { positive: '#22c55e', neutral: '#94a3b8', negative: '#ef4444', mixed: '#f59e0b' };
    let html = `<div style="margin-top:0.4rem;">`;
    // Interest + Sentiment badges
    html += `<div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin-bottom:0.3rem;">`;
    html += `<span style="padding:0.1rem 0.5rem;border-radius:10px;font-size:0.65rem;font-weight:600;background:${interestColors[d.interest] || '#94a3b8'};color:white;">Interest: ${esc(d.interest || '?')}</span>`;
    html += `<span style="padding:0.1rem 0.5rem;border-radius:10px;font-size:0.65rem;font-weight:600;background:${sentColors[d.sentiment] || '#94a3b8'};color:white;">${esc(d.sentiment || '?')}</span>`;
    html += `<span style="padding:0.1rem 0.5rem;border-radius:10px;font-size:0.65rem;font-weight:600;background:rgba(99,102,241,0.3);color:#a5b4fc;">${esc((d.category || '').replace(/_/g, ' '))}</span>`;
    if (d.urgency) html += `<span style="padding:0.1rem 0.5rem;border-radius:10px;font-size:0.65rem;font-weight:600;background:${d.urgency === 'high' ? '#ef4444' : d.urgency === 'medium' ? '#f59e0b' : '#6b7280'};color:white;">Urgency: ${esc(d.urgency)}</span>`;
    html += `</div>`;
    if (d.summary) html += `<div style="font-size:0.76rem;color:#e0e7ff;line-height:1.4;margin-bottom:0.3rem;">${esc(d.summary)}</div>`;
    if (d.pain_points && d.pain_points.length > 0) {
      html += `<div style="font-size:0.62rem;color:#86efac;font-weight:600;margin-bottom:0.1rem;">PAIN POINTS</div>`;
      html += d.pain_points.map(p => `<div style="font-size:0.72rem;color:#a5b4fc;padding-left:0.5rem;">&#x2022; ${esc(p)}</div>`).join('');
    }
    if (d.objections && d.objections.length > 0) {
      html += `<div style="font-size:0.62rem;color:#fbbf24;font-weight:600;margin-top:0.2rem;margin-bottom:0.1rem;">OBJECTIONS</div>`;
      html += d.objections.map(o => `<div style="font-size:0.72rem;color:#fcd34d;padding-left:0.5rem;">&#x26A0; ${esc(o)}</div>`).join('');
    }
    if (d.next_action) html += `<div style="font-size:0.74rem;color:#22c55e;font-weight:600;margin-top:0.3rem;padding:0.3rem;background:rgba(34,197,94,0.1);border-radius:4px;">Next: ${esc(d.next_action)}</div>`;
    if (d.suggested_reply_tone) html += `<div style="font-size:0.68rem;color:#94a3b8;margin-top:0.2rem;">Tone: ${esc(d.suggested_reply_tone)}</div>`;
    html += `</div>`;
    resultEl.innerHTML = html;
  } catch (err) {
    resultEl.innerHTML = `<div style="color:#fca5a5;font-size:0.72rem;margin-top:0.3rem;">Reply analysis error: ${esc(err.message)}</div>`;
  }
}
