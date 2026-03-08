// geo-map.js — Geographic views: coverage map, health score, growth chart, geo map, world view
// Dependencies: utils.js, app.js

// --- Coverage Map (Visual Grid) ---
async function loadCoverageMap() {
  try {
    const res = await fetch('/api/leads/coverage');
    const data = await safeJSON(res);
    const container = document.getElementById('coverage-map');
    if (!container || !data.length) return;

    // Group by country
    const countries = {};
    data.forEach(s => {
      if (!countries[s.country]) countries[s.country] = [];
      countries[s.country].push(s);
    });

    // Country display order and flag
    const countryOrder = ['US','CA','UK','AU','NZ','FR','IE','IT','DE','HK','SG'];
    const countryFlags = { US:'\u{1F1FA}\u{1F1F8}', CA:'\u{1F1E8}\u{1F1E6}', UK:'\u{1F1EC}\u{1F1E7}', AU:'\u{1F1E6}\u{1F1FA}', NZ:'\u{1F1F3}\u{1F1FF}', FR:'\u{1F1EB}\u{1F1F7}', IE:'\u{1F1EE}\u{1F1EA}', IT:'\u{1F1EE}\u{1F1F9}', DE:'\u{1F1E9}\u{1F1EA}', HK:'\u{1F1ED}\u{1F1F0}', SG:'\u{1F1F8}\u{1F1EC}' };

    // Color scale based on lead count
    const maxTotal = Math.max(...data.map(s => s.total));
    function cellColor(total, emailPct) {
      if (total === 0) return { bg: 'var(--bg-secondary, #f3f4f6)', border: 'var(--border, #e5e7eb)', text: 'var(--text-muted, #9ca3af)' };
      const intensity = Math.min(total / Math.max(maxTotal * 0.5, 1), 1);
      if (emailPct >= 30) return { bg: `rgba(34,197,94,${0.15 + intensity * 0.5})`, border: '#22c55e', text: '#166534' };
      if (emailPct > 0 || total > 100) return { bg: `rgba(59,130,246,${0.15 + intensity * 0.5})`, border: '#3b82f6', text: '#1e40af' };
      return { bg: `rgba(245,158,11,${0.15 + intensity * 0.4})`, border: '#f59e0b', text: '#92400e' };
    }

    let html = '';
    const sortedCountries = Object.keys(countries).sort((a, b) => {
      const ai = countryOrder.indexOf(a), bi = countryOrder.indexOf(b);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });

    sortedCountries.forEach(country => {
      const states = countries[country].sort((a, b) => b.total - a.total);
      const flag = countryFlags[country] || '';
      html += `<div style="width:100%;margin:0.5rem 0 0.25rem;display:flex;align-items:center;gap:0.3rem;">
        <span style="font-size:0.85rem;">${flag}</span>
        <span style="font-size:0.75rem;font-weight:600;color:var(--text-primary,#111);">${country}</span>
        <span style="font-size:0.7rem;color:var(--text-muted,#9ca3af);">${states.reduce((s,x) => s + x.total, 0).toLocaleString()} leads</span>
      </div>`;
      states.forEach(s => {
        const c = cellColor(s.total, s.email_pct);
        const shortState = s.state.replace(country + '-', '');
        const tooltip = `${s.state}: ${s.total.toLocaleString()} leads | Email: ${s.email_pct}% | Phone: ${s.phone_pct}% | Web: ${s.website_pct}%`;
        html += `<div class="cov-cell" title="${esc(tooltip)}" onclick="openStateModal('${esc(s.state)}')"
          style="display:inline-flex;flex-direction:column;align-items:center;justify-content:center;
          width:56px;height:48px;border-radius:6px;cursor:pointer;
          background:${c.bg};border:1.5px solid ${c.border};transition:transform 0.15s;"
          onmouseenter="this.style.transform='scale(1.08)'" onmouseleave="this.style.transform='scale(1)'">
          <span style="font-size:0.72rem;font-weight:700;color:${c.text};">${esc(shortState)}</span>
          <span style="font-size:0.6rem;color:${c.text};opacity:0.8;">${s.total >= 1000 ? (s.total/1000).toFixed(1)+'k' : s.total}</span>
        </div>`;
      });
    });

    // Legend
    html += `<div style="width:100%;margin-top:0.75rem;display:flex;gap:1rem;font-size:0.65rem;color:var(--text-muted,#9ca3af);align-items:center;">
      <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:rgba(34,197,94,0.5);margin-right:3px;vertical-align:middle;"></span>Has email</span>
      <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:rgba(59,130,246,0.5);margin-right:3px;vertical-align:middle;"></span>100+ leads</span>
      <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:rgba(245,158,11,0.4);margin-right:3px;vertical-align:middle;"></span>Sparse data</span>
      <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:var(--bg-secondary,#f3f4f6);border:1px solid var(--border,#e5e7eb);margin-right:3px;vertical-align:middle;"></span>No data</span>
    </div>`;

    container.innerHTML = html;
  } catch (err) {
    console.error('Failed to load coverage map:', err);
  }
}

// --- Database Health Score ---
async function loadHealthScore() {
  try {
    const res = await fetch('/api/leads/health-score');
    const d = await safeJSON(res);
    const el = document.getElementById('pb-health');
    if (!el) return;
    const colors = { A: '#22c55e', B: '#3b82f6', C: '#f59e0b', D: '#ef4444', F: '#991b1b' };
    el.textContent = `Health: ${d.grade} (${d.score})`;
    el.style.color = colors[d.grade] || '#9ca3af';
  } catch (err) { console.warn('loadHealthScore:', err.message); }
}

// --- Similar Leads ---
async function loadSimilarLeads(leadId) {
  try {
    const res = await fetch(`/api/leads/similar/${leadId}`);
    const d = await safeJSON(res);

    let html = '';
    if (d.sameFirm.length > 0) {
      html += `<div style="margin-top:0.75rem;border-top:1px solid var(--border);padding-top:0.5rem;">
        <div style="font-size:0.82rem;font-weight:600;color:var(--text-muted);margin-bottom:0.3rem;">Same Firm</div>
        <div style="display:flex;flex-wrap:wrap;gap:0.3rem;">
          ${d.sameFirm.map(l => `<span style="font-size:0.68rem;padding:0.15rem 0.4rem;background:var(--bg-secondary,#f3f4f6);border:1px solid var(--border,#e5e7eb);border-radius:4px;cursor:pointer;"
            onclick="showLeadDetail(${JSON.stringify(l).replace(/"/g, '&quot;')})">${esc(l.first_name)} ${esc(l.last_name)} ${l.email ? '\u2709' : ''}</span>`).join('')}
        </div>
      </div>`;
    }
    if (d.sameCity.length > 0) {
      html += `<div style="margin-top:0.5rem;">
        <div style="font-size:0.82rem;font-weight:600;color:var(--text-muted);margin-bottom:0.3rem;">Same City</div>
        <div style="display:flex;flex-wrap:wrap;gap:0.3rem;">
          ${d.sameCity.map(l => `<span style="font-size:0.68rem;padding:0.15rem 0.4rem;background:var(--bg-secondary,#f3f4f6);border:1px solid var(--border,#e5e7eb);border-radius:4px;cursor:pointer;"
            onclick="showLeadDetail(${JSON.stringify(l).replace(/"/g, '&quot;')})">${esc(l.first_name)} ${esc(l.last_name)} (${esc(l.firm_name || '').substring(0, 20)}) ${l.email ? '\u2709' : ''}</span>`).join('')}
        </div>
      </div>`;
    }
    return html;
  } catch {
    return '';
  }
}

// --- Firm Intelligence ---
async function loadFirmIntel() {
  try {
    const res = await fetch('/api/leads/top-firms?limit=12');
    const data = await safeJSON(res);
    const container = document.getElementById('firm-intel');
    if (!container) return;

    if (!data.length) {
      container.innerHTML = '<p style="font-size:0.75rem;color:var(--text-muted);text-align:center;grid-column:1/-1;padding:0.5rem;">No firm data yet.</p>';
      return;
    }

    const maxCount = data[0].lead_count;
    container.innerHTML = data.map(f => {
      const emailPct = Math.round(100 * f.with_email / f.lead_count);
      const phonePct = Math.round(100 * f.with_phone / f.lead_count);
      const states = (f.states || '').split(',').slice(0, 3).join(', ');
      const barW = Math.round(100 * f.lead_count / maxCount);
      return `<div style="background:var(--bg-secondary,#f9fafb);border:1px solid var(--border,#e5e7eb);border-radius:8px;padding:0.5rem 0.6rem;cursor:pointer;transition:border-color 0.15s;"
        onmouseenter="this.style.borderColor='#6366f1'" onmouseleave="this.style.borderColor=''"
        onclick="document.getElementById('search-input').value='${esc(f.firm_name.replace(/'/g, ''))}';loadLeads();">
        <div style="font-size:0.75rem;font-weight:600;color:var(--text-primary,#111);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${esc(f.firm_name)}">${esc(f.firm_name)}</div>
        <div style="display:flex;align-items:center;gap:0.3rem;margin:0.25rem 0;">
          <div style="flex:1;height:4px;background:var(--bg-primary,#e5e7eb);border-radius:2px;">
            <div style="width:${barW}%;height:100%;background:#6366f1;border-radius:2px;"></div>
          </div>
          <span style="font-size:0.68rem;font-weight:600;color:#6366f1;min-width:20px;text-align:right;">${f.lead_count}</span>
        </div>
        <div style="display:flex;gap:0.5rem;font-size:0.6rem;color:var(--text-muted,#9ca3af);">
          <span style="color:${emailPct > 0 ? '#22c55e' : '#d1d5db'};">\u2709 ${emailPct}%</span>
          <span style="color:${phonePct > 0 ? '#3b82f6' : '#d1d5db'};">\u{1F4DE} ${phonePct}%</span>
          <span>${states}</span>
        </div>
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load firm intel:', err);
  }
}

// --- Growth Chart (SVG) ---
async function loadGrowthChart(days = 7, btnEl) {
  // Update active button
  if (btnEl) {
    document.querySelectorAll('.growth-range-btn').forEach(b => b.classList.remove('active'));
    btnEl.classList.add('active');
  }

  try {
    const res = await fetch(`/api/leads/growth?days=${days}`);
    const data = await safeJSON(res);
    const container = document.getElementById('growth-chart');
    if (!container || !data.length) {
      if (container) container.innerHTML = '<p style="text-align:center;color:var(--text-muted);font-size:0.75rem;padding:2rem;">No growth data yet. Start scraping to see trends.</p>';
      return;
    }

    const w = container.clientWidth || 600;
    const h = 170;
    const pad = { top: 10, right: 10, bottom: 25, left: 45 };
    const chartW = w - pad.left - pad.right;
    const chartH = h - pad.top - pad.bottom;

    const maxVal = Math.max(...data.map(d => d.created), 1);
    const xScale = (i) => pad.left + (i / Math.max(data.length - 1, 1)) * chartW;
    const yScale = (v) => pad.top + chartH - (v / maxVal) * chartH;

    // Build SVG paths for each series
    const series = [
      { key: 'created', color: '#6366f1', label: 'Total' },
      { key: 'with_email', color: '#22c55e', label: 'Email' },
      { key: 'with_phone', color: '#3b82f6', label: 'Phone' },
      { key: 'with_website', color: '#8b5cf6', label: 'Website' },
    ];

    let svg = `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">`;

    // Grid lines
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (i / 4) * chartH;
      const val = Math.round(maxVal * (1 - i / 4));
      svg += `<line x1="${pad.left}" y1="${y}" x2="${w - pad.right}" y2="${y}" stroke="var(--border,#e5e7eb)" stroke-dasharray="3,3"/>`;
      svg += `<text x="${pad.left - 5}" y="${y + 4}" text-anchor="end" font-size="9" fill="var(--text-muted,#9ca3af)">${val >= 1000 ? (val/1000).toFixed(0) + 'k' : val}</text>`;
    }

    // X axis labels (show every few days)
    const step = Math.max(1, Math.floor(data.length / 6));
    data.forEach((d, i) => {
      if (i % step === 0 || i === data.length - 1) {
        const x = xScale(i);
        const label = new Date(d.day).toLocaleDateString('en', { month: 'short', day: 'numeric' });
        svg += `<text x="${x}" y="${h - 3}" text-anchor="middle" font-size="9" fill="var(--text-muted,#9ca3af)">${label}</text>`;
      }
    });

    // Draw filled area + line for each series
    series.forEach(s => {
      if (data.every(d => d[s.key] === 0)) return;

      // Filled area
      let areaPath = `M${xScale(0)},${yScale(data[0][s.key])}`;
      data.forEach((d, i) => { if (i > 0) areaPath += ` L${xScale(i)},${yScale(d[s.key])}`; });
      areaPath += ` L${xScale(data.length - 1)},${pad.top + chartH} L${xScale(0)},${pad.top + chartH} Z`;
      svg += `<path d="${areaPath}" fill="${s.color}" fill-opacity="0.08"/>`;

      // Line
      let linePath = `M${xScale(0)},${yScale(data[0][s.key])}`;
      data.forEach((d, i) => { if (i > 0) linePath += ` L${xScale(i)},${yScale(d[s.key])}`; });
      svg += `<path d="${linePath}" stroke="${s.color}" stroke-width="2" fill="none"/>`;

      // Data points with hover tooltips
      data.forEach((d, i) => {
        const val = d[s.key] || 0;
        const dateStr = new Date(d.day).toLocaleDateString('en', { month: 'short', day: 'numeric' });
        svg += `<circle cx="${xScale(i)}" cy="${yScale(val)}" r="3" fill="${s.color}" opacity="0" style="transition:opacity 0.15s;">
          <title>${s.label}: ${fmt(val)} (${dateStr})</title>
        </circle>`;
      });
    });

    // Hover areas for showing dots
    svg += `<style>svg circle:hover { opacity: 1 !important; r: 5; }</style>`;
    svg += '</svg>';
    container.innerHTML = svg;

    // Legend
    const legendEl = document.getElementById('growth-legend');
    if (legendEl) {
      legendEl.innerHTML = series.map(s =>
        `<span style="display:flex;align-items:center;gap:0.2rem;">
          <span style="width:10px;height:3px;background:${s.color};border-radius:1px;"></span> ${s.label}
        </span>`
      ).join('');
    }
  } catch (err) {
    console.error('Failed to load growth chart:', err);
  }
}

// --- Quality Alerts ---
async function runQualityCheck() {
  try {
    toast('Running quality checks...', 'info');
    const res = await fetch('/api/quality/check', { method: 'POST' });
    const d = await safeJSON(res);
    toast(`Found ${d.alertsCreated} quality issues`);
    loadQualityAlerts();
  } catch (err) {
    toast('Quality check failed', 'error');
  }
}

async function loadQualityAlerts() {
  try {
    const [alertsRes, summaryRes] = await Promise.all([
      fetch('/api/quality/alerts?limit=50'),
      fetch('/api/quality/summary'),
    ]);
    const alerts = await alertsRes.json();
    const summary = await summaryRes.json();

    // Badge
    const totalAlerts = summary.reduce((s, a) => s + a.cnt, 0);
    const badge = document.getElementById('alert-badge');
    if (badge) badge.textContent = totalAlerts > 0 ? `${totalAlerts} issues` : 'Clean';
    if (badge) badge.style.background = totalAlerts > 0 ? '#fef3c7' : '#dcfce7';
    if (badge) badge.style.color = totalAlerts > 0 ? '#92400e' : '#166534';

    // Summary chips
    const summaryEl = document.getElementById('alert-summary');
    if (summaryEl) {
      const colors = { error: '#fee2e2', warning: '#fef3c7', info: '#dbeafe' };
      const textColors = { error: '#991b1b', warning: '#92400e', info: '#1e40af' };
      summaryEl.innerHTML = summary.map(s =>
        `<span style="font-size:0.68rem;padding:0.15rem 0.5rem;border-radius:12px;background:${colors[s.severity] || '#f3f4f6'};color:${textColors[s.severity] || '#6b7280'};">
          ${esc(s.alert_type.replace(/-/g, ' '))} (${s.cnt})
        </span>`
      ).join('');
    }

    // Alert list
    const listEl = document.getElementById('alert-list');
    if (listEl) {
      if (alerts.length === 0) {
        listEl.innerHTML = '<p style="font-size:0.75rem;color:var(--text-muted);text-align:center;padding:0.5rem;">No quality alerts. Run a check to scan for issues.</p>';
        return;
      }
      const sevIcons = { error: '\u274C', warning: '\u26A0\uFE0F', info: '\u2139\uFE0F' };
      listEl.innerHTML = alerts.map(a =>
        `<div style="display:flex;align-items:flex-start;gap:0.5rem;padding:0.35rem 0;border-bottom:1px solid var(--border,#f3f4f6);font-size:0.72rem;">
          <span style="font-size:0.8rem;flex-shrink:0;">${sevIcons[a.severity] || '\u2139\uFE0F'}</span>
          <div style="flex:1;min-width:0;">
            <div style="color:var(--text-primary,#111);">${esc(a.message)}</div>
            ${a.first_name ? `<div style="color:var(--text-muted,#9ca3af);font-size:0.65rem;">${esc(a.first_name)} ${esc(a.last_name)} (${esc(a.state || '')})</div>` : ''}
          </div>
          <button style="font-size:0.6rem;padding:0.1rem 0.3rem;border:1px solid var(--border);border-radius:3px;background:transparent;color:var(--text-muted);cursor:pointer;flex-shrink:0;"
            onclick="resolveAlertUI(${a.id})">Dismiss</button>
        </div>`
      ).join('');
    }
  } catch (err) {
    console.error('Failed to load quality alerts:', err);
  }
}

async function resolveAlertUI(alertId) {
  try {
    await fetch(`/api/quality/resolve/${alertId}`, { method: 'POST' });
    loadQualityAlerts();
  } catch (err) { toast('Failed to resolve alert: ' + err.message, 'error'); }
}

// --- Export History ---
async function loadExportHistory() {
  try {
    const res = await fetch('/api/exports/history');
    const data = await safeJSON(res);
    const container = document.getElementById('export-history');
    if (!container) return;

    if (!data.length) {
      container.innerHTML = '<p style="font-size:0.75rem;color:var(--text-muted);text-align:center;padding:0.5rem;">No exports yet.</p>';
      return;
    }

    container.innerHTML = data.map(e => {
      const time = new Date(e.created_at + 'Z');
      const ago = timeAgo(time);
      return `<div style="display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0;border-bottom:1px solid var(--border,#f3f4f6);font-size:0.72rem;">
        <span style="font-size:0.85rem;">\u{1F4E5}</span>
        <div style="flex:1;">
          <span style="font-weight:500;">${esc(e.format)}</span>
          <span style="color:var(--text-muted,#9ca3af);margin-left:0.3rem;">${e.lead_count} leads</span>
        </div>
        <span style="color:var(--text-muted,#9ca3af);font-size:0.65rem;">${ago}</span>
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load export history:', err);
  }
}

// --- Lead Changelog in Modal ---
async function loadLeadChangelog(leadId) {
  try {
    const res = await fetch(`/api/leads/changelog/${leadId}`);
    const data = await safeJSON(res);
    if (!data.length) return '';

    return `<div style="margin-top:0.75rem;border-top:1px solid var(--border);padding-top:0.5rem;">
      <div style="font-size:0.82rem;font-weight:600;color:var(--text-muted);margin-bottom:0.3rem;">Change History</div>
      <div style="max-height:150px;overflow-y:auto;">
        ${data.slice(0, 10).map(c => {
          const time = new Date(c.created_at + 'Z');
          return `<div style="font-size:0.68rem;padding:0.2rem 0;border-bottom:1px solid var(--border,#f9fafb);color:var(--text-secondary,#6b7280);">
            <strong>${esc(c.action)}</strong> ${c.field ? esc(c.field) : ''}
            ${c.old_value ? `<span style="text-decoration:line-through;color:#ef4444;">${esc(c.old_value).substring(0, 30)}</span>` : ''}
            ${c.new_value ? `<span style="color:#22c55e;">\u2192 ${esc(c.new_value).substring(0, 30)}</span>` : ''}
            <span style="color:var(--text-muted,#9ca3af);margin-left:0.3rem;">${timeAgo(time)}</span>
          </div>`;
        }).join('')}
      </div>
    </div>`;
  } catch {
    return '';
  }
}

// --- Enrichment Dashboard ---
async function loadEnrichmentDashboard() {
  try {
    const res = await fetch('/api/leads/enrichment-stats');
    const data = await safeJSON(res);

    // Badge
    const badge = document.getElementById('enrichment-enriched-badge');
    if (badge) badge.textContent = `${data.enriched.toLocaleString()} enriched of ${data.total.toLocaleString()}`;

    // Opportunities cards
    const opps = document.getElementById('enrich-opportunities');
    if (opps) {
      const cards = [
        { label: 'Need Email', count: data.opportunities.needsEmail, color: '#ef4444', icon: '\u2709\uFE0F', pct: Math.round(100 * data.opportunities.needsEmail / Math.max(data.total, 1)) },
        { label: 'Need Phone', count: data.opportunities.needsPhone, color: '#f59e0b', icon: '\u{1F4DE}', pct: Math.round(100 * data.opportunities.needsPhone / Math.max(data.total, 1)) },
        { label: 'Website \u2192 Email', count: data.opportunities.hasWebsiteNoEmail, color: '#3b82f6', icon: '\u{1F310}', pct: Math.round(100 * data.opportunities.hasWebsiteNoEmail / Math.max(data.opportunities.needsEmail, 1)) },
        { label: 'Profile \u2192 Email', count: data.opportunities.hasProfileNoEmail, color: '#8b5cf6', icon: '\u{1F517}', pct: Math.round(100 * data.opportunities.hasProfileNoEmail / Math.max(data.opportunities.needsEmail, 1)) },
      ];
      opps.innerHTML = cards.map(c => `
        <div style="background:var(--bg-secondary,#f9fafb);border:1px solid var(--border,#e5e7eb);border-radius:8px;padding:0.6rem 0.75rem;text-align:center;">
          <div style="font-size:1.2rem;margin-bottom:0.2rem;">${c.icon}</div>
          <div style="font-size:1.1rem;font-weight:700;color:${c.color};">${c.count.toLocaleString()}</div>
          <div style="font-size:0.68rem;color:var(--text-muted,#6b7280);">${c.label}</div>
          <div style="margin-top:0.3rem;height:4px;background:var(--bg-primary,#e5e7eb);border-radius:2px;">
            <div style="width:${c.pct}%;height:100%;background:${c.color};border-radius:2px;"></div>
          </div>
        </div>
      `).join('');
    }

    // Source attribution bars
    function renderSources(containerId, sources, total, color) {
      const el = document.getElementById(containerId);
      if (!el || !sources.length) { if (el) el.innerHTML = '<span style="font-size:0.7rem;color:var(--text-muted,#9ca3af);">No data</span>'; return; }
      const max = sources[0].cnt;
      el.innerHTML = sources.map(s => {
        const pct = Math.round(100 * s.cnt / Math.max(total, 1));
        const barPct = Math.round(100 * s.cnt / Math.max(max, 1));
        const src = s.email_source || s.phone_source || s.website_source;
        return `<div style="display:flex;align-items:center;gap:0.4rem;margin-bottom:0.25rem;">
          <span style="font-size:0.68rem;min-width:70px;color:var(--text-secondary,#374151);">${esc(src)}</span>
          <div style="flex:1;height:6px;background:var(--bg-primary,#f3f4f6);border-radius:3px;">
            <div style="width:${barPct}%;height:100%;background:${color};border-radius:3px;"></div>
          </div>
          <span style="font-size:0.65rem;color:var(--text-muted,#9ca3af);min-width:30px;text-align:right;">${s.cnt}</span>
        </div>`;
      }).join('');
    }
    renderSources('enrich-email-sources', data.emailSources, data.withEmail, '#22c55e');
    renderSources('enrich-phone-sources', data.phoneSources, data.withPhone, '#3b82f6');
    renderSources('enrich-website-sources', data.websiteSources, data.withWebsite, '#8b5cf6');
  } catch (err) {
    console.error('Failed to load enrichment dashboard:', err);
  }
}

// loadActivityFeed() defined in app.js — superseded this older version

// --- Geographic Map View ---
// US state grid positions (row, col) — "tile grid map" layout
const US_GRID = {
  AK:[0,0],HI:[7,0],ME:[0,10],VT:[1,9],NH:[1,10],WA:[2,1],MT:[2,2],ND:[2,3],MN:[2,4],WI:[2,5],MI:[2,7],NY:[2,8],MA:[2,9],CT:[2,10],RI:[2,11],
  ID:[3,1],WY:[3,2],SD:[3,3],IA:[3,4],IL:[3,5],IN:[3,6],OH:[3,7],PA:[3,8],NJ:[3,9],
  OR:[4,1],NV:[4,2],CO:[4,3],NE:[4,4],MO:[4,5],KY:[4,6],WV:[4,7],VA:[4,8],MD:[4,9],DE:[4,10],DC:[4,11],
  CA:[5,1],UT:[5,2],NM:[5,3],KS:[5,4],AR:[5,5],TN:[5,6],NC:[5,7],SC:[5,8],
  AZ:[6,2],OK:[6,3],LA:[6,4],MS:[6,5],AL:[6,6],GA:[6,7],FL:[6,8],
  TX:[7,3],
};

function switchGeoTab(tab, btn) {
  document.querySelectorAll('.geo-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  document.getElementById('geo-map-view').style.display = tab === 'map' ? '' : 'none';
  document.getElementById('geo-world-view').style.display = tab === 'world' ? '' : 'none';
  document.getElementById('geo-grid-view').style.display = tab === 'grid' ? '' : 'none';
  if (tab === 'world') loadWorldView();
}

async function loadGeoMap() {
  try {
    const coverage = await fetch('/api/leads/coverage').then(safeJSON);
    const byState = {};
    let maxCount = 0;
    for (const row of coverage) {
      byState[row.state] = row;
      if (row.total > maxCount) maxCount = row.total;
    }

    const COLS = 12, ROWS = 8;
    const cellW = 100 / COLS;
    const cellH = 100 / ROWS;

    let html = '<div style="position:relative;width:100%;padding-bottom:' + (ROWS * 100 / COLS) + '%;">';
    for (const [st, [r, c]] of Object.entries(US_GRID)) {
      const d = byState[st];
      const count = d ? d.total : 0;
      const emailPct = d ? d.email_pct : 0;
      // Color: intensity based on lead count, hue based on email coverage
      let bg, textColor;
      if (count === 0) {
        bg = '#f3f4f6'; textColor = '#9ca3af';
      } else {
        const intensity = Math.min(count / Math.max(maxCount * 0.3, 1), 1);
        if (emailPct > 30) {
          bg = `rgba(16,185,129,${0.15 + intensity * 0.7})`; // green = has email
          textColor = intensity > 0.5 ? '#fff' : '#065f46';
        } else if (count > 50) {
          bg = `rgba(99,102,241,${0.15 + intensity * 0.7})`; // indigo = many leads
          textColor = intensity > 0.5 ? '#fff' : '#312e81';
        } else {
          bg = `rgba(245,158,11,${0.2 + intensity * 0.5})`; // amber = sparse
          textColor = '#92400e';
        }
      }
      html += `<div onclick="openStateModal('${st}')" title="${st}: ${count} leads, ${emailPct}% email"
        style="position:absolute;left:${c * cellW}%;top:${r * cellH * (COLS / ROWS)}%;width:${cellW - 0.3}%;padding-bottom:${cellW - 0.3}%;
          background:${bg};border-radius:4px;cursor:pointer;display:flex;align-items:center;justify-content:center;
          transition:transform 0.15s;font-size:0.65rem;font-weight:600;color:${textColor};"
        onmouseover="this.style.transform='scale(1.15)';this.style.zIndex=10" onmouseout="this.style.transform='';this.style.zIndex=''">
        <span style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);">${st}</span>
        ${count > 0 ? '<span style="position:absolute;bottom:1px;left:50%;transform:translateX(-50%);font-size:0.5rem;opacity:0.8;">' + (count > 999 ? (count/1000).toFixed(1)+'k' : count) + '</span>' : ''}
      </div>`;
    }
    html += '</div>';

    document.getElementById('us-geo-map').innerHTML = html;
    document.getElementById('geo-legend').innerHTML = `
      <span><span style="display:inline-block;width:10px;height:10px;background:#f3f4f6;border-radius:2px;margin-right:3px;"></span>No data</span>
      <span><span style="display:inline-block;width:10px;height:10px;background:rgba(245,158,11,0.5);border-radius:2px;margin-right:3px;"></span>Sparse</span>
      <span><span style="display:inline-block;width:10px;height:10px;background:rgba(99,102,241,0.6);border-radius:2px;margin-right:3px;"></span>Good coverage</span>
      <span><span style="display:inline-block;width:10px;height:10px;background:rgba(16,185,129,0.7);border-radius:2px;margin-right:3px;"></span>Has email data</span>
    `;
  } catch (err) { console.error('Geo map error:', err); }
}

async function loadWorldView() {
  try {
    const coverage = await fetch('/api/leads/coverage').then(safeJSON);
    // Group by country
    const countries = {};
    for (const row of coverage) {
      const c = row.country || 'US';
      if (!countries[c]) countries[c] = { states: [], total: 0, email: 0, phone: 0, website: 0 };
      countries[c].states.push(row);
      countries[c].total += row.total;
      countries[c].email += row.with_email || 0;
      countries[c].phone += row.with_phone || 0;
      countries[c].website += row.with_website || 0;
    }
    const FLAGS = { US: '&#x1F1FA;&#x1F1F8;', CA: '&#x1F1E8;&#x1F1E6;', UK: '&#x1F1EC;&#x1F1E7;', AU: '&#x1F1E6;&#x1F1FA;', FR: '&#x1F1EB;&#x1F1F7;', IE: '&#x1F1EE;&#x1F1EA;', IT: '&#x1F1EE;&#x1F1F9;', DE: '&#x1F1E9;&#x1F1EA;', HK: '&#x1F1ED;&#x1F1F0;', NZ: '&#x1F1F3;&#x1F1FF;', SG: '&#x1F1F8;&#x1F1EC;', IN: '&#x1F1EE;&#x1F1F3;', ZA: '&#x1F1FF;&#x1F1E6;' };
    const sorted = Object.entries(countries).sort((a, b) => b[1].total - a[1].total);

    let html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:0.75rem;">';
    for (const [code, data] of sorted) {
      const emailPct = data.total > 0 ? (data.email / data.total * 100).toFixed(0) : 0;
      const phonePct = data.total > 0 ? (data.phone / data.total * 100).toFixed(0) : 0;
      html += `<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:0.75rem;cursor:pointer;" onclick="document.getElementById('filter-country').value='${code}';loadLeads();">
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;">
          <span style="font-size:1.5rem;">${FLAGS[code] || '&#x1F30D;'}</span>
          <div>
            <div style="font-weight:700;font-size:0.9rem;">${code}</div>
            <div style="font-size:0.75rem;color:#6b7280;">${data.states.length} jurisdictions</div>
          </div>
          <span style="margin-left:auto;font-weight:700;font-size:1.1rem;">${data.total.toLocaleString()}</span>
        </div>
        <div style="display:flex;gap:0.5rem;font-size:0.7rem;">
          <span style="color:#059669;">${emailPct}% email</span>
          <span style="color:#0ea5e9;">${phonePct}% phone</span>
        </div>
        <div style="margin-top:0.4rem;height:4px;background:#f3f4f6;border-radius:2px;overflow:hidden;">
          <div style="height:100%;width:${emailPct}%;background:linear-gradient(90deg,#10b981,#059669);border-radius:2px;"></div>
        </div>
      </div>`;
    }
    html += '</div>';
    document.getElementById('geo-world-view').innerHTML = html;
  } catch (err) { console.error('World view error:', err); }
}
