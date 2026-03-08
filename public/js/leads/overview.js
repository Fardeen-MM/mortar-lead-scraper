// overview.js — Overview tab: stats, sparklines, scores, donut chart, recommendations, coverage
// Dependencies: utils.js, app.js

// --- Stats ---
async function loadStats() {
  try {
    const res = await fetch('/api/leads/stats');
    const d = await safeJSON(res);

    document.title = `Mortar (${fmt(d.total)}) Lead Database`;
    animateCount(document.getElementById('tam-total'), d.total);
    animateCount(document.getElementById('tam-contactable'), d.contactable || 0);
    animateCount(document.getElementById('tam-gold'), d.goldLeads || 0);
    animateCount(document.getElementById('tam-email'), d.withEmail);
    animateCount(document.getElementById('tam-phone'), d.withPhone);
    animateCount(document.getElementById('tam-website'), d.withWebsite);
    animateCount(document.getElementById('tam-firms'), d.uniqueFirms);
    animateCount(document.getElementById('tam-enriched'), d.enriched || 0);
    animateCount(document.getElementById('tam-enriched-24h'), d.enrichedLast24h || 0);
    animateCount(document.getElementById('tam-linkedin'), d.withLinkedin || 0);
    animateCount(document.getElementById('tam-title'), d.withTitle || 0);

    document.getElementById('tam-contactable-pct').textContent = (d.coverage.contactable || 0) + '%';
    document.getElementById('tam-email-pct').textContent = d.coverage.email + '%';
    document.getElementById('tam-phone-pct').textContent = d.coverage.phone + '%';
    document.getElementById('tam-website-pct').textContent = d.coverage.website + '%';

    document.getElementById('contactable-bar').style.width = (d.coverage.contactable || 0) + '%';
    document.getElementById('email-bar').style.width = d.coverage.email + '%';
    document.getElementById('phone-bar').style.width = d.coverage.phone + '%';
    document.getElementById('website-bar').style.width = d.coverage.website + '%';

    // By country — with bar charts
    document.getElementById('by-country').innerHTML = renderBreakdownRows(
      (d.byCountry || []).map(c => ({ label: c.country, count: c.count })),
      { onclick: (item) => `document.getElementById('filter-country').value='${item.label}';loadLeads()` }
    );

    // By state — with bar charts
    document.getElementById('by-state').innerHTML = renderBreakdownRows(
      (d.byState || []).slice(0, 15).map(s => ({ label: s.state, count: s.count })),
      { onclick: (item) => `document.getElementById('filter-state').value='${item.label}';loadLeads()` }
    );

    // By source — with bar charts
    document.getElementById('by-source').innerHTML = renderBreakdownRows(
      (d.bySource || []).slice(0, 15).map(s => ({ label: s.source, count: s.count })),
      { onclick: (item) => `document.getElementById('filter-source').value='${item.label}';loadLeads()` }
    );

    // Top cities — with bar charts
    document.getElementById('by-city').innerHTML = renderBreakdownRows(
      (d.topCities || []).slice(0, 15).map(c => ({ label: c.location, count: c.count }))
    );

    // Email sources — with bar charts
    document.getElementById('by-email-source').innerHTML = renderBreakdownRows(
      (d.emailSources || []).map(s => ({ label: s.email_source, count: s.count })),
      { empty: '<div style="font-size:0.78rem;color:var(--text-muted);padding:0.3rem 0;">No email sources yet</div>' }
    );

    // Practice areas — with bar charts
    document.getElementById('by-practice-area').innerHTML = renderBreakdownRows(
      (d.byPracticeArea || []).slice(0, 15).map(p => ({ label: p.practice_area, count: p.count })),
      { onclick: (item) => `document.getElementById('filter-practice').value='${item.label}';loadLeads()`,
        empty: '<div style="font-size:0.78rem;color:var(--text-muted);padding:0.3rem 0;">No practice areas yet</div>' }
    );

    // Populate state filter
    const stateSelect = document.getElementById('filter-state');
    const current = stateSelect.value;
    stateSelect.innerHTML = '<option value="">All States</option>';
    (d.byState || []).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s.state;
      opt.textContent = `${s.state} (${fmt(s.count)})`;
      stateSelect.appendChild(opt);
    });
    stateSelect.value = current;

    document.getElementById('empty-state').style.display = d.total === 0 ? '' : 'none';

    // Power bar
    document.getElementById('pb-total').textContent = fmt(d.total);
    document.getElementById('pb-states').textContent = (d.byState || []).length + ' states';
    document.getElementById('pb-countries').textContent = (d.byCountry || []).length + ' countries';
    document.getElementById('pb-emails').textContent = fmt(d.withEmail) + ' emails';
    document.getElementById('pb-phones').textContent = fmt(d.withPhone) + ' phones';
    document.getElementById('pb-websites').textContent = fmt(d.withWebsite) + ' websites';
    document.getElementById('pb-updated').textContent = 'Updated ' + new Date().toLocaleTimeString();

    // Tab badges
    const leadsBadge = document.getElementById('tab-badge-leads');
    if (leadsBadge) leadsBadge.textContent = d.total > 0 ? (d.total > 999 ? Math.round(d.total / 1000) + 'k' : d.total) : '';
    // Quality badge shows count of leads missing email (opportunity count)
    const qualityBadge = document.getElementById('tab-badge-quality');
    const missingEmail = d.total - (d.withEmail || 0);
    if (qualityBadge) qualityBadge.textContent = missingEmail > 0 ? (missingEmail > 999 ? Math.round(missingEmail / 1000) + 'k' : missingEmail) : '';
  } catch (err) {
    console.error('Failed to load stats:', err);
  }
}

// --- TAM Sparklines ---
async function loadSparklines() {
  try {
    const data = await fetch('/api/leads/growth?days=7').then(safeJSON);
    if (!data || data.length < 2) return;
    const el = (id) => document.getElementById(id);
    if (el('spark-total')) el('spark-total').innerHTML = sparkline(data.map(d => d.created), { color: '#6366f1', filled: true });
    if (el('spark-email')) el('spark-email').innerHTML = sparkline(data.map(d => d.with_email), { color: '#22c55e', filled: true });
    if (el('spark-phone')) el('spark-phone').innerHTML = sparkline(data.map(d => d.with_phone), { color: '#3b82f6', filled: true });
    if (el('spark-website')) el('spark-website').innerHTML = sparkline(data.map(d => d.with_website), { color: '#8b5cf6', filled: true });
  } catch (err) { /* sparklines are optional */ }
}

// --- Scores + Donut Chart ---
async function loadScores() {
  try {
    const res = await fetch('/api/leads/scores');
    const d = await safeJSON(res);
    document.getElementById('tam-avg-score').textContent = d.avgScore;
    document.getElementById('score-excellent').textContent = fmt(d.excellent);
    document.getElementById('score-good').textContent = fmt(d.good);
    document.getElementById('score-fair').textContent = fmt(d.fair);
    document.getElementById('score-poor').textContent = fmt(d.poor);
    renderDonut([
      { value: d.excellent || 0, color: '#22c55e', label: 'Excellent' },
      { value: d.good || 0, color: '#3b82f6', label: 'Good' },
      { value: d.fair || 0, color: '#f59e0b', label: 'Fair' },
      { value: d.poor || 0, color: '#ef4444', label: 'Poor' },
    ]);
  } catch (err) {
    console.error('Failed to load scores:', err);
  }
}

function renderDonut(segments) {
  const svg = document.getElementById('score-donut');
  const cx = 70, cy = 70, r = 55, strokeW = 18;
  const total = segments.reduce((s, d) => s + d.value, 0);
  if (total === 0) { svg.innerHTML = '<text x="70" y="75" text-anchor="middle" font-size="12" fill="#9ca3af">No data</text>'; return; }
  let html = '<style>svg circle.donut-seg { cursor: pointer; transition: stroke-width 0.2s; } svg circle.donut-seg:hover { stroke-width: 22; }</style>';
  const circumf = 2 * Math.PI * r;
  let offset = 0;
  for (const seg of segments) {
    if (seg.value === 0) continue;
    const pct = seg.value / total;
    const dash = pct * circumf;
    html += `<circle class="donut-seg" cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${seg.color}" stroke-width="${strokeW}" stroke-dasharray="${dash} ${circumf - dash}" stroke-dashoffset="${-offset}" transform="rotate(-90 ${cx} ${cy})" style="transition:stroke-dasharray 0.5s,stroke-dashoffset 0.5s,stroke-width 0.2s;">
      <title>${seg.label}: ${fmt(seg.value)} (${(pct * 100).toFixed(1)}%)</title>
    </circle>`;
    offset += dash;
  }
  html += `<text x="${cx}" y="${cy - 6}" text-anchor="middle" font-size="18" font-weight="700" fill="var(--text)">${fmt(total)}</text>`;
  html += `<text x="${cx}" y="${cy + 10}" text-anchor="middle" font-size="10" fill="var(--text-muted)">scored</text>`;
  svg.innerHTML = html;
}

// --- Recommendations ---
async function loadRecommendations() {
  try {
    const res = await fetch('/api/leads/recommendations');
    const recs = await safeJSON(res);
    const panel = document.getElementById('recs-panel');
    const list = document.getElementById('recs-list');

    if (!recs || recs.length === 0) { panel.style.display = 'none'; return; }

    panel.style.display = '';
    list.innerHTML = recs.map(r => `
      <div class="rec-item">
        <span class="rec-badge ${r.priority}">${r.priority}</span>
        <div class="rec-text">
          <div class="rec-title">${esc(r.title)}</div>
          <div class="rec-desc">${esc(r.description)}</div>
        </div>
      </div>
    `).join('');
  } catch (err) {
    console.error('Failed to load recommendations:', err);
  }
}

// --- Activity Timeline ---
async function loadDigest() {
  try {
    const d = await fetch('/api/leads/digest').then(safeJSON);
    const bar = document.getElementById('digest-bar');
    if (d.today.added === 0 && d.today.enriched === 0) { bar.style.display = 'none'; return; }
    bar.style.display = '';
    document.getElementById('digest-added').innerHTML = d.today.added > 0
      ? `<span style="background:#dcfce7;color:#166534;padding:0.1rem 0.4rem;border-radius:4px;font-weight:600;">+${d.today.added} leads</span>` +
        (d.today.withEmail > 0 ? ` <span style="color:#059669;">(${d.today.withEmail} with email)</span>` : '')
      : '';
    document.getElementById('digest-enriched').innerHTML = d.today.enriched > 0
      ? `<span style="background:#ede9fe;color:#5b21b6;padding:0.1rem 0.4rem;border-radius:4px;">${d.today.enriched} enriched</span>` : '';
    document.getElementById('digest-top-state').innerHTML = d.topState
      ? `Top: <strong>${d.topState.state}</strong> (${d.topState.count})` : '';
    const trends = { up: '<span style="color:#16a34a;">&#x25B2; Up from yesterday</span>', down: '<span style="color:#dc2626;">&#x25BC; Down from yesterday</span>', flat: '<span style="color:#6b7280;">&#x2500; Same as yesterday</span>' };
    document.getElementById('digest-trend').innerHTML = d.yesterday.added > 0 ? (trends[d.trend] || '') : '';
  } catch (err) { console.error('Digest error:', err); }
}

async function loadActivity() {
  try {
    const res = await fetch('/api/leads/activity?limit=20');
    const data = await safeJSON(res);
    const el = document.getElementById('activity-timeline');
    if (!data || data.length === 0) {
      el.innerHTML = '<div style="color:var(--text-muted);font-size:0.8rem;padding:0.5rem 0;">No scrape runs yet</div>';
      return;
    }
    el.innerHTML = data.map(r => {
      const dotClass = r.leads_found > 0 ? 'success' : 'zero';
      const dur = r.duration_secs ? `${Math.round(r.duration_secs)}s` : '';
      return `<div class="tl-item">
        <span class="tl-dot ${dotClass}"></span>
        <span class="tl-state">${esc(r.state)}</span>
        <span class="tl-stats">${fmt(r.leads_found)} leads, ${fmt(r.leads_new)} new${r.emails_found ? `, ${fmt(r.emails_found)} emails` : ''}${dur ? ` (${dur})` : ''}</span>
        <span class="tl-time">${timeAgo(r.completed_at)}</span>
      </div>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load activity:', err);
  }
}

// --- Practice Areas ---
async function loadPracticeAreas() {
  try {
    const res = await fetch('/api/leads/practice-areas');
    const data = await safeJSON(res);
    const select = document.getElementById('filter-practice');
    const current = select.value;
    select.innerHTML = '<option value="">All Practice Areas</option>';
    (data || []).forEach(p => {
      const opt = document.createElement('option');
      opt.value = p.practice_area;
      opt.textContent = `${p.practice_area} (${fmt(p.count)})`;
      select.appendChild(opt);
    });
    select.value = current;
  } catch (err) {
    console.error('Failed to load practice areas:', err);
  }
}

// --- Freshness ---
async function loadFreshness() {
  try {
    const res = await fetch('/api/leads/freshness');
    const data = await safeJSON(res);
    _freshnessMap = {};
    data.forEach(d => {
      _freshnessMap[d.state] = { days: d.days_since_scrape, last: d.last_scraped, runs: d.run_count };
    });
  } catch (err) {
    console.error('Failed to load freshness:', err);
  }
}

// --- Coverage + Freshness Table (with quality grade) ---
function qualityGrade(emailPct, phonePct, webPct) {
  const avg = (emailPct + phonePct + webPct) / 3;
  if (avg >= 60) return { grade: 'A', cls: 'grade-a' };
  if (avg >= 40) return { grade: 'B', cls: 'grade-b' };
  if (avg >= 20) return { grade: 'C', cls: 'grade-c' };
  if (avg > 0) return { grade: 'D', cls: 'grade-d' };
  return { grade: 'F', cls: 'grade-f' };
}

async function loadCoverage() {
  try {
    const res = await fetch('/api/leads/coverage');
    const data = await safeJSON(res);
    const tbody = document.getElementById('coverage-body');
    tbody.innerHTML = data.map(s => {
      const pctBar = (pct) => {
        const color = pct >= 70 ? '#22c55e' : pct >= 30 ? '#f59e0b' : pct > 0 ? '#ef4444' : '#e5e7eb';
        return `<div style="display:flex;align-items:center;gap:0.5rem;"><div style="width:50px;height:6px;background:#f3f4f6;border-radius:3px;"><div style="width:${pct}%;height:100%;background:${color};border-radius:3px;"></div></div><span style="font-size:0.75rem;min-width:35px;">${pct}%</span></div>`;
      };
      const f = _freshnessMap[s.state];
      let freshHtml = '<span style="color:var(--text-muted);font-size:0.75rem;">Never</span>';
      if (f) {
        const dotClass = f.days <= 3 ? 'fresh' : f.days <= 7 ? 'stale' : 'old';
        const label = f.days === 0 ? 'Today' : f.days === 1 ? '1d ago' : f.days + 'd ago';
        freshHtml = `<span class="freshness-dot ${dotClass}"></span><span style="font-size:0.75rem;">${label}</span>`;
      }
      const qg = qualityGrade(s.email_pct, s.phone_pct, s.website_pct);
      return `<tr>
        <td><strong style="cursor:pointer;color:#4f46e5;" onclick="openStateModal('${esc(s.state)}')">${esc(s.state)}</strong></td>
        <td>${esc(s.country)}</td>
        <td><span class="quality-grade ${qg.cls}">${qg.grade}</span></td>
        <td>${fmt(s.total)}</td>
        <td>${pctBar(s.email_pct)}</td>
        <td>${pctBar(s.phone_pct)}</td>
        <td>${pctBar(s.website_pct)}</td>
        <td>${freshHtml}</td>
        <td class="cov-actions">
          <button class="cov-btn" id="cov-scrape-${s.state}" onclick="quickScrape('${esc(s.state)}')">Scrape</button>
          <a class="cov-btn" href="/api/leads/export/state/${encodeURIComponent(s.state)}" style="text-decoration:none;">Export</a>
        </td>
      </tr>`;
    }).join('');
  } catch (err) {
    console.error('Failed to load coverage:', err);
  }
}

// --- Field Completeness ---
async function loadCompleteness() {
  try {
    const res = await fetch('/api/leads/completeness');
    const data = await safeJSON(res);
    const el = document.getElementById('completeness-bars');
    const labels = { email: 'Email', phone: 'Phone', website: 'Website', firm_name: 'Firm', bar_number: 'Bar #', practice_area: 'Practice', title: 'Title', education: 'Education' };
    const fields = Object.entries(labels).map(([key, label]) => {
      const f = data.fields[key] || { count: 0, pct: 0 };
      const color = f.pct >= 60 ? '#22c55e' : f.pct >= 25 ? '#f59e0b' : f.pct > 0 ? '#ef4444' : '#e5e7eb';
      return `<div class="completeness-item">
        <span class="completeness-label">${label}</span>
        <div class="completeness-bar-wrap"><div class="completeness-bar-fill" style="width:${f.pct}%;background:${color};"></div></div>
        <span class="completeness-pct" style="color:${color === '#e5e7eb' ? '#9ca3af' : color};">${f.pct}%</span>
      </div>`;
    });
    el.innerHTML = `<div class="completeness-grid">${fields.join('')}</div>`;
  } catch (err) {
    console.error('Failed to load completeness:', err);
  }
}

