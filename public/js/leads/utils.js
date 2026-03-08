// utils.js — Shared utility functions for the Leads Dashboard
// Dependencies: none (must load first)

// --- Breakdown Bar Helper ---
const BREAKDOWN_COLORS = ['#6366f1','#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#ec4899','#0ea5e9','#14b8a6','#f97316','#84cc16','#a855f7','#06b6d4','#e11d48','#65a30d'];
function renderBreakdownRows(items, opts = {}) {
  if (!items || items.length === 0) return opts.empty || '<div style="font-size:0.78rem;color:var(--text-muted);padding:0.3rem 0;">No data yet</div>';
  const max = Math.max(...items.map(i => i.count), 1);
  return items.map((item, idx) => {
    const pct = Math.max(3, (item.count / max) * 100);
    const color = BREAKDOWN_COLORS[idx % BREAKDOWN_COLORS.length];
    const onclick = opts.onclick ? ` onclick="${opts.onclick(item)}"` : '';
    return `<div class="breakdown-row"${onclick}>
      <span class="breakdown-key">${esc(item.label)}</span>
      <div class="breakdown-bar"><div class="fill" style="width:${pct.toFixed(1)}%;background:${color};"></div></div>
      <span class="breakdown-val">${fmt(item.count)}</span>
    </div>`;
  }).join('');
}

// --- Animated Count-up ---
function animateCount(el, target, duration = 600) {
  if (!el) return;
  const start = parseInt(el.textContent.replace(/,/g, '')) || 0;
  if (start === target) return;
  const startTime = performance.now();
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(elapsed / duration, 1);
    // Ease out cubic
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = Math.round(start + (target - start) * eased);
    el.textContent = current.toLocaleString();
    if (progress < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

// --- Sparkline Generator ---
function sparkline(values, opts = {}) {
  const { width = 60, height = 16, color = '#6366f1', filled = false } = opts;
  if (!values || values.length < 2) return '';
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${(i * step).toFixed(1)},${(height - ((v - min) / range) * height).toFixed(1)}`);
  let svg = `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="display:block;margin:0.2rem auto 0;">`;
  if (filled) {
    svg += `<path d="M0,${height} ${points.map(p => 'L' + p).join(' ')} L${width},${height} Z" fill="${color}" fill-opacity="0.15"/>`;
  }
  svg += `<polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>`;
  // End dot
  const lastPt = points[points.length - 1].split(',');
  svg += `<circle cx="${lastPt[0]}" cy="${lastPt[1]}" r="1.5" fill="${color}"/>`;
  svg += '</svg>';
  return svg;
}

// --- Toast Notification System ---
function toast(msg, type = 'success') {
  const container = document.getElementById('toast-container');
  const icons = { success: '&#x2705;', error: '&#x274C;', info: '&#x2139;&#xFE0F;', warning: '&#x26A0;&#xFE0F;' };
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${icons[type] || icons.info}</span><span>${msg}</span>`;
  container.appendChild(el);
  setTimeout(() => { el.classList.add('leaving'); setTimeout(() => el.remove(), 300); }, 4000);
}

// Safe JSON response parser — throws on HTTP errors instead of silently failing
function safeJSON(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }

// --- Panel loading helper: shows spinner during load, empty state if no data ---
function panelLoading(id) {
  const el = document.getElementById(id);
  if (!el) return;
  const content = el.querySelector('.panel-content');
  if (content) content.innerHTML = '<div style="padding:0.5rem;"><div class="skeleton skeleton-line w90"></div><div class="skeleton skeleton-line w75"></div><div class="skeleton skeleton-line w50"></div></div>';
}
function panelEmpty(id, msg = 'No data yet', icon = '') {
  const el = document.getElementById(id);
  if (!el) return;
  const content = el.querySelector('.panel-content');
  if (content) content.innerHTML = `<div class="panel-empty">${icon ? '<div class="icon">' + icon + '</div>' : ''}<div class="msg">${msg}</div></div>`;
}

function esc(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function fmt(n) {
  if (typeof n !== 'number') return '-';
  return n.toLocaleString();
}

function timeAgo(input) {
  if (!input) return '';
  const d = input instanceof Date ? input : new Date(String(input) + (String(input).includes('Z') ? '' : 'Z'));
  const secs = Math.floor((Date.now() - d.getTime()) / 1000);
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  if (secs < 604800) return Math.floor(secs / 86400) + 'd ago';
  return d.toLocaleDateString();
}
