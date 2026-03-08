// app.js — App shell: tab navigation, global state, refreshAll, notifications, WebSocket, init
// Dependencies: utils.js (must load first)

// --- Main Tab Navigation ---
// --- Tab Navigation: which panels belong to which tab ---
// Panels not in any tab will be hidden on all tabs except 'overview' (catch-all)
const TAB_PANELS = {
  overview: [
    'quick-actions',
    'donut-section-wrap', 'recs-panel',
    'kpi-panel', 'pipeline-board',
    'breakdown-section-1', 'breakdown-section-2',
    'geo-panel', 'growth-panel',
    'activity-panel', 'top-firms-panel',
    'deal-panel', 'outreach-cal-panel', 'risk-panel', 'network-panel',
    'journey-panel', 'lifecycle-funnel-panel', 'funnel-panel',
    'leaderboard-panel',
    'growth-analytics-panel', 'pipeline-velocity-panel', 'pipeline-health-panel',
    'coverage-panel',
    'bulk-ops-panel', 'csv-import-panel',
  ],
  leads: [
    'lists-panel', 'import-panel', 'notes-panel',
    'comparison-panel', 'contacts-panel',
  ],
  analytics: [
    'enrichment-panel', 'analytics-panel',
    'scoring-audit-panel', 'scoring-cal-panel', 'scoring-compare-panel',
    'enrichment-roi-panel', 'engagement-pred-panel', 'engagement-panel',
    'campaign-perf-panel', 'outreach-analytics-panel',
    'source-attribution-panel', 'source-roi-panel', 'revenue-attr-panel',
    'attribution-panel',
    'firm-agg-panel', 'firm-directory-panel', 'firm-intel-panel', 'firm-growth-panel',
    'practice-analytics-panel', 'practice-market-panel',
    'geo-expansion-panel', 'geo-cluster-panel', 'saturation-heat-panel', 'saturation-panel',
    'relationship-panel', 'relationship-graph-panel',
    'priority-matrix-panel', 'priority-inbox-panel',
    'propensity-panel', 'cohort-panel', 'cluster-panel',
    'ab-test-panel', 'competitive-panel',
    'benchmark-panel', 'velocity-panel',
    'channel-pref-panel', 'predictive-panel',
    'team-perf-panel', 'icp-scoring-panel',
    'affinity-panel', 'lookalike-panel',
    'reengage-panel', 'sla-panel',
    'cadence-opt-panel', 'improvement-panel',
    'smart-list-panel', 'saturation-heat-panel',
    'revenue-attr-panel',
    'engagement-heatmap-panel',
  ],
  quality: [
    'verification-panel', 'email-class-panel', 'confidence-panel',
    'staleness-panel', 'freshness-monitor-panel', 'freshness-index-panel', 'freshness-alerts-panel',
    'quality-panel', 'quality-scorecard-panel', 'quality-rules-panel', 'quality-alerts-panel',
    'completeness-matrix-panel',
    'dedup-intel-panel', 'dedup-queue-panel', 'duplicates-panel', 'cross-source-panel', 'merge-panel',
    'aging-report-panel',
    'changes-panel', 'email-validation-panel',
    'deliverability-panel',
    'outbound-ready-panel',
    'scraper-gaps-panel',
    'enrich-waterfall-panel', 'waterfall-history-panel', 'recently-enriched-panel', 'enrichment-priority-panel', 'enrichment-coverage-panel', 'enrichment-failures-panel', 'bulk-enrichment-panel',
    'enrichment-queue-panel', 'enrichment-queue-panel2',
    'heatmap-panel',
  ],
  outreach: [
    'ai-command-panel', 'sequences-panel', 'sequence-analytics-panel', 'seq-templates-panel',
    'campaigns-panel',
    'dnc-panel', 'compliance-panel',
    'territory-panel', 'routing-panel',
    'intent-panel', 'admissions-panel',
    'warmup-panel', 'activity-scoring-panel',
    'nurture-cadence-panel',
    'timeline-panel',
    'export-sched-panel', 'export-profiles-panel', 'export-templates-panel', 'export-history-panel',
    'instantly-panel',
    'lifecycle-panel',
    'webhooks-panel',
  ],
  settings: [
    'tags-panel', 'tag-rules-panel',
    'icp-panel', 'saved-searches-panel',
    'scoring-panel', 'scoring-models-panel',
    'automation-panel', 'custom-fields-panel',
    'score-decay-panel', 'decay-panel',
    'smart-lists-panel',
    'owners-panel',
    'audit-log-panel',
    'schedules-panel', 'segments-panel',
  ],
};

let _activeTab = 'overview';

function switchMainTab(tab) {
  _activeTab = tab;
  // Update tab buttons
  document.querySelectorAll('.tab-nav .tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });

  // Build set of visible panels for this tab
  const visible = new Set(TAB_PANELS[tab] || []);

  // Build set of ALL managed panels across all tabs
  const allManaged = new Set();
  Object.values(TAB_PANELS).forEach(ids => ids.forEach(id => allManaged.add(id)));

  // Show/hide all managed panels
  allManaged.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('tab-hidden', !visible.has(id));
  });

  // Lead Explorer wrapper — only on leads tab
  const explorer = document.getElementById('lead-explorer-wrapper');
  if (explorer) explorer.classList.toggle('tab-hidden', tab !== 'leads');

  // Scroll to top
  window.scrollTo({ top: 0, behavior: 'smooth' });

  // Update status bar
  if (typeof updateStatusBar === 'function') updateStatusBar();
}


let currentOffset = 0;
const PAGE_SIZE = 100;
let searchTimeout = null;
let _freshnessMap = {};
let _currentSort = 'score';
let _currentOrder = 'desc';
let _totalResults = 0;
let _selectedIds = new Set();
let _currentLeads = []; // Cache current page leads

// --- Refresh All ---
async function refreshAll() {
  const bar = document.getElementById('refresh-bar');
  bar.style.width = '5%';
  const loaders = [loadStats, loadScores, loadRecommendations, loadFreshness, loadActivity, loadPracticeAreas, loadCompleteness, loadLists, loadEnrichmentDashboard, loadActivityFeed, loadQualityAlerts, loadExportHistory, loadGrowthChart, loadFirmIntel, loadHealthScore, loadPipeline, loadSegments, loadSchedules, loadWebhooks, loadEnrichmentQueue, loadVerificationStats, loadExportTemplates, loadAnalytics, loadEmailClassification, loadConfidencePanel, loadStalenessPanel, loadTagDefinitions, loadChangeSignals, loadIcpPanel, loadSavedSearches, loadAdmissionSignals, loadSequences, loadFirmDirectory, loadEngagementPanel, loadDncPanel, loadSmartDuplicates, loadTerritories, loadSourceAttribution, loadDecayPreview, loadIntentSignals, loadRoutingRules, loadCompletenessHeatmap, loadRecentNotes, loadSmartLists, loadKpiMetrics, loadScoringModels, loadCampaigns, loadCrossSourceDuplicates, loadEngagementHeatmap, loadOwners, loadLeaderboard, loadAutomationRules, loadDataQuality, loadExportProfiles, loadRecentContacts, loadWarmUpScores, loadFilterFacets, loadEnrichQueue, loadFirmIntelligence, loadDedupQueue, loadAuditLog, loadLifecycleAnalytics, loadSequencePerformance, loadActivityScores, loadBulkEnrichmentRuns, loadFirmNetwork, loadFreshnessMonitor, loadScoringRankings, loadGeoClusters, loadPriorityInbox, loadPracticeAnalytics, loadSourceROI, loadComplianceDashboard, loadPredictiveScores, loadTeamPerformance, loadEmailDeliverability, loadTagRulesEngine, loadNurtureCadence, loadCustomFields, loadScoreDecayConfig, loadLookalikes, loadConversionFunnel, loadLeadVelocity, loadCompletenessMatrix, loadLeadClusters, loadAbTests, loadReengagement, loadAttribution, loadSLATracker, loadSaturation, loadEnrichWaterfall, loadCompetitiveIntel, loadSeqTemplates, loadQualityRulesPanel, loadTimelinePanel, loadExportScheduler, loadPropensity, loadCohorts, loadChannelPrefs, loadBenchmarks, loadDealEstimates, loadOutreachCal, loadRiskScores, loadNetworkMap, loadJourneyMapping, loadScoringAudit, loadGeoExpansion, loadFreshnessAlerts2, loadMergeCandidates, loadOutreachAnalytics, loadIcpScoring, loadPipelineVelocity, loadRelationshipGraph, loadEnrichmentROI, loadEngagementPrediction, loadCampaignPerformance2, loadPrioritizationMatrix, loadFirmAggregation, loadImprovementRecs, loadLifecycleFunnel, loadCadenceOptimizer, loadScoringCalibration, loadPracticeMarketSize, loadPipelineHealthPanel, loadAffinityScoring, loadScraperGaps, loadFreshnessIndex, loadFirmGrowthPanel, loadRevenueAttribution, loadSaturationHeatmap2, loadSmartListBuilder, loadQualityScorecard, loadDedupIntelligence, loadOutboundReadiness, loadAgingReport, loadGrowthAnalytics2, loadWaterfallHistory, loadRecentlyEnriched, loadEnrichmentPriority, loadEnrichmentCoverage, loadEnrichmentFailures, loadAiBrief, loadDigest, loadSparklines];
  let done = 0;
  const total = loaders.length;
  const tracked = loaders.map(fn => fn().then(() => { done++; bar.style.width = Math.round((done / total) * 95) + '%'; }).catch(err => { done++; bar.style.width = Math.round((done / total) * 95) + '%'; console.warn('[refresh]', fn.name || 'loader', 'failed:', err.message); }));
  await Promise.all(tracked);
  bar.style.width = '100%';
  setTimeout(() => { bar.style.width = '0'; bar.style.transition = 'none'; requestAnimationFrame(() => { bar.style.transition = 'width 0.3s ease'; }); }, 500);
  loadCoverage();
  loadCoverageMap();
  loadGeoMap();
  loadLeads();
  _lastRefreshTime = Date.now();
  if (typeof updateStatusBar === 'function') updateStatusBar();
}

// --- Notification Center ---
let _notifications = JSON.parse(localStorage.getItem('mortar_notifs') || '[]');
let _notifOpen = false;

function addNotification(icon, text, type = 'info') {
  const notif = { id: Date.now(), icon, text, type, time: new Date().toISOString(), read: false };
  _notifications.unshift(notif);
  if (_notifications.length > 50) _notifications = _notifications.slice(0, 50);
  localStorage.setItem('mortar_notifs', JSON.stringify(_notifications));
  updateNotifBadge();
  renderNotifList();
  // Ring the bell
  const bell = document.getElementById('notif-bell');
  if (bell) { bell.classList.add('ringing'); setTimeout(() => bell.classList.remove('ringing'), 600); }
  // Also add to activity feed
  addActivityItem(icon, text, type);
}

function updateNotifBadge() {
  const badge = document.getElementById('notif-badge');
  const unread = _notifications.filter(n => !n.read).length;
  if (badge) badge.textContent = unread > 0 ? (unread > 9 ? '9+' : unread) : '';
}

function toggleNotifPanel() {
  _notifOpen = !_notifOpen;
  const panel = document.getElementById('notif-panel');
  panel.style.display = _notifOpen ? 'block' : 'none';
  if (_notifOpen) {
    _notifications.forEach(n => n.read = true);
    localStorage.setItem('mortar_notifs', JSON.stringify(_notifications));
    updateNotifBadge();
    renderNotifList();
  }
}

function renderNotifList() {
  const list = document.getElementById('notif-list');
  if (!list) return;
  if (_notifications.length === 0) {
    list.innerHTML = '<div class="notif-empty">No notifications yet</div>';
    return;
  }
  list.innerHTML = _notifications.slice(0, 30).map(n => {
    const ago = timeAgo(n.time);
    return `<div class="notif-item${n.read ? '' : ' unread'}">
      <span class="notif-icon">${n.icon}</span>
      <div class="notif-body">
        <div class="notif-text">${n.text}</div>
        <div class="notif-time">${ago}</div>
      </div>
    </div>`;
  }).join('');
}

function clearAllNotifs() {
  _notifications = [];
  localStorage.setItem('mortar_notifs', JSON.stringify(_notifications));
  updateNotifBadge();
  renderNotifList();
}

// timeAgo() defined in utils.js — reuse it

// Close notification panel when clicking outside
document.addEventListener('click', (e) => {
  if (_notifOpen && !e.target.closest('#notif-panel') && !e.target.closest('#notif-bell')) {
    _notifOpen = false;
    document.getElementById('notif-panel').style.display = 'none';
  }
});

// --- Activity Feed ---
let _activityItems = [];

function addActivityItem(icon, text, type = 'info') {
  const colors = { success: '#22c55e', error: '#ef4444', info: '#6366f1', warning: '#f59e0b', scrape: '#3b82f6' };
  _activityItems.unshift({ icon, text, type, color: colors[type] || colors.info, time: new Date().toISOString() });
  if (_activityItems.length > 30) _activityItems = _activityItems.slice(0, 30);
  renderActivityFeed();
}

function renderActivityFeed() {
  const feed = document.getElementById('activity-feed');
  if (!feed) return;
  if (_activityItems.length === 0) {
    feed.innerHTML = '<div style="padding:1rem;text-align:center;font-size:0.78rem;color:#9ca3af;">Activity will appear here as you work</div>';
    return;
  }
  feed.innerHTML = _activityItems.slice(0, 15).map(item => `
    <div class="activity-item">
      <div class="activity-dot" style="background:${item.color}"></div>
      <div class="activity-text">${item.text}</div>
      <span class="activity-time">${timeAgo(item.time)}</span>
    </div>
  `).join('');
}

async function loadActivityFeed() {
  try {
    const data = await fetch('/api/audit-log?limit=15').then(safeJSON);
    const items = (data.entries || data || []).slice(0, 15);
    const feed = document.getElementById('activity-feed');
    if (!feed) return;
    if (items.length === 0 && _activityItems.length === 0) {
      feed.innerHTML = '<div style="padding:1rem;text-align:center;font-size:0.78rem;color:#9ca3af;">No recent activity</div>';
      return;
    }
    // Merge audit log items with live items
    const auditIcons = { create: '&#x2795;', update: '&#x270F;&#xFE0F;', delete: '&#x1F5D1;', merge: '&#x1F500;', enrich: '&#x2728;', export: '&#x1F4E5;', import: '&#x1F4E4;', scrape: '&#x1F310;' };
    const auditColors = { create: '#22c55e', update: '#3b82f6', delete: '#ef4444', merge: '#8b5cf6', enrich: '#f59e0b', export: '#0ea5e9', import: '#ec4899', scrape: '#10b981' };
    const auditHtml = items.map(item => `
      <div class="activity-item">
        <div class="activity-dot" style="background:${auditColors[item.action] || '#6366f1'}"></div>
        <div class="activity-text">${auditIcons[item.action] || '&#x2022;'} <strong>${esc(item.action || 'action')}</strong> ${esc(item.details || item.description || '')} ${item.lead_name ? '— ' + esc(item.lead_name) : ''}</div>
        <span class="activity-time">${timeAgo(item.created_at || item.timestamp)}</span>
      </div>
    `).join('');
    // Show live items first, then audit items
    const liveHtml = _activityItems.slice(0, 5).map(item => `
      <div class="activity-item">
        <div class="activity-dot" style="background:${item.color}"></div>
        <div class="activity-text">${item.text}</div>
        <span class="activity-time">${timeAgo(item.time)}</span>
      </div>
    `).join('');
    feed.innerHTML = liveHtml + auditHtml;
  } catch (err) {
    renderActivityFeed(); // Fall back to live-only items
  }
}

// --- WebSocket Real-Time Updates ---
let _ws = null;
let _wsReconnectTimer = null;

function connectWebSocket() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  _ws = new WebSocket(`${protocol}//${location.host}/ws`);

  _ws.onopen = () => {
    console.log('[ws] Connected to server');
    _ws.send(JSON.stringify({ type: 'subscribe-all' }));
    const pulse = document.querySelector('#power-bar span:first-child span');
    if (pulse) pulse.style.background = '#22c55e';
    addActivityItem('&#x1F4E1;', 'Connected to server', 'success');
    updateStatusWs(true);
  };

  _ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'db-update') {
        const state = msg.state || 'Scrape';
        const newLeads = msg.stats?.netNew || 0;
        toast(`${state} complete: ${newLeads} new leads`, 'success');
        addNotification('&#x1F310;', `<strong>${esc(state)}</strong> scrape complete — ${newLeads} new leads added`, 'scrape');
        refreshAll();
      } else if (msg.type === 'validation-progress') {
        const pct = msg.total > 0 ? Math.round((msg.validated / msg.total) * 100) : 0;
        const bar = document.getElementById('validation-bar');
        if (bar) bar.style.width = pct + '%';
        const status = document.getElementById('validation-status');
        if (status) status.textContent = `Validating ${fmt(msg.validated)}/${fmt(msg.total)} (${msg.valid} valid, ${msg.invalid} invalid)`;
      } else if (msg.type === 'validation-complete') {
        addNotification('&#x2705;', `Email validation complete — ${msg.valid || 0} valid, ${msg.invalid || 0} invalid`, 'success');
      } else if (msg.type === 'lead') {
        const pbTotal = document.getElementById('pb-total');
        if (pbTotal) {
          const current = parseInt(pbTotal.textContent.replace(/,/g, '')) || 0;
          pbTotal.textContent = fmt(current + 1);
          pbTotal.style.color = '#86efac';
          setTimeout(() => { pbTotal.style.color = ''; }, 500);
        }
      } else if (msg.type === 'enrichment-progress') {
        if (msg.step === 'complete') {
          addNotification('&#x2728;', `Enrichment complete — ${msg.enriched || 0} leads enriched`, 'success');
        }
      } else if (msg.type === 'error') {
        addNotification('&#x274C;', `Error: ${esc(msg.message || 'Unknown error')}`, 'error');
      }
    } catch (err) { console.warn('ws message parse:', err.message); }
  };

  _ws.onclose = () => {
    console.log('[ws] Disconnected');
    const pulse = document.querySelector('#power-bar span:first-child span');
    if (pulse) pulse.style.background = '#f59e0b';
    addActivityItem('&#x26A0;', 'Disconnected from server — reconnecting...', 'warning');
    updateStatusWs(false);
    if (!_wsReconnectTimer) {
      _wsReconnectTimer = setTimeout(() => {
        _wsReconnectTimer = null;
        connectWebSocket();
      }, 5000);
    }
  };

  _ws.onerror = () => {};
}

// --- Status Bar ---
let _lastRefreshTime = null;
function updateStatusBar() {
  const tabEl = document.getElementById('status-tab');
  const countEl = document.getElementById('status-count');
  const refreshEl = document.getElementById('status-refresh');
  if (tabEl) tabEl.textContent = _activeTab.charAt(0).toUpperCase() + _activeTab.slice(1);
  if (countEl) countEl.textContent = _totalResults > 0 ? fmt(_totalResults) + ' leads' : '';
  if (refreshEl && _lastRefreshTime) {
    const ago = Math.floor((Date.now() - _lastRefreshTime) / 1000);
    refreshEl.textContent = ago < 5 ? 'Just refreshed' : ago < 60 ? ago + 's ago' : Math.floor(ago / 60) + 'm ago';
  }
}
function updateStatusWs(connected) {
  const el = document.getElementById('status-ws');
  if (!el) return;
  el.innerHTML = connected
    ? '<span style="width:6px;height:6px;border-radius:50%;background:#22c55e;display:inline-block;"></span> Connected'
    : '<span style="width:6px;height:6px;border-radius:50%;background:#f59e0b;display:inline-block;"></span> Reconnecting';
}
setInterval(updateStatusBar, 10000);

// Initial load
renderViews();
switchMainTab('overview');
updateNotifBadge();
renderNotifList();
renderActivityFeed();
refreshAll();
connectWebSocket();

// Auto-refresh every 60s
setInterval(refreshAll, 60000);

// Waterfall live progress bar (polls every 5s when running)
let _waterfallPollInterval = null;
async function pollWaterfallStatus() {
  try {
    const res = await fetch('/api/leads/waterfall/status');
    const d = await safeJSON(res);
    const bar = document.getElementById('waterfall-live-bar');
    if (d.running) {
      bar.style.display = '';
      const p = d.progress || {};
      const pct = p.total > 0 ? Math.round((p.current / p.total) * 100) : 0;
      document.getElementById('waterfall-live-fill').style.width = pct + '%';
      document.getElementById('waterfall-live-text').textContent =
        `${p.step || 'Working'}: ${p.current || 0}/${p.total || 0} (${pct}%)${p.detail ? ' — ' + p.detail : ''}`;
      if (!_waterfallPollInterval) {
        _waterfallPollInterval = setInterval(pollWaterfallStatus, 5000);
      }
    } else {
      if (_waterfallPollInterval) {
        clearInterval(_waterfallPollInterval);
        _waterfallPollInterval = null;
      }
      if (d.progress && d.progress.saved > 0) {
        bar.style.display = '';
        document.getElementById('waterfall-live-fill').style.width = '100%';
        document.getElementById('waterfall-live-text').textContent = d.progress.step || 'Done';
        setTimeout(() => { bar.style.display = 'none'; }, 10000);
        refreshAll(); // Refresh data after enrichment completes
      } else {
        bar.style.display = 'none';
      }
    }
  } catch { /* silently fail */ }
}
pollWaterfallStatus();
setInterval(pollWaterfallStatus, 30000); // Check every 30s even when not actively polling
