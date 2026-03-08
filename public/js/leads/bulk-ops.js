// bulk-ops.js — Bulk operations: scrape, verify, merge, waterfall, website finder, enrich
// Dependencies: utils.js, app.js

// --- Bulk Scrape ---
async function startBulkScrape(test = false) {
  const btn = document.getElementById(test ? 'bulk-test-btn' : 'bulk-btn');
  btn.disabled = true;
  btn.textContent = 'Starting...';

  try {
    const res = await fetch('/api/scrape/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ test }),
    });
    const d = await safeJSON(res);
    if (d.status === 'already_running') {
      btn.textContent = 'Already running';
    } else {
      btn.textContent = 'Running...';
    }
    pollBulkStatus(btn, test);
  } catch (err) {
    btn.textContent = test ? 'Test Mode' : 'Scrape All';
    btn.disabled = false;
  }
}

function pollBulkStatus(btn, test) {
  const statusEl = document.getElementById('bulk-status');
  const interval = setInterval(async () => {
    try {
      const res = await fetch('/api/scrape/bulk/status');
      const d = await safeJSON(res);
      if (d.running) {
        const pct = d.total > 0 ? Math.round((d.completed + d.failed + d.skipped) / d.total * 100) : 0;
        statusEl.innerHTML = `<strong>${esc(d.currentScraper)}</strong> | ${d.completed + d.failed + d.skipped}/${d.total} (${pct}%) | ${fmt(d.totalLeads)} leads | ${fmt(d.totalNew)} new`;
        btn.textContent = `${esc(d.currentScraper)}... (${pct}%)`;
      } else {
        clearInterval(interval);
        btn.textContent = test ? 'Test Mode' : 'Scrape All';
        btn.disabled = false;
        if (d.totalLeads > 0) {
          statusEl.innerHTML = `<strong>Done</strong> | ${d.completed} scrapers | ${fmt(d.totalLeads)} leads | ${fmt(d.totalNew)} new`;
        }
        refreshAll();
      }
    } catch {
      clearInterval(interval);
      btn.textContent = test ? 'Test Mode' : 'Scrape All';
      btn.disabled = false;
    }
  }, 5000);
}

// --- Email Verify ---
async function startVerify() {
  const btn = document.getElementById('email-btn');
  btn.disabled = true;
  btn.textContent = 'Starting...';
  try {
    const res = await fetch('/api/leads/verify-emails', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"limit":200}' });
    const d = await safeJSON(res);
    if (d.status === 'already_running') {
      btn.textContent = 'Already running';
    } else {
      btn.textContent = 'Running...';
      pollVerifyStatus(btn);
    }
  } catch {
    btn.textContent = 'Find Emails (SMTP)';
    btn.disabled = false;
  }
}

function pollVerifyStatus(btn) {
  const interval = setInterval(async () => {
    try {
      const res = await fetch('/api/leads/verify-status');
      const d = await safeJSON(res);
      const p = d.progress || {};
      document.getElementById('verify-status').textContent = `${p.current || 0}/${p.total || 0} processed, ${p.found || 0} found`;
      if (!d.running) {
        clearInterval(interval);
        btn.textContent = 'Find Emails (SMTP)';
        btn.disabled = false;
        document.getElementById('verify-status').textContent = `Done: ${p.found || 0} emails found`;
        refreshAll();
      }
    } catch {
      clearInterval(interval);
      btn.textContent = 'Find Emails (SMTP)';
      btn.disabled = false;
    }
  }, 3000);
}

// --- Merge Duplicates ---
async function autoMergeAll() {
  if (!confirm('Auto-merge all high-confidence duplicates (90%+ match)? This cannot be undone.')) return;
  try {
    const result = await fetch('/api/leads/auto-merge', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confidenceThreshold: 90 })
    }).then(safeJSON);
    toast(`Auto-merged ${result.merged} duplicates (${result.fieldsRecovered} fields recovered)`, 'success');
    refreshAll();
  } catch (err) { toast('Auto-merge failed', 'error'); }
}

async function mergeDuplicates() {
  const btn = document.getElementById('dedup-btn');
  btn.disabled = true;
  btn.textContent = 'Merging...';
  const statusEl = document.getElementById('bulk-status');
  try {
    const res = await fetch('/api/leads/merge-duplicates', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dryRun: false }),
    });
    const d = await safeJSON(res);
    statusEl.textContent = `Merged ${d.merged} duplicates (${d.fieldsRecovered} fields recovered)`;
    toast(`Merged ${d.merged} duplicates`);
    refreshAll();
  } catch {
    statusEl.textContent = 'Merge failed';
  }
  btn.textContent = 'Merge Dupes';
  btn.disabled = false;
}

// --- Website Finder ---
async function startWebsiteFinder() {
  const btn = document.getElementById('website-btn');
  btn.disabled = true;
  btn.textContent = 'Finding...';
  try {
    const res = await fetch('/api/leads/find-websites', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ limit: 500 }),
    });
    const d = await safeJSON(res);
    if (d.status === 'already_running') btn.textContent = 'Already running';
    pollWebsiteStatus(btn);
  } catch {
    btn.textContent = 'Find Websites';
    btn.disabled = false;
  }
}

function pollWebsiteStatus(btn) {
  const statusEl = document.getElementById('bulk-status');
  const interval = setInterval(async () => {
    try {
      const res = await fetch('/api/leads/find-websites/status');
      const d = await safeJSON(res);
      const p = d.progress || {};
      if (d.running) {
        statusEl.textContent = `Websites: ${p.current || 0}/${p.total || 0} checked, ${p.found || 0} found`;
      } else {
        clearInterval(interval);
        btn.textContent = 'Find Websites';
        btn.disabled = false;
        statusEl.textContent = `Website finder done: ${p.found || 0} websites found`;
        refreshAll();
      }
    } catch {
      clearInterval(interval);
      btn.textContent = 'Find Websites';
      btn.disabled = false;
    }
  }, 3000);
}

// --- Pre-populate ---
async function startPrepopulate() {
  const btn = document.getElementById('prepop-btn');
  btn.disabled = true;
  btn.textContent = 'Starting...';
  try {
    const res = await fetch('/api/leads/prepopulate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sources: ['AVVO', 'FINDLAW'], test: true }),
    });
    const d = await safeJSON(res);
    if (d.status === 'already_running') btn.textContent = 'Already running';
    else btn.textContent = 'Running...';
    pollPrepopStatus(btn);
  } catch {
    btn.textContent = 'Pre-populate DB';
    btn.disabled = false;
  }
}

function pollPrepopStatus(btn) {
  const statusEl = document.getElementById('bulk-status');
  const interval = setInterval(async () => {
    try {
      const res = await fetch('/api/leads/prepopulate/status');
      const d = await safeJSON(res);
      const p = d.progress || {};
      if (d.running) {
        statusEl.innerHTML = `Pre-populating <strong>${esc(p.currentSource)}</strong> (${p.current}/${p.total}) | ${fmt(p.totalLeads)} leads | ${fmt(p.totalNew)} new`;
        btn.textContent = `${esc(p.currentSource)}... (${p.current}/${p.total})`;
      } else {
        clearInterval(interval);
        btn.textContent = 'Pre-populate DB';
        btn.disabled = false;
        if (p.totalLeads > 0) {
          statusEl.innerHTML = `Pre-population done: ${fmt(p.totalLeads)} leads, ${fmt(p.totalNew)} new in DB`;
        }
        refreshAll();
      }
    } catch {
      clearInterval(interval);
      btn.textContent = 'Pre-populate DB';
      btn.disabled = false;
    }
  }, 5000);
}

// --- Waterfall Enrich (profile pages + cross-ref + name lookups) ---
async function startWaterfall() {
  const btn = document.getElementById('waterfall-btn');
  btn.disabled = true;
  btn.textContent = 'Starting...';
  const statusEl = document.getElementById('bulk-status');

  try {
    const res = await fetch('/api/leads/waterfall', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ limit: 200, fetchProfiles: true, crossRefMartindale: true, crossRefLawyersCom: true, nameLookups: true }),
    });
    const d = await safeJSON(res);
    if (d.status === 'already_running') {
      btn.textContent = 'Already running';
      setTimeout(() => { btn.textContent = 'Waterfall Enrich'; btn.disabled = false; }, 3000);
      return;
    }

    btn.textContent = 'Running...';
    const interval = setInterval(async () => {
      try {
        const res2 = await fetch('/api/leads/waterfall/status');
        const d2 = await safeJSON(res2);
        const p = d2.progress || {};
        if (d2.running) {
          const pct = p.total > 0 ? Math.round((p.current / p.total) * 100) : 0;
          statusEl.innerHTML = `<span style="color:#8b5cf6;">Waterfall:</span> <strong>${esc(p.step || '')}</strong> ${p.current || 0}/${p.total || 0} (${pct}%)${p.detail ? ' — ' + esc(p.detail) : ''}`;
          btn.textContent = `Running ${pct}%`;
        } else {
          clearInterval(interval);
          btn.textContent = 'Waterfall Enrich';
          btn.disabled = false;
          const s = p.stats || {};
          statusEl.innerHTML = `<strong style="color:#8b5cf6;">Waterfall done</strong> | ${s.totalFieldsFilled || 0} fields filled | ${s.profilesFetched || 0} profiles | ${s.crossRefMatches || 0} cross-refs | ${p.saved || 0} leads saved`;
          refreshAll();
        }
      } catch {
        clearInterval(interval);
        btn.textContent = 'Waterfall Enrich';
        btn.disabled = false;
      }
    }, 3000);
  } catch (err) {
    statusEl.textContent = 'Waterfall error: ' + err.message;
    btn.textContent = 'Waterfall Enrich';
    btn.disabled = false;
  }
}

// --- Multi-State Waterfall Enrich ---
async function startMultiWaterfall() {
  const btn = document.getElementById('multi-waterfall-btn');
  btn.disabled = true;
  btn.textContent = 'Starting...';
  const statusEl = document.getElementById('bulk-status');

  try {
    const res = await fetch('/api/leads/waterfall/multi', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ limitPerState: 200, maxStates: 10 }),
    });
    const d = await safeJSON(res);
    if (d.status === 'already_running') {
      btn.textContent = 'Already running';
      setTimeout(() => { btn.textContent = 'Multi-State Enrich'; btn.disabled = false; }, 3000);
      return;
    }

    btn.textContent = 'Running...';
    const interval = setInterval(async () => {
      try {
        const res2 = await fetch('/api/leads/waterfall/status');
        const d2 = await safeJSON(res2);
        const p = d2.progress || {};
        if (d2.running) {
          const pct = p.total > 0 ? Math.round((p.current / p.total) * 100) : 0;
          statusEl.innerHTML = `<span style="color:#059669;">Multi-State:</span> <strong>${esc(p.step || '')}</strong> ${p.current || 0}/${p.total || 0} states (${pct}%)${p.detail ? '<br><span style="font-size:0.72rem;color:#78716c;">' + esc(p.detail) + '</span>' : ''}`;
          btn.textContent = `Running ${pct}%`;
        } else {
          clearInterval(interval);
          btn.textContent = 'Multi-State Enrich';
          btn.disabled = false;
          const s = p.stats || {};
          statusEl.innerHTML = `<strong style="color:#059669;">Multi-state done</strong> | ${s.totalFieldsFilled || 0} fields filled | ${s.statesProcessed || 0} states | ${p.saved || 0} leads updated`;
          refreshAll();
        }
      } catch {
        clearInterval(interval);
        btn.textContent = 'Multi-State Enrich';
        btn.disabled = false;
      }
    }, 5000);
  } catch (err) {
    statusEl.textContent = 'Multi-state error: ' + err.message;
    btn.textContent = 'Multi-State Enrich';
    btn.disabled = false;
  }
}

// --- Enrich All ---
async function enrichAll() {
  const btn = document.getElementById('enrich-all-btn');
  btn.disabled = true;
  btn.textContent = 'Enriching...';
  const statusEl = document.getElementById('bulk-status');

  try {
    const res = await fetch('/api/leads/enrich-all', { method: 'POST' });
    const d = await safeJSON(res);
    if (d.status === 'already_running') { btn.textContent = 'Already running'; return; }
    const interval = setInterval(async () => {
      try {
        const res2 = await fetch('/api/leads/enrich-all/status');
        const d2 = await res2.json();
        const p = d2.progress || {};
        if (d2.running) {
          statusEl.innerHTML = `Enriching: <strong>${esc(p.step)}</strong>`;
        } else {
          clearInterval(interval);
          btn.textContent = 'Enrich All';
          btn.disabled = false;
          const stepSummary = (p.steps || []).map(s => `${s.name}: ${s.result}`).join(' | ');
          statusEl.innerHTML = `<strong>Enrichment done</strong> | ${p.totalUpdated} updates | ${stepSummary}`;
          refreshAll();
        }
      } catch {
        clearInterval(interval);
        btn.textContent = 'Enrich All';
        btn.disabled = false;
      }
    }, 2000);
  } catch {
    btn.textContent = 'Enrich All';
    btn.disabled = false;
  }
}

// --- Share Firm Data ---
async function shareFirmData() {
  const btn = document.getElementById('firm-share-btn');
  btn.disabled = true;
  btn.textContent = 'Sharing...';
  const statusEl = document.getElementById('bulk-status');
  try {
    const res = await fetch('/api/leads/share-firm-data', { method: 'POST' });
    const d = await safeJSON(res);
    statusEl.textContent = `Shared data across ${d.firmsProcessed} firms, ${d.leadsUpdated} leads updated, ${d.fieldsShared} fields shared`;
    toast(`Shared firm data: ${d.leadsUpdated} leads updated`);
    refreshAll();
  } catch {
    statusEl.textContent = 'Firm data sharing failed';
  }
  btn.textContent = 'Share Firm Data';
  btn.disabled = false;
}

// --- Deduce Websites ---
async function deduceWebsites() {
  const btn = document.getElementById('deduce-btn');
  btn.disabled = true;
  btn.textContent = 'Deducing...';
  const statusEl = document.getElementById('bulk-status');
  try {
    const res = await fetch('/api/leads/deduce-websites', { method: 'POST' });
    const d = await safeJSON(res);
    statusEl.textContent = `Deduced ${d.leadsUpdated} websites from email domains`;
    toast(`Deduced ${d.leadsUpdated} websites`);
    refreshAll();
  } catch {
    statusEl.textContent = 'Website deduction failed';
  }
  btn.textContent = 'Deduce Websites';
  btn.disabled = false;
}

// --- Score Leads ---
async function scoreLeads() {
  const btn = document.getElementById('score-btn');
  btn.disabled = true;
  btn.textContent = 'Scoring...';
  const statusEl = document.getElementById('bulk-status');
  try {
    const res = await fetch('/api/leads/score', { method: 'POST' });
    const d = await safeJSON(res);
    statusEl.textContent = `Scored ${fmt(d.scored)} leads (avg: ${d.avgScore})`;
    toast(`Scored ${fmt(d.scored)} leads (avg: ${d.avgScore})`);
    refreshAll();
  } catch {
    statusEl.textContent = 'Scoring failed';
  }
  btn.textContent = 'Score All';
  btn.disabled = false;
}

