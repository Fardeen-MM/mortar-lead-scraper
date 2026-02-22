#!/usr/bin/env node
// Comprehensive API endpoint tester
// Tests all GET endpoints and POST endpoints with minimal/empty bodies

const http = require('http');

const BASE = 'http://localhost:3000';

function req(method, path, body) {
  return new Promise((resolve) => {
    const url = new URL(path, BASE);
    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method,
      headers: { 'Content-Type': 'application/json' },
      timeout: 10000,
    };
    const r = http.request(options, (res) => {
      let data = '';
      res.on('data', (c) => data += c);
      res.on('end', () => resolve({ status: res.statusCode, data: data.substring(0, 200) }));
    });
    r.on('error', (e) => resolve({ status: 0, data: e.message }));
    r.on('timeout', () => { r.destroy(); resolve({ status: 0, data: 'TIMEOUT' }); });
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

// All GET endpoints extracted from server.js
const GET_ENDPOINTS = [
  '/api/config',
  '/api/health',
  '/api/signals',
  '/api/signals/scan-status',
  '/api/scrape/bulk/status',
  '/api/leads/find-websites/status',
  '/api/leads/stats',
  '/api/leads',
  '/api/leads/coverage',
  '/api/leads/merge-preview',
  '/api/leads/export',
  '/api/leads/export/instantly',
  '/api/leads/prepopulate/status',
  '/api/leads/freshness',
  '/api/leads/recommendations',
  '/api/leads/scores',
  '/api/leads/enrich-all/status',
  '/api/leads/export/smartlead',
  '/api/leads/export/by-score',
  '/api/leads/verify-status',
  '/api/leads/activity',
  '/api/leads/practice-areas',
  '/api/leads/tags',
  '/api/leads/health-score',
  '/api/leads/top-firms',
  '/api/leads/changelog',
  '/api/exports/history',
  '/api/quality/alerts',
  '/api/quality/summary',
  '/api/pipeline/stats',
  '/api/schedules',
  '/api/segments',
  '/api/export-templates',
  '/api/analytics/funnel',
  '/api/analytics/source-effectiveness',
  '/api/leads/suggest',
  '/api/scoring/rules',
  '/api/leads/verification-stats',
  '/api/webhooks',
  '/api/leads/email-classification',
  '/api/leads/confidence',
  '/api/leads/changes',
  '/api/leads/firm-changes',
  '/api/tag-definitions',
  '/api/leads/compare',
  '/api/leads/staleness',
  '/api/icp/criteria',
  '/api/icp/distribution',
  '/api/saved-searches',
  '/api/saved-searches/alerts',
  '/api/signals/admissions',
  '/api/signals/admission-stats',
  '/api/sequences',
  '/api/leads/most-engaged',
  '/api/firms/directory',
  '/api/leads/decay-preview',
  '/api/dnc',
  '/api/dnc/check',
  '/api/leads/smart-duplicates',
  '/api/territories',
  '/api/leads/source-attribution',
  '/api/leads/intent-signals',
  '/api/leads/practice-trends',
  '/api/routing-rules',
  '/api/leads/completeness-heatmap',
  '/api/leads/enrichment-recommendations',
  '/api/leads/export/instantly/preview',
  '/api/notes/recent',
  '/api/smart-lists',
  '/api/scoring-models',
  '/api/campaigns',
  '/api/leads/cross-source-duplicates',
  '/api/leads/kpi-metrics',
  '/api/leads/engagement-heatmap',
  '/api/leads/engagement-timeline',
  '/api/owners',
  '/api/leads/leaderboard',
  '/api/leads/leaderboard-by-state',
  '/api/automation-rules',
  '/api/leads/data-quality',
  '/api/leads/data-quality-summary',
  '/api/export-profiles',
  '/api/contacts/recent',
  '/api/leads/warm-up-batch',
  '/api/leads/kanban',
  '/api/leads/card-view',
  '/api/leads/typeahead',
  '/api/leads/filter-facets',
  '/api/enrichment-queue/status',
  '/api/firms',
  '/api/dedup/queue',
  '/api/dedup/stats',
  '/api/audit/log',
  '/api/audit/stats',
  '/api/audit/export',
  '/api/lifecycle/analytics',
  '/api/sequences/performance',
  '/api/activity-scores/batch',
  '/api/activity-scores/config',
  '/api/bulk-enrichment/runs',
  '/api/firm-network',
  '/api/freshness/report',
  '/api/scoring-models/compare',
  '/api/scoring-models/rankings',
  '/api/geographic/clusters',
  '/api/priority-inbox',
  '/api/smart-recommendations',
  '/api/practice-areas/analytics',
  '/api/source-roi',
  '/api/compliance/dashboard',
  '/api/predictive-scores',
  '/api/team/performance',
  '/api/email/deliverability',
  '/api/tag-rules',
  '/api/nurture/cadence',
  '/api/nurture/analytics',
  '/api/custom-fields',
  '/api/custom-fields/stats',
  '/api/score-decay/config',
  '/api/score-decay/preview',
  '/api/funnel',
  '/api/velocity',
  '/api/completeness-matrix',
  '/api/clusters',
  '/api/ab-tests',
  '/api/reengagement',
  '/api/attribution',
  '/api/sla',
  '/api/saturation',
  '/api/enrichment-waterfall',
  '/api/competitive',
  '/api/sequence-templates',
  '/api/quality-rules',
  '/api/export-schedules',
  '/api/propensity',
  '/api/cohorts',
  '/api/channel-preferences',
  '/api/benchmarks',
  '/api/deals',
  '/api/outreach-calendar',
  '/api/risk-scores',
  '/api/network-map',
  '/api/journey-mapping',
  '/api/scoring-audit',
  '/api/geo-expansion',
  '/api/freshness-alerts',
  '/api/merge-candidates',
  '/api/outreach-analytics',
  '/api/icp-scoring',
  '/api/pipeline-velocity',
  '/api/relationship-graph',
  '/api/enrichment-roi',
  '/api/engagement-prediction',
  '/api/campaign-performance',
  '/api/prioritization-matrix',
  '/api/firm-aggregation',
  '/api/improvement-recs',
  '/api/lifecycle-funnel',
  '/api/cadence-optimizer',
  '/api/scoring-calibration',
  '/api/practice-market-size',
  '/api/pipeline-health',
  '/api/affinity-scoring',
  '/api/scraper-gaps',
  '/api/freshness-index',
  '/api/firm-growth',
  '/api/revenue-attribution',
  '/api/saturation-heatmap',
  '/api/smart-list-builder',
  '/api/quality-scorecard',
  '/api/dedup-intelligence',
  '/api/outbound-readiness',
  '/api/aging-report',
  '/api/growth-analytics',
  '/api/table-config',
  '/api/leads/sources',
  '/api/lists',
  '/api/scrapers/health',
  '/api/leads/enrichment-stats',
  '/api/activity',
  '/api/leads/growth',
  '/api/leads/completeness',
  '/api/leads/duplicates',
  '/api/leads/waterfall/status',
  '/api/leads/waterfall/history',
  '/api/leads/waterfall/summary',
  '/api/leads/enrichment-priority',
  '/api/leads/enrichment-coverage',
  '/api/leads/recently-enriched',
  '/api/leads/enrichment-failures',
];

// GET endpoints with path params (test with dummy values)
const GET_PARAM_ENDPOINTS = [
  '/api/leads/state-details/FL',
  '/api/leads/changelog/1',
  '/api/leads/1/score-breakdown',
  '/api/leads/validate-email/test@test.com',
  '/api/leads/1/notes',
  '/api/leads/1/timeline',
  '/api/leads/1/engagement',
  '/api/leads/1/activities',
  '/api/leads/1/change-history',
  '/api/leads/1/lookalikes',
  '/api/leads/1/contacts',
  '/api/leads/1/contact-stats',
  '/api/leads/1/warm-up',
  '/api/leads/1/engagement-sparkline',
  '/api/leads/1/enrichment-info',
  '/api/leads/1/custom-fields',
  '/api/leads/1/lifecycle',
  '/api/leads/1/activity-score',
  '/api/leads/1/relationships',
  '/api/leads/1/freshness',
  '/api/leads/1/journey',
  '/api/leads/1',
  '/api/owners/test/leads',
  '/api/firms/test',
  '/api/webhooks/1/deliveries',
  '/api/geographic/penetration/FL',
  '/api/source-roi/compare?sources=bar,martindale',
  '/api/compliance/check/test@test.com',
  '/api/sequences/1/variants',
  '/api/sequences/1/enrollments',
  '/api/sequences/1/analytics',
  '/api/merge-preview/1/2',
  '/api/bulk-enrichment/diff/1',
  '/api/smart-lists/1/leads',
  '/api/campaigns/1/leads',
  '/api/territories/1/leads',
  '/api/lists/1',
  '/api/scrape/test-id/status',
  '/api/pipeline/stage/new',
  '/api/export-profiles/1/run',
  '/api/sequence-templates/1/render/1',
  '/api/leads/1/timeline',
];

// POST endpoints with minimal bodies
const POST_ENDPOINTS = [
  ['/api/quality/check', {}],
  ['/api/segments/query', { conditions: [] }],
  ['/api/segments/query/leads', { conditions: [] }],
  ['/api/leads/classify-emails', {}],
  ['/api/leads/compute-confidence', {}],
  ['/api/leads/auto-tag', {}],
  ['/api/icp/score-all', {}],
  ['/api/leads/validate-emails', { emails: [] }],
  ['/api/webhooks/test', { url: 'http://localhost:9999', event: 'test' }],
  ['/api/leads/bulk-update', { ids: [], updates: {} }],
  ['/api/leads/compare', { ids: [1, 2] }],
  ['/api/leads/merge-with-picks', { winnerId: 1, loserId: 2, picks: {} }],
  ['/api/pipeline/move', { leadId: 1, stage: 'new' }],
  ['/api/pipeline/bulk-move', { ids: [], stage: 'new' }],
  ['/api/leads/apply-decay', {}],
  ['/api/routing-rules/run', {}],
  ['/api/leads/auto-merge', {}],
  ['/api/dedup/scan', {}],
  ['/api/audit/log', { action: 'test', details: 'test' }],
  ['/api/freshness/verify', { ids: [] }],
  ['/api/scoring-models/apply', {}],
  ['/api/bulk-enrichment/run', { source: 'test' }],
  ['/api/score-decay/run', {}],
  ['/api/automation-rules/run', {}],
  ['/api/quality-rules/run', {}],
  ['/api/tag-rules/run', {}],
  ['/api/leads/bulk/tag', { ids: [], tag: 'test' }],
  ['/api/leads/bulk/remove-tag', { ids: [], tag: 'test' }],
  ['/api/leads/bulk/assign-owner', { ids: [], owner: 'test' }],
  ['/api/compliance/opt-out', { email: 'test@test.com' }],
  ['/api/compliance/consent', { email: 'test@test.com', type: 'email' }],
  ['/api/enrichment-queue/add', { ids: [] }],
  ['/api/enrichment-queue/process', {}],
  ['/api/merge-execute', { id1: 1, id2: 2 }],
  ['/api/leads/score', {}],
];

async function main() {
  let pass = 0, fail = 0, errors = [];
  const total = GET_ENDPOINTS.length + GET_PARAM_ENDPOINTS.length + POST_ENDPOINTS.length;

  console.log(`Testing ${total} endpoints...\n`);

  // Test GET endpoints (no params)
  console.log('=== GET ENDPOINTS (no params) ===');
  for (const path of GET_ENDPOINTS) {
    const r = await req('GET', path);
    const ok = r.status >= 200 && r.status < 500; // 4xx is OK (validation), 5xx is bad
    if (r.status === 200) {
      pass++;
    } else if (r.status >= 500 || r.status === 0) {
      fail++;
      errors.push({ method: 'GET', path, status: r.status, data: r.data });
      console.log(`  FAIL ${r.status} GET ${path}: ${r.data.substring(0, 100)}`);
    } else {
      pass++; // 3xx/4xx are acceptable
    }
  }
  console.log(`  Completed: ${GET_ENDPOINTS.length} endpoints\n`);

  // Test GET endpoints with params
  console.log('=== GET ENDPOINTS (with params) ===');
  for (const path of GET_PARAM_ENDPOINTS) {
    const r = await req('GET', path);
    if (r.status === 200 || (r.status >= 400 && r.status < 500)) {
      pass++;
    } else if (r.status >= 500 || r.status === 0) {
      fail++;
      errors.push({ method: 'GET', path, status: r.status, data: r.data });
      console.log(`  FAIL ${r.status} GET ${path}: ${r.data.substring(0, 100)}`);
    } else {
      pass++;
    }
  }
  console.log(`  Completed: ${GET_PARAM_ENDPOINTS.length} endpoints\n`);

  // Test POST endpoints
  console.log('=== POST ENDPOINTS ===');
  for (const [path, body] of POST_ENDPOINTS) {
    const r = await req('POST', path, body);
    if (r.status === 200 || (r.status >= 400 && r.status < 500)) {
      pass++;
    } else if (r.status >= 500 || r.status === 0) {
      fail++;
      errors.push({ method: 'POST', path, status: r.status, data: r.data });
      console.log(`  FAIL ${r.status} POST ${path}: ${r.data.substring(0, 100)}`);
    } else {
      pass++;
    }
  }
  console.log(`  Completed: ${POST_ENDPOINTS.length} endpoints\n`);

  // Summary
  console.log('========================================');
  console.log(`TOTAL: ${pass + fail}/${total}`);
  console.log(`PASS:  ${pass}`);
  console.log(`FAIL:  ${fail}`);
  console.log('========================================');

  if (errors.length > 0) {
    console.log('\n=== FAILURES (500/timeout) ===');
    for (const e of errors) {
      console.log(`  ${e.method} ${e.path} → ${e.status}: ${e.data.substring(0, 150)}`);
    }
  }
}

main().catch(console.error);
