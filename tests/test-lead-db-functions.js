#!/usr/bin/env node
/**
 * Comprehensive test of all lead-db.js exported functions.
 * Tests that every function can be called without throwing.
 */

// Use a test DB to avoid modifying production data
process.env.LEAD_DB_PATH = '/tmp/test-lead-db-' + Date.now() + '.sqlite';

const leadDb = require('../lib/lead-db');

let pass = 0, fail = 0, skip = 0;
const errors = [];

function test(name, fn) {
  try {
    const result = fn();
    // Handle async functions
    if (result && typeof result.then === 'function') {
      return result.then(() => {
        pass++;
      }).catch(err => {
        fail++;
        errors.push({ name, error: err.message });
        console.log(`  FAIL ${name}: ${err.message}`);
      });
    }
    pass++;
    return Promise.resolve();
  } catch (err) {
    fail++;
    errors.push({ name, error: err.message });
    console.log(`  FAIL ${name}: ${err.message}`);
    return Promise.resolve();
  }
}

async function main() {
  console.log('Testing lead-db.js functions...\n');

  // First, seed some test data
  console.log('=== Seeding test data ===');
  const testLead = {
    first_name: 'John', last_name: 'Smith',
    email: 'john.smith@example.com', phone: '5551234567',
    firm_name: 'Smith & Associates', city: 'Houston', state: 'TX',
    practice_area: 'Personal Injury', bar_number: 'TX12345',
    website: 'https://smithlaw.com', source: 'texas_bar',
    primary_source: 'texas_bar', country: 'US',
  };
  leadDb.upsertLead(testLead);
  leadDb.upsertLead({
    ...testLead, first_name: 'Jane', last_name: 'Doe',
    email: 'jane.doe@lawfirm.com', bar_number: 'TX54321',
    city: 'Dallas',
  });
  leadDb.upsertLead({
    ...testLead, first_name: 'Bob', last_name: 'Jones',
    email: '', phone: '', bar_number: 'FL99999',
    state: 'FL', city: 'Miami',
  });
  console.log('  Seeded 3 test leads\n');

  // === No-arg getter functions (the bulk of what we test) ===
  console.log('=== No-arg getters ===');
  const noArgGetters = [
    'getStats', 'getStateCoverage', 'getScoreDistribution',
    'getRecommendations', 'getRecentActivity', 'getDistinctPracticeAreas',
    'getDistinctTags', 'getDatabaseHealth', 'getFieldCompleteness',
    'getScraperHealth', 'getEnrichmentStats', 'getActivityFeed',
    'getDistinctSources', 'getTopFirms', 'getExportHistory',
    'getAlertSummary', 'getPipelineStats', 'getSchedules',
    'getSegments', 'getWebhooks', 'getScoringRules',
    'getVerificationStats', 'getExportTemplates',
    'getPipelineFunnel', 'getSourceEffectiveness',
    'getEmailClassification', 'getConfidenceDistribution',
    'getTagDefinitions', 'getStalenessReport',
    'getIcpCriteria', 'getIcpDistribution', 'getSavedSearches',
    'getRecentAdmissions', 'getAdmissionSignals', 'getSequences',
    'getMostEngagedLeads', 'getDecayPreview', 'getDncList',
    'findSmartDuplicates', 'getTerritories', 'getSourceAttribution',
    'getIntentSignals', 'getPracticeAreaTrends', 'getRoutingRules',
    'getCompletenessHeatmap', 'getEnrichmentRecommendations',
    'getRecentNotes', 'getSmartLists', 'getScoringModels',
    'getCampaigns', 'getCrossSourceDuplicates', 'getKpiMetrics',
    'getEngagementHeatmap', 'getEngagementTimeline',
    'getOwners', 'getLeaderboard', 'getLeaderboardByState',
    'getAutomationRules', 'getDataQualityReport', 'getDataQualitySummary',
    'getExportProfiles', 'getRecentContacts',
    'getFilterFacets', 'getEnrichmentQueueStatus',
    'getFirmIntelligence', 'getDedupQueue', 'getDedupStats',
    'getAuditStats', 'getLifecycleAnalytics',
    'getAllSequencePerformance', 'getActivityScoreConfig',
    'getBulkEnrichmentRuns', 'getFirmNetwork',
    'getFreshnessReport', 'getScoringModelRankings',
    'getGeographicClusters', 'getPriorityInbox',
    'getSmartRecommendations', 'getPracticeAreaAnalytics',
    'getSourceROI', 'getComplianceDashboard',
    'getPredictiveScores', 'getTeamPerformance',
    'getEmailDeliverability', 'getTagRules', 'getNurtureCadence',
    'getCadenceAnalytics', 'getCustomFieldDefs', 'getCustomFieldStats',
    'getDecayConfig', 'getConversionFunnel', 'getLeadVelocity',
    'getCompletenessMatrix', 'getLeadClusters', 'getAbTests',
    'getReengagementLeads', 'getAttributionModel',
    'getResponseTimeSLA', 'getMarketSaturation',
    'getEnrichmentWaterfall', 'getCompetitiveIntelligence',
    'getSequenceTemplates', 'getQualityRules',
    'getExportSchedules', 'getPropensityScores',
    'getCohortAnalysis', 'getChannelPreferences',
    'getJurisdictionBenchmarks', 'getDealEstimates',
    'getOutreachCalendar', 'getRiskScores', 'getNetworkMap',
    'getJourneyMapping', 'getScoringAudit', 'getGeoExpansion',
    'getFreshnessAlerts', 'getMergeCandidates',
    'getOutreachAnalytics', 'getIcpScoring', 'getPipelineVelocity',
    'getRelationshipGraph', 'getEnrichmentROI',
    'getEngagementPrediction', 'getCampaignPerformance',
    'getPrioritizationMatrix', 'getFirmAggregation',
    'getImprovementRecs', 'getLifecycleFunnel',
    'getCadenceOptimizer', 'getScoringCalibration',
    'getPracticeMarketSize', 'getPipelineHealth',
    'getAffinityScoring', 'getScraperGaps', 'getFreshnessIndex',
    'getFirmGrowth', 'getRevenueAttribution', 'getSaturationHeatmap',
    'getSmartListBuilder', 'getQualityScorecard',
    'getDedupIntelligence', 'getOutboundReadiness',
    'getAgingReport', 'getGrowthAnalytics',
    'getTableConfig', 'getLists', 'findPotentialDuplicates',
  ];

  for (const name of noArgGetters) {
    if (typeof leadDb[name] !== 'function') {
      skip++;
      console.log(`  SKIP ${name}: not exported`);
      continue;
    }
    await test(name, () => leadDb[name]());
  }

  // === Functions with arguments ===
  console.log('\n=== Functions with arguments ===');

  await test('searchLeads', () => leadDb.searchLeads({ limit: 10, offset: 0 }));
  await test('searchLeads(query)', () => leadDb.searchLeads({ query: 'Smith', limit: 5 }));
  await test('searchLeads(state)', () => leadDb.searchLeads({ state: 'TX', limit: 5 }));
  await test('exportLeads', () => leadDb.exportLeads({}));
  await test('getLeadById(1)', () => leadDb.getLeadById(1));
  await test('getLeadById(999)', () => leadDb.getLeadById(999));
  await test('getStateDetails(TX)', () => leadDb.getStateDetails('TX'));
  await test('getStateDetails(XX)', () => leadDb.getStateDetails('XX'));
  await test('findSimilarLeads(1)', () => leadDb.findSimilarLeads(1));
  await test('getLeadChangelog(1)', () => leadDb.getLeadChangelog(1));
  await test('getRecentChanges', () => leadDb.getRecentChanges());
  await test('getLeadsByStage(new)', () => leadDb.getLeadsByStage('new'));
  await test('getSearchSuggestions(sm)', () => leadDb.getSearchSuggestions('sm'));
  await test('classifyEmail(test@gmail.com)', () => leadDb.classifyEmail('test@gmail.com'));
  await test('classifyEmail(test@firm.com)', () => leadDb.classifyEmail('test@firm.com'));
  await test('computeConfidenceScore(1)', () => leadDb.computeConfidenceScore(1));
  await test('getLeadChangeHistory(1)', () => leadDb.getLeadChangeHistory(1));
  await test('compareLeads(1,2)', () => leadDb.compareLeads(1, 2));
  await test('validateEmailSyntax(good)', () => leadDb.validateEmailSyntax('test@example.com'));
  await test('validateEmailSyntax(bad)', () => leadDb.validateEmailSyntax('not-an-email'));
  await test('computeIcpScore(1)', () => leadDb.computeIcpScore(1));
  await test('getLeadNotes(1)', () => leadDb.getLeadNotes(1));
  await test('getLeadTimeline(1)', () => leadDb.getLeadTimeline(1));
  await test('getEngagementScore(1)', () => leadDb.getEngagementScore(1));
  await test('getFirmDetail(Smith)', () => leadDb.getFirmDetail('Smith & Associates'));
  await test('findLookalikes(1)', () => leadDb.findLookalikes(1));
  await test('checkDnc(test@test.com)', () => leadDb.checkDnc('test@test.com'));
  await test('getLeadForEnrichment(1)', () => leadDb.getLeadForEnrichment(1));
  await test('getScoreBreakdown(1)', () => leadDb.getScoreBreakdown(1));
  await test('getContactTimeline(1)', () => leadDb.getContactTimeline(1));
  await test('getContactStats(1)', () => leadDb.getContactStats(1));
  await test('computeWarmUpScore(1)', () => leadDb.computeWarmUpScore(1));
  await test('getKanbanData', () => leadDb.getKanbanData());
  await test('getCardViewData', () => leadDb.getCardViewData({ limit: 10 }));
  await test('searchTypeahead(sm)', () => leadDb.searchTypeahead('sm'));
  await test('getMarketPenetration(TX)', () => leadDb.getMarketPenetration('TX'));
  await test('getSourceComparison', () => leadDb.getSourceComparison('bar', 'martindale'));
  await test('checkEmailCompliance', () => leadDb.checkEmailCompliance('test@example.com'));
  await test('getLeadJourney(1)', () => leadDb.getLeadJourney(1));
  await test('computeActivityScore(1)', () => leadDb.computeActivityScore(1));
  await test('getLeadFreshness(1)', () => leadDb.getLeadFreshness(1));
  await test('getLeadLifecycle(1)', () => leadDb.getLeadLifecycle(1));
  await test('buildRelationshipGraph(1)', () => leadDb.buildRelationshipGraph(1));
  await test('getCustomFieldValues(1)', () => leadDb.getCustomFieldValues(1));
  await test('getDecayPreview2', () => leadDb.getDecayPreview2());
  await test('getDailyGrowth(7)', () => leadDb.getDailyGrowth(7));
  await test('getDailyGrowth(30)', () => leadDb.getDailyGrowth(30));
  await test('getMergePreview(1,2)', () => leadDb.getMergePreview(1, 2));
  await test('getMergePreview(999,998)', () => leadDb.getMergePreview(999, 998));
  await test('querySegment(empty)', () => leadDb.querySegment([]));
  await test('getAuditLog', () => leadDb.getAuditLog({ limit: 10 }));
  await test('exportAuditLog', () => leadDb.exportAuditLog());
  await test('getBulkEnrichmentDiff(1)', () => leadDb.getBulkEnrichmentDiff(1));
  await test('compareScoringModels', () => leadDb.compareScoringModels());
  await test('getLeadEngagementSparkline(1)', () => leadDb.getLeadEngagementSparkline(1));

  // === Write operations (should not throw) ===
  console.log('\n=== Write operations ===');

  await test('batchScoreLeads', () => leadDb.batchScoreLeads());
  await test('batchComputeConfidence', () => leadDb.batchComputeConfidence());
  await test('classifyAllEmails', () => leadDb.classifyAllEmails());
  await test('shareFirmData', () => leadDb.shareFirmData());
  await test('deduceWebsitesFromEmail', () => leadDb.deduceWebsitesFromEmail());
  await test('computeLeadScore(1)', () => leadDb.computeLeadScore(1));
  await test('addNote(1)', () => leadDb.addNote(1, 'Test note'));
  await test('logChange', () => leadDb.logChange(1, 'test', 'email', null, 'new@test.com'));
  await test('runQualityChecks', () => leadDb.runQualityChecks());
  await test('tagLeads([1],test)', () => leadDb.tagLeads([1], 'test-tag'));
  await test('addToDnc', () => leadDb.addToDnc('block@test.com', 'test'));
  await test('removeFromDnc(999)', () => leadDb.removeFromDnc(999));
  await test('logAuditEvent', () => leadDb.logAuditEvent('test', 'testing'));
  await test('recordScrapeRun', () => leadDb.recordScrapeRun('TX', 100, 50, 10));
  await test('saveTableConfig', () => leadDb.saveTableConfig({ columns: ['first_name','last_name'] }));
  await test('trackActivity(1)', () => leadDb.trackActivity(1, 'email_sent'));
  await test('logContact(1)', () => leadDb.logContact(1, 'email', 'follow-up'));
  await test('scanForDuplicates', () => leadDb.scanForDuplicates());
  await test('runAutoTagging', () => leadDb.runAutoTagging());

  // === Create/delete operations ===
  console.log('\n=== CRUD operations ===');

  await test('createList', () => {
    const l = leadDb.createList('Test List', 'Test description');
    if (!l || !l.id) throw new Error('createList returned no id');
    const fetched = leadDb.getList(l.id);
    if (!fetched) throw new Error('getList returned null');
    leadDb.addToList(l.id, [1]);
    leadDb.removeFromList(l.id, [1]);
    leadDb.deleteList(l.id);
  });

  await test('createSchedule', () => {
    const s = leadDb.createSchedule({ state: 'FL', frequency: 'weekly', dayOfWeek: 1, hour: 6 });
    if (s && s.id) {
      leadDb.updateSchedule(s.id, { state: 'GA' });
      leadDb.deleteSchedule(s.id);
    }
  });

  await test('createSegment', () => {
    const s = leadDb.createSegment('Test Seg', 'Test description', [{ field: 'state', op: '=', value: 'TX' }]);
    if (s && s.id) {
      leadDb.deleteSegment(s.id);
    }
  });

  await test('createCampaign', () => {
    const c = leadDb.createCampaign('Test Campaign', 'Test desc');
    if (c && c.id) {
      leadDb.addLeadsToCampaign(c.id, [1]);
      leadDb.getCampaignLeads(c.id);
      leadDb.updateCampaignStatus(c.id, 'active');
      leadDb.deleteCampaign(c.id);
    }
  });

  await test('createSmartList', () => {
    const s = leadDb.createSmartList('Test Smart', 'Test desc', { logic: 'AND', conditions: [{ field: 'state', operator: 'equals', value: 'TX' }] });
    if (s && s.lastInsertRowid) {
      leadDb.getSmartListLeads(s.lastInsertRowid);
      leadDb.deleteSmartList(s.lastInsertRowid);
    }
  });

  await test('createScoringModel', () => {
    const m = leadDb.createScoringModel('Test Model', [{ field: 'email', weight: 10 }]);
    if (m && m.id) {
      leadDb.deleteScoringModel(m.id);
    }
  });

  // Summary
  console.log('\n========================================');
  console.log(`TOTAL: ${pass + fail + skip}`);
  console.log(`PASS:  ${pass}`);
  console.log(`FAIL:  ${fail}`);
  console.log(`SKIP:  ${skip}`);
  console.log('========================================');

  if (errors.length > 0) {
    console.log('\n=== FAILURES ===');
    for (const e of errors) {
      console.log(`  ${e.name}: ${e.error}`);
    }
  }

  // Cleanup test DB
  try { require('fs').unlinkSync(process.env.LEAD_DB_PATH); } catch(e) {}

  process.exit(fail > 0 ? 1 : 0);
}

main().catch(err => { console.error(err); process.exit(1); });
