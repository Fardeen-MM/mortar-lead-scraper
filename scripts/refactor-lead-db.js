#!/usr/bin/env node
/**
 * Refactor lead-db.js into modular files under lib/lead-db/
 *
 * Reads the monolithic file, splits it by section boundaries,
 * groups sections into domain modules, and writes them out.
 */

const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, '..', 'lib', 'lead-db.js');
const DEST_DIR = path.join(__dirname, '..', 'lib', 'lead-db');

// Read the entire source file
const lines = fs.readFileSync(SRC, 'utf8').split('\n');
console.log(`Read ${lines.length} lines from lead-db.js`);

// Find all section headers (// === or // ---) with their line numbers
const sections = [];
for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  if (line.match(/^\/\/ =====/) || line.match(/^\/\/ ---.*---\s*$/)) {
    sections.push({ line: i, text: line.trim() });
  }
}

// Find module.exports start
let exportsStart = -1;
for (let i = 0; i < lines.length; i++) {
  if (lines[i].startsWith('module.exports')) {
    exportsStart = i;
    break;
  }
}
console.log(`Found ${sections.length} section headers, exports at line ${exportsStart + 1}`);

// Define module boundaries by function names
// Each module maps to an array of function names it should contain
const MODULE_DEFS = {
  'core': {
    description: 'DB setup, schema, CRUD fundamentals',
    // Lines 1-860 approximately (before first dedup/merge section)
    endBefore: 862,
    // Also include some specific functions from later in the file
    extraFunctions: ['getLeadsNeedingEmail', 'updateEmailVerification', 'exportLeads',
      'getStateCoverage', 'getScrapeHistory', 'getRecommendations', 'shareFirmData',
      'deduceWebsitesFromEmail', 'getRecentActivity', 'getDistinctPracticeAreas',
      'getDistinctTags', 'tagLeads', 'getLeadById', 'getDailyGrowth',
      'getFieldCompleteness', 'updateLead', 'bulkUpdateLeads', 'getScraperHealth',
      'getEnrichmentStats', 'getActivityFeed', 'getDistinctSources', 'getStateDetails',
      'PIPELINE_STAGES', 'getRecentAdmissions', 'getAdmissionSignals',
      'getTableConfig', 'saveTableConfig', 'getKanbanData', 'getCardViewData',
      'computeWarmUpScore', 'batchComputeWarmUp']
  },
  'search-query': {
    description: 'Search, filtering, segments, smart lists, saved searches',
    functions: ['searchLeads', 'lookupByNameCity', 'batchLookupByNameCity',
      'getSearchSuggestions', 'getSegments', 'createSegment', 'updateSegment',
      'deleteSegment', 'querySegment', 'querySegmentLeads',
      'getSmartLists', 'createSmartList', 'deleteSmartList', 'getSmartListLeads',
      'previewSmartListCount', 'updateSmartList', 'getSmartListStats',
      'searchTypeahead', 'getFilterFacets',
      'getSavedSearches', 'createSavedSearch', 'deleteSavedSearch', 'checkSavedSearchAlerts']
  },
  'scoring': {
    description: 'Lead scoring, ICP, confidence, activity scoring, decay, custom models',
    functions: ['computeLeadScore', 'batchScoreLeads', 'getScoreDistribution',
      'getScoringRules', 'updateScoringRule', 'addScoringRule', 'deleteScoringRule',
      'getScoreBreakdown', 'computeConfidenceScore', 'batchComputeConfidence',
      'getConfidenceDistribution', 'getIcpCriteria', 'addIcpCriterion',
      'deleteIcpCriterion', 'updateIcpCriterion', 'computeIcpScore',
      'batchComputeIcpScores', 'getIcpDistribution', 'applyScoreDecay',
      'getDecayPreview', 'getScoringModels', 'createScoringModel',
      'activateScoringModel', 'deleteScoringModel', 'applyCustomScoring',
      'computeActivityScore', 'batchActivityScores', 'getActivityScoreConfig',
      'updateActivityScoreConfig', 'getDecayConfig', 'updateDecayConfig',
      'runScoreDecay', 'getDecayPreview2']
  },
  'enrichment': {
    description: 'Enrichment queue, firm enrichment, bulk enrichment, waterfall tracking',
    functions: ['enrichFirmData', 'getFirmDirectory', 'getLeadForEnrichment',
      'getEnrichmentQueueStatus', 'addToEnrichmentQueue', 'processEnrichmentQueue',
      'clearEnrichmentQueue', 'getFirmIntelligence', 'getFirmDetail',
      'createBulkEnrichmentRun', 'getBulkEnrichmentRuns',
      'processBulkEnrichmentBatch', 'getBulkEnrichmentDiff',
      'recordWaterfallRun', 'getWaterfallRuns', 'getWaterfallSummary',
      'getEnrichmentFailures', 'getEnrichmentFailureStats', 'clearEnrichmentErrors']
  },
  'dedup-merge': {
    description: 'Deduplication, merge, comparison, DNC',
    functions: ['mergeDuplicates', 'findPotentialDuplicates', 'mergeLeadPair',
      'getMergePreview', 'autoMergeDuplicates', 'compareLeads',
      'mergeLeadsWithChoices', 'findSmartDuplicates', 'addToDnc', 'removeFromDnc',
      'getDncList', 'checkDnc', 'batchCheckDnc', 'getCrossSourceDuplicates',
      'scanForDuplicates', 'getDedupQueue', 'resolveDedupItem', 'getDedupStats',
      'getLeadComparisonData', 'mergeLeadsWithPicks', 'getMergeCandidates', 'executeMerge']
  },
  'outreach': {
    description: 'Lists, pipeline, sequences, campaigns, territories, routing, lifecycle',
    functions: ['createList', 'getLists', 'getList', 'updateList', 'deleteList',
      'addToList', 'removeFromList', 'getLeadLists', 'getPipelineStats',
      'getLeadsByStage', 'moveLeadToStage', 'bulkMoveToStage',
      'getSequences', 'createSequence', 'addSequenceStep', 'deleteSequence',
      'enrollInSequence', 'getSequenceEnrollments', 'renderSequenceStep',
      'getCampaigns', 'createCampaign', 'deleteCampaign', 'addLeadsToCampaign',
      'getCampaignLeads', 'updateCampaignStatus',
      'getTerritories', 'createTerritory', 'deleteTerritory',
      'assignLeadsToTerritory', 'getTerritoryLeads',
      'getRoutingRules', 'createRoutingRule', 'deleteRoutingRule', 'runRoutingRules',
      'recordStageTransition', 'getLifecycleAnalytics', 'getLeadLifecycle',
      'recordSequenceEvent', 'getSequenceAnalytics', 'getAllSequencePerformance',
      'getContactTimeline', 'logContact', 'getContactStats', 'getRecentContacts',
      'getSchedules', 'createSchedule', 'updateSchedule', 'deleteSchedule',
      'markScheduleRun', 'getDueSchedules',
      'getSequenceTemplates', 'createSequenceTemplate', 'updateSequenceTemplate',
      'deleteSequenceTemplate', 'renderSequenceTemplate',
      'getNurtureCadence', 'getCadenceAnalytics',
      'getExportSchedules', 'createExportSchedule', 'deleteExportSchedule', 'runExportSchedule']
  },
  'email-quality': {
    description: 'Email classification, validation, compliance, Instantly export',
    functions: ['classifyEmail', 'classifyAllEmails', 'getEmailClassification',
      'validateEmailSyntax', 'validateEmailMX', 'batchValidateEmails',
      'exportForInstantly', 'getVerificationStats', 'bulkImportVerification',
      'recordConsent', 'addOptOut', 'removeOptOut', 'getComplianceDashboard',
      'checkEmailCompliance', 'getEmailDeliverability']
  },
  'tags-automation': {
    description: 'Tags, automation rules, custom fields, webhooks, notes, audit',
    functions: ['getWebhooks', 'createWebhook', 'updateWebhook', 'deleteWebhook',
      'getWebhooksByEvent', 'logWebhookDelivery', 'getWebhookDeliveries',
      'addNote', 'getLeadNotes', 'deleteNote', 'togglePinNote', 'getRecentNotes',
      'getLeadTimeline', 'getTagDefinitions', 'createTagDefinition',
      'updateTagDefinition', 'deleteTagDefinition', 'runAutoTagging',
      'bulkTagLeads', 'bulkRemoveTag', 'bulkAssignOwner',
      'bulkEnrollInCampaign', 'bulkEnrollInSequence', 'getOwners', 'getLeadsByOwner',
      'getAutomationRules', 'createAutomationRule', 'deleteAutomationRule',
      'toggleAutomationRule', 'runAutomationRules',
      'getTagRules', 'createTagRule', 'deleteTagRule', 'toggleTagRule', 'runTagRules',
      'getCustomFieldDefs', 'createCustomField', 'deleteCustomField',
      'setCustomFieldValue', 'getCustomFieldValues', 'getCustomFieldStats',
      'logAuditEvent', 'getAuditLog', 'getAuditStats', 'exportAuditLog',
      'getLeaderboard', 'getLeaderboardByState']
  },
  'data-quality': {
    description: 'Quality checks, freshness, change detection, import/export',
    functions: ['runQualityChecks', 'getQualityAlerts', 'resolveAlert', 'getAlertSummary',
      'getDatabaseHealth', 'getTopFirms', 'findSimilarLeads',
      'detectChanges', 'getRecentChanges2', 'getFirmChanges', 'getLeadChangeHistory',
      'getStalenessReport', 'previewImportMapping', 'importLeads', 'getImportFieldMapping',
      'getDataQualityReport', 'getDataQualitySummary',
      'recordFieldVerification', 'getFreshnessReport', 'getLeadFreshness',
      'logChange', 'getLeadChangelog', 'getRecentChanges',
      'recordExport', 'getExportHistory',
      'getExportProfiles', 'createExportProfile', 'deleteExportProfile', 'runExportProfile',
      'getExportTemplates', 'createExportTemplate', 'deleteExportTemplate',
      'getQualityRules', 'createQualityRule', 'deleteQualityRule', 'runQualityRules']
  },
  'analytics': {
    description: 'KPI, dashboards, all read-only analytics (Batch 27-42)',
    functions: ['getKpiMetrics', 'getSourceAttribution', 'getIntentSignals',
      'getPracticeAreaTrends', 'getCompletenessHeatmap', 'getEnrichmentRecommendations',
      'getSequenceVariantStats', 'assignVariant', 'trackActivity',
      'getLeadActivities', 'getEngagementScore', 'getMostEngagedLeads',
      'getPipelineFunnel', 'getSourceEffectiveness',
      'getEngagementHeatmap', 'getLeadEngagementSparkline', 'getEngagementTimeline',
      'buildRelationshipGraph', 'addRelationship', 'getFirmNetwork',
      'getGeographicClusters', 'getMarketPenetration',
      'getPriorityInbox', 'getSmartRecommendations',
      'getPracticeAreaAnalytics', 'getSourceROI', 'getSourceComparison',
      'compareScoringModels', 'getScoringModelRankings',
      'getLeadJourney', 'getPredictiveScores', 'getTeamPerformance',
      'findLookalikes', 'findBatchLookalikes',
      'getConversionFunnel', 'getLeadVelocity', 'getCompletenessMatrix',
      'getLeadClusters', 'getAbTests', 'createAbTest', 'assignLeadsToAbTest',
      'recordAbTestOutcome', 'deleteAbTest', 'getReengagementLeads',
      'getAttributionModel', 'getResponseTimeSLA', 'getMarketSaturation',
      'getEnrichmentWaterfall', 'getCompetitiveIntelligence',
      'getPropensityScores', 'getCohortAnalysis', 'getChannelPreferences',
      'getJurisdictionBenchmarks', 'getDealEstimates', 'getOutreachCalendar',
      'getRiskScores', 'getNetworkMap', 'getJourneyMapping', 'getScoringAudit',
      'getGeoExpansion', 'getFreshnessAlerts', 'getMergeCandidates',
      'getOutreachAnalytics', 'getIcpScoring', 'getPipelineVelocity',
      'getRelationshipGraph', 'getEnrichmentROI', 'getEngagementPrediction',
      'getCampaignPerformance', 'getPrioritizationMatrix', 'getFirmAggregation',
      'getImprovementRecs', 'getLifecycleFunnel', 'getCadenceOptimizer',
      'getScoringCalibration', 'getPracticeMarketSize', 'getPipelineHealth',
      'getAffinityScoring', 'getScraperGaps', 'getFreshnessIndex', 'getFirmGrowth',
      'getRevenueAttribution', 'getSaturationHeatmap', 'getSmartListBuilder',
      'getQualityScorecard', 'getDedupIntelligence', 'getOutboundReadiness',
      'getAgingReport', 'getGrowthAnalytics', 'getTodayDigest']
  }
};

// Find all top-level function definitions and their line ranges
// Pattern: function name(...) or const name = function or const name = (...) =>
const funcDefs = [];
const funcRegex = /^(?:function\s+(\w+)\s*\(|(?:const|let|var)\s+(\w+)\s*=\s*(?:function|\(|async\s+function|async\s*\())/;

for (let i = 0; i < exportsStart; i++) {
  const match = lines[i].match(funcRegex);
  if (match) {
    const name = match[1] || match[2];
    funcDefs.push({ name, startLine: i });
  }
}

// Also find constants like PIPELINE_STAGES
for (let i = 0; i < exportsStart; i++) {
  if (lines[i].match(/^const\s+PIPELINE_STAGES\s*=/)) {
    funcDefs.push({ name: 'PIPELINE_STAGES', startLine: i });
  }
}

// Sort by startLine
funcDefs.sort((a, b) => a.startLine - b.startLine);

// Set endLine for each function (start of next function - 1, but include blank lines/comments before next)
for (let i = 0; i < funcDefs.length; i++) {
  if (i + 1 < funcDefs.length) {
    // Walk backwards from next function to find where this one's content ends
    let endLine = funcDefs[i + 1].startLine - 1;
    // Include section headers that precede the next function as part of the next function
    while (endLine > funcDefs[i].startLine && (lines[endLine].trim() === '' || lines[endLine].startsWith('//'))) {
      endLine--;
    }
    funcDefs[i].endLine = endLine;
  } else {
    funcDefs[i].endLine = exportsStart - 1;
  }
}

console.log(`Found ${funcDefs.length} top-level function/constant definitions`);

// Build a map of function name -> source lines (with preceding comments)
const funcMap = {};
for (const fd of funcDefs) {
  // Include preceding comments and section headers
  let start = fd.startLine;
  while (start > 0 && (lines[start - 1].trim().startsWith('//') || lines[start - 1].trim().startsWith('/*') || lines[start - 1].trim().startsWith('*') || lines[start - 1].trim() === '')) {
    // Don't go past a previous function's end
    const prevFunc = funcDefs.find(f => f.endLine === start - 2 || f.endLine === start - 1);
    if (prevFunc) break;
    start--;
  }
  funcMap[fd.name] = {
    startLine: start,
    endLine: fd.endLine,
    lines: lines.slice(start, fd.endLine + 1)
  };
}

// Create destination directory
if (!fs.existsSync(DEST_DIR)) {
  fs.mkdirSync(DEST_DIR, { recursive: true });
}

// Get the preamble (requires, DB_PATH, getDb, schema, etc.) — lines 0 to first function that's NOT in core
const preambleEnd = funcDefs.find(f => f.startLine > 0)?.startLine || 0;

// Write core.js — includes the preamble + all core functions
// The core module gets everything up to line ~860 plus specific extra functions
const coreLines = [];
// Add everything from line 0 to the endBefore line
coreLines.push(...lines.slice(0, MODULE_DEFS.core.endBefore));

// Now for each remaining module, collect functions
const usedFunctions = new Set();

// Track which functions go to core from the extraFunctions list
for (const fn of (MODULE_DEFS.core.extraFunctions || [])) {
  usedFunctions.add(fn);
}

// Track functions for each non-core module
for (const [modName, modDef] of Object.entries(MODULE_DEFS)) {
  if (modName === 'core') continue;
  for (const fn of (modDef.functions || [])) {
    usedFunctions.add(fn);
  }
}

// Find functions in core range that aren't claimed by other modules
const coreFunctions = new Set(MODULE_DEFS.core.extraFunctions || []);
for (const fd of funcDefs) {
  if (fd.startLine < MODULE_DEFS.core.endBefore) {
    coreFunctions.add(fd.name);
  }
}

// Write each module
for (const [modName, modDef] of Object.entries(MODULE_DEFS)) {
  const modPath = path.join(DEST_DIR, `${modName}.js`);
  const modLines = [];

  if (modName === 'core') {
    // Core gets the entire preamble (requires, schema, getDb, etc.)
    modLines.push(`/**`);
    modLines.push(` * Lead Database — Core (schema, CRUD, fundamentals)`);
    modLines.push(` * Split from monolithic lead-db.js`);
    modLines.push(` */`);
    modLines.push('');
    // Copy everything up to endBefore
    modLines.push(...lines.slice(0, MODULE_DEFS.core.endBefore));

    // Add extra functions that are defined after endBefore but belong to core
    for (const fn of (MODULE_DEFS.core.extraFunctions || [])) {
      if (funcMap[fn] && funcMap[fn].startLine >= MODULE_DEFS.core.endBefore) {
        modLines.push('');
        modLines.push(...funcMap[fn].lines);
      }
    }

    // Export getDb and all core functions
    modLines.push('');
    modLines.push('module.exports = {');
    modLines.push('  getDb,');
    modLines.push('  deriveCountry,');
    modLines.push('  _normalizePhone,');
    // Add all functions defined in the core range
    for (const fd of funcDefs) {
      if (fd.startLine < MODULE_DEFS.core.endBefore && fd.name !== 'getDb' && fd.name !== 'deriveCountry' && fd.name !== '_normalizePhone') {
        modLines.push(`  ${fd.name},`);
      }
    }
    // Add extra functions
    for (const fn of (MODULE_DEFS.core.extraFunctions || [])) {
      if (!funcDefs.find(f => f.name === fn && f.startLine < MODULE_DEFS.core.endBefore)) {
        modLines.push(`  ${fn},`);
      }
    }
    modLines.push('};');
  } else {
    // Non-core modules
    modLines.push(`/**`);
    modLines.push(` * Lead Database — ${modDef.description}`);
    modLines.push(` * Split from monolithic lead-db.js`);
    modLines.push(` */`);
    modLines.push('');
    modLines.push(`const { getDb } = require('./core');`);
    modLines.push('');

    // Collect all functions for this module
    const moduleFuncs = modDef.functions || [];
    const addedFuncs = [];

    for (const fn of moduleFuncs) {
      if (funcMap[fn]) {
        modLines.push(...funcMap[fn].lines);
        modLines.push('');
        addedFuncs.push(fn);
      }
    }

    // Export
    modLines.push('module.exports = {');
    for (const fn of addedFuncs) {
      modLines.push(`  ${fn},`);
    }
    modLines.push('};');
  }

  fs.writeFileSync(modPath, modLines.join('\n'));
  console.log(`  Written ${modName}.js (${modLines.length} lines)`);
}

// Write index.js that re-exports everything
const indexLines = [];
indexLines.push(`/**`);
indexLines.push(` * Lead Database — Module Index`);
indexLines.push(` * Re-exports all sub-modules for backwards compatibility.`);
indexLines.push(` * Existing require('./lib/lead-db') calls continue to work unchanged.`);
indexLines.push(` */`);
indexLines.push('');
indexLines.push(`module.exports = {`);
for (const modName of Object.keys(MODULE_DEFS)) {
  indexLines.push(`  ...require('./${modName}'),`);
}
indexLines.push(`};`);

fs.writeFileSync(path.join(DEST_DIR, 'index.js'), indexLines.join('\n'));
console.log(`  Written index.js`);

// Verify: check that all exported functions from original are covered
const originalExports = [];
for (let i = exportsStart; i < lines.length; i++) {
  const match = lines[i].match(/^\s+(\w+),?\s*$/);
  if (match && match[1] !== 'module') {
    originalExports.push(match[1]);
  }
  // Also match inline exports like: getTagRules, createTagRule, ...
  const inlineMatches = lines[i].match(/(\w+),/g);
  if (inlineMatches && !lines[i].startsWith('module') && !lines[i].startsWith('//')) {
    for (const m of inlineMatches) {
      const name = m.replace(',', '').trim();
      if (name && name !== 'module' && name !== 'exports' && !name.startsWith('//')) {
        originalExports.push(name);
      }
    }
  }
}

// Deduplicate
const uniqueExports = [...new Set(originalExports)];
console.log(`\nOriginal exports: ${uniqueExports.length} unique functions`);

// Check which are missing from our modules
const allModuleFuncs = new Set();
for (const [modName, modDef] of Object.entries(MODULE_DEFS)) {
  if (modName === 'core') {
    for (const fd of funcDefs) {
      if (fd.startLine < MODULE_DEFS.core.endBefore) {
        allModuleFuncs.add(fd.name);
      }
    }
    for (const fn of (modDef.extraFunctions || [])) {
      allModuleFuncs.add(fn);
    }
  } else {
    for (const fn of (modDef.functions || [])) {
      allModuleFuncs.add(fn);
    }
  }
}

const missing = uniqueExports.filter(fn => !allModuleFuncs.has(fn));
if (missing.length > 0) {
  console.log(`\nWARNING: ${missing.length} functions not assigned to any module:`);
  for (const fn of missing) {
    console.log(`  - ${fn}`);
  }
} else {
  console.log(`\nAll original exports are covered by modules.`);
}

console.log('\nDone! Files written to lib/lead-db/');
console.log('Original file preserved at lib/lead-db.js');
console.log('To test: node -e "const db = require(\'./lib/lead-db\'); console.log(Object.keys(db).length + \' functions exported\')"');
