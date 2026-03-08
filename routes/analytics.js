/**
 * Analytics Routes — Dashboard analytics, funnel, velocity, clustering,
 * A/B tests, attribution, SLA, saturation, competitive intelligence,
 * propensity, cohorts, benchmarks, deals, risk scores, journey mapping,
 * scoring audit, geo-expansion, pipeline velocity, engagement prediction,
 * campaign performance, prioritization matrix, firm aggregation,
 * improvement recs, lifecycle funnel, cadence optimizer, scoring calibration,
 * practice market size, pipeline health, affinity scoring, scraper gaps,
 * firm growth, revenue attribution, saturation heatmap, smart list builder,
 * dedup intelligence, outbound readiness, aging report, growth analytics,
 * source ROI, geographic clustering, priority inbox, smart recommendations,
 * practice area analytics, predictive scores, team performance
 */

const router = require('express').Router();

// --- Conversion Funnel (Batch 27) ---
router.get('/funnel', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getConversionFunnel()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lead Velocity (Batch 27) ---
router.get('/velocity', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const days = parseInt(req.query.days) || 30;
    res.json(leadDb.getLeadVelocity(days));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lead Clustering (Batch 28) ---
router.get('/clusters', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLeadClusters()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- A/B Test Framework (Batch 28) ---
router.get('/ab-tests', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAbTests()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/ab-tests', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, variantA, variantB, metric } = req.body;
    res.json(leadDb.createAbTest(name, description, variantA, variantB, metric));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/ab-tests/:id/assign', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    res.json(leadDb.assignLeadsToAbTest(parseInt(req.params.id), leadIds || []));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/ab-tests/:id/outcome', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadId, responded } = req.body;
    res.json(leadDb.recordAbTestOutcome(parseInt(req.params.id), leadId, responded));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/ab-tests/:id', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    leadDb.deleteAbTest(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Re-engagement Scoring (Batch 28) ---
router.get('/reengagement', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getReengagementLeads(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Attribution Model (Batch 28) ---
router.get('/attribution', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAttributionModel()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Response Time SLA (Batch 29) ---
router.get('/sla', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getResponseTimeSLA()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Market Saturation (Batch 29) ---
router.get('/saturation', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getMarketSaturation()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Competitive Intelligence (Batch 29) ---
router.get('/competitive', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCompetitiveIntelligence()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Propensity Model (Batch 31) ---
router.get('/propensity', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getPropensityScores(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Cohort Analysis (Batch 31) ---
router.get('/cohorts', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCohortAnalysis()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Channel Preferences (Batch 31) ---
router.get('/channel-preferences', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getChannelPreferences(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Jurisdiction Benchmarks (Batch 31) ---
router.get('/benchmarks', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getJurisdictionBenchmarks()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Deal Estimation (Batch 32) ---
router.get('/deals', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 40;
    res.json(leadDb.getDealEstimates(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Outreach Calendar (Batch 32) ---
router.get('/outreach-calendar', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getOutreachCalendar()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Risk Scoring (Batch 32) ---
router.get('/risk-scores', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getRiskScores(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Network Mapping (Batch 32) ---
router.get('/network-map', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getNetworkMap(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Journey Mapping (Batch 33) ---
router.get('/journey-mapping', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getJourneyMapping(parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scoring Audit (Batch 33) ---
router.get('/scoring-audit', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScoringAudit(parseInt(req.query.limit) || 40)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Geographic Expansion (Batch 33) ---
router.get('/geo-expansion', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getGeoExpansion()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Outreach Analytics (Batch 34) ---
router.get('/outreach-analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getOutreachAnalytics()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- ICP Scoring (Batch 34) ---
router.get('/icp-scoring', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getIcpScoring(parseInt(req.query.limit) || 40)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Pipeline Velocity (Batch 34) ---
router.get('/pipeline-velocity', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPipelineVelocity()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Relationship Graph (Batch 35) ---
router.get('/relationship-graph', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRelationshipGraph(parseInt(req.query.limit) || 30)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Engagement Prediction (Batch 35) ---
router.get('/engagement-prediction', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getEngagementPrediction(parseInt(req.query.limit) || 40)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Campaign Performance (Batch 35) ---
router.get('/campaign-performance', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCampaignPerformance()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Prioritization Matrix (Batch 36) ---
router.get('/prioritization-matrix', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPrioritizationMatrix(parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Firm Aggregation (Batch 36) ---
router.get('/firm-aggregation', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getFirmAggregation(parseInt(req.query.limit) || 30)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Improvement Recommendations (Batch 36) ---
router.get('/improvement-recs', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getImprovementRecs()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lifecycle Funnel (Batch 36) ---
router.get('/lifecycle-funnel', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLifecycleFunnel()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Cadence Optimizer (Batch 37) ---
router.get('/cadence-optimizer', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCadenceOptimizer()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scoring Calibration (Batch 37) ---
router.get('/scoring-calibration', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScoringCalibration()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Practice Market Size (Batch 37) ---
router.get('/practice-market-size', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPracticeMarketSize()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Pipeline Health (Batch 37) ---
router.get('/pipeline-health', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPipelineHealth()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Affinity Scoring (Batch 38) ---
router.get('/affinity-scoring', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAffinityScoring(parseInt(req.query.limit) || 40)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scraper Gaps (Batch 38) ---
router.get('/scraper-gaps', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScraperGaps()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Firm Growth (Batch 38) ---
router.get('/firm-growth', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getFirmGrowth()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Revenue Attribution (Batch 39) ---
router.get('/revenue-attribution', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRevenueAttribution()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Saturation Heatmap (Batch 39) ---
router.get('/saturation-heatmap', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSaturationHeatmap()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Smart List Builder (Batch 39) ---
router.get('/smart-list-builder', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSmartListBuilder()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Dedup Intelligence (Batch 40) ---
router.get('/dedup-intelligence', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDedupIntelligence(parseInt(req.query.limit) || 25)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Outbound Readiness (Batch 40) ---
router.get('/outbound-readiness', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getOutboundReadiness()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Aging Report (Batch 40) ---
router.get('/aging-report', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAgingReport()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Growth Analytics (Batch 40) ---
router.get('/growth-analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getGrowthAnalytics()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Source ROI (Batch 24) ---
router.get('/source-roi', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSourceROI()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/source-roi/compare', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { sourceA, sourceB } = req.query;
    if (!sourceA || !sourceB) return res.status(400).json({ error: 'sourceA and sourceB required' });
    res.json(leadDb.getSourceComparison(sourceA, sourceB));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Geographic Clustering (Batch 23) ---
router.get('/geographic/clusters', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    res.json(leadDb.getGeographicClusters({ minClusterSize: parseInt(req.query.minSize) || 5, limit: parseInt(req.query.limit) || 50 }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/geographic/penetration/:state', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getMarketPenetration(req.params.state)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Priority Inbox (Batch 24) ---
router.get('/priority-inbox', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPriorityInbox(parseInt(req.query.limit) || 25)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/smart-recommendations', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSmartRecommendations()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Practice Area Analytics (Batch 24) ---
router.get('/practice-areas/analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPracticeAreaAnalytics(parseInt(req.query.limit) || 30)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Predictive Scoring (Batch 25) ---
router.get('/predictive-scores', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPredictiveScores(parseInt(req.query.limit) || 100)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Team Performance (Batch 25) ---
router.get('/team/performance', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getTeamPerformance()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
