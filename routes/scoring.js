/**
 * Scoring Routes — Scoring rules, ICP, activity scores, score decay, scoring models
 */

const router = require('express').Router();

// --- Scoring Rules ---
router.get('/scoring/rules', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScoringRules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/scoring/rules', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { field, points, condition } = req.body;
    if (!field || points === undefined) return res.status(400).json({ error: 'field and points required' });
    res.json(leadDb.addScoringRule(field, points, condition));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/scoring/rules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateScoringRule(parseInt(req.params.id), req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/scoring/rules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteScoringRule(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/score-breakdown', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const lead = leadDb.getLeadById(parseInt(req.params.id));
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    res.json(leadDb.getScoreBreakdown(lead));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- ICP ---
router.get('/icp/criteria', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getIcpCriteria()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/icp/criteria', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { field, operator, value, weight, label } = req.body;
    if (!field || !operator) return res.status(400).json({ error: 'field and operator required' });
    const result = leadDb.addIcpCriterion(field, operator, value || '', weight || 10, label || '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/icp/criteria/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateIcpCriterion(parseInt(req.params.id), req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/icp/criteria/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteIcpCriterion(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/icp/distribution', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getIcpDistribution()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/icp/score-all', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.batchComputeIcpScores()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Activity Scoring ---
router.get('/leads/:id/activity-score', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.computeActivityScore(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/activity-scores/batch', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.batchActivityScores(parseInt(req.query.limit) || 100)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/activity-scores/config', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getActivityScoreConfig()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/activity-scores/config', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateActivityScoreConfig(req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Score Decay ---
router.get('/score-decay/config', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDecayConfig()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/score-decay/config', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateDecayConfig(req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/score-decay/run', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runScoreDecay()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/score-decay/preview', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.getDecayPreview2(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scoring Models ---
router.get('/scoring-models', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScoringModels()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/scoring-models', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, weights } = req.body;
    if (!name || !weights) return res.status(400).json({ error: 'name and weights required' });
    const result = leadDb.createScoringModel(name, weights);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/scoring-models/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteScoringModel(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/scoring-models/:id/activate', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.activateScoringModel(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/scoring-models/apply', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.applyCustomScoring()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/scoring-models/compare', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { modelA, modelB } = req.query;
    if (!modelA || !modelB) return res.status(400).json({ error: 'modelA and modelB query params required' });
    res.json(leadDb.compareScoringModels(parseInt(modelA), parseInt(modelB)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/scoring-models/rankings', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScoringModelRankings()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Confidence ---
router.get('/leads/confidence', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getConfidenceDistribution()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/leads/compute-confidence', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.batchComputeConfidence()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
