/**
 * Data Quality Routes — Quality checks, freshness, changelog,
 * export history, data quality reports, completeness matrix, quality rules
 */

const router = require('express').Router();

// --- Quality ---
router.post('/quality/check', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runQualityChecks()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/quality/alerts', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { type, resolved, limit } = req.query;
    res.json(leadDb.getQualityAlerts({
      type,
      resolved: resolved === 'true',
      limit: parseInt(limit) || 100,
    }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/quality/summary', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAlertSummary()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/quality/resolve/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.resolveAlert(parseInt(req.params.id)); res.json({ resolved: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Quality Rules ---
router.get('/quality-rules', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getQualityRules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/quality-rules', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, field, checkType, checkValue, severity, flagTag } = req.body;
    res.json(leadDb.createQualityRule(name, field, checkType, checkValue, severity, flagTag));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/quality-rules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteQualityRule(parseInt(req.params.id)); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/quality-rules/run', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runQualityRules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Freshness ---
router.get('/leads/freshness', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScrapeHistory()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/freshness/report', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    res.json(leadDb.getFreshnessReport({ staleDays: parseInt(req.query.staleDays) || 90, limit: parseInt(req.query.limit) || 100 }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/freshness', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLeadFreshness(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/freshness/verify', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadId, fieldName, source } = req.body;
    if (!leadId || !fieldName) return res.status(400).json({ error: 'leadId and fieldName required' });
    res.json(leadDb.recordFieldVerification(parseInt(leadId), fieldName, source));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/freshness-alerts', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getFreshnessAlerts()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/freshness-index', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getFreshnessIndex(parseInt(req.query.limit) || 40)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Changelog ---
router.get('/leads/changelog', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRecentChanges(parseInt(req.query.limit) || 100)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/changelog/:leadId', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLeadChangelog(parseInt(req.params.leadId))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Export History ---
router.get('/exports/history', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getExportHistory()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Data Quality Report ---
router.get('/leads/data-quality', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDataQualityReport(parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/data-quality-summary', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDataQualitySummary()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Data Quality Analytics ---
router.get('/data-quality/report', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDataQualityReport(parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Completeness Matrix ---
router.get('/completeness-matrix', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCompletenessMatrix()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Quality Scorecard ---
router.get('/quality-scorecard', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getQualityScorecard()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
