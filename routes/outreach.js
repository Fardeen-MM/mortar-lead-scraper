/**
 * Outreach Routes — Pipeline, schedules, lists, sequences, campaigns,
 * contacts, territories, routing, lifecycle, nurture, segments
 */

const path = require('path');
const router = require('express').Router();

// --- Pipeline ---
router.get('/pipeline/stats', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getPipelineStats()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/pipeline/stage/:stage', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getLeadsByStage(req.params.stage, limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/pipeline/move', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadId, stage } = req.body;
    if (!leadId || !stage) return res.status(400).json({ error: 'leadId and stage required' });
    res.json(leadDb.moveLeadToStage(leadId, stage));
  } catch (err) {
    const status = err.message.includes('not found') ? 404 : err.message.includes('Invalid') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

router.post('/pipeline/bulk-move', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds, stage } = req.body;
    if (!Array.isArray(leadIds) || !leadIds.length || !stage) return res.status(400).json({ error: 'leadIds (array) and stage required' });
    res.json(leadDb.bulkMoveToStage(leadIds, stage));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Schedules ---
router.get('/schedules', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSchedules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/schedules', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { state, practiceArea, frequency, dayOfWeek, hour } = req.body;
    if (!state) return res.status(400).json({ error: 'state required' });
    res.json(leadDb.createSchedule({ state, practiceArea, frequency, dayOfWeek, hour }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/schedules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateSchedule(parseInt(req.params.id), req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/schedules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteSchedule(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Segments ---
router.get('/segments', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSegments()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/segments', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, filters, color } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    res.json(leadDb.createSegment(name, description || '', filters, color));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/segments/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateSegment(parseInt(req.params.id), req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/segments/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteSegment(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/segments/query', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { filters } = req.body;
    if (!filters || !Array.isArray(filters)) return res.status(400).json({ error: 'filters must be an array of {field, op, value}' });
    res.json(leadDb.querySegment(filters));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/segments/query/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { filters, limit = 100, offset = 0 } = req.body;
    if (!filters || !Array.isArray(filters)) return res.status(400).json({ error: 'filters must be an array of {field, op, value}' });
    res.json(leadDb.querySegmentLeads(filters, limit, offset));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Sequences ---
router.get('/sequences', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSequences()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/sequences', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createSequence(name, description || '');
    res.json({ id: result.lastInsertRowid, name });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/sequences/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteSequence(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/sequences/:id/steps', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { stepNumber, channel, subject, body, delayDays, variant } = req.body;
    const result = leadDb.addSequenceStep(parseInt(req.params.id), stepNumber || 1, channel || 'email', subject || '', body || '', delayDays || 0, variant || 'A');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/sequences/:id/enroll', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    res.json(leadDb.enrollInSequence(parseInt(req.params.id), leadIds));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequences/:id/enrollments', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSequenceEnrollments(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequences/render/:stepId/:leadId', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const result = leadDb.renderSequenceStep(parseInt(req.params.stepId), parseInt(req.params.leadId));
    if (!result) return res.status(404).json({ error: 'Step or lead not found' });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequences/:id/variants', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSequenceVariantStats(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/sequences/:id/event', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { stepNumber, leadId, eventType, variant, metadata } = req.body;
    if (!leadId || !eventType) return res.status(400).json({ error: 'leadId and eventType required' });
    res.json(leadDb.recordSequenceEvent(parseInt(req.params.id), stepNumber || 1, parseInt(leadId), eventType, variant, metadata));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequences/:id/analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSequenceAnalytics(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequences/performance', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAllSequencePerformance()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Campaigns ---
router.get('/campaigns', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCampaigns()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/campaigns', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createCampaign(name, description || '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/campaigns/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteCampaign(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/campaigns/:id/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
    res.json(leadDb.addLeadsToCampaign(parseInt(req.params.id), leadIds));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/campaigns/:id/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getCampaignLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/campaigns/:id/status', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { status } = req.body;
    if (!status) return res.status(400).json({ error: 'status required' });
    leadDb.updateCampaignStatus(parseInt(req.params.id), status);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Contacts ---
router.get('/leads/:id/contacts', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getContactTimeline(parseInt(req.params.id), parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/leads/:id/contacts', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { channel, direction, subject, notes, outcome } = req.body;
    if (!channel) return res.status(400).json({ error: 'channel required' });
    const result = leadDb.logContact(parseInt(req.params.id), channel, direction, subject, notes, outcome);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/contact-stats', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getContactStats(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/contacts/recent', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRecentContacts(parseInt(req.query.limit) || 30)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Territories ---
router.get('/territories', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getTerritories()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/territories', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, states, cities, owner } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createTerritory(name, description || '', states || '', cities || '', owner || '');
    res.json({ id: result.lastInsertRowid, name });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/territories/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteTerritory(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/territories/:id/assign', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.assignLeadsToTerritory(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/territories/:id/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getTerritoryLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Routing Rules ---
router.get('/routing-rules', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRoutingRules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/routing-rules', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, conditions, actionType, actionValue, priority } = req.body;
    if (!name || !conditions || !actionType || !actionValue) {
      return res.status(400).json({ error: 'name, conditions, actionType, actionValue required' });
    }
    const result = leadDb.createRoutingRule(name, conditions, actionType, actionValue, priority || 0);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/routing-rules/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteRoutingRule(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/routing-rules/run', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runRoutingRules()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lifecycle ---
router.post('/leads/:id/stage-transition', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { fromStage, toStage, triggeredBy } = req.body;
    if (!toStage) return res.status(400).json({ error: 'toStage required' });
    res.json(leadDb.recordStageTransition(parseInt(req.params.id), fromStage, toStage, triggeredBy));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/lifecycle/analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLifecycleAnalytics()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/lifecycle', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLeadLifecycle(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Nurture ---
router.get('/nurture/cadence', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getNurtureCadence(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/nurture/analytics', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCadenceAnalytics()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Activity tracking ---
router.post('/leads/:id/activity', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { action, details } = req.body;
    if (!action) return res.status(400).json({ error: 'action required' });
    leadDb.trackActivity(parseInt(req.params.id), action, details || '');
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/activities', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getLeadActivities(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/:id/engagement', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json({ score: leadDb.getEngagementScore(parseInt(req.params.id)) }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Warm-Up Scoring ---
router.get('/leads/:id/warm-up', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.computeWarmUpScore(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/leads/warm-up-batch', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.batchComputeWarmUp(parseInt(req.query.limit) || 50)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lists ---
router.get('/lists', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLists()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/lists', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, color } = req.body;
    if (!name || !name.trim()) return res.status(400).json({ error: 'Name required' });
    const list = leadDb.createList(name.trim(), description || '', color || '#6366f1');
    res.json(list);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/lists/:id', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const list = leadDb.getList(parseInt(req.params.id));
    if (!list) return res.status(404).json({ error: 'List not found' });
    res.json(list);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/lists/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.updateList(parseInt(req.params.id), req.body); res.json({ updated: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/lists/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteList(parseInt(req.params.id)); res.json({ deleted: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/lists/:id/add', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    const result = leadDb.addToList(parseInt(req.params.id), leadIds);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/lists/:id/remove', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    const result = leadDb.removeFromList(parseInt(req.params.id), leadIds);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/lists/:id/export', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { createObjectCsvWriter } = require('csv-writer');
    const fs = require('fs');
    const OUTPUT_DIR = path.join(__dirname, '..', 'data', 'output');
    const list = leadDb.getList(parseInt(req.params.id));
    if (!list || list.members.length === 0) return res.status(404).json({ error: 'List empty or not found' });
    const tmpPath = path.join(OUTPUT_DIR, `list-${list.name.replace(/\W+/g, '-')}-${Date.now()}.csv`);
    const headers = Object.keys(list.members[0]).filter(k => k !== 'id' && k !== 'lead_score')
      .map(k => ({ id: k, title: k }));
    const writer = createObjectCsvWriter({ path: tmpPath, header: headers });
    writer.writeRecords(list.members).then(() => {
      res.download(tmpPath, `${list.name.replace(/\W+/g, '-')}-leads.csv`, () => {
        try { fs.unlinkSync(tmpPath); } catch {}
      });
    }).catch(err => res.status(500).json({ error: err.message }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Smart Lists ---
router.get('/smart-lists', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSmartLists()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/smart-lists', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, filters } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    const result = leadDb.createSmartList(name, description || '', filters);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/smart-lists/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteSmartList(parseInt(req.params.id)); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/smart-lists/:id/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getSmartListLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/smart-lists/preview', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { filters } = req.body;
    if (!filters) return res.status(400).json({ error: 'filters required' });
    const count = leadDb.previewSmartListCount(filters);
    res.json({ count });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/smart-lists/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.updateSmartList(parseInt(req.params.id), req.body); res.json({ success: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/smart-lists/stats', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSmartListStats()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Saved Searches ---
router.get('/saved-searches', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSavedSearches()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/saved-searches', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, filters, alertEnabled } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    const result = leadDb.createSavedSearch(name, filters, alertEnabled);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/saved-searches/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteSavedSearch(parseInt(req.params.id))); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/saved-searches/alerts', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.checkSavedSearchAlerts()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Owners ---
router.get('/owners', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getOwners()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/owners/:name/leads', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getLeadsByOwner(req.params.name, limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Sequence Templates ---
router.get('/sequence-templates', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getSequenceTemplates()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/sequence-templates', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const { name, description, steps } = req.body;
    res.json(leadDb.createSequenceTemplate(name, description, steps));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/sequence-templates/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateSequenceTemplate(parseInt(req.params.id), req.body)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/sequence-templates/:id', (req, res) => {
  try { const leadDb = require('../lib/lead-db'); leadDb.deleteSequenceTemplate(parseInt(req.params.id)); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/sequence-templates/:id/render/:leadId', (req, res) => {
  try {
    const leadDb = require('../lib/lead-db');
    const result = leadDb.renderSequenceTemplate(parseInt(req.params.id), parseInt(req.params.leadId));
    res.json(result || { error: 'Template or lead not found' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
