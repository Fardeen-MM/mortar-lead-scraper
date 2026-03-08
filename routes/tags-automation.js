/**
 * Tags & Automation Routes — Webhooks, notes, tag definitions, tag rules,
 * custom fields, automation rules, audit log, export profiles/templates,
 * dedup queue, relationships
 */

const express = require('express');
const router = express.Router();

module.exports = function ({ fireWebhook, fireWebhookEvent }) {

  // --- Webhooks ---
  router.get('/webhooks', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getWebhooks()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/webhooks', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { url, events, secret } = req.body;
      if (!url || !events) return res.status(400).json({ error: 'url and events required' });
      try {
        const parsed = new URL(url);
        if (!['https:', 'http:'].includes(parsed.protocol)) {
          return res.status(400).json({ error: 'Webhook URL must use http or https' });
        }
        const host = parsed.hostname.toLowerCase();
        if (host === 'localhost' || host === '127.0.0.1' || host === '0.0.0.0' || host.startsWith('192.168.') || host.startsWith('10.') || host.startsWith('172.')) {
          return res.status(400).json({ error: 'Webhook URL must not point to private/local addresses' });
        }
      } catch { return res.status(400).json({ error: 'Invalid webhook URL format' }); }
      res.json(leadDb.createWebhook(url, events, secret));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.patch('/webhooks/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateWebhook(parseInt(req.params.id), req.body)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/webhooks/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteWebhook(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/webhooks/:id/deliveries', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getWebhookDeliveries(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/webhooks/test', (req, res) => {
    try {
      const { url } = req.body;
      if (!url) return res.status(400).json({ error: 'url required' });
      fireWebhook(url, 'test', { message: 'Webhook test from Mortar Lead Scraper', timestamp: new Date().toISOString() });
      res.json({ sent: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Notes ---
  router.get('/leads/:id/notes', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getLeadNotes(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/:id/notes', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { content, author } = req.body;
      if (!content) return res.status(400).json({ error: 'content required' });
      res.json(leadDb.addNote(parseInt(req.params.id), content, author));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/notes/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteNote(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/notes/:id/pin', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.togglePinNote(parseInt(req.params.id)); res.json({ success: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/notes/recent', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getRecentNotes(parseInt(req.query.limit) || 20)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/:id/timeline', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getLeadTimeline(parseInt(req.params.id), limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Tag Definitions ---
  router.get('/tag-definitions', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getTagDefinitions()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/tag-definitions', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, color, description, autoRule } = req.body;
      if (!name) return res.status(400).json({ error: 'name required' });
      const result = leadDb.createTagDefinition(name, color, description, autoRule ? JSON.stringify(autoRule) : '');
      res.json({ id: result.lastInsertRowid });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.patch('/tag-definitions/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.updateTagDefinition(parseInt(req.params.id), req.body)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/tag-definitions/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteTagDefinition(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/auto-tag', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runAutoTagging()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Tag Rules ---
  router.get('/tag-rules', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getTagRules()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/tag-rules', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, tag, conditions, logic } = req.body;
      res.json(leadDb.createTagRule(name, tag, conditions, logic));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/tag-rules/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.deleteTagRule(req.params.id); res.json({ ok: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/tag-rules/:id/toggle', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.toggleTagRule(req.params.id)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/tag-rules/run', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.runTagRules()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Custom Fields ---
  router.get('/custom-fields', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCustomFieldDefs()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/custom-fields', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { fieldName, fieldType, options, required, defaultValue } = req.body;
      res.json(leadDb.createCustomField(fieldName, fieldType, options, required, defaultValue));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/custom-fields/:name', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.deleteCustomField(req.params.name); res.json({ ok: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/custom-fields/stats', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCustomFieldStats()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/:id/custom-fields', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getCustomFieldValues(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.put('/leads/:id/custom-fields', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { fieldName, value } = req.body;
      res.json(leadDb.setCustomFieldValue(parseInt(req.params.id), fieldName, value));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Automation Rules ---
  router.get('/automation-rules', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAutomationRules()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/automation-rules', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, triggerEvent, conditions, actionType, actionValue } = req.body;
      if (!name || !triggerEvent || !actionType) return res.status(400).json({ error: 'name, triggerEvent, and actionType required' });
      const result = leadDb.createAutomationRule(name, triggerEvent, conditions || {}, actionType, actionValue || '');
      res.json({ id: result.lastInsertRowid });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/automation-rules/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.deleteAutomationRule(parseInt(req.params.id)); res.json({ success: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/automation-rules/:id/toggle', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.toggleAutomationRule(parseInt(req.params.id)); res.json({ success: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/automation-rules/run', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { event, leadIds } = req.body;
      const leads = leadIds ? leadIds.map(id => leadDb.getLeadById(id)).filter(Boolean) : [];
      res.json(leadDb.runAutomationRules(event || 'lead_added', leads));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Audit Log ---
  router.post('/audit/log', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { action, entityType, entityId, details, userName } = req.body;
      if (!action) return res.status(400).json({ error: 'action required' });
      res.json({ id: leadDb.logAuditEvent(action, entityType, entityId, details, userName) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/audit/log', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getAuditLog({
        action: req.query.action,
        entityType: req.query.entityType,
        userName: req.query.userName,
        limit: parseInt(req.query.limit) || 50,
        offset: parseInt(req.query.offset) || 0,
      }));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/audit/stats', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getAuditStats()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/audit/export', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.exportAuditLog({ startDate: req.query.startDate, endDate: req.query.endDate }));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Export Templates ---
  router.get('/export-templates', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getExportTemplates()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/export-templates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, columns, columnRenames, filters } = req.body;
      if (!name || !columns) return res.status(400).json({ error: 'name and columns required' });
      res.json(leadDb.createExportTemplate(name, columns, columnRenames, filters));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/export-templates/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.deleteExportTemplate(parseInt(req.params.id))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/export-templates/:id/export', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { writeCSV, generateOutputPath } = require('../lib/csv-handler');
      const path = require('path');
      const templates = leadDb.getExportTemplates();
      const template = templates.find(t => t.id === parseInt(req.params.id));
      if (!template) return res.status(404).json({ error: 'Template not found' });
      const leads = leadDb.exportLeads(template.filters || {});
      const mapped = leads.map(lead => {
        const row = {};
        for (const col of template.columns) {
          const renamed = template.columnRenames[col] || col;
          row[renamed] = lead[col] || '';
        }
        return row;
      });
      const outPath = generateOutputPath(`export-${template.name.toLowerCase().replace(/\s+/g, '-')}`);
      writeCSV(outPath, mapped);
      leadDb.recordExport(template.name, mapped.length, JSON.stringify(template.filters), path.basename(outPath));
      res.download(outPath);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Export Profiles ---
  router.get('/export-profiles', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getExportProfiles()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/export-profiles', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, description, filters, columns } = req.body;
      if (!name) return res.status(400).json({ error: 'name required' });
      const result = leadDb.createExportProfile(name, description, filters, columns);
      res.json({ id: result.lastInsertRowid });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/export-profiles/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.deleteExportProfile(parseInt(req.params.id)); res.json({ success: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/export-profiles/:id/run', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const leads = leadDb.runExportProfile(parseInt(req.params.id));
      res.json({ count: leads.length, leads: leads.slice(0, 20) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Dedup Queue ---
  router.post('/dedup/scan', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.scanForDuplicates(Math.min(Math.max(parseInt(req.body.limit) || 200, 1), 2000))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/dedup/queue', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDedupQueue(parseInt(req.query.limit) || 50)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/dedup/resolve/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { resolution, keepId } = req.body;
      if (!resolution) return res.status(400).json({ error: 'resolution required (merge|skip)' });
      res.json(leadDb.resolveDedupItem(parseInt(req.params.id), resolution, keepId ? parseInt(keepId) : null));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/dedup/stats', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDedupStats()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Relationships ---
  router.post('/relationships', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIdA, leadIdB, type, strength } = req.body;
      if (!leadIdA || !leadIdB || !type) return res.status(400).json({ error: 'leadIdA, leadIdB, type required' });
      res.json(leadDb.addRelationship(parseInt(leadIdA), parseInt(leadIdB), type, strength || 0.5));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/firm-network', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getFirmNetwork(parseInt(req.query.limit) || 30)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Export Schedules ---
  router.get('/export-schedules', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getExportSchedules()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/export-schedules', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { name, filters, columns, frequency } = req.body;
      res.json(leadDb.createExportSchedule(name, filters, columns, frequency));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/export-schedules/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.deleteExportSchedule(parseInt(req.params.id)); res.json({ ok: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/export-schedules/:id/run', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const result = leadDb.runExportSchedule(parseInt(req.params.id));
      res.json(result || { error: 'Schedule not found' });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Merge Candidates ---
  router.get('/merge-candidates', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getMergeCandidates(parseInt(req.query.limit) || 30)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/merge-preview/:id1/:id2', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getMergePreview(parseInt(req.params.id1), parseInt(req.params.id2))); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/merge-execute', express.json(), (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { keepId, mergeId } = req.body;
      res.json(leadDb.executeMerge(keepId, mergeId));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  return router;
};
