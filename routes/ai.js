/**
 * AI Intelligence Routes — AI status, ask brain, lead insights, email writer,
 * practice area classifier, data quality audit, dashboard brief, summarize,
 * score explanation, talking points, reply analysis, call scripts,
 * win probability, sequence variants, outreach plan, practice market,
 * data fixes, ICP builder, source ROI analysis
 */

const router = require('express').Router();

function aiError(res, err) {
  const msg = err.message || '';
  if (msg.includes('credit') || msg.includes('depleted') || msg.includes('API key')) {
    return res.status(503).json({ error: msg });
  }
  return res.status(500).json({ error: msg });
}

// AI status check (lightweight — no API call, just checks key exists)
router.get('/ai/status', (req, res) => {
  const ai = require('../lib/ai');
  res.json({ available: ai.isAvailable(), models: { fast: ai.HAIKU, deep: ai.SONNET } });
});

// AI Ask Brain — natural language queries
router.post('/ai/ask', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { question } = req.body;
    if (!question) return res.status(400).json({ error: 'question required' });
    const result = await ai.askBrain(question, leadDb.getDb());
    res.json(result);
  } catch (err) { aiError(res, err); }
});

// AI Lead Insights — per-lead analysis
router.post('/ai/lead-insights', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId } = req.body;
    if (!leadId) return res.status(400).json({ error: 'leadId required' });
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const insights = await ai.analyzeLeadInsights(lead);
    res.json({ lead: { id: lead.id, name: `${lead.first_name} ${lead.last_name}` }, insights });
  } catch (err) { aiError(res, err); }
});

// AI Email Writer — generate personalized cold email
router.post('/ai/write-email', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId, tone, goal, template } = req.body;
    if (!leadId) return res.status(400).json({ error: 'leadId required' });
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const email = await ai.writeEmail(lead, { tone, goal, template });
    res.json({ lead: { id: lead.id, name: `${lead.first_name} ${lead.last_name}` }, email });
  } catch (err) { aiError(res, err); }
});

// AI Practice Area Classifier — batch auto-tag
let _classifyRunning = false;
let _classifyProgress = null;

router.post('/ai/classify-practice-areas', async (req, res) => {
  if (_classifyRunning) return res.json({ running: true, progress: _classifyProgress });
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    _classifyRunning = true;
    _classifyProgress = { processed: 0, total: 0, updated: 0 };
    res.json({ started: true });

    // Run in background
    const db = leadDb.getDb();
    const leads = db.prepare("SELECT id, first_name, last_name, firm_name, title, bio, city, state, practice_area FROM leads WHERE (practice_area IS NULL OR practice_area = '') LIMIT 500").all();
    _classifyProgress.total = leads.length;

    const results = await ai.classifyPracticeAreas(leads);
    let updated = 0;
    for (const r of results) {
      if (r.id && r.practice_area) {
        try {
          leadDb.updateLead(r.id, { practice_area: r.practice_area });
          updated++;
        } catch (err) { console.error(`[Classify] updateLead failed for lead ${r.id}:`, err.message); }
      }
      _classifyProgress.processed++;
      _classifyProgress.updated = updated;
    }
    _classifyRunning = false;
    _classifyProgress = { processed: leads.length, total: leads.length, updated, done: true };
  } catch (err) {
    _classifyRunning = false;
    _classifyProgress = { error: err.message };
    console.error('[AI] Practice area classification error:', err.message);
  }
});

router.get('/ai/classify-practice-areas/status', (req, res) => {
  res.json({ running: _classifyRunning, progress: _classifyProgress });
});

// AI Data Quality Audit
router.post('/ai/audit-quality', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const state = req.body.state || null;
    const limit = Math.min(parseInt(req.body.limit) || 50, 100);
    const db = leadDb.getDb();
    let sql = "SELECT id, first_name, last_name, firm_name, city, state, email, phone, lead_score, practice_area, website FROM leads";
    const params = [];
    if (state) { sql += " WHERE state = ?"; params.push(state); }
    sql += " ORDER BY lead_score ASC LIMIT ?";
    params.push(limit);
    const leads = db.prepare(sql).all(...params);
    const audit = await ai.auditDataQuality(leads);
    res.json(audit);
  } catch (err) { aiError(res, err); }
});

// AI Dashboard Intelligence
router.get('/ai/dashboard-brief', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const stats = leadDb.getStats();
    const brief = await ai.generateDashboardBrief(stats);
    res.json(brief);
  } catch (err) { aiError(res, err); }
});

// AI Summarize search results
router.post('/ai/summarize', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { query, filters } = req.body;
    const results = leadDb.searchLeads({ ...filters, limit: 100 });
    const summary = await ai.summarizeBatch(results.leads || results, query);
    res.json({ summary, count: (results.leads || results).length });
  } catch (err) { aiError(res, err); }
});

// AI Score Explanation — why a lead has its score
router.post('/ai/explain-score', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId } = req.body;
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const explanation = await ai.explainScore(lead);
    res.json(explanation);
  } catch (err) { aiError(res, err); }
});

// AI Talking Points — personalized outreach points
router.post('/ai/talking-points', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId } = req.body;
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const points = await ai.generateTalkingPoints(lead);
    res.json(points);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/analyze-reply', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { replyText, leadId } = req.body;
    if (!replyText) return res.status(400).json({ error: 'replyText required' });
    const lead = leadId ? leadDb.getLeadById(leadId) : {};
    const analysis = await ai.analyzeReply(replyText, lead || {});
    // Log to contact_log if lead provided
    if (leadId && analysis) {
      try {
        leadDb.logContact(leadId, 'email', 'inbound', analysis.category || 'reply', replyText.slice(0, 500), analysis.interest || 'unknown');
      } catch (e) { /* non-critical */ }
    }
    res.json(analysis);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/call-script', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId, context } = req.body;
    if (!leadId) return res.status(400).json({ error: 'leadId required' });
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const script = await ai.generateCallScript(lead, { context });
    res.json(script);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/win-probability', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadId } = req.body;
    if (!leadId) return res.status(400).json({ error: 'leadId required' });
    const lead = leadDb.getLeadById(leadId);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const engagement = leadDb.getContactStats(leadId);
    const result = await ai.scoreWinProbability(lead, engagement);
    res.json(result);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/sequence-variants', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const { stepContext, numVariants } = req.body;
    if (!stepContext) return res.status(400).json({ error: 'stepContext required' });
    const variants = await ai.generateSequenceVariants(stepContext, numVariants || 3);
    res.json(variants);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/outreach-plan', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadIds, goal } = req.body;
    let leads;
    if (leadIds && leadIds.length > 0) {
      leads = leadIds.map(id => leadDb.getLeadById(id)).filter(Boolean);
    } else {
      const result = leadDb.searchLeads('', { sort: 'lead_score', order: 'desc', limit: 15, minScore: 30 });
      leads = result.leads || [];
    }
    if (leads.length === 0) return res.status(400).json({ error: 'No leads found' });
    const plan = await ai.planOutreach(leads, { goal });
    res.json(plan);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/practice-market', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const practiceStats = leadDb.getPracticeAreaStats ? leadDb.getPracticeAreaStats() : {};
    const geoStats = leadDb.getGeographicClusters ? leadDb.getGeographicClusters() : {};
    const tam = leadDb.getTamStats ? leadDb.getTamStats() : {};
    const stats = {
      total_leads: tam.total || 0,
      contactable: tam.contactable || 0,
      with_email: tam.withEmail || 0,
      practice_areas: practiceStats,
      geographic: geoStats,
    };
    const result = await ai.analyzePracticeAreaMarket(stats);
    res.json(result);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/data-fixes', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const { leadIds } = req.body;
    let leads;
    if (leadIds && leadIds.length > 0) {
      leads = leadIds.map(id => leadDb.getLeadById(id)).filter(Boolean);
    } else {
      const result = leadDb.searchLeads('', { limit: 15, sort: 'id', order: 'desc' });
      leads = result.leads || [];
    }
    if (leads.length === 0) return res.status(400).json({ error: 'No leads found' });
    const result = await ai.suggestDataFixes(leads);
    res.json(result);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/build-icp', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const topResult = leadDb.searchLeads('', { sort: 'lead_score', order: 'desc', limit: 20, minScore: 50 });
    const topLeads = topResult.leads || [];
    if (topLeads.length < 3) return res.status(400).json({ error: 'Need at least 3 high-scoring leads to build ICP' });
    const tam = leadDb.getTamStats ? leadDb.getTamStats() : {};
    const stats = { total: tam.total || 0, contactable: tam.contactable || 0, withEmail: tam.withEmail || 0, withPhone: tam.withPhone || 0 };
    const result = await ai.buildICP(topLeads, stats);
    res.json(result);
  } catch (err) { aiError(res, err); }
});

router.post('/ai/source-roi', async (req, res) => {
  try {
    const ai = require('../lib/ai');
    const leadDb = require('../lib/lead-db');
    const sources = leadDb.getDistinctSources ? leadDb.getDistinctSources() : [];
    const sourceData = sources.slice(0, 15).map(s => {
      try {
        const result = leadDb.searchLeads('', { source: s, limit: 1 });
        const total = result.total || 0;
        const withEmail = leadDb.searchLeads('', { source: s, hasEmail: true, limit: 1 }).total || 0;
        const withPhone = leadDb.searchLeads('', { source: s, hasPhone: true, limit: 1 }).total || 0;
        return { source: s, total, withEmail, withPhone, emailRate: total ? Math.round((withEmail / total) * 100) : 0, phoneRate: total ? Math.round((withPhone / total) * 100) : 0 };
      } catch { return { source: s, total: 0 }; }
    });
    const result = await ai.analyzeSourceROI(sourceData);
    res.json(result);
  } catch (err) { aiError(res, err); }
});

module.exports = router;
