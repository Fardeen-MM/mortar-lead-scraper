/**
 * Infrastructure Routes — Table config, scraper health, activity feed,
 * debug endpoints, backfill enrichment dates
 */

const router = require('express').Router();

module.exports = function ({ jobs, metrics, readLogTail }) {

  // --- Table Configuration ---
  router.get('/table-config', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getTableConfig() || { columns: null }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/table-config', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.saveTableConfig(req.body)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Scraper Health ---
  router.get('/scrapers/health', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getScraperHealth()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Activity Feed ---
  router.get('/activity', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getActivityFeed(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Backfill Enrichment Dates ---
  router.post('/leads/backfill-enrichment-dates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const db = leadDb.getDb();
      const result = db.prepare(`
        UPDATE leads SET last_enriched_at = updated_at, enrichment_steps = 'profile'
        WHERE last_enriched_at IS NULL
          AND profile_url IS NOT NULL AND profile_url != ''
          AND phone IS NOT NULL AND phone != ''
          AND updated_at > datetime('now', '-7 days')
      `).run();
      res.json({ updated: result.changes, message: `Backfilled ${result.changes} leads with enrichment dates` });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Debug Endpoints (dev only) ---
  if (process.env.NODE_ENV !== 'production') {
    router.get('/debug/logs', (req, res) => {
      const lines = parseInt(req.query.lines) || 100;
      res.json(readLogTail(lines));
    });

    router.get('/debug/metrics', (req, res) => {
      res.json(metrics.getSummary());
    });

    router.get('/debug/jobs', (req, res) => {
      const activeJobs = [];
      for (const [id, job] of jobs.entries()) {
        if (id.startsWith('upload-')) continue;
        activeJobs.push({
          id,
          state: job.state,
          practice: job.practice,
          status: job.status,
          leadCount: job.leads.length,
        });
      }
      res.json({ active: activeJobs, recent: metrics.getRecentJobs() });
    });
  }

  return router;
};
