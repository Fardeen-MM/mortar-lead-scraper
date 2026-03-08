/**
 * Scraping Routes — Scrape job management, signals, health checks
 *
 * Routes: /config, /upload, /scrape/*, /health, /signals/*
 *
 * This module exports a factory function because it needs shared state
 * (jobs Map, broadcast fn, upload multer, runPipeline, broadcastAll, fireWebhookEvent).
 */

const path = require('path');
const fs = require('fs');
const router = require('express').Router();

module.exports = function ({ jobs, broadcast, broadcastAll, upload, runPipeline, fireWebhookEvent }) {

  // --- Get available scrapers and their practice areas ---
  router.get('/config', (req, res) => {
    try {
      const { getScraperMetadata } = require('../lib/registry');
      const metadata = getScraperMetadata();
      res.json({
        states: metadata,
        hasAnthropicKey: !!process.env.ANTHROPIC_API_KEY,
        enrichmentFeatures: [
          { id: 'deriveWebsite', label: 'Derive website from email', default: true, cost: 'free' },
          { id: 'scrapeWebsite', label: 'Scrape firm websites', default: true, cost: 'free, ~5s/lead' },
          { id: 'findLinkedIn', label: 'Find LinkedIn profiles', default: true, cost: 'free' },
          { id: 'extractWithAI', label: 'AI fallback for missing data', default: false, cost: '~$0.001/lead' },
        ],
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Upload existing leads CSV for dedup ---
  router.post('/upload', upload.single('file'), async (req, res) => {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    try {
      const { readCSV } = require('../lib/csv-handler');
      const leads = await readCSV(req.file.path);
      const uploadId = req.file.filename;
      jobs.set(`upload-${uploadId}`, { leads, originalName: req.file.originalname });
      setTimeout(() => jobs.delete(`upload-${uploadId}`), 30 * 60 * 1000);

      fs.unlink(req.file.path, (err) => {
        if (err) console.error('[upload] Failed to delete temp file:', err.message);
      });

      res.json({
        uploadId,
        count: leads.length,
        originalName: req.file.originalname,
      });
    } catch (err) {
      fs.unlink(req.file.path, () => {});
      console.error('[upload] CSV parse error:', err.message, err.stack);
      res.status(400).json({ error: `Failed to parse CSV: ${err.message}` });
    }
  });

  // --- Start a scrape job ---
  router.post('/scrape/start', (req, res) => {
    try {
      const { state, practice, city, test, uploadId, enrich, enrichOptions, waterfall, niche, personExtract, radius } = req.body;

      if (!state) {
        return res.status(400).json({ error: 'State is required' });
      }

      const { getRegistry } = require('../lib/registry');
      const registry = getRegistry();
      if (!registry[state]) {
        return res.status(400).json({ error: `Unknown scraper: ${state}` });
      }

      const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;

      let existingLeads = [];
      if (uploadId) {
        const uploadData = jobs.get(`upload-${uploadId}`);
        if (uploadData) {
          existingLeads = uploadData.leads;
        }
      }

      const emitter = runPipeline({
        state,
        practice: practice || undefined,
        city: city || undefined,
        test: !!test,
        emailScrape: req.body.emailScrape !== false,
        existingLeads,
        enrich: !!enrich,
        enrichOptions: enrichOptions || {},
        waterfall: waterfall || {},
        niche: niche || undefined,
        personExtract: !!personExtract,
      });

      const job = {
        id: jobId,
        state,
        practice,
        city,
        test,
        status: 'running',
        cancelled: false,
        emitter,
        leads: [],
        stats: null,
        outputFile: null,
      };
      jobs.set(jobId, job);
      console.log(`[job:${jobId}] Started — state=${state} practice=${practice || 'all'} city=${city || 'all'} test=${!!test} enrich=${!!enrich}`);

      emitter.on('lead', (data) => {
        job.leads.push(data.data);
        broadcast(jobId, { type: 'lead', data: data.data });
      });

      emitter.on('progress', (data) => {
        broadcast(jobId, { type: 'progress', ...data });
      });

      emitter.on('log', (data) => {
        broadcast(jobId, { type: 'log', level: data.level, message: data.message });
      });

      emitter.on('enrichment-progress', (data) => {
        broadcast(jobId, { type: 'enrichment-progress', ...data });
      });

      emitter.on('waterfall-progress', (data) => {
        broadcast(jobId, { type: 'waterfall-progress', ...data });
      });

      emitter.on('person-extract-progress', (data) => {
        broadcast(jobId, { type: 'person-extract-progress', ...data });
      });

      function scheduleJobCleanup() {
        setTimeout(() => {
          jobs.delete(jobId);
          console.log(`[job:${jobId}] Cleaned up from memory (TTL expired)`);
        }, 30 * 60 * 1000);
      }

      emitter.on('complete', (data) => {
        job.status = 'complete';
        job.stats = data.stats;
        job.outputFile = data.outputFile;
        console.log(`[job:${jobId}] Complete — ${data.stats.netNew} new leads, output=${data.outputFile || 'none'}`);

        if (data.leads && data.leads.length > 0) {
          try {
            const leadDb = require('../lib/lead-db');
            const dbStats = leadDb.batchUpsert(data.leads, `scraper:${state}`);
            leadDb.recordScrapeRun({
              state, source: `scraper:${state}`,
              practice_area: practice || 'all',
              leadsFound: data.leads.length,
              leadsNew: dbStats.inserted,
              leadsUpdated: dbStats.updated,
              emailsFound: data.leads.filter(l => l.email).length,
            });

            if (dbStats.inserted > 0) {
              const firmResult = leadDb.shareFirmData();
              const deduceResult = leadDb.deduceWebsitesFromEmail();
              if (firmResult.leadsUpdated > 0 || deduceResult.leadsUpdated > 0) {
                console.log(`[job:${jobId}] Auto-enriched: ${firmResult.leadsUpdated} firm shares, ${deduceResult.leadsUpdated} websites deduced`);
              }
            }

            console.log(`[job:${jobId}] Saved to master DB: ${dbStats.inserted} new, ${dbStats.updated} updated, ${dbStats.unchanged} unchanged`);
          } catch (err) {
            console.error(`[job:${jobId}] Failed to save to master DB:`, err.message);
          }
        }

        broadcast(jobId, { type: 'complete', stats: data.stats, jobId });
        broadcastAll({ type: 'db-update', event: 'scrape-complete', state: job.state, stats: data.stats });
        fireWebhookEvent('scrape.complete', { state: job.state, stats: data.stats });
        scheduleJobCleanup();
      });

      emitter.on('cancelled-complete', (data) => {
        job.status = 'cancelled';
        job.stats = data.stats;
        job.outputFile = data.outputFile;
        console.log(`[job:${jobId}] Cancelled — ${data.stats.netNew} leads collected before cancel`);
        broadcast(jobId, { type: 'cancelled-complete', stats: data.stats, jobId });
        scheduleJobCleanup();
      });

      emitter.on('error', (data) => {
        job.status = 'error';
        console.error(`[job:${jobId}] Error:`, data.message);
        broadcast(jobId, { type: 'error', message: data.message });
        scheduleJobCleanup();
      });

      res.json({ jobId });
    } catch (err) {
      console.error('[scrape/start] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Industry Scrape ---
  router.post('/scrape/industry', (req, res) => {
    try {
      const { niche, location, radius, test } = req.body;

      if (!niche || !location) {
        return res.status(400).json({ error: 'niche and location are required' });
      }

      const { getRegistry } = require('../lib/registry');
      const registry = getRegistry();

      if (!registry['GOOGLE-MAPS']) {
        return res.status(400).json({ error: 'Google Maps scraper not available' });
      }

      const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;

      const emitter = runPipeline({
        state: 'GOOGLE-MAPS',
        city: location.split(',')[0].trim(),
        test: !!test,
        emailScrape: true,
        enrich: false,
        niche,
        personExtract: true,
        waterfall: {
          masterDbLookup: false,
          fetchProfiles: false,
          crossRefMartindale: false,
          crossRefLawyersCom: false,
          nameLookups: false,
          emailCrawl: true,
        },
      });

      const job = {
        id: jobId,
        state: 'GOOGLE-MAPS',
        practice: '',
        city: location,
        test: !!test,
        status: 'running',
        cancelled: false,
        emitter,
        leads: [],
        stats: null,
        outputFile: null,
        niche,
      };
      jobs.set(jobId, job);
      console.log(`[job:${jobId}] Industry scrape started — niche=${niche} location=${location} test=${!!test}`);

      emitter.on('lead', (data) => { job.leads.push(data.data); broadcast(jobId, { type: 'lead', data: data.data }); });
      emitter.on('progress', (data) => { broadcast(jobId, { type: 'progress', ...data }); });
      emitter.on('log', (data) => { broadcast(jobId, { type: 'log', level: data.level, message: data.message }); });
      emitter.on('enrichment-progress', (data) => { broadcast(jobId, { type: 'enrichment-progress', ...data }); });
      emitter.on('waterfall-progress', (data) => { broadcast(jobId, { type: 'waterfall-progress', ...data }); });
      emitter.on('person-extract-progress', (data) => { broadcast(jobId, { type: 'person-extract-progress', ...data }); });

      function scheduleJobCleanup() {
        setTimeout(() => { jobs.delete(jobId); }, 30 * 60 * 1000);
      }

      emitter.on('complete', (data) => {
        job.status = 'complete';
        job.stats = data.stats;
        job.outputFile = data.outputFile;
        console.log(`[job:${jobId}] Industry scrape complete — ${data.stats.netNew} leads`);
        broadcast(jobId, { type: 'complete', stats: data.stats, jobId });
        scheduleJobCleanup();
      });

      emitter.on('cancelled-complete', (data) => {
        job.status = 'cancelled';
        job.stats = data.stats;
        broadcast(jobId, { type: 'cancelled-complete', stats: data.stats, jobId });
        scheduleJobCleanup();
      });

      emitter.on('error', (data) => {
        job.status = 'error';
        console.error(`[job:${jobId}] Industry scrape error:`, data.message);
        broadcast(jobId, { type: 'error', message: data.message });
        scheduleJobCleanup();
      });

      res.json({ jobId });
    } catch (err) {
      console.error('[scrape/industry] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Bulk Scrape ---
  let _bulkScraper = null;

  router.post('/scrape/bulk', (req, res) => {
    try {
      const BulkScraper = require('../lib/bulk-scraper');
      if (_bulkScraper && _bulkScraper.running) {
        return res.json({ status: 'already_running', progress: _bulkScraper.getProgress() });
      }

      _bulkScraper = new BulkScraper();
      const { test, countries, scrapers } = req.body || {};

      res.json({ status: 'started', message: 'Bulk scrape started in background' });

      _bulkScraper.run({ test: !!test, countries, scrapers })
        .catch(err => console.error('[Bulk] Error:', err.message));
    } catch (err) {
      console.error('[scrape/bulk] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/scrape/bulk/status', (req, res) => {
    try {
      if (!_bulkScraper) {
        return res.json({ running: false, progress: null });
      }
      res.json(_bulkScraper.getProgress());
    } catch (err) {
      console.error('[scrape/bulk/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  router.post('/scrape/bulk/cancel', (req, res) => {
    try {
      if (_bulkScraper && _bulkScraper.running) {
        _bulkScraper.cancel();
        res.json({ ok: true });
      } else {
        res.json({ ok: false, message: 'No bulk scrape running' });
      }
    } catch (err) {
      console.error('[scrape/bulk/cancel] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Get job status ---
  router.get('/scrape/:id/status', (req, res) => {
    try {
      const job = jobs.get(req.params.id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      res.json({
        jobId: job.id,
        status: job.status,
        state: job.state,
        practice: job.practice,
        city: job.city,
        test: job.test,
        stats: job.stats,
        leadCount: job.leads.length,
        leads: job.leads.slice(-20),
        hasOutputFile: !!job.outputFile,
      });
    } catch (err) {
      console.error('[scrape/:id/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Cancel a running scrape job ---
  router.post('/scrape/:id/cancel', (req, res) => {
    try {
      const job = jobs.get(req.params.id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      if (job.status !== 'running') {
        return res.status(400).json({ error: 'Job is not running' });
      }
      job.cancelled = true;
      job.emitter.emit('cancel');
      res.json({ ok: true });
    } catch (err) {
      console.error('[scrape/:id/cancel] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Download CSV for a completed job ---
  router.get('/scrape/:id/download', (req, res) => {
    try {
      const job = jobs.get(req.params.id);
      if (!job) {
        console.error(`[download] Job not found: ${req.params.id} (active jobs: ${[...jobs.keys()].filter(k => !k.startsWith('upload-')).join(', ') || 'none'})`);
        return res.status(404).json({ error: 'Job not found' });
      }
      if (!job.outputFile) {
        console.error(`[download] No output file for ${req.params.id} (status=${job.status}, leads=${job.leads.length})`);
        return res.status(400).json({ error: 'No output file available' });
      }
      if (!fs.existsSync(job.outputFile)) {
        console.error(`[download] Output file missing from disk: ${job.outputFile}`);
        return res.status(404).json({ error: 'Output file not found on disk' });
      }

      const filename = path.basename(job.outputFile);
      res.download(job.outputFile, filename);
    } catch (err) {
      console.error('[scrape/:id/download] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrichment preview ---
  router.get('/scrape/:id/enrich-preview', async (req, res) => {
    const job = jobs.get(req.params.id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }
    if (job.leads.length === 0) {
      return res.json({ samples: [] });
    }

    const enrichable = job.leads.filter(l => l.email || l.website);
    const pool = enrichable.length > 0 ? enrichable : job.leads;
    const samples = pool.slice(0, 3).map(l => ({
      name: `${l.first_name} ${l.last_name}`,
      hasEmail: !!l.email,
      hasWebsite: !!l.website,
      hasPhone: !!l.phone,
      canDeriveWebsite: !!(l.email && l.email.includes('@') && !['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'].some(d => l.email.endsWith(d))),
      canFindLinkedIn: !!l.website,
      potentialEnrichment: (!l.website && l.email ? 'website' : '') + (!l.linkedin_url && l.website ? ', linkedin' : '') + (!l.title && l.website ? ', title' : ''),
    }));

    res.json({ samples, totalLeads: job.leads.length, enrichableCount: enrichable.length });
  });

  // --- Health check ---
  router.get('/health', async (req, res) => {
    try {
      const { getRegistry } = require('../lib/registry');
      const SCRAPERS = getRegistry();
      const results = {};
      const https = require('https');
      const http = require('http');

      const checkUrl = (url) => new Promise((resolve) => {
        const protocol = url.startsWith('https') ? https : http;
        const req = protocol.get(url, { timeout: 8000 }, (r) => {
          r.resume();
          resolve(r.statusCode < 500 ? 'green' : 'yellow');
        });
        req.on('error', () => resolve('red'));
        req.on('timeout', () => { req.destroy(); resolve('red'); });
      });

      const checks = Object.entries(SCRAPERS).map(async ([code, loader]) => {
        const scraper = loader();
        try {
          const status = await checkUrl(scraper.baseUrl);
          results[code] = status;
        } catch {
          results[code] = 'red';
        }
      });

      await Promise.all(checks);
      res.json(results);
    } catch (err) {
      console.error('[health] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Signal Engine Endpoints ---
  router.get('/signals', (req, res) => {
    try {
      const { getRecent, getCount } = require('../lib/signal-db');
      const limit = Math.min(parseInt(req.query.limit) || 50, 5000);
      const signals = getRecent(limit);
      const total = getCount();
      res.json({ signals, total });
    } catch (err) {
      console.error('[signals] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  let _scanRunning = false;
  let _lastScanResult = null;

  router.post('/signals/scan', (req, res) => {
    try {
      if (_scanRunning) {
        return res.json({ status: 'already_running', message: 'Scan is already in progress' });
      }

      _scanRunning = true;
      res.json({ status: 'started', message: 'Scan started in background' });

      const jobBoards = require('../watchers/job-boards');
      jobBoards.run()
        .then(count => {
          _lastScanResult = { newSignals: count, completedAt: new Date().toISOString() };
          console.log(`[Signal] Manual scan complete — ${count} new signals`);
        })
        .catch(err => {
          _lastScanResult = { error: err.message, completedAt: new Date().toISOString() };
          console.error('[Signal] Manual scan error:', err.message);
        })
        .finally(() => { _scanRunning = false; });
    } catch (err) {
      console.error('[signals/scan] Error:', err.message, err.stack);
      _scanRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/signals/scan-status', (req, res) => {
    try {
      res.json({
        running: _scanRunning,
        lastResult: _lastScanResult,
      });
    } catch (err) {
      console.error('[signals/scan-status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/signals/admissions', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const months = parseInt(req.query.months) || 6;
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getRecentAdmissions(months, limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/signals/admission-stats', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getAdmissionSignals());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Quick Scrape ---
  router.post('/scrape/quick', (req, res) => {
    const { state, test = true } = req.body;
    if (!state) return res.status(400).json({ error: 'state required' });

    const jobId = `quick-${state}-${Date.now()}`;
    const emitter = runPipeline({ state, test: !!test });

    const job = { id: jobId, state, status: 'running', leads: [], stats: {}, startedAt: Date.now() };
    jobs.set(jobId, job);

    emitter.on('lead', d => job.leads.push(d.data));
    emitter.on('complete', (data) => {
      job.status = 'complete';
      job.stats = data.stats || {};

      if (job.leads.length > 0) {
        try {
          const leadDb = require('../lib/lead-db');
          const dbStats = leadDb.batchUpsert(job.leads, `scraper:${state}`);
          leadDb.recordScrapeRun({
            state, source: `${state.toLowerCase()}_bar`,
            leadsFound: job.leads.length,
            leadsNew: dbStats.inserted,
            leadsUpdated: dbStats.updated,
            emailsFound: job.leads.filter(l => l.email).length,
          });
          if (dbStats.inserted > 0) {
            leadDb.shareFirmData();
            leadDb.deduceWebsitesFromEmail();
          }
          job.dbStats = dbStats;
        } catch (err) {
          console.error(`[quick:${state}] DB save failed:`, err.message);
        }
      }
    });
    emitter.on('error', (data) => {
      job.status = 'error';
      job.error = data.message;
    });

    setTimeout(() => jobs.delete(jobId), 10 * 60 * 1000);

    res.json({ jobId, state });
  });

  router.get('/scrape/quick/:jobId', (req, res) => {
    const job = jobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json({
      status: job.status,
      state: job.state,
      leads: job.leads.length,
      stats: job.stats,
      dbStats: job.dbStats,
      error: job.error,
    });
  });

  return router;
};
