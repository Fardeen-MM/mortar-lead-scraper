#!/usr/bin/env node
/**
 * Mortar Lead Scraper — Web Server
 *
 * Express + WebSocket server that provides a browser-based wizard UI
 * for the scrape pipeline. Serves static files from public/, handles
 * CSV uploads, starts scrape jobs, and streams live progress via WS.
 */

require('dotenv').config();
const path = require('path');
const fs = require('fs');
const express = require('express');
const multer = require('multer');
const { WebSocketServer } = require('ws');
const { readCSV } = require('./lib/csv-handler');
const { runPipeline } = require('./lib/pipeline');
const { getScraperMetadata } = require('./lib/registry');
const { readLogTail } = require('./lib/logger');
const metrics = require('./lib/metrics');

const app = express();
const PORT = process.env.PORT || 3000;

// Ensure upload and output dirs exist
const UPLOAD_DIR = path.join(__dirname, 'data', 'uploads');
const OUTPUT_DIR = path.join(__dirname, 'data', 'output');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// Multer for file uploads
const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB max
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.toLowerCase().endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Only CSV files are allowed'));
    }
  },
});

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Request logging
app.use((req, res, next) => {
  if (req.path.startsWith('/api/')) {
    const start = Date.now();
    res.on('finish', () => {
      const ms = Date.now() - start;
      const status = res.statusCode;
      const label = status >= 400 ? '✗' : '✓';
      console.log(`  ${label} ${req.method} ${req.path} → ${status} (${ms}ms)`);
    });
  }
  next();
});

// Active jobs store
const jobs = new Map();

// --- REST Endpoints ---

// Get available scrapers and their practice areas
app.get('/api/config', (req, res) => {
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
});

// Upload existing leads CSV for dedup
app.post('/api/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  try {
    const leads = await readCSV(req.file.path);
    // Store parsed leads associated with the upload
    const uploadId = req.file.filename;
    jobs.set(`upload-${uploadId}`, { leads, originalName: req.file.originalname });

    // Clean up uploaded temp file after successful parsing
    fs.unlink(req.file.path, (err) => {
      if (err) console.error('[upload] Failed to delete temp file:', err.message);
    });

    res.json({
      uploadId,
      count: leads.length,
      originalName: req.file.originalname,
    });
  } catch (err) {
    // Clean up temp file on error too
    fs.unlink(req.file.path, () => {});
    console.error('[upload] CSV parse error:', err.message, err.stack);
    res.status(400).json({ error: `Failed to parse CSV: ${err.message}` });
  }
});

// Start a scrape job
app.post('/api/scrape/start', (req, res) => {
  const { state, practice, city, test, uploadId, enrich, enrichOptions, waterfall, niche, personExtract } = req.body;

  if (!state) {
    return res.status(400).json({ error: 'State is required' });
  }

  // Validate state against registered scrapers
  const { getRegistry } = require('./lib/registry');
  const registry = getRegistry();
  if (!registry[state]) {
    return res.status(400).json({ error: `Unknown scraper: ${state}` });
  }

  const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;

  // Get existing leads from upload if provided
  let existingLeads = [];
  if (uploadId) {
    const uploadData = jobs.get(`upload-${uploadId}`);
    if (uploadData) {
      existingLeads = uploadData.leads;
    }
  }

  // Start pipeline
  const emitter = runPipeline({
    state,
    practice: practice || undefined,
    city: city || undefined,
    test: !!test,
    emailScrape: req.body.emailScrape !== false, // Default on — crawl firm websites for emails
    existingLeads,
    enrich: !!enrich,
    enrichOptions: enrichOptions || {},
    waterfall: waterfall || {},
    niche: niche || undefined,
    personExtract: !!personExtract,
  });

  // Store job
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

  // Forward emitter events to all WebSocket clients subscribed to this job
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

  // Schedule job cleanup after terminal state (30 min TTL)
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

    // Auto-save leads to master database
    if (data.leads && data.leads.length > 0) {
      try {
        const leadDb = require('./lib/lead-db');
        const dbStats = leadDb.batchUpsert(data.leads, `scraper:${state}`);
        leadDb.recordScrapeRun({
          state, source: `scraper:${state}`,
          practice_area: practice || 'all',
          leadsFound: data.leads.length,
          leadsNew: dbStats.inserted,
          leadsUpdated: dbStats.updated,
          emailsFound: data.leads.filter(l => l.email).length,
        });

        // Auto-enrich: share firm data + deduce websites for new leads
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
});

// Get job status (for session restore after page refresh)
app.get('/api/scrape/:id/status', (req, res) => {
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
});

// Cancel a running scrape job
app.post('/api/scrape/:id/cancel', (req, res) => {
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
});

// Download CSV for a completed job
app.get('/api/scrape/:id/download', (req, res) => {
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
});

// Health check — test each scraper's connectivity
app.get('/api/health', async (req, res) => {
  const { getRegistry } = require('./lib/registry');
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
});

// --- Signal Engine Endpoints ---

// Get signals (paginated)
app.get('/api/signals', (req, res) => {
  const { getRecent, getCount } = require('./lib/signal-db');
  const limit = Math.min(parseInt(req.query.limit) || 50, 5000);
  const signals = getRecent(limit);
  const total = getCount();
  res.json({ signals, total });
});

// Trigger a manual scan (non-blocking — responds immediately, runs in background)
let _scanRunning = false;
let _lastScanResult = null;

app.post('/api/signals/scan', (req, res) => {
  if (_scanRunning) {
    return res.json({ status: 'already_running', message: 'Scan is already in progress' });
  }

  _scanRunning = true;
  res.json({ status: 'started', message: 'Scan started in background' });

  const jobBoards = require('./watchers/job-boards');
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
});

// Check scan status
app.get('/api/signals/scan-status', (req, res) => {
  res.json({
    running: _scanRunning,
    lastResult: _lastScanResult,
  });
});

// --- Bulk Scraper Endpoints ---

let _bulkScraper = null;

app.post('/api/scrape/bulk', (req, res) => {
  const BulkScraper = require('./lib/bulk-scraper');
  if (_bulkScraper && _bulkScraper.running) {
    return res.json({ status: 'already_running', progress: _bulkScraper.getProgress() });
  }

  _bulkScraper = new BulkScraper();
  const { test, countries, scrapers } = req.body || {};

  res.json({ status: 'started', message: 'Bulk scrape started in background' });

  _bulkScraper.run({ test: !!test, countries, scrapers })
    .catch(err => console.error('[Bulk] Error:', err.message));
});

app.get('/api/scrape/bulk/status', (req, res) => {
  if (!_bulkScraper) {
    return res.json({ running: false, progress: null });
  }
  res.json(_bulkScraper.getProgress());
});

app.post('/api/scrape/bulk/cancel', (req, res) => {
  if (_bulkScraper && _bulkScraper.running) {
    _bulkScraper.cancel();
    res.json({ ok: true });
  } else {
    res.json({ ok: false, message: 'No bulk scrape running' });
  }
});

// --- Website Finder Endpoint ---

let _websiteFinderRunning = false;
let _websiteFinderProgress = null;

app.post('/api/leads/find-websites', (req, res) => {
  if (_websiteFinderRunning) {
    return res.json({ status: 'already_running', progress: _websiteFinderProgress });
  }

  _websiteFinderRunning = true;
  _websiteFinderProgress = { current: 0, total: 0, found: 0 };
  res.json({ status: 'started', message: 'Website finder started in background' });

  const { batchFindWebsites } = require('./lib/website-finder');
  const leadDb = require('./lib/lead-db');

  (async () => {
    // Get leads without website from master DB
    const db = leadDb.getDb();
    const leadsNeedingWebsite = db.prepare(`
      SELECT * FROM leads
      WHERE (website = '' OR website IS NULL)
        AND firm_name != '' AND firm_name IS NOT NULL
      ORDER BY updated_at DESC
      LIMIT ?
    `).all(req.body.limit || 500);

    _websiteFinderProgress.total = leadsNeedingWebsite.length;

    const stats = await batchFindWebsites(leadsNeedingWebsite, {
      onProgress: (current, total) => {
        _websiteFinderProgress.current = current;
        _websiteFinderProgress.total = total;
      },
      googleSearch: req.body.googleSearch !== false,
    });

    _websiteFinderProgress.found = stats.found;

    // Save found websites back to DB
    for (const lead of leadsNeedingWebsite) {
      if (lead.website) {
        leadDb.upsertLead(lead);
      }
    }

    console.log(`[Website Finder] Done — found ${stats.found} websites out of ${leadsNeedingWebsite.length}`);
  })()
    .catch(err => console.error('[Website Finder] Error:', err.message))
    .finally(() => { _websiteFinderRunning = false; });
});

app.get('/api/leads/find-websites/status', (req, res) => {
  res.json({ running: _websiteFinderRunning, progress: _websiteFinderProgress });
});

// --- Lead Database Endpoints ---

// Get TAM stats
app.get('/api/leads/stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getStats());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Search leads (enhanced with sorting, score range, practice area, tags)
app.get('/api/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { q, state, country, hasEmail, hasPhone, hasWebsite, practiceArea, minScore, maxScore, tags, tag, source, sort, order, limit, offset } = req.query;
    const result = leadDb.searchLeads(q, {
      state, country, practiceArea, tags: tags || tag, source,
      hasEmail: hasEmail === 'true',
      hasPhone: hasPhone === 'true',
      hasWebsite: hasWebsite === 'true',
      minScore: minScore ? Number(minScore) : undefined,
      maxScore: maxScore ? Number(maxScore) : undefined,
      sort, order,
      limit: Math.max(1, Math.min(parseInt(limit) || 100, 1000)),
      offset: Math.max(0, parseInt(offset) || 0),
    });
    res.json({ leads: result.leads, count: result.leads.length, total: result.total });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get per-state coverage analysis
app.get('/api/leads/coverage', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getStateCoverage());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Merge duplicate leads
app.post('/api/leads/merge-duplicates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const dryRun = req.body.dryRun === true;
    const result = leadDb.mergeDuplicates({ dryRun });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/leads/merge-preview', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { keepId, deleteId } = req.query;
    if (!keepId || !deleteId) return res.status(400).json({ error: 'keepId and deleteId required' });
    const preview = leadDb.getMergePreview(parseInt(keepId), parseInt(deleteId));
    if (!preview) return res.status(404).json({ error: 'Lead not found' });
    res.json(preview);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/auto-merge', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const threshold = req.body.confidenceThreshold || 90;
    const result = leadDb.autoMergeDuplicates(threshold);
    fireWebhookEvent('lead.merged', { merged: result.merged });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Export leads as CSV
app.get('/api/leads/export', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { writeCSV, generateOutputPath } = require('./lib/csv-handler');
    const { state, country, hasEmail, hasPhone, verified } = req.query;
    const leads = leadDb.exportLeads({
      state, country,
      hasEmail: hasEmail === 'true',
      hasPhone: hasPhone === 'true',
      verified: verified === 'true',
    });
    if (leads.length === 0) {
      return res.status(404).json({ error: 'No leads match filters' });
    }
    const outputFile = generateOutputPath('EXPORT', '');
    writeCSV(outputFile, leads).then(() => {
      const filename = path.basename(outputFile);
      leadDb.recordExport('csv', leads.length, req.query, filename);
      res.download(outputFile, filename);
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Export leads in Instantly.ai format (email, first_name, last_name, company_name, personalization)
app.get('/api/leads/export/instantly', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { createObjectCsvWriter } = require('csv-writer');
    const { generateOutputPath } = require('./lib/csv-handler');
    const { state, country } = req.query;

    const leads = leadDb.exportLeads({
      state, country,
      hasEmail: true, // Instantly requires email
    });

    if (leads.length === 0) {
      return res.status(404).json({ error: 'No leads with email found' });
    }

    // Map to Instantly format
    const instantlyLeads = leads.map(l => ({
      email: l.email,
      first_name: l.first_name,
      last_name: l.last_name,
      company_name: l.firm_name || '',
      phone: l.phone || '',
      website: l.website || '',
      city: l.city || '',
      state: l.state || '',
      personalization: l.practice_area
        ? `I noticed you practice ${l.practice_area} in ${l.city || l.state}`
        : `I came across your firm${l.firm_name ? ' ' + l.firm_name : ''} in ${l.city || l.state}`,
    }));

    const outputFile = generateOutputPath('INSTANTLY-EXPORT', '');
    const writer = createObjectCsvWriter({
      path: outputFile,
      header: [
        { id: 'email', title: 'email' },
        { id: 'first_name', title: 'first_name' },
        { id: 'last_name', title: 'last_name' },
        { id: 'company_name', title: 'company_name' },
        { id: 'phone', title: 'phone' },
        { id: 'website', title: 'website' },
        { id: 'city', title: 'city' },
        { id: 'state', title: 'state' },
        { id: 'personalization', title: 'personalization' },
      ],
    });

    writer.writeRecords(instantlyLeads).then(() => {
      res.download(outputFile, path.basename(outputFile));
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Pre-populate Master DB from Directories ---

let _prepopRunning = false;
let _prepopProgress = null;

app.post('/api/leads/prepopulate', (req, res) => {
  if (_prepopRunning) {
    return res.json({ status: 'already_running', progress: _prepopProgress });
  }

  _prepopRunning = true;
  _prepopProgress = { current: 0, total: 0, totalLeads: 0, totalNew: 0, currentSource: '', results: [] };
  res.json({ status: 'started', message: 'Pre-population started in background' });

  const leadDb = require('./lib/lead-db');
  const { runPipeline } = require('./lib/pipeline');
  const sources = req.body.sources || ['AVVO', 'FINDLAW'];
  const test = req.body.test !== false; // Default test mode for safety

  const scraperQueue = [];
  for (const source of sources) {
    scraperQueue.push({ state: source, test });
  }

  _prepopProgress.total = scraperQueue.length;

  (async () => {
    for (let i = 0; i < scraperQueue.length; i++) {
      if (!_prepopRunning) break;
      const { state, test: isTest } = scraperQueue[i];
      _prepopProgress.current = i + 1;
      _prepopProgress.currentSource = state;

      try {
        const result = await new Promise((resolve, reject) => {
          const startTime = Date.now();
          const emitter = runPipeline({
            state,
            test: isTest,
            emailScrape: false,
            waterfall: {
              masterDbLookup: false,
              fetchProfiles: false,
              crossRefMartindale: false,
              crossRefLawyersCom: false,
              nameLookups: false,
              emailCrawl: false,
            },
          });

          let leads = [];
          emitter.on('lead', d => leads.push(d.data));

          emitter.on('complete', (data) => {
            const time = Math.round((Date.now() - startTime) / 1000);
            let dbStats = { inserted: 0, updated: 0 };
            if (leads.length > 0) {
              try { dbStats = leadDb.batchUpsert(leads, `prepop:${state}`); } catch {}
            }
            resolve({ state, leads: leads.length, newInDb: dbStats.inserted, updated: dbStats.updated, time });
          });

          emitter.on('error', (data) => reject(new Error(data.message)));
          setTimeout(() => { emitter.emit('cancel'); reject(new Error('Timeout')); }, 10 * 60 * 1000);
        });

        _prepopProgress.totalLeads += result.leads;
        _prepopProgress.totalNew += result.newInDb;
        _prepopProgress.results.push(result);
        console.log(`[Prepop] ${state}: ${result.leads} leads, ${result.newInDb} new (${result.time}s)`);
      } catch (err) {
        _prepopProgress.results.push({ state, error: err.message });
        console.error(`[Prepop] ${state} failed: ${err.message}`);
      }
    }
    _prepopProgress.currentSource = '';
    console.log(`[Prepop] Done: ${_prepopProgress.totalLeads} leads, ${_prepopProgress.totalNew} new`);
  })()
    .catch(err => console.error('[Prepop] Error:', err.message))
    .finally(() => { _prepopRunning = false; });
});

app.get('/api/leads/prepopulate/status', (req, res) => {
  res.json({ running: _prepopRunning, progress: _prepopProgress });
});

// --- Freshness + Recommendations + Scoring ---

// Get scrape freshness (when each state was last scraped)
app.get('/api/leads/freshness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScrapeHistory());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get smart recommendations
app.get('/api/leads/recommendations', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRecommendations());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get lead score distribution
app.get('/api/leads/scores', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoreDistribution());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Share data across firm members
app.post('/api/leads/share-firm-data', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.shareFirmData();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Deduce websites from email domains
app.post('/api/leads/deduce-websites', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.deduceWebsitesFromEmail();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// One-click "Enrich All" — chains all enrichment steps
let _enrichAllRunning = false;
let _enrichAllProgress = null;

app.post('/api/leads/enrich-all', (req, res) => {
  if (_enrichAllRunning) {
    return res.json({ status: 'already_running', progress: _enrichAllProgress });
  }

  _enrichAllRunning = true;
  _enrichAllProgress = { step: '', steps: [], totalUpdated: 0 };
  res.json({ status: 'started', message: 'Enrich All started in background' });

  const leadDb = require('./lib/lead-db');

  (async () => {
    // Step 1: Merge duplicates
    _enrichAllProgress.step = 'Merging duplicates...';
    const mergeResult = leadDb.mergeDuplicates({});
    _enrichAllProgress.steps.push({ name: 'Merge Dupes', result: `${mergeResult.merged} merged, ${mergeResult.fieldsRecovered} fields recovered` });
    _enrichAllProgress.totalUpdated += mergeResult.merged;

    // Step 2: Share firm data
    _enrichAllProgress.step = 'Sharing firm data...';
    const firmResult = leadDb.shareFirmData();
    _enrichAllProgress.steps.push({ name: 'Firm Share', result: `${firmResult.leadsUpdated} leads updated across ${firmResult.firmsProcessed} firms` });
    _enrichAllProgress.totalUpdated += firmResult.leadsUpdated;

    // Step 3: Deduce websites from email
    _enrichAllProgress.step = 'Deducing websites...';
    const deduceResult = leadDb.deduceWebsitesFromEmail();
    _enrichAllProgress.steps.push({ name: 'Website Deduction', result: `${deduceResult.leadsUpdated} websites from email domains` });
    _enrichAllProgress.totalUpdated += deduceResult.leadsUpdated;

    // Step 4: Re-score all leads
    _enrichAllProgress.step = 'Scoring leads...';
    const scoreResult = leadDb.batchScoreLeads();
    _enrichAllProgress.steps.push({ name: 'Score', result: `${scoreResult.scored} scored, avg: ${scoreResult.avgScore}` });

    _enrichAllProgress.step = 'Done';
    console.log(`[Enrich All] Done: ${_enrichAllProgress.totalUpdated} total updates`);
  })()
    .catch(err => {
      _enrichAllProgress.step = `Error: ${err.message}`;
      console.error('[Enrich All] Error:', err.message);
    })
    .finally(() => { _enrichAllRunning = false; });
});

app.get('/api/leads/enrich-all/status', (req, res) => {
  res.json({ running: _enrichAllRunning, progress: _enrichAllProgress });
});

// Batch score all leads
app.post('/api/leads/score', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchScoreLeads());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- SmartLead Export ---

app.get('/api/leads/export/smartlead', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { createObjectCsvWriter } = require('csv-writer');
    const { generateOutputPath } = require('./lib/csv-handler');
    const { state, country } = req.query;

    const leads = leadDb.exportLeads({ state, country, hasEmail: true });
    if (leads.length === 0) {
      return res.status(404).json({ error: 'No leads with email found' });
    }

    // SmartLead format: email, first_name, last_name, company, phone, tags
    const smartLeads = leads.map(l => ({
      email: l.email,
      first_name: l.first_name,
      last_name: l.last_name,
      company: l.firm_name || '',
      phone: l.phone || '',
      location: [l.city, l.state].filter(Boolean).join(', '),
      tags: [l.practice_area, l.state, l.country].filter(Boolean).join(';'),
    }));

    const outputFile = generateOutputPath('SMARTLEAD-EXPORT', '');
    const writer = createObjectCsvWriter({
      path: outputFile,
      header: [
        { id: 'email', title: 'email' },
        { id: 'first_name', title: 'first_name' },
        { id: 'last_name', title: 'last_name' },
        { id: 'company', title: 'company' },
        { id: 'phone', title: 'phone' },
        { id: 'location', title: 'location' },
        { id: 'tags', title: 'tags' },
      ],
    });

    writer.writeRecords(smartLeads).then(() => {
      res.download(outputFile, path.basename(outputFile));
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Score-based Export ---

app.get('/api/leads/export/by-score', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { writeCSV, generateOutputPath } = require('./lib/csv-handler');
    const minScore = parseInt(req.query.minScore) || 55;
    const { state, country } = req.query;

    const db = leadDb.getDb();
    let where = [`lead_score >= ?`];
    let params = [minScore];

    if (state) { where.push('state = ?'); params.push(state); }
    if (country) { where.push('country = ?'); params.push(country); }

    const leads = db.prepare(
      `SELECT * FROM leads WHERE ${where.join(' AND ')} ORDER BY lead_score DESC, state, city`
    ).all(...params);

    if (leads.length === 0) {
      return res.status(404).json({ error: `No leads with score >= ${minScore}` });
    }

    const outputFile = generateOutputPath(`SCORE-${minScore}`, '');
    writeCSV(outputFile, leads).then(() => {
      res.download(outputFile, path.basename(outputFile));
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Email Verification Endpoint ---

// Verify emails for leads in the database (or a specific batch)
let _verifyRunning = false;
let _verifyProgress = null;

app.post('/api/leads/verify-emails', (req, res) => {
  if (_verifyRunning) {
    return res.json({ status: 'already_running', progress: _verifyProgress });
  }

  _verifyRunning = true;
  _verifyProgress = { current: 0, total: 0, found: 0, verified: 0, invalid: 0 };
  res.json({ status: 'started', message: 'Email verification started in background' });

  const EmailVerifier = require('./lib/email-verifier');
  const leadDb = require('./lib/lead-db');
  const verifier = new EmailVerifier();

  (async () => {
    // Get leads needing email (have website but no email)
    const leadsNeedingEmail = leadDb.getLeadsNeedingEmail(req.body.limit || 200);
    _verifyProgress.total = leadsNeedingEmail.length;

    const stats = await verifier.batchProcess(leadsNeedingEmail, {
      verifyExisting: false,
      findMissing: true,
      onProgress: (current, total) => {
        _verifyProgress.current = current;
        _verifyProgress.total = total;
      },
      isCancelled: () => !_verifyRunning,
    });

    _verifyProgress.found = stats.found;
    _verifyProgress.verified = stats.verified;
    _verifyProgress.invalid = stats.invalid;

    // Save found emails back to DB
    for (const lead of leadsNeedingEmail) {
      if (lead.email) {
        leadDb.upsertLead(lead);
      }
    }

    console.log(`[Email Verify] Done — found ${stats.found} new emails, verified ${stats.verified}`);
  })()
    .catch(err => console.error('[Email Verify] Error:', err.message))
    .finally(() => { _verifyRunning = false; });
});

app.get('/api/leads/verify-status', (req, res) => {
  res.json({ running: _verifyRunning, progress: _verifyProgress });
});

// Enrichment preview — sample 3 leads from a job
app.get('/api/scrape/:id/enrich-preview', async (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  if (job.leads.length === 0) {
    return res.json({ samples: [] });
  }

  // Pick up to 3 leads that have a website or email (most enrichable)
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

// --- Activity Timeline ---
app.get('/api/leads/activity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    res.json(leadDb.getRecentActivity(limit));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Practice Areas ---
app.get('/api/leads/practice-areas', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDistinctPracticeAreas());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Tags ---
app.get('/api/leads/tags', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDistinctTags());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/leads/state-details/:state', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const data = leadDb.getStateDetails(req.params.state);
    if (!data) return res.status(404).json({ error: 'No data for this state' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Database Health Score ---
app.get('/api/leads/health-score', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDatabaseHealth());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Similar Leads ---
app.get('/api/leads/similar/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.findSimilarLeads(parseInt(req.params.id)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Email Template Generator ---
app.post('/api/leads/generate-personalization', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, template } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) {
      return res.status(400).json({ error: 'leadIds array required' });
    }

    const db = leadDb.getDb();
    const leads = db.prepare(`SELECT * FROM leads WHERE id IN (${leadIds.map(() => '?').join(',')})`).all(...leadIds);

    const tmpl = template || 'default';
    const results = leads.map(lead => {
      let personalization = '';
      const firstName = lead.first_name || 'there';
      const city = lead.city || '';
      const state = lead.state || '';
      const firm = lead.firm_name || '';
      const practice = lead.practice_area || '';

      if (tmpl === 'default' || tmpl === 'intro') {
        const parts = [`Hi ${firstName}`];
        if (firm && firm !== 'N/A') parts.push(`I noticed you're at ${firm}`);
        else if (city) parts.push(`I noticed you're based in ${city}`);
        if (practice) parts.push(`and specialize in ${practice.toLowerCase()}`);
        personalization = parts.join(', ') + '.';
      } else if (tmpl === 'referral') {
        personalization = `Hi ${firstName}, I came across your profile${firm ? ' at ' + firm : ''}${city ? ' in ' + city : ''} and thought you might be interested in a quick conversation.`;
      } else if (tmpl === 'value') {
        personalization = `Hi ${firstName}, I work with ${practice ? practice.toLowerCase() + ' ' : ''}attorneys${city ? ' in ' + city : ''} and wanted to share something that might help your practice.`;
      }

      return {
        id: lead.id,
        email: lead.email,
        first_name: lead.first_name,
        last_name: lead.last_name,
        company_name: firm,
        personalization,
      };
    });

    res.json({ leads: results, count: results.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Top Firms ---
app.get('/api/leads/top-firms', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.getTopFirms(limit));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Lead Changelog ---
app.get('/api/leads/changelog', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getRecentChanges(limit));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/leads/changelog/:leadId', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadChangelog(parseInt(req.params.leadId)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Export History ---
app.get('/api/exports/history', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getExportHistory());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Quality Alerts ---
app.post('/api/quality/check', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.runQualityChecks();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/quality/alerts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { type, resolved, limit } = req.query;
    res.json(leadDb.getQualityAlerts({
      type,
      resolved: resolved === 'true',
      limit: parseInt(limit) || 100,
    }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/quality/summary', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAlertSummary());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/quality/resolve/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.resolveAlert(parseInt(req.params.id));
    res.json({ resolved: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Pipeline Stages ---
app.get('/api/pipeline/stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPipelineStats());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/pipeline/stage/:stage', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getLeadsByStage(req.params.stage, limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/pipeline/move', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadId, stage } = req.body;
    if (!leadId || !stage) return res.status(400).json({ error: 'leadId and stage required' });
    res.json(leadDb.moveLeadToStage(leadId, stage));
  } catch (err) {
    const status = err.message.includes('not found') ? 404 : err.message.includes('Invalid') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post('/api/pipeline/bulk-move', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, stage } = req.body;
    if (!leadIds || !stage) return res.status(400).json({ error: 'leadIds and stage required' });
    res.json(leadDb.bulkMoveToStage(leadIds, stage));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scheduled Scrapes ---
app.get('/api/schedules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSchedules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/schedules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { state, practiceArea, frequency, dayOfWeek, hour } = req.body;
    if (!state) return res.status(400).json({ error: 'state required' });
    res.json(leadDb.createSchedule({ state, practiceArea, frequency, dayOfWeek, hour }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/schedules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateSchedule(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/schedules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteSchedule(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Smart Segments ---
app.get('/api/segments', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSegments());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/segments', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, filters, color } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    res.json(leadDb.createSegment(name, description || '', filters, color));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/segments/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateSegment(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/segments/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteSegment(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/segments/query', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { filters } = req.body;
    if (!filters) return res.status(400).json({ error: 'filters required' });
    res.json(leadDb.querySegment(filters));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/segments/query/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { filters, limit = 100, offset = 0 } = req.body;
    if (!filters) return res.status(400).json({ error: 'filters required' });
    res.json(leadDb.querySegmentLeads(filters, limit, offset));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Export Templates ---
app.get('/api/export-templates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getExportTemplates());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/export-templates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, columns, columnRenames, filters } = req.body;
    if (!name || !columns) return res.status(400).json({ error: 'name and columns required' });
    res.json(leadDb.createExportTemplate(name, columns, columnRenames, filters));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/export-templates/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteExportTemplate(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/export-templates/:id/export', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { writeCSV, generateOutputPath } = require('./lib/csv-handler');
    const templates = leadDb.getExportTemplates();
    const template = templates.find(t => t.id === parseInt(req.params.id));
    if (!template) return res.status(404).json({ error: 'Template not found' });
    const leads = leadDb.exportLeads(template.filters || {});
    // Apply column selection and renames
    const mapped = leads.map(lead => {
      const row = {};
      for (const col of template.columns) {
        const renamed = template.columnRenames[col] || col;
        row[renamed] = lead[col] || '';
      }
      return row;
    });
    const outPath = generateOutputPath(`export-${template.name.toLowerCase().replace(/\s+/g, '-')}`);
    writeCSV(mapped, outPath);
    leadDb.recordExport(template.name, mapped.length, JSON.stringify(template.filters), require('path').basename(outPath));
    res.download(outPath);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Pipeline Analytics ---
app.get('/api/analytics/funnel', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPipelineFunnel());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/analytics/source-effectiveness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSourceEffectiveness());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Search Suggestions ---
app.get('/api/leads/suggest', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { q } = req.query;
    res.json(leadDb.getSearchSuggestions(q));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Scoring Rules ---
app.get('/api/scoring/rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoringRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scoring/rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { field, points, condition } = req.body;
    if (!field || points === undefined) return res.status(400).json({ error: 'field and points required' });
    res.json(leadDb.addScoringRule(field, points, condition));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/scoring/rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateScoringRule(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/scoring/rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteScoringRule(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/score-breakdown', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const lead = leadDb.getLeadById(parseInt(req.params.id));
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    res.json(leadDb.getScoreBreakdown(lead));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Email Verification ---
app.get('/api/leads/verification-stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getVerificationStats());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/import-verification', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    const leadDb = require('./lib/lead-db');
    const csv = require('./lib/csv-handler');
    const rows = csv.readCSV(req.file.path);
    const verifications = rows.map(r => ({
      email: r.email || r.Email || r.EMAIL,
      valid: (r.valid || r.status || r.result || '').toString().toLowerCase() === 'valid' ||
             (r.valid || r.status || r.result || '').toString().toLowerCase() === 'true' ||
             (r.valid || r.status || r.result || '').toString() === '1',
      catchAll: (r.catch_all || r.catchAll || r.catch_all_domain || '').toString().toLowerCase() === 'true' ||
                (r.catch_all || r.catchAll || '').toString() === '1',
    })).filter(v => v.email);
    const result = leadDb.bulkImportVerification(verifications);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Webhooks ---
app.get('/api/webhooks', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getWebhooks());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/webhooks', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { url, events, secret } = req.body;
    if (!url || !events) return res.status(400).json({ error: 'url and events required' });
    res.json(leadDb.createWebhook(url, events, secret));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/webhooks/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateWebhook(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/webhooks/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteWebhook(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/webhooks/:id/deliveries', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getWebhookDeliveries(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/webhooks/test', (req, res) => {
  try {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'url required' });
    fireWebhook(url, 'test', { message: 'Webhook test from Mortar Lead Scraper', timestamp: new Date().toISOString() });
    res.json({ sent: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Lead Notes ---
app.get('/api/leads/:id/notes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadNotes(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/:id/notes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { content, author } = req.body;
    if (!content) return res.status(400).json({ error: 'content required' });
    res.json(leadDb.addNote(parseInt(req.params.id), content, author));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/notes/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteNote(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/timeline', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadTimeline(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- Bulk Update ---
app.post('/api/leads/bulk-update', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, updates } = req.body;
    if (!leadIds || !updates) return res.status(400).json({ error: 'leadIds and updates required' });
    const result = leadDb.bulkUpdateLeads(leadIds, updates);
    // Fire webhook
    fireWebhookEvent('lead.bulk_updated', { count: result.updated, updates });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Email Classification ===
app.get('/api/leads/email-classification', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEmailClassification());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/classify-emails', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.classifyAllEmails());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Confidence Scoring ===
app.get('/api/leads/confidence', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getConfidenceDistribution());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/compute-confidence', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchComputeConfidence());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Change Detection / Signals ===
app.get('/api/leads/changes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getRecentChanges2(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/firm-changes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getFirmChanges(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/change-history', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadChangeHistory(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Tag Definitions ===
app.get('/api/tag-definitions', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getTagDefinitions());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tag-definitions', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, color, description, autoRule } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createTagDefinition(name, color, description, autoRule ? JSON.stringify(autoRule) : '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/tag-definitions/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateTagDefinition(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/tag-definitions/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteTagDefinition(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/auto-tag', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.runAutoTagging());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Comparison & Merge ===
app.get('/api/leads/compare', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const id1 = parseInt(req.query.id1);
    const id2 = parseInt(req.query.id2);
    if (!id1 || !id2) return res.status(400).json({ error: 'id1 and id2 required' });
    const result = leadDb.compareLeads(id1, id2);
    if (!result) return res.status(404).json({ error: 'Lead not found' });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/merge-with-choices', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { keepId, mergeId, fieldChoices } = req.body;
    if (!keepId || !mergeId) return res.status(400).json({ error: 'keepId and mergeId required' });
    const result = leadDb.mergeLeadsWithChoices(keepId, mergeId, fieldChoices || {});
    fireWebhookEvent('lead.merged', { keepId, mergeId });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Data Staleness ===
app.get('/api/leads/staleness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getStalenessReport());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Import Preview ===
app.post('/api/leads/import-preview', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    const leadDb = require('./lib/lead-db');
    const csvParse = require('csv-parser');
    const fsImport = require('fs');
    const rows = [];
    const stream = fsImport.createReadStream(req.file.path).pipe(csvParse());
    stream.on('data', (row) => { if (rows.length < 10) rows.push(row); });
    stream.on('end', () => {
      try { fsImport.unlinkSync(req.file.path); } catch {}
      if (rows.length === 0) return res.json({ error: 'Empty CSV' });
      const headers = Object.keys(rows[0]);
      res.json(leadDb.previewImportMapping(headers, rows));
    });
    stream.on('error', (err) => {
      try { fsImport.unlinkSync(req.file.path); } catch {}
      res.status(400).json({ error: 'Failed to parse CSV: ' + err.message });
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Email Validation ===
app.post('/api/leads/validate-emails', async (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = await leadDb.batchValidateEmails((progress) => {
      broadcastAll({ type: 'validation-progress', ...progress });
    });
    broadcastAll({ type: 'validation-complete', ...result });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/validate-email/:email', async (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const syntax = leadDb.validateEmailSyntax(req.params.email);
    if (!syntax.valid) return res.json(syntax);
    const mx = await leadDb.validateEmailMX(req.params.email);
    res.json(mx);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === ICP Scoring ===
app.get('/api/icp/criteria', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getIcpCriteria());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/icp/criteria', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { field, operator, value, weight, label } = req.body;
    if (!field || !operator) return res.status(400).json({ error: 'field and operator required' });
    const result = leadDb.addIcpCriterion(field, operator, value || '', weight || 10, label || '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/icp/criteria/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateIcpCriterion(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/icp/criteria/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteIcpCriterion(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/icp/distribution', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getIcpDistribution());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/icp/score-all', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchComputeIcpScores());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Saved Searches & Alerts ===
app.get('/api/saved-searches', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSavedSearches());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/saved-searches', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, filters, alertEnabled } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    const result = leadDb.createSavedSearch(name, filters, alertEnabled);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/saved-searches/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.deleteSavedSearch(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/saved-searches/alerts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.checkSavedSearchAlerts());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Signals: Recent Admissions ===
app.get('/api/signals/admissions', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const months = parseInt(req.query.months) || 6;
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getRecentAdmissions(months, limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/signals/admission-stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAdmissionSignals());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Outreach Sequences ===
app.get('/api/sequences', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSequences());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/sequences', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createSequence(name, description || '');
    res.json({ id: result.lastInsertRowid, name });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/sequences/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteSequence(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/sequences/:id/steps', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { stepNumber, channel, subject, body, delayDays, variant } = req.body;
    const result = leadDb.addSequenceStep(parseInt(req.params.id), stepNumber || 1, channel || 'email', subject || '', body || '', delayDays || 0, variant || 'A');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/sequences/:id/enroll', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    res.json(leadDb.enrollInSequence(parseInt(req.params.id), leadIds));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sequences/:id/enrollments', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSequenceEnrollments(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sequences/render/:stepId/:leadId', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.renderSequenceStep(parseInt(req.params.stepId), parseInt(req.params.leadId));
    if (!result) return res.status(404).json({ error: 'Step or lead not found' });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Activity Tracking ===
app.post('/api/leads/:id/activity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { action, details } = req.body;
    if (!action) return res.status(400).json({ error: 'action required' });
    leadDb.trackActivity(parseInt(req.params.id), action, details || '');
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/activities', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getLeadActivities(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/engagement', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json({ score: leadDb.getEngagementScore(parseInt(req.params.id)) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/most-engaged', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.getMostEngagedLeads(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Firm Enrichment ===
app.post('/api/firms/enrich', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.enrichFirmData());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/firms/directory', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    const minSize = parseInt(req.query.minSize) || 2;
    res.json(leadDb.getFirmDirectory(limit, minSize));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lookalike Finder ===
app.get('/api/leads/:id/lookalikes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.findLookalikes(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/batch-lookalikes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, limit } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    res.json(leadDb.findBatchLookalikes(leadIds, limit || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Score Decay ===
app.get('/api/leads/decay-preview', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const days = parseInt(req.query.days) || 30;
    res.json(leadDb.getDecayPreview(days));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/apply-decay', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { decayPercent, inactiveDays } = req.body;
    res.json(leadDb.applyScoreDecay(decayPercent || 5, inactiveDays || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Do Not Contact List ===
app.get('/api/dnc', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDncList(req.query.type || null));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/dnc', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { type, value, reason } = req.body;
    if (!type || !value) return res.status(400).json({ error: 'type and value required' });
    leadDb.addToDnc(type, value, reason || '');
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/dnc/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.removeFromDnc(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/dnc/check', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchCheckDnc());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Smart Duplicate Detection ===
app.get('/api/leads/smart-duplicates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.findSmartDuplicates(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/auto-merge', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const dryRun = req.body.dryRun !== false;
    res.json(leadDb.autoMergeDuplicates(dryRun));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Territory Management ===
app.get('/api/territories', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getTerritories());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/territories', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, states, cities, owner } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createTerritory(name, description || '', states || '', cities || '', owner || '');
    res.json({ id: result.lastInsertRowid, name });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/territories/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteTerritory(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/territories/:id/assign', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.assignLeadsToTerritory(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/territories/:id/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getTerritoryLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Source Attribution ===
app.get('/api/leads/source-attribution', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSourceAttribution());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Single Lead Enrichment Info ===
app.get('/api/leads/:id/enrichment-info', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.getLeadForEnrichment(parseInt(req.params.id));
    if (!result) return res.status(404).json({ error: 'Lead not found' });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Intent Signals ===
app.get('/api/leads/intent-signals', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getIntentSignals());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/practice-trends', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPracticeAreaTrends());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Routing Rules ===
app.get('/api/routing-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRoutingRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/routing-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, conditions, actionType, actionValue, priority } = req.body;
    if (!name || !conditions || !actionType || !actionValue) {
      return res.status(400).json({ error: 'name, conditions, actionType, actionValue required' });
    }
    const result = leadDb.createRoutingRule(name, conditions, actionType, actionValue, priority || 0);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/routing-rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteRoutingRule(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/routing-rules/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.runRoutingRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Completeness Heatmap ===
app.get('/api/leads/completeness-heatmap', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCompletenessHeatmap());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/enrichment-recommendations', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEnrichmentRecommendations());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Instantly-Optimized Export ===
app.get('/api/leads/export/instantly', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const leads = leadDb.exportForInstantly({
      state: req.query.state, practiceArea: req.query.practice,
      minScore: req.query.minScore, pipelineStage: req.query.stage,
      tags: req.query.tags,
    });
    if (leads.length === 0) return res.status(404).json({ error: 'No leads match filters' });

    const { createObjectCsvWriter } = require('csv-writer');
    const tmpPath = path.join(OUTPUT_DIR, `instantly-export-${Date.now()}.csv`);
    const headers = Object.keys(leads[0]).map(k => ({ id: k, title: k }));
    const writer = createObjectCsvWriter({ path: tmpPath, header: headers });
    writer.writeRecords(leads).then(() => {
      res.download(tmpPath, 'instantly-leads.csv', () => {
        try { fs.unlinkSync(tmpPath); } catch {}
      });
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/export/instantly/preview', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const leads = leadDb.exportForInstantly({
      state: req.query.state, practiceArea: req.query.practice,
      minScore: req.query.minScore, pipelineStage: req.query.stage,
      tags: req.query.tags,
    });
    res.json({ count: leads.length, sample: leads.slice(0, 5) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Notes ===
app.get('/api/leads/:id/notes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadNotes(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/:id/notes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { content, author } = req.body;
    if (!content) return res.status(400).json({ error: 'content required' });
    res.json(leadDb.addNote(parseInt(req.params.id), content, author || 'user'));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/notes/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteNote(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/notes/:id/pin', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.togglePinNote(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/notes/recent', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRecentNotes(parseInt(req.query.limit) || 20));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Timeline ===
app.get('/api/leads/:id/timeline', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadTimeline(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === A/B Variant Stats ===
app.get('/api/sequences/:id/variants', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSequenceVariantStats(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Smart Lists ===
app.get('/api/smart-lists', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSmartLists());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/smart-lists', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, filters } = req.body;
    if (!name || !filters) return res.status(400).json({ error: 'name and filters required' });
    const result = leadDb.createSmartList(name, description || '', filters);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/smart-lists/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteSmartList(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/smart-lists/:id/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getSmartListLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Custom Scoring Models ===
app.get('/api/scoring-models', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoringModels());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scoring-models', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, weights } = req.body;
    if (!name || !weights) return res.status(400).json({ error: 'name and weights required' });
    const result = leadDb.createScoringModel(name, weights);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/scoring-models/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteScoringModel(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scoring-models/:id/activate', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.activateScoringModel(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scoring-models/apply', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.applyCustomScoring());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Campaign Management ===
app.get('/api/campaigns', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCampaigns());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/campaigns', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createCampaign(name, description || '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/campaigns/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteCampaign(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/campaigns/:id/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
    res.json(leadDb.addLeadsToCampaign(parseInt(req.params.id), leadIds));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/campaigns/:id/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getCampaignLeads(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/campaigns/:id/status', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { status } = req.body;
    if (!status) return res.status(400).json({ error: 'status required' });
    leadDb.updateCampaignStatus(parseInt(req.params.id), status);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Cross-Source Dedup ===
app.get('/api/leads/cross-source-duplicates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getCrossSourceDuplicates(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === KPI Dashboard ===
app.get('/api/leads/kpi-metrics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getKpiMetrics());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Import (Batch 18) ===
app.post('/api/leads/import', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leads, options } = req.body;
    if (!leads || !Array.isArray(leads)) return res.status(400).json({ error: 'leads (array) required' });
    res.json(leadDb.importLeads(leads, options || {}));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/import/map-fields', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { headers } = req.body;
    if (!headers || !Array.isArray(headers)) return res.status(400).json({ error: 'headers (array) required' });
    res.json(leadDb.getImportFieldMapping(headers));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Engagement Heatmap (Batch 18) ===
app.get('/api/leads/engagement-heatmap', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEngagementHeatmap());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/engagement-sparkline', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadEngagementSparkline(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/engagement-timeline', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const days = parseInt(req.query.days) || 30;
    res.json(leadDb.getEngagementTimeline(days));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Bulk Actions (Batch 18) ===
app.post('/api/leads/bulk/tag', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, tag } = req.body;
    if (!leadIds || !tag) return res.status(400).json({ error: 'leadIds and tag required' });
    res.json(leadDb.bulkTagLeads(leadIds, tag));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/bulk/remove-tag', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, tag } = req.body;
    if (!leadIds || !tag) return res.status(400).json({ error: 'leadIds and tag required' });
    res.json(leadDb.bulkRemoveTag(leadIds, tag));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/bulk/assign-owner', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, owner } = req.body;
    if (!leadIds || !owner) return res.status(400).json({ error: 'leadIds and owner required' });
    res.json(leadDb.bulkAssignOwner(leadIds, owner));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/bulk/enroll-campaign', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, campaignId } = req.body;
    if (!leadIds || !campaignId) return res.status(400).json({ error: 'leadIds and campaignId required' });
    res.json(leadDb.bulkEnrollInCampaign(leadIds, campaignId));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/bulk/enroll-sequence', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, sequenceId } = req.body;
    if (!leadIds || !sequenceId) return res.status(400).json({ error: 'leadIds and sequenceId required' });
    res.json(leadDb.bulkEnrollInSequence(leadIds, sequenceId));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/owners', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getOwners());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/owners/:name/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 100;
    res.json(leadDb.getLeadsByOwner(req.params.name, limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Comparison & Merge (Batch 18) ===
app.post('/api/leads/compare', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || leadIds.length < 2) return res.status(400).json({ error: 'leadIds (array of 2+) required' });
    res.json(leadDb.getLeadComparisonData(leadIds));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/merge-with-picks', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { targetId, sourceIds, fieldPicks } = req.body;
    if (!targetId || !sourceIds) return res.status(400).json({ error: 'targetId and sourceIds required' });
    res.json(leadDb.mergeLeadsWithPicks(targetId, sourceIds, fieldPicks || {}));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Leaderboard (Batch 19) ===
app.get('/api/leads/leaderboard', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeaderboard({
      state: req.query.state, practiceArea: req.query.practice,
      metric: req.query.metric, limit: parseInt(req.query.limit) || 50,
    }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/leaderboard-by-state', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeaderboardByState(parseInt(req.query.limit) || 10));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Automation Rules (Batch 19) ===
app.get('/api/automation-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAutomationRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/automation-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, triggerEvent, conditions, actionType, actionValue } = req.body;
    if (!name || !triggerEvent || !actionType) return res.status(400).json({ error: 'name, triggerEvent, and actionType required' });
    const result = leadDb.createAutomationRule(name, triggerEvent, conditions || {}, actionType, actionValue || '');
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/automation-rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteAutomationRule(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/automation-rules/:id/toggle', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.toggleAutomationRule(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/automation-rules/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { event, leadIds } = req.body;
    const leads = leadIds ? leadIds.map(id => leadDb.getLeadById(id)).filter(Boolean) : [];
    res.json(leadDb.runAutomationRules(event || 'lead_added', leads));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Data Quality (Batch 19) ===
app.get('/api/leads/data-quality', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDataQualityReport(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/data-quality-summary', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDataQualitySummary());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Export Profiles (Batch 19) ===
app.get('/api/export-profiles', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getExportProfiles());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/export-profiles', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, filters, columns } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    const result = leadDb.createExportProfile(name, description, filters, columns);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/export-profiles/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteExportProfile(parseInt(req.params.id));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/export-profiles/:id/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const leads = leadDb.runExportProfile(parseInt(req.params.id));
    res.json({ count: leads.length, leads: leads.slice(0, 20) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Contact Timeline (Batch 20) ===
app.get('/api/leads/:id/contacts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getContactTimeline(parseInt(req.params.id), parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/:id/contacts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { channel, direction, subject, notes, outcome } = req.body;
    if (!channel) return res.status(400).json({ error: 'channel required' });
    const result = leadDb.logContact(parseInt(req.params.id), channel, direction, subject, notes, outcome);
    res.json({ id: result.lastInsertRowid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/contact-stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getContactStats(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/contacts/recent', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRecentContacts(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Warm-Up Scoring (Batch 20) ===
app.get('/api/leads/:id/warm-up', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.computeWarmUpScore(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/warm-up-batch', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchComputeWarmUp(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Multi-View (Batch 20) ===
app.get('/api/leads/kanban', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getKanbanData(parseInt(req.query.limit) || 200));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/card-view', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCardViewData({
      state: req.query.state, sortBy: req.query.sort,
      limit: parseInt(req.query.limit) || 50, offset: parseInt(req.query.offset) || 0,
    }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Search & Filters (Batch 20) ===
app.get('/api/leads/typeahead', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.searchTypeahead(req.query.q, parseInt(req.query.limit) || 10));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/filter-facets', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFilterFacets());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Enrichment Queue (Batch 21) ===
app.get('/api/enrichment-queue/status', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEnrichmentQueueStatus());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/enrichment-queue/add', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, source, fieldsRequested } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
    res.json(leadDb.addToEnrichmentQueue(leadIds, source, fieldsRequested));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/enrichment-queue/process', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.processEnrichmentQueue(parseInt(req.body.batchSize) || 10));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/enrichment-queue/clear', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.clearEnrichmentQueue(req.query.status || 'completed'));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Firm Intelligence (Batch 21) ===
app.get('/api/firms', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFirmIntelligence(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/firms/:name', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFirmDetail(req.params.name));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Dedup Merge Queue (Batch 21) ===
app.post('/api/dedup/scan', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.scanForDuplicates(parseInt(req.body.limit) || 200));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/dedup/queue', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDedupQueue(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/dedup/resolve/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { resolution, keepId } = req.body;
    if (!resolution) return res.status(400).json({ error: 'resolution required (merge|skip)' });
    res.json(leadDb.resolveDedupItem(parseInt(req.params.id), resolution, keepId ? parseInt(keepId) : null));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/dedup/stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDedupStats());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Audit Log (Batch 21) ===
app.post('/api/audit/log', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { action, entityType, entityId, details, userName } = req.body;
    if (!action) return res.status(400).json({ error: 'action required' });
    res.json({ id: leadDb.logAuditEvent(action, entityType, entityId, details, userName) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/audit/log', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAuditLog({
      action: req.query.action,
      entityType: req.query.entityType,
      userName: req.query.userName,
      limit: parseInt(req.query.limit) || 50,
      offset: parseInt(req.query.offset) || 0,
    }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/audit/stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAuditStats());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/audit/export', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.exportAuditLog({ startDate: req.query.startDate, endDate: req.query.endDate }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lifecycle Tracking (Batch 22) ===
app.post('/api/leads/:id/stage-transition', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { fromStage, toStage, triggeredBy } = req.body;
    if (!toStage) return res.status(400).json({ error: 'toStage required' });
    res.json(leadDb.recordStageTransition(parseInt(req.params.id), fromStage, toStage, triggeredBy));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/lifecycle/analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLifecycleAnalytics());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/lifecycle', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadLifecycle(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Sequence Analytics (Batch 22) ===
app.post('/api/sequences/:id/event', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { stepNumber, leadId, eventType, variant, metadata } = req.body;
    if (!leadId || !eventType) return res.status(400).json({ error: 'leadId and eventType required' });
    res.json(leadDb.recordSequenceEvent(parseInt(req.params.id), stepNumber || 1, parseInt(leadId), eventType, variant, metadata));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sequences/:id/analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSequenceAnalytics(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sequences/performance', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAllSequencePerformance());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Activity Scoring (Batch 22) ===
app.get('/api/leads/:id/activity-score', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.computeActivityScore(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/activity-scores/batch', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.batchActivityScores(parseInt(req.query.limit) || 100));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/activity-scores/config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getActivityScoreConfig());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/activity-scores/config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateActivityScoreConfig(req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Bulk Enrichment (Batch 22) ===
app.post('/api/bulk-enrichment/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, sourceFilter } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
    res.json(leadDb.createBulkEnrichmentRun(leadIds, sourceFilter));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/bulk-enrichment/runs', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getBulkEnrichmentRuns(parseInt(req.query.limit) || 20));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/bulk-enrichment/process/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.processBulkEnrichmentBatch(parseInt(req.params.id), parseInt(req.body.batchSize) || 20));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/bulk-enrichment/diff/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getBulkEnrichmentDiff(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Relationship Graph (Batch 23) ===
app.get('/api/leads/:id/relationships', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.buildRelationshipGraph(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/relationships', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIdA, leadIdB, type, strength } = req.body;
    if (!leadIdA || !leadIdB || !type) return res.status(400).json({ error: 'leadIdA, leadIdB, type required' });
    res.json(leadDb.addRelationship(parseInt(leadIdA), parseInt(leadIdB), type, strength || 0.5));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/firm-network', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFirmNetwork(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Data Freshness (Batch 23) ===
app.get('/api/freshness/report', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFreshnessReport({ staleDays: parseInt(req.query.staleDays) || 90, limit: parseInt(req.query.limit) || 100 }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/:id/freshness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadFreshness(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/freshness/verify', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadId, fieldName, source } = req.body;
    if (!leadId || !fieldName) return res.status(400).json({ error: 'leadId and fieldName required' });
    res.json(leadDb.recordFieldVerification(parseInt(leadId), fieldName, source));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Scoring Model Comparison (Batch 23) ===
app.get('/api/scoring-models/compare', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { modelA, modelB } = req.query;
    if (!modelA || !modelB) return res.status(400).json({ error: 'modelA and modelB query params required' });
    res.json(leadDb.compareScoringModels(parseInt(modelA), parseInt(modelB)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/scoring-models/rankings', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoringModelRankings());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Geographic Clustering (Batch 23) ===
app.get('/api/geographic/clusters', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getGeographicClusters({ minClusterSize: parseInt(req.query.minSize) || 5, limit: parseInt(req.query.limit) || 50 }));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/geographic/penetration/:state', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getMarketPenetration(req.params.state));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Priority Inbox (Batch 24) ===
app.get('/api/priority-inbox', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPriorityInbox(parseInt(req.query.limit) || 25));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/smart-recommendations', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSmartRecommendations());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Practice Area Analytics (Batch 24) ===
app.get('/api/practice-areas/analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPracticeAreaAnalytics(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Source ROI (Batch 24) ===
app.get('/api/source-roi', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSourceROI());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/source-roi/compare', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { sourceA, sourceB } = req.query;
    if (!sourceA || !sourceB) return res.status(400).json({ error: 'sourceA and sourceB required' });
    res.json(leadDb.getSourceComparison(sourceA, sourceB));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Compliance Dashboard (Batch 24) ===
app.get('/api/compliance/dashboard', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getComplianceDashboard());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/compliance/opt-out', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { email, reason, source } = req.body;
    if (!email) return res.status(400).json({ error: 'email required' });
    res.json(leadDb.addOptOut(email, reason, source));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/compliance/opt-out', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'email required' });
    res.json(leadDb.removeOptOut(email));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/compliance/check/:email', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.checkEmailCompliance(req.params.email));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/compliance/consent', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadId, consentType, status, source, notes } = req.body;
    if (!leadId || !consentType) return res.status(400).json({ error: 'leadId and consentType required' });
    res.json(leadDb.recordConsent(parseInt(leadId), consentType, status, source, notes));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Journey Timeline (Batch 25) ===
app.get('/api/leads/:id/journey', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadJourney(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Predictive Scoring (Batch 25) ===
app.get('/api/predictive-scores', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPredictiveScores(parseInt(req.query.limit) || 100));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Team Performance (Batch 25) ===
app.get('/api/team/performance', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getTeamPerformance());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Email Deliverability (Batch 25) ===
app.get('/api/email/deliverability', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEmailDeliverability());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Tagging Rules Engine (Batch 26) ===
app.get('/api/tag-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getTagRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/tag-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, tag, conditions, logic } = req.body;
    res.json(leadDb.createTagRule(name, tag, conditions, logic));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/tag-rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteTagRule(req.params.id);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/tag-rules/:id/toggle', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.toggleTagRule(req.params.id));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/tag-rules/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.runTagRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Nurture Cadence (Batch 26) ===
app.get('/api/nurture/cadence', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getNurtureCadence(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/nurture/analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCadenceAnalytics());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Custom Fields (Batch 26) ===
app.get('/api/custom-fields', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCustomFieldDefs());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/custom-fields', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { fieldName, fieldType, options, required, defaultValue } = req.body;
    res.json(leadDb.createCustomField(fieldName, fieldType, options, required, defaultValue));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/custom-fields/:name', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteCustomField(req.params.name);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/custom-fields/stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCustomFieldStats());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/leads/:id/custom-fields', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCustomFieldValues(parseInt(req.params.id)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.put('/api/leads/:id/custom-fields', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { fieldName, value } = req.body;
    res.json(leadDb.setCustomFieldValue(parseInt(req.params.id), fieldName, value));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Score Decay (Batch 26) ===
app.get('/api/score-decay/config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDecayConfig());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.put('/api/score-decay/config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateDecayConfig(req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/score-decay/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.runScoreDecay());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/score-decay/preview', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.getDecayPreview2(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lookalike Finder (Batch 27) ===
app.get('/api/leads/:id/lookalikes', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 20;
    res.json(leadDb.findLookalikes(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Conversion Funnel (Batch 27) ===
app.get('/api/funnel', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getConversionFunnel());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Velocity (Batch 27) ===
app.get('/api/velocity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const days = parseInt(req.query.days) || 30;
    res.json(leadDb.getLeadVelocity(days));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Completeness Matrix (Batch 27) ===
app.get('/api/completeness-matrix', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCompletenessMatrix());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lead Clustering (Batch 28) ===
app.get('/api/clusters', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLeadClusters());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === A/B Test Framework (Batch 28) ===
app.get('/api/ab-tests', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAbTests());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/ab-tests', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, variantA, variantB, metric } = req.body;
    res.json(leadDb.createAbTest(name, description, variantA, variantB, metric));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/ab-tests/:id/assign', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    res.json(leadDb.assignLeadsToAbTest(parseInt(req.params.id), leadIds || []));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/ab-tests/:id/outcome', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadId, responded } = req.body;
    res.json(leadDb.recordAbTestOutcome(parseInt(req.params.id), leadId, responded));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/ab-tests/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteAbTest(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Re-engagement Scoring (Batch 28) ===
app.get('/api/reengagement', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getReengagementLeads(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Attribution Model (Batch 28) ===
app.get('/api/attribution', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAttributionModel());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Response Time SLA (Batch 29) ===
app.get('/api/sla', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getResponseTimeSLA());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Market Saturation (Batch 29) ===
app.get('/api/saturation', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getMarketSaturation());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Enrichment Waterfall (Batch 29) ===
app.get('/api/enrichment-waterfall', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEnrichmentWaterfall());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Competitive Intelligence (Batch 29) ===
app.get('/api/competitive', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCompetitiveIntelligence());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Sequence Templates (Batch 30) ===
app.get('/api/sequence-templates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSequenceTemplates());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/sequence-templates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, steps } = req.body;
    res.json(leadDb.createSequenceTemplate(name, description, steps));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.put('/api/sequence-templates/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.updateSequenceTemplate(parseInt(req.params.id), req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/sequence-templates/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteSequenceTemplate(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/sequence-templates/:id/render/:leadId', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.renderSequenceTemplate(parseInt(req.params.id), parseInt(req.params.leadId));
    res.json(result || { error: 'Template or lead not found' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Data Quality Rules (Batch 30) ===
app.get('/api/quality-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getQualityRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/quality-rules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, field, checkType, checkValue, severity, flagTag } = req.body;
    res.json(leadDb.createQualityRule(name, field, checkType, checkValue, severity, flagTag));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/quality-rules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteQualityRule(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/quality-rules/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.runQualityRules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Unified Timeline (Batch 30) ===
app.get('/api/leads/:id/timeline', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getLeadTimeline(parseInt(req.params.id), limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Export Scheduler (Batch 30) ===
app.get('/api/export-schedules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getExportSchedules());
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/export-schedules', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, filters, columns, frequency } = req.body;
    res.json(leadDb.createExportSchedule(name, filters, columns, frequency));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/api/export-schedules/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteExportSchedule(parseInt(req.params.id));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/export-schedules/:id/run', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const result = leadDb.runExportSchedule(parseInt(req.params.id));
    res.json(result || { error: 'Schedule not found' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Propensity Model (Batch 31) ===
app.get('/api/propensity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getPropensityScores(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Cohort Analysis (Batch 31) ===
app.get('/api/cohorts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCohortAnalysis());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Channel Preferences (Batch 31) ===
app.get('/api/channel-preferences', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getChannelPreferences(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Jurisdiction Benchmarks (Batch 31) ===
app.get('/api/benchmarks', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getJurisdictionBenchmarks());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Deal Estimation (Batch 32) ===
app.get('/api/deals', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 40;
    res.json(leadDb.getDealEstimates(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Outreach Calendar (Batch 32) ===
app.get('/api/outreach-calendar', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getOutreachCalendar());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Risk Scoring (Batch 32) ===
app.get('/api/risk-scores', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getRiskScores(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Network Mapping (Batch 32) ===
app.get('/api/network-map', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 30;
    res.json(leadDb.getNetworkMap(limit));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Journey Mapping (Batch 33) ===
app.get('/api/journey-mapping', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getJourneyMapping(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Scoring Audit (Batch 33) ===
app.get('/api/scoring-audit', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoringAudit(parseInt(req.query.limit) || 40));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Geographic Expansion (Batch 33) ===
app.get('/api/geo-expansion', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getGeoExpansion());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Freshness Alerts (Batch 33) ===
app.get('/api/freshness-alerts', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFreshnessAlerts());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Merge Candidates (Batch 34) ===
app.get('/api/merge-candidates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getMergeCandidates(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/api/merge-preview/:id1/:id2', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getMergePreview(parseInt(req.params.id1), parseInt(req.params.id2)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/api/merge-execute', express.json(), (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { keepId, mergeId } = req.body;
    res.json(leadDb.executeMerge(keepId, mergeId));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Outreach Analytics (Batch 34) ===
app.get('/api/outreach-analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getOutreachAnalytics());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === ICP Scoring (Batch 34) ===
app.get('/api/icp-scoring', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getIcpScoring(parseInt(req.query.limit) || 40));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Pipeline Velocity (Batch 34) ===
app.get('/api/pipeline-velocity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPipelineVelocity());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Relationship Graph (Batch 35) ===
app.get('/api/relationship-graph', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRelationshipGraph(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Enrichment ROI (Batch 35) ===
app.get('/api/enrichment-roi', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEnrichmentROI());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Engagement Prediction (Batch 35) ===
app.get('/api/engagement-prediction', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEngagementPrediction(parseInt(req.query.limit) || 40));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Campaign Performance (Batch 35) ===
app.get('/api/campaign-performance', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCampaignPerformance());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Prioritization Matrix (Batch 36) ===
app.get('/api/prioritization-matrix', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPrioritizationMatrix(parseInt(req.query.limit) || 50));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Firm Aggregation (Batch 36) ===
app.get('/api/firm-aggregation', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFirmAggregation(parseInt(req.query.limit) || 30));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Improvement Recommendations (Batch 36) ===
app.get('/api/improvement-recs', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getImprovementRecs());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Lifecycle Funnel (Batch 36) ===
app.get('/api/lifecycle-funnel', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLifecycleFunnel());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Cadence Optimizer (Batch 37) ===
app.get('/api/cadence-optimizer', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getCadenceOptimizer());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Scoring Calibration (Batch 37) ===
app.get('/api/scoring-calibration', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScoringCalibration());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Practice Market Size (Batch 37) ===
app.get('/api/practice-market-size', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPracticeMarketSize());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Pipeline Health (Batch 37) ===
app.get('/api/pipeline-health', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getPipelineHealth());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Affinity Scoring (Batch 38) ===
app.get('/api/affinity-scoring', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAffinityScoring(parseInt(req.query.limit) || 40));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Scraper Gaps (Batch 38) ===
app.get('/api/scraper-gaps', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScraperGaps());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Freshness Index (Batch 38) ===
app.get('/api/freshness-index', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFreshnessIndex(parseInt(req.query.limit) || 40));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Firm Growth (Batch 38) ===
app.get('/api/firm-growth', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getFirmGrowth());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Revenue Attribution (Batch 39) ===
app.get('/api/revenue-attribution', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getRevenueAttribution());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Saturation Heatmap (Batch 39) ===
app.get('/api/saturation-heatmap', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSaturationHeatmap());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Smart List Builder (Batch 39) ===
app.get('/api/smart-list-builder', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getSmartListBuilder());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Quality Scorecard (Batch 39) ===
app.get('/api/quality-scorecard', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getQualityScorecard());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Dedup Intelligence (Batch 40) ===
app.get('/api/dedup-intelligence', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDedupIntelligence(parseInt(req.query.limit) || 25));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Outbound Readiness (Batch 40) ===
app.get('/api/outbound-readiness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getOutboundReadiness());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Aging Report (Batch 40) ===
app.get('/api/aging-report', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getAgingReport());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Growth Analytics (Batch 40) ===
app.get('/api/growth-analytics', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getGrowthAnalytics());
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// === Table Configuration ===
app.get('/api/table-config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getTableConfig() || { columns: null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/table-config', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.saveTableConfig(req.body));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/sources', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getDistinctSources());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/leads/tag', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds, tag, remove } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || !tag) {
      return res.status(400).json({ error: 'leadIds (array) and tag (string) required' });
    }
    res.json(leadDb.tagLeads(leadIds, tag, remove === true));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Delete Leads ---
app.post('/api/leads/delete', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) {
      return res.status(400).json({ error: 'leadIds (array) required' });
    }
    res.json(leadDb.deleteLeads(leadIds));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- CSV Import ---
app.post('/api/leads/import', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const leadDb = require('./lib/lead-db');
    const csvParse = require('csv-parser');
    const fsImport = require('fs');

    const results = [];
    const stream = fsImport.createReadStream(req.file.path).pipe(csvParse());

    stream.on('data', (row) => results.push(row));
    stream.on('end', () => {
      // Clean up uploaded file
      try { fsImport.unlinkSync(req.file.path); } catch {}

      if (results.length === 0) {
        return res.json({ imported: 0, skipped: 0, error: 'Empty CSV file' });
      }

      // Auto-detect column mapping
      const headers = Object.keys(results[0]);
      const mapping = autoMapColumns(headers);

      // Transform and import
      let imported = 0, updated = 0, skipped = 0;
      const source = req.body.source || 'csv-import';

      for (const row of results) {
        const lead = {};
        for (const [field, csvCol] of Object.entries(mapping)) {
          if (csvCol && row[csvCol]) {
            lead[field] = row[csvCol].trim();
          }
        }

        // Handle full_name split
        if (mapping._full_name && row[mapping._full_name] && !lead.first_name && !lead.last_name) {
          const parts = row[mapping._full_name].trim().split(/\s+/);
          lead.first_name = parts[0] || '';
          lead.last_name = parts.slice(1).join(' ') || '';
        }

        // Must have at least a name
        if (!lead.first_name && !lead.last_name) {
          skipped++;
          continue;
        }

        lead.primary_source = source;
        lead.source = source;
        const result = leadDb.upsertLead(lead);
        if (result.isNew) imported++;
        else if (result.wasUpdated) updated++;
        else skipped++;
      }

      // Post-import enrichment
      if (imported > 0) {
        leadDb.shareFirmData();
        leadDb.deduceWebsitesFromEmail();
      }

      res.json({
        total: results.length,
        imported,
        updated,
        skipped,
        columns: headers,
        mapping,
      });
    });

    stream.on('error', (err) => {
      try { fsImport.unlinkSync(req.file.path); } catch {}
      res.status(400).json({ error: 'Failed to parse CSV: ' + err.message });
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

function autoMapColumns(headers) {
  const mapping = {};
  const lowerHeaders = headers.map(h => h.toLowerCase().replace(/[^a-z0-9]/g, '_'));

  const fieldMaps = {
    first_name: ['first_name', 'firstname', 'first', 'given_name', 'fname'],
    last_name: ['last_name', 'lastname', 'last', 'surname', 'family_name', 'lname'],
    email: ['email', 'email_address', 'e_mail', 'emailaddress'],
    phone: ['phone', 'phone_number', 'telephone', 'tel', 'mobile', 'cell', 'work_phone'],
    firm_name: ['firm_name', 'firm', 'company', 'company_name', 'organization', 'org', 'employer'],
    city: ['city', 'town', 'locality'],
    state: ['state', 'state_code', 'province', 'region'],
    country: ['country', 'country_code'],
    website: ['website', 'url', 'web', 'site', 'homepage', 'website_url'],
    bar_number: ['bar_number', 'bar_num', 'license_number', 'license', 'bar_id'],
    bar_status: ['bar_status', 'status', 'license_status'],
    practice_area: ['practice_area', 'practice', 'specialty', 'specialization', 'area_of_practice'],
    title: ['title', 'job_title', 'position'],
    linkedin_url: ['linkedin_url', 'linkedin', 'linkedin_profile'],
  };

  for (const [field, aliases] of Object.entries(fieldMaps)) {
    for (let i = 0; i < lowerHeaders.length; i++) {
      if (aliases.includes(lowerHeaders[i])) {
        mapping[field] = headers[i];
        break;
      }
    }
  }

  // Handle "name" or "full_name" → split into first/last
  if (!mapping.first_name && !mapping.last_name) {
    const nameIdx = lowerHeaders.findIndex(h => h === 'name' || h === 'full_name' || h === 'fullname');
    if (nameIdx >= 0) {
      mapping._full_name = headers[nameIdx];
    }
  }

  return mapping;
}

// === Lead Lists / Campaigns ===
app.get('/api/lists', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getLists());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/lists', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { name, description, color } = req.body;
    if (!name || !name.trim()) return res.status(400).json({ error: 'Name required' });
    const list = leadDb.createList(name.trim(), description || '', color || '#6366f1');
    res.json(list);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/lists/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const list = leadDb.getList(parseInt(req.params.id));
    if (!list) return res.status(404).json({ error: 'List not found' });
    res.json(list);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/lists/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.updateList(parseInt(req.params.id), req.body);
    res.json({ updated: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/lists/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    leadDb.deleteList(parseInt(req.params.id));
    res.json({ deleted: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/lists/:id/add', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    const result = leadDb.addToList(parseInt(req.params.id), leadIds);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/lists/:id/remove', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
    const result = leadDb.removeFromList(parseInt(req.params.id), leadIds);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/lists/:id/export', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { createObjectCsvWriter } = require('csv-writer');
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
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// === Scraper Health ===
app.get('/api/scrapers/health', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getScraperHealth());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Enrichment Stats ---
app.get('/api/leads/enrichment-stats', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    res.json(leadDb.getEnrichmentStats());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Activity Feed ---
app.get('/api/activity', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    res.json(leadDb.getActivityFeed(limit));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Growth/Trend Data for Charts ---
app.get('/api/leads/growth', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const days = parseInt(req.query.days) || 30;
    const growth = leadDb.getDailyGrowth(days);
    res.json(growth);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Field Completeness for Data Quality Charts ---
app.get('/api/leads/completeness', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const data = leadDb.getFieldCompleteness();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Find potential duplicates ---
app.get('/api/leads/duplicates', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const limit = parseInt(req.query.limit) || 50;
    const dupes = leadDb.findPotentialDuplicates(limit);
    res.json(dupes);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Merge a pair of duplicate leads ---
app.post('/api/leads/merge', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { keepId, deleteId } = req.body;
    if (!keepId || !deleteId) return res.status(400).json({ error: 'keepId and deleteId required' });
    const result = leadDb.mergeLeadPair(keepId, deleteId);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Get Lead Detail ---
app.get('/api/leads/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const lead = leadDb.getLeadById(parseInt(req.params.id));
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    res.json(lead);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Update a single lead (inline editing) ---
app.patch('/api/leads/:id', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const id = parseInt(req.params.id);
    const lead = leadDb.getLeadById(id);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    const result = leadDb.updateLead(id, req.body);
    if (result.updated) {
      leadDb.computeLeadScore(id);
      const updated = leadDb.getLeadById(id);
      res.json(updated);
    } else {
      res.json({ message: 'No changes' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Quick Scrape (single state from coverage table) ---
app.post('/api/scrape/quick', (req, res) => {
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

    // Save to master DB
    if (job.leads.length > 0) {
      try {
        const leadDb = require('./lib/lead-db');
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

  res.json({ jobId, state });
});

app.get('/api/scrape/quick/:jobId', (req, res) => {
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

// --- Export leads for a specific state (per-state quick export) ---
app.get('/api/leads/export/state/:state', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { createObjectCsvWriter } = require('csv-writer');
    const leads = leadDb.exportLeads({ state: req.params.state });
    if (leads.length === 0) return res.status(404).json({ error: 'No leads for this state' });

    const tmpPath = path.join(OUTPUT_DIR, `${req.params.state}-export-${Date.now()}.csv`);
    const headers = Object.keys(leads[0]).filter(k => k !== 'id' && k !== 'lead_score')
      .map(k => ({ id: k, title: k }));
    const writer = createObjectCsvWriter({ path: tmpPath, header: headers });
    writer.writeRecords(leads).then(() => {
      res.download(tmpPath, `${req.params.state}-leads.csv`, () => {
        try { fs.unlinkSync(tmpPath); } catch {}
      });
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Debug Endpoints (dev only) ---

if (process.env.NODE_ENV !== 'production') {
  app.get('/api/debug/logs', (req, res) => {
    const lines = parseInt(req.query.lines) || 100;
    res.json(readLogTail(lines));
  });

  app.get('/api/debug/metrics', (req, res) => {
    res.json(metrics.getSummary());
  });

  app.get('/api/debug/jobs', (req, res) => {
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

// --- Express error middleware ---

app.use((err, req, res, _next) => {
  console.error('[express] Unhandled error:', err.stack || err.message);
  res.status(500).json({ error: 'Internal server error' });
});

// --- HTTP Server + WebSocket ---

const server = app.listen(PORT, () => {
  console.log(`\n  🧱 Mortar Lead Scraper`);
  console.log(`  ──────────────────────`);
  console.log(`  UI:  http://localhost:${PORT}`);
  console.log(`  API: http://localhost:${PORT}/api/config`);
  console.log(`  WS:  ws://localhost:${PORT}/ws\n`);

  // Signal Engine — scan Indeed every 6 hours
  if (process.env.NODE_ENV === 'production' || process.env.ENABLE_CRONS) {
    const cron = require('node-cron');
    cron.schedule('0 */6 * * *', () => {
      require('./watchers/job-boards').run()
        .then(count => console.log(`[Cron] Signal scan found ${count} new signals`))
        .catch(err => console.error(`[Cron] Signal scan failed: ${err.message}`));
    });
    console.log('  📡 Signal Engine: job board scan scheduled every 6 hours');

    // Bulk Scraper — run all scrapers daily at 2 AM
    cron.schedule('0 2 * * *', () => {
      const BulkScraper = require('./lib/bulk-scraper');
      const bulk = new BulkScraper();
      bulk.run({ test: false, emailScrape: false })
        .then(results => console.log(`[Cron] Bulk scrape: ${results.totalLeads} leads, ${results.totalNew} new`))
        .catch(err => console.error(`[Cron] Bulk scrape failed: ${err.message}`));
    });
    console.log('  📊 Bulk Scraper: daily scrape scheduled at 2:00 AM\n');
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });

// Track which job each WS client is subscribed to
const wsClients = new Map(); // ws → jobId

wss.on('connection', (ws) => {
  console.log(`[ws] Client connected (${wsClients.size + 1} total)`);

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'subscribe' && msg.jobId) {
        wsClients.set(ws, msg.jobId);
        console.log(`[ws] Client subscribed to ${msg.jobId}`);
      } else if (msg.type === 'subscribe-all') {
        wsClients.set(ws, '__all__');
        console.log(`[ws] Client subscribed to all events`);
      }
    } catch (err) {
      console.error('[ws] Failed to parse message:', raw.toString().substring(0, 200), err.message);
    }
  });

  ws.on('close', () => {
    const jobId = wsClients.get(ws);
    wsClients.delete(ws);
    console.log(`[ws] Client disconnected${jobId ? ` (was on ${jobId})` : ''} (${wsClients.size} remaining)`);
  });
});

// --- Global Error Handlers ---

process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught exception:', err.stack || err.message);
  try { require('./lib/logger').log.error(`Uncaught exception: ${err.message}`); } catch {}
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.stack : String(reason);
  console.error('[WARN] Unhandled rejection:', msg);
  try { require('./lib/logger').log.error(`Unhandled rejection: ${msg}`); } catch {}
});

function broadcast(jobId, data) {
  const payload = JSON.stringify(data);
  for (const [ws, subscribedJob] of wsClients.entries()) {
    if (subscribedJob === jobId && ws.readyState === 1) {
      ws.send(payload);
    }
  }
}

// Broadcast to ALL connected WS clients (for global notifications like DB updates)
function broadcastAll(data) {
  const payload = JSON.stringify(data);
  for (const ws of wss.clients) {
    if (ws.readyState === 1) {
      ws.send(payload);
    }
  }
}

// --- Webhook Firing ---
function fireWebhook(url, event, payload) {
  const data = JSON.stringify({ event, timestamp: new Date().toISOString(), data: payload });
  const parsed = new URL(url);
  const options = {
    hostname: parsed.hostname, port: parsed.port, path: parsed.pathname + parsed.search,
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data), 'User-Agent': 'Mortar-Webhook/1.0' },
    timeout: 10000,
  };
  const proto = parsed.protocol === 'https:' ? require('https') : require('http');
  const req = proto.request(options, (res) => {
    let body = '';
    res.on('data', d => body += d);
    res.on('end', () => {
      try {
        const leadDb = require('./lib/lead-db');
        // Find webhook by URL for logging
        const hooks = leadDb.getWebhooks().filter(w => w.url === url);
        for (const h of hooks) {
          leadDb.logWebhookDelivery(h.id, event, res.statusCode, body.slice(0, 500), res.statusCode >= 200 && res.statusCode < 300);
        }
      } catch (err) { /* ignore logging errors */ }
    });
  });
  req.on('error', (err) => {
    console.error(`[webhook] Error delivering to ${url}:`, err.message);
  });
  req.write(data);
  req.end();
}

function fireWebhookEvent(event, payload) {
  try {
    const leadDb = require('./lib/lead-db');
    const hooks = leadDb.getWebhooksByEvent(event);
    for (const hook of hooks) {
      fireWebhook(hook.url, event, payload);
    }
  } catch (err) { /* ignore */ }
}

// --- Schedule Runner (checks every 5 minutes for due scrapes) ---
setInterval(() => {
  try {
    const leadDb = require('./lib/lead-db');
    const due = leadDb.getDueSchedules();
    for (const sched of due) {
      if (jobs.has('schedule-' + sched.id)) continue; // already running
      console.log(`[schedule] Running scheduled scrape for ${sched.state}`);
      const jobId = 'schedule-' + sched.id + '-' + Date.now();
      const job = {
        id: jobId, state: sched.state, practiceArea: sched.practice_area || '',
        testMode: false, status: 'running', stats: { scraped: 0, dupes: 0, new: 0, emails: 0 },
        startTime: Date.now(), leads: [],
      };
      jobs.set(jobId, job);
      runPipeline({
        state: sched.state, practiceArea: sched.practice_area || '',
        testMode: false, emailScrape: false, enrich: false,
        waterfall: { fetchProfiles: true, crossrefMartindale: false, crossrefLawyersCom: false, nameLookups: false, emailCrawl: false },
        dedup: { enabled: true, masterDb: true },
        onProgress: (data) => {
          if (data.stats) Object.assign(job.stats, data.stats);
          if (data.type === 'complete') {
            job.status = 'complete';
            leadDb.markScheduleRun(sched.id);
            broadcastAll({ type: 'db-update', event: 'schedule-complete', state: sched.state });
            setTimeout(() => jobs.delete(jobId), 300000);
          }
        },
      }).catch(err => {
        console.error(`[schedule] Error scraping ${sched.state}:`, err.message);
        job.status = 'error';
        setTimeout(() => jobs.delete(jobId), 300000);
      });
    }
  } catch (err) {
    console.error('[schedule] Error checking schedules:', err.message);
  }
}, 5 * 60 * 1000); // Check every 5 minutes
