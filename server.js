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
  const { state, practice, city, test, uploadId, enrich, enrichOptions, waterfall } = req.body;

  if (!state) {
    return res.status(400).json({ error: 'State is required' });
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
        console.log(`[job:${jobId}] Saved to master DB: ${dbStats.inserted} new, ${dbStats.updated} updated, ${dbStats.unchanged} unchanged`);
      } catch (err) {
        console.error(`[job:${jobId}] Failed to save to master DB:`, err.message);
      }
    }

    broadcast(jobId, { type: 'complete', stats: data.stats, jobId });
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

// Search leads
app.get('/api/leads', (req, res) => {
  try {
    const leadDb = require('./lib/lead-db');
    const { q, state, country, hasEmail, hasPhone, limit, offset } = req.query;
    const leads = leadDb.searchLeads(q, {
      state, country,
      hasEmail: hasEmail === 'true',
      hasPhone: hasPhone === 'true',
      limit: Math.min(parseInt(limit) || 100, 1000),
      offset: parseInt(offset) || 0,
    });
    res.json({ leads, count: leads.length });
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
