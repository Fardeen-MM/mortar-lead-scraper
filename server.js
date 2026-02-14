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
  const { state, practice, city, test, uploadId, enrich, enrichOptions } = req.body;

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
    emailScrape: false, // Bar scraping doesn't typically need Puppeteer email scraping
    existingLeads,
    enrich: !!enrich,
    enrichOptions: enrichOptions || {},
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
