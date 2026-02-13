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
const { runPipeline, SCRAPERS } = require('./lib/pipeline');

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
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Only CSV files are allowed'));
    }
  },
});

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Active jobs store
const jobs = new Map();

// --- REST Endpoints ---

// Get available scrapers and their practice areas
app.get('/api/config', (req, res) => {
  const florida = require('./scrapers/bars/florida');
  res.json({
    states: Object.keys(SCRAPERS),
    practiceAreas: Object.keys(florida.PRACTICE_AREA_CODES),
    floridaCities: [
      'Miami', 'Fort Lauderdale', 'West Palm Beach', 'Orlando',
      'Tampa', 'Jacksonville', 'St. Petersburg', 'Naples',
      'Boca Raton', 'Tallahassee', 'Gainesville', 'Sarasota',
      'Fort Myers', 'Daytona Beach', 'Pensacola', 'Coral Gables',
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

    res.json({
      uploadId,
      count: leads.length,
      originalName: req.file.originalname,
    });
  } catch (err) {
    res.status(400).json({ error: `Failed to parse CSV: ${err.message}` });
  }
});

// Start a scrape job
app.post('/api/scrape/start', (req, res) => {
  const { state, practice, city, test, uploadId } = req.body;

  if (!state) {
    return res.status(400).json({ error: 'State is required' });
  }

  const jobId = `job-${Date.now()}`;

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
  });

  // Store job
  const job = {
    id: jobId,
    state,
    practice,
    city,
    test,
    status: 'running',
    emitter,
    leads: [],
    stats: null,
    outputFile: null,
  };
  jobs.set(jobId, job);

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

  emitter.on('complete', (data) => {
    job.status = 'complete';
    job.stats = data.stats;
    job.outputFile = data.outputFile;
    broadcast(jobId, { type: 'complete', stats: data.stats, jobId });
  });

  emitter.on('error', (data) => {
    job.status = 'error';
    broadcast(jobId, { type: 'error', message: data.message });
  });

  res.json({ jobId });
});

// Download CSV for a completed job
app.get('/api/scrape/:id/download', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  if (!job.outputFile) {
    return res.status(400).json({ error: 'No output file available' });
  }
  if (!fs.existsSync(job.outputFile)) {
    return res.status(404).json({ error: 'Output file not found on disk' });
  }

  const filename = path.basename(job.outputFile);
  res.download(job.outputFile, filename);
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
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'subscribe' && msg.jobId) {
        wsClients.set(ws, msg.jobId);
      }
    } catch {}
  });

  ws.on('close', () => {
    wsClients.delete(ws);
  });
});

function broadcast(jobId, data) {
  const payload = JSON.stringify(data);
  for (const [ws, subscribedJob] of wsClients.entries()) {
    if (subscribedJob === jobId && ws.readyState === 1) {
      ws.send(payload);
    }
  }
}
