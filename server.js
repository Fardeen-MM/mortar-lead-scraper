#!/usr/bin/env node
/**
 * Mortar Lead Scraper — Web Server
 *
 * Express + WebSocket server that provides a browser-based wizard UI
 * for the scrape pipeline. Serves static files from public/, handles
 * CSV uploads, starts scrape jobs, and streams live progress via WS.
 *
 * Route handlers are split into modules under routes/:
 *   scraping.js        — Scrape job management, signals, health
 *   leads-core.js      — Core lead CRUD, search, export, import, lists
 *   enrichment.js      — Waterfall, enrichment queue, firm intelligence
 *   scoring.js         — Scoring rules, ICP, activity scores, decay, models
 *   outreach.js        — Pipeline, sequences, campaigns, territories, nurture
 *   email-quality.js   — Email verification, classification, compliance, DNC
 *   tags-automation.js — Webhooks, notes, tags, custom fields, automation, audit
 *   data-quality.js    — Quality checks, freshness, changelog, export history
 *   analytics.js       — Dashboard analytics (batches 27-40+)
 *   ai.js              — AI intelligence endpoints
 *   infrastructure.js  — Table config, scraper health, debug, backfill
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
      const label = status >= 400 ? '\u2717' : '\u2713';
      console.log(`  ${label} ${req.method} ${req.path} \u2192 ${status} (${ms}ms)`);
    });
  }
  next();
});

// Active jobs store
const jobs = new Map();

// ============================================================
// === ROUTE MODULES ===
// ============================================================
//
// IMPORTANT: leads-core.js MUST be mounted LAST because it contains
// a wildcard route GET /leads/:id that would catch specific /leads/*
// routes from other modules (data-quality, scoring, enrichment, etc.)
// if mounted before them.

// --- Scraping (factory — needs jobs, broadcast, upload, runPipeline) ---
const scrapingRouter = require('./routes/scraping')({
  jobs, broadcast, broadcastAll, upload, runPipeline, fireWebhookEvent,
});
app.use('/api', scrapingRouter);

// --- Enrichment (factory — needs broadcastAll) ---
const enrichmentRouter = require('./routes/enrichment')({ broadcastAll });
app.use('/api', enrichmentRouter);

// --- Scoring (plain router) ---
app.use('/api', require('./routes/scoring'));

// --- Outreach (plain router) ---
app.use('/api', require('./routes/outreach'));

// --- Email Quality (factory — needs broadcastAll, upload) ---
app.use('/api', require('./routes/email-quality')({ broadcastAll, upload }));

// --- Tags & Automation (factory — needs fireWebhook, fireWebhookEvent) ---
app.use('/api', require('./routes/tags-automation')({ fireWebhook, fireWebhookEvent }));

// --- Data Quality (plain router) ---
app.use('/api', require('./routes/data-quality'));

// --- Analytics (plain router) ---
app.use('/api', require('./routes/analytics'));

// --- AI Intelligence (plain router) ---
app.use('/api', require('./routes/ai'));

// --- Infrastructure (factory — needs jobs, metrics, readLogTail) ---
app.use('/api', require('./routes/infrastructure')({ jobs, metrics, readLogTail }));

// --- Leads Core (factory — needs upload, OUTPUT_DIR) ---
// MUST be last: contains GET /leads/:id wildcard that would shadow specific routes
const leadsCoreRouter = require('./routes/leads-core')({ upload, OUTPUT_DIR });
app.use('/api', leadsCoreRouter);

// ============================================================
// === ERROR MIDDLEWARE ===
// ============================================================

app.use((err, req, res, _next) => {
  console.error('[express] Unhandled error:', err.stack || err.message);
  res.status(500).json({ error: 'Internal server error' });
});

// ============================================================
// === HTTP SERVER + WEBSOCKET ===
// ============================================================

const server = app.listen(PORT, () => {
  console.log(`\n  \uD83E\uDDF1 Mortar Lead Scraper`);
  console.log(`  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500`);
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
    console.log('  \uD83D\uDCE1 Signal Engine: job board scan scheduled every 6 hours');

    // Bulk Scraper — run all scrapers daily at 2 AM
    cron.schedule('0 2 * * *', () => {
      const BulkScraper = require('./lib/bulk-scraper');
      const bulk = new BulkScraper();
      bulk.run({ test: false, emailScrape: false })
        .then(results => console.log(`[Cron] Bulk scrape: ${results.totalLeads} leads, ${results.totalNew} new`))
        .catch(err => console.error(`[Cron] Bulk scrape failed: ${err.message}`));
    });
    console.log('  \uD83D\uDCCA Bulk Scraper: daily scrape scheduled at 2:00 AM');

    // Auto-Enrichment — waterfall enrich unenriched DB leads every 12 hours
    cron.schedule('0 8,20 * * *', async () => {
      // Access waterfall state from the enrichment router
      const waterfallState = enrichmentRouter._getWaterfallState();
      if (waterfallState.running) {
        console.log('[Cron] Skipping auto-enrich — waterfall already running');
        return;
      }
      console.log('[Cron] Starting auto-enrichment waterfall...');
      enrichmentRouter._setWaterfallRunning(true);
      enrichmentRouter._setWaterfallProgress({ step: 'Auto-enrichment starting...', current: 0, total: 0, stats: null });

      try {
        const leadDb = require('./lib/lead-db');
        const { runWaterfall } = require('./lib/waterfall');
        const db = leadDb.getDb();

        // Get 200 leads with profile_url that are missing phone — prioritize never-enriched
        const dbLeads = db.prepare(`
          SELECT * FROM leads WHERE profile_url != '' AND profile_url IS NOT NULL
          AND (phone = '' OR phone IS NULL)
          ORDER BY last_enriched_at IS NOT NULL, lead_score ASC LIMIT 200
        `).all();

        if (dbLeads.length === 0) {
          console.log('[Cron] No leads need enrichment');
          enrichmentRouter._setWaterfallRunning(false);
          return;
        }

        enrichmentRouter._setWaterfallProgress({ step: 'Auto-enrichment starting...', current: 0, total: dbLeads.length, stats: null });
        const EventEmitter = require('events');
        const emitter = new EventEmitter();
        emitter.on('waterfall-progress', (p) => {
          enrichmentRouter._setWaterfallProgress({
            step: p.step,
            current: p.current,
            total: p.total,
            detail: p.detail,
          });
        });

        const leads = dbLeads.map(row => ({ ...row, source: row.primary_source || '' }));
        const stats = await runWaterfall(leads, {
          fetchProfiles: true,
          crossRefMartindale: false, // skip slow cross-ref for auto-enrich
          crossRefLawyersCom: false,
          nameLookups: false,
          emailCrawl: false, // no Puppeteer in cron
          smtpPatterns: true, // fast email discovery via SMTP (zero deps)
          masterDbLookup: false,
          emitter,
          isCancelled: () => !enrichmentRouter._getWaterfallState().running,
          maxProfileFetches: 200,
        });

        // Save enriched data back
        const origMap = new Map(dbLeads.map(d => [d.id, d]));
        let saved = 0;
        for (const lead of leads) {
          const orig = origMap.get(lead.id) || {};
          const updates = {};
          for (const f of ['email', 'phone', 'website', 'bar_number', 'admission_date', 'firm_name', 'title', 'education', 'city', 'state']) {
            if (lead[f] && !orig[f]) updates[f] = lead[f];
          }
          for (const f of ['email_source', 'phone_source', 'website_source']) {
            if (lead[f]) updates[f] = lead[f];
          }
          if (lead._enrichmentError) {
            updates.last_enrichment_error = lead._enrichmentError;
            updates.enrichment_attempts = (orig.enrichment_attempts || 0) + 1;
          }
          if (Object.keys(updates).length > 0) {
            updates.last_enriched_at = new Date().toISOString();
            updates.enrichment_steps = 'profile,smtp';
            try { leadDb.updateLead(lead.id, updates); saved++; } catch (err) { console.error(`[Cron] updateLead failed for lead ${lead.id}:`, err.message); }
          } else if (lead._enrichmentError) {
            try {
              leadDb.updateLead(lead.id, {
                last_enrichment_error: lead._enrichmentError,
                enrichment_attempts: (orig.enrichment_attempts || 0) + 1,
                last_enriched_at: new Date().toISOString(),
              });
            } catch (err) { console.error(`[Cron] updateLead enrichment-error failed for lead ${lead.id}:`, err.message); }
          }
        }
        leadDb.batchScoreLeads();

        // Record the run
        try {
          leadDb.recordWaterfallRun({
            stateFilter: 'all',
            leadsProcessed: dbLeads.length,
            leadsEnriched: saved,
            fieldsFilled: stats.totalFieldsFilled,
            profilesFetched: stats.profilesFetched || 0,
            profilesAttempted: stats.profilesAttempted || 0,
            profilesFailed: stats.profilesFailed || 0,
            websitesFound: stats.websitesFound || 0,
            emailsFound: stats.emailsCrawled || 0,
            trigger: 'auto-cron',
            stats: { ...stats, profileErrors: (stats.profileErrors || []).slice(0, 50) },
            durationSecs: 0,
          });
        } catch (err) { console.error('[Cron] recordWaterfallRun failed:', err.message); }

        console.log(`[Cron] Auto-enrich done: ${stats.totalFieldsFilled} fields filled, ${saved} leads saved`);
      } catch (err) {
        console.error(`[Cron] Auto-enrich failed: ${err.message}`);
      } finally {
        enrichmentRouter._setWaterfallRunning(false);
      }
    });
    console.log('  \uD83D\uDD04 Auto-Enrichment: waterfall scheduled at 8 AM and 8 PM\n');
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

  ws.on('error', (err) => {
    console.error('[ws] Connection error:', err.message);
    wsClients.delete(ws);
  });
});

// ============================================================
// === GLOBAL ERROR HANDLERS ===
// ============================================================

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

// ============================================================
// === BROADCAST & WEBHOOK UTILITIES ===
// ============================================================

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
  req.on('timeout', () => {
    req.destroy(new Error('Webhook request timed out'));
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

// ============================================================
// === SCHEDULE RUNNER ===
// ============================================================

// Check every 5 minutes for due scrapes
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
