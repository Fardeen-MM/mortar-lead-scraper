/**
 * Enrichment Routes — Waterfall, enrichment queue, firm intelligence,
 * website finder, prepopulate, enrich-all, batch enrichment
 */

const router = require('express').Router();

module.exports = function ({ broadcastAll }) {

  // --- Website Finder ---
  let _websiteFinderRunning = false;
  let _websiteFinderProgress = null;

  router.post('/leads/find-websites', (req, res) => {
    try {
      if (_websiteFinderRunning) {
        return res.json({ status: 'already_running', progress: _websiteFinderProgress });
      }

      _websiteFinderRunning = true;
      _websiteFinderProgress = { current: 0, total: 0, found: 0 };
      res.json({ status: 'started', message: 'Website finder started in background' });

      const { batchFindWebsites } = require('../lib/website-finder');
      const leadDb = require('../lib/lead-db');

      (async () => {
        const db = leadDb.getDb();
        const leadsNeedingWebsite = db.prepare(`
          SELECT * FROM leads
          WHERE (website = '' OR website IS NULL)
            AND firm_name != '' AND firm_name IS NOT NULL
          ORDER BY updated_at DESC
          LIMIT ?
        `).all(Math.min(Math.max(parseInt(req.body.limit) || 500, 1), 5000));

        _websiteFinderProgress.total = leadsNeedingWebsite.length;

        const stats = await batchFindWebsites(leadsNeedingWebsite, {
          onProgress: (current, total) => {
            _websiteFinderProgress.current = current;
            _websiteFinderProgress.total = total;
          },
          googleSearch: req.body.googleSearch !== false,
        });

        _websiteFinderProgress.found = stats.found;

        for (const lead of leadsNeedingWebsite) {
          if (lead.website) {
            leadDb.upsertLead(lead);
          }
        }

        console.log(`[Website Finder] Done — found ${stats.found} websites out of ${leadsNeedingWebsite.length}`);
      })()
        .catch(err => console.error('[Website Finder] Error:', err.message))
        .finally(() => { _websiteFinderRunning = false; });
    } catch (err) {
      console.error('[leads/find-websites] Error:', err.message, err.stack);
      _websiteFinderRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/find-websites/status', (req, res) => {
    try {
      res.json({ running: _websiteFinderRunning, progress: _websiteFinderProgress });
    } catch (err) {
      console.error('[leads/find-websites/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Pre-populate Master DB ---
  let _prepopRunning = false;
  let _prepopProgress = null;

  router.post('/leads/prepopulate', (req, res) => {
    try {
      if (_prepopRunning) {
        return res.json({ status: 'already_running', progress: _prepopProgress });
      }

      _prepopRunning = true;
      _prepopProgress = { current: 0, total: 0, totalLeads: 0, totalNew: 0, currentSource: '', results: [] };
      res.json({ status: 'started', message: 'Pre-population started in background' });

      const leadDb = require('../lib/lead-db');
      const { runPipeline } = require('../lib/pipeline');
      let sources = req.body.sources || ['AVVO', 'FINDLAW'];
      if (!Array.isArray(sources)) sources = ['AVVO', 'FINDLAW'];
      sources = sources.slice(0, 20);
      const test = req.body.test !== false;

      const scraperQueue = [];
      for (const source of sources) {
        if (typeof source === 'string' && source.length <= 30) {
          scraperQueue.push({ state: source, test });
        }
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
                  masterDbLookup: false, fetchProfiles: false,
                  crossRefMartindale: false, crossRefLawyersCom: false,
                  nameLookups: false, emailCrawl: false,
                },
              });

              let leads = [];
              emitter.on('lead', d => leads.push(d.data));

              emitter.on('complete', (data) => {
                const time = Math.round((Date.now() - startTime) / 1000);
                let dbStats = { inserted: 0, updated: 0 };
                if (leads.length > 0) {
                  try { dbStats = leadDb.batchUpsert(leads, `prepop:${state}`); } catch (err) { console.error(`[Prepop] batchUpsert failed for ${state}:`, err.message); }
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
    } catch (err) {
      console.error('[leads/prepopulate] Error:', err.message, err.stack);
      _prepopRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/prepopulate/status', (req, res) => {
    try {
      res.json({ running: _prepopRunning, progress: _prepopProgress });
    } catch (err) {
      console.error('[leads/prepopulate/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Share firm data ---
  router.post('/leads/share-firm-data', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const result = leadDb.shareFirmData();
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Deduce websites ---
  router.post('/leads/deduce-websites', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const result = leadDb.deduceWebsitesFromEmail();
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrich All ---
  let _enrichAllRunning = false;
  let _enrichAllProgress = null;

  router.post('/leads/enrich-all', (req, res) => {
    try {
      if (_enrichAllRunning) {
        return res.json({ status: 'already_running', progress: _enrichAllProgress });
      }

      _enrichAllRunning = true;
      _enrichAllProgress = { step: '', steps: [], totalUpdated: 0 };
      res.json({ status: 'started', message: 'Enrich All started in background' });

      const leadDb = require('../lib/lead-db');

      (async () => {
        _enrichAllProgress.step = 'Merging duplicates...';
        const mergeResult = leadDb.mergeDuplicates({});
        _enrichAllProgress.steps.push({ name: 'Merge Dupes', result: `${mergeResult.merged} merged, ${mergeResult.fieldsRecovered} fields recovered` });
        _enrichAllProgress.totalUpdated += mergeResult.merged;

        _enrichAllProgress.step = 'Sharing firm data...';
        const firmResult = leadDb.shareFirmData();
        _enrichAllProgress.steps.push({ name: 'Firm Share', result: `${firmResult.leadsUpdated} leads updated across ${firmResult.firmsProcessed} firms` });
        _enrichAllProgress.totalUpdated += firmResult.leadsUpdated;

        _enrichAllProgress.step = 'Deducing websites...';
        const deduceResult = leadDb.deduceWebsitesFromEmail();
        _enrichAllProgress.steps.push({ name: 'Website Deduction', result: `${deduceResult.leadsUpdated} websites from email domains` });
        _enrichAllProgress.totalUpdated += deduceResult.leadsUpdated;

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
    } catch (err) {
      console.error('[leads/enrich-all] Error:', err.message, err.stack);
      _enrichAllRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/enrich-all/status', (req, res) => {
    try {
      res.json({ running: _enrichAllRunning, progress: _enrichAllProgress });
    } catch (err) {
      console.error('[leads/enrich-all/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  // --- Batch Waterfall Enrichment ---
  let _waterfallRunning = false;
  let _waterfallProgress = null;
  let _waterfallStartTime = null;

  // Expose waterfall state for cron access
  router._getWaterfallState = () => ({ running: _waterfallRunning, progress: _waterfallProgress });
  router._setWaterfallRunning = (val) => { _waterfallRunning = val; };
  router._setWaterfallProgress = (val) => { _waterfallProgress = val; };

  router.post('/leads/waterfall', (req, res) => {
    try {
      if (_waterfallRunning) {
        return res.json({ status: 'already_running', progress: _waterfallProgress });
      }

      _waterfallRunning = true;
      _waterfallStartTime = Date.now();
      _waterfallProgress = { step: 'Loading leads...', current: 0, total: 0, stats: null };
      res.json({ status: 'started', message: 'Waterfall enrichment started in background' });

      const leadDb = require('../lib/lead-db');
      const { runWaterfall } = require('../lib/waterfall');

      const limit = Math.min(Math.max(parseInt(req.body.limit) || 200, 1), 2000);
      const state = req.body.state || null;
      const steps = {
        fetchProfiles: req.body.fetchProfiles !== false,
        crossRefMartindale: req.body.crossRefMartindale !== false,
        crossRefLawyersCom: req.body.crossRefLawyersCom !== false,
        nameLookups: req.body.nameLookups !== false,
        emailCrawl: req.body.emailCrawl === true,
        smtpPatterns: req.body.smtpPatterns !== false,
        masterDbLookup: false,
      };

      (async () => {
        const db = leadDb.getDb();
        let query = `SELECT * FROM leads WHERE profile_url != '' AND profile_url IS NOT NULL
          AND (email = '' OR email IS NULL OR phone = '' OR phone IS NULL OR website = '' OR website IS NULL)`;
        const params = [];
        if (state) {
          query += ` AND state = ?`;
          params.push(state);
        }
        query += ` ORDER BY lead_score ASC LIMIT ?`;
        params.push(limit);

        const dbLeads = db.prepare(query).all(...params);
        _waterfallProgress.total = dbLeads.length;
        _waterfallProgress.step = `Enriching ${dbLeads.length} leads...`;

        if (dbLeads.length === 0) {
          _waterfallProgress.step = 'No leads need enrichment';
          _waterfallRunning = false;
          return;
        }

        const leads = dbLeads.map(row => ({
          ...row,
          source: row.primary_source || '',
        }));

        const EventEmitter = require('events');
        const emitter = new EventEmitter();
        emitter.on('waterfall-progress', (progress) => {
          _waterfallProgress.step = progress.step;
          _waterfallProgress.current = progress.current;
          _waterfallProgress.total = progress.total;
          _waterfallProgress.detail = progress.detail;
        });

        const stats = await runWaterfall(leads, {
          ...steps,
          emitter,
          isCancelled: () => !_waterfallRunning,
          maxProfileFetches: limit,
        });

        _waterfallProgress.stats = stats;
        _waterfallProgress.step = 'Saving to database...';

        let saved = 0;
        const origMap = new Map(dbLeads.map(d => [d.id, d]));
        for (const lead of leads) {
          const orig = origMap.get(lead.id) || {};
          const updates = {};
          const fillable = ['email', 'phone', 'website', 'bar_number', 'admission_date',
            'firm_name', 'title', 'education', 'city', 'state'];
          for (const f of fillable) {
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
            const newSteps = [];
            if (steps.fetchProfiles) newSteps.push('profile');
            if (steps.crossRefMartindale) newSteps.push('martindale');
            if (steps.crossRefLawyersCom) newSteps.push('lawyerscom');
            if (steps.nameLookups) newSteps.push('name-lookup');
            if (steps.emailCrawl) newSteps.push('email-crawl');
            if (steps.smtpPatterns) newSteps.push('smtp');
            const existingSteps = (orig.enrichment_steps || '').split(',').filter(Boolean);
            const allSteps = [...new Set([...existingSteps, ...newSteps])];
            updates.enrichment_steps = allSteps.join(',');
            try {
              leadDb.updateLead(lead.id, updates);
              saved++;
            } catch (err) {
              console.error(`[Waterfall] Failed to save lead ${lead.id}:`, err.message);
            }
          } else if (lead._enrichmentError) {
            try {
              leadDb.updateLead(lead.id, {
                last_enrichment_error: lead._enrichmentError,
                enrichment_attempts: (orig.enrichment_attempts || 0) + 1,
                last_enriched_at: new Date().toISOString(),
              });
            } catch (err) {
              console.error(`[Waterfall] Failed to save error for lead ${lead.id}:`, err.message);
            }
          }
        }

        leadDb.batchScoreLeads();

        _waterfallProgress.step = `Done — ${stats.totalFieldsFilled} fields filled, ${saved} leads updated`;
        _waterfallProgress.saved = saved;
        console.log(`[Waterfall] Done: ${stats.totalFieldsFilled} fields filled, ${saved} leads saved to DB`);

        try {
          leadDb.recordWaterfallRun({
            stateFilter: state || 'all',
            leadsProcessed: dbLeads.length,
            leadsEnriched: saved,
            fieldsFilled: stats.totalFieldsFilled,
            profilesFetched: stats.profilesFetched || 0,
            profilesAttempted: stats.profilesAttempted || 0,
            profilesFailed: stats.profilesFailed || 0,
            websitesFound: stats.websitesFound || 0,
            emailsFound: stats.emailsCrawled || 0,
            trigger: 'manual',
            stats: { ...stats, profileErrors: (stats.profileErrors || []).slice(0, 50) },
            durationSecs: Math.round((Date.now() - _waterfallStartTime) / 1000),
          });
        } catch (err) { console.error('[Waterfall] recordWaterfallRun failed:', err.message); }
      })()
        .catch(err => {
          _waterfallProgress.step = `Error: ${err.message}`;
          console.error('[Waterfall] Error:', err.message);
        })
        .finally(() => { _waterfallRunning = false; });
    } catch (err) {
      console.error('[leads/waterfall] Error:', err.message, err.stack);
      _waterfallRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/waterfall/status', (req, res) => {
    try {
      res.json({ running: _waterfallRunning, progress: _waterfallProgress });
    } catch (err) {
      console.error('[leads/waterfall/status] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  router.post('/leads/waterfall/cancel', (req, res) => {
    try {
      if (_waterfallRunning) {
        _waterfallRunning = false;
        res.json({ cancelled: true });
      } else {
        res.json({ cancelled: false, message: 'Not running' });
      }
    } catch (err) {
      console.error('[leads/waterfall/cancel] Error:', err.message, err.stack);
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/waterfall/history', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = Math.min(parseInt(req.query.limit) || 20, 100);
      res.json(leadDb.getWaterfallRuns(limit));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/waterfall/summary', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getWaterfallSummary());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Multi-state waterfall ---
  router.post('/leads/waterfall/multi', (req, res) => {
    if (_waterfallRunning) {
      return res.json({ status: 'already_running', progress: _waterfallProgress });
    }

    _waterfallRunning = true;
    _waterfallStartTime = Date.now();
    _waterfallProgress = { step: 'Loading enrichment priorities...', current: 0, total: 0, stats: null, multiState: true };
    res.json({ status: 'started', message: 'Multi-state waterfall enrichment started' });

    const leadDb = require('../lib/lead-db');
    const { runWaterfall } = require('../lib/waterfall');
    const limitPerState = Math.min(Math.max(parseInt(req.body.limitPerState) || 200, 1), 1000);
    const maxStates = Math.min(Math.max(parseInt(req.body.maxStates) || 10, 1), 30);

    (async () => {
      const db = leadDb.getDb();
      const states = db.prepare(`
        SELECT state,
          SUM(CASE WHEN profile_url IS NOT NULL AND profile_url != '' AND (phone IS NULL OR phone = '') THEN 1 ELSE 0 END) as enrichable
        FROM leads
        GROUP BY state
        HAVING enrichable > 0
        ORDER BY enrichable DESC
        LIMIT ?
      `).all(maxStates);

      _waterfallProgress.total = states.length;
      let totalFieldsFilled = 0;
      let totalSaved = 0;

      for (let i = 0; i < states.length; i++) {
        if (!_waterfallRunning) break;

        const { state } = states[i];
        _waterfallProgress.step = `Enriching ${state} (${i + 1}/${states.length})`;
        _waterfallProgress.current = i;
        _waterfallProgress.detail = `Loading ${state} leads...`;

        const dbLeads = db.prepare(`
          SELECT * FROM leads WHERE profile_url != '' AND profile_url IS NOT NULL
          AND (email = '' OR email IS NULL OR phone = '' OR phone IS NULL OR website = '' OR website IS NULL)
          AND state = ? ORDER BY lead_score ASC LIMIT ?
        `).all(state, limitPerState);

        if (dbLeads.length === 0) continue;

        const leads = dbLeads.map(row => ({ ...row, source: row.primary_source || '' }));
        const EventEmitter = require('events');
        const emitter = new EventEmitter();
        emitter.on('waterfall-progress', (p) => {
          _waterfallProgress.detail = `${state}: ${p.step} ${p.current}/${p.total} — ${p.detail || ''}`;
        });

        const stats = await runWaterfall(leads, {
          fetchProfiles: true,
          crossRefMartindale: false, crossRefLawyersCom: false,
          nameLookups: false, emailCrawl: false, smtpPatterns: true,
          masterDbLookup: false,
          emitter,
          isCancelled: () => !_waterfallRunning,
          maxProfileFetches: limitPerState,
        });

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
            try { leadDb.updateLead(lead.id, updates); saved++; } catch (err) { console.error(`[Waterfall Multi] updateLead failed for lead ${lead.id}:`, err.message); }
          } else if (lead._enrichmentError) {
            try {
              leadDb.updateLead(lead.id, {
                last_enrichment_error: lead._enrichmentError,
                enrichment_attempts: (orig.enrichment_attempts || 0) + 1,
                last_enriched_at: new Date().toISOString(),
              });
            } catch (err) { console.error(`[Waterfall Multi] updateLead enrichment-error failed for lead ${lead.id}:`, err.message); }
          }
        }

        totalFieldsFilled += stats.totalFieldsFilled;
        totalSaved += saved;

        try {
          leadDb.recordWaterfallRun({
            stateFilter: state,
            leadsProcessed: dbLeads.length,
            leadsEnriched: saved,
            fieldsFilled: stats.totalFieldsFilled,
            profilesFetched: stats.profilesFetched || 0,
            profilesAttempted: stats.profilesAttempted || 0,
            profilesFailed: stats.profilesFailed || 0,
            websitesFound: stats.websitesFound || 0,
            emailsFound: (stats.emailsCrawled || 0) + (stats.smtpPatternsFound || 0),
            trigger: 'multi-state',
            stats: { ...stats, profileErrors: (stats.profileErrors || []).slice(0, 50) },
            durationSecs: 0,
          });
        } catch (err) { console.error(`[Waterfall Multi] recordWaterfallRun failed for ${state}:`, err.message); }

        console.log(`[Waterfall Multi] ${state}: ${stats.totalFieldsFilled} fields, ${saved} saved`);
      }

      leadDb.batchScoreLeads();
      _waterfallProgress.step = `Done — ${totalFieldsFilled} fields filled across ${states.length} states, ${totalSaved} leads updated`;
      _waterfallProgress.current = states.length;
      _waterfallProgress.saved = totalSaved;
      _waterfallProgress.stats = { totalFieldsFilled, totalSaved, statesProcessed: states.length };
      console.log(`[Waterfall Multi] Done: ${totalFieldsFilled} fields, ${totalSaved} saved across ${states.length} states`);
    })()
      .catch(err => {
        _waterfallProgress.step = `Error: ${err.message}`;
        console.error('[Waterfall Multi] Error:', err.message);
      })
      .finally(() => { _waterfallRunning = false; });
  });

  // --- Enrichment coverage ---
  router.get('/leads/enrichment-coverage', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const db = leadDb.getDb();
      const states = db.prepare(`
        SELECT state,
          COUNT(*) as total,
          SUM(CASE WHEN last_enriched_at IS NOT NULL THEN 1 ELSE 0 END) as enriched,
          SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as withPhone,
          SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as withEmail,
          SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as withWebsite,
          SUM(CASE WHEN profile_url IS NOT NULL AND profile_url != '' THEN 1 ELSE 0 END) as hasProfileUrl,
          SUM(CASE WHEN lead_score >= 80 THEN 1 ELSE 0 END) as excellent,
          SUM(CASE WHEN lead_score >= 55 AND lead_score < 80 THEN 1 ELSE 0 END) as good,
          ROUND(AVG(lead_score), 1) as avgScore
        FROM leads
        GROUP BY state
        HAVING total > 0
        ORDER BY total DESC
      `).all();
      const totals = {
        total: states.reduce((s, r) => s + r.total, 0),
        enriched: states.reduce((s, r) => s + r.enriched, 0),
        withPhone: states.reduce((s, r) => s + r.withPhone, 0),
        withEmail: states.reduce((s, r) => s + r.withEmail, 0),
        withWebsite: states.reduce((s, r) => s + r.withWebsite, 0),
      };
      res.json({ states, totals });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrichment priority ---
  router.get('/leads/enrichment-priority', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const db = leadDb.getDb();
      const gaps = db.prepare(`
        SELECT state,
          COUNT(*) as total,
          SUM(CASE WHEN profile_url IS NOT NULL AND profile_url != '' AND (phone IS NULL OR phone = '') THEN 1 ELSE 0 END) as enrichable,
          SUM(CASE WHEN (phone IS NULL OR phone = '') THEN 1 ELSE 0 END) as missing_phone,
          SUM(CASE WHEN (email IS NULL OR email = '') THEN 1 ELSE 0 END) as missing_email,
          SUM(CASE WHEN (website IS NULL OR website = '') THEN 1 ELSE 0 END) as missing_website
        FROM leads
        GROUP BY state
        HAVING enrichable > 0
        ORDER BY enrichable DESC
      `).all();
      res.json(gaps);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Recently enriched ---
  router.get('/leads/recently-enriched', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const db = leadDb.getDb();
      const hours = parseInt(req.query.hours) || 24;
      const limit = Math.min(parseInt(req.query.limit) || 100, 500);
      const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
      const leads = db.prepare(`
        SELECT id, first_name, last_name, firm_name, city, state, email, phone, website,
               last_enriched_at, enrichment_steps, lead_score, profile_url
        FROM leads
        WHERE last_enriched_at >= ?
        ORDER BY last_enriched_at DESC
        LIMIT ?
      `).all(cutoff, limit);
      const stats = db.prepare(`
        SELECT COUNT(*) as total,
          SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as withEmail,
          SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as withPhone,
          SUM(CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) as withWebsite
        FROM leads WHERE last_enriched_at >= ?
      `).get(cutoff);
      res.json({ leads, stats, cutoff, hours });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrichment failures ---
  router.get('/leads/enrichment-failures', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const state = req.query.state || '';
      const limit = Math.min(parseInt(req.query.limit) || 100, 500);
      const failures = leadDb.getEnrichmentFailures(state || undefined, limit);
      const stats = leadDb.getEnrichmentFailureStats();
      res.json({ failures, stats });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.post('/leads/enrichment-failures/clear', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { state } = req.body || {};
      const result = leadDb.clearEnrichmentErrors(state || undefined);
      res.json({ cleared: result.changes });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrichment stats ---
  router.get('/leads/enrichment-stats', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEnrichmentStats());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Enrichment info per lead ---
  router.get('/leads/:id/enrichment-info', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const result = leadDb.getLeadForEnrichment(parseInt(req.params.id));
      if (!result) return res.status(404).json({ error: 'Lead not found' });
      res.json(result);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Enrichment Queue ---
  router.get('/enrichment-queue/status', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEnrichmentQueueStatus());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/enrichment-queue/add', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, source, fieldsRequested } = req.body;
      if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
      res.json(leadDb.addToEnrichmentQueue(leadIds, source, fieldsRequested));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/enrichment-queue/process', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.processEnrichmentQueue(parseInt(req.body.batchSize) || 10));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/enrichment-queue/clear', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.clearEnrichmentQueue(req.query.status || 'completed'));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Firm Intelligence ---
  router.get('/firms', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getFirmIntelligence(parseInt(req.query.limit) || 50));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/firms/:name', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getFirmDetail(req.params.name));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/firms/enrich', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.enrichFirmData());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/firms/directory', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      const minSize = parseInt(req.query.minSize) || 2;
      res.json(leadDb.getFirmDirectory(limit, minSize));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Bulk Enrichment ---
  router.post('/bulk-enrichment/run', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, sourceFilter } = req.body;
      if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds (array) required' });
      res.json(leadDb.createBulkEnrichmentRun(leadIds, sourceFilter));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/bulk-enrichment/runs', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getBulkEnrichmentRuns(parseInt(req.query.limit) || 20));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/bulk-enrichment/process/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.processBulkEnrichmentBatch(parseInt(req.params.id), parseInt(req.body.batchSize) || 20));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/bulk-enrichment/diff/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getBulkEnrichmentDiff(parseInt(req.params.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Enrichment Waterfall analytics ---
  router.get('/enrichment-waterfall', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEnrichmentWaterfall());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Enrichment ROI ---
  router.get('/enrichment-roi', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEnrichmentROI());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Backfill enrichment dates ---
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
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Score leads ---
  router.post('/leads/score', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.batchScoreLeads());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
};
