/**
 * Leads Core Routes — CRUD, search, coverage, merge, export, import, dashboard
 *
 * Routes: /leads/*, /dashboard/overview, /leads/:id (CRUD)
 */

const path = require('path');
const fs = require('fs');
const router = require('express').Router();

module.exports = function ({ upload, OUTPUT_DIR }) {

  // --- TAM stats ---
  router.get('/leads/stats', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getStats());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Search leads ---
  router.get('/leads', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { q, state, country, hasEmail, hasPhone, hasWebsite, practiceArea, minScore, maxScore, tags, tag, source, sort, order, limit, offset, enrichedAfter, hasProfileUrl, hasLinkedin, hasEnrichmentError, createdAfter } = req.query;
      const result = leadDb.searchLeads(q, {
        state, country, practiceArea, tags: tags || tag, source,
        hasEmail: hasEmail === 'true',
        hasPhone: hasPhone === 'true',
        hasWebsite: hasWebsite === 'true',
        hasProfileUrl: hasProfileUrl === 'true',
        hasLinkedin: hasLinkedin === 'true',
        hasEnrichmentError: hasEnrichmentError === 'true',
        enrichedAfter, createdAfter,
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

  // --- Coverage ---
  router.get('/leads/coverage', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getStateCoverage());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Merge duplicates ---
  router.post('/leads/merge-duplicates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const dryRun = req.body.dryRun === true;
      const result = leadDb.mergeDuplicates({ dryRun });
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/merge-preview', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { keepId, deleteId } = req.query;
      if (!keepId || !deleteId) return res.status(400).json({ error: 'keepId and deleteId required' });
      const preview = leadDb.getMergePreview(parseInt(keepId), parseInt(deleteId));
      if (!preview) return res.status(404).json({ error: 'Lead not found' });
      res.json(preview);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/auto-merge', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const threshold = req.body.confidenceThreshold || 90;
      const dryRun = req.body.dryRun !== undefined ? req.body.dryRun !== false : false;
      const result = leadDb.autoMergeDuplicates(dryRun);
      res.json(result);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Export leads as CSV ---
  router.get('/leads/export', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { writeCSV, generateOutputPath } = require('../lib/csv-handler');
      const { state, country, hasEmail, hasPhone, hasWebsite, verified, enrichedAfter, practiceArea, tags, minScore } = req.query;
      const leads = leadDb.exportLeads({
        state, country,
        hasEmail: hasEmail === 'true',
        hasPhone: hasPhone === 'true',
        hasWebsite: hasWebsite === 'true',
        verified: verified === 'true',
        enrichedAfter, practiceArea, tags,
        minScore: minScore ? Number(minScore) : undefined,
      });
      if (leads.length === 0) {
        return res.status(404).json({ error: 'No leads match filters' });
      }
      const outputFile = generateOutputPath('EXPORT', '');
      writeCSV(outputFile, leads).then(() => {
        const filename = path.basename(outputFile);
        leadDb.recordExport('csv', leads.length, req.query, filename);
        res.download(outputFile, filename);
      }).catch(err => res.status(500).json({ error: 'CSV write failed: ' + err.message }));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Export leads as JSON ---
  router.get('/leads/export/json', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { state, country, hasEmail, hasPhone, hasWebsite, practiceArea, minScore, limit } = req.query;
      const leads = leadDb.exportLeads({
        state, country,
        hasEmail: hasEmail === 'true',
        hasPhone: hasPhone === 'true',
        hasWebsite: hasWebsite === 'true',
        practiceArea,
        minScore: minScore ? Number(minScore) : undefined,
      });
      const maxLeads = Math.min(parseInt(limit) || leads.length, leads.length);
      const exportData = leads.slice(0, maxLeads).map(l => ({
        id: l.id, first_name: l.first_name, last_name: l.last_name,
        email: l.email, phone: l.phone, firm_name: l.firm_name,
        title: l.title, city: l.city, state: l.state, country: l.country,
        website: l.website, linkedin_url: l.linkedin_url,
        practice_area: l.practice_area, bar_number: l.bar_number,
        bar_status: l.bar_status, admission_date: l.admission_date,
        lead_score: l.lead_score, tags: l.tags,
      }));
      res.setHeader('Content-Disposition', 'attachment; filename=leads-export.json');
      res.json(exportData);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- SmartLead Export ---
  router.get('/leads/export/smartlead', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { createObjectCsvWriter } = require('csv-writer');
      const { generateOutputPath } = require('../lib/csv-handler');
      const { state, country, enrichedAfter } = req.query;

      const leads = leadDb.exportLeads({ state, country, enrichedAfter, hasEmail: true });
      if (leads.length === 0) {
        return res.status(404).json({ error: 'No leads with email found' });
      }

      const smartLeads = leads.map(l => ({
        email: l.email,
        first_name: l.first_name,
        last_name: l.last_name,
        company: l.firm_name || '',
        title: l.title || '',
        phone: l.phone || '',
        website: l.website || '',
        linkedin_url: l.linkedin_url || '',
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
          { id: 'title', title: 'title' },
          { id: 'phone', title: 'phone' },
          { id: 'website', title: 'website' },
          { id: 'linkedin_url', title: 'linkedin_url' },
          { id: 'location', title: 'location' },
          { id: 'tags', title: 'tags' },
        ],
      });

      writer.writeRecords(smartLeads).then(() => {
        res.download(outputFile, path.basename(outputFile));
      }).catch(err => res.status(500).json({ error: 'CSV write failed: ' + err.message }));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Score-based Export ---
  router.get('/leads/export/by-score', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { writeCSV, generateOutputPath } = require('../lib/csv-handler');
      const minScore = parseInt(req.query.minScore) || 55;
      const { state, country, enrichedAfter } = req.query;

      const db = leadDb.getDb();
      let where = [`lead_score >= ?`];
      let params = [minScore];

      if (state) { where.push('state = ?'); params.push(state); }
      if (country) { where.push('country = ?'); params.push(country); }
      if (enrichedAfter) { where.push('last_enriched_at >= ?'); params.push(enrichedAfter); }

      const leads = db.prepare(
        `SELECT * FROM leads WHERE ${where.join(' AND ')} ORDER BY lead_score DESC, state, city`
      ).all(...params);

      if (leads.length === 0) {
        return res.status(404).json({ error: `No leads with score >= ${minScore}` });
      }

      const outputFile = generateOutputPath(`SCORE-${minScore}`, '');
      writeCSV(outputFile, leads).then(() => {
        res.download(outputFile, path.basename(outputFile));
      }).catch(err => res.status(500).json({ error: 'CSV write failed: ' + err.message }));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Export per-state ---
  router.get('/leads/export/state/:state', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { createObjectCsvWriter } = require('csv-writer');
      const { enrichedAfter, hasEmail, hasPhone } = req.query;
      const leads = leadDb.exportLeads({
        state: req.params.state,
        enrichedAfter,
        hasEmail: hasEmail === 'true',
        hasPhone: hasPhone === 'true',
      });
      if (leads.length === 0) return res.status(404).json({ error: 'No leads for this state' });

      const tmpPath = path.join(OUTPUT_DIR, `${req.params.state}-export-${Date.now()}.csv`);
      const headers = Object.keys(leads[0]).filter(k => k !== 'id' && k !== 'lead_score')
        .map(k => ({ id: k, title: k }));
      const writer = createObjectCsvWriter({ path: tmpPath, header: headers });
      writer.writeRecords(leads).then(() => {
        res.download(tmpPath, `${req.params.state}-leads.csv`, () => {
          try { fs.unlinkSync(tmpPath); } catch {}
        });
      }).catch(err => res.status(500).json({ error: err.message }));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Instantly Preview ---
  router.get('/leads/export/instantly/preview', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const leads = leadDb.exportForInstantly({
        state: req.query.state, practiceArea: req.query.practice,
        minScore: req.query.minScore, pipelineStage: req.query.stage,
        tags: req.query.tags,
      });
      res.json({ count: leads.length, sample: leads.slice(0, 5) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Sources ---
  router.get('/leads/sources', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getDistinctSources());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Growth ---
  router.get('/leads/growth', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const days = parseInt(req.query.days) || 30;
      const growth = leadDb.getDailyGrowth(days);
      res.json(growth);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Completeness ---
  router.get('/leads/completeness', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const data = leadDb.getFieldCompleteness();
      res.json(data);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Duplicates ---
  router.get('/leads/duplicates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      const dupes = leadDb.findPotentialDuplicates(limit);
      res.json(dupes);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Merge a pair ---
  router.post('/leads/merge', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { keepId, deleteId } = req.body;
      if (!keepId || !deleteId) return res.status(400).json({ error: 'keepId and deleteId required' });
      const result = leadDb.mergeLeadPair(keepId, deleteId);
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Delete Leads ---
  router.post('/leads/delete', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds } = req.body;
      if (!leadIds || !Array.isArray(leadIds)) {
        return res.status(400).json({ error: 'leadIds (array) required' });
      }
      res.json(leadDb.deleteLeads(leadIds));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Bulk delete ---
  router.post('/leads/bulk-delete', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds } = req.body;
      if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
        return res.status(400).json({ error: 'leadIds array required' });
      }
      if (leadIds.length > 500) {
        return res.status(400).json({ error: 'Max 500 leads per bulk delete' });
      }
      leadDb.deleteLeads(leadIds.map(Number));
      res.json({ deleted: leadIds.length });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- CSV Import ---
  router.post('/leads/import/csv', upload.single('file'), (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

      const leadDb = require('../lib/lead-db');
      const csvParse = require('csv-parser');
      const fsImport = require('fs');

      const results = [];
      const stream = fsImport.createReadStream(req.file.path).pipe(csvParse());

      stream.on('data', (row) => results.push(row));
      stream.on('end', () => {
        try { fsImport.unlinkSync(req.file.path); } catch {}

        if (results.length === 0) {
          return res.json({ imported: 0, skipped: 0, error: 'Empty CSV file' });
        }

        const headers = Object.keys(results[0]);
        const mapping = autoMapColumns(headers);

        let imported = 0, updated = 0, skipped = 0;
        const source = req.body.source || 'csv-import';

        for (const row of results) {
          const lead = {};
          for (const [field, csvCol] of Object.entries(mapping)) {
            if (csvCol && row[csvCol]) {
              lead[field] = row[csvCol].trim();
            }
          }

          if (mapping._full_name && row[mapping._full_name] && !lead.first_name && !lead.last_name) {
            const parts = row[mapping._full_name].trim().split(/\s+/);
            lead.first_name = parts[0] || '';
            lead.last_name = parts.slice(1).join(' ') || '';
          }

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

  // --- Import Preview ---
  router.post('/leads/import-preview', upload.single('file'), (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      const leadDb = require('../lib/lead-db');
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

  // --- Import ---
  router.post('/leads/import', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leads, options } = req.body;
      if (!leads || !Array.isArray(leads)) return res.status(400).json({ error: 'leads (array) required' });
      res.json(leadDb.importLeads(leads, options || {}));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/import/map-fields', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { headers } = req.body;
      if (!headers || !Array.isArray(headers)) return res.status(400).json({ error: 'headers (array) required' });
      res.json(leadDb.getImportFieldMapping(headers));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Digest ---
  router.get('/leads/digest', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getTodayDigest());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Activity Timeline ---
  router.get('/leads/activity', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = Math.min(parseInt(req.query.limit) || 50, 200);
      res.json(leadDb.getRecentActivity(limit));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Practice Areas ---
  router.get('/leads/practice-areas', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getDistinctPracticeAreas());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Tags ---
  router.get('/leads/tags', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getDistinctTags());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- State details ---
  router.get('/leads/state-details/:state', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const data = leadDb.getStateDetails(req.params.state);
      if (!data) return res.status(404).json({ error: 'No data for this state' });
      res.json(data);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Database Health Score ---
  router.get('/leads/health-score', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getDatabaseHealth());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Similar Leads ---
  router.get('/leads/similar/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.findSimilarLeads(parseInt(req.params.id)));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Top Firms ---
  router.get('/leads/top-firms', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = Math.min(parseInt(req.query.limit) || 20, 500);
      res.json(leadDb.getTopFirms(limit));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Scores ---
  router.get('/leads/scores', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getScoreDistribution());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Recommendations ---
  router.get('/leads/recommendations', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getRecommendations());
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Suggest ---
  router.get('/leads/suggest', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { q } = req.query;
      res.json(leadDb.getSearchSuggestions(q));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Bulk Update ---
  router.post('/leads/bulk-update', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, updates } = req.body;
      if (!Array.isArray(leadIds) || !leadIds.length || !updates || typeof updates !== 'object') return res.status(400).json({ error: 'leadIds (array) and updates (object) required' });
      const result = leadDb.bulkUpdateLeads(leadIds, updates);
      res.json(result);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Tag leads ---
  router.post('/leads/tag', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, tag, remove } = req.body;
      if (!leadIds || !Array.isArray(leadIds) || !tag) {
        return res.status(400).json({ error: 'leadIds (array) and tag (string) required' });
      }
      res.json(leadDb.tagLeads(leadIds, tag, remove === true));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Source attribution ---
  router.get('/leads/source-attribution', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getSourceAttribution());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Staleness ---
  router.get('/leads/staleness', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getStalenessReport());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Compare ---
  router.get('/leads/compare', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const id1 = parseInt(req.query.id1);
      const id2 = parseInt(req.query.id2);
      if (!id1 || !id2) return res.status(400).json({ error: 'id1 and id2 required' });
      const result = leadDb.compareLeads(id1, id2);
      if (!result) return res.status(404).json({ error: 'Lead not found' });
      res.json(result);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/compare', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds } = req.body;
      if (!leadIds || !Array.isArray(leadIds) || leadIds.length < 2) return res.status(400).json({ error: 'leadIds (array of 2+) required' });
      res.json(leadDb.getLeadComparisonData(leadIds));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Merge with choices ---
  router.post('/leads/merge-with-choices', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { keepId, mergeId, fieldChoices } = req.body;
      if (!keepId || !mergeId) return res.status(400).json({ error: 'keepId and mergeId required' });
      const result = leadDb.mergeLeadsWithChoices(keepId, mergeId, fieldChoices || {});
      res.json(result);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/merge-with-picks', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { targetId, sourceIds, fieldPicks } = req.body;
      if (!targetId || !Array.isArray(sourceIds) || !sourceIds.length) return res.status(400).json({ error: 'targetId and sourceIds (array) required' });
      res.json(leadDb.mergeLeadsWithPicks(targetId, sourceIds, fieldPicks || {}));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Smart duplicates ---
  router.get('/leads/smart-duplicates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 100;
      res.json(leadDb.findSmartDuplicates(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Cross-source duplicates ---
  router.get('/leads/cross-source-duplicates', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getCrossSourceDuplicates(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- KPI metrics ---
  router.get('/leads/kpi-metrics', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getKpiMetrics());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Kanban ---
  router.get('/leads/kanban', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getKanbanData(parseInt(req.query.limit) || 200));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Card view ---
  router.get('/leads/card-view', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getCardViewData({
        state: req.query.state, sortBy: req.query.sort,
        limit: parseInt(req.query.limit) || 50, offset: parseInt(req.query.offset) || 0,
      }));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Typeahead ---
  router.get('/leads/typeahead', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.searchTypeahead(req.query.q, parseInt(req.query.limit) || 10));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Filter facets ---
  router.get('/leads/filter-facets', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getFilterFacets());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Lookalikes ---
  router.get('/leads/:id/lookalikes', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 20;
      res.json(leadDb.findLookalikes(parseInt(req.params.id), limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/batch-lookalikes', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, limit } = req.body;
      if (!leadIds || !Array.isArray(leadIds)) return res.status(400).json({ error: 'leadIds array required' });
      res.json(leadDb.findBatchLookalikes(leadIds, limit || 50));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Personalization ---
  router.post('/leads/generate-personalization', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, template } = req.body;
      if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
        return res.status(400).json({ error: 'leadIds array required' });
      }
      if (leadIds.length > 1000) {
        return res.status(400).json({ error: 'Maximum 1000 leadIds per request' });
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

  // --- Intent Signals ---
  router.get('/leads/intent-signals', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getIntentSignals());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/practice-trends', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getPracticeAreaTrends());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Completeness Heatmap ---
  router.get('/leads/completeness-heatmap', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getCompletenessHeatmap());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Enrichment Recommendations ---
  router.get('/leads/enrichment-recommendations', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEnrichmentRecommendations());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Leaderboard ---
  router.get('/leads/leaderboard', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getLeaderboard({
        state: req.query.state, practiceArea: req.query.practice,
        metric: req.query.metric, limit: parseInt(req.query.limit) || 50,
      }));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/leaderboard-by-state', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getLeaderboardByState(parseInt(req.query.limit) || 10));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Changes ---
  router.get('/leads/changes', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getRecentChanges2(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/firm-changes', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 50;
      res.json(leadDb.getFirmChanges(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Most engaged ---
  router.get('/leads/most-engaged', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const limit = parseInt(req.query.limit) || 20;
      res.json(leadDb.getMostEngagedLeads(limit));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Engagement heatmap ---
  router.get('/leads/engagement-heatmap', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getEngagementHeatmap());
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/engagement-timeline', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const days = parseInt(req.query.days) || 30;
      res.json(leadDb.getEngagementTimeline(days));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Bulk tag/remove/assign/enroll ---
  router.post('/leads/bulk/tag', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, tag } = req.body;
      if (!leadIds || !tag) return res.status(400).json({ error: 'leadIds and tag required' });
      res.json(leadDb.bulkTagLeads(leadIds, tag));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/bulk/remove-tag', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, tag } = req.body;
      if (!leadIds || !tag) return res.status(400).json({ error: 'leadIds and tag required' });
      res.json(leadDb.bulkRemoveTag(leadIds, tag));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/bulk/assign-owner', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, owner } = req.body;
      if (!leadIds || !owner) return res.status(400).json({ error: 'leadIds and owner required' });
      res.json(leadDb.bulkAssignOwner(leadIds, owner));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/bulk/enroll-campaign', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, campaignId } = req.body;
      if (!leadIds || !campaignId) return res.status(400).json({ error: 'leadIds and campaignId required' });
      res.json(leadDb.bulkEnrollInCampaign(leadIds, campaignId));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/bulk/enroll-sequence', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadIds, sequenceId } = req.body;
      if (!leadIds || !sequenceId) return res.status(400).json({ error: 'leadIds and sequenceId required' });
      res.json(leadDb.bulkEnrollInSequence(leadIds, sequenceId));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Dashboard overview ---
  router.get('/dashboard/overview', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const [stats, scores, freshness, recommendations, digest] = [
        leadDb.getStats(),
        leadDb.getScoreDistribution(),
        leadDb.getFreshnessReport(),
        leadDb.getRecommendations(),
        leadDb.getTodayDigest()
      ];
      res.json({ stats, scores, freshness, recommendations, digest });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Decay preview (batch 15 version) ---
  router.get('/leads/decay-preview', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const days = parseInt(req.query.days) || 30;
      res.json(leadDb.getDecayPreview(days));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/apply-decay', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { decayPercent, inactiveDays } = req.body;
      res.json(leadDb.applyScoreDecay(decayPercent || 5, inactiveDays || 30));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Lead Detail (MUST be last due to :id wildcard) ---
  router.get('/leads/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const lead = leadDb.getLeadById(parseInt(req.params.id));
      if (!lead) return res.status(404).json({ error: 'Lead not found' });
      res.json(lead);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.patch('/leads/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
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

  router.delete('/leads/:id', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const id = parseInt(req.params.id);
      const lead = leadDb.getLeadById(id);
      if (!lead) return res.status(404).json({ error: 'Lead not found' });
      leadDb.deleteLeads([id]);
      res.json({ deleted: true, id });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Lead sub-resources ---
  router.get('/leads/:id/change-history', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getLeadChangeHistory(parseInt(req.params.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/:id/engagement-sparkline', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getLeadEngagementSparkline(parseInt(req.params.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/:id/journey', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.getLeadJourney(parseInt(req.params.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/leads/:id/relationships', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      res.json(leadDb.buildRelationshipGraph(parseInt(req.params.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  return router;
};

// --- Helper: autoMapColumns ---
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

  if (!mapping.first_name && !mapping.last_name) {
    const nameIdx = lowerHeaders.findIndex(h => h === 'name' || h === 'full_name' || h === 'fullname');
    if (nameIdx >= 0) {
      mapping._full_name = headers[nameIdx];
    }
  }

  return mapping;
}
