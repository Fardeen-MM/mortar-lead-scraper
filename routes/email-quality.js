/**
 * Email Quality Routes — Email verification, classification, validation,
 * Instantly export, compliance, DNC
 */

const router = require('express').Router();

module.exports = function ({ broadcastAll, upload }) {

  // --- Email Verification ---
  let _verifyRunning = false;
  let _verifyProgress = null;

  router.post('/leads/verify-emails', (req, res) => {
    if (_verifyRunning) {
      return res.json({ status: 'already_running', progress: _verifyProgress });
    }

    _verifyRunning = true;
    _verifyProgress = { current: 0, total: 0, found: 0, verified: 0, invalid: 0 };
    res.json({ status: 'started', message: 'Email verification started in background' });

    const EmailVerifier = require('../lib/email-verifier');
    const leadDb = require('../lib/lead-db');
    const verifier = new EmailVerifier();

    (async () => {
      const leadsNeedingEmail = leadDb.getLeadsNeedingEmail(Math.min(Math.max(parseInt(req.body.limit) || 200, 1), 5000));
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

  router.get('/leads/verify-status', (req, res) => {
    res.json({ running: _verifyRunning, progress: _verifyProgress });
  });

  // --- Verification stats ---
  router.get('/leads/verification-stats', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getVerificationStats()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Import verification results ---
  router.post('/leads/import-verification', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      const leadDb = require('../lib/lead-db');
      const csv = require('../lib/csv-handler');
      const rows = await csv.readCSV(req.file.path);
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

  // --- Email classification ---
  router.get('/leads/email-classification', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getEmailClassification()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/leads/classify-emails', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.classifyAllEmails()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Email validation ---
  let _validateEmailsRunning = false;
  let _validateEmailsProgress = null;

  router.post('/leads/validate-emails', async (req, res) => {
    if (_validateEmailsRunning) return res.json({ running: true, progress: _validateEmailsProgress });
    try {
      const leadDb = require('../lib/lead-db');
      _validateEmailsRunning = true;
      _validateEmailsProgress = { started: true, processed: 0 };
      res.json({ started: true });
      leadDb.batchValidateEmails((progress) => {
        _validateEmailsProgress = progress;
        broadcastAll({ type: 'validation-progress', ...progress });
      }).then(result => {
        _validateEmailsRunning = false;
        _validateEmailsProgress = { ...result, done: true };
        broadcastAll({ type: 'validation-complete', ...result });
      }).catch(err => {
        _validateEmailsRunning = false;
        _validateEmailsProgress = { error: err.message };
        console.error('[validate-emails] Error:', err.message);
      });
    } catch (err) {
      _validateEmailsRunning = false;
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/leads/validate-emails/status', (req, res) => {
    res.json({ running: _validateEmailsRunning, progress: _validateEmailsProgress });
  });

  router.get('/leads/validate-email/:email', async (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const syntax = leadDb.validateEmailSyntax(req.params.email);
      if (!syntax.valid) return res.json(syntax);
      const mx = await leadDb.validateEmailMX(req.params.email);
      res.json(mx);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Instantly export ---
  router.get('/leads/export/instantly', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { createObjectCsvWriter } = require('csv-writer');
      const { generateOutputPath } = require('../lib/csv-handler');
      const path = require('path');
      const { state, country, enrichedAfter } = req.query;

      const leads = leadDb.exportLeads({
        state, country, enrichedAfter,
        hasEmail: true,
      });

      if (leads.length === 0) {
        return res.status(404).json({ error: 'No leads with email found' });
      }

      const instantlyLeads = leads.map(l => ({
        email: l.email,
        first_name: l.first_name,
        last_name: l.last_name,
        company_name: l.firm_name || '',
        title: l.title || '',
        phone: l.phone || '',
        website: l.website || '',
        linkedin_url: l.linkedin_url || '',
        city: l.city || '',
        state: l.state || '',
        country: l.country || '',
        practice_area: l.practice_area || '',
        personalization: (() => {
          const parts = [];
          if (l.title && l.firm_name) parts.push(`I see you're ${l.title} at ${l.firm_name}`);
          else if (l.firm_name) parts.push(`I came across ${l.firm_name}`);
          if (l.practice_area) parts.push(`specializing in ${l.practice_area}`);
          if (l.city && l.state) parts.push(`in ${l.city}, ${l.state}`);
          else if (l.city || l.state) parts.push(`in ${l.city || l.state}`);
          return parts.length > 0 ? parts.join(' ') : `I came across your profile`;
        })(),
      }));

      const outputFile = generateOutputPath('INSTANTLY-EXPORT', '');
      const writer = createObjectCsvWriter({
        path: outputFile,
        header: [
          { id: 'email', title: 'email' },
          { id: 'first_name', title: 'first_name' },
          { id: 'last_name', title: 'last_name' },
          { id: 'company_name', title: 'company_name' },
          { id: 'title', title: 'title' },
          { id: 'phone', title: 'phone' },
          { id: 'website', title: 'website' },
          { id: 'linkedin_url', title: 'linkedin_url' },
          { id: 'city', title: 'city' },
          { id: 'state', title: 'state' },
          { id: 'country', title: 'country' },
          { id: 'practice_area', title: 'practice_area' },
          { id: 'personalization', title: 'personalization' },
        ],
      });

      writer.writeRecords(instantlyLeads).then(() => {
        res.download(outputFile, path.basename(outputFile));
      }).catch(err => res.status(500).json({ error: 'CSV write failed: ' + err.message }));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- Compliance ---
  router.get('/compliance/dashboard', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getComplianceDashboard()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/compliance/opt-out', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { email, reason, source } = req.body;
      if (!email) return res.status(400).json({ error: 'email required' });
      res.json(leadDb.addOptOut(email, reason, source));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/compliance/opt-out', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { email } = req.body;
      if (!email) return res.status(400).json({ error: 'email required' });
      res.json(leadDb.removeOptOut(email));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/compliance/check/:email', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.checkEmailCompliance(req.params.email)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/compliance/consent', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { leadId, consentType, status, source, notes } = req.body;
      if (!leadId || !consentType) return res.status(400).json({ error: 'leadId and consentType required' });
      res.json(leadDb.recordConsent(parseInt(leadId), consentType, status, source, notes));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- DNC ---
  router.get('/dnc', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getDncList(req.query.type || null)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.post('/dnc', (req, res) => {
    try {
      const leadDb = require('../lib/lead-db');
      const { type, value, reason } = req.body;
      if (!type || !value) return res.status(400).json({ error: 'type and value required' });
      leadDb.addToDnc(type, value, reason || '');
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.delete('/dnc/:id', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); leadDb.removeFromDnc(parseInt(req.params.id)); res.json({ success: true }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  router.get('/dnc/check', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.batchCheckDnc()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // --- Email Deliverability ---
  router.get('/email/deliverability', (req, res) => {
    try { const leadDb = require('../lib/lead-db'); res.json(leadDb.getEmailDeliverability()); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  return router;
};
