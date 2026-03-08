/**
 * Lead Database — Enrichment queue, firm intelligence, waterfall tracking
 */
const all = require('./_all');

module.exports = {
  enrichFirmData: all.enrichFirmData,
  getFirmDirectory: all.getFirmDirectory,
  getLeadForEnrichment: all.getLeadForEnrichment,
  getEnrichmentQueueStatus: all.getEnrichmentQueueStatus,
  addToEnrichmentQueue: all.addToEnrichmentQueue,
  processEnrichmentQueue: all.processEnrichmentQueue,
  clearEnrichmentQueue: all.clearEnrichmentQueue,
  getFirmIntelligence: all.getFirmIntelligence,
  getFirmDetail: all.getFirmDetail,
  createBulkEnrichmentRun: all.createBulkEnrichmentRun,
  getBulkEnrichmentRuns: all.getBulkEnrichmentRuns,
  processBulkEnrichmentBatch: all.processBulkEnrichmentBatch,
  getBulkEnrichmentDiff: all.getBulkEnrichmentDiff,
  recordWaterfallRun: all.recordWaterfallRun,
  getWaterfallRuns: all.getWaterfallRuns,
  getWaterfallSummary: all.getWaterfallSummary,
  getEnrichmentFailures: all.getEnrichmentFailures,
  getEnrichmentFailureStats: all.getEnrichmentFailureStats,
  clearEnrichmentErrors: all.clearEnrichmentErrors,
};
