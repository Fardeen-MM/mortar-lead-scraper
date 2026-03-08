/**
 * Lead Database — Deduplication, merge, comparison, DNC
 */
const all = require('./_all');

module.exports = {
  mergeDuplicates: all.mergeDuplicates,
  findPotentialDuplicates: all.findPotentialDuplicates,
  mergeLeadPair: all.mergeLeadPair,
  getMergePreview: all.getMergePreview,
  autoMergeDuplicates: all.autoMergeDuplicates,
  compareLeads: all.compareLeads,
  mergeLeadsWithChoices: all.mergeLeadsWithChoices,
  findSmartDuplicates: all.findSmartDuplicates,
  addToDnc: all.addToDnc,
  removeFromDnc: all.removeFromDnc,
  getDncList: all.getDncList,
  checkDnc: all.checkDnc,
  batchCheckDnc: all.batchCheckDnc,
  getCrossSourceDuplicates: all.getCrossSourceDuplicates,
  scanForDuplicates: all.scanForDuplicates,
  getDedupQueue: all.getDedupQueue,
  resolveDedupItem: all.resolveDedupItem,
  getDedupStats: all.getDedupStats,
  getLeadComparisonData: all.getLeadComparisonData,
  mergeLeadsWithPicks: all.mergeLeadsWithPicks,
  getMergeCandidates: all.getMergeCandidates,
  executeMerge: all.executeMerge,
};
