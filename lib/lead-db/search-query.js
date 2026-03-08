/**
 * Lead Database — Search, filtering, segments, smart lists
 */
const all = require('./_all');

module.exports = {
  searchLeads: all.searchLeads,
  lookupByNameCity: all.lookupByNameCity,
  batchLookupByNameCity: all.batchLookupByNameCity,
  getSearchSuggestions: all.getSearchSuggestions,
  getSegments: all.getSegments,
  createSegment: all.createSegment,
  updateSegment: all.updateSegment,
  deleteSegment: all.deleteSegment,
  querySegment: all.querySegment,
  querySegmentLeads: all.querySegmentLeads,
  getSmartLists: all.getSmartLists,
  createSmartList: all.createSmartList,
  deleteSmartList: all.deleteSmartList,
  getSmartListLeads: all.getSmartListLeads,
  previewSmartListCount: all.previewSmartListCount,
  updateSmartList: all.updateSmartList,
  getSmartListStats: all.getSmartListStats,
  searchTypeahead: all.searchTypeahead,
  getFilterFacets: all.getFilterFacets,
  getSavedSearches: all.getSavedSearches,
  createSavedSearch: all.createSavedSearch,
  deleteSavedSearch: all.deleteSavedSearch,
  checkSavedSearchAlerts: all.checkSavedSearchAlerts,
};
