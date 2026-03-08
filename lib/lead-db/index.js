/**
 * Lead Database — Module Index
 *
 * Re-exports everything from the implementation file.
 * Also exposes domain-specific sub-modules for focused imports:
 *
 *   const leadDb = require('./lib/lead-db');           // all 383 functions
 *   const { searchLeads } = require('./lib/lead-db');  // destructured
 *   const scoring = require('./lib/lead-db/scoring');  // domain module
 */

module.exports = require('./_all');
