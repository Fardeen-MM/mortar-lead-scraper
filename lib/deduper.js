/**
 * Deduper — check scraped leads against known leads set
 *
 * Matching rules (ANY match = duplicate):
 *   1. firm_name + state  (fuzzy on firm name)
 *   2. first_name + last_name + city
 *   3. website domain (exact)
 *   4. phone digits (exact)
 *
 * Uses Fuse.js for fuzzy firm name matching.
 */

const Fuse = require('fuse.js');
const { normalizeRecord } = require('./normalizer');

class Deduper {
  constructor(existingLeads = []) {
    // Normalize all existing leads
    this.leads = existingLeads.map(normalizeRecord);
    this.stats = { checked: 0, duplicates: 0, unique: 0 };

    // Build lookup indexes for fast exact matching
    this.domainSet = new Set();
    this.phoneSet = new Set();
    this.nameLocationSet = new Set(); // "first|last|city" keys
    this.emailSet = new Set();

    for (const lead of this.leads) {
      const n = lead._norm;
      if (n.domain) this.domainSet.add(n.domain);
      if (n.phone && n.phone.length >= 7) this.phoneSet.add(n.phone);
      if (n.email) this.emailSet.add(n.email);
      if (n.firstName && n.lastName && n.city) {
        this.nameLocationSet.add(`${n.firstName}|${n.lastName}|${n.city}`);
      }
    }

    // Build Fuse index for fuzzy firm name matching, grouped by state
    this.firmsByState = {};
    for (const lead of this.leads) {
      const n = lead._norm;
      if (!n.firm || !n.state) continue;
      if (!this.firmsByState[n.state]) this.firmsByState[n.state] = [];
      this.firmsByState[n.state].push({ firm: n.firm });
    }

    this.fuseByState = {};
    for (const [state, firms] of Object.entries(this.firmsByState)) {
      this.fuseByState[state] = new Fuse(firms, {
        keys: ['firm'],
        threshold: 0.3,      // 0 = exact, 1 = match anything
        includeScore: true,
        minMatchCharLength: 3,
      });
    }
  }

  /**
   * Check if a scraped record is a duplicate of any existing lead.
   * Returns { isDuplicate: boolean, matchReason: string|null }
   */
  check(record) {
    this.stats.checked++;
    const norm = normalizeRecord(record)._norm;

    // Rule 1: website domain (exact)
    if (norm.domain && this.domainSet.has(norm.domain)) {
      this.stats.duplicates++;
      return { isDuplicate: true, matchReason: `domain: ${norm.domain}` };
    }

    // Rule 2: phone digits (exact)
    if (norm.phone && norm.phone.length >= 7 && this.phoneSet.has(norm.phone)) {
      this.stats.duplicates++;
      return { isDuplicate: true, matchReason: `phone: ${norm.phone}` };
    }

    // Rule 3: email (exact)
    if (norm.email && this.emailSet.has(norm.email)) {
      this.stats.duplicates++;
      return { isDuplicate: true, matchReason: `email: ${norm.email}` };
    }

    // Rule 4: first_name + last_name + city
    if (norm.firstName && norm.lastName && norm.city) {
      const key = `${norm.firstName}|${norm.lastName}|${norm.city}`;
      if (this.nameLocationSet.has(key)) {
        this.stats.duplicates++;
        return { isDuplicate: true, matchReason: `name+city: ${norm.firstName} ${norm.lastName} in ${norm.city}` };
      }
    }

    // Rule 5: firm_name + state (fuzzy)
    if (norm.firm && norm.state && this.fuseByState[norm.state]) {
      const results = this.fuseByState[norm.state].search(norm.firm);
      if (results.length > 0 && results[0].score < 0.3) {
        this.stats.duplicates++;
        return {
          isDuplicate: true,
          matchReason: `firm+state: "${norm.firm}" ≈ "${results[0].item.firm}" in ${norm.state} (score: ${results[0].score.toFixed(3)})`
        };
      }
    }

    this.stats.unique++;
    return { isDuplicate: false, matchReason: null };
  }

  /**
   * Add a new lead to the known set (for deduping within a single scrape run).
   */
  addToKnown(record) {
    const norm = normalizeRecord(record)._norm;
    if (norm.domain) this.domainSet.add(norm.domain);
    if (norm.phone && norm.phone.length >= 7) this.phoneSet.add(norm.phone);
    if (norm.email) this.emailSet.add(norm.email);
    if (norm.firstName && norm.lastName && norm.city) {
      this.nameLocationSet.add(`${norm.firstName}|${norm.lastName}|${norm.city}`);
    }
    // Note: we don't add to Fuse index dynamically (it's expensive) —
    // exact domain/phone/name matching catches intra-run dupes fine.
  }

  getStats() {
    return { ...this.stats };
  }
}

module.exports = Deduper;
