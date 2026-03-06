/**
 * AI Campaign Analyzer
 *
 * Analyzes a business's ad presence and competitive landscape.
 * Uses Google Ads Transparency data + lead database to generate
 * actionable insights about who's advertising and how.
 *
 * Features:
 *   - Competitor ad spend detection
 *   - Ad copy analysis (keywords, messaging, offers)
 *   - Gap identification (what competitors do that you don't)
 *   - Market saturation scoring per niche + location
 *   - Recommended targeting strategy
 *
 * Usage:
 *   const { CampaignAnalyzer } = require('./lib/campaign-analyzer');
 *   const analyzer = new CampaignAnalyzer();
 *   const report = await analyzer.analyze({
 *     niche: 'personal injury lawyer',
 *     location: 'Miami, FL',
 *     firmName: 'Smith & Associates',
 *   });
 */

const fs = require('fs');
const path = require('path');

class CampaignAnalyzer {
  constructor(config = {}) {
    this.config = config;
    this.reportsDir = config.reportsDir || path.join(__dirname, '..', 'data', 'campaign-reports');
    if (!fs.existsSync(this.reportsDir)) fs.mkdirSync(this.reportsDir, { recursive: true });
  }

  /**
   * Full campaign analysis for a firm in a market.
   */
  async analyze({ niche, location, firmName, competitors = [], adData = [], leads = [] }) {
    const report = {
      firm: firmName,
      niche,
      location,
      generatedAt: new Date().toISOString(),
      marketOverview: null,
      competitorAnalysis: null,
      adInsights: null,
      recommendations: null,
      score: 0,
    };

    // Step 1: Market overview from lead database
    report.marketOverview = this._analyzeMarket(leads, niche, location);

    // Step 2: Competitor analysis
    report.competitorAnalysis = this._analyzeCompetitors(competitors, adData, firmName);

    // Step 3: Ad copy insights
    report.adInsights = this._analyzeAdCopy(adData);

    // Step 4: Generate recommendations
    report.recommendations = this._generateRecommendations(report);

    // Step 5: Overall campaign score (0-100)
    report.score = this._calculateScore(report);

    // Save report
    const filename = `${firmName.replace(/[^a-z0-9]/gi, '_').toLowerCase()}_${Date.now()}.json`;
    fs.writeFileSync(path.join(this.reportsDir, filename), JSON.stringify(report, null, 2));

    return report;
  }

  /**
   * Analyze market saturation from lead data.
   */
  _analyzeMarket(leads, niche, location) {
    const locationParts = (location || '').split(',').map(s => s.trim());
    const city = locationParts[0] || '';
    const state = locationParts[1] || '';

    // Filter leads by location
    const localLeads = leads.filter(l => {
      const matchCity = !city || (l.city || '').toLowerCase().includes(city.toLowerCase());
      const matchState = !state || (l.state || '').toLowerCase().includes(state.toLowerCase());
      return matchCity || matchState;
    });

    // Count firms by size indicators
    const firms = new Map();
    for (const lead of localLeads) {
      const firm = lead.firm_name || lead.domain || 'Unknown';
      if (!firms.has(firm)) {
        firms.set(firm, { name: firm, people: 0, hasEmail: 0, hasPhone: 0, hasWebsite: 0 });
      }
      const f = firms.get(firm);
      f.people++;
      if (lead.email) f.hasEmail++;
      if (lead.phone) f.hasPhone++;
      if (lead.website) f.hasWebsite++;
    }

    const firmList = [...firms.values()].sort((a, b) => b.people - a.people);

    return {
      totalLeads: localLeads.length,
      totalFirms: firms.size,
      avgFirmSize: firms.size > 0 ? Math.round(localLeads.length / firms.size * 10) / 10 : 0,
      topFirms: firmList.slice(0, 20).map(f => ({
        name: f.name,
        people: f.people,
        contactable: f.hasEmail + f.hasPhone,
      })),
      emailCoverage: localLeads.length > 0
        ? Math.round(localLeads.filter(l => l.email).length / localLeads.length * 100) : 0,
      phoneCoverage: localLeads.length > 0
        ? Math.round(localLeads.filter(l => l.phone).length / localLeads.length * 100) : 0,
      saturationScore: Math.min(100, Math.round(firms.size / 5)), // rough saturation metric
    };
  }

  /**
   * Analyze competitors based on ad data.
   */
  _analyzeCompetitors(competitors, adData, firmName) {
    // Group ads by advertiser
    const byAdvertiser = new Map();
    for (const ad of adData) {
      const name = ad.advertiser_name || ad.firm_name || '';
      if (!byAdvertiser.has(name)) {
        byAdvertiser.set(name, { name, adCount: 0, formats: new Set(), regions: new Set(), ads: [] });
      }
      const a = byAdvertiser.get(name);
      a.adCount += (ad.ad_count || 1);
      if (ad.ad_format) a.formats.add(ad.ad_format);
      if (ad.region) a.regions.add(ad.region);
      a.ads.push(ad);
    }

    const advertiserList = [...byAdvertiser.values()]
      .sort((a, b) => b.adCount - a.adCount)
      .map(a => ({
        name: a.name,
        adCount: a.adCount,
        formats: [...a.formats],
        regions: [...a.regions],
        isYou: a.name.toLowerCase().includes((firmName || '').toLowerCase()),
      }));

    // Find your position
    const yourPosition = advertiserList.findIndex(a => a.isYou);

    return {
      totalAdvertisers: advertiserList.length,
      topAdvertisers: advertiserList.slice(0, 15),
      yourPosition: yourPosition >= 0 ? yourPosition + 1 : null,
      yourAdCount: yourPosition >= 0 ? advertiserList[yourPosition].adCount : 0,
      topCompetitorAdCount: advertiserList.length > 0 ? advertiserList[0].adCount : 0,
      avgAdCount: advertiserList.length > 0
        ? Math.round(advertiserList.reduce((s, a) => s + a.adCount, 0) / advertiserList.length) : 0,
    };
  }

  /**
   * Analyze ad copy for keyword and messaging patterns.
   */
  _analyzeAdCopy(adData) {
    const keywords = new Map();
    const offers = [];
    const callsToAction = [];

    // Common legal marketing keywords
    const LEGAL_KEYWORDS = [
      'free consultation', 'no fee', 'no win no fee', 'results', 'experience',
      'aggressive', 'compassionate', 'trusted', 'proven', 'award-winning',
      'millions recovered', 'billion', 'settlement', 'verdict', 'injury',
      'accident', 'negligence', 'malpractice', 'wrongful death', 'slip and fall',
      'car accident', 'truck accident', 'motorcycle', 'pedestrian', 'workers comp',
      'family law', 'divorce', 'custody', 'criminal defense', 'dui', 'dwi',
      'immigration', 'bankruptcy', 'estate planning', 'real estate', 'business law',
      '24/7', 'se habla español', 'bilingual', 'local', 'nearby',
    ];

    for (const ad of adData) {
      const text = `${ad.headline || ''} ${ad.description || ''} ${ad.ad_text || ''}`.toLowerCase();
      if (!text.trim()) continue;

      for (const kw of LEGAL_KEYWORDS) {
        if (text.includes(kw)) {
          keywords.set(kw, (keywords.get(kw) || 0) + 1);
        }
      }

      // Detect offers
      if (text.includes('free') && text.includes('consult')) offers.push('Free Consultation');
      if (text.includes('no fee') || text.includes('no cost')) offers.push('No Fee Unless You Win');
      if (text.includes('% off') || text.includes('discount')) offers.push('Discount');
      if (text.includes('24/7') || text.includes('24 hours')) offers.push('24/7 Availability');

      // Detect CTAs
      if (text.includes('call now') || text.includes('call today')) callsToAction.push('Call Now');
      if (text.includes('get started') || text.includes('contact us')) callsToAction.push('Contact Us');
      if (text.includes('schedule') || text.includes('book')) callsToAction.push('Schedule');
      if (text.includes('learn more') || text.includes('find out')) callsToAction.push('Learn More');
    }

    // Deduplicate and count
    const offerCounts = {};
    offers.forEach(o => offerCounts[o] = (offerCounts[o] || 0) + 1);
    const ctaCounts = {};
    callsToAction.forEach(c => ctaCounts[c] = (ctaCounts[c] || 0) + 1);

    return {
      totalAdsAnalyzed: adData.length,
      topKeywords: [...keywords.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)
        .map(([kw, count]) => ({ keyword: kw, count })),
      commonOffers: Object.entries(offerCounts).sort((a, b) => b[1] - a[1])
        .map(([offer, count]) => ({ offer, count })),
      commonCTAs: Object.entries(ctaCounts).sort((a, b) => b[1] - a[1])
        .map(([cta, count]) => ({ cta, count })),
    };
  }

  /**
   * Generate actionable recommendations.
   */
  _generateRecommendations(report) {
    const recs = [];
    const { marketOverview, competitorAnalysis, adInsights } = report;

    // Market-based recommendations
    if (marketOverview) {
      if (marketOverview.saturationScore > 70) {
        recs.push({
          priority: 'high',
          category: 'market',
          title: 'High market saturation',
          detail: `${marketOverview.totalFirms} firms in this area. Focus on differentiation — specialization, unique value prop, or underserved sub-niches.`,
        });
      }
      if (marketOverview.emailCoverage < 50) {
        recs.push({
          priority: 'medium',
          category: 'outreach',
          title: 'Low email coverage in market',
          detail: `Only ${marketOverview.emailCoverage}% of leads have email. Run email waterfall enrichment to unlock cold email outreach to this market.`,
        });
      }
    }

    // Competitor-based recommendations
    if (competitorAnalysis) {
      if (competitorAnalysis.yourPosition === null) {
        recs.push({
          priority: 'high',
          category: 'advertising',
          title: 'No ad presence detected',
          detail: 'Your firm is not running Google Ads in this market. Competitors are spending — you\'re invisible to paid search traffic.',
        });
      } else if (competitorAnalysis.yourPosition > 5) {
        recs.push({
          priority: 'high',
          category: 'advertising',
          title: `Ranked #${competitorAnalysis.yourPosition} in ad volume`,
          detail: `Top competitor runs ${competitorAnalysis.topCompetitorAdCount} ads vs your ${competitorAnalysis.yourAdCount}. Increase ad volume or optimize for higher CTR.`,
        });
      }
      if (competitorAnalysis.totalAdvertisers > 20) {
        recs.push({
          priority: 'medium',
          category: 'strategy',
          title: 'Crowded ad landscape',
          detail: `${competitorAnalysis.totalAdvertisers} advertisers in this space. Consider long-tail keywords, geo-targeting, or time-based bidding to reduce CPC.`,
        });
      }
    }

    // Ad copy recommendations
    if (adInsights && adInsights.topKeywords.length > 0) {
      const topKws = adInsights.topKeywords.slice(0, 5).map(k => k.keyword);
      recs.push({
        priority: 'medium',
        category: 'messaging',
        title: 'Market messaging trends',
        detail: `Most-used keywords: ${topKws.join(', ')}. Ensure your ads address these themes or deliberately differentiate.`,
      });

      if (!adInsights.topKeywords.find(k => k.keyword.includes('español') || k.keyword.includes('bilingual'))) {
        recs.push({
          priority: 'low',
          category: 'messaging',
          title: 'Bilingual opportunity',
          detail: 'No competitors advertising bilingual services. If applicable, this is an underserved angle.',
        });
      }
    }

    // Default recommendations
    if (recs.length === 0) {
      recs.push({
        priority: 'medium',
        category: 'general',
        title: 'Insufficient data for analysis',
        detail: 'Run the Google Ads scraper and lead scraper for this market to generate detailed recommendations.',
      });
    }

    return recs.sort((a, b) => {
      const p = { high: 0, medium: 1, low: 2 };
      return (p[a.priority] || 3) - (p[b.priority] || 3);
    });
  }

  /**
   * Overall campaign health score (0-100).
   */
  _calculateScore(report) {
    let score = 50; // baseline

    const { marketOverview, competitorAnalysis, adInsights } = report;

    if (marketOverview) {
      // More leads = better data = higher score
      if (marketOverview.totalLeads > 100) score += 10;
      if (marketOverview.emailCoverage > 60) score += 5;
      if (marketOverview.saturationScore < 50) score += 5; // less saturated = more opportunity
    }

    if (competitorAnalysis) {
      if (competitorAnalysis.yourPosition !== null) {
        score += 10; // you're at least advertising
        if (competitorAnalysis.yourPosition <= 3) score += 10; // top 3
        if (competitorAnalysis.yourPosition <= 1) score += 5;  // #1
      }
      if (competitorAnalysis.yourAdCount > competitorAnalysis.avgAdCount) score += 5;
    }

    if (adInsights) {
      if (adInsights.totalAdsAnalyzed > 10) score += 5;
    }

    return Math.min(100, Math.max(0, score));
  }

  /**
   * Generate a formatted text report.
   */
  formatReport(report) {
    const lines = [];
    lines.push('');
    lines.push('╔═══════════════════════════════════════════════════════════╗');
    lines.push(`║  CAMPAIGN ANALYSIS: ${(report.firm || 'Unknown').substring(0, 36).padEnd(37)}║`);
    lines.push('╠═══════════════════════════════════════════════════════════╣');
    lines.push(`║  Niche:    ${(report.niche || '').padEnd(46)}║`);
    lines.push(`║  Location: ${(report.location || '').padEnd(46)}║`);
    lines.push(`║  Score:    ${String(report.score + '/100').padEnd(46)}║`);
    lines.push('╠═══════════════════════════════════════════════════════════╣');

    if (report.marketOverview) {
      const m = report.marketOverview;
      lines.push('║  MARKET OVERVIEW                                          ║');
      lines.push(`║    Total firms:    ${String(m.totalFirms).padEnd(38)}║`);
      lines.push(`║    Total leads:    ${String(m.totalLeads).padEnd(38)}║`);
      lines.push(`║    Email coverage: ${String(m.emailCoverage + '%').padEnd(38)}║`);
      lines.push(`║    Saturation:     ${String(m.saturationScore + '/100').padEnd(38)}║`);
      lines.push('╠═══════════════════════════════════════════════════════════╣');
    }

    if (report.competitorAnalysis) {
      const c = report.competitorAnalysis;
      lines.push('║  COMPETITOR LANDSCAPE                                     ║');
      lines.push(`║    Advertisers:    ${String(c.totalAdvertisers).padEnd(38)}║`);
      lines.push(`║    Your position:  ${String(c.yourPosition || 'Not advertising').padEnd(38)}║`);
      lines.push(`║    Your ads:       ${String(c.yourAdCount).padEnd(38)}║`);
      lines.push(`║    Top competitor: ${String(c.topCompetitorAdCount + ' ads').padEnd(38)}║`);
      if (c.topAdvertisers.length > 0) {
        lines.push('║                                                           ║');
        lines.push('║    Top advertisers:                                       ║');
        for (const a of c.topAdvertisers.slice(0, 5)) {
          const label = `      ${a.isYou ? '→ ' : '  '}${a.name.substring(0, 30).padEnd(30)} ${String(a.adCount).padStart(4)} ads`;
          lines.push(`║  ${label.padEnd(56)}║`);
        }
      }
      lines.push('╠═══════════════════════════════════════════════════════════╣');
    }

    if (report.recommendations && report.recommendations.length > 0) {
      lines.push('║  RECOMMENDATIONS                                          ║');
      for (const rec of report.recommendations) {
        const icon = rec.priority === 'high' ? '!!' : rec.priority === 'medium' ? ' !' : '  ';
        lines.push(`║  ${icon} [${rec.priority.toUpperCase().padEnd(6)}] ${rec.title.substring(0, 42).padEnd(42)}║`);
      }
      lines.push('╠═══════════════════════════════════════════════════════════╣');
    }

    lines.push(`║  Generated: ${(report.generatedAt || '').substring(0, 19).padEnd(44)}║`);
    lines.push('╚═══════════════════════════════════════════════════════════╝');
    lines.push('');

    return lines.join('\n');
  }
}

module.exports = { CampaignAnalyzer };
