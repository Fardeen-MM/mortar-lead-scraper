# Session Deliverables — Immigration Lead Pipeline (2026-04-16)

**Elapsed**: Evening + overnight session after earlier 14.5hr morning run.

## What was shipped

### 📊 Data (ready for cold email)

| File | Count | What it is |
|------|-------|------------|
| `output/master-immigration-leads.csv` | ~20,650 | All unique immigration-focused leads |
| `output/cold-email-ready.csv` | ~11,400 | Validated + deduped emails in Instantly/Smartlead format |
| `output/leads-scored.csv` | 20,650 scored | Priority ranked (8,855 HOT 80+) |
| `output/cold-email-by-country/*.csv` | split | Per-country CSVs (CA 8,664, US 620, UK 295, India 92, etc.) |

### 🛠 Scrapers built (scripts/)

| Script | Yield | Details |
|--------|-------|---------|
| `scrape-english-uk-schools.js` | 278 ESL schools | British Council accredited, 100% email |
| `scrape-icef-ias.js` | 2,216 global migration agents | Undocumented JSON API, base64 encoded |
| `scrape-immigration-podcasts.js` | 1,310 podcast hosts | iTunes × RSS itunes:owner, 28 kw × 15 countries |
| `scrape-propublica-immigration.js` | 6,399 nonprofits | 12 kw × ProPublica API |
| `parse-eoir-pdf.py` | 5,217 US accredited reps | Python pdfplumber table extraction |

### ⚙️ Pipeline tools (scripts/)

| Tool | Purpose |
|------|---------|
| `find-leads.js` | Unified Apollo-clone CLI: `node scripts/find-leads.js "immigration consultant" "Miami FL"` |
| `resolve-org-domains.js` | Company name → domain via DNS guessing (50-84% hit) |
| `crawl-websites-for-emails.js` | HTTP website crawler w/ CF decode (70-80% email hit rate) |
| `merge-immigration-leads.js` | Master dedup merger (3-tier: email → domain → firm+city) |
| `export-for-cold-email.js` | Validation + country split + Instantly format |
| `score-leads.js` | Priority scoring (country × source × email-type) |
| `analyze-master.js` | Stats dashboard |
| `setup-searxng-docker.sh` | Self-host SearxNG for free unlimited SERP discovery |

### 🔧 Lib patches (per 2026-04-16 audit)

**email-waterfall.js — CRITICAL FIX**
- Autodiscover: HTTP 200 was returning 92% confidence (false positives for federated tenants). Now tiered: 302→mailbox 85%, 302→error 0%, 200+protocol 60%, 200 bare 45%, 404 0%.
- Gravatar confidence demoted 65 → 50 (post-2024 Gravatar adoption decline).

**email-finder.js**
- Added Cloudflare email decoder (`decodeCFEmail`).
- Added HTML entity decode, JS concatenation detection.
- Audit flagged 30% false-negative rate on CF-obfuscated sites — fixed.

**person-extractor.js**
- Name dictionary expanded 300 → ~1,500 across 20+ cultures: South Asian, Korean, Vietnamese, Filipino, Chinese, Japanese, African, Eastern European, Nordic, Dutch, Arabic, Persian, Turkish, Greek.
- TEAM_PATHS expanded 10 → 35 paths (legal, healthcare, business, education, i18n).

**website-finder.js**
- Added Brandfetch free tier (500K/mo, Clearbit successor).
- Removed broken Google HTTP scraping (80% CAPTCHA rate in 2026).
- Added DuckDuckGo fallback.

**duckduckgo-scraper.js**
- Polite delays 8-12s → 15-25s.
- Retry backoff 8s-16s → 30s-60s per open issues.

**lib/searxng-scraper.js (new)**
- SearxNG multi-engine metasearch client. Set `SEARXNG_URL=http://localhost:8080` after running `scripts/setup-searxng-docker.sh`.

### 📚 Memory updated

- `research-10x-plays-consolidated.md` — top 10 lead plays + tool inventory
- `mortar-lib-audit-2026-04-16.md` — audit scores per lib file
- `lead-scraping-session-2026-04-16-late.md` — full session log
- `feedback_immigration_first_webinar_niches.md` — immigration > course creators (mid-session pivot)
- `feedback_research_10x_better.md` — research before build

## How to use the data

1. Import `output/cold-email-ready.csv` to Instantly/Smartlead/Lemlist.
2. Warm up sender accounts for 2 weeks.
3. Use `output/cold-email-by-country/canada.csv` first (8,664 proven immigration consultants).
4. A/B test subjects per country (Canadian RCIC copy ≠ US immigration lawyer copy).
5. Use `output/leads-scored.csv` to prioritize top 8,855 HOT leads first.

## How to add more leads on demand

```bash
# Option A: Free (self-hosted SearxNG, ~20s/query)
./scripts/setup-searxng-docker.sh
export SEARXNG_URL=http://localhost:8888
node scripts/find-leads.js "immigration lawyer" "Los Angeles CA" --count 50

# Option B: Paid ($1/1K queries via Serper)
export SERPER_API_KEY=<get free key at serper.dev>
node scripts/find-leads.js "visa consultant" "London UK" --count 100

# Option C: Direct website crawl (if you already have a domain list CSV)
node scripts/crawl-websites-for-emails.js --in your-domains.csv --out enriched.csv
```

## Projected final (when ProPublica crawl completes, ~22 min from now)

- Total leads: ~21,000
- Emails: ~13,000 (up from 11,400 currently)
- Additional: ~1,500+ new nonprofit emails from US immigration ecosystem

## Deferred (would yield +10-15K more emails)

- **MIA Australia** (3,500 RMAs, Puppeteer form flow complex)
- **IAA UK** (3,800 advisers, Salesforce Aura)
- **Meta Ad Library immigration** (needs Meta app verification, 1-5K advertisers)
- **LinkedIn Sales Nav trial** (user action, 2,500-75K leads)
- **YouTube channel emails** (captcha-gated, needs API key)
- **ICEF extra course-graduate lists** (UKACTCGS, CCGs, QEACs — ~5K more agents)

## Budget path forward

- **$0/mo**: Current stack + self-hosted SearxNG Docker = unlimited. Scales to 100K leads/month via find-leads.js + crawl.
- **$90/mo**: Add Serper API = best-quality Google SERPs.
- **$110/mo**: + Hetzner VPS for SMTP verification (port 25 unblocked).
- **$200/mo**: + MillionVerifier for catch-all disambiguation.

At full production: ~$0.0003-0.001 per verified B2B email. 30-150× cheaper than Apollo/Hunter/Lead Magic.
