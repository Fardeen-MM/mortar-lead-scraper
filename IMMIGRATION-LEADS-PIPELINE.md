# Immigration Leads Pipeline

One-sentence: **Scrape 20K+ immigration-focused B2B leads with emails for cold outreach.**

## Quick start — get leads now

```bash
# Final output (already exists):
cat output/master-immigration-leads.csv    # ~20K+ leads
cat output/cold-email-ready.csv            # validated + deduped (import to Instantly)
ls output/cold-email-by-country/           # split by country
```

## What's in it

| Source | Yield | Email coverage |
|--------|-------|----------------|
| CICC Canada (immigration consultants) | 10,390 | 83% |
| ProPublica US immigration nonprofits | 5,000+ | ~40% after enrichment |
| EOIR US accredited immigration reps | 5,217 | ~17% after enrichment |
| ICEF Global migration/education agents | 2,216 | ~78% after enrichment |
| Immigration Podcasts (global hosts) | 1,434 | 88% (iTunes RSS) |
| English UK ESL Schools (adjacent) | 278 | 95% |

Total: ~20-24K leads, ~13-15K emails.

## Countries (emails, ranked)

Canada · United States · United Kingdom · India · Australia · Nepal · Nigeria · Pakistan · Brazil · Turkey · Mexico · Bangladesh · Spain · Taiwan · UAE · Philippines

## On-demand lead finding (Apollo clone)

```bash
# Set discovery engine (pick one):
export SERPER_API_KEY=<your-key>              # Best: Google results, $1/1K
# OR self-host SearxNG (free):
./scripts/setup-searxng-docker.sh
export SEARXNG_URL=http://localhost:8888

# Then query anything:
node scripts/find-leads.js "immigration consultant" "Miami FL" --count 50
node scripts/find-leads.js "visa lawyer" "Toronto" --count 100 --out toronto.csv
```

## Pipeline steps (what runs under the hood)

1. **Discovery**: SERP → list of business websites per (niche, city)
2. **Person extraction**: Puppeteer visits each website, extracts team members
3. **Email waterfall** (`lib/email-waterfall.js`):
   - MS365 GetCredentialType (free, covers ~40% of B2B)
   - Google Workspace SMTP RCPT
   - Catch-all detection
   - 15-pattern generation with per-domain pattern learning
   - Gravatar existence check (confidence 50, weak signal)
4. **Dedup**: email → domain → firm+city

## Additional tools

| Script | Purpose |
|--------|---------|
| `scripts/scrape-icef-ias.js` | Pull ICEF global agents (free JSON API) |
| `scripts/scrape-immigration-podcasts.js` | iTunes × RSS for podcast hosts |
| `scripts/scrape-propublica-immigration.js` | 12 keywords × nonprofit DB |
| `scripts/parse-eoir-pdf.py` | EOIR PDF → CSV via pdfplumber |
| `scripts/resolve-org-domains.js` | Company name → domain (DNS guess, 50% hit) |
| `scripts/crawl-websites-for-emails.js` | HTTP crawler w/ Cloudflare decode, 70-80% hit |
| `scripts/enrich-generic-emails.js` | info@/contact@ per domain (low yield from blocked port 25) |
| `scripts/merge-immigration-leads.js` | Master dedupe merger |
| `scripts/export-for-cold-email.js` | Validate + country split + Instantly format |

## Library patches applied (per 2026-04-16 audit)

- `lib/email-waterfall.js`: Autodiscover semantic bug fixed (200 was 92% confidence false positive). Tiered confidence per response code.
- `lib/email-finder.js`: Cloudflare email decode, HTML entity decode, JS concatenation detection.
- `lib/person-extractor.js`: Name dict 300 → 1,500 across 20+ cultures; TEAM_PATHS 10 → 35 paths.
- `lib/website-finder.js`: Brandfetch (500K/mo free) + DDG fallback; dropped Google scraping.
- `lib/duckduckgo-scraper.js`: Delays 8-12s → 15-25s; retry backoff 8s-16s → 30s-60s.

## Key environment variables

```bash
# Optional — better discovery
export SERPER_API_KEY=<key>              # $1/1K queries, best quality
export SEARXNG_URL=http://localhost:8888 # self-hosted, unlimited
export BRANDFETCH_API_KEY=<key>          # free 500K/mo, company→domain

# Production email verification (not required for existing data)
# Rent Hetzner/OVH VPS with port 25 unblocked, run reacherhq.
```

## Projected post-crawl numbers (when running jobs finish)

- Final leads: **22,000-25,000**
- Final emails: **13,000-15,000**
- Final countries: 150+ (primary: CA, US, UK, India, AU, Nepal, Nigeria, Pakistan)

## Cost

- Free path (SearxNG + domain guessing + HTTP crawl): **$0** — 20K+ leads shipped
- Production path ($110/mo): Hetzner VPS + SOCKS5 SMTP + MillionVerifier for catch-alls → 100K+ emails/mo at $0.001/email

## Limitations / caveats

- Port 25 is blocked by residential ISP — SMTP verification falls back to MS365 API (works) + Gravatar (weak). Production needs VPS.
- DuckDuckGo aggressively rate-limits the free scraper — use Serper or SearxNG for > 10 queries.
- Cloudflare/Imperva block the broken Google scraping approach — library patched to remove it.
- Meta Ad Library scraper not yet built (complex Meta app setup required).
