#!/usr/bin/env python3
"""
Meta Ads Checker — Pure HTTP, no browser, no API key.

Uses MetaAdsCollector (reverse-engineered Meta GraphQL API) to check
if a business runs Facebook/Instagram ads.

Usage:
  python3 scripts/meta-ads-check.py "Business Name"
  python3 scripts/meta-ads-check.py "Business Name" US

Output: JSON with page_name, page_id, has_ads, ad_count, category
"""

import sys
import json

# Add local pip packages
sys.path.insert(0, '/Users/fardeenchoudhury/Library/Python/3.9/lib/python/site-packages')

import warnings
warnings.filterwarnings('ignore')

# Suppress MetaAdsCollector's info messages
import logging
logging.disable(logging.WARNING)

from meta_ads_collector import MetaAdsCollector

def check_business(name, country='US'):
    try:
        with MetaAdsCollector(rate_limit_delay=1.0, jitter=0.5) as collector:
            # Step 1: Find the page
            pages = collector.search_pages(name, country=country)
            if not pages:
                return {'found': False, 'business': name, 'error': 'no_page'}

            page = pages[0]
            result = {
                'found': True,
                'business': name,
                'page_name': page.page_name,
                'page_id': page.page_id,
                'page_alias': page.page_alias or '',
                'category': page.category or '',
                'likes': page.page_like_count or 0,
                'verified': page.page_verified or '',
            }

            # Step 2: Check for active ads
            try:
                ads = list(collector.search(page_ids=[page.page_id], country=country, max_results=5))
                result['ad_count'] = len(ads)
                result['has_active_ads'] = any(getattr(a, 'is_active', False) for a in ads)

                # Get first ad details
                if ads:
                    a = ads[0]
                    result['first_ad'] = {
                        'id': getattr(a, 'id', ''),
                        'active': getattr(a, 'is_active', False),
                        'platforms': getattr(a, 'publisher_platforms', []),
                    }
                    if a.creatives:
                        c = a.creatives[0]
                        result['first_ad']['body'] = (getattr(c, 'body', '') or '')[:200]
                        result['first_ad']['title'] = (getattr(c, 'title', '') or '')[:100]
            except Exception as e:
                result['ad_count'] = 0
                result['has_active_ads'] = False
                result['ad_error'] = str(e)[:100]

            return result
    except Exception as e:
        return {'found': False, 'business': name, 'error': str(e)[:200]}

if __name__ == '__main__':
    name = sys.argv[1] if len(sys.argv) > 1 else ''
    country = sys.argv[2] if len(sys.argv) > 2 else 'US'

    if not name:
        print(json.dumps({'error': 'Usage: python3 meta-ads-check.py "Business Name" [country]'}))
        sys.exit(1)

    result = check_business(name, country)
    print(json.dumps(result))
