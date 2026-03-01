#!/bin/bash
cd /Users/fardeenchoudhury/mortar-lead-scraper
exec script -q /dev/null node scripts/marathon-scrape.js 2>&1 | tee data/logs/marathon-scrape.log
