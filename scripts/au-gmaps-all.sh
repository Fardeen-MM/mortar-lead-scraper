#!/bin/bash
# Run all 6 niches sequentially (each gets its own Node process for clean memory)
cd /Users/fardeenchoudhury/mortar-lead-scraper

NICHES=("real estate agent" "dentist" "physiotherapist" "accountant" "plumber")

echo "=== Starting AU Google Maps scrape: ${#NICHES[@]} niches ==="
echo "Start time: $(date)"

for niche in "${NICHES[@]}"; do
  slug=$(echo "$niche" | tr ' ' '-')
  echo ""
  echo ">>> Starting: $niche ($(date))"
  node scripts/au-gmaps-single.js "$niche" 2>&1
  echo ">>> Finished: $niche ($(date))"
  echo ""
  # Brief pause between niches
  sleep 5
done

echo ""
echo "=== ALL NICHES COMPLETE ==="
echo "End time: $(date)"
echo ""
echo "Generated CSVs:"
ls -la output/gmaps-au-*.csv 2>/dev/null
