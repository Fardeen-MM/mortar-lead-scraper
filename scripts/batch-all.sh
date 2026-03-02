#!/bin/bash
# Master batch: US → Canada → UK law firms
# Usage: nohup bash scripts/batch-all.sh > /tmp/batch-all-lawfirms.log 2>&1 &

echo "=== STARTING FULL LAW FIRM SCRAPE ==="
echo "$(date): US states..."

node scripts/batch-states.js --concurrency 3 --resume

echo ""
echo "$(date): Canada provinces..."

node scripts/batch-canada.js --concurrency 3 --resume

echo ""
echo "$(date): UK cities..."

node scripts/batch-uk.js --concurrency 3 --resume

echo ""
echo "=== ALL REGIONS COMPLETE ==="
echo "$(date)"

# Merge all CSVs into one master file
echo "Merging CSVs..."
head -1 output/law-firms_*.csv 2>/dev/null | head -1 > output/ALL-LAWFIRMS-MASTER.csv
for f in output/law-firms_*.csv output/solicitors_*.csv; do
  [ -f "$f" ] && tail -n +2 "$f" >> output/ALL-LAWFIRMS-MASTER.csv
done

total=$(wc -l < output/ALL-LAWFIRMS-MASTER.csv)
echo "Master CSV: output/ALL-LAWFIRMS-MASTER.csv ($((total - 1)) leads)"
