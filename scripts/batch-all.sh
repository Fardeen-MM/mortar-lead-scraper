#!/bin/bash
# Master batch: US (states + deep) → Canada → UK → Australia → Ireland
# Usage: nohup bash scripts/batch-all.sh > /tmp/batch-all-lawfirms.log 2>&1 &

echo "=== STARTING FULL LAW FIRM SCRAPE ==="
echo "$(date): US states..."

node scripts/batch-states.js --concurrency 3 --resume

echo ""
echo "$(date): US deep (106 cities)..."

node scripts/batch-deep-us.js --concurrency 3 --resume

echo ""
echo "$(date): Canada provinces..."

node scripts/batch-canada.js --concurrency 3 --resume

echo ""
echo "$(date): UK cities..."

node scripts/batch-uk.js --concurrency 3 --resume

echo ""
echo "$(date): Australia..."

node scripts/batch-australia.js --concurrency 3 --resume

echo ""
echo "$(date): Ireland..."

node scripts/batch-ireland.js --concurrency 3 --resume

echo ""
echo "=== ALL REGIONS COMPLETE ==="
echo "$(date)"

# Merge all CSVs into one master file
echo "Merging CSVs..."

# Get header from first available CSV
header=""
for f in output/law-firms_*.csv output/solicitors_*.csv; do
  if [ -f "$f" ]; then
    header=$(head -1 "$f")
    break
  fi
done

if [ -n "$header" ]; then
  echo "$header" > output/ALL-LAWFIRMS-MASTER.csv
  for f in output/law-firms_*.csv output/solicitors_*.csv; do
    [ -f "$f" ] && tail -n +2 "$f" >> output/ALL-LAWFIRMS-MASTER.csv
  done
  total=$(wc -l < output/ALL-LAWFIRMS-MASTER.csv)
  echo "Master CSV: output/ALL-LAWFIRMS-MASTER.csv ($((total - 1)) leads)"
else
  echo "No CSV files found to merge."
fi

# Per-region master CSVs
for region in US CANADA UK AUSTRALIA IRELAND; do
  case $region in
    US) pattern="output/law-firms_*_us*.csv output/law-firms_*_{al,ak,az,ar,ca,co,ct,de,fl,ga,hi,id,il,in,ia,ks,ky,la,me,md,ma,mi,mn,ms,mo,mt,ne,nv,nh,nj,nm,ny,nc,nd,oh,ok,or,pa,ri,sc,sd,tn,tx,ut,vt,va,wa,wv,wi,wy,dc}*.csv" ;;
    CANADA) pattern="output/law-firms_*_canada*.csv output/law-firms_*_{toronto,vancouver,calgary,montreal,winnipeg,saskatoon,halifax,fredericton,st-john,charlottetown,yellowknife,whitehorse,ottawa,victoria,edmonton,quebec}*.csv" ;;
    UK) pattern="output/solicitors_*.csv" ;;
    AUSTRALIA) pattern="output/law-firms_*_australia*.csv output/law-firms_*_{sydney,melbourne,brisbane,perth,adelaide,hobart,canberra,darwin,gold-coast,sunshine-coast,newcastle,wollongong,geelong,townsville,cairns,bunbury,mount-gambier,parramatta}*.csv" ;;
    IRELAND) pattern="output/solicitors_*_ireland*.csv output/solicitors_*_{dublin,cork,galway,limerick,waterford,kilkenny,drogheda,dundalk,sligo,athlone,wexford,tralee,belfast,derry}*.csv" ;;
  esac
done

echo "Done."
