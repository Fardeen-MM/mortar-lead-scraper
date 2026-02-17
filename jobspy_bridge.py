import sys, json
from jobspy import scrape_jobs

search_term = sys.argv[1]
location = sys.argv[2] if len(sys.argv) > 2 else "United States"
hours_old = int(sys.argv[3]) if len(sys.argv) > 3 else 24

try:
    jobs = scrape_jobs(
        site_name=["indeed"],
        search_term=search_term,
        location=location,
        results_wanted=50,
        hours_old=hours_old,
        country_indeed="USA"
    )
    results = []
    for _, job in jobs.iterrows():
        results.append({
            "title": str(job.get("title", "")),
            "company": str(job.get("company_name", "")),
            "city": str(job.get("city", "")),
            "state": str(job.get("state", "")),
            "date_posted": str(job.get("date_posted", "")),
            "job_url": str(job.get("job_url", "")),
            "description": str(job.get("description", ""))[:300],
        })
    print(json.dumps(results))
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)
