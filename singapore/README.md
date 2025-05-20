# Singapore Case Law Scraper

## Setting up as a Cron Job

The scraper has been updated to only fetch new cases by starting from the first pages (where new content appears) and implementing proper URL checking to skip already processed cases.

### Create a cron job script

Create a file named `cron_scrape.sh` with the following content:

```bash
#!/bin/bash

# Path to project directory
PROJECT_DIR="/path/to/votum-findlaw-scraper-cloudflare"
cd "$PROJECT_DIR/singapore"

# Activate virtual environment if using one
# source /path/to/your/venv/bin/activate

# Set max pages to check for new cases
MAX_PAGES=10

# Run with logging
echo "Starting Singapore case law scraping at $(date)" >> cron_log.txt
python caselaw_index.py --max-pages $MAX_PAGES >> cron_log.txt 2>&1
echo "Finished at $(date)" >> cron_log.txt
echo "----------------------------------------" >> cron_log.txt
```

Make it executable:
```
chmod +x cron_scrape.sh
```

### Add to crontab

Run `crontab -e` and add a line like this to run the scraper daily at 1 AM:

```
0 1 * * * /path/to/votum-findlaw-scraper-cloudflare/singapore/cron_scrape.sh
```

## How It Works

1. The script now starts from page 1 (newest cases) instead of page 340
2. It checks if URLs have already been processed by querying the database
3. It stops after:
   - Reaching the maximum number of pages specified (default: 10)
   - Finding 3 consecutive pages with no new cases (indicating it has caught up)
4. It logs the number of new cases found in each run

## Parameters

You can adjust these parameters in the `cron_scrape.sh` file:

- `MAX_PAGES`: Maximum number of pages to check for new cases (default: 10)

And these parameters in the `caselaw_index.py` file:

- `CONFIG["maxEntriesPerPage"]`: Maximum entries to process per page
- `CONFIG["requestInterval"]`: Delay between page requests 