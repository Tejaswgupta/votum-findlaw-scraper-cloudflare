
# Path to project directory
# PROJECT_DIR="$(dirname "$(realpath "$0")")/.."
# cd "$PROJECT_DIR/singapore"

# Activate virtual environment if using one
# source /path/to/your/venv/bin/activate

# Set max pages to check for new cases
MAX_PAGES=10

# Run with logging
echo "Starting Singapore case law scraping at $(date)" >> cron_log.txt
python caselaw_index.py --max-pages $MAX_PAGES >> cron_log.txt 2>&1
echo "Finished at $(date)" >> cron_log.txt
echo "----------------------------------------" >> cron_log.txt 
#!/bin/bash