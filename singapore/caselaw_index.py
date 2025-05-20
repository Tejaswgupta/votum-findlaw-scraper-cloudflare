import argparse
import json
import os
import re
import time

import requests
from dotenv import load_dotenv
from utils.cron_tracker import complete_job, fail_job, start_job

from supabase import create_client

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# URL for the Singapore Cloudflare worker
CLOUDFLARE_URL = "https://votum-scraper-singapore.tejasw.workers.dev/api"

# Configuration parameters
CONFIG = {
    "maxPages": 10,  # Default limit for cron job to check for new cases
    "maxEntriesPerPage": 10,  # Maximum number of entries to process per page
    "filterBatchSize": 10000,  # Size of batches for filtering URLs
    "requestInterval": 1,  # Delay in seconds between pages
}


def sleep(seconds):
    """Simple sleep function"""
    time.sleep(seconds)


def fetch_with_retry(url, retries=5, delay_sec=2):
    """Fetch a URL with retry logic"""
    for i in range(retries):
        response = requests.get(url)
        if response.ok:
            return response
        elif i < retries - 1:
            sleep(delay_sec)

    return {"url": url, "status": "error", "error": "Failed after multiple retries"}


def check_if_citation_exists(citation):
    """
    Check if a case with the given citation already exists in the database

    Args:
        citation: The citation to check
    Returns:
        True if the citation exists, False otherwise
    """
    if not citation:
        return False

    try:
        response = (
            supabase.table("caselaw_singapore")
            .select("id")
            .eq("citation", citation)
            .execute()
        )

        return bool(response.data)
    except Exception as e:
        print(f"Error checking citation status: {e}")
        return False


def insert_case_law(case_data, source_url):
    """
    Insert Singapore case law data into the caselaw_singapore table

    Args:
        case_data: The case data to insert
        source_url: The URL from which the case was scraped
    Returns:
        The ID of the case record
    """
    try:
        # Check if the citation already exists
        if case_data.get("citation"):
            citation_exists = check_if_citation_exists(case_data["citation"])
            if citation_exists:
                print(f"Skipping case with existing citation: {case_data['citation']}")
                return None

        # Standardize court name
        standardized_court = standardize_court_name(case_data.get("court_name"))

        if not case_data.get("case_text") or len(case_data["case_text"].strip()) == 0:
            print(f"Skipping case with empty text: {case_data['case_name']}")
            return None

        # Insert the case data with standardized court name
        response = (
            supabase.table("caselaw_singapore")
            .insert(
                {
                    "standard_court_name": standardized_court,
                    "case_name": case_data.get("case_name"),
                    "case_no": case_data.get("case_no"),
                    "date": case_data.get("date"),
                    "case_text": case_data.get("case_text"),
                    "citation": case_data.get("citation"),
                    "country": "Singapore",
                }
            )
            .execute()
        )
        if not response.data:
            raise Exception("No data returned from insert operation")

        case_id = response.data[0]["id"]
        print(f"Inserted case: {case_data['case_name']} (ID: {case_id})")

        # Record that this URL has been processed
        supabase.table("caselaw_scraping_urls").upsert(
            {
                "url": source_url,
                "case_id": case_id,
                "processed": True,
                "processing_date": str(time.strftime("%Y-%m-%d %H:%M:%S")),
                "status": "success",
                "country": "Singapore",
            },
            on_conflict="url",
        ).execute()

        return case_id

    except Exception as e:
        # Record the error for this URL
        supabase.table("caselaw_scraping_urls").upsert(
            {
                "url": source_url,
                "processed": False,
                "processing_date": str(time.strftime("%Y-%m-%d %H:%M:%S")),
                "status": "error",
                "error_message": str(e),
                "country": "Singapore",
            },
            on_conflict="url",
        ).execute()

        print(f"Error inserting Singapore case law: {e}")
        raise e


def check_if_url_processed(url):
    """
    Check if a URL has been processed

    Args:
        url: The URL to check
    Returns:
        True if the URL has been processed, False otherwise
    """
    try:
        response = (
            supabase.table("caselaw_scraping_urls")
            .select("processed")
            .eq("url", url)
            .eq("processed", True)
            .execute()
        )

        return bool(response.data)
    except Exception as e:
        print(f"Error checking URL status: {e}")
        return False


def scrape_singapore_case_laws(max_pages=None):
    """
    Main function to scrape Singapore case laws

    Args:
        max_pages: Maximum number of pages to process, overrides CONFIG setting
    """
    job_name = "singapore_caselaw_scraper"
    new_cases_found = 0
    page_index = 1  # Start from page 1 for new cases
    pages_processed_count = 0  # Renamed from page_index to avoid confusion
    job_run_id = None

    try:
        # Start job tracking
        job_run_id, success = start_job(supabase, job_name)
        if not success:
            print("Warning: Failed to start job tracking. Continuing without tracking.")

        has_more_pages = True
        consecutive_pages_with_no_new_cases = 0

        # Use provided max_pages if specified, otherwise use CONFIG
        max_pages_to_process = max_pages or CONFIG["maxPages"]  # Renamed from max_pages

        # Process pages until no more results or safety limit reached
        while (
            has_more_pages and page_index <= max_pages_to_process
        ):  # Use renamed max_pages_to_process
            current_page_processed = False
            print(f"Processing page {page_index}...")

            # Get list of case URLs for current page
            url = f"{CLOUDFLARE_URL}/sitemap/cases?index={page_index}"
            print(url)

            cases_response = requests.get(url)
            if not cases_response.ok:
                print(
                    f"Failed to fetch cases for page {page_index}: {cases_response.status_code}"
                )
                has_more_pages = False
                pages_processed_count = page_index - 1  # Record actual pages attempted
                break

            case_urls = cases_response.json()
            if not case_urls or not len(case_urls):
                print(f"No cases found on page {page_index}, stopping pagination")
                has_more_pages = False
                pages_processed_count = page_index - 1  # Record actual pages attempted
                break

            print(f"Found {len(case_urls)} cases on page {page_index}")
            current_page_processed = True

            # Limit the number of cases processed per page if needed
            cases_to_process = case_urls[: CONFIG["maxEntriesPerPage"]]

            # Check which URLs have already been processed
            urls_to_process = []
            for url in cases_to_process:
                is_processed = check_if_url_processed(url)
                if not is_processed:
                    urls_to_process.append(url)

            print(f"{len(urls_to_process)} cases need processing on page {page_index}")

            # If no new cases on this page, increment the counter
            if not urls_to_process:
                consecutive_pages_with_no_new_cases += 1
                # If we've seen 3 consecutive pages with no new cases, assume we've caught up
                if consecutive_pages_with_no_new_cases >= 3:
                    print("Found 3 consecutive pages with no new cases. Stopping.")
                    break
                # Continue to next page
                page_index += 1
                pages_processed_count = page_index - 1  # Update before sleep/continue
                sleep(CONFIG["requestInterval"])
                continue
            else:
                # Reset the counter if we found new cases
                consecutive_pages_with_no_new_cases = 0

            # Process each URL
            results = []
            for url in urls_to_process:
                try:
                    # First try without isOld parameter
                    full_url = f"{CLOUDFLARE_URL}/scrape/cases?url={requests.utils.quote(f'https://www.elitigation.sg{url}')}"
                    print(f"First attempt: {full_url}")

                    response = fetch_with_retry(full_url)

                    if isinstance(response, requests.Response) and response.ok:
                        data = response.json()

                        # If case_text is blank, try again with isOld=true
                        if (
                            not data.get("case_text")
                            or len(data.get("case_text", "").strip()) == 0
                        ):
                            print(
                                f"Blank case_text found, retrying with isOld=true for: {url}"
                            )
                            full_url = f"{CLOUDFLARE_URL}/scrape/cases?url={requests.utils.quote(f'https://www.elitigation.sg{url}')}&isOld=true"
                            print(f"Second attempt: {full_url}")

                            response = fetch_with_retry(full_url)
                            if isinstance(response, requests.Response) and response.ok:
                                data = response.json()

                        data["url"] = url
                        results.append(data)
                    else:
                        error_msg = (
                            response.get("error")
                            if isinstance(response, dict)
                            else f"HTTP error! Status: {response.status_code}"
                        )
                        results.append(
                            {"url": url, "status": "error", "error": error_msg}
                        )
                except Exception as e:
                    results.append({"url": url, "status": "error", "error": str(e)})

            # Store results in database
            for result in results:
                if result.get("status") == "error":
                    print(f"Error scraping {result['url']}: {result['error']}")
                    continue

                try:
                    case_id = insert_case_law(result, result["url"])
                    if case_id:
                        new_cases_found += 1
                except Exception as e:
                    print(f"Failed to insert case from {result['url']}: {e}")

            # Add a delay between pages
            if page_index < max_pages_to_process:
                sleep(CONFIG["requestInterval"])

            page_index += 1
            if (
                current_page_processed
            ):  # Only increment if the page was actually processed (not skipped early)
                pages_processed_count = page_index - 1

        print(
            f"Singapore case law scraping completed. Found {new_cases_found} new cases. Processed {pages_processed_count} pages."
        )

        # Job completed successfully
        complete_job(
            supabase,
            job_run_id,
            {
                "new_cases_found": new_cases_found,
                "pages_processed": pages_processed_count,
            },
        )

    except Exception as e:
        error_message = str(e)
        print(f"Fatal error in Singapore case law scraping: {e}")

        # Log the failure
        fail_job(
            supabase,
            job_run_id,
            {
                "new_cases_found": new_cases_found,
                "pages_processed": pages_processed_count,
            },
            error_message,
        )

        raise e  # Re-raise the exception after logging


def test_cloudflare_api():
    """
    Test the Cloudflare worker API for Singapore case law.
    Fetches the first page and prints the number of results and a sample URL.
    """
    test_url = f"{CLOUDFLARE_URL}/sitemap/cases?index=1"
    print(f"Testing API: {test_url}")
    response = requests.get(test_url)
    if not response.ok:
        print(f"API request failed with status code: {response.status_code}")
        return
    data = response.json()
    print(f"Number of case URLs returned: {len(data)}")
    if data:
        print(f"Sample case URL: {data[0]}")
    else:
        print("No case URLs returned.")


def test_fetch_case_data_for_first_10():
    """
    Test fetching actual case data for the first 10 cases from the API.
    Prints a summary for each case.
    """
    test_url = f"{CLOUDFLARE_URL}/sitemap/cases?index=1"
    print(f"Testing API: {test_url}")
    response = requests.get(test_url)
    if not response.ok:
        print(f"API request failed with status code: {response.status_code}")
        return
    case_urls = response.json()
    print(f"Number of case URLs returned: {len(case_urls)}")
    for i, case_url in enumerate(case_urls[:10]):
        print(f"\nFetching case {i+1}: {case_url}")
        full_url = f"{CLOUDFLARE_URL}/scrape/cases?url={requests.utils.quote('https://www.elitigation.sg' + case_url)}"
        case_response = requests.get(full_url)
        if not case_response.ok:
            print(f"  Failed to fetch case data (status {case_response.status_code})")
            continue
        data = case_response.json()
        print(f"  Case Name: {data.get('case_name')}")
        print(f"  Citation: {data.get('citation')}")
        text_snippet = (data.get("case_text") or "")[:200].replace("\n", " ")
        print(f"  Text Snippet: {text_snippet}...")


def standardize_court_name(raw_court):
    """Standardize court names and filter for binding precedents only"""
    if not raw_court:
        return None

    # Clean the input
    cleaned = re.sub(
        r"\$\$.*?\$\$|&emsp;|fig\.\s*\d*|\d+", "", raw_court
    )  # Remove citations and special chars
    cleaned = re.sub(r"\s+", " ", cleaned).strip()  # Normalize whitespace
    cleaned = cleaned.lower()

    # Define binding court patterns with standardized names
    court_patterns = {
        r"court\s+of\s+appeal": "Court of Appeal",
        r"high\s+court\s+appellate\s+division": "High Court (Appellate Division)",
        r"high\s+court\s+general\s+division": "High Court (General Division)",
        r"singapore\s+international\s+commercial\s+court": "Singapore International Commercial Court (SICC)",
        r"family\s+justice\s+courts": "Family Justice Courts",
        r"court\s+of\s+three\s+judges|court\s+of\s+3\s+judges": "Court of Three Judges",
        r"high\s+court": "High Court",
    }

    # Try to match against patterns
    for pattern, standard_name in court_patterns.items():
        if re.search(pattern, cleaned):
            return standard_name

    return None  # Not a binding court


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Singapore case laws")
    parser.add_argument(
        "--max-pages", type=int, help="Maximum number of pages to check for new cases"
    )
    parser.add_argument(
        "--test-api",
        action="store_true",
        help="Test the Cloudflare worker API and exit",
    )
    parser.add_argument(
        "--test-cases",
        action="store_true",
        help="Test fetching actual case data for 10 cases and exit",
    )
    args = parser.parse_args()

    if args.test_api:
        test_cloudflare_api()
    elif args.test_cases:
        test_fetch_case_data_for_first_10()
    else:
        print("Starting Singapore case law scraping process...")
        scrape_singapore_case_laws(args.max_pages)
        print("Singapore case law scraping process finished")
