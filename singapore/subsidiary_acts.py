import json
import logging
import os
import re
import time  # For potential delays
from urllib.parse import urljoin, urlparse  # Keep urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import Client, create_client

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Supabase Setup ---
load_dotenv()  # Load .env file if it exists

supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logging.error(
        "Supabase URL or Key not found. Please set SUPABASE_URL and SUPABASE_KEY environment variables."
    )
    # Provide fallback defaults ONLY if absolutely necessary for testing, otherwise exit.
    # supabase_url = supabase_url or 'YOUR_FALLBACK_URL'
    # supabase_key = supabase_key or 'YOUR_FALLBACK_KEY'
    # logging.warning("Using fallback Supabase credentials. Ensure this is intended.")
    exit(1)  # Exit if credentials aren't properly set

try:
    supabase: Client = create_client(supabase_url, supabase_key)
    logging.info("Supabase client created successfully.")
except Exception as e:
    logging.error(f"Failed to create Supabase client: {e}")
    exit(1)

# --- Supabase Helper Functions ---


def get_existing_act(source_id: str, supabase_client: Client) -> dict | None:
    """
    Check if an act (main or subsidiary) already exists by its source_id.
    """
    if not source_id:
        logging.warning("Attempted to check for existing act with empty source_id.")
        return None
    try:
        # logging.debug(f"Checking if act with source_id '{source_id}' exists...")
        response = (
            supabase_client.table("acts")
            .select("act_id, source_id")
            .eq("source_id", source_id)
            .limit(1)
            .execute()
        )

        if hasattr(response, "data") and response.data:
            # logging.debug(f"Act with source_id '{source_id}' found with act_id: {response.data[0]['act_id']}")
            return response.data[0]
        # logging.debug(f"Act with source_id '{source_id}' not found.")
        return None
    except Exception as e:
        logging.error(
            f"Error checking for existing act with source_id '{source_id}': {e}"
        )
        return None


def get_act_id_by_source_id(source_id: str, supabase_client: Client) -> int | None:
    """
    Get the act_id for a given source_id. Used to find the parent_id for SL.
    """
    if not source_id:
        logging.warning("Attempted to get act_id with empty source_id.")
        return None
    logging.debug(f"Looking up parent act_id for source_id: {source_id}")
    existing_act = get_existing_act(source_id, supabase_client)
    if existing_act:
        parent_id = existing_act.get("act_id")
        logging.debug(f"Found parent act_id: {parent_id} for source_id: {source_id}")
        return parent_id
    else:
        logging.warning(f"Parent act with source_id '{source_id}' not found in DB.")
        return None


def get_existing_sections(act_id: int, supabase_client: Client) -> list:
    """
    Get existing section titles for a specific act_id.
    """
    if not act_id:
        logging.warning("Attempted to get existing sections with invalid act_id.")
        return []
    try:
        # logging.debug(f"Fetching existing sections for act_id {act_id}...")
        response = (
            supabase_client.table("sections")
            .select("section_title")
            .eq("act_id", act_id)
            .execute()
        )

        if hasattr(response, "data") and response.data:
            # logging.debug(f"Found {len(response.data)} existing sections for act_id {act_id}.")
            return response.data
        # logging.debug(f"No existing sections found for act_id {act_id}.")
        return []
    except Exception as e:
        logging.error(f"Error fetching existing sections for act_id {act_id}: {e}")
        return []


def store_in_supabase(
    act_data: dict, all_sections_data: list, supabase_client: Client
) -> bool:
    """
    Inserts Subsidiary Legislation (SL) and its Sections into Supabase,
    handling existing data and parent_id linkage.
    """
    inserted_act_id = None
    sl_source_id = act_data.get("source_id")
    sl_name_for_logs = act_data.get("act_name", f"Unknown SL ({sl_source_id})")

    logging.info(f"--- Starting Supabase upsert process for SL: {sl_name_for_logs} ---")

    if not sl_source_id:
        logging.error(
            "SL data is missing 'source_id'. Cannot proceed with Supabase operation."
        )
        return False

    # 1. Check if SL already exists by source_id
    existing_sl = get_existing_act(sl_source_id, supabase_client)

    if existing_sl:
        inserted_act_id = existing_sl["act_id"]
        logging.info(
            f"SL '{sl_name_for_logs}' (source_id: {sl_source_id}) already exists with act_id: {inserted_act_id}."
        )
        # Optional: Update existing SL metadata if needed (e.g., description, name)
        # Consider implementing an update mechanism if required.
        # try:
        #     update_data = {k: v for k, v in act_data.items() if k != 'source_id' and v is not None}
        #     # Ensure parent_id isn't accidentally removed if not in update_data but exists
        #     if 'parent_id' not in update_data and existing_sl.get('parent_id'):
        #          update_data['parent_id'] = existing_sl.get('parent_id')
        #     if update_data:
        #         supabase_client.table('acts').update(update_data).eq('act_id', inserted_act_id).execute()
        #         logging.info(f"Updated metadata for existing SL act_id: {inserted_act_id}")
        # except Exception as e:
        #     logging.error(f"Error updating metadata for existing SL act_id {inserted_act_id}: {e}")

    else:
        # 2. Insert new SL
        try:
            # Prepare SL data: Remove None values unless column allows NULL
            sl_to_insert = {
                k: v
                for k, v in act_data.items()
                # Ensure parent_id is included, even if None
                if v is not None
                or k in ["act_description", "source", "source_id", "parent_id"]
            }
            if not sl_to_insert.get("act_name") or not sl_to_insert.get("country"):
                logging.error("Error: SL name and country are required for insertion.")
                return False
            # Ensure parent_id is handled correctly (NULL if not found/provided)
            sl_to_insert["parent_id"] = sl_to_insert.get(
                "parent_id"
            )  # Explicitly set to None if not present

            logging.info(f"Attempting to insert SL: {sl_name_for_logs}")
            logging.debug(
                f"SL data for insertion: {json.dumps(sl_to_insert, indent=2)}"
            )
            sl_insert_response = (
                supabase_client.table("acts").insert(sl_to_insert).execute()
            )

            if hasattr(sl_insert_response, "data") and sl_insert_response.data:
                inserted_act_id = sl_insert_response.data[0]["act_id"]
                logging.info(
                    f"Successfully inserted SL '{sl_name_for_logs}', got act_id: {inserted_act_id}"
                )
            else:
                error_message = "Unknown error during SL insertion."
                if hasattr(sl_insert_response, "error") and sl_insert_response.error:
                    error_message = sl_insert_response.error.message
                elif hasattr(sl_insert_response, "message"):
                    error_message = sl_insert_response.message
                logging.error(
                    f"Error inserting SL '{sl_name_for_logs}': {error_message}"
                )
                logging.debug(f"Full SL Insert Response: {sl_insert_response}")
                # If parent_id constraint fails, it might show up here
                if (
                    "violates foreign key constraint" in error_message
                    and "parent_id" in error_message
                ):
                    logging.error(
                        f"Potential issue: The parent_id ({act_data.get('parent_id')}) might not exist in the acts table."
                    )
                return False

        except Exception as e:
            logging.error(
                f"An exception occurred during SL insertion for '{sl_name_for_logs}': {e}"
            )
            return False

    # 3. Handle Sections (Insert new ones)
    if not inserted_act_id:
        logging.error(
            f"Cannot insert sections as act_id was not obtained for '{sl_name_for_logs}'."
        )
        return False

    # Get existing section titles for this act_id to avoid duplicates
    existing_sections = get_existing_sections(inserted_act_id, supabase_client)
    existing_section_titles = {
        section["section_title"] for section in existing_sections
    }
    logging.info(
        f"Found {len(existing_section_titles)} existing sections for SL act_id {inserted_act_id}."
    )

    # Filter out sections that already exist based on title
    new_sections_data = [
        section
        for section in all_sections_data
        if section.get("section_title") not in existing_section_titles
    ]

    if not new_sections_data:
        logging.info(
            f"No new sections found to insert for SL act_id {inserted_act_id}."
        )
        return True  # SL exists or was inserted, no new sections needed

    logging.info(
        f"Found {len(new_sections_data)} new sections to insert for SL act_id {inserted_act_id}."
    )

    # Prepare sections for batch insertion
    sections_to_insert = []
    for section in new_sections_data:
        section["act_id"] = inserted_act_id  # Link section to the SL

        # Prepare section data: Remove None values unless column allows NULL
        section_prepared = {
            k: v
            for k, v in section.items()
            if v is not None or k in ["questions", "cot_pairs", "additional"]
        }

        # Basic validation for required fields
        if not all(
            key in section_prepared
            for key in ["section_title", "section_content", "act_id", "country"]
        ):
            logging.warning(
                f"Skipping section due to missing required fields: {section_prepared.get('section_title', 'NO TITLE')} for SL act_id {inserted_act_id}"
            )
            continue

        sections_to_insert.append(section_prepared)

    if not sections_to_insert:
        logging.warning(
            f"All {len(new_sections_data)} potential new sections were filtered out due to missing data for SL act_id {inserted_act_id}."
        )
        return True  # SL part was successful

    # Batch insert sections
    try:
        logging.info(
            f"Attempting to insert {len(sections_to_insert)} new sections linked to SL act_id {inserted_act_id}..."
        )
        batch_size = 100  # Adjust as needed
        all_sections_inserted = True

        for i in range(0, len(sections_to_insert), batch_size):
            batch = sections_to_insert[i : i + batch_size]
            logging.info(
                f"  Inserting section batch {i // batch_size + 1} ({len(batch)} sections)..."
            )
            sections_insert_response = (
                supabase_client.table("sections").insert(batch).execute()
            )

            if not (
                hasattr(sections_insert_response, "data")
                and sections_insert_response.data
            ):
                error_message = f"Unknown error during Section batch insertion (Batch {i // batch_size + 1})."
                if (
                    hasattr(sections_insert_response, "error")
                    and sections_insert_response.error
                ):
                    error_message = sections_insert_response.error.message
                elif hasattr(sections_insert_response, "message"):
                    error_message = sections_insert_response.message
                logging.error(
                    f"Error inserting Sections for SL act_id {inserted_act_id}: {error_message}"
                )
                logging.debug(
                    f"Full Section Insert Response: {sections_insert_response}"
                )
                all_sections_inserted = False
                break  # Stop inserting further batches on error

        if all_sections_inserted:
            logging.info(
                f"Successfully inserted all {len(sections_to_insert)} new sections for SL act_id {inserted_act_id}."
            )
            return True
        else:
            logging.error(
                f"Section insertion failed for one or more batches for SL act_id {inserted_act_id}."
            )
            return False  # Indicate partial failure

    except Exception as e:
        logging.error(
            f"An exception occurred during Section insertion for SL act_id {inserted_act_id}: {e}"
        )
        return False


# --- Configuration ---
BASE_URL = "https://sso.agc.gov.sg"

# --- Request Headers (Base) ---
base_headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",  # More typical browser accept
    "accept-language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',  # Example
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',  # Example
    "sec-fetch-dest": "document",  # Usually 'document' for initial page load
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",  # Or 'none' if coming from elsewhere initially
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",  # Example UA
    # 'x-requested-with': 'XMLHttpRequest', # Removed - typically not needed for direct page loads
}


# --- Helper Function: Extract Sections from HTML Fragment ---
# (No changes needed from previous version, already handles SL structure)
def extract_sections_from_html(html_content: str, sl_path_for_logs: str) -> list:
    """Parses HTML content from an SL page and extracts section data."""
    sections = []
    if not html_content:
        logging.warning(
            f"[{sl_path_for_logs}] No HTML content provided to extract_sections_from_html."
        )
        return sections

    try:
        soup = BeautifulSoup(html_content, "lxml")  # Use lxml for speed

        # Remove amendment notes first
        for amendNote in soup.find_all(class_="amendNote"):
            amendNote.decompose()

        # Find the main content container for SL pages
        sections_container = soup.find("div", id="legisContent")

        if not sections_container:
            logging.warning(
                f"[{sl_path_for_logs}] Could not find 'div#legisContent'. Trying body."
            )
            sections_container = soup.find(
                "div", class_="body-content"
            )  # Broader fallback
            if not sections_container:
                logging.warning(
                    f"[{sl_path_for_logs}] Could not find content container. Trying entire soup."
                )
                sections_container = soup  # Last resort

        # Find section divs (prov1 seems consistent) and schedules
        section_divs = sections_container.find_all("div", class_=re.compile(r"prov\d+"))
        schedule_divs = sections_container.find_all("div", class_="schedule")

        if not section_divs and not schedule_divs:
            logging.warning(
                f"[{sl_path_for_logs}] No 'div.provX' or 'div.schedule' elements found within the content."
            )
            return sections

        all_prov_elements = section_divs + schedule_divs

        for element in all_prov_elements:
            section = {}
            additional_info = {}
            header_tag = None
            content_tag = None
            title_text_only = "Unknown Title"
            header_id = None

            # Adapt selectors based on element type
            element_classes = element.get("class", [])
            is_prov = any("prov" in cls for cls in element_classes)
            is_schedule = "schedule" in element_classes

            if is_prov:
                header_tag = element.find(
                    ["td", "div"], class_=re.compile(r"prov\d+Hdr")
                )
                content_tag = element.find(
                    ["td", "div"], class_=re.compile(r"prov\d+Txt")
                )
                if not content_tag:
                    content_tag = element  # Fallback if no specific Txt tag
                if header_tag:
                    title_text_only = header_tag.get_text(strip=True)
                if header_tag:
                    header_id = header_tag.get("id")

            elif is_schedule:
                header_tag = element.find(
                    ["td", "div", "p"], class_=re.compile(r"(sHdr|scHdr)")
                )
                content_tag = element  # Use the whole schedule div
                if header_tag:
                    title_text_only = header_tag.get_text(strip=True)
                if header_tag:
                    header_id = header_tag.get("id")
                # Prepend "Schedule" to title if not already there
                if header_tag and not title_text_only.lower().startswith("schedule"):
                    title_text_only = f"SCHEDULE {title_text_only}"

            else:
                continue  # Skip if neither prov nor schedule

            if not content_tag:
                logging.warning(
                    f"[{sl_path_for_logs}] Could not find content tag for element: {title_text_only[:50]}..."
                )
                continue

            # --- Title and Number Extraction ---
            section_number_text = ""
            full_title = title_text_only.strip()

            # Try extracting number from <strong> tag inside content (common pattern)
            number_tag = content_tag.find("strong")
            if number_tag:
                potential_number = number_tag.get_text(strip=True)
                if re.match(
                    r"^\d+[A-Z]?\.?$", potential_number
                ):  # Match "1." or "2A." etc.
                    section_number_text = potential_number
                    # Clean title: remove number if present, then format
                    clean_title = re.sub(
                        r"^" + re.escape(potential_number) + r"\s*", "", full_title
                    ).strip()
                    full_title = (
                        f"Rule {section_number_text} {clean_title}".strip()
                    )  # Use "Rule" for SL? Or keep generic? Let's try Rule.

            # Fallback: Try extracting number from header ID (e.g., id="pr1-")
            if not section_number_text and header_id:
                match_prov = re.match(r"pr(\d+[A-Z]?)-?", header_id)
                if match_prov:
                    section_number_text = match_prov.group(1) + "."
                    full_title = (
                        f"Rule {section_number_text} {title_text_only}"  # Use Rule
                    )
                else:
                    match_sched = re.match(r"Sc(\d+)-", header_id)
                    if match_sched:
                        # Title extracted earlier for schedules should be okay
                        full_title = title_text_only  # Keep SCHEDULE X title

            section["section_title"] = full_title.strip()
            if not section["section_title"]:
                section["section_title"] = "Untitled Section/Schedule"  # Fallback title

            # --- Content Extraction ---
            section_content_raw = content_tag.get_text(separator="\n", strip=True)
            processed_content = re.sub(
                r"\n{3,}", r"\n\n", section_content_raw
            )  # Clean excessive newlines
            # Remove leading number if captured and present at start of content
            if section_number_text:
                escaped_num = re.escape(section_number_text)
                processed_content = re.sub(
                    r"^" + escaped_num + r"[\s\n]*", "", processed_content, count=1
                ).strip()
            # Specific cleanup for "( \n a \n )" pattern
            processed_content = re.sub(
                r"\(\s*\n\s*([a-zA-Z0-9]+)\s*\n\s*\)", r"(\1)", processed_content
            )

            section["section_content"] = processed_content.strip()

            # --- Standard Fields ---
            section["act_id"] = None  # Will be filled later
            section["country"] = "SINGAPORE"
            section["questions"] = None
            section["cot_pairs"] = None

            # --- Additional Info ---
            additional_info["source_element_class"] = element.get("class")
            additional_info["header_id"] = header_id
            section["additional"] = json.dumps(additional_info)

            sections.append(section)

    except Exception as e:
        logging.error(
            f"[{sl_path_for_logs}] Error during HTML parsing in extract_sections_from_html: {e}",
            exc_info=True,
        )

    return sections


# --- Main Scraping Logic for SL ---
def scrape_subsidiary_legislation(
    sl_path: str, session: requests.Session
) -> tuple[dict | None, list | None]:
    """
    Scrapes a single Subsidiary Legislation page from Singapore Statutes Online.

    Args:
        sl_path: The path part of the URL (must start with "/SL/").
        session: The requests Session object.

    Returns:
        Tuple of (sl_data, all_sections_data) or (None, None) on failure.
    """
    if not sl_path or not sl_path.startswith("/SL/"):
        logging.error(
            f"Invalid path provided to scrape_subsidiary_legislation: '{sl_path}'. Path must start with /SL/."
        )
        return None, None

    sl_data = {}
    all_sections_data = []
    target_url = urljoin(BASE_URL, sl_path)

    logging.info(f"--- Starting scrape for SL: {sl_path} ---")
    logging.info(f"Target URL: {target_url}")

    try:
        # 1. Initial Fetch
        headers = base_headers.copy()
        headers["referer"] = BASE_URL  # General referer for initial nav
        response_initial = session.get(
            target_url, headers=headers, timeout=20
        )  # Added timeout
        response_initial.raise_for_status()  # Check for HTTP errors
        soup_initial = BeautifulSoup(response_initial.content, "lxml")
        initial_html_content = response_initial.text  # Use this content directly

        # 2. Extract Metadata and Parent Info
        sl_data["country"] = "SINGAPORE"
        sl_data["source"] = "Singapore Statutes Online (sso.agc.gov.sg)"
        sl_data["source_id"] = sl_path.strip("/").split("/")[-1]  # e.g., "AA2004-R5"
        sl_data["parent_id"] = None  # Initialize

        # Extract SL Title
        title_tag = soup_initial.find("td", class_="slTitle")
        if not title_tag:
            title_div = soup_initial.find("div", class_="legis-title")
            if title_div:
                title_tag = title_div.find("span")  # Try span inside legis-title
        sl_data["act_name"] = (
            title_tag.get_text(strip=True)
            if title_tag
            else f"UNKNOWN SL ({sl_data['source_id']})"
        )
        sl_data["act_description"] = (
            ""  # SL usually doesn't have a separate long description field like Acts
        )

        # Find Parent Act Link and ID
        # NOTE: Parent Act MUST exist in the database first for the foreign key constraint
        parent_act_link = soup_initial.find(
            "a", string=re.compile("Authorising Act", re.IGNORECASE), href=True
        )
        if parent_act_link:
            parent_act_path = parent_act_link["href"]
            # Basic validation of parent path
            if parent_act_path and parent_act_path.startswith("/Act/"):
                parent_source_id = parent_act_path.strip("/").split("/")[-1]
                logging.info(
                    f"[{sl_path}] Found parent Act link: {parent_act_path} (Source ID: {parent_source_id})"
                )
                # Look up parent ID in the database
                parent_act_id = get_act_id_by_source_id(parent_source_id, supabase)
                if parent_act_id:
                    sl_data["parent_id"] = parent_act_id
                    logging.info(f"[{sl_path}] Found parent act_id: {parent_act_id}")
                else:
                    # Critical: If parent doesn't exist, insertion will fail due to FK constraint.
                    # Options: a) Skip SL, b) Insert SL with NULL parent_id, c) Fail script.
                    # Let's log a warning and proceed with NULL parent_id (schema must allow NULL).
                    logging.warning(
                        f"[{sl_path}] Parent Act with source_id '{parent_source_id}' not found in DB. 'parent_id' will be NULL."
                    )
                    sl_data["parent_id"] = None  # Explicitly set to None
            else:
                logging.warning(
                    f"[{sl_path}] Found 'Authorising Act' link, but href '{parent_act_path}' is not a valid Act path."
                )
        else:
            logging.warning(
                f"[{sl_path}] Could not find 'Authorising Act' link. 'parent_id' will be NULL."
            )

        # 3. Extract Sections from the initial HTML
        logging.info(f"[{sl_path}] Extracting sections from initial page content...")
        all_sections_data = extract_sections_from_html(initial_html_content, sl_path)

        logging.info(f"--- Extracted SL Data for {sl_path} ---")
        logging.info(json.dumps(sl_data, indent=2))
        logging.info(
            f"--- Total Sections Extracted for {sl_path}: {len(all_sections_data)} ---"
        )

        if all_sections_data:
            # Sort sections (optional but good practice)
            def sort_key(section):
                title = section.get("section_title", "")
                match = re.search(
                    r"(?:Rule|Section|SCHEDULE)\s*(\d+)([A-Z]*)", title, re.IGNORECASE
                )
                if match:
                    num_part = int(match.group(1))
                    alpha_part = match.group(2).upper()
                    alpha_val = sum(
                        (ord(char) - ord("A") + 1) * (100**i)
                        for i, char in enumerate(reversed(alpha_part))
                    )
                    is_schedule = "schedule" in title.lower()
                    return (1 if is_schedule else 0, num_part, alpha_val)
                return (2, 0, title)  # Fallback sort

            try:
                all_sections_data.sort(key=sort_key)
                logging.info(f"[{sl_path}] Sections sorted successfully.")
                # logging.debug(f"First extracted section:\n{json.dumps(all_sections_data[0], indent=2)}")
            except Exception as sort_e:
                logging.warning(f"[{sl_path}] Could not sort sections: {sort_e}")

        return sl_data, all_sections_data

    except requests.exceptions.Timeout:
        logging.error(f"[{sl_path}] Request timed out accessing {target_url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[{sl_path}] Network request failed: {e}")
    except (
        AttributeError,
        TypeError,
        ValueError,
    ) as e:  # Catch parsing/extraction errors
        logging.error(
            f"[{sl_path}] Data extraction or processing error: {e}", exc_info=True
        )
    except Exception as e:
        logging.error(f"[{sl_path}] An unexpected error occurred: {e}", exc_info=True)

    return None, None


# --- Batch Processing for SL ---
def scrape_and_store_multiple_sls(sl_paths_to_scrape: list):
    """
    Scrapes and stores multiple Subsidiary Legislations based on a list of paths.

    Args:
        sl_paths_to_scrape: List of SL paths (e.g., ["/SL/AA2004-R5", "/SL/BCPA1999-R1"])
    """
    results = {}
    total_paths = len(sl_paths_to_scrape)
    session = requests.Session()  # Use a session for connection pooling

    logging.info(f"\n=== Starting batch scraping of {total_paths} SL paths ===\n")

    for index, sl_path in enumerate(sl_paths_to_scrape, 1):
        logging.info(f"\n[{index}/{total_paths}] Processing SL path: {sl_path}")

        try:
            # 1. Scrape the SL
            sl_data, all_sections_data = scrape_subsidiary_legislation(sl_path, session)

            # 2. Store if data was retrieved
            if (
                sl_data and all_sections_data is not None
            ):  # Check sections is not None (can be empty list)
                sl_name_log = sl_data.get("act_name", f"Unknown ({sl_path})")
                logging.info(
                    f"Successfully scraped SL '{sl_name_log}' with {len(all_sections_data)} sections."
                )
                logging.info("Storing/Updating SL in Supabase...")

                # Ensure parent act exists before attempting to store SL if parent_id is required
                parent_id = sl_data.get("parent_id")
                # Add logic here if you want to *enforce* parent existence before storing
                # For now, store_in_supabase handles the NULL case if parent wasn't found

                success = store_in_supabase(sl_data, all_sections_data, supabase)

                if success:
                    logging.info(
                        f"Successfully stored/updated SL '{sl_name_log}' in Supabase."
                    )
                    results[sl_path] = True
                else:
                    logging.error(
                        f"Failed to store/update SL '{sl_name_log}' in Supabase."
                    )
                    results[sl_path] = False
            elif sl_data and all_sections_data is None:
                logging.error(
                    f"Scraped metadata for SL '{sl_path}' but failed to extract sections."
                )
                results[sl_path] = False
            else:
                logging.error(f"Failed to scrape SL path: {sl_path}")
                results[sl_path] = False

        except Exception as e:
            logging.error(
                f"Critical error processing SL path {sl_path}: {e}", exc_info=True
            )
            results[sl_path] = False

        # Optional delay between requests to be polite
        time.sleep(0.5)  # Adjust delay as needed (e.g., 0.5 to 2 seconds)

        # Add a separator for clarity
        logging.info(f"\n{'-'*60}\n")

    # Summary Report
    successful = sum(1 for success in results.values() if success)
    failed = total_paths - successful
    logging.info(f"\n=== SL Batch scraping completed ===")
    logging.info(f"Successfully processed: {successful}/{total_paths}")
    logging.info(f"Failed: {failed}/{total_paths}")
    if failed > 0:
        logging.warning("Failed SL paths:")
        for path, success in results.items():
            if not success:
                logging.warning(f"  - {path}")

    return results


# --- Fetch All SL Paths ---
def get_all_sl_paths(session: requests.Session) -> list:
    """Gets all Subsidiary Legislation paths from the browse page."""
    index = 0
    sl_paths = set()
    page_size = 500  # Match the PageSize parameter
    logging.info("Fetching all Subsidiary Legislation (SL) paths...")
    while True:
        # Note: The index in the URL seems to be page number (0-based)
        url = f"https://sso.agc.gov.sg/Browse/SL/Current/All/{index}?PageSize={page_size}&SortBy=Number&SortOrder=ASC"
        logging.info(f"Fetching SL browse page: {url}")
        try:
            headers = base_headers.copy()
            headers["referer"] = (
                "https://sso.agc.gov.sg/Browse/SL/Current/All"  # Appropriate referer
            )
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")
            # Find links specifically starting with /SL/ within the results area
            browse_results = soup.find(
                "div", class_="browse-list-row"
            )  # Target results area
            links_found = []
            if browse_results:
                links_found = browse_results.find_all(
                    "a", href=lambda href: href and href.startswith("/SL/")
                )
            else:
                logging.warning(
                    f"Could not find 'div.browseResults' on page {index}. Trying whole page."
                )
                links_found = soup.find_all(
                    "a", href=lambda href: href and href.startswith("/SL/")
                )

            if not links_found:
                logging.info(
                    f"No more '/SL/' links found on page index {index}. Stopping."
                )
                break

            count_on_page = 0
            for link in links_found:
                href = link["href"]
                base_url_path = href.split("?")[0]  # Remove query params
                if base_url_path not in sl_paths:
                    sl_paths.add(base_url_path)
                    count_on_page += 1

            logging.info(
                f"Found {count_on_page} new SL paths on index {index}. Total unique paths: {len(sl_paths)}"
            )

            # Check if this was the last page
            print(len(count_on_page))
            print(page_size)
            print(len(count_on_page) < page_size)
            if len(count_on_page) < page_size:
                logging.info(
                    f"Found {len(links_found)} links (less than PageSize={page_size}), assuming end of list."
                )
                break

            index += 1  # Move to the next page index

        except requests.exceptions.Timeout:
            logging.error(
                f"Timeout fetching SL browse page {url}. Stopping path collection."
            )
            break
        except requests.exceptions.RequestException as e:
            logging.error(
                f"Failed to fetch SL browse page {url}: {e}. Stopping path collection."
            )
            break
        except Exception as e:
            logging.error(
                f"Error parsing SL browse page {url}: {e}. Stopping path collection."
            )
            break

        # Optional delay between browse page requests
        time.sleep(1)

    logging.info(f"Finished fetching SL paths. Total unique found: {len(sl_paths)}")
    return sorted(list(sl_paths))


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Subsidiary Legislation Scraper Script ---")

    # IMPORTANT PRE-REQUISITE: Ensure main Acts are already scraped and present in the database
    # so that parent_id foreign key lookups can succeed.

    req_session = requests.Session()

    # Fetch all SL paths
    all_sl_paths_to_scrape = get_all_sl_paths(req_session)

    # Example: Scrape only a specific SL for testing
    # all_sl_paths_to_scrape = ["/SL/AA2004-R5"]
    # logging.info(f"TEST MODE: Scraping only: {all_sl_paths_to_scrape}")

    if not all_sl_paths_to_scrape:
        logging.warning("No SL paths found or selected for scraping.")
    else:
        logging.info(f"Found {len(all_sl_paths_to_scrape)} SL paths to process.")
        # Run the SL scraper
        scrape_and_store_multiple_sls(all_sl_paths_to_scrape)

    logging.info("--- SL Scraper Script Finished ---")
