import requests
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from supabase import Client, create_client
import os
from dotenv import load_dotenv # Optional: for loading credentials from .env

# --- Supabase Insertion Function ---

supabase_url = os.environ.get("SUPABASE_URL",'https://supabase.thevotum.com')
supabase_key = os.environ.get("SUPABASE_SERVICE_KEY","eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzE3OTU3ODAwLAogICJleHAiOiAxODc1NzI0MjAwCn0.XrCbkNQDLY0fvtqJ7ZHdimDSihI7sRfbqtIjqOXgrNg") # Use SERVICE KEY for backend operations

supabase = create_client(supabase_url, supabase_key)

def store_in_supabase(act_data: dict, all_sections_data: list, supabase_client: Client):
    """
    Inserts the extracted Act and its Sections into Supabase tables.

    Args:
        act_data: Dictionary containing data for the 'acts' table.
        all_sections_data: List of dictionaries, each containing data for the 'sections' table.
        supabase_client: Initialized Supabase client instance.

    Returns:
        True if insertion was successful (Act and Sections), False otherwise.
    """
    inserted_act_id = None
    print("\n--- Starting Supabase Insertion ---")

    # Check if act already exists
    existing_act = get_existing_act(act_data.get('source_id'), supabase_client)
    
    if existing_act:
        inserted_act_id = existing_act['act_id']
        print(f"Act already exists with ID: {inserted_act_id}. Will check for new sections.")
        
        # Get existing section titles for this act
        existing_sections = get_existing_sections(inserted_act_id, supabase_client)
        existing_section_titles = {section['section_title'] for section in existing_sections}
        
        # Filter out sections that already exist
        new_sections_data = [
            section for section in all_sections_data 
            if section.get('section_title') not in existing_section_titles
        ]
        
        if not new_sections_data:
            print("No new sections found for this act.")
            return True
        
        print(f"Found {len(new_sections_data)} new sections to insert.")
        all_sections_data = new_sections_data
    else:
        # 1. Insert new Act
        try:
            # Prepare act data: Remove None values unless column allows NULL
            # Assuming 'act_description', 'source', 'source_id' can be NULL based on schema example
            act_to_insert = {k: v for k, v in act_data.items() if v is not None or k in ['act_description', 'source', 'source_id']}
            if not act_to_insert.get('act_name') or not act_to_insert.get('country'):
                print("Error: Act name and country are required for insertion.")
                return False

            print(f"Attempting to insert Act: {act_to_insert.get('act_name')}")
            act_insert_response = supabase_client.table('acts').insert(act_to_insert).execute()

            # Check for errors after executing
            if hasattr(act_insert_response, 'data') and act_insert_response.data:
                inserted_act_id = act_insert_response.data[0]['act_id']
                print(f"Successfully inserted Act, got act_id: {inserted_act_id}")
            else:
                # Handle potential API error structure
                error_message = "Unknown error during Act insertion."
                if hasattr(act_insert_response, 'error') and act_insert_response.error:
                    error_message = act_insert_response.error.message
                elif hasattr(act_insert_response, 'message'):
                    error_message = act_insert_response.message # Another possible error format
                print(f"Error inserting Act: {error_message}")
                # print(f"Full Act Insert Response: {act_insert_response}") # Debugging
                return False # Stop if Act insertion fails

        except Exception as e:
            print(f"An exception occurred during Act insertion: {e}")
            return False

    # 2. Insert Sections (only if Act was inserted or already exists and sections exist)
    if inserted_act_id and all_sections_data:
        print(f"\nPreparing {len(all_sections_data)} sections for insertion...")
        sections_to_insert = []
        for section in all_sections_data:
            # Link section to the inserted Act or existing Act
            section['act_id'] = inserted_act_id

            # Prepare section data: Remove None values unless column allows NULL
            # Assuming 'questions', 'cot_pairs', 'additional' can be NULL
            # Make sure required fields like title, content, act_id, country are present
            section_prepared = {k: v for k, v in section.items() if v is not None or k in ['questions', 'cot_pairs', 'additional']}

            if not section_prepared.get('section_title') or not section_prepared.get('section_content') or not section_prepared.get('act_id') or not section_prepared.get('country'):
                print(f"Warning: Skipping section due to missing required fields: {section_prepared.get('section_title', 'NO TITLE')}")
                continue # Skip this section if essential data is missing

            sections_to_insert.append(section_prepared)

        if not sections_to_insert:
            print("No valid sections prepared for insertion.")
            # Technically the Act was inserted, so maybe return True? Or False because sections failed?
            # Let's return True as the Act part was successful.
            return True

        try:
            print(f"Attempting to insert {len(sections_to_insert)} sections linked to act_id {inserted_act_id}...")
            # Insert sections in batches (adjust batch_size as needed)
            batch_size = 100 # Supabase often handles ~500-1000 reasonably well, but smaller is safer
            all_sections_inserted = True

            for i in range(0, len(sections_to_insert), batch_size):
                batch = sections_to_insert[i:i + batch_size]
                print(f"  Inserting batch {i // batch_size + 1} ({len(batch)} sections)...")
                sections_insert_response = supabase_client.table('sections').insert(batch).execute()

                # Check batch response
                if not (hasattr(sections_insert_response, 'data') and sections_insert_response.data):
                    error_message = f"Unknown error during Section batch insertion (Batch {i // batch_size + 1})."
                    if hasattr(sections_insert_response, 'error') and sections_insert_response.error:
                         error_message = sections_insert_response.error.message
                    elif hasattr(sections_insert_response, 'message'):
                         error_message = sections_insert_response.message
                    print(f"Error inserting Sections: {error_message}")
                    # print(f"Full Section Insert Response: {sections_insert_response}") # Debugging
                    all_sections_inserted = False
                    break # Stop inserting further batches on error

            if all_sections_inserted:
                print(f"Successfully inserted all {len(sections_to_insert)} sections.")
                return True
            else:
                print("Section insertion failed for one or more batches.")
                return False # Indicate partial failure

        except Exception as e:
            print(f"An exception occurred during Section insertion: {e}")
            return False

    elif inserted_act_id and not all_sections_data:
        print("Act inserted successfully, but no sections were extracted or provided to insert.")
        return True # Act insertion was successful
    else:
        # This case should theoretically not be reached if act insertion failed
        print("Act insertion failed, skipping section insertion.")
        return False


def get_existing_act(source_id, supabase_client):
    """
    Check if an act already exists in the database by its source_id.
    
    Args:
        source_id: The source_id of the act (e.g., "ASA2007")
        supabase_client: Initialized Supabase client instance
        
    Returns:
        Dictionary containing act data if it exists, None otherwise
    """
    try:
        print(f"Checking if act with source_id '{source_id}' already exists...")
        response = supabase_client.table('acts').select('*').eq('source_id', source_id).execute()
        
        if hasattr(response, 'data') and response.data and len(response.data) > 0:
            return response.data[0]
        return None
    except Exception as e:
        print(f"Error checking for existing act: {e}")
        return None


def get_existing_sections(act_id, supabase_client):
    """
    Get all existing sections for an act.
    
    Args:
        act_id: The ID of the act
        supabase_client: Initialized Supabase client instance
        
    Returns:
        List of section dictionaries
    """
    try:
        print(f"Fetching existing sections for act_id {act_id}...")
        response = supabase_client.table('sections').select('section_title').eq('act_id', act_id).execute()
        
        if hasattr(response, 'data') and response.data:
            return response.data
        return []
    except Exception as e:
        print(f"Error fetching existing sections: {e}")
        return []


# --- Configuration ---
BASE_URL = "https://sso.agc.gov.sg"

def generate_act_url(act_path):
    """
    Generates the initial URL for a specific act with WholeDoc=1 parameter.
    
    Args:
        act_path: The path part of the URL for the act (e.g., "/Act/ASA2007")
        
    Returns:
        Full URL with WholeDoc=1 parameter
    """
    initial_act_url_base = urljoin(BASE_URL, act_path)
    
    # Construct URL with ?WholeDoc=1
    url_parts = list(urlparse(initial_act_url_base))
    query = parse_qs(url_parts[4])
    query['WholeDoc'] = '1'
    url_parts[4] = urlencode(query, doseq=True)
    
    return urlunparse(url_parts)

# Default act path for testing
ACT_URL_PATH = "/Act/ASA2007" # Example path
INITIAL_ACT_URL = generate_act_url(ACT_URL_PATH)

LAZY_LOAD_ENDPOINT = "/Details/GetLazyLoadContent"

# --- Request Headers ---
headers = {
    'accept': '*/*',
    'referer': INITIAL_ACT_URL, # Use the modified URL
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Brave";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'accept-language': 'en-US,en;q=0.9',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'x-requested-with': 'XMLHttpRequest',
}

# --- Helper Function: Extract Sections from HTML Fragment ---
# (Remains the same)
def extract_sections_from_html(html_fragment_content):
    """Parses an HTML fragment and extracts section data."""
    sections = []
    if not html_fragment_content:
        return sections

    soup_full_content = BeautifulSoup(html_fragment_content, 'html.parser')

    # remove all divs with class_='amendNote'
    for amendNote in soup_full_content.find_all(class_='amendNote'):
        amendNote.decompose()

    sections_container = soup_full_content.find('div', class_='body')
    if not sections_container:
        print("Warning: Could not find the main 'div.body' container in the fetched content.")
        sections_container = soup_full_content # Fallback

    section_divs = sections_container.find_all('div', class_='prov1')
    if not section_divs:
         print("Warning: No 'div.prov1' elements (sections) found within the content container.")
         return sections

    for section_div in section_divs:
        section = {}
        additional_info = {}
        header_tag = section_div.find('td', class_='prov1Hdr')
        title_text_only = header_tag.get_text(strip=True) if header_tag else "Unknown Title"
        content_tag = section_div.find('td', class_='prov1Txt')

        if content_tag:
            number_tag = content_tag.find('strong')
            section_number_text = ""
            full_title = title_text_only

            if number_tag:
                potential_number = number_tag.get_text(strip=True)
                if re.match(r'^\d+[A-Z]?\.?$', potential_number):
                    section_number_text = potential_number
                    full_title = f"Section {section_number_text} {title_text_only}"

            if not section_number_text and header_tag and header_tag.get('id'):
                 header_id = header_tag.get('id')
                 match = re.match(r'pr(\d+[A-Z]?)', header_id)
                 if match:
                     section_number_text = match.group(1) + "."
                     full_title = f"Section {section_number_text} {title_text_only}"

            section['section_title'] = full_title

            section_content_raw = content_tag.get_text(separator='\n', strip=True)
            processed_content = re.sub(r'\(\s*\n\s*([a-zA-Z0-9]+)\s*\n\s*\)', r'(\1)', section_content_raw)
            processed_content = re.sub(r'\n{3,}', r'\n\n', processed_content)
            if section_number_text:
                escaped_num = re.escape(section_number_text)
                processed_content = re.sub(r'^' + escaped_num + r'\n', escaped_num + ' ', processed_content, count=1)
            section['section_content'] = processed_content.strip()

            section['act_id'] = None
            section['country'] = "SINGAPORE"
            section['questions'] = None
            section['cot_pairs'] = None
            additional_info['header_id'] = header_tag.get('id') if header_tag else None
            section['additional'] = json.dumps(additional_info)
            sections.append(section)
        else:
            print(f"Warning: Could not find content tag 'prov1Txt' for section starting with header: {title_text_only}")
    return sections


# --- Main Scraping Logic ---
def scrape_act(act_path=ACT_URL_PATH):
    """
    Scrape an act from Singapore Statutes Online using the provided act path.
    
    Args:
        act_path: The path part of the URL for the act (e.g., "/Act/ASA2007")
        
    Returns:
        Tuple of (act_data, all_sections_data)
    """
    all_sections_data = []
    act_data = {}
    toc_sys_id = None
    series_id_for_full_content = None
    
    try:
        # 1. Generate Initial URL and make request
        initial_act_url = generate_act_url(act_path)
        print(f"Fetching initial page: {initial_act_url}")
        session = requests.Session()
        session.headers.update(headers)
        response_initial = session.get(initial_act_url)
        response_initial.raise_for_status()
        soup_initial = BeautifulSoup(response_initial.content, 'html.parser')

        # 2. Extract Initial Metadata
        act_title_tag = soup_initial.find('td', class_='actHd')
        long_title_tag = soup_initial.find('td', class_='longTitle')
        act_data['act_name'] = act_title_tag.get_text(strip=True) if act_title_tag else f"UNKNOWN ACT ({act_path})"
        act_data['act_description'] = long_title_tag.get_text(separator=' ', strip=True) if long_title_tag else ""
        act_data['country'] = "SINGAPORE"
        act_data['source'] = "Singapore Statutes Online (sso.agc.gov.sg)"
        act_data['source_id'] = act_path.split('/')[-1]

        # Extract tocSysId and the first key from fragments dictionary
        global_vars_divs = soup_initial.find_all('div', class_='global-vars')
        config_data = None
        fragments_dict = None
        for div in global_vars_divs:
            data_json_str = div.get('data-json')
            if data_json_str:
                try:
                    potential_config = json.loads(data_json_str)
                    # Check for tocSysId and the fragments dictionary
                    if 'tocSysId' in potential_config and 'fragments' in potential_config:
                        config_data = potential_config
                        toc_sys_id = config_data.get('tocSysId')
                        fragments_dict = config_data.get('fragments')
                        break # Found the necessary config
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse JSON from global-vars: {e}")
                    continue

        if not toc_sys_id:
            raise ValueError("Could not extract tocSysId from configuration JSON.")

        # --- Get the first key from the fragments dictionary ---
        if fragments_dict and isinstance(fragments_dict, dict) and fragments_dict:
            try:
                # Get the list of keys and take the first one
                first_fragment_key = next(iter(fragments_dict.keys()))
                series_id_for_full_content = first_fragment_key
            except StopIteration:
                 raise ValueError("Fragments dictionary is empty, cannot get the first key.")
        else:
            raise ValueError("Could not find a valid 'fragments' dictionary in the configuration JSON.")
        # --- End of Series ID extraction ---

        if not series_id_for_full_content:
             raise ValueError("Failed to extract the first fragment key to use as SeriesId.")

        print(f"Found TocSysId: {toc_sys_id}")
        print(f"Using first fragment key as SeriesId for full content: {series_id_for_full_content}")

        print("\n--- Extracted Act Data ---")
        print(json.dumps(act_data, indent=2))

        # 3. Fetch Full Content Fragment
        print("\n--- Fetching Full Content Fragment ---")
        lazy_load_url = urljoin(BASE_URL, LAZY_LOAD_ENDPOINT)
        params = {
            'TocSysId': toc_sys_id,
            'SeriesId': series_id_for_full_content # Use the extracted first key
        }
        print(f"Fetching full content fragment (SeriesId: {series_id_for_full_content})...")

        response_full_content = session.get(lazy_load_url, params=params)
        response_full_content.raise_for_status()

        # 4. Extract Sections from the Full Fragment
        full_html = response_full_content.text
        all_sections_data = extract_sections_from_html(full_html)

        print(f"\n--- Total Sections Extracted: {len(all_sections_data)} ---")
        if all_sections_data:
            # Sorting logic (remains useful)
            def sort_key(section):
                match = re.search(r'Section (\d+)([A-Z]?)\.?\s', section.get('section_title', ''))
                if match:
                    num_part = int(match.group(1))
                    alpha_part = match.group(2)
                    alpha_val = ord(alpha_part) if alpha_part else 0
                    return (num_part, alpha_val)
                return (float('inf'), 0)
            all_sections_data.sort(key=sort_key)

            print("First extracted section:")
            print(json.dumps(all_sections_data[0], indent=2))
        else:
             print("No sections were successfully extracted from the full content fragment.")
        
        return act_data, all_sections_data

    except requests.exceptions.RequestException as e:
        print(f"Error during network request: {e}")
    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return None, None


def scrape_and_store_multiple_acts(act_paths):
    """
    Scrape and store multiple acts based on the provided list of act paths.
    
    Args:
        act_paths: List of act paths to scrape (e.g. ["/Act/ASA2007", "/Act/ANOTHER_ACT"])
        
    Returns:
        Dictionary of results with act paths as keys and success status as values
    """
    results = {}
    total_acts = len(act_paths)
    
    print(f"\n=== Starting batch scraping of {total_acts} acts ===\n")
    
    for index, act_path in enumerate(act_paths, 1):
        print(f"\n[{index}/{total_acts}] Processing act: {act_path}")
        
        try:
            # 1. Scrape the act
            act_data, all_sections_data = scrape_act(act_path)
            
            # 2. If data was retrieved successfully, store it in Supabase
            if act_data and all_sections_data:
                print(f"Successfully scraped act '{act_data.get('act_name')}' with {len(all_sections_data)} sections")
                print("Storing in Supabase...")
                
                success = store_in_supabase(act_data, all_sections_data, supabase)
                
                if success:
                    print(f"Successfully stored act '{act_data.get('act_name')}' in Supabase")
                    results[act_path] = True
                else:
                    print(f"Failed to store act '{act_data.get('act_name')}' in Supabase")
                    results[act_path] = False
            else:
                print(f"Failed to scrape act: {act_path}")
                results[act_path] = False
                
        except Exception as e:
            print(f"Error processing act {act_path}: {str(e)}")
            results[act_path] = False
        
        # Add a separator between acts for better readability in logs
        print(f"\n{'='*50}\n")
    
    # Summary report
    successful = sum(1 for success in results.values() if success)
    print(f"\n=== Batch scraping completed ===")
    print(f"Successfully processed {successful}/{total_acts} acts")
    
    return results



def get_all_act_paths():
    index = 0
    act_paths = set()  # Using a set to automatically remove duplicates
    while True:
        response = requests.get(f"https://sso.agc.gov.sg/Browse/Act/Current/All/{index}?PageSize=500&SortBy=Title&SortOrder=ASC",headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith("/Act/"):
                # Only keep clean URLs without query parameters
                base_url = href.split('?')[0]
                act_paths.add(base_url)
        if len(soup.find_all('a', href=True)) < 500:
            break
        index += 1
    return list(act_paths)  # Convert set back to list
    

# Example usage
if __name__ == "__main__":
    # Example list of act paths to scrape
    # act_paths_to_scrape = get_all_act_paths()
    # print(act_paths_to_scrape)
    # print(len(act_paths_to_scrape))

    # scrape_and_store_multiple_acts(act_paths_to_scrape)

    response = requests.get(BASE_URL + "/SL/AA2004-R5",headers=headers  )
    soup = BeautifulSoup(response.text, 'html.parser')
    print(soup.prettify())

    sections = extract_sections_from_html(response.text)
    print(json.dumps(sections,indent=2))