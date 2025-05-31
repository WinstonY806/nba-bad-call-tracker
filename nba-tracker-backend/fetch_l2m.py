import os
import requests
# BeautifulSoup is no longer strictly needed in fetch_report_links if we only use the regex,
# but it doesn't hurt to keep it for now if the <a> tag scan is ever re-enabled or useful.
from bs4 import BeautifulSoup 
import re
import time
import json # To pretty-print JSON if needed, and for the AI script later

# Define a User-Agent to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
# Increased timeout
REQUEST_TIMEOUT = 30 # seconds

def fetch_game_ids_from_index(index_url: str) -> list[str]:
    """
    Scrape all L2M report game_ids from the given index URL.
    This function primarily looks for gameId patterns in hrefs.
    """
    print(f"[fetch_game_ids_from_index] Attempting to fetch game IDs from: {index_url}")
    game_ids = set()
    try:
        resp = requests.get(index_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        print(f"[fetch_game_ids_from_index] Successfully fetched {index_url}, status: {resp.status_code}")

        # Using BeautifulSoup to parse the page and find all links
        soup = BeautifulSoup(resp.text, "html.parser")
        links_found_matching_pattern = 0
        total_links_inspected = 0

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            total_links_inspected += 1
            
            # Regex to find gameId in L2MReport.html links
            match = re.search(r"L2MReport\.html\?gameId=(\d{10})", href)
            if match:
                game_id = match.group(1) 
                game_ids.add(game_id)
                links_found_matching_pattern += 1
        
        print(f"[fetch_game_ids_from_index] Inspected {total_links_inspected} <a> tags with hrefs on {index_url}.")

        if links_found_matching_pattern == 0:
            print(f"[fetch_game_ids_from_index] No game IDs matching pattern r'L2MReport\\.html\\?gameId=(\\d{{10}})' found on {index_url} via <a> tag scan.")
            # You could add a raw HTML regex fallback here if needed, similar to your other script,
            # but the <a> tag scan with the current regex should be effective for the main L2M page.
        else:
            print(f"[fetch_game_ids_from_index] Found {links_found_matching_pattern} unique game ID(s) on {index_url} via <a> tag scan.")

    except requests.exceptions.RequestException as e:
        print(f"[fetch_game_ids_from_index] Error fetching {index_url}: {e}")
    except Exception as e:
        print(f"[fetch_game_ids_from_index] An unexpected error occurred: {e}")

    return sorted(list(game_ids))

def fetch_l2m_json_data(game_id: str) -> dict | None:
    """
    Fetch the L2M JSON data for a given game_id.
    """
    # Construct the JSON URL based on the pattern observed in the L2MReport.html's JavaScript
    json_url = f"https://official.nba.com/l2m/json/{game_id}.json"
    print(f"[fetch_l2m_json_data] Fetching JSON for game ID: {game_id} from {json_url}")
    try:
        resp = requests.get(json_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        # The response should be JSON, so we parse it directly
        data = resp.json()
        print(f"[fetch_l2m_json_data] Successfully fetched and parsed JSON for game ID: {game_id}")
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"[fetch_l2m_json_data] HTTP error for game ID {game_id} ({json_url}): {http_err}")
    except requests.exceptions.JSONDecodeError as json_err:
        print(f"[fetch_l2m_json_data] Error decoding JSON for game ID {game_id} ({json_url}): {json_err}")
    except requests.exceptions.RequestException as e:
        print(f"[fetch_l2m_json_data] Error fetching JSON for game ID {game_id} ({json_url}): {e}")
    return None

def save_raw_json_reports(output_dir: str, test_mode_limit: int = 0):
    """
    Fetch L2M JSON data for each game ID and save it to the given directory.
    If test_mode_limit is > 0, only that many reports will be processed.
    """
    # Directly fetch game IDs from the current‑season index; archive handling will be added later.
    os.makedirs(output_dir, exist_ok=True)
    print(f"[save_raw_json_reports] Output directory: '{os.path.abspath(output_dir)}'")

    # add archive_seasons later on
    current_season_url = "https://official.nba.com/2024-25-nba-officiating-last-two-minute-reports/"
    all_game_ids = fetch_game_ids_from_index(current_season_url)

    if not all_game_ids:
        print("[save_raw_json_reports] No game IDs found. Exiting.")
        return

    ids_to_process = all_game_ids
    if test_mode_limit > 0 and test_mode_limit < len(all_game_ids):
        print(f"[save_raw_json_reports] TEST MODE: Processing up to {test_mode_limit} reports.")
        ids_to_process = all_game_ids[:test_mode_limit]
    
    if not ids_to_process:
        print("[save_raw_json_reports] No reports to process (after applying limit). Exiting.")
        return

    print(f"[save_raw_json_reports] Found {len(all_game_ids)} total unique L2M game IDs. Processing {len(ids_to_process)} JSON reports.")
    saved_count = 0
    for i, game_id in enumerate(ids_to_process):
        print(f"[save_raw_json_reports] Processing report {i+1}/{len(ids_to_process)}: Game ID {game_id}")
        
        # Define path for the JSON file
        json_file_path = os.path.join(output_dir, f"{game_id}.json")

        # Optional: Check if JSON file already exists
        if os.path.exists(json_file_path):
            print(f"[save_raw_json_reports]   → {game_id}.json already exists; skipping download.")
            # If you want to count it as "saved" or "processed" for the test limit, adjust logic here.
            # For now, it just skips. If you want it to count towards the limit, you might
            # increment saved_count or ensure it's part of the initial ids_to_process.
            continue

        json_data = fetch_l2m_json_data(game_id)
        
        if json_data:
            try:
                with open(json_file_path, "w", encoding="utf-8") as f:
                    json.dump(json_data, f, indent=4) # Save pretty-printed JSON
                print(f"[save_raw_json_reports]   → Saved raw JSON report for {game_id} to {json_file_path}")
                saved_count += 1
            except IOError as io_err:
                print(f"[save_raw_json_reports]   Error saving JSON file for {game_id}: {io_err}")
        else:
            print(f"[save_raw_json_reports]   Skipping save for {game_id} due to fetch error or empty JSON data.")
        
        if i < len(ids_to_process) - 1:
            time.sleep(0.5) 

    print(f"[save_raw_json_reports] Finished. Successfully saved {saved_count}/{len(ids_to_process)} JSON reports in this run.")

if __name__ == "__main__":
    save_raw_json_reports(output_dir="1nba-bad-call-tracker/raw_reports_json", test_mode_limit=0) # Changed output dir name
    
    # To run for all reports, set test_mode_limit to 0 or remove it:
    # save_raw_json_reports(output_dir="raw_reports_json")
