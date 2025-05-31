import os
import json
from openai import OpenAI, RateLimitError, APIStatusError # Import specific error types
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

from nba_api.stats.endpoints import BoxScoreSummaryV2 # Keep for referee lookup
import logging
import time
import re 

# --- Configuration & Initialization ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

QUOTA_ERROR_DETECTED = False

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not OPENAI_API_KEY:
    logging.error("OPENAI_API_KEY environment variable not set.")
    raise ValueError("OPENAI_API_KEY environment variable not set.")
if not SUPABASE_URL:
    logging.error("SUPABASE_URL environment variable not set.")
    raise ValueError("SUPABASE_URL environment variable not set.")
if not SUPABASE_KEY:
    logging.error("SUPABASE_KEY environment variable not set.")
    raise ValueError("SUPABASE_KEY environment variable not set.")

try:
    client = OpenAI(api_key=OPENAI_API_KEY)
    options = ClientOptions(postgrest_client_timeout=10, storage_client_timeout=10, schema="public")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)
    logging.info("OpenAI and Supabase clients initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing API clients: {e}")
    raise

# --- Helper Function to Extract Team Abbreviation ---
def extract_team_from_player_string(player_string: str | None) -> str | None:
    """
    Extracts a team abbreviation (e.g., 'LAL') from a player string like 'James, LeBron (LAL)'.
    """
    if not player_string:
        return None
    match = re.search(r'\(([A-Z]{2,3})\)', player_string) # Matches 2 or 3 uppercase letters in parens
    if match:
        return match.group(1)
    return None

# --- Referee Lookup ---
def fetch_game_officials(game_id: str) -> dict:
    """
    Return a dictionary of officials' full names for the given NBA game ID.
    """
    try:
        time.sleep(0.6) 
        summary = BoxScoreSummaryV2(game_id=game_id, timeout=15)
        data = summary.get_normalized_dict()
        names: list[str] = []
        for row in data.get("Officials", []):
            first = (row.get("FIRST_NAME") or "").title()
            last = (row.get("LAST_NAME") or "").title()
            if first or last:
                names.append(f"{first} {last}".strip())
            elif row.get("OFFICIAL_NAME"):
                names.append(row["OFFICIAL_NAME"].title())
        
        officials_dict = {
            "ref_1": names[0] if len(names) > 0 else None,
            "ref_2": names[1] if len(names) > 1 else None,
            "ref_3": names[2] if len(names) > 2 else None
        }
        logging.info(
            f"Fetched officials for game_id {game_id}: "
            f"{', '.join(filter(None, officials_dict.values())) or 'None'}"
        )
        return officials_dict
    except Exception as e:
        logging.error(f"Error fetching officials for game_id {game_id}: {e}")
        return {"ref_1": None, "ref_2": None, "ref_3": None}

# --- AI Parsing (Focused Task) ---
def get_favored_penalized_teams_with_ai(plays_for_ai_processing: list[dict], game_id_for_context: str) -> list[dict]:
    """
    Sends a list of pre-processed plays to OpenAI.
    AI's task is to add 'team_favored' and 'team_penalized' to each play object
    ONLY for incorrect calls/non-calls.
    """
    global QUOTA_ERROR_DETECTED
    if QUOTA_ERROR_DETECTED:
        logging.warning(f"Skipping OpenAI call for game_id {game_id_for_context} due to previously detected quota error.")
        return plays_for_ai_processing # Return original plays, favored/penalized will be null

    if not plays_for_ai_processing:
        logging.info(f"No plays provided to AI for game_id {game_id_for_context}.")
        return []

    # Prepare context for AI: game_id and the list of plays (which now include Python-derived fields)
    context_for_ai = {
        "game_id_context": game_id_for_context,
        "plays_to_augment": plays_for_ai_processing # Plays already have period, decision, is_correct_decision etc.
    }

    system_prompt = (
        "You are an expert NBA Last Two Minute (L2M) report analyst.\n"
        "You will be given a JSON object containing 'game_id_context' and 'plays_to_augment'.\n"
        "'plays_to_augment' is an array of play objects that have already been partially processed. Each play object includes 'period', 'time', 'call_type', 'decision', 'is_correct_decision', 'description', and original context fields like 'source_CP' (Committing Player string from L2M JSON) and 'source_DP' (Disadvantaged Player string from L2M JSON).\n"
        "Your task is to ANALYZE EACH play object and ADD two new keys: 'team_favored' and 'team_penalized'.\n"
        "Return **one JSON object** with a single key `\"augmented_plays\"`. The value must be an array of these play objects, each now including 'team_favored' and 'team_penalized'.\n"
        "Rules for 'team_favored' and 'team_penalized':\n"
        "1. If 'is_correct_decision' in the input play object is `true` (i.e., decision is 'CC' or 'CNC'), then 'team_favored' and 'team_penalized' for that play MUST be `null`.\n"
        "2. If 'is_correct_decision' is `false` (i.e., decision is 'IC' or 'INC'):\n"
        "   - Analyze the 'description', 'source_CP', and 'source_DP' fields to infer the teams.\n"
        "   - 'source_CP' and 'source_DP' are strings like 'Player Name (TEAM_ABBREVIATION)'. Extract the TEAM_ABBREVIATION.\n"
        "   - If 'decision' is 'IC' (Incorrect Call):\n"
        "     - `team_penalized`: Should be the team of the player in 'source_CP' (who was incorrectly called).\n"
        "     - `team_favored`: Should be the team of the player in 'source_DP' (or the opposing team to 'source_CP').\n"
        "   - If 'decision' is 'INC' (Incorrect Non-Call):\n"
        "     - `team_penalized`: Should be the team of the player in 'source_DP' (who was disadvantaged by the missed call).\n"
        "     - `team_favored`: Should be the team of the player in 'source_CP' (who committed the uncalled infraction).\n"
        "   - If a team cannot be clearly determined from the provided context even for an incorrect call, set the respective field to `null`.\n"
        "3. Ensure the output for each play includes ALL original keys from the input 'plays_to_augment' PLUS the new 'team_favored' and 'team_penalized' keys.\n"
        "Example of an input play object in 'plays_to_augment':\n"
        "    {\n"
        "      \"period\": 4,\n"
        "      \"time\": \"0:46.2\",\n"
        "      \"call_type\": \"Foul: Shooting\",\n"
        "      \"decision\": \"IC\",\n"
        "      \"is_correct_decision\": false,\n"
        "      \"description\": \"Holiday (BOS) makes contact with the arm of Nembhard (IND) during his jump shot attempt.\",\n"
        "      \"source_CP\": \"Holiday, Jrue (BOS)\",\n"
        "      \"source_DP\": \"Nembhard, Andrew (IND)\"\n"
        "    }\n"
        "Expected output for this play within the 'augmented_plays' array:\n"
        "    {\n"
        "      \"period\": 4,\n"
        "      \"time\": \"0:46.2\",\n"
        "      \"call_type\": \"Foul: Shooting\",\n"
        "      \"decision\": \"IC\",\n"
        "      \"is_correct_decision\": false,\n"
        "      \"description\": \"Holiday (BOS) makes contact with the arm of Nembhard (IND) during his jump shot attempt.\",\n"
        "      \"source_CP\": \"Holiday, Jrue (BOS)\",\n"
        "      \"source_DP\": \"Nembhard, Andrew (IND)\",\n"
        "      \"team_favored\": \"IND\",\n"
        "      \"team_penalized\": \"BOS\"\n"
        "    }\n"
    )

    user_msg = {"role": "user", "content": json.dumps(context_for_ai, indent=2)} 

    logging.info(f"Sending {len(plays_for_ai_processing)} pre-processed plays for game_id {game_id_for_context} to OpenAI for augmentation.")
    response_content = None
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role": "system", "content": system_prompt}, user_msg],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        response_content = completion.choices[0].message.content
        parsed_json = json.loads(response_content)

        if isinstance(parsed_json, dict) and "augmented_plays" in parsed_json and isinstance(parsed_json["augmented_plays"], list):
            augmented_plays_list = parsed_json["augmented_plays"]
            # Basic validation: check if the number of plays returned matches the input
            if len(augmented_plays_list) == len(plays_for_ai_processing):
                logging.info(f"Successfully augmented {len(augmented_plays_list)} plays from AI for game_id {game_id_for_context}.")
                return augmented_plays_list
            else:
                logging.warning(f"AI returned a different number of plays ({len(augmented_plays_list)}) than expected ({len(plays_for_ai_processing)}) for game_id {game_id_for_context}. Using original plays.")
                # Fallback to original plays if AI response structure is problematic regarding play count
                return plays_for_ai_processing 
        else:
            logging.warning(f"AI response for {game_id_for_context} missing 'augmented_plays' key or not a list. Full response: {response_content[:300]}... Using original plays.")
            return plays_for_ai_processing
    except RateLimitError as e:
        logging.error(f"OpenAI RateLimitError for game_id {game_id_for_context}: {e}")
        if hasattr(e, 'body') and e.body and 'type' in e.body and e.body['type'] == 'insufficient_quota':
            logging.error("INSUFFICIENT QUOTA DETECTED. Further OpenAI calls will be skipped.")
            QUOTA_ERROR_DETECTED = True
        return plays_for_ai_processing # Return original on error
    except APIStatusError as e:
        logging.error(f"OpenAI APIStatusError for game_id {game_id_for_context}: {e.status_code} - {e.response}")
        if e.status_code == 429 and hasattr(e, 'body') and e.body and 'type' in e.body and e.body['type'] == 'insufficient_quota':
             logging.error("INSUFFICIENT QUOTA DETECTED (via APIStatusError). Further OpenAI calls will be skipped.")
             QUOTA_ERROR_DETECTED = True
        return plays_for_ai_processing # Return original on error
    except json.JSONDecodeError as e:
        logging.error(f"JSONDecodeError for game_id {game_id_for_context}: {e}. Response from AI: {response_content[:500] if response_content else 'N/A'}. Using original plays.")
        return plays_for_ai_processing # Return original on error
    except Exception as e:
        logging.error(f"Unexpected error calling OpenAI API for game_id {game_id_for_context}: {e}. Using original plays.")
        return plays_for_ai_processing # Return original on error


# --- Supabase Interaction ---
def delete_existing_plays(game_id: str):
    """Deletes existing plays for a given game_id to ensure idempotency."""
    try:
        logging.info(f"Deleting existing plays for game_id: {game_id} from table 'calls'.")
        response = supabase.table("calls").delete().eq("game_id", game_id).execute()
        if hasattr(response, 'error') and response.error:
            logging.error(f"Error deleting plays for game_id {game_id}: {response.error.message if hasattr(response.error, 'message') else response.error}")
            raise Exception(f"Supabase delete error: {response.error.message if hasattr(response.error, 'message') else response.error}")
        else:
            logging.info(f"Delete operation for existing plays for game_id {game_id} completed.")
    except Exception as e:
        logging.error(f"Exception during deletion of plays for game_id {game_id}: {e}")
        raise 

def insert_plays_to_supabase(game_id: str, plays_to_insert: list[dict], officials: dict):
    """
    Inserts processed plays into the Supabase 'calls' table using batch insert.
    'plays_to_insert' contains plays with Python-derived fields and AI-augmented favored/penalized teams.
    """
    if not plays_to_insert:
        logging.info(f"No plays to insert for game_id: {game_id}")
        return 0

    records_to_insert = []
    for play_data in plays_to_insert: # play_data now comes from AI or Python pre-processing
        record = {
            "game_id": game_id, 
            "period": play_data.get("period"), # Should be set by Python pre-processing
            "time": play_data.get("time"), # Should be set by Python pre-processing
            "call_type": play_data.get("call_type"), # Should be set by Python pre-processing
            "decision": play_data.get("decision"), # Should be set by Python pre-processing
            "is_correct_decision": play_data.get("is_correct_decision"), # Should be set by Python pre-processing
            "description": play_data.get("description"), # Should be set by Python pre-processing
            "team_favored": play_data.get("team_favored"), # This is from AI
            "team_penalized": play_data.get("team_penalized"), # This is from AI
            "ref_1": officials.get("ref_1"), 
            "ref_2": officials.get("ref_2"),
            "ref_3": officials.get("ref_3"),
        }
        # Validate essential fields that should have been set by Python pre-processing or AI
        if not all(key in record and record[key] is not None for key in ["time", "call_type", "description", "period", "is_correct_decision", "decision"]):
            logging.warning(f"Skipping play for game_id {game_id} due to missing critical fields after AI processing: {play_data}")
            continue
        records_to_insert.append(record)

    if not records_to_insert:
        logging.info(f"No valid plays to insert for game_id: {game_id} after validation.")
        return 0

    try:
        logging.info(f"Attempting to batch insert {len(records_to_insert)} plays for game_id: {game_id} into table 'calls'.")
        response = supabase.table("calls").insert(records_to_insert).execute()
        
        if hasattr(response, 'error') and response.error:
            error_message = response.error.message if hasattr(response.error, 'message') else str(response.error)
            logging.error(f"Error inserting plays for game_id {game_id}: {error_message}")
            if "Could not find the" in error_message and "column" in error_message:
                column_name_match = re.search(r"Could not find the '([^']*)' column", error_message)
                if column_name_match:
                    missing_column = column_name_match.group(1)
                    logging.error(f"HINT: The column '{missing_column}' seems to be missing in your Supabase 'calls' table or has a different name. Please check your table schema.")
                else:
                    logging.error(f"HINT: A column specified in the script might be missing or misspelled in your Supabase 'calls' table. Please check your table schema against the script's 'record' dictionary.")
            return 0
        elif hasattr(response, 'data') and response.data: 
            logging.info(f"Successfully inserted {len(response.data)} plays for game_id {game_id}.")
            return len(response.data)
        elif not (hasattr(response, 'error') and response.error): 
            logging.info(f"Supabase insert for game_id {game_id} executed. Response indicates success but returned no data. Assuming {len(records_to_insert)} plays inserted.")
            return len(records_to_insert)
        else: 
            logging.warning(f"Supabase insert for game_id {game_id} executed but returned an unexpected response structure. Assuming 0 inserts. Response: {response}")
            return 0
    except Exception as e: 
        logging.error(f"Exception during Supabase insert for game_id {game_id}: {e}")
        return 0

# --- Main Processing Logic ---
def process_raw_reports(input_dir: str, test_mode_limit: int = 0):
    """
    Reads saved L2M JSON files, pre-processes data in Python,
    uses AI for specific inferences, and inserts into Supabase.
    """
    global QUOTA_ERROR_DETECTED
    if not os.path.isdir(input_dir):
        logging.error(f"Input directory '{input_dir}' not found.")
        return

    files = sorted([f for f in os.listdir(input_dir) if f.endswith(".json")])
    
    if not files:
        logging.info(f"No .json files found in '{input_dir}'.")
        return

    logging.info(f"Found {len(files)} raw L2M JSON files in '{input_dir}'.")

    files_to_process = files
    if test_mode_limit > 0 and test_mode_limit < len(files):
        logging.info(f"TEST MODE: Processing up to {test_mode_limit} JSON files.")
        files_to_process = files[:test_mode_limit]

    processed_files_count = 0
    total_plays_inserted_count = 0

    for filename in files_to_process:
        if QUOTA_ERROR_DETECTED:
            logging.warning("OpenAI quota previously exceeded. Halting further processing of files.")
            break 

        game_id = filename[:-5] 
        file_path = os.path.join(input_dir, filename)
        logging.info(f"--- Processing file: {filename} (Game ID: {game_id}) ---")
        
        current_file_plays_inserted = 0
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                l2m_json_content = json.load(f) 

            if not l2m_json_content or "l2m" not in l2m_json_content:
                logging.warning(f"File {filename} is empty, not valid JSON, or missing 'l2m' key. Skipping.")
                continue 
            
            source_plays = l2m_json_content.get("l2m", [])
            python_processed_plays = []
            for play in source_plays:
                # Python-based transformations
                period_name = play.get("PeriodName", "Q4") # Default to Q4 if missing
                period = 4 # Default
                if "OT" in period_name:
                    try:
                        period = 4 + int(period_name.replace("OT", ""))
                    except ValueError:
                        logging.warning(f"Could not parse OT period: {period_name} for game {game_id}. Defaulting to 5 for OT.")
                        period = 5 # Generic OT
                
                decision = play.get("CallRatingName")
                is_correct = decision in ["CC", "CNC"] if decision else None

                processed_play = {
                    "period": period,
                    "time": play.get("PCTime"),
                    "call_type": play.get("CallType"),
                    "decision": decision,
                    "is_correct_decision": is_correct,
                    "description": play.get("Comment"),
                    # Pass original CP/DP for AI context, AI will use these to infer team favored/penalized
                    "source_CP": play.get("CP"), 
                    "source_DP": play.get("DP"),
                    "source_posTeamId": play.get("posTeamId"), # Might also be useful context for AI
                    # Initialize fields AI will populate
                    "team_favored": None, 
                    "team_penalized": None
                }
                python_processed_plays.append(processed_play)
            
            if not python_processed_plays:
                logging.info(f"No plays to process after Python pre-processing for {game_id}.")
                processed_files_count += 1
                continue

            # AI call to get team_favored and team_penalized
            ai_augmented_plays = get_favored_penalized_teams_with_ai(python_processed_plays, game_id)
            
            officials = fetch_game_officials(game_id)

            if ai_augmented_plays: # Check if AI returned anything (even if it's the original list on error)
                try:
                    delete_existing_plays(game_id) 
                    current_file_plays_inserted = insert_plays_to_supabase(game_id, ai_augmented_plays, officials)
                    total_plays_inserted_count += current_file_plays_inserted
                except Exception as e: 
                    logging.error(f"Failed to process Supabase operations for {game_id} due to: {e}. Skipping DB operations for this game.")
            else:
                logging.info(f"No plays returned from AI augmentation for {game_id}.")
            
            processed_files_count += 1 

        except FileNotFoundError:
            logging.error(f"File not found: {file_path}. Skipping.")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON from file {filename}: {e}. Skipping.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing file {filename}: {e}")

    logging.info(f"--- Processing complete ---")
    if QUOTA_ERROR_DETECTED:
        logging.warning("OpenAI quota was exceeded during the run; some files may not have been fully processed by AI.")
    logging.info(f"Attempted to process {processed_files_count}/{len(files_to_process)} files.")
    logging.info(f"Total plays inserted across all processed games: {total_plays_inserted_count}.")

if __name__ == "__main__":
    input_directory = "1nba-bad-call-tracker/raw_reports_json" 
    limit_files = 10000000 
    process_raw_reports(input_dir=input_directory, test_mode_limit=limit_files)
