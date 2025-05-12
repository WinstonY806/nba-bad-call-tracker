from dotenv import load_dotenv
import os
from pathlib import Path
from supabase import create_client, Client
from postgrest import APIError

# Load environment variables from .env in project root
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Retrieve API keys and URLs
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Verify env variables loaded
print("OpenAI Key Loaded:", OPENAI_API_KEY is not None)
print("Supabase URL Loaded:", SUPABASE_URL is not None)
print("Supabase Key Loaded:", SUPABASE_KEY is not None)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def insert_call(call_data: dict):
    """
    Insert a single call record into the Supabase 'calls' table.
    call_data must include keys:
      - game_id (str)
      - period (int)
      - time (str)
      - call_type (str)
      - decision (str)
      - correct (bool)
      - referee (str, optional)
      - team_favored (str, optional)
      - team_penalized (str, optional)
      - notes (str, optional)
    """
    try:
        response = supabase.table("calls").insert(call_data).execute()
        status = getattr(response, 'status_code', None) or getattr(response, 'status', None)
        print(f"Insert status: {status}")
        print("Inserted data:", response.data)
        if response.error:
            print("Error details:", response.error)
    except APIError as e:
        print("Supabase APIError encountered:", e)
        try:
            print("Error payload:", e.args[0])
        except Exception:
            pass

def main():
    sample_calls = [
        {
            "game_id": "0022300034",
            "period": 4,
            "time": "01:00",
            "call_type": "Shooting Foul",
            "decision": "Incorrect",
            "correct": False,
            "referee": "Zach Zarba",
            "team_favored": "LAL",
            "team_penalized": "BOS",
            "notes": "Test: late-game contact"
        },
        {
            "game_id": "0022300035",
            "period": 5,
            "time": "00:45",
            "call_type": "Loose Ball",
            "decision": "Correct",
            "correct": True,
            "referee": "Josh Tiven",
            "team_favored": "BOS",
            "team_penalized": "LAL",
            "notes": "OT hustle play"
        }
    ]
    for call in sample_calls:
        insert_call(call)

if __name__ == "__main__":
    main()