from dotenv import load_dotenv
import os
from pathlib import Path

# Explicitly load the .env file from current directory
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

# Retrieve values
openai_api_key = os.getenv("OPENAI_API_KEY")
supabase_url = os.getenv("SUPABASE_URL")

print("OpenAI Key Loaded:", openai_api_key is not None)
print("Supabase URL:", supabase_url)
