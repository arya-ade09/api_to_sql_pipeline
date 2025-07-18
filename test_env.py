# test_env.py
from dotenv import load_dotenv
import os

load_dotenv()

test_key = os.getenv("API_KEY")
print("Test Key from .env:", test_key)

# Also, let's print all loaded variables to see if anything is there
# This can sometimes show if python-dotenv is finding _something_ but not what you expect
for key, value in os.environ.items():
    if key.startswith(('API_', 'DB_')): # Only show relevant variables
        print(f"Environment variable: {key}={value}")