import os
from dotenv import find_dotenv, load_dotenv

# Find .env automatically
dotenv_path = find_dotenv()

# Load up the entries as environment variables
load_dotenv(dotenv_path)

### Get the environment variables set in .env

LOGIC_APPS_URL = os.getenv("LOGIC_APPS_URL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") 
STORAGE_ACCOUNT_CONNECTION_STRING = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
APP_NAME = os.getenv("WEBSITE_SITE_NAME")