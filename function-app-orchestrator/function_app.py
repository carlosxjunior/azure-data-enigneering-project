import logging
import azure.functions as func
import os
import json
import requests
from dotenv import find_dotenv, load_dotenv

# Find .env automatically
dotenv_path = find_dotenv()
# Load up the entries as environment variables
load_dotenv(dotenv_path)

APP_NAME = os.environ.get("WEBSITE_SITE_NAME")
FUNCTION_NAME = os.environ.get("FUNCTION_NAME")
FUNCTION_KEY = os.environ.get("FUNCTION_KEY")
LOGIC_APPS_URL = os.environ.get("LOGIC_APPS_URL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def load_request_body(file_path="tournaments_to_ingest.json"):
    with open(file_path, "r") as file:
        tournaments = json.load(file)
    return tournaments

def notificator(logic_apps_url=LOGIC_APPS_URL, chat_id=TELEGRAM_CHAT_ID, message="❌ JOB FAILURE ALERT"):
    payload = {
        "chat_id": chat_id,
        "message": message
    }
    try:
        # Make the HTTP POST request to trigger the Logic App
        response = requests.post(logic_apps_url, json=payload)
        response.raise_for_status()
        logging.info("PYLOG: Notification sent successfully through Logic Apps!")
        return True
    except requests.exceptions.RequestException as e:
        logging.info(f"PYLOG: Error trying to send notification using Logic Apps.\nError: {str(e)}")
        return False

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=False)
@app.function_name("dailyIngestLatestEvents")
def main(myTimer: func.TimerRequest) -> None:
    logging.info("PYLOG: Started main()")
    request_bodies = load_request_body()
    for request_body in request_bodies:
        logging.info(f"PYLOG: Calling function ingest-latest-events for {request_body.get('tournament')}")
        request_url = f"https://{FUNCTION_NAME}.azurewebsites.net/api/ingest-latest-events?code={FUNCTION_KEY}"
        try:
            response = requests.post(url=request_url, json=request_body)
            response.raise_for_status()
        except Exception as e:
            logging.info(f"PYLOG: Error trying to call function ingest-latest-events for {request_body.get('tournament')}\nError: {str(e)}")
            notification_message = ("❌ JOB FAILURE ALERT \n\n"
            f"🏷️ Resource: {APP_NAME}\n"
            "🔎 Job name: dailyIngestLatestEvents\n"
            "📋 Error details:\n" 
            "----------\n" 
            f"{str(e)}\n" 
            "----------\n\n" 
            "⚠️ Action Required: Please check the logs and resolve the issue."
            )
            notificator(message=notification_message)
    logging.info("PYLOG: Finished main()")
    return