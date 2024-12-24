import azure.functions as func
from functools import partial
import logging

from main_functions import *
from environment_variables import LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME

# TODO: Add return to function execute_with_notification so the http functions may return an error as well

app = func.FunctionApp()

@app.route(route="ingest-events-season", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingestEventsSeason")
def main_ingest_events_season(req: func.HttpRequest, context: func.Context) -> func.HttpResponse: 
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    season = req.get_json().get("season")
    logging.info(f"PYLOG: Executing http function to ingest events from {tournament} - {season}")
    status_code, response = fetch_and_upload_events(sport, tournament, season, 0)
    function_notificator(LOGIC_APPS_URL, TELEGRAM_CHAT_ID, status_code, response, APP_NAME, context.function_name, notificate_success=True)

    return http_response_template(func, status_code, response)   


@app.route(route="ingest-odds-season", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingestOddsSeason")
def main_ingest_odds(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    season = req.get_json().get("season")
    logging.info(f"PYLOG: Executing http trigger function to ingest odds for {tournament} - {season}")
    status_code, response = fetch_and_upload_odds(sport, tournament, season)
    function_notificator(LOGIC_APPS_URL, TELEGRAM_CHAT_ID, status_code, response, APP_NAME, context.function_name, notificate_success=True)
    
    return http_response_template(func, status_code, response)


@app.route(route="ingest-latest-events", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingestLatestEvents")
def main_ingest_latest_events(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    logging.info(f"PYLOG: Executing http function to ingest the latest events from {tournament}")
    status_code, response = ingest_latest_events(sport, tournament)
    function_notificator(LOGIC_APPS_URL, TELEGRAM_CHAT_ID, status_code, response, APP_NAME, context.function_name, notificate_success=True)

    return http_response_template(func, status_code, response)

@app.blob_trigger(
    arg_name="blob", 
    path="logs/sofascore/{sport}/{tournament}/events/{season}/ids.txt", 
    connection="AzureWebJobsStorage"
)
@app.function_name("ingestOddsBlobTrigger")
def main_ingest_odds(blob: func.InputStream, context: func.Context):
    blob_name = blob.name
    blob_parts = blob_name.split('/')
    sport, tournament, season = blob_parts[2], blob_parts[3], blob_parts[5]
    logging.info(f"PYLOG: Executing blob trigger function to ingest odds for {tournament} - {season}")
    status_code, response = fetch_and_upload_odds(sport, tournament, season)
    function_notificator(LOGIC_APPS_URL, TELEGRAM_CHAT_ID, status_code, f"{response}\n\nTriggered by blob:\n{blob_name}", APP_NAME, context.function_name, notificate_success=True)
    
    return http_response_template(func, status_code, response)