import azure.functions as func
from functools import partial
import logging

from main_functions import *
from environment_variables import LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME

app = func.FunctionApp()

@app.route(route="ingest-events-season", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingest-events-season")
def main_ingest_events_season(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    function_name = context.function_name
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    season = req.get_json().get("season")
    logging.info(f"PYLOG: Executing http function to ingest events from {tournament} - {season}")
    execute_with_notification(partial(fetch_and_upload_events, sport, tournament, season, 0), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, function_name, notificate_success=True)
    return func.HttpResponse(
        f"Function {function_name} with body {req.get_json()}\nFinished at: {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y - %H:%M:%S")}",
        status_code=200
    )


@app.route(route="ingest-latest-events", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingest-latest-events")
def main_ingest_latest_events(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    function_name = context.function_name
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    logging.info(f"PYLOG: Executing http function to ingest the latest events from {tournament}")
    execute_with_notification(partial(ingest_latest_events, sport, tournament), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, function_name, notificate_success=True)
    return func.HttpResponse(
        f"Function {function_name} with body {req.get_json()}\nFinished at: {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y - %H:%M:%S")}",
        status_code=200
    )


@app.route(route="ingest-odds-season", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.FUNCTION)
@app.function_name(name="ingest-odds-events")
def main_ingest_odds(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    sport = req.get_json().get("sport")
    tournament = req.get_json().get("tournament")
    season = req.get_json().get("season")
    function_name = context.function_name
    logging.info(f"PYLOG: Executing http trigger function to ingest odds for {tournament} - {season}")
    execute_with_notification(partial(fetch_and_upload_odds, sport, tournament, season), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, function_name, notificate_success=True)
    return func.HttpResponse(
        f"Function {function_name} with body {req.get_json()}\nFinished at: {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y - %H:%M:%S")}",
        status_code=200
    )


@app.blob_trigger(
    arg_name="blob", 
    path="logs/sofascore/{sport}/{tournament}/events/{season}/ids.txt", 
    connection="AzureWebJobsStorage"
)
@app.function_name("ingest-odds")
def main_ingest_odds(blob: func.InputStream, context: func.Context):
    blob_name = blob.name
    blob_parts = blob_name.split('/')
    sport, tournament, season = blob_parts[2], blob_parts[3], blob_parts[5]
    function_name = context.function_name
    logging.info(f"PYLOG: Executing blob trigger function to ingest odds for {tournament} - {season}")
    execute_with_notification(partial(fetch_and_upload_odds, sport, tournament, season), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, function_name, notificate_success=True)
    return


'''# Define main function
def main():
    request_bodies = load_request_body()
    for request_body in request_bodies:
        request_url = f"https://{APP_NAME}.azurewebsites.net/api/sofascore-ingest-latest-events"
        # execute_with_notification(partial(requests.post, url=request_url, json=request_body), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, "main()")
        execute_with_notification(partial(function_post_request, request_url="http://localhost:7071/api/sofascore-ingest-latest-events", request_body=request_body), LOGIC_APPS_URL, TELEGRAM_CHAT_ID, APP_NAME, "main()")
    return

# Call main()
if __name__=="__main__":
    main()'''