import random
import json
import time
import re

from sofascore_api import SofascoreAPI
from utils import *
from environment_variables import STORAGE_ACCOUNT_CONNECTION_STRING

sofascore_api = SofascoreAPI()
blob_service_client = create_blob_service_client(connection_string=STORAGE_ACCOUNT_CONNECTION_STRING)

def fetch_and_upload_events(sport, tournament, season, start_page):
    """
    Fetches and uploads events for a specific season starting from a given page.
    :param sport: The name of the sport.
    :param tournament: The name of the tournament.
    :param season: The season to fetch events for.
    :param start_page: The page number to start fetching from.
    """
    page = start_page
    events_inserted = set()
    while True:
        logging.info(f"PYLOG: Fetching data from the {tournament} {season} season, page {page}")
        try:
            data_json = sofascore_api.get_tournament_events(sport, tournament, season, page)
        except Exception as e:
            if "403 Client Error: Forbidden for url" in str(e):
            # This means we're blocked temporarily by Sofascore so there is no point trying to get more data
                logging.info("PYLOG: '403 Client Error: Forbidden for url' while calling Sofascore APIs. Stopping execution...")
                raise
        events = data_json["events"]
        event_ids = {event["id"] for event in events}
        data_str = json.dumps(data_json)
        sport_formatted, tournament_formatted, season_formatted = sport.lower(), tournament.replace(' ', '_').lower(), season.replace('/', '-')
        
        # Upload data from the events in 'raw' container
        blob_path = f"sofascore/{sport_formatted}/{tournament_formatted}/events/{season_formatted}/{page}.json"
        upload_blob(blob_service_client, container_name="raw", blob_path=blob_path, data=data_str)
        # Keep track of the ingested event ids using the events_inserted set
        events_inserted.update(event_ids)

        if not data_json.get("hasNextPage"):
            break
        
        page += 1
        # time.sleep(random.uniform(2, 10))
    
    # Save ingested event ids in 'logs' container
    ingest_new_event_ids(sport, tournament, season, "events", events_inserted)

def get_events_with_no_odds_ingested(sport: str, tournament: str, season: str) -> set:
    path_log_events = f"sofascore/{sport}/{tournament}/events/{season}/ids.txt"
    path_log_odds = f"sofascore/{sport}/{tournament}/odds/{season}/ids.txt"
    blob_log_events = read_blob(blob_service_client, "logs", path_log_events)
    try:
        blob_log_odds = read_blob(blob_service_client, "logs", path_log_odds)
    except Exception as e:
        if str(e.__class__) == "<class 'azure.core.exceptions.ResourceNotFoundError'>" and e.error_code == "BlobNotFound":
            blob_log_odds = ""
        else:
            raise
    # Get the difference between events and odds inserted to know which events still need to get their odds collected
    events_ingested = set(map(int, blob_log_events.splitlines()))
    odds_ingested = set(map(int, blob_log_odds.splitlines())) 
    events_no_odds = events_ingested.difference(odds_ingested)
    if events_no_odds:
        return events_no_odds
    else:
        # Raise an exception in case no new events are dectected. This will go to the notification and I'll know how to handle this issue, that could be due to the tournament being stopped or just a normal day with no matches
        # TODO: find a way to define custom start and end time for tournaments so the ingestion will only run for a tournament in the specified time period
        logging.info(f"PYLOG: No new odds to collect for {tournament} - {season}")
        return


def fetch_and_upload_odds(sport: str, tournament: str, season: str):
    sport_formatted, tournament_formatted, season_formatted = sport.lower(), tournament.replace(' ', '_').lower(), season.replace('/', '-')
    events_no_odds = get_events_with_no_odds_ingested(sport_formatted, tournament_formatted, season_formatted)
    if events_no_odds is None:
        return
    odds_inserted = set()
    for event_id in events_no_odds:
        try:
            data_json = sofascore_api.get_event_odds(event_id)
        except Exception as e:
            raise
        data_str = json.dumps(data_json)
        upload_blob(blob_service_client, container_name="raw", blob_path=f"sofascore/{sport_formatted}/{tournament_formatted}/odds/{season_formatted}/{event_id}.json", data=data_str)
        odds_inserted.add(event_id)
        time.sleep(random.uniform(0, 0.95))
    # Save ingested event ids in 'logs' container
    ingest_new_event_ids(sport, tournament, season, "odds", odds_inserted)
    return

def ingest_new_event_ids(sport: str, tournament: str, season: str, data_source: str, new_event_ids: set):
    """
    Receive a set of recently collected event ids and update the ids.txt file in the logs container for the given tournament and season
    """
    sport_formatted, tournament_formatted, season_formatted = sport.lower(), tournament.replace(' ', '_').lower(), season.replace('/', '-')
    blob_path = f"sofascore/{sport_formatted}/{tournament_formatted}/{data_source}/{season_formatted}/ids.txt"
    try:
        blob_ids = read_blob(blob_service_client, "logs", blob_path)
    except Exception as e:
        if str(e.__class__) == "<class 'azure.core.exceptions.ResourceNotFoundError'>" and e.error_code == "BlobNotFound":
            blob_ids = ""
        else:
            raise
    ids = set(map(int, blob_ids.splitlines()))
    ids.update(new_event_ids)
    ids_string = "\n".join(map(str, ids))
    upload_blob(blob_service_client, container_name="logs", blob_path=blob_path, data=ids_string)
    return


def ingest_latest_events(sport, tournament):
    """
    Ingest data from the latest events for a specific tournament into Azure Blob Storage. But first, get data from Blob Storage to check what events need to be collected and ingested.
    """
    latest_season = sofascore_api.get_latest_season(sport, tournament)
    blobs = list_blobs(blob_service_client, "raw", f"sofascore/{sport.lower()}/{tournament.replace(" ", "_").lower()}/events/{latest_season.replace("/", "-")}/")
    if not blobs:
        latest_page = 0
    else: 
        match = re.search(r"/(\d+)\.json$", blobs[-1])
        latest_page = int(match.group(1))
    fetch_and_upload_events(sport, tournament, latest_season, latest_page)

def ingest_all_events(sport, tournament):
    """
    Fetch events for all seasons for a specific tournament and saves data in Azure Blob Storage.
    """
    # Find the specific sport and tournament in the data
    sport_data = find_item_in_array_of_objects(sofascore_api.sofascore_sports["sports"], key="name", value=sport, item_name="Sport")
    tournament_data = find_item_in_array_of_objects(sport_data["tournaments"], key="name", value=tournament, item_name="Tournament")

    # Iterate through all seasons of the specified tournament
    for season in tournament_data["seasons"]:
        fetch_and_upload_events(sport, tournament, season["value"], start_page=0)

def ingest_events(sport, tournament):
    blobs = list_blobs(blob_service_client, "raw", f"sofascore/{sport.lower()}/{tournament.lower()}/")
    if not blobs:
        logging.info(f"PYLOG: Ingesting all events for {tournament}")
        ingest_all_events(sport, tournament)
    else:
        logging.info(f"PYLOG: Ingesting latest events for {tournament}")
        ingest_latest_events(sport, tournament)