import json
import re

from sofascore_api import SofascoreAPI
from utils import *
from environment_variables import STORAGE_ACCOUNT_CONNECTION_STRING

sofascore_api = SofascoreAPI()
# TODO: change to managed identity connection
blob_service_client = create_blob_service_client(connection_string=STORAGE_ACCOUNT_CONNECTION_STRING)

def fetch_and_upload_events(sport: str, tournament: str, season:str, start_page: int) -> tuple:
    """
    Fetches and uploads events for a specific season starting from a given page.
    :param sport: The name of the sport to fetch data from.
    :param tournament: The name of the tournament to fetch data from.
    :param season: The season to fetch data from.
    :param start_page: The page number to start fetching from.
    :return: A tuple with the status code and a message.
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
            return 500, e
        events = data_json["events"]
        event_ids = {event["id"] for event in events}
        data_str = json.dumps(data_json)
        sport_formatted, tournament_formatted, season_formatted = sport.lower(), tournament.replace(' ', '_').lower(), season.replace('/', '-')
        
        # Upload data from the events in 'raw' container
        blob_path = f"sofascore/{sport_formatted}/{tournament_formatted}/events/{season_formatted}/{page}.json"
        try:
            upload_blob(blob_service_client, container_name="raw", blob_path=blob_path, data=data_str)
        except Exception as e:
            return 500, e
        # Keep track of the ingested event ids using the events_inserted set
        events_inserted.update(event_ids)

        if not data_json.get("hasNextPage"):
            break
        
        page += 1
    try:
        # Save ingested event ids in 'logs' container
        ingest_new_event_ids(sport, tournament, season, "events", events_inserted)
    except Exception as e:
        return 500, e
    
    return 200, f"Events ingested for {tournament} - {season}, from page {start_page}"
    

def get_events_with_no_odds_ingested(sport: str, tournament: str, season: str) -> set:
    """
    Get the event ids for the events that have been ingested but still don't have their odds collected.
    :param sport: The name of the sport to fetch data from.
    :param tournament: The name of the tournament to fetch data from.
    :param season: The season to fetch data from.
    :return: A set with the event ids that still don't have their odds collected.
    """
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


def fetch_and_upload_odds(sport: str, tournament: str, season: str) -> tuple:
    """
    Fetches and uploads events for a specific season starting from a given page.
    :param sport: The name of the sport to fetch data from.
    :param tournament: The name of the tournament to fetch data from.
    :param season: The season to fetch data from.
    :param start_page: The page number to start fetching from.
    :return: A tuple with the status code and a message.
    """
    sport_formatted, tournament_formatted, season_formatted = sport.lower(), tournament.replace(' ', '_').lower(), season.replace('/', '-')
    try:
        events_no_odds = get_events_with_no_odds_ingested(sport_formatted, tournament_formatted, season_formatted)
    except Exception as e:
        return 500, e
    if events_no_odds is None:
        return 200, f"No new odds to collect for {tournament} - {season}"
    odds_inserted = set()
    odds_not_found = set()
    for event_id in events_no_odds:
        try:
            data_json = sofascore_api.get_event_odds(event_id)
        except Exception as e:
            # Handle the case when the event is not found in Sofascore
            if "404 Client Error: Not Found for url" in str(e):
                odds_not_found.add(event_id)
                pass
                # TODO: add a way to report the missing event to the notification
            else:
                return 500, e
        data_str = json.dumps(data_json)
        try:
            upload_blob(blob_service_client, container_name="raw", blob_path=f"sofascore/{sport_formatted}/{tournament_formatted}/odds/{season_formatted}/{event_id}.json", data=data_str)
            odds_inserted.add(event_id)
        except Exception as e:
            return 500, e
    try:
        # Save ingested event ids in 'logs' container
        ingest_new_event_ids(sport, tournament, season, "odds", odds_inserted)
    except Exception as e:
        return 500, e
    
    if odds_not_found:
        return 200, f"Odds ingested for {tournament} - {season} from {len(odds_inserted)} events.\n{len(odds_not_found)} events were not found in Sofascore."
    else:
        return 200, f"Odds ingested for {tournament} - {season} from {len(odds_inserted)} events"

def ingest_new_event_ids(sport: str, tournament: str, season: str, data_source: str, new_event_ids: set) -> None:
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


def ingest_latest_events(sport: str, tournament: str) -> tuple:
    """
    Ingest data from the latest events for a specific tournament into Azure Blob Storage. But first, get data from Blob Storage to check what events need to be collected and ingested.
    """
    try:
        latest_season = sofascore_api.get_latest_season(sport, tournament)
    except Exception as e:
        return 500, e
    blobs = list_blobs(blob_service_client, "raw", f"sofascore/{sport.lower()}/{tournament.replace(' ', '_').lower()}/events/{latest_season.replace('/', '-')}/")
    if not blobs:
        latest_page = 0
    else: 
        match = re.search(r"/(\d+)\.json$", blobs[-1])
        latest_page = int(match.group(1))

    return fetch_and_upload_events(sport, tournament, latest_season, latest_page)