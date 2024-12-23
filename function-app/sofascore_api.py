import requests
import logging
import json

class SofascoreAPI:

    BASE_URL = "https://www.sofascore.com"
    API_BASE_URL = "https://sofascore.com/api/v1"

    def __init__(self):
        """
        Initializes SofascoreAPIs with sports and tournaments data.
        """
        # Initialize data with the provided data in file sofascore_sports.json
        with open("sofascore_sports.json", "r") as file:
            self.sofascore_sports = json.load(file)
        
        self.timeout = 10

    
    def fetch_data(self, url):
        """Generic method to fetch data from the API."""
        try:
            logging.info(f"PYLOG: Running get request for url > {url}")
            response = requests.get(url, timeout=self.timeout)  
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_error:
            logging.info(f"PYLOG: HTTP error in function fetch_data\nError: {str(http_error)}")
            raise http_error
        except requests.exceptions.ConnectionError as connection_error:
            logging.info(f"PYLOG: Connection error in function fetch_data\nError: {str(connection_error)}")
            raise connection_error
        except requests.exceptions.Timeout as timeout_error:
            logging.info(f"PYLOG: Timeout error in function fetch_data\nError: {str(timeout_error)}")
            raise timeout_error
        except requests.exceptions.RequestException as request_error:
            logging.info(f"PYLOG: Error in function fetch_data\nError: {str(request_error)}")
            raise request_error


    def find_tournament_and_season(self, sport_name, tournament_name, season_value):
        """
        Finds the unique tournament ID and season ID for a given sport, tournament, and season.
        :param sport_name: The name of the sport (e.g., "Football", "Basketball").
        :param tournament_name: The name of the tournament (e.g., "NBA").
        :param season_value: The exact season value (e.g., "2024/2025").
        :return: A tuple of (unique_tournament_id, season_id) or (None, None) if not found.
        """
        sport_name_lower = sport_name.lower()
        tournament_name_lower = tournament_name.lower()

        # Find the sport by name
        for sport in self.sofascore_sports['sports']:
            if sport['name'].lower() == sport_name_lower:
                # Find the tournament within the sport
                for tournament in sport['tournaments']:
                    if tournament['name'].lower() == tournament_name_lower:
                        # Find the season within the tournament
                        for season in tournament['seasons']:
                            if season['value'] == season_value:
                                return tournament['id'], season['id']
                        # If no season is found, return the tournament and None for the season
                        return tournament['id'], None
                break # Exit loop if tournament is found but no valid season matches

        return None, None
    
    def get_latest_season(self, sport, tournament):
        """
        Get the latest season for a given sport and tournament from sofascore_sports.json.
        :param sport: The name of the sport (e.g., "Basketball").
        :param tournament: The name of the tournament (e.g., "NBA").
        :return: The latest season name (e.g., "2024/2025") or None if not found.
        """
        # Access the sport data
        sport_data = next((s for s in self.sofascore_sports["sports"] if s["name"] == sport), None)
        if not sport_data:
            raise ValueError(f"Sport '{sport}' not found.")
        
        # Access the tournament data
        tournament_data = next((t for t in sport_data["tournaments"] if t["name"] == tournament), None)
        if not tournament_data:
            raise ValueError(f"Tournament '{tournament}' not found under sport '{sport}'.")
        
        # Access the latest season
        seasons = tournament_data.get("seasons", [])
        if not seasons:
            raise ValueError(f"No seasons found for tournament '{tournament}' under sport '{sport}'.")
        
        return seasons[0]["value"]  # The first season is the latest


    def get_tournament_events(self, sport, tournament, season, page):
        """
        Fetch events for a specific tournament and season using user-friendly inputs.
        :param tournament: The name of the tournament (e.g., "NBA").
        :param season: The season value (e.g., "2024/2025").
        :param page: The page number for the API pagination.
        :return: API response data or None if an error occurs.
        """
        unique_tournament_id, season_id = self.find_tournament_and_season(sport, tournament, season)
        if not unique_tournament_id or not season_id:
            logging.info("PYLOG: Invalid sport, tournament or season. Cannot fetch events.")
            return None
        url = f"{self.API_BASE_URL}/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}"
        return self.fetch_data(url)
    

    def get_event_odds(self, event_id):
        """
        Fetch odds for a specific event.
        :param event_id: The unique id for the event.
        """
        url = f"{self.API_BASE_URL}/event/{event_id}/odds/1/all"
        return self.fetch_data(url)