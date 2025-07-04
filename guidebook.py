from loguru import logger
import requests


class Guidebook:
    def __init__(self, api_key:str):
        self.api_key = api_key

    def headers(self):
        return {
            "Authorization": "JWT " + self.api_key
        }

    def get_response(self, url, params=None) -> requests.Response:
        response = requests.get(url, params=params, headers=self.headers())
        logger.info("Requested {}", response.url)
        return response

    def get_guides(self):
        URL = "https://builder.guidebook.com/open-api/v1.1/guides/"
        json = self.get_response(URL).json()
        return json

    def get_sessions(self, guide_id=None, ordering="start_time"):
        response_urls = []
        url = "https://builder.guidebook.com/open-api/v1.1/sessions/"
        params = {
            "guide": guide_id,
            # "ordering": ordering,
        }
        
        sessions = []
        response = self.get_response(url, params)
        response_urls.append(response.url)
        data = response.json()
        sessions += data["results"]
        next_url = data["next"]

        while next_url:
            response = self.get_response(next_url)
            response_urls.append(response.url)
            data = response.json()
            sessions += data["results"]
            next_url = data["next"]
        
        # Guidebook cannot reliably sort by start_times. It will
        # return duplicate IDs across different request/response pages
        # and miss some sessions.
        sessions.sort(key=lambda s: s["start_time"])

        return sessions, response_urls
    
    def get_locations(self, guide_id=None):
        response_urls = []
        url = "https://builder.guidebook.com/open-api/v1.1/locations/"
        params = {
            "guide": guide_id
        }

        locations = []
        response = self.get_response(url, params)
        response_urls.append(response.url)
        data = response.json()
        locations += data["results"]
        next_url = data["next"]

        while next_url:
            response = self.get_response(next_url)
            response_urls.append(response.url)
            data = response.json()
            locations += data["results"]
            next_url = data["next"]
        
        return locations, response_urls

    def get_schedule_tracks(self, guide_id=None):
        response_urls = []
        url = "https://builder.guidebook.com/open-api/v1.1/schedule-tracks/"
        params = {
            "guide": guide_id
        }

        schedule_tracks = []
        response = self.get_response(url, params)
        response_urls.append(response.url)
        data = response.json()
        schedule_tracks += data["results"]
        next_url = data["next"]

        while next_url:
            response = self.get_response(next_url)
            response_urls.append(response.url)
            data = response.json()
            schedule_tracks += data["results"]
            next_url = data["next"]
        
        return schedule_tracks, response_urls