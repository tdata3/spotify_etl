from secrets import REFRESH_TOKEN, base_64
import requests
import json

class Refresh:
    def __init__(self):
        self.REFRESH_TOKEN = REFRESH_TOKEN
        self.base_64 = base_64

    # Get new access token in order to access API
    def refresh(self):

        query = "https://accounts.spotify.com/api/token"
        request_post = requests.post(query,
                                data={"grant_type": "refresh_token", 
                                        "refresh_token": REFRESH_TOKEN},
                                headers={"Authorization": "Basic " + base_64})

        response_data = request_post.json()
        return response_data["access_token"]