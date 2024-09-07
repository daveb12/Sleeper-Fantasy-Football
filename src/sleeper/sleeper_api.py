import requests

class SleeperAPI:
    BASE_URL = 'https://api.sleeper.app/v1/'

    def __init__(self, league_id):
        self.league_id = league_id

    def _make_request(self, endpoint):
        url = f"{self.BASE_URL}{endpoint}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_league_info(self):
        endpoint = f'league/{self.league_id}'
        return self._make_request(endpoint)

    def get_weekly_matchups(self, week):
        endpoint = f'league/{self.league_id}/matchups/{week}'
        return self._make_request(endpoint)

    def get_rosters(self):
        endpoint = f'league/{self.league_id}/rosters'
        return self._make_request(endpoint)
    
    def get_users(self):
        endpoint = f'league/{self.league_id}/users'
        return self._make_request(endpoint)