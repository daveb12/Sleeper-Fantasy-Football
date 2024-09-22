import json
from src.sleeper.sleeper_api import SleeperAPI
from src.config import LEAGUE_ID

def update_player_data():
    # Initialize SleeperAPI with your league ID
    sleeper = SleeperAPI(LEAGUE_ID)

    # Fetch player data
    player_data = sleeper.get_players()

    # Save player data to a JSON file
    with open('players_data.json', 'w') as json_file:
        json.dump(player_data, json_file, indent=4)

    print("Player data updated and saved to players_data.json")

if __name__ == "__main__":
    update_player_data()
