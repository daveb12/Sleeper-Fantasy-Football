import json
from src.sleeper.sleeper_api import SleeperAPI
from src.config import LEAGUE_ID
from data.db_loader import DBLoader
import pandas as pd

def update_player_data():
    # Initialize SleeperAPI with your league ID
    sleeper = SleeperAPI(LEAGUE_ID)

    # Fetch player data
    player_data = sleeper.get_players()


    try: 

        player_records = []

        for sleeper_id, player in player_data.items():
            if player is None:  # Check if player is None
                continue

            player_record = {
                'sleeper_id': sleeper_id,
                'yahoo_id': player.get('yahoo_id'),
                'birth_country': player.get('birth_country'),
                'college': player.get('college'),
                'birth_date': pd.to_datetime(player.get('birth_date'), errors='coerce'),  # Convert to datetime
                'search_first_name': player.get('search_first_name'),
                'search_rank': player.get('search_rank'),
                'sportradar_id': player.get('sportradar_id'),
                'team_changed_at': pd.to_datetime(player.get('team_changed_at'), errors='coerce') if player.get('team_changed_at') else None,  # Convert to datetime
                'injury_body_part': player.get('injury_body_part'),
                'hashtag': player.get('hashtag'),
                'swish_id': player.get('swish_id'),
                'espn_id': player.get('espn_id'),
                'search_last_name': player.get('search_last_name'),
                'injury_status': player.get('injury_status'),
                'opta_id': player.get('opta_id'),
                'active': player.get('active'),
                'channel_id': player.get('metadata', {}).get('channel_id') if player.get('metadata') else None,
                'rookie_year': player.get('metadata', {}).get('rookie_year') if player.get('metadata') else None,
                'first_name': player.get('first_name'),
                'practice_participation': player.get('practice_participation'),
                'team': player.get('team'),
                'status': player.get('status'),
                'rotowire_id': player.get('rotowire_id'),
                'last_name': player.get('last_name'),
                'gsis_id': player.get('gsis_id'),
                'news_updated': player.get('news_updated'),
                'number': player.get('number'),
                'depth_chart_order': player.get('depth_chart_order'),
                'fantasy_positions': player.get('fantasy_positions', []),  # Ensure it's a list
                'high_school': player.get('high_school'),
                'full_name': player.get('full_name'),
                'rotoworld_id': player.get('rotoworld_id'),
                'fantasy_data_id': player.get('fantasy_data_id'),
                'birth_city': player.get('birth_city'),
                'depth_chart_position': player.get('depth_chart_position'),
                'search_full_name': player.get('search_full_name'),
                'practice_description': player.get('practice_description'),
                'stats_id': player.get('stats_id'),
                'pandascore_id': player.get('pandascore_id'),
                'sport': player.get('sport'),
                'age': player.get('age'),
                'position': player.get('position'),
                'birth_state': player.get('birth_state'),
                'injury_start_date': pd.to_datetime(player.get('injury_start_date'), errors='coerce') if player.get('injury_start_date') else None,  # Convert to datetime
                'height': player.get('height'),
                'years_exp': player.get('years_exp'),
                'team_abbr': player.get('team_abbr'),
                'weight': player.get('weight'),
                'oddsjam_id': player.get('oddsjam_id'),
                'injury_notes': player.get('injury_notes'),
            }
            player_records.append(player_record)

        df = pd.DataFrame(player_records)

         # Convert any necessary columns to specific dtypes
        df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
        df['team_changed_at'] = pd.to_datetime(df['team_changed_at'], errors='coerce')
        df['fantasy_positions'] = df['fantasy_positions'].apply(lambda x: '{' + ','.join(x) + '}' if isinstance(x, list) and x else '{}')
        df['pandascore_id'] = df['pandascore_id'].astype('Int64')  # Use 'Int64' for nullable integers
        df['injury_start_date'] = pd.to_datetime(df['injury_start_date'], errors='coerce')

        # Load data into the PostgreSQL database using DBLoader
        db_loader = DBLoader()  # Initialize your DBLoader
        conflict_columns = ['sleeper_id']  # Define your conflict columns
        db_loader.upsert_dataframe('players', df, conflict_columns)  # Upsert the DataFrame to the players table
        

        return {"status": "success", "message": "Records updated successfully!"}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    update_player_data()
