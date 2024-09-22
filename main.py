import pandas as pd
from src.sleeper.sleeper_api import SleeperAPI
from src.config import LEAGUE_ID, YEAR
from data.db_setup import DBSetup
from data.db_loader import DBLoader

def main():
    # Initialize SleeperAPI with your league ID
    sleeper = SleeperAPI(LEAGUE_ID)
    year = YEAR

    # Create an empty DataFrame to store all matchups data
    all_matchups_df = pd.DataFrame()

    # Loop through each week and fetch the matchups
    for week in range(1, 15):  # Weeks 1 to 14
        print(f"Processing week {week}...")

        # Get users and create a mapping with team_name
        users = sleeper.get_users()
        user_id_to_team_name = {
            user.get('user_id', 'unknown_id'): user.get('metadata', {}).get('team_name', user.get('display_name', 'Unknown'))
            for user in users
        }

        # Get rosters and map roster IDs to team names
        rosters = sleeper.get_rosters()
        roster_id_to_team_name = {
            roster['roster_id']: user_id_to_team_name.get(roster['owner_id'], 'Unknown')
            for roster in rosters
        }

        # Fetch matchups for the specific week
        matchups = sleeper.get_weekly_matchups(week)

        # Process each matchup and store the results in a DataFrame
        processed_matchups = {}
        for matchup in matchups:
            team1_roster_id = matchup.get('roster_id', None)
            team1_name = roster_id_to_team_name.get(team1_roster_id, 'Unknown')
            team1_starters_points = sum(matchup.get('starters_points', []))

            # Find opponent's roster ID and name
            opponent_roster_id = None
            for other_matchup in matchups:
                if other_matchup['matchup_id'] == matchup['matchup_id'] and other_matchup['roster_id'] != team1_roster_id:
                    opponent_roster_id = other_matchup['roster_id']
                    break

            team2_name = roster_id_to_team_name.get(opponent_roster_id, 'Unknown')
            team2_starters_points = sum(next((m.get('starters_points', []) for m in matchups if m['roster_id'] == opponent_roster_id), [])) if opponent_roster_id else 0.0

            # Ensure each matchup is only processed once
            if matchup['matchup_id'] not in processed_matchups:
                processed_matchups[matchup['matchup_id']] = {
                    'matchup_id': matchup['matchup_id'],
                    'week': week,
                    'year': year,
                    'team1_name': team1_name,
                    'team1_starters_points': team1_starters_points,
                    'team2_name': team2_name,
                    'team2_starters_points': team2_starters_points
                }

        # Convert matchups data to a DataFrame and append to the main DataFrame
        week_df = pd.DataFrame.from_dict(processed_matchups, orient='index')
        all_matchups_df = pd.concat([all_matchups_df, week_df], ignore_index=True)

        print(all_matchups_df)

    # Initialize DBLoader and upsert the DataFrame into the matchups table
    db_loader = DBLoader()
    db_loader.upsert_dataframe('matchups', all_matchups_df, conflict_columns=['matchup_id', 'week', 'year'])

if __name__ == "__main__":
    main()