import pandas as pd
from src.sleeper.sleeper_api import SleeperAPI
from src.config import LEAGUE_ID, YEAR
from data.db_loader import DBLoader
from data.db_connection import DatabaseConnection  


def main(week=None):
    # Initialize SleeperAPI with your league ID
    sleeper = SleeperAPI(LEAGUE_ID)
    year = YEAR
    db_connection = DatabaseConnection()  # Using your db_connection.py file to connect to the DB

    # Create an empty DataFrame to store all matchups data
    all_matchups_df = pd.DataFrame()

    # Set the range of weeks to process
    weeks_to_process = [week] if week else range(1, 15)  # Process 1 week or all weeks (1 to 14)
    
    for week in weeks_to_process:
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

            # Get the starters' Sleeper IDs for both teams
            team1_starters_ids = matchup.get('starters', [])
            team2_starters_ids = next((m.get('starters', []) for m in matchups if m['roster_id'] == opponent_roster_id), []) if opponent_roster_id else []

            # Fetch player names from the database based on Sleeper IDs
            team1_player_names = get_player_names(team1_starters_ids, db_connection)
            team2_player_names = get_player_names(team2_starters_ids, db_connection)

            team1_player_names = get_player_names(team1_starters_ids, db_connection)
            team2_player_names = get_player_names(team2_starters_ids, db_connection)

            # Combine player names, teams, and positions into a string for display
            team1_starters_info = ', '.join(
                f"{player['full_name']} ({player['team']}, {player['position']})" 
                for player in team1_player_names
            )
            team2_starters_info = ', '.join(
                f"{player['full_name']} ({player['team']}, {player['position']})" 
                for player in team2_player_names
            )            

            # Ensure each matchup is only processed once
            if matchup['matchup_id'] not in processed_matchups:
                processed_matchups[matchup['matchup_id']] = {
                    'matchup_id': matchup['matchup_id'],
                    'week': week,
                    'year': year,
                    'team1_name': team1_name,
                    'team1_starters_points': team1_starters_points,
                    'team2_name': team2_name,
                    'team2_starters_points': team2_starters_points,
                    'team1_starters': team1_starters_info,
                    'team2_starters': team2_starters_info
                }

        # Convert matchups data to a DataFrame and append to the main DataFrame
        week_df = pd.DataFrame.from_dict(processed_matchups, orient='index')
        all_matchups_df = pd.concat([all_matchups_df, week_df], ignore_index=True)

        print(all_matchups_df)


def get_player_names(player_ids, db_connection):
    if not player_ids:
        return []

    # Print incoming player IDs for debugging
    print("Incoming player IDs:", player_ids)

    # Filter out non-numeric player IDs
    valid_player_ids = [str(pid) for pid in player_ids if isinstance(pid, (int, str)) and str(pid).isdigit()]

    # Print valid player IDs for debugging
    print("Valid player IDs:", valid_player_ids)

    # Prepare a query to fetch player names, teams, and positions
    if not valid_player_ids:  # Check if valid_player_ids is empty
        return []

    valid_ids_str = ', '.join(f"'{pid}'" for pid in valid_player_ids)
    query = f"""
    SELECT sleeper_id, full_name, team, position 
    FROM players 
    WHERE sleeper_id IN ({valid_ids_str})
    """

    # Use SQLAlchemy connection
    with db_connection.connect() as conn:
        player_data = pd.read_sql(query, conn)

    # Combine player names, teams, and positions
    player_details = player_data.to_dict(orient='records')

    return player_details




    # Initialize DBLoader and upsert the DataFrame into the matchups table
    # db_loader = DBLoader()
    # db_loader.upsert_dataframe('matchups', all_matchups_df, conflict_columns=['matchup_id', 'week', 'year'])

if __name__ == "__main__":
    # You can run for one specific week by passing the week number, or leave it empty for all weeks
    main(week=1)  # Example for week 1
    # main()  # Example for all weeks
