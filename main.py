from src.sleeper.sleeper_api import SleeperAPI
from src.config import LEAGUE_ID
from data.db_setup import DBSetup  # Import the DBSetup class

def main():
    # Initialize the database setup and create tables
    db_setup = DBSetup()
    db_setup.create_tables()

    # Initialize SleeperAPI with your league ID
    sleeper = SleeperAPI(LEAGUE_ID)

    # Get users and create a mapping with team_name
    users = sleeper.get_users()
    user_id_to_team_name = {
        user.get('user_id', 'unknown_id'): user.get('metadata', {}).get('team_name', user.get('display_name', 'Unknown'))
        for user in users
    }

    # Get league info
    league_info = sleeper.get_league_info()
    print(f"League Name: {league_info['name']}")
    print(f"Number of Teams: {league_info['total_rosters']}")

    # Get rosters and map roster IDs to team names
    rosters = sleeper.get_rosters()
    roster_id_to_team_name = {
        roster['roster_id']: user_id_to_team_name.get(roster['owner_id'], 'Unknown')
        for roster in rosters
    }

    # Fetch matchups for a specific week
    week = 1  # Replace this with the actual week number
    matchups = sleeper.get_weekly_matchups(week)

    # Create a dictionary to store processed matchups
    processed_matchups = {}

    # Process each matchup
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
        team2_starters_points = sum(other_matchup.get('starters_points', [])) if opponent_roster_id else 0.0

        # Ensure each matchup is only printed once
        if matchup['matchup_id'] not in processed_matchups:
            processed_matchups[matchup['matchup_id']] = {
                'team1_name': team1_name,
                'team1_starters_points': team1_starters_points,
                'team2_name': team2_name,
                'team2_starters_points': team2_starters_points,
            }

    # Sort and print matchups based on matchup ID (descending order)
    for matchup_id, result in sorted(processed_matchups.items(), key=lambda x: x[0], reverse=True):
        print(f"Matchup ID: {matchup_id}")
        print(f"Team 1: {result['team1_name']} - Starters Points: {result['team1_starters_points']:.1f}")
        print(f"Team 2: {result['team2_name']} - Starters Points: {result['team2_starters_points']:.1f}")
        print("-" * 40)

if __name__ == "__main__":
    main()
