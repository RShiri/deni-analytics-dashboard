import requests
import json
from datetime import datetime

# Function to fetch the Portland Trail Blazers' schedule

def fetch_portland_schedule():
    url = 'https://api.nba.com/schedule/portland-trail-blazers'
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception('Failed to fetch schedule')

# Function to update the schedule based on game completion status

def update_schedule():
    schedule = fetch_portland_schedule()
    for game in schedule['games']:
        game_date = datetime.strptime(game['date'], '%Y-%m-%d')
        if game_date.date() < datetime.utcnow().date() and game['status'] == 'completed':
            # Logic to update completed games
            print(f'Updating data for completed game: {game['home_team']} vs {game['away_team']}')
        elif game_date.date() >= datetime.utcnow().date():
            # Logic to handle upcoming games
            print(f'Upcoming game: {game['home_team']} vs {game['away_team']}')
    # Add your logic to persist the updated data here

if __name__ == '__main__':
    update_schedule()