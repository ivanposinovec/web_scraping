import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from time import sleep
from random import random
from tqdm import tqdm
from teams import teams_dict, teams_inv
from aux_functions import assign_teams

class PlayerLogScraper:
    def __init__(self, games):
        self.games = games
        
        self.players = pd.DataFrame()
        
    def get_match_players_stats(self, url, away_code, home_code, season):
        # Request Game URL
        req = requests.get(url)
        if req.status_code==200:
            # Get the 4 tables (2 for basic and advanced stats for home and away teams)
            soup = BeautifulSoup(req.content, 'html.parser')
            away_basic_html = soup.find_all('table', id="box-" + away_code + "-game-basic")[0]
            away_basic = pd.read_html(StringIO(str(away_basic_html)))[0]
            away_advanced_html = soup.find_all('table', id="box-" + away_code + "-game-advanced")[0]
            away_advanced = pd.read_html(StringIO(str(away_advanced_html)))[0]
            
            home_basic_html = soup.find_all('table', id="box-" + home_code + "-game-basic")[0]
            home_basic = pd.read_html(StringIO(str(home_basic_html)))[0]
            home_advanced_html = soup.find_all('table', id="box-" + home_code + "-game-advanced")[0]
            home_advanced = pd.read_html(StringIO(str(home_advanced_html)))[0]

            # Merge columns based on players names
            home_basic.columns = home_basic.columns.droplevel()
            home_basic["starting"] = [1]*5 + [0]*(len(home_basic)-5)
            home_basic['is_home'] = 1
            home_advanced.columns = home_advanced.columns.droplevel()
            
            home = pd.merge(home_basic, home_advanced, on = 'Starters', how = 'outer', suffixes =('','_y'))
            home = home.drop(columns=home.filter(like='_y').columns)
            
            away_basic.columns = away_basic.columns.droplevel()
            away_basic["starting"] = [1]*5 + [0]*(len(away_basic)-5)
            away_basic['is_home'] = 0
            away_advanced.columns = away_advanced.columns.droplevel()
            
            away = pd.merge(away_basic, away_advanced, on = 'Starters', how = 'outer', suffixes =('','_y'))
            away = away.drop(columns=away.filter(like='_y').columns)
            
            # Clean columns and create their own variables
            home_totals = home[home['Starters'] == 'Team Totals']
            away_totals = away[away['Starters'] == 'Team Totals']
            
            home_totals_team = pd.concat([home_totals] * (len(home)-2), ignore_index=True)
            home_totals_team = home_totals_team.drop(columns=['Starters', 'MP', '+/-', 'starting', 'is_home', 'USG%', 'BPM']).add_suffix('_team')
            home_totals_rival = pd.concat([away_totals] * (len(home)-2), ignore_index=True)
            home_totals_rival = home_totals_rival.drop(columns=['Starters', 'MP', '+/-', 'starting', 'is_home', 'USG%', 'BPM']).add_suffix('_rival')
            
            away_totals_team = pd.concat([away_totals] * (len(away)-2), ignore_index=True)
            away_totals_team = away_totals_team.drop(columns=['Starters', 'MP', '+/-', 'starting', 'is_home', 'USG%','BPM']).add_suffix('_team')
            away_totals_rival = pd.concat([home_totals] * (len(away)-2), ignore_index=True)
            away_totals_rival = away_totals_rival.drop(columns=['Starters', 'MP', '+/-', 'starting', 'is_home', 'USG%','BPM']).add_suffix('_rival')
            
            # Concat all columns for both home and away
            home = home[~home['Starters'].isin(['Team Totals', 'Reserves'])].reset_index(drop = True)
            home = pd.concat([home, home_totals_team, home_totals_rival], axis = 1)
            
            away = away[~away['Starters'].isin(['Team Totals', 'Reserves'])].reset_index(drop = True)
            away = pd.concat([away, away_totals_team, away_totals_rival], axis = 1)
            
            # Get Home Players URL
            home_players = home_basic_html.find('tbody').find_all('tr', class_ = False)
            home_players_url = []
            for player in home_players:
                player_url = player.find('th').find('a')['href'].replace('.html', f'/gamelog/{season}')
                home_players_url.append("https://www.basketball-reference.com" + player_url)
            try:
                home['player_url'] = home_players_url
            except:
                print(f"Error while getting home players url in game: {url}")
                
            # Get Away Players URL
            away_players = away_basic_html.find('tbody').find_all('tr', class_ = False)
            away_players_url = []
            for player in away_players:
                player_url = player.find('th').find('a')['href'].replace('.html', f'/gamelog/{season}')
                away_players_url.append("https://www.basketball-reference.com" + player_url)
            try:
                away['player_url'] = away_players_url
            except:
                print(f"Error while getting away players url in game: {url}")
                
            # Concat home and away df in a unique df
            df = pd.concat([home,away], ignore_index = True)
            return df
        else:
            print(f'--- Request {url} failed with status: {req.status_code} ---')   

    def run(self):
        for index, row in tqdm(self.games.iterrows(), desc = "Getting game stats"):
            # Get match stats
            players_df = self.get_match_players_stats(row['boxscore'], row['away_code'], row['home_code'], row['season'])
            
            # Merge match stats with the data in  games df
            rows = pd.concat([pd.DataFrame([row])] * len(players_df), ignore_index = True)
            players_df = pd.merge(rows, players_df, left_index=True, right_index=True)	
            
            # Add rows to the players df
            self.players = pd.concat([self.players, players_df], axis = 0, ignore_index = True)
            
            # Sleeps for 3 seconds according to Basketball Reference Terms & Conditions.
            sleep(3 + random())
        
        self.players = self.players.apply(assign_teams, axis=1).drop(columns = ['home', 'home_pts', 'away', 'away_pts'])
        self.players.rename(columns = {'Starters':'player'}, inplace = True)
        self.players = self.players[['player', 'player_url', 'team', 'rival', 'pts_team', 'pts_rival'] + [col for col in self.players.columns if col not in ['player', 'player_url', 'team', 'rival', 'pts_team', 'pts_rival']]]

def main():
    Scraper = PlayerLogScraper(
        games = pd.read_csv('nba_scraper/games.csv')
    )
    
    # Run code
    Scraper.run()
    
    # Save players df to csv
    Scraper.players.to_csv('players.csv', index=False)

if __name__ == '__main__':
	main()
