import pandas as pd
import requests
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup
from time import sleep
from random import random
from tqdm import tqdm
from aux_functions import convert_time_format
from teams import teams_dict,teams_inv

class GamesScraper:
    def __init__(self, season):
        self.season = season
        self.games = pd.DataFrame()
        
    def get_games(self, month):
        req = requests.get(f'https://www.basketball-reference.com/leagues/NBA_{self.season}_games-{month.lower()}.html')
        if req.status_code==200:
            # Get table content
            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find('table', attrs={'id': 'schedule'})
            if table:
                month_games = pd.read_html(StringIO(str(table)))[0]
                
            # Get games urls
            table = soup.find('table').find('tbody').findAll("tr", class_ = False)

            boxscore_links = []
            for row in table:
                try:
                    boxscore = row.find("td", attrs = {'data-stat':'box_score_text'}).find('a')['href']
                    boxscore_links.append('https://www.basketball-reference.com'+ boxscore)
                except:
                    boxscore_links.append(None)
            month_games['boxscore'] = boxscore_links
            return month_games
        else:
            print(f'--- Request {self.season}-{month.lower()} failed with status: {req.status_code} ---')   

    def run(self, playoffs=False):
        # Seasons have different month associated with
        if self.season==2020:
            months = ['October-2019', 'November', 'December', 'January', 'February', 'March',
                    'July', 'August', 'September', 'October-2020']
        elif self.season==2021:
            months = ['December', 'January', 'February', 'March', 'May', 'June',
                    'July']
        elif self.season==2024:
            months = ['October', 'November', 'December', 'January']
        else:
            months = ['October', 'November', 'December', 'January', 'February', 'March',
            'April', 'May', 'June']

        for month in tqdm(months, desc = f'Getting games from season {self.season}'):
            month_games = self.get_games(month)
            self.games = pd.concat([self.games, month_games], axis = 0)
            
            # Sleeps for 3 seconds according to Basketball Reference Terms & Conditions.
            sleep(3+random())
        
        self.games = self.games.reset_index(drop = True)
        self.games = self.games.drop(['Unnamed: 6', 'Attend.', 'Arena'], axis=1)
        self.games.columns = ['date', 'start_time', 'away', 'away_pts', 'home', 'home_pts', 'overtime', 'type', 'boxscore']

        self.games['season'] = self.season


def main():
    # Run code for every season
    seasons = [2023, 2024]
    df = pd.DataFrame()
    for season in seasons:
        Scraper = GamesScraper(season = season)
        Scraper.run(season)
        df = pd.concat([df, Scraper.games], axis=0)
    
    # Cleaning
    df['start_time'] = df['start_time'].apply(convert_time_format)
    df['datetime'] = df['date'] + ' ' +  df['start_time']

    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%a, %b %d, %Y"))
    df['datetime'] = df['datetime'].apply(lambda x: datetime.strptime(x, "%a, %b %d, %Y %H:%M:%S"))

    df['timestamp'] = df['datetime'].apply(lambda x: int(x.timestamp()))

    df['away_code'] = df['away'].map(teams_inv)
    df['home_code'] = df['home'].map(teams_inv)
    
    df = df[['date', 'datetime', 'timestamp', 'season', 'away', 'away_code', 'away_pts', 'home', 'home_code', 'home_pts', 'overtime', 'type', 'boxscore']]
    
    # Save file
    df.to_csv('games.csv', index=False)

if __name__ == '__main__':
    main()
