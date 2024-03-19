import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree
import requests
from time import sleep
from random import random
from requests.exceptions import RequestException
import re
import itertools
from io import StringIO
from tqdm import tqdm
from competitions import competitions
import warnings
warnings.filterwarnings('ignore')

class FBREFScraper:
	def __init__(self, seasons, leagues):
		self.seasons = seasons
		self.leagues = leagues

		self.competitions = {'season': self.seasons, 'league': self.leagues}
		self.competitions_df = pd.DataFrame(list(itertools.product(*self.competitions.values())), columns = self.competitions.keys())
  
		self.games = pd.DataFrame()
		
	def get_comp_games(self, league, season):
		league_id = competitions[league]
		url = "https://fbref.com/en/comps/" + str(league_id) + "/" + str(season) + "/schedule/" + str(season) + "-" + league.replace(' ','-') + "-Scores-and-Fixtures"
		
		# Get competition page
		req = requests.get(url)
		if req.status_code==200:
      		# Get table of contents
			soup = BeautifulSoup(req.content, features="html.parser")
			table = soup.find_all('table')[0]

			df = pd.read_html(StringIO(str(table)))[0]
			df['season'] = season
			df['league'] = league

			df = df[(df['Home'].notna()) & (df['Home'] != 'Home')].reset_index(drop=True)

			# Get games' url
			table_rows = table.find('tbody').findAll("tr", class_ = False) 
			games_url = []
			for game in table_rows:
				game_url = game.find('td', attrs = {'data-stat':'match_report'}).find('a')['href']
				games_url.append('https://fbref.com/' + game_url)

			df['Match Report'] = games_url

			df['HomeGoals'] = df['Score'].apply(lambda x: x.split('–')[0] if '–' in x else None)
			df['AwayGoals'] = df['Score'].apply(lambda x: x.split('–')[1] if '–' in x else None)
   
			return df
		else:
			print(f'--- Request {url} failed with status: {req.status_code} ---')  
	
	def get_games(self):
		for index, row in tqdm(self.competitions_df.iterrows(), desc = 'Getting games'):
			try:
				df = self.get_comp_games(league= row['league'], season = row['season'])
				self.games = pd.concat([self.games, df], axis = 0)
			except:
				print(f'--- Error while scraping {row['league']} - {row['season']} games ---')
   
			# Sleeps for 3 seconds according to FBREF Terms & Conditions.
			sleep(3 + random())

	def get_stats(self, index, row):
		url = row['Match Report']
		req = requests.get(url)
		if req.status_code==200:
			req = requests.get(url)
			soup = BeautifulSoup(req.content, features="html.parser")

			# Coach and captain
			try:
				managers_captains = soup.find_all('div', class_ = 'datapoint')
    
				self.games.loc[index, 'manager_home'] = managers_captains[0].text.replace("Manager: ", "")
				self.games.loc[index, 'captain_home'] = managers_captains[1].text.replace("Captain: ", "")
				self.games.loc[index, 'manager_away'] = managers_captains[2].text.replace("Manager: ", "")
				self.games.loc[index, 'captain_away'] = managers_captains[3].text.replace("Captain: ", "")
			
			except:
				print("-- Error while getting Coachs or Captains --")

			# Venue city
			try:
				venue_city = soup.find('div', class_ = 'scorebox_meta').find_all('strong')[-2].find_next_sibling('small').text
				
				self.games.loc[index, 'venue_city'] = venue_city.split(', ')[1]

			except:
				print( "-- Error while getting Venue City --" )

			# Possession
			try:
				possession = soup.find('tr', string = 'Possession').find_next_sibling('tr').find_all('strong')
				
				self.games.loc[index, 'possessiontime_home'] = float(possession[0].text.strip('%')) / 100 if possession[0].text != '' else 0 
				self.games.loc[index, 'possessiontime_away'] = float(possession[1].text.strip('%')) / 100 if possession[1].text != '' else 0  
			except (AttributeError, IndexError, ValueError, TypeError, KeyError):
				print( "-- Error while getting Possession --" )

			# Total Shots
			try:
				shots_total = soup.find('tr', string = 'Shots on Target').find_next_sibling('tr').find_all('td')

				shots_total_home = shots_total[0].find('div').find('div').text
				shots_total_away = shots_total[1].find('div').find('div').text

				self.games.loc[index, 'shots_total_home'] = int(shots_total_home.split()[2]) if shots_total_home.split()[2] != '' else 0
				self.games.loc[index, 'shots_total_away'] = int(shots_total_away.split()[-1]) if shots_total_away.split()[-1] != '' else 0

			except (AttributeError, IndexError, ValueError, TypeError, KeyError):
				print( "-- Error while getting Total Shots --" )

			# Shots ongoal and offgoal
			try:
				shots_ongoal = soup.select('div[id*="all_keeper_stats_"]')

				shots_ongoal_against_home = shots_ongoal[0].find_all('td', attrs = {'data-stat': 'gk_shots_on_target_against'})
				shots_ongoal_against_away = shots_ongoal[1].find_all('td', attrs = {'data-stat': 'gk_shots_on_target_against'})

				shots_ongoal_away = 0
				for i in range(0, len(shots_ongoal_against_home)):
					shot = shots_ongoal_against_home[i].text if shots_ongoal_against_home[i].text != "" else 0
					shots_ongoal_away += int(shot) 

				shots_ongoal_home = 0
				for i in range(0, len(shots_ongoal_against_away)):
					shot = shots_ongoal_against_away[i].text if shots_ongoal_against_away[i].text != "" else 0 
					shots_ongoal_home += int(shot)

				self.games.loc[index, 'shots_ongoal_home'] = shots_ongoal_home
				self.games.loc[index, 'shots_ongoal_away'] = shots_ongoal_away

				self.games.loc[index, 'shots_offgoal_home'] = self.games['shots_total_home'][index] - self.games['shots_ongoal_home'][index]
				self.games.loc[index, 'shots_offgoal_away'] = self.games['shots_total_away'][index] - self.games['shots_ongoal_away'][index]

			except: 
				print( "-- Error while getting Shots ongoal and offgoal --" )


			# Saves
			try:
				saves = soup.find('tr', string = 'Saves').find_next_sibling('tr').find_all('td')

				saves_home = saves[0].find('div').find('div').text
				saves_away = saves[1].find('div').find('div').text

				self.games.loc[index, 'saves_home'] = saves_home.split()[0]
				self.games.loc[index, 'saves_away'] = saves_away.split()[-3]

			except:
				print( "-- Error while getting Saves --" )

			# Cards
			try: 
				cards = soup.find('tr', string = 'Cards').find_next_sibling('tr').find_all('td')

				yellow_cards_home = cards[0].find('div', class_ = 'cards').find_all('span', class_ = 'yellow_card')
				red_cards_home = cards[0].find('div', class_ = 'cards').find_all('span', class_ = 'red_card')
				yellow_red_cards_home = cards[0].find('div', class_ = 'cards').find_all('span', class_ = 'yellow_red_card')

				yellow_cards_away = cards[1].find('div', class_ = 'cards').find_all('span', class_ = 'yellow_card')
				red_cards_away = cards[1].find('div', class_ = 'cards').find_all('span', class_ = 'red_card')
				yellow_red_cards_away = cards[1].find('div', class_ = 'cards').find_all('span', class_ = 'yellow_red_card')

				self.games.loc[index, 'yellow_cards_home'] = len(yellow_cards_home)
				self.games.loc[index, 'red_cards_home'] = len(red_cards_home)
				self.games.loc[index, 'yellowred_cards_home'] = len(yellow_red_cards_home)
				self.games.loc[index, 'yellow_cards_away'] = len(yellow_cards_away)
				self.games.loc[index, 'red_cards_away'] = len(red_cards_away)
				self.games.loc[index, 'yellowred_cards_away'] = len(yellow_red_cards_away)

			except:
				print(f"-- Error while getting Cards: {url} -- " )

			# Fouls
			try:
				fouls_home = soup.find('div', string = 'Fouls').find_previous_sibling('div').text
				fouls_away = soup.find('div', string = 'Fouls').find_next_sibling('div').text

				self.games.loc[index, 'fouls_home'] = fouls_home
				self.games.loc[index, 'fouls_away'] = fouls_away

			except:
				print( "-- Error while getting Fouls --" )

			# Offsides
			try:
				offsides_home = soup.find('div', string = 'Offsides').find_previous_sibling('div').text
				offsides_away = soup.find('div', string = 'Offsides').find_next_sibling('div').text

				self.games.loc[index, 'offsides_home'] = offsides_home
				self.games.loc[index, 'offsides_away'] = offsides_away

			except:
				print( "-- Error while getting Offsides --" )

			# Formation
			try:
				formation_home = soup.find('div', class_ = 'lineup', id='a').find('tr').text
				formation_away = soup.find('div', class_ = 'lineup', id='b').find('tr').text

				self.games.loc[index, 'formation_home'] = formation_home.split(" ")[-1].split("(")[1].split(")")[0]
				self.games.loc[index, 'formation_away'] = formation_away.split(" ")[-1].split("(")[1].split(")")[0]

			except:
				print( "-- Error while getting formations --" )

			# Home lineups
			starting_num = 0
			bench_num = 0
			try:
				lineups_home = soup.find('div', class_ = 'lineup', id='a').find_all('a')

				starting_home = []
				bench_home = []
				for i in range(0, len(lineups_home)):
					if i in range(0, 11):
						starting_home.append(lineups_home[i].text)
					else:
						bench_home.append(lineups_home[i].text)

				try:
					players_stats_home = soup.select('div[id*="all_player_stats_"]')[0].find('tbody').find_all('tr')[::-1]
				except:
					print( "-- Error while getting player stats --" )

				for j in range(0, len(players_stats_home)):
					if players_stats_home[j].find('a').text in starting_home:
						try:
							self.games.loc[index, "starting_name_home" + str(starting_num + 1)] = players_stats_home[j].find('a').text
						except:
							print( f"-- Error while getting player home: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_age_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player home age: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_position_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player home position: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_minutes_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player home minutes: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_goals_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player home goals: {starting_num + 1} --" )
						starting_num += 1
					
					elif players_stats_home[j].find('a').text in bench_home:
						try:
							self.games.loc[index, "bench_name_home" + str(bench_num + 1)] = players_stats_home[j].find('a').text
						except:
							print( f"-- Error while getting player home: {bench_num+1} --" )
						try:
							self.games.loc[index, "bench_age_home" + str(bench_num + 1 )] = players_stats_home[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player home age: {bench_num+1} --" )
						try:
							self.games.loc[index, "bench_position_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player home position: {bench_num+1} --" )
						try:
							self.games.loc[index, "bench_minutes_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player home minutes: {bench_num+1} --" )
						try:
							self.games.loc[index, "bench_goals_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player home goals: {bench_num+1} --" )
						bench_num += 1
			except:
				pattern = re.compile(r'^\s*(&nbsp;|\s)+')
				try:
					players_stats_home = soup.select('div[id*="all_player_stats_"]')[0].find('tbody').find_all('tr')[::-1]
				except:
					print( "-- Error while getting player stats --" )
				
				for j in range(0, len(players_stats_home)):
					player = players_stats_home[j].find('th').text
					if pattern.match(player):
						try:
							self.games.loc[index, "bench_name_home" + str(bench_num + 1)] = players_stats_home[j].find('a').text
						except:
							print( f"-- Error while getting player home: {bench_num + 1} --" )
						try:
							self.games.loc[index, "bench_age_home" + str(bench_num + 1 )] = players_stats_home[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player home age: {bench_num + 1} --" )
						try:
							self.games.loc[index, "bench_position_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player home position: {bench_num + 1} --" )
						try:
							self.games.loc[index, "bench_minutes_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player home minutes: {bench_num + 1} --" )
						try:
							self.games.loc[index, "bench_goals_home" + str(bench_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player home goals: {bench_num + 1} --" )
						bench_num += 1
					else:
						try:
							self.games.loc[index, "starting_name_home" + str(starting_num + 1)] = players_stats_home[j].find('a').text
						except:
							print( f"-- Error while getting player home: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_age_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player home age: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_position_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player home position: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_minutes_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player home minutes: {starting_num + 1} --" )
						try:
							self.games.loc[index, "starting_goals_home" + str(starting_num + 1)] = players_stats_home[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player home goals: {starting_num + 1} --" )
						starting_num += 1
			
			# Away lineups
			starting_num = 0
			bench_num = 0
			try:
				lineups_away = soup.find('div', class_ = 'lineup', id='b').find_all('a')

				starting_away = []
				bench_away = []
				for i in range(0, len(lineups_away)):
					if i in range(0, 11):
						starting_away.append(lineups_away[i].text)
					else:
						bench_away.append(lineups_away[i].text)
				try:
					players_stats_away = soup.select('div[id*="all_player_stats_"]')[1].find('tbody').find_all('tr')[::-1]
				except:
					print( "-- Error while getting player stats --" )

				for j in range(0, len(players_stats_away)):
					if players_stats_away[j].find('a').text in starting_away:
						try:
							self.games.loc[index, "starting_name_away" + str(starting_num + 1)] = players_stats_away[j].find('a').text
						except:
							print( f"-- Error while getting player away: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_age_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player away age: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_position_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player away position: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_minutes_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player away minutes: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_goals_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player away goals: {starting_num + 1}" )
						starting_num += 1

					elif players_stats_away[j].find('a').text in bench_away:
						try:
							self.games.loc[index, "bench_name_away" + str(bench_num + 1)] = players_stats_away[j].find('a').text
						except:
							print( f"-- Error while getting player away: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_age_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player away age: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_position_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player away position: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_minutes_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player away minutes: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_goals_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player away goals: {bench_num + 1}" )
						bench_num += 1
			except:
				try:
					players_stats_away = soup.select('div[id*="all_player_stats_"]')[1].find('tbody').find_all('tr')[::-1]
				except:
					print( "-- Error while getting player stats --" )

				for j in range(0, len(players_stats_away)):
					player = players_stats_away[j].find('th').text
					if pattern.match(player):
						try:
							self.games.loc[index, "bench_name_away" + str(bench_num + 1)] = players_stats_away[j].find('a').text
						except:
							print( f"-- Error while getting player away: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_age_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player away age: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_position_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player away position: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_minutes_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player away minutes: {bench_num + 1}" )
						try:
							self.games.loc[index, "bench_goals_away" + str(bench_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player away goals: {bench_num + 1}" )
						bench_num += 1
					else:
						try:
							self.games.loc[index, "starting_name_away" + str(starting_num + 1)] = players_stats_away[j].find('a').text
						except:
							print( f"-- Error while getting player away: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_age_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'age'}).text
						except:
							print( f"-- Error while getting player away age: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_position_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'position'}).text
						except:
							print( f"-- Error while getting player away position: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_minutes_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'minutes'}).text
						except:
							print( f"-- Error while getting player away minutes: {starting_num + 1}" )
						try:
							self.games.loc[index, "starting_goals_away" + str(starting_num + 1)] = players_stats_away[j].find('td', attrs = {'data-stat': 'goals'}).text
						except:
							print( f"-- Error while getting player away goals: {starting_num + 1}" )
						starting_num += 1
 
	def get_games_stats(self):
		for index, row in tqdm(self.games.iterrows(), desc = 'Scraping games stats'):
			self.get_stats(index =index, row=row)

			# Sleeps for 3 seconds according to FBREF Terms & Conditions.
			sleep(3 + random())

	def run(self):
		# Get games
		self.get_games()

		# Get games stats
		self.get_games_stats()

def main():
	Scraper = FBREFScraper(
		seasons = [2022, 2023],
		leagues = ['Copa Libertadores', 'Copa Sudamericana', 'Primera Division', 'Copa de la Liga Profesional']
	)
	
	# Get games
	Scraper.run()
 
	# Save games df to csv
	Scraper.games.to_csv('games.csv', encoding='utf-8-sig', index = False)
 
 
if __name__ == "__main__":
    main()

