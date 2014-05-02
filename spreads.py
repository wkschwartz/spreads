import multiprocessing
from concurrent import futures
import itertools
from urllib.request import urlopen

import pandas as pd
from pandas.io.html import read_html
from bs4 import BeautifulSoup


SEASON_URL_TEMPLATE = "http://www.pro-football-reference.com/years/{year:n}/games.htm"

class CantFindTheRightTable(Exception): pass


def one_game_url(hometeam, awayteam, week, year):
	"Calculate the URL for the spreads from hometeam to awayteam."
	base = "http://www.teamrankings.com/nfl/matchup/"
	template = "{hometeam}-{awayteam}-{week}-{year}"
	tail = "/spread-movement"
	if not isinstance(week, str):
		week = 'week-' + str(week)
	result = template.format(hometeam=hometeam, awayteam=awayteam, week=week, year=year)
	return ''.join([base, result, tail])


def one_game_table(hometeam, awayteam, week, year):
	"""Download, parse, and clean the spreads table for one game.

	The columns are pinnacle, betonline, bookmaker, datetime, hometeam,
	awayteam, week. The first three are the bookies.
	"""
	with urlopen(one_game_url(hometeam, awayteam, week, year)) as connection:
		page = connection.read()
	# Note that infer_types is deprecated and won't work starting in Pandas 0.14
	data = read_html(io=page.decode('utf-8'),
					 match="History", attrs={'id': 'table-000'},
					 infer_types=False, header=0,
					 skiprows=[1, 2, 3])
	if len(data) != 1:
		raise CantFindTheRightTable
	data = data.pop()

	# Cleaning.
	datetime = pd.to_datetime(data['Unnamed: 0'].replace(
		r'(\d\d?/\d\d?)', r'\1/' + str(year), regex=True))
	del data['Unnamed: 0']

	# Replace all the '--' as missing so we can convert numbers to floats.
	for column in data.keys():
		data[column] = data[column].replace('--', 'nan').replace('(Pick)', 0).apply(float)

	# Add datetime back in after the str-to-float conversion so we don't do it
	# for the datetime.
	data['datetime'] = datetime

	# Add this function's arguments to the table.
	data['hometeam'] = hometeam
	data['awayteam'] = awayteam
	data['week'] = week

	# Lowercase column names for ease of programming later
	data.columns = [h.lower() for h in data.columns]

	# Get favored team from the big "WAS -4.0" that shows up in the middle of
	# the page.
	soup = BeautifulSoup(page)
	abbrev = soup.find('div', attrs={'class': 'module point-spreads'}).find(
		'a').contents[0].split()[0]
	# It'll be something like WAS for Redskins or PHI for Eagles. Translate by
	# finding the links in the page that show up as WAS but have links to the
	# Redskins.
	links = soup.find('p', attrs={'class': 'h1-sub'}).find('strong').findAll('a')
	for link in links:
		if abbrev in link:
			data['favored'] = link['href'].split('-')[-1]
			break
	else:
		raise ValueError("couldn't figure out who %s is" % abbrev)

	return data


def season_spreads_table(year, timeout=120, concurrency=None):
	def worker(args):
		retried = False
		while True:
			print('Attempting: %s' % (args,))
			try:
				game = one_game_table(*args)
			except (CantFindTheRightTable, ValueError):
				if not retried:
					# Maybe the home/away info is bad, so swap teams.
					args = list(args)
					args[0], args[1] = args[1], args[0]
					retried = True
				else:
					return None
			else:
				if retried:
					awayteam, hometeam = game.hometeam.copy(), game.awayteam.copy()
					game.hometeam, game.awayteam = hometeam, awayteam
					game['home_away_discrepency'] = True
				return game
	futures_to_args  = {}
	tables = []
	if concurrency is None:
		concurrency = multiprocessing.cpu_count() * 2
	print('Concurrency: %d' % concurrency)
	season = season_table(year)
	weeks = []
	for week in season.week:
		try:
			weeks.append(int(week))
		except ValueError:
			weeks.append(week)
	args = zip(season.hometeam, season.awayteam, weeks)
	fail = []
	with futures.ThreadPoolExecutor(concurrency) as pool:
		for arg in args:
			arg = arg + (year,)
			futures_to_args[pool.submit(worker, arg)] = arg
		for future in futures.as_completed(futures_to_args, timeout=timeout):
			args = futures_to_args[future]
			try:
				table = future.result()
			except Exception as exc:
				print("%r generated an exception: %s" % (args, exc))
				fail.append(args)
			else:
				if table is None:
					fail.append(args)
				else:
					print('Success: %s' % (args,))
					tables.append(table)
	for args in fail:
		print('Fail: %s' % (args,))
	return season.merge(pd.concat(tables), on=('hometeam', 'awayteam', 'week'))


def season_table(year):
	"""Return a table of games and scores for the given season.

	The columns are Week, PtsW, PtsL, winner, loser, hometeam, awayteam, date.
	"""
	data = read_html(io=SEASON_URL_TEMPLATE.format(year=year),
					  attrs={'id': 'games'},
					  infer_types=False,
					  header=0)
	if len(data) != 1:
		raise CantFindTheRightTable
	data = data.pop()

	# Cleaning.
	del data["Unnamed: 3"]
	data = data[data.Week != "Week"]
	data = data[data.Week != "nan"]
	data['week'] = data.Week.replace("WildCard", "wild-card").replace("Division", "divisional").replace("ConfChamp", "conference").replace("SuperBowl", "super-bowl")
	data.week = data.week.apply(
		lambda s: int(s) if all(c in '1234567890' for c in s) else s)
	del data['Week']

	data['game_date'] = pd.to_datetime(data.Date.replace("$", ", %d" % year, regex=True))
	del data['Date']

	for column in "PtsW", "PtsL", "YdsW", "TOW", "YdsL", "TOL":
	    data[column] = data[column].apply(int)

	data['WatL'] = data['Unnamed: 5'].apply(lambda x: x == '@')
	del data['Unnamed: 5']
	data['hometeam'] =  data.WatL * data['Winner/tie'] + ~data.WatL * data['Loser/tie']
	data['awayteam'] = ~data.WatL * data['Winner/tie'] +  data.WatL * data['Loser/tie']
	data['winner'] = data['Winner/tie']
	data['loser'] = data['Loser/tie']
	for column in 'Winner/tie', 'Loser/tie', "WatL":
		del data[column]
	for column in 'hometeam', 'awayteam', 'winner', 'loser':
		data[column] = data[column].apply(lambda s: s.split()[-1].lower())

	return data
