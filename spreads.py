import multiprocessing
from concurrent import futures
import itertools
from urllib.request import urlopen

import pandas as pd
from pandas.io.html import read_html
from bs4 import BeautifulSoup


_GAME_URL_TEMPLATE = ("http://www.teamrankings.com/nfl/matchup/"
					 "{hometeam}-{awayteam}-{week}-{year:n}"
					 "/spread-movement")
_SEASON_URL_TEMPLATE = ("http://www.pro-football-reference.com/years/"
					   "{year:n}"
					   "/games.htm")


class CantFindTheRightTable(Exception): pass


def game_url(hometeam, awayteam, week, year):
	"Calculate the URL for the spreads from hometeam to awayteam."
	if not isinstance(week, str):
		week = 'week-' + str(week)
	return _GAME_URL_TEMPLATE.format(
		hometeam=hometeam, awayteam=awayteam, week=week, year=year)


def game(hometeam, awayteam, week, year):
	"""Download, parse, and clean the spreads table for one game.

	The columns are pinnacle, betonline, bookmaker, datetime, hometeam,
	awayteam, week. The first three are the bookies.
	"""
	with urlopen(game_url(hometeam, awayteam, week, year)) as connection:
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


def season_games_url(year):
	"Calculate the URL for the games in season starting in `year`."
	return _SEASON_URL_TEMPLATE.format(year=year)


def season_games(year):
	"""Return a table of games and scores for the given season.

	The columns are Week, PtsW, PtsL, winner, loser, hometeam, awayteam, date.
	"""
	data = read_html(io=season_games_url(year),
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


def _download_game(args):
	retried = False
	while True:
		print('Attempting: %s' % (args,))
		try:
			g = game(*args)
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
				awayteam, hometeam = g.hometeam.copy(), g.awayteam.copy()
				g.hometeam, g.awayteam = hometeam, awayteam
				g['home_away_discrepency'] = True
	return g


def season(year, timeout=120, concurrency=None):
	futures_to_args  = {}
	tables = []
	if concurrency is None:
		concurrency = multiprocessing.cpu_count() * 2
	print('Concurrency: %d' % concurrency)
	games = season_games(year)
	weeks = []
	for week in games.week:
		try:
			weeks.append(int(week))
		except ValueError:
			weeks.append(week)
	args = zip(games.hometeam, games.awayteam, weeks)
	fail = []
	with futures.ThreadPoolExecutor(concurrency) as pool:
		for arg in args:
			arg = arg + (year,)
			futures_to_args[pool.submit(_download_game, arg)] = arg
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
	return games.merge(pd.concat(tables), on=('hometeam', 'awayteam', 'week'))
