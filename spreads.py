"""Download and clean historical betting spreads for NFL games.

In all the functions below, identify games by a home team, away team, week, and
year. We indicate teams by name rather than city: 'redskins' rathern than
'Washington', 'eagles' rather than 'PHL', '49ers' rathern than 'San Francisco
Forty Niners'. We give weeks as integers like, `1` or `16`, or as one of
'wild-card', 'division', 'conference',  or 'super-bowl'. Year is the year in
which the season starts.
"""

import logging
import datetime
import sys
from multiprocessing import cpu_count
from concurrent import futures
from urllib.request import urlopen

import pandas as pd
from pandas.io.html import read_html
from bs4 import BeautifulSoup


__author__ = ('William Schwartz', 'Christopher Holt')
EARLIEST_DATA_SEASON = 2008
_GAME_URL_TEMPLATE = ("http://www.teamrankings.com/nfl/matchup/"
					 "{hometeam}-{awayteam}-{week}-{year:n}"
					 "/spread-movement")
_SEASON_URL_TEMPLATE = ("http://www.pro-football-reference.com/years/"
					   "{year:n}"
					   "/games.htm")
LOG = logging.getLogger(__name__)


class CantFindTheRightTable(Exception): pass


def game_url(hometeam, awayteam, week, year):
	"Calculate the URL for the spreads for the given game."
	if not isinstance(week, str):
		week = 'week-' + str(week)
	return _GAME_URL_TEMPLATE.format(
		hometeam=hometeam, awayteam=awayteam, week=week, year=year)


def game(hometeam, awayteam, week, year):
	"""Download, parse, and clean the spreads table for one game.

	The columns are pinnacle, betonline, bookmaker, datetime, hometeam,
	awayteam, week. The first three are the bookies and give the spreads from
	the point of view of the favored team (so they're generally nonpositive).
	"""
	with urlopen(game_url(hometeam, awayteam, week, year)) as connection:
		page = connection.read()
	# Note that infer_types is deprecated and won't work starting in Pandas 0.14
	LOG.debug('Getting game %s',
			  {'hometeam': hometeam, 'awayteam': awayteam, 'week': week,
			   'year': year})
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
		data[column] = (data[column]
						.replace('--', 'nan')
						.replace('(Pick)', 0)
						.apply(float))

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
	abbrev = (soup
			  .find('div', attrs={'class': 'module point-spreads'})
			  .find('a')
			  .contents[0]
			  .split()[0])
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
	"""Download, parse, and clean a table of games and scores for given season.

	The columns are week; hometeam; awayteam; winner; date; points, yards, and
	turn overs for the winning team; and points, yards, and turn overs for the
	losing team.
	"""
	LOG.debug('Getting season %d', year)
	data = read_html(io=season_games_url(year),
					  attrs={'id': 'games'},
					  infer_types=False,
					  header=0)
	if len(data) != 1:
		raise CantFindTheRightTable
	data = data.pop()

	# Cleaning.
	del data["Unnamed: 3"]
	# These rows are mid-table header rows.
	data = data[data.Week != "Week"][data.Week != "nan"]

	data['week'] = (data.Week
					.replace("WildCard", "wild-card")
					.replace("Division", "divisional")
					.replace("ConfChamp", "conference")
					.replace("SuperBowl", "super-bowl")
					.apply(
						lambda s: (int(s)
								   if all(c in '1234567890' for c in s)
								   else s)))
	del data['Week']

	data['game_date'] = pd.to_datetime(
		data.Date.replace("$", ", %d" % year, regex=True))
	del data['Date']

	for column in "PtsW", "PtsL", "YdsW", "TOW", "YdsL", "TOL":
	    data[column] = data[column].apply(int)

	data['WatL'] = data['Unnamed: 5'].apply(lambda x: x == '@')
	del data['Unnamed: 5']
	data['hometeam'] = (data.WatL * data['Winner/tie'] +
						~data.WatL * data['Loser/tie'])
	data['awayteam'] = (~data.WatL * data['Winner/tie'] +
						data.WatL * data['Loser/tie'])
	data['winner'] = data['Winner/tie']
	for column in 'Winner/tie', 'Loser/tie', "WatL":
		del data[column]
	for column in 'hometeam', 'awayteam', 'winner':
		data[column] = data[column].apply(lambda s: s.split()[-1].lower())

	return data


def _download_game(args):
	"Thread worker. Only to be used in `season` function."
	retried = False
	while True:
		try:
			g = game(*args)
		except (CantFindTheRightTable, ValueError):
			if retried:
				raise
			else:
				# Maybe the home/away info is bad, so swap teams.
				args = list(args)
				args[0], args[1] = args[1], args[0]
				retried = True
		else:
			if retried:
				awayteam, hometeam = g.hometeam.copy(), g.awayteam.copy()
				g.hometeam, g.awayteam = hometeam, awayteam
				g['home_away_discrepency'] = True
	return g


def season(year, timeout=120, concurrency=2 * cpu_count()):
	"""Download, parse, and clean the scores & spreads for all games in a season

	`timeout` is in seconds and `concurrency` is the number of threads to use,
	defaulting to twice the number of CPUs (as this function is IO-bound).

	The returned table is the JOIN of the tables that `season_game` and `game`
	return.
	"""
	LOG.debug('Concurrency = %d', concurrency)
	games = season_games(year)
	weeks, tables, futures_to_args = [], [], {}
	# See https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example.
	with futures.ThreadPoolExecutor(concurrency) as pool:
		for arg in zip(games.hometeam, games.awayteam, games.week):
			arg = arg + (year,)
			futures_to_args[pool.submit(_download_game, arg)] = arg
		for future in futures.as_completed(futures_to_args, timeout=timeout):
			args = futures_to_args[future]
			try:
				table = future.result()
			except Exception as exc:
				LOG.exception('Error from %s: %s', args, exc)
			else:
				if table is None:
					LOG.error('Failure: %s', args)
				else:
					LOG.info('Success: %s', args)
					tables.append(table)
	return games.merge(pd.concat(tables), on=('hometeam', 'awayteam', 'week'))


def seasons(years, timeout=None, concurrency=2 * cpu_count()):
	"""Download, parse, and clean multiple seasons of NFL games and spreads.

	`years` is an iterable of integers. `timeout` is measured in seconds and
	defaults to 120 s times the number of years to obtain. `concurrency is the
	number of threads to use, defaulting to twice the number of CPUs.

	The returned table has all the columns from `game` and `season_games`.
	"""
	tables = None
	years = list(years)
	if timeout is None:
		timeout = 120 * len(years)
	for year in years:
		LOG.info('=' * 10 + ' %d ' + '=' * 10, year)
		table = season(year, timeout=timeout, concurrency=concurrency)
		if tables is None:
			tables = table
		else:
			tables.append(table)
	return tables


def latest_season_before(date):
	"""Return the latest football season that started before the given `date`.

	`date` should be a `datetime.date` object. This function merely assumes that
	football season starts at the beginning of September.
	"""
	if date.month < 9:
		return date.year - 1
	return date.year


def main(args):
	"Print the `seasons` table for all years from 2008 to the present."
	logging.basicConfig(
		level=logging.DEBUG,
		format="[%(levelname)-8s %(asctime)s] %(message)s")
	from_ = EARLIEST_DATA_SEASON
	to = latest_season_before(datetime.date.today())
	table = seasons(range(from_, to + 1))
	table.to_csv(sys.stdout, index=False)
	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv[:1]))
