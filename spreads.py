"""Download and clean historical betting spreads for NFL games.

In all the functions below, identify games by a home team, away team, week, and
year. We indicate teams by name rather than city: 'redskins' rathern than
'Washington', 'eagles' rather than 'PHL', '49ers' rathern than 'San Francisco
Forty Niners'. We give weeks as integers like, `1` or `16`, or as one of
'wild-card', 'divisional', 'conference',  or 'super-bowl'. Year is the year in
which the season starts.

All the functions that return tables return them as Pandas DataFrames.
"""

import logging
import datetime
import sys
import re
import warnings
import argparse
from multiprocessing import cpu_count
from concurrent import futures
from urllib.request import urlopen

import pandas as pd
from pandas.io.html import read_html
from bs4 import BeautifulSoup


__author__ = ('William Schwartz', 'Christopher Holt')
__version__ = '0.1.0' # Try to stick to semver.org standards.
EARLIEST_DATA_SEASON = 2008
_GAME_URL_TEMPLATE = ("http://www.teamrankings.com/nfl/matchup/"
					  "{hometeam}-{awayteam}-{week}-{year:n}")
_SPREAD_URL_TEMPLATE = _GAME_URL_TEMPLATE + "/spread-movement"
_OVER_UNDER_URL_TEMPLATE = _GAME_URL_TEMPLATE + "/over-under-movement"
_FAVORED_RE = re.compile(r'\|\s+Odds:\s+(?P<city>[a-zA-Z. ]+)\s+by\s+[0-9.]+,')
_SEASON_URL_TEMPLATE = ("http://www.pro-football-reference.com/years/"
					   "{year:n}"
					   "/games.htm")
_DEFAULT_CONCURRENCY = cpu_count()
LOG = logging.getLogger(__name__)


class CantFindTheRightTable(Exception): pass


def spread_url(hometeam, awayteam, week, year):
	"Calculate the URL for the spreads for the given game."
	if not isinstance(week, str):
		week = 'week-' + str(week)
	return _SPREAD_URL_TEMPLATE.format(
		hometeam=hometeam, awayteam=awayteam, week=week, year=year)


def over_under_url(hometeam, awayteam, week, year):
	"Calculate the URL for the spreads for the given game."
	if not isinstance(week, str):
		week = 'week-' + str(week)
	return _OVER_UNDER_URL_TEMPLATE.format(
		hometeam=hometeam, awayteam=awayteam, week=week, year=year)


def game(hometeam, awayteam, week, year):
	"""Download, parse, and clean the spreads & over-under tables for one game.

	The columns are pinnacle, betonline, bookmaker each with suffix _spread or
	_over_under; datetime; hometeam, awayteam, favored; week. The first three
	are the bookies and give the spreads from the point of view of the favored
	team (so they're generally nonpositive).
	"""
	with urlopen(spread_url(hometeam, awayteam, week, year)) as connection:
		spreads_page = connection.read()
	# Note that infer_types is deprecated and won't work starting in Pandas 0.14
	LOG.debug('Getting game %s', (hometeam, awayteam, week, year))
	sp = read_html(io=spreads_page.decode('utf-8'),
					 match="History", attrs={'id': 'table-000'},
					 infer_types=False, header=0,
					 skiprows=[1, 2, 3])
	if len(sp) != 1:
		raise CantFindTheRightTable
	sp = sp.pop()

	# Get the over-under page
	ou = read_html(io=over_under_url(hometeam, awayteam, week, year),
				   match="History", attrs={'cellspacing': 0},
				   infer_types=False, header=0,
				   skiprows=[1, 2, 3])
	if len(ou) != 1:
		raise CantFindTheRightTable
	ou = ou.pop()

	# Cleaning.
	for t, name, date_col in (sp, 'spread', 'Unnamed: 0'), (ou, 'over_under', '\xa0'):
		datetime = pd.to_datetime(
			t[date_col]
			.replace(r'(\d\d?/\d\d?)', r'\1/%d' % year, regex=True)
			.replace(r'(01|02)/(\d\d?)/\d{4}', r'\1/\2/%d' % (year + 1),
					 regex=True))
		del t[date_col]

		# Replace all the '--' as missing so we can convert numbers to floats.
		for column in t.keys():
			t[column] = (t[column]
						 .replace('--', 'nan')
						 .replace('(Pick)', 0)
						 .apply(float))

		# Add datetime back in after the str-to-float conversion so we don't do
		# it for the datetime.
		t['datetime'] = datetime

		# Lowercase column names for ease of programming later
		t.columns = [h.lower() for h in t.columns]

		# Give spreads/over-under their suffixes
		for col in 'pinnacle', 'betonline', 'bookmaker':
			t[col + '_' + name] = t[col]
			del t[col]

	data = sp.merge(ou, on=['datetime'], how='outer')
	assert set(data.datetime) == (set(sp.datetime) | set(ou.datetime))

	# Add this function's arguments to the table.
	data['hometeam'] = hometeam
	data['awayteam'] = awayteam
	data['week'] = week

	# Get favored team from the big "Odds: Washington by 4," that shows up at the
	# top of the page.
	soup = BeautifulSoup(spreads_page)
	subheader = soup.find('p', attrs={'class': 'h1-sub'}).find('strong')
	m = _FAVORED_RE.search(subheader.contents[0])
	if m is None or not m.group('city'):
		raise ValueError("Couldn't figure out who was favored: %r" %
						 (subheader.contents))
	city = m.group('city').replace(' ', '-').replace('.', '').lower()
	# city will be something like 'san-francisco' after the transformations
	# above. Find what team that is by looking for the links to the teams that
	# are also in that subheader.
	for link in subheader.findAll('a'):
		link = link['href']
		if city in link:
			data['favored'] = link.split('-')[-1]
			break
	else:
		raise ValueError("couldn't figure out who %s is" % city)

	return data


def season_games_url(year):
	"Calculate the URL for the games in season starting in `year`."
	return _SEASON_URL_TEMPLATE.format(year=year)


def season_games(year):
	"""Download, parse, and clean a table of games and scores for given season.

	The columns are week; hometeam; awayteam; winner; date; points, yards, and
	turn overs for the winning team; points, yards, and turn overs for the
	losing team; and season.
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
	# The code below issues "UserWarning: " So we catch UserWarnings.
	with warnings.catch_warnings():
		warnings.filterwarnings(action='ignore', category=UserWarning,
								module=r'pandas\.core\.frame',
								message=(r"Boolean Series key will be reindexed"
										 r" to match DataFrame index\."))
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

	data['season'] = year
	data['game_date'] = pd.to_datetime(
		data.Date
		.replace(r"$", r", %d" % year, regex=True)
		.replace(r"^(January|February) (\d+), \d+$", r"\1 \2, %d" % (year + 1),
				 regex=True))
	del data['Date']

	for column in "PtsW", "PtsL", "YdsW", "TOW", "YdsL", "TOL":
	    data[column] = data[column].apply(int)

	data['WatL'] = data['Unnamed: 5'].apply(lambda x: x == '@')
	del data['Unnamed: 5']
	data['hometeam'] = (~data.WatL * data['Winner/tie'] +
						data.WatL * data['Loser/tie'])
	data['awayteam'] = (data.WatL * data['Winner/tie'] +
						~data.WatL * data['Loser/tie'])
	data['winner'] = data['Winner/tie']
	for column in 'Winner/tie', 'Loser/tie', "WatL":
		del data[column]
	for column in 'hometeam', 'awayteam', 'winner':
		data[column] = data[column].apply(lambda s: s.split()[-1].lower())

	return data


def game_unknown_homeaway(team_a, team_b, week, year):
	"""Convenience wrapper for `game` when you're not sure who's the home team.

	We first try calling `game` with `team_a` as the home team, and if that
	doesn't work, we next try `team_b`. In the former case we add a column
	called `home_away_discrepency` equal to `False`. In the latter case the
	column contains `True` and the `awayteam` and `hometeam` columns are swapped
	to make it easier to merge with data from `season_games`.
	"""
	try:
		g = game(team_a, team_b, week, year)
	except (CantFindTheRightTable, ValueError):
		g = game(team_b, team_a, week, year)
		awayteam, hometeam = g.hometeam.copy(), g.awayteam.copy()
		g.hometeam, g.awayteam = hometeam, awayteam
		g['home_away_discrepency'] = True
	else:
		g['home_away_discrepency'] = False
	return g


def season(year, week=None, timeout=None, concurrency=_DEFAULT_CONCURRENCY):
	"""Download, parse, and clean the scores & spreads for all games in a season

	`timeout` is in seconds and `concurrency` is the number of threads to use,
	defaulting to the number of CPUs. If not `None`, `week` limits the games
	fetched to those in the given week.

	This function returns two values. The first is the table, which is the the
	merger of the tables that `season_games` and `game` return. The second is a
	list of `game` arguments that caused `game` to fail.
	"""
	LOG.debug('Concurrency = %d', concurrency)
	games = season_games(year)
	if week is not None:
		games = games[games.week == week]
	expected_n = len(games)
	tables, futures_to_args, failures = [], {}, []
	# See https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example.
	with futures.ThreadPoolExecutor(concurrency) as pool:
		for arg in zip(games.hometeam, games.awayteam, games.week):
			arg = arg + (year,)
			futures_to_args[pool.submit(game_unknown_homeaway, *arg)] = arg
		for future in futures.as_completed(futures_to_args, timeout=timeout):
			args = futures_to_args[future]
			try:
				table = future.result()
			except Exception as exc:
				LOG.exception('Error from %s: %s', args, exc)
			else:
				if table is None:
					LOG.error('Failure: %s', args)
					failures.append(args)
				else:
					LOG.info('Success: %s', args)
					tables.append(table)
	tables = games.merge(pd.concat(tables), on=('hometeam', 'awayteam', 'week'))
	if __debug__:
		n = len(tables.groupby(['hometeam', 'awayteam', 'week']))
		assert n == expected_n, "Expected %d games, got %d" % (expected_n, n)
	return tables, failures


def seasons(years, timeout=None, concurrency=_DEFAULT_CONCURRENCY):
	"""Download, parse, and clean multiple seasons of NFL games and spreads.

	`years` is an iterable of integers. `timeout` is measured in
	seconds. `concurrency is the number of threads to use, defaulting to the
	number of CPUs.

	This function returns two values. The first is the table, which is the the
	merger of the tables that `season_games` and `game` return. The second is a
	list of `game` arguments that caused `game` to fail.
	"""
	tables, failures = None
	years = list(years)
	for year in years:
		LOG.info('=' * 10 + ' %d ' + '=' * 10, year)
		table, failure = season(year, timeout=timeout, concurrency=concurrency)
		if tables is None:
			tables = table
			failures = failure
		else:
			tables.append(table)
			failures.extend(failure)
	return tables, failures


def hometeamify(t):
	"""Convert a `season`-generated table `t` so the data is home-team centric.

	`season` generates a table whose points, yards, and turn-overs columns are
	broken down by winning and losing team and whose spreads columns are
	relative to the favored team. Convert all these winner/loser based columns
	to be home/away based and convert the spreads columns to be home-team
	based.

	The resulting table replaces columns Pts, Yds, TO (all with W and L
	suffixes) with columns points, yards, turn_overs (all with home and away
	suffixes), but leaves the spreads columns intact (but modified). The winner
	and favored columns are removed. The returned table is a copy, leaving the
	function argument unodified.
	"""
	t = t.copy()
	# Winner/loser based columns
	hw, aw = t.hometeam == t.winner, t.awayteam == t.winner
	assert (hw == ~aw).all()
	# Suffix for keys = W for winner L for loser. Values are new names
	for old, new in {'Pts': 'points', 'Yds': 'yards', 'TO': 'turn_overs'}.items():
		t[new + '_home'] = (hw * t[old + 'W'] + aw * t[old + 'L'])
		t[new + '_away'] = (aw * t[old + 'W'] + hw * t[old + 'L'])
		del t[old + 'L'], t[old + 'W']
	del t['winner']
	# Favored-team based columns
	to_swap, to_keep = t.favored == t.awayteam, t.favored == t.hometeam
	assert (to_keep == ~to_swap).all()
	for col in 'pinnacle_spread', 'betonline_spread', 'bookmaker_spread':
		t[col] = -1 * to_swap * t[col] + to_keep * t[col]
	del t['favored']
	return t


def latest_season_before(date):
	"""Return the latest football season that started before the given `date`.

	`date` should be a `datetime.date` object. This function merely assumes that
	football season starts at the beginning of September.
	"""
	if date.month < 9:
		return date.year - 1
	return date.year


def _download_and_print(file, year=None, week=None, timeout=None,
					   concurrency=cpu_count()):
	latest = latest_season_before(datetime.date.today())
	if year is None:
		if week is not None:
			raise TypeError('Cannot give a week without a year')
		table, failures = seasons(range(EARLIEST_DATA_SEASON, latest + 1),
								  timeout=timeout, concurrency=concurrency)
	else:
		if year < EARLIEST_DATA_SEASON or latest < year:
			raise ValueError('year=%d not in [%d, %d]' %
							 (year, EARLIEST_DATA_SEASON, latest))
		if week is None:
			table, failures = season(year, timeout=timeout,
									 concurrency=concurrency)
		else:
			table, failures = season(year, week=week, timeout=timeout,
									 concurrency=concurrency)
	if failures:
		LOG.error('FAILURES:\n%s', '\n'.join(map(str, failures)))
	table = hometeamify(table)
	table.to_csv(sys.stdout, index=False)


def parse_args(args):
	a = argparse.ArgumentParser(
		description=("Historical NFL and betting data: download, parse, and "
					 "print as CSV to stdout."),
		epilog=("Must give a year argument to give a week argument. Without a "
				"year argument, download all data from %d to present. Weeks"
				"are integers or one of 'wild-card', 'divisional', "
				"'conference', or 'super-bowl'." % EARLIEST_DATA_SEASON))
	a.add_argument('--version', action='version', version=__version__)
	a.add_argument('-y', '--year', type=int, metavar='YEAR',
				   help='return data only from season beginning in YEAR')
	a.add_argument('-w', '--week',
				   help='--year required; return data only from this week')
	a.add_argument('--timeout', type=float, metavar='T',
				   help='in seconds')
	a.add_argument('--concurrency', type=int, metavar='N',
				   default=_DEFAULT_CONCURRENCY,
				   help='number of processors (default %(default)d)')
	a.add_argument('--verbosity', default='INFO',
				   choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
				   help='set amount of logging output (default: %(default)s)')
	args = a.parse_args(args)
	if args.week is not None:
		try:
			args.week = int(args.week)
		except ValueError:
			pass
	return args


def main(args):
	args = parse_args(args)
	logging.basicConfig(
		level=args.verbosity,
		format="[%(levelname)-8s %(asctime)s] %(message)s")
	logging.captureWarnings(capture=True)
	with open(sys.stdout, newlines='', closefd=False) as stdout:
		_download_and_print(file=stdout, year=args.year, week=args.week,
							timeout=args.timeout, concurrency=args.concurrency)
	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
