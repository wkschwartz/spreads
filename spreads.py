import multiprocessing
from concurrent import futures
import itertools
import re

import pandas as pd
from pandas.io.html import read_html


GAME_URL_RE = re.compile(
	r"""http://www.teamrankings.com/nfl/matchup/
        (?P<team_a>\w+)-
        (?P<team_b>\w+)-
        (?:week-)?            # Regular integer weeks start with this
        (?P<week>
            (?:\d+)           # The part of a regular week we want
            |                 # OR
            (?:[a-zA-Z0-9-]+) # "super-bowl" or "divisional"
        )-
        (?P<year>\d{4})
        /spread-movement""",
	flags=re.VERBOSE)


TEAMS = ("49ers", "bears", "bengals", "bills", "broncos", "browns",
		 "buccaneers", "cardinals", "chargers", "chiefs", "colts", "cowboys",
		 "dolphins", "eagles", "falcons", "giants", "jaguars", "jets", "lions",
		 "packers", "panthers", "patriots", "raiders", "rams", "ravens",
		 "redskins", "saints", "seahawks", "steelers", "texans", "titans",
		 "vikings")

WEEKS = tuple(i for i in range(1, 17)) + ('wild-card', 'divisional',
										  'conference', 'super-bowl')

def one_game_url(team_a, team_b, week, year):
	"Calculate the URL for the spreads from team_a to team_b."
	base = "http://www.teamrankings.com/nfl/matchup/"
	template = "{team_a}-{team_b}-{week}-{year}"
	tail = "/spread-movement"
	if not isinstance(week, str):
		week = 'week-' + str(week)
	result = template.format(team_a=team_a, team_b=team_b, week=week, year=year)
	return ''.join([base, result, tail])


def one_game_table(team_a, team_b, week, year):
	"Download, parse, and clean the spreads table for one game."
	# Note that infer_types is deprecated and won't work starting in Pandas 0.14
	data = read_html(io=one_game_url(team_a, team_b, week, year),
					 match="History", attrs={'id': 'table-000'},
					 infer_types=False, header=0,
					 skiprows=[1, 2, 3])
	if len(data) != 1:
		raise ValueError("Couldn't find the correct table.")
	data = data.pop()

	# Cleaning.
	datetime = pd.to_datetime(data['Unnamed: 0'].replace(
		r'(\d\d?/\d\d?)', r'\1/' + str(year), regex=True))
	del data['Unnamed: 0']

	# Replace all the '--' as missing so we can convert numbers to floats.
	for column in data.keys():
		data[column] = data[column].replace('--', 'nan').apply(float)

	# Add datetime back in after the str-to-float conversion so we don't do it
	# for the datetime.
	data['datetime'] = datetime

	# Add this function's arguments to the table.
	data['team_a'] = team_a
	data['team_b'] = team_b
	data['week'] = week

	# Lowercase column names for ease of programming later
	data.columns = [h.lower() for h in data.columns]

	return data


def concatenate_tables(tables):
	"Append an iterable of tables together."
	last = None
	for table in tables:
		if last is None:
			last = table
		else:
			last = last.append(table)
	return last


def all_possible_games(year, weeks=WEEKS, teams=TEAMS):
	"Weeks is an iterable of `week` parameters to pass to one_game_url."
	for team_a, team_b in itertools.permutations(teams, 2):
		for week in weeks:
			yield team_a, team_b, week, year


def season_table(year, timeout=60, concurrency=None):
	def worker(args):
		print('Attempting: %s' % (args,))
		try:
			return one_game_table(*args)
		except ValueError:
			return None
	futures_to_args  = {}
	tables = []
	if concurrency is None:
		concurrency = multiprocessing.cpu_count() * 2
	print('Concurrency: %d' % concurrency)
	with futures.ThreadPoolExecutor(concurrency) as pool:
		for args in all_possible_games(year):
			futures_to_args[pool.submit(worker, args)] = args
		for future in futures.as_completed(futures_to_args, timeout=timeout):
			args = futures_to_args[future]
			try:
				table = future.result()
			except Exception as exc:
				print("%r generated an exception: %s" % (args, exc))
			else:
				if table is not None:
					print('Success: %s' % (args,))
					tables.append(table)
	return concatenate_tables(tables)
