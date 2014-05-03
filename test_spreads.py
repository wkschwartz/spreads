import math
import unittest

import numpy as np
import pandas as pd

import spreads


TEAMS = frozenset([
	"49ers", "bears", "bengals", "bills", "broncos", "browns", "buccaneers",
	"cardinals", "chargers", "chiefs", "colts", "cowboys", "dolphins", "eagles",
	"falcons", "giants", "jaguars", "jets", "lions", "packers", "panthers",
	"patriots", "raiders", "rams", "ravens", "redskins", "saints", "seahawks",
	"steelers", "texans", "titans", "vikings"])
WEEKS = frozenset(list(range(1, 17 + 1)) + ['wild-card', 'divisional',
											'conference', 'super-bowl'])


class TestOneGame(unittest.TestCase):

	def test_game_url(self):
		self.assertEqual(
			spreads.game_url('ravens', 'broncos', 1, 2013),
			"http://www.teamrankings.com/nfl/matchup/ravens-broncos-week-1-2013/spread-movement")
		self.assertEqual(
			spreads.game_url('seahawks', 'broncos', 'super-bowl', 2013),
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/spread-movement")

	def assert_columns(self, data, year):
		# Date: check type, check year
		self.assertEqual(data.datetime.dtype, np.dtype('<M8[ns]'))
		for date in data.datetime:
			self.assertEqual(date.year, year)
		# pinnacle, betonline, bookmaker: it's a float
		for column in 'pinnacle', 'betonline', 'bookmaker':
			self.assertIs(data[column].dtype, np.dtype('float64'))
		# Has four other columns
		for col in 'hometeam', 'awayteam', 'week', 'favored':
			self.assertIn(col, data.keys())
		self.assertTrue((data.favored == data.hometeam).all() or
						(data.favored == data.awayteam).all())

	def test_game(self):
		hometeam, awayteam, week, year = 'ravens', 'broncos', 1, 2013
		data = spreads.game(hometeam, awayteam, week, year)
		self.assert_columns(data, year)
		# hometeam=ravens, awayteam=broncos, week=1, favored=broncos
		for x in data.hometeam:
			self.assertEqual(x, hometeam)
		for col in data.awayteam, data.favored:
			for x in data.awayteam:
				self.assertEqual(x, awayteam)
		for x in data.week:
			self.assertEqual(x, week)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2013-09-05 21:05:00')
		self.assertTrue(math.isnan(row.pinnacle))
		self.assertTrue(math.isnan(row.betonline))
		self.assertEqual(float(row.bookmaker), -7)

	def test_season_games_url(self):
		self.assertEqual(
			spreads.season_games_url(2012),
			"http://www.pro-football-reference.com/years/2012/games.htm")
		self.assertRaises(ValueError, spreads.season_games_url, '2012')

	def test_season_games(self):
		year, nonplayoff = 2013, range(1, 17 + 1)
		games = spreads.season_games(year)
		self.assertEqual(frozenset(games.week), WEEKS)
		self.assertEqual(games.game_date.dtype, np.dtype('<M8[ns]'))
		for date, week in zip(games.game_date, games.week):
			if isinstance(week, int):
				self.assertEqual(date.year, year)
			else: # Playoffs
				self.assertEqual(date.year, year + 1)
		for column in 'PtsW', 'PtsL', 'YdsW', 'YdsL', 'TOW', 'TOL':
			self.assertIs(games[column].dtype, np.dtype('int64'))
		for col in games.awayteam, games.hometeam, games.winner:
			for team in col:
				self.assertIn(team, TEAMS)
		self.assertTrue(((games.winner == games.hometeam) |
						 (games.winner == games.awayteam)).all())
