import math
import unittest
import datetime

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

	def test_spread_url(self):
		self.assertEqual(
			spreads.spread_url('ravens', 'broncos', 1, 2013),
			"http://www.teamrankings.com/nfl/matchup/ravens-broncos-week-1-2013/spread-movement")
		self.assertEqual(
			spreads.spread_url('seahawks', 'broncos', 'super-bowl', 2013),
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/spread-movement")

	def test_over_under_url(self):
		self.assertEqual(
			spreads.over_under_url('ravens', 'broncos', 1, 2013),
			"http://www.teamrankings.com/nfl/matchup/ravens-broncos-week-1-2013/over-under-movement")
		self.assertEqual(
			spreads.over_under_url('seahawks', 'broncos', 'super-bowl', 2013),
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/over-under-movement")

	def assert_columns(self, data, hometeam, awayteam, week, year):
		# Date: check type, check year
		self.assertEqual(data.datetime.dtype, np.dtype('<M8[ns]'))
		for date in data.datetime:
			self.assertEqual(date.year, year)
		# Check arguments are in the table correctly
		for x in data.awayteam:
			self.assertEqual(x, awayteam)
		for x in data.hometeam:
			self.assertEqual(x, hometeam)
		for x in data.week:
			self.assertEqual(x, week)
		# pinnacle, betonline, bookmaker: it's a float
		for prefix in 'pinnacle', 'betonline', 'bookmaker':
			for suffix in '_spread', '_over_under':
				self.assertIs(data[prefix + suffix].dtype, np.dtype('float64'))
		# Has four other columns
		for col in 'hometeam', 'awayteam', 'week', 'favored':
			self.assertIn(col, data.keys())
		self.assertTrue((data.favored == data.hometeam).all() or
						(data.favored == data.awayteam).all())

	def test_game(self):
		hometeam, awayteam, week, year = 'ravens', 'broncos', 1, 2013
		data = spreads.game(hometeam, awayteam, week, year)
		self.assert_columns(data, hometeam, awayteam, week, year)
		for x in data.favored:
			self.assertEqual(x, awayteam)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2013-09-05 21:05:00')
		self.assertTrue(math.isnan(row.pinnacle_spread))
		self.assertTrue(math.isnan(row.betonline_spread))
		self.assertEqual(row.bookmaker_spread, -7)

	def test_playoff_game(self):
		hometeam, awayteam, week, year = 'seahawks', 'broncos', 'super-bowl', 2013
		data = spreads.game(hometeam, awayteam, week, year)
		self.assert_columns(data, hometeam, awayteam, week, year + 1)
		for x in data.favored:
			self.assertEqual(x, awayteam)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2014-02-02 18:35:00')
		for x in (row.pinnacle_spread, row.betonline_spread,
				  row.pinnacle_over_under, row.betonline_over_under):
			self.assertTrue(math.isnan(x))
		self.assertEqual(row.bookmaker_spread, -2)
		self.assertEqual(row.bookmaker_over_under, 47.0)

	def test_favored_team_with_dot_in_city_name(self):
		"St. Louis was messing up the favored-team detector."
		hometeam, awayteam, week, year = 'cardinals', 'rams', 1, 2013
		data = spreads.game(hometeam, awayteam, week, year)
		self.assert_columns(data, hometeam, awayteam, week, year)
		for x in data.favored:
			self.assertEqual(x, awayteam)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2013-09-08 16:35:00')
		for x in (row.pinnacle_spread, row.betonline_spread,
				  row.pinnacle_over_under, row.betonline_over_under):
			self.assertTrue(math.isnan(x))
		self.assertEqual(row.bookmaker_spread, -3.5)
		self.assertEqual(row.bookmaker_over_under, 43.5)

	def test_game_unknown_homeaway(self):
		# In reality, ravens were home
		hometeam, awayteam, week, year = 'broncos', 'ravens', 1, 2013
		data = spreads.game_unknown_homeaway(hometeam, awayteam, week, year)
		self.assert_columns(data, hometeam, awayteam, week, year)
		for x in data.favored:
			self.assertEqual(x, hometeam)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2013-09-05 21:05:00')
		for x in (row.pinnacle_spread, row.betonline_spread,
				  row.pinnacle_over_under, row.betonline_over_under):
			self.assertTrue(math.isnan(x))
		self.assertEqual(row.bookmaker_spread, -7)
		self.assertEqual(row.bookmaker_over_under, 48.0)

		for x in data.home_away_discrepency:
			self.assertEqual(x, True)


class TestSeason(unittest.TestCase):

	# When the tests in this class run, they have access to `self.table`, which
	# is the `season` table for 2013 week 1.

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		cls.year, cls.week = 2013, 1
		cls.table, cls.failures = spreads.season(cls.year, week=cls.week)

	def setUp(self):
		super().setUp()
		self.table = self.table.copy()
		self.failures = list(self.failures)

	def test_season_games_url(self):
		self.assertEqual(
			spreads.season_games_url(2012),
			"http://www.pro-football-reference.com/years/2012/games.htm")
		self.assertRaises(ValueError, spreads.season_games_url, '2012')

	def test_season(self):
		self.assertFalse(self.failures)
		for x in self.table.week:
			self.assertEqual(x, self.week)

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
		for column in 'PtsW', 'PtsL', 'YdsW', 'YdsL', 'TOW', 'TOL', 'season':
			self.assertIs(games[column].dtype, np.dtype('int64'))
		for x in games.season:
			self.assertEqual(x, year)
		for col in games.awayteam, games.hometeam, games.winner:
			for team in col:
				self.assertIn(team, TEAMS)
		self.assertTrue(((games.winner == games.hometeam) |
						 (games.winner == games.awayteam)).all())
		# Check that home/away is calculated correctly. Just do first week.
		g = games[games.week == 1]
		self.assertEqual(len(g[g.hometeam == 'broncos']), 1)
		self.assertEqual(len(g[g.awayteam == 'ravens']), 1)
		self.assertEqual(len(g[g.hometeam == 'chargers']), 1)
		self.assertEqual(len(g[g.awayteam == 'texans']), 1)

	def test_latest_season_before(self):
		d = datetime.date(2014, 2, 7)
		self.assertEqual(spreads.latest_season_before(d), 2013)
		d = datetime.date(2014, 10, 7)
		self.assertEqual(spreads.latest_season_before(d), 2014)

	def test_hometeamify(self):
		spread_cols = (prefix + suffix for prefix in
					   ('pinnacle', 'betonline', 'bookmaker') for suffix in
					   ('_spread', '_over_under'))
		table = spreads.hometeamify(self.table)

		# Deleted columns
		cols = table.keys()
		deleted = ('winner', 'favored', 'PtsW', 'PtsL', 'YdsW', 'YdsL', 'TOW',
				   'TOL')
		for d in deleted:
			self.assertNotIn(d, cols)

		# New column types
		for prefix in 'points', 'yards', 'turn_overs':
			for suffix in '_home', '_away':
				self.assertEqual(table[prefix+suffix].dtype, np.dtype('int64'))
		for col in spread_cols:
			self.assertEqual(table[col].dtype, np.dtype('float64'))

		# Four Cases
		# favored = home, winner = home
		hometeam, awayteam = 'broncos', 'ravens'
		t = table[table.hometeam == hometeam]
		self.assertGreater(len(t), 0)
		self.assertTrue((t.points_home == 49).all())
		self.assertTrue((t.points_away == 27).all())
		self.assertTrue((t.yards_home == 510).all())
		self.assertTrue((t.yards_away == 393).all())
		self.assertTrue((t.turn_overs_home == 2).all())
		self.assertTrue((t.turn_overs_away == 2).all())
		for col in spread_cols:
			for v in t[col]:
				if not math.isnan(v):
					self.assertLess(v, 0)

		# favored = away, winner = home
		hometeam, awayteam = 'jets', 'buccaneers'
		t = table[table.hometeam == hometeam]
		self.assertGreater(len(t), 0)
		self.assertTrue((t.points_home == 18).all())
		self.assertTrue((t.points_away == 17).all())
		self.assertTrue((t.yards_home == 304).all())
		self.assertTrue((t.yards_away == 250).all())
		self.assertTrue((t.turn_overs_home == 2).all())
		self.assertTrue((t.turn_overs_away == 2).all())
		for col in spread_cols:
			for v in t[col]:
				if not math.isnan(v):
					self.assertGreater(v, 0)

		# favored = home, winner = away
		hometeam, awayteam = 'steelers', 'titans'
		t = table[table.hometeam == hometeam]
		self.assertGreater(len(t), 0)
		self.assertTrue((t.points_home == 9).all())
		self.assertTrue((t.points_away == 16).all())
		self.assertTrue((t.yards_home == 194).all())
		self.assertTrue((t.yards_away == 229).all())
		self.assertTrue((t.turn_overs_home == 2).all())
		self.assertTrue((t.turn_overs_away == 0).all())
		for col in spread_cols:
			for v in t[col]:
				if not math.isnan(v):
					self.assertLess(v, 0)

		# favored = away, winner = away
		hometeam, awayteam = 'bills', 'patriots'
		t = table[table.hometeam == hometeam]
		self.assertGreater(len(t), 0)
		self.assertTrue((t.points_home == 21).all())
		self.assertTrue((t.points_away == 23).all())
		self.assertTrue((t.yards_home == 286).all())
		self.assertTrue((t.yards_away == 431).all())
		self.assertTrue((t.turn_overs_home == 2).all())
		self.assertTrue((t.turn_overs_away == 3).all())
		for col in spread_cols:
			for v in t[col]:
				if not math.isnan(v):
					self.assertGreater(v, 0)
