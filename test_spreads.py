import math
import unittest

import numpy as np
import pandas as pd

import spreads


class TestOneGame(unittest.TestCase):

	def test_one_game_url(self):
		self.assertEqual(
			spreads.one_game_url('ravens', 'broncos', 1, 2013),
			"http://www.teamrankings.com/nfl/matchup/ravens-broncos-week-1-2013/spread-movement")
		self.assertEqual(
			spreads.one_game_url('seahawks', 'broncos', 'super-bowl', 2013),
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/spread-movement")

	def assert_columns(self, data, year):
		# Date: check type, check year
		self.assertEqual(data.datetime.dtype, np.dtype('<M8[ns]'))
		for date in data.datetime:
			self.assertEqual(date.year, year)
		# pinnacle, betonline, bookmaker: it's a float
		for column in 'pinnacle', 'betonline', 'bookmaker':
			self.assertIs(data[column].dtype, np.dtype('float64'))
		# Has three other columns
		for col in 'team_a', 'team_b', 'week':
			self.assertIn(col, data.keys())

	def test_one_game_table(self):
		team_a, team_b, week, year = 'ravens', 'broncos', 1, 2013
		data = spreads.one_game_table(team_a, team_b, week, year)
		self.assert_columns(data, year)
		# team_a=ravens, team_b=broncos, week=1
		for x in data.team_a:
			self.assertEqual(x, team_a)
		for x in data.team_b:
			self.assertEqual(x, team_b)
		for x in data.week:
			self.assertEqual(x, week)
		# Test the contents of the first row
		row = data.loc[0]
		self.assertEqual(str(row.datetime), '2013-09-05 21:05:00')
		self.assertTrue(math.isnan(row.pinnacle))
		self.assertTrue(math.isnan(row.betonline))
		self.assertEqual(float(row.bookmaker), -7)

	def test_concatenate_tables(self):
		# Get two tables
		year = 2013
		one = spreads.one_game_table('ravens', 'broncos', 1, year)
		two = spreads.one_game_table('ravens', 'bengals', 17, year)
		data = spreads.concatenate_tables([one, two])
		self.assert_columns(data, year)
		self.assertEqual(len(one) + len(two), len(data))

	def test_game_url(self):
		m = spreads.GAME_URL_RE.match(
			"http://www.teamrankings.com/nfl/matchup/ravens-broncos-week-1-2013/spread-movement")
		self.assertEqual(m.group('team_a'), 'ravens')
		self.assertEqual(m.group('team_b'), 'broncos')
		self.assertEqual(m.group('week'), '1')
		self.assertEqual(m.group('year'), '2013')

		m = spreads.GAME_URL_RE.match(
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/spread-movement")
		self.assertEqual(m.group('team_a'), 'seahawks')
		self.assertEqual(m.group('team_b'), 'broncos')
		self.assertEqual(m.group('week'), 'super-bowl')
		self.assertEqual(m.group('year'), '2013')