import math
import unittest

import numpy as np
import pandas as pd

import spreads


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
		# Has three other columns
		for col in 'hometeam', 'awayteam', 'week':
			self.assertIn(col, data.keys())

	def test_game(self):
		hometeam, awayteam, week, year = 'ravens', 'broncos', 1, 2013
		data = spreads.game(hometeam, awayteam, week, year)
		self.assert_columns(data, year)
		# hometeam=ravens, awayteam=broncos, week=1
		for x in data.hometeam:
			self.assertEqual(x, hometeam)
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
