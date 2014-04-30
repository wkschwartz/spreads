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
		for col in 'hometeam', 'awayteam', 'week':
			self.assertIn(col, data.keys())

	def test_one_game_table(self):
		hometeam, awayteam, week, year = 'ravens', 'broncos', 1, 2013
		data = spreads.one_game_table(hometeam, awayteam, week, year)
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
		self.assertEqual(m.group('hometeam'), 'ravens')
		self.assertEqual(m.group('awayteam'), 'broncos')
		self.assertEqual(m.group('week'), '1')
		self.assertEqual(m.group('year'), '2013')

		m = spreads.GAME_URL_RE.match(
			"http://www.teamrankings.com/nfl/matchup/falcons-49ers-week-16-2013/spread-movement")
		self.assertEqual(m.group('hometeam'), 'falcons')
		self.assertEqual(m.group('awayteam'), '49ers')
		self.assertEqual(m.group('week'), '16')
		self.assertEqual(m.group('year'), '2013')

		m = spreads.GAME_URL_RE.match(
			"http://www.teamrankings.com/nfl/matchup/seahawks-broncos-super-bowl-2013/spread-movement")
		self.assertEqual(m.group('hometeam'), 'seahawks')
		self.assertEqual(m.group('awayteam'), 'broncos')
		self.assertEqual(m.group('week'), 'super-bowl')
		self.assertEqual(m.group('year'), '2013')

	def test_all_possible_games(self):
		generator = spreads.all_possible_games(2013, [1, 'a'], ['b', 'c'])
		self.assertCountEqual(list(generator),
							  [('b', 'c', 1, 2013), ('c', 'b', 1, 2013),
							   ('b', 'c', 'a', 2013), ('c', 'b', 'a', 2013)])
