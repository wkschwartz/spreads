import pandas as pd
from pandas.io.html import read_html


def one_game_url(team_a, team_b, week, year):
	"Calculate the URL for the spreads from team_a to team_b."
	base = "http://www.teamrankings.com/nfl/matchup/"
	template = "{team_a}-{team_b}-week-{week}-{year}"
	tail = "/spread-movement"
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
	# Parse the date and set it as the index for the dataframe
	data['datetime'] = pd.to_datetime(data['Unnamed: 0'].replace(
		r'(\d\d?/\d\d?)', r'\1/' + str(year), regex=True))
	del data['Unnamed: 0']
	data.set_index(keys='datetime', inplace=True, verify_integrity=True)

	# Replace all the '--' as missing so we can convert numbers to floats.
	for column in data.keys():
		data[column] = data[column].replace('--', 'nan').apply(float)

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
