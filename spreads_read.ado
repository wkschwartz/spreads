*! stata
* version 0.1.0
* William Schwartz and Christopher Holt

/**
 * Read in data produced by spreads.py.
 *
 * Syntax is
 *    spreads_read using[, clear]
 * See `help insheet` for a description of `using` and `clear`.
 */
program define spreads_read, nclass
	version 10
	syntax using[, clear]
	preserve
	quietly insheet `using', `clear' comma names

	/************************* Individual variables ***************************/
	quietly {
		tempvar t

		drop day // If needed: generate byte day = dow(game_date)

		encode week, generate(`t') label(week)
		drop week
		rename `t' week
		label variable week "Week of season on which game occured"

		format season %4.0f

		generate double `t' = date(game_date, "20YMD#")
		drop game_date
		rename `t' game_date
		label variable game_date "Date of game"
		format game_date %td

		encode hometeam, generate(`t') label(teams)
		drop hometeam
		rename `t' hometeam

		encode awayteam, generate(`t') label(teams)
		drop awayteam
		rename `t' awayteam

		generate double `t' = clock(datetime, "20YMD hms")
		drop datetime
		rename `t' datetime
		label variable datetime "Time of book maker observation"
		format datetime %tc

		format *_spread *_over_under %4.1f

		generate byte `t' = (home_away_discrepency == "True")
		drop home_away_discrepency
		rename `t' home_away_discrepency
		local l "Disagreement between sources on which team was home/away"
		label variable home_away_discrepency "`l'"
		format home_away_discrepency %1.0f

		format turn_overs* %2.0f
		format yards* %4.0f
		format points* %2.0f
	}

	/******************************* Whole data set ***************************/
	quietly {
		compress

		notes _dta: Sources: pro-football-reference.com for games, points, yards, and turnovers. teamrankings.com for spreads and over-unders.
		notes _dta: Pinnacle, BetOnline, and Bookmaker are the book makers that track spreads and over-unders.

		order season week hometeam awayteam game_date *spread *under datetime

		label data "Historical NFL games and the bets placed on them."
	}
	restore, not
	display as text "(`c(k)' vars, `c(N)' obs)"
end
