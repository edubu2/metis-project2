"""
    This script contains nested functions that will apply all data cleaning necessary to model our data.
"""
import pandas as pd
import numpy as np


def clean_games(scraped_games_data):
    """
        A series of nested functions that readies our data for modeling.

        Parameters:
            - scraped_games_data (required): the relative filepath to the 
              .pickle file containing the scraped game_df from web scraping.
    """
    game_df = pd.read_pickle(scraped_games_data)

    def home_game(game_df):
        """
            Changes the 'home' column from '@' to boolean 
                - i.e. 1 = True/ home game, 0 = False/ away game)
        """

        # create function that will be used with the df.apply() method
        def apply_home_game(row):
            """Simple function used for the home_game func to apply to each row. """
            if row == "@":
                return 0
            return 1

        game_df["home"] = game_df.game_location.apply(apply_home_game)
        game_df.drop("game_location", axis=1, errors="ignore", inplace=True)

        return game_df

    def fix_date(game_df):
        """ Converts game_date to datetime format (and renames col to 'date'). """

        f = r"%B %d-%Y"
        full_game_date = game_df.game_date + "-" + game_df.year.astype(str)
        full_game_date = pd.to_datetime(full_game_date, format=f)
        game_df.insert(loc=2, column="date", value=full_game_date)
        game_df.drop("game_date", axis=1, inplace=True, errors="ignore")

        # add decade column
        decades = game_df["year"] // 10
        game_df.insert(loc=5, column="decade", value=decades)

        return game_df

    def add_ids(game_df):
        """ Inserts 'team_year' and 'game_id' columns to game_df. """

        # insert team year column
        team_years = game_df["team"] + "-" + game_df["year"].astype(str)
        game_df.insert(loc=2, column="team_year", value=team_years)

        # create func to use with .apply() to get a unique game identifier (the same for both rows of the same game)
        def apply_game_id(row):
            teams = []
            teams.append(str(row["team"]))
            teams.append(str(row["opp"]))
            teams.sort()

            game_id = teams[0] + "-" + teams[1] + "-" + str(row["date"])[:-9]
            return game_id

        # insert game_id column using above function
        game_df["game_id"] = game_df.apply(apply_game_id, axis=1)

        return game_df

    def convert_game_outcomes(game_df):
        """ Converts the game_outcome columns to dummy W/T columns"""

        game_df[["result_tie", "result_win"]] = pd.get_dummies(
            game_df.game_outcome, drop_first=True
        )

        return game_df

    def convert_team_records(game_df):
        """ This function splits team_record (format W-L-T) into three new columns. """

        game_df = game_df.assign(
            wins=game_df.team_record.str.split("-").str.get(0),
            losses=game_df.team_record.str.split("-").str.get(1),
            ties=game_df.team_record.str.split("-").str.get(2),
        )
        game_df["ties"] = game_df.ties.fillna(0)

        return game_df

    def add_margin_col(game_df):
        margins = game_df.pts_off - game_df.pts_def
        game_df.insert(loc=11, column="margin", value=margins)

        return game_df

    def add_prev_week_cols(game_df):

        cols_to_shift = [
            "wins",
            "losses",
            "ties",
            "pts_off",
            "pts_def",
            "margin",
            "first_down_off",
            "yards_off",
            "pass_yds_off",
            "rush_yds_off",
            "to_off",
            "first_down_def",
            "yards_def",
            "pass_yds_def",
            "rush_yds_def",
            "to_def",
            "result_tie",
            "result_win",
        ]

        # convert NaNs to zero (0s came thru as NaN values)
        game_df[cols_to_shift] = game_df[cols_to_shift].fillna(0)

        # drop rows for bye weeks & drop exp_pts cols (must do this for shift to work)
        game_df.dropna(axis=0, how="any", subset=["game_outcome"], inplace=True)

        game_df.drop(["exp_pts_off", "exp_pts_def", "exp_pts_st"], axis=1)

        for col in cols_to_shift:
            new_col = "prev_" + col
            game_df[new_col] = game_df.groupby("team_year")[col].apply(
                lambda grp: grp.shift(1)
            )

        return game_df

    def add_roll_cols(game_df):
        """
            Adds two sets of columns:
                1. roll3_<stat_column>: rolling average for the past 3 weeks.
                2. ewma_<stat_column>: exponentially weighted moving average of the last 3-16 weeks (greedy)
                    - gives highest weighting to the most recent week; lowest weighting to the oldest week
        """

        roll_cols = [
            "pts_off",
            "pts_def",
            "margin",
            "first_down_off",
            "yards_off",
            "pass_yds_off",
            "rush_yds_off",
            "to_off",
            "yards_def",
            "pass_yds_def",
            "rush_yds_def",
            "to_def",
        ]

        # add 3 week rolling average
        roll3_cols = ["roll3_" + col_name for col_name in roll_cols]

        game_df[roll3_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).rolling(3).mean(), 3)
        )

        # add 3roll_wins & ties (sum instead of mean) (no cols to prevent multicolinearity)
        rolling_wins = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).rolling(3).sum(), 3)
        )

        game_df.insert(loc=38, column="roll3_wins", value=rolling_wins)

        rolling_ties = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).rolling(3).sum(), 3)
        )

        game_df.insert(loc=38, column="roll3_ties", value=rolling_ties)

        # add 3-16 week EWMA cols
        ewma_cols = ["ewma_" + col_name for col_name in roll_cols]

        game_df[ewma_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).ewm(span=16, min_periods=3).mean(), 3)
        )

        return game_df

    def drop_first_three(game_df):
        """ Drops first 3 weeks of each year. """

        # drop rows without moving averages (first 3 weeks of every season)
        game_df.dropna(axis=0, how="any", subset=["roll3_pts_off"], inplace=True)

        return game_df

    def main(game_df):
        game_df = home_game(game_df)
        game_df = fix_date(game_df)
        game_df = add_ids(game_df)
        game_df = convert_game_outcomes(game_df)
        game_df = convert_team_records(game_df)
        game_df = add_margin_col(game_df)
        game_df = add_prev_week_cols(game_df)
        game_df = add_roll_cols(game_df)
        game_df = drop_first_three(game_df)
        return game_df

    game_df = main(game_df)
    return game_df
