"""
    This script contains nested functions that will apply all data cleaning necessary to model our data.
"""
import pandas as pd
import numpy as np


def clean_games(scraped_games_data, start_year=1960):
    """
        A series of nested functions that readies our data for modeling.

        Parameters:
            - scraped_games_data (required): the relative filepath to the 
              .pickle file containing the scraped game_df from web scraping.
    """
    game_df = pd.read_pickle(scraped_games_data)
    assert start_year >= 1960, AssertionError(
        "Please choose a start_year value between 1961 and 2019."
    )
    if start_year > 1960:
        mask = game_df["year"] >= start_year
        game_df = game_df[mask]

    def add_cols(game_df):
        """ 
            Adds the following columns to game_df:
                - team_home_game
                - log_year
                - decade
                - team_year
                - game_id
                - win (boolean)
                - tie (boolean)
                - margin
                - team_home_game
        """
        # add team_home_game column
        def apply_home_game(row):
            """Used to determine whether or not the game is a home_game. """
            if row == "@":
                return 0
            return 1

        game_df["team_home_game"] = game_df.game_location.apply(apply_home_game)

        # add log_year and decade cols
        log_year = np.log(game_df["season_year"])
        game_df.insert(loc=5, column="log_year", value=log_year)

        decade = game_df["season_year"] // 10
        game_df.insert(loc=5, column="decade", value=decade)

        # insert team year column
        team_years = game_df["team"] + "-" + game_df["year"].astype(str)
        game_df.insert(loc=2, column="team_year", value=team_years)

        # create func to use with .apply() to get a unique game identifier (the same for both rows of the same game)
        def apply_game_id(row):
            teams = []
            teams.append(str(row["team"]))
            teams.append(str(row["opp"]))
            teams.sort()

            game_id = teams[0] + "-" + teams[1] + "-" + str(row["full_game_date"])[:-9]
            return game_id

        # insert game_id column using above function
        game_ids = game_df.apply(apply_game_id, axis=1)
        game_df.insert(loc=0, column="game_id", value=game_ids)

        # add game_outcome dummy cols

        game_df[["result_tie", "result_win"]] = pd.get_dummies(
            game_df.game_outcome, drop_first=True
        )

        margins = game_df.pts_off - game_df.pts_def
        game_df.insert(loc=11, column="margin", value=margins)

        return game_df

    def drop_useless_cols(game_df):
        """ 
            Removes the following columns:
                - 'game_day_of_week'
                - 'game_date': will instead be using 'full_game_date'
                - 'game_location': since we replaced with boolean ('team_home_game')
        """
        game_df.drop("game_date", axis=1, inplace=True, errors="ignore")
        game_df.drop("game_day_of_week", axis=1, inplace=True, errors="ignore")
        game_df.drop("game_location", axis=1, errors="ignore", inplace=True)

        return game_df

    def add_prev_week_cols(game_df):

        cols_to_shift = [
            "season_week",
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
            "to2_off",
            "first_down_def",
            "yards_def",
            "pass_yds_def",
            "rush_yds_def",
            "to_def",
            "to2_def",
            "result_tie",
            "result_win",
        ]

        # convert NaNs to zero (0s came thru as NaN values)
        game_df[cols_to_shift] = game_df[cols_to_shift].fillna(0)

        # drop rows for bye weeks & drop exp_pts cols (must do this for shift to work)
        game_df.dropna(axis=0, how="any", subset=["game_outcome"], inplace=True)

        game_df.drop(["exp_pts_off", "exp_pts_def", "exp_pts_st"], axis=1, inplace=True)

        for col in cols_to_shift:
            new_col = "prev_" + col
            game_df[new_col] = game_df.groupby("team_year")[col].apply(
                lambda grp: grp.shift(1)
            )

        return game_df

    def add_off_bye_col(game_df):
        def apply_off_bye(row):
            if row["season_week"] > 1:  # playoff games have week_num = 0
                off_bye = row["season_week"] - row["prev_season_week"] == 2
            else:
                off_bye = False
            if off_bye == True:
                return 1
            return 0

        game_df["off_bye"] = game_df[["season_week", "prev_season_week"]].apply(
            apply_off_bye, axis=1
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
            "result_win",
            "result_tie",
            "pts_off",
            "pts_def",
            "margin",
            "first_down_off",
            "yards_off",
            "pass_yds_off",
            "rush_yds_off",
            "to_off",
            "to2_off",
            "yards_def",
            "pass_yds_def",
            "rush_yds_def",
            "to_def",
            "to2_def",
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

        game_df.insert(loc=53, column="roll3_num_wins", value=rolling_wins)

        rolling_ties = game_df.groupby("team_year")["result_tie"].transform(
            lambda x: round(x.shift(1).rolling(3).sum(), 3)
        )

        game_df.insert(loc=53, column="roll3_num_ties", value=rolling_ties)

        # add 3-16 roll_wins & ties (sum instead of mean) (no loss cols to prevent multicolinearity)
        roll16_num_wins = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).rolling(16, 3).sum(), 3)
        )

        game_df.insert(loc=53, column="roll16_num_wins", value=roll16_num_wins)

        roll16_num_ties = game_df.groupby("team_year")["result_tie"].transform(
            lambda x: round(x.shift(1).rolling(16, 3).sum(), 3)
        )

        game_df.insert(loc=53, column="roll16_num_ties", value=roll16_num_ties)

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

    def self_join_opp_cols(game_df):
        """
            Currently, each row has all the stats needed for the team in the 'team' column.
            However, we don't have the same information for the opponent in the same row. 

            This function fixes that. It also drops duplicates so that there is only one row per game.
        """
        opp_pull_cols = [
            "game_id",
            "team",
            "opp",
            "off_bye",
            "prev_wins",
            "prev_losses",
            "prev_result_win",
            "prev_result_tie",
            "prev_ties",
            "prev_pts_off",
            "prev_pts_def",
            "prev_margin",
            "prev_first_down_off",
            "prev_yards_off",
            "prev_pass_yds_off",
            "prev_rush_yds_off",
            "prev_to_off",
            "prev_to2_off",
            "prev_first_down_def",
            "prev_yards_def",
            "prev_pass_yds_def",
            "prev_rush_yds_def",
            "prev_to_def",
            "prev_to2_def",
            "roll16_num_wins",
            "roll16_num_ties",
            "roll3_num_wins",
            "roll3_num_ties",
            "roll3_result_win",
            "roll3_result_tie",
            "roll3_result_tie",
            "roll3_pts_off",
            "roll3_pts_def",
            "roll3_margin",
            "roll3_first_down_off",
            "roll3_yards_off",
            "roll3_pass_yds_off",
            "roll3_rush_yds_off",
            "roll3_to_off",
            "roll3_to2_off",
            "roll3_yards_def",
            "roll3_pass_yds_def",
            "roll3_rush_yds_def",
            "roll3_to_def",
            "roll3_to2_def",
            "ewma_result_win",
            "ewma_result_win",
            "ewma_pts_off",
            "ewma_pts_def",
            "ewma_margin",
            "ewma_first_down_off",
            "ewma_yards_off",
            "ewma_pass_yds_off",
            "ewma_rush_yds_off",
            "ewma_to_off",
            "ewma_to2_off",
            "ewma_yards_def",
            "ewma_pass_yds_def",
            "ewma_rush_yds_def",
            "ewma_to_def",
            "ewma_to2_def",
        ]

        # convert all numeric cols to float
        for col in opp_pull_cols[3:]:
            game_df[col] = game_df[col].astype(float)

        # self-join & merge dataFrame on itself, where:
        #   - left_row 'game_id' & 'team' == right_row 'game_id' & 'opp'
        game_df = game_df.merge(
            right=game_df[opp_pull_cols],
            left_on=["game_id", "team"],
            right_on=["game_id", "opp"],
            suffixes=[None, "_opp"],
        )

        game_df.drop_duplicates(subset=["game_id"], inplace=True)

        return game_df

    def add_features(game_df):
        """ 
            Adds several features to game_df, including:
                - ewma_margin_diff: the difference between 'team' ewma margin and 'opp' ewma margin.
        """

        game_df["ewma_margin_diff"] = (
            game_df["ewma_margin"] - game_df["ewma_margin_opp"]
        )

        return game_df

    def main(game_df):
        game_df = add_cols(game_df)
        game_df = drop_useless_cols(game_df)
        game_df = add_prev_week_cols(game_df)
        game_df = add_off_bye_col(game_df)
        game_df = add_roll_cols(game_df)
        game_df = drop_first_three(game_df)
        game_df = self_join_opp_cols(game_df)
        game_df = add_features(game_df)
        return game_df

    # back to clean_games (parent func)
    game_df = main(game_df)
    return game_df
