"""
    This script contains nested functions that will apply all data cleaning necessary to model our data.
"""
import pandas as pd
import numpy as np


def clean_games(scraped_games_data, start_year=1980):
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
        mask = game_df["season_year"] >= start_year
        game_df = game_df[mask]

    roll_cols = [
        "pts_off",
        "margin",
        "pts_def",
        "pass_cmp",
        "pass_att",
        "pass_yds",
        "pass_td",
        "pass_int",
        "pass_sacked",
        "pass_sacked_yds",
        "pass_yds_per_att",
        "pass_net_yds_per_att",
        "pass_cmp_perc",
        "pass_rating",
        "rush_att",
        "rush_yds",
        "rush_yds_per_att",
        "rush_td",
        "fgm",
        "fga",
        "xpm",
        "xpa",
        "punt",
        "punt_yds",
        "third_down_success",
        "third_down_att",
        "fourth_down_success",
        "fourth_down_att",
        "team_home_game",
        "result_tie",
        "result_win",
    ]

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
                - td_off
                - total_yds_off
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
        team_years = game_df["team"] + "-" + game_df["season_year"].astype(str)
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

        # DROPNA RUSH_YDS PASS_YDS THIS COULD BREAK SOMETHING LATER ON
        game_df.dropna(subset=roll_cols, how="any", inplace=True)

        # add total_td_off col
        game_df["total_td_off"] = game_df["rush_td"] + game_df["pass_td"]
        roll_cols.append("total_td_off")

        # add total_yds_off col
        game_df["total_yds_off"] = game_df["rush_yds"].astype(int) + game_df[
            "pass_yds"
        ].astype(int)
        roll_cols.append("total_yds_off")

        return game_df

    def drop_useless_cols(game_df):
        """ 
            Removes rows where the 
            Removes the following columns:
                - 'game_day_of_week'
                - 'game_date': will instead be using 'full_game_date'
                - 'game_location': since we replaced with boolean ('team_home_game')
        """
        game_df.drop("game_date", axis=1, inplace=True, errors="ignore")
        game_df.drop("game_day_of_week", axis=1, inplace=True, errors="ignore")
        game_df.drop("game_location", axis=1, errors="ignore", inplace=True)

        return game_df

    def drop_missing_values(game_df):
        """
            Drops any rows missing critical data.
        """

        # remove missing 'opp' rows
        game_df["opp"] = game_df.opp.astype(str)
        game_df["opp"].replace({"nan": np.NaN}, inplace=True)
        game_df.dropna(subset=["opp", "game_outcome"], how="any", inplace=True)

        return game_df

    def fix_formats(game_df):

        numeric_cols = [
            "pass_yds",
            "pass_yds_per_att",
            "pass_net_yds_per_att",
            "rush_yds",
            "rush_yds_per_att",
        ]
        game_df[numeric_cols] = game_df[numeric_cols].astype(float)

        game_df.dropna(
            subset=[
                "third_down_success",
                "third_down_att",
                "fourth_down_success",
                "fourth_down_att",
            ],
            how="any",
            inplace=True,
        )

        # convert overtime column to boolean
        game_df["overtime"].fillna(0, inplace=True)
        game_df["overtime"].replace({"OT": 1}, inplace=True)

        return game_df

    def add_prev_week_cols(game_df):

        cols_to_shift = ["week_num", "result_win", "result_tie", "margin"]

        # convert NaNs to zero (0s came thru as NaN values)
        game_df[cols_to_shift] = game_df[cols_to_shift].fillna(0)

        # drop rows for bye weeks (must do this for shift to work)
        game_df.dropna(axis=0, how="any", subset=["game_outcome"], inplace=True)

        for col in cols_to_shift:
            new_col = "prev_" + col
            game_df[new_col] = game_df.groupby("team_year")[col].apply(
                lambda grp: grp.shift(1)
            )

        return game_df

    def add_off_bye_col(game_df):
        # add 'off_bye' (boolean) col
        def apply_off_bye(row):
            if row["week_num"] > 1:  # playoff games have week_num = 0
                off_bye = row["week_num"] - row["prev_week_num"] == 2
            else:
                off_bye = False
            if off_bye == True:
                return 1
            return 0

        game_df["off_bye"] = game_df[["week_num", "prev_week_num"]].apply(
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
        game_df.sort_values(["season_year", "team", "week_num"], inplace=True)

        # add 1-20 week rolling sum (AKA season totals)
        roll19_cols = ["sn_total_" + col_name for col_name in roll_cols]

        game_df[roll19_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).rolling(19, 1).sum(), 3)
        )

        # add 3roll_wins & ties (sum instead of mean) (no cols to prevent multicolinearity)
        game_df["roll3_num_wins"] = game_df.groupby("team_year")[
            "result_win"
        ].transform(lambda x: round(x.shift(1).rolling(3).sum(), 3))

        game_df["roll3_num_ties"] = game_df.groupby("team_year")[
            "result_tie"
        ].transform(lambda x: round(x.shift(1).rolling(3).sum(), 3))

        # add 3 & 19 week EWMA cols
        ewma3_cols = ["ewma3_" + col_name for col_name in roll_cols]

        game_df[ewma3_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).ewm(span=3, min_periods=3).mean(), 3)
        )

        ewma19_cols = ["ewma19_" + col_name for col_name in roll_cols]

        game_df[ewma19_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).ewm(span=19, min_periods=3).mean(), 3)
        )

        return game_df

    def drop_first_three(game_df):
        """ Drops first 3 weeks of each year. """

        # drop rows without moving averages (first 3 weeks of every season)
        game_df.dropna(axis=0, how="any", subset=["ewma19_pass_yds"], inplace=True)

        return game_df

    def self_join_opp_cols(game_df):
        """
            Currently, each row has all the stats needed for the team in the 'team' column.
            However, we don't have the same information for the opponent in the same row. 

            This function fixes that. It also drops duplicates so that there is only one row per game.
        """
        # put together a list of all columns that need to be merged into the relevant row
        opp_pull_cols = [col for col in roll_cols]
        prefixes = ["prev_", "sn_to", "roll3", "ewma3", "ewma1"]
        [opp_pull_cols.append(col) for col in game_df.columns if col[:5] in prefixes]
        opp_pull_cols.insert(0, "opp")
        opp_pull_cols.insert(0, "team")
        opp_pull_cols.insert(0, "game_id")
        opp_pull_cols = list(set(opp_pull_cols))  # removes duplicates

        # self-join & merge dataFrame on itself, where:
        #   - left_row 'game_id' & 'team' == right_row 'game_id' & 'opp'
        game_df = game_df.merge(
            right=game_df[opp_pull_cols],
            left_on=["game_id", "team"],
            right_on=["game_id", "opp"],
            suffixes=[None, "_opp"],
        )

        # drop duplicate rows, resulting in one row per game.
        game_df.drop_duplicates(subset=["game_id"], inplace=True)

        return game_df

    def add_features(game_df):
        """ 
            Adds several features to game_df, including:
                - ewma_margin_diff: the difference between 'team' ewma margin and 'opp' ewma margin.
        """

        game_df["ewma3_margin_diff"] = (
            game_df["ewma3_margin"] - game_df["ewma19_margin_opp"]
        )

        game_df["ewma19_margin_diff"] = (
            game_df["ewma19_margin"] - game_df["ewma19_margin_opp"]
        )

        return game_df

    def main(game_df):
        game_df = add_cols(game_df)
        game_df = drop_useless_cols(game_df)
        game_df = drop_missing_values(game_df)
        game_df = fix_formats(game_df)
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
