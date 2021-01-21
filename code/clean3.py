"""
    This script contains nested functions that will apply all data cleaning necessary to model our data.
"""
import pandas as pd
import numpy as np


def clean_games(scraped_games_data, start_year=1990):
    """
        A series of nested functions that readies our data for modeling.

        Takes:
            - scraped_games_data (required): the relative filepath to the 
              .pickle file containing the scraped game_df from web scraping.
            - start_year (int, optional, default=1990): the first season_year
              that will be included in the dataFrame.

        Returns:
            - game_df (DataFrame)
    """
    # define lists of columns that will be used repeatedly
    def_unnamed_cols = [
        "team_home_game",
        "game_id",
        "team",
        "opp",
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
        "third_down_success",
        "third_down_att",
        "fourth_down_success",
        "fourth_down_att",
    ]
    def_named_cols = [col + "_def" for col in def_unnamed_cols]
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
        "pass_cmp_perc",
        "pass_rating",
        "rush_att",
        "rush_yds",
        "rush_yds_per_att",
        "rush_td",
        "fgm",
        "fga",
        "third_down_success",
        "third_down_att",
        "fourth_down_success",
        "fourth_down_att",
        "team_home_game",
        "result_tie",
        "result_win",
    ]

    def read_pickle(scraped_games_data, start_year=1980):

        game_df = pd.read_pickle(scraped_games_data)
        assert start_year >= 1960, AssertionError(
            "Please choose a start_year value between 1961 and 2019."
        )
        if start_year > 1960:
            mask = game_df["season_year"] >= start_year
            game_df = game_df[mask]

        return game_df

    def clean_home_games(game_df):
        # clean the game_location column & apply change. def home_game(row):
        def apply_home_game(row):
            if row == "@":
                return 0
            return 1

        game_df["team_home_game"] = game_df.game_location.apply(apply_home_game)
        game_df.drop("game_location", axis=1, errors="ignore", inplace=True)

        return game_df

    def add_initial_cols(game_df):
        team_years = game_df["team"] + "-" + game_df["season_year"].astype(str)
        game_df.insert(loc=2, column="team_year", value=team_years)
        game_df.sample()

        # now drop the game_date col
        game_df.drop("game_date", axis=1, inplace=True, errors="ignore")
        game_df.sample(2)  # to confirm

        # add decade column
        decades = game_df["season_year"] // 10
        game_df.insert(loc=5, column="decade", value=decades)
        game_df[["full_game_date", "decade"]].sample(10)

        def apply_game_id(row):
            teams = []
            teams.append(str(row["team"]))
            teams.append(str(row["opp"]))
            teams.sort()

            game_id = teams[0] + "-" + teams[1] + "-" + str(row["full_game_date"])[:-9]
            return game_id

        game_ids = game_df.apply(apply_game_id, axis=1)
        game_df.insert(loc=0, column="game_id", value=game_ids)

        return game_df

    def get_def_stats(game_df):

        game_df = game_df.merge(
            right=game_df[def_unnamed_cols],
            left_on=["game_id", "team"],
            right_on=["game_id", "opp"],
            suffixes=[None, "_def"],
        )

        game_df.drop(columns=["team_def", "opp_def"], axis=1, inplace=True)

        return game_df

    def cleanup_dtypes_nans(game_df):
        # remove missing 'opp' rows
        game_df["opp"] = game_df.opp.astype(str)
        game_df["opp"].replace({"nan": np.NaN}, inplace=True)
        game_df.dropna(subset=["opp", "game_outcome"], how="any", inplace=True)

        game_df["pass_yds_per_att"].replace({str("-0"): 0.0}, inplace=True)
        game_df["pass_net_yds_per_att"].replace({str("-0"): 0.0}, inplace=True)

        game_df[
            [
                "pass_yds",
                "pass_yds_per_att",
                "pass_net_yds_per_att",
                "rush_yds",
                "rush_yds_per_att",
            ]
        ] = game_df[
            [
                "pass_yds",
                "pass_yds_per_att",
                "pass_net_yds_per_att",
                "rush_yds",
                "rush_yds_per_att",
            ]
        ].astype(
            float
        )

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

        # now convert overtime col to binary
        game_df.fillna(value={"overtime": 0}, inplace=True)
        game_df["overtime"].replace({"OT": 1}, inplace=True)

        game_df[["result_tie", "result_win"]] = pd.get_dummies(
            game_df.game_outcome, drop_first=True
        )

        # add 'margin' col

        margins = game_df["pts_off"] - game_df["pts_def"]
        game_df.insert(loc=11, column="margin", value=margins)

        game_df.dropna(axis=0, how="any", subset=["game_outcome"], inplace=True)
        return game_df

    def perform_shifts(game_df):
        # add 'prev_week_num' col
        game_df["prev_week_num"] = game_df.groupby("team_year")["week_num"].apply(
            lambda grp: grp.shift(1)
        )

        # add 'prev_result_win' col
        game_df["prev_result_win"] = game_df.groupby("team_year")["result_win"].apply(
            lambda grp: grp.shift(1)
        )

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

        game_df.sort_values(["season_year", "team", "week_num"], inplace=True)

        [roll_cols.append(col) for col in def_named_cols]

        # Add roll3_wins column
        game_df["roll3_wins"] = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).rolling(3).sum())
        )

        roll_cols.remove("game_id_def")
        roll_cols.remove("team_def")
        roll_cols.remove("opp_def")

        # add ewma cols
        game_df[roll_cols] = game_df[roll_cols].astype(float)

        ewma19_cols = ["ewma_" + col_name for col_name in roll_cols]

        game_df[ewma19_cols] = game_df.groupby("team_year")[roll_cols].transform(
            lambda x: round(x.shift(1).ewm(span=19, min_periods=3).mean(), 3)
        )

        return game_df

    def add_features(game_df):
        """
            Adds the following features to game_df for modeling:
                - roll19_wins       (sum)
                - roll19_margin     (sum)
                - ewma10_wins       (mean)
                - ewma10_margin     (mean)
                - ewma4_wins        (mean)
                - ewma4_margin      (mean)
                - log_year
                - and the following 19 week rolling means:   
                    - ewma_third_conv_pct
                    - ewma_third_conv_pct_opp
                    - ewma_pass_cmp_def
                    - ewma_pass_cmp_perc_def
                    - ewma_pass_cmp_perc_def_opp
                    - ewma4_margin_opp
        """

        # add rolling 19 wins & margin as well as alternate ewma 19 wins
        game_df[["roll19_wins"]] = game_df.groupby("team_year")["result_win"].transform(
            lambda x: x.shift(1).rolling(19, min_periods=3).sum()
        )
        game_df[["roll19_margin"]] = game_df.groupby("team_year")["margin"].transform(
            lambda x: x.shift(1).rolling(19, min_periods=3).sum()
        )

        game_df[["ewma10_wins"]] = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).ewm(span=10, min_periods=3).mean())
        )
        game_df[["ewma10_margin"]] = game_df.groupby("team_year")["margin"].transform(
            lambda x: round(x.shift(1).ewm(span=10, min_periods=3).mean())
        )

        game_df[["ewma4_wins"]] = game_df.groupby("team_year")["result_win"].transform(
            lambda x: round(x.shift(1).ewm(span=4, min_periods=3).mean())
        )
        game_df[["ewma4_margin"]] = game_df.groupby("team_year")["margin"].transform(
            lambda x: round(x.shift(1).ewm(span=4, min_periods=3).mean())
        )

        return game_df

    def pull_opposing_stats(game_df):
        # gather one list of columns to get from the other game record.
        prefixes = ["prev", "roll", "ewma"]

        opp_pull_cols = [col for col in game_df.columns if col[:4] in prefixes]
        # add defensive columns
        [opp_pull_cols.append(col) for col in game_df.columns if col[-4:] == "_def"]
        additional_cols = [
            "roll19_wins",
            "roll19_margin",
            "ewma10_wins",
            "ewma10_margin",
            "ewma4_wins",
            "ewma4_margin",
        ]
        [opp_pull_cols.append(col) for col in additional_cols]

        opp_pull_cols = list(set(opp_pull_cols))
        opp_pull_cols.sort()
        opp_pull_cols.insert(0, "opp")
        opp_pull_cols.insert(0, "team")
        opp_pull_cols.insert(0, "game_id")

        game_df = game_df.merge(
            right=game_df[opp_pull_cols],
            left_on=["game_id", "team"],
            right_on=["game_id", "opp"],
            suffixes=[None, "_opp"],
        )

        game_df.drop(columns=["team_opp", "opp_opp"], axis=1, inplace=True)

        return game_df

    def add_more_features(game_df):
        # add log_year feature
        game_df["log_year"] = np.log(game_df.season_year)

        game_df["ewma_third_conv_pct"] = (
            game_df["ewma_third_down_success"] / game_df["ewma_third_down_att"]
        )

        game_df["ewma_third_conv_pct_def"] = (
            game_df["ewma_third_down_success_def"] / game_df["ewma_third_down_att_def"]
        )

        game_df["ewma_third_conv_pct_opp"] = (
            game_df["ewma_third_down_success_opp"] / game_df["ewma_third_down_att_opp"]
        )

        game_df["ewma_third_conv_pct_def_opp"] = (
            game_df["ewma_third_down_success_def_opp"]
            / game_df["ewma_third_down_att_def_opp"]
        )

        # total yds

        game_df["ewma_total_yds_off"] = (
            game_df["ewma_pass_yds"] + game_df["ewma_pass_td"]
        )

        game_df["ewma_total_yds_def"] = (
            game_df["ewma_pass_yds_def"] + game_df["ewma_pass_yds_opp"]
        )

        game_df["ewma_total_yds_off_opp"] = (
            game_df["ewma_pass_yds_opp"] + game_df["ewma_rush_yds_opp"]
        )

        game_df["ewma_total_yds_def_opp"] = (
            game_df["ewma_pass_yds_def_opp"] + game_df["ewma_rush_yds_def_opp"]
        )

        # add trending margin (ewma4 margin - ewma19 margin)
        game_df["trend_ewma4_19"] = game_df["ewma4_margin"] - game_df["ewma_margin"]

        game_df["trend_ewma4_19_opp"] = (
            game_df["ewma4_margin_opp"] - game_df["ewma_margin_opp"]
        )

        return game_df

    def main(scraped_games_data, start_year):
        game_df = read_pickle(scraped_games_data, start_year)
        game_df = clean_home_games(game_df)
        game_df = add_initial_cols(game_df)
        game_df = get_def_stats(game_df)
        game_df = cleanup_dtypes_nans(game_df)
        game_df = perform_shifts(game_df)
        game_df = add_features(game_df)
        game_df = pull_opposing_stats(game_df)
        game_df = add_more_features(game_df)

        return game_df

    game_df = main(scraped_games_data, start_year)
    return game_df
