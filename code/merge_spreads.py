import pandas as pd
import numpy as np
from clean import clean_games
from bs4 import BeautifulSoup
import requests
import re


def get_spreads(game_df):
    def pull_historical():

        spread_df = pd.read_csv("data/spreadspoke_scores.csv")
        spread_df.drop(
            columns=[
                "schedule_week",
                "over_under_line",
                "stadium",
                "stadium_neutral",
                "weather_temperature",
                "weather_wind_mph",
                "weather_humidity",
                "weather_detail",
            ],
            inplace=True,
        )

        team_map = {
            "Miami Dolphins": "mia",
            "Tampa Bay Buccaneers": "tam",
            "Chicago Bears": "chi",
            "Oakland Raiders": "rai",
            "Detroit Lions": "det",
            "Atlanta Falcons": "atl",
            "Cleveland Browns": "cle",
            "San Francisco 49ers": "sfo",
            "Baltimore Colts": "clt",
            "Denver Broncos": "den",
            "Houston Oilers": "oti",
            "San Diego Chargers": "sdg",
            "New York Giants": "nyg",
            "Dallas Cowboys": "dal",
            "Los Angeles Rams": "ram",
            "Pittsburgh Steelers": "pit",
            "New York Jets": "nyj",
            "New Orleans Saints": "nor",
            "Seattle Seahawks": "sea",
            "Cincinnati Bengals": "cin",
            "Philadelphia Eagles": "phi",
            "Washington Redskins": "was",
            "St. Louis Cardinals": "crd",
            "Minnesota Vikings": "min",
            "Kansas City Chiefs": "kan",
            "Green Bay Packers": "gnb",
            "Buffalo Bills": "buf",
            "New England Patriots": "nwe",
            "Los Angeles Raiders": "rai",
            "Indianapolis Colts": "clt",
            "Phoenix Cardinals": "crd",
            "Arizona Cardinals": "crd",
            "Carolina Panthers": "car",
            "St. Louis Rams": "ram",
            "Jacksonville Jaguars": "jax",
            "Baltimore Ravens": "rav",
            "Tennessee Oilers": "oti",
            "Tennessee Titans": "oti",
            "Houston Texans": "htx",
            "Los Angeles Chargers": "sdg",
            "Las Vegas Raiders": "rai",
            "Washington Football Team": "was",
        }
        spread_df["team_home"] = spread_df["team_home"].map(team_map)
        spread_df["team_away"] = spread_df["team_away"].map(team_map)

        abbrev_map = {
            "MIA": "mia",
            "TB": "tam",
            "CHI": "chi",
            "KC": "kan",
            "LAR": "ram",
            "MIN": "min",
            "NE": "nwe",
            "NO": "nor",
            "NYJ": "nyj",
            "PHI": "phi",
            "PIT": "pit",
            "LAC": "sdg",
            "ARI": "crd",
            "WAS": "was",
            "PICK": "PICK",
            "DEN": "den",
            "DET": "det",
            "SF": "sfo",
            "TEN": "oti",
            "ATL": "atl",
            "CLE": "cle",
            "DAL": "dal",
            "LVR": "rai",
            "SEA": "sea",
            "IND": "clt",
            "BUF": "buf",
            "CIN": "cin",
            "GB": "gnb",
            "NYG": "nyg",
            "BAL": "rav",
            "JAX": "jax",
            "CAR": "car",
            "HOU": "htx",
        }

        spread_df["team_favorite_id"] = spread_df["team_favorite_id"].map(abbrev_map)
        spread_df["schedule_date"] = pd.to_datetime(spread_df["schedule_date"])
        spread_df.dropna(axis=0, subset=["spread_favorite"], inplace=True)

        def apply_game_id(row):
            teams = []
            teams.append(str(row["team_home"]))
            teams.append(str(row["team_away"]))
            teams.sort()

            game_id = teams[0] + "-" + teams[1] + "-" + str(row["schedule_date"])[:-9]
            return game_id

        spread_df["game_id"] = spread_df.apply(apply_game_id, axis=1)
        spread_df.index = spread_df.game_id

        def apply_spread_cols(row):
            id_ = row["game_id"]
            tm = row["team"]
            opp = row["opp"]

            try:
                fav = spread_df.loc[id_]["team_favorite_id"]
                spread = spread_df.loc[id_]["spread_favorite"]

                if fav == "PICK":
                    return 0

                if tm == fav:
                    return (
                        spread * -1.0
                    )  # margin should be positive in game_df if 'team' is favored

                if opp == fav:
                    return spread
            except:
                return np.NaN

        game_df["vegas_pred_margin"] = game_df.apply(apply_spread_cols, axis=1)

        return game_df

    def pull_2020_spreads(game_df):
        response = requests.get("https://www.oddsshark.com/nfl/2020-spreads-all-games")
        soup = BeautifulSoup(response.text, "lxml")

        h2s = str(soup.find_all("div")).split("<h2>")

        i = 89

        spreads = []

        while i <= 105:

            text = h2s[i]
            soup = BeautifulSoup(text, "lxml")
            week_num = int(re.search(r"NFL Week ([0-9]+) Odds", str(text))[1])

            soup = BeautifulSoup(text, "lxml")
            weeks = soup.find_all("tr")

            for week in weeks:
                team_spread = {}
                pattern = r"<td>([ \w]+)</td><td>([\+-][0-9]+)<"
                try:
                    result = re.search(pattern, str(week))
                    team_name = result[1]
                    spread = float(result[2])
                    team_spread["week_num"] = week_num
                    team_spread["team"] = team_name
                    team_spread["spread"] = spread
                    spreads.append(team_spread)
                except:
                    continue

            i += 1

        spread_df_2020 = pd.DataFrame(spreads)
        oddshark_map = {
            "Miami Dolphins": "mia",
            "New England Patriots": "nwe",
            "Seattle Seahawks": "sea",
            "Atlanta Falcons": "atl",
            "Chicago Bears": "chi",
            "Detroit Lions": "det",
            "Los Angeles Chargers": "sdg",
            "Cincinnati Bengals": "cin",
            "Arizona Cardinals": "crd",
            "San Francisco 49ers": "sfo",
            "Pittsburgh Steelers": "pit",
            "New York Giants": "nyg",
            "Cleveland Browns": "cle",
            "Los Angeles Rams": "ram",
            "Philadelphia Eagles": "phi",
            "Carolina Panthers": "car",
            "Tampa Bay Buccaneers": "tam",
            "Denver Broncos": "den",
            "Dallas Cowboys": "dal",
            "Buffalo Bills": "buf",
            "Jacksonville Jaguars": "jax",
            "Tennessee Titans": "oti",
            "Baltimore Ravens": "rav",
            "Houston Texans": "htx",
            "Kansas City Chiefs": "kan",
            "Washington Football Team": "was",
            "Green Bay Packers": "gnb",
            "New York Jets": "nyj",
            "Indianapolis Colts": "col",
            "Minnesota Vikings": "min",
            "New Orleans Saints": "nor",
            "Las Vegas Raiders": "rai",
        }

        spread_df_2020["team"] = spread_df_2020["team"].map(oddshark_map)

        spread_df_2020["team_week"] = (
            spread_df_2020["week_num"].astype(str) + "-" + spread_df_2020["team"]
        )
        spread_df_2020.index = spread_df_2020["team_week"]

        def apply_2020_spreads(row):
            if row["vegas_pred_margin"] == np.NaN:

                team = row["team"]
                week = row["week_num"]

                team_week = week.astype(str) + "-" + team
                return team_week
            else:
                return row["vegas_pred_margin"]

        week_nums = [
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "Wild Card",
            "18",
            "Division",
            "Conf",
            "SuperBowl",
        ]

        game_df["oddshark_spread"] = game_df[
            ["team", "week_num", "year", "vegas_pred_margin"]
        ].apply(apply_2020_spreads, axis=1)

        return game_df

    def main():
        game_df = pull_historical()
        game_df = pull_2020_spreads(game_df=game_df)

        return game_df

    game_df = main()
    return game_df

