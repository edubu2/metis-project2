from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
import pickle
import requests
import datetime
import re

team_abbrevs = [
    "crd",
    "atl",
    "rav",
    "buf",
    "car",
    "chi",
    "cin",
    "cle",
    "dal",
    "den",
    "det",
    "gnb",
    "htx",
    "clt",
    "jax",
    "kan",
    "mia",
    "min",
    "nwe",
    "nor",
    "nyg",
    "nyj",
    "rai",
    "phi",
    "pit",
    "sdg",
    "sfo",
    "sea",
    "ram",
    "tam",
    "oti",
    "was",
]


def scrape_reg_season(start_year, end_year, outfile_path):
    games = []
    for year in range(start_year, end_year + 1):

        for team in team_abbrevs:
            url = f"https://www.pro-football-reference.com/teams/{team}/{year}/gamelog/"
            response = requests.get(url)
            if response.status_code == 404:
                continue
            soup = BeautifulSoup(response.text, "lxml")
            try:
                tables = soup.find_all("table")
                reg_table = tables[0]
            except:
                continue

            if len(tables) == 4:
                playoff_team = True
                playoff_table = tables[1]
                defense_reg_table = tables[2]
                playoff_def_table = tables[3]

            elif len(tables) == 2:
                playoff_team = False
                defense_reg_table = tables[1]
                playoff_table = None
                playoff_def_table = None

            tables_to_parse = [
                reg_table,
                playoff_table,
                defense_reg_table,
                playoff_def_table,
            ]
            # get offense
            for table in tables_to_parse:
                if table == None:
                    continue
                if table == defense_reg_table or table == playoff_def_table:
                    defense = True
                else:
                    defense = False
                rows = table.find_all("tr")

                # get regular season games
                if defense == False:
                    for row in rows[2:]:
                        game_stats = {}

                        game_stats["season_year"] = year

                        # pull week_num
                        try:
                            week_num = re.search(
                                r"data-stat=\"week_num\" scope=\"row\">([0-9]+)",
                                str(row),
                            )[1]
                            game_stats["week_num"] = float(week_num)

                        except:
                            pass

                        try:
                            full_game_date = re.search(
                                r"class=\"left\" csk=\"([-0-9]+)\" data-stat=\"game_date",
                                str(row),
                            )[1]
                            full_game_date = datetime.datetime.strptime(
                                full_game_date, "%Y-%m-%d"
                            )
                            game_stats["full_game_date"] = full_game_date
                        except:
                            pass

                        game_stats["team"] = team
                        try:
                            opponent = re.search(
                                r"data-stat=\"opp\"><a href=\"/teams/([\w]{3})/[0-9]{4}\.htm\">[ \w]+</a>",
                                str(row),
                            )[1]
                            game_stats["opp"] = opponent
                        except:
                            pass

                    # pull all stats
                    results = re.findall(r"data-stat=\"([\w]+)\">([ :@\w-]+)", str(row))

                    for result in results:
                        if result != []:
                            stat, val = result
                        if val.isdigit():
                            if defense == False:
                                game_stats[stat] = float(val)
                            else:
                                game_stats[stat + "_def"] = float(val)
                        else:
                            if defense == False:
                                game_stats[stat] = str(val)
                            else:
                                game_stats[stat + "_def"] = str(val)

                    games.append(game_stats)

    game_df = pd.DataFrame(games)
    if outfile_path != None:
        pd.to_pickle(game_df, outfile_path)

    return game_df


game_df = scrape_reg_season(1990, 2020, "data/games.pickle")
