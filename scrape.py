"""Contains a function to scrape team stats from pro-football-reference.com. """

import re
import pandas as pd
import requests
from bs4 import BeautifulSoup


def scrape_team_stats(start_year, end_year, save_to=False):
    """
    Scrapes pro-football-reference.com for team stats data from first_year to last_year (inclusive).

    Returns a pandas DataFrame.

    Takes:
        - start_year (int, required): will scrape seasons from this point on
        - last_year (int, required): will scrape seasons up to & including this season
        - save_to (string, optional): Saves pickled DF to the 'data/' dir. with the given filename
            - file will only be saved if save_to != False
    """
    nfl_team_stats = []  # will contain one dict for each row

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

    for team in team_abbrevs:

        for year in range(start_year, end_year + 1):

            stats = {}  # will contain {stat: value} & be appended to nfl_team_stats

            stats["team"] = team
            stats["year"] = year

            url = f"https://www.pro-football-reference.com/teams/{team}/{year}.htm"

            response = requests.get(url)
            if response.status_code == 404:
                continue

            soup = BeautifulSoup(response.text, "lxml")

            table = soup.find("tbody")
            rows = table.find_all("tr")
            team_table = rows[0]
            opp_table = rows[1]

            opp_stats = {}

            for row in team_table:
                result = re.findall(r"data-stat=\"([\w]+)\">([\w]+)", str(row))
                if result != []:
                    stat, val = result[0]
                    try:
                        val = int(val)
                    except:
                        pass

                    stats[stat] = val

            for row in opp_table:
                result = re.findall(r"data-stat=\"([\w]+)\">([\w]+)", str(row))
                if result != []:
                    stat, val = result[0]
                    try:
                        val = int(val)
                    except:
                        pass
                    opp_stats[stat] = val

            # add 'opp' to the beginning of all opponents stats and merge into team_stats dict
            for stat in opp_stats:
                new_stat_name = "opp_" + stat
                opp_stat = opp_stats[stat]
                stats[new_stat_name] = opp_stat

            nfl_team_stats.append(stats)

    teams_df = pd.DataFrame(nfl_team_stats)

    if save_to:
        teams_df.to_pickle("data/" + str(save_to))

    return teams_df

