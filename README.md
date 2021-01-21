# Metis Data Science Bootcamp | Project 2

## Linear Regression: Using NFL statistics to predict the victory margin of a given NFL game

Duration: 8 days (in progress, due Jan 22)

---
## Objective

To experiment with Ridge and Lasso Regression techniques to create a model that can be used to identify games in which sportsbook predictions are skewed to one side.

---
## Tech Stack

Python3 libraries:
- pandas
- numpy
- BeautifulSoup
- scikit-learn
- StatsModels
- requests

---
## Gathering & Cleaning the Data

Related files: `scrape.py` & `clean.py`

The work that went into these scripts can be found in: `scrape_game_data.ipynb` and `clean_game_data.ipynb`.

### Key Processes

`scrape.py` uses the `BeautifulSoup` python library to programatically scrape statistics from every NFL game dating back to 1960 from the [Pro Football Reference website](pro-football-reference.com). We could have gone back further, but the overall objective here is to predict	
  * see: `scrape.py` and `scrape_team_schedules.ipynb`

---
## Algorithms

### Feature Engineering Highlights

* Introduce $turnovers^2$ feature to our model to account for low variance
* Introduce several rolling statistics for each category in the data:
  * 3-game rolling mean statistics 
  * 3-16 week Exponentially Weighted Moving Average (see: [EWMA (wikipedia)](https://en.wikipedia.org/wiki/Moving_average#Exponential%20moving%20average))

### Model Selection

Standard Linear Regression model consistently yielded the highest R^2 values. 
Lasso: Used to enhance feature selection process & further increase R^2 in Standard Linear Regression


---
## Ideas/Notes (in progress)

- are different features better predictors for different teams? (i.e. bears + 300 passing yards may have larger impact than Chiefs)
- must get L1/L2/L3 game trends for each team_year (weighting last game higher than previous, higher than previous)
- add DECADE as a feature?
	- floor divide year // 100

---
## Data Sources

- NFL Team Statistics: [Pro Football Reference](pro-football-reference.com)
- NFL Historical Spread Data: [kaggle.com/tobycrabtree](https://www.kaggle.com/tobycrabtree/nfl-scores-and-betting-data)
