# Metis Data Science Bootcamp | Project 2

## Predicting points margin of NFL games

By Elliot Wilens, Metis Data Scientist

Duration: 8 days 

---
## Objective

To use data gathered via web scraping to predict the resulting points margin of NFL games.

---
## Tech Stack

Python3 libraries:
- pandas
- numpy
- BeautifulSoup web scraping library
- scikit-learn
  - LinearRegression, Lasso, LassoCV, Ridge, RidgeCV Models
  - StandardScaler, K-Fold CV, Polynomial Features
- StatsModels
- requests

---
## Gathering & Cleaning the Data

Related files: `scrape.py` & `clean.py`

The data is pulled from pro-football-reference.com using Python3's BeautifulSoup web scraping library. The work that went into the web scraping script (`scrape.py`) can be found in: `code/other-notebooks/scrape_game_data.ipynb`.

Data cleaning was a tedious process, as I needed to convert the data into time-series data. In addition, the raw data contains two rows per game: one for each team. And, since the data is at the team/game level, the outcome of the game was in the same row as the stats for that game. We can't predict the outcome of a game using that game's stats. The solutions to these problems are summarized in the bullet points below, and the work going into the code can be found in `code/clean_game_data.ipynb`.

- **Time-Series**: implement moving averages; calculations beginning with the team's previous week
- **Self-Join**: in order to pull the opponent stats from the adjacent row, I self-joined the dataFrame to itself on a custom-created game-id field. 

---
## Feature Engineering Highlights

* Introduce several rolling statistics for each category in the data:
  * 3-game rolling mean 
  * 19-week rolling sum 
  * 3, 10, and 19-week Exponentially Weighted Moving Average (see: [EWMA (wikipedia)](https://en.wikipedia.org/wiki/Moving_average#Exponential%20moving%20average))
  * these features are all automatically generated using the functions in `clean.py`. 

---
## Feature Selection

Once I gathered moving averages for all of the available statistics, and then pulled all of them from the opponent's row, there were over 200 potential features. Even though there were 20,000 games in our sample, I needed to reduce the compexity (num. features) significantly the model to generalize well to new data (i.e. avoid overfitting).

To do this, I standardized the features in my train/test set and performed LASSO Regularization to compute the coefficients (slopes) for each modified set of features. After hundreds of experiments with different features, I was able to narrow it down to 20 features that yielded the highest R2 (note that my final model used standard linear regression, as it yielded higher R2 scores than the LASSO and RIDGE models).

Most of the final 20 features are simply variants of exponentially weighted moving averages of game margins. For example, the model seemed to respond well when I included the feature: `4_week_EWMA_margin - 19_week_EWMA_margin`. The model was seemingly able to deduce that this calculation is an indicator for how well the team has played recently relative to their performance all season.

The top three non-margin related features that improved our predictions, in order, were:

1. Home/Away game
2. 3rd Down conversion percentage
3. Defensive yards allowed

---
## Model Selection & Test Results

See: `code/final_model.ipynb`

The Standard Linear Regression model consistently yielded the highest R^2 values, so that's what I chose to use on my test set. 20% of my data was set aside for the test set prior to any modeling. Since none of this data was used to train the model, it will be 'perceived' (if you will) as fresh data for predictions.  

The model scored a .150 on the test set (compared to a .153 during K-Fold cross-validation). This tells us that the model generalized fairly well, and did not seem to be overfit to the training dataset. Having 20,000 datapoints also helped to prevent that.

What does that mean?

- well, it means that the features I've selected can account for 15% of the variance in points margin. Considering that we are literally measuring levels of human error, that's pretty good. It's not going to beat any Vegas lines, but finding large variances to Vegas lines may or may not yield some value for an undecided bettor.
- Please, do not place any bets based on results from this model.

The model correctly determined the outcome of 64% of the games during testing. It also will correctly predict the margin within 13.8 points 68% of the time.

![Prediction vs Actual](https://github.com/edubu2/metis-project2/blob/main/code/figures/pred_vs_actual.png)

---
## Data Sources

- NFL Team Statistics: [Pro Football Reference](https://www.pro-football-reference.com/)
