from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, PolynomialFeatures, OneHotEncoder


def split_and_validate2(X, y, print_results=False):
    """
        For a set of features and target X, y, perform a 80/20 train/val split, 
        fit and validate a linear regression model, and report results
    """

    # perform train/val split
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # fit linear regression to training data
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    # score fit model on validation data
    val_score = lr_model.score(X_val, y_val)

    # report results
    if print_results:
        print("\nValidation R^2 score was:", val_score)
        print("Feature coefficient results: \n")
        for feature, coef in zip(X.columns, lr_model.coef_):
            print(feature, ":", f"{coef:.2f}")

    return lr_model
