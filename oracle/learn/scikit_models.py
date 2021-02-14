"""Define ML models."""
from collections import namedtuple

from sklearn.svm import NuSVR, SVR
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor


def svm(X, y, config_map=None):
    """Support Vector Regression.

    Example kernels:
        Linear (default): {"kernel": "linear", "C": 1e3}
        RBF: {"kernel": "rbf", "C": 1e3, "gamma": 0.1}
        Polynomial: {"kernel": "poly", "C": 1e3, "degree": 2}
    """
    # TODO: pick a setting, make sure how to use these models correctly
    # TODO: just svm, lr, nearest-neighbor
    # TODO: changing the value of C and do validation, try C in the log space
    # TODO: plot C-error

    config_map = config_map if config_map else {"kernel": "linear"}

    if "cache_size" not in config_map:
        config_map["cache_size"] = 8000

    if "max_iter" not in config_map:
        config_map["max_iter"] = 50000

    svr = SVR(**config_map)
    model = svr.fit(X, y)

    return model


def linear_regression(X, y, config_map=None):
    """Ordinary Least Square."""
    config_map: dict = config_map if config_map else {}
    if "max_iter" in config_map:
        del config_map["max_iter"]

    lr = LinearRegression(**config_map)
    model = lr.fit(X, y)

    return model


def svm_kernelized(X, y, config_map={"kernel": "rbf"}):
    """Kernelized Support Vector Regression."""
    # return svm(X, y, {"kernel": "rbf", "C": 1e3, "gamma": 0.1})
    return svm(X, y, config_map)


def nearest_neighbors(X, y, config_map=None):
    """K-Nearest-Neighbors Regressor."""
    config_map = config_map if config_map else {}

    if "max_iter" in config_map:
        del config_map["max_iter"]
    if config_map.get("n_neighbors", 1) > len(X):
        config_map["n_neighbors"] = len(X)

    knr = KNeighborsRegressor(**config_map)

    model = knr.fit(X, y)

    return model


def random_forest(X, y, config_map=None):
    """Random Forest Regressor."""
    config_map = config_map if config_map else {"max_depth": 2, "random_state": 42}
    if "max_iter" in config_map:
        del config_map["max_iter"]

    rfr = RandomForestRegressor(**config_map)
    model = rfr.fit(X, y)

    return model


def neural_network(X, y, config_map=None):
    """Multi-Layer Perceptron Regressor."""
    # TODO: do validation on max_iter, range log space?
    config_map = config_map if config_map else {"max_iter": 5000, "early_stopping": True}

    mlpr = MLPRegressor(**config_map)
    model = mlpr.fit(X, y)

    return model


ml = namedtuple("ml_model", ["model", "config"])

Model = ml

basic_models = [
    ml(linear_regression, None),
    ml(nearest_neighbors, None),
]

intermediate_models = [
    ml(svm, None),
    ml(svm_kernelized, None),
]

adv_models = [
    ml(random_forest, None),
    ml(neural_network, None),
]

fast_models = [
    ml(linear_regression, None),
    ml(nearest_neighbors, None),
    ml(random_forest, None),
]

all_models = basic_models + intermediate_models + adv_models

nn = [ml(neural_network, None)]
