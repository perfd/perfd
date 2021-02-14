import pprint as pp
import numpy as np
import time
import math
from collections import OrderedDict

from sklearn.model_selection import RandomizedSearchCV, GridSearchCV

import sklearn
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import make_scorer, mean_squared_error

from oracle.learn.eval import rrmse, rmsre, make_rmsre_with_scaler
import oracle.learn.scikit_models as sk


def random_search(model, params):
    # run randomized search
    n_iter_search = 20
    return RandomizedSearchCV(model, param_distributions=params,
                              n_iter=n_iter_search, cv=5, verbose=2)


def grid_search(model, params, scorer=None):
    return GridSearchCV(model, scoring=scorer,
                        param_grid=params, cv=3, verbose=0, n_jobs=4)  # TODO: make cv to 2


def search(model, X, y, params, search_func=grid_search, scorer=None, quiet=False):
    start = time.time()
    s = search_func(model, params, scorer)
    # print(X.shape, y.shape)
    # print(sklearn.__path__)
    # print(type(s))
    quiet = False
    print(X.shape, y.shape)
    s.fit(X, y)

    print("Search took %.2f seconds parameter settings." % (time.time() - start))
    return report(s.cv_results_, quiet=quiet)


# Utility function to report best scores
def report(results, n_top=1, quiet=False):
    opt_params = {}
    for i in range(1, n_top + 1):
        candidates = np.flatnonzero(results['rank_test_score'] == i)
        for j, candidate in enumerate(candidates):
            if j >= n_top:
                break
            if not quiet:
                print("Model with rank: {0}".format(i))
                print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
                    results['mean_test_score'][candidate],
                    results['std_test_score'][candidate]))
                print("Parameters: {0}".format(results['params'][candidate]))
                print("")

            opt_params = results['params'][candidate]
    return opt_params


class _DefaultModels:
    lr = LinearRegression()
    lr_params = {
        "fit_intercept": [True, False],
        "normalize": [True, False],
    }

    knn = KNeighborsRegressor()
    knn_params = {
        "n_neighbors": [2],
        "weights": ["distance", "uniform"],
        "algorithm": ["auto", "ball_tree", "kd_tree", "brute"],
    }

    rf = RandomForestRegressor()
    rf_params = {
        'bootstrap': [True, False],
        'max_depth': [1, 10, 100, None],
        'max_features': ['auto', 'sqrt'],
        'min_samples_leaf': [1, 10],
        'min_samples_split': [2, 10],
        'n_estimators': [1, 10, 100]
    }

    svm_linear = SVR()
    svm_linear_params = {
        'C': [0.01, 0.05, 0.1, 0.5, 1, 2],
        'kernel': ['linear'],
        'max_iter': [20000],
        'epsilon': [0.01, 0.05, 0.1, 0.5, 1, 2],
    }

    svm = SVR()
    svm_params = {
        'C': [0.01, 0.1, 1, 10],
        'gamma': [0.1, 0.001, 0.0001],
        'epsilon': [0.01, 0.1, 1],
        'kernel': ['rbf'],
        'max_iter': [20000]
    }

    nn = MLPRegressor()
    nn_params = {
        'hidden_layer_sizes': [(), (10,), (50, 50,), (100,), ],
        'activation': ['tanh', 'relu'],
        'solver': ['adam', 'lbfgs', 'sgd'],  # the bias term
        'alpha': [0.0001, 0.001, 0.01, 1],
        'learning_rate': ['constant', 'invscaling', 'adaptive'],
        'max_iter': [20000],
        'early_stopping': [True],
    }


class Model:
    def __init__(self, name, algorithm, params, runner, alias):
        self.name = name
        self.algorithm = algorithm
        self.params = params
        self.runner = runner
        self.alias = alias


default_models = OrderedDict({
    "linear_regression": Model(name="linear_regression", algorithm=_DefaultModels.lr,
                               params=_DefaultModels.lr_params, runner=sk.linear_regression, alias="lr"),
    "nearest_neighbors": Model(name="nearest_neighbors", algorithm=_DefaultModels.knn,
                               params=_DefaultModels.knn_params, runner=sk.nearest_neighbors, alias="knn"),
    "random_forest": Model(name="random_forest", algorithm=_DefaultModels.rf,
                           params=_DefaultModels.rf_params, runner=sk.random_forest, alias="rf"),
    "svm": Model(name="svm", algorithm=_DefaultModels.svm_linear,
                 params=_DefaultModels.svm_linear_params, runner=sk.svm, alias="svm"),
    "svm_kernelized": Model(name="svm_kernelized", algorithm=_DefaultModels.svm,
                            params=_DefaultModels.svm_params, runner=sk.svm_kernelized, alias="svm_k"),
    "neural_network": Model(name="neural_network", algorithm=_DefaultModels.nn,
                            params=_DefaultModels.nn_params, runner=sk.neural_network, alias="nn"),
})


def hypertune(X, y, models: list = None, ow_params=None, scorer=None, quiet=False):
    if scorer is None:
        scorer = make_scorer(rmsre, greater_is_better=False)

    models = default_models if models is None else models

    # some dataset specific patches
    if "nearest_neighbors" in models:
        # TODO: add a warning for too few samples
        models["nearest_neighbors"].params["n_neighbors"] = [2 ** i for i in range(min(int(math.log(len(y), 2)), 6))]

    opt_params = []
    for _, m in models.items():
        params_to_search = m.params if ow_params is None else {**m.params, **ow_params}
        # opt_params.append((m.name, search(m.algorithm, X, y, params_to_search, scorer=scorer, quiet=quiet)))
        # print(opt_params)
        try:
            opt_params.append((m.name, search(m.algorithm, X, y, params_to_search, scorer=scorer, quiet=quiet)))
            print(opt_params)
        except Exception as e: # TODO: this is just temporary workaround
            print("tune: search failed on model:", m.name, "use the default parameters because of:", e)
            opt_params.append((m.name, {'max_iter': 40000}))

    if not quiet:
        print("------copy-and-paste below to your code------\n")
        print("[")
        for name, op in opt_params:
            print("     models.ml(models.{},{}),".format(name, op))
        print("]")
    return opt_params


def hypertune_e2e(*args, **kwargs):
    """Given a dataset, returns all 6 models' optimal parameter sets."""
    start = time.time()
    print("hyptertune: start tuning..")
    opt_params = hypertune(*args, **kwargs)
    print("hyptertune: took %d s" % int(time.time() - start))
    return opt_params


if __name__ == '__main__':
    import redis, pickle
    import oracle.learn.preproc as preproc
    from sklearn.preprocessing import StandardScaler
    from oracle.learn.preproc import get_dataset
    from oracle.feature.utils import make_feature_mod

    # loading the dataset
    r = redis.StrictRedis(host='localhost')
    # df = pickle.loads(r.get("orc_nes_100_nf_src"))
    # df = pickle.loads(r.get("orc_dss_100"))
    df, le = pickle.loads(r.get("orc_ins_100_src"))
    df = preproc.remove_outlier(df, "jct")

    feature_gate = []
    feature_mode = ["static"]
    # feature_select = ["num_executor"]
    feature_select = []

    # use the same scaler
    scaler = StandardScaler()
    rmsre = make_rmsre_with_scaler(scaler)  # make_rmsre_with_scaler
    scorer = make_scorer(rmsre, greater_is_better=False)

    for fm in feature_mode:
        X, y = get_dataset(df.reset_index(drop=True),
                           scale_func=scaler.fit_transform,
                           feature_mod=make_feature_mod(fm, feature_gate, feature_select))
        hypertune(X, y, scorer)
    # X, y = preproc.get_dataset(df_test)

    # hypertune(X, y)
