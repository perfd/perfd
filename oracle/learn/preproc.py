"""Define ML-ready dataset, feature selection, and evaluation metrics."""
import numpy as np
import pandas as pd

from sklearn import preprocessing
from sklearn.feature_selection import VarianceThreshold
from sklearn.utils import shuffle

from sklearn.model_selection import cross_val_score, ShuffleSplit, train_test_split
from sklearn.ensemble import RandomForestRegressor

from typing import List, Iterable


def per_feature_train_test_split(df, feature, *args, **kwargs):
    print("debug, per feature split", feature)

    if callable(feature):
        # TODO: support multiple features
        feature = feature(df.columns)[0]
        print("debug, processed feature", feature)

    print("debug, dataframe type:", type(df), "feature type:", type(feature))
    values = sorted(list(set(df[feature].tolist())))
    train_frames, test_frames = list(), list()
    for v in values:
        df_v = df[df[feature] == v]
        # print("debug, per feature split, before split:",
        #       "feature:", feature,
        #       "value:", v,
        #       "num samples:", len(df_v))
        if len(df_v) * kwargs["test_size"] < 1:
            print("debug, per feature split: use same df for train and test num samples:", len(df_v))
            local_train = local_test = df_v
        else:
            local_train, local_test = train_test_split(df_v, *args, **kwargs)
        print("debug, per feature split, after split:", len(local_train), len(local_test))
        train_frames.append(local_train)
        test_frames.append(local_test)
    df_train, df_test = pd.concat(train_frames), pd.concat(test_frames)
    # print("debug: ", df_train[feature])
    return df_train, df_test


class DummyScaler:
    def fit_transform(self, X):
        return np.asarray(X)

    def transform(self, X):
        return np.asarray(X)

    def inverse_transform(self, X):
        return np.asarray(X)


def remove_nested_features(df):
    df_pd = get_df_pd(df)
    cols_to_remove = list()

    for i in range(0, df_pd.shape[1]):
        col = df_pd.columns[i]
        # TODO: this should be more generic than handling python list
        if isinstance(df_pd[col][0], list):
            cols_to_remove.append(col)

    for c in cols_to_remove:
        df_pd = df_pd.drop([c], axis=1)
    return df_pd


def get_df_pd(df):
    if isinstance(df, pd.DataFrame):
        df_pd = df
    else:
        df_pd = df.toPandas()

    return df_pd


def get_df_pd_with_labelencoder(df, target_col="API_Name", target_le=None):
    """Return pandas dataframe."""
    df_pd = get_df_pd(df)

    # pre-process categorical values
    le_to_return = None
    for i in range(0, df_pd.shape[1]):
        le = preprocessing.LabelEncoder()
        col = df_pd.columns[i]

        if col == target_col and target_le is not None:
            df_pd[col] = target_le.transform(df_pd[col])
            le_to_return = target_le
        elif df_pd.dtypes[i] == 'object':
            df_pd[col] = le.fit_transform(df_pd[col])
            le_to_return = le
    # print(list(target_le.classes_))
    if target_le is None:
        return df_pd
    else:
        return df_pd, le_to_return


def remove_outlier(df_pd, feature):
    # TODO: more robust way of outlier detection
    # print("before:", len(df_pd))
    try:
        df_pd = df_pd.astype({feature: "float64"})
    except:
        print("preproc: unable to cast the col %s to float64" % feature)
    df_pd = df_pd[df_pd[feature] < df_pd[feature].quantile(0.99)]
    df_pd = df_pd.reset_index(drop=True)
    # print("after:", len(df_pd))
    return df_pd


def make_drop_feature_func(to_drop: Iterable = ("jct",)):
    def drop_feature(features: List):
        for d in to_drop:
            features.remove(d)
        return features

    return drop_feature


def get_dataset_with_feature_select(df_pd, target_feature="jct", scale_func=None, num_rows=None,
                                    feature_mod=make_drop_feature_func(),
                                    feature_select=False, feature_map=None, quiet=False):
    """Obtain the X, y form dataset from the pandas data frame.

    - feature_mod is a function that allows user to modify the feature set;
    - feature_select is to select features with ml methods.
    """
    # df_pd = shuffle(df_pd, random_state=42)
    np.random.seed(42)
    df_pd.reindex(np.random.permutation(df_pd.index))

    # select columns used as features
    features = list(df_pd)

    if feature_mod is not None:
        features = feature_mod(features)

    num_rows = num_rows if num_rows else df_pd.shape[0]

    # get features
    X = df_pd.loc[:num_rows, features].values.astype(float)
    # get label/output
    # TODO: make it generic to other label name
    y = df_pd[target_feature][:num_rows + 1].values

    support = [True] * X.shape[1]
    # with pd.option_context('display.max_rows', None, 'display.max_columns',
    #                        None):  # more options can be specified also
    #     print(df_pd)

    # apply feature selection
    if feature_select:
        # if a feature map is given, use it to filter feature vectors
        if feature_map is not None:
            X = np.array([col for i, col in enumerate(X.T) if feature_map[i]]).T
            support = feature_map
        else:
            X, support = select_feature(X)

    selected_features = [features[i] for i, j in enumerate(support) if j is True]

    # apply feature scaling
    if scale_func is not None:
        if type(scale_func) is list:
            func_x, func_y = scale_func
        else:
            func_x = func_y = scale_func
        assert callable(func_x) and callable(func_y)
        if not quiet:
            print("X:", X.shape, "y:", y.shape)
        return func_x(X), func_y(y.reshape(-1, 1)).reshape(-1), selected_features
    else:
        return X, y, selected_features


def get_dataset(df_pd, target_feature="jct", scale_func=None, num_rows=None, feature_mod=make_drop_feature_func(),
                quiet=False):
    X, y, _ = get_dataset_with_feature_select(df_pd, target_feature, scale_func, num_rows, feature_mod, quiet=quiet)
    return X, y


# feature selection
def select_feature(X):
    selector = VarianceThreshold()
    return selector.fit_transform(X), selector.get_support()


def select_feature_by_model_rf(X, y, names):
    """Model based feature selection with Random Forest."""
    rf = RandomForestRegressor(n_estimators=20, max_depth=4)
    scores = []
    for i in range(X.shape[1]):
        score = cross_val_score(rf, X[:, i:i + 1], y, scoring="r2",
                                cv=ShuffleSplit(len(X), 3, .3))

        scores.append((round(np.mean(score), 3), names[i]))
        print("on column: {}".format(names[i]))
    return sorted(scores, reverse=True)


if __name__ == '__main__':
    import redis, pickle

    r = redis.StrictRedis(host='localhost')
    # r.set("orc_test", pickle.dumps(get_df_pd(gen_test_df())))
    df_pd_test = pickle.loads(r.get("orc_test"))

    get_dataset(df_pd_test)
