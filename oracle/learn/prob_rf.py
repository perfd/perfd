from typing import Dict
import copy
import sys
from collections import defaultdict
from oracle.learn.preproc import DummyScaler
import matplotlib.pyplot as plt
import numpy as np
from ray import tune
from sklearn import mixture
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor, plot_tree
from itertools import chain


def show_decision_tree_path(estimator, X_test):
    # The decision estimator has an attribute called tree_  which stores the entire
    # tree structure and allows access to low level attributes. The binary tree
    # tree_ is represented as a number of parallel arrays. The i-th element of each
    # array holds information about the node `i`. Node 0 is the tree's root. NOTE:
    # Some of the arrays only apply to either leaves or split nodes, resp. In this
    # case the values of nodes of the other type are arbitrary!
    #
    # Among those arrays, we have:
    #   - left_child, id of the left child of the node
    #   - right_child, id of the right child of the node
    #   - feature, feature used for splitting the node
    #   - threshold, threshold value at the node
    #

    # Using those arrays, we can parse the tree structure:

    n_nodes = estimator.tree_.node_count
    children_left = estimator.tree_.children_left
    children_right = estimator.tree_.children_right
    feature = estimator.tree_.feature
    threshold = estimator.tree_.threshold

    # The tree structure can be traversed to compute various properties such
    # as the depth of each node and whether or not it is a leaf.
    node_depth = np.zeros(shape=n_nodes, dtype=np.int64)
    is_leaves = np.zeros(shape=n_nodes, dtype=bool)
    stack = [(0, -1)]  # seed is the root node id and its parent depth
    while len(stack) > 0:
        node_id, parent_depth = stack.pop()
        node_depth[node_id] = parent_depth + 1

        # If we have a test node
        if (children_left[node_id] != children_right[node_id]):
            stack.append((children_left[node_id], parent_depth + 1))
            stack.append((children_right[node_id], parent_depth + 1))
        else:
            is_leaves[node_id] = True

    print("The binary tree structure has %s nodes and has "
          "the following tree structure:"
          % n_nodes)
    for i in range(n_nodes):
        if is_leaves[i]:
            print("%snode=%s leaf node." % (node_depth[i] * "\t", i))
        else:
            print("%snode=%s test node: go to node %s if X[:, %s] <= %s else to "
                  "node %s."
                  % (node_depth[i] * "\t",
                     i,
                     children_left[i],
                     feature[i],
                     threshold[i],
                     children_right[i],
                     ))
    print()

    # First let's retrieve the decision path of each sample. The decision_path
    # method allows to retrieve the node indicator functions. A non zero element of
    # indicator matrix at the position (i, j) indicates that the sample i goes
    # through the node j.

    node_indicator = estimator.decision_path(X_test)

    # Similarly, we can also have the leaves ids reached by each sample.

    leave_id = estimator.apply(X_test)

    # Now, it's possible to get the tests that were used to predict a sample or
    # a group of samples. First, let's make it for the sample.

    sample_id = 0
    node_index = node_indicator.indices[node_indicator.indptr[sample_id]:
                                        node_indicator.indptr[sample_id + 1]]

    print('Rules used to predict sample %s: ' % sample_id)
    for node_id in node_index:
        if leave_id[sample_id] == node_id:
            continue

        if (X_test[sample_id, feature[node_id]] <= threshold[node_id]):
            threshold_sign = "<="
        else:
            threshold_sign = ">"

        print("decision id node %s : (X_test[%s, %s] (= %s) %s %s)"
              % (node_id,
                 sample_id,
                 feature[node_id],
                 X_test[sample_id, feature[node_id]],
                 threshold_sign,
                 threshold[node_id]))

    # For a group of samples, we have the following common node.
    sample_ids = list(range(0, 10))

    common_nodes = (node_indicator.toarray()[sample_ids].sum(axis=0) ==
                    len(sample_ids))

    print(node_indicator)

    common_node_id = np.arange(n_nodes)[common_nodes]

    print("\nThe following samples %s share the node %s in the tree"
          % (sample_ids, common_node_id))
    print("It is %s %% of all nodes." % (100 * len(common_node_id) / n_nodes,))


def display_gmm(gmm, y):
    fig = plt.figure()

    x = np.linspace(min(y) - 1, max(y) + 1, 1000)
    logprob = gmm.score_samples(x.reshape(-1, 1))
    resps = gmm.predict_proba(x.reshape(-1, 1))

    pdf = np.exp(logprob)
    pdf_comp = resps * pdf[:, np.newaxis]
    plt.plot(x, pdf, '-k')
    plt.plot(x, pdf_comp, '--k')
    plt.xlabel("train-set samples")
    plt.ylabel("pdf")

    y = [float(i[0]) for i in y]
    plt.tick_params(labelbottom=False)
    plt.xticks(y, rotation='vertical')


def labels_by_x(X, y):
    """Returns {feature_vector: [value_1, value_2, ...]}"""
    results = defaultdict(list)
    for x, y in zip(X, y):
        results[tuple(x)].append(y)
    return results


def make_prob_dt(dt, X_train):
    """Returns a decision tree with probability output given a decision tree
    point-estimator and the training samples used to train the estimator."""

    class ProbDT:
        def __init__(self, dt, X_train, y_train, gmm_k=2):
            self.dt = dt
            self.X_train = X_train
            self.y_train = y_train

            self.gmm_by_node = dict()
            self.gmm_k = gmm_k

            self._prepare_gmms()

        def _prepare_gmms(self):
            """On each leaf node of the DT, prepare the gmm for the samples."""
            from sklearn import mixture

            nid_sample_map = defaultdict(list)
            leave_ids = dt.apply(self.X_train)

            for s, i in zip(self.y_train, leave_ids):
                nid_sample_map[i].append(list(s))

            for n, samples in nid_sample_map.items():
                # fit a Gaussian Mixture Model with two components
                if len(samples) < self.gmm_k:
                    k = len(samples)
                else:
                    k = self.gmm_k

                gmm = mixture.GaussianMixture(n_components=k, covariance_type='full')
                gmm.fit(samples)

                self.gmm_by_node[n] = gmm

        def predict(self, X_test):
            """Return an array-like of lists of gmms"""
            return [self.gmm_by_node[n] for n in self.dt.apply(X_test)]

    return ProbDT(dt, X_train)


def eval_prob_rf(prf, X, y, scalers):
    """Evaluate a gmm with negative log likelihood.

    Generate the GMM at each configuration and evaluate the GMM against the groundtruth."""

    dataset = labels_by_x(X, y)

    scores = list()
    for ft, values in dataset.items():
        ft = np.array(ft).reshape(-1, 1)
        gmm = prf.predict(ft)[0]
        # figure out how the gmm would predict the groundtruth values
        y_true = np.array(values).reshape(-1, 1)

        scores.append((scalers[0].inverse_transform(ft)[0], gmm.score(y_true)))

    return scores


class ProbRF:
    """Given a random forest point-estimator for regression, return one with
    probability output."""

    def __init__(self, rf=None):
        self.rf = rf

        self.X_train = None
        self.y_train = None
        self.gmm_k = None

        self.nid_sample_map = defaultdict(list)

    def _prepare_sample_map(self, X_train, y_train):
        """On each leaf node of the DT, prepare the gmm for the samples."""
        leaves_of_nodes = self.rf.apply(X_train)

        # build an index for the training samples at the leave nodes of the forest
        for sample, leaves in zip(y_train, leaves_of_nodes):
            for i, l in enumerate(leaves):
                self.nid_sample_map[(i, l)].append(sample.reshape(1))

    def _gmm_or_samples(self, X, gmm=True):
        """Walk through the leave nodes and return either the GMM or raw samples."""
        # obtain the leave nodes
        rf_nodes = self.rf.apply(X)

        results = list()
        for nodes in rf_nodes:
            samples = list()
            for i, n in enumerate(nodes):
                samples.append(self.nid_sample_map[(i, n)])
            samples = list(chain(*samples))  # flatten out

            assert len(samples) > 0

            # TODO: confirm whether this is what we want to handle the case of samples equal to 1
            if len(samples) < 2:
                samples += samples

            if len(samples) < self.gmm_k:
                k = len(samples)
            else:
                k = self.gmm_k

            if gmm:
                gmm = mixture.GaussianMixture(n_components=k, covariance_type='full')
                gmm.fit(samples)

                results.append(gmm)
            else:
                results.append(samples)
        return results

    def _tune_train_rf(self, X_train, y_train):
        # TODO:
        from oracle.learn.tune import default_models, hypertune_e2e
        model = {"random_forest": default_models["random_forest"]}
        # print(sum(X_train))
        # opt_params = hypertune_e2e(X_train, y_train, model, quiet=True)
        return None

    def fit(self, X_train, y_train, gmm_k=2):
        if self.rf is None:
            self.rf = self._tune_train_rf(X_train, y_train)

        self.X_train = X_train
        self.y_train = y_train
        self.gmm_k = gmm_k

        self._prepare_sample_map(X_train, y_train)

    def predict(self, X):
        """Return an array-like of lists of gmms. The gmm cannot be precomputed
        and cached, unlike the DT, since the ensemble is not known beforeahand."""
        return self._gmm_or_samples(X, gmm=True)

    def predict_proba(self, X, y):
        return [g.predict_proba(_y) for _y, g in zip(y, self._gmm_or_samples(X, gmm=True))]

    def apply(self, X):
        """Returns the samples that used to generate the GMMs."""
        return self._gmm_or_samples(X, gmm=False)


class TwoStageRF:
    """Note that training the second stage will use all training samples across all trees."""

    class TwoStageDT:
        """First-stage tree is a usual decision tree. Second-stage are decision trees at leave nodes.
        Each leave node contains exactly one decision tree."""

        def __init__(self, first_dt: DecisionTreeRegressor, sec_dts: Dict[np.int64, DecisionTreeRegressor]):
            self.first_stage_dt = first_dt

            # decision tree indexed by first-stage dt's leave node ids
            self.sec_stage_dts = sec_dts

        def predict(self, X, X_extra):
            """First get the first-stage leave, then use X_extra to walk the second stage."""
            # TODO:
            leaves = self.first_stage_dt.apply(X)

            y_pred = list()
            for x_, l in zip(X_extra, leaves):
                sec_dt = self.sec_stage_dts[l]
                y_pred.append(sec_dt.predict(np.array(x_).reshape(1, -1)))

            return np.asarray(y_pred).reshape(1, -1)[0]

        def apply(self):
            pass

        def display(self, first_stage_features=None, sec_stage_features=None, class_names=None):
            plt.figure(figsize=(20, 10))

            setup = {
                "impurity": False,
                "fontsize": 12,
                "rotate": True,
                "rounded": True,
                "node_ids": False,
                "max_depth": 3,
                "class_names": class_names,
            }

            plot_tree(self.first_stage_dt, feature_names=first_stage_features, **setup)

            # stage-2 trees
            setup["node_ids"] = False
            for k, v in self.sec_stage_dts.items():
                plt.figure(figsize=(20, 6))
                plot_tree(v, feature_names=sec_stage_features, **setup)

        def combine_stages(self):
            """Returns a combined tree."""
            self.first_stage_dt: DecisionTreeRegressor

    def __init__(self, rf: RandomForestRegressor):
        self.src_rf = rf

        self.estimators = list()

    def fit(self, train_df, stage_1_features: set, label_feature: str, stage_2_features: set = None, scalers=None):
        X = train_df[list(stage_1_features)].values.astype(float)
        X_extra = train_df[list(stage_2_features)].values
        y = train_df[list(label_feature)].values

        if scalers is not None:
            X = scalers[0].transform(X)
            y = scalers[1].transform(y)

        # TODO: pre-proc the X_extra and y

        for est in self.src_rf.estimators_:
            # get the leaf node reached by the data sample and
            # find the leaves with original features
            leaves = est.apply(X)
            leave_samples = defaultdict(list)

            # build an index for the training samples at the leave nodes of the forest
            # each sample contains only the additional features in stage two
            for x_extra, y_, l, in zip(X_extra, y, leaves):
                leave_samples[l].append((x_extra, y_))

            # import pprint as pp
            # print(sum([len(v) for v in leave_samples.values()]))
            # pp.pprint(leave_samples)

            # train decision trees for each leave node
            dt_by_leave = dict()
            for l, leave_samples in leave_samples.items():
                X_ = np.array([i[0] for i in leave_samples])
                y_ = np.array([i[1] for i in leave_samples])

                # print("samples:")
                # print(X_, y_)

                dt = DecisionTreeRegressor(random_state=42)
                dt.fit(X_, y_)
                dt_by_leave[l] = dt

            # create two-stage dt
            self.estimators.append(self.TwoStageDT(est, dt_by_leave))

    def predict(self, test_df, stage_1_features: set, stage_2_features: set, scalers=None):
        X = test_df[list(stage_1_features)].values.astype(float)
        if scalers is not None:
            X = scalers[0].transform(X)

        X_extra = test_df[list(stage_2_features)].values

        # ensemble by averaging the estimator results
        # TODO: alternatively, replace the original rf's estimators with two-stage DT
        est_y_pred = list()
        for est in self.estimators:
            est_y_pred.append(est.predict(X, X_extra))

        # compute the average
        return np.asarray([np.mean(i) for i in np.asarray(est_y_pred).transpose()])

    def apply(self):
        pass

    def predict_prob(self, X_test, y_test):
        pass

    def display(self, first_stage_features=None, sec_stage_features=None, class_names=None, max_count=2):
        count = 0
        print("n_estimators:", len(self.estimators))
        for est in self.estimators:
            est.display(first_stage_features, sec_stage_features, class_names)
            count += 1
            if max_count is not None and count >= max_count:
                break


default_search_space = {
    "num_component": tune.grid_search([1, 2, 3, 4, 5]),
}


def tune_prob_rf(ds, rf, search_space=default_search_space):
    search_space = {**default_search_space, **search_space}

    def train_eval_rf(config):
        for i in range(1):
            score = train_eval_prob_rf(ds, rf, **config)
            tune.track.log(mean_accuracy=-score)

    analysis = tune.run(train_eval_rf, config=search_space)
    return analysis


def eval_gmms(gmms, y_test, scalers):
    from oracle.learn.eval import rmsre
    y_pred = list()

    for gmm, y in zip(gmms, y_test):
        min_diff = sys.maxsize
        selected = None

        values = gmm.means_ if scalers is None else scalers[1].inverse_transform(gmm.means_)

        for m in values:
            diff_ = abs(m[0] - y)
            if diff_ < min_diff:
                min_diff = diff_
                selected = m[0]
        y_pred.append(selected)
    return rmsre(y_pred, y_test)


def train_eval_prob_rf(ds, rf, num_component):
    X_train, y_train, X_test, y_test, scalers = ds

    prf = ProbRF(rf)
    prf.fit(X_train, y_train, gmm_k=num_component)

    gmms = prf.predict(X_test)

    y_test = scalers[1].inverse_transform(y_test)
    score = eval_gmms(gmms, y_test, scalers)
    return score


from examples.ml_util.mdn import plot_num_component_rmsre
