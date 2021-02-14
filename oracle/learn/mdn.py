import sys
import numpy as np
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from ray import tune

tf.enable_eager_execution()
tf.random.set_random_seed(42)
np.random.seed(42)

from tensorflow_probability import distributions as tfd

from tensorflow.keras.layers import Input, Dense, Activation, Concatenate
from tensorflow.keras.callbacks import EarlyStopping, TensorBoard, ReduceLROnPlateau

import warnings

warnings.filterwarnings("always")


class MDN(tf.keras.Model):
    def __init__(self, neurons=100, components=2):
        super(MDN, self).__init__(name="MDN")
        self.neurons = neurons
        self.components = components

        self.h1 = Dense(neurons, activation="relu", name="h1")
        self.h2 = Dense(neurons, activation="relu", name="h2")

        self.alphas = Dense(components, activation="softmax", name="alphas")
        self.mus = Dense(components, name="mus")
        self.sigmas = Dense(components, activation="nnelu", name="sigmas")
        self.pvec = Concatenate(name="pvec")

    def call(self, inputs):
        x = self.h1(inputs)
        x = self.h2(x)

        alpha_v = self.alphas(x)
        mu_v = self.mus(x)
        sigma_v = self.sigmas(x)

        return self.pvec([alpha_v, mu_v, sigma_v])


def nnelu(input):
    """ Computes the Non-Negative Exponential Linear Unit
    """
    return tf.add(tf.constant(1, dtype=tf.float32), tf.nn.elu(input))


tf.keras.utils.get_custom_objects().update({'nnelu': Activation(nnelu)})


def slice_parameter_vectors(parameter_vector, components, num_parameter=3):
    """ Returns an unpacked list of paramter vectors.
    """
    return [parameter_vector[:, i * components:(i + 1) * components] for i in range(num_parameter)]


def make_mdn(components=2, neurons=50, num_parameter=3):
    opt = tf.compat.v1.train.AdamOptimizer(1e-3)

    def gnll_loss(y, parameter_vector):
        """ Computes the mean negative log-likelihood loss of y given the mixture parameters.
        """
        alpha, mu, sigma = slice_parameter_vectors(parameter_vector, components,
                                                   num_parameter)  # Unpack parameter vectors

        gm = tfd.MixtureSameFamily(
            mixture_distribution=tfd.Categorical(probs=alpha),
            components_distribution=tfd.Normal(
                loc=mu,
                scale=sigma))

        log_likelihood = gm.log_prob(tf.transpose(y))  # Evaluate log-probability of y

        return -tf.reduce_mean(log_likelihood, axis=-1)

    mdn = MDN(neurons=neurons, components=components)
    mdn.compile(loss=gnll_loss, optimizer=opt)
    return mdn


default_search_space = {
    "num_component": tune.grid_search([1, 2, 3, 4, 5]),
    "num_neuron": tune.grid_search([10, 20, 40, 80, 160]),
    "batch_size": tune.grid_search([1, 2, 4, 8, 16]),
}


def tune_mdn(ds, search_space=default_search_space):
    search_space = {**default_search_space, **search_space}

    def train_eval(config):
        for i in range(1):
            score = train_eval_mdn(ds, **config)
            tune.track.log(mean_accuracy=-score)

    analysis = tune.run(train_eval, config=search_space)
    return analysis


def train_eval_mdn(ds, num_component, num_neuron, batch_size, plot=False):
    X_train, y_train, X_test, y_test, scalers = ds

    mdn = make_mdn(num_component, num_neuron)
    mon = EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=0, mode='auto')

    mdn.fit(x=X_train, y=y_train, epochs=1000, validation_data=(X_test, y_test), callbacks=[mon], batch_size=batch_size,
            verbose=1)

    y_pred = mdn.predict(X_test)
    alpha_pred, mu_pred, sigma_pred = slice_parameter_vectors(y_pred, num_component)

    y_test = scalers[1].inverse_transform(y_test)
    mu_pred = scalers[1].inverse_transform(mu_pred)

    score = eval_mdn_top_k(y_test, mu_pred)

    return score


def eval_mdn_top_k(y_test, mu_pred):
    from oracle.learn.eval import rmsre
    y_pred = list()
    for mus, y in zip(mu_pred, y_test):
        min_diff = sys.maxsize
        selected = None
        for m in mus:
            diff_ = abs(m - y)
            if diff_ < min_diff:
                min_diff = diff_
                selected = m
        y_pred.append(selected)
    return rmsre(y_pred, y_test)


def plot_mdn_prediction(df: pd.DataFrame, mdn):
    ### TODO:

    pass


def plot_num_component_rmsre(df: pd.DataFrame, plt_name=None, text_under=False):
    x, y = list(), list()
    ncs = sorted(set(df["config/num_component"]))
    for nc in ncs:
        df_local = df[df["config/num_component"] == nc]
        acc = df_local["mean_accuracy"].max()
        x.append(nc)
        y.append(-acc * 100)
    plt.plot(x, y, marker="o")
    plt.xlabel("# components")
    plt.xticks(ncs)
    plt.ylabel("rMSRE (%)")
    plt.grid()
    plt.title("MDN error rate by number of components" if plt_name is None else plt_name)

    for x_, y_ in zip(x, y):
        label = "{:.2f}".format(y_)

        plt.annotate(label,  # this is the text
                     (x_, y_ if not text_under else y_ - 3),  # this is the point to label
                     textcoords="offset points",  # how to position the text
                     xytext=(0, 10),  # distance from text to points (x,y)
                     ha='center')  # horizontal alignment can be left, right or center


