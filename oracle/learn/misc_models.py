from collections import defaultdict
import numpy as np


def oracle_predict(data: list, scaler=None, multi_dim=True):
    """Data contains tuple of X, y pairs."""
    bins = defaultdict(list)

    results_true = list()
    results_pred = list()
    for x, y in data:
        if multi_dim and len(x) > 1:
            bins[tuple(x)].append(y)
        else:
            bins[x[0]].append(y)

    for x, b in bins.items():
        if scaler is not None:
            jcts = scaler.inverse_transform(b).tolist()
        else:
            jcts = b

        m = strange_mean(jcts)

        results_true += jcts
        results_pred += [m] * len(jcts)

    return results_true, results_pred


def strange_mean(values):
    part_1 = sum(1 / v for v in values)
    part_2 = sum(1 / (v ** 2) for v in values)
    return part_1 / part_2


def naive_predict(data: list, scaler=None):
    """Data contains tuple of X, y pairs."""
    bins = defaultdict(list)

    results_true = list()
    results_pred = list()
    for x, y in data:
        bins[x[0]].append(y)

    # m = np.mean([y for _, y in data])
    if scaler is not None:
        all_jcts = scaler.inverse_transform([y for _, y in data]).tolist()
    else:
        all_jcts = [y for _, y in data]
    m = np.mean(all_jcts)
    # print(m)

    for x, b in bins.items():
        # get the harmonic mean as the predicted value
        if scaler is not None:
            jcts = scaler.inverse_transform(b).tolist()
        else:
            jcts = b

        results_true += jcts
        results_pred += [m] * len(jcts)

    return results_true, results_pred