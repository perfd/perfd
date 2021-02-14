import math
import numpy as np
from typing import List, Tuple
from sklearn.metrics import mean_squared_error

# evaluators
def rrse(y, y_pred):
    """Return the relative root squared error."""
    assert len(y) == len(y_pred)
    mean_y = np.mean(y)
    variance = sum([(mean_y - y[i]) ** 2 for i in range(len(y))])
    squared_error = sum([(y_pred[i] - y[i]) ** 2 for i in range(len(y))])

    if variance == 0:
        # TODO: figure out how to handle this
        return 0 if squared_error == 0 else 1
    else:
        return math.sqrt(squared_error / variance)


def rrmse(y, y_pred):
    """Return the relative root mean squared error."""
    rmse = math.sqrt(mean_squared_error(y, y_pred))

    return rmse / np.mean(y)


def make_rmsre_with_scaler(scaler=None, pc=None):
    def rmsre(y, y_pred):
        y = scaler.inverse_transform(y)
        y_pred = scaler.inverse_transform(y_pred)
        msre = 1 / len(y) * sum([((i - j) / i) ** 2 for i, j in zip(y, y_pred)])
        if msre > 100000:
            msre = float('inf')
        return math.sqrt(msre)

    return rmsre


def rmsre(y, y_pred):
    msre = 1 / len(y) * sum([((i - j) / i) ** 2 for i, j in zip(y, y_pred)])
    return math.sqrt(msre)


def relative_errors(y, y_pred):
    errors = list()
    for yt, yp in zip(y, y_pred):
        error = abs(abs(yt - yp) / yt)
        if error > 100000:
            error = float('inf')
            print(error)

        errors.append(error)
    return errors


def squared_error_cdf(ys, y_preds) -> List[Tuple[float, float]]:
    ys = np.array(ys)
    y_preds = np.array(y_preds)

    errors = list(map(abs, (ys - y_preds) / ys))
    cdf = [(e, i / len(errors)) for i, e in enumerate(sorted(errors))]

    return cdf


def evaluate(X_test, y_test, model):
    y_pred = model.predict(X_test)
    return rrmse(y_test, y_pred)


def baseline_mean(y_train, y_test):
    mean = np.mean(y_train)
    y_pred = [mean for _ in y_test]
    return rrmse(y_test, y_pred)


if __name__ == '__main__':
    y = np.random.randint(0, 1000, 100)
    y_pred = np.random.randint(0, 1000, 100)

    print(squared_error_cdf(y, y_pred))
