import numpy as np


def stats(vs: list):
    vs = [float(v) for v in vs]

    if len(vs) == 0:
        return {
            "max": -1,
            "mean": -1,
            "min": -1,
            "50pc": -1,
            "75pc": -1,
            "95pc": -1,
            "99pc": -1,
        }

    return {
        "max": np.max(vs),
        "mean": np.mean(vs),
        "min": np.min(vs),
        "50pc": np.percentile(vs, 50, interpolation="nearest"),
        "75pc": np.percentile(vs, 75, interpolation="nearest"),
        "95pc": np.percentile(vs, 95, interpolation="nearest"),
        "99pc": np.percentile(vs, 99, interpolation="nearest"),
    }
