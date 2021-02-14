"""Convert and normalize resource config values."""


def is_int(s, strict=True):
    try:
        int(s)
        if strict and "." in s:
            return False
        else:
            return True
    except ValueError:
        return False


def is_float(s, strict=True):
    # TODO: confirm pandas scientific notation
    try:
        float(s)
        if strict and "." not in s:
            return False
        else:
            return True
    except ValueError:
        return False


def normalized_key(key: str, readable=False):
    return key.replace(" ", "_")


def normalized_value(value: str, readable=False):
    if value in {"Yes"}:
        return 1
    # TODO: reason about handling of unknown values
    elif value in {"unavailable", "unknown", "No", "N/A"}:
        return 0
    else:
        if not readable:
            value = value.split()[0]   # remove unit
            value = value.lstrip("$")  # remove prefix

        if is_int(value):
            value = int(value)
        elif is_float(value):
            value = float(value)

        return value
