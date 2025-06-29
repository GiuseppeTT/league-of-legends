from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc)


def convert_epoch_to_datetime(epoch: int):
    return datetime.fromtimestamp(epoch / 1000, timezone.utc)


def flatten_match(match: dict):
    for participant in match["info"]["participants"]:
        del participant["challenges"]
    flattened_match = flatten(match)
    return flattened_match


def flatten(dictionary: dict, parent_key="", separator="_"):
    flattened_dictionary = {}
    for key, value in dictionary.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened_dictionary.update(flatten(value, new_key, separator=separator))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened_dictionary.update(flatten(item, f"{new_key}{i}", separator=separator))
                else:
                    flattened_dictionary[f"{new_key}{i}"] = item
        else:
            flattened_dictionary[new_key] = value
    return flattened_dictionary
