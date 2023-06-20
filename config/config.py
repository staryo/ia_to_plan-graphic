from yaml import safe_load

__all__ = [
    'read_config',
]


def read_config(config_filepath):
    with open(config_filepath, 'r', encoding="utf-8") as f:
        return safe_load(f)
