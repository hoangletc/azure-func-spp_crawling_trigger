import json
from pathlib import Path
from typing import Tuple


def get_configs(path_dir_conf: str) -> Tuple[dict, dict]:
    """Parse config from JSON file

    Returns:
        dict: signal config
    """

    assert Path(path_dir_conf).exists()

    # Load signal info
    path_signal_info = Path(path_dir_conf) / "signals.json"
    with open(path_signal_info) as fp:
        signal_info = json.load(fp)

    # Load general config
    path_general_conf = Path(path_dir_conf) / "general.json"
    with open(path_general_conf) as fp:
        general_conf = json.load(fp)

    return signal_info, general_conf
