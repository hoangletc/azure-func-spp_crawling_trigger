import json
import logging
import traceback
from pathlib import Path
from typing import Tuple

import azure.functions as func

from . import modes

PATH_DIR_CONFIG = "configs"


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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        body = req.get_json()
        mode_funcapp = body.get("mode_funcapp", "")

        assert mode_funcapp in (
            "quantity_prober",
            "crawling_config_generator",
            "gen_blobpath",
            "processing",
            "collect_rowstamp",
        ), "Incorrect field 'mode_funcapp'"

        # Get config
        signal_info, conf = get_configs(PATH_DIR_CONFIG)

        if mode_funcapp == "quantity_prober":
            out = modes.quantity_prober(signal_info, body)
        elif mode_funcapp == "crawling_config_generator":
            out = modes.gen_crawling_conf(signal_info, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)

        elif mode_funcapp == "gen_blobpath":
            out = modes.gen_blobpath(conf, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)
        elif mode_funcapp == "processing":
            out = modes.process(signal_info, conf, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)

        elif mode_funcapp == "collect_rowstamp":
            out = modes.collect_maxrowstamp(conf, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)
        else:
            raise NotImplementedError()
        status_code = 200

    except Exception:
        out = traceback.format_exc()
        status_code = 500

    return func.HttpResponse(out, status_code=status_code, mimetype="application/json")
