import json
import logging
import re
import traceback
from pathlib import Path
from typing import Tuple

import azure.functions as func

from . import crawl

PATH_DIR_CONFIG = "configs"


def get_configs() -> Tuple[dict, dict]:
    """Parse configs from JSON files

    Returns:
        Tuple[dict, dict]: 2 configs
    """

    path_dir_config = Path(PATH_DIR_CONFIG)
    assert path_dir_config.exists()

    # Read general configs
    # path_conf_general = path_dir_config / "general.json"
    # with open(path_conf_general) as fp:
    #     conf_general = json.load(fp)

    # Read config of signals
    path_conf_signal = path_dir_config / "signals.json"
    with open(path_conf_signal) as fp:
        conf_signals = json.load(fp)

    return conf_signals


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get fields
        body = req.get_json()

        api_module = body.get("api_module", 'creator')
        mode = body.get('mode', "incremental")
        ip = body.get('ip', "10.100.62.26")
        port = body.get('port', "9084")
        names = body.get('names')
        pagesize = body.get('pagesize', None)
        endpoint = body.get('endpoint', "maximo/oslc/os")

        total_pages = body.get('totalPages', None)
        href = body.get('href', None)

        # Get config
        conf_signals = get_configs()

        # Establish urls
        if mode == "incremental":
            is_date_filter = True
        elif mode == "historical":
            is_date_filter = False
        else:
            raise NotImplementedError()
        if api_module == "prober":
            maxpage = 1
        elif api_module == "creator":
            assert href is not None and total_pages is not None
            names = re.findall(r"\/(BI_\w+)", href)[0]
            maxpage = int(total_pages)
        else:
            raise NotImplementedError()

        urls = crawl.get_urls(names, conf_signals=conf_signals, ip=ip, port=port,
                              endpoint=endpoint, pagesize=pagesize, maxpage=maxpage,
                              is_date_filter=is_date_filter)

        # Add additional info
        out = json.dumps(urls)
        status_code = 200

    except Exception:
        out = traceback.format_exc()
        status_code = 500

    return func.HttpResponse(out, status_code=status_code, mimetype="application/json")
