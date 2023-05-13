import json
import logging
import traceback

import azure.functions as func

from . import modes, utils

PATH_DIR_CONFIG = "configs"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        body = req.get_json()
        mode_funcapp = body.get("mode_funcapp", "")

        assert mode_funcapp in (
            "quantity_prober",
            "crawling_config_generator",
            "processing",
        ), "Incorrect field 'mode_funcapp'"

        # Get config
        signal_info, conf = utils.get_configs(PATH_DIR_CONFIG)

        if mode_funcapp == "quantity_prober":
            out = modes.quantity_prober(signal_info, body)
        elif mode_funcapp == "crawling_config_generator":
            out = modes.gen_crawling_conf(signal_info, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)
        elif mode_funcapp == "processing":
            out = modes.processing(signal_info, conf, body)
            out = json.dumps(out, ensure_ascii=False, indent=2)
        else:
            raise NotImplementedError()
        status_code = 200

    except Exception:
        out = traceback.format_exc()
        status_code = 500

    return func.HttpResponse(out, status_code=status_code, mimetype="application/json")
