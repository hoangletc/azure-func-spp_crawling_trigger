from typing import List

from . import utils


def collect_maxrowstamp(conf: dict, body: dict) -> List[dict]:
    env = body.get("environment", "dev")

    assert env in ["dev", "prod"]

    # Establish all valid paths
    conn_str = utils.gen_con_str(conf, env)

    valid_path_configs = utils.list_blob(conf, conn_str, list_type="rowstamp_conf")

    last_rowstamp = {}
    for path in valid_path_configs:
        d = utils.download_blob(conf["storage"]["container"], path, conn_str)

        signal, rowstamp = d["signal"], d["rowstamp"]
        if signal not in last_rowstamp or last_rowstamp.get(signal, 0) < rowstamp:
            last_rowstamp[signal] = rowstamp

    list_rowstamps = [{"signal": k, "max_rowstamp": v} for k, v in last_rowstamp.items()]

    return list_rowstamps
