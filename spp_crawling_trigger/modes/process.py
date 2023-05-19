import re
from typing import List

from . import utils


def _parser(name: str, d: List[dict], signal_info: dict) -> dict:
    def parse(r, fields: set):
        if "_imagelibref" in r:
            del r["_imagelibref"]
        if "href" in r:
            del r["href"]
        if "localref" in r:
            del r["localref"]

        entry = {**r}

        existing_fields = set(r.keys())
        nonexisted_fields = set(fields).difference(existing_fields)
        for f in nonexisted_fields:
            entry[f] = None

        return entry

    out = {name: []}

    for x in d:
        if name == "BI_ASSET":
            p_asset = parse(x, signal_info["BI_ASSET"]["fields"])

            if "assetancestor" in x:
                if "BI_ASSETANCESTOR" not in out:
                    out["BI_ASSETANCESTOR"] = []
                if "assetancestor" in p_asset:
                    del p_asset["assetancestor"]

                for x1 in x["assetancestor"]:
                    p = parse(x1, signal_info["BI_ASSETANCESTOR"]["fields"])
                    p["assetuid"] = p_asset["assetuid"]

                    out["BI_ASSETANCESTOR"].append(p)

            out[name].append(p_asset)
        elif name == "BI_INVE":
            p_inve = parse(x, signal_info["BI_INVE"]["fields"])

            if "invcost" in x:
                if "BI_INVCOST" not in out:
                    out["BI_INVCOST"] = []
                if "invcost" in p_inve:
                    del p_inve["invcost"]

                for x1 in x["invcost"]:
                    p = parse(x1, signal_info["BI_INVCOST"]["fields"])
                    p["inventoryid"] = p_inve["inventoryid"]

                    out["BI_INVCOST"].append(p)

            out[name].append(p_inve)
        elif name == "BI_MATU":
            if "BI_INVU" not in out:
                out["BI_INVU"] = []
            if "BI_INVUL" not in out:
                out["BI_INVUL"] = []

            p_matu = parse(x, signal_info["BI_MATU"]["fields"])

            if "invuse" in x:
                if "invuse" in p_matu:
                    del p_matu["invuse"]

                if isinstance(x["invuse"], dict):
                    x["invuse"] = [x["invuse"]]
                for x1 in x["invuse"]:
                    p = parse(x1, signal_info["BI_INVU"]["fields"])
                    p["from"] = "MATU"

                    out["BI_INVU"].append(p)

            if "invuseline" in x:
                if "invuseline" in p_matu:
                    del p_matu["invuseline"]

                if isinstance(x["invuseline"], dict):
                    x["invuseline"] = [x["invuseline"]]
                for x1 in x["invuseline"]:
                    p = parse(x1, signal_info["BI_INVUL"]["fields"])
                    p["from"] = "MATU"

                    out["BI_INVUL"].append(p)

            out[name].append(p_matu)

        elif name == "BI_MATR":
            if "BI_INVU" not in out:
                out["BI_INVU"] = []
            if "BI_INVUL" not in out:
                out["BI_INVUL"] = []

            p_matr = parse(x, signal_info["BI_MATR"]["fields"])

            if "invuse" in x:
                if "invuse" in p_matr:
                    del p_matr["invuse"]

                if isinstance(x["invuse"], dict):
                    x["invuse"] = [x["invuse"]]
                for x1 in x["invuse"]:
                    p = parse(x1, signal_info["BI_INVU"]["fields"])
                    p["from"] = "MATR"

                    out["BI_INVU"].append(p)
            if "invuseline" in x:
                if "invuseline" in p_matr:
                    del p_matr["invuseline"]

                if isinstance(x["invuseline"], dict):
                    x["invuseline"] = [x["invuseline"]]
                for x1 in x["invuseline"]:
                    p = parse(x1, signal_info["BI_INVUL"]["fields"])
                    p["from"] = "MATR"

                    out["BI_INVUL"].append(p)

            out[name].append(p_matr)

        elif name == "BI_WO":
            if "BI_WOSTATUS" not in out:
                out["BI_WOSTATUS"] = []

            p_wo = parse(x, signal_info["BI_WO"]["fields"])

            if "wostatus" in x:
                if "wostatus" in p_wo:
                    del p_wo["wostatus"]

                for x1 in x["wostatus"]:
                    p = parse(x1, signal_info["BI_WOSTATUS"]["fields"])
                    p["workorderid"] = p_wo["workorderid"]

                    out["BI_WOSTATUS"].append(p)

            out[name].append(p_wo)

        else:
            p = parse(x, signal_info[name]["fields"])
            out[name].append(p)

    return out


def _find_maxrowstamp(d: List[dict]) -> int:
    max_rowstamp = 0

    for r in d:
        if int(r["_rowstamp"]) > max_rowstamp:
            max_rowstamp = int(r["_rowstamp"])

    return max_rowstamp


def gen_blobpath(conf: dict, body: dict) -> List[str]:
    signal = body.get("signal")
    env = body.get("environment", "dev")

    assert env in ["dev", "prod"]
    assert signal is not None, "Field 'signal' not specified"

    # Establish all valid paths
    conn_str = utils.gen_con_str(conf, env)
    valid_blobs = utils.list_blob(conf, conn_str, list_type="data", signal=signal)

    return valid_blobs


def process(signal_info: dict, conf: dict, body: dict):
    signal = body.get("signal")
    env = body.get("environment", "dev")
    blob = body.get("blob")

    assert env in ["dev", "prod"]
    assert signal is not None, "Field 'signal' not specified"
    assert blob is not None

    # Establish all valid paths
    conn_str = utils.gen_con_str(conf, env)

    data = utils.download_blob(conf["storage"]["container"], blob, conn_str)

    # Process
    processed = _parser(signal, data["member"], signal_info)

    # Store file
    pat = r"page_(\d+)_.*\.json"
    pagenum = re.findall(pat, blob)[0]

    for signal_name, d in processed.items():
        # Save backup
        store_filename = f"{conf['storage']['processed']}/{signal_name}/{utils.get_dt_str(1)}/page_{pagenum}_{utils.get_dt_str(2)}.json"  # noqa: E501
        utils.save_blob(d, store_filename, conf["storage"]["container"], conn_str)

        # Save for later loading
        store_filename = f"{signal_name}/now/page_{pagenum}_{utils.get_dt_str(1)}_{utils.get_dt_str(2)}.json"
        utils.save_blob(d, store_filename, conf["storage"]["container"], conn_str)

        # Save last_rowstamp
        max_rowstamp = _find_maxrowstamp(d)

        store_filename = f"{signal_name}/last_rowstamp/page_{pagenum}_{utils.get_dt_str(1)}_{utils.get_dt_str(2)}.json"
        last_rowstamp_conf = {
            "info": "_rowstamp",
            "signal": signal_name,
            "rowstamp": max_rowstamp,
        }

        utils.save_blob(last_rowstamp_conf, store_filename, conf["storage"]["container"], conn_str)
