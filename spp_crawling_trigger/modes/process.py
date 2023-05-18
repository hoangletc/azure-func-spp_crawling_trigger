import json
import re
from datetime import datetime, timedelta
from typing import Iterable, List, Union

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

# CONNECTION_STR = "DefaultEndpointsProtocol=https;AccountName=spvbstoragedevv2;AccountKey=mnql8TSM53Myn/rHlSiVMTSpXz9zL1oUnv3U8tIvtVIsHRELVjMPjwRU2qj58V7w+zevlopk8X2vrqxqb+OSUA==;EndpointSuffix=core.windows.net"


def _gen_con_str(conf: dict, environment: str):
    conf_env = conf["storage"][environment]

    return ";".join(
        [
            f"DefaultEndpointsProtocol={conf_env['DefaultEndpointsProtocol']}",
            f"AccountName={conf_env['AccountName']}",
            f"AccountKey={conf_env['AccountKey']}",
            f"EndpointSuffix={conf_env['EndpointSuffix']}",
        ]
    )


def _get_dt_str(option: int = 1):
    a = datetime.utcnow() + timedelta(hours=7)

    if option == 1:
        return a.strftime("%Y%m%d")
    elif option == 2:
        return a.strftime("%H%M%S")


def _download_blob(container_name: str, blob_name: str, conn_str: str) -> List[dict]:
    client = BlobClient.from_connection_string(conn_str, container_name=container_name, blob_name=blob_name)

    data = client.download_blob().readall()
    return json.loads(data)


def _save_blob(data: Union[dict, list], filename: str, path_store: str, conn_str: str):
    # Establish connection
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=path_store, blob=filename)

    # Convert str to binary
    data_encoded = bytes(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")

    # Write to Azure Blob
    blob_client.upload_blob(data_encoded, blob_type="BlockBlob", overwrite=True)


def _list_blob(signal: str, conf: dict, conn_str: str) -> Iterable[str]:
    today = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")

    tag = rf"{conf['storage']['raw']}/{today}/{signal}/.+\.json"

    client = ContainerClient.from_connection_string(conn_str, container_name=conf["storage"]["container"])

    valid_blobs = []
    for path in client.list_blob_names():
        if re.findall(tag, path) != []:
            valid_blobs.append(path)

    return valid_blobs


def _parser(name: str, d: List[dict], signal_info: dict) -> dict:
    def parse(r, fields: set):
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
        if name == "BI_INVE":
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


def processing(signal_info: dict, conf: dict, body: dict) -> List[dict]:
    signal = body.get("signal")
    env = body.get("environment", "dev")
    body.get("container", "spp")

    assert env in ["dev", "prod"]
    assert signal is not None, "Field 'signal' not specified"

    last_rowstamp = {}

    # Establish all valid paths
    conn_str = _gen_con_str(conf, env)
    valid_blobs = _list_blob(signal, conf, conn_str)

    for blob in valid_blobs:
        data = _download_blob(conf["storage"]["container"], blob, conn_str)

        # Process
        processed = _parser(signal, data["member"], signal_info)

        # Store file
        pat = r"page_(\d+)_.*\.json"
        pagenum = re.findall(pat, blob)[0]

        for signal_name, d in processed.items():
            # Save backup
            store_filename = f"{conf['storage']['processed']}/{signal_name}/{_get_dt_str(1)}/page_{pagenum}_{_get_dt_str(2)}.json"  # noqa: E501
            _save_blob(d, store_filename, conf["storage"]["container"], conn_str)

            # Save for later loading
            store_filename = f"{signal_name}/now/page_{pagenum}_{_get_dt_str(1)}_{_get_dt_str(2)}.json"
            _save_blob(d, store_filename, conf["storage"]["container"], conn_str)

            # Update last_rowstamp
            max_rowstamp = _find_maxrowstamp(d)

            if signal_name not in last_rowstamp:
                last_rowstamp[signal_name] = max_rowstamp
            elif max_rowstamp > last_rowstamp[signal_name]:
                last_rowstamp[signal_name] = max_rowstamp

    # Make up output
    list_max_rowstamp = [{"signal": k, "max_rowstamp": v} for k, v in last_rowstamp.items()]

    return list_max_rowstamp
