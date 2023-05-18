import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Tuple, Union

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient


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


def gen_con_str(conf: dict, environment: str):
    conf_env = conf["storage"][environment]

    return ";".join(
        [
            f"DefaultEndpointsProtocol={conf_env['DefaultEndpointsProtocol']}",
            f"AccountName={conf_env['AccountName']}",
            f"AccountKey={conf_env['AccountKey']}",
            f"EndpointSuffix={conf_env['EndpointSuffix']}",
        ]
    )


def get_dt_str(option: int = 1):
    a = datetime.utcnow() + timedelta(hours=7)

    if option == 1:
        return a.strftime("%Y%m%d")
    elif option == 2:
        return a.strftime("%H%M%S")


def download_blob(container_name: str, blob_name: str, conn_str: str) -> Any:
    client = BlobClient.from_connection_string(conn_str, container_name=container_name, blob_name=blob_name)

    data = client.download_blob().readall()
    return json.loads(data)


def save_blob(data: Union[dict, list], filename: str, path_store: str, conn_str: str):
    # Establish connection
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=path_store, blob=filename)

    # Convert str to binary
    data_encoded = bytes(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")

    # Write to Azure Blob
    blob_client.upload_blob(data_encoded, blob_type="BlockBlob", overwrite=True)


def list_blob(conf: dict, conn_str: str, list_type: str = "data", signal: str = "") -> List[str]:
    today = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d")

    if list_type == "data":
        assert signal != ""
        tag = rf"{conf['storage']['raw']}/{today}/{signal}/.+\.json"
    elif list_type == "rowstamp_conf":
        tag = r"BI_\w+\/last_rowstamp\/.+\.json"
    else:
        raise NotImplementedError()

    client = ContainerClient.from_connection_string(conn_str, container_name=conf["storage"]["container"])

    valid_blobs = []
    for path in client.list_blob_names():
        if re.findall(tag, path) != []:
            valid_blobs.append(path)

    return valid_blobs
