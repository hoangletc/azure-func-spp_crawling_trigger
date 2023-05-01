from datetime import datetime, timedelta
from typing import Any

from requests.models import PreparedRequest


def _get_probing_url(
    signal_info: dict,
    signal: str,
    mode_crawl: str = "historical",
    scheme: str = "http",
    ip: str = "10.100.62.26",
    port: int = 9081,
    path: str = "maxdev/oslc/os",
    where: str = "",
) -> str:
    params: dict[str, Any] = {
        "lean": 1,
        "pageno": 1,
        "oslc.pageSize": 1,
        "collectioncount": 1,
        "ignorecollectionref": 1,
        "oslc.where": where,
    }

    # Add 'signal'
    prober_field = signal_info.get(signal, {}).get("prober_field", None)
    params["oslc.select"] = prober_field

    # (Optional) Add date
    # NOTE: HoangLe [May-01]: Phần này sẽ được thay thế bởi fetch _rowstamp sau khi có được cách filter với _rowstamp đúng
    if mode_crawl == "incremental":
        start_crawling_date = (datetime.now() + timedelta(days=-2)).isoformat()
        incremental_field = signal_info.get(signal)
        params["oslc.where"] = (
            params["oslc.where"] + "and" + f'{incremental_field}>"{start_crawling_date}'
        )

    # Prepare final url
    api_name = signal_info.get(signal, {}).get("api_name", None)
    if params["oslc.where"] == "":
        del params["oslc.where"]

    req = PreparedRequest()
    url = f"{scheme}://{ip}:{port}/{path}/{api_name}"
    req.prepare_url(url, params)

    return req.url


def quantity_prober(signal_info: dict, body: dict) -> Any:
    ip: str = body.get("ip", "10.100.60.130")
    port: int = body.get("port", 9081)
    path: str = body.get("path", "maxdev/oslc/os")

    mode_crawl: str = body.get("mode_crawl", "historical")
    signal: str = body.get("signal", None)
    where: str = body.get("where", "")

    # Initial checks
    assert signal is not None and signal in signal_info, "Incorrect field 'signal'"
    assert mode_crawl in (
        "historical",
        "incremental",
    ), "Field 'mode_crawl ony accepts 2 values: 'historical', 'incremental'"

    # Handle main
    url = _get_probing_url(
        signal_info, signal, mode_crawl, "http", ip, port, path, where
    )

    return url
