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
    lastfetchs: str = "",
) -> str:
    params: dict[str, Any] = {
        "lean": 1,
        "pageno": 1,
        "oslc.pageSize": 1,
        "collectioncount": 1,
        "ignorecollectionref": 1,
    }

    if mode_crawl == "incremental":
        params |= {"fetchmodedelta": 1, "lastfetchts": lastfetchs}

    # Add 'signal'
    prober_field = signal_info.get(signal, {}).get("prober_field", None)
    params["oslc.select"] = prober_field

    # Prepare final url
    api_name = signal_info.get(signal, {}).get("api_name", None)

    req = PreparedRequest()
    url = f"{scheme}://{ip}:{port}/{path}/{api_name}"
    req.prepare_url(url, params)

    return req.url


def quantity_prober(signal_info: dict, body: dict) -> Any:
    ip: str = body.get("ip", "10.100.60.130")
    port: int = body.get("port", 9081)
    path: str = body.get("path", "maxdev/oslc/os")

    mode_crawl: str = body.get("mode_crawl", None)
    signal: str = body.get("signal", None)
    lastfetchs: str = body.get("lastfetchs", "")

    # Initial checks
    assert signal is not None and signal in signal_info, "Incorrect field 'signal'"
    assert mode_crawl in (
        "historical",
        "incremental",
    ), "Field 'mode_crawl ony accepts 2 values: 'historical', 'incremental'"

    # Handle main
    url = _get_probing_url(signal_info, signal, mode_crawl, "http", ip, port, path, lastfetchs)

    return url
