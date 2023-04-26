from datetime import datetime, timedelta
from typing import List, Union

# BI_ASSET_1 chính là assetstatus
SIGNALS = [
    "BI_MATU", "BI_ASSET_01", "BI_INVE",
    "BI_ITEM", "BI_MATR", "BI_WO",
    "BI_SERV", "BI_INVT", "BI_LOC",
    "BI_ASSET_02"
    # "BI_INVU_MATU", "BI_INVUL_MATU",
    # "BI_INVU_MATR", "BI_INVUL_MATR",
    # "BI_ASSETSTATUS", "BI_WOSTATUS"
]


def get_urls(
    api_names: Union[str, list],
    conf_signals: dict,
    scheme: str = 'http',
    ip: str = "10.100.62.26",
    port: str = "9084",
    endpoint: str = "maximo/oslc/os",
    pagesize: int = None,
    maxpage: int = 100,
    is_date_filter: bool = False
) -> List[str]:

    # Prepare something before establishing URLs
    ports = port.split(',')

    if isinstance(api_names, str):
        if api_names == "all":
            api_names = SIGNALS
        else:
            api_names = api_names.split(',')

    out = []
    i_port = 0
    for name in api_names:
        signal = conf_signals[name]

        # fields = signal['fields']
        pagesize = pagesize if pagesize is not None else signal['pagesize']
        changedate = signal['changedate']

        for pageno in range(1, maxpage + 1):
            # params = {
            #     'lean': 1,
            #     'pageno': pageno,
            #     'oslc.pageSize': pagesize,
            #     'collectioncount': 1,
            #     'ignorecollectionref': 1
            # }

            # params['oslc.select'] = ','.join(fields)
            tail = f"&oslc.pageSize={pagesize}&pageno={pageno}"

            if is_date_filter is True:
                start_crawling_date = (datetime.now() + timedelta(days=-2)).isoformat()
                # params['oslc.where'] = f"{changedate}>\"{start_crawling_date}\""
                tail = tail + f"&oslc.where={changedate}>\"{start_crawling_date}\""

            url = f"{scheme}://{ip}:{ports[i_port]}/{endpoint}/{name}/?{signal['url']}{tail}"
            i_port = (i_port + 1) % len(ports)

            out.append(url)

    return out
