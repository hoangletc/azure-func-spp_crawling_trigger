from datetime import datetime, timedelta
from typing import List


def gen_crawling_conf(signal_info: dict, body: dict) -> List[dict]:
    mode_crawl: str = body.get("mode_crawl", "historical")
    signal: str = body.get("signal", None)
    pagesize: str = body.get("pagesize", "")
    where: str = body.get("where", "")
    totalpages: int = body.get("totalpages", 1)

    # Initial checks
    assert signal is not None and signal in signal_info, "Incorrect field 'signal'"
    assert mode_crawl in (
        "historical",
        "incremental",
    ), "Field 'mode_crawl ony accepts 2 values: 'historical', 'incremental'"

    # Prepare 'api_name'
    api_name = signal_info.get(signal, {}).get("api_name", None)

    # Prepare 'select'
    select = signal_info.get(signal, {}).get("fields", [])

    # Prepare 'pagenum'
    if pagesize == "":
        pagesize = signal_info.get(signal, {}).get("pagesize", 1000)
    pagenum = totalpages // int(pagesize)
    if totalpages % int(pagesize) > 0:
        pagenum += 1

    # Prepare 'where'
    # NOTE: HoangLe [May-01]: Phần này sẽ được thay thế bởi fetch _rowstamp sau khi có được cách filter với _rowstamp đúng
    if mode_crawl == "incremental":
        start_crawling_date = (datetime.now() + timedelta(days=-2)).isoformat()
        incremental_field = signal_info.get(signal)
        where = where + "and" + f'{incremental_field}>"{start_crawling_date}"'

    out = [
        {"api_name": api_name, "select": select, "where": where, "pageno": i}
        for i in range(1, pagenum + 1)
    ]

    return out
