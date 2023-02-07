import json
import urllib.parse
from typing import List

URLS = {
    'material_use_trans': "BI_MATU",
    "asset": "BI_ASSET",
    "inventory": "BI_INVE",
    "item": "BI_ITEM",
    "material_receipt_trans": "BI_MATR",
    "work_order": "BI_WO",
    "services": "BI_SERV",
    "inventory_trans": "BI_INVT",
    "inventory_balance": "BI_INVB",
    "location": "BI_LOC"
}


def get_urls(
    api_name: str,
    fields: List[str],
    orderby: str = None,
    where_con: str = None,
    scheme: str = 'http',
    ip: str = "10.100.60.130",
    port: int = 9082,
    endpoint: str = "maxtest/oslc/os",
    pagesize: int = 500,
    lean: int = 1,
    max_pages: int = 200
) -> List[str]:

    out = []

    api_names = [api_name]
    selected = ','.join(fields) if fields != [] else None
    if api_name == 'all':
        api_names = URLS.keys()
        selected = '*'

        # As selecting all, disregard order and where_con
        orderby = where_con = None

    for name in api_names:
        for pageno in range(1, max_pages + 1):
            params = {
                'lean': lean,
                'pageno': pageno,
                'oslc.pageSize': pagesize,
                'collectioncount': 1,
                'ignorecollectionref': 1
            }

            if selected is not None:
                params['oslc.select'] = selected
            if orderby is not None:
                params['oslc.orderby'] = orderby
            if where_con is not None:
                params['oslc.where'] = where_con

            url = f"{scheme}://{ip}:{port}/{endpoint}/{URLS[name]}/?{urllib.parse.urlencode(params)}"

            out.append(url)

    return out


if __name__ == '__main__':
    req = {
        "res": "material_use_trans",
        "orderby": "+matusetransid",
        "where": "transdate>=\"2023-02-01\""
    }

    orderby = req.get('orderby', None)
    where_con = req.get('where', None)
    fields = req.get('fields', [])
    res = req['res']

    urls = get_urls(res, fields, orderby, where_con)

    print(json.dumps(urls, indent=2))
