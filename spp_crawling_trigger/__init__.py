import json
import logging
import traceback

import azure.functions as func

from . import crawling_trigger as crt


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    output: str = None
    err: bool = False

    try:
        req_raw = req.get_json()
        if isinstance(req_raw, str):
            req = json.loads(req_raw)
        elif isinstance(req_raw, dict):
            req = req_raw

        assert isinstance(req, dict) \
            and 'res' in req \
            and isinstance(req['res'], str)

        orderby = req.get('orderby', None)
        where_con = req.get('where', None)
        pagesize = req.get('pagesize', 500)
        max_pages = req.get('maxpages', 100)
        fields = req.get('fields', [])
        res = req['res']

        urls = crt.get_urls(res, fields, orderby, where_con,
                            pagesize=pagesize, max_pages=max_pages)

        output = json.dumps(urls)

    except Exception:
        err = True
        output = traceback.format_exc()

    # NOTE: HoangLe [Feb-07]: Must be wrapped by try-catch

    if err is True:
        status_code = 500
    else:
        status_code = 200

    return func.HttpResponse(output, status_code=status_code, mimetype="application/json")
